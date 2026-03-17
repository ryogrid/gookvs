package txn

import (
	"testing"

	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- AcquirePessimisticLock Tests ---

func TestAcquirePessimisticLockBasic(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 10,
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	require.NoError(t, err)

	applyTxnModifies(t, eng, mvccTxn)

	// Verify the lock exists and is pessimistic.
	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	lock, err := reader2.LoadLock([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, txntypes.LockTypePessimistic, lock.LockType)
	assert.Equal(t, txntypes.TimeStamp(10), lock.StartTS)
	assert.Equal(t, txntypes.TimeStamp(10), lock.ForUpdateTS)
}

func TestAcquirePessimisticLockIdempotent(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// First acquisition.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 15, // Higher ForUpdateTS
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	require.NoError(t, err)

	// Should update ForUpdateTS.
	assert.Greater(t, len(mvccTxn.Modifies), 0)
}

func TestAcquirePessimisticLockConflictWithOtherTxn(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Another transaction holds the lock.
	txn1 := mvcc.NewMvccTxn(5)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("k1"),
		StartTS:  5,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 10,
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	assert.ErrorIs(t, err, ErrKeyIsLocked)
}

func TestAcquirePessimisticLockWriteConflict(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write committed at TS=20 (after our ForUpdateTS=10).
	txn1 := mvcc.NewMvccTxn(15)
	txn1.PutWrite([]byte("k1"), 20, &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    15,
		ShortValue: []byte("v"),
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 10, // Conflict: write at TS=20 > ForUpdateTS=10
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	assert.ErrorIs(t, err, ErrWriteConflict)
}

func TestAcquirePessimisticLockNoConflictWithOldWrite(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write committed at TS=5 (before our ForUpdateTS=10).
	txn1 := mvcc.NewMvccTxn(3)
	txn1.PutWrite([]byte("k1"), 5, &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    3,
		ShortValue: []byte("v"),
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 10,
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	assert.NoError(t, err)
}

func TestAcquirePessimisticLockAlreadyPrewrote(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Key already has a normal lock from same txn.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("k1"),
		StartTS:  10,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticLockProps{
		StartTS:     10,
		ForUpdateTS: 10,
		Primary:     []byte("k1"),
		LockTTL:     5000,
	}

	err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
	assert.NoError(t, err) // Already has a lock from same txn.
}

// --- PrewritePessimistic Tests ---

func TestPrewritePessimisticUpgrade(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Acquire pessimistic lock first.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		ForUpdateTS:   10,
		IsPessimistic: true,
	}

	err := PrewritePessimistic(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("value"),
	})
	require.NoError(t, err)

	applyTxnModifies(t, eng, mvccTxn)

	// Verify: lock should now be a normal Put lock.
	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	lock, err := reader2.LoadLock([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, txntypes.LockTypePut, lock.LockType)
	assert.Equal(t, []byte("value"), lock.ShortValue)
}

func TestPrewritePessimisticMissingLock(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		ForUpdateTS:   10,
		IsPessimistic: true,
	}

	err := PrewritePessimistic(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("value"),
	})
	assert.ErrorIs(t, err, ErrTxnLockNotFound)
}

func TestPrewritePessimisticNonPessimisticNew(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		IsPessimistic: false, // Not pessimistic.
	}

	err := PrewritePessimistic(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("value"),
	})
	require.NoError(t, err)
}

func TestPrewritePessimisticIdempotent(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Already has a normal lock from same txn.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("k1"),
		StartTS:  10,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := PessimisticPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		IsPessimistic: true,
	}

	err := PrewritePessimistic(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("value"),
	})
	assert.NoError(t, err) // Idempotent - already has normal lock.
}

// --- PessimisticRollback Tests ---

func TestPessimisticRollbackBasic(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Acquire pessimistic lock.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	errs := PessimisticRollback(mvccTxn, reader, []mvcc.Key{[]byte("k1")}, 10, 10)
	for _, err := range errs {
		assert.NoError(t, err)
	}

	applyTxnModifies(t, eng, mvccTxn)

	// Lock should be removed.
	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	lock, err := reader2.LoadLock([]byte("k1"))
	require.NoError(t, err)
	assert.Nil(t, lock)
}

func TestPessimisticRollbackNonPessimisticLock(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Normal lock (not pessimistic).
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("k1"),
		StartTS:  10,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	errs := PessimisticRollback(mvccTxn, reader, []mvcc.Key{[]byte("k1")}, 10, 0)
	for _, err := range errs {
		assert.NoError(t, err)
	}

	// Lock should NOT be removed (it's not pessimistic).
	assert.Empty(t, mvccTxn.Modifies)
}

func TestPessimisticRollbackNoLock(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	errs := PessimisticRollback(mvccTxn, reader, []mvcc.Key{[]byte("k1")}, 10, 10)
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Empty(t, mvccTxn.Modifies)
}

func TestPessimisticRollbackForUpdateTSMismatch(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	// Different ForUpdateTS.
	errs := PessimisticRollback(mvccTxn, reader, []mvcc.Key{[]byte("k1")}, 10, 20)
	for _, err := range errs {
		assert.NoError(t, err)
	}
	// Lock should NOT be removed (ForUpdateTS mismatch).
	assert.Empty(t, mvccTxn.Modifies)
}

func TestPessimisticRollbackMultipleKeys(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	txn1.PutLock([]byte("k2"), &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     []byte("k1"),
		StartTS:     10,
		TTL:         5000,
		ForUpdateTS: 10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	errs := PessimisticRollback(mvccTxn, reader, []mvcc.Key{[]byte("k1"), []byte("k2")}, 10, 0)
	for _, err := range errs {
		assert.NoError(t, err)
	}
	assert.Equal(t, 2, len(mvccTxn.Modifies))
}

// --- End-to-end Pessimistic Flow ---

func TestPessimisticFlowEndToEnd(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Step 1: Acquire pessimistic locks.
	{
		snap := eng.NewSnapshot()
		reader := mvcc.NewMvccReader(snap)
		mvccTxn := mvcc.NewMvccTxn(10)

		props := PessimisticLockProps{
			StartTS:     10,
			ForUpdateTS: 10,
			Primary:     []byte("k1"),
			LockTTL:     5000,
		}

		err := AcquirePessimisticLock(mvccTxn, reader, props, []byte("k1"))
		require.NoError(t, err)
		err = AcquirePessimisticLock(mvccTxn, reader, props, []byte("k2"))
		require.NoError(t, err)

		applyTxnModifies(t, eng, mvccTxn)
		reader.Close()
	}

	// Step 2: Prewrite (upgrade pessimistic locks).
	{
		snap := eng.NewSnapshot()
		reader := mvcc.NewMvccReader(snap)
		mvccTxn := mvcc.NewMvccTxn(10)

		props := PessimisticPrewriteProps{
			PrewriteProps: PrewriteProps{
				StartTS:   10,
				Primary:   []byte("k1"),
				LockTTL:   5000,
				IsPrimary: true,
			},
			ForUpdateTS:   10,
			IsPessimistic: true,
		}

		err := PrewritePessimistic(mvccTxn, reader, props, Mutation{
			Op: MutationOpPut, Key: []byte("k1"), Value: []byte("val1"),
		})
		require.NoError(t, err)

		props.IsPrimary = false
		err = PrewritePessimistic(mvccTxn, reader, props, Mutation{
			Op: MutationOpPut, Key: []byte("k2"), Value: []byte("val2"),
		})
		require.NoError(t, err)

		applyTxnModifies(t, eng, mvccTxn)
		reader.Close()
	}

	// Step 3: Commit.
	{
		snap := eng.NewSnapshot()
		reader := mvcc.NewMvccReader(snap)
		mvccTxn := mvcc.NewMvccTxn(10)

		err := Commit(mvccTxn, reader, []byte("k1"), 10, 20)
		require.NoError(t, err)
		err = Commit(mvccTxn, reader, []byte("k2"), 10, 20)
		require.NoError(t, err)

		applyTxnModifies(t, eng, mvccTxn)
		reader.Close()
	}

	// Step 4: Verify reads work.
	{
		snap := eng.NewSnapshot()
		reader := mvcc.NewMvccReader(snap)
		defer reader.Close()

		pg := mvcc.NewPointGetter(reader, 25, mvcc.IsolationLevelSI)
		val, err := pg.Get([]byte("k1"))
		require.NoError(t, err)
		assert.Equal(t, []byte("val1"), val)

		pg2 := mvcc.NewPointGetter(reader, 25, mvcc.IsolationLevelSI)
		val2, err := pg2.Get([]byte("k2"))
		require.NoError(t, err)
		assert.Equal(t, []byte("val2"), val2)
	}
}
