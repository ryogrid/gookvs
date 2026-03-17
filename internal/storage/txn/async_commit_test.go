package txn

import (
	"path/filepath"
	"testing"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupAsyncTestEngine(t *testing.T) (traits.KvEngine, func()) {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	return eng, func() {
		eng.Close()
	}
}

func applyTxnModifies(t *testing.T, eng traits.KvEngine, txn *mvcc.MvccTxn) {
	t.Helper()
	wb := eng.NewWriteBatch()
	for _, m := range txn.Modifies {
		switch m.Type {
		case mvcc.ModifyTypePut:
			require.NoError(t, wb.Put(m.CF, m.Key, m.Value))
		case mvcc.ModifyTypeDelete:
			require.NoError(t, wb.Delete(m.CF, m.Key))
		}
	}
	require.NoError(t, wb.Commit())
}

// --- Async Commit Prewrite Tests ---

func TestPrewriteAsyncCommitBasic(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := AsyncCommitPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("pk"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		UseAsyncCommit: true,
		Secondaries:    [][]byte{[]byte("sk1"), []byte("sk2")},
		MaxCommitTS:    15,
	}

	err := PrewriteAsyncCommit(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("pk"),
		Value: []byte("value"),
	})
	require.NoError(t, err)

	// Apply and verify.
	applyTxnModifies(t, eng, mvccTxn)

	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	lock, err := reader2.LoadLock([]byte("pk"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.True(t, lock.UseAsyncCommit)
	assert.Equal(t, 2, len(lock.Secondaries))
	assert.Equal(t, txntypes.TimeStamp(16), lock.MinCommitTS) // max(10+1, 15+1) = 16
}

func TestPrewriteAsyncCommitSecondary(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := AsyncCommitPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("pk"),
			LockTTL:   5000,
			IsPrimary: false,
		},
		UseAsyncCommit: true,
		MaxCommitTS:    12,
	}

	err := PrewriteAsyncCommit(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("sk1"),
		Value: []byte("val1"),
	})
	require.NoError(t, err)

	applyTxnModifies(t, eng, mvccTxn)

	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	lock, err := reader2.LoadLock([]byte("sk1"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.True(t, lock.UseAsyncCommit)
	// Secondary locks don't store secondaries list (empty or nil).
	assert.Empty(t, lock.Secondaries)
	assert.Equal(t, txntypes.TimeStamp(13), lock.MinCommitTS) // max(11, 13) = 13
}

func TestPrewriteAsyncCommitConflict(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// First, write and commit a value at TS=20.
	txn1 := mvcc.NewMvccTxn(15)
	txn1.PutLock([]byte("k1"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("k1"),
		StartTS:  15,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := AsyncCommitPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		UseAsyncCommit: true,
	}

	err := PrewriteAsyncCommit(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("v"),
	})
	assert.ErrorIs(t, err, ErrKeyIsLocked)
}

func TestPrewriteAsyncCommitIdempotent(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// First prewrite.
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
	props := AsyncCommitPrewriteProps{
		PrewriteProps: PrewriteProps{
			StartTS:   10,
			Primary:   []byte("k1"),
			LockTTL:   5000,
			IsPrimary: true,
		},
		UseAsyncCommit: true,
	}

	// Same StartTS - should be idempotent.
	err := PrewriteAsyncCommit(mvccTxn, reader, props, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("k1"),
		Value: []byte("v"),
	})
	assert.NoError(t, err)
}

// --- 1PC Tests ---

func TestPrewriteAndCommit1PCBasic(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := OnePCProps{
		StartTS:  10,
		CommitTS: 15,
		Primary:  []byte("k1"),
		LockTTL:  5000,
	}

	mutations := []Mutation{
		{Op: MutationOpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: MutationOpPut, Key: []byte("k2"), Value: []byte("v2")},
	}

	errs := PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)
	for _, err := range errs {
		assert.NoError(t, err)
	}

	applyTxnModifies(t, eng, mvccTxn)

	// Verify: reads should find the committed values (no locks in CF_LOCK).
	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	// No locks should exist.
	lock, err := reader2.LoadLock([]byte("k1"))
	require.NoError(t, err)
	assert.Nil(t, lock)

	// Commit records should exist.
	write, _, err := reader2.GetTxnCommitRecord([]byte("k1"), 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	assert.Equal(t, txntypes.WriteTypePut, write.WriteType)
	assert.Equal(t, txntypes.TimeStamp(10), write.StartTS)
}

func TestPrewriteAndCommit1PCWithShortValue(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := OnePCProps{
		StartTS:  10,
		CommitTS: 15,
		Primary:  []byte("k1"),
	}

	shortVal := []byte("short")
	mutations := []Mutation{
		{Op: MutationOpPut, Key: []byte("k1"), Value: shortVal},
	}

	errs := PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)
	for _, err := range errs {
		assert.NoError(t, err)
	}

	applyTxnModifies(t, eng, mvccTxn)

	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	write, _, err := reader2.GetTxnCommitRecord([]byte("k1"), 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	assert.Equal(t, shortVal, write.ShortValue)
}

func TestPrewriteAndCommit1PCWriteConflict(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write and commit at TS=20.
	txn1 := mvcc.NewMvccTxn(15)
	txn1.PutWrite([]byte("k1"), 20, &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    15,
		ShortValue: []byte("existing"),
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := OnePCProps{
		StartTS:  10,
		CommitTS: 12,
		Primary:  []byte("k1"),
	}

	mutations := []Mutation{
		{Op: MutationOpPut, Key: []byte("k1"), Value: []byte("v1")},
	}

	errs := PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)
	assert.ErrorIs(t, errs[0], ErrWriteConflict)
}

func TestPrewriteAndCommit1PCDelete(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(10)
	props := OnePCProps{
		StartTS:  10,
		CommitTS: 15,
		Primary:  []byte("k1"),
	}

	mutations := []Mutation{
		{Op: MutationOpDelete, Key: []byte("k1")},
	}

	errs := PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)
	for _, err := range errs {
		assert.NoError(t, err)
	}

	applyTxnModifies(t, eng, mvccTxn)

	snap2 := eng.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)
	defer reader2.Close()

	write, _, err := reader2.GetTxnCommitRecord([]byte("k1"), 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	assert.Equal(t, txntypes.WriteTypeDelete, write.WriteType)
}

// --- Eligibility Tests ---

func TestIs1PCEligible(t *testing.T) {
	tests := []struct {
		name     string
		muts     []Mutation
		maxSize  int
		eligible bool
	}{
		{"empty", nil, 64, false},
		{"single small", []Mutation{{Key: []byte("k"), Value: []byte("v")}}, 64, true},
		{"too many", make([]Mutation, 100), 64, false},
		{"default threshold", []Mutation{{Key: []byte("k"), Value: []byte("v")}}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.eligible, Is1PCEligible(tt.muts, tt.maxSize))
		})
	}
}

func TestIsAsyncCommitEligible(t *testing.T) {
	tests := []struct {
		name     string
		muts     []Mutation
		maxKeys  int
		eligible bool
	}{
		{"empty", nil, 256, false},
		{"single", []Mutation{{Key: []byte("k")}}, 256, true},
		{"at limit", make([]Mutation, 256), 256, true},
		{"over limit", make([]Mutation, 257), 256, false},
		{"default limit", []Mutation{{Key: []byte("k")}}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.eligible, IsAsyncCommitEligible(tt.muts, tt.maxKeys))
		})
	}
}

// --- CheckAsyncCommitStatus Tests ---

func TestCheckAsyncCommitStatusLocked(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write an async commit lock.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("pk"), &txntypes.Lock{
		LockType:       txntypes.LockTypePut,
		Primary:        []byte("pk"),
		StartTS:        10,
		TTL:            5000,
		UseAsyncCommit: true,
		MinCommitTS:    15,
		Secondaries:    [][]byte{[]byte("sk1")},
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	commitTS, err := CheckAsyncCommitStatus(reader, []byte("pk"), 10)
	require.NoError(t, err)
	assert.Equal(t, txntypes.TimeStamp(15), commitTS)
}

func TestCheckAsyncCommitStatusCommitted(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write a commit record.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutWrite([]byte("pk"), 20, &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    10,
		ShortValue: []byte("v"),
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	commitTS, err := CheckAsyncCommitStatus(reader, []byte("pk"), 10)
	require.NoError(t, err)
	assert.Equal(t, txntypes.TimeStamp(20), commitTS)
}

func TestCheckAsyncCommitStatusRolledBack(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write a rollback record.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutWrite([]byte("pk"), 10, &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   10,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	commitTS, err := CheckAsyncCommitStatus(reader, []byte("pk"), 10)
	require.NoError(t, err)
	assert.Equal(t, txntypes.TimeStamp(0), commitTS)
}

func TestCheckAsyncCommitStatusNotAsyncCommit(t *testing.T) {
	eng, cleanup := setupAsyncTestEngine(t)
	defer cleanup()

	// Write a normal (non-async commit) lock.
	txn1 := mvcc.NewMvccTxn(10)
	txn1.PutLock([]byte("pk"), &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  10,
		TTL:      5000,
	})
	applyTxnModifies(t, eng, txn1)

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	commitTS, err := CheckAsyncCommitStatus(reader, []byte("pk"), 10)
	require.NoError(t, err)
	assert.Equal(t, txntypes.TimeStamp(0), commitTS)
}
