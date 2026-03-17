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

func newTestEngine(t *testing.T) traits.KvEngine {
	t.Helper()
	dir := t.TempDir()
	e, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

func applyModifies(t *testing.T, engine traits.KvEngine, modifies []mvcc.Modify) {
	t.Helper()
	wb := engine.NewWriteBatch()
	for _, m := range modifies {
		switch m.Type {
		case mvcc.ModifyTypePut:
			require.NoError(t, wb.Put(m.CF, m.Key, m.Value))
		case mvcc.ModifyTypeDelete:
			require.NoError(t, wb.Delete(m.CF, m.Key))
		}
	}
	require.NoError(t, wb.Commit())
}

// ============================================================================
// Prewrite
// ============================================================================

func TestPrewriteBasic(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	txn := mvcc.NewMvccTxn(startTS)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	props := PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}
	mutation := Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: []byte("hello"),
	}

	err := Prewrite(txn, reader, props, mutation)
	require.NoError(t, err)
	assert.Equal(t, 1, txn.ModifyCount()) // Lock only (short value inlined).
}

func TestPrewriteWriteConflict(t *testing.T) {
	engine := newTestEngine(t)

	// Write a committed value at ts=20.
	commitTxn := mvcc.NewMvccTxn(txntypes.ComposeTS(15, 0))
	write := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(15, 0),
		ShortValue: []byte("existing"),
	}
	commitTxn.PutWrite([]byte("key1"), txntypes.ComposeTS(20, 0), write)
	applyModifies(t, engine, commitTxn.Modifies)

	// Try to prewrite at ts=10 (before the commit).
	startTS := txntypes.ComposeTS(10, 0)
	txn := mvcc.NewMvccTxn(startTS)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	props := PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}
	mutation := Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: []byte("new-value"),
	}

	err := Prewrite(txn, reader, props, mutation)
	assert.ErrorIs(t, err, ErrWriteConflict)
}

func TestPrewriteKeyLocked(t *testing.T) {
	engine := newTestEngine(t)

	// Write a lock from another transaction.
	otherTxn := mvcc.NewMvccTxn(txntypes.ComposeTS(5, 0))
	otherLock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  txntypes.ComposeTS(5, 0),
		TTL:      3000,
	}
	otherTxn.PutLock([]byte("key1"), otherLock)
	applyModifies(t, engine, otherTxn.Modifies)

	// Try to prewrite on the same key.
	startTS := txntypes.ComposeTS(10, 0)
	txn := mvcc.NewMvccTxn(startTS)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	props := PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}
	mutation := Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: []byte("value"),
	}

	err := Prewrite(txn, reader, props, mutation)
	assert.ErrorIs(t, err, ErrKeyIsLocked)
}

func TestPrewriteIdempotent(t *testing.T) {
	engine := newTestEngine(t)

	// Write own lock first.
	startTS := txntypes.ComposeTS(10, 0)
	ownTxn := mvcc.NewMvccTxn(startTS)
	ownLock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("key1"),
		StartTS:  startTS,
		TTL:      3000,
	}
	ownTxn.PutLock([]byte("key1"), ownLock)
	applyModifies(t, engine, ownTxn.Modifies)

	// Re-prewrite should be idempotent.
	txn := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	props := PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}
	mutation := Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: []byte("hello"),
	}

	err := Prewrite(txn, reader, props, mutation)
	require.NoError(t, err)
}

func TestPrewriteLargeValue(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	txn := mvcc.NewMvccTxn(startTS)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	largeValue := make([]byte, 300) // > ShortValueMaxLen

	props := PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}
	mutation := Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: largeValue,
	}

	err := Prewrite(txn, reader, props, mutation)
	require.NoError(t, err)
	// Should have 2 modifies: lock (no short value) + value in CF_DEFAULT.
	assert.Equal(t, 2, txn.ModifyCount())
}

// ============================================================================
// Commit
// ============================================================================

func TestCommitBasic(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	// Prewrite.
	preTxn := mvcc.NewMvccTxn(startTS)
	lock := &txntypes.Lock{
		LockType:   txntypes.LockTypePut,
		Primary:    []byte("key1"),
		StartTS:    startTS,
		TTL:        3000,
		ShortValue: []byte("hello"),
	}
	preTxn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, preTxn.Modifies)

	// Commit.
	commitTxn := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Commit(commitTxn, reader, []byte("key1"), startTS, commitTS)
	require.NoError(t, err)

	// Should have 2 modifies: delete lock + put write.
	assert.Equal(t, 2, commitTxn.ModifyCount())
}

func TestCommitLockNotFound(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	commitTxn := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Commit(commitTxn, reader, []byte("key1"), startTS, commitTS)
	assert.ErrorIs(t, err, ErrTxnLockNotFound)
}

func TestCommitWrongStartTS(t *testing.T) {
	engine := newTestEngine(t)

	// Write a lock with different startTS.
	preTxn := mvcc.NewMvccTxn(txntypes.ComposeTS(5, 0))
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("key1"),
		StartTS:  txntypes.ComposeTS(5, 0),
		TTL:      3000,
	}
	preTxn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, preTxn.Modifies)

	// Try to commit with different startTS.
	commitTxn := mvcc.NewMvccTxn(txntypes.ComposeTS(10, 0))
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Commit(commitTxn, reader, []byte("key1"), txntypes.ComposeTS(10, 0), txntypes.ComposeTS(15, 0))
	assert.ErrorIs(t, err, ErrTxnLockNotFound)
}

// ============================================================================
// Rollback
// ============================================================================

func TestRollbackBasic(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)

	// Prewrite.
	preTxn := mvcc.NewMvccTxn(startTS)
	lock := &txntypes.Lock{
		LockType:   txntypes.LockTypePut,
		Primary:    []byte("key1"),
		StartTS:    startTS,
		TTL:        3000,
		ShortValue: []byte("hello"),
	}
	preTxn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, preTxn.Modifies)

	// Rollback.
	rbTxn := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Rollback(rbTxn, reader, []byte("key1"), startTS)
	require.NoError(t, err)

	// Should have 2 modifies: delete lock + put rollback record.
	assert.Equal(t, 2, rbTxn.ModifyCount())
}

func TestRollbackIdempotent(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)

	// Write a rollback record.
	rbTxn := mvcc.NewMvccTxn(startTS)
	rollback := &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   startTS,
	}
	rbTxn.PutWrite([]byte("key1"), startTS, rollback)
	applyModifies(t, engine, rbTxn.Modifies)

	// Re-rollback should be idempotent.
	rbTxn2 := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Rollback(rbTxn2, reader, []byte("key1"), startTS)
	require.NoError(t, err)
}

func TestRollbackAlreadyCommitted(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	// Write a commit record.
	cTxn := mvcc.NewMvccTxn(startTS)
	write := &txntypes.Write{
		WriteType: txntypes.WriteTypePut,
		StartTS:   startTS,
	}
	cTxn.PutWrite([]byte("key1"), commitTS, write)
	applyModifies(t, engine, cTxn.Modifies)

	// Rollback should fail.
	rbTxn := mvcc.NewMvccTxn(startTS)
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	err := Rollback(rbTxn, reader, []byte("key1"), startTS)
	assert.ErrorIs(t, err, ErrAlreadyCommitted)
}

// ============================================================================
// CheckTxnStatus
// ============================================================================

func TestCheckTxnStatusLocked(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)

	preTxn := mvcc.NewMvccTxn(startTS)
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  startTS,
		TTL:      3000,
	}
	preTxn.PutLock([]byte("pk"), lock)
	applyModifies(t, engine, preTxn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	status, err := CheckTxnStatus(reader, []byte("pk"), startTS)
	require.NoError(t, err)
	assert.True(t, status.IsLocked)
	assert.NotNil(t, status.Lock)
}

func TestCheckTxnStatusCommitted(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	cTxn := mvcc.NewMvccTxn(startTS)
	write := &txntypes.Write{
		WriteType: txntypes.WriteTypePut,
		StartTS:   startTS,
	}
	cTxn.PutWrite([]byte("pk"), commitTS, write)
	applyModifies(t, engine, cTxn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	status, err := CheckTxnStatus(reader, []byte("pk"), startTS)
	require.NoError(t, err)
	assert.False(t, status.IsLocked)
	assert.Equal(t, commitTS, status.CommitTS)
}

func TestCheckTxnStatusRolledBack(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)

	rbTxn := mvcc.NewMvccTxn(startTS)
	write := &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   startTS,
	}
	rbTxn.PutWrite([]byte("pk"), startTS, write)
	applyModifies(t, engine, rbTxn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := mvcc.NewMvccReader(snap)

	status, err := CheckTxnStatus(reader, []byte("pk"), startTS)
	require.NoError(t, err)
	assert.True(t, status.IsRolledBack)
}

// ============================================================================
// End-to-end: Prewrite -> Commit -> Read
// ============================================================================

func TestPrewriteCommitRead(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	// Step 1: Prewrite.
	preTxn := mvcc.NewMvccTxn(startTS)
	snap1 := engine.NewSnapshot()
	reader1 := mvcc.NewMvccReader(snap1)

	err := Prewrite(preTxn, reader1, PrewriteProps{
		StartTS: startTS,
		Primary: []byte("key1"),
		LockTTL: 3000,
	}, Mutation{
		Op:    MutationOpPut,
		Key:   []byte("key1"),
		Value: []byte("value1"),
	})
	require.NoError(t, err)
	snap1.Close()
	applyModifies(t, engine, preTxn.Modifies)

	// Step 2: Commit.
	commitTxnObj := mvcc.NewMvccTxn(startTS)
	snap2 := engine.NewSnapshot()
	reader2 := mvcc.NewMvccReader(snap2)

	err = Commit(commitTxnObj, reader2, []byte("key1"), startTS, commitTS)
	require.NoError(t, err)
	snap2.Close()
	applyModifies(t, engine, commitTxnObj.Modifies)

	// Step 3: Read.
	snap3 := engine.NewSnapshot()
	defer snap3.Close()
	reader3 := mvcc.NewMvccReader(snap3)
	pg := mvcc.NewPointGetter(reader3, txntypes.ComposeTS(20, 0), mvcc.IsolationLevelSI)

	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
}
