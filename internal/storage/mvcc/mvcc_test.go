package mvcc

import (
	"path/filepath"
	"testing"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
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

// applyModifies writes all modifications from an MvccTxn to the engine.
func applyModifies(t *testing.T, engine traits.KvEngine, modifies []Modify) {
	t.Helper()
	wb := engine.NewWriteBatch()
	for _, m := range modifies {
		switch m.Type {
		case ModifyTypePut:
			require.NoError(t, wb.Put(m.CF, m.Key, m.Value))
		case ModifyTypeDelete:
			require.NoError(t, wb.Delete(m.CF, m.Key))
		}
	}
	require.NoError(t, wb.Commit())
}

// ============================================================================
// Key Encoding
// ============================================================================

func TestEncodeDecodeKey(t *testing.T) {
	key := []byte("user-key")
	ts := txntypes.ComposeTS(100, 5)

	encoded := EncodeKey(key, ts)
	gotKey, gotTS, err := DecodeKey(encoded)
	require.NoError(t, err)
	assert.Equal(t, key, gotKey)
	assert.Equal(t, ts, gotTS)
}

func TestKeyOrdering(t *testing.T) {
	key := []byte("key")
	ts1 := txntypes.ComposeTS(100, 0)
	ts2 := txntypes.ComposeTS(200, 0)

	// Higher ts should produce SMALLER encoded key (descending order).
	enc1 := EncodeKey(key, ts1)
	enc2 := EncodeKey(key, ts2)

	assert.True(t, string(enc2) < string(enc1),
		"newer timestamp should sort before older (descending ts encoding)")
}

func TestEncodeLockKey(t *testing.T) {
	key := []byte("lock-key")
	encoded := EncodeLockKey(key)
	decoded, err := DecodeLockKey(encoded)
	require.NoError(t, err)
	assert.Equal(t, key, decoded)
}

// ============================================================================
// MvccTxn
// ============================================================================

func TestMvccTxnPutLock(t *testing.T) {
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))

	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("primary"),
		StartTS:  txntypes.ComposeTS(10, 0),
		TTL:      3000,
	}
	txn.PutLock([]byte("key1"), lock)

	assert.Equal(t, 1, txn.ModifyCount())
	assert.Equal(t, cfnames.CFLock, txn.Modifies[0].CF)
	assert.Equal(t, ModifyTypePut, txn.Modifies[0].Type)
}

func TestMvccTxnUnlockKey(t *testing.T) {
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	released := txn.UnlockKey([]byte("key1"), false)

	assert.Equal(t, 1, txn.ModifyCount())
	assert.Equal(t, cfnames.CFLock, txn.Modifies[0].CF)
	assert.Equal(t, ModifyTypeDelete, txn.Modifies[0].Type)
	assert.Equal(t, txntypes.ComposeTS(10, 0), released.StartTS)
	assert.False(t, released.IsPessimistic)
}

func TestMvccTxnPutValue(t *testing.T) {
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	txn.PutValue([]byte("key1"), txntypes.ComposeTS(10, 0), []byte("large-value"))

	assert.Equal(t, 1, txn.ModifyCount())
	assert.Equal(t, cfnames.CFDefault, txn.Modifies[0].CF)
}

func TestMvccTxnPutWrite(t *testing.T) {
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	write := &txntypes.Write{
		WriteType: txntypes.WriteTypePut,
		StartTS:   txntypes.ComposeTS(10, 0),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(15, 0), write)

	assert.Equal(t, 1, txn.ModifyCount())
	assert.Equal(t, cfnames.CFWrite, txn.Modifies[0].CF)
}

// ============================================================================
// MvccReader
// ============================================================================

func TestMvccReaderLoadLock(t *testing.T) {
	engine := newTestEngine(t)

	// Write a lock.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  txntypes.ComposeTS(10, 0),
		TTL:      3000,
	}
	txn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, txn.Modifies)

	// Read the lock.
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	gotLock, err := reader.LoadLock([]byte("key1"))
	require.NoError(t, err)
	require.NotNil(t, gotLock)
	assert.Equal(t, txntypes.LockTypePut, gotLock.LockType)
	assert.Equal(t, txntypes.ComposeTS(10, 0), gotLock.StartTS)

	// No lock for other key.
	gotLock, err = reader.LoadLock([]byte("key2"))
	require.NoError(t, err)
	assert.Nil(t, gotLock)
}

func TestMvccReaderSeekWrite(t *testing.T) {
	engine := newTestEngine(t)

	// Write a committed record.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	write := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(10, 0),
		ShortValue: []byte("hello"),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(15, 0), write)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	// Seek at ts=20 should find the write at ts=15.
	gotWrite, commitTS, err := reader.SeekWrite([]byte("key1"), txntypes.ComposeTS(20, 0))
	require.NoError(t, err)
	require.NotNil(t, gotWrite)
	assert.Equal(t, txntypes.WriteTypePut, gotWrite.WriteType)
	assert.Equal(t, txntypes.ComposeTS(15, 0), commitTS)

	// Seek at ts=10 should not find the write (committed at 15).
	gotWrite, _, err = reader.SeekWrite([]byte("key1"), txntypes.ComposeTS(10, 0))
	require.NoError(t, err)
	assert.Nil(t, gotWrite)
}

func TestMvccReaderGetWriteSkipsRollback(t *testing.T) {
	engine := newTestEngine(t)

	// Write a rollback at ts=20, then a put at ts=10.
	txn := NewMvccTxn(txntypes.ComposeTS(5, 0))

	rollback := &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   txntypes.ComposeTS(20, 0),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(20, 0), rollback)

	put := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(5, 0),
		ShortValue: []byte("value"),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(10, 0), put)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	// GetWrite at ts=25 should skip the rollback and find the put.
	gotWrite, commitTS, err := reader.GetWrite([]byte("key1"), txntypes.ComposeTS(25, 0))
	require.NoError(t, err)
	require.NotNil(t, gotWrite)
	assert.Equal(t, txntypes.WriteTypePut, gotWrite.WriteType)
	assert.Equal(t, txntypes.ComposeTS(10, 0), commitTS)
}

func TestMvccReaderGetWriteReturnsNilForDelete(t *testing.T) {
	engine := newTestEngine(t)

	txn := NewMvccTxn(txntypes.ComposeTS(5, 0))
	del := &txntypes.Write{
		WriteType: txntypes.WriteTypeDelete,
		StartTS:   txntypes.ComposeTS(5, 0),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(10, 0), del)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	gotWrite, _, err := reader.GetWrite([]byte("key1"), txntypes.ComposeTS(15, 0))
	require.NoError(t, err)
	assert.Nil(t, gotWrite)
}

func TestMvccReaderGetValue(t *testing.T) {
	engine := newTestEngine(t)

	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	txn.PutValue([]byte("key1"), txntypes.ComposeTS(10, 0), []byte("large-value-data"))
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	val, err := reader.GetValue([]byte("key1"), txntypes.ComposeTS(10, 0))
	require.NoError(t, err)
	assert.Equal(t, []byte("large-value-data"), val)

	// Non-existent.
	val, err = reader.GetValue([]byte("key1"), txntypes.ComposeTS(20, 0))
	require.NoError(t, err)
	assert.Nil(t, val)
}

// ============================================================================
// PointGetter
// ============================================================================

func TestPointGetterBasicRead(t *testing.T) {
	engine := newTestEngine(t)

	// Write a committed value with short value.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	write := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(10, 0),
		ShortValue: []byte("hello"),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(15, 0), write)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)

	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), val)
}

func TestPointGetterReadLargeValue(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	// Write large value to CF_DEFAULT and write record to CF_WRITE.
	txn := NewMvccTxn(startTS)
	largeValue := make([]byte, 300) // > ShortValueMaxLen
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	txn.PutValue([]byte("key1"), startTS, largeValue)
	write := &txntypes.Write{
		WriteType: txntypes.WriteTypePut,
		StartTS:   startTS,
		// No ShortValue — value is in CF_DEFAULT.
	}
	txn.PutWrite([]byte("key1"), commitTS, write)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)

	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, largeValue, val)
}

func TestPointGetterKeyNotFound(t *testing.T) {
	engine := newTestEngine(t)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)

	val, err := pg.Get([]byte("nonexistent"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestPointGetterBlockedByLock(t *testing.T) {
	engine := newTestEngine(t)

	// Write a lock.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  txntypes.ComposeTS(10, 0),
		TTL:      3000,
	}
	txn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)

	_, err := pg.Get([]byte("key1"))
	assert.ErrorIs(t, err, ErrKeyIsLocked)
}

func TestPointGetterBypassOwnLock(t *testing.T) {
	engine := newTestEngine(t)

	startTS := txntypes.ComposeTS(10, 0)
	commitTS := txntypes.ComposeTS(15, 0)

	// Write a lock and a committed value.
	txn := NewMvccTxn(startTS)
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  startTS,
		TTL:      3000,
	}
	txn.PutLock([]byte("key1"), lock)
	write := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(5, 0),
		ShortValue: []byte("old-value"),
	}
	txn.PutWrite([]byte("key1"), commitTS, write)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)
	pg.SetBypassLocks(map[txntypes.TimeStamp]bool{startTS: true})

	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("old-value"), val)
}

func TestPointGetterReadCommittedIgnoresLocks(t *testing.T) {
	engine := newTestEngine(t)

	// Write a lock.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	lock := &txntypes.Lock{
		LockType: txntypes.LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  txntypes.ComposeTS(10, 0),
		TTL:      3000,
	}
	txn.PutLock([]byte("key1"), lock)
	applyModifies(t, engine, txn.Modifies)

	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelRC)

	// Read Committed mode should not check locks.
	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Nil(t, val) // No committed value exists.
}

func TestPointGetterMultipleVersions(t *testing.T) {
	engine := newTestEngine(t)

	// Write version 1.
	txn1 := NewMvccTxn(txntypes.ComposeTS(10, 0))
	w1 := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(10, 0),
		ShortValue: []byte("v1"),
	}
	txn1.PutWrite([]byte("key1"), txntypes.ComposeTS(15, 0), w1)

	// Write version 2.
	w2 := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(20, 0),
		ShortValue: []byte("v2"),
	}
	txn1.PutWrite([]byte("key1"), txntypes.ComposeTS(25, 0), w2)
	applyModifies(t, engine, txn1.Modifies)

	// Read at ts=20 should see v1.
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)

	pg1 := NewPointGetter(reader, txntypes.ComposeTS(20, 0), IsolationLevelSI)
	val, err := pg1.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	// Need a new reader/snapshot for second read since iterators are consumed.
	snap2 := engine.NewSnapshot()
	defer snap2.Close()
	reader2 := NewMvccReader(snap2)

	// Read at ts=30 should see v2.
	pg2 := NewPointGetter(reader2, txntypes.ComposeTS(30, 0), IsolationLevelSI)
	val, err = pg2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestPointGetterDeletedKey(t *testing.T) {
	engine := newTestEngine(t)

	// Write a put then a delete.
	txn := NewMvccTxn(txntypes.ComposeTS(10, 0))
	w1 := &txntypes.Write{
		WriteType:  txntypes.WriteTypePut,
		StartTS:    txntypes.ComposeTS(10, 0),
		ShortValue: []byte("v1"),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(15, 0), w1)

	w2 := &txntypes.Write{
		WriteType: txntypes.WriteTypeDelete,
		StartTS:   txntypes.ComposeTS(20, 0),
	}
	txn.PutWrite([]byte("key1"), txntypes.ComposeTS(25, 0), w2)
	applyModifies(t, engine, txn.Modifies)

	// Read at ts=30 should see deletion (nil).
	snap := engine.NewSnapshot()
	defer snap.Close()
	reader := NewMvccReader(snap)
	pg := NewPointGetter(reader, txntypes.ComposeTS(30, 0), IsolationLevelSI)

	val, err := pg.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Nil(t, val)
}
