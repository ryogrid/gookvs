package gc

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
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

// writeVersion writes a Put or Delete to CF_WRITE (and CF_DEFAULT for large values).
func writeVersion(t *testing.T, engine traits.KvEngine, key []byte, startTS, commitTS txntypes.TimeStamp, wt txntypes.WriteType, value []byte) {
	t.Helper()
	w := txntypes.Write{
		WriteType: wt,
		StartTS:   startTS,
	}
	if wt == txntypes.WriteTypePut && len(value) <= 64 {
		w.ShortValue = value
	}
	writeData := w.Marshal()
	writeKey := mvcc.EncodeKey(key, commitTS)
	require.NoError(t, engine.Put(cfnames.CFWrite, writeKey, writeData))

	if wt == txntypes.WriteTypePut && len(value) > 64 {
		defaultKey := mvcc.EncodeKey(key, startTS)
		require.NoError(t, engine.Put(cfnames.CFDefault, defaultKey, value))
	}
}

func countWriteVersions(t *testing.T, engine traits.KvEngine, key []byte) int {
	t.Helper()
	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close() // Close reader also closes snapshot.

	count := 0
	ts := txntypes.TSMax
	for {
		w, commitTS, err := reader.SeekWrite(key, ts)
		require.NoError(t, err)
		if w == nil {
			break
		}
		count++
		ts = commitTS - 1
	}
	return count
}

func TestGC_BasicPutVersions(t *testing.T) {
	engine := newTestEngine(t)
	key := []byte("key1")

	// Write versions: Put@10, Put@20, Put@30.
	writeVersion(t, engine, key, 5, 10, txntypes.WriteTypePut, []byte("v1"))
	writeVersion(t, engine, key, 15, 20, txntypes.WriteTypePut, []byte("v2"))
	writeVersion(t, engine, key, 25, 30, txntypes.WriteTypePut, []byte("v3"))

	assert.Equal(t, 3, countWriteVersions(t, engine, key))

	// GC at safe_point=15: should keep Put@10 (latest at or below 15), remove nothing below it.
	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	txn := mvcc.NewMvccTxn(0)

	info, err := GC(txn, reader, key, 15)
	require.NoError(t, err)
	assert.Equal(t, 3, info.FoundVersions)
	assert.Equal(t, 0, info.DeletedVersions) // Put@10 is kept, nothing older.

	reader.Close()

	// GC at safe_point=25: keep Put@20, delete Put@10.
	snap = engine.NewSnapshot()
	reader = mvcc.NewMvccReader(snap)
	txn = mvcc.NewMvccTxn(0)

	info, err = GC(txn, reader, key, 25)
	require.NoError(t, err)
	assert.Equal(t, 1, info.DeletedVersions) // Put@10 deleted.

	reader.Close()

	// Apply the deletions.
	applyModifies(t, engine, txn)

	// Should have 2 versions left.
	assert.Equal(t, 2, countWriteVersions(t, engine, key))
}

func TestGC_LockAndRollbackRemoval(t *testing.T) {
	engine := newTestEngine(t)
	key := []byte("key2")

	// Write versions: Put@10, Lock@20, Rollback@25, Put@30.
	writeVersion(t, engine, key, 5, 10, txntypes.WriteTypePut, []byte("v1"))
	writeVersion(t, engine, key, 15, 20, txntypes.WriteTypeLock, nil)
	writeVersion(t, engine, key, 22, 25, txntypes.WriteTypeRollback, nil)
	writeVersion(t, engine, key, 27, 30, txntypes.WriteTypePut, []byte("v2"))

	assert.Equal(t, 4, countWriteVersions(t, engine, key))

	// GC at safe_point=28: Lock@20 and Rollback@25 should be removed, Put@10 kept as latest.
	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	txn := mvcc.NewMvccTxn(0)

	info, err := GC(txn, reader, key, 28)
	require.NoError(t, err)
	// Lock@20 and Rollback@25 deleted, Put@10 kept (latest Put below safe point).
	assert.Equal(t, 2, info.DeletedVersions)

	reader.Close()
	applyModifies(t, engine, txn)

	assert.Equal(t, 2, countWriteVersions(t, engine, key))
}

func TestGC_DeleteVersion(t *testing.T) {
	engine := newTestEngine(t)
	key := []byte("key3")

	// Write versions: Put@10, Delete@20, Put@30.
	writeVersion(t, engine, key, 5, 10, txntypes.WriteTypePut, []byte("v1"))
	writeVersion(t, engine, key, 15, 20, txntypes.WriteTypeDelete, nil)
	writeVersion(t, engine, key, 25, 30, txntypes.WriteTypePut, []byte("v2"))

	// GC at safe_point=25: Delete@20 kept (latest data-changing below safe point), Put@10 removed.
	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	txn := mvcc.NewMvccTxn(0)

	info, err := GC(txn, reader, key, 25)
	require.NoError(t, err)
	assert.Equal(t, 1, info.DeletedVersions) // Put@10 deleted.

	reader.Close()
	applyModifies(t, engine, txn)

	assert.Equal(t, 2, countWriteVersions(t, engine, key))
}

func TestGC_LargeValue(t *testing.T) {
	engine := newTestEngine(t)
	key := []byte("key4")

	largeVal := make([]byte, 100) // > 64 bytes, stored in CF_DEFAULT
	for i := range largeVal {
		largeVal[i] = byte(i)
	}

	writeVersion(t, engine, key, 5, 10, txntypes.WriteTypePut, largeVal)
	writeVersion(t, engine, key, 15, 20, txntypes.WriteTypePut, []byte("small"))

	// Verify CF_DEFAULT entry exists.
	defaultKey := mvcc.EncodeKey(key, 5)
	_, err := engine.Get(cfnames.CFDefault, defaultKey)
	require.NoError(t, err)

	// GC at safe_point=15: keep Put@10, nothing to delete.
	// GC at safe_point=25: keep Put@20, delete Put@10 + CF_DEFAULT.
	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	txn := mvcc.NewMvccTxn(0)

	info, err := GC(txn, reader, key, 25)
	require.NoError(t, err)
	assert.Equal(t, 1, info.DeletedVersions)

	reader.Close()
	applyModifies(t, engine, txn)

	// CF_DEFAULT entry should be deleted too.
	_, err = engine.Get(cfnames.CFDefault, defaultKey)
	assert.Equal(t, traits.ErrNotFound, err)
}

func TestGC_NoVersions(t *testing.T) {
	engine := newTestEngine(t)

	snap := engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	txn := mvcc.NewMvccTxn(0)

	info, err := GC(txn, reader, []byte("nonexistent"), 100)
	require.NoError(t, err)
	assert.Equal(t, 0, info.FoundVersions)
	assert.Equal(t, 0, info.DeletedVersions)
	assert.True(t, info.IsCompleted)

	reader.Close()
}

func TestGCWorker_Basic(t *testing.T) {
	engine := newTestEngine(t)

	// Write multiple keys with multiple versions.
	for i := byte(1); i <= 3; i++ {
		key := []byte{0x01, i}
		writeVersion(t, engine, key, txntypes.TimeStamp(i*10-5), txntypes.TimeStamp(i*10), txntypes.WriteTypePut, []byte{i, 0x01})
		writeVersion(t, engine, key, txntypes.TimeStamp(i*10+5), txntypes.TimeStamp(i*10+10), txntypes.WriteTypePut, []byte{i, 0x02})
	}

	worker := NewGCWorker(engine, DefaultGCConfig())
	worker.Start()
	defer worker.Stop()

	done := make(chan error, 1)
	err := worker.Schedule(GCTask{
		SafePoint: 25,
		Callback: func(e error) {
			done <- e
		},
	})
	require.NoError(t, err)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("GC worker timed out")
	}

	stats := worker.Stats()
	assert.True(t, stats.KeysScanned > 0, "should have scanned keys")
}

func TestGCWorker_QueueFull(t *testing.T) {
	engine := newTestEngine(t)
	config := DefaultGCConfig()
	worker := &GCWorker{
		engine: engine,
		taskCh: make(chan GCTask, 1), // Very small queue.
		config: config,
		stopCh: make(chan struct{}),
	}

	// Fill the queue.
	worker.taskCh <- GCTask{SafePoint: 10}

	// Next schedule should fail.
	err := worker.Schedule(GCTask{SafePoint: 20})
	assert.Equal(t, ErrGCTaskQueueFull, err)
}

func TestMockSafePointProvider(t *testing.T) {
	provider := &MockSafePointProvider{}
	provider.SetSafePoint(100)

	ts, err := provider.GetGCSafePoint(nil)
	require.NoError(t, err)
	assert.Equal(t, txntypes.TimeStamp(100), ts)
}

func applyModifies(t *testing.T, engine traits.KvEngine, txn *mvcc.MvccTxn) {
	t.Helper()
	if len(txn.Modifies) == 0 {
		return
	}
	wb := engine.NewWriteBatch()
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
