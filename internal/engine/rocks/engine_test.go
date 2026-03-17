package rocks

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestEngine creates a temporary Engine for testing.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	dir := t.TempDir()
	e, err := Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

// ============================================================================
// Basic CRUD operations
// ============================================================================

func TestPutGet(t *testing.T) {
	e := newTestEngine(t)

	for _, cf := range cfnames.AllCFs {
		t.Run(cf, func(t *testing.T) {
			key := []byte("hello")
			val := []byte("world")

			err := e.Put(cf, key, val)
			require.NoError(t, err)

			got, err := e.Get(cf, key)
			require.NoError(t, err)
			assert.Equal(t, val, got)
		})
	}
}

func TestGetNotFound(t *testing.T) {
	e := newTestEngine(t)

	_, err := e.Get(cfnames.CFDefault, []byte("nonexistent"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestGetInvalidCF(t *testing.T) {
	e := newTestEngine(t)

	_, err := e.Get("invalid_cf", []byte("key"))
	assert.ErrorIs(t, err, traits.ErrCFNotFound)
}

func TestPutInvalidCF(t *testing.T) {
	e := newTestEngine(t)

	err := e.Put("invalid_cf", []byte("key"), []byte("val"))
	assert.ErrorIs(t, err, traits.ErrCFNotFound)
}

func TestDelete(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("to-delete")
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("value")))

	// Key should exist before delete.
	_, err := e.Get(cfnames.CFDefault, key)
	require.NoError(t, err)

	// Delete.
	require.NoError(t, e.Delete(cfnames.CFDefault, key))

	// Key should be gone.
	_, err = e.Get(cfnames.CFDefault, key)
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestDeleteRange(t *testing.T) {
	e := newTestEngine(t)

	// Insert keys a, b, c, d, e.
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v")))
	}

	// Delete range [b, d) — should delete b and c.
	require.NoError(t, e.DeleteRange(cfnames.CFDefault, []byte("b"), []byte("d")))

	// a should still exist.
	_, err := e.Get(cfnames.CFDefault, []byte("a"))
	assert.NoError(t, err)

	// b and c should be gone.
	_, err = e.Get(cfnames.CFDefault, []byte("b"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
	_, err = e.Get(cfnames.CFDefault, []byte("c"))
	assert.ErrorIs(t, err, traits.ErrNotFound)

	// d and e should still exist.
	_, err = e.Get(cfnames.CFDefault, []byte("d"))
	assert.NoError(t, err)
	_, err = e.Get(cfnames.CFDefault, []byte("e"))
	assert.NoError(t, err)
}

// ============================================================================
// Column Family isolation
// ============================================================================

func TestColumnFamilyIsolation(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("same-key")

	// Put same key in different CFs with different values.
	for i, cf := range cfnames.AllCFs {
		val := []byte(fmt.Sprintf("value-%d", i))
		require.NoError(t, e.Put(cf, key, val))
	}

	// Each CF should return its own value.
	for i, cf := range cfnames.AllCFs {
		expected := []byte(fmt.Sprintf("value-%d", i))
		got, err := e.Get(cf, key)
		require.NoError(t, err)
		assert.Equal(t, expected, got, "CF %s should have its own value", cf)
	}
}

func TestDeleteOnlyClearsTargetCF(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("shared-key")

	// Put in default and lock CFs.
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("default-val")))
	require.NoError(t, e.Put(cfnames.CFLock, key, []byte("lock-val")))

	// Delete from default only.
	require.NoError(t, e.Delete(cfnames.CFDefault, key))

	// Default should be gone.
	_, err := e.Get(cfnames.CFDefault, key)
	assert.ErrorIs(t, err, traits.ErrNotFound)

	// Lock should still exist.
	got, err := e.Get(cfnames.CFLock, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("lock-val"), got)
}

// ============================================================================
// Empty keys and values
// ============================================================================

func TestEmptyValue(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("empty-val")
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte{}))

	got, err := e.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, []byte{}, got)
}

func TestEmptyKey(t *testing.T) {
	e := newTestEngine(t)

	require.NoError(t, e.Put(cfnames.CFDefault, []byte{}, []byte("value")))

	got, err := e.Get(cfnames.CFDefault, []byte{})
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), got)
}

// ============================================================================
// Overwrite
// ============================================================================

func TestOverwrite(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("overwrite-key")
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("v1")))
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("v2")))

	got, err := e.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)
}

// ============================================================================
// Large values
// ============================================================================

func TestLargeValue(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("large-key")
	val := make([]byte, 1<<20) // 1MB
	for i := range val {
		val[i] = byte(i % 256)
	}

	require.NoError(t, e.Put(cfnames.CFDefault, key, val))

	got, err := e.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, val, got)
}

// ============================================================================
// Snapshot
// ============================================================================

func TestSnapshotIsolation(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("snap-key")
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("before")))

	// Take snapshot.
	snap := e.NewSnapshot()
	defer snap.Close()

	// Write after snapshot.
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("after")))

	// Snapshot should see old value.
	got, err := snap.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("before"), got)

	// Engine should see new value.
	got, err = e.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("after"), got)
}

func TestSnapshotNotFound(t *testing.T) {
	e := newTestEngine(t)

	snap := e.NewSnapshot()
	defer snap.Close()

	_, err := snap.Get(cfnames.CFDefault, []byte("missing"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestSnapshotDoesNotSeeDeletes(t *testing.T) {
	e := newTestEngine(t)

	key := []byte("snap-del-key")
	require.NoError(t, e.Put(cfnames.CFDefault, key, []byte("exists")))

	snap := e.NewSnapshot()
	defer snap.Close()

	// Delete after snapshot.
	require.NoError(t, e.Delete(cfnames.CFDefault, key))

	// Snapshot should still see it.
	got, err := snap.Get(cfnames.CFDefault, key)
	require.NoError(t, err)
	assert.Equal(t, []byte("exists"), got)

	// Engine should not.
	_, err = e.Get(cfnames.CFDefault, key)
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

// ============================================================================
// WriteBatch
// ============================================================================

func TestWriteBatchPutAndCommit(t *testing.T) {
	e := newTestEngine(t)

	wb := e.NewWriteBatch()

	// Add multiple puts.
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k1"), []byte("v1")))
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k2"), []byte("v2")))
	require.NoError(t, wb.Put(cfnames.CFLock, []byte("k3"), []byte("v3")))

	assert.Equal(t, 3, wb.Count())

	// Before commit, keys should not be visible.
	_, err := e.Get(cfnames.CFDefault, []byte("k1"))
	assert.ErrorIs(t, err, traits.ErrNotFound)

	// Commit.
	require.NoError(t, wb.Commit())

	// After commit, all keys should be visible.
	got, err := e.Get(cfnames.CFDefault, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), got)

	got, err = e.Get(cfnames.CFDefault, []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)

	got, err = e.Get(cfnames.CFLock, []byte("k3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), got)
}

func TestWriteBatchDelete(t *testing.T) {
	e := newTestEngine(t)

	// Pre-populate.
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("del1"), []byte("v1")))
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("del2"), []byte("v2")))

	wb := e.NewWriteBatch()
	require.NoError(t, wb.Delete(cfnames.CFDefault, []byte("del1")))
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("del3"), []byte("v3")))
	require.NoError(t, wb.Commit())

	_, err := e.Get(cfnames.CFDefault, []byte("del1"))
	assert.ErrorIs(t, err, traits.ErrNotFound)

	got, err := e.Get(cfnames.CFDefault, []byte("del2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), got)

	got, err = e.Get(cfnames.CFDefault, []byte("del3"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), got)
}

func TestWriteBatchClear(t *testing.T) {
	e := newTestEngine(t)

	wb := e.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k1"), []byte("v1")))
	assert.Equal(t, 1, wb.Count())

	wb.Clear()
	assert.Equal(t, 0, wb.Count())
	assert.Equal(t, 0, wb.DataSize())
}

func TestWriteBatchDeleteRange(t *testing.T) {
	e := newTestEngine(t)

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v")))
	}

	wb := e.NewWriteBatch()
	require.NoError(t, wb.DeleteRange(cfnames.CFDefault, []byte("b"), []byte("d")))
	require.NoError(t, wb.Commit())

	_, err := e.Get(cfnames.CFDefault, []byte("a"))
	assert.NoError(t, err)
	_, err = e.Get(cfnames.CFDefault, []byte("b"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
	_, err = e.Get(cfnames.CFDefault, []byte("c"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
	_, err = e.Get(cfnames.CFDefault, []byte("d"))
	assert.NoError(t, err)
}

func TestWriteBatchMultiCF(t *testing.T) {
	e := newTestEngine(t)

	wb := e.NewWriteBatch()
	for i, cf := range cfnames.AllCFs {
		require.NoError(t, wb.Put(cf, []byte("key"), []byte(fmt.Sprintf("val-%d", i))))
	}
	require.NoError(t, wb.Commit())

	for i, cf := range cfnames.AllCFs {
		got, err := e.Get(cf, []byte("key"))
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("val-%d", i)), got)
	}
}

// ============================================================================
// Iterator
// ============================================================================

func TestIteratorForward(t *testing.T) {
	e := newTestEngine(t)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v-"+k)))
	}

	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	var got []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		got = append(got, string(iter.Key()))
	}
	assert.NoError(t, iter.Error())
	assert.Equal(t, keys, got)
}

func TestIteratorReverse(t *testing.T) {
	e := newTestEngine(t)

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v-"+k)))
	}

	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	var got []string
	for iter.SeekToLast(); iter.Valid(); iter.Prev() {
		got = append(got, string(iter.Key()))
	}
	assert.NoError(t, iter.Error())
	assert.Equal(t, []string{"e", "d", "c", "b", "a"}, got)
}

func TestIteratorSeek(t *testing.T) {
	e := newTestEngine(t)

	for _, k := range []string{"a", "c", "e", "g"} {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v")))
	}

	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	// Seek to "c" — should find "c".
	iter.Seek([]byte("c"))
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	// Seek to "d" — should find "e" (first key >= "d").
	iter.Seek([]byte("d"))
	require.True(t, iter.Valid())
	assert.Equal(t, "e", string(iter.Key()))

	// Seek past end.
	iter.Seek([]byte("z"))
	assert.False(t, iter.Valid())
}

func TestIteratorSeekForPrev(t *testing.T) {
	e := newTestEngine(t)

	for _, k := range []string{"a", "c", "e", "g"} {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v")))
	}

	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	// SeekForPrev "c" — should find "c" (last key <= "c").
	iter.SeekForPrev([]byte("c"))
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	// SeekForPrev "d" — should find "c" (last key <= "d").
	iter.SeekForPrev([]byte("d"))
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	// SeekForPrev before first key.
	iter.SeekForPrev([]byte("0"))
	assert.False(t, iter.Valid())
}

func TestIteratorBounds(t *testing.T) {
	e := newTestEngine(t)

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, e.Put(cfnames.CFDefault, []byte(k), []byte("v")))
	}

	// Iterate with bounds [b, d).
	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	defer iter.Close()

	var got []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		got = append(got, string(iter.Key()))
	}
	assert.Equal(t, []string{"b", "c"}, got)
}

func TestIteratorCFIsolation(t *testing.T) {
	e := newTestEngine(t)

	// Put keys in different CFs.
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("d-key"), []byte("d-val")))
	require.NoError(t, e.Put(cfnames.CFLock, []byte("l-key"), []byte("l-val")))
	require.NoError(t, e.Put(cfnames.CFWrite, []byte("w-key"), []byte("w-val")))

	// Iterator on CFDefault should only see d-key.
	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	assert.Equal(t, []string{"d-key"}, keys)
}

func TestIteratorEmpty(t *testing.T) {
	e := newTestEngine(t)

	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	iter.SeekToFirst()
	assert.False(t, iter.Valid())
}

func TestIteratorInvalidCF(t *testing.T) {
	e := newTestEngine(t)

	iter := e.NewIterator("bogus", traits.IterOptions{})
	defer iter.Close()

	iter.SeekToFirst()
	assert.False(t, iter.Valid())
	assert.Error(t, iter.Error())
}

// ============================================================================
// Snapshot Iterator
// ============================================================================

func TestSnapshotIterator(t *testing.T) {
	e := newTestEngine(t)

	require.NoError(t, e.Put(cfnames.CFDefault, []byte("a"), []byte("1")))
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("b"), []byte("2")))

	snap := e.NewSnapshot()
	defer snap.Close()

	// Add more data after snapshot.
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("c"), []byte("3")))

	// Snapshot iterator should only see a, b.
	iter := snap.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	assert.Equal(t, []string{"a", "b"}, keys)
}

// ============================================================================
// GetMsg / PutMsg
// ============================================================================

// testMsg implements Marshal/Unmarshal for testing.
type testMsg struct {
	data []byte
}

func (m *testMsg) Marshal() ([]byte, error) {
	return m.data, nil
}

func (m *testMsg) Unmarshal(b []byte) error {
	m.data = make([]byte, len(b))
	copy(m.data, b)
	return nil
}

func TestPutMsgGetMsg(t *testing.T) {
	e := newTestEngine(t)

	msg := &testMsg{data: []byte("proto-like-data")}
	require.NoError(t, e.PutMsg(cfnames.CFDefault, []byte("msg-key"), msg))

	got := &testMsg{}
	require.NoError(t, e.GetMsg(cfnames.CFDefault, []byte("msg-key"), got))
	assert.Equal(t, msg.data, got.data)
}

// ============================================================================
// SyncWAL
// ============================================================================

func TestSyncWAL(t *testing.T) {
	e := newTestEngine(t)

	require.NoError(t, e.Put(cfnames.CFDefault, []byte("sync-key"), []byte("sync-val")))
	require.NoError(t, e.SyncWAL())

	got, err := e.Get(cfnames.CFDefault, []byte("sync-key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("sync-val"), got)
}

// ============================================================================
// GetProperty
// ============================================================================

func TestGetProperty(t *testing.T) {
	e := newTestEngine(t)

	prop, err := e.GetProperty(cfnames.CFDefault, "rocksdb.stats")
	require.NoError(t, err)
	assert.NotEmpty(t, prop)
}

// ============================================================================
// Open/Close
// ============================================================================

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test-db")

	// Open.
	e, err := Open(path)
	require.NoError(t, err)

	// Write some data.
	require.NoError(t, e.Put(cfnames.CFDefault, []byte("persist"), []byte("data")))
	require.NoError(t, e.Close())

	// Reopen.
	e2, err := Open(path)
	require.NoError(t, err)
	defer e2.Close()

	// Data should persist.
	got, err := e2.Get(cfnames.CFDefault, []byte("persist"))
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), got)
}

func TestOpenNonexistentPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep", "nested", "db")

	// Pebble should create intermediate directories.
	e, err := Open(path)
	if err != nil {
		// If it doesn't, ensure the path exists.
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		e, err = Open(path)
	}
	require.NoError(t, err)
	defer e.Close()
}

// ============================================================================
// Bulk operations
// ============================================================================

func TestBulkInsertAndScan(t *testing.T) {
	e := newTestEngine(t)

	// Insert 1000 keys.
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		require.NoError(t, e.Put(cfnames.CFDefault, key, val))
	}

	// Scan all and verify count.
	iter := e.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		count++
	}
	assert.Equal(t, 1000, count)
}

// ============================================================================
// Key prefix helper tests
// ============================================================================

func TestPrefixKeyStripPrefix(t *testing.T) {
	key := []byte("test-key")
	prefixed := prefixKey(0x01, key)

	assert.Equal(t, byte(0x01), prefixed[0])
	assert.Equal(t, key, prefixed[1:])

	stripped := stripPrefix(prefixed)
	assert.Equal(t, key, stripped)
}

func TestStripPrefixEmpty(t *testing.T) {
	assert.Nil(t, stripPrefix(nil))
	assert.Nil(t, stripPrefix([]byte{}))
	assert.Nil(t, stripPrefix([]byte{0x01}))
}

func TestCfPrefixMap(t *testing.T) {
	// Verify all CFs have unique prefixes.
	seen := make(map[byte]string)
	for cf, prefix := range cfPrefixMap {
		if prev, ok := seen[prefix]; ok {
			t.Fatalf("CF %q and %q share prefix %02x", cf, prev, prefix)
		}
		seen[prefix] = cf
	}

	// All AllCFs should be in the map.
	for _, cf := range cfnames.AllCFs {
		_, err := cfPrefix(cf)
		assert.NoError(t, err, "CF %q should have a prefix", cf)
	}
}
