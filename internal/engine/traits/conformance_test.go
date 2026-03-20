package traits_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/pkg/cfnames"
)

// engineFactory creates a fresh KvEngine backed by a temp directory.
func engineFactory(t *testing.T) traits.KvEngine {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	return eng
}

// --- WriteBatch Atomicity ---

func TestWriteBatchAtomicCommit(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k1"), []byte("v1")))
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k2"), []byte("v2")))
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k3"), []byte("v3")))
	require.NoError(t, wb.Commit())

	for _, k := range []string{"k1", "k2", "k3"} {
		v, err := eng.Get(cfnames.CFDefault, []byte(k))
		require.NoError(t, err)
		assert.Equal(t, "v"+k[1:], string(v))
	}
}

func TestWriteBatchClearDiscardsAll(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("k1"), []byte("v1")))
	assert.Equal(t, 1, wb.Count())
	wb.Clear()
	assert.Equal(t, 0, wb.Count())
	require.NoError(t, wb.Commit())

	_, err := eng.Get(cfnames.CFDefault, []byte("k1"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestWriteBatchCountAndDataSize(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	assert.Equal(t, 0, wb.Count())
	assert.Equal(t, 0, wb.DataSize())

	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("key"), []byte("val")))
	assert.Equal(t, 1, wb.Count())
	assert.Greater(t, wb.DataSize(), 0)
}

// --- WriteBatch SavePoint / Rollback ---

func TestWriteBatchSavePointRollback(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("keep"), []byte("yes")))
	wb.SetSavePoint()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("discard"), []byte("no")))
	require.NoError(t, wb.RollbackToSavePoint())
	require.NoError(t, wb.Commit())

	v, err := eng.Get(cfnames.CFDefault, []byte("keep"))
	require.NoError(t, err)
	assert.Equal(t, "yes", string(v))

	_, err = eng.Get(cfnames.CFDefault, []byte("discard"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestWriteBatchNestedSavePoints(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("a"), []byte("1")))
	wb.SetSavePoint()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("b"), []byte("2")))
	wb.SetSavePoint()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("c"), []byte("3")))
	require.NoError(t, wb.RollbackToSavePoint()) // rolls back "c"
	require.NoError(t, wb.Commit())

	_, err := eng.Get(cfnames.CFDefault, []byte("a"))
	require.NoError(t, err)
	_, err = eng.Get(cfnames.CFDefault, []byte("b"))
	require.NoError(t, err)
	_, err = eng.Get(cfnames.CFDefault, []byte("c"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestWriteBatchPopSavePoint(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("a"), []byte("1")))
	wb.SetSavePoint()
	require.NoError(t, wb.Put(cfnames.CFDefault, []byte("b"), []byte("2")))
	require.NoError(t, wb.PopSavePoint()) // discard save point, keep "b"
	require.NoError(t, wb.Commit())

	_, err := eng.Get(cfnames.CFDefault, []byte("b"))
	require.NoError(t, err)
}

func TestWriteBatchRollbackNoSavePoint(t *testing.T) {
	eng := engineFactory(t)
	wb := eng.NewWriteBatch()
	err := wb.RollbackToSavePoint()
	assert.Error(t, err)
}

// --- Snapshot Isolation ---

func TestSnapshotIsolation(t *testing.T) {
	eng := engineFactory(t)
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("k"), []byte("before")))

	snap := eng.NewSnapshot()
	defer snap.Close()

	// Write after snapshot
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("k"), []byte("after")))
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("new"), []byte("val")))

	// Snapshot sees old value
	v, err := snap.Get(cfnames.CFDefault, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, "before", string(v))

	// Snapshot does not see new key
	_, err = snap.Get(cfnames.CFDefault, []byte("new"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
}

func TestSnapshotDoesNotSeeDeletes(t *testing.T) {
	eng := engineFactory(t)
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("k"), []byte("v")))

	snap := eng.NewSnapshot()
	defer snap.Close()

	require.NoError(t, eng.Delete(cfnames.CFDefault, []byte("k")))

	v, err := snap.Get(cfnames.CFDefault, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, "v", string(v))
}

// --- Iterator Boundaries ---

func TestIteratorLowerUpperBound(t *testing.T) {
	eng := engineFactory(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, eng.Put(cfnames.CFDefault, []byte(k), []byte(k)))
	}

	iter := eng.NewIterator(cfnames.CFDefault, traits.IterOptions{
		LowerBound: []byte("b"),
		UpperBound: []byte("d"),
	})
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	assert.Equal(t, []string{"b", "c"}, keys)
}

func TestIteratorSeekForPrev(t *testing.T) {
	eng := engineFactory(t)
	for _, k := range []string{"a", "c", "e"} {
		require.NoError(t, eng.Put(cfnames.CFDefault, []byte(k), []byte(k)))
	}

	iter := eng.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	// SeekForPrev("d") should land on "c" (last key <= "d")
	iter.SeekForPrev([]byte("d"))
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	// SeekForPrev("c") should land on "c" exactly
	iter.SeekForPrev([]byte("c"))
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	// SeekForPrev on something before first key
	iter.SeekForPrev([]byte("0"))
	assert.False(t, iter.Valid())
}

func TestIteratorSeekToLast(t *testing.T) {
	eng := engineFactory(t)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, eng.Put(cfnames.CFDefault, []byte(k), []byte(k)))
	}

	iter := eng.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	iter.SeekToLast()
	require.True(t, iter.Valid())
	assert.Equal(t, "c", string(iter.Key()))

	iter.Prev()
	require.True(t, iter.Valid())
	assert.Equal(t, "b", string(iter.Key()))
}

func TestIteratorEmptyRange(t *testing.T) {
	eng := engineFactory(t)
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("a"), []byte("v")))

	iter := eng.NewIterator(cfnames.CFDefault, traits.IterOptions{
		LowerBound: []byte("m"),
		UpperBound: []byte("z"),
	})
	defer iter.Close()

	iter.SeekToFirst()
	assert.False(t, iter.Valid())
}

// --- Cross-CF Isolation ---

func TestCrossCFIsolation(t *testing.T) {
	eng := engineFactory(t)
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("k"), []byte("default")))
	require.NoError(t, eng.Put(cfnames.CFWrite, []byte("k"), []byte("write")))

	v, err := eng.Get(cfnames.CFDefault, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, "default", string(v))

	v, err = eng.Get(cfnames.CFWrite, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, "write", string(v))

	// Delete from one CF doesn't affect the other
	require.NoError(t, eng.Delete(cfnames.CFDefault, []byte("k")))
	_, err = eng.Get(cfnames.CFDefault, []byte("k"))
	assert.ErrorIs(t, err, traits.ErrNotFound)

	v, err = eng.Get(cfnames.CFWrite, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, "write", string(v))
}

// --- DeleteRange ---

func TestDeleteRange(t *testing.T) {
	eng := engineFactory(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, eng.Put(cfnames.CFDefault, []byte(k), []byte(k)))
	}

	require.NoError(t, eng.DeleteRange(cfnames.CFDefault, []byte("b"), []byte("d")))

	// "a" and "d", "e" should survive; "b", "c" deleted
	_, err := eng.Get(cfnames.CFDefault, []byte("a"))
	require.NoError(t, err)
	_, err = eng.Get(cfnames.CFDefault, []byte("b"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
	_, err = eng.Get(cfnames.CFDefault, []byte("c"))
	assert.ErrorIs(t, err, traits.ErrNotFound)
	_, err = eng.Get(cfnames.CFDefault, []byte("d"))
	require.NoError(t, err)
	_, err = eng.Get(cfnames.CFDefault, []byte("e"))
	require.NoError(t, err)
}

// --- Concurrent Access ---

func TestConcurrentReadWrite(t *testing.T) {
	eng := engineFactory(t)
	const goroutines = 8
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			prefix := []byte{byte(id)}
			for i := 0; i < opsPerGoroutine; i++ {
				key := append(prefix, byte(i))
				val := []byte{byte(id), byte(i)}
				require.NoError(t, eng.Put(cfnames.CFDefault, key, val))
			}
			for i := 0; i < opsPerGoroutine; i++ {
				key := append(prefix, byte(i))
				v, err := eng.Get(cfnames.CFDefault, key)
				require.NoError(t, err)
				assert.True(t, bytes.Equal(v, []byte{byte(id), byte(i)}))
			}
		}(g)
	}
	wg.Wait()
}

func TestConcurrentWriteBatch(t *testing.T) {
	eng := engineFactory(t)
	const goroutines = 4
	const keysPerBatch = 20

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			wb := eng.NewWriteBatch()
			for i := 0; i < keysPerBatch; i++ {
				key := []byte{byte(id), byte(i)}
				require.NoError(t, wb.Put(cfnames.CFDefault, key, key))
			}
			require.NoError(t, wb.Commit())
		}(g)
	}
	wg.Wait()

	// Verify all keys present
	for g := 0; g < goroutines; g++ {
		for i := 0; i < keysPerBatch; i++ {
			key := []byte{byte(g), byte(i)}
			v, err := eng.Get(cfnames.CFDefault, key)
			require.NoError(t, err)
			assert.Equal(t, key, v)
		}
	}
}

// --- Snapshot Iterator ---

func TestSnapshotIterator(t *testing.T) {
	eng := engineFactory(t)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, eng.Put(cfnames.CFDefault, []byte(k), []byte(k)))
	}

	snap := eng.NewSnapshot()
	defer snap.Close()

	// Add more data after snapshot
	require.NoError(t, eng.Put(cfnames.CFDefault, []byte("d"), []byte("d")))

	iter := snap.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	defer iter.Close()

	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	assert.Equal(t, []string{"a", "b", "c"}, keys)
}
