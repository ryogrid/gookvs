package latch

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicAcquireRelease(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	lock := l.GenLock(keys)

	acquired := l.Acquire(lock, 1)
	assert.True(t, acquired)

	wakeUp := l.Release(lock, 1)
	assert.Empty(t, wakeUp)
}

func TestConflictingAcquire(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("shared-key")}

	lock1 := l.GenLock(keys)
	lock2 := l.GenLock(keys)

	// First command acquires.
	assert.True(t, l.Acquire(lock1, 1))

	// Second command should be blocked.
	assert.False(t, l.Acquire(lock2, 2))

	// Release first command.
	wakeUp := l.Release(lock1, 1)
	assert.Contains(t, wakeUp, uint64(2))

	// Second command should now succeed.
	assert.True(t, l.Acquire(lock2, 2))
	l.Release(lock2, 2)
}

func TestNonOverlappingKeys(t *testing.T) {
	l := New(256)

	lock1 := l.GenLock([][]byte{[]byte("key1")})
	lock2 := l.GenLock([][]byte{[]byte("key2")})

	// Both should acquire immediately (different keys).
	assert.True(t, l.Acquire(lock1, 1))
	assert.True(t, l.Acquire(lock2, 2))

	l.Release(lock1, 1)
	l.Release(lock2, 2)
}

func TestDuplicateKeys(t *testing.T) {
	l := New(256)

	// Same key appears multiple times — should be deduplicated.
	keys := [][]byte{[]byte("key1"), []byte("key1"), []byte("key1")}
	lock := l.GenLock(keys)

	assert.Len(t, lock.RequiredHashes, 1, "duplicate keys should be deduplicated")

	assert.True(t, l.Acquire(lock, 1))
	l.Release(lock, 1)
}

func TestSortedAcquisitionOrder(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("z"), []byte("a"), []byte("m")}
	lock := l.GenLock(keys)

	// Hashes should be sorted.
	for i := 1; i < len(lock.RequiredHashes); i++ {
		assert.LessOrEqual(t, lock.RequiredHashes[i-1], lock.RequiredHashes[i],
			"hashes should be sorted")
	}
}

func TestReacquireAfterRelease(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("key1")}
	lock := l.GenLock(keys)

	assert.True(t, l.Acquire(lock, 1))
	l.Release(lock, 1)

	// Should be able to acquire again.
	lock2 := l.GenLock(keys)
	assert.True(t, l.Acquire(lock2, 2))
	l.Release(lock2, 2)
}

func TestMultipleWaiters(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("key1")}

	lock1 := l.GenLock(keys)
	lock2 := l.GenLock(keys)
	lock3 := l.GenLock(keys)

	assert.True(t, l.Acquire(lock1, 1))
	assert.False(t, l.Acquire(lock2, 2))
	assert.False(t, l.Acquire(lock3, 3))

	// Release should wake first waiter.
	wakeUp := l.Release(lock1, 1)
	assert.Contains(t, wakeUp, uint64(2))

	// Command 2 should now acquire.
	assert.True(t, l.Acquire(lock2, 2))

	// Release should wake command 3.
	wakeUp = l.Release(lock2, 2)
	assert.Contains(t, wakeUp, uint64(3))

	assert.True(t, l.Acquire(lock3, 3))
	l.Release(lock3, 3)
}

func TestConcurrentDeadlockFree(t *testing.T) {
	l := New(256)

	// Multiple goroutines acquiring latches on overlapping key sets.
	// Since hashes are sorted, this should be deadlock-free.
	const goroutines = 50
	const iterations = 100

	var counter atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				keys := [][]byte{
					[]byte("shared1"),
					[]byte("shared2"),
					[]byte("shared3"),
				}
				lock := l.GenLock(keys)
				cmdID := uint64(id*iterations + i + 1)

				// Spin until acquired.
				for !l.Acquire(lock, cmdID) {
					// Reset and retry.
					lock = l.GenLock(keys)
				}

				counter.Add(1)
				l.Release(lock, cmdID)
			}
		}(g)
	}

	wg.Wait()
	assert.Equal(t, int64(goroutines*iterations), counter.Load())
}

func TestEmptyKeys(t *testing.T) {
	l := New(256)

	lock := l.GenLock(nil)
	assert.Empty(t, lock.RequiredHashes)

	// Acquire with no keys should succeed trivially.
	assert.True(t, l.Acquire(lock, 1))
	wakeUp := l.Release(lock, 1)
	assert.Empty(t, wakeUp)
}

func TestPowerOfTwoSlots(t *testing.T) {
	// Non-power-of-2 input should be rounded up.
	l := New(100)
	assert.Equal(t, 128, l.size)

	l2 := New(256)
	assert.Equal(t, 256, l2.size)

	l3 := New(1)
	assert.Equal(t, 1, l3.size)
}

func TestGenLockIsDeterministic(t *testing.T) {
	l := New(256)

	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	lock1 := l.GenLock(keys)
	lock2 := l.GenLock(keys)

	require.Equal(t, lock1.RequiredHashes, lock2.RequiredHashes)
}
