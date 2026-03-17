package concurrency

import (
	"sync"
	"testing"

	"github.com/ryogrid/gookvs/pkg/txntypes"
	"github.com/stretchr/testify/assert"
)

func TestUpdateMaxTS(t *testing.T) {
	m := New()

	assert.Equal(t, txntypes.TSZero, m.MaxTS())

	m.UpdateMaxTS(txntypes.ComposeTS(100, 0))
	assert.Equal(t, txntypes.ComposeTS(100, 0), m.MaxTS())

	// Lower value should not update.
	m.UpdateMaxTS(txntypes.ComposeTS(50, 0))
	assert.Equal(t, txntypes.ComposeTS(100, 0), m.MaxTS())

	// Higher value should update.
	m.UpdateMaxTS(txntypes.ComposeTS(200, 0))
	assert.Equal(t, txntypes.ComposeTS(200, 0), m.MaxTS())
}

func TestUpdateMaxTSConcurrent(t *testing.T) {
	m := New()

	var wg sync.WaitGroup
	const goroutines = 100

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(ts int) {
			defer wg.Done()
			m.UpdateMaxTS(txntypes.ComposeTS(int64(ts), 0))
		}(i)
	}
	wg.Wait()

	// MaxTS should be the maximum of all updates.
	assert.Equal(t, txntypes.ComposeTS(int64(goroutines-1), 0), m.MaxTS())
}

func TestLockKey(t *testing.T) {
	m := New()

	guard := m.LockKey([]byte("key1"), txntypes.ComposeTS(10, 0))
	assert.Equal(t, 1, m.LockCount())

	handle, ok := m.IsKeyLocked([]byte("key1"))
	assert.True(t, ok)
	assert.Equal(t, txntypes.ComposeTS(10, 0), handle.StartTS)

	// Non-locked key.
	_, ok = m.IsKeyLocked([]byte("key2"))
	assert.False(t, ok)

	// Release.
	guard.Release()
	assert.Equal(t, 0, m.LockCount())

	_, ok = m.IsKeyLocked([]byte("key1"))
	assert.False(t, ok)
}

func TestMultipleLocks(t *testing.T) {
	m := New()

	g1 := m.LockKey([]byte("key1"), txntypes.ComposeTS(10, 0))
	g2 := m.LockKey([]byte("key2"), txntypes.ComposeTS(20, 0))
	g3 := m.LockKey([]byte("key3"), txntypes.ComposeTS(5, 0))

	assert.Equal(t, 3, m.LockCount())

	g2.Release()
	assert.Equal(t, 2, m.LockCount())

	g1.Release()
	g3.Release()
	assert.Equal(t, 0, m.LockCount())
}

func TestGlobalMinLock(t *testing.T) {
	m := New()

	// No locks.
	assert.Nil(t, m.GlobalMinLock())

	g1 := m.LockKey([]byte("key1"), txntypes.ComposeTS(10, 0))
	g2 := m.LockKey([]byte("key2"), txntypes.ComposeTS(5, 0))
	g3 := m.LockKey([]byte("key3"), txntypes.ComposeTS(20, 0))

	minTS := m.GlobalMinLock()
	assert.NotNil(t, minTS)
	assert.Equal(t, txntypes.ComposeTS(5, 0), *minTS)

	// Release the min lock.
	g2.Release()
	minTS = m.GlobalMinLock()
	assert.NotNil(t, minTS)
	assert.Equal(t, txntypes.ComposeTS(10, 0), *minTS)

	g1.Release()
	g3.Release()
}

func TestDoubleRelease(t *testing.T) {
	m := New()

	guard := m.LockKey([]byte("key1"), txntypes.ComposeTS(10, 0))
	guard.Release()
	guard.Release() // Should not panic.

	assert.Equal(t, 0, m.LockCount())
}

func TestConcurrentLockUnlock(t *testing.T) {
	m := New()

	var wg sync.WaitGroup
	const goroutines = 100

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			key := []byte{byte(id)}
			guard := m.LockKey(key, txntypes.ComposeTS(int64(id), 0))
			guard.Release()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 0, m.LockCount())
}
