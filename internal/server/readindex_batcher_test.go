package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDispatcher implements ReadIndexDispatcher for testing.
type mockDispatcher struct {
	mu         sync.Mutex
	callCount  atomic.Int32
	delay      time.Duration // simulate Raft round-trip
	err        error         // error to return
}

func (d *mockDispatcher) Dispatch(regionID uint64) error {
	d.callCount.Add(1)
	if d.delay > 0 {
		time.Sleep(d.delay)
	}
	d.mu.Lock()
	err := d.err
	d.mu.Unlock()
	return err
}

func (d *mockDispatcher) setError(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
}

func TestBatcher_SingleRequest(t *testing.T) {
	d := &mockDispatcher{delay: 5 * time.Millisecond}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	err := b.Wait(2 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, int32(1), d.callCount.Load())
}

func TestBatcher_BatchCoalescing(t *testing.T) {
	d := &mockDispatcher{delay: 10 * time.Millisecond}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	var wg sync.WaitGroup
	var errCount atomic.Int32
	n := 10

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Wait(2 * time.Second); err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(0), errCount.Load(), "all waiters should succeed")
	// With 1ms batch window, 10 concurrent requests should be batched into 1-2 dispatches.
	assert.LessOrEqual(t, d.callCount.Load(), int32(2), "should batch most requests into 1-2 dispatches")
}

func TestBatcher_ErrorFanout(t *testing.T) {
	d := &mockDispatcher{
		delay: 5 * time.Millisecond,
		err:   fmt.Errorf("not leader"),
	}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	var wg sync.WaitGroup
	var errCount atomic.Int32
	n := 5

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := b.Wait(2 * time.Second); err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(int32(n)), errCount.Load(), "all waiters should receive the error")
}

func TestBatcher_Timeout(t *testing.T) {
	d := &mockDispatcher{delay: 500 * time.Millisecond} // slow dispatch
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	err := b.Wait(50 * time.Millisecond) // short timeout
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestBatcher_MaxBatchSize(t *testing.T) {
	d := &mockDispatcher{delay: 5 * time.Millisecond}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	var wg sync.WaitGroup
	n := defaultMaxBatchSize + 10 // exceed max batch size

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Wait(2 * time.Second)
		}()
	}
	wg.Wait()

	// With 74 requests and max batch 64, at least 2 dispatches should occur.
	assert.GreaterOrEqual(t, d.callCount.Load(), int32(2), "should dispatch at least 2 batches")
}

func TestBatcher_SequentialBatches(t *testing.T) {
	d := &mockDispatcher{delay: 5 * time.Millisecond}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	// First batch.
	err := b.Wait(2 * time.Second)
	require.NoError(t, err)

	// Second batch (after first completes).
	err = b.Wait(2 * time.Second)
	require.NoError(t, err)

	assert.GreaterOrEqual(t, d.callCount.Load(), int32(2), "sequential waits should dispatch separately")
}

func TestBatcher_StopDrainsWaiters(t *testing.T) {
	// Stop() should prevent new Wait() calls from succeeding.
	d := &mockDispatcher{delay: 5 * time.Millisecond}
	b := NewReadIndexBatcher(1, d)

	// First Wait succeeds normally.
	err := b.Wait(2 * time.Second)
	require.NoError(t, err)

	b.Stop()

	// Wait after Stop returns error immediately.
	err = b.Wait(2 * time.Second)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stopped")
}

func TestBatcher_IdleEviction(t *testing.T) {
	d := &mockDispatcher{}
	b := NewReadIndexBatcher(1, d)
	defer b.Stop()

	// Not idle immediately.
	assert.False(t, b.IsIdle())

	// Simulate passage of time by setting lastUsed in the past.
	b.mu.Lock()
	b.lastUsed = time.Now().Add(-batcherIdleTimeout - time.Second)
	b.mu.Unlock()

	assert.True(t, b.IsIdle())
}
