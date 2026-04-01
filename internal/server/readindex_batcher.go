package server

import (
	"fmt"
	"sync"
	"time"
)

const (
	defaultBatchWindow  = 1 * time.Millisecond
	defaultMaxBatchSize = 64
	batcherIdleTimeout  = 10 * time.Second
)

// readIndexWaiter represents a single caller waiting for a ReadIndex result.
type readIndexWaiter struct {
	doneCh chan error
}

// ReadIndexDispatcher is the interface for dispatching a single ReadIndex
// and returning the result. This allows mocking in unit tests.
type ReadIndexDispatcher interface {
	// Dispatch sends a ReadIndex to the Raft peer and blocks until completion.
	// Returns nil on success, or an error.
	Dispatch(regionID uint64) error
}

// ReadIndexBatcher accumulates ReadIndex requests for a single region
// and dispatches them as a single Raft ReadIndex operation.
type ReadIndexBatcher struct {
	regionID   uint64
	dispatcher ReadIndexDispatcher

	mu       sync.Mutex
	pending  []readIndexWaiter
	timer    *time.Timer
	stopped  bool
	lastUsed time.Time
}

// NewReadIndexBatcher creates a batcher for the given region.
func NewReadIndexBatcher(regionID uint64, dispatcher ReadIndexDispatcher) *ReadIndexBatcher {
	return &ReadIndexBatcher{
		regionID:   regionID,
		dispatcher: dispatcher,
		lastUsed:   time.Now(),
	}
}

// Wait adds a waiter and blocks until the batched ReadIndex completes or timeout.
func (b *ReadIndexBatcher) Wait(timeout time.Duration) error {
	doneCh := make(chan error, 1)

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return fmt.Errorf("readindex batcher stopped")
	}
	b.pending = append(b.pending, readIndexWaiter{doneCh: doneCh})
	b.lastUsed = time.Now()

	// Start batch timer if not already running.
	if b.timer == nil {
		b.timer = time.AfterFunc(defaultBatchWindow, b.flush)
	}

	// Flush immediately if batch is full.
	if len(b.pending) >= defaultMaxBatchSize {
		b.flushLocked()
	}
	b.mu.Unlock()

	select {
	case err := <-doneCh:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("raftstore: read index timeout for region %d", b.regionID)
	}
}

// Stop drains all pending waiters with a shutdown error.
func (b *ReadIndexBatcher) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stopped = true
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	for _, w := range b.pending {
		w.doneCh <- fmt.Errorf("readindex batcher stopped")
	}
	b.pending = nil
}

// IsIdle returns true if the batcher has had no activity for the idle timeout.
func (b *ReadIndexBatcher) IsIdle() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending) == 0 && time.Since(b.lastUsed) > batcherIdleTimeout
}

// flush is called by the batch window timer.
func (b *ReadIndexBatcher) flush() {
	b.mu.Lock()
	b.flushLocked()
	b.mu.Unlock()
}

// flushLocked dispatches a single ReadIndex for all pending waiters.
// Must be called with b.mu held.
func (b *ReadIndexBatcher) flushLocked() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}

	if len(b.pending) == 0 || b.stopped {
		return
	}

	// Take the pending batch.
	batch := b.pending
	b.pending = nil

	// Dispatch ReadIndex asynchronously to avoid holding the lock
	// during the Raft round-trip.
	go b.dispatchAndFanout(batch)
}

// dispatchAndFanout sends a single ReadIndex via the dispatcher and fans out the result.
func (b *ReadIndexBatcher) dispatchAndFanout(batch []readIndexWaiter) {
	err := b.dispatcher.Dispatch(b.regionID)
	for _, w := range batch {
		w.doneCh <- err
	}
}
