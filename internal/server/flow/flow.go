// Package flow implements flow control and backpressure mechanisms for gookv.
// It provides read pool busy threshold (EWMA), write flow controller,
// and scheduler memory quota enforcement.
package flow

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrServerIsBusy is returned when the server is too busy to handle requests.
	ErrServerIsBusy = errors.New("server is busy")
	// ErrSchedTooBusy is returned when the scheduler memory quota is exhausted.
	ErrSchedTooBusy = errors.New("scheduler too busy")
)

// ServerIsBusyError provides details about why the server rejected a request.
type ServerIsBusyError struct {
	Reason          string
	EstimatedWaitMs uint32
}

func (e *ServerIsBusyError) Error() string {
	return e.Reason
}

func (e *ServerIsBusyError) Is(target error) bool {
	return target == ErrServerIsBusy
}

// ReadPool estimates wait time using EWMA of task execution time.
// It provides a busy threshold check for incoming requests.
type ReadPool struct {
	workers    int
	taskCh     chan func()
	ewmaSlice  atomic.Int64 // EWMA of task execution time (nanoseconds)
	queueDepth atomic.Int64 // current tasks queued
	alpha      float64      // EWMA smoothing factor (0-1)
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewReadPool creates a read pool with the given number of workers.
func NewReadPool(workers int) *ReadPool {
	if workers <= 0 {
		workers = 4
	}
	rp := &ReadPool{
		workers: workers,
		taskCh:  make(chan func(), workers*16),
		alpha:   0.3, // EWMA smoothing factor
		stopCh:  make(chan struct{}),
	}

	rp.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer rp.wg.Done()
			rp.worker()
		}()
	}

	return rp
}

// Submit submits a task to the read pool and tracks timing.
func (rp *ReadPool) Submit(task func()) {
	rp.queueDepth.Add(1)
	rp.taskCh <- func() {
		start := time.Now()
		task()
		elapsed := time.Since(start).Nanoseconds()
		rp.updateEWMA(elapsed)
		rp.queueDepth.Add(-1)
	}
}

// CheckBusy rejects the request if estimated wait exceeds the client's threshold.
func (rp *ReadPool) CheckBusy(ctx context.Context, thresholdMs uint32) error {
	if thresholdMs == 0 {
		return nil // No threshold set.
	}

	ewma := rp.ewmaSlice.Load()
	depth := rp.queueDepth.Load()
	estimatedWait := ewma * depth / int64(rp.workers)

	if estimatedWait > int64(thresholdMs)*int64(time.Millisecond) {
		return &ServerIsBusyError{
			Reason:          "estimated wait time exceeds threshold",
			EstimatedWaitMs: uint32(estimatedWait / int64(time.Millisecond)),
		}
	}
	return nil
}

// QueueDepth returns the current number of queued tasks.
func (rp *ReadPool) QueueDepth() int64 {
	return rp.queueDepth.Load()
}

// EWMASlice returns the current EWMA of task execution time in nanoseconds.
func (rp *ReadPool) EWMASlice() int64 {
	return rp.ewmaSlice.Load()
}

// Workers returns the number of worker goroutines.
func (rp *ReadPool) Workers() int {
	return rp.workers
}

// Stop stops the read pool. It signals workers to stop and waits for them
// to drain all pending tasks before returning.
func (rp *ReadPool) Stop() {
	close(rp.stopCh)
	rp.wg.Wait()
}

func (rp *ReadPool) worker() {
	for {
		select {
		case task := <-rp.taskCh:
			task()
		case <-rp.stopCh:
			// Drain remaining tasks before exiting.
			for {
				select {
				case task := <-rp.taskCh:
					task()
				default:
					return
				}
			}
		}
	}
}

func (rp *ReadPool) updateEWMA(elapsedNs int64) {
	for {
		old := rp.ewmaSlice.Load()
		var newVal int64
		if old == 0 {
			newVal = elapsedNs
		} else {
			// EWMA: new = alpha * sample + (1 - alpha) * old
			newVal = int64(rp.alpha*float64(elapsedNs) + (1-rp.alpha)*float64(old))
		}
		if rp.ewmaSlice.CompareAndSwap(old, newVal) {
			return
		}
	}
}

// FlowController monitors write pressure and applies backpressure.
// It implements probabilistic request dropping based on compaction pressure
// and write rate limiting.
type FlowController struct {
	discardRatio atomic.Uint32 // Fixed-point 0-1000 representing 0.0-1.0
	softLimit    int64         // Pending compaction bytes soft limit
	hardLimit    int64         // Pending compaction bytes hard limit
}

// NewFlowController creates a FlowController with the given compaction byte limits.
func NewFlowController(softLimit, hardLimit int64) *FlowController {
	return &FlowController{
		softLimit: softLimit,
		hardLimit: hardLimit,
	}
}

// ShouldDrop probabilistically drops requests based on compaction pressure.
func (fc *FlowController) ShouldDrop() bool {
	ratio := float64(fc.discardRatio.Load()) / 1000.0
	if ratio > 0 && rand.Float64() < ratio {
		return true
	}
	return false
}

// UpdatePendingCompactionBytes recalculates the discard ratio based on
// the current pending compaction bytes.
func (fc *FlowController) UpdatePendingCompactionBytes(pending int64) {
	if pending < fc.softLimit {
		fc.discardRatio.Store(0)
		return
	}
	if pending >= fc.hardLimit {
		fc.discardRatio.Store(1000)
		return
	}

	// Linear interpolation between soft and hard limits.
	ratio := float64(pending-fc.softLimit) / float64(fc.hardLimit-fc.softLimit)
	fc.discardRatio.Store(uint32(ratio * 1000))
}

// DiscardRatio returns the current discard ratio as a float (0.0-1.0).
func (fc *FlowController) DiscardRatio() float64 {
	return float64(fc.discardRatio.Load()) / 1000.0
}

// SetDiscardRatio sets the discard ratio directly (0.0-1.0).
func (fc *FlowController) SetDiscardRatio(ratio float64) {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	fc.discardRatio.Store(uint32(ratio * 1000))
}

// MemoryQuota enforces a memory limit on the scheduler.
// It uses atomic CAS for lock-free allocation and release.
type MemoryQuota struct {
	capacity int64 // Maximum memory in bytes
	used     atomic.Int64
}

// NewMemoryQuota creates a MemoryQuota with the given capacity in bytes.
func NewMemoryQuota(capacity int64) *MemoryQuota {
	return &MemoryQuota{capacity: capacity}
}

// Acquire allocates memory for a task. Returns ErrSchedTooBusy if quota exhausted.
func (mq *MemoryQuota) Acquire(size int64) error {
	for {
		old := mq.used.Load()
		if old+size > mq.capacity {
			return ErrSchedTooBusy
		}
		if mq.used.CompareAndSwap(old, old+size) {
			return nil
		}
	}
}

// Release frees memory when a task completes.
func (mq *MemoryQuota) Release(size int64) {
	mq.used.Add(-size)
}

// Used returns the current memory usage in bytes.
func (mq *MemoryQuota) Used() int64 {
	return mq.used.Load()
}

// Capacity returns the maximum memory capacity.
func (mq *MemoryQuota) Capacity() int64 {
	return mq.capacity
}

// Available returns the remaining available memory.
func (mq *MemoryQuota) Available() int64 {
	return mq.capacity - mq.used.Load()
}
