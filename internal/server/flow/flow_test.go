package flow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- ReadPool Tests ---

func TestNewReadPool(t *testing.T) {
	rp := NewReadPool(4)
	defer rp.Stop()
	assert.Equal(t, 4, rp.Workers())
}

func TestNewReadPoolDefaultWorkers(t *testing.T) {
	rp := NewReadPool(0)
	defer rp.Stop()
	assert.Equal(t, 4, rp.Workers())
}

func TestReadPoolSubmit(t *testing.T) {
	rp := NewReadPool(2)
	defer rp.Stop()

	var executed atomic.Bool
	done := make(chan struct{})

	rp.Submit(func() {
		executed.Store(true)
		close(done)
	})

	select {
	case <-done:
		assert.True(t, executed.Load())
	case <-time.After(2 * time.Second):
		t.Fatal("task did not complete in time")
	}
}

func TestReadPoolEWMAUpdate(t *testing.T) {
	rp := NewReadPool(2)
	defer rp.Stop()

	done := make(chan struct{})
	rp.Submit(func() {
		time.Sleep(10 * time.Millisecond)
		close(done)
	})

	<-done
	time.Sleep(10 * time.Millisecond) // Give EWMA time to update.

	ewma := rp.EWMASlice()
	assert.Greater(t, ewma, int64(0), "EWMA should be positive after a task")
}

func TestReadPoolCheckBusyNoThreshold(t *testing.T) {
	rp := NewReadPool(2)
	defer rp.Stop()

	err := rp.CheckBusy(context.Background(), 0)
	assert.NoError(t, err)
}

func TestReadPoolCheckBusyNotBusy(t *testing.T) {
	rp := NewReadPool(4)
	defer rp.Stop()

	err := rp.CheckBusy(context.Background(), 1000) // 1000ms threshold
	assert.NoError(t, err)
}

func TestReadPoolCheckBusyIsBusy(t *testing.T) {
	rp := NewReadPool(1)
	defer rp.Stop()

	// Artificially set high EWMA and queue depth.
	rp.ewmaSlice.Store(int64(100 * time.Millisecond))
	rp.queueDepth.Store(20)

	err := rp.CheckBusy(context.Background(), 10) // 10ms threshold
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrServerIsBusy))

	var busyErr *ServerIsBusyError
	assert.True(t, errors.As(err, &busyErr))
	assert.Greater(t, busyErr.EstimatedWaitMs, uint32(0))
}

func TestReadPoolQueueDepth(t *testing.T) {
	rp := NewReadPool(2)
	defer rp.Stop()
	assert.Equal(t, int64(0), rp.QueueDepth())
}

func TestReadPoolConcurrentSubmit(t *testing.T) {
	rp := NewReadPool(4)
	defer rp.Stop()

	var count atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		rp.Submit(func() {
			count.Add(1)
			wg.Done()
		})
	}

	wg.Wait()
	assert.Equal(t, int64(100), count.Load())
}

// --- FlowController Tests ---

func TestNewFlowController(t *testing.T) {
	fc := NewFlowController(100, 200)
	assert.NotNil(t, fc)
	assert.Equal(t, 0.0, fc.DiscardRatio())
}

func TestFlowControllerShouldDropNoDiscard(t *testing.T) {
	fc := NewFlowController(100, 200)
	// With 0 discard ratio, should never drop.
	for i := 0; i < 100; i++ {
		assert.False(t, fc.ShouldDrop())
	}
}

func TestFlowControllerShouldDropAlways(t *testing.T) {
	fc := NewFlowController(100, 200)
	fc.SetDiscardRatio(1.0) // Always discard

	// Should always drop (with probability 1.0).
	dropped := 0
	for i := 0; i < 100; i++ {
		if fc.ShouldDrop() {
			dropped++
		}
	}
	assert.Equal(t, 100, dropped)
}

func TestFlowControllerUpdatePendingBelowSoft(t *testing.T) {
	fc := NewFlowController(100, 200)
	fc.UpdatePendingCompactionBytes(50)
	assert.Equal(t, 0.0, fc.DiscardRatio())
}

func TestFlowControllerUpdatePendingAboveHard(t *testing.T) {
	fc := NewFlowController(100, 200)
	fc.UpdatePendingCompactionBytes(300)
	assert.Equal(t, 1.0, fc.DiscardRatio())
}

func TestFlowControllerUpdatePendingMidRange(t *testing.T) {
	fc := NewFlowController(100, 200)
	fc.UpdatePendingCompactionBytes(150) // 50% between soft and hard
	ratio := fc.DiscardRatio()
	assert.InDelta(t, 0.5, ratio, 0.01)
}

func TestFlowControllerSetDiscardRatioClamped(t *testing.T) {
	fc := NewFlowController(100, 200)

	fc.SetDiscardRatio(-0.5)
	assert.Equal(t, 0.0, fc.DiscardRatio())

	fc.SetDiscardRatio(1.5)
	assert.Equal(t, 1.0, fc.DiscardRatio())
}

func TestFlowControllerShouldDropProbabilistic(t *testing.T) {
	fc := NewFlowController(100, 200)
	fc.SetDiscardRatio(0.5) // 50% discard rate

	dropped := 0
	total := 10000
	for i := 0; i < total; i++ {
		if fc.ShouldDrop() {
			dropped++
		}
	}

	// Should be roughly 50% (with some variance).
	ratio := float64(dropped) / float64(total)
	assert.InDelta(t, 0.5, ratio, 0.1) // Allow 10% tolerance.
}

// --- MemoryQuota Tests ---

func TestNewMemoryQuota(t *testing.T) {
	mq := NewMemoryQuota(256 * 1024 * 1024) // 256 MB
	assert.Equal(t, int64(256*1024*1024), mq.Capacity())
	assert.Equal(t, int64(0), mq.Used())
	assert.Equal(t, int64(256*1024*1024), mq.Available())
}

func TestMemoryQuotaAcquireRelease(t *testing.T) {
	mq := NewMemoryQuota(1000)

	err := mq.Acquire(500)
	require.NoError(t, err)
	assert.Equal(t, int64(500), mq.Used())
	assert.Equal(t, int64(500), mq.Available())

	mq.Release(500)
	assert.Equal(t, int64(0), mq.Used())
	assert.Equal(t, int64(1000), mq.Available())
}

func TestMemoryQuotaExhausted(t *testing.T) {
	mq := NewMemoryQuota(100)

	err := mq.Acquire(60)
	require.NoError(t, err)

	err = mq.Acquire(60) // Would exceed capacity
	assert.ErrorIs(t, err, ErrSchedTooBusy)
}

func TestMemoryQuotaExactCapacity(t *testing.T) {
	mq := NewMemoryQuota(100)

	err := mq.Acquire(100)
	require.NoError(t, err)

	assert.Equal(t, int64(100), mq.Used())
	assert.Equal(t, int64(0), mq.Available())

	err = mq.Acquire(1) // Over capacity by 1
	assert.ErrorIs(t, err, ErrSchedTooBusy)
}

func TestMemoryQuotaConcurrent(t *testing.T) {
	mq := NewMemoryQuota(10000)

	var wg sync.WaitGroup
	successCount := atomic.Int64{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := mq.Acquire(10); err == nil {
				successCount.Add(1)
				defer mq.Release(10)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(0), mq.Used()) // All released.
	assert.Equal(t, int64(100), successCount.Load()) // All should succeed.
}

func TestMemoryQuotaConcurrentContention(t *testing.T) {
	mq := NewMemoryQuota(50) // Only room for 5 acquisitions of 10

	var wg sync.WaitGroup
	successCount := atomic.Int64{}
	failCount := atomic.Int64{}
	done := make(chan struct{})

	// Try 20 concurrent acquisitions of 10 bytes each.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-done
			if err := mq.Acquire(10); err == nil {
				successCount.Add(1)
				time.Sleep(10 * time.Millisecond)
				mq.Release(10)
			} else {
				failCount.Add(1)
			}
		}()
	}

	close(done) // Release all goroutines simultaneously.
	wg.Wait()

	// Some should succeed, some should fail.
	assert.Greater(t, successCount.Load(), int64(0))
	assert.Greater(t, failCount.Load(), int64(0))
}

// --- Error Tests ---

func TestServerIsBusyErrorIs(t *testing.T) {
	err := &ServerIsBusyError{Reason: "too busy", EstimatedWaitMs: 100}
	assert.True(t, errors.Is(err, ErrServerIsBusy))
}

func TestServerIsBusyErrorMessage(t *testing.T) {
	err := &ServerIsBusyError{Reason: "too busy", EstimatedWaitMs: 100}
	assert.Equal(t, "too busy", err.Error())
}
