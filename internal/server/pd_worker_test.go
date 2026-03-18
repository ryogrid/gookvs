package server

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookvs/pkg/pdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestPDWorker(t *testing.T) (*PDWorker, *pdclient.MockClient) {
	t.Helper()
	mock := pdclient.NewMockClient(1)

	cfg := PDWorkerConfig{
		StoreID:                1,
		PDClient:               mock,
		Coordinator:            nil, // nil coordinator = standalone test
		StoreHeartbeatInterval: 50 * time.Millisecond,
		TaskChannelCapacity:    16,
	}

	w := NewPDWorker(cfg)
	return w, mock
}

func TestPDWorkerStoreHeartbeat(t *testing.T) {
	w, mock := newTestPDWorker(t)
	w.Run()
	defer w.Stop()

	// Wait for at least one store heartbeat.
	time.Sleep(150 * time.Millisecond)

	// Verify store heartbeat was received by mock.
	stats := mock.GetStoreStats(1)
	require.NotNil(t, stats, "expected at least one store heartbeat")
	assert.Equal(t, uint64(1), stats.StoreId)
}

func TestPDWorkerRegionHeartbeat(t *testing.T) {
	w, mock := newTestPDWorker(t)
	w.Run()
	defer w.Stop()

	data := &RegionHeartbeatData{
		Term: 5,
		Region: &metapb.Region{
			Id: 1,
			RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		},
		Peer:         &metapb.Peer{Id: 1, StoreId: 1},
		WrittenBytes: 1024,
		WrittenKeys:  10,
	}

	w.ScheduleTask(PDTask{
		Type: PDTaskRegionHeartbeat,
		Data: data,
	})

	// Wait for task to be processed.
	time.Sleep(100 * time.Millisecond)

	region, _, err := mock.GetRegionByID(context.Background(), 1)
	require.NoError(t, err)
	require.NotNil(t, region, "expected region from heartbeat")
	assert.Equal(t, uint64(1), region.GetId())
}

func TestPDWorkerBackpressure(t *testing.T) {
	w, _ := newTestPDWorker(t)
	// Don't start the worker -- tasks should be dropped when channel is full.

	for i := 0; i < 100; i++ {
		w.ScheduleTask(PDTask{
			Type: PDTaskStoreHeartbeat,
		})
	}

	// Channel capacity is 16, so tasks beyond 16 should be dropped silently.
	// The test should not hang.
	assert.True(t, true, "no deadlock from backpressure")
}

func TestPDWorkerGracefulShutdown(t *testing.T) {
	w, _ := newTestPDWorker(t)
	w.Run()

	done := make(chan struct{})
	go func() {
		w.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Graceful shutdown succeeded.
	case <-time.After(5 * time.Second):
		t.Fatal("PDWorker.Stop() did not return within timeout")
	}
}

func TestPDWorkerReportBatchSplit(t *testing.T) {
	w, mock := newTestPDWorker(t)
	w.Run()
	defer w.Stop()

	regions := []*metapb.Region{
		{Id: 10, StartKey: nil, EndKey: []byte("m")},
		{Id: 11, StartKey: []byte("m"), EndKey: nil},
	}

	w.ScheduleTask(PDTask{
		Type: PDTaskReportBatchSplit,
		Data: regions,
	})

	time.Sleep(100 * time.Millisecond)

	// Verify regions were reported.
	r1, _, _ := mock.GetRegionByID(context.Background(), 10)
	assert.NotNil(t, r1)
	r2, _, _ := mock.GetRegionByID(context.Background(), 11)
	assert.NotNil(t, r2)
}
