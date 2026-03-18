package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookvs/pkg/pdclient"
)

// PDTaskType identifies the type of PD task.
type PDTaskType int

const (
	// PDTaskStoreHeartbeat sends store-level stats to PD.
	PDTaskStoreHeartbeat PDTaskType = iota
	// PDTaskRegionHeartbeat sends region-level info to PD.
	PDTaskRegionHeartbeat
	// PDTaskReportBatchSplit reports a completed batch split.
	PDTaskReportBatchSplit
)

// PDTask is a unit of work for the PDWorker.
type PDTask struct {
	Type PDTaskType
	Data interface{}
}

// RegionHeartbeatData carries region heartbeat information.
type RegionHeartbeatData struct {
	Term            uint64
	Region          *metapb.Region
	Peer            *metapb.Peer
	DownPeers       []*pdpb.PeerStats
	PendingPeers    []*metapb.Peer
	WrittenBytes    uint64
	WrittenKeys     uint64
	ApproximateSize uint64
	ApproximateKeys uint64
}

// PDWorkerConfig holds configuration for the PDWorker.
type PDWorkerConfig struct {
	StoreID                uint64
	PDClient               pdclient.Client
	Coordinator            *StoreCoordinator
	StoreHeartbeatInterval time.Duration
	TaskChannelCapacity    int
}

// DefaultPDWorkerConfig returns sensible defaults.
func DefaultPDWorkerConfig() PDWorkerConfig {
	return PDWorkerConfig{
		StoreHeartbeatInterval: 10 * time.Second,
		TaskChannelCapacity:    256,
	}
}

// PDWorker manages PD communication: store heartbeats, region heartbeats,
// and PD scheduling command processing.
type PDWorker struct {
	storeID     uint64
	pdClient    pdclient.Client
	coordinator *StoreCoordinator

	taskCh    chan PDTask
	startTime time.Time

	storeHeartbeatInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewPDWorker creates a new PDWorker.
func NewPDWorker(cfg PDWorkerConfig) *PDWorker {
	ctx, cancel := context.WithCancel(context.Background())
	capacity := cfg.TaskChannelCapacity
	if capacity == 0 {
		capacity = 256
	}
	interval := cfg.StoreHeartbeatInterval
	if interval == 0 {
		interval = 10 * time.Second
	}

	return &PDWorker{
		storeID:                cfg.StoreID,
		pdClient:               cfg.PDClient,
		coordinator:            cfg.Coordinator,
		taskCh:                 make(chan PDTask, capacity),
		startTime:              time.Now(),
		storeHeartbeatInterval: interval,
		ctx:                    ctx,
		cancel:                 cancel,
	}
}

// Run starts the PDWorker goroutines: store heartbeat loop and task processor.
func (w *PDWorker) Run() {
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		w.storeHeartbeatLoop()
	}()
	go func() {
		defer w.wg.Done()
		w.taskProcessorLoop()
	}()
}

// Stop gracefully stops the PDWorker.
func (w *PDWorker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// ScheduleTask adds a task to the PDWorker's queue.
// Non-blocking: drops the task if the channel is full.
func (w *PDWorker) ScheduleTask(task PDTask) {
	select {
	case w.taskCh <- task:
	default:
		slog.Warn("pd worker task channel full, dropping task", "type", task.Type)
	}
}

// TaskCh returns the task channel for external producers (e.g., Peer).
func (w *PDWorker) TaskCh() chan<- PDTask {
	return w.taskCh
}

// storeHeartbeatLoop periodically sends store heartbeats to PD.
func (w *PDWorker) storeHeartbeatLoop() {
	ticker := time.NewTicker(w.storeHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.sendStoreHeartbeat()
		}
	}
}

// sendStoreHeartbeat sends a single store heartbeat to PD.
func (w *PDWorker) sendStoreHeartbeat() {
	var regionCount uint32
	var isBusy bool
	if w.coordinator != nil {
		regionCount = uint32(w.coordinator.RegionCount())
		isBusy = w.coordinator.IsBusy()
	}

	stats := &pdpb.StoreStats{
		StoreId:     w.storeID,
		RegionCount: regionCount,
		StartTime:   uint32(w.startTime.Unix()),
		IsBusy:      isBusy,
	}

	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	if err := w.pdClient.StoreHeartbeat(ctx, stats); err != nil {
		slog.Warn("store heartbeat failed", "err", err)
	}
}

// taskProcessorLoop processes incoming PD tasks.
func (w *PDWorker) taskProcessorLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.taskCh:
			w.processTask(task)
		}
	}
}

func (w *PDWorker) processTask(task PDTask) {
	switch task.Type {
	case PDTaskRegionHeartbeat:
		if data, ok := task.Data.(*RegionHeartbeatData); ok {
			w.sendRegionHeartbeat(data)
		}
	case PDTaskReportBatchSplit:
		if regions, ok := task.Data.([]*metapb.Region); ok {
			w.reportBatchSplit(regions)
		}
	case PDTaskStoreHeartbeat:
		w.sendStoreHeartbeat()
	}
}

func (w *PDWorker) sendRegionHeartbeat(data *RegionHeartbeatData) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	req := &pdpb.RegionHeartbeatRequest{
		Term:            data.Term,
		Region:          data.Region,
		Leader:          data.Peer,
		DownPeers:       data.DownPeers,
		PendingPeers:    data.PendingPeers,
		BytesWritten:    data.WrittenBytes,
		KeysWritten:     data.WrittenKeys,
		ApproximateSize: data.ApproximateSize,
		ApproximateKeys: data.ApproximateKeys,
	}

	if err := w.pdClient.ReportRegionHeartbeat(ctx, req); err != nil {
		slog.Warn("region heartbeat failed", "region", data.Region.GetId(), "err", err)
	}
}

func (w *PDWorker) reportBatchSplit(regions []*metapb.Region) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	if err := w.pdClient.ReportBatchSplit(ctx, regions); err != nil {
		slog.Warn("report batch split failed", "err", err)
	}
}
