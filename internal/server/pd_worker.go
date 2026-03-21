package server

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookv/internal/raftstore"
	"github.com/ryogrid/gookv/pkg/pdclient"
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

	taskCh     chan PDTask
	peerTaskCh chan interface{} // receives RegionHeartbeatInfo from peers
	startTime  time.Time

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

	peerTaskCh := make(chan interface{}, capacity)

	return &PDWorker{
		storeID:                cfg.StoreID,
		pdClient:               cfg.PDClient,
		coordinator:            cfg.Coordinator,
		taskCh:                 make(chan PDTask, capacity),
		peerTaskCh:             peerTaskCh,
		startTime:              time.Now(),
		storeHeartbeatInterval: interval,
		ctx:                    ctx,
		cancel:                 cancel,
	}
}

// PeerTaskCh returns the channel that peers can send RegionHeartbeatInfo to.
func (w *PDWorker) PeerTaskCh() chan<- interface{} {
	return w.peerTaskCh
}

// SetCoordinator sets the coordinator after construction (needed when coordinator
// depends on PDWorker's channel and PDWorker depends on coordinator).
func (w *PDWorker) SetCoordinator(coord *StoreCoordinator) {
	w.coordinator = coord
}

// Run starts the PDWorker goroutines: store heartbeat loop, task processor,
// and peer task processor.
func (w *PDWorker) Run() {
	w.wg.Add(3)
	go func() {
		defer w.wg.Done()
		w.storeHeartbeatLoop()
	}()
	go func() {
		defer w.wg.Done()
		w.taskProcessorLoop()
	}()
	go func() {
		defer w.wg.Done()
		w.peerTaskProcessorLoop()
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

	resp, err := w.pdClient.ReportRegionHeartbeat(ctx, req)
	if err != nil {
		slog.Warn("region heartbeat failed", "region", data.Region.GetId(), "err", err)
		return
	}

	if resp != nil && data.Region != nil {
		w.handleSchedulingCommand(data.Region.GetId(), resp)
	}
}

// handleSchedulingCommand processes PD scheduling commands from a heartbeat response.
func (w *PDWorker) handleSchedulingCommand(regionID uint64, resp *pdpb.RegionHeartbeatResponse) {
	if resp.GetTransferLeader() != nil {
		w.sendScheduleMsg(regionID, &raftstore.ScheduleMsg{
			Type:           raftstore.ScheduleMsgTransferLeader,
			TransferLeader: resp.GetTransferLeader(),
		})
	}
	if resp.GetChangePeer() != nil {
		w.sendScheduleMsg(regionID, &raftstore.ScheduleMsg{
			Type:       raftstore.ScheduleMsgChangePeer,
			ChangePeer: resp.GetChangePeer(),
		})
	}
	if resp.GetMerge() != nil {
		w.sendScheduleMsg(regionID, &raftstore.ScheduleMsg{
			Type:  raftstore.ScheduleMsgMerge,
			Merge: resp.GetMerge(),
		})
	}
}

// sendScheduleMsg delivers a scheduling message to a peer via the router.
func (w *PDWorker) sendScheduleMsg(regionID uint64, msg *raftstore.ScheduleMsg) {
	if w.coordinator == nil {
		return
	}
	peerMsg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeSchedule,
		Data: msg,
	}
	if err := w.coordinator.Router().Send(regionID, peerMsg); err != nil {
		slog.Warn("failed to send schedule message",
			"region", regionID, "type", msg.Type, "err", err)
	}
}

// peerTaskProcessorLoop receives RegionHeartbeatInfo from peers and forwards
// them to PD as region heartbeats.
func (w *PDWorker) peerTaskProcessorLoop() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.peerTaskCh:
			if info, ok := task.(*raftstore.RegionHeartbeatInfo); ok {
				// Send heartbeat asynchronously to avoid blocking
				// the task channel when PD is slow or unreachable.
				data := &RegionHeartbeatData{
					Region: info.Region,
					Peer:   info.Peer,
				}
				go w.sendRegionHeartbeat(data)
			}
		}
	}
}

func (w *PDWorker) reportBatchSplit(regions []*metapb.Region) {
	ctx, cancel := context.WithTimeout(w.ctx, 5*time.Second)
	defer cancel()

	if err := w.pdClient.ReportBatchSplit(ctx, regions); err != nil {
		slog.Warn("report batch split failed", "err", err)
	}
}
