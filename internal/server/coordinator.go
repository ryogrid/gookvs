package server

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/internal/raftstore"
	"github.com/ryogrid/gookv/internal/raftstore/router"
	"github.com/ryogrid/gookv/internal/raftstore/split"
	"github.com/ryogrid/gookv/internal/server/transport"
	"github.com/ryogrid/gookv/internal/storage/mvcc"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

// StoreCoordinator manages the lifecycle of Raft peers for a single store.
// It bridges the Raft layer with the gRPC transport.
type StoreCoordinator struct {
	mu      sync.RWMutex
	storeID uint64
	engine  traits.KvEngine
	storage *Storage
	router  *router.Router
	client  *transport.RaftClient
	cfg     raftstore.PeerConfig

	pdTaskCh chan<- interface{}

	// Split check support.
	splitCheckWorker *split.SplitCheckWorker
	pdClient         pdclient.Client

	// Snapshot generation worker.
	snapWorker    *raftstore.SnapWorker
	snapTaskCh    chan raftstore.GenSnapTask
	snapStopCh    chan struct{}
	snapSemaphore chan struct{} // limits concurrent snapshot sends

	peers   map[uint64]*raftstore.Peer
	cancels map[uint64]context.CancelFunc
	dones   map[uint64]chan struct{}
}

// StoreCoordinatorConfig holds the configuration for creating a StoreCoordinator.
type StoreCoordinatorConfig struct {
	StoreID       uint64
	Engine        traits.KvEngine
	Storage       *Storage
	Router        *router.Router
	Client        *transport.RaftClient
	PeerCfg       raftstore.PeerConfig
	PDTaskCh      chan<- interface{}       // Optional: channel for PD heartbeat tasks
	PDClient      pdclient.Client          // Optional: PD client for split coordination
	SplitCheckCfg split.SplitCheckWorkerConfig // Split check worker configuration
}

// NewStoreCoordinator creates a new StoreCoordinator.
func NewStoreCoordinator(cfg StoreCoordinatorConfig) *StoreCoordinator {
	// Create snapshot worker channel and worker.
	snapTaskCh := make(chan raftstore.GenSnapTask, 64)
	snapStopCh := make(chan struct{})
	snapWorker := raftstore.NewSnapWorker(cfg.Engine, snapTaskCh, snapStopCh)
	go snapWorker.Run()

	sc := &StoreCoordinator{
		storeID:       cfg.StoreID,
		engine:        cfg.Engine,
		storage:       cfg.Storage,
		router:        cfg.Router,
		client:        cfg.Client,
		cfg:           cfg.PeerCfg,
		pdTaskCh:      cfg.PDTaskCh,
		pdClient:      cfg.PDClient,
		snapWorker:    snapWorker,
		snapTaskCh:    snapTaskCh,
		snapStopCh:    snapStopCh,
		snapSemaphore: make(chan struct{}, 3),
		peers:         make(map[uint64]*raftstore.Peer),
		cancels:       make(map[uint64]context.CancelFunc),
		dones:         make(map[uint64]chan struct{}),
	}

	// Create and start the split check worker if PD client is available.
	if cfg.PDClient != nil {
		splitCfg := cfg.SplitCheckCfg
		if splitCfg.SplitSize == 0 {
			splitCfg = split.DefaultSplitCheckWorkerConfig()
		}
		sc.splitCheckWorker = split.NewSplitCheckWorker(cfg.Engine, splitCfg)
		go sc.splitCheckWorker.Run()
	}

	return sc
}

// BootstrapRegion creates and starts a Raft peer for a region.
// allPeers contains the initial Raft peer list for bootstrap.
// If allPeers is nil, the peer restarts from persisted engine state.
func (sc *StoreCoordinator) BootstrapRegion(region *metapb.Region, allPeers []raft.Peer) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	regionID := region.GetId()
	if _, ok := sc.peers[regionID]; ok {
		return fmt.Errorf("raftstore: region %d already exists", regionID)
	}

	// Find this store's peer ID from the region metadata.
	var peerID uint64
	for _, p := range region.GetPeers() {
		if p.GetStoreId() == sc.storeID {
			peerID = p.GetId()
			break
		}
	}
	if peerID == 0 {
		return fmt.Errorf("raftstore: store %d not found in region %d peers", sc.storeID, regionID)
	}

	peer, err := raftstore.NewPeer(regionID, peerID, sc.storeID, region, sc.engine, sc.cfg, allPeers)
	if err != nil {
		return err
	}

	// Wire sendFunc to use gRPC transport.
	peer.SetSendFunc(func(msgs []raftpb.Message) {
		for i := range msgs {
			sc.sendRaftMessage(regionID, region, peerID, &msgs[i])
		}
	})

	// Wire applyFunc to apply committed entries to the KV storage engine.
	peer.SetApplyFunc(func(regionID uint64, entries []raftpb.Entry) {
		sc.applyEntries(entries)
	})

	// Wire PD task channel for region heartbeats.
	if sc.pdTaskCh != nil {
		peer.SetPDTaskCh(sc.pdTaskCh)
	}

	// Wire split check channel for PD-coordinated splits.
	if sc.splitCheckWorker != nil {
		peer.SetSplitCheckCh(sc.splitCheckWorker.TaskCh())
	}

	// Wire snapshot task channel for async snapshot generation.
	if sc.snapTaskCh != nil {
		peer.SetSnapTaskCh(sc.snapTaskCh)
	}

	// Register with router.
	if err := sc.router.Register(regionID, peer.Mailbox); err != nil {
		return err
	}

	// Start the peer goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sc.peers[regionID] = peer
	sc.cancels[regionID] = cancel
	sc.dones[regionID] = done

	go func() {
		peer.Run(ctx)
		close(done)
	}()

	return nil
}

// applyEntries applies committed Raft entries to the KV storage engine.
func (sc *StoreCoordinator) applyEntries(entries []raftpb.Entry) {
	for _, entry := range entries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		var req raft_cmdpb.RaftCmdRequest
		if err := req.Unmarshal(entry.Data); err != nil {
			continue
		}

		if len(req.Requests) == 0 {
			continue
		}

		// Convert protobuf requests back to MVCC modifications and apply.
		modifies := RequestsToModifies(req.Requests)
		if len(modifies) > 0 {
			_ = sc.storage.ApplyModifies(modifies)
		}
	}
}

// ProposeModifies proposes MVCC modifications via Raft for the given region.
// It serializes the modifications as raft_cmdpb.RaftCmdRequest (protobuf binary)
// and waits for Raft consensus. Returns after the entry is committed and applied.
func (sc *StoreCoordinator) ProposeModifies(regionID uint64, modifies []mvcc.Modify, timeout time.Duration) error {
	sc.mu.RLock()
	peer, ok := sc.peers[regionID]
	sc.mu.RUnlock()

	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}

	if !peer.IsLeader() {
		return fmt.Errorf("raftstore: not leader for region %d", regionID)
	}

	// Build RaftCmdRequest with the modifications as Put/Delete requests.
	reqs := ModifiesToRequests(modifies)
	cmdReq := &raft_cmdpb.RaftCmdRequest{
		Requests: reqs,
	}

	// Send as a RaftCommand via the peer's mailbox with a callback.
	doneCh := make(chan struct{}, 1)

	cmd := &raftstore.RaftCommand{
		Request: cmdReq,
		Callback: func(resp *raft_cmdpb.RaftCmdResponse) {
			doneCh <- struct{}{}
		},
	}

	msg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeRaftCommand,
		Data: cmd,
	}

	if err := sc.router.Send(regionID, msg); err != nil {
		return fmt.Errorf("raftstore: send proposal: %w", err)
	}

	// Wait for commit or timeout.
	select {
	case <-doneCh:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("raftstore: proposal timeout for region %d", regionID)
	}
}

// HandleRaftMessage dispatches an incoming RaftMessage to the appropriate peer.
// If the region is not known, the message is routed to the store worker for
// potential dynamic peer creation.
func (sc *StoreCoordinator) HandleRaftMessage(msg *raft_serverpb.RaftMessage) error {
	regionID := msg.GetRegionId()

	raftMsg, err := raftstore.EraftpbToRaftpb(msg.GetMessage())
	if err != nil {
		return fmt.Errorf("raftstore: convert message: %w", err)
	}

	peerMsg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeRaftMessage,
		Data: &raftMsg,
	}

	err = sc.router.Send(regionID, peerMsg)
	if err == router.ErrRegionNotFound {
		// Route to store worker for potential peer creation.
		storeMsg := raftstore.StoreMsg{
			Type: raftstore.StoreMsgTypeRaftMessage,
			Data: msg,
		}
		return sc.router.SendStore(storeMsg)
	}
	return err
}

// GetPeer returns the Peer for the given region, or nil.
func (sc *StoreCoordinator) GetPeer(regionID uint64) *raftstore.Peer {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.peers[regionID]
}

// Stop stops all peers and cleans up.
func (sc *StoreCoordinator) Stop() {
	// Stop the split check worker first.
	if sc.splitCheckWorker != nil {
		sc.splitCheckWorker.Stop()
	}

	// Stop the snapshot worker.
	if sc.snapStopCh != nil {
		close(sc.snapStopCh)
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()

	for regionID, cancel := range sc.cancels {
		cancel()
		<-sc.dones[regionID]
		sc.router.Unregister(regionID)
	}
	sc.peers = make(map[uint64]*raftstore.Peer)
	sc.cancels = make(map[uint64]context.CancelFunc)
	sc.dones = make(map[uint64]chan struct{})
}

// Router returns the router for message routing.
func (sc *StoreCoordinator) Router() *router.Router {
	return sc.router
}

// RegionCount returns the number of active regions.
func (sc *StoreCoordinator) RegionCount() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return len(sc.peers)
}

// IsBusy returns whether the store is under heavy load.
func (sc *StoreCoordinator) IsBusy() bool {
	return false // TODO: integrate with flow control
}

// RunStoreWorker processes store-level messages from the Router.
// Should be started as a goroutine.
func (sc *StoreCoordinator) RunStoreWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sc.router.StoreCh():
			sc.handleStoreMsg(msg)
		}
	}
}

func (sc *StoreCoordinator) handleStoreMsg(msg raftstore.StoreMsg) {
	switch msg.Type {
	case raftstore.StoreMsgTypeCreatePeer:
		req := msg.Data.(*raftstore.CreatePeerRequest)
		_ = sc.CreatePeer(req)
	case raftstore.StoreMsgTypeDestroyPeer:
		req := msg.Data.(*raftstore.DestroyPeerRequest)
		_ = sc.DestroyPeer(req)
	case raftstore.StoreMsgTypeRaftMessage:
		raftMsg := msg.Data.(*raft_serverpb.RaftMessage)
		sc.maybeCreatePeerForMessage(raftMsg)
	}
}

// CreatePeer creates and starts a new peer for the given region.
// If the peer has no persisted Raft state, it is bootstrapped with the region's
// full peer list so the Raft node knows the correct cluster configuration.
func (sc *StoreCoordinator) CreatePeer(req *raftstore.CreatePeerRequest) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	regionID := req.Region.GetId()
	if _, exists := sc.peers[regionID]; exists {
		return fmt.Errorf("raftstore: peer for region %d already exists", regionID)
	}

	// Check if persisted Raft state exists for this region.
	// If not, bootstrap with the region's peers so the Raft node
	// has the correct cluster configuration from the start.
	var raftPeers []raft.Peer
	if !raftstore.HasPersistedRaftState(sc.engine, regionID) {
		for _, p := range req.Region.GetPeers() {
			raftPeers = append(raftPeers, raft.Peer{ID: p.GetId()})
		}
	}

	peer, err := raftstore.NewPeer(
		regionID, req.PeerID, sc.storeID,
		req.Region, sc.engine, sc.cfg,
		raftPeers,
	)
	if err != nil {
		return err
	}

	// Wire sendFunc and applyFunc (same pattern as BootstrapRegion).
	peer.SetSendFunc(func(msgs []raftpb.Message) {
		for i := range msgs {
			sc.sendRaftMessage(regionID, req.Region, req.PeerID, &msgs[i])
		}
	})
	peer.SetApplyFunc(func(regionID uint64, entries []raftpb.Entry) {
		sc.applyEntries(entries)
	})
	if sc.pdTaskCh != nil {
		peer.SetPDTaskCh(sc.pdTaskCh)
	}

	// Wire split check channel for PD-coordinated splits.
	if sc.splitCheckWorker != nil {
		peer.SetSplitCheckCh(sc.splitCheckWorker.TaskCh())
	}

	// Wire snapshot task channel for async snapshot generation.
	if sc.snapTaskCh != nil {
		peer.SetSnapTaskCh(sc.snapTaskCh)
	}

	if err := sc.router.Register(regionID, peer.Mailbox); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	sc.peers[regionID] = peer
	sc.cancels[regionID] = cancel
	sc.dones[regionID] = done

	go func() {
		peer.Run(ctx)
		close(done)
	}()

	return nil
}

// DestroyPeer stops and removes a peer for the given region.
func (sc *StoreCoordinator) DestroyPeer(req *raftstore.DestroyPeerRequest) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	regionID := req.RegionID
	cancel, ok := sc.cancels[regionID]
	if !ok {
		return fmt.Errorf("raftstore: peer for region %d not found", regionID)
	}

	// Cancel the peer goroutine.
	cancel()
	<-sc.dones[regionID]

	// Unregister from Router.
	sc.router.Unregister(regionID)

	// Remove from internal maps.
	delete(sc.peers, regionID)
	delete(sc.cancels, regionID)
	delete(sc.dones, regionID)

	// Clean up persisted Raft state for this region.
	if err := raftstore.CleanupRegionData(sc.engine, regionID); err != nil {
		return fmt.Errorf("raftstore: cleanup region %d data: %w", regionID, err)
	}

	return nil
}

// maybeCreatePeerForMessage creates a peer if a RaftMessage arrives for an unknown region.
// When PD is available, it queries PD for full region metadata so the child peer
// is bootstrapped with the correct cluster configuration (all peers).
func (sc *StoreCoordinator) maybeCreatePeerForMessage(msg *raft_serverpb.RaftMessage) {
	regionID := msg.GetRegionId()
	if sc.router.HasRegion(regionID) {
		return
	}

	slog.Info("creating peer for unknown region",
		"region_id", regionID,
		"from_store", msg.GetFromPeer().GetStoreId(),
		"to_peer", msg.GetToPeer().GetId())

	// Try to get full region metadata from PD for correct peer list.
	var region *metapb.Region
	if sc.pdClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, _, err := sc.pdClient.GetRegionByID(ctx, regionID)
		cancel()
		if err == nil && resp != nil {
			region = resp
		}
	}

	// Fallback: construct minimal region from the message.
	if region == nil {
		region = &metapb.Region{
			Id: regionID,
			Peers: []*metapb.Peer{
				msg.GetFromPeer(),
				msg.GetToPeer(),
			},
		}
	}

	req := &raftstore.CreatePeerRequest{
		Region: region,
		PeerID: msg.GetToPeer().GetId(),
	}
	_ = sc.CreatePeer(req)
}

// RunSplitResultHandler processes split check results from the SplitCheckWorker
// and coordinates the full split flow: ask PD for IDs, execute the split,
// bootstrap new child regions, and report to PD.
// Should be started as a goroutine.
func (sc *StoreCoordinator) RunSplitResultHandler(ctx context.Context) {
	if sc.splitCheckWorker == nil {
		return
	}
	resultCh := sc.splitCheckWorker.ResultCh()
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-resultCh:
			sc.handleSplitCheckResult(result)
		}
	}
}

func (sc *StoreCoordinator) handleSplitCheckResult(result split.SplitCheckResult) {
	if result.SplitKey == nil {
		return // no split needed
	}

	peer := sc.GetPeer(result.RegionID)
	if peer == nil {
		slog.Warn("split: peer not found for region", "region", result.RegionID)
		return
	}
	if !peer.IsLeader() {
		return // only leaders execute splits
	}

	region := peer.Region()

	// 1. Ask PD for new region/peer IDs.
	resp, err := sc.pdClient.AskBatchSplit(context.Background(), region, 1)
	if err != nil {
		slog.Warn("split: AskBatchSplit failed", "region", result.RegionID, "err", err)
		return
	}

	ids := resp.GetIds()
	if len(ids) == 0 {
		slog.Warn("split: AskBatchSplit returned no IDs", "region", result.RegionID)
		return
	}

	// 2. Extract new region IDs and peer ID sets.
	newRegionIDs := make([]uint64, len(ids))
	newPeerIDSets := make([][]uint64, len(ids))
	for i, splitID := range ids {
		newRegionIDs[i] = splitID.GetNewRegionId()
		newPeerIDSets[i] = splitID.GetNewPeerIds()
	}

	// 3. Execute the split to produce new region metadata.
	splitResult, err := split.ExecBatchSplit(region, [][]byte{result.SplitKey}, newRegionIDs, newPeerIDSets)
	if err != nil {
		slog.Warn("split: ExecBatchSplit failed", "region", result.RegionID, "err", err)
		return
	}

	slog.Info("split: region split executed",
		"parent", result.RegionID,
		"splitKey", fmt.Sprintf("%x", result.SplitKey),
		"newRegions", len(splitResult.Regions),
	)

	// 4. Update the parent region's metadata.
	peer.UpdateRegion(splitResult.Derived)

	// 5. Bootstrap new child regions as peers on this store.
	for _, newRegion := range splitResult.Regions {
		// Build raft.Peer list for bootstrap.
		raftPeers := make([]raft.Peer, 0, len(newRegion.GetPeers()))
		for _, p := range newRegion.GetPeers() {
			raftPeers = append(raftPeers, raft.Peer{ID: p.GetId()})
		}

		if err := sc.BootstrapRegion(newRegion, raftPeers); err != nil {
			slog.Warn("split: failed to bootstrap child region",
				"region", newRegion.GetId(), "err", err)
		}
	}

	// 6. Report all regions (parent + children) to PD.
	allRegions := make([]*metapb.Region, 0, 1+len(splitResult.Regions))
	allRegions = append(allRegions, splitResult.Derived)
	allRegions = append(allRegions, splitResult.Regions...)
	if err := sc.pdClient.ReportBatchSplit(context.Background(), allRegions); err != nil {
		slog.Warn("split: ReportBatchSplit failed", "err", err)
	}
}

// ResolveRegionForKey returns the region ID whose key range contains the given key.
// When multiple regions match (e.g., after a split where the parent's metadata is stale),
// returns the most specific (narrowest) match.
// Returns 0 if no matching region is found.
func (sc *StoreCoordinator) ResolveRegionForKey(key []byte) uint64 {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	var bestID uint64
	var bestStartKey []byte
	bestStartSet := false

	for regionID, peer := range sc.peers {
		region := peer.Region()
		startKey := region.GetStartKey()
		endKey := region.GetEndKey()

		// Check if key >= startKey.
		if len(startKey) > 0 && bytes.Compare(key, startKey) < 0 {
			continue
		}
		// Check if key < endKey (empty endKey means unbounded).
		if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
			continue
		}

		// Among all matching regions, pick the one with the largest startKey
		// (most specific / narrowest range). This handles stale parent regions
		// whose key range hasn't been updated after a split.
		if !bestStartSet || bytes.Compare(startKey, bestStartKey) > 0 {
			bestID = regionID
			bestStartKey = startKey
			bestStartSet = true
		}
	}
	return bestID
}

// sendRaftMessage converts and sends a single raftpb.Message via gRPC transport.
// For MsgSnap messages, uses streaming SendSnapshot instead of regular Send.
func (sc *StoreCoordinator) sendRaftMessage(regionID uint64, region *metapb.Region, fromPeerID uint64, msg *raftpb.Message) {
	var toStoreID uint64
	for _, p := range region.GetPeers() {
		if p.GetId() == msg.To {
			toStoreID = p.GetStoreId()
			break
		}
	}
	if toStoreID == 0 {
		return
	}

	eMsg, err := raftstore.RaftpbToEraftpb(msg)
	if err != nil {
		return
	}

	raftMessage := &raft_serverpb.RaftMessage{
		RegionId: regionID,
		FromPeer: &metapb.Peer{Id: fromPeerID, StoreId: sc.storeID},
		ToPeer:   &metapb.Peer{Id: msg.To, StoreId: toStoreID},
		Message:  eMsg,
	}

	if msg.Type == raftpb.MsgSnap {
		snapData := msg.Snapshot.Data
		go func() {
			sc.snapSemaphore <- struct{}{} // acquire
			defer func() { <-sc.snapSemaphore }() // release

			if err := sc.client.SendSnapshot(toStoreID, raftMessage, snapData); err != nil {
				slog.Warn("snapshot send failed", "to_store", toStoreID, "region", regionID, "err", err)
				sc.reportSnapshotStatus(regionID, msg.To, raft.SnapshotFailure)
			} else {
				sc.reportSnapshotStatus(regionID, msg.To, raft.SnapshotFinish)
			}
		}()
		return
	}

	if err := sc.client.Send(toStoreID, raftMessage); err != nil {
		slog.Warn("raft send failed", "to_store", toStoreID, "err", err)
		sc.reportUnreachable(regionID, msg.To)
	}
}

// reportSnapshotStatus sends a snapshot status report back to the source peer.
func (sc *StoreCoordinator) reportSnapshotStatus(regionID uint64, toPeerID uint64, status raft.SnapshotStatus) {
	peerMsg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeSignificant,
		Data: &raftstore.SignificantMsg{
			Type:     raftstore.SignificantMsgTypeSnapshotStatus,
			RegionID: regionID,
			ToPeerID: toPeerID,
			Status:   status,
		},
	}
	_ = sc.router.Send(regionID, peerMsg) // Best-effort
}

// reportUnreachable notifies a peer that a target is unreachable.
func (sc *StoreCoordinator) reportUnreachable(regionID uint64, toPeerID uint64) {
	peerMsg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeSignificant,
		Data: &raftstore.SignificantMsg{
			Type:     raftstore.SignificantMsgTypeUnreachable,
			RegionID: regionID,
			ToPeerID: toPeerID,
		},
	}
	_ = sc.router.Send(regionID, peerMsg) // Best-effort
}

// HandleSnapshotMessage processes an incoming snapshot received via gRPC streaming.
func (sc *StoreCoordinator) HandleSnapshotMessage(msg *raft_serverpb.RaftMessage, data []byte) error {
	regionID := msg.GetRegionId()

	raftMsg, err := raftstore.EraftpbToRaftpb(msg.GetMessage())
	if err != nil {
		return err
	}

	// Attach snapshot data to the raft message.
	raftMsg.Snapshot.Data = data

	peerMsg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeRaftMessage,
		Data: &raftMsg,
	}

	// If region doesn't exist locally, try to create it.
	if !sc.router.HasRegion(regionID) {
		sc.maybeCreatePeerForMessage(msg)
	}

	return sc.router.Send(regionID, peerMsg)
}
