package server

import (
	"bytes"
	"context"
	"encoding/binary"
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
	"os"

	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/keys"
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

	// splitResultCh receives split results from peers for child region bootstrapping.
	splitResultCh chan *raftstore.SplitRegionResult

	// Performance optimization components (nil when disabled).
	raftLogWriter   *raftstore.RaftLogWriter
	applyWorkerPool *raftstore.ApplyWorkerPool

	// ReadIndex batching: per-region batchers for coalescing concurrent reads.
	batchers map[uint64]*ReadIndexBatcher
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
	SplitCheckCfg        split.SplitCheckWorkerConfig // Split check worker configuration
	EnableBatchRaftWrite bool                         // Enable Raft log batch writer
	EnableApplyPipeline  bool                         // Enable async apply worker pool
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
		splitResultCh: make(chan *raftstore.SplitRegionResult, 16),
		batchers:      make(map[uint64]*ReadIndexBatcher),
	}

	// Create Raft log batch writer if enabled.
	if cfg.EnableBatchRaftWrite {
		sc.raftLogWriter = raftstore.NewRaftLogWriter(cfg.Engine, 256)
		slog.Info("Raft log batch writer enabled")
	}

	// Create apply worker pool if enabled.
	if cfg.EnableApplyPipeline {
		sc.applyWorkerPool = raftstore.NewApplyWorkerPool(4)
		slog.Info("Apply pipeline enabled")
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

// RecoverPersistedRegions scans the engine for regions that have persisted
// Raft state (hard state or apply state) and recreates their peers.
// This must be called at startup before processing any Raft messages,
// so that restarted nodes don't lose already-committed data.
func (sc *StoreCoordinator) RecoverPersistedRegions() int {
	// Scan CFRaft for all keys to find persisted region IDs.
	regionIDs := make(map[uint64]bool)

	prefix := []byte{keys.LocalPrefix, keys.RegionRaftPrefix}
	iter := sc.engine.NewIterator(cfnames.CFRaft, traits.IterOptions{})
	defer iter.Close()

	scanCount := 0
	for iter.Seek(prefix); iter.Valid(); iter.Next() {
		scanCount++
		k := iter.Key()
		if len(k) < 2 || k[0] != keys.LocalPrefix || k[1] != keys.RegionRaftPrefix {
			break
		}
		regionID, err := keys.RegionIDFromRaftKey(k)
		if err != nil {
			continue
		}
		regionIDs[regionID] = true
	}

	// Log scan results to stdout (not slog) to ensure visibility even if slog file is stale.
	fmt.Fprintf(os.Stderr, "[RecoverPersistedRegions] store=%d scanned=%d regionIDs=%d regions=%v\n",
		sc.storeID, scanCount, len(regionIDs), regionIDs)

	recovered := 0
	for regionID := range regionIDs {
		sc.mu.RLock()
		_, exists := sc.peers[regionID]
		sc.mu.RUnlock()
		if exists {
			continue
		}

		// Query PD for complete region metadata.
		var region *metapb.Region
		if sc.pdClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r, _, err := sc.pdClient.GetRegionByID(ctx, regionID)
			cancel()
			if err == nil && r != nil {
				region = r
			}
		}
		if region == nil {
			continue // can't recover without region metadata
		}

		// Find our peer ID in the region.
		var peerID uint64
		for _, p := range region.GetPeers() {
			if p.GetStoreId() == sc.storeID {
				peerID = p.GetId()
				break
			}
		}
		if peerID == 0 {
			continue // this store is no longer part of this region
		}

		req := &raftstore.CreatePeerRequest{
			Region: region,
			PeerID: peerID,
		}
		if err := sc.CreatePeer(req); err != nil {
			slog.Warn("recover region failed", "region", regionID, "err", err)
			continue
		}
		recovered++
		slog.Info("recovered region", "region", regionID, "peer", peerID)
	}
	return recovered
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

	// Persist initial Raft state immediately so that if the node crashes
	// before the first Ready cycle, HasPersistedRaftState returns true
	// on restart and the peer is recovered instead of re-bootstrapped.
	if err := peer.FlushInitialState(); err != nil {
		slog.Warn("BootstrapRegion: flush initial state failed", "region", regionID, "err", err)
	}

	// Wire sendFunc to use gRPC transport.
	// Use peer.Region() (not the captured region) so that ConfChange updates
	// (added/removed peers) are visible when resolving msg.To → store ID.
	peer.SetSendFunc(func(msgs []raftpb.Message) {
		currentRegion := peer.Region()
		for i := range msgs {
			sc.sendRaftMessage(regionID, currentRegion, peerID, &msgs[i])
		}
	})

	// Wire applyFunc to apply committed entries to the KV storage engine.
	peer.SetApplyFunc(func(_ uint64, entries []raftpb.Entry) {
		sc.applyEntriesForPeer(peer, entries)
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

	// Wire split result channel for Raft-based split.
	peer.SetSplitResultCh(sc.splitResultCh)

	// Wire Raft log batch writer if enabled.
	if sc.raftLogWriter != nil {
		peer.SetRaftLogWriter(sc.raftLogWriter)
	}

	// Wire apply worker pool if enabled.
	if sc.applyWorkerPool != nil {
		peer.SetApplyWorkerPool(sc.applyWorkerPool)
	}

	// Register with router.
	if err := sc.router.Register(regionID, peer.Mailbox); err != nil {
		return err
	}

	// Register peer-to-region mapping for all peers in this region.
	// This enables correct loopback routing after splits.
	for _, p := range region.GetPeers() {
		sc.router.RegisterPeer(p.GetId(), regionID)
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

// applyEntriesForPeer applies committed Raft entries to the KV storage engine.
// Entries arriving here have already been filtered by the peer to exclude admin
// entries (ConfChange, SplitAdmin, CompactLog). Each entry carries an 8-byte
// proposal ID prefix that must be stripped before protobuf unmarshaling.
// For backward compatibility during replay of old entries without the prefix,
// falls back to unmarshaling the original data if stripping fails.
func (sc *StoreCoordinator) applyEntriesForPeer(peer *raftstore.Peer, entries []raftpb.Entry) {
	for _, entry := range entries {
		if entry.Type != raftpb.EntryNormal || len(entry.Data) == 0 {
			continue
		}

		var req raft_cmdpb.RaftCmdRequest

		if len(entry.Data) > 8 {
			// Try new format: strip 8-byte proposal ID prefix.
			cmdData := entry.Data[8:]
			if err := req.Unmarshal(cmdData); err != nil {
				// Fall back to old format (no prefix) for backward compatibility.
				req.Reset()
				if err2 := req.Unmarshal(entry.Data); err2 != nil {
					continue
				}
			}
		} else {
			// Data too short for new format; try old format directly.
			if err := req.Unmarshal(entry.Data); err != nil {
				continue
			}
		}

		if len(req.Requests) == 0 {
			continue
		}

		modifies := RequestsToModifies(req.Requests)
		if len(modifies) == 0 {
			continue
		}

		if err := sc.storage.ApplyModifies(modifies); err != nil {
			slog.Warn("[APPLY-TRACE] ApplyModifies failed",
				"modifies", len(modifies), "err", err)
		}
	}
}


// ReadIndex performs a linearizable read index check for the given region.
// The Raft leader confirms it is still the leader by contacting a quorum,
// then waits for the applied index to reach the committed index.
// Returns nil when it is safe to read from the local engine.
func (sc *StoreCoordinator) ReadIndex(regionID uint64, timeout time.Duration) error {
	sc.mu.RLock()
	peer, ok := sc.peers[regionID]
	sc.mu.RUnlock()

	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}

	if !peer.IsLeader() {
		return fmt.Errorf("raftstore: not leader for region %d", regionID)
	}

	// Leader lease is implemented but disabled: the lease confirms leadership
	// but does not guarantee that all committed Raft entries have been applied
	// to the engine. ReadIndex (ReadOnlySafe) guarantees both leadership AND
	// that appliedIndex >= readIndex, preventing stale reads.

	// Use batching to coalesce concurrent ReadIndex requests for the same region.
	batcher := sc.getOrCreateBatcher(regionID, peer)
	return batcher.Wait(timeout)
}

// getOrCreateBatcher returns the batcher for a region, creating one if needed.
func (sc *StoreCoordinator) getOrCreateBatcher(regionID uint64, peer *raftstore.Peer) *ReadIndexBatcher {
	// Fast path: read lock.
	sc.mu.RLock()
	b, ok := sc.batchers[regionID]
	sc.mu.RUnlock()
	if ok {
		return b
	}

	// Slow path: write lock with double-check.
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if b, ok = sc.batchers[regionID]; ok {
		return b
	}
	b = NewReadIndexBatcher(regionID, &coordinatorDispatcher{sc: sc, peer: peer})
	sc.batchers[regionID] = b
	return b
}

// coordinatorDispatcher implements ReadIndexDispatcher using the coordinator's
// existing ReadIndex protocol (peer + router).
type coordinatorDispatcher struct {
	sc   *StoreCoordinator
	peer *raftstore.Peer
}

func (d *coordinatorDispatcher) Dispatch(regionID uint64) error {
	id := d.peer.NextReadID()
	requestCtx := make([]byte, 8)
	binary.BigEndian.PutUint64(requestCtx, id)

	doneCh := make(chan error, 1)
	req := &raftstore.ReadIndexRequest{
		RequestCtx: requestCtx,
		Callback:   func(err error) { doneCh <- err },
	}

	msg := raftstore.PeerMsg{
		Type: raftstore.PeerMsgTypeReadIndex,
		Data: req,
	}

	if err := d.sc.router.Send(regionID, msg); err != nil {
		return fmt.Errorf("raftstore: send read index: %w", err)
	}

	select {
	case err := <-doneCh:
		return err
	case <-time.After(30 * time.Second):
		d.peer.CancelPendingRead(requestCtx)
		return fmt.Errorf("raftstore: read index timeout for region %d", regionID)
	}
}

// ProposeModifies proposes MVCC modifications via Raft for the given region.
// It serializes the modifications as raft_cmdpb.RaftCmdRequest (protobuf binary)
// and waits for Raft consensus. Returns after the entry is committed and applied.
// If reqEpoch is provided and the region's current epoch differs, the proposal
// is rejected with an epoch-not-match error (client should retry with fresh region info).
func (sc *StoreCoordinator) ProposeModifies(regionID uint64, modifies []mvcc.Modify, timeout time.Duration, reqEpoch ...*metapb.RegionEpoch) error {
	sc.mu.RLock()
	peer, ok := sc.peers[regionID]
	sc.mu.RUnlock()

	if !ok {
		return fmt.Errorf("raftstore: region %d not found", regionID)
	}

	if !peer.IsLeader() {
		return fmt.Errorf("raftstore: not leader for region %d", regionID)
	}

	// Propose-time epoch check: reject if region epoch has changed since
	// the RPC was received. This prevents stale proposals from entering
	// the Raft log after a split.
	currentEpoch := peer.Region().GetRegionEpoch()
	if len(reqEpoch) > 0 && reqEpoch[0] != nil && currentEpoch != nil {
		re := reqEpoch[0]
		if re.GetVersion() != currentEpoch.GetVersion() ||
			re.GetConfVer() != currentEpoch.GetConfVer() {
			return fmt.Errorf("raftstore: epoch not match for region %d", regionID)
		}
	}

	// Build RaftCmdRequest with epoch in header for apply-level filtering.
	reqs := ModifiesToRequests(modifies)
	cmdReq := &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId:    regionID,
			RegionEpoch: currentEpoch,
		},
		Requests: reqs,
	}

	// Send as a RaftCommand via the peer's mailbox with a callback.
	doneCh := make(chan error, 1)

	cmd := &raftstore.RaftCommand{
		Request: cmdReq,
		Callback: func(resp *raft_cmdpb.RaftCmdResponse) {
			if resp != nil && resp.Header != nil && resp.Header.Error != nil {
				doneCh <- fmt.Errorf("raft proposal error: %s", resp.Header.Error.Message)
			} else {
				doneCh <- nil
			}
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
	case err := <-doneCh:
		return err
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

	slog.Debug("HandleRaftMessage", "region", regionID, "from", msg.GetFromPeer().GetId(), "to", msg.GetToPeer().GetId())
	err = sc.router.Send(regionID, peerMsg)
	if err == router.ErrMailboxFull {
		// Consensus messages must not be silently dropped. Retry briefly.
		for i := 0; i < 3; i++ {
			time.Sleep(time.Millisecond)
			err = sc.router.Send(regionID, peerMsg)
			if err != router.ErrMailboxFull {
				break
			}
		}
		if err == router.ErrMailboxFull {
			slog.Debug("HandleRaftMessage: mailbox full after retries",
				"region", regionID, "msgType", raftMsg.Type)
		}
	}
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
		// Unregister peer-to-region mappings.
		if peer, ok := sc.peers[regionID]; ok {
			for _, p := range peer.Region().GetPeers() {
				sc.router.UnregisterPeer(p.GetId())
			}
		}
		sc.router.Unregister(regionID)
	}
	sc.peers = make(map[uint64]*raftstore.Peer)
	sc.cancels = make(map[uint64]context.CancelFunc)
	sc.dones = make(map[uint64]chan struct{})

	// Stop all ReadIndex batchers.
	for id, b := range sc.batchers {
		b.Stop()
		delete(sc.batchers, id)
	}

	// Stop Raft log writer AFTER all peers have exited (they may still submit tasks).
	if sc.raftLogWriter != nil {
		sc.raftLogWriter.Stop()
	}

	// Stop apply worker pool AFTER all peers have exited and raft log writer stopped.
	// Workers drain remaining tasks before returning.
	if sc.applyWorkerPool != nil {
		sc.applyWorkerPool.Stop()
	}
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
	batcherEvictTicker := time.NewTicker(batcherIdleTimeout)
	defer batcherEvictTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-sc.router.StoreCh():
			sc.handleStoreMsg(msg)
		case <-batcherEvictTicker.C:
			sc.evictIdleBatchers()
		}
	}
}

// evictIdleBatchers removes batchers that have had no activity for the idle timeout.
func (sc *StoreCoordinator) evictIdleBatchers() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for id, b := range sc.batchers {
		if b.IsIdle() {
			b.Stop()
			delete(sc.batchers, id)
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

	// For split-created regions (non-empty StartKey) bootstrapped on a NEW
	// node (via maybeCreatePeerForMessage / PD rebalance), advance
	// TruncatedIndex so the leader sends a full snapshot containing
	// pre-split engine data instead of Raft log entries that lack it.
	// This does NOT affect peers created via BootstrapRegion (the original
	// split-apply path) because those use BootstrapRegion, not CreatePeer.
	if len(raftPeers) > 0 && len(req.Region.GetStartKey()) > 0 {
		state := raftstore.ApplyState{
			AppliedIndex:   0,
			TruncatedIndex: 1,
			TruncatedTerm:  1,
		}
		peer.SetApplyState(state)
	}

	// Persist initial Raft state immediately so that if the node crashes
	// before the first Ready cycle, HasPersistedRaftState returns true
	// on restart and the peer is recovered instead of re-bootstrapped.
	if err := peer.FlushInitialState(); err != nil {
		slog.Warn("CreatePeer: flush initial state failed", "region", regionID, "err", err)
	}

	// Wire sendFunc and applyFunc (same pattern as BootstrapRegion).
	// Use peer.Region() (not captured req.Region) so ConfChange updates are visible.
	peer.SetSendFunc(func(msgs []raftpb.Message) {
		currentRegion := peer.Region()
		for i := range msgs {
			sc.sendRaftMessage(regionID, currentRegion, req.PeerID, &msgs[i])
		}
	})
	peer.SetApplyFunc(func(_ uint64, entries []raftpb.Entry) {
		sc.applyEntriesForPeer(peer, entries)
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

	// Wire split result channel for Raft-based split.
	peer.SetSplitResultCh(sc.splitResultCh)

	// Wire Raft log batch writer if enabled.
	if sc.raftLogWriter != nil {
		peer.SetRaftLogWriter(sc.raftLogWriter)
	}

	// Wire apply worker pool if enabled.
	if sc.applyWorkerPool != nil {
		peer.SetApplyWorkerPool(sc.applyWorkerPool)
	}

	if err := sc.router.Register(regionID, peer.Mailbox); err != nil {
		return err
	}

	// Register peer-to-region mapping for all peers in this region.
	for _, p := range req.Region.GetPeers() {
		sc.router.RegisterPeer(p.GetId(), regionID)
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

	// Unregister peer-to-region mappings for all peers in this region.
	if peer, ok := sc.peers[regionID]; ok {
		for _, p := range peer.Region().GetPeers() {
			sc.router.UnregisterPeer(p.GetId())
		}
	}

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

// RunSplitResultHandler processes both split check results (from SplitCheckWorker)
// and split apply results (from peers after Raft commit). Should be started as a goroutine.
func (sc *StoreCoordinator) RunSplitResultHandler(ctx context.Context) {
	var splitCheckCh <-chan split.SplitCheckResult
	if sc.splitCheckWorker != nil {
		splitCheckCh = sc.splitCheckWorker.ResultCh()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-splitCheckCh:
			sc.handleSplitCheckResult(result)
		case result := <-sc.splitResultCh:
			sc.handleSplitApplyResult(result)
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
		return // only leaders propose splits
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

	// 3. Propose split via Raft (fire-and-forget, same as CompactLog).
	// The split is executed when the entry is committed and applied
	// in handleReady → applySplitAdminEntry. This ensures strict
	// ordering between data entries and the split in the Raft log.
	req := raftstore.SplitAdminRequest{
		SplitKey:      result.SplitKey,
		NewRegionIDs:  newRegionIDs,
		NewPeerIDSets: newPeerIDSets,
	}
	if err := peer.ProposeSplit(req); err != nil {
		slog.Warn("split: propose failed", "region", result.RegionID, "err", err)
	}
}

// handleSplitApplyResult processes a split result from a peer after Raft apply.
// It bootstraps child regions and reports to PD.
func (sc *StoreCoordinator) handleSplitApplyResult(result *raftstore.SplitRegionResult) {
	if result == nil {
		return
	}
	// Bootstrap child regions.
	for _, newRegion := range result.Regions {
		raftPeers := make([]raft.Peer, 0, len(newRegion.GetPeers()))
		for _, p := range newRegion.GetPeers() {
			raftPeers = append(raftPeers, raft.Peer{ID: p.GetId()})
		}
		if err := sc.BootstrapRegion(newRegion, raftPeers); err != nil {
			slog.Warn("split: failed to bootstrap child region",
				"region", newRegion.GetId(), "err", err)
			continue
		}
		// Kick Raft activity on child to trigger peer creation on followers.
		_ = sc.router.Send(newRegion.GetId(), raftstore.PeerMsg{
			Type: raftstore.PeerMsgTypeTick,
		})
	}
	// Report to PD.
	if sc.pdClient != nil {
		allRegions := make([]*metapb.Region, 0, 1+len(result.Regions))
		allRegions = append(allRegions, result.Derived)
		allRegions = append(allRegions, result.Regions...)
		if err := sc.pdClient.ReportBatchSplit(context.Background(), allRegions); err != nil {
			slog.Warn("split: ReportBatchSplit failed", "err", err)
		}
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
		slog.Warn("sendRaftMessage: cannot resolve store for peer",
			"region", regionID, "msgTo", msg.To, "msgType", msg.Type,
			"peerCount", len(region.GetPeers()))
		return
	}

	// Debug: trace region 1 messages to newly added peers (ID >= 1000).
	if regionID == 1 && msg.To >= 1000 {
		slog.Info("sendRaftMessage: region 1 → new peer",
			"msgTo", msg.To, "toStore", toStoreID, "msgType", msg.Type,
			"fromPeer", fromPeerID, "selfStore", sc.storeID)
	}

	// Loopback: if target peer is on this store, deliver directly via router
	// instead of gRPC. This is critical for ReadIndex (ReadOnlySafe mode)
	// which requires heartbeat responses from local follower peers.
	if toStoreID == sc.storeID {
		// Find the target region ID for this peer. After a split, the target
		// peer may belong to a different region than the source.
		targetRegionID := regionID // fallback to source region
		if rid, ok := sc.router.FindRegionByPeerID(msg.To); ok {
			targetRegionID = rid
		}

		peerMsg := raftstore.PeerMsg{
			Type: raftstore.PeerMsgTypeRaftMessage,
			Data: msg,
		}
		if err := sc.router.Send(targetRegionID, peerMsg); err == router.ErrMailboxFull {
			for i := 0; i < 3; i++ {
				time.Sleep(time.Millisecond)
				if err = sc.router.Send(targetRegionID, peerMsg); err != router.ErrMailboxFull {
					break
				}
			}
		}
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

	// Send asynchronously so the peer loop is not blocked by slow/dead stores.
	go func() {
		if err := sc.client.Send(toStoreID, raftMessage); err != nil {
			slog.Warn("raft send failed", "to_store", toStoreID, "region", regionID,
				"msgTo", msg.To, "msgType", msg.Type, "err", err)
			sc.reportUnreachable(regionID, msg.To)
		}
	}()
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
