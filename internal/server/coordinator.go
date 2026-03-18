package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/raftstore"
	"github.com/ryogrid/gookvs/internal/raftstore/router"
	"github.com/ryogrid/gookvs/internal/server/transport"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
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

	peers   map[uint64]*raftstore.Peer
	cancels map[uint64]context.CancelFunc
	dones   map[uint64]chan struct{}
}

// StoreCoordinatorConfig holds the configuration for creating a StoreCoordinator.
type StoreCoordinatorConfig struct {
	StoreID uint64
	Engine  traits.KvEngine
	Storage *Storage
	Router  *router.Router
	Client  *transport.RaftClient
	PeerCfg raftstore.PeerConfig
}

// NewStoreCoordinator creates a new StoreCoordinator.
func NewStoreCoordinator(cfg StoreCoordinatorConfig) *StoreCoordinator {
	return &StoreCoordinator{
		storeID: cfg.StoreID,
		engine:  cfg.Engine,
		storage: cfg.Storage,
		router:  cfg.Router,
		client:  cfg.Client,
		cfg:     cfg.PeerCfg,
		peers:   make(map[uint64]*raftstore.Peer),
		cancels: make(map[uint64]context.CancelFunc),
		dones:   make(map[uint64]chan struct{}),
	}
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

	return sc.router.Send(regionID, peerMsg)
}

// GetPeer returns the Peer for the given region, or nil.
func (sc *StoreCoordinator) GetPeer(regionID uint64) *raftstore.Peer {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.peers[regionID]
}

// Stop stops all peers and cleans up.
func (sc *StoreCoordinator) Stop() {
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
func (sc *StoreCoordinator) CreatePeer(req *raftstore.CreatePeerRequest) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	regionID := req.Region.GetId()
	if _, exists := sc.peers[regionID]; exists {
		return fmt.Errorf("raftstore: peer for region %d already exists", regionID)
	}

	peer, err := raftstore.NewPeer(
		regionID, req.PeerID, sc.storeID,
		req.Region, sc.engine, sc.cfg,
		nil, // nil peers = non-bootstrap, recover from engine or start empty
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
func (sc *StoreCoordinator) maybeCreatePeerForMessage(msg *raft_serverpb.RaftMessage) {
	regionID := msg.GetRegionId()
	if sc.router.HasRegion(regionID) {
		return
	}

	region := &metapb.Region{
		Id: regionID,
		Peers: []*metapb.Peer{
			msg.GetFromPeer(),
			msg.GetToPeer(),
		},
	}

	req := &raftstore.CreatePeerRequest{
		Region: region,
		PeerID: msg.GetToPeer().GetId(),
	}
	_ = sc.CreatePeer(req)
}

// sendRaftMessage converts and sends a single raftpb.Message via gRPC transport.
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

	_ = sc.client.Send(toStoreID, raftMessage)
}
