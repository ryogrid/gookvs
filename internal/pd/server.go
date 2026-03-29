// Package pd implements a simplified Placement Driver server for gookv.
// It provides TSO allocation, cluster metadata management, heartbeat processing,
// ID allocation, and GC safe point management.
package pd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"

	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/codec"
	"github.com/ryogrid/gookv/pkg/keys"
)

// StoreState represents the lifecycle state of a TiKV store.
type StoreState int

const (
	StoreStateUp           StoreState = iota // Heartbeat within disconnect threshold
	StoreStateDisconnected                   // Heartbeat missed > DisconnectDuration
	StoreStateDown                           // Disconnected > DownDuration — replicas should be repaired
	StoreStateTombstone                      // Permanently removed
)

// PDServerRaftConfig holds the Raft-cluster configuration for a PD server.
// If nil in PDServerConfig.RaftConfig, the server runs in single-node mode.
type PDServerRaftConfig struct {
	PDNodeID             uint64            // this node's Raft ID
	InitialCluster       map[uint64]string // peerID -> peer gRPC address
	PeerAddr             string            // listen address for peer-to-peer gRPC
	ClientAddrs          map[uint64]string // peerID -> client gRPC address (for forwarding)
	RaftTickInterval     time.Duration
	ElectionTimeoutTicks int
	HeartbeatTicks       int
}

// PDServerConfig holds configuration for the PD server.
type PDServerConfig struct {
	ListenAddr string
	DataDir    string
	ClusterID  uint64

	TSOSaveInterval           time.Duration
	TSOUpdatePhysicalInterval time.Duration

	MaxPeerCount int

	StoreDisconnectDuration time.Duration
	StoreDownDuration       time.Duration

	RegionBalanceThreshold float64
	RegionBalanceRateLimit int

	// RaftConfig enables Raft-based replication when non-nil.
	// When nil, the server operates in single-node mode (backward compatible).
	RaftConfig *PDServerRaftConfig
}

// DefaultPDServerConfig returns default PD server configuration.
func DefaultPDServerConfig() PDServerConfig {
	return PDServerConfig{
		ListenAddr:                "0.0.0.0:2379",
		DataDir:                   "/tmp/gookv-pd",
		ClusterID:                 1,
		TSOSaveInterval:           3 * time.Second,
		TSOUpdatePhysicalInterval: 50 * time.Millisecond,
		MaxPeerCount:              3,
		StoreDisconnectDuration:   30 * time.Second,
		StoreDownDuration:         30 * time.Minute,
		RegionBalanceThreshold:    0.05,
		RegionBalanceRateLimit:    4,
	}
}

// PDServer implements the pdpb.PDServer gRPC interface.
type PDServer struct {
	pdpb.UnimplementedPDServer

	cfg       PDServerConfig
	clusterID uint64

	tso         *TSOAllocator
	meta        *MetadataStore
	idAlloc     *IDAllocator
	gcMgr       *GCSafePointManager
	scheduler   *Scheduler
	moveTracker *MoveTracker

	grpcServer *grpc.Server
	listener   net.Listener

	// Raft replication (nil in single-node mode).
	raftPeer       *PDRaftPeer
	raftStorage    *PDRaftStorage
	transport      *PDTransport
	raftCfg        *PDServerRaftConfig
	raftEngine     traits.KvEngine // dedicated engine for PD Raft logs
	peerGrpcServer *grpc.Server    // separate gRPC server for peer port
	peerListener   net.Listener

	// Buffered allocators (non-nil only in Raft mode).
	tsoBuffer *TSOBuffer
	idBuffer  *IDBuffer

	// Leader forwarding (cached connection to current leader for follower forwarding).
	leaderConnMu    sync.Mutex
	cachedLeaderConn *grpc.ClientConn
	cachedLeaderID  uint64

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped sync.Once
}

// NewPDServer creates a new PD server.
func NewPDServer(cfg PDServerConfig) (*PDServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	meta := NewMetadataStore(cfg.ClusterID, cfg.StoreDisconnectDuration, cfg.StoreDownDuration)
	tso := NewTSOAllocator(cfg.TSOSaveInterval)
	idAlloc := NewIDAllocator()
	gcMgr := NewGCSafePointManager()

	grpcSrv := grpc.NewServer()

	moveTracker := NewMoveTracker()
	scheduler := NewScheduler(meta, idAlloc, cfg.MaxPeerCount,
		cfg.RegionBalanceThreshold, cfg.RegionBalanceRateLimit, moveTracker)

	s := &PDServer{
		cfg:         cfg,
		clusterID:   cfg.ClusterID,
		tso:         tso,
		meta:        meta,
		idAlloc:     idAlloc,
		gcMgr:       gcMgr,
		scheduler:   scheduler,
		moveTracker: moveTracker,
		grpcServer:  grpcSrv,
		ctx:         ctx,
		cancel:      cancel,
	}

	pdpb.RegisterPDServer(grpcSrv, s)

	// Set up Raft replication if configured.
	if cfg.RaftConfig != nil {
		if err := s.initRaft(cfg.RaftConfig); err != nil {
			cancel()
			return nil, fmt.Errorf("pd: init raft: %w", err)
		}
	}

	return s, nil
}

// initRaft initializes the Raft subsystem for replicated PD mode.
func (s *PDServer) initRaft(rc *PDServerRaftConfig) error {
	s.raftCfg = rc

	// 1. Open a dedicated engine for PD Raft logs.
	raftDataDir := s.cfg.DataDir + "/raft"
	engine, err := rocks.Open(raftDataDir)
	if err != nil {
		return fmt.Errorf("open raft engine at %s: %w", raftDataDir, err)
	}
	s.raftEngine = engine

	// 2. Create PDRaftStorage.
	s.raftStorage = NewPDRaftStorage(s.cfg.ClusterID, engine)

	// 3. Build Raft peer config.
	peerCfg := DefaultPDRaftConfig()
	if rc.RaftTickInterval > 0 {
		peerCfg.RaftTickInterval = rc.RaftTickInterval
	}
	if rc.ElectionTimeoutTicks > 0 {
		peerCfg.ElectionTimeoutTicks = rc.ElectionTimeoutTicks
	}
	if rc.HeartbeatTicks > 0 {
		peerCfg.HeartbeatTicks = rc.HeartbeatTicks
	}

	// 4. Decide whether to bootstrap or recover.
	var raftPeers []raft.Peer
	isRestart := HasPersistedPDRaftState(engine, s.cfg.ClusterID)
	if isRestart {
		// Recovering from persisted state.
		if err := s.raftStorage.RecoverFromEngine(); err != nil {
			engine.Close()
			return fmt.Errorf("recover raft storage: %w", err)
		}
		raftPeers = nil // signal to NewPDRaftPeer: no bootstrap
	} else {
		// Fresh start: build peer list from InitialCluster.
		for id := range rc.InitialCluster {
			raftPeers = append(raftPeers, raft.Peer{ID: id})
		}
	}

	// 5. Create PDRaftPeer.
	peer, err := NewPDRaftPeer(rc.PDNodeID, s.raftStorage, raftPeers, rc.InitialCluster, peerCfg)
	if err != nil {
		engine.Close()
		return fmt.Errorf("create raft peer: %w", err)
	}
	s.raftPeer = peer

	// 6. Create PDTransport and wire it up.
	s.transport = NewPDTransport(rc.InitialCluster)
	peer.WireTransport(s.transport)

	// 7. Set the apply function and snapshot functions.
	peer.SetApplyFunc(s.applyCommand)
	peer.SetApplySnapshotFunc(s.ApplySnapshot)
	s.raftStorage.SetSnapshotGenFunc(s.GenerateSnapshot)

	// 8. Initialize buffered allocators and wire leader change callback.
	s.tsoBuffer = NewTSOBuffer(peer)
	s.idBuffer = NewIDBuffer(peer)
	peer.SetLeaderChangeFunc(func(isLeader bool) {
		if s.tsoBuffer != nil {
			s.tsoBuffer.Reset()
		}
		if s.idBuffer != nil {
			s.idBuffer.Reset()
		}
	})

	// 9. On restart, replay committed entries to rebuild in-memory state.
	if isRestart {
		if err := s.replayRaftLog(); err != nil {
			engine.Close()
			return fmt.Errorf("replay raft log: %w", err)
		}
	}

	return nil
}

// replayRaftLog replays committed but un-applied entries from the Raft log
// to rebuild in-memory PD state after a restart.
func (s *PDServer) replayRaftLog() error {
	as := s.raftStorage.GetApplyState()
	appliedIndex := as.AppliedIndex
	lastIndex, err := s.raftStorage.LastIndex()
	if err != nil {
		return fmt.Errorf("get last index: %w", err)
	}

	if appliedIndex >= lastIndex {
		slog.Info("pd: no entries to replay",
			"appliedIndex", appliedIndex, "lastIndex", lastIndex)
		return nil
	}

	// Read entries from appliedIndex+1 through lastIndex (inclusive).
	entries, err := s.raftStorage.Entries(appliedIndex+1, lastIndex+1, 0)
	if err != nil {
		return fmt.Errorf("read entries [%d, %d): %w", appliedIndex+1, lastIndex+1, err)
	}

	slog.Info("pd: replaying raft log entries",
		"from", appliedIndex+1, "to", lastIndex, "count", len(entries))

	for _, e := range entries {
		// Skip empty entries (leader election no-ops).
		if len(e.Data) == 0 {
			continue
		}
		// Skip conf change entries.
		if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
			continue
		}

		cmd, err := UnmarshalPDCommand(e.Data)
		if err != nil {
			slog.Warn("pd: skip unrecognized entry during replay",
				"index", e.Index, "err", err)
			continue
		}

		if _, err := s.applyCommand(cmd); err != nil {
			slog.Warn("pd: error applying entry during replay",
				"index", e.Index, "err", err)
		}
	}

	// Update applied index.
	as.AppliedIndex = lastIndex
	s.raftStorage.SetApplyState(as)

	return nil
}

// Start starts the PD server.
func (s *PDServer) Start() error {
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("pd: listen %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = lis

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(lis); err != nil {
			select {
			case <-s.ctx.Done():
			default:
				slog.Error("PD gRPC server error", "err", err)
			}
		}
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.runStoreStateWorker(s.ctx)
	}()

	// Start Raft subsystem if configured.
	if s.raftPeer != nil {
		if err := s.startRaft(); err != nil {
			return fmt.Errorf("pd: start raft: %w", err)
		}
	}

	return nil
}

// startRaft starts the peer gRPC server and the Raft event loop.
func (s *PDServer) startRaft() error {
	// Start peer gRPC server on the configured peer address.
	peerLis, err := net.Listen("tcp", s.raftCfg.PeerAddr)
	if err != nil {
		return fmt.Errorf("listen peer %s: %w", s.raftCfg.PeerAddr, err)
	}
	s.peerListener = peerLis

	s.peerGrpcServer = grpc.NewServer()
	RegisterPDPeerService(s.peerGrpcServer, s.raftPeer)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.peerGrpcServer.Serve(peerLis); err != nil {
			select {
			case <-s.ctx.Done():
			default:
				slog.Error("PD peer gRPC server error", "err", err)
			}
		}
	}()

	// Start the Raft event loop.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.raftPeer.Run(s.ctx)
	}()

	return nil
}

// runStoreStateWorker periodically updates store states based on heartbeat timestamps
// and cleans up stale pending moves.
func (s *PDServer) runStoreStateWorker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.meta.updateStoreStates()
			if s.moveTracker != nil {
				s.moveTracker.CleanupStale(10 * time.Minute)
			}
		}
	}
}

// Addr returns the listen address.
func (s *PDServer) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.ListenAddr
}

// DataDir returns the data directory path configured for this server.
func (s *PDServer) DataDir() string {
	return s.cfg.DataDir
}

// IsRaftLeader returns whether this server's Raft peer believes it is the leader.
// Returns false if the server is running in single-node mode (no Raft).
func (s *PDServer) IsRaftLeader() bool {
	if s.raftPeer == nil {
		return false
	}
	return s.raftPeer.IsLeader()
}

// Stop gracefully stops the PD server. It is safe to call multiple times.
func (s *PDServer) Stop() {
	s.stopped.Do(func() {
		s.cancel()

		// Stop Raft subsystem first.
		if s.peerGrpcServer != nil {
			s.peerGrpcServer.GracefulStop()
		}
		if s.transport != nil {
			s.transport.Close()
		}

		// Close cached leader connection.
		s.leaderConnMu.Lock()
		if s.cachedLeaderConn != nil {
			s.cachedLeaderConn.Close()
			s.cachedLeaderConn = nil
			s.cachedLeaderID = 0
		}
		s.leaderConnMu.Unlock()

		s.grpcServer.GracefulStop()
		s.wg.Wait()

		if s.raftEngine != nil {
			s.raftEngine.Close()
		}
	})
}

// HasPersistedPDRaftState checks whether the engine has persisted PD Raft state
// for the given cluster. Returns true if a hard state key exists.
func HasPersistedPDRaftState(engine traits.KvEngine, clusterID uint64) bool {
	_, err := engine.Get(cfnames.CFRaft, keys.RaftStateKey(clusterID))
	return err == nil
}

// --- gRPC handlers ---

func (s *PDServer) GetMembers(ctx context.Context, req *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
	if s.raftCfg == nil {
		// Single-node mode: return this node only.
		return &pdpb.GetMembersResponse{
			Header: s.header(),
			Leader: &pdpb.Member{
				Name:       "gookv-pd-1",
				ClientUrls: []string{"http://" + s.Addr()},
			},
		}, nil
	}

	// Raft mode: return all cluster members.
	var members []*pdpb.Member
	var leader *pdpb.Member
	leaderID := s.raftPeer.LeaderID()

	for id, clientAddr := range s.raftCfg.ClientAddrs {
		peerAddr := s.raftCfg.InitialCluster[id]
		m := &pdpb.Member{
			Name:       fmt.Sprintf("gookv-pd-%d", id),
			MemberId:   id,
			ClientUrls: []string{"http://" + clientAddr},
			PeerUrls:   []string{"http://" + peerAddr},
		}
		members = append(members, m)
		if id == leaderID {
			leader = m
		}
	}

	return &pdpb.GetMembersResponse{
		Header:  s.header(),
		Members: members,
		Leader:  leader,
	}, nil
}

func (s *PDServer) Tso(stream pdpb.PD_TsoServer) error {
	if s.raftPeer != nil && !s.raftPeer.IsLeader() {
		return s.forwardTso(stream)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		count := req.GetCount()
		if count == 0 {
			count = 1
		}

		if s.raftPeer == nil {
			// Single-node mode: direct allocation.
			ts, err := s.tso.Allocate(int(count))
			if err != nil {
				return err
			}
			resp := &pdpb.TsoResponse{
				Header:    s.header(),
				Count:     count,
				Timestamp: ts,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		} else {
			// Leader: allocate from buffered TSO (amortized Raft cost).
			ts, err := s.tsoBuffer.GetTS(stream.Context(), int(count))
			if err != nil {
				return err
			}
			resp := &pdpb.TsoResponse{
				Header:    s.header(),
				Count:     count,
				Timestamp: ts,
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
		}
	}
}

func (s *PDServer) Bootstrap(ctx context.Context, req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		resp := &pdpb.BootstrapResponse{Header: s.header()}
		if s.meta.IsBootstrapped() {
			resp.Header = s.errorHeader("cluster already bootstrapped")
			return resp, nil
		}
		store := req.GetStore()
		region := req.GetRegion()
		if store != nil {
			s.meta.PutStore(store)
		}
		if region != nil {
			s.meta.PutRegion(region, nil)
		}
		s.meta.SetBootstrapped(true)
		return resp, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardBootstrap(ctx, req)
	}

	// Leader: validate then propose via Raft.
	if s.meta.IsBootstrapped() {
		return &pdpb.BootstrapResponse{
			Header: s.errorHeader("cluster already bootstrapped"),
		}, nil
	}

	// Atomic bootstrap: combine SetBootstrapped + PutStore + PutRegion
	// into a single Raft proposal to avoid partially-bootstrapped state.
	bTrue := true
	cmd := PDCommand{
		Type:         CmdSetBootstrapped,
		Bootstrapped: &bTrue,
		Store:        req.GetStore(),
		Region:       req.GetRegion(),
	}
	if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
		return nil, err
	}

	return &pdpb.BootstrapResponse{Header: s.header()}, nil
}

func (s *PDServer) IsBootstrapped(ctx context.Context, req *pdpb.IsBootstrappedRequest) (*pdpb.IsBootstrappedResponse, error) {
	return &pdpb.IsBootstrappedResponse{
		Header:       s.header(),
		Bootstrapped: s.meta.IsBootstrapped(),
	}, nil
}

func (s *PDServer) AllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: direct allocation.
		id := s.idAlloc.Alloc()
		return &pdpb.AllocIDResponse{
			Header: s.header(),
			Id:     id,
		}, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardAllocID(ctx, req)
	}

	// Leader: allocate from buffered ID allocator (amortized Raft cost).
	id, err := s.idBuffer.Alloc(ctx)
	if err != nil {
		return nil, err
	}
	return &pdpb.AllocIDResponse{
		Header: s.header(),
		Id:     id,
	}, nil
}

func (s *PDServer) GetStore(ctx context.Context, req *pdpb.GetStoreRequest) (*pdpb.GetStoreResponse, error) {
	store := s.meta.GetStore(req.GetStoreId())
	resp := &pdpb.GetStoreResponse{Header: s.header()}
	if store != nil {
		resp.Store = store
	}
	return resp, nil
}

func (s *PDServer) PutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		s.meta.PutStore(req.GetStore())
		return &pdpb.PutStoreResponse{Header: s.header()}, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardPutStore(ctx, req)
	}

	// Leader: propose via Raft.
	cmd := PDCommand{Type: CmdPutStore, Store: req.GetStore()}
	if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
		return nil, err
	}
	return &pdpb.PutStoreResponse{Header: s.header()}, nil
}

func (s *PDServer) GetAllStores(ctx context.Context, req *pdpb.GetAllStoresRequest) (*pdpb.GetAllStoresResponse, error) {
	stores := s.meta.GetAllStores()
	return &pdpb.GetAllStoresResponse{
		Header: s.header(),
		Stores: stores,
	}, nil
}

func (s *PDServer) StoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		if stats := req.GetStats(); stats != nil {
			s.meta.UpdateStoreStats(stats.GetStoreId(), stats)
		}
		return &pdpb.StoreHeartbeatResponse{Header: s.header()}, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardStoreHeartbeat(ctx, req)
	}

	// Leader: propose via Raft.
	if stats := req.GetStats(); stats != nil {
		cmd := PDCommand{
			Type:       CmdUpdateStoreStats,
			StoreID:    stats.GetStoreId(),
			StoreStats: stats,
		}
		if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
			return nil, err
		}
	}
	return &pdpb.StoreHeartbeatResponse{Header: s.header()}, nil
}

func (s *PDServer) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	if s.raftPeer != nil && !s.raftPeer.IsLeader() {
		return s.forwardRegionHeartbeat(stream)
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		region := req.GetRegion()
		leader := req.GetLeader()

		if s.raftPeer == nil {
			// Single-node mode: existing direct code.
			if region != nil {
				s.meta.PutRegion(region, leader)
			}
		} else {
			// Leader: propose via Raft.
			if region != nil {
				cmd := PDCommand{Type: CmdPutRegion, Region: region, Leader: leader}
				if _, err := s.raftPeer.ProposeAndWait(stream.Context(), cmd); err != nil {
					return err
				}
			}
		}

		resp := &pdpb.RegionHeartbeatResponse{Header: s.header()}

		// Run scheduler to produce commands (on leader or single-node).
		if region != nil && leader != nil && s.scheduler != nil {
			if cmd := s.scheduler.Schedule(region.GetId(), region, leader); cmd != nil {
				if cmd.TransferLeader != nil {
					resp.TransferLeader = cmd.TransferLeader
				}
				if cmd.ChangePeer != nil {
					resp.ChangePeer = cmd.ChangePeer
				}
				if cmd.Merge != nil {
					resp.Merge = cmd.Merge
				}
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *PDServer) GetRegion(ctx context.Context, req *pdpb.GetRegionRequest) (*pdpb.GetRegionResponse, error) {
	region, leader := s.meta.GetRegionByKey(req.GetRegionKey())
	resp := &pdpb.GetRegionResponse{Header: s.header()}
	if region != nil {
		resp.Region = region
		resp.Leader = leader
	}
	return resp, nil
}

func (s *PDServer) GetRegionByID(ctx context.Context, req *pdpb.GetRegionByIDRequest) (*pdpb.GetRegionResponse, error) {
	region, leader := s.meta.GetRegionByID(req.GetRegionId())
	resp := &pdpb.GetRegionResponse{Header: s.header()}
	if region != nil {
		resp.Region = region
		resp.Leader = leader
	}
	return resp, nil
}

func (s *PDServer) AskBatchSplit(ctx context.Context, req *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		resp := &pdpb.AskBatchSplitResponse{Header: s.header()}
		splitCount := int(req.GetSplitCount())
		if splitCount == 0 {
			splitCount = 1
		}
		for i := 0; i < splitCount; i++ {
			newRegionID := s.idAlloc.Alloc()
			var peerIDs []uint64
			for j := 0; j < s.cfg.MaxPeerCount; j++ {
				peerIDs = append(peerIDs, s.idAlloc.Alloc())
			}
			splitID := &pdpb.SplitID{
				NewRegionId: newRegionID,
				NewPeerIds:  peerIDs,
			}
			resp.Ids = append(resp.Ids, splitID)
		}
		return resp, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardAskBatchSplit(ctx, req)
	}

	// Leader: allocate IDs via Raft proposals.
	resp := &pdpb.AskBatchSplitResponse{Header: s.header()}
	splitCount := int(req.GetSplitCount())
	if splitCount == 0 {
		splitCount = 1
	}

	for i := 0; i < splitCount; i++ {
		// Allocate new region ID from buffered allocator.
		newRegionID, err := s.idBuffer.Alloc(ctx)
		if err != nil {
			return nil, err
		}

		// Allocate peer IDs from buffered allocator.
		var peerIDs []uint64
		for j := 0; j < s.cfg.MaxPeerCount; j++ {
			peerID, err := s.idBuffer.Alloc(ctx)
			if err != nil {
				return nil, err
			}
			peerIDs = append(peerIDs, peerID)
		}

		splitID := &pdpb.SplitID{
			NewRegionId: newRegionID,
			NewPeerIds:  peerIDs,
		}
		resp.Ids = append(resp.Ids, splitID)
	}

	return resp, nil
}

func (s *PDServer) ReportBatchSplit(ctx context.Context, req *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		// Set the first peer of each region as the leader. The caller is the
		// leader that executed the split, and it's typically the first peer.
		for _, region := range req.GetRegions() {
			var leader *metapb.Peer
			if len(region.GetPeers()) > 0 {
				leader = region.GetPeers()[0]
			}
			s.meta.PutRegion(region, leader)
		}
		return &pdpb.ReportBatchSplitResponse{Header: s.header()}, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardReportBatchSplit(ctx, req)
	}

	// Leader: propose each region via Raft.
	for _, region := range req.GetRegions() {
		var leader *metapb.Peer
		if len(region.GetPeers()) > 0 {
			leader = region.GetPeers()[0]
		}
		cmd := PDCommand{Type: CmdPutRegion, Region: region, Leader: leader}
		if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
			return nil, err
		}
	}
	return &pdpb.ReportBatchSplitResponse{Header: s.header()}, nil
}

func (s *PDServer) GetGCSafePoint(ctx context.Context, req *pdpb.GetGCSafePointRequest) (*pdpb.GetGCSafePointResponse, error) {
	return &pdpb.GetGCSafePointResponse{
		Header:    s.header(),
		SafePoint: s.gcMgr.GetSafePoint(),
	}, nil
}

func (s *PDServer) UpdateGCSafePoint(ctx context.Context, req *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	if s.raftPeer == nil {
		// Single-node mode: existing direct code.
		newSP := s.gcMgr.UpdateSafePoint(req.GetSafePoint())
		return &pdpb.UpdateGCSafePointResponse{
			Header:       s.header(),
			NewSafePoint: newSP,
		}, nil
	}

	if !s.raftPeer.IsLeader() {
		return s.forwardUpdateGCSafePoint(ctx, req)
	}

	// Leader: propose via Raft.
	cmd := PDCommand{Type: CmdUpdateGCSafePoint, GCSafePoint: req.GetSafePoint()}
	result, err := s.raftPeer.ProposeAndWait(ctx, cmd)
	if err != nil {
		return nil, err
	}
	newSP := binary.BigEndian.Uint64(result)
	return &pdpb.UpdateGCSafePointResponse{
		Header:       s.header(),
		NewSafePoint: newSP,
	}, nil
}

func (s *PDServer) header() *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
	}
}

func (s *PDServer) errorHeader(msg string) *pdpb.ResponseHeader {
	return &pdpb.ResponseHeader{
		ClusterId: s.clusterID,
		Error: &pdpb.Error{
			Message: msg,
		},
	}
}

// --- Helper types ---

// MetadataStore manages cluster metadata in memory.
type MetadataStore struct {
	mu sync.RWMutex

	clusterID    uint64
	bootstrapped bool
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*metapb.Region
	leaders      map[uint64]*metapb.Peer
	storeStats   map[uint64]*pdpb.StoreStats

	// Store liveness tracking.
	storeLastHeartbeat map[uint64]time.Time

	// Store state machine.
	storeStates        map[uint64]StoreState
	disconnectDuration time.Duration
	downDuration       time.Duration

	// nowFunc allows injecting a custom clock for testing.
	nowFunc func() time.Time
}

func NewMetadataStore(clusterID uint64, disconnectDuration, downDuration time.Duration) *MetadataStore {
	if disconnectDuration == 0 {
		disconnectDuration = 30 * time.Second
	}
	if downDuration == 0 {
		downDuration = 30 * time.Minute
	}
	return &MetadataStore{
		clusterID:          clusterID,
		stores:             make(map[uint64]*metapb.Store),
		regions:            make(map[uint64]*metapb.Region),
		leaders:            make(map[uint64]*metapb.Peer),
		storeStats:         make(map[uint64]*pdpb.StoreStats),
		storeLastHeartbeat: make(map[uint64]time.Time),
		storeStates:        make(map[uint64]StoreState),
		disconnectDuration: disconnectDuration,
		downDuration:       downDuration,
		nowFunc:            time.Now,
	}
}

// now returns the current time, using nowFunc for testability.
func (m *MetadataStore) now() time.Time {
	return m.nowFunc()
}

func (m *MetadataStore) IsBootstrapped() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.bootstrapped
}

func (m *MetadataStore) SetBootstrapped(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bootstrapped = v
}

func (m *MetadataStore) PutStore(store *metapb.Store) {
	if store == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stores[store.GetId()] = store
}

func (m *MetadataStore) GetStore(id uint64) *metapb.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.stores[id]
}

func (m *MetadataStore) GetAllStores() []*metapb.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stores := make([]*metapb.Store, 0, len(m.stores))
	for _, s := range m.stores {
		stores = append(stores, s)
	}
	return stores
}

func (m *MetadataStore) UpdateStoreStats(storeID uint64, stats *pdpb.StoreStats) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storeStats[storeID] = stats
	m.storeLastHeartbeat[storeID] = m.now()

	// Heartbeat received: transition back to Up unless Tombstone.
	if state, ok := m.storeStates[storeID]; !ok || state != StoreStateTombstone {
		m.storeStates[storeID] = StoreStateUp
	}
}

// IsStoreAlive returns whether a store is considered alive (Up or Disconnected).
// Kept for backward compatibility; prefer GetStoreState or IsStoreSchedulable.
func (m *MetadataStore) IsStoreAlive(storeID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.storeStates[storeID]
	if !ok {
		// No state recorded yet — check heartbeat directly for backward compat.
		t, hbOK := m.storeLastHeartbeat[storeID]
		if !hbOK {
			return false
		}
		return m.now().Sub(t) < m.disconnectDuration
	}
	return state == StoreStateUp || state == StoreStateDisconnected
}

// GetDeadStores returns store IDs that are in the Down state.
func (m *MetadataStore) GetDeadStores() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var dead []uint64
	for id, state := range m.storeStates {
		if state == StoreStateDown {
			dead = append(dead, id)
		}
	}
	return dead
}

// GetStoreState returns the current state of a store.
func (m *MetadataStore) GetStoreState(storeID uint64) StoreState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.storeStates[storeID]
}

// SetStoreState sets the state for a store (e.g. Tombstone).
func (m *MetadataStore) SetStoreState(storeID uint64, state StoreState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.storeStates[storeID] = state
}

// IsStoreSchedulable returns true only if the store is in the Up state.
func (m *MetadataStore) IsStoreSchedulable(storeID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.storeStates[storeID] == StoreStateUp
}

// updateStoreStates transitions store states based on heartbeat timestamps.
// Called periodically by the store state worker.
func (m *MetadataStore) updateStoreStates() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	for storeID := range m.stores {
		currentState := m.storeStates[storeID]

		// Tombstone is permanent — never change it.
		if currentState == StoreStateTombstone {
			continue
		}

		t, ok := m.storeLastHeartbeat[storeID]
		if !ok {
			// No heartbeat ever received — treat as Down.
			m.storeStates[storeID] = StoreStateDown
			continue
		}

		elapsed := now.Sub(t)
		switch {
		case elapsed >= m.downDuration:
			m.storeStates[storeID] = StoreStateDown
		case elapsed >= m.disconnectDuration:
			m.storeStates[storeID] = StoreStateDisconnected
		default:
			m.storeStates[storeID] = StoreStateUp
		}
	}
}

// GetRegionCountPerStore returns the number of region peers per store.
func (m *MetadataStore) GetRegionCountPerStore() map[uint64]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := make(map[uint64]int)
	for _, region := range m.regions {
		for _, peer := range region.GetPeers() {
			counts[peer.GetStoreId()]++
		}
	}
	return counts
}

// GetLeaderCountPerStore returns the number of region leaders per store.
func (m *MetadataStore) GetLeaderCountPerStore() map[uint64]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	counts := make(map[uint64]int)
	for _, leader := range m.leaders {
		if leader != nil {
			counts[leader.GetStoreId()]++
		}
	}
	return counts
}

// GetAllRegions returns all regions.
func (m *MetadataStore) GetAllRegions() map[uint64]*metapb.Region {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[uint64]*metapb.Region, len(m.regions))
	for k, v := range m.regions {
		result[k] = v
	}
	return result
}

func (m *MetadataStore) PutRegion(region *metapb.Region, leader *metapb.Peer) {
	if region == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	// Reject stale heartbeats: if the stored region has a newer epoch, skip.
	if existing, ok := m.regions[region.GetId()]; ok {
		storedEpoch := existing.GetRegionEpoch()
		incomingEpoch := region.GetRegionEpoch()
		if storedEpoch != nil && incomingEpoch != nil {
			if incomingEpoch.GetVersion() < storedEpoch.GetVersion() ||
				incomingEpoch.GetConfVer() < storedEpoch.GetConfVer() {
				return // stale, discard
			}
		}
	}

	m.regions[region.GetId()] = region
	if leader != nil {
		m.leaders[region.GetId()] = leader
	}
}

func (m *MetadataStore) GetRegionByID(id uint64) (*metapb.Region, *metapb.Peer) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.regions[id], m.leaders[id]
}

func (m *MetadataStore) GetRegionByKey(key []byte) (*metapb.Region, *metapb.Peer) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Encode the raw user key to match the format of region boundaries
	// (which use memcomparable encoding from the split checker).
	encodedKey := codec.EncodeBytes(nil, key)

	// Linear scan for simplicity. In production, use a B-tree.
	for id, region := range m.regions {
		startKey := region.GetStartKey()
		endKey := region.GetEndKey()

		if len(startKey) > 0 && bytes.Compare(encodedKey, startKey) < 0 {
			continue
		}
		if len(endKey) > 0 && bytes.Compare(encodedKey, endKey) >= 0 {
			continue
		}
		return region, m.leaders[id]
	}
	return nil, nil
}

// --- TSO Allocator ---

// TSOAllocator generates monotonically increasing timestamps.
type TSOAllocator struct {
	mu           sync.Mutex
	physical     int64 // milliseconds since epoch
	logical      int64
	saveInterval time.Duration
}

func NewTSOAllocator(saveInterval time.Duration) *TSOAllocator {
	if saveInterval == 0 {
		saveInterval = 3 * time.Second
	}
	return &TSOAllocator{
		saveInterval: saveInterval,
	}
}

// Allocate allocates `count` timestamps and returns the last one.
func (t *TSOAllocator) Allocate(count int) (*pdpb.Timestamp, error) {
	if count <= 0 {
		return nil, fmt.Errorf("tso: count must be positive, got %d", count)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now().UnixMilli()
	if now > t.physical {
		t.physical = now
		t.logical = 0
	}

	t.logical += int64(count)

	// If logical overflows the 18-bit space, advance physical and reset.
	if t.logical >= (1 << 18) {
		// Re-read wall clock to avoid creating timestamps behind the real
		// time when the clock has already advanced past physical+1.
		next := t.physical + 1
		now = time.Now().UnixMilli()
		if now >= next {
			next = now
		}
		t.physical = next
		t.logical = int64(count)
	}

	return &pdpb.Timestamp{
		Physical: t.physical,
		Logical:  t.logical,
	}, nil
}

// --- ID Allocator ---

// IDAllocator provides monotonically increasing unique IDs.
type IDAllocator struct {
	mu     sync.Mutex
	nextID uint64
}

func NewIDAllocator() *IDAllocator {
	return &IDAllocator{nextID: 1000} // Start from 1000 to avoid collisions with bootstrap IDs.
}

// Alloc allocates the next ID.
func (a *IDAllocator) Alloc() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	id := a.nextID
	a.nextID++
	return id
}

// --- GC Safe Point Manager ---

// GCSafePointManager manages the GC safe point.
type GCSafePointManager struct {
	mu        sync.RWMutex
	safePoint uint64
}

func NewGCSafePointManager() *GCSafePointManager {
	return &GCSafePointManager{}
}

func (g *GCSafePointManager) GetSafePoint() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.safePoint
}

func (g *GCSafePointManager) UpdateSafePoint(newSP uint64) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if newSP > g.safePoint {
		g.safePoint = newSP
	}
	return g.safePoint
}
