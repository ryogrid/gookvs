package raftstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/internal/raftstore/split"
)

// PeerConfig holds configuration for a Peer goroutine.
type PeerConfig struct {
	// RaftBaseTickInterval is the base tick interval for Raft.
	RaftBaseTickInterval time.Duration
	// RaftElectionTimeoutTicks is the election timeout in ticks.
	RaftElectionTimeoutTicks int
	// RaftHeartbeatTicks is the heartbeat interval in ticks.
	RaftHeartbeatTicks int
	// MaxInflightMsgs is the maximum number of in-flight messages.
	MaxInflightMsgs int
	// MaxSizePerMsg is the maximum size of a single Raft message.
	MaxSizePerMsg uint64
	// PreVote enables the PreVote protocol.
	PreVote bool
	// MailboxCapacity is the size of the peer's mailbox channel.
	MailboxCapacity int

	// RaftLogGCTickInterval is how often the log GC tick fires.
	RaftLogGCTickInterval time.Duration
	// RaftLogGCCountLimit triggers compaction when excess entry count exceeds this.
	RaftLogGCCountLimit uint64
	// RaftLogGCSizeLimit triggers compaction when estimated log size exceeds this.
	RaftLogGCSizeLimit uint64
	// RaftLogGCThreshold is the minimum number of entries to keep.
	RaftLogGCThreshold uint64

	// SplitCheckTickInterval is how often the split check tick fires.
	// If zero, split checking is disabled.
	SplitCheckTickInterval time.Duration

	// PdHeartbeatTickInterval is how often the leader sends a region heartbeat to PD.
	// This enables PD's scheduler to run periodically (region balance, move tracking).
	// If zero, region heartbeats are only sent on leadership change.
	PdHeartbeatTickInterval time.Duration
}

// DefaultPeerConfig returns a PeerConfig with sensible defaults.
func DefaultPeerConfig() PeerConfig {
	return PeerConfig{
		RaftBaseTickInterval:     1 * time.Millisecond,
		RaftElectionTimeoutTicks: 50,
		RaftHeartbeatTicks:       2,
		MaxInflightMsgs:          256,
		MaxSizePerMsg:            1 << 20, // 1 MiB
		PreVote:                  true,
		MailboxCapacity:          256,
		RaftLogGCTickInterval:    10 * time.Second,
		RaftLogGCCountLimit:      72000,
		RaftLogGCSizeLimit:       72 * 1024 * 1024, // 72 MiB
		RaftLogGCThreshold:       50,
		SplitCheckTickInterval:   10 * time.Second,
		PdHeartbeatTickInterval:  60 * time.Second,
	}
}

// Peer represents a single region's Raft peer goroutine.
// Each Peer owns its own RawNode and PeerStorage.
type Peer struct {
	regionID uint64
	peerID   uint64
	storeID  uint64
	region   *metapb.Region
	regionMu sync.RWMutex // protects region field (accessed from peer goroutine + gRPC handlers)

	rawNode *raft.RawNode
	storage *PeerStorage
	engine  traits.KvEngine

	cfg PeerConfig

	// Mailbox is the channel for receiving messages.
	Mailbox chan PeerMsg

	// sendFunc is called to send Raft messages to other peers.
	// It is set by the store/router.
	sendFunc func([]raftpb.Message)

	// applyFunc is called to send committed entries for application.
	applyFunc func(regionID uint64, entries []raftpb.Entry)

	// nextProposalID is a per-peer monotonically increasing counter for proposal IDs.
	nextProposalID uint64
	// currentTerm caches the Raft term, updated in handleReady() from HardState.
	currentTerm uint64
	// pendingProposals tracks in-flight proposals: proposalID -> entry.
	pendingProposals map[uint64]proposalEntry

	// raftLogWriter batches Raft log persistence across regions.
	// nil = use inline SaveReady (default/legacy).
	raftLogWriter *RaftLogWriter

	// applyWorkerPool processes committed entries asynchronously.
	// nil = use inline apply (default/legacy).
	applyWorkerPool *ApplyWorkerPool
	// applyInFlight is true when an async apply task is outstanding for this region.
	applyInFlight bool
	// pendingApplyTasks buffers apply tasks when an apply is already in-flight.
	pendingApplyTasks []*ApplyTask

	// raftLogSizeHint tracks estimated size of the Raft log.
	raftLogSizeHint uint64

	// lastCompactedIdx tracks the last index scheduled for GC.
	lastCompactedIdx uint64

	// logGCWorkerCh sends deletion tasks to the background worker.
	// May be nil if no GC worker is configured.
	logGCWorkerCh chan<- RaftLogGCTask

	// pdTaskCh sends PD tasks (e.g., region heartbeats) to the PDWorker.
	// May be nil if PD integration is not configured.
	pdTaskCh chan<- interface{}

	// splitCheckCh sends split check tasks to the SplitCheckWorker.
	// May be nil if split checking is not configured.
	splitCheckCh chan<- split.SplitCheckTask

	// pendingReads tracks in-flight read index requests.
	// Key: string(requestCtx), Value: *pendingRead.
	pendingReads map[string]*pendingRead

	// nextReadID generates unique request contexts for ReadIndex.
	nextReadID atomic.Uint64

	// Leader lease: allows serving reads without ReadIndex round-trip
	// when the leader has recently confirmed its leadership via heartbeats.
	// Lease duration is 80% of election timeout (conservative).
	leaseExpiryNanos atomic.Int64  // Unix nanoseconds; accessed from multiple goroutines
	leaseValid       atomic.Bool
	committedIndex   atomic.Uint64 // updated from peer goroutine only in handleReady()

	// splitResultCh sends split results to the coordinator for child
	// region bootstrapping. Only used by the leader.
	splitResultCh chan<- *SplitRegionResult

	// State flags.
	stopped     atomic.Bool
	isLeader    atomic.Bool
	initialized bool

	// splitOrigin is set for regions created by a split. When this peer
	// becomes leader, it proposes a CompactLog to advance TruncatedIndex,
	// forcing new replicas to receive a snapshot (which includes pre-split
	// engine data not present in the child's Raft log).
	splitOrigin     bool
	splitCompactDone bool
}

// SetApplyState updates the peer's apply state (used by coordinator for
// forcing snapshot on new replicas of split-created regions).
func (p *Peer) SetApplyState(state ApplyState) {
	p.storage.SetApplyState(state)
}

// applyCompactLogEntry processes a committed CompactLog admin entry.
// It advances TruncatedIndex so that future new replicas receive a snapshot
// instead of incomplete Raft log entries.
func (p *Peer) applyCompactLogEntry(e raftpb.Entry) {
	req, ok := unmarshalCompactLogRequest(e.Data)
	if !ok {
		return
	}
	state := p.storage.GetApplyState()
	result, err := execCompactLog(&state, req)
	if err != nil {
		slog.Warn("compact log failed", "region", p.regionID, "err", err)
		return
	}
	if result == nil {
		return // no-op (already compacted)
	}
	p.storage.SetApplyState(state)
	if err := p.storage.PersistApplyState(); err != nil {
		slog.Error("persist apply state after compact log failed", "region", p.regionID, "err", err)
	}
	// Schedule background deletion of compacted entries.
	if p.logGCWorkerCh != nil {
		select {
		case p.logGCWorkerCh <- RaftLogGCTask{
			RegionID: p.regionID,
			StartIdx: result.FirstIndex,
			EndIdx:   result.TruncatedIndex + 1,
		}:
		default:
		}
	}
}

// SetSplitResultCh sets the channel for receiving split results.
func (p *Peer) SetSplitResultCh(ch chan<- *SplitRegionResult) {
	p.splitResultCh = ch
}

// SetRaftLogWriter sets the batch writer for Raft log persistence.
// When set, handleReady submits a WriteTask to the writer instead of
// calling SaveReady directly.
func (p *Peer) SetRaftLogWriter(w *RaftLogWriter) {
	p.raftLogWriter = w
}

// SetApplyWorkerPool sets the async apply pool.
// When set, handleReady submits committed data entries to the pool instead
// of applying them inline. Admin entries are always applied inline.
func (p *Peer) SetApplyWorkerPool(pool *ApplyWorkerPool) {
	p.applyWorkerPool = pool
}

// pendingRead represents a pending read index request waiting for
// appliedIndex to advance past readIndex.
type pendingRead struct {
	readIndex uint64
	callback  func(error)
}

// NewPeer creates a new Peer for the given region.
// peers is the initial list of Raft peers (for bootstrapping).
func NewPeer(
	regionID, peerID, storeID uint64,
	region *metapb.Region,
	engine traits.KvEngine,
	cfg PeerConfig,
	peers []raft.Peer,
) (*Peer, error) {
	storage := NewPeerStorage(regionID, engine)
	var splitOriginFlag bool

	if len(peers) > 0 {
		// For bootstrap, we need truly empty storage (matching MemoryStorage convention).
		// etcd/raft will set the hard state during Bootstrap().
		storage.SetApplyState(ApplyState{
			AppliedIndex:   0,
			TruncatedIndex: 0,
			TruncatedTerm:  0,
		})
		storage.SetPersistedLastIndex(0)
		// Add dummy entry at index 0 (term 0), matching MemoryStorage convention.
		storage.SetDummyEntry()
	} else {
		// Non-bootstrap (restart): recover persisted Raft state from engine
		// BEFORE creating RawNode, so it reads the correct initial state.
		if err := storage.RecoverFromEngine(); err != nil {
			return nil, fmt.Errorf("raftstore: recover from engine: %w", err)
		}
	}

	raftCfg := &raft.Config{
		ID:              peerID,
		ElectionTick:    cfg.RaftElectionTimeoutTicks,
		HeartbeatTick:   cfg.RaftHeartbeatTicks,
		Storage:         storage,
		MaxInflightMsgs: cfg.MaxInflightMsgs,
		MaxSizePerMsg:   cfg.MaxSizePerMsg,
		CheckQuorum:     true,
		PreVote:         cfg.PreVote,
		ReadOnlyOption:  raft.ReadOnlySafe,
	}

	rawNode, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, fmt.Errorf("raftstore: new raw node: %w", err)
	}

	if len(peers) > 0 {
		if err := rawNode.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("raftstore: bootstrap: %w", err)
		}
		// Mark split-created regions so the leader proposes CompactLog after
		// election to force snapshot for future new replicas.
		if len(region.GetStartKey()) > 0 {
			splitOriginFlag = true
		}
	}

	p := &Peer{
		regionID:         regionID,
		peerID:           peerID,
		storeID:          storeID,
		region:           region,
		rawNode:          rawNode,
		storage:          storage,
		engine:           engine,
		cfg:              cfg,
		Mailbox:          make(chan PeerMsg, cfg.MailboxCapacity),
		nextProposalID:   1,
		pendingProposals: make(map[uint64]proposalEntry),
		pendingReads:     make(map[string]*pendingRead),
		initialized:      true,
		splitOrigin:      splitOriginFlag,
	}

	return p, nil
}

// FlushInitialState persists the initial Raft hard state and apply state to
// the engine. This should be called immediately after peer creation (before
// the peer goroutine starts) to ensure that HasPersistedRaftState returns true
// even if the node crashes before the first Ready cycle processes SaveReady.
func (p *Peer) FlushInitialState() error {
	return p.storage.FlushInitialState()
}

// RegionID returns the region ID.
func (p *Peer) RegionID() uint64 { return p.regionID }

// PeerID returns the peer ID.
func (p *Peer) PeerID() uint64 { return p.peerID }

// IsLeader returns whether this peer believes it is the Raft leader.
func (p *Peer) IsLeader() bool { return p.isLeader.Load() }

// GetLeaderPeer returns the metapb.Peer of the current Raft leader, or nil if unknown.
func (p *Peer) GetLeaderPeer() *metapb.Peer {
	lead := p.rawNode.Status().Lead
	if lead == 0 {
		return nil
	}
	p.regionMu.RLock()
	region := p.region
	p.regionMu.RUnlock()
	for _, peer := range region.GetPeers() {
		if peer.GetId() == lead {
			return peer
		}
	}
	return nil
}

// IsStopped returns whether this peer has been stopped.
func (p *Peer) IsStopped() bool { return p.stopped.Load() }

// Storage returns the peer's PeerStorage.
func (p *Peer) Storage() *PeerStorage { return p.storage }

// SetSendFunc sets the function used to send Raft messages.
func (p *Peer) SetSendFunc(f func([]raftpb.Message)) { p.sendFunc = f }

// SetApplyFunc sets the function used to send committed entries for application.
func (p *Peer) SetApplyFunc(f func(uint64, []raftpb.Entry)) { p.applyFunc = f }

// SetPDTaskCh sets the channel for sending PD tasks (region heartbeats).
func (p *Peer) SetPDTaskCh(ch chan<- interface{}) { p.pdTaskCh = ch }

// SetSplitCheckCh sets the channel for sending split check tasks.
func (p *Peer) SetSplitCheckCh(ch chan<- split.SplitCheckTask) { p.splitCheckCh = ch }

// SetSnapTaskCh wires the snapshot generation task channel to PeerStorage.
func (p *Peer) SetSnapTaskCh(ch chan<- GenSnapTask) {
	p.storage.SetSnapTaskCh(ch)
	p.storage.SetRegion(p.region)
}

// NextReadID returns a unique ID for a ReadIndex request.
// Thread-safe: uses atomic counter.
func (p *Peer) NextReadID() uint64 {
	return p.nextReadID.Add(1)
}

// IsLeaseValid returns true if the leader lease is currently valid.
// Thread-safe: uses atomic bool.
func (p *Peer) IsLeaseValid() bool {
	if !p.leaseValid.Load() {
		return false
	}
	if time.Now().UnixNano() > p.leaseExpiryNanos.Load() {
		p.leaseValid.Store(false)
		return false
	}
	return true
}

// IsAppliedCurrent returns true if all committed entries have been applied.
// Safe to call from any goroutine (both fields are atomic/mutex-protected).
func (p *Peer) IsAppliedCurrent() bool {
	return p.storage.AppliedIndex() >= p.committedIndex.Load()
}

// CancelPendingRead removes a timed-out pending read from the map.
// Called by the coordinator when ReadIndex times out, to prevent
// stale entries from accumulating in the peer.
// Thread-safe: sends a cancel message via the mailbox.
func (p *Peer) CancelPendingRead(requestCtx []byte) {
	msg := PeerMsg{
		Type: PeerMsgTypeCancelRead,
		Data: requestCtx,
	}
	select {
	case p.Mailbox <- msg:
	default:
		// Mailbox full — the pending read will be cleaned up eventually
		// when the peer processes other messages.
	}
}

// UpdateRegion replaces the peer's region metadata (e.g., after a split).
// Thread-safe: uses regionMu to protect against concurrent access from gRPC handlers.
func (p *Peer) UpdateRegion(r *metapb.Region) {
	p.regionMu.Lock()
	defer p.regionMu.Unlock()
	p.region = r
}

// Run starts the peer's main event loop. Blocks until the context is cancelled
// or the peer is destroyed.
func (p *Peer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.RaftBaseTickInterval)
	defer ticker.Stop()

	// Optional GC ticker.
	var gcTickerCh <-chan time.Time
	if p.cfg.RaftLogGCTickInterval > 0 {
		gcTicker := time.NewTicker(p.cfg.RaftLogGCTickInterval)
		defer gcTicker.Stop()
		gcTickerCh = gcTicker.C
	}

	// Optional split check ticker.
	var splitCheckTickerCh <-chan time.Time
	if p.cfg.SplitCheckTickInterval > 0 {
		splitCheckTicker := time.NewTicker(p.cfg.SplitCheckTickInterval)
		defer splitCheckTicker.Stop()
		splitCheckTickerCh = splitCheckTicker.C
	}

	// Optional PD region heartbeat ticker.
	var pdHeartbeatTickerCh <-chan time.Time
	if p.cfg.PdHeartbeatTickInterval > 0 {
		pdHeartbeatTicker := time.NewTicker(p.cfg.PdHeartbeatTickInterval)
		defer pdHeartbeatTicker.Stop()
		pdHeartbeatTickerCh = pdHeartbeatTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			p.stopped.Store(true)
			return

		case <-ticker.C:
			p.rawNode.Tick()

		case <-gcTickerCh:
			p.onRaftLogGCTick()

		case <-splitCheckTickerCh:
			p.onSplitCheckTick()

		case <-pdHeartbeatTickerCh:
			if p.isLeader.Load() && p.pdTaskCh != nil {
				p.sendRegionHeartbeatToPD()
			}

		case msg, ok := <-p.Mailbox:
			if !ok {
				p.stopped.Store(true)
				return
			}
			p.handleMessage(msg)
			// Drain additional queued messages (up to 63 more) before
			// calling handleReady. This reduces mailbox pressure and
			// ensures heartbeat responses are processed promptly.
			for i := 0; i < 63; i++ {
				select {
				case msg, ok = <-p.Mailbox:
					if !ok {
						p.stopped.Store(true)
						return
					}
					p.handleMessage(msg)
				default:
					goto drained
				}
			}
		drained:
		}

		if p.stopped.Load() {
			return
		}
		p.handleReady()
	}
}

func (p *Peer) handleMessage(msg PeerMsg) {
	switch msg.Type {
	case PeerMsgTypeRaftMessage:
		raftMsg := msg.Data.(*raftpb.Message)
		func() {
			defer func() {
				if r := recover(); r != nil {
					lastIdx, _ := p.storage.LastIndex()
					slog.Error("[RAFT-PANIC] rawNode.Step panicked",
						"region", p.regionID, "peer", p.peerID,
						"msgType", raftMsg.Type.String(),
						"msgFrom", raftMsg.From, "msgTo", raftMsg.To,
						"msgTerm", raftMsg.Term, "msgCommit", raftMsg.Commit,
						"msgLogTerm", raftMsg.LogTerm, "msgIndex", raftMsg.Index,
						"msgEntriesLen", len(raftMsg.Entries),
						"localLastIndex", lastIdx,
						"panic", r)
				}
			}()
			if err := p.rawNode.Step(*raftMsg); err != nil {
				_ = err
			}
		}()

	case PeerMsgTypeRaftCommand:
		cmd := msg.Data.(*RaftCommand)
		p.propose(cmd)

	case PeerMsgTypeTick:
		p.rawNode.Tick()

	case PeerMsgTypeApplyResult:
		result := msg.Data.(*ApplyResult)
		p.onApplyResult(result)

	case PeerMsgTypeSignificant:
		sig := msg.Data.(*SignificantMsg)
		p.handleSignificantMessage(sig)

	case PeerMsgTypeSchedule:
		sched := msg.Data.(*ScheduleMsg)
		p.handleScheduleMessage(sched)

	case PeerMsgTypeDestroy:
		p.stopped.Store(true)
		// Don't close Mailbox — external goroutines may still send to it.
		// The Run() loop will exit via the stopped flag check.

	case PeerMsgTypeReadIndex:
		req := msg.Data.(*ReadIndexRequest)
		p.handleReadIndexRequest(req)

	case PeerMsgTypeCancelRead:
		ctx := msg.Data.([]byte)
		delete(p.pendingReads, string(ctx))

	default:
		// Unknown message type; ignore.
	}
}

func (p *Peer) handleSignificantMessage(msg *SignificantMsg) {
	switch msg.Type {
	case SignificantMsgTypeUnreachable:
		p.rawNode.ReportUnreachable(msg.ToPeerID)

	case SignificantMsgTypeSnapshotStatus:
		p.rawNode.ReportSnapshot(msg.ToPeerID, msg.Status)

	case SignificantMsgTypeMergeResult:
		slog.Info("merge result received, destroying peer",
			"region", p.regionID, "peer", p.peerID)
		p.stopped.Store(true)
	}
}

func (p *Peer) handleScheduleMessage(msg *ScheduleMsg) {
	if !p.isLeader.Load() {
		return // Only leader executes scheduling.
	}

	switch msg.Type {
	case ScheduleMsgTransferLeader:
		targetPeer := msg.TransferLeader.GetPeer()
		// Find peer ID for the target store.
		for _, peer := range p.region.GetPeers() {
			if peer.GetStoreId() == targetPeer.GetStoreId() {
				p.rawNode.TransferLeader(peer.GetId())
				slog.Info("transfer leader scheduled",
					"region", p.regionID, "to_peer", peer.GetId())
				return
			}
		}

	case ScheduleMsgChangePeer:
		cp := msg.ChangePeer
		changeType := raftpb.ConfChangeAddNode
		if cp.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
			changeType = raftpb.ConfChangeRemoveNode
		}
		if err := p.ProposeConfChange(changeType, cp.GetPeer().GetId(), cp.GetPeer().GetStoreId()); err != nil {
			slog.Warn("change peer scheduling failed", "region", p.regionID, "err", err)
		}

	case ScheduleMsgMerge:
		// Merge is complex; log and skip for initial implementation.
		slog.Info("merge scheduling not yet implemented",
			"region", p.regionID, "target", msg.Merge.GetTarget().GetId())
	}
}

func (p *Peer) propose(cmd *RaftCommand) {
	ProposalTotal.Inc()

	if cmd.Request == nil {
		slog.Warn("propose: nil request", "region", p.regionID)
		return
	}

	proposalID := p.nextProposalID
	p.nextProposalID++

	data, err := cmd.Request.Marshal()
	if err != nil {
		slog.Warn("propose: marshal failed", "region", p.regionID, "err", err)
		if cmd.Callback != nil {
			cmd.Callback(errorResponse(err))
		}
		return
	}

	// Prepend 8-byte proposal ID to data.
	tagged := make([]byte, 8+len(data))
	binary.BigEndian.PutUint64(tagged[:8], proposalID)
	copy(tagged[8:], data)

	if err := p.rawNode.Propose(tagged); err != nil {
		slog.Warn("propose: rawNode.Propose failed", "region", p.regionID, "err", err)
		if cmd.Callback != nil {
			cmd.Callback(errorResponse(err))
		}
		return
	}

	if cmd.Callback != nil {
		p.pendingProposals[proposalID] = proposalEntry{
			callback: cmd.Callback,
			term:     p.currentTerm,
			proposed: time.Now(),
		}
	}
}

func (p *Peer) handleReady() {
	start := time.Now()
	defer func() { HandleReadyDuration.Observe(time.Since(start).Seconds()) }()

	if !p.rawNode.HasReady() {
		return
	}

	rd := p.rawNode.Ready()

	// Update leader status and notify PD on leadership change.
	if rd.SoftState != nil {
		wasLeader := p.isLeader.Load()
		p.isLeader.Store(rd.SoftState.Lead == p.peerID)
		if p.isLeader.Load() && !wasLeader {
			LeaderCount.Inc()
			p.sendRegionHeartbeatToPD()
			// Extend leader lease on leadership confirmation.
			electionTimeout := time.Duration(p.cfg.RaftElectionTimeoutTicks) * p.cfg.RaftBaseTickInterval
			p.leaseExpiryNanos.Store(time.Now().Add(electionTimeout * 4 / 5).UnixNano())
			p.leaseValid.Store(true)
		}
		// Invalidate lease, fail pending proposals, and cleanup pending reads on leader stepdown.
		if !p.isLeader.Load() && wasLeader {
			LeaderCount.Dec()
			p.leaseValid.Store(false)
			p.failAllPendingProposals(fmt.Errorf("leader stepped down"))
		}
		if !p.isLeader.Load() && len(p.pendingReads) > 0 {
			for key, pr := range p.pendingReads {
				pr.callback(fmt.Errorf("not leader for region %d", p.regionID))
				delete(p.pendingReads, key)
			}
		}
	}

	// Cache current term and committed index for cross-goroutine access.
	if !raft.IsEmptyHardState(rd.HardState) {
		p.currentTerm = rd.HardState.Term
		p.committedIndex.Store(rd.HardState.Commit)
	}

	// Persist entries and hard state.
	persistStart := time.Now()
	if p.raftLogWriter != nil {
		// Batch write path: submit to shared writer for I/O coalescing.
		task, err := p.storage.BuildWriteTask(rd)
		if err != nil {
			slog.Error("BuildWriteTask failed", "region", p.regionID, "err", err)
			return
		}
		p.raftLogWriter.Submit(task)
		if err := <-task.Done; err != nil {
			slog.Error("RaftLogWriter batch commit failed", "region", p.regionID, "err", err)
			return
		}
		p.storage.ApplyWriteTaskPostPersist(task)
	} else {
		// Legacy path: inline SaveReady with per-region fsync.
		if err := p.storage.SaveReady(rd); err != nil {
			return
		}
	}
	LogPersistDuration.Observe(time.Since(persistStart).Seconds())

	// Apply snapshot if present.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			slog.Error("failed to apply snapshot", "region", p.regionID, "err", err)
		}
	}

	// Debug: log ConfChange committed entries for ALL regions.
	for _, e := range rd.CommittedEntries {
		if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
			slog.Info("committed ConfChange entry",
				"region", p.regionID, "peer", p.peerID,
				"type", e.Type, "index", e.Index, "term", e.Term)
		}
	}

	// Send Raft messages to other peers.
	if p.sendFunc != nil && len(rd.Messages) > 0 {
		// Debug: trace region 1 messages to new peers.
		if p.regionID == 1 {
			for _, m := range rd.Messages {
				if m.To >= 1000 {
					slog.Info("peer.Ready: region 1 msg to new peer",
						"to", m.To, "type", m.Type, "from", p.peerID)
				}
			}
		}
		p.sendFunc(rd.Messages)
		SendMessageTotal.Add(float64(len(rd.Messages)))
	}

	// Apply committed entries.
	if len(rd.CommittedEntries) > 0 {
		// Process admin commands first (ConfChange, SplitAdmin, CompactLog) before
		// sending data entries to applyFunc. This ensures region metadata
		// is updated BEFORE data entries are applied, maintaining the
		// ordering guarantee that Raft provides.
		for _, e := range rd.CommittedEntries {
			if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
				AdminCmdTotal.WithLabelValues("conf_change").Inc()
				p.applyConfChangeEntry(e)
			} else if e.Type == raftpb.EntryNormal && IsSplitAdmin(e.Data) {
				AdminCmdTotal.WithLabelValues("split").Inc()
				eCopy := e
				p.applySplitAdminEntry(&eCopy)
			} else if e.Type == raftpb.EntryNormal && IsCompactLog(e.Data) {
				AdminCmdTotal.WithLabelValues("compact_log").Inc()
				p.applyCompactLogEntry(e)
			}
		}

		if p.applyWorkerPool != nil {
			// Async path: send data entries to apply pool.
			p.submitToApplyWorker(rd.CommittedEntries)
		} else {
			// Legacy path: inline apply (current behavior).
			p.applyInline(rd.CommittedEntries)
		}
	}

	// Process ReadStates from ReadIndex.
	if len(rd.ReadStates) > 0 && p.isLeader.Load() {
		// ReadStates confirm quorum — extend leader lease.
		electionTimeout := time.Duration(p.cfg.RaftElectionTimeoutTicks) * p.cfg.RaftBaseTickInterval
		p.leaseExpiryNanos.Store(time.Now().Add(electionTimeout * 4 / 5).UnixNano())
		p.leaseValid.Store(true)
	}
	for _, rs := range rd.ReadStates {
		key := string(rs.RequestCtx)
		if pr, ok := p.pendingReads[key]; ok {
			pr.readIndex = rs.Index
			appliedIdx := p.storage.AppliedIndex()
			if appliedIdx >= rs.Index {
				pr.callback(nil)
				delete(p.pendingReads, key)
			}
		}
	}

	// Sweep pendingReads for any that are now satisfiable
	// (applied index may have advanced from committed entries above).
	if len(p.pendingReads) > 0 {
		appliedIdx := p.storage.AppliedIndex()
		for key, pr := range p.pendingReads {
			if pr.readIndex > 0 && appliedIdx >= pr.readIndex {
				pr.callback(nil)
				delete(p.pendingReads, key)
			}
		}
	}

	// For split-created regions: once the leader has applied initial entries,
	// propose CompactLog to advance TruncatedIndex. This forces future new
	// replicas (added by PD rebalance) to receive a snapshot containing
	// pre-split engine data that is not in the child's Raft log.
	if p.splitOrigin && !p.splitCompactDone && p.IsLeader() {
		appliedIdx := p.storage.AppliedIndex()
		if appliedIdx >= 1 {
			req := CompactLogRequest{
				CompactIndex: appliedIdx,
				CompactTerm:  1,
			}
			data := marshalCompactLogRequest(req)
			if err := p.rawNode.Propose(data); err == nil {
				p.splitCompactDone = true
			}
		}
	}

	// Advance the Raft state machine.
	p.rawNode.Advance(rd)
}

// failAllPendingProposals invokes all pending proposal callbacks with the given error
// and clears the pending proposals map. Called on leader stepdown.
func (p *Peer) failAllPendingProposals(err error) {
	for id, entry := range p.pendingProposals {
		entry.callback(errorResponse(err))
		delete(p.pendingProposals, id)
	}
}

// sweepStaleProposals fails proposals that have been pending longer than maxAge.
func (p *Peer) sweepStaleProposals(maxAge time.Duration) {
	now := time.Now()
	for id, entry := range p.pendingProposals {
		if now.Sub(entry.proposed) > maxAge {
			entry.callback(errorResponse(fmt.Errorf("proposal expired")))
			delete(p.pendingProposals, id)
		}
	}
}

// applyInline applies committed entries synchronously (legacy path).
// This preserves the original inline apply behavior when applyWorkerPool is nil.
func (p *Peer) applyInline(committedEntries []raftpb.Entry) {
	applyStart := time.Now()
	defer func() { ApplyDuration.Observe(time.Since(applyStart).Seconds()) }()

	// Filter admin entries out before passing to applyFunc.
	// Only send data entries (non-admin, with 8-byte proposal ID prefix) to the apply worker.
	if p.applyFunc != nil {
		var dataEntries []raftpb.Entry
		for _, e := range committedEntries {
			if e.Type == raftpb.EntryNormal && !IsSplitAdmin(e.Data) && !IsCompactLog(e.Data) && len(e.Data) > 8 {
				dataEntries = append(dataEntries, e)
			}
		}
		if len(dataEntries) > 0 {
			p.applyFunc(p.regionID, dataEntries)
		}
	}

	// Invoke pending proposal callbacks for committed entries.
	// Match by proposal ID embedded in the first 8 bytes of entry data.
	for _, e := range committedEntries {
		if e.Type != raftpb.EntryNormal || len(e.Data) < 8 {
			continue
		}
		proposalID := binary.BigEndian.Uint64(e.Data[:8])
		if proposalID == 0 {
			continue
		}
		if entry, ok := p.pendingProposals[proposalID]; ok {
			if e.Term == entry.term {
				entry.callback(nil) // success
			} else {
				entry.callback(errorResponse(
					fmt.Errorf("term mismatch: proposed in %d, committed in %d",
						entry.term, e.Term)))
			}
			delete(p.pendingProposals, proposalID)
		}
	}

	// Update applied index so ReadIndex can confirm data is readable.
	lastEntry := committedEntries[len(committedEntries)-1]
	p.storage.SetAppliedIndex(lastEntry.Index)

	// Persist ApplyState so it survives restarts.
	if err := p.storage.PersistApplyState(); err != nil {
		slog.Error("APPLY-STATE-PERSIST failed", "error", err, "region", p.regionID)
	}
}

// submitToApplyWorker builds an ApplyTask from committed entries and submits
// it to the apply worker pool for async processing. Admin entries have already
// been processed inline before this method is called.
func (p *Peer) submitToApplyWorker(committedEntries []raftpb.Entry) {
	// Capture LastCommittedIndex from the full committed entries slice
	// (before filtering), including admin entries.
	lastCommittedIndex := committedEntries[len(committedEntries)-1].Index

	// Filter to data entries only.
	var dataEntries []raftpb.Entry
	for _, e := range committedEntries {
		if e.Type == raftpb.EntryNormal && !IsSplitAdmin(e.Data) && !IsCompactLog(e.Data) && len(e.Data) > 8 {
			dataEntries = append(dataEntries, e)
		}
	}

	if len(dataEntries) == 0 {
		// Admin-only batch: update applied index inline.
		// No apply task is submitted, so no ApplyResult will arrive.
		p.storage.SetAppliedIndex(lastCommittedIndex)
		if err := p.storage.PersistApplyState(); err != nil {
			slog.Error("APPLY-STATE-PERSIST failed", "error", err, "region", p.regionID)
		}
		// Sweep pending reads that may now be satisfiable.
		if len(p.pendingReads) > 0 {
			appliedIdx := p.storage.AppliedIndex()
			for key, pr := range p.pendingReads {
				if pr.readIndex > 0 && appliedIdx >= pr.readIndex {
					pr.callback(nil)
					delete(p.pendingReads, key)
				}
			}
		}
		return
	}

	// Extract callbacks for data entries from pendingProposals.
	callbacks := make(map[uint64]proposalEntry)
	for _, e := range dataEntries {
		if e.Type != raftpb.EntryNormal || len(e.Data) < 8 {
			continue
		}
		proposalID := binary.BigEndian.Uint64(e.Data[:8])
		if proposalID == 0 {
			continue
		}
		if entry, ok := p.pendingProposals[proposalID]; ok {
			callbacks[proposalID] = entry
			delete(p.pendingProposals, proposalID)
		}
	}

	task := &ApplyTask{
		RegionID:           p.regionID,
		Entries:            dataEntries,
		ApplyFunc:          p.applyFunc,
		Callbacks:          callbacks,
		CurrentTerm:        p.currentTerm,
		ResultCh:           p.Mailbox,
		LastCommittedIndex: lastCommittedIndex,
	}

	if p.applyInFlight {
		// Previous apply is still in-flight. Buffer this task.
		p.pendingApplyTasks = append(p.pendingApplyTasks, task)
	} else {
		p.applyInFlight = true
		if err := p.applyWorkerPool.Submit(task); err != nil {
			slog.Error("apply worker pool submit failed",
				"region", p.regionID, "err", err)
			p.applyInFlight = false
		}
	}
}

func (p *Peer) onApplyResult(result *ApplyResult) {
	if result == nil {
		return
	}

	// Update applied index from async apply worker.
	if result.AppliedIndex > 0 {
		currentApplied := p.storage.AppliedIndex()
		// Monotonicity guard: never regress the applied index.
		if result.AppliedIndex > currentApplied {
			p.storage.SetAppliedIndex(result.AppliedIndex)

			// Persist apply state (same as current inline path).
			if err := p.storage.PersistApplyState(); err != nil {
				slog.Error("APPLY-STATE-PERSIST failed",
					"error", err, "region", p.regionID)
			}

			// Sweep pending reads that may now be satisfiable.
			if len(p.pendingReads) > 0 {
				for key, pr := range p.pendingReads {
					if pr.readIndex > 0 && result.AppliedIndex >= pr.readIndex {
						pr.callback(nil)
						delete(p.pendingReads, key)
					}
				}
			}
		}
	}

	// Clear in-flight flag so next batch can be submitted.
	p.applyInFlight = false

	// Submit the next queued task, if any (preserves per-region FIFO ordering).
	if len(p.pendingApplyTasks) > 0 {
		next := p.pendingApplyTasks[0]
		p.pendingApplyTasks = p.pendingApplyTasks[1:]
		p.applyInFlight = true
		if err := p.applyWorkerPool.Submit(next); err != nil {
			slog.Error("apply worker pool submit failed (queued task)",
				"region", p.regionID, "err", err)
			p.applyInFlight = false
		}
	}

	// Process exec results (CompactLog, etc.) -- existing logic.
	for _, r := range result.Results {
		switch r.Type {
		case ExecResultTypeCompactLog:
			if clr, ok := r.Data.(*CompactLogResult); ok {
				p.onReadyCompactLog(*clr)
			}
		default:
			// Other result types handled as needed.
		}
	}
}

// onRaftLogGCTick evaluates whether the Raft log should be compacted.
// Only the leader proposes CompactLog commands.
func (p *Peer) onRaftLogGCTick() {
	if !p.isLeader.Load() {
		return
	}

	firstIdx, _ := p.storage.FirstIndex()
	appliedIdx := p.storage.AppliedIndex()

	if appliedIdx <= firstIdx {
		return
	}

	excessCount := appliedIdx - firstIdx
	exceedsCount := excessCount >= p.cfg.RaftLogGCCountLimit
	exceedsSize := p.raftLogSizeHint >= p.cfg.RaftLogGCSizeLimit

	if !exceedsCount && !exceedsSize {
		return
	}

	// Compute compact_idx from follower match indices.
	status := p.rawNode.Status()
	var compactIdx uint64 = appliedIdx

	if len(status.Progress) > 1 {
		// Find the minimum match index across all followers.
		minMatch := appliedIdx
		for id, pr := range status.Progress {
			if id == p.peerID {
				continue
			}
			if pr.Match < minMatch {
				minMatch = pr.Match
			}
		}
		compactIdx = minMatch
	}

	// Don't compact past applied index - 1. Guard against underflow.
	if appliedIdx < 2 {
		return
	}
	if compactIdx > appliedIdx-1 {
		compactIdx = appliedIdx - 1
	}

	// Ensure we keep at least RaftLogGCThreshold entries.
	if compactIdx > firstIdx+p.cfg.RaftLogGCThreshold {
		// compactIdx is already valid — keep as is.
	} else if excessCount >= p.cfg.RaftLogGCCountLimit*3 {
		// Force compact when way over limit, even if followers are slow.
		compactIdx = appliedIdx - 1
	} else {
		return
	}

	if compactIdx <= p.lastCompactedIdx {
		return
	}

	// Get the term at the compact index.
	compactTerm, err := p.storage.Term(compactIdx)
	if err != nil {
		return
	}

	// Propose CompactLog through Raft.
	req := CompactLogRequest{
		CompactIndex: compactIdx,
		CompactTerm:  compactTerm,
	}
	data := marshalCompactLogRequest(req)
	_ = p.rawNode.Propose(data)
}

// onSplitCheckTick sends a split check task to the SplitCheckWorker if this
// peer is the leader and the split check channel is configured.
func (p *Peer) onSplitCheckTick() {
	if !p.isLeader.Load() || p.splitCheckCh == nil {
		return
	}

	p.regionMu.RLock()
	region := p.region
	p.regionMu.RUnlock()
	task := split.SplitCheckTask{
		RegionID: p.regionID,
		Region:   region,
		StartKey: region.GetStartKey(),
		EndKey:   region.GetEndKey(),
		Policy:   split.CheckPolicyScan,
	}

	// Non-blocking send; if the channel is full, skip this round.
	select {
	case p.splitCheckCh <- task:
	default:
	}
}

// onReadyCompactLog handles the apply result of a CompactLog command.
func (p *Peer) onReadyCompactLog(result CompactLogResult) {
	// Update raft log size hint proportionally.
	if result.FirstIndex > 0 {
		totalEntries := p.storage.AppliedIndex() - result.FirstIndex + 1
		remainingEntries := p.storage.AppliedIndex() - result.TruncatedIndex
		if totalEntries > 0 {
			p.raftLogSizeHint = p.raftLogSizeHint * remainingEntries / totalEntries
		}
	}

	// Compact the in-memory entry cache.
	p.storage.CompactTo(result.TruncatedIndex + 1)

	// Schedule physical log deletion.
	p.scheduleRaftLogGC(result.TruncatedIndex + 1)

	p.lastCompactedIdx = result.TruncatedIndex
}

// scheduleRaftLogGC sends a deletion task to the background worker.
func (p *Peer) scheduleRaftLogGC(compactTo uint64) {
	if p.logGCWorkerCh == nil {
		return
	}

	task := RaftLogGCTask{
		RegionID: p.regionID,
		StartIdx: 0,
		EndIdx:   compactTo,
	}

	// Non-blocking send; if the channel is full, skip this round.
	select {
	case p.logGCWorkerCh <- task:
	default:
	}
}

// SetLogGCWorkerCh sets the channel for sending log GC tasks.
func (p *Peer) SetLogGCWorkerCh(ch chan<- RaftLogGCTask) {
	p.logGCWorkerCh = ch
}

// RegionHeartbeatInfo carries region heartbeat data from a Peer to the PDWorker.
type RegionHeartbeatInfo struct {
	Region *metapb.Region
	Peer   *metapb.Peer
}

// sendRegionHeartbeatToPD sends a region heartbeat via the pdTaskCh if configured.
func (p *Peer) sendRegionHeartbeatToPD() {
	if p.pdTaskCh == nil {
		return
	}
	p.regionMu.RLock()
	region := p.region
	p.regionMu.RUnlock()
	info := &RegionHeartbeatInfo{
		Region: region,
		Peer:   &metapb.Peer{Id: p.peerID, StoreId: p.storeID},
	}
	// Non-blocking send.
	select {
	case p.pdTaskCh <- info:
	default:
	}
}

// handleReadIndexRequest handles a linearizable read index request.
func (p *Peer) handleReadIndexRequest(req *ReadIndexRequest) {
	if !p.isLeader.Load() {
		req.Callback(fmt.Errorf("not leader for region %d", p.regionID))
		return
	}
	ReadIndexTotal.Inc()

	// Propose a no-op entry to ensure committedEntryInCurrentTerm() is true.
	// etcd raft postpones ReadIndex until the leader has committed at least
	// one entry in the current term (raft.go:1083). After a leader transfer
	// or split, this may not be the case yet. Proposing nil accelerates the
	// first commit so ReadIndex can proceed.
	_ = p.rawNode.Propose(nil)
	p.rawNode.ReadIndex(req.RequestCtx)
	p.pendingReads[string(req.RequestCtx)] = &pendingRead{
		readIndex: 0,
		callback:  req.Callback,
	}
}

// Propose proposes data to the Raft group.
func (p *Peer) Propose(data []byte) error {
	return p.rawNode.Propose(data)
}

// Campaign triggers an election.
func (p *Peer) Campaign() error {
	return p.rawNode.Campaign()
}

// Status returns the current Raft status.
func (p *Peer) Status() raft.Status {
	return p.rawNode.Status()
}
