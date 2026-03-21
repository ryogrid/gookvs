package raftstore

import (
	"context"
	"fmt"
	"log/slog"
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
		RaftBaseTickInterval:     100 * time.Millisecond,
		RaftElectionTimeoutTicks: 10,
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

	// pendingProposals tracks in-flight proposals: index -> callback.
	pendingProposals map[uint64]func([]byte, error)

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

	// State flags.
	stopped     atomic.Bool
	isLeader    atomic.Bool
	initialized bool
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
	}

	rawNode, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, fmt.Errorf("raftstore: new raw node: %w", err)
	}

	if len(peers) > 0 {
		if err := rawNode.Bootstrap(peers); err != nil {
			return nil, fmt.Errorf("raftstore: bootstrap: %w", err)
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
		pendingProposals: make(map[uint64]func([]byte, error)),
		initialized:      true,
	}

	return p, nil
}

// RegionID returns the region ID.
func (p *Peer) RegionID() uint64 { return p.regionID }

// PeerID returns the peer ID.
func (p *Peer) PeerID() uint64 { return p.peerID }

// IsLeader returns whether this peer believes it is the Raft leader.
func (p *Peer) IsLeader() bool { return p.isLeader.Load() }

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

// UpdateRegion replaces the peer's region metadata (e.g., after a split).
func (p *Peer) UpdateRegion(r *metapb.Region) { p.region = r }

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
		}

		p.handleReady()
	}
}

func (p *Peer) handleMessage(msg PeerMsg) {
	switch msg.Type {
	case PeerMsgTypeRaftMessage:
		raftMsg := msg.Data.(*raftpb.Message)
		if err := p.rawNode.Step(*raftMsg); err != nil {
			// Log error but continue — invalid messages are expected
			// during normal operation (e.g., stale messages).
			_ = err
		}

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
		// Drain mailbox.
		close(p.Mailbox)

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
	if cmd.Request == nil {
		if cmd.Callback != nil {
			cmd.Callback(nil)
		}
		return
	}

	data, err := cmd.Request.Marshal()
	if err != nil {
		if cmd.Callback != nil {
			cmd.Callback(nil)
		}
		return
	}

	if err := p.rawNode.Propose(data); err != nil {
		if cmd.Callback != nil {
			cmd.Callback(nil)
		}
		return
	}

	// Track the proposal callback.
	// We use the last index + 1 as the expected index for this proposal.
	lastIdx, _ := p.storage.LastIndex()
	expectedIdx := lastIdx + 1
	if cmd.Callback != nil {
		p.pendingProposals[expectedIdx] = func(_ []byte, _ error) {
			cmd.Callback(nil)
		}
	}
}

func (p *Peer) handleReady() {
	if !p.rawNode.HasReady() {
		return
	}

	rd := p.rawNode.Ready()

	// Update leader status and notify PD on leadership change.
	if rd.SoftState != nil {
		wasLeader := p.isLeader.Load()
		p.isLeader.Store(rd.SoftState.Lead == p.peerID)
		if p.isLeader.Load() && !wasLeader {
			p.sendRegionHeartbeatToPD()
		}
	}

	// Persist entries and hard state.
	if err := p.storage.SaveReady(rd); err != nil {
		// Fatal: persistence failure. In production, this should trigger
		// a panic or store shutdown.
		return
	}

	// Apply snapshot if present.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
			slog.Error("failed to apply snapshot", "region", p.regionID, "err", err)
		}
	}

	// Send Raft messages to other peers.
	if p.sendFunc != nil && len(rd.Messages) > 0 {
		p.sendFunc(rd.Messages)
	}

	// Apply committed entries.
	if len(rd.CommittedEntries) > 0 {
		// Process conf changes first (must be applied via RawNode.ApplyConfChange).
		// This also updates region metadata (peer list, epoch).
		for _, e := range rd.CommittedEntries {
			if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
				p.applyConfChangeEntry(e)
			}
		}
		// Send to apply worker for state machine application.
		if p.applyFunc != nil {
			p.applyFunc(p.regionID, rd.CommittedEntries)
		}
		// Invoke pending proposal callbacks for committed entries.
		for _, e := range rd.CommittedEntries {
			if cb, ok := p.pendingProposals[e.Index]; ok {
				cb(e.Data, nil)
				delete(p.pendingProposals, e.Index)
			}
		}
	}

	// Advance the Raft state machine.
	p.rawNode.Advance(rd)
}

func (p *Peer) onApplyResult(result *ApplyResult) {
	if result == nil {
		return
	}
	// Process results and invoke pending proposal callbacks.
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

	// Don't compact past applied index - 1.
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

	task := split.SplitCheckTask{
		RegionID: p.regionID,
		Region:   p.region,
		StartKey: p.region.GetStartKey(),
		EndKey:   p.region.GetEndKey(),
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
	info := &RegionHeartbeatInfo{
		Region: p.region,
		Peer:   &metapb.Peer{Id: p.peerID, StoreId: p.storeID},
	}
	// Non-blocking send.
	select {
	case p.pdTaskCh <- info:
	default:
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
