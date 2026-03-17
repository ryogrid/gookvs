package raftstore

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookvs/internal/engine/traits"
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

	// For bootstrap, we need truly empty storage (matching MemoryStorage convention).
	// etcd/raft will set the hard state during Bootstrap().
	if len(peers) > 0 {
		storage.SetApplyState(ApplyState{
			AppliedIndex:   0,
			TruncatedIndex: 0,
			TruncatedTerm:  0,
		})
		storage.SetPersistedLastIndex(0)
		// Add dummy entry at index 0 (term 0), matching MemoryStorage convention.
		storage.SetDummyEntry()
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

// Run starts the peer's main event loop. Blocks until the context is cancelled
// or the peer is destroyed.
func (p *Peer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.RaftBaseTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.stopped.Store(true)
			return

		case <-ticker.C:
			p.rawNode.Tick()

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

	case PeerMsgTypeDestroy:
		p.stopped.Store(true)
		// Drain mailbox.
		close(p.Mailbox)

	default:
		// Unknown message type; ignore.
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

	// Update leader status.
	if rd.SoftState != nil {
		p.isLeader.Store(rd.SoftState.Lead == p.peerID)
	}

	// Persist entries and hard state.
	if err := p.storage.SaveReady(rd); err != nil {
		// Fatal: persistence failure. In production, this should trigger
		// a panic or store shutdown.
		return
	}

	// Send Raft messages to other peers.
	if p.sendFunc != nil && len(rd.Messages) > 0 {
		p.sendFunc(rd.Messages)
	}

	// Apply committed entries.
	if len(rd.CommittedEntries) > 0 {
		// Process conf changes first (must be applied via RawNode.ApplyConfChange).
		for _, e := range rd.CommittedEntries {
			if e.Type == raftpb.EntryConfChange {
				var cc raftpb.ConfChange
				if err := cc.Unmarshal(e.Data); err == nil {
					p.rawNode.ApplyConfChange(cc)
				}
			} else if e.Type == raftpb.EntryConfChangeV2 {
				var cc raftpb.ConfChangeV2
				if err := cc.Unmarshal(e.Data); err == nil {
					p.rawNode.ApplyConfChange(cc)
				}
			}
		}
		// Send to apply worker for state machine application.
		if p.applyFunc != nil {
			p.applyFunc(p.regionID, rd.CommittedEntries)
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
		_ = r // Process specific result types if needed.
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
