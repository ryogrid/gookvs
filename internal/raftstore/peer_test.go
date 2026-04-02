package raftstore

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeerBootstrap(t *testing.T) {
	engine := newTestEngine(t)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	peers := []raft.Peer{{ID: 1}}
	cfg := DefaultPeerConfig()
	cfg.RaftBaseTickInterval = 50 * time.Millisecond

	p, err := NewPeer(1, 1, 1, region, engine, cfg, peers)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), p.RegionID())
	assert.Equal(t, uint64(1), p.PeerID())
	assert.False(t, p.IsStopped())
}

func TestPeerRunAndStop(t *testing.T) {
	engine := newTestEngine(t)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	peers := []raft.Peer{{ID: 1}}
	cfg := DefaultPeerConfig()
	cfg.RaftBaseTickInterval = 50 * time.Millisecond

	p, err := NewPeer(1, 1, 1, region, engine, cfg, peers)
	require.NoError(t, err)

	// Collect sent messages.
	var sentMsgs []raftpb.Message
	p.SetSendFunc(func(msgs []raftpb.Message) {
		sentMsgs = append(sentMsgs, msgs...)
	})

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()

	// Let it tick a few times.
	time.Sleep(200 * time.Millisecond)

	// Stop the peer.
	cancel()

	select {
	case <-done:
		assert.True(t, p.IsStopped())
	case <-time.After(2 * time.Second):
		t.Fatal("peer did not stop in time")
	}
}

func TestPeerSingleNodeElection(t *testing.T) {
	engine := newTestEngine(t)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	peers := []raft.Peer{{ID: 1}}
	cfg := DefaultPeerConfig()
	cfg.RaftBaseTickInterval = 20 * time.Millisecond

	p, err := NewPeer(1, 1, 1, region, engine, cfg, peers)
	require.NoError(t, err)

	var appliedEntries []raftpb.Entry
	p.SetApplyFunc(func(regionID uint64, entries []raftpb.Entry) {
		appliedEntries = append(appliedEntries, entries...)
	})
	p.SetSendFunc(func(msgs []raftpb.Message) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Run(ctx)

	// Wait for the peer loop to process the bootstrap Ready (which contains
	// the conf change entry) and potentially self-elect.
	time.Sleep(200 * time.Millisecond)

	// Campaign to become leader (skip if already leader due to auto-election).
	if !p.IsLeader() {
		require.NoError(t, p.Campaign())
	}

	// Wait for election to complete.
	time.Sleep(300 * time.Millisecond)

	// Single node should become leader.
	assert.True(t, p.IsLeader(), "single node should become leader")

	// Verify Raft status.
	status := p.Status()
	assert.Equal(t, uint64(1), status.Lead)
}

func TestPeerProposeAndApply(t *testing.T) {
	engine := newTestEngine(t)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	peers := []raft.Peer{{ID: 1}}
	cfg := DefaultPeerConfig()
	cfg.RaftBaseTickInterval = 20 * time.Millisecond

	p, err := NewPeer(1, 1, 1, region, engine, cfg, peers)
	require.NoError(t, err)

	var appliedEntries []raftpb.Entry
	p.SetApplyFunc(func(regionID uint64, entries []raftpb.Entry) {
		appliedEntries = append(appliedEntries, entries...)
	})
	p.SetSendFunc(func(msgs []raftpb.Message) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Run(ctx)

	// Wait for bootstrap Ready to be processed and potential auto-election.
	time.Sleep(200 * time.Millisecond)

	// Become leader first (skip Campaign if already leader due to auto-election).
	if !p.IsLeader() {
		require.NoError(t, p.Campaign())
		time.Sleep(300 * time.Millisecond)
	}
	require.True(t, p.IsLeader())

	// Propose some data via the raw Propose() API.
	// Since handleReady now filters entries for applyFunc (only entries with
	// 8-byte proposal ID prefix pass), raw Propose() entries are filtered out.
	// We pad each entry with a zero 8-byte prefix so they pass the filter.
	helloData := append(make([]byte, 8), []byte("hello")...)
	worldData := append(make([]byte, 8), []byte("world")...)
	require.NoError(t, p.Propose(helloData))
	require.NoError(t, p.Propose(worldData))

	// Wait for entries to be applied.
	time.Sleep(300 * time.Millisecond)

	// Should have applied entries (at least the proposals + conf change entry).
	assert.True(t, len(appliedEntries) >= 2, "should have applied at least 2 entries, got %d", len(appliedEntries))

	// Find our data among applied entries (data includes 8-byte prefix).
	found := 0
	for _, e := range appliedEntries {
		if len(e.Data) > 8 {
			payload := string(e.Data[8:])
			if payload == "hello" || payload == "world" {
				found++
			}
		}
	}
	assert.Equal(t, 2, found, "both proposals should be applied")
}

func TestPeerMessageHandling(t *testing.T) {
	engine := newTestEngine(t)

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	peers := []raft.Peer{{ID: 1}}
	cfg := DefaultPeerConfig()
	cfg.RaftBaseTickInterval = 20 * time.Millisecond

	p, err := NewPeer(1, 1, 1, region, engine, cfg, peers)
	require.NoError(t, err)

	p.SetSendFunc(func(msgs []raftpb.Message) {})
	p.SetApplyFunc(func(regionID uint64, entries []raftpb.Entry) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go p.Run(ctx)

	// Send a tick message.
	p.Mailbox <- PeerMsg{Type: PeerMsgTypeTick}

	// Should not panic or hang.
	time.Sleep(100 * time.Millisecond)
}

func TestDefaultPeerConfig(t *testing.T) {
	cfg := DefaultPeerConfig()
	assert.Equal(t, 1*time.Millisecond, cfg.RaftBaseTickInterval)
	assert.Equal(t, 50, cfg.RaftElectionTimeoutTicks)
	assert.Equal(t, 2, cfg.RaftHeartbeatTicks)
	assert.Equal(t, 256, cfg.MaxInflightMsgs)
	assert.Equal(t, uint64(1<<20), cfg.MaxSizePerMsg)
	assert.True(t, cfg.PreVote)
	assert.Equal(t, 256, cfg.MailboxCapacity)
}
