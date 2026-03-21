package pd

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMoveTracker_FullCycle(t *testing.T) {
	// Full cycle: Adding -> Transferring -> Removing -> complete.
	// Source is leader during the Adding phase.
	tracker := NewMoveTracker()

	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	targetStoreID := uint64(3)

	tracker.StartMove(100, sourcePeer, targetStoreID)
	assert.True(t, tracker.HasPendingMove(100))
	assert.Equal(t, 1, tracker.ActiveMoveCount())

	// Phase 1: Adding — target peer not yet in region.
	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
		},
	}
	leader := &metapb.Peer{Id: 10, StoreId: 1}
	cmd := tracker.Advance(100, region, leader)
	assert.Nil(t, cmd, "should wait while target peer is not yet added")

	// Phase 2: Adding — target peer now present, source is leader.
	// Expect TransferLeader command and transition to Transferring.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should emit TransferLeader")
	assert.NotNil(t, cmd.TransferLeader, "command should be TransferLeader")
	assert.Nil(t, cmd.ChangePeer)
	// Transfer target should be a peer not on store 1 (the source).
	assert.NotEqual(t, uint64(1), cmd.TransferLeader.GetPeer().GetStoreId())

	// Phase 3: Transferring — leader still on source, retry TransferLeader.
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should retry TransferLeader")
	assert.NotNil(t, cmd.TransferLeader)

	// Phase 4: Transferring — leader moved off source. Expect RemoveNode.
	newLeader := &metapb.Peer{Id: 20, StoreId: 2}
	cmd = tracker.Advance(100, region, newLeader)
	require.NotNil(t, cmd, "should emit RemoveNode after leader transfer")
	assert.NotNil(t, cmd.ChangePeer)
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
	assert.Equal(t, uint64(1), cmd.ChangePeer.GetPeer().GetStoreId())

	// Phase 5: Removing — source peer still present, retry RemoveNode.
	cmd = tracker.Advance(100, region, newLeader)
	require.NotNil(t, cmd, "should retry RemoveNode")
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())

	// Phase 6: Removing — source peer gone. Move complete.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	cmd = tracker.Advance(100, region, newLeader)
	assert.Nil(t, cmd, "should return nil when move is complete")
	assert.False(t, tracker.HasPendingMove(100))
	assert.Equal(t, 0, tracker.ActiveMoveCount())
}

func TestMoveTracker_SourceNotLeader(t *testing.T) {
	// Source is not the leader during Adding phase.
	// Should skip Transferring and go directly to Removing.
	tracker := NewMoveTracker()

	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	targetStoreID := uint64(3)
	tracker.StartMove(100, sourcePeer, targetStoreID)

	// Target peer added, but leader is on store 2 (not source).
	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 20, StoreId: 2}

	cmd := tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should emit RemoveNode directly")
	assert.NotNil(t, cmd.ChangePeer)
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
	assert.Equal(t, uint64(1), cmd.ChangePeer.GetPeer().GetStoreId())

	// Verify state is now Removing (source still present -> retry).
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should retry RemoveNode")
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())

	// Source peer removed -> complete.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	cmd = tracker.Advance(100, region, leader)
	assert.Nil(t, cmd, "should return nil when move is complete")
	assert.False(t, tracker.HasPendingMove(100))
}

func TestMoveTracker_StaleCleanup(t *testing.T) {
	tracker := NewMoveTracker()

	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)
	tracker.StartMove(200, sourcePeer, 4)

	assert.Equal(t, 2, tracker.ActiveMoveCount())

	// Manually backdate one move's StartedAt.
	tracker.mu.Lock()
	tracker.moves[100].StartedAt = time.Now().Add(-10 * time.Minute)
	tracker.mu.Unlock()

	// Cleanup with 5-minute timeout should remove region 100 only.
	tracker.CleanupStale(5 * time.Minute)

	assert.Equal(t, 1, tracker.ActiveMoveCount())
	assert.False(t, tracker.HasPendingMove(100), "stale move should be cleaned up")
	assert.True(t, tracker.HasPendingMove(200), "fresh move should remain")
}

func TestMoveTracker_ActiveCount(t *testing.T) {
	tracker := NewMoveTracker()
	assert.Equal(t, 0, tracker.ActiveMoveCount())

	sourcePeer1 := &metapb.Peer{Id: 10, StoreId: 1}
	sourcePeer2 := &metapb.Peer{Id: 20, StoreId: 2}

	tracker.StartMove(100, sourcePeer1, 3)
	assert.Equal(t, 1, tracker.ActiveMoveCount())

	tracker.StartMove(200, sourcePeer2, 4)
	assert.Equal(t, 2, tracker.ActiveMoveCount())

	// Complete move 100: target present, source not leader -> Removing.
	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 30, StoreId: 3}
	tracker.Advance(100, region, leader) // -> Removing, emit RemoveNode

	// Source peer removed.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 30, StoreId: 3},
		},
	}
	tracker.Advance(100, region, leader)
	assert.Equal(t, 1, tracker.ActiveMoveCount(), "count should decrement after move completes")

	// Region 200 still pending.
	assert.True(t, tracker.HasPendingMove(200))
}

func TestMoveTracker_HasPendingMove(t *testing.T) {
	tracker := NewMoveTracker()

	assert.False(t, tracker.HasPendingMove(100), "should return false for unknown region")
	assert.False(t, tracker.HasPendingMove(0), "should return false for zero region")

	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	assert.True(t, tracker.HasPendingMove(100), "should return true after StartMove")
	assert.False(t, tracker.HasPendingMove(200), "should return false for different region")
}
