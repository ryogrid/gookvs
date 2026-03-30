package pd

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// advanceStabilizing calls Advance (stabilizeHeartbeats - 1) times to drive
// through the MoveStateStabilizing phase WITHOUT triggering the final command.
// The caller should call Advance one more time to get the actual command.
func advanceStabilizing(t *testing.T, tracker *MoveTracker, regionID uint64, region *metapb.Region, leader *metapb.Peer) {
	t.Helper()
	for i := 0; i < stabilizeHeartbeats-1; i++ {
		cmd := tracker.Advance(regionID, region, leader)
		assert.Nil(t, cmd, "should return nil during stabilization (cycle %d)", i+1)
	}
}

func TestMoveTracker_FullCycle(t *testing.T) {
	// Full cycle: Adding -> Stabilizing -> Transferring -> Removing -> complete.
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

	// Phase 2: Adding — target peer now present → transitions to Stabilizing.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	cmd = tracker.Advance(100, region, leader)
	assert.Nil(t, cmd, "should transition to Stabilizing and return nil")

	// Phase 3: Stabilizing — wait for stabilizeHeartbeats cycles, then emit TransferLeader.
	for i := 0; i < stabilizeHeartbeats-1; i++ {
		cmd = tracker.Advance(100, region, leader)
		assert.Nil(t, cmd, "should still be stabilizing (cycle %d)", i+1)
	}
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should emit TransferLeader after stabilization")
	assert.NotNil(t, cmd.TransferLeader, "command should be TransferLeader")
	assert.Nil(t, cmd.ChangePeer)
	assert.NotEqual(t, uint64(1), cmd.TransferLeader.GetPeer().GetStoreId())

	// Phase 4: Transferring — leader still on source, retry TransferLeader.
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should retry TransferLeader")
	assert.NotNil(t, cmd.TransferLeader)

	// Phase 5: Transferring — leader moved off source. Expect RemoveNode.
	newLeader := &metapb.Peer{Id: 20, StoreId: 2}
	cmd = tracker.Advance(100, region, newLeader)
	require.NotNil(t, cmd, "should emit RemoveNode after leader transfer")
	assert.NotNil(t, cmd.ChangePeer)
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
	assert.Equal(t, uint64(1), cmd.ChangePeer.GetPeer().GetStoreId())

	// Phase 6: Removing — source peer still present, retry RemoveNode.
	cmd = tracker.Advance(100, region, newLeader)
	require.NotNil(t, cmd, "should retry RemoveNode")
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())

	// Phase 7: Removing — source peer gone. Move complete.
	region = &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	cmd = tracker.Advance(100, region, newLeader)
	assert.Nil(t, cmd, "should return nil when move is complete")
	// Move is complete but region is in cooldown — HasPendingMove still true.
	assert.True(t, tracker.HasPendingMove(100), "should be in cooldown after completion")
	assert.Equal(t, 0, tracker.ActiveMoveCount())
}

func TestMoveTracker_SourceNotLeader(t *testing.T) {
	// Source is not the leader during Adding phase.
	// After stabilization, should skip Transferring and go directly to Removing.
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

	// Adding → Stabilizing (peer found).
	cmd := tracker.Advance(100, region, leader)
	assert.Nil(t, cmd, "should transition to Stabilizing")

	// Drive through stabilization.
	advanceStabilizing(t, tracker, 100, region, leader)

	// After stabilization: source not leader → emit RemoveNode directly.
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd, "should emit RemoveNode after stabilization")
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
	assert.True(t, tracker.HasPendingMove(100), "should be in cooldown after completion")
}

func TestMoveTracker_CooldownExpiry(t *testing.T) {
	// After a move completes, HasPendingMove should return true during cooldown
	// and false after cooldown expires.
	tracker := NewMoveTracker()
	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 20, StoreId: 2}

	// Adding → Stabilizing.
	tracker.Advance(100, region, leader)
	// Drive through stabilization.
	advanceStabilizing(t, tracker, 100, region, leader)
	// Stabilization → Removing (source not leader).
	tracker.Advance(100, region, leader)
	// Source peer removed → complete + cooldown.
	regionAfter := &metapb.Region{
		Id:    100,
		Peers: []*metapb.Peer{{Id: 20, StoreId: 2}, {Id: 30, StoreId: 3}},
	}
	tracker.Advance(100, regionAfter, leader)

	// Should be in cooldown.
	assert.True(t, tracker.HasPendingMove(100), "should be in cooldown")
	assert.Equal(t, 0, tracker.ActiveMoveCount())

	// Advance enough times to expire the cooldown (each Advance increments heartbeatCount).
	// Need to advance with a different region to tick the counter without affecting region 100's move.
	for i := 0; i < moveCooldownHeartbeats+1; i++ {
		tracker.Advance(999, &metapb.Region{Id: 999}, nil) // tick counter
	}

	// Cooldown should have expired.
	assert.False(t, tracker.HasPendingMove(100), "cooldown should have expired")
}

func TestMoveTracker_StabilizingWait(t *testing.T) {
	// Verify the exact number of Advance calls during stabilization.
	tracker := NewMoveTracker()
	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 20, StoreId: 2} // not source

	// Advance 1 (from Adding): peer found → Stabilizing, returns nil.
	cmd := tracker.Advance(100, region, leader)
	assert.Nil(t, cmd)

	// Advance 2-4 (Stabilizing): count 1,2,3.
	for i := 1; i <= stabilizeHeartbeats; i++ {
		cmd = tracker.Advance(100, region, leader)
		if i < stabilizeHeartbeats {
			assert.Nil(t, cmd, "stabilizing cycle %d should return nil", i)
		} else {
			// Last stabilizing cycle → emit command.
			require.NotNil(t, cmd, "should emit command after %d stabilizing cycles", stabilizeHeartbeats)
		}
	}

	// Should have emitted RemoveNode (source not leader).
	assert.NotNil(t, cmd.ChangePeer)
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
}

func TestMoveTracker_StabilizingWithLeaderOnSource(t *testing.T) {
	tracker := NewMoveTracker()
	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 10, StoreId: 1} // source IS leader

	// Adding → Stabilizing.
	cmd := tracker.Advance(100, region, leader)
	assert.Nil(t, cmd)

	// Drive through stabilization.
	advanceStabilizing(t, tracker, 100, region, leader)

	// After stabilization with leader on source → should emit TransferLeader.
	cmd = tracker.Advance(100, region, leader)
	require.NotNil(t, cmd)
	assert.NotNil(t, cmd.TransferLeader, "should emit TransferLeader when source is leader")
	assert.Nil(t, cmd.ChangePeer)
}

func TestMoveTracker_NilRegionDuringStabilizing(t *testing.T) {
	tracker := NewMoveTracker()
	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 20, StoreId: 2}

	// Adding → Stabilizing.
	tracker.Advance(100, region, leader)

	// Call Advance with nil region — should not panic.
	cmd := tracker.Advance(100, nil, leader)
	assert.Nil(t, cmd, "nil region should return nil without panic")

	// Move should still be pending.
	assert.True(t, tracker.HasPendingMove(100))
}

func TestMoveTracker_StaleCleanupDuringStabilizing(t *testing.T) {
	tracker := NewMoveTracker()
	sourcePeer := &metapb.Peer{Id: 10, StoreId: 1}
	tracker.StartMove(100, sourcePeer, 3)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 20, StoreId: 2},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 20, StoreId: 2}

	// Advance to Stabilizing.
	tracker.Advance(100, region, leader)

	// Backdate StartedAt to simulate a long-running move.
	tracker.mu.Lock()
	tracker.moves[100].StartedAt = time.Now().Add(-10 * time.Minute)
	tracker.mu.Unlock()

	// Cleanup with 5-minute timeout should remove the stale stabilizing move.
	tracker.CleanupStale(5 * time.Minute)
	assert.False(t, tracker.HasPendingMove(100), "stale move in stabilizing state should be cleaned up")
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

	// Complete move 100: target present, source not leader.
	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 10, StoreId: 1},
			{Id: 30, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 30, StoreId: 3}

	// Adding → Stabilizing.
	tracker.Advance(100, region, leader)
	// Drive through stabilization.
	for i := 0; i < stabilizeHeartbeats; i++ {
		tracker.Advance(100, region, leader)
	}
	// Stabilization complete → Removing, emit RemoveNode.
	tracker.Advance(100, region, leader)

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

func TestMoveState_String(t *testing.T) {
	assert.Equal(t, "Adding", MoveStateAdding.String())
	assert.Equal(t, "Stabilizing", MoveStateStabilizing.String())
	assert.Equal(t, "Transferring", MoveStateTransferring.String())
	assert.Equal(t, "Removing", MoveStateRemoving.String())
	assert.Equal(t, "Unknown", MoveState(99).String())
}
