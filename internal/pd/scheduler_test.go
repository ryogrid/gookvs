package pd

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestScheduler creates a Scheduler with a test MetadataStore.
// All stores are marked Up via a heartbeat so they are schedulable.
func newTestScheduler(maxPeerCount int, storeIDs []uint64) (*Scheduler, *MetadataStore) {
	meta := NewMetadataStore(1, 30*time.Second, 30*time.Minute)
	idAlloc := NewIDAllocator()
	for _, id := range storeIDs {
		meta.PutStore(&metapb.Store{Id: id, Address: "addr"})
		meta.UpdateStoreStats(id, &pdpb.StoreStats{StoreId: id})
	}
	sched := NewScheduler(meta, idAlloc, maxPeerCount, 0.05, 4, nil)
	return sched, meta
}

// putDummyRegions puts n dummy regions on storeID to inflate region counts.
// Each region has a single peer on the given store. Region IDs start from baseRegionID.
func putDummyRegions(meta *MetadataStore, storeID uint64, n int, baseRegionID uint64) {
	for i := 0; i < n; i++ {
		rid := baseRegionID + uint64(i)
		meta.PutRegion(&metapb.Region{
			Id:    rid,
			Peers: []*metapb.Peer{{Id: rid * 10, StoreId: storeID}},
		}, nil)
	}
}

func TestExcessShedding_RemovesMostLoadedPeer(t *testing.T) {
	// 4 stores, maxPeerCount=3.
	// Region 100 has 4 peers on stores 1-4.
	// Store region counts: store1=10, store2=5, store3=3, store4=8.
	sched, meta := newTestScheduler(3, []uint64{1, 2, 3, 4})

	// Inflate region counts per store via dummy regions.
	putDummyRegions(meta, 1, 9, 200)  // store1 will get +1 from region 100 = 10
	putDummyRegions(meta, 2, 4, 300)  // store2 = 5
	putDummyRegions(meta, 3, 2, 400)  // store3 = 3
	putDummyRegions(meta, 4, 7, 500)  // store4 = 8

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
			{Id: 1003, StoreId: 3},
			{Id: 1004, StoreId: 4},
		},
	}
	meta.PutRegion(region, nil)

	leader := &metapb.Peer{Id: 1003, StoreId: 3}
	cmd := sched.scheduleExcessReplicaShedding(100, region, leader)
	require.NotNil(t, cmd, "should produce a shedding command")
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
	// Should remove the peer on store 1 (most regions = 10), not leader (store 3).
	assert.Equal(t, uint64(1), cmd.ChangePeer.GetPeer().GetStoreId())
	assert.Equal(t, uint64(1001), cmd.ChangePeer.GetPeer().GetId())
}

func TestExcessShedding_NoPeersToRemove(t *testing.T) {
	// Region with exactly maxPeerCount peers -> no shedding needed.
	sched, meta := newTestScheduler(3, []uint64{1, 2, 3})

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
			{Id: 1003, StoreId: 3},
		},
	}
	meta.PutRegion(region, nil)

	cmd := sched.scheduleExcessReplicaShedding(100, region, nil)
	assert.Nil(t, cmd, "should not produce a command when peers == maxPeerCount")
}

func TestRegionBalance_MovesToEmpty(t *testing.T) {
	// 3 stores. Store 1 has 10 regions, store 2 has 10, store 3 has 0.
	// A region on stores 1,2 should be balanced to store 3.
	sched, meta := newTestScheduler(3, []uint64{1, 2, 3})

	// Put 9 dummy regions on store 1 (region 100 will be the 10th).
	putDummyRegions(meta, 1, 9, 200)
	// Put 9 dummy regions on store 2 (region 100 will be the 10th).
	putDummyRegions(meta, 2, 9, 300)
	// Store 3 has 0 regions.

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
		},
	}
	meta.PutRegion(region, nil)
	leader := &metapb.Peer{Id: 1001, StoreId: 1}

	cmd := sched.scheduleRegionBalance(100, region, leader)
	require.NotNil(t, cmd, "should produce a balance command")
	assert.Equal(t, eraftpb.ConfChangeType_AddNode, cmd.ChangePeer.GetChangeType())
	assert.Equal(t, uint64(3), cmd.ChangePeer.GetPeer().GetStoreId())
}

func TestRegionBalance_WithinThreshold(t *testing.T) {
	// 3 stores with [10, 10, 9] regions. Mean = ~9.67.
	// 10 > 9.67*1.05 = 10.15? No. So no store is overloaded.
	sched, meta := newTestScheduler(3, []uint64{1, 2, 3})

	putDummyRegions(meta, 1, 10, 200)
	putDummyRegions(meta, 2, 10, 300)
	putDummyRegions(meta, 3, 9, 400)

	region := &metapb.Region{
		Id: 200, // reuse ID 200 — already on store 1
		Peers: []*metapb.Peer{
			{Id: 2000, StoreId: 1},
			{Id: 2001, StoreId: 2},
			{Id: 2002, StoreId: 3},
		},
	}
	leader := &metapb.Peer{Id: 2000, StoreId: 1}

	cmd := sched.scheduleRegionBalance(200, region, leader)
	assert.Nil(t, cmd, "should not balance when within threshold")
}

// mockMoveTracker implements MoveTrackerInterface for testing rate limiting.
type mockMoveTracker struct {
	activeCount int
	pending     map[uint64]bool
	started     []uint64
}

func (m *mockMoveTracker) HasPendingMove(regionID uint64) bool {
	return m.pending[regionID]
}

func (m *mockMoveTracker) ActiveMoveCount() int {
	return m.activeCount
}

func (m *mockMoveTracker) StartMove(regionID uint64, sourcePeer *metapb.Peer, targetStoreID uint64) {
	m.started = append(m.started, regionID)
}

func (m *mockMoveTracker) Advance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
	return nil
}

func TestRegionBalance_RateLimit(t *testing.T) {
	// With moveTracker at rate limit, balance should be skipped.
	meta := NewMetadataStore(1, 30*time.Second, 30*time.Minute)
	idAlloc := NewIDAllocator()
	for _, id := range []uint64{1, 2, 3} {
		meta.PutStore(&metapb.Store{Id: id, Address: "addr"})
		meta.UpdateStoreStats(id, &pdpb.StoreStats{StoreId: id})
	}

	tracker := &mockMoveTracker{
		activeCount: 4, // equal to rate limit
		pending:     make(map[uint64]bool),
	}
	sched := NewScheduler(meta, idAlloc, 3, 0.05, 4, tracker)

	// Set up an imbalanced situation that would normally trigger a move.
	putDummyRegions(meta, 1, 10, 200)
	putDummyRegions(meta, 2, 10, 300)
	// Store 3 has 0 regions.

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
		},
	}
	meta.PutRegion(region, nil)
	leader := &metapb.Peer{Id: 1001, StoreId: 1}

	cmd := sched.scheduleRegionBalance(100, region, leader)
	assert.Nil(t, cmd, "should skip balance when at rate limit")

	// Also test pending move for this specific region.
	tracker.activeCount = 0 // under rate limit
	tracker.pending[100] = true
	cmd = sched.scheduleRegionBalance(100, region, leader)
	assert.Nil(t, cmd, "should skip balance when region has pending move")
}

func TestSchedule_PriorityOrder(t *testing.T) {
	// When excess shedding applies, balance and leader schedulers should NOT run.
	// Set up: 4 peers (maxPeerCount=3) AND an imbalanced leader distribution.
	sched, meta := newTestScheduler(3, []uint64{1, 2, 3, 4})

	// Inflate store 1 to be the most loaded (for shedding to pick it).
	putDummyRegions(meta, 1, 10, 200)
	putDummyRegions(meta, 2, 2, 300)
	putDummyRegions(meta, 3, 2, 400)
	putDummyRegions(meta, 4, 2, 500)

	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
			{Id: 1003, StoreId: 3},
			{Id: 1004, StoreId: 4},
		},
	}
	meta.PutRegion(region, nil)
	leader := &metapb.Peer{Id: 1001, StoreId: 1}

	cmd := sched.Schedule(100, region, leader)
	require.NotNil(t, cmd)
	// Should be an excess shedding command (RemoveNode), not a balance or transfer.
	assert.NotNil(t, cmd.ChangePeer)
	assert.Equal(t, eraftpb.ConfChangeType_RemoveNode, cmd.ChangePeer.GetChangeType())
	assert.Nil(t, cmd.TransferLeader, "leader balance should not run when excess shedding applies")
}

func TestSchedule_PendingMoveBlocksExcessShedding(t *testing.T) {
	// When a move is pending (e.g., in MoveStateStabilizing), the excess
	// shedding scheduler should be blocked to avoid conflicting ConfChanges.
	meta := NewMetadataStore(1, 30*time.Second, 30*time.Minute)
	idAlloc := NewIDAllocator()
	for _, id := range []uint64{1, 2, 3, 4} {
		meta.PutStore(&metapb.Store{Id: id, Address: "addr"})
		meta.UpdateStoreStats(id, &pdpb.StoreStats{StoreId: id})
	}
	tracker := NewMoveTracker()
	sched := NewScheduler(meta, idAlloc, 3, 0.05, 4, tracker)

	putDummyRegions(meta, 1, 5, 200)
	putDummyRegions(meta, 2, 5, 300)
	putDummyRegions(meta, 3, 5, 400)
	putDummyRegions(meta, 4, 5, 500)

	// Region 100 has 4 peers (exceeds maxPeerCount=3).
	region := &metapb.Region{
		Id: 100,
		Peers: []*metapb.Peer{
			{Id: 1001, StoreId: 1},
			{Id: 1002, StoreId: 2},
			{Id: 1003, StoreId: 3},
			{Id: 1004, StoreId: 4},
		},
	}
	meta.PutRegion(region, nil)
	leader := &metapb.Peer{Id: 1001, StoreId: 1}

	// Without a pending move, excess shedding should fire.
	cmd := sched.Schedule(100, region, leader)
	require.NotNil(t, cmd, "should emit RemoveNode from excess shedding")
	assert.NotNil(t, cmd.ChangePeer)

	// Now start a pending move for region 100 (simulates MoveStateStabilizing).
	tracker.StartMove(100, &metapb.Peer{Id: 1001, StoreId: 1}, 4)

	// With a pending move, Schedule should return nil (excess shedding blocked).
	cmd = sched.Schedule(100, region, leader)
	assert.Nil(t, cmd, "should block all schedulers while move is pending")
}
