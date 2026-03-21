package pd

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClock provides a controllable clock for testing time-dependent logic.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock {
	return &testClock{now: time.Now()}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestMetadataStore(disconnectDur, downDur time.Duration) (*MetadataStore, *testClock) {
	clock := newTestClock()
	meta := NewMetadataStore(1, disconnectDur, downDur)
	meta.nowFunc = clock.Now
	return meta, clock
}

func TestStoreState_Transitions(t *testing.T) {
	disconnectDur := 30 * time.Second
	downDur := 5 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	// Register store and send initial heartbeat.
	meta.PutStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})

	// Immediately after heartbeat, state should be Up.
	assert.Equal(t, StoreStateUp, meta.GetStoreState(1))

	// Advance past disconnect threshold, run state update.
	clock.Advance(disconnectDur + 1*time.Second)
	meta.updateStoreStates()
	assert.Equal(t, StoreStateDisconnected, meta.GetStoreState(1))

	// Advance past down threshold, run state update.
	clock.Advance(downDur) // total elapsed > downDur from last heartbeat
	meta.updateStoreStates()
	assert.Equal(t, StoreStateDown, meta.GetStoreState(1))
}

func TestStoreState_HeartbeatResetsToUp(t *testing.T) {
	disconnectDur := 30 * time.Second
	downDur := 5 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	meta.PutStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})

	// Move to Disconnected state.
	clock.Advance(disconnectDur + 1*time.Second)
	meta.updateStoreStates()
	require.Equal(t, StoreStateDisconnected, meta.GetStoreState(1))

	// Heartbeat arrives — should reset to Up.
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})
	assert.Equal(t, StoreStateUp, meta.GetStoreState(1))

	// Also move to Down state and verify heartbeat resets.
	clock.Advance(downDur + 1*time.Second)
	meta.updateStoreStates()
	require.Equal(t, StoreStateDown, meta.GetStoreState(1))

	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})
	assert.Equal(t, StoreStateUp, meta.GetStoreState(1))
}

func TestStoreState_TombstonePermanent(t *testing.T) {
	disconnectDur := 30 * time.Second
	downDur := 5 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	meta.PutStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})

	// Set to Tombstone.
	meta.SetStoreState(1, StoreStateTombstone)
	assert.Equal(t, StoreStateTombstone, meta.GetStoreState(1))

	// updateStoreStates should NOT change Tombstone.
	_ = clock // keep clock reference; advance is not needed but won't hurt
	meta.updateStoreStates()
	assert.Equal(t, StoreStateTombstone, meta.GetStoreState(1))

	// Even a heartbeat should NOT change Tombstone.
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})
	assert.Equal(t, StoreStateTombstone, meta.GetStoreState(1))
}

func TestIsStoreSchedulable(t *testing.T) {
	disconnectDur := 30 * time.Second
	downDur := 5 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	meta.PutStore(&metapb.Store{Id: 1, Address: "addr"})
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})

	// Up => schedulable.
	assert.True(t, meta.IsStoreSchedulable(1))

	// Disconnected => not schedulable.
	clock.Advance(disconnectDur + 1*time.Second)
	meta.updateStoreStates()
	assert.False(t, meta.IsStoreSchedulable(1))

	// Down => not schedulable.
	clock.Advance(downDur)
	meta.updateStoreStates()
	assert.False(t, meta.IsStoreSchedulable(1))

	// Tombstone => not schedulable.
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1}) // reset to Up first
	meta.SetStoreState(1, StoreStateTombstone)
	assert.False(t, meta.IsStoreSchedulable(1))
}

func TestGetDeadStores_ReturnsDownOnly(t *testing.T) {
	disconnectDur := 10 * time.Second
	downDur := 1 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	// Set up 3 stores.
	for i := uint64(1); i <= 3; i++ {
		meta.PutStore(&metapb.Store{Id: i, Address: "addr"})
		meta.UpdateStoreStats(i, &pdpb.StoreStats{StoreId: i})
	}

	// All up, no dead stores.
	dead := meta.GetDeadStores()
	assert.Empty(t, dead)

	// Advance past disconnect but not down — still no dead.
	clock.Advance(disconnectDur + 1*time.Second)
	meta.updateStoreStates()
	dead = meta.GetDeadStores()
	assert.Empty(t, dead)

	// Advance past down threshold.
	clock.Advance(downDur)
	meta.updateStoreStates()
	dead = meta.GetDeadStores()
	assert.Len(t, dead, 3)
}

func TestIsStoreAlive_BackwardCompat(t *testing.T) {
	disconnectDur := 10 * time.Second
	downDur := 1 * time.Minute

	meta, clock := newTestMetadataStore(disconnectDur, downDur)

	meta.PutStore(&metapb.Store{Id: 1, Address: "addr"})
	meta.UpdateStoreStats(1, &pdpb.StoreStats{StoreId: 1})

	// Up => alive.
	assert.True(t, meta.IsStoreAlive(1))

	// Disconnected => still alive (might come back).
	clock.Advance(disconnectDur + 1*time.Second)
	meta.updateStoreStates()
	assert.True(t, meta.IsStoreAlive(1))

	// Down => not alive.
	clock.Advance(downDur)
	meta.updateStoreStates()
	assert.False(t, meta.IsStoreAlive(1))

	// Unknown store => not alive.
	assert.False(t, meta.IsStoreAlive(999))
}

func TestGetRegionCountPerStore(t *testing.T) {
	meta := NewMetadataStore(1, 30*time.Second, 30*time.Minute)

	meta.PutRegion(&metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 2, StoreId: 2},
			{Id: 3, StoreId: 3},
		},
	}, nil)
	meta.PutRegion(&metapb.Region{
		Id: 2,
		Peers: []*metapb.Peer{
			{Id: 4, StoreId: 1},
			{Id: 5, StoreId: 2},
		},
	}, nil)

	counts := meta.GetRegionCountPerStore()
	assert.Equal(t, 2, counts[1])
	assert.Equal(t, 2, counts[2])
	assert.Equal(t, 1, counts[3])
}
