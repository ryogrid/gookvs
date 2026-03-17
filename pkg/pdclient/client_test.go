package pdclient

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeStampEncoding(t *testing.T) {
	tests := []struct {
		physical int64
		logical  int64
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{100, 50},
		{1234567890, 12345},
	}

	for _, tc := range tests {
		ts := TimeStamp{Physical: tc.physical, Logical: tc.logical}
		encoded := ts.ToUint64()
		decoded := TimeStampFromUint64(encoded)
		assert.Equal(t, ts, decoded, "round-trip for physical=%d logical=%d", tc.physical, tc.logical)
	}
}

func TestTimeStampMonotonicity(t *testing.T) {
	// Earlier physical time should give smaller uint64.
	ts1 := TimeStamp{Physical: 100, Logical: 0}
	ts2 := TimeStamp{Physical: 200, Logical: 0}
	assert.Less(t, ts1.ToUint64(), ts2.ToUint64())

	// Same physical, earlier logical should give smaller uint64.
	ts3 := TimeStamp{Physical: 100, Logical: 1}
	ts4 := TimeStamp{Physical: 100, Logical: 2}
	assert.Less(t, ts3.ToUint64(), ts4.ToUint64())
}

func TestMockClientGetTS(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	// Get several timestamps and verify monotonicity.
	var prev uint64
	for i := 0; i < 100; i++ {
		ts, err := c.GetTS(ctx)
		require.NoError(t, err)
		curr := ts.ToUint64()
		assert.Greater(t, curr, prev, "timestamp should be monotonically increasing")
		prev = curr
	}
}

func TestMockClientGetTSConcurrent(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	const goroutines = 10
	const perGoroutine = 100

	var mu sync.Mutex
	all := make(map[uint64]bool)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ts, err := c.GetTS(ctx)
				require.NoError(t, err)
				v := ts.ToUint64()
				mu.Lock()
				assert.False(t, all[v], "duplicate timestamp %d", v)
				all[v] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Len(t, all, goroutines*perGoroutine, "should have exactly %d unique timestamps", goroutines*perGoroutine)
}

func TestMockClientBootstrap(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	// Not bootstrapped initially.
	bootstrapped, err := c.IsBootstrapped(ctx)
	require.NoError(t, err)
	assert.False(t, bootstrapped)

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}
	resp, err := c.Bootstrap(ctx, store, region)
	require.NoError(t, err)
	assert.NotNil(t, resp)

	// Should be bootstrapped now.
	bootstrapped, err = c.IsBootstrapped(ctx)
	require.NoError(t, err)
	assert.True(t, bootstrapped)

	// Double bootstrap should fail.
	_, err = c.Bootstrap(ctx, store, region)
	assert.Error(t, err)
}

func TestMockClientStoreOperations(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	store := &metapb.Store{
		Id:      1,
		Address: "127.0.0.1:20160",
		State:   metapb.StoreState_Up,
	}

	// Put store.
	require.NoError(t, c.PutStore(ctx, store))

	// Get store.
	got, err := c.GetStore(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, store.GetAddress(), got.GetAddress())
	assert.Equal(t, store.GetState(), got.GetState())

	// Get nonexistent store.
	_, err = c.GetStore(ctx, 999)
	assert.Error(t, err)
}

func TestMockClientRegionOperations(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	// Set up a region covering [a, z).
	region := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	leader := &metapb.Peer{Id: 1, StoreId: 1}
	c.SetRegion(region, leader)

	// Get region by key.
	gotRegion, gotLeader, err := c.GetRegion(ctx, []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gotRegion.GetId())
	assert.Equal(t, uint64(1), gotLeader.GetId())

	// Key outside range.
	_, _, err = c.GetRegion(ctx, []byte("0"))
	assert.Error(t, err)

	// Get by ID.
	gotRegion, gotLeader, err = c.GetRegionByID(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gotRegion.GetId())

	// Nonexistent region ID.
	_, _, err = c.GetRegionByID(ctx, 999)
	assert.Error(t, err)
}

func TestMockClientRegionUnboundedEndKey(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	// Region with empty end key (unbounded).
	region := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   nil, // unbounded
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}
	c.SetRegion(region, &metapb.Peer{Id: 1, StoreId: 1})

	// Any key >= "a" should match.
	_, _, err := c.GetRegion(ctx, []byte("z"))
	require.NoError(t, err)

	_, _, err = c.GetRegion(ctx, []byte("zzz"))
	require.NoError(t, err)
}

func TestMockClientHeartbeat(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}
	leader := &metapb.Peer{Id: 1, StoreId: 1}

	req := &pdpb.RegionHeartbeatRequest{
		Region: region,
		Leader: leader,
	}
	require.NoError(t, c.ReportRegionHeartbeat(ctx, req))

	// Region should be registered.
	gotRegion, gotLeader, err := c.GetRegionByID(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), gotRegion.GetId())
	assert.Equal(t, uint64(1), gotLeader.GetId())
}

func TestMockClientStoreHeartbeat(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	stats := &pdpb.StoreStats{
		StoreId:       1,
		Capacity:      1024 * 1024 * 1024,
		Available:     512 * 1024 * 1024,
		RegionCount:   10,
		StartTime:     1000,
	}
	require.NoError(t, c.StoreHeartbeat(ctx, stats))
}

func TestMockClientAllocID(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		id, err := c.AllocID(ctx)
		require.NoError(t, err)
		assert.False(t, ids[id], "duplicate ID %d", id)
		ids[id] = true
	}
}

func TestMockClientAskBatchSplit(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	region := &metapb.Region{Id: 1}
	resp, err := c.AskBatchSplit(ctx, region, 3)
	require.NoError(t, err)
	assert.Len(t, resp.GetIds(), 3)

	// Each split ID should have unique region and peer IDs.
	regionIDs := make(map[uint64]bool)
	for _, id := range resp.GetIds() {
		assert.False(t, regionIDs[id.GetNewRegionId()], "duplicate new region ID")
		regionIDs[id.GetNewRegionId()] = true
		assert.Len(t, id.GetNewPeerIds(), 1)
	}
}

func TestMockClientReportBatchSplit(t *testing.T) {
	c := NewMockClient(1)
	ctx := context.Background()

	regions := []*metapb.Region{
		{Id: 10, StartKey: []byte("a"), EndKey: []byte("m"), Peers: []*metapb.Peer{{Id: 1, StoreId: 1}}},
		{Id: 11, StartKey: []byte("m"), EndKey: []byte("z"), Peers: []*metapb.Peer{{Id: 2, StoreId: 1}}},
	}
	require.NoError(t, c.ReportBatchSplit(ctx, regions))

	// Both regions should be registered.
	all := c.GetAllRegions()
	assert.Len(t, all, 2)
}

func TestMockClientGetClusterID(t *testing.T) {
	c := NewMockClient(42)
	assert.Equal(t, uint64(42), c.GetClusterID(context.Background()))
}

func TestMockClientClose(t *testing.T) {
	c := NewMockClient(1)
	c.Close() // Should not panic.
}

func TestContainsKey(t *testing.T) {
	tests := []struct {
		start    string
		end      string
		key      string
		contains bool
	}{
		{"a", "z", "m", true},
		{"a", "z", "a", true},
		{"a", "z", "z", false},  // end is exclusive
		{"a", "z", "0", false},  // before start
		{"", "z", "a", true},    // empty start = unbounded
		{"a", "", "z", true},    // empty end = unbounded
		{"", "", "anything", true}, // both empty = everything
	}

	for _, tc := range tests {
		region := &metapb.Region{
			StartKey: []byte(tc.start),
			EndKey:   []byte(tc.end),
		}
		got := containsKey(region, []byte(tc.key))
		assert.Equal(t, tc.contains, got,
			"containsKey([%s,%s), %s) should be %v", tc.start, tc.end, tc.key, tc.contains)
	}
}
