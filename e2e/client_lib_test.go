package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/ryogrid/gookv/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clientTestCluster wraps a multi-region cluster with a client library instance.
type clientTestCluster struct {
	t      *testing.T
	mc     *multiRegionCluster
	client *client.RawKVClient
	top    *client.Client
}

// newClientTestCluster creates a multi-region cluster and connects a client library.
func newClientTestCluster(t *testing.T, regionDefs []multiRegionDef) *clientTestCluster {
	t.Helper()

	mc := newMultiRegionCluster(t, regionDefs)

	// Wait for leaders on all regions.
	time.Sleep(500 * time.Millisecond)
	for _, r := range mc.regions {
		mc.campaignIfNeeded(r.GetId())
	}
	for _, r := range mc.regions {
		mc.waitForRegionLeader(r.GetId(), 15*time.Second)
	}

	// Create client connected to PD.
	ctx := context.Background()
	topClient, err := client.NewClient(ctx, client.Config{
		PDAddrs:       []string{mc.pdAddr},
		DialTimeout:   5 * time.Second,
		MaxRetries:    5,
		StoreCacheTTL: 30 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { topClient.Close() })

	return &clientTestCluster{
		t:      t,
		mc:     mc,
		client: topClient.RawKV(),
		top:    topClient,
	}
}

// --- Region Cache Tests ---

func TestClientRegionCacheMiss(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: nil},
	})
	ctx := context.Background()

	// Cold cache: Put and Get should succeed by querying PD.
	err := tc.client.Put(ctx, []byte("key1"), []byte("val1"))
	require.NoError(t, err)

	val, notFound, err := tc.client.Get(ctx, []byte("key1"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("val1"), val)
}

func TestClientRegionCacheHit(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: nil},
	})
	ctx := context.Background()

	// First put populates cache.
	err := tc.client.Put(ctx, []byte("a"), []byte("1"))
	require.NoError(t, err)

	// Second put should use cached region.
	err = tc.client.Put(ctx, []byte("b"), []byte("2"))
	require.NoError(t, err)

	val, notFound, err := tc.client.Get(ctx, []byte("a"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("1"), val)

	val, notFound, err = tc.client.Get(ctx, []byte("b"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("2"), val)
}

// --- Store Resolution Tests ---

func TestClientStoreResolution(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: nil},
	})
	ctx := context.Background()

	// Client discovers store address from PD, connects, and writes.
	err := tc.client.Put(ctx, []byte("key"), []byte("val"))
	require.NoError(t, err)

	val, notFound, err := tc.client.Get(ctx, []byte("key"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("val"), val)
}

// --- Batch Operation Tests ---

func TestClientBatchGetAcrossRegions(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	// Put keys in different regions.
	require.NoError(t, tc.client.Put(ctx, []byte("apple"), []byte("1")))
	require.NoError(t, tc.client.Put(ctx, []byte("mango"), []byte("2")))

	// BatchGet across regions.
	pairs, err := tc.client.BatchGet(ctx, [][]byte{[]byte("apple"), []byte("mango")})
	require.NoError(t, err)
	assert.Equal(t, 2, len(pairs))

	// Build result map for order-independent assertion.
	results := make(map[string]string)
	for _, p := range pairs {
		results[string(p.Key)] = string(p.Value)
	}
	assert.Equal(t, "1", results["apple"])
	assert.Equal(t, "2", results["mango"])
}

func TestClientBatchPutAcrossRegions(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	// BatchPut keys across regions.
	err := tc.client.BatchPut(ctx, []client.KvPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("z"), Value: []byte("2")},
	})
	require.NoError(t, err)

	val, notFound, err := tc.client.Get(ctx, []byte("a"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("1"), val)

	val, notFound, err = tc.client.Get(ctx, []byte("z"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("2"), val)
}

// --- Scan Tests ---

func TestClientScanAcrossRegions(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	// Put keys in both regions.
	keys := []string{"a", "b", "c", "m", "n"}
	for i, k := range keys {
		require.NoError(t, tc.client.Put(ctx, []byte(k), []byte{byte('0' + i)}))
	}

	// Scan all.
	pairs, err := tc.client.Scan(ctx, nil, nil, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(pairs), 5)

	// Verify keys are in order.
	gotKeys := make([]string, len(pairs))
	for i, p := range pairs {
		gotKeys[i] = string(p.Key)
	}
	for i := 1; i < len(gotKeys); i++ {
		assert.LessOrEqual(t, gotKeys[i-1], gotKeys[i], "keys should be sorted")
	}
}

func TestClientScanWithLimit(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	keys := []string{"a", "b", "c", "m", "n"}
	for i, k := range keys {
		require.NoError(t, tc.client.Put(ctx, []byte(k), []byte{byte('0' + i)}))
	}

	// Scan with limit 3 — should only return 3 keys from the first region.
	pairs, err := tc.client.Scan(ctx, nil, nil, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, len(pairs))
}

// --- Advanced Operation Tests ---

func TestClientCompareAndSwap(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	// Put a key in region 2.
	require.NoError(t, tc.client.Put(ctx, []byte("zebra"), []byte("old")))

	// CAS: replace "old" with "new".
	ok, _, err := tc.client.CompareAndSwap(ctx, []byte("zebra"), []byte("new"), []byte("old"), false)
	require.NoError(t, err)
	assert.True(t, ok)

	// Verify.
	val, notFound, err := tc.client.Get(ctx, []byte("zebra"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("new"), val)
}

// --- Delete Tests ---

func TestClientBatchDeleteAcrossRegions(t *testing.T) {
	tc := newClientTestCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})
	ctx := context.Background()

	// Put keys.
	require.NoError(t, tc.client.Put(ctx, []byte("alpha"), []byte("v1")))
	require.NoError(t, tc.client.Put(ctx, []byte("zeta"), []byte("v2")))

	// Delete across regions.
	err := tc.client.BatchDelete(ctx, [][]byte{[]byte("alpha"), []byte("zeta")})
	require.NoError(t, err)

	// Verify deleted.
	_, notFound, err := tc.client.Get(ctx, []byte("alpha"))
	require.NoError(t, err)
	assert.True(t, notFound)

	_, notFound, err = tc.client.Get(ctx, []byte("zeta"))
	require.NoError(t, err)
	assert.True(t, notFound)
}
