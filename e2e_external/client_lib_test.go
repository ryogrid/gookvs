package e2e_external_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/client"
)

// --- Region Cache Tests ---

func TestClientRegionCacheMiss(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Cold cache: Put and Get should succeed by querying PD.
	err := rawKV.Put(ctx, []byte("key1"), []byte("val1"))
	require.NoError(t, err)

	val, notFound, err := rawKV.Get(ctx, []byte("key1"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("val1"), val)
}

func TestClientRegionCacheHit(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// First put populates cache.
	err := rawKV.Put(ctx, []byte("a"), []byte("1"))
	require.NoError(t, err)

	// Second put should use cached region.
	err = rawKV.Put(ctx, []byte("b"), []byte("2"))
	require.NoError(t, err)

	val, notFound, err := rawKV.Get(ctx, []byte("a"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("1"), val)

	val, notFound, err = rawKV.Get(ctx, []byte("b"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("2"), val)
}

// --- Store Resolution Tests ---

func TestClientStoreResolution(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Client discovers store address from PD, connects, and writes.
	err := rawKV.Put(ctx, []byte("key"), []byte("val"))
	require.NoError(t, err)

	val, notFound, err := rawKV.Get(ctx, []byte("key"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("val"), val)
}

// --- Batch Operation Tests ---

func TestClientBatchGetAcrossRegions(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Put keys in different regions.
	require.NoError(t, rawKV.Put(ctx, []byte("apple"), []byte("1")))
	require.NoError(t, rawKV.Put(ctx, []byte("mango"), []byte("2")))

	// BatchGet across regions.
	pairs, err := rawKV.BatchGet(ctx, [][]byte{[]byte("apple"), []byte("mango")})
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
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// BatchPut keys across regions.
	err := rawKV.BatchPut(ctx, []client.KvPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("z"), Value: []byte("2")},
	})
	require.NoError(t, err)

	val, notFound, err := rawKV.Get(ctx, []byte("a"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("1"), val)

	val, notFound, err = rawKV.Get(ctx, []byte("z"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("2"), val)
}

// --- Scan Tests ---

func TestClientScanAcrossRegions(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Put keys in both regions.
	keys := []string{"a", "b", "c", "m", "n"}
	for i, k := range keys {
		require.NoError(t, rawKV.Put(ctx, []byte(k), []byte{byte('0' + i)}))
	}

	// Scan all.
	pairs, err := rawKV.Scan(ctx, nil, nil, 100)
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
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	keys := []string{"a", "b", "c", "m", "n"}
	for i, k := range keys {
		require.NoError(t, rawKV.Put(ctx, []byte(k), []byte{byte('0' + i)}))
	}

	// Scan with limit 3 -- should only return 3 keys from the first region.
	pairs, err := rawKV.Scan(ctx, nil, nil, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, len(pairs))
}

// --- Advanced Operation Tests ---

func TestClientCompareAndSwap(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Put a key in region 2.
	require.NoError(t, rawKV.Put(ctx, []byte("zebra"), []byte("old")))

	// CAS: replace "old" with "new".
	ok, _, err := rawKV.CompareAndSwap(ctx, []byte("zebra"), []byte("new"), []byte("old"), false)
	require.NoError(t, err)
	assert.True(t, ok)

	// Verify.
	val, notFound, err := rawKV.Get(ctx, []byte("zebra"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("new"), val)
}

// --- Delete Tests ---

func TestClientBatchDeleteAcrossRegions(t *testing.T) {
	_, rawKV := newClientCluster(t)
	ctx := context.Background()

	// Put keys.
	require.NoError(t, rawKV.Put(ctx, []byte("alpha"), []byte("v1")))
	require.NoError(t, rawKV.Put(ctx, []byte("zeta"), []byte("v2")))

	// Delete across regions.
	err := rawKV.BatchDelete(ctx, [][]byte{[]byte("alpha"), []byte("zeta")})
	require.NoError(t, err)

	// Verify deleted.
	_, notFound, err := rawKV.Get(ctx, []byte("alpha"))
	require.NoError(t, err)
	assert.True(t, notFound)

	_, notFound, err = rawKV.Get(ctx, []byte("zeta"))
	require.NoError(t, err)
	assert.True(t, notFound)
}
