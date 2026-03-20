package client

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupMockPD() *pdclient.MockClient {
	mock := pdclient.NewMockClient(1)
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})
	mock.SetStore(&metapb.Store{Id: 2, Address: "127.0.0.1:20161"})
	return mock
}

func TestLocateKey_ColdCache(t *testing.T) {
	mock := setupMockPD()
	region := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(region, &metapb.Peer{Id: 10, StoreId: 1})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)

	info, err := cache.LocateKey(context.Background(), []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.Region.GetId())
	assert.Equal(t, "127.0.0.1:20160", info.StoreAddr)
}

func TestLocateKey_WarmCache(t *testing.T) {
	mock := setupMockPD()
	region := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(region, &metapb.Peer{Id: 10, StoreId: 1})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)
	ctx := context.Background()

	info1, err := cache.LocateKey(ctx, []byte("a"))
	require.NoError(t, err)

	info2, err := cache.LocateKey(ctx, []byte("b"))
	require.NoError(t, err)

	// Same region.
	assert.Equal(t, info1.Region.GetId(), info2.Region.GetId())
}

func TestLocateKey_MultiRegion(t *testing.T) {
	mock := setupMockPD()
	r1 := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      []byte("m"),
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	r2 := &metapb.Region{
		Id:          2,
		StartKey:    []byte("m"),
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 20, StoreId: 2}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(r1, &metapb.Peer{Id: 10, StoreId: 1})
	mock.SetRegion(r2, &metapb.Peer{Id: 20, StoreId: 2})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)
	ctx := context.Background()

	info1, err := cache.LocateKey(ctx, []byte("apple"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info1.Region.GetId())

	info2, err := cache.LocateKey(ctx, []byte("zebra"))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), info2.Region.GetId())
}

func TestInvalidateRegion(t *testing.T) {
	mock := setupMockPD()
	region := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(region, &metapb.Peer{Id: 10, StoreId: 1})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)
	ctx := context.Background()

	_, err := cache.LocateKey(ctx, []byte("key"))
	require.NoError(t, err)

	cache.InvalidateRegion(1)

	// Cache should be empty now; will refetch from PD.
	cache.mu.RLock()
	assert.Equal(t, 0, len(cache.regions))
	cache.mu.RUnlock()

	// Should still work after invalidation.
	info, err := cache.LocateKey(ctx, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.Region.GetId())
}

func TestUpdateLeader(t *testing.T) {
	mock := setupMockPD()
	region := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}, {Id: 20, StoreId: 2}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(region, &metapb.Peer{Id: 10, StoreId: 1})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)
	ctx := context.Background()

	info, err := cache.LocateKey(ctx, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, uint64(10), info.Leader.GetId())
	assert.Equal(t, "127.0.0.1:20160", info.StoreAddr)

	// Update leader to peer 20 on store 2.
	newLeader := &metapb.Peer{Id: 20, StoreId: 2}
	cache.UpdateLeader(1, newLeader, "127.0.0.1:20161")

	info, err = cache.LocateKey(ctx, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, uint64(20), info.Leader.GetId())
	assert.Equal(t, "127.0.0.1:20161", info.StoreAddr)
}

func TestGroupKeysByRegion(t *testing.T) {
	mock := setupMockPD()
	r1 := &metapb.Region{
		Id:          1,
		StartKey:    nil,
		EndKey:      []byte("m"),
		Peers:       []*metapb.Peer{{Id: 10, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	r2 := &metapb.Region{
		Id:          2,
		StartKey:    []byte("m"),
		EndKey:      nil,
		Peers:       []*metapb.Peer{{Id: 20, StoreId: 2}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}
	mock.SetRegion(r1, &metapb.Peer{Id: 10, StoreId: 1})
	mock.SetRegion(r2, &metapb.Peer{Id: 20, StoreId: 2})

	resolver := NewPDStoreResolver(mock, 30*time.Second)
	cache := NewRegionCache(mock, resolver)

	groups, err := cache.GroupKeysByRegion(context.Background(), [][]byte{
		[]byte("apple"), []byte("banana"), []byte("mango"), []byte("zebra"),
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(groups))
	assert.Equal(t, 2, len(groups[1].Keys)) // apple, banana
	assert.Equal(t, 2, len(groups[2].Keys)) // mango, zebra
}
