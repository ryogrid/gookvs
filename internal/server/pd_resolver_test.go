package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPDClient implements pdclient.Client for testing PDStoreResolver.
type mockPDClient struct {
	stores       map[uint64]*metapb.Store
	getStoreCalls atomic.Int64
}

func newMockPDClient() *mockPDClient {
	return &mockPDClient{
		stores: make(map[uint64]*metapb.Store),
	}
}

func (m *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	m.getStoreCalls.Add(1)
	store, ok := m.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("store %d not found", storeID)
	}
	return store, nil
}

// Unused interface methods — stub implementations.
func (m *mockPDClient) GetTS(context.Context) (pdclient.TimeStamp, error) {
	return pdclient.TimeStamp{}, nil
}
func (m *mockPDClient) GetRegion(context.Context, []byte) (*metapb.Region, *metapb.Peer, error) {
	return nil, nil, nil
}
func (m *mockPDClient) GetRegionByID(context.Context, uint64) (*metapb.Region, *metapb.Peer, error) {
	return nil, nil, nil
}
func (m *mockPDClient) Bootstrap(context.Context, *metapb.Store, *metapb.Region) (*pdpb.BootstrapResponse, error) {
	return nil, nil
}
func (m *mockPDClient) GetAllStores(context.Context) ([]*metapb.Store, error) { return nil, nil }
func (m *mockPDClient) IsBootstrapped(context.Context) (bool, error)         { return false, nil }
func (m *mockPDClient) PutStore(context.Context, *metapb.Store) error { return nil }
func (m *mockPDClient) ReportRegionHeartbeat(context.Context, *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	return nil, nil
}
func (m *mockPDClient) StoreHeartbeat(context.Context, *pdpb.StoreStats) error { return nil }
func (m *mockPDClient) AskBatchSplit(context.Context, *metapb.Region, uint32) (*pdpb.AskBatchSplitResponse, error) {
	return nil, nil
}
func (m *mockPDClient) ReportBatchSplit(context.Context, []*metapb.Region) error { return nil }
func (m *mockPDClient) GetGCSafePoint(context.Context) (uint64, error)           { return 0, nil }
func (m *mockPDClient) UpdateGCSafePoint(context.Context, uint64) (uint64, error) { return 0, nil }
func (m *mockPDClient) AllocID(context.Context) (uint64, error)                   { return 0, nil }
func (m *mockPDClient) GetClusterID(context.Context) uint64                       { return 0 }
func (m *mockPDClient) Close()                                                    {}

func TestPDStoreResolver_CacheHit(t *testing.T) {
	mock := newMockPDClient()
	mock.stores[1] = &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}

	resolver := NewPDStoreResolver(mock, 30*time.Second)

	// First resolve: queries PD.
	addr, err := resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20160", addr)
	assert.Equal(t, int64(1), mock.getStoreCalls.Load())

	// Second resolve: should return from cache (no additional PD call).
	addr, err = resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20160", addr)
	assert.Equal(t, int64(1), mock.getStoreCalls.Load()) // Still 1 call.
}

func TestPDStoreResolver_CacheMiss_TTLExpired(t *testing.T) {
	mock := newMockPDClient()
	mock.stores[1] = &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}

	resolver := NewPDStoreResolver(mock, 100*time.Millisecond)

	now := time.Now()
	resolver.nowFunc = func() time.Time { return now }

	// First resolve.
	addr, err := resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20160", addr)
	assert.Equal(t, int64(1), mock.getStoreCalls.Load())

	// Advance time past TTL.
	resolver.nowFunc = func() time.Time { return now.Add(200 * time.Millisecond) }

	// Update the store address in PD.
	mock.stores[1] = &metapb.Store{Id: 1, Address: "127.0.0.1:20161"}

	// Resolve again: should re-query PD.
	addr, err = resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20161", addr)
	assert.Equal(t, int64(2), mock.getStoreCalls.Load())
}

func TestPDStoreResolver_UnknownStore(t *testing.T) {
	mock := newMockPDClient()

	resolver := NewPDStoreResolver(mock, 30*time.Second)

	_, err := resolver.ResolveStore(999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve store 999")
}

func TestPDStoreResolver_InvalidateStore(t *testing.T) {
	mock := newMockPDClient()
	mock.stores[1] = &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}

	resolver := NewPDStoreResolver(mock, 30*time.Second)

	// Populate cache.
	_, err := resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), mock.getStoreCalls.Load())

	// Invalidate and resolve again — should re-query PD.
	resolver.InvalidateStore(1)

	_, err = resolver.ResolveStore(1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), mock.getStoreCalls.Load())
}

func TestPDStoreResolver_DefaultTTL(t *testing.T) {
	mock := newMockPDClient()
	resolver := NewPDStoreResolver(mock, 0) // 0 should default to 30s
	assert.Equal(t, 30*time.Second, resolver.ttl)
}
