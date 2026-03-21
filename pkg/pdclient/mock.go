package pdclient

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// MockClient is an in-memory PD client for testing.
// It simulates PD's TSO, region management, and store registration.
type MockClient struct {
	mu sync.RWMutex

	clusterID    uint64
	bootstrapped bool
	nextID       atomic.Uint64

	// TSO state.
	tsoPhysical atomic.Int64
	tsoLogical  atomic.Int64

	// Store registry: storeID -> store.
	stores map[uint64]*metapb.Store

	// Region registry: regionID -> region.
	regions map[uint64]*metapb.Region

	// Region leaders: regionID -> leader peer.
	leaders map[uint64]*metapb.Peer

	// Heartbeat responses: regionID -> scheduled commands.
	heartbeatResponses map[uint64]*pdpb.RegionHeartbeatResponse

	// Store stats: storeID -> last heartbeat stats.
	storeStats map[uint64]*pdpb.StoreStats

	// GC safe point.
	gcSafePoint atomic.Uint64
}

// NewMockClient creates a new mock PD client.
func NewMockClient(clusterID uint64) *MockClient {
	c := &MockClient{
		clusterID:          clusterID,
		stores:             make(map[uint64]*metapb.Store),
		regions:            make(map[uint64]*metapb.Region),
		leaders:            make(map[uint64]*metapb.Peer),
		heartbeatResponses: make(map[uint64]*pdpb.RegionHeartbeatResponse),
		storeStats:         make(map[uint64]*pdpb.StoreStats),
	}
	c.nextID.Store(1000) // Start IDs at 1000 to avoid conflicts.
	c.tsoPhysical.Store(1)
	return c
}

// Ensure MockClient implements Client.
var _ Client = (*MockClient)(nil)

func (c *MockClient) GetTS(_ context.Context) (TimeStamp, error) {
	logical := c.tsoLogical.Add(1)
	physical := c.tsoPhysical.Load()

	// Roll over logical counter.
	if logical >= (1 << 18) {
		c.tsoPhysical.Add(1)
		c.tsoLogical.Store(0)
		physical = c.tsoPhysical.Load()
		logical = 0
	}

	return TimeStamp{
		Physical: physical,
		Logical:  logical,
	}, nil
}

func (c *MockClient) GetRegion(_ context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, r := range c.regions {
		if containsKey(r, key) {
			leader := c.leaders[r.GetId()]
			return r, leader, nil
		}
	}
	return nil, nil, fmt.Errorf("pdclient: region not found for key %x", key)
}

func (c *MockClient) GetRegionByID(_ context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	r, ok := c.regions[regionID]
	if !ok {
		return nil, nil, fmt.Errorf("pdclient: region %d not found", regionID)
	}
	leader := c.leaders[regionID]
	return r, leader, nil
}

func (c *MockClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.stores[storeID]
	if !ok {
		return nil, fmt.Errorf("pdclient: store %d not found", storeID)
	}
	return s, nil
}

func (c *MockClient) GetAllStores(_ context.Context) ([]*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	stores := make([]*metapb.Store, 0, len(c.stores))
	for _, s := range c.stores {
		stores = append(stores, s)
	}
	return stores, nil
}

func (c *MockClient) Bootstrap(_ context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.bootstrapped {
		return nil, fmt.Errorf("pdclient: cluster already bootstrapped")
	}

	c.stores[store.GetId()] = store
	c.regions[region.GetId()] = region
	if len(region.GetPeers()) > 0 {
		c.leaders[region.GetId()] = region.GetPeers()[0]
	}
	c.bootstrapped = true

	return &pdpb.BootstrapResponse{
		Header: &pdpb.ResponseHeader{ClusterId: c.clusterID},
	}, nil
}

func (c *MockClient) IsBootstrapped(_ context.Context) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bootstrapped, nil
}

func (c *MockClient) PutStore(_ context.Context, store *metapb.Store) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores[store.GetId()] = store
	return nil
}

func (c *MockClient) ReportRegionHeartbeat(_ context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	region := req.GetRegion()
	if region != nil {
		c.regions[region.GetId()] = region
		if req.GetLeader() != nil {
			c.leaders[region.GetId()] = req.GetLeader()
		}
	}

	// Return canned response if set.
	if region != nil {
		if resp, ok := c.heartbeatResponses[region.GetId()]; ok {
			delete(c.heartbeatResponses, region.GetId())
			return resp, nil
		}
	}

	return &pdpb.RegionHeartbeatResponse{}, nil
}

func (c *MockClient) StoreHeartbeat(_ context.Context, stats *pdpb.StoreStats) error {
	if stats != nil {
		c.mu.Lock()
		c.storeStats[stats.GetStoreId()] = stats
		c.mu.Unlock()
	}
	return nil
}

// GetStoreStats returns the last store heartbeat stats for the given store.
func (c *MockClient) GetStoreStats(storeID uint64) *pdpb.StoreStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.storeStats[storeID]
}

func (c *MockClient) AskBatchSplit(_ context.Context, _ *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error) {
	var ids []*pdpb.SplitID
	for i := uint32(0); i < count; i++ {
		newRegionID := c.nextID.Add(1)
		newPeerID := c.nextID.Add(1)
		ids = append(ids, &pdpb.SplitID{
			NewRegionId: newRegionID,
			NewPeerIds:  []uint64{newPeerID},
		})
	}
	return &pdpb.AskBatchSplitResponse{
		Header: &pdpb.ResponseHeader{ClusterId: c.clusterID},
		Ids:    ids,
	}, nil
}

func (c *MockClient) ReportBatchSplit(_ context.Context, regions []*metapb.Region) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, r := range regions {
		c.regions[r.GetId()] = r
		if len(r.GetPeers()) > 0 {
			c.leaders[r.GetId()] = r.GetPeers()[0]
		}
	}
	return nil
}

func (c *MockClient) GetGCSafePoint(_ context.Context) (uint64, error) {
	return c.gcSafePoint.Load(), nil
}

func (c *MockClient) UpdateGCSafePoint(_ context.Context, safePoint uint64) (uint64, error) {
	for {
		old := c.gcSafePoint.Load()
		if safePoint <= old {
			return old, nil // PD only moves forward
		}
		if c.gcSafePoint.CompareAndSwap(old, safePoint) {
			return safePoint, nil
		}
	}
}

func (c *MockClient) AllocID(_ context.Context) (uint64, error) {
	return c.nextID.Add(1), nil
}

func (c *MockClient) GetClusterID(_ context.Context) uint64 {
	return c.clusterID
}

func (c *MockClient) Close() {
	// No-op for mock.
}

// --- Mock helper methods ---

// SetRegion adds or updates a region in the mock.
func (c *MockClient) SetRegion(region *metapb.Region, leader *metapb.Peer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.regions[region.GetId()] = region
	if leader != nil {
		c.leaders[region.GetId()] = leader
	}
}

// SetStore adds or updates a store in the mock.
func (c *MockClient) SetStore(store *metapb.Store) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stores[store.GetId()] = store
}

// SetHeartbeatResponse sets a canned heartbeat response for a region.
func (c *MockClient) SetHeartbeatResponse(regionID uint64, resp *pdpb.RegionHeartbeatResponse) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartbeatResponses[regionID] = resp
}

// GetAllRegions returns all registered regions sorted by start key.
func (c *MockClient) GetAllRegions() []*metapb.Region {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var regions []*metapb.Region
	for _, r := range c.regions {
		regions = append(regions, r)
	}
	sort.Slice(regions, func(i, j int) bool {
		return string(regions[i].GetStartKey()) < string(regions[j].GetStartKey())
	})
	return regions
}

// containsKey checks if a region's key range contains the given key.
func containsKey(region *metapb.Region, key []byte) bool {
	start := region.GetStartKey()
	end := region.GetEndKey()

	// Start key check.
	if len(start) > 0 && string(key) < string(start) {
		return false
	}
	// End key check (empty end key means unbounded).
	if len(end) > 0 && string(key) >= string(end) {
		return false
	}
	return true
}
