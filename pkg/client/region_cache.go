package client

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

// RegionInfo holds cached region metadata with its leader and store address.
type RegionInfo struct {
	Region    *metapb.Region
	Leader    *metapb.Peer
	StoreAddr string
}

// KeyGroup holds keys grouped by region for batch operations.
type KeyGroup struct {
	Info *RegionInfo
	Keys [][]byte
}

// RegionCache caches key-to-region mappings with PD fallback.
type RegionCache struct {
	mu       sync.RWMutex
	pdClient pdclient.Client
	resolver *PDStoreResolver

	regions []*RegionInfo    // sorted by StartKey
	byID    map[uint64]int   // regionID -> index in regions
}

// NewRegionCache creates a new empty region cache.
func NewRegionCache(pdClient pdclient.Client, resolver *PDStoreResolver) *RegionCache {
	return &RegionCache{
		pdClient: pdClient,
		resolver: resolver,
		byID:     make(map[uint64]int),
	}
}

// LocateKey returns the RegionInfo for the given key.
// Returns from cache if available; queries PD on cache miss.
func (c *RegionCache) LocateKey(ctx context.Context, key []byte) (*RegionInfo, error) {
	c.mu.RLock()
	if info := c.findInCache(key); info != nil {
		c.mu.RUnlock()
		return info, nil
	}
	c.mu.RUnlock()

	slog.Debug("cache.GetRegion", "key", fmt.Sprintf("%x", key))
	return c.loadFromPD(ctx, key)
}

// findInCache performs binary search on the sorted regions slice.
// Must be called with at least RLock held.
func (c *RegionCache) findInCache(key []byte) *RegionInfo {
	if len(c.regions) == 0 {
		return nil
	}

	// Find the rightmost region whose StartKey <= key.
	// sort.Search returns the first index where regions[i].StartKey > key.
	idx := sort.Search(len(c.regions), func(i int) bool {
		return bytes.Compare(c.regions[i].Region.GetStartKey(), key) > 0
	})
	idx-- // step back to the region whose StartKey <= key

	if idx < 0 {
		return nil
	}

	info := c.regions[idx]
	endKey := info.Region.GetEndKey()
	// Check key < endKey (empty endKey means unbounded).
	if len(endKey) > 0 && bytes.Compare(key, endKey) >= 0 {
		return nil
	}
	return info
}

// loadFromPD fetches region info from PD, resolves the store address, and inserts into cache.
func (c *RegionCache) loadFromPD(ctx context.Context, key []byte) (*RegionInfo, error) {
	region, leader, err := c.pdClient.GetRegion(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("get region for key: %w", err)
	}
	if region == nil {
		return nil, fmt.Errorf("no region found for key %q", key)
	}
	if leader == nil {
		// No leader yet; pick the first peer as a best-effort target.
		if len(region.GetPeers()) > 0 {
			leader = region.GetPeers()[0]
		} else {
			return nil, fmt.Errorf("region %d has no peers", region.GetId())
		}
	}

	addr, err := c.resolver.Resolve(ctx, leader.GetStoreId())
	if err != nil {
		return nil, err
	}

	info := &RegionInfo{
		Region:    region,
		Leader:    leader,
		StoreAddr: addr,
	}

	c.mu.Lock()
	c.insertLocked(info)
	c.mu.Unlock()

	return info, nil
}

// insertLocked inserts a RegionInfo into the sorted slice and byID map.
// Must be called with write lock held.
func (c *RegionCache) insertLocked(info *RegionInfo) {
	regionID := info.Region.GetId()

	// If already present, replace it.
	if idx, ok := c.byID[regionID]; ok {
		c.regions[idx] = info
		return
	}

	// Find insertion point.
	startKey := info.Region.GetStartKey()
	pos := sort.Search(len(c.regions), func(i int) bool {
		return bytes.Compare(c.regions[i].Region.GetStartKey(), startKey) > 0
	})

	// Insert at pos.
	c.regions = append(c.regions, nil)
	copy(c.regions[pos+1:], c.regions[pos:])
	c.regions[pos] = info

	// Rebuild byID (indices shifted).
	c.rebuildByID()
}

// rebuildByID reconstructs the byID index from the current regions slice.
func (c *RegionCache) rebuildByID() {
	c.byID = make(map[uint64]int, len(c.regions))
	for i, info := range c.regions {
		c.byID[info.Region.GetId()] = i
	}
}

// InvalidateRegion removes the cached entry for the given region ID.
func (c *RegionCache) InvalidateRegion(regionID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx, ok := c.byID[regionID]
	if !ok {
		return
	}

	// Remove from slice (order-preserving).
	c.regions = append(c.regions[:idx], c.regions[idx+1:]...)
	c.rebuildByID()
}

// UpdateLeader updates the cached leader for a region with the given store address.
func (c *RegionCache) UpdateLeader(regionID uint64, leader *metapb.Peer, storeAddr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	idx, ok := c.byID[regionID]
	if !ok {
		return
	}

	c.regions[idx].Leader = leader
	c.regions[idx].StoreAddr = storeAddr
}

// GroupKeysByRegion groups a set of keys by their region.
func (c *RegionCache) GroupKeysByRegion(ctx context.Context, keys [][]byte) (map[uint64]*KeyGroup, error) {
	groups := make(map[uint64]*KeyGroup)
	for _, key := range keys {
		info, err := c.LocateKey(ctx, key)
		if err != nil {
			return nil, err
		}
		regionID := info.Region.GetId()
		g, ok := groups[regionID]
		if !ok {
			g = &KeyGroup{Info: info}
			groups[regionID] = g
		}
		g.Keys = append(g.Keys, key)
	}
	return groups, nil
}
