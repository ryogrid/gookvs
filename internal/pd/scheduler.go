package pd

import (
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// MoveTrackerInterface is used by the scheduler to check pending moves.
// Nil-safe: scheduler checks if moveTracker != nil before calling.
type MoveTrackerInterface interface {
	HasPendingMove(regionID uint64) bool
	ActiveMoveCount() int
	StartMove(regionID uint64, sourcePeer *metapb.Peer, targetStoreID uint64)
	Advance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand
}

// Scheduler produces scheduling commands based on cluster state.
type Scheduler struct {
	meta                   *MetadataStore
	idAlloc                *IDAllocator
	maxPeerCount           int
	regionBalanceThreshold float64
	regionBalanceRateLimit int
	moveTracker            MoveTrackerInterface
}

// NewScheduler creates a new Scheduler.
func NewScheduler(meta *MetadataStore, idAlloc *IDAllocator, maxPeerCount int,
	threshold float64, rateLimit int, moveTracker MoveTrackerInterface) *Scheduler {
	if maxPeerCount <= 0 {
		maxPeerCount = 3
	}
	if threshold <= 0 {
		threshold = 0.05
	}
	if rateLimit <= 0 {
		rateLimit = 4
	}
	return &Scheduler{
		meta:                   meta,
		idAlloc:                idAlloc,
		maxPeerCount:           maxPeerCount,
		regionBalanceThreshold: threshold,
		regionBalanceRateLimit: rateLimit,
		moveTracker:            moveTracker,
	}
}

// ScheduleCommand represents a scheduling command to return in a heartbeat response.
type ScheduleCommand struct {
	RegionID       uint64
	TransferLeader *pdpb.TransferLeader
	ChangePeer     *pdpb.ChangePeer
	Merge          *pdpb.Merge
}

// Schedule evaluates the cluster state for a given region and returns a command if needed.
// Returns nil if no scheduling action is required.
func (s *Scheduler) Schedule(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
	// Priority 0: Advance pending multi-step moves.
	if s.moveTracker != nil {
		if cmd := s.moveTracker.Advance(regionID, region, leader); cmd != nil {
			return cmd
		}
	}
	// Priority 1: Remove excess replicas.
	if cmd := s.scheduleExcessReplicaShedding(regionID, region); cmd != nil {
		return cmd
	}
	// Priority 2: Repair under-replicated regions.
	if cmd := s.scheduleReplicaRepair(regionID, region); cmd != nil {
		return cmd
	}
	// Priority 3: Balance region distribution.
	if cmd := s.scheduleRegionBalance(regionID, region, leader); cmd != nil {
		return cmd
	}
	// Priority 4: Balance leader distribution.
	if cmd := s.scheduleLeaderBalance(regionID, region, leader); cmd != nil {
		return cmd
	}
	return nil
}

// scheduleReplicaRepair checks if a region needs a new replica because one is on a dead store.
// Only stores in Down state are considered lost. Disconnected stores may come back.
func (s *Scheduler) scheduleReplicaRepair(regionID uint64, region *metapb.Region) *ScheduleCommand {
	var healthyPeers []*metapb.Peer
	for _, peer := range region.GetPeers() {
		state := s.meta.GetStoreState(peer.GetStoreId())
		if state == StoreStateUp || state == StoreStateDisconnected {
			healthyPeers = append(healthyPeers, peer)
		}
	}

	if len(healthyPeers) >= s.maxPeerCount {
		return nil // Enough replicas
	}

	// Find a store that doesn't already host this region.
	targetStore := s.pickStoreForRegion(region)
	if targetStore == 0 {
		return nil
	}

	newPeerID := s.idAlloc.Alloc()
	return &ScheduleCommand{
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			Peer: &metapb.Peer{
				Id:      newPeerID,
				StoreId: targetStore,
			},
			ChangeType: eraftpb.ConfChangeType_AddNode,
		},
	}
}

// scheduleLeaderBalance checks if leaders should be rebalanced across stores.
func (s *Scheduler) scheduleLeaderBalance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
	if leader == nil {
		return nil
	}

	leaderCounts := s.meta.GetLeaderCountPerStore()
	leaderStore := leader.GetStoreId()
	leaderCount := leaderCounts[leaderStore]

	// Find store with minimum leaders among this region's peers.
	var minStore uint64
	minCount := int(^uint(0) >> 1) // max int
	for _, peer := range region.GetPeers() {
		storeID := peer.GetStoreId()
		if !s.meta.IsStoreSchedulable(storeID) {
			continue
		}
		if count := leaderCounts[storeID]; count < minCount {
			minCount = count
			minStore = storeID
		}
	}

	// Transfer if difference > 1.
	if minStore != 0 && minStore != leaderStore && leaderCount-minCount > 1 {
		return &ScheduleCommand{
			RegionID: regionID,
			TransferLeader: &pdpb.TransferLeader{
				Peer: &metapb.Peer{StoreId: minStore},
			},
		}
	}
	return nil
}

// scheduleExcessReplicaShedding checks if a region has more peers than maxPeerCount
// and removes the peer on the store with the highest region count.
func (s *Scheduler) scheduleExcessReplicaShedding(regionID uint64, region *metapb.Region) *ScheduleCommand {
	if len(region.GetPeers()) <= s.maxPeerCount {
		return nil
	}

	regionCounts := s.meta.GetRegionCountPerStore()

	// Find the peer on the store with the most regions.
	var removePeer *metapb.Peer
	maxCount := -1
	for _, peer := range region.GetPeers() {
		count := regionCounts[peer.GetStoreId()]
		if count > maxCount {
			maxCount = count
			removePeer = peer
		}
	}

	if removePeer == nil {
		return nil
	}

	return &ScheduleCommand{
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			Peer:       removePeer,
			ChangeType: eraftpb.ConfChangeType_RemoveNode,
		},
	}
}

// scheduleRegionBalance moves a region peer from an overloaded store to an underloaded one.
func (s *Scheduler) scheduleRegionBalance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
	if leader == nil {
		return nil
	}

	// Check rate limit and pending moves via moveTracker.
	if s.moveTracker != nil {
		if s.moveTracker.ActiveMoveCount() >= s.regionBalanceRateLimit {
			return nil
		}
		if s.moveTracker.HasPendingMove(regionID) {
			return nil
		}
	}

	regionCounts := s.meta.GetRegionCountPerStore()

	// Compute mean across all schedulable stores.
	stores := s.meta.GetAllStores()
	var totalRegions int
	var schedulableCount int
	for _, store := range stores {
		if !s.meta.IsStoreSchedulable(store.GetId()) {
			continue
		}
		totalRegions += regionCounts[store.GetId()]
		schedulableCount++
	}
	if schedulableCount == 0 {
		return nil
	}
	mean := float64(totalRegions) / float64(schedulableCount)

	// Check if any of the region's peer stores is overloaded.
	overloaded := false
	for _, peer := range region.GetPeers() {
		count := regionCounts[peer.GetStoreId()]
		if float64(count) > mean*(1+s.regionBalanceThreshold) {
			overloaded = true
			break
		}
	}
	if !overloaded {
		return nil
	}

	// Build set of stores already hosting this region.
	existingStores := make(map[uint64]bool)
	for _, peer := range region.GetPeers() {
		existingStores[peer.GetStoreId()] = true
	}

	// Find an underloaded schedulable store not already hosting this region.
	var targetStoreID uint64
	for _, store := range stores {
		storeID := store.GetId()
		if existingStores[storeID] {
			continue
		}
		if !s.meta.IsStoreSchedulable(storeID) {
			continue
		}
		count := regionCounts[storeID]
		if float64(count) < mean*(1-s.regionBalanceThreshold) {
			targetStoreID = storeID
			break
		}
	}
	if targetStoreID == 0 {
		return nil
	}

	newPeerID := s.idAlloc.Alloc()

	// Start move tracking if moveTracker is available.
	if s.moveTracker != nil {
		// Find the overloaded peer to use as the source.
		var sourcePeer *metapb.Peer
		for _, peer := range region.GetPeers() {
			count := regionCounts[peer.GetStoreId()]
			if float64(count) > mean*(1+s.regionBalanceThreshold) {
				sourcePeer = peer
				break
			}
		}
		if sourcePeer != nil {
			s.moveTracker.StartMove(regionID, sourcePeer, targetStoreID)
		}
	}

	return &ScheduleCommand{
		RegionID: regionID,
		ChangePeer: &pdpb.ChangePeer{
			Peer: &metapb.Peer{
				Id:      newPeerID,
				StoreId: targetStoreID,
			},
			ChangeType: eraftpb.ConfChangeType_AddNode,
		},
	}
}

// pickStoreForRegion finds a schedulable store that doesn't already host this region.
func (s *Scheduler) pickStoreForRegion(region *metapb.Region) uint64 {
	existingStores := make(map[uint64]bool)
	for _, peer := range region.GetPeers() {
		existingStores[peer.GetStoreId()] = true
	}

	stores := s.meta.GetAllStores()
	for _, store := range stores {
		storeID := store.GetId()
		if existingStores[storeID] {
			continue
		}
		if !s.meta.IsStoreSchedulable(storeID) {
			continue
		}
		return storeID
	}
	return 0
}
