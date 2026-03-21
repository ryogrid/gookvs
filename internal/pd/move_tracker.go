package pd

import (
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

// MoveState tracks the current stage of a region move.
type MoveState int

const (
	MoveStateAdding       MoveState = iota // Waiting for AddPeer to complete
	MoveStateTransferring                   // Waiting for leader transfer
	MoveStateRemoving                       // Waiting for RemovePeer to complete
)

// PendingMove tracks a single in-progress region move.
type PendingMove struct {
	RegionID      uint64
	SourcePeer    *metapb.Peer
	TargetStoreID uint64
	TargetPeerID  uint64
	State         MoveState
	StartedAt     time.Time
}

// MoveTracker tracks in-progress region moves across heartbeat cycles.
type MoveTracker struct {
	mu    sync.Mutex
	moves map[uint64]*PendingMove // regionID -> pending move
}

// Compile-time check that MoveTracker implements MoveTrackerInterface.
var _ MoveTrackerInterface = (*MoveTracker)(nil)

// NewMoveTracker creates a new MoveTracker.
func NewMoveTracker() *MoveTracker {
	return &MoveTracker{
		moves: make(map[uint64]*PendingMove),
	}
}

// StartMove begins tracking a new region move from sourcePeer's store to targetStoreID.
func (t *MoveTracker) StartMove(regionID uint64, sourcePeer *metapb.Peer, targetStoreID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.moves[regionID] = &PendingMove{
		RegionID:      regionID,
		SourcePeer:    sourcePeer,
		TargetStoreID: targetStoreID,
		State:         MoveStateAdding,
		StartedAt:     time.Now(),
	}
}

// HasPendingMove returns true if the given region has an in-progress move.
func (t *MoveTracker) HasPendingMove(regionID uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.moves[regionID]
	return ok
}

// ActiveMoveCount returns the number of in-progress moves.
func (t *MoveTracker) ActiveMoveCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.moves)
}

// Advance progresses the move state machine for the given region based on the
// current region metadata and leader. Returns a ScheduleCommand if an action
// is needed, or nil if no action is required (either waiting or move is complete).
func (t *MoveTracker) Advance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
	t.mu.Lock()
	defer t.mu.Unlock()

	move, ok := t.moves[regionID]
	if !ok {
		return nil
	}

	switch move.State {
	case MoveStateAdding:
		// Check if target store now has a peer in the region.
		if !hasPeerOnStore(region, move.TargetStoreID) {
			return nil // Still waiting for AddPeer to complete.
		}
		// Record the target peer ID for later use.
		for _, p := range region.GetPeers() {
			if p.GetStoreId() == move.TargetStoreID {
				move.TargetPeerID = p.GetId()
				break
			}
		}

		sourceStoreID := move.SourcePeer.GetStoreId()
		if leader != nil && leader.GetStoreId() == sourceStoreID {
			// Source is leader — transfer leadership before removing.
			move.State = MoveStateTransferring
			transferTarget := pickTransferTarget(region, sourceStoreID, 0)
			if transferTarget == nil {
				// No suitable transfer target; skip to removing directly.
				move.State = MoveStateRemoving
				return &ScheduleCommand{
					RegionID: regionID,
					ChangePeer: &pdpb.ChangePeer{
						Peer:       move.SourcePeer,
						ChangeType: eraftpb.ConfChangeType_RemoveNode,
					},
				}
			}
			return &ScheduleCommand{
				RegionID: regionID,
				TransferLeader: &pdpb.TransferLeader{
					Peer: transferTarget,
				},
			}
		}
		// Source is not leader — go directly to removing.
		move.State = MoveStateRemoving
		return &ScheduleCommand{
			RegionID: regionID,
			ChangePeer: &pdpb.ChangePeer{
				Peer:       move.SourcePeer,
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
			},
		}

	case MoveStateTransferring:
		sourceStoreID := move.SourcePeer.GetStoreId()
		if leader != nil && leader.GetStoreId() != sourceStoreID {
			// Leader has moved off source — proceed to removing.
			move.State = MoveStateRemoving
			return &ScheduleCommand{
				RegionID: regionID,
				ChangePeer: &pdpb.ChangePeer{
					Peer:       move.SourcePeer,
					ChangeType: eraftpb.ConfChangeType_RemoveNode,
				},
			}
		}
		// Leader is still on source — retry transfer.
		transferTarget := pickTransferTarget(region, sourceStoreID, 0)
		if transferTarget == nil {
			return nil
		}
		return &ScheduleCommand{
			RegionID: regionID,
			TransferLeader: &pdpb.TransferLeader{
				Peer: transferTarget,
			},
		}

	case MoveStateRemoving:
		if !hasPeerOnStore(region, move.SourcePeer.GetStoreId()) {
			// Source peer is gone — move complete.
			delete(t.moves, regionID)
			return nil
		}
		// Source peer still present — retry RemoveNode.
		return &ScheduleCommand{
			RegionID: regionID,
			ChangePeer: &pdpb.ChangePeer{
				Peer:       move.SourcePeer,
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
			},
		}
	}

	return nil
}

// CleanupStale removes moves that have been in progress longer than timeout.
func (t *MoveTracker) CleanupStale(timeout time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	for regionID, move := range t.moves {
		if now.Sub(move.StartedAt) > timeout {
			delete(t.moves, regionID)
		}
	}
}

// hasPeerOnStore returns true if the region has a peer on the given store.
func hasPeerOnStore(region *metapb.Region, storeID uint64) bool {
	for _, p := range region.GetPeers() {
		if p.GetStoreId() == storeID {
			return true
		}
	}
	return false
}

// pickTransferTarget finds a peer in the region that is not on excludeStore1 or excludeStore2.
// Returns nil if no suitable target is found.
func pickTransferTarget(region *metapb.Region, excludeStore1, excludeStore2 uint64) *metapb.Peer {
	for _, p := range region.GetPeers() {
		storeID := p.GetStoreId()
		if storeID != excludeStore1 && storeID != excludeStore2 {
			return p
		}
	}
	return nil
}
