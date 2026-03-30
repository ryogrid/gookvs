package raftstore

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// CreatePeerRequest is sent via StoreMsgTypeCreatePeer to create a new peer.
type CreatePeerRequest struct {
	Region *metapb.Region
	PeerID uint64
}

// DestroyPeerRequest is sent via StoreMsgTypeDestroyPeer to destroy a peer.
type DestroyPeerRequest struct {
	RegionID uint64
	PeerID   uint64
}

// ChangePeerResult carries the result of a conf change back to the peer FSM.
type ChangePeerResult struct {
	Index      uint64                // Raft log index of the conf change
	ChangeType raftpb.ConfChangeType // AddNode, RemoveNode, AddLearnerNode
	Peer       *metapb.Peer         // The peer being added or removed
	Region     *metapb.Region       // Updated region metadata
}

// applyConfChangeEntry processes a committed conf change entry and updates
// region metadata accordingly. Returns a ChangePeerResult, or nil if no action taken.
func (p *Peer) applyConfChangeEntry(e raftpb.Entry) *ChangePeerResult {
	slog.Info("applyConfChangeEntry called", "region", p.regionID, "entryType", e.Type, "index", e.Index)
	if e.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(e.Data); err != nil {
			return nil
		}
		p.rawNode.ApplyConfChange(cc)
		result := p.processConfChange(cc.Type, cc.NodeID, e.Index, cc.Context)
		if p.regionID == 1 {
			slog.Info("region 1 ConfChange applied",
				"type", cc.Type, "nodeID", cc.NodeID,
				"peers", len(result.Region.GetPeers()),
				"isLeader", p.isLeader.Load(), "myPeer", p.peerID)
		}
		return result

	} else if e.Type == raftpb.EntryConfChangeV2 {
		var cc raftpb.ConfChangeV2
		if err := cc.Unmarshal(e.Data); err != nil {
			return nil
		}
		p.rawNode.ApplyConfChange(cc)
		// Process the first single change (V2 with single transition).
		if len(cc.Changes) > 0 {
			change := cc.Changes[0]
			return p.processConfChange(change.Type, change.NodeID, e.Index, cc.Context)
		}
	}
	return nil
}

// processConfChange updates the region's peer list and epoch based on the conf change.
func (p *Peer) processConfChange(
	changeType raftpb.ConfChangeType,
	nodeID uint64,
	index uint64,
	context []byte,
) *ChangePeerResult {
	// Clone region to avoid mutating shared state.
	p.regionMu.RLock()
	region := cloneRegion(p.region)
	p.regionMu.RUnlock()
	if region.RegionEpoch == nil {
		region.RegionEpoch = &metapb.RegionEpoch{}
	}
	region.RegionEpoch.ConfVer++

	var targetPeer *metapb.Peer

	switch changeType {
	case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
		// Decode peer info from context.
		targetPeer = decodePeerFromContext(context, nodeID)
		if targetPeer == nil {
			// Fallback: create a minimal peer with just the node ID.
			targetPeer = &metapb.Peer{Id: nodeID}
		}
		// Check if peer already exists.
		for _, existing := range region.Peers {
			if existing.GetId() == nodeID {
				// Already exists, update and return.
				p.regionMu.Lock()
				p.region = region
				p.regionMu.Unlock()
				return &ChangePeerResult{
					Index:      index,
					ChangeType: changeType,
					Peer:       existing,
					Region:     region,
				}
			}
		}
		region.Peers = append(region.Peers, targetPeer)

	case raftpb.ConfChangeRemoveNode:
		var removed *metapb.Peer
		removed, region.Peers = removePeerByNodeID(region.Peers, nodeID)
		targetPeer = removed
		if targetPeer == nil {
			// Peer not found in list, still update epoch.
			targetPeer = &metapb.Peer{Id: nodeID}
		}

		// If removing self, mark for destruction.
		if nodeID == p.peerID {
			p.stopped.Store(true)
		}
	}

	p.regionMu.Lock()
	p.region = region
	p.regionMu.Unlock()

	return &ChangePeerResult{
		Index:      index,
		ChangeType: changeType,
		Peer:       targetPeer,
		Region:     region,
	}
}

// Region returns the current region metadata.
// Thread-safe: uses regionMu to protect against concurrent access from gRPC handlers.
func (p *Peer) Region() *metapb.Region {
	p.regionMu.RLock()
	defer p.regionMu.RUnlock()
	return p.region
}

// decodePeerFromContext extracts a metapb.Peer from the conf change context bytes.
// Context format: [8 bytes storeID big-endian]
// If context is empty or invalid, returns a peer with just the nodeID.
func decodePeerFromContext(context []byte, nodeID uint64) *metapb.Peer {
	if len(context) >= 8 {
		storeID := binary.BigEndian.Uint64(context[:8])
		return &metapb.Peer{Id: nodeID, StoreId: storeID}
	}
	return nil
}

// EncodePeerContext encodes a store ID into the conf change context bytes.
func EncodePeerContext(storeID uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, storeID)
	return buf
}

// removePeerByNodeID removes the first peer with the given ID from the slice.
// Returns the removed peer and the updated slice.
func removePeerByNodeID(peers []*metapb.Peer, nodeID uint64) (*metapb.Peer, []*metapb.Peer) {
	for i, p := range peers {
		if p.GetId() == nodeID {
			removed := p
			result := make([]*metapb.Peer, 0, len(peers)-1)
			result = append(result, peers[:i]...)
			result = append(result, peers[i+1:]...)
			return removed, result
		}
	}
	return nil, peers
}

// cloneRegion creates a deep copy of a metapb.Region.
func cloneRegion(r *metapb.Region) *metapb.Region {
	if r == nil {
		return nil
	}
	clone := &metapb.Region{
		Id:       r.Id,
		StartKey: append([]byte(nil), r.StartKey...),
		EndKey:   append([]byte(nil), r.EndKey...),
	}
	if r.RegionEpoch != nil {
		clone.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: r.RegionEpoch.ConfVer,
			Version: r.RegionEpoch.Version,
		}
	}
	for _, p := range r.Peers {
		clone.Peers = append(clone.Peers, &metapb.Peer{
			Id:      p.Id,
			StoreId: p.StoreId,
			Role:    p.Role,
		})
	}
	return clone
}

// ProposeConfChange proposes a configuration change through Raft.
func (p *Peer) ProposeConfChange(changeType raftpb.ConfChangeType, peerID uint64, storeID uint64) error {
	cc := raftpb.ConfChange{
		Type:    changeType,
		NodeID:  peerID,
		Context: EncodePeerContext(storeID),
	}
	return p.rawNode.ProposeConfChange(cc)
}

// RegionCount returns 1 since a single Peer manages one region.
// StoreCoordinator aggregates across all peers.
func (p *Peer) RegionCount() int {
	return 1
}

// IsBusy returns whether the peer is busy (mailbox nearly full).
func (p *Peer) IsBusy() bool {
	return len(p.Mailbox) > cap(p.Mailbox)*3/4
}

// String returns a human-readable representation of the peer.
func (p *Peer) String() string {
	return fmt.Sprintf("Peer{region=%d, peer=%d, store=%d, leader=%v}",
		p.regionID, p.peerID, p.storeID, p.isLeader.Load())
}
