package raftstore

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookv/internal/raftstore/split"
)

// TagSplitAdmin identifies a split admin command in the Raft log.
// CompactLog uses 0x01. Both are safe from collision with protobuf
// RaftCmdRequest (which starts with 0x0A for field 1, wire type 2).
const TagSplitAdmin byte = 0x02

// SplitAdminRequest carries the information needed to execute a region split.
type SplitAdminRequest struct {
	SplitKey      []byte
	NewRegionIDs  []uint64
	NewPeerIDSets [][]uint64
}

// IsSplitAdmin returns true if the data is a split admin command.
func IsSplitAdmin(data []byte) bool {
	return len(data) > 0 && data[0] == TagSplitAdmin
}

// MarshalSplitAdminRequest serializes a SplitAdminRequest for Raft proposal.
// Format: tag(1) + splitKeyLen(4) + splitKey(N) + numRegions(4) + [regionID(8) + numPeers(4) + peerIDs(8*N)]...
func MarshalSplitAdminRequest(req SplitAdminRequest) []byte {
	// Calculate size.
	size := 1 + 4 + len(req.SplitKey) + 4
	for _, peerIDs := range req.NewPeerIDSets {
		size += 8 + 4 + 8*len(peerIDs)
	}

	data := make([]byte, size)
	off := 0

	data[off] = TagSplitAdmin
	off++

	binary.BigEndian.PutUint32(data[off:off+4], uint32(len(req.SplitKey)))
	off += 4
	copy(data[off:off+len(req.SplitKey)], req.SplitKey)
	off += len(req.SplitKey)

	binary.BigEndian.PutUint32(data[off:off+4], uint32(len(req.NewRegionIDs)))
	off += 4
	for i, regionID := range req.NewRegionIDs {
		binary.BigEndian.PutUint64(data[off:off+8], regionID)
		off += 8
		peerIDs := req.NewPeerIDSets[i]
		binary.BigEndian.PutUint32(data[off:off+4], uint32(len(peerIDs)))
		off += 4
		for _, pid := range peerIDs {
			binary.BigEndian.PutUint64(data[off:off+8], pid)
			off += 8
		}
	}
	return data
}

// UnmarshalSplitAdminRequest deserializes a SplitAdminRequest.
func UnmarshalSplitAdminRequest(data []byte) (SplitAdminRequest, error) {
	if len(data) < 5 || data[0] != TagSplitAdmin {
		return SplitAdminRequest{}, fmt.Errorf("invalid split admin data")
	}
	off := 1

	splitKeyLen := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	if off+splitKeyLen > len(data) {
		return SplitAdminRequest{}, fmt.Errorf("truncated split key")
	}
	splitKey := make([]byte, splitKeyLen)
	copy(splitKey, data[off:off+splitKeyLen])
	off += splitKeyLen

	if off+4 > len(data) {
		return SplitAdminRequest{}, fmt.Errorf("truncated region count")
	}
	numRegions := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4

	regionIDs := make([]uint64, numRegions)
	peerIDSets := make([][]uint64, numRegions)
	for i := 0; i < numRegions; i++ {
		if off+8 > len(data) {
			return SplitAdminRequest{}, fmt.Errorf("truncated region ID")
		}
		regionIDs[i] = binary.BigEndian.Uint64(data[off : off+8])
		off += 8

		if off+4 > len(data) {
			return SplitAdminRequest{}, fmt.Errorf("truncated peer count")
		}
		numPeers := int(binary.BigEndian.Uint32(data[off : off+4]))
		off += 4

		peerIDs := make([]uint64, numPeers)
		for j := 0; j < numPeers; j++ {
			if off+8 > len(data) {
				return SplitAdminRequest{}, fmt.Errorf("truncated peer ID")
			}
			peerIDs[j] = binary.BigEndian.Uint64(data[off : off+8])
			off += 8
		}
		peerIDSets[i] = peerIDs
	}

	return SplitAdminRequest{
		SplitKey:      splitKey,
		NewRegionIDs:  regionIDs,
		NewPeerIDSets: peerIDSets,
	}, nil
}

// ExecSplitAdmin executes a region split during Raft apply. It updates the
// parent peer's region metadata and returns the split result for child region
// bootstrapping.
func ExecSplitAdmin(peer *Peer, req SplitAdminRequest) (*SplitRegionResult, error) {
	region := peer.Region()

	// Validate: ensure the region hasn't already been split (stale proposal).
	// After a split, the epoch version is incremented. If another split or
	// ConfChange has already occurred, the region metadata is stale.
	// We still execute the split because the entry is committed and must be
	// applied deterministically on all replicas. Epoch validation happens
	// at propose time; at apply time we trust the log ordering.

	allPeers := make([]raft.Peer, 0)
	for _, p := range region.GetPeers() {
		allPeers = append(allPeers, raft.Peer{ID: p.GetId()})
	}

	result, err := split.ExecBatchSplit(
		region,
		[][]byte{req.SplitKey},
		req.NewRegionIDs,
		req.NewPeerIDSets,
	)
	if err != nil {
		slog.Warn("ExecSplitAdmin failed", "region", peer.RegionID(), "err", err)
		return nil, err
	}

	// Update parent region metadata atomically.
	peer.UpdateRegion(result.Derived)

	slog.Info("split: region split applied via Raft",
		"parent", peer.RegionID(),
		"splitKey", fmt.Sprintf("%x", req.SplitKey),
		"newRegions", len(result.Regions))

	return &SplitRegionResult{
		Derived: result.Derived,
		Regions: result.Regions,
	}, nil
}

// ProposeSplit proposes a split admin command via the peer's Raft group.
func (p *Peer) ProposeSplit(req SplitAdminRequest) error {
	data := MarshalSplitAdminRequest(req)
	return p.rawNode.Propose(data)
}

// applySplitAdminEntry processes a committed split admin entry.
func (p *Peer) applySplitAdminEntry(e *raftpb.Entry) {
	data := e.Data
	if !IsSplitAdmin(data) {
		return
	}
	req, err := UnmarshalSplitAdminRequest(data)
	if err != nil {
		slog.Warn("applySplitAdminEntry: unmarshal failed", "region", p.regionID, "err", err)
		return
	}

	result, err := ExecSplitAdmin(p, req)
	if err != nil {
		return
	}

	// Send result to coordinator for child region bootstrapping.
	// All replicas (not just the leader) must bootstrap child regions so that
	// the new Raft group can establish quorum. Without this, the leader sends
	// heartbeats to follower peers that don't exist, ReadIndex can't confirm
	// quorum, and reads time out.
	if p.splitResultCh != nil {
		select {
		case p.splitResultCh <- result:
		default:
			slog.Warn("splitResultCh full, dropping split result", "region", p.regionID)
		}
	}
}
