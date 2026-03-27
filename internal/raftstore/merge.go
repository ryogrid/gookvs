package raftstore

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
)

// PeerState represents the lifecycle state of a region peer.
type PeerState int

const (
	// PeerStateNormal is the default state.
	PeerStateNormal PeerState = iota
	// PeerStateMerging indicates the source region has committed PrepareMerge.
	PeerStateMerging
	// PeerStateTombstone indicates the region has been destroyed (merged or removed).
	PeerStateTombstone
)

// MergeState tracks the state of a pending merge on the source region.
type MergeState struct {
	MinIndex uint64         // min(matched_index) across all followers at PrepareMerge time
	Commit   uint64         // commit index of the PrepareMerge entry
	Target   *metapb.Region // target region to merge into
}

// CatchUpLogs is sent from the target apply worker to the source peer
// to ensure the source has applied all entries before CommitMerge finalizes.
type CatchUpLogs struct {
	TargetRegionID uint64
	LogsUpToDate   atomic.Uint64 // 0 = not ready, non-zero = ready
}

// MergeResultKind classifies how a merge completed.
type MergeResultKind int

const (
	// MergeResultFromTargetLog indicates normal completion via target's commit log.
	MergeResultFromTargetLog MergeResultKind = iota
	// MergeResultFromTargetSnap indicates completion via target snapshot.
	MergeResultFromTargetSnap
	// MergeResultStale indicates a stale merge result (already handled).
	MergeResultStale
)

// PrepareMergeResult is the data for ExecResultTypePrepareMerge.
type PrepareMergeResult struct {
	Region *metapb.Region
	State  *MergeState
}

// CommitMergeResult is the data for ExecResultTypeCommitMerge.
type CommitMergeResult struct {
	Index  uint64         // commit index of the CommitMerge entry
	Region *metapb.Region // target region after merge (expanded key range)
	Source *metapb.Region // source region (now tombstoned)
}

// RollbackMergeResult is the data for ExecResultTypeRollbackMerge.
type RollbackMergeResult struct {
	Region *metapb.Region
	Commit uint64
}

// Additional ExecResultType constants for merge operations.
const (
	ExecResultTypePrepareMerge  ExecResultType = 10
	ExecResultTypeCommitMerge   ExecResultType = 11
	ExecResultTypeRollbackMerge ExecResultType = 12
)

// ExecPrepareMerge simulates the apply-side PrepareMerge execution.
// It bumps both version and conf_ver, sets the region state to Merging.
func ExecPrepareMerge(region *metapb.Region, target *metapb.Region, minIndex, commitIndex uint64) (*PrepareMergeResult, error) {
	if region == nil || target == nil {
		return nil, fmt.Errorf("merge: nil region or target")
	}

	// Clone and update epoch.
	updated := cloneMergeRegion(region)
	if updated.RegionEpoch == nil {
		updated.RegionEpoch = &metapb.RegionEpoch{}
	}
	updated.RegionEpoch.Version++
	updated.RegionEpoch.ConfVer++

	state := &MergeState{
		MinIndex: minIndex,
		Commit:   commitIndex,
		Target:   target,
	}

	return &PrepareMergeResult{
		Region: updated,
		State:  state,
	}, nil
}

// ExecCommitMerge simulates the apply-side CommitMerge execution.
// It extends the target region's key range to absorb the source region.
func ExecCommitMerge(target *metapb.Region, source *metapb.Region) (*CommitMergeResult, error) {
	if target == nil || source == nil {
		return nil, fmt.Errorf("merge: nil target or source")
	}

	// Clone target for mutation.
	merged := cloneMergeRegion(target)
	if merged.RegionEpoch == nil {
		merged.RegionEpoch = &metapb.RegionEpoch{}
	}

	// Determine merge direction. Source must be adjacent to target.
	if len(source.GetStartKey()) > 0 && bytes.Equal(target.GetEndKey(), source.GetStartKey()) {
		// Right merge: source is right-adjacent to target.
		merged.EndKey = append([]byte(nil), source.GetEndKey()...)
	} else if len(source.GetEndKey()) > 0 && bytes.Equal(target.GetStartKey(), source.GetEndKey()) {
		// Left merge: source is left-adjacent to target.
		merged.StartKey = append([]byte(nil), source.GetStartKey()...)
	} else {
		return nil, fmt.Errorf("merge: source region %d [%x, %x) is not adjacent to target region %d [%x, %x)",
			source.GetId(), source.GetStartKey(), source.GetEndKey(),
			target.GetId(), target.GetStartKey(), target.GetEndKey())
	}

	// Set epoch: max(source.version, target.version) + 1.
	srcVer := source.GetRegionEpoch().GetVersion()
	tgtVer := merged.RegionEpoch.Version
	if srcVer > tgtVer {
		merged.RegionEpoch.Version = srcVer + 1
	} else {
		merged.RegionEpoch.Version = tgtVer + 1
	}

	return &CommitMergeResult{
		Region: merged,
		Source: source,
	}, nil
}

// ExecRollbackMerge simulates the apply-side RollbackMerge execution.
// It resets the region to Normal state and bumps the epoch version.
func ExecRollbackMerge(region *metapb.Region, expectedCommit uint64, mergeState *MergeState) (*RollbackMergeResult, error) {
	if region == nil {
		return nil, fmt.Errorf("merge: nil region")
	}

	// Validate commit index matches.
	if mergeState != nil && expectedCommit != 0 && mergeState.Commit != expectedCommit {
		return nil, fmt.Errorf("merge: rollback commit %d != merge state commit %d", expectedCommit, mergeState.Commit)
	}

	// Clone and update epoch.
	updated := cloneMergeRegion(region)
	if updated.RegionEpoch == nil {
		updated.RegionEpoch = &metapb.RegionEpoch{}
	}
	updated.RegionEpoch.Version++

	return &RollbackMergeResult{
		Region: updated,
		Commit: expectedCommit,
	}, nil
}

func cloneMergeRegion(r *metapb.Region) *metapb.Region {
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
