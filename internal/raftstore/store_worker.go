package raftstore

import (
	"fmt"

	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/keys"
)

// CleanupRegionData deletes all Raft-related data for a region from the engine.
// This includes raft log entries, hard state, apply state, and region state.
func CleanupRegionData(engine interface{ DeleteRange(cf string, startKey, endKey []byte) error }, regionID uint64) error {
	// Delete all raft log entries.
	logStart, logEnd := keys.RaftLogKeyRange(regionID)
	if err := engine.DeleteRange(cfnames.CFRaft, logStart, logEnd); err != nil {
		return fmt.Errorf("raftstore: cleanup raft logs for region %d: %w", regionID, err)
	}

	// Delete raft hard state.
	raftStateKey := keys.RaftStateKey(regionID)
	if err := engine.DeleteRange(cfnames.CFRaft, raftStateKey, nextKey(raftStateKey)); err != nil {
		return fmt.Errorf("raftstore: cleanup raft state for region %d: %w", regionID, err)
	}

	// Delete apply state.
	applyStateKey := keys.ApplyStateKey(regionID)
	if err := engine.DeleteRange(cfnames.CFRaft, applyStateKey, nextKey(applyStateKey)); err != nil {
		return fmt.Errorf("raftstore: cleanup apply state for region %d: %w", regionID, err)
	}

	// Delete region state (uses RegionMetaPrefix, not RegionRaftPrefix).
	regionStateKey := keys.RegionStateKey(regionID)
	if err := engine.DeleteRange(cfnames.CFRaft, regionStateKey, nextKey(regionStateKey)); err != nil {
		return fmt.Errorf("raftstore: cleanup region state for region %d: %w", regionID, err)
	}

	return nil
}

// nextKey returns a key with 0x00 appended, forming an exclusive upper bound
// for DeleteRange to cover exactly one key.
func nextKey(key []byte) []byte {
	result := make([]byte, len(key)+1)
	copy(result, key)
	return result
}
