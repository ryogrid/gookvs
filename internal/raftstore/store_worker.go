package raftstore

// This file defines store worker types and utilities.
// The actual store worker goroutine runs in internal/server/coordinator.go
// as StoreCoordinator.runStoreWorker().

// CleanupRegionData deletes all Raft-related data for a region from the engine.
// This includes raft log entries, hard state, apply state, and region state.
func CleanupRegionData(engine interface{ DeleteRange(cf string, startKey, endKey []byte) error }, regionID uint64) error {
	// TODO: implement full cleanup with proper key encoding.
	// For now, this is a placeholder that will be filled in when
	// the full key encoding for raft log ranges is wired up.
	return nil
}
