package e2e

import (
	"path/filepath"
	"testing"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/raftstore"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCleanupRegionDataE2E verifies that CleanupRegionData removes all
// persisted Raft state for a region from a real RocksDB engine while
// leaving other regions' data intact.
func TestCleanupRegionDataE2E(t *testing.T) {
	dir := t.TempDir()
	engine, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })

	targetRegion := uint64(10)
	otherRegion := uint64(20)

	// Seed raft state for both regions.
	for _, rid := range []uint64{targetRegion, otherRegion} {
		for i := uint64(1); i <= 10; i++ {
			require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftLogKey(rid, i), []byte("log-entry")))
		}
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftStateKey(rid), []byte("hard-state")))
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.ApplyStateKey(rid), []byte("apply-state")))
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.RegionStateKey(rid), []byte("region-state")))
	}

	// Verify all state exists for both regions.
	for _, rid := range []uint64{targetRegion, otherRegion} {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftStateKey(rid))
		require.NoError(t, err, "region %d raft state should exist before cleanup", rid)
	}

	// Clean up target region.
	require.NoError(t, raftstore.CleanupRegionData(engine, targetRegion))

	// Target region: all state should be gone.
	for i := uint64(1); i <= 10; i++ {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftLogKey(targetRegion, i))
		assert.ErrorIs(t, err, traits.ErrNotFound, "target region raft log %d should be deleted", i)
	}
	_, err = engine.Get(cfnames.CFRaft, keys.RaftStateKey(targetRegion))
	assert.ErrorIs(t, err, traits.ErrNotFound, "target region raft state should be deleted")
	_, err = engine.Get(cfnames.CFRaft, keys.ApplyStateKey(targetRegion))
	assert.ErrorIs(t, err, traits.ErrNotFound, "target region apply state should be deleted")
	_, err = engine.Get(cfnames.CFRaft, keys.RegionStateKey(targetRegion))
	assert.ErrorIs(t, err, traits.ErrNotFound, "target region region state should be deleted")

	// Other region: all state should be intact.
	for i := uint64(1); i <= 10; i++ {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftLogKey(otherRegion, i))
		require.NoError(t, err, "other region raft log %d should survive", i)
	}
	_, err = engine.Get(cfnames.CFRaft, keys.RaftStateKey(otherRegion))
	require.NoError(t, err, "other region raft state should survive")
	_, err = engine.Get(cfnames.CFRaft, keys.ApplyStateKey(otherRegion))
	require.NoError(t, err, "other region apply state should survive")
	_, err = engine.Get(cfnames.CFRaft, keys.RegionStateKey(otherRegion))
	require.NoError(t, err, "other region region state should survive")

	t.Log("CleanupRegionData E2E passed: target region cleaned, other region intact")
}
