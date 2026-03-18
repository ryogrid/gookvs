package raftstore

import (
	"testing"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanupRegionData(t *testing.T) {
	engine := newTestEngine(t)
	regionID := uint64(42)

	// Seed raft log entries.
	for i := uint64(1); i <= 10; i++ {
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftLogKey(regionID, i), []byte{byte(i)}))
	}
	// Seed raft hard state.
	require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftStateKey(regionID), []byte("hard-state")))
	// Seed apply state.
	require.NoError(t, engine.Put(cfnames.CFRaft, keys.ApplyStateKey(regionID), []byte("apply-state")))
	// Seed region state.
	require.NoError(t, engine.Put(cfnames.CFRaft, keys.RegionStateKey(regionID), []byte("region-state")))

	// Verify all state exists.
	for i := uint64(1); i <= 10; i++ {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftLogKey(regionID, i))
		require.NoError(t, err, "raft log %d should exist before cleanup", i)
	}
	_, err := engine.Get(cfnames.CFRaft, keys.RaftStateKey(regionID))
	require.NoError(t, err)
	_, err = engine.Get(cfnames.CFRaft, keys.ApplyStateKey(regionID))
	require.NoError(t, err)
	_, err = engine.Get(cfnames.CFRaft, keys.RegionStateKey(regionID))
	require.NoError(t, err)

	// Run cleanup.
	require.NoError(t, CleanupRegionData(engine, regionID))

	// Verify all state is gone.
	for i := uint64(1); i <= 10; i++ {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftLogKey(regionID, i))
		assert.ErrorIs(t, err, traits.ErrNotFound, "raft log %d should be deleted", i)
	}
	_, err = engine.Get(cfnames.CFRaft, keys.RaftStateKey(regionID))
	assert.ErrorIs(t, err, traits.ErrNotFound, "raft state should be deleted")
	_, err = engine.Get(cfnames.CFRaft, keys.ApplyStateKey(regionID))
	assert.ErrorIs(t, err, traits.ErrNotFound, "apply state should be deleted")
	_, err = engine.Get(cfnames.CFRaft, keys.RegionStateKey(regionID))
	assert.ErrorIs(t, err, traits.ErrNotFound, "region state should be deleted")
}

func TestCleanupRegionData_IsolatesRegions(t *testing.T) {
	engine := newTestEngine(t)

	// Seed state for regions 1 and 2.
	for _, rid := range []uint64{1, 2} {
		for i := uint64(1); i <= 5; i++ {
			require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftLogKey(rid, i), []byte{byte(i)}))
		}
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.RaftStateKey(rid), []byte("hs")))
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.ApplyStateKey(rid), []byte("as")))
		require.NoError(t, engine.Put(cfnames.CFRaft, keys.RegionStateKey(rid), []byte("rs")))
	}

	// Cleanup region 1 only.
	require.NoError(t, CleanupRegionData(engine, 1))

	// Region 1 state should be gone.
	_, err := engine.Get(cfnames.CFRaft, keys.RaftStateKey(1))
	assert.ErrorIs(t, err, traits.ErrNotFound)

	// Region 2 state should be intact.
	for i := uint64(1); i <= 5; i++ {
		_, err := engine.Get(cfnames.CFRaft, keys.RaftLogKey(2, i))
		require.NoError(t, err, "region 2 raft log %d should survive", i)
	}
	_, err = engine.Get(cfnames.CFRaft, keys.RaftStateKey(2))
	require.NoError(t, err, "region 2 raft state should survive")
	_, err = engine.Get(cfnames.CFRaft, keys.ApplyStateKey(2))
	require.NoError(t, err, "region 2 apply state should survive")
	_, err = engine.Get(cfnames.CFRaft, keys.RegionStateKey(2))
	require.NoError(t, err, "region 2 region state should survive")
}
