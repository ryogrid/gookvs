package raftstore

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecPrepareMerge(t *testing.T) {
	source := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("m"),
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 5},
	}
	target := &metapb.Region{
		Id:       2,
		StartKey: []byte("m"),
		EndKey:   []byte("z"),
	}

	result, err := ExecPrepareMerge(source, target, 10, 15)
	require.NoError(t, err)

	assert.Equal(t, uint64(6), result.Region.RegionEpoch.Version)  // 5+1
	assert.Equal(t, uint64(4), result.Region.RegionEpoch.ConfVer)  // 3+1
	assert.Equal(t, uint64(10), result.State.MinIndex)
	assert.Equal(t, uint64(15), result.State.Commit)
	assert.Equal(t, uint64(2), result.State.Target.GetId())
}

func TestExecCommitMerge_RightMerge(t *testing.T) {
	target := &metapb.Region{
		Id:          2,
		StartKey:    []byte("a"),
		EndKey:      []byte("m"),
		Peers:       []*metapb.Peer{{Id: 2, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 3},
	}
	source := &metapb.Region{
		Id:          1,
		StartKey:    []byte("m"),
		EndKey:      []byte("z"),
		Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 5},
	}

	result, err := ExecCommitMerge(target, source)
	require.NoError(t, err)

	// Target should expand: [a, z).
	assert.Equal(t, []byte("a"), result.Region.StartKey)
	assert.Equal(t, []byte("z"), result.Region.EndKey)
	// Version = max(5, 3) + 1 = 6.
	assert.Equal(t, uint64(6), result.Region.RegionEpoch.Version)
}

func TestExecCommitMerge_LeftMerge(t *testing.T) {
	target := &metapb.Region{
		Id:          2,
		StartKey:    []byte("m"),
		EndKey:      []byte("z"),
		Peers:       []*metapb.Peer{{Id: 2, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 3},
	}
	source := &metapb.Region{
		Id:          1,
		StartKey:    []byte("a"),
		EndKey:      []byte("m"),
		Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
	}

	result, err := ExecCommitMerge(target, source)
	require.NoError(t, err)

	// Target should expand: [a, z).
	assert.Equal(t, []byte("a"), result.Region.StartKey)
	assert.Equal(t, []byte("z"), result.Region.EndKey)
	// Version = max(2, 3) + 1 = 4.
	assert.Equal(t, uint64(4), result.Region.RegionEpoch.Version)
}

func TestExecRollbackMerge(t *testing.T) {
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte("a"),
		EndKey:      []byte("m"),
		Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 4, Version: 6},
	}

	mergeState := &MergeState{
		MinIndex: 10,
		Commit:   15,
	}

	result, err := ExecRollbackMerge(region, 15, mergeState)
	require.NoError(t, err)

	// Version bumped by 1.
	assert.Equal(t, uint64(7), result.Region.RegionEpoch.Version)
	// ConfVer unchanged.
	assert.Equal(t, uint64(4), result.Region.RegionEpoch.ConfVer)
	assert.Equal(t, uint64(15), result.Commit)
}

func TestExecRollbackMerge_CommitMismatch(t *testing.T) {
	region := &metapb.Region{
		Id:          1,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	mergeState := &MergeState{
		Commit: 15,
	}

	_, err := ExecRollbackMerge(region, 20, mergeState)
	assert.Error(t, err, "should fail when commit index does not match")
}

func TestPrepareMerge_RejectsNilInput(t *testing.T) {
	_, err := ExecPrepareMerge(nil, nil, 0, 0)
	assert.Error(t, err)
}

func TestCommitMerge_RejectsNilInput(t *testing.T) {
	_, err := ExecCommitMerge(nil, nil)
	assert.Error(t, err)
}

func TestRollbackMerge_RejectsNilInput(t *testing.T) {
	_, err := ExecRollbackMerge(nil, 0, nil)
	assert.Error(t, err)
}

func TestMergeState_Fields(t *testing.T) {
	state := &MergeState{
		MinIndex: 42,
		Commit:   100,
		Target:   &metapb.Region{Id: 5},
	}

	assert.Equal(t, uint64(42), state.MinIndex)
	assert.Equal(t, uint64(100), state.Commit)
	assert.Equal(t, uint64(5), state.Target.GetId())
}

func TestCatchUpLogs_Signal(t *testing.T) {
	cul := &CatchUpLogs{
		TargetRegionID: 10,
	}

	assert.Equal(t, uint64(0), cul.LogsUpToDate.Load())
	cul.LogsUpToDate.Store(1)
	assert.Equal(t, uint64(1), cul.LogsUpToDate.Load())
}
