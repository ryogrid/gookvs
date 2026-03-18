package e2e

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookvs/internal/pd"
	"github.com/ryogrid/gookvs/internal/raftstore"
	"github.com/ryogrid/gookvs/pkg/pdclient"
)

// TestRegionSplitWithPD tests the end-to-end region split flow:
// 1. Start PD server
// 2. Bootstrap cluster with 1 region
// 3. Request split IDs from PD (AskBatchSplit)
// 4. Execute region split at the data layer
// 5. Report split results to PD (ReportBatchSplit)
// 6. Verify PD metadata reflects the split
func TestRegionSplitWithPD(t *testing.T) {
	// Start PD server.
	pdCfg := pd.DefaultPDServerConfig()
	pdCfg.ListenAddr = "127.0.0.1:0"
	pdCfg.ClusterID = 1
	pdCfg.MaxPeerCount = 1

	pdSrv, err := pd.NewPDServer(pdCfg)
	require.NoError(t, err)
	require.NoError(t, pdSrv.Start())
	defer pdSrv.Stop()

	// Create PD client.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pdClient, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{pdSrv.Addr()},
	})
	require.NoError(t, err)
	defer pdClient.Close()

	// Bootstrap cluster.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	initialRegion := &metapb.Region{
		Id:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	_, err = pdClient.Bootstrap(ctx, store, initialRegion)
	require.NoError(t, err)

	// Request split IDs from PD.
	splitResp, err := pdClient.AskBatchSplit(ctx, initialRegion, 1)
	require.NoError(t, err)
	require.Len(t, splitResp.GetIds(), 1)

	splitID := splitResp.GetIds()[0]
	newRegionID := splitID.GetNewRegionId()
	newPeerIDs := splitID.GetNewPeerIds()
	require.NotZero(t, newRegionID)
	require.NotEmpty(t, newPeerIDs)

	// Simulate the split: create left and right regions.
	splitKey := []byte("m")
	leftRegion := &metapb.Region{
		Id:       initialRegion.Id,
		StartKey: nil,
		EndKey:   splitKey,
		Peers:    initialRegion.Peers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}
	rightRegion := &metapb.Region{
		Id:       newRegionID,
		StartKey: splitKey,
		EndKey:   nil,
		Peers:    []*metapb.Peer{{Id: newPeerIDs[0], StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}

	// Report split to PD.
	err = pdClient.ReportBatchSplit(ctx, []*metapb.Region{leftRegion, rightRegion})
	require.NoError(t, err)

	// Verify PD knows about both regions.
	rLeft, _, err := pdClient.GetRegion(ctx, []byte("abc"))
	require.NoError(t, err)
	require.NotNil(t, rLeft)
	assert.Equal(t, leftRegion.GetId(), rLeft.GetId(), "key 'abc' < 'm' should belong to left region")
	assert.Equal(t, splitKey, rLeft.GetEndKey())

	rRight, _, err := pdClient.GetRegion(ctx, []byte("xyz"))
	require.NoError(t, err)
	require.NotNil(t, rRight)
	assert.Equal(t, rightRegion.GetId(), rRight.GetId(), "key 'xyz' >= 'm' should belong to right region")
	assert.Equal(t, splitKey, rRight.GetStartKey())

	// Verify region by ID.
	rByID, _, err := pdClient.GetRegionByID(ctx, newRegionID)
	require.NoError(t, err)
	require.NotNil(t, rByID)
	assert.Equal(t, newRegionID, rByID.GetId())

	t.Log("Region split with PD passed")
}

// TestRegionMergeLogic tests the region merge logic:
// ExecPrepareMerge -> ExecCommitMerge -> verify key range expansion.
func TestRegionMergeLogic(t *testing.T) {
	// Create two adjacent regions: left [nil, "m") and right ["m", nil).
	leftRegion := &metapb.Region{
		Id:       1,
		StartKey: nil,
		EndKey:   []byte("m"),
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}
	rightRegion := &metapb.Region{
		Id:       2,
		StartKey: []byte("m"),
		EndKey:   nil,
		Peers:    []*metapb.Peer{{Id: 2, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}

	// Phase 1: PrepareMerge on the source (right region -> merging into left).
	prepareResult, err := raftstore.ExecPrepareMerge(rightRegion, leftRegion, 10, 15)
	require.NoError(t, err)
	require.NotNil(t, prepareResult)

	// Verify epoch was bumped.
	assert.Equal(t, uint64(3), prepareResult.Region.RegionEpoch.Version, "version bumped on PrepareMerge")
	assert.Equal(t, uint64(2), prepareResult.Region.RegionEpoch.ConfVer, "conf_ver bumped on PrepareMerge")
	assert.Equal(t, leftRegion, prepareResult.State.Target, "target should be the left region")

	// Phase 2: CommitMerge on the target (left absorbs right).
	commitResult, err := raftstore.ExecCommitMerge(leftRegion, rightRegion)
	require.NoError(t, err)
	require.NotNil(t, commitResult)

	// The merged region should have expanded key range.
	mergedRegion := commitResult.Region
	assert.True(t, len(mergedRegion.GetStartKey()) == 0, "merged region should have nil start key")
	assert.True(t, len(mergedRegion.GetEndKey()) == 0, "merged region should have nil end key (covers entire keyspace)")
	assert.Greater(t, mergedRegion.RegionEpoch.Version, leftRegion.RegionEpoch.Version, "merged epoch > original")

	t.Log("Region merge logic passed")
}

// TestRegionMergeRollback tests the rollback path of a merge.
func TestRegionMergeRollback(t *testing.T) {
	region := &metapb.Region{
		Id:       1,
		StartKey: nil,
		EndKey:   []byte("m"),
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 2,
		},
	}

	mergeState := &raftstore.MergeState{
		MinIndex: 10,
		Commit:   15,
		Target:   &metapb.Region{Id: 2},
	}

	rollbackResult, err := raftstore.ExecRollbackMerge(region, 15, mergeState)
	require.NoError(t, err)
	require.NotNil(t, rollbackResult)

	// Version should be bumped.
	assert.Equal(t, uint64(3), rollbackResult.Region.RegionEpoch.Version, "version bumped on rollback")
	// Key range should be unchanged.
	assert.Nil(t, rollbackResult.Region.GetStartKey())
	assert.Equal(t, []byte("m"), rollbackResult.Region.GetEndKey())

	t.Log("Region merge rollback passed")
}

// TestRegionSplitAndMergeRoundTrip tests split followed by merge, verifying the
// full lifecycle.
func TestRegionSplitAndMergeRoundTrip(t *testing.T) {
	// Start PD.
	pdCfg := pd.DefaultPDServerConfig()
	pdCfg.ListenAddr = "127.0.0.1:0"
	pdCfg.ClusterID = 1
	pdCfg.MaxPeerCount = 1

	pdSrv, err := pd.NewPDServer(pdCfg)
	require.NoError(t, err)
	require.NoError(t, pdSrv.Start())
	defer pdSrv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pdClient, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{pdSrv.Addr()},
	})
	require.NoError(t, err)
	defer pdClient.Close()

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	_, err = pdClient.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// --- SPLIT ---
	splitResp, err := pdClient.AskBatchSplit(ctx, region, 1)
	require.NoError(t, err)
	splitID := splitResp.GetIds()[0]
	splitKey := []byte("m")

	leftRegion := &metapb.Region{
		Id: 1, StartKey: nil, EndKey: splitKey,
		Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
	}
	rightRegion := &metapb.Region{
		Id: splitID.GetNewRegionId(), StartKey: splitKey, EndKey: nil,
		Peers:       []*metapb.Peer{{Id: splitID.GetNewPeerIds()[0], StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
	}
	err = pdClient.ReportBatchSplit(ctx, []*metapb.Region{leftRegion, rightRegion})
	require.NoError(t, err)

	// Verify split.
	rL, _, err := pdClient.GetRegion(ctx, []byte("abc"))
	require.NoError(t, err)
	assert.Equal(t, leftRegion.GetId(), rL.GetId())
	rR, _, err := pdClient.GetRegion(ctx, []byte("xyz"))
	require.NoError(t, err)
	assert.Equal(t, rightRegion.GetId(), rR.GetId())

	// --- MERGE (right into left) ---
	commitResult, err := raftstore.ExecCommitMerge(leftRegion, rightRegion)
	require.NoError(t, err)
	mergedRegion := commitResult.Region

	// Report merged region to PD.
	err = pdClient.ReportBatchSplit(ctx, []*metapb.Region{mergedRegion})
	require.NoError(t, err)

	// Verify: the merged region covers entire keyspace.
	rMerged, _, err := pdClient.GetRegionByID(ctx, leftRegion.GetId())
	require.NoError(t, err)
	require.NotNil(t, rMerged)
	assert.True(t, len(rMerged.GetStartKey()) == 0 || bytes.Equal(rMerged.GetStartKey(), mergedRegion.GetStartKey()))
	assert.True(t, len(rMerged.GetEndKey()) == 0 || bytes.Equal(rMerged.GetEndKey(), mergedRegion.GetEndKey()))

	t.Log("Region split and merge round trip passed")
}
