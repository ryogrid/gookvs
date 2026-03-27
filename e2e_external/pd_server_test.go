package e2e_external_test

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/e2elib"
)

// startPDOnly creates a standalone PD node (no gookv-server needed).
func startPDOnly(t *testing.T) *e2elib.PDNode {
	t.Helper()
	e2elib.SkipIfNoBinary(t, "gookv-pd")

	alloc := e2elib.NewPortAllocator()
	t.Cleanup(func() { alloc.ReleaseAll() })

	pd := e2elib.NewPDNode(t, alloc, e2elib.PDNodeConfig{})
	require.NoError(t, pd.Start())
	require.NoError(t, pd.WaitForReady(30*1e9)) // 30 seconds
	return pd
}

// TestPDServerBootstrapAndTSO verifies PD bootstrap, TSO allocation, and cluster metadata.
func TestPDServerBootstrapAndTSO(t *testing.T) {
	pd := startPDOnly(t)
	client := pd.Client()
	ctx := context.Background()

	// Before bootstrap, IsBootstrapped should be false.
	bootstrapped, err := client.IsBootstrapped(ctx)
	require.NoError(t, err)
	assert.False(t, bootstrapped, "cluster should not be bootstrapped initially")

	// Bootstrap the cluster.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}
	_, err = client.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// After bootstrap, IsBootstrapped should be true.
	bootstrapped, err = client.IsBootstrapped(ctx)
	require.NoError(t, err)
	assert.True(t, bootstrapped, "cluster should be bootstrapped")

	// TSO allocation: timestamps must be monotonically increasing.
	ts1, err := client.GetTS(ctx)
	require.NoError(t, err)
	ts2, err := client.GetTS(ctx)
	require.NoError(t, err)
	assert.Greater(t, ts2.ToUint64(), ts1.ToUint64(), "TSO must be monotonically increasing")

	// AllocID: IDs must be unique and increasing.
	id1, err := client.AllocID(ctx)
	require.NoError(t, err)
	id2, err := client.AllocID(ctx)
	require.NoError(t, err)
	assert.Greater(t, id2, id1, "AllocID must be monotonically increasing")

	t.Log("PD bootstrap, TSO, and AllocID passed")
}

// TestPDServerStoreAndRegionMetadata verifies store and region metadata management via PD.
func TestPDServerStoreAndRegionMetadata(t *testing.T) {
	pd := startPDOnly(t)
	client := pd.Client()
	ctx := context.Background()

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
	}
	_, err := client.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// Put additional stores.
	err = client.PutStore(ctx, &metapb.Store{Id: 2, Address: "127.0.0.1:20161"})
	require.NoError(t, err)
	err = client.PutStore(ctx, &metapb.Store{Id: 3, Address: "127.0.0.1:20162"})
	require.NoError(t, err)

	// GetStore should return the stored metadata.
	s, err := client.GetStore(ctx, 2)
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, uint64(2), s.GetId())
	assert.Equal(t, "127.0.0.1:20161", s.GetAddress())

	// GetRegionByID should return the bootstrapped region.
	r, _, err := client.GetRegionByID(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.Equal(t, uint64(1), r.GetId())

	// GetRegion by key should return region covering the key.
	r, _, err = client.GetRegion(ctx, []byte("some-key"))
	require.NoError(t, err)
	require.NotNil(t, r, "region should cover any key since it has no start/end key bounds")

	t.Log("PD store and region metadata passed")
}

// TestPDAskBatchSplitAndReport verifies the AskBatchSplit and ReportBatchSplit RPCs.
func TestPDAskBatchSplitAndReport(t *testing.T) {
	pd := startPDOnly(t)
	client := pd.Client()
	ctx := context.Background()

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
	}
	_, err := client.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// AskBatchSplit for 1 split.
	resp, err := client.AskBatchSplit(ctx, region, 1)
	require.NoError(t, err)
	require.Len(t, resp.GetIds(), 1, "should get 1 split ID")

	splitID := resp.GetIds()[0]
	assert.NotZero(t, splitID.GetNewRegionId(), "new region ID should be non-zero")
	assert.NotEmpty(t, splitID.GetNewPeerIds(), "new peer IDs should be allocated")

	// Report the split: create two regions (left and right).
	leftRegion := &metapb.Region{
		Id:       region.GetId(),
		StartKey: nil,
		EndKey:   []byte("m"),
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			Version: 2,
			ConfVer: 1,
		},
	}
	rightRegion := &metapb.Region{
		Id:       splitID.GetNewRegionId(),
		StartKey: []byte("m"),
		EndKey:   nil,
		Peers:    []*metapb.Peer{{Id: splitID.GetNewPeerIds()[0], StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{
			Version: 2,
			ConfVer: 1,
		},
	}
	err = client.ReportBatchSplit(ctx, []*metapb.Region{leftRegion, rightRegion})
	require.NoError(t, err)

	// Verify PD now has both regions.
	rLeft, _, err := client.GetRegion(ctx, []byte("abc"))
	require.NoError(t, err)
	require.NotNil(t, rLeft)
	assert.Equal(t, leftRegion.GetId(), rLeft.GetId(), "key 'abc' should map to the left region")

	rRight, _, err := client.GetRegion(ctx, []byte("xyz"))
	require.NoError(t, err)
	require.NotNil(t, rRight)
	assert.Equal(t, rightRegion.GetId(), rRight.GetId(), "key 'xyz' should map to the right region")

	t.Log("PD AskBatchSplit and ReportBatchSplit passed")
}

// TestPDStoreHeartbeat verifies sending store heartbeats to PD.
func TestPDStoreHeartbeat(t *testing.T) {
	pd := startPDOnly(t)
	client := pd.Client()
	ctx := context.Background()

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
	}
	_, err := client.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// Send a store heartbeat (should not error).
	err = client.StoreHeartbeat(ctx, &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 5,
		IsBusy:      false,
	})
	require.NoError(t, err)

	t.Log("PD store heartbeat passed")
}
