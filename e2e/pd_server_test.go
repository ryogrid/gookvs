package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookvs/internal/pd"
	"github.com/ryogrid/gookvs/pkg/pdclient"
)

// startPDServer creates and starts a PD server on a random port, returning the server and its address.
func startPDServer(t *testing.T) (*pd.PDServer, string) {
	t.Helper()
	cfg := pd.DefaultPDServerConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.ClusterID = 1

	srv, err := pd.NewPDServer(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Stop() })
	return srv, srv.Addr()
}

// newPDClient creates a PD client connected to the given PD address.
func newPDClient(t *testing.T, addr string) pdclient.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{addr},
	})
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	return client
}

// TestPDServerBootstrapAndTSO verifies PD bootstrap, TSO allocation, and cluster metadata.
func TestPDServerBootstrapAndTSO(t *testing.T) {
	_, addr := startPDServer(t)
	client := newPDClient(t, addr)
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
	_, addr := startPDServer(t)
	client := newPDClient(t, addr)
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
	_, addr := startPDServer(t)
	client := newPDClient(t, addr)
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
	_, addr := startPDServer(t)
	client := newPDClient(t, addr)
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
