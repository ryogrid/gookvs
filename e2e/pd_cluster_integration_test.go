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
	"github.com/ryogrid/gookvs/internal/server"
	"github.com/ryogrid/gookvs/pkg/pdclient"
)

// TestPDClusterStoreAndRegionHeartbeat tests the full heartbeat loop:
// PD server -> PD client -> PDWorker sends store/region heartbeats.
func TestPDClusterStoreAndRegionHeartbeat(t *testing.T) {
	// Start PD server.
	pdCfg := pd.DefaultPDServerConfig()
	pdCfg.ListenAddr = "127.0.0.1:0"
	pdCfg.ClusterID = 1
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

	// Bootstrap.
	store := &metapb.Store{Id: 1, Address: "127.0.0.1:20160"}
	region := &metapb.Region{
		Id:    1,
		Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
	}
	_, err = pdClient.Bootstrap(ctx, store, region)
	require.NoError(t, err)

	// Create PDWorker with a fast heartbeat interval.
	workerCfg := server.DefaultPDWorkerConfig()
	workerCfg.StoreID = 1
	workerCfg.PDClient = pdClient
	workerCfg.StoreHeartbeatInterval = 100 * time.Millisecond
	workerCfg.Coordinator = nil // No coordinator needed for heartbeat test.

	pdWorker := server.NewPDWorker(workerCfg)
	pdWorker.Run()
	defer pdWorker.Stop()

	// Send a region heartbeat via the PDWorker.
	pdWorker.ScheduleTask(server.PDTask{
		Type: server.PDTaskRegionHeartbeat,
		Data: &server.RegionHeartbeatData{
			Term:   1,
			Region: region,
			Peer:   &metapb.Peer{Id: 1, StoreId: 1},
		},
	})

	// Send a store heartbeat explicitly.
	err = pdClient.StoreHeartbeat(ctx, &pdpb.StoreStats{
		StoreId:     1,
		RegionCount: 1,
	})
	require.NoError(t, err)

	// Wait for store heartbeat loop to fire at least once.
	time.Sleep(300 * time.Millisecond)

	// Verify the store is registered.
	s, err := pdClient.GetStore(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, s)
	assert.Equal(t, uint64(1), s.GetId())

	t.Log("PD cluster store and region heartbeat passed")
}

// TestPDClusterTSOForTransactions tests using PD TSO for transaction timestamps.
func TestPDClusterTSOForTransactions(t *testing.T) {
	// Start PD server.
	pdCfg := pd.DefaultPDServerConfig()
	pdCfg.ListenAddr = "127.0.0.1:0"
	pdCfg.ClusterID = 1
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

	// Allocate multiple timestamps and verify monotonicity.
	var prevTS uint64
	for i := 0; i < 100; i++ {
		ts, err := pdClient.GetTS(ctx)
		require.NoError(t, err)
		tsUint := ts.ToUint64()
		assert.Greater(t, tsUint, prevTS, "TS %d must be > previous %d (iteration %d)", tsUint, prevTS, i)
		prevTS = tsUint
	}

	t.Log("PD TSO monotonicity for transactions passed")
}

// TestPDClusterGCSafePoint tests the GC safe point management via PD.
func TestPDClusterGCSafePoint(t *testing.T) {
	// Start PD server.
	pdCfg := pd.DefaultPDServerConfig()
	pdCfg.ListenAddr = "127.0.0.1:0"
	pdCfg.ClusterID = 1
	pdSrv, err := pd.NewPDServer(pdCfg)
	require.NoError(t, err)
	require.NoError(t, pdSrv.Start())
	defer pdSrv.Stop()

	// Connect directly to PD gRPC (pdclient doesn't expose GC safe point, so use direct gRPC).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pdClient, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{pdSrv.Addr()},
	})
	require.NoError(t, err)
	defer pdClient.Close()

	// Use PD server directly to test GC safe point (since pdclient.Client doesn't expose it).
	// GetGCSafePoint should be 0 initially.
	getSPResp, err := pdSrv.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), getSPResp.GetSafePoint())

	// Update GC safe point.
	updateResp, err := pdSrv.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{SafePoint: 1000})
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), updateResp.GetNewSafePoint())

	// GC safe point should not go backwards.
	updateResp, err = pdSrv.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{SafePoint: 500})
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), updateResp.GetNewSafePoint(), "safe point should not go backwards")

	t.Log("PD GC safe point management passed")
}
