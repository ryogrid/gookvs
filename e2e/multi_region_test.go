package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"

	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/raftstore"
	raftrouter "github.com/ryogrid/gookv/internal/raftstore/router"
	"github.com/ryogrid/gookv/internal/server"
	"github.com/ryogrid/gookv/internal/server/transport"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

const multiRegionNodeCount = 3

// multiRegionDef describes a region to create during cluster bootstrap.
type multiRegionDef struct {
	startKey []byte // nil means unbounded left
	endKey   []byte // nil means unbounded right
}

// multiRegionCluster holds the state of a multi-region test cluster.
type multiRegionCluster struct {
	t        *testing.T
	nodes    []*pdClusterNode
	pdClient pdclient.Client
	pdAddr   string
	regions  []*metapb.Region // metadata for each bootstrapped region
}

// newMultiRegionCluster creates a cluster with the given region definitions.
// Each region is bootstrapped across all nodes. Region and peer IDs are
// allocated from PD to guarantee uniqueness.
func newMultiRegionCluster(t *testing.T, regionDefs []multiRegionDef) *multiRegionCluster {
	t.Helper()

	// Start PD server.
	_, pdAddr := startPDServer(t)
	pdClient := newPDClient(t, pdAddr)
	ctx := context.Background()

	// Pre-allocate resolver.
	addrMap := make(map[uint64]string)
	for i := 0; i < multiRegionNodeCount; i++ {
		addrMap[uint64(i+1)] = ""
	}
	resolver := server.NewStaticStoreResolver(addrMap)

	// --- Build first region (for bootstrap) ---
	// Allocate IDs for first region.
	firstRegionID, err := pdClient.AllocID(ctx)
	require.NoError(t, err)

	firstMetaPeers := make([]*metapb.Peer, multiRegionNodeCount)
	firstRaftPeers := make([]raft.Peer, multiRegionNodeCount)
	for i := 0; i < multiRegionNodeCount; i++ {
		peerID, err := pdClient.AllocID(ctx)
		require.NoError(t, err)
		storeID := uint64(i + 1)
		firstMetaPeers[i] = &metapb.Peer{Id: peerID, StoreId: storeID}
		firstRaftPeers[i] = raft.Peer{ID: peerID}
	}

	firstRegion := &metapb.Region{
		Id:       firstRegionID,
		StartKey: regionDefs[0].startKey,
		EndKey:   regionDefs[0].endKey,
		Peers:    firstMetaPeers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}

	// Bootstrap cluster in PD with the first store and first region.
	store1 := &metapb.Store{Id: 1, Address: "placeholder"}
	_, err = pdClient.Bootstrap(ctx, store1, firstRegion)
	require.NoError(t, err)

	// Prepare additional region definitions (2nd region onwards).
	type regionBootstrap struct {
		region    *metapb.Region
		raftPeers []raft.Peer
	}
	additionalRegions := make([]regionBootstrap, 0, len(regionDefs)-1)

	for _, rDef := range regionDefs[1:] {
		regID, err := pdClient.AllocID(ctx)
		require.NoError(t, err)

		metaPeers := make([]*metapb.Peer, multiRegionNodeCount)
		raftPeers := make([]raft.Peer, multiRegionNodeCount)
		for i := 0; i < multiRegionNodeCount; i++ {
			peerID, err := pdClient.AllocID(ctx)
			require.NoError(t, err)
			storeID := uint64(i + 1)
			metaPeers[i] = &metapb.Peer{Id: peerID, StoreId: storeID}
			raftPeers[i] = raft.Peer{ID: peerID}
		}

		region := &metapb.Region{
			Id:       regID,
			StartKey: rDef.startKey,
			EndKey:   rDef.endKey,
			Peers:    metaPeers,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
		}
		additionalRegions = append(additionalRegions, regionBootstrap{
			region:    region,
			raftPeers: raftPeers,
		})
	}

	// --- Create nodes ---
	nodes := make([]*pdClusterNode, multiRegionNodeCount)

	for i := 0; i < multiRegionNodeCount; i++ {
		storeID := uint64(i + 1)

		dir := t.TempDir()
		engine, err := rocks.Open(dir)
		require.NoError(t, err)
		t.Cleanup(func() { engine.Close() })

		storage := server.NewStorage(engine)
		srvCfg := server.ServerConfig{ListenAddr: "127.0.0.1:0"}
		srv := server.NewServer(srvCfg, storage)

		raftClient := transport.NewRaftClient(resolver, transport.DefaultRaftClientConfig())
		rtr := raftrouter.New(256)

		peerCfg := raftstore.DefaultPeerConfig()
		peerCfg.RaftBaseTickInterval = 20 * time.Millisecond
		peerCfg.RaftElectionTimeoutTicks = 10
		peerCfg.RaftHeartbeatTicks = 2

		// Create PDWorker.
		nodePDClient := newPDClient(t, pdAddr)
		pdWorker := server.NewPDWorker(server.PDWorkerConfig{
			StoreID:  storeID,
			PDClient: nodePDClient,
		})

		coord := server.NewStoreCoordinator(server.StoreCoordinatorConfig{
			StoreID:  storeID,
			Engine:   engine,
			Storage:  storage,
			Router:   rtr,
			Client:   raftClient,
			PeerCfg:  peerCfg,
			PDTaskCh: pdWorker.PeerTaskCh(),
		})
		srv.SetCoordinator(coord)
		pdWorker.SetCoordinator(coord)
		pdWorker.Run()

		// Bootstrap first region.
		require.NoError(t, coord.BootstrapRegion(firstRegion, firstRaftPeers))

		// Bootstrap additional regions.
		for _, ar := range additionalRegions {
			require.NoError(t, coord.BootstrapRegion(ar.region, ar.raftPeers))
		}

		require.NoError(t, srv.Start())
		addr := srv.Addr()
		resolver.UpdateAddr(storeID, addr)

		require.NoError(t, pdClient.PutStore(ctx, &metapb.Store{Id: storeID, Address: addr}))

		nodes[i] = &pdClusterNode{
			storeID:    storeID,
			srv:        srv,
			coord:      coord,
			pdWorker:   pdWorker,
			raftClient: raftClient,
			addr:       addr,
		}
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				n.pdWorker.Stop()
				n.coord.Stop()
				n.srv.Stop()
				n.raftClient.Close()
			}
		}
	})

	// Collect all region metadata.
	allRegions := make([]*metapb.Region, 0, len(regionDefs))
	allRegions = append(allRegions, firstRegion)
	for _, ar := range additionalRegions {
		allRegions = append(allRegions, ar.region)
	}

	// Report all regions to PD so it knows about them.
	err = pdClient.ReportBatchSplit(ctx, allRegions)
	require.NoError(t, err)

	return &multiRegionCluster{
		t:        t,
		nodes:    nodes,
		pdClient: pdClient,
		pdAddr:   pdAddr,
		regions:  allRegions,
	}
}

// findRegionLeaderIdx finds the node index that is leader for the given region.
func (mc *multiRegionCluster) findRegionLeaderIdx(regionID uint64) int {
	for i, n := range mc.nodes {
		if n == nil {
			continue
		}
		peer := n.coord.GetPeer(regionID)
		if peer != nil && peer.IsLeader() {
			return i
		}
	}
	return -1
}

// waitForRegionLeader waits until a leader is elected for the given region.
func (mc *multiRegionCluster) waitForRegionLeader(regionID uint64, timeout time.Duration) int {
	mc.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if idx := mc.findRegionLeaderIdx(regionID); idx >= 0 {
			return idx
		}
		time.Sleep(50 * time.Millisecond)
	}
	mc.t.Fatalf("no leader elected for region %d within timeout", regionID)
	return -1
}

// campaignIfNeeded triggers a campaign on node 0 for the given region if no leader exists.
func (mc *multiRegionCluster) campaignIfNeeded(regionID uint64) {
	mc.t.Helper()
	if mc.findRegionLeaderIdx(regionID) >= 0 {
		return
	}
	// Try each node until campaign succeeds.
	for _, n := range mc.nodes {
		if n == nil {
			continue
		}
		peer := n.coord.GetPeer(regionID)
		if peer != nil {
			_ = peer.Campaign()
			break
		}
	}
}

// --- Multi-Region Tests ---

// TestMultiRegionKeyRouting verifies that keys are routed to the correct
// region based on key ranges. Region 1 covers ["", "m"), region 2 covers ["m", "").
func TestMultiRegionKeyRouting(t *testing.T) {
	mc := newMultiRegionCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})

	// Allow time for election and campaign if needed.
	time.Sleep(500 * time.Millisecond)
	for _, r := range mc.regions {
		mc.campaignIfNeeded(r.GetId())
	}

	// Wait for leaders for both regions.
	region1ID := mc.regions[0].GetId()
	region2ID := mc.regions[1].GetId()
	mc.waitForRegionLeader(region1ID, 15*time.Second)
	mc.waitForRegionLeader(region2ID, 15*time.Second)

	// Verify that key "alpha" (< "m") resolves to region 1.
	for _, n := range mc.nodes {
		if n == nil {
			continue
		}
		resolvedRegion := n.coord.ResolveRegionForKey([]byte("alpha"))
		assert.Equal(t, region1ID, resolvedRegion,
			"key 'alpha' should resolve to region 1 on store %d", n.storeID)
		break // One node is enough to verify routing.
	}

	// Verify that key "zebra" (>= "m") resolves to region 2.
	for _, n := range mc.nodes {
		if n == nil {
			continue
		}
		resolvedRegion := n.coord.ResolveRegionForKey([]byte("zebra"))
		assert.Equal(t, region2ID, resolvedRegion,
			"key 'zebra' should resolve to region 2 on store %d", n.storeID)
		break
	}

	t.Log("Multi-region key routing passed")
}

// TestMultiRegionIndependentLeaders verifies that two regions can elect
// independent leaders, potentially on different nodes.
func TestMultiRegionIndependentLeaders(t *testing.T) {
	mc := newMultiRegionCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})

	time.Sleep(500 * time.Millisecond)
	for _, r := range mc.regions {
		mc.campaignIfNeeded(r.GetId())
	}

	region1ID := mc.regions[0].GetId()
	region2ID := mc.regions[1].GetId()

	leader1Idx := mc.waitForRegionLeader(region1ID, 15*time.Second)
	leader2Idx := mc.waitForRegionLeader(region2ID, 15*time.Second)

	assert.GreaterOrEqual(t, leader1Idx, 0, "region 1 should have a leader")
	assert.GreaterOrEqual(t, leader2Idx, 0, "region 2 should have a leader")

	t.Logf("Region %d leader: node %d (store %d), Region %d leader: node %d (store %d)",
		region1ID, leader1Idx+1, mc.nodes[leader1Idx].storeID,
		region2ID, leader2Idx+1, mc.nodes[leader2Idx].storeID)

	t.Log("Multi-region independent leaders passed")
}

// TestMultiRegionRawKV verifies that RawPut/RawGet works correctly when
// keys span multiple regions.
func TestMultiRegionRawKV(t *testing.T) {
	mc := newMultiRegionCluster(t, []multiRegionDef{
		{startKey: nil, endKey: []byte("m")},
		{startKey: []byte("m"), endKey: nil},
	})

	time.Sleep(500 * time.Millisecond)
	for _, r := range mc.regions {
		mc.campaignIfNeeded(r.GetId())
	}

	region1ID := mc.regions[0].GetId()
	region2ID := mc.regions[1].GetId()
	mc.waitForRegionLeader(region1ID, 15*time.Second)
	mc.waitForRegionLeader(region2ID, 15*time.Second)

	ctx := context.Background()

	// rawPutOnLeader tries all nodes to find the region leader for the key.
	rawPutOnLeader := func(key, value []byte) {
		t.Helper()
		for _, n := range mc.nodes {
			_, client := dialTikvClient(t, n.addr)
			resp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{Key: key, Value: value})
			if err == nil && resp.GetError() == "" {
				return
			}
		}
		t.Fatalf("RawPut failed on all nodes for key %s", key)
	}

	rawPutOnLeader([]byte("alpha-key"), []byte("alpha-value"))
	rawPutOnLeader([]byte("zulu-key"), []byte("zulu-value"))

	// RawGet can use any node since all share the same engine.
	_, client := dialTikvClient(t, mc.nodes[0].addr)

	getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: []byte("alpha-key")})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound(), "alpha-key should be found")
	assert.Equal(t, []byte("alpha-value"), getResp.GetValue())

	getResp2, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: []byte("zulu-key")})
	require.NoError(t, err)
	assert.False(t, getResp2.GetNotFound(), "zulu-key should be found")
	assert.Equal(t, []byte("zulu-value"), getResp2.GetValue())

	t.Log("Multi-region RawKV passed")
}
