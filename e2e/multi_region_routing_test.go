package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"

	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/raftstore"
	raftrouter "github.com/ryogrid/gookv/internal/raftstore/router"
	"github.com/ryogrid/gookv/internal/raftstore/split"
	"github.com/ryogrid/gookv/internal/server"
	"github.com/ryogrid/gookv/internal/server/transport"
)

// --- helpers for multi-region routing tests ---

// tryTxnRPC sends a transaction RPC to each node in order, returning the first
// successful response. "not leader" / Unavailable errors are retried on the next
// node. This mirrors the rawPutOnLeader pattern used by TestMultiRegionRawKV.
func tryPrewrite(t *testing.T, mc *multiRegionCluster, req *kvrpcpb.PrewriteRequest) *kvrpcpb.PrewriteResponse {
	t.Helper()
	ctx := context.Background()
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.KvPrewrite(ctx, req)
		if err != nil {
			continue // gRPC-level error (e.g. Unavailable), try next node
		}
		// Check for retryable errors (not leader).
		hasRetryable := false
		for _, e := range resp.GetErrors() {
			if e.GetRetryable() != "" {
				hasRetryable = true
				break
			}
		}
		if hasRetryable {
			continue
		}
		return resp
	}
	t.Fatal("KvPrewrite failed on all nodes")
	return nil
}

func tryCommit(t *testing.T, mc *multiRegionCluster, req *kvrpcpb.CommitRequest) *kvrpcpb.CommitResponse {
	t.Helper()
	ctx := context.Background()
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.KvCommit(ctx, req)
		if err != nil {
			continue
		}
		if resp.GetError() != nil && resp.GetError().GetRetryable() != "" {
			continue
		}
		return resp
	}
	t.Fatal("KvCommit failed on all nodes")
	return nil
}

func tryGet(t *testing.T, mc *multiRegionCluster, key []byte, version uint64) *kvrpcpb.GetResponse {
	t.Helper()
	ctx := context.Background()
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: version})
		if err != nil {
			continue
		}
		return resp
	}
	t.Fatal("KvGet failed on all nodes")
	return nil
}

func tryScanLock(t *testing.T, client tikvpb.TikvClient, startKey, endKey []byte, maxVersion uint64) *kvrpcpb.ScanLockResponse {
	t.Helper()
	ctx := context.Background()
	resp, err := client.KvScanLock(ctx, &kvrpcpb.ScanLockRequest{
		MaxVersion: maxVersion,
		StartKey:   startKey,
		EndKey:     endKey,
		Limit:      100,
	})
	require.NoError(t, err)
	return resp
}

// setupTwoRegionCluster creates a 2-region cluster with boundary at "m" and
// waits for leaders in both regions. Returns the cluster and region IDs.
func setupTwoRegionCluster(t *testing.T) (*multiRegionCluster, uint64, uint64) {
	t.Helper()
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

	return mc, region1ID, region2ID
}

// rawPutOnLeaderMC is a cluster-level rawPutOnLeader helper.
func rawPutOnLeaderMC(t *testing.T, mc *multiRegionCluster, key, value []byte) {
	t.Helper()
	ctx := context.Background()
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{Key: key, Value: value})
		if err == nil && resp.GetError() == "" {
			return
		}
	}
	t.Fatalf("RawPut failed on all nodes for key %s", key)
}

// --- Tests ---

// TestMultiRegionTransactions verifies that KvPrewrite and KvCommit work
// correctly when mutations span two regions (["","m") and ["m","")).
func TestMultiRegionTransactions(t *testing.T) {
	mc, _, _ := setupTwoRegionCluster(t)

	startTS := uint64(100)
	commitTS := uint64(110)

	// Prewrite with primary in region1 and a secondary in region2.
	prewriteResp := tryPrewrite(t, mc, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("alpha"), Value: []byte("v1")},
			{Op: kvrpcpb.Op_Put, Key: []byte("zebra"), Value: []byte("v2")},
		},
		PrimaryLock:  []byte("alpha"),
		StartVersion: startTS,
		LockTtl:      5000,
	})
	assert.Empty(t, prewriteResp.GetErrors(), "prewrite should succeed")

	// Commit both keys.
	commitResp := tryCommit(t, mc, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("alpha"), []byte("zebra")},
		StartVersion:  startTS,
		CommitVersion: commitTS,
	})
	assert.Nil(t, commitResp.GetError(), "commit should succeed")

	// Verify both keys are readable.
	readTS := commitTS + 1
	getResp1 := tryGet(t, mc, []byte("alpha"), readTS)
	assert.False(t, getResp1.GetNotFound(), "alpha should be found")
	assert.Equal(t, []byte("v1"), getResp1.GetValue())

	getResp2 := tryGet(t, mc, []byte("zebra"), readTS)
	assert.False(t, getResp2.GetNotFound(), "zebra should be found")
	assert.Equal(t, []byte("v2"), getResp2.GetValue())

	t.Log("Multi-region transactions passed")
}

// TestMultiRegionRawKVBatchScan verifies that RawBatchScan works across two
// regions, returning correct results for each scan range.
func TestMultiRegionRawKVBatchScan(t *testing.T) {
	mc, _, _ := setupTwoRegionCluster(t)

	// Write keys in region1 (< "m").
	rawPutOnLeaderMC(t, mc, []byte("a1"), []byte("val-a1"))
	rawPutOnLeaderMC(t, mc, []byte("a2"), []byte("val-a2"))
	rawPutOnLeaderMC(t, mc, []byte("a3"), []byte("val-a3"))

	// Write keys in region2 (>= "m").
	rawPutOnLeaderMC(t, mc, []byte("m1"), []byte("val-m1"))
	rawPutOnLeaderMC(t, mc, []byte("m2"), []byte("val-m2"))

	// RawBatchScan with two ranges.
	ctx := context.Background()
	var batchScanResp *kvrpcpb.RawBatchScanResponse
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.RawBatchScan(ctx, &kvrpcpb.RawBatchScanRequest{
			Ranges: []*kvrpcpb.KeyRange{
				{StartKey: []byte("a"), EndKey: []byte("b")},
				{StartKey: []byte("m"), EndKey: []byte("n")},
			},
			EachLimit: 100,
		})
		if err == nil {
			batchScanResp = resp
			break
		}
	}
	require.NotNil(t, batchScanResp, "RawBatchScan should succeed on at least one node")

	// RawBatchScan returns all pairs from all ranges concatenated.
	// First range ["a","b") should return a1,a2,a3; second ["m","n") should return m1,m2.
	kvs := batchScanResp.GetKvs()
	assert.Equal(t, 5, len(kvs), "should return 5 total pairs (3 + 2)")

	// Verify ordering: first 3 from range1, then 2 from range2.
	if len(kvs) >= 3 {
		assert.Equal(t, []byte("a1"), kvs[0].GetKey())
		assert.Equal(t, []byte("a2"), kvs[1].GetKey())
		assert.Equal(t, []byte("a3"), kvs[2].GetKey())
	}
	if len(kvs) >= 5 {
		assert.Equal(t, []byte("m1"), kvs[3].GetKey())
		assert.Equal(t, []byte("m2"), kvs[4].GetKey())
	}

	t.Log("Multi-region RawKV batch scan passed")
}

// TestMultiRegionSplitWithLiveTraffic verifies that a manual region split
// preserves data: 20 keys written before split are all readable after split.
func TestMultiRegionSplitWithLiveTraffic(t *testing.T) {
	// Start with a single region covering all keys.
	mc := newMultiRegionCluster(t, []multiRegionDef{
		{startKey: nil, endKey: nil},
	})

	time.Sleep(500 * time.Millisecond)
	mc.campaignIfNeeded(mc.regions[0].GetId())
	regionID := mc.regions[0].GetId()
	leaderIdx := mc.waitForRegionLeader(regionID, 15*time.Second)

	// Write 20 keys via Raw KV.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		value := []byte(fmt.Sprintf("value-%02d", i))
		rawPutOnLeaderMC(t, mc, key, value)
	}

	// Manually trigger a split at "m" using the coordinator's split flow.
	leaderNode := mc.nodes[leaderIdx]
	peer := leaderNode.coord.GetPeer(regionID)
	require.NotNil(t, peer, "leader peer should exist")
	parentRegion := peer.Region()

	// Ask PD for new region/peer IDs.
	ctx := context.Background()
	splitResp, err := mc.pdClient.AskBatchSplit(ctx, parentRegion, 1)
	require.NoError(t, err)
	require.Len(t, splitResp.GetIds(), 1)

	splitID := splitResp.GetIds()[0]
	newRegionID := splitID.GetNewRegionId()
	newPeerIDs := splitID.GetNewPeerIds()

	// Execute the split.
	splitResult, err := split.ExecBatchSplit(
		parentRegion,
		[][]byte{[]byte("m")},
		[]uint64{newRegionID},
		[][]uint64{newPeerIDs},
	)
	require.NoError(t, err)

	// Update parent region metadata.
	peer.UpdateRegion(splitResult.Derived)

	// Bootstrap the child region on all nodes.
	for _, n := range mc.nodes {
		childRegion := splitResult.Regions[0]
		raftPeers := make([]raft.Peer, len(childRegion.GetPeers()))
		for j, p := range childRegion.GetPeers() {
			raftPeers[j] = raft.Peer{ID: p.GetId()}
		}
		// BootstrapRegion may fail on some nodes if the region already exists;
		// that's okay since the leader already has it.
		_ = n.coord.BootstrapRegion(childRegion, raftPeers)
	}

	// Report split to PD.
	allRegions := append([]*metapb.Region{splitResult.Derived}, splitResult.Regions...)
	err = mc.pdClient.ReportBatchSplit(ctx, allRegions)
	require.NoError(t, err)

	// Wait for the child region's leader.
	time.Sleep(500 * time.Millisecond)
	mc.campaignIfNeeded(newRegionID)
	mc.waitForRegionLeader(newRegionID, 15*time.Second)

	// Verify all 20 keys are still readable.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		found := false
		for _, n := range mc.nodes {
			_, client := dialTikvClient(t, n.addr)
			resp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: key})
			if err == nil && !resp.GetNotFound() {
				assert.Equal(t, []byte(fmt.Sprintf("value-%02d", i)), resp.GetValue())
				found = true
				break
			}
		}
		assert.True(t, found, "key %s should still be readable after split", key)
	}

	// Verify coordinator now has 2 regions on the leader node.
	assert.Equal(t, 2, leaderNode.coord.RegionCount(),
		"leader node should have 2 regions after split")

	t.Log("Multi-region split with live traffic passed")
}

// TestMultiRegionPDCoordinatedSplit tests automatic PD-coordinated split.
// It creates a cluster with a very small SplitSize, writes enough data to
// exceed it, and waits for the auto-split to happen.
func TestMultiRegionPDCoordinatedSplit(t *testing.T) {
	// This test uses a custom cluster setup with PD integration and small split size.
	_, pdAddr := startPDServer(t)
	pdClient := newPDClient(t, pdAddr)
	ctx := context.Background()

	// Use single-node to avoid the issue where split children need Raft-based
	// replication of the split command to create peers on other nodes.
	// With a single node, the split check worker creates the child region locally
	// and the single peer automatically becomes leader.
	const nodeCount = 1

	// Pre-allocate resolver.
	addrMap := make(map[uint64]string)
	for i := 0; i < nodeCount; i++ {
		addrMap[uint64(i+1)] = ""
	}
	resolver := server.NewStaticStoreResolver(addrMap)

	// Allocate first region IDs.
	firstRegionID, err := pdClient.AllocID(ctx)
	require.NoError(t, err)

	firstMetaPeers := make([]*metapb.Peer, nodeCount)
	firstRaftPeers := make([]raft.Peer, nodeCount)
	for i := 0; i < nodeCount; i++ {
		peerID, err := pdClient.AllocID(ctx)
		require.NoError(t, err)
		storeID := uint64(i + 1)
		firstMetaPeers[i] = &metapb.Peer{Id: peerID, StoreId: storeID}
		firstRaftPeers[i] = raft.Peer{ID: peerID}
	}

	firstRegion := &metapb.Region{
		Id:    firstRegionID,
		Peers: firstMetaPeers,
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}

	// Bootstrap cluster in PD.
	store1 := &metapb.Store{Id: 1, Address: "placeholder"}
	_, err = pdClient.Bootstrap(ctx, store1, firstRegion)
	require.NoError(t, err)

	// Create nodes with small split size and fast split check interval.
	type pdCoordNode struct {
		storeID    uint64
		srv        *server.Server
		coord      *server.StoreCoordinator
		pdWorker   *server.PDWorker
		raftClient *transport.RaftClient
		addr       string
	}
	nodes := make([]*pdCoordNode, nodeCount)

	for i := 0; i < nodeCount; i++ {
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
		peerCfg.SplitCheckTickInterval = 100 * time.Millisecond // Fast split check

		nodePDClient := newPDClient(t, pdAddr)
		pdWorker := server.NewPDWorker(server.PDWorkerConfig{
			StoreID:  storeID,
			PDClient: nodePDClient,
		})

		// Use a very small split size (1 KB) to trigger split quickly.
		splitCfg := split.SplitCheckWorkerConfig{
			SplitSize: 1024,      // 1 KB
			MaxSize:   2048,      // 2 KB
			SplitKeys: 10,
			MaxKeys:   20,
		}

		coord := server.NewStoreCoordinator(server.StoreCoordinatorConfig{
			StoreID:       storeID,
			Engine:        engine,
			Storage:       storage,
			Router:        rtr,
			Client:        raftClient,
			PeerCfg:       peerCfg,
			PDTaskCh:      pdWorker.PeerTaskCh(),
			PDClient:      nodePDClient,
			SplitCheckCfg: splitCfg,
		})
		srv.SetCoordinator(coord)
		pdWorker.SetCoordinator(coord)
		pdWorker.Run()

		// Start the split result handler goroutine.
		splitCtx, splitCancel := context.WithCancel(context.Background())
		go coord.RunSplitResultHandler(splitCtx)

		require.NoError(t, coord.BootstrapRegion(firstRegion, firstRaftPeers))
		require.NoError(t, srv.Start())
		addr := srv.Addr()
		resolver.UpdateAddr(storeID, addr)

		require.NoError(t, pdClient.PutStore(ctx, &metapb.Store{Id: storeID, Address: addr}))

		nodes[i] = &pdCoordNode{
			storeID:    storeID,
			srv:        srv,
			coord:      coord,
			pdWorker:   pdWorker,
			raftClient: raftClient,
			addr:       addr,
		}

		t.Cleanup(func() {
			splitCancel()
			pdWorker.Stop()
			coord.Stop()
			srv.Stop()
			raftClient.Close()
		})
	}

	// Wait for leader election.
	time.Sleep(500 * time.Millisecond)
	// Campaign on first node for the initial region.
	if peer := nodes[0].coord.GetPeer(firstRegionID); peer != nil {
		_ = peer.Campaign()
	}

	// Wait for leader.
	deadline := time.Now().Add(15 * time.Second)
	leaderFound := false
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			peer := n.coord.GetPeer(firstRegionID)
			if peer != nil && peer.IsLeader() {
				leaderFound = true
				break
			}
		}
		if leaderFound {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, leaderFound, "leader should be elected for initial region")

	// Write enough data to exceed split size (50 keys x ~100 bytes = ~5KB > 1KB).
	// After a split occurs mid-write, the new child region may not yet have a leader,
	// so we retry with backoff.
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("split-key-%03d", i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte('A' + (i % 26))
		}
		putOk := false
		for attempt := 0; attempt < 10; attempt++ {
			for _, n := range nodes {
				_, client := dialTikvClient(t, n.addr)
				resp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{Key: key, Value: value})
				if err == nil && resp.GetError() == "" {
					putOk = true
					break
				}
			}
			if putOk {
				break
			}
			time.Sleep(200 * time.Millisecond) // wait for new region leader election
		}
		require.True(t, putOk, "RawPut should succeed for key %s", key)
	}

	// Wait for auto-split to happen (poll region count).
	splitDeadline := time.Now().Add(30 * time.Second)
	splitOccurred := false
	for time.Now().Before(splitDeadline) {
		for _, n := range nodes {
			if n.coord.RegionCount() > 1 {
				splitOccurred = true
				break
			}
		}
		if splitOccurred {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Verify that at least one node has more than 1 region.
	maxRegions := 0
	for _, n := range nodes {
		rc := n.coord.RegionCount()
		if rc > maxRegions {
			maxRegions = rc
		}
	}
	assert.Greater(t, maxRegions, 1,
		"at least one node should have > 1 region after auto-split (got %d)", maxRegions)

	t.Log("Multi-region PD-coordinated split passed")
}

// TestMultiRegionAsyncCommit verifies async commit prewrite and
// CheckSecondaryLocks across two regions.
func TestMultiRegionAsyncCommit(t *testing.T) {
	mc, _, _ := setupTwoRegionCluster(t)

	ctx := context.Background()
	startTS := uint64(500)

	// Async commit prewrite: primary in region1, secondary in region2.
	primaryKey := []byte("ac-primary")   // region1 (< "m")
	secondaryKey := []byte("zz-secondary") // region2 (>= "m")

	prewriteResp := tryPrewrite(t, mc, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: primaryKey, Value: []byte("primary-val")},
			{Op: kvrpcpb.Op_Put, Key: secondaryKey, Value: []byte("secondary-val")},
		},
		PrimaryLock:    primaryKey,
		StartVersion:   startTS,
		LockTtl:        5000,
		UseAsyncCommit: true,
		Secondaries:    [][]byte{secondaryKey},
	})
	assert.Empty(t, prewriteResp.GetErrors(), "async commit prewrite should succeed")
	assert.Greater(t, prewriteResp.GetMinCommitTs(), uint64(0),
		"MinCommitTs should be > 0 for async commit prewrite")

	// CheckSecondaryLocks on the secondary key.
	var checkResp *kvrpcpb.CheckSecondaryLocksResponse
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp, err := client.KvCheckSecondaryLocks(ctx, &kvrpcpb.CheckSecondaryLocksRequest{
			Keys:         [][]byte{secondaryKey},
			StartVersion: startTS,
		})
		if err == nil {
			checkResp = resp
			break
		}
	}
	require.NotNil(t, checkResp, "CheckSecondaryLocks should succeed")
	assert.Nil(t, checkResp.GetError(), "should not return an error")
	assert.NotEmpty(t, checkResp.GetLocks(), "should find the lock on the secondary key")
	assert.Equal(t, uint64(0), checkResp.GetCommitTs(), "commitTs should be 0 while lock is held")

	// Commit the transaction.
	commitTS := startTS + 10
	commitResp := tryCommit(t, mc, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{primaryKey, secondaryKey},
		StartVersion:  startTS,
		CommitVersion: commitTS,
	})
	assert.Nil(t, commitResp.GetError(), "commit should succeed")

	// Verify both keys readable.
	readTS := commitTS + 1
	getResp1 := tryGet(t, mc, primaryKey, readTS)
	assert.False(t, getResp1.GetNotFound(), "primary key should be readable")
	assert.Equal(t, []byte("primary-val"), getResp1.GetValue())

	getResp2 := tryGet(t, mc, secondaryKey, readTS)
	assert.False(t, getResp2.GetNotFound(), "secondary key should be readable")
	assert.Equal(t, []byte("secondary-val"), getResp2.GetValue())

	t.Log("Multi-region async commit passed")
}

// TestMultiRegionScanLock verifies that ScanLock returns only locks within
// the requested key range, respecting region boundaries.
func TestMultiRegionScanLock(t *testing.T) {
	mc, _, _ := setupTwoRegionCluster(t)

	ctx := context.Background()

	// Create a lock in region1 via prewrite (key "alpha" < "m").
	startTS1 := uint64(700)
	prewriteResp1 := tryPrewrite(t, mc, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("alpha"), Value: []byte("val-alpha")},
		},
		PrimaryLock:  []byte("alpha"),
		StartVersion: startTS1,
		LockTtl:      10000,
	})
	assert.Empty(t, prewriteResp1.GetErrors(), "prewrite alpha should succeed")

	// Create a lock in region2 via prewrite (key "zeta" >= "m").
	startTS2 := uint64(800)
	prewriteResp2 := tryPrewrite(t, mc, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("zeta"), Value: []byte("val-zeta")},
		},
		PrimaryLock:  []byte("zeta"),
		StartVersion: startTS2,
		LockTtl:      10000,
	})
	assert.Empty(t, prewriteResp2.GetErrors(), "prewrite zeta should succeed")

	// ScanLock for region1 range: startKey="" endKey="m" should find only "alpha".
	var scanResp1 *kvrpcpb.ScanLockResponse
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp := tryScanLock(t, client, []byte(""), []byte("m"), 900)
		if resp.GetError() == nil {
			scanResp1 = resp
			break
		}
	}
	require.NotNil(t, scanResp1, "ScanLock for region1 should succeed")
	found1 := false
	for _, lock := range scanResp1.GetLocks() {
		if string(lock.GetKey()) == "alpha" {
			found1 = true
		}
		// Verify no lock from region2 leaks into region1 scan.
		assert.NotEqual(t, "zeta", string(lock.GetKey()),
			"zeta should not appear in region1 scan range")
	}
	assert.True(t, found1, "alpha lock should be found in region1 scan")

	// ScanLock for region2 range: startKey="m" should find only "zeta".
	var scanResp2 *kvrpcpb.ScanLockResponse
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		resp := tryScanLock(t, client, []byte("m"), nil, 900)
		if resp.GetError() == nil {
			scanResp2 = resp
			break
		}
	}
	require.NotNil(t, scanResp2, "ScanLock for region2 should succeed")
	found2 := false
	for _, lock := range scanResp2.GetLocks() {
		if string(lock.GetKey()) == "zeta" {
			found2 = true
		}
		// Verify no lock from region1 leaks into region2 scan.
		assert.NotEqual(t, "alpha", string(lock.GetKey()),
			"alpha should not appear in region2 scan range")
	}
	assert.True(t, found2, "zeta lock should be found in region2 scan")

	// Clean up: rollback both locks.
	for _, n := range mc.nodes {
		_, client := dialTikvClient(t, n.addr)
		_, _ = client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Keys:         [][]byte{[]byte("alpha")},
			StartVersion: startTS1,
		})
		_, _ = client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Keys:         [][]byte{[]byte("zeta")},
			StartVersion: startTS2,
		})
	}

	t.Log("Multi-region scan lock passed")
}
