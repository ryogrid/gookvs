# Multi-Region E2E Test Infrastructure

## 1. Overview

All existing e2e tests in the gookv project operate on a **single Raft region** (region ID 1) that spans the entire keyspace. The two cluster test helpers demonstrate this:

- **`testServerCluster`** (`e2e/cluster_server_test.go`): A 5-node cluster that bootstraps one `metapb.Region{Id: 1}` with no start/end key bounds across all nodes. Tests cover leader election, KV operations, cross-node replication, node failure, and leader failover -- all within the single region.
- **`newPDIntegratedCluster`** (`e2e/pd_leader_discovery_test.go`): A 3-node cluster with PD integration that again bootstraps a single `metapb.Region{Id: 1}`. Tests cover store registration, leader tracking, and PD-coordinated failover.

This single-region setup is insufficient for validating multi-region behaviors that are central to gookv's design:

| Behavior | Why single-region testing misses it |
|----------|-------------------------------------|
| Key routing | All keys land on the same region; no routing decisions are made |
| Cross-region 2PC | Prewrite/Commit always target one region; coordinator hardcodes `regionID=1` |
| Scan across region boundaries | Scanner never encounters a region boundary |
| Split under live traffic | Split creates new regions, but existing tests never exercise reads/writes against them |
| PD-coordinated auto-split | PD heartbeat checking thresholds never triggers because all data is in one unbounded region |
| Async commit across regions | CheckSecondaryLocks needs to resolve locks on different regions |
| Lock scanning boundaries | ScanLock operates within a single unbounded range |

This document specifies the test infrastructure and test scenarios needed to cover multi-region e2e behavior.

## 2. Test Infrastructure Design

### 2.1 Core Struct: `multiRegionCluster`

```go
// multiRegionCluster manages a multi-node cluster with multiple Raft regions.
// Each region has its own Raft group spanning all nodes.
type multiRegionCluster struct {
    t        *testing.T
    nodes    []*multiRegionNode
    resolver *server.StaticStoreResolver
    pdClient pdclient.Client       // nil when PD is not used
    pdAddr   string                 // empty when PD is not used
    regions  []*metapb.Region       // region metadata for all bootstrapped regions
}

type multiRegionNode struct {
    storeID    uint64
    srv        *server.Server
    coord      *server.StoreCoordinator
    raftClient *transport.RaftClient
    pdWorker   *server.PDWorker       // nil when PD is not used
    addr       string
}
```

This extends the existing `testServerCluster` pattern with:
- Multiple `metapb.Region` entries, each with non-overlapping key ranges.
- Each region bootstrapped as a separate Raft group on every node via `StoreCoordinator.BootstrapRegion()`.
- The `Router` (`internal/raftstore/router/router.go`) naturally supports multiple regions -- it maps region IDs to peer mailbox channels via `sync.Map`, so registering additional regions requires no router changes.

### 2.2 Constructor: `newMultiRegionCluster`

```go
func newMultiRegionCluster(t *testing.T, nodeCount int, regionDefs []regionDef) *multiRegionCluster

type regionDef struct {
    RegionID uint64
    StartKey []byte // nil = beginning of keyspace
    EndKey   []byte // nil = end of keyspace
}
```

Construction steps mirror `newTestServerCluster` but loop over multiple regions:

1. Pre-allocate `StaticStoreResolver` with placeholder addresses for `nodeCount` stores.
2. For each region definition, build `metapb.Region` with peers spanning all stores. Peer IDs are allocated deterministically: `peerID = regionID*1000 + storeID` to avoid collisions across regions.
3. For each node (1..nodeCount):
   a. Create a temp directory, open `rocks.Engine`.
   b. Create `server.Storage`, `server.Server`, `transport.RaftClient`, `router.Router`.
   c. Configure `raftstore.PeerConfig` with fast election timers (20ms base tick, 10 election ticks, 2 heartbeat ticks).
   d. Create `StoreCoordinator`.
   e. **For each region**: call `coord.BootstrapRegion(region, raftPeers)`. This registers the region in the router and starts a peer goroutine.
   f. Start the gRPC server, update the resolver with the actual address.
4. Register `t.Cleanup` to stop all nodes.

### 2.3 Constructor (PD variant): `newMultiRegionClusterWithPD`

```go
func newMultiRegionClusterWithPD(t *testing.T, nodeCount int, regionDefs []regionDef) *multiRegionCluster
```

Same as above but additionally:
- Starts a PD server via `startPDServer(t)`.
- Creates `pdclient.Client` for each node.
- Creates `PDWorker` per node, wires `PDTaskCh` into the coordinator.
- Calls `pdClient.Bootstrap()` with the first region, then `pdClient.PutStore()` for each store.
- Bootstraps additional regions into PD via `pdClient.PutRegion()` or equivalent.

### 2.4 Helper: `bootstrapRegions`

```go
func (mrc *multiRegionCluster) bootstrapRegions(regionDefs []regionDef) []*metapb.Region
```

Converts `regionDef` entries into `metapb.Region` objects:

```go
func (mrc *multiRegionCluster) bootstrapRegions(regionDefs []regionDef) []*metapb.Region {
    nodeCount := len(mrc.nodes)
    regions := make([]*metapb.Region, len(regionDefs))
    for i, def := range regionDefs {
        peers := make([]*metapb.Peer, nodeCount)
        for j := 0; j < nodeCount; j++ {
            storeID := uint64(j + 1)
            peerID := def.RegionID*1000 + storeID
            peers[j] = &metapb.Peer{Id: peerID, StoreId: storeID}
        }
        regions[i] = &metapb.Region{
            Id:       def.RegionID,
            StartKey: def.StartKey,
            EndKey:   def.EndKey,
            Peers:    peers,
            RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
        }
    }
    return regions
}
```

Key ranges must be non-overlapping and collectively cover the desired keyspace. Example for 3 regions:

| Region ID | StartKey | EndKey |
|-----------|----------|--------|
| 1 | nil | `[]byte("m")` |
| 2 | `[]byte("m")` | `[]byte("t")` |
| 3 | `[]byte("t")` | nil |

### 2.5 Helper: `waitForLeader(regionID)`

The existing `testServerCluster.waitForLeader()` always queries region 1. The multi-region version takes a `regionID` parameter:

```go
func (mrc *multiRegionCluster) waitForLeader(regionID uint64, timeout time.Duration) int {
    mrc.t.Helper()
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        for i, node := range mrc.nodes {
            if node == nil {
                continue
            }
            peer := node.coord.GetPeer(regionID)
            if peer != nil && peer.IsLeader() {
                return i
            }
        }
        time.Sleep(20 * time.Millisecond)
    }
    mrc.t.Fatalf("no leader elected for region %d within timeout", regionID)
    return -1
}
```

### 2.6 Helper: `waitForAllLeaders`

Convenience method that waits for leaders on all bootstrapped regions:

```go
func (mrc *multiRegionCluster) waitForAllLeaders(timeout time.Duration) map[uint64]int {
    mrc.t.Helper()
    leaders := make(map[uint64]int)
    for _, region := range mrc.regions {
        leaders[region.Id] = mrc.waitForLeader(region.Id, timeout)
    }
    return leaders
}
```

### 2.7 Helper: `findRegionForKey`

Resolves a user key to the correct region by checking start/end key bounds:

```go
func (mrc *multiRegionCluster) findRegionForKey(key []byte) *metapb.Region {
    for _, region := range mrc.regions {
        startKey := region.GetStartKey()
        endKey := region.GetEndKey()
        if (len(startKey) == 0 || bytes.Compare(key, startKey) >= 0) &&
           (len(endKey) == 0 || bytes.Compare(key, endKey) < 0) {
            return region
        }
    }
    return nil
}
```

### 2.8 Helper: `dialLeader`

Connects to the leader node for a specific region:

```go
func (mrc *multiRegionCluster) dialLeader(regionID uint64) (*grpc.ClientConn, tikvpb.TikvClient) {
    leaderIdx := mrc.waitForLeader(regionID, 10*time.Second)
    return mrc.dialNode(leaderIdx)
}
```

### 2.9 Helper: `campaignAll`

Triggers a Raft campaign on all regions to accelerate leader election:

```go
func (mrc *multiRegionCluster) campaignAll() {
    for _, region := range mrc.regions {
        peer := mrc.nodes[0].coord.GetPeer(region.Id)
        if peer != nil {
            _ = peer.Campaign()
        }
    }
}
```

## 3. Test Scenarios

### 3.1 TestMultiRegionKeyRouting

**Purpose**: Verify that keys written to different regions are stored and retrievable on the correct region's leader.

**Setup**: 3 regions on a 3-node cluster.

| Region | StartKey | EndKey |
|--------|----------|--------|
| 1 | nil | `"m"` |
| 2 | `"m"` | `"t"` |
| 3 | `"t"` | nil |

**Steps**:
1. Create `multiRegionCluster` with 3 nodes, 3 regions.
2. Wait for leaders on all regions.
3. For each region, write a key that falls within its range:
   - Region 1: key `"alpha"` (< `"m"`)
   - Region 2: key `"noon"` (>= `"m"`, < `"t"`)
   - Region 3: key `"zebra"` (>= `"t"`)
4. For each key:
   a. Find the region via `findRegionForKey`.
   b. Dial the region's leader.
   c. KvPrewrite + KvCommit (using the correct `regionID` in proposals).
   d. KvGet from the same leader, assert value matches.
5. Cross-verify: reading `"alpha"` from region 2's leader should return not-found or error (wrong region).

**Assertions**:
- Each key is readable on its region's leader.
- `findRegionForKey` returns the expected region for each key.

### 3.2 TestMultiRegionTransactions

**Purpose**: Verify that a 2PC transaction spanning two regions commits atomically.

**Setup**: 2 regions on a 3-node cluster.

| Region | StartKey | EndKey |
|--------|----------|--------|
| 1 | nil | `"m"` |
| 2 | `"m"` | nil |

**Steps**:
1. Create cluster, wait for all leaders.
2. Start a transaction at `startTS=100`:
   a. KvPrewrite key `"abc"` (region 1) and key `"xyz"` (region 2) with primary lock `"abc"`.
   b. Send prewrite to each region's leader respectively.
3. KvCommit both keys at `commitTS=200`:
   a. Send commit for `"abc"` to region 1's leader.
   b. Send commit for `"xyz"` to region 2's leader.
4. KvGet both keys at `version=300`:
   a. Read `"abc"` from region 1's leader -- assert found.
   b. Read `"xyz"` from region 2's leader -- assert found.

**Assertions**:
- Both prewrite responses have no errors.
- Both commit responses have no errors.
- Both reads return the expected values.

### 3.3 TestMultiRegionRawKVBatchScan

**Purpose**: Verify that a raw KV scan across region boundaries returns results from multiple regions.

**Setup**: 3 regions on a 3-node cluster with key boundaries at `"d"` and `"h"`.

**Steps**:
1. Write raw KV pairs: `"a"`, `"b"`, `"c"` (region 1), `"d"`, `"e"`, `"f"` (region 2), `"h"`, `"i"`, `"j"` (region 3) using `RawPut` to each region's leader.
2. Issue `RawScan(startKey="b", endKey="i")` to a single node.
3. Depending on server-side scan implementation, the scan may only return keys from the local region or require client-side multi-region fan-out.

**Assertions**:
- At minimum, keys within the scanned region are returned.
- Document whether cross-region scan requires client coordination (expected: yes, similar to TiKV client behavior).

### 3.4 TestMultiRegionSplitWithLiveTraffic

**Purpose**: Verify that a region split completes correctly while concurrent writes are in progress.

**Setup**: 1 initial region (unbounded) on a 3-node cluster.

**Steps**:
1. Create cluster with 1 region.
2. Start a background goroutine writing keys `"key-0000"` through `"key-9999"` via KvPrewrite + KvCommit in a loop.
3. Trigger a region split at midpoint key `"key-5000"`:
   a. Send a split request via the store coordinator or PD.
   b. Wait for the new region (ID 2) to appear in `coord.GetPeer(2)`.
4. Wait for leaders on both the original and new regions.
5. Stop the background writer.
6. Read a sample of keys from each region:
   - Keys < `"key-5000"` from region 1.
   - Keys >= `"key-5000"` from region 2.

**Assertions**:
- Split completes without error.
- Both regions elect leaders.
- Keys written before and during the split are readable from the correct region.
- No data loss or duplication.

### 3.5 TestMultiRegionPDCoordinatedSplit

**Purpose**: Verify that PD triggers an automatic split when a region exceeds the size threshold.

**Setup**: 1 initial region on a 3-node PD-integrated cluster with a low split threshold.

**Steps**:
1. Create `multiRegionClusterWithPD` with 3 nodes, 1 region.
2. Configure the split checker with a low threshold (e.g., 1 KB or 100 keys).
3. Write enough data to exceed the threshold.
4. Wait for PD to issue a split command (poll `coord.RegionCount()` on each node until it exceeds 1).
5. Verify both regions are functional:
   a. Wait for leaders.
   b. Write and read a key in each region.

**Assertions**:
- `RegionCount()` increases from 1 to 2 on all nodes.
- PD reports two regions via `pdClient.GetRegion()`.
- Both regions accept reads and writes.

### 3.6 TestMultiRegionAsyncCommit

**Purpose**: Verify that async commit with `CheckSecondaryLocks` works across region boundaries.

**Setup**: 2 regions on a 3-node cluster.

**Steps**:
1. Create cluster with 2 regions (boundary at `"m"`).
2. Prewrite with async commit enabled:
   - Primary key `"abc"` on region 1.
   - Secondary key `"xyz"` on region 2.
   - Set `UseAsyncCommit = true` and populate `Secondaries` list.
3. Commit the primary key `"abc"` on region 1.
4. Before committing the secondary, issue `CheckSecondaryLocks` for key `"xyz"` on region 2.
5. The check should resolve the lock based on the primary's committed state.
6. Read `"xyz"` at a version after `commitTS` -- should return the value.

**Assertions**:
- Async commit prewrite succeeds with min_commit_ts returned.
- Primary commit succeeds.
- `CheckSecondaryLocks` resolves the secondary lock.
- Both keys are readable after resolution.

### 3.7 TestMultiRegionScanLock

**Purpose**: Verify that `KvScanLock` respects region key boundaries and only returns locks within the scanned range.

**Setup**: 2 regions on a 3-node cluster (boundary at `"m"`).

**Steps**:
1. Create locks on both regions:
   - Prewrite (but do not commit) key `"aaa"` (region 1) at `startTS=100`.
   - Prewrite (but do not commit) key `"zzz"` (region 2) at `startTS=200`.
2. Issue `KvScanLock` with `maxVersion=300` on region 1's leader.
3. The response should contain the lock on `"aaa"` but not `"zzz"`.
4. Issue `KvScanLock` with `maxVersion=300` on region 2's leader.
5. The response should contain the lock on `"zzz"` but not `"aaa"`.

**Assertions**:
- Each region's scan lock returns only locks within that region's key range.
- Lock metadata (startTS, primary, TTL) is correct.

## 4. Helper Function Signatures Summary

```go
// --- Cluster construction ---

type regionDef struct {
    RegionID uint64
    StartKey []byte
    EndKey   []byte
}

func newMultiRegionCluster(t *testing.T, nodeCount int, regionDefs []regionDef) *multiRegionCluster
func newMultiRegionClusterWithPD(t *testing.T, nodeCount int, regionDefs []regionDef) *multiRegionCluster

// --- Cluster operations ---

func (mrc *multiRegionCluster) bootstrapRegions(regionDefs []regionDef) []*metapb.Region
func (mrc *multiRegionCluster) waitForLeader(regionID uint64, timeout time.Duration) int
func (mrc *multiRegionCluster) waitForAllLeaders(timeout time.Duration) map[uint64]int
func (mrc *multiRegionCluster) findRegionForKey(key []byte) *metapb.Region
func (mrc *multiRegionCluster) dialNode(idx int) (*grpc.ClientConn, tikvpb.TikvClient)
func (mrc *multiRegionCluster) dialLeader(regionID uint64) (*grpc.ClientConn, tikvpb.TikvClient)
func (mrc *multiRegionCluster) campaignAll()
func (mrc *multiRegionCluster) stopAll()
func (mrc *multiRegionCluster) stopNode(idx int)
```

### Common Test Patterns

**Pattern: Write-then-read per region**

```go
func writeAndVerify(t *testing.T, client tikvpb.TikvClient, key, value []byte, startTS, commitTS uint64) {
    ctx := context.Background()
    prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
        Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
        PrimaryLock:  key,
        StartVersion: startTS,
        LockTtl:      5000,
    })
    require.NoError(t, err)
    assert.Empty(t, prewriteResp.GetErrors())

    commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
        Keys:          [][]byte{key},
        StartVersion:  startTS,
        CommitVersion: commitTS,
    })
    require.NoError(t, err)
    assert.Nil(t, commitResp.GetError())

    getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
        Key:     key,
        Version: commitTS + 1,
    })
    require.NoError(t, err)
    assert.False(t, getResp.GetNotFound())
    assert.Equal(t, value, getResp.GetValue())
}
```

**Pattern: Multi-region leader election trigger**

```go
func (mrc *multiRegionCluster) ensureAllLeaders() map[uint64]int {
    time.Sleep(500 * time.Millisecond) // Allow natural election.
    mrc.campaignAll()                  // Trigger campaigns if needed.
    return mrc.waitForAllLeaders(10 * time.Second)
}
```

## 5. Required Server-Side Changes

The current `tikvService` methods (e.g., `KvPrewrite`, `KvCommit`) in `internal/server/server.go` hardcode `regionID=1` when calling `coord.ProposeModifies(1, ...)`. For multi-region support, each RPC must resolve the correct `regionID` from the request's key:

```go
// In KvPrewrite:
regionID := svc.server.resolveRegion(req.GetMutations()[0].GetKey())
if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
    // ...
}
```

The `resolveRegion` method can either:
- Query the `StoreCoordinator`'s region map.
- Use the `kvrpcpb.Context.RegionId` field from the request (TiKV's approach -- the client supplies the region ID).

This server-side change is a **prerequisite** for the multi-region tests and should be tracked as a separate implementation task.

## 6. Files to Create/Modify

| File | Change |
|------|--------|
| `e2e/multi_region_test.go` | New file: `multiRegionCluster` struct, constructors, all 7 test functions |
| `e2e/helpers_test.go` | Add `writeAndVerify` and other shared helpers |
| `internal/server/server.go` | Change hardcoded `regionID=1` to resolve from request context or key |
| `internal/server/coordinator.go` | Add `FindRegionForKey(key) uint64` method |

## 7. Test Execution Considerations

| Concern | Approach |
|---------|----------|
| Election timing | Use fast Raft timers (20ms tick) + explicit `Campaign()` + 10s timeout for leader wait |
| Resource cleanup | All engines, servers, and clients registered via `t.Cleanup` |
| Test isolation | Each test creates its own cluster in `t.TempDir()` |
| Parallelism | Tests can run in parallel (`t.Parallel()`) since each uses independent temp directories and ports |
| Flakiness mitigation | Use `require.Eventually` with generous timeouts for async operations (leader election, replication) |
| Test duration | Expect 5-15 seconds per test due to Raft election and replication delays |
