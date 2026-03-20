# Client Library — E2E Test Design

## 1. Test Infrastructure

### 1.1 Test Cluster Setup

Tests use the existing PD-integrated cluster infrastructure (`startPDServer`, `newPDClient`) plus the new client library. Each test:

1. Starts an embedded PD server.
2. Creates N gookv-server nodes, each bootstrapping the same set of regions.
3. Creates a `client.RawKVClient` connected to PD (not to individual nodes).
4. The client discovers stores and regions via PD — no hardcoded addresses.

### 1.2 Helper: `newClientTestCluster`

```go
// clientTestCluster wraps a multi-node cluster with a client library instance.
type clientTestCluster struct {
    t        *testing.T
    pdAddr   string
    pdClient pdclient.Client
    nodes    []*serverNode       // gookv-server nodes
    client   *client.RawKVClient // the client under test
}

func newClientTestCluster(t *testing.T, nodeCount int, regionDefs []regionDef) *clientTestCluster
```

This helper:
- Starts PD, creates nodes, bootstraps regions (reusing existing patterns).
- Creates a `client.RawKVClient` via `client.NewClient(ctx, Config{PDAddrs: []string{pdAddr}})`.
- Registers cleanup.

### 1.3 Multi-Region Pre-Split Helper

For tests that need multiple regions from the start, the helper pre-splits the initial region by calling `AskBatchSplit` + `ExecBatchSplit` programmatically before the client is created.

---

## 2. Test Cases

### 2.1 Region Cache Tests

#### `TestClientRegionCacheMiss`
**Goal:** Verify the client queries PD on first access (cold cache).

**Steps:**
1. Create a single-region cluster (1 node).
2. Create `RawKVClient` (cache is empty).
3. Call `client.Put(ctx, "key1", "val1")`.
4. Assert success — the client internally called `PD.GetRegion("key1")` to discover the region and store.
5. Call `client.Get(ctx, "key1")` — should return "val1".

#### `TestClientRegionCacheHit`
**Goal:** Verify subsequent requests use the cache (no PD round-trip).

**Steps:**
1. Create a single-region cluster.
2. `client.Put(ctx, "a", "1")` — populates cache.
3. `client.Put(ctx, "b", "2")` — should use cached region info (same region).
4. Both puts succeed.

#### `TestClientRegionCacheInvalidationOnSplit`
**Goal:** Verify the client handles region splits gracefully.

**Steps:**
1. Create a single-region cluster.
2. `client.Put(ctx, "alpha", "v1")` — cache populated for the full-range region.
3. Trigger a manual split at key "m" (via coordinator + PD, not client-side).
4. `client.Put(ctx, "zebra", "v2")` — first attempt may hit stale cache (old region's endKey was ""), get `EpochNotMatch` or `KeyNotInRegion`.
5. Assert the client automatically retries after cache invalidation and the put succeeds.
6. `client.Get(ctx, "zebra")` returns "v2".

### 2.2 Store Resolution Tests

#### `TestClientStoreResolution`
**Goal:** Verify the client resolves store addresses from PD.

**Steps:**
1. Create a 3-node cluster with 1 region.
2. Create `RawKVClient` (no hardcoded store addresses).
3. `client.Put(ctx, "key", "val")` — client discovers region leader's store ID from PD, resolves address via `PD.GetStore()`, connects, and writes.
4. Assert success.

#### `TestClientStoreFailover`
**Goal:** Verify the client handles store unavailability.

**Steps:**
1. Create a 3-node cluster.
2. `client.Put(ctx, "key", "val")` — succeeds.
3. Stop the leader node.
4. Wait for leader re-election (different node becomes leader).
5. `client.Get(ctx, "key")` — first attempt may fail (old leader), client gets `NotLeader` error, updates leader in cache, retries on new leader.
6. Assert success.

### 2.3 Batch Operation Tests

#### `TestClientBatchGetAcrossRegions`
**Goal:** Verify `BatchGet` splits keys by region and merges results.

**Steps:**
1. Create a 2-region cluster: ["", "m") and ["m", "").
2. `client.Put(ctx, "apple", "1")` and `client.Put(ctx, "mango", "2")`.
3. `pairs := client.BatchGet(ctx, [][]byte{"apple", "mango"})`.
4. Assert `len(pairs) == 2` with correct values.

#### `TestClientBatchPutAcrossRegions`
**Goal:** Verify `BatchPut` routes each key to its region.

**Steps:**
1. Create a 2-region cluster.
2. `client.BatchPut(ctx, []KvPair{{"a","1"}, {"z","2"}})`.
3. `client.Get(ctx, "a")` returns "1".
4. `client.Get(ctx, "z")` returns "2".

### 2.4 Scan Across Regions

#### `TestClientScanAcrossRegions`
**Goal:** Verify `Scan` transparently crosses region boundaries.

**Steps:**
1. Create a 2-region cluster: ["", "m") and ["m", "").
2. Put 3 keys in region 1: "a", "b", "c". Put 2 keys in region 2: "m", "n".
3. `pairs := client.Scan(ctx, nil, nil, 100)` — scan all.
4. Assert `len(pairs) == 5`, keys in order: "a", "b", "c", "m", "n".

#### `TestClientScanWithLimit`
**Goal:** Verify scan respects the limit parameter even across regions.

**Steps:**
1. Same setup as above (5 keys across 2 regions).
2. `pairs := client.Scan(ctx, nil, nil, 3)` — limit to 3.
3. Assert `len(pairs) == 3`, keys: "a", "b", "c".

### 2.5 Advanced Operations

#### `TestClientCompareAndSwap`
**Goal:** Verify CAS routes to the correct region.

**Steps:**
1. Create a 2-region cluster.
2. `client.Put(ctx, "zebra", "old")`.
3. `ok, prev := client.CompareAndSwap(ctx, "zebra", "new", "old", false)`.
4. Assert `ok == true`.
5. `client.Get(ctx, "zebra")` returns "new".

### 2.6 Error Handling

#### `TestClientRetriesOnNotLeader`
**Goal:** Verify automatic retry when the cached leader is stale.

**Steps:**
1. Create a 3-node cluster.
2. `client.Put(ctx, "key", "val")` — succeeds, leader info cached.
3. Manually trigger a leadership transfer (stop leader, wait for re-election).
4. `client.Get(ctx, "key")` — first attempt gets `NotLeader`, client retries on new leader.
5. Assert returns "val".

---

## 3. Implementation Instructions

### 3.1 File locations

| File | Content |
|------|---------|
| `e2e/client_lib_test.go` | All tests above + `newClientTestCluster` helper |

### 3.2 Test execution

```bash
# Run all client library e2e tests
go test -v -timeout 300s -run "TestClient" ./e2e/

# Run specific test
go test -v -timeout 60s -run "TestClientScanAcrossRegions" ./e2e/
```

### 3.3 Test dependencies

Tests depend on:
- `pkg/client` package (the client library being tested)
- `e2e/helpers_test.go` (`dialTikvClient`, `startPDServer`, `newPDClient`)
- `internal/server` (server setup for test nodes)
- `internal/raftstore/split` (for manual split in cache invalidation test)

### 3.4 Timeout guidance

| Test type | Recommended timeout |
|-----------|-------------------|
| Single-region, no failover | 30s |
| Multi-region routing | 60s |
| Leader failover | 60s |
| PD-coordinated split | 120s |

### 3.5 Implementation order

1. **First**: Implement `PDStoreResolver` and `RegionCache` with unit tests.
2. **Then**: Implement `RegionRequestSender` with unit tests (mock PD + mock server).
3. **Then**: Implement `RawKVClient` with unit tests.
4. **Finally**: Write and run e2e tests against real PD + gookv-server cluster.

Each step should be independently compilable and testable before moving to the next.
