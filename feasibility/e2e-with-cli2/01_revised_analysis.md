# Revised Feasibility Analysis: Replacing e2elib Client Calls with gookv-cli

Date: 2026-03-29
Revision: 2 (relaxed constraints)

## 1. Executive Summary

The first feasibility study concluded that **31% of tests (23/75)** were fully replaceable with the current CLI, rising to **43% (32/75)** with CLI additions. Under significantly relaxed constraints, this second study finds:

| Metric | Study 1 | Study 2 (Relaxed) |
|---|---|---|
| Fully replaceable today | 23 (31%) | 24 (32%) |
| Replaceable with enhancements | 32 (43%) | **63 (84%)** |
| Permanently library-only | 36 (48%) | **12 (16%)** |

The key enablers are:
1. **`--addr` flag** on gookv-cli for direct node targeting (replaces `DialTikvClient` for RawKV operations)
2. **10 new CLI commands** for PD admin operations (BOOTSTRAP, PUT STORE, ALLOC ID, etc.)
3. **e2elib CLI wrapper functions** that encapsulate CLI invocation, output parsing, and polling
4. **Test rewriting** to use CLI assertions instead of Go API assertions

Only **12 tests (16%)** are truly irreplaceable — all inspect protocol-level gRPC response fields (OnePcCommitTs, MinCommitTs, lock structures, MVCC version-filtered scans) that have no CLI equivalent.

## 2. Relaxed Constraints

This analysis operates under constraints significantly relaxed from the first study:

1. **Test rewriting allowed** — Tests may be rewritten as long as they verify the same intended behavior. Structure and assertions can change; only the verification goal must be preserved.
2. **e2elib enhancements allowed** — New functions may be added to `pkg/e2elib/` including CLI wrapper functions, output parsing utilities, and polling helpers that invoke gookv-cli internally.
3. **gookv-cli enhancements allowed** — New commands may be added to cover gaps (BOOTSTRAP, PUT STORE, ALLOC ID, IS BOOTSTRAPPED, ASK SPLIT, REPORT SPLIT, STORE HEARTBEAT, SCANLOCK, PREWRITE, etc.).
4. **Shell script execution allowed** — e2elib may exec POSIX shell scripts, encapsulated in e2elib functions.
5. **`--addr` flag for direct node targeting** — gookv-cli may accept `--addr` to connect directly to a specific KV node's gRPC endpoint instead of routing through PD.

**Key Principle:** From the perspective of each test function, no direct Go function calls to `pkg/client` or `pkg/pdclient` should be needed, except for tests that exercise low-level gRPC protocol behavior that a human would never perform through a CLI.

## 3. Revised e2elib Architecture

### 3.1 Proposed CLI Wrapper Functions

New functions in `pkg/e2elib/` that internally invoke `gookv-cli` as a subprocess:

| Function | Signature | Description |
|---|---|---|
| `CLIExec` | `(t *testing.T, pdAddr string, stmt string) string` | Generic CLI execution via `gookv-cli --pd <addr> -e "<stmt>"` |
| `CLIPut` | `(t *testing.T, pdAddr, key, value string)` | Wraps `PUT <key> <value>` |
| `CLIGet` | `(t *testing.T, pdAddr, key string) (string, bool)` | Wraps `GET <key>`, returns (value, found) |
| `CLINodeExec` | `(t *testing.T, nodeAddr, stmt string) string` | CLI with `--addr` for direct KV node targeting |
| `CLIWaitForCondition` | `(t *testing.T, pdAddr, stmt string, checkFn func(string) bool, timeout time.Duration)` | Polls CLI command until checkFn returns true |
| `CLIWaitForStoreCount` | `(t *testing.T, pdAddr string, minCount int, timeout time.Duration) int` | Parses `STORE LIST` output row count |
| `CLIWaitForRegionLeader` | `(t *testing.T, pdAddr, key string, timeout time.Duration) uint64` | Parses `REGION <key>` output for leader store ID |
| `CLIWaitForRegionCount` | `(t *testing.T, pdAddr string, minCount int, timeout time.Duration) int` | Parses `REGION LIST` output row count |
| `CLIParallel` | `(t *testing.T, addrs []string, stmt string) []string` | Concurrent per-node CLI execution (goroutines + subprocesses) |

### 3.2 Proposed `--addr` Flag

```
gookv-cli --addr 127.0.0.1:20160 -e "PUT mykey myvalue"
```

When `--addr` is specified, gookv-cli connects directly to the given KV node's gRPC endpoint using `pkg/client` internally (bypassing PD discovery). This replaces the `DialTikvClient` pattern for tests that target specific nodes.

### 3.3 Shell Script Encapsulation

Complex polling and multi-step CLI workflows are encapsulated inside e2elib Go functions. The test code remains clean Go; shell complexity is hidden:

```go
// Example: e2elib internally runs a polling loop
func CLIWaitForStoreCount(t *testing.T, pdAddr string, minCount int, timeout time.Duration) int {
    t.Helper()
    var count int
    WaitForCondition(t, timeout, fmt.Sprintf("waiting for %d stores", minCount), func() bool {
        out := CLIExec(t, pdAddr, "STORE LIST")
        count = countOutputRows(out)
        return count >= minCount
    })
    return count
}
```

## 4. Revised Test Classification

### 4.1 Full Classification Table

| File | Test Function | Study 1 Verdict | Study 2 Verdict | Category | What Changes | New CLI Commands |
|---|---|---|---|---|---|---|
| **client_lib_test.go** | | | | | | |
| | `TestClientRegionCacheMiss` | Full | Full | A | — | — |
| | `TestClientRegionCacheHit` | Full | Full | A | — | — |
| | `TestClientStoreResolution` | Full | Full | A | — | — |
| | `TestClientBatchGetAcrossRegions` | Full | Full | A | — | — |
| | `TestClientBatchPutAcrossRegions` | Full | Full | A | — | — |
| | `TestClientScanAcrossRegions` | Full | Full | A | — | — |
| | `TestClientScanWithLimit` | Full | Full | A | — | — |
| | `TestClientCompareAndSwap` | Full | Full | A | — | — |
| | `TestClientBatchDeleteAcrossRegions` | Full | Full | A | — | — |
| **cluster_raw_kv_test.go** | | | | | | |
| | `TestClusterRawKVOperations` | Full | Full | A | — | — |
| | `TestClusterRawKVBatchPutAndScan` | Full | Full | A | — | — |
| **cluster_server_test.go** | | | | | | |
| | `TestClusterServerLeaderElection` | Full | Full | A | — | — |
| | `TestClusterServerKvOperations` | Full | Full | A | — | — |
| | `TestClusterServerCrossNodeReplication` | Partial | **Full** | B | DialTikvClient per-node RawGet → `CLINodeExec(nodeAddr, "GET repl-key")` | `--addr` |
| | `TestClusterServerNodeFailure` | Partial | **Full** | D | StopNode stays Go; RawKV.Put/Get → CLIPut/CLIGet | — |
| | `TestClusterServerLeaderFailover` | Partial | **Full** | D | StopNode stays Go; WaitForRegionLeader + RawKV → CLI wrappers | — |
| **add_node_test.go** | | | | | | |
| | `TestAddNode_JoinRegistersWithPD` | Partial | **Full** | D | AddNode stays Go; GetAllStores → `CLIExec("STORE LIST")` | — |
| | `TestAddNode_PDSchedulesRegionToNewStore` | Partial | **Full** | D | AddNode stays Go; GetStore → `CLIExec("STORE STATUS <id>")` | — |
| | `TestAddNode_FullMoveLifecycle` | Partial | **Full** | D | AddNode stays Go; RawKV.Put/Get → CLIPut/CLIGet | — |
| | `TestAddNode_MultipleJoinNodes` | Partial | **Full** | D | AddNode stays Go; GetAllStores + RawKV → CLI wrappers | — |
| **pd_server_test.go** | | | | | | |
| | `TestPDServerBootstrapAndTSO` | No | **Full** | C | Rewrite: `IS BOOTSTRAPPED; BOOTSTRAP 1 <addr>; TSO; ALLOC ID` | `IS BOOTSTRAPPED`, `BOOTSTRAP`, `ALLOC ID` |
| | `TestPDServerStoreAndRegionMetadata` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; PUT STORE 2 <addr>; STORE STATUS 2; REGION ID 1; REGION <key>` | `BOOTSTRAP`, `PUT STORE` |
| | `TestPDAskBatchSplitAndReport` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; ASK SPLIT 1 1; REPORT SPLIT <json>; REGION <key>` | `BOOTSTRAP`, `ASK SPLIT`, `REPORT SPLIT` |
| | `TestPDStoreHeartbeat` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; STORE HEARTBEAT 1 REGIONS 5` | `BOOTSTRAP`, `STORE HEARTBEAT` |
| **pd_cluster_integration_test.go** | | | | | | |
| | `TestPDClusterStoreAndRegionHeartbeat` | Partial | **Full** | A | WaitForStoreCount + GetStore + GetRegion → existing CLI: `STORE LIST`, `STORE STATUS`, `REGION ""` | — |
| | `TestPDClusterTSOForTransactions` | Full | Full | A | — | — |
| | `TestPDClusterGCSafePoint` | No | **Full** | C | Rewrite: `GC SAFEPOINT; GC SAFEPOINT SET 1000; GC SAFEPOINT` | `GC SAFEPOINT SET` |
| **pd_leader_discovery_test.go** | | | | | | |
| | `TestPDStoreRegistration` | Full | Full | A | — | — |
| | `TestPDRegionLeaderTracking` | Full | Full | A | — | — |
| | `TestPDLeaderFailover` | Partial | **Full** | D | StopNode stays Go; GetRegion + GetStore → CLI: `REGION ""; STORE STATUS <id>` | — |
| **pd_replication_test.go** | | | | | | |
| | `TestPDReplication_LeaderElection` | No | **Full** | C | Rewrite: `IS BOOTSTRAPPED` on PD via CLI | `IS BOOTSTRAPPED`, `BOOTSTRAP` |
| | `TestPDReplication_WriteForwarding` | No | **Full** | C | Rewrite: `gookv-cli --pd <follower> -e "PUT STORE 100 <addr>"` + polling `STORE STATUS 100` | `PUT STORE` |
| | `TestPDReplication_Bootstrap` | No | **Full** | C | Rewrite: `gookv-cli --pd <nodeN> -e "IS BOOTSTRAPPED"` per-node | `IS BOOTSTRAPPED` |
| | `TestPDReplication_TSOMonotonicity` | Full | Full | A | — | — |
| | `TestPDReplication_LeaderFailover` | No | **Full** | D | StopNode stays Go; PutStore + GetStore + GetTS → CLI with `--pd` targeting surviving nodes | `PUT STORE` |
| | `TestPDReplication_SingleNodeCompat` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; TSO (10x); ALLOC ID (10x)` | `BOOTSTRAP`, `ALLOC ID` |
| | `TestPDReplication_IDAllocMonotonicity` | No | **Full** | C | Rewrite: `ALLOC ID` 50x, parse output, check monotonicity | `ALLOC ID` |
| | `TestPDReplication_GCSafePoint` | No | **Full** | C | Rewrite: `GC SAFEPOINT SET 2000` on leader, `gookv-cli --pd <nodeN> -e "GC SAFEPOINT"` per-node | `GC SAFEPOINT SET` |
| | `TestPDReplication_RegionHeartbeat` | Full | Full | A | — | — |
| | `TestPDReplication_AskBatchSplit` | No | **Full** | C | Rewrite: `REGION ID 1; ASK SPLIT <regionID> 2`, check output for unique IDs | `ASK SPLIT` |
| | `TestPDReplication_ConcurrentWritesFromMultipleClients` | No | **Full** | D | Rewrite: e2elib spawns 3 goroutines, each runs `gookv-cli --pd <nodeN> -e "ALLOC ID"` 10x concurrently | `ALLOC ID` |
| | `TestPDReplication_TSOViaFollower` | Partial | **Full** | C | Rewrite: `gookv-cli --pd <nodeN> -e "TSO"` per-node 10x | — |
| | `TestPDReplication_TSOViaFollowerForwarding` | Partial | **Full** | C | Rewrite: `gookv-cli --pd <nodeN> -e "TSO"` per-node 20x | — |
| | `TestPDReplication_RegionHeartbeatViaFollower` | Partial | **Full** | C | Rewrite: `gookv-cli --pd <nodeN> -e "REGION ID 1"` per-node | — |
| | `TestPDReplication_5NodeCluster` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; PUT STORE 1 <addr>; TSO` | `BOOTSTRAP`, `PUT STORE` |
| | `TestPDReplication_CatchUpRecovery` | No | **Full** | D | StopNode + RestartNode stay Go; PutStore + GetStore → CLI per-node | `PUT STORE` |
| **region_split_test.go** | | | | | | |
| | `TestRegionSplitWithPD` | No | **Full** | C | Rewrite: `BOOTSTRAP ...; ASK SPLIT 1 1; REPORT SPLIT <json>; REGION <key>` + polling | `BOOTSTRAP`, `ASK SPLIT`, `REPORT SPLIT` |
| **multi_region_test.go** | | | | | | |
| | `TestMultiRegionKeyRouting` | Full | Full | A | — | — |
| | `TestMultiRegionIndependentLeaders` | Full | Full | A | — | — |
| | `TestMultiRegionRawKV` | Full | Full | A | — | — |
| **multi_region_routing_test.go** | | | | | | |
| | `TestMultiRegionTransactions` | No | **No** | F | KvPrewrite + KvCommit with explicit startTS across regions | — |
| | `TestMultiRegionRawKVBatchScan` | Full | Full | A | — | — |
| | `TestMultiRegionSplitWithLiveTraffic` | Partial | **Full** | D | Cluster lifecycle stays Go; RawKV.Put/Get + WaitForSplit → CLI wrappers | — |
| | `TestMultiRegionPDCoordinatedSplit` | Full | Full | A | — | — |
| | `TestMultiRegionAsyncCommit` | No | **No** | F | KvPrewrite with UseAsyncCommit + MinCommitTs inspection across regions | — |
| | `TestMultiRegionScanLock` | No | **No** | F | KvPrewrite + KvScanLock + lock version inspection across regions | — |
| **raw_kv_test.go** | | | | | | |
| | `TestRawKVPutGetDelete` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "PUT raw-key-1 raw-val-1; GET raw-key-1; DELETE raw-key-1; GET raw-key-1"` | `--addr` |
| | `TestRawKVBatchOperations` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "BPUT ...; BGET ...; SCAN ..."`, parse counts | `--addr` |
| | `TestRawKVDeleteRange` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "PUT ...; DELETE RANGE dr-key-01 dr-key-04; GET ..."` | `--addr` |
| **raw_kv_extended_test.go** | | | | | | |
| | `TestRawBatchScan` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "BSCAN range1 range2 EACH_LIMIT 10"`, parse output | `--addr`, `BSCAN` |
| | `TestRawGetKeyTTL` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "PUT ttl-key val; TTL ttl-key; TTL nonexistent"` | `--addr` |
| | `TestRawCompareAndSwap` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "CAS cas-key initial \"\" NOT_EXIST; GET cas-key; CAS cas-key updated initial"` | `--addr` |
| | `TestRawChecksum` | No | **Full** | B | Rewrite: `gookv-cli --addr <node> -e "PUT ...; CHECKSUM csum-00 csum-99"`, parse totalKvs/totalBytes/checksum | `--addr` |
| **async_commit_test.go** | | | | | | |
| | `TestAsyncCommit1PCPrewrite` | No | **No** | F | Inspects `prewriteResp.GetOnePcCommitTs() > 0`, uses commitTs for versioned KvGet | — |
| | `TestAsyncCommitPrewrite` | No | **No** | F | Inspects `prewriteResp.GetMinCommitTs() > 0`, verifies key NOT readable while locked | — |
| | `TestCheckSecondaryLocks` | No | **No** | F | Uses `KvCheckSecondaryLocks`, inspects `Locks` array and `CommitTs` | — |
| | `TestScanLock` | No | **No** | F | Creates locks via KvPrewrite, then `KvScanLock` with maxVersion, inspects `lock.GetLockVersion()` | — |
| **txn_rpc_test.go** | | | | | | |
| | `TestTxnPessimisticLockAcquire` | No | **No** | F | `KvPessimisticLock` with explicit ForUpdateTs, LockTtl, mutation | — |
| | `TestTxnPessimisticRollback` | No | **No** | F | `KVPessimisticRollback` with explicit StartVersion, ForUpdateTs | — |
| | `TestTxnHeartBeat` | No | **No** | F | Inspects `hbResp.GetLockTtl() >= 20000` from `KvTxnHeartBeat` | — |
| | `TestTxnResolveLock` | No | **No** | F | `KvResolveLock` with CommitVersion, then versioned `KvGet` at version 320 | — |
| | `TestTxnScanWithVersionVisibility` | No | **No** | F | `KvScan` at version 25 vs 15 for MVCC visibility | — |
| **restart_replay_test.go** | | | | | | |
| | `TestRestartDataSurvives` | Partial | **Full** | D | RestartNode stays Go; RawKV.Put/Get → CLIPut/CLIGet + CLIWaitForCondition | — |
| | `TestRestartLeaderFailoverAndReplay` | Partial | **Full** | D | StopNode + RestartNode stay Go; WaitForRegionLeader + RawKV → CLI wrappers | — |
| | `TestRestartAllNodesDataSurvives` | Partial | **Full** | D | StopNode(3x) + RestartNode(3x) stay Go; RawKV → CLI wrappers | — |

### 4.2 Verdict Change Summary

| Change | Count | Tests |
|---|---|---|
| No → Full (new CLI commands) | 24 | pd_server (4), pd_replication (12), pd_cluster_integration (1), region_split (1), raw_kv (3), raw_kv_extended (4) — note: some pd_replication overlap with process mgmt |
| Partial → Full (CLI wrappers) | 15 | cluster_server (2), add_node (4), pd_leader_discovery (1), pd_replication (2), pd_cluster_integration (1), multi_region_routing (1), restart_replay (3) — note: pd_cluster StoreAndRegionHeartbeat was Partial, now A |
| Full → Full (unchanged) | 23 | client_lib (9), cluster_raw_kv (2), cluster_server (2), pd_cluster_integration (1), pd_leader_discovery (2), pd_replication (2), multi_region (3), multi_region_routing (2) — note: 1 moved from Partial to A |
| No → No (still irreplaceable) | 12 | async_commit (4), txn_rpc (5), multi_region_routing (3) |

## 5. Revised Coverage Numbers

| Category | Count | % | Description |
|---|---|---|---|
| **A: Already replaceable** | 24 | 32% | Existing CLI commands suffice |
| **B: Replaceable with `--addr` flag** | 8 | 11% | raw_kv + raw_kv_extended + CrossNodeReplication |
| **C: Replaceable with new PD CLI commands** | 16 | 21% | pd_server, pd_replication, region_split, pd_cluster GCSafePoint |
| **D: Hybrid (process mgmt Go + CLI)** | 15 | 20% | StopNode/RestartNode/AddNode stay Go; KV/PD via CLI |
| **F: Not replaceable** | 12 | 16% | Inspect protocol-level gRPC response fields |
| | | | |
| **Total replaceable (A+B+C+D)** | **63** | **84%** | |
| **Total not replaceable (F)** | **12** | **16%** | |

### Comparison with First Study

| Metric | Study 1 | Study 2 |
|---|---|---|
| Replaceable today (no changes) | 23 (31%) | 24 (32%) |
| Replaceable with CLI additions only | 32 (43%) | 48 (64%) |
| Replaceable with all enhancements | 32 (43%) | **63 (84%)** |
| Permanently library-only | 36 (48%) | **12 (16%)** |
| New CLI commands needed | 8 | 10 |
| e2elib changes needed | 0 | 8 new functions |

The 41-point improvement (43% → 84%) comes from:
- **`--addr` flag** (+8 tests): Raw KV gRPC tests are functionally equivalent to CLI operations
- **Test rewriting** (+7 tests): PD protocol tests can be rewritten to parse CLI output
- **e2elib CLI wrappers** (+15 tests): Process management stays in Go; only KV/PD assertions switch to CLI
- **Per-node `--pd` targeting** (+6 tests): TSO/RegionHeartbeat follower tests

## 6. Irreplaceable Tests — Detailed Justification

### 6.1 async_commit_test.go (4 tests)

**TestAsyncCommit1PCPrewrite** — Sends `KvPrewrite` with `TryOnePc: true`, then asserts `prewriteResp.GetOnePcCommitTs() > 0`. Uses the returned commit timestamp to construct a versioned `KvGet` at `commitTs+1`. A CLI cannot expose the one-PC commit timestamp from the prewrite response — it is an internal protocol optimization detail.

**TestAsyncCommitPrewrite** — Sends `KvPrewrite` with `UseAsyncCommit: true` and a secondaries list, then asserts `prewriteResp.GetMinCommitTs() > 0`. Verifies the key is NOT readable via `KvGet` while only prewritten (not committed). Testing the async commit protocol's min-commit-ts guarantee is protocol-internal.

**TestCheckSecondaryLocks** — Uses `KvCheckSecondaryLocks` RPC which returns a `Locks` array and `CommitTs`. This RPC is used internally by the lock resolution protocol — no human would invoke it through a CLI. The test verifies the lock resolution mechanism works correctly.

**TestScanLock** — Creates 3 locks at different `StartVersion` values via `KvPrewrite`, then calls `KvScanLock` with `MaxVersion` filtering. Inspects `lock.GetLockVersion()` for each returned lock. Version-filtered lock scanning is a protocol-internal operation for garbage collection and lock resolution.

### 6.2 txn_rpc_test.go (5 tests)

**TestTxnPessimisticLockAcquire** — Calls `KvPessimisticLock` with explicit `ForUpdateTs`, `LockTtl`, `WaitTimeout`, and mutation parameters. Inspects `pessResp.GetErrors()`. Pessimistic locking is a protocol-internal operation (the high-level Txn API handles it automatically).

**TestTxnPessimisticRollback** — Calls `KVPessimisticRollback` with explicit `StartVersion` and `ForUpdateTs`. This is an internal cleanup operation that the transaction protocol performs automatically.

**TestTxnHeartBeat** — Creates a lock via `KvPrewrite`, then calls `KvTxnHeartBeat` with `AdviseLockTtl: 20000`. Asserts `hbResp.GetLockTtl() >= 20000`. Lock TTL heartbeat is protocol-internal — the client library performs it automatically in background goroutines.

**TestTxnResolveLock** — Creates a lock via `KvPrewrite`, resolves it via `KvResolveLock` with explicit `CommitVersion: 300`, then does a versioned `KvGet` at `Version: 320` to verify the value is readable. Explicit version-based reads are protocol-internal.

**TestTxnScanWithVersionVisibility** — Writes two versions of the same key (prewrite+commit at ts=10, then at ts=20), then scans at `Version: 25` (sees latest) vs `Version: 15` (sees only first). MVCC version-filtered scanning is protocol-internal — the high-level API always reads at the latest snapshot.

### 6.3 multi_region_routing_test.go (3 tests)

**TestMultiRegionTransactions** — Uses `pdclient.GetTS()` for explicit start timestamp, then calls `KvPrewrite` and `KvCommit` with those timestamps across multiple regions. The multi-region prewrite/commit sequence with explicit timestamp control is protocol-internal.

**TestMultiRegionAsyncCommit** — Same as `TestMultiRegionTransactions` but with `UseAsyncCommit: true` flag and inspection of `MinCommitTs` and `RegionError` across regions.

**TestMultiRegionScanLock** — Calls `KvPrewrite` to create locks across regions, then `KvScanLock` to verify lock visibility across region boundaries. Inspects `scanResp.GetLocks()` with version filtering.

### 6.4 Common Pattern

All 12 irreplaceable tests share one or more of these characteristics:
1. **Construct raw gRPC requests** with specific protocol fields (`StartVersion`, `CommitVersion`, `ForUpdateTs`, `UseAsyncCommit`, `TryOnePc`, `MinCommitTs`)
2. **Inspect raw gRPC response fields** (`OnePcCommitTs`, `MinCommitTs`, `LockTtl`, `Locks[]`, `Errors[]`)
3. **Perform versioned reads** (`KvGet` or `KvScan` at specific MVCC timestamps)
4. **Test internal protocol RPCs** that have no user-facing equivalent (`KvCheckSecondaryLocks`, `KvScanLock`, `KvPessimisticLock`, `KvTxnHeartBeat`, `KvResolveLock`)

These operations verify **protocol correctness**, not user-facing behavior. A human would never perform them through a CLI — they are the equivalent of PostgreSQL's `libpq` protocol tests that remain in C.

## 7. Required gookv-cli Additions

### 7.1 New Flag: `--addr`

```
gookv-cli --addr 127.0.0.1:20160 -e "PUT mykey myvalue"
```

When `--addr` is specified, the CLI connects directly to the given KV node's gRPC endpoint (bypassing PD-based region routing). This enables direct-node testing equivalent to `DialTikvClient`. Mutually exclusive with `--pd`.

**Implementation:** In `main.go`, add `--addr` flag. When set, create a `RawKVClient` that connects to the given address directly (similar to how standalone node tests work).

### 7.2 New Commands

| # | Command | Syntax | Backing API | Difficulty | Tests Unlocked |
|---|---|---|---|---|---|
| 1 | `BOOTSTRAP` | `BOOTSTRAP <storeID> <addr> [<regionID>]` | `pdclient.Bootstrap(ctx, store, region)` | Medium | TestPDServerBootstrapAndTSO, TestPDServerStoreAndRegionMetadata, TestPDAskBatchSplitAndReport, TestPDStoreHeartbeat, TestPDReplication_LeaderElection, TestPDReplication_SingleNodeCompat, TestPDReplication_5NodeCluster, TestRegionSplitWithPD |
| 2 | `PUT STORE` | `PUT STORE <id> <addr>` | `pdclient.PutStore(ctx, &metapb.Store{Id: id, Address: addr})` | Low | TestPDServerStoreAndRegionMetadata, TestPDReplication_WriteForwarding, TestPDReplication_LeaderFailover, TestPDReplication_5NodeCluster, TestPDReplication_CatchUpRecovery |
| 3 | `ALLOC ID` | `ALLOC ID` | `pdclient.AllocID(ctx)` | Low | TestPDServerBootstrapAndTSO, TestPDReplication_SingleNodeCompat, TestPDReplication_IDAllocMonotonicity, TestPDReplication_ConcurrentWritesFromMultipleClients |
| 4 | `IS BOOTSTRAPPED` | `IS BOOTSTRAPPED` | `pdclient.IsBootstrapped(ctx)` | Low | TestPDReplication_LeaderElection, TestPDReplication_Bootstrap, TestPDServerBootstrapAndTSO |
| 5 | `ASK SPLIT` | `ASK SPLIT <regionID> <splitCount>` | `pdclient.AskBatchSplit(ctx, region, count)` | Medium | TestPDAskBatchSplitAndReport, TestPDReplication_AskBatchSplit, TestRegionSplitWithPD |
| 6 | `REPORT SPLIT` | `REPORT SPLIT <leftJSON> <rightJSON>` | `pdclient.ReportBatchSplit(ctx, regions)` | Medium | TestPDAskBatchSplitAndReport, TestRegionSplitWithPD |
| 7 | `STORE HEARTBEAT` | `STORE HEARTBEAT <storeID> [REGIONS <n>]` | `pdclient.StoreHeartbeat(ctx, stats)` | Medium | TestPDStoreHeartbeat |
| 8 | `GC SAFEPOINT SET` | `GC SAFEPOINT SET <timestamp>` | `pdclient.UpdateGCSafePoint(ctx, ts)` | Low | TestPDClusterGCSafePoint, TestPDReplication_GCSafePoint |
| 9 | `BSCAN` | `BSCAN <s1> <e1> [<s2> <e2> ...] [EACH_LIMIT <n>]` | `rawKV.BatchScan(ctx, ranges, eachLimit)` | Medium | TestRawBatchScan |

**Total new handlers: 9 commands + 1 flag**

### 7.3 Implementation Effort Summary

| Difficulty | Commands | Est. Effort |
|---|---|---|
| Low | PUT STORE, ALLOC ID, IS BOOTSTRAPPED, GC SAFEPOINT SET | 1 day |
| Medium | BOOTSTRAP, ASK SPLIT, REPORT SPLIT, STORE HEARTBEAT, BSCAN, --addr flag | 3 days |
| **Total** | 10 items | **4 days** |
