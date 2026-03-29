# Feasibility Analysis: Replacing e2elib Client Calls with gookv-cli

Date: 2026-03-29

## Section 1: e2elib Client Function Inventory

Source: `pkg/e2elib/helpers.go` (14 exported functions + 1 internal)

| Function | Underlying API | Replaceable by CLI | Notes |
|---|---|---|---|
| `NewStandaloneNode` | Process management | No | Cluster lifecycle; not a client call |
| `PutAndVerify` | `RawKVClient.Put` + `Get` | Yes | CLI: `PUT <k> <v>` then `GET <k>` |
| `GetAndAssert` | `RawKVClient.Get` | Yes | CLI: `GET <k>` |
| `GetAndAssertNotFound` | `RawKVClient.Get` | Yes | CLI: `GET <k>` (check "not found" output) |
| `WaitForCondition` | Generic polling loop | Partial | Shell `while` loop + CLI command; no direct CLI equivalent |
| `WaitForRegionCount` | `pdclient.GetRegion` (walk keyspace) | Yes | CLI: `REGION LIST` + count rows |
| `WaitForSplit` | Wrapper for `WaitForRegionCount(..., 2, ...)` | Yes | CLI: `REGION LIST` + count >= 2 |
| `countRegions` | `pdclient.GetRegion` (internal, unexported) | Yes | CLI: `REGION LIST` |
| `WaitForStoreCount` | `pdclient.GetAllStores` | Yes | CLI: `STORE LIST` + count rows |
| `WaitForRegionLeader` | `pdclient.GetRegion` | Yes | CLI: `REGION <key>` + parse leader field |
| `SeedAccounts` | `TxnKVClient.Begin/Set/Commit` | Yes | CLI: `BEGIN; SET ...; COMMIT` in loop |
| `ReadAllBalances` | `TxnKVClient.Begin/Get/Commit` | Yes | CLI: `BEGIN; GET ...; COMMIT` in loop |
| `DialTikvClient` | Raw gRPC `tikvpb.TikvClient` | **No** | Returns raw gRPC stub; no CLI equivalent |

**Summary:** 11 of 14 exported functions are fully or partially replaceable. `DialTikvClient` is fundamentally not replaceable because it returns a raw gRPC client used to call protocol-level RPCs (KvPrewrite, KvCommit, KvScanLock, etc.).

## Section 2: e2e_external Test Classification

### 2.1 raw_kv_test.go (3 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestRawKVPutGetDelete` | `DialTikvClient`, `RawPut`, `RawGet`, `RawDelete` (gRPC) | No | Raw gRPC `RawPut`/`RawGet`/`RawDelete` |
| `TestRawKVBatchOperations` | `DialTikvClient`, `RawBatchPut`, `RawBatchGet`, `RawScan` (gRPC) | No | Raw gRPC batch RPCs |
| `TestRawKVDeleteRange` | `DialTikvClient`, `RawPut`, `RawDeleteRange`, `RawGet` (gRPC) | No | Raw gRPC `RawDeleteRange` |

### 2.2 raw_kv_extended_test.go (4 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestRawBatchScan` | `DialTikvClient`, `RawPut`, `RawBatchScan` (gRPC) | No | Raw gRPC `RawBatchScan` (no CLI equivalent) |
| `TestRawGetKeyTTL` | `DialTikvClient`, `RawPut`, `RawGetKeyTTL` (gRPC) | No | Raw gRPC `RawGetKeyTTL` |
| `TestRawCompareAndSwap` | `DialTikvClient`, `RawCompareAndSwap`, `RawGet` (gRPC) | No | Raw gRPC `RawCompareAndSwap` (4 subtests) |
| `TestRawChecksum` | `DialTikvClient`, `RawPut`, `RawChecksum` (gRPC) | No | Raw gRPC `RawChecksum` |

### 2.3 async_commit_test.go (4 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestAsyncCommit1PCPrewrite` | `DialTikvClient`, `KvPrewrite` (TryOnePc), `KvGet` | No | Raw gRPC `KvPrewrite` with `TryOnePc` flag |
| `TestAsyncCommitPrewrite` | `DialTikvClient`, `KvPrewrite` (UseAsyncCommit), `KvGet`, `KvBatchRollback` | No | Raw gRPC async commit protocol |
| `TestCheckSecondaryLocks` | `DialTikvClient`, `KvPrewrite`, `KvCheckSecondaryLocks`, `KvCommit` | No | Raw gRPC `KvCheckSecondaryLocks` |
| `TestScanLock` | `DialTikvClient`, `KvPrewrite`, `KvScanLock`, `KvBatchRollback` | No | Raw gRPC `KvScanLock` |

### 2.4 client_lib_test.go (7 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestClientRegionCacheMiss` | `RawKVClient.Put`, `Get` | **Full** | -- |
| `TestClientRegionCacheHit` | `RawKVClient.Put`, `Get` | **Full** | -- |
| `TestClientStoreResolution` | `RawKVClient.Put`, `Get` | **Full** | -- |
| `TestClientBatchGetAcrossRegions` | `RawKVClient.Put`, `BatchGet` | **Full** | -- |
| `TestClientBatchPutAcrossRegions` | `RawKVClient.BatchPut`, `Get` | **Full** | -- |
| `TestClientScanAcrossRegions` | `RawKVClient.Put`, `Scan` | **Full** | -- |
| `TestClientScanWithLimit` | `RawKVClient.Put`, `Scan` | **Full** | -- |
| `TestClientCompareAndSwap` | `RawKVClient.Put`, `CompareAndSwap`, `Get` | **Full** | -- |
| `TestClientBatchDeleteAcrossRegions` | `RawKVClient.Put`, `BatchDelete`, `Get` | **Full** | -- |

### 2.5 cluster_raw_kv_test.go (2 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestClusterRawKVOperations` | `RawKVClient.Put`, `Get`, `Delete` | **Full** | -- |
| `TestClusterRawKVBatchPutAndScan` | `RawKVClient.Put`, `Scan` | **Full** | -- |

### 2.6 cluster_server_test.go (5 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestClusterServerLeaderElection` | `pdclient.GetRegion` via `WaitForRegionLeader` | **Full** | -- |
| `TestClusterServerKvOperations` | `RawKVClient.Put`, `Get` | **Full** | -- |
| `TestClusterServerCrossNodeReplication` | `RawKVClient.Put`, `Get` + `DialTikvClient` + `RawGet` (gRPC) | **Partial** | gRPC verification of per-node replication |
| `TestClusterServerNodeFailure` | `RawKVClient.Put`, `Get` + `WaitForCondition` + `cluster.StopNode` | Partial | `StopNode` is process management |
| `TestClusterServerLeaderFailover` | `RawKVClient.Put`, `Get` + `WaitForRegionLeader` + `StopNode` | Partial | `StopNode` is process management |

### 2.7 add_node_test.go (4 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestAddNode_JoinRegistersWithPD` | `pdclient.GetAllStores` + `cluster.AddNode` | Partial | `AddNode` is process management |
| `TestAddNode_PDSchedulesRegionToNewStore` | `pdclient.GetStore` + `cluster.AddNode` + `WaitForCondition` | Partial | `AddNode` + `GetStore` by ID |
| `TestAddNode_FullMoveLifecycle` | `RawKVClient.Put`, `Get` + `cluster.AddNode` | Partial | `AddNode` is process management |
| `TestAddNode_MultipleJoinNodes` | `pdclient.GetAllStores` + `cluster.AddNode` + `RawKVClient.Put`, `Get` | Partial | `AddNode` is process management |

### 2.8 pd_server_test.go (4 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestPDServerBootstrapAndTSO` | `pdclient.IsBootstrapped`, `Bootstrap`, `GetTS`, `AllocID` | No | `Bootstrap`, `AllocID` not in CLI |
| `TestPDServerStoreAndRegionMetadata` | `pdclient.Bootstrap`, `PutStore`, `GetStore`, `GetRegionByID`, `GetRegion` | No | `Bootstrap`, `PutStore` not in CLI |
| `TestPDAskBatchSplitAndReport` | `pdclient.Bootstrap`, `AskBatchSplit`, `ReportBatchSplit`, `GetRegion` | No | `Bootstrap`, `AskBatchSplit`, `ReportBatchSplit` not in CLI |
| `TestPDStoreHeartbeat` | `pdclient.Bootstrap`, `StoreHeartbeat` | No | `Bootstrap`, `StoreHeartbeat` not in CLI |

### 2.9 region_split_test.go (1 test)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestRegionSplitWithPD` | `pdclient.Bootstrap`, `AskBatchSplit`, `ReportBatchSplit`, `GetRegion` + `WaitForCondition` | No | `Bootstrap`, `AskBatchSplit`, `ReportBatchSplit` not in CLI |

### 2.10 pd_cluster_integration_test.go (3 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestPDClusterStoreAndRegionHeartbeat` | `pdclient.GetAllStores`, `GetStore`, `GetRegion` | Partial | Read-only PD queries all in CLI, but heartbeat is server-driven |
| `TestPDClusterTSOForTransactions` | `pdclient.GetTS` (100x loop) | **Full** | CLI: `TSO` command |
| `TestPDClusterGCSafePoint` | `pdclient.GetGCSafePoint`, `UpdateGCSafePoint` | No | `UpdateGCSafePoint` not in CLI |

### 2.11 pd_leader_discovery_test.go (3 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestPDStoreRegistration` | `pdclient.GetAllStores` + `cluster.Nodes()` | **Full** | CLI: `STORE LIST` |
| `TestPDRegionLeaderTracking` | `pdclient.GetRegion`, `GetStore` + `WaitForRegionLeader` | **Full** | CLI: `REGION <key>`, `STORE STATUS <id>` |
| `TestPDLeaderFailover` | `pdclient.GetRegion` + `WaitForCondition` + `cluster.StopNode` | Partial | `StopNode` is process management |

### 2.12 pd_replication_test.go (16 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestPDReplication_LeaderElection` | `pdclient.IsBootstrapped` + `Bootstrap` | No | `Bootstrap` not in CLI |
| `TestPDReplication_WriteForwarding` | `pdclient.PutStore`, `GetStore` + per-node clients | No | `PutStore` not in CLI |
| `TestPDReplication_Bootstrap` | `pdclient.IsBootstrapped` + per-node clients | No | per-node `IsBootstrapped` |
| `TestPDReplication_TSOMonotonicity` | `pdclient.GetTS` (100x loop) | **Full** | CLI: `TSO` |
| `TestPDReplication_LeaderFailover` | `pdclient.PutStore`, `GetTS`, `GetStore` + `StopNode` + new client | No | `PutStore`, `StopNode`, manual client creation |
| `TestPDReplication_SingleNodeCompat` | `pdclient.Bootstrap`, `GetTS`, `AllocID` | No | `Bootstrap`, `AllocID` not in CLI |
| `TestPDReplication_IDAllocMonotonicity` | `pdclient.AllocID` (50x loop) | No | `AllocID` not in CLI |
| `TestPDReplication_GCSafePoint` | `pdclient.UpdateGCSafePoint`, `GetGCSafePoint` + per-node clients | No | `UpdateGCSafePoint` not in CLI |
| `TestPDReplication_RegionHeartbeat` | `pdclient.GetRegionByID` | **Full** | CLI: `REGION ID <id>` |
| `TestPDReplication_AskBatchSplit` | `pdclient.GetRegionByID`, `AskBatchSplit` | No | `AskBatchSplit` not in CLI |
| `TestPDReplication_ConcurrentWritesFromMultipleClients` | `pdclient.AllocID` (concurrent, per-node) | No | `AllocID` not in CLI; concurrent per-node |
| `TestPDReplication_TSOViaFollower` | `pdclient.GetTS` (per-node) | Partial | Per-node targeting not in CLI |
| `TestPDReplication_TSOViaFollowerForwarding` | `pdclient.GetTS` (per-node, 20x) | Partial | Per-node targeting not in CLI |
| `TestPDReplication_RegionHeartbeatViaFollower` | `pdclient.GetRegionByID` (per-node) | Partial | Per-node targeting not in CLI |
| `TestPDReplication_5NodeCluster` | `pdclient.Bootstrap`, `PutStore`, `GetTS` | No | `Bootstrap`, `PutStore` not in CLI |
| `TestPDReplication_CatchUpRecovery` | `pdclient.PutStore`, `GetStore` + `StopNode`, `RestartNode` + per-node | No | `PutStore`, process mgmt |

### 2.13 multi_region_test.go (3 tests + 1 helper)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `newMultiRegionCluster` (helper) | `RawKVClient.Put`, `Get` + `pdclient.GetRegion` + `WaitForSplit` + `WaitForCondition` | Partial | Process lifecycle |
| `TestMultiRegionKeyRouting` | `pdclient.GetRegion` + `WaitForRegionCount` | **Full** | CLI: `REGION <key>`, `REGION LIST` |
| `TestMultiRegionIndependentLeaders` | `pdclient.GetRegion` via `WaitForRegionLeader` | **Full** | CLI: `REGION <key>` |
| `TestMultiRegionRawKV` | `RawKVClient.Put`, `Get` + `WaitForCondition` | **Full** | CLI: `PUT`, `GET` (polling via shell) |

### 2.14 multi_region_routing_test.go (6 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestMultiRegionTransactions` | `DialTikvClient`, `KvPrewrite`, `KvCommit` + `pdclient.GetTS` | No | Raw gRPC `KvPrewrite`, `KvCommit` |
| `TestMultiRegionRawKVBatchScan` | `RawKVClient.Put`, `Scan` + `WaitForCondition` | **Full** | CLI: `PUT`, `SCAN` |
| `TestMultiRegionSplitWithLiveTraffic` | `RawKVClient.Put`, `Get` + `WaitForSplit` + `WaitForCondition` | Partial | Process lifecycle + polling |
| `TestMultiRegionPDCoordinatedSplit` | `pdclient.GetRegion` via `WaitForRegionCount` | **Full** | CLI: `REGION LIST` |
| `TestMultiRegionAsyncCommit` | `DialTikvClient`, `KvPrewrite` (UseAsyncCommit) + `pdclient.GetTS` | No | Raw gRPC async commit |
| `TestMultiRegionScanLock` | `DialTikvClient`, `KvPrewrite`, `KvScanLock` + `pdclient.GetTS` | No | Raw gRPC `KvScanLock` |

### 2.15 txn_rpc_test.go (5 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestTxnPessimisticLockAcquire` | `DialTikvClient`, `KvPessimisticLock` | No | Raw gRPC pessimistic lock |
| `TestTxnPessimisticRollback` | `DialTikvClient`, `KVPessimisticRollback` | No | Raw gRPC pessimistic rollback |
| `TestTxnHeartBeat` | `DialTikvClient`, `KvPrewrite`, `KvTxnHeartBeat`, `KvBatchRollback` | No | Raw gRPC heartbeat |
| `TestTxnResolveLock` | `DialTikvClient`, `KvPrewrite`, `KvResolveLock`, `KvGet` | No | Raw gRPC resolve lock |
| `TestTxnScanWithVersionVisibility` | `DialTikvClient`, `KvPrewrite`, `KvCommit`, `KvScan` | No | Raw gRPC MVCC scan |

### 2.16 restart_replay_test.go (3 tests)

| Test Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `TestRestartDataSurvives` | `RawKVClient.Put`, `Get` + `cluster.RestartNode` + `WaitForCondition` | Partial | `RestartNode` is process management |
| `TestRestartLeaderFailoverAndReplay` | `RawKVClient.Put`, `Get` + `WaitForRegionLeader` + `StopNode`, `RestartNode` | Partial | `StopNode`, `RestartNode` |
| `TestRestartAllNodesDataSurvives` | `RawKVClient.Put`, `Get` + `StopNode`, `RestartNode` (all 3) | Partial | `StopNode`, `RestartNode` |

### 2.17 helpers_test.go (2 helpers, 0 test functions)

| Function | Client APIs Used | Replaceable | Blocking Reason |
|---|---|---|---|
| `newClusterWithLeader` | `RawKVClient.Put` + `WaitForCondition` | N/A (helper) | Cluster lifecycle helper |
| `newClientCluster` | Wrapper around `newClusterWithLeader` | N/A (helper) | Cluster lifecycle helper |

## Section 3: gookv-cli Coverage Assessment

### 3.1 e2elib Function Replaceability

| Category | Count | Functions |
|---|---|---|
| Fully replaceable | 8 | `PutAndVerify`, `GetAndAssert`, `GetAndAssertNotFound`, `SeedAccounts`, `ReadAllBalances`, `WaitForRegionCount`, `WaitForSplit`, `WaitForStoreCount` |
| Partially replaceable | 2 | `WaitForCondition` (shell loop), `WaitForRegionLeader` (parse leader from `REGION` output) |
| Not replaceable | 2 | `DialTikvClient` (raw gRPC), `NewStandaloneNode` (process mgmt) |

**Percentage replaceable: 10/14 = 71%** (excluding `countRegions` which is internal)

### 3.2 Test Function Replaceability

Across all 16 files, there are **75 test functions** (excluding helpers `newClusterWithLeader`, `newClientCluster`, `newMultiRegionCluster`, `startPDOnly`, `newPDCluster`, `bootstrapPDCluster`, `waitForPDClusterLeader`).

| Verdict | Count | Percentage |
|---|---|---|
| **Full** | 23 | 31% |
| **Partial** (KV ops replaceable, but needs process mgmt or shell polling) | 16 | 21% |
| **No** (requires raw gRPC or missing CLI commands) | 36 | 48% |

**Fully replaceable tests (23):**
- `client_lib_test.go`: all 9 tests
- `cluster_raw_kv_test.go`: all 2 tests
- `cluster_server_test.go`: `TestClusterServerLeaderElection`, `TestClusterServerKvOperations` (2)
- `pd_cluster_integration_test.go`: `TestPDClusterTSOForTransactions` (1)
- `pd_leader_discovery_test.go`: `TestPDStoreRegistration`, `TestPDRegionLeaderTracking` (2)
- `pd_replication_test.go`: `TestPDReplication_TSOMonotonicity`, `TestPDReplication_RegionHeartbeat` (2)
- `multi_region_test.go`: `TestMultiRegionKeyRouting`, `TestMultiRegionIndependentLeaders`, `TestMultiRegionRawKV` (3)
- `multi_region_routing_test.go`: `TestMultiRegionRawKVBatchScan`, `TestMultiRegionPDCoordinatedSplit` (2)

### 3.3 CLI Commands Needed to Add

For broader coverage beyond the 22 fully-replaceable tests, these CLI commands would need to be added:

| Command | Enables | Difficulty |
|---|---|---|
| `BOOTSTRAP <storeID> <addr>` | 4 pd_server tests, 7 pd_replication tests | Medium (new pdclient method exposure) |
| `PUT STORE <id> <addr>` | 3 pd_replication tests | Low (pdclient.PutStore already exists) |
| `ALLOC ID` | 3 pd_replication tests, 1 pd_server test | Low (pdclient.AllocID already exists) |
| `STORE HEARTBEAT <id>` | 1 pd_server test | Medium |
| `ASK SPLIT <regionID> <count>` | 2 tests (split flow) | Medium |
| `REPORT SPLIT ...` | 2 tests (split flow) | Medium |
| `GC SAFEPOINT SET <ts>` | 2 tests | Low (pdclient.UpdateGCSafePoint exists) |
| `IS BOOTSTRAPPED` | 3 pd_replication tests | Low |

Adding these 8 commands would raise the fully-replaceable count from 23 to approximately 32 (+9), bringing the percentage from 31% to 43%.

### 3.4 Percentage That Could Eliminate pkg/client Import

Currently the `e2e_external` package imports:
- `pkg/e2elib` (all 16 files)
- `pkg/client` (2 files: `client_lib_test.go`, `helpers_test.go`)
- `kvrpcpb` (7 files: raw gRPC tests)
- `pdclient` (1 file: `pd_replication_test.go`)
- `metapb` (3 files)
- `pdpb` (1 file)

If `client_lib_test.go` and `cluster_raw_kv_test.go` (11 tests) were converted to CLI-based tests, the `pkg/client` import could be removed from the test package for those files. However, `helpers_test.go` would still need `pkg/client` for the `newClientCluster` helper.

## Section 4: Gap Analysis

### 4.1 Fundamentally Library-Only Operations

These raw gRPC RPCs cannot meaningfully be exposed through a CLI because they require fine-grained protocol control (specific field values, multi-step protocol sequences, direct response inspection):

| gRPC RPC | Used In | Why Not CLI |
|---|---|---|
| `KvPrewrite` | 11 tests | Requires setting `StartVersion`, `LockTtl`, `UseAsyncCommit`, `TryOnePc`, `Secondaries`, `MinCommitTs` |
| `KvCommit` | 4 tests | Requires `StartVersion`, `CommitVersion`, specific key lists |
| `KvBatchRollback` | 3 tests | Requires `StartVersion`, specific key lists |
| `KvGet` (versioned) | 3 tests | Requires explicit `Version` for MVCC reads |
| `KvScan` (versioned) | 1 test | Requires explicit `Version` for MVCC visibility |
| `KvScanLock` | 2 tests | Requires `MaxVersion`, `StartKey`, `EndKey`, `Limit` |
| `KvCheckSecondaryLocks` | 1 test | Requires `StartVersion`, specific key list |
| `KvPessimisticLock` | 1 test | Requires `ForUpdateTs`, `LockTtl`, mutation list |
| `KVPessimisticRollback` | 1 test | Requires `StartVersion`, `ForUpdateTs` |
| `KvTxnHeartBeat` | 1 test | Requires `StartVersion`, `AdviseLockTtl` |
| `KvResolveLock` | 1 test | Requires `StartVersion`, `CommitVersion` |
| `RawPut/RawGet/RawDelete` (gRPC) | 3 tests | Already covered by CLI, but tests explicitly test gRPC layer |
| `RawBatchPut/RawBatchGet/RawScan` (gRPC) | 1 test | Same: testing gRPC layer specifically |
| `RawDeleteRange` (gRPC) | 1 test | Testing gRPC layer |
| `RawBatchScan` (gRPC) | 1 test | No CLI equivalent for multi-range batch scan |
| `RawGetKeyTTL` (gRPC) | 1 test | Already covered by CLI `TTL` |
| `RawCompareAndSwap` (gRPC) | 1 test | Already covered by CLI `CAS` |
| `RawChecksum` (gRPC) | 1 test | Already covered by CLI `CHECKSUM` |

**Total: 36 tests (48%) are fundamentally tied to raw gRPC or missing CLI commands** and should remain as library-based tests.

### 4.2 Addable to CLI

| Operation | Effort | Tests Unlocked |
|---|---|---|
| `BOOTSTRAP` | Medium | 11 tests across pd_server, pd_replication |
| `PUT STORE` | Low | 5 tests |
| `ALLOC ID` | Low | 4 tests |
| `GC SAFEPOINT SET <ts>` | Low | 2 tests |
| `STORE HEARTBEAT` | Medium | 1 test |
| `ASK SPLIT` + `REPORT SPLIT` | Medium | 3 tests |
| `IS BOOTSTRAPPED` | Low | 3 tests |
| **Total** | | +29 test operations covered |

### 4.3 Shell-Based Polling for WaitForCondition

`WaitForCondition` polls a function every 200ms until it returns true or times out. A shell equivalent:

```bash
# Wait for at least 2 regions
for i in $(seq 1 150); do  # 30s at 200ms
  count=$(gookv-cli -e "REGION LIST" | grep -c "^[0-9]")
  [ "$count" -ge 2 ] && break
  sleep 0.2
done
```

This is verbose but functional. For CLI-based tests, a helper shell function could wrap this pattern.

### 4.4 Process Management Gap

16 tests (21%) use `cluster.StopNode`, `cluster.RestartNode`, or `cluster.AddNode`. These are process management operations that are inherently outside the scope of a database CLI. CLI-based tests would need a separate test harness (shell script or Go test helper) for process lifecycle control.
