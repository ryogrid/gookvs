# PD Server Raft Replication: Test Plan

This document specifies the complete test plan for PD Raft replication. All test files, function names, scenarios, and assertions are defined precisely for implementation by a coding agent.

## Section 1: Unit Tests

All unit tests use `testify/assert` and `testify/require`. Engine instances use `rocks.Open(t.TempDir())` with `t.Cleanup()` for teardown.

---

### `internal/pd/command_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDCommand_MarshalRoundTrip` | Each of 11 command types serializes and deserializes correctly | Create one `PDCommand` per type with realistic payloads (e.g., `CmdPutStore` with `&metapb.Store{Id: 1, Address: "127.0.0.1:20160"}`) | `Marshal()` → `UnmarshalPDCommand()` produces struct equal to original for all 11 types |
| `TestPDCommand_InvalidType` | Unknown type byte in serialized data | Construct `[]byte{0xFF, '{', '}'}` | `UnmarshalPDCommand()` returns non-nil error |
| `TestPDCommand_EmptyPayload` | Command types that carry no payload fields | Create `PDCommand{Type: CmdCleanupStaleMove}` with zero-value fields | `Marshal()` succeeds, `UnmarshalPDCommand()` succeeds, does not panic |
| `TestPDCommand_LargePayload` | Command with large store metadata | Create `CmdPutStore` with a store containing 1000-char address string | Round-trip succeeds, address preserved exactly |
| `TestPDCommand_AllFieldsPreserved` | `CmdPutRegion` with both Region and Leader set | Set `Region` with 3 peers + `RegionEpoch`, set `Leader` | After round-trip, `Region.Peers` has length 3, `Leader.StoreId` matches |

---

### `internal/pd/raft_storage_test.go`

Each test creates a fresh RocksDB engine via `rocks.Open(t.TempDir())`.

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDRaftStorage_InitialState` | Fresh storage returns empty hard state | `NewPDRaftStorage(clusterID=1, engine)` | `InitialState()` returns zero `HardState`, empty `ConfState`, nil error |
| `TestPDRaftStorage_SaveReady` | Save entries + hard state | Create `raft.Ready` with 3 entries (indices 1-3, term 1) and `HardState{Term: 1, Vote: 1, Commit: 3}` | After `SaveReady()`: `LastIndex()` returns 3, `Term(2)` returns 1, `Entries(1, 4, math.MaxUint64)` returns 3 entries |
| `TestPDRaftStorage_RecoverFromEngine` | Persist, create new storage, recover | Save 5 entries via `SaveReady()`, create new `PDRaftStorage` on same engine, call `RecoverFromEngine()` | `LastIndex()` == 5, `Entries(1, 6, math.MaxUint64)` returns 5 entries, `InitialState()` returns saved hard state |
| `TestPDRaftStorage_Entries` | Request range from cache and engine | Save 10 entries, compact cache to index 5 (keep only 6-10 in cache) | `Entries(1, 6, math.MaxUint64)` reads from engine (indices 1-5), `Entries(6, 11, math.MaxUint64)` reads from cache (indices 6-10) |
| `TestPDRaftStorage_EntriesSizeLimit` | Size-limited entry reads | Save 10 entries each ~100 bytes | `Entries(1, 11, 250)` returns <= 3 entries (size capped) |
| `TestPDRaftStorage_Term` | Query term at various indices | Save entries at indices 1-5 with terms [1,1,2,2,3] | `Term(1)` == 1, `Term(3)` == 2, `Term(5)` == 3 |
| `TestPDRaftStorage_TermCompacted` | Query term below truncated index | Save entries, set `TruncatedIndex = 3, TruncatedTerm = 2` | `Term(2)` returns `raft.ErrCompacted`, `Term(3)` returns 2 (truncated term) |
| `TestPDRaftStorage_FirstLastIndex` | First and last index tracking | Save entries 1-5, then set `TruncatedIndex = 2` | `FirstIndex()` == 3 (truncated+1), `LastIndex()` == 5 |
| `TestPDRaftStorage_Snapshot` | Snapshot returns metadata-only | Save entries, set apply state | `Snapshot()` returns snapshot with `Metadata.Index == AppliedIndex` |
| `TestPDRaftStorage_DummyEntry` | Bootstrap with dummy entry | `SetDummyEntry()` | `entries[0]` has Index=0, Term=0; `LastIndex()` == 0 |

---

### `internal/pd/raft_peer_test.go`

These tests create 3 in-process `PDRaftPeer` instances connected via direct function calls (no gRPC, no network).

Helper function:
```go
func createTestPDRaftCluster(t *testing.T, nodeCount int) ([]*PDRaftPeer, context.CancelFunc)
```
- Creates `nodeCount` peers with in-memory RocksDB engines
- Wires `sendFunc` on each peer to deliver messages to the target peer's `Mailbox`
- Starts `Run()` goroutines
- Returns peers and cancel function for cleanup

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDRaftPeer_LeaderElection` | 3 in-process peers elect a leader | `createTestPDRaftCluster(t, 3)` | Within 3 seconds, exactly one peer reports `IsLeader() == true`; the other two report `IsLeader() == false` |
| `TestPDRaftPeer_ProposeAndApply` | Propose command on leader | Find leader peer, `ProposeAndWait(ctx, CmdPutStore{...})` | Returns nil error; apply function was called with the correct `PDCommand`; apply result bytes are returned to caller |
| `TestPDRaftPeer_ProposeOnFollower` | Propose on follower | Find follower peer, `ProposeAndWait(ctx, CmdIDAlloc{})` | Returns `ErrNotLeader` |
| `TestPDRaftPeer_MultipleProposals` | 10 sequential proposals on leader | Find leader, propose 10 `CmdIDAlloc` commands | All 10 return without error; each returns a unique result |
| `TestPDRaftPeer_LeaderChange` | Leader steps down, new leader elected | Find leader, stop it (cancel its context), wait for new leader | New leader elected within 3 seconds; proposals on new leader succeed |
| `TestPDRaftPeer_ConcurrentProposals` | 50 concurrent proposals from goroutines | Launch 50 goroutines each proposing `CmdIDAlloc` on leader | All 50 complete without error; all results are unique |

---

### `internal/pd/apply_test.go`

Each test creates a `PDServer` in single-node mode (no Raft) and calls `applyCommand` directly.

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestApplyCommand_SetBootstrapped` | Apply `CmdSetBootstrapped(true)` | Fresh PDServer | `s.meta.IsBootstrapped()` returns `true` |
| `TestApplyCommand_PutStore` | Apply `CmdPutStore` | Fresh PDServer | `s.meta.GetStore(storeID)` returns the store with correct address |
| `TestApplyCommand_PutRegion` | Apply `CmdPutRegion` with leader | Fresh PDServer | `s.meta.GetRegionByID(regionID)` returns region and leader |
| `TestApplyCommand_UpdateStoreStats` | Apply `CmdUpdateStoreStats` | PDServer with store registered | A `GetStoreStats(storeID)` accessor needs to be added to `MetadataStore` (or use the `storeStats` map directly in tests); returns updated stats |
| `TestApplyCommand_SetStoreState` | Apply `CmdSetStoreState(Tombstone)` | PDServer with store registered | Store state is `StoreStateTombstone` |
| `TestApplyCommand_TSOAllocate` | Apply `CmdTSOAllocate(100)` | Fresh PDServer | Returns non-nil bytes that decode to valid `pdpb.Timestamp` with Physical > 0 |
| `TestApplyCommand_IDAlloc` | Apply `CmdIDAlloc` | Fresh PDServer | Returns 8 bytes decoding to a non-zero uint64; second call returns higher value |
| `TestApplyCommand_UpdateGCSafePoint` | Apply `CmdUpdateGCSafePoint(1000)` | Fresh PDServer | Returns 8 bytes decoding to 1000; `s.gcMgr.GetSafePoint()` == 1000 |
| `TestApplyCommand_StartMove` | Apply `CmdStartMove` | PDServer with region | `s.moveTracker.HasPendingMove(regionID)` returns `true` |
| `TestApplyCommand_AdvanceMove` | Apply `CmdAdvanceMove` | PDServer with active move | Move state is advanced |
| `TestApplyCommand_CleanupStaleMove` | Apply `CmdCleanupStaleMove` | PDServer with stale move (old StartedAt) | Stale move is removed |
| `TestApplyCommand_AllTypes` | Apply all 11 types sequentially | Fresh PDServer | No panics; each type modifies state correctly |

---

### `internal/pd/snapshot_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDSnapshot_GenerateAndApply` | Full round-trip | Populate PDServer with: 2 stores, 3 regions with leaders, store stats, GC safe point = 500, 1 pending move | `GenerateSnapshot()` succeeds; create fresh PDServer; `ApplySnapshot(data)` succeeds; verify: `GetStore(1)` matches, `GetRegionByID(1)` matches, `GetSafePoint()` == 500, pending move exists |
| `TestPDSnapshot_EmptyState` | Snapshot of empty PD | Fresh PDServer (no bootstrap, no stores) | `GenerateSnapshot()` succeeds; `ApplySnapshot()` on fresh PDServer succeeds; `IsBootstrapped()` == false; store count == 0 |
| `TestPDSnapshot_TSOState` | TSO state preserved | Allocate 100 timestamps, snapshot, apply to fresh PDServer, allocate 1 more | New timestamp is > all 100 previous timestamps (TSO physical clock preserved) |
| `TestPDSnapshot_IDAllocState` | ID allocator state preserved | Allocate 10 IDs (last ID = N), snapshot, apply, allocate 1 more | New ID > N |
| `TestPDSnapshot_LargeState` | Snapshot with many regions | Register 1000 regions with leaders | Snapshot + apply round-trip preserves all 1000 regions |

---

### `internal/pd/tso_buffer_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestTSOBuffer_AllocateFromBuffer` | Pre-fill buffer, allocate | Create `TSOBuffer` with mock raft peer, pre-fill buffer via initial Raft proposal | Returns monotonically increasing timestamps |
| `TestTSOBuffer_RefillOnDepletion` | Deplete buffer | Create `TSOBuffer`, exhaust all pre-allocated timestamps | Calls Raft propose for new batch; subsequent allocation succeeds |
| `TestTSOBuffer_LeaderChange` | Reset buffer | Create `TSOBuffer`, allocate some timestamps, simulate leader change (reset buffer) | Next allocation starts fresh batch via new Raft proposal |

---

### `internal/pd/id_buffer_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestIDBuffer_AllocateFromBuffer` | Pre-fill buffer, allocate | Create `IDBuffer` with mock raft peer, pre-fill buffer via initial Raft proposal | Returns sequential IDs |
| `TestIDBuffer_RefillOnDepletion` | Deplete buffer | Create `IDBuffer`, exhaust all pre-allocated IDs | Calls Raft propose for new batch; subsequent allocation succeeds |

---

### `internal/pd/transport_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDTransport_SendMessage` | Send message to peer | Start mock gRPC server with `PDPeerService`, create `PDTransport` | `Send(peerID, raftpb.Message{...})` returns nil error; mock server received the message |
| `TestPDTransport_LazyConnection` | Connection created on first send | Create `PDTransport` with 3 peer addresses | No connections initially; after `Send(1, ...)`, exactly 1 connection exists |
| `TestPDTransport_ConnectionError` | Send to unreachable peer | Create `PDTransport` with invalid address | `Send()` returns non-nil error; does not panic |
| `TestPDTransport_Close` | Close cleans up connections | Create transport, send to 2 peers, `Close()` | No goroutine leaks (verified by `goleak` or connection count) |

---

### `internal/pd/peer_service_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestPDPeerService_ReceiveMessage` | Handler delivers message to peer mailbox | Create `PDRaftPeer`, register `PDPeerService` on gRPC server, send message via client | Message appears in `peer.Mailbox` with correct `Type` and converted `raftpb.Message` |
| `TestPDPeerService_InvalidMessage` | Malformed message | Send `RaftMessage` with nil `Message` field | Handler does not panic; returns appropriate error or ignores |

---

### `internal/pd/forward_test.go`

| Test Function | Scenario | Setup | Assertions |
|---|---|---|---|
| `TestForward_GetLeaderClient` | Resolves leader client connection | Start 3-PD cluster, call `getLeaderClient()` from a follower | Returns non-nil `pdpb.PDClient`; calling `GetMembers` on it succeeds |
| `TestForward_PutStoreViaFollower` | PutStore forwarded to leader | Start 3-PD cluster, call `PutStore` on a follower | Returns success; `GetStore` on all 3 nodes returns the store |
| `TestForward_NoLeader` | Forward when no leader elected | Create PDServer with raftPeer but leaderID==0 | `forwardPutStore()` returns error containing "no leader" |

---

## Section 2: E2E Tests

**File**: `e2e/pd_replication_test.go`

### Helper Functions

```go
// startPDCluster starts a multi-node PD cluster for testing.
// Each node gets a random client port and a random peer port.
// Returns the servers and a slice of client addresses.
func startPDCluster(t *testing.T, nodeCount int) ([]*pd.PDServer, []string)
```
Implementation:
1. Allocate `nodeCount` random TCP ports for client and peer listeners (use `net.Listen("tcp", "127.0.0.1:0")`, get port, close listener)
2. Build `InitialCluster` map from allocated peer ports
3. Build `ClientAddrs` map from allocated client ports
4. Create `PDServerConfig` for each node with `RaftConfig` set
5. Call `NewPDServer()` and `Start()` for each
6. Register `t.Cleanup()` to call `Stop()` on all servers in reverse order
7. Return servers and client address list

```go
// newPDClusterClient creates a PD client connected to all endpoints of a PD cluster.
func newPDClusterClient(t *testing.T, addrs []string) pdclient.Client
```
Implementation:
1. Create `pdclient.Config{Endpoints: addrs, RetryInterval: 200ms, RetryMaxCount: 20}`
2. Call `pdclient.NewClient()` with 10-second timeout
3. Register `t.Cleanup()` to call `Close()`

```go
// waitForLeader polls GetMembers until a leader is reported, with timeout.
func waitForLeader(t *testing.T, client pdclient.Client, timeout time.Duration) *pdpb.Member
```

```go
// findLeaderIndex returns the index of the leader server in the servers slice.
func findLeaderIndex(t *testing.T, servers []*pd.PDServer) int
```

---

### E2E Test Table

| Test Function | Scenario | Steps | Assertions |
|---|---|---|---|
| `TestPDReplication_Bootstrap` | 3-PD cluster bootstrap | 1. `startPDCluster(t, 3)` 2. `newPDClusterClient(t, addrs)` 3. Wait for leader 4. `client.Bootstrap(ctx, store, region)` 5. Query `IsBootstrapped` from each node | `IsBootstrapped` returns `true` on all 3 nodes |
| `TestPDReplication_LeaderElection` | Leader election converges | 1. `startPDCluster(t, 3)` 2. Create 3 separate PD clients (one per node) 3. Wait up to 5s | All 3 `GetMembers` responses report the same `Leader.MemberId`; exactly one server's `raftPeer.IsLeader()` is true |
| `TestPDReplication_WriteForwarding` | Write on follower forwarded to leader | 1. Start 3-PD cluster, bootstrap 2. Find a follower address 3. Create client connected ONLY to follower 4. `client.PutStore(ctx, store2)` | `PutStore` succeeds (no error); `GetStore(store2.Id)` on all 3 nodes returns `store2` |
| `TestPDReplication_TSOMonotonicity` | TSO monotonically increasing across nodes | 1. Start 3-PD cluster, bootstrap 2. Create client with all 3 endpoints 3. Call `GetTS()` 100 times | All 100 timestamps are strictly increasing: `ts[i].ToUint64() < ts[i+1].ToUint64()` for all i |
| `TestPDReplication_IDAllocMonotonicity` | ID allocation unique and increasing | 1. Start 3-PD cluster, bootstrap 2. Call `AllocID()` 50 times | All 50 IDs are unique; IDs are strictly increasing |
| `TestPDReplication_LeaderFailover` | Leader failure triggers re-election | 1. Start 3-PD cluster, bootstrap, write store1 2. Find leader, call `leader.Stop()` 3. Wait up to 10s for new leader via remaining 2 endpoints 4. `client.PutStore(ctx, store2)` via surviving nodes 5. `client.GetStore(ctx, store2.Id)` | New leader elected (different `MemberId`); `PutStore` succeeds; `GetStore` returns `store2` on surviving nodes |
| `TestPDReplication_SingleNodeCompat` | Single-node backward compatibility | 1. `startPDServer(t)` (existing helper in `e2e/pd_server_test.go`, no `--initial-cluster`) 2. Run same operations as `TestPDServerBootstrapAndTSO` | Bootstrap succeeds; TSO is monotonic; AllocID is increasing; identical behavior to existing single-PD tests |
| `TestPDReplication_CatchUpRecovery` | Node catches up via Raft log replay | 1. Start 3-PD cluster, bootstrap 2. Write 5 stores 3. Stop node 3 4. Write 5 more stores 5. Restart node 3 (create new PDServer with same config + data dir) 6. Wait for node 3 to catch up via log replay | `GetAllStores()` on node 3 returns all 10 stores after catch-up (poll with 10s timeout). Note: this tests log replay catch-up, not snapshot transfer. To test snapshot transfer, trigger log compaction on the leader before restarting node 3 so a real snapshot transfer is required. |
| `TestPDReplication_RegionHeartbeat` | Region heartbeat via replicated PD | 1. Start 3-PD cluster, bootstrap 2. Create `pdclient.Client` with all endpoints 3. `ReportRegionHeartbeat` with region + leader | Heartbeat succeeds (no error); `GetRegionByID` returns the region on all nodes |
| `TestPDReplication_GCSafePoint` | GC safe point replication | 1. Start 3-PD cluster, bootstrap 2. `client.UpdateGCSafePoint(ctx, 1000)` 3. Query `GetGCSafePoint` on each node individually | All 3 nodes return `SafePoint == 1000` |
| `TestPDReplication_AskBatchSplit` | Split ID allocation via replicated PD | 1. Start 3-PD cluster, bootstrap with region 2. `client.AskBatchSplit(ctx, region, 2)` | Returns 2 `SplitID` entries; all `NewRegionId` values are unique; all `NewPeerIds` are unique |
| `TestPDReplication_ConcurrentWritesFromMultipleClients` | Concurrent writes from 3 clients | 1. Start 3-PD cluster, bootstrap 2. Create 3 clients (one per endpoint) 3. Each client concurrently does 10 `AllocID` calls | All 30 IDs are unique (no duplicates across clients) |
| `TestPDReplication_TSOViaFollower` | TSO via follower | 1. Start 3-PD cluster, bootstrap 2. Create client with only follower endpoint 3. Call `GetTS()` 10 times | All timestamps strictly increasing (forwarding works) |
| `TestPDReplication_5NodeCluster` | 5-node cluster operates correctly | 1. `startPDCluster(t, 5)` 2. Bootstrap, PutStore, GetTS | Leader elected; PutStore succeeds; TSO works; GetStore returns on all 5 nodes |

---

## Section 3: Existing Test Compatibility

All existing PD-related E2E tests must continue to pass unchanged after implementation. These tests use `startPDServer(t)` (defined in `e2e/pd_server_test.go`) which creates a single-PD server with `cfg.RaftConfig = nil`, exercising the backward-compatible single-node code path.

### Tests that must pass without modification

**`e2e/pd_server_test.go`**:
| Test | What It Tests |
|---|---|
| `TestPDServerBootstrapAndTSO` | Bootstrap, TSO allocation, AllocID |
| `TestPDServerStoreAndRegionMetadata` | PutStore, GetStore, GetAllStores, GetRegion, GetRegionByID |
| `TestPDAskBatchSplitAndReport` | AskBatchSplit, ReportBatchSplit |
| `TestPDStoreHeartbeat` | StoreHeartbeat, store stats |

**`e2e/pd_cluster_integration_test.go`**:
| Test | What It Tests |
|---|---|
| `TestPDClusterStoreAndRegionHeartbeat` | Full heartbeat loop with PDWorker |
| `TestPDClusterTSOForTransactions` | TSO allocation used by transaction layer |
| `TestPDClusterGCSafePoint` | GC safe point via PDWorker |

**`e2e/pd_leader_discovery_test.go`**:
| Test | What It Tests |
|---|---|
| `TestPDStoreRegistration` | Store registration via PD |
| `TestPDRegionLeaderTracking` | Region leader tracking via heartbeats |
| `TestPDLeaderFailover` | Multi-node KVS cluster with PD (single PD node) |

### Verification command

```bash
# All existing tests pass:
go test ./e2e/... -v -count=1 -timeout 120s

# All unit tests pass:
go test ./internal/pd/... -v -count=1

# Specific PD tests:
go test ./e2e/... -run "TestPDServer|TestPDCluster|TestPDStore|TestPDRegion|TestPDLeader" -v -count=1
```

---

## Section 4: Test Infrastructure Requirements

### Engine for Unit Tests

Unit tests in `internal/pd/` that need a `KvEngine` should use RocksDB:
```go
engine, err := rocks.Open(t.TempDir())
require.NoError(t, err)
t.Cleanup(func() { engine.Close() })
```
Import: `github.com/ryogrid/gookv/internal/engine/rocks`

### Port Allocation for E2E Tests

Use ephemeral port allocation to avoid port conflicts:
```go
lis, err := net.Listen("tcp", "127.0.0.1:0")
require.NoError(t, err)
port := lis.Addr().(*net.TCPAddr).Port
lis.Close()
```

### Timeouts

| Context | Timeout | Rationale |
|---|---|---|
| Leader election wait | 5 seconds | Election timeout is ~1s (10 ticks * 100ms). Allow 5x margin. |
| Proposal completion | 3 seconds | Raft commit should complete in < 500ms for 3-node in-process cluster. |
| State convergence (follower catch-up) | 10 seconds | Snapshot transfer may take a few seconds. |
| E2E test overall | 120 seconds | Matches existing `Makefile` test-e2e timeout. |

### Test Isolation

- Each test creates its own PD cluster with fresh temp directories.
- No shared state between tests.
- All cleanup via `t.Cleanup()` (no `defer` in test helpers).
- Tests that stop/restart servers must use fresh `PDServer` instances pointing at the same `DataDir` to simulate process restart.

---

## Section 5: Test Execution Order and CI

### Recommended execution order during development

1. **Phase 1 unit tests first** (fast feedback):
   ```bash
   go test ./internal/pd/ -run "TestPDCommand|TestPDRaftStorage|TestApplyCommand" -v
   ```

2. **Phase 1 integration** (in-process Raft cluster):
   ```bash
   go test ./internal/pd/ -run "TestPDRaftPeer" -v
   ```

3. **Phase 2 transport tests**:
   ```bash
   go test ./internal/pd/ -run "TestPDTransport|TestPDPeerService" -v
   ```

4. **Phase 3 forwarding tests**:
   ```bash
   go test ./internal/pd/ -run "TestForward" -v
   ```

5. **Backward compatibility check** (must pass after each phase):
   ```bash
   go test ./e2e/... -run "TestPDServer|TestPDCluster|TestPDStore|TestPDRegion|TestPDLeader" -v -count=1
   ```

6. **Full E2E replication tests** (Phase 3+ complete):
   ```bash
   go test ./e2e/... -run "TestPDReplication" -v -count=1 -timeout 120s
   ```

7. **Full test suite** (final validation):
   ```bash
   go test ./pkg/... ./internal/... -v -count=1
   go test ./e2e/... -v -count=1 -timeout 120s
   ```

### CI gate criteria

All of the following must pass for the implementation to be considered complete:
- `go test ./internal/pd/... -v -count=1` (0 failures)
- `go test ./e2e/... -v -count=1 -timeout 120s` (0 failures)
- `go vet ./internal/pd/...` (0 warnings)
- No data races: `go test ./internal/pd/... -race -count=1`
- No data races in E2E: `go test ./e2e/... -race -count=1 -timeout 180s`
