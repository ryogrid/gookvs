# Remaining Unimplemented and Incomplete Features in gookv

## 1. Overview

This document tracks features in the gookv codebase that are not yet fully implemented or remain partially complete. Items are verified against the Go source code.

As of 2026-03-28, nine rounds of feature completion have been performed, followed by a cross-region transactional client implementation. The first round (branch `feat/remaining-items-and-multiregion-e2e`) addressed 12 items, and the second round (branch `feature/lack-features-3`, commit `c52b215b7`) implemented 6 additional features. The features implemented in the first round were:

- **Async Commit / 1PC gRPC path** — `KvPrewrite` handler routes to 1PC or async commit paths based on request flags.
- **KvCheckSecondaryLocks** — Full handler with lock inspection and commit detection.
- **KvScanLock** — Iterates CF_LOCK with version filter and limit.
- **CLI `compact`** — Already implemented with `CompactAll()`/`CompactCF()` and `--flush-only` flag.
- **CLI `dump` SST parsing** — `--sst` flag for direct SST file reading via `pebble/sstable`.
- **Raw KV partial RPCs** — `RawBatchScan`, `RawGetKeyTTL` (with full TTL encoding), `RawCompareAndSwap` (TTL-aware), `RawChecksum` (CRC64 XOR).
- **TSO integration** — Server uses PD-allocated timestamps for 1PC commitTS and async commit maxCommitTS.
- **PD leader failover / retry** — Endpoint rotation, exponential backoff, reconnection on failure.
- **PD-coordinated split** — Split check ticker in peer, SplitCheckWorker wired in coordinator, AskBatchSplit/ExecBatchSplit/ReportBatchSplit flow.
- **Engine traits conformance tests** — 17 test cases covering WriteBatch, Snapshot, Iterator, cross-CF, concurrency.
- **Codec fuzz tests** — 6 fuzz targets for bytes and number codecs using `testing.F`.

Additionally, the **Client Library for Multi-Region Routing** has been implemented in `pkg/client/`:

- **PDStoreResolver** — Dynamic `storeID → gRPC address` resolution via PD, with TTL caching.
- **RegionCache** — Client-side `key → region → leader store` cache with sorted-slice binary search and error-driven invalidation.
- **RegionRequestSender** — gRPC connection pool with automatic retry on region errors (NotLeader, EpochNotMatch, etc.).
- **RawKVClient** — Full Raw KV API: Get, Put, PutWithTTL, Delete, GetKeyTTL, BatchGet, BatchPut, BatchDelete, Scan (cross-region), DeleteRange, CompareAndSwap, Checksum.
- **Server-side region validation** — `validateRegionContext()` added to 8 read-only Raw KV handlers for proper `region_error` propagation.
- **9 E2E tests** validating region routing, batch operations, scan across regions, and CAS.

The second round (`feature/lack-features-3`) implemented the following 6 features:

- **Snapshot transfer (end-to-end)** — `SnapWorker` wired via `SetSnapTaskCh`, `handleReady` applies snapshots via `storage.ApplySnapshot()`, `sendRaftMessage` detects `MsgSnap` and uses `SendSnapshot` streaming, gRPC `Snapshot` handler receives chunks, `HandleSnapshotMessage` attaches data and creates peers, `reportSnapshotStatus` feeds back to Raft.
- **Store goroutine** — `RunStoreWorker` started in `main.go`, `HandleRaftMessage` falls back to `storeCh` on `ErrRegionNotFound` for dynamic peer creation via the store worker.
- **Significant messages** — `handleSignificantMessage` now handles all three types: `Unreachable` (calls `rawNode.ReportUnreachable`), `SnapshotStatus` (calls `rawNode.ReportSnapshot`), and `MergeResult` (sets `stopped = true`).
- **GC safe point PD centralization** — `pdclient.Client` interface extended with `GetGCSafePoint` and `UpdateGCSafePoint` methods (15 methods total), `PDSafePointProvider` wraps the PD client, `KvGC` handler calls `UpdateGCSafePoint` after local GC.
- **KvDeleteRange** — `ModifyTypeDeleteRange` (value 2) and `EndKey` field added to `Modify` struct, `KvDeleteRange` gRPC handler implemented, `raftcmd` serialization supports delete-range operations.
- **PD scheduling** — `Scheduler` struct with `scheduleReplicaRepair` and `scheduleLeaderBalance` strategies, `MetadataStore` tracks store liveness (`storeLastHeartbeat`, `IsStoreAlive`, `GetDeadStores`, `GetLeaderCountPerStore`), `RegionHeartbeat` runs scheduler and returns commands, `PDWorker.handleSchedulingCommand` and `sendScheduleMsg` deliver commands to peers, `Peer.handleScheduleMessage` executes TransferLeader/ChangePeer/Merge.

A third implementation round (branch `cross-region-txn`, commit `616cacf16`) added the **cross-region transactional client (TxnClient)** in `pkg/client/`:

- **TxnKVClient** — Transactional KV API entry point: `Begin()` allocates a start timestamp from PD and returns a `TxnHandle`. Supports functional options (`WithPessimistic`, `WithAsyncCommit`, `With1PC`, `WithLockTTL`).
- **TxnHandle** — Per-transaction handle with `Get()`, `BatchGet()`, `Set()`, `Delete()`, `Commit()`, `Rollback()`. Buffers mutations in a local `membuf` and acquires pessimistic locks eagerly in pessimistic mode.
- **LockResolver** — Resolves stale locks encountered during reads by checking transaction status (`checkTxnStatus`) and committing or rolling back (`resolveLock`). Uses a channel-based `resolving` map for deduplication.
- **twoPhaseCommitter** — Executes the 2PC protocol: `selectPrimary` → prewrite (primary-first, secondaries parallel) → `getCommitTS` from PD → `commitPrimary` (sync) → `commitSecondaries` (sync, parallel). Supports 1PC and async commit paths.
- **Server-side enhancements** — `LockError` structured error type (in `mvcc` package) replaces bare `ErrKeyIsLocked`, enabling full `LockInfo` propagation in read RPCs via `lockToLockInfo()`. Multi-region Raft proposal routing via `proposeModifiesToRegionsWithRegionError()`. `BatchRollbackModifies()` for cluster-mode rollback.

A subsequent fix (branch `new-demo-impl`, commit `e09c358fe`) resolved 6 infrastructure bugs required for cross-region transactions to work end-to-end, and added a cross-region transaction demo (`make txn-demo-start/verify/stop` with `scripts/txn-demo-verify/main.go`):

- **SplitCheckCfg wiring** — `SplitCheckCfg` and `SplitCheckTickInterval` are now properly passed from TOML config through `main.go` to the coordinator, instead of falling back to hardcoded defaults.
- **PD metadata query for child peers** — `maybeCreatePeerForMessage` queries PD via `GetRegionByID()` for full region metadata when creating child peers after split, falling back to minimal metadata from the Raft message.
- **MVCC codec key decoding** — `groupModifiesByRegion()` now decodes MVCC codec-encoded keys (via `mvcc.DecodeKey`) before region routing, because modify keys use `EncodeLockKey`/`EncodeKey` encoding.
- **Narrowest-match region resolution** — `ResolveRegionForKey` selects the most specific region (largest startKey) among matches, handling stale parent regions after split.
- **Context RegionId in KvPrewrite/KvCommit** — Standard 2PC `KvPrewrite` and `KvCommit` use `req.GetContext().GetRegionId()` directly instead of multi-region grouping, since the client groups mutations by region.
- **Proposal timeout as retriable error** — `proposeErrorToRegionError()` treats timeout errors as `NotLeader`, enabling client retry.

A fifth implementation round (branch `feat/add-node`) added **dynamic node addition** — joining new KVS nodes to a running cluster via PD, with automatic region rebalancing:

| # | Feature | Status |
|---|---------|--------|
| 1 | Server-side PDStoreResolver (`internal/server/pd_resolver.go`) | Done |
| 2 | Join mode startup with store ID persistence (`internal/server/store_ident.go`) | Done |
| 3 | Store state machine — Up/Disconnected/Down/Tombstone (`internal/pd/server.go`) | Done |
| 4 | Region balance scheduler (`internal/pd/scheduler.go:scheduleRegionBalance`) | Done |
| 5 | Excess replica shedding scheduler (`internal/pd/scheduler.go:scheduleExcessReplicaShedding`) | Done |
| 6 | MoveTracker — 3-step region move protocol (`internal/pd/move_tracker.go`) | Done |
| 7 | Snapshot send semaphore — concurrent limit of 3 (`internal/server/coordinator.go`) | Done |
| 8 | gookv-ctl `store list` and `store status` commands (`cmd/gookv-ctl/main.go`) | Done |
| 9 | `GetAllStores` pdclient method (`pkg/pdclient/client.go`) | Done |
| 10 | E2E tests for node addition (`e2e/add_node_test.go`) | Done |

A sixth implementation round (branch `feat/pd-replication-design` + `master`, commits `5a7ece43e` through `fc690c0bb`) added **PD server Raft replication** — multi-node PD clusters with Raft consensus for high availability:

| # | Feature | Status |
|---|---------|--------|
| 1 | PDCommand encoding (12 command types, 1-byte type + JSON wire format) (`internal/pd/command.go`) | Done |
| 2 | PDRaftStorage (`raft.Storage` impl, Pebble CF_RAFT, entry cache) (`internal/pd/raft_storage.go`) | Done |
| 3 | PDRaftPeer (event loop, propose-and-wait, leader change, log GC) (`internal/pd/raft_peer.go`) | Done |
| 4 | Apply (12-command state machine dispatcher) (`internal/pd/apply.go`) | Done |
| 5 | Snapshot (full-state GenerateSnapshot/ApplySnapshot) (`internal/pd/snapshot.go`) | Done |
| 6 | PDTransport (lazy gRPC connection pool, raftpb/eraftpb conversion) (`internal/pd/transport.go`) | Done |
| 7 | PDPeerService (hand-coded gRPC for SendPDRaftMessage) (`internal/pd/peer_service.go`) | Done |
| 8 | Follower forwarding (7 unary + 2 streaming RPC proxy) (`internal/pd/forward.go`) | Done |
| 9 | TSOBuffer (batch 1000, Raft-amortized TSO allocation) (`internal/pd/tso_buffer.go`) | Done |
| 10 | IDBuffer (batch 100, Raft-amortized ID allocation) (`internal/pd/id_buffer.go`) | Done |
| 11 | PDServer Raft integration (initRaft, startRaft, replayRaftLog, 3-way routing) (`internal/pd/server.go`) | Done |
| 12 | gookv-pd CLI flags (--pd-id, --initial-cluster, --peer-port, --client-cluster) (`cmd/gookv-pd/main.go`) | Done |
| 13 | PD client leader discovery (discoverLeader, enhanced reconnect) (`pkg/pdclient/client.go`) | Done |
| 14 | Async commit prewrite routing fix (propose all to primary region) (`internal/server/server.go`) | Done |
| 15 | E2E test suite (16 PD replication tests) (`e2e/pd_replication_test.go`) | Done |

A seventh round (branch `txn-integrity-demo`, commits `e0913492b` through `7bda1abb9`) added a **transaction integrity demo** and several bug fixes for cross-region transaction reliability:

- **Transaction integrity demo** — 3-phase bank transfer stress test (`make txn-integrity-demo-start/verify/stop` with `scripts/txn-integrity-demo-verify/main.go`). Seeds 1000 accounts, runs concurrent transfers for 30 seconds, verifies total balance conservation.
- **CheckTxnStatusWithCleanup** — New write-capable variant of `CheckTxnStatus` with TTL-based expired lock cleanup and `RollbackIfNotExist` (`internal/storage/txn/actions.go`, `internal/server/storage.go`).
- **RC isolation level support** — `KvGet` handler now respects `IsolationLevel_RC` from request context, delegating to `Storage.GetWithIsolation()`.
- **RegionCache key encoding fix** — `LocateKey()` now encodes raw user keys via `codec.EncodeBytes()` before comparing with MVCC-encoded region boundaries.
- **groupModifiesByRegion routing fix** — Uses encoded modify keys directly for region routing instead of decoding back to raw keys.
- **Pessimistic Rollback fix** — `TxnHandle.Rollback()` now calls both `pessimisticRollback` and `batchRollback` in pessimistic mode to clean up prewrite locks from partial commits.
- **Synchronous secondary commits** — `commitSecondaries` changed from background goroutine to synchronous execution to prevent orphan locks.
- **Cleanup handler enhancement** — `Storage.Cleanup()` now checks the primary key's transaction status before committing or rolling back a secondary key's lock.

An eighth round (branch `external-e2e-tests`) added an **E2E test library** and migrated existing tests from internal-API-based to external-binary-based testing:

- **pkg/e2elib/** — PostgreSQL TAP-style test library for managing external gookv-server and gookv-pd processes:
  - `GokvNode` — manages a single gookv-server process (start/stop/restart, port allocation, log capture).
  - `PDNode` — manages a single gookv-pd process.
  - `GokvCluster` — orchestrates PD + N gookv-server nodes with auto-split configuration support.
  - `PDCluster` — manages multi-node PD clusters with Raft replication (`--initial-cluster`, `--pd-id`).
  - `PortAllocator` — file-based flock port allocation (range 10200–32767).
  - Test helpers: `WaitForCondition`, `WaitForRegionCount`, `WaitForStoreCount`, `WaitForRegionLeader`, `SeedAccounts`, `ReadAllBalances`, `DialTikvClient`.
- **e2e_external/** — 72 external-binary-based tests across 15 files, covering raw KV, transactions, async commit, cluster operations, multi-region routing, PD replication, region splits, add-node, and leader failover.
- **Standalone server mode** — `gookv-server` auto-detects standalone mode when no `--store-id`, `--pd-endpoints`, or `--initial-cluster` flags are provided. PD endpoint validation relaxed to allow empty endpoints.
- **Makefile** — `test-e2e-external` target (depends on `build`).

A ninth round (branch `apply-review-result`) applied fixes for a **comprehensive code review** covering all 77 identified issues (19 Critical, 25 High, 33 Medium) across the entire codebase:

**Storage/MVCC layer:**
- Backward scan iterator bound conditions fixed (`scanner.go`).
- PointGetter now skips pessimistic locks (invisible to readers, matching Scanner behavior).
- Async commit prewrite conflict check loops through `SeekBound*2` write records (matching regular Prewrite).
- `CleanupModifies` writes rollback record on primary check failure (prevents late commit).
- Latch `AcquireBlocking` with channel-based wake (replaces 21 spin-wait sites in `storage.go`).
- GC worker acquires latches per batch before writing; shared atomic command ID counter with Storage.
- GC removes orphaned Delete markers when no older Put versions exist.

**Client library:**
- Region cache evicts overlapping stale entries after split.
- `BatchGet` groups keys by region (was sending all to single region).
- `isPrimaryCommitted` returns error instead of swallowing.
- Deterministic primary key selection (lexicographic sort, cached).
- `DeleteRange` and `Checksum` span region boundaries.
- `commitSecondaries` batched by region with per-key fallback on `TxnLockNotFound`.
- `Scan` recomputes region bounds inside `SendToRegion` callback for fresh boundaries on retry.
- `LockResolver` resolving map periodically recreated to allow GC of old backing array.

**Server/RPC layer:**
- `KVPessimisticRollback` and `KvTxnHeartBeat` routed through Raft in cluster mode (were bypassing replication).
- Region validation (`validateRegionContext`) added to `RawPut`, `RawDelete`, `KVPessimisticRollback`, `KvTxnHeartBeat`.
- `KvDeleteRange` uses request region ID instead of hardcoded `1`.
- Long-lived gRPC stream per store for Raft transport (replaces per-message stream creation).
- Connection pool round-robin via atomic counter (was hardcoded index 0).
- Loopback routing resolves target peer's region ID via `FindRegionByPeerID`.
- `ReadPool.Stop()` drains pending tasks before returning.

**Raft layer:**
- `RecoverFromEngine` restores `ApplyState` from disk (was resetting to defaults on restart).
- `PersistApplyState` called in `handleReady` after committed entries (was never persisted).
- `InitialState` derives `ConfState` from region peer list (was returning empty).
- `ApplySnapshot` passes region start/end keys to clear stale data.
- Proposal callback redesign: monotonic proposal ID embedded in entry data, replaces `lastIdx+1` index tracking.
- Admin entries (ConfChange, SplitAdmin, CompactLog) filtered from `applyFunc` input.
- `failAllPendingProposals` on leader stepdown; `sweepStaleProposals` for timeout cleanup.
- `PeerMsgTypeDestroy` no longer closes Mailbox (prevents panic on concurrent sends).
- `leaseExpiry` stored as `atomic.Int64` (was unprotected `time.Time`).
- `handleReady` guarded by `stopped` flag check.
- `readEntriesFromEngine` returns error on entry gap.

**PD layer:**
- Atomic bootstrap in Raft mode (single `CmdSetBootstrapped` proposal with Store + Region).
- `ReportBatchSplit` sets Leader field in Raft mode proposals.
- FIFO proposal tracking replaces index-based map (`LastIndex()+1` bug).
- `PDSnapshot` includes `StoreLastHeartbeat` for state transfer.
- TSO overflow advances physical to `max(physical+1, now_ms)`.
- Scheduler excludes leader peer from excess replica shedding.
- `GCSafePointManager` uses `RWMutex` for reads.
- `MockClient` TSO protected by mutex.

**Entry/Config/Codec:**
- Coprocessor Float64 encoding uses `math.Float64bits`/`Float64frombits` (was reading `I64` field).
- `--store-id=0` with `--initial-cluster` validated as error.
- `SelectionExecutor` uses kind-aware `IsZeroValue()` truthiness check.
- `ReadableSize` parser uses `strconv.ParseUint` with validation.
- `parseInitialCluster` returns fatal error on malformed entries.
- Config validates `StatusAddr` non-empty.
- `EncodeRPNExpression` handles all constant types (Uint64, Float64, Bytes, Null).

**Infrastructure:**
- gRPC upgraded from v1.59.0 to v1.79.3; `grpc.Dial` migrated to `grpc.NewClient`.
- `ExecCommitMerge` dead code removed; error returned for non-adjacent regions.

See `review_results/` for detailed review reports and design documents.

## 2. Remaining Items

| # | Category | Feature | Status | Notes |
|---|----------|---------|--------|-------|
| 1 | gRPC / Coprocessor | BatchCoprocessor | Not implemented | Only `Coprocessor` and `CoprocessorStream` are wired. `BatchCoprocessor` remains a stub. |
| 2 | Client Library | TSO batching | Not implemented | Batch `GetTS` calls and dispense from local buffer. Low priority optimization. |
| 3 | Raftstore | Streaming snapshot generation | Not implemented | Current implementation holds all region data in memory; may OOM for large regions. |
| 4 | Raftstore | Region epoch validation in handleScheduleMessage | Not implemented | Currently relies on Raft's built-in rejection. |
| 5 | PD | Store heartbeat capacity fields | Not implemented | Capacity/Available/UsedSize not yet populated. |
| 6 | PD | PD Raft dynamic membership change | Not implemented | PD cluster topology is fixed at startup via `--initial-cluster`. Adding or removing PD nodes at runtime requires a full cluster restart with updated topology. |
| 7 | Transaction | Cross-region 2PC under high concurrency | **RESOLVED** | See 2.3. Fully functional after 10 cumulative fixes. |
| 8 | Raftstore | Split key selection from dominant CF | Deferred | `scanRegionSize` picks split key from whichever CF reaches the midpoint first. Low impact — splits may be slightly unbalanced but data integrity is unaffected. |

### 2.1 BatchCoprocessor

`BatchCoprocessor` is a server-streaming RPC that dispatches a coprocessor request across multiple regions in a single call. Only `Coprocessor` (unary) and `CoprocessorStream` (server-streaming, single region) are currently implemented. `BatchCoprocessor` falls through to `UnimplementedTikvServer`.

This is a low-priority item since the single-region `Coprocessor` and `CoprocessorStream` RPCs cover the core functionality. `BatchCoprocessor` would primarily be a performance optimization for multi-region queries.

### 2.2 PD Raft Dynamic Membership Change

The current PD Raft implementation uses a fixed cluster topology specified via `--initial-cluster` at startup. All PD nodes must be configured with the same initial cluster map. There is no mechanism for runtime PD node addition or removal (unlike KVS nodes, which support join mode via PD).

To change the PD cluster topology, all PD nodes must be stopped and restarted with updated `--initial-cluster` flags. This is acceptable for the typical 3-or-5-node PD deployment but prevents online PD scaling.

### 2.3 Cross-Region Transaction Concurrency — RESOLVED

Cross-region transaction integrity under high concurrency (32 workers, 1000 accounts) is now fully functional. The transaction integrity demo passes 3 consecutive runs with exact $100,000 balance conservation.

**Fixes applied (chronological order):**

1. **ReadIndex protocol** (ReadOnlySafe mode) — linearizable reads via Raft quorum confirmation. Includes no-op propose for `committedEntryInCurrentTerm`, `AppliedIndex` tracking in `handleReady`, `CancelPendingRead` for timeout cleanup, `ErrMailboxFull` retry, and batch mailbox drain.
2. **Region Epoch validation** — `validateRegionContext` checks epoch version + confVer on all RPC handlers.
3. **Per-key commitSecondaries** — each secondary committed individually via `SendToRegion` with retry. `TxnLockNotFound` retried 5 times before accepting as resolved.
4. **KvCommit / KvPrewrite key range validation** — `validateRegionContext` with `codec.EncodeBytes` encoding.
5. **LatchGuard pattern** — holds latch across Raft proposal to prevent stale snapshot reads.
6. **Split checker boundary encoding** — decodes MVCC keys and re-encodes as `EncodeLockKey` for consistent region boundaries.
7. **Propose-time epoch check** — `ProposeModifies` validates epoch (Version + ConfVer) before proposing. Returns `EpochNotMatch` on mismatch.
8. **isPrimaryCommitted check** — after `commitPrimary` error, checks via `KvCheckTxnStatus` whether primary was actually committed (Raft may commit but response lost). If committed, proceeds with secondaries.
9. **Split as Raft admin command** — region splits are proposed and committed through the Raft log (tag byte `0x02`), ensuring strict ordering between data entries and split operations. This eliminates the timing gap that was the fundamental root cause.
10. **propose() failure handling** — `Peer.propose()` no longer calls `cmd.Callback(nil)` when `rawNode.Propose()` fails. This was the final bug: silent success on propose failure caused prewrite locks to never be written, leading to `TxnLockNotFound` on commit.

**Test results:** 32 workers, 1000 accounts, 30s — 3/3 PASS ($100,000 exact).

See `design_doc/split_as_raft_admin/` and `design_doc/cross_region_2pc_integrity/` for detailed design documents.
