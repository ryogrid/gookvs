# Remaining Unimplemented and Incomplete Features in gookv

## 1. Overview

This document tracks features in the gookv codebase that are not yet fully implemented or remain partially complete. Items are verified against the Go source code.

As of 2026-03-20, two rounds of feature completion have been performed, followed by a cross-region transactional client implementation. The first round (branch `feat/remaining-items-and-multiregion-e2e`) addressed 12 items, and the second round (branch `feature/lack-features-3`, commit `c52b215b7`) implemented 6 additional features. The features implemented in the first round were:

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

## 2. Remaining Items

| # | Category | Feature | Status | Notes |
|---|----------|---------|--------|-------|
| 1 | gRPC / Coprocessor | BatchCoprocessor | Not implemented | Only `Coprocessor` and `CoprocessorStream` are wired. `BatchCoprocessor` remains a stub. |
| 2 | Client Library | TSO batching | Not implemented | Batch `GetTS` calls and dispense from local buffer. Low priority optimization. |
| 3 | Raftstore | Streaming snapshot generation | Not implemented | Current implementation holds all region data in memory; may OOM for large regions. |
| 4 | Raftstore | Region epoch validation in handleScheduleMessage | Not implemented | Currently relies on Raft's built-in rejection. |
| 5 | PD | Store heartbeat capacity fields | Not implemented | Capacity/Available/UsedSize not yet populated. |
| 6 | PD | PD Raft dynamic membership change | Not implemented | PD cluster topology is fixed at startup via `--initial-cluster`. Adding or removing PD nodes at runtime requires a full cluster restart with updated topology. |
| 7 | Transaction | Cross-region 2PC under high concurrency | Known limitation | See 2.3 |

### 2.1 BatchCoprocessor

`BatchCoprocessor` is a server-streaming RPC that dispatches a coprocessor request across multiple regions in a single call. Only `Coprocessor` (unary) and `CoprocessorStream` (server-streaming, single region) are currently implemented. `BatchCoprocessor` falls through to `UnimplementedTikvServer`.

This is a low-priority item since the single-region `Coprocessor` and `CoprocessorStream` RPCs cover the core functionality. `BatchCoprocessor` would primarily be a performance optimization for multi-region queries.

### 2.2 PD Raft Dynamic Membership Change

The current PD Raft implementation uses a fixed cluster topology specified via `--initial-cluster` at startup. All PD nodes must be configured with the same initial cluster map. There is no mechanism for runtime PD node addition or removal (unlike KVS nodes, which support join mode via PD).

To change the PD cluster topology, all PD nodes must be stopped and restarted with updated `--initial-cluster` flags. This is acceptable for the typical 3-or-5-node PD deployment but prevents online PD scaling.

### 2.3 Cross-Region Transaction Concurrency Limitation

At high concurrency (32 concurrent workers), cross-region transactions show $50-$100 balance divergence in the bank transfer demo despite extensive fixes across 8 rounds.

**What has been fixed (all implemented and verified):**

1. **ReadIndex protocol** (ReadOnlySafe mode) — linearizable reads via Raft quorum confirmation. Includes no-op propose for `committedEntryInCurrentTerm`, `AppliedIndex` tracking in `handleReady`, `CancelPendingRead` for timeout cleanup, `ErrMailboxFull` retry, and batch mailbox drain.
2. **Region Epoch validation** — `validateRegionContext` checks epoch version + confVer on all RPC handlers. Returns `EpochNotMatch` with current region metadata.
3. **Apply-level key range filtering** — `applyEntriesForPeer` filters out-of-range modify keys per-key (CF-aware: `DecodeLockKey` for CF_LOCK, `DecodeKey` for CF_WRITE/CF_DEFAULT). Matches TiKV's `check_key_in_region` in `handle_put`/`handle_delete`.
4. **Per-key commitSecondaries** — each secondary committed individually via `SendToRegion` with retry. `TxnLockNotFound` retried 5 times before accepting as resolved.
5. **KvCommit key range validation** — `validateRegionContext` checks key against region boundaries with `codec.EncodeBytes` encoding.
6. **KvPrewrite per-key validation** — defense-in-depth: validates all mutation keys belong to current region when `RegionId` is set.
7. **KvPrewrite region routing fix** — uses `mutations[0].Key` instead of primary lock key for `resolveRegionID` (primary may be in a different region for secondary prewrites).
8. **Leader Lease** — implemented but disabled. The lease confirms leadership but does not guarantee `appliedIndex >= commitIndex`, so `ReadOnlySafe` is used instead.
9. **LatchGuard pattern** — holds latch across Raft proposal to prevent stale snapshot reads.
10. **Split checker boundary encoding** — decodes MVCC keys and re-encodes as `EncodeLockKey` for consistent region boundaries.

**Remaining root cause: missing propose-time epoch check**

TiKV uses `CmdEpochChecker` to reject stale-epoch proposals BEFORE they enter the Raft log (`tikv/components/raftstore/src/store/peer.rs:4723-4731`). gookv checks epoch at the RPC level (`validateRegionContext`) but NOT at Raft propose time. Between the RPC validation and the Raft proposal, a split can change region boundaries, allowing a prewrite to be proposed to a region that no longer owns the key.

Additionally, TiKV has store-wide latches + shared engine snapshot that serialize prewrites for the same key regardless of region. gookv also has these (`latch.New(2048)` in `storage.go`, `engine.NewSnapshot()` shared across regions), so same-key conflict detection works correctly within a single store. The issue arises when two stores each have a different region's leader for the same key range during a split transition.

See `design_doc/cross_region_2pc_integrity/` for the detailed design document addressing this issue.
