# gookvs Implementation Progress

## IMPL-001 Proto generation - DONE
- [x] Reuse kvproto via github.com/pingcap/kvproto Go module dependency
- [x] Copy essential .proto files to proto/ for reference
- [x] Verify all key packages importable (kvrpcpb, metapb, errorpb, eraftpb, raft_serverpb, raft_cmdpb, pdpb, tikvpb, coprocessor, debugpb, diagnosticspb)
- [x] Proto serialization round-trip verified

## IMPL-002 pkg/codec: memcomparable byte encoding - DONE
- [x] Port TiKV codec tests to Go (byte.rs, number.rs)
- [x] Implement EncodeBytes/DecodeBytes (ascending + descending)
- [x] Implement number encoding (uint64, int64, float64) with ordering guarantees
- [x] Implement VarInt (unsigned + signed)
- [x] Ordering tests verified

## IMPL-003 pkg/keys: user key / internal key encoding - DONE
- [x] Implement DataKey/OriginKey
- [x] Implement RaftLogKey, RaftStateKey, ApplyStateKey, RegionStateKey
- [x] Implement RaftLogKeyRange, RegionIDFromRaftKey, RegionIDFromMetaKey
- [x] Key ordering tests verified

## IMPL-004 pkg/cfnames: column family constants - DONE
- [x] Define CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT
- [x] Define DataCFs, AllCFs slices

## IMPL-005 pkg/txntypes: Lock, Write, Mutation structs - DONE
- [x] Implement Lock struct with full TiKV-compatible binary serialization
- [x] Implement Write struct with full TiKV-compatible binary serialization
- [x] Implement Mutation, TimeStamp types
- [x] All optional field tags match TiKV format
- [x] Marshal/Unmarshal round-trip tests passing

## IMPL-006 internal/engine/traits: KvEngine interfaces - DONE
- [x] Define KvEngine, Snapshot, WriteBatch, Iterator interfaces
- [x] Define IterOptions, error types
- [x] Support 4 column families via string-based CF parameter

## IMPL-007 internal/engine/rocks: RocksDB backend - DONE
- [x] Implement Engine (KvEngine) using Pebble as pure-Go backend (grocksdb requires CGo/RocksDB C lib not available)
- [x] Column family emulation via key prefixing (4 CFs: default, lock, write, raft)
- [x] Implement Snapshot with read isolation
- [x] Implement WriteBatch with atomic commit
- [x] Implement Iterator with Seek, SeekForPrev, bounds support
- [x] CF isolation verified (keys in one CF invisible to others)
- [x] Persistence verified (data survives close/reopen)
- [x] 36 tests passing (CRUD, CF isolation, snapshot, batch, iterator, bounds, bulk)

## IMPL-008 pkg/pdclient: PD client - DONE
- [x] Define Client interface (GetTS, GetRegion, Bootstrap, heartbeats, split, AllocID)
- [x] Implement real gRPC client (grpcClient) connecting to PD endpoints
- [x] Implement TSO timestamp allocation with encoding (physical<<18|logical)
- [x] Implement MockClient for testing (in-memory TSO, region registry, store registry)
- [x] TSO monotonicity and uniqueness verified under concurrent access
- [x] Bootstrap, store/region CRUD, heartbeat, batch split operations tested
- [x] 16 tests passing

## IMPL-009 internal/raftstore: Raft consensus and region management - DONE
- [x] 9a: Router - sync.Map-based message dispatch (14 tests)
  - Register/Unregister/Send/Broadcast/SendStore
  - Concurrent safety verified
  - ErrRegionNotFound, ErrMailboxFull error handling
- [x] 9b: Core peer loop - single-region propose/apply via etcd/raft
  - PeerStorage implements raft.Storage (persistent log, cache, hard state)
  - Peer goroutine with mailbox, tick, Ready handling
  - Bootstrap, election, proposal, apply pipeline working
  - Conf change entries properly applied via ApplyConfChange
  - 20 tests passing (14 router + 6 peer/storage)
- [x] Message types defined (PeerMsg, StoreMsg, SignificantMsg, RaftCommand, ApplyResult)
- [x] etcd/raft v3.5.17 integration (RawNode, ConfChange, PreVote, CheckQuorum)
- Note: Snapshot (9c) and Region Split (9d) are deferred to when needed by higher layers

## IMPL-010 internal/storage/mvcc: MVCC reader/writer - DONE
- [x] MVCC key encoding: EncodeKey/DecodeKey with descending timestamp for correct ordering
- [x] MvccTxn: write accumulator (PutLock, UnlockKey, PutValue, DeleteValue, PutWrite, DeleteWrite)
- [x] MvccReader: LoadLock, SeekWrite, GetWrite (with Rollback skipping), GetValue, GetTxnCommitRecord
- [x] PointGetter: optimized single-key MVCC reads with lock checking and bypass
- [x] SI and RC isolation levels supported
- [x] Multiple version reads verified (reads correct version based on ts)
- [x] 20 tests passing

## IMPL-011 internal/storage/txn: scheduler, latch, concurrency, actions - DONE
- [x] 11a: Latch system - deadlock-free key serialization (11 tests)
  - Hash-based slot locking with sorted acquisition order
  - Wait queue for blocked commands
  - Concurrent deadlock-free verified
- [x] 11b: ConcurrencyManager - in-memory lock table + max_ts tracking (7 tests)
  - Atomic max_ts updates
  - Key locking/unlocking with guards
  - GlobalMinLock computation
- [x] 11c: Percolator actions (15 tests)
  - Prewrite: lock writing, conflict detection, short value inlining, large value support
  - Commit: lock removal, write record creation
  - Rollback: lock cleanup, rollback record, idempotency
  - CheckTxnStatus: locked/committed/rolledback detection
  - End-to-end: Prewrite -> Commit -> Read verified
- [x] 33 total tests passing

## IMPL-012 internal/server: gRPC server and KV service - DONE
- [x] Storage layer bridging engine, MVCC, and transaction layers
  - Get, Scan, BatchGet (read path via PointGetter + MvccReader)
  - Prewrite, Commit, BatchRollback, Cleanup, CheckTxnStatus (write path via latches + MvccTxn)
  - Atomic batch writes via WriteBatch
- [x] gRPC server implementing tikvpb.TikvServer (kvproto generated stubs)
  - KvGet, KvScan, KvPrewrite, KvCommit, KvBatchGet, KvBatchRollback
  - KvCleanup, KvCheckTxnStatus
  - BatchCommands bidirectional streaming (routes to individual RPC handlers)
- [x] Server lifecycle (Start/Stop with graceful shutdown)
- [x] Cluster ID interceptor framework
- [x] Error mapping (internal errors -> kvrpcpb.KeyError: Locked, Conflict, TxnLockNotFound, etc.)
- [x] 18 end-to-end tests passing via gRPC client:
  - Point get, scan, batch get, prewrite+commit, rollback, cleanup
  - Write conflict detection, delete mutation, multiple versions
  - BatchCommands stream, server lifecycle, idempotent operations, large values
  - M5 milestone: full transaction lifecycle (prewrite -> commit -> read) via gRPC

## IMPL-013 internal/config: configuration system - DONE
- [x] Config struct hierarchy with nested sub-configs (Log, Server, Storage, PD, RaftStore, Coprocessor, PessimisticTxn)
- [x] TOML loading via BurntSushi/toml with defaults
- [x] Validation pipeline (log level, format, addresses, raft timers, region sizes, region keys)
- [x] ReadableSize type (KB/MB/GB parsing and formatting)
- [x] Duration wrapper for TOML compatibility
- [x] Config diff via reflection
- [x] Clone and SaveToFile support
- [x] 17 tests passing

## IMPL-014 internal/log: logging and diagnostics - DONE
- [x] log/slog-based structured logging with LevelFilter (atomic runtime level changes)
- [x] LogDispatcher routing by attributes (normal, slow_log, rocksdb_log, raftdb_log)
- [x] SlowLogHandler with threshold-based filtering (duration "takes" attribute)
- [x] File rotation via lumberjack (RotatingFileWriter)
- [x] Setup function for global logger initialization (text/json formats)
- [x] 22 tests passing

## IMPL-015 internal/server/transport: inter-node Raft transport - DONE
- [x] RaftClient with connection pooling (lazy connection establishment)
- [x] Send via Raft client-streaming RPC
- [x] BatchSend via BatchRaft streaming with configurable batch size
- [x] SendSnapshot via chunked gRPC (1MB chunks)
- [x] MessageBatcher for accumulating and flushing batched messages
- [x] HashRegionForConn for FNV-based connection selection
- [x] StoreResolver interface for address resolution
- [x] Connection lifecycle management (RemoveConnection, Close)
- [x] 20 tests passing

## IMPL-016 internal/server/status: HTTP status and diagnostics server - DONE
- [x] pprof endpoints (/debug/pprof/*)
- [x] Prometheus metrics endpoint (/metrics)
- [x] Config endpoint (/config) returning current config as JSON
- [x] Status endpoint (/status) and health check (/health)
- [x] Graceful shutdown with timeout
- [x] 13 tests passing

## IMPL-017 Flow control and backpressure - DONE
- [x] ReadPool with EWMA-based busy threshold (CheckBusy with estimated wait)
- [x] FlowController with probabilistic request dropping (ShouldDrop based on compaction pressure)
- [x] Linear interpolation discard ratio between soft/hard pending compaction byte limits
- [x] MemoryQuota with lock-free CAS-based acquire/release (scheduler memory quota enforcement)
- [x] ServerIsBusyError with error chain support
- [x] 24 tests passing

## IMPL-018 internal/coprocessor: query push-down framework - DONE
- [x] Datum type system (Null, Int64, Uint64, Float64, String, Bytes) with conversions
- [x] RPN expression engine (stack-based postfix evaluation)
  - Column refs, constants, function calls
  - Comparison: EQ, LT, LE, GT, GE, NE
  - Logic: AND, OR, NOT (with 3-valued NULL logic)
  - Arithmetic: Plus, Minus, Mul, Div (with division by zero handling)
  - IS NULL predicate
- [x] TableScanExecutor (scans CF_WRITE, reads values from CF_DEFAULT or short value)
- [x] SelectionExecutor (WHERE clause filtering via RPN predicates)
- [x] LimitExecutor (LIMIT/OFFSET)
- [x] SimpleAggrExecutor (COUNT, SUM, AVG, MIN, MAX without GROUP BY)
- [x] HashAggrExecutor (hash-based GROUP BY aggregation)
- [x] ExecutorsRunner (pipeline driver with batch size growth and deadline)
- [x] Pipeline composition (source -> selection -> limit, source -> selection -> aggregation)
- [x] 47 tests passing

## IMPL-019 Async commit and 1PC optimizations - DONE
- [x] PrewriteAsyncCommit: primary stores all secondary keys, MinCommitTS computed from max_ts
- [x] CheckAsyncCommitStatus: determines commit status from primary lock
- [x] PrewriteAndCommit1PC: single-step prewrite+commit skipping CF_LOCK (write records directly)
- [x] Is1PCEligible: size-based eligibility check
- [x] IsAsyncCommitEligible: key count-based eligibility check
- [x] 15 tests passing (async commit: basic, secondary, conflict, idempotent; 1PC: basic, short value, conflict, delete; eligibility; status checks)

## IMPL-020 Pessimistic transactions - DONE
- [x] AcquirePessimisticLock: write LockTypePessimistic to CF_LOCK with ForUpdateTS
- [x] PrewritePessimistic: upgrade pessimistic lock to normal lock during prewrite
- [x] PessimisticRollback: remove pessimistic locks only (no rollback record)
- [x] Conflict detection against ForUpdateTS (not StartTS)
- [x] Idempotent acquisition with ForUpdateTS update
- [x] End-to-end flow: acquire -> prewrite(upgrade) -> commit -> read verified
- [x] 16 tests passing

## IMPL-021 cmd/gookvs-ctl: Admin CLI - DONE
- [x] scan: Scan keys in a column family with start/end bounds and limit
- [x] get: Get a single key by hex
- [x] mvcc: Show MVCC info (lock, write records) for a user key as JSON
- [x] dump: Raw hex key-value dump
- [x] size: Show approximate data size per column family
- [x] compact: Trigger WAL sync
- [x] help: Usage documentation
- [x] Binary builds successfully
- [x] 5 tests passing

## Summary
All 21 IMPL user stories complete. Total new tests: 179 across 8 new packages.
