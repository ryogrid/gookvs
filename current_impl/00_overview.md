# gookv Architecture Overview

## 1. Project Identity

| Field | Value |
|---|---|
| Module | `github.com/ryogrid/gookv` |
| Go version | 1.22.2 |
| License | See `LICENSE` |

### Key Dependencies

| Dependency | Role |
|---|---|
| `github.com/cockroachdb/pebble` v1.1.5 | Storage engine (pure Go, RocksDB-compatible) |
| `go.etcd.io/etcd/raft/v3` v3.5.17 | Raft consensus (RawNode API) |
| `github.com/pingcap/kvproto` | TiKV-compatible protobuf definitions (tikvpb, kvrpcpb, raft_serverpb, raft_cmdpb, eraftpb, metapb, pdpb) |
| `google.golang.org/grpc` v1.79.3 | RPC framework for client-server and inter-node communication (uses `grpc.NewClient`) |
| `github.com/prometheus/client_golang` v1.15.0 | Metrics exposition (Prometheus /metrics endpoint) |
| `github.com/stretchr/testify` v1.11.1 | Test assertions |
| `github.com/BurntSushi/toml` v1.6.0 | TOML config file parsing |
| `gopkg.in/natefinch/lumberjack.v2` v2.2.1 | Log file rotation |

---

## 2. Package Inventory

### Public Packages (`pkg/`)

| Package | Description |
|---|---|
| `pkg/codec` | Memcomparable byte encoding (`EncodeBytes`/`DecodeBytes`) and number encoding (`EncodeUint64Desc`, `EncodeInt64`, `EncodeFloat64`, varint). Wire-compatible with TiKV. |
| `pkg/keys` | Internal key construction for Raft logs, hard state, apply state, region metadata. Defines `DataPrefix` (0x7A) and `LocalPrefix` (0x01) key namespaces. |
| `pkg/cfnames` | Column family name constants: `default`, `lock`, `write`, `raft`. Defines `DataCFs` and `AllCFs` slices. |
| `pkg/txntypes` | Transaction type definitions: `TimeStamp` (hybrid logical clock, physical<<18 \| logical), `Lock` (with TiKV-compatible binary serialization), `Write` (commit/rollback records), `Mutation`, `LockType`, `WriteType`. |
| `pkg/pdclient` | PD (Placement Driver) gRPC client interface: TSO allocation (`GetTS`), region lookup, store management, heartbeats, split requests, cluster bootstrap. Provides `Client` interface and `grpcClient` implementation with multi-endpoint failover and retry logic. |
| `pkg/client` | Multi-region client library: `Client` (entry point, PD-backed), `RawKVClient` (full Raw KV API with transparent cross-region routing), `RegionCache` (sorted-slice binary-search key→region cache with PD fallback), `RegionRequestSender` (gRPC connection pool with region-error retry), `PDStoreResolver` (TTL-cached storeID→address resolver). |
| `pkg/e2elib` | PostgreSQL TAP-style end-to-end test library. `GokvNode` / `GokvCluster` (manage gookv-server processes), `PDNode` / `PDCluster` (manage gookv-pd processes), `PortAllocator` (file-lock-based port allocation), helper functions (`NewStandaloneNode`, `PutAndVerify`, `WaitForSplit`, etc.). |

### Private Packages (`internal/`)

| Package | Description |
|---|---|
| `internal/engine/traits` | Storage engine interface abstractions: `KvEngine` (multi-CF get/put/delete/snapshot/iterator/write-batch), `Snapshot`, `WriteBatch`, `Iterator`, `IterOptions`. Analogous to TiKV's `engine_traits` crate. |
| `internal/engine/rocks` | `KvEngine` implementation using Pebble. Column families emulated via single-byte key prefixing (default=0x00, lock=0x01, write=0x02, raft=0x03). Implements `Engine`, `snapshot`, `writeBatch`, `iterator`. |
| `internal/raftstore` | Raft consensus and region management. Contains `Peer` (one goroutine per region replica, owns `raft.RawNode`), `PeerStorage` (implements `raft.Storage` with engine-backed persistence and in-memory entry cache), `RaftLogWriter` (batches multiple regions' Raft log writes into a single `WriteBatch` + fsync; config: `EnableBatchRaftWrite`), `ApplyWorkerPool` (decouples committed entry application from the peer goroutine via a shared worker pool; config: `EnableApplyPipeline`), message types (`PeerMsg`, `StoreMsg`, `RaftCommand`, `ApplyResult`), and `eraftpb`/`raftpb` protobuf conversion. |
| `internal/raftstore/router` | `sync.Map`-based message routing: maps region ID to peer mailbox channels. Non-blocking send with backpressure (`ErrMailboxFull`). Supports broadcast. |
| `internal/raftstore/snap` | Snapshot generation, serialization, and application. `SnapWorker` processes background snapshot tasks. Note: implemented in `internal/raftstore/snapshot.go` (not a subdirectory). |
| `internal/raftstore/split` | Region split checking and execution. `SplitCheckWorker` scans region sizes; `ExecBatchSplit` creates new regions. |
| `internal/storage/mvcc` | MVCC layer: `MvccTxn` (write accumulator collecting `Modify` operations across CFs), `MvccReader` (snapshot-based reads for locks, writes, values), `PointGetter` (optimized single-key MVCC read with SI/RC isolation and lock bypass), `Scanner` (forward/reverse MVCC range scan with SI/RC isolation in `scanner.go`), key encoding (`EncodeKey`, `EncodeLockKey`, `DecodeKey`). |
| `internal/storage/txn` | Percolator 2PC transaction actions: `Prewrite`, `Commit`, `Rollback`, `CheckTxnStatus`. Also `AcquirePessimisticLock`, `PrewritePessimistic`, `PessimisticRollback`, `PrewriteAsyncCommit`, `CheckAsyncCommitStatus`, `PrewriteAndCommit1PC`. Defines `Mutation`, `PrewriteProps`, `TxnStatus`. |
| `internal/storage/txn/latch` | Deadlock-free key serialization using hash-based slots. Keys are hashed to sorted slot indices; commands acquire slots in order. `Latches.GenLock` / `Acquire` / `Release`. |
| `internal/storage/txn/concurrency` | `ConcurrencyManager`: in-memory lock table (`sync.Map`) and atomic `max_ts` tracking for async commit correctness. `LockKey`/`IsKeyLocked`/`UpdateMaxTS`/`GlobalMinLock`. |
| `internal/storage/gc` | GC (garbage collection) for old MVCC versions. `GCWorker` with 3-state machine (Rewind/RemoveIdempotent/RemoveAll), `SafePointProvider` integration. |
| `internal/storage/txn/scheduler` | TxnScheduler command dispatcher with worker pool, latch-based key serialization, `Command` interface. |
| `internal/raftstore/raftlog_gc.go` | Raft log compaction: `RaftLogGCWorker` for background entry deletion, `execCompactLog` for log truncation. |
| `internal/raftstore/conf_change.go` | Raft configuration changes: `applyConfChangeEntry`, `processConfChange`, `ProposeConfChange`. |
| `internal/raftstore/merge.go` | Region merge: `ExecPrepareMerge` / `ExecCommitMerge` / `ExecRollbackMerge`. |
| `internal/raftstore/store_worker.go` | Region data cleanup: `CleanupRegionData` removes all Raft state for destroyed regions. |
| `internal/pd` | Embedded PD server with optional Raft-based replication for multi-node HA. Core components: TSO allocation, metadata store, ID allocation, GC safe point management, scheduling, move tracking. Raft replication adds: `raft_peer.go` (PDRaftPeer event loop), `raft_storage.go` (Pebble-backed raft.Storage), `command.go` (12 replicated command types), `apply.go` (state machine dispatcher), `forward.go` (follower-to-leader RPC forwarding), `tso_buffer.go`/`id_buffer.go` (batched allocation), `snapshot.go` (full-state snapshots), `transport.go`/`peer_service.go` (inter-PD Raft transport). Implements `pdpb.PD` gRPC service. See [`09_pd_replication.md`](09_pd_replication.md) for details. |
| `internal/server/pd_worker.go` | PDWorker: store heartbeat loop, region heartbeat forwarding, batch split reporting. |
| `internal/server/raw_storage.go` | Non-transactional Raw KV storage layer bypassing MVCC. Supports TTL encoding, CAS, batch scan, checksum computation. |
| `internal/server` | gRPC server (`tikvpb.Tikv` service): `Server` (gRPC lifecycle with optional PD client for TSO), `tikvService` (implements KvGet, KvScan, KvPrewrite, KvCommit, KvBatchGet, KvBatchRollback, KvCleanup, KvCheckTxnStatus, KvCheckSecondaryLocks, KvScanLock, KvPessimisticLock, KvResolveLock, KvTxnHeartBeat, RawBatchScan, RawGetKeyTTL, RawCompareAndSwap, RawChecksum, BatchCommands, Raft, BatchRaft), `Storage` (transaction-aware bridge between engine/MVCC/txn layers with latch-based serialization, async commit, and 1PC support), `StoreCoordinator` (Raft peer lifecycle management, proposal routing, entry application, PD-coordinated split detection and execution), `StaticStoreResolver` (storeID-to-address mapping), `PDStoreResolver` (`pd_resolver.go` — PD-based store resolver for dynamic node discovery), `StoreIdent` (`store_ident.go` — store identity persistence for join mode), `ModifiesToRequests`/`RequestsToModifies` (MVCC modify <-> raft_cmdpb conversion). Region-aware request validation via `validateRegionContext()`. |
| `internal/server/transport` | Inter-node Raft message transport over gRPC: `RaftClient` (connection pooling, `Send`/`BatchSend`/`SendSnapshot`), `MessageBatcher` (batch accumulation), `StoreResolver` interface. |
| `internal/server/status` | HTTP diagnostics server: `/debug/pprof/*`, `/metrics` (Prometheus), `/config`, `/status`, `/health`. |
| `internal/server/flow` | Flow control and backpressure: `ReadPool` (EWMA-based busy detection, worker pool), `FlowController` (probabilistic request dropping based on compaction pressure), `MemoryQuota` (lock-free scheduler memory enforcement). |
| `internal/config` | TOML-based configuration system: `Config` (root), `ServerConfig`, `StorageConfig`, `PDConfig`, `RaftStoreConfig` (includes `EnableBatchRaftWrite` and `EnableApplyPipeline`, both default `true`), `CoprocessorConfig`, `PessimisticTxnConfig`. Supports `LoadFromFile`, `Validate`, `SaveToFile`, `Clone`, `Diff`. Custom types `Duration` and `ReadableSize`. |
| `internal/log` | Structured logging using `log/slog`: `LogDispatcher` (routes records to normal/slow/rocksdb/raft handlers), `SlowLogHandler` (threshold-based filtering), `LevelFilter` (runtime log level changes), `RotatingFileWriter` (lumberjack integration). |
| `internal/coprocessor` | Push-down query execution framework: `TableScanExecutor` (MVCC-aware range scan), `SelectionExecutor` (RPN predicate filtering), `LimitExecutor`, `SimpleAggrExecutor` (COUNT/SUM/MIN/MAX/AVG), `HashAggrExecutor` (GROUP BY), `RPNExpression` (stack-based expression evaluator with comparison/logic/arithmetic ops), `ExecutorsRunner`, `Endpoint`. |

### Command Packages (`cmd/`)

| Package | Description |
|---|---|
| `cmd/gookv-server` | Main server entry point. Parses CLI flags, loads TOML config, opens Pebble engine, creates Storage and gRPC Server, optionally bootstraps Raft cluster mode (with `RaftLogWriter` and `ApplyWorkerPool` when enabled), starts HTTP status server, handles graceful shutdown on SIGINT/SIGTERM. Runs in standalone mode (no PD, no Raft) when no cluster flags are provided. |
| `cmd/gookv-ctl` | Admin CLI. Subcommands: `scan` (range scan by CF), `get` (point read), `mvcc` (MVCC info as JSON), `dump` (raw hex dump with `--decode` and `--sst` for direct SST file parsing), `size` (per-CF key count and size), `compact` (LSM compaction with `CompactAll`/`CompactCF` and `--flush-only` flag), `region` (metadata inspection with `--id`, `--all`, `--limit`). Opens Pebble engine directly. |
| `cmd/gookv-pd` | PD server entry point. Parses `--addr`, `--data-dir`, `--cluster-id` flags for single-node mode, plus `--pd-id`, `--initial-cluster`, `--peer-port`, `--client-cluster` for Raft-replicated cluster mode. |

---

## 3. Layer Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        CLI["gookv-ctl<br/>(Admin CLI)"]
        GRPC_CLIENT["gRPC Clients<br/>(TiKV-compatible)"]
        CLIENT_LIB["pkg/client<br/>RawKVClient, RegionCache,<br/>PDStoreResolver,<br/>RegionRequestSender"]
    end

    subgraph "gRPC Server Layer"
        TIKV_SVC["tikvpb.Tikv Service<br/>KvGet, KvScan, KvPrewrite,<br/>KvCommit, KvBatchGet,<br/>KvBatchRollback, KvCleanup,<br/>KvCheckTxnStatus"]
        BATCH_CMD["BatchCommands<br/>(Multiplexed Streaming)"]
        RAFT_ENDPOINT["Raft / BatchRaft<br/>(Inter-node Streaming)"]
        STATUS_SRV["HTTP Status Server<br/>/metrics, /debug/pprof,<br/>/config, /health"]
    end

    subgraph "Storage / Transaction Layer"
        STORAGE["Storage<br/>(server.Storage)"]
        RAW_STORAGE["RawStorage<br/>(Non-txn Raw KV)"]
        LATCHES["Latches<br/>(Hash-slot Serialization)"]
        CONC_MGR["ConcurrencyManager<br/>(In-memory Lock Table,<br/>max_ts Tracking)"]
        TXN_ACTIONS["Transaction Actions<br/>(Prewrite, Commit, Rollback,<br/>CheckTxnStatus, Pessimistic,<br/>AsyncCommit, 1PC)"]
        TXN_SCHEDULER["TxnScheduler<br/>(Worker Pool,<br/>Latch Serialization)"]
        GC_WORKER["GCWorker<br/>(3-state MVCC GC)"]
    end

    subgraph "MVCC Layer"
        MVCC_TXN["MvccTxn<br/>(Write Accumulator)"]
        MVCC_READER["MvccReader<br/>(LoadLock, SeekWrite,<br/>GetWrite, GetValue)"]
        POINT_GETTER["PointGetter<br/>(SI/RC Isolation)"]
        SCANNER["Scanner<br/>(Forward/Reverse<br/>MVCC Range Scan)"]
        KEY_ENC["Key Encoding<br/>(EncodeKey, EncodeLockKey)"]
    end

    subgraph "Raft Consensus Layer"
        COORD["StoreCoordinator<br/>(Peer Lifecycle)"]
        PEER["Peer<br/>(goroutine per region,<br/>owns raft.RawNode)"]
        PEER_STORAGE["PeerStorage<br/>(raft.Storage impl,<br/>entry cache + engine)"]
        ROUTER["Router<br/>(sync.Map,<br/>regionID -> mailbox)"]
        TRANSPORT["RaftClient<br/>(gRPC connection pool,<br/>message batching)"]
        PD_WORKER["PDWorker<br/>(Heartbeats,<br/>Split Reporting)"]
        RAFT_LOG_WRITER["RaftLogWriter<br/>(Batched fsync<br/>across regions)"]
        APPLY_POOL["ApplyWorkerPool<br/>(Async entry<br/>application)"]
    end

    subgraph "PD Layer"
        PD_SERVER["PDServer<br/>(TSO, Metadata,<br/>GC SafePoint,<br/>Scheduling)"]
        PD_RAFT["PDRaftPeer<br/>(Raft consensus,<br/>PDRaftStorage)"]
        PD_TRANSPORT["PDTransport<br/>(inter-PD gRPC)"]
        PD_BUFFERS["TSOBuffer / IDBuffer<br/>(batched allocation)"]
        PD_FORWARD["Follower Forwarding<br/>(7 unary + 2 streaming)"]
    end

    subgraph "Engine Layer"
        KV_ENGINE["KvEngine Interface<br/>(traits.KvEngine)"]
        PEBBLE["Pebble Engine<br/>(4 CF via key prefix:<br/>default=0x00, lock=0x01,<br/>write=0x02, raft=0x03)"]
    end

    subgraph "Coprocessor Layer"
        COPROCESSOR["Coprocessor Endpoint<br/>(TableScan, Selection,<br/>Limit, Aggregation,<br/>RPN Expressions)"]
    end

    GRPC_CLIENT --> TIKV_SVC
    GRPC_CLIENT --> BATCH_CMD
    CLIENT_LIB --> TIKV_SVC
    CLIENT_LIB --> PD_SERVER
    CLI --> KV_ENGINE

    TIKV_SVC --> STORAGE
    BATCH_CMD --> TIKV_SVC
    RAFT_ENDPOINT --> COORD

    STORAGE --> LATCHES
    STORAGE --> TXN_ACTIONS
    STORAGE --> CONC_MGR

    TXN_ACTIONS --> MVCC_TXN
    TXN_ACTIONS --> MVCC_READER

    MVCC_READER --> KEY_ENC
    MVCC_TXN --> KEY_ENC
    POINT_GETTER --> MVCC_READER

    STORAGE --> POINT_GETTER

    COORD --> PEER
    COORD --> ROUTER
    COORD --> TRANSPORT
    COORD --> STORAGE

    PEER --> PEER_STORAGE
    PEER --> RAFT_LOG_WRITER
    PEER --> APPLY_POOL
    RAFT_LOG_WRITER --> KV_ENGINE
    PEER_STORAGE --> KV_ENGINE

    ROUTER --> PEER

    MVCC_READER --> KV_ENGINE
    STORAGE --> KV_ENGINE
    KV_ENGINE --> PEBBLE

    COPROCESSOR --> KV_ENGINE
    COPROCESSOR --> MVCC_READER

    TIKV_SVC --> RAW_STORAGE
    TIKV_SVC --> GC_WORKER
    TIKV_SVC --> SCANNER

    RAW_STORAGE --> KV_ENGINE
    GC_WORKER --> KV_ENGINE
    GC_WORKER --> MVCC_TXN
    GC_WORKER --> MVCC_READER
    SCANNER --> MVCC_READER

    TXN_SCHEDULER --> LATCHES
    TXN_SCHEDULER --> TXN_ACTIONS

    COORD --> PD_WORKER
    PD_WORKER --> PD_SERVER

    PD_SERVER --> PD_RAFT
    PD_SERVER --> PD_BUFFERS
    PD_BUFFERS --> PD_RAFT
    PD_RAFT --> PD_TRANSPORT
    PD_FORWARD --> PD_SERVER
```

---

## 4. Cluster Mode vs Standalone Mode

The server operates in one of two modes, determined by the `--store-id` and `--initial-cluster` CLI flags.

### Standalone Mode (default)

When no cluster flags (`--store-id`, `--initial-cluster`, `--pd-endpoints`) are provided, the server runs without Raft consensus and without PD. The default PD endpoints are cleared automatically, so gookv-server starts fully self-contained. All writes go directly to the local Pebble engine via `Storage.Prewrite()` / `Storage.Commit()`:

1. gRPC handler calls `Storage.Prewrite(mutations, ...)`.
2. `Storage` acquires latches, takes an engine snapshot, creates `MvccTxn` + `MvccReader`.
3. Transaction actions (`txn.Prewrite`) compute MVCC modifications (lock writes, value writes).
4. If all succeed, `Storage.ApplyModifies()` writes a single atomic `WriteBatch` to Pebble.

The `StoreCoordinator` is nil, so no Raft proposal path is taken.

### Cluster Mode

When `--store-id N --initial-cluster "1=addr1,2=addr2,..."` is provided:

1. A `StaticStoreResolver` maps store IDs to addresses.
2. A `RaftClient` manages gRPC connections to peer stores.
3. A `Router` maps region IDs to peer mailbox channels.
4. A `StoreCoordinator` is created and attached to the `Server`.
5. A single region (region 1) is bootstrapped spanning all stores. Each store gets one `Peer` goroutine.

### Join Mode

A node can join an existing cluster with only `--pd-endpoints` (no `--initial-cluster` needed). In this mode the node allocates a store ID from PD automatically, persists the identity to disk via `store_ident.go`, and waits for PD to schedule region replicas onto it. The `PDStoreResolver` (`pd_resolver.go`) resolves peer addresses dynamically from PD instead of using a static cluster map. PD's region balance scheduler and excess replica shedding scheduler automatically distribute replicas to the new node.

**Write path in cluster mode:**
1. gRPC handler calls `Storage.PrewriteModifies(mutations, ...)` to compute MVCC modifications without applying them.
2. The handler calls `StoreCoordinator.ProposeModifies(regionID, modifies, timeout)`.
3. `ProposeModifies` serializes modifications as `raft_cmdpb.RaftCmdRequest`, sends via the peer's mailbox.
4. The `Peer` goroutine proposes the data through `raft.RawNode.Propose()`.
5. Raft log entries are persisted via `RaftLogWriter` (when `EnableBatchRaftWrite` is true), which batches multiple regions' writes into a single `WriteBatch` + fsync.
6. After Raft consensus, committed entries are submitted to the `ApplyWorkerPool` (when `EnableApplyPipeline` is true), which applies them via `Storage.ApplyModifies()` using `CommitNoSync` -- durability is already guaranteed by the Raft log fsync.

**Inter-node communication:**
- Raft messages are sent via `RaftClient.Send()` (gRPC streaming using `tikvpb.Tikv/Raft`).
- Incoming Raft messages arrive at `tikvService.Raft()` / `tikvService.BatchRaft()`, which dispatch to `StoreCoordinator.HandleRaftMessage()` -> `Router.Send()` -> peer mailbox.

---

## 5. Server Startup Sequence

The startup flow in `cmd/gookv-server/main.go`:

```
main()
  |
  +-- 1. Parse CLI flags
  |     --config, --addr, --status-addr, --data-dir, --pd-endpoints,
  |     --store-id, --initial-cluster
  |
  +-- 2. Load configuration
  |     config.LoadFromFile(path) or config.DefaultConfig()
  |     Apply CLI overrides for addr, status-addr, data-dir, pd-endpoints
  |     config.Validate()
  |
  +-- 3. Open Pebble engine
  |     rocks.Open(cfg.Storage.DataDir)
  |     -> Creates/opens Pebble DB at the data directory
  |
  +-- 4. Create Storage and auxiliary layers
  |     server.NewStorage(engine)
  |     -> Initializes Latches (2048 slots), ConcurrencyManager
  |     server.NewRawStorage(engine)
  |     -> Non-transactional Raw KV storage (bypasses MVCC)
  |     gc.NewGCWorker(engine, safePointProvider)
  |     -> Starts background GC goroutine (3-state machine)
  |
  +-- 5. Create gRPC Server
  |     server.NewServer(cfg, storage, rawStorage, gcWorker)
  |     -> Creates grpc.Server with max message size 16MB
  |     -> Registers tikvpb.TikvServer (all KV + Raft + RawKV RPCs)
  |     -> Enables gRPC server reflection
  |
  +-- 6. [Cluster mode only] Create Raft infrastructure
  |     if --store-id > 0 && --initial-cluster != "":
  |       a. Parse initial-cluster map (storeID=addr,...)
  |       b. StaticStoreResolver(clusterMap)
  |       c. transport.NewRaftClient(resolver, config)
  |       d. router.New(256)
  |       e0. If EnableBatchRaftWrite: NewRaftLogWriter(engine, 256)
  |       e1. If EnableApplyPipeline: NewApplyWorkerPool(4)
  |       e. StoreCoordinator(storeID, engine, storage, router, client, peerCfg)
  |       f. srv.SetCoordinator(coord)
  |       g. Bootstrap region 1 with all peers
  |          coord.BootstrapRegion(region, raftPeers)
  |          -> NewPeer (creates raft.RawNode, PeerStorage)
  |          -> SetSendFunc (wired to RaftClient)
  |          -> SetApplyFunc (wired to StoreCoordinator.applyEntries)
  |          -> Router.Register(regionID, mailbox)
  |          -> go peer.Run(ctx) -- starts event loop goroutine
  |       h. Connect to PD server
  |          pdclient.NewClient(pdEndpoints)
  |          -> Establishes gRPC connection to PD cluster
  |       i. Register store with PD
  |          pdClient.PutStore(storeID, addr)
  |       j. Create and start PDWorker
  |          server.NewPDWorker(pdClient, storeID, coord)
  |          -> Wires pdTaskCh for heartbeat/split reporting
  |          -> go pdWorker.Run(ctx) -- store heartbeat loop, region heartbeat forwarding
  |       k. Start split result handler
  |          go coord.RunSplitResultHandler(ctx)
  |          -> Processes split check results, coordinates with PD for new IDs,
  |             executes ExecBatchSplit, bootstraps child regions, reports to PD
  |
  +-- 6b. [Join mode] Create Raft infrastructure (alternative to 6)
  |     if --pd-endpoints provided && --initial-cluster not provided:
  |       a. Allocate or load store ID (store_ident.go)
  |          If data-dir has persisted identity, reuse it; otherwise call
  |          pdClient.AllocID() and persist to disk.
  |       b. PDStoreResolver(pdClient) — resolves storeID→addr via PD
  |       c. transport.NewRaftClient(pdResolver, config)
  |       d. router.New(256)
  |       e. StoreCoordinator(storeID, engine, storage, router, client, peerCfg)
  |       f. srv.SetCoordinator(coord)
  |       g. Register store with PD: pdClient.PutStore(storeID, addr)
  |       h. Create and start PDWorker (heartbeats trigger scheduling)
  |       i. Node starts empty; PD schedules region replicas via heartbeat
  |          responses, which the coordinator handles as AddPeer commands.
  |
  +-- 7. Start gRPC server
  |     srv.Start()
  |     -> net.Listen("tcp", addr)
  |     -> go grpcServer.Serve(listener)
  |
  +-- 8. Start HTTP status server
  |     statusserver.New(config).Start()
  |     -> Registers /debug/pprof/*, /metrics, /config, /status, /health
  |     -> go httpServer.Serve(listener)
  |
  +-- 9. Signal handling
  |     Wait for SIGINT or SIGTERM
  |
  +-- 10. Graceful shutdown
        coord.Stop()        -- cancels all peer contexts, waits for goroutines
        statusSrv.Stop()    -- HTTP graceful shutdown (5s timeout)
        srv.Stop()          -- gRPC GracefulStop()
        engine.Close()      -- (deferred) closes Pebble
```

---

## 6. Component Dependency Diagram

```mermaid
graph LR
    subgraph "cmd"
        SERVER["gookv-server"]
        CTL["gookv-ctl"]
        PD_BIN["gookv-pd"]
    end

    subgraph "internal/server"
        SRV["server.Server"]
        STOR["server.Storage"]
        COORD["server.StoreCoordinator"]
        RESOLVER["server.StaticStoreResolver"]
        RAFTCMD["server.raftcmd"]
    end

    subgraph "internal/server/*"
        TRANSPORT["transport.RaftClient"]
        STATUS["status.Server"]
        FLOW["flow.ReadPool<br/>FlowController<br/>MemoryQuota"]
    end

    subgraph "internal/raftstore"
        PEER["raftstore.Peer"]
        PSTORAGE["raftstore.PeerStorage"]
        MSG["raftstore.msg"]
        CONVERT["raftstore.convert"]
    end

    subgraph "internal/raftstore/*"
        ROUTER["router.Router"]
    end

    subgraph "internal/storage/mvcc"
        MVCC_TXN["mvcc.MvccTxn"]
        MVCC_READER["mvcc.MvccReader"]
        MVCC_PG["mvcc.PointGetter"]
        MVCC_SCANNER["mvcc.Scanner"]
        MVCC_KEY["mvcc.key"]
    end

    subgraph "internal/storage/txn"
        ACTIONS["txn.actions"]
        PESSIMISTIC["txn.pessimistic"]
        ASYNC["txn.async_commit"]
    end

    subgraph "internal/storage/txn/*"
        LATCH["latch.Latches"]
        CONCMGR["concurrency.Manager"]
        TXN_SCHED["scheduler.TxnScheduler"]
    end

    subgraph "internal/engine"
        TRAITS["traits.KvEngine"]
        ROCKS["rocks.Engine<br/>(Pebble)"]
    end

    subgraph "internal/pd"
        PD_SRV["pd.PDServer"]
        PD_RAFT_PEER["pd.PDRaftPeer"]
        PD_RAFT_STORAGE["pd.PDRaftStorage"]
        PD_TRANSPORT_PD["pd.PDTransport"]
        PD_CMD["pd.PDCommand"]
        PD_SNAP["pd.PDSnapshot"]
        PD_FWD["pd.forward"]
        PD_BUF["pd.TSOBuffer<br/>pd.IDBuffer"]
    end

    subgraph "internal/storage/gc"
        GC_WORKER["gc.GCWorker"]
    end

    subgraph "internal/server (extended)"
        PD_WORKER["server.PDWorker"]
        RAW_STOR["server.RawStorage"]
    end

    subgraph "internal/raftstore (extended)"
        RAFTLOG_GC["raftstore.RaftLogGCWorker"]
        CONF_CHANGE["raftstore.confChange"]
        MERGE["raftstore.merge"]
        STORE_WORKER["raftstore.storeWorker"]
        SNAP["raftstore.SnapWorker"]
        SPLIT["split.SplitCheckWorker"]
        RAFT_LOG_WRITER["raftstore.RaftLogWriter"]
        APPLY_WORKER["raftstore.ApplyWorkerPool"]
    end

    subgraph "internal"
        CONFIG["config.Config"]
        LOG["log.LogDispatcher"]
        COPROCESSOR["coprocessor.Endpoint"]
    end

    subgraph "pkg"
        CODEC["codec"]
        KEYS["keys"]
        CFNAMES["cfnames"]
        TXNTYPES["txntypes"]
        PDCLIENT["pdclient"]
        PKG_CLIENT["client<br/>(RawKVClient,<br/>RegionCache)"]
        E2ELIB["e2elib<br/>(GokvNode, PDNode,<br/>GokvCluster, PDCluster,<br/>PortAllocator)"]
    end

    %% cmd dependencies
    SERVER --> SRV
    SERVER --> STOR
    SERVER --> COORD
    SERVER --> CONFIG
    SERVER --> ROCKS
    SERVER --> ROUTER
    SERVER --> TRANSPORT
    SERVER --> STATUS
    SERVER --> PEER

    CTL --> ROCKS
    CTL --> TRAITS
    CTL --> MVCC_READER
    CTL --> CFNAMES
    CTL --> TXNTYPES

    %% server internal deps
    SRV --> STOR
    SRV --> COORD
    SRV --> MVCC_PG
    SRV --> ACTIONS
    SRV --> TXNTYPES

    STOR --> TRAITS
    STOR --> MVCC_TXN
    STOR --> MVCC_READER
    STOR --> MVCC_PG
    STOR --> ACTIONS
    STOR --> LATCH
    STOR --> CONCMGR

    COORD --> ROUTER
    COORD --> TRANSPORT
    COORD --> PEER
    COORD --> STOR
    COORD --> TRAITS
    COORD --> RAFTCMD

    RAFTCMD --> MVCC_TXN

    TRANSPORT --> RESOLVER

    %% raftstore deps
    PEER --> PSTORAGE
    PEER --> TRAITS
    PEER --> MSG
    PSTORAGE --> TRAITS
    PSTORAGE --> KEYS
    PSTORAGE --> CFNAMES
    CONVERT --> MSG

    ROUTER --> MSG

    %% mvcc deps
    MVCC_TXN --> CFNAMES
    MVCC_TXN --> TXNTYPES
    MVCC_KEY --> CODEC
    MVCC_KEY --> TXNTYPES
    MVCC_READER --> TRAITS
    MVCC_READER --> CFNAMES
    MVCC_READER --> TXNTYPES
    MVCC_PG --> MVCC_READER
    MVCC_PG --> TXNTYPES

    %% txn deps
    ACTIONS --> MVCC_TXN
    ACTIONS --> MVCC_READER
    ACTIONS --> TXNTYPES
    PESSIMISTIC --> MVCC_TXN
    PESSIMISTIC --> MVCC_READER
    PESSIMISTIC --> TXNTYPES
    ASYNC --> MVCC_TXN
    ASYNC --> MVCC_READER
    ASYNC --> TXNTYPES

    %% engine deps
    ROCKS --> CFNAMES
    ROCKS --> TRAITS

    %% coprocessor deps
    COPROCESSOR --> TRAITS
    COPROCESSOR --> MVCC_KEY
    COPROCESSOR --> CFNAMES
    COPROCESSOR --> TXNTYPES

    %% scanner deps
    MVCC_SCANNER --> MVCC_READER
    MVCC_SCANNER --> TXNTYPES

    %% gc deps
    GC_WORKER --> MVCC_TXN
    GC_WORKER --> MVCC_READER
    GC_WORKER --> TRAITS

    %% raw storage deps
    RAW_STOR --> TRAITS
    RAW_STOR --> CFNAMES
    SRV --> RAW_STOR
    SRV --> GC_WORKER

    %% pd deps
    PD_BIN --> PD_SRV
    PD_SRV --> TXNTYPES
    PD_WORKER --> PDCLIENT
    PD_WORKER --> COORD
    COORD --> PD_WORKER

    %% pd internal deps
    PD_SRV --> PD_RAFT_PEER
    PD_SRV --> PD_BUF
    PD_SRV --> PD_FWD
    PD_RAFT_PEER --> PD_RAFT_STORAGE
    PD_RAFT_PEER --> PD_CMD
    PD_RAFT_PEER --> PD_SNAP
    PD_RAFT_PEER --> PD_TRANSPORT_PD
    PD_RAFT_STORAGE --> TRAITS
    PD_RAFT_STORAGE --> KEYS
    PD_BUF --> PD_RAFT_PEER

    %% scheduler deps
    TXN_SCHED --> LATCH
    TXN_SCHED --> ACTIONS

    %% raftstore extended deps
    RAFT_LOG_WRITER --> TRAITS
    APPLY_WORKER --> PEER
    PEER --> RAFT_LOG_WRITER
    PEER --> APPLY_WORKER
    RAFTLOG_GC --> TRAITS
    RAFTLOG_GC --> KEYS
    CONF_CHANGE --> PEER
    MERGE --> PEER
    MERGE --> PSTORAGE
    STORE_WORKER --> TRAITS
    STORE_WORKER --> KEYS
    SNAP --> TRAITS
    SNAP --> PSTORAGE
    SPLIT --> TRAITS
    SPLIT --> KEYS

    %% pkg internal deps
    KEYS --> CODEC
    TXNTYPES -.-> CODEC
    PDCLIENT -.-> TXNTYPES
    PKG_CLIENT --> PDCLIENT
    PKG_CLIENT --> SRV
```

### Key Architectural Notes

- **Column family emulation**: Pebble is a single-keyspace LSM. gookv emulates TiKV's 4 column families (`default`, `lock`, `write`, `raft`) by prepending a one-byte prefix (0x00-0x03) to every key. Iterator bounds are scoped per-CF.

- **MVCC three-CF scheme**: Following TiKV's Percolator model:
  - `CF_LOCK` stores active transaction locks (keyed by encoded user key, no timestamp).
  - `CF_WRITE` stores commit/rollback records (keyed by encoded user key + descending commit timestamp).
  - `CF_DEFAULT` stores large values (keyed by encoded user key + descending start timestamp). Values under 255 bytes are inlined in the `CF_WRITE` record as `ShortValue`.

- **One goroutine per peer**: Each Raft region replica runs in its own goroutine with a ticker-driven event loop (`Peer.Run`). Messages arrive via a buffered channel mailbox. This replaces TiKV's thread pool + batch system.

- **Latch-based command serialization**: Before any transactional operation, the `Storage` layer acquires latches on the affected keys. Latches use FNV-1a hashing to map keys to sorted slot indices, ensuring deadlock-free acquisition.

- **Dual write path**: In standalone mode, MVCC modifications are applied directly via `WriteBatch`. In cluster mode, modifications are serialized as `raft_cmdpb.Request` entries, proposed through Raft, and applied on all replicas after consensus.

- **CommitNoSync on apply**: `Storage.ApplyModifies()` uses `WriteBatch.CommitNoSync()` (Pebble `NoSync` option) because durability is guaranteed by the Raft log, which is persisted with a full fsync via `RaftLogWriter`. On crash recovery, unapplied entries are replayed from the persisted Raft log.

- **RaftLogWriter**: Batches `WriteTask`s from multiple peer goroutines into a single `WriteBatch` + fsync call, amortizing disk I/O across regions. Enabled by `RaftStoreConfig.EnableBatchRaftWrite` (default `true`).

- **ApplyWorkerPool**: Decouples committed entry application from the peer goroutine. A pool of worker goroutines (default 4) processes `ApplyTask`s, allowing peers to immediately handle the next Raft Ready cycle. Enabled by `RaftStoreConfig.EnableApplyPipeline` (default `true`).

- **Binary compatibility**: Lock and Write serialization formats use the same tag-based binary encoding as TiKV, ensuring wire compatibility. Key encodings (memcomparable bytes, descending uint64) are also byte-identical.
