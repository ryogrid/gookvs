# TiKV Architecture Overview

This document provides a comprehensive architecture overview of TiKV, a distributed transactional key-value store. It serves as the entry point for understanding TiKV's design before diving into subsystem-specific documents.

## 1. System Overview

TiKV is an open-source, distributed, transactional key-value database inspired by Google's Spanner, BigTable, and Percolator. It provides:

- **ACID-compliant distributed transactions** using a Percolator-based two-phase commit protocol
- **Horizontal scalability** to 100+ TB via automatic region-based sharding
- **Strong consistency** through Multi-Raft consensus (one Raft group per region)
- **Multi-Version Concurrency Control (MVCC)** for snapshot isolation reads

TiKV is designed as the storage layer for TiDB (a distributed SQL database) but also operates as a standalone transactional KV store.

## 2. Component Dependency Graph

### 2.1 Crate Organization

TiKV is organized as a Rust workspace with 80+ crates in three main directories:

```
tikv/
├── cmd/                          # Binary entry points
│   ├── tikv-server/              # Main server binary
│   └── tikv-ctl/                 # Admin/diagnostic CLI
├── src/                          # Core TiKV library
│   ├── config/                   # Configuration definitions
│   ├── coprocessor/              # TiDB push-down query execution
│   ├── coprocessor_v2/           # Plugin-based coprocessor
│   ├── import/                   # SST import
│   ├── server/                   # gRPC server, services, transport
│   └── storage/                  # Transaction layer, MVCC, scheduler
│       ├── mvcc/                 # Multi-Version Concurrency Control
│       └── txn/                  # Transaction processing (Percolator)
├── components/                   # Modular library crates
│   ├── engine_traits/            # Storage engine abstraction (traits)
│   ├── engine_rocks/             # RocksDB engine implementation
│   ├── raftstore/                # Raft consensus & region management (v1)
│   ├── raftstore-v2/             # Next-gen raftstore
│   ├── batch-system/             # FSM-based message batching
│   ├── pd_client/                # Placement Driver client
│   ├── tikv_util/                # Shared utilities
│   ├── txn_types/                # Transaction type definitions
│   ├── concurrency_manager/      # Lock table & concurrency control
│   ├── keys/                     # Key encoding/decoding
│   ├── codec/                    # Binary serialization primitives
│   ├── cdc/                      # Change Data Capture
│   ├── backup/                   # Full backup
│   ├── backup-stream/            # Log backup (PITR)
│   ├── resolved_ts/              # Resolved timestamp tracking
│   ├── resource_control/         # Resource group QoS
│   ├── encryption/               # Encryption at rest
│   ├── security/                 # TLS & security
│   ├── sst_importer/             # SST file import
│   ├── external_storage/         # Cloud storage (S3/GCS/Azure)
│   ├── server/                   # Server bootstrap & status server
│   └── ...                       # Additional utility crates
└── tests/                        # Integration tests
```

### 2.2 Dependency Layers

Crates are organized in a layered dependency hierarchy (bottom-up):

| Layer | Crates | Role |
|-------|--------|------|
| **L1: Foundational** | `error_code`, `codec`, `collections`, `tikv_alloc`, `crypto` | No internal deps; primitives |
| **L2: Utilities** | `tikv_util`, `file_system`, `log_wrappers`, `online_config`, `tracker` | Core utilities used everywhere |
| **L3: Security** | `encryption`, `security` | Cryptography, TLS |
| **L4: Engine Abstraction** | `engine_traits`, `keys`, `txn_types` | Storage engine interface |
| **L5: Engine Impl** | `engine_rocks`, `raft_log_engine`, `hybrid_engine` | Concrete engine backends |
| **L6: Concurrency** | `concurrency_manager` | Lock table, MVCC coordination |
| **L7: Consensus** | `batch-system`, `raftstore`, `raftstore-v2` | Raft replication |
| **L8: Coordination** | `pd_client`, `resolved_ts`, `causal_ts` | Cluster coordination |
| **L9: Features** | `cdc`, `backup`, `backup-stream`, `sst_importer`, `resource_control` | Distributed features |
| **L10: Query** | `tidb_query_*` crates | Coprocessor push-down execution |
| **L11: Transaction KV** | `tikv_kv` | High-level transactional KV interface |
| **L12: Server** | `server` (component), `service` | gRPC server, HTTP status |
| **L13: Application** | `tikv` (root lib), `tikv-server`, `tikv-ctl` | Binary entry points |

### 2.3 Key Inter-Crate Dependencies

```
tikv-server
  └── server (component)
        ├── tikv (root library)
        │     ├── storage (txn scheduler, MVCC)
        │     │     ├── engine_traits (abstract engine)
        │     │     ├── txn_types
        │     │     └── concurrency_manager
        │     ├── coprocessor (push-down query)
        │     └── server (gRPC services)
        │           └── tikv_kv → raftstore → engine_rocks
        ├── raftstore (+ batch-system)
        │     ├── pd_client
        │     ├── engine_traits
        │     └── sst_importer
        ├── cdc → resolved_ts → raftstore
        ├── backup → external_storage
        └── backup-stream → resolved_ts
```

## 3. Thread / Async-Task Model

TiKV uses a mix of dedicated thread pools (via YATP — Yet Another Thread Pool) and Tokio async runtimes. Each pool is purpose-built for its workload characteristics.

### 3.1 Thread Pools

| Thread Pool | Config Key | Purpose | Model |
|-------------|-----------|---------|-------|
| **gRPC Server** | `server.grpc_concurrency` | Handles all gRPC I/O | grpcio `Environment` (completion queues) |
| **Unified Read Pool** | `readpool.unified.{min,max}_thread_count` | All read operations (storage + coprocessor) | YATP with priority lanes (low/normal/high) |
| **Raft Batch System** | `raft_store.store_batch_system` | Raft consensus, peer/store FSM processing | Custom batch-system with FSM polling |
| **Apply Batch System** | (part of raftstore) | Applies committed Raft entries to KV engine | Separate batch-system, parallel to consensus |
| **Scheduler Worker Pool** | (internal to TxnScheduler) | Executes transaction commands after latch acquisition | Thread pool for CPU-bound txn work |
| **Background Worker** | `server.background_thread_count` | General background tasks | `WorkerBuilder`-based MPSC |
| **PD Worker** | (single LazyWorker) | PD heartbeats, split requests, scheduling | Single-threaded MPSC |
| **CDC Worker** | (dedicated) | Change Data Capture event streaming | Dedicated with memory quota |
| **Check Leader Worker** | (single thread) | Region leader checking for TiCDC | Time-sensitive single thread |
| **Debug Thread Pool** | (single Tokio thread) | Profiling, diagnostics | Tokio single-worker runtime |
| **Transport Stats Pool** | `server.stats_concurrency` | gRPC load statistics collection | Tokio multi-thread runtime |

### 3.2 Communication Channels

- **Batch-System Mailboxes**: Lock-free per-FSM message queues between Router and Poller threads
- **MPSC Channels**: Worker tasks (PD, background) use `tikv_util::worker::Scheduler<T>` backed by unbounded MPSC
- **Paired Futures**: Write commands use `paired_future_callback()` returning `(Callback, Future)` — callback resolves the future for async/await integration
- **gRPC Streaming**: PD heartbeats and CDC use bidirectional gRPC streams

### 3.3 Thread-Local State

- Each **read pool thread** stores a thread-local engine instance (set in `after_start` callback, accessed via `Self::with_tls_engine()`)
- Each thread allocates an **exclusive jemalloc arena** for accurate per-thread memory tracking
- Thread group properties enable coordinated shutdown

## 4. Request Lifecycle

### 4.1 KV Read (e.g., `kv_get`)

```
Client
  │
  ▼
gRPC thread pool
  │  handle_request!(kv_get) macro
  │  Validates request, creates Tracker
  ▼
Storage.get_entry(ctx, key, start_ts)
  │  Checks API version, prepares snapshot context
  │  read_pool_spawn_with_busy_check()
  ▼
Read pool thread (YATP)
  │  Acquires region snapshot from RaftKv
  │  (snapshot = consistent view at a timestamp)
  ▼
SnapshotStore.get_entry()
  │  MVCC read: scans CF_LOCK for conflicts,
  │  reads CF_WRITE for latest commit ≤ start_ts,
  │  reads CF_DEFAULT for value
  ▼
Response assembly
  │  Value + commit_ts + scan_detail_v2 + time_detail_v2
  ▼
gRPC sink → Client
```

**Key timing measurements**: `schedule_wait_time` (queue wait), `snapshot_wait_time` (region snapshot), `process_wall_time` (MVCC execution).

### 4.2 KV Write (e.g., `kv_prewrite` — Phase 1 of Percolator 2PC)

```
Client
  │
  ▼
gRPC thread pool
  │  handle_request!(kv_prewrite) macro
  │  Converts to TypedCommand
  ▼
Storage.sched_txn_command(cmd, callback)
  │
  ▼
TxnScheduler event loop
  │  Enqueues command
  │  Acquires latches for all touched keys
  │  (latches serialize overlapping commands)
  ▼
Scheduler worker pool thread (after latches acquired)
  │  Creates snapshot
  │  Executes Percolator prewrite:
  │    - Check for write conflicts (CF_WRITE)
  │    - Check for lock conflicts (CF_LOCK)
  │    - Write intent locks + data
  │  Produces WriteBatch
  ▼
RaftKv.write(write_batch)
  │  Wraps in RaftCmdRequest
  │  Routes to region peer via StoreMsg::RaftCmd
  ▼
Raft Batch System (leader peer FSM)
  │  Appends to Raft log
  │  Replicates to followers
  │  Waits for quorum acknowledgment
  ▼
Apply Batch System
  │  Applies committed entry to RocksDB
  │  Invokes on_applied callback
  ▼
Response via paired future → gRPC sink → Client
```

### 4.3 KV Commit (Phase 2 of Percolator 2PC)

Same flow as prewrite, but:
- Writes **commit record** to CF_WRITE (instead of lock to CF_LOCK)
- **Removes lock** from CF_LOCK
- Returns `TxnStatus::Committed { commit_ts }`

### 4.4 Batch Commands RPC

The `batch_commands` gRPC endpoint multiplexes multiple requests in a single RPC:
1. Routes each request by type using `handle_cmd!` macro
2. Optional `ReqBatcher` groups similar requests for efficiency
3. Collects all responses into `BatchCommandsResponse`

## 5. Cluster Topology

### 5.1 PD-Based Discovery

TiKV nodes do **not** discover each other directly. All coordination flows through the **Placement Driver (PD)**:

```
                    ┌──────────┐
                    │    PD    │  (cluster metadata, TSO, scheduling)
                    │ Cluster  │
                    └────┬─────┘
                ┌────────┼────────┐
                ▼        ▼        ▼
           ┌────────┐ ┌────────┐ ┌────────┐
           │ TiKV 1 │ │ TiKV 2 │ │ TiKV 3 │
           │Store 1 │ │Store 2 │ │Store 3 │
           └────────┘ └────────┘ └────────┘
```

**Initial bootstrap**: TiKV connects to PD via configured `pd_endpoints`. PD returns cluster membership, store IDs, and the initial region map.

**Ongoing coordination**:
- **Store heartbeats**: Each TiKV node periodically reports store-level stats (capacity, usage, I/O rates, CPU) to PD
- **Region heartbeats**: Each region leader reports region stats (size, keys, throughput, down/pending peers) to PD
- **Address resolution**: `PdStoreAddrResolver` maps store IDs to network addresses, cached with auto-refresh

### 5.2 Region Assignment

- PD maintains the authoritative mapping of regions → stores
- Regions are contiguous key ranges, each replicated across multiple stores as a Raft group
- PD balances regions across stores based on reported statistics
- Region metadata includes: region ID, key range `[start_key, end_key)`, peer list, region epoch (conf_ver + version)

### 5.3 Leader Balancing and Scheduling

PD embeds scheduling instructions in heartbeat responses:

| Operation | Trigger | Effect |
|-----------|---------|--------|
| **Add Peer** | Under-replicated region | Grows replication factor |
| **Remove Peer** | Over-replicated or offline peer | Shrinks replication factor |
| **Transfer Leader** | Load imbalance | Recommends new leader via Raft |
| **Split Region** | Region exceeds size/key threshold | Splits into two regions |
| **Merge Regions** | Adjacent small regions | Merges into one region |

These instructions are processed by the PD worker and converted into Raft admin commands (ConfigChange, Split, Merge) for the target region's peer FSM.

### 5.4 Timestamp Oracle (TSO)

PD provides a globally unique, monotonically increasing timestamp service. TiKV (and TiDB) acquire `start_ts` and `commit_ts` from PD's TSO for transaction ordering. The PD client maintains a persistent gRPC stream for TSO requests with automatic reconnection.

## 6. Design Philosophy and Key Trade-offs

### 6.1 Percolator Over Other 2PC Variants

TiKV implements the Percolator protocol (from Google's Percolator paper) rather than traditional two-phase commit:

| Aspect | Percolator (TiKV) | Traditional 2PC |
|--------|-------------------|-----------------|
| **Coordinator** | Distributed — each txn picks a primary key as pseudo-coordinator | Centralized coordinator node |
| **Fault tolerance** | No single point of failure; any node can resolve a stuck txn by inspecting the primary lock | Coordinator failure blocks all participants |
| **Lock storage** | Locks stored in the same KV engine alongside data (CF_LOCK) | Separate lock manager |
| **Recovery** | Self-healing: readers encountering stale locks can resolve them by checking primary key status | Requires coordinator recovery or timeout |

**Trade-off**: Percolator adds per-key lock overhead and requires MVCC GC for old versions, but eliminates the coordinator single-point-of-failure and enables decentralized transaction resolution.

TiKV extends classic Percolator with:
- **Pessimistic locks**: Acquired during execution phase (before prewrite) for `SELECT FOR UPDATE` semantics
- **Async commit**: One-phase commit optimization when safe, reducing latency
- **Shared locks**: For concurrent readers without mutual blocking [Inferred]

See [Transaction and MVCC](transaction_and_mvcc.md) for protocol details.

### 6.2 Region-Based Sharding Rationale

TiKV uses contiguous key-range regions rather than hash-based sharding:

| Aspect | Range-Based Regions (TiKV) | Hash-Based Sharding |
|--------|---------------------------|-------------------|
| **Range queries** | Efficient — keys in range are co-located | Must scatter-gather across all partitions |
| **Coprocessor push-down** | Can push range scans to single region | Must push to all shards |
| **Rebalancing** | Move contiguous ranges (region split/merge) | Rehash and redistribute keys |
| **Hot spot handling** | Dynamic region split isolates hot keys | Must rehash everything |
| **Geo-replication** | Natural key-range locality | Random distribution |

**Trade-off**: Range-based sharding requires active topology management (PD) and can suffer from sequential-write hot spots, but it enables efficient range queries critical for TiDB's SQL layer. PD's dynamic region splitting mitigates hot spot issues.

### 6.3 Multi-Raft (One Raft Group Per Region)

Instead of a single global Raft group, TiKV runs independent Raft groups per region:

| Aspect | Multi-Raft (TiKV) | Single Global Raft |
|--------|-------------------|-------------------|
| **Fault isolation** | One region's failure doesn't affect others | Single failure can partition entire system |
| **Parallelism** | Different regions process concurrently | All regions compete for one log |
| **Failover** | Per-region leader election (fast) | Global election (slow at scale) |
| **Scalability** | Linear with region count | Bounded by single log throughput |

**Trade-off**: Multi-Raft adds complexity in region management and cross-region transactions, but provides much better fault isolation and parallelism at scale.

### 6.4 Engine Abstraction Layer

`engine_traits` defines a comprehensive trait-based abstraction (`KvEngine`, `Snapshot`, `WriteBatch`, etc.) with zero transitive dependencies on RocksDB:

- **Current primary backend**: RocksDB via `engine_rocks`
- **Design goal**: Enable alternative engine backends (e.g., TiRocks, Pebble) without changing upper layers
- **Trade-off**: More boilerplate and associated types vs. direct RocksDB usage, but enables engine experimentation and testing (`engine_panic` for fail-fast tests)

See [Key Encoding and Data Formats](key_encoding_and_data_formats.md) for on-disk format details.

### 6.5 Batch-System for Raft Message Processing

The `batch-system` crate implements a lock-free, FSM-based message batching architecture:

```
Messages → Router (routes by region_id)
         → Mailbox (per-FSM queue)
         → Batch (collects from multiple mailboxes)
         → Poller (executes batch in worker thread)
         → FSM.handle(msg)
```

- **Two FSM types**: `PeerFsm` (one per region replica) and `StoreFsm` (one per store, handles global operations)
- **Batch processing** amortizes lock acquisition, context switching, and I/O submission costs
- **Trade-off**: Introduces batching latency but dramatically improves throughput under load

See [Raft and Replication](raft_and_replication.md) for consensus details.

## 7. Cross-References

| Subsystem | Document |
|-----------|----------|
| Key encoding, MVCC formats, column families | [Key Encoding and Data Formats](key_encoding_and_data_formats.md) |
| Raft protocol, region lifecycle, raftstore | [Raft and Replication](raft_and_replication.md) |
| Percolator protocol, MVCC, lock types, GC | [Transaction and MVCC](transaction_and_mvcc.md) |
| Coprocessor, DAG executors, expressions | [Coprocessor](coprocessor.md) |
| gRPC services, routing, flow control | [gRPC API and Server](grpc_api_and_server.md) |
| CDC, backup, PITR, observer pattern | [CDC and Backup](cdc_and_backup.md) |
| Resource control, encryption, TLS, config | [Resource Control, Security, and Config](resource_control_security_config.md) |
