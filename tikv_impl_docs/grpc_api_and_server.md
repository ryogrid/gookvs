# gRPC API and Server

This document specifies TiKV's gRPC server layer: all services and RPCs, request routing to internal subsystems, flow control and backpressure mechanisms, connection management, the HTTP status server, the PD client protocol, and inter-node Raft message transport. It serves as the build specification for reimplementing TiKV's network and server infrastructure.

**Cross-references:** [Architecture Overview](architecture_overview.md) for system-level request lifecycle, [Raft and Replication](raft_and_replication.md) for raftstore internals, [Transaction and MVCC](transaction_and_mvcc.md) for transaction protocol details, [Coprocessor](coprocessor.md) for push-down execution.

---

## 1. gRPC Services Overview

TiKV exposes three gRPC services, all registered on a single gRPC server instance:

| Service | Source | Purpose |
|---------|--------|---------|
| **TikvService** | `src/server/service/kv.rs` | Primary KV operations, transactions, replication, coprocessor |
| **DebugService** | `src/server/service/debug.rs` | Internal debugging and state inspection |
| **DiagnosticsService** | `src/server/service/diagnostics/mod.rs` | System info and log searching |

All RPCs validate the cluster ID from request context before processing. If the cluster ID does not match, the request is rejected immediately.

---

## 2. TikvService RPCs

The TikvService contains ~70 RPCs organized into the categories below. Three macro-based dispatch patterns handle request routing:

### 2.1 Dispatch Macros

**`handle_request!`** — Used for read/query operations. Validates cluster ID, extracts resource control metadata, records metrics, and spawns an async task on the gRPC thread pool. Used by: `kv_get`, `kv_scan`, `raw_get`, `coprocessor`, etc.

**`txn_command_future!`** — Used for transactional write operations. Converts the gRPC request into a `TypedCommand`, routes it through `Storage::sched_txn_command()` (which acquires latches and schedules execution), and collects scan/write metrics. Used by: `kv_prewrite`, `kv_commit`, `kv_cleanup`, etc.

**`handle_cmd!`** — Routes individual commands within a `batch_commands` streaming RPC. Dispatches to the appropriate `future_*` handler based on command type. Supports `ReqBatcher` for batching compatible sub-requests.

### 2.2 KV Transactional RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_prewrite` | `Prewrite` command → scheduler | Percolator prewrite: acquires locks, writes provisional data |
| `kv_commit` | `Commit` command → scheduler | Percolator commit: converts locks to write records |
| `kv_cleanup` | `Cleanup` command → scheduler | Cleans up a timed-out or failed transaction's lock |
| `kv_batch_rollback` | `BatchRollback` command → scheduler | Rolls back locks for a list of keys |
| `kv_txn_heart_beat` | `TxnHeartBeat` command → scheduler | Extends lock TTL to prevent premature cleanup |
| `kv_check_txn_status` | `CheckTxnStatus` command → scheduler | Checks transaction state, resolves expired locks |
| `kv_check_secondary_locks` | `CheckSecondaryLocks` command → scheduler | Checks secondary lock status for async commit |

### 2.3 Pessimistic Lock RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_pessimistic_lock` | `AcquirePessimisticLock` command → scheduler | Acquires pessimistic locks before prewrite |
| `kv_pessimistic_rollback` | `PessimisticRollback` command → scheduler | Releases pessimistic locks on abort |

### 2.4 Lock Resolution RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_resolve_lock` | `ResolveLock` command → scheduler | Batch-resolves locks for a transaction (commit or rollback) |
| `kv_scan_lock` | `storage.scan_lock()` | Scans locks within a key range (used by GC) |

### 2.5 KV Read RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_get` | `storage.get_entry()` | Point read with MVCC snapshot |
| `kv_scan` | `storage.scan()` | Range scan with MVCC snapshot |
| `kv_batch_get` | `storage.batch_get()` | Multi-key batch read |
| `kv_buffer_batch_get` | Buffered batch get | Batch get with buffering optimization |
| `mvcc_get_by_key` | MVCC introspection | Returns MVCC info (locks, writes, values) for a key |
| `mvcc_get_by_start_ts` | MVCC introspection | Returns MVCC info for a transaction's start_ts |

### 2.6 Raw KV RPCs

Raw KV operations bypass the transaction layer entirely, operating directly on the storage engine.

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `raw_get` | `storage.raw_get()` | Single key get |
| `raw_batch_get` | `storage.raw_batch_get()` | Multi-key batch get |
| `raw_put` | `storage.raw_put()` | Single key put |
| `raw_batch_put` | `storage.raw_batch_put()` | Multi-key batch put |
| `raw_delete` | `storage.raw_delete()` | Single key delete |
| `raw_batch_delete` | `storage.raw_batch_delete()` | Multi-key batch delete |
| `raw_scan` | `storage.raw_scan()` | Forward range scan |
| `raw_batch_scan` | `storage.raw_batch_scan()` | Multiple range scans |
| `raw_delete_range` | `storage.raw_delete_range()` | Deletes all keys in a range |
| `raw_get_key_ttl` | `storage.raw_get_key_ttl()` | Gets TTL for a raw key |
| `raw_compare_and_swap` | `storage.raw_compare_and_swap()` | Atomic CAS operation |
| `raw_checksum` | `storage.raw_checksum()` | Computes checksum over a key range |

### 2.7 Coprocessor RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `coprocessor` | `copr.parse_and_handle_unary_request()` | Push-down DAG/Analyze/Checksum execution |
| `coprocessor_stream` | Streaming coprocessor | Server-streaming variant for large result sets |
| `raw_coprocessor` | Coprocessor V2 plugin dispatch | Runs V2 plugin code against raw KV |

### 2.8 Raft Replication RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `raft` | `handle_raft_message()` → raft router | Receives individual Raft messages (legacy) |
| `batch_raft` | `handle_raft_message()` for each → raft router | Receives batched Raft messages (modern) |
| `snapshot` | `SnapTask::Recv` → snap scheduler | Receives snapshot chunk stream |
| `tablet_snapshot` | Tablet snapshot handler | Receives raftstore-v2 tablet snapshots |

### 2.9 Region Management RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `split_region` | `engine.raft_extension().split()` | Triggers region split |
| `unsafe_destroy_range` | `gc_worker.unsafe_destroy_range()` | Destroys all data in a range (admin operation) |
| `kv_delete_range` | Delete range command | Deletes keys within a range |

### 2.10 Batch Operations

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `batch_commands` | `handle_batch_commands_request()` | Multiplexed bidirectional stream carrying sub-requests |

The `batch_commands` RPC is the primary transport for TiDB→TiKV communication. It carries a stream of `BatchCommandsRequest` messages, each containing multiple sub-requests. The `handle_cmd!` macro dispatches each sub-request to its corresponding `future_*` handler. A `ReqBatcher` aggregates compatible get/raw-get sub-requests to reduce per-request overhead.

### 2.11 Cluster and Health RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `check_leader` | `check_leader_scheduler` | Checks region leader status for stale reads |
| `get_store_safe_ts` | `check_leader_scheduler` | Returns store-level safe timestamp |
| `get_lock_wait_info` | `storage.dump_wait_for_entries()` | Returns current pessimistic lock wait chains |
| `get_health_feedback` | `HealthFeedbackAttacher` | Returns health status for client-side load balancing |

### 2.12 Flashback RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_prepare_flashback_to_version` | Prepare flashback command | Prepares region for flashback to a historical version |
| `kv_flashback_to_version` | Flashback command | Executes flashback to the target version |

### 2.13 Other RPCs

| RPC | Internal Routing | Description |
|-----|-----------------|-------------|
| `kv_flush` | Flush command | Flushes buffered writes for pipelined DML |
| `broadcast_txn_status` | Broadcast command | Broadcasts transaction commit/rollback status |

### 2.14 DebugService RPCs

The debug service (~20 RPCs) provides internal state inspection:

- **Raft inspection:** `get_region_info`, `raft_log` — query raft state machine and log entries
- **Engine inspection:** `get`, `scan_mvcc`, `compact` — direct engine operations
- **Store operations:** `get_store_info`, `get_cluster_info` — cluster metadata
- **Metrics:** `get_metrics` — retrieve Prometheus metrics, RocksDB stats, CPU profiles
- **Consistency:** `check_region_consistency` — verifies data consistency across replicas
- **Recovery:** `recover_mvcc`, `reset_to_version` — repair/reset operations

### 2.15 DiagnosticsService RPCs

| RPC | Description |
|-----|-------------|
| `search_log` | Server-streaming log search with regex, time range, and log level filters |
| `server_info` | Returns system diagnostics: hardware info, load info, system info, or all combined |

---

## 3. Request Routing Architecture

### 3.1 Read Path

```
gRPC thread
  → handle_request! macro (validation, metrics, resource control)
    → storage.get() / storage.scan() / storage.batch_get()
      → read_pool.spawn() (YATP thread pool)
        → snapshot.get() / Scanner (MvccReader over engine snapshot)
          → RocksDB point lookup or range iteration
```

Read requests are dispatched to the **read pool** (a YATP-based thread pool). Each read pool thread holds a thread-local engine clone [Inferred]. The read pool supports a busy threshold check (see §4.1) that can reject requests before queueing.

### 3.2 Write Path (Transactional)

```
gRPC thread
  → txn_command_future! macro (converts request to TypedCommand)
    → storage.sched_txn_command()
      → TxnScheduler.run_cmd()
        → memory quota allocation check
        → flow_controller.should_drop() check
        → latch acquisition (hash-based, deadlock-free)
          → execute command (prewrite/commit/rollback/etc.)
            → MvccTxn accumulates writes
            → write to RaftKv (Raft replication)
              → Raft proposal → commit → apply → RocksDB
```

Write commands go through the **TxnScheduler** which provides latch-based serialization (sorted key hashes prevent deadlocks), memory quota enforcement, and flow control before execution.

### 3.3 Coprocessor Path

```
gRPC thread
  → handle_request! macro
    → copr.parse_and_handle_unary_request()
      → busy threshold check
      → read_pool.spawn()
        → DAGRequest parsing → executor pipeline
          → BatchExecutor chain (TableScan → Selection → Aggregation → ...)
            → RPN expression evaluation
```

Coprocessor requests share the read pool with KV reads and undergo the same busy threshold check. A "light task" optimization (requests < 5ms) bypasses the concurrency semaphore [Inferred].

---

## 4. Flow Control and Backpressure

TiKV implements multi-layered overload protection:

### 4.1 Read Pool Busy Threshold

**Location:** `src/read_pool.rs`

The read pool estimates wait time using an EWMA (Exponential Weighted Moving Average) of task execution time multiplied by queue depth per worker:

```
estimated_wait = ewma_time_slice × queue_size_per_worker
```

- EWMA is updated every 200ms (`UPDATE_EWMA_TIME_SLICE_INTERVAL`)
- Clients specify `busy_threshold_ms` in their request context
- If `estimated_wait > busy_threshold`, the request is rejected with `ServerIsBusy` error containing the actual estimated wait time in milliseconds
- Both KV reads and coprocessor requests check this threshold before spawning

### 4.2 Write Flow Controller

**Location:** `src/storage/txn/flow_controller/`

Two implementations exist: `SingletonFlowController` (per-engine, v1) and `TabletFlowController` (per-region, v2).

#### 4.2.1 Probabilistic Request Dropping

The flow controller calculates a **discard ratio** (0.0–1.0) based on RocksDB compaction pending bytes pressure:

```
if pending_compaction_bytes is between soft_limit and hard_limit:
    discard_ratio = (pending - soft) / (hard - soft)
```

Each incoming write command calls `should_drop(region_id)`. A random number is compared against the discard ratio to probabilistically drop requests, reducing write amplification under compaction pressure. The discard ratio is stored in an `AtomicU32` for lock-free access.

#### 4.2.2 Write Rate Limiting

The `consume(region_id, bytes)` method implements a token-bucket rate limiter that returns a `Duration` the caller must wait. The rate limit adapts dynamically based on five RocksDB signals:

1. **Memtable fullness** — immutable memtable count
2. **L0 file accumulation** — pending L0 compactions
3. **L0 production flow** — flush bytes/sec
4. **L0 consumption flow** — compaction read bytes/sec
5. **Pending compaction bytes** — tracked via a 1024-sample smoother

Minimum wait granularity is 1ms with 1ms token refill for smooth throttling.

### 4.3 Scheduler Memory Quota

**Location:** `src/storage/txn/scheduler.rs`

Default capacity: **256 MB** (`DEFAULT_TXN_MEMORY_QUOTA_CAPACITY`).

Each `Task` allocates memory from the quota (based on `approximate_heap_size()`) **before** acquiring latches. If the quota is exhausted, the request is rejected immediately with `ErrorInner::SchedTooBusy` ("scheduler is busy"). Memory is freed when the Task is dropped (RAII).

### 4.4 Raft Message Rejection on Memory Pressure

**Location:** `src/server/service/kv.rs`

```
fn needs_reject_raft_append(reject_messages_on_memory_ratio: f64) -> bool:
    if memory_usage_reaches_high_water:
        raft_usage = MEMTRACE_RAFT_ENTRIES + MEMTRACE_RAFT_MESSAGES
        if (raft_usage + cached_entries + applying_entries) > total_usage × ratio:
            return true
```

When system memory is under pressure and Raft-related memory (entries, messages, apply buffers) exceeds the configured ratio of total usage, incoming `MsgAppend` Raft messages are rejected. This prevents write amplification from overwhelming the node. Configuration: `reject_messages_on_memory_ratio` (0.0 = disabled).

### 4.5 gRPC-Level Resource Limits

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `grpc_concurrent_stream` | 1024 | Max concurrent HTTP/2 streams per connection |
| `grpc_memory_pool_quota` | `isize::MAX` | Memory quota for gRPC operations |
| `grpc_stream_initial_window_size` | 2 MB | HTTP/2 flow control window per stream |

### 4.6 Quota Limiter (CPU, Bandwidth, IOPS)

**Location:** `components/tikv_util/src/quota_limiter.rs`

A three-tier token-bucket limiter enforces per-operation quotas:

- **CPU time** — consumes millicpu tokens (100ms refill interval)
- **Write bandwidth** — bytes consumed per write (1ms refill)
- **Read bandwidth** — bytes consumed per read (1ms refill)

Separate foreground (user-facing) and background (GC, compaction) limiters allow prioritizing user traffic. The `consume_sample()` async method returns a `Duration` the caller should delay.

### 4.7 Error Propagation

All busy/overload conditions produce structured errors that flow back to clients:

| Condition | Error Type | Reason String |
|-----------|-----------|---------------|
| Read pool overloaded | `ServerIsBusy` | "estimated wait time exceeds threshold" + `estimated_wait_ms` |
| Scheduler memory full | `ServerIsBusy` | "scheduler is busy" |
| GC worker overloaded | `ServerIsBusy` | "gc worker is busy" |
| Flow controller drop | Request dropped | Client retries |
| Raft message rejected | Message dropped | Sender retries via Raft protocol |

---

## 5. Connection and Session Management

### 5.1 gRPC Server Creation

**Location:** `src/server/server.rs`, `components/server/src/server.rs`

The `Server<S, E>` struct encapsulates the gRPC server:

```rust
struct Server<S, E> {
    env: Arc<Environment>,              // gRPC thread pool
    builder_or_server: Option<Either<ServerBuilder, GrpcServer>>,
    grpc_mem_quota: ResourceQuota,
    local_addr: SocketAddr,
    health_controller: HealthController,
    grpc_thread_load: Arc<ThreadLoadPool>,
    trans: ServerTransport<...>,        // Raft message transport
    snap_worker: LazyWorker<SnapTask>,  // Snapshot handler
    stats_pool: Option<Runtime>,        // Statistics collection
}
```

The gRPC environment is created with:
- Completion queue count = `grpc_concurrency` (calculated from CPU cores)
- Thread name prefix = `"grpc-server"`
- `after_start` / `before_stop` hooks for memory tracking setup/teardown

### 5.2 Server Lifecycle

1. **`new()`** — Creates `ServerBuilder` with channel arguments (window size, max streams, keepalive, compression, memory quota)
2. **`register_service()`** — Registers TikvService, DebugService, DiagnosticsService with the builder
3. **`build_and_bind()`** — Transitions builder to `GrpcServer`, binds to port
4. **`start()`** — Starts snapshot worker, starts gRPC server, initializes load statistics and memory monitoring. Sets health controller to "serving"
5. **`stop()`** — Graceful shutdown: stops snap worker → shuts down gRPC server → shuts down stats pool → shuts down health controller
6. **`pause()` / `resume()`** — Supports zero-downtime configuration updates by recreating the builder while keeping the server address

### 5.3 TLS Configuration

TLS is applied through `SecurityManager`:
- `SecurityManager::bind(server_builder, ip, port)` applies certificate settings
- Supports mutual TLS (mTLS) with certificate authority validation
- Certificate paths: `cert_path`, `key_path`, `ca_path` in security config
- Allowed Common Names: `cert_allowed_cn` restricts which clients can connect
- Certificates auto-reload when files change on disk [Inferred]

### 5.4 gRPC Thread Load Tracking

**Location:** `src/server/load_statistics/mod.rs`

```rust
struct ThreadLoadPool {
    stats: Mutex<HashMap<Pid, Arc<AtomicUsize>>>,
    threshold: usize,
    total_load: AtomicUsize,
}
```

Per-thread CPU load is tracked by parsing `/proc/stat` on Linux. The `current_thread_in_heavy_load()` method checks if the current gRPC thread exceeds the configured `heavy_load_threshold`. This signal is used by the Raft message transport to decide whether to delay batch flushes (see §7.4).

### 5.5 Request Proxying

**Location:** `src/server/proxy.rs`

TiKV supports request forwarding for network-isolated nodes:
- Uses gRPC metadata key `tikv-forwarded-host` for routing
- Maintains a `ClientPool` per destination address with load balancing across multiple connections
- Enables multi-hop forwarding through isolated regions [Inferred]

---

## 6. Status Server (HTTP)

**Location:** `src/server/status_server/mod.rs`

TiKV runs a separate HTTP server (Hyper-based, HTTP/1.1) alongside the gRPC server for metrics, profiling, and administrative operations. It runs on its own thread pool (configurable via `status_thread_pool_size`) and listens on `status_addr` (default: `127.0.0.1:10080`).

### 6.1 Endpoints

#### Metrics and Health

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/metrics` | GET | Prometheus-format metrics. Supports gzip compression via `Accept-Encoding` header |
| `/status` | GET | Basic health check, returns 200 OK |
| `/ready` | GET | Readiness check. Returns 200 if `GLOBAL_SERVER_READINESS.is_ready()`, else 500. Supports `?verbose` for JSON details |

#### CPU Profiling (pprof-compatible)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/debug/pprof/profile` | GET | CPU profiling. Params: `seconds` (default 10), `frequency` (default 99Hz). Returns SVG flamegraph or protobuf |
| `/debug/pprof/cmdline` | GET | Returns command-line arguments |
| `/debug/pprof/symbol` | GET/POST | Symbol resolution for pprof. GET returns symbol count; POST resolves hex addresses |

#### Heap Profiling

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/debug/pprof/heap` | GET | Heap profile. `?jeprof=true` for SVG output via jeprof, otherwise raw profile |

#### Configuration

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/config` | GET | Returns current configuration as JSON. `?full=true` includes hidden items |
| `/config` | POST | Updates configuration online. JSON body with key-value pairs. `?persist=true` (default) saves to disk |
| `/config/reload` | PUT | Reloads configuration from TOML file |

#### Diagnostics and Administration

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/engine_type` | GET | Returns storage engine type (e.g., "RocksDB") |
| `/region/<id>` | GET | Returns region metadata as JSON |
| `/log-level` | PUT | Changes log level dynamically. Body: `{"log_level": "debug"}` |
| `/resource_groups` | GET | Returns all resource groups with configuration |
| `/async_tasks` | GET | Dumps async task trace information |
| `/pause_grpc` | PUT | Pauses gRPC server |
| `/resume_grpc` | PUT | Resumes gRPC server |
| `/debug/ime/cached_regions` | GET | Dumps in-memory engine cached region info |
| `/force_partition_ranges` | GET/POST/DELETE | Manages force partition ranges |

#### Fail Points (test builds only)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/fail` | GET | Lists active fail points |
| `/fail/{name}` | PUT | Sets fail point actions |
| `/fail/{name}` | DELETE | Removes fail point |

### 6.2 Security

- Endpoints requiring authentication check the peer certificate's Common Name (CN) against `cert_allowed_cn`
- Public endpoints (no certificate required): `/metrics`, `/debug/pprof/profile`
- All other endpoints require valid TLS certificates when TLS is enabled
- TLS certificates auto-reload when modified

### 6.3 Lite Status Server

A lightweight variant exists for tikv-ctl batch tasks, supporting only: `/metrics`, `/debug/pprof/profile`, `/debug/pprof/heap`, `/async_tasks`.

---

## 7. Inter-Node Raft Message Transport

**Location:** `src/server/raft_client.rs`, `src/server/transport.rs`, `src/server/snap.rs`

### 7.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    RaftClient (Sender)                           │
│                                                                  │
│  send(msg) → hash region_id → select connection → push to queue │
│  flush()   → notify all dirty queues to wake senders            │
│                                                                  │
│  ConnectionPool: HashMap<(store_id, conn_id), ConnectionInfo>   │
│    ConnectionInfo { queue: Arc<Queue>, channel: Option<Channel>}│
│    Queue: ArrayQueue<(RaftMessage, Instant)> (lock-free ring)   │
│                                                                  │
│  Per-connection async task:                                      │
│    1. Resolve store address via PD                               │
│    2. Establish gRPC channel                                     │
│    3. Open batch_raft stream (fallback: raft stream)             │
│    4. Pop messages from queue → buffer                           │
│    5. Flush buffer to gRPC when full or timeout                  │
└──────────────────────────────┬──────────────────────────────────┘
                               │ gRPC stream
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│              gRPC Service (Receiver - kv.rs)                     │
│                                                                  │
│  batch_raft(stream) → for each msg: handle_raft_message()       │
│  raft(stream)       → for each msg: handle_raft_message()       │
│  snapshot(stream)   → SnapTask::Recv → write chunks to file     │
│                                                                  │
│  handle_raft_message():                                         │
│    1. Validate destination store_id matches self                 │
│    2. Check memory-based rejection filter                        │
│    3. Feed to raft router: ch.feed(msg, false)                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 Connection Management

**Connection ID selection:** Each Raft message is assigned to one of `grpc_raft_conn_num` (default: 1) connections per peer store using `seahash::hash(region_id) % grpc_raft_conn_num`. This distributes load across connections while keeping messages for the same region on the same connection (preserving ordering).

**Connection states:**

| State | Behavior |
|-------|----------|
| `Established` | Normal operation. `push()` succeeds or returns `Full` |
| `Paused` | Filtered by store allowlist. `push()` returns `Paused` |
| `Disconnected` | Permanent failure. `push()` returns `Disconnected` |

**Connection lifecycle:**
1. **Address resolution** — Async call to `StoreAddrResolver` which queries PD for the store's address. Results are cached for ~60 seconds.
2. **Channel creation** — gRPC channel with keepalive, compression, and backoff configuration. TLS applied via `SecurityManager`.
3. **RPC stream establishment** — First tries `batch_raft` (batched). Falls back to `raft` (unbatched) if the peer returns `UNIMPLEMENTED`.
4. **Reconnection** — On failure: reports unreachable to raft router, applies exponential backoff (initial 1s, max 10s), clears pending messages if address resolution fails.

**Source store identification:** gRPC metadata header `source_store_id` is injected on outgoing streams and extracted on the receiver for per-store metrics.

### 7.3 Message Batching

**Batch mode** (default, `BatchMessageBuffer`): Aggregates multiple `RaftMessage` into a single `BatchRaftMessage`. Flushed when:
- Buffer reaches `raft_msg_max_batch_size` (default: 256 messages)
- Total size exceeds `max_grpc_send_msg_len - raft_client_grpc_send_msg_buffer` (default: ~15.5 MB)
- Flush timer expires (adaptive delay based on thread load)

**Legacy mode** (`MessageBuffer`): One message per RPC. Flushes when buffer reaches 2 messages.

### 7.4 Adaptive Flush Delay

When gRPC threads are under heavy load (detected via `ThreadLoadPool`), the sender delays the next flush by `heavy_load_wait_duration`. This allows more messages to accumulate in the buffer, increasing batch efficiency at the cost of slightly higher latency.

### 7.5 Snapshot Transfer

Snapshots are **not** sent over the `batch_raft`/`raft` streams. They use a dedicated `snapshot` streaming RPC:

**Send path** (`send_snap()` in `src/server/snap.rs`):
1. Extract snapshot metadata from Raft message
2. Register as "Sending" in `SnapManager`
3. Open snapshot file, chunk into 1 MB (`SNAP_CHUNK_LEN`) pieces
4. First chunk includes the original `RaftMessage` as metadata
5. Stream chunks via `TikvClient::snapshot()` (client streaming sink)
6. Report `SnapshotStatus::Finish` or `Failure` to raft router

**Receive path** (`recv_snap()`):
1. Receive chunks via `RequestStream<SnapshotChunk>`
2. Write to temporary snapshot file
3. Save to persistent storage
4. Feed the embedded `RaftMessage` to raft router

**Concurrency control:**
- `concurrent_send_snap_limit` (default: 4) — max concurrent outgoing snapshots
- `snap_io_max_bytes_per_sec` — rate limiting for snapshot I/O
- Receiver returns `RESOURCE_EXHAUSTED` if snapshot queue is full

### 7.6 Health Checking

**Location:** `raft_client.rs` (`HealthChecker`)

Background tasks perform periodic gRPC health checks (`grpc.health.v1.Health/Check`) to each connected peer store:
- Interval: configurable via `inspect_network_interval` (0 = disabled)
- Timeout: 5 seconds per check
- Tracks maximum observed latency per store
- Latency data reported to `HealthController` for PD-based peer selection

### 7.7 Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `grpc_raft_conn_num` | 1 | Parallel gRPC connections per peer store |
| `raft_client_queue_size` | 16384 | Per-connection message queue capacity |
| `raft_msg_max_batch_size` | 256 | Max messages per `BatchRaftMessage` |
| `max_grpc_send_msg_len` | 16 MB | Max gRPC message size |
| `raft_client_grpc_send_msg_buffer` | 512 KB | Reserved buffer for message encoding overhead |
| `raft_client_max_backoff` | 10s | Max reconnection backoff delay |
| `raft_client_initial_reconnect_backoff` | 1s | Initial reconnection delay |
| `grpc_keepalive_time` | 10s | TCP keepalive ping interval |
| `grpc_keepalive_timeout` | 3s | Keepalive response timeout |

---

## 8. PD Client Protocol

**Location:** `components/pd_client/src/`

The PD (Placement Driver) client manages all communication between TiKV and the PD cluster for service discovery, timestamp allocation, and scheduling coordination.

### 8.1 PdClient Trait

The `PdClient` trait defines the protocol contract. Two implementations exist:
- **V1 (`client.rs`)** — Original RPC-based client with distributed connection management
- **V2 (`client_v2.rs`)** — Modern client with centralized reconnection loop and response caching

### 8.2 Region Heartbeat (Bidirectional Streaming)

Region leaders periodically send heartbeats to PD:

**Request (`RegionHeartbeatRequest`):**
- Region ID, epoch, leader peer
- Statistics: `written_bytes`, `written_keys`, `read_bytes`, `read_keys`, `query_stats`
- `approximate_size`, `approximate_keys` (region size estimates)
- `down_peers`, `pending_peers` (peer health)
- `cpu_stats` (CPU usage details)
- `replication_status` (disaster recovery mode status)

**Response (`RegionHeartbeatResponse`):**

PD responds with zero or one scheduling command:

| Command | Effect |
|---------|--------|
| `change_peer` / `change_peer_v2` | Add/remove replicas |
| `transfer_leader` | Move leadership to another peer |
| `split_region` | Split region at specified keys |
| `merge` | Merge with adjacent region |
| `switch_witnesses` | Change witness peers for geo-replication |

The PD worker (`components/raftstore/src/store/worker/pd.rs`) converts these commands into internal admin requests dispatched to the raftstore.

### 8.3 Store Heartbeat

Each TiKV store (not per-region) sends periodic heartbeats:

**Request (`StoreHeartbeatRequest`):**
- Store ID, address
- Capacity, available space
- Region count
- Engine statistics (bytes read/written, keys)
- CPU/IO metrics
- `StoreReport` (region split threshold estimates)
- `StoreDrAutoSyncStatus` (disaster recovery sync status)

**Response (`StoreHeartbeatResponse`):**
- `cluster_version` — used to enable/disable features based on cluster-wide version

### 8.4 Timestamp Oracle (TSO)

**Location:** `components/pd_client/src/tso.rs`

Provides monotonically increasing timestamps for transactions via bidirectional streaming RPC.

**Protocol:**
1. Client calls `get_tso(count)` which sends a request to a bounded channel (max 65536 pending)
2. Background `TimestampOracle` worker batches requests (max 64 per batch)
3. Sends `TsoRequest { cluster_id, count }` to PD
4. PD responds with `TsoResponse { timestamp: (physical, logical), count }`
5. Timestamps allocated backwards: `[logical - count + 1, ..., logical]`
6. Each client request receives its slice of the range
7. Timeout: 2 seconds per request

**Properties:**
- Monotonic: each timestamp strictly greater than previous
- Batched: multiple requests combined into one RPC
- Flow-controlled: bounded queue prevents memory exhaustion

### 8.5 Scheduling Tasks (TiKV → PD)

The `PdTask` enum (`components/raftstore/src/store/worker/pd.rs`) drives outbound communication:

| Task | Purpose |
|------|---------|
| `AskSplit` / `AskBatchSplit` | Request new region IDs for split |
| `ReportBatchSplit` | Notify PD of completed split |
| `Heartbeat` | Region heartbeat |
| `StoreHeartbeat` | Store heartbeat |
| `ReadStats` / `WriteStats` | Report traffic statistics |
| `ValidatePeer` | Check if a peer is still valid |
| `DestroyPeer` | Notify PD of peer destruction |
| `ReportMinResolvedTs` | Report store-level min resolved timestamp |
| `ReportBuckets` | Sub-region traffic statistics for hot-key balancing |
| `UpdateMaxTimestamp` | Update max timestamp for async commit correctness |
| `QueryRegionLeader` | Query which peer is the current leader |
| `StoreInfos` | CPU usage, read/write IO rates |
| `UpdateSlowScore` | Report node slowness score |
| `ControlGrpcServer` | PD-initiated gRPC server pause/resume |

### 8.6 Connection Failure Handling

**Reconnection strategy:**
- Exponential backoff starting at configured `retry_interval` (default 300ms), capped at 3 seconds
- V1: Two background loops — periodic member update (every 10 minutes) and TSO stream health monitoring
- V2: Single `reconnect_loop` coroutine with reactive reconnection on channel state changes

**Failure modes:**
1. **Network error** (gRPC UNAVAILABLE/DEADLINE_EXCEEDED) — triggers reconnection with backoff
2. **Members change** — `GetMembers` returns different topology → reconnect even if current connection works
3. **Forwarding mode** — when direct leader connection fails, connect to a PD follower which forwards requests to the leader (enabled via `enable_forwarding` config)

**Request retry:**
- Retryable errors (gRPC, cluster not bootstrapped, stream disconnect): retry with reconnection
- Non-retryable errors (incompatible, tombstone): return immediately
- Default max retries: 10 for most requests; 1 for periodic operations (heartbeats, TSO)

### 8.7 Configuration

```rust
Config {
    endpoints: Vec<String>,             // Default: ["127.0.0.1:2379"]
    retry_interval: ReadableDuration,   // Default: 300ms
    retry_max_count: isize,             // Default: -1 (infinite)
    retry_log_every: usize,             // Default: 10 (log every 10th error)
    update_interval: ReadableDuration,  // Default: 10 minutes
    enable_forwarding: bool,            // Default: false
}
```

---

## 9. Safety Invariants

1. **Cluster ID validation:** Every RPC validates that the request's cluster ID matches the server's cluster ID before processing, preventing cross-cluster data corruption.
2. **Region epoch checking:** Write operations carry region epoch; stale requests are rejected with `EpochNotMatch` error (see [Raft and Replication](raft_and_replication.md) §6).
3. **Store ID validation for Raft messages:** `handle_raft_message()` verifies `to_peer.store_id == self.store_id`. Mismatches return `StoreNotMatch` and reset the connection.
4. **Memory pressure protection:** Multiple layers (scheduler quota, Raft message rejection, gRPC memory quota) prevent OOM cascading.
5. **Latch ordering:** Scheduler latches are acquired in sorted hash order to prevent deadlocks (see [Transaction and MVCC](transaction_and_mvcc.md) §7).
6. **TSO monotonicity:** The TSO protocol guarantees strictly increasing timestamps across all transactions in the cluster.
7. **Snapshot transfer isolation:** Snapshots use dedicated streams with concurrency limits and rate limiting to prevent overwhelming the receiver.
