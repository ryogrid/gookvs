# TiKV Server Layer and Request Lifecycle Architecture

## Overview
TiKV follows a layered architecture for handling KV requests:
- **gRPC Service Layer** (src/server/service/kv.rs) - Entry point for all requests
- **Storage Layer** (src/storage/mod.rs) - Transaction processing and MVCC
- **Scheduler Layer** (src/storage/txn/scheduler.rs) - Command scheduling and latch management
- **RaftKv Engine Wrapper** (src/server/raftkv/mod.rs) - Raft replication abstraction
- **RocksDB Engine** (components/engine_rocks/) - Persistent storage

## Key Structs

### Server Struct (src/server/server.rs:140)
- Hosts gRPC server, raftstore router, snapshot workers
- Main fields:
  - `env`: gRPC environment with thread pools
  - `builder_or_server`: Lazy gRPC server
  - `raft_router`: Connection to raftstore
  - `snap_mgr`: Snapshot manager
  - `snap_worker`: Worker for snapshot operations
  - `stats_pool`: Optional Runtime for transport stats
  - `grpc_thread_load`: Thread load tracking
  - `yatp_read_pool`: YATP read pool
  - `debug_thread_pool`: Debug operations runtime

### Service Struct (src/server/service/kv.rs:111)
- Implements gRPC Tikv service trait
- Main fields:
  - `storage`: Storage instance for KV operations
  - `gc_worker`: Garbage collection worker
  - `copr`: Coprocessor endpoint
  - `snap_scheduler`: Snapshot task scheduler
  - `check_leader_scheduler`: Leader check scheduler
  - `grpc_thread_load`: Load pool for GRPC threads
  - `resource_manager`: Resource group management

### Storage Struct (src/storage/mod.rs:197)
- Primary entry point for storage operations
- Main fields:
  - `engine`: Underlying Engine (RaftKv wrapper)
  - `sched`: TxnScheduler for command execution
  - `read_pool`: ReadPoolHandle for read operations
  - `concurrency_manager`: MVCC concurrency control
  - `quota_limiter`: QoS limiter
  - `resource_manager`: Resource group manager
  - `max_key_size`: Validation limit
  - `api_version`: API version support (V1, V1ttl, V2)

### TxnScheduler Struct (src/storage/txn/scheduler.rs)
- Single-threaded event loop for command scheduling
- Uses latches to serialize access to overlapping keys
- Manages TaskContext for each command
- Coordinates with worker thread pools

## Thread Model

### Read Pools (src/storage/read_pool.rs:32)
Three separate YATP pools for different priorities:
1. `store-read-low`: Low priority reads
2. `store-read-normal`: Normal priority reads
3. `store-read-high`: High priority reads

Each pool:
- Stores TLS engine copy
- Has io_type set to ForegroundRead
- Runs custom PoolTicker for metrics flushing

### gRPC Thread Pool (src/server/server.rs)
- Created by grpcio::Environment
- Handles incoming gRPC requests
- Spawns async tasks for request processing
- Thread count configurable

### Transport Stats Pool (src/server/server.rs:192-202)
- Optional Tokio runtime if stats_concurrency > 0
- Handles transport layer statistics

### Scheduler Worker Pool
- Part of TxnScheduler (src/storage/txn/scheduler.rs)
- Executes storage commands after latch acquisition
- Managed by SchedPool

## KV Read Request Lifecycle

### 1. gRPC Handler Entry (src/server/service/kv.rs:339,1614)
```
handle_request!(kv_get, future_get, GetRequest, GetResponse)
  → future_get(storage, req) spawned as async task
  → Creates Tracker for request tracing
```

### 2. Service Layer (src/server/service/kv.rs:1614-1669)
```
future_get():
  1. Extract request context (region_id, peer, key, version)
  2. Create Tracker token for monitoring
  3. Call storage.get_entry(ctx, key, start_ts, need_commit_ts)
  4. Returns Future for async execution
```

### 3. Storage Layer (src/storage/mod.rs:625-813)
```
storage.get_entry():
  1. Extract deadline, priority, resource_limiter
  2. Prepare resource tag for metering
  3. Call read_pool_spawn_with_busy_check() to spawn on read pool
  
  On read pool thread:
  4. Check deadline
  5. Validate API version
  6. Create snap_ctx from region snapshot
  7. Call Self::snapshot(engine, snap_ctx) to get snapshot
  8. Create SnapshotStore with MVCC parameters
  9. Call snap_store.get_entry(&key, load_commit_ts, &stats)
  10. Collect scan details and flow metrics
  11. Apply quota limiter if configured
  12. Return (ValueEntry, KvGetStatistics)
```

### 4. Engine Layer (RaftKv wrapper)
```
RaftKv.snapshot(snap_ctx):
  1. Check region epoch and leader
  2. Use LocalReadRouter for consistent reads
  3. Returns snapshot from RocksDB or in-memory cache
```

### 5. MVCC Read (src/storage/mvcc/)
```
SnapshotStore.get_entry():
  1. Check bypass_locks and access_locks sets
  2. Scan MVCC versions at snapshot timestamp
  3. Resolve locks if needed
  4. Return value if found and visible
  5. Collect statistics (keys scanned, bytes read)
```

### 6. Response Construction
```
Service returns GetResponse:
  1. Extract value from ValueEntry
  2. Set commit_ts if needed
  3. Add scan details v2
  4. Add time details (wait, process, schedule times)
  5. Add RU metrics
  6. Send via gRPC sink
```

### Key Timing Measurements
- `schedule_wait_time`: Time waiting to be scheduled on read pool
- `snapshot_wait_time`: Time acquiring region snapshot
- `process_wall_time`: Time spent in MVCC read
- `wait_wall_time`: Total wait before processing
- `total_rpc_wall_time`: Complete RPC duration

## KV Write Request Lifecycle (Two-Phase Commit)

### Phase 1: Prewrite (src/server/service/kv.rs:2465-2471)

#### 1. gRPC Handler Entry
```
kv_prewrite() → future_prewrite(storage, req)
  → txn_command_future! macro:
     - Convert PrewriteRequest to TypedCommand
     - Create Tracker token
     - Call storage.sched_txn_command(cmd, callback)
     - Returns paired_future_callback() future
```

#### 2. Storage Scheduler (src/storage/mod.rs:1857-1933)
```
storage.sched_txn_command():
  1. Validate key sizes
  2. Create Command enum from TypedCommand
  3. Check API version
  4. Call self.sched.run_cmd(cmd, callback)
  5. Return immediately (async execution)
```

#### 3. TxnScheduler (src/storage/txn/scheduler.rs)
```
TxnScheduler.run_cmd():
  1. Enqueue command to internal queue
  2. Event loop processes commands:
     a. Acquire latches for keys in command
        - Latches ensure serial access to overlapping keys
        - Prevents write-write conflicts at command level
     b. Create TaskContext with:
        - Command, callback, lock (latch)
        - Owned flag for callback ownership
     c. Spawn execution on worker pool
```

#### 4. Worker Thread Execution
```
Task execution (after latches acquired):
  1. Create snapshot at command start timestamp
  2. For Prewrite command:
     a. Get concurrency locks for keys
     b. Check transaction state (not already committed)
     c. For each mutation:
        - Write lock to CF_LOCK
        - Write data value (if mutation type is Put/Insert)
     d. Collect write batch
     e. Track min_commit_ts (if one-pc enabled)
  3. Call engine.write(snap_ctx, write_batch, callback)
     - This goes to RaftKv which:
       * Wraps in RaftCmdRequest with CmdType::Write
       * Routes to raftstore leader
       * Queues for replication
  4. On write callback:
     a. Return PrewriteResult with min_commit_ts, one_pc_commit_ts
     b. Return locks that couldn't be written (conflicts)
     c. Call user callback with result
```

#### 5. Raft Replication
```
RaftKv routes write via StoreMsg::RaftCmd:
  1. Wraps in RaftCmdRequest
  2. Routes to region peer FSM
  3. Leader appends to Raft log
  4. Followers replicate via Raft consensus
  5. On quorum:
     a. Leader applies to state machine
     b. Calls on_applied callback
     c. Applies modifies to RocksDB in batch
     d. Returns response to client
```

#### 6. Response (src/server/service/kv.rs:2465-2471)
```
Response construction:
  1. Set min_commit_ts from PrewriteResult
  2. Set one_pc_commit_ts (if applicable)
  3. Set errors for any lock conflicts
  4. Add exec details v2
  5. Send via gRPC
```

### Phase 2: Commit (src/server/service/kv.rs:2517-2525)

#### Similar flow as Prewrite:
```
future_commit():
  1. txn_command_future! macro → storage.sched_txn_command()
  2. TxnScheduler acquires latches for keys
  3. Commit command:
     a. Get transaction status
     b. Write commit record to CF_WRITE
     c. Batch apply to RocksDB
  4. On success, return TxnStatus::Committed { commit_ts }
  5. Construct response with commit_ts
```

## Request Batching (Batch Commands RPC)

### Batch Processing (src/server/service/kv.rs:1381-1539)
- Multiple requests in single RPC
- Uses handle_cmd! macro to route each request type
- Requests routed through:
  - `ReqBatcher` (if enabled and batchable)
    * Groups similar requests
    * Defers execution for better batching
  - Direct spawning on appropriate pool
- Responses collected into BatchCommandsResponse
- Exec details collected per-request
- Metrics recorded per-request

### Request Batcher
- Can batch Get requests
- Can batch RawGet requests
- Improves cache locality and throughput
- Controlled by `enable_request_batch` config

## Key Design Patterns

### 1. Latch System (src/storage/txn/latch.rs)
- Ensures serial execution of overlapping commands
- Uses hash-based locks on key ranges
- Two-level: checking locks, then acquiring locks
- Prevents write-write and read-write conflicts at command level

### 2. Callback-based Async (src/server/service/kv.rs:2442-2451)
```
paired_future_callback():
  - Returns (Callback, Future) pair
  - Callback sends result to Future
  - Future awaits in async context
  - Enables Rust async/await style
```

### 3. Thread-Local Engine (src/storage/read_pool.rs:50-57)
```
Each read pool thread:
  1. Sets TLS engine via after_start callback
  2. Can access via Self::with_tls_engine()
  3. Destroyed on thread stop via before_stop callback
  4. Enables lock-free engine access in thread
```

### 4. Resource Control
- ResourceGroupManager manages resource groups
- ResourceLimiter enforces quotas per group
- TaskMetadata tracks request source
- Quotas applied to:
  - CPU (via quota_limiter)
  - I/O (via flow controller)
  - Memory (via memory quota)

### 5. Metrics and Tracking
- GLOBAL_TRACKERS for per-request tracking
- Tracker collects:
  - Scan details (keys read, bytes read)
  - Write details (keys written, bytes written)
  - Time details (wall time, CPU time)
  - RU metrics for resource metering
- Flushed per operation via PoolTicker

## Critical Paths

### Read Critical Path
gRPC → Storage.get_entry() → read_pool → MVCC snapshot read → Response

### Write Critical Path
gRPC → Storage.sched_txn_command() → TxnScheduler latches → Worker pool → RaftKv.write() → Raft replication → Apply → Response

### Batching Critical Path
gRPC → batch_commands() → ReqBatcher (optional) → Route per-request → Collect responses → Batch response

## Configuration Points
- `grpc_compression_type`: gRPC compression algorithm
- `grpc_stream_initial_window_size`: gRPC flow control
- `grpc_concurrent_stream`: Max concurrent streams
- `grpc_memory_pool_quota`: gRPC memory quota
- `enable_request_batch`: Enable batch command processing
- `stats_concurrency`: Transport stats thread count
- `heavy_load_threshold`: Load threshold for load tracking
- `storage.read_pool`: Read pool size per priority
- `quota_limiter`: CPU quota configuration
- `resource_control`: Resource group configuration
