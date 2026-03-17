# CDC and Backup

This document specifies the Change Data Capture (CDC), full backup, log backup (PITR), and resolved timestamp subsystems. These components share a common raftstore observer pattern for capturing data changes and cooperate to provide real-time change streaming, point-in-time backup, and consistent read timestamps.

## 1. CDC Architecture

### 1.1 Overview

CDC captures data changes from TiKV's Raft apply path and streams them to downstream consumers (primarily TiCDC). The architecture follows a producer-consumer pattern with three layers:

1. **Observer Layer** (`CdcObserver`): Hooks into raftstore to capture applied commands
2. **Delegate Layer** (`Delegate`): Per-region event processing, filtering, and old value retrieval
3. **Service/Endpoint Layer**: Manages client connections, task coordination, and event distribution

**Key source files:**
- `components/cdc/src/observer.rs` — Raftstore integration
- `components/cdc/src/delegate.rs` — Per-region event processing
- `components/cdc/src/endpoint.rs` — Task coordination
- `components/cdc/src/service.rs` — gRPC service
- `components/cdc/src/channel.rs` — Event channels and batching
- `components/cdc/src/old_value.rs` — Old value fetching and caching
- `components/cdc/src/initializer.rs` — Snapshot scanning for initialization

### 1.2 Event Generation from Raft Apply

CDC events originate from the raftstore's apply path. The `CdcObserver` implements the `CmdObserver` trait and is registered with the `CoprocessorHost`:

```
Raft Apply (state machine)
    ↓
CdcObserver::on_flush_applied_cmd_batch()
    ├── Creates RegionSnapshot (prevents old value GC during processing)
    ├── Creates old_value_cb closure (captures snapshot)
    ├── Allocates memory quota for the batch
    └── Schedules Task::MultiBatch { multi: Vec<CmdBatch>, old_value_cb }

Endpoint worker thread (single-threaded, FIFO)
    ↓
For each CmdBatch in MultiBatch:
    ├── Looks up or creates Delegate for region_id
    └── Calls delegate.on_batch(batch, old_value_cb)

Delegate::on_batch()
    ├── Validates batch belongs to this delegate (ObserveId match)
    ├── Extracts mutations from each Cmd (Puts, Deletes, Locks)
    ├── Calls old_value_cb for each mutation needing old values
    ├── Creates EventRow for each KV change:
    │   ├── Key, Value (post-mutation)
    │   ├── Old Value (fetched from snapshot)
    │   ├── Operation type (Put/Delete)
    │   ├── Start TS, Commit TS
    │   └── Txn source (BDR/PITR tracking)
    ├── Updates lock tracking for resolved TS
    └── Calls apply_to_downstreams() to fan out events
```

**Observer registration priorities** [Inferred]:
- `CmdObserver`: priority 0 (highest priority, runs first)
- `RoleObserver`: priority 100
- `RegionChangeObserver`: priority 100

### 1.3 Delegate Model

Each observed region has a `Delegate` that manages event processing and multiple downstream subscribers:

```
Delegate {
    region_id: u64,
    handle: ObserveHandle,              // Link to raftstore observer (ObserveId for ABA protection)
    memory_quota: Arc<MemoryQuota>,     // Global memory quota
    lock_tracker: LockTracker,          // Transaction lock tracking for resolved TS
    downstreams: Vec<Downstream>,       // Multiple downstream subscribers per region
    txn_extra_op: Arc<AtomicCell<TxnExtraOp>>,
    failed: bool,                       // Marks delegate as failed (no more events)
}
```

**LockTracker** tracks in-flight transactions for resolved timestamp computation:

```
LockTracker enum:
    Pending                                   // No locks tracked yet
    Preparing(Vec<MiniLock>)                  // Collecting locks from commands
    Prepared { locks: BTreeMap<Key, isize> }  // Final lock set for resolved TS
```

#### Downstream State Machine

Each `Downstream` (one per client subscription per region) follows this state machine:

```
Uninitialized ──[scan starts]──> Initializing ──[scan completes]──> Normal ──[error/deregister]──> Stopped
       │                              │                                │
       └──────────────────────────────┴────────────────────────────────┴──[error/deregister]──> Stopped
```

**State semantics:**
- `Uninitialized`: Created but incremental scan not yet started. No events accepted.
- `Initializing`: Incremental scan in progress. Change events accepted (from Raft apply) but resolved TS events not sent.
- `Normal`: Fully operational. Both change events and resolved TS events flow to client.
- `Stopped`: Terminal state after error or deregistration.

State transitions use `AtomicCell<DownstreamState>` for lock-free concurrent access [Inferred].

#### Downstream Struct

```
Downstream {
    id: DownstreamId,                     // Unique identifier
    peer: String,                         // Client IP address
    region_epoch: RegionEpoch,            // Region version at subscription time
    conn_id: ConnId,                      // Connection identifier
    kv_api: ChangeDataRequestKvApi,       // TiDB or RawKV mode
    filter_loop: bool,                    // Filter looped transactions (BDR)
    observed_range: ObservedRange,        // Key range filter [start_key, end_key)
    sink: Option<Sink>,                   // Event channel to client
    state: Arc<AtomicCell<DownstreamState>>,
    scan_truncated: Arc<AtomicBool>,      // Scan interrupted flag
    lock_heap: Option<BTreeMap<TimeStamp, isize>>,  // Per-downstream lock tracking
    advanced_to: TimeStamp,               // Latest resolved TS sent to this downstream
}
```

### 1.4 Event Filtering and Assembly

Events pass through multiple filtering stages before reaching the client:

#### Per-Downstream Filtering

```
apply_to_downstreams(entries: Vec<EventRow>)
    └── For each downstream:
        ├── Check state: ready_for_change_events()? (Initializing or Normal)
        ├── Apply ObservedRange filter: start_key <= key < end_key
        ├── Apply KvApi filter: TiDB vs RawKV mode
        ├── Apply filter_loop: skip if txn_source != 0 && filter_loop (BDR loop detection)
        └── Send filtered EventEntries to downstream's Sink
```

#### Event Batching

Events are batched at multiple levels for efficiency:

1. **EventBatcher** (per-connection):
   - Maximum 64 events per batch (`CDC_EVENT_MAX_COUNT`)
   - Maximum 6 MB per response (`CDC_RESP_MAX_BYTES`)
   - ResolvedTs events batched separately from data events

2. **Channel level** (Sink/Drain pair):
   - Unbounded channel for observed events (from Raft apply, force-allocated)
   - Bounded channel (128 capacity) for scanned events (from initializer)
   - Memory quota enforcement per event

3. **gRPC level**:
   - Flow control via HTTP/2 window size
   - Batched message writes with `WriteFlags`

### 1.5 CDC Streaming Protocol

#### Connection Lifecycle

```
Client calls EventFeed RPC
    ↓
Service::handle_event_feed()
    ├── Create ConnId (unique per connection)
    ├── Create Sink/Drain channel pair
    ├── Parse HTTP/2 headers for features
    ├── Schedule Task::OpenConn
    ├── Spawn recv_req task:
    │   ├── Read first request (extract version)
    │   ├── Schedule Task::SetConnVersion
    │   └── Process subsequent requests (Register/Deregister)
    ├── Spawn send_events task:
    │   ├── Forward Drain to gRPC server-streaming sink
    │   └── Track last_flush_time
    └── Start connection watchdog (idle detection)
```

#### Feature Gates

Protocol capabilities negotiated via headers [Inferred]:

| Feature | Min Version | Description |
|---------|------------|-------------|
| `BATCH_RESOLVED_TS` | v4.0.8 | Batch resolved TS events |
| `VALIDATE_CLUSTER_ID` | v5.3.0 | Cluster ID validation |
| `STREAM_MULTIPLEXING` | explicit | Multiple regions per stream |

#### Task Types

The CDC endpoint processes these task variants:

```
Task::Register { request, downstream }     // Subscribe to a region
Task::Deregister(Deregister)               // Unsubscribe (region/request/connection level)
Task::OpenConn { conn }                    // New client connection
Task::SetConnVersion { conn_id, version }  // Set protocol version
Task::MultiBatch { multi, old_value_cb }   // Process Raft apply events
Task::MinTs { regions, min_ts }            // Advance resolved timestamp
Task::ChangeConfig(ConfigChange)           // Dynamic configuration update
Task::Validate(Validate)                   // Validation queries

Deregister variants:
    Region { conn_id, request_id, region_id }  // Single region
    Request { conn_id, request_id }            // Single request
    Delegate { region_id, observe_id, err }    // All downstreams for region
    Conn(conn_id)                              // All subscriptions for connection
```

### 1.6 Incremental Scan (Initializer)

When a downstream subscribes to a region, an `Initializer` performs a snapshot scan to establish the starting point before incremental events flow:

```
Initializer flow:
    1. Capture snapshot at current timestamp
    2. Scan KV range [start_key, end_key) in batches
    3. Convert existing KV pairs to EventRows
    4. Send to downstream via bounded sink channel
    5. On completion: transition downstream Initializing → Normal
    6. Enable resolved TS advancement for this downstream
```

During initialization, incremental events from Raft apply are still processed (the downstream accepts change events in `Initializing` state). This ensures no events are lost between the snapshot point and the start of incremental streaming.

### 1.7 Old Value Handling

CDC needs old values (pre-mutation values) for downstream consumers to construct complete change events.

#### Old Value Cache

```
OldValueCache {
    cache: LruCache<Key, (OldValue, Option<MutationType>)>,
    access_count, miss_count, miss_none_count, update_count,  // Metrics
}

OldValue enum:
    None                        // Key didn't exist before mutation
    Value { value }             // Immediate value (small, < 1KB) [Inferred]
    ValueTimeStamp { start_ts } // Reference to previous version's timestamp
    SeekWrite(WriteRef)         // Write metadata reference
    Unspecified                 // Unknown / not yet fetched
```

#### Old Value Fetch Flow

```
on_flush_applied_cmd_batch()
    └── Creates snapshot (prevents GC during processing)

delegate.on_batch(old_value_cb)
    └── For each Put/Delete mutation:
        └── Call old_value_cb(key, query_ts, cache, stats)

old_value_cb flow:
    1. Check LRU cache (hit → return cached value)
    2. On cache miss:
       a. Seek in CF_WRITE for the key at query_ts
       b. Check GC fence (value must not be GC'd)
       c. Fetch from CF_DEFAULT if needed (for long values)
    3. Update cache and metrics
```

**Old value scenarios:**
- **Insert**: old_value = None (key didn't exist)
- **Update (Put)**: old_value = previous value
- **Delete**: old_value = current value before deletion
- **Lock-only**: old_value = None (no data change)

### 1.8 Transaction Source Tracking

`TxnSource` is a bitmap that tracks the origin of transactions for BDR (Bidirectional Replication) and other purposes:

```
TxnSource(u64) bitfield:
    Bits 0-7:   CDC write source ID (1-255) for BDR sync loop detection
    Bits 8-15:  Lossy DDL reorg backfill tracking
    Bit 16:     Lightning physical import mode flag
    Bits 17-63: Reserved
```

When `filter_loop` is enabled on a downstream and `txn_source != 0`, the event is skipped to prevent infinite replication loops in BDR setups.

### 1.9 Memory Quota

CDC enforces memory limits at multiple layers:

```
Global MemoryQuota (shared across all regions)
    ├── Allocated during on_flush_applied_cmd_batch() (force allocation)
    ├── Freed after endpoint processes MultiBatch
    │
Sink Memory Quota (per connection)
    ├── Unbounded channel: force=true (observed events never dropped)
    ├── Bounded channel: standard quota (scanned events can backpressure)
    │
OldValueCache Quota
    ├── Configurable capacity
    ├── LRU eviction when full
    │
Connection Watchdog
    ├── Warning at 60s idle
    ├── Force deregister at 1200s (20 min) idle + >99.9% quota used
```

### 1.10 Error Handling and Deregistration

Deregistration triggers:
1. **Leader election**: NotLeader error → deregister all downstreams for region
2. **Region split/merge/destroy**: RegionNotFound error → deregister region
3. **Client request**: Explicit deregister from client
4. **Connection close**: Deregister all subscriptions for connection
5. **Delegate failure**: Repeated errors mark delegate as failed

Error propagation follows the observer pattern: `CdcObserver` receives raftstore events (role changes, region changes) and schedules `Task::Deregister` with appropriate error information for the endpoint to handle.

---

## 2. Raftstore Observer Pattern

CDC, backup-stream, and resolved_ts all use the same raftstore coprocessor observer pattern to capture state changes. The `CoprocessorHost` in `components/raftstore/src/coprocessor/mod.rs` provides the registration infrastructure.

### 2.1 Observer Traits

Three traits define the observer interface:

#### CmdObserver

```rust
pub trait CmdObserver<E>: Coprocessor {
    /// Called after flushing applied commands to the state machine.
    /// Receives the full batch of commands that were applied.
    fn on_flush_applied_cmd_batch(
        &self,
        max_level: ObserveLevel,
        cmd_batches: &mut Vec<CmdBatch>,
        engine: &E,
    );

    /// Called when leader first applies on its term.
    fn on_applied_current_term(&self, role: StateRole, region: &Region);
}
```

#### RoleObserver

```rust
pub trait RoleObserver: Coprocessor {
    /// Called when peer role changes (Leader/Follower/Candidate).
    /// Note: not called in realtime — there may be a delay.
    fn on_role_change(&self, ctx: &mut ObserverContext<'_>, change: &RoleChange);
}
```

#### RegionChangeObserver

```rust
pub trait RegionChangeObserver: Coprocessor {
    /// Called when a region changes on this TiKV (split, merge, destroy, update).
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    );

    /// Called before writing to KvEngine. Returns false to block the write.
    fn pre_persist(&self, ctx: &mut ObserverContext<'_>, is_finished: bool, cmd: Option<&RaftCmdRequest>) -> bool;
}
```

### 2.2 ObserveLevel

Each region subscription specifies an `ObserveLevel` that determines what data is captured:

```
ObserveLevel:
    None   — No observation
    LockRelated — Only CF_LOCK and CF_WRITE operations (used by resolved_ts)
    All    — All column family operations including CF_DEFAULT (used by CDC and backup-stream)
```

The maximum observe level across all registered observers determines what data the raftstore includes in `CmdBatch`. When only resolved_ts is observing (no CDC or backup-stream), `max_level = LockRelated` and CF_DEFAULT data is omitted to save memory [Inferred].

### 2.3 Registration

Each observer registers with the `CoprocessorHost` with a priority. Lower priority numbers execute first:

| Component | CmdObserver Priority | RoleObserver Priority | RegionChangeObserver Priority |
|-----------|--------------------|--------------------|------------------------------|
| CDC | 0 | 100 | 100 |
| Backup-stream | (CmdObserver) | 100 | 100 |
| Resolved TS | 1000 | 100 | 100 |

Resolved TS uses priority 1000 (lowest) for `CmdObserver` to ensure it runs last and sees the complete command batch after CDC and backup-stream have processed it [Inferred].

### 2.4 CmdBatch Structure

```
CmdBatch {
    observe_id: ObserveId,     // Unique subscription identifier (ABA protection)
    region_id: u64,
    cmds: Vec<Cmd>,            // Applied commands
}

Cmd {
    index: u64,                // Raft log index
    term: u64,                 // Raft term
    request: RaftCmdRequest,   // The command
    response: RaftCmdResponse, // The result
}
```

The `ObserveId` prevents ABA problems: if a region is deregistered and re-registered, a new `ObserveId` is generated so stale batches from the old subscription are discarded.

---

## 3. Full Backup

### 3.1 Overview

The full backup component (`components/backup/`) exports consistent snapshots of TiKV data to external storage as SST files. It operates on a per-region basis with configurable concurrency.

**Key source files:**
- `components/backup/src/endpoint.rs` — Backup task coordination
- `components/backup/src/writer.rs` — SST file writing
- `components/backup/src/service.rs` — gRPC service
- `components/backup/src/disk_snap.rs` — Disk snapshot support
- `components/backup/src/utils.rs` — Key encoding and API version conversion
- `components/backup/src/softlimit.rs` — CPU-based concurrency adjustment

### 3.2 Backup Request Pipeline

```
BackupRequest (gRPC)
    ↓
Task::new() — Parse request with key range, timestamps, compression, encryption
    ↓
Endpoint::handle_backup_task()
    ├── Validate API version compatibility (V1/V1TTL/V2)
    ├── Flush causal timestamps for RawKV backups
    ├── Create ExternalStorage backend (S3, GCS, Azure, local, HDFS)
    └── Spawn N backup worker tasks (N = configured num_threads)

Each worker:
    ↓
Progress::forward() — Find next region to backup
    ├── Seek regions from RegionInfoProvider
    ├── Filter by peer availability and role
    └── Return BackupRange
    ↓
BackupRange::backup() (TxnKV) or backup_raw_kv_to_file() (RawKV)
    ├── Capture engine snapshot (consistent view)
    ├── Create scanner (TxnEntryScanner or CursorBuilder)
    └── Feed entries to BackupWriter
    ↓
BackupWriter — Buffer entries into in-memory SST files
    ├── Separate CF_DEFAULT and CF_WRITE column families
    ├── Add data_key prefix (z) to keys
    ├── Calculate CRC64-XOR checksum
    └── Split files when exceeding sst_max_size
    ↓
Writer::save_and_build_file()
    ├── writer.finish_read() — Get in-memory SST as reader
    ├── EncrypterReader — Encrypt with AES-256-CTR (IV per file)
    ├── Sha256Reader — Compute integrity hash
    ├── Limiter — Rate-limit upload
    └── ExternalStorage::write() — Upload to backend
    ↓
File protobuf — Metadata (name, sha256, crc64xor, cipher_iv, sizes)
    ↓
save_backup_file_worker — Batch completed files
    ↓
BackupResponse (gRPC streaming) — Stream results to client
```

### 3.3 SST File Writing

**BackupWriter** manages two internal `Writer` instances, one per column family:

```
BackupWriter<EK: KvEngine> {
    name: String,
    default: Writer<SstWriter>,     // CF_DEFAULT (user values)
    write: Writer<SstWriter>,       // CF_WRITE (transaction metadata)
    rate_limiter: Limiter,
    sst_max_size: u64,
    cipher: CipherInfo,
}

Writer<W: SstWriter> {
    writer: W,                      // In-memory SST writer
    total_kvs: u64,                 // KV pair count
    total_bytes: u64,               // Total byte count
    checksum: u64,                  // Running CRC64-XOR
    digest: crc64fast::Digest,      // Checksum calculator
}
```

**File naming convention:**
```
{store_id}_{region_id}_{sha256_hash(start_key)}_{backend_name}.sst
```

**For RawKV backups**, a separate `BackupRawKvWriter` handles single-CF writes with value validation:
- Checks TTL expiration
- Filters deleted keys
- Handles API version conversion (V1 → V2 uses `BACKUP_V1_TO_V2_TS = 1` as causal timestamp)

### 3.4 External Storage Abstraction

The `ExternalStorage` trait (`components/external_storage/src/lib.rs`) provides a unified interface for cloud and local storage:

```rust
pub trait ExternalStorage: 'static + Send + Sync {
    fn name(&self) -> &'static str;
    fn url(&self) -> io::Result<Url>;

    // Write a file to storage
    async fn write(&self, name: &str, reader: UnpinReader<'_>, content_length: u64) -> io::Result<()>;

    // Read a file (full or partial)
    fn read(&self, name: &str) -> ExternalData<'_>;
    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_>;

    // Restore a file with decompression/decryption
    async fn restore(&self, storage_name: &str, restore_name: PathBuf, expected_length: u64,
                     speed_limiter: &Limiter, restore_config: RestoreConfig) -> io::Result<()>;

    // List and delete
    fn iter_prefix(&self, prefix: &str) -> LocalBoxStream<'_, Result<BlobObject>>;
    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>>;
}
```

**Backend implementations:**
- `LocalStorage` — Local filesystem
- `HdfsStorage` — Hadoop Distributed File System
- Cloud blob storage — S3, GCS, Azure (via `cloud::blob` module)
- `NoopStorage` — No-operation (testing)

**RestoreConfig** supports:
- Byte range reads
- Zstd decompression
- SHA256 checksum verification (plaintext and encrypted)
- File decryption metadata

### 3.5 Disk Snapshot Support

Modern backup approach using the Raft snapshot mechanism for faster, more consistent backups:

```
Env<SR: SnapshotBrHandle> {
    handle: SR,                              // Snapshot BR handle
    rejector: Arc<PrepareDiskSnapObserver>,   // Prevents writes during snapshot
    active_stream: Arc<AtomicU64>,           // Active stream count
    runtime: Handle or Arc<Runtime>,
}
```

**Disk snapshot flow (StreamHandleLoop):**

1. **UpdateLease**: Client periodically refreshes backup lease duration. Server validates lease hasn't expired. Prevents regions from applying writes during backup.

2. **WaitApply**: Broadcasts `SnapshotBrWaitApplyRequest` to leader peers. Creates syncer with oneshot channel. Waits for write buffer flush completion.

3. **Finish**: Resets lease state and closes gRPC stream.

**Abort reasons:**
- `EpochNotMatch`: Region configuration changed during backup
- `StaleCommand`: Leadership changed during backup
- Connection errors

### 3.6 Resource Management

**SoftLimit** (CPU-based concurrency adjustment):

```
SoftLimitByCpu<Statistics: CpuStatistics> {
    metrics: Statistics,
    total_time: f64,
    keep_remain: usize,           // Reserved cores for non-backup work
}
```

- Dynamically adjusts worker count based on CPU usage
- `SoftLimitKeeper` runs in background, periodically recalculates quota
- Configuration: `enable_auto_tune`, `auto_tune_refresh_interval`, `auto_tune_remain_threads`
- Uses `tokio::sync::Semaphore` for concurrent task gating

---

## 4. Backup-Stream (PITR / Log Backup)

### 4.1 Overview

The backup-stream component (`components/backup-stream/`) implements continuous log backup for Point-In-Time Recovery (PITR). It captures incremental changes from TiKV and writes them to external storage, enabling restoration to any point in time.

**Key source files:**
- `components/backup-stream/src/endpoint.rs` — Main orchestrator
- `components/backup-stream/src/subscription_manager.rs` — Initial scan coordination
- `components/backup-stream/src/subscription_track.rs` — Region state machine and lock tracking
- `components/backup-stream/src/event_loader.rs` — Snapshot scanning
- `components/backup-stream/src/checkpoint_manager.rs` — Checkpoint lifecycle
- `components/backup-stream/src/router.rs` — Event routing and batching
- `components/backup-stream/src/tempfiles.rs` — Temporary file buffering
- `components/backup-stream/src/observer.rs` — Raftstore observer
- `components/backup-stream/src/metadata/` — Metadata storage (etcd)

### 4.2 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    gRPC Service Layer                         │
│  BackupStreamGrpcService: flush_now, get_checkpoint, subscribe│
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│                    Endpoint (Main Orchestrator)               │
│  - Task scheduling and routing                               │
│  - Region observation lifecycle                              │
│  - Checkpoint and subscription coordination                  │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
  ┌──────────┐ ┌──────────┐ ┌──────────────────┐
  │ Observer │ │  Router  │ │  Subscription    │
  │ (events) │ │ (batch)  │ │  Manager (scan)  │
  └──────────┘ └──────────┘ └──────────────────┘
                     │
        ┌────────────┼────────────────────────┐
        ▼            ▼                        ▼
  ┌──────────┐ ┌──────────────┐ ┌────────────────────┐
  │ Temp     │ │ Checkpoint   │ │ Metadata Client    │
  │ Files    │ │ Manager      │ │ (etcd persistence) │
  └──────────┘ └──────────────┘ └────────────────────┘
```

### 4.3 Endpoint Task Types

```
Task::ModifyObserve(ObserveOp)              // Start/Stop/Destroy region observation
Task::BatchEvent(Vec<CmdBatch>)             // Process batch of KV events from raftstore
Task::ForceFlush(TaskSelector, Sender)      // Trigger flush to external storage
Task::RegionCheckpointsOp(op)               // Checkpoint query/update operations
Task::RefreshResolver(...)                  // Update region checkpoint information
Task::ChangeConfig(cfg)                     // Dynamic configuration update

ObserveOp variants:
    Start { region }                        // Begin observing a region
    Stop { region }                         // Stop observing
    Destroy { region }                      // Deregister region permanently
    RefreshResolver { region }              // Update resolver state
```

### 4.4 Subscription Tracking and State Machine

Each region follows a state machine for observation:

```
Absent ──[Start]──> Pending ──[scan complete]──> Running (Active)
  ▲                    │                            │
  └────[Stop]──────────┴─────────[Stop]─────────────┘
```

**SubscribeState:**
```
Pending(Region)                  // Preparing for observation, initial scan queued
Running(ActiveSubscription)      // Active observation with resolver

ActiveSubscription {
    meta: Region,
    handle: ObserveHandle,       // From coprocessor
    resolver: TwoPhaseResolver,  // Lock tracker
}
```

`SubscriptionTracer` uses `DashMap<u64, SubscribeState>` for thread-safe region state tracking.

#### TwoPhaseResolver

The `TwoPhaseResolver` handles the concurrency challenge between initial scanning (phase 1) and incremental events (phase 2):

```
TwoPhaseResolver {
    resolver: Resolver,                    // Base resolver from resolved_ts component
    future_locks: Vec<FutureLock>,         // Locks from phase 2 buffered during phase 1
    stable_ts: Option<TimeStamp>,          // If Some, phase 1 is still active
}

FutureLock:
    Lock(key, start_ts, generation)        // New lock from incremental event
    Unlock(key)                            // Lock release from incremental event
```

**Phase 1 (initial scan):**
- `track_phase_one_lock()`: Track locks found during scanning
- Incremental locks/unlocks from Raft apply are buffered in `future_locks`
- `resolve()` returns `min(stable_ts, resolver.resolve())` — capped at scan start point

**Phase 2 (incremental):**
- `phase_one_done()`: Transition to phase 2. Replays all buffered `future_locks` into the resolver.
- `track_lock()` / `untrack_lock()`: Directly update resolver
- `resolve()` returns resolver's resolved TS without cap

### 4.5 Event Loading (Initial Scanning)

```
EventLoader<S: Snapshot> {
    scanner: DeltaScanner<S>,              // MVCC delta scanner
    region: Region,
    entry_batch: Vec<TxnEntry>,            // Batch buffer (1024 entries)
}
```

**InitialDataLoader** manages the scan lifecycle:

```
InitialDataLoader flow:
    1. Acquire concurrency semaphore permit
    2. Capture snapshot from coprocessor (with retry, up to TRY_START_OBSERVE_MAX_RETRY_TIME=24)
    3. Create EventLoader with DeltaScanner for region's key range
    4. Batch entries (1024 at a time) with memory quota tracking
    5. Convert TxnEntry to ApplyEvent:
       - Prewrite entries → emit default CF value + track lock
       - Commit entries → emit write CF + default CF values
    6. Send events to Router
    7. On completion: call phase_one_done() on TwoPhaseResolver
    8. Transition region to Running state
```

**Retry backoff:** Exponential from 1s to 16s max, up to 24 attempts. OOM backoff: 60s with jitter.

### 4.6 Event Routing and Processing

#### ApplyEvents

```
ApplyEvents {
    events: Vec<ApplyEvent>,
    region_id: u64,
    region_resolved_ts: u64,
}

ApplyEvent {
    key: Vec<u8>,
    value: Vec<u8>,
    cf: CfName,            // CF_DEFAULT or CF_WRITE (CF_LOCK handled separately)
    cmd_type: CmdType,     // Put or Delete
}
```

`ApplyEvents::from_cmd_batch()` converts `CmdBatch` to events and updates the resolver with lock changes. CF_LOCK operations are processed for lock tracking but not emitted as data events [Inferred].

#### TaskSelector

Routes operations to specific tasks:
```
TaskSelector:
    ByName(String)                 // Specific task
    ByKey(Vec<u8>)                 // Single key
    ByRange(Vec<u8>, Vec<u8>)      // Key range
    All                            // All tasks
```

### 4.7 Checkpoint Management

The `CheckpointManager` implements a three-phase checkpoint lifecycle:

```
Phase 1: resolved_ts (incoming, unfrozen)
    ↓ freeze()
Phase 2: frozen_resolved_ts (about to be flushed)
    ↓ flush_and_notify()
Phase 3: checkpoint_ts (flushed, persisted)
```

```
CheckpointManager {
    checkpoint_ts: HashMap<u64, LastFlushTsOfRegion>,      // Flushed checkpoints
    frozen_resolved_ts: HashMap<u64, LastFlushTsOfRegion>,  // About-to-flush
    resolved_ts: HashMap<u64, LastFlushTsOfRegion>,         // Incoming (unfrozen)
    manager_handle: Option<Sender<SubscriptionOp>>,         // To subscription manager
}
```

**Subscriber notification:**
- Subscribers connect via `subscribe_flush_event` gRPC
- `FlushEvent` broadcast groups events by subscriber (1024 per response)
- Failed subscribers are automatically removed

#### CheckpointType

```
CheckpointType:
    MinTs                                       // No in-flight transactions
    StartTsOfInitialScan                        // Initial scan still running
    StartTsOfTxn(Option<(TimeStamp, TxnLocks)>) // Oldest in-flight transaction
```

### 4.8 Temporary File Management

Events are buffered to temporary files before flushing to external storage:

```
TempFilePool
    ├── File (per task/region group)
    │   ├── ForWrite (compression layer)
    │   │   └── ForWriteCore (write state)
    │   └── ForRead (reader cursor)
    │       └── FileCore (actual content + swap state)
    └── SwappedOut (overflow to disk)
```

**Features:**
- Configurable compression type
- In-memory buffer with swap-to-disk under memory pressure
- `BackupEncryptionManager` integration for transparent encryption
- IV generation per file
- Reference counting and cleanup

### 4.9 Metadata Storage (Etcd)

Backup-stream persists metadata to etcd for coordination across TiKV nodes:

```
/tidb/br-stream/
    ├── info/<task_name>                                     → StreamBackupTaskInfo (protobuf)
    ├── ranges/<task_name>/<start_key>                       → end_key
    ├── checkpoint/<task_name>/
    │   ├── store/<store_id>                                 → ts (u64, big-endian)
    │   ├── region/<region_id>/<epoch_version>                → ts
    │   └── central_global                                   → ts
    ├── storage-checkpoint/<task_name>/<store_id>             → ts
    ├── pause/<task_name>                                    → "" (existence = paused)
    └── last-error/<task_name>/<store_id>                     → StreamBackupError (protobuf)
```

**MetadataClient:**
```
MetadataClient<Store> {
    store_id: u64,
    caches: Arc<DashMap<String, CheckpointCache>>,
    meta_store: Store,
}
```

Key operations:
- `init_task()`: Initialize checkpoint for store if not present
- `report_last_error()`: Upload fatal error to etcd
- `check_task_paused()`: Check pause status
- `global_checkpoint_of_task()`: Get coordinator-computed global checkpoint

**CheckpointCache:** Simple lease-based cache with 12s TTL (matching coordinator tick interval) [Inferred].

### 4.10 Observer Pattern (Backup-Stream)

```
BackupStreamObserver {
    scheduler: Scheduler<Task>,
    ranges: Arc<RwLock<SegmentSet<Vec<u8>>>>,   // Key ranges to observe
}
```

**Implements:**
- `CmdObserver`: `on_flush_applied_cmd_batch()` — captures KV changes for regions overlapping configured ranges
- `RoleObserver`: `on_role_change()` — stops observing on transition to follower
- `RegionChangeObserver`: `on_region_changed()` — handles splits/merges/destroys

**Filtering:** Only processes regions whose key range overlaps with registered observation ranges. Uses `SegmentSet` for efficient range intersection. When no ranges are registered (`is_hibernating() = true`), events are completely skipped [Inferred].

### 4.11 Service Layer

```
BackupStreamGrpcService methods:
    flush_now(FlushNowRequest)                    // Force immediate flush to external storage
    get_last_flush_ts_of_region(request)          // Query checkpoint for specific regions
    subscribe_flush_event(request)                // Stream subscriber for flush notifications
```

### 4.12 End-to-End Data Flow

#### Initial Subscription

```
1. Observer detects new leader region overlapping task key range
2. Sends Task::ModifyObserve(Start{region})
3. Endpoint transitions region to Pending state
4. Subscription manager enqueues scan command
5. InitialDataLoader::do_initial_scan():
   a. Capture snapshot from coprocessor
   b. Create EventLoader with DeltaScanner
   c. Batch and emit ApplyEvents to Router
   d. Track locks in TwoPhaseResolver (phase 1)
6. phase_one_done() replays buffered locks
7. Endpoint transitions region to Running state
```

#### Incremental Event Flow

```
1. KV write applied in region via Raft
2. BackupStreamObserver::on_flush_applied_cmd_batch()
3. Task::BatchEvent sent to Endpoint
4. Endpoint converts CmdBatch → ApplyEvents, updates locks
5. Router buffers events in temp files (with compression/encryption)
6. Periodically or on demand: Task::ForceFlush
7. Router batches into DataFileGroup, computes checksums
8. Writes to external storage (S3, GCS, etc.)
```

#### Checkpoint Update Flow

```
1. SubscriptionTracer::resolve_with(min_ts, regions)
   → For each region's TwoPhaseResolver, compute checkpoint
   → Returns Vec<ResolveResult>
2. CheckpointManager::resolve_regions(results)
   → Updates resolved_ts map
3. On flush: CheckpointManager::freeze()
   → Moves resolved_ts → frozen_resolved_ts
4. On flush completion: CheckpointManager::flush_and_notify(results)
   → Moves frozen → checkpoint_ts
   → Notifies subscribers with FlushEvent
   → Persists to metadata store
```

---

## 5. Resolved Timestamp

### 5.1 Overview

Resolved Timestamp (Resolved TS) tracks the minimum timestamp guarantee: all transactions with `commit_ts ≤ resolved_ts` have been fully observed. This enables:
- **CDC**: Downstream consumers know they have received all changes up to resolved_ts
- **Stale reads**: Followers can serve reads at timestamps ≤ resolved_ts without Raft round-trips
- **Backup-stream**: Checkpoint computation for PITR consistency

**Invariant:** `resolved_ts = min(oldest_in-flight_transaction.start_ts, latest_PD_TSO)`

If there are no in-flight locks, resolved_ts advances to the latest available PD timestamp.

**Key source files:**
- `components/resolved_ts/src/resolver.rs` — Per-region lock tracking
- `components/resolved_ts/src/advance.rs` — Store-level TS advancement
- `components/resolved_ts/src/endpoint.rs` — Orchestration
- `components/resolved_ts/src/observer.rs` — Raftstore observer
- `components/resolved_ts/src/scanner.rs` — Lock scanning for initialization
- `components/resolved_ts/src/cmd.rs` — Command parsing

### 5.2 Resolver (Per-Region Lock Tracking)

```
Resolver {
    region_id: u64,
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,           // key → start_ts
    lock_ts_heap: BTreeMap<TimeStamp, TxnLocks>,           // start_ts → {count, sample_key} (sorted)
    large_txns: HashMap<TimeStamp, TxnLocks>,              // Large transaction tracking
    large_txn_key_representative: HashMap<Vec<u8>, TimeStamp>,
    resolved_ts: TimeStamp,                                 // Current resolved timestamp
    min_ts: TimeStamp,                                      // Min ts for advancement
    tracked_index: u64,                                     // Highest Raft index processed
    read_progress: Option<Arc<RegionReadProgress>>,         // For stale read serving
    memory_quota: Arc<MemoryQuota>,
    txn_status_cache: Arc<TxnStatusCache>,                  // Cache for large txn status
    stopped: bool,
}
```

#### Dual Lock Tracking Strategy

**Normal transactions (generation=0):**
- Tracked via `locks_by_key` (key → start_ts) + `lock_ts_heap` (start_ts → TxnLocks)
- Full key-level tracking
- Memory: ~(key_len + sizeof(TimeStamp)) per lock

**Large transactions (generation>0):**
- Tracked separately in `large_txns` HashMap
- Uses `TxnStatusCache` to look up `min_commit_ts` instead of just `start_ts`
- Only stores a representative key (not all keys) to reduce memory [Inferred]
- Status cache entries: `Ongoing { min_commit_ts }`, `Committed { commit_ts }`, `RolledBack`

#### Core Operations

**`track_lock(start_ts, key, index, generation)`:**
```
if generation == 0:
    locks_by_key[key] = start_ts
    lock_ts_heap[start_ts].count += 1
    lock_ts_heap[start_ts].sample_key = key  (if first)
    memory_quota.alloc(key_len + sizeof(TimeStamp))
else:
    large_txns[start_ts] = TxnLocks{count, sample_key}
    large_txn_key_representative[key] = start_ts
```

**`untrack_lock(key, index)`:**
```
if key in locks_by_key:
    start_ts = locks_by_key.remove(key)
    lock_ts_heap[start_ts].count -= 1
    if count == 0: lock_ts_heap.remove(start_ts)
    memory_quota.free(key_len + sizeof(TimeStamp))
else if key in large_txn_key_representative:
    start_ts = large_txn_key_representative.remove(key)
    large_txns.remove(start_ts)
```

**`resolve(min_ts, now, source) → TimeStamp`:**
```
1. oldest_normal = lock_ts_heap.first_key()    // BTreeMap min = O(1)
2. oldest_large = min(large_txns.values().min_commit_ts)
   (filtering out Committed/RolledBack via TxnStatusCache)
3. oldest = min(oldest_normal, oldest_large)
4. new_resolved_ts = min(oldest, min_ts)
5. resolved_ts = max(resolved_ts, new_resolved_ts)  // Never decreases
6. Update RegionReadProgress.safe_ts for stale reads
7. Return resolved_ts
```

**Memory management:**
- Every lock tracks its heap size for quota accounting
- Aggressive HashMap shrinking every 10 seconds
- Lazy shrinking on untrack (ratio-based, ratio=8, amortizes rehashing)
- Warning on drop if >64MB of tracked locks remain

#### TsSource (Resolved TS Attribution)

```
TsSource:
    Lock(TxnLocks)      // Limited by actual lock
    MemoryLock(Key)      // From ConcurrencyManager (in-memory lock table)
    PdTso                // From PD timestamp oracle
    BackupStream         // From backup-stream consumer
    Cdc                  // From CDC consumer
```

### 5.3 AdvanceTsWorker (Store-Level Advancement)

The `AdvanceTsWorker` periodically advances resolved TS across all regions on the store:

```
AdvanceTsWorker {
    pd_client: Arc<dyn PdClient>,
    worker: Runtime,                                         // 1-thread async runtime
    scheduler: Scheduler<Task>,
    concurrency_manager: ConcurrencyManager,
    last_pd_tso: Arc<Mutex<Option<(TimeStamp, Instant)>>>,   // Cached PD TSO
}
```

#### Advancement Flow

```
Every advance_ts_interval:
    1. Fetch latest PD TSO
    2. Update ConcurrencyManager.max_ts (for async commit)
    3. Check CM for global min memory lock:
       if CM.global_min_lock() < PD TSO:
           min_ts = CM.global_min_lock()
           ts_source = MemoryLock
       else:
           min_ts = PD TSO
           ts_source = PdTso
    4. Verify leadership via LeadershipResolver
    5. Schedule Task::ResolvedTsAdvanced { valid_regions, min_ts, ts_source }
    6. Re-schedule next advancement round
```

#### LeadershipResolver (Multi-Store Quorum Verification)

Before advancing resolved TS, leadership must be verified across the cluster to prevent split-brain:

```
LeadershipResolver {
    tikv_clients: HashMap<u64, TikvClient>,         // Cached gRPC clients
    store_id: u64,                                   // Local store ID
    region_read_progress: RegionReadProgressRegistry,
    progresses: HashMap<u64, RegionProgress>,        // Quorum tracking
}
```

**Verification process:**
1. **Local check**: If region has quorum on local store, resolve immediately
2. **Batch CheckLeader RPCs**: Send batched requests to remote stores (gzip compressed for large payloads)
3. **Quorum validation**:
   ```
   (resp_voters + resp_incoming_voters) >= (voters + incoming_voters)/2 + 1
   AND
   (resp_voters + resp_demoting_voters) >= (voters + demoting_voters)/2 + 1
   ```
   Learners don't count toward quorum but still need leadership check [Inferred].
4. **Return valid regions**: Only regions with confirmed quorum leadership are advanced

### 5.4 Observer (Resolved TS)

```
Observer {
    scheduler: Scheduler<Task>,
    memory_quota: Arc<MemoryQuota>,
}
```

**Registration priorities:**
- `CmdObserver`: priority 1000 (runs last — sees complete command batch)
- `RoleObserver`: priority 100
- `RegionChangeObserver`: priority 100

**Lock-only filter** (`lock_only_filter`):
```
When ONLY resolved_ts observing (CDC not active):
    Keep: CF_LOCK and CF_WRITE operations
    Drop: CF_DEFAULT (user values not needed for lock tracking)

When both CDC and resolved_ts observing:
    Keep: All operations (CDC needs complete picture)
```

This optimization reduces memory consumption when only resolved_ts is tracking a region.

**Role change handling:** When a peer loses leadership (`role != Leader`), schedules `Task::DeRegisterRegion` to stop tracking.

### 5.5 Scanner (Initialization)

When a region becomes leader, existing locks must be discovered:

```
ScannerPool<T: CdcHandle, E: KvEngine> {
    workers: Arc<Runtime>,           // Multi-threaded async runtime
    cdc_handle: T,                   // For getting region snapshots
}

ScanTask {
    handle: ObserveHandle,           // ObserveId for consistency
    region: Region,
    checkpoint_ts: TimeStamp,        // Start scanning from here
    backoff: Option<Duration>,       // Backoff before starting
    cancelled: Receiver<()>,         // Cancellation channel
}
```

**Scan execution flow:**
1. **Backoff** (optional, for re-registration after errors, exponential: 10^(retry-1))
2. **Get snapshot** (up to 3 attempts):
   - `cdc_handle.capture_change()` for region snapshot
   - Non-retryable errors: epoch_not_match, stale observe_id
3. **Scan locks** (batched, 128 entries per batch):
   - `MvccReader::scan_locks_from_storage()`
   - Only scans Put and Delete lock types (skips Lock, Pessimistic)
   - Sends `ScanEntries::Lock(vec)` to endpoint
4. **Complete**: Sends `ScanEntries::None` to signal scan done

**Concurrency control:** `Semaphore` limits concurrent scans (default: `incremental_scan_concurrency = 4`).

### 5.6 Command Processing

**ChangeLog** (output of processing applied commands):
```
ChangeLog:
    Error(errorpb::Error)
    Rows { index: u64, rows: Vec<ChangeRow> }
    Admin(AdminCmdType)
```

**ChangeRow** (individual transaction operations):
```
ChangeRow::Prewrite {
    key, start_ts, lock_type,
    value: Option<Value>,
    generation: u64,          // 0 = normal, >0 = large txn
}

ChangeRow::Commit {
    key, write_type,
    start_ts: Option<TimeStamp>,
    commit_ts: Option<TimeStamp>,
}

ChangeRow::OnePc {
    key, write_type,
    commit_ts, value,
}

ChangeRow::IngestSsT          // SST file ingestion
```

**Decoding process:**
1. Group commands by key (a single key may have multiple CF operations)
2. Pattern match:
   - CF_WRITE present → `ChangeRow::Commit` or `ChangeRow::OnePc`
   - CF_LOCK present (no write) → `ChangeRow::Prewrite`
   - CF_LOCK deleted → `ChangeRow::Commit { write_type: Rollback }`

**Lock filtering:**
- `decode_lock()`: Only accepts Put and Delete lock types. Skips Lock, Pessimistic, SharedLocks (these don't affect resolved_ts).
- `decode_write()`: Handles gc_fence optimization (drops rewritten writes with overlapped rollback).

### 5.7 Endpoint (Orchestration)

```
Endpoint<T: CdcHandle, E: KvEngine, S: StoreRegionMeta> {
    regions: HashMap<u64, ObserveRegion>,       // Per-region state
    advance_worker: AdvanceTsWorker,            // Store-level advancement
    scanner_pool: ScannerPool<T, E>,            // Lock scanning workers
    memory_quota: Arc<MemoryQuota>,
    scan_concurrency_semaphore: Arc<Semaphore>,
    cfg: ResolvedTsConfig,
}
```

**ObserveRegion state:**
```
ObserveRegion {
    meta: Region,
    handle: ObserveHandle,                      // ABA protection
    resolver: Resolver,                         // Per-region lock tracking
    resolver_status: ResolverStatus,
}

ResolverStatus:
    Pending {
        tracked_index: u64,                     // Index from region snapshot
        locks: Vec<PendingLock>,                // Buffered lock changes during scan
        cancelled: Option<Sender<()>>,          // Cancel token
        memory_quota: Arc<MemoryQuota>,
    }
    Ready                                       // Scan completed, tracking live
```

**Task processing:**
```
Task::RegisterRegion { region }
    → Create ObserveRegion in Pending state
    → Spawn scanner to discover existing locks
    → Buffer lock changes until scan completes

Task::ChangeLog { cmd_batch }
    → If Pending: buffer locks/commits in PendingLock vec
    → If Ready: directly update resolver (track_lock/untrack_lock)

Task::ScanLocks { region_id, entries, apply_index }
    → Track scanned locks
    → On ScanEntries::None: transition Pending → Ready
    → Replay all buffered PendingLock entries

Task::ResolvedTsAdvanced { regions, ts, ts_source }
    → Call resolver.resolve(min_ts, ts_source) for each valid region
    → Publish safe_ts to RegionReadProgress for stale reads

Task::DeRegisterRegion { region_id }
    → Remove region from tracking, stop scanner if running

Task::RegionUpdated { region }
    → May trigger re-registration if key range changed

Task::RegionDestroyed { region }
    → Deregister destroyed region
```

### 5.8 Resolved TS Propagation

```
Store Level (AdvanceTsWorker)
    ├── PD TSO fetch (latest timestamp)
    ├── ConcurrencyManager min_lock check (in-memory locks)
    ├── Leadership verification (CheckLeader quorum)
    └── min_ts = min(PD_TSO, CM_min_lock)

Region Level (Resolver)
    ├── Track/untrack locks from Raft apply
    ├── resolved_ts = min(oldest_lock.start_ts, min_ts)
    └── Publish to RegionReadProgress.safe_ts

Consumers:
    ├── CDC: uses resolved_ts to send ResolvedTs events to downstream
    ├── Stale reads: followers serve reads at ts ≤ safe_ts
    └── Backup-stream: uses resolved_ts for checkpoint computation
```

### 5.9 Diagnostics

**Per-region metrics** (via `collect_stats`):
- Min leader resolved_ts and gap from current time
- Min follower safe_ts and resolved_ts
- Lock counts and transaction counts
- Duration since last update

**Slow region detection** (`log_slow_regions`):
- Leader threshold: `expected_interval + 1s` grace
- Follower threshold: `2 × expected_interval + 1s` grace
- Logs: region ID, gap, lock details, min_memory_lock from ConcurrencyManager

**Diagnosis callback** (`handle_get_diagnosis_info`):
- Returns: stopped flag, resolved_ts, tracked_index, num_locks, num_transactions

---

## 6. Component Interactions

### 6.1 Shared Observer Registration

All three components (CDC, backup-stream, resolved_ts) register with the same `CoprocessorHost`:

```
CoprocessorHost
    ├── CmdObserver registry (sorted by priority)
    │   ├── CdcObserver (priority 0)
    │   ├── BackupStreamObserver
    │   └── ResolvedTsObserver (priority 1000)
    │
    ├── RoleObserver registry
    │   ├── CdcObserver
    │   ├── BackupStreamObserver
    │   └── ResolvedTsObserver
    │
    └── RegionChangeObserver registry
        ├── CdcObserver
        ├── BackupStreamObserver
        └── ResolvedTsObserver
```

Each observer independently processes events and manages its own subscriptions, but they share the same raftstore hooks. The `ObserveLevel` of the `CmdBatch` is the maximum requested by any active observer.

### 6.2 CDC ↔ Resolved TS

- CDC uses resolved_ts to generate `ResolvedTs` events for downstream consumers
- CDC's `Delegate` maintains its own lock tracking (`LockTracker`) independently from the `resolved_ts` component's `Resolver`
- Both share the `ConcurrencyManager` for in-memory lock visibility
- The `Endpoint` in resolved_ts publishes `safe_ts` to `RegionReadProgress`, which CDC does not directly consume [Inferred]

### 6.3 Backup-Stream ↔ Resolved TS

- Backup-stream's `TwoPhaseResolver` wraps the `Resolver` from the `resolved_ts` crate
- Checkpoint computation directly depends on resolved TS values
- Both use the same `MemoryQuota` accounting pattern
- Backup-stream adds the two-phase initialization protocol on top of the base resolver

### 6.4 Full Backup Independence

Full backup operates independently of CDC and resolved_ts:
- Uses engine snapshots directly (not raftstore observer pattern)
- Does not track or compute resolved timestamps
- Can use disk snapshot mode for Raft-level consistency
- Shares `ExternalStorage` abstraction with backup-stream
