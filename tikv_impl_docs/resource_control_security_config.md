# Resource Control, Security, and Configuration

This document specifies the resource control, encryption at rest, TLS security, and configuration subsystems of TiKV. These systems form the operational infrastructure that enables multi-tenancy, data protection, and runtime management.

## 1. Resource Control

### 1.1 Architecture Overview

TiKV implements a multi-tenant resource control system that enforces CPU and I/O quotas across **resource groups**. The system is managed by PD (Placement Driver) and uses a combination of virtual-time priority scheduling and token bucket rate limiting.

**Core Components:**

| Component | Location | Role |
|-----------|----------|------|
| `ResourceGroupManager` | `components/resource_control/src/resource_group.rs` | Central registry of resource groups; holds DashMap of groups and references to controllers |
| `ResourceController` | `components/resource_control/src/resource_group.rs` | Per-pool (read/write) priority scheduler using virtual time; implements YATP `TaskPriorityProvider` |
| `ResourceLimiter` | `components/resource_control/src/resource_limiter.rs` | Token bucket rate limiter for CPU (microseconds) and I/O (bytes) |
| `QuotaLimiter` | `components/resource_control/src/resource_limiter.rs` | Single-resource-type limiter (CPU or IO) with atomic statistics tracking |
| `ResourceManagerService` | `components/resource_control/src/service.rs` | Watches PD for group config changes; reports consumption back to PD |
| `GroupQuotaAdjustWorker` | `components/resource_control/src/worker.rs` | Background worker that dynamically adjusts background task quotas based on system utilization |
| `PriorityLimiterAdjustWorker` | `components/resource_control/src/worker.rs` | Adjusts CPU quotas across High/Medium/Low priority levels [currently disabled, issue #18939] |

### 1.2 Resource Group Model

Resource groups are configured in PD and synced to TiKV nodes via the `ResourceManagerService`. Each group has:

- **RU (Request Unit) Quota** — an abstract cost unit combining CPU and I/O
- **Priority Level** — High (11-16), Medium (7-10, default), or Low (1-6)
- **Background Source Types** — job types (e.g., "br", "lightning") matched against request source
- **Optional ResourceLimiter** — for background tasks only

```
Resource Group Structure:
├── PbResourceGroup (protobuf definition from PD)
├── limiter: Option<Arc<ResourceLimiter>>   // Token bucket for background tasks
├── background_source_types: HashSet<String> // Job type matching
└── fallback_default: bool                   // Whether to fall back to default group
```

A **default resource group** always exists and is used when a request does not specify a group.

**Request Source Format:** `{external|internal}_{tidb_req_source}_{source_task_name}`

### 1.3 RU (Request Unit) Cost Model

Resource consumption is measured in Request Units (RU). The default pricing model (configured from PD):

| Metric | Default Cost |
|--------|-------------|
| Read base cost | 1/8 RU per read |
| Read byte cost | 1/(64×1024) RU per byte |
| Read CPU cost | 1/3 RU per millisecond of CPU |
| Write base cost | 1.0 RU per write |
| Write byte cost | 1/1024 RU per byte |

```
read_ru  = (read_cpu_ms_cost × cpu_consumed_ms) + (read_cost_per_byte × read_bytes)
write_ru = write_cost_per_byte × write_bytes
```

Consumption statistics are periodically reported back to PD for billing and quota enforcement.

### 1.4 Priority Scheduling (Virtual Time Algorithm)

The `ResourceController` implements priority scheduling for the YATP thread pool using a **virtual time** algorithm. This ensures fair CPU time allocation proportional to each group's RU quota.

**Algorithm:**

```
FUNCTION priority_of(task_metadata) -> u64:
    group = lookup_group(task_metadata.group_name)
    tracker = group.priority_tracker

    // Priority = (group_priority << 60) | virtual_time
    // Lower virtual_time = higher scheduling priority
    RETURN (tracker.group_priority << 60) + tracker.virtual_time

FUNCTION consume(group_name, resource_type, delta):
    tracker = lookup_tracker(group_name)

    // Virtual time advances proportional to consumption × weight
    // weight = max_ru_quota / group_ru_quota × 10.0
    // Groups with lower RU quotas have higher weight → faster vt advance → lower priority
    IF resource_type == READ:
        vt_delta = DEFAULT_PRIORITY_PER_READ_TASK × tracker.weight  // 50µs × weight
    ELSE:  // WRITE
        vt_delta = actual_consumed × tracker.weight

    tracker.virtual_time.fetch_add(vt_delta, Relaxed)

FUNCTION periodic_vt_balance():  // Every 1 second (MIN_PRIORITY_UPDATE_INTERVAL)
    // Find minimum virtual time across all active groups
    min_vt = MIN(all_trackers.virtual_time)

    // Advance lagging groups toward min to prevent starvation
    FOR each tracker:
        IF tracker.virtual_time < min_vt:
            tracker.virtual_time = min_vt

    // Reset all if approaching overflow (u64::MAX / 16)
    IF max_vt > u64::MAX / 16:
        base = min_vt
        FOR each tracker:
            tracker.virtual_time -= base
```

**Task Metadata Propagation:**

```
gRPC ResourceControlContext (in request)
    ↓ encode
TaskMetadata (1-byte mask + optional priority + group_name)
    ↓ set_metadata
YATP Extras.metadata()
    ↓ read by
ResourceController.priority_of() (TaskPriorityProvider trait impl)
    ↓ determines
Task queue ordering in YATP thread pool
```

### 1.5 Token Bucket Rate Limiting

Background tasks use a `ResourceLimiter` with separate CPU and I/O token buckets:

```
ResourceLimiter:
├── limiters[CPU]:  QuotaLimiter  // Tokens = microseconds of CPU time
├── limiters[IO]:   QuotaLimiter  // Tokens = bytes (read + write)
└── is_background: bool

QuotaLimiter (token bucket):
├── limiter: Limiter              // 1-second refill interval, 1ms minimum wait
├── total_wait_dur_us: AtomicU64  // Cumulative wait time
├── read_bytes: AtomicU64         // Bytes read
├── write_bytes: AtomicU64        // Bytes written
└── req_count: AtomicU64          // Request count
```

**Consumption:** `consume(tokens) -> Duration` returns the wait duration before the task may proceed. `async_consume()` awaits the delay. Minimum wait duration is 1ms to avoid busy-spinning.

### 1.6 Background Quota Adjustment

The `GroupQuotaAdjustWorker` runs every 10 seconds (`BACKGROUND_LIMIT_ADJUST_DURATION`) and dynamically adjusts background task quotas based on system resource utilization:

```
FUNCTION adjust_background_quotas():
    // 1. Measure system utilization
    total_cpu = system_cpu_cores
    total_io = system_io_bandwidth
    current_used = observed_utilization
    background_consumed = sum(background_group_consumption)

    // 2. Calculate available budget (reserve 20% for foreground spikes)
    available = (total_quota - current_used + background_consumed) × 0.8

    // 3. Allocate across background groups
    // Minimum floor: 10% of total per group
    // Sort by (expected_cost_rate / ru_quota) for balanced allocation
    IF available >= sum(expected_costs):
        // Sufficient: each group gets max(expected, minimum_share)
    ELSE:
        // Constrained: allocate by RU share proportion
```

**Priority Control Strategy** (configurable, default Moderate):
- **Aggressive:** Targets 50% utilization (favors high-priority tasks)
- **Moderate:** Targets 70% utilization (balanced)
- **Conservative:** Targets 90% utilization (favors throughput)

### 1.7 Integration with Request Pipeline

Resource control integrates at multiple points in the request processing pipeline:

```
gRPC Request (ResourceControlContext)
    ↓
┌───────────────────────────────────────────┐
│ Read Pool (YATP)                          │
│  ResourceController → priority scheduling │
│  ControlledFuture → CPU consumption track │
│  LimitedFuture → token bucket waiting     │
└───────────────────────────────────────────┘
    ↓
┌───────────────────────────────────────────┐
│ TxnScheduler                              │
│  ResourceController → write task priority │
│  QuotaLimiter → quota delay after exec    │
└───────────────────────────────────────────┘
    ↓
┌───────────────────────────────────────────┐
│ Storage Layer                             │
│  get_resource_limiter() per command       │
│  Background task token bucket enforcement │
└───────────────────────────────────────────┘
```

**Read Pool Double-Wrapping Pattern** (read_pool.rs):
1. `ControlledFuture` — wraps the future to track CPU time via `ResourceController.consume()`
2. `LimitedFuture` (via `with_resource_limiter()`) — wraps again for token bucket delay enforcement

**Storage Layer** (storage/mod.rs): Each storage command calls `resource_mgr.get_resource_limiter(resource_group_tag, request_source, override_priority)` to obtain an `Option<Arc<ResourceLimiter>>` for background task limiting.

### 1.8 Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `tikv_resource_control_background_quota_limiter` | Gauge | Per-group quota limits |
| `tikv_resource_control_background_resource_consumption` | Counter | Consumption by group |
| `tikv_resource_control_background_task_wait_duration` | Histogram | Wait time by group |
| `tikv_resource_control_priority_quota_limit` | Gauge | Per-priority quotas |
| `tikv_resource_control_priority_task_exec_duration` | Histogram | CPU time by priority |
| `tikv_resource_control_priority_wait_duration` | Histogram | Priority-based wait |
| `tikv_resource_control_bg_resource_utilization` | Gauge | Utilization percentage |

---

## 2. Encryption at Rest

### 2.1 Architecture Overview

TiKV implements **transparent encryption at rest** using a two-tier key hierarchy. All file-level encryption is transparent to upper layers via RocksDB's encryption environment integration.

```
Master Key (external: file, KMS, or in-memory)
    │
    │  encrypts/decrypts (AES-256-GCM)
    ▼
Key Dictionary (key_id → DataKey, stored on disk encrypted)
    │
    │  provides data keys for
    ▼
File Dictionary (filename → FileInfo{key_id, method, iv}, stored on disk plaintext)
    │
    │  looked up per file by
    ▼
RocksDB Encryption Environment (transparent per-file encryption)
```

### 2.2 Key Hierarchy

#### Master Key

The master key protects the key dictionary. It is never used to encrypt data files directly. Supported backends:

| Backend | Location | Description |
|---------|----------|-------------|
| `PlaintextBackend` | `encryption/src/master_key/mod.rs` | No encryption (testing only) |
| `FileBackend` | `encryption/src/master_key/file.rs` | Hex-encoded 256-bit key in a 65-byte file; uses AES-256-GCM to encrypt key dictionary |
| `KmsBackend` | `encryption/src/master_key/kms.rs` | Cloud KMS (AWS, Azure, GCP); 10-second timeout per operation; caches encrypted data key |
| `MultiMasterKeyBackend` | `encryption/src/master_key/mod.rs` | Multiple keys for backup/restore operations |

**Master Key Configuration:**

```rust
enum MasterKeyConfig {
    Plaintext,                        // No encryption
    File { path: String },            // Local file containing hex-encoded key
    Kms {
        key_id: String,               // KMS key identifier
        region: String,               // Cloud region
        endpoint: String,             // KMS endpoint URL
        vendor: String,               // "aws", "azure", or "gcp"
        // Vendor-specific config (AwsConfig, AzureConfig, GcpConfig)
    },
}
```

#### Data Keys

Data keys are generated randomly and used for actual file encryption. Each data key has a unique 64-bit ID.

```
FUNCTION generate_data_key(method) -> (key_id, key_bytes):
    key_id = rand_u64()                      // Random 64-bit identifier
    key_length = method_key_length(method)   // 16, 24, or 32 bytes
    key_bytes = rand_bytes(key_length)       // Cryptographically random
    RETURN (key_id, key_bytes)
```

**Supported Encryption Methods:**

| Method | Key Length | Mode |
|--------|-----------|------|
| AES-128-CTR | 16 bytes | Counter mode, 16-byte IV |
| AES-192-CTR | 24 bytes | Counter mode, 16-byte IV |
| AES-256-CTR | 32 bytes | Counter mode, 16-byte IV |
| SM4-CTR | 16 bytes | Counter mode (Chinese national standard) |

### 2.3 DataKeyManager

The `DataKeyManager` (`components/encryption/src/manager/mod.rs`) is the central coordinator for all encryption operations.

**Internal Structure:**

```
DataKeyManager:
├── dicts: Arc<Dicts>
│   ├── file_dict: Mutex<FileDictionary>       // filename → {key_id, method, iv}
│   ├── file_dict_file: Mutex<FileDictionaryFile>  // Persistent file dict with log
│   ├── key_dict: Mutex<KeyDictionary>         // key_id → DataKey (encrypted on disk)
│   ├── current_key_id: AtomicU64              // Lock-free current key lookup
│   ├── rotation_period: Duration              // Default: 7 days
│   └── base: PathBuf                          // Dictionary storage path
├── method: EncryptionMethod                   // Active encryption algorithm
├── rotate_tx: Sender<RotateTask>              // Channel to background worker
└── background_worker: JoinHandle              // Key rotation thread
```

**File Operations (RocksDB integration):**

| Operation | Method | Behavior |
|-----------|--------|----------|
| Lookup | `get_file(fname)` | Returns FileInfo{key_id, method, iv} for existing file |
| Create | `new_file(fname)` | Allocates new IV, assigns current data key, records in file_dict |
| Delete | `delete_file(fname)` | Removes file from file_dict |
| Link | `link_file(src, dst)` | Copies encryption metadata (same key_id and iv) |
| Rename | `rename_file(src, dst)` | Moves encryption metadata to new filename |

### 2.4 Key Rotation

**Rotation Mechanism:**

```
Background Worker (separate thread):
    LOOP:
        WAIT min(10 minutes, rotation_period)    // ROTATE_CHECK_PERIOD = 10min

        current_key = key_dict[current_key_id]
        IF current_key.creation_time + rotation_period < now():
            // Generate new data key
            (new_id, new_key) = generate_data_key(method)

            // Add to key dictionary
            key_dict.insert(new_id, DataKey { key: new_key, ... })

            // Re-encrypt key dictionary with master key and save to disk
            encrypted_dict = master_key.encrypt(serialize(key_dict))
            write_to_disk(encrypted_dict)

            // Atomically update current key ID
            current_key_id.store(new_id, SeqCst)
```

**Master Key Rotation:** TiKV supports seamless master key rotation via the `previous_master_key` config field. During startup, if decryption with the current master key fails with `WrongMasterKey`, it falls back to `previous_master_key`. If both fail, a `BothMasterKeyFail` error is returned.

### 2.5 Encrypted File Format

```
File Header (16 bytes):
┌─────────┬──────────┬────────┬──────────┐
│ Version │ Reserved │ CRC32  │   Size   │
│ (1 byte)│ (3 bytes)│(4 bytes)│(8 bytes) │
└─────────┴──────────┴────────┴──────────┘

Version:
  V1: encrypted content only
  V2: encrypted content + variable unencrypted log records (file dictionary logging)
```

**Stream Encryption:** `EncrypterReader` and `DecrypterReader` (`encryption/src/io.rs`) provide streaming encryption/decryption for large files, avoiding full in-memory loading. Maximum in-place encryption block size is 1 MB.

### 2.6 RocksDB Integration

The `WrappedEncryptionKeyManager` (`components/engine_rocks/src/encryption.rs`) adapts `DataKeyManager` to RocksDB's `EncryptionKeyManager` trait:

```
RocksDB Environment Setup:
    IF DataKeyManager configured:
        env = create_encrypted_env(base_env, WrappedEncryptionKeyManager(manager))
        // All SST files, WAL files, MANIFEST files encrypted transparently
    ELSE:
        env = base_env  // No encryption
```

RocksDB calls `new_file()` when creating SST/WAL files and `get_file()` when reading them. The encryption is completely transparent to the storage and transaction layers above RocksDB.

### 2.7 Encryption Configuration

```rust
EncryptionConfig {
    data_encryption_method: Plaintext,              // Default: disabled
    data_key_rotation_period: ReadableDuration(7d),  // Key rotation interval
    enable_file_dictionary_log: true,                // Incremental file dict updates
    file_dictionary_rewrite_threshold: 1_000_000,    // Full rewrite after N log entries
    master_key: MasterKeyConfig::Plaintext,          // Master key backend
    previous_master_key: MasterKeyConfig::Plaintext, // For master key rotation
}
```

### 2.8 Backup Encryption

The `BackupEncryptionManager` (`components/encryption/src/backup/backup_encryption.rs`) handles encryption for backup/restore operations, supporting:
- User-provided plaintext data keys (`CipherInfo`)
- Master-key-based file encryption for backup SST files
- `MultiMasterKeyBackend` for restore operations with multiple possible keys

---

## 3. TLS and Security

### 3.1 SecurityConfig

The `SecurityConfig` (`components/security/src/lib.rs`) controls all TLS and access control settings:

```rust
SecurityConfig {
    ca_path: String,                    // CA certificate file (PEM)
    cert_path: String,                  // Server/client certificate file (PEM)
    key_path: String,                   // Private key file (PEM)
    override_ssl_target: String,        // Test override for SSL target name
    cert_allowed_cn: HashSet<String>,   // Allowed client Common Names (mTLS)
    redact_info_log: RedactOption,      // Log redaction for sensitive data
    encryption: EncryptionConfig,       // Encryption at rest config (nested)
}
```

**Validation Rule:** All three files (ca_path, cert_path, key_path) must be either all present or all absent. Partial configuration is rejected.

### 3.2 SecurityManager

The `SecurityManager` provides TLS-enabled gRPC channel and server construction:

**Client Connection:**

```
FUNCTION connect(channel_builder, addr) -> Channel:
    IF ca_path is empty:
        RETURN channel_builder.connect(addr)       // Plaintext
    ELSE:
        (ca, cert, key) = load_certs()
        credentials = ChannelCredentials {
            root_cert: ca,
            client_cert: cert,
            client_key: key,
        }
        RETURN channel_builder.secure_connect(addr, credentials)  // mTLS
```

**Server Binding:**

```
FUNCTION bind(server_builder, addr, port) -> ServerBuilder:
    IF ca_path is empty:
        RETURN server_builder.bind(addr, port)     // Plaintext
    ELSE:
        IF cert_allowed_cn is not empty:
            // Add CN checker for client certificate validation
            server_builder.add_checker(CnChecker { allowed_cn })

        // Use certificate fetcher for hot-reload support
        RETURN server_builder.bind_with_fetcher(
            addr, port, Fetcher { cfg },
            RequestAndRequireClientCertificateAndVerify  // Mutual TLS
        )
```

### 3.3 Mutual TLS (mTLS) Enforcement

When `cert_allowed_cn` is configured, the server enforces mutual TLS with Common Name validation:

```
FUNCTION check_common_name(allowed_cn, rpc_context) -> Result:
    auth_ctx = rpc_context.auth_context()
    cn = auth_ctx.find("x509_common_name")

    IF cn is None:
        RETURN Err("no client certificate")
    IF cn NOT IN allowed_cn:
        RETURN Err("CN not allowed: {cn}")
    RETURN Ok(())
```

The `CnChecker` implements `ServerChecker` and is called for every incoming gRPC request. Currently uses exact string match (no wildcard support). [Inferred]

### 3.4 Certificate Hot-Reload

The `Fetcher` struct implements `ServerCredentialsFetcher` for zero-downtime certificate rotation:

```
FUNCTION fetch() -> Option<ServerCredentials>:
    IF cfg.is_modified(last_modified_time):
        (ca, cert, key) = cfg.load_certs()
        RETURN Some(new_credentials(ca, cert, key))
    ELSE:
        RETURN None  // Keep using previous certificates
```

**Modification detection:** Tracks file modification timestamps. If loading fails, the server continues with the previous certificates (graceful degradation).

### 3.5 Log Redaction

The `redact_info_log` option controls whether sensitive data (user keys, values) is redacted in log output:

| Value | Behavior |
|-------|----------|
| `false` (default) | Full data in logs |
| `true` | Sensitive data replaced with `?` |
| `marker` | Sensitive data wrapped with markers for selective filtering |

---

## 4. Configuration System

### 4.1 TikvConfig Struct Hierarchy

The root `TikvConfig` struct (`src/config/mod.rs`, line 3628) contains all configuration as a nested hierarchy of sub-config structs. Each sub-config is annotated with `#[online_config(submodule)]` if it supports runtime changes.

```
TikvConfig
├── log: LogConfig                    [submodule] - Log level, format, file rotation
├── memory: MemoryConfig              [submodule] - Heap profiling settings
├── quota: QuotaConfig                [submodule] - Global quota limits
├── readpool: ReadPoolConfig          [submodule] - Read thread pool sizing
├── server: ServerConfig              [submodule] - gRPC server settings
├── storage: StorageConfig            [submodule] - Block cache, flow control, scheduler
├── pd: PdConfig                      [skip]      - PD endpoints (startup only)
├── raft_store: RaftstoreConfig       [submodule] - Raft timers, region size limits
├── coprocessor: CopConfig            [submodule] - Coprocessor settings
├── coprocessor_v2: CopV2Config       [skip]      - Plugin config (startup only)
├── rocksdb: DbConfig                 [submodule] - RocksDB tuning parameters
├── raftdb: RaftDbConfig              [submodule] - Raft log DB parameters
├── raft_engine: RaftEngineConfig     [skip]      - Raft engine (startup only)
├── security: SecurityConfig          [submodule] - TLS and encryption
├── import: ImportConfig              [submodule] - SST import settings
├── backup: BackupConfig              [submodule] - Backup settings
├── log_backup: BackupStreamConfig    [submodule] - PITR settings
├── pessimistic_txn: PessimisticTxnConfig [submodule] - Pessimistic lock config
├── gc: GcConfig                      [submodule] - GC settings
├── split: SplitConfig                [submodule] - Region split settings
├── cdc: CdcConfig                    [submodule] - CDC settings
├── resolved_ts: ResolvedTsConfig     [submodule] - Resolved TS settings
├── resource_metering: ResourceMeteringConfig [submodule]
├── causal_ts: CausalTsConfig         [skip]      - Causal timestamp (startup only)
├── resource_control: ResourceControlConfig [submodule]
├── in_memory_engine: InMemoryEngineConfig  [submodule]
├── cfg_path: String                  [hidden]    - Config file path (internal)
├── slow_log_file: String             [skip]      - Slow log file path
├── slow_log_threshold: ReadableDuration [skip]   - Slow log threshold (default: 1s)
└── metric: MetricConfig              [hidden]    - Metrics reporting (internal)
```

### 4.2 OnlineConfig Trait and Derive Macro

The `OnlineConfig` trait (`components/online_config/src/lib.rs`) enables runtime configuration changes through a diff-update model:

```rust
trait OnlineConfig<'a> {
    type Encoder: Serialize;

    fn diff(&self, other: &Self) -> ConfigChange;     // Compute difference
    fn update(&mut self, change: ConfigChange) -> Result<()>;  // Apply difference
    fn get_encoder(&'a self) -> Self::Encoder;         // Serialize (excluding hidden)
    fn typed(&self) -> ConfigChange;                   // Get all fields with types
}
```

**ConfigChange** is `HashMap<String, ConfigValue>` where `ConfigValue` is:

| Variant | Rust Type | Description |
|---------|-----------|-------------|
| `Duration(u64)` | milliseconds | Time durations |
| `Size(u64)` | bytes | Memory/disk sizes |
| `U64`, `F64`, `I32`, `U32`, `Usize`, `Bool` | primitives | Numeric/boolean values |
| `String(String)` | | String values |
| `Module(ConfigChange)` | nested | Sub-config changes (recursive) |
| `Skip` | | Field not eligible for online change |
| `None` | | Field unchanged |

**Derive Macro Field Attributes** (`components/online_config/online_config_derive/`):

| Attribute | Behavior |
|-----------|----------|
| (none) | Field supports online change; included in diff/update/encoder |
| `#[online_config(skip)]` | Excluded from diff; not returned by encoder; startup-only |
| `#[online_config(hidden)]` | Like skip, but also hidden from encoder serialization (internal/deprecated) |
| `#[online_config(submodule)]` | Field is itself an OnlineConfig struct; changes propagated recursively |

### 4.3 ConfigController

The `ConfigController` (`src/config/mod.rs`, line 5120) is the runtime config change coordinator:

```
ConfigController:
├── current: TikvConfig                              // Current active config
└── config_mgrs: HashMap<Module, Box<dyn ConfigManager>>  // Per-module managers
```

**Module Enum:** Defines all configurable modules — `Readpool`, `Server`, `Raftstore`, `Coprocessor`, `Rocksdb`, `Raftdb`, `Storage`, `Security`, `Encryption`, `Import`, `Backup`, `PessimisticTxn`, `Gc`, `Split`, `Cdc`, `ResolvedTs`, `ResourceControl`, `BackupStream`, `Quota`, `Log`, `Memory`, etc.

**Online Config Change Flow:**

```
FUNCTION update(changes: HashMap<String, String>) -> Result:
    // 1. VALIDATION PHASE
    candidate = current.clone()
    candidate.apply(changes)
    candidate.validate()
    diff = current.diff(candidate)

    // 2. DISPATCH PHASE
    FOR each (module, module_change) in diff:
        IF config_mgrs.contains(module):
            manager = config_mgrs[module]
            manager.dispatch(module_change)?    // Apply side effects
            // Only update internal state if dispatch succeeds
            current.update(module_change)

    // 3. PERSISTENCE PHASE (if persist=true)
    toml_writer.merge_changes(config_file, changes)
```

**Key Properties:**
- **Atomic per-module:** If a module's dispatch fails, that module's changes are rolled back; other modules may still succeed
- **Validation before dispatch:** Config constraints are checked before any side effects
- **Persistent by default:** Changes written back to TOML file via `TomlWriter`
- **File reload:** `update_from_toml_file()` reloads the entire config file and computes diff from current state

### 4.4 ConfigManager Trait

Each module implements `ConfigManager` to handle the side effects of config changes:

```rust
trait ConfigManager: Send + Sync {
    fn dispatch(&mut self, change: ConfigChange) -> Result<()>;
}
```

**Example Implementations:**

| Manager | Module | Side Effects |
|---------|--------|-------------|
| `LogConfigManager` | Log | Calls `set_log_level()` to update global log level atomically |
| `MemoryConfigManager` | Memory | Activates/deactivates heap profiling; sets profiling sample rate |
| `StorageConfigManger` | Storage | Updates block cache capacity, flow controller, scheduler pool size, I/O rate limits |
| `DbConfigManger` | Rocksdb | Calls `set_cf_config()` per column family; updates background jobs, compaction, rate limits |
| `RaftstoreConfigManager` | Raftstore | Updates Raft timers, region size limits, tick intervals |

**Implementation Pattern:**
1. Extract specific fields from `ConfigChange` using `changes.get("field_name")` or `changes.remove("nested")`
2. Apply runtime changes (call engine APIs, update atomics, toggle features)
3. Handle nested `ConfigValue::Module` for sub-configs
4. Log changes with `info!("update X config"; "config" => ?changes)`
5. Return `Err` only for actual failures (validation errors, engine errors)

### 4.5 Configuration Loading and Validation

**Startup Flow:**

```
1. Parse CLI args (--config, --log-level, --log-file, --data-dir, --pd-endpoints, ...)
2. Load TikvConfig:
   IF --config provided:
       config = TikvConfig::from_file(path)      // TOML deserialization
       detect_unrecognized_keys(path)              // Warn about unknown fields
   ELSE:
       config = TikvConfig::default()
3. Apply CLI overrides:
   overwrite_config_with_cmd_args(&mut config, &matches)
4. Logger compatibility:
   config.logger_compatible_adjust()               // Handle deprecated log fields
5. Validate and persist:
   validate_and_persist_config(&mut config, persist)
```

**Validation Pipeline** (`validate_and_persist_config`):

```
1. Load last_tikv.toml from data directory (previous config snapshot)
2. compatible_adjust(last_cfg)         // Inherit deprecated field values
3. config.validate()                    // Check all constraints:
   - Path setup and consistency
   - RocksDB/RaftDB path validation
   - Engine-specific checks
   - Block cache auto-sizing (default: 45% of system memory)
   - Resource-based optimization
4. optional_default_cfg_adjust_with(last_cfg)  // Fine-tune defaults
5. check_critical_cfg_with(last_cfg)    // Prevent accidental breaking changes
6. Persist to last_tikv.toml            // Snapshot for next startup
```

**Readable Types** (`components/tikv_util/src/config.rs`):
- `ReadableSize` — Parses "1GB", "256MB", "4KB" → bytes
- `ReadableDuration` — Parses "1h", "30s", "500ms" → duration
- Both implement `From<T> for ConfigValue` for online config integration

---

## 5. Logging and Diagnostics

### 5.1 Logging Architecture

TiKV uses `slog` as its logging framework with a custom dispatcher that routes logs to separate drains based on tags.

```
Log Record
    ↓
GlobalLevelFilter (atomic level check, runtime-changeable)
    ↓
LogDispatcher (routes by tag):
    ├── tag "slow_log*"    → SlowLogFilter → Slow Log Drain (separate file)
    ├── tag "rocksdb_log*" → RocksDB Log Drain
    ├── tag "raftdb_log*"  → RaftDB Log Drain
    └── (default)          → Normal Log Drain
    ↓
ThreadIDrain (adds thread_id to every record)
    ↓
Output (file with rotation, stderr, or both)
```

### 5.2 Log Initialization

```rust
fn init_log(
    drain: D,            // Output destination
    level: Level,        // Initial log level
    use_async: bool,     // Async logging (default: true)
    init_stdlog: bool,   // Redirect std log to slog
    disabled_targets: Vec<String>,  // Modules to silence
    slow_threshold: u64, // Slow log threshold in milliseconds
) -> Result<(), SetLoggerError>
```

### 5.3 Log Formats

**Text Format (TikvFormat)** — Unified TiDB log format:
```
[2024/01/15 10:30:45.123 +08:00] [INFO] [server.rs:256] [msg] key1=value1 key2=value2
```

**JSON Format:**
```json
{"time":"2024-01-15T10:30:45.123+08:00","level":"INFO","caller":"server.rs:256","message":"msg","key1":"value1"}
```

### 5.4 Slow Log

The slow log captures operations that exceed a configurable threshold (default: 1 second).

**Mechanism:**
- Operations emit logs with the `# "slow_log"` slog tag and a `"takes"` key containing a `LogCost(duration_ms)` value
- The `SlowLogFilter` extracts the `"takes"` value and suppresses the log if `cost <= threshold`
- Slow logs are routed to a separate file via the `LogDispatcher`

**Usage Pattern:**
```rust
slog_info!(logger, # "slow_log", "slow operation"; "takes" => LogCost(duration_ms));
```

### 5.5 Log Level Runtime Changes

Log level can be changed at runtime without restart via the online config system:

```
LogConfigManager.dispatch({"level": "debug"})
    ↓
set_log_level(Level::Debug)
    ↓
LOG_LEVEL.store(level, SeqCst)        // AtomicUsize, lock-free
    ↓
GlobalLevelFilter checks LOG_LEVEL.load(Relaxed) per log record
```

### 5.6 File Rotation

Log files are managed by `RotatingFileLogger` with configurable:
- **max_size** — Maximum file size in MB before rotation
- **max_backups** — Number of rotated files to keep
- **max_days** — Maximum age in days before deletion

---

## 6. Cross-References

- **Request flow and flow control integration:** See [gRPC API and Server](grpc_api_and_server.md) §3 (Flow Control)
- **RocksDB engine configuration:** See [Key Encoding and Data Formats](key_encoding_and_data_formats.md) §6 (RocksDB Engine)
- **Raft configuration (timers, region limits):** See [Raft and Replication](raft_and_replication.md) §4 (Raftstore)
- **CDC and backup configuration:** See [CDC and Backup](cdc_and_backup.md)
- **Transaction configuration (pessimistic locks, GC):** See [Transaction and MVCC](transaction_and_mvcc.md)
