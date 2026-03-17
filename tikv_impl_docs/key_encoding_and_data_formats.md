# Key Encoding and Data Formats

This document provides byte-level specifications for all key encoding schemes and data formats used in TiKV. It serves as a complete reference for implementing compatible storage layers.

**Source components:** `components/keys/`, `components/codec/`, `components/txn_types/`, `components/engine_traits/`, `components/engine_rocks/`

---

## 1. Key Space Organization

TiKV's entire key space is divided into two non-overlapping regions distinguished by a single-byte prefix:

| Prefix | Byte | Range | Purpose |
|--------|------|-------|---------|
| `LOCAL_PREFIX` | `0x01` | `[0x01, 0x02)` | Internal metadata: Raft state, region info, apply state |
| `DATA_PREFIX` | `0x7A` (`'z'`) | `[0x7A, 0x7B)` | User data keys (transaction and raw KV data) |

**Source:** `components/keys/src/lib.rs`

### 1.1 Data Key Layout

User keys are prefixed with `DATA_PREFIX` to form data keys:

```
data_key(user_key) = [0x7A] + user_key
```

The `data_key()` function simply prepends the single byte `0x7A`. The reverse operation `origin_key()` strips this prefix. All user-visible keys in RocksDB carry this prefix; the prefix separates user data from internal metadata stored in the same RocksDB instance.

### 1.2 Local Key Layout

Local keys store Raft and region metadata. They are organized with a two-level prefix scheme:

```
Local key = [0x01] + [category_prefix] + [region_id: u64 BE] + [suffix: u8] [+ optional sub_id: u64 BE]
```

**Category prefixes:**

| Category | Byte | Purpose |
|----------|------|---------|
| `REGION_RAFT_PREFIX` | `0x02` | Raft logs and hard state |
| `REGION_META_PREFIX` | `0x03` | Region metadata (region state) |

**Suffixes (under REGION_RAFT_PREFIX):**

| Suffix | Byte | Key Length | Content |
|--------|------|------------|---------|
| `RAFT_LOG_SUFFIX` | `0x01` | 19 bytes | Raft log entry (sub_id = log_index) |
| `RAFT_STATE_SUFFIX` | `0x02` | 11 bytes | Raft hard state (term, vote, commit) |
| `APPLY_STATE_SUFFIX` | `0x03` | 11 bytes | Apply state (applied_index, truncated_state) |
| `SNAPSHOT_RAFT_STATE_SUFFIX` | `0x04` | 11 bytes | Snapshot raft state |

**Suffixes (under REGION_META_PREFIX):**

| Suffix | Byte | Key Length | Content |
|--------|------|------------|---------|
| `REGION_STATE_SUFFIX` | `0x01` | 11 bytes | Region state (peers, key range, epoch) |

**Example — Raft log key for region 42, log index 100:**
```
[0x01, 0x02,  0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x2A,  0x01,  0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x64]
 └─────────┘  └──────────────────────────────────────┘   └──┘   └──────────────────────────────────────┘
 prefix(2B)          region_id=42 (8B BE)              suffix     log_index=100 (8B BE)
```

**Global local keys** (no region_id):

| Key | Bytes | Purpose |
|-----|-------|---------|
| `STORE_IDENT_KEY` | `[0x01, 0x01]` | Store identity (cluster_id, store_id) |
| `PREPARE_BOOTSTRAP_KEY` | `[0x01, 0x02]` | Bootstrap preparation marker |
| `RECOVER_STATE_KEY` | `[0x01, 0x03]` | Recovery state |

---

## 2. Memcomparable Byte Encoding

Raw user keys (and other byte sequences) are encoded into a format that preserves lexicographic ordering under byte comparison. This is the MyRocks-style memcomparable encoding.

**Source:** `components/codec/src/byte.rs`, `components/tikv_util/src/codec/bytes.rs`

### 2.1 Encoding Algorithm (Ascending Order)

**Constants:**
- `ENC_GROUP_SIZE = 8` — bytes per group
- `ENC_MARKER = 0xFF` — separator/terminator for complete groups
- Padding byte: `0x00`

**Algorithm:**
1. Split input into 8-byte chunks
2. For each **complete** 8-byte chunk: emit the 8 bytes followed by marker `0xFF`
3. For the **final** chunk (0 to 8 bytes remaining):
   - If 0 bytes remain, still emit a padding group
   - Pad to 8 bytes with `0x00`
   - Calculate `pad_count = 8 - remaining_bytes`
   - Emit the padded 8 bytes followed by marker `(0xFF - pad_count)`

**Encoded length:** `((input_len / 8) + 1) * 9` bytes

**Example — encoding `"hello"` (5 bytes):**
```
Input:    [0x68, 0x65, 0x6C, 0x6C, 0x6F]               (5 bytes)
Padded:   [0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0x00, 0x00]  (pad_count = 3)
Marker:   0xFF - 3 = 0xFC
Encoded:  [0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0x00, 0x00, 0xFC]  (9 bytes)
```

**Example — encoding `"12345678ab"` (10 bytes):**
```
Group 1:  [0x31..0x38] + [0xFF]                          (9 bytes, complete group)
Group 2:  [0x61, 0x62, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] + [0xF9]  (pad_count = 6)
Encoded:  18 bytes total
```

### 2.2 Descending Order Encoding

For descending order (used in timestamp encoding), every byte of the encoded output is bitwise-inverted (`!byte`). Padding bytes become `0xFF` and the marker logic is correspondingly inverted. This ensures that larger raw values sort before smaller ones under unsigned byte comparison.

### 2.3 Ordering Guarantee

For any raw byte sequences `a` and `b`: `a < b` (lexicographic) ⟺ `encode(a) < encode(b)` (byte comparison). This property is critical for RocksDB's sorted key storage.

---

## 3. Number Encoding

**Source:** `components/codec/src/number.rs`

### 3.1 Fixed-Width Integer Encoding

| Type | Size | Encoding | Ordering |
|------|------|----------|----------|
| `u8` | 1 byte | Direct byte | Ascending |
| `u16` | 2 bytes | Big-endian | Ascending |
| `u32` | 4 bytes | Big-endian | Ascending |
| `u64` | 8 bytes | Big-endian | Ascending |
| `u64_desc` | 8 bytes | Big-endian, all bits inverted (`!v`) | Descending |
| `i64` | 8 bytes | Big-endian, sign bit flipped | Comparable ascending |
| `f64` | 8 bytes | Custom bit reordering | Comparable ascending |

### 3.2 Variable-Length Integer Encoding (VarInt)

Used in Lock and Write record serialization for space efficiency.

- Maximum encoded length: `MAX_VARINT64_LENGTH = 10` bytes
- Standard LEB128-style variable-length encoding
- Used for: `start_ts`, `ttl`, counts, and other metadata fields

### 3.3 Descending u64 Encoding

```rust
fn encode_u64_desc(v: u64) -> [u8; 8] {
    (!v).to_be_bytes()  // Big-endian of bitwise NOT
}
```

This is used for MVCC timestamp encoding in keys, ensuring that **newer versions (larger timestamps) sort before older ones** in RocksDB's ascending key order.

---

## 4. MVCC Key Format

**Source:** `components/txn_types/src/types.rs`

### 4.1 The Key Struct

The `Key` struct represents an encoded user key, optionally with an appended timestamp:

```
Key = memcomparable_encode(raw_user_key) [+ encode_u64_desc(timestamp)]
```

**Construction:**
- `Key::from_raw(raw_key)` — encodes raw bytes using memcomparable encoding
- `Key::append_ts(ts)` — appends an 8-byte descending timestamp

### 4.2 Full MVCC Key Layout

```
┌─────────────────────────────────────────────────────────┐
│ memcomparable_encoded_user_key  │  timestamp (8B desc)  │
│      (variable length)          │  (!ts.to_be_bytes())  │
└─────────────────────────────────────────────────────────┘
```

**Byte-level example — user key `"k1"` at timestamp 400:**
```
Memcomparable("k1"):  [0x6B, 0x31, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF9]  (9 bytes)
encode_u64_desc(400): [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFE, 0x6F]        (8 bytes)
Full key:             [9 bytes of encoded key] + [8 bytes of inverted ts]       (17 bytes)
```

### 4.3 Timestamp (TimeStamp)

**Source:** `components/txn_types/src/timestamp.rs`

A `TimeStamp` is a `u64` composed of two parts:

```
TimeStamp = (physical_ms << 18) | logical
```

- **Physical** (upper 46 bits): Unix timestamp in milliseconds
- **Logical** (lower 18 bits): Sequence counter within the same millisecond (up to 262,144 per ms)

Timestamps are obtained from PD's Timestamp Oracle (TSO), which guarantees monotonic allocation across the cluster.

### 4.4 Key Ordering Properties

1. **Same user key, different timestamps:** Newer versions (larger ts) sort **before** older versions (smaller ts) because of descending timestamp encoding
2. **Different user keys:** Keys are ordered by the raw user key (memcomparable preserves order)
3. **Lock CF keys have no timestamp suffix** — one entry per user key
4. **Write and Default CF keys include the timestamp** — supporting multiple versions

This ordering enables efficient MVCC reads: a forward scan from `encode(user_key) + encode_u64_desc(read_ts)` naturally encounters the most recent visible version first.

---

## 5. Column Family Layouts

TiKV uses four RocksDB column families. Three store transaction data; one stores Raft consensus data.

**Source:** `components/engine_traits/src/cf_defs.rs`

```rust
pub const CF_DEFAULT: &str = "default";
pub const CF_LOCK: &str = "lock";
pub const CF_WRITE: &str = "write";
pub const CF_RAFT: &str = "raft";

pub const DATA_CFS: &[&str] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];  // length = 3
pub const ALL_CFS: &[&str] = &[CF_DEFAULT, CF_LOCK, CF_WRITE, CF_RAFT];
```

### 5.1 CF_DEFAULT — Value Storage

| Aspect | Detail |
|--------|--------|
| **Key format** | `encoded_user_key + encode_u64_desc(start_ts)` |
| **Value** | Raw user value bytes |
| **Purpose** | Stores actual data values that are too large to inline |
| **When used** | Only for values exceeding `SHORT_VALUE_MAX_LEN` (255 bytes) |

Values ≤ 255 bytes are stored inline in Write records (CF_WRITE), so CF_DEFAULT is only used for larger values. The key uses `start_ts` (not `commit_ts`) to link back from the Write record.

### 5.2 CF_LOCK — Lock Storage

| Aspect | Detail |
|--------|--------|
| **Key format** | `encoded_user_key` (no timestamp) |
| **Value** | Serialized `Lock` struct (see §6) |
| **Purpose** | Active transaction locks for concurrency control |
| **Semantics** | Key exists iff there is an active lock on that user key |

Since there is no timestamp in the key, only one lock can exist per user key at any time. This enforces single-writer semantics at the MVCC layer.

### 5.3 CF_WRITE — Write Record Storage

| Aspect | Detail |
|--------|--------|
| **Key format** | `encoded_user_key + encode_u64_desc(commit_ts)` |
| **Value** | Serialized `Write` struct (see §7) |
| **Purpose** | Commit metadata and version history |
| **Contains** | One record per committed (or rolled-back) version |

The key uses `commit_ts` (not `start_ts`), enabling efficient visibility checks: scan backward from `read_ts` to find the latest committed version.

### 5.4 CF_RAFT — Raft State Storage

| Aspect | Detail |
|--------|--------|
| **Key format** | Local key format (§1.2) |
| **Value** | Protobuf-serialized Raft state |
| **Purpose** | Raft log entries, hard state, apply state |

[Inferred] CF_RAFT is typically stored in a separate RocksDB instance from the data CFs to allow independent compaction and lifecycle management.

---

## 6. Lock Record Format

**Source:** `components/txn_types/src/lock.rs`

### 6.1 Lock Struct

```rust
pub struct Lock {
    pub lock_type: LockType,
    pub primary: Vec<u8>,
    pub ts: TimeStamp,              // start_ts of the transaction
    pub ttl: u64,                   // Time-to-live in milliseconds
    pub short_value: Option<Value>, // Inlined value (≤255 bytes)
    pub for_update_ts: TimeStamp,   // Pessimistic lock timestamp
    pub txn_size: u64,              // Hint for transaction size
    pub min_commit_ts: TimeStamp,   // Minimum allowed commit timestamp
    pub use_async_commit: bool,     // Async commit protocol flag
    pub secondaries: Vec<Vec<u8>>,  // Secondary keys for async commit
    pub rollback_ts: Vec<TimeStamp>,// Collapsed rollback timestamps
    pub last_change: LastChange,    // MVCC optimization hint
    pub txn_source: u64,            // Transaction origin identifier
    pub pessimistic_lock_with_conflict: bool,
    pub generation: u64,            // Pipelined DML generation
}
```

### 6.2 Lock Type Flags

| Lock Type | Byte | Description |
|-----------|------|-------------|
| `Put` | `0x50` (`'P'`) | Write intent (optimistic) |
| `Delete` | `0x44` (`'D'`) | Delete intent |
| `Lock` | `0x4C` (`'L'`) | Read lock (SELECT FOR UPDATE without modification) |
| `Pessimistic` | `0x53` (`'S'`) | Pessimistic lock placeholder |
| `Shared` | `0x48` (`'H'`) | Shared read lock |

### 6.3 Serialized Layout

The Lock is serialized as a variable-length byte sequence with prefix-tagged optional fields:

```
┌────────────────────────────────────────────────────────────────────┐
│ Required fields (always present):                                  │
│   [lock_type: 1B]                                                  │
│   [primary_len: varint] [primary_bytes: N bytes]                   │
│   [start_ts: varint]                                               │
│   [ttl: varint]                                                    │
├────────────────────────────────────────────────────────────────────┤
│ Optional fields (prefix-tagged, order-dependent):                  │
│   ['v' 0x76] [len: 1B] [value: len bytes]    — short_value        │
│   ['f' 0x66] [for_update_ts: 8B BE]          — pessimistic ts     │
│   ['t' 0x74] [txn_size: 8B BE]               — transaction size   │
│   ['c' 0x63] [min_commit_ts: 8B BE]          — min commit ts      │
│   ['a' 0x61] [count: varint] {[len: varint] [key: N bytes]}*      │
│                                               — async commit keys  │
│   ['r' 0x72] [count: varint] [ts: 8B BE]*    — rollback ts list   │
│   ['l' 0x6C] [ts: 8B BE] [versions: varint]  — last change hint   │
│   ['s' 0x73] [source: varint]                 — txn source         │
│   ['F' 0x46]                                  — pessimistic w/     │
│                                                 conflict flag      │
│   ['g' 0x67] [generation: 8B BE]             — pipelined gen      │
└────────────────────────────────────────────────────────────────────┘
```

This prefix-tagged format is forward-compatible: unknown prefixes can be skipped by a parser that doesn't recognize them, and new fields can be appended.

---

## 7. Write Record Format

**Source:** `components/txn_types/src/write.rs`

### 7.1 Write Struct

```rust
pub struct Write {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub short_value: Option<Value>,
    pub has_overlapped_rollback: bool,
    pub gc_fence: Option<TimeStamp>,
    pub last_change: LastChange,
    pub txn_source: u64,
}
```

### 7.2 Write Type Flags

| Write Type | Byte | Description |
|------------|------|-------------|
| `Put` | `0x50` (`'P'`) | Data was written |
| `Delete` | `0x44` (`'D'`) | Data was deleted |
| `Lock` | `0x4C` (`'L'`) | Lock-only transaction (no data change) |
| `Rollback` | `0x52` (`'R'`) | Transaction was rolled back |

### 7.3 Serialized Layout

```
┌────────────────────────────────────────────────────────────────────┐
│ Required fields:                                                   │
│   [write_type: 1B]                                                 │
│   [start_ts: varint]                                               │
├────────────────────────────────────────────────────────────────────┤
│ Optional fields (prefix-tagged):                                   │
│   ['v' 0x76] [len: 1B] [value: len bytes]    — short_value        │
│   ['R' 0x52]                                  — overlapped rollback│
│   ['F' 0x46] [gc_fence: 8B BE]               — GC fence ts        │
│   ['l' 0x6C] [ts: 8B BE] [versions: varint]  — last change hint   │
│   ['S' 0x53] [source: varint]                 — txn source         │
└────────────────────────────────────────────────────────────────────┘
```

### 7.4 Short Value Optimization

**Source:** `components/txn_types/src/types.rs`

```
SHORT_VALUE_MAX_LEN = 255
SHORT_VALUE_PREFIX  = 0x76 ('v')
```

- Values ≤ 255 bytes are stored **inline** in the Write record under the `'v'` prefix
- Values > 255 bytes are stored in **CF_DEFAULT** keyed by `(encoded_user_key, start_ts)`
- The Write record's `start_ts` field serves as the pointer to locate the full value in CF_DEFAULT

This optimization avoids a second CF lookup for the majority of small values (typical in OLTP workloads).

### 7.5 GC Fence

The `gc_fence` field (prefix `'F'`) records a timestamp boundary for GC safety. When an overlapped rollback is detected (`has_overlapped_rollback = true`), the GC fence prevents premature collection of a Put record that may still be needed by ongoing reads. [Inferred] The GC process uses this fence to determine the earliest safe point for version cleanup.

### 7.6 Last Change Hint

The `last_change` field (prefix `'l'`) is an MVCC optimization that records information about the previous data-changing version:

```
[last_change_ts: 8B BE] [estimated_versions_to_last_change: varint]
```

This allows MVCC reads to skip over consecutive Lock/Rollback write records efficiently by jumping directly to the last version that actually changed data, rather than scanning through them one by one. [Inferred]

---

## 8. On-Disk Format: RocksDB Usage

**Source:** `components/engine_rocks/src/engine.rs`, `components/engine_rocks/src/util.rs`

### 8.1 RocksDB Instance Layout

TiKV uses RocksDB as its local storage engine. The `RocksEngine` struct wraps a RocksDB `DB` handle:

```rust
struct RocksEngine {
    db: Arc<DB>,
    support_multi_batch_write: bool,
    ingest_latch: Arc<RangeLatch>,  // Mutex between compaction filter and SST ingestion
}
```

Key characteristics:
- The `DB` handle is wrapped in `Arc` for thread-safe shared access across TiKV's thread pools
- `ingest_latch` provides mutual exclusion between compaction filter writes and SST file ingestion to prevent data races
- Column families are created at DB open time; missing CFs are auto-created (`create_missing_column_families(true)`)

### 8.2 Engine Abstraction

**Source:** `components/engine_traits/src/engine.rs`

TiKV abstracts the storage engine behind traits (`KvEngine`, `Snapshot`, etc.), allowing alternative engine implementations (e.g., Titan, future engines). The key trait hierarchy:

- `KvEngine` — core engine operations (get, put, delete, snapshot, sync)
- `Peekable` — point lookups
- `Iterable` — range scans and iteration
- `Mutable` — write operations
- `WriteBatch` — atomic batch writes across column families

### 8.3 SST File Format and Usage

**Source:** `components/engine_traits/src/sst.rs`, `components/sst_importer/src/sst_writer.rs`

SST (Sorted String Table) files are RocksDB's on-disk file format, used in TiKV for:
- **Bulk import** (BR restore, TiDB Lightning)
- **Snapshot transfer** between Raft peers
- **Compaction output**

**SST Writer traits:**
```rust
trait SstWriter {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<()>;
    fn file_size(&mut self) -> u64;
    fn finish(self) -> Result<ExternalSstFileInfo>;
}
```

**SST metadata (ExternalSstFileInfo):**
- `file_path` — path on disk
- `smallest_key` / `largest_key` — key range bounds
- `sequence_number` — RocksDB sequence number
- `file_size` — bytes on disk
- `num_entries` — number of key-value pairs

**Transaction-aware SST writing** (from `sst_importer`):
- The SST writer splits data across two CFs:
  - **CF_DEFAULT** — long values (> 255 bytes), keyed by `(encoded_key, start_ts)`
  - **CF_WRITE** — Write records with short values inlined, keyed by `(encoded_key, commit_ts)`

**Compression options:** `Lz4` (fast), `Snappy` (balanced), `Zstd` (high ratio)

### 8.4 Read and Write Options

**Source:** `components/engine_rocks/src/options.rs`

**Read options of note:**
- `fill_cache` — whether to cache lookup results in block cache
- `total_order_seek_used` — disables bloom filter prefix matching for full range scans
- `hint_min_ts` / `hint_max_ts` — [Inferred] timestamp-aware filtering for MVCC-aware compaction or read optimization
- `iterate_lower_bound` / `iterate_upper_bound` — restricts iteration range (leveraged for region-scoped reads)

**Write options of note:**
- `sync` — forces WAL sync to disk (durability guarantee)
- `disable_wal` — skips write-ahead log (used in specific bulk-load paths)
- `no_slowdown` — fail fast rather than block under write stall

---

## 9. Protobuf Definitions and Key Proto Files

TiKV depends on the `kvproto` crate which provides protobuf definitions shared between TiKV, TiDB, and PD. Key proto-generated types used in the storage and transaction layer:

### 9.1 kvrpcpb (KV RPC Protocol Buffers)

**Used in:** `components/txn_types/src/types.rs`, `components/txn_types/src/lock.rs`, `src/storage/`

| Type | Purpose |
|------|---------|
| `kvrpcpb::Mutation` | Client-submitted key mutation (Put, Delete, Lock, Insert, CheckNotExists, SharedLock) |
| `kvrpcpb::Op` | Operation type enum matching `Mutation` variants |
| `kvrpcpb::LockInfo` | Lock information returned to clients on conflict |
| `kvrpcpb::WriteConflictReason` | Reason code for write conflict errors |
| `kvrpcpb::Assertion` | Key existence assertion for constraint checking |
| `kvrpcpb::IsolationLevel` | Transaction isolation level (SI, RC) |

The internal `Mutation` enum in `txn_types` converts from `kvrpcpb::Mutation`:
```rust
pub enum Mutation {
    Put((Key, Value), Assertion),
    Delete(Key, Assertion),
    Lock(Key, Assertion),
    Insert((Key, Value), Assertion),  // Put with key-not-exists constraint
    CheckNotExists(Key, Assertion),
    SharedLock(Key, u64),             // Shared lock with generation
}
```

### 9.2 metapb (Metadata Protocol Buffers)

| Type | Purpose |
|------|---------|
| `metapb::Region` | Region metadata: id, key range (start_key, end_key), peers, epoch |
| `metapb::Peer` | Node in a Raft group: store_id, peer role |
| `metapb::RegionEpoch` | Version tracking for region splits/merges (conf_ver, version) |

### 9.3 Serialization Conventions

- **Proto messages** (Raft state, region metadata, RPC requests/responses): Protobuf binary encoding via `kvproto`
- **Lock and Write records**: Custom compact binary encoding (§6, §7) — not protobuf — for space efficiency in the hot path
- **User key encoding**: Memcomparable byte encoding (§2) for ordering-preserving storage
- **Numeric fields in keys**: Big-endian fixed-width encoding (§3) for byte-comparable ordering
- **Numeric fields in values**: VarInt encoding where space efficiency matters (timestamps, counts in Lock/Write records)

---

## 10. MVCC Read Path — Key Format in Action

To illustrate how these formats work together, here is the key lookup sequence for an MVCC point read at `read_ts`:

```
1. Check lock:     CF_LOCK[encode(user_key)]
                   → If lock exists with ts ≤ read_ts, report conflict

2. Find version:   Seek CF_WRITE to encode(user_key) + encode_u64_desc(read_ts)
                   → Scan forward (descending ts) to find first Write with commit_ts ≤ read_ts
                   → Read Write record: extract write_type and start_ts

3. Read value:     If write_type == Put:
                     If short_value present → return inline value
                     Else → CF_DEFAULT[encode(user_key) + encode_u64_desc(start_ts)]
                   If write_type == Delete → return "not found"
```

This design ensures that:
- Lock checks are O(1) point lookups (no timestamp in lock keys)
- Version scans naturally find the newest visible version first (descending timestamp order)
- Small values require only one CF lookup (short value optimization)

---

## Cross-References

- **Architecture Overview:** [architecture_overview.md](architecture_overview.md) — system context and request lifecycle
- **Transaction and MVCC:** `impl_docs/transaction_and_mvcc.md` (planned) — Percolator protocol, conflict detection, GC
- **Raft and Replication:** `impl_docs/raft_and_replication.md` (planned) — Raft state stored in local keys and CF_RAFT
- **CDC and Backup:** `impl_docs/cdc_and_backup.md` (planned) — SST export for backup, resolved timestamp tracking
