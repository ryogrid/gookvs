# Transaction and MVCC

This document specifies TiKV's transaction layer, including the Percolator two-phase commit protocol, MVCC (Multi-Version Concurrency Control), conflict detection, concurrency control, GC, and resolved timestamps. It serves as a complete build specification for reimplementing the transaction subsystem.

> **Cross-references:** Key encoding and CF layouts are detailed in [key_encoding_and_data_formats.md](./key_encoding_and_data_formats.md). Raft replication of committed writes is covered in [raft_and_replication.md](./raft_and_replication.md). gRPC request routing into the storage layer is covered in [grpc_api_and_server.md](./grpc_api_and_server.md).

---

## 1. Overview of the Transaction Model

TiKV implements the **Percolator** distributed transaction protocol (Google, 2010), adapted for a Raft-replicated, region-sharded storage engine. Key properties:

- **Snapshot Isolation (SI)** by default, with optional Read Committed (RC) mode
- **Optimistic and pessimistic** transaction modes
- **Distributed two-phase commit (2PC)** with async commit and one-phase commit (1PC) optimizations
- **MVCC** using three RocksDB column families: `CF_LOCK`, `CF_WRITE`, `CF_DEFAULT`
- **Timestamp Oracle** via Placement Driver (PD) for globally ordered `start_ts` and `commit_ts`

### 1.1 Timestamp Semantics

| Timestamp | Source | Purpose |
|-----------|--------|---------|
| `start_ts` | PD TSO at txn begin | Defines the MVCC snapshot; used as version for values in CF_DEFAULT |
| `commit_ts` | PD TSO at commit (or calculated for async commit) | Used as version for write records in CF_WRITE; determines visibility |
| `for_update_ts` | PD TSO at pessimistic lock acquisition | Determines which write conflicts to detect for pessimistic locking |
| `min_commit_ts` | Calculated during prewrite | Lower bound on `commit_ts`; ensures no reader misses uncommitted data |

All timestamps are 64-bit, encoding `(physical_ms << 18) | logical_counter`. They are totally ordered and monotonically increasing.

---

## 2. Column Family Layout (Transaction Perspective)

The three column families store distinct aspects of MVCC state:

### CF_LOCK — Active Locks

- **Key:** `user_key` (no timestamp suffix — one lock per key)
- **Value:** Compact binary-encoded `Lock` record (not protobuf)
- **Lifetime:** Created at prewrite, removed at commit/rollback

### CF_WRITE — Committed Version Metadata

- **Key:** `user_key + commit_ts` (big-endian, bitwise-inverted for descending order)
- **Value:** Compact binary-encoded `Write` record
- **Purpose:** Records what happened (Put/Delete/Lock/Rollback) and points to the value

### CF_DEFAULT — Values

- **Key:** `user_key + start_ts`
- **Value:** Raw user value bytes
- **Used when:** Value exceeds 255 bytes (short values are inlined in the Write or Lock record)

> See [key_encoding_and_data_formats.md](./key_encoding_and_data_formats.md) for byte-level encoding details.

---

## 3. Lock Types and State Machine

**Source:** `components/txn_types/src/lock.rs`

### 3.1 LockType Enum

```
LockType::Put         — Prewrite for a value write
LockType::Delete      — Prewrite for a deletion
LockType::Lock        — Explicit lock-only mutation (no value change)
LockType::Pessimistic — Pessimistic lock acquired before prewrite
```

[Inferred] Shared locks (`SharedLocks`) are a separate mechanism allowing multiple transactions to hold read-locks on the same key concurrently.

### 3.2 Lock Record Fields

```rust
pub struct Lock {
    pub lock_type: LockType,
    pub primary: Vec<u8>,           // Primary key of the transaction
    pub ts: TimeStamp,              // Transaction start_ts
    pub ttl: u64,                   // Time-to-live in milliseconds
    pub short_value: Option<Value>, // Inlined value if ≤ 255 bytes
    pub for_update_ts: TimeStamp,   // Pessimistic lock timestamp (0 for optimistic)
    pub txn_size: u64,              // Estimated transaction size
    pub min_commit_ts: TimeStamp,   // Lower bound on commit_ts
    pub use_async_commit: bool,     // Whether async commit protocol is used
    pub use_one_pc: bool,           // Whether 1PC is used [Inferred]
    pub secondaries: Vec<Vec<u8>>,  // Secondary key list (async commit primary only)
    pub rollback_ts: Vec<TimeStamp>,// Protected rollback timestamps
    pub last_change: LastChange,    // Pointer to last actual Put/Delete version
    pub generation: u64,            // Pipelined DML generation counter
}
```

### 3.3 Lock State Transitions

```
                          ┌─────────────────┐
                          │   No Lock        │
                          └────┬────────┬────┘
         Pessimistic Acquire   │        │  Optimistic Prewrite
                               ▼        ▼
                    ┌──────────────┐  ┌─────────────┐
                    │ Pessimistic  │  │   Locked     │
                    │ (for_update) │  │ (Put/Del/Lk) │
                    └──────┬───────┘  └──┬────┬──────┘
          Prewrite         │             │    │
          (upgrade)        ▼             │    │
                    ┌──────────────┐     │    │
                    │   Locked     │◄────┘    │
                    │ (Put/Del/Lk) │          │
                    └──┬───────┬───┘          │
           Commit      │       │  Rollback    │  TTL Expiry
                       ▼       ▼              ▼
                ┌──────────┐ ┌──────────┐  ┌──────────┐
                │ Write    │ │ Rollback │  │ Rollback │
                │ Record   │ │ Record   │  │ Record   │
                └──────────┘ └──────────┘  └──────────┘
```

**Key invariants:**
- A pessimistic lock MUST be upgraded to a prewrite lock before commit
- A pessimistic lock that isn't upgraded is rolled back at commit time (not committed)
- Lock TTL is checked as: `lock.ts.physical() + lock.ttl < current_ts.physical()`

---

## 4. Write Record Types and Semantics

**Source:** `components/txn_types/src/write.rs`

### 4.1 WriteType Enum

```
WriteType::Put      — Committed value write. Value in short_value or CF_DEFAULT at start_ts
WriteType::Delete   — Committed deletion. Key logically ceases to exist at this version
WriteType::Lock     — Lock-only write record (no value change). Used for explicit locking
WriteType::Rollback — Transaction was aborted. Serves as a tombstone to prevent re-prewrite
```

### 4.2 Write Record Fields

```rust
pub struct Write {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,               // Transaction's start_ts
    pub short_value: Option<Value>,         // Inlined value (≤ 255 bytes)
    pub has_overlapped_rollback: bool,      // A rollback was merged into this record
    pub gc_fence: Option<TimeStamp>,        // GC fence for correctness (see §4.3)
    pub last_change: LastChange,            // Optimization: skip Lock/Rollback records
    pub txn_source: u64,                    // CDC source identifier
}
```

### 4.3 GC Fence Mechanism

**Problem:** When a rollback overlaps with an existing commit record (`has_overlapped_rollback = true`), GC may delete the overlapped version. A stale follower reading at the overlapped timestamp could then see incorrect data.

**Solution:** The `gc_fence` field stores the `commit_ts` of the *next* version. When reading:

```
FUNCTION check_gc_fence_as_latest_version(gc_fence, gc_fence_limit):
    IF gc_fence IS NONE:
        RETURN true                          // No fence, version is valid
    IF gc_fence == 0:
        RETURN false                         // Version was GC'd
    IF gc_fence_limit IS SOME AND gc_fence > gc_fence_limit:
        RETURN false                         // Fence beyond our read horizon
    RETURN true
```

### 4.4 LastChange Optimization

For `Lock` and `Rollback` write records (which carry no data), `last_change` stores:

```rust
pub struct LastChange {
    pub ts: TimeStamp,             // commit_ts of the last Put or Delete
    pub estimated_versions_to_last_change: u64,  // Number of versions to skip
}
```

When `estimated_versions_to_last_change >= SEEK_BOUND (32)`, readers can seek directly to `last_change.ts` instead of iterating through intermediate Lock/Rollback records.

---

## 5. Percolator Protocol: Algorithmic Pseudocode

### 5.1 Prewrite (First Phase of 2PC)

**Source:** `src/storage/txn/actions/prewrite.rs`

```
FUNCTION prewrite(txn, reader, txn_props, mutation, secondary_keys, pessimistic_action):
    // 1. For Insert operations, update max_ts for linearizability
    IF mutation.should_not_exist:
        concurrency_manager.update_max_ts(start_ts)

    // 2. Check existing lock on this key
    lock_status = check_lock(reader, key, txn_props, pessimistic_action)
    MATCH lock_status:
        Locked(min_commit_ts):
            RETURN (min_commit_ts, old_value)   // Already prewrote (idempotent)
        Conflict(lock_info):
            RETURN KeyIsLocked error
        None | Pessimistic:
            CONTINUE

    // 3. Check for write conflicts (newer writes since start_ts)
    (write, commit_ts) = reader.seek_write(key, TimeStamp::MAX)
    IF write IS SOME AND commit_ts > start_ts:
        IF write.write_type != Rollback AND write.write_type != Lock:
            RETURN WriteConflict error {
                start_ts, conflict_start_ts: write.start_ts,
                conflict_commit_ts: commit_ts, key, primary
            }

    // 4. For pessimistic transactions, check for_update_ts
    IF txn_props.is_pessimistic AND pessimistic_action == DoPessimisticCheck:
        IF commit_ts > for_update_ts:
            RETURN WriteConflict error

    // 5. Check data constraints (Insert must not exist)
    IF mutation.should_not_exist:
        check_data_constraint(reader, key, write)
            // Error if latest Put exists before start_ts

    // 6. Calculate min_commit_ts
    min_commit_ts = start_ts + 1
    IF commit_kind IS Async(max_commit_ts) OR OnePc(max_commit_ts):
        min_commit_ts = MAX(txn_props.min_commit_ts,
                           concurrency_manager.max_ts() + 1)
        IF min_commit_ts > max_commit_ts:
            RETURN CommitTsTooLarge error   // Cannot satisfy constraints

    // 7. Write lock and value
    lock = Lock {
        lock_type: mutation_type_to_lock_type(mutation.op),
        primary: txn_props.primary,
        ts: start_ts,
        ttl: txn_props.lock_ttl,
        short_value: IF is_short_value(value) THEN Some(value) ELSE None,
        for_update_ts, min_commit_ts, use_async_commit, secondaries, ...
    }

    IF NOT is_short_value(value):
        txn.put_value(key, start_ts, value)   // Write to CF_DEFAULT

    IF commit_kind IS OnePc:
        txn.locks_for_1pc.push((key, lock, delete_pessimistic_lock))
    ELSE:
        txn.put_lock(key, lock)               // Write to CF_LOCK

    RETURN (min_commit_ts, old_value)
```

### 5.2 Commit (Second Phase of 2PC)

**Source:** `src/storage/txn/actions/commit.rs`

```
FUNCTION commit(txn, reader, key, lock, commit_ts):
    // 1. Validate lock ownership
    IF lock.ts != start_ts:
        // Lock belongs to different transaction or is missing
        RETURN TxnLockNotFound error

    // 2. Check min_commit_ts constraint
    IF commit_ts < lock.min_commit_ts:
        // Async commit: advance commit_ts
        IF lock.use_async_commit:
            commit_ts = lock.min_commit_ts
        ELSE:
            RETURN error   // Cannot commit before min_commit_ts

    // 3. Handle pessimistic lock that wasn't prewrote
    IF lock.lock_type == Pessimistic:
        // Transaction decided not to write this key
        txn.unlock_key(key, is_pessimistic=true)
        RETURN

    // 4. Remove lock
    released_lock = txn.unlock_key(key, is_pessimistic=false)

    // 5. Write commit record
    write = Write {
        write_type: lock_type_to_write_type(lock.lock_type),
        start_ts: lock.ts,
        short_value: lock.short_value,   // Move short value from lock to write
    }
    txn.put_write(key, commit_ts, write)    // Write to CF_WRITE

    RETURN released_lock   // Used for lock manager wake-up
```

### 5.3 Rollback

**Source:** `src/storage/txn/actions/cleanup.rs`, `src/storage/txn/actions/rollback.rs`

```
FUNCTION rollback_lock(txn, reader, key, lock, is_pessimistic, collapse_prev_rollback):
    // 1. Remove the lock
    released = txn.unlock_key(key, is_pessimistic)

    // 2. Check for existing rollback record to avoid duplicates
    overlapped_write = reader.get_txn_commit_record(key, start_ts)
    MATCH overlapped_write:
        TxnCommitRecord::SingleRecord { commit_ts, write }:
            // Overlapped rollback: mark existing write record
            write.has_overlapped_rollback = true
            txn.put_write(key, commit_ts, write)
            RETURN released
        TxnCommitRecord::None:
            CONTINUE   // Write new rollback

    // 3. Collapse previous rollback if allowed
    IF collapse_prev_rollback:
        collapse_prev_rollback(txn, reader, key)
            // Deletes prior rollback record to save space

    // 4. Write rollback marker
    write = Write::new_rollback(start_ts, protected=is_pessimistic)
    txn.put_write(key, start_ts, write)     // Rollback uses start_ts as commit_ts

    RETURN released
```

**Rollback collapse:** To save space, a new rollback deletes the immediately prior rollback record for the same key. Protected rollbacks (from pessimistic transactions) carry a special marker `b"p"` in `short_value` and are not collapsed.

### 5.4 CheckTxnStatus

**Source:** `src/storage/txn/actions/check_txn_status.rs`

Used by the transaction coordinator to determine the fate of a transaction when resolving locks. Called on the **primary key**.

```
FUNCTION check_txn_status(txn, reader, primary_key, lock_ts, caller_start_ts,
                          current_ts, rollback_if_not_exist, force_sync_commit,
                          resolving_pessimistic_lock):
    // Update max_ts for linearizability
    concurrency_manager.update_max_ts(MAX(lock_ts, current_ts, caller_start_ts))

    lock = reader.load_lock(primary_key)

    IF lock IS SOME AND lock.ts == lock_ts:
        // --- Lock exists ---
        RETURN check_txn_status_lock_exists(...)
    ELSE:
        // --- Lock missing ---
        RETURN check_txn_status_missing_lock(...)

FUNCTION check_txn_status_lock_exists(txn, reader, key, lock, current_ts, ...):
    // 1. Handle pessimistic primary lock specially
    IF lock.is_pessimistic_lock():
        result = check_from_pessimistic_primary_lock(...)
        IF result.status IS SOME: RETURN result

    // 2. Force sync commit if requested (disable async commit)
    IF force_sync_commit AND lock.use_async_commit:
        lock.use_async_commit = false
        lock.secondaries = []
        txn.put_lock(key, lock)

    // 3. Check TTL expiration
    IF lock.ts.physical() + lock.ttl < current_ts.physical():
        // Lock expired → rollback
        rollback_lock(txn, reader, key, lock, ...)
        RETURN TxnStatus::TtlExpire
    ELSE:
        // Lock still alive → report status
        RETURN TxnStatus::Uncommitted { lock, min_commit_ts_pushed }

FUNCTION check_txn_status_missing_lock(txn, reader, key, action):
    // Look for commit or rollback record in CF_WRITE
    commit_record = reader.get_txn_commit_record(key, lock_ts)
    MATCH commit_record:
        SingleRecord { commit_ts, write }:
            IF write.write_type != Rollback:
                RETURN TxnStatus::Committed(commit_ts)
            ELSE:
                RETURN TxnStatus::RolledBack
        OverlappedRollback:
            RETURN TxnStatus::RolledBack
        None:
            IF rollback_if_not_exist:
                // Pessimistic rollback: write rollback to prevent future prewrite
                write_rollback_record(txn, reader, key, lock_ts)
                RETURN TxnStatus::LockNotExist
            ELSE:
                RETURN TxnStatus::LockNotExist
```

---

## 6. MvccTxn: Write Accumulator

**Source:** `src/storage/mvcc/txn.rs`

`MvccTxn` is a **write-only** abstraction that accumulates all modifications during a transaction action (prewrite, commit, rollback, etc.) and flushes them as a single batch write to the engine.

### 6.1 Structure

```rust
pub struct MvccTxn {
    pub start_ts: TimeStamp,
    pub write_size: usize,                       // Accumulated write bytes
    pub modifies: Vec<Modify>,                   // Pending CF writes
    pub locks_for_1pc: Vec<(Key, Lock, bool)>,   // Cached locks for 1PC
    pub new_locks: Vec<LockInfo>,                // Newly acquired lock info
    pub concurrency_manager: ConcurrencyManager, // In-memory lock table
    pub guards: Vec<KeyHandleGuard>,             // Lock handle guards
}
```

### 6.2 Key Methods

| Method | CF | Key Format | Purpose |
|--------|----|------------|---------|
| `put_lock(key, lock)` | CF_LOCK | `user_key` | Write a lock record |
| `unlock_key(key, pessimistic, commit_ts)` | CF_LOCK | `user_key` | Delete lock, return `ReleasedLock` |
| `put_value(key, start_ts, value)` | CF_DEFAULT | `user_key{start_ts}` | Write large value |
| `delete_value(key, start_ts)` | CF_DEFAULT | `user_key{start_ts}` | Delete value (GC/rollback) |
| `put_write(key, commit_ts, write)` | CF_WRITE | `user_key{commit_ts}` | Write commit/rollback record |
| `delete_write(key, commit_ts)` | CF_WRITE | `user_key{commit_ts}` | Delete write record (GC) |

All modifications are collected in `modifies: Vec<Modify>` and written atomically via `engine.write(modifies)` after the action completes.

### 6.3 Concurrency Manager Integration

When a lock is created, `MvccTxn` registers it with the `ConcurrencyManager`'s in-memory lock table via `KeyHandleGuard`. This allows concurrent readers to detect locks without hitting CF_LOCK:

```
prewrite → txn.put_lock(key, lock)
         → concurrency_manager.lock_key(key) → KeyHandleGuard (held in txn.guards)
```

---

## 7. MvccReader and Read Path

**Source:** `src/storage/mvcc/reader/reader.rs`

### 7.1 MvccReader Structure

```rust
pub struct MvccReader<S: EngineSnapshot> {
    snapshot: S,
    data_cursor: Option<Cursor<S::Iter>>,    // CF_DEFAULT (lazy)
    lock_cursor: Option<Cursor<S::Iter>>,    // CF_LOCK (lazy)
    write_cursor: Option<Cursor<S::Iter>>,   // CF_WRITE (lazy)
    scan_mode: Option<ScanMode>,             // Forward/Backward/Mixed
    hint_min_ts: Option<Bound<TimeStamp>>,   // Write CF timestamp filter
    statistics: Statistics,                    // Per-CF read metrics
}
```

### 7.2 SnapshotReader Wrapper

```rust
pub struct SnapshotReader<S: EngineSnapshot> {
    pub reader: MvccReader<S>,
    pub start_ts: TimeStamp,
}
```

Provides an MVCC-consistent view at `start_ts`. All version filtering uses this timestamp.

### 7.3 Key Read Operations

**`load_lock(key) → Option<Lock>`**
1. Check in-memory pessimistic locks first (via concurrency manager)
2. If not found, read from CF_LOCK via point get or cursor

**`seek_write(key, ts) → Option<(commit_ts, Write)>`**
1. Seek CF_WRITE cursor to `key{ts}` (finds first entry with `commit_ts ≤ ts`)
2. Verify the user key matches
3. Parse and return the `Write` record with its `commit_ts`

**`get_write(key, ts, gc_fence_limit) → Option<Write>`**
```
FUNCTION get_write(key, ts, gc_fence_limit):
    LOOP:
        (commit_ts, write) = seek_write(key, ts)?
        MATCH write.write_type:
            Put:
                IF NOT write.check_gc_fence(gc_fence_limit): RETURN None
                RETURN Some(write)
            Delete:
                RETURN None
            Lock | Rollback:
                // Skip non-data records
                IF write.last_change.estimated_versions >= SEEK_BOUND:
                    // Optimization: jump directly to last data version
                    ts = write.last_change.ts
                ELSE:
                    ts = commit_ts - 1   // Try next older version
                CONTINUE
```

**`get_txn_commit_record(key, start_ts) → TxnCommitRecord`**

Scans CF_WRITE to find a transaction's commit record by matching `start_ts`:

```
FUNCTION get_txn_commit_record(key, start_ts):
    SEEK write cursor to key{start_ts}
    WHILE cursor is valid AND cursor.key is for same user_key:
        write = parse(cursor.value)
        IF write.start_ts == start_ts:
            RETURN SingleRecord { commit_ts, write }
        IF write.start_ts < start_ts:
            // Check for overlapped rollback
            IF write.has_overlapped_rollback:
                RETURN OverlappedRollback
            RETURN None
        cursor.next()
    RETURN None
```

### 7.4 PointGetter — Optimized Single-Key Read

**Source:** `src/storage/mvcc/reader/point_getter.rs`

```rust
pub struct PointGetter<S: Snapshot> {
    snapshot: S,
    ts: TimeStamp,
    isolation_level: IsolationLevel,
    bypass_locks: TsSet,          // Locks to ignore (e.g., own transaction)
    access_locks: TsSet,          // Locks to read through
    write_cursor: Cursor<S::Iter>,
    statistics: Statistics,
}
```

**Read algorithm:**
```
FUNCTION get(key):
    // 1. Check for blocking locks (SI mode only)
    IF isolation_level == SI:
        lock = load_lock(key)
        IF lock IS SOME AND lock.ts NOT IN bypass_locks:
            IF lock.ts IN access_locks:
                // Read through: use value from lock
                RETURN load_data_from_lock(key, lock)
            ELSE:
                RETURN KeyIsLocked error

    // 2. Find visible write record
    seek write cursor to key{ts}
    LOOP:
        write = read_write_record()
        MATCH write.write_type:
            Put:
                IF write.short_value IS SOME:
                    RETURN write.short_value
                ELSE:
                    RETURN snapshot.get(CF_DEFAULT, key{write.start_ts})
            Delete:
                RETURN None
            Lock | Rollback:
                // Use last_change optimization or iterate
                CONTINUE to next older version
```

### 7.5 Scanner — Range Scans

**Source:** `src/storage/mvcc/reader/scanner/`

Two implementations: `ForwardScanner` and `BackwardScanner`.

**ForwardScanner architecture:**
```
Cursors: write_cursor (CF_WRITE), lock_cursor (CF_LOCK), default_cursor (CF_DEFAULT, lazy)

FUNCTION read_next() → Option<(Key, Value)>:
    // Merge lock and write cursors to find next user key
    (user_key, has_write, has_lock) = advance_to_next_user_key()

    IF has_lock AND isolation_level == SI:
        policy.handle_lock(user_key, lock)
            → HandleRes::Return(entry)    // Read through lock
            → HandleRes::Skip             // Key locked, skip
            → HandleRes::MoveToNext       // Continue

    IF has_write:
        // Find version ≤ ts using move_write_cursor_to_ts()
        write = find_visible_version(user_key, ts)
        policy.handle_write(user_key, write)
            → HandleRes::Return(entry)    // Found visible value
            → HandleRes::Skip             // Deleted or invisible
```

**SEEK_BOUND optimization:** When scanning through versions, the scanner first tries `next()` up to `SEEK_BOUND` (32) times. If the target version is not found within 32 iterations, it falls back to `seek()` for a direct jump. This avoids expensive seek operations for short version chains.

**Scan policies** (pluggable via `ScanPolicy` trait):
- `LatestKvPolicy`: Returns latest `(Key, Value)` pairs — standard reads
- `LatestEntryPolicy`: Returns `TxnEntry` with metadata — used for CDC initial scan
- `DeltaEntryPolicy`: Returns per-version changes — used for CDC incremental scan

---

## 8. Conflict Detection

### 8.1 Write Conflicts

Detected during **prewrite** by checking CF_WRITE for commits after `start_ts`:

```
(latest_write, commit_ts) = seek_write(key, TimeStamp::MAX)
IF commit_ts > start_ts AND write_type IN {Put, Delete}:
    RETURN WriteConflict {
        start_ts, conflict_start_ts: latest_write.start_ts,
        conflict_commit_ts: commit_ts, key, primary, reason
    }
```

For **pessimistic transactions**, the check uses `for_update_ts` instead of `start_ts`:
```
IF commit_ts > for_update_ts:
    RETURN WriteConflict   // A write happened after we acquired pessimistic lock
```

### 8.2 Lock Conflicts

Detected when a transaction encounters another transaction's lock:

**During prewrite:**
```
existing_lock = reader.load_lock(key)
IF existing_lock IS SOME AND existing_lock.ts != start_ts:
    RETURN KeyIsLocked { lock_info }
```

**During read (PointGetter/Scanner):**
```
lock = load_lock(key)
IF lock IS SOME AND lock.ts <= read_ts AND lock.ts NOT IN bypass_locks:
    IF lock.ts IN access_locks:
        RETURN value from lock   // Read through own txn's lock
    ELSE:
        RETURN KeyIsLocked { lock_info, ... }
```

### 8.3 Data Constraint Checking

**Source:** `src/storage/txn/actions/check_data_constraint.rs`

For `Insert` and `CheckNotExists` mutations:

```
FUNCTION check_data_constraint(reader, key, write, start_ts):
    IF write IS NONE:
        RETURN Ok   // Key never existed
    MATCH write.write_type:
        Put:
            RETURN AlreadyExist error
        Delete:
            RETURN Ok   // Key was deleted
        Lock | Rollback:
            // Check older versions
            older_write = reader.get_write(key, write_commit_ts - 1)
            RECURSE with older_write
```

---

## 9. Concurrency Control at the Storage Layer

### 9.1 Storage and Scheduler Architecture

**Source:** `src/storage/mod.rs`, `src/storage/txn/scheduler.rs`

```
                     ┌──────────────────────┐
   gRPC Request ──►  │     Storage API       │
                     │  (async dispatcher)   │
                     └──────────┬────────────┘
                                │
                     ┌──────────▼────────────┐
                     │    TxnScheduler       │
                     │ (command queue +       │
                     │  latch management)     │
                     └──────────┬────────────┘
                                │
                     ┌──────────▼────────────┐
                     │   Worker Thread Pool   │
                     │ (YATP-based, executes  │
                     │  commands with latches) │
                     └──────────┬────────────┘
                                │
                     ┌──────────▼────────────┐
                     │   RaftKv / Engine      │
                     │  (Raft-replicated      │
                     │   write path)          │
                     └────────────────────────┘
```

### 9.2 Latch System

**Source:** `src/storage/txn/latch.rs`

The latch system serializes commands that touch overlapping keys:

```rust
pub struct Latches {
    slots: Vec<CachePadded<Mutex<Latch>>>,  // Power-of-2 hash slots
    size: usize,                             // Number of slots
}

pub struct Lock {
    pub required_hashes: Vec<u64>,   // Sorted, deduplicated key hashes
    pub owned_count: usize,          // How many latches acquired so far
}
```

**Acquisition algorithm (deadlock-free by design):**

```
FUNCTION acquire(lock, command_id) → bool:
    FOR EACH key_hash IN lock.required_hashes[lock.owned_count..]:
        slot = slots[key_hash & (size - 1)]
        first_waiting = slot.get_first_req_by_hash(key_hash)
        MATCH first_waiting:
            Some(cid) WHERE cid == command_id:
                lock.owned_count += 1     // Already at front, acquired
            Some(other_cid):
                slot.enqueue(key_hash, command_id)
                RETURN false              // Must wait
            None:
                slot.enqueue(key_hash, command_id)
                lock.owned_count += 1     // First in queue, acquired
    RETURN lock.owned_count == lock.required_hashes.len()
```

**Invariant:** Key hashes are sorted before acquisition. Since all commands acquire latches in the same order, deadlock is impossible.

**Release:** When a command completes, it releases all latches and wakes the next command waiting on each slot.

### 9.3 ConcurrencyManager — In-Memory Lock Table

**Source:** `components/concurrency_manager/src/lib.rs`

```rust
pub struct ConcurrencyManager {
    max_ts: Arc<AtomicU64>,              // Tracks maximum observed timestamp
    lock_table: LockTable,               // In-memory lock registry
    max_ts_limit: Arc<AtomicCell<MaxTsLimit>>,  // Assertion limit
    // ...
}
```

**Responsibilities:**

1. **`max_ts` tracking:** Updated by prewrite, check_txn_status, and PD TSO sync. Ensures that `min_commit_ts > max_ts` for async commit correctness.

2. **In-memory lock table:** Stores active locks so readers can check for conflicts without CF_LOCK reads. This is critical for `async commit`, where `min_commit_ts` must account for concurrent reads.

3. **`global_min_lock()`:** Returns the minimum `start_ts` across all in-memory locks. Used by resolved timestamp computation.

4. **`update_max_ts(ts)`:** Called whenever a timestamp is observed:
   ```
   FUNCTION update_max_ts(ts):
       LOOP:
           current = max_ts.load()
           IF ts <= current: RETURN
           IF max_ts.compare_and_swap(current, ts): RETURN
   ```

---

## 10. Async Commit and 1PC Optimizations

### 10.1 Async Commit Protocol

**Purpose:** Reduce commit latency by allowing the client to consider the transaction committed once all prewrite locks are successfully written, without waiting for a separate commit phase.

**CommitKind enum:**
```rust
pub enum CommitKind {
    TwoPc,                  // Standard 2-phase commit
    OnePc(TimeStamp),       // Single-phase commit (max_commit_ts)
    Async(TimeStamp),       // Async commit (max_commit_ts)
}
```

**Async commit prewrite:**
```
FUNCTION prewrite_async_commit(txn, key, lock, max_commit_ts):
    min_commit_ts = MAX(txn_props.min_commit_ts,
                        concurrency_manager.max_ts() + 1)
    IF min_commit_ts > max_commit_ts:
        RETURN CommitTsTooLarge   // Fallback to 2PC

    lock.use_async_commit = true
    lock.min_commit_ts = min_commit_ts
    lock.secondaries = secondary_keys   // Primary lock only
    txn.put_lock(key, lock)
```

**Async commit resolution:**

When a reader encounters an async-commit lock, it can determine the transaction's commit status by checking all secondary locks:

```
commit_ts = MAX(primary.min_commit_ts,
                MAX(secondary.min_commit_ts for each secondary))
```

**Fallback:** If `force_sync_commit` is set during `check_txn_status`, the async commit flag is cleared and the transaction falls back to standard 2PC.

### 10.2 One-Phase Commit (1PC)

**Purpose:** For single-region transactions, skip the lock phase entirely and write commit records directly.

**1PC prewrite:**
```
FUNCTION prewrite_1pc(txn, key, lock, max_commit_ts):
    min_commit_ts = MAX(txn_props.min_commit_ts,
                        concurrency_manager.max_ts() + 1)
    IF min_commit_ts > max_commit_ts:
        RETURN CommitTsTooLarge

    lock.use_one_pc = true
    lock.min_commit_ts = min_commit_ts
    // Don't write to CF_LOCK; cache in txn.locks_for_1pc
    txn.locks_for_1pc.push((key, lock, delete_pessimistic))
```

**1PC finalization** (after all mutations prewrote successfully):
```
FUNCTION handle_1pc_locks(txn):
    FOR EACH (key, lock, _) IN txn.locks_for_1pc:
        commit_ts = lock.min_commit_ts
        write = Write {
            write_type: lock_type_to_write_type(lock.lock_type),
            start_ts: lock.ts,
            short_value: lock.short_value,
        }
        txn.put_write(key, commit_ts, write)    // Direct to CF_WRITE
    // No CF_LOCK writes at all — single atomic batch
```

---

## 11. Pessimistic Transactions

### 11.1 Pessimistic Lock Acquisition

Before prewrite, pessimistic transactions acquire locks to prevent write-write conflicts early:

```
FUNCTION acquire_pessimistic_lock(txn, reader, key, start_ts, for_update_ts):
    // 1. Check for conflicting writes after for_update_ts
    (write, commit_ts) = reader.seek_write(key, TimeStamp::MAX)
    IF commit_ts > for_update_ts:
        RETURN WriteConflict

    // 2. Check for existing lock
    lock = reader.load_lock(key)
    IF lock IS SOME AND lock.ts != start_ts:
        RETURN KeyIsLocked

    // 3. Write pessimistic lock
    lock = Lock {
        lock_type: LockType::Pessimistic,
        ts: start_ts,
        for_update_ts: for_update_ts,
        primary, ttl, ...
    }
    txn.put_lock(key, lock)
```

### 11.2 Pessimistic Prewrite (Lock Upgrade)

When prewriting a key that already has a pessimistic lock:

```
FUNCTION check_lock_for_pessimistic(existing_lock, start_ts, for_update_ts):
    IF existing_lock.ts == start_ts AND existing_lock.is_pessimistic_lock():
        // Upgrade pessimistic lock to prewrite lock
        // Delete old pessimistic lock, write new prewrite lock
        RETURN LockStatus::Pessimistic(for_update_ts)
```

The pessimistic lock is replaced by a standard prewrite lock (Put/Delete/Lock type).

### 11.3 Pessimistic Rollback

If a pessimistic transaction is aborted:
```
FUNCTION pessimistic_rollback(txn, reader, key, start_ts, for_update_ts):
    lock = reader.load_lock(key)
    IF lock.ts == start_ts AND lock.is_pessimistic_lock():
        txn.unlock_key(key, is_pessimistic=true)
        // No rollback marker written (pessimistic locks are invisible to readers)
```

---

## 12. GC and Compaction Filter

### 12.1 GC Architecture

**Source:** `src/server/gc_worker/`

```
                    ┌─────────────┐
                    │     PD      │
                    │ (safe_point)│
                    └──────┬──────┘
                           │ periodic poll
                    ┌──────▼──────┐
                    │  GcManager  │  Automatic GC orchestration
                    │ (10s poll)  │  Scans all leader regions
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  GcWorker   │  Task executor
                    │             │  GcTask::Gc, GcKeys, ...
                    └──────┬──────┘
                           │
               ┌───────────┼───────────┐
               ▼           ▼           ▼
          ┌─────────┐ ┌─────────┐ ┌──────────────┐
          │  MvccGc  │ │  GcKeys │ │  Compaction   │
          │ (legacy) │ │ (batch) │ │  Filter (§12.3)│
          └─────────┘ └─────────┘ └──────────────┘
```

### 12.2 Safe Point Semantics

The **safe point** is a timestamp obtained from PD. It guarantees:
- All transactions with `commit_ts ≤ safe_point` have fully committed or been rolled back
- MVCC versions before the safe point can be deleted (keeping the latest per key)

**GcManager polling:**
```
FUNCTION gc_loop():
    LOOP every POLL_SAFE_POINT_INTERVAL_SECS (10s):
        safe_point = pd_client.get_gc_safe_point()
        IF safe_point > last_safe_point:
            FOR EACH region WHERE this_store IS leader:
                schedule GcTask::Gc { safe_point, region }
            last_safe_point = safe_point
```

[Inferred] The GcManager implements a "rewinding" pattern: if the safe point advances during a GC round, it completes the current round with the new safe point, then re-scans earlier regions.

### 12.3 MVCC GC Algorithm

**Source:** `src/storage/txn/actions/gc.rs`

Per-key GC using a state machine:

```
FUNCTION gc(reader, key, safe_point) → GcInfo:
    state = Rewind(safe_point)

    LOOP:
        (commit_ts, write) = reader.seek_write(key, cursor_ts)
        IF write IS NONE: BREAK

        MATCH state:
            Rewind(safe_point):
                IF commit_ts > safe_point:
                    cursor_ts = commit_ts - 1
                    CONTINUE
                ELSE:
                    state = RemoveIdempotent

            RemoveIdempotent:
                MATCH write.write_type:
                    Rollback | Lock:
                        DELETE write at (key, commit_ts)
                        // These are always safe to remove below safe_point
                    Put:
                        // Keep this version (latest before safe_point)
                        state = RemoveAll
                    Delete:
                        // Keep the delete marker; remove everything older
                        state = RemoveAll

            RemoveAll:
                DELETE write at (key, commit_ts)
                IF write.write_type IN {Put, Delete} AND write.short_value IS NONE:
                    DELETE value at (key, write.start_ts) from CF_DEFAULT

        cursor_ts = commit_ts - 1
```

### 12.4 Compaction Filter GC

**Source:** `src/server/gc_worker/compaction_filter.rs`

When enabled, GC runs passively during RocksDB compaction of CF_WRITE:

```rust
pub struct WriteCompactionFilter {
    safe_point: u64,
    engine: RocksEngine,
    // State machine per user key
    state: CompactionFilterState,
}
```

**State machine:**
```
RemoveIdempotent
  ├── Rollback/Lock → DELETE (remove failed txn artifacts)
  ├── Put → transition to RemoveAll (keep latest value)
  └── Delete → transition to RemoveAll (keep delete marker)

RemoveAll
  └── DELETE all older versions
```

**MVCC deletion marks:** When a `Delete` write record is encountered at the bottommost compaction level, the filter schedules a `GcTask::GcKeys` to clean up corresponding CF_DEFAULT values.

**Orphan version handling:** If the RocksDB write batch can't flush (e.g., DB is stalled), versions are sent as `GcTask::OrphanVersions` to the GC worker for deferred cleanup.

### 12.5 Lock TTL and Cleanup

**Source:** `src/storage/txn/actions/cleanup.rs`

Abandoned locks are cleaned via TTL expiration:

```
FUNCTION cleanup(txn, reader, key, current_ts, protect_rollback):
    lock = reader.load_lock(key)
    IF lock IS NONE:
        // Check for existing rollback
        RETURN check_txn_status_missing_lock(...)

    IF current_ts == 0 OR lock.ts.physical() + lock.ttl < current_ts.physical():
        // Lock expired or force cleanup
        rollback_lock(txn, reader, key, lock, is_pessimistic, protect_rollback)
        RETURN TxnStatus::TtlExpire
    ELSE:
        RETURN KeyIsLocked   // Lock still alive
```

---

## 13. Resolved Timestamp

**Source:** `components/resolved_ts/src/`

### 13.1 Definition

The **resolved timestamp** is a per-region watermark with the property:
- **No future commits below it:** All transactions that will commit with `commit_ts ≤ resolved_ts` have already done so
- **Safe for snapshot reads:** A consistent snapshot at `resolved_ts` will never be invalidated by a future commit

### 13.2 Per-Region Resolver

**Source:** `components/resolved_ts/src/resolver.rs`

```rust
pub struct Resolver {
    locks_by_key: HashMap<Arc<[u8]>, TimeStamp>,    // key → start_ts
    lock_ts_heap: BTreeMap<TimeStamp, TxnLocks>,    // start_ts → lock info
    large_txns: HashMap<TimeStamp, TxnLocks>,       // Large txn optimization
    resolved_ts: TimeStamp,
    min_ts: TimeStamp,
}
```

### 13.3 Resolved TS Computation Algorithm

```
FUNCTION resolve(min_ts) → TimeStamp:
    // min_ts = MIN(pd_tso, concurrency_manager.global_min_lock_ts)

    // Find oldest outstanding lock
    min_lock_ts = lock_ts_heap.first_key()   // BTreeMap min, O(1)

    IF min_lock_ts IS SOME:
        new_resolved_ts = MIN(min_lock_ts, min_ts)
    ELSE:
        new_resolved_ts = min_ts

    // Resolved TS only advances forward
    IF new_resolved_ts > resolved_ts:
        resolved_ts = new_resolved_ts

    RETURN resolved_ts
```

### 13.4 Lock Tracking

The resolver receives lock change events from the raftstore observer:

```
ON lock_created(key, start_ts):
    locks_by_key[key] = start_ts
    lock_ts_heap[start_ts].lock_count += 1

ON lock_removed(key):
    start_ts = locks_by_key.remove(key)
    lock_ts_heap[start_ts].lock_count -= 1
    IF lock_ts_heap[start_ts].lock_count == 0:
        lock_ts_heap.remove(start_ts)
```

### 13.5 AdvanceTsWorker

**Source:** `components/resolved_ts/src/advance.rs`

Periodically advances `min_ts` using PD TSO:

```
FUNCTION advance_ts():
    // 1. Get latest timestamp from PD
    min_ts = pd_client.get_tso()

    // 2. Sync with concurrency manager
    concurrency_manager.update_max_ts(min_ts)

    // 3. Check in-memory locks
    mem_lock_min = concurrency_manager.global_min_lock()
    IF mem_lock_min < min_ts:
        min_ts = mem_lock_min

    // 4. Advance all region resolvers
    FOR EACH region IN observed_regions:
        region.resolver.resolve(min_ts)
```

### 13.6 Consumers

| Consumer | Usage |
|----------|-------|
| **CDC** | Events are safe to emit once `resolved_ts` advances past their `commit_ts` |
| **Stale reads** | A follower can serve reads at timestamp `t` if `t ≤ safe_ts` (derived from resolved_ts) |
| **TiFlash** | Replica consistency checkpoint based on resolved_ts |

---

## 14. Isolation Levels

TiKV supports multiple isolation levels, configured per-request:

| Level | Lock Checking | Version Visibility | Use Case |
|-------|--------------|-------------------|----------|
| **SI (Snapshot Isolation)** | Yes — blocks on locks from other txns | Latest committed version ≤ `start_ts` | Default for transactions |
| **RC (Read Committed)** | No — ignores all locks | Latest committed version ≤ `start_ts` | Non-transactional reads |
| **RcCheckTs** | Returns error if newer writes exist | Latest committed version ≤ `start_ts` | Optimistic read validation |

---

## 15. Concurrency Control Summary

```
Layer               Mechanism                         Granularity
─────────────────── ───────────────────────────────── ──────────────────
gRPC                Request queuing, backpressure      Per-connection
Storage API         TxnScheduler + Latches             Per-key hash slot
MVCC                Lock records in CF_LOCK            Per-user-key
                    ConcurrencyManager in-memory locks Per-user-key
Raft                Proposal serialization per region  Per-region
Engine (RocksDB)    Internal concurrency control       Per-CF
```

**Key invariant chain:**
1. Latches prevent two commands from operating on the same keys simultaneously
2. MVCC locks prevent two transactions from writing the same key simultaneously
3. `min_commit_ts > max_ts` prevents a committed-but-not-yet-visible write from being missed by a concurrent reader
4. Resolved timestamp provides a safe read point for CDC and stale reads

---

## 16. Key Source File Reference

| File | Purpose |
|------|---------|
| `src/storage/mod.rs` | Storage API entry point |
| `src/storage/txn/scheduler.rs` | TxnScheduler, command dispatch, latch management |
| `src/storage/txn/latch.rs` | Latch implementation (deadlock-free key serialization) |
| `src/storage/txn/actions/prewrite.rs` | Prewrite action, CommitKind, TransactionProperties |
| `src/storage/txn/actions/commit.rs` | Commit action |
| `src/storage/txn/actions/cleanup.rs` | Rollback/cleanup action |
| `src/storage/txn/actions/check_txn_status.rs` | Transaction status resolution |
| `src/storage/txn/actions/check_data_constraint.rs` | Insert/CheckNotExists constraint |
| `src/storage/txn/actions/gc.rs` | Per-key MVCC GC |
| `src/storage/mvcc/txn.rs` | MvccTxn write accumulator |
| `src/storage/mvcc/reader/reader.rs` | MvccReader, SnapshotReader |
| `src/storage/mvcc/reader/point_getter.rs` | Single-key MVCC read |
| `src/storage/mvcc/reader/scanner/forward.rs` | Forward MVCC scanner |
| `src/storage/mvcc/reader/scanner/backward.rs` | Backward MVCC scanner |
| `components/txn_types/src/lock.rs` | Lock type definitions and serialization |
| `components/txn_types/src/write.rs` | Write type definitions and serialization |
| `components/concurrency_manager/src/lib.rs` | ConcurrencyManager, in-memory lock table |
| `src/server/gc_worker/gc_worker.rs` | GC task scheduler |
| `src/server/gc_worker/gc_manager.rs` | Automatic GC orchestration |
| `src/server/gc_worker/compaction_filter.rs` | RocksDB compaction filter for GC |
| `components/resolved_ts/src/resolver.rs` | Per-region resolved timestamp |
| `components/resolved_ts/src/advance.rs` | Resolved TS advancement worker |
