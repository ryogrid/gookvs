# Transaction Processing Layer

## 1. Overview

The gookv transaction layer implements Percolator-style two-phase commit (2PC) with multi-version concurrency control (MVCC). The design stores multiple versions of each key across three column families -- CF_DEFAULT (large values), CF_LOCK (active transaction locks), and CF_WRITE (commit/rollback metadata) -- and uses timestamp-based visibility to provide snapshot isolation and read-committed isolation levels.

The layer is organized into the following components:

- **MVCC Key Encoding** (`internal/storage/mvcc/key.go`): Encodes user keys with descending timestamps so that newer versions sort first in the storage engine.
- **MvccTxn** (`internal/storage/mvcc/txn.go`): A write-only accumulator that collects all column-family modifications during a single transaction action and flushes them as one atomic batch.
- **MvccReader** (`internal/storage/mvcc/reader.go`): Provides MVCC-aware reads across all column families, backed by a storage engine snapshot.
- **PointGetter** (`internal/storage/mvcc/point_getter.go`): An optimized single-key reader supporting SI and RC isolation levels.
- **2PC Actions** (`internal/storage/txn/actions.go`): Prewrite, Commit, Rollback, CheckTxnStatus, and CheckTxnStatusWithCleanup -- the core Percolator protocol operations.
- **Async Commit and 1PC** (`internal/storage/txn/async_commit.go`): Optimizations that reduce latency for eligible transactions.
- **Pessimistic Locking** (`internal/storage/txn/pessimistic.go`): Lock-before-write support for interactive transactions.
- **Latches** (`internal/storage/txn/latch/latch.go`): Hash-based slot array providing deadlock-free command serialization.
- **Concurrency Manager** (`internal/storage/txn/concurrency/manager.go`): Tracks max_ts (atomically) and an in-memory lock table (sync.Map) for async commit correctness.

In addition to PointGetter for single-key reads, the package now includes a full **Scanner** (`internal/storage/mvcc/scanner.go`) for MVCC-aware range scans. A **GC Worker** (`internal/storage/gc/gc.go`) reclaims stale MVCC versions below a safe point, and a **TxnScheduler** (`internal/storage/txn/scheduler/scheduler.go`) dispatches transaction commands through a worker pool with latch-based key serialization.

### Column Families

Defined in `pkg/cfnames/cfnames.go`:

| Constant    | Value       | Purpose                                |
|-------------|-------------|----------------------------------------|
| `CFDefault` | `"default"` | Large values (> 255 bytes)             |
| `CFLock`    | `"lock"`    | Active transaction locks               |
| `CFWrite`   | `"write"`   | Commit / rollback metadata             |
| `CFRaft`    | `"raft"`    | Raft state (not used by txn layer)     |

### Timestamp Model

Defined in `pkg/txntypes/timestamp.go`. `TimeStamp` is a `uint64` hybrid logical clock value from PD's TSO. The upper 46 bits hold a physical millisecond component; the lower 18 bits (`TSLogicalBits = 18`) hold a logical sequence number. Key constants: `TSMax = math.MaxUint64`, `TSZero = 0`. Helper methods: `Physical()`, `Logical()`, `Prev()`, `Next()`, `IsZero()`, and `ComposeTS(physical, logical)`.

---

## 2. MVCC Key Encoding

**File:** `internal/storage/mvcc/key.go`

### EncodeKey / DecodeKey

`EncodeKey(userKey, ts)` produces the encoded key used in CF_WRITE and CF_DEFAULT:

```
[codec.EncodeBytes(userKey)] [codec.EncodeUint64Desc(ts)]
```

The timestamp is encoded in **descending** order (`EncodeUint64Desc`) so that newer versions sort before older versions in the storage engine's lexicographic ordering. This means a simple forward seek finds the newest version first.

`DecodeKey(encodedKey)` reverses the process, returning the user key and the timestamp. If the encoded key has fewer than 8 bytes after the user-key portion, the timestamp is returned as 0 (this is the case for CF_LOCK keys).

### EncodeLockKey / DecodeLockKey

Lock keys in CF_LOCK carry **no timestamp**:

```go
func EncodeLockKey(key Key) []byte {
    return codec.EncodeBytes(nil, key)
}
```

`DecodeLockKey` simply calls `codec.DecodeBytes` to extract the user key.

### TruncateToUserKey

Strips the last 8 bytes (the timestamp suffix) from an encoded MVCC key, returning only the encoded user-key portion. Used by `MvccReader.SeekWrite` to verify that a found key belongs to the expected user key.

### SeekBound

```go
const SeekBound = 32
```

When `GetWrite` encounters more than `SeekBound` consecutive non-data-changing write versions (Lock or Rollback records), it switches to the `LastChange` optimization -- jumping directly to the timestamp of the last known data-changing version instead of scanning one-by-one.

---

## 3. MvccTxn (Write Accumulator)

**File:** `internal/storage/mvcc/txn.go`

`MvccTxn` is a pure accumulator: it collects modifications during a single transaction action (prewrite, commit, rollback) and does not perform any reads. All collected modifications are later flushed to the storage engine as one atomic batch write.

### Struct

```go
type MvccTxn struct {
    StartTS   txntypes.TimeStamp
    Modifies  []Modify
    WriteSize int
}
```

### Modify

```go
type ModifyType int

const (
    ModifyTypePut         ModifyType = 0
    ModifyTypeDelete      ModifyType = 1
    ModifyTypeDeleteRange ModifyType = 2
)

type Modify struct {
    Type   ModifyType   // ModifyTypePut, ModifyTypeDelete, or ModifyTypeDeleteRange
    CF     string       // Column family name
    Key    []byte       // Encoded key (start key for DeleteRange)
    Value  []byte       // Value (nil for deletes)
    EndKey []byte       // End key (exclusive) — only used with ModifyTypeDeleteRange
}
```

`ModifyTypeDeleteRange` is used by `KvDeleteRange` to delete all keys in a range within a column family. The `EndKey` field specifies the exclusive upper bound of the range to delete.

### ReleasedLock

Returned by `UnlockKey` to carry information about the released lock for lock-manager wake-up notifications:

```go
type ReleasedLock struct {
    Key           Key
    StartTS       txntypes.TimeStamp
    IsPessimistic bool
}
```

### Methods

| Method | Description |
|--------|-------------|
| `PutLock(key, lock)` | Marshals the lock and appends a Put to CF_LOCK with `EncodeLockKey(key)`. |
| `UnlockKey(key, isPessimistic)` | Appends a Delete to CF_LOCK. Returns a `*ReleasedLock`. |
| `PutValue(key, startTS, value)` | Appends a Put to CF_DEFAULT with `EncodeKey(key, startTS)`. Used for large values (> `ShortValueMaxLen`). |
| `DeleteValue(key, startTS)` | Appends a Delete to CF_DEFAULT. Used during rollback to clean up written values. |
| `PutWrite(key, commitTS, write)` | Marshals the write record and appends a Put to CF_WRITE with `EncodeKey(key, commitTS)`. |
| `DeleteWrite(key, commitTS)` | Appends a Delete to CF_WRITE. |
| `ModifyCount()` | Returns `len(Modifies)`. |

All methods update `WriteSize` to track the approximate byte cost of the accumulated batch.

---

## 4. MvccReader

**File:** `internal/storage/mvcc/reader.go`

`MvccReader` wraps a `traits.Snapshot` (a read-only point-in-time view of the storage engine) and provides MVCC-aware read operations.

### Struct

```go
type MvccReader struct {
    snapshot traits.Snapshot
}
```

### LoadLock

```go
func (r *MvccReader) LoadLock(key Key) (*txntypes.Lock, error)
```

Reads the lock for `key` from CF_LOCK. Returns `(nil, nil)` if no lock exists (distinguishes "not found" via `traits.ErrNotFound`). Calls `txntypes.UnmarshalLock` to deserialize the binary lock data.

### SeekWrite

```go
func (r *MvccReader) SeekWrite(key Key, ts TimeStamp) (*txntypes.Write, TimeStamp, error)
```

Finds the first write record for `key` with `commitTS <= ts`. Creates an iterator on CF_WRITE, seeks to `EncodeKey(key, ts)`, and checks that the found key's encoded user-key prefix matches. Returns `(nil, 0, nil)` if no matching write exists.

### GetWrite

```go
func (r *MvccReader) GetWrite(key Key, ts TimeStamp) (*txntypes.Write, TimeStamp, error)
```

Finds the latest **data-changing** write (Put or Delete) visible at `ts`. Uses a single iterator on CF_WRITE that scans downward through all write records for the key. The iterator seeks to `EncodeKey(key, ts)` and then advances via `iter.Next()`, checking each record's write type:

- **Put** or **Delete**: Returns immediately (data-changing write found). For `WriteTypeDelete`, returns `(nil, 0, nil)` — the key is considered deleted at that version.
- **Lock** or **Rollback** (non-data-changing): Two skip strategies:
  1. **LastChange optimization**: If `write.LastChange.EstimatedVersions >= SeekBound` and the LastChange timestamp is non-zero, jumps directly via `iter.Seek(EncodeKey(key, write.LastChange.TS))` to skip many stale versions.
  2. **Sequential scan**: Otherwise, calls `iter.Next()` to check the next older version.

The loop continues until a data-changing write is found or the scan exhausts all records for the key (iterator moves past the key's encoded user-key prefix). Unlike the previous `SeekWrite`-based loop, there is no fixed iteration limit — this eliminates the issue where excessive rollback/lock records could prevent finding the correct data-changing write.

### GetTxnCommitRecord

```go
func (r *MvccReader) GetTxnCommitRecord(key Key, startTS TimeStamp) (*txntypes.Write, TimeStamp, error)
```

Scans CF_WRITE from `TSMax` downward to find the write record whose `StartTS` matches the given `startTS`. Stops scanning when `commitTS < startTS` (the record cannot exist below that point). Used by Rollback and CheckTxnStatus to determine whether a transaction has already been committed or rolled back.

### GetValue

```go
func (r *MvccReader) GetValue(key Key, startTS TimeStamp) ([]byte, error)
```

Point-reads from CF_DEFAULT using `EncodeKey(key, startTS)`. Returns `(nil, nil)` on `ErrNotFound`.

### Close

Releases the underlying snapshot.

---

## 5. PointGetter

**File:** `internal/storage/mvcc/point_getter.go`

`PointGetter` performs optimized single-key MVCC reads, supporting two isolation levels.

### Isolation Levels

```go
const (
    IsolationLevelSI IsolationLevel = iota   // Snapshot Isolation (default)
    IsolationLevelRC                          // Read Committed
)
```

### Struct

```go
type PointGetter struct {
    reader         *MvccReader
    ts             txntypes.TimeStamp
    isolationLevel IsolationLevel
    bypassLocks    map[txntypes.TimeStamp]bool
}
```

`bypassLocks` allows specific lock timestamps to be skipped (e.g., the transaction's own locks).

### Get Algorithm

`Get(key)` proceeds in three steps:

1. **Lock check (SI only):** Calls `reader.LoadLock(key)`. If a lock exists with `StartTS <= ts`, is not `LockTypePessimistic`, and is not in `bypassLocks`, returns `&LockError{Key: key, Lock: lock}`. Pessimistic locks (`LockTypePessimistic`) are skipped because they are invisible to readers -- they represent a lock-before-write reservation, not an actual data modification. This matches the Scanner's behavior. The `LockError` struct (defined in `point_getter.go`) carries the conflicting key and lock metadata. `errors.Is(err, ErrKeyIsLocked)` still works via `LockError.Is()`. Under RC isolation, this step is skipped entirely.

2. **Find visible write:** Calls `reader.GetWrite(key, ts)` to find the latest data-changing write (Put) visible at `ts`. If no write is found (or it was a Delete), returns `(nil, nil)`.

3. **Value retrieval:** If the write record contains a `ShortValue` (inlined, <= 255 bytes), returns it directly. Otherwise, calls `reader.GetValue(key, write.StartTS)` to fetch the large value from CF_DEFAULT.

---

## 6. Scanner

**File:** `internal/storage/mvcc/scanner.go`

`Scanner` performs MVCC-aware range scans with persistent cursor state, supporting both forward and backward iteration with SI and RC isolation levels.

### ScannerConfig

```go
type ScannerConfig struct {
    Snapshot       traits.Snapshot
    ReadTS         txntypes.TimeStamp
    Desc           bool               // true for backward scan
    IsolationLevel IsolationLevel
    KeyOnly        bool               // skip value loading
    LowerBound     Key
    UpperBound     Key
    BypassLocks    map[txntypes.TimeStamp]bool
}
```

`BypassLocks` allows specific lock timestamps to be skipped (e.g., the transaction's own locks), matching the same concept used by `PointGetter`.

### ScanStatistics

```go
type ScanStatistics struct {
    ScannedKeys   int64
    ProcessedKeys int64
    ScannedLocks  int64
    DefaultReads  int64
    OverSeekBound int64
}
```

Counters are accumulated during scanning. `TakeStatistics()` returns the accumulated values and resets all counters to zero.

### Scanner Struct

```go
type Scanner struct {
    cfg           ScannerConfig
    writeCursor   traits.Iterator   // always created
    lockCursor    traits.Iterator   // nil under RC isolation
    defaultCursor traits.Iterator   // lazily created on first large-value read
    isStarted     bool
    stats         ScanStatistics
}
```

`NewScanner(cfg)` creates iterators on CF_WRITE and (for SI) CF_LOCK with bounds derived from `LowerBound`/`UpperBound`. The default CF iterator is not created upfront; it is lazily initialized the first time `loadValueFromDefault` is called. The caller must call `Close()` when done to release all iterator resources.

### Next() Algorithm

`Next()` returns the next visible key-value pair, or `(nil, nil, nil)` when the scan is exhausted.

1. **Initial seek:** On the first call, the write cursor and lock cursor are positioned at the first (forward) or last (backward) entry within bounds.

2. **Current user key selection:** At each iteration, the scanner extracts the user key from both the write and lock cursors. For forward scans, the minimum user key is chosen; for backward scans, the maximum. This determines which cursor(s) have data for the current key.

3. **Bound check:** The selected user key is checked against `UpperBound` (forward) or `LowerBound` (backward). If out of range, the scan terminates.

4. **Lock handling (SI only):** If the lock cursor has an entry for the current user key, `handleLock` checks for conflicts. Pessimistic locks are skipped. If a non-pessimistic lock exists with `StartTS <= ReadTS` and is not in `BypassLocks`, the scanner returns `&LockError{Key: userKey, Lock: lock}` (which satisfies `errors.Is(err, ErrKeyIsLocked)` via the `Is()` method). After checking, the lock cursor is advanced past this key.

5. **Write handling:** If the write cursor has an entry for the current user key, `handleWrite` searches for the latest visible version.

6. **Skip keys with no visible data:** If only a lock exists (no write) or `handleWrite` finds no visible version (deleted or no version at `ReadTS`), the loop continues to the next user key.

### handleWrite

`handleWrite(userKey)` finds the latest data-changing write record with `commitTS <= ReadTS`:

1. For backward scans, the write cursor is first repositioned with `Seek(EncodeKey(userKey, ReadTS))` since the natural backward position may be at an older version.

2. `moveWriteCursorToTS` advances through write entries with `commitTS > ReadTS`, using linear iteration up to `SeekBound` attempts before falling back to `Seek`.

3. The method iterates write records for the user key and dispatches by write type:
   - **Put:** If `KeyOnly` is false, retrieves the value. Short values (inlined in the write record) are returned directly. Large values are fetched via `loadValueFromDefault`. Returns `(value, true, nil)`.
   - **Delete:** The key is considered deleted at this version. Returns `(nil, false, nil)`.
   - **Lock / Rollback:** Non-data-changing records are skipped. If `LastChange.EstimatedVersions >= SeekBound` and the `LastChange.TS` is non-zero, the cursor jumps directly to that timestamp (same optimization as `MvccReader.GetWrite`). Otherwise, `Next()` advances to the next version.

4. After processing, `moveWriteCursorToNextUserKey` advances the write cursor past all remaining versions of the current user key.

### Forward / Backward Scan Support

Forward and backward scans share the same `Next()` loop but differ in cursor movement:

- **Forward:** Cursors advance with `Next()`. `moveWriteCursorToNextUserKey` calls `Next()` up to `SeekBound` times, then falls back to `Seek` with a key one byte past the current user key.
- **Backward:** Cursors advance with `Prev()`. After the forward version lookup within a user key, `repositionForBackward` seeks to the first entry of the current user key (highest `commitTS`) and then calls `Prev()` to position before all versions of that key.

### Lazy Default CF Cursor

`loadValueFromDefault(userKey, startTS)` creates the default CF iterator on first use via `ensureDefaultCursor()`. It seeks to `EncodeKey(userKey, startTS)` and verifies the found key matches exactly. Each call increments `DefaultReads` in `ScanStatistics`.

---

## 6.5. GC Worker

**File:** `internal/storage/gc/gc.go`

The GC subsystem reclaims stale MVCC versions below a safe point, removing obsolete write records from CF_WRITE and large values from CF_DEFAULT.

### GC() Function

```go
func GC(txn *MvccTxn, reader *MvccReader, key Key, safePoint TimeStamp) (*GCInfo, error)
```

Performs garbage collection on a single key using a 3-state machine that scans CF_WRITE from newest to oldest:

1. **gcStateRewind:** Skips all write records with `commitTS > safePoint`. These versions are still live and must be preserved.

2. **gcStateRemoveIdempotent:** Entered when the first write record at or below the safe point is reached. Lock and Rollback records are deleted immediately (they carry no user-visible data). The first Put or Delete record is kept as the latest visible version below the safe point, and the state transitions to `gcStateRemoveAll`.

3. **gcStateRemoveAll:** All remaining older versions are deleted. For Put records with large values (no `ShortValue`), the corresponding CF_DEFAULT entry is also deleted via `txn.DeleteValue(key, write.StartTS)`.

Returns a `GCInfo` with `FoundVersions`, `DeletedVersions`, and `IsCompleted`.

### GCConfig

```go
type GCConfig struct {
    PollSafePointInterval time.Duration   // default: 10s
    MaxWriteBytesPerSec   int64           // default: 0 (unlimited)
    MaxTxnWriteSize       int             // default: 32 KB
    BatchKeys             int             // default: 512
}
```

`DefaultGCConfig()` returns a config with the defaults listed above. `MaxTxnWriteSize` controls when accumulated modifications are flushed to the engine during a GC pass.

### GCTask and GCWorkerStats

```go
type GCTask struct {
    SafePoint txntypes.TimeStamp
    StartKey  []byte
    EndKey    []byte
    Callback  func(error)
}

type GCWorkerStats struct {
    KeysScanned     int64
    VersionsDeleted int64
}
```

`GCTask` represents a unit of GC work scoped to a key range. The `Callback` is invoked with the result of the task.

### GCWorker

```go
type GCWorker struct {
    engine          traits.KvEngine
    taskCh          chan GCTask         // buffered, capacity 64
    config          *GCConfig
    keysScanned     atomic.Int64
    versionsDeleted atomic.Int64
    stopCh          chan struct{}
    wg              sync.WaitGroup
}
```

| Method | Description |
|--------|-------------|
| `NewGCWorker(engine, config)` | Creates a new worker. If `config` is nil, uses `DefaultGCConfig()`. |
| `Start()` | Launches the background goroutine that processes tasks from `taskCh`. |
| `Stop()` | Closes `stopCh` and waits for the goroutine to exit. |
| `Schedule(task)` | Non-blocking enqueue. Returns `ErrGCTaskQueueFull` if the channel is full. |
| `Stats()` | Returns a snapshot of `GCWorkerStats` (atomic loads). |

### processTask

`processTask` takes a snapshot of the engine, creates an `MvccReader`, and scans CF_WRITE within the task's `[StartKey, EndKey)` range. It deduplicates by user key (tracking `lastUserKey`) so that each key is GC'd exactly once on first encounter. For each unique user key, it calls `GC(txn, reader, key, safePoint)` to collect modifications. When `txn.WriteSize` exceeds `MaxTxnWriteSize`, the accumulated modifications are flushed atomically via `applyModifies`, which creates a `WriteBatch`, applies all puts and deletes, and calls `Commit()`.

### SafePointProvider

```go
type SafePointProvider interface {
    GetGCSafePoint(ctx context.Context) (txntypes.TimeStamp, error)
}
```

Abstracts the retrieval of the GC safe point (typically from PD). A `MockSafePointProvider` is provided for testing, with `SetSafePoint(ts)` and an `atomic.Uint64` backing store.

### PD Safe Point Integration

**File:** `internal/storage/gc/pd_safe_point.go`

`PDSafePointProvider` implements `SafePointProvider` by wrapping a `pdclient.Client`:

```go
type PDSafePointProvider struct {
    pdClient pdclient.Client
}
```

- `NewPDSafePointProvider(pdClient)` — Creates a provider backed by PD.
- `GetGCSafePoint(ctx)` — Calls `pdClient.GetGCSafePoint(ctx)` and converts the returned `uint64` to `txntypes.TimeStamp`.

The `KvGC` gRPC handler now integrates with PD: after performing local GC, it calls `pdClient.UpdateGCSafePoint(ctx, safePoint)` to centralize the safe point in PD. This ensures all nodes in the cluster observe a consistent GC safe point.

---

## 6.6. TxnScheduler

**File:** `internal/storage/txn/scheduler/scheduler.go`

The TxnScheduler dispatches transaction commands to a worker pool with latch-based key serialization, decoupling gRPC request handling from MVCC operations.

### Command Interface

```go
type Command interface {
    Kind() CommandKind
    Keys() [][]byte
    Execute(ctx CommandContext) (*CommandResult, error)
}
```

Each command declares the keys it touches (for latch acquisition) and implements `Execute`, which receives a `CommandContext` containing the `KvEngine` and `ConcurrencyManager`.

### CommandKind Constants

```go
const (
    CmdPrewrite CommandKind = iota
    CmdCommit
    CmdBatchRollback
    CmdCleanup
    CmdCheckTxnStatus
    CmdPessimisticLock
    CmdPessimisticRollback
    CmdResolveLock
    CmdTxnHeartBeat
)
```

### Config

```go
type Config struct {
    SchedulerConcurrency  int             // latch slot count (default: 2048)
    WorkerCount           int             // goroutine pool size (default: runtime.NumCPU())
    PendingWriteThreshold int64           // default: 100 MB
    Engine                traits.KvEngine
    ConcurrencyManager    *concurrency.Manager
}
```

### TxnScheduler Struct

```go
type TxnScheduler struct {
    latches               *latch.Latches
    taskSlots             []taskSlot          // 4096 slots
    idAlloc               atomic.Uint64
    taskCh                chan scheduledTask   // buffered, capacity WorkerCount*4
    runningWriteBytes     atomic.Int64
    pendingWriteThreshold int64
    engine                traits.KvEngine
    concMgr               *concurrency.Manager
    ctx                   context.Context
    cancel                context.CancelFunc
}
```

The scheduler maintains 4096 task slots (hash-indexed by command ID) for tracking in-flight commands, and a fixed-size worker pool consuming from `taskCh`.

### RunCommand / RunCommandSync

| Method | Description |
|--------|-------------|
| `RunCommand(cmd, callback)` | Submits a command for asynchronous execution. Allocates a command ID, creates a `latch.Lock` from the command's keys, stores the task context in a slot, and attempts to acquire latches. If latches are acquired immediately, the task is dispatched to the worker channel. If not, the task waits in its slot until woken. The callback is invoked exactly once. |
| `RunCommandSync(cmd)` | Blocking wrapper around `RunCommand`. Creates a buffered channel, submits the command with a callback that sends the result, and blocks on the channel. Returns `(*CommandResult, error)`. |
| `Stop()` | Cancels the context, closes `taskCh`, and waits for all workers to finish via `workerWg.Wait()`. |
| `WaitIdle(timeout)` | Polls all task slots until none have pending tasks, or the timeout expires. Returns `true` if idle. Intended for testing. |

### Latch Wakeup Chain

When a command finishes, `finishCommand` invokes the callback, then calls `latches.Release(lock, cmdID)`. Release returns a list of command IDs that were blocked waiting for the released latches. For each woken ID, `tryToWakeUp` looks up the task context in its slot and re-attempts latch acquisition via `latches.Acquire`. If all latches are now held, the task is dispatched to the worker channel. This chain continues until no more commands can make progress, providing fair and deadlock-free command serialization.

---

## 7. 2PC Actions

**File:** `internal/storage/txn/actions.go`

This file implements the four core Percolator protocol operations as stateless functions that operate on an `MvccTxn` (write accumulator) and `MvccReader` (snapshot reader).

### Error Types

```go
var (
    ErrWriteConflict    = errors.New("txn: write conflict")
    ErrKeyIsLocked      = errors.New("txn: key is locked")
    ErrTxnLockNotFound  = errors.New("txn: lock not found")
    ErrAlreadyCommitted = errors.New("txn: already committed")
)
```

### Supporting Types

```go
type PrewriteProps struct {
    StartTS   txntypes.TimeStamp
    Primary   []byte
    LockTTL   uint64
    IsPrimary bool
}

type Mutation struct {
    Op    MutationOp    // MutationOpPut, MutationOpDelete, MutationOpLock
    Key   mvcc.Key
    Value []byte
}
```

### Prewrite

```go
func Prewrite(txn *MvccTxn, reader *MvccReader, props PrewriteProps, mutation Mutation) error
```

Phase 1 of 2PC for a single key:

1. **Lock check:** Load existing lock. If a lock exists with the same `StartTS`, treat as idempotent (return nil). If a lock exists with a different `StartTS`, return `ErrKeyIsLocked`.
2. **Write conflict check:** Loop through write records in descending `commitTS` order via repeated `SeekWrite(key, seekTS)` calls, skipping Rollback and Lock records to find the most recent data-changing write (matching TiKV's `check_for_newer_version` logic). If a data-changing write (Put or Delete) is found with `commitTS > StartTS`, return `ErrWriteConflict`. The loop is bounded to `SeekBound * 2` iterations. Debug logging is available via the `GOOKV_TRACE_PREWRITE=1` environment variable.
3. **Write lock:** Create a `Lock` with the appropriate `LockType` (derived from `MutationOp`). If the value is <= `ShortValueMaxLen` (255 bytes), inline it in the lock's `ShortValue` field. Call `txn.PutLock(key, lock)`.
4. **Write value:** If the mutation is a Put with a large value, call `txn.PutValue(key, startTS, value)` to store in CF_DEFAULT.

### Commit

```go
func Commit(txn *MvccTxn, reader *MvccReader, key Key, startTS, commitTS TimeStamp) error
```

Phase 2 of 2PC for a single key:

1. **Load and validate lock:** The lock must exist and have the matching `StartTS`. Otherwise return `ErrTxnLockNotFound`.
2. **MinCommitTS constraint:** `commitTS` must be >= `lock.MinCommitTS`.
3. **Pessimistic lock handling:** If the lock is `LockTypePessimistic`, simply unlock the key (the pessimistic lock was never prewrote) and return.
4. **Remove lock:** `txn.UnlockKey(key, false)`.
5. **Write commit record:** Convert lock type to write type, create a `Write` record carrying the `StartTS` and any `ShortValue`, and call `txn.PutWrite(key, commitTS, write)`.

### Rollback

```go
func Rollback(txn *MvccTxn, reader *MvccReader, key Key, startTS TimeStamp) error
```

1. **Check if committed:** `GetTxnCommitRecord(key, startTS)`. If a non-rollback write record exists, return `ErrAlreadyCommitted`. If a rollback record exists, return nil (idempotent).
2. **Remove lock:** If a lock exists with the matching `StartTS`, remove it. If the lock had a large value in CF_DEFAULT (no ShortValue and LockTypePut), delete it with `txn.DeleteValue(key, startTS)`.
3. **Write rollback record:** `PutWrite(key, startTS, rollbackWrite)` with `WriteTypeRollback`. Note: the rollback record's commit timestamp equals the start timestamp.

### CheckTxnStatus

```go
func CheckTxnStatus(reader *MvccReader, primaryKey Key, startTS TimeStamp) (*TxnStatus, error)
```

Returns a `TxnStatus` struct:

```go
type TxnStatus struct {
    IsLocked     bool
    Lock         *txntypes.Lock
    CommitTS     txntypes.TimeStamp
    IsRolledBack bool
}
```

1. **Check lock:** If a lock exists with the matching `StartTS`, return `{IsLocked: true, Lock: lock}`.
2. **Check write record:** `GetTxnCommitRecord(primaryKey, startTS)`. If it is a Rollback, return `{IsRolledBack: true}`. Otherwise return `{CommitTS: commitTS}`.
3. **Not found:** Return an empty `TxnStatus` (lock expired or transaction never existed).

### CheckTxnStatusWithCleanup

```go
func CheckTxnStatusWithCleanup(txn *MvccTxn, reader *MvccReader, primaryKey Key,
    startTS, callerStartTS TimeStamp, rollbackIfNotExist bool) (*TxnStatus, error)
```

A write-capable variant of `CheckTxnStatus`. Unlike the read-only version, this function takes an `MvccTxn` accumulator and may produce modifications (lock removals, rollback record writes). It handles two additional scenarios that the read-only version does not:

1. **TTL-based expired lock cleanup:** If the lock's TTL has expired, force-rollback the lock.
2. **RollbackIfNotExist:** If no lock and no commit/rollback record exists, write a protective rollback record to prevent late-arriving prewrites from succeeding.

**Algorithm:**

```mermaid
flowchart TD
    A[Load lock for primaryKey] -->|Lock exists with matching startTS| B{TTL expired?}
    B -->|"Yes: (startTS >> 18) + TTL <= (callerStartTS >> 18)"| C["Unlock key + delete CF_DEFAULT value<br/>(if put prewrite without short_value)<br/>+ write rollback record"]
    C --> D["Return {IsRolledBack: true}"]
    B -->|No: lock still alive| E["Return {IsLocked: true, Lock}"]
    A -->|No lock for this startTS| F{Commit/rollback record?}
    F -->|Committed| G["Return {CommitTS}"]
    F -->|Rolled back| H["Return {IsRolledBack: true}"]
    F -->|Not found| I{rollbackIfNotExist?}
    I -->|Yes| J["Write protective rollback record"]
    J --> K["Return {IsRolledBack: true}"]
    I -->|No| L["Return empty TxnStatus"]
```

The TTL check uses the HLC (Hybrid Logical Clock) physical time component. gookv timestamps encode `(physical_ms << 18) | logical`, so extracting the physical time is `ts >> 18`. A lock is considered expired when:

```
lockPhysicalTime + lock.TTL <= callerPhysicalTime
```

where `lockPhysicalTime = startTS >> 18` and `callerPhysicalTime = callerStartTS >> 18`.

When rolling back an expired lock, the function distinguishes between pessimistic and optimistic locks via `lock.LockType == LockTypePessimistic` and passes this to `UnlockKey`.

### CleanupModifies

**File:** `internal/server/storage.go`

```go
func (s *Storage) CleanupModifies(key []byte, startTS txntypes.TimeStamp) (txntypes.TimeStamp, []mvcc.Modify, error, *LatchGuard)
```

`CleanupModifies` resolves a lock on a (typically secondary) key by checking the primary key's transaction status via `CheckTxnStatus`. It returns modifications instead of applying them directly, so the caller can propose them via Raft in cluster mode.

**Algorithm:**

1. If no lock exists for `startTS` on the key, calls `CheckTxnStatus` and returns the commit timestamp (if any).
2. If a lock exists, reads the `Primary` field from the lock and calls `CheckTxnStatus` on the primary key.
3. If the primary status check **fails** (error), the function writes a **rollback record** on the key and removes the lock. This prevents a late-arriving commit from succeeding after the lock was cleaned up. Without this rollback record, a race condition could allow a slow commit to land after cleanup, violating transaction atomicity.
4. If the primary is committed (`CommitTS != 0`), calls `ResolveLock` to commit the secondary.
5. If the primary is rolled back, not found, or still locked, removes the lock and writes a rollback record.

### Helper Functions

- `mutationOpToLockType(op)`: Maps `MutationOpPut -> LockTypePut`, `MutationOpDelete -> LockTypeDelete`, `MutationOpLock -> LockTypeLock`.
- `lockTypeToWriteType(lt)`: Maps `LockTypePut -> WriteTypePut`, `LockTypeDelete -> WriteTypeDelete`, `LockTypeLock -> WriteTypeLock`.

---

## 8. Async Commit and 1PC

**File:** `internal/storage/txn/async_commit.go`

### Async Commit

Async commit reduces commit latency by making the transaction logically committed as soon as the primary key's lock is persisted. The primary lock stores all secondary keys; readers can determine commit status by inspecting the primary lock.

**gRPC Integration**: The `KvPrewrite` handler inspects `req.UseAsyncCommit` and routes to `PrewriteAsyncCommit` / `PrewriteAsyncCommitModifies`. When a PD client is configured, the server uses PD-allocated timestamps (via `pdClient.GetTS()`) for `MaxCommitTS`, ensuring linearizability. `KvCheckSecondaryLocks` is fully implemented: it inspects locks on secondary keys and returns lock status and commit timestamps for async commit resolution.

#### AsyncCommitPrewriteProps

```go
type AsyncCommitPrewriteProps struct {
    PrewriteProps
    UseAsyncCommit bool
    Secondaries    [][]byte
    MaxCommitTS    txntypes.TimeStamp
}
```

#### PrewriteAsyncCommit

```go
func PrewriteAsyncCommit(txn, reader, props, mutation) error
```

Follows the same conflict-check logic as regular `Prewrite` -- looping through up to `SeekBound*2` (64) write records to skip past Rollback and Lock records before concluding there is no write conflict -- then adds async-commit-specific fields to the lock:

- `lock.UseAsyncCommit = true`
- `lock.Secondaries = props.Secondaries` (only on the primary key)
- `lock.MinCommitTS = max(startTS + 1, MaxCommitTS + 1)` -- ensures the commit timestamp respects the concurrency manager's observed max_ts for linearizability.

#### CheckAsyncCommitStatus

```go
func CheckAsyncCommitStatus(reader, primaryKey, startTS) (TimeStamp, error)
```

1. If the primary lock is still present with `UseAsyncCommit = true`, returns `lock.MinCommitTS` as the candidate commit timestamp (transaction still in progress).
2. If the primary lock is gone, checks the commit record via `GetTxnCommitRecord`. Returns the `commitTS` if committed, or 0 if rolled back / not found.

### 1PC Optimization

For small, single-region transactions, 1PC skips CF_LOCK entirely and writes commit records directly to CF_WRITE in one batch.

**gRPC Integration**: The `KvPrewrite` handler checks 1PC eligibility (`Is1PCEligible`) and routes to `Prewrite1PC` / `Prewrite1PCModifies`. The server uses PD-allocated timestamps for the `CommitTS` when a PD client is configured.

#### OnePCProps

```go
type OnePCProps struct {
    StartTS  txntypes.TimeStamp
    CommitTS txntypes.TimeStamp
    Primary  []byte
    LockTTL  uint64
}
```

#### PrewriteAndCommit1PC

```go
func PrewriteAndCommit1PC(txn, reader, props, mutations) []error
```

Two-pass algorithm:

1. **Conflict check pass:** For every mutation, check for existing locks and write conflicts (same logic as Prewrite). If any error is found, return all errors without writing anything.
2. **Write pass:** For each mutation, build a `Write` record directly (no lock). Short values are inlined; large values go to CF_DEFAULT. Calls `txn.PutWrite(key, commitTS, write)`.

#### Eligibility Checks

```go
func Is1PCEligible(mutations []Mutation, maxSize int) bool
```

- Mutation count must be <= `maxSize` (default 64).
- Total key+value size must be < 256 KB.
- Empty mutation sets are ineligible.

```go
func IsAsyncCommitEligible(mutations []Mutation, maxKeys int) bool
```

- Mutation count must be <= `maxKeys` (default 256).
- Empty mutation sets are ineligible.

---

## 9. Pessimistic Locking

**File:** `internal/storage/txn/pessimistic.go`

Pessimistic locking allows clients to acquire locks before prewrite, preventing write conflicts during interactive transactions. The flow is: `AcquirePessimisticLock` -> `PrewritePessimistic` (upgrade) -> `Commit`.

Key semantics:
- Pessimistic locks use `LockTypePessimistic` (`'S'`).
- Pessimistic locks are invisible to readers (readers skip them).
- Write conflicts are checked against `ForUpdateTS` (not `StartTS`).
- Pessimistic rollback removes only pessimistic locks; no rollback record is written.

### AcquirePessimisticLock

```go
func AcquirePessimisticLock(txn, reader, props PessimisticLockProps, key) error
```

1. **Lock check:** If our own pessimistic lock exists, update `ForUpdateTS` if the new value is larger (idempotent). If our own normal lock exists, return nil. If another transaction's lock exists, return `ErrKeyIsLocked`.
2. **Write conflict check:** `SeekWrite(key, TSMax)`. Conflicts are tested against `ForUpdateTS` (not `StartTS`): if `commitTS > ForUpdateTS` and the write is data-changing, return `ErrWriteConflict`.
3. **Write lock:** Create a `Lock` with `LockTypePessimistic`, `ForUpdateTS`, and the usual primary/TTL fields. No value is written.

### PrewritePessimistic

```go
func PrewritePessimistic(txn, reader, props PessimisticPrewriteProps, mutation) error
```

Upgrades a pessimistic lock to a normal lock during the prewrite phase:

1. If a lock exists with matching `StartTS` and `LockTypePessimistic`, remove the old pessimistic lock (`UnlockKey` with `isPessimistic=true`) and write a new normal lock.
2. If a lock exists with matching `StartTS` but is already a normal lock, return nil (idempotent).
3. If `props.IsPessimistic` is true but no lock is found, return `ErrTxnLockNotFound` (lock expired and was cleaned up).
4. If not pessimistic and no lock exists, perform a standard write-conflict check before writing the lock.

### PessimisticRollback

```go
func PessimisticRollback(txn, reader, keys, startTS, forUpdateTS) []error
```

Removes pessimistic locks only (no rollback record). For each key:
- Skip if no lock, wrong `StartTS`, or not `LockTypePessimistic`.
- Skip if `forUpdateTS != 0` and `lock.ForUpdateTS != forUpdateTS`.
- Call `txn.UnlockKey(key, true)`.

---

## 10. Latches

**File:** `internal/storage/txn/latch/latch.go`

The latch subsystem provides deadlock-free key serialization. Commands that touch overlapping keys are serialized through latches to prevent concurrent modification.

### Latches Struct

```go
type Latches struct {
    slots []latchSlot
    size  int          // Always a power of 2
}
```

The slot count is rounded up to the nearest power of 2. Each slot contains:

```go
type latchSlot struct {
    mu        sync.Mutex
    owner     uint64       // Command ID (0 = free)
    waitQueue []uint64     // Waiting command IDs
    wakeCh    chan uint64   // Buffered channel (cap 64) for signaling
}
```

### Lock Struct

```go
type Lock struct {
    RequiredHashes []uint64   // Sorted, deduplicated slot indices
    OwnedCount     int        // Number of latches acquired so far
}
```

### GenLock

```go
func (l *Latches) GenLock(keys [][]byte) *Lock
```

1. Hash each key with FNV-1a (`hash/fnv.New64a()`).
2. Map each hash to a slot index via `h & (size - 1)`.
3. Deduplicate slot indices (using a `map[uint64]bool`).
4. Sort the indices in ascending order.

Sorted acquisition is the deadlock-prevention mechanism: all commands acquire latches in the same global order, so circular waits cannot form.

### Acquire

```go
func (l *Latches) Acquire(lock *Lock, commandID uint64) bool
```

Iterates from `lock.OwnedCount` through `RequiredHashes`. For each slot:
- If the slot is free (`owner == 0`) or already owned by this command, take ownership and increment `OwnedCount`.
- If owned by another command, append `commandID` to the slot's `waitQueue` and return `false`.

Returns `true` only if all required latches are acquired.

### Release

```go
func (l *Latches) Release(lock *Lock, commandID uint64) []uint64
```

Releases all owned latches. For each slot owned by this command:
- Set `owner = 0`.
- If the wait queue is non-empty, dequeue the first waiter and add it to the returned wake-up list.

Resets `OwnedCount` to 0.

---

## 11. Concurrency Manager

**File:** `internal/storage/txn/concurrency/manager.go`

The concurrency manager provides two services: max_ts tracking (for async commit correctness) and an in-memory lock table (for quick lock lookups without hitting the storage engine).

### Manager Struct

```go
type Manager struct {
    maxTS     atomic.Uint64
    lockTable sync.Map       // key (string) -> *LockHandle
}
```

### Max Timestamp Tracking

- `UpdateMaxTS(ts)`: Atomically updates `maxTS` using a CAS loop. Only updates if `ts > current`. This ensures that async commit's `MinCommitTS` correctly accounts for all observed read timestamps.
- `MaxTS()`: Returns the current maximum observed timestamp.

### In-Memory Lock Table

- `LockKey(key, startTS)`: Stores a `LockHandle{Key, StartTS}` in the lock table. Returns a `KeyHandleGuard` whose `Release()` method deletes the entry.
- `IsKeyLocked(key)`: Checks if a key has an in-memory lock. Returns `(*LockHandle, bool)`.
- `GlobalMinLock()`: Iterates all entries via `lockTable.Range` and returns a pointer to the minimum `StartTS` across all locks. Returns `nil` if no locks exist. This is O(n).
- `LockCount()`: Returns the number of entries (O(n), intended for testing only).

### KeyHandleGuard

```go
type KeyHandleGuard struct {
    mgr *Manager
    key string
}
func (g *KeyHandleGuard) Release() { g.mgr.lockTable.Delete(g.key) }
```

RAII-style guard: the caller must call `Release()` when the lock is removed from the storage engine.

---

## 12. Diagrams

### 2PC Flow (Prewrite -> Commit)

```mermaid
sequenceDiagram
    participant Client
    participant TxnLayer as Txn Actions
    participant MvccTxn as MvccTxn (Accumulator)
    participant Reader as MvccReader
    participant Engine as Storage Engine

    Note over Client,Engine: Phase 1 -- Prewrite

    Client->>TxnLayer: Prewrite(txn, reader, props, mutation)
    TxnLayer->>Reader: LoadLock(key)
    Reader->>Engine: Get(CF_LOCK, encodedLockKey)
    Engine-->>Reader: nil (no existing lock)
    Reader-->>TxnLayer: nil

    TxnLayer->>Reader: SeekWrite(key, TSMax)
    Reader->>Engine: NewIterator(CF_WRITE) + Seek
    Engine-->>Reader: (no conflicting write)
    Reader-->>TxnLayer: nil

    TxnLayer->>MvccTxn: PutLock(key, lock)
    Note over MvccTxn: Appends Modify{Put, CF_LOCK, ...}

    alt Large value (> 255 bytes)
        TxnLayer->>MvccTxn: PutValue(key, startTS, value)
        Note over MvccTxn: Appends Modify{Put, CF_DEFAULT, ...}
    end

    TxnLayer-->>Client: nil (success)
    Client->>Engine: WriteBatch(txn.Modifies)

    Note over Client,Engine: Phase 2 -- Commit

    Client->>TxnLayer: Commit(txn, reader, key, startTS, commitTS)
    TxnLayer->>Reader: LoadLock(key)
    Reader->>Engine: Get(CF_LOCK, encodedLockKey)
    Engine-->>Reader: lock data
    Reader-->>TxnLayer: lock (startTS matches)

    TxnLayer->>MvccTxn: UnlockKey(key, false)
    Note over MvccTxn: Appends Modify{Delete, CF_LOCK, ...}

    TxnLayer->>MvccTxn: PutWrite(key, commitTS, write)
    Note over MvccTxn: Appends Modify{Put, CF_WRITE, ...}

    TxnLayer-->>Client: nil (success)
    Client->>Engine: WriteBatch(txn.Modifies)
```

### MVCC / Txn Layer Component Dependencies

```mermaid
graph TB
    subgraph "pkg/txntypes"
        TS[TimeStamp]
        LockT[Lock]
        WriteT[Write]
        CF[cfnames]
    end

    subgraph "internal/storage/mvcc"
        KeyEnc[key.go<br/>EncodeKey / DecodeKey<br/>EncodeLockKey / TruncateToUserKey]
        TxnAcc[txn.go<br/>MvccTxn<br/>Modify accumulator]
        MReader[reader.go<br/>MvccReader<br/>LoadLock / SeekWrite / GetWrite<br/>GetTxnCommitRecord / GetValue]
        PG[point_getter.go<br/>PointGetter<br/>SI / RC isolation]
        Scan[scanner.go<br/>Scanner<br/>Forward / Backward range scans]
    end

    subgraph "internal/storage/txn"
        Actions[actions.go<br/>Prewrite / Commit<br/>Rollback / CheckTxnStatus]
        Async[async_commit.go<br/>PrewriteAsyncCommit<br/>CheckAsyncCommitStatus<br/>PrewriteAndCommit1PC]
        Pessim[pessimistic.go<br/>AcquirePessimisticLock<br/>PrewritePessimistic<br/>PessimisticRollback]
    end

    subgraph "internal/storage/txn/latch"
        Latch[latch.go<br/>Latches<br/>GenLock / Acquire / Release]
    end

    subgraph "internal/storage/txn/concurrency"
        ConcMgr[manager.go<br/>Manager<br/>maxTS / lockTable]
    end

    subgraph "internal/storage/txn/scheduler"
        Sched[scheduler.go<br/>TxnScheduler<br/>RunCommand / RunCommandSync]
    end

    subgraph "internal/storage/gc"
        GCW[gc.go<br/>GCWorker / GC<br/>3-state version reclamation]
    end

    subgraph "internal/engine"
        Snap[traits.Snapshot]
    end

    %% Dependencies
    Actions --> TxnAcc
    Actions --> MReader
    Async --> TxnAcc
    Async --> MReader
    Pessim --> TxnAcc
    Pessim --> MReader

    PG --> MReader
    Scan --> Snap
    Scan --> KeyEnc
    MReader --> Snap
    MReader --> KeyEnc
    TxnAcc --> KeyEnc

    GCW --> TxnAcc
    GCW --> MReader
    GCW --> Snap

    Sched --> Latch
    Sched --> ConcMgr
    Sched --> Snap

    KeyEnc --> TS
    TxnAcc --> LockT
    TxnAcc --> WriteT
    TxnAcc --> CF
    MReader --> LockT
    MReader --> WriteT
    MReader --> CF
    Scan --> LockT
    Scan --> WriteT
    Scan --> CF

    Actions --> LockT
    Actions --> WriteT
    Async --> LockT
    Async --> WriteT
    Pessim --> LockT

    Latch -.->|serializes| Actions
    Sched -.->|dispatches| Actions
    ConcMgr -.->|maxTS for| Async
```

### Async Commit Flow

```mermaid
sequenceDiagram
    participant Client
    participant TxnLayer as Txn Actions
    participant ConcMgr as Concurrency Manager
    participant MvccTxn as MvccTxn
    participant Reader as MvccReader
    participant Engine as Storage Engine

    Note over Client,Engine: Async Commit Prewrite (Primary)

    Client->>ConcMgr: MaxTS()
    ConcMgr-->>Client: currentMaxTS

    Client->>TxnLayer: PrewriteAsyncCommit(txn, reader, props, mutation)
    Note over TxnLayer: props.UseAsyncCommit = true<br/>props.Secondaries = [sec1, sec2]<br/>props.MaxCommitTS = currentMaxTS

    TxnLayer->>Reader: LoadLock(key)
    Reader-->>TxnLayer: nil
    TxnLayer->>Reader: SeekWrite(key, TSMax)
    Reader-->>TxnLayer: nil (no conflict)

    Note over TxnLayer: Build lock with:<br/>UseAsyncCommit = true<br/>Secondaries = [sec1, sec2]<br/>MinCommitTS = max(startTS+1, maxTS+1)

    TxnLayer->>MvccTxn: PutLock(key, lock)
    TxnLayer-->>Client: nil (success)
    Client->>Engine: WriteBatch(txn.Modifies)

    Note over Client,Engine: Transaction is logically committed<br/>when primary lock is persisted

    Note over Client,Engine: Async Commit Resolution

    Client->>TxnLayer: CheckAsyncCommitStatus(reader, primaryKey, startTS)
    TxnLayer->>Reader: LoadLock(primaryKey)
    alt Primary lock still present
        Reader-->>TxnLayer: lock (UseAsyncCommit=true)
        TxnLayer-->>Client: lock.MinCommitTS (candidate)
    else Primary lock gone
        Reader-->>TxnLayer: nil
        TxnLayer->>Reader: GetTxnCommitRecord(primaryKey, startTS)
        Reader-->>TxnLayer: (write, commitTS)
        TxnLayer-->>Client: commitTS
    end
```
