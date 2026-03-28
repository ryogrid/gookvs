# Raftstore Layer — Raft-Based Replication in gookv

## 1. Overview

The raftstore layer implements region-based Raft consensus using the etcd/raft library (`go.etcd.io/etcd/raft/v3`). Each region replica is driven by a **Peer** goroutine that owns an etcd `raft.RawNode`, persists state through a **PeerStorage** adapter, and receives messages via a **Router** that maps region IDs to buffered Go channels (mailboxes).

Key design decisions:

- **One goroutine per region replica** — each `Peer.Run()` loop processes ticks, incoming Raft messages, client proposals, and apply results sequentially, eliminating the need for fine-grained locking within a single peer.
- **etcd/raft as a library** — gookv uses `raft.RawNode` directly (not a higher-level server), giving full control over when to call `Tick()`, `Step()`, `Propose()`, `Ready()`, and `Advance()`.
- **Protobuf wire-format conversion** — the `convert.go` file bridges kvproto's `eraftpb.Message` (used on the gRPC transport) and etcd's `raftpb.Message` via marshal/unmarshal, since both share the same protobuf wire format.

Source files:

| File | Purpose |
|------|---------|
| `internal/raftstore/peer.go` | Peer struct, lifecycle, event loop, Ready processing, proposal tracking |
| `internal/raftstore/storage.go` | PeerStorage (raft.Storage impl), persistence, recovery, `BuildWriteTask`/`ApplyWriteTaskPostPersist` |
| `internal/raftstore/msg.go` | Message/tick/result type definitions, `proposalEntry`, `errorResponse()`, `IsCompactLog()` |
| `internal/raftstore/raft_log_writer.go` | `RaftLogWriter`: batches `WriteTask`s from multiple peers into single WriteBatch + fsync |
| `internal/raftstore/apply_worker.go` | `ApplyWorkerPool`: processes `ApplyTask`s in background goroutine pool |
| `internal/raftstore/router/router.go` | sync.Map-based message routing |
| `internal/raftstore/convert.go` | eraftpb <-> raftpb protobuf conversion |
| `internal/raftstore/snapshot.go` | Snapshot generation, serialization, transfer, and application; `SnapWorker` background processing |
| `internal/raftstore/split/checker.go` | Region split checking (`SplitCheckWorker`) and execution (`ExecBatchSplit`) |
| `internal/raftstore/split_admin.go` | Split as Raft admin command: `SplitAdminRequest`, `ExecSplitAdmin`, `ProposeSplit` |
| `internal/raftstore/raftlog_gc.go` | Raft log compaction: `execCompactLog`, `RaftLogGCWorker` background deletion |
| `internal/raftstore/conf_change.go` | Conf change processing (`applyConfChangeEntry`, `processConfChange`, `ProposeConfChange`) |
| `internal/raftstore/merge.go` | Region merge: `ExecPrepareMerge`, `ExecCommitMerge`, `ExecRollbackMerge` |
| `internal/raftstore/store_worker.go` | Region data cleanup (`CleanupRegionData`) |

---

## 2. Key Types and Interfaces

### 2.1 Peer

```go
// internal/raftstore/peer.go
type Peer struct {
    regionID         uint64
    peerID           uint64
    storeID          uint64
    region           *metapb.Region
    regionMu         sync.RWMutex                   // protects region field (concurrent gRPC access)

    rawNode          *raft.RawNode
    storage          *PeerStorage
    engine           traits.KvEngine

    cfg              PeerConfig

    Mailbox          chan PeerMsg                    // buffered channel for incoming messages
    sendFunc         func([]raftpb.Message)          // outbound Raft message delivery
    applyFunc        func(regionID uint64, entries []raftpb.Entry)  // committed entry application

    nextProposalID   uint64                         // per-peer monotonic counter for proposal IDs
    currentTerm      uint64                         // cached Raft term, updated from HardState in handleReady
    pendingProposals map[uint64]proposalEntry        // proposalID -> entry (callback, term, timestamp)

    raftLogWriter    *RaftLogWriter                  // batches Raft log persistence across regions (nil = legacy)

    applyWorkerPool  *ApplyWorkerPool                // processes committed entries asynchronously (nil = legacy)
    applyInFlight    bool                            // true when an async apply task is outstanding
    pendingApplyTasks []*ApplyTask                   // buffered apply tasks when apply is in-flight

    raftLogSizeHint  uint64                         // estimated Raft log size for GC decisions
    lastCompactedIdx uint64                         // last index sent to RaftLogGCWorker
    logGCWorkerCh    chan<- RaftLogGCTask            // sends GC tasks to RaftLogGCWorker
    pdTaskCh         chan<- interface{}              // sends RegionHeartbeatInfo to PDWorker
    splitCheckCh     chan<- split.SplitCheckTask     // sends split check tasks to SplitCheckWorker

    pendingReads     map[string]*pendingRead         // ReadIndex requests awaiting apply
    nextReadID       atomic.Uint64                   // unique request contexts for ReadIndex
    leaseExpiryNanos atomic.Int64                    // Unix nanoseconds; accessed from multiple goroutines
    leaseValid       atomic.Bool                     // leader lease validity flag
    splitResultCh    chan<- *SplitRegionResult        // split results to coordinator (leader only)

    stopped          atomic.Bool
    isLeader         atomic.Bool
    initialized      bool
}
```

`Peer` is the central type. It owns the Raft state machine (`rawNode`), persistent storage (`storage`), and the mailbox channel. Two injectable function fields -- `sendFunc` and `applyFunc` -- decouple the peer from transport and state-machine concerns. Proposal tracking uses monotonic `nextProposalID` with `pendingProposals` mapping to `proposalEntry` (callback, term, timestamp). The optional `raftLogWriter` enables I/O coalescing across regions, and the optional `applyWorkerPool` enables async entry application. The `raftLogSizeHint` and `lastCompactedIdx` fields track Raft log growth for compaction decisions. The `logGCWorkerCh` and `pdTaskCh` channels connect the peer to background workers for log GC and PD heartbeat reporting, respectively.

### 2.2 PeerConfig

```go
type PeerConfig struct {
    RaftBaseTickInterval     time.Duration  // default: 100ms
    RaftElectionTimeoutTicks int            // default: 10
    RaftHeartbeatTicks       int            // default: 2
    MaxInflightMsgs          int            // default: 256
    MaxSizePerMsg            uint64         // default: 1 MiB
    PreVote                  bool           // default: true
    MailboxCapacity          int            // default: 256
    RaftLogGCTickInterval    time.Duration  // default: 10s
    RaftLogGCCountLimit      uint64         // default: 72000
    RaftLogGCSizeLimit       uint64         // default: 72 MiB
    RaftLogGCThreshold       uint64         // default: 50
    SplitCheckTickInterval   time.Duration  // default: 10s (0 = disabled)
    PdHeartbeatTickInterval  time.Duration  // default: 60s (0 = disabled)
}
```

### 2.3 PeerStorage

```go
// internal/raftstore/storage.go
type PeerStorage struct {
    mu                 sync.RWMutex
    regionID           uint64
    engine             traits.KvEngine

    hardState          raftpb.HardState
    applyState         ApplyState
    entries            []raftpb.Entry   // in-memory cache (last 1024 entries)
    persistedLastIndex uint64
}
```

`PeerStorage` implements the `raft.Storage` interface. It caches recent entries in memory and falls back to the KvEngine (CF_RAFT column family) for older entries.

### 2.4 ApplyState

```go
type ApplyState struct {
    AppliedIndex   uint64
    TruncatedIndex uint64
    TruncatedTerm  uint64
}
```

Tracks which Raft log entries have been applied to the state machine and where log truncation stands.

### 2.5 Router

```go
// internal/raftstore/router/router.go
type Router struct {
    peers   sync.Map                    // regionID -> chan raftstore.PeerMsg
    storeCh chan raftstore.StoreMsg      // store-level message channel
}
```

### 2.6 Class Diagram

```mermaid
classDiagram
    class Peer {
        -regionID uint64
        -peerID uint64
        -storeID uint64
        -region *metapb.Region
        -rawNode *raft.RawNode
        -storage *PeerStorage
        -engine traits.KvEngine
        -cfg PeerConfig
        +Mailbox chan PeerMsg
        -sendFunc func([]raftpb.Message)
        -applyFunc func(uint64, []raftpb.Entry)
        -nextProposalID uint64
        -currentTerm uint64
        -pendingProposals map[uint64]proposalEntry
        -raftLogWriter *RaftLogWriter
        -applyWorkerPool *ApplyWorkerPool
        -applyInFlight bool
        -pendingApplyTasks []*ApplyTask
        -raftLogSizeHint uint64
        -lastCompactedIdx uint64
        -logGCWorkerCh chan RaftLogGCTask
        -pdTaskCh chan interface
        -stopped atomic.Bool
        -isLeader atomic.Bool
        -initialized bool
        +Run(ctx context.Context)
        +Propose(data []byte) error
        +Campaign() error
        +Status() raft.Status
        +SetPDTaskCh(ch chan interface)
        +SetLogGCWorkerCh(ch chan RaftLogGCTask)
        +SetRaftLogWriter(w *RaftLogWriter)
        +SetApplyWorkerPool(p *ApplyWorkerPool)
        -handleMessage(msg PeerMsg)
        -propose(cmd *RaftCommand)
        -handleReady()
        -failAllPendingProposals(err error)
        -sweepStaleProposals(maxAge time.Duration)
        -applyInline(entries []raftpb.Entry)
        -submitToApplyWorker(entries []raftpb.Entry)
        -onApplyResult(result *ApplyResult)
        -onRaftLogGCTick()
        -onReadyCompactLog(compactIdx, compactTerm uint64)
        -scheduleRaftLogGC(compactIdx uint64)
        -sendRegionHeartbeatToPD()
        -onSplitCheckTick()
        +SetSplitCheckCh(ch chan SplitCheckTask)
        +UpdateRegion(region *metapb.Region)
    }

    class PeerStorage {
        -mu sync.RWMutex
        -regionID uint64
        -engine traits.KvEngine
        -hardState raftpb.HardState
        -applyState ApplyState
        -entries []raftpb.Entry
        -persistedLastIndex uint64
        +InitialState() (HardState, ConfState, error)
        +Entries(lo, hi, maxSize uint64) ([]Entry, error)
        +Term(i uint64) (uint64, error)
        +LastIndex() (uint64, error)
        +FirstIndex() (uint64, error)
        +Snapshot() (Snapshot, error)
        +SaveReady(rd raft.Ready) error
        +BuildWriteTask(rd raft.Ready) (*WriteTask, error)
        +ApplyWriteTaskPostPersist(task *WriteTask)
        +RecoverFromEngine() error
        +PersistApplyState() error
        -appendToCache(entries []Entry)
        -readEntriesFromEngine(lo, hi uint64) ([]Entry, error)
    }

    class Router {
        -peers sync.Map
        -storeCh chan StoreMsg
        +Register(regionID uint64, ch chan PeerMsg) error
        +Unregister(regionID uint64)
        +Send(regionID uint64, msg PeerMsg) error
        +SendStore(msg StoreMsg) error
        +Broadcast(msg PeerMsg)
        +HasRegion(regionID uint64) bool
        +RegionCount() int
        +GetMailbox(regionID uint64) chan PeerMsg
    }

    class PeerMsg {
        +Type PeerMsgType
        +Data interface
    }

    class RaftCommand {
        +Request *raft_cmdpb.RaftCmdRequest
        +Callback func(*RaftCmdResponse)
    }

    Peer --> PeerStorage : storage
    Peer --> PeerMsg : receives via Mailbox
    Router --> PeerMsg : delivers via Send()
    PeerMsg --> RaftCommand : Data (when Type=RaftCommand)
    PeerStorage ..|> raft.Storage : implements
```

---

## 3. Peer Lifecycle

### 3.1 Initialization (`NewPeer`)

`NewPeer` accepts a region ID, peer ID, store ID, the region metadata, a KvEngine, config, and an optional list of initial Raft peers (for bootstrap).

**Bootstrap path** (when `len(peers) > 0`):
1. Create a `PeerStorage` with default `ApplyState` (indices at 0).
2. Call `SetDummyEntry()` to insert a sentinel entry at index 0, term 0 — matching etcd/raft's `MemoryStorage` convention.
3. Create a `raft.RawNode` with the storage.
4. Call `rawNode.Bootstrap(peers)` to initialize the Raft cluster membership.

**Restart path** (when `len(peers) == 0`):
1. Create a `PeerStorage`.
2. Call `RecoverFromEngine()` to restore hard state and log entries from `CF_RAFT`.
3. Create a `raft.RawNode` — it reads the recovered state via the `raft.Storage` interface.

The `raft.Config` is created with:
- `CheckQuorum: true` — leader steps down if it does not hear from a quorum.
- `PreVote: true` (by default) — prevents disruptive elections from partitioned nodes.

### 3.2 Event Loop (`Peer.Run`)

```go
func (p *Peer) Run(ctx context.Context) {
    ticker := time.NewTicker(p.cfg.RaftBaseTickInterval)  // 100ms default
    // Optional periodic PD region heartbeat.
    var pdHeartbeatTickerCh <-chan time.Time
    if p.cfg.PdHeartbeatTickInterval > 0 {
        pdHeartbeatTicker := time.NewTicker(p.cfg.PdHeartbeatTickInterval)
        defer pdHeartbeatTicker.Stop()
        pdHeartbeatTickerCh = pdHeartbeatTicker.C
    }
    for {
        select {
        case <-ctx.Done():           // shutdown
            p.stopped.Store(true)
            return
        case <-ticker.C:             // Raft tick
            p.rawNode.Tick()
        case <-gcTickerCh:           // log GC check (10s)
            p.onRaftLogGCTick()
        case <-splitCheckTickerCh:   // split check (10s)
            p.onSplitCheckTick()
        case <-pdHeartbeatTickerCh:  // periodic PD region heartbeat
            if p.isLeader.Load() && p.pdTaskCh != nil {
                p.sendRegionHeartbeatToPD()
            }
        case msg, ok := <-p.Mailbox: // incoming message
            if !ok { p.stopped.Store(true); return }
            p.handleMessage(msg)
            // Drain up to 63 more queued messages before handleReady
            for i := 0; i < 63; i++ { ... }
        }
        if p.stopped.Load() { return }  // exit if PeerMsgTypeDestroy set stopped
        p.handleReady()  // process Raft Ready after every event
    }
}
```

The loop processes Raft ticks, log GC ticks, split check ticks, incoming messages, and an optional periodic PD heartbeat. Every event is followed by a `handleReady()` call to drain any pending Raft state changes. The `stopped` flag is checked after each event to handle `PeerMsgTypeDestroy` without closing the mailbox. Incoming messages are batch-drained (up to 64 per cycle) to reduce `handleReady()` overhead. The `pdHeartbeatTickerCh` case sends periodic region heartbeats to PD when `PdHeartbeatTickInterval > 0` (default 60s).

### 3.3 Message Handling (`handleMessage`)

| PeerMsgType | Action |
|-------------|--------|
| `RaftMessage` | Unwrap `*raftpb.Message`, call `rawNode.Step()` |
| `RaftCommand` | Unwrap `*RaftCommand`, call `propose()` |
| `Tick` | Call `rawNode.Tick()` (additional tick beyond the timer) |
| `ApplyResult` | Unwrap `*ApplyResult`, call `onApplyResult()` — updates applied index (monotonicity guard), persists `ApplyState`, sweeps pending reads, clears `applyInFlight` flag, submits next queued task, processes `ExecResultTypeCompactLog` via `onReadyCompactLog()` |
| `Significant` | Unwrap `*SignificantMsg`, call `handleSignificantMessage()` — dispatches `Unreachable` (→ `rawNode.ReportUnreachable`), `SnapshotStatus` (→ `rawNode.ReportSnapshot`), and `MergeResult` (→ `stopped = true`) |
| `Schedule` | Unwrap `*ScheduleMsg`, call `handleScheduleMessage()` — only executes if this peer is the leader; routes `TransferLeader`, `ChangePeer`, `Merge` |
| `Destroy` | Set `stopped = true` (mailbox is NOT closed to avoid panics from concurrent senders; the `Run()` loop exits via the `stopped` flag check after each event) |
| Others | Silently ignored |

### 3.4 Ready Processing (`handleReady`)

This is the core Raft integration point. It runs after every event in the loop:

1. **Guard**: `rawNode.HasReady()` -- return early if nothing to do.
2. **Get Ready**: `rd := rawNode.Ready()` -- a batch of state changes.
3. **Update leader**: If `rd.SoftState` is non-nil, update `isLeader` based on whether `SoftState.Lead == peerID`. On becoming leader: call `sendRegionHeartbeatToPD()` and extend leader lease. On stepping down: invalidate lease, call `failAllPendingProposals()`, and cancel all pending reads.
3b. **Cache term**: If `rd.HardState` is non-empty, cache `currentTerm = rd.HardState.Term` for use in `propose()`.
4. **Persist** (conditional path):
   - If `raftLogWriter != nil`: call `storage.BuildWriteTask(rd)`, submit to `RaftLogWriter`, wait on `task.Done`, then call `storage.ApplyWriteTaskPostPersist(task)`.
   - Else (legacy): call `storage.SaveReady(rd)` -- write hard state and new entries to `CF_RAFT` via a `WriteBatch`.
4b. **Apply snapshot**: If `rd.Snapshot` is non-empty, call `storage.ApplySnapshot(rd.Snapshot)` to apply the received snapshot data (passes region start/end keys to `ApplySnapshotData`).
5. **Send messages**: If `sendFunc` is set and `rd.Messages` is non-empty, deliver outbound Raft messages.
6. **Apply committed entries**:
   - Process admin entries inline first: `ConfChange`/`ConfChangeV2` via `applyConfChangeEntry()`, `SplitAdmin` via `applySplitAdminEntry()`. (CompactLog entries are filtered out in the apply path but not processed inline.)
   - Conditional apply path:
     - If `applyWorkerPool != nil`: call `submitToApplyWorker()` -- filters to data entries only (`EntryNormal && !IsSplitAdmin && !IsCompactLog && len>8`), extracts callbacks from `pendingProposals`, submits `ApplyTask`.
     - Else (legacy): call `applyInline()` -- filters data entries, calls `applyFunc`, matches callbacks by extracting proposalID from first 8 bytes of entry data (with term check), updates applied index, persists `ApplyState`.
7. **Advance**: `rawNode.Advance(rd)` -- signal to etcd/raft that the Ready has been processed.

---

## 4. PeerStorage

### 4.1 raft.Storage Interface Implementation

`PeerStorage` implements all six methods of `raft.Storage`:

| Method | Behavior |
|--------|----------|
| `InitialState()` | Returns the in-memory `hardState` and a `ConfState` derived from region peers (if `region` is set, each peer ID is added to `cs.Voters`; otherwise empty) |
| `Entries(lo, hi, maxSize)` | Serves from cache if range is covered; falls back to per-index engine reads; applies `limitSize` byte cap |
| `Term(i)` | Returns truncated term for `TruncatedIndex`; otherwise checks cache, then engine |
| `LastIndex()` | Returns last cache entry index, or `persistedLastIndex` if cache is empty |
| `FirstIndex()` | Returns `TruncatedIndex + 1` |
| `Snapshot()` | Returns an empty snapshot with metadata set to `TruncatedIndex`/`TruncatedTerm` |

### 4.2 Persistence — `SaveReady()` (Legacy) and `BuildWriteTask()`/`ApplyWriteTaskPostPersist()` (Batch)

**Legacy path** (`SaveReady`): Persists a `raft.Ready` batch atomically:

1. Create a `WriteBatch` from the engine.
2. If hard state is non-empty, marshal and write to `keys.RaftStateKey(regionID)` in `CF_RAFT`.
3. For each new entry, marshal and write to `keys.RaftLogKey(regionID, index)` in `CF_RAFT`.
4. Commit the `WriteBatch` atomically.
5. Update the in-memory entry cache (`appendToCache`) and `persistedLastIndex`.

**Batch path** (when `raftLogWriter` is configured): Persistence is split into two phases:

1. **`BuildWriteTask(rd)`** serializes entries and hard state into `WriteOp` structs (CF, Key, Value) without writing to the engine. Returns a `WriteTask` containing `Ops`, `Entries` (for cache update), `HardState` (for in-memory update), and a `Done` channel.
2. The peer submits the `WriteTask` to the `RaftLogWriter` via `Submit()` and waits on `task.Done`.
3. **`ApplyWriteTaskPostPersist(task)`** updates the in-memory hard state, entry cache, and `persistedLastIndex` from the task's stored fields.

### 4.3 Recovery — `RecoverFromEngine()`

Called on restart (non-bootstrap path):

1. Read hard state from `keys.RaftStateKey(regionID)` in `CF_RAFT`; unmarshal into `hardState`.
2. Scan all entries in `keys.RaftLogKeyRange(regionID)` using an iterator over `CF_RAFT`.
3. Unmarshal each entry, track the maximum index as `persistedLastIndex`.
4. Keep the last 1024 entries in the in-memory cache.
5. Read `ApplyState` from `keys.ApplyStateKey(regionID)` in `CF_RAFT` (24-byte big-endian encoding: 8 bytes each for `AppliedIndex`, `TruncatedIndex`, `TruncatedTerm`). If not found, the default `ApplyState` from `NewPeerStorage` is retained.

### 4.3.1 `HasPersistedRaftState(engine, regionID)`

A package-level utility function that checks whether the engine has persisted Raft state for a given region by looking up `keys.RaftStateKey(regionID)` in `CF_RAFT`. Returns `true` if the key exists.

Used by `StoreCoordinator.CreatePeer` to decide whether a newly created peer should be bootstrapped with the region's full peer list (no persisted state → bootstrap with peers) or should recover from the engine (persisted state exists → `RecoverFromEngine`).

### 4.4 Entry Cache

The cache is a `[]raftpb.Entry` slice holding the most recent entries (up to 1024). The `appendToCache` method handles overlap:

- If new entries start at or before the cache's first index, the cache is replaced entirely.
- Otherwise, the cache is truncated at the overlap point and new entries are appended.
- If the total exceeds 1024 entries, the oldest are discarded.

This avoids engine reads for the common case where Raft needs recent log entries.

### 4.5 Engine Fallback — `readEntriesFromEngine()`

For entries outside the cache, reads are done one index at a time via `engine.Get(CF_RAFT, RaftLogKey(regionID, idx))`. If the first requested entry is not found, reading stops (compacted). If a subsequent entry is missing (gap in the log), an error is returned instead of silently stopping -- this ensures gap detection for consistency.

### 4.6 Initialization Constants

```go
const (
    RaftInitLogTerm  uint64 = 5
    RaftInitLogIndex uint64 = 5
)
```

These match TiKV conventions: a newly created `PeerStorage` (non-bootstrap) starts with `AppliedIndex = 5`, `TruncatedIndex = 5`, `TruncatedTerm = 5`.

---

## 5. Router

The `Router` type provides `sync.Map`-based routing from region IDs to peer mailboxes.

### 5.1 Registration

```go
func (r *Router) Register(regionID uint64, ch chan PeerMsg) error
```

Uses `sync.Map.LoadOrStore` — returns `ErrPeerAlreadyRegistered` if the region already exists. The channel passed in is typically the `Peer.Mailbox` created during `NewPeer`.

### 5.2 Message Delivery

```go
func (r *Router) Send(regionID uint64, msg PeerMsg) error
```

Looks up the mailbox channel via `sync.Map.Load`, then performs a **non-blocking send** (`select` with `default`). Returns `ErrRegionNotFound` or `ErrMailboxFull` on failure. This prevents a slow peer from blocking the caller.

### 5.3 Broadcast

```go
func (r *Router) Broadcast(msg PeerMsg)
```

Iterates all registered peers via `sync.Map.Range`, attempting a non-blocking send to each. Messages to full mailboxes are silently dropped — broadcast is best-effort.

### 5.4 Store-Level Messages

The router also carries a `storeCh chan StoreMsg` for store-level messages (peer creation, destruction, etc.). `SendStore` uses the same non-blocking send pattern.

### 5.5 Mailbox Design

Mailboxes are buffered Go channels with a default capacity of 256 (configurable via `PeerConfig.MailboxCapacity` or `router.DefaultMailboxCapacity`). The non-blocking send pattern means back-pressure is handled by dropping rather than blocking, which prevents cascading slowdowns across regions.

---

## 6. Message Types

### 6.1 PeerMsgType (messages to peer goroutines)

| Constant | Value | Description |
|----------|-------|-------------|
| `PeerMsgTypeRaftMessage` | 0 | Raft protocol message from another peer |
| `PeerMsgTypeRaftCommand` | 1 | Client read/write request to be proposed |
| `PeerMsgTypeTick` | 2 | Timer tick (supplemental to the built-in ticker) |
| `PeerMsgTypeApplyResult` | 3 | Results from the apply worker |
| `PeerMsgTypeSignificant` | 4 | High-priority control messages |
| `PeerMsgTypeStart` | 5 | Peer initialization signal |
| `PeerMsgTypeDestroy` | 6 | Peer destruction request |
| `PeerMsgTypeCasual` | 7 | Low-priority, droppable messages |
| `PeerMsgTypeSchedule` | 8 | Scheduling commands from PD (leader transfer, peer change, merge) |
| `PeerMsgTypeReadIndex` | 9 | Linearizable read index request from coordinator |
| `PeerMsgTypeCancelRead` | 10 | Cancel a timed-out pending read |

### 6.2 PeerTickType (tick classifications)

| Constant | Value | Description |
|----------|-------|-------------|
| `PeerTickRaft` | 0 | Drives heartbeats and election timeout |
| `PeerTickRaftLogGC` | 1 | Triggers log garbage collection |
| `PeerTickSplitRegionCheck` | 2 | Triggers region size check for split |
| `PeerTickPdHeartbeat` | 3 | Triggers region heartbeat to PD |
| `PeerTickCheckMerge` | 4 | Checks merge proposal status |
| `PeerTickCheckPeerStaleState` | 5 | Detects stale leadership |

### 6.3 ExecResultType (apply execution results)

| Constant | Value | Description |
|----------|-------|-------------|
| `ExecResultTypeNormal` | 0 | Normal command execution |
| `ExecResultTypeSplitRegion` | 1 | Region split completed |
| `ExecResultTypeCompactLog` | 2 | Log compaction completed |
| `ExecResultTypeChangePeer` | 3 | Membership change completed |

### 6.4 StoreMsgType (messages to the store goroutine)

| Constant | Value | Description |
|----------|-------|-------------|
| `StoreMsgTypeRaftMessage` | 0 | Raft message for routing |
| `StoreMsgTypeStoreUnreachable` | 1 | Store unreachable notification |
| `StoreMsgTypeTick` | 2 | Store-level tick |
| `StoreMsgTypeStart` | 3 | Store initialization |
| `StoreMsgTypeCreatePeer` | 4 | Create a new peer |
| `StoreMsgTypeDestroyPeer` | 5 | Destroy an existing peer |

### 6.5 SignificantMsgType (high-priority control)

| Constant | Value | Description |
|----------|-------|-------------|
| `SignificantMsgTypeSnapshotStatus` | 0 | Snapshot send/receive status — `handleSignificantMessage` calls `rawNode.ReportSnapshot(msg.ToPeerID, msg.Status)` |
| `SignificantMsgTypeUnreachable` | 1 | Peer unreachable notification — `handleSignificantMessage` calls `rawNode.ReportUnreachable(msg.ToPeerID)` |
| `SignificantMsgTypeMergeResult` | 2 | Region merge result — `handleSignificantMessage` sets `stopped = true` on the peer |

All three `SignificantMsgType` values are fully handled in `Peer.handleSignificantMessage()`.

### 6.6 Data Structs

| Struct | Fields | Purpose |
|--------|--------|---------|
| `PeerMsg` | `Type PeerMsgType`, `Data interface{}` | Envelope for all peer messages |
| `RaftCommand` | `Request *raft_cmdpb.RaftCmdRequest`, `Callback func(*RaftCmdResponse)` | Client request + response callback |
| `proposalEntry` | `callback func(*RaftCmdResponse)`, `term uint64`, `proposed time.Time` | Tracks in-flight proposal callback with term and timestamp |
| `ApplyResult` | `RegionID uint64`, `AppliedIndex uint64`, `Results []ExecResult` | Apply worker output (includes applied index for async path) |
| `ExecResult` | `Type ExecResultType`, `Data interface{}` | Single execution result |
| `SplitRegionResult` | `Derived *metapb.Region`, `Regions []*metapb.Region` | Split operation output |
| `StoreMsg` | `Type StoreMsgType`, `Data interface{}` | Store-level message envelope |
| `SignificantMsg` | `Type`, `RegionID`, `ToPeerID`, `Status` | High-priority control message |
| `ScheduleMsg` | `Type ScheduleMsgType`, `TransferLeader *pdpb.TransferLeader`, `ChangePeer *pdpb.ChangePeer`, `Merge *pdpb.Merge` | PD scheduling command delivered to a peer |

**Helper functions in `msg.go`:**

| Function | Purpose |
|----------|---------|
| `errorResponse(err error) *RaftCmdResponse` | Builds a `RaftCmdResponse` with the error in the header's `Error` field |
| `IsCompactLog(data []byte) bool` | Returns true if entry data starts with tag byte `0x01` |

**ScheduleMsgType constants:**

| Constant | Value | Description |
|----------|-------|-------------|
| `ScheduleMsgTypeTransferLeader` | 0 | Transfer Raft leadership to another peer |
| `ScheduleMsgTypeChangePeer` | 1 | Add or remove a peer (conf change) |
| `ScheduleMsgTypeMerge` | 2 | Merge this region with a target region |

---

## 7. Processing Flows

### 7.1 Client Write Proposal

```mermaid
sequenceDiagram
    participant Client
    participant Router
    participant Peer
    participant RawNode
    participant PeerStorage
    participant ApplyWorker as applyFunc

    Client->>Router: Send(regionID, PeerMsg{Type: RaftCommand, Data: &RaftCommand{...}})
    Router->>Peer: Mailbox <- msg (non-blocking)
    Peer->>Peer: handleMessage(msg)
    Peer->>Peer: propose(cmd)
    Note over Peer: Generate proposalID, prepend 8-byte ID to data
    Peer->>RawNode: rawNode.Propose(tagged)
    Peer->>Peer: register pendingProposals[proposalID] = proposalEntry{callback, term, now}

    Note over Peer: Raft replicates entry to followers via Ready

    Peer->>Peer: handleReady()
    Peer->>RawNode: rawNode.HasReady() -> true
    Peer->>RawNode: rawNode.Ready() -> rd
    Peer->>PeerStorage: Persist [SaveReady or BuildWriteTask+RaftLogWriter]
    Peer->>Router: sendFunc(rd.Messages) [send to followers]

    Note over Peer: After quorum ack, entry appears in rd.CommittedEntries

    Peer->>Peer: Filter admin entries (inline), then data entries
    Peer->>ApplyWorker: applyFunc(regionID, dataEntries)
    Peer->>Peer: Match callback by proposalID from first 8 bytes (with term check)
    Peer->>Client: callback invoked
    Peer->>RawNode: rawNode.Advance(rd)
```

### 7.2 Raft Message from Remote Peer

```mermaid
sequenceDiagram
    participant Transport as gRPC Transport
    participant Convert as convert.go
    participant Router
    participant Peer
    participant RawNode
    participant PeerStorage

    Transport->>Convert: EraftpbToRaftpb(eraftpb.Message)
    Convert-->>Transport: raftpb.Message
    Transport->>Router: Send(regionID, PeerMsg{Type: RaftMessage, Data: &raftpb.Message})
    Router->>Peer: Mailbox <- msg (non-blocking)
    Peer->>Peer: handleMessage(msg)
    Peer->>RawNode: rawNode.Step(raftMsg)

    Note over Peer: RawNode processes the message (e.g., AppendEntries)

    Peer->>Peer: handleReady()
    Peer->>RawNode: rawNode.HasReady() -> true
    Peer->>RawNode: rawNode.Ready() -> rd
    Peer->>PeerStorage: SaveReady(rd) [persist any new state]
    Peer->>Transport: sendFunc(rd.Messages) [reply messages]
    Peer->>RawNode: rawNode.Advance(rd)
```

---

## 8. Snapshot, Split, and Other Subsystems

### 8.1 Snapshots

Source: `internal/raftstore/snapshot.go`

The snapshot subsystem handles Raft snapshot generation, serialization, transfer, and application.

**State Machine** — `SnapState` tracks snapshot progress per peer:

| State | Meaning |
|-------|---------|
| `SnapStateRelax` | No snapshot in progress |
| `SnapStateGenerating` | Background generation running; `GenSnapTask.ResultCh` pending |
| `SnapStateApplying` | Received snapshot being applied to engine |

**Core Types:**

| Type | Fields | Purpose |
|------|--------|---------|
| `SnapshotData` | `RegionID`, `Version`, `CFFiles []SnapshotCFFile` | Top-level snapshot container |
| `SnapshotCFFile` | `CF`, `KVPairs []SnapKVPair`, `Checksum uint32` | Per-CF key-value data with integrity check |
| `SnapKVPair` | `Key`, `Value []byte` | Single key-value entry |
| `GenSnapTask` | `RegionID`, `Region`, `SnapKey`, `Canceled`, `ResultCh` | Background generation request |
| `SnapKey` | `RegionID`, `Term`, `Index` | Unique snapshot identifier |

**Generation** — `GenerateSnapshotData(engine, region)` scans all three data CFs (`CF_DEFAULT`, `CF_LOCK`, `CF_WRITE`) within the region's key range using an engine snapshot. Each CF's data is collected into a `SnapshotCFFile` with a CRC32 checksum computed by `ComputeCFChecksum`.

**Serialization** — `MarshalSnapshotData` / `UnmarshalSnapshotData` convert `SnapshotData` to/from a binary format using `encoding/gob`.

**Application** — `ApplySnapshotData(engine, data, startKey, endKey)` applies a received snapshot:
1. Clears the existing key range `[startKey, endKey)` via `DeleteRange` on all three data CFs.
2. Verifies the CRC32 checksum of each CF file.
3. Writes all key-value pairs via a `WriteBatch`.

The `startKey`/`endKey` are the region's key range boundaries, passed from `PeerStorage.ApplySnapshot()` which obtains them from `s.region`.

**Background Worker** — `SnapWorker` runs a goroutine that consumes `GenSnapTask` from its task channel. Each task calls `GenerateSnapshotData` and sends the result (or error) back via `GenSnapTask.ResultCh`.

**PeerStorage Integration:**
- `SetSnapTaskCh(ch)` — Wires the async snapshot task channel from the coordinator to the peer storage.
- `SetRegion(region)` — Stores region metadata used when generating snapshot metadata.
- `Snapshot()` — If `snapTaskCh` is wired, calls `RequestSnapshot()` to trigger async generation via the `SnapWorker`. Otherwise returns metadata-only snapshot.
- `RequestSnapshot()` — Initiates async snapshot generation. Sets state to `SnapStateGenerating` and submits a `GenSnapTask` to the `SnapWorker`.
- `ApplySnapshot(snap)` — Validates the incoming snapshot metadata, calls `ApplySnapshotData`, then atomically updates the apply state, hard state, and region state in `CF_RAFT`.
- `CancelGeneratingSnap()` — Cancels an in-progress generation via the `Canceled` atomic flag.

**End-to-End Snapshot Transfer Pipeline:**

The full snapshot transfer flow is now wired end-to-end:

```mermaid
sequenceDiagram
    participant Leader as Leader Peer
    participant PS as PeerStorage
    participant SW as SnapWorker
    participant Send as sendRaftMessage
    participant TC as RaftClient
    participant GRPC as Remote gRPC
    participant Coord as Remote Coordinator
    participant Follower as Follower Peer

    Leader->>Leader: handleReady() — rd.Messages contains MsgSnap
    Leader->>Send: sendFunc(messages)
    Send->>Send: Detect msg.Type == MsgSnap
    Send->>TC: SendSnapshot(storeID, raftMsg, snapData)
    TC->>GRPC: Snapshot stream (1MB chunks)
    GRPC->>Coord: Snapshot() gRPC handler
    Coord->>Coord: HandleSnapshotMessage(msg, data)
    Coord->>Coord: Create peer if needed (maybeCreatePeerForMessage)
    Coord->>Follower: Route message to peer mailbox
    Follower->>Follower: handleReady() — rd.Snapshot non-empty
    Follower->>PS: ApplySnapshot(snap)
    PS->>PS: ApplySnapshotData (clear range, verify checksums, write KVs)
    Follower->>Coord: reportSnapshotStatus(regionID, peerID, status)
    Coord->>Leader: SignificantMsg{SnapshotStatus} via router
    Leader->>Leader: handleSignificantMessage → rawNode.ReportSnapshot
```

On the leader side, `PeerStorage.Snapshot()` triggers `SnapWorker` to generate `SnapshotData` (scanning all three data CFs). When the snapshot result arrives via `GenSnapTask.ResultCh`, the leader's next `handleReady()` includes the snapshot in `rd.Messages` as a `MsgSnap`. The `sendRaftMessage` function detects `MsgSnap` and uses `RaftClient.SendSnapshot` (streaming 1MB chunks) instead of the normal `Send` path. On send failure, `reportSnapshotStatus` with `SnapshotFailure` is sent back.

On the receiving side, the gRPC `Snapshot` handler reassembles the chunks and calls `HandleSnapshotMessage`, which attaches the snapshot data to the Raft message and creates a peer if one doesn't exist for the region (via `maybeCreatePeerForMessage`). When PD is available, `maybeCreatePeerForMessage` queries `pdClient.GetRegionByID()` to obtain full region metadata (including the complete peer list), so the child peer is bootstrapped with the correct cluster configuration. If PD is unavailable or the query fails, it falls back to constructing minimal metadata from the `FromPeer`/`ToPeer` in the Raft message. The follower's `handleReady()` detects a non-empty `rd.Snapshot` and calls `storage.ApplySnapshot()` to apply the data.

### 8.2 Region Split (Raft Admin Command)

Source: `internal/raftstore/split/checker.go`, `internal/raftstore/split_admin.go`

Region splits are implemented as **Raft admin commands**, ensuring strict ordering between data entries and split operations in the Raft log. This eliminates timing gaps where data writes could be proposed with one region epoch but applied with another.

**Split Check (detection):**

| Type | Fields | Purpose |
|------|--------|---------|
| `SplitCheckWorker` | `engine`, `cfg`, `taskCh`, `resultCh`, `stopCh` | Background worker for split checks |
| `SplitCheckWorkerConfig` | `SplitSize` (96 MiB), `MaxSize` (144 MiB), `SplitKeys`, `MaxKeys` | Size thresholds |
| `SplitCheckTask` | `RegionID`, `Region`, `StartKey`, `EndKey`, `Policy` | Check request |
| `SplitCheckResult` | `RegionID`, `SplitKey`, `RegionSize` | Check outcome |

**Size Scanning** — `scanRegionSize(task)` iterates entries across `CF_DEFAULT`, `CF_LOCK`, `CF_WRITE` within `[StartKey, EndKey)`. It accumulates key-value sizes and records a midpoint split key. The scanner decodes the MVCC-encoded key via `mvcc.DecodeKey()` to extract the raw user key, then re-encodes it as `mvcc.EncodeLockKey()` (memcomparable encoding, without timestamp), ensuring consistent region boundary format.

**Split Proposal and Execution:**

```mermaid
sequenceDiagram
    participant SC as StoreCoordinator
    participant Peer as Peer (handleReady)
    participant Raft as Raft Log
    participant PD as PD Server

    SC->>PD: AskBatchSplit(region, splitKey)
    PD-->>SC: newRegionIDs, newPeerIDs

    SC->>Peer: peer.ProposeSplit(SplitAdminRequest)
    Peer->>Raft: rawNode.Propose(data) [tag=0x02]

    Note over Raft: Split enters log at index N<br/>Ordered with data entries

    Raft->>Peer: CommittedEntries includes split admin
    Peer->>Peer: First loop: applySplitAdminEntry()<br/>→ ExecSplitAdmin → UpdateRegion
    Peer->>Peer: applyFunc: ALL entries to engine<br/>(split admin harmlessly skipped)

    Peer->>SC: splitResultCh ← SplitRegionResult
    SC->>SC: BootstrapRegion(child)
    SC->>PD: ReportBatchSplit
```

**Split Admin Command Format** — Tag byte `0x02` (CompactLog uses `0x01`; safe from collision with protobuf `RaftCmdRequest` which starts with `0x0A`):

| Field | Size | Description |
|-------|------|-------------|
| Tag | 1 byte | `0x02` |
| SplitKeyLen | 4 bytes | uint32, big-endian |
| SplitKey | variable | Split boundary key |
| NumNewRegions | 4 bytes | uint32 |
| Per region: RegionID | 8 bytes | uint64 |
| Per region: NumPeerIDs | 4 bytes | uint32 |
| Per region: PeerIDs | 8×N bytes | uint64 each |

**Key Functions:**

| Function | File | Purpose |
|----------|------|---------|
| `MarshalSplitAdminRequest` | `split_admin.go` | Serialize split request |
| `UnmarshalSplitAdminRequest` | `split_admin.go` | Deserialize split request |
| `IsSplitAdmin(data)` | `split_admin.go` | Detect by tag byte |
| `ExecSplitAdmin(peer, req)` | `split_admin.go` | Execute split: call `ExecBatchSplit`, `UpdateRegion` |
| `ProposeSplit(req)` | `split_admin.go` | Propose via `rawNode.Propose(data)` |
| `applySplitAdminEntry(e)` | `split_admin.go` | Process committed split entry |

**Split Execution** — `ExecBatchSplit(region, splitKeys, newRegionIDs, newPeerIDs)` (in `split/checker.go`) creates new region metadata:
1. Validates that split keys fall within the region's range and are in order.
2. Creates new `metapb.Region` entries for each split, assigning new IDs and peers.
3. Updates the original region's `EndKey` and bumps `RegionEpoch.Version`.
4. Returns a `SplitRegionResult` with the derived parent and new child regions.

**handleReady Integration** — In the committed entries processing loop, split admin entries are detected alongside ConfChange entries and processed inline. The split executes BEFORE data entries are sent to the apply path (whether inline or async). Only data entries (`EntryNormal && !IsSplitAdmin && !IsCompactLog && len>8`) are forwarded to `applyFunc`; admin entries are filtered out before reaching the apply worker.

**Follower Behavior** — `applySplitAdminEntry` runs on all replicas (leader and follower). Child regions on followers are created via `maybeCreatePeerForMessage` when the leader's child region sends Raft messages.

**Background Worker** — `SplitCheckWorker.Run()` consumes `SplitCheckTask` from its channel, calls `checkRegion`, and sends `SplitCheckResult` to `resultCh`. The coordinator's `RunSplitResultHandler` processes both split check results (proposing via Raft) and split apply results (bootstrapping children).

### 8.3 Raft Log Compaction

Source: `internal/raftstore/raftlog_gc.go`

Raft log compaction (GC) removes obsolete log entries to prevent unbounded storage growth.

**Core Types:**

| Type | Purpose |
|------|---------|
| `RaftLogGCWorker` | Background goroutine that deletes old Raft log entries |
| `RaftLogGCTask` (via channel) | Contains `regionID`, `startIdx`, `endIdx` for deletion range |

**Compaction Decision** — `Peer.onRaftLogGCTick()` evaluates whether compaction is needed:
1. Computes the gap between the applied index and the last compacted index.
2. If the gap exceeds `RaftLogGCCountLimit` or `raftLogSizeHint` exceeds `RaftLogGCSizeLimit`, a `CompactLog` proposal is submitted to Raft.
3. The compact index is set to `appliedIndex - RaftLogGCThreshold` to retain a tail of entries.

**Proposal Execution** — `execCompactLog(applyState, compactIdx, compactTerm)` validates the request (compact index must exceed current truncated index) and updates `ApplyState.TruncatedIndex` / `TruncatedTerm`.

**Background Deletion** — When the peer processes a `ExecResultTypeCompactLog` apply result, `onReadyCompactLog()` sends a task to `RaftLogGCWorker` via `logGCWorkerCh`. The worker's `gcRaftLog(regionID, startIdx, endIdx)` deletes entries using `engine.DeleteRange(CF_RAFT, startKey, endKey)` and adjusts `raftLogSizeHint`.

**Serialization** — `marshalCompactLogRequest` / `unmarshalCompactLogRequest` encode the compact index and term into the Raft proposal data.

### 8.4 Configuration Changes

Source: `internal/raftstore/conf_change.go`

Configuration changes handle adding and removing Raft group members (peers).

**Core Types:**

| Type | Fields | Purpose |
|------|--------|---------|
| `ChangePeerResult` | `Index`, `ChangeType`, `Peer`, `Region` | Result of a conf change operation |
| `CreatePeerRequest` | `Region`, `PeerID` | Request to create a new peer |
| `DestroyPeerRequest` | `RegionID`, `PeerID` | Request to destroy a peer |

**Proposal** — `ProposeConfChange(changeType, peer)` encodes the peer via `EncodePeerContext` and proposes a `ConfChangeV2` through `rawNode.ProposeConfChange`.

**Application** — `applyConfChangeEntry(entry)` processes committed conf change entries:
1. Unmarshals the `ConfChangeV2` from the entry data.
2. Calls `processConfChange(cc)` to update region metadata.
3. Applies the conf change to etcd/raft via `rawNode.ApplyConfChange`.

**Region Update** — `processConfChange(cc)` modifies the region's peer list:
- **AddNode**: Appends the new peer and bumps `RegionEpoch.ConfVer`.
- **RemoveNode**: Calls `removePeerByNodeID` to remove the peer, bumps `ConfVer`, and detects self-removal (sets `stopped = true`).

**Helpers** — `EncodePeerContext` / `decodePeerFromContext` serialize peer ID and store ID for the conf change context. `cloneRegion` creates a deep copy of region metadata for safe mutation.

### 8.5 Region Merge

Source: `internal/raftstore/merge.go`

Region merge combines two adjacent regions into one, reducing the number of Raft groups.

**State Machine** — `PeerState` tracks merge progress:

| State | Meaning |
|-------|---------|
| `PeerStateNormal` | Normal operation |
| `PeerStateMerging` | Merge in progress (source region) |
| `PeerStateTombstone` | Region has been merged away |

**Core Types:**

| Type | Fields | Purpose |
|------|--------|---------|
| `MergeState` | `MinIndex`, `Commit`, `Target *metapb.Region` | Tracks in-progress merge state |
| `CatchUpLogs` | `TargetRegionID`, `LogsUpToDate` | Signals target has caught up on source logs |
| `PrepareMergeResult` | `Region`, `State *MergeState` | Result of prepare phase |
| `CommitMergeResult` | `Index`, `Region`, `Source *metapb.Region` | Result of commit phase |
| `RollbackMergeResult` | `Region`, `Commit` | Result of rollback |

**Merge Protocol** (three phases):

1. **Prepare** — `ExecPrepareMerge(source, target)` bumps the source region's `RegionEpoch.Version` and creates a `MergeState` recording the target and minimum log index.

2. **Commit** — `ExecCommitMerge(target, source, entries)` extends the target region's key range to encompass the source. It calculates the merged epoch as the maximum of both regions' versions plus one.

3. **Rollback** — `ExecRollbackMerge(region, commit)` resets the region to `PeerStateNormal`, bumps the version, and clears the merge state.

**Result Types** — Three `ExecResultType` constants (`ExecResultTypePrepareMerge`, `ExecResultTypeCommitMerge`, `ExecResultTypeRollbackMerge`) carry merge results through the apply pipeline.

**Merge Result Kinds** — `MergeResultFromTargetLog`, `MergeResultFromTargetSnap`, `MergeResultStale` classify how the target peer received the source's data.

### 8.6 Region Data Cleanup

Source: `internal/raftstore/store_worker.go`

`CleanupRegionData(engine, regionID)` removes all persistent state for a destroyed region:
- Deletes the Raft log range via `DeleteRange(CF_RAFT, startLogKey, endLogKey)`.
- Deletes the Raft hard state key.
- Deletes the apply state key.
- Deletes the region state key.

This is used after a peer is removed via conf change or region merge.

### 8.7 RaftLogWriter (I/O Coalescing)

Source: `internal/raftstore/raft_log_writer.go`

`RaftLogWriter` batches `WriteTask` submissions from multiple peer goroutines into a single `WriteBatch` + fsync per cycle, reducing the number of fsyncs from N (one per region) to 1 per batch cycle.

**Core Types:**

| Type | Fields | Purpose |
|------|--------|---------|
| `WriteOp` | `CF string`, `Key []byte`, `Value []byte` | Single key-value write for Raft log persistence |
| `WriteTask` | `RegionID`, `Ops []WriteOp`, `Done chan error`, `Entries []raftpb.Entry`, `HardState *raftpb.HardState` | One region's Raft log changes for batch persistence |
| `RaftLogWriter` | `engine`, `taskCh chan *WriteTask`, `stopCh`, `wg` | Shared writer goroutine |

**Processing Loop** — `run()` waits for a task on `taskCh`, then `processBatch()` drains all additionally queued tasks, builds a single `WriteBatch` from all tasks' `Ops`, and commits with a single fsync. All tasks in the batch receive the same error (or nil) on their `Done` channel. On panic, `failRemaining()` drains and fails all pending tasks.

**PeerStorage Integration:**

1. `BuildWriteTask(rd)` — Serializes `rd.HardState` and `rd.Entries` into `WriteOp` structs. Stores `Entries` and `HardState` in the task for post-persist in-memory updates.
2. `ApplyWriteTaskPostPersist(task)` — Called by the peer after `task.Done` is signaled. Updates in-memory `hardState`, `appendToCache(entries)`, and `persistedLastIndex`.

**Peer Integration** — In `handleReady()`, if `p.raftLogWriter != nil`, the peer calls `BuildWriteTask`, `Submit`, waits on `Done`, then `ApplyWriteTaskPostPersist`. Otherwise, the legacy `SaveReady` path is used.

### 8.8 ApplyWorkerPool (Async Apply)

Source: `internal/raftstore/apply_worker.go`

`ApplyWorkerPool` processes committed data entries asynchronously in a goroutine pool, decoupling Raft log commit from state machine application.

**Core Types:**

| Type | Fields | Purpose |
|------|--------|---------|
| `ApplyTask` | `RegionID`, `Entries []raftpb.Entry`, `ApplyFunc`, `Callbacks map[uint64]proposalEntry`, `CurrentTerm`, `ResultCh chan<- PeerMsg`, `LastCommittedIndex uint64` | Committed data entries + metadata for one region |
| `ApplyWorkerPool` | `workers int`, `taskCh`, `stopCh`, `stopped`, `wg` | Fixed-size goroutine pool (default 4 workers) |

**Task Processing** (`processTask`):
1. Call `ApplyFunc(regionID, entries)` to write data entries to the KV engine.
2. Invoke proposal callbacks: extract `proposalID` from first 8 bytes of each entry, look up in `Callbacks`, call with nil on success or `errorResponse` on term mismatch.
3. Set `appliedIndex = LastCommittedIndex` and send `ApplyResult` back to peer via `ResultCh` (the peer's mailbox).

**Peer Integration:**

- `submitToApplyWorker(committedEntries)`: Filters data entries (`EntryNormal && !IsSplitAdmin && !IsCompactLog && len>8`), extracts callbacks from `pendingProposals`, builds `ApplyTask`. For admin-only batches (no data entries), updates applied index inline and persists ApplyState. Uses `applyInFlight` flag to enforce per-region FIFO: if an apply is in-flight, the task is buffered in `pendingApplyTasks`.
- `onApplyResult(result)`: Monotonically advances applied index via `SetAppliedIndex`, persists `ApplyState`, sweeps `pendingReads`, clears `applyInFlight`, and submits the next queued task from `pendingApplyTasks`.

---

## 9. Implementation Status

### Implemented

- **Peer lifecycle** — `NewPeer` with bootstrap and restart paths; proper `raft.Config` construction with `CheckQuorum` and `PreVote`.
- **Event loop** — `Peer.Run()` with `select` on context cancellation, ticker, and mailbox; `handleReady()` called after every event.
- **Ready processing** — Full pipeline: leader status update (with `failAllPendingProposals` on stepdown), PD heartbeat on leader transition, `currentTerm` caching from HardState, conditional persistence (SaveReady or BuildWriteTask+RaftLogWriter), admin entry inline processing, conditional apply (applyInline or submitToApplyWorker), `Advance`. Guarded by `stopped` flag check.
- **PeerStorage with CF_RAFT persistence** — Atomic `WriteBatch` writes for hard state and entries; key scheme via `keys.RaftStateKey` and `keys.RaftLogKey`.
- **PeerStorage recovery** — `RecoverFromEngine()` restores hard state, scans log entries, and recovers `ApplyState` from the engine on restart. `InitialState()` derives `ConfState` from region peers. `readEntriesFromEngine()` returns an error on gap detection.
- **Entry cache** — In-memory cache of last 1024 entries with overlap-aware append logic.
- **Router with sync.Map dispatch** — Non-blocking sends, broadcast, store-level channel, error sentinels for not-found and full mailboxes.
- **Protobuf conversion** — `EraftpbToRaftpb` / `RaftpbToEraftpb` for transport interop.
- **Proposal tracking** — `pendingProposals` maps `proposalID` to `proposalEntry` (callback, term, timestamp). `propose()` generates monotonic `nextProposalID`, prepends 8-byte ID to entry data. `handleReady` matches callbacks by extracting proposalID from first 8 bytes of committed entry data, with term-mismatch detection. `failAllPendingProposals()` on leader stepdown, `sweepStaleProposals()` for timeout cleanup. `errorResponse()` helper builds error responses.
- **ConfChange processing** — `applyConfChangeEntry` parses and applies ConfChange/ConfChangeV2 entries; `processConfChange` updates region metadata (peer list, epoch); `ProposeConfChange` proposes membership changes; self-removal detection.
- **Snapshot generation and application** — `SnapWorker` generates snapshots in the background; `PeerStorage.ApplySnapshot` applies received snapshots with checksum verification; `SnapState` FSM tracks progress.
- **Region split (Raft admin command)** — `SplitCheckWorker` detects oversized regions via CF scanning. Splits are proposed as Raft admin commands (tag byte `0x02`) and executed during `handleReady` in the committed entries loop. `ExecSplitAdmin` calls `ExecBatchSplit` and updates parent region metadata atomically within the Raft apply path. Child regions are bootstrapped by the coordinator after receiving the split result via `splitResultCh`.
- **Raft log compaction** — `onRaftLogGCTick` evaluates size/count thresholds; `execCompactLog` advances `TruncatedIndex`; `RaftLogGCWorker` deletes old entries in the background.
- **Region merge** — `ExecPrepareMerge` / `ExecCommitMerge` / `ExecRollbackMerge` implement the three-phase merge protocol with epoch management.
- **PD heartbeat** — `sendRegionHeartbeatToPD()` sends region leader info to PD via `pdTaskCh` on leadership change.
- **Apply result processing** — `onApplyResult()` updates `AppliedIndex` (with monotonicity guard), persists `ApplyState`, sweeps pending reads, clears `applyInFlight` flag, and submits next queued task from `pendingApplyTasks`. Processes `ExecResultTypeCompactLog` via `onReadyCompactLog()`.
- **Region data cleanup** — `CleanupRegionData` removes all Raft state for destroyed regions.
- **RaftLogWriter (I/O coalescing)** — `raft_log_writer.go`: `RaftLogWriter` batches `WriteTask`s from multiple peer goroutines into a single `WriteBatch` + fsync. `storage.go`: `BuildWriteTask()` + `ApplyWriteTaskPostPersist()` split `SaveReady` into build/submit/post-persist phases. `peer.go`: conditional path -- `raftLogWriter != nil` submits `WriteTask`, else legacy `SaveReady`.
- **ApplyWorkerPool (async apply)** — `apply_worker.go`: `ApplyWorkerPool` processes `ApplyTask`s in a goroutine pool (default 4 workers). `peer.go`: `submitToApplyWorker()` filters data entries, extracts callbacks, and submits `ApplyTask`; `applyInline()` is the legacy synchronous path. `onApplyResult()` handles async results: `SetAppliedIndex`, `PersistApplyState`, sweep `pendingReads`. `LastCommittedIndex` in `ApplyTask` ensures applied index advances past admin-only batches. `applyInFlight` flag + `pendingApplyTasks` buffer enforce per-region FIFO ordering.
- **Leader lease as atomic.Int64** — `leaseExpiryNanos` stored as `atomic.Int64` (Unix nanoseconds) for safe concurrent access from multiple goroutines, replacing the unprotected `time.Time` field.
- **Mailbox not closed on Destroy** — `PeerMsgTypeDestroy` sets `stopped = true` without closing the mailbox channel, preventing panics from concurrent senders. The `Run()` loop exits via the `stopped` flag check after each event.

- **Store goroutine** — `RunStoreWorker` (in `StoreCoordinator`) is started in `main.go`. It listens on `router.StoreCh()` and handles `CreatePeer`, `DestroyPeer`, and `RaftMessage` (for unknown regions). `HandleRaftMessage` falls back to `storeCh` on `ErrRegionNotFound`, enabling dynamic peer creation. The `maybeCreatePeerForMessage` function queries PD via `pdClient.GetRegionByID()` for full region metadata when creating child peers (falling back to minimal metadata from the message if PD is unavailable).
- **Significant messages** — All three `SignificantMsgType` values are fully handled in `handleSignificantMessage()`: `Unreachable` → `rawNode.ReportUnreachable`, `SnapshotStatus` → `rawNode.ReportSnapshot`, `MergeResult` → `stopped = true`.
- **PD scheduling messages** — `PeerMsgTypeSchedule` (value 8) dispatches to `handleScheduleMessage()`. Only the leader executes; routes `TransferLeader`, `ChangePeer`, and `Merge` commands from PD's scheduler.
- **Snapshot transfer** — Fully wired end-to-end: `PeerStorage.Snapshot()` → `SnapWorker` generation → `handleReady` applies snapshots → `sendRaftMessage` detects `MsgSnap` → `SendSnapshot` streaming → remote `Snapshot` gRPC handler → `HandleSnapshotMessage` → `ApplySnapshot` → `reportSnapshotStatus`.
- **PD-coordinated split (Raft admin command)** — Wired end-to-end. Peers call `onSplitCheckTick()` at a configurable interval (`SplitCheckTickInterval`, default 10s) when acting as leader. The coordinator's `handleSplitCheckResult` calls `AskBatchSplit` on PD for new IDs, then proposes a `SplitAdminRequest` via `peer.ProposeSplit()` (fire-and-forget, same pattern as CompactLog). When the split entry is committed, `handleReady` detects it and calls `applySplitAdminEntry` → `ExecSplitAdmin` → `UpdateRegion`. The coordinator's `RunSplitResultHandler` receives the result via `splitResultCh` and bootstraps child regions, sends `PeerMsgTypeTick` to each for prompt peer creation on followers, and reports to PD via `ReportBatchSplit`.

### Not Implemented

- **Casual messages** — `PeerMsgTypeCasual` is defined but not handled in `handleMessage`.
- **PeerMsgTypeStart** — Defined but not handled.
