# Raft and Replication

This document specifies TiKV's Raft consensus, region management, and replication layer. It covers the protocol implementation, region lifecycle, batch-system architecture, snapshot mechanics, and safety invariants needed to reimplement the consensus layer from scratch.

Cross-references: [Architecture Overview](architecture_overview.md) for system context, [Key Encoding and Data Formats](key_encoding_and_data_formats.md) for key layout consumed by the raftstore.

## 1. Raft Protocol Implementation

### 1.1 External Raft Library

TiKV uses the **raft-rs** crate (`raft` v0.7.0, forked at `github.com/tikv/raft-rs`) for core Raft consensus. The raftstore wraps raft-rs with region-aware application logic — it does not reimplement the Raft algorithm itself.

Key types from raft-rs consumed by TiKV:

| Type | Purpose |
|------|---------|
| `RawNode<T: Storage>` | Main Raft state machine; accepts proposals, steps messages, produces `Ready` |
| `Ready` | Bundle of pending state changes: new entries, committed entries, messages, snapshots, hard/soft state updates |
| `LightReady` | Lightweight subset of `Ready` containing only committed entries and messages (no persistence needed) |
| `Storage` trait | Interface TiKV implements to provide persistent log access to raft-rs |
| `eraftpb::Message` | Raft protocol messages (MsgAppend, MsgVote, MsgHeartbeat, etc.) |
| `eraftpb::Entry` | Log entry: `(term, index, entry_type, data)` |
| `eraftpb::HardState` | Persistent Raft state: `(term, vote, commit)` |
| `eraftpb::ConfState` | Cluster membership: voters and learners |
| `eraftpb::Snapshot` | Snapshot metadata and data blob |

### 1.2 RawNode Lifecycle

The `RawNode` API drives the Raft state machine through a poll-based loop:

```
loop {
    // 1. Receive and step incoming messages
    for msg in incoming_messages {
        raw_node.step(msg);
    }

    // 2. Advance Raft ticks (heartbeat, election)
    raw_node.tick();

    // 3. Propose new entries (leader only)
    raw_node.propose(context, data);

    // 4. Check for state changes
    if raw_node.has_ready() {
        let ready = raw_node.ready();

        // 4a. Persist entries and hard state
        persist(ready.entries(), ready.hs());

        // 4b. Send messages to peers
        send(ready.messages());

        // 4c. Apply committed entries to state machine
        apply(ready.committed_entries());

        // 4d. Install snapshot if present
        if !ready.snapshot().is_empty() {
            install_snapshot(ready.snapshot());
        }

        // 4e. Advance the state machine
        let light_ready = raw_node.advance_append(ready);
        // Or async: raw_node.advance_append_async(ready);

        // 4f. Notify applied index
        raw_node.advance_apply(applied_index);
    }
}
```

### 1.3 Storage Trait Implementation

TiKV implements the `Storage` trait via `PeerStorage` (`components/raftstore/src/store/peer_storage.rs`):

```rust
impl<EK: KvEngine, ER: RaftEngine> Storage for PeerStorage<EK, ER> {
    fn initial_state(&self) -> Result<RaftState>;           // HardState + ConfState from disk
    fn entries(&self, low: u64, high: u64, max_size: impl Into<Option<u64>>,
               context: GetEntriesContext) -> Result<Vec<Entry>>;
    fn term(&self, idx: u64) -> Result<u64>;
    fn first_index(&self) -> Result<u64>;                   // truncated_index + 1
    fn last_index(&self) -> Result<u64>;
    fn snapshot(&self, request_index: u64, to: u64) -> Result<Snapshot>;  // Async generation
}
```

**PeerStorage** wraps **EntryStorage** (`components/raftstore/src/store/entry_storage.rs`) which manages:

| Field | Type | Purpose |
|-------|------|---------|
| `raft_engine` | `ER` | Persistent Raft log storage (separate from KV engine) |
| `cache` | `EntryCache` | In-memory LRU cache of recent entries |
| `term_cache` | `TermCache` | Fast term lookups without reading entries |
| `raft_state` | `RaftLocalState` | Persisted: `{hard_state, last_index}` |
| `apply_state` | `RaftApplyState` | Persisted: `{applied_index, truncated_state}` |

**Key index invariants:**

```
truncated_index < first_index <= applied_index <= commit_index <= last_index
```

- `truncated_index`: Oldest retained log entry (entries before this are GC'd)
- `first_index`: `truncated_index + 1` (first available entry)
- `applied_index`: Highest entry applied to state machine
- `commit_index`: Highest entry replicated on a majority
- `last_index`: Highest entry in the log

### 1.4 Raft Configuration

Configuration is defined in `components/raftstore/src/store/config.rs` and mapped to `raft::Config`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `raft_base_tick_interval` | 100ms | Base unit for all tick intervals |
| `raft_heartbeat_ticks` | 2 | Heartbeat every 2 ticks (200ms) |
| `raft_election_timeout_ticks` | 10 | Election timeout is 10 ticks (1000ms) |
| `raft_min_election_timeout_ticks` | 0 | Randomization floor (0 = disabled) |
| `raft_max_election_timeout_ticks` | 0 | Randomization ceiling (0 = disabled) |
| `raft_max_inflight_msgs` | 256 | Max unacknowledged messages in flight |
| `raft_max_size_per_msg` | 1 MiB | Maximum size of individual Raft messages |
| `raft_entry_max_size` | 8 MiB | Maximum single entry size |
| `check_quorum` | true | Leader must maintain quorum contact |
| `skip_bcast_commit` | true | Don't broadcast commit index on heartbeats |
| `pre_vote` | configurable | Enable PreVote phase for safer elections |

**Derived raft::Config mapping** (config.rs ~line 680-695):

```rust
raft::Config {
    id: self_peer_id,
    election_tick: raft_election_timeout_ticks,
    heartbeat_tick: raft_heartbeat_ticks,
    max_inflight_msgs: raft_max_inflight_msgs,
    max_size_per_msg: raft_max_size_per_msg,
    check_quorum: true,
    skip_bcast_commit: true,
    pre_vote: cfg.prevote,
    ..
}
```

### 1.5 Election and Leader Transfer

**Election flow** (managed by raft-rs, orchestrated by TiKV):

1. Follower detects no heartbeat within `election_timeout` ticks
2. If `pre_vote` enabled: transition to PreCandidate, send PreVote RPCs
3. If PreVote majority: transition to Candidate, increment term, vote for self, send RequestVote RPCs
4. If majority of RequestVote responses: become Leader
5. New Leader sends initial empty AppendEntries as heartbeat to all peers

**Campaign types** (msg.rs):

```rust
pub enum CampaignType {
    ForceLeader,           // Unsafe recovery: forcibly become leader
    UnsafeSplitCampaign,   // After split when parent was leader
}
```

**Leader transfer**: Leader voluntarily transfers leadership via `RawNode::transfer_leader(transferee)`. Used when PD rebalances leaders.

### 1.6 Log Replication

**Replication states per follower** (region_meta.rs):

| State | Behavior |
|-------|----------|
| `Probe` | Send one entry, wait for ACK before sending more. Used for new/slow followers. |
| `Replicate` | Normal state. Send multiple entries (up to `max_inflight_msgs`). Pipeline mode. |
| `Snapshot` | Follower too far behind. Send snapshot instead of entries. |

**Replication algorithm** (pseudocode):

```
// Leader side (per follower):
on_tick_heartbeat:
    for each follower:
        if state == Replicate:
            send_entries(next_index..last_index, max_size_per_msg)
        elif state == Probe:
            send_entries(next_index..next_index+1)
        elif state == Snapshot:
            if snapshot_ready:
                send_snapshot()

on_append_response(from, match_index):
    update follower.match_index = match_index
    follower.next_index = match_index + 1
    if state == Probe:
        transition to Replicate

    // Commit check
    new_commit = quorum_match_index()
    if new_commit > commit_index:
        commit_index = new_commit

on_append_reject(from, reject_index):
    // Follower's log diverges
    follower.next_index = min(reject_index, follower.match_index + 1)
    if state == Replicate:
        transition to Probe
```

### 1.7 Configuration Changes (Conf-Change)

TiKV supports both simple and joint-consensus configuration changes:

```rust
pub enum ConfChangeKind {
    Simple,        // Single peer add/remove
    EnterJoint,    // Enter joint consensus (multi-change)
    LeaveJoint,    // Complete joint consensus transition
}
```

**Simple conf-change** (single peer add/remove):
1. Leader proposes `ConfChange` entry
2. Entry is replicated and committed via normal Raft
3. On apply: update membership, increment `conf_ver` in region epoch
4. [Inferred] Old and new configurations both used during transition

**Joint consensus** (multiple changes atomically):
1. Leader proposes `ConfChangeV2` with `EnterJoint` transition
2. On apply: enter joint config (`C_old ∪ C_new`), quorum requires majority of both old and new
3. Leader proposes `ConfChangeV2` with `LeaveJoint` transition
4. On apply: finalize to `C_new` only

**Peer roles** during joint consensus:

| Role | Meaning |
|------|---------|
| `Voter` | Full voting member |
| `Learner` | Receives replication but does not vote |
| `IncomingVoter` | New voter during joint consensus (votes in C_new) |
| `DemotingVoter` | Departing voter during joint consensus (votes in C_old) |

## 2. Region Abstraction

### 2.1 Region Definition

A **region** is a contiguous key range managed by a single Raft group. Defined in kvproto (`metapb.proto`):

```protobuf
message Region {
    uint64 id = 1;                      // Unique region ID (assigned by PD)
    bytes start_key = 2;                // Inclusive start key
    bytes end_key = 3;                  // Exclusive end key (empty = +∞)
    RegionEpoch region_epoch = 4;       // Versioning for staleness detection
    repeated Peer peers = 5;            // All replicas in this Raft group
}

message RegionEpoch {
    uint64 conf_ver = 1;                // Incremented on membership change
    uint64 version = 2;                 // Incremented on split/merge
}

message Peer {
    uint64 id = 1;                      // Unique peer ID (assigned by PD)
    uint64 store_id = 2;                // Which TiKV store hosts this peer
    PeerRole role = 3;                  // Voter / Learner / IncomingVoter / DemotingVoter
    bool is_witness = 4;                // Witness peer (no data, only votes)
}
```

**Key range semantics**: A key `k` belongs to region `R` iff `R.start_key <= k < R.end_key`. Empty `end_key` means unbounded (extends to +∞).

### 2.2 Region Epoch Semantics

The epoch is a two-component version vector preventing stale operations:

- **`version`**: Incremented on **split** and **merge** — tracks key range changes
- **`conf_ver`**: Incremented on **membership change** (add/remove peer) — tracks replica set changes

**Epoch staleness check** (util.rs):

```rust
fn is_epoch_stale(epoch: &RegionEpoch, check_epoch: &RegionEpoch) -> bool {
    epoch.version < check_epoch.version
        || epoch.conf_ver < check_epoch.conf_ver
}
```

**Which operations check/change which epoch component** (util.rs admin_cmd_epoch_lookup):

| Admin Command | Check ver | Check conf_ver | Change ver | Change conf_ver |
|---------------|-----------|----------------|------------|-----------------|
| `ChangePeer` | | ✓ | | ✓ |
| `Split` / `BatchSplit` | ✓ | ✓ | ✓ | |
| `PrepareMerge` | ✓ | ✓ | ✓ | ✓ |
| `CommitMerge` | ✓ | ✓ | ✓ | |
| `RollbackMerge` | ✓ | ✓ | ✓ | |
| `TransferLeader` | | | | |
| `CompactLog` | | | | |

**Request validation flow**:
1. Client sends request with its cached `RegionEpoch`
2. Server compares against current epoch via `check_region_epoch()`
3. If stale: returns `EpochNotMatch` error with current region info
4. Client updates cache, retries with new epoch

Normal (non-admin) read/write requests only check `version` (key range validity), not `conf_ver`.

### 2.3 Region Metadata Storage

**In-memory** — `StoreMeta` (fsm/store.rs):

```rust
pub struct StoreMeta {
    pub store_id: Option<u64>,
    pub region_ranges: BTreeMap<Vec<u8>, u64>,      // end_key -> region_id (ordered index)
    pub regions: HashMap<u64, Region>,               // region_id -> Region
    pub readers: HashMap<u64, ReadDelegate>,          // region_id -> read delegate
    pub pending_msgs: RingQueue<RaftMessage>,         // Messages for not-yet-created peers
    pub pending_snapshot_regions: Vec<Region>,         // Regions with pending snapshots
    pub pending_merge_targets: HashMap<u64, HashMap<u64, Region>>,
    pub region_read_progress: RegionReadProgressRegistry,
}
```

**On-disk** — Persisted per-region in the KV engine under CF_RAFT:

| Key | Value | Purpose |
|-----|-------|---------|
| `region_state_key(region_id)` | `RegionLocalState` | Region metadata + peer state (Normal/Applying/Merging/Tombstone) |
| `raft_state_key(region_id)` | `RaftLocalState` | Hard state + last log index |
| `apply_state_key(region_id)` | `RaftApplyState` | Applied index + truncated state |

## 3. Region Lifecycle

### 3.1 Initial Bootstrap

When a TiKV cluster starts fresh:

1. First node initializes with a single region covering the entire key space `["", "")` (region 1)
2. PD assigns `region_id=1` and peer IDs
3. Node creates `PeerStorage` with empty Raft log
4. `RaftLocalState` initialized: `term=RAFT_INIT_LOG_TERM(5), vote=0, last_index=RAFT_INIT_LOG_INDEX(5)`
5. First election: single-node immediately becomes leader
6. Additional nodes join via conf-change (PD schedules AddPeer)

[Inferred] The initial term/index constants (both 5) are chosen to distinguish bootstrapped state from zero-value defaults.

### 3.2 Peer Creation

New peers are created in two scenarios:

**From PD scheduling** (AddPeer conf-change):
1. PD sends `ChangePeer` schedule to leader
2. Leader proposes `ConfChange` entry
3. On apply: region metadata updated with new peer
4. Leader starts sending Raft messages to new store
5. New store receives `RaftMessage` for unknown region → creates uninitialized `PeerFsm`
6. New peer catches up via log replication or snapshot

**From region split** (see §3.3):
1. Parent region applies split command
2. Creates `SplitInit` data for each new peer
3. Store FSM creates new `PeerFsm` for each derived region
4. New peers initialized from split data (no snapshot needed)

### 3.3 Region Split

Split divides one region into multiple regions with non-overlapping key ranges. Triggered when a region exceeds the size threshold (default ~96 MiB, checked by `SplitCheckRunner`).

**Split flow** (raftstore apply.rs `exec_batch_split`, raftstore-v2 operation/command/admin/split.rs):

```
Phase 1 — Proposal:
  1. SplitCheckRunner detects region size exceeds threshold
  2. Sends CasualMessage::SplitRegion or HalfSplitRegion to peer FSM
  3. Peer proposes BatchSplit admin command with split keys
  4. Validates: split keys are in ascending order, all within region range
  5. PD assigns new region_id and peer_ids for each new region

Phase 2 — Apply (exec_batch_split):
  1. Increment region epoch version by number of new regions
  2. For each split key, create a new metapb::Region:
     - Assign new region_id and peers
     - Set key range: [prev_split_key, split_key)
     - Copy epoch from parent (with incremented version)
  3. Derived region (parent) gets remaining key range
  4. If right_derive: original region_id goes to rightmost region
  5. Write all region states atomically to KV engine
  6. Return ExecResult::SplitRegion { regions, derived }

Phase 3 — Post-apply (on_ready_split_region):
  1. Parent peer updates in-memory metadata (StoreMeta)
  2. Update region_ranges BTreeMap with new boundaries
  3. Create PeerFsm for each new region on this store
  4. If parent was leader: new peers campaign immediately
  5. Report new regions to PD via heartbeat
```

**SplitResult structure** (raftstore-v2):

```rust
pub struct SplitResult {
    pub regions: Vec<Region>,            // All regions including source
    pub derived_index: usize,            // Index of the region that continues parent data
    pub tablet_index: u64,               // Checkpoint index for v2 tablets
    pub share_source_region_size: bool,  // Share parent's size estimates
}
```

**Region epoch change**: `version` incremented by the number of new regions.

### 3.4 Region Merge

Merge combines two adjacent regions into one. Triggered when a region is too small (typically < 20 MiB). This is a complex three-phase protocol:

```
Phase 1 — PrepareMerge (source region):
  1. PD schedules merge: source region merges INTO target region
  2. Source leader proposes PrepareMerge admin command
  3. On apply:
     a. Compute min_index = min(matched_index) across all followers
     b. Create MergeState { min_index, commit, target: target_region }
     c. Set source peer state to Merging
     d. Increment both version and conf_ver in source epoch
  4. Source region STOPS accepting new write proposals
  5. Source continues replicating logs until all peers reach min_index

Phase 2 — CommitMerge (target region):
  1. Target receives CatchUpLogs from source (via SignificantMsg)
  2. Target proposes CommitMerge admin command
  3. On apply (exec_commit_merge):
     a. Target waits for source to apply all logs (WaitMergeSource)
     b. Merge source's data into target's key range
     c. Expand target's key range to cover source's range
     d. Set source region state to Tombstone
     e. Increment target's version
  4. Target peer reports merged region to PD

Phase 3 — Cleanup:
  1. Source peers receive MergeResult via SignificantMsg
  2. Source peer FSMs destroy themselves
  3. Source region metadata cleared from StoreMeta
```

**Merge rollback**: If the leader changes during merge or merge times out:
1. New leader proposes `RollbackMerge` admin command
2. Source returns to `PeerState::Normal`
3. Version incremented to prevent duplicate rollback
4. Write proposals resume

**Safety invariant**: CommitMerge is only applied when the target region has verified that the source region's logs are fully applied. This prevents data loss from partially-applied merges.

### 3.5 Region Destruction

Regions are destroyed in these scenarios:

| Trigger | Process |
|---------|---------|
| Merge complete (source) | Source receives `MergeResult`, clears state, FSM stops |
| PD removes peer | Peer receives `ConfChange` remove, marks `pending_remove`, drains in-flight ops, clears state |
| Stale peer detection | Peer receives message with higher epoch, detects self is stale, self-destructs |

**Destruction sequence**:
1. Set `pending_remove = Some(reason)`
2. Reject all new read/write proposals
3. Wait for in-flight apply tasks to complete
4. Send `SignificantMsg::ReadyToDestroyPeer` to confirm
5. Clear Raft state and region metadata from both KV and Raft engines
6. Remove from `StoreMeta.regions` and `region_ranges`
7. FSM enters stopped state

## 4. Batch System Architecture

### 4.1 Overview

The batch system (`components/batch-system/`) is a custom FSM runtime that drives all Raft processing. It provides:

- **Batched message processing**: Multiple messages processed per poll cycle (reduces syscall overhead)
- **Two FSM types**: `StoreFsm` (one per store) and `PeerFsm` (one per region)
- **Lock-free routing**: Messages routed to FSM mailboxes via `DashMap`-based `Router`

### 4.2 Core Abstractions

```
┌──────────────────────────────────────────────────────┐
│                      Router                           │
│  normals: DashMap<region_id, BasicMailbox<PeerFsm>>  │
│  control_box: BasicMailbox<StoreFsm>                  │
├──────────────────────────────────────────────────────┤
│  route(region_id, msg) → peer mailbox → enqueue msg  │
│  route_to_store(msg) → control mailbox → enqueue msg │
│  broadcast(msg) → all peer mailboxes                  │
└──────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────┐         ┌──────────────────────┐
│   StoreFsm      │         │  PeerFsm (per region)│
│  (control FSM)  │         │                      │
│                 │         │  peer: Peer<EK,ER>   │
│  Handles:       │         │  mailbox: BasicMailbox│
│  - StoreMsg     │         │  receiver: Receiver  │
│  - StoreTick    │         │  tick_registry: [bool]│
│  - Lifecycle    │         │  hibernate_state     │
└─────────────────┘         └──────────────────────┘
```

**Router** (`batch-system/src/router.rs`):

```rust
pub struct Router<N: Fsm, C: Fsm, Ns, Cs> {
    normals: Arc<DashMap<u64, BasicMailbox<N>>>,   // region_id → peer mailbox
    control_box: BasicMailbox<C>,                   // store mailbox (singleton)
    normal_scheduler: Ns,                           // schedules peer FSMs to poll threads
    control_scheduler: Cs,                          // schedules store FSM
    state_cnt: Arc<AtomicUsize>,                   // active FSM count
    shutdown: Arc<AtomicBool>,
}
```

**FSM state machine** (`batch-system/src/fsm.rs`):

```rust
// FsmState transitions (atomic CAS):
const NOTIFYSTATE_IDLE = 1;       // FSM is idle, owns itself
const NOTIFYSTATE_NOTIFIED = 0;   // FSM is scheduled, executor owns it
const NOTIFYSTATE_DROP = 2;       // FSM is being destroyed
```

Notification protocol:
1. Message arrives at mailbox
2. CAS `FsmState` from `IDLE → NOTIFIED`
3. If successful: FSM is taken from mailbox, scheduled to a poll thread
4. Poll thread calls handler with all pending messages (batched)
5. Handler returns; FSM released: `NOTIFIED → IDLE`
6. If messages arrived during handling: FSM immediately re-scheduled

### 4.3 Message Types

#### PeerMsg — Messages to individual regions

```rust
pub enum PeerMsg<EK: KvEngine> {
    RaftMessage(Box<InspectedRaftMessage>, Option<Instant>),  // Raft protocol message from peer
    RaftCommand(Box<RaftCommand<EK::Snapshot>>),              // Client read/write request
    Tick(PeerTick),                                           // Timer tick
    ApplyRes(Box<ApplyTaskRes<EK::Snapshot>>),                // Result from apply FSM
    SignificantMsg(Box<SignificantMsg<EK::Snapshot>>),         // High-priority control message
    Start,                                                     // Initialize peer
    Noop,                                                      // No-op (wake up FSM)
    Persisted { peer_id: u64, ready_number: u64 },            // I/O persistence complete
    CasualMessage(Box<CasualMessage<EK>>),                    // Low-priority, droppable message
    HeartbeatPd,                                               // Trigger PD heartbeat
    UpdateReplicationMode,                                     // Replication mode change
    Destroy(u64),                                              // Destroy this peer
}
```

**Message reliability guarantees**:
- **Must deliver**: `RaftMessage`, `RaftCommand`, `ApplyRes`, `SignificantMsg` — dropping causes correctness issues
- **Best effort**: `CasualMessage`, `Noop`, `HeartbeatPd` — can be dropped under pressure

#### StoreMsg — Messages to the store FSM

```rust
pub enum StoreMsg<EK: KvEngine> {
    RaftMessage(Box<InspectedRaftMessage>),        // Message for unknown region (pending)
    StoreUnreachable { store_id: u64 },            // Remote store unreachable
    CompactedEvent(EK::CompactedEvent),            // RocksDB compaction notification
    ClearRegionSizeInRange { start_key, end_key }, // Clear cached sizes after split
    Tick(StoreTick),                                // Store-level timer tick
    Start { store: metapb::Store },                 // Bootstrap the store
    UpdateReplicationMode(ReplicationStatus),        // DR auto-sync mode update
    UnsafeRecoveryReport(pdpb::StoreReport),        // Unsafe recovery status
    UnsafeRecoveryCreatePeer { syncer, create },     // Force-create peer for recovery
    GcSnapshotFinish,                               // Snapshot GC round complete
    AwakenRegions { abnormal_stores, region_ids },  // Wake hibernated regions
}
```

#### SignificantMsg — High-priority control messages

```rust
pub enum SignificantMsg<SK: Snapshot> {
    SnapshotStatus { region_id, to_peer_id, status },   // Snapshot send result
    StoreUnreachable { store_id },                       // Peer on unreachable store
    Unreachable { region_id, to_peer_id },               // Specific peer unreachable
    CatchUpLogs(CatchUpLogs),                            // For merge: source catches up
    MergeResult { target_region_id, target, result },    // Merge outcome
    StoreResolved { store_id, group_id },                // Resolved ts for store
    CaptureChange { cmd, region_epoch, callback },       // CDC observer registration
    LeaderCallback(Callback),                            // Execute callback on leader
    RaftLogGcFlushed,                                    // Log GC I/O complete
    EnterForceLeaderState { syncer, failed_stores },     // Unsafe recovery
    ExitForceLeaderState,                                // Exit forced leadership
    UnsafeRecoveryDemoteFailedVoters { .. },             // Demote failed voters
    UnsafeRecoveryDestroy(..),                           // Destroy for recovery
    ReadyToDestroyPeer { to_peer_id, merged_by_target, .. }, // Destruction confirmed
}
```

#### CasualMessage — Droppable messages

```rust
pub enum CasualMessage<EK: KvEngine> {
    SplitRegion { region_epoch, split_keys, callback, source, .. },  // Trigger split
    HalfSplitRegion { region_epoch, start_key, end_key, policy, .. }, // Half-split
    RegionApproximateSize { size, splitable },          // Size estimate update
    RegionApproximateKeys { keys, splitable },          // Key count estimate
    CompactionDeclinedBytes { bytes },                  // Compaction pressure signal
    GcSnap { snaps: Vec<(SnapKey, bool)> },            // GC old snapshots
    ClearRegionSize,                                    // Reset size estimate
    RegionOverlapped,                                   // Key range overlap detected
    SnapshotGenerated,                                  // Snapshot ready for sending
    SnapshotApplied { peer_id, tombstone },             // Snapshot applied
    ForceCompactRaftLogs,                               // Force log compaction
    Campaign(CampaignType),                             // Trigger election
    AccessPeer(Box<dyn FnOnce(RegionMeta)>),            // Read-only peer access
    RenewLease,                                         // Renew leader lease
    // ... additional variants
}
```

### 4.4 Tick System

Both Peer and Store FSMs are driven by periodic ticks:

**PeerTick** (msg.rs):

| Tick | Purpose | Default Interval |
|------|---------|-----------------|
| `Raft` | Heartbeat and election timeout | `raft_base_tick_interval` (100ms) |
| `RaftLogGc` | Log garbage collection | 10s |
| `SplitRegionCheck` | Check region size for split | 10s |
| `PdHeartbeat` | Report region info to PD | 60s |
| `CheckMerge` | Check merge proposal status | 2s |
| `CheckPeerStaleState` | Detect stale leadership | 5min |
| `EntryCacheEvict` | Evict cold entries from cache | 1min |
| `CheckLeaderLease` | Validate leader lease | [Inferred] on demand |
| `CheckLongUncommitted` | Monitor stuck proposals | [Inferred] periodic |

**StoreTick**:

| Tick | Purpose |
|------|---------|
| `PdStoreHeartbeat` | Report store-level stats to PD |
| `SnapGc` | Clean up stale snapshot files |
| `CompactLockCf` | Compact CF_LOCK periodically |
| `ConsistencyCheck` | Schedule consistency verification |
| `CleanupImportSst` | Clean up imported SST files |

### 4.5 PeerFsm Message Dispatch

When `PeerFsm` is polled, all pending messages are dispatched sequentially (fsm/peer.rs `handle_msgs`):

```rust
fn handle_msgs(&mut self, msgs: &mut Vec<PeerMsg<EK>>) {
    for m in msgs.drain(..) {
        match m {
            PeerMsg::RaftMessage(msg, sent_time) => self.on_raft_message(msg),
            PeerMsg::RaftCommand(cmd) => {
                self.propose_raft_command(cmd.request, cmd.callback, cmd.disk_full_opt)
            }
            PeerMsg::Tick(tick) => self.on_tick(tick),
            PeerMsg::ApplyRes(res) => self.on_apply_res(*res),
            PeerMsg::SignificantMsg(msg) => self.on_significant_msg(*msg),
            PeerMsg::CasualMessage(msg) => self.on_casual_msg(*msg),
            PeerMsg::Start => self.start(),
            PeerMsg::Persisted { peer_id, ready_number } => {
                self.on_persisted_msg(peer_id, ready_number)
            }
            PeerMsg::Destroy(peer_id) => self.maybe_destroy(peer_id),
            // ...
        }
    }
}
```

After message handling, the poll handler checks `has_ready` and drives the Raft state machine through `handle_raft_ready_append()` if there are pending state changes.

### 4.6 Apply FSM

The **Apply FSM** is a separate FSM that handles applying committed Raft entries to the KV engine. It runs on its own thread pool, separate from the Raft poll threads.

**Why separate**: Applying entries (writing to RocksDB) is I/O-bound and would block Raft ticking and message processing if done inline.

**Communication flow**:

```
PeerFsm                              ApplyFsm
   │                                    │
   │── ApplyTask(committed_entries) ──→│
   │                                    │── write to KV engine
   │                                    │── execute admin commands
   │←── ApplyRes(exec_results) ────────│
   │                                    │
   │   update metadata, respond to      │
   │   client callbacks                 │
```

**Apply results** (`ExecResult` enum, apply.rs):

```rust
pub enum ExecResult<S> {
    ChangePeer(ChangePeer),
    CompactLog { state, first_index, has_pending },
    SplitRegion { regions, derived, new_split_regions, share_source_region_size },
    PrepareMerge { region, state: MergeState },
    CommitMerge { index, region, source },
    RollbackMerge { region, commit },
    ComputeHash { region, index, context, snap },
    VerifyHash { index, context, hash },
    DeleteRange { ranges },
    IngestSst { ssts },
    TransferLeader { term },
    Flashback { region },
    BatchSwitchWitness(SwitchWitness),
}
```

[Inferred] The apply FSM uses a `WaitMergeSource(Arc<AtomicU64>)` return value to pause CommitMerge processing until the source region has fully applied its logs.

## 5. Snapshot Generation and Application

### 5.1 Snapshot State Machine

```rust
pub enum SnapState {
    Relax,                              // No snapshot activity
    Generating {                        // Async generation in progress
        canceled: Arc<AtomicBool>,      // Cancellation flag
        index: Arc<AtomicU64>,          // Target index
        receiver: Receiver<Snapshot>,   // Result channel
    },
    Applying(Arc<AtomicUsize>),         // Being applied by this peer
    ApplyAborted,                       // Application failed
}
```

### 5.2 Snapshot Data Format

**Constants** (snap.rs):

```rust
pub const SNAPSHOT_VERSION: u64 = 2;          // Raftstore v1
pub const TABLET_SNAPSHOT_VERSION: u64 = 3;   // Raftstore v2
pub const SNAPSHOT_CFS: &[CfName] = &[CF_DEFAULT, CF_LOCK, CF_WRITE];  // CF_RAFT excluded
```

**Protobuf definition** (raft_serverpb.proto):

```protobuf
message RaftSnapshotData {
    Region region = 1;                      // Region metadata at snapshot point
    uint64 version = 2;                     // Snapshot format version
    SnapshotMeta meta = 3;                  // File checksums and metadata
    repeated SnapshotCfFile cf_files = 4;   // Per-CF SST files
}
```

**Snapshot identification**:

```rust
pub struct SnapKey {
    pub region_id: u64,
    pub term: u64,
    pub idx: u64,    // Applied index at snapshot point
}
```

### 5.3 Generation Flow

```
1. Leader's raft-rs detects follower in Snapshot state (too far behind)
   └→ Calls PeerStorage::snapshot(request_index, to_peer_id)

2. PeerStorage creates GenSnapTask:
   GenSnapTask {
       region_id,
       to_peer: target_peer_id,
       index: Arc<AtomicU64>,       // For cancellation check
       canceled: Arc<AtomicBool>,
   }

3. GenSnapTask sent to snapshot generation worker (async)
   └→ Worker thread processes task:
       a. Take KV engine snapshot at current applied_index
       b. Scan all keys in region range [start_key, end_key)
       c. For each CF in [CF_DEFAULT, CF_LOCK, CF_WRITE]:
          - Write matching KV pairs to SST file
          - Compute checksum
       d. Create RaftSnapshotData with region metadata + SST files

4. Result sent back via receiver channel
   └→ PeerFsm receives CasualMessage::SnapshotGenerated

5. Leader wraps snapshot in eraftpb::Snapshot message
   └→ Sends to follower via Raft transport
```

### 5.4 Transfer Protocol

[Inferred] Snapshots are transferred via chunked streaming:

- Snapshot data split into chunks (IO_LIMITER_CHUNK_SIZE, typically small for flow control)
- Sent via gRPC `raft_client` transport alongside normal Raft messages
- Receiver reassembles chunks, validates checksums
- On completion: sends `SignificantMsg::SnapshotStatus` back to leader

### 5.5 Application Flow

```
1. Follower receives complete snapshot
   └→ Validates:
       - Region matches (region_id, epoch)
       - Snapshot index >= current state (not a regression)
       - Term matches

2. Sets SnapState to Applying
   └→ Apply worker processes snapshot installation:
       a. Validate checksums on all SST files
       b. Clear existing data for this region in KV engine
       c. Ingest SST files into KV engine (atomic bulk load)
       d. Update persisted state atomically:
          - RaftLocalState.hard_state.commit = snapshot_index
          - RaftLocalState.last_index = snapshot_index
          - RaftApplyState.applied_index = snapshot_index
          - RaftApplyState.truncated_state.index = snapshot_index
          - RegionLocalState with snapshot's region metadata

3. Peer resumes as follower, synced with leader at snapshot_index
   └→ Subsequent entries replicated normally via AppendEntries
```

**Raftstore-v2 optimization**: In v2, snapshots use RocksDB checkpoint mechanism instead of SST file export. Checkpoints are near-instant copy-on-write operations, significantly reducing snapshot generation time and I/O.

### 5.6 Snapshot Garbage Collection

Old snapshots are cleaned up periodically by the `GcSnapshotRunner`:
- Triggered by `StoreTick::SnapGc`
- Removes snapshot files that have been successfully applied or are too old
- Coordinates via `CasualMessage::GcSnap` to peer FSMs

## 6. Raftstore-v2 Differences

### 6.1 Architectural Changes

Raftstore-v2 (`components/raftstore-v2/`) is a ground-up redesign with the same Raft semantics but different physical architecture:

| Aspect | Raftstore v1 | Raftstore v2 |
|--------|-------------|-------------|
| **Storage model** | Shared RocksDB instance, all regions in one DB | Per-region tablet (separate RocksDB per region) |
| **Snapshot** | SST file export/import | RocksDB checkpoint (near-instant) |
| **Code organization** | Monolithic `peer.rs` (5000+ lines) | Operation-based modules: `operation/{command,ready,life}.rs` |
| **Apply model** | Separate apply FSM with channels | [Inferred] Integrated apply within peer |
| **Snapshot version** | `SNAPSHOT_VERSION = 2` | `TABLET_SNAPSHOT_VERSION = 3` |

### 6.2 Tablet-based Storage

Each region owns a dedicated RocksDB instance (tablet):

- **Isolation**: Regions don't share compaction, memtable flushing, or write amplification
- **Fast snapshots**: `RocksDB::Checkpoint()` creates a point-in-time snapshot as hardlinks — O(1) I/O
- **Independent lifecycle**: Tablet can be destroyed without affecting other regions
- **Trade-off**: More file descriptors and memory overhead per region

**Tablet registry**: `TabletRegistry<EK>` manages tablet creation, caching, and cleanup.

### 6.3 Operation-based Code Structure

```
components/raftstore-v2/src/
├── raft/
│   ├── peer.rs          # Peer struct (slimmer than v1)
│   └── storage.rs       # Storage trait implementation
├── fsm/
│   ├── peer.rs          # PeerFsm
│   └── store.rs         # StoreFsm
├── operation/
│   ├── command/
│   │   ├── admin/
│   │   │   ├── split.rs       # Region split
│   │   │   ├── merge/         # Region merge (prepare, commit, rollback)
│   │   │   └── conf_change.rs # Membership changes
│   │   └── mod.rs
│   ├── ready/
│   │   └── snapshot.rs   # Snapshot generation/application
│   └── life.rs           # Region creation/destruction
└── lib.rs
```

### 6.4 Merge Implementation (v2)

Raftstore-v2 uses a `MergeContext` with explicit state machine:

```rust
pub enum MergeContext {
    prepare_status: Option<PrepareStatus>,
}

enum PrepareStatus {
    WaitForFence { fence, req, ctx },      // Waiting for applied_index >= fence
    WaitForTrimStatus { .. },               // Waiting for follower trim
    Applied(MergeState),                    // PrepareMerge committed
}
```

The v2 merge handles tablet-level data movement:

```rust
pub fn merge_source_path<EK>(
    registry: &TabletRegistry<EK>,
    source_region_id: u64,
    index: u64,
) -> PathBuf
// Creates isolated tablet copy at commit index for merge
```

## 7. Safety Invariants

### 7.1 Consistency Invariants

**Log safety** (guaranteed by raft-rs):
- If two entries have the same index and term, they contain the same data
- If an entry is committed, all entries before it are committed
- A committed entry will eventually be present in the log of every non-crashed server

**Apply ordering**:
- `applied_index` is monotonically increasing
- Entries are applied in log order (index 1, 2, 3, ...)
- Applied entries must be identical across all replicas of a region
- No entry can be applied unless committed (replicated to majority)

**Epoch linearization**:
- Commands that check epoch: validate BEFORE processing
- Commands that change epoch: update AFTER proposal is committed and applied
- This prevents the "multiple regions covering the same key range" anomaly during splits

### 7.2 Durability Guarantees

**Atomic write semantics** — When persisting Raft state:

1. Create `WriteBatch` for KV engine and `RaftLogBatch` for Raft engine
2. Bundle all state changes (entries, hard state, apply state) into a single batch
3. Write batch atomically (either all succeed or all fail)
4. On recovery: detect and fill gaps from crash-incomplete writes

**Persistence points** (what is durable):

```
RaftLocalState {
    hard_state: HardState { term, vote, commit },  // Raft consensus state
    last_index: u64,                                 // Highest log entry
}

RaftApplyState {
    applied_index: u64,                             // Highest applied entry
    truncated_state: RaftTruncatedState {
        index: u64,                                 // GC watermark
        term: u64,
    },
}
```

**Critical rule**: Hard state (term, vote) MUST be persisted before sending any messages at the new term. Violating this can cause split-brain.

### 7.3 Leader Lease

The leader lease provides local reads without Raft round-trips:

```rust
pub struct Lease {
    bound: Option<Either<Suspect, Valid>>,  // Lease timestamp bound
    max_lease: Duration,                    // Maximum lease duration (default: 9s)
    max_drift: Duration,                    // Clock drift allowance (max_lease / 3)
}

pub enum LeaseState {
    Suspect,    // May be invalid (election just happened, no quorum confirmation)
    Valid,      // Lease proven valid (received quorum heartbeat responses)
    Expired,    // Lease time elapsed
}
```

**Lease semantics**:
- When a leader sends a heartbeat, it starts a lease timer
- When it receives a quorum of heartbeat responses, the lease is marked `Valid`
- Lease duration bounded by `max_lease` (must be < election timeout)
- While lease is `Valid`, leader can serve reads locally without ReadIndex
- After leadership change, lease enters `Suspect` until quorum contact is re-established

**Safety guarantee**: If the lease is valid, no other node can have become leader (they would need to wait for election timeout, which is longer than `max_lease`).

### 7.4 Stale Peer Detection

Multiple mechanisms detect and handle stale peers:

| Mechanism | Timeout | Action |
|-----------|---------|--------|
| `CheckPeerStaleState` tick | 5 min | Report peer down status to PD |
| `abnormal_leader_missing_duration` | 5 min | Alert monitoring |
| `max_leader_missing_duration` | 10 min | Ask PD if this peer is still valid |
| Epoch-based detection | immediate | Peer receives message with higher epoch → self-destroys |

### 7.5 Proposal Tracking

**ProposalQueue** ensures every client request gets a response:

```rust
struct Proposal {
    index: u64,          // Raft log index
    term: u64,           // Raft term when proposed
    cb: Callback,        // Client callback
}
```

**Callback phases**:
1. `proposed_cb`: Fired after proposal added to leader's log
2. `committed_cb`: Fired after entry committed (majority replicated)
3. `set_result`: Fired after entry applied, with the response

**Stale proposal detection**: When a proposal's term doesn't match the current term at apply time, it is rejected (the leader changed, so the proposal may not have been committed).

### 7.6 CmdEpochChecker

Prevents conflicting admin commands from being proposed concurrently:

```rust
pub struct CmdEpochChecker<S: Snapshot> {
    proposed_admin_cmd: VecDeque<ProposedAdminCmd>,  // At most 2 pending
    term: u64,
}
```

**Invariant**: At most two epoch-changing admin commands can be in flight simultaneously (one version-changing and one conf_ver-changing). New proposals are rejected if they would conflict with pending ones.

## 8. Background Workers

The raftstore relies on several background workers for async processing:

| Worker | File | Responsibility |
|--------|------|----------------|
| `SnapGenRunner` | `worker/snap_gen.rs` | Async snapshot generation |
| `RaftlogGcRunner` | `worker/raftlog_gc.rs` | Raft log garbage collection |
| `SplitCheckRunner` | `worker/split_check.rs` | Monitor region size, trigger splits |
| `RegionRunner` | `worker/region.rs` | Region-level operations |
| `CleanupRunner` | `worker/cleanup.rs` | Clean up peer data during destruction |
| `GcSnapshotRunner` | `worker/cleanup_snapshot.rs` | Remove stale snapshot files |
| `CompactRunner` | `worker/compact.rs` | RocksDB manual compaction |
| `PdRunner` | `worker/pd.rs` | PD communication (heartbeats, reporting) |
| `ReadRunner` | `worker/read.rs` | Async read operations |
| `ConsistencyCheckRunner` | `worker/consistency_check.rs` | Hash-based consistency verification |
| `DiskCheckRunner` | `worker/disk_check.rs` | Disk health monitoring |

**Raft log GC** — triggered by `PeerTick::RaftLogGc`:

```
Conditions for compaction:
  - Entry count > raft_log_gc_count_limit (default: 10000 entries)
  - Log size > raft_log_gc_size_limit (default: 200MB for v2)
  - truncated_index + 1 < applied_index (there are entries to compact)

Algorithm:
  1. Compute compact_index = max(applied_index - trailing_count, truncated_index + 1)
  2. Propose CompactLog admin command
  3. On apply: update truncated_state, purge entries from Raft engine
```

## 9. Hibernation

Regions with no activity hibernate to reduce CPU overhead:

```rust
pub enum GroupState {
    Ordered,      // Active, processing messages and ticks
    Chaos,        // Pre-campaign state (randomized election timeout)
    Idle,         // Temporarily idle
    PreChaos,     // Transitioning to Chaos
}
```

**Hibernate flow**:
1. Leader detects no write/read activity for configurable duration
2. Leader sends `MsgCheckQuorum` → followers respond
3. If all followers respond: entire group enters hibernation
4. Tick intervals extended, reducing CPU usage
5. On new request: group awakened, resumes normal ticking
6. `StoreMsg::AwakenRegions` can force-wake specific regions

[Inferred] Hibernation is a major optimization for clusters with many regions but uneven write distribution (common in TiDB workloads where some tables are hot and others cold).

## 10. Key File Reference

| Component | File Path | Key Contents |
|-----------|-----------|--------------|
| **Raft config** | `components/raftstore/src/store/config.rs` | Raft parameters, tick intervals |
| **Peer** | `components/raftstore/src/store/peer.rs` | Core Peer struct wrapping RawNode |
| **Peer FSM** | `components/raftstore/src/store/fsm/peer.rs` | PeerFsm, message dispatch |
| **Store FSM** | `components/raftstore/src/store/fsm/store.rs` | StoreFsm, StoreMeta |
| **Apply FSM** | `components/raftstore/src/store/fsm/apply.rs` | Entry application, admin cmd execution |
| **Peer storage** | `components/raftstore/src/store/peer_storage.rs` | Storage trait impl, SnapState |
| **Entry storage** | `components/raftstore/src/store/entry_storage.rs` | EntryStorage, log caching |
| **Messages** | `components/raftstore/src/store/msg.rs` | PeerMsg, StoreMsg, PeerTick, CasualMessage |
| **Utilities** | `components/raftstore/src/store/util.rs` | Epoch checks, Lease, ConfChangeKind |
| **Region meta** | `components/raftstore/src/store/region_meta.rs` | RegionMeta, GroupState |
| **Snapshots** | `components/raftstore/src/store/snap.rs` | SnapKey, snapshot constants |
| **Snap gen** | `components/raftstore/src/store/worker/snap_gen.rs` | Snapshot generation worker |
| **Split check** | `components/raftstore/src/store/worker/split_check.rs` | Region split trigger logic |
| **Batch system** | `components/batch-system/src/` | Router, FSM scheduling, mailboxes |
| **v2 Peer** | `components/raftstore-v2/src/raft/peer.rs` | Raftstore-v2 Peer |
| **v2 Storage** | `components/raftstore-v2/src/raft/storage.rs` | V2 Storage trait impl |
| **v2 Split** | `components/raftstore-v2/src/operation/command/admin/split.rs` | V2 split |
| **v2 Merge** | `components/raftstore-v2/src/operation/command/admin/merge/` | V2 merge |
| **v2 Snapshot** | `components/raftstore-v2/src/operation/ready/snapshot.rs` | V2 snapshot |
