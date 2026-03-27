# Code Review: Raft Layer

## Summary

The Raft layer implements a goroutine-per-peer architecture using etcd/raft's RawNode. The code is generally well-structured with clear separation of concerns: `peer.go` handles the event loop, `storage.go` implements `raft.Storage`, `split_admin.go` / `conf_change.go` handle admin commands, and `snapshot.go` handles snapshot generation and application. The router is a clean sync.Map-based message dispatcher.

However, there are several critical issues around data consistency that could cause data loss or corruption in production, alongside a number of high-severity bugs related to recovery, concurrency, and protocol correctness.

---

## Critical Issues

### C1. RecoverFromEngine does not restore ApplyState

- **File**: `internal/raftstore/storage.go:296`
- **Severity**: Critical
- **Description**: `RecoverFromEngine()` restores `hardState` and rebuilds the entry cache by scanning Raft log entries, but it never reads the persisted `ApplyState` from the engine. The `ApplyState` (containing `AppliedIndex`, `TruncatedIndex`, `TruncatedTerm`) remains at the default values set in `NewPeerStorage()` (all equal to `RaftInitLogIndex = 5`). After restart, the peer believes its truncated index is 5 regardless of how far it actually compacted. This causes:
  - `FirstIndex()` returns a wrong value (`TruncatedIndex + 1 = 6`), which may be lower than what was actually compacted. Raft will request entries that no longer exist on disk, returning `ErrUnavailable` or corrupt data.
  - `AppliedIndex` is reset to 5, causing all entries after index 5 to be re-applied to the state machine (double application). For a transactional KV store, this corrupts MVCC state.
  - `Term()` for the truncated index returns wrong term, potentially confusing leader election.
- **Impact**: Data corruption on restart. Re-application of committed entries to the state machine. Raft log reads returning wrong results.
- **Suggested fix**: Add `ApplyState` recovery to `RecoverFromEngine()`. Read `keys.ApplyStateKey(s.regionID)` from the engine, unmarshal the 24-byte format used in `PersistApplyState()`, and set `s.applyState` accordingly.

### C2. ApplyState is never persisted during normal operation

- **File**: `internal/raftstore/storage.go:401` and `internal/raftstore/peer.go:599`
- **Severity**: Critical
- **Description**: `PersistApplyState()` exists but is never called anywhere in the production code. In `handleReady()` (peer.go:599), `SetAppliedIndex()` updates the in-memory `applyState.AppliedIndex` but does not persist it. Similarly, `execCompactLog()` (raftlog_gc.go:15) mutates `applyState.TruncatedIndex` and `TruncatedTerm` in memory only. The `SaveReady()` method persists entries and hard state but not the apply state.
- **Impact**: On crash and restart, all applied entries are lost from tracking. Combined with C1, this means the system cannot recover its apply progress. The `AppliedIndex` resets to default, causing full re-application of the log.
- **Suggested fix**: Call `PersistApplyState()` after updating the applied index in `handleReady()`, or batch it into the `SaveReady()` write batch. Also persist after `execCompactLog()` updates the truncated state.

### C3. InitialState returns empty ConfState, breaking multi-node restart

- **File**: `internal/raftstore/storage.go:68`
- **Severity**: Critical
- **Description**: `InitialState()` always returns an empty `raftpb.ConfState{}`. After a restart of a multi-node cluster, etcd/raft uses the ConfState from `InitialState()` to know which peers are in the group. Returning empty ConfState means the Raft node has no knowledge of its peers (no voters, no learners). This prevents the node from participating in elections or replicating logs after restart.
- **Impact**: Multi-node clusters cannot recover after a full restart. A single-node cluster works only because it bootstraps fresh.
- **Suggested fix**: Persist the ConfState (either separately or derive it from the region's peer list stored in region metadata). On restart, reconstruct the ConfState from the region's peer list and return it from `InitialState()`.

### C4. Snapshot application passes nil start/end keys, fails to clear old region data

- **File**: `internal/raftstore/snapshot.go:384`
- **Severity**: Critical
- **Description**: When `ApplySnapshot()` calls `ApplySnapshotData(s.engine, sd, nil, nil)`, it passes `nil` for both `startKey` and `endKey`. In `ApplySnapshotData()` (line 269), the `DeleteRange` that clears old data is gated by `if len(startKey) > 0 || len(endKey) > 0`, so with both nil, **no old data is deleted**. The snapshot data is then written on top of whatever stale data exists. For a follower catching up after a split, this means the old region's full key range data remains, and the snapshot only overwrites the keys it contains. Any keys deleted by the leader since the follower fell behind will remain as stale data on the follower.
- **Impact**: Data inconsistency between leader and follower after snapshot application. Stale keys persist on followers.
- **Suggested fix**: Pass the region's `startKey`/`endKey` from `s.region` to `ApplySnapshotData()`. The `PeerStorage` already has a `region` field. Alternatively, embed the key range in the `SnapshotData` itself.

### C5. Proposal callback index tracking is unreliable

- **File**: `internal/raftstore/peer.go:515-521`
- **Severity**: High
- **Description**: In `propose()`, the expected index for the proposal callback is computed as `lastIdx + 1` from storage. However, `lastIdx` reflects the last entry in the local cache/engine, not the actual index Raft will assign. If multiple proposals are batched before `handleReady()` persists them, the second proposal also computes `lastIdx + 1` (same value as the first), overwriting the first callback in `pendingProposals`. Additionally, leader changes or term changes can cause Raft to assign different indices than expected.
- **Impact**: Proposal callbacks may be invoked for the wrong proposal or lost entirely. In a transactional KV store, this means clients may receive success/failure notifications for the wrong operation.
- **Suggested fix**: Track proposals by a unique proposal ID embedded in the entry data rather than by predicted index. Alternatively, increment a local counter that accurately tracks the next expected index (noting that `rawNode.Propose()` may internally assign indices).

---

## High Issues

### H1. PeerMsgTypeDestroy closes Mailbox channel causing panic on concurrent sends

- **File**: `internal/raftstore/peer.go:425-428`
- **Severity**: High
- **Description**: When `PeerMsgTypeDestroy` is received, the handler calls `close(p.Mailbox)`. However, the `Mailbox` channel is shared with external senders (the router, gRPC handlers, other peers via `sendFunc`). Any concurrent send to a closed channel panics in Go. The comment says "Drain mailbox" but `close()` doesn't drain -- it just marks the channel as closed. The `Run()` loop does handle the `ok == false` case from reads, but concurrent writes from other goroutines to the now-closed channel will cause runtime panics.
- **Impact**: Runtime panics that crash the entire process during peer destruction.
- **Suggested fix**: Do not close the Mailbox channel from the receiver side. Instead, set `p.stopped = true` and have the router unregister the peer first (which stops new sends), then let the `Run()` goroutine exit naturally via context cancellation. Or use a dedicated `stopCh` channel and never close the `Mailbox` from the handler.

### H2. Leader lease accessed from multiple goroutines without synchronization

- **File**: `internal/raftstore/peer.go:131-132, 268-277, 538-540, 606-608`
- **Severity**: High
- **Description**: `leaseValid` is an `atomic.Bool`, which is thread-safe. However, `leaseExpiry` is a plain `time.Time` value with no synchronization. `IsLeaseValid()` (called from gRPC handler goroutines) reads `leaseExpiry` concurrently with `handleReady()` (the peer goroutine) writing it. This is a data race.
- **Impact**: Data race on `leaseExpiry`. Could lead to stale reads being served based on a corrupt/half-written timestamp, or cause panics depending on the Go runtime's handling of concurrent `time.Time` access.
- **Suggested fix**: Protect `leaseExpiry` with a mutex, or store it as an `atomic.Int64` (Unix nanoseconds). Alternatively, only access `leaseExpiry` from within the peer goroutine and expose `IsLeaseValid()` via a message to the mailbox.

### H3. handleReady does not check for stopped state before processing

- **File**: `internal/raftstore/peer.go:376-378`
- **Severity**: High
- **Description**: After the drain loop in `Run()`, `handleReady()` is called unconditionally outside the `select` statement. If a `PeerMsgTypeDestroy` message was processed during the drain, `p.stopped` is true and the `Mailbox` is closed, but `handleReady()` still runs. This processes Raft state after the peer is logically destroyed, potentially sending messages, applying entries, or updating state for a dead peer.
- **Impact**: Stale Raft messages sent after peer destruction. State machine mutations for a destroyed peer. Potential panics if the peer's resources have been partially cleaned up.
- **Suggested fix**: Add `if p.stopped.Load() { return }` at the top of `handleReady()`, or check `p.stopped` before calling `handleReady()` in the `Run()` loop.

### H4. Committed entries are applied twice: once inline and once via applyFunc

- **File**: `internal/raftstore/peer.go:579-600`
- **Severity**: High
- **Description**: In `handleReady()`, committed entries are processed in two passes: first, admin commands (`ConfChange`, `SplitAdmin`) are applied inline (lines 579-586). Then, ALL committed entries (including admin entries) are sent to `applyFunc` (line 589). The `applyFunc` (wired in coordinator.go) calls `applyEntriesForPeer()`, which attempts to unmarshal all entries as `RaftCmdRequest`. While admin entries happen to fail unmarshal and get skipped, this relies on the implicit behavior that admin entry binary format is incompatible with protobuf `RaftCmdRequest` format. A future change to the admin entry format could accidentally make it parseable as a valid `RaftCmdRequest`, leading to double application.
- **Impact**: Currently benign (admin entries fail protobuf unmarshal). However, fragile -- relies on binary format incompatibility as a correctness guarantee rather than explicit filtering.
- **Suggested fix**: Filter admin entries before passing to `applyFunc`. Only send `EntryNormal` entries that are not `IsSplitAdmin()` and not CompactLog (tag 0x01) to the apply worker.

### H5. Split admin applied only on leader, not deterministically on all replicas

- **File**: `internal/raftstore/peer.go:579-586` and `internal/raftstore/split_admin.go:189`
- **Severity**: High
- **Description**: In `handleReady()`, `applySplitAdminEntry()` is called for all peers that process the committed entry. However, inside `applySplitAdminEntry()` (split_admin.go:189), the `splitResultCh` notification is only sent when `p.isLeader.Load()` is true. The actual region metadata update via `ExecSplitAdmin` -> `peer.UpdateRegion()` is executed on all replicas, which is correct. But the `ExecSplitAdmin` function calls `split.ExecBatchSplit()` which modifies the region epoch -- and this happens inside the peer goroutine's `handleReady()`. This is correct for deterministic application.

  However, there is a subtlety: `ExecSplitAdmin` calls `peer.Region()` which acquires `regionMu.RLock()`, while `peer.UpdateRegion()` acquires `regionMu.Lock()`. Between these two calls, a concurrent gRPC handler could read the old region, then receive the updated region from another source, creating a brief inconsistency window. More importantly, the split region metadata is applied to the parent peer, but the `handleReady()` then passes all committed entries (including the split admin entry) to `applyFunc()` for state machine application. The `applyFunc` still sees the old region metadata (from before `UpdateRegion` was called? or after?). This depends on timing.

  *Upon re-reading*: `UpdateRegion` is called in `ExecSplitAdmin` (line 152), which is called from `applySplitAdminEntry`, which runs before `applyFunc` in the loop (lines 579-589). So `applyFunc` sees the updated region. But the `applyFunc`'s epoch-aware filtering in `applyEntriesForPeer` may then incorrectly filter entries that were proposed with the old epoch. This is a design concern, not an immediate bug, because the epoch check in the apply function would use the new (post-split) epoch, and entries from before the split would have the old epoch.
- **Impact**: After a split, entries in the same `handleReady` batch that were committed before the split admin entry but appear in the same `CommittedEntries` slice will be filtered by the epoch-aware apply function using the post-split epoch. These entries could be silently dropped.
- **Suggested fix**: Process entries in order: for each committed entry, first check if it is an admin entry and apply it, then apply the data entry (or skip it). Do not batch all admin entries first. Alternatively, snapshot the region epoch before processing admin entries and use the pre-admin epoch for epoch-aware filtering of data entries that precede the admin entry in the log.

---

## Medium Issues

### M1. readEntriesFromEngine silently stops on ErrNotFound gap

- **File**: `internal/raftstore/storage.go:481-484`
- **Severity**: Medium
- **Description**: When reading entries from the engine, if an entry at index `idx` returns `ErrNotFound`, the loop breaks and returns whatever entries have been read so far. This silently truncates the result. If there's a gap in the persisted log (e.g., due to a bug or partial write), entries after the gap are lost without any error indication. The Raft library may interpret this as "these are all the entries" and proceed with incomplete data.
- **Impact**: Silent data loss if a gap exists in the persisted Raft log. The Raft library could make decisions based on incomplete log data.
- **Suggested fix**: Return an error when a gap is detected (when `idx > lo` and we get `ErrNotFound`). Only treat `ErrNotFound` for `idx == lo` as `ErrCompacted`.

### M2. onRaftLogGCTick can underflow when appliedIdx equals 1

- **File**: `internal/raftstore/peer.go:696`
- **Severity**: Medium
- **Description**: `compactIdx = appliedIdx - 1` can underflow to `math.MaxUint64` if `appliedIdx` is 0. While `appliedIdx <= firstIdx` is checked earlier (returning early if true), with the default `RaftInitLogIndex = 5`, this is unlikely in practice. But if initialization produces `appliedIdx = 0` (e.g., during bootstrap path where `ApplyState` is set to all zeros at peer.go:170-174), and `firstIdx = 1`, the check `appliedIdx <= firstIdx` is true and we return early. Safe for now, but fragile.
- **Impact**: Potential `uint64` underflow leading to enormous compact index, though current initialization prevents it.
- **Suggested fix**: Add an explicit guard: `if appliedIdx < 2 { return }`.

### M3. Snapshot ConfState not set in generated snapshot metadata

- **File**: `internal/raftstore/snapshot.go:353-359`
- **Severity**: Medium
- **Description**: When `generateSnapshot()` creates the snapshot metadata, it only sets `Index` and `Term` but not `ConfState`. The `ConfState` in the snapshot metadata tells the receiving node what the cluster configuration was at the time of the snapshot. Without it, the receiver doesn't know which peers are in the group after applying the snapshot.
- **Impact**: Followers receiving this snapshot may not correctly configure their peer membership, potentially leading to election failures or split-brain scenarios.
- **Suggested fix**: Include `ConfState` (derived from the region's peer list) in the snapshot metadata. The `GenSnapTask` already carries a `Region` field with peer information.

### M4. scanRegionSize accumulates sizes across CFs but picks split key from first CF only

- **File**: `internal/raftstore/split/checker.go:140-191`
- **Severity**: Medium
- **Description**: `scanRegionSize` iterates CFs sequentially (CFDefault, CFWrite, CFLock). The `totalSize` accumulates across all CFs. However, the split key selection (`if splitKey == nil && totalSize >= halfSize`) only triggers once. If the first CF (CFDefault) is small, the split key will be chosen from CFWrite or CFLock at whatever key happens to push total size past the halfway mark. This could pick a split key from a CF that has different key distributions than the primary data CF, leading to unbalanced splits.
- **Impact**: Suboptimal split point selection. Not a correctness issue but can lead to performance problems with skewed region sizes after split.
- **Suggested fix**: Calculate size per-CF first, then select the split key from the dominant CF (typically CFWrite for a TiKV-compatible store), or compute the split key based on CFDefault key distribution specifically.

### M5. ExecCommitMerge has dead code and fragile fallback logic

- **File**: `internal/raftstore/merge.go:118-140`
- **Severity**: Medium
- **Description**: The merge direction detection has a confusing condition at lines 118-121 that checks for right merge but then has a comment "This shouldn't happen for right merge" and an empty block. The fallback at lines 130-140 tries to extend the range to cover both regions but has complex logic that may produce incorrect results for edge cases (e.g., when `source.EndKey` is nil/empty, meaning unbounded).
- **Impact**: Incorrect region key range after merge in edge cases. Could lead to key range gaps or overlaps between regions.
- **Suggested fix**: Remove the dead code block at lines 118-121. Ensure the right-merge and left-merge cases are exhaustive and return an error for non-adjacent regions rather than attempting a fallback.

### M6. Multiple proposals in flight can produce stale callbacks

- **File**: `internal/raftstore/peer.go:493-521`
- **Severity**: Medium
- **Description**: The `propose()` function wraps the original callback in a closure that ignores the entry data and error, always calling `cmd.Callback(nil)`. This means even if the entry data is different from what was proposed (e.g., due to index collision with another proposal), the callback still reports success. Combined with C5 (unreliable index tracking), this could notify the wrong client of success.
- **Impact**: Client receives success notification for a different operation's commit.
- **Suggested fix**: Either verify the entry data matches the proposal before invoking the callback, or use a unique proposal ID scheme.

---

## Potential Issues

### P1. Race between IsLeaseValid (gRPC goroutine) and leaseExpiry writes (peer goroutine)

Already covered as H2 above. Flagging separately because `IsLeaseValid()` at `peer.go:268` is explicitly documented as "Thread-safe: uses atomic bool" but the `time.Time` it reads is not atomic.

### P2. SplitCheckTask captures region pointer without protection

- **File**: `internal/raftstore/peer.go:737-738`
- **Severity**: Low/Potential
- **Description**: `onSplitCheckTick()` reads `p.region` directly (without `regionMu`) to construct the `SplitCheckTask`. The `Region` and key fields are pointer/slice values that could be mutated concurrently by `UpdateRegion()` from `applySplitAdminEntry()`.
- **Impact**: The split check worker could see a partially-updated region, potentially checking the wrong key range.

### P3. pendingReads map iteration with deletion during map ranging

- **File**: `internal/raftstore/peer.go:546-550, 626-631`
- **Severity**: Low/Potential
- **Description**: Deleting keys from a map while iterating over it with `range` is safe in Go, but the pattern appears in two places. This is a Go idiom that is correct but worth calling out for maintainability.

### P4. Snapshot result channel drop silently loses snapshots

- **File**: `internal/raftstore/snapshot.go:323-326`
- **Severity**: Low/Potential
- **Description**: In `SnapWorker.Run()`, if `task.ResultCh` send fails (channel full), the result is silently dropped. The `PeerStorage` will remain in `SnapStateGenerating` until the next `Snapshot()` call tries to read from the channel. Since the result was dropped, the receiver will block forever on `snapReceiver` (or get nothing from the non-blocking read).
- **Impact**: The `ResultCh` is a buffered channel of size 1, and there's only one sender per task, so in practice this shouldn't happen. But if it did, the snapshot state machine would be stuck.

### P5. `close(p.Mailbox)` in Destroy handler races with `for range` in Run()

Already covered as H1. The `for i := 0; i < 63; i++` drain loop at lines 363-374 reads from the same channel that `PeerMsgTypeDestroy` closes. If destroy is processed during a drain iteration, the next read at line 365 would return `ok == false` and exit cleanly. But any external goroutine sending to the channel after close will panic.

---

## Observations

### O1. Duplicate SplitRegionResult type

`SplitRegionResult` is defined in both `internal/raftstore/msg.go:96` and `internal/raftstore/split/checker.go:196`. They have identical fields but are distinct types. This requires conversion when passing results between packages and is confusing.

### O2. PersistApplyState uses manual byte serialization

- **File**: `internal/raftstore/storage.go:406-434`
- The manual big-endian serialization is correct but verbose. Consider using `binary.BigEndian.PutUint64` consistently (the function already does this correctly) or `encoding/binary.Write` for cleaner code. Not a bug, just style.

### O3. cloneRegion duplicated across files

`cloneRegion()` in `conf_change.go:169` and `cloneMergeRegion()` in `merge.go:182` and `cloneRegionForSplit()` in `split/checker.go:280` are identical functions. Consider consolidating into a single shared utility.

### O4. CompactLog not applied in handleReady committed entries loop

The `handleReady()` first loop (lines 579-586) processes `ConfChange` and `SplitAdmin` entries but does not process CompactLog entries (tag 0x01). CompactLog is handled via the `onApplyResult` path (peer.go:646). This means CompactLog application follows a different path than other admin commands, which is architecturally inconsistent but functionally correct since CompactLog only updates metadata and doesn't need to be applied synchronously with the commit.

### O5. No stale proposal cleanup

`pendingProposals` is only cleaned up when matching committed entries arrive (peer.go:592-596). If a proposal is never committed (e.g., leader change, the entry gets overwritten), the callback remains in the map forever. There is no timeout or cleanup mechanism for stale proposals.

### O6. Router stores raw channels, no lifecycle management

The `Router` in `router/router.go` stores `chan raftstore.PeerMsg` values but has no mechanism to close them or detect dead peers. `Unregister()` only removes the mapping. If a peer goroutine dies without being unregistered, the channel remains referenced in the `sync.Map`, preventing garbage collection.
