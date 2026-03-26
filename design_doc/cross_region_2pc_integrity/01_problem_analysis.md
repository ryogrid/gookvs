# Cross-Region 2PC Integrity: Problem Analysis

## 1. Problem Statement

The transaction integrity demo shows $50-$100 balance divergence with 32 workers and 3+ regions. All prior fixes (ReadIndex, epoch validation at RPC level, apply-level key filtering, per-key commit routing) have been applied. The replayed total from committed transfers is always $100,000, but actual account balances differ.

### Demo evidence

```
DISCREPANCY acct:0274: replayed=$147 actual=$100 diff=-47
DISCREPANCY acct:0892: replayed=$53  actual=$53  diff=0
```

Transfer credits acct:0274 by $47 (to $147), but actual balance is $100 (initial value). The credit was committed (Raft proposal succeeded) but the write was either filtered at apply time or proposed to the wrong region.

## 2. Root Cause

### 2.1 The timing gap between RPC validation and Raft proposal

```mermaid
sequenceDiagram
    participant Client
    participant Server as Store (RPC Handler)
    participant Raft as Raft Propose
    participant Split as Split Handler

    Client->>Server: KvPrewrite(key=X, regionId=A, epoch=v1)
    Server->>Server: validateRegionContext ✓<br/>epoch v1 matches, key X in range

    Note over Split: Region A splits!<br/>epoch v1 → v2<br/>key X now in Region B

    Server->>Server: PrewriteModifies → compute lock
    Server->>Raft: ProposeModifies(regionId=A, lock(X))
    Note over Raft: NO epoch check at propose time!<br/>Proposal accepted with stale epoch

    Raft->>Raft: Apply entry
    Note over Raft: applyEntriesForPeer filters<br/>key X (out of Region A range)<br/>→ lock NOT written
```

The split occurs BETWEEN `validateRegionContext` (which passes) and `ProposeModifies` (which has no epoch check). The entry enters the Raft log for Region A, but the apply-level filter correctly drops it because key X is no longer in Region A's range.

### 2.2 Why the client doesn't recover

```mermaid
flowchart TD
    A["Client: KvPrewrite succeeds<br/>(server returned no error)"] --> B["Client: KvCommit for key X"]
    B --> C{"Server: CommitModifies<br/>reads lock from engine"}
    C -->|"Lock found<br/>(another region applied it)"| D["Commit succeeds ✓"]
    C -->|"Lock NOT found<br/>(filtered at apply)"| E["TxnLockNotFound error"]
    E --> F{"Client: retry count"}
    F -->|"< 5 retries"| G["Retry via SendToRegion<br/>(region cache refresh)"]
    F -->|">= 5 retries"| H["Accept as 'resolved'<br/>→ DATA LOSS"]
    G --> C

    style H fill:#fdd,stroke:#a00
```

After 5 `TxnLockNotFound` retries, the client accepts the error as "lock already resolved by another transaction." But the lock was never written — it was filtered at apply. The primary commit succeeds (different key, different region), but the secondary is lost.

### 2.3 The follower divergence problem

All regions on a store share ONE RocksDB engine. The latch is also store-wide. So:

- Two prewrites for the same key on the same store ARE serialized (latch prevents concurrency)
- The engine snapshot DOES see all regions' data (no per-region isolation)
- Conflict detection works correctly within a single store

The issue is on **follower stores** that apply entries after the region metadata has been updated by the split:

```mermaid
sequenceDiagram
    participant Leader as Store 1 (Region A leader)
    participant Follower as Store 2 (Region A follower)

    Note over Leader,Follower: Region A: ["" .. ∞), epoch v1

    Leader->>Leader: TxnA prewrites key X (X > "m")<br/>→ Raft propose (epoch v1)
    Leader->>Leader: Apply: write lock(X) to engine ✓

    Note over Leader,Follower: Split! Region A: ["" .. "m"), epoch v2<br/>Region B: ["m" .. ∞)

    Leader->>Follower: Raft replicates entry (epoch v1)
    Note over Follower: Follower's Region A metadata<br/>already updated to epoch v2, range ["".."m")
    Follower->>Follower: applyEntriesForPeer:<br/>peer.Region() = ["".."m") (post-split)<br/>key X > "m" → FILTERED
    Note over Follower: Lock NOT written on follower!<br/>Follower diverges from leader
```

The entry was proposed and applied on the leader BEFORE the split — correctly. But the follower receives the entry AFTER its region metadata was updated by the split handler. The apply-level filter uses post-split metadata (`peer.Region()`) and incorrectly rejects the pre-split entry.

**Note:** `applyEntriesForPeer` only controls whether to call `ApplyModifies` for new entries from `rd.CommittedEntries`. It does NOT delete data that was already written. The issue is that the follower never writes the data in the first place.

## 3. Root Cause Summary

Two interacting bugs:

1. **Apply-level filter uses wrong region metadata** — `peer.Region()` returns post-split range for pre-split entries on followers. The filter should use the epoch from the time of proposal, not the current epoch.

2. **No propose-time epoch check** — stale proposals can enter the Raft log even after the region's epoch has changed. TiKV's `CmdEpochChecker` rejects these before they reach the log.

## 4. The TiKV Solution

TiKV handles this correctly because:

1. **Splits are Raft admin commands** — ordered in the Raft log alongside data entries. Entries before the split admin command are applied with pre-split metadata; entries after are applied with post-split metadata.
2. **Apply delegate maintains its own region copy** (`self.region`) — updated atomically when the split admin command is applied. Pre-split entries see the pre-split range.
3. **Propose-time epoch check** — `CmdEpochChecker` rejects proposals whose epoch conflicts with pending admin commands.

gookv's splits execute outside Raft (via `handleSplitCheckResult` in the coordinator). There is no ordering between split execution and data entry apply. The fix must compensate for this architectural difference by embedding the epoch in each proposal (see `03_design.md`).
