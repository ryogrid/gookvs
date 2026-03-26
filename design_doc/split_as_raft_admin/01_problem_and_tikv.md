# Split as Raft Admin Command — Problem Analysis and TiKV Reference

## 1. Problem Statement

gookv's region splits execute outside the Raft log. This creates a timing gap between data entry proposals and split execution, causing data integrity failures at high concurrency (32 workers, 1000 accounts: $33-$100 balance divergence).

### Current gookv split flow

```mermaid
sequenceDiagram
    participant SC as StoreCoordinator
    participant Peer as Region A Peer
    participant Raft as Region A Raft Log
    participant PD as PD Server

    Note over SC: SplitCheckWorker detects region too large
    SC->>PD: AskBatchSplit(region, splitKey)
    PD-->>SC: newRegionIDs, newPeerIDs

    SC->>SC: split.ExecBatchSplit() — pure function
    SC->>Peer: peer.UpdateRegion(derived) — IMMEDIATE
    SC->>SC: BootstrapRegion(childRegion) — IMMEDIATE

    Note over Raft: Raft log has NO record of the split!<br/>Data entries before/after split are<br/>indistinguishable in the log.
```

### The timing gap

```mermaid
sequenceDiagram
    participant Client
    participant RPC as RPC Handler
    participant Propose as ProposeModifies
    participant Split as handleSplitCheckResult
    participant Apply as applyEntriesForPeer

    Client->>RPC: KvPrewrite(key=X, epoch=v1)
    RPC->>RPC: validateRegionContext ✓ (epoch=v1)
    RPC->>Propose: ProposeModifies(regionA, modifies, epoch=v1)
    Propose->>Propose: epoch check ✓ (still v1)
    Propose->>Propose: rawNode.Propose(data)

    Note over Split: Split executes NOW (outside Raft)<br/>epoch v1→v2, key X moves to Region B

    Propose-->>RPC: nil (success)
    RPC-->>Client: Prewrite OK

    Note over Apply: Entry from Raft log applied<br/>But epoch is now v2, key X out of range<br/>→ lock written to shared engine (no filter)<br/>→ OR filtered (if filter enabled) → lock lost
```

Both outcomes are problematic:
- **Without apply filter**: lock is written but belongs to wrong region logically
- **With apply filter**: lock is silently dropped, prewrite reports success but lock is lost

### Evidence from demo runs

| Scale | Total Balance | Discrepancy |
|-------|-------------|-------------|
| 8w/100a | $10,000 (3/3 PASS) | 0 |
| 16w/500a | $50,000 (3/3 PASS) | 0 |
| 32w/1000a | $99,967 (FAIL) | −$33 (1 account) |

The discrepancy increases with scale because more splits occur during active transactions.

## 2. TiKV's Solution: Split as Raft Admin Command

### TiKV split flow

```mermaid
sequenceDiagram
    participant PD as PD Worker
    participant Peer as Peer FSM
    participant Raft as Raft Log
    participant Apply as Apply Delegate

    PD->>Peer: send_admin_request(BatchSplit)
    Peer->>Raft: rawNode.Propose(AdminCmd)
    Note over Raft: Split enters Raft log<br/>at index N

    Raft->>Apply: Entry N-2: data write (pre-split)
    Apply->>Apply: self.region = old range → apply ✓

    Raft->>Apply: Entry N-1: data write (pre-split)
    Apply->>Apply: self.region = old range → apply ✓

    Raft->>Apply: Entry N: BatchSplit admin cmd
    Apply->>Apply: exec_batch_split()
    Apply->>Apply: self.region = NEW range (atomic)

    Raft->>Apply: Entry N+1: data write (post-split)
    Apply->>Apply: self.region = new range → validate key ✓
```

### Key TiKV source references

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| Split proposal creation | `tikv/components/raftstore/src/store/worker/pd.rs` | 2630-2651 | `new_batch_split_region_request()` |
| Propose to Raft | `tikv/components/raftstore/src/store/peer.rs` | 4763-4784 | `propose_normal()` serializes and proposes |
| Apply exec_batch_split | `tikv/components/raftstore/src/store/fsm/apply.rs` | 2632-2841 | Split execution during Raft apply |
| Region update in delegate | `tikv/components/raftstore/src/store/fsm/apply.rs` | 1641 | `self.region = derived.clone()` |
| Child peer creation | `tikv/components/raftstore/src/store/fsm/peer.rs` | 4643-4894 | `on_ready_split_region()` |
| Key range validation | `tikv/components/raftstore/src/store/fsm/apply.rs` | 1897-1902 | `check_key_in_region()` per entry |

### Why this eliminates the timing gap

1. **Strict ordering**: The split admin command has a specific index in the Raft log. All entries before it are applied with pre-split metadata; all entries after with post-split metadata.
2. **Atomic region update**: The apply delegate's `self.region` is updated when the split command is applied — not asynchronously by a coordinator.
3. **No race condition**: Since both data writes and splits go through the same Raft log, there is no window where a data write can be proposed with one epoch and applied with another.

## 3. Architecture Gap Summary

```mermaid
flowchart TD
    subgraph TiKV["TiKV: Split in Raft Log"]
        T1["Entry 100: KvPrewrite(key=X)"] --> T2["Entry 101: KvPrewrite(key=Y)"]
        T2 --> T3["Entry 102: BatchSplit admin cmd<br/>(epoch v1→v2, range changes)"]
        T3 --> T4["Entry 103: KvPrewrite(key=Z)"]
        T3 -->|"Apply updates self.region"| T5["Region metadata = v2"]
        T4 -->|"Validated against v2"| T5
    end

    subgraph gookv["gookv: Split outside Raft"]
        G1["Entry 100: KvPrewrite(key=X)"] --> G2["Entry 101: KvPrewrite(key=Y)"]
        G2 --> G3["Entry 102: KvPrewrite(key=Z)"]
        G_SPLIT["handleSplitCheckResult()<br/>(epoch v1→v2)"] -.->|"Asynchronous,<br/>no log ordering"| G2
    end

    style T3 fill:#dfd,stroke:#0a0
    style G_SPLIT fill:#fdd,stroke:#a00
```
