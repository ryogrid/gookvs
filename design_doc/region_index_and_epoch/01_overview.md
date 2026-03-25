# Read Index and Region Epoch: Architecture Overview

## 1. Problem Statement

gookv's transaction integrity demo fails under concurrent cross-region workloads because of two missing safety mechanisms:

1. **Reads bypass Raft**: `KvGet`, `KvScan`, `KvBatchGet` read directly from the Pebble engine without confirming that the server is the current leader or that all committed writes have been applied. This violates linearizability.

2. **Region epoch is not validated**: After a region split, RPCs carrying stale region metadata (old epoch) are not rejected. This allows writes to be proposed to the wrong region and reads to return data from an outdated key range.

### Failure Scenario

```mermaid
sequenceDiagram
    participant TxnA as Transaction A
    participant TxnB as Transaction B
    participant Node1 as Node 1 (Region A leader)
    participant Node2 as Node 2 (Region B leader)

    Note over TxnA: Transfer $20: acct:0100 → acct:0500
    TxnA->>Node1: KvPrewrite(acct:0100) [Region A]
    TxnA->>Node2: KvPrewrite(acct:0500) [Region B]

    Note over TxnA: Both prewrites succeed

    TxnA->>Node1: KvCommit(primary=acct:0100)
    Note over Node1: Primary committed

    TxnA->>Node2: KvCommit(secondary=acct:0500)
    Note over Node2: Region B split during commit!
    Note over Node2: CommitSecondary sent to wrong node
    Note over Node2: LOCK_NOT_FOUND (prewrite lock not replicated yet)

    TxnB->>Node2: KvGet(acct:0500)
    Note over Node2: Reads directly from engine
    Note over Node2: No Raft check → returns stale value
    Note over TxnB: TxnB reads pre-transfer balance
    Note over TxnB: TxnB's prewrite succeeds (no conflict detected)
    Note over TxnB: TxnB overwrites TxnA's value → LOST UPDATE
```

## 2. Current Architecture (Broken Read Path)

```mermaid
flowchart TD
    subgraph Client
        C[TxnKVClient]
    end

    subgraph "Server (tikvService)"
        KvGet[KvGet / KvScan]
        KvWrite[KvPrewrite / KvCommit]
        VRC["validateRegionContext()<br/>checks: regionId, leader, key range<br/>⚠ NO epoch check"]
    end

    subgraph StoreCoordinator
        PM[ProposeModifies<br/>Raft consensus]
    end

    subgraph Storage
        S["storage.GetWithIsolation()<br/>⚠ Direct engine read"]
    end

    subgraph Engine
        E[Pebble]
    end

    C -->|RPC| VRC
    VRC --> KvGet
    VRC --> KvWrite
    KvGet -->|"⚠ Bypasses Raft"| S
    S --> E
    KvWrite --> PM
    PM -->|Raft consensus| E

    style KvGet fill:#fdd,stroke:#a00
    style S fill:#fdd,stroke:#a00
    style VRC fill:#ffd,stroke:#aa0
```

**Problems:**
- Reads go directly to engine without Raft coordination
- No leader confirmation before serving reads
- No epoch validation → stale RPCs succeed after splits

## 3. Target Architecture

```mermaid
flowchart TD
    subgraph Client
        C[TxnKVClient]
    end

    subgraph "Server (tikvService)"
        VRC["validateRegionContext()<br/>checks: regionId, leader, key range,<br/>✅ epoch (Version + ConfVer)"]
        KvGet[KvGet / KvScan]
        KvWrite[KvPrewrite / KvCommit]
    end

    subgraph StoreCoordinator
        PM[ProposeModifies]
        RI["ReadIndex() ✅ NEW"]
    end

    subgraph "Peer goroutine"
        RN[RawNode]
        HR["handleReady()<br/>✅ Process ReadStates"]
        PR["pendingReads ✅ NEW"]
    end

    subgraph Storage
        S[storage.GetWithIsolation]
    end

    subgraph Engine
        E[Pebble]
    end

    C -->|RPC| VRC
    VRC -->|epoch OK| KvGet
    VRC -->|epoch OK| KvWrite
    VRC -->|"epoch mismatch"| C

    KvGet -->|"ReadIndex request"| RI
    RI -->|PeerMsgTypeReadIndex| RN
    RN -->|"Ready.ReadStates"| HR
    HR -->|"appliedIndex >= readIndex"| PR
    PR -->|"callback: safe to read"| S
    S --> E

    KvWrite --> PM
    PM -->|PeerMsgTypeRaftCommand| RN

    style RI fill:#dfd,stroke:#0a0
    style HR fill:#dfd,stroke:#0a0
    style PR fill:#dfd,stroke:#0a0
    style VRC fill:#dfd,stroke:#0a0
```

**Improvements:**
- Reads go through ReadIndex → Raft confirms leadership → wait for applied index
- Epoch validated on ALL RPCs → stale requests rejected immediately
- Client retries with fresh region metadata on EpochNotMatch

## 4. Two Features

| Feature | Purpose | Mechanism |
|---------|---------|-----------|
| **Region Epoch** | Reject stale RPCs after split/merge/conf-change | Compare `{Version, ConfVer}` in request context against current region |
| **Read Index** | Ensure reads see all committed writes (linearizability) | Leader contacts quorum via `rawNode.ReadIndex()`, waits for `appliedIndex >= readIndex` |

## 5. TiKV Reference

Both features are standard in TiKV:
- **Region Epoch**: `check_region_epoch()` in `raftstore/src/store/util.rs`
- **Read Index**: `async_snapshot()` in `server/raftkv/mod.rs`, with optional Leader Lease optimization

gookv uses the same protobuf types (`metapb.RegionEpoch`, `errorpb.EpochNotMatch`, `kvrpcpb.Context.RegionEpoch`) and the same Raft library (`go.etcd.io/etcd/raft/v3` which supports `RawNode.ReadIndex()` and `Ready.ReadStates`).
