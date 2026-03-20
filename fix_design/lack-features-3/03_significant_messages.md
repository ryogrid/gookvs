# Item 3: Significant Messages (Unreachable, MergeResult)

## Impact: Medium

## Current State

### What exists
- `PeerMsgTypeSignificant` defined in `internal/raftstore/msg.go`
- `SignificantMsg` struct with `Type`, `RegionID`, `ToPeerID`, `Status` fields
- `SignificantMsgTypeSnapshotStatus`, `SignificantMsgTypeUnreachable`, `SignificantMsgTypeMergeResult` constants defined
- `SnapshotStatus` is partially handled (etcd/raft `ReportSnapshot()` may be called elsewhere)

### What is missing
- `Peer.handleMessage()` has no `case PeerMsgTypeSignificant:` branch (`peer.go:265-294`)
- No code calls `rawNode.ReportUnreachable(peerID)` anywhere
- No code processes `MergeResult` to destroy source regions after merge
- `RaftClient.Send()` / `BatchSend()` do not generate Unreachable notifications on failure

## Design

### 1. Add PeerMsgTypeSignificant handler in Peer

**File**: `internal/raftstore/peer.go`

Add case to `handleMessage()`:

```go
case PeerMsgTypeSignificant:
    sig := msg.Data.(*SignificantMsg)
    p.handleSignificantMessage(sig)
```

New method:

```go
func (p *Peer) handleSignificantMessage(msg *SignificantMsg) {
    switch msg.Type {
    case SignificantMsgTypeUnreachable:
        p.rawNode.ReportUnreachable(msg.ToPeerID)

    case SignificantMsgTypeSnapshotStatus:
        p.rawNode.ReportSnapshot(msg.ToPeerID, msg.Status)

    case SignificantMsgTypeMergeResult:
        p.handleMergeResult(msg)
    }
}

func (p *Peer) handleMergeResult(msg *SignificantMsg) {
    // MergeResult signals that this region (the source) has been
    // merged into the target. Mark self for destruction.
    slog.Info("merge result received, destroying peer",
        "region", p.regionID, "peer", p.peerID)
    p.stopped.Store(true)
}
```

### 2. Generate Unreachable on transport failure

**File**: `internal/server/coordinator.go`

Modify `sendRaftMessage()` to notify on send failure:

```go
func (sc *StoreCoordinator) sendRaftMessage(...) {
    // ... existing code to build raftMessage ...

    if err := sc.client.Send(toStoreID, raftMessage); err != nil {
        slog.Warn("raft send failed", "to_store", toStoreID, "err", err)
        // Notify the peer that the target is unreachable
        sc.reportUnreachable(regionID, msg.To)
    }
}

func (sc *StoreCoordinator) reportUnreachable(regionID uint64, toPeerID uint64) {
    peerMsg := raftstore.PeerMsg{
        Type: raftstore.PeerMsgTypeSignificant,
        Data: &raftstore.SignificantMsg{
            Type:     raftstore.SignificantMsgTypeUnreachable,
            RegionID: regionID,
            ToPeerID: toPeerID,
        },
    }
    _ = sc.router.Send(regionID, peerMsg) // Best-effort
}
```

### 3. Add MergeResult to SignificantMsg

**File**: `internal/raftstore/msg.go`

Add fields needed for merge result (if not already present):

```go
type SignificantMsg struct {
    Type     SignificantMsgType
    RegionID uint64
    ToPeerID uint64
    Status   raft.SnapshotStatus
    // Merge fields
    MergeResultType MergeResultKind  // existing type from merge.go
}
```

Use Serena to verify the exact MergeResultKind type:
```
find_symbol("MergeResult", relative_path="internal/raftstore")
```

### 4. Wire MergeResult into merge execution

**File**: `internal/raftstore/peer.go` or `internal/server/coordinator.go`

When `ExecCommitMerge` completes (in the apply path), send a `SignificantMsgTypeMergeResult` to the source region's peer:

```go
// After ExecCommitMerge succeeds for the target region:
sourcePeerMsg := raftstore.PeerMsg{
    Type: raftstore.PeerMsgTypeSignificant,
    Data: &raftstore.SignificantMsg{
        Type:     raftstore.SignificantMsgTypeMergeResult,
        RegionID: sourceRegionID,
    },
}
_ = sc.router.Send(sourceRegionID, sourcePeerMsg)
```

Use Serena to locate the ExecCommitMerge call site:
```
search_for_pattern("ExecCommitMerge", relative_path="internal")
```

## File Changes

| File | Changes |
|------|---------|
| `internal/raftstore/peer.go` | Add `PeerMsgTypeSignificant` case; add `handleSignificantMessage()`, `handleMergeResult()` |
| `internal/raftstore/msg.go` | Add merge-related fields to `SignificantMsg` if needed |
| `internal/server/coordinator.go` | Add `reportUnreachable()` call on send failure; wire merge result notification |

## Tests

### Unit Tests

**File**: `internal/raftstore/significant_msg_test.go`

```
TestHandleUnreachableCallsReportUnreachable
  - Create Peer with mock rawNode
  - Send PeerMsgTypeSignificant with SignificantMsgTypeUnreachable
  - Verify rawNode.ReportUnreachable() called with correct peerID

TestHandleSnapshotStatusCallsReportSnapshot
  - Create Peer with mock rawNode
  - Send SignificantMsgTypeSnapshotStatus with SnapshotFinish
  - Verify rawNode.ReportSnapshot() called

TestHandleMergeResultStopsPeer
  - Create Peer
  - Send SignificantMsgTypeMergeResult
  - Verify peer.stopped.Load() == true

TestSendFailureGeneratesUnreachable
  - Create StoreCoordinator with mock RaftClient that returns error
  - Call sendRaftMessage()
  - Verify PeerMsgTypeSignificant with Unreachable appears in peer mailbox
```

### E2E Tests

**File**: `e2e/unreachable_notification_test.go`

```
TestUnreachablePeerSuppressesSending
  1. Start 3-node cluster
  2. Write data, confirm replication
  3. Kill node 3 (simulate unreachable)
  4. Write more data — leader will attempt to send to node 3
  5. Verify that after a few failures, leader stops sending (election timeout)
  6. Restart node 3, verify it catches up
```

## Risks

- **Unreachable storm**: If a node is down, every message to it generates an Unreachable. etcd/raft handles this gracefully (stops sending after a few reports). The log volume is the main concern — use `slog.Warn` with rate limiting if needed.
- **MergeResult race**: The source peer might process the MergeResult before the commit merge is fully applied. The `stopped.Store(true)` is safe regardless of timing.
