# Item 2: Store Goroutine Completion

## Impact: Medium (prerequisite for PD scheduling)

## Current State

### What exists
- `StoreCoordinator.RunStoreWorker()` consumes `router.StoreCh()` and dispatches to `handleStoreMsg()` (`coordinator.go:291-301`)
- `handleStoreMsg()` handles `StoreMsgTypeCreatePeer`, `StoreMsgTypeDestroyPeer`, and `StoreMsgTypeRaftMessage` (`coordinator.go:304-316`)
- `maybeCreatePeerForMessage()` creates a peer if a RaftMessage arrives for an unknown region (`coordinator.go:404+`)
- `CreatePeer()` is fully implemented with sendFunc/applyFunc wiring (`coordinator.go:318-371`)

### What is missing
- `tikvService.Raft()` and `BatchRaft()` route ALL messages directly to `coordinator.HandleRaftMessage()`, which calls `router.Send()`. If the region is unknown, `router.Send()` returns `ErrRegionNotFound` and the message is silently dropped.
- No code path sends `StoreMsgTypeRaftMessage` to `router.SendStore()` on unknown regions.
- `RunStoreWorker()` may not be started in `cmd/gookv-server/main.go`.

## Design

### 1. Route unknown-region Raft messages to storeCh

**File**: `internal/server/coordinator.go`

Modify `HandleRaftMessage()` to fall back to `storeCh` when the region is not found:

```go
func (sc *StoreCoordinator) HandleRaftMessage(msg *raft_serverpb.RaftMessage) error {
    regionID := msg.GetRegionId()

    raftMsg, err := raftstore.EraftpbToRaftpb(msg.GetMessage())
    if err != nil {
        return fmt.Errorf("raftstore: convert message: %w", err)
    }

    peerMsg := raftstore.PeerMsg{
        Type: raftstore.PeerMsgTypeRaftMessage,
        Data: &raftMsg,
    }

    err = sc.router.Send(regionID, peerMsg)
    if err == router.ErrRegionNotFound {
        // Route to store worker for potential peer creation
        storeMsg := raftstore.StoreMsg{
            Type: raftstore.StoreMsgTypeRaftMessage,
            Data: msg,  // Pass original raft_serverpb.RaftMessage
        }
        return sc.router.SendStore(storeMsg)
    }
    return err
}
```

### 2. Ensure RunStoreWorker is started

**File**: `cmd/gookv-server/main.go`

Verify that `go coord.RunStoreWorker(ctx)` is called after coordinator creation. Use Serena's `search_for_pattern` to check:
```
search_for_pattern("RunStoreWorker", relative_path="cmd/gookv-server")
```

If not present, add it alongside `RunSplitResultHandler`:
```go
go coord.RunStoreWorker(storeCtx)
```

### 3. Improve maybeCreatePeerForMessage logging

**File**: `internal/server/coordinator.go`

Add structured logging so that dynamic peer creation is observable:
```go
func (sc *StoreCoordinator) maybeCreatePeerForMessage(msg *raft_serverpb.RaftMessage) {
    regionID := msg.GetRegionId()
    if sc.router.HasRegion(regionID) {
        return
    }
    slog.Info("creating peer for unknown region",
        "region_id", regionID,
        "from_store", msg.GetFromPeer().GetStoreId())
    // ... existing creation logic ...
}
```

## File Changes

| File | Changes |
|------|---------|
| `internal/server/coordinator.go` | Modify `HandleRaftMessage()` to fallback to `SendStore()` on unknown region; improve logging in `maybeCreatePeerForMessage()` |
| `cmd/gookv-server/main.go` | Ensure `RunStoreWorker()` is started (verify, add if needed) |

## Tests

### Unit Tests

**File**: `internal/server/coordinator_store_test.go`

```
TestHandleRaftMessageRoutesToStoreCh
  - Create coordinator with router (no peers registered)
  - Call HandleRaftMessage() with a message for unknown region
  - Verify message appears on router.StoreCh()

TestHandleRaftMessageRoutesToPeerMailbox
  - Create coordinator, register a peer for region 1
  - Call HandleRaftMessage() with message for region 1
  - Verify message appears in peer mailbox (not on storeCh)

TestMaybeCreatePeerForMessageCreatesPeer
  - Create coordinator with mock PD client
  - Call maybeCreatePeerForMessage() with valid RaftMessage
  - Verify peer is created and registered in router
```

### E2E Tests

**File**: `e2e/dynamic_peer_creation_test.go`

```
TestDynamicPeerCreationViaRaftMessage
  1. Start PD + 3-node cluster (nodes 1-3)
  2. Write data to leader
  3. Start node 4 (new, empty store)
  4. Propose ConfChange AddNode for node 4 on leader
  5. Verify node 4 receives Raft messages and auto-creates a peer
  6. Wait for node 4 to catch up (via snapshot from item 1)
  7. Read data from node 4, verify consistency
```

## Risks

- **Message storms**: If many messages arrive for an unknown region simultaneously, they will all be routed to storeCh. `maybeCreatePeerForMessage()` checks `router.HasRegion()` first, so duplicate creations are prevented. The storeCh channel has limited capacity (256), providing natural backpressure.
- **PD dependency**: `maybeCreatePeerForMessage()` may need to query PD for region metadata. If PD is unavailable, peer creation fails. This is acceptable — retry will happen on next message.
