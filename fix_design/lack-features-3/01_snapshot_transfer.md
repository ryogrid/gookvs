# Item 1: Raft Snapshot gRPC Streaming Transfer

## Impact: High

## Current State

### What exists
- **Generation**: `GenerateSnapshotData()` scans 3 data CFs and produces `SnapshotData` with CRC32 checksums (`internal/raftstore/snapshot.go:209-230`)
- **Serialization**: `MarshalSnapshotData()` / `UnmarshalSnapshotData()` for binary format (`snapshot.go:75-197`)
- **Application**: `ApplySnapshotData()` clears old data, verifies checksums, writes atomically (`snapshot.go:262-293`)
- **Worker**: `SnapWorker` processes `GenSnapTask` asynchronously via channel (`snapshot.go:295-362`)
- **PeerStorage**: `RequestSnapshot()` with FSM (Relax→Generating), `ApplySnapshot()` for follower application (`snapshot.go:365-489`)
- **Transport**: `RaftClient.SendSnapshot()` sends 1MB-chunked `SnapshotChunk` over gRPC stream (`internal/server/transport/transport.go:147-203`)

### What is missing
1. **`tikvService.Snapshot()` gRPC handler** — server returns "Unimplemented" for Snapshot RPC
2. **`Peer.handleReady()` snapshot processing** — `rd.Snapshot` from etcd/raft is ignored (`peer.go:330-382`)
3. **Snapshot send path** — `sendRaftMessage()` uses `RaftClient.Send()` for all messages including snapshots; never calls `SendSnapshot()` (`coordinator.go:545-571`)
4. **`PeerStorage.Snapshot()`** returns only empty metadata, never triggers actual data generation (`storage.go:162-173`)

## Design

### 1. Wire PeerStorage.Snapshot() to SnapWorker

**File**: `internal/raftstore/storage.go`

Modify `Snapshot()` to call `RequestSnapshot()` instead of returning empty metadata:

```go
func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    if s.snapTaskCh == nil {
        // No snap worker wired — return metadata-only (standalone fallback)
        return raftpb.Snapshot{
            Metadata: raftpb.SnapshotMetadata{
                Index: s.applyState.TruncatedIndex,
                Term:  s.applyState.TruncatedTerm,
            },
        }, nil
    }

    return s.RequestSnapshot(s.snapTaskCh, s.region)
}
```

Add fields to PeerStorage:
- `snapTaskCh chan<- GenSnapTask` — wired by coordinator at peer creation
- `region *metapb.Region` — already available via Peer, pass at construction

**File**: `internal/raftstore/peer.go`

Add `SetSnapTaskCh(ch chan<- GenSnapTask)` method. Coordinator calls this after peer creation.

### 2. Handle rd.Snapshot in Peer.handleReady()

**File**: `internal/raftstore/peer.go`

In `handleReady()`, after `SaveReady(rd)` and before sending messages, add:

```go
if !raft.IsEmptySnap(rd.Snapshot) {
    if err := p.storage.ApplySnapshot(rd.Snapshot); err != nil {
        slog.Error("failed to apply snapshot", "region", p.regionID, "err", err)
    }
}
```

Use `go.etcd.io/etcd/raft/v3.IsEmptySnap()` to check.

### 3. Distinguish snapshot messages in sendFunc

**File**: `internal/server/coordinator.go`

Modify `sendRaftMessage()` to detect MsgSnap type and use `SendSnapshot()`:

```go
func (sc *StoreCoordinator) sendRaftMessage(regionID uint64, region *metapb.Region, fromPeerID uint64, msg *raftpb.Message) {
    // ... resolve target store ...

    if msg.Type == raftpb.MsgSnap {
        snapData := msg.Snapshot.Data
        // Send via streaming RPC
        go func() {
            if err := sc.client.SendSnapshot(toStoreID, raftMessage, snapData); err != nil {
                slog.Warn("snapshot send failed", "to_store", toStoreID, "err", err)
                // Report snapshot failure back to peer
                sc.reportSnapshotStatus(regionID, msg.To, raft.SnapshotFailure)
            } else {
                sc.reportSnapshotStatus(regionID, msg.To, raft.SnapshotFinish)
            }
        }()
        return
    }

    // Normal message path (existing code)
    sc.client.Send(toStoreID, raftMessage)
}
```

Add helper `reportSnapshotStatus()` that sends `PeerMsgTypeSignificant` with `SignificantMsgTypeSnapshotStatus` to the source peer.

### 4. Implement tikvService.Snapshot() gRPC handler

**File**: `internal/server/server.go`

```go
func (svc *tikvService) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
    var raftMsg *raft_serverpb.RaftMessage
    var dataBuf bytes.Buffer

    for {
        chunk, err := stream.Recv()
        if err == io.EOF {
            break
        }
        if err != nil {
            return err
        }
        if chunk.GetMessage() != nil {
            raftMsg = chunk.GetMessage()
        }
        dataBuf.Write(chunk.GetData())
    }

    if raftMsg == nil {
        return stream.SendAndClose(&raft_serverpb.Done{})
    }

    coord := svc.server.coordinator
    if coord == nil {
        return stream.SendAndClose(&raft_serverpb.Done{})
    }

    if err := coord.HandleSnapshotMessage(raftMsg, dataBuf.Bytes()); err != nil {
        slog.Warn("snapshot receive failed", "err", err)
    }

    return stream.SendAndClose(&raft_serverpb.Done{})
}
```

### 5. Add HandleSnapshotMessage to StoreCoordinator

**File**: `internal/server/coordinator.go`

```go
func (sc *StoreCoordinator) HandleSnapshotMessage(msg *raft_serverpb.RaftMessage, data []byte) error {
    regionID := msg.GetRegionId()

    // Convert the eraftpb message
    raftMsg, err := raftstore.EraftpbToRaftpb(msg.GetMessage())
    if err != nil {
        return err
    }

    // Attach snapshot data
    raftMsg.Snapshot.Data = data

    peerMsg := raftstore.PeerMsg{
        Type: raftstore.PeerMsgTypeRaftMessage,
        Data: &raftMsg,
    }

    // If region doesn't exist locally, try to create it
    if !sc.router.HasRegion(regionID) {
        sc.maybeCreatePeerForMessage(msg)
    }

    return sc.router.Send(regionID, peerMsg)
}
```

### 6. Wire SnapWorker channel in StoreCoordinator

**File**: `internal/server/coordinator.go`

In `BootstrapRegion()` and `CreatePeer()`, wire the snap task channel:

```go
if sc.snapWorker != nil {
    peer.SetSnapTaskCh(sc.snapWorker.TaskCh())
}
```

Add `snapWorker *raftstore.SnapWorker` field to StoreCoordinator. Create and start it in `NewStoreCoordinator()`.

## File Changes

| File | Changes |
|------|---------|
| `internal/raftstore/storage.go` | Add `snapTaskCh`, `region` fields; modify `Snapshot()` |
| `internal/raftstore/peer.go` | Add `SetSnapTaskCh()`; handle `rd.Snapshot` in `handleReady()` |
| `internal/server/coordinator.go` | Add `snapWorker` field; create/start SnapWorker; modify `sendRaftMessage()` for MsgSnap; add `HandleSnapshotMessage()`, `reportSnapshotStatus()` |
| `internal/server/server.go` | Implement `tikvService.Snapshot()` handler |
| `internal/raftstore/snapshot.go` | Add `TaskCh()` accessor to SnapWorker (if not present) |

## Tests

### Unit Tests

**File**: `internal/raftstore/snapshot_transfer_test.go`

```
TestPeerStorageSnapshotTriggersGeneration
  - Create PeerStorage with snapTaskCh wired
  - Call Snapshot()
  - Verify GenSnapTask is sent to channel
  - Verify ErrSnapshotTemporarilyUnavailable returned

TestPeerHandleReadyAppliesSnapshot
  - Create Peer with mock engine
  - Inject raft.Ready with non-empty Snapshot
  - Verify ApplySnapshotData is called on engine
  - Verify apply state is updated

TestSendRaftMessageDetectsSnapAndUsesStreaming
  - Create StoreCoordinator with mock RaftClient
  - Call sendRaftMessage with MsgSnap
  - Verify SendSnapshot() called (not Send())
  - Verify SnapshotStatus reported back on success/failure
```

**File**: `internal/server/snapshot_handler_test.go`

```
TestSnapshotHandlerReceivesChunks
  - Mock gRPC stream with 3 chunks (first has metadata, all have data)
  - Call Snapshot() handler
  - Verify coordinator.HandleSnapshotMessage() called with reassembled data

TestSnapshotHandlerEmptyStream
  - Send EOF immediately
  - Verify no error, Done returned
```

### E2E Tests

**File**: `e2e/snapshot_transfer_test.go`

```
TestSnapshotTransferOnLaggingFollower
  1. Start PD + 3-node cluster
  2. Write 1000 keys to leader
  3. Stop node 3, write 5000 more keys
  4. Compact Raft log on leader (force log truncation)
  5. Restart node 3
  6. Wait for node 3 to catch up via snapshot
  7. Read keys from node 3, verify all 6000 present

TestSnapshotTransferToNewPeer
  1. Start PD + 3-node cluster
  2. Write data
  3. Add a 4th node via ConfChange
  4. Wait for snapshot transfer to new node
  5. Verify data on new node
```

## Risks

- **Large snapshot data**: If a region has GB of data, the snapshot may exceed memory. Mitigation: stream chunks to disk. For initial implementation, in-memory is acceptable with a size warning log.
- **Concurrent snapshot requests**: Multiple followers requesting snapshots simultaneously could overwhelm the leader. Mitigation: SnapWorker already serializes generation. Rate limiting can be added later.
- **Snapshot data consistency**: Snapshot is taken from a Pebble snapshot (point-in-time), so consistency is guaranteed by the engine.
