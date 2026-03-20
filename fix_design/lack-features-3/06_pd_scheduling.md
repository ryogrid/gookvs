# Item 6: PD Scheduling Commands

## Impact: High

## Prerequisites
- Item 1 (Snapshot transfer) — required for new replica data transfer
- Item 2 (Store goroutine completion) — required for dynamic peer creation

## Current State

### What exists
- `PDServer.RegionHeartbeat()` receives heartbeats, stores region/leader/stats in `MetadataStore` (`internal/pd/server.go:238-260`)
- `MetadataStore` has `stores`, `regions`, `leaders`, `storeStats` maps
- `StoreHeartbeat` records `StoreStats` per store
- `pdpb.RegionHeartbeatResponse` proto has fields: `change_peer`, `transfer_leader`, `merge`, `split_region`, `change_peer_v2`
- `PDWorker.sendRegionHeartbeat()` sends heartbeats but **discards the response** (`pd_worker.go:224-243`)
- `Peer.ProposeConfChange()` exists for membership changes (`conf_change.go`)
- `ExecPrepareMerge` / `ExecCommitMerge` exist for merge execution (`merge.go`)

### What is missing
- **Scheduling engine**: No logic in PDServer to decide TransferLeader / ChangePeer / Merge
- **Store alive/dead detection**: No heartbeat-timestamp-based liveness check
- **PDWorker response handling**: Response commands are not parsed or forwarded to peers
- **Peer command handlers**: No TransferLeader execution; no PD-initiated ChangePeer or Merge flow
- **New scheduling message type**: No way to deliver scheduling commands to peers

## Design

### Phase 1: Store Liveness Detection

**File**: `internal/pd/server.go`

Add heartbeat timestamp tracking to `MetadataStore`:

```go
type MetadataStore struct {
    // ... existing fields ...
    storeLastHeartbeat map[uint64]time.Time  // New: storeID -> last heartbeat time
}

const StoreDownDuration = 30 * time.Second  // Store is considered dead after 30s

func (m *MetadataStore) UpdateStoreHeartbeat(storeID uint64) {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.storeLastHeartbeat[storeID] = time.Now()
}

func (m *MetadataStore) IsStoreAlive(storeID uint64) bool {
    m.mu.RLock()
    defer m.mu.RUnlock()
    t, ok := m.storeLastHeartbeat[storeID]
    if !ok {
        return false
    }
    return time.Since(t) < StoreDownDuration
}

func (m *MetadataStore) GetDeadStores() []uint64 {
    m.mu.RLock()
    defer m.mu.RUnlock()
    var dead []uint64
    for id, t := range m.storeLastHeartbeat {
        if time.Since(t) >= StoreDownDuration {
            dead = append(dead, id)
        }
    }
    return dead
}
```

Update `StoreHeartbeat` handler to call `UpdateStoreHeartbeat()`.

### Phase 2: Scheduling Engine

**File**: `internal/pd/scheduler.go` (new)

Create a scheduler that produces commands based on cluster state:

```go
package pd

type Scheduler struct {
    meta           *MetadataStore
    maxPeerCount   int  // Target replica count (default: 3)
}

type ScheduleCommand struct {
    RegionID       uint64
    TransferLeader *pdpb.TransferLeader
    ChangePeer     *pdpb.ChangePeer
    Merge          *pdpb.Merge
}

func (s *Scheduler) Schedule(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
    // Try each scheduler in priority order
    if cmd := s.scheduleReplicaRepair(regionID, region); cmd != nil {
        return cmd
    }
    if cmd := s.scheduleLeaderBalance(regionID, region, leader); cmd != nil {
        return cmd
    }
    if cmd := s.scheduleRegionMerge(regionID, region); cmd != nil {
        return cmd
    }
    return nil
}
```

#### a. Replica Repair

```go
func (s *Scheduler) scheduleReplicaRepair(regionID uint64, region *metapb.Region) *ScheduleCommand {
    // Check if any peer is on a dead store
    var alivePeers []*metapb.Peer
    for _, peer := range region.GetPeers() {
        if s.meta.IsStoreAlive(peer.GetStoreId()) {
            alivePeers = append(alivePeers, peer)
        }
    }

    if len(alivePeers) >= s.maxPeerCount {
        return nil  // Enough replicas
    }

    // Find a store that doesn't already host this region
    targetStore := s.pickStoreForRegion(region)
    if targetStore == 0 {
        return nil  // No available store
    }

    newPeerID, _ := s.meta.allocID()
    return &ScheduleCommand{
        RegionID: regionID,
        ChangePeer: &pdpb.ChangePeer{
            Peer: &metapb.Peer{
                Id:      newPeerID,
                StoreId: targetStore,
            },
            ChangeType: eraftpb.ConfChangeType_AddNode,
        },
    }
}
```

#### b. Leader Balance

```go
func (s *Scheduler) scheduleLeaderBalance(regionID uint64, region *metapb.Region, leader *metapb.Peer) *ScheduleCommand {
    // Count leaders per store
    leaderCounts := s.meta.GetLeaderCountPerStore()

    leaderStore := leader.GetStoreId()
    leaderCount := leaderCounts[leaderStore]

    // Find store with minimum leaders
    var minStore uint64
    minCount := int(^uint(0) >> 1)
    for _, peer := range region.GetPeers() {
        storeID := peer.GetStoreId()
        if !s.meta.IsStoreAlive(storeID) {
            continue
        }
        if count := leaderCounts[storeID]; count < minCount {
            minCount = count
            minStore = storeID
        }
    }

    // Transfer if difference > 1
    if minStore != 0 && minStore != leaderStore && leaderCount-minCount > 1 {
        return &ScheduleCommand{
            RegionID: regionID,
            TransferLeader: &pdpb.TransferLeader{
                Peer: &metapb.Peer{StoreId: minStore},
            },
        }
    }
    return nil
}
```

#### c. Region Merge (simplified)

```go
func (s *Scheduler) scheduleRegionMerge(regionID uint64, region *metapb.Region) *ScheduleCommand {
    stats := s.meta.GetRegionStats(regionID)
    if stats == nil || stats.ApproximateSize > RegionMergeThreshold {
        return nil  // Too large to merge
    }

    // Find adjacent region
    adjacent := s.meta.FindAdjacentRegion(region)
    if adjacent == nil {
        return nil
    }

    return &ScheduleCommand{
        RegionID: regionID,
        Merge: &pdpb.Merge{
            Target: adjacent,
        },
    }
}
```

### Phase 3: Return Commands in RegionHeartbeat

**File**: `internal/pd/server.go`

Modify `RegionHeartbeat` handler:

```go
func (s *PDServer) RegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
    for {
        req, err := stream.Recv()
        // ... existing error handling ...

        region := req.GetRegion()
        leader := req.GetLeader()
        s.meta.PutRegion(region, leader)

        resp := &pdpb.RegionHeartbeatResponse{Header: s.header()}

        // Run scheduler
        if cmd := s.scheduler.Schedule(region.GetId(), region, leader); cmd != nil {
            if cmd.TransferLeader != nil {
                resp.TransferLeader = cmd.TransferLeader
            }
            if cmd.ChangePeer != nil {
                resp.ChangePeer = cmd.ChangePeer
            }
            if cmd.Merge != nil {
                resp.Merge = cmd.Merge
            }
        }

        if err := stream.Send(resp); err != nil {
            return err
        }
    }
}
```

### Phase 4: PDWorker Processes Scheduling Commands

**File**: `internal/server/pd_worker.go`

First, change `ReportRegionHeartbeat` to return the response. Use Serena to check the current signature:
```
find_symbol("ReportRegionHeartbeat", relative_path="pkg/pdclient", include_body=True)
```

The `pdclient.Client.ReportRegionHeartbeat()` currently returns only `error`. To get the scheduling response, either:
- (a) Change the return type to include the response, or
- (b) Add a separate `RegionHeartbeatWithResponse` method

Option (a) is cleaner — update the interface:

```go
ReportRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error)
```

Then in PDWorker:

```go
func (w *PDWorker) sendRegionHeartbeat(data *RegionHeartbeatData) {
    // ... build req ...

    resp, err := w.pdClient.ReportRegionHeartbeat(ctx, req)
    if err != nil {
        slog.Warn("region heartbeat failed", "region", data.Region.GetId(), "err", err)
        return
    }

    w.handleSchedulingCommand(data.Region.GetId(), resp)
}

func (w *PDWorker) handleSchedulingCommand(regionID uint64, resp *pdpb.RegionHeartbeatResponse) {
    if resp.GetTransferLeader() != nil {
        w.sendScheduleMsg(regionID, &ScheduleMsg{
            Type:           ScheduleMsgTransferLeader,
            TransferLeader: resp.GetTransferLeader(),
        })
    }
    if resp.GetChangePeer() != nil {
        w.sendScheduleMsg(regionID, &ScheduleMsg{
            Type:       ScheduleMsgChangePeer,
            ChangePeer: resp.GetChangePeer(),
        })
    }
    if resp.GetMerge() != nil {
        w.sendScheduleMsg(regionID, &ScheduleMsg{
            Type:  ScheduleMsgMerge,
            Merge: resp.GetMerge(),
        })
    }
}
```

### Phase 5: Peer Handles Scheduling Messages

**File**: `internal/raftstore/msg.go`

Add new message type:

```go
PeerMsgTypeSchedule PeerMsgType = 8  // PD scheduling command

type ScheduleMsg struct {
    Type           ScheduleMsgType
    TransferLeader *pdpb.TransferLeader
    ChangePeer     *pdpb.ChangePeer
    Merge          *pdpb.Merge
}

type ScheduleMsgType int
const (
    ScheduleMsgTransferLeader ScheduleMsgType = iota
    ScheduleMsgChangePeer
    ScheduleMsgMerge
)
```

**File**: `internal/raftstore/peer.go`

Add handler:

```go
case PeerMsgTypeSchedule:
    sched := msg.Data.(*ScheduleMsg)
    p.handleScheduleMessage(sched)

func (p *Peer) handleScheduleMessage(msg *ScheduleMsg) {
    if !p.isLeader.Load() {
        return  // Only leader executes scheduling
    }

    switch msg.Type {
    case ScheduleMsgTransferLeader:
        targetPeer := msg.TransferLeader.GetPeer()
        // Find peer ID for the target store
        for _, peer := range p.region.GetPeers() {
            if peer.GetStoreId() == targetPeer.GetStoreId() {
                p.rawNode.TransferLeader(peer.GetId())
                return
            }
        }

    case ScheduleMsgChangePeer:
        cp := msg.ChangePeer
        changeType := raftpb.ConfChangeAddNode
        if cp.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
            changeType = raftpb.ConfChangeRemoveNode
        }
        _ = p.ProposeConfChange(changeType, cp.GetPeer().GetId(), cp.GetPeer().GetStoreId())

    case ScheduleMsgMerge:
        // Initiate merge protocol
        _ = p.proposeMerge(msg.Merge.GetTarget())
    }
}
```

## File Changes

| File | Changes |
|------|---------|
| `internal/pd/server.go` | Add liveness tracking, wire scheduler |
| `internal/pd/scheduler.go` | New file: scheduling engine |
| `pkg/pdclient/client.go` | Change `ReportRegionHeartbeat` return type |
| `pkg/pdclient/mock.go` | Update mock for new return type |
| `internal/server/pd_worker.go` | Process scheduling response, forward to peers |
| `internal/raftstore/msg.go` | Add `PeerMsgTypeSchedule`, `ScheduleMsg` |
| `internal/raftstore/peer.go` | Add `handleScheduleMessage()` with TransferLeader/ChangePeer/Merge |

## Tests

### Unit Tests

**File**: `internal/pd/scheduler_test.go`

```
TestSchedulerReplicaRepair
  - Set up MetadataStore with 3 stores, 1 dead
  - Region has 3 peers, 1 on dead store
  - Schedule() → returns ChangePeer AddNode to a live store

TestSchedulerLeaderBalance
  - Store A has 5 leaders, Store B has 1 leader
  - Region has peers on A and B, leader on A
  - Schedule() → returns TransferLeader to B

TestSchedulerNoActionWhenBalanced
  - All stores healthy, leaders balanced
  - Schedule() → returns nil

TestSchedulerRegionMerge
  - Region with approximate_size below threshold
  - Adjacent region exists
  - Schedule() → returns Merge command
```

**File**: `internal/server/pd_worker_schedule_test.go`

```
TestPDWorkerForwardsTransferLeader
  - Mock PD client returns TransferLeader in heartbeat response
  - Verify ScheduleMsg delivered to peer mailbox

TestPDWorkerForwardsChangePeer
  - Mock PD returns ChangePeer AddNode
  - Verify ScheduleMsg delivered

TestPDWorkerIgnoresEmptyResponse
  - Mock PD returns empty response
  - Verify no scheduling message sent
```

**File**: `internal/raftstore/peer_schedule_test.go`

```
TestPeerHandlesTransferLeader
  - Peer is leader, receives ScheduleMsgTransferLeader
  - Verify rawNode.TransferLeader() called

TestPeerHandlesChangePeerAddNode
  - Peer is leader, receives ScheduleMsgChangePeer AddNode
  - Verify ProposeConfChange called

TestPeerIgnoresScheduleWhenNotLeader
  - Peer is follower, receives schedule message
  - Verify no action taken
```

### E2E Tests

**File**: `e2e/pd_scheduling_test.go`

```
TestLeaderBalancing
  1. Start PD + 3-node cluster
  2. Create 10 regions, all leaders on node 1
  3. Wait for PD to issue TransferLeader commands
  4. Verify leader distribution becomes more balanced (timeout 30s)

TestReplicaRepair
  1. Start PD + 3-node cluster (replica count = 3)
  2. Write data
  3. Kill node 3
  4. Wait for PD to detect dead store (30s)
  5. Verify PD issues ChangePeer to add replica on remaining stores
  6. Verify region has 3 live replicas again

TestNoSchedulingOnHealthyCluster
  1. Start PD + 3-node cluster with balanced state
  2. Monitor heartbeats for 10s
  3. Verify no scheduling commands issued
```

## Risks

- **Scheduling storms**: Multiple heartbeats in quick succession could generate conflicting commands. Mitigation: add a cooldown period per region (only schedule once per N seconds).
- **Split brain**: If PD is partitioned from some nodes, it may incorrectly mark them dead. Mitigation: `StoreDownDuration` should be generous (30s+), and RemoveNode should be delayed.
- **ReportRegionHeartbeat interface change**: Changing the return type breaks `MockClient` and the gRPC implementation. Both must be updated atomically.
- **Merge complexity**: Region merge requires coordination between source and target regions and is the most complex scheduling command. Initial implementation can defer merge to a later phase.

## Recommended Incremental Approach

1. **Phase 6a**: Store liveness detection + leader balancing (simplest scheduling)
2. **Phase 6b**: Replica repair (requires item 1 + 2)
3. **Phase 6c**: Region merge scheduling (deferred)
