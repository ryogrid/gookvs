# PD Server Raft Replication — Detailed Design

## 1. Architecture Overview

PD server replication embeds a single `raft.RawNode` inside `PDServer` to replicate all mutable state across a cluster of PD nodes. Unlike KVS raftstore, which runs one Raft group per region, PD runs exactly one Raft group for the entire PD cluster.

All mutating operations are serialized as `PDCommand` structs and proposed through Raft. Once committed, the command is applied to the in-memory state machine (`MetadataStore`, `TSOAllocator`, `IDAllocator`, `GCSafePointManager`, `MoveTracker`). Read-only operations may be served from any node (stale reads acceptable for metadata lookups) or forwarded to the leader (for consistency-sensitive reads like TSO).

### Component Diagram

```
PDServer
├── PDRaftPeer          (internal/pd/raft_peer.go)
│   ├── raft.RawNode
│   ├── PDRaftStorage   (internal/pd/raft_storage.go)   ── raft.Storage impl
│   ├── sendFunc        ── wired to PDTransport
│   └── applyFunc       ── wired to applyCommand()
├── PDTransport         (internal/pd/transport.go)      ── gRPC inter-PD messaging
├── MetadataStore       (internal/pd/server.go)          ── state machine target
├── TSOAllocator        (internal/pd/server.go)          ── state machine target
├── IDAllocator         (internal/pd/server.go)          ── state machine target
├── GCSafePointManager  (internal/pd/server.go)          ── state machine target
├── MoveTracker         (internal/pd/move_tracker.go)    ── state machine target
└── Scheduler           (internal/pd/scheduler.go)       ── stateless, reads only
```

## 2. PDRaftPeer (`internal/pd/raft_peer.go`)

Modeled after `Peer` in `internal/raftstore/peer.go`. Simplified: no region management, no split check, no log GC worker, no apply worker. The PD Raft group handles a single, low-throughput state machine.

### Struct Definition

```go
type PDRaftMsg struct {
    Type PDRaftMsgType
    Data interface{}
}

type PDRaftMsgType uint8

const (
    PDRaftMsgTypeRaftMessage PDRaftMsgType = iota + 1 // *raftpb.Message from other PD nodes
    PDRaftMsgTypeProposal                             // *PDProposal to propose through Raft
)

type PDProposal struct {
    Command  PDCommand
    Callback func([]byte, error) // invoked after commit+apply
}

type PDRaftConfig struct {
    NodeID         uint64            // this node's ID in the Raft cluster
    InitialCluster map[uint64]string // nodeID → peer address (for Raft peer communication)
    ClientAddrs    map[uint64]string // nodeID → client gRPC address (for leader forwarding)

    RaftBaseTickInterval     time.Duration // default 100ms
    RaftElectionTimeoutTicks int           // default 10
    RaftHeartbeatTicks       int           // default 2
    MaxInflightMsgs          int           // default 256
    MaxSizePerMsg            uint64        // default 1 MiB
    MailboxCapacity          int           // default 256
    // PreVote is always enabled (hardcoded to `true` in raft.Config). No configuration needed.
}

type PDRaftPeer struct {
    rawNode   *raft.RawNode
    storage   *PDRaftStorage

    mailbox   chan PDRaftMsg                    // proposals + Raft messages
    sendFunc  func([]raftpb.Message)            // outbound Raft messages → PDTransport
    applyFunc func(cmd PDCommand) ([]byte, error) // state machine apply

    pendingProposals map[uint64]func([]byte, error) // index → callback

    isLeader  atomic.Bool
    leaderID  atomic.Uint64
    stopped   atomic.Bool

    cfg PDRaftConfig // contains NodeID, InitialCluster (peer addrs), ClientAddrs
}
```

### Constructor

```go
func NewPDRaftPeer(
    storage *PDRaftStorage,
    peers []raft.Peer,
    cfg PDRaftConfig,
) (*PDRaftPeer, error)
```

Follows the same pattern as `NewPeer` in `internal/raftstore/peer.go:127`:
1. If `peers` is non-empty (bootstrap), set storage to empty state and add a dummy entry at index 0, then call `rawNode.Bootstrap(peers)`.
2. If `peers` is empty (restart), call `storage.RecoverFromEngine()` before creating the `RawNode`.
3. Create `raft.Config` with `ID: cfg.NodeID`, `Storage: storage`, `PreVote: true`, and the tick/inflight settings from `cfg`.
4. Create `raft.RawNode` via `raft.NewRawNode(raftCfg)`.

### Event Loop (`Run`)

```go
func (p *PDRaftPeer) Run(ctx context.Context) {
    ticker := time.NewTicker(p.cfg.RaftBaseTickInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            p.stopped.Store(true)
            return

        case <-ticker.C:
            p.rawNode.Tick()

        case msg, ok := <-p.mailbox:
            if !ok {
                p.stopped.Store(true)
                return
            }
            p.handleMessage(msg)
        }

        p.handleReady()
    }
}
```

### handleMessage

```go
func (p *PDRaftPeer) handleMessage(msg PDRaftMsg) {
    switch msg.Type {
    case PDRaftMsgTypeRaftMessage:
        raftMsg := msg.Data.(*raftpb.Message)
        _ = p.rawNode.Step(*raftMsg) // errors on stale messages are benign

    case PDRaftMsgTypeProposal:
        proposal := msg.Data.(*PDProposal)
        p.propose(proposal)
    }
}
```

### handleReady

Follows `Peer.handleReady()` at `internal/raftstore/peer.go:415`:

```go
func (p *PDRaftPeer) handleReady() {
    if !p.rawNode.HasReady() {
        return
    }

    rd := p.rawNode.Ready()

    // 1. Update leader state from SoftState.
    if rd.SoftState != nil {
        p.isLeader.Store(rd.SoftState.Lead == p.cfg.NodeID)
        p.leaderID.Store(rd.SoftState.Lead)
    }

    // 2. Persist entries and hard state.
    if err := p.storage.SaveReady(rd); err != nil {
        // Fatal: log and return. In production this should panic.
        return
    }

    // 3. Apply snapshot if present.
    if !raft.IsEmptySnap(rd.Snapshot) {
        p.applySnapshot(rd.Snapshot)
    }

    // 4. Send outbound messages.
    if p.sendFunc != nil && len(rd.Messages) > 0 {
        p.sendFunc(rd.Messages)
    }

    // 5. Apply committed entries.
    for _, e := range rd.CommittedEntries {
        if e.Type == raftpb.EntryConfChange || e.Type == raftpb.EntryConfChangeV2 {
            var cc raftpb.ConfChange
            if err := cc.Unmarshal(e.Data); err == nil {
                p.rawNode.ApplyConfChange(cc)
            }
            continue
        }

        if len(e.Data) == 0 {
            // Empty entry (leader election noop); skip.
            if cb, ok := p.pendingProposals[e.Index]; ok {
                cb(nil, nil)
                delete(p.pendingProposals, e.Index)
            }
            continue
        }

        // Decode and apply PDCommand.
        cmd, err := UnmarshalPDCommand(e.Data)
        if err != nil {
            if cb, ok := p.pendingProposals[e.Index]; ok {
                cb(nil, err)
                delete(p.pendingProposals, e.Index)
            }
            continue
        }

        result, applyErr := p.applyFunc(cmd)

        if cb, ok := p.pendingProposals[e.Index]; ok {
            cb(result, applyErr)
            delete(p.pendingProposals, e.Index)
        }
    }

    // 6. Advance Raft.
    p.rawNode.Advance(rd)
}
```

### Propose

Follows `Peer.propose()` at `internal/raftstore/peer.go:381`:

```go
func (p *PDRaftPeer) propose(proposal *PDProposal) {
    data, err := proposal.Command.Marshal()
    if err != nil {
        if proposal.Callback != nil {
            proposal.Callback(nil, err)
        }
        return
    }

    if err := p.rawNode.Propose(data); err != nil {
        if proposal.Callback != nil {
            proposal.Callback(nil, err)
        }
        return
    }

    // Track callback by expected index.
    lastIdx, _ := p.storage.LastIndex()
    expectedIdx := lastIdx + 1
    if proposal.Callback != nil {
        p.pendingProposals[expectedIdx] = proposal.Callback
    }
}
```

### ProposeAndWait

Synchronous helper for RPC handlers. Sends a proposal and blocks until the callback fires or context expires.

```go
func (p *PDRaftPeer) ProposeAndWait(ctx context.Context, cmd PDCommand) ([]byte, error) {
    ch := make(chan proposeResult, 1)
    proposal := &PDProposal{
        Command: cmd,
        Callback: func(result []byte, err error) {
            ch <- proposeResult{result: result, err: err}
        },
    }

    select {
    case p.mailbox <- PDRaftMsg{Type: PDRaftMsgTypeProposal, Data: proposal}:
    case <-ctx.Done():
        return nil, ctx.Err()
    }

    select {
    case r := <-ch:
        return r.result, r.err
    case <-ctx.Done():
        return nil, ctx.Err()
    }
}

type proposeResult struct {
    result []byte
    err    error
}
```

### Leader Accessors

```go
func (p *PDRaftPeer) IsLeader() bool       { return p.isLeader.Load() }
func (p *PDRaftPeer) LeaderID() uint64      { return p.leaderID.Load() }

func (p *PDRaftPeer) LeaderAddr() string {
    id := p.leaderID.Load()
    if id == 0 {
        return ""
    }
    return p.cfg.InitialCluster[id]
}
```

## 3. PDCommand Encoding (`internal/pd/command.go`)

### Command Type Enum

```go
type PDCommandType uint8

const (
    CmdSetBootstrapped   PDCommandType = iota + 1
    CmdPutStore
    CmdPutRegion
    CmdUpdateStoreStats
    CmdSetStoreState
    CmdTSOAllocate
    CmdIDAlloc
    CmdUpdateGCSafePoint
    CmdStartMove
    CmdAdvanceMove
    CmdCleanupStaleMove
)
```

### PDCommand Struct

```go
type PDCommand struct {
    Type PDCommandType `json:"type"`

    // CmdSetBootstrapped
    Bootstrapped *bool `json:"bootstrapped,omitempty"`

    // CmdPutStore
    Store *metapb.Store `json:"store,omitempty"`

    // CmdPutRegion
    Region *metapb.Region `json:"region,omitempty"`
    Leader *metapb.Peer   `json:"leader,omitempty"`

    // CmdUpdateStoreStats
    StoreID    uint64          `json:"store_id,omitempty"`
    StoreStats *pdpb.StoreStats `json:"store_stats,omitempty"`

    // CmdSetStoreState
    StoreState *StoreState `json:"store_state,omitempty"`

    // CmdTSOAllocate
    TSOBatchSize int `json:"tso_batch_size,omitempty"`

    // CmdIDAlloc
    IDBatchSize int `json:"id_batch_size,omitempty"`

    // CmdUpdateGCSafePoint
    GCSafePoint uint64 `json:"gc_safe_point,omitempty"`

    // CmdStartMove
    MoveRegionID      uint64       `json:"move_region_id,omitempty"`
    MoveSourcePeer    *metapb.Peer `json:"move_source_peer,omitempty"`
    MoveTargetStoreID uint64       `json:"move_target_store_id,omitempty"`

    // CmdAdvanceMove
    AdvanceRegion *metapb.Region `json:"advance_region,omitempty"`
    AdvanceLeader *metapb.Peer   `json:"advance_leader,omitempty"`

    // CmdCleanupStaleMove
    CleanupTimeout time.Duration `json:"cleanup_timeout,omitempty"`
}
```

### Serialization

Wire format: `[1 byte type][N bytes JSON payload]`

```go
func (c *PDCommand) Marshal() ([]byte, error) {
    payload, err := json.Marshal(c)
    if err != nil {
        return nil, fmt.Errorf("pd: marshal command: %w", err)
    }
    data := make([]byte, 1+len(payload))
    data[0] = byte(c.Type)
    copy(data[1:], payload)
    return data, nil
}

func UnmarshalPDCommand(data []byte) (PDCommand, error) {
    if len(data) < 1 {
        return PDCommand{}, errors.New("pd: empty command data")
    }
    var cmd PDCommand
    if err := json.Unmarshal(data[1:], &cmd); err != nil {
        return PDCommand{}, fmt.Errorf("pd: unmarshal command: %w", err)
    }
    cmd.Type = PDCommandType(data[0])
    return cmd, nil
}
```

JSON is chosen over protobuf for payload encoding because PD's write throughput is low (heartbeats, metadata updates) and the simplicity benefit outweighs any performance cost.

## 4. PDRaftStorage (`internal/pd/raft_storage.go`)

Modeled after `PeerStorage` in `internal/raftstore/storage.go`. Key simplifications:
- Uses `clusterID` instead of `regionID` for key construction.
- No snapshot state machine (PD snapshots are a simple full-state JSON dump).
- No async snapshot worker or `SnapState` tracking.

### Struct Definition

```go
type PDRaftStorage struct {
    mu sync.RWMutex

    engine    traits.KvEngine
    clusterID uint64

    hardState          raftpb.HardState
    applyState         ApplyState // reuse from internal/raftstore/storage.go
    entries            []raftpb.Entry
    persistedLastIndex uint64
    lastAppliedTerm    uint64 // tracks the term of the last applied entry, used for snapshot metadata
}

var _ raft.Storage = (*PDRaftStorage)(nil)
```

### Constructor

```go
func NewPDRaftStorage(clusterID uint64, engine traits.KvEngine) *PDRaftStorage {
    return &PDRaftStorage{
        clusterID: clusterID,
        engine:    engine,
        applyState: ApplyState{
            AppliedIndex:   RaftInitLogIndex,
            TruncatedIndex: RaftInitLogIndex,
            TruncatedTerm:  RaftInitLogTerm,
        },
        persistedLastIndex: RaftInitLogIndex,
    }
}
```

`RaftInitLogIndex` and `RaftInitLogTerm` are reused from `internal/raftstore/` constants (both equal to `5`).

### raft.Storage Interface (6 methods)

```go
func (s *PDRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error)
func (s *PDRaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
func (s *PDRaftStorage) Term(i uint64) (uint64, error)
func (s *PDRaftStorage) LastIndex() (uint64, error)
func (s *PDRaftStorage) FirstIndex() (uint64, error)
func (s *PDRaftStorage) Snapshot() (raftpb.Snapshot, error)
```

Implementations are identical to `PeerStorage` (same logic in `internal/raftstore/storage.go:68-189`), substituting `s.clusterID` for `s.regionID` in all `keys.RaftStateKey()` and `keys.RaftLogKey()` calls. The `Snapshot()` method returns a metadata-only snapshot (index and term from `applyState`); the actual snapshot data is handled separately via `PDSnapshot` (Section 9).

### Key Storage Layout

Uses the same `CF_RAFT` column family as KVS raftstore:
- Hard state: `keys.RaftStateKey(clusterID)` — format `[0x01][0x02][clusterID:8B BE][0x02]`
- Log entries: `keys.RaftLogKey(clusterID, index)` — format `[0x01][0x02][clusterID:8B BE][0x01][index:8B BE]`
- Apply state: `keys.ApplyStateKey(clusterID)` — format `[0x01][0x02][clusterID:8B BE][0x03]`

The `clusterID` occupies the `regionID` slot. Since PD uses its own separate RocksDB instance (in `<data-dir>/pd-raft/`), there is no collision with KVS region IDs.

### SaveReady

Same as `PeerStorage.SaveReady()` at `internal/raftstore/storage.go:206`:

```go
func (s *PDRaftStorage) SaveReady(rd raft.Ready) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    wb := s.engine.NewWriteBatch()

    if !raft.IsEmptyHardState(rd.HardState) {
        s.hardState = rd.HardState
        data, _ := s.hardState.Marshal()
        wb.Put(cfnames.CFRaft, keys.RaftStateKey(s.clusterID), data)
    }

    for i := range rd.Entries {
        data, _ := rd.Entries[i].Marshal()
        wb.Put(cfnames.CFRaft, keys.RaftLogKey(s.clusterID, rd.Entries[i].Index), data)
    }

    if err := wb.Commit(); err != nil {
        return fmt.Errorf("pd: commit raft ready: %w", err)
    }

    if len(rd.Entries) > 0 {
        s.appendToCache(rd.Entries)
        last := rd.Entries[len(rd.Entries)-1]
        if last.Index > s.persistedLastIndex {
            s.persistedLastIndex = last.Index
        }
    }

    return nil
}
```

### RecoverFromEngine

Same as `PeerStorage.RecoverFromEngine()` at `internal/raftstore/storage.go:296`, substituting `clusterID` for `regionID`.

## 5. Inter-PD Transport (`internal/pd/transport.go`)

Simpler than the KVS `RaftClient` in `internal/server/transport/transport.go` — no batching, no snapshot streaming, no connection pooling beyond a single connection per peer.

### Struct Definition

```go
type PDTransport struct {
    mu    sync.RWMutex
    conns map[uint64]*grpc.ClientConn // nodeID → gRPC connection
    addrs map[uint64]string           // nodeID → peer address
}
```

### Constructor

```go
func NewPDTransport(addrs map[uint64]string) *PDTransport {
    return &PDTransport{
        conns: make(map[uint64]*grpc.ClientConn),
        addrs: addrs,
    }
}
```

### gRPC Service for PD-to-PD Raft Messages

Rather than generating a new proto file, reuse the existing `raft_serverpb.RaftMessage` and `raft_serverpb.Done` types. Register a handler manually on the PD peer gRPC server:

```protobuf
// Conceptual — implemented via manual gRPC registration, not a new .proto file
service PDRaft {
    rpc SendRaftMessage(raft_serverpb.RaftMessage) returns (raft_serverpb.Done);
}
```

Alternatively, define a minimal proto service in `proto/pdraft.proto` if manual registration proves cumbersome. The decision is left to the implementer; both approaches are acceptable.

### Send

```go
func (t *PDTransport) Send(peerID uint64, msg raftpb.Message) error {
    conn, err := t.getOrDial(peerID)
    if err != nil {
        return err
    }

    // Convert etcd raftpb.Message → kvproto eraftpb.Message for wire format.
    eMsg, err := raftstore.RaftpbToEraftpb(&msg)
    if err != nil {
        return fmt.Errorf("pd transport: convert message: %w", err)
    }

    raftMsg := &raft_serverpb.RaftMessage{
        RegionId: 0, // unused for PD
        Message:  eMsg,
    }

    // Use the Raft unary RPC (no streaming needed for PD throughput).
    client := tikvpb.NewTikvClient(conn) // or custom PDRaft client
    _, err = client.Raft(context.TODO(), /* ... */)
    // Implementation detail: use the registered PDRaft service client.
    return err
}
```

The conversion uses `RaftpbToEraftpb` from `internal/raftstore/convert.go:24`.

### SendFunc Adapter

Wire `PDRaftPeer.sendFunc` to broadcast messages:

```go
func (t *PDTransport) MakeSendFunc() func([]raftpb.Message) {
    return func(msgs []raftpb.Message) {
        for i := range msgs {
            if err := t.Send(msgs[i].To, msgs[i]); err != nil {
                slog.Warn("pd transport: send failed",
                    "to", msgs[i].To, "type", msgs[i].Type, "err", err)
            }
        }
    }
}
```

### Receive Path

On the PD peer gRPC server, the registered handler converts inbound messages and delivers them to `PDRaftPeer.mailbox`:

```go
func (s *PDServer) handlePDRaftMessage(ctx context.Context, raftMsg *raft_serverpb.RaftMessage) error {
    msg, err := raftstore.EraftpbToRaftpb(raftMsg.GetMessage())
    if err != nil {
        return err
    }
    s.raftPeer.mailbox <- PDRaftMsg{
        Type: PDRaftMsgTypeRaftMessage,
        Data: &msg,
    }
    return nil
}
```

The conversion uses `EraftpbToRaftpb` from `internal/raftstore/convert.go:10`.

### Close

```go
func (t *PDTransport) Close() {
    t.mu.Lock()
    defer t.mu.Unlock()
    for _, conn := range t.conns {
        conn.Close()
    }
    t.conns = make(map[uint64]*grpc.ClientConn)
}
```

## 6. State Machine Apply (`internal/pd/apply.go`)

The `applyFunc` wired into `PDRaftPeer` dispatches each committed `PDCommand` to the corresponding sub-component method.

```go
func (s *PDServer) applyCommand(cmd PDCommand) ([]byte, error) {
    switch cmd.Type {
    case CmdSetBootstrapped:
        s.meta.SetBootstrapped(*cmd.Bootstrapped)
        return nil, nil

    case CmdPutStore:
        s.meta.PutStore(cmd.Store)
        return nil, nil

    case CmdPutRegion:
        s.meta.PutRegion(cmd.Region, cmd.Leader)
        return nil, nil

    case CmdUpdateStoreStats:
        s.meta.UpdateStoreStats(cmd.StoreID, cmd.StoreStats)
        return nil, nil

    case CmdSetStoreState:
        s.meta.SetStoreState(cmd.StoreID, *cmd.StoreState)
        return nil, nil

    case CmdTSOAllocate:
        ts, err := s.tso.Allocate(cmd.TSOBatchSize)
        if err != nil {
            return nil, err
        }
        data, _ := json.Marshal(ts)
        return data, nil

    case CmdIDAlloc:
        ids := make([]uint64, 0, cmd.IDBatchSize)
        for i := 0; i < cmd.IDBatchSize; i++ {
            ids = append(ids, s.idAlloc.Alloc())
        }
        data, _ := json.Marshal(ids)
        return data, nil

    case CmdUpdateGCSafePoint:
        newSP := s.gcMgr.UpdateSafePoint(cmd.GCSafePoint)
        data, _ := json.Marshal(newSP)
        return data, nil

    case CmdStartMove:
        s.moveTracker.StartMove(cmd.MoveRegionID, cmd.MoveSourcePeer, cmd.MoveTargetStoreID)
        return nil, nil

    case CmdAdvanceMove:
        result := s.moveTracker.Advance(cmd.MoveRegionID, cmd.AdvanceRegion, cmd.AdvanceLeader)
        if result != nil {
            data, _ := json.Marshal(result)
            return data, nil
        }
        return nil, nil

    case CmdCleanupStaleMove:
        s.moveTracker.CleanupStale(cmd.CleanupTimeout)
        return nil, nil

    default:
        return nil, fmt.Errorf("pd: unknown command type %d", cmd.Type)
    }
}
```

Every sub-component method (`PutStore`, `SetBootstrapped`, etc.) already exists in `internal/pd/server.go`. The apply function calls them directly. Since these methods hold their own mutexes, no additional locking is needed in `applyCommand` itself. All apply calls run sequentially on the `PDRaftPeer` goroutine, so there is no concurrent apply concern.

## 7. TSO and ID Pre-allocation

TSO and ID allocation are high-frequency operations. Per-request Raft consensus would add unacceptable latency. Both use a batch pre-allocation strategy.

### TSO Pre-allocation (`internal/pd/tso_buffer.go`)

```go
type TSOBuffer struct {
    mu       sync.Mutex
    physical int64
    logical  int64
    remain   int
    raftPeer *PDRaftPeer
}
```

**Flow**:
1. Leader proposes `CmdTSOAllocate{TSOBatchSize: 1000}` through Raft.
2. After commit, every node's state machine calls `tso.Allocate(1000)`, advancing the allocator by 1000 logical slots.
3. The leader's proposal callback receives the resulting `pdpb.Timestamp` and stores it in `TSOBuffer` as the upper bound.
4. Subsequent `Tso` streaming RPC calls on the leader are served from this buffer without Raft, decrementing `remain` and incrementing the returned logical value.
5. When `remain` reaches 0, the leader proposes another batch.
6. On leader change, the new leader proposes a fresh batch. The old leader's remaining buffer is discarded. Monotonicity is guaranteed because each new batch's starting point is determined by the committed Raft log — the TSO allocator state on all nodes is identical after applying the same log entries.

```go
func (b *TSOBuffer) GetTS(count int) (*pdpb.Timestamp, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.remain < count {
        // Trigger async refill via Raft proposal.
        // Block until refill completes or timeout.
        if err := b.refill(); err != nil {
            return nil, err
        }
    }

    ts := &pdpb.Timestamp{
        Physical: b.physical,
        Logical:  b.logical + int64(count),
    }
    b.logical += int64(count)
    b.remain -= count
    return ts, nil
}
```

### ID Pre-allocation (`internal/pd/id_buffer.go`)

```go
type IDBuffer struct {
    mu       sync.Mutex
    base     uint64 // start of allocated range
    end      uint64 // exclusive end of allocated range
    raftPeer *PDRaftPeer
}
```

**Flow**:
1. Leader proposes `CmdIDAlloc{IDBatchSize: 100}` through Raft.
2. After commit, every node's state machine calls `idAlloc.Alloc()` 100 times, advancing the allocator's `nextID` by 100.
3. The leader's proposal callback receives the allocated IDs and stores the range `[base, base+100)` in `IDBuffer`.
4. Subsequent `AllocID` and `AskBatchSplit` RPC calls on the leader are served from this buffer.
5. When depleted, the leader proposes another batch.
6. On leader change, the new leader proposes a fresh batch.

```go
func (b *IDBuffer) Alloc() (uint64, error) {
    b.mu.Lock()
    defer b.mu.Unlock()

    if b.base >= b.end {
        if err := b.refill(); err != nil {
            return 0, err
        }
    }

    id := b.base
    b.base++
    return id, nil
}
```

## 8. Leader Forwarding (`internal/pd/forward.go`)

Non-leader PD nodes forward mutating RPCs to the leader.

### ForwardToLeader

```go
func (s *PDServer) forwardToLeader(ctx context.Context, method string, req proto.Message) (proto.Message, error) {
    leaderID := s.raftPeer.LeaderID()
    if leaderID == 0 {
        return nil, status.Error(codes.Unavailable, "no PD leader elected")
    }
    leaderAddr, ok := s.cfg.RaftConfig.ClientAddrs[leaderID]
    if !ok {
        return nil, status.Errorf(codes.Unavailable, "no client address for leader %d", leaderID)
    }

    conn, err := grpc.DialContext(ctx, leaderAddr,
        grpc.WithTransportCredentials(insecure.NewCredentials()),
        grpc.WithBlock(),
    )
    if err != nil {
        return nil, status.Errorf(codes.Unavailable, "pd: dial leader %s: %v", leaderAddr, err)
    }
    defer conn.Close()

    client := pdpb.NewPDClient(conn)
    return s.routeToLeader(ctx, client, method, req)
}
```

### routeToLeader

Routes by method name to the correct `pdpb.PDClient` method:

```go
func (s *PDServer) routeToLeader(ctx context.Context, client pdpb.PDClient, method string, req proto.Message) (proto.Message, error) {
    switch method {
    case "Bootstrap":
        return client.Bootstrap(ctx, req.(*pdpb.BootstrapRequest))
    case "PutStore":
        return client.PutStore(ctx, req.(*pdpb.PutStoreRequest))
    case "AllocID":
        return client.AllocID(ctx, req.(*pdpb.AllocIDRequest))
    case "StoreHeartbeat":
        return client.StoreHeartbeat(ctx, req.(*pdpb.StoreHeartbeatRequest))
    case "UpdateGCSafePoint":
        return client.UpdateGCSafePoint(ctx, req.(*pdpb.UpdateGCSafePointRequest))
    case "ReportBatchSplit":
        return client.ReportBatchSplit(ctx, req.(*pdpb.ReportBatchSplitRequest))
    case "AskBatchSplit":
        return client.AskBatchSplit(ctx, req.(*pdpb.AskBatchSplitRequest))
    default:
        return nil, status.Errorf(codes.Unimplemented, "pd: unknown method %s", method)
    }
}
```

Streaming RPCs (`Tso`, `RegionHeartbeat`) require special handling: the follower opens a streaming connection to the leader and proxies messages bidirectionally. This is described separately in the streaming forwarding section below.

### Streaming Forward (Tso)

```go
func (s *PDServer) forwardTsoStream(inStream pdpb.PD_TsoServer) error {
    leaderID := s.raftPeer.LeaderID()
    if leaderID == 0 {
        return status.Error(codes.Unavailable, "no PD leader")
    }
    leaderAddr, ok := s.cfg.RaftConfig.ClientAddrs[leaderID]
    if !ok {
        return status.Errorf(codes.Unavailable, "no client address for leader %d", leaderID)
    }

    conn, err := grpc.Dial(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    if err != nil {
        return err
    }
    defer conn.Close()

    client := pdpb.NewPDClient(conn)
    outStream, err := client.Tso(inStream.Context())
    if err != nil {
        return err
    }

    // Bidirectional proxy: forward requests to leader, relay responses to client.
    for {
        req, err := inStream.Recv()
        if err != nil {
            return err
        }
        if err := outStream.Send(req); err != nil {
            return err
        }
        resp, err := outStream.Recv()
        if err != nil {
            return err
        }
        if err := inStream.Send(resp); err != nil {
            return err
        }
    }
}
```

### Read RPCs

Read-only RPCs (`GetStore`, `GetRegion`, `GetRegionByID`, `GetAllStores`, `IsBootstrapped`, `GetMembers`, `GetGCSafePoint`) are served locally from any node without forwarding. This provides stale-read semantics, which is acceptable because:
- Region and store metadata changes propagate via Raft with minimal lag (sub-second).
- Clients that need strong consistency (e.g., TSO) always go to the leader.

## 9. Snapshot (`internal/pd/snapshot.go`)

### PDSnapshot Struct

```go
type PDSnapshot struct {
    Bootstrapped       bool                       `json:"bootstrapped"`
    Stores             map[uint64]*metapb.Store   `json:"stores"`
    Regions            map[uint64]*metapb.Region  `json:"regions"`
    Leaders            map[uint64]*metapb.Peer    `json:"leaders"`
    StoreStats         map[uint64]*pdpb.StoreStats `json:"store_stats"`
    StoreStates        map[uint64]StoreState       `json:"store_states"`
    StoreLastHeartbeat map[uint64]int64            `json:"store_last_heartbeat"` // Unix millis
    TSOPhysical        int64                       `json:"tso_physical"`
    TSOLogical         int64                       `json:"tso_logical"`
    NextID             uint64                      `json:"next_id"`
    GCSafePoint        uint64                      `json:"gc_safe_point"`
    Moves              map[uint64]*PendingMove     `json:"moves"`
}
```

### Snapshot Generation

```go
func (s *PDServer) generateSnapshot() ([]byte, error) {
    snap := PDSnapshot{
        Bootstrapped:       s.meta.IsBootstrapped(),
        Stores:             s.meta.cloneStores(),
        Regions:            s.meta.cloneRegions(),
        Leaders:            s.meta.cloneLeaders(),
        StoreStats:         s.meta.cloneStoreStats(),
        StoreStates:        s.meta.cloneStoreStates(),
        StoreLastHeartbeat: s.meta.cloneHeartbeatTimestamps(),
        TSOPhysical:        s.tso.getPhysical(),
        TSOLogical:         s.tso.getLogical(),
        NextID:             s.idAlloc.getNextID(),
        GCSafePoint:        s.gcMgr.GetSafePoint(),
        Moves:              s.moveTracker.cloneMoves(),
    }
    return json.Marshal(snap)
}
```

Each sub-component needs a `clone*()` method that acquires its mutex, copies the map, and returns. These are small additions to the existing types in `internal/pd/server.go` and `internal/pd/move_tracker.go`.

The `TSOAllocator` and `IDAllocator` need getter methods (`getPhysical`, `getLogical`, `getNextID`) that acquire their mutex and return the current value.

### Snapshot Application

```go
func (s *PDServer) applySnapshot(snap PDSnapshot) {
    // Acquire all mutexes in a fixed order to avoid deadlocks.
    s.meta.mu.Lock()
    s.tso.mu.Lock()
    s.idAlloc.mu.Lock()
    s.gcMgr.mu.Lock()
    s.moveTracker.mu.Lock()

    defer s.moveTracker.mu.Unlock()
    defer s.gcMgr.mu.Unlock()
    defer s.idAlloc.mu.Unlock()
    defer s.tso.mu.Unlock()
    defer s.meta.mu.Unlock()

    // Replace MetadataStore state.
    s.meta.bootstrapped = snap.Bootstrapped
    s.meta.stores = snap.Stores
    s.meta.regions = snap.Regions
    s.meta.leaders = snap.Leaders
    s.meta.storeStats = snap.StoreStats
    s.meta.storeStates = snap.StoreStates
    for k, v := range snap.StoreLastHeartbeat {
        s.meta.storeLastHeartbeat[k] = time.UnixMilli(v)
    }

    // Replace TSO state.
    s.tso.physical = snap.TSOPhysical
    s.tso.logical = snap.TSOLogical

    // Replace ID allocator state.
    s.idAlloc.nextID = snap.NextID

    // Replace GC safe point.
    s.gcMgr.safePoint = snap.GCSafePoint

    // Replace MoveTracker state.
    s.moveTracker.moves = snap.Moves
}
```

### Raft Snapshot Integration

`PDRaftPeer.applySnapshot` is called from `handleReady()` when `rd.Snapshot` is non-empty:

```go
func (p *PDRaftPeer) applySnapshot(snap raftpb.Snapshot) {
    // Decode PDSnapshot from snapshot data.
    var pdSnap PDSnapshot
    if err := json.Unmarshal(snap.Data, &pdSnap); err != nil {
        slog.Error("pd: failed to unmarshal snapshot", "err", err)
        return
    }

    // Apply to state machine (applyFunc is *not* used here; direct application).
    // The PDServer reference is captured via closure or interface.
    p.snapshotApplyFunc(pdSnap)

    // Update storage state to match snapshot metadata.
    p.storage.mu.Lock()
    p.storage.applyState.AppliedIndex = snap.Metadata.Index
    p.storage.applyState.TruncatedIndex = snap.Metadata.Index
    p.storage.applyState.TruncatedTerm = snap.Metadata.Term
    p.storage.entries = nil // clear entry cache
    p.storage.mu.Unlock()
}
```

`PDRaftPeer` has an additional function field for snapshot application:

```go
snapshotApplyFunc func(PDSnapshot) // wired to PDServer.applySnapshot
```

For snapshot generation (when a follower is far behind), the `PDRaftStorage.Snapshot()` method is extended to produce a full `raftpb.Snapshot` with data:

```go
func (s *PDRaftStorage) Snapshot() (raftpb.Snapshot, error) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    data, err := s.snapGenFunc() // wired to PDServer.generateSnapshot
    if err != nil {
        return raftpb.Snapshot{}, err
    }

    // NOTE: For correctness, Term should be the term of the last applied entry,
    // not TruncatedTerm (which is the term of the last truncated entry).
    // Use s.lastAppliedTerm which tracks the term of each entry as it is applied.
    return raftpb.Snapshot{
        Data: data,
        Metadata: raftpb.SnapshotMetadata{
            Index: s.applyState.AppliedIndex,
            Term:  s.lastAppliedTerm,
        },
    }, nil
}
```

`PDRaftStorage` has an additional function field:

```go
snapGenFunc func() ([]byte, error) // wired to PDServer.generateSnapshot
```

## 10. Server Integration (`internal/pd/server.go`)

### Modified PDServer Struct

```go
type PDServer struct {
    pdpb.UnimplementedPDServer

    cfg       PDServerConfig
    clusterID uint64

    tso         *TSOAllocator
    meta        *MetadataStore
    idAlloc     *IDAllocator
    gcMgr       *GCSafePointManager
    scheduler   *Scheduler
    moveTracker *MoveTracker

    grpcServer *grpc.Server  // client-facing gRPC (port 2379)
    listener   net.Listener

    // Raft replication (nil in single-node mode).
    raftPeer    *PDRaftPeer
    raftStorage *PDRaftStorage
    transport   *PDTransport
    tsoBuffer   *TSOBuffer
    idBuffer    *IDBuffer
    peerServer  *grpc.Server  // PD-peer gRPC (port 2380)
    peerListener net.Listener

    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
}
```

### Modified PDServerConfig

```go
type PDServerConfig struct {
    ListenAddr string // client gRPC address (default "0.0.0.0:2379")
    PeerAddr   string // peer gRPC address (default "0.0.0.0:2380")
    DataDir    string
    ClusterID  uint64

    // Raft cluster configuration. nil = single-node mode (no replication).
    RaftConfig *PDRaftConfig

    TSOSaveInterval           time.Duration
    TSOUpdatePhysicalInterval time.Duration
    MaxPeerCount              int
    StoreDisconnectDuration   time.Duration
    StoreDownDuration         time.Duration
    RegionBalanceThreshold    float64
    RegionBalanceRateLimit    int
}
```

### Startup Flow (Replicated Mode)

When `cfg.RaftConfig` is non-nil:

```go
func (s *PDServer) Start() error {
    if s.cfg.RaftConfig != nil {
        if err := s.startRaft(); err != nil {
            return fmt.Errorf("pd: start raft: %w", err)
        }
    }

    // Start client gRPC server (same as today).
    lis, err := net.Listen("tcp", s.cfg.ListenAddr)
    // ...
    s.grpcServer.Serve(lis)

    // Start store state worker.
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        s.runStoreStateWorker(s.ctx)
    }()

    return nil
}

func (s *PDServer) startRaft() error {
    // 1. Create RocksDB engine in <data-dir>/pd-raft/.
    raftDir := filepath.Join(s.cfg.DataDir, "pd-raft")
    engine, err := rocks.Open(raftDir, cfnames.AllCFs)
    if err != nil {
        return err
    }

    // 2. Create PDRaftStorage.
    s.raftStorage = NewPDRaftStorage(s.clusterID, engine)

    // 3. Determine if this is a fresh bootstrap or restart.
    var peers []raft.Peer
    if !HasPersistedRaftState(engine, s.clusterID) {
        // Fresh bootstrap: build initial peer list.
        for nodeID := range s.cfg.RaftConfig.InitialCluster {
            peers = append(peers, raft.Peer{ID: nodeID})
        }
    }

    // 4. Create PDRaftPeer.
    s.raftPeer, err = NewPDRaftPeer(
        s.raftStorage,
        peers,
        *s.cfg.RaftConfig,
    )
    if err != nil {
        return err
    }

    // 5. Create PDTransport.
    s.transport = NewPDTransport(s.cfg.RaftConfig.InitialCluster)

    // 6. Wire functions.
    s.raftPeer.sendFunc = s.transport.MakeSendFunc()
    s.raftPeer.applyFunc = s.applyCommand
    s.raftPeer.snapshotApplyFunc = s.applySnapshot
    s.raftStorage.snapGenFunc = s.generateSnapshot

    // 7. Create pre-allocation buffers.
    s.tsoBuffer = NewTSOBuffer(s.raftPeer)
    s.idBuffer = NewIDBuffer(s.raftPeer)

    // 8. Start PDRaftPeer event loop.
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        s.raftPeer.Run(s.ctx)
    }()

    // 9. Start peer gRPC server.
    s.peerServer = grpc.NewServer()
    registerPDRaftService(s.peerServer, s) // register SendRaftMessage handler
    peerLis, err := net.Listen("tcp", s.cfg.PeerAddr)
    if err != nil {
        return err
    }
    s.peerListener = peerLis
    s.wg.Add(1)
    go func() {
        defer s.wg.Done()
        s.peerServer.Serve(peerLis)
    }()

    return nil
}
```

### Startup Flow (Single-Node Mode)

When `cfg.RaftConfig` is nil, `raftPeer` remains nil. All RPCs operate directly on the sub-components, exactly as today. No behavioral change.

### Shutdown

```go
func (s *PDServer) Stop() {
    s.cancel()
    s.grpcServer.GracefulStop()
    if s.peerServer != nil {
        s.peerServer.GracefulStop()
    }
    if s.transport != nil {
        s.transport.Close()
    }
    s.wg.Wait()
}
```

## 11. RPC Handler Pattern

### Write RPCs (Replicated Mode)

Each mutating RPC handler follows this pattern:

```go
func (s *PDServer) PutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
    if s.raftPeer == nil {
        // Single-node mode: apply directly (existing behavior).
        s.meta.PutStore(req.GetStore())
        return &pdpb.PutStoreResponse{Header: s.header()}, nil
    }

    if !s.raftPeer.IsLeader() {
        resp, err := s.forwardToLeader(ctx, "PutStore", req)
        if err != nil {
            return nil, err
        }
        return resp.(*pdpb.PutStoreResponse), nil
    }

    // Leader: propose through Raft.
    cmd := PDCommand{Type: CmdPutStore, Store: req.GetStore()}
    _, err := s.raftPeer.ProposeAndWait(ctx, cmd)
    if err != nil {
        return nil, err
    }
    return &pdpb.PutStoreResponse{Header: s.header()}, nil
}
```

### Mutating RPCs to Replicate

| RPC | Command Type | Notes |
|-----|-------------|-------|
| `Bootstrap` | `CmdSetBootstrapped` + `CmdPutStore` + `CmdPutRegion` | Multiple commands in sequence |
| `PutStore` | `CmdPutStore` | |
| `StoreHeartbeat` | `CmdUpdateStoreStats` | |
| `RegionHeartbeat` | `CmdPutRegion` | Streaming; each message is a separate proposal |
| `AllocID` | Served from `IDBuffer` | No per-call Raft |
| `AskBatchSplit` | Served from `IDBuffer` | No per-call Raft |
| `Tso` | Served from `TSOBuffer` | No per-call Raft |
| `ReportBatchSplit` | `CmdPutRegion` (per region) | |
| `UpdateGCSafePoint` | `CmdUpdateGCSafePoint` | |

### Read RPCs (Local)

| RPC | Source | Notes |
|-----|--------|-------|
| `GetMembers` | Local (leader info from `raftPeer`) | |
| `IsBootstrapped` | Local `MetadataStore` | |
| `GetStore` | Local `MetadataStore` | |
| `GetAllStores` | Local `MetadataStore` | |
| `GetRegion` | Local `MetadataStore` | |
| `GetRegionByID` | Local `MetadataStore` | |
| `GetGCSafePoint` | Local `GCSafePointManager` | |

### GetMembers Update

In replicated mode, `GetMembers` returns information about all PD cluster members and identifies the current leader:

```go
func (s *PDServer) GetMembers(ctx context.Context, req *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
    resp := &pdpb.GetMembersResponse{Header: s.header()}

    if s.raftPeer == nil {
        // Single-node mode (existing behavior).
        resp.Leader = &pdpb.Member{
            Name:       "gookv-pd-1",
            ClientUrls: []string{"http://" + s.Addr()},
        }
        return resp, nil
    }

    // Replicated mode: return all members and identify the leader.
    leaderID := s.raftPeer.LeaderID()
    for nodeID, addr := range s.raftPeer.cfg.InitialCluster {
        member := &pdpb.Member{
            Name:       fmt.Sprintf("gookv-pd-%d", nodeID),
            MemberId:   nodeID,
            PeerUrls:   []string{"http://" + addr},
        }
        resp.Members = append(resp.Members, member)
        if nodeID == leaderID {
            resp.Leader = member
        }
    }

    return resp, nil
}
```

## 12. Store State Worker

`runStoreStateWorker` is modified to only perform mutations when this node is the Raft leader (or when running in single-node mode).

```go
func (s *PDServer) runStoreStateWorker(ctx context.Context) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            if s.raftPeer != nil && !s.raftPeer.IsLeader() {
                continue // only the leader runs state transitions
            }

            if s.raftPeer == nil {
                // Single-node mode: apply directly (existing behavior).
                s.meta.updateStoreStates()
                if s.moveTracker != nil {
                    s.moveTracker.CleanupStale(10 * time.Minute)
                }
            } else {
                // Replicated mode: compute transitions and propose via Raft.
                transitions := s.meta.computeStoreStateTransitions()
                for storeID, newState := range transitions {
                    cmd := PDCommand{
                        Type:       CmdSetStoreState,
                        StoreID:    storeID,
                        StoreState: &newState,
                    }
                    s.raftPeer.ProposeAndWait(ctx, cmd)
                }

                cmd := PDCommand{
                    Type:           CmdCleanupStaleMove,
                    CleanupTimeout: 10 * time.Minute,
                }
                s.raftPeer.ProposeAndWait(ctx, cmd)
            }
        }
    }
}
```

A new helper method on `MetadataStore` computes the transitions without applying them:

```go
func (m *MetadataStore) computeStoreStateTransitions() map[uint64]StoreState {
    m.mu.RLock()
    defer m.mu.RUnlock()

    transitions := make(map[uint64]StoreState)
    now := m.now()
    for storeID := range m.stores {
        current := m.storeStates[storeID]
        if current == StoreStateTombstone {
            continue
        }
        t, ok := m.storeLastHeartbeat[storeID]
        if !ok {
            if current != StoreStateDown {
                transitions[storeID] = StoreStateDown
            }
            continue
        }
        elapsed := now.Sub(t)
        var newState StoreState
        switch {
        case elapsed >= m.downDuration:
            newState = StoreStateDown
        case elapsed >= m.disconnectDuration:
            newState = StoreStateDisconnected
        default:
            newState = StoreStateUp
        }
        if newState != current {
            transitions[storeID] = newState
        }
    }
    return transitions
}
```

This ensures state transitions are proposed through Raft and applied consistently on all nodes, rather than being computed independently (which could diverge due to clock skew).
