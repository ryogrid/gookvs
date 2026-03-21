# PD Server Raft Replication: Implementation Plan

This document specifies the implementation steps for adding Raft-based replication to the PD server. Each step lists exact files, function signatures, reuse references, LOC estimates, verification criteria, and dependencies.

## Phase 1: PD Raft Core (~800 LOC)

### Step 1: PD Command Encoding

**Files**: `internal/pd/command.go` (~200 LOC)

**What**:
- Define `PDCommandType uint8` enum with 11 constants:
  ```go
  const (
      CmdSetBootstrapped  PDCommandType = iota + 1 // 1
      CmdPutStore                                    // 2
      CmdPutRegion                                   // 3
      CmdUpdateStoreStats                            // 4
      CmdSetStoreState                               // 5
      CmdTSOAllocate                                 // 6
      CmdIDAlloc                                     // 7
      CmdUpdateGCSafePoint                           // 8
      CmdStartMove                                   // 9
      CmdAdvanceMove                                 // 10
      CmdCleanupStaleMove                            // 11
  )
  ```
- Define `PDCommand` struct:
  ```go
  type PDCommand struct {
      Type PDCommandType `json:"type"`

      // Payload fields (only the relevant field is non-nil for each type).
      Bootstrapped    *bool                    `json:"bootstrapped,omitempty"`
      Store           *metapb.Store            `json:"store,omitempty"`
      Region          *metapb.Region           `json:"region,omitempty"`
      Leader          *metapb.Peer             `json:"leader,omitempty"`
      StoreStats      *pdpb.StoreStats         `json:"store_stats,omitempty"`
      StoreID         uint64                   `json:"store_id,omitempty"`
      StoreState      *StoreState              `json:"store_state,omitempty"`
      TSOBatchSize    int                      `json:"tso_batch_size,omitempty"`
      IDBatchSize     int                      `json:"id_batch_size,omitempty"`
      GCSafePoint     uint64                   `json:"gc_safe_point,omitempty"`
      MoveRegionID    uint64                   `json:"move_region_id,omitempty"`
      MoveSourcePeer  *metapb.Peer             `json:"move_source_peer,omitempty"`
      MoveTargetStoreID uint64                 `json:"move_target_store_id,omitempty"`
      AdvanceRegion   *metapb.Region           `json:"advance_region,omitempty"`
      AdvanceLeader   *metapb.Peer             `json:"advance_leader,omitempty"`
      CleanupTimeout  time.Duration            `json:"cleanup_timeout,omitempty"`
  }
  ```
- Implement:
  ```go
  func (c *PDCommand) Marshal() ([]byte, error)
  func UnmarshalPDCommand(data []byte) (PDCommand, error)
  ```
- Encoding format: 1-byte type prefix + JSON payload. This is simple and adequate for PD's low throughput (~100s of ops/sec max).

**Reuse**: Pattern from `ModifiesToRequests` / `RequestsToModifies` in `internal/server/raftcmd.go` (type-switch based serialization). The PD version is simpler because it uses JSON instead of protobuf for the payload.

**Verification**: Unit tests in `internal/pd/command_test.go` for round-trip serialization of all 11 command types.

**Dependencies**: None.

---

### Step 2: PD Raft Storage

**Files**: `internal/pd/raft_storage.go` (~250 LOC)

**What**:
- Define `PDRaftStorage` struct:
  ```go
  type PDRaftStorage struct {
      mu                 sync.RWMutex
      clusterID          uint64
      engine             traits.KvEngine
      hardState          raftpb.HardState
      applyState         ApplyState  // reuse raftstore.ApplyState type
      entries            []raftpb.Entry
      persistedLastIndex uint64
  }
  ```
  Use `clusterID` (not `regionID`) as the key namespace discriminator. Raft log keys use `keys.RaftLogKey(clusterID, logIndex)`, hard state uses `keys.RaftStateKey(clusterID)`.

- Implement `raft.Storage` interface (6 methods):
  ```go
  func (s *PDRaftStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error)
  func (s *PDRaftStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error)
  func (s *PDRaftStorage) Term(i uint64) (uint64, error)
  func (s *PDRaftStorage) LastIndex() (uint64, error)
  func (s *PDRaftStorage) FirstIndex() (uint64, error)
  func (s *PDRaftStorage) Snapshot() (raftpb.Snapshot, error)
  ```

- Implement persistence methods:
  ```go
  func (s *PDRaftStorage) SaveReady(rd raft.Ready) error
  func (s *PDRaftStorage) RecoverFromEngine() error
  func (s *PDRaftStorage) SetApplyState(state ApplyState)
  func (s *PDRaftStorage) GetApplyState() ApplyState
  func (s *PDRaftStorage) SetDummyEntry()
  func (s *PDRaftStorage) SetPersistedLastIndex(idx uint64)
  func (s *PDRaftStorage) AppliedIndex() uint64
  ```

- `SaveReady` writes entries and hard state atomically via `engine.NewWriteBatch()` + `wb.Put(cfnames.CFRaft, ...)` + `wb.Commit()`, then updates in-memory cache.
- `RecoverFromEngine` scans `keys.RaftLogKeyRange(clusterID)` to rebuild entry cache, and reads `keys.RaftStateKey(clusterID)` for hard state.

**Reuse**: Direct model on `PeerStorage` in `internal/raftstore/storage.go`:
- `SaveReady()` at line 206 (same WriteBatch pattern)
- `RecoverFromEngine()` at line 296 (same scan + unmarshal pattern)
- `Entries()` at line 75 (same cache-first, engine-fallback logic)
- `Term()` at line 115 (same cache lookup + engine fallback)

The key difference: `PDRaftStorage` uses `clusterID` instead of `regionID` in all key functions. No snapshot worker integration (snapshots are handled differently in Phase 4).

**Verification**: Unit tests in `internal/pd/raft_storage_test.go`:
- SaveReady + read back via Entries/Term
- RecoverFromEngine round-trip
- Entry cache behavior and size-limited reads

**Dependencies**: None.

---

### Step 3: PD Raft Peer

**Files**: `internal/pd/raft_peer.go` (~250 LOC)

**What**:
- Define `PDRaftPeer` struct:
  ```go
  type PDRaftPeer struct {
      nodeID    uint64
      rawNode   *raft.RawNode
      storage   *PDRaftStorage
      peerAddrs map[uint64]string
      Mailbox   chan PDRaftMsg
      sendFunc  func([]raftpb.Message)

      pendingProposals map[uint64]func([]byte, error)

      applyFunc func(PDCommand) ([]byte, error)  // called for committed entries

      isLeader  atomic.Bool
      leaderID  atomic.Uint64
      stopped   atomic.Bool

      cfg PDRaftConfig
  }
  ```

- Define `PDRaftMsg` (simplified version of `raftstore.PeerMsg`):
  ```go
  type PDRaftMsgType int
  const (
      PDRaftMsgTypeRaftMessage PDRaftMsgType = iota
      PDRaftMsgTypeProposal
  )

  type PDRaftMsg struct {
      Type PDRaftMsgType
      Data interface{}
  }

  type PDProposal struct {
      Command  PDCommand
      Callback func([]byte, error)
  }
  ```

- Define `PDRaftConfig`:
  ```go
  type PDRaftConfig struct {
      RaftTickInterval     time.Duration  // default 100ms
      ElectionTimeoutTicks int            // default 10
      HeartbeatTicks       int            // default 2
      MaxInflightMsgs      int            // default 256
      MaxSizePerMsg        uint64         // default 1MiB
      MailboxCapacity      int            // default 256
  }
  ```

- Implement constructor:
  ```go
  func NewPDRaftPeer(
      nodeID uint64,
      storage *PDRaftStorage,
      peers []raft.Peer,  // nil for restart
      peerAddrs map[uint64]string,
      cfg PDRaftConfig,
  ) (*PDRaftPeer, error)
  ```
  Creates `raft.RawNode` with the config. If `peers` is non-nil, calls `rawNode.Bootstrap(peers)`. If nil, storage must have been recovered via `RecoverFromEngine()`.

- Implement event loop:
  ```go
  func (p *PDRaftPeer) Run(ctx context.Context)
  ```
  Select loop: ticker → `rawNode.Tick()`, mailbox → `handleMessage()`. After every select case, call `handleReady()`.

- Implement ready handling:
  ```go
  func (p *PDRaftPeer) handleReady()
  ```
  Same structure as `Peer.handleReady()` in `internal/raftstore/peer.go` line 415:
  1. Check `rawNode.HasReady()`
  2. Get `rd := rawNode.Ready()`
  3. Update leader status from `rd.SoftState`
  4. Call `storage.SaveReady(rd)`
  5. Send messages via `sendFunc`
  6. Apply committed entries (unmarshal `PDCommand`, call `applyFunc`, invoke pending proposal callbacks)
  7. Call `rawNode.Advance(rd)`

- Implement proposal:
  ```go
  func (p *PDRaftPeer) ProposeAndWait(ctx context.Context, cmd PDCommand) ([]byte, error)
  ```
  Creates a callback channel, sends `PDProposal` to mailbox, blocks on callback or ctx.Done(). Returns `ErrNotLeader` if not leader (checked before proposing).

- Implement accessors:
  ```go
  func (p *PDRaftPeer) IsLeader() bool
  func (p *PDRaftPeer) LeaderID() uint64
  func (p *PDRaftPeer) SetSendFunc(f func([]raftpb.Message))
  func (p *PDRaftPeer) SetApplyFunc(f func(PDCommand) ([]byte, error))
  ```

**Reuse**: Direct model on `Peer` in `internal/raftstore/peer.go`:
- `NewPeer()` at line 127 (same RawNode creation + Bootstrap pattern)
- `Run()` at line 232 (same ticker + mailbox select loop)
- `handleReady()` at line 415 (same 7-step ready processing)
- `propose()` at line 381 (same proposal tracking by index)
- `pendingProposals` map at line 99 (same callback-by-index pattern)

Key simplifications vs. `Peer`:
- No region management, no conf change handling, no split checks, no log GC ticks, no PD heartbeat ticks
- `applyFunc` directly returns `([]byte, error)` instead of going through an apply worker
- Committed entries are applied inline in `handleReady()`, not via a separate goroutine

**Verification**: Unit test in `internal/pd/raft_peer_test.go`:
- Create 3 in-process `PDRaftPeer` instances with in-memory engines
- Wire `sendFunc` to route messages between peers via mailboxes
- Wait for leader election
- Propose a `CmdPutStore` command on leader, verify callback fires

**Dependencies**: Steps 1, 2.

---

### Step 4: Apply Function

**Files**: `internal/pd/apply.go` (~100 LOC)

**What**:
- Implement the state machine apply function:
  ```go
  func (s *PDServer) applyCommand(cmd PDCommand) ([]byte, error)
  ```
  Switch on `cmd.Type`, call the corresponding method on `s.meta`, `s.tso`, `s.idAlloc`, `s.gcMgr`, or `s.moveTracker`:

  | Command Type | Action | Return Value |
  |---|---|---|
  | `CmdSetBootstrapped` | `s.meta.SetBootstrapped(*cmd.Bootstrapped)` | nil |
  | `CmdPutStore` | `s.meta.PutStore(cmd.Store)` | nil |
  | `CmdPutRegion` | `s.meta.PutRegion(cmd.Region, cmd.Leader)` | nil |
  | `CmdUpdateStoreStats` | `s.meta.UpdateStoreStats(cmd.StoreID, cmd.StoreStats)` | nil |
  | `CmdSetStoreState` | `s.meta.SetStoreState(cmd.StoreID, *cmd.StoreState)` | nil |
  | `CmdTSOAllocate` | `ts, err := s.tso.Allocate(cmd.TSOBatchSize)` | JSON-encoded `*pdpb.Timestamp` |
  | `CmdIDAlloc` | `id := s.idAlloc.Alloc()` | 8-byte big-endian uint64 |
  | `CmdUpdateGCSafePoint` | `newSP := s.gcMgr.UpdateSafePoint(cmd.GCSafePoint)` | 8-byte big-endian uint64 |
  | `CmdStartMove` | `s.moveTracker.StartMove(cmd.MoveRegionID, cmd.MoveSourcePeer, cmd.MoveTargetStoreID)` | nil |
  | `CmdAdvanceMove` | `s.moveTracker.Advance(cmd.MoveRegionID, cmd.AdvanceRegion, cmd.AdvanceLeader)` | nil |
  | `CmdCleanupStaleMove` | `s.moveTracker.CleanupStale(cmd.CleanupTimeout)` | nil |

**Reuse**: Pattern analogous to `RequestsToModifies` in `internal/server/raftcmd.go` line 48 (type-switch dispatching to state-modifying operations).

**Verification**: Unit test in `internal/pd/apply_test.go`:
- Create a `PDServer` (single-node, no Raft)
- Call `applyCommand` for each of the 11 command types
- Verify side effects via corresponding getter methods

**Dependencies**: Step 1.

---

## Phase 2: PD-to-PD Transport (~300 LOC)

### Step 5: PD Peer gRPC Service

**Files**: `internal/pd/peer_service.go` (~80 LOC)

**What**:
- Define a gRPC service for receiving PD Raft messages. Since there is no pre-generated `.proto` for PD-to-PD communication, reuse the existing `raft_serverpb.RaftMessage` proto type and register a custom gRPC service inline:
  ```go
  type PDPeerService struct {
      peer *PDRaftPeer
  }

  func (s *PDPeerService) SendRaftMessage(
      stream grpc.ServerStream,  // or use unary: ctx, *raft_serverpb.RaftMessage
  ) error
  ```

  For simplicity, use a **unary RPC** that receives a single `raft_serverpb.RaftMessage`:
  ```go
  func RegisterPDPeerService(srv *grpc.Server, peer *PDRaftPeer)
  ```
  This registers a hand-coded gRPC service descriptor (using `grpc.ServiceDesc`) with a single unary method `SendPDRaftMessage`.

- Handler implementation:
  1. Receive `raft_serverpb.RaftMessage`
  2. Convert to `raftpb.Message` via `raftstore.EraftpbToRaftpb()` from `internal/raftstore/convert.go` line 10
  3. Send to `peer.Mailbox` as `PDRaftMsg{Type: PDRaftMsgTypeRaftMessage, Data: &raftpbMsg}`

**Reuse**: Pattern from `tikvService.Raft()` in `internal/server/server.go` line 1426:
- Same receive-convert-dispatch flow
- Uses `raftstore.EraftpbToRaftpb()` for eraftpb→raftpb conversion

**Verification**: Integration test with Step 7.

**Dependencies**: Step 3.

---

### Step 6: PD Transport Client

**Files**: `internal/pd/transport.go` (~150 LOC)

**What**:
- Define `PDTransport` struct:
  ```go
  type PDTransport struct {
      mu    sync.RWMutex
      conns map[uint64]*grpc.ClientConn  // peerID -> connection
      addrs map[uint64]string             // peerID -> peer address (from initial-cluster)
  }
  ```

- Implement:
  ```go
  func NewPDTransport(peerAddrs map[uint64]string) *PDTransport
  func (t *PDTransport) Send(peerID uint64, msg raftpb.Message) error
  func (t *PDTransport) Close()
  ```

- `Send` implementation:
  1. Get or create `grpc.ClientConn` for `peerID` (lazy connection, single connection per peer)
  2. Convert `raftpb.Message` to `eraftpb.Message` via `raftstore.RaftpbToEraftpb()` from `internal/raftstore/convert.go` line 24
  3. Wrap in `raft_serverpb.RaftMessage` (set `RegionId = 0` as sentinel for PD Raft)
  4. Call the unary `SendPDRaftMessage` RPC
  5. On connection error: log warning, close stale connection, return error (Raft retries automatically)

- Connection creation uses `grpc.Dial()` with `insecure.NewCredentials()` and keepalive options.

**Reuse**: Simplified version of `RaftClient` in `internal/server/transport/transport.go` line 21:
- Same lazy connection map pattern
- Same `RaftpbToEraftpb` conversion
- Simpler: no connection pool (single conn per peer), no batching (PD messages are infrequent)

**Verification**: Unit test with mock gRPC server confirming message delivery.

**Dependencies**: Step 5.

---

### Step 7: Wire Transport into PDRaftPeer

**Files**: Modify `internal/pd/raft_peer.go` (~70 LOC of changes)

**What**:
- Add a helper method to create the `sendFunc` from a `PDTransport`:
  ```go
  func (p *PDRaftPeer) WireTransport(transport *PDTransport)
  ```
  Sets `p.sendFunc` to a function that iterates over outbound messages and calls `transport.Send(msg.To, msg)` for each. Errors are logged and skipped (Raft handles retransmission).

- Handle the case where `msg.To == p.nodeID` (local message from self) by delivering directly to own mailbox instead of going through the network.

**Reuse**: Pattern from `Peer.SetSendFunc()` at `internal/raftstore/peer.go` line 210 and `Peer.handleReady()` line 446 (send loop).

**Verification**: Integration test in `internal/pd/raft_peer_test.go`:
- Start 3 `PDRaftPeer` instances each with their own gRPC server and `PDTransport`
- Verify leader election completes
- Propose `CmdPutStore` on leader, verify all 3 peers apply the command

**Dependencies**: Steps 3, 5, 6.

---

## Phase 3: Server Integration (~500 LOC)

### Step 8: Embed Raft in PDServer

**Files**: Modify `internal/pd/server.go` (~150 LOC)

**What**:
- Add new fields to `PDServer`:
  ```go
  type PDServer struct {
      // ... existing fields ...

      // Raft replication (nil in single-node mode).
      raftPeer    *PDRaftPeer
      raftStorage *PDRaftStorage
      transport   *PDTransport
      raftCfg     *PDRaftConfig

      peerGrpcServer *grpc.Server    // separate gRPC server for peer port
      peerListener   net.Listener
  }
  ```

- Add `PDRaftConfig` struct to `PDServerConfig`:
  ```go
  type PDRaftConfig struct {
      PDNodeID       uint64              // this node's ID
      InitialCluster map[uint64]string   // peerID -> peer address
      PeerAddr       string              // listen address for peer gRPC
      ClientAddrs    map[uint64]string   // peerID -> client address (for forwarding)

      RaftTickInterval     time.Duration
      ElectionTimeoutTicks int
      HeartbeatTicks       int
  }
  ```
  Add `RaftConfig *PDRaftConfig` field to `PDServerConfig` (pointer, nil means single-node).

- Modify `NewPDServer()`:
  - If `cfg.RaftConfig != nil`:
    1. Open a dedicated RocksDB engine at `cfg.DataDir + "/raft"` for Raft log persistence
    2. Create `PDRaftStorage` with `clusterID = cfg.ClusterID`
    3. Build `[]raft.Peer` from `cfg.RaftConfig.InitialCluster`
    4. Create `PDRaftPeer` with the storage and peers
    5. Create `PDTransport` with peer addresses
    6. Wire transport, set `applyFunc` to `s.applyCommand`

- Modify `Start()`:
  - If `s.raftPeer != nil`:
    1. Start peer gRPC server on `s.raftCfg.PeerAddr`
    2. Register `PDPeerService` on the peer gRPC server
    3. Start `s.raftPeer.Run(s.ctx)` in a goroutine

- Modify `Stop()`:
  - If `s.raftPeer != nil`:
    1. Stop raftPeer (context cancellation)
    2. GracefulStop peer gRPC server
    3. Close transport connections
    4. Close Raft engine

**Reuse**: Server lifecycle pattern from existing `NewPDServer()` at `internal/pd/server.go` line 86 and `Start()` at line 120.

**Verification**: Start 3-PD cluster programmatically, verify leader election via `GetMembers` returning a leader.

**Dependencies**: Steps 3, 4, 5, 6, 7.

---

### Step 8a: TSO and ID Pre-allocation Buffers

**Files**: `internal/pd/tso_buffer.go` (~100 LOC), `internal/pd/id_buffer.go` (~100 LOC)

**What**:
- Create `TSOBuffer` struct that pre-allocates TSO ranges via Raft, serves from local buffer:
  ```go
  type TSOBuffer struct {
      mu       sync.Mutex
      physical int64
      logical  int64
      remain   int
      raftPeer *PDRaftPeer
  }

  func NewTSOBuffer(raftPeer *PDRaftPeer) *TSOBuffer
  func (b *TSOBuffer) GetTS(count int) (*pdpb.Timestamp, error)
  ```
  - On first call (or when `remain` reaches 0), proposes `CmdTSOAllocate` with a batch size via Raft.
  - The proposal callback stores the resulting timestamp as the upper bound in the buffer.
  - Subsequent calls are served from the local buffer without Raft, decrementing `remain` and incrementing the logical value.
  - On leader change, the buffer is reset; the new leader proposes a fresh batch.

- Create `IDBuffer` struct that pre-allocates ID ranges via Raft, serves from local buffer:
  ```go
  type IDBuffer struct {
      mu       sync.Mutex
      base     uint64
      end      uint64
      raftPeer *PDRaftPeer
  }

  func NewIDBuffer(raftPeer *PDRaftPeer) *IDBuffer
  func (b *IDBuffer) Alloc() (uint64, error)
  ```
  - On first call (or when `base >= end`), proposes `CmdIDAlloc` with a batch size via Raft.
  - The proposal callback stores the allocated range `[base, base+batchSize)` in the buffer.
  - Subsequent calls are served from the local buffer, incrementing `base`.
  - On leader change, the buffer is reset; the new leader proposes a fresh batch.

**Reuse**: Pattern described in `02_detailed_design.md` Section 7 (TSO and ID pre-allocation buffers).

**Verification**: Unit tests in `internal/pd/tso_buffer_test.go` and `internal/pd/id_buffer_test.go` for buffer allocation, depletion, and refill.

**Dependencies**: Step 3.

---

### Step 9: Convert RPC Handlers to Raft-Aware

**Files**: Modify `internal/pd/server.go` (~200 LOC)

**What**:
- For each write RPC handler, add Raft routing logic. The pattern for unary RPCs:
  ```go
  func (s *PDServer) PutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
      if s.raftPeer == nil {
          // Single-node mode: apply directly (existing behavior).
          s.meta.PutStore(req.GetStore())
          return &pdpb.PutStoreResponse{Header: s.header()}, nil
      }
      if !s.raftPeer.IsLeader() {
          // Follower: forward to leader.
          return s.forwardPutStore(ctx, req)
      }
      // Leader: propose via Raft.
      cmd := PDCommand{Type: CmdPutStore, Store: req.GetStore()}
      _, err := s.raftPeer.ProposeAndWait(ctx, cmd)
      if err != nil {
          return nil, err
      }
      return &pdpb.PutStoreResponse{Header: s.header()}, nil
  }
  ```

- Apply this pattern to 9 write RPCs:
  | RPC Method | PDCommand Type | Special Handling |
  |---|---|---|
  | `Bootstrap` | `CmdSetBootstrapped` + `CmdPutStore` + `CmdPutRegion` | 3 commands proposed sequentially |
  | `PutStore` | `CmdPutStore` | None |
  | `AllocID` | `CmdIDAlloc` | Return allocated ID from apply result |
  | `Tso` | `CmdTSOAllocate` | Streaming RPC; propose per request (or batch) |
  | `StoreHeartbeat` | `CmdUpdateStoreStats` | None |
  | `RegionHeartbeat` | `CmdPutRegion` | Streaming RPC; scheduler runs locally after apply |
  | `AskBatchSplit` | `CmdIDAlloc` (multiple) | Allocate region + peer IDs via Raft |
  | `ReportBatchSplit` | `CmdPutRegion` (multiple) | One proposal per region |
  | `UpdateGCSafePoint` | `CmdUpdateGCSafePoint` | Return new safe point from apply result |

- Read RPCs remain unchanged (serve from local in-memory state):
  `GetStore`, `GetAllStores`, `GetRegion`, `GetRegionByID`, `IsBootstrapped`, `GetGCSafePoint`

**Reuse**: Existing handler implementations at `internal/pd/server.go` lines 224-394. Each handler is modified in-place with the 3-way if/else (nil/follower/leader).

- **Store state transitions**: `MetadataStore` needs a new method `computeStoreStateTransitions() map[uint64]StoreState` that computes state transitions (e.g., detecting stores that should transition to `Tombstone` or `Offline`) without applying them. The leader calls this method periodically and proposes the resulting `CmdSetStoreState` commands via Raft so all nodes converge on the same store states. See `02_detailed_design.md` for the full logic.

**Verification**:
- Write via any node in a 3-PD cluster
- Read from all 3 nodes, verify consistency

**Dependencies**: Steps 3, 4, 8.

---

### Step 10: Leader Forwarding

**Files**: `internal/pd/forward.go` (~100 LOC)

**What**:
- Implement forwarding methods for each write RPC:
  ```go
  func (s *PDServer) forwardPutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error)
  func (s *PDServer) forwardBootstrap(ctx context.Context, req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error)
  func (s *PDServer) forwardAllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error)
  func (s *PDServer) forwardUpdateGCSafePoint(ctx context.Context, req *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error)
  func (s *PDServer) forwardStoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error)
  func (s *PDServer) forwardAskBatchSplit(ctx context.Context, req *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error)
  func (s *PDServer) forwardReportBatchSplit(ctx context.Context, req *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error)
  ```

- Common helper:
  ```go
  func (s *PDServer) getLeaderClient() (pdpb.PDClient, error)
  ```
  Resolves current leader ID from `s.raftPeer.LeaderID()`, looks up client address from `s.raftCfg.ClientAddrs[leaderID]`, creates (or reuses cached) `grpc.ClientConn`, returns `pdpb.NewPDClient(conn)`.

- For streaming RPCs (`Tso`, `RegionHeartbeat`): follower mode creates a new stream to the leader and proxies messages bidirectionally. This is more complex but follows the same connection pattern.

**Reuse**: gRPC connection creation pattern from `pdclient.NewClient()` in `pkg/pdclient/client.go` line 136 (same `grpc.DialContext` + `insecure.NewCredentials()`).

**Verification**: `PutStore` on a follower node succeeds, `GetStore` on all nodes returns the store.

**Dependencies**: Step 8.

---

### Step 11: Enhanced GetMembers

**Files**: Modify `internal/pd/server.go` (~50 LOC)

**What**:
- Modify `GetMembers()` handler (currently at line 183):
  ```go
  func (s *PDServer) GetMembers(ctx context.Context, req *pdpb.GetMembersRequest) (*pdpb.GetMembersResponse, error) {
      resp := &pdpb.GetMembersResponse{Header: s.header()}

      if s.raftCfg == nil {
          // Single-node mode (existing behavior).
          resp.Leader = &pdpb.Member{
              Name:       "gookv-pd-1",
              ClientUrls: []string{"http://" + s.Addr()},
          }
          return resp, nil
      }

      // Multi-node mode: return all members.
      for id, clientAddr := range s.raftCfg.ClientAddrs {
          member := &pdpb.Member{
              Name:       fmt.Sprintf("gookv-pd-%d", id),
              MemberId:   id,
              ClientUrls: []string{"http://" + clientAddr},
              PeerUrls:   []string{"http://" + s.raftCfg.InitialCluster[id]},
          }
          resp.Members = append(resp.Members, member)
      }

      // Indicate current leader.
      leaderID := s.raftPeer.LeaderID()
      if leaderID > 0 {
          if addr, ok := s.raftCfg.ClientAddrs[leaderID]; ok {
              resp.Leader = &pdpb.Member{
                  Name:       fmt.Sprintf("gookv-pd-%d", leaderID),
                  MemberId:   leaderID,
                  ClientUrls: []string{"http://" + addr},
              }
          }
      }

      return resp, nil
  }
  ```

**Verification**: `GetMembers` from any node in a 3-PD cluster returns 3 members with the correct leader.

**Dependencies**: Step 8.

---

## Phase 4: Snapshot & Recovery (~300 LOC)

### Step 12: Snapshot Generation

**Files**: `internal/pd/snapshot.go` (~100 LOC)

**What**:
- Define `PDSnapshot` struct capturing all mutable state:
  ```go
  type PDSnapshot struct {
      Bootstrapped   bool                           `json:"bootstrapped"`
      Stores         map[uint64]*metapb.Store        `json:"stores"`
      Regions        map[uint64]*metapb.Region       `json:"regions"`
      Leaders        map[uint64]*metapb.Peer         `json:"leaders"`
      StoreStats     map[uint64]*pdpb.StoreStats     `json:"store_stats"`
      StoreStates    map[uint64]StoreState            `json:"store_states"`
      NextID         uint64                           `json:"next_id"`
      TSOState       TSOSnapshotState                 `json:"tso_state"`
      GCSafePoint    uint64                           `json:"gc_safe_point"`
      PendingMoves   map[uint64]*PendingMove          `json:"pending_moves"`
  }

  type TSOSnapshotState struct {
      Physical int64 `json:"physical"`
      Logical  int64 `json:"logical"`
  }
  ```

- Implement:
  ```go
  func (s *PDServer) GenerateSnapshot() ([]byte, error)
  ```
  Acquires read locks on `s.meta`, `s.tso`, `s.idAlloc`, `s.gcMgr`, `s.moveTracker`, copies all state into `PDSnapshot`, JSON-encodes it.

**Verification**: Generate snapshot, decode, verify all fields match in-memory state.

**Dependencies**: None (can be implemented in parallel with Phase 1-3).

---

### Step 13: Snapshot Application

**Files**: Add to `internal/pd/snapshot.go` (~100 LOC)

**What**:
- Implement:
  ```go
  func (s *PDServer) ApplySnapshot(data []byte) error
  ```
  JSON-decodes `PDSnapshot`, acquires write locks on all components, replaces in-memory state.

- Wire into `PDRaftPeer.handleReady()`: when `rd.Snapshot` is not empty, extract snapshot data from `rd.Snapshot.Data` and call `s.ApplySnapshot(data)`.

- Wire into `PDRaftStorage.Snapshot()`: when Raft requests a snapshot (for slow followers), call `s.GenerateSnapshot()` and return it as `raftpb.Snapshot{Data: data, Metadata: ...}`.

**Verification**: Populate state, generate snapshot, apply to fresh PDServer, verify all state matches.

**Dependencies**: Step 12.

---

### Step 14: Recovery on Restart

**Files**: Modify `internal/pd/raft_storage.go` + `internal/pd/raft_peer.go` (~100 LOC)

**What**:
- On PDServer startup with Raft enabled:
  1. Call `storage.RecoverFromEngine()` to restore Raft log + hard state
  2. Create `PDRaftPeer` with `peers = nil` (non-bootstrap restart path)
  3. Replay all committed entries from `applyState.AppliedIndex + 1` through `storage.LastIndex()` by calling `s.applyCommand()` for each
  4. This rebuilds in-memory state from the Raft log

- Detect bootstrap vs. restart:
  ```go
  func HasPersistedPDRaftState(engine traits.KvEngine, clusterID uint64) bool
  ```
  Check if `keys.RaftStateKey(clusterID)` exists in engine. Pattern from `raftstore.HasPersistedRaftState()` at `internal/raftstore/storage.go` line 289.

- In `NewPDServer()`, branch on whether persisted state exists:
  ```go
  if HasPersistedPDRaftState(raftEngine, cfg.ClusterID) {
      // Restart path: recover, replay
  } else {
      // Bootstrap path: create with peers
  }
  ```

**Verification**: Start PD cluster, write data, stop all nodes, restart all nodes, verify data preserved.

**Dependencies**: Steps 2, 3, 4, 8.

---

## Phase 5: CLI & Configuration (~200 LOC)

### Step 15: New CLI Flags

**Files**: Modify `cmd/gookv-pd/main.go` (~50 LOC)

**What**:
- Add flags:
  ```go
  pdID := flag.Uint64("pd-id", 0, "PD node ID (required with --initial-cluster)")
  initialCluster := flag.String("initial-cluster", "", "PD cluster topology: ID1=HOST:PORT,ID2=HOST:PORT,...")
  peerPort := flag.String("peer-port", "0.0.0.0:2380", "Listen address for PD-to-PD Raft peer communication")
  clientCluster := flag.String("client-cluster", "", "PD client addresses: ID1=HOST:PORT,... (optional, derived from initial-cluster if not set)")
  ```

- Parse `--initial-cluster` into `map[uint64]string`:
  ```go
  func parseInitialCluster(s string) (map[uint64]string, error)
  ```
  Split on `,`, then split each entry on `=`. Parse the left side as `uint64`, right side as address string. Return error on parse failure or even count.

**Verification**: `./gookv-pd --help` shows new flags. Parse valid and invalid `--initial-cluster` strings.

**Dependencies**: None.

---

### Step 16: PDRaftConfig Construction

**Files**: Modify `cmd/gookv-pd/main.go` (~50 LOC)

**What**:
- After flag parsing, if `*initialCluster != ""`:
  1. Validate `--pd-id` is non-zero
  2. Parse `--initial-cluster`
  3. Validate `--pd-id` exists in parsed map
  4. Validate odd count
  5. Construct `pd.PDRaftConfig` and set `cfg.RaftConfig`

- Also need `ClientAddrs` map (peerID -> client address). This requires a new flag or convention. Use the convention: each entry in `--initial-cluster` specifies the **peer** address. The client address is obtained by having each node know its own `--addr`. For other nodes' client addresses, add a companion flag:
  ```go
  clientCluster := flag.String("client-cluster", "", "PD client addresses: ID1=HOST:PORT,... (optional, derived from initial-cluster if not set)")
  ```
  If not specified, the forwarding feature requires the leader to be contacted directly (clients already do endpoint rotation).

  Simpler alternative: store the `--addr` of each PD node alongside its peer address in `--initial-cluster` using the format `ID=PEER_ADDR/CLIENT_ADDR`. But for simplicity, use a separate `--client-cluster` flag.

**Verification**: Config construction tests with various flag combinations.

**Dependencies**: Step 15.

---

### Step 17: Backward-Compatible Startup

**Files**: Modify `cmd/gookv-pd/main.go` (~100 LOC)

**What**:
- If `--initial-cluster` is NOT specified:
  - `cfg.RaftConfig = nil` (default)
  - PDServer starts in single-node mode (existing behavior, no changes)

- If `--initial-cluster` IS specified:
  - Construct and set `cfg.RaftConfig`
  - PDServer starts with Raft replication

- Full startup code:
  ```go
  if *initialCluster != "" {
      peers, err := parseInitialCluster(*initialCluster)
      if err != nil { ... }
      if *pdID == 0 { ... }
      if _, ok := peers[*pdID]; !ok { ... }
      if len(peers)%2 == 0 { ... }

      var clientAddrs map[uint64]string
      if *clientCluster != "" {
          clientAddrs, err = parseInitialCluster(*clientCluster)
          if err != nil { ... }
      }

      cfg.RaftConfig = &pd.PDRaftConfig{
          PDNodeID:             *pdID,
          InitialCluster:       peers,
          PeerAddr:             *peerPort,
          ClientAddrs:          clientAddrs,
          RaftTickInterval:     100 * time.Millisecond,
          ElectionTimeoutTicks: 10,
          HeartbeatTicks:       2,
      }
  }
  ```

**Verification**: All existing PD tests pass without `--initial-cluster`:
- `TestPDServerBootstrapAndTSO`
- `TestPDServerStoreAndRegionMetadata`
- `TestPDAskBatchSplitAndReport`
- `TestPDStoreHeartbeat`
- `TestPDClusterStoreAndRegionHeartbeat`
- `TestPDClusterTSOForTransactions`
- `TestPDClusterGCSafePoint`

Run: `go test ./e2e/... -run TestPD -v -count=1`

**Dependencies**: Steps 8, 15, 16.

---

## Implementation Order (Critical Path)

```
Step 1 (Command Encoding)  ──┐
Step 2 (Raft Storage)   ─────┴──→ Step 3 (Raft Peer) ──→ Step 7 (Wire Transport)
                                                              │
Step 4 (Apply Function) ──────────────────────────────────────┤
Step 5 (Peer Service) ───────────────────────────────→ Step 8 (Embed in Server)
Step 6 (Transport Client) ───────────────────────────┘        │
                                          Step 8a (Buffers) ──┤
                                                              ↓
Step 12 (Snapshot Gen) ──→ Step 13 (Snapshot Apply) → Step 9 (Convert Handlers)
                                                      Step 10 (Forwarding)
Step 15 (CLI Flags) ──→ Step 16 (Config) ──→ Step 17 (Backward Compat)
                                                      Step 11 (GetMembers)
                                                      Step 14 (Recovery)
```

Parallelizable work:
- Steps 1, 2, 4, 5, 6, 12, 15 can all be implemented in parallel
- Step 3 depends on 1+2; Step 7 depends on 3+5+6; Step 4 depends on 1
- Step 8a depends on Step 3
- Phase 3 (Steps 8-11) depends on Phase 1+2 completion and Step 4
- Step 14 depends on Steps 2, 3, 4, 8

## Total Estimated LOC: ~2300

| Phase | LOC | Files Created | Files Modified |
|---|---|---|---|
| Phase 1: PD Raft Core | ~800 | `command.go`, `raft_storage.go`, `raft_peer.go`, `apply.go` | None |
| Phase 2: Transport | ~300 | `peer_service.go`, `transport.go` | `raft_peer.go` |
| Phase 3: Server Integration | ~700 | `forward.go`, `tso_buffer.go`, `id_buffer.go` | `server.go` |
| Phase 4: Snapshot & Recovery | ~300 | `snapshot.go` | `raft_storage.go`, `raft_peer.go` |
| Phase 5: CLI & Config | ~200 | None | `main.go`, `server.go` |
