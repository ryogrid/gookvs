# Code Review Summary: gookv

## Overview

Full code review of ~50,800 lines across 163 Go files, performed by 7 parallel review agents.
Focus: bug detection and data consistency design flaws.

## Critical & High Issue Count by Area

| # | Area | Critical | High | Medium | Review File |
|---|------|----------|------|--------|-------------|
| 1 | Raft Layer | 5 | 5 | 6 | `01_raft_layer.md` |
| 2 | Server/RPC Layer | 6 | — | — | `02_server_rpc_layer.md` |
| 3 | Storage/MVCC Layer | — | 3 | 4 | `03_storage_mvcc_layer.md` |
| 4 | Client Library | 3 | 4 | 6 | `04_client_library.md` |
| 5 | PD Layer | 4 | 5 | 6 | `05_pd_layer.md` |
| 6 | Entry/Config/Codec | 1 | 4 | 6 | `06_entry_config_codec.md` |
| 7 | Test Code | — | 4 | 5 | `07_test_code.md` |
| | **Total** | **19** | **25** | **33** | |

## Top Critical Issues (Data Consistency Impact)

### Raft Layer — Restart Loses Apply State
- **File**: `internal/raftstore/storage.go` (RecoverFromEngine / PersistApplyState)
- `RecoverFromEngine()` never restores `ApplyState` from disk. On restart, applied index resets to default, causing double-application of committed entries.
- `PersistApplyState()` is defined but never called — apply progress is never durably saved.

### Raft Layer — InitialState Returns Empty ConfState
- **File**: `internal/raftstore/storage.go` (InitialState)
- Always returns `ConfState{}`. Multi-node clusters cannot restart because Raft doesn't know its peers after recovery.

### Raft Layer — Snapshot Doesn't Clear Old Data
- **File**: `internal/raftstore/snapshot.go` (ApplySnapshot → ApplySnapshotData)
- Passes `nil, nil` for key range, so stale keys are never cleared before snapshot data is written.

### Raft Layer — Proposal Callback Index Tracking
- **File**: `internal/raftstore/peer.go`
- Uses `lastIdx + 1` which breaks with batched proposals. Callbacks can fire for wrong operations.

### Server/RPC — PessimisticRollback & TxnHeartBeat Bypass Raft
- **File**: `internal/server/server.go:804-830`
- `KVPessimisticRollback` and `KvTxnHeartBeat` write directly to storage in cluster mode without Raft replication. Changes are lost on leader failover.

### Server/RPC — ProposeModifies Can Block Forever
- **File**: `internal/server/coordinator.go:317-343`
- If the peer silently drops a command, the callback is never invoked and the caller blocks until the 10-second timeout.

### Server/RPC — KvDeleteRange Hardcodes Region ID 1
- **File**: `internal/server/server.go`
- All delete range proposals go to region 1 regardless of actual key range.

### Client — Region Cache Never Evicts Stale Entries After Split
- **File**: `pkg/client/region_cache.go` (insertLocked)
- After a split, old region entries covering the full key range are never removed, causing misdirected RPCs.

### Client — BatchGet Sends All Keys to Single Region
- **File**: `pkg/client/txn.go` (BatchGet)
- All keys sent to the region owning `keys[0]`. Keys in other regions silently return no results.

### PD — Proposal Index Tracking Bug
- **File**: `internal/pd/raft_peer.go`
- Uses `storage.LastIndex()+1` after `Propose()` but storage doesn't reflect the proposed entry yet. Callbacks may fire for wrong proposals.

### PD — Non-Atomic Bootstrap in Raft Mode
- **File**: `internal/pd/server.go`
- Bootstrap uses 3 separate proposals. If first commits but second fails, cluster is permanently wedged.

### PD — ReportBatchSplit Missing Leader Info
- **File**: `internal/pd/server.go`
- Raft mode `CmdPutRegion` doesn't set Leader field. Post-split regions have no leader info, breaking routing.

### Storage/MVCC — Backward Scan Bounds Swapped
- **File**: `internal/storage/mvcc/scanner.go:63-74`
- Upper/lower bound conditions are reversed for backward scans.

### Storage/MVCC — PointGetter Doesn't Skip Pessimistic Locks
- **File**: `internal/storage/mvcc/point_getter.go:74-86`
- Returns LockError for pessimistic locks that should be invisible to readers (unlike Scanner which correctly skips them).

### Entry/Config — Coprocessor Float64 Encoding Bug
- **File**: `internal/coprocessor/endpoint.go:609,652`
- Reads `d.I64` instead of `math.Float64bits(d.F64)`. Every float64 value in coprocessor responses is corrupted to zero.

## Recommended Fix Priority

**P0 — Fix immediately (data loss / corruption risk):**
1. Raft ApplyState persistence (C1+C2 in 01)
2. PessimisticRollback & TxnHeartBeat Raft bypass (C2+C3 in 02)
3. Region cache stale entry eviction (C1 in 04)
4. PointGetter pessimistic lock handling (BUG-02 in 03)
5. Backward scan bounds (BUG-01 in 03)

**P1 — Fix soon (correctness under failure):**
1. InitialState empty ConfState (C3 in 01)
2. Snapshot not clearing old data (C4 in 01)
3. PD bootstrap atomicity (C2 in 05)
4. PD proposal index tracking (C1 in 05)
5. Client BatchGet single-region bug (C2 in 04)
6. KvDeleteRange hardcoded region ID (02)

**P2 — Fix when convenient (edge cases, robustness):**
1. Proposal callback tracking (C5 in 01, C1 in 05)
2. ProposeModifies timeout (C5 in 02)
3. Async commit conflict detection (BUG-03 in 03)
4. PD ReportBatchSplit leader info (C3 in 05)
5. Coprocessor float64 encoding (C-1 in 06)
6. Client DeleteRange/Checksum single-region (H3/H4 in 04)
