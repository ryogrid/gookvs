# Unimplemented Features — Design Overview

## 1. Scope and Implementation Order

Investigation revealed that **KvGC RPC (originally item 6) is already fully implemented**.
`tikvService.KvGC()` schedules a `GCTask` via `GCWorker.Schedule()` and waits for completion
via callback (`internal/server/server.go:1127-1152`). Only PD safe-point integration is missing,
which is covered under item 4.

Additionally, **Store goroutine (originally item 3)** already has `RunStoreWorker()`,
`handleStoreMsg()`, and `maybeCreatePeerForMessage()` in `internal/server/coordinator.go:291-410`.
The gap is that `tikvService.Raft()` / `BatchRaft()` do not route unknown-region messages
to `storeCh`, so dynamic peer creation via gRPC is incomplete.

### Revised Items (6 items)

| # | Feature | Impact | Design Doc |
|---|---------|--------|------------|
| 1 | Raft Snapshot gRPC streaming transfer | High | `01_snapshot_transfer.md` |
| 2 | Store goroutine completion (gRPC→storeCh routing) | Medium | `02_store_goroutine.md` |
| 3 | Significant messages (Unreachable, MergeResult) | Medium | `03_significant_messages.md` |
| 4 | GC safe point PD centralization | Medium | `04_gc_safepoint_pd.md` |
| 5 | KvDeleteRange RPC (transactional) | Low | `05_kv_delete_range.md` |
| 6 | PD scheduling commands | High | `06_pd_scheduling.md` |

### Dependency DAG

```
                        ┌──────────────────┐
                        │  1. Snapshot      │
                        │     Transfer      │
                        └────────┬─────────┘
                                 │
           ┌─────────────────────┼─────────────────────┐
           │                     │                      │
           ▼                     ▼                      ▼
┌──────────────────┐  ┌──────────────────┐   ┌──────────────────┐
│ 2. Store          │  │ 3. Significant   │   │ 4. GC SafePoint  │
│    Goroutine      │  │    Messages      │   │    PD Central.   │
└────────┬─────────┘  └──────────────────┘   └──────────────────┘
         │
         ▼                                   ┌──────────────────┐
┌──────────────────┐                         │ 5. KvDeleteRange │
│ 6. PD Scheduling │                         │    (independent) │
└──────────────────┘                         └──────────────────┘
```

- **Item 1 (Snapshot)** is the top priority. Prerequisite for items 2 and 6.
- **Item 2 (Store goroutine)** is a prerequisite for item 6.
- **Items 3, 4, 5** are independently implementable.
- **Item 6 (PD scheduling)** requires items 1 and 2.

### Recommended Phases

```
Phase 1: Item 1 (Snapshot transfer)
Phase 2: Items 2, 3, 4, 5 (parallelizable)
Phase 3: Item 6 (PD scheduling)
```

## 2. Common Constraints

- Follow CLAUDE.md TDD workflow (test-first)
- Prefer Go idioms (context.Context, goroutine+channel, sync.Map)
- Each item is independently committable
- No external paid APIs
- Use Serena's `find_symbol`, `get_symbols_overview`, `search_for_pattern` tools for
  efficient Go code exploration; minimize full-file reads

## 3. Testing Strategy (All Items)

Each item requires two levels of tests:

1. **Unit tests**: Add `_test.go` files in the target package. Use mocks/stubs.
2. **E2E tests**: Add new test files in `e2e/`. Spin up real server processes.

Execution:
```bash
# Unit tests
go test ./target/package/... -v -count=1 -run TestXxx

# E2E tests
go test ./e2e/... -v -count=1 -run TestXxx -timeout 120s
```

On test failure:
1. Read error messages and stack traces
2. Use Serena's `search_for_pattern` to locate related code and root-cause the issue
3. Apply the fix and re-run
4. Repeat until all tests (unit + E2E) pass

## 4. Note on KvGC RPC

KvGC is **already implemented** at `internal/server/server.go:1127-1152`.

What it does:
- Accepts `GCRequest.SafePoint` and submits a `GCTask` to `GCWorker.Schedule()`
- Blocks on callback completion or context cancellation
- Returns errors via `kvrpcpb.GCResponse.Error`

What it does NOT do:
- Call `pdClient.UpdateGCSafePoint()` to propagate the safe point to PD

The PD integration gap is addressed as part of item 4 (GC SafePoint PD centralization).
