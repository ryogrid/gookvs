# Leader Lease Read — Implementation Plan

## Phase 0: Prerequisite — Increase Election Timeout

- [ ] **`internal/raftstore/peer.go`** — Change `DefaultPeerConfig()`:
  ```go
  RaftElectionTimeoutTicks: 200,  // was 10. 200ms with 1ms tick.
  ```
  This provides 40ms safety margin (200ms - 160ms lease) >> 8ms WAL sync.

- [ ] Update `internal/raftstore/peer_test.go` `TestDefaultPeerConfig` assertion.

## Phase 1: Core Lease Read Path

### Files to Modify

- [ ] **`internal/raftstore/peer.go`** — Add `committedIndex` atomic field:
  ```go
  // In Peer struct:
  committedIndex atomic.Uint64  // updated from peer goroutine only
  ```

- [ ] **`internal/raftstore/peer.go`** — Update `handleReady()` to track committed index:
  ```go
  // After processing HardState:
  if !raft.IsEmptyHardState(rd.HardState) {
      p.committedIndex.Store(rd.HardState.Commit)
  }
  ```

- [ ] **`internal/raftstore/peer.go`** — New method `IsAppliedCurrent()`:
  ```go
  func (p *Peer) IsAppliedCurrent() bool {
      return p.storage.AppliedIndex() >= p.committedIndex.Load()
  }
  ```
  Safe to call from any goroutine (both fields are atomic/mutex-protected).

- [ ] **`internal/server/coordinator.go`** — Add `EnableLeaseRead` to `StoreCoordinatorConfig`:
  ```go
  EnableLeaseRead bool // default: true
  ```

- [ ] **`internal/server/coordinator.go`** — Modify `ReadIndex()`:
  ```go
  // Lease read: bypass ReadIndex entirely.
  if sc.cfg.EnableLeaseRead && peer.IsLeaseValid() && peer.IsAppliedCurrent() {
      leaseReadTotal.Inc()
      return nil  // No readindexDuration observation on this path
  }

  // Fall through to ReadIndex via batcher.
  start := time.Now()
  defer func() { readindexDuration.Observe(time.Since(start).Seconds()) }()
  batcher := sc.getOrCreateBatcher(regionID, peer)
  return batcher.Wait(timeout)
  ```

- [ ] **`internal/server/metrics.go`** — Add `leaseReadTotal` counter and register in `init()`.

### Verification
```bash
go build ./internal/...
go vet ./...
make test
# Start a cluster, send reads, verify:
# - First read after leader election uses ReadIndex (lease not yet valid)
# - Subsequent reads use lease (check via Prometheus: readindex_duration_seconds_count stays flat)
```

---

## Phase 2: Explicit Lease Invalidation on Admin Commands

### Files to Modify

- [ ] **`internal/raftstore/peer.go`** — In `applyConfChangeEntry()`, add `p.leaseValid.Store(false)` in each **success branch** (after `p.rawNode.ApplyConfChange()` is called), NOT at the function top or in error-return paths. A malformed entry that fails unmarshal does not change membership and should not invalidate the lease.

- [ ] **`internal/raftstore/peer.go`** — In `applySplitAdminEntry()`, at the beginning of the method:
  ```go
  p.leaseValid.Store(false)
  ```

### Verification
```bash
# Run fuzz test with splits and verify no stale reads:
FUZZ_ITERATIONS=100 FUZZ_CLIENTS=4 go test ./e2e_external/... -v -count=1 -timeout 900s -run TestFuzzCluster
```

---

## Phase 3: Prometheus Metrics for Lease Reads

### Files to Modify

- [ ] **`internal/server/metrics.go`** — Add lease read counter:
  ```go
  var leaseReadTotal = prometheus.NewCounter(
      prometheus.CounterOpts{
          Namespace: "gookv",
          Name:      "lease_read_total",
          Help:      "Total reads served via leader lease (bypassing ReadIndex).",
      },
  )
  ```
  Register in `init()`.

- [ ] **`internal/server/coordinator.go`** — Increment counter on lease read:
  ```go
  if sc.cfg.PeerCfg.EnableLeaseRead && peer.IsLeaseValid() && peer.IsAppliedCurrent() {
      leaseReadTotal.Inc()
      return nil
  }
  ```

### Verification
```bash
# During demo, curl /metrics and check:
# - gookv_lease_read_total > 0 (lease reads are happening)
# - gookv_readindex_duration_seconds_count should be much lower than gookv_grpc_msg_total{type="kv_get"}
```

---

## Phase 4: Full Validation

- [ ] `go vet ./...`
- [ ] `make build`
- [ ] `make test`
- [ ] Fuzz test: `FUZZ_ITERATIONS=200 FUZZ_CLIENTS=4`
- [ ] Run txn-integrity-demo and compare metrics:
  - `gookv_grpc_msg_duration_seconds_sum{type="kv_get"}` / count → should be <5ms (vs ~65ms before)
  - `gookv_lease_read_total` → should be >0
  - `gookv_readindex_duration_seconds_count` → should be much lower
- [ ] Disable lease reads via config flag and verify fallback works

---

## Rollback Strategy

If lease reads cause correctness issues:
1. Set `EnableLeaseRead: false` in `DefaultPeerConfig()`
2. All reads fall back to ReadIndex (the existing, proven path)
3. No code removal needed — the lease check is a simple conditional bypass

---

## Testing Strategy

### Unit Tests

- [ ] `TestIsAppliedCurrent`: Verify returns true when applied==committed, false when behind
- [ ] `TestLeaseReadBypass`: Mock peer with valid lease and current apply → ReadIndex returns nil immediately
- [ ] `TestLeaseReadFallback`: Mock peer with expired lease → falls through to batcher
- [ ] `TestLeaseInvalidationOnSplit`: After split, lease is invalid
- [ ] `TestLeaseInvalidationOnConfChange`: After ConfChange, lease is invalid

### Race Detection (MANDATORY)

```bash
go test -race ./internal/raftstore/... ./internal/server/... -count=1
```
Run under write load to verify no races on `committedIndex`, `leaseExpiryNanos`, `leaseValid`.

### Integration Tests

- [ ] Fuzz test with lease reads enabled (default) — balance conservation must hold
- [ ] txn-integrity-demo — all 3 phases must pass
- [ ] Lease reads during concurrent writes — no stale data

### Stale Read Prevention Test

- [ ] Write value A, read immediately → must see A (not stale)
- [ ] Write A, trigger leader transfer, read on new leader → must see A
- [ ] Write A, trigger split, read on child region → must see A
