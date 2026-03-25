# Bug 12: Commit Sent to Wrong Node After Split — Test Plan

## 1. Unit Tests

### 1.1 `TestCommitSecondaryPerKey` — commitSecondaries sends per-key RPCs

**File:** `pkg/client/committer_test.go`

**Purpose:** Verify that `commitSecondaries` sends one `KvCommit` RPC per secondary key instead of batching by region group.

**Setup:**
- Create a `twoPhaseCommitter` with 3 mutations: primary + 2 secondaries
- Mock `SendToRegion` to record the keys passed to each call

**Assertions:**
- `SendToRegion` is called exactly 2 times (one per secondary)
- Each call receives exactly 1 key in the `CommitRequest.Keys` slice
- All secondary keys are covered

### 1.2 `TestCommitSecondaryRetryOnLockNotFound` — retry on TxnLockNotFound

**File:** `pkg/client/committer_test.go`

**Purpose:** Verify that when `KvCommit` returns `TxnLockNotFound`, the commit converts it to a region error and `SendToRegion` retries with a fresh region lookup.

**Setup:**
- Create a committer with 1 primary + 1 secondary
- Mock server: first `KvCommit` call returns `TxnLockNotFound`, second call succeeds
- Mock `LocateKey`: first call returns stale Region 1, second call returns correct Region 2

**Assertions:**
- `SendToRegion` retries at least once
- The second attempt uses a different `RegionInfo` (Region 2)
- The secondary is successfully committed

### 1.3 `TestCommitSecondaryAllKeysCommitted` — no silent failures

**File:** `pkg/client/committer_test.go`

**Purpose:** Verify that all secondary keys are committed even when some require retries.

**Setup:**
- 5 mutations (1 primary + 4 secondaries) across 3 regions
- Mock: 2 of the 4 secondaries fail on first attempt (TxnLockNotFound), succeed on retry

**Assertions:**
- All 4 secondaries are eventually committed
- No `slog.Warn("commitSecondary failed")` messages

### 1.4 `TestCommitSecondaryMaxRetriesExhausted` — graceful failure

**File:** `pkg/client/committer_test.go`

**Purpose:** Verify that when all retries are exhausted, `commitSecondaries` logs a warning but does not panic or hang.

**Setup:**
- 1 secondary key
- Mock server: all `KvCommit` calls return `TxnLockNotFound` (simulating permanently unreachable lock)

**Assertions:**
- `commitSecondaries` completes without panic
- Warning is logged
- (Primary commit is not affected — it was already committed before `commitSecondaries` is called)

## 2. Integration Tests (Server-Side)

### 2.1 `TestKvCommitLockNotFoundReturnsRegionError` — server returns correct error

**File:** `internal/server/server_test.go` or `e2e/txn_rpc_test.go`

**Purpose:** Verify that when `KvCommit` is called for a key whose lock is in a different region (after split), the server returns a region error (`KeyNotInRegion`) instead of `TxnLockNotFound`.

**Setup:**
- Start a single-node server
- Prewrite a lock for key "k1" in Region 1
- Simulate a split: Region 1 now covers [..k0), Region 2 covers [k0..)
- Send `KvCommit` for "k1" with `RegionId=Region1` (stale context)

**Assertions:**
- Response has `RegionError.KeyNotInRegion` set (not `Error.TxnLockNotFound`)
- The key "k1" is identified as being outside Region 1's range

### 2.2 `TestKvCommitLockNotFoundSameRegion` — genuine lock-not-found

**File:** `internal/server/server_test.go` or `e2e/txn_rpc_test.go`

**Purpose:** Verify that when `KvCommit` is called for a key whose lock was already resolved (not a routing issue), the server returns `TxnLockNotFound` as before.

**Setup:**
- Start a single-node server
- Prewrite a lock for key "k1"
- Rollback the lock (simulate another transaction's lock resolver)
- Send `KvCommit` for "k1" with the correct region context

**Assertions:**
- Response has `Error.TxnLockNotFound` set (not a region error)
- This is the correct behavior: the lock was genuinely cleaned up

## 3. E2E Tests

### 3.1 `TestCrossRegionCommitAfterSplit` — commit succeeds after split

**File:** `e2e/txn_cross_region_test.go` (new file)

**Purpose:** End-to-end test that a 2PC transaction with keys spanning two regions completes correctly even when a region split occurs between prewrite and commit.

**Scenario:**
```
1. Start a 3-node cluster with region-split-size=20KB
2. Seed 100 accounts (acct:0000 to acct:0099) with $100 each
3. Wait for at least 2 regions to form
4. Execute a single transfer: acct:0020 (Region A) → acct:0080 (Region B), amount=$50
5. Verify: acct:0020 = $50, acct:0080 = $150, total = $200
```

**Assertions:**
- Transfer commits successfully (no error from `txn.Commit()`)
- Both accounts have correct balances after commit
- Total balance is conserved ($200)

### 3.2 `TestCrossRegionCommitDuringSplit` — commit survives concurrent split

**File:** `e2e/txn_cross_region_test.go`

**Purpose:** Verify that concurrent splits during the commit phase do not cause partial commits.

**Scenario:**
```
1. Start a 3-node cluster with region-split-size=10KB (aggressive splits)
2. Seed 200 accounts with $100 each ($20,000 total)
3. Wait for >= 3 regions
4. Run 8 concurrent transfer workers for 10 seconds
5. Wait for all workers to finish
6. Verify total balance = $20,000
```

**Assertions:**
- Total balance is exactly $20,000
- No orphan locks remain after 5-second cleanup
- All transfers either fully committed or fully rolled back (no partial)

### 3.3 `TestCrossRegionCommitConsistency_32Workers` — full stress test

**File:** `e2e/txn_cross_region_test.go`

**Purpose:** Replicate the transaction integrity demo conditions as an automated E2E test.

**Scenario:**
```
1. Start a 3-node cluster with demo config (region-split-size=20KB, split-check=5s)
2. Seed 1000 accounts with $100 each ($100,000 total)
3. Wait for >= 3 regions with stable leaders
4. Run 32 concurrent transfer workers for 15 seconds
5. Clean up orphan locks (max 3 passes)
6. Read all balances (SI isolation, retry up to 5 times)
7. Verify total = $100,000
```

**Assertions:**
- Total balance is exactly $100,000
- At least 50 successful transfers occurred (proves meaningful concurrency)
- Zero errors (only conflicts and insufficient-funds are acceptable)
- Orphan lock cleanup completes in <= 3 passes

### 3.4 `TestCommitSecondaryRetryPath` — forced stale-cache scenario

**File:** `e2e/txn_cross_region_test.go`

**Purpose:** Force the exact Bug 12 scenario: prewrite succeeds, split occurs, commit goes to wrong node, retry fixes it.

**Scenario:**
```
1. Start a 3-node cluster with split disabled (region-split-size=1GB)
2. Write two keys: key_a = "hello", key_b = "world" (both in Region 1)
3. Begin transaction T1
4. T1.Set(key_a, "A"), T1.Set(key_b, "B")
5. Trigger a manual split so key_b moves to Region 2
6. T1.Commit()  — primary (key_a) commits to Region 1
                 — secondary (key_b) commit initially goes to Region 1 (stale cache)
                 — should retry and find Region 2
7. Verify: key_a = "A", key_b = "B"
```

**Assertions:**
- `T1.Commit()` returns nil (success)
- Both key_a and key_b have the expected values
- (If split timing is tricky, retry the entire test up to 3 times)

## 4. Regression Tests

### 4.1 Existing test suite must pass

| Test suite | Command | Expected |
|------------|---------|----------|
| Unit tests | `make test` | All PASS (3 consecutive runs) |
| E2E tests | `make test-e2e` | All PASS |
| go vet | `go vet ./...` | Clean |

### 4.2 Transaction integrity demo

| Check | Command | Expected |
|-------|---------|----------|
| Demo pass 1 | `make txn-integrity-demo-verify` | All 3 phases PASS |
| Demo pass 2 | (restart cluster + verify) | All 3 phases PASS |
| Demo pass 3 | (restart cluster + verify) | All 3 phases PASS |

## 5. Verification Checklist

- [ ] `commitSecondaries` sends per-key RPCs via `SendToRegion`
- [ ] `TxnLockNotFound` in commitSecondary is converted to retriable region error
- [ ] Server-side `KvCommit` returns `KeyNotInRegion` when key is outside region range
- [ ] Unit tests: 4 new tests pass
- [ ] Integration tests: 2 new tests pass
- [ ] E2E tests: 4 new tests pass
- [ ] `make test` — 3 consecutive passes
- [ ] `make test-e2e` — all pass
- [ ] Transaction integrity demo — 3 consecutive PASSes (32 workers, 3+ regions)
- [ ] No balance divergence in any test run
- [ ] Documentation updated (03_current_issues.md, TODO.md)
