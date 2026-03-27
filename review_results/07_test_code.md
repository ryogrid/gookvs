# Code Review: Test Code (e2e, e2e_external, pkg/e2elib)

**Reviewer:** Claude Opus 4.6 (1M context)
**Date:** 2026-03-27
**Scope:** e2e/ (5 files), e2e_external/ (15 files), pkg/e2elib/ (7 files)

---

## Summary

The test suite is generally well-structured with clear naming, good use of `t.Helper()`, proper resource cleanup via `t.Cleanup`, and consistent assertion style. The e2elib package provides a solid foundation for external process management with file-lock-based port allocation. However, there are several correctness issues, missing edge cases, flaky patterns, and resource management concerns that should be addressed.

**Severity Scale:** CRITICAL (breaks correctness) | HIGH (likely causes real issues) | MEDIUM (should fix) | LOW (improvement)

---

## 1. Test Correctness Issues

### 1.1 [HIGH] GC test does not verify old version was actually deleted
**File:** `/home/ryo/work/gookv/e2e/gc_worker_test.go:96-99`

`TestGCWorkerCleansOldVersions` verifies the latest version is readable after GC, but never checks that the *old* version (committed at version 20) was actually garbage collected. The test would pass even if GC was a no-op.

**Fix:** Add a read at version `25` (between the old commit version 20 and the GC safe point 35) and verify it returns `NotFound` or the value is absent.

### 1.2 [HIGH] TestMultiRegionTransactions declares secondaryKey but never prewrites it
**File:** `/home/ryo/work/gookv/e2e_external/multi_region_routing_test.go:29,70`

The variable `secondaryKey` is declared and used only in a dead `_ = secondaryKey` assignment. The test claims to test "transactional prewrite/commit across regions" but only touches a single key on a single region. The comment on line 70 acknowledges this is incomplete.

**Recommendation:** Either implement the actual cross-region transaction (prewrite on multiple nodes/regions) or rename the test to reflect what it actually verifies (single-region txn prewrite/commit in a multi-region cluster).

### 1.3 [HIGH] TestMultiRegionAsyncCommit leaves an uncommitted lock
**File:** `/home/ryo/work/gookv/e2e_external/multi_region_routing_test.go:175-209`

The test prewrites with `UseAsyncCommit=true` but never commits or rolls back the transaction. This orphaned lock could interfere with other tests if they run against the same cluster (though each test creates its own cluster via `newMultiRegionCluster`, so in practice this is contained). Still, the test only verifies that prewrite succeeded -- it does not verify async commit semantics (MinCommitTs value, secondary lock resolution, etc.).

### 1.4 [MEDIUM] TestClusterServerCrossNodeReplication silently ignores non-leader nodes
**File:** `/home/ryo/work/gookv/e2e_external/cluster_server_test.go:63-75`

The loop that checks replication on each node via raw gRPC skips nodes that return errors with `continue`. This means the test can pass even if no node has replicated data. The assertion only runs on nodes that don't error, but all 3 could error.

**Fix:** Track how many nodes were successfully checked and assert at least a quorum succeeded.

### 1.5 [MEDIUM] TestPDReplication_TSOViaFollower does not assert anything on failure paths
**File:** `/home/ryo/work/gookv/e2e_external/pd_replication_test.go:333-360`

If all 3 nodes fail TSO or produce non-monotonic timestamps, the test still passes because the final `t.Log` runs unconditionally. The per-node checks only log success but never fail the test on violation.

**Fix:** Add an assertion that at least one node succeeded the monotonicity check, or use `require` instead of silently breaking.

### 1.6 [MEDIUM] TestPDReplication_TSOViaFollowerForwarding silently skips errors
**File:** `/home/ryo/work/gookv/e2e_external/pd_replication_test.go:363-385`

Inside the inner loop, `GetTS` errors are silently continued. If every call fails, `prevTS` stays 0 and the test passes with no assertions ever firing.

**Fix:** Count successes and assert a minimum number.

### 1.7 [MEDIUM] TestPDReplication_ConcurrentWritesFromMultipleClients uses weak assertion
**File:** `/home/ryo/work/gookv/e2e_external/pd_replication_test.go:301-330`

The test expects `>= 25` unique IDs from 30 attempts (3 goroutines x 10 allocations each). AllocID errors are silently skipped. The threshold of 25 seems arbitrary and could mask significant failure rates (up to 17% loss). A test that silently tolerates 5 lost AllocID calls may not catch regressions.

### 1.8 [LOW] TestPDReplication_WriteForwarding does not assert write forwarding actually happened
**File:** `/home/ryo/work/gookv/e2e_external/pd_replication_test.go:78-101`

The test tries PutStore on each node and breaks on first success, then verifies the store is visible. But it cannot distinguish whether the write was forwarded (hit a follower) or direct (hit the leader). The test name implies forwarding verification, but this is not verified.

### 1.9 [LOW] Region merge test checks confVer bump on PrepareMerge, but the expected value may be wrong
**File:** `/home/ryo/work/gookv/e2e/region_split_merge_test.go:52`

The assertion expects `confVer = 2` after PrepareMerge. In TiKV, PrepareMerge bumps `version` but typically does not bump `conf_ver` unless there is a config change. Verify this matches the gookv implementation of `ExecPrepareMerge`.

---

## 2. Missing Edge Cases

### 2.1 [HIGH] No test for GC of delete tombstones
The GC tests only cover put operations. There is no test verifying that GC correctly handles delete tombstones (Op_Del mutations that leave MVCC markers).

### 2.2 [HIGH] No test for transaction conflict detection
None of the transactional tests verify write-write conflict detection (two transactions trying to prewrite the same key). This is a fundamental correctness property of the transaction system.

### 2.3 [HIGH] No test for lock timeout / TTL expiry
Transaction tests create locks with TTLs (e.g., 5000ms) but never test the timeout path -- what happens when a lock expires and another transaction tries to access the key.

### 2.4 [MEDIUM] No test for empty key/value edge cases
No test verifies behavior with empty keys (`[]byte{}`) or empty values. These are edge cases that can expose off-by-one errors in key encoding.

### 2.5 [MEDIUM] No test for large values or keys
No test exercises large values (e.g., 1MB+) that might expose issues with protobuf message size limits, RocksDB write batch sizes, or Raft log entry sizes.

### 2.6 [MEDIUM] TestClusterMajorityFailure uses fixed sleep instead of proper negative assertion
**File:** `/home/ryo/work/gookv/e2e/raft_cluster_test.go:346-349`

The test sleeps for 1 second and then checks the proposal was not applied. This is inherently fragile -- a slow machine might not have processed the proposal within 1 second even with quorum. A more robust approach would be to use `require.Never` or poll for a longer period.

### 2.7 [LOW] No test for ScanLock with empty key range
The ScanLock test uses specific start/end keys. There is no test for scanning with nil start/end keys (full range scan).

### 2.8 [LOW] No test for concurrent RawKV operations
No test exercises concurrent Put/Get/Delete operations to verify thread safety under load.

---

## 3. Flaky Patterns

### 3.1 [HIGH] Fixed time.Sleep before Campaign in all raft_cluster_test.go tests
**File:** `/home/ryo/work/gookv/e2e/raft_cluster_test.go:235,259,285,325,359,403`

Every test uses `time.Sleep(300 * time.Millisecond)` before triggering `Campaign()`. This is a guess at how long bootstrap takes. On a loaded CI machine this could be insufficient, causing Campaign to fail or behave unexpectedly. A better approach would be to poll until all nodes are ready (e.g., check that the raft state machine has been initialized).

### 3.2 [MEDIUM] secondDuration constant is confusing and brittle
**File:** `/home/ryo/work/gookv/pkg/e2elib/cluster.go:143-144`

```go
const secondDuration = 1000000000 // time.Second in nanoseconds as a typed duration
```

This untyped constant works because Go allows it to be implicitly converted to `time.Duration`, but it's unusual and confusing. Using `time.Second` directly (e.g., `30 * time.Second`) would be clearer.

### 3.3 [MEDIUM] WaitForReady(30*1e9) uses floating-point arithmetic for duration
**File:** `/home/ryo/work/gookv/e2e_external/pd_server_test.go:25`

```go
require.NoError(t, pd.WaitForReady(30*1e9)) // 30 seconds
```

`1e9` is a float constant. While this works in Go (the constant is exact and fits int64), it is an unusual pattern and the comment is the only hint at what `30*1e9` means. Use `30 * time.Second` instead.

### 3.4 [MEDIUM] Multiple PD clients created per test without reuse
**File:** `/home/ryo/work/gookv/pkg/e2elib/pdcluster.go:204-222` and `225-244`

Both `Client()` and `ClientForNode()` create new `pdclient.Client` instances on every call. If a test calls `Client()` multiple times, it creates multiple connections. While cleanup is registered, this wastes resources and could cause port exhaustion. The `GokvCluster.Client()` method caches the client, but `PDCluster` does not.

### 3.5 [LOW] Potential TOCTOU in port allocation
**File:** `/home/ryo/work/gookv/pkg/e2elib/port.go:67-76`

After acquiring the flock, the allocator verifies the port is bindable by briefly listening and then immediately closing. Between `ln.Close()` and the actual process binding the port, another process could grab it. The flock mitigates this for gookv tests, but external processes are not aware of the flock.

---

## 4. Resource Leaks and Cleanup Issues

### 4.1 [HIGH] Log file handles leaked in pdcluster.go Start()
**File:** `/home/ryo/work/gookv/pkg/e2elib/pdcluster.go:142-149`

The `logFile` created by `os.Create(logPath)` is never explicitly closed. It remains open as long as the process uses it for stdout/stderr, but on process stop the file descriptor is not cleaned up by the `PDClusterNode.Stop()` method. The OS will eventually close it, but this is a resource leak during the test run.

Similarly in `gokvnode.go:127-130`, the log file handle from `os.Create(n.logPath)` is never stored or closed. When the process dies, the file descriptor becomes orphaned.

### 4.2 [MEDIUM] PDClusterNode.Restart() leaks old log file handle
**File:** `/home/ryo/work/gookv/pkg/e2elib/pdcluster.go:294-319`

`Restart()` opens a new log file with `os.OpenFile` (append mode) but the old log file handle from the initial `Start()` is never closed. The `Stop()` method kills the process but does not close the file handle.

### 4.3 [MEDIUM] Double cleanup registration in GokvCluster
**File:** `/home/ryo/work/gookv/pkg/e2elib/cluster.go:146-165` and `cluster.go:256-264`

Both `GokvCluster.Stop()` and the individual node `t.Cleanup` handlers attempt to stop nodes. The `GokvCluster.Stop()` is called via `t.Cleanup` in tests (e.g., `cluster_raw_kv_test.go:23`), and each `GokvNode` also has its own `t.Cleanup(func() { _ = n.Stop() })`. This double-stop is harmless (Stop is idempotent) but wasteful. Similarly, ports are double-released: once by the node cleanup and once by `alloc.ReleaseAll()`.

### 4.4 [MEDIUM] PDCluster.Client() and ClientForNode() create new clients every call
**File:** `/home/ryo/work/gookv/pkg/e2elib/pdcluster.go:204-244`

Unlike `GokvCluster.Client()` which caches, `PDCluster.Client()` creates a new client on every invocation. Tests like `TestPDReplication_LeaderFailover` call `cluster.Client()` once in `newPDCluster` (via `bootstrapPDCluster`) and again in the test body, creating two clients.

### 4.5 [LOW] GokvNode.ensureClient() panics if PDEndpoints is nil (standalone mode)
**File:** `/home/ryo/work/gookv/pkg/e2elib/gokvnode.go:213-234`

In standalone mode, `PDEndpoints` is nil. Calling `ensureClient()` (indirectly via `RawKV()` or `TxnKV()`) will attempt `client.NewClient` with nil PDAddrs, which may panic or fail. While standalone tests use `DialTikvClient` instead, this is a footgun.

---

## 5. Code Quality / Maintainability

### 5.1 [MEDIUM] Duplicated cluster setup helpers across test files

`newClusterWithLeader` is defined in `cluster_raw_kv_test.go` and used by `add_node_test.go`, `cluster_server_test.go`, `pd_cluster_integration_test.go`, `pd_leader_discovery_test.go`. Similarly, `newMultiRegionCluster` is in `multi_region_test.go` and used by `multi_region_routing_test.go`. These are test helpers that span files, which is fine in the same package, but could be consolidated in a shared test helper file (e.g., `test_helpers_test.go`) for clarity.

### 5.2 [MEDIUM] newClientCluster vs newClusterWithLeader are near-duplicates
**Files:** `client_lib_test.go:17-36` vs `cluster_raw_kv_test.go:16-33`

Both create a 3-node cluster and wait for leader election. `newClientCluster` additionally returns the `RawKVClient`. These could share a common implementation.

### 5.3 [LOW] Blank lines and empty comment blocks in several e2e_external tests
Several test files (e.g., `async_commit_test.go:20-22`, `txn_rpc_test.go:17-19`, `raw_kv_extended_test.go:21-23`) have suspicious blank lines between `NewStandaloneNode(t)` and the next statement. These look like leftover artifacts from removed code.

### 5.4 [LOW] TestPessimisticRollback rolls back a key that was never locked
**File:** `/home/ryo/work/gookv/e2e_external/txn_rpc_test.go:41-59`

The test sends a `PessimisticRollbackRequest` for `pessimistic-key-1` but this test function creates its own standalone node -- the key was never locked in this test's server instance. The test only verifies the RPC does not error, not that it actually rolled back a lock. This is essentially testing the RPC stub, not the logic.

### 5.5 [LOW] Inconsistent use of require vs assert for critical preconditions
Throughout the tests, there is inconsistent usage of `require` (stop on failure) vs `assert` (continue on failure). For example, in `gc_worker_test.go:56-57`, `assert.Empty` is used for prewrite errors -- if prewrite fails, the commit on the next line will produce confusing errors. Critical preconditions should use `require`.

---

## 6. Positive Observations

- **Port allocation**: The file-lock-based `PortAllocator` is well-designed for parallel test execution and inter-process safety.
- **Process lifecycle management**: `GokvNode` and `PDNode` have clean Start/Stop/Restart with proper graceful shutdown (SIGINT then SIGKILL after timeout).
- **Test isolation**: Each test creates its own cluster/node, preventing cross-test contamination.
- **Proper use of `t.Helper()`**: All helper functions correctly call `t.Helper()` for accurate line reporting.
- **Good coverage of raft scenarios**: The raft_cluster_test.go covers leader election, proposal replication, minority/majority failure, re-election, and node recovery -- a solid set of distributed systems scenarios.
- **Table-driven subtests**: `TestRawCompareAndSwap` uses subtests effectively to cover multiple CAS scenarios.
- **Cleanup ordering**: `GokvCluster.Stop()` stops nodes in reverse order (last started, first stopped), which is a good practice for dependency ordering.

---

## Prioritized Fix List

| Priority | Issue | File(s) |
|----------|-------|---------|
| HIGH | GC test does not verify old version deletion | e2e/gc_worker_test.go |
| HIGH | No test for transaction conflict detection | e2e_external/ (missing) |
| HIGH | No test for lock TTL expiry | e2e_external/ (missing) |
| HIGH | Log file handles leaked | pkg/e2elib/gokvnode.go, pdcluster.go |
| HIGH | Fixed sleep before Campaign | e2e/raft_cluster_test.go |
| MEDIUM | Cross-node replication test silently ignores errors | e2e_external/cluster_server_test.go |
| MEDIUM | TSO via follower tests have no failure assertions | e2e_external/pd_replication_test.go |
| MEDIUM | secondDuration and 1e9 duration patterns | pkg/e2elib/cluster.go, e2e_external/pd_server_test.go |
| MEDIUM | PDCluster.Client() not cached | pkg/e2elib/pdcluster.go |
| MEDIUM | Duplicate cluster setup helpers | e2e_external/ |
| LOW | TestPessimisticRollback tests stub, not logic | e2e_external/txn_rpc_test.go |
| LOW | Suspicious blank lines in standalone tests | e2e_external/ (multiple) |
