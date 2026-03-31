# Test Plan: TxnKV Scan API

## 1. Unit Tests

### 1.1 TxnHandle.Scan Membuf Tests

**File**: `pkg/client/txn_test.go`

| Test | Description |
|------|-------------|
| `TestScan_MembufOnly` | Set keys via `txn.Set`, scan without RPC. Verify membuf entries in range returned sorted, capped at limit. |
| `TestScan_MembufDeleteExcluded` | Set then Delete a key. Scan should not return the deleted key. |
| `TestScan_MembufRangeFiltering` | Set keys outside `[startKey, endKey)`. Verify excluded. |
| `TestScan_MembufLimit` | Set 10 keys. Scan with limit=3 returns only 3. |
| `TestScan_MembufEmptyRange` | Scan range with no keys. Returns empty slice, no error. |
| `TestScan_AfterCommit` | Mark txn committed. Scan returns `ErrTxnCommitted`. |
| `TestScan_AfterRollback` | Mark txn rolled back. Scan returns `ErrTxnRolledBack`. |

Note: These use `TxnHandle` with nil client (membuf-only path). The `Scan` method must handle nil `t.client` gracefully for membuf-only scans — skip the RPC loop and return membuf results only.

### 1.2 CLI Parser Tests

**File**: `cmd/gookv-cli/parser_test.go`

| Test | Description |
|------|-------------|
| `SCAN in txn context` | `ParseCommand(["SCAN", "a", "z"], inTxn=true)` returns `CmdTxnScan` |
| `SCAN outside txn` | Existing test — `ParseCommand(["SCAN", "a", "z"], inTxn=false)` returns `CmdScan` |
| `SCAN LIMIT in txn` | `ParseCommand(["SCAN", "a", "z", "LIMIT", "10"], inTxn=true)` returns `CmdTxnScan` with IntArg=10 |

### 1.3 CLI Executor Tests

**File**: `cmd/gookv-cli/executor_test.go`

| Test | Description |
|------|-------------|
| `TestTxnScanNoActiveTxn` | Execute `CmdTxnScan` without BEGIN. Expect "no active transaction" error. |

Note: `activeTxn` is `*client.TxnHandle` (not the interface), so mock-based testing is not feasible without refactoring the executor. Executor integration testing is covered by E2E tests.

## 2. E2E Tests

### 2.1 Client Library Scan Tests

**File**: `e2e_external/client_lib_test.go` (or new file)

| Test | Description |
|------|-------------|
| `TestClientTxnScanBasic` | Put keys via raw API, BEGIN + Scan + ROLLBACK, verify all keys returned. |
| `TestClientTxnScanWithMembuf` | BEGIN, Set keys, Scan reads them back (membuf merge), ROLLBACK. |
| `TestClientTxnScanWithLimit` | Put 5 keys, Scan with limit=3, verify exactly 3 results. |
| `TestClientTxnScanMultiRegion` | Use cluster with SplitSize to create multiple regions, put keys spanning regions, verify Scan returns all. |

### 2.2 CLI Integration Tests

| Test | Description |
|------|-------------|
| `TestCLITxnScan` | Use `CLITxnScan` e2elib helper, verify output contains expected keys. |

## 3. Regression

All existing tests must continue to pass:

```bash
make -f Makefile test
make -f Makefile test-e2e-external
FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make -f Makefile test-fuzz-cluster
```

## 4. Execution Commands

```bash
go test ./pkg/client/... -v -run TestScan -count=1
go test ./cmd/gookv-cli/... -v -count=1
make -f Makefile test
make -f Makefile test-e2e-external
FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make -f Makefile test-fuzz-cluster
```
