# Implementation Checklist: TxnKV Scan API

Complete each step in order.

## Phase 1: Client Library

### Step 1: Add `Scan` and `scanMembuf` to TxnHandle

**File**: `pkg/client/txn.go`

1. Add `"bytes"` and `"sort"` to imports
2. Add `scanMembuf(startKey, endKey []byte) []KvPair` method
3. Add `Scan(ctx, startKey, endKey, limit) ([]KvPair, error)` method
   - State guards (committed/rolledBack)
   - Multi-region loop (following RawKVClient.Scan pattern)
   - Lock resolution retry (following Get pattern)
   - Membuf merge at the end

See `01_design.md` §1 for full design.

**Verify**: `go vet ./pkg/client/...`

### Step 2: Add unit tests for Scan

**File**: `pkg/client/txn_test.go`

Add membuf-only tests: `TestScan_MembufOnly`, `TestScan_MembufDeleteExcluded`, `TestScan_MembufRangeFiltering`, `TestScan_MembufLimit`, `TestScan_MembufEmptyRange`, `TestScan_AfterCommit`, `TestScan_AfterRollback`.

**Verify**: `go test ./pkg/client/... -v -run TestScan -count=1`

## Phase 2: CLI

### Step 3: Add CmdTxnScan to parser

**File**: `cmd/gookv-cli/parser.go`

1. Add `CmdTxnScan` constant after `CmdTxnDelete` (line 305)
2. Change `parseScan(args)` → `parseScan(args []Token, inTxn bool)` (line 566)
3. In `parseScan`: return `CmdTxnScan` when `inTxn` is true
4. Update `ParseCommand` switch (line 385): `return parseScan(args, inTxn)`

**Verify**: `go vet ./cmd/gookv-cli/...`

### Step 4: Add execTxnScan to executor

**File**: `cmd/gookv-cli/executor.go`

1. Add `Scan` to `txnHandleAPI` interface (line 47-55)
2. Add `CmdTxnScan` case to `Exec` switch
3. Add `execTxnScan` function (follows `execRawScan` pattern)

**Verify**: `go vet ./cmd/gookv-cli/...`

### Step 5: Add CLI tests

**Files**: `cmd/gookv-cli/parser_test.go`, `cmd/gookv-cli/executor_test.go`

Add tests per `02_test_plan.md` §1.2 and §1.3.

**Verify**: `go test ./cmd/gookv-cli/... -v -count=1`

## Phase 3: e2elib

### Step 6: Add CLITxnScan helper

**File**: `pkg/e2elib/cli.go`

Add `CLITxnScan` and `CLITxnScanRaw` per `01_design.md` §3.

**Verify**: `go vet ./pkg/e2elib/...`

## Phase 4: Build & Unit Test

### Step 7: Full build and unit test

```bash
go vet ./...
make -f Makefile build
make -f Makefile test
```

All unit tests must pass.

## Phase 5: Demo Replacement

### Step 8: Replace readAllBalances in txn-integrity-demo

**File**: `scripts/txn-integrity-demo-verify/main.go`

Replace `readAllBalances` (line 733) with Scan-based version per `01_design.md` §4.

**Verify**: `go vet ./scripts/...` && `go build ./scripts/txn-integrity-demo-verify/`

## Phase 6: Fuzz Test Replacement

### Step 9: Replace doAudit in fuzz test

**File**: `e2e_external/fuzz_cluster_test.go`

Replace `doAudit` Get loop with Scan per `01_design.md` §5.

**Verify**: `go vet ./e2e_external/...`

## Phase 7: E2E and Integration Test

### Step 10: Run E2E tests

```bash
make -f Makefile test-e2e-external
```

### Step 11: Run fuzz test

```bash
FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make -f Makefile test-fuzz-cluster
```

## Phase 8: Commit

```
feat: add TxnKV Scan API to client library and CLI

Add TxnHandle.Scan() with multi-region iteration, lock resolution,
and local membuf merge. Make CLI SCAN command context-aware (uses
TxnKV path inside BEGIN/COMMIT blocks). Add CLITxnScan e2elib helper.

Replace individual-Get loops with Scan in:
- txn-integrity-demo readAllBalances (N Gets → 1 Scan)
- fuzz test doAudit (5000 Gets → 1 Scan)

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
```
