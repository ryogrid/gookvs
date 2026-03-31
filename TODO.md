# TxnKV Scan Implementation

## Phase 1: Client Library
- [ ] Add `Scan`, `collectMembuf`, `mergeResults` to TxnHandle (`pkg/client/txn.go`)
- [ ] Add membuf unit tests (`pkg/client/txn_test.go`)

## Phase 2: CLI
- [ ] Add `CmdTxnScan` to parser, update `parseScan` (`cmd/gookv-cli/parser.go`)
- [ ] Add `execTxnScan` to executor, update `txnHandleAPI` (`cmd/gookv-cli/executor.go`)
- [ ] Add CLI parser/executor tests

## Phase 3: e2elib
- [ ] Add `CLITxnScan` and `CLITxnScanRaw` (`pkg/e2elib/cli.go`)

## Phase 4: Build & Unit Test
- [ ] `go vet ./...`
- [ ] `make build`
- [ ] `make test`

## Phase 5: Demo Replacement
- [ ] Replace `readAllBalances` in `scripts/txn-integrity-demo-verify/main.go`

## Phase 6: Fuzz Test Replacement
- [ ] Replace `doAudit` in `e2e_external/fuzz_cluster_test.go`

## Phase 7: E2E Test
- [ ] `make test-e2e`
- [ ] `make test-e2e-external`
- [ ] `FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make test-fuzz-cluster`

## Deferred Items
(none yet)
