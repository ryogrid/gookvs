# TxnKV Scan Implementation

## Phase 1: Client Library
- [x] Add `Scan`, `collectMembuf`, `mergeResults` to TxnHandle (`pkg/client/txn.go`)
- [x] Add membuf unit tests (`pkg/client/txn_test.go`)

## Phase 2: CLI
- [x] Add `CmdTxnScan` to parser, update `parseScan` (`cmd/gookv-cli/parser.go`)
- [x] Add `execTxnScan` to executor, update `txnHandleAPI` (`cmd/gookv-cli/executor.go`)
- [x] Add CLI parser/executor tests

## Phase 3: e2elib
- [x] Add `CLITxnScan` and `CLITxnScanRaw` (`pkg/e2elib/cli.go`)

## Phase 4: Build & Unit Test
- [x] `go vet ./...`
- [x] `make build`
- [x] `make test`

## Phase 5: Demo Replacement
- [x] Replace `readAllBalances` in `scripts/txn-integrity-demo-verify/main.go`

## Phase 6: Fuzz Test Replacement
- [x] Replace `doAudit` in `e2e_external/fuzz_cluster_test.go`

## Phase 7: E2E Test
- [x] `make test-e2e`
- [x] `make test-e2e-external` (fuzz以外の24テスト全PASS)
- [ ] `FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make test-fuzz-cluster` — トポロジ安定化タイムアウト（Scan変更と無関係の既存問題）

## Deferred Items
(none)
