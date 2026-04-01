# Performance Optimizations

## Phase 1: gRPC Connection Pooling
- [x] Create `pkg/client/conn_pool.go` with `ConnPool`, `storePool`, `dialFunc` injection
- [x] Add unit tests `pkg/client/conn_pool_test.go` (using fake dialer)
- [x] Add `ConnPoolSize` to `client.Config` (default 2)
- [x] Refactor `RegionRequestSender` to use `ConnPool`
- [x] Remove `getOrDial()`, `closeConn()`, `mu`, `conns` fields
- [x] Verify all existing dial options (`MaxCallRecvMsgSize`, keepalive) preserved
- [x] Update `SendToRegion` to use `pool.Get()` (no invalidation)
- [x] `go vet ./...` and `go build ./...`
- [x] Run race detector: `go test -race ./pkg/client/...`
- [x] Run existing e2e tests
- [x] Run fuzz test

## Phase 2: ReadIndex Batching
- [x] Create `internal/server/readindex_batcher.go` with `ReadIndexBatcher`
- [x] Batcher.Wait() accepts `time.Duration`
- [x] Retain `IsLeader()` pre-check in coordinator.ReadIndex()
- [x] Double-check pattern for lazy batcher creation
- [x] Stop() drains pending waiters with shutdown error
- [x] Add unit tests `internal/server/readindex_batcher_test.go`
- [x] Add `batchers map[uint64]*ReadIndexBatcher` to `StoreCoordinator`
- [x] Modify `coordinator.ReadIndex()` to delegate to batcher
- [x] Add batcher cleanup in `coordinator.Stop()`
- [x] Add idle batcher eviction (sweep every 10s)
- [x] `go vet ./...` and `go build ./...`
- [x] Run existing e2e tests
- [x] Run fuzz test

## Phase 3: Validation
- [x] Run fuzz test (2 consecutive passes at 100 iterations)
- [ ] Profile with pprof during load — Deferred: requires separate session with long-running test
- [ ] Document throughput numbers in `perf_analysis/` — Deferred: requires benchmark comparison

## Deferred
- Piggyback optimization (requires exporting Peer.AppliedIndex across packages)
