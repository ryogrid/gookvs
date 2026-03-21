# Dynamic Node Addition — Implementation TODO

## Phase 1: Server-Side PDStoreResolver
- [x] Create `internal/server/pd_resolver.go` — PDStoreResolver implementing transport.StoreResolver with TTL cache
- [x] Create `internal/server/pd_resolver_test.go` — Unit tests (cache hit, cache miss/TTL, unknown store)
- [x] Modify `cmd/gookv-server/main.go` — Use PDStoreResolver when PD available
- [x] Run `go vet` and fix any issues
- [x] Run unit tests and verify pass

## Phase 2: Join Mode Startup
- [x] Create `internal/server/store_ident.go` — SaveStoreIdent/LoadStoreIdent for store ID persistence
- [x] Create `internal/server/store_ident_test.go` — Unit tests (save/load/missing file)
- [x] Modify `cmd/gookv-server/main.go` — Add join mode detection and joinCluster() function
- [x] Run `go vet` and fix any issues
- [x] Run unit tests and verify pass
- [x] Verify build succeeds

## Phase 3: Store State Machine in PD
- [x] Modify `internal/pd/server.go` — Add StoreState enum, storeStates map, state methods, config fields, runStoreStateWorker()
- [x] Modify `internal/pd/scheduler.go` — Use GetStoreState/IsStoreSchedulable instead of IsStoreAlive
- [x] Create `internal/pd/store_state_test.go` — State transition tests
- [x] Modify `internal/pd/scheduler_test.go` — Update for state-aware scheduling
- [x] Run `go vet` and fix any issues
- [x] Run unit tests and verify pass

## Phase 4: Region Balance + Excess Shedding Schedulers
- [x] Modify `internal/pd/scheduler.go` — Add scheduleExcessReplicaShedding(), scheduleRegionBalance(), update Schedule() priority chain, extend Scheduler struct
- [x] Modify `internal/pd/server.go` — Add GetRegionCountPerStore(), config fields, update constructor
- [x] Modify `internal/pd/scheduler_test.go` — Tests for new schedulers
- [x] Run `go vet` and fix any issues
- [x] Run unit tests and verify pass

## Phase 5: Multi-Step Move Tracking (MoveTracker)
- [x] Create `internal/pd/move_tracker.go` — MoveTracker, PendingMove, MoveState, Advance() logic
- [x] Create `internal/pd/move_tracker_test.go` — Full cycle, skip transfer, stale cleanup, rate limit
- [x] Modify `internal/pd/scheduler.go` — Wire moveTracker, call Advance() in Schedule()
- [x] Modify `internal/pd/server.go` — Create MoveTracker, wire into Scheduler, cleanup goroutine
- [x] Modify `internal/server/coordinator.go` — Add snapSemaphore for concurrent snapshot limit
- [x] Run `go vet` and fix any issues
- [x] Run unit tests and verify pass

## Phase 6: E2E Testing
- [ ] Create `e2e/add_node_test.go` — E2E tests for node join, region convergence, snapshot transfer
- [ ] Run E2E tests and verify pass

## Phase 7: gookv-ctl Extensions
- [ ] Modify `pkg/pdclient/client.go` — Add GetAllStores to Client interface and grpcClient implementation
- [ ] Modify `cmd/gookv-ctl/main.go` — Add store subcommand (list, status)
- [ ] Run `go vet` and fix any issues
- [ ] Verify build succeeds

## Final Quality Gates
- [ ] `go vet ./...` passes with no issues
- [ ] `make -f Makefile.gookv test` passes
- [ ] `make -f Makefile.gookv build` succeeds
- [ ] All TODO.md items marked [x]
- [ ] No leftover TODO/FIXME/HACK/XXX comments in scope
- [ ] Deferred items documented and reported
