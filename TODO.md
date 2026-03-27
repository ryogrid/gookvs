# Code Review Fix Tracker

## 01 Raft Layer (`internal/raftstore/`)

### Critical
- [ ] C1: RecoverFromEngine never restores ApplyState from disk (storage.go:296)
- [ ] C2: PersistApplyState never called in production; apply progress not saved (storage.go:401, peer.go:599)
- [ ] C3: InitialState returns empty ConfState; multi-node restart fails (storage.go:68)
- [ ] C4: ApplySnapshot passes nil key range; stale data not cleared (snapshot.go:384)

### High
- [ ] H1: PeerMsgTypeDestroy closes Mailbox causing panic on concurrent sends (peer.go:425-428)
- [ ] H2: leaseExpiry accessed from multiple goroutines without sync (peer.go:131-132)
- [ ] H3: handleReady runs after peer destroyed (peer.go:376-378)
- [ ] H4: Admin entries sent to applyFunc; relies on format incompatibility (peer.go:579-600)
- [ ] C5/H5: Proposal callback index tracking breaks with batched proposals (peer.go:515-521)

### Medium
- [ ] M1: readEntriesFromEngine silently stops on gap (storage.go:481-484)
- [ ] M2: onRaftLogGCTick underflow when appliedIdx=1 (peer.go:696)
- [ ] M3: Snapshot ConfState not set in metadata (snapshot.go:353-359)
- [ ] M4: scanRegionSize picks split key from first CF only (split/checker.go:140-191)
- [ ] M5: ExecCommitMerge dead code and fragile fallback (merge.go:118-140)
- [ ] M6: Proposal callback always calls success regardless of entry data (peer.go:493-521)

## 02 Server/RPC Layer (`internal/server/`)

### Critical
- [ ] C1: Loopback routing uses source region ID for target peer (coordinator.go:770-783)
- [ ] C2: KVPessimisticRollback bypasses Raft in cluster mode (server.go:804-816)
- [ ] C3: KvTxnHeartBeat bypasses Raft in cluster mode (server.go:819-830)
- [ ] C4: C2+C3 skip validateRegionContext (server.go:804, 819)
- [ ] C5: ProposeModifies callback never invoked on silent drop (coordinator.go:317-343)
- [ ] C6: Transport creates new gRPC stream per Send (transport.go:78-102)

### Potential/Medium
- [ ] P1: ReadPool.Stop() doesn't drain pending tasks (flow/flow.go:112-114)
- [ ] P3: Connection pool always uses index 0 (transport.go:249)
- [ ] P4: TOCTOU in pd_resolver (pd_resolver.go:52-75)
- [ ] P6: KvDeleteRange hardcodes region ID 1 (server.go:1541)
- [ ] P7: RawPut/RawDelete skip region validation (server.go:1194-1233)

## 03 Storage/MVCC Layer

### High
- [x] BUG-01: Backward scan bound conditions swapped (scanner.go:63-74)
- [x] BUG-02: PointGetter does not skip pessimistic locks (point_getter.go:74-86)
- [x] BUG-03: Async commit only checks single write record (async_commit.go:44-52)

### Medium
- [ ] BUG-04: Latch spin-wait without backoff (server/storage.go)
- [x] ISSUE-05: CleanupModifies removes lock without rollback record (server/storage.go)
- [ ] ISSUE-06: GC runs without latches (gc/gc.go)
- [ ] ISSUE-07: GC does not remove Delete markers (gc/gc.go)

## 04 Client Library (`pkg/client/`)

### Critical
- [x] C1: Region cache never evicts stale entries after split (region_cache.go:132-154)
- [x] C2: BatchGet sends all keys to single region (txn.go:198-231)
- [ ] C3: commitSecondariesPerKey silently swallows TxnLockNotFound (committer.go:337-378)

### High
- [x] H1: isPrimaryCommitted swallows errors (committer.go:213-234)
- [x] H2: primaryKey() uses random map iteration (txn.go:316-322)
- [x] H3: DeleteRange does not span regions (rawkv.go:341-361)
- [x] H4: Checksum does not span regions (rawkv.go:394-419)

### Medium
- [ ] M1: prewriteRegion returns conflict instead of retrying after lock resolve (committer.go:193-201)
- [ ] M2: lockNotFoundRetries counter accumulates across retries (committer.go:294,344)
- [x] M3: isRetryableRegionError is dead code (request_sender.go:132-142)
- [ ] M4: lock_resolver resolving map grows unboundedly (lock_resolver.go:22-23,69)
- [ ] M5: Scan reuses stale scanEnd after region error (rawkv.go:274-338)

## 05 PD Layer (`internal/pd/`, `pkg/pdclient/`)

### Critical
- [ ] C1: Proposal index tracking reads stale LastIndex (raft_peer.go:250-255)
- [ ] C2: Non-atomic bootstrap in Raft mode (server.go:574-605)
- [ ] C3: ReportBatchSplit missing Leader field in Raft mode (server.go:858-864)
- [ ] C4: Snapshot doesn't include storeLastHeartbeat (snapshot.go)

### Moderate
- [ ] M1: TSO logical overflow may produce non-monotonic timestamps (server.go:1199-1221)
- [ ] M3: scheduleExcessReplicaShedding may remove leader (scheduler.go:156-187)
- [ ] M5: MockClient TSO race condition (pdclient/mock.go:64-80)

### Minor
- [ ] m1: GetSafePoint uses Mutex instead of RWMutex (server.go:1256)
- [ ] m5: Replace deprecated grpc.Dial with grpc.NewClient (various)

## 06 Entry/Config/Codec

### Critical
- [ ] C-1: Float64 datum encoding reads d.I64 instead of Float64bits (endpoint.go:609,652)

### High
- [ ] H-1: --store-id=0 with --initial-cluster creates invalid Raft peers (main.go)
- [ ] H-2: SelectionExecutor truthiness check uses I64 for all types (coprocessor.go:357)
- [ ] H-3: ReadableSize parser doesn't validate trailing chars (config.go:42-73)
- [ ] H-4: parseInitialCluster silently drops malformed entries (main.go:442-460)

### Medium
- [ ] M-2: EncodeRPNExpression only handles Int64/String constants (endpoint.go:500-528)
- [ ] M-4: Config validation doesn't check StatusAddr (config.go:240-291)

## 07 Test Code

### High
- [ ] 1.1: GC test doesn't verify old version deleted (gc_worker_test.go)
- [ ] 4.1: Log file handles leaked in e2elib process management (pdcluster.go, gokvnode.go)
- [ ] 4.2: PDCluster Restart() leaks old log file handle (pdcluster.go)

### Medium
- [ ] 4.4: PDCluster.Client() creates new client per call (pdcluster.go)
- [ ] 3.2: secondDuration uses raw nanosecond constant (cluster.go:143-144)
- [ ] 1.4: Cross-node replication test ignores all errors (cluster_server_test.go)
- [ ] 1.5: TSO via follower test has no failure assertions (pd_replication_test.go)
- [ ] 1.6: TSO forwarding test passes with zero successes (pd_replication_test.go)
- [ ] 5.1: Duplicate newClusterWithLeader/newClientCluster helpers (e2e_external/)
