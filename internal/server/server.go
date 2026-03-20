package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	copkg "github.com/ryogrid/gookv/internal/coprocessor"
	"github.com/ryogrid/gookv/internal/storage/gc"
	"github.com/ryogrid/gookv/internal/storage/mvcc"
	"github.com/ryogrid/gookv/internal/storage/txn"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/ryogrid/gookv/pkg/txntypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// ServerConfig holds configuration for the gRPC server.
type ServerConfig struct {
	ListenAddr string
	ClusterID  uint64
}

// Server encapsulates the gRPC server and all server-side components.
type Server struct {
	cfg         ServerConfig
	grpcServer  *grpc.Server
	storage     *Storage
	rawStorage  *RawStorage
	gcWorker    *gc.GCWorker
	coordinator *StoreCoordinator
	pdClient    pdclient.Client // optional PD client for TSO allocation (nil = no PD)
	listener    net.Listener

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// SetCoordinator sets the StoreCoordinator for Raft message handling.
// Must be called before Start() if Raft endpoints are needed.
func (s *Server) SetCoordinator(coord *StoreCoordinator) {
	s.coordinator = coord
}

// SetPDClient sets the PD client for TSO allocation.
// If set, the server uses PD-allocated timestamps for 1PC and async commit paths.
func (s *Server) SetPDClient(client pdclient.Client) {
	s.pdClient = client
}

// NewServer creates a Server with all dependencies.
func NewServer(cfg ServerConfig, storage *Storage) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	opts := buildServerOptions(cfg)
	grpcSrv := grpc.NewServer(opts...)

	gcWorker := gc.NewGCWorker(storage.Engine(), gc.DefaultGCConfig())
	gcWorker.Start()

	s := &Server{
		cfg:        cfg,
		grpcServer: grpcSrv,
		storage:    storage,
		rawStorage: NewRawStorage(storage.Engine()),
		gcWorker:   gcWorker,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Register the TikvService.
	tikvpb.RegisterTikvServer(grpcSrv, &tikvService{server: s})

	// Enable gRPC server reflection for tools like grpcurl.
	reflection.Register(grpcSrv)

	return s
}

// Start binds the gRPC server and begins accepting connections.
func (s *Server) Start() error {
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.cfg.ListenAddr, err)
	}
	s.listener = lis

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.grpcServer.Serve(lis); err != nil {
			// grpc.Server.Serve returns on GracefulStop, so only log unexpected errors.
			select {
			case <-s.ctx.Done():
				// Normal shutdown.
			default:
				fmt.Printf("gRPC server error: %v\n", err)
			}
		}
	}()

	return nil
}

// Addr returns the listener address. Only valid after Start().
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.cfg.ListenAddr
}

// Stop gracefully shuts down all server components.
func (s *Server) Stop() {
	s.cancel()
	if s.gcWorker != nil {
		s.gcWorker.Stop()
	}
	s.grpcServer.GracefulStop()
	s.wg.Wait()
}

// buildServerOptions creates gRPC server options.
func buildServerOptions(cfg ServerConfig) []grpc.ServerOption {
	var opts []grpc.ServerOption

	if cfg.ClusterID != 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(
			clusterIDInterceptor(cfg.ClusterID),
		))
	}

	opts = append(opts,
		grpc.MaxRecvMsgSize(16*1024*1024), // 16 MB
		grpc.MaxSendMsgSize(16*1024*1024), // 16 MB
	)

	return opts
}

// clusterIDInterceptor validates cluster ID on every unary request.
func clusterIDInterceptor(expectedID uint64) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {
		// Extract cluster ID from context field if present.
		type contextHolder interface {
			GetContext() *kvrpcpb.Context
		}
		if ch, ok := req.(contextHolder); ok {
			if rctx := ch.GetContext(); rctx != nil {
				// Note: kvrpcpb.Context doesn't directly expose ClusterID in all versions,
				// but the pattern is here for when it does.
			}
		}
		return handler(ctx, req)
	}
}

// tikvService implements the tikvpb.TikvServer interface.
type tikvService struct {
	tikvpb.UnimplementedTikvServer
	server *Server
}

// KvGet implements transactional point read.
func (svc *tikvService) KvGet(ctx context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	resp := &kvrpcpb.GetResponse{}

	value, err := svc.server.storage.Get(req.GetKey(), txntypes.TimeStamp(req.GetVersion()))
	if err != nil {
		if errors.Is(err, mvcc.ErrKeyIsLocked) {
			resp.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{
					Key:         req.GetKey(),
					LockVersion: req.GetVersion(),
				},
			}
			return resp, nil
		}
		return nil, status.Errorf(codes.Internal, "get failed: %v", err)
	}

	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}

	return resp, nil
}

// KvScan implements transactional range scan.
func (svc *tikvService) KvScan(ctx context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	resp := &kvrpcpb.ScanResponse{}

	pairs, err := svc.server.storage.Scan(
		req.GetStartKey(),
		req.GetEndKey(),
		req.GetLimit(),
		txntypes.TimeStamp(req.GetVersion()),
		req.GetKeyOnly(),
	)
	if err != nil {
		if errors.Is(err, mvcc.ErrKeyIsLocked) {
			resp.Error = &kvrpcpb.KeyError{
				Locked: &kvrpcpb.LockInfo{},
			}
			return resp, nil
		}
		return nil, status.Errorf(codes.Internal, "scan failed: %v", err)
	}

	for _, p := range pairs {
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   p.Key,
			Value: p.Value,
		})
	}

	return resp, nil
}

// KvPrewrite implements the first phase of 2PC, with async commit and 1PC support.
func (svc *tikvService) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	resp := &kvrpcpb.PrewriteResponse{}

	mutations := make([]txn.Mutation, len(req.GetMutations()))
	for i, m := range req.GetMutations() {
		var op txn.MutationOp
		switch m.GetOp() {
		case kvrpcpb.Op_Put, kvrpcpb.Op_Insert:
			op = txn.MutationOpPut
		case kvrpcpb.Op_Del:
			op = txn.MutationOpDelete
		case kvrpcpb.Op_Lock, kvrpcpb.Op_CheckNotExists:
			op = txn.MutationOpLock
		default:
			op = txn.MutationOpPut
		}
		mutations[i] = txn.Mutation{
			Op:    op,
			Key:   m.GetKey(),
			Value: m.GetValue(),
		}
	}

	startTS := txntypes.TimeStamp(req.GetStartVersion())
	primary := req.GetPrimaryLock()
	lockTTL := req.GetLockTtl()

	// --- 1PC path ---
	if req.GetTryOnePc() && txn.Is1PCEligible(mutations, 0) {
		commitTS := startTS + 1
		// If PD client is available, allocate commitTS from PD's TSO.
		if svc.server.pdClient != nil {
			pdTS, err := svc.server.pdClient.GetTS(ctx)
			if err == nil {
				commitTS = txntypes.TimeStamp(pdTS.ToUint64())
			}
			// On PD error, fall back to startTS+1.
		}
		if coord := svc.server.coordinator; coord != nil {
			modifies, errs, onePCCommitTS := svc.server.storage.Prewrite1PCModifies(mutations, primary, startTS, commitTS, lockTTL)
			for _, err := range errs {
				if err != nil {
					resp.Errors = append(resp.Errors, errToKeyError(err))
				}
			}
			if len(resp.Errors) > 0 || len(modifies) == 0 {
				return resp, nil
			}
			regionID := svc.resolveRegionID(primary)
			if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
				return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
			}
			resp.OnePcCommitTs = uint64(onePCCommitTS)
		} else {
			errs, onePCCommitTS := svc.server.storage.Prewrite1PC(mutations, primary, startTS, commitTS, lockTTL)
			for _, err := range errs {
				if err != nil {
					resp.Errors = append(resp.Errors, errToKeyError(err))
				}
			}
			if len(resp.Errors) == 0 {
				resp.OnePcCommitTs = uint64(onePCCommitTS)
			}
		}
		return resp, nil
	}

	// --- Async commit path ---
	if req.GetUseAsyncCommit() && txn.IsAsyncCommitEligible(mutations, 0) {
		secondaries := req.GetSecondaries()
		maxCommitTS := txntypes.TimeStamp(req.GetMaxCommitTs())
		// If maxCommitTS is 0 and PD client is available, get a timestamp from PD.
		if maxCommitTS == 0 && svc.server.pdClient != nil {
			pdTS, err := svc.server.pdClient.GetTS(ctx)
			if err == nil {
				maxCommitTS = txntypes.TimeStamp(pdTS.ToUint64())
			}
			// On PD error, proceed with maxCommitTS=0 (fallback behavior).
		}
		if coord := svc.server.coordinator; coord != nil {
			modifies, errs, minCommitTS := svc.server.storage.PrewriteAsyncCommitModifies(mutations, primary, startTS, lockTTL, secondaries, maxCommitTS)
			for _, err := range errs {
				if err != nil {
					resp.Errors = append(resp.Errors, errToKeyError(err))
				}
			}
			if len(resp.Errors) > 0 || len(modifies) == 0 {
				return resp, nil
			}
			regionID := svc.resolveRegionID(primary)
			if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
				return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
			}
			resp.MinCommitTs = uint64(minCommitTS)
		} else {
			errs, minCommitTS := svc.server.storage.PrewriteAsyncCommit(mutations, primary, startTS, lockTTL, secondaries, maxCommitTS)
			for _, err := range errs {
				if err != nil {
					resp.Errors = append(resp.Errors, errToKeyError(err))
				}
			}
			if len(resp.Errors) == 0 {
				resp.MinCommitTs = uint64(minCommitTS)
			}
		}
		return resp, nil
	}

	// --- Standard 2PC path ---
	// Cluster mode: compute modifications then propose via Raft.
	if coord := svc.server.coordinator; coord != nil {
		modifies, errs := svc.server.storage.PrewriteModifies(mutations, primary, startTS, lockTTL)
		for _, err := range errs {
			if err != nil {
				keyErr := errToKeyError(err)
				resp.Errors = append(resp.Errors, keyErr)
			}
		}
		if len(resp.Errors) > 0 || len(modifies) == 0 {
			return resp, nil
		}
		// Propose modifications via Raft for replication to all nodes.
		regionID := svc.resolveRegionID(primary)
		if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
		return resp, nil
	}

	// Standalone mode: direct write.
	errs := svc.server.storage.Prewrite(mutations, primary, startTS, lockTTL)
	for _, err := range errs {
		if err != nil {
			keyErr := errToKeyError(err)
			resp.Errors = append(resp.Errors, keyErr)
		}
	}

	return resp, nil
}

// KvCommit implements the second phase of 2PC.
func (svc *tikvService) KvCommit(ctx context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	resp := &kvrpcpb.CommitResponse{}

	keys := req.GetKeys()
	startTS := txntypes.TimeStamp(req.GetStartVersion())
	commitTS := txntypes.TimeStamp(req.GetCommitVersion())

	// Cluster mode: compute modifications then propose via Raft.
	if coord := svc.server.coordinator; coord != nil {
		modifies, err := svc.server.storage.CommitModifies(keys, startTS, commitTS)
		if err != nil {
			resp.Error = errToKeyError(err)
			return resp, nil
		}
		if len(modifies) > 0 {
			regionID := svc.resolveRegionID(keys[0])
			if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
				return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
			}
		}
		return resp, nil
	}

	// Standalone mode: direct write.
	err := svc.server.storage.Commit(keys, startTS, commitTS)
	if err != nil {
		resp.Error = errToKeyError(err)
	}

	return resp, nil
}

// KvBatchGet implements transactional multi-key read.
func (svc *tikvService) KvBatchGet(ctx context.Context, req *kvrpcpb.BatchGetRequest) (*kvrpcpb.BatchGetResponse, error) {
	resp := &kvrpcpb.BatchGetResponse{}

	pairs, err := svc.server.storage.BatchGet(
		req.GetKeys(),
		txntypes.TimeStamp(req.GetVersion()),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "batch get failed: %v", err)
	}

	for _, p := range pairs {
		if p.Err != nil {
			if errors.Is(p.Err, mvcc.ErrKeyIsLocked) {
				resp.Error = &kvrpcpb.KeyError{
					Locked: &kvrpcpb.LockInfo{
						Key: p.Key,
					},
				}
				return resp, nil
			}
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   p.Key,
			Value: p.Value,
		})
	}

	return resp, nil
}

// KvBatchRollback implements batch rollback.
func (svc *tikvService) KvBatchRollback(ctx context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	resp := &kvrpcpb.BatchRollbackResponse{}

	err := svc.server.storage.BatchRollback(
		req.GetKeys(),
		txntypes.TimeStamp(req.GetStartVersion()),
	)
	if err != nil {
		resp.Error = errToKeyError(err)
	}

	return resp, nil
}

// KvCleanup implements lock cleanup.
func (svc *tikvService) KvCleanup(ctx context.Context, req *kvrpcpb.CleanupRequest) (*kvrpcpb.CleanupResponse, error) {
	resp := &kvrpcpb.CleanupResponse{}

	commitTS, err := svc.server.storage.Cleanup(
		req.GetKey(),
		txntypes.TimeStamp(req.GetStartVersion()),
	)
	if err != nil {
		resp.Error = errToKeyError(err)
	}
	if commitTS != 0 {
		resp.CommitVersion = uint64(commitTS)
	}

	return resp, nil
}

// KvCheckTxnStatus implements transaction status check.
func (svc *tikvService) KvCheckTxnStatus(ctx context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	resp := &kvrpcpb.CheckTxnStatusResponse{}

	txnStatus, err := svc.server.storage.CheckTxnStatus(
		req.GetPrimaryKey(),
		txntypes.TimeStamp(req.GetLockTs()),
	)
	if err != nil {
		resp.Error = errToKeyError(err)
		return resp, nil
	}

	if txnStatus.IsLocked && txnStatus.Lock != nil {
		resp.LockTtl = txnStatus.Lock.TTL
		resp.LockInfo = &kvrpcpb.LockInfo{
			PrimaryLock: txnStatus.Lock.Primary,
			LockVersion: uint64(txnStatus.Lock.StartTS),
			LockTtl:     txnStatus.Lock.TTL,
		}
	} else if txnStatus.CommitTS != 0 {
		resp.CommitVersion = uint64(txnStatus.CommitTS)
	}
	// else: rolled back (both LockTtl and CommitVersion are 0)

	return resp, nil
}

// --- Pessimistic Lock / ResolveLock / TxnHeartBeat handlers ---

// KvPessimisticLock implements the KvPessimisticLock RPC.
func (svc *tikvService) KvPessimisticLock(ctx context.Context, req *kvrpcpb.PessimisticLockRequest) (*kvrpcpb.PessimisticLockResponse, error) {
	resp := &kvrpcpb.PessimisticLockResponse{}
	keys := make([][]byte, len(req.GetMutations()))
	for i, m := range req.GetMutations() {
		keys[i] = m.GetKey()
	}
	primary := req.GetPrimaryLock()
	startTS := txntypes.TimeStamp(req.GetStartVersion())
	forUpdateTS := txntypes.TimeStamp(req.GetForUpdateTs())
	lockTTL := req.GetLockTtl()

	if coord := svc.server.coordinator; coord != nil {
		modifies, errs := svc.server.storage.PessimisticLockModifies(keys, primary, startTS, forUpdateTS, lockTTL)
		for _, err := range errs {
			if err != nil {
				resp.Errors = append(resp.Errors, errToKeyError(err))
			}
		}
		if len(resp.Errors) > 0 || len(modifies) == 0 {
			return resp, nil
		}
		regionID := svc.resolveRegionID(primary)
		if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		errs := svc.server.storage.PessimisticLock(keys, primary, startTS, forUpdateTS, lockTTL)
		for _, err := range errs {
			if err != nil {
				resp.Errors = append(resp.Errors, errToKeyError(err))
			}
		}
	}
	return resp, nil
}

// KVPessimisticRollback implements the KVPessimisticRollback RPC.
func (svc *tikvService) KVPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
	resp := &kvrpcpb.PessimisticRollbackResponse{}
	startTS := txntypes.TimeStamp(req.GetStartVersion())
	forUpdateTS := txntypes.TimeStamp(req.GetForUpdateTs())

	errs := svc.server.storage.PessimisticRollbackKeys(req.GetKeys(), startTS, forUpdateTS)
	for _, err := range errs {
		if err != nil {
			resp.Errors = append(resp.Errors, errToKeyError(err))
		}
	}
	return resp, nil
}

// KvTxnHeartBeat implements the KvTxnHeartBeat RPC.
func (svc *tikvService) KvTxnHeartBeat(ctx context.Context, req *kvrpcpb.TxnHeartBeatRequest) (*kvrpcpb.TxnHeartBeatResponse, error) {
	resp := &kvrpcpb.TxnHeartBeatResponse{}
	startTS := txntypes.TimeStamp(req.GetStartVersion())

	ttl, err := svc.server.storage.TxnHeartBeat(req.GetPrimaryLock(), startTS, req.GetAdviseLockTtl())
	if err != nil {
		resp.Error = errToKeyError(err)
		return resp, nil
	}
	resp.LockTtl = ttl
	return resp, nil
}

// KvResolveLock implements the KvResolveLock RPC.
func (svc *tikvService) KvResolveLock(ctx context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	resp := &kvrpcpb.ResolveLockResponse{}
	startTS := txntypes.TimeStamp(req.GetStartVersion())
	commitTS := txntypes.TimeStamp(req.GetCommitVersion())

	if coord := svc.server.coordinator; coord != nil {
		modifies, err := svc.server.storage.ResolveLockModifies(startTS, commitTS, req.GetKeys())
		if err != nil {
			resp.Error = errToKeyError(err)
			return resp, nil
		}
		if len(modifies) > 0 {
			regionID := uint64(1)
			if reqKeys := req.GetKeys(); len(reqKeys) > 0 {
				regionID = svc.resolveRegionID(reqKeys[0])
			}
			if err := coord.ProposeModifies(regionID, modifies, 10*time.Second); err != nil {
				return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
			}
		}
	} else {
		if err := svc.server.storage.ResolveLock(startTS, commitTS, req.GetKeys()); err != nil {
			resp.Error = errToKeyError(err)
		}
	}
	return resp, nil
}

// --- Async Commit / ScanLock handlers ---

// KvCheckSecondaryLocks implements the KvCheckSecondaryLocks RPC for async commit.
func (svc *tikvService) KvCheckSecondaryLocks(ctx context.Context, req *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
	resp := &kvrpcpb.CheckSecondaryLocksResponse{}
	startTS := txntypes.TimeStamp(req.GetStartVersion())

	statuses, commitTS, err := svc.server.storage.CheckSecondaryLocks(req.GetKeys(), startTS)
	if err != nil {
		resp.Error = errToKeyError(err)
		return resp, nil
	}

	for _, s := range statuses {
		if s.Lock != nil {
			resp.Locks = append(resp.Locks, lockToLockInfo(s.Key, s.Lock))
		}
	}
	if commitTS != 0 {
		resp.CommitTs = uint64(commitTS)
	}

	return resp, nil
}

// KvScanLock implements the KvScanLock RPC, scanning for locks with StartTS <= maxVersion.
func (svc *tikvService) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
	resp := &kvrpcpb.ScanLockResponse{}

	results, err := svc.server.storage.ScanLock(
		txntypes.TimeStamp(req.GetMaxVersion()),
		req.GetStartKey(),
		req.GetEndKey(),
		req.GetLimit(),
	)
	if err != nil {
		resp.Error = errToKeyError(err)
		return resp, nil
	}

	for _, r := range results {
		resp.Locks = append(resp.Locks, lockToLockInfo(r.Key, r.Lock))
	}

	return resp, nil
}

// lockToLockInfo converts an internal Lock and its user key to a kvrpcpb.LockInfo proto.
func lockToLockInfo(key []byte, lock *txntypes.Lock) *kvrpcpb.LockInfo {
	info := &kvrpcpb.LockInfo{
		PrimaryLock:     lock.Primary,
		LockVersion:     uint64(lock.StartTS),
		Key:             key,
		LockTtl:         lock.TTL,
		TxnSize:         lock.TxnSize,
		LockForUpdateTs: uint64(lock.ForUpdateTS),
		UseAsyncCommit:  lock.UseAsyncCommit,
		MinCommitTs:     uint64(lock.MinCommitTS),
		Secondaries:     lock.Secondaries,
	}

	// Map internal LockType to proto Op.
	switch lock.LockType {
	case txntypes.LockTypePut:
		info.LockType = kvrpcpb.Op_Put
	case txntypes.LockTypeDelete:
		info.LockType = kvrpcpb.Op_Del
	case txntypes.LockTypeLock:
		info.LockType = kvrpcpb.Op_Lock
	case txntypes.LockTypePessimistic:
		info.LockType = kvrpcpb.Op_PessimisticLock
	default:
		info.LockType = kvrpcpb.Op_Put
	}

	return info
}

// --- Raw KV handlers ---

// RawGet implements the RawGet RPC.
func (svc *tikvService) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	value, err := svc.server.rawStorage.Get(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if value == nil {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// resolveRegionID returns the region ID for the given key using the coordinator's
// region routing. Falls back to region ID 1 if the coordinator has no ResolveRegionForKey
// or if the key doesn't match any region.
func (svc *tikvService) resolveRegionID(key []byte) uint64 {
	if coord := svc.server.coordinator; coord != nil {
		if rid := coord.ResolveRegionForKey(key); rid != 0 {
			return rid
		}
	}
	return 1
}

// groupModifiesByRegion groups modifies by their target region.
// This is used for batch Raw KV operations where keys may span multiple regions.
func (svc *tikvService) groupModifiesByRegion(modifies []mvcc.Modify) map[uint64][]mvcc.Modify {
	groups := make(map[uint64][]mvcc.Modify)
	for _, m := range modifies {
		regionID := svc.resolveRegionID(m.Key)
		groups[regionID] = append(groups[regionID], m)
	}
	return groups
}

// proposeModifiesToRegions proposes modifies grouped by region, each to its own region leader.
func (svc *tikvService) proposeModifiesToRegions(coord *StoreCoordinator, modifies []mvcc.Modify, timeout time.Duration) error {
	groups := svc.groupModifiesByRegion(modifies)
	for regionID, regionModifies := range groups {
		if err := coord.ProposeModifies(regionID, regionModifies, timeout); err != nil {
			return err
		}
	}
	return nil
}

// RawPut implements the RawPut RPC.
func (svc *tikvService) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	ttl := req.GetTtl()
	if coord := svc.server.coordinator; coord != nil {
		modify := svc.server.rawStorage.PutModify(req.GetCf(), req.GetKey(), req.GetValue(), ttl)
		regionID := svc.resolveRegionID(req.GetKey())
		if err := coord.ProposeModifies(regionID, []mvcc.Modify{modify}, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.Put(req.GetCf(), req.GetKey(), req.GetValue(), ttl); err != nil {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

// RawDelete implements the RawDelete RPC.
func (svc *tikvService) RawDelete(ctx context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	if coord := svc.server.coordinator; coord != nil {
		modify := svc.server.rawStorage.DeleteModify(req.GetCf(), req.GetKey())
		regionID := svc.resolveRegionID(req.GetKey())
		if err := coord.ProposeModifies(regionID, []mvcc.Modify{modify}, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.Delete(req.GetCf(), req.GetKey()); err != nil {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

// RawScan implements the RawScan RPC.
func (svc *tikvService) RawScan(ctx context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	pairs, err := svc.server.rawStorage.Scan(
		req.GetCf(), req.GetStartKey(), req.GetEndKey(),
		req.GetLimit(), req.GetKeyOnly(), req.GetReverse(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "raw scan failed: %v", err)
	}
	for _, p := range pairs {
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: p.Key, Value: p.Value})
	}
	return resp, nil
}

// RawBatchGet implements the RawBatchGet RPC.
func (svc *tikvService) RawBatchGet(ctx context.Context, req *kvrpcpb.RawBatchGetRequest) (*kvrpcpb.RawBatchGetResponse, error) {
	resp := &kvrpcpb.RawBatchGetResponse{}
	pairs, err := svc.server.rawStorage.BatchGet(req.GetCf(), req.GetKeys())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "raw batch get failed: %v", err)
	}
	for _, p := range pairs {
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{Key: p.Key, Value: p.Value})
	}
	return resp, nil
}

// RawBatchPut implements the RawBatchPut RPC.
func (svc *tikvService) RawBatchPut(ctx context.Context, req *kvrpcpb.RawBatchPutRequest) (*kvrpcpb.RawBatchPutResponse, error) {
	resp := &kvrpcpb.RawBatchPutResponse{}
	pairs := make([]KvPair, len(req.GetPairs()))
	for i, p := range req.GetPairs() {
		pairs[i] = KvPair{Key: p.GetKey(), Value: p.GetValue()}
	}

	// Resolve per-key TTLs: prefer Ttls (per-key), fall back to deprecated Ttl (uniform).
	ttls := resolveBatchTTLs(req.GetTtls(), req.GetTtl(), len(pairs))

	if coord := svc.server.coordinator; coord != nil {
		modifies := make([]mvcc.Modify, len(pairs))
		for i, p := range pairs {
			var t uint64
			if i < len(ttls) {
				t = ttls[i]
			}
			modifies[i] = svc.server.rawStorage.PutModify(req.GetCf(), p.Key, p.Value, t)
		}
		if err := svc.proposeModifiesToRegions(coord, modifies, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.BatchPutWithTTL(req.GetCf(), pairs, ttls); err != nil {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

// resolveBatchTTLs resolves per-key TTLs from the request fields.
// If ttls has exactly one element, it applies to all keys.
// If ttls matches nPairs, use as-is.
// Otherwise fall back to the deprecated uniform ttl field.
func resolveBatchTTLs(ttls []uint64, uniformTTL uint64, nPairs int) []uint64 {
	if len(ttls) == nPairs {
		return ttls
	}
	if len(ttls) == 1 {
		result := make([]uint64, nPairs)
		for i := range result {
			result[i] = ttls[0]
		}
		return result
	}
	if uniformTTL > 0 {
		result := make([]uint64, nPairs)
		for i := range result {
			result[i] = uniformTTL
		}
		return result
	}
	return nil
}

// RawBatchDelete implements the RawBatchDelete RPC.
func (svc *tikvService) RawBatchDelete(ctx context.Context, req *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	resp := &kvrpcpb.RawBatchDeleteResponse{}
	if coord := svc.server.coordinator; coord != nil {
		modifies := make([]mvcc.Modify, len(req.GetKeys()))
		for i, key := range req.GetKeys() {
			modifies[i] = svc.server.rawStorage.DeleteModify(req.GetCf(), key)
		}
		if err := svc.proposeModifiesToRegions(coord, modifies, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.BatchDelete(req.GetCf(), req.GetKeys()); err != nil {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

// RawDeleteRange implements the RawDeleteRange RPC.
func (svc *tikvService) RawDeleteRange(ctx context.Context, req *kvrpcpb.RawDeleteRangeRequest) (*kvrpcpb.RawDeleteRangeResponse, error) {
	resp := &kvrpcpb.RawDeleteRangeResponse{}
	if err := svc.server.rawStorage.DeleteRange(req.GetCf(), req.GetStartKey(), req.GetEndKey()); err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawBatchScan implements the RawBatchScan RPC.
func (svc *tikvService) RawBatchScan(ctx context.Context, req *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
	resp := &kvrpcpb.RawBatchScanResponse{}

	ranges := make([]KeyRange, len(req.GetRanges()))
	for i, r := range req.GetRanges() {
		ranges[i] = KeyRange{StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
	}

	pairs, err := svc.server.rawStorage.BatchScan(
		req.GetCf(), ranges, req.GetEachLimit(), req.GetKeyOnly(), req.GetReverse(),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "raw batch scan failed: %v", err)
	}
	for _, p := range pairs {
		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: p.Key, Value: p.Value})
	}
	return resp, nil
}

// RawGetKeyTTL implements the RawGetKeyTTL RPC.
func (svc *tikvService) RawGetKeyTTL(ctx context.Context, req *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
	resp := &kvrpcpb.RawGetKeyTTLResponse{}

	ttl, notFound, err := svc.server.rawStorage.GetKeyTTL(req.GetCf(), req.GetKey())
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	if notFound {
		resp.NotFound = true
	} else {
		resp.Ttl = ttl
	}
	return resp, nil
}

// RawCompareAndSwap implements the RawCompareAndSwap RPC.
func (svc *tikvService) RawCompareAndSwap(ctx context.Context, req *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
	resp := &kvrpcpb.RawCASResponse{}

	succeed, prevNotExist, prevValue, err := svc.server.rawStorage.CompareAndSwap(
		req.GetCf(),
		req.GetKey(),
		req.GetValue(),
		req.GetPreviousValue(),
		req.GetPreviousNotExist(),
		req.GetDelete(),
		req.GetTtl(),
	)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	resp.Succeed = succeed
	resp.PreviousNotExist = prevNotExist
	resp.PreviousValue = prevValue
	return resp, nil
}

// RawChecksum implements the RawChecksum RPC.
func (svc *tikvService) RawChecksum(ctx context.Context, req *kvrpcpb.RawChecksumRequest) (*kvrpcpb.RawChecksumResponse, error) {
	resp := &kvrpcpb.RawChecksumResponse{}

	ranges := make([]KeyRange, len(req.GetRanges()))
	for i, r := range req.GetRanges() {
		ranges[i] = KeyRange{StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
	}

	checksum, totalKvs, totalBytes, err := svc.server.rawStorage.Checksum(cfnames.CFDefault, ranges)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}

	resp.Checksum = checksum
	resp.TotalKvs = totalKvs
	resp.TotalBytes = totalBytes
	return resp, nil
}

// --- GC handler ---

// KvGC implements the KvGC RPC.
func (svc *tikvService) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
	resp := &kvrpcpb.GCResponse{}
	if svc.server.gcWorker == nil {
		return resp, nil
	}

	done := make(chan error, 1)
	task := gc.GCTask{
		SafePoint: txntypes.TimeStamp(req.GetSafePoint()),
		Callback:  func(err error) { done <- err },
	}
	if err := svc.server.gcWorker.Schedule(task); err != nil {
		resp.Error = errToKeyError(err)
		return resp, nil
	}

	select {
	case err := <-done:
		if err != nil {
			resp.Error = errToKeyError(err)
		}
	case <-ctx.Done():
		resp.Error = errToKeyError(ctx.Err())
	}
	return resp, nil
}

// --- Coprocessor handlers ---

// Coprocessor implements the Coprocessor RPC.
func (svc *tikvService) Coprocessor(ctx context.Context, req *coprocessor.Request) (*coprocessor.Response, error) {
	ep := copkg.NewEndpoint(svc.server.storage.Engine())

	ranges := make([]copkg.CopKeyRange, len(req.GetRanges()))
	for i, r := range req.GetRanges() {
		ranges[i] = copkg.CopKeyRange{Start: r.GetStart(), End: r.GetEnd()}
	}

	result, err := ep.Handle(ctx, req.GetTp(), req.GetData(), req.GetStartTs(), ranges)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "coprocessor failed: %v", err)
	}

	resp := &coprocessor.Response{}
	if result.OtherError != "" {
		resp.OtherError = result.OtherError
	} else {
		resp.Data = result.Data
	}
	return resp, nil
}

// CoprocessorStream implements the CoprocessorStream RPC.
func (svc *tikvService) CoprocessorStream(req *coprocessor.Request, stream tikvpb.Tikv_CoprocessorStreamServer) error {
	ep := copkg.NewEndpoint(svc.server.storage.Engine())

	ranges := make([]copkg.CopKeyRange, len(req.GetRanges()))
	for i, r := range req.GetRanges() {
		ranges[i] = copkg.CopKeyRange{Start: r.GetStart(), End: r.GetEnd()}
	}

	return ep.HandleStream(stream.Context(), req.GetTp(), req.GetData(), req.GetStartTs(), ranges,
		func(result *copkg.CopResponse) error {
			resp := &coprocessor.Response{}
			if result.OtherError != "" {
				resp.OtherError = result.OtherError
			} else {
				resp.Data = result.Data
			}
			return stream.Send(resp)
		},
	)
}

// BatchCommands implements the multiplexed bidirectional streaming RPC.
func (svc *tikvService) BatchCommands(stream tikvpb.Tikv_BatchCommandsServer) error {
	for {
		batchReq, err := stream.Recv()
		if err != nil {
			return err
		}

		batchResp := &tikvpb.BatchCommandsResponse{
			RequestIds: batchReq.GetRequestIds(),
		}

		for _, req := range batchReq.GetRequests() {
			resp := svc.handleBatchCmd(stream.Context(), req)
			batchResp.Responses = append(batchResp.Responses, resp)
		}

		if err := stream.Send(batchResp); err != nil {
			return err
		}
	}
}

// handleBatchCmd routes a single sub-command within BatchCommands.
func (svc *tikvService) handleBatchCmd(ctx context.Context, req *tikvpb.BatchCommandsRequest_Request) *tikvpb.BatchCommandsResponse_Response {
	resp := &tikvpb.BatchCommandsResponse_Response{}

	switch cmd := req.GetCmd().(type) {
	case *tikvpb.BatchCommandsRequest_Request_Get:
		r, _ := svc.KvGet(ctx, cmd.Get)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Get{Get: r}

	case *tikvpb.BatchCommandsRequest_Request_Scan:
		r, _ := svc.KvScan(ctx, cmd.Scan)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Scan{Scan: r}

	case *tikvpb.BatchCommandsRequest_Request_Prewrite:
		r, _ := svc.KvPrewrite(ctx, cmd.Prewrite)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Prewrite{Prewrite: r}

	case *tikvpb.BatchCommandsRequest_Request_Commit:
		r, _ := svc.KvCommit(ctx, cmd.Commit)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Commit{Commit: r}

	case *tikvpb.BatchCommandsRequest_Request_BatchGet:
		r, _ := svc.KvBatchGet(ctx, cmd.BatchGet)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_BatchGet{BatchGet: r}

	case *tikvpb.BatchCommandsRequest_Request_BatchRollback:
		r, _ := svc.KvBatchRollback(ctx, cmd.BatchRollback)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_BatchRollback{BatchRollback: r}

	case *tikvpb.BatchCommandsRequest_Request_Cleanup:
		r, _ := svc.KvCleanup(ctx, cmd.Cleanup)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Cleanup{Cleanup: r}

	case *tikvpb.BatchCommandsRequest_Request_CheckTxnStatus:
		r, _ := svc.KvCheckTxnStatus(ctx, cmd.CheckTxnStatus)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_CheckTxnStatus{CheckTxnStatus: r}

	case *tikvpb.BatchCommandsRequest_Request_PessimisticLock:
		r, _ := svc.KvPessimisticLock(ctx, cmd.PessimisticLock)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_PessimisticLock{PessimisticLock: r}

	case *tikvpb.BatchCommandsRequest_Request_PessimisticRollback:
		r, _ := svc.KVPessimisticRollback(ctx, cmd.PessimisticRollback)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_PessimisticRollback{PessimisticRollback: r}

	case *tikvpb.BatchCommandsRequest_Request_TxnHeartBeat:
		r, _ := svc.KvTxnHeartBeat(ctx, cmd.TxnHeartBeat)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_TxnHeartBeat{TxnHeartBeat: r}

	case *tikvpb.BatchCommandsRequest_Request_ResolveLock:
		r, _ := svc.KvResolveLock(ctx, cmd.ResolveLock)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_ResolveLock{ResolveLock: r}

	case *tikvpb.BatchCommandsRequest_Request_CheckSecondaryLocks:
		r, _ := svc.KvCheckSecondaryLocks(ctx, cmd.CheckSecondaryLocks)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_CheckSecondaryLocks{CheckSecondaryLocks: r}

	case *tikvpb.BatchCommandsRequest_Request_ScanLock:
		r, _ := svc.KvScanLock(ctx, cmd.ScanLock)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_ScanLock{ScanLock: r}

	case *tikvpb.BatchCommandsRequest_Request_RawGet:
		r, _ := svc.RawGet(ctx, cmd.RawGet)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawGet{RawGet: r}

	case *tikvpb.BatchCommandsRequest_Request_RawPut:
		r, _ := svc.RawPut(ctx, cmd.RawPut)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawPut{RawPut: r}

	case *tikvpb.BatchCommandsRequest_Request_RawDelete:
		r, _ := svc.RawDelete(ctx, cmd.RawDelete)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawDelete{RawDelete: r}

	case *tikvpb.BatchCommandsRequest_Request_RawScan:
		r, _ := svc.RawScan(ctx, cmd.RawScan)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawScan{RawScan: r}

	case *tikvpb.BatchCommandsRequest_Request_RawBatchGet:
		r, _ := svc.RawBatchGet(ctx, cmd.RawBatchGet)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawBatchGet{RawBatchGet: r}

	case *tikvpb.BatchCommandsRequest_Request_RawBatchPut:
		r, _ := svc.RawBatchPut(ctx, cmd.RawBatchPut)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawBatchPut{RawBatchPut: r}

	case *tikvpb.BatchCommandsRequest_Request_RawBatchDelete:
		r, _ := svc.RawBatchDelete(ctx, cmd.RawBatchDelete)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawBatchDelete{RawBatchDelete: r}

	case *tikvpb.BatchCommandsRequest_Request_RawDeleteRange:
		r, _ := svc.RawDeleteRange(ctx, cmd.RawDeleteRange)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawDeleteRange{RawDeleteRange: r}

	case *tikvpb.BatchCommandsRequest_Request_RawBatchScan:
		r, _ := svc.RawBatchScan(ctx, cmd.RawBatchScan)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawBatchScan{RawBatchScan: r}

	case *tikvpb.BatchCommandsRequest_Request_Coprocessor:
		r, _ := svc.Coprocessor(ctx, cmd.Coprocessor)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_Coprocessor{Coprocessor: r}

	default:
		// Unsupported command type - return empty response.
	}

	return resp
}

// Raft implements the Tikv_RaftServer streaming endpoint.
// It receives Raft messages from other nodes and dispatches them to local peers.
func (svc *tikvService) Raft(stream tikvpb.Tikv_RaftServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&raft_serverpb.Done{})
		}
		if err != nil {
			return err
		}

		coord := svc.server.coordinator
		if coord == nil {
			continue // No coordinator; drop messages.
		}

		if err := coord.HandleRaftMessage(msg); err != nil {
			// Log but don't fail the stream — message loss is expected.
			_ = err
		}
	}
}

// BatchRaft implements the Tikv_BatchRaftServer streaming endpoint.
// It receives batched Raft messages from other nodes.
func (svc *tikvService) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	for {
		batch, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&raft_serverpb.Done{})
		}
		if err != nil {
			return err
		}

		coord := svc.server.coordinator
		if coord == nil {
			continue
		}

		for _, msg := range batch.GetMsgs() {
			if err := coord.HandleRaftMessage(msg); err != nil {
				_ = err
			}
		}
	}
}

// errToKeyError converts an internal error to a kvrpcpb.KeyError.
func errToKeyError(err error) *kvrpcpb.KeyError {
	if err == nil {
		return nil
	}

	keyErr := &kvrpcpb.KeyError{}

	switch {
	case errors.Is(err, txn.ErrKeyIsLocked):
		keyErr.Locked = &kvrpcpb.LockInfo{}
	case errors.Is(err, txn.ErrWriteConflict):
		keyErr.Conflict = &kvrpcpb.WriteConflict{}
	case errors.Is(err, txn.ErrTxnLockNotFound):
		keyErr.TxnLockNotFound = &kvrpcpb.TxnLockNotFound{}
	case errors.Is(err, txn.ErrAlreadyCommitted):
		keyErr.Abort = err.Error()
	default:
		keyErr.Retryable = err.Error()
	}

	return keyErr
}
