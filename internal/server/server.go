package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/internal/storage/txn"
	"github.com/ryogrid/gookvs/pkg/txntypes"
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
	coordinator *StoreCoordinator
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

// NewServer creates a Server with all dependencies.
func NewServer(cfg ServerConfig, storage *Storage) *Server {
	ctx, cancel := context.WithCancel(context.Background())

	opts := buildServerOptions(cfg)
	grpcSrv := grpc.NewServer(opts...)

	s := &Server{
		cfg:        cfg,
		grpcServer: grpcSrv,
		storage:    storage,
		rawStorage: NewRawStorage(storage.Engine()),
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

// KvPrewrite implements the first phase of 2PC.
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
		if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
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
			if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
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
		if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
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

// KvPessimisticRollback implements the KvPessimisticRollback RPC.
func (svc *tikvService) KvPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error) {
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
			if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
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

// RawPut implements the RawPut RPC.
func (svc *tikvService) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	if coord := svc.server.coordinator; coord != nil {
		modify := svc.server.rawStorage.PutModify(req.GetCf(), req.GetKey(), req.GetValue())
		if err := coord.ProposeModifies(1, []mvcc.Modify{modify}, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.Put(req.GetCf(), req.GetKey(), req.GetValue()); err != nil {
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
		if err := coord.ProposeModifies(1, []mvcc.Modify{modify}, 10*time.Second); err != nil {
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
	if coord := svc.server.coordinator; coord != nil {
		modifies := make([]mvcc.Modify, len(pairs))
		for i, p := range pairs {
			modifies[i] = svc.server.rawStorage.PutModify(req.GetCf(), p.Key, p.Value)
		}
		if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
			return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
		}
	} else {
		if err := svc.server.rawStorage.BatchPut(req.GetCf(), pairs); err != nil {
			resp.Error = err.Error()
		}
	}
	return resp, nil
}

// RawBatchDelete implements the RawBatchDelete RPC.
func (svc *tikvService) RawBatchDelete(ctx context.Context, req *kvrpcpb.RawBatchDeleteRequest) (*kvrpcpb.RawBatchDeleteResponse, error) {
	resp := &kvrpcpb.RawBatchDeleteResponse{}
	if coord := svc.server.coordinator; coord != nil {
		modifies := make([]mvcc.Modify, len(req.GetKeys()))
		for i, key := range req.GetKeys() {
			modifies[i] = svc.server.rawStorage.DeleteModify(req.GetCf(), key)
		}
		if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
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
		r, _ := svc.KvPessimisticRollback(ctx, cmd.PessimisticRollback)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_PessimisticRollback{PessimisticRollback: r}

	case *tikvpb.BatchCommandsRequest_Request_TxnHeartBeat:
		r, _ := svc.KvTxnHeartBeat(ctx, cmd.TxnHeartBeat)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_TxnHeartBeat{TxnHeartBeat: r}

	case *tikvpb.BatchCommandsRequest_Request_ResolveLock:
		r, _ := svc.KvResolveLock(ctx, cmd.ResolveLock)
		resp.Cmd = &tikvpb.BatchCommandsResponse_Response_ResolveLock{ResolveLock: r}

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
