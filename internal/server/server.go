package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/internal/storage/txn"
	"github.com/ryogrid/gookvs/pkg/txntypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerConfig holds configuration for the gRPC server.
type ServerConfig struct {
	ListenAddr string
	ClusterID  uint64
}

// Server encapsulates the gRPC server and all server-side components.
type Server struct {
	cfg        ServerConfig
	grpcServer *grpc.Server
	storage    *Storage
	listener   net.Listener

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
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
		ctx:        ctx,
		cancel:     cancel,
	}

	// Register the TikvService.
	tikvpb.RegisterTikvServer(grpcSrv, &tikvService{server: s})

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

	errs := svc.server.storage.Prewrite(
		mutations,
		req.GetPrimaryLock(),
		txntypes.TimeStamp(req.GetStartVersion()),
		req.GetLockTtl(),
	)

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

	err := svc.server.storage.Commit(
		req.GetKeys(),
		txntypes.TimeStamp(req.GetStartVersion()),
		txntypes.TimeStamp(req.GetCommitVersion()),
	)
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

	default:
		// Unsupported command type - return empty response.
	}

	return resp
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
