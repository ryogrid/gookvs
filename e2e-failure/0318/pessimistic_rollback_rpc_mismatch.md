# Failure Report: KVPessimisticRollback gRPC Method Name Mismatch

## Summary

The `KVPessimisticRollback` gRPC endpoint is unreachable via the standard TiKV client because the server-side handler method name does not match the proto-generated interface.

## Details

### Proto Definition (tikvpb.proto)

```protobuf
rpc KVPessimisticRollback(kvrpcpb.PessimisticRollbackRequest) returns (kvrpcpb.PessimisticRollbackResponse) {}
```

The proto defines the RPC as `KVPessimisticRollback` (with uppercase `KV`).

### Generated Interface (tikvpb.pb.go)

The generated Go server interface expects:
```go
func (*UnimplementedTikvServer) KVPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error)
```

### Server Implementation (internal/server/server.go:446)

The server defines:
```go
func (svc *tikvService) KvPessimisticRollback(ctx context.Context, req *kvrpcpb.PessimisticRollbackRequest) (*kvrpcpb.PessimisticRollbackResponse, error)
```

Note the lowercase `v` in `Kv` vs the uppercase `V` in `KV`. This means:
- The server method `KvPessimisticRollback` is a regular method on `tikvService` but does NOT override the interface method.
- The `UnimplementedTikvServer.KVPessimisticRollback` method (which returns `Unimplemented`) is what actually handles the gRPC call.
- All calls to `KVPessimisticRollback` from clients receive `Unimplemented` error.

### Evidence

The E2E test `TestTxnPessimisticRollbackRPCMismatch` confirms this:
```
rpc error: code = Unimplemented desc = method KVPessimisticRollback not implemented
```

### Impact

- Clients cannot perform pessimistic lock rollbacks via gRPC.
- The `BatchCommands` routing also has a mismatch: it calls `svc.KvPessimisticRollback()` directly (bypassing gRPC dispatch), so batch commands **do** work for this operation.

### Fix

Rename the server method from `KvPessimisticRollback` to `KVPessimisticRollback` in `internal/server/server.go:446`:

```go
// Before:
func (svc *tikvService) KvPessimisticRollback(...)

// After:
func (svc *tikvService) KVPessimisticRollback(...)
```

Also update the BatchCommands routing call site at `internal/server/server.go:771`:
```go
// Before:
r, _ := svc.KvPessimisticRollback(ctx, cmd.PessimisticRollback)

// After:
r, _ := svc.KVPessimisticRollback(ctx, cmd.PessimisticRollback)
```

### Files Affected

- `/home/ryo/work/gookvs/internal/server/server.go` (lines 446, 771)

### Severity

Medium - Pessimistic lock rollback is unreachable via standard unary gRPC but accessible via BatchCommands.
