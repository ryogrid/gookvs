# Failure Report: GCWorker Background Goroutine Never Started

## Summary

The `GCWorker` created in `NewServer()` is never started, causing the `KvGC` gRPC endpoint to hang indefinitely when processing GC requests.

## Details

### Server Initialization (internal/server/server.go:55-80)

```go
func NewServer(cfg ServerConfig, storage *Storage) *Server {
    // ...
    gcWorker := gc.NewGCWorker(storage.Engine(), gc.DefaultGCConfig())

    s := &Server{
        // ...
        gcWorker:   gcWorker,
        // ...
    }
    // ...
    return s
}
```

`NewGCWorker()` creates the worker with an internal task channel, but `gcWorker.Start()` is never called. The `Start()` method launches the background goroutine that processes GC tasks from the channel.

### KvGC Handler (internal/server/server.go:632-657)

```go
func (svc *tikvService) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
    // ...
    done := make(chan error, 1)
    task := gc.GCTask{
        SafePoint: txntypes.TimeStamp(req.GetSafePoint()),
        Callback:  func(err error) { done <- err },
    }
    if err := svc.server.gcWorker.Schedule(task); err != nil {
        // ...
    }

    select {
    case err := <-done:    // <-- HANGS HERE FOREVER
        // ...
    case <-ctx.Done():
        // ...
    }
}
```

The task is successfully enqueued to the GCWorker's channel (the channel capacity is 64), but since no background goroutine is consuming from the channel, the callback is never invoked. The handler blocks on `<-done` forever (unless the client's context times out).

### Evidence

During E2E testing, calling `KvGC` via gRPC caused the test to hang for 2 minutes until the Go test timeout killed it. Stack trace showed the handler blocked at the `select` on line 648.

### Impact

- The `KvGC` gRPC endpoint is effectively non-functional (hangs until client timeout).
- MVCC garbage collection cannot be triggered via the gRPC API.

### Fix

Call `gcWorker.Start()` in `NewServer()` or `Start()`, and `gcWorker.Stop()` in `Stop()`:

```go
// Option A: Start in NewServer() after creation
func NewServer(cfg ServerConfig, storage *Storage) *Server {
    // ...
    gcWorker := gc.NewGCWorker(storage.Engine(), gc.DefaultGCConfig())
    gcWorker.Start()  // ADD THIS
    // ...
}

// In Stop():
func (s *Server) Stop() {
    s.cancel()
    if s.gcWorker != nil {
        s.gcWorker.Stop()  // ADD THIS
    }
    s.grpcServer.GracefulStop()
    s.wg.Wait()
}
```

### Files Affected

- `/home/ryo/work/gookvs/internal/server/server.go` (NewServer and Stop methods)

### Severity

High - The KvGC endpoint is completely non-functional. GC cannot be triggered via gRPC.
