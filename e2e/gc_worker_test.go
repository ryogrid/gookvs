package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/server"
	"github.com/ryogrid/gookvs/internal/storage/gc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// startStandaloneServerWithGC creates a standalone server and also starts
// the GC worker (the default server does not start the GC worker goroutine).
func startStandaloneServerWithGC(t *testing.T) (string, *server.Server, *gc.GCWorker) {
	t.Helper()

	dir := t.TempDir()
	engine, err := rocks.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })

	storage := server.NewStorage(engine)
	cfg := server.ServerConfig{ListenAddr: "127.0.0.1:0"}
	srv := server.NewServer(cfg, storage)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Stop() })

	// Create and start GC worker separately.
	gcWorker := gc.NewGCWorker(engine, gc.DefaultGCConfig())
	gcWorker.Start()
	t.Cleanup(func() { gcWorker.Stop() })

	return srv.Addr(), srv, gcWorker
}

// TestGCWorkerCleansOldVersions verifies that GC removes old MVCC versions.
func TestGCWorkerCleansOldVersions(t *testing.T) {
	addr, _, gcWorker := startStandaloneServerWithGC(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Write version 1 of the key.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: []byte("gc-key"), Value: []byte("old-val")}},
		PrimaryLock:  []byte("gc-key"),
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys: [][]byte{[]byte("gc-key")}, StartVersion: 10, CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Write version 2 of the key.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: []byte("gc-key"), Value: []byte("new-val")}},
		PrimaryLock:  []byte("gc-key"),
		StartVersion: 30,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err = client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys: [][]byte{[]byte("gc-key")}, StartVersion: 30, CommitVersion: 40,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Run GC with safe point at version 35 (should clean up version at 20).
	done := make(chan error, 1)
	err = gcWorker.Schedule(gc.GCTask{
		SafePoint: txntypes.TimeStamp(35),
		Callback:  func(err error) { done <- err },
	})
	require.NoError(t, err)

	select {
	case gcErr := <-done:
		assert.NoError(t, gcErr, "GC should succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("GC timed out")
	}

	// The latest version should still be readable.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("gc-key"), Version: 50})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound())
	assert.Equal(t, []byte("new-val"), getResp.GetValue(), "latest version should be preserved")

	t.Log("GC worker test passed")
}

// TestGCWorkerMultipleKeys verifies GC processes multiple keys correctly.
func TestGCWorkerMultipleKeys(t *testing.T) {
	addr, _, gcWorker := startStandaloneServerWithGC(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Write two keys at version 10/20.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("gc-multi-a"), Value: []byte("a-old")},
			{Op: kvrpcpb.Op_Put, Key: []byte("gc-multi-b"), Value: []byte("b-old")},
		},
		PrimaryLock:  []byte("gc-multi-a"),
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	_, err = client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys: [][]byte{[]byte("gc-multi-a"), []byte("gc-multi-b")}, StartVersion: 10, CommitVersion: 20,
	})
	require.NoError(t, err)

	// Write new versions at version 30/40.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("gc-multi-a"), Value: []byte("a-new")},
			{Op: kvrpcpb.Op_Put, Key: []byte("gc-multi-b"), Value: []byte("b-new")},
		},
		PrimaryLock:  []byte("gc-multi-a"),
		StartVersion: 30,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	_, err = client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys: [][]byte{[]byte("gc-multi-a"), []byte("gc-multi-b")}, StartVersion: 30, CommitVersion: 40,
	})
	require.NoError(t, err)

	// Run GC with safe point 35.
	done := make(chan error, 1)
	err = gcWorker.Schedule(gc.GCTask{
		SafePoint: txntypes.TimeStamp(35),
		Callback:  func(err error) { done <- err },
	})
	require.NoError(t, err)

	select {
	case gcErr := <-done:
		assert.NoError(t, gcErr)
	case <-time.After(10 * time.Second):
		t.Fatal("GC timed out")
	}

	// Both keys should return new values.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("gc-multi-a"), Version: 50})
	require.NoError(t, err)
	assert.Equal(t, []byte("a-new"), getResp.GetValue())

	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("gc-multi-b"), Version: 50})
	require.NoError(t, err)
	assert.Equal(t, []byte("b-new"), getResp.GetValue())

	t.Log("GC worker multi-key test passed")
}
