package client

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/ryogrid/gookv/pkg/txntypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestTxnHandle creates a TxnHandle with a nil client, suitable for
// membuf and state-machine tests that do not issue RPCs.
func newTestTxnHandle(startTS txntypes.TimeStamp) *TxnHandle {
	return newTxnHandle(nil, startTS, TxnOptions{})
}

// ---------------------------------------------------------------------------
// 1. Membuf operations
// ---------------------------------------------------------------------------

func TestMembuf_SetAndGet(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(100)

	err := txn.Set(ctx, []byte("k1"), []byte("v1"))
	require.NoError(t, err)

	val, err := txn.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestMembuf_GetMissReturnsNil(t *testing.T) {
	// Get on a key not in the buffer with nil client will panic on RPC.
	// We verify buffer-miss behaviour by checking that a buffered key works
	// and that a deleted key returns nil without RPC.
	ctx := context.Background()
	txn := newTestTxnHandle(100)

	// Set then delete — Get must return nil (no RPC needed).
	err := txn.Set(ctx, []byte("k1"), []byte("v1"))
	require.NoError(t, err)

	err = txn.Delete(ctx, []byte("k1"))
	require.NoError(t, err)

	val, err := txn.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.Nil(t, val, "deleted key should return nil value")
}

func TestMembuf_OverwriteValue(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(100)

	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v1")))
	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v2")))

	val, err := txn.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val, "latest Set should win")
}

func TestMembuf_DeleteThenSet(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(100)

	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v1")))
	require.NoError(t, txn.Delete(ctx, []byte("k1")))

	// Re-set after delete should resurrect the key.
	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v3")))

	val, err := txn.Get(ctx, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v3"), val, "Set after Delete should resurrect the key")
}

func TestMembuf_MultipleKeys(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(100)

	keys := []string{"c", "a", "b"}
	for _, k := range keys {
		require.NoError(t, txn.Set(ctx, []byte(k), []byte("val-"+k)))
	}

	for _, k := range keys {
		val, err := txn.Get(ctx, []byte(k))
		require.NoError(t, err)
		assert.Equal(t, []byte("val-"+k), val)
	}
}

// ---------------------------------------------------------------------------
// 2. Primary key selection
// ---------------------------------------------------------------------------

func TestCommitter_PrimaryKeyIsLexicographicallyFirst(t *testing.T) {
	tests := []struct {
		name     string
		keys     []string
		wantPrim string
	}{
		{
			name:     "single key",
			keys:     []string{"alpha"},
			wantPrim: "alpha",
		},
		{
			name:     "already sorted",
			keys:     []string{"a", "b", "c"},
			wantPrim: "a",
		},
		{
			name:     "reverse sorted",
			keys:     []string{"c", "b", "a"},
			wantPrim: "a",
		},
		{
			name:     "mixed order",
			keys:     []string{"banana", "apple", "cherry"},
			wantPrim: "apple",
		},
		{
			name:     "numeric-like keys",
			keys:     []string{"key3", "key1", "key2"},
			wantPrim: "key1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			txn := newTestTxnHandle(200)

			for _, k := range tt.keys {
				require.NoError(t, txn.Set(ctx, []byte(k), []byte("val")))
			}

			txn.mu.Lock()
			committer := newTwoPhaseCommitter(txn)
			txn.mu.Unlock()

			assert.Equal(t, tt.wantPrim, string(committer.primary),
				"primary should be the lexicographically smallest key")
		})
	}
}

func TestCommitter_MutationsSortedByKey(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(300)

	inputKeys := []string{"delta", "alpha", "charlie", "bravo"}
	for _, k := range inputKeys {
		require.NoError(t, txn.Set(ctx, []byte(k), []byte("v")))
	}

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	sortedKeys := make([]string, len(committer.mutations))
	for i, m := range committer.mutations {
		sortedKeys[i] = string(m.key)
	}
	assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta"}, sortedKeys,
		"mutations should be sorted lexicographically by key")
}

// ---------------------------------------------------------------------------
// 3. State machine guards
// ---------------------------------------------------------------------------

func TestStateMachine_SetAfterCommit(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	// Manually mark committed (empty txn commits immediately).
	txn.mu.Lock()
	txn.committed = true
	txn.mu.Unlock()

	err := txn.Set(ctx, []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_SetAfterRollback(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.rolledBack = true
	txn.mu.Unlock()

	err := txn.Set(ctx, []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrTxnRolledBack)
}

func TestStateMachine_GetAfterCommit(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.committed = true
	txn.mu.Unlock()

	_, err := txn.Get(ctx, []byte("k"))
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_GetAfterRollback(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.rolledBack = true
	txn.mu.Unlock()

	_, err := txn.Get(ctx, []byte("k"))
	assert.ErrorIs(t, err, ErrTxnRolledBack)
}

func TestStateMachine_DeleteAfterCommit(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.committed = true
	txn.mu.Unlock()

	err := txn.Delete(ctx, []byte("k"))
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_DeleteAfterRollback(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.rolledBack = true
	txn.mu.Unlock()

	err := txn.Delete(ctx, []byte("k"))
	assert.ErrorIs(t, err, ErrTxnRolledBack)
}

func TestStateMachine_CommitAfterCommit(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.committed = true
	txn.mu.Unlock()

	err := txn.Commit(ctx)
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_CommitAfterRollback(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.rolledBack = true
	txn.mu.Unlock()

	err := txn.Commit(ctx)
	assert.ErrorIs(t, err, ErrTxnRolledBack)
}

func TestStateMachine_RollbackAfterCommit(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.committed = true
	txn.mu.Unlock()

	err := txn.Rollback(ctx)
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_RollbackAfterRollback(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(400)

	txn.mu.Lock()
	txn.rolledBack = true
	txn.mu.Unlock()

	// Second rollback should be idempotent (return nil).
	err := txn.Rollback(ctx)
	assert.NoError(t, err)
}

func TestStateMachine_CommitEmptyTxn(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(500)

	// Commit with no mutations should succeed and mark committed.
	err := txn.Commit(ctx)
	assert.NoError(t, err)

	// Subsequent Set should fail.
	err = txn.Set(ctx, []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrTxnCommitted)
}

func TestStateMachine_RollbackEmptyTxn(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(500)

	// Rollback with no mutations should succeed.
	err := txn.Rollback(ctx)
	assert.NoError(t, err)

	// Subsequent Set should fail.
	err = txn.Set(ctx, []byte("k"), []byte("v"))
	assert.ErrorIs(t, err, ErrTxnRolledBack)
}

// ---------------------------------------------------------------------------
// 4. Mutation grouping
// ---------------------------------------------------------------------------

func TestCommitter_MutationGrouping(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(600)

	// Buffer various mutation types.
	require.NoError(t, txn.Set(ctx, []byte("put1"), []byte("v1")))
	require.NoError(t, txn.Set(ctx, []byte("put2"), []byte("v2")))
	require.NoError(t, txn.Delete(ctx, []byte("del1")))
	require.NoError(t, txn.Set(ctx, []byte("put3"), []byte("v3")))
	require.NoError(t, txn.Delete(ctx, []byte("del2")))

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	assert.Len(t, committer.mutations, 5, "all mutations should be captured")

	// Build a lookup for verification.
	mutMap := make(map[string]mutationWithKey)
	for _, m := range committer.mutations {
		mutMap[string(m.key)] = m
	}

	// Verify Put mutations.
	for _, k := range []string{"put1", "put2", "put3"} {
		m, ok := mutMap[k]
		require.True(t, ok, "mutation for %q should exist", k)
		assert.Equal(t, kvrpcpb.Op_Put, m.op, "key %q should be Op_Put", k)
	}
	assert.Equal(t, []byte("v1"), mutMap["put1"].value)
	assert.Equal(t, []byte("v2"), mutMap["put2"].value)
	assert.Equal(t, []byte("v3"), mutMap["put3"].value)

	// Verify Delete mutations.
	for _, k := range []string{"del1", "del2"} {
		m, ok := mutMap[k]
		require.True(t, ok, "mutation for %q should exist", k)
		assert.Equal(t, kvrpcpb.Op_Del, m.op, "key %q should be Op_Del", k)
		assert.Nil(t, m.value, "deleted key %q should have nil value", k)
	}
}

func TestCommitter_OverwrittenKeyHasLatestMutation(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(700)

	// Set, then overwrite with a new value.
	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("old")))
	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("new")))

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	require.Len(t, committer.mutations, 1, "overwritten key should appear once")
	assert.Equal(t, kvrpcpb.Op_Put, committer.mutations[0].op)
	assert.Equal(t, []byte("new"), committer.mutations[0].value)
}

func TestCommitter_SetThenDeleteProducesDelete(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(800)

	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v1")))
	require.NoError(t, txn.Delete(ctx, []byte("k1")))

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	require.Len(t, committer.mutations, 1)
	assert.Equal(t, kvrpcpb.Op_Del, committer.mutations[0].op,
		"Set followed by Delete should result in a Del mutation")
	assert.Nil(t, committer.mutations[0].value)
}

func TestCommitter_DeleteThenSetProducesPut(t *testing.T) {
	ctx := context.Background()
	txn := newTestTxnHandle(900)

	require.NoError(t, txn.Delete(ctx, []byte("k1")))
	require.NoError(t, txn.Set(ctx, []byte("k1"), []byte("v1")))

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	require.Len(t, committer.mutations, 1)
	assert.Equal(t, kvrpcpb.Op_Put, committer.mutations[0].op,
		"Delete followed by Set should result in a Put mutation")
	assert.Equal(t, []byte("v1"), committer.mutations[0].value)
}

func TestCommitter_StartTSPropagated(t *testing.T) {
	ctx := context.Background()
	var ts txntypes.TimeStamp = 42
	txn := newTestTxnHandle(ts)

	require.NoError(t, txn.Set(ctx, []byte("k"), []byte("v")))

	txn.mu.Lock()
	committer := newTwoPhaseCommitter(txn)
	txn.mu.Unlock()

	assert.Equal(t, ts, committer.startTS, "committer should inherit the txn startTS")
}

func TestTxnHandle_StartTS(t *testing.T) {
	var ts txntypes.TimeStamp = 12345
	txn := newTestTxnHandle(ts)
	assert.Equal(t, ts, txn.StartTS())
}
