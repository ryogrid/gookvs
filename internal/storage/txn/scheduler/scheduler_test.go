package scheduler

import (
	"bytes"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/internal/storage/txn/concurrency"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T) traits.KvEngine {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(filepath.Join(dir, "sched-test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	return eng
}

func newTestScheduler(t *testing.T, eng traits.KvEngine) *TxnScheduler {
	t.Helper()
	cfg := Config{
		SchedulerConcurrency: 256,
		WorkerCount:          4,
		Engine:               eng,
		ConcurrencyManager:   concurrency.New(),
	}
	s := New(cfg)
	t.Cleanup(func() { s.Stop() })
	return s
}

// testPutCommand is a simple test command that puts a key-value pair.
type testPutCommand struct {
	key   []byte
	value []byte
	cf    string
}

func (c *testPutCommand) Kind() CommandKind  { return CmdPrewrite }
func (c *testPutCommand) Keys() [][]byte     { return [][]byte{c.key} }
func (c *testPutCommand) Execute(ctx CommandContext) (*CommandResult, error) {
	cf := c.cf
	if cf == "" {
		cf = cfnames.CFDefault
	}
	// Simulate an MVCC write by creating a modify.
	mods := []mvcc.Modify{
		{Type: mvcc.ModifyTypePut, CF: cf, Key: c.key, Value: c.value},
	}
	// Apply directly for standalone mode test.
	wb := ctx.Engine.NewWriteBatch()
	for _, m := range mods {
		if err := wb.Put(m.CF, m.Key, m.Value); err != nil {
			return nil, err
		}
	}
	if err := wb.Commit(); err != nil {
		return nil, err
	}
	return &CommandResult{Modifies: mods, Response: "ok"}, nil
}

// testSlowCommand sleeps for a specified duration.
type testSlowCommand struct {
	key      []byte
	duration time.Duration
}

func (c *testSlowCommand) Kind() CommandKind  { return CmdPrewrite }
func (c *testSlowCommand) Keys() [][]byte     { return [][]byte{c.key} }
func (c *testSlowCommand) Execute(ctx CommandContext) (*CommandResult, error) {
	time.Sleep(c.duration)
	return &CommandResult{Response: "slow-done"}, nil
}

// testReadCommand reads a key without modifying.
type testReadCommand struct {
	key []byte
}

func (c *testReadCommand) Kind() CommandKind  { return CmdCheckTxnStatus }
func (c *testReadCommand) Keys() [][]byte     { return [][]byte{c.key} }
func (c *testReadCommand) Execute(ctx CommandContext) (*CommandResult, error) {
	val, err := ctx.Engine.Get(cfnames.CFDefault, c.key)
	if err != nil {
		if err == traits.ErrNotFound {
			return &CommandResult{Response: nil}, nil
		}
		return nil, err
	}
	return &CommandResult{Response: val}, nil
}

func TestRunCommandBasic(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	cmd := &testPutCommand{key: []byte("k1"), value: []byte("v1")}
	result, err := sched.RunCommandSync(cmd)
	require.NoError(t, err)
	assert.Equal(t, "ok", result.Response)

	// Verify data was written.
	val, err := eng.Get(cfnames.CFDefault, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestConcurrentNonOverlappingKeys(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	const n = 20
	var wg sync.WaitGroup
	errors := make([]error, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := []byte{byte(idx)}
			cmd := &testPutCommand{key: key, value: []byte{byte(idx + 100)}}
			_, errors[idx] = sched.RunCommandSync(cmd)
		}(i)
	}
	wg.Wait()

	for i, err := range errors {
		assert.NoError(t, err, "command %d failed", i)
	}

	// Verify all keys written.
	for i := 0; i < n; i++ {
		val, err := eng.Get(cfnames.CFDefault, []byte{byte(i)})
		require.NoError(t, err)
		assert.Equal(t, []byte{byte(i + 100)}, val)
	}
}

func TestLatchSerialization(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	// Two commands with the same key should execute serially.
	var order []int
	var mu sync.Mutex

	var wg sync.WaitGroup
	wg.Add(2)

	// Command 1: slow
	sched.RunCommand(&testSlowCommand{key: []byte("shared"), duration: 50 * time.Millisecond}, func(result *CommandResult, err error) {
		mu.Lock()
		order = append(order, 1)
		mu.Unlock()
		wg.Done()
	})

	// Small delay to ensure command 1 gets latches first.
	time.Sleep(5 * time.Millisecond)

	// Command 2: also on same key
	sched.RunCommand(&testSlowCommand{key: []byte("shared"), duration: 10 * time.Millisecond}, func(result *CommandResult, err error) {
		mu.Lock()
		order = append(order, 2)
		mu.Unlock()
		wg.Done()
	})

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{1, 2}, order, "commands should execute in order due to latch serialization")
}

func TestLatchWakeup(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	// First command writes, second command reads after first finishes.
	done := make(chan struct{})
	var readResult interface{}

	sched.RunCommand(&testPutCommand{key: []byte("wk"), value: []byte("wv")}, func(result *CommandResult, err error) {
		require.NoError(t, err)
	})

	// Wait for first command.
	sched.WaitIdle(time.Second)

	sched.RunCommand(&testReadCommand{key: []byte("wk")}, func(result *CommandResult, err error) {
		require.NoError(t, err)
		readResult = result.Response
		close(done)
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for read command")
	}

	assert.Equal(t, []byte("wv"), readResult)
}

func TestGracefulStop(t *testing.T) {
	eng := newTestEngine(t)
	cfg := Config{
		SchedulerConcurrency: 256,
		WorkerCount:          2,
		Engine:               eng,
		ConcurrencyManager:   concurrency.New(),
	}
	sched := New(cfg)

	// Submit some commands.
	var completed atomic.Int32
	for i := 0; i < 5; i++ {
		cmd := &testPutCommand{key: []byte{byte(i)}, value: []byte{byte(i)}}
		sched.RunCommand(cmd, func(result *CommandResult, err error) {
			completed.Add(1)
		})
	}

	// Wait briefly then stop.
	time.Sleep(50 * time.Millisecond)
	sched.Stop()

	// All submitted commands should have completed.
	assert.Equal(t, int32(5), completed.Load())
}

func TestCallbackInvokedOnce(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	var callCount atomic.Int32
	cmd := &testPutCommand{key: []byte("cb"), value: []byte("cv")}
	sched.RunCommand(cmd, func(result *CommandResult, err error) {
		callCount.Add(1)
	})

	sched.WaitIdle(time.Second)
	assert.Equal(t, int32(1), callCount.Load())
}

func TestCommandKinds(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	// Test that different command kinds work.
	tests := []struct {
		name string
		cmd  Command
	}{
		{"put", &testPutCommand{key: []byte("a"), value: []byte("1")}},
		{"read", &testReadCommand{key: []byte("nonexist")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := sched.RunCommandSync(tt.cmd)
			require.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}

// TestPrewriteCommand is a more realistic test with MVCC-style operations.
type testPrewriteCommand struct {
	keys    [][]byte
	primary []byte
	startTS txntypes.TimeStamp
}

func (c *testPrewriteCommand) Kind() CommandKind  { return CmdPrewrite }
func (c *testPrewriteCommand) Keys() [][]byte     { return c.keys }
func (c *testPrewriteCommand) Execute(ctx CommandContext) (*CommandResult, error) {
	snap := ctx.Engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close() // closes the snapshot too

	txn := mvcc.NewMvccTxn(c.startTS)
	for _, key := range c.keys {
		// Simplified prewrite: just write a lock.
		lock := &txntypes.Lock{
			LockType: txntypes.LockTypePut,
			Primary:  c.primary,
			StartTS:  c.startTS,
			TTL:      3000,
		}
		txn.PutLock(key, lock)
	}

	// Apply modifies.
	wb := ctx.Engine.NewWriteBatch()
	for _, m := range txn.Modifies {
		switch m.Type {
		case mvcc.ModifyTypePut:
			if err := wb.Put(m.CF, m.Key, m.Value); err != nil {
				return nil, err
			}
		case mvcc.ModifyTypeDelete:
			if err := wb.Delete(m.CF, m.Key); err != nil {
				return nil, err
			}
		}
	}
	if err := wb.Commit(); err != nil {
		return nil, err
	}

	return &CommandResult{Modifies: txn.Modifies, Response: "prewritten"}, nil
}

func TestSchedulerPrewriteCommand(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	cmd := &testPrewriteCommand{
		keys:    [][]byte{[]byte("pk"), []byte("sk")},
		primary: []byte("pk"),
		startTS: 10,
	}

	result, err := sched.RunCommandSync(cmd)
	require.NoError(t, err)
	assert.Equal(t, "prewritten", result.Response)
	assert.Len(t, result.Modifies, 2)

	// Verify locks were written to CF_LOCK.
	for _, key := range cmd.keys {
		lockKey := mvcc.EncodeLockKey(key)
		val, err := eng.Get(cfnames.CFLock, lockKey)
		require.NoError(t, err)
		lock, err := txntypes.UnmarshalLock(val)
		require.NoError(t, err)
		assert.Equal(t, txntypes.LockTypePut, lock.LockType)
		assert.True(t, bytes.Equal(cmd.primary, lock.Primary))
	}
}

func TestHighConcurrency(t *testing.T) {
	eng := newTestEngine(t)
	sched := newTestScheduler(t, eng)

	const numCommands = 100
	var wg sync.WaitGroup
	errCh := make(chan error, numCommands)

	for i := 0; i < numCommands; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Use modular keys to create some contention.
			keyIdx := byte(idx % 10)
			cmd := &testPutCommand{
				key:   []byte{keyIdx},
				value: []byte{byte(idx)},
			}
			_, err := sched.RunCommandSync(cmd)
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("command failed: %v", err)
	}

	// Verify all 10 keys exist.
	for i := 0; i < 10; i++ {
		_, err := eng.Get(cfnames.CFDefault, []byte{byte(i)})
		require.NoError(t, err)
	}
}
