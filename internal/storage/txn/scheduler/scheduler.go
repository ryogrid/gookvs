// Package scheduler implements the TxnScheduler command dispatcher for gookvs.
// It provides a worker pool with latch-based key serialization for transaction
// command execution, decoupling gRPC request handling from MVCC operations.
package scheduler

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/internal/storage/txn/concurrency"
	"github.com/ryogrid/gookvs/internal/storage/txn/latch"
)

// ErrSchedTooBusy is returned when the scheduler cannot accept more commands.
var ErrSchedTooBusy = errors.New("scheduler: too busy")

// CommandKind identifies the type of scheduler command.
type CommandKind int

const (
	CmdPrewrite CommandKind = iota
	CmdCommit
	CmdBatchRollback
	CmdCleanup
	CmdCheckTxnStatus
	CmdPessimisticLock
	CmdPessimisticRollback
	CmdResolveLock
	CmdTxnHeartBeat
)

// CommandContext provides the execution environment for a command.
type CommandContext struct {
	Engine  traits.KvEngine
	ConcMgr *concurrency.Manager
}

// CommandResult holds the output of command execution.
type CommandResult struct {
	Modifies []mvcc.Modify  // nil for read-only commands
	Response interface{}    // command-specific result
}

// Command represents a transaction command to be executed by the scheduler.
type Command interface {
	// Kind returns the command type.
	Kind() CommandKind

	// Keys returns the keys involved, used for latch acquisition.
	Keys() [][]byte

	// Execute runs the command against the given engine/snapshot.
	Execute(ctx CommandContext) (*CommandResult, error)
}

// Callback is invoked when a command completes.
type Callback func(result *CommandResult, err error)

// Config holds TxnScheduler configuration.
type Config struct {
	SchedulerConcurrency  int // latch slot count
	WorkerCount           int // goroutine pool size (default: runtime.NumCPU())
	PendingWriteThreshold int64
	Engine                traits.KvEngine
	ConcurrencyManager    *concurrency.Manager
}

const taskSlotCount = 4096

// taskContext tracks the state of a scheduled command.
type taskContext struct {
	cmd      Command
	cmdID    uint64
	lock     *latch.Lock
	callback Callback
}

type taskSlot struct {
	mu    sync.Mutex
	tasks map[uint64]*taskContext
}

type scheduledTask struct {
	cmdID uint64
	task  *taskContext
}

// TxnScheduler dispatches transaction commands to a worker pool with
// latch-based key serialization.
type TxnScheduler struct {
	latches   *latch.Latches
	taskSlots []taskSlot
	idAlloc   atomic.Uint64

	taskCh   chan scheduledTask
	workerWg sync.WaitGroup

	runningWriteBytes     atomic.Int64
	pendingWriteThreshold int64

	engine  traits.KvEngine
	concMgr *concurrency.Manager

	ctx    context.Context
	cancel context.CancelFunc
}

// New creates a new TxnScheduler.
func New(cfg Config) *TxnScheduler {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = runtime.NumCPU()
	}
	if cfg.SchedulerConcurrency <= 0 {
		cfg.SchedulerConcurrency = 2048
	}
	if cfg.PendingWriteThreshold <= 0 {
		cfg.PendingWriteThreshold = 100 * 1024 * 1024 // 100MB
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &TxnScheduler{
		latches:               latch.New(cfg.SchedulerConcurrency),
		taskSlots:             make([]taskSlot, taskSlotCount),
		taskCh:                make(chan scheduledTask, cfg.WorkerCount*4),
		pendingWriteThreshold: cfg.PendingWriteThreshold,
		engine:                cfg.Engine,
		concMgr:               cfg.ConcurrencyManager,
		ctx:                   ctx,
		cancel:                cancel,
	}

	for i := range s.taskSlots {
		s.taskSlots[i].tasks = make(map[uint64]*taskContext)
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		s.workerWg.Add(1)
		go s.worker()
	}

	return s
}

// RunCommand submits a command for asynchronous execution.
// The callback will be invoked exactly once when the command completes.
func (s *TxnScheduler) RunCommand(cmd Command, cb Callback) {
	select {
	case <-s.ctx.Done():
		cb(nil, context.Canceled)
		return
	default:
	}

	// Allocate command ID.
	cmdID := s.idAlloc.Add(1)

	// Create task context.
	keys := cmd.Keys()
	lock := s.latches.GenLock(keys)

	tc := &taskContext{
		cmd:      cmd,
		cmdID:    cmdID,
		lock:     lock,
		callback: cb,
	}

	// Store in slot.
	slot := &s.taskSlots[cmdID%taskSlotCount]
	slot.mu.Lock()
	slot.tasks[cmdID] = tc
	slot.mu.Unlock()

	// Try to acquire latches.
	s.scheduleCommand(tc, cmdID)
}

// RunCommandSync submits a command and blocks until it completes.
// Returns the result and any error.
func (s *TxnScheduler) RunCommandSync(cmd Command) (*CommandResult, error) {
	type resultPair struct {
		result *CommandResult
		err    error
	}
	ch := make(chan resultPair, 1)
	s.RunCommand(cmd, func(result *CommandResult, err error) {
		ch <- resultPair{result, err}
	})
	r := <-ch
	return r.result, r.err
}

// Stop shuts down the scheduler, waiting for in-flight commands to complete.
func (s *TxnScheduler) Stop() {
	s.cancel()
	close(s.taskCh)
	s.workerWg.Wait()
}

func (s *TxnScheduler) scheduleCommand(tc *taskContext, cmdID uint64) {
	if s.latches.Acquire(tc.lock, cmdID) {
		// Latches acquired, dispatch to worker.
		select {
		case s.taskCh <- scheduledTask{cmdID: cmdID, task: tc}:
		case <-s.ctx.Done():
			s.finishCommand(cmdID, nil, context.Canceled)
		}
	}
	// If not acquired, the command waits in its slot.
	// When latches are released, tryToWakeUp will re-attempt.
}

func (s *TxnScheduler) worker() {
	defer s.workerWg.Done()
	for task := range s.taskCh {
		s.executeTask(task)
	}
}

func (s *TxnScheduler) executeTask(task scheduledTask) {
	tc := task.task
	if tc == nil {
		return
	}

	cmdCtx := CommandContext{
		Engine:  s.engine,
		ConcMgr: s.concMgr,
	}

	result, err := tc.cmd.Execute(cmdCtx)
	s.finishCommand(task.cmdID, result, err)
}

func (s *TxnScheduler) finishCommand(cmdID uint64, result *CommandResult, err error) {
	// Dequeue task context.
	slot := &s.taskSlots[cmdID%taskSlotCount]
	slot.mu.Lock()
	tc, ok := slot.tasks[cmdID]
	if ok {
		delete(slot.tasks, cmdID)
	}
	slot.mu.Unlock()

	if !ok || tc == nil {
		return
	}

	// Invoke callback.
	tc.callback(result, err)

	// Release latches and wake up blocked commands.
	wakeupList := s.latches.Release(tc.lock, cmdID)
	for _, wokenID := range wakeupList {
		s.tryToWakeUp(wokenID)
	}
}

func (s *TxnScheduler) tryToWakeUp(cmdID uint64) {
	slot := &s.taskSlots[cmdID%taskSlotCount]
	slot.mu.Lock()
	tc, ok := slot.tasks[cmdID]
	slot.mu.Unlock()

	if !ok || tc == nil {
		return
	}

	if s.latches.Acquire(tc.lock, cmdID) {
		select {
		case s.taskCh <- scheduledTask{cmdID: cmdID, task: tc}:
		case <-s.ctx.Done():
			s.finishCommand(cmdID, nil, context.Canceled)
		}
	}
}

// WaitIdle waits for all pending tasks to complete (for testing).
func (s *TxnScheduler) WaitIdle(timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return false
		default:
			busy := false
			for i := range s.taskSlots {
				s.taskSlots[i].mu.Lock()
				if len(s.taskSlots[i].tasks) > 0 {
					busy = true
				}
				s.taskSlots[i].mu.Unlock()
				if busy {
					break
				}
			}
			if !busy {
				return true
			}
			time.Sleep(time.Millisecond)
		}
	}
}
