// Package gc implements MVCC garbage collection for gookvs.
// It reclaims stale versions from CF_WRITE and CF_DEFAULT below a safe point.
package gc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// GC state machine states.
type gcState int

const (
	gcStateRewind          gcState = iota // Skip versions above safePoint
	gcStateRemoveIdempotent               // Remove Lock/Rollback, keep first Put/Delete
	gcStateRemoveAll                      // Remove all remaining older versions
)

// GCInfo contains statistics from GC processing of a single key.
type GCInfo struct {
	FoundVersions   int
	DeletedVersions int
	IsCompleted     bool
}

// GCConfig holds tunable parameters for the GC subsystem.
type GCConfig struct {
	// PollSafePointInterval is how often safe point is polled.
	PollSafePointInterval time.Duration
	// MaxWriteBytesPerSec limits GC write throughput (0 = unlimited).
	MaxWriteBytesPerSec int64
	// MaxTxnWriteSize is the max accumulated write size per key before flushing.
	MaxTxnWriteSize int
	// BatchKeys is the number of keys to scan per GC batch.
	BatchKeys int
}

// DefaultGCConfig returns a GCConfig with sensible defaults.
func DefaultGCConfig() *GCConfig {
	return &GCConfig{
		PollSafePointInterval: 10 * time.Second,
		MaxWriteBytesPerSec:   0, // unlimited
		MaxTxnWriteSize:       32 * 1024,
		BatchKeys:             512,
	}
}

// GC performs garbage collection on a single key, removing obsolete MVCC versions.
// It scans CF_WRITE from newest to oldest, applying the state machine.
func GC(
	txn *mvcc.MvccTxn,
	reader *mvcc.MvccReader,
	key mvcc.Key,
	safePoint txntypes.TimeStamp,
) (*GCInfo, error) {
	info := &GCInfo{IsCompleted: true}

	// Start from the newest version and walk backwards.
	state := gcStateRewind
	ts := txntypes.TSMax

	for {
		write, commitTS, err := reader.SeekWrite(key, ts)
		if err != nil {
			return nil, err
		}
		if write == nil {
			break
		}

		info.FoundVersions++

		switch state {
		case gcStateRewind:
			if commitTS > safePoint {
				// Above safe point - skip.
				ts = commitTS - 1
				continue
			}
			// Transition to RemoveIdempotent.
			state = gcStateRemoveIdempotent
			fallthrough

		case gcStateRemoveIdempotent:
			switch write.WriteType {
			case txntypes.WriteTypePut:
				// Keep this version (latest Put at or below safe point).
				state = gcStateRemoveAll
				ts = commitTS - 1
				continue

			case txntypes.WriteTypeDelete:
				// Keep the Delete marker, but transition to remove all older versions.
				state = gcStateRemoveAll
				ts = commitTS - 1
				continue

			case txntypes.WriteTypeLock, txntypes.WriteTypeRollback:
				// Remove Lock/Rollback records below safe point.
				txn.DeleteWrite(key, commitTS)
				info.DeletedVersions++
				ts = commitTS - 1
				continue
			}

		case gcStateRemoveAll:
			// Delete all remaining versions.
			txn.DeleteWrite(key, commitTS)
			info.DeletedVersions++

			// If it's a Put with a large value (no short value), also delete from CF_DEFAULT.
			if write.WriteType == txntypes.WriteTypePut && write.ShortValue == nil {
				txn.DeleteValue(key, write.StartTS)
			}

			ts = commitTS - 1
			continue
		}

		ts = commitTS - 1
	}

	return info, nil
}

// GCTask represents a unit of GC work.
type GCTask struct {
	SafePoint txntypes.TimeStamp
	StartKey  []byte
	EndKey    []byte
	Callback  func(error)
}

// GCWorkerStats holds worker statistics.
type GCWorkerStats struct {
	KeysScanned     int64
	VersionsDeleted int64
}

// GCWorker executes GC tasks.
type GCWorker struct {
	engine traits.KvEngine
	taskCh chan GCTask
	config *GCConfig

	keysScanned     atomic.Int64
	versionsDeleted atomic.Int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewGCWorker creates a new GCWorker.
func NewGCWorker(engine traits.KvEngine, config *GCConfig) *GCWorker {
	if config == nil {
		config = DefaultGCConfig()
	}
	return &GCWorker{
		engine: engine,
		taskCh: make(chan GCTask, 64),
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start begins the worker goroutine.
func (w *GCWorker) Start() {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.run()
	}()
}

// Stop gracefully shuts down the worker.
func (w *GCWorker) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

// Schedule enqueues a GC task. Returns an error if the queue is full.
func (w *GCWorker) Schedule(task GCTask) error {
	select {
	case w.taskCh <- task:
		return nil
	default:
		return ErrGCTaskQueueFull
	}
}

// Stats returns worker statistics.
func (w *GCWorker) Stats() GCWorkerStats {
	return GCWorkerStats{
		KeysScanned:     w.keysScanned.Load(),
		VersionsDeleted: w.versionsDeleted.Load(),
	}
}

func (w *GCWorker) run() {
	for {
		select {
		case <-w.stopCh:
			return
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}
			err := w.processTask(task)
			if task.Callback != nil {
				task.Callback(err)
			}
		}
	}
}

func (w *GCWorker) processTask(task GCTask) error {
	snap := w.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close() // Close reader also closes snapshot.

	// Scan CF_WRITE for keys in the task's range.
	opts := traits.IterOptions{}
	if len(task.StartKey) > 0 {
		opts.LowerBound = mvcc.EncodeKey(task.StartKey, txntypes.TSMax)
	}
	if len(task.EndKey) > 0 {
		opts.UpperBound = mvcc.EncodeKey(task.EndKey, 0)
	}

	iter := snap.NewIterator(cfnames.CFWrite, opts)
	defer iter.Close()

	txn := mvcc.NewMvccTxn(0) // startTS not relevant for GC

	// Track unique user keys to avoid duplicate GC.
	var lastUserKey []byte

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		userKey, _, err := mvcc.DecodeKey(iter.Key())
		if err != nil {
			continue
		}

		// Skip if same user key (we GC all versions on first encounter).
		if string(userKey) == string(lastUserKey) {
			continue
		}
		lastUserKey = append(lastUserKey[:0], userKey...)

		w.keysScanned.Add(1)

		info, err := GC(txn, reader, userKey, task.SafePoint)
		if err != nil {
			continue // Best effort.
		}

		w.versionsDeleted.Add(int64(info.DeletedVersions))

		// Flush when batch gets large.
		if txn.WriteSize >= w.config.MaxTxnWriteSize {
			if err := w.applyModifies(txn); err != nil {
				return err
			}
			txn = mvcc.NewMvccTxn(0)
		}
	}

	// Flush remaining modifies.
	if len(txn.Modifies) > 0 {
		return w.applyModifies(txn)
	}

	return nil
}

func (w *GCWorker) applyModifies(txn *mvcc.MvccTxn) error {
	if len(txn.Modifies) == 0 {
		return nil
	}
	wb := w.engine.NewWriteBatch()
	for _, m := range txn.Modifies {
		switch m.Type {
		case mvcc.ModifyTypePut:
			if err := wb.Put(m.CF, m.Key, m.Value); err != nil {
				return err
			}
		case mvcc.ModifyTypeDelete:
			if err := wb.Delete(m.CF, m.Key); err != nil {
				return err
			}
		}
	}
	return wb.Commit()
}

// SafePointProvider abstracts PD safe point retrieval.
type SafePointProvider interface {
	GetGCSafePoint(ctx context.Context) (txntypes.TimeStamp, error)
}

// MockSafePointProvider returns a configurable safe point for testing.
type MockSafePointProvider struct {
	safePoint atomic.Uint64
}

func (m *MockSafePointProvider) GetGCSafePoint(_ context.Context) (txntypes.TimeStamp, error) {
	return txntypes.TimeStamp(m.safePoint.Load()), nil
}

func (m *MockSafePointProvider) SetSafePoint(ts txntypes.TimeStamp) {
	m.safePoint.Store(uint64(ts))
}

// Errors.
var ErrGCTaskQueueFull = fmt.Errorf("gc: task queue full")
