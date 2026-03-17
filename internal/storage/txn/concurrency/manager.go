// Package concurrency provides the ConcurrencyManager for tracking in-memory locks
// and the maximum observed timestamp (max_ts) for async commit correctness.
package concurrency

import (
	"sync"
	"sync/atomic"

	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// Manager tracks in-memory locks and max_ts for async commit correctness.
type Manager struct {
	maxTS atomic.Uint64

	// lockTable maps key (string) -> LockHandle.
	lockTable sync.Map
}

// LockHandle represents an in-memory lock on a key.
type LockHandle struct {
	Key     []byte
	StartTS txntypes.TimeStamp
}

// KeyHandleGuard is returned when a key is locked and must be released
// when the lock is no longer needed.
type KeyHandleGuard struct {
	mgr *Manager
	key string
}

// Release removes the lock from the in-memory lock table.
func (g *KeyHandleGuard) Release() {
	if g.mgr != nil {
		g.mgr.lockTable.Delete(g.key)
	}
}

// New creates a new ConcurrencyManager.
func New() *Manager {
	return &Manager{}
}

// UpdateMaxTS atomically updates the maximum observed timestamp.
// Only updates if the new timestamp is greater than the current max.
func (m *Manager) UpdateMaxTS(ts txntypes.TimeStamp) {
	for {
		current := m.maxTS.Load()
		if uint64(ts) <= current {
			return
		}
		if m.maxTS.CompareAndSwap(current, uint64(ts)) {
			return
		}
	}
}

// MaxTS returns the current maximum observed timestamp.
func (m *Manager) MaxTS() txntypes.TimeStamp {
	return txntypes.TimeStamp(m.maxTS.Load())
}

// LockKey registers a lock in the in-memory lock table.
// Returns a guard that must be released when the lock is removed.
func (m *Manager) LockKey(key []byte, startTS txntypes.TimeStamp) *KeyHandleGuard {
	keyStr := string(key)
	m.lockTable.Store(keyStr, &LockHandle{
		Key:     key,
		StartTS: startTS,
	})
	return &KeyHandleGuard{
		mgr: m,
		key: keyStr,
	}
}

// IsKeyLocked checks if a key has an in-memory lock.
func (m *Manager) IsKeyLocked(key []byte) (*LockHandle, bool) {
	v, ok := m.lockTable.Load(string(key))
	if !ok {
		return nil, false
	}
	return v.(*LockHandle), true
}

// GlobalMinLock returns the minimum start_ts across all in-memory locks.
// Returns nil if there are no locks.
func (m *Manager) GlobalMinLock() *txntypes.TimeStamp {
	var minTS *txntypes.TimeStamp

	m.lockTable.Range(func(_, value interface{}) bool {
		handle := value.(*LockHandle)
		if minTS == nil || handle.StartTS < *minTS {
			ts := handle.StartTS
			minTS = &ts
		}
		return true
	})

	return minTS
}

// LockCount returns the number of in-memory locks. O(n) - for testing only.
func (m *Manager) LockCount() int {
	count := 0
	m.lockTable.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}
