package raftstore

import (
	"fmt"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/keys"
)

// ApplyState tracks which Raft entries have been applied to the state machine.
type ApplyState struct {
	AppliedIndex   uint64
	TruncatedIndex uint64
	TruncatedTerm  uint64
}

// PeerStorage implements raft.Storage for a single region.
// It provides persistent Raft log access backed by a KvEngine.
type PeerStorage struct {
	mu sync.RWMutex

	regionID uint64
	engine   traits.KvEngine

	// In-memory cache of Raft state.
	hardState         raftpb.HardState
	applyState        ApplyState
	entries           []raftpb.Entry // In-memory entry cache (most recent entries).
	persistedLastIndex uint64         // Last index written to engine (tracked independently of cache).
}

// Ensure PeerStorage implements raft.Storage.
var _ raft.Storage = (*PeerStorage)(nil)

// NewPeerStorage creates a PeerStorage for the given region.
// initialState should be set for new (bootstrapped) regions.
func NewPeerStorage(regionID uint64, engine traits.KvEngine) *PeerStorage {
	return &PeerStorage{
		regionID: regionID,
		engine:   engine,
		applyState: ApplyState{
			AppliedIndex:   RaftInitLogIndex,
			TruncatedIndex: RaftInitLogIndex,
			TruncatedTerm:  RaftInitLogTerm,
		},
		persistedLastIndex: RaftInitLogIndex,
	}
}

// InitialState returns the initial HardState and ConfState from storage.
func (s *PeerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hardState, raftpb.ConfState{}, nil
}

// Entries returns a slice of Raft log entries in [lo, hi), capped at maxSize bytes.
func (s *PeerStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo > hi {
		return nil, fmt.Errorf("raftstore: invalid range [%d, %d)", lo, hi)
	}

	first := s.firstIndexLocked()
	if lo < first {
		return nil, raft.ErrCompacted
	}

	last := s.lastIndexLocked()
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}

	// Try to serve from in-memory cache.
	if len(s.entries) > 0 {
		cacheFirst := s.entries[0].Index
		cacheLast := s.entries[len(s.entries)-1].Index

		if lo >= cacheFirst && hi <= cacheLast+1 {
			start := lo - cacheFirst
			end := hi - cacheFirst
			entries := s.entries[start:end]
			return limitSize(entries, maxSize), nil
		}
	}

	// Fall back to reading from engine.
	entries, err := s.readEntriesFromEngine(lo, hi)
	if err != nil {
		return nil, err
	}
	return limitSize(entries, maxSize), nil
}

// Term returns the term of the entry at the given index.
func (s *PeerStorage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i == s.applyState.TruncatedIndex {
		return s.applyState.TruncatedTerm, nil
	}

	first := s.firstIndexLocked()
	if i < first {
		return 0, raft.ErrCompacted
	}

	last := s.lastIndexLocked()
	if i > last {
		return 0, raft.ErrUnavailable
	}

	// Try cache.
	if len(s.entries) > 0 {
		cacheFirst := s.entries[0].Index
		cacheLast := s.entries[len(s.entries)-1].Index
		if i >= cacheFirst && i <= cacheLast {
			return s.entries[i-cacheFirst].Term, nil
		}
	}

	// Fall back to engine.
	entries, err := s.readEntriesFromEngine(i, i+1)
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		return 0, raft.ErrUnavailable
	}
	return entries[0].Term, nil
}

// LastIndex returns the index of the last log entry.
func (s *PeerStorage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked(), nil
}

// FirstIndex returns the index of the first available log entry.
func (s *PeerStorage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked(), nil
}

// Snapshot returns the most recent snapshot.
// In the initial implementation, we return an empty snapshot.
func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: s.applyState.TruncatedIndex,
			Term:  s.applyState.TruncatedTerm,
		},
	}, nil
}

// SaveReady persists the Raft state changes from a Ready batch.
func (s *PeerStorage) SaveReady(rd raft.Ready) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	wb := s.engine.NewWriteBatch()

	// Persist hard state.
	if !raft.IsEmptyHardState(rd.HardState) {
		s.hardState = rd.HardState
		data, err := s.hardState.Marshal()
		if err != nil {
			return fmt.Errorf("raftstore: marshal hard state: %w", err)
		}
		if err := wb.Put(cfnames.CFRaft, keys.RaftStateKey(s.regionID), data); err != nil {
			return err
		}
	}

	// Persist new entries.
	for i := range rd.Entries {
		data, err := rd.Entries[i].Marshal()
		if err != nil {
			return fmt.Errorf("raftstore: marshal entry %d: %w", rd.Entries[i].Index, err)
		}
		if err := wb.Put(cfnames.CFRaft, keys.RaftLogKey(s.regionID, rd.Entries[i].Index), data); err != nil {
			return err
		}
	}

	if err := wb.Commit(); err != nil {
		return fmt.Errorf("raftstore: commit ready: %w", err)
	}

	// Update in-memory cache and persisted index tracker.
	if len(rd.Entries) > 0 {
		s.appendToCache(rd.Entries)
		lastEntry := rd.Entries[len(rd.Entries)-1]
		if lastEntry.Index > s.persistedLastIndex {
			s.persistedLastIndex = lastEntry.Index
		}
	}

	return nil
}

// SetApplyState updates the apply state.
func (s *PeerStorage) SetApplyState(state ApplyState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyState = state
}

// GetApplyState returns the current apply state.
func (s *PeerStorage) GetApplyState() ApplyState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applyState
}

// SetHardState sets the hard state directly (for initialization).
func (s *PeerStorage) SetHardState(hs raftpb.HardState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hardState = hs
}

// SetPersistedLastIndex sets the persisted last index (for initialization).
func (s *PeerStorage) SetPersistedLastIndex(idx uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistedLastIndex = idx
}

// SetDummyEntry adds a dummy entry at index 0 with term 0,
// matching etcd/raft's MemoryStorage convention for empty storage.
func (s *PeerStorage) SetDummyEntry() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = []raftpb.Entry{{Index: 0, Term: 0}}
}

// --- Internal helpers ---

func (s *PeerStorage) firstIndexLocked() uint64 {
	return s.applyState.TruncatedIndex + 1
}

func (s *PeerStorage) lastIndexLocked() uint64 {
	if len(s.entries) > 0 {
		return s.entries[len(s.entries)-1].Index
	}
	return s.persistedLastIndex
}

func (s *PeerStorage) appendToCache(entries []raftpb.Entry) {
	if len(s.entries) == 0 {
		s.entries = append(s.entries, entries...)
		return
	}

	// Truncate cache if new entries overlap.
	first := entries[0].Index
	cacheFirst := s.entries[0].Index
	if first <= cacheFirst {
		s.entries = append([]raftpb.Entry{}, entries...)
	} else {
		// Keep cache entries before the new ones.
		keepTo := first - cacheFirst
		if keepTo > uint64(len(s.entries)) {
			keepTo = uint64(len(s.entries))
		}
		s.entries = append(s.entries[:keepTo], entries...)
	}

	// Limit cache size (keep last 1024 entries).
	const maxCacheSize = 1024
	if len(s.entries) > maxCacheSize {
		s.entries = s.entries[len(s.entries)-maxCacheSize:]
	}
}

func (s *PeerStorage) readEntriesFromEngine(lo, hi uint64) ([]raftpb.Entry, error) {
	var entries []raftpb.Entry
	for idx := lo; idx < hi; idx++ {
		data, err := s.engine.Get(cfnames.CFRaft, keys.RaftLogKey(s.regionID, idx))
		if err != nil {
			if err == traits.ErrNotFound {
				break
			}
			return nil, fmt.Errorf("raftstore: read entry %d: %w", idx, err)
		}
		var entry raftpb.Entry
		if err := entry.Unmarshal(data); err != nil {
			return nil, fmt.Errorf("raftstore: unmarshal entry %d: %w", idx, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// limitSize returns a prefix of entries whose total size does not exceed maxSize.
// If maxSize is 0, all entries are returned.
func limitSize(entries []raftpb.Entry, maxSize uint64) []raftpb.Entry {
	if maxSize == 0 || len(entries) == 0 {
		return entries
	}
	var size uint64
	for i, e := range entries {
		size += uint64(e.Size())
		if size > maxSize && i > 0 {
			return entries[:i]
		}
	}
	return entries
}
