package raftstore

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/keys"
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

	// Snapshot state machine.
	snapState    SnapState
	snapReceiver <-chan GenSnapResult
	snapCanceled *atomic.Bool
	snapTriedCnt int

	// Snapshot worker wiring (set by coordinator).
	snapTaskCh chan<- GenSnapTask
	region     *metapb.Region
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
// ConfState is derived from the region's peer list so that Raft knows its
// cluster membership after restart.
func (s *PeerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var cs raftpb.ConfState
	if s.region != nil {
		for _, peer := range s.region.GetPeers() {
			cs.Voters = append(cs.Voters, peer.GetId())
		}
	}
	return s.hardState, cs, nil
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
// If a SnapWorker is wired, triggers async generation via RequestSnapshot.
// Otherwise returns metadata-only snapshot (standalone fallback).
func (s *PeerStorage) Snapshot() (raftpb.Snapshot, error) {
	s.mu.RLock()
	taskCh := s.snapTaskCh
	region := s.region
	s.mu.RUnlock()

	if taskCh == nil {
		// No snap worker wired — return metadata-only (standalone fallback).
		s.mu.RLock()
		defer s.mu.RUnlock()
		return raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: s.applyState.TruncatedIndex,
				Term:  s.applyState.TruncatedTerm,
			},
		}, nil
	}

	return s.RequestSnapshot(taskCh, region)
}

// SetSnapTaskCh sets the snapshot task channel for async generation.
func (s *PeerStorage) SetSnapTaskCh(ch chan<- GenSnapTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapTaskCh = ch
}

// SetRegion sets the region metadata on the storage.
func (s *PeerStorage) SetRegion(region *metapb.Region) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.region = region
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

// HasPersistedRaftState checks whether the engine has persisted Raft state
// for the given region. Returns true if a hard state key exists.
func HasPersistedRaftState(engine traits.KvEngine, regionID uint64) bool {
	_, err := engine.Get(cfnames.CFRaft, keys.RaftStateKey(regionID))
	return err == nil
}

// RecoverFromEngine restores PeerStorage state from the engine.
// This should be called when restarting a peer that already has persisted Raft state.
func (s *PeerStorage) RecoverFromEngine() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recover hard state.
	data, err := s.engine.Get(cfnames.CFRaft, keys.RaftStateKey(s.regionID))
	if err == nil {
		var hs raftpb.HardState
		if err := hs.Unmarshal(data); err != nil {
			return fmt.Errorf("raftstore: unmarshal hard state: %w", err)
		}
		s.hardState = hs
	} else if err != traits.ErrNotFound {
		return fmt.Errorf("raftstore: read hard state: %w", err)
	}

	// Scan Raft log entries to find the last persisted index and rebuild cache.
	startKey, endKey := keys.RaftLogKeyRange(s.regionID)
	iter := s.engine.NewIterator(cfnames.CFRaft, traits.IterOptions{
		LowerBound: startKey,
		UpperBound: endKey,
	})
	defer iter.Close()

	var lastIdx uint64
	var entries []raftpb.Entry
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		var entry raftpb.Entry
		if err := entry.Unmarshal(iter.Value()); err != nil {
			return fmt.Errorf("raftstore: unmarshal entry during recovery: %w", err)
		}
		entries = append(entries, entry)
		if entry.Index > lastIdx {
			lastIdx = entry.Index
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("raftstore: iterate entries during recovery: %w", err)
	}

	if lastIdx > 0 {
		s.persistedLastIndex = lastIdx
		// Keep only the most recent entries in cache.
		const maxCacheSize = 1024
		if len(entries) > maxCacheSize {
			entries = entries[len(entries)-maxCacheSize:]
		}
		s.entries = entries
	}

	// Recover ApplyState.
	applyData, err := s.engine.Get(cfnames.CFRaft, keys.ApplyStateKey(s.regionID))
	if err == nil && len(applyData) == 24 {
		s.applyState.AppliedIndex = uint64(applyData[0])<<56 | uint64(applyData[1])<<48 |
			uint64(applyData[2])<<40 | uint64(applyData[3])<<32 |
			uint64(applyData[4])<<24 | uint64(applyData[5])<<16 |
			uint64(applyData[6])<<8 | uint64(applyData[7])
		s.applyState.TruncatedIndex = uint64(applyData[8])<<56 | uint64(applyData[9])<<48 |
			uint64(applyData[10])<<40 | uint64(applyData[11])<<32 |
			uint64(applyData[12])<<24 | uint64(applyData[13])<<16 |
			uint64(applyData[14])<<8 | uint64(applyData[15])
		s.applyState.TruncatedTerm = uint64(applyData[16])<<56 | uint64(applyData[17])<<48 |
			uint64(applyData[18])<<40 | uint64(applyData[19])<<32 |
			uint64(applyData[20])<<24 | uint64(applyData[21])<<16 |
			uint64(applyData[22])<<8 | uint64(applyData[23])
	} else if err != nil && err != traits.ErrNotFound {
		return fmt.Errorf("raftstore: read apply state: %w", err)
	}

	return nil
}

// TruncatedIndex returns the current truncated index.
func (s *PeerStorage) TruncatedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applyState.TruncatedIndex
}

// TruncatedTerm returns the current truncated term.
func (s *PeerStorage) TruncatedTerm() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applyState.TruncatedTerm
}

// AppliedIndex returns the current applied index.
func (s *PeerStorage) AppliedIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applyState.AppliedIndex
}

// SetAppliedIndex updates the applied index after entries have been applied.
func (s *PeerStorage) SetAppliedIndex(index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.applyState.AppliedIndex = index
}

// CompactTo removes entries from the in-memory cache up to compactTo.
// Entries before compactTo will no longer be served from cache.
func (s *PeerStorage) CompactTo(compactTo uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.entries) == 0 {
		return
	}

	cacheFirst := s.entries[0].Index
	if compactTo <= cacheFirst {
		return
	}

	offset := compactTo - cacheFirst
	if offset >= uint64(len(s.entries)) {
		s.entries = nil
		return
	}
	s.entries = s.entries[offset:]
}

// PersistApplyState writes the current ApplyState to the engine.
func (s *PeerStorage) PersistApplyState() error {
	s.mu.RLock()
	state := s.applyState
	s.mu.RUnlock()

	// Serialize ApplyState as a simple binary format:
	// [AppliedIndex:8][TruncatedIndex:8][TruncatedTerm:8]
	data := make([]byte, 24)
	data[0] = byte(state.AppliedIndex >> 56)
	data[1] = byte(state.AppliedIndex >> 48)
	data[2] = byte(state.AppliedIndex >> 40)
	data[3] = byte(state.AppliedIndex >> 32)
	data[4] = byte(state.AppliedIndex >> 24)
	data[5] = byte(state.AppliedIndex >> 16)
	data[6] = byte(state.AppliedIndex >> 8)
	data[7] = byte(state.AppliedIndex)
	data[8] = byte(state.TruncatedIndex >> 56)
	data[9] = byte(state.TruncatedIndex >> 48)
	data[10] = byte(state.TruncatedIndex >> 40)
	data[11] = byte(state.TruncatedIndex >> 32)
	data[12] = byte(state.TruncatedIndex >> 24)
	data[13] = byte(state.TruncatedIndex >> 16)
	data[14] = byte(state.TruncatedIndex >> 8)
	data[15] = byte(state.TruncatedIndex)
	data[16] = byte(state.TruncatedTerm >> 56)
	data[17] = byte(state.TruncatedTerm >> 48)
	data[18] = byte(state.TruncatedTerm >> 40)
	data[19] = byte(state.TruncatedTerm >> 32)
	data[20] = byte(state.TruncatedTerm >> 24)
	data[21] = byte(state.TruncatedTerm >> 16)
	data[22] = byte(state.TruncatedTerm >> 8)
	data[23] = byte(state.TruncatedTerm)

	return s.engine.Put(cfnames.CFRaft, keys.ApplyStateKey(s.regionID), data)
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
