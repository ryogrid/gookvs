package raftstore

import (
	"path/filepath"
	"testing"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T) traits.KvEngine {
	t.Helper()
	dir := t.TempDir()
	e, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	return e
}

func TestPeerStorageInitialState(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	hs, cs, err := s.InitialState()
	require.NoError(t, err)
	assert.Equal(t, raftpb.HardState{}, hs)
	assert.Equal(t, raftpb.ConfState{}, cs)
}

func TestPeerStorageFirstLastIndex(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	// Initial state: first = truncated + 1, last = truncated.
	first, err := s.FirstIndex()
	require.NoError(t, err)
	assert.Equal(t, RaftInitLogIndex+1, first)

	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, RaftInitLogIndex, last)
}

func TestPeerStorageTermAtTruncatedIndex(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	term, err := s.Term(RaftInitLogIndex)
	require.NoError(t, err)
	assert.Equal(t, RaftInitLogTerm, term)
}

func TestPeerStorageTermBeforeTruncated(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	_, err := s.Term(RaftInitLogIndex - 1)
	assert.Equal(t, raft.ErrCompacted, err)
}

func TestPeerStorageTermAfterLast(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	_, err := s.Term(RaftInitLogIndex + 100)
	assert.Equal(t, raft.ErrUnavailable, err)
}

func TestPeerStorageSaveReadyAndEntries(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	// Create a Ready with some entries.
	entries := []raftpb.Entry{
		{Index: RaftInitLogIndex + 1, Term: 1, Data: []byte("entry-1")},
		{Index: RaftInitLogIndex + 2, Term: 1, Data: []byte("entry-2")},
		{Index: RaftInitLogIndex + 3, Term: 1, Data: []byte("entry-3")},
	}
	hs := raftpb.HardState{Term: 1, Vote: 1, Commit: RaftInitLogIndex + 3}
	rd := raft.Ready{
		HardState: hs,
		Entries:   entries,
	}

	require.NoError(t, s.SaveReady(rd))

	// Verify hard state.
	gotHS, _, err := s.InitialState()
	require.NoError(t, err)
	assert.Equal(t, hs, gotHS)

	// Verify entries.
	gotEntries, err := s.Entries(RaftInitLogIndex+1, RaftInitLogIndex+4, 0)
	require.NoError(t, err)
	assert.Len(t, gotEntries, 3)
	assert.Equal(t, []byte("entry-1"), gotEntries[0].Data)
	assert.Equal(t, []byte("entry-3"), gotEntries[2].Data)

	// Verify last index.
	last, err := s.LastIndex()
	require.NoError(t, err)
	assert.Equal(t, RaftInitLogIndex+3, last)
}

func TestPeerStorageEntriesFromEngine(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	entries := []raftpb.Entry{
		{Index: RaftInitLogIndex + 1, Term: 1, Data: []byte("a")},
		{Index: RaftInitLogIndex + 2, Term: 1, Data: []byte("b")},
	}
	rd := raft.Ready{Entries: entries}
	require.NoError(t, s.SaveReady(rd))

	// Entries should be readable from cache.
	got, err := s.Entries(RaftInitLogIndex+1, RaftInitLogIndex+3, 0)
	require.NoError(t, err)
	assert.Len(t, got, 2)

	// Clear cache and read from engine.
	s.mu.Lock()
	s.entries = nil
	s.mu.Unlock()

	got, err = s.Entries(RaftInitLogIndex+1, RaftInitLogIndex+3, 0)
	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, []byte("a"), got[0].Data)
}

func TestPeerStorageTermFromEntries(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	entries := []raftpb.Entry{
		{Index: RaftInitLogIndex + 1, Term: 3, Data: []byte("x")},
		{Index: RaftInitLogIndex + 2, Term: 4, Data: []byte("y")},
	}
	rd := raft.Ready{Entries: entries}
	require.NoError(t, s.SaveReady(rd))

	term, err := s.Term(RaftInitLogIndex + 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), term)

	term, err = s.Term(RaftInitLogIndex + 2)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), term)
}

func TestPeerStorageEntriesCompacted(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	// Try to read entries before the first index.
	_, err := s.Entries(1, 3, 0)
	assert.Equal(t, raft.ErrCompacted, err)
}

func TestPeerStorageEntriesUnavailable(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	// Try to read entries after the last index.
	_, err := s.Entries(RaftInitLogIndex+1, RaftInitLogIndex+100, 0)
	assert.Equal(t, raft.ErrUnavailable, err)
}

func TestPeerStorageSnapshot(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	snap, err := s.Snapshot()
	require.NoError(t, err)
	assert.Equal(t, RaftInitLogIndex, snap.Metadata.Index)
	assert.Equal(t, RaftInitLogTerm, snap.Metadata.Term)
}

func TestPeerStorageApplyState(t *testing.T) {
	engine := newTestEngine(t)
	s := NewPeerStorage(1, engine)

	state := ApplyState{
		AppliedIndex:   10,
		TruncatedIndex: 5,
		TruncatedTerm:  2,
	}
	s.SetApplyState(state)

	got := s.GetApplyState()
	assert.Equal(t, state, got)
}

func TestLimitSize(t *testing.T) {
	entries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: make([]byte, 100)},
		{Index: 2, Term: 1, Data: make([]byte, 100)},
		{Index: 3, Term: 1, Data: make([]byte, 100)},
	}

	// maxSize 0 returns all.
	got := limitSize(entries, 0)
	assert.Len(t, got, 3)

	// At least one entry is always returned.
	got = limitSize(entries, 1)
	assert.Len(t, got, 1)

	// Large maxSize returns all.
	got = limitSize(entries, 100000)
	assert.Len(t, got, 3)
}
