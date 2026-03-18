package raftstore

import (
	"path/filepath"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func newTestPeerForConfChange(t *testing.T) *Peer {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })

	region := &metapb.Region{
		Id: 1,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 2, StoreId: 2},
			{Id: 3, StoreId: 3},
		},
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 1,
			Version: 1,
		},
	}

	cfg := DefaultPeerConfig()
	peers := []raft.Peer{{ID: 1}, {ID: 2}, {ID: 3}}

	peer, err := NewPeer(1, 1, 1, region, eng, cfg, peers)
	require.NoError(t, err)

	return peer
}

func TestProcessConfChangeAddNode(t *testing.T) {
	p := newTestPeerForConfChange(t)

	context := EncodePeerContext(4)
	result := p.processConfChange(raftpb.ConfChangeAddNode, 4, 10, context)

	require.NotNil(t, result)
	assert.Equal(t, raftpb.ConfChangeAddNode, result.ChangeType)
	assert.Equal(t, uint64(4), result.Peer.GetId())
	assert.Equal(t, uint64(4), result.Peer.GetStoreId())
	assert.Equal(t, uint64(10), result.Index)

	// Verify region was updated.
	assert.Equal(t, uint64(2), result.Region.RegionEpoch.ConfVer) // incremented
	assert.Len(t, result.Region.Peers, 4)

	// Verify peer's region is updated.
	assert.Equal(t, uint64(2), p.region.RegionEpoch.ConfVer)
	assert.Len(t, p.region.Peers, 4)
}

func TestProcessConfChangeRemoveNode(t *testing.T) {
	p := newTestPeerForConfChange(t)

	result := p.processConfChange(raftpb.ConfChangeRemoveNode, 3, 11, nil)

	require.NotNil(t, result)
	assert.Equal(t, raftpb.ConfChangeRemoveNode, result.ChangeType)
	assert.Equal(t, uint64(3), result.Peer.GetId())
	assert.Equal(t, uint64(11), result.Index)

	// Verify region was updated.
	assert.Equal(t, uint64(2), result.Region.RegionEpoch.ConfVer) // incremented
	assert.Len(t, result.Region.Peers, 2)

	// Verify peer 3 was removed.
	for _, peer := range result.Region.Peers {
		assert.NotEqual(t, uint64(3), peer.GetId())
	}
}

func TestProcessConfChangeSelfRemoval(t *testing.T) {
	p := newTestPeerForConfChange(t)

	result := p.processConfChange(raftpb.ConfChangeRemoveNode, 1, 12, nil) // peerID = 1

	require.NotNil(t, result)
	assert.True(t, p.IsStopped(), "peer should be stopped after self-removal")
}

func TestProcessConfChangeDuplicateAdd(t *testing.T) {
	p := newTestPeerForConfChange(t)

	// Add peer 2 which already exists.
	context := EncodePeerContext(2)
	result := p.processConfChange(raftpb.ConfChangeAddNode, 2, 13, context)

	require.NotNil(t, result)
	// Should not add duplicate.
	assert.Len(t, result.Region.Peers, 3) // unchanged
	assert.Equal(t, uint64(2), result.Region.RegionEpoch.ConfVer) // epoch still incremented
}

func TestProcessConfChangeRemoveMissing(t *testing.T) {
	p := newTestPeerForConfChange(t)

	result := p.processConfChange(raftpb.ConfChangeRemoveNode, 99, 14, nil)

	require.NotNil(t, result)
	assert.Equal(t, uint64(99), result.Peer.GetId())
	// Peers list unchanged.
	assert.Len(t, result.Region.Peers, 3)
}

func TestEncodePeerContext(t *testing.T) {
	ctx := EncodePeerContext(42)
	assert.Len(t, ctx, 8)

	peer := decodePeerFromContext(ctx, 7)
	require.NotNil(t, peer)
	assert.Equal(t, uint64(7), peer.GetId())
	assert.Equal(t, uint64(42), peer.GetStoreId())
}

func TestDecodePeerContextEmpty(t *testing.T) {
	peer := decodePeerFromContext(nil, 5)
	assert.Nil(t, peer)

	peer = decodePeerFromContext([]byte{1, 2, 3}, 5)
	assert.Nil(t, peer)
}

func TestRemovePeerByNodeID(t *testing.T) {
	peers := []*metapb.Peer{
		{Id: 1, StoreId: 1},
		{Id: 2, StoreId: 2},
		{Id: 3, StoreId: 3},
	}

	removed, remaining := removePeerByNodeID(peers, 2)
	require.NotNil(t, removed)
	assert.Equal(t, uint64(2), removed.GetId())
	assert.Len(t, remaining, 2)

	// Remove non-existent.
	removed, remaining = removePeerByNodeID(remaining, 99)
	assert.Nil(t, removed)
	assert.Len(t, remaining, 2)
}

func TestCloneRegion(t *testing.T) {
	original := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 5, Version: 3},
	}

	clone := cloneRegion(original)

	assert.Equal(t, original.Id, clone.Id)
	assert.Equal(t, original.StartKey, clone.StartKey)
	assert.Equal(t, original.EndKey, clone.EndKey)
	assert.Equal(t, original.RegionEpoch.ConfVer, clone.RegionEpoch.ConfVer)
	assert.Len(t, clone.Peers, 1)

	// Mutate clone, verify original unchanged.
	clone.RegionEpoch.ConfVer = 99
	assert.Equal(t, uint64(5), original.RegionEpoch.ConfVer)
}
