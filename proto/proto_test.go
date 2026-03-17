package proto

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/assert"
)

// TestProtoPackagesImportable verifies that all required kvproto packages
// are importable and that key types can be instantiated.
func TestProtoPackagesImportable(t *testing.T) {
	// kvrpcpb: KV RPC protocol
	t.Run("kvrpcpb", func(t *testing.T) {
		mut := &kvrpcpb.Mutation{
			Op:    kvrpcpb.Op_Put,
			Key:   []byte("test-key"),
			Value: []byte("test-value"),
		}
		assert.Equal(t, kvrpcpb.Op_Put, mut.Op)
		assert.Equal(t, []byte("test-key"), mut.Key)

		lockInfo := &kvrpcpb.LockInfo{
			PrimaryLock: []byte("primary"),
			LockVersion: 100,
			Key:         []byte("locked-key"),
			LockTtl:     3000,
		}
		assert.Equal(t, uint64(100), lockInfo.LockVersion)
	})

	// metapb: cluster metadata
	t.Run("metapb", func(t *testing.T) {
		region := &metapb.Region{
			Id:       1,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: 1,
				Version: 1,
			},
			Peers: []*metapb.Peer{
				{Id: 1, StoreId: 1},
				{Id: 2, StoreId: 2},
				{Id: 3, StoreId: 3},
			},
		}
		assert.Equal(t, uint64(1), region.Id)
		assert.Len(t, region.Peers, 3)
	})

	// errorpb: error types
	t.Run("errorpb", func(t *testing.T) {
		err := &errorpb.Error{
			Message: "test error",
			NotLeader: &errorpb.NotLeader{
				RegionId: 1,
			},
		}
		assert.Equal(t, "test error", err.Message)
		assert.NotNil(t, err.NotLeader)
	})

	// eraftpb: Raft protocol
	t.Run("eraftpb", func(t *testing.T) {
		entry := &eraftpb.Entry{
			EntryType: eraftpb.EntryType_EntryNormal,
			Term:      1,
			Index:     1,
			Data:      []byte("raft-data"),
		}
		assert.Equal(t, uint64(1), entry.Term)

		hs := &eraftpb.HardState{
			Term:   1,
			Vote:   1,
			Commit: 0,
		}
		assert.Equal(t, uint64(1), hs.Term)
	})

	// raft_serverpb: Raft server types
	t.Run("raft_serverpb", func(t *testing.T) {
		msg := &raft_serverpb.RaftMessage{
			RegionId: 1,
			FromPeer: &metapb.Peer{Id: 1, StoreId: 1},
			ToPeer:   &metapb.Peer{Id: 2, StoreId: 2},
			Message:  &eraftpb.Message{},
		}
		assert.Equal(t, uint64(1), msg.RegionId)

		state := &raft_serverpb.RegionLocalState{
			State:  raft_serverpb.PeerState_Normal,
			Region: &metapb.Region{Id: 1},
		}
		assert.Equal(t, raft_serverpb.PeerState_Normal, state.State)
	})

	// raft_cmdpb: Raft command protocol
	t.Run("raft_cmdpb", func(t *testing.T) {
		req := &raft_cmdpb.RaftCmdRequest{
			Header: &raft_cmdpb.RaftRequestHeader{
				RegionId: 1,
			},
		}
		assert.Equal(t, uint64(1), req.Header.RegionId)
	})

	// pdpb: PD protocol
	t.Run("pdpb", func(t *testing.T) {
		ts := &pdpb.Timestamp{
			Physical: 1000,
			Logical:  1,
		}
		assert.Equal(t, int64(1000), ts.Physical)
	})

	// tikvpb: TiKV gRPC service (verify the service interface exists)
	t.Run("tikvpb", func(t *testing.T) {
		// Verify the Tikv service interface exists by checking it's not nil when cast
		var _ tikvpb.TikvServer = nil // compile-time check
	})

	// coprocessor: Coprocessor protocol
	t.Run("coprocessor", func(t *testing.T) {
		req := &coprocessor.Request{
			Tp:      1,
			StartTs: 100,
		}
		assert.Equal(t, uint64(100), req.StartTs)
	})

	// debugpb: Debug service
	t.Run("debugpb", func(t *testing.T) {
		var _ debugpb.DebugServer = nil // compile-time check
	})

	// diagnosticspb: Diagnostics service
	t.Run("diagnosticspb", func(t *testing.T) {
		var _ diagnosticspb.DiagnosticsServer = nil // compile-time check
	})
}

// TestProtoSerialization verifies that proto messages can be marshaled and unmarshaled.
func TestProtoSerialization(t *testing.T) {
	region := &metapb.Region{
		Id:       42,
		StartKey: []byte("start"),
		EndKey:   []byte("end"),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: 5,
			Version: 10,
		},
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
	}

	data, err := region.Marshal()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)

	decoded := &metapb.Region{}
	err = decoded.Unmarshal(data)
	assert.NoError(t, err)
	assert.Equal(t, uint64(42), decoded.Id)
	assert.Equal(t, []byte("start"), decoded.StartKey)
	assert.Equal(t, []byte("end"), decoded.EndKey)
	assert.Equal(t, uint64(5), decoded.RegionEpoch.ConfVer)
	assert.Equal(t, uint64(10), decoded.RegionEpoch.Version)
}

// TestKvrpcpbTypes verifies key kvrpcpb types needed by the transaction layer.
func TestKvrpcpbTypes(t *testing.T) {
	// Verify Op enum values match TiKV's
	assert.Equal(t, kvrpcpb.Op(0), kvrpcpb.Op_Put)
	assert.Equal(t, kvrpcpb.Op(1), kvrpcpb.Op_Del)
	assert.Equal(t, kvrpcpb.Op(2), kvrpcpb.Op_Lock)
	assert.Equal(t, kvrpcpb.Op(3), kvrpcpb.Op_Rollback)
	assert.Equal(t, kvrpcpb.Op(4), kvrpcpb.Op_Insert)
	assert.Equal(t, kvrpcpb.Op(5), kvrpcpb.Op_PessimisticLock)
	assert.Equal(t, kvrpcpb.Op(6), kvrpcpb.Op_CheckNotExists)

	// Verify Assertion enum
	assert.Equal(t, kvrpcpb.Assertion(0), kvrpcpb.Assertion_None)
	assert.Equal(t, kvrpcpb.Assertion(1), kvrpcpb.Assertion_Exist)
	assert.Equal(t, kvrpcpb.Assertion(2), kvrpcpb.Assertion_NotExist)

	// Verify IsolationLevel
	assert.Equal(t, kvrpcpb.IsolationLevel(0), kvrpcpb.IsolationLevel_SI)
	assert.Equal(t, kvrpcpb.IsolationLevel(1), kvrpcpb.IsolationLevel_RC)
}
