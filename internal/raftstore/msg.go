// Package raftstore implements the Raft consensus and region management layer.
// It uses etcd/raft for core consensus, with one goroutine per peer (region replica)
// and sync.Map-based message routing.
package raftstore

import (
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"go.etcd.io/etcd/raft/v3"
)

// PeerMsgType classifies messages delivered to peer goroutines.
type PeerMsgType int

const (
	// PeerMsgTypeRaftMessage carries a Raft protocol message from a peer.
	PeerMsgTypeRaftMessage PeerMsgType = iota
	// PeerMsgTypeRaftCommand carries a client read/write request.
	PeerMsgTypeRaftCommand
	// PeerMsgTypeTick delivers a timer tick.
	PeerMsgTypeTick
	// PeerMsgTypeApplyResult delivers results from the apply worker.
	PeerMsgTypeApplyResult
	// PeerMsgTypeSignificant carries high-priority control messages.
	PeerMsgTypeSignificant
	// PeerMsgTypeStart initializes the peer.
	PeerMsgTypeStart
	// PeerMsgTypeDestroy requests peer destruction.
	PeerMsgTypeDestroy
	// PeerMsgTypeCasual carries low-priority, droppable messages.
	PeerMsgTypeCasual
)

// PeerMsg is a message delivered to a peer goroutine's mailbox.
type PeerMsg struct {
	Type PeerMsgType
	Data interface{}
}

// PeerTickType identifies what kind of tick fired.
type PeerTickType int

const (
	// PeerTickRaft drives Raft heartbeats and election timeout.
	PeerTickRaft PeerTickType = iota
	// PeerTickRaftLogGC triggers log garbage collection.
	PeerTickRaftLogGC
	// PeerTickSplitRegionCheck triggers region size check for split.
	PeerTickSplitRegionCheck
	// PeerTickPdHeartbeat triggers region heartbeat to PD.
	PeerTickPdHeartbeat
	// PeerTickCheckMerge checks merge proposal status.
	PeerTickCheckMerge
	// PeerTickCheckPeerStaleState detects stale leadership.
	PeerTickCheckPeerStaleState
)

// RaftCommand encapsulates a client request to be proposed to Raft.
type RaftCommand struct {
	Request  *raft_cmdpb.RaftCmdRequest
	Callback func(*raft_cmdpb.RaftCmdResponse)
}

// ApplyResult contains the outcome of applying committed entries.
type ApplyResult struct {
	RegionID uint64
	Results  []ExecResult
}

// ExecResult represents the result of executing a single committed entry.
type ExecResult struct {
	// Type indicates what kind of result this is.
	Type ExecResultType
	// Data contains type-specific result data.
	Data interface{}
}

// ExecResultType classifies apply execution results.
type ExecResultType int

const (
	ExecResultTypeNormal      ExecResultType = iota
	ExecResultTypeSplitRegion
	ExecResultTypeCompactLog
	ExecResultTypeChangePeer
)

// SplitRegionResult contains the result of a region split operation.
type SplitRegionResult struct {
	Derived *metapb.Region   // The parent region after split
	Regions []*metapb.Region // New regions created by split
}

// StoreMsgType classifies messages to the store goroutine.
type StoreMsgType int

const (
	StoreMsgTypeRaftMessage      StoreMsgType = iota
	StoreMsgTypeStoreUnreachable
	StoreMsgTypeTick
	StoreMsgTypeStart
	StoreMsgTypeCreatePeer
	StoreMsgTypeDestroyPeer
)

// StoreMsg is a message delivered to the store goroutine.
type StoreMsg struct {
	Type StoreMsgType
	Data interface{}
}

// SignificantMsgType classifies high-priority control messages.
type SignificantMsgType int

const (
	SignificantMsgTypeSnapshotStatus SignificantMsgType = iota
	SignificantMsgTypeUnreachable
	SignificantMsgTypeMergeResult
)

// SignificantMsg wraps a high-priority control message.
type SignificantMsg struct {
	Type     SignificantMsgType
	RegionID uint64
	// SnapshotStatus fields.
	ToPeerID uint64
	Status   raft.SnapshotStatus
}

// Raft initialization constants (matching TiKV).
const (
	RaftInitLogTerm  uint64 = 5
	RaftInitLogIndex uint64 = 5
)
