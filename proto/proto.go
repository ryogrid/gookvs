// Package proto documents the protobuf dependencies for gookvs.
//
// gookvs reuses TiKV's kvproto definitions from github.com/pingcap/kvproto.
// The .proto source files in this directory are kept for reference only.
// All Go code is imported from the kvproto module's pre-generated packages.
//
// Key proto packages used by gookvs:
//
//   - kvrpcpb:          KV RPC protocol (Mutation, Op, LockInfo, etc.)
//   - metapb:           Cluster metadata (Region, Peer, RegionEpoch)
//   - errorpb:          Structured error types
//   - eraftpb:          Raft protocol (Entry, HardState, Snapshot, Message)
//   - raft_serverpb:    Raft server (RaftMessage, RegionLocalState, RaftApplyState)
//   - raft_cmdpb:       Raft command protocol
//   - pdpb:             PD protocol (heartbeats, TSO, bootstrap)
//   - tikvpb:           TiKV gRPC service definitions
//   - coprocessor:      Coprocessor push-down protocol
//   - debugpb:          Debug service protocol
//   - diagnosticspb:    Diagnostics service protocol
package proto
