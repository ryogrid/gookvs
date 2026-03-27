package pd

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/ryogrid/gookv/internal/raftstore"
)

// PDTransport manages gRPC connections to other PD peers for Raft message transport.
// It uses lazy, single-connection-per-peer semantics since PD Raft traffic is
// relatively infrequent compared to TiKV store Raft traffic.
type PDTransport struct {
	mu    sync.RWMutex
	conns map[uint64]*grpc.ClientConn // peerID -> connection
	addrs map[uint64]string           // peerID -> peer address
}

// NewPDTransport creates a new PDTransport with the given peer address map.
// peerAddrs maps peerID to network address (e.g., "127.0.0.1:2380").
func NewPDTransport(peerAddrs map[uint64]string) *PDTransport {
	addrs := make(map[uint64]string, len(peerAddrs))
	for id, addr := range peerAddrs {
		addrs[id] = addr
	}
	return &PDTransport{
		conns: make(map[uint64]*grpc.ClientConn),
		addrs: addrs,
	}
}

// Send sends a single raftpb.Message to the specified peer via gRPC.
// It converts the message from raftpb to eraftpb format, wraps it in a
// RaftMessage (with RegionId=0 as PD sentinel), and calls the remote
// SendPDRaftMessage unary RPC.
func (t *PDTransport) Send(peerID uint64, msg raftpb.Message) error {
	conn, err := t.getOrCreateConn(peerID)
	if err != nil {
		return fmt.Errorf("pd transport: get connection to peer %d: %w", peerID, err)
	}

	// Convert raftpb.Message → eraftpb.Message.
	eMsg, err := raftstore.RaftpbToEraftpb(&msg)
	if err != nil {
		return fmt.Errorf("pd transport: convert raftpb to eraftpb: %w", err)
	}

	// Wrap in RaftMessage with RegionId=0 as PD Raft sentinel.
	req := &raft_serverpb.RaftMessage{
		RegionId: 0,
		Message:  eMsg,
	}

	// Call the unary RPC directly via Invoke.
	resp := &raft_serverpb.RaftMessage{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = conn.Invoke(ctx, "/pd.PDPeer/SendPDRaftMessage", req, resp)
	if err != nil {
		slog.Warn("pd transport: send failed, closing stale connection",
			"peer", peerID, "err", err)
		t.closeConn(peerID)
		return fmt.Errorf("pd transport: send to peer %d: %w", peerID, err)
	}

	return nil
}

// Close closes all connections managed by this transport.
func (t *PDTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for id, conn := range t.conns {
		if err := conn.Close(); err != nil {
			slog.Warn("pd transport: close connection error",
				"peer", id, "err", err)
		}
	}
	t.conns = make(map[uint64]*grpc.ClientConn)
}

// getOrCreateConn returns an existing connection to the peer or lazily creates one.
func (t *PDTransport) getOrCreateConn(peerID uint64) (*grpc.ClientConn, error) {
	// Fast path: read lock.
	t.mu.RLock()
	conn, ok := t.conns[peerID]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	// Slow path: write lock to create connection.
	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock.
	if conn, ok := t.conns[peerID]; ok {
		return conn, nil
	}

	addr, ok := t.addrs[peerID]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID %d", peerID)
	}

	slog.Debug("pd transport: dialing peer", "peer", peerID, "addr", addr)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: false,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial peer %d at %s: %w", peerID, addr, err)
	}

	t.conns[peerID] = conn
	return conn, nil
}

// closeConn closes and removes a stale connection for the given peer.
func (t *PDTransport) closeConn(peerID uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[peerID]; ok {
		conn.Close()
		delete(t.conns, peerID)
	}
}
