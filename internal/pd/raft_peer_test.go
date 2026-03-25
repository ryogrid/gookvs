package pd

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestPDRaftCluster creates nodeCount PDRaftPeer instances connected
// via direct function calls (no gRPC, no network). Each peer gets its own
// in-memory RocksDB engine. sendFunc on each peer routes messages to the
// target peer's Mailbox. Run() goroutines are started for all peers.
// Returns the peers and a cancel function for cleanup.
func createTestPDRaftCluster(t *testing.T, nodeCount int) ([]*PDRaftPeer, context.CancelFunc) {
	t.Helper()

	// Build peer list and address map.
	var raftPeers []raft.Peer
	peerAddrs := make(map[uint64]string)
	for i := 1; i <= nodeCount; i++ {
		raftPeers = append(raftPeers, raft.Peer{ID: uint64(i)})
		peerAddrs[uint64(i)] = fmt.Sprintf("127.0.0.1:%d", 10000+i)
	}

	cfg := DefaultPDRaftConfig()
	// Use faster ticks for tests.
	cfg.RaftTickInterval = 10 * time.Millisecond
	cfg.ElectionTimeoutTicks = 10
	cfg.HeartbeatTicks = 2

	// Create peers.
	peers := make([]*PDRaftPeer, nodeCount)
	for i := 0; i < nodeCount; i++ {
		engine := newTestEngine(t)
		storage := NewPDRaftStorage(1, engine)
		nodeID := uint64(i + 1)

		p, err := NewPDRaftPeer(nodeID, storage, raftPeers, peerAddrs, cfg)
		require.NoError(t, err, "failed to create peer %d", nodeID)
		peers[i] = p
	}

	// Wire sendFunc: route messages to the target peer's Mailbox.
	for i := range peers {
		localPeers := peers // capture for closure
		peers[i].SetSendFunc(func(msgs []raftpb.Message) {
			for _, m := range msgs {
				targetID := m.To
				for _, p := range localPeers {
					if p.nodeID == targetID && !p.IsStopped() {
						msgCopy := m
						select {
						case p.Mailbox <- PDRaftMsg{
							Type: PDRaftMsgTypeRaftMessage,
							Data: &msgCopy,
						}:
						default:
							// Mailbox full, drop message (simulates network loss).
						}
						break
					}
				}
			}
		})
	}

	// Start Run goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(peer *PDRaftPeer) {
			defer wg.Done()
			peer.Run(ctx)
		}(p)
	}

	// Return a cleanup function that cancels and waits.
	cleanup := func() {
		cancel()
		wg.Wait()
	}

	return peers, cleanup
}

// waitForLeader polls the peers until one becomes leader, or the timeout expires.
// Returns the leader peer index, or -1 if no leader is found.
func waitForLeader(t *testing.T, peers []*PDRaftPeer, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, p := range peers {
			if p.IsLeader() {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return -1
}

// TestPDRaftPeer_LeaderElection verifies that 3 in-process peers elect exactly
// one leader within 5 seconds.
func TestPDRaftPeer_LeaderElection(t *testing.T) {
	peers, cleanup := createTestPDRaftCluster(t, 3)
	defer cleanup()

	leaderIdx := waitForLeader(t, peers, 5*time.Second)
	require.NotEqual(t, -1, leaderIdx, "no leader elected within 5 seconds")

	// Exactly one leader.
	leaderCount := 0
	for _, p := range peers {
		if p.IsLeader() {
			leaderCount++
		}
	}
	assert.Equal(t, 1, leaderCount, "expected exactly 1 leader, got %d", leaderCount)

	// The other two should not be leaders.
	for i, p := range peers {
		if i == leaderIdx {
			assert.True(t, p.IsLeader(), "peer %d should be leader", p.nodeID)
		} else {
			assert.False(t, p.IsLeader(), "peer %d should not be leader", p.nodeID)
		}
	}

	// All peers should agree on the leader ID.
	leaderID := peers[leaderIdx].nodeID
	for _, p := range peers {
		assert.Equal(t, leaderID, p.LeaderID(),
			"peer %d has incorrect leader ID", p.nodeID)
	}
}

// TestPDRaftPeer_ProposeAndApply verifies that proposing a CmdPutStore command
// on the leader succeeds, the apply function is called, and the result is returned.
func TestPDRaftPeer_ProposeAndApply(t *testing.T) {
	peers, cleanup := createTestPDRaftCluster(t, 3)
	defer cleanup()

	leaderIdx := waitForLeader(t, peers, 3*time.Second)
	require.NotEqual(t, -1, leaderIdx, "no leader elected")

	// Set up applyFunc on all peers to record and return a result.
	var mu sync.Mutex
	appliedCmds := make(map[uint64][]PDCommand) // nodeID -> applied commands
	for _, p := range peers {
		nodeID := p.nodeID
		p.SetApplyFunc(func(cmd PDCommand) ([]byte, error) {
			mu.Lock()
			appliedCmds[nodeID] = append(appliedCmds[nodeID], cmd)
			mu.Unlock()
			return []byte("ok"), nil
		})
	}

	// Propose a CmdPutStore command on the leader.
	leader := peers[leaderIdx]
	store := &metapb.Store{Id: 42, Address: "127.0.0.1:20160"}
	cmd := PDCommand{
		Type:  CmdPutStore,
		Store: store,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := leader.ProposeAndWait(ctx, cmd)
	require.NoError(t, err)
	assert.Equal(t, []byte("ok"), result)

	// Wait briefly for replication to followers.
	time.Sleep(200 * time.Millisecond)

	// All peers should have applied the command.
	mu.Lock()
	defer mu.Unlock()
	for _, p := range peers {
		cmds := appliedCmds[p.nodeID]
		require.NotEmpty(t, cmds, "peer %d did not apply any commands", p.nodeID)
		// Find the CmdPutStore command.
		found := false
		for _, c := range cmds {
			if c.Type == CmdPutStore && c.Store != nil && c.Store.Id == 42 {
				found = true
				break
			}
		}
		assert.True(t, found, "peer %d did not apply CmdPutStore for store 42", p.nodeID)
	}
}

// TestPDRaftPeer_ProposeOnFollower verifies that proposing on a follower
// returns ErrNotLeader.
func TestPDRaftPeer_ProposeOnFollower(t *testing.T) {
	peers, cleanup := createTestPDRaftCluster(t, 3)
	defer cleanup()

	leaderIdx := waitForLeader(t, peers, 3*time.Second)
	require.NotEqual(t, -1, leaderIdx, "no leader elected")

	// Find a follower.
	var follower *PDRaftPeer
	for i, p := range peers {
		if i != leaderIdx {
			follower = p
			break
		}
	}
	require.NotNil(t, follower, "no follower found")

	cmd := PDCommand{
		Type:        CmdIDAlloc,
		IDBatchSize: 10,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := follower.ProposeAndWait(ctx, cmd)
	assert.ErrorIs(t, err, ErrNotLeader)
}

// TestPDRaftPeer_MultipleProposals verifies that 10 sequential proposals on
// the leader all succeed, each returning a unique result.
func TestPDRaftPeer_MultipleProposals(t *testing.T) {
	peers, cleanup := createTestPDRaftCluster(t, 3)
	defer cleanup()

	leaderIdx := waitForLeader(t, peers, 3*time.Second)
	require.NotEqual(t, -1, leaderIdx, "no leader elected")

	// Set up applyFunc that returns a unique result for each CmdIDAlloc.
	var mu sync.Mutex
	allocCounter := uint64(0)
	for _, p := range peers {
		p.SetApplyFunc(func(cmd PDCommand) ([]byte, error) {
			if cmd.Type == CmdIDAlloc {
				mu.Lock()
				allocCounter++
				val := allocCounter
				mu.Unlock()
				buf := make([]byte, 8)
				binary.BigEndian.PutUint64(buf, val)
				return buf, nil
			}
			return nil, nil
		})
	}

	leader := peers[leaderIdx]
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		cmd := PDCommand{
			Type:        CmdIDAlloc,
			IDBatchSize: 1,
		}
		result, err := leader.ProposeAndWait(ctx, cmd)
		require.NoError(t, err, "proposal %d failed", i)
		require.NotNil(t, result, "proposal %d returned nil result", i)
		results[i] = result
	}

	// Verify all results are unique.
	seen := make(map[uint64]bool)
	for i, r := range results {
		val := binary.BigEndian.Uint64(r)
		assert.False(t, seen[val], "proposal %d has duplicate result %d", i, val)
		seen[val] = true
	}
	assert.Equal(t, 10, len(seen), "expected 10 unique results")
}

// TestPDRaftPeer_LeaderChangeCallback verifies that the leaderChangeFunc
// callback fires with isLeader=true on the leader and isLeader=false on
// followers after leader election.
func TestPDRaftPeer_LeaderChangeCallback(t *testing.T) {
	peers, cleanup := createTestPDRaftCluster(t, 3)
	defer cleanup()

	// Track callback invocations per node.
	type callbackEvent struct {
		nodeID   uint64
		isLeader bool
	}
	var mu sync.Mutex
	var events []callbackEvent

	for _, p := range peers {
		nodeID := p.nodeID
		p.SetLeaderChangeFunc(func(isLeader bool) {
			mu.Lock()
			events = append(events, callbackEvent{nodeID: nodeID, isLeader: isLeader})
			mu.Unlock()
		})
	}

	// Wait for leader election.
	leaderIdx := waitForLeader(t, peers, 3*time.Second)
	require.NotEqual(t, -1, leaderIdx, "no leader elected within 3 seconds")

	// Give a bit of time for all callbacks to fire.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// The leader should have received a callback with isLeader=true.
	leaderNodeID := peers[leaderIdx].nodeID
	leaderGotTrue := false
	for _, ev := range events {
		if ev.nodeID == leaderNodeID && ev.isLeader {
			leaderGotTrue = true
			break
		}
	}
	assert.True(t, leaderGotTrue,
		"leader node %d should have received leaderChangeFunc(true)", leaderNodeID)

	// Each follower should have NOT received isLeader=true as their final state.
	// (They may have received intermediate callbacks, but the last callback
	// for a follower must be isLeader=false, or no callback at all.)
	for i, p := range peers {
		if i == leaderIdx {
			continue
		}
		// Find the last callback for this node.
		lastIsLeader := false
		hasCallback := false
		for _, ev := range events {
			if ev.nodeID == p.nodeID {
				lastIsLeader = ev.isLeader
				hasCallback = true
			}
		}
		if hasCallback {
			assert.False(t, lastIsLeader,
				"follower node %d should not have isLeader=true as final state", p.nodeID)
		}
		// It's also valid for a follower to have no callback if it was never
		// a leader candidate (wasLeader starts as false, stays false).
	}
}
