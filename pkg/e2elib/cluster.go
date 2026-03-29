package e2elib

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ryogrid/gookv/pkg/client"
)

// GokvClusterConfig holds configuration for a multi-node gookv cluster.
type GokvClusterConfig struct {
	NumNodes           int
	PDConfig           PDNodeConfig
	SplitSize          string // e.g., "20KB"
	SplitCheckInterval string // e.g., "5s"
	ServerBinaryPath   string
	PDBinaryPath       string
}

// GokvCluster manages a PD node and multiple gookv-server nodes for e2e testing.
type GokvCluster struct {
	t         *testing.T
	cfg       GokvClusterConfig
	alloc     *PortAllocator
	pd        *PDNode
	nodes     []*GokvNode
	topClient *client.Client
	started   bool
}

// NewGokvCluster creates a new cluster (but does not start it).
func NewGokvCluster(t *testing.T, cfg GokvClusterConfig) *GokvCluster {
	t.Helper()
	alloc := NewPortAllocator()

	t.Cleanup(func() {
		alloc.ReleaseAll()
	})

	return &GokvCluster{
		t:     t,
		cfg:   cfg,
		alloc: alloc,
	}
}

// Start starts the PD node and all gookv-server nodes.
func (c *GokvCluster) Start() error {
	if c.started {
		return fmt.Errorf("e2elib: cluster already started")
	}

	// Create and start PD. Set MaxPeerCount to NumNodes so the scheduler
	// doesn't try to remove peers from regions that already have all nodes.
	pdCfg := c.cfg.PDConfig
	if pdCfg.BinaryPath == "" {
		pdCfg.BinaryPath = c.cfg.PDBinaryPath
	}
	if pdCfg.MaxPeerCount == 0 {
		pdCfg.MaxPeerCount = c.cfg.NumNodes
	}
	c.pd = NewPDNode(c.t, c.alloc, pdCfg)
	if err := c.pd.Start(); err != nil {
		return fmt.Errorf("e2elib: start PD: %w", err)
	}
	if err := c.pd.WaitForReady(30 * time.Second); err != nil {
		return fmt.Errorf("e2elib: PD not ready: %w", err)
	}

	// Allocate ports for all nodes upfront and build initialCluster string.
	type portPair struct {
		grpc   int
		status int
	}
	pairs := make([]portPair, c.cfg.NumNodes)
	for i := 0; i < c.cfg.NumNodes; i++ {
		grpcPort, err := c.alloc.AllocPort()
		if err != nil {
			return fmt.Errorf("e2elib: alloc gRPC port for node %d: %w", i, err)
		}
		statusPort, err := c.alloc.AllocPort()
		if err != nil {
			return fmt.Errorf("e2elib: alloc status port for node %d: %w", i, err)
		}
		pairs[i] = portPair{grpc: grpcPort, status: statusPort}
	}

	// Build initial cluster string: "1=127.0.0.1:port1,2=127.0.0.1:port2,..."
	var parts []string
	for i, p := range pairs {
		parts = append(parts, fmt.Sprintf("%d=127.0.0.1:%d", i+1, p.grpc))
	}
	initialCluster := strings.Join(parts, ",")

	pdEndpoints := []string{c.pd.Addr()}

	// Build TOML config if split settings are provided.
	var tomlConfig string
	if c.cfg.SplitSize != "" || c.cfg.SplitCheckInterval != "" {
		var lines []string
		lines = append(lines, "[raft-store]")
		if c.cfg.SplitSize != "" {
			lines = append(lines, fmt.Sprintf("region-split-size = %q", c.cfg.SplitSize))
		}
		if c.cfg.SplitCheckInterval != "" {
			lines = append(lines, fmt.Sprintf("split-check-tick-interval = %q", c.cfg.SplitCheckInterval))
		}
		tomlConfig = strings.Join(lines, "\n") + "\n"
	}

	// Create and start each node.
	for i := 0; i < c.cfg.NumNodes; i++ {
		nodeCfg := GokvNodeConfig{
			BinaryPath:     c.cfg.ServerBinaryPath,
			StoreID:        uint64(i + 1),
			PDEndpoints:    pdEndpoints,
			InitialCluster: initialCluster,
			LogLevel:       "info",
		}

		// Create the node manually with pre-allocated ports.
		node := newGokvNodeWithPorts(c.t, c.alloc, nodeCfg, pairs[i].grpc, pairs[i].status)

		if tomlConfig != "" {
			node.WriteConfig(tomlConfig)
		}

		if err := node.Start(); err != nil {
			return fmt.Errorf("e2elib: start node %d: %w", i, err)
		}
		c.nodes = append(c.nodes, node)
	}

	// Wait for all nodes to be ready.
	for i, node := range c.nodes {
		if err := node.WaitForReady(30 * time.Second); err != nil {
			return fmt.Errorf("e2elib: node %d not ready: %w", i, err)
		}
	}

	c.started = true
	return nil
}

// Stop stops all nodes and the PD node.
func (c *GokvCluster) Stop() error {
	if c.topClient != nil {
		_ = c.topClient.Close()
		c.topClient = nil
	}

	var firstErr error
	for i := len(c.nodes) - 1; i >= 0; i-- {
		if err := c.nodes[i].Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.pd != nil {
		if err := c.pd.Stop(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PD returns the PD node.
func (c *GokvCluster) PD() *PDNode {
	return c.pd
}

// Node returns the gookv-server node at the given index.
func (c *GokvCluster) Node(idx int) *GokvNode {
	if idx < 0 || idx >= len(c.nodes) {
		c.t.Fatalf("e2elib: node index %d out of range [0, %d)", idx, len(c.nodes))
	}
	return c.nodes[idx]
}

// Nodes returns all gookv-server nodes.
func (c *GokvCluster) Nodes() []*GokvNode {
	return c.nodes
}

// AddNode creates and starts a new node in join mode (no InitialCluster).
func (c *GokvCluster) AddNode() (*GokvNode, error) {
	if c.pd == nil {
		return nil, fmt.Errorf("e2elib: PD not started")
	}

	storeID := uint64(len(c.nodes) + 1)
	nodeCfg := GokvNodeConfig{
		BinaryPath:  c.cfg.ServerBinaryPath,
		StoreID:     storeID,
		PDEndpoints: []string{c.pd.Addr()},
		LogLevel:    "info",
	}

	node := NewGokvNode(c.t, c.alloc, nodeCfg)

	// Apply split config if set.
	if c.cfg.SplitSize != "" || c.cfg.SplitCheckInterval != "" {
		var lines []string
		lines = append(lines, "[raft-store]")
		if c.cfg.SplitSize != "" {
			lines = append(lines, fmt.Sprintf("region-split-size = %q", c.cfg.SplitSize))
		}
		if c.cfg.SplitCheckInterval != "" {
			lines = append(lines, fmt.Sprintf("split-check-tick-interval = %q", c.cfg.SplitCheckInterval))
		}
		node.WriteConfig(strings.Join(lines, "\n") + "\n")
	}

	if err := node.Start(); err != nil {
		return nil, fmt.Errorf("e2elib: start new node: %w", err)
	}
	if err := node.WaitForReady(30 * time.Second); err != nil {
		return nil, fmt.Errorf("e2elib: new node not ready: %w", err)
	}

	c.nodes = append(c.nodes, node)
	return node, nil
}

// StopNode stops the node at the given index.
func (c *GokvCluster) StopNode(idx int) error {
	if idx < 0 || idx >= len(c.nodes) {
		return fmt.Errorf("e2elib: node index %d out of range [0, %d)", idx, len(c.nodes))
	}
	return c.nodes[idx].Stop()
}

// RestartNode restarts the node at the given index.
func (c *GokvCluster) RestartNode(idx int) error {
	if idx < 0 || idx >= len(c.nodes) {
		return fmt.Errorf("e2elib: node index %d out of range [0, %d)", idx, len(c.nodes))
	}
	return c.nodes[idx].Restart()
}

// Client returns a shared *client.Client connected to the cluster via PD.
func (c *GokvCluster) Client() *client.Client {
	c.t.Helper()
	if c.topClient != nil {
		return c.topClient
	}

	ctx := context.Background()
	cl, err := client.NewClient(ctx, client.Config{
		PDAddrs:    []string{c.pd.Addr()},
		MaxRetries: 30,
	})
	if err != nil {
		c.t.Fatalf("e2elib: create cluster client: %v", err)
	}
	c.topClient = cl

	c.t.Cleanup(func() {
		if c.topClient != nil {
			_ = c.topClient.Close()
			c.topClient = nil
		}
	})

	return c.topClient
}

// ResetClient closes the cached client and forces a new one to be created
// on the next Client()/RawKV()/TxnKV() call. Useful after region splits
// to clear the stale region cache.
func (c *GokvCluster) ResetClient() {
	if c.topClient != nil {
		_ = c.topClient.Close()
		c.topClient = nil
	}
}

// RawKV returns a RawKVClient connected to the cluster.
func (c *GokvCluster) RawKV() *client.RawKVClient {
	c.t.Helper()
	return c.Client().RawKV()
}

// TxnKV returns a TxnKVClient connected to the cluster.
func (c *GokvCluster) TxnKV() *client.TxnKVClient {
	c.t.Helper()
	return c.Client().TxnKV()
}

// newGokvNodeWithPorts creates a GokvNode with pre-allocated ports (used by cluster Start).
func newGokvNodeWithPorts(t *testing.T, alloc *PortAllocator, cfg GokvNodeConfig, grpcPort, statusPort int) *GokvNode {
	t.Helper()

	dataDir := t.TempDir()
	logPath := fmt.Sprintf("%s/gookv-server.log", dataDir)

	n := &GokvNode{
		t:          t,
		cfg:        cfg,
		grpcPort:   grpcPort,
		statusPort: statusPort,
		dataDir:    dataDir,
		logPath:    logPath,
		alloc:      alloc,
	}

	t.Cleanup(func() {
		_ = n.Stop()
		// Ports are managed by the cluster's allocator; they were pre-allocated,
		// so the cluster's ReleaseAll will handle cleanup. But we release here
		// too for safety if the node is used standalone.
		alloc.Release(grpcPort)
		alloc.Release(statusPort)
	})

	return n
}
