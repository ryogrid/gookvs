package e2elib

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"net"

	"github.com/ryogrid/gookv/pkg/pdclient"
)

// PDClusterConfig holds configuration for a multi-node PD cluster.
type PDClusterConfig struct {
	NumNodes   int
	ClusterID  uint64
	BinaryPath string
	LogLevel   string
}

// PDCluster manages multiple PD nodes with Raft replication for testing.
type PDCluster struct {
	t         *testing.T
	cfg       PDClusterConfig
	alloc     *PortAllocator
	nodes     []*PDClusterNode
	topClient pdclient.Client
}

// PDClusterNode represents a single PD node in a replicated cluster.
type PDClusterNode struct {
	t          *testing.T
	id         uint64
	clientPort int
	peerPort   int
	dataDir    string
	logPath    string
	cmd        *exec.Cmd
	logFile    *os.File
	running    bool
}

// NewPDCluster creates a multi-node PD cluster (but does not start it).
func NewPDCluster(t *testing.T, cfg PDClusterConfig) *PDCluster {
	t.Helper()

	if cfg.NumNodes == 0 {
		cfg.NumNodes = 3
	}
	if cfg.ClusterID == 0 {
		cfg.ClusterID = 1
	}

	alloc := NewPortAllocator()
	t.Cleanup(func() { alloc.ReleaseAll() })

	return &PDCluster{
		t:     t,
		cfg:   cfg,
		alloc: alloc,
	}
}

// Start starts all PD nodes in the cluster.
func (c *PDCluster) Start() error {
	binary := c.cfg.BinaryPath
	if binary == "" {
		binary = FindBinary("gookv-pd")
	}
	if binary == "" {
		return fmt.Errorf("e2elib: gookv-pd binary not found")
	}

	logLevel := c.cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	// Allocate ports for all nodes.
	type portPair struct {
		client int
		peer   int
	}
	pairs := make([]portPair, c.cfg.NumNodes)
	for i := 0; i < c.cfg.NumNodes; i++ {
		clientPort, err := c.alloc.AllocPort()
		if err != nil {
			return fmt.Errorf("e2elib: alloc client port for PD %d: %w", i, err)
		}
		peerPort, err := c.alloc.AllocPort()
		if err != nil {
			return fmt.Errorf("e2elib: alloc peer port for PD %d: %w", i, err)
		}
		pairs[i] = portPair{client: clientPort, peer: peerPort}
	}

	// Build --initial-cluster string: "1=127.0.0.1:peerPort1,2=127.0.0.1:peerPort2,..."
	var clusterParts []string
	for i, p := range pairs {
		clusterParts = append(clusterParts, fmt.Sprintf("%d=127.0.0.1:%d", i+1, p.peer))
	}
	initialCluster := strings.Join(clusterParts, ",")

	// Build --client-cluster string: "1=127.0.0.1:clientPort1,2=127.0.0.1:clientPort2,..."
	var clientParts []string
	for i, p := range pairs {
		clientParts = append(clientParts, fmt.Sprintf("%d=127.0.0.1:%d", i+1, p.client))
	}
	clientCluster := strings.Join(clientParts, ",")

	// Create and start each node.
	for i := 0; i < c.cfg.NumNodes; i++ {
		pdID := uint64(i + 1)
		dataDir := c.t.TempDir()
		logPath := filepath.Join(dataDir, "pd.log")

		node := &PDClusterNode{
			t:          c.t,
			id:         pdID,
			clientPort: pairs[i].client,
			peerPort:   pairs[i].peer,
			dataDir:    dataDir,
			logPath:    logPath,
		}

		args := []string{
			"--addr", node.Addr(),
			"--cluster-id", strconv.FormatUint(c.cfg.ClusterID, 10),
			"--data-dir", dataDir,
			"--log-level", logLevel,
			"--pd-id", strconv.FormatUint(pdID, 10),
			"--initial-cluster", initialCluster,
			"--peer-port", fmt.Sprintf("0.0.0.0:%d", pairs[i].peer),
			"--client-cluster", clientCluster,
		}

		node.cmd = exec.Command(binary, args...)
		node.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

		lf, err := os.Create(logPath)
		if err != nil {
			return fmt.Errorf("e2elib: create PD log: %w", err)
		}
		node.cmd.Stdout = lf
		node.cmd.Stderr = lf

		if err := node.cmd.Start(); err != nil {
			lf.Close()
			return fmt.Errorf("e2elib: start PD %d: %w", pdID, err)
		}
		node.logFile = lf
		node.running = true

		c.nodes = append(c.nodes, node)
	}

	// Register cleanup.
	c.t.Cleanup(func() {
		c.Stop()
	})

	// Wait for all nodes to be ready.
	for i, node := range c.nodes {
		if err := node.WaitForReady(30 * time.Second); err != nil {
			return fmt.Errorf("e2elib: PD %d not ready: %w", i+1, err)
		}
	}

	return nil
}

// Stop stops all PD nodes.
func (c *PDCluster) Stop() {
	for i := len(c.nodes) - 1; i >= 0; i-- {
		_ = c.nodes[i].Stop()
	}
}

// Node returns the PD node at the given index (0-based).
func (c *PDCluster) Node(idx int) *PDClusterNode {
	if idx < 0 || idx >= len(c.nodes) {
		c.t.Fatalf("e2elib: PD node index %d out of range [0, %d)", idx, len(c.nodes))
	}
	return c.nodes[idx]
}

// Nodes returns all PD nodes.
func (c *PDCluster) Nodes() []*PDClusterNode {
	return c.nodes
}

// ClientAddrs returns the client addresses of all nodes.
func (c *PDCluster) ClientAddrs() []string {
	addrs := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		addrs[i] = n.Addr()
	}
	return addrs
}

// Client returns a cached pdclient.Client connected to all PD nodes.
// On the first call it creates and caches the client; subsequent calls return the same instance.
func (c *PDCluster) Client() pdclient.Client {
	c.t.Helper()

	if c.topClient != nil {
		return c.topClient
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: c.ClientAddrs(),
	})
	if err != nil {
		c.t.Fatalf("e2elib: create PD cluster client: %v", err)
	}

	c.topClient = cl
	c.t.Cleanup(func() {
		if c.topClient != nil {
			c.topClient.Close()
			c.topClient = nil
		}
	})

	return c.topClient
}

// ClientForNode creates a pdclient.Client connected only to a specific node.
func (c *PDCluster) ClientForNode(idx int) pdclient.Client {
	c.t.Helper()
	node := c.Node(idx)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{node.Addr()},
	})
	if err != nil {
		c.t.Fatalf("e2elib: create PD client for node %d: %v", idx, err)
	}

	c.t.Cleanup(func() {
		cl.Close()
	})

	return cl
}

// StopNode stops the PD node at the given index.
func (c *PDCluster) StopNode(idx int) error {
	return c.Node(idx).Stop()
}

// RestartNode restarts the PD node at the given index.
func (c *PDCluster) RestartNode(idx int) error {
	node := c.Node(idx)
	if err := node.Stop(); err != nil {
		return err
	}
	return node.Restart()
}

// --- PDClusterNode methods ---

// Addr returns the client address.
func (n *PDClusterNode) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", n.clientPort)
}

// Stop stops this PD node.
func (n *PDClusterNode) Stop() error {
	if !n.running || n.cmd == nil || n.cmd.Process == nil {
		return nil
	}

	if err := n.cmd.Process.Signal(os.Interrupt); err != nil {
		_ = n.cmd.Process.Kill()
	}

	done := make(chan error, 1)
	go func() {
		done <- n.cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = n.cmd.Process.Kill()
		<-done
	}

	n.running = false

	if n.logFile != nil {
		_ = n.logFile.Close()
		n.logFile = nil
	}
	return nil
}

// Restart starts a stopped PD node again with the same configuration.
func (n *PDClusterNode) Restart() error {
	if n.running {
		return fmt.Errorf("e2elib: PD node %d still running", n.id)
	}

	// Re-create the command with the same args.
	oldArgs := n.cmd.Args[1:] // Skip the binary path.
	binary := n.cmd.Path

	n.cmd = exec.Command(binary, oldArgs...)
	n.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	lf, err := os.OpenFile(n.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("e2elib: open PD log for restart: %w", err)
	}
	n.cmd.Stdout = lf
	n.cmd.Stderr = lf

	if err := n.cmd.Start(); err != nil {
		lf.Close()
		return fmt.Errorf("e2elib: restart PD %d: %w", n.id, err)
	}

	n.logFile = lf
	n.running = true
	return n.WaitForReady(30 * time.Second)
}

// WaitForReady waits until this PD node accepts TCP connections.
func (n *PDClusterNode) WaitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", n.Addr(), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("e2elib: PD node %d at %s not ready after %v", n.id, n.Addr(), timeout)
}

// IsRunning returns whether this node is currently running.
func (n *PDClusterNode) IsRunning() bool {
	return n.running
}

// LogFile returns the log file path.
func (n *PDClusterNode) LogFile() string {
	return n.logPath
}

// DataDir returns the data directory.
func (n *PDClusterNode) DataDir() string {
	return n.dataDir
}
