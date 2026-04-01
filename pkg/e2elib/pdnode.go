package e2elib

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ryogrid/gookv/pkg/pdclient"
)

// PDNodeConfig holds configuration for starting a PD node.
type PDNodeConfig struct {
	BinaryPath   string
	ClusterID    uint64
	LogLevel     string
	MaxPeerCount int // 0 = PD default (3)
}

// PDNode represents a running PD process managed for testing.
type PDNode struct {
	t          *testing.T
	cfg        PDNodeConfig
	port       int
	statusPort int
	dataDir    string
	logPath    string
	cmd        *exec.Cmd
	alloc      *PortAllocator
	running    bool
}

// NewPDNode creates a new PDNode. It allocates a port and creates a temp directory for data.
// It registers t.Cleanup to stop the node and release the port.
func NewPDNode(t *testing.T, alloc *PortAllocator, cfg PDNodeConfig) *PDNode {
	t.Helper()

	port, err := alloc.AllocPort()
	if err != nil {
		t.Fatalf("e2elib: alloc port for PD: %v", err)
	}
	statusPort, err := alloc.AllocPort()
	if err != nil {
		alloc.Release(port)
		t.Fatalf("e2elib: alloc status port for PD: %v", err)
	}

	dataDir := t.TempDir()
	logPath := filepath.Join(dataDir, "pd.log")

	n := &PDNode{
		t:          t,
		cfg:        cfg,
		port:       port,
		statusPort: statusPort,
		dataDir:    dataDir,
		logPath:    logPath,
		alloc:      alloc,
	}

	t.Cleanup(func() {
		_ = n.Stop()
		alloc.Release(port)
		alloc.Release(statusPort)
	})

	return n
}

// Start starts the PD process.
func (n *PDNode) Start() error {
	if n.running {
		return fmt.Errorf("e2elib: PD node already running")
	}

	binary := n.cfg.BinaryPath
	if binary == "" {
		binary = FindBinary("gookv-pd")
	}
	if binary == "" {
		return fmt.Errorf("e2elib: gookv-pd binary not found")
	}

	logLevel := n.cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	clusterID := n.cfg.ClusterID
	if clusterID == 0 {
		clusterID = 1
	}

	args := []string{
		"--addr", n.Addr(),
		"--status-addr", n.StatusAddr(),
		"--cluster-id", strconv.FormatUint(clusterID, 10),
		"--data-dir", n.dataDir,
		"--log-level", logLevel,
	}
	if n.cfg.MaxPeerCount > 0 {
		args = append(args, "--max-peer-count", strconv.Itoa(n.cfg.MaxPeerCount))
	}

	n.cmd = exec.Command(binary, args...)

	logFile, err := os.Create(n.logPath)
	if err != nil {
		return fmt.Errorf("e2elib: create PD log file: %w", err)
	}
	n.cmd.Stdout = logFile
	n.cmd.Stderr = logFile

	if err := n.cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("e2elib: start PD: %w", err)
	}

	n.running = true
	return nil
}

// Stop stops the PD process gracefully, falling back to kill.
func (n *PDNode) Stop() error {
	if !n.running || n.cmd == nil || n.cmd.Process == nil {
		return nil
	}

	// Try graceful shutdown first.
	if err := n.cmd.Process.Signal(os.Interrupt); err != nil {
		// If interrupt fails, force kill.
		_ = n.cmd.Process.Kill()
	}

	// Wait for the process to exit (with timeout).
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
	return nil
}

// Addr returns the PD address in "127.0.0.1:port" format.
func (n *PDNode) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", n.port)
}

// StatusAddr returns the HTTP status/pprof address.
func (n *PDNode) StatusAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", n.statusPort)
}

// Client creates a pdclient.Client connected to this PD node.
// The client is registered for cleanup when the test finishes.
func (n *PDNode) Client() pdclient.Client {
	n.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: []string{n.Addr()},
	})
	if err != nil {
		n.t.Fatalf("e2elib: create PD client: %v", err)
	}

	n.t.Cleanup(func() {
		c.Close()
	})

	return c
}

// LogFile returns the path to the PD log file.
func (n *PDNode) LogFile() string {
	return n.logPath
}

// DataDir returns the PD data directory path.
func (n *PDNode) DataDir() string {
	return n.dataDir
}

// WaitForReady waits until the PD node is accepting TCP connections.
func (n *PDNode) WaitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", n.Addr(), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("e2elib: PD node at %s not ready after %v", n.Addr(), timeout)
}
