package e2elib

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ryogrid/gookv/pkg/client"
)

// GokvNodeConfig holds configuration for starting a gookv-server node.
type GokvNodeConfig struct {
	BinaryPath     string
	StoreID        uint64
	PDEndpoints    []string
	InitialCluster string
	LogLevel       string
	ExtraFlags     []string
}

// GokvNode represents a running gookv-server process managed for testing.
type GokvNode struct {
	t          *testing.T
	cfg        GokvNodeConfig
	grpcPort   int
	statusPort int
	dataDir    string
	logPath    string
	configPath string
	cmd        *exec.Cmd
	logFile    *os.File
	alloc      *PortAllocator
	topClient  *client.Client
	running    bool
}

// NewGokvNode creates a new GokvNode. It allocates two ports (gRPC and status)
// and creates a temp directory for data. It registers t.Cleanup to stop the node
// and release ports.
func NewGokvNode(t *testing.T, alloc *PortAllocator, cfg GokvNodeConfig) *GokvNode {
	t.Helper()

	grpcPort, err := alloc.AllocPort()
	if err != nil {
		t.Fatalf("e2elib: alloc gRPC port: %v", err)
	}
	statusPort, err := alloc.AllocPort()
	if err != nil {
		alloc.Release(grpcPort)
		t.Fatalf("e2elib: alloc status port: %v", err)
	}

	dataDir := t.TempDir()
	logPath := filepath.Join(dataDir, "gookv-server.log")

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
		alloc.Release(grpcPort)
		alloc.Release(statusPort)
	})

	return n
}

// Start starts the gookv-server process.
func (n *GokvNode) Start() error {
	if n.running {
		return fmt.Errorf("e2elib: gookv node already running")
	}

	binary := n.cfg.BinaryPath
	if binary == "" {
		binary = FindBinary("gookv-server")
	}
	if binary == "" {
		return fmt.Errorf("e2elib: gookv-server binary not found")
	}

	logLevel := n.cfg.LogLevel
	if logLevel == "" {
		logLevel = "info"
	}

	args := []string{
		"--addr", n.Addr(),
		"--status-addr", n.StatusAddr(),
		"--data-dir", n.dataDir,
		"--log-level", logLevel,
	}

	// Cluster mode flags (omitted for standalone mode).
	if n.cfg.StoreID != 0 {
		args = append(args, "--store-id", strconv.FormatUint(n.cfg.StoreID, 10))
	}
	if len(n.cfg.PDEndpoints) > 0 {
		args = append(args, "--pd-endpoints", strings.Join(n.cfg.PDEndpoints, ","))
	}
	if n.cfg.InitialCluster != "" {
		args = append(args, "--initial-cluster", n.cfg.InitialCluster)
	}

	if n.configPath != "" {
		args = append(args, "--config", n.configPath)
	}

	// Direct slog output to the same file as stdout/stderr for unified logging.
	args = append(args, "--log-file", n.logPath)

	args = append(args, n.cfg.ExtraFlags...)

	n.cmd = exec.Command(binary, args...)
	n.cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	lf, err := os.OpenFile(n.logPath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("e2elib: open server log file: %w", err)
	}
	n.cmd.Stdout = lf
	n.cmd.Stderr = lf

	if err := n.cmd.Start(); err != nil {
		lf.Close()
		return fmt.Errorf("e2elib: start gookv-server: %w", err)
	}

	n.logFile = lf
	n.running = true
	return nil
}

// Stop stops the gookv-server process gracefully, falling back to kill.
func (n *GokvNode) Stop() error {
	if !n.running || n.cmd == nil || n.cmd.Process == nil {
		return nil
	}

	// Close client if open.
	if n.topClient != nil {
		_ = n.topClient.Close()
		n.topClient = nil
	}

	// Try graceful shutdown.
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

	// Close log file handle.
	if n.logFile != nil {
		_ = n.logFile.Close()
		n.logFile = nil
	}
	return nil
}

// Restart stops and then starts the node.
func (n *GokvNode) Restart() error {
	if err := n.Stop(); err != nil {
		return err
	}
	return n.Start()
}

// IsRunning returns whether the node process is currently running.
func (n *GokvNode) IsRunning() bool {
	return n.running
}

// WaitForReady waits until the gookv-server is accepting TCP connections on its gRPC port.
func (n *GokvNode) WaitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", n.Addr(), 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("e2elib: gookv node at %s not ready after %v", n.Addr(), timeout)
}

// Addr returns the gRPC address in "127.0.0.1:port" format.
func (n *GokvNode) Addr() string {
	return fmt.Sprintf("127.0.0.1:%d", n.grpcPort)
}

// StatusAddr returns the HTTP status address in "127.0.0.1:port" format.
func (n *GokvNode) StatusAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", n.statusPort)
}

// ensureClient creates the top-level client if it hasn't been created yet.
func (n *GokvNode) ensureClient() {
	n.t.Helper()
	if n.topClient != nil {
		return
	}

	ctx := context.Background()
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs: n.cfg.PDEndpoints,
	})
	if err != nil {
		n.t.Fatalf("e2elib: create client for node %d: %v", n.cfg.StoreID, err)
	}
	n.topClient = c

	n.t.Cleanup(func() {
		if n.topClient != nil {
			_ = n.topClient.Close()
			n.topClient = nil
		}
	})
}

// RawKV returns a RawKVClient connected via PD to the cluster this node belongs to.
func (n *GokvNode) RawKV() *client.RawKVClient {
	n.t.Helper()
	n.ensureClient()
	return n.topClient.RawKV()
}

// TxnKV returns a TxnKVClient connected via PD to the cluster this node belongs to.
func (n *GokvNode) TxnKV() *client.TxnKVClient {
	n.t.Helper()
	n.ensureClient()
	return n.topClient.TxnKV()
}

// LogFile returns the path to the server log file.
func (n *GokvNode) LogFile() string {
	return n.logPath
}

// DataDir returns the server data directory path.
func (n *GokvNode) DataDir() string {
	return n.dataDir
}

// WriteConfig writes a TOML configuration file for this node.
// The content is written to a temp file and the configPath is set so Start() will
// pass --config to the binary.
func (n *GokvNode) WriteConfig(content string) {
	n.t.Helper()
	configPath := filepath.Join(n.dataDir, "gookv.toml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		n.t.Fatalf("e2elib: write config: %v", err)
	}
	n.configPath = configPath
}
