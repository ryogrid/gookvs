// gookv-pd is the Placement Driver server for gookv clusters.
// It provides TSO allocation, cluster metadata management, and region scheduling.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	goolog "github.com/ryogrid/gookv/internal/log"
	"github.com/ryogrid/gookv/internal/pd"
)

// parseClusterString parses a cluster topology string of the form
// "ID1=HOST:PORT,ID2=HOST:PORT,..." into a map[uint64]string.
func parseClusterString(s string) (map[uint64]string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty cluster string")
	}

	result := make(map[uint64]string)
	entries := strings.Split(s, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid cluster entry %q: expected ID=HOST:PORT", entry)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid node ID %q in cluster entry %q: %w", parts[0], entry, err)
		}
		addr := strings.TrimSpace(parts[1])
		if addr == "" {
			return nil, fmt.Errorf("empty address in cluster entry %q", entry)
		}
		result[id] = addr
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid entries in cluster string")
	}

	return result, nil
}

func main() {
	listenAddr := flag.String("addr", "0.0.0.0:2379", "gRPC listen address")
	dataDir := flag.String("data-dir", "/tmp/gookv-pd", "Data directory for metadata")
	clusterID := flag.Uint64("cluster-id", 1, "Cluster ID")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (overrides default)")
	logFile := flag.String("log-file", "", "Log file path (overrides default)")

	// Raft replication flags (Step 15).
	pdID := flag.Uint64("pd-id", 0, "PD node ID (required with --initial-cluster)")
	initialCluster := flag.String("initial-cluster", "", "PD cluster topology: ID1=HOST:PORT,ID2=HOST:PORT,...")
	peerPort := flag.String("peer-port", "0.0.0.0:2380", "Listen address for PD-to-PD Raft peer communication")
	clientCluster := flag.String("client-cluster", "", "PD client addresses: ID1=HOST:PORT,... (for leader forwarding)")
	maxPeerCount := flag.Int("max-peer-count", 3, "Maximum number of replicas per region")

	flag.Parse()

	// Set up structured logging.
	logDir := filepath.Join(*dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create log directory: %v\n", err)
		os.Exit(1)
	}
	level := "info"
	if *logLevel != "" {
		level = *logLevel
	}
	logFilePath := filepath.Join(logDir, "pd.log")
	if *logFile != "" {
		logFilePath = *logFile
		if !filepath.IsAbs(logFilePath) {
			logFilePath = filepath.Join(logDir, logFilePath)
		}
	}
	goolog.Setup(goolog.LogConfig{
		Level:     level,
		Format:    "text",
		Filename:  logFilePath,
		MaxSizeMB: 300,
	})

	cfg := pd.DefaultPDServerConfig()
	cfg.ListenAddr = *listenAddr
	cfg.DataDir = *dataDir
	cfg.ClusterID = *clusterID
	if *maxPeerCount > 0 {
		cfg.MaxPeerCount = *maxPeerCount
	}

	// Step 16 & 17: Construct PDServerRaftConfig if --initial-cluster is specified.
	if *initialCluster != "" {
		// Validate --pd-id is non-zero.
		if *pdID == 0 {
			fmt.Fprintf(os.Stderr, "Error: --pd-id is required and must be non-zero when --initial-cluster is specified\n")
			os.Exit(1)
		}

		// Parse --initial-cluster.
		peers, err := parseClusterString(*initialCluster)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: invalid --initial-cluster: %v\n", err)
			os.Exit(1)
		}

		// Validate --pd-id exists in the parsed map.
		if _, ok := peers[*pdID]; !ok {
			fmt.Fprintf(os.Stderr, "Error: --pd-id %d not found in --initial-cluster\n", *pdID)
			os.Exit(1)
		}

		// Validate odd count (1, 3, 5, ...).
		if len(peers)%2 == 0 {
			fmt.Fprintf(os.Stderr, "Error: --initial-cluster must have an odd number of entries (got %d)\n", len(peers))
			os.Exit(1)
		}

		// Parse --client-cluster if provided.
		var clientAddrs map[uint64]string
		if *clientCluster != "" {
			clientAddrs, err = parseClusterString(*clientCluster)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid --client-cluster: %v\n", err)
				os.Exit(1)
			}
		}

		cfg.RaftConfig = &pd.PDServerRaftConfig{
			PDNodeID:             *pdID,
			InitialCluster:       peers,
			PeerAddr:             *peerPort,
			ClientAddrs:          clientAddrs,
			RaftTickInterval:     100 * time.Millisecond,
			ElectionTimeoutTicks: 10,
			HeartbeatTicks:       2,
		}
	}
	// If --initial-cluster is NOT specified, cfg.RaftConfig remains nil (single-node mode).

	server, err := pd.NewPDServer(cfg)
	if err != nil {
		slog.Error("Failed to create PD server", "error", err)
		os.Exit(1)
	}

	if err := server.Start(); err != nil {
		slog.Error("Failed to start PD server", "error", err)
		os.Exit(1)
	}

	fmt.Printf("gookv-pd listening on %s (cluster-id: %d)\n", server.Addr(), *clusterID)
	slog.Info("gookv-pd listening", "addr", server.Addr(), "cluster-id", *clusterID)

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("Shutting down PD server")
	server.Stop()
	slog.Info("PD server stopped")
}
