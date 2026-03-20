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
	"syscall"

	goolog "github.com/ryogrid/gookv/internal/log"
	"github.com/ryogrid/gookv/internal/pd"
)

func main() {
	listenAddr := flag.String("addr", "0.0.0.0:2379", "gRPC listen address")
	dataDir := flag.String("data-dir", "/tmp/gookv-pd", "Data directory for metadata")
	clusterID := flag.Uint64("cluster-id", 1, "Cluster ID")
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (overrides default)")
	logFile := flag.String("log-file", "", "Log file path (overrides default)")
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
