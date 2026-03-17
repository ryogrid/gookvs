// gookvs-server is the main entry point for the gookvs distributed KV store.
// It starts the gRPC server, status HTTP server, and all supporting components.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ryogrid/gookvs/internal/config"
	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/server"
	statusserver "github.com/ryogrid/gookvs/internal/server/status"
)

func main() {
	configPath := flag.String("config", "", "Path to TOML config file")
	addr := flag.String("addr", "", "gRPC listen address (overrides config)")
	statusAddr := flag.String("status-addr", "", "HTTP status listen address (overrides config)")
	dataDir := flag.String("data-dir", "", "Storage data directory (overrides config)")
	pdEndpoints := flag.String("pd-endpoints", "", "PD endpoints, comma separated (overrides config)")
	flag.Parse()

	// Load configuration.
	var cfg *config.Config
	if *configPath != "" {
		var err error
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	// Apply CLI overrides.
	if *addr != "" {
		cfg.Server.Addr = *addr
	}
	if *statusAddr != "" {
		cfg.Server.StatusAddr = *statusAddr
	}
	if *dataDir != "" {
		cfg.Storage.DataDir = *dataDir
	}
	if *pdEndpoints != "" {
		cfg.PD.Endpoints = splitEndpoints(*pdEndpoints)
	}

	// Validate configuration.
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	fmt.Printf("gookvs-server starting\n")
	fmt.Printf("  gRPC addr:   %s\n", cfg.Server.Addr)
	fmt.Printf("  status addr: %s\n", cfg.Server.StatusAddr)
	fmt.Printf("  data dir:    %s\n", cfg.Storage.DataDir)
	fmt.Printf("  PD:          %v\n", cfg.PD.Endpoints)

	// Open storage engine.
	engine, err := rocks.Open(cfg.Storage.DataDir)
	if err != nil {
		log.Fatalf("Failed to open engine at %s: %v", cfg.Storage.DataDir, err)
	}
	defer engine.Close()

	// Create storage layer.
	storage := server.NewStorage(engine)

	// Create and start gRPC server.
	srvCfg := server.ServerConfig{
		ListenAddr: cfg.Server.Addr,
		ClusterID:  cfg.Server.ClusterID,
	}
	srv := server.NewServer(srvCfg, storage)
	if err := srv.Start(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
	fmt.Printf("gRPC server listening on %s\n", srv.Addr())

	// Create and start HTTP status server.
	statusSrv := statusserver.New(statusserver.Config{
		Addr: cfg.Server.StatusAddr,
		ConfigFn: func() interface{} {
			return cfg
		},
	})
	if err := statusSrv.Start(); err != nil {
		log.Fatalf("Failed to start status server: %v", err)
	}
	fmt.Printf("Status server listening on %s\n", statusSrv.Addr())

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\nReceived signal %v, shutting down...\n", sig)

	// Graceful shutdown.
	_ = statusSrv.Stop()
	srv.Stop()
	fmt.Println("gookvs-server stopped")
}

func splitEndpoints(s string) []string {
	var parts []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}
