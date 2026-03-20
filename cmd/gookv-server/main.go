// gookv-server is the main entry point for the gookv distributed KV store.
// It starts the gRPC server, status HTTP server, and all supporting components.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"

	"github.com/ryogrid/gookv/internal/config"
	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/raftstore"
	raftrouter "github.com/ryogrid/gookv/internal/raftstore/router"
	"github.com/ryogrid/gookv/internal/server"
	"github.com/ryogrid/gookv/internal/server/transport"
	statusserver "github.com/ryogrid/gookv/internal/server/status"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

func main() {
	configPath := flag.String("config", "", "Path to TOML config file")
	addr := flag.String("addr", "", "gRPC listen address (overrides config)")
	statusAddr := flag.String("status-addr", "", "HTTP status listen address (overrides config)")
	dataDir := flag.String("data-dir", "", "Storage data directory (overrides config)")
	pdEndpoints := flag.String("pd-endpoints", "", "PD endpoints, comma separated (overrides config)")
	storeID := flag.Uint64("store-id", 0, "Store ID for this node (enables cluster mode)")
	initialCluster := flag.String("initial-cluster", "", "Initial cluster topology: storeID=addr,... (e.g. 1=127.0.0.1:20160,2=127.0.0.1:20161)")
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

	fmt.Printf("gookv-server starting\n")
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

	// Cluster mode: set up Raft coordination if --store-id is specified.
	var coord *server.StoreCoordinator
	var pdWorker *server.PDWorker
	if *storeID > 0 && *initialCluster != "" {
		clusterMap := parseInitialCluster(*initialCluster)
		if len(clusterMap) == 0 {
			log.Fatalf("Invalid --initial-cluster format")
		}

		fmt.Printf("  store-id:    %d\n", *storeID)
		fmt.Printf("  cluster:     %v\n", clusterMap)

		resolver := server.NewStaticStoreResolver(clusterMap)
		raftClient := transport.NewRaftClient(resolver, transport.DefaultRaftClientConfig())
		rtr := raftrouter.New(256)

		peerCfg := raftstore.DefaultPeerConfig()
		if cfg.RaftStore.RaftBaseTickInterval.Duration > 0 {
			peerCfg.RaftBaseTickInterval = cfg.RaftStore.RaftBaseTickInterval.Duration
		}

		// Connect to PD if endpoints are configured.
		var pdTaskCh chan<- interface{}
		var pdClient pdclient.Client
		if len(cfg.PD.Endpoints) > 0 && cfg.PD.Endpoints[0] != "" {
			pdCtx, pdCancel := context.WithTimeout(context.Background(), 10*time.Second)
			var pdErr error
			pdClient, pdErr = pdclient.NewClient(pdCtx, pdclient.Config{Endpoints: cfg.PD.Endpoints})
			pdCancel()
			if pdErr != nil {
				pdClient = nil // ensure nil on failure
				fmt.Printf("  PD connection failed (continuing without PD): %v\n", pdErr)
			} else {
				fmt.Printf("  PD connected: %v\n", cfg.PD.Endpoints)

				// Wire PD client into the server for TSO allocation.
				srv.SetPDClient(pdClient)

				regCtx, regCancel := context.WithTimeout(context.Background(), 5*time.Second)
				// Register all stores with PD so it knows their addresses.
				for sid, addr := range clusterMap {
					_ = pdClient.PutStore(regCtx, &metapb.Store{Id: sid, Address: addr})
				}
				regCancel()

				// Bootstrap cluster via PD (idempotent — only the first call succeeds).
				bsCtx, bsCancel := context.WithTimeout(context.Background(), 5*time.Second)
				bootstrapped, _ := pdClient.IsBootstrapped(bsCtx)
				if !bootstrapped {
					peers := make([]*metapb.Peer, 0, len(clusterMap))
					for sid := range clusterMap {
						peers = append(peers, &metapb.Peer{Id: sid, StoreId: sid})
					}
					region := &metapb.Region{Id: 1, Peers: peers}
					store := &metapb.Store{Id: *storeID, Address: cfg.Server.Addr}
					_, _ = pdClient.Bootstrap(bsCtx, store, region)
					fmt.Println("  PD cluster bootstrapped")
				}
				bsCancel()

				// Create PDWorker and get its task channel for peers.
				pdWorker = server.NewPDWorker(server.PDWorkerConfig{
					StoreID:  *storeID,
					PDClient: pdClient,
				})
				pdTaskCh = pdWorker.PeerTaskCh()
			}
		}

		coord = server.NewStoreCoordinator(server.StoreCoordinatorConfig{
			StoreID:  *storeID,
			Engine:   engine,
			Storage:  storage,
			Router:   rtr,
			Client:   raftClient,
			PeerCfg:  peerCfg,
			PDTaskCh: pdTaskCh,
			PDClient: pdClient,
		})
		srv.SetCoordinator(coord)

		if pdWorker != nil {
			pdWorker.SetCoordinator(coord)
			pdWorker.Run()
		}

		// Start split result handler if PD-coordinated splits are enabled.
		if pdClient != nil {
			splitCtx, splitCancel := context.WithCancel(context.Background())
			go coord.RunSplitResultHandler(splitCtx)
			defer splitCancel()
		}

		// Bootstrap a single region (region 1) spanning all stores.
		peers := make([]*metapb.Peer, 0, len(clusterMap))
		raftPeers := make([]raft.Peer, 0, len(clusterMap))
		for sid := range clusterMap {
			peers = append(peers, &metapb.Peer{Id: sid, StoreId: sid})
			raftPeers = append(raftPeers, raft.Peer{ID: sid})
		}
		region := &metapb.Region{
			Id:    1,
			Peers: peers,
		}
		if err := coord.BootstrapRegion(region, raftPeers); err != nil {
			log.Fatalf("Failed to bootstrap region: %v", err)
		}
		fmt.Printf("Raft cluster bootstrapped (region 1, %d peers)\n", len(raftPeers))
	}

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
	if pdWorker != nil {
		pdWorker.Stop()
	}
	if coord != nil {
		coord.Stop()
	}
	_ = statusSrv.Stop()
	srv.Stop()
	fmt.Println("gookv-server stopped")
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

// parseInitialCluster parses "storeID=addr,storeID=addr,..." into a map.
func parseInitialCluster(s string) map[uint64]string {
	result := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		id, err := strconv.ParseUint(part[:eqIdx], 10, 64)
		if err != nil {
			continue
		}
		result[id] = part[eqIdx+1:]
	}
	return result
}
