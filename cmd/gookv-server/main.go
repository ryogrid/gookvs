// gookv-server is the main entry point for the gookv distributed KV store.
// It starts the gRPC server, status HTTP server, and all supporting components.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"

	"github.com/ryogrid/gookv/internal/config"
	"github.com/ryogrid/gookv/internal/engine/rocks"
	goolog "github.com/ryogrid/gookv/internal/log"
	"github.com/ryogrid/gookv/internal/raftstore"
	raftrouter "github.com/ryogrid/gookv/internal/raftstore/router"
	"github.com/ryogrid/gookv/internal/raftstore/split"
	"github.com/ryogrid/gookv/internal/server"
	statusserver "github.com/ryogrid/gookv/internal/server/status"
	"github.com/ryogrid/gookv/internal/server/transport"
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
	logLevel := flag.String("log-level", "", "Log level: debug, info, warn, error (overrides config)")
	logFile := flag.String("log-file", "", "Log file path (overrides config)")
	maxPeerCount := flag.Int("max-peer-count", 3, "Maximum replicas per region (must match PD --max-peer-count)")
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
	} else if *configPath == "" && *storeID == 0 && *initialCluster == "" {
		// No explicit PD endpoints, no config file, no cluster flags:
		// clear the default PD endpoints to enable standalone mode.
		cfg.PD.Endpoints = nil
	}
	if *logLevel != "" {
		cfg.Log.Level = *logLevel
	}
	if *logFile != "" {
		cfg.Log.File.Filename = *logFile
	}

	// Validate configuration.
	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Resolve log file path.
	logFilename := cfg.Log.File.Filename
	if logFilename != "" && !filepath.IsAbs(logFilename) {
		logDir := filepath.Join(cfg.Storage.DataDir, "log")
		if err := os.MkdirAll(logDir, 0755); err != nil {
			log.Fatalf("Failed to create log directory %s: %v", logDir, err)
		}
		logFilename = filepath.Join(logDir, logFilename)
	}

	slowLogFile := cfg.SlowLogFile
	if slowLogFile != "" && !filepath.IsAbs(slowLogFile) {
		logDir := filepath.Join(cfg.Storage.DataDir, "log")
		_ = os.MkdirAll(logDir, 0755)
		slowLogFile = filepath.Join(logDir, slowLogFile)
	}

	goolog.Setup(goolog.LogConfig{
		Level:            cfg.Log.Level,
		Format:           cfg.Log.Format,
		Filename:         logFilename,
		MaxSizeMB:        cfg.Log.File.MaxSize,
		MaxBackups:       cfg.Log.File.MaxBackups,
		MaxAgeDays:       cfg.Log.File.MaxDays,
		SlowLogFile:      slowLogFile,
		SlowLogThreshold: cfg.SlowLogThreshold.Duration,
	})

	fmt.Printf("gookv-server starting\n")
	fmt.Printf("  gRPC addr:   %s\n", cfg.Server.Addr)
	fmt.Printf("  status addr: %s\n", cfg.Server.StatusAddr)
	fmt.Printf("  data dir:    %s\n", cfg.Storage.DataDir)
	fmt.Printf("  PD:          %v\n", cfg.PD.Endpoints)
	slog.Info("gookv-server starting",
		"addr", cfg.Server.Addr,
		"status-addr", cfg.Server.StatusAddr,
		"data-dir", cfg.Storage.DataDir,
		"pd", cfg.PD.Endpoints,
	)

	// Open storage engine.
	engine, err := rocks.Open(cfg.Storage.DataDir)
	if err != nil {
		slog.Error("Failed to open engine", "data-dir", cfg.Storage.DataDir, "error", err)
		os.Exit(1)
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

	// Cluster mode: bootstrap (with --initial-cluster) or join (with PD only).
	var coord *server.StoreCoordinator
	var pdWorker *server.PDWorker
	hasPD := len(cfg.PD.Endpoints) > 0 && cfg.PD.Endpoints[0] != ""
	hasInitialCluster := *initialCluster != ""

	if hasInitialCluster {
		// Bootstrap mode: static initial cluster topology.
		if *storeID == 0 {
			slog.Error("--store-id is required and must be non-zero when --initial-cluster is specified")
			os.Exit(1)
		}

		clusterMap := parseInitialCluster(*initialCluster)
		if len(clusterMap) == 0 {
			slog.Error("Invalid --initial-cluster format")
			os.Exit(1)
		}

		slog.Info("cluster mode enabled", "store-id", *storeID, "cluster", clusterMap)

		rtr := raftrouter.New(256)

		peerCfg := raftstore.DefaultPeerConfig()
		if cfg.RaftStore.RaftBaseTickInterval.Duration > 0 {
			peerCfg.RaftBaseTickInterval = cfg.RaftStore.RaftBaseTickInterval.Duration
		}
		if cfg.RaftStore.SplitCheckTickInterval.Duration > 0 {
			peerCfg.SplitCheckTickInterval = cfg.RaftStore.SplitCheckTickInterval.Duration
		}
		if cfg.RaftStore.PdHeartbeatTickInterval.Duration > 0 {
			peerCfg.PdHeartbeatTickInterval = cfg.RaftStore.PdHeartbeatTickInterval.Duration
		}
		// Connect to PD if endpoints are configured.
		var pdTaskCh chan<- interface{}
		var pdClient pdclient.Client
		if hasPD {
			pdCtx, pdCancel := context.WithTimeout(context.Background(), 10*time.Second)
			var pdErr error
			pdClient, pdErr = pdclient.NewClient(pdCtx, pdclient.Config{Endpoints: cfg.PD.Endpoints})
			pdCancel()
			if pdErr != nil {
				pdClient = nil // ensure nil on failure
				slog.Warn("PD connection failed, continuing without PD", "error", pdErr)
			} else {
				slog.Info("PD connected", "endpoints", cfg.PD.Endpoints)

				// Wire PD client into the server for TSO allocation.
				srv.SetPDClient(pdClient)

				regCtx, regCancel := context.WithTimeout(context.Background(), 5*time.Second)
				// Register all stores with PD so it knows their addresses.
				for sid, saddr := range clusterMap {
					_ = pdClient.PutStore(regCtx, &metapb.Store{Id: sid, Address: saddr})
				}
				regCancel()

				// Bootstrap cluster via PD (idempotent — only the first call succeeds).
				bsCtx, bsCancel := context.WithTimeout(context.Background(), 5*time.Second)
				bootstrapped, _ := pdClient.IsBootstrapped(bsCtx)
				if !bootstrapped {
					bsSIDs, _ := bootstrapStoreIDs(clusterMap, *maxPeerCount)
					peers := make([]*metapb.Peer, 0, len(bsSIDs))
					for _, sid := range bsSIDs {
						peers = append(peers, &metapb.Peer{Id: sid, StoreId: sid})
					}
					region := &metapb.Region{Id: 1, Peers: peers}
					store := &metapb.Store{Id: *storeID, Address: cfg.Server.Addr}
					_, _ = pdClient.Bootstrap(bsCtx, store, region)
					slog.Info("PD cluster bootstrapped")
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

		// Create store resolver: use PD-based resolver if PD is available
		// (enables discovering dynamically added stores), otherwise use static map.
		var resolver transport.StoreResolver
		if pdClient != nil {
			resolver = server.NewPDStoreResolver(pdClient, 30*time.Second)
		} else {
			resolver = server.NewStaticStoreResolver(clusterMap)
		}
		raftClient := transport.NewRaftClient(resolver, transport.DefaultRaftClientConfig())

		coord = server.NewStoreCoordinator(server.StoreCoordinatorConfig{
			StoreID:  *storeID,
			Engine:   engine,
			Storage:  storage,
			Router:   rtr,
			Client:   raftClient,
			PeerCfg:  peerCfg,
			PDTaskCh: pdTaskCh,
			PDClient: pdClient,
			SplitCheckCfg: split.SplitCheckWorkerConfig{
				SplitSize: uint64(cfg.RaftStore.RegionSplitSize),
				MaxSize:   uint64(cfg.RaftStore.RegionMaxSize),
				SplitKeys: uint64(cfg.Coprocessor.RegionSplitKeys),
				MaxKeys:   uint64(cfg.Coprocessor.RegionMaxKeys),
			},
			EnableBatchRaftWrite: cfg.RaftStore.EnableBatchRaftWrite,
			EnableApplyPipeline:  cfg.RaftStore.EnableApplyPipeline,
			EnableLeaseRead:      true,
		})
		srv.SetCoordinator(coord)

		if pdWorker != nil {
			pdWorker.SetCoordinator(coord)
			pdWorker.Run()
		}

		// Start store worker for dynamic peer creation.
		storeWorkerCtx, storeWorkerCancel := context.WithCancel(context.Background())
		go coord.RunStoreWorker(storeWorkerCtx)
		defer storeWorkerCancel()

		// Start split result handler if PD-coordinated splits are enabled.
		if pdClient != nil {
			splitCtx, splitCancel := context.WithCancel(context.Background())
			go coord.RunSplitResultHandler(splitCtx)
			defer splitCancel()
		}

		// Recover any persisted regions from a previous run (restart case).
		recovered := coord.RecoverPersistedRegions()
		if recovered > 0 {
			slog.Info("Recovered persisted regions", "count", recovered)
		}

		// Bootstrap a single region (region 1) with a voter subset.
		// Only the first min(NumNodes, MaxPeerCount) stores become initial
		// Raft voters. Non-bootstrap nodes start empty and receive regions
		// from PD scheduling (via maybeCreatePeerForMessage → CreatePeer).
		// Skip if regions were already recovered (restart scenario).
		if recovered == 0 {
			bsSIDs, _ := bootstrapStoreIDs(clusterMap, *maxPeerCount)

			isBootstrapNode := false
			for _, sid := range bsSIDs {
				if sid == *storeID {
					isBootstrapNode = true
					break
				}
			}

			if isBootstrapNode {
				peers := make([]*metapb.Peer, 0, len(bsSIDs))
				raftPeers := make([]raft.Peer, 0, len(bsSIDs))
				for _, sid := range bsSIDs {
					peers = append(peers, &metapb.Peer{Id: sid, StoreId: sid})
					raftPeers = append(raftPeers, raft.Peer{ID: sid})
				}
				region := &metapb.Region{
					Id:    1,
					Peers: peers,
				}
				if err := coord.BootstrapRegion(region, raftPeers); err != nil {
					slog.Error("Failed to bootstrap region", "error", err)
					os.Exit(1)
				}
				slog.Info("Raft cluster bootstrapped", "region", 1, "peers", len(raftPeers))
			} else {
				slog.Info("Non-bootstrap node: starting empty, waiting for PD region scheduling",
					"store-id", *storeID, "bootstrap-stores", bsSIDs)
			}
		}
	} else if hasPD {
		// Join mode: connect to PD, obtain store ID, register, start empty.
		pdCtx, pdCancel := context.WithTimeout(context.Background(), 10*time.Second)
		pdClient, pdErr := pdclient.NewClient(pdCtx, pdclient.Config{Endpoints: cfg.PD.Endpoints})
		pdCancel()
		if pdErr != nil {
			slog.Error("join mode requires PD connection", "error", pdErr)
			os.Exit(1)
		}
		slog.Info("PD connected (join mode)", "endpoints", cfg.PD.Endpoints)

		// Determine store ID: flag > persisted > allocate from PD.
		joinStoreID := *storeID
		if joinStoreID == 0 {
			loaded, loadErr := server.LoadStoreIdent(cfg.Storage.DataDir)
			if loadErr == nil && loaded > 0 {
				joinStoreID = loaded
				slog.Info("join mode: loaded persisted store ID", "store-id", joinStoreID)
			} else {
				allocCtx, allocCancel := context.WithTimeout(context.Background(), 5*time.Second)
				allocated, allocErr := pdClient.AllocID(allocCtx)
				allocCancel()
				if allocErr != nil {
					slog.Error("join mode: failed to allocate store ID from PD", "error", allocErr)
					os.Exit(1)
				}
				joinStoreID = allocated
				slog.Info("join mode: allocated store ID from PD", "store-id", joinStoreID)
			}
		}

		// Persist store ID for future restarts.
		if err := server.SaveStoreIdent(cfg.Storage.DataDir, joinStoreID); err != nil {
			slog.Error("join mode: failed to save store ident", "error", err)
			os.Exit(1)
		}

		slog.Info("join mode: registering with PD", "store-id", joinStoreID, "addr", cfg.Server.Addr)

		// Register this store with PD.
		regCtx, regCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := pdClient.PutStore(regCtx, &metapb.Store{Id: joinStoreID, Address: cfg.Server.Addr}); err != nil {
			slog.Error("join mode: failed to register store with PD", "error", err)
			os.Exit(1)
		}
		regCancel()

		// Wire PD client into the server for TSO allocation.
		srv.SetPDClient(pdClient)

		// Build peer config.
		peerCfg := raftstore.DefaultPeerConfig()
		if cfg.RaftStore.RaftBaseTickInterval.Duration > 0 {
			peerCfg.RaftBaseTickInterval = cfg.RaftStore.RaftBaseTickInterval.Duration
		}
		if cfg.RaftStore.SplitCheckTickInterval.Duration > 0 {
			peerCfg.SplitCheckTickInterval = cfg.RaftStore.SplitCheckTickInterval.Duration
		}
		if cfg.RaftStore.PdHeartbeatTickInterval.Duration > 0 {
			peerCfg.PdHeartbeatTickInterval = cfg.RaftStore.PdHeartbeatTickInterval.Duration
		}

		// PD-based resolver for dynamic peer discovery.
		resolver := server.NewPDStoreResolver(pdClient, 30*time.Second)
		raftClient := transport.NewRaftClient(resolver, transport.DefaultRaftClientConfig())

		rtr := raftrouter.New(256)

		// Create PDWorker and get its task channel.
		pdWorker = server.NewPDWorker(server.PDWorkerConfig{
			StoreID:  joinStoreID,
			PDClient: pdClient,
		})
		pdTaskCh := pdWorker.PeerTaskCh()

		coord = server.NewStoreCoordinator(server.StoreCoordinatorConfig{
			StoreID:  joinStoreID,
			Engine:   engine,
			Storage:  storage,
			Router:   rtr,
			Client:   raftClient,
			PeerCfg:  peerCfg,
			PDTaskCh: pdTaskCh,
			PDClient: pdClient,
			SplitCheckCfg: split.SplitCheckWorkerConfig{
				SplitSize: uint64(cfg.RaftStore.RegionSplitSize),
				MaxSize:   uint64(cfg.RaftStore.RegionMaxSize),
				SplitKeys: uint64(cfg.Coprocessor.RegionSplitKeys),
				MaxKeys:   uint64(cfg.Coprocessor.RegionMaxKeys),
			},
			EnableBatchRaftWrite: cfg.RaftStore.EnableBatchRaftWrite,
			EnableApplyPipeline:  cfg.RaftStore.EnableApplyPipeline,
			EnableLeaseRead:      true,
		})
		srv.SetCoordinator(coord)

		pdWorker.SetCoordinator(coord)
		pdWorker.Run()

		// Start store worker for dynamic peer creation.
		storeWorkerCtx, storeWorkerCancel := context.WithCancel(context.Background())
		go coord.RunStoreWorker(storeWorkerCtx)
		defer storeWorkerCancel()

		// Start split result handler.
		splitCtx, splitCancel := context.WithCancel(context.Background())
		go coord.RunSplitResultHandler(splitCtx)
		defer splitCancel()

		// Recover any persisted regions from a previous run (restart case).
		if n := coord.RecoverPersistedRegions(); n > 0 {
			slog.Info("Recovered persisted regions", "count", n)
		}

		// NOTE: No BootstrapRegion — join mode starts empty; PD will assign regions.
		slog.Info("join mode: store started (waiting for PD region assignment)",
			"store-id", joinStoreID)
	}

	if err := srv.Start(); err != nil {
		slog.Error("Failed to start gRPC server", "error", err)
		os.Exit(1)
	}
	slog.Info("gRPC server listening", "addr", srv.Addr())

	// Create and start HTTP status server.
	statusSrv := statusserver.New(statusserver.Config{
		Addr: cfg.Server.StatusAddr,
		ConfigFn: func() interface{} {
			return cfg
		},
	})
	if err := statusSrv.Start(); err != nil {
		slog.Error("Failed to start status server", "error", err)
		os.Exit(1)
	}
	slog.Info("Status server listening", "addr", statusSrv.Addr())

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("Received signal, shutting down", "signal", sig)

	// Graceful shutdown.
	if pdWorker != nil {
		pdWorker.Stop()
	}
	if coord != nil {
		coord.Stop()
	}
	_ = statusSrv.Stop()
	srv.Stop()
	slog.Info("gookv-server stopped")
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

// bootstrapStoreIDs selects the subset of store IDs that participate in the
// initial Raft group bootstrap. Returns sorted slices of bootstrap and join IDs.
// Bootstrap stores become initial Raft voters; join stores start empty and
// receive regions from PD scheduling.
func bootstrapStoreIDs(clusterMap map[uint64]string, maxPeerCount int) (bootstrap, join []uint64) {
	all := make([]uint64, 0, len(clusterMap))
	for sid := range clusterMap {
		all = append(all, sid)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	bootstrapCount := len(all)
	if maxPeerCount > 0 && maxPeerCount < bootstrapCount {
		bootstrapCount = maxPeerCount
	}
	return all[:bootstrapCount], all[bootstrapCount:]
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
			log.Fatalf("Invalid --initial-cluster entry (missing '='): %q", part)
		}
		id, err := strconv.ParseUint(part[:eqIdx], 10, 64)
		if err != nil {
			log.Fatalf("Invalid --initial-cluster entry (bad ID): %q: %v", part, err)
		}
		addr := part[eqIdx+1:]
		if addr == "" {
			log.Fatalf("Invalid --initial-cluster entry (empty address): %q", part)
		}
		result[id] = addr
	}
	return result
}
