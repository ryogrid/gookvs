// Package status implements the HTTP status and diagnostics server for gookvs.
// It provides pprof endpoints, Prometheus metrics, and config inspection.
package status

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the HTTP status and diagnostics server.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	addr       string
	configFn   func() interface{} // Returns current config for /config endpoint
	mu         sync.Mutex
	started    bool
}

// Config configures the status server.
type Config struct {
	Addr     string                // Listen address (e.g., "127.0.0.1:20180")
	ConfigFn func() interface{}   // Function returning current config
}

// New creates a new status Server.
func New(cfg Config) *Server {
	mux := http.NewServeMux()
	s := &Server{
		addr:     cfg.Addr,
		configFn: cfg.ConfigFn,
	}

	// Register pprof endpoints.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// Register metrics endpoint.
	mux.Handle("/metrics", promhttp.Handler())

	// Register config endpoint.
	mux.HandleFunc("/config", s.handleConfig)

	// Register status endpoint.
	mux.HandleFunc("/status", s.handleStatus)

	// Register health check.
	mux.HandleFunc("/health", s.handleHealth)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start binds and begins serving HTTP.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("status server already started")
	}

	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = lis
	s.started = true

	go func() {
		if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			fmt.Printf("status server error: %v\n", err)
		}
	}()

	return nil
}

// Addr returns the actual listen address (useful when binding to :0).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Stop gracefully shuts down the status server.
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.started = false
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.configFn == nil {
		http.Error(w, "Config not available", http.StatusServiceUnavailable)
		return
	}

	cfg := s.configFn()
	w.Header().Set("Content-Type", "application/json")
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(cfg); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode config: %v", err), http.StatusInternalServerError)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := map[string]interface{}{
		"status":  "ok",
		"version": "gookvs-dev",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
