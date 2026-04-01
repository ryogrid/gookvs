package client

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
)

// ConnPoolConfig configures a ConnPool.
type ConnPoolConfig struct {
	PoolSize int                                              // connections per store (default 2)
	DialFunc func(addr string) (*grpc.ClientConn, error)     // injectable dialer for testing
}

// ConnPool manages a pool of gRPC connections per store address.
// It uses round-robin selection to distribute RPCs across connections.
type ConnPool struct {
	mu       sync.RWMutex
	pools    map[string]*storePool
	poolSize int
	dialFunc func(addr string) (*grpc.ClientConn, error)
	closed   bool
}

// storePool holds connections to a single store.
type storePool struct {
	mu      sync.Mutex
	addr    string
	conns   []*grpc.ClientConn
	nextIdx atomic.Uint64
	dialFn  func(addr string) (*grpc.ClientConn, error)
}

// NewConnPool creates a new ConnPool.
func NewConnPool(cfg ConnPoolConfig) *ConnPool {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = 2
	}
	if cfg.DialFunc == nil {
		cfg.DialFunc = defaultDial
	}
	return &ConnPool{
		pools:    make(map[string]*storePool),
		poolSize: cfg.PoolSize,
		dialFunc: cfg.DialFunc,
	}
}

// Get returns a gRPC connection to the given address using round-robin selection.
func (p *ConnPool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("conn pool is closed")
	}
	sp, ok := p.pools[addr]
	p.mu.RUnlock()

	if !ok {
		sp = p.getOrCreatePool(addr)
		if sp == nil {
			return nil, fmt.Errorf("conn pool is closed")
		}
	}
	return sp.roundRobin()
}

func (p *ConnPool) getOrCreatePool(addr string) *storePool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	if sp, ok := p.pools[addr]; ok {
		return sp
	}
	sp := &storePool{
		addr:   addr,
		conns:  make([]*grpc.ClientConn, p.poolSize),
		dialFn: p.dialFunc,
	}
	p.pools[addr] = sp
	return sp
}

// Close closes all pooled connections.
func (p *ConnPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	for addr, sp := range p.pools {
		sp.closeAll()
		delete(p.pools, addr)
	}
}

func (sp *storePool) roundRobin() (*grpc.ClientConn, error) {
	idx := int(sp.nextIdx.Add(1) % uint64(len(sp.conns)))

	sp.mu.Lock()
	conn := sp.conns[idx]
	if conn != nil {
		sp.mu.Unlock()
		return conn, nil
	}

	// Lazy dial for this slot.
	slog.Debug("conn_pool.dial", "addr", sp.addr, "slot", idx)
	var err error
	conn, err = sp.dialFn(sp.addr)
	if err != nil {
		sp.mu.Unlock()
		return nil, fmt.Errorf("dial %s: %w", sp.addr, err)
	}
	sp.conns[idx] = conn
	sp.mu.Unlock()
	return conn, nil
}

func (sp *storePool) closeAll() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for i, conn := range sp.conns {
		if conn != nil {
			conn.Close()
			sp.conns[i] = nil
		}
	}
}

func defaultDial(addr string) (*grpc.ClientConn, error) {
	// This will be replaced when integrating with RegionRequestSender.
	// The actual dial options are configured there.
	return grpc.NewClient(addr)
}
