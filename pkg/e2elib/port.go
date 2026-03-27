package e2elib

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// PortAllocator provides inter-process safe port allocation using file-based flock locking.
type PortAllocator struct {
	basePort int
	maxPort  int
	lockDir  string
	mu       sync.Mutex
	next     int
	held     map[int]*os.File
}

// NewPortAllocator creates a PortAllocator with default settings (port range 10200-32767).
func NewPortAllocator() *PortAllocator {
	lockDir := filepath.Join(os.TempDir(), "gookv-port-locks")
	_ = os.MkdirAll(lockDir, 0o755)
	return &PortAllocator{
		basePort: 10200,
		maxPort:  32767,
		lockDir:  lockDir,
		next:     10200,
		held:     make(map[int]*os.File),
	}
}

// AllocPort allocates a free port with inter-process safety using flock.
// It acquires an exclusive file lock and then verifies the port is actually bindable.
func (pa *PortAllocator) AllocPort() (int, error) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	startPort := pa.next
	for attempts := 0; attempts < (pa.maxPort - pa.basePort + 1); attempts++ {
		port := pa.next
		pa.next++
		if pa.next > pa.maxPort {
			pa.next = pa.basePort
		}

		// Skip already held ports.
		if _, ok := pa.held[port]; ok {
			continue
		}

		// Try to acquire flock.
		lockPath := filepath.Join(pa.lockDir, fmt.Sprintf("port-%d.lock", port))
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			continue
		}

		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err != nil {
			f.Close()
			continue
		}

		// Verify port is actually bindable.
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			f.Close()
			continue
		}
		ln.Close()

		pa.held[port] = f
		return port, nil
	}

	return 0, fmt.Errorf("e2elib: no free port found in range %d-%d (started at %d)", pa.basePort, pa.maxPort, startPort)
}

// Release releases a previously allocated port and its flock.
func (pa *PortAllocator) Release(port int) {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	if f, ok := pa.held[port]; ok {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		lockPath := filepath.Join(pa.lockDir, fmt.Sprintf("port-%d.lock", port))
		_ = os.Remove(lockPath)
		delete(pa.held, port)
	}
}

// ReleaseAll releases all held ports.
func (pa *PortAllocator) ReleaseAll() {
	pa.mu.Lock()
	defer pa.mu.Unlock()

	for port, f := range pa.held {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		lockPath := filepath.Join(pa.lockDir, fmt.Sprintf("port-%d.lock", port))
		_ = os.Remove(lockPath)
		delete(pa.held, port)
	}
}
