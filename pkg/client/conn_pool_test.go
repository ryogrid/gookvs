package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeDialer tracks dial calls for testing.
type fakeDialer struct {
	mu        sync.Mutex
	dialCount int
	failAddr  string // if set, dial to this addr returns error
}

func (d *fakeDialer) dial(addr string) (*grpc.ClientConn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failAddr != "" && addr == d.failAddr {
		return nil, fmt.Errorf("dial failed: %s", addr)
	}
	d.dialCount++
	// Use grpc.NewClient with insecure credentials for a real but lazy conn.
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func (d *fakeDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dialCount
}

func TestConnPool_SingleAddr(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})
	defer pool.Close()

	conn, err := pool.Get("127.0.0.1:20160")
	require.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestConnPool_RoundRobin(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})
	defer pool.Close()

	conns := make([]*grpc.ClientConn, 10)
	for i := 0; i < 10; i++ {
		c, err := pool.Get("127.0.0.1:20160")
		require.NoError(t, err)
		conns[i] = c
	}

	// With pool size 2, we should see 2 distinct connections.
	// Round-robin means alternating: slot 1, slot 0, slot 1, slot 0, ...
	assert.True(t, conns[0] != conns[1], "consecutive gets should return different connections")
	assert.True(t, conns[0] == conns[2], "every other get should return the same connection")
	assert.Equal(t, fd.count(), 2, "should dial exactly 2 connections")
}

func TestConnPool_LazyDial(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 4, DialFunc: fd.dial})
	defer pool.Close()

	// No dials yet.
	assert.Equal(t, 0, fd.count())

	// First Get dials one slot.
	_, err := pool.Get("127.0.0.1:20160")
	require.NoError(t, err)
	assert.Equal(t, 1, fd.count())

	// Second Get might hit same slot or different. After 4 Gets, all slots dialed.
	for i := 0; i < 3; i++ {
		_, err = pool.Get("127.0.0.1:20160")
		require.NoError(t, err)
	}
	assert.Equal(t, 4, fd.count(), "all 4 slots should be dialed after 4 gets")
}

func TestConnPool_ConcurrentGet(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})
	defer pool.Close()

	var wg sync.WaitGroup
	var errCount atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := pool.Get("127.0.0.1:20160")
			if err != nil {
				errCount.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(0), errCount.Load(), "no errors expected")
	assert.Equal(t, 2, fd.count(), "should dial exactly 2 connections")
}

func TestConnPool_MultipleAddrs(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})
	defer pool.Close()

	c1, err := pool.Get("127.0.0.1:20160")
	require.NoError(t, err)
	c2, err := pool.Get("127.0.0.1:20161")
	require.NoError(t, err)
	assert.True(t, c1 != c2, "different addresses should return different connections")
	assert.Equal(t, 2, fd.count(), "should dial once per address (first slot)")
}

func TestConnPool_Close(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})

	_, err := pool.Get("127.0.0.1:20160")
	require.NoError(t, err)

	pool.Close()

	_, err = pool.Get("127.0.0.1:20160")
	assert.Error(t, err, "Get after Close should return error")
}

func TestConnPool_DialError(t *testing.T) {
	fd := &fakeDialer{failAddr: "bad:1234"}
	pool := NewConnPool(ConnPoolConfig{PoolSize: 2, DialFunc: fd.dial})
	defer pool.Close()

	_, err := pool.Get("bad:1234")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dial failed")

	// Good address still works.
	conn, err := pool.Get("127.0.0.1:20160")
	require.NoError(t, err)
	assert.NotNil(t, conn)
}

func TestConnPool_DefaultPoolSize(t *testing.T) {
	fd := &fakeDialer{}
	pool := NewConnPool(ConnPoolConfig{DialFunc: fd.dial}) // PoolSize=0 -> default 2
	defer pool.Close()

	for i := 0; i < 4; i++ {
		_, err := pool.Get("127.0.0.1:20160")
		require.NoError(t, err)
	}
	assert.Equal(t, 2, fd.count(), "default pool size should be 2")
}
