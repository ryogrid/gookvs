package router

import (
	"sync"
	"testing"

	"github.com/ryogrid/gookvs/internal/raftstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterAndSend(t *testing.T) {
	r := New(16)

	ch := make(chan raftstore.PeerMsg, DefaultMailboxCapacity)
	require.NoError(t, r.Register(1, ch))

	msg := raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick}
	require.NoError(t, r.Send(1, msg))

	got := <-ch
	assert.Equal(t, raftstore.PeerMsgTypeTick, got.Type)
}

func TestSendToUnregisteredRegion(t *testing.T) {
	r := New(16)

	err := r.Send(999, raftstore.PeerMsg{})
	assert.ErrorIs(t, err, ErrRegionNotFound)
}

func TestRegisterDuplicate(t *testing.T) {
	r := New(16)

	ch := make(chan raftstore.PeerMsg, 1)
	require.NoError(t, r.Register(1, ch))

	err := r.Register(1, ch)
	assert.ErrorIs(t, err, ErrPeerAlreadyRegistered)
}

func TestUnregister(t *testing.T) {
	r := New(16)

	ch := make(chan raftstore.PeerMsg, 1)
	require.NoError(t, r.Register(1, ch))
	assert.True(t, r.HasRegion(1))

	r.Unregister(1)
	assert.False(t, r.HasRegion(1))

	err := r.Send(1, raftstore.PeerMsg{})
	assert.ErrorIs(t, err, ErrRegionNotFound)
}

func TestMailboxFull(t *testing.T) {
	r := New(16)

	ch := make(chan raftstore.PeerMsg, 1) // capacity 1
	require.NoError(t, r.Register(1, ch))

	// First send should succeed.
	require.NoError(t, r.Send(1, raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick}))

	// Second send should fail (mailbox full, non-blocking).
	err := r.Send(1, raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick})
	assert.ErrorIs(t, err, ErrMailboxFull)
}

func TestBroadcast(t *testing.T) {
	r := New(16)

	const numRegions = 5
	channels := make([]chan raftstore.PeerMsg, numRegions)
	for i := 0; i < numRegions; i++ {
		channels[i] = make(chan raftstore.PeerMsg, DefaultMailboxCapacity)
		require.NoError(t, r.Register(uint64(i+1), channels[i]))
	}

	msg := raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick, Data: raftstore.PeerTickRaft}
	r.Broadcast(msg)

	for i, ch := range channels {
		got := <-ch
		assert.Equal(t, raftstore.PeerMsgTypeTick, got.Type, "region %d should receive broadcast", i+1)
	}
}

func TestBroadcastDropsFullMailboxes(t *testing.T) {
	r := New(16)

	chFull := make(chan raftstore.PeerMsg, 1)
	chOpen := make(chan raftstore.PeerMsg, DefaultMailboxCapacity)

	require.NoError(t, r.Register(1, chFull))
	require.NoError(t, r.Register(2, chOpen))

	// Fill up region 1's mailbox.
	chFull <- raftstore.PeerMsg{}

	// Broadcast should not block despite region 1 being full.
	msg := raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick}
	r.Broadcast(msg)

	// Region 2 should still receive the message.
	got := <-chOpen
	assert.Equal(t, raftstore.PeerMsgTypeTick, got.Type)
}

func TestSendStore(t *testing.T) {
	r := New(16)

	msg := raftstore.StoreMsg{Type: raftstore.StoreMsgTypeTick}
	require.NoError(t, r.SendStore(msg))

	got := <-r.StoreCh()
	assert.Equal(t, raftstore.StoreMsgTypeTick, got.Type)
}

func TestSendStoreMailboxFull(t *testing.T) {
	r := New(1) // capacity 1

	require.NoError(t, r.SendStore(raftstore.StoreMsg{}))
	err := r.SendStore(raftstore.StoreMsg{})
	assert.ErrorIs(t, err, ErrMailboxFull)
}

func TestRegionCount(t *testing.T) {
	r := New(16)

	assert.Equal(t, 0, r.RegionCount())

	for i := uint64(1); i <= 5; i++ {
		ch := make(chan raftstore.PeerMsg, 1)
		require.NoError(t, r.Register(i, ch))
	}
	assert.Equal(t, 5, r.RegionCount())

	r.Unregister(3)
	assert.Equal(t, 4, r.RegionCount())
}

func TestHasRegion(t *testing.T) {
	r := New(16)

	assert.False(t, r.HasRegion(1))

	ch := make(chan raftstore.PeerMsg, 1)
	require.NoError(t, r.Register(1, ch))
	assert.True(t, r.HasRegion(1))
}

func TestGetMailbox(t *testing.T) {
	r := New(16)

	assert.Nil(t, r.GetMailbox(1))

	ch := make(chan raftstore.PeerMsg, 1)
	require.NoError(t, r.Register(1, ch))

	got := r.GetMailbox(1)
	assert.NotNil(t, got)

	// Should be the same channel.
	got <- raftstore.PeerMsg{Type: raftstore.PeerMsgTypeStart}
	recv := <-ch
	assert.Equal(t, raftstore.PeerMsgTypeStart, recv.Type)
}

func TestConcurrentSend(t *testing.T) {
	r := New(16)

	ch := make(chan raftstore.PeerMsg, 10000)
	require.NoError(t, r.Register(1, ch))

	const goroutines = 10
	const perGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				err := r.Send(1, raftstore.PeerMsg{Type: raftstore.PeerMsgTypeTick})
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, goroutines*perGoroutine, len(ch))
}

func TestConcurrentRegisterUnregister(t *testing.T) {
	r := New(16)

	var wg sync.WaitGroup
	const ops = 100

	// Concurrent register/unregister should not panic.
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := uint64(0); i < ops; i++ {
			ch := make(chan raftstore.PeerMsg, 1)
			r.Register(i, ch)
		}
	}()
	go func() {
		defer wg.Done()
		for i := uint64(0); i < ops; i++ {
			r.Unregister(i)
		}
	}()
	wg.Wait()
}
