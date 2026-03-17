// Package router provides sync.Map-based message routing for raftstore.
// It maps region IDs to peer goroutine mailboxes (buffered channels),
// replacing TiKV's DashMap-based Router.
package router

import (
	"errors"
	"sync"

	"github.com/ryogrid/gookvs/internal/raftstore"
)

var (
	// ErrRegionNotFound is returned when a region is not registered in the router.
	ErrRegionNotFound = errors.New("raftstore: region not found")
	// ErrMailboxFull is returned when a peer's mailbox channel is full.
	ErrMailboxFull = errors.New("raftstore: peer mailbox full")
	// ErrPeerAlreadyRegistered is returned when trying to register a peer that already exists.
	ErrPeerAlreadyRegistered = errors.New("raftstore: peer already registered")
)

// DefaultMailboxCapacity is the default buffer size for peer mailbox channels.
const DefaultMailboxCapacity = 256

// Router routes messages to peer goroutines by region ID.
type Router struct {
	// peers maps regionID -> chan<- PeerMsg
	peers sync.Map

	// storeCh is the channel for store-level messages.
	storeCh chan raftstore.StoreMsg
}

// New creates a new Router with the given store channel capacity.
func New(storeChCap int) *Router {
	return &Router{
		storeCh: make(chan raftstore.StoreMsg, storeChCap),
	}
}

// Register associates a region ID with a peer mailbox channel.
// Returns ErrPeerAlreadyRegistered if the region is already registered.
func (r *Router) Register(regionID uint64, ch chan raftstore.PeerMsg) error {
	_, loaded := r.peers.LoadOrStore(regionID, ch)
	if loaded {
		return ErrPeerAlreadyRegistered
	}
	return nil
}

// Unregister removes a region from the router.
// The caller is responsible for closing the channel after unregistering.
func (r *Router) Unregister(regionID uint64) {
	r.peers.Delete(regionID)
}

// Send delivers a message to the peer goroutine for the given region.
// Returns ErrRegionNotFound if the region is not registered.
// Returns ErrMailboxFull if the channel is full (non-blocking send).
func (r *Router) Send(regionID uint64, msg raftstore.PeerMsg) error {
	v, ok := r.peers.Load(regionID)
	if !ok {
		return ErrRegionNotFound
	}
	ch := v.(chan raftstore.PeerMsg)
	select {
	case ch <- msg:
		return nil
	default:
		return ErrMailboxFull
	}
}

// SendStore delivers a message to the store goroutine.
// Returns ErrMailboxFull if the store channel is full.
func (r *Router) SendStore(msg raftstore.StoreMsg) error {
	select {
	case r.storeCh <- msg:
		return nil
	default:
		return ErrMailboxFull
	}
}

// StoreCh returns the store message channel for consumption by the store goroutine.
func (r *Router) StoreCh() <-chan raftstore.StoreMsg {
	return r.storeCh
}

// Broadcast sends a message to all registered peer mailboxes.
// Silently drops messages for full mailboxes.
func (r *Router) Broadcast(msg raftstore.PeerMsg) {
	r.peers.Range(func(key, value interface{}) bool {
		ch := value.(chan raftstore.PeerMsg)
		select {
		case ch <- msg:
		default:
			// Drop if full — broadcast messages are best-effort.
		}
		return true
	})
}

// HasRegion returns true if the region is registered in the router.
func (r *Router) HasRegion(regionID uint64) bool {
	_, ok := r.peers.Load(regionID)
	return ok
}

// RegionCount returns the number of registered regions.
// Note: this iterates the sync.Map, so it's O(n).
func (r *Router) RegionCount() int {
	count := 0
	r.peers.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// GetMailbox returns the mailbox channel for a region, or nil if not found.
func (r *Router) GetMailbox(regionID uint64) chan raftstore.PeerMsg {
	v, ok := r.peers.Load(regionID)
	if !ok {
		return nil
	}
	return v.(chan raftstore.PeerMsg)
}
