// Package split implements region split checking and execution.
package split

import (
	"bytes"
	"fmt"
	"log/slog"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
)

// CheckPolicy controls how the split checker estimates region size.
type CheckPolicy int

const (
	// CheckPolicyScan iterates all keys to compute exact size.
	CheckPolicyScan CheckPolicy = iota
	// CheckPolicyApproximate uses engine properties for estimation.
	CheckPolicyApproximate
)

// SplitCheckTask is sent from the peer to the split check worker.
type SplitCheckTask struct {
	RegionID uint64
	Region   *metapb.Region
	StartKey []byte
	EndKey   []byte
	Policy   CheckPolicy
}

// SplitCheckResult is sent back from the worker to the peer.
type SplitCheckResult struct {
	RegionID   uint64
	SplitKey   []byte // nil if no split needed
	RegionSize uint64
}

// SplitCheckWorkerConfig holds configuration for the split check worker.
type SplitCheckWorkerConfig struct {
	SplitSize uint64 // Region size threshold (bytes) to trigger split
	MaxSize   uint64 // Region hard limit (bytes)
	SplitKeys uint64 // Key count threshold (unused for now)
	MaxKeys   uint64 // Key count hard limit (unused for now)
}

// DefaultSplitCheckWorkerConfig returns sensible defaults.
func DefaultSplitCheckWorkerConfig() SplitCheckWorkerConfig {
	return SplitCheckWorkerConfig{
		SplitSize: 96 * 1024 * 1024,  // 96 MiB
		MaxSize:   144 * 1024 * 1024,  // 144 MiB
		SplitKeys: 960000,
		MaxKeys:   1440000,
	}
}

// SplitCheckWorker runs split checks in a background goroutine.
type SplitCheckWorker struct {
	engine   traits.KvEngine
	cfg      SplitCheckWorkerConfig
	taskCh   chan SplitCheckTask
	resultCh chan SplitCheckResult
	stopCh   chan struct{}
}

// NewSplitCheckWorker creates a new SplitCheckWorker.
func NewSplitCheckWorker(engine traits.KvEngine, cfg SplitCheckWorkerConfig) *SplitCheckWorker {
	return &SplitCheckWorker{
		engine:   engine,
		cfg:      cfg,
		taskCh:   make(chan SplitCheckTask, 16),
		resultCh: make(chan SplitCheckResult, 16),
		stopCh:   make(chan struct{}),
	}
}

// Run starts the worker's main loop.
func (w *SplitCheckWorker) Run() {
	for {
		select {
		case <-w.stopCh:
			return
		case task := <-w.taskCh:
			w.checkRegion(task)
		}
	}
}

// Stop stops the worker.
func (w *SplitCheckWorker) Stop() {
	close(w.stopCh)
}

// Schedule sends a split check task to the worker.
func (w *SplitCheckWorker) Schedule(task SplitCheckTask) {
	select {
	case w.taskCh <- task:
	default:
		slog.Debug("split check worker busy, dropping task", "region", task.RegionID)
	}
}

// ResultCh returns the channel for receiving split check results.
func (w *SplitCheckWorker) ResultCh() <-chan SplitCheckResult {
	return w.resultCh
}

func (w *SplitCheckWorker) checkRegion(task SplitCheckTask) {
	size, splitKey, err := w.scanRegionSize(task.StartKey, task.EndKey)
	if err != nil {
		slog.Warn("split check scan failed", "region", task.RegionID, "err", err)
		return
	}

	result := SplitCheckResult{
		RegionID:   task.RegionID,
		RegionSize: size,
	}

	if size >= w.cfg.SplitSize && splitKey != nil {
		result.SplitKey = splitKey
	}

	select {
	case w.resultCh <- result:
	default:
		slog.Debug("split check result channel full", "region", task.RegionID)
	}
}

// scanRegionSize iterates all CFs in the key range and computes total size.
// Returns the total size and a candidate split key (approximately the midpoint).
func (w *SplitCheckWorker) scanRegionSize(startKey, endKey []byte) (uint64, []byte, error) {
	var totalSize uint64
	var splitKey []byte
	halfSize := w.cfg.SplitSize / 2

	// Scan each data CF.
	for _, cf := range []string{cfnames.CFDefault, cfnames.CFWrite, cfnames.CFLock} {
		opts := traits.IterOptions{
			LowerBound: startKey,
		}
		if len(endKey) > 0 {
			opts.UpperBound = endKey
		}

		iter := w.engine.NewIterator(cf, opts)

		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()
			entrySize := uint64(len(key) + len(value))
			totalSize += entrySize

			// Record the split key at approximately the midpoint.
			if splitKey == nil && totalSize >= halfSize {
				splitKey = append([]byte(nil), key...)
			}

			// Early exit if we exceed max size.
			if totalSize >= w.cfg.MaxSize {
				iter.Close()
				return totalSize, splitKey, nil
			}
		}

		if err := iter.Error(); err != nil {
			iter.Close()
			return 0, nil, fmt.Errorf("split: scan %s: %w", cf, err)
		}
		iter.Close()
	}

	return totalSize, splitKey, nil
}

// --- Split Execution ---

// SplitRegionResult contains the result of a region split operation.
type SplitRegionResult struct {
	Derived *metapb.Region   // The parent region after split (left half)
	Regions []*metapb.Region // New regions created by split (right half)
}

// ExecBatchSplit executes a batch split operation, creating new regions.
// splitKeys are the boundary keys for the new regions.
// newRegionIDs and newPeerIDs are pre-allocated by PD.
func ExecBatchSplit(
	region *metapb.Region,
	splitKeys [][]byte,
	newRegionIDs []uint64,
	newPeerIDSets [][]uint64,
) (*SplitRegionResult, error) {
	if len(splitKeys) == 0 {
		return nil, fmt.Errorf("split: no split keys provided")
	}
	if len(splitKeys) != len(newRegionIDs) {
		return nil, fmt.Errorf("split: split keys count (%d) != new region IDs count (%d)", len(splitKeys), len(newRegionIDs))
	}

	// Validate split keys are within region range and ascending.
	for i, key := range splitKeys {
		if len(region.GetStartKey()) > 0 && bytes.Compare(key, region.GetStartKey()) <= 0 {
			return nil, fmt.Errorf("split: key %x <= region start key %x", key, region.GetStartKey())
		}
		if len(region.GetEndKey()) > 0 && bytes.Compare(key, region.GetEndKey()) >= 0 {
			return nil, fmt.Errorf("split: key %x >= region end key %x", key, region.GetEndKey())
		}
		if i > 0 && bytes.Compare(splitKeys[i-1], key) >= 0 {
			return nil, fmt.Errorf("split: keys not ascending: %x >= %x", splitKeys[i-1], key)
		}
	}

	// Clone the derived (parent) region.
	derived := cloneRegionForSplit(region)
	if derived.RegionEpoch == nil {
		derived.RegionEpoch = &metapb.RegionEpoch{}
	}
	derived.RegionEpoch.Version += uint64(len(splitKeys))

	// The derived region keeps the left half: [original.StartKey, splitKeys[0])
	derived.EndKey = append([]byte(nil), splitKeys[0]...)

	// Create new regions for each split.
	var newRegions []*metapb.Region
	for i, splitKey := range splitKeys {
		newRegion := &metapb.Region{
			Id: newRegionIDs[i],
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: derived.RegionEpoch.ConfVer,
				Version: derived.RegionEpoch.Version,
			},
		}

		// Key range: [splitKeys[i], splitKeys[i+1]) or [splitKeys[i], original.EndKey)
		newRegion.StartKey = append([]byte(nil), splitKey...)
		if i+1 < len(splitKeys) {
			newRegion.EndKey = append([]byte(nil), splitKeys[i+1]...)
		} else {
			newRegion.EndKey = append([]byte(nil), region.GetEndKey()...)
		}

		// Create peers for the new region using allocated IDs.
		if i < len(newPeerIDSets) {
			for j, peerID := range newPeerIDSets[i] {
				if j < len(region.GetPeers()) {
					newRegion.Peers = append(newRegion.Peers, &metapb.Peer{
						Id:      peerID,
						StoreId: region.GetPeers()[j].GetStoreId(),
					})
				}
			}
		}

		newRegions = append(newRegions, newRegion)
	}

	return &SplitRegionResult{
		Derived: derived,
		Regions: newRegions,
	}, nil
}

func cloneRegionForSplit(r *metapb.Region) *metapb.Region {
	if r == nil {
		return nil
	}
	clone := &metapb.Region{
		Id:       r.Id,
		StartKey: append([]byte(nil), r.StartKey...),
		EndKey:   append([]byte(nil), r.EndKey...),
	}
	if r.RegionEpoch != nil {
		clone.RegionEpoch = &metapb.RegionEpoch{
			ConfVer: r.RegionEpoch.ConfVer,
			Version: r.RegionEpoch.Version,
		}
	}
	for _, p := range r.Peers {
		clone.Peers = append(clone.Peers, &metapb.Peer{
			Id:      p.Id,
			StoreId: p.StoreId,
			Role:    p.Role,
		})
	}
	return clone
}
