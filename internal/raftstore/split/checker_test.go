package split

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestEngine(t *testing.T) traits.KvEngine {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(filepath.Join(dir, "split-test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	return eng
}

func TestScanRegionSize_Empty(t *testing.T) {
	eng := newTestEngine(t)
	w := NewSplitCheckWorker(eng, DefaultSplitCheckWorkerConfig())

	size, splitKey, err := w.scanRegionSize(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), size)
	assert.Nil(t, splitKey)
}

func TestScanRegionSize_BelowThreshold(t *testing.T) {
	eng := newTestEngine(t)

	// Write a small amount of data.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := []byte(fmt.Sprintf("value%04d", i))
		require.NoError(t, eng.Put(cfnames.CFDefault, key, value))
	}

	cfg := SplitCheckWorkerConfig{
		SplitSize: 1024 * 1024, // 1 MiB
		MaxSize:   2 * 1024 * 1024,
	}
	w := NewSplitCheckWorker(eng, cfg)

	size, splitKey, err := w.scanRegionSize(nil, nil)
	require.NoError(t, err)
	assert.True(t, size > 0 && size < cfg.SplitSize)
	_ = splitKey // may or may not be set depending on size vs halfSize
}

func TestScanRegionSize_AboveThreshold(t *testing.T) {
	eng := newTestEngine(t)

	// Write enough data to exceed the split size.
	largeValue := make([]byte, 1024) // 1 KB per entry
	for i := 0; i < 200; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		require.NoError(t, eng.Put(cfnames.CFDefault, key, largeValue))
	}

	cfg := SplitCheckWorkerConfig{
		SplitSize: 100 * 1024, // 100 KiB
		MaxSize:   200 * 1024,
	}
	w := NewSplitCheckWorker(eng, cfg)

	size, splitKey, err := w.scanRegionSize(nil, nil)
	require.NoError(t, err)
	assert.True(t, size >= cfg.SplitSize, "size %d should exceed split size %d", size, cfg.SplitSize)
	assert.NotNil(t, splitKey, "split key should be set when size exceeds threshold")
}

func TestScanRegionSize_WithBounds(t *testing.T) {
	eng := newTestEngine(t)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := make([]byte, 100)
		require.NoError(t, eng.Put(cfnames.CFDefault, key, value))
	}

	cfg := SplitCheckWorkerConfig{
		SplitSize: 5000,
		MaxSize:   10000,
	}
	w := NewSplitCheckWorker(eng, cfg)

	// Scan only a sub-range.
	size, _, err := w.scanRegionSize([]byte("key0020"), []byte("key0040"))
	require.NoError(t, err)
	assert.True(t, size > 0)
}

// --- ExecBatchSplit tests ---

func TestExecBatchSplit_SingleSplit(t *testing.T) {
	region := &metapb.Region{
		Id:       1,
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 2, StoreId: 2},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	result, err := ExecBatchSplit(
		region,
		[][]byte{[]byte("m")},
		[]uint64{100},
		[][]uint64{{101, 102}},
	)
	require.NoError(t, err)

	// Derived region: [a, m)
	assert.Equal(t, []byte("a"), result.Derived.StartKey)
	assert.Equal(t, []byte("m"), result.Derived.EndKey)
	assert.Equal(t, uint64(2), result.Derived.RegionEpoch.Version) // 1 + 1

	// New region: [m, z)
	require.Len(t, result.Regions, 1)
	assert.Equal(t, uint64(100), result.Regions[0].Id)
	assert.Equal(t, []byte("m"), result.Regions[0].StartKey)
	assert.Equal(t, []byte("z"), result.Regions[0].EndKey)
	assert.Len(t, result.Regions[0].Peers, 2)
}

func TestExecBatchSplit_MultiSplit(t *testing.T) {
	region := &metapb.Region{
		Id:       1,
		StartKey: nil,
		EndKey:   nil,
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	result, err := ExecBatchSplit(
		region,
		[][]byte{[]byte("d"), []byte("h"), []byte("m")},
		[]uint64{10, 11, 12},
		[][]uint64{{20}, {21}, {22}},
	)
	require.NoError(t, err)

	// Derived: [nil, d)
	assert.Nil(t, result.Derived.StartKey)
	assert.Equal(t, []byte("d"), result.Derived.EndKey)
	assert.Equal(t, uint64(4), result.Derived.RegionEpoch.Version) // 1 + 3

	// 3 new regions
	require.Len(t, result.Regions, 3)

	// Region 10: [d, h)
	assert.Equal(t, []byte("d"), result.Regions[0].StartKey)
	assert.Equal(t, []byte("h"), result.Regions[0].EndKey)

	// Region 11: [h, m)
	assert.Equal(t, []byte("h"), result.Regions[1].StartKey)
	assert.Equal(t, []byte("m"), result.Regions[1].EndKey)

	// Region 12: [m, nil)
	assert.Equal(t, []byte("m"), result.Regions[2].StartKey)
	assert.Nil(t, result.Regions[2].EndKey)
}

func TestExecBatchSplit_InvalidKeys(t *testing.T) {
	region := &metapb.Region{
		Id:       1,
		StartKey: []byte("b"),
		EndKey:   []byte("y"),
		Peers:    []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}

	// Key before start.
	_, err := ExecBatchSplit(region, [][]byte{[]byte("a")}, []uint64{10}, [][]uint64{{20}})
	assert.Error(t, err)

	// Key after end.
	_, err = ExecBatchSplit(region, [][]byte{[]byte("z")}, []uint64{10}, [][]uint64{{20}})
	assert.Error(t, err)

	// Non-ascending keys.
	_, err = ExecBatchSplit(region, [][]byte{[]byte("m"), []byte("g")}, []uint64{10, 11}, [][]uint64{{20}, {21}})
	assert.Error(t, err)
}

func TestExecBatchSplit_NoKeys(t *testing.T) {
	region := &metapb.Region{Id: 1}
	_, err := ExecBatchSplit(region, nil, nil, nil)
	assert.Error(t, err)
}

func TestExecBatchSplit_EpochIncrement(t *testing.T) {
	region := &metapb.Region{
		Id:          1,
		StartKey:    []byte("a"),
		EndKey:      []byte("z"),
		Peers:       []*metapb.Peer{{Id: 1, StoreId: 1}},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 3, Version: 5},
	}

	result, err := ExecBatchSplit(region, [][]byte{[]byte("m")}, []uint64{10}, [][]uint64{{20}})
	require.NoError(t, err)

	assert.Equal(t, uint64(3), result.Derived.RegionEpoch.ConfVer) // unchanged
	assert.Equal(t, uint64(6), result.Derived.RegionEpoch.Version) // 5 + 1
}

func TestSplitCheckWorker_TaskAndResult(t *testing.T) {
	eng := newTestEngine(t)

	// Write enough data.
	largeValue := make([]byte, 512)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		require.NoError(t, eng.Put(cfnames.CFDefault, key, largeValue))
	}

	cfg := SplitCheckWorkerConfig{
		SplitSize: 20 * 1024,
		MaxSize:   40 * 1024,
	}
	w := NewSplitCheckWorker(eng, cfg)
	go w.Run()
	defer w.Stop()

	w.Schedule(SplitCheckTask{
		RegionID: 1,
		Region:   &metapb.Region{Id: 1},
	})

	result := <-w.ResultCh()
	assert.Equal(t, uint64(1), result.RegionID)
	assert.True(t, result.RegionSize > 0)
	assert.NotNil(t, result.SplitKey, "expected split key for data exceeding threshold")
}

func TestSplitKeyAtMidpoint(t *testing.T) {
	eng := newTestEngine(t)

	// Write data with known sizes.
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := make([]byte, 100) // 100 bytes per value
		require.NoError(t, eng.Put(cfnames.CFDefault, key, value))
	}

	cfg := SplitCheckWorkerConfig{
		SplitSize: 5000,
		MaxSize:   10000,
	}
	w := NewSplitCheckWorker(eng, cfg)

	_, splitKey, err := w.scanRegionSize(nil, nil)
	require.NoError(t, err)
	require.NotNil(t, splitKey)

	// The split key should be roughly in the middle of the key range.
	assert.True(t, string(splitKey) > "key0020" && string(splitKey) < "key0080",
		"split key %q should be near the midpoint", splitKey)
}
