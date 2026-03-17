package keys

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataKey(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{}, []byte{0x7A}},
		{[]byte("hello"), []byte{0x7A, 'h', 'e', 'l', 'l', 'o'}},
		{[]byte{0x00, 0xFF}, []byte{0x7A, 0x00, 0xFF}},
	}

	for _, c := range cases {
		result := DataKey(c.input)
		assert.Equal(t, c.expected, result)
	}
}

func TestOriginKey(t *testing.T) {
	// Round-trip
	original := []byte("test-key")
	dataKey := DataKey(original)
	recovered := OriginKey(dataKey)
	assert.Equal(t, original, recovered)

	// Non-data key passes through
	localKey := []byte{0x01, 0x02}
	assert.Equal(t, localKey, OriginKey(localKey))
}

func TestRaftLogKey(t *testing.T) {
	key := RaftLogKey(1, 100)
	assert.Len(t, key, 19)
	assert.Equal(t, LocalPrefix, key[0])
	assert.Equal(t, RegionRaftPrefix, key[1])

	// Check regionID
	regionID := binary.BigEndian.Uint64(key[2:10])
	assert.Equal(t, uint64(1), regionID)

	// Check suffix
	assert.Equal(t, RaftLogSuffix, key[10])

	// Check logIndex
	logIndex := binary.BigEndian.Uint64(key[11:19])
	assert.Equal(t, uint64(100), logIndex)
}

func TestRaftLogKeyOrdering(t *testing.T) {
	// Same region, different log indices should be ordered
	key1 := RaftLogKey(1, 1)
	key2 := RaftLogKey(1, 2)
	key3 := RaftLogKey(1, 100)
	assert.True(t, bytes.Compare(key1, key2) < 0)
	assert.True(t, bytes.Compare(key2, key3) < 0)

	// Different regions should be ordered by region ID
	key4 := RaftLogKey(1, 100)
	key5 := RaftLogKey(2, 1)
	assert.True(t, bytes.Compare(key4, key5) < 0)
}

func TestRaftStateKey(t *testing.T) {
	key := RaftStateKey(42)
	assert.Len(t, key, 11)
	assert.Equal(t, LocalPrefix, key[0])
	assert.Equal(t, RegionRaftPrefix, key[1])
	assert.Equal(t, RaftStateSuffix, key[10])

	regionID := binary.BigEndian.Uint64(key[2:10])
	assert.Equal(t, uint64(42), regionID)
}

func TestApplyStateKey(t *testing.T) {
	key := ApplyStateKey(42)
	assert.Len(t, key, 11)
	assert.Equal(t, LocalPrefix, key[0])
	assert.Equal(t, RegionRaftPrefix, key[1])
	assert.Equal(t, ApplyStateSuffix, key[10])

	regionID := binary.BigEndian.Uint64(key[2:10])
	assert.Equal(t, uint64(42), regionID)
}

func TestRegionStateKey(t *testing.T) {
	key := RegionStateKey(42)
	assert.Len(t, key, 11)
	assert.Equal(t, LocalPrefix, key[0])
	assert.Equal(t, RegionMetaPrefix, key[1])
	assert.Equal(t, RegionStateSuffix, key[10])

	regionID := binary.BigEndian.Uint64(key[2:10])
	assert.Equal(t, uint64(42), regionID)
}

func TestRaftLogKeyRange(t *testing.T) {
	start, end := RaftLogKeyRange(1)

	// Start should be the first raft log key for region 1
	assert.Equal(t, RaftLogKey(1, 0), start)

	// End should be the raft state key (suffix 0x02 > 0x01)
	assert.Equal(t, RaftStateKey(1), end)

	// Any raft log key should be in range
	for _, idx := range []uint64{0, 1, 100, ^uint64(0)} {
		logKey := RaftLogKey(1, idx)
		assert.True(t, bytes.Compare(logKey, start) >= 0,
			"log key should be >= start")
		assert.True(t, bytes.Compare(logKey, end) < 0,
			"log key should be < end")
	}
}

func TestLocalKeyOrdering(t *testing.T) {
	// All local keys should be < data keys
	localKey := RaftStateKey(1)
	dataKey := DataKey([]byte("any"))
	assert.True(t, bytes.Compare(localKey, dataKey) < 0,
		"local keys should sort before data keys")
}

func TestStoreIdentKey(t *testing.T) {
	assert.Equal(t, []byte{0x01, 0x01}, StoreIdentKey)
	assert.Len(t, StoreIdentKey, 2)
}

func TestPrepareBootstrapKey(t *testing.T) {
	assert.Equal(t, []byte{0x01, 0x02}, PrepareBootstrapKey)
}

func TestIsDataKey(t *testing.T) {
	assert.True(t, IsDataKey(DataKey([]byte("test"))))
	assert.False(t, IsDataKey(RaftStateKey(1)))
	assert.False(t, IsDataKey([]byte{}))
}

func TestIsLocalKey(t *testing.T) {
	assert.True(t, IsLocalKey(RaftStateKey(1)))
	assert.True(t, IsLocalKey(StoreIdentKey))
	assert.False(t, IsLocalKey(DataKey([]byte("test"))))
	assert.False(t, IsLocalKey([]byte{}))
}

func TestRegionIDFromRaftKey(t *testing.T) {
	key := RaftLogKey(42, 100)
	id, err := RegionIDFromRaftKey(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), id)

	key = RaftStateKey(99)
	id, err = RegionIDFromRaftKey(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), id)

	// Too short
	_, err = RegionIDFromRaftKey([]byte{0x01, 0x02})
	assert.Error(t, err)

	// Wrong prefix
	_, err = RegionIDFromRaftKey(DataKey([]byte("test")))
	assert.Error(t, err)
}

func TestRegionIDFromMetaKey(t *testing.T) {
	key := RegionStateKey(42)
	id, err := RegionIDFromMetaKey(key)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), id)

	_, err = RegionIDFromMetaKey(RaftStateKey(1))
	assert.Error(t, err)
}

func TestSuffixOrdering(t *testing.T) {
	regionID := uint64(1)

	// For same region: raft log < raft state < apply state
	raftLog := RaftLogKey(regionID, 0)
	raftState := RaftStateKey(regionID)
	applyState := ApplyStateKey(regionID)

	assert.True(t, bytes.Compare(raftLog, raftState) < 0)
	assert.True(t, bytes.Compare(raftState, applyState) < 0)

	// Region state is in a different prefix, so it sorts differently
	regionState := RegionStateKey(regionID)
	// RegionMetaPrefix (0x03) > RegionRaftPrefix (0x02)
	assert.True(t, bytes.Compare(applyState, regionState) < 0)
}
