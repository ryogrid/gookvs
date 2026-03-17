// Package keys provides functions for constructing internal key encodings.
// These key formats are byte-identical to TiKV's for storage compatibility.
package keys

import (
	"encoding/binary"

	"github.com/ryogrid/gookvs/pkg/codec"
)

const (
	// LocalPrefix is the prefix for internal metadata keys.
	LocalPrefix = byte(0x01)
	// DataPrefix is the prefix for user data keys.
	DataPrefix = byte(0x7A) // 'z'

	// RegionRaftPrefix is the category prefix for Raft logs and hard state.
	RegionRaftPrefix = byte(0x02)
	// RegionMetaPrefix is the category prefix for region metadata.
	RegionMetaPrefix = byte(0x03)

	// RaftLogSuffix is the suffix for Raft log entries.
	RaftLogSuffix = byte(0x01)
	// RaftStateSuffix is the suffix for Raft hard state.
	RaftStateSuffix = byte(0x02)
	// ApplyStateSuffix is the suffix for apply state.
	ApplyStateSuffix = byte(0x03)

	// RegionStateSuffix is the suffix for region state.
	RegionStateSuffix = byte(0x01)

	// StoreIdentKeySuffix is the global suffix for store identity.
	StoreIdentKeySuffix = byte(0x01)
	// PrepareBootstrapKeySuffix is the global suffix for bootstrap preparation marker.
	PrepareBootstrapKeySuffix = byte(0x02)
)

var (
	// StoreIdentKey is the key for store identity.
	StoreIdentKey = []byte{LocalPrefix, StoreIdentKeySuffix}
	// PrepareBootstrapKey is the key for bootstrap preparation marker.
	PrepareBootstrapKey = []byte{LocalPrefix, PrepareBootstrapKeySuffix}

	// LocalMinKey is the minimum local key.
	LocalMinKey = []byte{LocalPrefix}
	// LocalMaxKey is the maximum local key (exclusive upper bound).
	LocalMaxKey = []byte{LocalPrefix + 1}

	// DataMinKey is the minimum data key.
	DataMinKey = []byte{DataPrefix}
	// DataMaxKey is the maximum data key (exclusive upper bound).
	DataMaxKey = []byte{DataPrefix + 1}
)

// DataKey prepends DataPrefix to a user key for storage in data CFs.
func DataKey(key []byte) []byte {
	result := make([]byte, 1+len(key))
	result[0] = DataPrefix
	copy(result[1:], key)
	return result
}

// OriginKey strips DataPrefix from a data key to recover the user key.
// Returns the original key if it doesn't start with DataPrefix.
func OriginKey(dataKey []byte) []byte {
	if len(dataKey) > 0 && dataKey[0] == DataPrefix {
		return dataKey[1:]
	}
	return dataKey
}

// RaftLogKey builds the local key for a Raft log entry.
// Format: [LocalPrefix][RegionRaftPrefix][regionID: 8B BE][RaftLogSuffix][logIndex: 8B BE]
func RaftLogKey(regionID uint64, logIndex uint64) []byte {
	key := make([]byte, 0, 19) // 1 + 1 + 8 + 1 + 8
	key = append(key, LocalPrefix, RegionRaftPrefix)
	key = appendUint64BE(key, regionID)
	key = append(key, RaftLogSuffix)
	key = appendUint64BE(key, logIndex)
	return key
}

// RaftStateKey builds the local key for a region's Raft hard state.
// Format: [LocalPrefix][RegionRaftPrefix][regionID: 8B BE][RaftStateSuffix]
func RaftStateKey(regionID uint64) []byte {
	key := make([]byte, 0, 11) // 1 + 1 + 8 + 1
	key = append(key, LocalPrefix, RegionRaftPrefix)
	key = appendUint64BE(key, regionID)
	key = append(key, RaftStateSuffix)
	return key
}

// ApplyStateKey builds the local key for a region's apply state.
// Format: [LocalPrefix][RegionRaftPrefix][regionID: 8B BE][ApplyStateSuffix]
func ApplyStateKey(regionID uint64) []byte {
	key := make([]byte, 0, 11)
	key = append(key, LocalPrefix, RegionRaftPrefix)
	key = appendUint64BE(key, regionID)
	key = append(key, ApplyStateSuffix)
	return key
}

// RegionStateKey builds the local key for a region's metadata.
// Format: [LocalPrefix][RegionMetaPrefix][regionID: 8B BE][RegionStateSuffix]
func RegionStateKey(regionID uint64) []byte {
	key := make([]byte, 0, 11)
	key = append(key, LocalPrefix, RegionMetaPrefix)
	key = appendUint64BE(key, regionID)
	key = append(key, RegionStateSuffix)
	return key
}

// RaftLogKeyRange returns the range [start, end) for all Raft log entries of a region.
func RaftLogKeyRange(regionID uint64) ([]byte, []byte) {
	start := RaftLogKey(regionID, 0)
	end := RaftStateKey(regionID) // RaftStateSuffix (0x02) > RaftLogSuffix (0x01)
	return start, end
}

// RegionIDFromRaftKey extracts the region ID from a raft-prefixed local key.
func RegionIDFromRaftKey(key []byte) (uint64, error) {
	if len(key) < 10 {
		return 0, codec.ErrInsufficientData
	}
	if key[0] != LocalPrefix || key[1] != RegionRaftPrefix {
		return 0, codec.ErrInvalidEncodedKey
	}
	return binary.BigEndian.Uint64(key[2:10]), nil
}

// RegionIDFromMetaKey extracts the region ID from a meta-prefixed local key.
func RegionIDFromMetaKey(key []byte) (uint64, error) {
	if len(key) < 10 {
		return 0, codec.ErrInsufficientData
	}
	if key[0] != LocalPrefix || key[1] != RegionMetaPrefix {
		return 0, codec.ErrInvalidEncodedKey
	}
	return binary.BigEndian.Uint64(key[2:10]), nil
}

// IsDataKey returns true if the key is a data key (starts with DataPrefix).
func IsDataKey(key []byte) bool {
	return len(key) > 0 && key[0] == DataPrefix
}

// IsLocalKey returns true if the key is a local key (starts with LocalPrefix).
func IsLocalKey(key []byte) bool {
	return len(key) > 0 && key[0] == LocalPrefix
}

// appendUint64BE appends a uint64 in big-endian format.
func appendUint64BE(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}
