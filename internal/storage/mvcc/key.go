// Package mvcc implements Multi-Version Concurrency Control for gookvs.
// It provides MvccTxn (write accumulator), MvccReader, PointGetter, and Scanner.
package mvcc

import (
	"github.com/ryogrid/gookvs/pkg/codec"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// Key represents a user key (unencoded).
type Key = []byte

// EncodeKey encodes a user key with a timestamp for storage in CF_WRITE or CF_DEFAULT.
// The timestamp is encoded in descending order so that newer versions sort first.
// Format: encoded_user_key + ts_desc
func EncodeKey(key Key, ts txntypes.TimeStamp) []byte {
	encoded := codec.EncodeBytes(nil, key)
	return codec.EncodeUint64Desc(encoded, uint64(ts))
}

// DecodeKey decodes an encoded MVCC key, returning the user key and timestamp.
func DecodeKey(encodedKey []byte) (Key, txntypes.TimeStamp, error) {
	key, rest, err := codec.DecodeBytes(encodedKey)
	if err != nil {
		return nil, 0, err
	}
	if len(rest) < 8 {
		return key, 0, nil // Key without timestamp (e.g., CF_LOCK key).
	}
	tsVal, _, err := codec.DecodeUint64Desc(rest)
	if err != nil {
		return nil, 0, err
	}
	return key, txntypes.TimeStamp(tsVal), nil
}

// EncodeLockKey encodes a user key for storage in CF_LOCK (no timestamp).
func EncodeLockKey(key Key) []byte {
	return codec.EncodeBytes(nil, key)
}

// DecodeLockKey decodes a CF_LOCK key to get the user key.
func DecodeLockKey(encodedKey []byte) (Key, error) {
	key, _, err := codec.DecodeBytes(encodedKey)
	return key, err
}

// TruncateToUserKey strips the timestamp suffix from an encoded MVCC key,
// returning just the encoded user key portion.
func TruncateToUserKey(encodedKey []byte) []byte {
	if len(encodedKey) <= 8 {
		return encodedKey
	}
	return encodedKey[:len(encodedKey)-8]
}

// SeekBound is the number of non-data-changing versions (Lock, Rollback)
// to iterate through before using the LastChange optimization.
const SeekBound = 32
