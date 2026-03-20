package server

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"sync"
	"time"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/internal/storage/mvcc"
	"github.com/ryogrid/gookv/pkg/cfnames"
)

const (
	// ttlFlagByte is appended as the last byte to indicate TTL metadata is present.
	ttlFlagByte byte = 0x01
	// ttlMetaSize is the total overhead for TTL encoding: 8 bytes timestamp + 1 byte flag.
	ttlMetaSize = 9
)

// encodeValueWithTTL encodes a value with TTL metadata.
// If ttl == 0, returns the raw value unchanged (no TTL).
func encodeValueWithTTL(value []byte, ttl uint64) []byte {
	if ttl == 0 {
		return value
	}
	expireTS := uint64(time.Now().Unix()) + ttl
	buf := make([]byte, len(value)+ttlMetaSize)
	copy(buf, value)
	binary.BigEndian.PutUint64(buf[len(value):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	return buf
}

// decodeValueTTL extracts user value and remaining TTL from an encoded value.
// Returns (userValue, remainingTTL, expired, hasTTL).
func decodeValueTTL(raw []byte) (value []byte, ttl uint64, expired bool, hasTTL bool) {
	if len(raw) < ttlMetaSize || raw[len(raw)-1] != ttlFlagByte {
		return raw, 0, false, false
	}
	expireTS := binary.BigEndian.Uint64(raw[len(raw)-ttlMetaSize : len(raw)-1])
	now := uint64(time.Now().Unix())
	userValue := raw[:len(raw)-ttlMetaSize]
	if now >= expireTS {
		return userValue, 0, true, true
	}
	return userValue, expireTS - now, false, true
}

// KeyRange represents a start/end key range for batch scan and checksum operations.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// KvPair holds a key-value pair for raw KV operations.
type KvPair struct {
	Key   []byte
	Value []byte
}

// RawStorage provides non-transactional key-value operations
// directly against the storage engine, bypassing MVCC.
type RawStorage struct {
	engine traits.KvEngine
	casMu  sync.Mutex // global mutex for CAS atomicity
}

// NewRawStorage creates a new RawStorage backed by the given engine.
func NewRawStorage(engine traits.KvEngine) *RawStorage {
	return &RawStorage{engine: engine}
}

func (rs *RawStorage) resolveCF(cf string) string {
	if cf == "" {
		return cfnames.CFDefault
	}
	return cf
}

// Get retrieves a value by key. Returns (nil, nil) if not found or expired.
func (rs *RawStorage) Get(cf string, key []byte) ([]byte, error) {
	cf = rs.resolveCF(cf)
	raw, err := rs.engine.Get(cf, key)
	if err != nil {
		if errors.Is(err, traits.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	value, _, expired, _ := decodeValueTTL(raw)
	if expired {
		return nil, nil
	}
	return value, nil
}

// Put stores a key-value pair with an optional TTL in seconds (0 means no expiry).
func (rs *RawStorage) Put(cf string, key, value []byte, ttl ...uint64) error {
	cf = rs.resolveCF(cf)
	var t uint64
	if len(ttl) > 0 {
		t = ttl[0]
	}
	encoded := encodeValueWithTTL(value, t)
	return rs.engine.Put(cf, key, encoded)
}

// Delete removes a key.
func (rs *RawStorage) Delete(cf string, key []byte) error {
	cf = rs.resolveCF(cf)
	return rs.engine.Delete(cf, key)
}

// BatchGet reads multiple keys from a consistent snapshot, filtering expired entries.
func (rs *RawStorage) BatchGet(cf string, keys [][]byte) ([]KvPair, error) {
	cf = rs.resolveCF(cf)
	snap := rs.engine.NewSnapshot()
	defer snap.Close()

	pairs := make([]KvPair, 0, len(keys))
	for _, key := range keys {
		raw, err := snap.Get(cf, key)
		if err != nil {
			if errors.Is(err, traits.ErrNotFound) {
				continue
			}
			return nil, err
		}
		value, _, expired, _ := decodeValueTTL(raw)
		if expired {
			continue
		}
		pairs = append(pairs, KvPair{Key: key, Value: value})
	}
	return pairs, nil
}

// BatchPutWithTTL atomically writes multiple key-value pairs with per-key TTLs.
// ttls slice must be empty (no TTL) or match the length of pairs.
func (rs *RawStorage) BatchPutWithTTL(cf string, pairs []KvPair, ttls []uint64) error {
	cf = rs.resolveCF(cf)
	wb := rs.engine.NewWriteBatch()
	for i, p := range pairs {
		var t uint64
		if i < len(ttls) {
			t = ttls[i]
		}
		encoded := encodeValueWithTTL(p.Value, t)
		if err := wb.Put(cf, p.Key, encoded); err != nil {
			return err
		}
	}
	return wb.Commit()
}

// BatchPut atomically writes multiple key-value pairs (no TTL).
func (rs *RawStorage) BatchPut(cf string, pairs []KvPair) error {
	return rs.BatchPutWithTTL(cf, pairs, nil)
}

// BatchDelete atomically deletes multiple keys.
func (rs *RawStorage) BatchDelete(cf string, keys [][]byte) error {
	cf = rs.resolveCF(cf)
	wb := rs.engine.NewWriteBatch()
	for _, key := range keys {
		if err := wb.Delete(cf, key); err != nil {
			return err
		}
	}
	return wb.Commit()
}

// DeleteRange removes all keys in [startKey, endKey).
func (rs *RawStorage) DeleteRange(cf string, startKey, endKey []byte) error {
	cf = rs.resolveCF(cf)
	return rs.engine.DeleteRange(cf, startKey, endKey)
}

// Scan iterates over keys in [startKey, endKey) with a limit, filtering expired entries.
func (rs *RawStorage) Scan(cf string, startKey, endKey []byte, limit uint32, keyOnly bool, reverse bool) ([]KvPair, error) {
	cf = rs.resolveCF(cf)
	snap := rs.engine.NewSnapshot()
	defer snap.Close()

	opts := traits.IterOptions{}
	if !reverse {
		opts.LowerBound = startKey
		if len(endKey) > 0 {
			opts.UpperBound = endKey
		}
	} else {
		if len(endKey) > 0 {
			opts.LowerBound = endKey
		}
		opts.UpperBound = startKey
	}

	iter := snap.NewIterator(cf, opts)
	defer iter.Close()

	if reverse {
		iter.SeekToLast()
	} else {
		iter.SeekToFirst()
	}

	var pairs []KvPair
	for iter.Valid() && (limit == 0 || uint32(len(pairs)) < limit) {
		raw := iter.Value()
		_, _, expired, _ := decodeValueTTL(raw)
		if expired {
			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
			continue
		}

		key := append([]byte(nil), iter.Key()...)
		var value []byte
		if !keyOnly {
			userValue, _, _, _ := decodeValueTTL(raw)
			value = append([]byte(nil), userValue...)
		}
		pairs = append(pairs, KvPair{Key: key, Value: value})

		if reverse {
			iter.Prev()
		} else {
			iter.Next()
		}
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}
	return pairs, nil
}

// BatchScan scans multiple key ranges, returning up to eachLimit pairs per range.
// Expired TTL entries are filtered out.
func (rs *RawStorage) BatchScan(cf string, ranges []KeyRange, eachLimit uint32, keyOnly bool, reverse bool) ([]KvPair, error) {
	var allPairs []KvPair
	for _, r := range ranges {
		pairs, err := rs.Scan(cf, r.StartKey, r.EndKey, eachLimit, keyOnly, reverse)
		if err != nil {
			return nil, err
		}
		allPairs = append(allPairs, pairs...)
	}
	return allPairs, nil
}

// GetKeyTTL returns the remaining TTL in seconds for a key.
// If the key has no TTL, returns ttl=0. If not found or expired, returns notFound=true.
func (rs *RawStorage) GetKeyTTL(cf string, key []byte) (ttl uint64, notFound bool, err error) {
	cf = rs.resolveCF(cf)
	raw, err := rs.engine.Get(cf, key)
	if err != nil {
		if errors.Is(err, traits.ErrNotFound) {
			return 0, true, nil
		}
		return 0, false, err
	}
	_, remainingTTL, expired, hasTTL := decodeValueTTL(raw)
	if expired {
		return 0, true, nil
	}
	if !hasTTL {
		return 0, false, nil
	}
	return remainingTTL, false, nil
}

// CompareAndSwap atomically compares and swaps a value.
// If prevNotExist is true, the operation succeeds only when the key doesn't exist.
// If isDelete is true, the key is deleted on match instead of being set to value.
// Returns (succeed, prevNotExistOut, prevValueOut, err).
func (rs *RawStorage) CompareAndSwap(cf string, key, value, prevValue []byte, prevNotExist bool, isDelete bool, ttl uint64) (succeed bool, prevNotExistOut bool, prevValueOut []byte, err error) {
	rs.casMu.Lock()
	defer rs.casMu.Unlock()

	cf = rs.resolveCF(cf)

	// Read current value.
	raw, getErr := rs.engine.Get(cf, key)
	if getErr != nil && !errors.Is(getErr, traits.ErrNotFound) {
		return false, false, nil, getErr
	}

	// Decode TTL, treat expired as not-found.
	var currentValue []byte
	currentNotExist := errors.Is(getErr, traits.ErrNotFound)
	if !currentNotExist {
		var expired bool
		currentValue, _, expired, _ = decodeValueTTL(raw)
		if expired {
			currentNotExist = true
			currentValue = nil
		}
	}

	// Compare with expected.
	var match bool
	if prevNotExist {
		match = currentNotExist
	} else {
		if currentNotExist {
			match = false
		} else {
			match = bytesEqual(currentValue, prevValue)
		}
	}

	if !match {
		return false, currentNotExist, currentValue, nil
	}

	// Apply the change.
	if isDelete {
		if err := rs.engine.Delete(cf, key); err != nil {
			return false, currentNotExist, currentValue, err
		}
	} else {
		encoded := encodeValueWithTTL(value, ttl)
		if err := rs.engine.Put(cf, key, encoded); err != nil {
			return false, currentNotExist, currentValue, err
		}
	}

	return true, currentNotExist, currentValue, nil
}

// crc64Table is the ECMA CRC-64 table used for RawChecksum.
var crc64Table = crc64.MakeTable(crc64.ECMA)

// Checksum computes CRC64-XOR checksum over all non-expired KV pairs in the given ranges.
// Returns (checksum, totalKvs, totalBytes, err).
func (rs *RawStorage) Checksum(cf string, ranges []KeyRange) (checksum uint64, totalKvs uint64, totalBytes uint64, err error) {
	cf = rs.resolveCF(cf)
	snap := rs.engine.NewSnapshot()
	defer snap.Close()

	for _, r := range ranges {
		opts := traits.IterOptions{
			LowerBound: r.StartKey,
		}
		if len(r.EndKey) > 0 {
			opts.UpperBound = r.EndKey
		}

		iter := snap.NewIterator(cf, opts)
		iter.SeekToFirst()

		for iter.Valid() {
			raw := iter.Value()
			userValue, _, expired, _ := decodeValueTTL(raw)
			if expired {
				iter.Next()
				continue
			}

			k := iter.Key()
			digest := crc64.New(crc64Table)
			digest.Write(k)
			digest.Write(userValue)
			checksum ^= digest.Sum64()

			totalKvs++
			totalBytes += uint64(len(k)) + uint64(len(userValue))

			iter.Next()
		}

		if iterErr := iter.Error(); iterErr != nil {
			iter.Close()
			return 0, 0, 0, iterErr
		}
		iter.Close()
	}

	return checksum, totalKvs, totalBytes, nil
}

// PutModify returns a Modify for use with Raft proposal (cluster mode).
// Accepts an optional TTL in seconds; if ttl > 0, the value is encoded with TTL metadata.
func (rs *RawStorage) PutModify(cf string, key, value []byte, ttl ...uint64) mvcc.Modify {
	cf = rs.resolveCF(cf)
	var t uint64
	if len(ttl) > 0 {
		t = ttl[0]
	}
	encoded := encodeValueWithTTL(value, t)
	return mvcc.Modify{
		Type:  mvcc.ModifyTypePut,
		CF:    cf,
		Key:   key,
		Value: encoded,
	}
}

// DeleteModify returns a Modify for use with Raft proposal (cluster mode).
func (rs *RawStorage) DeleteModify(cf string, key []byte) mvcc.Modify {
	cf = rs.resolveCF(cf)
	return mvcc.Modify{
		Type: mvcc.ModifyTypeDelete,
		CF:   cf,
		Key:  key,
	}
}

// bytesEqual returns true if a and b have the same contents.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
