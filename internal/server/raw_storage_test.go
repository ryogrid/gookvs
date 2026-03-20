package server

import (
	"encoding/binary"
	"hash/crc64"
	"path/filepath"
	"testing"
	"time"

	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRawStorage(t *testing.T) *RawStorage {
	t.Helper()
	dir := t.TempDir()
	eng, err := rocks.Open(filepath.Join(dir, "test-db"))
	require.NoError(t, err)
	t.Cleanup(func() { eng.Close() })
	return NewRawStorage(eng)
}

func TestRawGet_Found(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v1")))

	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestRawGet_NotFound(t *testing.T) {
	rs := newTestRawStorage(t)
	val, err := rs.Get("", []byte("missing"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestRawPut_Overwrite(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v1")))
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v2")))

	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestRawDelete(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v1")))
	require.NoError(t, rs.Delete("", []byte("k1")))

	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestRawScan_Forward(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	pairs, err := rs.Scan("", []byte("b"), []byte("d"), 10, false, false)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("b"), pairs[0].Key)
	assert.Equal(t, []byte("c"), pairs[1].Key)
}

func TestRawScan_Limit(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	pairs, err := rs.Scan("", nil, nil, 3, false, false)
	require.NoError(t, err)
	assert.Len(t, pairs, 3)
}

func TestRawScan_KeyOnly(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v1")))

	pairs, err := rs.Scan("", nil, nil, 10, true, false)
	require.NoError(t, err)
	require.Len(t, pairs, 1)
	assert.Equal(t, []byte("k1"), pairs[0].Key)
	assert.Nil(t, pairs[0].Value)
}

func TestRawScan_Reverse(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	pairs, err := rs.Scan("", nil, nil, 10, false, true)
	require.NoError(t, err)
	require.Len(t, pairs, 3)
	assert.Equal(t, []byte("c"), pairs[0].Key)
	assert.Equal(t, []byte("b"), pairs[1].Key)
	assert.Equal(t, []byte("a"), pairs[2].Key)
}

func TestRawBatchGet(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("a"), []byte("1")))
	require.NoError(t, rs.Put("", []byte("c"), []byte("3")))

	pairs, err := rs.BatchGet("", [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	require.NoError(t, err)
	assert.Len(t, pairs, 2) // "b" is missing
	assert.Equal(t, []byte("a"), pairs[0].Key)
	assert.Equal(t, []byte("c"), pairs[1].Key)
}

func TestRawBatchPut(t *testing.T) {
	rs := newTestRawStorage(t)
	pairs := []KvPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	require.NoError(t, rs.BatchPut("", pairs))

	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	val, err = rs.Get("", []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestRawBatchDelete(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("a"), []byte("1")))
	require.NoError(t, rs.Put("", []byte("b"), []byte("2")))
	require.NoError(t, rs.Put("", []byte("c"), []byte("3")))

	require.NoError(t, rs.BatchDelete("", [][]byte{[]byte("a"), []byte("c")}))

	val, err := rs.Get("", []byte("a"))
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = rs.Get("", []byte("b"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), val)

	val, err = rs.Get("", []byte("c"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestRawDeleteRange(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v")))
	}

	require.NoError(t, rs.DeleteRange("", []byte("b"), []byte("d")))

	// a and d,e should exist; b,c should not.
	val, err := rs.Get("", []byte("a"))
	require.NoError(t, err)
	assert.NotNil(t, val)

	val, err = rs.Get("", []byte("b"))
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = rs.Get("", []byte("c"))
	require.NoError(t, err)
	assert.Nil(t, val)

	val, err = rs.Get("", []byte("d"))
	require.NoError(t, err)
	assert.NotNil(t, val)
}

func TestRawCFSupport(t *testing.T) {
	rs := newTestRawStorage(t)

	// Write to different CFs.
	require.NoError(t, rs.Put(cfnames.CFDefault, []byte("k"), []byte("default")))
	require.NoError(t, rs.Put(cfnames.CFWrite, []byte("k"), []byte("write")))

	// Read from specific CFs.
	val, err := rs.Get(cfnames.CFDefault, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("default"), val)

	val, err = rs.Get(cfnames.CFWrite, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("write"), val)
}

func TestRawResolveCFEmpty(t *testing.T) {
	rs := newTestRawStorage(t)

	// Empty CF should default to CF_DEFAULT.
	require.NoError(t, rs.Put("", []byte("k"), []byte("v")))
	val, err := rs.Get(cfnames.CFDefault, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), val)
}

func TestRawPutModify(t *testing.T) {
	rs := newTestRawStorage(t)
	m := rs.PutModify("", []byte("k"), []byte("v"))
	assert.Equal(t, cfnames.CFDefault, m.CF)
	assert.Equal(t, []byte("k"), m.Key)
	assert.Equal(t, []byte("v"), m.Value)
}

func TestRawDeleteModify(t *testing.T) {
	rs := newTestRawStorage(t)
	m := rs.DeleteModify("", []byte("k"))
	assert.Equal(t, cfnames.CFDefault, m.CF)
	assert.Equal(t, []byte("k"), m.Key)
}

// --- TTL encoding/decoding unit tests ---

func TestEncodeDecodeValueTTL_NoTTL(t *testing.T) {
	original := []byte("hello")
	encoded := encodeValueWithTTL(original, 0)
	assert.Equal(t, original, encoded, "ttl=0 should return value unchanged")

	value, ttl, expired, hasTTL := decodeValueTTL(encoded)
	assert.Equal(t, original, value)
	assert.Equal(t, uint64(0), ttl)
	assert.False(t, expired)
	assert.False(t, hasTTL)
}

func TestEncodeDecodeValueTTL_WithTTL(t *testing.T) {
	original := []byte("hello")
	encoded := encodeValueWithTTL(original, 3600)

	// Encoded should be 9 bytes longer.
	assert.Equal(t, len(original)+ttlMetaSize, len(encoded))
	assert.Equal(t, byte(ttlFlagByte), encoded[len(encoded)-1])

	value, ttl, expired, hasTTL := decodeValueTTL(encoded)
	assert.Equal(t, original, value)
	assert.True(t, hasTTL)
	assert.False(t, expired)
	// TTL should be approximately 3600 (allow 2 seconds of test execution time).
	assert.InDelta(t, 3600, float64(ttl), 2)
}

func TestDecodeValueTTL_Expired(t *testing.T) {
	// Manually create an expired TTL value: expireTS = now - 10.
	original := []byte("data")
	expireTS := uint64(time.Now().Unix()) - 10
	buf := make([]byte, len(original)+ttlMetaSize)
	copy(buf, original)
	binary.BigEndian.PutUint64(buf[len(original):], expireTS)
	buf[len(buf)-1] = ttlFlagByte

	value, ttl, expired, hasTTL := decodeValueTTL(buf)
	assert.Equal(t, original, value)
	assert.True(t, hasTTL)
	assert.True(t, expired)
	assert.Equal(t, uint64(0), ttl)
}

func TestDecodeValueTTL_ShortValue(t *testing.T) {
	// Value shorter than ttlMetaSize should be treated as no-TTL.
	short := []byte("hi")
	value, ttl, expired, hasTTL := decodeValueTTL(short)
	assert.Equal(t, short, value)
	assert.False(t, hasTTL)
	assert.False(t, expired)
	assert.Equal(t, uint64(0), ttl)
}

func TestDecodeValueTTL_NoFlagByte(t *testing.T) {
	// 9+ byte value without flag byte should be treated as no-TTL.
	data := []byte("123456789ABC")
	value, _, _, hasTTL := decodeValueTTL(data)
	assert.Equal(t, data, value)
	assert.False(t, hasTTL)
}

// --- TTL integration with storage methods ---

func TestRawPut_WithTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k1"), []byte("v1"), 3600))

	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)
}

func TestRawGet_ExpiredTTL(t *testing.T) {
	rs := newTestRawStorage(t)

	// Write a value that is already expired by writing raw bytes directly.
	expireTS := uint64(time.Now().Unix()) - 10
	val := []byte("old")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("expired"), buf))

	// Get should return nil (expired).
	result, err := rs.Get("", []byte("expired"))
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestRawScan_FiltersExpiredTTL(t *testing.T) {
	rs := newTestRawStorage(t)

	// Write a live key.
	require.NoError(t, rs.Put("", []byte("a"), []byte("alive"), 3600))

	// Write an expired key directly.
	expireTS := uint64(time.Now().Unix()) - 10
	val := []byte("dead")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("b"), buf))

	// Write another live key without TTL.
	require.NoError(t, rs.Put("", []byte("c"), []byte("no-ttl")))

	pairs, err := rs.Scan("", nil, nil, 10, false, false)
	require.NoError(t, err)
	assert.Len(t, pairs, 2)
	assert.Equal(t, []byte("a"), pairs[0].Key)
	assert.Equal(t, []byte("alive"), pairs[0].Value)
	assert.Equal(t, []byte("c"), pairs[1].Key)
	assert.Equal(t, []byte("no-ttl"), pairs[1].Value)
}

func TestRawBatchGet_FiltersExpiredTTL(t *testing.T) {
	rs := newTestRawStorage(t)

	require.NoError(t, rs.Put("", []byte("live"), []byte("v1"), 3600))

	// Write an expired key directly.
	expireTS := uint64(time.Now().Unix()) - 10
	val := []byte("dead")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("expired"), buf))

	pairs, err := rs.BatchGet("", [][]byte{[]byte("live"), []byte("expired"), []byte("missing")})
	require.NoError(t, err)
	assert.Len(t, pairs, 1)
	assert.Equal(t, []byte("live"), pairs[0].Key)
}

func TestRawBatchPutWithTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	pairs := []KvPair{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	ttls := []uint64{3600, 7200}
	require.NoError(t, rs.BatchPutWithTTL("", pairs, ttls))

	// Both should be readable.
	val, err := rs.Get("", []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	val, err = rs.Get("", []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)

	// Check TTLs.
	ttl, notFound, err := rs.GetKeyTTL("", []byte("k1"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.InDelta(t, 3600, float64(ttl), 2)

	ttl, notFound, err = rs.GetKeyTTL("", []byte("k2"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.InDelta(t, 7200, float64(ttl), 2)
}

// --- BatchScan tests ---

func TestRawBatchScan(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	ranges := []KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("c")},
		{StartKey: []byte("d"), EndKey: []byte("f")},
	}
	pairs, err := rs.BatchScan("", ranges, 10, false, false)
	require.NoError(t, err)
	assert.Len(t, pairs, 4) // a,b from range 1; d,e from range 2
	assert.Equal(t, []byte("a"), pairs[0].Key)
	assert.Equal(t, []byte("b"), pairs[1].Key)
	assert.Equal(t, []byte("d"), pairs[2].Key)
	assert.Equal(t, []byte("e"), pairs[3].Key)
}

func TestRawBatchScan_EachLimit(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e", "f"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	ranges := []KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("d")},
		{StartKey: []byte("d"), EndKey: []byte("g")},
	}
	pairs, err := rs.BatchScan("", ranges, 2, false, false)
	require.NoError(t, err)
	assert.Len(t, pairs, 4) // 2 from each range
	assert.Equal(t, []byte("a"), pairs[0].Key)
	assert.Equal(t, []byte("b"), pairs[1].Key)
	assert.Equal(t, []byte("d"), pairs[2].Key)
	assert.Equal(t, []byte("e"), pairs[3].Key)
}

func TestRawBatchScan_KeyOnly(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("a"), []byte("v1")))
	require.NoError(t, rs.Put("", []byte("b"), []byte("v2")))

	ranges := []KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}
	pairs, err := rs.BatchScan("", ranges, 10, true, false)
	require.NoError(t, err)
	require.Len(t, pairs, 2)
	assert.Nil(t, pairs[0].Value)
	assert.Nil(t, pairs[1].Value)
}

// --- GetKeyTTL tests ---

func TestRawGetKeyTTL_NotFound(t *testing.T) {
	rs := newTestRawStorage(t)
	_, notFound, err := rs.GetKeyTTL("", []byte("missing"))
	require.NoError(t, err)
	assert.True(t, notFound)
}

func TestRawGetKeyTTL_NoTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("v")))

	ttl, notFound, err := rs.GetKeyTTL("", []byte("k"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, uint64(0), ttl)
}

func TestRawGetKeyTTL_WithTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("v"), 600))

	ttl, notFound, err := rs.GetKeyTTL("", []byte("k"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.InDelta(t, 600, float64(ttl), 2)
}

func TestRawGetKeyTTL_Expired(t *testing.T) {
	rs := newTestRawStorage(t)

	// Write an expired key directly.
	expireTS := uint64(time.Now().Unix()) - 5
	val := []byte("old")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("k"), buf))

	_, notFound, err := rs.GetKeyTTL("", []byte("k"))
	require.NoError(t, err)
	assert.True(t, notFound)
}

// --- CompareAndSwap tests ---

func TestRawCAS_PutWhenNotExist(t *testing.T) {
	rs := newTestRawStorage(t)

	succeed, prevNotExist, prevValue, err := rs.CompareAndSwap("", []byte("k"), []byte("new"), nil, true, false, 0)
	require.NoError(t, err)
	assert.True(t, succeed)
	assert.True(t, prevNotExist)
	assert.Nil(t, prevValue)

	val, err := rs.Get("", []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), val)
}

func TestRawCAS_PutWhenNotExist_Fails(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("existing")))

	succeed, prevNotExist, prevValue, err := rs.CompareAndSwap("", []byte("k"), []byte("new"), nil, true, false, 0)
	require.NoError(t, err)
	assert.False(t, succeed)
	assert.False(t, prevNotExist)
	assert.Equal(t, []byte("existing"), prevValue)

	// Original value should be unchanged.
	val, err := rs.Get("", []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("existing"), val)
}

func TestRawCAS_SwapValue(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("v1")))

	succeed, _, prevValue, err := rs.CompareAndSwap("", []byte("k"), []byte("v2"), []byte("v1"), false, false, 0)
	require.NoError(t, err)
	assert.True(t, succeed)
	assert.Equal(t, []byte("v1"), prevValue)

	val, err := rs.Get("", []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
}

func TestRawCAS_SwapValue_Mismatch(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("v1")))

	succeed, _, prevValue, err := rs.CompareAndSwap("", []byte("k"), []byte("v2"), []byte("wrong"), false, false, 0)
	require.NoError(t, err)
	assert.False(t, succeed)
	assert.Equal(t, []byte("v1"), prevValue)
}

func TestRawCAS_Delete(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("k"), []byte("v1")))

	succeed, _, _, err := rs.CompareAndSwap("", []byte("k"), nil, []byte("v1"), false, true, 0)
	require.NoError(t, err)
	assert.True(t, succeed)

	val, err := rs.Get("", []byte("k"))
	require.NoError(t, err)
	assert.Nil(t, val)
}

func TestRawCAS_WithTTL(t *testing.T) {
	rs := newTestRawStorage(t)

	succeed, _, _, err := rs.CompareAndSwap("", []byte("k"), []byte("v"), nil, true, false, 3600)
	require.NoError(t, err)
	assert.True(t, succeed)

	ttl, notFound, err := rs.GetKeyTTL("", []byte("k"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.InDelta(t, 3600, float64(ttl), 2)
}

func TestRawCAS_ExpiredTreatedAsNotExist(t *testing.T) {
	rs := newTestRawStorage(t)

	// Write an expired key directly.
	expireTS := uint64(time.Now().Unix()) - 5
	val := []byte("old")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("k"), buf))

	// CAS with prevNotExist=true should succeed since the key is expired.
	succeed, prevNotExist, _, err := rs.CompareAndSwap("", []byte("k"), []byte("new"), nil, true, false, 0)
	require.NoError(t, err)
	assert.True(t, succeed)
	assert.True(t, prevNotExist)

	result, err := rs.Get("", []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), result)
}

// --- Checksum tests ---

func TestRawChecksum_Basic(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("a"), []byte("1")))
	require.NoError(t, rs.Put("", []byte("b"), []byte("2")))
	require.NoError(t, rs.Put("", []byte("c"), []byte("3")))

	ranges := []KeyRange{{StartKey: []byte("a"), EndKey: []byte("d")}}
	checksum, totalKvs, totalBytes, err := rs.Checksum("", ranges)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), totalKvs)
	assert.Equal(t, uint64(6), totalBytes) // 3 keys (1 byte each) + 3 values (1 byte each)
	assert.NotZero(t, checksum)

	// Verify checksum manually.
	table := crc64.MakeTable(crc64.ECMA)
	var expected uint64
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}, {"c", "3"}} {
		d := crc64.New(table)
		d.Write([]byte(kv.k))
		d.Write([]byte(kv.v))
		expected ^= d.Sum64()
	}
	assert.Equal(t, expected, checksum)
}

func TestRawChecksum_FiltersExpired(t *testing.T) {
	rs := newTestRawStorage(t)
	require.NoError(t, rs.Put("", []byte("a"), []byte("alive")))

	// Write an expired key directly.
	expireTS := uint64(time.Now().Unix()) - 5
	val := []byte("dead")
	buf := make([]byte, len(val)+ttlMetaSize)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], expireTS)
	buf[len(buf)-1] = ttlFlagByte
	require.NoError(t, rs.engine.Put(cfnames.CFDefault, []byte("b"), buf))

	ranges := []KeyRange{{StartKey: []byte("a"), EndKey: []byte("c")}}
	_, totalKvs, _, err := rs.Checksum("", ranges)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), totalKvs) // Only "a" counted, "b" is expired.
}

func TestRawChecksum_MultipleRanges(t *testing.T) {
	rs := newTestRawStorage(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, rs.Put("", []byte(k), []byte("v-"+k)))
	}

	ranges := []KeyRange{
		{StartKey: []byte("a"), EndKey: []byte("c")}, // a, b
		{StartKey: []byte("d"), EndKey: []byte("f")}, // d, e
	}
	_, totalKvs, _, err := rs.Checksum("", ranges)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), totalKvs)
}

func TestRawChecksum_EmptyRange(t *testing.T) {
	rs := newTestRawStorage(t)

	ranges := []KeyRange{{StartKey: []byte("x"), EndKey: []byte("z")}}
	checksum, totalKvs, totalBytes, err := rs.Checksum("", ranges)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), checksum)
	assert.Equal(t, uint64(0), totalKvs)
	assert.Equal(t, uint64(0), totalBytes)
}

// --- PutModify with TTL ---

func TestRawPutModify_WithTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	m := rs.PutModify("", []byte("k"), []byte("v"), 3600)
	assert.Equal(t, cfnames.CFDefault, m.CF)
	assert.Equal(t, []byte("k"), m.Key)
	// Value should be encoded with TTL (original + 9 bytes).
	assert.Equal(t, len([]byte("v"))+ttlMetaSize, len(m.Value))
	assert.Equal(t, byte(ttlFlagByte), m.Value[len(m.Value)-1])
}

func TestRawPutModify_NoTTL(t *testing.T) {
	rs := newTestRawStorage(t)
	m := rs.PutModify("", []byte("k"), []byte("v"))
	assert.Equal(t, []byte("v"), m.Value, "no TTL should leave value unchanged")
}
