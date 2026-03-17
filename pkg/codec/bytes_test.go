package codec

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test cases ported from TiKV: components/codec/src/byte.rs

func TestEncodedBytesLength(t *testing.T) {
	cases := []struct {
		srcLen     int
		encodedLen int
	}{
		{0, 9},
		{4, 9},
		{7, 9},
		{8, 18},
		{9, 18},
		{16, 27},
		{17, 27},
	}
	for _, c := range cases {
		assert.Equal(t, c.encodedLen, EncodedBytesLength(c.srcLen),
			"EncodedBytesLength(%d)", c.srcLen)
	}
}

func TestEncodeDecodeBytes(t *testing.T) {
	// Test cases from TiKV: test_read_bytes and test_memcmp_encode_all
	cases := []struct {
		src     []byte
		encoded []byte
	}{
		{[]byte{}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 247}},
		{[]byte{0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 248}},
		{[]byte{1, 2, 3}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 250}},
		{[]byte{1, 2, 3, 0}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 251}},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{1, 2, 3, 4, 5, 6, 7, 0, 254}},
		{
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247},
		},
		{
			[]byte{1, 2, 3, 4, 5, 6, 7, 8},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247},
		},
		{
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248},
		},
	}

	for _, c := range cases {
		// Test encoding
		encoded := EncodeBytes(nil, c.src)
		assert.Equal(t, c.encoded, encoded, "EncodeBytes(%v)", c.src)

		// Test decoding
		decoded, remaining, err := DecodeBytes(encoded)
		require.NoError(t, err, "DecodeBytes(%v)", c.encoded)
		assert.Equal(t, c.src, decoded, "DecodeBytes(%v)", c.encoded)
		assert.Empty(t, remaining)
	}
}

func TestEncodeBytesDesc(t *testing.T) {
	// Descending encoding test cases from TiKV: test_memcmp_encode_all
	cases := []struct {
		src     []byte
		encoded []byte
	}{
		{[]byte{}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 8}},
		{[]byte{0}, []byte{255, 255, 255, 255, 255, 255, 255, 255, 7}},
		{[]byte{1, 2, 3}, []byte{254, 253, 252, 255, 255, 255, 255, 255, 5}},
		{[]byte{1, 2, 3, 0}, []byte{254, 253, 252, 255, 255, 255, 255, 255, 4}},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{254, 253, 252, 251, 250, 249, 248, 255, 1}},
		{
			[]byte{0, 0, 0, 0, 0, 0, 0, 0},
			[]byte{
				255, 255, 255, 255, 255, 255, 255, 255, 0,
				255, 255, 255, 255, 255, 255, 255, 255, 8,
			},
		},
		{
			[]byte{1, 2, 3, 4, 5, 6, 7, 8},
			[]byte{
				254, 253, 252, 251, 250, 249, 248, 247, 0,
				255, 255, 255, 255, 255, 255, 255, 255, 8,
			},
		},
		{
			[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
			[]byte{
				254, 253, 252, 251, 250, 249, 248, 247, 0,
				246, 255, 255, 255, 255, 255, 255, 255, 7,
			},
		},
	}

	for _, c := range cases {
		encoded := EncodeBytesDesc(nil, c.src)
		assert.Equal(t, c.encoded, encoded, "EncodeBytesDesc(%v)", c.src)

		decoded, remaining, err := DecodeBytesDesc(encoded)
		require.NoError(t, err, "DecodeBytesDesc(%v)", c.encoded)
		assert.Equal(t, c.src, decoded, "DecodeBytesDesc(%v)", c.encoded)
		assert.Empty(t, remaining)
	}
}

func TestEncodeBytesOrdering(t *testing.T) {
	// Verify that encoded bytes preserve lexicographic ordering.
	inputs := [][]byte{
		{},
		{0x00},
		{0x00, 0x01},
		{0x01},
		{0x01, 0x00},
		{0x01, 0x01},
		{0xFF},
		{0xFF, 0x00},
		{0xFF, 0xFF},
	}

	// Sort by raw bytes
	sorted := make([][]byte, len(inputs))
	copy(sorted, inputs)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})

	// Encode and sort
	encodedSorted := make([][]byte, len(inputs))
	for i, v := range sorted {
		encodedSorted[i] = EncodeBytes(nil, v)
	}

	// Verify encoded order matches raw order
	for i := 1; i < len(encodedSorted); i++ {
		assert.True(t, bytes.Compare(encodedSorted[i-1], encodedSorted[i]) < 0,
			"ordering violated: Encode(%v) should be < Encode(%v)",
			sorted[i-1], sorted[i])
	}
}

func TestEncodeBytesDescOrdering(t *testing.T) {
	// Verify that descending encoded bytes reverse the ordering.
	inputs := [][]byte{
		{},
		{0x00},
		{0x01},
		{0xFF},
		{0x01, 0x02, 0x03},
		{0xFF, 0xFF, 0xFF},
	}

	// Sort raw ascending
	sorted := make([][]byte, len(inputs))
	copy(sorted, inputs)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i], sorted[j]) < 0
	})

	// Encode descending
	encoded := make([][]byte, len(sorted))
	for i, v := range sorted {
		encoded[i] = EncodeBytesDesc(nil, v)
	}

	// Verify descending order: larger raw values should have smaller encoded values
	for i := 1; i < len(encoded); i++ {
		assert.True(t, bytes.Compare(encoded[i-1], encoded[i]) > 0,
			"desc ordering violated")
	}
}

func TestDecodeBytesWithRemaining(t *testing.T) {
	// Encode two values back to back
	data := EncodeBytes(nil, []byte("hello"))
	data = EncodeBytes(data, []byte("world"))

	decoded1, remaining, err := DecodeBytes(data)
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), decoded1)
	assert.NotEmpty(t, remaining)

	decoded2, remaining, err := DecodeBytes(remaining)
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), decoded2)
	assert.Empty(t, remaining)
}

func TestDecodeBytesErrors(t *testing.T) {
	// Too short
	_, _, err := DecodeBytes([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrInsufficientData)

	// Empty
	_, _, err = DecodeBytes([]byte{})
	assert.ErrorIs(t, err, ErrInsufficientData)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for length := 0; length <= 64; length++ {
		data := make([]byte, length)
		rng.Read(data)

		// Ascending
		encoded := EncodeBytes(nil, data)
		decoded, remaining, err := DecodeBytes(encoded)
		require.NoError(t, err)
		assert.Equal(t, data, decoded)
		assert.Empty(t, remaining)

		// Descending
		encodedDesc := EncodeBytesDesc(nil, data)
		decodedDesc, remaining, err := DecodeBytesDesc(encodedDesc)
		require.NoError(t, err)
		assert.Equal(t, data, decodedDesc)
		assert.Empty(t, remaining)
	}
}

func TestEncodeBytesAppendToDst(t *testing.T) {
	prefix := []byte("prefix:")
	encoded := EncodeBytes(prefix, []byte("data"))

	// The prefix should be preserved
	assert.Equal(t, []byte("prefix:"), encoded[:7])

	// Decode should work on the encoded portion
	decoded, _, err := DecodeBytes(encoded[7:])
	require.NoError(t, err)
	assert.Equal(t, []byte("data"), decoded)
}
