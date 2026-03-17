package codec

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test cases ported from TiKV: components/codec/src/number.rs

func TestEncodeDecodeUint64(t *testing.T) {
	samples := []uint64{
		0, 1, 2, 127, 128, 255, 256,
		math.MaxUint16 - 1, math.MaxUint16, math.MaxUint16 + 1,
		math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1,
		math.MaxUint64 - 1, math.MaxUint64,
	}

	for _, v := range samples {
		encoded := EncodeUint64(nil, v)
		assert.Len(t, encoded, 8)

		decoded, remaining, err := DecodeUint64(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
		assert.Empty(t, remaining)
	}
}

func TestUint64Ordering(t *testing.T) {
	// Ascending encoding should preserve ordering
	samples := []uint64{0, 1, 100, 1000, math.MaxUint32, math.MaxUint64}

	encoded := make([][]byte, len(samples))
	for i, v := range samples {
		encoded[i] = EncodeUint64(nil, v)
	}

	for i := 1; i < len(encoded); i++ {
		assert.True(t, bytes.Compare(encoded[i-1], encoded[i]) < 0,
			"uint64 ordering: %d should be < %d", samples[i-1], samples[i])
	}
}

func TestEncodeDecodeUint64Desc(t *testing.T) {
	samples := []uint64{0, 1, 128, math.MaxUint32, math.MaxUint64}

	for _, v := range samples {
		encoded := EncodeUint64Desc(nil, v)
		decoded, remaining, err := DecodeUint64Desc(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
		assert.Empty(t, remaining)
	}
}

func TestUint64DescOrdering(t *testing.T) {
	// Descending encoding should reverse ordering
	samples := []uint64{0, 1, 100, 1000, math.MaxUint32, math.MaxUint64}

	encoded := make([][]byte, len(samples))
	for i, v := range samples {
		encoded[i] = EncodeUint64Desc(nil, v)
	}

	for i := 1; i < len(encoded); i++ {
		assert.True(t, bytes.Compare(encoded[i-1], encoded[i]) > 0,
			"uint64 desc ordering: %d should be > %d in encoded form", samples[i-1], samples[i])
	}
}

func TestEncodeDecodeInt64(t *testing.T) {
	samples := []int64{
		math.MinInt64, math.MinInt64 + 1,
		-1000, -100, -1, 0, 1, 100, 1000,
		math.MaxInt64 - 1, math.MaxInt64,
	}

	for _, v := range samples {
		encoded := EncodeInt64(nil, v)
		decoded, remaining, err := DecodeInt64(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
		assert.Empty(t, remaining)
	}
}

func TestInt64Ordering(t *testing.T) {
	samples := []int64{
		math.MinInt64, -1000, -1, 0, 1, 1000, math.MaxInt64,
	}

	encoded := make([][]byte, len(samples))
	for i, v := range samples {
		encoded[i] = EncodeInt64(nil, v)
	}

	for i := 1; i < len(encoded); i++ {
		assert.True(t, bytes.Compare(encoded[i-1], encoded[i]) < 0,
			"int64 ordering: %d should be < %d", samples[i-1], samples[i])
	}
}

func TestEncodeDecodeFloat64(t *testing.T) {
	samples := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e308, -1e10, -1.0, -math.SmallestNonzeroFloat64,
		0,
		math.SmallestNonzeroFloat64,
		1.0, 1e10, 1e308,
		math.MaxFloat64,
		math.Inf(1),
	}

	for _, v := range samples {
		encoded := EncodeFloat64(nil, v)
		decoded, remaining, err := DecodeFloat64(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded, "float64 round-trip for %v", v)
		assert.Empty(t, remaining)
	}
}

func TestFloat64Ordering(t *testing.T) {
	samples := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e10, -1.0, -math.SmallestNonzeroFloat64,
		0,
		math.SmallestNonzeroFloat64,
		1.0, 1e10,
		math.MaxFloat64,
		math.Inf(1),
	}

	// Verify samples are already in ascending order
	assert.True(t, sort.Float64sAreSorted(samples))

	encoded := make([][]byte, len(samples))
	for i, v := range samples {
		encoded[i] = EncodeFloat64(nil, v)
	}

	for i := 1; i < len(encoded); i++ {
		assert.True(t, bytes.Compare(encoded[i-1], encoded[i]) < 0,
			"float64 ordering: %v should be < %v", samples[i-1], samples[i])
	}
}

func TestEncodeDecodeVarint(t *testing.T) {
	samples := []uint64{
		0, 1, 127, 128, 255, 256,
		16383, 16384,
		math.MaxUint16,
		math.MaxUint32,
		math.MaxUint64,
	}

	for _, v := range samples {
		encoded := EncodeVarint(nil, v)
		decoded, remaining, err := DecodeVarint(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded, "varint round-trip for %d", v)
		assert.Empty(t, remaining)
	}
}

func TestEncodeDecodeSignedVarint(t *testing.T) {
	samples := []int64{
		math.MinInt64, -1000, -1, 0, 1, 1000, math.MaxInt64,
	}

	for _, v := range samples {
		encoded := EncodeSignedVarint(nil, v)
		decoded, remaining, err := DecodeSignedVarint(encoded)
		require.NoError(t, err)
		assert.Equal(t, v, decoded)
		assert.Empty(t, remaining)
	}
}

func TestVarintWithRemaining(t *testing.T) {
	data := EncodeVarint(nil, 42)
	data = EncodeVarint(data, 100)

	v1, rest, err := DecodeVarint(data)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), v1)

	v2, rest, err := DecodeVarint(rest)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), v2)
	assert.Empty(t, rest)
}

func TestDecodeErrors(t *testing.T) {
	// Uint64 too short
	_, _, err := DecodeUint64([]byte{1, 2, 3})
	assert.ErrorIs(t, err, ErrInsufficientData)

	// Uint64Desc too short
	_, _, err = DecodeUint64Desc([]byte{1, 2})
	assert.ErrorIs(t, err, ErrInsufficientData)

	// Varint empty
	_, _, err = DecodeVarint([]byte{})
	assert.ErrorIs(t, err, ErrInsufficientData)
}

func TestNegativeZeroFloat64(t *testing.T) {
	posZero := EncodeFloat64(nil, 0.0)
	negZero := EncodeFloat64(nil, math.Copysign(0, -1))

	v1, _, err := DecodeFloat64(posZero)
	require.NoError(t, err)
	v2, _, err := DecodeFloat64(negZero)
	require.NoError(t, err)

	// Both should be == 0.0 in Go comparison
	assert.True(t, v1 == 0.0)
	assert.True(t, v2 == 0.0)

	// -0.0 sorts before +0.0 in encoded form (negative values sort first)
	assert.True(t, bytes.Compare(negZero, posZero) < 0,
		"-0.0 should sort before +0.0 in encoded form")
}

func TestUint64WithPrefix(t *testing.T) {
	prefix := []byte{0x7A} // DATA_PREFIX
	encoded := EncodeUint64(prefix, 12345)
	assert.Equal(t, byte(0x7A), encoded[0])
	assert.Len(t, encoded, 9)

	v, _, err := DecodeUint64(encoded[1:])
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), v)
}
