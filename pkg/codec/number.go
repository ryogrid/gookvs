package codec

import (
	"encoding/binary"
	"math"
)

const (
	// MaxVarintLen64 is the maximum length of a varint-encoded uint64.
	MaxVarintLen64 = 10
)

// EncodeUint64 encodes a uint64 in big-endian (ascending) order.
func EncodeUint64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

// EncodeUint64Desc encodes a uint64 in descending order (big-endian of ^v).
func EncodeUint64Desc(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], ^v)
	return append(dst, buf[:]...)
}

// DecodeUint64 decodes a big-endian uint64. Returns (value, remaining, error).
func DecodeUint64(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, ErrInsufficientData
	}
	v := binary.BigEndian.Uint64(data[:8])
	return v, data[8:], nil
}

// DecodeUint64Desc decodes a descending uint64. Returns (value, remaining, error).
func DecodeUint64Desc(data []byte) (uint64, []byte, error) {
	if len(data) < 8 {
		return 0, nil, ErrInsufficientData
	}
	v := ^binary.BigEndian.Uint64(data[:8])
	return v, data[8:], nil
}

// EncodeInt64 encodes an int64 in comparable ascending order.
// The sign bit is flipped so that negative numbers sort before positive ones.
func EncodeInt64(dst []byte, v int64) []byte {
	return EncodeUint64(dst, encodeInt64ToComparable(v))
}

// DecodeInt64 decodes a comparable-encoded int64.
func DecodeInt64(data []byte) (int64, []byte, error) {
	u, rest, err := DecodeUint64(data)
	if err != nil {
		return 0, nil, err
	}
	return decodeComparableToInt64(u), rest, nil
}

// EncodeFloat64 encodes a float64 in comparable ascending order.
func EncodeFloat64(dst []byte, v float64) []byte {
	return EncodeUint64(dst, encodeFloat64ToComparable(v))
}

// DecodeFloat64 decodes a comparable-encoded float64.
func DecodeFloat64(data []byte) (float64, []byte, error) {
	u, rest, err := DecodeUint64(data)
	if err != nil {
		return 0, nil, err
	}
	return decodeComparableToFloat64(u), rest, nil
}

// EncodeVarint encodes a uint64 using unsigned LEB128 variable-length encoding.
func EncodeVarint(dst []byte, v uint64) []byte {
	var buf [MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

// DecodeVarint decodes a LEB128 variable-length uint64.
// Returns (value, remaining, error).
func DecodeVarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n == 0 {
		return 0, nil, ErrInsufficientData
	}
	if n < 0 {
		// Overflow
		return 0, nil, ErrInvalidEncodedKey
	}
	return v, data[n:], nil
}

// EncodeSignedVarint encodes an int64 using signed LEB128 variable-length encoding.
func EncodeSignedVarint(dst []byte, v int64) []byte {
	var buf [MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	return append(dst, buf[:n]...)
}

// DecodeSignedVarint decodes a signed LEB128 variable-length int64.
func DecodeSignedVarint(data []byte) (int64, []byte, error) {
	v, n := binary.Varint(data)
	if n == 0 {
		return 0, nil, ErrInsufficientData
	}
	if n < 0 {
		return 0, nil, ErrInvalidEncodedKey
	}
	return v, data[n:], nil
}

// encodeInt64ToComparable flips the sign bit so that negative numbers sort
// before positive ones in unsigned byte comparison.
func encodeInt64ToComparable(v int64) uint64 {
	return uint64(v) ^ (1 << 63)
}

// decodeComparableToInt64 reverses encodeInt64ToComparable.
func decodeComparableToInt64(u uint64) int64 {
	return int64(u ^ (1 << 63))
}

// encodeFloat64ToComparable converts a float64 to a uint64 that preserves
// ordering under unsigned comparison.
func encodeFloat64ToComparable(v float64) uint64 {
	bits := math.Float64bits(v)
	if bits&(1<<63) == 0 {
		// Positive (including +0): flip sign bit
		bits ^= 1 << 63
	} else {
		// Negative (including -0): flip all bits
		bits = ^bits
	}
	return bits
}

// decodeComparableToFloat64 reverses encodeFloat64ToComparable.
func decodeComparableToFloat64(u uint64) float64 {
	if u&(1<<63) != 0 {
		// Was positive: flip sign bit back
		u ^= 1 << 63
	} else {
		// Was negative: flip all bits back
		u = ^u
	}
	return math.Float64frombits(u)
}
