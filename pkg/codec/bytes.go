// Package codec provides memcomparable encoding and number encoding functions.
// These encodings are byte-identical to TiKV's for wire and storage compatibility.
package codec

import (
	"errors"
)

const (
	encGroupSize = 8
	encMarker    = byte(0xFF)
	encPad       = byte(0x00)
)

var (
	// ErrInsufficientData is returned when the input data is too short.
	ErrInsufficientData = errors.New("codec: insufficient data")
	// ErrInvalidEncodedKey is returned when the encoded key is malformed.
	ErrInvalidEncodedKey = errors.New("codec: invalid encoded key")
)

// EncodedBytesLength returns the length of the encoded output for the given input length.
func EncodedBytesLength(srcLen int) int {
	return (srcLen/encGroupSize + 1) * (encGroupSize + 1)
}

// EncodeBytes encodes a byte slice using memcomparable encoding (ascending order).
// The encoded output preserves lexicographic ordering under byte comparison.
// dst is the buffer to append to (may be nil for a new allocation).
func EncodeBytes(dst []byte, data []byte) []byte {
	dLen := EncodedBytesLength(len(data))
	result := reallocBytes(dst, dLen)

	idx := 0
	for len(data) >= encGroupSize {
		copy(result[len(dst)+idx:], data[:encGroupSize])
		result[len(dst)+idx+encGroupSize] = encMarker
		data = data[encGroupSize:]
		idx += encGroupSize + 1
	}

	// Final group: pad with zeros
	padCount := encGroupSize - len(data)
	copy(result[len(dst)+idx:], data)
	// Zero-fill remainder
	for i := len(data); i < encGroupSize; i++ {
		result[len(dst)+idx+i] = encPad
	}
	result[len(dst)+idx+encGroupSize] = encMarker - byte(padCount)

	return result
}

// EncodeBytesDesc encodes a byte slice using memcomparable encoding (descending order).
// Every byte of the ascending encoding is bitwise-inverted.
func EncodeBytesDesc(dst []byte, data []byte) []byte {
	result := EncodeBytes(dst, data)
	// Invert only the newly appended bytes
	start := len(dst)
	for i := start; i < len(result); i++ {
		result[i] = ^result[i]
	}
	return result
}

// DecodeBytes decodes a memcomparable-encoded byte slice (ascending order).
// Returns (decoded, remaining, error) where remaining is the unconsumed input.
func DecodeBytes(data []byte) ([]byte, []byte, error) {
	result := make([]byte, 0)

	for {
		if len(data) < encGroupSize+1 {
			return nil, nil, ErrInsufficientData
		}

		group := data[:encGroupSize]
		marker := data[encGroupSize]
		data = data[encGroupSize+1:]

		if marker == encMarker {
			// Full group, no padding
			result = append(result, group...)
		} else {
			// Final group
			padCount := int(encMarker - marker)
			if padCount > encGroupSize {
				return nil, nil, ErrInvalidEncodedKey
			}
			realLen := encGroupSize - padCount
			result = append(result, group[:realLen]...)
			// Verify padding bytes are zeros
			for i := realLen; i < encGroupSize; i++ {
				if group[i] != encPad {
					return nil, nil, ErrInvalidEncodedKey
				}
			}
			return result, data, nil
		}
	}
}

// DecodeBytesDesc decodes a descending memcomparable-encoded byte slice.
// Returns (decoded, remaining, error).
func DecodeBytesDesc(data []byte) ([]byte, []byte, error) {
	// Invert as we go to avoid allocating a full copy.
	result := make([]byte, 0)
	for {
		if len(data) < encGroupSize+1 {
			return nil, nil, ErrInsufficientData
		}

		// Invert the group and marker
		var group [encGroupSize]byte
		for i := 0; i < encGroupSize; i++ {
			group[i] = ^data[i]
		}
		marker := ^data[encGroupSize]
		data = data[encGroupSize+1:]

		if marker == encMarker {
			result = append(result, group[:]...)
		} else {
			padCount := int(encMarker - marker)
			if padCount > encGroupSize {
				return nil, nil, ErrInvalidEncodedKey
			}
			realLen := encGroupSize - padCount
			result = append(result, group[:realLen]...)
			for i := realLen; i < encGroupSize; i++ {
				if group[i] != encPad {
					return nil, nil, ErrInvalidEncodedKey
				}
			}
			return result, data, nil
		}
	}
}

// reallocBytes ensures the result slice has capacity for additional n bytes.
func reallocBytes(b []byte, n int) []byte {
	if cap(b)-len(b) >= n {
		return b[:len(b)+n]
	}
	newCap := len(b) + n
	if newCap < 2*cap(b) {
		newCap = 2 * cap(b)
	}
	nb := make([]byte, len(b)+n, newCap)
	copy(nb, b)
	return nb
}
