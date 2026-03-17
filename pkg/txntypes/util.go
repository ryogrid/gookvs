package txntypes

import (
	"encoding/binary"
	"io"
)

// appendUint64BE appends a uint64 in big-endian format.
func appendUint64BE(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

// appendVarint appends a varint-encoded uint64.
func appendVarint(b []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(b, buf[:n]...)
}

// appendVarBytes appends a varint-length-prefixed byte slice.
func appendVarBytes(b []byte, data []byte) []byte {
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

// readVarint reads a varint from data. Returns (value, remaining, error).
func readVarint(data []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(data)
	if n == 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return v, data[n:], nil
}

// readVarBytes reads a varint-length-prefixed byte slice.
func readVarBytes(data []byte) ([]byte, []byte, error) {
	length, data, err := readVarint(data)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(data)) < length {
		return nil, nil, io.ErrUnexpectedEOF
	}
	result := make([]byte, length)
	copy(result, data[:length])
	return result, data[length:], nil
}
