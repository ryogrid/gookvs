package codec

import (
	"bytes"
	"math"
	"testing"
)

// --- Bytes codec fuzz tests ---

func FuzzEncodeDecodeBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}) // 8-byte aligned
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}) // 9-byte crosses group
	f.Add([]byte("hello world"))
	f.Add(bytes.Repeat([]byte{0xFF}, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		encoded := EncodeBytes(nil, data)
		decoded, remaining, err := DecodeBytes(encoded)
		if err != nil {
			t.Fatalf("DecodeBytes failed: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(remaining))
		}
		if !bytes.Equal(data, decoded) {
			t.Fatalf("round-trip mismatch: got %x, want %x", decoded, data)
		}
	})
}

func FuzzEncodeBytesDesc(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte("test data"))

	f.Fuzz(func(t *testing.T, data []byte) {
		encoded := EncodeBytesDesc(nil, data)
		decoded, remaining, err := DecodeBytesDesc(encoded)
		if err != nil {
			t.Fatalf("DecodeBytesDesc failed: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(remaining))
		}
		if !bytes.Equal(data, decoded) {
			t.Fatalf("round-trip mismatch: got %x, want %x", decoded, data)
		}
	})
}

// --- Number codec fuzz tests ---

func FuzzEncodeDecodeUint64(f *testing.F) {
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(math.MaxUint64))
	f.Add(uint64(math.MaxUint32))
	f.Add(uint64(0x0102030405060708))

	f.Fuzz(func(t *testing.T, v uint64) {
		encoded := EncodeUint64(nil, v)
		decoded, remaining, err := DecodeUint64(encoded)
		if err != nil {
			t.Fatalf("DecodeUint64 failed: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(remaining))
		}
		if decoded != v {
			t.Fatalf("round-trip mismatch: got %d, want %d", decoded, v)
		}
	})
}

func FuzzEncodeDecodeInt64(f *testing.F) {
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(-1))
	f.Add(int64(math.MinInt64))
	f.Add(int64(math.MaxInt64))

	f.Fuzz(func(t *testing.T, v int64) {
		encoded := EncodeInt64(nil, v)
		decoded, remaining, err := DecodeInt64(encoded)
		if err != nil {
			t.Fatalf("DecodeInt64 failed: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(remaining))
		}
		if decoded != v {
			t.Fatalf("round-trip mismatch: got %d, want %d", decoded, v)
		}
	})
}

func FuzzEncodeDecodeFloat64(f *testing.F) {
	f.Add(float64(0))
	f.Add(float64(-0))
	f.Add(float64(1.0))
	f.Add(float64(-1.0))
	f.Add(math.Inf(1))
	f.Add(math.Inf(-1))
	f.Add(math.SmallestNonzeroFloat64)
	f.Add(math.MaxFloat64)

	f.Fuzz(func(t *testing.T, v float64) {
		if math.IsNaN(v) {
			return // NaN != NaN, skip
		}
		encoded := EncodeFloat64(nil, v)
		decoded, remaining, err := DecodeFloat64(encoded)
		if err != nil {
			t.Fatalf("DecodeFloat64 failed: %v", err)
		}
		if len(remaining) != 0 {
			t.Fatalf("unexpected remaining bytes: %d", len(remaining))
		}
		if decoded != v {
			t.Fatalf("round-trip mismatch: got %g, want %g", decoded, v)
		}
	})
}

// FuzzOrderPreservation verifies that EncodeBytes preserves lexicographic ordering.
func FuzzOrderPreservation(f *testing.F) {
	f.Add([]byte("a"), []byte("b"))
	f.Add([]byte{}, []byte{0x00})
	f.Add([]byte{0x00}, []byte{0x01})
	f.Add([]byte{0xFF}, []byte{0xFF, 0x00})

	f.Fuzz(func(t *testing.T, a, b []byte) {
		ea := EncodeBytes(nil, a)
		eb := EncodeBytes(nil, b)
		rawCmp := bytes.Compare(a, b)
		encCmp := bytes.Compare(ea, eb)
		if rawCmp != encCmp {
			t.Fatalf("order not preserved: Compare(%x,%x)=%d but Compare(encoded)=%d",
				a, b, rawCmp, encCmp)
		}
	})
}
