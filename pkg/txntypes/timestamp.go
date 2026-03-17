// Package txntypes defines transaction type definitions for gookvs.
// Lock, Write, and Mutation structs with binary serialization that is
// byte-identical to TiKV's format.
package txntypes

import "math"

// TimeStamp represents a hybrid logical clock value from PD's TSO.
type TimeStamp uint64

const (
	// TSLogicalBits is the number of bits used for the logical component.
	TSLogicalBits = 18
	// TSMax is the maximum possible timestamp.
	TSMax = TimeStamp(math.MaxUint64)
	// TSZero is the zero timestamp.
	TSZero = TimeStamp(0)
)

// Physical returns the physical (millisecond) component.
func (ts TimeStamp) Physical() int64 {
	return int64(ts >> TSLogicalBits)
}

// Logical returns the logical (sequence) component.
func (ts TimeStamp) Logical() int64 {
	return int64(ts & ((1 << TSLogicalBits) - 1))
}

// ComposeTS creates a TimeStamp from physical and logical components.
func ComposeTS(physical int64, logical int64) TimeStamp {
	return TimeStamp(uint64(physical)<<TSLogicalBits | uint64(logical))
}

// IsZero returns true if the timestamp is zero (unset).
func (ts TimeStamp) IsZero() bool {
	return ts == TSZero
}

// Prev returns the immediately preceding timestamp.
func (ts TimeStamp) Prev() TimeStamp {
	if ts == TSZero {
		return TSZero
	}
	return ts - 1
}

// Next returns the immediately following timestamp.
func (ts TimeStamp) Next() TimeStamp {
	if ts == TSMax {
		return TSMax
	}
	return ts + 1
}
