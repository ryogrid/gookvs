package traits

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestInterfaceCompilability verifies that all interfaces are well-formed
// and that the expected methods are declared.
func TestInterfaceCompilability(t *testing.T) {
	// These are compile-time checks - if the interfaces are malformed,
	// this file won't compile.
	var _ KvEngine = nil
	var _ Snapshot = nil
	var _ WriteBatch = nil
	var _ Iterator = nil
}

func TestErrors(t *testing.T) {
	assert.Equal(t, "engine: key not found", ErrNotFound.Error())
	assert.Equal(t, "engine: column family not found", ErrCFNotFound.Error())
}

func TestIterOptionsDefaults(t *testing.T) {
	opts := IterOptions{}
	assert.Nil(t, opts.LowerBound)
	assert.Nil(t, opts.UpperBound)
	assert.False(t, opts.FillCache)
	assert.False(t, opts.PrefixSameAsStart)
}
