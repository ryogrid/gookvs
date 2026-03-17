package cfnames

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstants(t *testing.T) {
	// Verify exact string values match TiKV
	assert.Equal(t, "default", CFDefault)
	assert.Equal(t, "lock", CFLock)
	assert.Equal(t, "write", CFWrite)
	assert.Equal(t, "raft", CFRaft)
}

func TestDataCFs(t *testing.T) {
	assert.Len(t, DataCFs, 3)
	assert.Contains(t, DataCFs, CFDefault)
	assert.Contains(t, DataCFs, CFLock)
	assert.Contains(t, DataCFs, CFWrite)
	assert.NotContains(t, DataCFs, CFRaft)
}

func TestAllCFs(t *testing.T) {
	assert.Len(t, AllCFs, 4)
	assert.Contains(t, AllCFs, CFDefault)
	assert.Contains(t, AllCFs, CFLock)
	assert.Contains(t, AllCFs, CFWrite)
	assert.Contains(t, AllCFs, CFRaft)
}
