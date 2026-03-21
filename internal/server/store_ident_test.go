package server

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIdentSaveLoad(t *testing.T) {
	dir := t.TempDir()
	var expected uint64 = 42

	err := SaveStoreIdent(dir, expected)
	require.NoError(t, err)

	got, err := LoadStoreIdent(dir)
	require.NoError(t, err)
	assert.Equal(t, expected, got)
}

func TestStoreIdentMissing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nonexistent")

	got, err := LoadStoreIdent(dir)
	assert.Error(t, err)
	assert.Equal(t, uint64(0), got)
}

func TestStoreIdentOverwrite(t *testing.T) {
	dir := t.TempDir()

	err := SaveStoreIdent(dir, 100)
	require.NoError(t, err)

	err = SaveStoreIdent(dir, 200)
	require.NoError(t, err)

	got, err := LoadStoreIdent(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(200), got)
}
