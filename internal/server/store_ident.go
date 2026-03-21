package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const storeIdentFile = "store_ident"

// SaveStoreIdent persists the store ID to a file in the data directory.
func SaveStoreIdent(dataDir string, storeID uint64) error {
	p := filepath.Join(dataDir, storeIdentFile)
	data := []byte(strconv.FormatUint(storeID, 10))
	return os.WriteFile(p, data, 0644)
}

// LoadStoreIdent reads the persisted store ID from the data directory.
// Returns 0, error if the file does not exist.
func LoadStoreIdent(dataDir string) (uint64, error) {
	p := filepath.Join(dataDir, storeIdentFile)
	data, err := os.ReadFile(p)
	if err != nil {
		return 0, fmt.Errorf("load store ident: %w", err)
	}
	id, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse store ident: %w", err)
	}
	return id, nil
}
