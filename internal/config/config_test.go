package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
	assert.Equal(t, "127.0.0.1:20160", cfg.Server.Addr)
	assert.Equal(t, "127.0.0.1:20180", cfg.Server.StatusAddr)
	assert.Equal(t, ReadableSize(16*MB), cfg.Server.GRPCMaxRecvMsgSize)
	assert.Equal(t, "/tmp/gookv/data", cfg.Storage.DataDir)
	assert.Equal(t, []string{"127.0.0.1:2379"}, cfg.PD.Endpoints)
	assert.Equal(t, 2, cfg.RaftStore.RaftHeartbeatTicks)
	assert.Equal(t, 10, cfg.RaftStore.RaftElectionTimeoutTicks)
	assert.Equal(t, ReadableSize(144*MB), cfg.RaftStore.RegionMaxSize)
	assert.Equal(t, time.Second, cfg.SlowLogThreshold.Duration)
}

func TestValidateDefault(t *testing.T) {
	cfg := DefaultConfig()
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateInvalidLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Log.Level = "invalid"
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level")
}

func TestValidateInvalidLogFormat(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Log.Format = "yaml"
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log format")
}

func TestValidateEmptyAddr(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.Addr = ""
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "server addr must not be empty")
}

func TestValidateEmptyDataDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.DataDir = ""
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "storage data-dir must not be empty")
}

func TestValidateEmptyPDEndpoints(t *testing.T) {
	// Empty PD endpoints are valid (standalone mode).
	cfg := DefaultConfig()
	cfg.PD.Endpoints = nil
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestValidateRaftTimers(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RaftStore.RaftElectionTimeoutTicks = 1
	cfg.RaftStore.RaftHeartbeatTicks = 5
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "raft-election-timeout-ticks must be greater than raft-heartbeat-ticks")
}

func TestValidateRegionSizes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RaftStore.RegionSplitSize = 200 * MB
	cfg.RaftStore.RegionMaxSize = 100 * MB
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region-split-size must not be greater than region-max-size")
}

func TestValidateRegionKeys(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Coprocessor.RegionSplitKeys = 2000000
	cfg.Coprocessor.RegionMaxKeys = 1000000
	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region-split-keys must not be greater than region-max-keys")
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.toml")

	content := `
[log]
level = "debug"
format = "json"

[server]
addr = "0.0.0.0:20160"
status-addr = "0.0.0.0:20180"
grpc-concurrency = 8

[storage]
data-dir = "/data/gookv"

[pd]
endpoints = ["10.0.0.1:2379", "10.0.0.2:2379"]

[raft-store]
raft-base-tick-interval = "2s"
raft-heartbeat-ticks = 3
raft-election-timeout-ticks = 15
region-max-size = "256MB"
region-split-size = "192MB"

[coprocessor]
region-max-keys = 2000000
region-split-keys = 1500000

[pessimistic-txn]
wait-for-lock-timeout = "5s"
pipelined = false
`

	err := os.WriteFile(path, []byte(content), 0o644)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)

	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, "0.0.0.0:20160", cfg.Server.Addr)
	assert.Equal(t, 8, cfg.Server.GRPCConcurrency)
	assert.Equal(t, "/data/gookv", cfg.Storage.DataDir)
	assert.Equal(t, []string{"10.0.0.1:2379", "10.0.0.2:2379"}, cfg.PD.Endpoints)
	assert.Equal(t, 2*time.Second, cfg.RaftStore.RaftBaseTickInterval.Duration)
	assert.Equal(t, 3, cfg.RaftStore.RaftHeartbeatTicks)
	assert.Equal(t, 15, cfg.RaftStore.RaftElectionTimeoutTicks)
	assert.Equal(t, ReadableSize(256*MB), cfg.RaftStore.RegionMaxSize)
	assert.Equal(t, ReadableSize(192*MB), cfg.RaftStore.RegionSplitSize)
	assert.Equal(t, uint64(2000000), cfg.Coprocessor.RegionMaxKeys)
	assert.Equal(t, 5*time.Second, cfg.PessimisticTxn.WaitForLockTimeout.Duration)
	assert.Equal(t, false, cfg.PessimisticTxn.Pipelined)
}

func TestLoadFromFileMissing(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/config.toml")
	assert.Error(t, err)
}

func TestLoadFromFileMalformed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.toml")
	err := os.WriteFile(path, []byte("this is not [valid toml"), 0o644)
	require.NoError(t, err)

	_, err = LoadFromFile(path)
	assert.Error(t, err)
}

func TestSaveToFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.toml")

	cfg := DefaultConfig()
	cfg.Log.Level = "warn"
	cfg.Storage.DataDir = "/custom/data"

	err := cfg.SaveToFile(path)
	require.NoError(t, err)

	// Reload and verify.
	loaded, err := LoadFromFile(path)
	require.NoError(t, err)
	assert.Equal(t, "warn", loaded.Log.Level)
	assert.Equal(t, "/custom/data", loaded.Storage.DataDir)
}

func TestClone(t *testing.T) {
	cfg := DefaultConfig()
	clone := cfg.Clone()

	// Modify clone and ensure original is unaffected.
	clone.Log.Level = "error"
	clone.PD.Endpoints = append(clone.PD.Endpoints, "extra")

	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, 1, len(cfg.PD.Endpoints))
}

func TestDiff(t *testing.T) {
	a := DefaultConfig()
	b := a.Clone()

	b.Log.Level = "debug"
	b.Server.Addr = "0.0.0.0:20160"

	changes := Diff(a, b)
	assert.Contains(t, changes, "log.level")
	assert.Contains(t, changes, "server.addr")
	assert.Equal(t, "debug", changes["log.level"])
	assert.Equal(t, "0.0.0.0:20160", changes["server.addr"])
}

func TestDiffNoChanges(t *testing.T) {
	a := DefaultConfig()
	b := a.Clone()

	changes := Diff(a, b)
	assert.Empty(t, changes)
}

func TestReadableSize(t *testing.T) {
	tests := []struct {
		input    string
		expected ReadableSize
	}{
		{"1B", 1},
		{"1KB", 1024},
		{"1MB", 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"256MB", 256 * 1024 * 1024},
		{"16MB", 16 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var s ReadableSize
			err := s.UnmarshalText([]byte(tt.input))
			require.NoError(t, err)
			assert.Equal(t, tt.expected, s)
		})
	}
}

func TestReadableSizeMarshal(t *testing.T) {
	tests := []struct {
		size     ReadableSize
		expected string
	}{
		{1 * GB, "1GB"},
		{256 * MB, "256MB"},
		{4 * KB, "4KB"},
		{100, "100B"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			text, err := tt.size.MarshalText()
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(text))
		})
	}
}

func TestDuration(t *testing.T) {
	var d Duration
	err := d.UnmarshalText([]byte("5s"))
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, d.Duration)

	text, err := d.MarshalText()
	require.NoError(t, err)
	assert.Equal(t, "5s", string(text))
}

func TestMultipleValidationErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Log.Level = "invalid"
	cfg.Server.Addr = ""
	cfg.Storage.DataDir = ""
	cfg.PD.Endpoints = nil

	err := cfg.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid log level")
	assert.Contains(t, err.Error(), "server addr")
	assert.Contains(t, err.Error(), "data-dir")
	// PD endpoints are optional (standalone mode), so no error expected.
}

func TestLoadFromFileDefaults(t *testing.T) {
	// A minimal config file should have defaults filled in.
	dir := t.TempDir()
	path := filepath.Join(dir, "minimal.toml")

	content := `
[log]
level = "warn"
`
	err := os.WriteFile(path, []byte(content), 0o644)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)

	// Specified value.
	assert.Equal(t, "warn", cfg.Log.Level)
	// Default values still present.
	assert.Equal(t, "127.0.0.1:20160", cfg.Server.Addr)
	assert.Equal(t, "/tmp/gookv/data", cfg.Storage.DataDir)
}
