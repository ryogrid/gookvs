// Package config implements the configuration system for gookv.
// It supports TOML-based configuration loading, validation, and runtime changes.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// Duration is a wrapper around time.Duration that supports TOML unmarshaling.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(d.Duration.String()), nil
}

// ReadableSize represents a human-readable size (e.g., "1GB", "256MB").
type ReadableSize uint64

const (
	B  ReadableSize = 1
	KB ReadableSize = 1024
	MB ReadableSize = 1024 * KB
	GB ReadableSize = 1024 * MB
)

func (s *ReadableSize) UnmarshalText(text []byte) error {
	str := strings.TrimSpace(string(text))
	if len(str) == 0 {
		return errors.New("empty size string")
	}

	var multiplier ReadableSize = 1
	upper := strings.ToUpper(str)

	switch {
	case strings.HasSuffix(upper, "GB"):
		multiplier = GB
		str = str[:len(str)-2]
	case strings.HasSuffix(upper, "MB"):
		multiplier = MB
		str = str[:len(str)-2]
	case strings.HasSuffix(upper, "KB"):
		multiplier = KB
		str = str[:len(str)-2]
	case strings.HasSuffix(upper, "B"):
		multiplier = B
		str = str[:len(str)-1]
	}

	var val uint64
	_, err := fmt.Sscanf(str, "%d", &val)
	if err != nil {
		return fmt.Errorf("invalid size %q: %w", string(text), err)
	}
	*s = ReadableSize(val) * multiplier
	return nil
}

func (s ReadableSize) MarshalText() ([]byte, error) {
	switch {
	case s >= GB && s%GB == 0:
		return []byte(fmt.Sprintf("%dGB", s/GB)), nil
	case s >= MB && s%MB == 0:
		return []byte(fmt.Sprintf("%dMB", s/MB)), nil
	case s >= KB && s%KB == 0:
		return []byte(fmt.Sprintf("%dKB", s/KB)), nil
	default:
		return []byte(fmt.Sprintf("%dB", s)), nil
	}
}

// Config is the root configuration struct for gookv.
type Config struct {
	Log            LogConfig        `toml:"log"`
	Server         ServerConfig     `toml:"server"`
	Storage        StorageConfig    `toml:"storage"`
	PD             PDConfig         `toml:"pd"`
	RaftStore      RaftStoreConfig  `toml:"raft-store"`
	Coprocessor    CoprocessorConfig `toml:"coprocessor"`
	PessimisticTxn PessimisticTxnConfig `toml:"pessimistic-txn"`
	SlowLogFile    string           `toml:"slow-log-file"`
	SlowLogThreshold Duration       `toml:"slow-log-threshold"`
}

// LogConfig controls logging behavior.
type LogConfig struct {
	Level      string `toml:"level"`
	Format     string `toml:"format"`
	File       LogFileConfig `toml:"file"`
	EnableTimestamp bool `toml:"enable-timestamp"`
}

// LogFileConfig controls log file rotation.
type LogFileConfig struct {
	Filename   string `toml:"filename"`
	MaxSize    int    `toml:"max-size"`    // MB
	MaxDays    int    `toml:"max-days"`
	MaxBackups int    `toml:"max-backups"`
}

// ServerConfig controls the gRPC server.
type ServerConfig struct {
	Addr           string       `toml:"addr"`
	StatusAddr     string       `toml:"status-addr"`
	GRPCConcurrency int        `toml:"grpc-concurrency"`
	GRPCMaxRecvMsgSize ReadableSize `toml:"grpc-max-recv-msg-size"`
	GRPCMaxSendMsgSize ReadableSize `toml:"grpc-max-send-msg-size"`
	ClusterID      uint64       `toml:"cluster-id"`
}

// StorageConfig controls the storage engine.
type StorageConfig struct {
	DataDir        string       `toml:"data-dir"`
	ReserveSpace   ReadableSize `toml:"reserve-space"`
	BlockCacheSize ReadableSize `toml:"block-cache-size"`
	SchedulerConcurrency int   `toml:"scheduler-concurrency"`
}

// PDConfig controls the PD client.
type PDConfig struct {
	Endpoints []string `toml:"endpoints"`
}

// RaftStoreConfig controls the raft store.
type RaftStoreConfig struct {
	RaftBaseTickInterval     Duration `toml:"raft-base-tick-interval"`
	RaftHeartbeatTicks       int      `toml:"raft-heartbeat-ticks"`
	RaftElectionTimeoutTicks int      `toml:"raft-election-timeout-ticks"`
	RegionMaxSize            ReadableSize `toml:"region-max-size"`
	RegionSplitSize          ReadableSize `toml:"region-split-size"`
	RaftLogGCThreshold       uint64   `toml:"raft-log-gc-threshold"`
	RaftLogGCCountLimit      uint64   `toml:"raft-log-gc-count-limit"`
	SplitCheckTickInterval   Duration `toml:"split-check-tick-interval"`
}

// CoprocessorConfig controls the coprocessor.
type CoprocessorConfig struct {
	RegionMaxKeys   uint64       `toml:"region-max-keys"`
	RegionSplitKeys uint64       `toml:"region-split-keys"`
	SplitRegionOnTable bool     `toml:"split-region-on-table"`
}

// PessimisticTxnConfig controls pessimistic transaction behavior.
type PessimisticTxnConfig struct {
	WaitForLockTimeout Duration `toml:"wait-for-lock-timeout"`
	WakeUpDelayDuration Duration `toml:"wake-up-delay-duration"`
	Pipelined          bool      `toml:"pipelined"`
}

// DefaultConfig returns a Config with sensible default values.
func DefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			Level:  "info",
			Format: "text",
			File: LogFileConfig{
				Filename:   "server.log", // resolved to <data-dir>/log/server.log in main
				MaxSize:    300,
				MaxDays:    0,
				MaxBackups: 0,
			},
			EnableTimestamp: true,
		},
		Server: ServerConfig{
			Addr:               "127.0.0.1:20160",
			StatusAddr:         "127.0.0.1:20180",
			GRPCConcurrency:    4,
			GRPCMaxRecvMsgSize: 16 * MB,
			GRPCMaxSendMsgSize: 16 * MB,
		},
		Storage: StorageConfig{
			DataDir:              "/tmp/gookv/data",
			ReserveSpace:         5 * GB,
			BlockCacheSize:       0, // Auto-size (45% of system memory)
			SchedulerConcurrency: 2048,
		},
		PD: PDConfig{
			Endpoints: []string{"127.0.0.1:2379"},
		},
		RaftStore: RaftStoreConfig{
			RaftBaseTickInterval:     Duration{time.Second},
			RaftHeartbeatTicks:       2,
			RaftElectionTimeoutTicks: 10,
			RegionMaxSize:            144 * MB,
			RegionSplitSize:          96 * MB,
			RaftLogGCThreshold:       50,
			RaftLogGCCountLimit:      72000,
			SplitCheckTickInterval:   Duration{10 * time.Second},
		},
		Coprocessor: CoprocessorConfig{
			RegionMaxKeys:      1440000,
			RegionSplitKeys:    960000,
			SplitRegionOnTable: false,
		},
		PessimisticTxn: PessimisticTxnConfig{
			WaitForLockTimeout:  Duration{time.Second},
			WakeUpDelayDuration: Duration{20 * time.Millisecond},
			Pipelined:           true,
		},
		SlowLogFile:      "",
		SlowLogThreshold: Duration{time.Second},
	}
}

// LoadFromFile loads configuration from a TOML file.
// Values not specified in the file use defaults.
func LoadFromFile(path string) (*Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if _, err := toml.Decode(string(data), cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return cfg, nil
}

// Validate checks the configuration for invalid values.
func (c *Config) Validate() error {
	var errs []error

	// Validate log level.
	switch strings.ToLower(c.Log.Level) {
	case "debug", "info", "warn", "error", "fatal":
		// valid
	default:
		errs = append(errs, fmt.Errorf("invalid log level %q (must be debug/info/warn/error/fatal)", c.Log.Level))
	}

	// Validate log format.
	switch strings.ToLower(c.Log.Format) {
	case "text", "json":
		// valid
	default:
		errs = append(errs, fmt.Errorf("invalid log format %q (must be text/json)", c.Log.Format))
	}

	// Validate server address.
	if c.Server.Addr == "" {
		errs = append(errs, errors.New("server addr must not be empty"))
	}

	// Validate storage data dir.
	if c.Storage.DataDir == "" {
		errs = append(errs, errors.New("storage data-dir must not be empty"))
	}

	// Validate PD endpoints.
	if len(c.PD.Endpoints) == 0 {
		errs = append(errs, errors.New("pd endpoints must not be empty"))
	}

	// Validate raft store.
	if c.RaftStore.RaftHeartbeatTicks <= 0 {
		errs = append(errs, errors.New("raft-heartbeat-ticks must be positive"))
	}
	if c.RaftStore.RaftElectionTimeoutTicks <= c.RaftStore.RaftHeartbeatTicks {
		errs = append(errs, errors.New("raft-election-timeout-ticks must be greater than raft-heartbeat-ticks"))
	}
	if c.RaftStore.RegionSplitSize > c.RaftStore.RegionMaxSize {
		errs = append(errs, errors.New("region-split-size must not be greater than region-max-size"))
	}

	// Validate coprocessor.
	if c.Coprocessor.RegionSplitKeys > c.Coprocessor.RegionMaxKeys {
		errs = append(errs, errors.New("region-split-keys must not be greater than region-max-keys"))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// SaveToFile persists the configuration to a TOML file.
func (c *Config) SaveToFile(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer f.Close()

	encoder := toml.NewEncoder(f)
	return encoder.Encode(c)
}

// Clone returns a deep copy of the config.
func (c *Config) Clone() *Config {
	clone := *c
	// Deep copy slices.
	if c.PD.Endpoints != nil {
		clone.PD.Endpoints = make([]string, len(c.PD.Endpoints))
		copy(clone.PD.Endpoints, c.PD.Endpoints)
	}
	return &clone
}

// ConfigChange represents a set of configuration field changes.
type ConfigChange map[string]interface{}

// Diff computes the differences between two configs.
// Returns a ConfigChange containing only the changed fields.
func Diff(a, b *Config) ConfigChange {
	changes := make(ConfigChange)
	diffStructs(reflect.ValueOf(a).Elem(), reflect.ValueOf(b).Elem(), "", changes)
	return changes
}

func diffStructs(a, b reflect.Value, prefix string, changes ConfigChange) {
	t := a.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		fieldA := a.Field(i)
		fieldB := b.Field(i)

		key := field.Name
		if tag := field.Tag.Get("toml"); tag != "" {
			parts := strings.Split(tag, ",")
			key = parts[0]
		}
		if prefix != "" {
			key = prefix + "." + key
		}

		if field.Type.Kind() == reflect.Struct && field.Type != reflect.TypeOf(Duration{}) && field.Type != reflect.TypeOf(ReadableSize(0)) {
			diffStructs(fieldA, fieldB, key, changes)
			continue
		}

		if !reflect.DeepEqual(fieldA.Interface(), fieldB.Interface()) {
			changes[key] = fieldB.Interface()
		}
	}
}
