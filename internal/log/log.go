// Package log implements structured logging for gookvs.
// It uses log/slog with a custom LogDispatcher that routes log records
// to different outputs based on attributes (normal, slow, rocksdb, raft).
package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// globalLevel stores the current log level atomically for runtime changes.
var globalLevel atomic.Value

func init() {
	globalLevel.Store(slog.LevelInfo)
}

// SetLevel changes the global log level at runtime.
func SetLevel(level slog.Level) {
	globalLevel.Store(level)
}

// GetLevel returns the current global log level.
func GetLevel() slog.Level {
	return globalLevel.Load().(slog.Level)
}

// ParseLevel converts a string to slog.Level.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "fatal":
		return slog.Level(12) // Above error
	default:
		return slog.LevelInfo
	}
}

// LevelFilter wraps a handler and checks the global level before dispatching.
type LevelFilter struct {
	inner slog.Handler
}

// NewLevelFilter creates a LevelFilter wrapping the given handler.
func NewLevelFilter(inner slog.Handler) *LevelFilter {
	return &LevelFilter{inner: inner}
}

func (f *LevelFilter) Enabled(_ context.Context, level slog.Level) bool {
	return level >= GetLevel()
}

func (f *LevelFilter) Handle(ctx context.Context, r slog.Record) error {
	return f.inner.Handle(ctx, r)
}

func (f *LevelFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LevelFilter{inner: f.inner.WithAttrs(attrs)}
}

func (f *LevelFilter) WithGroup(name string) slog.Handler {
	return &LevelFilter{inner: f.inner.WithGroup(name)}
}

// LogDispatcher routes log records to different handlers based on attributes.
type LogDispatcher struct {
	normal  slog.Handler
	slow    slog.Handler
	rocksdb slog.Handler
	raft    slog.Handler
}

// NewLogDispatcher creates a dispatcher with the given handlers.
func NewLogDispatcher(normal, slow, rocksdb, raft slog.Handler) *LogDispatcher {
	return &LogDispatcher{
		normal:  normal,
		slow:    slow,
		rocksdb: rocksdb,
		raft:    raft,
	}
}

func (d *LogDispatcher) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= GetLevel()
}

func (d *LogDispatcher) Handle(ctx context.Context, r slog.Record) error {
	handler := d.normal

	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "slow_log":
			handler = d.slow
			return false
		case "rocksdb_log":
			handler = d.rocksdb
			return false
		case "raftdb_log":
			handler = d.raft
			return false
		}
		return true
	})

	return handler.Handle(ctx, r)
}

func (d *LogDispatcher) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &LogDispatcher{
		normal:  d.normal.WithAttrs(attrs),
		slow:    d.slow.WithAttrs(attrs),
		rocksdb: d.rocksdb.WithAttrs(attrs),
		raft:    d.raft.WithAttrs(attrs),
	}
}

func (d *LogDispatcher) WithGroup(name string) slog.Handler {
	return &LogDispatcher{
		normal:  d.normal.WithGroup(name),
		slow:    d.slow.WithGroup(name),
		rocksdb: d.rocksdb.WithGroup(name),
		raft:    d.raft.WithGroup(name),
	}
}

// SlowLogHandler wraps a handler with threshold-based filtering.
type SlowLogHandler struct {
	inner     slog.Handler
	threshold time.Duration
}

// NewSlowLogHandler creates a handler that only passes through records
// with a "takes" attribute exceeding the threshold.
func NewSlowLogHandler(inner slog.Handler, threshold time.Duration) *SlowLogHandler {
	return &SlowLogHandler{inner: inner, threshold: threshold}
}

func (h *SlowLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *SlowLogHandler) Handle(ctx context.Context, r slog.Record) error {
	var takes time.Duration
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "takes" {
			takes = a.Value.Duration()
			found = true
			return false
		}
		return true
	})

	if !found || takes <= h.threshold {
		return nil // Suppress: not slow enough or no duration.
	}
	return h.inner.Handle(ctx, r)
}

func (h *SlowLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &SlowLogHandler{inner: h.inner.WithAttrs(attrs), threshold: h.threshold}
}

func (h *SlowLogHandler) WithGroup(name string) slog.Handler {
	return &SlowLogHandler{inner: h.inner.WithGroup(name), threshold: h.threshold}
}

// RotatingFileWriter creates an io.Writer that handles log file rotation.
func RotatingFileWriter(filename string, maxSizeMB, maxBackups, maxAgeDays int) io.Writer {
	return &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    maxSizeMB,
		MaxBackups: maxBackups,
		MaxAge:     maxAgeDays,
		LocalTime:  true,
	}
}

// LogConfig holds logging configuration.
type LogConfig struct {
	Level       string
	Format      string
	Filename    string
	MaxSizeMB   int
	MaxBackups  int
	MaxAgeDays  int
	SlowLogFile string
	SlowLogThreshold time.Duration
}

// Setup initializes the global logger with the given configuration.
func Setup(cfg LogConfig) {
	SetLevel(ParseLevel(cfg.Level))

	// Create normal handler.
	var normalWriter io.Writer = os.Stdout
	if cfg.Filename != "" {
		normalWriter = RotatingFileWriter(cfg.Filename, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays)
	}

	var normalHandler slog.Handler
	if cfg.Format == "json" {
		normalHandler = slog.NewJSONHandler(normalWriter, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else {
		normalHandler = slog.NewTextHandler(normalWriter, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	// Create slow log handler.
	var slowHandler slog.Handler
	threshold := cfg.SlowLogThreshold
	if threshold == 0 {
		threshold = time.Second
	}
	if cfg.SlowLogFile != "" {
		slowWriter := RotatingFileWriter(cfg.SlowLogFile, cfg.MaxSizeMB, cfg.MaxBackups, cfg.MaxAgeDays)
		slowHandler = NewSlowLogHandler(
			slog.NewTextHandler(slowWriter, &slog.HandlerOptions{Level: slog.LevelDebug}),
			threshold,
		)
	} else {
		slowHandler = NewSlowLogHandler(normalHandler, threshold)
	}

	// Create dispatcher (rocksdb and raft share normal handler for now).
	dispatcher := NewLogDispatcher(normalHandler, slowHandler, normalHandler, normalHandler)
	levelFilter := NewLevelFilter(dispatcher)

	slog.SetDefault(slog.New(levelFilter))
}
