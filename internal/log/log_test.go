package log

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseLevelValid(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"DEBUG", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"fatal", slog.Level(12)},
		{"unknown", slog.LevelInfo}, // fallback
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, ParseLevel(tt.input))
		})
	}
}

func TestSetGetLevel(t *testing.T) {
	original := GetLevel()
	defer SetLevel(original)

	SetLevel(slog.LevelDebug)
	assert.Equal(t, slog.LevelDebug, GetLevel())

	SetLevel(slog.LevelError)
	assert.Equal(t, slog.LevelError, GetLevel())
}

func TestLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := NewLevelFilter(inner)

	original := GetLevel()
	defer SetLevel(original)

	SetLevel(slog.LevelWarn)

	// Debug should be filtered out.
	assert.False(t, filter.Enabled(context.Background(), slog.LevelDebug))
	// Warn and above should pass.
	assert.True(t, filter.Enabled(context.Background(), slog.LevelWarn))
	assert.True(t, filter.Enabled(context.Background(), slog.LevelError))
}

func TestLevelFilterWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := NewLevelFilter(inner)

	filtered := filter.WithAttrs([]slog.Attr{slog.String("component", "test")})
	assert.NotNil(t, filtered)
	_, ok := filtered.(*LevelFilter)
	assert.True(t, ok)
}

func TestLevelFilterWithGroup(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := NewLevelFilter(inner)

	filtered := filter.WithGroup("test")
	assert.NotNil(t, filtered)
	_, ok := filtered.(*LevelFilter)
	assert.True(t, ok)
}

func TestLogDispatcherNormal(t *testing.T) {
	var normalBuf, slowBuf, rocksBuf, raftBuf bytes.Buffer

	normalHandler := slog.NewTextHandler(&normalBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := slog.NewTextHandler(&slowBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	rocksHandler := slog.NewTextHandler(&rocksBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	raftHandler := slog.NewTextHandler(&raftBuf, &slog.HandlerOptions{Level: slog.LevelDebug})

	dispatcher := NewLogDispatcher(normalHandler, slowHandler, rocksHandler, raftHandler)

	original := GetLevel()
	defer SetLevel(original)
	SetLevel(slog.LevelDebug)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "normal message", 0)
	err := dispatcher.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, normalBuf.String(), "normal message")
	assert.Empty(t, slowBuf.String())
	assert.Empty(t, rocksBuf.String())
	assert.Empty(t, raftBuf.String())
}

func TestLogDispatcherSlowLog(t *testing.T) {
	var normalBuf, slowBuf bytes.Buffer

	normalHandler := slog.NewTextHandler(&normalBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := slog.NewTextHandler(&slowBuf, &slog.HandlerOptions{Level: slog.LevelDebug})

	dispatcher := NewLogDispatcher(normalHandler, slowHandler, normalHandler, normalHandler)

	original := GetLevel()
	defer SetLevel(original)
	SetLevel(slog.LevelDebug)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "slow operation", 0)
	r.AddAttrs(slog.Bool("slow_log", true))
	err := dispatcher.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, slowBuf.String(), "slow operation")
}

func TestLogDispatcherRocksDB(t *testing.T) {
	var normalBuf, rocksBuf bytes.Buffer

	normalHandler := slog.NewTextHandler(&normalBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	rocksHandler := slog.NewTextHandler(&rocksBuf, &slog.HandlerOptions{Level: slog.LevelDebug})

	dispatcher := NewLogDispatcher(normalHandler, normalHandler, rocksHandler, normalHandler)

	original := GetLevel()
	defer SetLevel(original)
	SetLevel(slog.LevelDebug)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "rocksdb compaction", 0)
	r.AddAttrs(slog.Bool("rocksdb_log", true))
	err := dispatcher.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, rocksBuf.String(), "rocksdb compaction")
}

func TestLogDispatcherRaft(t *testing.T) {
	var normalBuf, raftBuf bytes.Buffer

	normalHandler := slog.NewTextHandler(&normalBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	raftHandler := slog.NewTextHandler(&raftBuf, &slog.HandlerOptions{Level: slog.LevelDebug})

	dispatcher := NewLogDispatcher(normalHandler, normalHandler, normalHandler, raftHandler)

	original := GetLevel()
	defer SetLevel(original)
	SetLevel(slog.LevelDebug)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "raft election", 0)
	r.AddAttrs(slog.Bool("raftdb_log", true))
	err := dispatcher.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, raftBuf.String(), "raft election")
}

func TestLogDispatcherWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	dispatcher := NewLogDispatcher(handler, handler, handler, handler)

	withAttrs := dispatcher.WithAttrs([]slog.Attr{slog.String("key", "value")})
	assert.NotNil(t, withAttrs)
	_, ok := withAttrs.(*LogDispatcher)
	assert.True(t, ok)
}

func TestLogDispatcherWithGroup(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	dispatcher := NewLogDispatcher(handler, handler, handler, handler)

	withGroup := dispatcher.WithGroup("test")
	assert.NotNil(t, withGroup)
	_, ok := withGroup.(*LogDispatcher)
	assert.True(t, ok)
}

func TestSlowLogHandlerBelowThreshold(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := NewSlowLogHandler(inner, time.Second)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "operation", 0)
	r.AddAttrs(slog.Duration("takes", 500*time.Millisecond))
	err := slowHandler.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Empty(t, buf.String()) // Below threshold, filtered out.
}

func TestSlowLogHandlerAboveThreshold(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := NewSlowLogHandler(inner, time.Second)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "slow operation", 0)
	r.AddAttrs(slog.Duration("takes", 2*time.Second))
	err := slowHandler.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "slow operation")
}

func TestSlowLogHandlerNoTakes(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := NewSlowLogHandler(inner, time.Second)

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "no takes attr", 0)
	err := slowHandler.Handle(context.Background(), r)
	assert.NoError(t, err)
	assert.Empty(t, buf.String()) // No "takes" attribute, filtered out.
}

func TestSlowLogHandlerWithAttrs(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := NewSlowLogHandler(inner, time.Second)

	withAttrs := slowHandler.WithAttrs([]slog.Attr{slog.String("key", "value")})
	assert.NotNil(t, withAttrs)
	_, ok := withAttrs.(*SlowLogHandler)
	assert.True(t, ok)
}

func TestSlowLogHandlerWithGroup(t *testing.T) {
	var buf bytes.Buffer
	inner := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	slowHandler := NewSlowLogHandler(inner, time.Second)

	withGroup := slowHandler.WithGroup("test")
	assert.NotNil(t, withGroup)
	_, ok := withGroup.(*SlowLogHandler)
	assert.True(t, ok)
}

func TestSetup(t *testing.T) {
	cfg := LogConfig{
		Level:            "debug",
		Format:           "text",
		SlowLogThreshold: time.Second,
	}
	Setup(cfg)

	assert.Equal(t, slog.LevelDebug, GetLevel())
}

func TestSetupJSON(t *testing.T) {
	cfg := LogConfig{
		Level:  "info",
		Format: "json",
	}
	Setup(cfg)

	assert.Equal(t, slog.LevelInfo, GetLevel())
}

func TestSetupWithFileRotation(t *testing.T) {
	dir := t.TempDir()
	logFile := dir + "/test.log"

	cfg := LogConfig{
		Level:      "info",
		Format:     "text",
		Filename:   logFile,
		MaxSizeMB:  10,
		MaxBackups: 3,
		MaxAgeDays: 7,
	}
	Setup(cfg)
	assert.Equal(t, slog.LevelInfo, GetLevel())
}

func TestDispatcherEnabled(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	dispatcher := NewLogDispatcher(handler, handler, handler, handler)

	original := GetLevel()
	defer SetLevel(original)

	SetLevel(slog.LevelWarn)
	assert.False(t, dispatcher.Enabled(context.Background(), slog.LevelInfo))
	assert.True(t, dispatcher.Enabled(context.Background(), slog.LevelWarn))
}

func TestRotatingFileWriter(t *testing.T) {
	dir := t.TempDir()
	writer := RotatingFileWriter(dir+"/test.log", 10, 3, 7)
	assert.NotNil(t, writer)

	// Write some data.
	n, err := writer.Write([]byte("test line\n"))
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
}

func TestIntegrationLoggerOutput(t *testing.T) {
	var buf bytes.Buffer

	original := GetLevel()
	defer SetLevel(original)
	SetLevel(slog.LevelDebug)

	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(NewLevelFilter(handler))

	logger.Info("test message", "key", "value")
	output := buf.String()
	assert.True(t, strings.Contains(output, "test message"))
	assert.True(t, strings.Contains(output, "key=value"))
}
