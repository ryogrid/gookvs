package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestFormatter(mode OutputFormat) (*Formatter, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	f := NewFormatter(buf)
	f.SetOutputFormat(mode)
	return f, buf
}

// ---------------------------------------------------------------------------
// Table mode tests
// ---------------------------------------------------------------------------

func TestFormatterTableOK(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.Format(&Result{Type: ResultOK}, 1*time.Millisecond)
	assert.Contains(t, buf.String(), "OK")
}

func TestFormatterTableValue(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.Format(&Result{Type: ResultValue, Value: []byte("hello")}, 800*time.Microsecond)
	out := buf.String()
	assert.Contains(t, out, `"hello"`)
}

func TestFormatterTableNotFound(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.Format(&Result{Type: ResultNotFound}, 500*time.Microsecond)
	assert.Contains(t, buf.String(), "(not found)")
}

func TestFormatterTableRows(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	rows := []KvPairResult{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
		{Key: []byte("k3"), Value: []byte("v3")},
	}
	f.Format(&Result{Type: ResultRows, Rows: rows}, 2*time.Millisecond)
	out := buf.String()
	assert.Contains(t, out, "Key")
	assert.Contains(t, out, "Value")
	assert.Contains(t, out, "k1")
	assert.Contains(t, out, "v3")
	assert.Contains(t, out, "3 rows")
}

func TestFormatterTableAdminTable(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.Format(&Result{
		Type:    ResultTable,
		Columns: []string{"StoreID", "Address", "State"},
		TableRows: [][]string{
			{"1", "127.0.0.1:20160", "Up"},
			{"2", "127.0.0.1:20161", "Up"},
		},
	}, 3*time.Millisecond)
	out := buf.String()
	assert.Contains(t, out, "StoreID")
	assert.Contains(t, out, "Address")
	assert.Contains(t, out, "127.0.0.1:20160")
	assert.Contains(t, out, "2 rows")
}

// ---------------------------------------------------------------------------
// Plain mode tests
// ---------------------------------------------------------------------------

func TestFormatterPlainOK(t *testing.T) {
	f, buf := newTestFormatter(FormatPlain)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultOK}, 0)
	assert.Equal(t, "OK\n", buf.String())
}

func TestFormatterPlainValue(t *testing.T) {
	f, buf := newTestFormatter(FormatPlain)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultValue, Value: []byte("hello")}, 0)
	assert.Equal(t, "\"hello\"\n", buf.String())
}

func TestFormatterPlainNotFound(t *testing.T) {
	f, buf := newTestFormatter(FormatPlain)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultNotFound}, 0)
	assert.Equal(t, "(not found)\n", buf.String())
}

func TestFormatterPlainRows(t *testing.T) {
	f, buf := newTestFormatter(FormatPlain)
	f.SetTiming(false)
	rows := []KvPairResult{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}
	f.Format(&Result{Type: ResultRows, Rows: rows}, 0)
	assert.Equal(t, "k1\tv1\nk2\tv2\n", buf.String())
}

func TestFormatterPlainNil(t *testing.T) {
	f, buf := newTestFormatter(FormatPlain)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultNil}, 0)
	assert.Equal(t, "(nil)\n", buf.String())
}

// ---------------------------------------------------------------------------
// Hex mode tests
// ---------------------------------------------------------------------------

func TestFormatterHexValuePrintable(t *testing.T) {
	f, buf := newTestFormatter(FormatHex)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultValue, Value: []byte("hello")}, 0)
	assert.Equal(t, "68656c6c6f\n", buf.String())
}

func TestFormatterHexValueBinary(t *testing.T) {
	f, buf := newTestFormatter(FormatHex)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultValue, Value: []byte{0x00, 0x01}}, 0)
	assert.Equal(t, "0001\n", buf.String())
}

func TestFormatterHexRows(t *testing.T) {
	f, buf := newTestFormatter(FormatHex)
	f.SetTiming(false)
	rows := []KvPairResult{
		{Key: []byte("a"), Value: []byte("1")},
	}
	f.Format(&Result{Type: ResultRows, Rows: rows}, 0)
	assert.Contains(t, buf.String(), "61\t31")
}

// ---------------------------------------------------------------------------
// formatBytes tests (auto mode)
// ---------------------------------------------------------------------------

func TestFormatBytesAutoPrintable(t *testing.T) {
	f := NewFormatter(&bytes.Buffer{})
	f.SetDisplayMode(DisplayAuto)
	assert.Equal(t, "abc", f.formatBytes([]byte("abc")))
}

func TestFormatBytesAutoBinary(t *testing.T) {
	f := NewFormatter(&bytes.Buffer{})
	f.SetDisplayMode(DisplayAuto)
	result := f.formatBytes([]byte{0x00, 0xFF})
	assert.Contains(t, result, "\\x00")
	assert.Contains(t, result, "\\xff")
}

// ---------------------------------------------------------------------------
// Timing tests
// ---------------------------------------------------------------------------

func TestFormatterTimingOn(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(true)
	f.Format(&Result{Type: ResultOK}, 1500*time.Microsecond)
	assert.Contains(t, buf.String(), "1.5ms")
}

func TestFormatterTimingOff(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{Type: ResultOK}, 1500*time.Microsecond)
	assert.NotContains(t, buf.String(), "ms")
}

// ---------------------------------------------------------------------------
// Special result type tests
// ---------------------------------------------------------------------------

func TestFormatterCASSwapped(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{
		Type:    ResultCAS,
		Swapped: true,
		PrevVal: []byte("old"),
	}, 0)
	out := buf.String()
	assert.Contains(t, out, "OK (swapped)")
	assert.Contains(t, out, `"old"`)
}

func TestFormatterCASNotSwapped(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{
		Type:    ResultCAS,
		Swapped: false,
		PrevVal: []byte("current"),
	}, 0)
	out := buf.String()
	assert.Contains(t, out, "FAILED (not swapped)")
	assert.Contains(t, out, `"current"`)
}

func TestFormatterChecksum(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{
		Type:       ResultChecksum,
		Checksum:   0xa3f7e2b104c8d91e,
		TotalKvs:   1247,
		TotalBytes: 89412,
	}, 0)
	out := buf.String()
	assert.Contains(t, out, "0xa3f7e2b104c8d91e")
	assert.Contains(t, out, "1,247")
	assert.Contains(t, out, "89,412")
}

func TestFormatterBegin(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{
		Type:    ResultBegin,
		Message: "Transaction started (optimistic, startTS=123456)",
	}, 0)
	assert.Contains(t, buf.String(), "Transaction started")
}

func TestFormatterCommit(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(false)
	f.Format(&Result{
		Type:    ResultCommit,
		Message: "OK (commitTS=789012, 3 keys written)",
	}, 0)
	assert.Contains(t, buf.String(), "OK (commitTS=789012")
}

// ---------------------------------------------------------------------------
// formatCommas helper test
// ---------------------------------------------------------------------------

func TestFormatCommas(t *testing.T) {
	tests := []struct {
		input uint64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1247, "1,247"},
		{89412, "89,412"},
		{1000000, "1,000,000"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatCommas(tt.input))
	}
}

// ---------------------------------------------------------------------------
// FormatError test
// ---------------------------------------------------------------------------

func TestFormatError(t *testing.T) {
	errBuf := &bytes.Buffer{}
	f := NewFormatter(&bytes.Buffer{})
	f.SetErrOut(errBuf)
	f.FormatError(assert.AnError)
	assert.True(t, strings.HasPrefix(errBuf.String(), "ERROR:"))
}

// ---------------------------------------------------------------------------
// Rows with NotFoundCount
// ---------------------------------------------------------------------------

func TestFormatterRowsNotFound(t *testing.T) {
	f, buf := newTestFormatter(FormatTable)
	f.SetTiming(true)
	rows := []KvPairResult{
		{Key: []byte("k1"), Value: []byte("v1")},
	}
	f.Format(&Result{Type: ResultRows, Rows: rows, NotFoundCount: 2}, 1*time.Millisecond)
	out := buf.String()
	assert.Contains(t, out, "1 rows")
	assert.Contains(t, out, "2 not found")
}
