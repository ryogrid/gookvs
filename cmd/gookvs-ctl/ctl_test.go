package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCommand(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"scan --db /tmp", "scan"},
		{"get --key abc", "get"},
		{"mvcc --key abc", "mvcc"},
		{"dump --db /tmp", "dump"},
		{"size --db /tmp", "size"},
		{"compact --db /tmp", "compact"},
		{"help", "help"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, ParseCommand(tt.input))
		})
	}
}

func TestTryPrintable(t *testing.T) {
	tests := []struct {
		input    []byte
		expected string
	}{
		{[]byte("hello"), "hello"},
		{[]byte{0x00, 0x01, 0x02}, "000102"},
		{[]byte("abc\x00def"), "61626300646566"},
		{[]byte(""), ""},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tryPrintable(tt.input))
	}
}

func TestFormatSize(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1536 * 1024, "1.50 MB"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, formatSize(tt.input))
		})
	}
}

func TestWriteTypeStr(t *testing.T) {
	assert.Equal(t, "Put", writeTypeStr('P'))
	assert.Equal(t, "Delete", writeTypeStr('D'))
	assert.Equal(t, "Rollback", writeTypeStr('R'))
	assert.Equal(t, "Lock", writeTypeStr('L'))
}

func TestUsageNotEmpty(t *testing.T) {
	assert.NotEmpty(t, usage)
	assert.Contains(t, usage, "gookvs-ctl")
	assert.Contains(t, usage, "scan")
	assert.Contains(t, usage, "get")
	assert.Contains(t, usage, "mvcc")
	assert.Contains(t, usage, "compact")
}
