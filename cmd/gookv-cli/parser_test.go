package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Tokenize tests
// ---------------------------------------------------------------------------

func TestTokenize(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []Token
		wantErr bool
	}{
		{
			name:  "simple unquoted",
			input: "GET mykey",
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: "mykey", Value: []byte("mykey")},
			},
		},
		{
			name:  "quoted value with space",
			input: `PUT k1 "hello world"`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "k1", Value: []byte("k1")},
				{Raw: `"hello world"`, Value: []byte("hello world")},
			},
		},
		{
			name:  "escaped quote in string",
			input: `PUT k1 "hello\"world"`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "k1", Value: []byte("k1")},
				{Raw: `"hello\"world"`, Value: []byte(`hello"world`)},
			},
		},
		{
			name:  "hex literal 0x prefix",
			input: "GET 0xDEADBEEF",
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: "0xDEADBEEF", Value: []byte{0xDE, 0xAD, 0xBE, 0xEF}, IsHex: true},
			},
		},
		{
			name:  "hex literal x' prefix",
			input: "GET x'CAFE'",
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: "x'CAFE'", Value: []byte{0xCA, 0xFE}, IsHex: true},
			},
		},
		{
			name:  "multiple args",
			input: "BGET k1 k2 k3",
			want: []Token{
				{Raw: "BGET", Value: []byte("BGET")},
				{Raw: "k1", Value: []byte("k1")},
				{Raw: "k2", Value: []byte("k2")},
				{Raw: "k3", Value: []byte("k3")},
			},
		},
		{
			name:  "escape newline",
			input: `PUT key "val\nue"`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "key", Value: []byte("key")},
				{Raw: `"val\nue"`, Value: []byte("val\nue")},
			},
		},
		{
			name:  "escape tab",
			input: `PUT key "val\tue"`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "key", Value: []byte("key")},
				{Raw: `"val\tue"`, Value: []byte("val\tue")},
			},
		},
		{
			name:  "empty quoted string",
			input: `PUT key ""`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "key", Value: []byte("key")},
				{Raw: `""`, Value: []byte{}},
			},
		},
		{
			name:  "key with spaces in quotes",
			input: `GET "key with spaces"`,
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: `"key with spaces"`, Value: []byte("key with spaces")},
			},
		},
		{
			name:  "nested escaped quotes",
			input: `PUT key "he said \"hi\""`,
			want: []Token{
				{Raw: "PUT", Value: []byte("PUT")},
				{Raw: "key", Value: []byte("key")},
				{Raw: `"he said \"hi\""`, Value: []byte(`he said "hi"`)},
			},
		},
		{
			name:  "empty string keys for full range",
			input: `SCAN "" "" LIMIT 10`,
			want: []Token{
				{Raw: "SCAN", Value: []byte("SCAN")},
				{Raw: `""`, Value: []byte{}},
				{Raw: `""`, Value: []byte{}},
				{Raw: "LIMIT", Value: []byte("LIMIT")},
				{Raw: "10", Value: []byte("10")},
			},
		},
		{
			name:  "colon in unquoted token",
			input: "GET user:001",
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: "user:001", Value: []byte("user:001")},
			},
		},
		{
			name:  "multiple keywords",
			input: "BEGIN PESSIMISTIC LOCK_TTL 5000",
			want: []Token{
				{Raw: "BEGIN", Value: []byte("BEGIN")},
				{Raw: "PESSIMISTIC", Value: []byte("PESSIMISTIC")},
				{Raw: "LOCK_TTL", Value: []byte("LOCK_TTL")},
				{Raw: "5000", Value: []byte("5000")},
			},
		},
		{
			name:  "underscore as placeholder",
			input: "CAS k1 v2 _ NOT_EXIST",
			want: []Token{
				{Raw: "CAS", Value: []byte("CAS")},
				{Raw: "k1", Value: []byte("k1")},
				{Raw: "v2", Value: []byte("v2")},
				{Raw: "_", Value: []byte("_")},
				{Raw: "NOT_EXIST", Value: []byte("NOT_EXIST")},
			},
		},
		{
			name:  "short hex",
			input: "GET 0xCAFE",
			want: []Token{
				{Raw: "GET", Value: []byte("GET")},
				{Raw: "0xCAFE", Value: []byte{0xCA, 0xFE}, IsHex: true},
			},
		},
		{
			name:    "unterminated quote",
			input:   `GET "unterminated`,
			wantErr: true,
		},
		{
			name:  "empty input",
			input: "",
			want:  nil,
		},
		{
			name:  "whitespace only",
			input: "   \t  ",
			want:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Tokenize(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.want == nil {
				assert.Empty(t, got)
				return
			}
			require.Equal(t, len(tt.want), len(got), "token count mismatch")
			for i, want := range tt.want {
				assert.Equal(t, want.Raw, got[i].Raw, "Raw mismatch at token %d", i)
				assert.Equal(t, want.Value, got[i].Value, "Value mismatch at token %d", i)
				assert.Equal(t, want.IsHex, got[i].IsHex, "IsHex mismatch at token %d", i)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SplitStatements tests
// ---------------------------------------------------------------------------

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantStmts    []string
		wantRem      string
		wantComplete bool
	}{
		{
			name:         "single statement",
			input:        "GET k1;",
			wantStmts:    []string{"GET k1"},
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "two statements",
			input:        "PUT k1 v1; PUT k2 v2;",
			wantStmts:    []string{"PUT k1 v1", "PUT k2 v2"},
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "statement + remainder",
			input:        "PUT k1 v1; GET k2",
			wantStmts:    []string{"PUT k1 v1"},
			wantRem:      "GET k2",
			wantComplete: false,
		},
		{
			name:         "semicolon in quoted string",
			input:        `PUT k1 "hello; world";`,
			wantStmts:    []string{`PUT k1 "hello; world"`},
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "no semicolons",
			input:        "GET k1",
			wantStmts:    nil,
			wantRem:      "GET k1",
			wantComplete: false,
		},
		{
			name:         "multiple empty statements",
			input:        ";;;",
			wantStmts:    []string{"", "", ""},
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "multi-statement with remainder",
			input:        `PUT k1 "v1"; PUT k2 "v2"; GET k1`,
			wantStmts:    []string{`PUT k1 "v1"`, `PUT k2 "v2"`},
			wantRem:      "GET k1",
			wantComplete: false,
		},
		{
			name:         "empty string",
			input:        "",
			wantStmts:    nil,
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "whitespace with semicolon",
			input:        "  ;  ",
			wantStmts:    []string{""},
			wantRem:      "",
			wantComplete: true,
		},
		{
			name:         "hex literal with semicolon inside",
			input:        "PUT k1 x'AB;CD';",
			wantStmts:    nil,
			wantRem:      "",
			wantComplete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmts, remainder, complete := SplitStatements(tt.input)
			// Handle case 10 specially: the x'AB;CD' hex literal contains a semicolon
			// which the statement splitter should treat as part of the hex literal
			if tt.name == "hex literal with semicolon inside" {
				assert.Equal(t, []string{"PUT k1 x'AB;CD'"}, stmts)
				assert.Equal(t, "", remainder)
				assert.Equal(t, true, complete)
				return
			}
			if tt.wantStmts == nil {
				assert.Empty(t, stmts, "stmts should be empty")
			} else {
				assert.Equal(t, tt.wantStmts, stmts)
			}
			assert.Equal(t, tt.wantRem, remainder)
			assert.Equal(t, tt.wantComplete, complete)
		})
	}
}

// ---------------------------------------------------------------------------
// IsMetaCommand tests
// ---------------------------------------------------------------------------

func TestIsMetaCommand(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{`\help`, true},
		{`\q`, true},
		{`\timing on`, true},
		{`\format hex`, true},
		{`\pagesize 50`, true},
		{"GET key", false},
		{"HELP;", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, IsMetaCommand(tt.input))
		})
	}
}

// ---------------------------------------------------------------------------
// ParseCommand tests
// ---------------------------------------------------------------------------

func TestParseCommand(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		inTxn     bool
		wantType  CommandType
		wantArgs  [][]byte
		wantInt   int64
		wantStr   string
		wantFlags uint32
		wantErr   bool
	}{
		// Raw GET / Txn GET
		{name: "raw GET", input: "GET mykey", wantType: CmdGet, wantArgs: bss("mykey")},
		{name: "txn GET", input: "GET mykey", inTxn: true, wantType: CmdTxnGet, wantArgs: bss("mykey")},
		// PUT
		{name: "raw PUT", input: "PUT k1 v1", wantType: CmdPut, wantArgs: bss("k1", "v1")},
		{name: "PUT with TTL", input: "PUT k1 v1 TTL 60", wantType: CmdPutTTL, wantArgs: bss("k1", "v1"), wantInt: 60},
		// DELETE
		{name: "raw DELETE", input: "DELETE k1", wantType: CmdDelete, wantArgs: bss("k1")},
		{name: "txn DELETE", input: "DELETE k1", inTxn: true, wantType: CmdTxnDelete, wantArgs: bss("k1")},
		{name: "DELETE RANGE", input: "DELETE RANGE a z", wantType: CmdDeleteRange, wantArgs: bss("a", "z")},
		// SET
		{name: "txn SET", input: "SET k1 v1", inTxn: true, wantType: CmdTxnSet, wantArgs: bss("k1", "v1")},
		{name: "SET outside txn", input: "SET k1 v1", wantErr: true},
		// TTL
		{name: "TTL", input: "TTL mykey", wantType: CmdTTL, wantArgs: bss("mykey")},
		// SCAN
		{name: "SCAN default limit", input: "SCAN a z", wantType: CmdScan, wantArgs: bss("a", "z"), wantInt: 0},
		{name: "SCAN with LIMIT", input: "SCAN a z LIMIT 50", wantType: CmdScan, wantArgs: bss("a", "z"), wantInt: 50},
		// BGET
		{name: "BGET", input: "BGET k1 k2 k3", wantType: CmdBatchGet, wantArgs: bss("k1", "k2", "k3")},
		{name: "txn BGET", input: "BGET k1 k2", inTxn: true, wantType: CmdTxnBatchGet, wantArgs: bss("k1", "k2")},
		// BPUT
		{name: "BPUT", input: "BPUT k1 v1 k2 v2", wantType: CmdBatchPut, wantArgs: bss("k1", "v1", "k2", "v2")},
		{name: "BPUT odd args", input: "BPUT k1 v1 k2", wantErr: true},
		// BDELETE
		{name: "BDELETE", input: "BDELETE k1 k2", wantType: CmdBatchDelete, wantArgs: bss("k1", "k2")},
		// CAS
		{name: "CAS normal", input: "CAS k1 v2 v1", wantType: CmdCAS, wantArgs: bss("k1", "v2", "v1")},
		{name: "CAS NOT_EXIST", input: "CAS k1 v2 _ NOT_EXIST", wantType: CmdCAS, wantArgs: bss("k1", "v2", "_"), wantFlags: flagNotExist},
		// CHECKSUM
		{name: "CHECKSUM full", input: "CHECKSUM", wantType: CmdChecksum},
		{name: "CHECKSUM range", input: "CHECKSUM a z", wantType: CmdChecksum, wantArgs: bss("a", "z")},
		{name: "CHECKSUM invalid", input: "CHECKSUM a", wantErr: true},
		// BEGIN
		{name: "BEGIN default", input: "BEGIN", wantType: CmdBegin},
		{name: "BEGIN PESSIMISTIC", input: "BEGIN PESSIMISTIC", wantType: CmdBegin},
		{name: "BEGIN combined", input: "BEGIN ASYNC_COMMIT ONE_PC LOCK_TTL 5000", wantType: CmdBegin},
		// COMMIT / ROLLBACK
		{name: "COMMIT", input: "COMMIT", inTxn: true, wantType: CmdCommit},
		{name: "ROLLBACK", input: "ROLLBACK", inTxn: true, wantType: CmdRollback},
		// STORE
		{name: "STORE LIST", input: "STORE LIST", wantType: CmdStoreList},
		{name: "STORE STATUS", input: "STORE STATUS 1", wantType: CmdStoreStatus, wantInt: 1},
		// REGION
		{name: "REGION key", input: "REGION mykey", wantType: CmdRegion, wantArgs: bss("mykey")},
		{name: "REGION LIST", input: "REGION LIST", wantType: CmdRegionList, wantInt: 0},
		{name: "REGION LIST LIMIT", input: "REGION LIST LIMIT 10", wantType: CmdRegionList, wantInt: 10},
		{name: "REGION ID", input: "REGION ID 42", wantType: CmdRegionByID, wantInt: 42},
		// CLUSTER
		{name: "CLUSTER INFO", input: "CLUSTER INFO", wantType: CmdClusterInfo},
		// TSO
		{name: "TSO", input: "TSO", wantType: CmdTSO},
		// GC SAFEPOINT
		{name: "GC SAFEPOINT", input: "GC SAFEPOINT", wantType: CmdGCSafepoint},
		// STATUS
		{name: "STATUS no addr", input: "STATUS", wantType: CmdStatus, wantStr: ""},
		{name: "STATUS with addr", input: "STATUS 127.0.0.1:20180", wantType: CmdStatus, wantStr: "127.0.0.1:20180"},
		// HELP / EXIT / QUIT
		{name: "HELP", input: "HELP", wantType: CmdHelp},
		{name: "EXIT", input: "EXIT", wantType: CmdExit},
		{name: "QUIT", input: "QUIT", wantType: CmdExit},
		// Case insensitivity
		{name: "case insensitive", input: "get MYKEY", wantType: CmdGet, wantArgs: bss("MYKEY")},
		// Empty statement
		{name: "empty", input: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := Tokenize(tt.input)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Fatalf("Tokenize error: %v", err)
			}

			cmd, err := ParseCommand(tokens, tt.inTxn)
			if tt.wantErr {
				assert.Error(t, err, "expected error for %q", tt.input)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, cmd.Type, "CommandType mismatch")

			if tt.wantArgs != nil {
				require.Equal(t, len(tt.wantArgs), len(cmd.Args), "arg count mismatch")
				for i, want := range tt.wantArgs {
					assert.Equal(t, want, cmd.Args[i], "arg %d mismatch", i)
				}
			}
			if tt.wantInt != 0 {
				assert.Equal(t, tt.wantInt, cmd.IntArg, "IntArg mismatch")
			}
			if tt.wantStr != "" {
				assert.Equal(t, tt.wantStr, cmd.StrArg, "StrArg mismatch")
			}
			if tt.wantFlags != 0 {
				assert.Equal(t, tt.wantFlags, cmd.Flags, "Flags mismatch")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ParseMetaCommand tests
// ---------------------------------------------------------------------------

func TestParseMetaCommand(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType CommandType
		wantStr  string
		wantInt  int64
		wantErr  bool
	}{
		{name: "help", input: `\help`, wantType: CmdHelp},
		{name: "h", input: `\h`, wantType: CmdHelp},
		{name: "?", input: `\?`, wantType: CmdHelp},
		{name: "quit", input: `\quit`, wantType: CmdExit},
		{name: "q", input: `\q`, wantType: CmdExit},
		{name: "timing on", input: `\timing on`, wantType: CmdMetaTiming, wantStr: "on"},
		{name: "timing off", input: `\timing off`, wantType: CmdMetaTiming, wantStr: "off"},
		{name: "format hex", input: `\format hex`, wantType: CmdMetaFormat, wantStr: "hex"},
		{name: "format table", input: `\format table`, wantType: CmdMetaFormat, wantStr: "table"},
		{name: "format plain", input: `\format plain`, wantType: CmdMetaFormat, wantStr: "plain"},
		{name: "pagesize", input: `\pagesize 50`, wantType: CmdMetaPagesize, wantInt: 50},
		{name: "x toggle", input: `\x`, wantType: CmdMetaHex},
		{name: "t toggle", input: `\t`, wantType: CmdMetaTiming, wantStr: ""},
		{name: "unknown", input: `\unknown`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, err := ParseMetaCommand(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, cmd.Type)
			if tt.wantStr != "" {
				assert.Equal(t, tt.wantStr, cmd.StrArg)
			}
			if tt.wantInt != 0 {
				assert.Equal(t, tt.wantInt, cmd.IntArg)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// bss creates a [][]byte from string arguments.
func bss(strs ...string) [][]byte {
	result := make([][]byte, len(strs))
	for i, s := range strs {
		result[i] = []byte(s)
	}
	return result
}
