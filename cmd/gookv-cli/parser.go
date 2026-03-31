package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/ryogrid/gookv/pkg/client"
)

// ---------------------------------------------------------------------------
// Token
// ---------------------------------------------------------------------------

// Token represents a parsed input token.
type Token struct {
	Raw   string // original text (for error messages)
	Value []byte // decoded byte value (hex decoded, unquoted, etc.)
	IsHex bool   // true if the token was a hex literal (0x... or x'...')
}

// ---------------------------------------------------------------------------
// Tokenize
// ---------------------------------------------------------------------------

// Tokenize splits a statement string into tokens.
// Handles: double-quoted strings ("..."), hex literals (0x..., x'...'),
// and unquoted whitespace-delimited tokens.
// Returns an error for unterminated quotes or invalid hex.
func Tokenize(stmt string) ([]Token, error) {
	var tokens []Token
	r := []rune(stmt)
	i := 0

	for i < len(r) {
		// Skip whitespace
		if unicode.IsSpace(r[i]) {
			i++
			continue
		}

		// Double-quoted string
		if r[i] == '"' {
			tok, next, err := tokenizeQuoted(r, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)
			i = next
			continue
		}

		// x'...' hex literal (MySQL-style)
		if (r[i] == 'x' || r[i] == 'X') && i+1 < len(r) && r[i+1] == '\'' {
			tok, next, err := tokenizeXHex(r, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)
			i = next
			continue
		}

		// 0x... hex literal
		if r[i] == '0' && i+1 < len(r) && (r[i+1] == 'x' || r[i+1] == 'X') {
			tok, next, err := tokenize0xHex(r, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, tok)
			i = next
			continue
		}

		// Unquoted token
		tok, next := tokenizeUnquoted(r, i)
		tokens = append(tokens, tok)
		i = next
	}

	return tokens, nil
}

// tokenizeQuoted parses a double-quoted string starting at r[pos] == '"'.
func tokenizeQuoted(r []rune, pos int) (Token, int, error) {
	start := pos
	pos++ // skip opening quote
	buf := make([]byte, 0) // non-nil empty slice for empty strings

	for pos < len(r) {
		ch := r[pos]
		if ch == '\\' && pos+1 < len(r) {
			pos++
			switch r[pos] {
			case '\\':
				buf = append(buf, '\\')
			case '"':
				buf = append(buf, '"')
			case 'n':
				buf = append(buf, '\n')
			case 't':
				buf = append(buf, '\t')
			default:
				buf = append(buf, '\\')
				buf = append(buf, []byte(string(r[pos]))...)
			}
			pos++
			continue
		}
		if ch == '"' {
			raw := string(r[start : pos+1])
			pos++ // skip closing quote
			return Token{Raw: raw, Value: buf, IsHex: false}, pos, nil
		}
		buf = append(buf, []byte(string(ch))...)
		pos++
	}
	return Token{}, 0, fmt.Errorf("unterminated quote starting at position %d", start)
}

// tokenizeXHex parses x'...' hex literal starting at r[pos].
func tokenizeXHex(r []rune, pos int) (Token, int, error) {
	start := pos
	pos += 2 // skip x'
	hexStart := pos

	for pos < len(r) && r[pos] != '\'' {
		pos++
	}
	if pos >= len(r) {
		return Token{}, 0, fmt.Errorf("unterminated hex literal starting at position %d", start)
	}

	hexStr := string(r[hexStart:pos])
	pos++ // skip closing '

	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return Token{}, 0, fmt.Errorf("invalid hex literal %q: %w", string(r[start:pos]), err)
	}

	return Token{Raw: string(r[start:pos]), Value: decoded, IsHex: true}, pos, nil
}

// tokenize0xHex parses 0x... hex literal starting at r[pos].
func tokenize0xHex(r []rune, pos int) (Token, int, error) {
	start := pos
	pos += 2 // skip 0x

	for pos < len(r) && isHexDigit(r[pos]) {
		pos++
	}

	hexStr := string(r[start+2 : pos])
	if len(hexStr) == 0 {
		return Token{}, 0, fmt.Errorf("empty hex literal at position %d", start)
	}

	if len(hexStr)%2 != 0 {
		hexStr = "0" + hexStr
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return Token{}, 0, fmt.Errorf("invalid hex literal %q: %w", string(r[start:pos]), err)
	}

	return Token{Raw: string(r[start:pos]), Value: decoded, IsHex: true}, pos, nil
}

// tokenizeUnquoted parses an unquoted token starting at r[pos].
func tokenizeUnquoted(r []rune, pos int) (Token, int) {
	start := pos
	for pos < len(r) && !unicode.IsSpace(r[pos]) && r[pos] != ';' && r[pos] != '"' {
		pos++
	}
	raw := string(r[start:pos])
	return Token{Raw: raw, Value: []byte(raw), IsHex: false}, pos
}

func isHexDigit(r rune) bool {
	return (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')
}

// ---------------------------------------------------------------------------
// SplitStatements
// ---------------------------------------------------------------------------

// SplitStatements splits input on semicolons, respecting quoted strings and
// hex literals. Returns:
//   - stmts: list of complete (semicolon-terminated) statements with the
//     semicolons stripped
//   - remainder: any trailing text after the last semicolon (incomplete)
//   - complete: true if the input ended with a semicolon (remainder is empty)
func SplitStatements(input string) (stmts []string, remainder string, complete bool) {
	r := []rune(input)
	var buf []rune
	i := 0

	for i < len(r) {
		ch := r[i]

		// Inside double-quoted string
		if ch == '"' {
			buf = append(buf, ch)
			i++
			for i < len(r) {
				if r[i] == '\\' && i+1 < len(r) {
					buf = append(buf, r[i], r[i+1])
					i += 2
					continue
				}
				buf = append(buf, r[i])
				if r[i] == '"' {
					i++
					break
				}
				i++
			}
			continue
		}

		// Inside x'...' hex literal
		if (ch == 'x' || ch == 'X') && i+1 < len(r) && r[i+1] == '\'' {
			buf = append(buf, ch, r[i+1])
			i += 2
			for i < len(r) && r[i] != '\'' {
				buf = append(buf, r[i])
				i++
			}
			if i < len(r) {
				buf = append(buf, r[i])
				i++
			}
			continue
		}

		// Semicolon: end of statement
		if ch == ';' {
			stmts = append(stmts, strings.TrimSpace(string(buf)))
			buf = buf[:0]
			i++
			continue
		}

		buf = append(buf, ch)
		i++
	}

	remaining := strings.TrimSpace(string(buf))
	if remaining == "" && len(buf) == 0 {
		// Input ended with a semicolon or was empty
		return stmts, "", true
	}
	if remaining == "" && len(stmts) > 0 {
		// There was trailing whitespace after last semicolon
		return stmts, "", true
	}
	if len(stmts) == 0 && remaining == "" {
		return nil, "", true
	}
	return stmts, remaining, false
}

// ---------------------------------------------------------------------------
// IsMetaCommand
// ---------------------------------------------------------------------------

// IsMetaCommand returns true if the line starts with a backslash command.
func IsMetaCommand(line string) bool {
	trimmed := strings.TrimSpace(line)
	return len(trimmed) > 0 && trimmed[0] == '\\'
}

// ---------------------------------------------------------------------------
// CommandType
// ---------------------------------------------------------------------------

// CommandType identifies which operation a parsed statement represents.
type CommandType int

const (
	// Raw KV
	CmdGet         CommandType = iota // GET (outside txn)
	CmdPut                            // PUT key value
	CmdPutTTL                         // PUT key value TTL n
	CmdDelete                         // DELETE (outside txn)
	CmdTTL                            // TTL key
	CmdScan                           // SCAN start end [LIMIT n]
	CmdBatchGet                       // BGET k1 k2 ...
	CmdBatchPut                       // BPUT k1 v1 k2 v2 ...
	CmdBatchDelete                    // BDELETE k1 k2 ...
	CmdDeleteRange                    // DELETE RANGE start end
	CmdCAS                            // CAS key new old [NOT_EXIST]
	CmdChecksum                       // CHECKSUM [start end]

	// Transactional (context-sensitive dispatch)
	CmdTxnGet      // GET (inside txn)
	CmdTxnBatchGet // BGET (inside txn)
	CmdTxnSet      // SET key value (inside txn only)
	CmdTxnDelete   // DELETE (inside txn)
	CmdTxnScan     // SCAN (inside txn)
	CmdBegin       // BEGIN [opts...]
	CmdCommit      // COMMIT
	CmdRollback    // ROLLBACK

	// Admin
	CmdClusterInfo // CLUSTER INFO
	CmdStoreList   // STORE LIST
	CmdStoreStatus // STORE STATUS <id>
	CmdRegion      // REGION <key>
	CmdRegionList  // REGION LIST [LIMIT n]
	CmdRegionByID  // REGION ID <id>
	CmdTSO         // TSO
	CmdGCSafepoint // GC SAFEPOINT
	CmdStatus      // STATUS [addr]

	// Meta (keyword forms requiring semicolons)
	CmdHelp // HELP
	CmdExit // EXIT / QUIT

	// Meta (backslash forms, no semicolons)
	CmdMetaTiming  // \timing on/off, \t
	CmdMetaFormat  // \format table/plain/hex
	CmdMetaPagesize // \pagesize N
	CmdMetaHex     // \x toggle

	// PD admin (new for e2e CLI migration)
	CmdBootstrap      // BOOTSTRAP <storeID> <addr> [<regionID>]
	CmdPutStore       // PUT STORE <id> <addr>
	CmdAllocID        // ALLOC ID
	CmdIsBootstrapped // IS BOOTSTRAPPED
	CmdAskSplit       // ASK SPLIT <regionID> <count>
	CmdReportSplit    // REPORT SPLIT <leftRegionID> <rightRegionID> <splitKey>
	CmdStoreHeartbeat // STORE HEARTBEAT <storeID> [REGIONS <n>]
	CmdBatchScan      // BSCAN <s1> <e1> [<s2> <e2> ...] [EACH_LIMIT <n>]
)

// CAS NOT_EXIST flag bit.
const flagNotExist uint32 = 1

// ---------------------------------------------------------------------------
// Command
// ---------------------------------------------------------------------------

// Command represents a single parsed statement ready for execution.
type Command struct {
	Type    CommandType
	Args    [][]byte          // positional arguments (keys, values) as raw bytes
	IntArg  int64             // numeric argument (limit, TTL, store ID, region ID)
	TxnOpts []client.TxnOption // BEGIN options
	StrArg  string            // string argument for meta commands and STATUS addr
	Flags   uint32            // bitfield: flagNotExist for CAS
}

// ---------------------------------------------------------------------------
// ParseCommand
// ---------------------------------------------------------------------------

// ParseCommand parses a list of tokens into a Command.
// inTxn indicates whether a transaction is currently active, which controls
// context-sensitive dispatch of GET, DELETE, and BGET.
func ParseCommand(tokens []Token, inTxn bool) (Command, error) {
	if len(tokens) == 0 {
		return Command{}, fmt.Errorf("empty statement")
	}

	keyword := strings.ToUpper(string(tokens[0].Value))
	args := tokens[1:]

	switch keyword {
	case "GET":
		return parseGet(args, inTxn)
	case "PUT":
		return parsePut(args)
	case "DELETE":
		return parseDelete(args, inTxn)
	case "SET":
		return parseSet(args, inTxn)
	case "TTL":
		return parseTTL(args)
	case "SCAN":
		return parseScan(args, inTxn)
	case "BGET":
		return parseBGet(args, inTxn)
	case "BPUT":
		return parseBPut(args)
	case "BDELETE":
		return parseBDelete(args)
	case "BSCAN":
		return parseBScan(args)
	case "CAS":
		return parseCAS(args)
	case "CHECKSUM":
		return parseChecksum(args)
	case "BEGIN":
		return parseBegin(args)
	case "COMMIT":
		return parseNoArgs(CmdCommit, args, "COMMIT")
	case "ROLLBACK":
		return parseNoArgs(CmdRollback, args, "ROLLBACK")
	case "STORE":
		return parseStore(args)
	case "REGION":
		return parseRegion(args)
	case "CLUSTER":
		return parseCluster(args)
	case "TSO":
		return parseNoArgs(CmdTSO, args, "TSO")
	case "GC":
		return parseGC(args)
	case "STATUS":
		return parseStatus(args)
	case "HELP":
		return parseNoArgs(CmdHelp, args, "HELP")
	case "EXIT", "QUIT":
		return parseNoArgs(CmdExit, args, keyword)
	case "BOOTSTRAP":
		return parseBootstrap(args)
	case "ALLOC":
		return parseAlloc(args)
	case "IS":
		return parseIs(args)
	case "ASK":
		return parseAsk(args)
	case "REPORT":
		return parseReport(args)
	default:
		return Command{}, fmt.Errorf("unknown command: %s", keyword)
	}
}

// ---------------------------------------------------------------------------
// ParseMetaCommand
// ---------------------------------------------------------------------------

// ParseMetaCommand parses a backslash meta command line.
func ParseMetaCommand(line string) (Command, error) {
	trimmed := strings.TrimSpace(line)
	if len(trimmed) == 0 || trimmed[0] != '\\' {
		return Command{}, fmt.Errorf("not a meta command")
	}

	parts := strings.Fields(trimmed)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case `\help`, `\h`, `\?`:
		return Command{Type: CmdHelp}, nil
	case `\quit`, `\q`:
		return Command{Type: CmdExit}, nil
	case `\timing`:
		if len(parts) >= 2 {
			return Command{Type: CmdMetaTiming, StrArg: strings.ToLower(parts[1])}, nil
		}
		// Toggle (no arg)
		return Command{Type: CmdMetaTiming, StrArg: ""}, nil
	case `\t`:
		// Short form: toggle timing
		return Command{Type: CmdMetaTiming, StrArg: ""}, nil
	case `\format`:
		if len(parts) < 2 {
			return Command{}, fmt.Errorf(`\format requires an argument (table, plain, hex)`)
		}
		return Command{Type: CmdMetaFormat, StrArg: strings.ToLower(parts[1])}, nil
	case `\pagesize`:
		if len(parts) < 2 {
			return Command{}, fmt.Errorf(`\pagesize requires a numeric argument`)
		}
		n, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf(`\pagesize: invalid number: %s`, parts[1])
		}
		return Command{Type: CmdMetaPagesize, IntArg: n}, nil
	case `\x`:
		return Command{Type: CmdMetaHex}, nil
	default:
		return Command{}, fmt.Errorf("unknown meta command: %s", cmd)
	}
}

// ---------------------------------------------------------------------------
// Per-command parsers
// ---------------------------------------------------------------------------

func parseGet(args []Token, inTxn bool) (Command, error) {
	if len(args) != 1 {
		return Command{}, fmt.Errorf("GET requires exactly 1 argument, got %d", len(args))
	}
	cmd := CmdGet
	if inTxn {
		cmd = CmdTxnGet
	}
	return Command{Type: cmd, Args: [][]byte{args[0].Value}}, nil
}

func parsePut(args []Token) (Command, error) {
	if len(args) < 2 {
		return Command{}, fmt.Errorf("PUT requires at least 2 arguments (key value), got %d", len(args))
	}
	// Check for PUT STORE <id> <addr>
	if strings.ToUpper(string(args[0].Value)) == "STORE" {
		if len(args) != 3 {
			return Command{}, fmt.Errorf("PUT STORE requires exactly 2 arguments (id addr), got %d", len(args)-1)
		}
		id, err := strconv.ParseInt(string(args[1].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("PUT STORE: invalid store ID: %s", args[1].Raw)
		}
		return Command{Type: CmdPutStore, IntArg: id, StrArg: string(args[2].Value)}, nil
	}
	if len(args) == 2 {
		return Command{Type: CmdPut, Args: [][]byte{args[0].Value, args[1].Value}}, nil
	}
	// Check for TTL keyword
	if len(args) == 4 && strings.ToUpper(string(args[2].Value)) == "TTL" {
		ttl, err := strconv.ParseInt(string(args[3].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("PUT TTL: invalid number: %s", args[3].Raw)
		}
		return Command{Type: CmdPutTTL, Args: [][]byte{args[0].Value, args[1].Value}, IntArg: ttl}, nil
	}
	return Command{}, fmt.Errorf("PUT: unexpected arguments after value (did you mean PUT key value TTL n?)")
}

func parseDelete(args []Token, inTxn bool) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("DELETE requires at least 1 argument")
	}
	// Check for DELETE RANGE
	if strings.ToUpper(string(args[0].Value)) == "RANGE" {
		if len(args) != 3 {
			return Command{}, fmt.Errorf("DELETE RANGE requires exactly 2 key arguments, got %d", len(args)-1)
		}
		return Command{Type: CmdDeleteRange, Args: [][]byte{args[1].Value, args[2].Value}}, nil
	}
	if len(args) != 1 {
		return Command{}, fmt.Errorf("DELETE requires exactly 1 argument, got %d", len(args))
	}
	cmd := CmdDelete
	if inTxn {
		cmd = CmdTxnDelete
	}
	return Command{Type: cmd, Args: [][]byte{args[0].Value}}, nil
}

func parseSet(args []Token, inTxn bool) (Command, error) {
	if !inTxn {
		return Command{}, fmt.Errorf("SET is only valid inside a transaction (use PUT for raw KV writes)")
	}
	if len(args) != 2 {
		return Command{}, fmt.Errorf("SET requires exactly 2 arguments (key value), got %d", len(args))
	}
	return Command{Type: CmdTxnSet, Args: [][]byte{args[0].Value, args[1].Value}}, nil
}

func parseTTL(args []Token) (Command, error) {
	if len(args) != 1 {
		return Command{}, fmt.Errorf("TTL requires exactly 1 argument, got %d", len(args))
	}
	return Command{Type: CmdTTL, Args: [][]byte{args[0].Value}}, nil
}

func parseScan(args []Token, inTxn bool) (Command, error) {
	if len(args) < 2 {
		return Command{}, fmt.Errorf("SCAN requires at least 2 arguments (start_key end_key), got %d", len(args))
	}
	cmd := CmdScan
	if inTxn {
		cmd = CmdTxnScan
	}
	if len(args) == 2 {
		return Command{Type: cmd, Args: [][]byte{args[0].Value, args[1].Value}, IntArg: 0}, nil
	}
	if len(args) == 4 && strings.ToUpper(string(args[2].Value)) == "LIMIT" {
		limit, err := strconv.ParseInt(string(args[3].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("SCAN LIMIT: invalid number: %s", args[3].Raw)
		}
		return Command{Type: cmd, Args: [][]byte{args[0].Value, args[1].Value}, IntArg: limit}, nil
	}
	return Command{}, fmt.Errorf("SCAN: unexpected arguments (expected: SCAN start end [LIMIT n])")
}

func parseBGet(args []Token, inTxn bool) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("BGET requires at least 1 key argument")
	}
	cmd := CmdBatchGet
	if inTxn {
		cmd = CmdTxnBatchGet
	}
	keys := make([][]byte, len(args))
	for i, a := range args {
		keys[i] = a.Value
	}
	return Command{Type: cmd, Args: keys}, nil
}

func parseBPut(args []Token) (Command, error) {
	if len(args) < 2 {
		return Command{}, fmt.Errorf("BPUT requires at least 2 arguments (key value pairs)")
	}
	if len(args)%2 != 0 {
		return Command{}, fmt.Errorf("BPUT requires an even number of key-value arguments, got %d", len(args))
	}
	pairs := make([][]byte, len(args))
	for i, a := range args {
		pairs[i] = a.Value
	}
	return Command{Type: CmdBatchPut, Args: pairs}, nil
}

func parseBDelete(args []Token) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("BDELETE requires at least 1 key argument")
	}
	keys := make([][]byte, len(args))
	for i, a := range args {
		keys[i] = a.Value
	}
	return Command{Type: CmdBatchDelete, Args: keys}, nil
}

func parseCAS(args []Token) (Command, error) {
	if len(args) < 3 {
		return Command{}, fmt.Errorf("CAS requires at least 3 arguments (key new_value old_value), got %d", len(args))
	}
	// Check for NOT_EXIST flag
	if len(args) == 4 && strings.ToUpper(string(args[3].Value)) == "NOT_EXIST" {
		return Command{
			Type:  CmdCAS,
			Args:  [][]byte{args[0].Value, args[1].Value, args[2].Value},
			Flags: flagNotExist,
		}, nil
	}
	if len(args) != 3 {
		return Command{}, fmt.Errorf("CAS: unexpected arguments (expected: CAS key new old [NOT_EXIST])")
	}
	return Command{
		Type: CmdCAS,
		Args: [][]byte{args[0].Value, args[1].Value, args[2].Value},
	}, nil
}

func parseChecksum(args []Token) (Command, error) {
	switch len(args) {
	case 0:
		return Command{Type: CmdChecksum}, nil
	case 2:
		return Command{Type: CmdChecksum, Args: [][]byte{args[0].Value, args[1].Value}}, nil
	default:
		return Command{}, fmt.Errorf("CHECKSUM requires 0 or 2 arguments, got %d", len(args))
	}
}

func parseBegin(args []Token) (Command, error) {
	var opts []client.TxnOption
	i := 0
	for i < len(args) {
		kw := strings.ToUpper(string(args[i].Value))
		switch kw {
		case "PESSIMISTIC":
			opts = append(opts, client.WithPessimistic())
			i++
		case "ASYNC_COMMIT":
			opts = append(opts, client.WithAsyncCommit())
			i++
		case "ONE_PC":
			opts = append(opts, client.With1PC())
			i++
		case "LOCK_TTL":
			i++
			if i >= len(args) {
				return Command{}, fmt.Errorf("BEGIN LOCK_TTL requires a value")
			}
			ttl, err := strconv.ParseUint(string(args[i].Value), 10, 64)
			if err != nil {
				return Command{}, fmt.Errorf("BEGIN LOCK_TTL: invalid number: %s", args[i].Raw)
			}
			opts = append(opts, client.WithLockTTL(ttl))
			i++
		default:
			return Command{}, fmt.Errorf("unknown BEGIN option: %s", args[i].Raw)
		}
	}
	return Command{Type: CmdBegin, TxnOpts: opts}, nil
}

func parseNoArgs(cmdType CommandType, args []Token, name string) (Command, error) {
	if len(args) != 0 {
		return Command{}, fmt.Errorf("%s takes no arguments, got %d", name, len(args))
	}
	return Command{Type: cmdType}, nil
}

func parseStore(args []Token) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("STORE requires a subcommand (LIST, STATUS, or HEARTBEAT)")
	}
	sub := strings.ToUpper(string(args[0].Value))
	switch sub {
	case "LIST":
		if len(args) != 1 {
			return Command{}, fmt.Errorf("STORE LIST takes no additional arguments")
		}
		return Command{Type: CmdStoreList}, nil
	case "STATUS":
		if len(args) != 2 {
			return Command{}, fmt.Errorf("STORE STATUS requires exactly 1 store ID argument")
		}
		id, err := strconv.ParseInt(string(args[1].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("STORE STATUS: invalid store ID: %s", args[1].Raw)
		}
		return Command{Type: CmdStoreStatus, IntArg: id}, nil
	case "HEARTBEAT":
		if len(args) < 2 {
			return Command{}, fmt.Errorf("STORE HEARTBEAT requires a store ID")
		}
		id, err := strconv.ParseInt(string(args[1].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("STORE HEARTBEAT: invalid store ID: %s", args[1].Raw)
		}
		cmd := Command{Type: CmdStoreHeartbeat, IntArg: id}
		// Optional REGIONS <n>
		if len(args) >= 4 && strings.ToUpper(string(args[2].Value)) == "REGIONS" {
			n, err := strconv.ParseInt(string(args[3].Value), 10, 64)
			if err != nil {
				return Command{}, fmt.Errorf("STORE HEARTBEAT REGIONS: invalid number: %s", args[3].Raw)
			}
			cmd.Args = [][]byte{[]byte(strconv.FormatInt(n, 10))}
		}
		return cmd, nil
	default:
		return Command{}, fmt.Errorf("unknown STORE subcommand: %s (expected LIST, STATUS, or HEARTBEAT)", sub)
	}
}

func parseRegion(args []Token) (Command, error) {
	if len(args) == 0 {
		return Command{}, fmt.Errorf("REGION requires a subcommand or key argument")
	}
	sub := strings.ToUpper(string(args[0].Value))
	switch sub {
	case "LIST":
		if len(args) == 1 {
			return Command{Type: CmdRegionList, IntArg: 0}, nil
		}
		if len(args) == 3 && strings.ToUpper(string(args[1].Value)) == "LIMIT" {
			limit, err := strconv.ParseInt(string(args[2].Value), 10, 64)
			if err != nil {
				return Command{}, fmt.Errorf("REGION LIST LIMIT: invalid number: %s", args[2].Raw)
			}
			return Command{Type: CmdRegionList, IntArg: limit}, nil
		}
		return Command{}, fmt.Errorf("REGION LIST: unexpected arguments (expected: REGION LIST [LIMIT n])")
	case "ID":
		if len(args) != 2 {
			return Command{}, fmt.Errorf("REGION ID requires exactly 1 region ID argument")
		}
		id, err := strconv.ParseInt(string(args[1].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("REGION ID: invalid region ID: %s", args[1].Raw)
		}
		return Command{Type: CmdRegionByID, IntArg: id}, nil
	default:
		// REGION <key> -- look up region by key
		if len(args) != 1 {
			return Command{}, fmt.Errorf("REGION <key> requires exactly 1 key argument")
		}
		return Command{Type: CmdRegion, Args: [][]byte{args[0].Value}}, nil
	}
}

func parseCluster(args []Token) (Command, error) {
	if len(args) == 0 || strings.ToUpper(string(args[0].Value)) != "INFO" {
		return Command{}, fmt.Errorf("CLUSTER requires subcommand INFO")
	}
	if len(args) != 1 {
		return Command{}, fmt.Errorf("CLUSTER INFO takes no additional arguments")
	}
	return Command{Type: CmdClusterInfo}, nil
}

func parseGC(args []Token) (Command, error) {
	if len(args) == 0 || strings.ToUpper(string(args[0].Value)) != "SAFEPOINT" {
		return Command{}, fmt.Errorf("GC requires subcommand SAFEPOINT")
	}
	if len(args) == 1 {
		return Command{Type: CmdGCSafepoint}, nil
	}
	// GC SAFEPOINT SET <timestamp>
	if len(args) == 3 && strings.ToUpper(string(args[1].Value)) == "SET" {
		ts, err := strconv.ParseUint(string(args[2].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("GC SAFEPOINT SET: invalid timestamp: %s", args[2].Raw)
		}
		return Command{Type: CmdGCSafepoint, IntArg: int64(ts), StrArg: "SET"}, nil
	}
	return Command{}, fmt.Errorf("GC SAFEPOINT: unexpected arguments (expected: GC SAFEPOINT [SET <ts>])")
}

func parseStatus(args []Token) (Command, error) {
	if len(args) == 0 {
		return Command{Type: CmdStatus, StrArg: ""}, nil
	}
	if len(args) == 1 {
		return Command{Type: CmdStatus, StrArg: string(args[0].Value)}, nil
	}
	return Command{}, fmt.Errorf("STATUS takes 0 or 1 arguments, got %d", len(args))
}

// ---------------------------------------------------------------------------
// PD admin parsers (e2e CLI migration)
// ---------------------------------------------------------------------------

func parseBootstrap(args []Token) (Command, error) {
	if len(args) < 2 || len(args) > 3 {
		return Command{}, fmt.Errorf("BOOTSTRAP requires 2-3 arguments (storeID addr [regionID]), got %d", len(args))
	}
	storeID, err := strconv.ParseInt(string(args[0].Value), 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("BOOTSTRAP: invalid store ID: %s", args[0].Raw)
	}
	addr := string(args[1].Value)
	cmd := Command{Type: CmdBootstrap, IntArg: storeID, StrArg: addr}
	if len(args) == 3 {
		regionID, err := strconv.ParseInt(string(args[2].Value), 10, 64)
		if err != nil {
			return Command{}, fmt.Errorf("BOOTSTRAP: invalid region ID: %s", args[2].Raw)
		}
		cmd.Args = [][]byte{[]byte(strconv.FormatInt(regionID, 10))}
	}
	return cmd, nil
}

func parseAlloc(args []Token) (Command, error) {
	if len(args) != 1 || strings.ToUpper(string(args[0].Value)) != "ID" {
		return Command{}, fmt.Errorf("expected ALLOC ID")
	}
	return Command{Type: CmdAllocID}, nil
}

func parseIs(args []Token) (Command, error) {
	if len(args) != 1 || strings.ToUpper(string(args[0].Value)) != "BOOTSTRAPPED" {
		return Command{}, fmt.Errorf("expected IS BOOTSTRAPPED")
	}
	return Command{Type: CmdIsBootstrapped}, nil
}

func parseAsk(args []Token) (Command, error) {
	if len(args) < 1 || strings.ToUpper(string(args[0].Value)) != "SPLIT" {
		return Command{}, fmt.Errorf("expected ASK SPLIT <regionID> <count>")
	}
	if len(args) != 3 {
		return Command{}, fmt.Errorf("ASK SPLIT requires exactly 2 arguments (regionID count), got %d", len(args)-1)
	}
	regionID, err := strconv.ParseInt(string(args[1].Value), 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("ASK SPLIT: invalid region ID: %s", args[1].Raw)
	}
	count, err := strconv.ParseInt(string(args[2].Value), 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("ASK SPLIT: invalid count: %s", args[2].Raw)
	}
	return Command{Type: CmdAskSplit, IntArg: regionID, Args: [][]byte{[]byte(strconv.FormatInt(count, 10))}}, nil
}

func parseReport(args []Token) (Command, error) {
	if len(args) < 1 || strings.ToUpper(string(args[0].Value)) != "SPLIT" {
		return Command{}, fmt.Errorf("expected REPORT SPLIT <leftRegionID> <rightRegionID> <splitKey>")
	}
	if len(args) != 4 {
		return Command{}, fmt.Errorf("REPORT SPLIT requires exactly 3 arguments (leftRegionID rightRegionID splitKey), got %d", len(args)-1)
	}
	leftID, err := strconv.ParseInt(string(args[1].Value), 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("REPORT SPLIT: invalid left region ID: %s", args[1].Raw)
	}
	rightID, err := strconv.ParseInt(string(args[2].Value), 10, 64)
	if err != nil {
		return Command{}, fmt.Errorf("REPORT SPLIT: invalid right region ID: %s", args[2].Raw)
	}
	splitKey := args[3].Value
	return Command{
		Type:   CmdReportSplit,
		IntArg: leftID,
		Args:   [][]byte{[]byte(strconv.FormatInt(rightID, 10)), splitKey},
	}, nil
}

func parseBScan(args []Token) (Command, error) {
	// BSCAN <s1> <e1> [<s2> <e2> ...] [EACH_LIMIT <n>]
	if len(args) < 2 {
		return Command{}, fmt.Errorf("BSCAN requires at least 2 arguments (start end), got %d", len(args))
	}

	// Check for optional EACH_LIMIT at the end.
	var eachLimit int64
	remaining := args
	if len(args) >= 2 {
		secondLast := strings.ToUpper(string(args[len(args)-2].Value))
		if secondLast == "EACH_LIMIT" {
			limit, err := strconv.ParseInt(string(args[len(args)-1].Value), 10, 64)
			if err != nil {
				return Command{}, fmt.Errorf("BSCAN EACH_LIMIT: invalid number: %s", args[len(args)-1].Raw)
			}
			eachLimit = limit
			remaining = args[:len(args)-2]
		}
	}

	if len(remaining)%2 != 0 {
		return Command{}, fmt.Errorf("BSCAN requires an even number of range arguments (start end pairs), got %d", len(remaining))
	}
	if len(remaining) == 0 {
		return Command{}, fmt.Errorf("BSCAN requires at least one range (start end)")
	}

	// Pack all range keys as Args.
	rangeArgs := make([][]byte, len(remaining))
	for i, a := range remaining {
		rangeArgs[i] = a.Value
	}
	return Command{Type: CmdBatchScan, Args: rangeArgs, IntArg: eachLimit}, nil
}
