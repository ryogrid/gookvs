package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
)

// ---------------------------------------------------------------------------
// Prompt helpers
// ---------------------------------------------------------------------------

func promptFor(inTxn bool, hasBuffer bool) string {
	if inTxn {
		if hasBuffer {
			return "           > " // 13 chars, aligns with gookv(txn)>
		}
		return "gookv(txn)> "
	}
	if hasBuffer {
		return "     > " // 7 chars, aligns with gookv>
	}
	return "gookv> "
}

// ---------------------------------------------------------------------------
// REPL loop
// ---------------------------------------------------------------------------

func runREPL(ctx context.Context, exec *Executor, fmtr *Formatter, pdEndpoints string) int {
	historyFile := filepath.Join(os.Getenv("HOME"), ".gookv_history")

	rl, err := readline.NewEx(&readline.Config{
		Prompt:            promptFor(false, false),
		HistoryFile:       historyFile,
		HistorySearchFold: true,
		InterruptPrompt:   "^C",
		EOFPrompt:         "",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: init readline: %v\n", err)
		return 1
	}
	defer rl.Close()

	exec.isInteractive = true
	fmt.Fprintf(fmtr.out, "Connected to gookv cluster (PD: %s)\n", pdEndpoints)
	fmt.Fprintf(fmtr.out, "Type \\help for help, \\q to quit.\n")

	var buf strings.Builder

	for {
		rl.SetPrompt(promptFor(exec.InTransaction(), buf.Len() > 0))

		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			// Ctrl-C: discard buffer, return to prompt
			buf.Reset()
			continue
		}
		if err == io.EOF {
			// Ctrl-D: exit
			return handleEOF(ctx, exec, fmtr)
		}

		trimmed := strings.TrimSpace(line)

		// Meta commands (backslash-prefixed) execute immediately, no semicolon.
		if IsMetaCommand(trimmed) {
			exitCode := handleMetaCommand(trimmed, exec, fmtr)
			if exitCode >= 0 {
				return exitCode
			}
			continue
		}

		// Accumulate input
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(line)

		// Split on semicolons
		stmts, _, complete := SplitStatements(buf.String())
		for _, s := range stmts {
			exitCode := executeStatement(ctx, s, exec, fmtr)
			if exec.WantExit() {
				return 0
			}
			_ = exitCode
		}
		if complete {
			buf.Reset()
		}
	}
}

// handleEOF handles Ctrl-D (EOF). Returns exit code.
func handleEOF(ctx context.Context, exec *Executor, fmtr *Formatter) int {
	if exec.InTransaction() {
		fmt.Fprintln(fmtr.out, "\nWARNING: Active transaction will be rolled back.")
		fmt.Fprintln(fmtr.out, "Rolling back...")
		_ = exec.activeTxn.Rollback(ctx)
		exec.activeTxn = nil
	}
	fmt.Fprintln(fmtr.out, "Goodbye.")
	return 0
}

// handleMetaCommand handles backslash meta commands.
// Returns -1 to continue REPL, or an exit code >= 0.
func handleMetaCommand(input string, exec *Executor, fmtr *Formatter) int {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return -1
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case `\help`, `\h`, `\?`:
		fmt.Fprintln(fmtr.out, helpText)
	case `\quit`, `\q`:
		if exec.InTransaction() {
			fmt.Fprintln(fmtr.out, "WARNING: Active transaction will be rolled back.")
			fmt.Fprintln(fmtr.out, "Rolling back...")
			ctx := context.Background()
			_ = exec.activeTxn.Rollback(ctx)
			exec.activeTxn = nil
		}
		fmt.Fprintln(fmtr.out, "Goodbye.")
		return 0
	case `\timing`:
		if len(parts) >= 2 {
			switch strings.ToLower(parts[1]) {
			case "on":
				fmtr.SetTiming(true)
				fmt.Fprintln(fmtr.out, "Timing: ON")
			case "off":
				fmtr.SetTiming(false)
				fmt.Fprintln(fmtr.out, "Timing: OFF")
			default:
				fmt.Fprintf(fmtr.errOut, "ERROR: usage: \\timing on|off\n")
			}
		} else {
			// Toggle
			on := !fmtr.showTiming
			fmtr.SetTiming(on)
			if on {
				fmt.Fprintln(fmtr.out, "Timing: ON")
			} else {
				fmt.Fprintln(fmtr.out, "Timing: OFF")
			}
		}
	case `\t`:
		// Short form: toggle timing
		on := !fmtr.showTiming
		fmtr.SetTiming(on)
		if on {
			fmt.Fprintln(fmtr.out, "Timing: ON")
		} else {
			fmt.Fprintln(fmtr.out, "Timing: OFF")
		}
	case `\format`:
		if len(parts) < 2 {
			fmt.Fprintf(fmtr.errOut, "ERROR: usage: \\format table|plain|hex\n")
			return -1
		}
		switch strings.ToLower(parts[1]) {
		case "table":
			fmtr.SetOutputFormat(FormatTable)
			fmt.Fprintln(fmtr.out, "Format: TABLE")
		case "plain":
			fmtr.SetOutputFormat(FormatPlain)
			fmt.Fprintln(fmtr.out, "Format: PLAIN")
		case "hex":
			fmtr.SetOutputFormat(FormatHex)
			fmt.Fprintln(fmtr.out, "Format: HEX")
		default:
			fmt.Fprintf(fmtr.errOut, "ERROR: unknown format %q\n", parts[1])
		}
	case `\pagesize`:
		if len(parts) < 2 {
			fmt.Fprintf(fmtr.out, "Page size: %d\n", exec.defaultScanLimit)
			return -1
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil || n <= 0 {
			fmt.Fprintf(fmtr.errOut, "ERROR: pagesize must be a positive integer\n")
			return -1
		}
		exec.defaultScanLimit = n
		fmt.Fprintf(fmtr.out, "Page size: %d\n", n)
	case `\x`:
		// Cycle display mode: Auto -> Hex -> String -> Auto
		switch fmtr.displayMode {
		case DisplayAuto:
			fmtr.SetDisplayMode(DisplayHex)
			fmt.Fprintln(fmtr.out, "Display mode: HEX")
		case DisplayHex:
			fmtr.SetDisplayMode(DisplayString)
			fmt.Fprintln(fmtr.out, "Display mode: STRING")
		case DisplayString:
			fmtr.SetDisplayMode(DisplayAuto)
			fmt.Fprintln(fmtr.out, "Display mode: AUTO")
		}
	default:
		fmt.Fprintf(fmtr.errOut, "ERROR: unknown meta command %q (try \\help)\n", parts[0])
	}
	return -1
}

// executeStatement tokenizes, parses, and executes a single statement.
// Returns 0 on success, 1 on error.
func executeStatement(ctx context.Context, stmt string, exec *Executor, fmtr *Formatter) int {
	trimmed := strings.TrimSpace(stmt)
	if trimmed == "" {
		return 0
	}

	tokens, err := Tokenize(trimmed)
	if err != nil {
		fmtr.FormatError(err)
		return 1
	}
	if len(tokens) == 0 {
		return 0
	}

	cmd, err := ParseCommand(tokens, exec.InTransaction())
	if err != nil {
		fmtr.FormatError(err)
		return 1
	}

	cmdCtx, cmdCancel := context.WithCancel(ctx)
	defer cmdCancel()

	start := time.Now()
	result, err := exec.Exec(cmdCtx, cmd)
	elapsed := time.Since(start)
	if err != nil {
		fmtr.FormatError(err)
		return 1
	}
	if result != nil {
		fmtr.Format(result, elapsed)
	}
	return 0
}

// ---------------------------------------------------------------------------
// Batch mode
// ---------------------------------------------------------------------------

func runBatch(ctx context.Context, exec *Executor, fmtr *Formatter, input string) int {
	stmts, _, _ := SplitStatements(input)
	for _, stmt := range stmts {
		trimmed := strings.TrimSpace(stmt)
		if trimmed == "" {
			continue
		}
		tokens, err := Tokenize(trimmed)
		if err != nil {
			fmtr.FormatError(err)
			return 1
		}
		if len(tokens) == 0 {
			continue
		}
		cmd, err := ParseCommand(tokens, exec.InTransaction())
		if err != nil {
			fmtr.FormatError(err)
			return 1
		}
		start := time.Now()
		result, err := exec.Exec(ctx, cmd)
		elapsed := time.Since(start)
		if err != nil {
			fmtr.FormatError(err)
			return 1
		}
		if result != nil {
			fmtr.Format(result, elapsed)
		}
		if exec.WantExit() {
			return 0
		}
	}
	return 0
}

// ---------------------------------------------------------------------------
// Pipe mode (stdin is not a TTY)
// ---------------------------------------------------------------------------

func isTerminal() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return true // default to interactive
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func runPipe(ctx context.Context, exec *Executor, fmtr *Formatter) int {
	scanner := bufio.NewScanner(os.Stdin)
	var buf strings.Builder

	for scanner.Scan() {
		line := scanner.Text()
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(line)

		stmts, _, complete := SplitStatements(buf.String())
		for _, s := range stmts {
			exitCode := executeStatement(ctx, s, exec, fmtr)
			if exitCode != 0 {
				return 1
			}
			if exec.WantExit() {
				return 0
			}
		}
		if complete {
			buf.Reset()
		}
	}
	return 0
}
