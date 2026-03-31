package e2elib

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ensureSemicolon appends a semicolon if the statement doesn't end with one.
// gookv-cli's -c flag requires semicolons to delimit statements.
func ensureSemicolon(stmt string) string {
	s := strings.TrimSpace(stmt)
	if s == "" || s[len(s)-1] == ';' {
		return s
	}
	return s + ";"
}

// CLIExec executes a gookv-cli statement against a PD-connected cluster.
// Returns trimmed stdout. Fatals on non-zero exit or timeout.
func CLIExec(t *testing.T, pdAddr string, stmt string) string {
	t.Helper()
	return cliRun(t, []string{"--pd", pdAddr, "-c", ensureSemicolon(stmt)})
}

// CLINodeExec executes a gookv-cli statement against a specific KV node.
// Uses --addr flag for direct connection (bypasses PD).
func CLINodeExec(t *testing.T, nodeAddr string, stmt string) string {
	t.Helper()
	return cliRun(t, []string{"--addr", nodeAddr, "-c", ensureSemicolon(stmt)})
}

// CLINodeExecRaw executes a CLI statement against a specific KV node
// and returns (stdout, stderr, error). Does not fatal on failure.
func CLINodeExecRaw(nodeAddr string, stmt string) (string, string, error) {
	return cliRunRaw([]string{"--addr", nodeAddr, "-c", ensureSemicolon(stmt)})
}

// CLIExecRaw executes a CLI statement and returns (stdout, stderr, error).
// Does not fatal on failure. Used by polling helpers.
func CLIExecRaw(t *testing.T, pdAddr string, stmt string) (string, string, error) {
	t.Helper()
	return cliRunRaw([]string{"--pd", pdAddr, "-c", ensureSemicolon(stmt)})
}

// cliRun is the internal helper that executes gookv-cli with given args.
func cliRun(t *testing.T, args []string) string {
	t.Helper()
	cliBin := FindBinary("gookv-cli")
	if cliBin == "" {
		t.Fatal("gookv-cli binary not found; run 'make build' first")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, cliBin, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("gookv-cli %v failed: %v\nOutput: %s", args, err, string(out))
	}
	return strings.TrimSpace(string(out))
}

// cliRunRaw is the internal helper that returns (stdout, stderr, error).
func cliRunRaw(args []string) (string, string, error) {
	cliBin := FindBinary("gookv-cli")
	if cliBin == "" {
		return "", "", fmt.Errorf("gookv-cli binary not found")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, cliBin, args...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()), err
}

// CLIPut writes a key-value pair via CLI PUT command.
func CLIPut(t *testing.T, pdAddr string, key, value string) {
	t.Helper()
	CLIExec(t, pdAddr, fmt.Sprintf("PUT %s %s", key, value))
}

// CLIGet reads a key via CLI GET command. Returns (value, found).
func CLIGet(t *testing.T, pdAddr string, key string) (string, bool) {
	t.Helper()
	out := CLIExec(t, pdAddr, fmt.Sprintf("GET %s", key))
	return parseCLIGetOutput(out)
}

// parseCLIGetOutput extracts a value from CLI GET output.
// Output format: "value"\n(1 row, X ms) or (not found)\n(0 rows, X ms)
func parseCLIGetOutput(out string) (string, bool) {
	if strings.Contains(out, "(not found)") {
		return "", false
	}
	// Take first line (before timing info), strip surrounding quotes.
	firstLine := strings.SplitN(out, "\n", 2)[0]
	firstLine = strings.TrimSpace(firstLine)
	if len(firstLine) >= 2 && firstLine[0] == '"' && firstLine[len(firstLine)-1] == '"' {
		return firstLine[1 : len(firstLine)-1], true
	}
	return firstLine, true
}

// CLIDelete deletes a key via CLI DELETE command.
func CLIDelete(t *testing.T, pdAddr string, key string) {
	t.Helper()
	CLIExec(t, pdAddr, fmt.Sprintf("DELETE %s", key))
}

// CLINodeGet reads a key from a specific node via --addr. Returns (value, found).
func CLINodeGet(t *testing.T, nodeAddr string, key string) (string, bool) {
	t.Helper()
	out := CLINodeExec(t, nodeAddr, fmt.Sprintf("GET %s", key))
	return parseCLIGetOutput(out)
}

// countTableRows counts non-header, non-border rows in table output.
// Counts lines that start with '|' and contain data (not just borders).
func countTableRows(output string) int {
	lines := strings.Split(output, "\n")
	count := 0
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Skip border lines (only + and -)
		if strings.Trim(line, "+-") == "" {
			continue
		}
		// Skip header (first data row after border)
		if strings.HasPrefix(line, "|") {
			count++
		}
	}
	// Subtract 1 for the header row
	if count > 0 {
		count--
	}
	return count
}

// parseScalar extracts a scalar value from CLI output.
// Handles formats like "123" or "true" or "TTL: 0 (no expiration)".
func parseScalar(output string) string {
	// For simple single-line output, return trimmed
	output = strings.TrimSpace(output)
	// Strip surrounding quotes if present
	if len(output) >= 2 && output[0] == '"' && output[len(output)-1] == '"' {
		return output[1 : len(output)-1]
	}
	return output
}

var storeIDRegex = regexp.MustCompile(`store:(\d+)`)

// parseLeaderStoreID extracts the leader store ID from REGION command output.
// Looks for "Leader:    store:<id>" pattern.
func parseLeaderStoreID(output string) (uint64, bool) {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.Contains(line, "Leader:") {
			m := storeIDRegex.FindStringSubmatch(line)
			if len(m) >= 2 {
				id, err := strconv.ParseUint(m[1], 10, 64)
				if err == nil {
					return id, true
				}
			}
		}
	}
	return 0, false
}

// CLIWaitForCondition polls a CLI command until checkFn returns true.
func CLIWaitForCondition(t *testing.T, pdAddr string, stmt string,
	checkFn func(output string) bool, timeout time.Duration) {
	t.Helper()
	WaitForCondition(t, timeout, fmt.Sprintf("CLI: %s", stmt), func() bool {
		out, _, err := CLIExecRaw(t, pdAddr, stmt)
		if err != nil {
			return false
		}
		return checkFn(out)
	})
}

// CLIWaitForStoreCount waits until STORE LIST shows at least minCount stores.
func CLIWaitForStoreCount(t *testing.T, pdAddr string, minCount int, timeout time.Duration) int {
	t.Helper()
	var count int
	WaitForCondition(t, timeout, fmt.Sprintf("waiting for %d stores", minCount), func() bool {
		out, _, err := CLIExecRaw(t, pdAddr, "STORE LIST")
		if err != nil {
			return false
		}
		count = countTableRows(out)
		return count >= minCount
	})
	return count
}

// CLIWaitForRegionLeader waits until REGION <key> shows a leader.
func CLIWaitForRegionLeader(t *testing.T, pdAddr string, key string, timeout time.Duration) uint64 {
	t.Helper()
	var leaderID uint64
	// Quote the key for the CLI; empty string needs explicit ""
	quotedKey := fmt.Sprintf("%q", key)
	stmt := fmt.Sprintf("REGION %s", quotedKey)
	WaitForCondition(t, timeout, fmt.Sprintf("waiting for region leader for key %q", key), func() bool {
		out, _, err := CLIExecRaw(t, pdAddr, stmt)
		if err != nil {
			return false
		}
		id, ok := parseLeaderStoreID(out)
		if ok && id > 0 {
			leaderID = id
			return true
		}
		return false
	})
	return leaderID
}

// CLIWaitForRegionCount waits until REGION LIST shows at least minCount regions.
func CLIWaitForRegionCount(t *testing.T, pdAddr string, minCount int, timeout time.Duration) int {
	t.Helper()
	var count int
	WaitForCondition(t, timeout, fmt.Sprintf("waiting for %d regions", minCount), func() bool {
		out, _, err := CLIExecRaw(t, pdAddr, "REGION LIST")
		if err != nil {
			return false
		}
		count = countTableRows(out)
		return count >= minCount
	})
	return count
}

// CLIParallel executes a CLI statement against multiple addresses concurrently.
func CLIParallel(t *testing.T, addrs []string, stmt string) map[string]string {
	t.Helper()
	results := make(map[string]string, len(addrs))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, a := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			out := CLIExec(t, addr, stmt)
			mu.Lock()
			results[addr] = out
			mu.Unlock()
		}(a)
	}
	wg.Wait()
	return results
}

// CLITxnScan performs a transactional SCAN via CLI.
// Executes BEGIN; SCAN startKey endKey [LIMIT n]; ROLLBACK as a batch.
func CLITxnScan(t *testing.T, pdAddr string, startKey, endKey string, limit int) string {
	t.Helper()
	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}
	stmt := fmt.Sprintf("BEGIN; SCAN %s %s%s; ROLLBACK", startKey, endKey, limitClause)
	return CLIExec(t, pdAddr, stmt)
}

// CLITxnScanRaw is the non-fatal variant that returns (stdout, stderr, error).
func CLITxnScanRaw(t *testing.T, pdAddr string, startKey, endKey string, limit int) (string, string, error) {
	t.Helper()
	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}
	stmt := fmt.Sprintf("BEGIN; SCAN %s %s%s; ROLLBACK", startKey, endKey, limitClause)
	return CLIExecRaw(t, pdAddr, stmt)
}
