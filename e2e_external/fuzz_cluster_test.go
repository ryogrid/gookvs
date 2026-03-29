package e2e_external_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/client"
	"github.com/ryogrid/gookv/pkg/e2elib"
)

// Fuzz test constants.
const (
	fuzzNumAccounts      = 5000
	fuzzInitialBalance   = 1000
	fuzzExpectedTotal    = fuzzNumAccounts * fuzzInitialBalance
	fuzzDefaultIters     = 500
	fuzzStabilizeTimeout = 60 * time.Second
	fuzzNodeCount        = 3
)

// opType represents the type of operation in the fuzz loop.
type opType int

const (
	opTransfer opType = iota
	opMultiTransfer
	opAudit
	opBatchRead
	opStopNode
	opRestartNode
	opKillLeader
)

// opWeight associates an operation with a selection weight.
type opWeight struct {
	op     opType
	weight int
}

var opWeights = []opWeight{
	{opTransfer, 30},
	{opMultiTransfer, 20},
	{opAudit, 15},
	{opBatchRead, 10},
	{opStopNode, 10},
	{opRestartNode, 10},
	{opKillLeader, 5},
}

var totalWeight int

func init() {
	for _, w := range opWeights {
		totalWeight += w.weight
	}
}

// pickOp selects a random operation based on weights.
func pickOp(rng *rand.Rand) opType {
	r := rng.IntN(totalWeight)
	cum := 0
	for _, w := range opWeights {
		cum += w.weight
		if r < cum {
			return w.op
		}
	}
	return opTransfer
}

// fuzzStats tracks operation counts during the fuzz run.
type fuzzStats struct {
	transfers      int
	multiTransfers int
	audits         int
	batchReads     int
	txnSuccesses   int
	txnConflicts   int
	txnTransient   int
	stopNodes      int
	restartNodes   int
	killLeaders    int
	auditPasses    int
	auditSkips     int
}

func (s fuzzStats) String() string {
	return fmt.Sprintf(
		"transfers=%d multiTransfers=%d audits=%d(pass=%d skip=%d) batchReads=%d "+
			"txnOK=%d conflicts=%d transient=%d faults(stop=%d restart=%d killLeader=%d)",
		s.transfers, s.multiTransfers, s.audits, s.auditPasses, s.auditSkips, s.batchReads,
		s.txnSuccesses, s.txnConflicts, s.txnTransient,
		s.stopNodes, s.restartNodes, s.killLeaders,
	)
}

// fuzzState holds all state for the fuzz run.
type fuzzState struct {
	t       *testing.T
	cluster *e2elib.GokvCluster
	pdAddr  string
	rng     *rand.Rand
	nodeUp  [fuzzNodeCount]bool
	stats   fuzzStats
}

// runningCount returns the number of currently running nodes.
func (s *fuzzState) runningCount() int {
	n := 0
	for _, up := range s.nodeUp {
		if up {
			n++
		}
	}
	return n
}

// accountKey returns the key for account i (matching SeedAccounts format).
func accountKey(i int) []byte {
	return []byte(fmt.Sprintf("account-%d", i))
}

// isTxnRetryable returns true if the error is a retryable transaction conflict.
func isTxnRetryable(err error) bool {
	return errors.Is(err, client.ErrWriteConflict) || errors.Is(err, client.ErrDeadlock)
}

// --- Transaction Operations ---

// doTransfer performs a 2-account balance transfer within a single transaction.
func (s *fuzzState) doTransfer() {
	s.stats.transfers++
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a := s.rng.IntN(fuzzNumAccounts)
	b := s.rng.IntN(fuzzNumAccounts)
	for b == a {
		b = s.rng.IntN(fuzzNumAccounts)
	}

	txn, err := s.cluster.TxnKV().Begin(ctx)
	if err != nil {
		s.stats.txnTransient++
		s.t.Logf("[transfer] begin err: %v", err)
		return
	}

	valA, err := txn.Get(ctx, accountKey(a))
	if err != nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}
	valB, err := txn.Get(ctx, accountKey(b))
	if err != nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}
	if valA == nil || valB == nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}

	balA, _ := strconv.Atoi(string(valA))
	balB, _ := strconv.Atoi(string(valB))

	if balA <= 0 {
		_ = txn.Rollback(ctx)
		return
	}

	amount := s.rng.IntN(min(balA, 100)) + 1
	newA := balA - amount
	newB := balB + amount

	if err := txn.Set(ctx, accountKey(a), []byte(strconv.Itoa(newA))); err != nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}
	if err := txn.Set(ctx, accountKey(b), []byte(strconv.Itoa(newB))); err != nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}

	if err := txn.Commit(ctx); err != nil {
		if isTxnRetryable(err) {
			s.stats.txnConflicts++
		} else {
			s.stats.txnTransient++
		}
		_ = txn.Rollback(ctx)
		return
	}
	s.stats.txnSuccesses++
}

// doMultiTransfer performs a 3-5 account redistribution within a single transaction.
// The total balance across the selected accounts is preserved.
func (s *fuzzState) doMultiTransfer() {
	s.stats.multiTransfers++
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n := s.rng.IntN(3) + 3 // 3 to 5 accounts
	indices := s.pickDistinct(n, fuzzNumAccounts)

	txn, err := s.cluster.TxnKV().Begin(ctx)
	if err != nil {
		s.stats.txnTransient++
		return
	}

	balances := make([]int, n)
	total := 0
	for i, idx := range indices {
		val, err := txn.Get(ctx, accountKey(idx))
		if err != nil {
			s.stats.txnTransient++
			_ = txn.Rollback(ctx)
			return
		}
		if val == nil {
			s.stats.txnTransient++
			_ = txn.Rollback(ctx)
			return
		}
		balances[i], _ = strconv.Atoi(string(val))
		total += balances[i]
	}

	// Redistribute: random split of total into n non-negative integers.
	newBalances := s.randomSplit(total, n)

	for i, idx := range indices {
		if err := txn.Set(ctx, accountKey(idx), []byte(strconv.Itoa(newBalances[i]))); err != nil {
			s.stats.txnTransient++
			_ = txn.Rollback(ctx)
			return
		}
	}

	if err := txn.Commit(ctx); err != nil {
		if isTxnRetryable(err) {
			s.stats.txnConflicts++
		} else {
			s.stats.txnTransient++
		}
		_ = txn.Rollback(ctx)
		return
	}
	s.stats.txnSuccesses++
}

// doAudit reads all accounts in a single transaction and verifies the total.
// Returns (total, error). error is non-nil on invariant violation or transient failure.
func (s *fuzzState) doAudit() (int, error) {
	s.stats.audits++
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	txn, err := s.cluster.TxnKV().Begin(ctx)
	if err != nil {
		s.stats.auditSkips++
		return 0, fmt.Errorf("begin: %w", err)
	}

	total := 0
	for i := 0; i < fuzzNumAccounts; i++ {
		val, err := txn.Get(ctx, accountKey(i))
		if err != nil {
			_ = txn.Rollback(ctx)
			s.stats.auditSkips++
			return 0, fmt.Errorf("get account-%d: %w", i, err)
		}
		if val == nil {
			_ = txn.Rollback(ctx)
			s.stats.auditSkips++
			return 0, fmt.Errorf("account-%d not found", i)
		}
		b, err := strconv.Atoi(string(val))
		if err != nil {
			_ = txn.Rollback(ctx)
			return 0, fmt.Errorf("parse account-%d balance %q: %w", i, val, err)
		}
		if b < 0 {
			_ = txn.Rollback(ctx)
			return total + b, fmt.Errorf("account-%d has negative balance: %d", i, b)
		}
		total += b
	}

	// Read-only commit; ignore errors.
	_ = txn.Commit(ctx)

	if total != fuzzExpectedTotal {
		return total, fmt.Errorf("balance mismatch: got %d, want %d", total, fuzzExpectedTotal)
	}

	s.stats.auditPasses++
	return total, nil
}

// doBatchRead reads a random subset of accounts and verifies balances are non-negative.
func (s *fuzzState) doBatchRead() {
	s.stats.batchReads++
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n := s.rng.IntN(6) + 5 // 5 to 10 accounts
	indices := s.pickDistinct(n, fuzzNumAccounts)

	txn, err := s.cluster.TxnKV().Begin(ctx)
	if err != nil {
		s.stats.txnTransient++
		return
	}

	keys := make([][]byte, n)
	for i, idx := range indices {
		keys[i] = accountKey(idx)
	}

	pairs, err := txn.BatchGet(ctx, keys)
	if err != nil {
		s.stats.txnTransient++
		_ = txn.Rollback(ctx)
		return
	}

	for _, pair := range pairs {
		b, err := strconv.Atoi(string(pair.Value))
		if err == nil && b < 0 {
			s.t.Errorf("[batchRead] negative balance: key=%s val=%d", pair.Key, b)
		}
	}

	_ = txn.Commit(ctx)
}

// --- Fault Injection ---

var fuzzStoreIDRegex = regexp.MustCompile(`store:(\d+)`)

// fuzzParseLeaderStore extracts the leader store ID from REGION command output.
func fuzzParseLeaderStore(output string) (uint64, bool) {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "Leader:") {
			m := fuzzStoreIDRegex.FindStringSubmatch(line)
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

func (s *fuzzState) doStopNode() {
	if s.runningCount() <= 2 {
		return // keep majority
	}

	// Pick a random running node.
	var candidates []int
	for i, up := range s.nodeUp {
		if up {
			candidates = append(candidates, i)
		}
	}
	idx := candidates[s.rng.IntN(len(candidates))]

	if err := s.cluster.StopNode(idx); err != nil {
		s.t.Logf("[stopNode] node %d: %v", idx, err)
		return
	}
	s.nodeUp[idx] = false
	s.cluster.ResetClient()
	s.stats.stopNodes++
	time.Sleep(500 * time.Millisecond)
}

func (s *fuzzState) doRestartNode() {
	// Find a stopped node.
	var candidates []int
	for i, up := range s.nodeUp {
		if !up {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return
	}
	idx := candidates[s.rng.IntN(len(candidates))]

	if err := s.cluster.RestartNode(idx); err != nil {
		s.t.Logf("[restartNode] node %d restart: %v", idx, err)
		return
	}
	if err := s.cluster.Node(idx).WaitForReady(30 * time.Second); err != nil {
		s.t.Logf("[restartNode] node %d not ready: %v", idx, err)
		return
	}
	s.nodeUp[idx] = true
	s.cluster.ResetClient()
	s.stats.restartNodes++
	time.Sleep(1 * time.Second)
}

func (s *fuzzState) doKillLeader() {
	if s.runningCount() < fuzzNodeCount {
		return // only safe when all nodes are up
	}

	stdout, _, err := e2elib.CLIExecRaw(s.t, s.pdAddr, "REGION \"\"")
	if err != nil {
		s.t.Logf("[killLeader] REGION cmd err: %v", err)
		return
	}

	leaderID, ok := fuzzParseLeaderStore(stdout)
	if !ok || leaderID == 0 {
		s.t.Logf("[killLeader] could not parse leader from output")
		return
	}

	leaderIdx := int(leaderID) - 1
	if leaderIdx < 0 || leaderIdx >= fuzzNodeCount || !s.nodeUp[leaderIdx] {
		s.t.Logf("[killLeader] invalid leader idx %d", leaderIdx)
		return
	}

	if err := s.cluster.StopNode(leaderIdx); err != nil {
		s.t.Logf("[killLeader] stop node %d: %v", leaderIdx, err)
		return
	}
	s.nodeUp[leaderIdx] = false
	s.cluster.ResetClient()
	s.stats.killLeaders++
	time.Sleep(2 * time.Second)
}

// --- Helpers ---

// pickDistinct returns n distinct random integers in [0, max).
func (s *fuzzState) pickDistinct(n, max int) []int {
	if n > max {
		n = max
	}
	set := make(map[int]struct{}, n)
	for len(set) < n {
		set[s.rng.IntN(max)] = struct{}{}
	}
	result := make([]int, 0, n)
	for v := range set {
		result = append(result, v)
	}
	return result
}

// randomSplit splits total into n non-negative integers that sum to total.
// Uses the "stars and bars" method: generate n-1 random breakpoints in [0, total].
func (s *fuzzState) randomSplit(total, n int) []int {
	if n == 1 {
		return []int{total}
	}
	if total == 0 {
		return make([]int, n)
	}

	breaks := make([]int, n-1)
	for i := range breaks {
		breaks[i] = s.rng.IntN(total + 1)
	}
	sort.Ints(breaks)

	result := make([]int, n)
	prev := 0
	for i, b := range breaks {
		result[i] = b - prev
		prev = b
	}
	result[n-1] = total - prev
	return result
}

// --- Main Test ---

func TestFuzzCluster(t *testing.T) {
	e2elib.SkipIfNoBinary(t, "gookv-server", "gookv-pd", "gookv-cli")

	// Parse configuration from environment.
	seed := time.Now().UnixNano()
	if s := os.Getenv("FUZZ_SEED"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			seed = v
		}
	}
	iterations := fuzzDefaultIters
	if s := os.Getenv("FUZZ_ITERATIONS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			iterations = v
		}
	}
	t.Logf("Fuzz seed: %d  iterations: %d", seed, iterations)

	// Start cluster.
	cluster := newClusterWithLeader(t)
	pdAddr := cluster.PD().Addr()

	// Seed accounts.
	t.Logf("Seeding %d accounts with initial balance %d...", fuzzNumAccounts, fuzzInitialBalance)
	e2elib.SeedAccounts(t, cluster.TxnKV(), fuzzNumAccounts, fuzzInitialBalance)

	// Verify initial invariant.
	total, balances := e2elib.ReadAllBalances(t, cluster.TxnKV(), fuzzNumAccounts)
	require.Equal(t, fuzzExpectedTotal, total, "initial total balance mismatch")
	for i, b := range balances {
		require.GreaterOrEqual(t, b, 0, "initial account-%d has negative balance", i)
	}
	t.Logf("Initial audit passed: total=%d", total)

	// Initialize fuzz state.
	state := &fuzzState{
		t:       t,
		cluster: cluster,
		pdAddr:  pdAddr,
		rng:     rand.New(rand.NewPCG(uint64(seed), uint64(seed>>32))),
		nodeUp:  [fuzzNodeCount]bool{true, true, true},
	}

	// Chaos loop.
	for i := 0; i < iterations; i++ {
		op := pickOp(state.rng)
		switch op {
		case opTransfer:
			state.doTransfer()
		case opMultiTransfer:
			state.doMultiTransfer()
		case opAudit:
			got, err := state.doAudit()
			if err != nil {
				// During chaos, transient errors are expected.
				// Invariant violations are real failures.
				if strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "negative") {
					t.Errorf("[iter %d] INVARIANT VIOLATION: %v", i, err)
				} else {
					t.Logf("[iter %d] audit skipped (transient): %v", i, err)
				}
			} else {
				if i%50 == 0 {
					t.Logf("[iter %d] audit ok: total=%d", i, got)
				}
			}
		case opBatchRead:
			state.doBatchRead()
		case opStopNode:
			state.doStopNode()
		case opRestartNode:
			state.doRestartNode()
		case opKillLeader:
			state.doKillLeader()
		}
	}

	// Restore all nodes.
	t.Log("Restoring all nodes...")
	for i := 0; i < fuzzNodeCount; i++ {
		if !state.nodeUp[i] {
			require.NoError(t, cluster.RestartNode(i))
			require.NoError(t, cluster.Node(i).WaitForReady(30*time.Second))
			state.nodeUp[i] = true
		}
	}
	cluster.ResetClient()

	// Wait for cluster stabilization via CLI (creates fresh connections).
	e2elib.CLIWaitForCondition(t, pdAddr, "PUT __fuzz-health__ ok",
		func(output string) bool {
			return strings.Contains(output, "OK")
		}, fuzzStabilizeTimeout)

	// Warmup the Go client connection after fault recovery.
	// ResetClient() cleared the cached client; the next TxnKV() call creates a fresh one.
	// Do a lightweight txn to ensure the new client's gRPC connections are live.
	cluster.ResetClient()
	e2elib.WaitForCondition(t, 30*time.Second, "go client warmup", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		txn, err := cluster.TxnKV().Begin(ctx)
		if err != nil {
			return false
		}
		_, err = txn.Get(ctx, []byte("account-0"))
		_ = txn.Commit(ctx)
		return err == nil
	})

	// Final consistency check using doAudit (has per-operation timeouts).
	t.Log("Running final audit...")
	var finalTotal int
	var finalErr error
	for attempt := 0; attempt < 3; attempt++ {
		finalTotal, finalErr = state.doAudit()
		if finalErr == nil {
			break
		}
		t.Logf("Final audit attempt %d failed: %v (retrying...)", attempt+1, finalErr)
		time.Sleep(5 * time.Second)
		cluster.ResetClient()
	}
	require.NoError(t, finalErr, "final audit failed after retries")
	assert.Equal(t, fuzzExpectedTotal, finalTotal, "final total balance mismatch")

	t.Logf("Fuzz stats: %s", state.stats)
	t.Logf("Final audit: total=%d (expected=%d)", finalTotal, fuzzExpectedTotal)
}
