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
	"sync"
	"sync/atomic"
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
	fuzzDefaultClients   = 8
	fuzzStabilizeTimeout = 180 * time.Second
	fuzzNodeCount        = 3
	fuzzFaultInterval    = 10 * time.Second // time between fault injection events
)

// --- Operation types and weights ---

type clientOpType int

const (
	copTransfer clientOpType = iota
	copMultiTransfer
	copAudit
	copBatchRead
)

type clientOpWeight struct {
	op     clientOpType
	weight int
}

var clientOpWeights = []clientOpWeight{
	{copTransfer, 40},
	{copMultiTransfer, 25},
	{copAudit, 10},
	{copBatchRead, 25},
}

var clientTotalWeight int

type faultOpType int

const (
	fopStopNode faultOpType = iota
	fopRestartNode
	fopKillLeader
)

type faultOpWeight struct {
	op     faultOpType
	weight int
}

var faultOpWeights = []faultOpWeight{
	{fopStopNode, 40},
	{fopRestartNode, 40},
	{fopKillLeader, 20},
}

var faultTotalWeight int

func init() {
	for _, w := range clientOpWeights {
		clientTotalWeight += w.weight
	}
	for _, w := range faultOpWeights {
		faultTotalWeight += w.weight
	}
}

func pickClientOp(rng *rand.Rand) clientOpType {
	r := rng.IntN(clientTotalWeight)
	cum := 0
	for _, w := range clientOpWeights {
		cum += w.weight
		if r < cum {
			return w.op
		}
	}
	return copTransfer
}

func pickFaultOp(rng *rand.Rand) faultOpType {
	r := rng.IntN(faultTotalWeight)
	cum := 0
	for _, w := range faultOpWeights {
		cum += w.weight
		if r < cum {
			return w.op
		}
	}
	return fopStopNode
}

// --- Atomic stats (goroutine-safe) ---

type fuzzStats struct {
	transfers      atomic.Int64
	multiTransfers atomic.Int64
	audits         atomic.Int64
	batchReads     atomic.Int64
	txnSuccesses   atomic.Int64
	txnConflicts   atomic.Int64
	txnTransient   atomic.Int64
	stopNodes      atomic.Int64
	restartNodes   atomic.Int64
	killLeaders    atomic.Int64
	auditPasses    atomic.Int64
	auditSkips     atomic.Int64
}

func (s *fuzzStats) String() string {
	return fmt.Sprintf(
		"transfers=%d multiTransfers=%d audits=%d(pass=%d skip=%d) batchReads=%d "+
			"txnOK=%d conflicts=%d transient=%d faults(stop=%d restart=%d killLeader=%d)",
		s.transfers.Load(), s.multiTransfers.Load(),
		s.audits.Load(), s.auditPasses.Load(), s.auditSkips.Load(),
		s.batchReads.Load(),
		s.txnSuccesses.Load(), s.txnConflicts.Load(), s.txnTransient.Load(),
		s.stopNodes.Load(), s.restartNodes.Load(), s.killLeaders.Load(),
	)
}

// --- Shared cluster state (protected by mutex for nodeUp) ---

type fuzzCluster struct {
	t       *testing.T
	cluster *e2elib.GokvCluster
	pdAddr  string

	mu     sync.Mutex
	nodeUp []bool

	stats fuzzStats
}

func (fc *fuzzCluster) majority() int {
	return len(fc.nodeUp)/2 + 1
}

func (fc *fuzzCluster) runningCount() int {
	n := 0
	for _, up := range fc.nodeUp {
		if up {
			n++
		}
	}
	return n
}

// --- Per-goroutine client ---

type fuzzClient struct {
	fc  *fuzzCluster
	rng *rand.Rand
	id  int
}

func accountKey(i int) []byte {
	return []byte(fmt.Sprintf("account-%d", i))
}

func isTxnRetryable(err error) bool {
	return errors.Is(err, client.ErrWriteConflict) || errors.Is(err, client.ErrDeadlock)
}

// --- Client transaction operations ---

func (c *fuzzClient) doTransfer() {
	c.fc.stats.transfers.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a := c.rng.IntN(fuzzNumAccounts)
	b := c.rng.IntN(fuzzNumAccounts)
	for b == a {
		b = c.rng.IntN(fuzzNumAccounts)
	}

	txn, err := c.fc.cluster.TxnKV().Begin(ctx)
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		return
	}

	valA, err := txn.Get(ctx, accountKey(a))
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}
	valB, err := txn.Get(ctx, accountKey(b))
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}
	if valA == nil || valB == nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}

	balA, _ := strconv.Atoi(string(valA))
	balB, _ := strconv.Atoi(string(valB))

	if balA <= 0 {
		_ = txn.Rollback(ctx)
		return
	}

	amount := c.rng.IntN(min(balA, 100)) + 1
	newA := balA - amount
	newB := balB + amount

	if err := txn.Set(ctx, accountKey(a), []byte(strconv.Itoa(newA))); err != nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}
	if err := txn.Set(ctx, accountKey(b), []byte(strconv.Itoa(newB))); err != nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}

	if err := txn.Commit(ctx); err != nil {
		if isTxnRetryable(err) {
			c.fc.stats.txnConflicts.Add(1)
		} else {
			c.fc.stats.txnTransient.Add(1)
		}
		_ = txn.Rollback(ctx)
		return
	}
	c.fc.stats.txnSuccesses.Add(1)
}

func (c *fuzzClient) doMultiTransfer() {
	c.fc.stats.multiTransfers.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n := c.rng.IntN(3) + 3 // 3 to 5 accounts
	indices := c.pickDistinct(n, fuzzNumAccounts)

	txn, err := c.fc.cluster.TxnKV().Begin(ctx)
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		return
	}

	balances := make([]int, n)
	total := 0
	for i, idx := range indices {
		val, err := txn.Get(ctx, accountKey(idx))
		if err != nil {
			c.fc.stats.txnTransient.Add(1)
			_ = txn.Rollback(ctx)
			return
		}
		if val == nil {
			c.fc.stats.txnTransient.Add(1)
			_ = txn.Rollback(ctx)
			return
		}
		balances[i], _ = strconv.Atoi(string(val))
		total += balances[i]
	}

	newBalances := c.randomSplit(total, n)

	for i, idx := range indices {
		if err := txn.Set(ctx, accountKey(idx), []byte(strconv.Itoa(newBalances[i]))); err != nil {
			c.fc.stats.txnTransient.Add(1)
			_ = txn.Rollback(ctx)
			return
		}
	}

	if err := txn.Commit(ctx); err != nil {
		if isTxnRetryable(err) {
			c.fc.stats.txnConflicts.Add(1)
		} else {
			c.fc.stats.txnTransient.Add(1)
		}
		_ = txn.Rollback(ctx)
		return
	}
	c.fc.stats.txnSuccesses.Add(1)
}

func (c *fuzzClient) doAudit() (int, error) {
	c.fc.stats.audits.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	txn, err := c.fc.cluster.TxnKV().Begin(ctx)
	if err != nil {
		c.fc.stats.auditSkips.Add(1)
		return 0, fmt.Errorf("begin: %w", err)
	}

	total := 0
	for i := 0; i < fuzzNumAccounts; i++ {
		val, err := txn.Get(ctx, accountKey(i))
		if err != nil {
			_ = txn.Rollback(ctx)
			c.fc.stats.auditSkips.Add(1)
			return 0, fmt.Errorf("get account-%d: %w", i, err)
		}
		if val == nil {
			_ = txn.Rollback(ctx)
			c.fc.stats.auditSkips.Add(1)
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

	_ = txn.Commit(ctx)

	if total != fuzzExpectedTotal {
		return total, fmt.Errorf("balance mismatch: got %d, want %d", total, fuzzExpectedTotal)
	}

	c.fc.stats.auditPasses.Add(1)
	return total, nil
}

func (c *fuzzClient) doBatchRead() {
	c.fc.stats.batchReads.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	n := c.rng.IntN(6) + 5 // 5 to 10 accounts
	indices := c.pickDistinct(n, fuzzNumAccounts)

	txn, err := c.fc.cluster.TxnKV().Begin(ctx)
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		return
	}

	keys := make([][]byte, n)
	for i, idx := range indices {
		keys[i] = accountKey(idx)
	}

	pairs, err := txn.BatchGet(ctx, keys)
	if err != nil {
		c.fc.stats.txnTransient.Add(1)
		_ = txn.Rollback(ctx)
		return
	}

	for _, pair := range pairs {
		b, err := strconv.Atoi(string(pair.Value))
		if err == nil && b < 0 {
			c.fc.t.Errorf("[client %d] negative balance: key=%s val=%d", c.id, pair.Key, b)
		}
	}

	_ = txn.Commit(ctx)
}

// --- Client helpers ---

func (c *fuzzClient) pickDistinct(n, max int) []int {
	if n > max {
		n = max
	}
	set := make(map[int]struct{}, n)
	for len(set) < n {
		set[c.rng.IntN(max)] = struct{}{}
	}
	result := make([]int, 0, n)
	for v := range set {
		result = append(result, v)
	}
	return result
}

func (c *fuzzClient) randomSplit(total, n int) []int {
	if n == 1 {
		return []int{total}
	}
	if total == 0 {
		return make([]int, n)
	}
	breaks := make([]int, n-1)
	for i := range breaks {
		breaks[i] = c.rng.IntN(total + 1)
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

// clientLoop runs transaction operations until opsLeft reaches zero or ctx is cancelled.
func (c *fuzzClient) clientLoop(ctx context.Context, opsLeft *atomic.Int64) {
	for opsLeft.Add(-1) >= 0 {
		if ctx.Err() != nil {
			return
		}
		op := pickClientOp(c.rng)
		switch op {
		case copTransfer:
			c.doTransfer()
		case copMultiTransfer:
			c.doMultiTransfer()
		case copAudit:
			total, err := c.doAudit()
			if err != nil {
				if strings.Contains(err.Error(), "mismatch") || strings.Contains(err.Error(), "negative") {
					c.fc.t.Errorf("[client %d] INVARIANT VIOLATION: %v", c.id, err)
				}
			} else {
				_ = total
			}
		case copBatchRead:
			c.doBatchRead()
		}
	}
}

// --- Fault injection (runs on dedicated goroutine) ---

var fuzzStoreIDRegex = regexp.MustCompile(`store:(\d+)`)

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

func (fc *fuzzCluster) doStopNode(rng *rand.Rand) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.runningCount() <= fc.majority() {
		return
	}

	var candidates []int
	for i, up := range fc.nodeUp {
		if up {
			candidates = append(candidates, i)
		}
	}
	idx := candidates[rng.IntN(len(candidates))]

	if err := fc.cluster.StopNode(idx); err != nil {
		fc.t.Logf("[fault] stopNode %d: %v", idx, err)
		return
	}
	fc.nodeUp[idx] = false
	fc.cluster.ResetClient()
	fc.stats.stopNodes.Add(1)
	fc.t.Logf("[fault] stopped node %d (running=%d)", idx, fc.runningCount())
}

func (fc *fuzzCluster) doRestartNode(rng *rand.Rand) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	var candidates []int
	for i, up := range fc.nodeUp {
		if !up {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) == 0 {
		return
	}
	idx := candidates[rng.IntN(len(candidates))]

	if err := fc.cluster.RestartNode(idx); err != nil {
		fc.t.Logf("[fault] restartNode %d: %v", idx, err)
		return
	}
	if err := fc.cluster.Node(idx).WaitForReady(30 * time.Second); err != nil {
		fc.t.Logf("[fault] node %d not ready: %v", idx, err)
		return
	}
	fc.nodeUp[idx] = true
	fc.cluster.ResetClient()
	fc.stats.restartNodes.Add(1)
	fc.t.Logf("[fault] restarted node %d (running=%d)", idx, fc.runningCount())
}

func (fc *fuzzCluster) doKillLeader(rng *rand.Rand) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if fc.runningCount() <= fc.majority() {
		return
	}

	// Release lock for CLI call (blocking I/O).
	fc.mu.Unlock()
	stdout, _, err := e2elib.CLIExecRaw(fc.t, fc.pdAddr, "REGION \"\"")
	fc.mu.Lock()

	if err != nil {
		return
	}

	leaderID, ok := fuzzParseLeaderStore(stdout)
	if !ok || leaderID == 0 {
		return
	}

	leaderIdx := int(leaderID) - 1
	if leaderIdx < 0 || leaderIdx >= len(fc.nodeUp) || !fc.nodeUp[leaderIdx] {
		return
	}

	// Re-check majority after re-acquiring lock.
	if fc.runningCount() <= fc.majority() {
		return
	}

	if err := fc.cluster.StopNode(leaderIdx); err != nil {
		fc.t.Logf("[fault] killLeader node %d: %v", leaderIdx, err)
		return
	}
	fc.nodeUp[leaderIdx] = false
	fc.cluster.ResetClient()
	fc.stats.killLeaders.Add(1)
	fc.t.Logf("[fault] killed leader node %d (running=%d)", leaderIdx, fc.runningCount())
}

// faultLoop injects faults at regular intervals until ctx is cancelled.
// Also prints stats every 10 seconds.
func (fc *fuzzCluster) faultLoop(ctx context.Context, rng *rand.Rand) {
	statsTicker := time.NewTicker(10 * time.Second)
	defer statsTicker.Stop()
	faultTicker := time.NewTicker(fuzzFaultInterval)
	defer faultTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-statsTicker.C:
			fc.t.Logf("[stats] %s", &fc.stats)
		case <-faultTicker.C:
			op := pickFaultOp(rng)
			switch op {
			case fopStopNode:
				fc.doStopNode(rng)
			case fopRestartNode:
				fc.doRestartNode(rng)
			case fopKillLeader:
				fc.doKillLeader(rng)
			}
		}
	}
}

// --- Main Test ---

func TestFuzzCluster(t *testing.T) {
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
	numClients := fuzzDefaultClients
	if s := os.Getenv("FUZZ_CLIENTS"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			numClients = v
		}
	}
	t.Logf("Fuzz seed: %d  iterations: %d  clients: %d  nodes: %d",
		seed, iterations, numClients, fuzzNodeCount)

	// Start cluster with split config for multi-region distribution.
	e2elib.SkipIfNoBinary(t, "gookv-server", "gookv-pd")
	cluster := e2elib.NewGokvCluster(t, e2elib.GokvClusterConfig{
		NumNodes:            fuzzNodeCount,
		SplitSize:           "128KB",
		SplitCheckInterval:  "5s",
		PdHeartbeatInterval: "5s",
	})
	require.NoError(t, cluster.Start())
	t.Cleanup(func() { cluster.Stop() })

	// Wait for Raft leader election.
	electionTimeout := 300 * time.Second
	rawKV := cluster.RawKV()
	e2elib.WaitForCondition(t, electionTimeout, "cluster leader election", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return rawKV.Put(ctx, []byte("__health__"), []byte("ok")) == nil
	})

	pdAddr := cluster.PD().Addr()

	// Seed accounts with retry. Region splits may occur during seeding
	// (SplitSize=128KB), causing transient "not leader" errors.
	t.Logf("Seeding %d accounts with initial balance %d...", fuzzNumAccounts, fuzzInitialBalance)
	{
		ctx := context.Background()
		balanceStr := strconv.Itoa(fuzzInitialBalance)
		batchSize := 50
		for start := 0; start < fuzzNumAccounts; start += batchSize {
			end := start + batchSize
			if end > fuzzNumAccounts {
				end = fuzzNumAccounts
			}
			var committed bool
			for attempt := 0; attempt < 10; attempt++ {
				txn, err := cluster.TxnKV().Begin(ctx)
				if err != nil {
					time.Sleep(2 * time.Second)
					cluster.ResetClient()
					continue
				}
				failed := false
				for i := start; i < end; i++ {
					if err := txn.Set(ctx, accountKey(i), []byte(balanceStr)); err != nil {
						failed = true
						break
					}
				}
				if failed {
					_ = txn.Rollback(ctx)
					time.Sleep(2 * time.Second)
					cluster.ResetClient()
					continue
				}
				if err := txn.Commit(ctx); err != nil {
					_ = txn.Rollback(ctx)
					t.Logf("Seed batch [%d,%d) attempt %d: %v", start, end, attempt+1, err)
					time.Sleep(2 * time.Second)
					cluster.ResetClient()
					continue
				}
				committed = true
				break
			}
			require.True(t, committed, "seed batch [%d,%d) failed after retries", start, end)
		}
	}

	// Wait for region topology to stabilize: no changes in region count,
	// peers, or leaders for 30 seconds.
	t.Log("Waiting for region topology to stabilize...")
	lastSnapshot := ""
	stableSince := time.Now()
	deadline := time.Now().Add(300 * time.Second)
	for time.Now().Before(deadline) {
		stdout, _, err := e2elib.CLIExecRaw(t, pdAddr, "REGION LIST")
		snap := ""
		if err == nil {
			// Strip the timing line (e.g. "(3 rows, 1.2ms)") to avoid false diffs.
			var filtered []string
			for _, line := range strings.Split(stdout, "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
					continue
				}
				filtered = append(filtered, line)
			}
			snap = strings.Join(filtered, "\n")
		}
		if snap != lastSnapshot {
			// Count regions from output for logging.
			lines := strings.Split(snap, "\n")
			regionRows := 0
			for _, line := range lines {
				if strings.HasPrefix(strings.TrimSpace(line), "|") && !strings.Contains(line, "RegionID") && !strings.Contains(line, "---") {
					regionRows++
				}
			}
			t.Logf("Region topology changed: %d regions", regionRows)
			lastSnapshot = snap
			stableSince = time.Now()
		}
		if time.Since(stableSince) >= 30*time.Second {
			break
		}
		time.Sleep(3 * time.Second)
	}
	t.Logf("Region topology stabilized (stable for 30s)")

	// Initialize shared cluster state.
	fc := &fuzzCluster{
		t:       t,
		cluster: cluster,
		pdAddr:  pdAddr,
		nodeUp:  make([]bool, fuzzNodeCount),
	}
	for i := range fc.nodeUp {
		fc.nodeUp[i] = true
	}

	// Verify initial invariant. Retry because post-split leader propagation takes time.
	// ResetClient once to start fresh, then let the client learn leaders via retries.
	cluster.ResetClient()
	initClient := &fuzzClient{fc: fc, rng: rand.New(rand.NewPCG(uint64(seed), uint64(seed>>32)))}
	var initTotal int
	var initErr error
	for attempt := 0; attempt < 10; attempt++ {
		initTotal, initErr = initClient.doAudit()
		if initErr == nil {
			break
		}
		t.Logf("Initial audit attempt %d: %v (retrying...)", attempt+1, initErr)
		time.Sleep(5 * time.Second)
		cluster.ResetClient() // refresh region cache after splits/balance
	}
	require.NoError(t, initErr, "initial audit failed after retries")
	require.Equal(t, fuzzExpectedTotal, initTotal, "initial total balance mismatch")
	t.Logf("Initial audit passed: total=%d", initTotal)

	// Shared operation counter for all client goroutines.
	var opsLeft atomic.Int64
	opsLeft.Store(int64(iterations))

	// Create a cancellable context so the fault loop stops when clients finish.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Launch client goroutines.
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		c := &fuzzClient{
			fc:  fc,
			rng: rand.New(rand.NewPCG(uint64(seed)+uint64(i)*7919, uint64(seed>>32)+uint64(i)*6271)),
			id:  i,
		}
		go func() {
			defer wg.Done()
			c.clientLoop(ctx, &opsLeft)
		}()
	}

	// Launch fault injection goroutine.
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	go func() {
		defer faultWg.Done()
		faultRng := rand.New(rand.NewPCG(uint64(seed)+999983, uint64(seed>>32)+999979))
		fc.faultLoop(ctx, faultRng)
	}()

	// Wait for all client operations to complete.
	wg.Wait()
	cancel() // signal fault loop to stop
	faultWg.Wait()

	t.Logf("Chaos phase complete. Stats so far: %s", &fc.stats)

	// Restore all nodes.
	t.Log("Restoring all nodes...")
	fc.mu.Lock()
	for i := 0; i < len(fc.nodeUp); i++ {
		if !fc.nodeUp[i] {
			require.NoError(t, cluster.RestartNode(i))
			require.NoError(t, cluster.Node(i).WaitForReady(60*time.Second))
			fc.nodeUp[i] = true
			time.Sleep(2 * time.Second)
		}
	}
	fc.mu.Unlock()
	cluster.ResetClient()

	// Wait for cluster stabilization via CLI.
	e2elib.CLIWaitForCondition(t, pdAddr, "PUT __fuzz-health__ ok",
		func(output string) bool {
			return strings.Contains(output, "OK")
		}, fuzzStabilizeTimeout)

	// Warmup Go client after fault recovery.
	cluster.ResetClient()
	e2elib.WaitForCondition(t, 60*time.Second, "go client warmup", func() bool {
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

	// Final consistency check.
	t.Log("Running final audit...")
	auditClient := &fuzzClient{
		fc:  fc,
		rng: rand.New(rand.NewPCG(uint64(seed)+111, uint64(seed>>32)+222)),
	}
	var finalTotal int
	var finalErr error
	for attempt := 0; attempt < 10; attempt++ {
		finalTotal, finalErr = auditClient.doAudit()
		if finalErr == nil {
			break
		}
		t.Logf("Final audit attempt %d failed: %v (retrying...)", attempt+1, finalErr)
		time.Sleep(5 * time.Second)
	}
	require.NoError(t, finalErr, "final audit failed after retries")
	assert.Equal(t, fuzzExpectedTotal, finalTotal, "final total balance mismatch")

	t.Logf("Final stats: %s", &fc.stats)
	t.Logf("Final audit: total=%d (expected=%d)", finalTotal, fuzzExpectedTotal)
}
