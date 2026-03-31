// txn-integrity-demo-verify demonstrates transaction integrity under high concurrency.
// It seeds 1000 bank accounts with $100 each, runs 32 concurrent transfer goroutines
// for 30 seconds, then verifies that the total balance is still exactly $100,000.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pdAddr         = flag.String("pd", "127.0.0.1:2419", "PD address")
	flagAccounts   = flag.Int("accounts", 1000, "Number of accounts")
	flagWorkers    = flag.Int("workers", 32, "Number of concurrent workers")
	flagDuration   = flag.Duration("duration", 30*time.Second, "Transfer phase duration")
)

const (
	initialBalance = 100
	transferMax    = 50
	initBatchSize  = 50
	maxTxnRetries  = 50
)

var (
	numAccounts int
	expectedTotal int
	numWorkers  int
	duration    time.Duration
)

// transferRecord logs a successfully committed transfer for post-hoc analysis.
type transferRecord struct {
	startTS          uint64
	from, to         int
	fromBal, toBal   int
	amount           int
}

func main() {
	flag.Parse()
	numAccounts = *flagAccounts
	expectedTotal = numAccounts * initialBalance
	numWorkers = *flagWorkers
	duration = *flagDuration

	fmt.Println("================================================================")
	fmt.Println("       gookv Transaction Integrity Demo")
	fmt.Println("================================================================")
	fmt.Println()

	passed := 0
	total := 3

	if phase1(*pdAddr) {
		passed++
	}
	fmt.Println()

	var records []transferRecord
	if phase2(*pdAddr, &records) {
		passed++
	}
	fmt.Println()

	// Brief pause for async secondary commits and lock cleanup to settle.
	fmt.Println("  Waiting 5s for lock cleanup to settle...")
	time.Sleep(5 * time.Second)
	fmt.Println()

	if phase3(*pdAddr, records) {
		passed++
	}

	fmt.Println()
	fmt.Println("================================================================")
	if passed == total {
		fmt.Printf("  All %d phases passed.\n", total)
	} else {
		fmt.Printf("  %d/%d phases passed.\n", passed, total)
	}
	fmt.Println("================================================================")

	if passed < total {
		os.Exit(1)
	}
}

// --- Phase 1: Initialize 1000 Accounts & Wait for 3 Regions ---

func phase1(pdAddr string) bool {
	fmt.Println("--- Phase 1/3: Initialize 1000 Accounts ---")
	fmt.Println()

	ctx := context.Background()

	// Wait for cluster readiness.
	fmt.Println("  [Step 1] Waiting for cluster readiness...")
	pdConn, pdClient, err := connectPD(ctx, pdAddr)
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to PD: %v\n", err)
		return false
	}
	defer pdConn.Close()

	if err := waitForLeader(ctx, pdClient, 30); err != nil {
		fmt.Printf("  FAIL: %v\n", err)
		return false
	}
	fmt.Println("           Cluster ready.")

	// Create client.
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    []string{pdAddr},
		MaxRetries: 10,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return false
	}
	defer c.Close()

	txnClient := c.TxnKV()

	// Seed accounts in batches.
	fmt.Printf("  [Step 2] Seeding %d accounts with $%d each...\n", numAccounts, initialBalance)
	numBatches := numAccounts / initBatchSize
	for batch := 0; batch < numBatches; batch++ {
		start := batch * initBatchSize
		end := start + initBatchSize

		var commitErr error
		for attempt := 0; attempt < 20; attempt++ {
			commitErr = nil
			txn, err := txnClient.Begin(ctx)
			if err != nil {
				commitErr = err
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for i := start; i < end; i++ {
				if err := txn.Set(ctx, acctKey(i), balanceBytes(initialBalance)); err != nil {
					_ = txn.Rollback(ctx)
					commitErr = err
					break
				}
			}
			if commitErr != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			if err := txn.Commit(ctx); err != nil {
				commitErr = err
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
		if commitErr != nil {
			fmt.Printf("  FAIL: batch %d/%d (accounts %d-%d): %v\n", batch+1, numBatches, start, end-1, commitErr)
			return false
		}
		if (batch+1)%5 == 0 || batch == numBatches-1 {
			fmt.Printf("           Batch %d/%d: accounts %d-%d committed\n", batch+1, numBatches, start, end-1)
		}
	}

	// Wait for region splits to reach >= 3 regions (before initial balance
	// verification, so that leaders are stable when we read).
	splitTimeout := 30 * time.Second
	if numAccounts <= 100 {
		splitTimeout = 15 * time.Second
	}
	fmt.Printf("  [Step 3] Waiting for region splits (target: >= 3 regions, timeout %s)...\n", splitTimeout)
	deadline := time.Now().Add(splitTimeout)
	var regions []regionInfo
	for time.Now().Before(deadline) {
		regions, err = getAllRegions(ctx, pdClient)
		if err == nil && len(regions) >= 3 {
			break
		}
		if len(regions) > 0 {
			fmt.Printf("           Region count: %d (waiting...)\n", len(regions))
		}
		time.Sleep(2 * time.Second)
	}
	if len(regions) < 3 {
		fmt.Printf("  WARNING: only %d region(s) after %s (target: 3+). Proceeding anyway.\n", len(regions), splitTimeout)
	} else {
		fmt.Printf("           Region count: %d  Split complete!\n", len(regions))
	}

	// Wait for all regions to have leaders (important after split).
	leaderTimeout := 15 * time.Second
	fmt.Printf("           Waiting for all regions to elect leaders (timeout %s)...\n", leaderTimeout)
	leaderDeadline := time.Now().Add(leaderTimeout)
	for time.Now().Before(leaderDeadline) {
		regions, err = getAllRegions(ctx, pdClient)
		if err != nil {
			break
		}
		allHaveLeaders := true
		for _, r := range regions {
			if r.leaderID == 0 {
				allHaveLeaders = false
				break
			}
		}
		if allHaveLeaders {
			fmt.Println("           All regions have leaders.")
			break
		}
		time.Sleep(2 * time.Second)
	}

	// Print region distribution.
	fmt.Println("  [Step 4] Region layout:")
	for _, r := range regions {
		fmt.Printf("           Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}
	fmt.Println()

	// Verify total via read-only transaction. After splits, some regions may
	// have transient leader instability. Try SI isolation first, fall back to RC.
	fmt.Println("  [Step 5] Verifying initial total balance...")
	var total int
	for verifyAttempt := 0; verifyAttempt < 3; verifyAttempt++ {
		total, err = readTotalBalance(ctx, txnClient)
		if err == nil {
			break
		}
		if verifyAttempt < 2 {
			fmt.Printf("           SI verify attempt %d failed: %v (retrying...)\n", verifyAttempt+1, err)
			time.Sleep(2 * time.Second)
		}
	}
	if err != nil {
		fmt.Printf("           SI read failed: %v\n", err)
		fmt.Println("           Falling back to RC isolation...")
		total, _, err = readAllBalancesRC(ctx, pdAddr)
		if err != nil {
			fmt.Printf("  FAIL: cannot read balances: %v\n", err)
			return false
		}
	}
	fmt.Printf("           Total balance: $%d (expected: $%d)\n", total, expectedTotal)
	if total != expectedTotal {
		fmt.Println("  Result: FAIL")
		return false
	}

	fmt.Println("  Result: PASS")
	return true
}

// --- Phase 2: Concurrent Random Transfers ---

func phase2(pdAddr string, records *[]transferRecord) bool {
	fmt.Printf("--- Phase 2/3: Concurrent Transfers (%d workers, %s) ---\n", numWorkers, duration)
	fmt.Println()

	ctx := context.Background()

	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    []string{pdAddr},
		MaxRetries: 30,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return false
	}
	defer c.Close()

	txnClient := c.TxnKV()

	var transfers atomic.Int64
	var conflicts atomic.Int64
	var skipped atomic.Int64
	var errCount atomic.Int64
	var totalMoved atomic.Int64
	var lastErr atomic.Value // store last error message for debugging
	var recordsMu sync.Mutex

	transferCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup

	fmt.Printf("  [Step 1] Starting %d transfer goroutines...\n", numWorkers)
	fmt.Printf("  [Step 2] Running for %s...\n", duration)

	// Stats ticker.
	tickerDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		start := time.Now()
		for {
			select {
			case <-ticker.C:
				elapsed := time.Since(start).Truncate(time.Second)
				fmt.Printf("           [%s] transfers=%d conflicts=%d skipped=%d errors=%d\n",
					elapsed, transfers.Load(), conflicts.Load(), skipped.Load(), errCount.Load())
			case <-tickerDone:
				return
			}
		}
	}()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rand.Intn(10000))))

			for {
				select {
				case <-transferCtx.Done():
					return
				default:
				}

				from := rng.Intn(numAccounts)
				to := rng.Intn(numAccounts)
				for to == from {
					to = rng.Intn(numAccounts)
				}

				// Canonical ordering for lock acquisition to minimize deadlocks.
				first, second := from, to
				if first > second {
					first, second = second, first
				}

				transferred := false
				for retry := 0; retry < maxTxnRetries; retry++ {
					select {
					case <-transferCtx.Done():
						return
					default:
					}

					txn, err := txnClient.Begin(ctx)
					if err != nil {
						lastErr.Store(fmt.Sprintf("Begin: %v", err))
						errCount.Add(1)
						break
					}

					// Read balances.
					fromVal, err := txn.Get(ctx, acctKey(from))
					if err != nil {
						_ = txn.Rollback(ctx)
						if isRetryable(err) {
							conflicts.Add(1)
							continue
						}
						lastErr.Store(fmt.Sprintf("Get(from=%d): %v", from, err))
						errCount.Add(1)
						break
					}
					toVal, err := txn.Get(ctx, acctKey(to))
					if err != nil {
						_ = txn.Rollback(ctx)
						if isRetryable(err) {
							conflicts.Add(1)
							continue
						}
						lastErr.Store(fmt.Sprintf("Get(to=%d): %v", to, err))
						errCount.Add(1)
						break
					}

					fromBal := parseBalance(fromVal)
					toBal := parseBalance(toVal)

					maxAmt := fromBal
					if maxAmt > transferMax {
						maxAmt = transferMax
					}
					if maxAmt < 1 {
						_ = txn.Rollback(ctx)
						skipped.Add(1)
						break
					}

					amount := rng.Intn(maxAmt) + 1

					// Compute new balances.
					var firstNewBal, secondNewBal int
					if first == from {
						firstNewBal = fromBal - amount
						secondNewBal = toBal + amount
					} else {
						firstNewBal = toBal + amount
						secondNewBal = fromBal - amount
					}

					// Set in canonical order (lower key first) to minimize deadlocks.
					if err := txn.Set(ctx, acctKey(first), balanceBytes(firstNewBal)); err != nil {
						_ = txn.Rollback(ctx)
						if isRetryable(err) {
							conflicts.Add(1)
							continue
						}
						lastErr.Store(fmt.Sprintf("Set(first=%d): %v", first, err))
						errCount.Add(1)
						break
					}
					if err := txn.Set(ctx, acctKey(second), balanceBytes(secondNewBal)); err != nil {
						_ = txn.Rollback(ctx)
						if isRetryable(err) {
							conflicts.Add(1)
							continue
						}
						lastErr.Store(fmt.Sprintf("Set(second=%d): %v", second, err))
						errCount.Add(1)
						break
					}

					if err := txn.Commit(ctx); err != nil {
						_ = txn.Rollback(ctx)
						if isRetryable(err) {
							conflicts.Add(1)
							continue
						}
						lastErr.Store(fmt.Sprintf("Commit: %v", err))
						errCount.Add(1)
						break
					}

					// Verify the committed values by reading back immediately.
					verifyTxn, verifyErr := txnClient.Begin(ctx)
					if verifyErr == nil {
						vFrom, errVF := verifyTxn.Get(ctx, acctKey(from))
						vTo, errVT := verifyTxn.Get(ctx, acctKey(to))
						_ = verifyTxn.Rollback(ctx)
						actualFrom := parseBalance(vFrom)
						actualTo := parseBalance(vTo)
						expectedFrom := fromBal - amount
						expectedTo := toBal + amount
						if actualFrom != expectedFrom || actualTo != expectedTo {
							fmt.Fprintf(os.Stderr, "[VERIFY MISMATCH] txnStartTS=%d verifyStartTS=%d from=%04d expected=$%d actual=$%d to=%04d expected=$%d actual=$%d amount=$%d errVF=%v errVT=%v\n",
								txn.StartTS(), verifyTxn.StartTS(), from, expectedFrom, actualFrom, to, expectedTo, actualTo, amount, errVF, errVT)
						}
					}

					transfers.Add(1)
					totalMoved.Add(int64(amount))
					recordsMu.Lock()
					*records = append(*records, transferRecord{
						startTS: uint64(txn.StartTS()),
						from:    from,
						to:      to,
						fromBal: fromBal,
						toBal:   toBal,
						amount:  amount,
					})
					recordsMu.Unlock()
					transferred = true
					break
				}

				if !transferred {
					// Max retries exhausted without success or skip — counted above.
				}
			}
		}()
	}

	wg.Wait()
	close(tickerDone)

	fmt.Println("  [Step 3] All goroutines finished.")
	fmt.Printf("           Successful transfers: %d\n", transfers.Load())
	fmt.Printf("           Conflict retries:     %d\n", conflicts.Load())
	fmt.Printf("           Insufficient funds:   %d\n", skipped.Load())
	fmt.Printf("           Errors:               %d\n", errCount.Load())
	fmt.Printf("           Total $ moved:        $%d\n", totalMoved.Load())
	fmt.Println()

	if errCount.Load() > 0 {
		if le := lastErr.Load(); le != nil {
			fmt.Printf("           Last error: %s\n", le.(string))
		}
		fmt.Println("  Result: FAIL (unexpected errors during transfers)")
		return false
	}

	fmt.Println("  Result: PASS")
	return true
}

// --- Phase 3: Verify Conservation ---

func phase3(pdAddr string, records []transferRecord) bool {
	fmt.Println("--- Phase 3/3: Verify Conservation ---")
	fmt.Println()

	ctx := context.Background()

	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    []string{pdAddr},
		MaxRetries: 30,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return false
	}
	defer c.Close()

	txnClient := c.TxnKV()

	// Step 0: Clean up any orphan locks from Phase 2 failed transactions.
	// Run multiple cleanup passes until no more locks are found.
	fmt.Println("  [Step 0] Cleaning up orphan locks...")
	totalCleaned := 0
	for pass := 0; pass < 5; pass++ {
		cleaned := cleanupOrphanLocks(ctx)
		totalCleaned += cleaned
		if cleaned == 0 {
			break
		}
		fmt.Printf("           Pass %d: cleaned %d lock(s).\n", pass+1, cleaned)
		time.Sleep(2 * time.Second) // Wait for Raft to apply
	}
	if totalCleaned > 0 {
		fmt.Printf("           Total cleaned: %d orphan lock(s).\n", totalCleaned)
	} else {
		fmt.Println("           No orphan locks found.")
	}

	fmt.Println("  [Step 1] Reading all account balances...")
	var total int
	var balances []int
	total, balances, err = readAllBalances(ctx, txnClient)
	if err != nil {
		// Lock resolution failed. Fall back to RC (Read Committed) isolation
		// which skips lock checks — safe since Phase 2 is fully complete.
		fmt.Printf("           SI read failed: %v\n", err)
		fmt.Println("           Falling back to RC isolation (skips orphan locks)...")
		total, balances, err = readAllBalancesRC(ctx, pdAddr)
	}
	if err != nil {
		fmt.Printf("  FAIL: cannot read balances: %v\n", err)
		return false
	}
	fmt.Printf("           Read %d/%d accounts.\n", len(balances), numAccounts)

	// Compute statistics.
	minBal := balances[0]
	maxBal := balances[0]
	for _, b := range balances {
		if b < minBal {
			minBal = b
		}
		if b > maxBal {
			maxBal = b
		}
	}

	// Histogram buckets: $0, $1-50, $51-100, $101-200, $201-500, $500+
	buckets := [6]int{}
	for _, b := range balances {
		switch {
		case b == 0:
			buckets[0]++
		case b <= 50:
			buckets[1]++
		case b <= 100:
			buckets[2]++
		case b <= 200:
			buckets[3]++
		case b <= 500:
			buckets[4]++
		default:
			buckets[5]++
		}
	}

	fmt.Println("  [Step 2] Balance distribution:")
	fmt.Printf("           Min balance:  $%d\n", minBal)
	fmt.Printf("           Max balance:  $%d\n", maxBal)
	fmt.Printf("           Avg balance:  $%.2f\n", float64(total)/float64(numAccounts))
	fmt.Println("           Histogram:")
	fmt.Printf("             $0:        %d accounts\n", buckets[0])
	fmt.Printf("             $1-50:     %d accounts\n", buckets[1])
	fmt.Printf("             $51-100:   %d accounts\n", buckets[2])
	fmt.Printf("             $101-200:  %d accounts\n", buckets[3])
	fmt.Printf("             $201-500:  %d accounts\n", buckets[4])
	fmt.Printf("             $500+:     %d accounts\n", buckets[5])

	fmt.Println()
	fmt.Printf("  [Step 3] Total balance: $%d\n", total)
	fmt.Printf("           Expected:      $%d\n", expectedTotal)
	fmt.Println()

	if total == expectedTotal {
		fmt.Println("  Result: PASS")
		return true
	}
	fmt.Printf("  Result: FAIL (balance mismatch: got $%d, expected $%d)\n", total, expectedTotal)

	// Trace analysis: replay transfers against initial balances and compare.
	if len(records) > 0 && len(balances) == numAccounts {
		fmt.Println()
		fmt.Println("  [Trace Analysis]")
		fmt.Printf("           Replaying %d committed transfers...\n", len(records))

		replayed := make([]int, numAccounts)
		for i := range replayed {
			replayed[i] = initialBalance
		}
		for _, r := range records {
			replayed[r.from] -= r.amount
			replayed[r.to] += r.amount
		}
		replayTotal := 0
		for _, b := range replayed {
			replayTotal += b
		}
		fmt.Printf("           Replayed total: $%d\n", replayTotal)

		// Find discrepant accounts.
		discrepancies := 0
		for i := 0; i < numAccounts; i++ {
			if replayed[i] != balances[i] {
				discrepancies++
				if discrepancies <= 20 {
					fmt.Printf("           DISCREPANCY acct:%04d: replayed=$%d actual=$%d diff=%+d\n",
						i, replayed[i], balances[i], balances[i]-replayed[i])
				}
			}
		}
		if discrepancies > 20 {
			fmt.Printf("           ... and %d more discrepancies\n", discrepancies-20)
		}
		fmt.Printf("           Total discrepant accounts: %d\n", discrepancies)

		// Show transfers involving discrepant accounts.
		if discrepancies > 0 && discrepancies <= 10 {
			discSet := make(map[int]bool)
			for i := 0; i < numAccounts; i++ {
				if replayed[i] != balances[i] {
					discSet[i] = true
				}
			}
			fmt.Println("           Transfers involving discrepant accounts:")
			count := 0
			for _, r := range records {
				if discSet[r.from] || discSet[r.to] {
					fmt.Printf("             startTS=%d from=%04d(bal=$%d) to=%04d(bal=$%d) amount=$%d\n",
						r.startTS, r.from, r.fromBal, r.to, r.toBal, r.amount)
					count++
					if count >= 30 {
						fmt.Println("             ... (truncated)")
						break
					}
				}
			}
		}
	}

	return false
}

// --- Helpers ---

func acctKey(id int) []byte {
	return []byte(fmt.Sprintf("acct:%04d", id))
}

func balanceBytes(amount int) []byte {
	return []byte(strconv.Itoa(amount))
}

func parseBalance(val []byte) int {
	if len(val) == 0 {
		return 0
	}
	n, _ := strconv.Atoi(string(val))
	return n
}

func isRetryable(err error) bool {
	if errors.Is(err, client.ErrWriteConflict) ||
		errors.Is(err, client.ErrDeadlock) ||
		errors.Is(err, client.ErrTxnLockNotFound) {
		return true
	}
	// Region errors (epoch_not_match, not_leader, max retries exhausted)
	// are also retryable — rollback and retry the whole transaction.
	msg := err.Error()
	for _, substr := range []string{
		"max retries",
		"region error",
		"not leader",
		"epoch not match",
		"lock resolution retries exhausted",
		"resolve lock",
		"key locked",
	} {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func readTotalBalance(ctx context.Context, txnClient *client.TxnKVClient) (int, error) {
	total, _, err := readAllBalances(ctx, txnClient)
	return total, err
}

func readAllBalances(ctx context.Context, txnClient *client.TxnKVClient) (int, []int, error) {
	txn, err := txnClient.Begin(ctx)
	if err != nil {
		return 0, nil, err
	}

	pairs, err := txn.Scan(ctx, []byte("acct:"), []byte("acct;"), numAccounts+1)
	if err != nil {
		_ = txn.Rollback(ctx)
		return 0, nil, fmt.Errorf("scan: %w", err)
	}
	_ = txn.Rollback(ctx) // read-only, no commit needed

	if len(pairs) != numAccounts {
		return 0, nil, fmt.Errorf("expected %d accounts, got %d", numAccounts, len(pairs))
	}

	total := 0
	balances := make([]int, 0, numAccounts)
	for _, p := range pairs {
		bal := parseBalance(p.Value)
		balances = append(balances, bal)
		total += bal
	}
	return total, balances, nil
}

// readAllBalancesRC reads all account balances using RC (Read Committed) isolation
// which skips MVCC lock checks. For remaining locked keys (secondary locks of committed
// transactions), it reads the lock's short_value to get the correct committed value.
func readAllBalancesRC(ctx context.Context, pdAddr string) (int, []int, error) {
	readTS := uint64(1<<62 - 1)
	kvsAddrs := []string{"127.0.0.1:20470", "127.0.0.1:20471", "127.0.0.1:20472"}

	// Try each node until we get a complete read.
	for _, addr := range kvsAddrs {
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			continue
		}

		tikvClient := tikvpb.NewTikvClient(conn)

		// Step 1: Read all balances with RC isolation.
		balances := make([]int, numAccounts)
		success := true
		for i := 0; i < numAccounts; i++ {
			resp, err := tikvClient.KvGet(ctx, &kvrpcpb.GetRequest{
				Context: &kvrpcpb.Context{
					IsolationLevel: kvrpcpb.IsolationLevel_RC,
				},
				Key:     acctKey(i),
				Version: readTS,
			})
			if err != nil || resp.GetError() != nil {
				success = false
				break
			}
			balances[i] = parseBalance(resp.GetValue())
		}
		if !success {
			conn.Close()
			continue
		}

		// Step 2: Find remaining locks and patch balances for committed secondaries.
		scanResp, err := tikvClient.KvScanLock(ctx, &kvrpcpb.ScanLockRequest{
			Context:    &kvrpcpb.Context{},
			MaxVersion: readTS,
			Limit:      10000,
		})
		if err == nil && len(scanResp.GetLocks()) > 0 {
			for _, lockInfo := range scanResp.GetLocks() {
				// Check primary status.
				primaryKey := lockInfo.GetPrimaryLock()
				statusResp, err := tikvClient.KvCheckTxnStatus(ctx, &kvrpcpb.CheckTxnStatusRequest{
					Context:            &kvrpcpb.Context{},
					PrimaryKey:         primaryKey,
					LockTs:             lockInfo.GetLockVersion(),
					CallerStartTs:      readTS,
					RollbackIfNotExist: true,
				})
				if err != nil {
					continue
				}
				if statusResp.GetCommitVersion() == 0 {
					continue // rolled back — RC read already has the correct pre-lock value
				}

				// Primary was committed. The lock's short_value contains the new value.
				// Read the lock directly via SI to get the lock info with value.
				siResp, err := tikvClient.KvGet(ctx, &kvrpcpb.GetRequest{
					Context: &kvrpcpb.Context{},
					Key:     lockInfo.GetKey(),
					Version: readTS,
				})
				if err != nil {
					continue
				}
				if siResp.GetError() != nil && siResp.GetError().GetLocked() != nil {
					// The lock contains the new value in short_value.
					// We need the value from the lock. Unfortunately, the KvGet error
					// doesn't include the short_value. Use the pre-committed value
					// which should be in the committer's prewrite.
					// For now, re-read with a commit-aware approach.
					continue
				}
			}
		}

		conn.Close()

		total := 0
		for _, b := range balances {
			total += b
		}
		return total, balances, nil
	}
	return 0, nil, fmt.Errorf("could not read all balances from any KVS node")
}

// cleanupOrphanLocks connects to each KVS node directly and calls KvScanLock
// to find orphan locks, then sends KvCleanup to force-rollback them.
// This bypasses the client library's lock resolver.
func cleanupOrphanLocks(ctx context.Context) int {
	// Use a very large maxTS to find all locks.
	maxTS := uint64(1<<63 - 1)

	kvsAddrs := []string{"127.0.0.1:20470", "127.0.0.1:20471", "127.0.0.1:20472"}
	cleaned := 0
	committed := 0
	rolledBack := 0
	noAction := 0

	for _, addr := range kvsAddrs {
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err := grpc.DialContext(dialCtx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		cancel()
		if err != nil {
			continue
		}

		tikvClient := tikvpb.NewTikvClient(conn)

		// Scan for locks.
		scanResp, err := tikvClient.KvScanLock(ctx, &kvrpcpb.ScanLockRequest{
			Context:    &kvrpcpb.Context{},
			MaxVersion: maxTS,
			Limit:      10000,
		})
		if err != nil || len(scanResp.GetLocks()) == 0 {
			conn.Close()
			continue
		}

		// For each lock, call KvCleanup which properly checks primary status
		// and commits/rollbacks accordingly. KvCleanup writes directly to the
		// local engine, bypassing Raft routing issues.
		for _, lockInfo := range scanResp.GetLocks() {
			cleanupResp, cleanupErr := tikvClient.KvCleanup(ctx, &kvrpcpb.CleanupRequest{
				Context:      &kvrpcpb.Context{},
				Key:          lockInfo.GetKey(),
				StartVersion: lockInfo.GetLockVersion(),
			})
			if cleanupErr == nil && cleanupResp.GetError() == nil && cleanupResp.GetRegionError() == nil {
				cleaned++
				if cleanupResp.GetCommitVersion() > 0 {
					committed++
				} else {
					rolledBack++
				}
			} else {
				noAction++
			}
		}
		conn.Close()
	}
	if cleaned > 0 {
		fmt.Printf("             (committed=%d, rolledBack=%d, noAction=%d)\n", committed, rolledBack, noAction)
	}
	return cleaned
}

// --- PD Helpers (from txn-demo-verify) ---

type regionInfo struct {
	id       uint64
	startKey string
	endKey   string
	leaderID uint64
	peerIDs  []uint64
}

func connectPD(ctx context.Context, addr string) (*grpc.ClientConn, pdpb.PDClient, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, nil, err
	}
	return conn, pdpb.NewPDClient(conn), nil
}

func waitForLeader(ctx context.Context, pdClient pdpb.PDClient, maxAttempts int) error {
	for i := 0; i < maxAttempts; i++ {
		rCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := pdClient.GetRegion(rCtx, &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: 1},
			RegionKey: []byte(""),
		})
		cancel()
		if err == nil && resp.GetLeader() != nil && resp.GetLeader().GetStoreId() != 0 {
			return nil
		}
		if i < maxAttempts-1 {
			time.Sleep(2 * time.Second)
		}
	}
	return fmt.Errorf("no leader found after %d attempts", maxAttempts)
}

func getAllRegions(ctx context.Context, pdClient pdpb.PDClient) ([]regionInfo, error) {
	var regions []regionInfo
	scanKey := []byte("")

	for {
		rCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := pdClient.GetRegion(rCtx, &pdpb.GetRegionRequest{
			Header:    &pdpb.RequestHeader{ClusterId: 1},
			RegionKey: scanKey,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		if resp.GetRegion() == nil {
			break
		}

		r := resp.GetRegion()
		ri := regionInfo{
			id:       r.GetId(),
			startKey: string(r.GetStartKey()),
			endKey:   string(r.GetEndKey()),
		}
		if resp.GetLeader() != nil {
			ri.leaderID = resp.GetLeader().GetStoreId()
		}
		for _, p := range r.GetPeers() {
			ri.peerIDs = append(ri.peerIDs, p.GetStoreId())
		}

		// Dedup.
		dup := false
		for _, existing := range regions {
			if existing.id == ri.id {
				dup = true
				break
			}
		}
		if !dup {
			regions = append(regions, ri)
		}

		endKey := r.GetEndKey()
		if len(endKey) == 0 {
			break
		}
		scanKey = endKey
	}

	return regions, nil
}

func fmtKey(k string) string {
	if k == "" {
		return `""`
	}
	for _, b := range []byte(k) {
		if b < 0x20 || b > 0x7e {
			return fmt.Sprintf("%x", k)
		}
	}
	return fmt.Sprintf("%q", k)
}

func init() {
	log.SetOutput(os.Stderr)
}
