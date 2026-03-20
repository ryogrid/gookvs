// txn-demo-verify demonstrates cross-region transactions in a gookv cluster.
// It runs three scenarios: single-region txn, region split, and cross-region 2PC.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookv/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var pdAddr = flag.String("pd", "127.0.0.1:2389", "PD address")

func main() {
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("       gookv Cross-Region Transaction Demo")
	fmt.Println("================================================================")
	fmt.Println()

	passed := 0
	total := 3

	if scenario1(*pdAddr) {
		passed++
	}
	fmt.Println()

	if scenario2(*pdAddr) {
		passed++
	}
	fmt.Println()

	// Wait for splits to settle before cross-region txn.
	fmt.Println("  Waiting 5s for splits to settle...")
	time.Sleep(5 * time.Second)
	fmt.Println()

	if scenario3(*pdAddr) {
		passed++
	}

	fmt.Println()
	fmt.Println("================================================================")
	if passed == total {
		fmt.Printf("  All %d scenarios passed.\n", total)
	} else {
		fmt.Printf("  %d/%d scenarios passed.\n", passed, total)
	}
	fmt.Println("================================================================")

	if passed < total {
		os.Exit(1)
	}
}

// regionInfo holds info about a region from PD.
type regionInfo struct {
	id       uint64
	startKey string
	endKey   string
	leaderID uint64
	peerIDs  []uint64
}

// scenario1: Single-Region Transaction (Baseline)
func scenario1(pdAddr string) bool {
	fmt.Println("--- Scenario 1/3: Single-Region Transaction (Baseline) ---")
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

	// Verify initial region count.
	regions, err := getAllRegions(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: cannot get regions: %v\n", err)
		return false
	}
	fmt.Printf("  [Step 2] Initial region count: %d\n", len(regions))
	for _, r := range regions {
		fmt.Printf("           Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}

	// Create client and perform transaction.
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs: []string{pdAddr},
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return false
	}
	defer c.Close()

	txnClient := c.TxnKV()

	fmt.Println("  [Step 3] Begin transaction...")
	txn, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin: %v\n", err)
		return false
	}
	fmt.Printf("           startTS=%d\n", txn.StartTS())

	fmt.Println("  [Step 4] Set account:alice=1000, account:bob=1000")
	if err := txn.Set(ctx, []byte("account:alice"), []byte("1000")); err != nil {
		fmt.Printf("  FAIL: Set alice: %v\n", err)
		return false
	}
	if err := txn.Set(ctx, []byte("account:bob"), []byte("1000")); err != nil {
		fmt.Printf("  FAIL: Set bob: %v\n", err)
		return false
	}

	fmt.Println("  [Step 5] Commit transaction...")
	if err := txn.Commit(ctx); err != nil {
		fmt.Printf("  FAIL: Commit: %v\n", err)
		return false
	}
	fmt.Println("           Committed OK.")

	// Verify: read back in new transaction.
	fmt.Println("  [Verify] Read back in new transaction:")
	txn2, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin read txn: %v\n", err)
		return false
	}

	aliceVal, err := txn2.Get(ctx, []byte("account:alice"))
	if err != nil {
		fmt.Printf("  FAIL: Get alice: %v\n", err)
		return false
	}
	bobVal, err := txn2.Get(ctx, []byte("account:bob"))
	if err != nil {
		fmt.Printf("  FAIL: Get bob: %v\n", err)
		return false
	}

	aliceOK := string(aliceVal) == "1000"
	bobOK := string(bobVal) == "1000"
	mark := func(ok bool) string {
		if ok {
			return "OK"
		}
		return "MISMATCH"
	}

	fmt.Printf("    account:alice = %q  %s\n", string(aliceVal), mark(aliceOK))
	fmt.Printf("    account:bob   = %q  %s\n", string(bobVal), mark(bobOK))
	fmt.Println()

	if aliceOK && bobOK {
		fmt.Println("  Result: PASS")
		return true
	}
	fmt.Println("  Result: FAIL")
	return false
}

// scenario2: Trigger Region Split
func scenario2(pdAddr string) bool {
	fmt.Println("--- Scenario 2/3: Region Split ---")
	fmt.Println()

	ctx := context.Background()

	pdConn, pdClient, err := connectPD(ctx, pdAddr)
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to PD: %v\n", err)
		return false
	}
	defer pdConn.Close()

	// Get initial region count.
	regionsBefore, err := getAllRegions(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: cannot get regions: %v\n", err)
		return false
	}
	fmt.Printf("  [Step 1] Region count before: %d\n", len(regionsBefore))

	// Write enough data to trigger split (config: split-size=1KB, max-size=2KB).
	// Use RawKV for simplicity — we need ~50 keys of ~100 bytes each.
	// Use higher MaxRetries because split may cause transient region errors.
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    []string{pdAddr},
		MaxRetries: 10,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return false
	}
	defer c.Close()

	rawClient := c.RawKV()

	fmt.Println("  [Step 2] Writing data to trigger split...")
	// Write enough to exceed 1KB split threshold but not so much as to
	// cause excessive cascading splits. 15 keys * 150 bytes = ~2.25KB.
	keyCount := 15
	valueSize := 150
	value := []byte(strings.Repeat("x", valueSize))
	written := 0

	for i := 0; i < keyCount; i++ {
		// Generate well-distributed keys: "fill:aaa" through "fill:aao"
		key := fmt.Sprintf("fill:%s", genKey(i))
		// Retry individual puts with backoff — split may cause transient failures
		// while the new region's leader is being elected.
		var putErr error
		for attempt := 0; attempt < 5; attempt++ {
			putErr = rawClient.Put(ctx, []byte(key), value)
			if putErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if putErr != nil {
			fmt.Printf("           Skipping %s after retries: %v\n", key, putErr)
			continue
		}
		written++
	}
	fmt.Printf("           Done: %d/%d keys written (%d bytes each).\n", written, keyCount, valueSize)
	if written < 5 {
		fmt.Printf("  FAIL: too few keys written (%d), split unlikely\n", written)
		return false
	}

	// Poll PD until region count increases.
	fmt.Println("  [Step 3] Waiting for split (timeout 60s)...")
	deadline := time.Now().Add(60 * time.Second)
	var regionsAfter []regionInfo
	for time.Now().Before(deadline) {
		regionsAfter, err = getAllRegions(ctx, pdClient)
		if err == nil && len(regionsAfter) > len(regionsBefore) {
			break
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("           Region count: %d (waiting...)\n", len(regionsAfter))
	}

	if len(regionsAfter) <= len(regionsBefore) {
		fmt.Printf("  FAIL: region count did not increase after 60s (still %d)\n", len(regionsAfter))
		return false
	}

	fmt.Printf("           Region count: %d -> %d  Split detected!\n", len(regionsBefore), len(regionsAfter))
	fmt.Println()

	fmt.Println("  [Verify] Region layout after split:")
	for _, r := range regionsAfter {
		fmt.Printf("    Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}
	fmt.Println()

	fmt.Println("  Result: PASS")
	return true
}

// scenario3: Cross-Region Transaction (2PC)
func scenario3(pdAddr string) bool {
	fmt.Println("--- Scenario 3/3: Cross-Region Transaction (2PC) ---")
	fmt.Println()

	ctx := context.Background()

	pdConn, pdClient, err := connectPD(ctx, pdAddr)
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to PD: %v\n", err)
		return false
	}
	defer pdConn.Close()

	// Wait for region count to stabilize (no more cascading splits).
	fmt.Println("  [Step 0] Waiting for region count to stabilize...")
	prevCount := 0
	stableRounds := 0
	var regions []regionInfo
	for stableRounds < 3 {
		var err2 error
		regions, err2 = getAllRegions(ctx, pdClient)
		if err2 != nil {
			fmt.Printf("  FAIL: cannot get regions: %v\n", err2)
			return false
		}
		if len(regions) == prevCount {
			stableRounds++
		} else {
			stableRounds = 0
		}
		prevCount = len(regions)
		time.Sleep(2 * time.Second)
	}
	fmt.Printf("           Region count stabilized at %d\n", len(regions))

	if len(regions) < 2 {
		fmt.Printf("  FAIL: need at least 2 regions for cross-region txn, got %d\n", len(regions))
		return false
	}

	// Pick keys guaranteed to be in different regions.
	// Use "account:alice" (in region 1, before split key) and a key in
	// the last region (after all split keys) for maximum separation.
	key1 := []byte("account:alice")
	lastRegion := regions[len(regions)-1]
	key2 := []byte(lastRegion.startKey + "balance")

	r1 := findRegionForKey(ctx, pdClient, key1)
	r2 := findRegionForKey(ctx, pdClient, key2)

	fmt.Printf("  [Step 1] Selected keys across regions:\n")
	if r1 != nil {
		fmt.Printf("           %q -> Region %d (peers=%v, leader=Store %d)\n",
			string(key1), r1.id, r1.peerIDs, r1.leaderID)
	} else {
		fmt.Printf("           %q -> Region unknown\n", string(key1))
	}
	if r2 != nil {
		fmt.Printf("           %q -> Region %d (peers=%v, leader=Store %d)\n",
			string(key2), r2.id, r2.peerIDs, r2.leaderID)
	} else {
		fmt.Printf("           %q -> Region unknown\n", string(key2))
	}

	crossRegion := r1 != nil && r2 != nil && r1.id != r2.id
	if crossRegion {
		fmt.Println("           Confirmed: keys are in DIFFERENT regions.")
	} else {
		fmt.Println("  WARNING: keys are in the same region; 2PC still runs but doesn't demonstrate cross-region.")
	}
	fmt.Println()

	// Create client with higher retries for post-split transient errors.
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

	// Initialize both keys with starting balances via a transaction.
	fmt.Println("  [Step 2] Initialize balances: key1=1000, key2=1000...")
	initTxn, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin init: %v\n", err)
		return false
	}
	if err := initTxn.Set(ctx, key1, []byte("1000")); err != nil {
		fmt.Printf("  FAIL: Set key1: %v\n", err)
		return false
	}
	if err := initTxn.Set(ctx, key2, []byte("1000")); err != nil {
		fmt.Printf("  FAIL: Set key2: %v\n", err)
		return false
	}
	if err := initTxn.Commit(ctx); err != nil {
		fmt.Printf("  FAIL: Commit init: %v\n", err)
		return false
	}
	fmt.Println("           Initialized OK.")

	// Read back to confirm.
	readTxn, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin read: %v\n", err)
		return false
	}
	val1, err := readTxn.Get(ctx, key1)
	if err != nil {
		fmt.Printf("  FAIL: Get key1: %v\n", err)
		return false
	}
	val2, err := readTxn.Get(ctx, key2)
	if err != nil {
		fmt.Printf("  FAIL: Get key2: %v\n", err)
		return false
	}
	bal1, _ := strconv.Atoi(string(val1))
	bal2, _ := strconv.Atoi(string(val2))
	fmt.Printf("           %s = %d\n", string(key1), bal1)
	fmt.Printf("           %s = %d\n", string(key2), bal2)

	// Transfer 100 from key1 to key2.
	fmt.Println("  [Step 3] Begin cross-region transaction: transfer 100...")
	txn, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin: %v\n", err)
		return false
	}
	fmt.Printf("           startTS=%d\n", txn.StartTS())

	newBal1 := strconv.Itoa(bal1 - 100)
	newBal2 := strconv.Itoa(bal2 + 100)

	fmt.Printf("  [Step 4] Set %s=%s, %s=%s\n", string(key1), newBal1, string(key2), newBal2)
	if err := txn.Set(ctx, key1, []byte(newBal1)); err != nil {
		fmt.Printf("  FAIL: Set key1: %v\n", err)
		return false
	}
	if err := txn.Set(ctx, key2, []byte(newBal2)); err != nil {
		fmt.Printf("  FAIL: Set key2: %v\n", err)
		return false
	}

	fmt.Println("  [Step 5] Commit transaction (2PC across regions)...")
	if err := txn.Commit(ctx); err != nil {
		fmt.Printf("  FAIL: Commit: %v\n", err)
		return false
	}
	fmt.Println("           Committed OK.")

	// Wait for secondary commits to propagate (background goroutine in committer).
	time.Sleep(3 * time.Second)

	// Verify: read back.
	fmt.Println("  [Verify] Read back in new transaction:")
	txn3, err := txnClient.Begin(ctx)
	if err != nil {
		fmt.Printf("  FAIL: Begin verify: %v\n", err)
		return false
	}

	final1, err := txn3.Get(ctx, key1)
	if err != nil {
		fmt.Printf("  FAIL: Get key1: %v\n", err)
		return false
	}
	final2, err := txn3.Get(ctx, key2)
	if err != nil {
		fmt.Printf("  FAIL: Get key2: %v\n", err)
		return false
	}

	ok1 := string(final1) == newBal1
	ok2 := string(final2) == newBal2
	mark := func(ok bool) string {
		if ok {
			return "OK"
		}
		return "MISMATCH"
	}

	r1After := findRegionForKey(ctx, pdClient, key1)
	r2After := findRegionForKey(ctx, pdClient, key2)

	regionStr := func(r *regionInfo) string {
		if r == nil {
			return "unknown"
		}
		return fmt.Sprintf("Region %d, leader=Store %d", r.id, r.leaderID)
	}

	fmt.Printf("    %s = %q  (%s)  %s\n", string(key1), string(final1), regionStr(r1After), mark(ok1))
	fmt.Printf("    %s = %q  (%s)  %s\n", string(key2), string(final2), regionStr(r2After), mark(ok2))

	if r1After != nil && r2After != nil && r1After.id != r2After.id {
		fmt.Println()
		fmt.Println("  Cross-region 2PC confirmed: keys committed atomically across different regions.")
	}
	fmt.Println()

	if ok1 && ok2 {
		fmt.Println("  Result: PASS")
		return true
	}
	fmt.Println("  Result: FAIL")
	return false
}

// --- Helpers ---

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

		// Dedup: check if we already have this region.
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

		// Move to next region.
		endKey := r.GetEndKey()
		if len(endKey) == 0 {
			break // last region
		}
		scanKey = endKey
	}

	return regions, nil
}

func findRegionForKey(ctx context.Context, pdClient pdpb.PDClient, key []byte) *regionInfo {
	rCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := pdClient.GetRegion(rCtx, &pdpb.GetRegionRequest{
		Header:    &pdpb.RequestHeader{ClusterId: 1},
		RegionKey: key,
	})
	if err != nil || resp.GetRegion() == nil {
		return nil
	}
	r := resp.GetRegion()
	ri := &regionInfo{
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
	return ri
}


func genKey(i int) string {
	// Generate 3-letter keys: aaa, aab, aac, ..., aaz, aba, ...
	a := i / (26 * 26)
	b := (i / 26) % 26
	c := i % 26
	return string([]byte{byte('a' + a), byte('a' + b), byte('a' + c)})
}

func fmtKey(k string) string {
	if k == "" {
		return `""`
	}
	// Show printable keys as-is, otherwise hex.
	for _, b := range []byte(k) {
		if b < 0x20 || b > 0x7e {
			return fmt.Sprintf("%x", k)
		}
	}
	return fmt.Sprintf("%q", k)
}

func init() {
	// Suppress log output from the client library.
	log.SetOutput(os.Stderr)
}
