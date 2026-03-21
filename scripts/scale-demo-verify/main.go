// scale-demo-verify demonstrates dynamic node addition in a gookv cluster.
// It runs two scenarios: verifying initial cluster state, then adding nodes
// and observing region split with data distribution.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookv/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pdAddr     = flag.String("pd", "127.0.0.1:2399", "PD address")
	dataDir    = flag.String("data-dir", "/tmp/gookv-scale-demo", "Data directory")
	configPath = flag.String("config", "scripts/txn-demo/config.toml", "Config file for new nodes")
)

func main() {
	flag.Parse()

	fmt.Println("================================================================")
	fmt.Println("       gookv Dynamic Node Addition Demo")
	fmt.Println("================================================================")
	fmt.Println()

	passed := 0
	total := 2

	if scenario1(*pdAddr) {
		passed++
	}
	fmt.Println()

	if scenario2(*pdAddr, *dataDir, *configPath) {
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

// scenario1: Initial Cluster State
func scenario1(pdAddr string) bool {
	fmt.Println("--- Scenario 1/2: Initial Cluster State ---")
	fmt.Println()

	ctx := context.Background()

	// Step 1: Connect to PD and wait for leader.
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

	// Step 2: Get all regions and verify exactly 1.
	regions, err := getAllRegions(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: cannot get regions: %v\n", err)
		return false
	}
	fmt.Printf("  [Step 2] Region count: %d\n", len(regions))

	// Step 3: Get stores from PD.
	stores, err := getAllStores(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: cannot get stores: %v\n", err)
		return false
	}
	fmt.Printf("  [Step 3] Store count: %d\n", len(stores))
	for _, s := range stores {
		fmt.Printf("           Store %d: addr=%s state=%s\n",
			s.GetId(), s.GetAddress(), s.GetState().String())
	}

	// Step 4: Print region details.
	fmt.Println("  [Step 4] Region layout:")
	for _, r := range regions {
		fmt.Printf("           Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}
	fmt.Println()

	if len(regions) == 1 {
		fmt.Println("  Result: PASS")
		return true
	}
	fmt.Printf("  Result: FAIL (expected 1 region, got %d)\n", len(regions))
	return false
}

// scenario2: Add Node + Region Split + Data Verification
func scenario2(pdAddr, dataDir, configPath string) bool {
	fmt.Println("--- Scenario 2/2: Add Node + Region Split ---")
	fmt.Println()

	ctx := context.Background()

	pdConn, pdClient, err := connectPD(ctx, pdAddr)
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to PD: %v\n", err)
		return false
	}
	defer pdConn.Close()

	// Step 1: Add 1 new node in join mode.
	fmt.Println("  [Step 1] Adding 1 new node in join mode...")

	i := 4
	grpcPort := 20269 + i   // 20273
	statusPort := 20289 + i // 20293
	nodeDataDir := filepath.Join(dataDir, fmt.Sprintf("node%d", i))
	if err := os.MkdirAll(nodeDataDir, 0755); err != nil {
		fmt.Printf("  FAIL: cannot create data dir for node %d: %v\n", i, err)
		return false
	}

	cmd := exec.Command("./gookv-server",
		"--pd-endpoints", pdAddr,
		"--addr", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--status-addr", fmt.Sprintf("127.0.0.1:%d", statusPort),
		"--data-dir", nodeDataDir,
		"--config", configPath,
	)

	logFile, err := os.Create(filepath.Join(dataDir, fmt.Sprintf("node%d.log", i)))
	if err != nil {
		fmt.Printf("  FAIL: cannot create log file for node %d: %v\n", i, err)
		return false
	}
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()
		fmt.Printf("  FAIL: cannot start node %d: %v\n", i, err)
		return false
	}

	// Write PID file.
	pidFile := filepath.Join(dataDir, fmt.Sprintf("node%d.pid", i))
	_ = os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644)

	fmt.Printf("           Node %d started: addr=127.0.0.1:%d pid=%d\n",
		i, grpcPort, cmd.Process.Pid)
	fmt.Println("           (join mode: no --store-id, no --initial-cluster)")

	fmt.Println("           Waiting 5s for node registration...")
	time.Sleep(5 * time.Second)

	// Poll stores from PD until count >= 4 (timeout 30s).
	fmt.Println("           Polling PD for store registration...")
	deadline := time.Now().Add(30 * time.Second)
	var stores []*metapb.Store
	for time.Now().Before(deadline) {
		stores, err = getAllStores(ctx, pdClient)
		if err == nil && len(stores) >= 4 {
			break
		}
		time.Sleep(2 * time.Second)
		if stores != nil {
			fmt.Printf("           Store count: %d (waiting for 4...)\n", len(stores))
		}
	}

	if len(stores) < 4 {
		fmt.Printf("  FAIL: expected >= 4 stores, got %d after 30s\n", len(stores))
		return false
	}

	fmt.Printf("           Store count: %d\n", len(stores))
	originalStores := map[uint64]bool{1: true, 2: true, 3: true}
	for _, s := range stores {
		marker := ""
		if !originalStores[s.GetId()] {
			marker = "  <- NEW"
		}
		fmt.Printf("           Store %d: addr=%s state=%s%s\n",
			s.GetId(), s.GetAddress(), s.GetState().String(), marker)
	}
	fmt.Println()

	// Step 2: Write data.
	fmt.Println("  [Step 2] Writing data to trigger split...")
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

	keyCount := 20
	value := []byte(strings.Repeat("x", 200))
	written := 0

	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("data:%s", genKey(i))
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
	fmt.Printf("           Done: %d/%d keys written (%d bytes each).\n", written, keyCount, len(value))
	if written < 5 {
		fmt.Printf("  FAIL: too few keys written (%d), split unlikely\n", written)
		return false
	}
	fmt.Println()

	// Step 3: Split detection + PUT values + show leaders.
	fmt.Println("  [Step 3] Waiting for region split (timeout 60s)...")
	deadline = time.Now().Add(60 * time.Second)
	var regionsAfter []regionInfo
	for time.Now().Before(deadline) {
		regionsAfter, err = getAllRegions(ctx, pdClient)
		if err == nil && len(regionsAfter) >= 2 {
			break
		}
		time.Sleep(2 * time.Second)
		fmt.Printf("           Region count: %d (waiting for >= 2...)\n", len(regionsAfter))
	}

	if len(regionsAfter) < 2 {
		fmt.Printf("  FAIL: region count did not reach 2 after 60s (still %d)\n", len(regionsAfter))
		return false
	}

	fmt.Printf("           Region count: %d  Split detected!\n", len(regionsAfter))
	fmt.Println()

	// Wait for all regions to have a leader.
	fmt.Println("           Waiting for all regions to have a leader (timeout 30s)...")
	leaderDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(leaderDeadline) {
		regionsAfter, err = getAllRegions(ctx, pdClient)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		allHaveLeader := true
		for _, r := range regionsAfter {
			if r.leaderID == 0 {
				allHaveLeader = false
				break
			}
		}
		if allHaveLeader {
			break
		}
		time.Sleep(2 * time.Second)
		fmt.Println("           (some regions still without leader...)")
	}

	fmt.Println("           Region layout with leaders:")
	for _, r := range regionsAfter {
		fmt.Printf("             Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}
	fmt.Println()

	// Step 3 (continued): PUT a test value into each of the first 2 regions and GET it back.
	fmt.Println("  [Step 3b] Writing and reading a value in each region...")

	testRegions := regionsAfter
	if len(testRegions) > 2 {
		testRegions = testRegions[:2]
	}

	allOK := true
	for idx, r := range testRegions {
		// Generate a key guaranteed to fall within this region's range.
		var key string
		if r.startKey == "" {
			key = "\x01verify" // sorts before "data:*"
		} else {
			key = r.startKey + "_verify"
		}
		val := fmt.Sprintf("hello-region-%d", idx+1)

		// PUT
		var putErr error
		for attempt := 0; attempt < 5; attempt++ {
			putErr = rawClient.Put(ctx, []byte(key), []byte(val))
			if putErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if putErr != nil {
			fmt.Printf("           PUT %q: FAIL %v\n", key, putErr)
			allOK = false
			continue
		}

		// GET
		got, notFound, getErr := rawClient.Get(ctx, []byte(key))
		if getErr != nil || notFound || string(got) != val {
			fmt.Printf("           PUT/GET %q: FAIL (err=%v notFound=%v got=%q)\n", key, getErr, notFound, string(got))
			allOK = false
			continue
		}
		fmt.Printf("           PUT/GET %q = %q  OK  (Region %d, leader Store %d)\n",
			key, val, r.id, r.leaderID)
	}
	fmt.Println()

	if !allOK {
		fmt.Println("  FAIL: data verification failed")
		return false
	}

	// Step 4: Show final cluster state.
	fmt.Println("  [Step 4] Final cluster state:")
	var finalRegions []regionInfo
	finalRegions, err = getAllRegions(ctx, pdClient)
	if err != nil {
		fmt.Printf("           (could not query final regions: %v)\n", err)
	} else {
		fmt.Println("           Region layout:")
		for _, r := range finalRegions {
			fmt.Printf("             Region %d: [%s .. %s)  peers=%v  leader=Store %d\n",
				r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
		}
	}
	fmt.Println()

	fmt.Println("  Result: PASS")
	return true
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

func getAllStores(ctx context.Context, pdClient pdpb.PDClient) ([]*metapb.Store, error) {
	rCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := pdClient.GetAllStores(rCtx, &pdpb.GetAllStoresRequest{
		Header: &pdpb.RequestHeader{ClusterId: 1},
	})
	if err != nil {
		return nil, err
	}
	if resp.GetHeader().GetError() != nil {
		return nil, fmt.Errorf("GetAllStores error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp.GetStores(), nil
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

func storeIDToPIDFile(storeID uint64, stores []*metapb.Store, dataDir string) string {
	if storeID >= 1 && storeID <= 3 {
		return filepath.Join(dataDir, fmt.Sprintf("node%d.pid", storeID))
	}
	for _, s := range stores {
		if s.GetId() == storeID {
			addr := s.GetAddress()
			// addr is like "127.0.0.1:20273"
			parts := strings.Split(addr, ":")
			if len(parts) == 2 {
				port, err := strconv.Atoi(parts[1])
				if err == nil {
					nodeNum := port - 20269
					return filepath.Join(dataDir, fmt.Sprintf("node%d.pid", nodeNum))
				}
			}
		}
	}
	return ""
}

func killProcess(pidFile string) (int, error) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, err
	}
	return pid, syscall.Kill(pid, syscall.SIGKILL)
}

func init() {
	// Suppress log output from the client library.
	log.SetOutput(os.Stderr)
}
