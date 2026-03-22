// pd-failover-demo-verify demonstrates PD leader failover in a gookv cluster.
// It writes data, kills the PD leader, verifies a new leader is elected,
// and confirms the cluster remains fully functional.
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
	pdEndpointsFlag = flag.String("pd", "127.0.0.1:2409,127.0.0.1:2411,127.0.0.1:2413", "PD endpoints (comma-separated)")
	dataDirFlag     = flag.String("data-dir", "/tmp/gookv-pd-failover-demo", "Data directory")
)

// pdMember holds info about a PD cluster member.
type pdMember struct {
	id        uint64
	name      string
	clientURL string
}

func main() {
	flag.Parse()
	pdEndpoints := strings.Split(*pdEndpointsFlag, ",")

	fmt.Println("================================================================")
	fmt.Println("       gookv PD Leader Failover Demo")
	fmt.Println("================================================================")
	fmt.Println()

	passed := 0
	total := 4

	// Phase 1: baseline.
	leaderID, members, ok := phase1(pdEndpoints)
	if ok {
		passed++
	}
	fmt.Println()

	if leaderID == 0 {
		fmt.Println("  Skipping remaining phases (no leader found).")
		printSummary(passed, total)
		os.Exit(1)
	}

	// Phase 2: kill leader.
	if phase2(*dataDirFlag, leaderID, members) {
		passed++
	}
	fmt.Println()

	// Phase 3: failover verification.
	newLeaderID, ok3 := phase3(pdEndpoints, leaderID, members)
	if ok3 {
		passed++
	}
	fmt.Println()

	// Phase 4: final status.
	if phase4(pdEndpoints, leaderID, newLeaderID) {
		passed++
	}

	fmt.Println()
	printSummary(passed, total)
	if passed < total {
		os.Exit(1)
	}
}

func printSummary(passed, total int) {
	fmt.Println("================================================================")
	if passed == total {
		fmt.Printf("  All %d phases passed.\n", total)
	} else {
		fmt.Printf("  %d/%d phases passed.\n", passed, total)
	}
	fmt.Println("================================================================")
}

// phase1: Baseline — Write Data & Confirm PD Leader
func phase1(pdEndpoints []string) (leaderID uint64, members []pdMember, ok bool) {
	fmt.Println("--- Phase 1/4: Baseline — Write Data & Confirm PD Leader ---")
	fmt.Println()

	ctx := context.Background()

	// Step 1: Connect to any PD and discover the cluster.
	fmt.Println("  [Step 1] Connecting to PD cluster...")
	conn, pdClient, connectedAddr, err := connectAnyPD(ctx, pdEndpoints)
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to any PD: %v\n", err)
		return 0, nil, false
	}
	defer conn.Close()
	fmt.Printf("           Connected to %s\n", connectedAddr)

	// Step 2: Discover PD cluster membership.
	fmt.Println("  [Step 2] Discovering PD cluster membership...")
	membersResp, err := getMembers(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: GetMembers: %v\n", err)
		return 0, nil, false
	}

	members = parseMemberList(membersResp)
	leader := membersResp.GetLeader()

	if leader == nil || leader.GetMemberId() == 0 {
		// Poll until leader is elected.
		fmt.Println("           Waiting for PD leader election...")
		for attempt := 0; attempt < 30; attempt++ {
			time.Sleep(1 * time.Second)
			membersResp, err = getMembers(ctx, pdClient)
			if err != nil {
				continue
			}
			leader = membersResp.GetLeader()
			if leader != nil && leader.GetMemberId() != 0 {
				members = parseMemberList(membersResp)
				break
			}
		}
		if leader == nil || leader.GetMemberId() == 0 {
			fmt.Println("  FAIL: PD leader not elected after 30s")
			return 0, nil, false
		}
	}

	leaderID = leader.GetMemberId()
	fmt.Println("           PD cluster members:")
	for _, m := range members {
		role := "FOLLOWER"
		if m.id == leaderID {
			role = "LEADER"
		}
		pidStr := "?"
		pidFile := filepath.Join(*dataDirFlag, fmt.Sprintf("pd%d.pid", m.id))
		if data, err := os.ReadFile(pidFile); err == nil {
			pidStr = strings.TrimSpace(string(data))
		}
		fmt.Printf("             PD %d (%s): %s  pid=%s\n", m.id, m.clientURL, role, pidStr)
	}
	fmt.Println()

	// Step 3: Wait for KVS region leader.
	fmt.Println("  [Step 3] Waiting for KVS region leader...")
	if err := waitForLeader(ctx, pdClient, 30); err != nil {
		fmt.Printf("  FAIL: %v\n", err)
		return 0, nil, false
	}
	fmt.Println("           KVS cluster ready.")

	// Step 4: Write test data.
	fmt.Println("  [Step 4] Writing test data...")
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    pdEndpoints,
		MaxRetries: 10,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return 0, nil, false
	}
	defer c.Close()

	rawClient := c.RawKV()
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("failover:key%d", i)
		val := fmt.Sprintf("value%d", i)
		if err := rawClient.Put(ctx, []byte(key), []byte(val)); err != nil {
			fmt.Printf("  FAIL: Put %s: %v\n", key, err)
			return 0, nil, false
		}
	}
	fmt.Println("           Wrote 5 keys (failover:key1..key5).")

	// Step 5: Read back.
	fmt.Println("  [Step 5] Reading back test data...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("failover:key%d", i)
		expected := fmt.Sprintf("value%d", i)
		got, notFound, err := rawClient.Get(ctx, []byte(key))
		if err != nil || notFound || string(got) != expected {
			fmt.Printf("  FAIL: Get %s: err=%v notFound=%v got=%q expected=%q\n",
				key, err, notFound, string(got), expected)
			return 0, nil, false
		}
	}
	fmt.Println("           All 5 keys verified.")

	// Step 6: Show cluster topology.
	fmt.Println("  [Step 6] Cluster topology:")
	stores, err := getAllStores(ctx, pdClient)
	if err != nil {
		fmt.Printf("           (could not query stores: %v)\n", err)
	} else {
		for _, s := range stores {
			fmt.Printf("           Store %d: addr=%s state=%s\n",
				s.GetId(), s.GetAddress(), s.GetState().String())
		}
	}
	fmt.Println()

	fmt.Println("  Result: PASS")
	return leaderID, members, true
}

// phase2: Kill PD Leader
func phase2(dataDir string, leaderID uint64, members []pdMember) bool {
	fmt.Println("--- Phase 2/4: Kill PD Leader ---")
	fmt.Println()

	// Find leader address.
	var leaderAddr string
	for _, m := range members {
		if m.id == leaderID {
			leaderAddr = m.clientURL
			break
		}
	}

	pidFile := filepath.Join(dataDir, fmt.Sprintf("pd%d.pid", leaderID))
	pidStr := "?"
	if data, err := os.ReadFile(pidFile); err == nil {
		pidStr = strings.TrimSpace(string(data))
	}
	fmt.Printf("  [Step 1] Killing PD leader: node %d (%s) pid=%s\n", leaderID, leaderAddr, pidStr)
	fmt.Printf("           PID file: %s\n", pidFile)
	showProcessStatus("Process status before kill")

	pid, err := killProcess(pidFile)
	if err != nil {
		fmt.Printf("  FAIL: kill PD %d: %v\n", leaderID, err)
		return false
	}
	fmt.Printf("           Sent SIGKILL to pid %d\n", pid)
	time.Sleep(1 * time.Second)
	showProcessStatus("Process status after kill")
	fmt.Println()

	fmt.Println("  Result: PASS")
	return true
}

// phase3: Failover Verification
func phase3(pdEndpoints []string, killedLeaderID uint64, members []pdMember) (newLeaderID uint64, ok bool) {
	fmt.Println("--- Phase 3/4: Failover Verification ---")
	fmt.Println()

	ctx := context.Background()

	// Build list of surviving PD endpoints.
	var killedAddr string
	for _, m := range members {
		if m.id == killedLeaderID {
			killedAddr = m.clientURL
			break
		}
	}
	var survivingEndpoints []string
	for _, ep := range pdEndpoints {
		if ep != killedAddr {
			survivingEndpoints = append(survivingEndpoints, ep)
		}
	}

	// Step 1: Wait for new leader.
	fmt.Println("  [Step 1] Waiting for new PD leader election (timeout 30s)...")
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, ep := range survivingEndpoints {
			conn, pdClient, err := connectPD(ctx, ep)
			if err != nil {
				continue
			}
			resp, err := getMembers(ctx, pdClient)
			conn.Close()
			if err != nil {
				continue
			}
			leader := resp.GetLeader()
			if leader != nil && leader.GetMemberId() != 0 && leader.GetMemberId() != killedLeaderID {
				newLeaderID = leader.GetMemberId()
				break
			}
		}
		if newLeaderID != 0 {
			break
		}
		time.Sleep(1 * time.Second)
		fmt.Println("           (waiting for leader election...)")
	}

	if newLeaderID == 0 {
		fmt.Println("  FAIL: no new leader elected after 30s")
		return 0, false
	}

	var newLeaderAddr string
	for _, m := range members {
		if m.id == newLeaderID {
			newLeaderAddr = m.clientURL
			break
		}
	}
	fmt.Printf("           New leader: PD %d (%s)\n", newLeaderID, newLeaderAddr)
	fmt.Println()

	// Step 2: Read back pre-failover data using ALL endpoints (including dead one).
	// Reorder so surviving endpoints come first — pdclient.NewClient dials
	// Endpoints[0] with WithBlock(), so putting the dead one first would hang.
	// The dead endpoint is still in the list to demonstrate the client handles it.
	reorderedEndpoints := append(survivingEndpoints, killedAddr)
	fmt.Printf("  [Step 2] Reading pre-failover data (endpoints: %v)...\n", reorderedEndpoints)
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs:    reorderedEndpoints,
		MaxRetries: 10,
	})
	if err != nil {
		fmt.Printf("  FAIL: cannot create client: %v\n", err)
		return newLeaderID, false
	}
	defer c.Close()

	rawClient := c.RawKV()
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("failover:key%d", i)
		expected := fmt.Sprintf("value%d", i)
		got, notFound, err := rawClient.Get(ctx, []byte(key))
		if err != nil || notFound || string(got) != expected {
			fmt.Printf("  FAIL: Get %s: err=%v notFound=%v got=%q\n", key, err, notFound, string(got))
			return newLeaderID, false
		}
	}
	fmt.Println("           All 5 pre-failover keys verified.")

	// Step 3: Write new data after failover.
	fmt.Println("  [Step 3] Writing new data after failover...")
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("failover:post%d", i)
		val := fmt.Sprintf("postvalue%d", i)
		var putErr error
		for attempt := 0; attempt < 5; attempt++ {
			putErr = rawClient.Put(ctx, []byte(key), []byte(val))
			if putErr == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if putErr != nil {
			fmt.Printf("  FAIL: Put %s: %v\n", key, putErr)
			return newLeaderID, false
		}
	}

	// Read back new data.
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("failover:post%d", i)
		expected := fmt.Sprintf("postvalue%d", i)
		got, notFound, err := rawClient.Get(ctx, []byte(key))
		if err != nil || notFound || string(got) != expected {
			fmt.Printf("  FAIL: Get %s: err=%v notFound=%v got=%q\n", key, err, notFound, string(got))
			return newLeaderID, false
		}
	}
	fmt.Println("           5 new keys written and verified after failover.")

	// Step 4: Verify cluster metadata.
	fmt.Println("  [Step 4] Verifying cluster metadata...")

	// Connect to a surviving PD for metadata queries.
	conn, pdClient, err := connectPD(ctx, survivingEndpoints[0])
	if err != nil {
		fmt.Printf("  FAIL: cannot connect to surviving PD: %v\n", err)
		return newLeaderID, false
	}
	defer conn.Close()

	stores, err := getAllStores(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: GetAllStores: %v\n", err)
		return newLeaderID, false
	}
	fmt.Printf("           KVS stores registered: %d\n", len(stores))
	for _, s := range stores {
		fmt.Printf("             Store %d: addr=%s state=%s\n",
			s.GetId(), s.GetAddress(), s.GetState().String())
	}
	if len(stores) < 3 {
		fmt.Printf("  FAIL: expected >= 3 stores, got %d\n", len(stores))
		return newLeaderID, false
	}

	regions, err := getAllRegions(ctx, pdClient)
	if err != nil {
		fmt.Printf("  FAIL: GetRegions: %v\n", err)
		return newLeaderID, false
	}
	fmt.Printf("           Regions: %d\n", len(regions))
	for _, r := range regions {
		fmt.Printf("             Region %d: [%s .. %s)  peers=%v leader=Store %d\n",
			r.id, fmtKey(r.startKey), fmtKey(r.endKey), r.peerIDs, r.leaderID)
	}
	fmt.Println()

	fmt.Println("  Result: PASS")
	return newLeaderID, true
}

// phase4: Final PD Cluster State
func phase4(pdEndpoints []string, killedLeaderID, newLeaderID uint64) bool {
	fmt.Println("--- Phase 4/4: Final PD Cluster State ---")
	fmt.Println()

	ctx := context.Background()

	// Connect to any surviving PD.
	var survivingEndpoints []string
	for _, ep := range pdEndpoints {
		survivingEndpoints = append(survivingEndpoints, ep)
	}

	var membersResp *pdpb.GetMembersResponse
	for _, ep := range survivingEndpoints {
		conn, pdClient, err := connectPD(ctx, ep)
		if err != nil {
			continue
		}
		membersResp, err = getMembers(ctx, pdClient)
		conn.Close()
		if err != nil {
			continue
		}
		break
	}

	if membersResp == nil {
		fmt.Println("  FAIL: cannot connect to any surviving PD")
		return false
	}

	fmt.Println("  PD cluster final state:")
	for _, m := range membersResp.GetMembers() {
		id := m.GetMemberId()
		addr := ""
		if len(m.GetClientUrls()) > 0 {
			addr = strings.TrimPrefix(m.GetClientUrls()[0], "http://")
		}
		var status string
		switch {
		case id == killedLeaderID:
			status = "DEAD (killed in Phase 2)"
		case id == newLeaderID:
			status = "LEADER (elected after failover)"
		default:
			status = "FOLLOWER"
		}
		fmt.Printf("    PD %d (%s): %s\n", id, addr, status)
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

func connectAnyPD(ctx context.Context, addrs []string) (*grpc.ClientConn, pdpb.PDClient, string, error) {
	for _, addr := range addrs {
		conn, pdClient, err := connectPD(ctx, addr)
		if err == nil {
			return conn, pdClient, addr, nil
		}
	}
	return nil, nil, "", fmt.Errorf("all %d PD endpoints unreachable", len(addrs))
}

func getMembers(ctx context.Context, pdClient pdpb.PDClient) (*pdpb.GetMembersResponse, error) {
	rCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return pdClient.GetMembers(rCtx, &pdpb.GetMembersRequest{})
}

func parseMemberList(resp *pdpb.GetMembersResponse) []pdMember {
	var members []pdMember
	for _, m := range resp.GetMembers() {
		addr := ""
		if len(m.GetClientUrls()) > 0 {
			addr = strings.TrimPrefix(m.GetClientUrls()[0], "http://")
		}
		members = append(members, pdMember{
			id:        m.GetMemberId(),
			name:      m.GetName(),
			clientURL: addr,
		})
	}
	return members
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
	return fmt.Errorf("no KVS region leader found after %d attempts", maxAttempts)
}

type regionInfo struct {
	id       uint64
	startKey string
	endKey   string
	leaderID uint64
	peerIDs  []uint64
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

func showProcessStatus(label string) {
	cmd := `ps ax | grep "[.]\/gookv-pd "`
	fmt.Println()
	fmt.Printf("           --- %s ---\n", label)
	fmt.Printf("           exec: %s\n", cmd)
	out, _ := exec.Command("bash", "-c", cmd).Output()
	if len(strings.TrimSpace(string(out))) == 0 {
		fmt.Println("           (no matching processes)")
	} else {
		for _, line := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
			fmt.Printf("           %s\n", line)
		}
	}
	fmt.Println("           --- end ---")
	fmt.Println()
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
