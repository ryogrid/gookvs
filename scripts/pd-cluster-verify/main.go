// pd-cluster-verify.go verifies cross-node data replication in a running gookvs cluster with PD.
// Usage: go run scripts/pd-cluster-verify.go
//
// It checks the PD server, writes data to the leader node, and reads it back from
// a different node to confirm Raft-based replication is working.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	pdAddr = "127.0.0.1:2379"

	nodeAddrs = []string{
		"127.0.0.1:20160",
		"127.0.0.1:20161",
		"127.0.0.1:20162",
		"127.0.0.1:20163",
		"127.0.0.1:20164",
	}

	statusAddrs = []string{
		"127.0.0.1:20180",
		"127.0.0.1:20181",
		"127.0.0.1:20182",
		"127.0.0.1:20183",
		"127.0.0.1:20184",
	}
)

func main() {
	fmt.Println("=== gookvs PD Cluster Verification ===")
	fmt.Println()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Step 1: PD health check.
	fmt.Println("[1/4] PD server check...")
	pdConn, err := grpc.DialContext(ctx, pdAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("  PD (%s): FAIL - cannot connect: %v", pdAddr, err)
	}
	pdClient := pdpb.NewPDClient(pdConn)
	membersResp, err := pdClient.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		pdConn.Close()
		log.Fatalf("  PD (%s): FAIL - GetMembers: %v", pdAddr, err)
	}
	fmt.Printf("  PD (%s): OK (leader=%s)\n", pdAddr, membersResp.GetLeader().GetName())
	pdConn.Close()
	fmt.Println()

	// Step 2: Health check all nodes.
	fmt.Println("[2/4] Node health checks...")
	httpClient := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
		},
	}
	for i, addr := range statusAddrs {
		resp, err := httpClient.Get("http://" + addr + "/status")
		if err != nil {
			fmt.Printf("  Node %d (%s): WARN - %v\n", i+1, addr, err)
			continue
		}
		resp.Body.Close()
		fmt.Printf("  Node %d (%s): OK (status %d)\n", i+1, addr, resp.StatusCode)
	}
	fmt.Println()

	// Step 3: Write data via cluster.
	fmt.Println("[3/4] Writing data via cluster...")
	var writerIdx int
	var writerClient tikvpb.TikvClient
	var writerConn *grpc.ClientConn

	testKey := []byte("pd-cluster-verify-key")
	testValue := []byte("pd-cluster-verify-value-" + time.Now().Format("150405"))
	startTS := uint64(time.Now().UnixNano() / 1000)
	commitTS := startTS + 10

	for i, addr := range nodeAddrs {
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			fmt.Printf("  Node %d (%s): cannot connect - %v\n", i+1, addr, err)
			continue
		}

		client := tikvpb.NewTikvClient(conn)
		prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: testKey, Value: testValue}},
			PrimaryLock:  testKey,
			StartVersion: startTS,
			LockTtl:      5000,
		})
		if err != nil {
			conn.Close()
			fmt.Printf("  Node %d (%s): prewrite failed - %v (probably not leader)\n", i+1, addr, err)
			continue
		}
		if len(prewriteResp.GetErrors()) > 0 {
			conn.Close()
			fmt.Printf("  Node %d (%s): prewrite errors - %v\n", i+1, addr, prewriteResp.GetErrors())
			continue
		}

		commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Keys:          [][]byte{testKey},
			StartVersion:  startTS,
			CommitVersion: commitTS,
		})
		if err != nil {
			conn.Close()
			fmt.Printf("  Node %d (%s): commit failed - %v\n", i+1, addr, err)
			continue
		}
		if commitResp.GetError() != nil {
			conn.Close()
			fmt.Printf("  Node %d (%s): commit error - %v\n", i+1, addr, commitResp.GetError())
			continue
		}

		writerIdx = i
		writerClient = client
		writerConn = conn
		fmt.Printf("  Wrote key=%s value=%s via node %d (%s)\n", testKey, testValue, i+1, addr)
		break
	}

	if writerClient == nil {
		log.Fatal("  FAIL: could not write to any node")
	}

	// Verify on the writer node.
	getResp, err := writerClient.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     testKey,
		Version: commitTS + 10,
	})
	if err != nil {
		log.Fatalf("  FAIL: get on writer node: %v", err)
	}
	if getResp.GetNotFound() {
		log.Fatal("  FAIL: key not found on writer node")
	}
	fmt.Printf("  Verified on writer node %d: value=%s\n", writerIdx+1, getResp.GetValue())
	writerConn.Close()
	fmt.Println()

	// Step 4: Read from a different node.
	fmt.Println("[4/4] Cross-node read verification...")
	time.Sleep(1 * time.Second)

	verified := 0
	for i, addr := range nodeAddrs {
		if i == writerIdx {
			continue
		}

		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			fmt.Printf("  Node %d (%s): cannot connect - %v\n", i+1, addr, err)
			continue
		}

		client := tikvpb.NewTikvClient(conn)
		resp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
			Key:     testKey,
			Version: commitTS + 10,
		})
		conn.Close()

		if err != nil {
			fmt.Printf("  Node %d (%s): FAIL - %v\n", i+1, addr, err)
			continue
		}
		if resp.GetNotFound() {
			fmt.Printf("  Node %d (%s): FAIL - key not found (replication may not have propagated)\n", i+1, addr)
			continue
		}
		if string(resp.GetValue()) != string(testValue) {
			fmt.Printf("  Node %d (%s): FAIL - value mismatch: got %s, want %s\n",
				i+1, addr, resp.GetValue(), testValue)
			continue
		}

		fmt.Printf("  Node %d (%s): OK - value=%s\n", i+1, addr, resp.GetValue())
		verified++
	}

	fmt.Println()
	if verified >= 1 {
		fmt.Printf("=== PASS: PD cluster cross-node replication verified (%d/%d follower nodes) ===\n", verified, len(nodeAddrs)-1)
	} else {
		fmt.Println("=== FAIL: PD cluster cross-node replication not verified ===")
		os.Exit(1)
	}
}
