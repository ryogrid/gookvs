// cluster-verify.go verifies cross-node data replication in a running gookvs cluster.
// Usage: go run scripts/cluster-verify.go
//
// It writes data to the leader node and reads it back from a different node
// to confirm Raft-based replication is working.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var nodeAddrs = []string{
	"127.0.0.1:20160",
	"127.0.0.1:20161",
	"127.0.0.1:20162",
	"127.0.0.1:20163",
	"127.0.0.1:20164",
}

var statusAddrs = []string{
	"127.0.0.1:20180",
	"127.0.0.1:20181",
	"127.0.0.1:20182",
	"127.0.0.1:20183",
	"127.0.0.1:20184",
}

func main() {
	fmt.Println("=== gookvs Cluster Verification ===")
	fmt.Println()

	// Step 1: Health check all nodes.
	fmt.Println("[1/3] Health check...")
	for i, addr := range statusAddrs {
		resp, err := http.Get("http://" + addr + "/status")
		if err != nil {
			log.Fatalf("  Node %d (%s): FAIL - %v", i+1, addr, err)
		}
		resp.Body.Close()
		fmt.Printf("  Node %d (%s): OK (status %d)\n", i+1, addr, resp.StatusCode)
	}
	fmt.Println()

	// Step 2: Write data to node 1 (may or may not be leader).
	// Try each node until we find the leader (prewrite succeeds without Raft error).
	fmt.Println("[2/3] Writing data via cluster...")
	var writerIdx int
	var writerClient tikvpb.TikvClient
	var writerConn *grpc.ClientConn

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testKey := []byte("cluster-verify-key")
	testValue := []byte("cluster-verify-value-" + time.Now().Format("150405"))
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

		// Commit
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

	// Step 3: Read from a different node.
	fmt.Println("[3/3] Cross-node read verification...")
	time.Sleep(1 * time.Second) // Allow replication to propagate.

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
		fmt.Printf("=== PASS: Cross-node replication verified (%d/%d follower nodes) ===\n", verified, len(nodeAddrs)-1)
	} else {
		fmt.Println("=== FAIL: Cross-node replication not verified ===")
		os.Exit(1)
	}
}
