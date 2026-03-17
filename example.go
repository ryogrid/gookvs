package main

import (
	"context"
	"fmt"
	"log"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to a running gookvs-server
	conn, err := grpc.Dial(
		"127.0.0.1:20160",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	ctx := context.Background()

	// Phase 1: Prewrite
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("hello"), Value: []byte("world")},
		},
		PrimaryLock:  []byte("hello"),
		StartVersion: 10,
		LockTtl:      5000,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Prewrite errors: %v\n", prewriteResp.GetErrors())

	// Phase 2: Commit
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("hello")},
		StartVersion:  10,
		CommitVersion: 20,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Commit error: %v\n", commitResp.GetError())

	// Read back
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     []byte("hello"),
		Version: 30,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: key=%s value=%s not_found=%v\n",
		"hello", string(getResp.GetValue()), getResp.GetNotFound())
}
