package e2elib

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/pkg/client"
	"github.com/ryogrid/gookv/pkg/codec"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewStandaloneNode creates and starts a single gookv-server in standalone mode
// (no PD, no Raft, no cluster). Useful for tests that only need basic KV operations.
func NewStandaloneNode(t *testing.T) *GokvNode {
	t.Helper()
	SkipIfNoBinary(t, "gookv-server")

	alloc := NewPortAllocator()
	t.Cleanup(func() { alloc.ReleaseAll() })

	node := NewGokvNode(t, alloc, GokvNodeConfig{
		// StoreID=0 and PDEndpoints=nil → standalone mode
	})
	if err := node.Start(); err != nil {
		t.Fatalf("NewStandaloneNode: start failed: %v", err)
	}
	if err := node.WaitForReady(15 * time.Second); err != nil {
		t.Fatalf("NewStandaloneNode: not ready: %v", err)
	}
	return node
}

// PutAndVerify puts a key-value pair and then reads it back to verify correctness.
func PutAndVerify(t *testing.T, rawKV *client.RawKVClient, key, value []byte) {
	t.Helper()
	ctx := context.Background()

	err := rawKV.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("PutAndVerify: put failed: %v", err)
	}

	got, notFound, err := rawKV.Get(ctx, key)
	if err != nil {
		t.Fatalf("PutAndVerify: get failed: %v", err)
	}
	if notFound {
		t.Fatalf("PutAndVerify: key %x not found after put", key)
	}
	if string(got) != string(value) {
		t.Fatalf("PutAndVerify: expected %q, got %q", value, got)
	}
}

// GetAndAssert gets a key and asserts the value matches expected.
func GetAndAssert(t *testing.T, rawKV *client.RawKVClient, key, expected []byte) {
	t.Helper()
	ctx := context.Background()

	got, notFound, err := rawKV.Get(ctx, key)
	if err != nil {
		t.Fatalf("GetAndAssert: get failed: %v", err)
	}
	if notFound {
		t.Fatalf("GetAndAssert: key %x not found", key)
	}
	if string(got) != string(expected) {
		t.Fatalf("GetAndAssert: key %x: expected %q, got %q", key, expected, got)
	}
}

// GetAndAssertNotFound gets a key and asserts it does not exist.
func GetAndAssertNotFound(t *testing.T, rawKV *client.RawKVClient, key []byte) {
	t.Helper()
	ctx := context.Background()

	_, notFound, err := rawKV.Get(ctx, key)
	if err != nil {
		t.Fatalf("GetAndAssertNotFound: get failed: %v", err)
	}
	if !notFound {
		t.Fatalf("GetAndAssertNotFound: key %x should not exist but was found", key)
	}
}

// WaitForCondition polls fn() every 200ms until it returns true or the timeout is reached.
func WaitForCondition(t *testing.T, timeout time.Duration, msg string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("WaitForCondition: timed out after %v: %s", timeout, msg)
}

// WaitForRegionCount waits until the PD cluster reports at least minCount regions.
// It probes PD using GetRegion with scanning across the key space.
// Returns the region count observed.
func WaitForRegionCount(t *testing.T, pd pdclient.Client, minCount int, timeout time.Duration) int {
	t.Helper()
	ctx := context.Background()

	var lastCount int
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count := countRegions(ctx, pd)
		lastCount = count
		if count >= minCount {
			return count
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("WaitForRegionCount: expected at least %d regions, got %d after %v", minCount, lastCount, timeout)
	return lastCount
}

// WaitForSplit waits until the cluster has more than 1 region, indicating a split has occurred.
// Returns the region count observed.
func WaitForSplit(t *testing.T, pd pdclient.Client, timeout time.Duration) int {
	t.Helper()
	return WaitForRegionCount(t, pd, 2, timeout)
}

// countRegions walks the key space via GetRegion to count distinct regions.
func countRegions(ctx context.Context, pd pdclient.Client) int {
	count := 0
	key := []byte("")
	for {
		region, _, err := pd.GetRegion(ctx, key)
		if err != nil || region == nil {
			break
		}
		count++
		endKey := region.GetEndKey()
		if len(endKey) == 0 {
			// This is the last region (covers to +infinity).
			break
		}
		// endKey is memcomparable-encoded; decode to raw key for the next GetRegion call.
		rawKey, _, err := codec.DecodeBytes(endKey)
		if err != nil {
			break
		}
		key = rawKey
	}
	return count
}

// WaitForStoreCount waits until PD reports at least minCount stores.
func WaitForStoreCount(t *testing.T, pd pdclient.Client, minCount int, timeout time.Duration) int {
	t.Helper()
	ctx := context.Background()

	var lastCount int
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stores, err := pd.GetAllStores(ctx)
		if err == nil {
			lastCount = len(stores)
			if lastCount >= minCount {
				return lastCount
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("WaitForStoreCount: expected at least %d stores, got %d after %v", minCount, lastCount, timeout)
	return lastCount
}

// WaitForRegionLeader waits until PD reports a leader for the region containing key.
// Returns the leader store ID.
func WaitForRegionLeader(t *testing.T, pd pdclient.Client, key []byte, timeout time.Duration) uint64 {
	t.Helper()
	ctx := context.Background()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, leader, err := pd.GetRegion(ctx, key)
		if err == nil && leader != nil && leader.GetStoreId() != 0 {
			return leader.GetStoreId()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("WaitForRegionLeader: no leader for key %x after %v", key, timeout)
	return 0
}

// SeedAccounts creates numAccounts keys ("account-0", "account-1", ...) each with
// initialBalance as value, using transactional writes in batches of 50.
func SeedAccounts(t *testing.T, txnKV *client.TxnKVClient, numAccounts, initialBalance int) {
	t.Helper()
	ctx := context.Background()
	balanceStr := strconv.Itoa(initialBalance)

	batchSize := 50
	for start := 0; start < numAccounts; start += batchSize {
		end := start + batchSize
		if end > numAccounts {
			end = numAccounts
		}

		txn, err := txnKV.Begin(ctx)
		if err != nil {
			t.Fatalf("SeedAccounts: begin txn: %v", err)
		}

		for i := start; i < end; i++ {
			key := []byte(fmt.Sprintf("account-%d", i))
			if err := txn.Set(ctx, key, []byte(balanceStr)); err != nil {
				t.Fatalf("SeedAccounts: set account-%d: %v", i, err)
			}
		}

		if err := txn.Commit(ctx); err != nil {
			t.Fatalf("SeedAccounts: commit batch [%d,%d): %v", start, end, err)
		}
	}
}

// ReadAllBalances reads all account balances (account-0 through account-(numAccounts-1))
// in a single transaction and returns the total balance and individual balances.
func ReadAllBalances(t *testing.T, txnKV *client.TxnKVClient, numAccounts int) (int, []int) {
	t.Helper()
	ctx := context.Background()

	txn, err := txnKV.Begin(ctx)
	if err != nil {
		t.Fatalf("ReadAllBalances: begin txn: %v", err)
	}

	balances := make([]int, numAccounts)
	total := 0
	for i := 0; i < numAccounts; i++ {
		key := []byte(fmt.Sprintf("account-%d", i))
		val, err := txn.Get(ctx, key)
		if err != nil {
			t.Fatalf("ReadAllBalances: get account-%d: %v", i, err)
		}
		if val == nil {
			t.Fatalf("ReadAllBalances: account-%d not found", i)
		}
		b, err := strconv.Atoi(string(val))
		if err != nil {
			t.Fatalf("ReadAllBalances: parse balance for account-%d: %v", i, err)
		}
		balances[i] = b
		total += b
	}

	// Read-only transaction; commit to release resources.
	if err := txn.Commit(ctx); err != nil {
		// Read-only commits may no-op; ignore errors.
		_ = err
	}

	return total, balances
}

// DialTikvClient creates a raw gRPC TikvClient connection to the given address.
// Uses insecure credentials. The connection is registered for cleanup.
func DialTikvClient(t *testing.T, addr string) tikvpb.TikvClient {
	t.Helper()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialTikvClient: dial %s: %v", addr, err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	return tikvpb.NewTikvClient(conn)
}
