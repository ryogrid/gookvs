package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookv/pkg/client"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Mock rawKVAPI
// ---------------------------------------------------------------------------

type mockRawKV struct {
	// data store
	data    map[string][]byte
	ttls    map[string]uint64
	scanFn  func(ctx context.Context, startKey, endKey []byte, limit int) ([]client.KvPair, error)
	casFn   func(ctx context.Context, key, value, prevValue []byte, prevNotExist bool) (bool, []byte, error)
	checksumFn func(ctx context.Context, startKey, endKey []byte) (uint64, uint64, uint64, error)
	errOnPut error
}

func newMockRawKV() *mockRawKV {
	return &mockRawKV{
		data: make(map[string][]byte),
		ttls: make(map[string]uint64),
	}
}

func (m *mockRawKV) Get(_ context.Context, key []byte) ([]byte, bool, error) {
	v, ok := m.data[string(key)]
	if !ok {
		return nil, true, nil
	}
	return v, false, nil
}

func (m *mockRawKV) Put(_ context.Context, key, value []byte) error {
	if m.errOnPut != nil {
		return m.errOnPut
	}
	m.data[string(key)] = value
	return nil
}

func (m *mockRawKV) PutWithTTL(_ context.Context, key, value []byte, ttl uint64) error {
	m.data[string(key)] = value
	m.ttls[string(key)] = ttl
	return nil
}

func (m *mockRawKV) Delete(_ context.Context, key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockRawKV) GetKeyTTL(_ context.Context, key []byte) (uint64, error) {
	ttl, ok := m.ttls[string(key)]
	if !ok {
		return 0, nil
	}
	return ttl, nil
}

func (m *mockRawKV) BatchGet(_ context.Context, keys [][]byte) ([]client.KvPair, error) {
	var pairs []client.KvPair
	for _, k := range keys {
		if v, ok := m.data[string(k)]; ok {
			pairs = append(pairs, client.KvPair{Key: k, Value: v})
		}
	}
	return pairs, nil
}

func (m *mockRawKV) BatchPut(_ context.Context, pairs []client.KvPair) error {
	for _, p := range pairs {
		m.data[string(p.Key)] = p.Value
	}
	return nil
}

func (m *mockRawKV) BatchDelete(_ context.Context, keys [][]byte) error {
	for _, k := range keys {
		delete(m.data, string(k))
	}
	return nil
}

func (m *mockRawKV) Scan(_ context.Context, startKey, endKey []byte, limit int) ([]client.KvPair, error) {
	if m.scanFn != nil {
		return m.scanFn(nil, startKey, endKey, limit)
	}
	return nil, nil
}

func (m *mockRawKV) DeleteRange(_ context.Context, startKey, endKey []byte) error {
	return nil
}

func (m *mockRawKV) CompareAndSwap(_ context.Context, key, value, prevValue []byte, prevNotExist bool) (bool, []byte, error) {
	if m.casFn != nil {
		return m.casFn(nil, key, value, prevValue, prevNotExist)
	}
	return true, prevValue, nil
}

func (m *mockRawKV) Checksum(_ context.Context, startKey, endKey []byte) (uint64, uint64, uint64, error) {
	if m.checksumFn != nil {
		return m.checksumFn(nil, startKey, endKey)
	}
	return 0xDEADBEEF, 42, 1024, nil
}

func (m *mockRawKV) BatchScan(_ context.Context, ranges []client.KeyRange, eachLimit int) ([]client.KvPair, error) {
	var result []client.KvPair
	for _, r := range ranges {
		count := 0
		// Collect keys in range, sorted.
		var keys []string
		for k := range m.data {
			if k >= string(r.StartKey) && (len(r.EndKey) == 0 || k < string(r.EndKey)) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		for _, k := range keys {
			if count >= eachLimit {
				break
			}
			result = append(result, client.KvPair{Key: []byte(k), Value: m.data[k]})
			count++
		}
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Mock txnKVAPI
// ---------------------------------------------------------------------------

type mockTxnKV struct {
	beginFn func(ctx context.Context, opts ...client.TxnOption) (*client.TxnHandle, error)
}

func (m *mockTxnKV) Begin(ctx context.Context, opts ...client.TxnOption) (*client.TxnHandle, error) {
	if m.beginFn != nil {
		return m.beginFn(ctx, opts...)
	}
	return nil, fmt.Errorf("mock: Begin not configured")
}

// ---------------------------------------------------------------------------
// Raw KV executor tests
// ---------------------------------------------------------------------------

func TestExecRawGet(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.data["mykey"] = []byte("myvalue")

	exec := newTestExecutor(raw, nil, nil)

	// Found
	result, err := exec.Exec(ctx, Command{Type: CmdGet, Args: [][]byte{[]byte("mykey")}})
	require.NoError(t, err)
	assert.Equal(t, ResultValue, result.Type)
	assert.Equal(t, []byte("myvalue"), result.Value)

	// Not found
	result, err = exec.Exec(ctx, Command{Type: CmdGet, Args: [][]byte{[]byte("nokey")}})
	require.NoError(t, err)
	assert.Equal(t, ResultNotFound, result.Type)

	// Empty key error
	_, err = exec.Exec(ctx, Command{Type: CmdGet, Args: [][]byte{[]byte("")}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key must not be empty")
}

func TestExecRawPut(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{Type: CmdPut, Args: [][]byte{[]byte("k1"), []byte("v1")}})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Equal(t, []byte("v1"), raw.data["k1"])

	// Empty key error
	_, err = exec.Exec(ctx, Command{Type: CmdPut, Args: [][]byte{[]byte(""), []byte("v")}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key must not be empty")

	// Error from backend
	raw.errOnPut = errors.New("connection refused")
	_, err = exec.Exec(ctx, Command{Type: CmdPut, Args: [][]byte{[]byte("k2"), []byte("v2")}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection refused")
}

func TestExecRawPutTTL(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdPutTTL,
		Args:   [][]byte{[]byte("k1"), []byte("v1")},
		IntArg: 3600,
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Equal(t, uint64(3600), raw.ttls["k1"])
}

func TestExecRawDelete(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.data["k1"] = []byte("v1")
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{Type: CmdDelete, Args: [][]byte{[]byte("k1")}})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	_, exists := raw.data["k1"]
	assert.False(t, exists)
}

func TestExecRawTTL(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.ttls["session"] = 3542
	exec := newTestExecutor(raw, nil, nil)

	// With TTL
	result, err := exec.Exec(ctx, Command{Type: CmdTTL, Args: [][]byte{[]byte("session")}})
	require.NoError(t, err)
	assert.Equal(t, ResultScalar, result.Type)
	assert.Equal(t, "TTL: 3542s", result.Scalar)

	// No expiration
	result, err = exec.Exec(ctx, Command{Type: CmdTTL, Args: [][]byte{[]byte("nokey")}})
	require.NoError(t, err)
	assert.Equal(t, "TTL: 0 (no expiration)", result.Scalar)
}

func TestExecRawScan(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.scanFn = func(_ context.Context, _, _ []byte, limit int) ([]client.KvPair, error) {
		pairs := []client.KvPair{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
			{Key: []byte("c"), Value: []byte("3")},
		}
		if limit < len(pairs) {
			return pairs[:limit], nil
		}
		return pairs, nil
	}
	exec := newTestExecutor(raw, nil, nil)

	// Default limit
	result, err := exec.Exec(ctx, Command{
		Type: CmdScan,
		Args: [][]byte{[]byte("a"), []byte("z")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultRows, result.Type)
	assert.Equal(t, 3, len(result.Rows))

	// With limit
	result, err = exec.Exec(ctx, Command{
		Type:   CmdScan,
		Args:   [][]byte{[]byte("a"), []byte("z")},
		IntArg: 2,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(result.Rows))

	// Empty result
	raw.scanFn = func(_ context.Context, _, _ []byte, _ int) ([]client.KvPair, error) {
		return nil, nil
	}
	result, err = exec.Exec(ctx, Command{
		Type: CmdScan,
		Args: [][]byte{[]byte("z"), []byte("zz")},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Rows))
}

func TestExecRawBatchGet(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.data["k1"] = []byte("v1")
	raw.data["k2"] = []byte("v2")
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type: CmdBatchGet,
		Args: [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultRows, result.Type)
	assert.Equal(t, 2, len(result.Rows))
	assert.Equal(t, 1, result.NotFoundCount)
}

func TestExecRawBatchPut(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type: CmdBatchPut,
		Args: [][]byte{[]byte("k1"), []byte("v1"), []byte("k2"), []byte("v2"), []byte("k3"), []byte("v3")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Contains(t, result.Message, "3 pairs")
	assert.Equal(t, []byte("v1"), raw.data["k1"])
	assert.Equal(t, []byte("v2"), raw.data["k2"])
	assert.Equal(t, []byte("v3"), raw.data["k3"])
}

func TestExecRawBatchDelete(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.data["k1"] = []byte("v1")
	raw.data["k2"] = []byte("v2")
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type: CmdBatchDelete,
		Args: [][]byte{[]byte("k1"), []byte("k2")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Contains(t, result.Message, "2 keys")
}

func TestExecRawDeleteRange(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type: CmdDeleteRange,
		Args: [][]byte{[]byte("a"), []byte("z")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
}

func TestExecRawCAS(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)

	// Swapped
	raw.casFn = func(_ context.Context, _, _, _ []byte, _ bool) (bool, []byte, error) {
		return true, []byte("old"), nil
	}
	result, err := exec.Exec(ctx, Command{
		Type: CmdCAS,
		Args: [][]byte{[]byte("key"), []byte("new"), []byte("old")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultCAS, result.Type)
	assert.True(t, result.Swapped)
	assert.Equal(t, []byte("old"), result.PrevVal)

	// Not swapped
	raw.casFn = func(_ context.Context, _, _, _ []byte, _ bool) (bool, []byte, error) {
		return false, []byte("actual"), nil
	}
	result, err = exec.Exec(ctx, Command{
		Type: CmdCAS,
		Args: [][]byte{[]byte("key"), []byte("new"), []byte("expected")},
	})
	require.NoError(t, err)
	assert.False(t, result.Swapped)
	assert.Equal(t, []byte("actual"), result.PrevVal)

	// NOT_EXIST
	raw.casFn = func(_ context.Context, _, _, _ []byte, notExist bool) (bool, []byte, error) {
		assert.True(t, notExist)
		return true, nil, nil
	}
	result, err = exec.Exec(ctx, Command{
		Type:  CmdCAS,
		Args:  [][]byte{[]byte("key"), []byte("new"), []byte("_")},
		Flags: flagNotExist,
	})
	require.NoError(t, err)
	assert.True(t, result.Swapped)
	assert.True(t, result.PrevNotFound)

	// Empty key error
	_, err = exec.Exec(ctx, Command{
		Type: CmdCAS,
		Args: [][]byte{[]byte(""), []byte("new"), []byte("old")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "key must not be empty")
}

func TestExecRawChecksum(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	raw.checksumFn = func(_ context.Context, _, _ []byte) (uint64, uint64, uint64, error) {
		return 0xA3F7E2B104C8D91E, 1247, 89412, nil
	}
	exec := newTestExecutor(raw, nil, nil)

	result, err := exec.Exec(ctx, Command{
		Type: CmdChecksum,
		Args: [][]byte{[]byte("user:"), []byte("user:~")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultChecksum, result.Type)
	assert.Equal(t, uint64(0xA3F7E2B104C8D91E), result.Checksum)
	assert.Equal(t, uint64(1247), result.TotalKvs)
	assert.Equal(t, uint64(89412), result.TotalBytes)

	// Full keyspace (no args)
	result, err = exec.Exec(ctx, Command{Type: CmdChecksum})
	require.NoError(t, err)
	assert.Equal(t, ResultChecksum, result.Type)
}

// ---------------------------------------------------------------------------
// Transaction executor tests
// ---------------------------------------------------------------------------

func TestTxnBeginAlreadyActive(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, &mockTxnKV{}, nil)
	// Simulate active txn
	exec.activeTxn = &client.TxnHandle{}

	_, err := exec.Exec(ctx, Command{Type: CmdBegin})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction already in progress")
}

func TestTxnSetWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{
		Type: CmdTxnSet,
		Args: [][]byte{[]byte("k1"), []byte("v1")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnGetWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{
		Type: CmdTxnGet,
		Args: [][]byte{[]byte("k1")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnDeleteWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{
		Type: CmdTxnDelete,
		Args: [][]byte{[]byte("k1")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnScanWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{
		Type: CmdTxnScan,
		Args: [][]byte{[]byte("a"), []byte("z")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnCommitWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{Type: CmdCommit})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnRollbackWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{Type: CmdRollback})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestTxnBatchGetWithoutTxn(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	_, err := exec.Exec(ctx, Command{
		Type: CmdTxnBatchGet,
		Args: [][]byte{[]byte("k1"), []byte("k2")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no active transaction")
}

func TestInTransaction(t *testing.T) {
	exec := newTestExecutor(nil, nil, nil)
	assert.False(t, exec.InTransaction())

	exec.activeTxn = &client.TxnHandle{}
	assert.True(t, exec.InTransaction())

	exec.activeTxn = nil
	assert.False(t, exec.InTransaction())
}

// ---------------------------------------------------------------------------
// Admin executor tests
// ---------------------------------------------------------------------------

func setupMockPD() *pdclient.MockClient {
	pd := pdclient.NewMockClient(1)

	// Add stores
	pd.SetStore(&metapb.Store{
		Id:      1,
		Address: "127.0.0.1:20160",
		State:   metapb.StoreState_Up,
	})
	pd.SetStore(&metapb.Store{
		Id:      2,
		Address: "127.0.0.1:20161",
		State:   metapb.StoreState_Up,
	})

	// Add a region covering the whole keyspace
	pd.SetRegion(&metapb.Region{
		Id:       10,
		StartKey: nil,
		EndKey:   []byte("m"),
		Peers: []*metapb.Peer{
			{Id: 100, StoreId: 1},
			{Id: 101, StoreId: 2},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
	}, &metapb.Peer{Id: 100, StoreId: 1})

	pd.SetRegion(&metapb.Region{
		Id:       11,
		StartKey: []byte("m"),
		EndKey:   nil,
		Peers: []*metapb.Peer{
			{Id: 102, StoreId: 1},
			{Id: 103, StoreId: 2},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
	}, &metapb.Peer{Id: 103, StoreId: 2})

	return pd
}

func TestAdminStoreList(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdStoreList})
	require.NoError(t, err)
	assert.Equal(t, ResultTable, result.Type)
	assert.Equal(t, []string{"StoreID", "Address", "State"}, result.Columns)
	assert.Equal(t, 2, len(result.TableRows))
}

func TestAdminStoreListEmpty(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdStoreList})
	require.NoError(t, err)
	assert.Equal(t, ResultTable, result.Type)
	assert.Equal(t, 0, len(result.TableRows))
}

func TestAdminStoreStatus(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdStoreStatus, IntArg: 1})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Store ID:  1")
	assert.Contains(t, result.Message, "127.0.0.1:20160")
	assert.Contains(t, result.Message, "Up")
}

func TestAdminStoreStatusNotFound(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	_, err := exec.Exec(ctx, Command{Type: CmdStoreStatus, IntArg: 99})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "99")
}

func TestAdminRegion(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdRegion, Args: [][]byte{[]byte("abc")}})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Region ID:  10")
}

func TestAdminRegionList(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdRegionList})
	require.NoError(t, err)
	assert.Equal(t, ResultTable, result.Type)
	assert.Equal(t, 2, len(result.TableRows))
}

func TestAdminRegionListLimit(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdRegionList, IntArg: 1})
	require.NoError(t, err)
	assert.Equal(t, ResultTable, result.Type)
	assert.Equal(t, 1, len(result.TableRows))
}

func TestAdminRegionByID(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdRegionByID, IntArg: 10})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Region ID:  10")
}

func TestAdminClusterInfo(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdClusterInfo})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Cluster ID: 1")
	assert.Contains(t, result.Message, "Stores: 2")
	assert.Contains(t, result.Message, "Regions: 2")
}

func TestAdminTSO(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdTSO})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Timestamp:")
	assert.Contains(t, result.Message, "physical:")
	assert.Contains(t, result.Message, "logical:")
}

func TestAdminGCSafePointNonZero(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	// Set a non-zero GC safe point
	pd.UpdateGCSafePoint(ctx, 1000<<18|42)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdGCSafepoint})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "GC SafePoint:")
	assert.Contains(t, result.Message, "physical:")
	assert.Contains(t, result.Message, "logical:")
}

func TestAdminGCSafePointZero(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdGCSafepoint})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "0 (not set)")
}

// ---------------------------------------------------------------------------
// PD admin executor tests (new commands)
// ---------------------------------------------------------------------------

func TestExecBootstrap(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdBootstrap,
		IntArg: 1,
		StrArg: "127.0.0.1:20160",
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Contains(t, result.Message, "Bootstrapped store 1")
	assert.Contains(t, result.Message, "127.0.0.1:20160")

	// Verify the store was actually registered
	store, err := pd.GetStore(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20160", store.GetAddress())

	// Second bootstrap should fail
	_, err = exec.Exec(ctx, Command{
		Type:   CmdBootstrap,
		IntArg: 2,
		StrArg: "127.0.0.1:20161",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already bootstrapped")
}

func TestExecBootstrapWithRegionID(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdBootstrap,
		IntArg: 1,
		StrArg: "127.0.0.1:20160",
		Args:   [][]byte{[]byte("5")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)

	// Verify the region was created with regionID=5
	region, _, err := pd.GetRegionByID(ctx, 5)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), region.GetId())
}

func TestExecPutStore(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdPutStore,
		IntArg: 2,
		StrArg: "127.0.0.1:20161",
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Contains(t, result.Message, "Registered store 2")
	assert.Contains(t, result.Message, "127.0.0.1:20161")

	// Verify store was registered
	store, err := pd.GetStore(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20161", store.GetAddress())
}

func TestExecAllocID(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{Type: CmdAllocID})
	require.NoError(t, err)
	assert.Equal(t, ResultScalar, result.Type)
	// MockClient starts nextID at 1000 and increments by 1
	assert.Equal(t, "1001", result.Scalar)

	// Second alloc should increment
	result, err = exec.Exec(ctx, Command{Type: CmdAllocID})
	require.NoError(t, err)
	assert.Equal(t, "1002", result.Scalar)
}

func TestExecIsBootstrapped(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	// Not bootstrapped yet
	result, err := exec.Exec(ctx, Command{Type: CmdIsBootstrapped})
	require.NoError(t, err)
	assert.Equal(t, ResultScalar, result.Type)
	assert.Equal(t, "false", result.Scalar)

	// Bootstrap the cluster
	_, err = exec.Exec(ctx, Command{
		Type:   CmdBootstrap,
		IntArg: 1,
		StrArg: "127.0.0.1:20160",
	})
	require.NoError(t, err)

	// Now bootstrapped
	result, err = exec.Exec(ctx, Command{Type: CmdIsBootstrapped})
	require.NoError(t, err)
	assert.Equal(t, "true", result.Scalar)
}

func TestExecAskSplit(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdAskSplit,
		IntArg: 10, // existing region from setupMockPD
		Args:   [][]byte{[]byte("2")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultTable, result.Type)
	assert.Equal(t, []string{"NewRegionID", "NewPeerIDs"}, result.Columns)
	assert.Equal(t, 2, len(result.TableRows)) // count=2
}

func TestExecAskSplitRegionNotFound(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	_, err := exec.Exec(ctx, Command{
		Type:   CmdAskSplit,
		IntArg: 999,
		Args:   [][]byte{[]byte("1")},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "999")
}

func TestExecReportSplit(t *testing.T) {
	ctx := context.Background()
	pd := setupMockPD()
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdReportSplit,
		IntArg: 10, // left region (existing from setupMockPD)
		Args:   [][]byte{[]byte("200"), []byte("f")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)
	assert.Contains(t, result.Message, "Split reported")

	// Verify regions were updated: left region should now end at "f"
	left, _, err := pd.GetRegionByID(ctx, 10)
	require.NoError(t, err)
	assert.Equal(t, []byte("f"), left.GetEndKey())

	// Right region should exist with start key "f"
	right, _, err := pd.GetRegionByID(ctx, 200)
	require.NoError(t, err)
	assert.Equal(t, []byte("f"), right.GetStartKey())
	assert.Equal(t, []byte("m"), right.GetEndKey()) // original end key of region 10
}

func TestExecStoreHeartbeat(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdStoreHeartbeat,
		IntArg: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)

	// Verify stats were recorded
	stats := pd.GetStoreStats(1)
	require.NotNil(t, stats)
	assert.Equal(t, uint64(1), stats.GetStoreId())
	assert.Equal(t, uint32(0), stats.GetRegionCount())
}

func TestExecStoreHeartbeatWithRegions(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdStoreHeartbeat,
		IntArg: 1,
		Args:   [][]byte{[]byte("5")},
	})
	require.NoError(t, err)
	assert.Equal(t, ResultOK, result.Type)

	// Verify region count in stats
	stats := pd.GetStoreStats(1)
	require.NotNil(t, stats)
	assert.Equal(t, uint32(5), stats.GetRegionCount())
}

func TestExecGCSafePointSet(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	result, err := exec.Exec(ctx, Command{
		Type:   CmdGCSafepoint,
		IntArg: 1000,
		StrArg: "SET",
	})
	require.NoError(t, err)
	assert.Equal(t, ResultScalar, result.Type)
	assert.Equal(t, "1000", result.Scalar)

	// Verify the safe point was actually set
	sp, err := pd.GetGCSafePoint(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), sp)
}

func TestExecGCSafePointSetOnlyForward(t *testing.T) {
	ctx := context.Background()
	pd := pdclient.NewMockClient(1)
	exec := newTestExecutor(nil, nil, pd)

	// Set to 1000
	_, err := exec.Exec(ctx, Command{
		Type: CmdGCSafepoint, IntArg: 1000, StrArg: "SET",
	})
	require.NoError(t, err)

	// Try to set to 500 (should be ignored, PD only moves forward)
	result, err := exec.Exec(ctx, Command{
		Type: CmdGCSafepoint, IntArg: 500, StrArg: "SET",
	})
	require.NoError(t, err)
	assert.Equal(t, "1000", result.Scalar) // returns old value
}

// ---------------------------------------------------------------------------
// Meta command tests
// ---------------------------------------------------------------------------

func TestMetaHelp(t *testing.T) {
	ctx := context.Background()
	exec := newTestExecutor(nil, nil, nil)

	// Via executor (HELP;)
	result, err := exec.Exec(ctx, Command{Type: CmdHelp})
	require.NoError(t, err)
	assert.Equal(t, ResultMessage, result.Type)
	assert.Contains(t, result.Message, "Raw KV Commands:")
	assert.Contains(t, result.Message, "Transaction Commands:")
	assert.Contains(t, result.Message, "Admin Commands:")
	assert.Contains(t, result.Message, "Meta Commands:")
}

func TestMetaTimingToggle(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	// Default is ON, toggle to OFF
	code := handleMetaCommand(`\t`, exec, fmtr)
	assert.Equal(t, -1, code)
	assert.False(t, fmtr.showTiming)
	assert.Contains(t, buf.String(), "Timing: OFF")

	buf.Reset()
	// Toggle back to ON
	code = handleMetaCommand(`\t`, exec, fmtr)
	assert.Equal(t, -1, code)
	assert.True(t, fmtr.showTiming)
	assert.Contains(t, buf.String(), "Timing: ON")
}

func TestMetaTimingOnOff(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	handleMetaCommand(`\timing off`, exec, fmtr)
	assert.False(t, fmtr.showTiming)

	handleMetaCommand(`\timing on`, exec, fmtr)
	assert.True(t, fmtr.showTiming)
}

func TestMetaFormatTable(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	handleMetaCommand(`\format table`, exec, fmtr)
	assert.Equal(t, FormatTable, fmtr.outputFmt)
	assert.Contains(t, buf.String(), "Format: TABLE")
}

func TestMetaFormatPlain(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	handleMetaCommand(`\format plain`, exec, fmtr)
	assert.Equal(t, FormatPlain, fmtr.outputFmt)
	assert.Contains(t, buf.String(), "Format: PLAIN")
}

func TestMetaFormatHex(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	handleMetaCommand(`\format hex`, exec, fmtr)
	assert.Equal(t, FormatHex, fmtr.outputFmt)
	assert.Contains(t, buf.String(), "Format: HEX")
}

func TestMetaPagesize(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	handleMetaCommand(`\pagesize 50`, exec, fmtr)
	assert.Equal(t, 50, exec.defaultScanLimit)
	assert.Contains(t, buf.String(), "Page size: 50")
}

func TestMetaHexCycle(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	// Auto -> Hex
	handleMetaCommand(`\x`, exec, fmtr)
	assert.Equal(t, DisplayHex, fmtr.displayMode)
	assert.Contains(t, buf.String(), "Display mode: HEX")

	buf.Reset()
	// Hex -> String
	handleMetaCommand(`\x`, exec, fmtr)
	assert.Equal(t, DisplayString, fmtr.displayMode)
	assert.Contains(t, buf.String(), "Display mode: STRING")

	buf.Reset()
	// String -> Auto
	handleMetaCommand(`\x`, exec, fmtr)
	assert.Equal(t, DisplayAuto, fmtr.displayMode)
	assert.Contains(t, buf.String(), "Display mode: AUTO")
}

func TestMetaQuit(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	buf := &bytes.Buffer{}
	fmtr.out = buf

	code := handleMetaCommand(`\q`, exec, fmtr)
	assert.Equal(t, 0, code)
	assert.Contains(t, buf.String(), "Goodbye.")
}

func TestMetaUnknown(t *testing.T) {
	fmtr := NewFormatter(&bytes.Buffer{})
	exec := newTestExecutor(nil, nil, nil)
	errBuf := &bytes.Buffer{}
	fmtr.errOut = errBuf

	code := handleMetaCommand(`\foobar`, exec, fmtr)
	assert.Equal(t, -1, code)
	assert.Contains(t, errBuf.String(), "unknown meta command")
}

// ---------------------------------------------------------------------------
// Batch mode test
// ---------------------------------------------------------------------------

func TestRunBatch(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)
	buf := &bytes.Buffer{}
	fmtr := NewFormatter(buf)

	code := runBatch(ctx, exec, fmtr, `PUT k1 v1; GET k1;`)
	assert.Equal(t, 0, code)
	out := buf.String()
	assert.Contains(t, out, "OK")
	assert.Contains(t, out, `"v1"`)
}

func TestRunBatchError(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)
	buf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	fmtr := NewFormatter(buf)
	fmtr.SetErrOut(errBuf)

	code := runBatch(ctx, exec, fmtr, `FROBNICATE;`)
	assert.Equal(t, 1, code)
	assert.Contains(t, errBuf.String(), "unknown command")
}

// ---------------------------------------------------------------------------
// Region info formatting tests
// ---------------------------------------------------------------------------

func TestFormatRegionInfo(t *testing.T) {
	region := &metapb.Region{
		Id:       2,
		StartKey: []byte("account:"),
		EndKey:   []byte("account:m"),
		Peers: []*metapb.Peer{
			{Id: 1, StoreId: 1},
			{Id: 2, StoreId: 2},
			{Id: 3, StoreId: 3},
		},
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 3},
	}
	leader := &metapb.Peer{Id: 2, StoreId: 2}

	msg := formatRegionInfo(region, leader)
	assert.Contains(t, msg, "Region ID:  2")
	assert.Contains(t, msg, hex.EncodeToString([]byte("account:")))
	assert.Contains(t, msg, "conf_ver:1 version:3")
	assert.Contains(t, msg, "store:1")
	assert.Contains(t, msg, "store:2")
	assert.Contains(t, msg, "Leader:    store:2")
}

// ---------------------------------------------------------------------------
// Store state name test
// ---------------------------------------------------------------------------

func TestStoreStateName(t *testing.T) {
	assert.Equal(t, "Up", storeStateName(metapb.StoreState_Up))
	assert.Equal(t, "Offline", storeStateName(metapb.StoreState_Offline))
	assert.Equal(t, "Tombstone", storeStateName(metapb.StoreState_Tombstone))
}

// ---------------------------------------------------------------------------
// executeStatement integration test
// ---------------------------------------------------------------------------

func TestExecuteStatement(t *testing.T) {
	ctx := context.Background()
	raw := newMockRawKV()
	exec := newTestExecutor(raw, nil, nil)
	buf := &bytes.Buffer{}
	fmtr := NewFormatter(buf)

	// PUT then GET
	code := executeStatement(ctx, `PUT mykey myvalue`, exec, fmtr)
	assert.Equal(t, 0, code)
	assert.Contains(t, buf.String(), "OK")

	buf.Reset()
	code = executeStatement(ctx, `GET mykey`, exec, fmtr)
	assert.Equal(t, 0, code)
	assert.Contains(t, buf.String(), `"myvalue"`)
}

// ---------------------------------------------------------------------------
// Help text sanity test
// ---------------------------------------------------------------------------

func TestHelpTextComplete(t *testing.T) {
	assert.True(t, strings.Contains(helpText, "GET <key>"))
	assert.True(t, strings.Contains(helpText, "PUT <key>"))
	assert.True(t, strings.Contains(helpText, "DELETE <key>"))
	assert.True(t, strings.Contains(helpText, "SCAN"))
	assert.True(t, strings.Contains(helpText, "BEGIN"))
	assert.True(t, strings.Contains(helpText, "COMMIT"))
	assert.True(t, strings.Contains(helpText, "ROLLBACK"))
	assert.True(t, strings.Contains(helpText, "STORE LIST"))
	assert.True(t, strings.Contains(helpText, "REGION"))
	assert.True(t, strings.Contains(helpText, "TSO"))
	assert.True(t, strings.Contains(helpText, "GC SAFEPOINT"))
	assert.True(t, strings.Contains(helpText, `\help`))
	assert.True(t, strings.Contains(helpText, `\quit`))
	assert.True(t, strings.Contains(helpText, `\timing`))
	assert.True(t, strings.Contains(helpText, `\format`))
	assert.True(t, strings.Contains(helpText, `\pagesize`))
	assert.True(t, strings.Contains(helpText, `\x`))
}

// ---------------------------------------------------------------------------
// Unused import sink (for linter satisfaction)
// ---------------------------------------------------------------------------

var _ = fmt.Sprintf
var _ = require.NoError
