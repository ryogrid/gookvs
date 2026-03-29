package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/ryogrid/gookv/pkg/client"
	"github.com/ryogrid/gookv/pkg/codec"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

// ---------------------------------------------------------------------------
// Mock-friendly interfaces
// ---------------------------------------------------------------------------

// rawKVAPI is the subset of client.RawKVClient methods used by the executor.
type rawKVAPI interface {
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Put(ctx context.Context, key, value []byte) error
	PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error
	Delete(ctx context.Context, key []byte) error
	GetKeyTTL(ctx context.Context, key []byte) (uint64, error)
	BatchGet(ctx context.Context, keys [][]byte) ([]client.KvPair, error)
	BatchPut(ctx context.Context, pairs []client.KvPair) error
	BatchDelete(ctx context.Context, keys [][]byte) error
	Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]client.KvPair, error)
	DeleteRange(ctx context.Context, startKey, endKey []byte) error
	CompareAndSwap(ctx context.Context, key, value, prevValue []byte, prevNotExist bool) (bool, []byte, error)
	Checksum(ctx context.Context, startKey, endKey []byte) (uint64, uint64, uint64, error)
	BatchScan(ctx context.Context, ranges []client.KeyRange, eachLimit int) ([]client.KvPair, error)
}

// txnKVAPI is the subset of client.TxnKVClient methods used by the executor.
type txnKVAPI interface {
	Begin(ctx context.Context, opts ...client.TxnOption) (*client.TxnHandle, error)
}

// txnHandleAPI is the subset of client.TxnHandle methods used by the executor.
type txnHandleAPI interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	BatchGet(ctx context.Context, keys [][]byte) ([]client.KvPair, error)
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	StartTS() interface{} // returns txntypes.TimeStamp (uint64)
}

// pdAPI is the subset of pdclient.Client methods used by the executor.
type pdAPI interface {
	GetTS(ctx context.Context) (pdclient.TimeStamp, error)
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error)
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetAllStores(ctx context.Context) ([]*metapb.Store, error)
	GetGCSafePoint(ctx context.Context) (uint64, error)
	GetClusterID(ctx context.Context) uint64
	// PD admin methods (e2e CLI migration)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)
	IsBootstrapped(ctx context.Context) (bool, error)
	PutStore(ctx context.Context, store *metapb.Store) error
	AllocID(ctx context.Context) (uint64, error)
	AskBatchSplit(ctx context.Context, region *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error)
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error)
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

// Executor dispatches parsed commands to the appropriate handler.
type Executor struct {
	rawKV    rawKVAPI
	txnKV    txnKVAPI
	pdClient pdAPI

	// Transaction state
	activeTxn *client.TxnHandle // nil when not in a transaction

	// Session settings
	defaultScanLimit int

	// Interactive mode flag (for DELETE RANGE confirmation)
	isInteractive bool

	// confirmReader reads confirmation input (for testing)
	confirmReader io.Reader

	// wantExit is set true by CmdExit handler
	wantExit bool
}

// NewExecutor creates an Executor from a connected client.
func NewExecutor(c *client.Client) *Executor {
	return &Executor{
		rawKV:            c.RawKV(),
		txnKV:            c.TxnKV(),
		pdClient:         c.PD(),
		defaultScanLimit: 100,
	}
}

// newTestExecutor creates an Executor with mock interfaces for testing.
func newTestExecutor(raw rawKVAPI, txn txnKVAPI, pd pdAPI) *Executor {
	return &Executor{
		rawKV:            raw,
		txnKV:            txn,
		pdClient:         pd,
		defaultScanLimit: 100,
	}
}

// InTransaction returns true if a transaction is active.
func (e *Executor) InTransaction() bool {
	return e.activeTxn != nil
}

// WantExit returns true if the exit command was issued.
func (e *Executor) WantExit() bool {
	return e.wantExit
}

// Exec dispatches a command to the appropriate handler.
func (e *Executor) Exec(ctx context.Context, cmd Command) (*Result, error) {
	switch cmd.Type {
	// Raw KV
	case CmdGet:
		return e.execRawGet(ctx, cmd)
	case CmdPut:
		return e.execRawPut(ctx, cmd)
	case CmdPutTTL:
		return e.execRawPutTTL(ctx, cmd)
	case CmdDelete:
		return e.execRawDelete(ctx, cmd)
	case CmdTTL:
		return e.execRawTTL(ctx, cmd)
	case CmdScan:
		return e.execRawScan(ctx, cmd)
	case CmdBatchGet:
		return e.execRawBatchGet(ctx, cmd)
	case CmdBatchPut:
		return e.execRawBatchPut(ctx, cmd)
	case CmdBatchDelete:
		return e.execRawBatchDelete(ctx, cmd)
	case CmdDeleteRange:
		return e.execRawDeleteRange(ctx, cmd)
	case CmdCAS:
		return e.execRawCAS(ctx, cmd)
	case CmdChecksum:
		return e.execRawChecksum(ctx, cmd)
	case CmdBatchScan:
		return e.execRawBatchScan(ctx, cmd)

	// Transactional
	case CmdBegin:
		return e.execBegin(ctx, cmd)
	case CmdTxnGet:
		return e.execTxnGet(ctx, cmd)
	case CmdTxnBatchGet:
		return e.execTxnBatchGet(ctx, cmd)
	case CmdTxnSet:
		return e.execTxnSet(ctx, cmd)
	case CmdTxnDelete:
		return e.execTxnDelete(ctx, cmd)
	case CmdCommit:
		return e.execCommit(ctx, cmd)
	case CmdRollback:
		return e.execRollback(ctx, cmd)

	// Admin
	case CmdStoreList:
		return e.execStoreList(ctx, cmd)
	case CmdStoreStatus:
		return e.execStoreStatus(ctx, cmd)
	case CmdRegion:
		return e.execRegion(ctx, cmd)
	case CmdRegionList:
		return e.execRegionList(ctx, cmd)
	case CmdRegionByID:
		return e.execRegionByID(ctx, cmd)
	case CmdClusterInfo:
		return e.execClusterInfo(ctx, cmd)
	case CmdTSO:
		return e.execTSO(ctx, cmd)
	case CmdGCSafepoint:
		return e.execGCSafePoint(ctx, cmd)
	case CmdStatus:
		return e.execStatus(ctx, cmd)

	// PD admin
	case CmdBootstrap:
		return e.execBootstrap(ctx, cmd)
	case CmdPutStore:
		return e.execPutStore(ctx, cmd)
	case CmdAllocID:
		return e.execAllocID(ctx, cmd)
	case CmdIsBootstrapped:
		return e.execIsBootstrapped(ctx, cmd)
	case CmdAskSplit:
		return e.execAskSplit(ctx, cmd)
	case CmdReportSplit:
		return e.execReportSplit(ctx, cmd)
	case CmdStoreHeartbeat:
		return e.execStoreHeartbeat(ctx, cmd)

	// Meta (keyword forms)
	case CmdHelp:
		return e.execHelp(ctx, cmd)
	case CmdExit:
		return e.execExit(ctx, cmd)

	default:
		return nil, fmt.Errorf("unhandled command type: %d", cmd.Type)
	}
}

// ---------------------------------------------------------------------------
// Raw KV handlers
// ---------------------------------------------------------------------------

func (e *Executor) execRawGet(ctx context.Context, cmd Command) (*Result, error) {
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	val, notFound, err := e.rawKV.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if notFound {
		return &Result{Type: ResultNotFound}, nil
	}
	return &Result{Type: ResultValue, Value: val}, nil
}

func (e *Executor) execRawPut(ctx context.Context, cmd Command) (*Result, error) {
	key, val := cmd.Args[0], cmd.Args[1]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	if err := e.rawKV.Put(ctx, key, val); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execRawPutTTL(ctx context.Context, cmd Command) (*Result, error) {
	key, val := cmd.Args[0], cmd.Args[1]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	if err := e.rawKV.PutWithTTL(ctx, key, val, uint64(cmd.IntArg)); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execRawDelete(ctx context.Context, cmd Command) (*Result, error) {
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	if err := e.rawKV.Delete(ctx, key); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execRawTTL(ctx context.Context, cmd Command) (*Result, error) {
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	ttl, err := e.rawKV.GetKeyTTL(ctx, key)
	if err != nil {
		return nil, err
	}
	var scalar string
	if ttl == 0 {
		scalar = "TTL: 0 (no expiration)"
	} else {
		scalar = fmt.Sprintf("TTL: %ds", ttl)
	}
	return &Result{Type: ResultScalar, Scalar: scalar}, nil
}

func (e *Executor) execRawScan(ctx context.Context, cmd Command) (*Result, error) {
	startKey, endKey := cmd.Args[0], cmd.Args[1]
	limit := int(cmd.IntArg)
	if limit <= 0 {
		limit = e.defaultScanLimit
	}
	pairs, err := e.rawKV.Scan(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}
	rows := make([]KvPairResult, len(pairs))
	for i, p := range pairs {
		rows[i] = KvPairResult{Key: p.Key, Value: p.Value}
	}
	return &Result{Type: ResultRows, Rows: rows}, nil
}

func (e *Executor) execRawBatchGet(ctx context.Context, cmd Command) (*Result, error) {
	keys := cmd.Args
	pairs, err := e.rawKV.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	rows := make([]KvPairResult, len(pairs))
	for i, p := range pairs {
		rows[i] = KvPairResult{Key: p.Key, Value: p.Value}
	}
	notFound := len(keys) - len(pairs)
	return &Result{Type: ResultRows, Rows: rows, NotFoundCount: notFound}, nil
}

func (e *Executor) execRawBatchPut(ctx context.Context, cmd Command) (*Result, error) {
	args := cmd.Args
	pairs := make([]client.KvPair, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs = append(pairs, client.KvPair{Key: args[i], Value: args[i+1]})
	}
	if err := e.rawKV.BatchPut(ctx, pairs); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK, Message: fmt.Sprintf("OK (%d pairs)", len(pairs))}, nil
}

func (e *Executor) execRawBatchDelete(ctx context.Context, cmd Command) (*Result, error) {
	keys := cmd.Args
	if err := e.rawKV.BatchDelete(ctx, keys); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK, Message: fmt.Sprintf("OK (%d keys)", len(keys))}, nil
}

func (e *Executor) execRawDeleteRange(ctx context.Context, cmd Command) (*Result, error) {
	startKey, endKey := cmd.Args[0], cmd.Args[1]
	if err := e.rawKV.DeleteRange(ctx, startKey, endKey); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execRawCAS(ctx context.Context, cmd Command) (*Result, error) {
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	newVal := cmd.Args[1]
	oldVal := cmd.Args[2]
	notExist := cmd.Flags&flagNotExist != 0

	swapped, prevVal, err := e.rawKV.CompareAndSwap(ctx, key, newVal, oldVal, notExist)
	if err != nil {
		return nil, err
	}
	return &Result{
		Type:         ResultCAS,
		Swapped:      swapped,
		PrevVal:      prevVal,
		PrevNotFound: prevVal == nil,
	}, nil
}

func (e *Executor) execRawChecksum(ctx context.Context, cmd Command) (*Result, error) {
	var startKey, endKey []byte
	if len(cmd.Args) >= 2 {
		startKey = cmd.Args[0]
		endKey = cmd.Args[1]
	}
	checksum, totalKvs, totalBytes, err := e.rawKV.Checksum(ctx, startKey, endKey)
	if err != nil {
		return nil, err
	}
	return &Result{
		Type:       ResultChecksum,
		Checksum:   checksum,
		TotalKvs:   totalKvs,
		TotalBytes: totalBytes,
	}, nil
}

func (e *Executor) execRawBatchScan(ctx context.Context, cmd Command) (*Result, error) {
	args := cmd.Args
	if len(args)%2 != 0 {
		return nil, fmt.Errorf("BSCAN requires pairs of start/end keys")
	}
	eachLimit := int(cmd.IntArg)
	if eachLimit <= 0 {
		eachLimit = e.defaultScanLimit
	}

	var ranges []client.KeyRange
	for i := 0; i < len(args); i += 2 {
		ranges = append(ranges, client.KeyRange{StartKey: args[i], EndKey: args[i+1]})
	}

	pairs, err := e.rawKV.BatchScan(ctx, ranges, eachLimit)
	if err != nil {
		return nil, err
	}
	rows := make([]KvPairResult, len(pairs))
	for i, p := range pairs {
		rows[i] = KvPairResult{Key: p.Key, Value: p.Value}
	}
	return &Result{Type: ResultRows, Rows: rows}, nil
}

// ---------------------------------------------------------------------------
// Transaction handlers
// ---------------------------------------------------------------------------

func (e *Executor) execBegin(ctx context.Context, cmd Command) (*Result, error) {
	if e.activeTxn != nil {
		return nil, fmt.Errorf("transaction already in progress; COMMIT or ROLLBACK first")
	}
	txn, err := e.txnKV.Begin(ctx, cmd.TxnOpts...)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	e.activeTxn = txn

	// Build mode description
	startTS := uint64(txn.StartTS())
	ts := pdclient.TimeStampFromUint64(startTS)
	t := time.UnixMilli(ts.Physical).UTC()

	var mode string
	hasPessimistic := false
	for _, opt := range cmd.TxnOpts {
		// Apply to a temporary TxnOptions to detect pessimistic
		opts := client.TxnOptions{}
		opt(&opts)
		if opts.Mode == client.TxnModePessimistic {
			hasPessimistic = true
		}
	}
	if hasPessimistic {
		mode = "pessimistic"
	} else {
		mode = "optimistic"
	}

	msg := fmt.Sprintf("Transaction started (%s, startTS=%d)\n  physical: %d (%s)\n  logical:  %d",
		mode, startTS, ts.Physical, t.Format(time.RFC3339), ts.Logical)

	return &Result{Type: ResultBegin, Message: msg}, nil
}

func (e *Executor) execTxnGet(ctx context.Context, cmd Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	val, err := e.activeTxn.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return &Result{Type: ResultNil}, nil
	}
	return &Result{Type: ResultValue, Value: val}, nil
}

func (e *Executor) execTxnBatchGet(ctx context.Context, cmd Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	keys := cmd.Args
	pairs, err := e.activeTxn.BatchGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	rows := make([]KvPairResult, len(pairs))
	for i, p := range pairs {
		rows[i] = KvPairResult{Key: p.Key, Value: p.Value}
	}
	notFound := len(keys) - len(pairs)
	return &Result{Type: ResultRows, Rows: rows, NotFoundCount: notFound}, nil
}

func (e *Executor) execTxnSet(ctx context.Context, cmd Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	key, val := cmd.Args[0], cmd.Args[1]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	if err := e.activeTxn.Set(ctx, key, val); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execTxnDelete(ctx context.Context, cmd Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	key := cmd.Args[0]
	if len(key) == 0 {
		return nil, fmt.Errorf("key must not be empty")
	}
	if err := e.activeTxn.Delete(ctx, key); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

func (e *Executor) execCommit(ctx context.Context, _ Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	err := e.activeTxn.Commit(ctx)
	if err != nil {
		// Keep txn active so user can ROLLBACK, unless already committed
		if err == client.ErrTxnCommitted {
			e.activeTxn = nil
		}
		return nil, err
	}
	e.activeTxn = nil
	return &Result{Type: ResultCommit, Message: "OK (committed)"}, nil
}

func (e *Executor) execRollback(ctx context.Context, _ Command) (*Result, error) {
	if e.activeTxn == nil {
		return nil, fmt.Errorf("no active transaction")
	}
	err := e.activeTxn.Rollback(ctx)
	if err != nil {
		return nil, err
	}
	e.activeTxn = nil
	return &Result{Type: ResultOK, Message: "OK (rolled back)"}, nil
}

// ---------------------------------------------------------------------------
// Admin handlers
// ---------------------------------------------------------------------------

func storeStateName(state metapb.StoreState) string {
	switch state {
	case metapb.StoreState_Up:
		return "Up"
	case metapb.StoreState_Offline:
		return "Offline"
	case metapb.StoreState_Tombstone:
		return "Tombstone"
	default:
		return fmt.Sprintf("Unknown(%d)", int32(state))
	}
}

func (e *Executor) execStoreList(ctx context.Context, _ Command) (*Result, error) {
	stores, err := e.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, err
	}
	columns := []string{"StoreID", "Address", "State"}
	tableRows := make([][]string, 0, len(stores))
	for _, s := range stores {
		tableRows = append(tableRows, []string{
			strconv.FormatUint(s.GetId(), 10),
			s.GetAddress(),
			storeStateName(s.GetState()),
		})
	}
	return &Result{
		Type:      ResultTable,
		Columns:   columns,
		TableRows: tableRows,
		Message:   fmt.Sprintf("(%d stores)", len(stores)),
	}, nil
}

func (e *Executor) execStoreStatus(ctx context.Context, cmd Command) (*Result, error) {
	storeID := uint64(cmd.IntArg)
	store, err := e.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	msg := fmt.Sprintf("Store ID:  %d\nAddress:   %s\nState:     %s",
		store.GetId(), store.GetAddress(), storeStateName(store.GetState()))
	return &Result{Type: ResultMessage, Message: msg}, nil
}

func formatRegionInfo(region *metapb.Region, leader *metapb.Peer) string {
	startHex := hex.EncodeToString(region.GetStartKey())
	endHex := hex.EncodeToString(region.GetEndKey())
	if startHex == "" {
		startHex = "(empty)"
	}
	if endHex == "" {
		endHex = "(empty)"
	}

	// ASCII hint for start/end keys
	startHint := asciiHint(region.GetStartKey())
	endHint := asciiHint(region.GetEndKey())
	if startHint != "" {
		startHex += " (" + startHint + ")"
	}
	if endHint != "" {
		endHex += " (" + endHint + ")"
	}

	// Epoch
	epoch := region.GetRegionEpoch()
	epochStr := fmt.Sprintf("conf_ver:%d version:%d",
		epoch.GetConfVer(), epoch.GetVersion())

	// Peers
	var peers []string
	for _, p := range region.GetPeers() {
		peers = append(peers, fmt.Sprintf("store:%d", p.GetStoreId()))
	}
	peersStr := "[" + strings.Join(peers, ", ") + "]"

	// Leader
	leaderStr := "(none)"
	if leader != nil {
		leaderStr = fmt.Sprintf("store:%d", leader.GetStoreId())
	}

	return fmt.Sprintf("Region ID:  %d\n  StartKey:  %s\n  EndKey:    %s\n  Epoch:     %s\n  Peers:     %s\n  Leader:    %s",
		region.GetId(), startHex, endHex, epochStr, peersStr, leaderStr)
}

func asciiHint(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	if isPrintableASCII(data) {
		return string(data)
	}
	return ""
}

func (e *Executor) execRegion(ctx context.Context, cmd Command) (*Result, error) {
	key := cmd.Args[0]
	region, leader, err := e.pdClient.GetRegion(ctx, key)
	if err != nil {
		return nil, err
	}
	return &Result{Type: ResultMessage, Message: formatRegionInfo(region, leader)}, nil
}

func (e *Executor) execRegionList(ctx context.Context, cmd Command) (*Result, error) {
	limit := int(cmd.IntArg)
	if limit <= 0 {
		limit = 100
	}

	columns := []string{"RegionID", "StartKey", "EndKey", "Peers", "Leader"}
	var tableRows [][]string
	var key []byte // start from ""

	for i := 0; i < limit; i++ {
		region, leader, err := e.pdClient.GetRegion(ctx, key)
		if err != nil {
			// If no region found for key, we've walked past the end
			break
		}
		if region == nil {
			break
		}

		startHex := hex.EncodeToString(region.GetStartKey())
		if startHex == "" {
			startHex = "(empty)"
		}
		endHex := hex.EncodeToString(region.GetEndKey())
		if endHex == "" {
			endHex = "(empty)"
		}

		var peers []string
		for _, p := range region.GetPeers() {
			peers = append(peers, strconv.FormatUint(p.GetStoreId(), 10))
		}

		leaderStr := ""
		if leader != nil {
			leaderStr = strconv.FormatUint(leader.GetStoreId(), 10)
		}

		tableRows = append(tableRows, []string{
			strconv.FormatUint(region.GetId(), 10),
			startHex,
			endHex,
			strings.Join(peers, ","),
			leaderStr,
		})

		// Advance to next region.
		// endKey is memcomparable-encoded; decode to raw key for the next GetRegion call.
		endKey := region.GetEndKey()
		if len(endKey) == 0 {
			// Last region (unbounded upper end)
			break
		}
		rawKey, _, err := codec.DecodeBytes(endKey)
		if err != nil {
			break
		}
		key = rawKey
	}

	return &Result{
		Type:      ResultTable,
		Columns:   columns,
		TableRows: tableRows,
		Message:   fmt.Sprintf("(%d regions)", len(tableRows)),
	}, nil
}

func (e *Executor) execRegionByID(ctx context.Context, cmd Command) (*Result, error) {
	regionID := uint64(cmd.IntArg)
	region, leader, err := e.pdClient.GetRegionByID(ctx, regionID)
	if err != nil {
		return nil, err
	}
	return &Result{Type: ResultMessage, Message: formatRegionInfo(region, leader)}, nil
}

func (e *Executor) execClusterInfo(ctx context.Context, _ Command) (*Result, error) {
	clusterID := e.pdClient.GetClusterID(ctx)

	stores, err := e.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, fmt.Errorf("get stores: %w", err)
	}

	// Count regions by walking keyspace
	regionCount := 0
	var key []byte
	for {
		region, _, err := e.pdClient.GetRegion(ctx, key)
		if err != nil || region == nil {
			break
		}
		regionCount++
		endKey := region.GetEndKey()
		if len(endKey) == 0 {
			break
		}
		key = endKey
	}

	// Build store table portion
	var storeLines []string
	for _, s := range stores {
		storeLines = append(storeLines, fmt.Sprintf("  %d\t%s\t%s",
			s.GetId(), s.GetAddress(), storeStateName(s.GetState())))
	}

	msg := fmt.Sprintf("Cluster ID: %d\nStores: %d\nRegions: %d total",
		clusterID, len(stores), regionCount)

	return &Result{Type: ResultMessage, Message: msg}, nil
}

func (e *Executor) execTSO(ctx context.Context, _ Command) (*Result, error) {
	ts, err := e.pdClient.GetTS(ctx)
	if err != nil {
		return nil, err
	}
	raw := ts.ToUint64()
	t := time.UnixMilli(ts.Physical).UTC()

	msg := fmt.Sprintf("Timestamp:  %d\n  physical: %d (%s)\n  logical:  %d",
		raw, ts.Physical, t.Format(time.RFC3339), ts.Logical)
	return &Result{Type: ResultMessage, Message: msg}, nil
}

func (e *Executor) execGCSafePoint(ctx context.Context, cmd Command) (*Result, error) {
	// GC SAFEPOINT SET <ts>
	if cmd.StrArg == "SET" {
		newSP, err := e.pdClient.UpdateGCSafePoint(ctx, uint64(cmd.IntArg))
		if err != nil {
			return nil, err
		}
		return &Result{Type: ResultScalar, Scalar: fmt.Sprintf("%d", newSP)}, nil
	}

	// GC SAFEPOINT (read)
	safePoint, err := e.pdClient.GetGCSafePoint(ctx)
	if err != nil {
		return nil, err
	}
	if safePoint == 0 {
		return &Result{Type: ResultMessage, Message: "GC SafePoint: 0 (not set)"}, nil
	}
	ts := pdclient.TimeStampFromUint64(safePoint)
	t := time.UnixMilli(ts.Physical).UTC()

	msg := fmt.Sprintf("GC SafePoint: %d\n  physical:   %d (%s)\n  logical:    %d",
		safePoint, ts.Physical, t.Format(time.RFC3339), ts.Logical)
	return &Result{Type: ResultMessage, Message: msg}, nil
}

func (e *Executor) execStatus(_ context.Context, cmd Command) (*Result, error) {
	addr := cmd.StrArg
	if addr == "" {
		return nil, fmt.Errorf("STATUS requires a server address (e.g., STATUS 127.0.0.1:20180)")
	}
	url := fmt.Sprintf("http://%s/status", addr)
	resp, err := http.Get(url) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("status request failed: %v", err)
	}
	defer resp.Body.Close()

	body := make([]byte, 4096)
	n, _ := resp.Body.Read(body)
	return &Result{Type: ResultMessage, Message: string(body[:n])}, nil
}

// ---------------------------------------------------------------------------
// PD admin handlers (e2e CLI migration)
// ---------------------------------------------------------------------------

func (e *Executor) execBootstrap(ctx context.Context, cmd Command) (*Result, error) {
	storeID := uint64(cmd.IntArg)
	addr := cmd.StrArg
	var regionID uint64 = 1
	if len(cmd.Args) > 0 {
		id, _ := strconv.ParseUint(string(cmd.Args[0]), 10, 64)
		if id > 0 {
			regionID = id
		}
	}
	store := &metapb.Store{Id: storeID, Address: addr}
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       []*metapb.Peer{{Id: regionID + 1, StoreId: storeID}},
	}
	_, err := e.pdClient.Bootstrap(ctx, store, region)
	if err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK, Message: fmt.Sprintf("Bootstrapped store %d at %s", storeID, addr)}, nil
}

func (e *Executor) execPutStore(ctx context.Context, cmd Command) (*Result, error) {
	storeID := uint64(cmd.IntArg)
	addr := cmd.StrArg
	store := &metapb.Store{Id: storeID, Address: addr}
	if err := e.pdClient.PutStore(ctx, store); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK, Message: fmt.Sprintf("Registered store %d at %s", storeID, addr)}, nil
}

func (e *Executor) execAllocID(ctx context.Context, _ Command) (*Result, error) {
	id, err := e.pdClient.AllocID(ctx)
	if err != nil {
		return nil, err
	}
	return &Result{Type: ResultScalar, Scalar: fmt.Sprintf("%d", id)}, nil
}

func (e *Executor) execIsBootstrapped(ctx context.Context, _ Command) (*Result, error) {
	bootstrapped, err := e.pdClient.IsBootstrapped(ctx)
	if err != nil {
		return nil, err
	}
	return &Result{Type: ResultScalar, Scalar: fmt.Sprintf("%v", bootstrapped)}, nil
}

func (e *Executor) execAskSplit(ctx context.Context, cmd Command) (*Result, error) {
	regionID := uint64(cmd.IntArg)
	count, _ := strconv.ParseUint(string(cmd.Args[0]), 10, 32)

	region, _, err := e.pdClient.GetRegionByID(ctx, regionID)
	if err != nil {
		return nil, fmt.Errorf("get region %d: %w", regionID, err)
	}
	if region == nil {
		return nil, fmt.Errorf("region %d not found", regionID)
	}

	resp, err := e.pdClient.AskBatchSplit(ctx, region, uint32(count))
	if err != nil {
		return nil, err
	}

	columns := []string{"NewRegionID", "NewPeerIDs"}
	var tableRows [][]string
	for _, id := range resp.GetIds() {
		var peerIDs []string
		for _, pid := range id.GetNewPeerIds() {
			peerIDs = append(peerIDs, strconv.FormatUint(pid, 10))
		}
		tableRows = append(tableRows, []string{
			strconv.FormatUint(id.GetNewRegionId(), 10),
			strings.Join(peerIDs, ","),
		})
	}
	return &Result{Type: ResultTable, Columns: columns, TableRows: tableRows}, nil
}

func (e *Executor) execReportSplit(ctx context.Context, cmd Command) (*Result, error) {
	leftID := uint64(cmd.IntArg)
	rightID, _ := strconv.ParseUint(string(cmd.Args[0]), 10, 64)
	splitKey := cmd.Args[1]

	// Fetch the left region to get its metadata
	leftRegion, _, err := e.pdClient.GetRegionByID(ctx, leftID)
	if err != nil {
		return nil, fmt.Errorf("get left region %d: %w", leftID, err)
	}
	if leftRegion == nil {
		return nil, fmt.Errorf("left region %d not found", leftID)
	}

	// Build the split report: left region keeps [startKey, splitKey), right gets [splitKey, endKey)
	origEndKey := leftRegion.GetEndKey()
	origEpoch := leftRegion.GetRegionEpoch()

	left := &metapb.Region{
		Id:          leftID,
		StartKey:    leftRegion.GetStartKey(),
		EndKey:      splitKey,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: origEpoch.GetConfVer(), Version: origEpoch.GetVersion() + 1},
		Peers:       leftRegion.GetPeers(),
	}
	right := &metapb.Region{
		Id:          rightID,
		StartKey:    splitKey,
		EndKey:      origEndKey,
		RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
		Peers:       leftRegion.GetPeers(), // inherit peers from parent
	}

	if err := e.pdClient.ReportBatchSplit(ctx, []*metapb.Region{left, right}); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK, Message: "Split reported"}, nil
}

func (e *Executor) execStoreHeartbeat(ctx context.Context, cmd Command) (*Result, error) {
	storeID := uint64(cmd.IntArg)
	var regionCount uint32
	if len(cmd.Args) > 0 {
		n, _ := strconv.ParseUint(string(cmd.Args[0]), 10, 32)
		regionCount = uint32(n)
	}
	stats := &pdpb.StoreStats{
		StoreId:     storeID,
		RegionCount: regionCount,
	}
	if err := e.pdClient.StoreHeartbeat(ctx, stats); err != nil {
		return nil, err
	}
	return &Result{Type: ResultOK}, nil
}

// ---------------------------------------------------------------------------
// Meta handlers
// ---------------------------------------------------------------------------

func (e *Executor) execHelp(_ context.Context, _ Command) (*Result, error) {
	return &Result{Type: ResultMessage, Message: helpText}, nil
}

func (e *Executor) execExit(ctx context.Context, _ Command) (*Result, error) {
	if e.activeTxn != nil {
		fmt.Fprintln(defaultStderr, "WARNING: Active transaction will be rolled back.")
		_ = e.activeTxn.Rollback(ctx)
		e.activeTxn = nil
	}
	e.wantExit = true
	return &Result{Type: ResultMessage, Message: "Goodbye."}, nil
}

// defaultStderr is used for warnings (overridable in tests).
var defaultStderr io.Writer

const helpText = `Raw KV Commands:
  GET <key>                          Get a value
  PUT <key> <value> [TTL <sec>]      Put a value (optional TTL)
  DELETE <key>                       Delete a key
  SCAN <start> <end> [LIMIT <n>]     Scan a key range
  BGET <k1> <k2> ...                 Batch get
  BPUT <k1> <v1> <k2> <v2> ...       Batch put
  BDELETE <k1> <k2> ...              Batch delete
  DELETE RANGE <start> <end>          Delete a key range
  TTL <key>                           Get remaining TTL
  CAS <key> <new> <old> [NOT_EXIST]   Compare and swap
  CHECKSUM [<start> <end>]            Compute range checksum

Transaction Commands:
  BEGIN [PESSIMISTIC] [ASYNC_COMMIT] [ONE_PC] [LOCK_TTL <ms>]
  SET <key> <value>                   Set within transaction
  GET <key>                           Get within transaction
  DELETE <key>                        Delete within transaction
  BGET <k1> <k2> ...                 Batch get within transaction
  COMMIT                              Commit transaction
  ROLLBACK                            Rollback transaction

Admin Commands:
  STORE LIST                          List all stores
  STORE STATUS <id>                   Show store details
  STORE HEARTBEAT <id> [REGIONS <n>]  Send store heartbeat
  REGION <key>                        Find region for key
  REGION LIST [LIMIT <n>]             List all regions
  REGION ID <id>                      Find region by ID
  CLUSTER INFO                        Show cluster overview
  TSO                                 Allocate a timestamp
  GC SAFEPOINT                        Show GC safe point
  GC SAFEPOINT SET <ts>               Update GC safe point
  STATUS [<addr>]                     Query server status

PD Admin Commands:
  BOOTSTRAP <storeID> <addr> [<rid>]  Bootstrap cluster
  PUT STORE <id> <addr>               Register store
  ALLOC ID                            Allocate unique ID
  IS BOOTSTRAPPED                     Check bootstrap status
  ASK SPLIT <regionID> <count>        Request split IDs
  REPORT SPLIT <left> <right> <key>   Report completed split

Meta Commands:
  \help, \h, \?                       Show this help
  \quit, \q                           Exit the REPL
  \timing on|off, \t                  Toggle timing display
  \format table|plain|hex             Set output format
  \pagesize <n>                       Set default SCAN limit
  \x                                  Cycle display mode (auto/hex/string)
  HELP;  EXIT;  QUIT;                 Keyword aliases (require ;)

Keys: Use 0x... for hex, "..." for quoted strings, or bare words.`
