// gookv-ctl is the admin CLI for gookv.
// It provides commands for inspecting regions, dumping data, and manual operations.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/sstable"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/internal/storage/mvcc"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/keys"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

const usage = `gookv-ctl - Admin CLI for gookv

Usage:
  gookv-ctl <command> [options]

Commands:
  scan        Scan keys in a column family
  get         Get a single key
  mvcc        Show MVCC info for a key
  region      Inspect region metadata
  compact     Trigger manual compaction
  dump        Dump key-value pairs (with optional MVCC decoding)
  size        Show approximate data size
  store       Inspect store metadata via PD

Global Options:
  --db <path>   Path to the data directory (required for local commands)
  --cf <name>   Column family (default, lock, write, raft)
`

func main() {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "scan":
		cmdScan(args)
	case "get":
		cmdGet(args)
	case "mvcc":
		cmdMvcc(args)
	case "region":
		cmdRegion(args)
	case "dump":
		cmdDump(args)
	case "size":
		cmdSize(args)
	case "compact":
		cmdCompact(args)
	case "store":
		cmdStore(args)
	case "help", "--help", "-h":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		fmt.Print(usage)
		os.Exit(1)
	}
}

func openDB(path string) traits.KvEngine {
	eng, err := rocks.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database at %s: %v\n", path, err)
		os.Exit(1)
	}
	return eng
}

func openDBRocks(path string) *rocks.Engine {
	eng, err := rocks.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database at %s: %v\n", path, err)
		os.Exit(1)
	}
	return eng
}

func cmdScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	cf := fs.String("cf", cfnames.CFDefault, "Column family")
	startKey := fs.String("start", "", "Start key (hex)")
	endKey := fs.String("end", "", "End key (hex)")
	limit := fs.Int("limit", 100, "Maximum keys to scan")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	var start, end []byte
	if *startKey != "" {
		var err error
		start, err = hex.DecodeString(*startKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid hex start key: %v\n", err)
			os.Exit(1)
		}
	}
	if *endKey != "" {
		var err error
		end, err = hex.DecodeString(*endKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid hex end key: %v\n", err)
			os.Exit(1)
		}
	}

	iter := eng.NewIterator(*cf, traits.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < *limit; iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("Key:   %s\n", hex.EncodeToString(key))
		fmt.Printf("Value: %s (%d bytes)\n", tryPrintable(value), len(value))
		fmt.Println("---")
		count++
	}

	fmt.Printf("\nTotal: %d keys\n", count)
}

func cmdGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	cf := fs.String("cf", cfnames.CFDefault, "Column family")
	keyHex := fs.String("key", "", "Key (hex)")
	fs.Parse(args)

	if *dbPath == "" || *keyHex == "" {
		fmt.Fprintln(os.Stderr, "Error: --db and --key are required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	key, err := hex.DecodeString(*keyHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid hex key: %v\n", err)
		os.Exit(1)
	}

	value, err := eng.Get(*cf, key)
	if err != nil {
		if err == traits.ErrNotFound {
			fmt.Println("Key not found")
			return
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("CF:    %s\n", *cf)
	fmt.Printf("Key:   %s\n", hex.EncodeToString(key))
	fmt.Printf("Value: %s (%d bytes)\n", tryPrintable(value), len(value))
}

func cmdMvcc(args []string) {
	fs := flag.NewFlagSet("mvcc", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	keyHex := fs.String("key", "", "User key (hex)")
	fs.Parse(args)

	if *dbPath == "" || *keyHex == "" {
		fmt.Fprintln(os.Stderr, "Error: --db and --key are required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	userKey, err := hex.DecodeString(*keyHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid hex key: %v\n", err)
		os.Exit(1)
	}

	snap := eng.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	info := MvccInfo{
		Key: hex.EncodeToString(userKey),
	}

	// Check for lock.
	lock, err := reader.LoadLock(userKey)
	if err == nil && lock != nil {
		info.Lock = &LockInfo{
			LockType:    string(lock.LockType),
			Primary:     hex.EncodeToString(lock.Primary),
			StartTS:     uint64(lock.StartTS),
			TTL:         lock.TTL,
			ForUpdateTS: uint64(lock.ForUpdateTS),
			MinCommitTS: uint64(lock.MinCommitTS),
			AsyncCommit: lock.UseAsyncCommit,
		}
		if lock.ShortValue != nil {
			info.Lock.ShortValue = tryPrintable(lock.ShortValue)
		}
	}

	// Scan writes.
	writeIter := snap.NewIterator(cfnames.CFWrite, traits.IterOptions{})
	defer writeIter.Close()

	seekKey := mvcc.EncodeKey(userKey, txntypes.TSMax)
	for writeIter.Seek(seekKey); writeIter.Valid(); writeIter.Next() {
		uk, ts, err := mvcc.DecodeKey(writeIter.Key())
		if err != nil || string(uk) != string(userKey) {
			break
		}
		writeData := writeIter.Value()
		write, err := txntypes.UnmarshalWrite(writeData)
		if err != nil {
			continue
		}

		wi := WriteInfo{
			CommitTS:  uint64(ts),
			StartTS:   uint64(write.StartTS),
			WriteType: writeTypeStr(write.WriteType),
		}
		if write.ShortValue != nil {
			wi.ShortValue = tryPrintable(write.ShortValue)
		}
		info.Writes = append(info.Writes, wi)

		if len(info.Writes) >= 10 {
			break
		}
	}

	// Output as JSON.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(info)
}

// --- region command ---

func cmdRegion(args []string) {
	fs := flag.NewFlagSet("region", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	regionID := fs.Uint64("id", 0, "Region ID to inspect")
	all := fs.Bool("all", false, "List all regions")
	limit := fs.Int("limit", 100, "Maximum regions to display")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	if *regionID != 0 {
		// Lookup specific region.
		key := keys.RegionStateKey(*regionID)
		data, err := eng.Get(cfnames.CFRaft, key)
		if err != nil {
			if err == traits.ErrNotFound {
				fmt.Printf("Region %d not found\n", *regionID)
				return
			}
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		state := &raft_serverpb.RegionLocalState{}
		if err := state.Unmarshal(data); err != nil {
			fmt.Fprintf(os.Stderr, "Error decoding region state: %v\n", err)
			os.Exit(1)
		}
		printRegionState(*regionID, state)
		return
	}

	// Default: list all regions (or when --all is set).
	_ = *all
	iter := eng.NewIterator(cfnames.CFRaft, traits.IterOptions{})
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < *limit; iter.Next() {
		rawKey := iter.Key()
		if !isRegionStateKey(rawKey) {
			continue
		}
		rid, err := keys.RegionIDFromMetaKey(rawKey)
		if err != nil {
			continue
		}
		state := &raft_serverpb.RegionLocalState{}
		if err := state.Unmarshal(iter.Value()); err != nil {
			fmt.Fprintf(os.Stderr, "[DECODE ERROR] Region %d: %v\n", rid, err)
			continue
		}
		printRegionState(rid, state)
		fmt.Println("---")
		count++
	}
	fmt.Printf("Total: %d regions\n", count)
}

func isRegionStateKey(key []byte) bool {
	// RegionStateKey format: [0x01][0x03][8 bytes regionID][0x01]
	return len(key) == 11 &&
		key[0] == keys.LocalPrefix &&
		key[1] == keys.RegionMetaPrefix &&
		key[10] == keys.RegionStateSuffix
}

func printRegionState(id uint64, state *raft_serverpb.RegionLocalState) {
	fmt.Printf("Region ID: %d\n", id)
	fmt.Printf("  State:     %s\n", state.GetState().String())
	if region := state.GetRegion(); region != nil {
		fmt.Printf("  StartKey:  %s\n", displayKey(region.GetStartKey()))
		fmt.Printf("  EndKey:    %s\n", displayKey(region.GetEndKey()))
		fmt.Printf("  Peers:     %v\n", region.GetPeers())
		if epoch := region.GetRegionEpoch(); epoch != nil {
			fmt.Printf("  Epoch:     conf_ver:%d version:%d\n", epoch.GetConfVer(), epoch.GetVersion())
		}
	}
}

func displayKey(key []byte) string {
	if len(key) == 0 {
		return "(empty)"
	}
	return hex.EncodeToString(key)
}

// --- dump command (enhanced with --decode) ---

func cmdDump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	sstPath := fs.String("sst", "", "Path to SST file (direct SST parsing, --db not required)")
	cf := fs.String("cf", cfnames.CFDefault, "Column family")
	limit := fs.Int("limit", 50, "Maximum entries")
	decode := fs.Bool("decode", false, "Decode MVCC keys and record values")
	startHex := fs.String("start", "", "Start key in hex (inclusive)")
	endHex := fs.String("end", "", "End key in hex (exclusive)")
	fs.Parse(args)

	if *sstPath != "" {
		cmdDumpSST(*sstPath, *cf, *limit, *decode)
		return
	}

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db or --sst is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	var start, end []byte
	if *startHex != "" {
		var err error
		start, err = hex.DecodeString(*startHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid hex start key: %v\n", err)
			os.Exit(1)
		}
	}
	if *endHex != "" {
		var err error
		end, err = hex.DecodeString(*endHex)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid hex end key: %v\n", err)
			os.Exit(1)
		}
	}

	iter := eng.NewIterator(*cf, traits.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < *limit; iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if !*decode {
			fmt.Printf("%s\t%s\n", hex.EncodeToString(key), hex.EncodeToString(value))
		} else {
			dumpDecoded(*cf, key, value)
		}
		count++
	}
}

// cmdDumpSST reads an SST file directly using Pebble's sstable package.
func cmdDumpSST(path string, cf string, limit int, decode bool) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening SST file: %v\n", err)
		os.Exit(1)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		fmt.Fprintf(os.Stderr, "Error creating readable: %v\n", err)
		os.Exit(1)
	}
	reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening SST reader: %v\n", err)
		os.Exit(1)
	}
	defer reader.Close()

	// Print SST metadata.
	props := reader.Properties
	fmt.Printf("SST File: %s\n", path)
	fmt.Printf("  Entries:     %d\n", props.NumEntries)
	fmt.Printf("  Data Size:   %d bytes\n", props.DataSize)
	fmt.Printf("  Index Size:  %d bytes\n", props.IndexSize)
	fmt.Printf("  Compression: %s\n", props.CompressionName)
	fmt.Println("---")

	iter, err := reader.NewIter(nil, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating iterator: %v\n", err)
		os.Exit(1)
	}
	defer iter.Close()

	count := 0
	ikey, val := iter.First()
	for ikey != nil && count < limit {
		key := append([]byte{}, ikey.UserKey...)
		value, _, err := val.Value(nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading value: %v\n", err)
			ikey, val = iter.Next()
			continue
		}
		valueCopy := append([]byte{}, value...)
		if !decode {
			fmt.Printf("%s\t%s\n", hex.EncodeToString(key), hex.EncodeToString(valueCopy))
		} else {
			dumpDecoded(cf, key, valueCopy)
		}
		count++
		ikey, val = iter.Next()
	}
	fmt.Printf("--- %d entries dumped\n", count)
}

func dumpDecoded(cf string, key, value []byte) {
	switch cf {
	case cfnames.CFWrite:
		dumpWriteCF(key, value)
	case cfnames.CFLock:
		dumpLockCF(key, value)
	case cfnames.CFDefault:
		dumpDefaultCF(key, value)
	case cfnames.CFRaft:
		dumpRaftCF(key, value)
	default:
		fmt.Printf("%s\t%s\n", hex.EncodeToString(key), hex.EncodeToString(value))
	}
}

func dumpWriteCF(key, value []byte) {
	userKey, commitTS, err := mvcc.DecodeKey(key)
	if err != nil {
		fmt.Printf("[DECODE ERROR] key=%s err=%v\n", hex.EncodeToString(key), err)
		return
	}
	write, err := txntypes.UnmarshalWrite(value)
	if err != nil {
		fmt.Printf("[DECODE ERROR] key=%s value_err=%v\n", hex.EncodeToString(key), err)
		return
	}
	fmt.Printf("[MVCC Write] UserKey: %s  CommitTS: %d\n", hex.EncodeToString(userKey), commitTS)
	sv := ""
	if write.ShortValue != nil {
		sv = fmt.Sprintf("  ShortValue: %q", tryPrintable(write.ShortValue))
	}
	fmt.Printf("  WriteType: %s  StartTS: %d%s\n", writeTypeStr(write.WriteType), write.StartTS, sv)
	fmt.Println("---")
}

func dumpLockCF(key, value []byte) {
	userKey, err := mvcc.DecodeLockKey(key)
	if err != nil {
		fmt.Printf("[DECODE ERROR] key=%s err=%v\n", hex.EncodeToString(key), err)
		return
	}
	lock, err := txntypes.UnmarshalLock(value)
	if err != nil {
		fmt.Printf("[DECODE ERROR] key=%s value_err=%v\n", hex.EncodeToString(key), err)
		return
	}
	fmt.Printf("[MVCC Lock] UserKey: %s\n", hex.EncodeToString(userKey))
	fmt.Printf("  LockType: %s  Primary: %s  StartTS: %d  TTL: %d\n",
		string(lock.LockType), hex.EncodeToString(lock.Primary), lock.StartTS, lock.TTL)
	fmt.Println("---")
}

func dumpDefaultCF(key, value []byte) {
	userKey, startTS, err := mvcc.DecodeKey(key)
	if err != nil {
		fmt.Printf("[DECODE ERROR] key=%s err=%v\n", hex.EncodeToString(key), err)
		return
	}
	fmt.Printf("[MVCC Default] UserKey: %s  StartTS: %d\n", hex.EncodeToString(userKey), startTS)
	fmt.Printf("  Value: %s (%d bytes)\n", hex.EncodeToString(value), len(value))
	fmt.Println("---")
}

func dumpRaftCF(key, value []byte) {
	if isRegionStateKey(key) {
		rid, _ := keys.RegionIDFromMetaKey(key)
		fmt.Printf("[Raft RegionState] RegionID: %d\n", rid)
		state := &raft_serverpb.RegionLocalState{}
		if err := state.Unmarshal(value); err == nil {
			fmt.Printf("  State: %s\n", state.GetState().String())
		}
		fmt.Println("---")
		return
	}
	if len(key) >= 11 && key[0] == keys.LocalPrefix && key[1] == keys.RegionRaftPrefix {
		rid := binary.BigEndian.Uint64(key[2:10])
		suffix := key[10]
		switch suffix {
		case keys.RaftLogSuffix:
			if len(key) >= 19 {
				logIdx := binary.BigEndian.Uint64(key[11:19])
				fmt.Printf("[Raft Log] RegionID: %d  LogIndex: %d\n", rid, logIdx)
			} else {
				fmt.Printf("[Raft Log] RegionID: %d\n", rid)
			}
		case keys.RaftStateSuffix:
			fmt.Printf("[Raft HardState] RegionID: %d\n", rid)
		case keys.ApplyStateSuffix:
			fmt.Printf("[Raft ApplyState] RegionID: %d\n", rid)
		default:
			fmt.Printf("[Raft Unknown] RegionID: %d  Suffix: %02x\n", rid, suffix)
		}
		fmt.Println("---")
		return
	}
	// Fallback to raw hex.
	fmt.Printf("[Raft] %s\t%s\n", hex.EncodeToString(key), hex.EncodeToString(value))
	fmt.Println("---")
}

// --- compact command (fixed with actual Pebble compaction) ---

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	cf := fs.String("cf", "", "Column family to compact (empty = all)")
	flushOnly := fs.Bool("flush-only", false, "Only flush WAL/memtable (old behavior)")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDBRocks(*dbPath)
	defer eng.Close()

	if *flushOnly {
		if err := eng.SyncWAL(); err != nil {
			fmt.Fprintf(os.Stderr, "Error flushing WAL: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("WAL flushed successfully.")
		return
	}

	if *cf != "" {
		fmt.Printf("Compacting CF %s...\n", *cf)
		if err := eng.CompactCF(*cf); err != nil {
			fmt.Fprintf(os.Stderr, "Error compacting CF %s: %v\n", *cf, err)
			os.Exit(1)
		}
	} else {
		fmt.Println("Compacting all column families...")
		if err := eng.CompactAll(); err != nil {
			fmt.Fprintf(os.Stderr, "Error compacting: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("Compaction completed successfully.")
}

func cmdSize(args []string) {
	fs := flag.NewFlagSet("size", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	cfs := []string{cfnames.CFDefault, cfnames.CFLock, cfnames.CFWrite, cfnames.CFRaft}
	for _, cf := range cfs {
		iter := eng.NewIterator(cf, traits.IterOptions{})
		count := 0
		totalSize := int64(0)
		for iter.SeekToFirst(); iter.Valid(); iter.Next() {
			count++
			totalSize += int64(len(iter.Key()) + len(iter.Value()))
		}
		iter.Close()
		fmt.Printf("CF %-10s: %6d keys, %s\n", cf, count, formatSize(totalSize))
	}
}

// --- Helper types ---

// MvccInfo holds MVCC information for display.
type MvccInfo struct {
	Key    string      `json:"key"`
	Lock   *LockInfo   `json:"lock,omitempty"`
	Writes []WriteInfo `json:"writes,omitempty"`
}

// LockInfo holds lock information for display.
type LockInfo struct {
	LockType    string `json:"lock_type"`
	Primary     string `json:"primary"`
	StartTS     uint64 `json:"start_ts"`
	TTL         uint64 `json:"ttl"`
	ForUpdateTS uint64 `json:"for_update_ts,omitempty"`
	MinCommitTS uint64 `json:"min_commit_ts,omitempty"`
	ShortValue  string `json:"short_value,omitempty"`
	AsyncCommit bool   `json:"async_commit,omitempty"`
}

// WriteInfo holds write record information for display.
type WriteInfo struct {
	CommitTS   uint64 `json:"commit_ts"`
	StartTS    uint64 `json:"start_ts"`
	WriteType  string `json:"write_type"`
	ShortValue string `json:"short_value,omitempty"`
}

func writeTypeStr(wt txntypes.WriteType) string {
	switch wt {
	case txntypes.WriteTypePut:
		return "Put"
	case txntypes.WriteTypeDelete:
		return "Delete"
	case txntypes.WriteTypeLock:
		return "Lock"
	case txntypes.WriteTypeRollback:
		return "Rollback"
	default:
		return fmt.Sprintf("Unknown(%d)", wt)
	}
}

func tryPrintable(data []byte) string {
	for _, b := range data {
		if b < 0x20 || b > 0x7E {
			return hex.EncodeToString(data)
		}
	}
	return string(data)
}

func formatSize(bytes int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	switch {
	case bytes >= gb:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// RunCommand is exported for testing purposes.
func RunCommand(args []string) int {
	if len(args) < 1 {
		return 1
	}

	cmd := args[0]
	cmdArgs := args[1:]

	switch cmd {
	case "scan":
		cmdScan(cmdArgs)
	case "get":
		cmdGet(cmdArgs)
	case "mvcc":
		cmdMvcc(cmdArgs)
	case "region":
		cmdRegion(cmdArgs)
	case "dump":
		cmdDump(cmdArgs)
	case "size":
		cmdSize(cmdArgs)
	case "compact":
		cmdCompact(cmdArgs)
	case "store":
		cmdStore(cmdArgs)
	case "help":
		fmt.Print(usage)
	default:
		return 1
	}
	return 0
}

// --- store command ---

const storeUsage = `gookv-ctl store - Inspect store metadata via PD

Usage:
  gookv-ctl store list   --pd <addr>
  gookv-ctl store status --pd <addr> --store-id <id>

Subcommands:
  list     List all stores in the cluster
  status   Show details for a specific store
`

func cmdStore(args []string) {
	if len(args) < 1 {
		fmt.Print(storeUsage)
		os.Exit(1)
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "list":
		cmdStoreList(subArgs)
	case "status":
		cmdStoreStatus(subArgs)
	default:
		fmt.Fprintf(os.Stderr, "Unknown store subcommand: %s\n", sub)
		fmt.Print(storeUsage)
		os.Exit(1)
	}
}

func connectPD(addr string) pdclient.Client {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints:     []string{addr},
		RetryInterval: 500 * time.Millisecond,
		RetryMaxCount: 3,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to PD at %s: %v\n", addr, err)
		os.Exit(1)
	}
	return client
}

func cmdStoreList(args []string) {
	fs := flag.NewFlagSet("store list", flag.ExitOnError)
	pdAddr := fs.String("pd", "127.0.0.1:2379", "PD server address")
	fs.Parse(args)

	client := connectPD(*pdAddr)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stores, err := client.GetAllStores(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting stores: %v\n", err)
		os.Exit(1)
	}

	if len(stores) == 0 {
		fmt.Println("No stores found.")
		return
	}

	fmt.Printf("%-10s %-25s %-10s\n", "StoreID", "Address", "State")
	for _, s := range stores {
		fmt.Printf("%-10d %-25s %-10s\n", s.GetId(), s.GetAddress(), "Up")
	}
}

func cmdStoreStatus(args []string) {
	fs := flag.NewFlagSet("store status", flag.ExitOnError)
	pdAddr := fs.String("pd", "127.0.0.1:2379", "PD server address")
	storeID := fs.Uint64("store-id", 0, "Store ID to inspect")
	fs.Parse(args)

	if *storeID == 0 {
		fmt.Fprintln(os.Stderr, "Error: --store-id is required")
		os.Exit(1)
	}

	client := connectPD(*pdAddr)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := client.GetStore(ctx, *storeID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting store %d: %v\n", *storeID, err)
		os.Exit(1)
	}

	if store == nil {
		fmt.Printf("Store %d not found.\n", *storeID)
		return
	}

	fmt.Printf("Store ID:  %d\n", store.GetId())
	fmt.Printf("Address:   %s\n", store.GetAddress())
}

// ParseCommand parses a command string into a command name for testing.
func ParseCommand(input string) string {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}
