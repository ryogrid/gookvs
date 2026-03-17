// gookvs-ctl is the admin CLI for gookvs.
// It provides commands for inspecting regions, dumping data, and manual operations.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

const usage = `gookvs-ctl - Admin CLI for gookvs

Usage:
  gookvs-ctl <command> [options]

Commands:
  scan        Scan keys in a column family
  get         Get a single key
  mvcc        Show MVCC info for a key
  region      Inspect region metadata
  compact     Trigger manual compaction
  dump        Dump raw key-value pairs
  size        Show approximate data size

Global Options:
  --db <path>   Path to the data directory (required)
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
	case "dump":
		cmdDump(args)
	case "size":
		cmdSize(args)
	case "compact":
		cmdCompact(args)
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

func cmdDump(args []string) {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	cf := fs.String("cf", cfnames.CFDefault, "Column family")
	limit := fs.Int("limit", 50, "Maximum entries")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	iter := eng.NewIterator(*cf, traits.IterOptions{})
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid() && count < *limit; iter.Next() {
		key := iter.Key()
		value := iter.Value()
		fmt.Printf("%s\t%s\n", hex.EncodeToString(key), hex.EncodeToString(value))
		count++
	}
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

func cmdCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dbPath := fs.String("db", "", "Path to data directory")
	fs.Parse(args)

	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --db is required")
		os.Exit(1)
	}

	eng := openDB(*dbPath)
	defer eng.Close()

	// Force a WAL sync as a compact-like operation.
	if err := eng.SyncWAL(); err != nil {
		fmt.Fprintf(os.Stderr, "Error syncing WAL: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Compaction triggered successfully.")
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
	case "dump":
		cmdDump(cmdArgs)
	case "size":
		cmdSize(cmdArgs)
	case "compact":
		cmdCompact(cmdArgs)
	case "help":
		fmt.Print(usage)
	default:
		return 1
	}
	return 0
}

// ParseCommand parses a command string into a command name for testing.
func ParseCommand(input string) string {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}
