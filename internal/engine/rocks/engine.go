// Package rocks implements the KvEngine interface using Pebble as the storage backend.
// While named "rocks" for consistency with TiKV's architecture, it uses Pebble
// (a pure Go RocksDB-compatible engine by CockroachDB) to avoid CGo build dependencies.
// Column families are emulated via key prefixing: each key is prefixed with a single
// byte identifying the column family.
package rocks

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
)

// Column family prefix bytes. These are used to partition the single Pebble keyspace
// into logical column families. The prefix byte is prepended to every key.
var cfPrefixMap = map[string]byte{
	cfnames.CFDefault: 0x00,
	cfnames.CFLock:    0x01,
	cfnames.CFWrite:   0x02,
	cfnames.CFRaft:    0x03,
}

// cfPrefix returns the prefix byte for the given column family name.
func cfPrefix(cf string) (byte, error) {
	p, ok := cfPrefixMap[cf]
	if !ok {
		return 0, fmt.Errorf("%w: %s", traits.ErrCFNotFound, cf)
	}
	return p, nil
}

// prefixKey prepends the CF prefix byte to a user key.
func prefixKey(prefix byte, key []byte) []byte {
	out := make([]byte, 1+len(key))
	out[0] = prefix
	copy(out[1:], key)
	return out
}

// stripPrefix removes the CF prefix byte from an internal key, returning the user key.
func stripPrefix(key []byte) []byte {
	if len(key) <= 1 {
		return nil
	}
	out := make([]byte, len(key)-1)
	copy(out, key[1:])
	return out
}

// cfUpperBound returns the exclusive upper bound for a column family prefix.
// This is prefix+1, used to bound iterators to a single CF.
func cfUpperBound(prefix byte) []byte {
	return []byte{prefix + 1}
}

// Engine implements traits.KvEngine using Pebble.
type Engine struct {
	db   *pebble.DB
	path string
}

// Ensure Engine implements KvEngine at compile time.
var _ traits.KvEngine = (*Engine)(nil)

// Open creates a new Engine at the given path. If the directory does not exist,
// it will be created.
func Open(path string) (*Engine, error) {
	opts := &pebble.Options{
		// Use default options for now. Can be tuned later.
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("rocks: open %s: %w", path, err)
	}
	return &Engine{db: db, path: path}, nil
}

// OpenWithOptions creates a new Engine with custom Pebble options.
func OpenWithOptions(path string, opts *pebble.Options) (*Engine, error) {
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("rocks: open %s: %w", path, err)
	}
	return &Engine{db: db, path: path}, nil
}

func (e *Engine) Get(cf string, key []byte) ([]byte, error) {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return nil, err
	}
	val, closer, err := e.db.Get(prefixKey(prefix, key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, traits.ErrNotFound
		}
		return nil, fmt.Errorf("rocks: get: %w", err)
	}
	defer closer.Close()
	// Copy value since closer will invalidate it.
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (e *Engine) GetMsg(cf string, key []byte, msg interface{ Unmarshal([]byte) error }) error {
	val, err := e.Get(cf, key)
	if err != nil {
		return err
	}
	return msg.Unmarshal(val)
}

func (e *Engine) Put(cf string, key, value []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	return e.db.Set(prefixKey(prefix, key), value, pebble.Sync)
}

func (e *Engine) PutMsg(cf string, key []byte, msg interface{ Marshal() ([]byte, error) }) error {
	val, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("rocks: marshal: %w", err)
	}
	return e.Put(cf, key, val)
}

func (e *Engine) Delete(cf string, key []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	return e.db.Delete(prefixKey(prefix, key), pebble.Sync)
}

func (e *Engine) DeleteRange(cf string, startKey, endKey []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	return e.db.DeleteRange(prefixKey(prefix, startKey), prefixKey(prefix, endKey), pebble.Sync)
}

func (e *Engine) NewSnapshot() traits.Snapshot {
	return &snapshot{
		snap: e.db.NewSnapshot(),
	}
}

func (e *Engine) NewWriteBatch() traits.WriteBatch {
	return &writeBatch{
		batch: e.db.NewBatch(),
		db:    e.db,
	}
}

func (e *Engine) NewIterator(cf string, opts traits.IterOptions) traits.Iterator {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return &errorIterator{err: err}
	}

	iterOpts := &pebble.IterOptions{}

	// Set bounds within the CF prefix range.
	if opts.LowerBound != nil {
		iterOpts.LowerBound = prefixKey(prefix, opts.LowerBound)
	} else {
		iterOpts.LowerBound = []byte{prefix}
	}

	if opts.UpperBound != nil {
		iterOpts.UpperBound = prefixKey(prefix, opts.UpperBound)
	} else {
		iterOpts.UpperBound = cfUpperBound(prefix)
	}

	iter, iterErr := e.db.NewIter(iterOpts)
	if iterErr != nil {
		return &errorIterator{err: iterErr}
	}
	return &iterator{
		iter:   iter,
		prefix: prefix,
	}
}

func (e *Engine) SyncWAL() error {
	return e.db.Flush()
}

func (e *Engine) GetProperty(cf string, name string) (string, error) {
	// Pebble has limited property support. Return metrics as a string.
	metrics := e.db.Metrics()
	return fmt.Sprintf("%v", metrics), nil
}

// Compact compacts the specified key range.
func (e *Engine) Compact(start, end []byte) error {
	return e.db.Compact(start, end, true)
}

// CompactCF compacts a specific column family's key range.
func (e *Engine) CompactCF(cf string) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	start := []byte{prefix}
	end := cfUpperBound(prefix)
	return e.db.Compact(start, end, true)
}

// CompactAll compacts the entire database across all CFs.
func (e *Engine) CompactAll() error {
	// Compact each CF individually since Pebble requires start < end.
	for _, cf := range []string{"default", "lock", "write", "raft"} {
		if err := e.CompactCF(cf); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Close() error {
	return e.db.Close()
}

// snapshot implements traits.Snapshot.
type snapshot struct {
	snap *pebble.Snapshot
}

var _ traits.Snapshot = (*snapshot)(nil)

func (s *snapshot) Get(cf string, key []byte) ([]byte, error) {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return nil, err
	}
	val, closer, err := s.snap.Get(prefixKey(prefix, key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, traits.ErrNotFound
		}
		return nil, fmt.Errorf("rocks: snapshot get: %w", err)
	}
	defer closer.Close()
	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (s *snapshot) GetMsg(cf string, key []byte, msg interface{ Unmarshal([]byte) error }) error {
	val, err := s.Get(cf, key)
	if err != nil {
		return err
	}
	return msg.Unmarshal(val)
}

func (s *snapshot) NewIterator(cf string, opts traits.IterOptions) traits.Iterator {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return &errorIterator{err: err}
	}

	iterOpts := &pebble.IterOptions{}
	if opts.LowerBound != nil {
		iterOpts.LowerBound = prefixKey(prefix, opts.LowerBound)
	} else {
		iterOpts.LowerBound = []byte{prefix}
	}
	if opts.UpperBound != nil {
		iterOpts.UpperBound = prefixKey(prefix, opts.UpperBound)
	} else {
		iterOpts.UpperBound = cfUpperBound(prefix)
	}

	iter, iterErr := s.snap.NewIter(iterOpts)
	if iterErr != nil {
		return &errorIterator{err: iterErr}
	}
	return &iterator{
		iter:   iter,
		prefix: prefix,
	}
}

func (s *snapshot) Close() {
	s.snap.Close()
}

// savePointState holds the batch state snapshot at a save point.
type savePointState struct {
	repr     []byte // serialized batch state via Batch.Repr()
	count    int    // operation count at save point
	dataSize int    // data size at save point
}

// writeBatch implements traits.WriteBatch.
type writeBatch struct {
	batch      *pebble.Batch
	db         *pebble.DB // needed to create new batch on rollback
	count      int
	dataSize   int
	savePoints []savePointState
	mu         sync.Mutex
}

var _ traits.WriteBatch = (*writeBatch)(nil)

func (wb *writeBatch) Put(cf string, key, value []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if err := wb.batch.Set(prefixKey(prefix, key), value, nil); err != nil {
		return err
	}
	wb.count++
	wb.dataSize += len(key) + len(value) + 1 // +1 for prefix
	return nil
}

func (wb *writeBatch) PutMsg(cf string, key []byte, msg interface{ Marshal() ([]byte, error) }) error {
	val, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("rocks: marshal: %w", err)
	}
	return wb.Put(cf, key, val)
}

func (wb *writeBatch) Delete(cf string, key []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if err := wb.batch.Delete(prefixKey(prefix, key), nil); err != nil {
		return err
	}
	wb.count++
	wb.dataSize += len(key) + 1
	return nil
}

func (wb *writeBatch) DeleteRange(cf string, startKey, endKey []byte) error {
	prefix, err := cfPrefix(cf)
	if err != nil {
		return err
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if err := wb.batch.DeleteRange(prefixKey(prefix, startKey), prefixKey(prefix, endKey), nil); err != nil {
		return err
	}
	wb.count++
	wb.dataSize += len(startKey) + len(endKey) + 2
	return nil
}

func (wb *writeBatch) Count() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.count
}

func (wb *writeBatch) DataSize() int {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	return wb.dataSize
}

func (wb *writeBatch) Clear() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.batch.Reset()
	wb.count = 0
	wb.dataSize = 0
	wb.savePoints = nil
}

func (wb *writeBatch) SetSavePoint() {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	repr := wb.batch.Repr()
	snapshot := make([]byte, len(repr))
	copy(snapshot, repr)
	wb.savePoints = append(wb.savePoints, savePointState{
		repr:     snapshot,
		count:    wb.count,
		dataSize: wb.dataSize,
	})
}

func (wb *writeBatch) RollbackToSavePoint() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.savePoints) == 0 {
		return fmt.Errorf("rocks: no save point set")
	}
	sp := wb.savePoints[len(wb.savePoints)-1]
	wb.savePoints = wb.savePoints[:len(wb.savePoints)-1]

	// Restore batch state from snapshot.
	newBatch := wb.db.NewBatch()
	if err := newBatch.SetRepr(sp.repr); err != nil {
		newBatch.Close()
		return fmt.Errorf("rocks: restore save point: %w", err)
	}
	wb.batch.Close()
	wb.batch = newBatch
	wb.count = sp.count
	wb.dataSize = sp.dataSize
	return nil
}

func (wb *writeBatch) PopSavePoint() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	if len(wb.savePoints) == 0 {
		return fmt.Errorf("rocks: no save point set")
	}
	wb.savePoints = wb.savePoints[:len(wb.savePoints)-1]
	return nil
}

func (wb *writeBatch) Commit() error {
	return wb.batch.Commit(pebble.Sync)
}

// iterator implements traits.Iterator.
type iterator struct {
	iter   *pebble.Iterator
	prefix byte
}

var _ traits.Iterator = (*iterator)(nil)

func (it *iterator) SeekToFirst() {
	it.iter.First()
}

func (it *iterator) SeekToLast() {
	it.iter.Last()
}

func (it *iterator) Seek(target []byte) {
	it.iter.SeekGE(prefixKey(it.prefix, target))
}

func (it *iterator) SeekForPrev(target []byte) {
	it.iter.SeekLT(prefixKey(it.prefix, target))
	// SeekForPrev should position at the last key <= target.
	// Pebble's SeekLT positions at the last key < target.
	// We need to check if the target key exists and position there if so.
	// Actually, let's use SeekGE then Prev to handle the <= case correctly.
	it.iter.SeekGE(prefixKey(it.prefix, target))
	if it.iter.Valid() && bytes.Equal(stripPrefix(it.iter.Key()), target) {
		// Positioned exactly at target, which is correct for <= target.
		return
	}
	// Key at SeekGE position is > target (or invalid), so go to previous.
	if it.iter.Valid() {
		it.iter.Prev()
	} else {
		// No key >= target, go to the last key.
		it.iter.Last()
	}
}

func (it *iterator) Next() {
	it.iter.Next()
}

func (it *iterator) Prev() {
	it.iter.Prev()
}

func (it *iterator) Valid() bool {
	return it.iter.Valid()
}

func (it *iterator) Key() []byte {
	return stripPrefix(it.iter.Key())
}

func (it *iterator) Value() []byte {
	val := it.iter.Value()
	out := make([]byte, len(val))
	copy(out, val)
	return out
}

func (it *iterator) Error() error {
	return it.iter.Error()
}

func (it *iterator) Close() {
	it.iter.Close()
}

// errorIterator is returned when an iterator cannot be created (e.g., invalid CF).
type errorIterator struct {
	err error
}

var _ traits.Iterator = (*errorIterator)(nil)

func (e *errorIterator) SeekToFirst()          {}
func (e *errorIterator) SeekToLast()           {}
func (e *errorIterator) Seek(target []byte)    {}
func (e *errorIterator) SeekForPrev([]byte)    {}
func (e *errorIterator) Next()                 {}
func (e *errorIterator) Prev()                 {}
func (e *errorIterator) Valid() bool           { return false }
func (e *errorIterator) Key() []byte           { return nil }
func (e *errorIterator) Value() []byte         { return nil }
func (e *errorIterator) Error() error          { return e.err }
func (e *errorIterator) Close()                {}
