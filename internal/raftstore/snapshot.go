package raftstore

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/metapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/keys"
)

// SnapState represents the state machine for snapshot generation.
type SnapState int

const (
	SnapStateRelax      SnapState = iota // No snapshot activity
	SnapStateGenerating                  // Async generation in progress
	SnapStateApplying                    // Being applied by this peer
)

// maxSnapTryCnt is the maximum retry count for snapshot generation.
const maxSnapTryCnt = 5

// SnapshotVersion is the snapshot format version.
const SnapshotVersion = 1

// SnapKey uniquely identifies a snapshot.
type SnapKey struct {
	RegionID uint64
	Term     uint64
	Index    uint64
}

// GenSnapTask is the request sent to the snapshot generation worker.
type GenSnapTask struct {
	RegionID uint64
	Region   *metapb.Region
	SnapKey  SnapKey
	Canceled *atomic.Bool
	ResultCh chan<- GenSnapResult
}

// GenSnapResult is the outcome of snapshot generation.
type GenSnapResult struct {
	Snapshot raftpb.Snapshot
	Err      error
}

// SnapshotData holds the serialized snapshot payload.
type SnapshotData struct {
	RegionID uint64
	Version  uint64
	CFFiles  []SnapshotCFFile
}

// SnapshotCFFile represents one column family's data in the snapshot.
type SnapshotCFFile struct {
	CF       string
	KVPairs  []SnapKVPair
	Checksum uint32
}

// SnapKVPair is a key-value pair within a snapshot CF file.
type SnapKVPair struct {
	Key   []byte
	Value []byte
}

// MarshalSnapshotData serializes SnapshotData to bytes.
// Format: [8:regionID][8:version][4:cfCount]
//
//	for each CF: [2:cfNameLen][cfName][4:kvCount][4:checksum]
//	  for each KV: [4:keyLen][key][4:valueLen][value]
func MarshalSnapshotData(sd *SnapshotData) ([]byte, error) {
	var buf []byte

	b8 := make([]byte, 8)
	binary.BigEndian.PutUint64(b8, sd.RegionID)
	buf = append(buf, b8...)
	binary.BigEndian.PutUint64(b8, sd.Version)
	buf = append(buf, b8...)

	b4 := make([]byte, 4)
	binary.BigEndian.PutUint32(b4, uint32(len(sd.CFFiles)))
	buf = append(buf, b4...)

	for _, cf := range sd.CFFiles {
		// CF name.
		b2 := make([]byte, 2)
		binary.BigEndian.PutUint16(b2, uint16(len(cf.CF)))
		buf = append(buf, b2...)
		buf = append(buf, []byte(cf.CF)...)

		// KV count.
		binary.BigEndian.PutUint32(b4, uint32(len(cf.KVPairs)))
		buf = append(buf, b4...)

		// Checksum.
		binary.BigEndian.PutUint32(b4, cf.Checksum)
		buf = append(buf, b4...)

		// KV pairs.
		for _, kv := range cf.KVPairs {
			binary.BigEndian.PutUint32(b4, uint32(len(kv.Key)))
			buf = append(buf, b4...)
			buf = append(buf, kv.Key...)
			binary.BigEndian.PutUint32(b4, uint32(len(kv.Value)))
			buf = append(buf, b4...)
			buf = append(buf, kv.Value...)
		}
	}

	return buf, nil
}

// UnmarshalSnapshotData deserializes SnapshotData from bytes.
func UnmarshalSnapshotData(data []byte) (*SnapshotData, error) {
	if len(data) < 20 {
		return nil, fmt.Errorf("raftstore: snapshot data too short")
	}

	sd := &SnapshotData{}
	pos := 0

	sd.RegionID = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8
	sd.Version = binary.BigEndian.Uint64(data[pos : pos+8])
	pos += 8

	cfCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4

	for i := 0; i < cfCount; i++ {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("raftstore: truncated CF name length at cf %d", i)
		}
		cfNameLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
		pos += 2
		if pos+cfNameLen > len(data) {
			return nil, fmt.Errorf("raftstore: truncated CF name at cf %d", i)
		}
		cfName := string(data[pos : pos+cfNameLen])
		pos += cfNameLen

		if pos+8 > len(data) {
			return nil, fmt.Errorf("raftstore: truncated KV count at cf %d", i)
		}
		kvCount := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		pos += 4
		checksum := binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4

		cf := SnapshotCFFile{
			CF:       cfName,
			Checksum: checksum,
			KVPairs:  make([]SnapKVPair, 0, kvCount),
		}

		for j := 0; j < kvCount; j++ {
			if pos+4 > len(data) {
				return nil, fmt.Errorf("raftstore: truncated key length at cf %d kv %d", i, j)
			}
			keyLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+keyLen > len(data) {
				return nil, fmt.Errorf("raftstore: truncated key at cf %d kv %d", i, j)
			}
			key := make([]byte, keyLen)
			copy(key, data[pos:pos+keyLen])
			pos += keyLen

			if pos+4 > len(data) {
				return nil, fmt.Errorf("raftstore: truncated value length at cf %d kv %d", i, j)
			}
			valLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
			if pos+valLen > len(data) {
				return nil, fmt.Errorf("raftstore: truncated value at cf %d kv %d", i, j)
			}
			val := make([]byte, valLen)
			copy(val, data[pos:pos+valLen])
			pos += valLen

			cf.KVPairs = append(cf.KVPairs, SnapKVPair{Key: key, Value: val})
		}

		sd.CFFiles = append(sd.CFFiles, cf)
	}

	return sd, nil
}

// ComputeCFChecksum computes CRC32 checksum for a list of KV pairs.
func ComputeCFChecksum(pairs []SnapKVPair) uint32 {
	h := crc32.NewIEEE()
	for _, kv := range pairs {
		h.Write(kv.Key)
		h.Write(kv.Value)
	}
	return h.Sum32()
}

// GenerateSnapshotData creates snapshot data by scanning the engine for a region's key range.
// CFs included: CF_DEFAULT, CF_LOCK, CF_WRITE (CF_RAFT excluded).
func GenerateSnapshotData(engine traits.KvEngine, regionID uint64, startKey, endKey []byte) (*SnapshotData, error) {
	snap := engine.NewSnapshot()
	defer snap.Close()

	sd := &SnapshotData{
		RegionID: regionID,
		Version:  SnapshotVersion,
	}

	dataCFs := []string{cfnames.CFDefault, cfnames.CFLock, cfnames.CFWrite}
	for _, cf := range dataCFs {
		cfFile, err := scanCFForSnapshot(snap, cf, startKey, endKey)
		if err != nil {
			return nil, fmt.Errorf("raftstore: scan CF %s for snapshot: %w", cf, err)
		}
		sd.CFFiles = append(sd.CFFiles, *cfFile)
	}

	return sd, nil
}

func scanCFForSnapshot(snap traits.Snapshot, cf string, startKey, endKey []byte) (*SnapshotCFFile, error) {
	opts := traits.IterOptions{}
	if len(startKey) > 0 {
		opts.LowerBound = startKey
	}
	if len(endKey) > 0 {
		opts.UpperBound = endKey
	}

	iter := snap.NewIterator(cf, opts)
	defer iter.Close()

	cfFile := &SnapshotCFFile{CF: cf}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		val := make([]byte, len(iter.Value()))
		copy(val, iter.Value())
		cfFile.KVPairs = append(cfFile.KVPairs, SnapKVPair{Key: key, Value: val})
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	cfFile.Checksum = ComputeCFChecksum(cfFile.KVPairs)
	return cfFile, nil
}

// ApplySnapshotData writes snapshot data to the engine, clearing old data first.
func ApplySnapshotData(engine traits.KvEngine, sd *SnapshotData, startKey, endKey []byte) error {
	wb := engine.NewWriteBatch()

	// Clear existing data for the region key range in data CFs.
	dataCFs := []string{cfnames.CFDefault, cfnames.CFLock, cfnames.CFWrite}
	for _, cf := range dataCFs {
		if len(startKey) > 0 || len(endKey) > 0 {
			if err := wb.DeleteRange(cf, startKey, endKey); err != nil {
				return fmt.Errorf("raftstore: clear CF %s: %w", cf, err)
			}
		}
	}

	// Write KV pairs from snapshot.
	for _, cfFile := range sd.CFFiles {
		// Verify checksum.
		expected := ComputeCFChecksum(cfFile.KVPairs)
		if expected != cfFile.Checksum {
			return fmt.Errorf("raftstore: checksum mismatch for CF %s: expected %d got %d",
				cfFile.CF, cfFile.Checksum, expected)
		}

		for _, kv := range cfFile.KVPairs {
			if err := wb.Put(cfFile.CF, kv.Key, kv.Value); err != nil {
				return fmt.Errorf("raftstore: write KV to CF %s: %w", cfFile.CF, err)
			}
		}
	}

	return wb.Commit()
}

// SnapWorker runs as a background goroutine processing snapshot generation tasks.
type SnapWorker struct {
	engine traits.KvEngine
	taskCh <-chan GenSnapTask
	stopCh <-chan struct{}
}

// NewSnapWorker creates a new SnapWorker.
func NewSnapWorker(engine traits.KvEngine, taskCh <-chan GenSnapTask, stopCh <-chan struct{}) *SnapWorker {
	return &SnapWorker{
		engine: engine,
		taskCh: taskCh,
		stopCh: stopCh,
	}
}

// Run processes snapshot generation tasks until stopped.
func (w *SnapWorker) Run() {
	for {
		select {
		case <-w.stopCh:
			return
		case task, ok := <-w.taskCh:
			if !ok {
				return
			}
			result := w.generateSnapshot(task)
			if task.ResultCh != nil {
				select {
				case task.ResultCh <- result:
				default:
				}
			}
		}
	}
}

func (w *SnapWorker) generateSnapshot(task GenSnapTask) GenSnapResult {
	if task.Canceled != nil && task.Canceled.Load() {
		return GenSnapResult{Err: fmt.Errorf("raftstore: snapshot generation cancelled")}
	}

	var startKey, endKey []byte
	if task.Region != nil {
		startKey = task.Region.GetStartKey()
		endKey = task.Region.GetEndKey()
	}

	sd, err := GenerateSnapshotData(w.engine, task.RegionID, startKey, endKey)
	if err != nil {
		return GenSnapResult{Err: err}
	}

	data, err := MarshalSnapshotData(sd)
	if err != nil {
		return GenSnapResult{Err: err}
	}

	snap := raftpb.Snapshot{
		Data: data,
		Metadata: raftpb.SnapshotMetadata{
			Index: task.SnapKey.Index,
			Term:  task.SnapKey.Term,
		},
	}

	return GenSnapResult{Snapshot: snap}
}

// UpdatePeerStorageFromSnapshot updates PeerStorage state after applying a snapshot.
func (s *PeerStorage) ApplySnapshot(snap raftpb.Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapIdx := snap.Metadata.Index
	snapTerm := snap.Metadata.Term

	// Validate: snapshot must be newer than our truncated state.
	if snapIdx <= s.applyState.TruncatedIndex {
		return nil // Stale snapshot, skip.
	}

	// Deserialize and apply data.
	if len(snap.Data) > 0 {
		sd, err := UnmarshalSnapshotData(snap.Data)
		if err != nil {
			return fmt.Errorf("raftstore: unmarshal snapshot data: %w", err)
		}

		var snapStartKey, snapEndKey []byte
		if s.region != nil {
			snapStartKey = s.region.GetStartKey()
			snapEndKey = s.region.GetEndKey()
		}
		if err := ApplySnapshotData(s.engine, sd, snapStartKey, snapEndKey); err != nil {
			return fmt.Errorf("raftstore: apply snapshot data: %w", err)
		}
	}

	// Update persisted state atomically.
	s.applyState.AppliedIndex = snapIdx
	s.applyState.TruncatedIndex = snapIdx
	s.applyState.TruncatedTerm = snapTerm

	s.hardState.Commit = snapIdx

	// Persist hard state and apply state.
	wb := s.engine.NewWriteBatch()
	hsData, err := s.hardState.Marshal()
	if err != nil {
		return fmt.Errorf("raftstore: marshal hard state: %w", err)
	}
	if err := wb.Put(cfnames.CFRaft, keys.RaftStateKey(s.regionID), hsData); err != nil {
		return err
	}
	if err := wb.Commit(); err != nil {
		return fmt.Errorf("raftstore: persist snapshot state: %w", err)
	}

	// Update in-memory state.
	s.entries = nil
	s.persistedLastIndex = snapIdx
	s.snapState = SnapStateRelax

	return nil
}

// RequestSnapshot triggers async snapshot generation.
// Returns the snapshot if already generated, or ErrSnapshotTemporarilyUnavailable.
func (s *PeerStorage) RequestSnapshot(taskCh chan<- GenSnapTask, region *metapb.Region) (raftpb.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch s.snapState {
	case SnapStateRelax:
		if s.snapTriedCnt >= maxSnapTryCnt {
			s.snapTriedCnt = 0
			return raftpb.Snapshot{}, fmt.Errorf("raftstore: snapshot generation failed after %d attempts", maxSnapTryCnt)
		}

		resultCh := make(chan GenSnapResult, 1)
		canceled := &atomic.Bool{}

		task := GenSnapTask{
			RegionID: s.regionID,
			Region:   region,
			SnapKey: SnapKey{
				RegionID: s.regionID,
				Term:     s.applyState.TruncatedTerm,
				Index:    s.applyState.AppliedIndex,
			},
			Canceled: canceled,
			ResultCh: resultCh,
		}

		select {
		case taskCh <- task:
		default:
			return raftpb.Snapshot{}, fmt.Errorf("raftstore: snap worker busy")
		}

		s.snapState = SnapStateGenerating
		s.snapReceiver = resultCh
		s.snapCanceled = canceled
		s.snapTriedCnt++

		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable

	case SnapStateGenerating:
		// Check if result is available.
		select {
		case result := <-s.snapReceiver:
			s.snapState = SnapStateRelax
			s.snapReceiver = nil
			s.snapCanceled = nil
			if result.Err != nil {
				return raftpb.Snapshot{}, result.Err
			}
			return result.Snapshot, nil
		default:
			return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
		}

	default:
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
}

// CancelGeneratingSnap cancels any in-progress snapshot generation.
func (s *PeerStorage) CancelGeneratingSnap() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.snapState == SnapStateGenerating && s.snapCanceled != nil {
		s.snapCanceled.Store(true)
		s.snapState = SnapStateRelax
		s.snapReceiver = nil
		s.snapCanceled = nil
	}
}
