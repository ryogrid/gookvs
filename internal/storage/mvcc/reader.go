package mvcc

import (
	"bytes"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// MvccReader provides MVCC-aware reads across column families.
type MvccReader struct {
	snapshot traits.Snapshot
}

// NewMvccReader creates a new MvccReader backed by the given snapshot.
func NewMvccReader(snap traits.Snapshot) *MvccReader {
	return &MvccReader{snapshot: snap}
}

// LoadLock reads the lock record for a key from CF_LOCK.
func (r *MvccReader) LoadLock(key Key) (*txntypes.Lock, error) {
	lockKey := EncodeLockKey(key)
	data, err := r.snapshot.Get(cfnames.CFLock, lockKey)
	if err != nil {
		if err == traits.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	lock, err := txntypes.UnmarshalLock(data)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// SeekWrite finds the first write record for key with commitTS <= ts.
// Returns the write record, the commit timestamp, and any error.
// Returns (nil, 0, nil) if no write is found.
func (r *MvccReader) SeekWrite(key Key, ts txntypes.TimeStamp) (*txntypes.Write, txntypes.TimeStamp, error) {
	seekKey := EncodeKey(key, ts)
	encodedUserKey := EncodeLockKey(key) // Same encoding, just the key part.

	iter := r.snapshot.NewIterator(cfnames.CFWrite, traits.IterOptions{})
	defer iter.Close()

	iter.Seek(seekKey)
	if !iter.Valid() {
		return nil, 0, iter.Error()
	}

	// Check that the found key belongs to the same user key.
	foundKey := iter.Key()
	foundUserKey := TruncateToUserKey(foundKey)
	if !bytes.Equal(foundUserKey, encodedUserKey) {
		return nil, 0, nil
	}

	// Decode the write record.
	_, commitTS, err := DecodeKey(foundKey)
	if err != nil {
		return nil, 0, err
	}

	write, err := txntypes.UnmarshalWrite(iter.Value())
	if err != nil {
		return nil, 0, err
	}

	return write, commitTS, nil
}

// GetWrite finds the latest data-changing write (Put or Delete) for key visible at ts.
// Skips Lock and Rollback records, using the LastChange optimization when available.
func (r *MvccReader) GetWrite(key Key, ts txntypes.TimeStamp) (*txntypes.Write, txntypes.TimeStamp, error) {
	seekTS := ts
	for i := 0; i < SeekBound*2; i++ { // Safety limit to prevent infinite loops.
		write, commitTS, err := r.SeekWrite(key, seekTS)
		if err != nil {
			return nil, 0, err
		}
		if write == nil {
			return nil, 0, nil
		}

		switch write.WriteType {
		case txntypes.WriteTypePut:
			return write, commitTS, nil
		case txntypes.WriteTypeDelete:
			return nil, 0, nil // Deleted.
		case txntypes.WriteTypeLock, txntypes.WriteTypeRollback:
			// Skip non-data-changing records.
			if write.LastChange.EstimatedVersions >= SeekBound && !write.LastChange.TS.IsZero() {
				// Use LastChange optimization: jump directly to last data version.
				seekTS = write.LastChange.TS
			} else {
				seekTS = commitTS.Prev()
			}
			continue
		default:
			return nil, 0, nil
		}
	}
	return nil, 0, nil
}

// GetTxnCommitRecord finds a specific transaction's commit record by matching start_ts.
func (r *MvccReader) GetTxnCommitRecord(key Key, startTS txntypes.TimeStamp) (*txntypes.Write, txntypes.TimeStamp, error) {
	// Start scanning from TSMax down.
	seekTS := txntypes.TSMax
	encodedUserKey := EncodeLockKey(key)

	iter := r.snapshot.NewIterator(cfnames.CFWrite, traits.IterOptions{})
	defer iter.Close()

	seekKey := EncodeKey(key, seekTS)
	iter.Seek(seekKey)

	for iter.Valid() {
		foundKey := iter.Key()
		foundUserKey := TruncateToUserKey(foundKey)
		if !bytes.Equal(foundUserKey, encodedUserKey) {
			break
		}

		_, commitTS, err := DecodeKey(foundKey)
		if err != nil {
			return nil, 0, err
		}

		write, err := txntypes.UnmarshalWrite(iter.Value())
		if err != nil {
			return nil, 0, err
		}

		if write.StartTS == startTS {
			return write, commitTS, nil
		}

		// If we've gone past where the write could be, stop.
		if commitTS < startTS {
			break
		}

		iter.Next()
	}

	return nil, 0, nil
}

// GetValue reads a value from CF_DEFAULT for the given key and start_ts.
func (r *MvccReader) GetValue(key Key, startTS txntypes.TimeStamp) ([]byte, error) {
	encodedKey := EncodeKey(key, startTS)
	data, err := r.snapshot.Get(cfnames.CFDefault, encodedKey)
	if err != nil {
		if err == traits.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

// Close releases the underlying snapshot.
func (r *MvccReader) Close() {
	r.snapshot.Close()
}
