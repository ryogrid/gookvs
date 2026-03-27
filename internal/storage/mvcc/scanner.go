package mvcc

import (
	"bytes"
	"fmt"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

// ScannerConfig holds configuration for creating a Scanner.
type ScannerConfig struct {
	Snapshot       traits.Snapshot
	ReadTS         txntypes.TimeStamp
	Desc           bool
	IsolationLevel IsolationLevel
	KeyOnly        bool
	LowerBound     Key
	UpperBound     Key
	BypassLocks    map[txntypes.TimeStamp]bool
}

// ScanStatistics tracks performance counters during a scan.
type ScanStatistics struct {
	ScannedKeys   int64
	ProcessedKeys int64
	ScannedLocks  int64
	DefaultReads  int64
	OverSeekBound int64
}

// Scanner performs MVCC-aware range scans with persistent cursor state.
type Scanner struct {
	cfg ScannerConfig

	writeCursor   traits.Iterator
	lockCursor    traits.Iterator // nil under RC isolation
	defaultCursor traits.Iterator // lazily created

	isStarted bool
	stats     ScanStatistics
}

// NewScanner creates a Scanner from the given configuration.
// The caller must call Scanner.Close() when done.
func NewScanner(cfg ScannerConfig) *Scanner {
	s := &Scanner{cfg: cfg}

	writeOpts := traits.IterOptions{}
	lockOpts := traits.IterOptions{}

	if !cfg.Desc {
		// Forward scan.
		if cfg.LowerBound != nil {
			writeOpts.LowerBound = EncodeKey(cfg.LowerBound, txntypes.TSMax)
			lockOpts.LowerBound = EncodeLockKey(cfg.LowerBound)
		}
		if cfg.UpperBound != nil {
			writeOpts.UpperBound = EncodeKey(cfg.UpperBound, txntypes.TSMax)
			lockOpts.UpperBound = EncodeLockKey(cfg.UpperBound)
		}
	} else {
		// Backward scan: swap bounds.
		if cfg.LowerBound != nil {
			writeOpts.LowerBound = EncodeKey(cfg.LowerBound, txntypes.TSMax)
			lockOpts.LowerBound = EncodeLockKey(cfg.LowerBound)
		}
		if cfg.UpperBound != nil {
			// For backward scan, the "upper" is the start position.
			writeOpts.UpperBound = EncodeKey(cfg.UpperBound, 0)
			lockOpts.UpperBound = EncodeLockKey(cfg.UpperBound)
		}
	}

	s.writeCursor = cfg.Snapshot.NewIterator(cfnames.CFWrite, writeOpts)

	if cfg.IsolationLevel == IsolationLevelSI {
		s.lockCursor = cfg.Snapshot.NewIterator(cfnames.CFLock, lockOpts)
	}

	return s
}

// Next returns the next visible key-value pair, or (nil, nil, nil) when exhausted.
func (s *Scanner) Next() (key Key, value []byte, err error) {
	if !s.isStarted {
		s.initialSeek()
		s.isStarted = true
	}

	for {
		if !s.writeCursorValid() && !s.lockCursorValid() {
			return nil, nil, nil // exhausted
		}

		// Determine current user key = min (forward) or max (backward) of write and lock cursors.
		currentUserKey, hasWrite, hasLock := s.currentUserKey()
		if currentUserKey == nil {
			return nil, nil, nil
		}

		// Check upper/lower bound.
		if !s.cfg.Desc {
			if s.cfg.UpperBound != nil && bytes.Compare(currentUserKey, s.cfg.UpperBound) >= 0 {
				return nil, nil, nil
			}
		} else {
			if s.cfg.LowerBound != nil && bytes.Compare(currentUserKey, s.cfg.LowerBound) < 0 {
				return nil, nil, nil
			}
		}

		// Handle lock if present.
		if hasLock {
			if err := s.handleLock(currentUserKey); err != nil {
				return nil, nil, err
			}
		}

		// Handle write if present.
		if hasWrite {
			value, found, err := s.handleWrite(currentUserKey)
			if err != nil {
				return nil, nil, err
			}
			if found {
				s.stats.ProcessedKeys++
				return currentUserKey, value, nil
			}
			// Key was deleted or no visible version, continue to next key.
			continue
		}

		// Only lock exists for this key (no write), skip to next.
		continue
	}
}

// TakeStatistics returns the accumulated statistics and resets the counters.
func (s *Scanner) TakeStatistics() ScanStatistics {
	stats := s.stats
	s.stats = ScanStatistics{}
	return stats
}

// Close releases all iterator resources.
func (s *Scanner) Close() {
	if s.writeCursor != nil {
		s.writeCursor.Close()
	}
	if s.lockCursor != nil {
		s.lockCursor.Close()
	}
	if s.defaultCursor != nil {
		s.defaultCursor.Close()
	}
}

func (s *Scanner) initialSeek() {
	if !s.cfg.Desc {
		s.writeCursor.SeekToFirst()
		if s.lockCursor != nil {
			s.lockCursor.SeekToFirst()
		}
	} else {
		s.writeCursor.SeekToLast()
		if s.lockCursor != nil {
			s.lockCursor.SeekToLast()
		}
	}
}

func (s *Scanner) writeCursorValid() bool {
	return s.writeCursor != nil && s.writeCursor.Valid()
}

func (s *Scanner) lockCursorValid() bool {
	return s.lockCursor != nil && s.lockCursor.Valid()
}

// currentUserKey determines the current user key from write and lock cursors.
// Returns the user key, whether write cursor has data for it, and whether lock cursor has data.
func (s *Scanner) currentUserKey() (Key, bool, bool) {
	var writeUserKey, lockUserKey Key

	if s.writeCursorValid() {
		encodedKey := s.writeCursor.Key()
		uk, _, err := DecodeKey(encodedKey)
		if err == nil {
			writeUserKey = uk
		}
	}

	if s.lockCursorValid() {
		encodedKey := s.lockCursor.Key()
		uk, err := DecodeLockKey(encodedKey)
		if err == nil {
			lockUserKey = uk
		}
	}

	if writeUserKey == nil && lockUserKey == nil {
		return nil, false, false
	}
	if writeUserKey == nil {
		// Only lock, advance lock cursor.
		s.advanceLockCursor()
		return lockUserKey, false, true
	}
	if lockUserKey == nil {
		return writeUserKey, true, false
	}

	cmp := bytes.Compare(writeUserKey, lockUserKey)
	if !s.cfg.Desc {
		// Forward: min
		if cmp < 0 {
			return writeUserKey, true, false
		} else if cmp > 0 {
			s.advanceLockCursor()
			return lockUserKey, false, true
		}
		// Equal
		return writeUserKey, true, true
	}
	// Backward: max
	if cmp > 0 {
		return writeUserKey, true, false
	} else if cmp < 0 {
		s.advanceLockCursor()
		return lockUserKey, false, true
	}
	return writeUserKey, true, true
}

func (s *Scanner) handleLock(userKey Key) error {
	if s.lockCursor == nil || !s.lockCursor.Valid() {
		return nil
	}

	// Decode lock key and check if it matches.
	encodedKey := s.lockCursor.Key()
	lk, err := DecodeLockKey(encodedKey)
	if err != nil {
		return nil // skip
	}
	if !bytes.Equal(lk, userKey) {
		return nil // not for this key
	}

	s.stats.ScannedLocks++

	lockData := s.lockCursor.Value()
	lock, err := txntypes.UnmarshalLock(lockData)
	if err != nil {
		s.advanceLockCursor()
		return nil // skip corrupt lock
	}

	// Advance lock cursor past this key.
	s.advanceLockCursor()

	// Skip pessimistic locks.
	if lock.LockType == txntypes.LockTypePessimistic {
		return nil
	}

	// Check for conflicting lock.
	if lock.StartTS <= s.cfg.ReadTS {
		if s.cfg.BypassLocks != nil && s.cfg.BypassLocks[lock.StartTS] {
			return nil
		}
		return &LockError{Key: userKey, Lock: lock}
	}

	return nil
}

// handleWrite processes the write cursor for the given user key.
// Returns (value, found, error). found=false means skip this key.
func (s *Scanner) handleWrite(userKey Key) ([]byte, bool, error) {
	// For backward scans, the cursor may be at a version with low commitTS.
	// We need to seek to the version with the highest commitTS <= readTS.
	if s.cfg.Desc {
		seekKey := EncodeKey(userKey, s.cfg.ReadTS)
		s.writeCursor.Seek(seekKey)
	}

	// Find the first write with commitTS <= readTS for this user key.
	if !s.moveWriteCursorToTS(userKey) {
		// No write visible at readTS.
		s.moveWriteCursorToNextUserKey(userKey)
		return nil, false, nil
	}

	// Iterate through write records for this user key (always forward for version search).
	for s.writeCursorValid() {
		encodedKey := s.writeCursor.Key()
		uk, commitTS, err := DecodeKey(encodedKey)
		if err != nil {
			s.moveWriteCursorToNextUserKey(userKey)
			return nil, false, nil
		}
		if !bytes.Equal(uk, userKey) {
			break // Moved past this user key.
		}
		if commitTS > s.cfg.ReadTS {
			s.writeCursor.Next() // Always forward for version iteration.
			continue
		}

		s.stats.ScannedKeys++

		writeData := s.writeCursor.Value()
		write, err := txntypes.UnmarshalWrite(writeData)
		if err != nil {
			s.moveWriteCursorToNextUserKey(userKey)
			return nil, false, nil
		}

		switch write.WriteType {
		case txntypes.WriteTypePut:
			var value []byte
			if !s.cfg.KeyOnly {
				if write.ShortValue != nil {
					value = write.ShortValue
				} else {
					value, err = s.loadValueFromDefault(userKey, write.StartTS)
					if err != nil {
						s.moveWriteCursorToNextUserKey(userKey)
						return nil, false, fmt.Errorf("scanner: default value not found for key %x at startTS %d: %w", userKey, write.StartTS, err)
					}
				}
			}
			s.moveWriteCursorToNextUserKey(userKey)
			return value, true, nil

		case txntypes.WriteTypeDelete:
			s.moveWriteCursorToNextUserKey(userKey)
			return nil, false, nil

		case txntypes.WriteTypeLock, txntypes.WriteTypeRollback:
			// Non-data-changing, try LastChange optimization.
			if write.LastChange.EstimatedVersions >= uint64(SeekBound) && !write.LastChange.TS.IsZero() {
				seekKey := EncodeKey(userKey, write.LastChange.TS)
				s.writeCursor.Seek(seekKey)
				s.stats.OverSeekBound++
			} else {
				s.writeCursor.Next() // Always forward for version iteration.
			}
			continue

		default:
			s.moveWriteCursorToNextUserKey(userKey)
			return nil, false, nil
		}
	}

	return nil, false, nil
}

// moveWriteCursorToTS positions the write cursor at the first entry for userKey
// with commitTS <= readTS. Returns true if such an entry exists.
func (s *Scanner) moveWriteCursorToTS(userKey Key) bool {
	if !s.writeCursorValid() {
		return false
	}

	// Check if current position is already at the right user key.
	encodedKey := s.writeCursor.Key()
	uk, commitTS, err := DecodeKey(encodedKey)
	if err != nil {
		return false
	}
	if bytes.Equal(uk, userKey) && commitTS <= s.cfg.ReadTS {
		return true
	}

	// If write cursor is ahead of readTS for this key, we need to advance.
	// Since timestamps are descending in encoding, entries with higher commitTS
	// come first. We need to find the first entry with commitTS <= readTS.
	if bytes.Equal(uk, userKey) && commitTS > s.cfg.ReadTS {
		// Try Next() up to SeekBound times.
		for i := 0; i < SeekBound; i++ {
			s.advanceWriteCursor()
			if !s.writeCursorValid() {
				return false
			}
			encodedKey = s.writeCursor.Key()
			uk, commitTS, err = DecodeKey(encodedKey)
			if err != nil || !bytes.Equal(uk, userKey) {
				return false
			}
			if commitTS <= s.cfg.ReadTS {
				return true
			}
		}
		// Fall back to Seek.
		s.stats.OverSeekBound++
		seekKey := EncodeKey(userKey, s.cfg.ReadTS)
		s.writeCursor.Seek(seekKey)
		if !s.writeCursorValid() {
			return false
		}
		encodedKey = s.writeCursor.Key()
		uk, commitTS, err = DecodeKey(encodedKey)
		if err != nil || !bytes.Equal(uk, userKey) {
			return false
		}
		return commitTS <= s.cfg.ReadTS
	}

	return false
}

// moveWriteCursorToNextUserKey advances the write cursor past all versions of the current user key.
func (s *Scanner) moveWriteCursorToNextUserKey(userKey Key) {
	if s.cfg.Desc {
		// For backward scan: after forward version lookup, reposition before this user key.
		s.repositionForBackward(userKey)
		return
	}

	// Forward scan: advance past all versions of this user key.
	encodedUserKey := EncodeLockKey(userKey) // Same as encoded user key prefix.

	for i := 0; i < SeekBound; i++ {
		s.writeCursor.Next()
		if !s.writeCursorValid() {
			return
		}
		foundKey := s.writeCursor.Key()
		foundUserKey := TruncateToUserKey(foundKey)
		if !bytes.Equal(foundUserKey, encodedUserKey) {
			return // Moved to a different user key.
		}
	}

	// Exceeded SeekBound, use Seek.
	s.stats.OverSeekBound++
	nextKey := append([]byte{}, userKey...)
	nextKey = append(nextKey, 0)
	seekKey := EncodeKey(nextKey, txntypes.TSMax)
	s.writeCursor.Seek(seekKey)
}

// repositionForBackward repositions the write cursor for backward traversal
// after a forward seek was used to find the correct version.
func (s *Scanner) repositionForBackward(userKey Key) {
	// We need to position before all versions of userKey.
	// Seek to the first entry of this user key (highest commitTS), then Prev.
	seekKey := EncodeKey(userKey, txntypes.TSMax)
	s.writeCursor.Seek(seekKey)
	if s.writeCursorValid() {
		uk, _, err := DecodeKey(s.writeCursor.Key())
		if err == nil && bytes.Equal(uk, userKey) {
			s.writeCursor.Prev()
			return
		}
		// Cursor is past this user key, go prev.
		s.writeCursor.Prev()
	} else {
		s.writeCursor.SeekToLast()
	}
}

func (s *Scanner) advanceWriteCursor() {
	if !s.cfg.Desc {
		s.writeCursor.Next()
	} else {
		s.writeCursor.Prev()
	}
}

func (s *Scanner) advanceLockCursor() {
	if s.lockCursor == nil {
		return
	}
	if !s.cfg.Desc {
		s.lockCursor.Next()
	} else {
		s.lockCursor.Prev()
	}
}

func (s *Scanner) ensureDefaultCursor() {
	if s.defaultCursor == nil {
		s.defaultCursor = s.cfg.Snapshot.NewIterator(cfnames.CFDefault, traits.IterOptions{})
	}
}

func (s *Scanner) loadValueFromDefault(userKey Key, startTS txntypes.TimeStamp) ([]byte, error) {
	s.ensureDefaultCursor()
	s.stats.DefaultReads++

	seekKey := EncodeKey(userKey, startTS)
	s.defaultCursor.Seek(seekKey)
	if !s.defaultCursor.Valid() {
		return nil, fmt.Errorf("not found")
	}

	// Verify the key matches.
	foundKey := s.defaultCursor.Key()
	uk, ts, err := DecodeKey(foundKey)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(uk, userKey) || ts != startTS {
		return nil, fmt.Errorf("not found")
	}

	value := s.defaultCursor.Value()
	return append([]byte(nil), value...), nil
}
