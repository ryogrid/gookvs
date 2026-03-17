package mvcc

import (
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// ModifyType represents the type of a CF modification.
type ModifyType int

const (
	ModifyTypePut    ModifyType = iota
	ModifyTypeDelete
)

// Modify represents a single column family operation.
type Modify struct {
	Type  ModifyType
	CF    string
	Key   []byte
	Value []byte
}

// ReleasedLock contains information about a released lock, used for
// lock manager wake-up notifications.
type ReleasedLock struct {
	Key           Key
	StartTS       txntypes.TimeStamp
	IsPessimistic bool
}

// MvccTxn is a write-only accumulator that collects all modifications
// during a transaction action (prewrite, commit, rollback, etc.) and
// flushes them as a single atomic batch write.
type MvccTxn struct {
	StartTS   txntypes.TimeStamp
	Modifies  []Modify
	WriteSize int
}

// NewMvccTxn creates a new MvccTxn with the given start timestamp.
func NewMvccTxn(startTS txntypes.TimeStamp) *MvccTxn {
	return &MvccTxn{
		StartTS: startTS,
	}
}

// PutLock writes a lock record to CF_LOCK.
func (txn *MvccTxn) PutLock(key Key, lock *txntypes.Lock) {
	lockData := lock.Marshal()
	txn.Modifies = append(txn.Modifies, Modify{
		Type:  ModifyTypePut,
		CF:    cfnames.CFLock,
		Key:   EncodeLockKey(key),
		Value: lockData,
	})
	txn.WriteSize += len(key) + len(lockData)
}

// UnlockKey removes a lock from CF_LOCK.
func (txn *MvccTxn) UnlockKey(key Key, isPessimistic bool) *ReleasedLock {
	txn.Modifies = append(txn.Modifies, Modify{
		Type: ModifyTypeDelete,
		CF:   cfnames.CFLock,
		Key:  EncodeLockKey(key),
	})
	return &ReleasedLock{
		Key:           key,
		StartTS:       txn.StartTS,
		IsPessimistic: isPessimistic,
	}
}

// PutValue writes a large value to CF_DEFAULT (for values > ShortValueMaxLen).
func (txn *MvccTxn) PutValue(key Key, startTS txntypes.TimeStamp, value []byte) {
	txn.Modifies = append(txn.Modifies, Modify{
		Type:  ModifyTypePut,
		CF:    cfnames.CFDefault,
		Key:   EncodeKey(key, startTS),
		Value: value,
	})
	txn.WriteSize += len(key) + len(value) + 8 // 8 for timestamp
}

// DeleteValue removes a value from CF_DEFAULT.
func (txn *MvccTxn) DeleteValue(key Key, startTS txntypes.TimeStamp) {
	txn.Modifies = append(txn.Modifies, Modify{
		Type: ModifyTypeDelete,
		CF:   cfnames.CFDefault,
		Key:  EncodeKey(key, startTS),
	})
}

// PutWrite writes a commit/rollback record to CF_WRITE.
func (txn *MvccTxn) PutWrite(key Key, commitTS txntypes.TimeStamp, write *txntypes.Write) {
	writeData := write.Marshal()
	txn.Modifies = append(txn.Modifies, Modify{
		Type:  ModifyTypePut,
		CF:    cfnames.CFWrite,
		Key:   EncodeKey(key, commitTS),
		Value: writeData,
	})
	txn.WriteSize += len(key) + len(writeData) + 8
}

// DeleteWrite removes a write record from CF_WRITE.
func (txn *MvccTxn) DeleteWrite(key Key, commitTS txntypes.TimeStamp) {
	txn.Modifies = append(txn.Modifies, Modify{
		Type: ModifyTypeDelete,
		CF:   cfnames.CFWrite,
		Key:  EncodeKey(key, commitTS),
	})
}

// ModifyCount returns the number of pending modifications.
func (txn *MvccTxn) ModifyCount() int {
	return len(txn.Modifies)
}
