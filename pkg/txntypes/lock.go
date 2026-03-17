package txntypes

import (
	"encoding/binary"
	"errors"
	"io"
)

// LockType represents the type of a transaction lock.
type LockType byte

const (
	LockTypePut         LockType = 'P' // 0x50 - Write intent (optimistic)
	LockTypeDelete      LockType = 'D' // 0x44 - Delete intent
	LockTypeLock        LockType = 'L' // 0x4C - Read lock (SELECT FOR UPDATE)
	LockTypePessimistic LockType = 'S' // 0x53 - Pessimistic lock placeholder
)

// Lock represents an active transaction lock on a user key.
type Lock struct {
	LockType                    LockType
	Primary                     []byte
	StartTS                     TimeStamp
	TTL                         uint64
	ShortValue                  []byte
	ForUpdateTS                 TimeStamp
	TxnSize                     uint64
	MinCommitTS                 TimeStamp
	UseAsyncCommit              bool
	Secondaries                 [][]byte
	RollbackTS                  []TimeStamp
	LastChange                  LastChange
	TxnSource                   uint64
	PessimisticLockWithConflict bool
	Generation                  uint64
}

// LastChange records information about the previous data-changing version.
type LastChange struct {
	TS                TimeStamp
	EstimatedVersions uint64
}

// Optional field prefix tags (byte-identical to TiKV).
const (
	lockTagShortValue        = byte('v') // 0x76
	lockTagForUpdateTS       = byte('f') // 0x66
	lockTagTxnSize           = byte('t') // 0x74
	lockTagMinCommitTS       = byte('c') // 0x63
	lockTagAsyncCommitKeys   = byte('a') // 0x61
	lockTagRollbackTS        = byte('r') // 0x72
	lockTagLastChange        = byte('l') // 0x6C
	lockTagTxnSource         = byte('s') // 0x73
	lockTagPessimConflict    = byte('F') // 0x46
	lockTagGeneration        = byte('g') // 0x67
)

// Marshal serializes a Lock into the TiKV-compatible binary format.
func (l *Lock) Marshal() []byte {
	var buf []byte

	// Required fields
	buf = append(buf, byte(l.LockType))
	buf = appendVarBytes(buf, l.Primary)
	buf = appendVarint(buf, uint64(l.StartTS))
	buf = appendVarint(buf, l.TTL)

	// Optional fields (order-dependent, must match TiKV)
	if len(l.ShortValue) > 0 {
		buf = append(buf, lockTagShortValue)
		buf = append(buf, byte(len(l.ShortValue)))
		buf = append(buf, l.ShortValue...)
	}

	if !l.ForUpdateTS.IsZero() {
		buf = append(buf, lockTagForUpdateTS)
		buf = appendUint64BE(buf, uint64(l.ForUpdateTS))
	}

	if l.TxnSize > 0 {
		buf = append(buf, lockTagTxnSize)
		buf = appendUint64BE(buf, l.TxnSize)
	}

	if !l.MinCommitTS.IsZero() {
		buf = append(buf, lockTagMinCommitTS)
		buf = appendUint64BE(buf, uint64(l.MinCommitTS))
	}

	if l.UseAsyncCommit {
		buf = append(buf, lockTagAsyncCommitKeys)
		buf = appendVarint(buf, uint64(len(l.Secondaries)))
		for _, sec := range l.Secondaries {
			buf = appendVarBytes(buf, sec)
		}
	}

	if len(l.RollbackTS) > 0 {
		buf = append(buf, lockTagRollbackTS)
		buf = appendVarint(buf, uint64(len(l.RollbackTS)))
		for _, ts := range l.RollbackTS {
			buf = appendUint64BE(buf, uint64(ts))
		}
	}

	if !l.LastChange.TS.IsZero() {
		buf = append(buf, lockTagLastChange)
		buf = appendUint64BE(buf, uint64(l.LastChange.TS))
		buf = appendVarint(buf, l.LastChange.EstimatedVersions)
	}

	if l.TxnSource > 0 {
		buf = append(buf, lockTagTxnSource)
		buf = appendVarint(buf, l.TxnSource)
	}

	if l.PessimisticLockWithConflict {
		buf = append(buf, lockTagPessimConflict)
	}

	if l.Generation > 0 {
		buf = append(buf, lockTagGeneration)
		buf = appendUint64BE(buf, l.Generation)
	}

	return buf
}

// UnmarshalLock deserializes a Lock from the TiKV-compatible binary format.
func UnmarshalLock(data []byte) (*Lock, error) {
	if len(data) < 1 {
		return nil, io.ErrUnexpectedEOF
	}

	l := &Lock{}

	// Required: lockType
	l.LockType = LockType(data[0])
	data = data[1:]

	// Required: primary
	var err error
	l.Primary, data, err = readVarBytes(data)
	if err != nil {
		return nil, err
	}

	// Required: startTS
	var v uint64
	v, data, err = readVarint(data)
	if err != nil {
		return nil, err
	}
	l.StartTS = TimeStamp(v)

	// Required: ttl
	v, data, err = readVarint(data)
	if err != nil {
		return nil, err
	}
	l.TTL = v

	// Optional fields
	for len(data) > 0 {
		tag := data[0]
		data = data[1:]

		switch tag {
		case lockTagShortValue:
			if len(data) < 1 {
				return nil, io.ErrUnexpectedEOF
			}
			vLen := int(data[0])
			data = data[1:]
			if len(data) < vLen {
				return nil, io.ErrUnexpectedEOF
			}
			l.ShortValue = make([]byte, vLen)
			copy(l.ShortValue, data[:vLen])
			data = data[vLen:]

		case lockTagForUpdateTS:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			l.ForUpdateTS = TimeStamp(binary.BigEndian.Uint64(data[:8]))
			data = data[8:]

		case lockTagTxnSize:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			l.TxnSize = binary.BigEndian.Uint64(data[:8])
			data = data[8:]

		case lockTagMinCommitTS:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			l.MinCommitTS = TimeStamp(binary.BigEndian.Uint64(data[:8]))
			data = data[8:]

		case lockTagAsyncCommitKeys:
			l.UseAsyncCommit = true
			var count uint64
			count, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}
			l.Secondaries = make([][]byte, count)
			for i := uint64(0); i < count; i++ {
				l.Secondaries[i], data, err = readVarBytes(data)
				if err != nil {
					return nil, err
				}
			}

		case lockTagRollbackTS:
			var count uint64
			count, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}
			l.RollbackTS = make([]TimeStamp, count)
			for i := uint64(0); i < count; i++ {
				if len(data) < 8 {
					return nil, io.ErrUnexpectedEOF
				}
				l.RollbackTS[i] = TimeStamp(binary.BigEndian.Uint64(data[:8]))
				data = data[8:]
			}

		case lockTagLastChange:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			l.LastChange.TS = TimeStamp(binary.BigEndian.Uint64(data[:8]))
			data = data[8:]
			l.LastChange.EstimatedVersions, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}

		case lockTagTxnSource:
			l.TxnSource, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}

		case lockTagPessimConflict:
			l.PessimisticLockWithConflict = true

		case lockTagGeneration:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			l.Generation = binary.BigEndian.Uint64(data[:8])
			data = data[8:]

		default:
			return nil, errors.New("txntypes: unknown lock tag")
		}
	}

	return l, nil
}

// IsExpired checks whether the lock has exceeded its TTL.
func (l *Lock) IsExpired(currentTS TimeStamp) bool {
	physicalMS := uint64(currentTS.Physical())
	lockPhysicalMS := uint64(l.StartTS.Physical())
	return physicalMS >= lockPhysicalMS+l.TTL
}
