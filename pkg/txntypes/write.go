package txntypes

import (
	"encoding/binary"
	"errors"
	"io"
)

// WriteType represents the type of a write record.
type WriteType byte

const (
	WriteTypePut      WriteType = 'P' // 0x50 - Data was written
	WriteTypeDelete   WriteType = 'D' // 0x44 - Data was deleted
	WriteTypeLock     WriteType = 'L' // 0x4C - Lock-only transaction
	WriteTypeRollback WriteType = 'R' // 0x52 - Transaction was rolled back
)

// ShortValueMaxLen is the maximum length for values inlined in Write records.
const ShortValueMaxLen = 255

// Write represents a committed (or rolled-back) version of a user key.
type Write struct {
	WriteType             WriteType
	StartTS               TimeStamp
	ShortValue            []byte
	HasOverlappedRollback bool
	GCFence               *TimeStamp
	LastChange            LastChange
	TxnSource             uint64
}

// Optional field prefix tags for Write records.
const (
	writeTagShortValue        = byte('v') // 0x76
	writeTagOverlappedRollback = byte('R') // 0x52
	writeTagGCFence           = byte('F') // 0x46
	writeTagLastChange        = byte('l') // 0x6C
	writeTagTxnSource         = byte('S') // 0x53
)

// Marshal serializes a Write into the TiKV-compatible binary format.
func (w *Write) Marshal() []byte {
	var buf []byte

	// Required fields
	buf = append(buf, byte(w.WriteType))
	buf = appendVarint(buf, uint64(w.StartTS))

	// Optional fields
	if len(w.ShortValue) > 0 {
		buf = append(buf, writeTagShortValue)
		buf = append(buf, byte(len(w.ShortValue)))
		buf = append(buf, w.ShortValue...)
	}

	if w.HasOverlappedRollback {
		buf = append(buf, writeTagOverlappedRollback)
	}

	if w.GCFence != nil {
		buf = append(buf, writeTagGCFence)
		buf = appendUint64BE(buf, uint64(*w.GCFence))
	}

	if !w.LastChange.TS.IsZero() {
		buf = append(buf, writeTagLastChange)
		buf = appendUint64BE(buf, uint64(w.LastChange.TS))
		buf = appendVarint(buf, w.LastChange.EstimatedVersions)
	}

	if w.TxnSource > 0 {
		buf = append(buf, writeTagTxnSource)
		buf = appendVarint(buf, w.TxnSource)
	}

	return buf
}

// UnmarshalWrite deserializes a Write from the TiKV-compatible binary format.
func UnmarshalWrite(data []byte) (*Write, error) {
	if len(data) < 1 {
		return nil, io.ErrUnexpectedEOF
	}

	w := &Write{}

	// Required: writeType
	w.WriteType = WriteType(data[0])
	data = data[1:]

	// Required: startTS
	var err error
	var v uint64
	v, data, err = readVarint(data)
	if err != nil {
		return nil, err
	}
	w.StartTS = TimeStamp(v)

	// Optional fields
	for len(data) > 0 {
		tag := data[0]
		data = data[1:]

		switch tag {
		case writeTagShortValue:
			if len(data) < 1 {
				return nil, io.ErrUnexpectedEOF
			}
			vLen := int(data[0])
			data = data[1:]
			if len(data) < vLen {
				return nil, io.ErrUnexpectedEOF
			}
			w.ShortValue = make([]byte, vLen)
			copy(w.ShortValue, data[:vLen])
			data = data[vLen:]

		case writeTagOverlappedRollback:
			w.HasOverlappedRollback = true

		case writeTagGCFence:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			ts := TimeStamp(binary.BigEndian.Uint64(data[:8]))
			w.GCFence = &ts
			data = data[8:]

		case writeTagLastChange:
			if len(data) < 8 {
				return nil, io.ErrUnexpectedEOF
			}
			w.LastChange.TS = TimeStamp(binary.BigEndian.Uint64(data[:8]))
			data = data[8:]
			w.LastChange.EstimatedVersions, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}

		case writeTagTxnSource:
			w.TxnSource, data, err = readVarint(data)
			if err != nil {
				return nil, err
			}

		default:
			return nil, errors.New("txntypes: unknown write tag")
		}
	}

	return w, nil
}

// NeedValue returns true if the value must be fetched from CF_DEFAULT.
func (w *Write) NeedValue() bool {
	return w.WriteType == WriteTypePut && len(w.ShortValue) == 0
}

// IsDataChanged returns true if this write represents a Put or Delete.
func (w *Write) IsDataChanged() bool {
	return w.WriteType == WriteTypePut || w.WriteType == WriteTypeDelete
}
