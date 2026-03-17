package txntypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests ported from TiKV: components/txn_types/src/lock.rs, write.rs

func TestTimeStamp(t *testing.T) {
	ts := ComposeTS(1000, 42)
	assert.Equal(t, int64(1000), ts.Physical())
	assert.Equal(t, int64(42), ts.Logical())
	assert.False(t, ts.IsZero())

	assert.True(t, TSZero.IsZero())
	assert.Equal(t, TSZero, TSZero.Prev())
	assert.Equal(t, TSMax, TSMax.Next())

	ts2 := ComposeTS(1000, 0)
	assert.Equal(t, int64(1000), ts2.Physical())
	assert.Equal(t, int64(0), ts2.Logical())

	// Prev and Next
	ts3 := ComposeTS(5, 5)
	assert.Equal(t, ts3-1, ts3.Prev())
	assert.Equal(t, ts3+1, ts3.Next())
}

func TestLockMarshalUnmarshal(t *testing.T) {
	// Minimal lock
	t.Run("minimal", func(t *testing.T) {
		l := &Lock{
			LockType: LockTypePut,
			Primary:  []byte("primary-key"),
			StartTS:  ComposeTS(100, 0),
			TTL:      3000,
		}

		data := l.Marshal()
		decoded, err := UnmarshalLock(data)
		require.NoError(t, err)

		assert.Equal(t, LockTypePut, decoded.LockType)
		assert.Equal(t, []byte("primary-key"), decoded.Primary)
		assert.Equal(t, ComposeTS(100, 0), decoded.StartTS)
		assert.Equal(t, uint64(3000), decoded.TTL)
		assert.Nil(t, decoded.ShortValue)
		assert.True(t, decoded.ForUpdateTS.IsZero())
	})

	// Lock with short value
	t.Run("short_value", func(t *testing.T) {
		l := &Lock{
			LockType:   LockTypePut,
			Primary:    []byte("pk"),
			StartTS:    ComposeTS(200, 1),
			TTL:        5000,
			ShortValue: []byte("hello world"),
		}

		data := l.Marshal()
		decoded, err := UnmarshalLock(data)
		require.NoError(t, err)
		assert.Equal(t, []byte("hello world"), decoded.ShortValue)
	})

	// Lock with all optional fields
	t.Run("all_fields", func(t *testing.T) {
		gcTS := ComposeTS(50, 0)
		l := &Lock{
			LockType:                    LockTypeDelete,
			Primary:                     []byte("primary"),
			StartTS:                     ComposeTS(300, 5),
			TTL:                         10000,
			ShortValue:                  []byte("sv"),
			ForUpdateTS:                 ComposeTS(250, 0),
			TxnSize:                     42,
			MinCommitTS:                 ComposeTS(301, 0),
			UseAsyncCommit:              true,
			Secondaries:                 [][]byte{[]byte("sec1"), []byte("sec2")},
			RollbackTS:                  []TimeStamp{ComposeTS(200, 0), ComposeTS(100, 0)},
			LastChange:                  LastChange{TS: gcTS, EstimatedVersions: 5},
			TxnSource:                   7,
			PessimisticLockWithConflict: true,
			Generation:                  3,
		}

		data := l.Marshal()
		decoded, err := UnmarshalLock(data)
		require.NoError(t, err)

		assert.Equal(t, LockTypeDelete, decoded.LockType)
		assert.Equal(t, []byte("primary"), decoded.Primary)
		assert.Equal(t, ComposeTS(300, 5), decoded.StartTS)
		assert.Equal(t, uint64(10000), decoded.TTL)
		assert.Equal(t, []byte("sv"), decoded.ShortValue)
		assert.Equal(t, ComposeTS(250, 0), decoded.ForUpdateTS)
		assert.Equal(t, uint64(42), decoded.TxnSize)
		assert.Equal(t, ComposeTS(301, 0), decoded.MinCommitTS)
		assert.True(t, decoded.UseAsyncCommit)
		assert.Len(t, decoded.Secondaries, 2)
		assert.Equal(t, []byte("sec1"), decoded.Secondaries[0])
		assert.Equal(t, []byte("sec2"), decoded.Secondaries[1])
		assert.Len(t, decoded.RollbackTS, 2)
		assert.Equal(t, ComposeTS(200, 0), decoded.RollbackTS[0])
		assert.Equal(t, ComposeTS(100, 0), decoded.RollbackTS[1])
		assert.Equal(t, gcTS, decoded.LastChange.TS)
		assert.Equal(t, uint64(5), decoded.LastChange.EstimatedVersions)
		assert.Equal(t, uint64(7), decoded.TxnSource)
		assert.True(t, decoded.PessimisticLockWithConflict)
		assert.Equal(t, uint64(3), decoded.Generation)
	})

	// Pessimistic lock
	t.Run("pessimistic", func(t *testing.T) {
		l := &Lock{
			LockType:    LockTypePessimistic,
			Primary:     []byte("pk"),
			StartTS:     ComposeTS(100, 0),
			TTL:         3000,
			ForUpdateTS: ComposeTS(150, 0),
		}

		data := l.Marshal()
		decoded, err := UnmarshalLock(data)
		require.NoError(t, err)
		assert.Equal(t, LockTypePessimistic, decoded.LockType)
		assert.Equal(t, ComposeTS(150, 0), decoded.ForUpdateTS)
	})
}

func TestLockIsExpired(t *testing.T) {
	l := &Lock{
		LockType: LockTypePut,
		Primary:  []byte("pk"),
		StartTS:  ComposeTS(1000, 0),
		TTL:      3000, // 3 seconds
	}

	// Not expired: current time = 1000ms + 2999ms
	assert.False(t, l.IsExpired(ComposeTS(3999, 0)))

	// Expired: current time = 1000ms + 3000ms
	assert.True(t, l.IsExpired(ComposeTS(4000, 0)))

	// Expired: current time = much later
	assert.True(t, l.IsExpired(ComposeTS(10000, 0)))
}

func TestWriteMarshalUnmarshal(t *testing.T) {
	// Minimal write
	t.Run("minimal", func(t *testing.T) {
		w := &Write{
			WriteType: WriteTypePut,
			StartTS:   ComposeTS(100, 0),
		}

		data := w.Marshal()
		decoded, err := UnmarshalWrite(data)
		require.NoError(t, err)

		assert.Equal(t, WriteTypePut, decoded.WriteType)
		assert.Equal(t, ComposeTS(100, 0), decoded.StartTS)
		assert.Nil(t, decoded.ShortValue)
		assert.False(t, decoded.HasOverlappedRollback)
		assert.Nil(t, decoded.GCFence)
	})

	// Write with short value
	t.Run("short_value", func(t *testing.T) {
		w := &Write{
			WriteType:  WriteTypePut,
			StartTS:    ComposeTS(200, 0),
			ShortValue: []byte("inline-value"),
		}

		data := w.Marshal()
		decoded, err := UnmarshalWrite(data)
		require.NoError(t, err)
		assert.Equal(t, []byte("inline-value"), decoded.ShortValue)
	})

	// Write with all fields
	t.Run("all_fields", func(t *testing.T) {
		gcFence := ComposeTS(50, 0)
		w := &Write{
			WriteType:             WriteTypeRollback,
			StartTS:               ComposeTS(300, 0),
			ShortValue:            []byte("sv"),
			HasOverlappedRollback: true,
			GCFence:               &gcFence,
			LastChange: LastChange{
				TS:                ComposeTS(250, 0),
				EstimatedVersions: 10,
			},
			TxnSource: 5,
		}

		data := w.Marshal()
		decoded, err := UnmarshalWrite(data)
		require.NoError(t, err)

		assert.Equal(t, WriteTypeRollback, decoded.WriteType)
		assert.Equal(t, ComposeTS(300, 0), decoded.StartTS)
		assert.Equal(t, []byte("sv"), decoded.ShortValue)
		assert.True(t, decoded.HasOverlappedRollback)
		require.NotNil(t, decoded.GCFence)
		assert.Equal(t, gcFence, *decoded.GCFence)
		assert.Equal(t, ComposeTS(250, 0), decoded.LastChange.TS)
		assert.Equal(t, uint64(10), decoded.LastChange.EstimatedVersions)
		assert.Equal(t, uint64(5), decoded.TxnSource)
	})

	// Delete write
	t.Run("delete", func(t *testing.T) {
		w := &Write{
			WriteType: WriteTypeDelete,
			StartTS:   ComposeTS(100, 0),
		}

		data := w.Marshal()
		decoded, err := UnmarshalWrite(data)
		require.NoError(t, err)
		assert.Equal(t, WriteTypeDelete, decoded.WriteType)
	})

	// Lock write
	t.Run("lock", func(t *testing.T) {
		w := &Write{
			WriteType: WriteTypeLock,
			StartTS:   ComposeTS(100, 0),
		}

		data := w.Marshal()
		decoded, err := UnmarshalWrite(data)
		require.NoError(t, err)
		assert.Equal(t, WriteTypeLock, decoded.WriteType)
	})
}

func TestWriteNeedValue(t *testing.T) {
	// Put without short value needs CF_DEFAULT lookup
	w := &Write{WriteType: WriteTypePut, StartTS: ComposeTS(100, 0)}
	assert.True(t, w.NeedValue())

	// Put with short value doesn't need CF_DEFAULT
	w.ShortValue = []byte("inline")
	assert.False(t, w.NeedValue())

	// Delete never needs value
	w2 := &Write{WriteType: WriteTypeDelete, StartTS: ComposeTS(100, 0)}
	assert.False(t, w2.NeedValue())

	// Rollback never needs value
	w3 := &Write{WriteType: WriteTypeRollback, StartTS: ComposeTS(100, 0)}
	assert.False(t, w3.NeedValue())
}

func TestWriteIsDataChanged(t *testing.T) {
	assert.True(t, (&Write{WriteType: WriteTypePut}).IsDataChanged())
	assert.True(t, (&Write{WriteType: WriteTypeDelete}).IsDataChanged())
	assert.False(t, (&Write{WriteType: WriteTypeLock}).IsDataChanged())
	assert.False(t, (&Write{WriteType: WriteTypeRollback}).IsDataChanged())
}

func TestLockTypeValues(t *testing.T) {
	// Verify byte values match TiKV exactly
	assert.Equal(t, byte('P'), byte(LockTypePut))
	assert.Equal(t, byte('D'), byte(LockTypeDelete))
	assert.Equal(t, byte('L'), byte(LockTypeLock))
	assert.Equal(t, byte('S'), byte(LockTypePessimistic))
}

func TestWriteTypeValues(t *testing.T) {
	assert.Equal(t, byte('P'), byte(WriteTypePut))
	assert.Equal(t, byte('D'), byte(WriteTypeDelete))
	assert.Equal(t, byte('L'), byte(WriteTypeLock))
	assert.Equal(t, byte('R'), byte(WriteTypeRollback))
}

func TestMutationOp(t *testing.T) {
	assert.Equal(t, MutationOp(0), MutationPut)
	assert.Equal(t, MutationOp(1), MutationDelete)
	assert.Equal(t, MutationOp(2), MutationLock)
	assert.Equal(t, MutationOp(3), MutationInsert)
	assert.Equal(t, MutationOp(4), MutationCheckNotExists)
}

func TestLockEmptyPrimary(t *testing.T) {
	l := &Lock{
		LockType: LockTypePut,
		Primary:  []byte{},
		StartTS:  ComposeTS(100, 0),
		TTL:      1000,
	}

	data := l.Marshal()
	decoded, err := UnmarshalLock(data)
	require.NoError(t, err)
	assert.Equal(t, []byte{}, decoded.Primary)
}

func TestAsyncCommitEmptySecondaries(t *testing.T) {
	l := &Lock{
		LockType:       LockTypePut,
		Primary:        []byte("pk"),
		StartTS:        ComposeTS(100, 0),
		TTL:            1000,
		UseAsyncCommit: true,
		Secondaries:    [][]byte{},
	}

	data := l.Marshal()
	decoded, err := UnmarshalLock(data)
	require.NoError(t, err)
	assert.True(t, decoded.UseAsyncCommit)
	assert.Empty(t, decoded.Secondaries)
}

func TestShortValueMaxLen(t *testing.T) {
	assert.Equal(t, 255, ShortValueMaxLen)

	// A value at exactly max len should be inlineable
	val := make([]byte, ShortValueMaxLen)
	for i := range val {
		val[i] = byte(i)
	}

	w := &Write{
		WriteType:  WriteTypePut,
		StartTS:    ComposeTS(100, 0),
		ShortValue: val,
	}

	data := w.Marshal()
	decoded, err := UnmarshalWrite(data)
	require.NoError(t, err)
	assert.Equal(t, val, decoded.ShortValue)
}
