package coprocessor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Datum Tests ---

func TestDatumIsNull(t *testing.T) {
	assert.True(t, NullDatum().IsNull())
	assert.False(t, Int64Datum(0).IsNull())
	assert.False(t, Float64Datum(0).IsNull())
	assert.False(t, StringDatum("").IsNull())
}

func TestDatumToInt64(t *testing.T) {
	tests := []struct {
		name     string
		datum    Datum
		expected int64
	}{
		{"int64", Int64Datum(42), 42},
		{"uint64", Uint64Datum(100), 100},
		{"float64", Float64Datum(3.7), 3},
		{"null", NullDatum(), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.datum.ToInt64()
			if tt.datum.Kind == KindNull {
				assert.NoError(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, v)
		})
	}
}

func TestDatumToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		datum    Datum
		expected float64
	}{
		{"float64", Float64Datum(3.14), 3.14},
		{"int64", Int64Datum(42), 42.0},
		{"uint64", Uint64Datum(100), 100.0},
		{"null", NullDatum(), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.datum.ToFloat64()
			assert.NoError(t, err)
			assert.InDelta(t, tt.expected, v, 0.001)
		})
	}
}

func TestDatumConstructors(t *testing.T) {
	assert.Equal(t, KindNull, NullDatum().Kind)
	assert.Equal(t, KindInt64, Int64Datum(1).Kind)
	assert.Equal(t, KindUint64, Uint64Datum(1).Kind)
	assert.Equal(t, KindFloat64, Float64Datum(1.0).Kind)
	assert.Equal(t, KindString, StringDatum("hello").Kind)
	assert.Equal(t, KindBytes, BytesDatum([]byte{1, 2, 3}).Kind)
}

// --- RPN Expression Tests ---

func TestRPNEvalConstant(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNConstant, Constant: Int64Datum(42)},
	})
	result, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.I64)
}

func TestRPNEvalColumnRef(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 1},
	})
	row := Row{Int64Datum(10), Int64Datum(20), Int64Datum(30)}
	result, err := expr.Eval(row)
	require.NoError(t, err)
	assert.Equal(t, int64(20), result.I64)
}

func TestRPNEvalColumnRefOutOfRange(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 5},
	})
	row := Row{Int64Datum(10)}
	_, err := expr.Eval(row)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrExprEval))
}

func TestRPNEvalEquality(t *testing.T) {
	// col[0] == 42
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(42)},
		{Type: RPNFuncCall, FuncType: RPNFuncEQ},
	})

	result, err := expr.Eval(Row{Int64Datum(42)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.I64)

	result, err = expr.Eval(Row{Int64Datum(10)})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.I64)
}

func TestRPNEvalComparisons(t *testing.T) {
	tests := []struct {
		name     string
		funcType RPNFuncType
		a, b     int64
		expected int64
	}{
		{"LT true", RPNFuncLT, 1, 2, 1},
		{"LT false", RPNFuncLT, 2, 1, 0},
		{"LE true eq", RPNFuncLE, 2, 2, 1},
		{"LE true lt", RPNFuncLE, 1, 2, 1},
		{"LE false", RPNFuncLE, 3, 2, 0},
		{"GT true", RPNFuncGT, 2, 1, 1},
		{"GT false", RPNFuncGT, 1, 2, 0},
		{"GE true eq", RPNFuncGE, 2, 2, 1},
		{"GE true gt", RPNFuncGE, 3, 2, 1},
		{"GE false", RPNFuncGE, 1, 2, 0},
		{"NE true", RPNFuncNE, 1, 2, 1},
		{"NE false", RPNFuncNE, 2, 2, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := NewRPNExpression([]RPNNode{
				{Type: RPNConstant, Constant: Int64Datum(tt.a)},
				{Type: RPNConstant, Constant: Int64Datum(tt.b)},
				{Type: RPNFuncCall, FuncType: tt.funcType},
			})
			result, err := expr.Eval(Row{})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.I64)
		})
	}
}

func TestRPNEvalArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		funcType RPNFuncType
		a, b     int64
		expected int64
	}{
		{"plus", RPNFuncPlus, 3, 4, 7},
		{"minus", RPNFuncMinus, 10, 3, 7},
		{"mul", RPNFuncMul, 3, 4, 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := NewRPNExpression([]RPNNode{
				{Type: RPNConstant, Constant: Int64Datum(tt.a)},
				{Type: RPNConstant, Constant: Int64Datum(tt.b)},
				{Type: RPNFuncCall, FuncType: tt.funcType},
			})
			result, err := expr.Eval(Row{})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.I64)
		})
	}
}

func TestRPNEvalDivision(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNConstant, Constant: Float64Datum(10.0)},
		{Type: RPNConstant, Constant: Float64Datum(3.0)},
		{Type: RPNFuncCall, FuncType: RPNFuncDiv},
	})
	result, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.InDelta(t, 3.333, result.F64, 0.01)
}

func TestRPNEvalDivisionByZero(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNConstant, Constant: Float64Datum(10.0)},
		{Type: RPNConstant, Constant: Float64Datum(0.0)},
		{Type: RPNFuncCall, FuncType: RPNFuncDiv},
	})
	result, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.True(t, result.IsNull())
}

func TestRPNEvalLogicAnd(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Datum
		expected Datum
	}{
		{"true AND true", Int64Datum(1), Int64Datum(1), Int64Datum(1)},
		{"true AND false", Int64Datum(1), Int64Datum(0), Int64Datum(0)},
		{"false AND true", Int64Datum(0), Int64Datum(1), Int64Datum(0)},
		{"false AND null", Int64Datum(0), NullDatum(), Int64Datum(0)},
		{"null AND false", NullDatum(), Int64Datum(0), Int64Datum(0)},
		{"null AND null", NullDatum(), NullDatum(), NullDatum()},
		{"true AND null", Int64Datum(1), NullDatum(), NullDatum()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := NewRPNExpression([]RPNNode{
				{Type: RPNConstant, Constant: tt.a},
				{Type: RPNConstant, Constant: tt.b},
				{Type: RPNFuncCall, FuncType: RPNFuncAnd},
			})
			result, err := expr.Eval(Row{})
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Kind, result.Kind)
			if !tt.expected.IsNull() {
				assert.Equal(t, tt.expected.I64, result.I64)
			}
		})
	}
}

func TestRPNEvalLogicOr(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Datum
		expected Datum
	}{
		{"true OR false", Int64Datum(1), Int64Datum(0), Int64Datum(1)},
		{"false OR false", Int64Datum(0), Int64Datum(0), Int64Datum(0)},
		{"true OR null", Int64Datum(1), NullDatum(), Int64Datum(1)},
		{"null OR true", NullDatum(), Int64Datum(1), Int64Datum(1)},
		{"null OR null", NullDatum(), NullDatum(), NullDatum()},
		{"false OR null", Int64Datum(0), NullDatum(), NullDatum()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := NewRPNExpression([]RPNNode{
				{Type: RPNConstant, Constant: tt.a},
				{Type: RPNConstant, Constant: tt.b},
				{Type: RPNFuncCall, FuncType: RPNFuncOr},
			})
			result, err := expr.Eval(Row{})
			require.NoError(t, err)
			assert.Equal(t, tt.expected.Kind, result.Kind)
			if !tt.expected.IsNull() {
				assert.Equal(t, tt.expected.I64, result.I64)
			}
		})
	}
}

func TestRPNEvalNot(t *testing.T) {
	tests := []struct {
		input    Datum
		expected Datum
	}{
		{Int64Datum(1), Int64Datum(0)},
		{Int64Datum(0), Int64Datum(1)},
		{NullDatum(), NullDatum()},
	}

	for _, tt := range tests {
		expr := NewRPNExpression([]RPNNode{
			{Type: RPNConstant, Constant: tt.input},
			{Type: RPNFuncCall, FuncType: RPNFuncNot},
		})
		result, err := expr.Eval(Row{})
		require.NoError(t, err)
		assert.Equal(t, tt.expected.Kind, result.Kind)
	}
}

func TestRPNEvalIsNull(t *testing.T) {
	tests := []struct {
		input    Datum
		expected int64
	}{
		{NullDatum(), 1},
		{Int64Datum(42), 0},
		{Float64Datum(3.14), 0},
	}

	for _, tt := range tests {
		expr := NewRPNExpression([]RPNNode{
			{Type: RPNConstant, Constant: tt.input},
			{Type: RPNFuncCall, FuncType: RPNFuncIsNull},
		})
		result, err := expr.Eval(Row{})
		require.NoError(t, err)
		assert.Equal(t, tt.expected, result.I64)
	}
}

func TestRPNEvalComplex(t *testing.T) {
	// (col[0] > 10) AND (col[1] < 50)
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(10)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
		{Type: RPNColumnRef, ColIdx: 1},
		{Type: RPNConstant, Constant: Int64Datum(50)},
		{Type: RPNFuncCall, FuncType: RPNFuncLT},
		{Type: RPNFuncCall, FuncType: RPNFuncAnd},
	})

	// Both true.
	result, err := expr.Eval(Row{Int64Datum(20), Int64Datum(30)})
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.I64)

	// First false.
	result, err = expr.Eval(Row{Int64Datum(5), Int64Datum(30)})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.I64)

	// Second false.
	result, err = expr.Eval(Row{Int64Datum(20), Int64Datum(60)})
	require.NoError(t, err)
	assert.Equal(t, int64(0), result.I64)
}

func TestRPNEvalStackError(t *testing.T) {
	// Invalid: function with empty stack.
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNFuncCall, FuncType: RPNFuncEQ},
	})
	_, err := expr.Eval(Row{})
	assert.Error(t, err)
}

func TestRPNEvalStackImbalance(t *testing.T) {
	// Two constants but no function - stack should have 2 elements.
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNConstant, Constant: Int64Datum(1)},
		{Type: RPNConstant, Constant: Int64Datum(2)},
	})
	_, err := expr.Eval(Row{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stack has 2 elements")
}

func TestRPNComparisonWithNull(t *testing.T) {
	expr := NewRPNExpression([]RPNNode{
		{Type: RPNConstant, Constant: NullDatum()},
		{Type: RPNConstant, Constant: Int64Datum(42)},
		{Type: RPNFuncCall, FuncType: RPNFuncEQ},
	})
	result, err := expr.Eval(Row{})
	require.NoError(t, err)
	assert.True(t, result.IsNull())
}

// --- SelectionExecutor Tests ---

type mockExecutor struct {
	batches []*BatchResult
	idx     int
	colCount int
}

func newMockExecutor(colCount int, batches ...*BatchResult) *mockExecutor {
	return &mockExecutor{batches: batches, colCount: colCount}
}

func (m *mockExecutor) ColumnCount() int { return m.colCount }

func (m *mockExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	if m.idx >= len(m.batches) {
		return &BatchResult{IsDrained: true}, nil
	}
	b := m.batches[m.idx]
	m.idx++
	return b, nil
}

func TestSelectionExecutorFilterAll(t *testing.T) {
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(10)},
				{Int64Datum(2), Int64Datum(20)},
				{Int64Datum(3), Int64Datum(30)},
			},
			IsDrained: true,
		},
	)

	// Filter: col[0] > 5 (no rows pass)
	pred := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(5)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
	})

	sel := NewSelectionExecutor(source, []*RPNExpression{pred})
	result, err := sel.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Empty(t, result.Rows)
	assert.True(t, result.IsDrained)
}

func TestSelectionExecutorFilterSome(t *testing.T) {
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(10)},
				{Int64Datum(5), Int64Datum(50)},
				{Int64Datum(3), Int64Datum(30)},
				{Int64Datum(8), Int64Datum(80)},
			},
			IsDrained: true,
		},
	)

	// Filter: col[0] > 3
	pred := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(3)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
	})

	sel := NewSelectionExecutor(source, []*RPNExpression{pred})
	result, err := sel.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2)
	assert.Equal(t, int64(5), result.Rows[0][0].I64)
	assert.Equal(t, int64(8), result.Rows[1][0].I64)
}

func TestSelectionExecutorMultiplePredicates(t *testing.T) {
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(100)},
				{Int64Datum(5), Int64Datum(50)},
				{Int64Datum(3), Int64Datum(30)},
				{Int64Datum(8), Int64Datum(80)},
			},
			IsDrained: true,
		},
	)

	// Predicate 1: col[0] > 2
	pred1 := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(2)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
	})
	// Predicate 2: col[1] < 70
	pred2 := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 1},
		{Type: RPNConstant, Constant: Int64Datum(70)},
		{Type: RPNFuncCall, FuncType: RPNFuncLT},
	})

	sel := NewSelectionExecutor(source, []*RPNExpression{pred1, pred2})
	result, err := sel.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 2) // (5,50) and (3,30)
}

func TestSelectionColumnCount(t *testing.T) {
	source := newMockExecutor(5)
	sel := NewSelectionExecutor(source, nil)
	assert.Equal(t, 5, sel.ColumnCount())
}

// --- LimitExecutor Tests ---

func TestLimitExecutor(t *testing.T) {
	source := newMockExecutor(1,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1)},
				{Int64Datum(2)},
				{Int64Datum(3)},
				{Int64Datum(4)},
				{Int64Datum(5)},
			},
			IsDrained: true,
		},
	)

	limit := NewLimitExecutor(source, 3)
	result, err := limit.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 3)
	assert.True(t, result.IsDrained)
}

func TestLimitExecutorAlreadyDrained(t *testing.T) {
	source := newMockExecutor(1,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1)},
			},
			IsDrained: true,
		},
	)

	limit := NewLimitExecutor(source, 3)

	result, err := limit.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	result, err = limit.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.True(t, result.IsDrained)
}

func TestLimitExecutorColumnCount(t *testing.T) {
	source := newMockExecutor(3)
	limit := NewLimitExecutor(source, 10)
	assert.Equal(t, 3, limit.ColumnCount())
}

// --- Aggregation Tests ---

func TestAggrStateCount(t *testing.T) {
	state := NewAggrState(AggrCount)
	state.Update(Int64Datum(1))
	state.Update(Int64Datum(2))
	state.Update(Int64Datum(3))
	state.Update(NullDatum()) // NULLs not counted

	result := state.Result()
	assert.Equal(t, KindInt64, result.Kind)
	assert.Equal(t, int64(3), result.I64)
}

func TestAggrStateSum(t *testing.T) {
	state := NewAggrState(AggrSum)
	state.Update(Int64Datum(10))
	state.Update(Int64Datum(20))
	state.Update(Int64Datum(30))

	result := state.Result()
	assert.Equal(t, KindFloat64, result.Kind)
	assert.InDelta(t, 60.0, result.F64, 0.001)
}

func TestAggrStateSumEmpty(t *testing.T) {
	state := NewAggrState(AggrSum)
	result := state.Result()
	assert.True(t, result.IsNull())
}

func TestAggrStateAvg(t *testing.T) {
	state := NewAggrState(AggrAvg)
	state.Update(Int64Datum(10))
	state.Update(Int64Datum(20))
	state.Update(Int64Datum(30))

	result := state.Result()
	assert.Equal(t, KindFloat64, result.Kind)
	assert.InDelta(t, 20.0, result.F64, 0.001)
}

func TestAggrStateAvgEmpty(t *testing.T) {
	state := NewAggrState(AggrAvg)
	result := state.Result()
	assert.True(t, result.IsNull())
}

func TestAggrStateMin(t *testing.T) {
	state := NewAggrState(AggrMin)
	state.Update(Int64Datum(30))
	state.Update(Int64Datum(10))
	state.Update(Int64Datum(20))

	result := state.Result()
	assert.Equal(t, int64(10), result.I64)
}

func TestAggrStateMinEmpty(t *testing.T) {
	state := NewAggrState(AggrMin)
	result := state.Result()
	assert.True(t, result.IsNull())
}

func TestAggrStateMax(t *testing.T) {
	state := NewAggrState(AggrMax)
	state.Update(Int64Datum(10))
	state.Update(Int64Datum(30))
	state.Update(Int64Datum(20))

	result := state.Result()
	assert.Equal(t, int64(30), result.I64)
}

func TestAggrStateMaxEmpty(t *testing.T) {
	state := NewAggrState(AggrMax)
	result := state.Result()
	assert.True(t, result.IsNull())
}

// --- SimpleAggrExecutor Tests ---

func TestSimpleAggrExecutor(t *testing.T) {
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(10)},
				{Int64Datum(2), Int64Datum(20)},
				{Int64Datum(3), Int64Datum(30)},
			},
			IsDrained: true,
		},
	)

	aggr := NewSimpleAggrExecutor(source, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1}, // COUNT(*)
		{FuncType: AggrSum, ColIdx: 1},    // SUM(col[1])
		{FuncType: AggrAvg, ColIdx: 1},    // AVG(col[1])
	})

	result, err := aggr.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.True(t, result.IsDrained)

	row := result.Rows[0]
	assert.Equal(t, int64(3), row[0].I64)          // COUNT = 3
	assert.InDelta(t, 60.0, row[1].F64, 0.001)     // SUM = 60
	assert.InDelta(t, 20.0, row[2].F64, 0.001)     // AVG = 20
}

func TestSimpleAggrExecutorColumnCount(t *testing.T) {
	source := newMockExecutor(2)
	aggr := NewSimpleAggrExecutor(source, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1},
		{FuncType: AggrSum, ColIdx: 1},
	})
	assert.Equal(t, 2, aggr.ColumnCount())
}

func TestSimpleAggrExecutorDoubleCall(t *testing.T) {
	source := newMockExecutor(1,
		&BatchResult{
			Rows:      []Row{{Int64Datum(1)}},
			IsDrained: true,
		},
	)

	aggr := NewSimpleAggrExecutor(source, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1},
	})

	result, err := aggr.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.Len(t, result.Rows, 1)

	// Second call should be drained.
	result, err = aggr.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.True(t, result.IsDrained)
	assert.Empty(t, result.Rows)
}

// --- HashAggrExecutor Tests ---

func TestHashAggrExecutor(t *testing.T) {
	source := newMockExecutor(3,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), StringDatum("a"), Int64Datum(10)},
				{Int64Datum(2), StringDatum("b"), Int64Datum(20)},
				{Int64Datum(3), StringDatum("a"), Int64Datum(30)},
				{Int64Datum(4), StringDatum("b"), Int64Datum(40)},
			},
			IsDrained: true,
		},
	)

	aggr := NewHashAggrExecutor(source, []int{1}, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1}, // COUNT(*)
		{FuncType: AggrSum, ColIdx: 2},    // SUM(col[2])
	})

	result, err := aggr.NextBatch(context.Background(), 100)
	require.NoError(t, err)
	assert.True(t, result.IsDrained)
	assert.Len(t, result.Rows, 2) // Two groups: "a" and "b"

	// Find group "a" and "b" (order not guaranteed with map).
	for _, row := range result.Rows {
		groupKey := row[0].Str
		switch groupKey {
		case "a":
			assert.Equal(t, int64(2), row[1].I64)          // COUNT = 2
			assert.InDelta(t, 40.0, row[2].F64, 0.001)     // SUM = 10+30 = 40
		case "b":
			assert.Equal(t, int64(2), row[1].I64)          // COUNT = 2
			assert.InDelta(t, 60.0, row[2].F64, 0.001)     // SUM = 20+40 = 60
		default:
			t.Fatalf("unexpected group key: %s", groupKey)
		}
	}
}

func TestHashAggrExecutorColumnCount(t *testing.T) {
	source := newMockExecutor(3)
	aggr := NewHashAggrExecutor(source, []int{0}, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1},
		{FuncType: AggrSum, ColIdx: 2},
	})
	assert.Equal(t, 3, aggr.ColumnCount()) // 1 group col + 2 aggr cols
}

// --- ExecutorsRunner Tests ---

func TestExecutorsRunnerBasic(t *testing.T) {
	source := newMockExecutor(1,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1)},
				{Int64Datum(2)},
			},
			IsDrained: false,
		},
		&BatchResult{
			Rows: []Row{
				{Int64Datum(3)},
			},
			IsDrained: true,
		},
	)

	runner := NewExecutorsRunner(source, time.Time{})
	rows, err := runner.Run(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 3)
}

func TestExecutorsRunnerDeadline(t *testing.T) {
	// Create a source that never drains.
	source := &infiniteExecutor{colCount: 1}

	deadline := time.Now().Add(50 * time.Millisecond)
	runner := NewExecutorsRunner(source, deadline)
	_, err := runner.Run(context.Background())
	assert.ErrorIs(t, err, ErrDeadlineExceeded)
}

type infiniteExecutor struct {
	colCount int
}

func (e *infiniteExecutor) ColumnCount() int { return e.colCount }

func (e *infiniteExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	rows := make([]Row, batchSize)
	for i := range rows {
		rows[i] = Row{Int64Datum(int64(i))}
	}
	return &BatchResult{Rows: rows, IsDrained: false}, nil
}

// --- Pipeline composition Tests ---

func TestPipelineTableScanSelection(t *testing.T) {
	// Simulate a pipeline: source -> selection -> limit
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(100)},
				{Int64Datum(5), Int64Datum(500)},
				{Int64Datum(3), Int64Datum(300)},
				{Int64Datum(7), Int64Datum(700)},
				{Int64Datum(2), Int64Datum(200)},
			},
			IsDrained: true,
		},
	)

	// Filter: col[0] > 2
	pred := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(2)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
	})
	sel := NewSelectionExecutor(source, []*RPNExpression{pred})
	limit := NewLimitExecutor(sel, 2)

	runner := NewExecutorsRunner(limit, time.Time{})
	rows, err := runner.Run(context.Background())
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}

func TestPipelineSelectionAggr(t *testing.T) {
	// Pipeline: source -> selection -> aggregation
	source := newMockExecutor(2,
		&BatchResult{
			Rows: []Row{
				{Int64Datum(1), Int64Datum(10)},
				{Int64Datum(5), Int64Datum(50)},
				{Int64Datum(3), Int64Datum(30)},
			},
			IsDrained: true,
		},
	)

	// Filter: col[0] > 2
	pred := NewRPNExpression([]RPNNode{
		{Type: RPNColumnRef, ColIdx: 0},
		{Type: RPNConstant, Constant: Int64Datum(2)},
		{Type: RPNFuncCall, FuncType: RPNFuncGT},
	})
	sel := NewSelectionExecutor(source, []*RPNExpression{pred})
	aggr := NewSimpleAggrExecutor(sel, []AggrDef{
		{FuncType: AggrCount, ColIdx: -1},
		{FuncType: AggrSum, ColIdx: 1},
	})

	runner := NewExecutorsRunner(aggr, time.Time{})
	rows, err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(2), rows[0][0].I64)          // COUNT = 2 (rows with col[0] > 2)
	assert.InDelta(t, 80.0, rows[0][1].F64, 0.001)     // SUM = 50+30 = 80
}
