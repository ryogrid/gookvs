// Package coprocessor implements the push-down query execution framework for gookvs.
// It provides a DAG executor pipeline with TableScan, Selection (filter),
// RPN expression evaluation, and aggregation support.
package coprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

var (
	// ErrUnsupportedExecutor is returned for executor types we don't support.
	ErrUnsupportedExecutor = errors.New("coprocessor: unsupported executor type")
	// ErrDeadlineExceeded is returned when execution exceeds the deadline.
	ErrDeadlineExceeded = errors.New("coprocessor: deadline exceeded")
	// ErrExprEval is returned on expression evaluation errors.
	ErrExprEval = errors.New("coprocessor: expression evaluation error")
)

// --- Column and Row types ---

// Datum represents a typed value in coprocessor evaluation.
type Datum struct {
	Kind DatumKind
	I64  int64
	U64  uint64
	F64  float64
	Str  string
	Buf  []byte
}

// DatumKind identifies the type of a Datum.
type DatumKind int

const (
	KindNull    DatumKind = iota
	KindInt64
	KindUint64
	KindFloat64
	KindString
	KindBytes
)

// IsNull returns true if the datum is null.
func (d Datum) IsNull() bool {
	return d.Kind == KindNull
}

// ToInt64 converts the datum to int64.
func (d Datum) ToInt64() (int64, error) {
	switch d.Kind {
	case KindInt64:
		return d.I64, nil
	case KindUint64:
		return int64(d.U64), nil
	case KindFloat64:
		return int64(d.F64), nil
	case KindNull:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %v to int64", d.Kind)
	}
}

// ToFloat64 converts the datum to float64.
func (d Datum) ToFloat64() (float64, error) {
	switch d.Kind {
	case KindFloat64:
		return d.F64, nil
	case KindInt64:
		return float64(d.I64), nil
	case KindUint64:
		return float64(d.U64), nil
	case KindNull:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %v to float64", d.Kind)
	}
}

// NullDatum creates a null datum.
func NullDatum() Datum { return Datum{Kind: KindNull} }

// Int64Datum creates an int64 datum.
func Int64Datum(v int64) Datum { return Datum{Kind: KindInt64, I64: v} }

// Uint64Datum creates a uint64 datum.
func Uint64Datum(v uint64) Datum { return Datum{Kind: KindUint64, U64: v} }

// Float64Datum creates a float64 datum.
func Float64Datum(v float64) Datum { return Datum{Kind: KindFloat64, F64: v} }

// StringDatum creates a string datum.
func StringDatum(v string) Datum { return Datum{Kind: KindString, Str: v} }

// BytesDatum creates a bytes datum.
func BytesDatum(v []byte) Datum { return Datum{Kind: KindBytes, Buf: v} }

// Row represents a row of data.
type Row []Datum

// --- Batch Result ---

// BatchResult is the output of a BatchExecutor.NextBatch call.
type BatchResult struct {
	Rows      []Row
	IsDrained bool
}

// --- BatchExecutor Interface ---

// BatchExecutor is the core interface for all coprocessor executors.
type BatchExecutor interface {
	// NextBatch pulls the next batch of rows.
	NextBatch(ctx context.Context, batchSize int) (*BatchResult, error)

	// Schema returns the number of output columns.
	ColumnCount() int
}

// --- KeyRange ---

// KeyRange represents a key range to scan.
type KeyRange struct {
	Start []byte
	End   []byte
}

// --- TableScanExecutor ---

// TableScanExecutor scans base table rows from CF_WRITE and CF_DEFAULT.
type TableScanExecutor struct {
	snap       traits.Snapshot
	ranges     []KeyRange
	version    txntypes.TimeStamp
	colCount   int
	currentRange int
	iter       traits.Iterator
	finished   bool
	desc       bool // Descending scan
}

// NewTableScanExecutor creates a TableScanExecutor.
func NewTableScanExecutor(snap traits.Snapshot, ranges []KeyRange, version txntypes.TimeStamp, colCount int, desc bool) *TableScanExecutor {
	return &TableScanExecutor{
		snap:     snap,
		ranges:   ranges,
		version:  version,
		colCount: colCount,
		desc:     desc,
	}
}

func (e *TableScanExecutor) ColumnCount() int { return e.colCount }

func (e *TableScanExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	if e.finished {
		return &BatchResult{IsDrained: true}, nil
	}

	var rows []Row

	for len(rows) < batchSize {
		if err := ctx.Err(); err != nil {
			return nil, ErrDeadlineExceeded
		}

		if e.iter == nil {
			if e.currentRange >= len(e.ranges) {
				e.finished = true
				return &BatchResult{Rows: rows, IsDrained: true}, nil
			}
			kr := e.ranges[e.currentRange]
			e.iter = e.snap.NewIterator(cfnames.CFWrite, traits.IterOptions{
				LowerBound: kr.Start,
				UpperBound: kr.End,
			})
			if e.desc {
				e.iter.SeekToLast()
			} else {
				e.iter.SeekToFirst()
			}
		}

		if !e.iter.Valid() {
			e.iter.Close()
			e.iter = nil
			e.currentRange++
			continue
		}

		// Read the key and decode it.
		encodedKey := e.iter.Key()
		userKey, commitTS, err := mvcc.DecodeKey(encodedKey)
		if err != nil {
			if e.desc {
				e.iter.Prev()
			} else {
				e.iter.Next()
			}
			continue
		}

		// Only consider writes at or before our version.
		if commitTS > e.version {
			if e.desc {
				e.iter.Prev()
			} else {
				e.iter.Next()
			}
			continue
		}

		// Read the write record to get the value.
		writeData := e.iter.Value()
		write, err := txntypes.UnmarshalWrite(writeData)
		if err != nil || write.WriteType == txntypes.WriteTypeRollback || write.WriteType == txntypes.WriteTypeLock {
			if e.desc {
				e.iter.Prev()
			} else {
				e.iter.Next()
			}
			continue
		}

		// Skip delete writes.
		if write.WriteType == txntypes.WriteTypeDelete {
			if e.desc {
				e.iter.Prev()
			} else {
				e.iter.Next()
			}
			continue
		}

		// Get the actual value.
		var value []byte
		if write.ShortValue != nil {
			value = write.ShortValue
		} else {
			// Read from CF_DEFAULT.
			defaultKey := mvcc.EncodeKey(userKey, write.StartTS)
			val, err := e.snap.Get(cfnames.CFDefault, defaultKey)
			if err != nil {
				if e.desc {
					e.iter.Prev()
				} else {
					e.iter.Next()
				}
				continue
			}
			value = val
		}

		// Build row from key and value.
		row := buildRow(userKey, value, e.colCount)
		rows = append(rows, row)

		// Skip to the next user key (avoid reading older versions).
		if e.desc {
			e.iter.Prev()
		} else {
			e.iter.Next()
		}
		// Skip duplicate user keys (older versions).
		for e.iter.Valid() {
			nextEncoded := e.iter.Key()
			nextUserKey, _, err := mvcc.DecodeKey(nextEncoded)
			if err != nil {
				break
			}
			if !bytes.Equal(nextUserKey, userKey) {
				break
			}
			if e.desc {
				e.iter.Prev()
			} else {
				e.iter.Next()
			}
		}
	}

	return &BatchResult{Rows: rows, IsDrained: false}, nil
}

// buildRow constructs a Row from a key and value.
// For simplicity, column 0 = key (as bytes), column 1 = value (as bytes),
// additional columns are decoded from the value if it looks like a simple encoding.
func buildRow(key, value []byte, colCount int) Row {
	row := make(Row, colCount)

	if colCount >= 1 {
		row[0] = BytesDatum(key)
	}
	if colCount >= 2 {
		row[1] = BytesDatum(value)
	}

	// Try to decode additional columns from the value.
	// Simple encoding: each column is an 8-byte int64 in big-endian.
	if colCount > 2 && len(value) >= (colCount-2)*8 {
		for i := 2; i < colCount; i++ {
			offset := (i - 2) * 8
			if offset+8 <= len(value) {
				v := int64(binary.BigEndian.Uint64(value[offset : offset+8]))
				row[i] = Int64Datum(v)
			}
		}
	}

	return row
}

// --- SelectionExecutor ---

// SelectionExecutor filters rows based on RPN predicate expressions.
type SelectionExecutor struct {
	source     BatchExecutor
	predicates []*RPNExpression
}

// NewSelectionExecutor creates a SelectionExecutor.
func NewSelectionExecutor(source BatchExecutor, predicates []*RPNExpression) *SelectionExecutor {
	return &SelectionExecutor{
		source:     source,
		predicates: predicates,
	}
}

func (e *SelectionExecutor) ColumnCount() int { return e.source.ColumnCount() }

func (e *SelectionExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	result, err := e.source.NextBatch(ctx, batchSize)
	if err != nil {
		return nil, err
	}

	var filtered []Row
	for _, row := range result.Rows {
		pass := true
		for _, pred := range e.predicates {
			val, err := pred.Eval(row)
			if err != nil {
				pass = false
				break
			}
			if val.IsNull() || val.I64 == 0 {
				pass = false
				break
			}
		}
		if pass {
			filtered = append(filtered, row)
		}
	}

	return &BatchResult{Rows: filtered, IsDrained: result.IsDrained}, nil
}

// --- LimitExecutor ---

// LimitExecutor implements LIMIT/OFFSET.
type LimitExecutor struct {
	source  BatchExecutor
	limit   int
	emitted int
}

// NewLimitExecutor creates a LimitExecutor.
func NewLimitExecutor(source BatchExecutor, limit int) *LimitExecutor {
	return &LimitExecutor{
		source: source,
		limit:  limit,
	}
}

func (e *LimitExecutor) ColumnCount() int { return e.source.ColumnCount() }

func (e *LimitExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	if e.emitted >= e.limit {
		return &BatchResult{IsDrained: true}, nil
	}

	remaining := e.limit - e.emitted
	if batchSize > remaining {
		batchSize = remaining
	}

	result, err := e.source.NextBatch(ctx, batchSize)
	if err != nil {
		return nil, err
	}

	if len(result.Rows)+e.emitted > e.limit {
		result.Rows = result.Rows[:e.limit-e.emitted]
		result.IsDrained = true
	}

	e.emitted += len(result.Rows)
	if e.emitted >= e.limit {
		result.IsDrained = true
	}

	return result, nil
}

// --- RPN Expression ---

// RPNNodeType identifies the type of an RPN node.
type RPNNodeType int

const (
	RPNColumnRef RPNNodeType = iota
	RPNConstant
	RPNFuncCall
)

// RPNFuncType identifies a built-in function.
type RPNFuncType int

const (
	RPNFuncEQ     RPNFuncType = iota // =
	RPNFuncLT                        // <
	RPNFuncLE                        // <=
	RPNFuncGT                        // >
	RPNFuncGE                        // >=
	RPNFuncNE                        // !=
	RPNFuncAnd                       // AND
	RPNFuncOr                        // OR
	RPNFuncNot                       // NOT
	RPNFuncPlus                      // +
	RPNFuncMinus                     // -
	RPNFuncMul                       // *
	RPNFuncDiv                       // /
	RPNFuncIsNull                    // IS NULL
)

// RPNNode is a single node in an RPN expression.
type RPNNode struct {
	Type     RPNNodeType
	ColIdx   int       // For RPNColumnRef
	Constant Datum     // For RPNConstant
	FuncType RPNFuncType // For RPNFuncCall
}

// RPNExpression represents a flat postfix expression for stack-based evaluation.
type RPNExpression struct {
	Nodes []RPNNode
}

// NewRPNExpression creates a new RPN expression with the given nodes.
func NewRPNExpression(nodes []RPNNode) *RPNExpression {
	return &RPNExpression{Nodes: nodes}
}

// Eval evaluates the RPN expression against a row and returns the result datum.
func (expr *RPNExpression) Eval(row Row) (Datum, error) {
	stack := make([]Datum, 0, len(expr.Nodes))

	for _, node := range expr.Nodes {
		switch node.Type {
		case RPNColumnRef:
			if node.ColIdx >= len(row) {
				return NullDatum(), fmt.Errorf("%w: column index %d out of range", ErrExprEval, node.ColIdx)
			}
			stack = append(stack, row[node.ColIdx])

		case RPNConstant:
			stack = append(stack, node.Constant)

		case RPNFuncCall:
			result, err := evalFunc(node.FuncType, stack)
			if err != nil {
				return NullDatum(), err
			}
			stack = result
		}
	}

	if len(stack) != 1 {
		return NullDatum(), fmt.Errorf("%w: stack has %d elements, expected 1", ErrExprEval, len(stack))
	}
	return stack[0], nil
}

func evalFunc(funcType RPNFuncType, stack []Datum) ([]Datum, error) {
	switch funcType {
	case RPNFuncNot:
		if len(stack) < 1 {
			return nil, fmt.Errorf("%w: NOT requires 1 operand", ErrExprEval)
		}
		a := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if a.IsNull() {
			stack = append(stack, NullDatum())
		} else {
			v, _ := a.ToInt64()
			if v == 0 {
				stack = append(stack, Int64Datum(1))
			} else {
				stack = append(stack, Int64Datum(0))
			}
		}
		return stack, nil

	case RPNFuncIsNull:
		if len(stack) < 1 {
			return nil, fmt.Errorf("%w: IS NULL requires 1 operand", ErrExprEval)
		}
		a := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if a.IsNull() {
			stack = append(stack, Int64Datum(1))
		} else {
			stack = append(stack, Int64Datum(0))
		}
		return stack, nil

	default:
		// Binary operations.
		if len(stack) < 2 {
			return nil, fmt.Errorf("%w: binary op requires 2 operands", ErrExprEval)
		}
		b := stack[len(stack)-1]
		a := stack[len(stack)-2]
		stack = stack[:len(stack)-2]

		result, err := evalBinaryOp(funcType, a, b)
		if err != nil {
			return nil, err
		}
		stack = append(stack, result)
		return stack, nil
	}
}

func evalBinaryOp(funcType RPNFuncType, a, b Datum) (Datum, error) {
	if a.IsNull() || b.IsNull() {
		if funcType == RPNFuncAnd || funcType == RPNFuncOr {
			return evalLogicWithNull(funcType, a, b)
		}
		return NullDatum(), nil
	}

	switch funcType {
	case RPNFuncEQ, RPNFuncLT, RPNFuncLE, RPNFuncGT, RPNFuncGE, RPNFuncNE:
		return evalComparison(funcType, a, b)
	case RPNFuncAnd:
		av, _ := a.ToInt64()
		bv, _ := b.ToInt64()
		if av != 0 && bv != 0 {
			return Int64Datum(1), nil
		}
		return Int64Datum(0), nil
	case RPNFuncOr:
		av, _ := a.ToInt64()
		bv, _ := b.ToInt64()
		if av != 0 || bv != 0 {
			return Int64Datum(1), nil
		}
		return Int64Datum(0), nil
	case RPNFuncPlus:
		return evalArith(a, b, func(x, y float64) float64 { return x + y })
	case RPNFuncMinus:
		return evalArith(a, b, func(x, y float64) float64 { return x - y })
	case RPNFuncMul:
		return evalArith(a, b, func(x, y float64) float64 { return x * y })
	case RPNFuncDiv:
		bv, _ := b.ToFloat64()
		if bv == 0 {
			return NullDatum(), nil // Division by zero returns NULL.
		}
		return evalArith(a, b, func(x, y float64) float64 { return x / y })
	default:
		return NullDatum(), fmt.Errorf("%w: unknown func type %d", ErrExprEval, funcType)
	}
}

func evalLogicWithNull(funcType RPNFuncType, a, b Datum) (Datum, error) {
	if funcType == RPNFuncAnd {
		// FALSE AND NULL = FALSE, NULL AND FALSE = FALSE
		if !a.IsNull() {
			av, _ := a.ToInt64()
			if av == 0 {
				return Int64Datum(0), nil
			}
		}
		if !b.IsNull() {
			bv, _ := b.ToInt64()
			if bv == 0 {
				return Int64Datum(0), nil
			}
		}
		return NullDatum(), nil
	}
	// OR
	if !a.IsNull() {
		av, _ := a.ToInt64()
		if av != 0 {
			return Int64Datum(1), nil
		}
	}
	if !b.IsNull() {
		bv, _ := b.ToInt64()
		if bv != 0 {
			return Int64Datum(1), nil
		}
	}
	return NullDatum(), nil
}

func evalComparison(funcType RPNFuncType, a, b Datum) (Datum, error) {
	af, _ := a.ToFloat64()
	bf, _ := b.ToFloat64()

	var result bool
	switch funcType {
	case RPNFuncEQ:
		result = af == bf
	case RPNFuncLT:
		result = af < bf
	case RPNFuncLE:
		result = af <= bf
	case RPNFuncGT:
		result = af > bf
	case RPNFuncGE:
		result = af >= bf
	case RPNFuncNE:
		result = af != bf
	}

	if result {
		return Int64Datum(1), nil
	}
	return Int64Datum(0), nil
}

func evalArith(a, b Datum, op func(float64, float64) float64) (Datum, error) {
	// If both are int64, try int64 arithmetic.
	if a.Kind == KindInt64 && b.Kind == KindInt64 {
		af := float64(a.I64)
		bf := float64(b.I64)
		result := op(af, bf)
		if result == math.Trunc(result) && result >= math.MinInt64 && result <= math.MaxInt64 {
			return Int64Datum(int64(result)), nil
		}
		return Float64Datum(result), nil
	}

	af, _ := a.ToFloat64()
	bf, _ := b.ToFloat64()
	return Float64Datum(op(af, bf)), nil
}

// --- Aggregation ---

// AggrFuncType identifies an aggregation function.
type AggrFuncType int

const (
	AggrCount AggrFuncType = iota
	AggrSum
	AggrMin
	AggrMax
	AggrAvg
)

// AggrState holds the state for one aggregation function.
type AggrState struct {
	FuncType AggrFuncType
	Count    int64
	Sum      float64
	Min      Datum
	Max      Datum
	HasValue bool
}

// NewAggrState creates a new aggregation state.
func NewAggrState(funcType AggrFuncType) *AggrState {
	return &AggrState{FuncType: funcType}
}

// Update feeds a new value into the aggregation.
func (s *AggrState) Update(d Datum) {
	if d.IsNull() {
		return
	}

	s.Count++

	switch s.FuncType {
	case AggrCount:
		// Just count (already incremented above).

	case AggrSum, AggrAvg:
		v, _ := d.ToFloat64()
		s.Sum += v

	case AggrMin:
		v, _ := d.ToFloat64()
		if !s.HasValue || v < s.Sum {
			s.Sum = v
			s.Min = d
		}
		s.HasValue = true

	case AggrMax:
		v, _ := d.ToFloat64()
		if !s.HasValue || v > s.Sum {
			s.Sum = v
			s.Max = d
		}
		s.HasValue = true
	}
}

// Result returns the final aggregation result.
func (s *AggrState) Result() Datum {
	switch s.FuncType {
	case AggrCount:
		return Int64Datum(s.Count)
	case AggrSum:
		if s.Count == 0 {
			return NullDatum()
		}
		return Float64Datum(s.Sum)
	case AggrAvg:
		if s.Count == 0 {
			return NullDatum()
		}
		return Float64Datum(s.Sum / float64(s.Count))
	case AggrMin:
		if !s.HasValue {
			return NullDatum()
		}
		return s.Min
	case AggrMax:
		if !s.HasValue {
			return NullDatum()
		}
		return s.Max
	default:
		return NullDatum()
	}
}

// --- SimpleAggrExecutor ---

// SimpleAggrExecutor performs non-grouped aggregation.
type SimpleAggrExecutor struct {
	source   BatchExecutor
	aggrDefs []AggrDef
	states   []*AggrState
	done     bool
}

// AggrDef defines an aggregation function and its input column.
type AggrDef struct {
	FuncType AggrFuncType
	ColIdx   int // Input column index (-1 for COUNT(*))
}

// NewSimpleAggrExecutor creates a SimpleAggrExecutor.
func NewSimpleAggrExecutor(source BatchExecutor, aggrDefs []AggrDef) *SimpleAggrExecutor {
	states := make([]*AggrState, len(aggrDefs))
	for i, def := range aggrDefs {
		states[i] = NewAggrState(def.FuncType)
	}
	return &SimpleAggrExecutor{
		source:   source,
		aggrDefs: aggrDefs,
		states:   states,
	}
}

func (e *SimpleAggrExecutor) ColumnCount() int { return len(e.aggrDefs) }

func (e *SimpleAggrExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	if e.done {
		return &BatchResult{IsDrained: true}, nil
	}

	// Consume all input.
	for {
		result, err := e.source.NextBatch(ctx, 1024)
		if err != nil {
			return nil, err
		}

		for _, row := range result.Rows {
			for i, def := range e.aggrDefs {
				if def.ColIdx < 0 {
					// COUNT(*) — count all rows.
					e.states[i].Update(Int64Datum(1))
				} else if def.ColIdx < len(row) {
					e.states[i].Update(row[def.ColIdx])
				}
			}
		}

		if result.IsDrained {
			break
		}
	}

	// Produce the single output row.
	row := make(Row, len(e.states))
	for i, state := range e.states {
		row[i] = state.Result()
	}

	e.done = true
	return &BatchResult{
		Rows:      []Row{row},
		IsDrained: true,
	}, nil
}

// --- HashAggrExecutor ---

// HashAggrExecutor performs hash-based GROUP BY aggregation.
type HashAggrExecutor struct {
	source    BatchExecutor
	groupCols []int
	aggrDefs  []AggrDef
	groups    map[string]*hashAggrGroup
	done      bool
}

type hashAggrGroup struct {
	key    Row
	states []*AggrState
}

// NewHashAggrExecutor creates a HashAggrExecutor.
func NewHashAggrExecutor(source BatchExecutor, groupCols []int, aggrDefs []AggrDef) *HashAggrExecutor {
	return &HashAggrExecutor{
		source:    source,
		groupCols: groupCols,
		aggrDefs:  aggrDefs,
		groups:    make(map[string]*hashAggrGroup),
	}
}

func (e *HashAggrExecutor) ColumnCount() int { return len(e.groupCols) + len(e.aggrDefs) }

func (e *HashAggrExecutor) NextBatch(ctx context.Context, batchSize int) (*BatchResult, error) {
	if e.done {
		return &BatchResult{IsDrained: true}, nil
	}

	// Consume all input.
	for {
		result, err := e.source.NextBatch(ctx, 1024)
		if err != nil {
			return nil, err
		}

		for _, row := range result.Rows {
			groupKey := e.buildGroupKey(row)
			group, ok := e.groups[groupKey]
			if !ok {
				groupRow := make(Row, len(e.groupCols))
				for i, col := range e.groupCols {
					if col < len(row) {
						groupRow[i] = row[col]
					}
				}
				states := make([]*AggrState, len(e.aggrDefs))
				for i, def := range e.aggrDefs {
					states[i] = NewAggrState(def.FuncType)
				}
				group = &hashAggrGroup{key: groupRow, states: states}
				e.groups[groupKey] = group
			}

			for i, def := range e.aggrDefs {
				if def.ColIdx < 0 {
					group.states[i].Update(Int64Datum(1))
				} else if def.ColIdx < len(row) {
					group.states[i].Update(row[def.ColIdx])
				}
			}
		}

		if result.IsDrained {
			break
		}
	}

	// Produce output rows.
	rows := make([]Row, 0, len(e.groups))
	for _, group := range e.groups {
		row := make(Row, len(e.groupCols)+len(e.aggrDefs))
		copy(row, group.key)
		for i, state := range group.states {
			row[len(e.groupCols)+i] = state.Result()
		}
		rows = append(rows, row)
	}

	e.done = true
	return &BatchResult{Rows: rows, IsDrained: true}, nil
}

func (e *HashAggrExecutor) buildGroupKey(row Row) string {
	var buf bytes.Buffer
	for _, col := range e.groupCols {
		if col >= len(row) {
			buf.WriteString("NULL|")
			continue
		}
		d := row[col]
		switch d.Kind {
		case KindNull:
			buf.WriteString("NULL|")
		case KindInt64:
			fmt.Fprintf(&buf, "%d|", d.I64)
		case KindUint64:
			fmt.Fprintf(&buf, "%d|", d.U64)
		case KindFloat64:
			fmt.Fprintf(&buf, "%f|", d.F64)
		case KindString:
			fmt.Fprintf(&buf, "%s|", d.Str)
		case KindBytes:
			fmt.Fprintf(&buf, "%x|", d.Buf)
		}
	}
	return buf.String()
}

// --- Endpoint ---

// Endpoint handles coprocessor requests.
type Endpoint struct {
	engine traits.KvEngine
}

// NewEndpoint creates a coprocessor Endpoint.
func NewEndpoint(engine traits.KvEngine) *Endpoint {
	return &Endpoint{engine: engine}
}

// ExecutorsRunner drives the executor pipeline to completion.
type ExecutorsRunner struct {
	executor       BatchExecutor
	initialBatch   int
	maxBatch       int
	deadline       time.Time
}

// NewExecutorsRunner creates a runner.
func NewExecutorsRunner(executor BatchExecutor, deadline time.Time) *ExecutorsRunner {
	return &ExecutorsRunner{
		executor:     executor,
		initialBatch: 32,
		maxBatch:     1024,
		deadline:     deadline,
	}
}

// Run drives the executor pipeline to completion and collects all rows.
func (r *ExecutorsRunner) Run(ctx context.Context) ([]Row, error) {
	var allRows []Row
	batchSize := r.initialBatch

	for {
		if !r.deadline.IsZero() && time.Now().After(r.deadline) {
			return allRows, ErrDeadlineExceeded
		}

		result, err := r.executor.NextBatch(ctx, batchSize)
		if err != nil {
			return allRows, err
		}

		allRows = append(allRows, result.Rows...)

		if result.IsDrained {
			break
		}

		// Grow batch size exponentially.
		batchSize *= 2
		if batchSize > r.maxBatch {
			batchSize = r.maxBatch
		}
	}

	return allRows, nil
}
