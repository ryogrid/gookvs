package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/olekukonko/tablewriter"
)

// ---------------------------------------------------------------------------
// Display & output enums
// ---------------------------------------------------------------------------

// DisplayMode controls how binary data is rendered.
type DisplayMode int

const (
	DisplayAuto   DisplayMode = iota // string if all printable ASCII, hex otherwise
	DisplayHex                       // always hex
	DisplayString                    // always string (non-printable as \xNN)
)

// OutputFormat controls the structural format of multi-row results.
type OutputFormat int

const (
	FormatTable OutputFormat = iota // ASCII box tables (default)
	FormatPlain                     // tab-separated, one pair per line
	FormatHex                       // like plain but always hex-encoded
)

// ---------------------------------------------------------------------------
// ResultType
// ---------------------------------------------------------------------------

// ResultType determines how the formatter renders the output.
type ResultType int

const (
	ResultOK       ResultType = iota // simple "OK" (PUT, DELETE, COMMIT, etc.)
	ResultValue                      // single value (GET)
	ResultNotFound                   // key not found (raw GET)
	ResultNil                        // txn not found (displays "(nil)")
	ResultRows                       // multi-row key-value table (SCAN, BGET)
	ResultTable                      // arbitrary column table (STORE LIST, CLUSTER INFO)
	ResultScalar                     // single scalar line (TTL, GC SAFEPOINT)
	ResultCAS                        // CAS result (swapped, previous value)
	ResultChecksum                   // checksum result
	ResultBegin                      // BEGIN output (startTS, mode description)
	ResultCommit                     // COMMIT output (commitTS, key count)
	ResultMessage                    // free-form text (HELP, TSO, REGION)
)

// ---------------------------------------------------------------------------
// Result
// ---------------------------------------------------------------------------

// KvPairResult holds a key-value pair for display.
type KvPairResult struct {
	Key   []byte
	Value []byte
}

// Result holds the output of a command execution.
type Result struct {
	Type          ResultType
	Value         []byte          // ResultValue
	Rows          []KvPairResult  // ResultRows
	Columns       []string        // ResultTable headers
	TableRows     [][]string      // ResultTable data
	Scalar        string          // ResultScalar
	Message       string          // ResultOK detail, ResultBegin, ResultCommit, ResultMessage
	Swapped       bool            // ResultCAS
	PrevVal       []byte          // ResultCAS
	PrevNotFound  bool            // ResultCAS (prev key did not exist)
	Checksum      uint64          // ResultChecksum
	TotalKvs      uint64          // ResultChecksum
	TotalBytes    uint64          // ResultChecksum
	NotFoundCount int             // ResultRows (BGET: count of keys not found)
}

// ---------------------------------------------------------------------------
// Formatter
// ---------------------------------------------------------------------------

// Formatter renders Results to the output writer.
type Formatter struct {
	out         io.Writer
	errOut      io.Writer
	displayMode DisplayMode
	outputFmt   OutputFormat
	showTiming  bool
	pageSize    int
}

// NewFormatter creates a Formatter with default settings.
func NewFormatter(out io.Writer) *Formatter {
	return &Formatter{
		out:         out,
		errOut:      out, // callers can override via SetErrOut
		displayMode: DisplayAuto,
		outputFmt:   FormatTable,
		showTiming:  true,
		pageSize:    100,
	}
}

// SetErrOut sets the writer for error output.
func (f *Formatter) SetErrOut(w io.Writer) {
	f.errOut = w
}

// SetDisplayMode changes the display mode (auto/hex/string).
func (f *Formatter) SetDisplayMode(m DisplayMode) {
	f.displayMode = m
}

// SetOutputFormat changes the output format (table/plain/hex).
func (f *Formatter) SetOutputFormat(fmt OutputFormat) {
	f.outputFmt = fmt
}

// SetTiming enables or disables timing display.
func (f *Formatter) SetTiming(on bool) {
	f.showTiming = on
}

// SetPageSize sets the default SCAN limit.
func (f *Formatter) SetPageSize(n int) {
	f.pageSize = n
}

// PageSize returns the current page size.
func (f *Formatter) PageSize() int {
	return f.pageSize
}

// Format writes the rendered result and optional timing to the output.
func (f *Formatter) Format(result *Result, elapsed time.Duration) {
	switch result.Type {
	case ResultOK:
		f.formatOK(result, elapsed)
	case ResultValue:
		f.formatValue(result, elapsed)
	case ResultNotFound:
		fmt.Fprintln(f.out, "(not found)")
		f.printTiming("0 rows", elapsed)
	case ResultNil:
		fmt.Fprintln(f.out, "(nil)")
		f.printTiming("", elapsed)
	case ResultRows:
		f.formatRows(result, elapsed)
	case ResultTable:
		f.formatTable(result, elapsed)
	case ResultScalar:
		fmt.Fprintln(f.out, result.Scalar)
		f.printTiming("", elapsed)
	case ResultCAS:
		f.formatCAS(result, elapsed)
	case ResultChecksum:
		f.formatChecksum(result, elapsed)
	case ResultBegin:
		fmt.Fprintln(f.out, result.Message)
		f.printTiming("", elapsed)
	case ResultCommit:
		fmt.Fprintln(f.out, result.Message)
		f.printTiming("", elapsed)
	case ResultMessage:
		fmt.Fprintln(f.out, result.Message)
		f.printTiming("", elapsed)
	}
}

// FormatError writes an error message to the error output.
func (f *Formatter) FormatError(err error) {
	fmt.Fprintf(f.errOut, "ERROR: %v\n", err)
}

// ---------------------------------------------------------------------------
// Internal formatting methods
// ---------------------------------------------------------------------------

func (f *Formatter) formatOK(result *Result, elapsed time.Duration) {
	if result.Message != "" {
		fmt.Fprintln(f.out, result.Message)
	} else {
		fmt.Fprintln(f.out, "OK")
	}
	f.printTiming("", elapsed)
}

func (f *Formatter) formatValue(result *Result, elapsed time.Duration) {
	switch f.outputFmt {
	case FormatHex:
		fmt.Fprintln(f.out, hex.EncodeToString(result.Value))
	default:
		displayed := f.formatBytes(result.Value)
		fmt.Fprintf(f.out, "\"%s\"\n", displayed)
	}
	f.printTiming("1 row", elapsed)
}

func (f *Formatter) formatRows(result *Result, elapsed time.Duration) {
	switch f.outputFmt {
	case FormatTable:
		f.formatRowsTable(result)
	case FormatPlain:
		f.formatRowsPlain(result, false)
	case FormatHex:
		f.formatRowsPlain(result, true)
	}
	f.printRowsTiming(len(result.Rows), result.NotFoundCount, elapsed)
}

func (f *Formatter) formatRowsTable(result *Result) {
	tw := tablewriter.NewWriter(f.out)
	tw.SetHeader([]string{"Key", "Value"})
	tw.SetAutoFormatHeaders(false)
	tw.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
	tw.SetCenterSeparator("+")
	for _, row := range result.Rows {
		tw.Append([]string{
			f.formatBytes(row.Key),
			f.formatBytes(row.Value),
		})
	}
	tw.Render()
}

func (f *Formatter) formatRowsPlain(result *Result, forceHex bool) {
	for _, row := range result.Rows {
		var k, v string
		if forceHex {
			k = hex.EncodeToString(row.Key)
			v = hex.EncodeToString(row.Value)
		} else {
			k = f.formatBytes(row.Key)
			v = f.formatBytes(row.Value)
		}
		fmt.Fprintf(f.out, "%s\t%s\n", k, v)
	}
}

func (f *Formatter) formatTable(result *Result, elapsed time.Duration) {
	switch f.outputFmt {
	case FormatTable:
		tw := tablewriter.NewWriter(f.out)
		tw.SetHeader(result.Columns)
		tw.SetAutoFormatHeaders(false)
		tw.SetBorders(tablewriter.Border{Left: true, Top: true, Right: true, Bottom: true})
		tw.SetCenterSeparator("+")
		for _, row := range result.TableRows {
			tw.Append(row)
		}
		tw.Render()
	case FormatPlain, FormatHex:
		// Print headers as first tab-separated line
		fmt.Fprintln(f.out, joinTab(result.Columns))
		for _, row := range result.TableRows {
			fmt.Fprintln(f.out, joinTab(row))
		}
	}
	if f.showTiming {
		fmt.Fprintf(f.out, "(%d rows, %s)\n", len(result.TableRows), formatElapsed(elapsed))
	}
}

func (f *Formatter) formatCAS(result *Result, elapsed time.Duration) {
	if result.Swapped {
		fmt.Fprintln(f.out, "OK (swapped)")
	} else {
		fmt.Fprintln(f.out, "FAILED (not swapped)")
	}
	if result.PrevNotFound {
		fmt.Fprintln(f.out, "  previous: (not found)")
	} else {
		fmt.Fprintf(f.out, "  previous: \"%s\"\n", f.formatBytes(result.PrevVal))
	}
	f.printTiming("", elapsed)
}

func (f *Formatter) formatChecksum(result *Result, elapsed time.Duration) {
	fmt.Fprintf(f.out, "Checksum:    0x%016x\n", result.Checksum)
	fmt.Fprintf(f.out, "Total keys:  %s\n", formatCommas(result.TotalKvs))
	fmt.Fprintf(f.out, "Total bytes: %s\n", formatCommas(result.TotalBytes))
	f.printTiming("", elapsed)
}

// ---------------------------------------------------------------------------
// Timing helpers
// ---------------------------------------------------------------------------

func (f *Formatter) printTiming(context string, elapsed time.Duration) {
	if !f.showTiming {
		return
	}
	if context != "" {
		fmt.Fprintf(f.out, "(%s, %s)\n", context, formatElapsed(elapsed))
	} else {
		fmt.Fprintf(f.out, "(%s)\n", formatElapsed(elapsed))
	}
}

func (f *Formatter) printRowsTiming(rowCount int, notFound int, elapsed time.Duration) {
	if !f.showTiming {
		return
	}
	if notFound > 0 {
		fmt.Fprintf(f.out, "(%d rows, %d not found, %s)\n", rowCount, notFound, formatElapsed(elapsed))
	} else {
		fmt.Fprintf(f.out, "(%d rows, %s)\n", rowCount, formatElapsed(elapsed))
	}
}

func formatElapsed(d time.Duration) string {
	ms := float64(d.Microseconds()) / 1000.0
	return fmt.Sprintf("%.1fms", ms)
}

// ---------------------------------------------------------------------------
// Display helpers
// ---------------------------------------------------------------------------

// formatBytes renders a byte slice according to the current DisplayMode.
func (f *Formatter) formatBytes(data []byte) string {
	switch f.displayMode {
	case DisplayHex:
		return hex.EncodeToString(data)
	case DisplayString:
		return formatAsString(data)
	default: // DisplayAuto
		if isPrintableASCII(data) {
			return string(data)
		}
		return formatAsString(data)
	}
}

// isPrintableASCII returns true if all bytes are in [0x20, 0x7E].
func isPrintableASCII(data []byte) bool {
	for _, b := range data {
		if b < 0x20 || b > 0x7E {
			return false
		}
	}
	return true
}

// formatAsString renders bytes as a string with non-printable bytes as \xNN.
func formatAsString(data []byte) string {
	var buf []byte
	for _, b := range data {
		if b >= 0x20 && b <= 0x7E {
			buf = append(buf, b)
		} else {
			buf = append(buf, []byte(fmt.Sprintf("\\x%02x", b))...)
		}
	}
	return string(buf)
}

// formatCommas formats a uint64 with comma separators.
func formatCommas(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var buf []byte
	remainder := len(s) % 3
	if remainder > 0 {
		buf = append(buf, s[:remainder]...)
	}
	for i := remainder; i < len(s); i += 3 {
		if len(buf) > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, s[i:i+3]...)
	}
	return string(buf)
}

// joinTab joins strings with tab separators.
func joinTab(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "\t"
		}
		result += p
	}
	return result
}
