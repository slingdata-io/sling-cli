package iop

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
)

// ArrowLane is the engine of the Arrow-native dataflow lane. It holds the
// type rules and the value work (cast allow-list, normalization, projection,
// max scan). The open build ships a stub; the official release sets
// newArrowLane in init().
//
// Ownership: Normalize and Project return a record the caller owns. They do
// not consume the input record, so the caller releases that one itself.
type ArrowLane interface {
	// CastSupported reports whether every value of `from` survives a cast to
	// `to` without loss. The reason is set for every decline.
	CastSupported(from, to arrow.DataType) (ok bool, reason string)
	// Normalize returns a record in the `to` schema, casting only the fields
	// whose type differs.
	Normalize(rec arrow.RecordBatch, to *arrow.Schema) (arrow.RecordBatch, error)
	// Project returns a record with the names and order of cols.
	Project(rec arrow.RecordBatch, cols Columns) (arrow.RecordBatch, error)
	// MaxOf returns the maximum value of an array as unix micro (or as the
	// int64 value for integer types). ok is false when the type is not
	// tracked or the array holds no non-null value.
	MaxOf(arr arrow.Array) (val int64, ok bool)
	// NewTransform returns the evaluator for a stage list. One per stream: it
	// holds the column types the first record fixed, as the row path's
	// transform does. A stage the lane cannot evaluate is an error: the gate
	// classifies the stages first, so this only fires on a wiring bug.
	NewTransform(stages []map[string]string, sp *StreamProcessor) (RecordTransform, error)
	// ClassifyTransform returns the reason the first stage the lane cannot
	// evaluate declines, or "" when it can evaluate every stage. cols may be
	// nil: stage 1 of the gate has no schema yet, so only the stage shapes are
	// checked there. Stage 2 passes the real columns, which also checks that
	// every stage names an existing column.
	ClassifyTransform(stages []map[string]string, cols Columns) (reason string)
}

// MetaColumn is one metadata column a stream appends to every record. Value is
// called once per row, with the stream's running row number.
type MetaColumn struct {
	Column Column
	Value  func(rowNum int64) any
}

// RecordTransform evaluates a stage list on records. The caller owns the
// result; the input record is not consumed.
type RecordTransform interface {
	// Transform evaluates the stages on one record and returns the new record
	// with the columns that follow it. The column types it reports stay fixed
	// for the stream: a later record whose result moves to another type class
	// is an error, where the row path would coerce it.
	Transform(rec arrow.RecordBatch, cols Columns) (arrow.RecordBatch, Columns, error)
}

// newArrowLane is set by the closed arrow_lane..go file. The stub declines so
// the open build compiles and always takes the row path.
var newArrowLane = func() (ArrowLane, string) {
	return nil, "arrow lane requires the official release of sling-cli"
}

// NewArrowLane returns the lane engine, or nil and the reason it is
// unavailable. The closed build checks the plan token here.
func NewArrowLane() (ArrowLane, string) {
	return newArrowLane()
}

// ArrowLaneBuffer is the record channel depth of a RecordStream. It is the
// lane's backpressure: the producer blocks once this many records wait for the
// consumer. SLING_ARROW_LANE_BUFFER overrides it.
var ArrowLaneBuffer = 8

func init() {
	if val, err := strconv.Atoi(strings.TrimSpace(os.Getenv("SLING_ARROW_LANE_BUFFER"))); err == nil && val > 0 {
		ArrowLaneBuffer = val
	}
}

// TotalRecordSize returns the in-memory size of a record's buffers.
func TotalRecordSize(rec arrow.RecordBatch) int64 {
	if rec == nil {
		return 0
	}
	size := uint64(0)
	for i := 0; i < int(rec.NumCols()); i++ {
		if arr := rec.Column(i); arr != nil {
			size += arr.Data().SizeInBytes()
		}
	}
	return int64(size)
}

// RecordStream carries Arrow record batches from a source to a sink. It is
// the lane's replacement for BatchChan: the sink pulls records instead of
// rows, and no []any row is built on the way.
//
// Ownership moves at Push: the caller retains, the stream releases. Next
// hands the record to the consumer, which releases it. Peek does not consume.
type RecordStream struct {
	Schema  *arrow.Schema
	Columns Columns

	lane   ArrowLane
	ctx    *g.Context
	ch     chan arrow.RecordBatch
	onTake func(rec arrow.RecordBatch)

	// set on a lazy wrapper (Normalize / Project)
	src   *RecordStream
	apply func(rec arrow.RecordBatch) (arrow.RecordBatch, error)

	// set by SetTransform / SetMetaColumns, read by the consumer only
	rt         RecordTransform
	metaCols   []MetaColumn
	metaSchema *arrow.Schema
	metaRowNum int64

	mu       sync.Mutex
	peekMu   sync.Mutex // serializes Peek callers; never held across a Push
	peeked   arrow.RecordBatch
	closed   bool
	draining bool
	err      error

	maxCol int
	maxVal atomic.Int64
	maxSet atomic.Bool

	// a string update key has no int64 form, so it is tracked beside the
	// numeric max. Only the producers touch it, under maxMu.
	maxMu     sync.Mutex
	maxStr    string
	maxStrSet bool

	nulls []int64
}

// NewRecordStream returns a stream that carries records in the given schema.
// The lane must not be nil: the gate never lets a nil lane reach here, so a
// nil lane marks a wiring bug.
func NewRecordStream(ctx *g.Context, lane ArrowLane, schema *arrow.Schema, size int) *RecordStream {
	if lane == nil {
		panic("iop.NewRecordStream: nil arrow lane")
	}
	if ctx == nil {
		ctx = g.NewContext(g.NewContext(nil).Ctx)
	}
	if size <= 0 {
		size = ArrowLaneBuffer
	}
	return &RecordStream{
		Schema:  schema,
		Columns: ArrowSchemaToColumns(schema),
		lane:    lane,
		ctx:     ctx,
		ch:      make(chan arrow.RecordBatch, size),
		maxCol:  -1,
	}
}

// Lane returns the engine of the stream.
func (rs *RecordStream) Lane() ArrowLane {
	return rs.root().lane
}

// SetOnTake sets the hook the stream calls when a record is taken by the
// consumer. The datastream counts rows and bytes there.
func (rs *RecordStream) SetOnTake(f func(rec arrow.RecordBatch)) {
	rs.root().onTake = f
}

func (rs *RecordStream) root() *RecordStream {
	for rs.src != nil {
		rs = rs.src
	}
	return rs
}

// Err returns the first error of the stream.
func (rs *RecordStream) Err() error {
	rs = rs.root()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.err
}

func (rs *RecordStream) setErr(err error) {
	if err == nil {
		return
	}
	rs = rs.root()
	rs.mu.Lock()
	if rs.err == nil {
		rs.err = err
	}
	rs.mu.Unlock()
}

// Push sends a record to the consumer. The caller retains the record; the
// stream releases it. Push blocks until the consumer takes it, which is the
// lane's backpressure, and returns when the context is done.
func (rs *RecordStream) Push(rec arrow.RecordBatch) error {
	rs = rs.root()
	rs.mu.Lock()
	closed := rs.closed
	rs.mu.Unlock()
	if closed {
		rec.Release()
		return g.Error("arrow lane: pushed a record to a closed stream")
	}

	select {
	case rs.ch <- rec:
		return nil
	case <-rs.ctx.Ctx.Done():
		rec.Release()
		if err := rs.ctx.Err(); err != nil {
			return err
		}
		return g.Error("arrow lane: stream context is done")
	}
}

// Close ends the stream. Only the producer calls it: the consumer side calls
// Fail. The first error wins.
func (rs *RecordStream) Close(err error) {
	rs = rs.root()
	rs.mu.Lock()
	if err != nil && rs.err == nil {
		rs.err = err
	}
	if rs.closed {
		rs.mu.Unlock()
		return
	}
	rs.closed = true
	close(rs.ch)
	rs.mu.Unlock()

	if err != nil {
		rs.ctx.CaptureErr(err)
	}
}

// Fail records an error from any goroutine and unblocks a producer that is
// waiting on the record channel. The producer then closes the stream.
func (rs *RecordStream) Fail(err error) {
	rs = rs.root()
	rs.setErr(err)
	if err != nil {
		rs.ctx.CaptureErr(err)
	}
}

// Peek returns the first record without consuming it. The stream keeps
// ownership, so the caller must not release it.
//
// Peek waits for the producer's first record, so it must not hold rs.mu: the
// producer takes rs.mu on every Push, and holding it here would deadlock the
// pair. peekMu serializes peekers instead, so one record is held at most.
func (rs *RecordStream) Peek() (arrow.RecordBatch, bool) {
	rs = rs.root()

	rs.peekMu.Lock()
	defer rs.peekMu.Unlock()

	rs.mu.Lock()
	if rs.peeked != nil {
		rec := rs.peeked
		rs.mu.Unlock()
		return rec, true
	}
	rs.mu.Unlock()

	rec, ok := rs.recv()
	if !ok {
		return nil, false
	}

	rs.mu.Lock()
	rs.peeked = rec
	rs.mu.Unlock()

	return rec, true
}

// SampleRows returns up to n rows of the first record as []any, for the
// datastream buffer (DDL and the sample checks). It does not consume.
func (rs *RecordStream) SampleRows(n int) [][]any {
	rec, ok := rs.Peek()
	if !ok {
		return nil
	}

	rows := int(rec.NumRows())
	if n > 0 && rows > n {
		rows = n
	}
	cols := int(rec.NumCols())

	out := make([][]any, 0, rows)
	for i := 0; i < rows; i++ {
		row := make([]any, cols)
		for c := 0; c < cols; c++ {
			val := GetValueFromArrowArray(rec.Column(c), i)
			if s, ok := val.(string); ok {
				val = strings.Clone(s) // do not reference the record's buffers
			}
			row[c] = val
		}
		out = append(out, row)
	}
	return out
}

// Next returns the next record, which the consumer owns and releases.
func (rs *RecordStream) Next() (rec arrow.RecordBatch, ok bool) {
	if rs.src != nil {
		in, ok := rs.src.Next()
		if !ok {
			return nil, false
		}
		out, err := rs.apply(in)
		in.Release()
		if err != nil {
			rs.src.Fail(err)
			return nil, false
		}
		return out, true
	}

	rec, ok = rs.takePeeked()
	if !ok {
		return nil, false
	}
	rs.take(rec)
	return rec, true
}

func (rs *RecordStream) takePeeked() (arrow.RecordBatch, bool) {
	rs.mu.Lock()
	if rs.peeked != nil {
		rec := rs.peeked
		rs.peeked = nil
		rs.mu.Unlock()
		return rec, true
	}
	rs.mu.Unlock()
	return rs.recv()
}

// SetTransform makes the stream evaluate the stage list on every record, with
// the lane's engine. Call it before the stream is consumed: the datastream
// sample reads through Peek, so the first record is transformed too, and the
// columns and schema the stream reports follow the transform.
//
// The stream processor supplies the transform functions (the same map the row
// path uses), so both paths evaluate the same expressions.
func (rs *RecordStream) SetTransform(stages []map[string]string, sp *StreamProcessor) error {
	rs = rs.root()

	rt, err := rs.lane.NewTransform(stages, sp)
	if err != nil {
		return err
	}
	rs.rt = rt
	return nil
}

// SetMetaColumns makes the stream append the metadata columns to every record,
// before the transforms run, as the row path does. Call it before the stream is
// consumed: the datastream sample reads through Peek, so the first record
// carries them too, and the columns the stream reports include them.
func (rs *RecordStream) SetMetaColumns(cols []MetaColumn) error {
	rs = rs.root()
	if len(cols) == 0 {
		return nil
	}
	if rs.rt != nil {
		return g.Error("arrow lane: set the metadata columns before the transforms")
	}

	rs.metaCols = cols

	// the stream owns its column slice: the datastream appends to it too
	streamCols := make(Columns, len(rs.Columns), len(rs.Columns)+len(cols))
	copy(streamCols, rs.Columns)
	for _, mc := range cols {
		col := mc.Column
		col.Position = len(streamCols) + 1
		streamCols = append(streamCols, col)
	}
	rs.Columns = streamCols

	return nil
}

// appendMeta appends the metadata columns to one record. The input record is
// released and the returned record is owned by the stream.
func (rs *RecordStream) appendMeta(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(rs.metaCols) == 0 || rec == nil {
		return rec, nil
	}

	rows := int(rec.NumRows())
	fields := rec.Schema().Fields()
	arrays := make([]arrow.Array, len(fields), len(fields)+len(rs.metaCols))
	for i := range fields {
		arrays[i] = rec.Column(i)
	}

	built := make([]arrow.Array, 0, len(rs.metaCols))
	defer func() {
		for _, arr := range built {
			arr.Release()
		}
	}()

	for _, mc := range rs.metaCols {
		vals := make([]any, rows)
		for r := 0; r < rows; r++ {
			vals[r] = mc.Value(rs.metaRowNum + int64(r) + 1)
		}
		arr, err := buildMetaArray(mc.Column, vals)
		if err != nil {
			return nil, err
		}
		built = append(built, arr)
		arrays = append(arrays, arr)
	}

	if rs.metaSchema == nil {
		metaFields := make([]arrow.Field, 0, len(rs.metaCols))
		for _, mc := range rs.metaCols {
			metaFields = append(metaFields, ColumnsToArrowSchema(Columns{mc.Column}).Field(0))
		}
		rs.metaSchema = arrow.NewSchema(append(append([]arrow.Field{}, fields...), metaFields...), nil)
	}

	rs.metaRowNum += int64(rows)
	return array.NewRecordBatch(rs.metaSchema, arrays, rec.NumRows()), nil
}

// buildMetaArray builds one metadata column array from the values of a record.
func buildMetaArray(col Column, vals []any) (arrow.Array, error) {
	schema := ColumnsToArrowSchema(Columns{col})
	builder := array.NewBuilder(memory.DefaultAllocator, schema.Field(0).Type)
	if builder == nil {
		return nil, g.Error("arrow lane: no builder for metadata column %q (%s)", col.Name, col.Type)
	}
	defer builder.Release()

	for _, val := range vals {
		AppendToBuilder(builder, &col, val)
	}

	arr := builder.NewArray()
	if arr == nil {
		return nil, g.Error("arrow lane: could not build metadata column %q", col.Name)
	}
	return arr, nil
}

// prepare applies the metadata columns and then the stage list to one received
// record, the same order the row path uses. The input record is released; the
// result is owned by the stream. An error fails the stream, so the consumer
// stops on it instead of writing a wrong record.
func (rs *RecordStream) prepare(rec arrow.RecordBatch, ok bool) (arrow.RecordBatch, bool) {
	if !ok || rec == nil {
		return rec, ok
	}

	if len(rs.metaCols) > 0 {
		out, err := rs.appendMeta(rec)
		if err != nil {
			rec.Release()
			rs.setErr(err)
			rs.ctx.CaptureErr(err)
			return nil, false
		}
		rec.Release()
		rec = out
	}

	return rs.transform(rec, true)
}

// transform applies the stage list to one received record. The input record is
// released; the result is owned by the stream. A transform error fails the
// stream, so the consumer stops on it instead of writing a wrong record.
func (rs *RecordStream) transform(rec arrow.RecordBatch, ok bool) (arrow.RecordBatch, bool) {
	if !ok || rec == nil || rs.rt == nil {
		return rec, ok
	}

	out, cols, err := rs.rt.Transform(rec, rs.Columns)
	if err != nil {
		rec.Release()
		rs.setErr(err)
		rs.ctx.CaptureErr(err)
		return nil, false
	}
	rec.Release()

	rs.Columns = cols
	if schema := out.Schema(); schema != nil {
		rs.Schema = schema
	}
	return out, true
}

// recv reads one record, preferring a queued record over a cancelled context
// so that a record already handed over is not dropped.
func (rs *RecordStream) recv() (arrow.RecordBatch, bool) {
	select {
	case rec, ok := <-rs.ch:
		return rs.prepare(rec, ok)
	default:
	}

	select {
	case rec, ok := <-rs.ch:
		return rs.prepare(rec, ok)
	case <-rs.ctx.Ctx.Done():
		select {
		case rec, ok := <-rs.ch:
			return rs.prepare(rec, ok)
		default:
			return nil, false
		}
	}
}

func (rs *RecordStream) take(rec arrow.RecordBatch) {
	if rs.onTake != nil {
		rs.onTake(rec)
	}

	if rs.nulls == nil {
		rs.nulls = make([]int64, rec.NumCols())
	}
	for i := 0; i < int(rec.NumCols()) && i < len(rs.nulls); i++ {
		if n := rec.Column(i).NullN(); n > 0 {
			rs.nulls[i] += int64(n)
		}
	}

	if rs.maxCol >= 0 && rs.maxCol < int(rec.NumCols()) {
		arr := rec.Column(rs.maxCol)
		if val, ok := rs.lane.MaxOf(arr); ok {
			rs.setMax(val)
		} else if val, ok := maxOfStringArray(arr); ok {
			rs.setMaxString(val)
		}
	}
}

// maxOfStringArray returns the maximum value of a string array. The lane's
// MaxOf covers the numeric and time types; a string update key is tracked
// here so that incremental state can advance on it.
func maxOfStringArray(arr arrow.Array) (string, bool) {
	var max string
	found := false
	consider := func(val string) {
		if !found || val > max {
			max, found = val, true
		}
	}
	switch a := arr.(type) {
	case *array.String:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				consider(a.Value(i))
			}
		}
	case *array.LargeString:
		for i := 0; i < a.Len(); i++ {
			if !a.IsNull(i) {
				consider(a.Value(i))
			}
		}
	}
	return max, found
}

func (rs *RecordStream) setMax(val int64) {
	if !rs.maxSet.Load() {
		rs.maxVal.Store(val)
		rs.maxSet.Store(true)
		return
	}
	for {
		old := rs.maxVal.Load()
		if val <= old || rs.maxVal.CompareAndSwap(old, val) {
			return
		}
	}
}

func (rs *RecordStream) setMaxString(val string) {
	rs.maxMu.Lock()
	if !rs.maxStrSet || val > rs.maxStr {
		rs.maxStr, rs.maxStrSet = val, true
	}
	rs.maxMu.Unlock()
}

// MaxString returns the tracked maximum of a string update-key column, and
// ok = false when no string was tracked.
func (rs *RecordStream) MaxString() (val string, ok bool) {
	rs = rs.root()
	rs.maxMu.Lock()
	defer rs.maxMu.Unlock()
	return rs.maxStr, rs.maxStrSet
}

// TrackedMaxString returns the tracked column index and its string maximum.
func (rs *RecordStream) TrackedMaxString() (colIdx int, val string, ok bool) {
	rs = rs.root()
	rs.maxMu.Lock()
	defer rs.maxMu.Unlock()
	if rs.maxCol < 0 || !rs.maxStrSet {
		return -1, "", false
	}
	return rs.maxCol, rs.maxStr, true
}

// TrackMax tracks the maximum value of one column, for incremental state.
// Call it before the stream is consumed.
func (rs *RecordStream) TrackMax(colIdx int) {
	rs.root().maxCol = colIdx
}

// Max returns the tracked maximum, as unix micro for date and timestamp
// columns, and ok = false when nothing was tracked or found.
func (rs *RecordStream) Max() (val int64, ok bool) {
	rs = rs.root()
	if rs.maxCol < 0 || !rs.maxSet.Load() {
		return 0, false
	}
	return rs.maxVal.Load(), true
}

// TrackedMax returns the tracked column index and its maximum value.
func (rs *RecordStream) TrackedMax() (colIdx int, val int64, ok bool) {
	rs = rs.root()
	if rs.maxCol < 0 || !rs.maxSet.Load() {
		return -1, 0, false
	}
	return rs.maxCol, rs.maxVal.Load(), true
}

// NullCounts returns the per-column null count of the records taken so far.
func (rs *RecordStream) NullCounts() []int64 {
	rs = rs.root()
	if rs.nulls == nil {
		return nil
	}
	out := make([]int64, len(rs.nulls))
	copy(out, rs.nulls)
	return out
}

// Reader returns the stream as an array.RecordReader, for the sinks that
// ingest a reader (adbc.IngestStream).
func (rs *RecordStream) Reader() array.RecordReader {
	return &recordStreamReader{rs: rs}
}

// Normalize returns a stream whose records carry the target schema. It is
// lazy: the lane runs once per record. Returns the receiver when the schema
// already matches.
func (rs *RecordStream) Normalize(target *arrow.Schema) (*RecordStream, error) {
	if target == nil {
		return rs, nil
	}
	if rs.Schema != nil && rs.Schema.Equal(target) {
		return rs, nil
	}
	lane := rs.Lane()
	if lane == nil {
		return nil, g.Error("arrow lane: no engine to normalize with")
	}
	return &RecordStream{
		Schema:  target,
		Columns: ArrowSchemaToColumns(target),
		lane:    lane,
		ctx:     rs.root().ctx,
		src:     rs,
		apply: func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
			return lane.Normalize(rec, target)
		},
		maxCol: -1,
	}, nil
}

// Project returns a stream whose records carry the names and order of cols.
// It is lazy: the lane runs once per record. Returns the receiver when the
// names and order already match.
func (rs *RecordStream) Project(cols Columns) (*RecordStream, error) {
	if len(cols) == 0 {
		return rs, nil
	}

	schema, err := projectSchema(rs.Schema, cols)
	if err != nil {
		return nil, err
	}
	if rs.Schema.Equal(schema) && sameNames(rs.Columns, cols) {
		return rs, nil
	}

	lane := rs.Lane()
	if lane == nil {
		return nil, g.Error("arrow lane: no engine to project with")
	}
	return &RecordStream{
		Schema:  schema,
		Columns: cols,
		lane:    lane,
		ctx:     rs.root().ctx,
		src:     rs,
		apply: func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
			return lane.Project(rec, cols)
		},
		maxCol: -1,
	}, nil
}

// Relabel returns a stream whose records carry schema. Only the field labels
// (metadata) may differ: the names and types must match, so no value changes.
// Returns the receiver when the schema already matches.
func (rs *RecordStream) Relabel(schema *arrow.Schema) (*RecordStream, error) {
	if schema == nil || rs.Schema.Equal(schema) {
		return rs, nil
	}
	if !arrowSchemaFieldsMatch(rs.Schema, schema) {
		return nil, g.Error("arrow lane: cannot relabel %s as %s", rs.Schema, schema)
	}
	return &RecordStream{
		Schema:  schema,
		Columns: rs.Columns,
		lane:    rs.Lane(),
		ctx:     rs.root().ctx,
		src:     rs,
		apply: func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
			return array.NewRecordBatch(schema, rec.Columns(), rec.NumRows()), nil
		},
		maxCol: -1,
	}, nil
}

// projectSchema builds the schema of a projected record: the type of every
// target column comes from the field it matches by name.
func projectSchema(schema *arrow.Schema, cols Columns) (*arrow.Schema, error) {
	if schema == nil {
		return nil, g.Error("arrow lane: no schema to project from")
	}
	fieldMap := map[string]arrow.Field{}
	for _, field := range schema.Fields() {
		fieldMap[strings.ToLower(field.Name)] = field
	}

	fields := make([]arrow.Field, len(cols))
	for i, col := range cols {
		field, ok := fieldMap[strings.ToLower(col.Name)]
		if !ok {
			return nil, g.Error("arrow lane: target column %q is not in the record schema", col.Name)
		}
		field.Name = col.Name
		fields[i] = field
	}
	return arrow.NewSchema(fields, nil), nil
}

func sameNames(a, b Columns) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name {
			return false
		}
	}
	return true
}

// Drain releases every record still queued or peeked, and keeps draining
// until the producer closes the stream. It is idempotent.
func (rs *RecordStream) Drain() {
	rs = rs.root()

	rs.mu.Lock()
	if rs.draining {
		rs.mu.Unlock()
		return
	}
	rs.draining = true
	peeked := rs.peeked
	rs.peeked = nil
	rs.mu.Unlock()

	if peeked != nil {
		peeked.Release()
	}

	for {
		select {
		case rec, ok := <-rs.ch:
			if !ok {
				return // producer is done, nothing left
			}
			rec.Release()
		default:
			go func() {
				for rec := range rs.ch {
					rec.Release()
				}
			}()
			return
		}
	}
}

type recordStreamReader struct {
	rs  *RecordStream
	cur arrow.RecordBatch
	err error
}

func (r *recordStreamReader) Retain() {}

func (r *recordStreamReader) Release() {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}

func (r *recordStreamReader) Schema() *arrow.Schema { return r.rs.Schema }

func (r *recordStreamReader) Next() bool {
	r.Release()
	rec, ok := r.rs.Next()
	if !ok {
		r.err = r.rs.Err()
		return false
	}
	r.cur = rec
	return true
}

func (r *recordStreamReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Deprecated: use RecordBatch
func (r *recordStreamReader) Record() arrow.RecordBatch { return r.cur }

func (r *recordStreamReader) Err() error { return r.err }
