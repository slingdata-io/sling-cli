package iop

// Tests for the open-build RecordStream plumbing. They run with a fake
// ArrowLane, so they pass in an unlicensed build where NewArrowLane returns
// nil. The fake is prefixed `openFake` to stay clear of arrow_lane__test.go.
import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openFakeLane stands in for the closed engine. It counts the calls each test
// cares about and does the minimum work the wrappers need. Only the test
// goroutine touches it, so no locking.
type openFakeLane struct {
	alloc          memory.Allocator
	normalizeCalls int
	projectCalls   int
	maxOfCalls     int
}

func (l *openFakeLane) CastSupported(from, to arrow.DataType) (bool, string) {
	if from.ID() == to.ID() {
		return true, ""
	}
	return false, "openFakeLane: only same-type casts"
}

// copyCols builds a new record in schema from the given source columns; a
// picked index of -1 becomes a null column.
func (l *openFakeLane) copyCols(rec arrow.RecordBatch, schema *arrow.Schema, picked []int) arrow.RecordBatch {
	b := array.NewRecordBuilder(l.alloc, schema)
	defer b.Release()
	for r := 0; r < int(rec.NumRows()); r++ {
		for i, ci := range picked {
			if ci < 0 {
				b.Field(i).AppendNull()
				continue
			}
			openFakeAppend(b.Field(i), GetValueFromArrowArray(rec.Column(ci), r))
		}
	}
	return b.NewRecordBatch()
}

// Normalize returns the record in the target schema, mapping fields by name.
func (l *openFakeLane) Normalize(rec arrow.RecordBatch, to *arrow.Schema) (arrow.RecordBatch, error) {
	l.normalizeCalls++
	src := openFakeIndex(rec)
	picked := make([]int, to.NumFields())
	for i, f := range to.Fields() {
		ci, ok := src[strings.ToLower(f.Name)]
		if !ok {
			ci = -1
		}
		picked[i] = ci
	}
	return l.copyCols(rec, to, picked), nil
}

// Project returns the record with the named columns, in the requested order.
func (l *openFakeLane) Project(rec arrow.RecordBatch, cols Columns) (arrow.RecordBatch, error) {
	l.projectCalls++
	src := openFakeIndex(rec)
	fields := rec.Schema().Fields()
	out := make([]arrow.Field, len(cols))
	picked := make([]int, len(cols))
	for i, col := range cols {
		ci, ok := src[strings.ToLower(col.Name)]
		if !ok {
			return nil, g.Error("openFakeLane: no column %q", col.Name)
		}
		picked[i] = ci
		f := fields[ci]
		f.Name = col.Name
		out[i] = f
	}
	return l.copyCols(rec, arrow.NewSchema(out, nil), picked), nil
}

// MaxOf reads int64 arrays only, which is enough to test the wiring.
// ClassifyTransform declines every stage: the fake evaluates no transform.
func (l *openFakeLane) ClassifyTransform(stages []map[string]string, cols Columns) string {
	if len(stages) > 0 {
		return "openFakeLane does not evaluate transforms"
	}
	return ""
}

// NewTransform is never reached: ClassifyTransform declines every stage.
func (l *openFakeLane) NewTransform(stages []map[string]string, sp *StreamProcessor) (RecordTransform, error) {
	return nil, g.Error("openFakeLane does not evaluate transforms")
}

func (l *openFakeLane) MaxOf(arr arrow.Array) (int64, bool) {
	l.maxOfCalls++
	a, ok := arr.(*array.Int64)
	if !ok {
		return 0, false
	}
	var max int64
	found := false
	for i := 0; i < a.Len(); i++ {
		if a.IsNull(i) {
			continue
		}
		if v := a.Value(i); !found || v > max {
			max, found = v, true
		}
	}
	return max, found
}

func openFakeIndex(rec arrow.RecordBatch) map[string]int {
	idx := map[string]int{}
	for i, f := range rec.Schema().Fields() {
		idx[strings.ToLower(f.Name)] = i
	}
	return idx
}

func openFakeAppend(b array.Builder, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch f := b.(type) {
	case *array.Int64Builder:
		f.Append(v.(int64))
	case *array.StringBuilder:
		f.Append(v.(string))
	default:
		panic(fmt.Sprintf("openFakeAppend: unsupported builder %T", b))
	}
}

// openFakeSchema is the two-column (int64, utf8) schema the tests share.
func openFakeSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// openFakeRecord builds a record in schema; a nil value becomes null.
func openFakeRecord(t testing.TB, alloc memory.Allocator, schema *arrow.Schema, rows [][]any) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			openFakeAppend(b.Field(i), v)
		}
	}
	return b.NewRecordBatch()
}

func openFakeVal(t testing.TB, rec arrow.RecordBatch, col, row int) any {
	t.Helper()
	return GetValueFromArrowArray(rec.Column(col), row)
}

// openFakeEnv is a stream on the fake lane plus the pieces the tests need. The
// checked allocator is asserted empty once the test ends.
type openFakeEnv struct {
	alloc  memory.Allocator
	ctx    *g.Context
	schema *arrow.Schema
	lane   *openFakeLane
	rs     *RecordStream
}

func openFakeStream(t *testing.T, size int) *openFakeEnv {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	ctx := g.NewContext(context.Background())
	t.Cleanup(ctx.Cancel)
	schema := openFakeSchema()
	lane := &openFakeLane{alloc: alloc}
	return &openFakeEnv{alloc, ctx, schema, lane, NewRecordStream(ctx, lane, schema, size)}
}

// push builds a one-row record and hands it to the stream.
func (e *openFakeEnv) push(t testing.TB, row ...any) {
	t.Helper()
	require.NoError(t, e.rs.Push(openFakeRecord(t, e.alloc, e.schema, [][]any{row})))
}

// TestRecordStream_PushNextOrder covers record order on the way out plus both
// end-of-stream forms: a clean Close(nil) and a Close with an error.
func TestRecordStream_PushNextOrder(t *testing.T) {
	e := openFakeStream(t, 4)
	want := [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}
	for _, row := range want {
		e.push(t, row...)
	}
	e.rs.Close(nil)

	for i, row := range want {
		rec, ok := e.rs.Next()
		require.True(t, ok, "record %d", i)
		assert.Equal(t, row[0], openFakeVal(t, rec, 0, 0))
		assert.Equal(t, row[1], openFakeVal(t, rec, 1, 0))
		rec.Release()
	}
	_, ok := e.rs.Next()
	assert.False(t, ok)
	assert.NoError(t, e.rs.Err())

	// a Close error still drains the queued record, then surfaces on Err
	e2 := openFakeStream(t, 2)
	e2.push(t, int64(7), "x")
	boom := errors.New("boom")
	e2.rs.Close(boom)

	rec, ok := e2.rs.Next()
	require.True(t, ok)
	rec.Release()
	_, ok = e2.rs.Next()
	assert.False(t, ok)
	assert.ErrorIs(t, e2.rs.Err(), boom)
}

func TestRecordStream_Drain(t *testing.T) {
	e := openFakeStream(t, 8)
	for i := 0; i < 3; i++ {
		e.push(t, int64(i), nil)
	}
	_, ok := e.rs.Peek()
	require.True(t, ok)
	e.rs.Close(nil)

	e.rs.Drain() // releases the peeked record and the three queued ones
	e.rs.Drain() // idempotent
	_, ok = e.rs.Next()
	assert.False(t, ok)
	assert.NoError(t, e.rs.Err())
}

// TestRecordStream_PeekThenNext covers Peek and SampleRows, which both read
// the first record without consuming it.
func TestRecordStream_PeekThenNext(t *testing.T) {
	e := openFakeStream(t, 2)
	e.push(t, int64(11), "p")
	e.push(t, int64(22), "q")
	e.rs.Close(nil)

	p1, ok := e.rs.Peek()
	require.True(t, ok)
	p2, ok := e.rs.Peek()
	require.True(t, ok)
	assert.True(t, p1 == p2, "Peek must not consume")

	rec, ok := e.rs.Next()
	require.True(t, ok)
	assert.True(t, rec == p1, "Next must return the peeked record exactly once")
	rec.Release()
	rec, ok = e.rs.Next()
	require.True(t, ok)
	assert.Equal(t, int64(22), openFakeVal(t, rec, 0, 0))
	rec.Release()

	e2 := openFakeStream(t, 2)
	rows := make([][]any, 0, 5)
	for i := 1; i <= 5; i++ {
		rows = append(rows, []any{int64(i), fmt.Sprintf("r%d", i)})
	}
	require.NoError(t, e2.rs.Push(openFakeRecord(t, e2.alloc, e2.schema, rows)))
	e2.rs.Close(nil)

	sample := e2.rs.SampleRows(2)
	require.Len(t, sample, 2)
	assert.Equal(t, []any{int64(1), "r1"}, sample[0])
	assert.Equal(t, []any{int64(2), "r2"}, sample[1])
	assert.Len(t, e2.rs.SampleRows(0), 5, "n <= 0 means all rows of the record")

	rec, ok = e2.rs.Next()
	require.True(t, ok)
	assert.Equal(t, int64(5), rec.NumRows(), "the sampled record is still next")
	rec.Release()
	_, ok = e2.rs.Next()
	assert.False(t, ok)
}

// TestRecordStream_TrackMax covers the max wiring and the per-column null
// counts; both read the records taken from the root stream.
func TestRecordStream_TrackMax(t *testing.T) {
	e := openFakeStream(t, 4)
	_, ok := e.rs.Max()
	assert.False(t, ok, "Max is unset before anything is taken")
	assert.Nil(t, e.rs.NullCounts(), "no counts before anything is taken")

	e.rs.TrackMax(0)
	e.push(t, int64(5), nil)
	e.push(t, int64(9), "b")
	e.rs.Close(nil)

	rec, ok := e.rs.Next()
	require.True(t, ok)
	rec.Release()
	assert.Equal(t, 1, e.lane.maxOfCalls, "MaxOf runs once per record taken")
	val, ok := e.rs.Max()
	require.True(t, ok)
	assert.Equal(t, int64(5), val)
	col, val, ok := e.rs.TrackedMax()
	require.True(t, ok)
	assert.Equal(t, 0, col)
	assert.Equal(t, int64(5), val)

	rec, ok = e.rs.Next()
	require.True(t, ok)
	rec.Release()
	assert.Equal(t, 2, e.lane.maxOfCalls)
	val, ok = e.rs.Max()
	require.True(t, ok)
	assert.Equal(t, int64(9), val)
	assert.Equal(t, []int64{0, 1}, e.rs.NullCounts())

	// the returned slice is a copy, so a caller cannot corrupt the counters
	counts := e.rs.NullCounts()
	counts[0] = 99
	assert.Equal(t, []int64{0, 1}, e.rs.NullCounts())
}

// TestRecordStream_TrackMaxString covers the string update-key maximum: the
// lane declines utf8, so the stream tracks it with its own loop.
func TestRecordStream_TrackMaxString(t *testing.T) {
	e := openFakeStream(t, 4)
	_, ok := e.rs.MaxString()
	assert.False(t, ok, "no string max before anything is taken")

	e.rs.TrackMax(1) // the utf8 column, which the lane declines
	e.push(t, int64(5), "b")
	e.push(t, int64(6), nil)
	e.push(t, int64(7), "a")
	e.rs.Close(nil)

	rec, ok := e.rs.Next()
	require.True(t, ok)
	rec.Release()
	_, ok = e.rs.Max()
	assert.False(t, ok, "the lane tracked no int64 max for a utf8 column")
	col, val, ok := e.rs.TrackedMaxString()
	require.True(t, ok)
	assert.Equal(t, 1, col)
	assert.Equal(t, "b", val)

	rec, ok = e.rs.Next() // a null row does not change the max
	require.True(t, ok)
	rec.Release()
	val, ok = e.rs.MaxString()
	require.True(t, ok)
	assert.Equal(t, "b", val)

	rec, ok = e.rs.Next() // 'a' sorts below 'b'
	require.True(t, ok)
	rec.Release()
	val, ok = e.rs.MaxString()
	require.True(t, ok)
	assert.Equal(t, "b", val)

	// the max is a byte-wise comparison, not a numeric one: the state
	// watermark for a string key follows the same order as the SQL filter
	e2 := openFakeStream(t, 4)
	e2.rs.TrackMax(1)
	e2.push(t, int64(1), "s999")
	e2.push(t, int64(2), "s1000")
	e2.push(t, int64(3), "s9999")
	e2.rs.Close(nil)
	for i := 0; i < 3; i++ {
		rec, ok = e2.rs.Next()
		require.True(t, ok)
		rec.Release()
	}
	val, ok = e2.rs.MaxString()
	require.True(t, ok)
	assert.Equal(t, "s9999", val)

	// a tracked int64 column leaves the string max unset
	e3 := openFakeStream(t, 2)
	e3.rs.TrackMax(0)
	e3.push(t, int64(3), "z")
	e3.rs.Close(nil)
	rec, ok = e3.rs.Next()
	require.True(t, ok)
	rec.Release()
	_, _, ok = e3.rs.TrackedMaxString()
	assert.False(t, ok)
}

func TestRecordStream_Reader(t *testing.T) {
	e := openFakeStream(t, 4)
	for i := 0; i < 3; i++ {
		e.push(t, int64(i), "v")
	}
	e.rs.Close(nil)

	rdr := e.rs.Reader()
	assert.True(t, rdr.Schema() == e.schema)

	var got []int64
	for rdr.Next() {
		rec := rdr.RecordBatch()
		require.NotNil(t, rec)
		got = append(got, openFakeVal(t, rec, 0, 0).(int64))
		rdr.Release() // the reader frees the previous record
	}
	assert.Equal(t, []int64{0, 1, 2}, got)
	assert.NoError(t, rdr.Err())
	assert.False(t, rdr.Next(), "the closed stream yields no more records")
	rdr.Release()

	_, ok := e.rs.Next()
	assert.False(t, ok, "the reader consumed the stream")
}

func TestRecordStream_NilLanePanics(t *testing.T) {
	ctx := g.NewContext(context.Background())
	defer ctx.Cancel()
	assert.PanicsWithValue(t, "iop.NewRecordStream: nil arrow lane", func() {
		NewRecordStream(ctx, nil, openFakeSchema(), 1)
	})
}

// TestRecordStream_Normalize covers the lazy Normalize wrapper and that the
// root stream still feeds TrackedMax and NullCounts.
func TestRecordStream_Normalize(t *testing.T) {
	e := openFakeStream(t, 4)

	// the schema already matches: the wrapper is the receiver, the lane idle
	same, err := e.rs.Normalize(e.schema)
	require.NoError(t, err)
	assert.True(t, same == e.rs)

	target := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "note", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	w, err := e.rs.Normalize(target)
	require.NoError(t, err)
	assert.True(t, w != e.rs)
	assert.True(t, w.Schema == target)
	assert.Equal(t, 0, e.lane.normalizeCalls, "Normalize is lazy: no lane call at wrap time")

	w.TrackMax(0) // the root tracks the raw int64 column
	e.rs = w
	e.push(t, int64(1), "a")
	e.push(t, int64(2), nil)
	w.Close(nil)
	assert.Nil(t, w.NullCounts(), "no counts before anything is taken")

	rec, ok := w.Next()
	require.True(t, ok)
	assert.Equal(t, 1, e.lane.normalizeCalls, "the lane runs once per record taken")
	require.Equal(t, 3, int(rec.NumCols()))
	assert.Equal(t, int64(1), openFakeVal(t, rec, 0, 0))
	assert.Nil(t, openFakeVal(t, rec, 2, 0), "the added column is null")
	val, ok := w.Max()
	require.True(t, ok)
	assert.Equal(t, int64(1), val, "Max reflects the root stream")
	rec.Release()

	rec, ok = w.Next()
	require.True(t, ok)
	assert.Nil(t, openFakeVal(t, rec, 1, 0))
	rec.Release()
	assert.Equal(t, 2, e.lane.normalizeCalls)

	// TrackedMax and NullCounts read the raw records of the root, not the
	// normalized output.
	col, val, ok := w.TrackedMax()
	require.True(t, ok)
	assert.Equal(t, 0, col)
	assert.Equal(t, int64(2), val)
	assert.Equal(t, []int64{0, 1}, w.NullCounts())

	_, ok = w.Next()
	assert.False(t, ok)
	assert.NoError(t, w.Err())
}

// TestRecordStream_Project covers the lazy Project wrapper and that a Push on
// a wrapper reaches the source channel.
func TestRecordStream_Project(t *testing.T) {
	e := openFakeStream(t, 4)

	// same names and order: the wrapper is the receiver
	same, err := e.rs.Project(e.rs.Columns)
	require.NoError(t, err)
	assert.True(t, same == e.rs)

	p, err := e.rs.Project(Columns{{Name: "name"}, {Name: "id"}})
	require.NoError(t, err)
	assert.True(t, p != e.rs)
	require.Len(t, p.Columns, 2)
	assert.Equal(t, "name", p.Columns[0].Name)
	assert.Equal(t, "id", p.Columns[1].Name)
	assert.Equal(t, 0, e.lane.projectCalls, "Project is lazy: no lane call at wrap time")

	_, err = e.rs.Project(Columns{{Name: "nope"}})
	assert.Error(t, err, "an unknown target column is an error")

	p.TrackMax(0)
	rec := openFakeRecord(t, e.alloc, e.schema, [][]any{{int64(5), "five"}})
	require.NoError(t, p.Push(rec))

	// a wrapper has no channel of its own: Push lands on the source
	raw, ok := e.rs.Next()
	require.True(t, ok)
	assert.True(t, raw == rec)
	raw.Release()

	require.NoError(t, p.Push(openFakeRecord(t, e.alloc, e.schema, [][]any{{int64(6), "six"}})))
	p.Close(nil)
	out, ok := p.Next()
	require.True(t, ok)
	assert.Equal(t, 1, e.lane.projectCalls)
	require.Equal(t, 2, int(out.NumCols()))
	assert.Equal(t, "name", out.Schema().Field(0).Name)
	assert.Equal(t, "id", out.Schema().Field(1).Name)
	assert.Equal(t, "six", openFakeVal(t, out, 0, 0))
	assert.Equal(t, int64(6), openFakeVal(t, out, 1, 0))
	out.Release()

	val, ok := p.Max()
	require.True(t, ok)
	assert.Equal(t, int64(6), val, "Max reflects the root stream")
	assert.Equal(t, []int64{0, 0}, p.NullCounts())

	_, ok = p.Next()
	assert.False(t, ok)
	assert.NoError(t, p.Err())
}

// TestRecordStream_Relabel covers the metadata-only rebind of the ingest.
func TestRecordStream_Relabel(t *testing.T) {
	e := openFakeStream(t, 4)

	same, err := e.rs.Relabel(e.schema)
	require.NoError(t, err)
	assert.True(t, same == e.rs)

	md := arrow.NewMetadata([]string{"ARROW:extension:name"}, []string{"arrow.json"})
	fields := e.schema.Fields()
	fields[1].Metadata = md
	labeled := arrow.NewSchema(fields, nil)

	r, err := e.rs.Relabel(labeled)
	require.NoError(t, err)

	_, err = e.rs.Relabel(arrow.NewSchema(fields[:1], nil))
	assert.Error(t, err, "a different field set is an error")

	e.push(t, int64(1), "a")
	e.rs.Close(nil)
	out, ok := r.Next()
	require.True(t, ok)
	assert.True(t, out.Schema().Equal(labeled))
	assert.Equal(t, "a", openFakeVal(t, out, 1, 0))
	out.Release()
	_, ok = r.Next()
	assert.False(t, ok)
}

func TestRecordStream_PushContextCancel(t *testing.T) {
	e := openFakeStream(t, 2)

	// fill the buffer so the next Push cannot proceed
	e.push(t, int64(0), nil)
	e.push(t, int64(1), nil)
	e.ctx.Cancel()

	// build before the goroutine: the record is released by Push, never read
	extra := openFakeRecord(t, e.alloc, e.schema, [][]any{{int64(9), nil}})
	done := make(chan error, 1)
	go func() { done <- e.rs.Push(extra) }()

	select {
	case err := <-done:
		assert.Error(t, err, "Push returns the context error instead of blocking")
	case <-time.After(5 * time.Second):
		t.Fatal("Push blocked after the context was cancelled")
	}

	// buffered records are still readable, even on a cancelled context
	for {
		rec, ok := e.rs.Next()
		if !ok {
			break
		}
		rec.Release()
	}
	e.rs.Close(nil)
}

// TestRecordStream_PeekUnblocksPush is the regression test for the deadlock
// where Peek held the stream mutex across its channel read: a producer whose
// first Push landed while Peek waited blocked on the mutex, so neither side
// could proceed. This is the shape the ADBC source uses: the producer
// goroutine starts, then the datastream calls SampleRows -> Peek.
func TestRecordStream_PeekUnblocksPush(t *testing.T) {
	ctx := g.NewContext(context.Background())
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	lane := &openFakeLane{alloc: alloc}
	schema := openFakeSchema()
	rs := NewRecordStream(ctx, lane, schema, ArrowLaneBuffer)

	defer alloc.AssertSize(t, 0)

	peeked := make(chan arrow.RecordBatch, 1)
	go func() {
		rec, ok := rs.Peek()
		if ok {
			peeked <- rec
		}
		close(peeked)
	}()

	// the producer pushes only after Peek is waiting
	time.Sleep(50 * time.Millisecond)
	rec := openFakeRecord(t, alloc, schema, [][]any{{int64(7), "a"}, {int64(8), "b"}})
	require.NoError(t, rs.Push(rec))

	select {
	case got := <-peeked:
		require.NotNil(t, got, "Peek must return the first record")
		assert.Equal(t, int64(2), got.NumRows())
	case <-time.After(5 * time.Second):
		t.Fatal("Peek did not return: Push and Peek deadlocked")
	}

	rs.Close(nil)
	rs.Drain()
}

// TestRecordStream_MetaColumnsBeforeTransform covers the order the row path
// uses: the metadata columns are appended before the stages run, so a stage can
// address them, and the stream refuses a late SetMetaColumns.
func TestRecordStream_MetaColumnsBeforeTransform(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	ctx := g.NewContext(context.Background())
	t.Cleanup(ctx.Cancel)

	schema := openFakeSchema()
	lane := &metaOrderLane{openFakeLane: &openFakeLane{alloc: alloc}}
	rs := NewRecordStream(ctx, lane, schema, 8)

	ds := &Datastream{Columns: ArrowSchemaToColumns(schema), Metadata: dsArrowTestMetadata()}
	require.NoError(t, rs.SetMetaColumns(ds.metaColumnValues()))
	require.NoError(t, rs.SetTransform([]map[string]string{{"col": "upper(name)"}}, &StreamProcessor{}))

	// a late call would leave the first records without the columns
	err := rs.SetMetaColumns(ds.metaColumnValues())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "before the transforms")

	require.NoError(t, rs.Push(openFakeRecord(t, alloc, schema, [][]any{{int64(1), "a"}})))
	rs.Close(nil)

	rec, ok := rs.Next()
	require.True(t, ok)
	assert.Equal(t, []string{"id", "name", "_sling_loaded_at", "_sling_synced_op", "_sling_synced_seq",
		"_sling_stream_url", "_sling_row_num", "_sling_row_id", "_sling_exec_id"}, lane.sawFields,
		"the stages see the metadata columns")
	rec.Release()

	// the stream reports the same columns the datastream does
	assert.Equal(t, lane.sawFields, rs.Columns.Names())
}

// TestRecordStreamNilLanePanics documents the wiring bug the gate prevents: a
// nil lane never reaches NewRecordStream.
func TestRecordStreamNilLanePanics(t *testing.T) {
	assert.PanicsWithValue(t, "iop.NewRecordStream: nil arrow lane", func() {
		NewRecordStream(g.NewContext(context.Background()), nil, dsArrowTestSchema(), ArrowLaneBuffer)
	})
}
