package iop

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBW(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected int64
	}{
		{
			name:     "ASCII only",
			input:    []string{"hello", "world", "123"},
			expected: 16, // "hello,world,123\n" = 5+1+5+1+3+1 = 13
		},
		{
			name:     "With Unicode",
			input:    []string{"hello", "世界", "123"},
			expected: 17, // "hello,世界,123\n" = 5+1+4+1+3+1 = 14
		},
		{
			name:     "Empty strings",
			input:    []string{"", "", ""},
			expected: 3, // ",,\n" = 1+1+1 = 3
		},
		{
			name:     "Mixed content",
			input:    []string{"ABC", "世界", "123"},
			expected: 15, // "ABC,世界,123\n" = 3+1+4+1+3+1 = 12
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test original writeBwCsv
			ds1 := NewDatastream(nil)
			ds1.bwCsv = csv.NewWriter(io.Discard)
			ds1.writeBwCsv(tt.input)
			originalBytes := ds1.Bytes.Load()

			// Test new writeBwCsvSafe
			ds2 := NewDatastream(nil)
			ds2.writeBwCsvSafe(tt.input)
			safeBytes := ds2.Bytes.Load()

			// Compare results
			if originalBytes != safeBytes {
				t.Errorf("Byte count mismatch for %s: original=%d, safe=%d",
					tt.name, originalBytes, safeBytes)
			}

			// Verify against expected
			if safeBytes != cast.ToUint64(tt.expected) {
				t.Errorf("Expected %d bytes for %s, got %d",
					tt.expected, tt.name, safeBytes)
			}
		})
	}
}

func TestEncodeRowAsJSONObject(t *testing.T) {
	stringCol := func(name string) Column { return Column{Name: name, Type: StringType} }
	jsonCol := func(name string) Column { return Column{Name: name, Type: JsonType} }

	tests := []struct {
		name    string
		row     []any
		columns Columns
		want    string
		wantErr bool
	}{
		{
			name:    "empty row produces empty object",
			row:     []any{},
			columns: Columns{},
			want:    `{}`,
		},
		{
			name:    "single field",
			row:     []any{42},
			columns: Columns{stringCol("id")},
			want:    `{"id":42}`,
		},
		{
			name:    "multiple fields preserve column order (not alphabetical)",
			row:     []any{1, "alice", true},
			columns: Columns{stringCol("zeta"), stringCol("alpha"), stringCol("mu")},
			want:    `{"zeta":1,"alpha":"alice","mu":true}`,
		},
		{
			name:    "nil values render as JSON null",
			row:     []any{nil, "x", nil},
			columns: Columns{stringCol("a"), stringCol("b"), stringCol("c")},
			want:    `{"a":null,"b":"x","c":null}`,
		},
		{
			name:    "JSON-typed column with valid JSON object string is inlined",
			row:     []any{`{"k":1}`},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":{"k":1}}`,
		},
		{
			name:    "JSON-typed column with valid JSON array string is inlined",
			row:     []any{`[1,2,3]`},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":[1,2,3]}`,
		},
		{
			name:    "JSON-typed column with literal 'null' string becomes JSON null",
			row:     []any{"null"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":null}`,
		},
		{
			name:    "JSON-typed column with non-JSON-looking string stays quoted",
			row:     []any{"hello"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":"hello"}`,
		},
		{
			name:    "JSON-typed column with malformed JSON-looking string stays quoted",
			row:     []any{"{not-json"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":"{not-json"}`,
		},
		{
			name:    "string-typed column with JSON-looking string stays quoted (no inlining)",
			row:     []any{`{"k":1}`},
			columns: Columns{stringCol("payload")},
			want:    `{"payload":"{\"k\":1}"}`,
		},
		{
			name:    "column names with special characters are escaped",
			row:     []any{1, 2},
			columns: Columns{stringCol(`a"b`), stringCol("c\nd")},
			want:    `{"a\"b":1,"c\nd":2}`,
		},
		{
			name:    "values with special characters are escaped",
			row:     []any{`he said "hi"` + "\n"},
			columns: Columns{stringCol("msg")},
			want:    `{"msg":"he said \"hi\"\n"}`,
		},
		{
			name:    "row longer than columns is truncated",
			row:     []any{1, 2, 3, 4},
			columns: Columns{stringCol("a"), stringCol("b")},
			want:    `{"a":1,"b":2}`,
		},
		{
			name:    "row shorter than columns stops at row length",
			row:     []any{1},
			columns: Columns{stringCol("a"), stringCol("b")},
			want:    `{"a":1}`,
		},
		{
			name:    "mixed JSON and scalar columns keep declared order",
			row:     []any{1, `{"nested":true}`, "tail"},
			columns: Columns{stringCol("id"), jsonCol("meta"), stringCol("tag")},
			want:    `{"id":1,"meta":{"nested":true},"tag":"tail"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeRowAsJSONObject(tt.row, tt.columns)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none; output=%s", string(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("output mismatch\n  got:  %s\n  want: %s", string(got), tt.want)
			}
			// Belt-and-suspenders: every successful result must be valid JSON.
			if !json.Valid(got) {
				t.Fatalf("output is not valid JSON: %s", string(got))
			}
		})
	}
}

func TestReaderReadyRetriesFailedOpenAndClose(t *testing.T) {
	opens := 0
	rr := &ReaderReady{
		URI: "s3://bucket/file.csv",
		Open: func() (io.Reader, error) {
			opens++
			if opens == 1 {
				return nil, errors.New("temporary")
			}
			return io.NopCloser(strings.NewReader("ok")), nil
		},
	}

	if _, err := rr.GetReader(); err == nil {
		t.Fatal("expected first Open to fail")
	}
	r, err := rr.GetReader()
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if opens != 2 {
		t.Fatalf("opens=%d, want 2", opens)
	}
	buf := make([]byte, 2)
	n, _ := r.Read(buf)
	if string(buf[:n]) != "ok" {
		t.Fatalf("got %q", buf[:n])
	}
	if err := rr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := rr.GetReader(); err == nil {
		t.Fatal("GetReader after Close should fail")
	}
}

// dsArrowTestMetadata requests every metadata column the task layer can ask for.
func dsArrowTestMetadata() Metadata {
	return Metadata{
		SyncedAt:  KeyValue{Key: "_sling_loaded_at", Value: time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC)},
		SyncedOp:  KeyValue{Key: "_sling_synced_op", Value: "I"},
		SyncedSeq: KeyValue{Key: "_sling_synced_seq", Value: int64(0)},
		StreamURL: KeyValue{Key: "_sling_stream_url", Value: "table/one"},
		RowID:     KeyValue{Key: "_sling_row_id"},
		ExecID:    KeyValue{Key: "_sling_exec_id", Value: "exec-1"},
		RowNum:    KeyValue{Key: "_sling_row_num"},
	}
}

// TestDatastreamMetaColumnValues pins the shared definitions: the same builder
// feeds the row path and the lane, so this is the contract for both.
func TestDatastreamMetaColumnValues(t *testing.T) {
	ds := &Datastream{
		Columns: Columns{
			{Name: "id", Type: BigIntType, Position: 1},
			{Name: "_sling_row_num", Type: BigIntType, Position: 2},
		},
		Metadata: dsArrowTestMetadata(),
	}

	cols := ds.metaColumnValues()
	require.Len(t, cols, 7)

	assert.Equal(t, []string{
		"_sling_loaded_at", "_sling_synced_op", "_sling_synced_seq",
		"_sling_stream_url", "_sling_row_num_", "_sling_row_id", "_sling_exec_id",
	}, lo.Map(cols, func(mc MetaColumn, _ int) string { return mc.Column.Name }),
		"the source column named _sling_row_num keeps its place, so that column is renamed")
	assert.Equal(t, 3, cols[0].Column.Position, "positions follow the stream columns")
	assert.Equal(t, TimestampzType, cols[0].Column.Type)
	assert.Equal(t, BigIntType, cols[2].Column.Type)
	assert.Equal(t, StringType, cols[1].Column.Type)
	assert.Equal(t, 4, cols[1].Column.DbPrecision)

	// row numbers and the synced sequence count rows, the rest are fixed
	assert.Equal(t, int64(1), cols[4].Value(1))
	assert.Equal(t, int64(7), cols[4].Value(7))
	assert.Equal(t, int64(1), cols[2].Value(1), "the synced sequence counts from the config value")
	assert.Equal(t, int64(2), cols[2].Value(2))
	assert.Equal(t, "I", cols[1].Value(1))
	assert.Equal(t, "exec-1", cols[6].Value(1))
	assert.Equal(t, "table/one", cols[3].Value(1))
	assert.Equal(t, time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC), cols[0].Value(1))

	rowID, ok := cols[5].Value(1).(string)
	require.True(t, ok, "the row id is a string")
	assert.Len(t, rowID, 27, "the row id is a ksuid")
	otherID, _ := cols[5].Value(1).(string)
	assert.NotEqual(t, rowID, otherID, "every row gets its own id")
}

// TestDatastreamMetaColumnValues_None covers a stream with no metadata columns.
func TestDatastreamMetaColumnValues_None(t *testing.T) {
	ds := &Datastream{Columns: Columns{{Name: "id", Type: BigIntType, Position: 1}}}
	assert.Empty(t, ds.metaColumnValues())
}

// TestDatastreamArrow_MetaColumns covers the lane: the datastream reports the
// metadata columns, the sample carries them, every record carries one value per
// row, and the counters run across records.
func TestDatastreamArrow_MetaColumns(t *testing.T) {
	origSampleSize := SampleSize
	SampleSize = 2
	defer func() { SampleSize = origSampleSize }()

	e := dsArrowTestEnvNew(t, 8)
	e.ds.Metadata = dsArrowTestMetadata()

	recs := []arrow.RecordBatch{
		dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(1), "a"}, {int64(2), "b"}}),
		dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(3), nil}}),
	}
	for _, rec := range recs {
		require.NoError(t, e.rs.Push(rec))
	}
	e.rs.Close(nil)

	require.NoError(t, e.ds.Start())

	wantNames := []string{
		"id", "name",
		"_sling_loaded_at", "_sling_synced_op", "_sling_synced_seq",
		"_sling_stream_url", "_sling_row_num", "_sling_row_id", "_sling_exec_id",
	}
	assert.Equal(t, wantNames, e.ds.Columns.Names(), "the datastream reports the metadata columns")
	assert.Len(t, e.ds.Buffer, 2, "the sample is the first record")
	require.Len(t, e.ds.Buffer[0], len(wantNames), "the sample rows carry the metadata values")
	assert.Equal(t, "I", e.ds.Buffer[0][3])
	assert.Equal(t, int64(1), e.ds.Buffer[0][6])
	assert.Equal(t, "I", e.ds.Buffer[1][3])
	assert.Equal(t, int64(2), e.ds.Buffer[1][6])

	// the records carry the values the sink writes
	rec, ok := e.rs.Next()
	require.True(t, ok, "the peeked record comes back")
	idx := openFakeIndex(rec)
	require.Len(t, rec.Schema().Fields(), len(wantNames))

	assert.Equal(t, int64(1), openFakeVal(t, rec, idx["_sling_row_num"], 0))
	assert.Equal(t, int64(2), openFakeVal(t, rec, idx["_sling_row_num"], 1))
	assert.Equal(t, int64(1), openFakeVal(t, rec, idx["_sling_synced_seq"], 0))
	assert.Equal(t, int64(2), openFakeVal(t, rec, idx["_sling_synced_seq"], 1))
	assert.Equal(t, "I", openFakeVal(t, rec, idx["_sling_synced_op"], 0))
	assert.Equal(t, "exec-1", openFakeVal(t, rec, idx["_sling_exec_id"], 0))
	assert.Equal(t, "table/one", openFakeVal(t, rec, idx["_sling_stream_url"], 0))
	assert.Equal(t, time.Date(2026, 9, 22, 10, 0, 0, 0, time.UTC), openFakeVal(t, rec, idx["_sling_loaded_at"], 0))
	assert.Len(t, openFakeVal(t, rec, idx["_sling_row_id"], 0), 27)
	assert.NotEqual(t,
		openFakeVal(t, rec, idx["_sling_row_id"], 0),
		openFakeVal(t, rec, idx["_sling_row_id"], 1),
	)
	rec.Release()

	rec, ok = e.rs.Next()
	require.True(t, ok)
	idx = openFakeIndex(rec)
	assert.Equal(t, int64(3), openFakeVal(t, rec, idx["_sling_row_num"], 0), "the counters run across records")
	assert.Equal(t, int64(3), openFakeVal(t, rec, idx["_sling_synced_seq"], 0))
	rec.Release()

	_, ok = e.rs.Next()
	assert.False(t, ok)
}

// metaOrderLane records the schema the transform was handed, so the test can
// tell whether the metadata columns were appended before the stages ran.
type metaOrderLane struct {
	*openFakeLane
	sawFields []string
}

func (l *metaOrderLane) NewTransform(stages []map[string]string, sp *StreamProcessor) (RecordTransform, error) {
	return &metaOrderTransform{lane: l}, nil
}

type metaOrderTransform struct {
	lane *metaOrderLane
}

func (t *metaOrderTransform) Transform(rec arrow.RecordBatch, cols Columns) (arrow.RecordBatch, Columns, error) {
	for _, f := range rec.Schema().Fields() {
		t.lane.sawFields = append(t.lane.sawFields, f.Name)
	}
	return rec, cols, nil
}

// TestDatastreamArrow_MetaColumnsNil covers a stream that never sets metadata:
// no columns are appended and the records keep their schema.
func TestDatastreamArrow_MetaColumnsNil(t *testing.T) {
	e := dsArrowTestEnvNew(t, 8)
	require.NoError(t, e.rs.Push(dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(1), "a"}})))
	e.rs.Close(nil)
	require.NoError(t, e.ds.Start())

	assert.Equal(t, []string{"id", "name"}, e.ds.Columns.Names())

	rec, ok := e.rs.Next()
	require.True(t, ok)
	assert.Equal(t, []string{"id", "name"}, lo.Map(rec.Schema().Fields(), func(f arrow.Field, _ int) string { return f.Name }))
	rec.Release()
}

// dsArrowTestSchema is the two-column (int64, utf8) schema the tests share.
func dsArrowTestSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func dsArrowTestAppend(b array.Builder, v any) {
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
		panic(fmt.Sprintf("dsArrowTestAppend: unsupported builder %T", b))
	}
}

// dsArrowTestRecord builds a record in schema; a nil value becomes null.
func dsArrowTestRecord(t testing.TB, alloc memory.Allocator, schema *arrow.Schema, rows [][]any) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	for _, row := range rows {
		for i, v := range row {
			dsArrowTestAppend(b.Field(i), v)
		}
	}
	return b.NewRecordBatch()
}

// dsArrowTestEnv is an Arrow datastream over a record stream on the fake lane.
// The checked allocator is asserted empty once the test ends, so every record
// the test builds must be consumed or drained.
type dsArrowTestEnv struct {
	alloc  *memory.CheckedAllocator
	schema *arrow.Schema
	rs     *RecordStream
	ds     *Datastream
}

func dsArrowTestEnvNew(t *testing.T, size int) *dsArrowTestEnv {
	t.Helper()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	ctx := g.NewContext(context.Background())
	t.Cleanup(ctx.Cancel)

	schema := dsArrowTestSchema()
	rs := NewRecordStream(ctx, &openFakeLane{alloc: alloc}, schema, size)
	ds := NewDatastreamArrow(ctx.Ctx, ArrowSchemaToColumns(schema), rs)
	return &dsArrowTestEnv{alloc: alloc, schema: schema, rs: rs, ds: ds}
}

// push builds a one-row record and hands it to the stream.
func (e *dsArrowTestEnv) push(t testing.TB, row ...any) {
	t.Helper()
	require.NoError(t, e.rs.Push(dsArrowTestRecord(t, e.alloc, e.schema, [][]any{row})))
}

// drain takes every record the stream still holds, the way a sink does, and
// releases each one.
func (e *dsArrowTestEnv) drain() {
	for {
		rec, ok := e.rs.Next()
		if !ok {
			return
		}
		rec.Release()
	}
}

// TestDatastreamArrow_Start covers the buffer the sink samples before the
// stream is ready: it is capped at SampleSize, the first record is peeked (not
// consumed), the columns pick up the stream's column_casing, and Count/Bytes
// match the records once they are all taken.
func TestDatastreamArrow_Start(t *testing.T) {
	origSampleSize := SampleSize
	SampleSize = 2
	defer func() { SampleSize = origSampleSize }()

	e := dsArrowTestEnvNew(t, 8)
	e.ds.SetConfig(map[string]string{
		"column_casing": "upper",
		"target_type":   string(dbio.TypeFileLocal),
	})

	recs := []arrow.RecordBatch{
		dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(1), "a"}, {int64(2), "b"}, {int64(3), "c"}}),
		dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(4), nil}}),
	}
	var wantBytes int64
	for _, rec := range recs {
		wantBytes += TotalRecordSize(rec)
		require.NoError(t, e.rs.Push(rec))
	}
	e.rs.Close(nil)

	require.NoError(t, e.ds.Start())
	assert.True(t, e.ds.Ready)
	assert.True(t, e.ds.Inferred, "an arrow stream is always inferred")
	assert.Len(t, e.ds.Buffer, SampleSize, "the buffer holds the first record up to SampleSize")
	assert.Equal(t, []string{"ID", "NAME"}, e.ds.Columns.Names(), "column_casing renames the columns")
	// the sample did not consume the first record
	assert.Equal(t, uint64(0), e.ds.Count)

	e.drain()
	require.NoError(t, e.rs.Err())
	assert.Equal(t, uint64(4), e.ds.Count, "Count is the rows of every record taken")
	assert.Equal(t, uint64(wantBytes), e.ds.Bytes.Load(), "Bytes is the buffer size of every record taken")
}

// TestDatastreamArrow_Empty covers an input that never pushes a record: the
// stream still goes ready and reports zero rows.
func TestDatastreamArrow_Empty(t *testing.T) {
	e := dsArrowTestEnvNew(t, 4)
	e.rs.Close(nil)

	require.NoError(t, e.ds.Start())
	assert.True(t, e.ds.Ready)
	assert.True(t, e.ds.Inferred)
	assert.True(t, e.ds.empty, "no record means an empty stream")
	assert.Empty(t, e.ds.Buffer)
	assert.Equal(t, uint64(0), e.ds.Count)
	assert.Equal(t, uint64(0), e.ds.Bytes.Load())
}

// TestDatastreamArrow_Pause covers pause/unpause on an Arrow stream. The
// bounded record channel is the backpressure, so the producer is held there
// and only a consumer taking a record releases it; the pause flag never
// blocks the caller.
func TestDatastreamArrow_Pause(t *testing.T) {
	e := dsArrowTestEnvNew(t, 1) // one slot: the second push must wait

	require.NoError(t, e.rs.Push(dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(1), "a"}})))

	pushed := make(chan error, 1)
	go func() {
		pushed <- e.rs.Push(dsArrowTestRecord(t, e.alloc, e.schema, [][]any{{int64(2), "b"}}))
	}()

	select {
	case <-pushed:
		t.Fatal("Push completed with the record channel full")
	case <-time.After(50 * time.Millisecond):
	}

	assert.True(t, e.ds.TryPause(), "TryPause always takes on an Arrow stream")
	assert.True(t, e.ds.paused)

	paused := make(chan struct{})
	go func() {
		e.ds.Pause()
		close(paused)
	}()
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("Pause blocked on an Arrow stream")
	}

	e.ds.Unpause()
	assert.False(t, e.ds.paused, "Unpause clears the flag")

	select {
	case <-pushed:
		t.Fatal("Unpause does not release the producer; only a consumer does")
	case <-time.After(50 * time.Millisecond):
	}

	rec, ok := e.rs.Next()
	require.True(t, ok)
	rec.Release()
	require.NoError(t, <-pushed, "taking a record releases the producer")

	e.rs.Close(nil)
	e.drain()
	require.NoError(t, e.rs.Err())
}

// TestDatastreamArrow_CloseMidStream covers Close on a stream whose producer
// is still going: everything queued or peeked is released.
func TestDatastreamArrow_CloseMidStream(t *testing.T) {
	e := dsArrowTestEnvNew(t, 8)
	for i := 0; i < 3; i++ {
		e.push(t, int64(i), "x")
	}

	require.NoError(t, e.ds.Start()) // peeks the first record
	rec, ok := e.rs.Next()
	require.True(t, ok)
	rec.Release()

	e.ds.Close()
	e.rs.Close(nil) // producer done; the drain keeps nothing
	assert.Equal(t, 0, e.alloc.CurrentAlloc(), "Close releases every remaining record")
}

// TestDatastreamArrow_RowConsumerGuard covers the two row-path entry points a
// sink must not take on an Arrow stream: they record the wiring bug on the
// stream context instead of panicking.
func TestDatastreamArrow_RowConsumerGuard(t *testing.T) {
	const guard = "arrow lane: row consumer on an Arrow stream"

	e := dsArrowTestEnvNew(t, 2)
	var batch *Batch
	require.NotPanics(t, func() { batch = e.ds.NewBatch(e.ds.Columns) })
	assert.NotNil(t, batch)
	require.ErrorContains(t, e.ds.Context.Err(), guard)

	e2 := dsArrowTestEnvNew(t, 2)
	rows := e2.ds.Rows()
	_, ok := <-rows
	assert.False(t, ok, "Rows yields a closed channel")
	require.ErrorContains(t, e2.ds.Context.Err(), guard)
}

// TestDatastreamArrow_SyncStats covers the stats the dataflow reports for an
// Arrow stream, which never runs the cast pass. TotalCnt and NullCnt must
// match the row path for the same records, and TrackMax must fill Max/LastVal.
func TestDatastreamArrow_SyncStats(t *testing.T) {
	e := dsArrowTestEnvNew(t, 8)
	e.rs.TrackMax(0)

	rows := [][]any{
		{int64(4), "d"},
		{int64(9), nil},
		{int64(2), "b"},
		{int64(7), "a"},
		{nil, nil},
	}
	for _, row := range rows {
		e.push(t, row...)
	}
	e.rs.Close(nil)

	require.NoError(t, e.ds.Start())
	df, err := MakeDataFlow(e.ds)
	require.NoError(t, err)

	e.drain()
	require.NoError(t, e.rs.Err())
	df.SyncStats()

	// reference: the row loop casts every row through ds.Sp.CastRow
	want := NewDatastreamContext(context.Background(), e.ds.Columns.Clone())
	defer want.Close()
	for _, row := range rows {
		want.Sp.CastRow(append([]any(nil), row...), want.Columns)
	}
	wantIdx := want.Columns.FieldMap(true)

	for _, col := range df.Columns {
		wi, ok := wantIdx[strings.ToLower(col.Name)]
		require.True(t, ok, "column %s is in the row path", col.Name)
		wantCs := want.Sp.ColStats()[wi]
		require.NotNil(t, wantCs, "column %s has row-path stats", col.Name)
		assert.Equal(t, wantCs.TotalCnt, col.Stats.TotalCnt, "TotalCnt of %s", col.Name)
		assert.Equal(t, wantCs.NullCnt, col.Stats.NullCnt, "NullCnt of %s", col.Name)
	}

	assert.Equal(t, uint64(len(rows)), df.Count())
	assert.Equal(t, int64(9), df.Columns[0].Stats.Max, "TrackMax fills Max")
	assert.Equal(t, int64(9), df.Columns[0].Stats.LastVal, "TrackMax fills LastVal")
}

// cdcArrowTestFile writes an Arrow IPC file with the given records, one nested
// slice per record, and returns its path. Every record is released: the file is
// the only fixture.
func cdcArrowTestFile(t testing.TB, alloc memory.Allocator, schema *arrow.Schema, records [][][]any) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "cache.arrow")
	file, err := os.Create(path)
	require.NoError(t, err)

	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(schema), ipc.WithAllocator(alloc))
	require.NoError(t, err)

	for _, rows := range records {
		rec := dsArrowTestRecord(t, alloc, schema, rows)
		require.NoError(t, writer.Write(rec))
		rec.Release()
	}

	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())

	return path
}

// cdcArrowTestDamageBody zeroes the record body of an Arrow IPC file and keeps
// its footer, so the schema still reads and the records do not.
func cdcArrowTestDamageBody(t testing.TB, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(data), 32)

	// the file ends with the footer size (4 bytes) and the magic (6 bytes);
	// the footer starts right before them
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-10 : len(data)-6]))
	bodyStart, bodyEnd := len(ipc.Magic)+2, len(data)-10-footerLen
	require.Less(t, bodyStart, bodyEnd, "the test file has no body to damage")

	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	defer file.Close()

	_, err = file.WriteAt(make([]byte, bodyEnd-bodyStart), int64(bodyStart))
	require.NoError(t, err)
}

// cdcArrowTestRows drains the records of the stream the way a sink does and
// returns the rows. Strings are cloned, so the values outlive the records.
func cdcArrowTestRows(t *testing.T, rs *RecordStream) (rows [][]any) {
	t.Helper()

	for {
		rec, ok := rs.Next()
		if !ok {
			break
		}

		for r := range int(rec.NumRows()) {
			row := make([]any, rec.NumCols())
			for c := range int(rec.NumCols()) {
				val := GetValueFromArrowArray(rec.Column(c), r)
				if s, ok := val.(string); ok {
					val = strings.Clone(s)
				}
				row[c] = val
			}
			rows = append(rows, row)
		}

		rec.Release()
	}

	require.NoError(t, rs.Err())
	return rows
}

// TestConsumeArrowRecords_RoundTrip checks the cache-file read: the records
// come through in order, the footer schema is the stream schema, the row and
// byte counts follow, and the datastream closes the file it was given.
func TestConsumeArrowRecords_RoundTrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	schema := dsArrowTestSchema()
	path := cdcArrowTestFile(t, alloc, schema, [][][]any{
		{{int64(1), "a"}, {int64(2), nil}, {int64(3), "c"}},
		{{int64(4), "d"}},
	})

	file, err := os.Open(path)
	require.NoError(t, err)

	ds := NewDatastreamContext(context.Background(), nil)
	require.NoError(t, ds.ConsumeArrowRecords(file, &openFakeLane{alloc: alloc}))

	assert.True(t, ds.ArrowOnly)
	assert.Equal(t, []string{"id", "name"}, ds.Columns.Names())
	assert.Equal(t, [][]any{{int64(1), "a"}, {int64(2), nil}, {int64(3), "c"}}, ds.Buffer,
		"the first record is sampled")

	rs := ds.RecordStream()
	require.NotNil(t, rs)
	assert.True(t, rs.Schema.Equal(schema), "the file footer is the stream schema")

	rows := cdcArrowTestRows(t, rs)
	assert.Equal(t, [][]any{
		{int64(1), "a"}, {int64(2), nil}, {int64(3), "c"}, {int64(4), "d"},
	}, rows)
	assert.Equal(t, uint64(4), ds.Count)
	assert.Greater(t, ds.Bytes.Load(), uint64(0))

	ds.Close()

	// the datastream owns the file: the read closed it
	_, err = file.Stat()
	assert.Error(t, err, "the cache file is closed with the datastream")
}

// TestConsumeArrowRecords_Errors checks error propagation: a file that is not
// Arrow fails at open, and a cache file with a damaged body fails on the first
// record, so the datastream reports it instead of hanging or panicking.
func TestConsumeArrowRecords_Errors(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })
	lane := &openFakeLane{alloc: alloc}

	t.Run("not an arrow file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "cache.arrow")
		require.NoError(t, os.WriteFile(path, []byte("not an arrow file at all"), 0o600))

		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()

		_, err = ArrowIPCFileSchema(file)
		require.Error(t, err)

		ds := NewDatastreamContext(context.Background(), nil)
		err = ds.ConsumeArrowRecords(file, lane)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not read arrow file")
	})

	t.Run("damaged record body", func(t *testing.T) {
		path := cdcArrowTestFile(t, alloc, dsArrowTestSchema(), [][][]any{{{int64(1), "a"}}})
		cdcArrowTestDamageBody(t, path)

		file, err := os.Open(path)
		require.NoError(t, err)
		defer file.Close()

		// the footer still reads, so the failure comes from the record
		footer, err := ArrowIPCFileSchema(file)
		require.NoError(t, err)
		require.Equal(t, dsArrowTestSchema().Fields(), footer.Fields())

		ds := NewDatastreamContext(context.Background(), nil)
		err = ds.ConsumeArrowRecords(file, lane)
		require.Error(t, err)
		// g.Error renders the innermost message: the failing record is named
		assert.Contains(t, err.Error(), "arrow record 0")
		assert.Error(t, ds.rs.Err(), "the stream carries the read error")
	})
}

// TestArrowIPCFileSchema_Rewinds checks the footer read the CDC gate uses: it
// reads the schema and rewinds, so the same handle then reads the records.
func TestArrowIPCFileSchema_Rewinds(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	schema := dsArrowTestSchema()
	path := cdcArrowTestFile(t, alloc, schema, [][][]any{{{int64(1), "a"}, {int64(2), nil}}})

	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()

	footer, err := ArrowIPCFileSchema(file)
	require.NoError(t, err)
	require.True(t, footer.Equal(schema))

	ds := NewDatastreamContext(context.Background(), nil)
	require.NoError(t, ds.ConsumeArrowRecords(file, &openFakeLane{alloc: alloc}))
	assert.Equal(t, [][]any{{int64(1), "a"}, {int64(2), nil}}, cdcArrowTestRows(t, ds.RecordStream()))
	ds.Close()
}
