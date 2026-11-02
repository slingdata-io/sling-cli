package iop

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArrowReadWrite(t *testing.T) {
	// Create test columns
	columns := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
		{Name: "value", Type: FloatType, Position: 3},
		{Name: "active", Type: BoolType, Position: 4},
		{Name: "dec_val", Type: DecimalType, DbPrecision: 38, DbScale: 6, Position: 5},
	}

	// Create a buffer to write to
	buf := &bytes.Buffer{}

	// Create writer
	writer, err := NewArrowWriter(buf, columns)
	assert.NoError(t, err)

	// Write some test data
	testData := [][]any{
		{1, "Alice", 10.5, true, "50000"},
		{2, "Bob", 20.3, false, "123.456"},
		{3, "Charlie", 30.7, true, "999.999995"},
		{4, nil, 40.2, nil, "1"},
		{5, "David", 50.1, true, "0.001"},
		{6, "Eve", 60.9, false, nil}, // Test null values
	}

	for _, row := range testData {
		err := writer.WriteRow(row)
		assert.NoError(t, err)
	}

	// Close writer
	err = writer.Close()
	assert.NoError(t, err)

	// Create a temp file from buffer
	tmpFile, err := os.CreateTemp("", "test_arrow_*.arrow")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Open file for reading
	readFile, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer readFile.Close()

	// Create reader
	reader, err := NewArrowFileReader(readFile, nil)
	assert.NoError(t, err)

	// Verify columns
	readCols := reader.Columns()
	assert.Equal(t, len(columns), len(readCols))
	for i, col := range columns {
		assert.Equal(t, col.Name, readCols[i].Name)
		assert.Equal(t, col.Type, readCols[i].Type)
	}

	// Read data using datastream
	ds := NewDatastreamContext(context.Background(), reader.columns)
	it := ds.NewIterator(reader.columns, reader.nextFunc)

	rowCount := 0
	for it.next() {
		row := it.Row
		assert.Equal(t, len(columns), len(row))

		// Verify data matches what we wrote
		if rowCount < len(testData) {
			expected := testData[rowCount]
			for i, val := range expected {
				if val == nil {
					assert.Nil(t, row[i])
				} else {
					// Handle type conversions for integer values
					if intVal, ok := val.(int); ok {
						assert.Equal(t, int64(intVal), row[i])
					} else if decVal, ok := val.(string); ok && i == 4 {
						// For decimal, compare string representation
						dec, err := decimal.NewFromString(decVal)
						assert.NoError(t, err)

						// The reader gives us a string, so we parse it back to decimal
						resDec, err := decimal.NewFromString(row[i].(string))
						assert.NoError(t, err)

						assert.True(t, dec.Equal(resDec), "expected %s, got %s", dec.String(), resDec.String())
					} else {
						assert.Equal(t, val, row[i])
					}
				}
			}
		}
		rowCount++
	}

	assert.Equal(t, len(testData), rowCount)
}

func TestArrowReaderWithSelectedColumns(t *testing.T) {
	// Create test columns
	columns := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
		{Name: "value", Type: FloatType, Position: 3},
	}

	// Create a buffer to write to
	buf := &bytes.Buffer{}

	// Create writer and write test data
	writer, err := NewArrowWriter(buf, columns)
	assert.NoError(t, err)

	testData := [][]any{
		{1, "Alice", 10.5},
		{2, "Bob", 20.3},
	}

	for _, row := range testData {
		err := writer.WriteRow(row)
		assert.NoError(t, err)
	}

	err = writer.Close()
	assert.NoError(t, err)

	// Create a temp file from buffer
	tmpFile, err := os.CreateTemp("", "test_arrow_selected_*.arrow")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Open file for reading with selected columns
	readFile, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer readFile.Close()

	// Create reader with only "id" and "value" columns
	reader, err := NewArrowFileReader(readFile, []string{"id", "value"})
	assert.NoError(t, err)

	// Verify selected columns
	readCols := reader.Columns()
	assert.Equal(t, 2, len(readCols))
	assert.Equal(t, "id", readCols[0].Name)
	assert.Equal(t, "value", readCols[1].Name)

	// Read data using datastream
	ds := NewDatastreamContext(context.Background(), reader.columns)
	it := ds.NewIterator(reader.columns, reader.nextFunc)

	rowCount := 0
	for it.next() {
		row := it.Row
		assert.Equal(t, 2, len(row)) // Only 2 selected columns

		if rowCount < len(testData) {
			// Handle type conversions for integer values
			assert.Equal(t, int64(testData[rowCount][0].(int)), row[0]) // id
			assert.Equal(t, testData[rowCount][2], row[1])              // value (skipping name)
		}
		rowCount++
	}

	assert.Equal(t, len(testData), rowCount)
}

// ColumnsToArrowSchema must map time/timez/uuid to their native Arrow types so the
// schema matches iopTypeToIcebergPrimitiveType. Previously they fell through to
// String, causing "cannot promote string to time/uuid" when appending to Iceberg.
func TestArrowColumnsToArrowSchemaTimeUUID(t *testing.T) {
	columns := Columns{
		{Name: "t", Type: TimeType, Position: 1},
		{Name: "tz", Type: TimezType, Position: 2},
		{Name: "u", Type: UUIDType, Position: 3},
	}

	schema := ColumnsToArrowSchema(columns)

	assert.IsType(t, &arrow.Time64Type{}, schema.Field(0).Type)
	assert.IsType(t, &arrow.Time64Type{}, schema.Field(1).Type)
	_, isUUID := schema.Field(2).Type.(*extensions.UUIDType)
	assert.True(t, isUUID, "uuid column should map to the arrow.uuid extension type, got %T", schema.Field(2).Type)
}

// TestArrowTimestampZonePreserved asserts a timestamp survives the arrow round
// trip with its zone *label* intact, not just its instant.
//
// The label matters because values are written out with RFC3339Nano, so it
// becomes the stored offset in targets like Snowflake TIMESTAMP_TZ. The read
// path used to force .UTC() on every timestamp, and ColumnsToArrowSchema built
// the field from the arrow.FixedWidthTypes.Timestamp_* globals, which are
// hardcoded to "UTC". Together those relabeled every CDC value: rows landed as
// +00:00 while snapshot rows of the same table kept -07:00/-08:00.
func TestArrowTimestampZonePreserved(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	col := Column{
		Name:     "create_time",
		Type:     TimestampzType,
		Position: 1,
		Metadata: map[string]string{"timeUnit": "ns", "timeZone": la.String()},
	}

	// The schema must carry the zone; otherwise the read path has nothing to
	// restore from.
	schema := ColumnsToArrowSchema(Columns{col})
	tsType, ok := schema.Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok, "expected arrow.TimestampType, got %T", schema.Field(0).Type)
	assert.Equal(t, la.String(), tsType.TimeZone, "schema must carry the column's zone")

	// 18:49:20 PDT — the wall clock a MySQL DATETIME would hold.
	orig := time.Date(2026, 8, 9, 18, 49, 20, 0, la)

	builder := array.NewTimestampBuilder(memory.NewGoAllocator(), tsType)
	defer builder.Release()
	AppendToBuilder(builder, &col, orig)

	arr := builder.NewArray()
	defer arr.Release()

	got, ok := GetValueFromArrowArray(arr, 0).(time.Time)
	require.True(t, ok, "expected time.Time from arrow array")

	assert.True(t, got.Equal(orig), "instant must be unchanged: got %s, want %s", got, orig)
	assert.Equal(t, orig.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano),
		"zone label must survive the round trip (this is what reaches Snowflake TIMESTAMP_TZ)")
}

// A column with no zone metadata must still round-trip as UTC, so existing
// behaviour is unchanged for producers that never set a zone.
func TestArrowTimestampDefaultsToUTC(t *testing.T) {
	col := Column{
		Name:     "ts",
		Type:     TimestampType,
		Position: 1,
		Metadata: map[string]string{"timeUnit": "ns"},
	}

	schema := ColumnsToArrowSchema(Columns{col})
	tsType, ok := schema.Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok)
	assert.Equal(t, "UTC", tsType.TimeZone)

	orig := time.Date(2026, 8, 9, 18, 49, 20, 0, time.UTC)

	builder := array.NewTimestampBuilder(memory.NewGoAllocator(), tsType)
	defer builder.Release()
	AppendToBuilder(builder, &col, orig)

	arr := builder.NewArray()
	defer arr.Release()

	got, ok := GetValueFromArrowArray(arr, 0).(time.Time)
	require.True(t, ok)
	assert.Equal(t, "2026-08-09T18:49:20Z", got.Format(time.RFC3339Nano))
}

// A datetime is a zone-less wall clock and must stay one across the arrow round
// trip. Arrow reads any non-empty TimeZone as "this is an instant", so giving
// the field a UTC zone by default promoted datetime to timestampz and landed
// MySQL DATETIME columns in Snowflake as TIMESTAMP_TZ instead of TIMESTAMP_NTZ.
func TestArrowDatetimeStaysZoneless(t *testing.T) {
	dt := Column{Name: "new_dt", Type: DatetimeType, Position: 1}
	tz := Column{
		Name:     "new_ts",
		Type:     TimestampzType,
		Position: 2,
		Metadata: map[string]string{"timeZone": "America/Los_Angeles"},
	}

	schema := ColumnsToArrowSchema(Columns{dt, tz})

	dtType, ok := schema.Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok)
	assert.Empty(t, dtType.TimeZone, "datetime must not carry a zone")

	tzType, ok := schema.Field(1).Type.(*arrow.TimestampType)
	require.True(t, ok)
	assert.Equal(t, "America/Los_Angeles", tzType.TimeZone, "timestampz must keep its zone")

	// The inferred columns are what drive target DDL.
	cols := ArrowSchemaToColumns(schema)
	assert.Equal(t, DatetimeType, cols[0].Type, "datetime must not be promoted to timestampz")
	assert.Equal(t, TimestampzType, cols[1].Type)

	// A zone-less column stores a wall clock, so the digits must survive even
	// when the incoming value carries an offset. Taking the epoch here shifted
	// 23:45 -07:00 to 06:45 in Snowflake TIMESTAMP_NTZ.
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)
	orig := time.Date(2026, 8, 9, 23, 45, 0, 0, la)

	builder := array.NewTimestampBuilder(memory.NewGoAllocator(), dtType)
	defer builder.Release()
	AppendToBuilder(builder, &dt, orig)

	arr := builder.NewArray()
	defer arr.Release()

	got, ok := GetValueFromArrowArray(arr, 0).(time.Time)
	require.True(t, ok)
	assert.Equal(t, "2026-08-09T23:45:00Z", got.Format(time.RFC3339Nano),
		"wall clock must be preserved for a zone-less column")
}

// A datetime given a zone keeps that label through the round trip without being
// promoted to timestampz. The CDC readers set the zone from the connection's
// `loc` so their rows carry the same offset the snapshot path gets from the
// driver, while the column still lands as TIMESTAMP_NTZ.
func TestArrowDatetimeKeepsZoneLabel(t *testing.T) {
	la, err := time.LoadLocation("America/Los_Angeles")
	require.NoError(t, err)

	col := Column{
		Name:     "create_time",
		Type:     DatetimeType,
		Position: 1,
		Metadata: map[string]string{"timeZone": la.String()},
	}

	schema := ColumnsToArrowSchema(Columns{col})
	tsType, ok := schema.Field(0).Type.(*arrow.TimestampType)
	require.True(t, ok)
	assert.Equal(t, la.String(), tsType.TimeZone, "zone must reach the schema")

	cols := ArrowSchemaToColumns(schema)
	assert.Equal(t, DatetimeType, cols[0].Type,
		"a zone must not promote a datetime to timestampz")

	orig := time.Date(2026, 8, 11, 14, 30, 0, 0, la)

	builder := array.NewTimestampBuilder(memory.NewGoAllocator(), tsType)
	defer builder.Release()
	AppendToBuilder(builder, &col, orig)

	arr := builder.NewArray()
	defer arr.Release()

	got, ok := GetValueFromArrowArray(arr, 0).(time.Time)
	require.True(t, ok)
	assert.True(t, got.Equal(orig), "instant must be unchanged")
	assert.Equal(t, orig.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano),
		"zone label must survive so CDC rows match snapshot rows")
}

// AppendToBuilder must fill a Time64 builder from a bare time-of-day string (as
// emitted by SQL `time` columns). Previously cast.ToTimeE rejected these and the
// value silently zeroed to 00:00:00.
func TestArrowAppendToBuilderTimeOfDay(t *testing.T) {
	col := &Column{Name: "t", Type: TimeType}
	builder := array.NewTime64Builder(memory.NewGoAllocator(), &arrow.Time64Type{Unit: arrow.Microsecond})
	defer builder.Release()

	AppendToBuilder(builder, col, "08:30:00.0000000")
	AppendToBuilder(builder, col, "08:30:00")
	AppendToBuilder(builder, col, nil)

	arr := builder.NewTime64Array()
	defer arr.Release()

	wantMicros := arrow.Time64(int64((8*3600 + 30*60) * 1e6))
	assert.Equal(t, wantMicros, arr.Value(0), "fractional time-of-day not parsed")
	assert.Equal(t, wantMicros, arr.Value(1), "plain time-of-day not parsed")
	assert.True(t, arr.IsNull(2), "nil should append null")
}

func TestArrowColumnsToArrowSchemaDecimal(t *testing.T) {
	cases := []struct {
		col  Column
		want arrow.DataType
	}{
		{Column{Name: "a", Type: DecimalType, DbPrecision: 25, DbScale: 8}, &arrow.Decimal128Type{Precision: 25, Scale: 8}},
		{Column{Name: "b", Type: DecimalType, DbPrecision: 10, DbScale: 0}, &arrow.Decimal128Type{Precision: 10, Scale: 0}},
		{Column{Name: "c", Type: DecimalType}, &arrow.Decimal128Type{Precision: int32(env.DdlMinDecLength), Scale: int32(env.DdlMinDecScale)}},
		{Column{Name: "d", Type: DecimalType, DbPrecision: 50, DbScale: 10}, &arrow.Decimal256Type{Precision: 50, Scale: 10}},
	}
	for _, c := range cases {
		got := ColumnsToArrowSchema(Columns{c.col}).Field(0).Type
		assert.Equal(t, c.want, got, c.col.Name)
	}
}

// the builder type gives the scale, not the column, and the value is cut
// toward zero
func TestArrowAppendToBuilderDecimal(t *testing.T) {
	col := &Column{Name: "d", Type: DecimalType} // no scale on the column
	mem := memory.NewGoAllocator()

	b128 := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 25, Scale: 8})
	defer b128.Release()
	for _, v := range []any{"12345678901234567.12345678", "-0.000000019", "0.000000019", "abc"} {
		AppendToBuilder(b128, col, v)
	}
	arr128 := b128.NewDecimal128Array()
	defer arr128.Release()
	assert.Equal(t, decimal128.FromBigInt(bigInt(t, "1234567890123456712345678")), arr128.Value(0))
	assert.Equal(t, decimal128.FromI64(-1), arr128.Value(1))
	assert.Equal(t, decimal128.FromI64(1), arr128.Value(2))
	assert.True(t, arr128.IsNull(3))

	b256 := array.NewDecimal256Builder(mem, &arrow.Decimal256Type{Precision: 50, Scale: 10})
	defer b256.Release()
	AppendToBuilder(b256, col, "1234567890123456789012345678901234567890.0123456789")
	arr256 := b256.NewDecimal256Array()
	defer arr256.Release()
	assert.Equal(t, "1234567890123456789012345678901234567890.0123456789", decimal256ToString(arr256.Value(0), 50, 10))
}

func bigInt(t *testing.T, s string) *big.Int {
	v, ok := new(big.Int).SetString(s, 10)
	require.True(t, ok)
	return v
}

func TestArrowParseTimeOfDayE(t *testing.T) {
	for _, s := range []string{"08:30:00.0000000", "08:30:00", "08:30"} {
		tVal, err := parseTimeOfDayE(s)
		assert.NoError(t, err, "input %q", s)
		assert.Equal(t, 8, tVal.Hour())
		assert.Equal(t, 30, tVal.Minute())
	}

	// full datetime still works (delegates to cast.ToTimeE)
	tVal, err := parseTimeOfDayE("2023-01-02 15:04:05")
	assert.NoError(t, err)
	assert.Equal(t, 15, tVal.Hour())

	_, err = parseTimeOfDayE("not a time")
	assert.Error(t, err)
}

// AppendToBuilder must round-trip a canonical UUID string into the arrow.uuid
// extension builder. With the schema fix, uuid columns now map to UUIDBuilder
// instead of String, so this value path is exercised end-to-end.
func TestArrowAppendToBuilderUUID(t *testing.T) {
	col := &Column{Name: "u", Type: UUIDType}
	builder := extensions.NewUUIDBuilder(memory.NewGoAllocator())
	defer builder.Release()

	AppendToBuilder(builder, col, "B86D9F47-F5E1-44A1-9576-1AEF61206EB1") // uppercase
	AppendToBuilder(builder, col, "b86d9f47-f5e1-44a1-9576-1aef61206eb1") // lowercase
	AppendToBuilder(builder, col, nil)

	arr := builder.NewArray().(*extensions.UUIDArray)
	defer arr.Release()

	want := "b86d9f47-f5e1-44a1-9576-1aef61206eb1"
	assert.Equal(t, want, arr.ValueStr(0), "uppercase uuid should normalize and round-trip")
	assert.Equal(t, want, arr.ValueStr(1), "lowercase uuid should round-trip")
	assert.True(t, arr.IsNull(2), "nil should append null")
}

// TestUnwrapSchemaExtensions pins the driver-extension handling the file sinks
// rely on: an ADBC reader hands back arrow.opaque fields whose arrays carry the
// storage values, and a Parquet file written with the extension in place cannot
// be read back.
func TestUnwrapSchemaExtensions(t *testing.T) {
	ext := extensions.NewOpaqueType(arrow.BinaryTypes.String, "numeric", "PostgreSQL")
	meta := arrow.NewMetadata(
		[]string{arrowExtensionNameKey, arrowExtensionMetadataKey, "ADBC:postgresql:typname"},
		[]string{"arrow.opaque", `{"type_name": "numeric"}`, "numeric"},
	)
	schema := arrow.NewSchema([]arrow.Field{{Name: "c_dec", Type: ext, Nullable: true, Metadata: meta}}, nil)

	out := unwrapSchemaExtensions(schema)
	require.NotSame(t, schema, out)
	assert.Equal(t, arrow.STRING, out.Field(0).Type.ID())
	assert.Equal(t, []string{"ADBC:postgresql:typname"}, out.Field(0).Metadata.Keys())

	plain := arrow.NewSchema([]arrow.Field{{Name: "c_str", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	assert.Same(t, plain, unwrapSchemaExtensions(plain))
	assert.Nil(t, unwrapSchemaExtensions(nil))
}

// TestUnwrapRecordExtensions covers the record side: the extension array is
// replaced by the storage array the target schema declares.
func TestUnwrapRecordExtensions(t *testing.T) {
	ext := extensions.NewOpaqueType(arrow.BinaryTypes.String, "numeric", "PostgreSQL")
	src := arrow.NewSchema([]arrow.Field{{Name: "c_dec", Type: ext, Nullable: true}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.Append("42")
	arr := array.NewExtensionArrayWithStorage(ext, b.NewArray())
	defer arr.Release()
	b.Release()
	rec := array.NewRecordBatch(src, []arrow.Array{arr}, 1)
	defer rec.Release()

	tgt := arrow.NewSchema([]arrow.Field{{Name: "c_dec", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	out, err := unwrapRecordExtensions(rec, tgt)
	require.NoError(t, err)
	defer out.Release()
	assert.Equal(t, tgt, out.Schema())
	assert.Equal(t, arrow.STRING, out.Column(0).DataType().ID())
	assert.Equal(t, "42", out.Column(0).(*array.String).Value(0))

	// a field the target schema does not carry is a hard error, not a cast
	other := arrow.NewSchema([]arrow.Field{{Name: "c_other", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	_, err = unwrapRecordExtensions(rec, other)
	require.Error(t, err)
}

// the writer raises the decimal size for its schema only; the columns give the target DDL
func TestArrowWriterKeepsColumns(t *testing.T) {
	columns := Columns{{Name: "d", Type: DecimalType, DbPrecision: 10, DbScale: 2}}
	var buf bytes.Buffer
	aw, err := NewArrowWriter(&buf, columns)
	require.NoError(t, err)
	require.NoError(t, aw.WriteRow([]any{decimal.RequireFromString("12.34")}))
	require.NoError(t, aw.Close())

	assert.Equal(t, 10, columns[0].DbPrecision)
	assert.Equal(t, 2, columns[0].DbScale)

	dt, ok := aw.arrowSchema.Field(0).Type.(*arrow.Decimal128Type)
	require.True(t, ok)
	assert.GreaterOrEqual(t, int(dt.Scale), 2)
}

func TestArrowWriterTimeColumn(t *testing.T) {
	columns := Columns{{Name: "t", Type: TimeType}}
	for _, newWriter := range []func(io.Writer, Columns, ...ipc.Option) (*ArrowWriter, error){NewArrowWriter, NewArrowStreamWriter} {
		var buf bytes.Buffer
		aw, err := newWriter(&buf, columns)
		require.NoError(t, err)
		require.NoError(t, aw.WriteRow([]any{"08:30:00"}))
		require.NoError(t, aw.WriteRow([]any{nil}))
		require.NoError(t, aw.Close())
	}
}

// TestArrowIPCFormats checks that a .arrow file reads back in both IPC
// formats: the file format (ARROW1) that sling writes, and the stream format
// of older lane output.
func TestArrowIPCFormats(t *testing.T) {
	columns := Columns{
		{Name: "id", Type: BigIntType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
	}
	rows := [][]any{{int64(1), "a"}, {int64(2), nil}, {int64(3), "c"}}

	writers := map[string]func(w io.Writer, cols Columns) (*ArrowWriter, error){
		"file":   func(w io.Writer, cols Columns) (*ArrowWriter, error) { return NewArrowWriter(w, cols) },
		"stream": func(w io.Writer, cols Columns) (*ArrowWriter, error) { return NewArrowStreamWriter(w, cols) },
	}

	for name, newWriter := range writers {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "data.arrow")
			file, err := os.Create(path)
			require.NoError(t, err)
			aw, err := newWriter(file, columns.Clone())
			require.NoError(t, err)
			for _, row := range rows {
				require.NoError(t, aw.WriteRow(row))
			}
			require.NoError(t, aw.Close())
			require.NoError(t, file.Close())

			file, err = os.Open(path)
			require.NoError(t, err)
			defer file.Close()

			isFile, err := IsArrowIPCFile(file)
			require.NoError(t, err)
			assert.Equal(t, name == "file", isFile)

			schema, err := ArrowIPCFileSchema(file)
			require.NoError(t, err)
			assert.Equal(t, []string{"id", "name"}, []string{schema.Field(0).Name, schema.Field(1).Name})
			pos, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			assert.Zero(t, pos, "the schema read rewinds the file")

			ds := NewDatastreamContext(context.Background(), nil)
			require.NoError(t, ds.ConsumeArrowReaderSeeker(file))
			data, err := ds.Collect(0)
			require.NoError(t, err)
			assert.Equal(t, rows, data.Rows)
		})
	}
}

// TestArrowRecordChnlWritesIPCFile checks that the lane file writer writes
// the IPC file format, as the row path does.
func TestArrowRecordChnlWritesIPCFile(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { alloc.AssertSize(t, 0) })

	schema := dsArrowTestSchema()
	path := cdcArrowTestFile(t, alloc, schema, [][][]any{{{int64(1), "a"}, {int64(2), nil}}})
	file, err := os.Open(path)
	require.NoError(t, err)

	ds := NewDatastreamContext(context.Background(), ArrowSchemaToColumns(schema))
	require.NoError(t, ds.ConsumeArrowRecords(file, &openFakeLane{alloc: alloc}))

	var out []byte
	for br := range ds.NewArrowRecordChnl(StreamConfig{}) {
		data, err := io.ReadAll(br.Reader)
		require.NoError(t, err)
		out = append(out, data...)
	}
	ds.Close()

	require.Greater(t, len(out), 6)
	assert.Equal(t, "ARROW1", string(out[:6]))

	reader, err := ipc.NewFileReader(bytes.NewReader(out))
	require.NoError(t, err)
	defer reader.Close()
	assert.Equal(t, 1, reader.NumRecords())
}

// a decimal precision that is not from the source DDL gets the max, so that
// later rows fit (SLING-135)
func TestColumnsFileDecimals(t *testing.T) {
	cols := Columns{
		{Name: "unsourced", Type: DecimalType, DbPrecision: 23, DbScale: 15},
		{Name: "sourced", Type: DecimalType, DbPrecision: 25, DbScale: 8, Sourced: true},
		{Name: "small", Type: DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
		{Name: "wide", Type: DecimalType, DbPrecision: 50, DbScale: 10, Sourced: true},
		{Name: "s", Type: StringType},
	}
	out := cols.FileDecimals()
	assert.Equal(t, [2]int{38, 15}, [2]int{out[0].DbPrecision, out[0].DbScale})
	assert.Equal(t, [2]int{25, 8}, [2]int{out[1].DbPrecision, out[1].DbScale})
	assert.Equal(t, [2]int{10, 2}, [2]int{out[2].DbPrecision, out[2].DbScale})
	assert.Equal(t, [2]int{50, 10}, [2]int{out[3].DbPrecision, out[3].DbScale})
	assert.Equal(t, 23, cols[0].DbPrecision, "the input columns do not change")

	asString, wide := out.DecimalsAsString(38)
	assert.Equal(t, []string{"wide (decimal(50,10))"}, wide)
	assert.Equal(t, StringType, asString[3].Type)
	assert.Equal(t, DecimalType, asString[0].Type)
	assert.Equal(t, DecimalType, out[3].Type, "the input columns do not change")
}

func TestArrowDecimalIntDigits(t *testing.T) {
	cases := map[string]int{
		"0":                       0,
		"0.5":                     0,
		"-0.000001":               0,
		"123.45":                  3,
		"-99999999999999999999.5": 20,
		"+007.1":                  1,
		"$1,234.5":                4,
		"1e21":                    22,
		"-1.5E3":                  4,
		"NaN":                     0,
		"Infinity":                0,
	}
	for val, want := range cases {
		assert.Equal(t, want, decimalIntDigits(val), val)
	}
}

// a value with more integer digits than the Arrow type holds is an error,
// not a value that is not valid (SLING-135)
func TestArrowWriterDecimalOverflow(t *testing.T) {
	// precision from the source: the value does not fit
	columns := Columns{{Name: "d", Type: DecimalType, DbPrecision: 25, DbScale: 8, Sourced: true}}
	aw, err := NewArrowWriter(&bytes.Buffer{}, columns)
	require.NoError(t, err)
	require.NoError(t, aw.WriteRow([]any{"99999999999999999.12345678"}))
	err = aw.WriteRow([]any{"999999999999999999.5"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not fit decimal(25,8)")

	// precision from the first rows: the max precision holds the value
	columns = Columns{{Name: "d", Type: DecimalType, DbPrecision: 23, DbScale: 15}}
	var buf bytes.Buffer
	aw, err = NewArrowWriter(&buf, columns)
	require.NoError(t, err)
	require.NoError(t, aw.WriteRow([]any{"-99999999999999999999.5"}))
	require.NoError(t, aw.Close())

	reader, err := ipc.NewFileReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer reader.Close()
	rec, err := reader.Record(0)
	require.NoError(t, err)
	assert.Equal(t, &arrow.Decimal128Type{Precision: 38, Scale: 15}, rec.Schema().Field(0).Type)
	assert.Equal(t, "-99999999999999999999.500000000000000", GetValueFromArrowArray(rec.Column(0), 0))
}

// ratDecimal128 is the big.Rat reference for parseDecimal128.
func ratDecimal128(s string, scale int32) (decimal128.Num, bool) {
	rat, ok := new(big.Rat).SetString(s)
	if !ok {
		return decimal128.Num{}, false
	}
	pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	return decimal128.FromBigInt(new(big.Int).Quo(new(big.Int).Mul(rat.Num(), pow), rat.Denom())), true
}

func TestArrowParseDecimal128(t *testing.T) {
	rnd := rand.New(rand.NewSource(131))
	digits := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('0' + rnd.Intn(10))
		}
		return string(b)
	}

	var vals []string
	scales := map[string]int32{}
	add := func(s string, scale int32) { vals = append(vals, s); scales[s] = scale }
	for p := 1; p <= 38; p++ {
		for n := 0; n < 200; n++ {
			s := int32(rnd.Intn(p + 1))
			v := "0" // the int part is 0 when p == s
			if p > int(s) {
				v = digits(1 + rnd.Intn(p-int(s)))
			}
			if f := rnd.Intn(int(s) + 1); f > 0 {
				v += "." + digits(f)
			}
			if rnd.Intn(2) == 0 {
				v = "-" + v
			}
			add(v, s)
		}
		s := int32(p / 2)
		max := strings.Repeat("9", p-int(s))
		if s > 0 {
			max += "." + strings.Repeat("9", int(s))
		}
		add(max, s)
		add("-"+max, s)
	}

	for _, v := range vals {
		got, ok := parseDecimal128(v, scales[v])
		want, _ := ratDecimal128(v, scales[v])
		if assert.True(t, ok, v) {
			assert.Equal(t, want, got, "%s scale %d", v, scales[v])
		}
	}

	// the edge cases: fast path or fallback, the result is the same as big.Rat
	cases := []struct {
		val  string
		fast bool
	}{
		{"0", true}, {"-0", true}, {"-0.00000000", true}, {"+12.5", true},
		{"0.00000001", true}, {"-0.00000001", true}, {"00012.50000000", true},
		{"99999999999999999.99999999", true},
		{"0.000000019", false}, // more fraction digits than the scale
		{"1e-5", false}, {"NaN", false}, {"Infinity", false}, {"-Infinity", false},
		{".5", false}, {"5.", false}, {"1,234.5", false}, {"$12", false}, {"", false}, {"-", false},
		{"1.2.3", false}, {"1" + strings.Repeat("0", 30), false}, // 31 + 8 digits > 38
	}
	mem := memory.NewGoAllocator()
	col := &Column{Name: "d", Type: DecimalType}
	for _, c := range cases {
		got, ok := parseDecimal128(c.val, 8)
		assert.Equal(t, c.fast, ok, c.val)
		want, wantOk := ratDecimal128(strings.ReplaceAll(strings.ReplaceAll(c.val, ",", ""), "$", ""), 8)
		if ok {
			assert.Equal(t, want, got, c.val)
		}

		b := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 38, Scale: 8})
		AppendToBuilder(b, col, c.val)
		arr := b.NewDecimal128Array()
		assert.Equal(t, !wantOk, arr.IsNull(0), c.val)
		if wantOk {
			assert.Equal(t, want, arr.Value(0), c.val)
		}
		arr.Release()
		b.Release()
	}
}

func BenchmarkArrowAppendToBuilderDecimal(b *testing.B) {
	rnd := rand.New(rand.NewSource(131))
	vals := make([]string, 1024)
	for i := range vals {
		vals[i] = strconv.FormatFloat((rnd.Float64()-0.5)*1e9, 'f', 8, 64)
	}
	col := &Column{Name: "d", Type: DecimalType}
	mem := memory.NewGoAllocator()
	for _, name := range []string{"fast", "rat"} {
		b.Run(name, func(b *testing.B) {
			builder := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 25, Scale: 8})
			defer builder.Release()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v := vals[i%len(vals)]
				if name == "rat" {
					num, _ := ratDecimal128(v, 8)
					builder.Append(num)
				} else {
					AppendToBuilder(builder, col, v)
				}
			}
		})
	}
}

func TestArrowDecimalFit(t *testing.T) {
	cases := []struct {
		name  string
		vals  []any
		typ   string
		fails bool
	}{
		{"empty", nil, "decimal(38, 0)", false},
		{"nulls and text", []any{nil, "NaN", "abc"}, "decimal(38, 0)", false},
		{"fraction", []any{"1.12345678901234567890", "-2.25"}, "decimal(38, 20)", false},
		{"int and fraction", []any{"12345678901234567890123.5", "0.000001"}, "decimal(38, 6)", false},
		{"exponent", []any{1e-7, "1.5E+3", 42}, "decimal(38, 7)", false},
		{"money", []any{"$1,234.50"}, "decimal(38, 2)", false},
		{"wide", []any{"123456789012345678901234567890.123456789012"}, "decimal256(76, 12)", false},
		{"too wide", []any{strings.Repeat("9", 70) + "." + strings.Repeat("1", 10)}, "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var fit DecimalFit
			for _, v := range c.vals {
				fit.Add(v)
			}
			typ, err := fit.ArrowType()
			if c.fails {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, c.typ, typ.String())
		})
	}
}
