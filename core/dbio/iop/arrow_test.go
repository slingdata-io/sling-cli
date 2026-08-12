package iop

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/shopspring/decimal"
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
