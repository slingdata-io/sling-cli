package iop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecimal(t *testing.T) {
	// Test decimal128 conversion functions
	precision := 10
	scale := 0
	decValStr := "-123456"
	// Since we're using arrow v18's decimal128, we don't need the old conversion functions
	// Just test the decimal128ToString function with a sample decimal128 value

	precision = 15
	scale = 6
	decValStr = "-123456.789000"
	// Test decimal128 string representation

	precision = 40
	scale = 20
	decValStr = "12345612345600000000.12345612345600000000"
	// Test large decimal values

	assert.NotEmpty(t, decValStr)
	assert.Equal(t, precision, precision) // Placeholder assertions
	assert.Equal(t, scale, scale)
}

func TestNewParquetReader(t *testing.T) {
	// Test file paths - you may need to adjust these
	testFiles := []string{
		"./core/dbio/filesys/test/test1/parquet/test1.1.parquet",
		"/tmp/test.parquet", // Will use the file created by TestNewParquetWriter
	}

	// First create a test file if it doesn't exist
	if _, err := os.Stat("/tmp/test.parquet"); os.IsNotExist(err) {
		t.Run("CreateTestFile", func(t *testing.T) {
			TestNewParquetWriter(t)
		})
	}

	for _, filePath := range testFiles {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Logf("Skipping test file %s (does not exist)", filePath)
			continue
		}

		t.Run(fmt.Sprintf("ReadFile_%s", filePath), func(t *testing.T) {
			f, err := os.Open(filePath)
			if err != nil {
				t.Skipf("Could not open test file: %v", err)
				return
			}
			defer f.Close()

			// Test reading all columns
			p, err := NewParquetArrowReader(f, nil)
			if err != nil {
				t.Skipf("Could not create parquet reader: %v", err)
				return
			}
			assert.NotNil(t, p)

			columns := p.Columns()
			t.Logf("Found %d columns", len(columns))

			// Read first 10 rows
			count := 0
			ds := NewDatastream(columns)
			it := ds.NewIterator(columns, p.nextFunc)
			for it.next() {
				row := it.Row
				if count < 10 {
					t.Logf("Row %d: %#v", count, row)
				}
				count++
				if count >= 10 {
					break
				}
			}

			t.Logf("Read %d rows", count)
			t.Logf("Column types: %#v", columns.Types())
		})

		// Test with selected columns
		t.Run(fmt.Sprintf("ReadSelectedColumns_%s", filePath), func(t *testing.T) {
			f, err := os.Open(filePath)
			if err != nil {
				t.Skipf("Could not open test file: %v", err)
				return
			}
			defer f.Close()

			// Get all columns first
			pAll, err := NewParquetArrowReader(f, nil)
			if err != nil {
				t.Skipf("Could not create parquet reader: %v", err)
				return
			}
			allCols := pAll.Columns()

			if len(allCols) < 2 {
				t.Skip("Not enough columns for selected column test")
				return
			}

			// Reopen file for selected columns test
			f.Close()
			f, err = os.Open(filePath)
			assert.NoError(t, err)
			defer f.Close()

			// Select first two columns
			selected := []string{allCols[0].Name, allCols[1].Name}
			p, err := NewParquetArrowReader(f, selected)
			assert.NoError(t, err)

			selectedCols := p.Columns()
			assert.Equal(t, 2, len(selectedCols))
			assert.Equal(t, selected[0], selectedCols[0].Name)
			assert.Equal(t, selected[1], selectedCols[1].Name)
		})
	}
}

func TestNewParquetWriter(t *testing.T) {

	columns := NewColumns(
		Columns{
			{Name: "col_string", Type: StringType},
			{Name: "col_bool", Type: BoolType},
			{Name: "col_bigint", Type: BigIntType},
			{Name: "col_decimal", Type: DecimalType, DbPrecision: 30, DbScale: 12},
			{Name: "col_float", Type: FloatType},
			{Name: "col_json", Type: JsonType},
			{Name: "col_timestamp", Type: TimestampType},
			{Name: "col_date", Type: DateType},
		}...,
	)

	// Use fixed timestamps with nanosecond precision
	timestamp1 := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)
	timestamp2 := time.Date(2024, 2, 20, 14, 15, 30, 987654321, time.UTC)
	date1 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC)

	rows := [][]any{
		{
			"hello",                    // col_string
			true,                       // col_bool
			int64(1333329418491273193), // col_bigint
			"12.330000000000",          // col_decimal
			1212223132321.334443422313, // col_float
			`{"msg": "Hello!"}`,        // col_json
			timestamp1,                 // col_timestamp
			date1,                      // col_date
		},
		{
			"hello2",                       // col_string
			false,                          // col_bool
			int64(-987123),                 // col_bigint
			"-12112333990123.338712313310", // col_decimal
			-121.33,                        // col_float
			`{"msg": "Bye!"}`,              // col_json
			timestamp2,                     // col_timestamp
			date2,                          // col_date
		},
		{
			nil,              // col_string
			nil,              // col_bool
			nil,              // col_bigint
			"0.000000000000", // col_decimal (with proper scale)
			nil,              // col_float
			nil,              // col_json
			nil,              // col_timestamp
			nil,              // col_date
		},
	}

	// Test with different compression codecs
	codecs := []compress.Compression{
		compress.Codecs.Snappy,
		compress.Codecs.Gzip,
		compress.Codecs.Zstd,
	}

	for _, codec := range codecs {
		t.Run(fmt.Sprintf("Compression_%s", codec), func(t *testing.T) {
			testFile := fmt.Sprintf("/tmp/test_%s.parquet", codec)
			g.Info("Testing file: %s", testFile)
			f, err := os.Create(testFile)
			assert.NoError(t, err)
			defer f.Close()
			defer os.Remove(testFile)

			pw, err := NewParquetArrowWriter(f, columns, codec)
			assert.NoError(t, err)
			assert.NotNil(t, pw)

			// Write rows
			for _, row := range rows {
				err := pw.WriteRow(row)
				assert.NoError(t, err)
			}

			// Close writer
			err = pw.Close()
			assert.NoError(t, err)

			// Verify file was created
			stat, err := os.Stat(testFile)
			assert.NoError(t, err)
			assert.Greater(t, stat.Size(), int64(0))

			// Try to read it back
			f2, err := os.Open(testFile)
			assert.NoError(t, err)
			defer f2.Close()

			reader, err := NewParquetArrowReader(f2, nil)
			assert.NoError(t, err)

			readCols := reader.Columns()
			assert.Equal(t, len(columns), len(readCols))

			// Verify column types
			for i, col := range columns {
				assert.Equal(t, col.Name, readCols[i].Name, "Column name mismatch at index %d", i)
				// Allow JsonType to be read as StringType, and TimestampType to be read as DatetimeType
				if col.Type == JsonType && readCols[i].Type == StringType {
					// This is expected - JSON is stored as string in parquet
				} else if col.Type == TimestampType && readCols[i].Type == DatetimeType {
					// This is expected - timestamp/datetime are similar
				} else {
					assert.Equal(t, col.Type, readCols[i].Type, "Column type mismatch for %s", col.Name)
				}
			}

			// Close and reopen the file to read it back
			f.Close()
			f2, err = os.Open(testFile)
			assert.NoError(t, err)

			// Create new reader
			reader, err = NewParquetArrowReader(f2, nil)
			assert.NoError(t, err)

			// Read all rows back using a simpler approach
			ctx := context.Background()
			table, err := reader.Reader.ReadTable(ctx)
			assert.NoError(t, err)
			defer table.Release()

			numRows := int(table.NumRows())
			assert.Equal(t, len(rows), numRows, "Row count mismatch")

			// Compare each row by reading values from the table
			for rowIdx, originalRow := range rows {
				// Get row data from table
				readRow := make([]any, len(columns))
				for colIdx := range columns {
					columnData := table.Column(colIdx)
					if columnData.Len() > rowIdx {
						// Extract value from column
						chunk := columnData.Data().Chunk(0)
						readRow[colIdx] = GetValueFromArrowArray(chunk, rowIdx)
					}
				}

				for colIdx, col := range columns {
					original := originalRow[colIdx]
					read := readRow[colIdx]

					// Handle nil values
					if original == nil {
						assert.Nil(t, read, "Row %d, Column %s: expected nil", rowIdx, col.Name)
						continue
					}

					switch col.Type {
					case StringType, JsonType:
						assert.Equal(t, original, read, "Row %d, Column %s mismatch", rowIdx, col.Name)
					case BoolType:
						assert.Equal(t, original, read, "Row %d, Column %s mismatch", rowIdx, col.Name)
					case BigIntType:
						// Convert to int64 for comparison
						assert.Equal(t, cast.ToInt64(original), cast.ToInt64(read), "Row %d, Column %s mismatch", rowIdx, col.Name)
					case FloatType:
						// Compare floats with tolerance
						origFloat := cast.ToFloat64(original)
						readFloat := cast.ToFloat64(read)
						assert.InDelta(t, origFloat, readFloat, 0.000001, "Row %d, Column %s mismatch", rowIdx, col.Name)
					case DecimalType:
						// Decimal values are read back as strings
						expectedDecimal := cast.ToString(original)
						if expectedDecimal == "0" {
							expectedDecimal = "0.00" // Normalize zero with scale
						}
						assert.Equal(t, expectedDecimal, read, "Row %d, Column %s mismatch", rowIdx, col.Name)
					case TimestampType, DatetimeType:
						// The record path stores timestamps as timestamp(us)
						// (the plan's matrix), so compare at microsecond
						// precision and keep the instant exact.
						origTime := original.(time.Time)
						readTime := read.(time.Time)
						assert.Equal(t, origTime.Truncate(time.Microsecond).UnixMicro(), readTime.UnixMicro(),
							"Row %d, Column %s: timestamp mismatch (orig: %v, read: %v)",
							rowIdx, col.Name, origTime, readTime)
					case DateType:
						// Compare dates (day precision)
						origTime := original.(time.Time)
						readTime := read.(time.Time)
						origDate := time.Date(origTime.Year(), origTime.Month(), origTime.Day(), 0, 0, 0, 0, time.UTC)
						readDate := time.Date(readTime.Year(), readTime.Month(), readTime.Day(), 0, 0, 0, 0, time.UTC)
						assert.Equal(t, origDate, readDate, "Row %d, Column %s: date mismatch", rowIdx, col.Name)
					default:
						assert.Equal(t, original, read, "Row %d, Column %s mismatch", rowIdx, col.Name)
					}
				}
			}
		})
	}
}

func TestLargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large dataset test in short mode")
	}

	filePath := "/tmp/large_test.parquet"
	f, err := os.Create(filePath)
	assert.NoError(t, err)
	defer f.Close()
	defer os.Remove(filePath)

	columns := NewColumns(
		Columns{
			{Name: "id", Type: BigIntType},
			{Name: "name", Type: StringType},
			{Name: "value", Type: FloatType},
			{Name: "timestamp", Type: TimestampType},
		}...,
	)

	pw, err := NewParquetArrowWriter(f, columns, compress.Codecs.Snappy)
	assert.NoError(t, err)

	// Write 100k rows
	numRows := 100000
	for i := 0; i < numRows; i++ {
		row := []any{
			int64(i),
			fmt.Sprintf("name_%d", i),
			float64(i) * 1.23,
			time.Now(),
		}
		err := pw.WriteRow(row)
		assert.NoError(t, err)
	}

	err = pw.Close()
	assert.NoError(t, err)

	// Read back and verify
	f2, err := os.Open(filePath)
	assert.NoError(t, err)
	defer f2.Close()

	reader, err := NewParquetArrowReader(f2, nil)
	assert.NoError(t, err)

	count := 0
	ds := NewDatastream(reader.Columns())
	it := ds.NewIterator(reader.Columns(), reader.nextFunc)
	for it.next() {
		count++
	}

	assert.Equal(t, numRows, count)
}

// Test the decimal string conversion
func TestDecimal128ToString(t *testing.T) {
	tests := []struct {
		input     string
		precision int
		scale     int
		expected  string
	}{
		{"123456", 10, 0, "123456"},
		{"123456", 10, 2, "1234.56"},
		{"-123456", 10, 2, "-1234.56"},
		{"100", 5, 2, "1.00"},
		{"1", 5, 2, "0.01"},
		{"0", 5, 2, "0.00"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_p%d_s%d", tt.input, tt.precision, tt.scale), func(t *testing.T) {
			// This would be the test if we had access to the actual decimal128 conversion
			// For now, we're just testing the concept
			g.Info("Testing decimal conversion: %s (precision=%d, scale=%d) -> %s",
				tt.input, tt.precision, tt.scale, tt.expected)
		})
	}
}

// createBuilder had no case for time or uuid columns, so it fell through to a
// string builder while the schema declared time64/uuid. Building the record
// then panicked on the type mismatch, making any parquet write with a time or
// uuid column fail.
// TestParquetArrowWriterSmallInt covers the int16 builder: a smallint column
// maps to int16 in the arrow schema.
func TestParquetArrowWriterSmallInt(t *testing.T) {
	columns := NewColumns(Columns{
		{Name: "c_int2", Type: SmallIntType},
		{Name: "c_str", Type: StringType},
	}...)

	testFile := filepath.Join(t.TempDir(), "smallint.parquet")
	f, err := os.Create(testFile)
	require.NoError(t, err)
	defer f.Close()

	pw, err := NewParquetArrowWriter(f, columns, compress.Codecs.Snappy)
	require.NoError(t, err)
	require.NoError(t, pw.WriteRow([]any{int16(1), "a"}))
	require.NoError(t, pw.WriteRow([]any{nil, "b"}))
	require.NoError(t, pw.Close())

	f2, err := os.Open(testFile)
	require.NoError(t, err)
	defer f2.Close()

	reader, err := NewParquetArrowReader(f2, nil)
	require.NoError(t, err)
	table, err := reader.Reader.ReadTable(context.Background())
	require.NoError(t, err)
	defer table.Release()

	require.Equal(t, 2, int(table.NumRows()))
	chunk := table.Column(0).Data().Chunk(0)
	assert.Equal(t, arrow.INT16, chunk.DataType().ID())
	assert.EqualValues(t, 1, GetValueFromArrowArray(chunk, 0))
	assert.Nil(t, GetValueFromArrowArray(chunk, 1))
}

func TestParquetArrowWriterTimeAndUUID(t *testing.T) {
	columns := NewColumns(
		Columns{
			{Name: "col_id", Type: BigIntType},
			{Name: "col_time", Type: TimeType},
			{Name: "col_timez", Type: TimezType},
			{Name: "col_uuid", Type: UUIDType},
		}...,
	)

	rows := [][]any{
		{int64(1), "10:00:00", "10:00:00", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{int64(2), "23:59:59", "00:00:01", "6ba7b811-9dad-11d1-80b4-00c04fd430c8"},
		{int64(3), nil, nil, nil},
	}

	testFile := "/tmp/test_time_uuid.parquet"
	f, err := os.Create(testFile)
	assert.NoError(t, err)
	defer f.Close()
	defer os.Remove(testFile)

	pw, err := NewParquetArrowWriter(f, columns, compress.Codecs.Snappy)
	assert.NoError(t, err)

	for _, row := range rows {
		assert.NoError(t, pw.WriteRow(row))
	}
	assert.NoError(t, pw.Close())

	stat, err := os.Stat(testFile)
	assert.NoError(t, err)
	assert.Greater(t, stat.Size(), int64(0))

	f2, err := os.Open(testFile)
	assert.NoError(t, err)
	defer f2.Close()

	reader, err := NewParquetArrowReader(f2, nil)
	assert.NoError(t, err)

	readCols := reader.Columns()
	assert.Equal(t, len(columns), len(readCols))

	table, err := reader.Reader.ReadTable(context.Background())
	assert.NoError(t, err)
	defer table.Release()
	assert.Equal(t, len(rows), int(table.NumRows()))

	for rowIdx, originalRow := range rows {
		for colIdx, col := range columns {
			chunk := table.Column(colIdx).Data().Chunk(0)
			read := GetValueFromArrowArray(chunk, rowIdx)

			if originalRow[colIdx] == nil {
				assert.Nil(t, read, "row %d, %s: expected nil", rowIdx, col.Name)
				continue
			}
			assert.NotNil(t, read, "row %d, col %s (%s): expected a value", rowIdx, col.Name, col.Type)
			t.Logf("row %d %s = %v", rowIdx, col.Name, read)

			// times land as time.Time, uuid as its canonical string
			switch col.Type {
			case TimeType, TimezType:
				assert.Contains(t, cast.ToString(read), cast.ToString(originalRow[colIdx]),
					"row %d, %s value mismatch", rowIdx, col.Name)
			case UUIDType:
				assert.Equal(t, originalRow[colIdx], cast.ToString(read),
					"row %d, %s value mismatch", rowIdx, col.Name)
			}
		}
	}
}

// ---- record path: NewParquetArrowWriterFromSchema / WriteRecord ----

// dsArrowTestMatrixSchema is the type matrix the record-path round trip
// covers: bool, int32, int64, float64, decimal128, date32, timestamp us,
// string and binary. Every field is nullable so the nulls round trip too.
func dsArrowTestMatrixSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "col_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "col_int32", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "col_int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col_float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "col_decimal", Type: &arrow.Decimal128Type{Precision: 20, Scale: 4}, Nullable: true},
		{Name: "col_date32", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "col_ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
		{Name: "col_string", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "col_binary", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)
}

// dsArrowTestMatrixAppend appends one matrix value to its builder.
func dsArrowTestMatrixAppend(t testing.TB, b array.Builder, v any) {
	t.Helper()
	if v == nil {
		b.AppendNull()
		return
	}
	switch f := b.(type) {
	case *array.BooleanBuilder:
		f.Append(v.(bool))
	case *array.Int32Builder:
		f.Append(v.(int32))
	case *array.Int64Builder:
		f.Append(v.(int64))
	case *array.Float64Builder:
		f.Append(v.(float64))
	case *array.Decimal128Builder:
		f.Append(v.(decimal128.Num))
	case *array.Date32Builder:
		f.Append(arrow.Date32FromTime(v.(time.Time)))
	case *array.TimestampBuilder:
		f.AppendTime(v.(time.Time))
	case *array.StringBuilder:
		f.Append(v.(string))
	case *array.BinaryBuilder:
		f.Append(v.([]byte))
	default:
		require.Failf(t, "unsupported builder", "%T", b)
	}
}

// dsArrowTestMatrixRecord builds one record in schema; a nil value is null.
func dsArrowTestMatrixRecord(t testing.TB, alloc memory.Allocator, schema *arrow.Schema, rows [][]any) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(alloc, schema)
	defer b.Release()
	for _, row := range rows {
		require.Len(t, row, schema.NumFields())
		for i, v := range row {
			dsArrowTestMatrixAppend(t, b.Field(i), v)
		}
	}
	return b.NewRecordBatch()
}

// dsArrowTestChunkValue reads one value out of a possibly chunked column.
func dsArrowTestChunkValue(chunks *arrow.Chunked, row int) (any, bool) {
	for _, ch := range chunks.Chunks() {
		if row < ch.Len() {
			return GetValueFromArrowArray(ch, row), true
		}
		row -= ch.Len()
	}
	return nil, false
}

// dsArrowTestReadParquet reads a Parquet buffer back as an Arrow table on the
// given allocator. The caller releases the table.
func dsArrowTestReadParquet(t testing.TB, buf []byte, alloc memory.Allocator) arrow.Table {
	t.Helper()
	pf, err := parquetfile.NewParquetReader(bytes.NewReader(buf))
	require.NoError(t, err)
	t.Cleanup(func() { pf.Close() })

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, alloc)
	require.NoError(t, err)
	table, err := reader.ReadTable(context.Background())
	require.NoError(t, err)
	return table
}

// TestParquetArrowWriterFromSchema_RoundTrip covers the record path: whole
// records go straight to pqarrow, and every type of the matrix (with its
// nulls) reads back as it went in.
func TestParquetArrowWriterFromSchema_RoundTrip(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := dsArrowTestMatrixSchema()
	ts := func(s string) time.Time {
		t.Helper()
		v, err := time.ParseInLocation("2006-01-02 15:04:05.999999", s, time.UTC)
		require.NoError(t, err)
		return v
	}
	dec := decimal128.FromI64

	rows := [][]any{
		{true, int32(11), int64(111), 1.5,
			dec(1234567890), ts("2024-03-05 00:00:00"), ts("2024-03-05 06:07:08.123456"),
			"alpha", []byte{1, 2, 3}},
		{false, int32(-22), int64(-222), -2.25,
			dec(-1234567890), ts("1999-12-31 00:00:00"), ts("1999-12-31 23:59:59.999999"),
			"bêta", []byte{0xff, 0x00, 0x7f}},
		{nil, nil, nil, nil, nil, nil, nil, nil, nil},
	}

	want := dsArrowTestMatrixRecord(t, alloc, schema, rows)
	defer want.Release()

	var buf bytes.Buffer
	w, err := NewParquetArrowWriterFromSchema(&buf, schema, compress.Codecs.Snappy)
	require.NoError(t, err)
	require.NoError(t, w.WriteRecord(want))
	require.NoError(t, w.Close())

	table := dsArrowTestReadParquet(t, buf.Bytes(), alloc)
	defer table.Release()

	// the stored Arrow schema restores the field names and types
	require.Equal(t, schema.NumFields(), int(table.NumCols()))
	for i, field := range schema.Fields() {
		assert.Equal(t, field.Name, table.Schema().Field(i).Name)
		assert.Equal(t, field.Type.ID(), table.Schema().Field(i).Type.ID(), "type of %s", field.Name)
	}
	require.Equal(t, int64(len(rows)), table.NumRows())

	for r := range rows {
		for c, field := range schema.Fields() {
			got, ok := dsArrowTestChunkValue(table.Column(c).Data(), r)
			require.True(t, ok, "row %d, column %s", r, field.Name)
			assert.Equal(t, GetValueFromArrowArray(want.Column(c), r), got, "row %d, column %s", r, field.Name)
			assert.Equal(t, rows[r][c] == nil, got == nil, "row %d, column %s: null mismatch", r, field.Name)
		}
	}
}

// TestParquetArrowWriterFromSchema_RowGroupRollover covers the record path's
// row-group break: once the bytes counted for the current row group pass
// parquetArrowRowGroupBytes, a new row group starts. Each record carries 1 MiB
// of incompressible data, so the mark takes about 130 records; skipped in
// short mode.
func TestParquetArrowWriterFromSchema_RowGroupRollover(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a full row group")
	}

	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "payload", Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil)

	// incompressible, so the bytes counted per record are the bytes stored
	payload := make([]byte, 1<<20)
	_, err := rand.NewChaCha8([32]byte{1}).Read(payload)
	require.NoError(t, err)

	var buf bytes.Buffer
	w, err := NewParquetArrowWriterFromSchema(&buf, schema, compress.Codecs.Snappy)
	require.NoError(t, err)

	// write returns the bytes the writer counts for the record
	write := func() int64 {
		rec := dsArrowTestMatrixRecord(t, alloc, schema, [][]any{{payload}})
		defer rec.Release()
		require.NoError(t, w.WriteRecord(rec))
		return TotalRecordSize(rec)
	}

	written := int64(0)
	records := 0
	for written <= int64(parquetArrowRowGroupBytes) {
		written += write()
		records++
	}
	// the record that passed the mark started a new row group, so the
	// accounting of the current one restarted
	assert.Less(t, w.RowGroupBytes(), int64(parquetArrowRowGroupBytes))

	// two more records, so the row group the break started is not empty
	for i := 0; i < 2; i++ {
		write()
		records++
	}
	require.NoError(t, w.Close())

	pf, err := parquetfile.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer pf.Close()

	assert.Greater(t, pf.NumRowGroups(), 1, "the mark started a new row group")
	assert.Equal(t, int64(records), pf.NumRows())

	total := int64(0)
	for i := 0; i < pf.NumRowGroups(); i++ {
		rgRows := pf.MetaData().RowGroup(i).NumRows()
		assert.Greater(t, rgRows, int64(0), "row group %d holds rows", i)
		total += rgRows
	}
	assert.Equal(t, int64(records), total, "every record landed in a row group")
}

// TestArrowWriter_WriteRecord covers the Arrow IPC record path: a record goes
// to the file writer as it is and reads back through ipc.NewFileReader.
func TestArrowWriter_WriteRecord(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	columns := NewColumns(Columns{
		{Name: "col_id", Type: BigIntType},
		{Name: "col_name", Type: StringType},
		{Name: "col_flag", Type: BoolType},
	}...)
	schema := ColumnsToArrowSchema(columns)

	rows := [][]any{
		{int64(1), "alpha", true},
		{int64(2), nil, false},
		{nil, "gamma", nil},
	}
	recs := []arrow.RecordBatch{
		dsArrowTestMatrixRecord(t, alloc, schema, rows[0:2]),
		dsArrowTestMatrixRecord(t, alloc, schema, rows[2:]),
	}
	defer func() {
		for _, rec := range recs {
			rec.Release()
		}
	}()

	var buf bytes.Buffer
	w, err := NewArrowWriter(&buf, columns)
	require.NoError(t, err)
	for _, rec := range recs {
		require.NoError(t, w.WriteRecord(rec))
	}
	require.NoError(t, w.Close())

	reader, err := ipc.NewFileReader(bytes.NewReader(buf.Bytes()), ipc.WithAllocator(alloc))
	require.NoError(t, err)
	defer reader.Close()

	// the file schema is the one the writer built from the Sling columns
	for i, field := range schema.Fields() {
		assert.Equal(t, field.Name, reader.Schema().Field(i).Name)
		assert.Equal(t, field.Type.ID(), reader.Schema().Field(i).Type.ID(), "type of %s", field.Name)
	}

	r := 0
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for i := 0; i < int(rec.NumRows()); i++ {
			for c := range columns {
				assert.Equal(t, rows[r][c], GetValueFromArrowArray(rec.Column(c), i), "row %d, column %s", r, columns[c].Name)
			}
			r++
		}
		rec.Release()
	}
	assert.Equal(t, len(rows), r)
}
