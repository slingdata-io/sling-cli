package iop

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func bParseString(sp *StreamProcessor, val string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		sp.ParseString(val)
	}
}

// go test -run BenchmarkParseString -bench=.
// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkParseString'
// assume worst case 1000ns * 100 columns * 100000 rows = 0.01sec
func BenchmarkParseString1String(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "hello my name is", b)
}
func BenchmarkParseString2Date1(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "01-JAN-02 15:04:05", b)
}
func BenchmarkParseString3Date2(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "2006-01-02 15:04:05", b)
}
func BenchmarkParseString4Date3(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "2006-01-02", b)
}
func BenchmarkParseString5Int(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "239189210510", b)
}
func BenchmarkParseString6Float(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "239189210510.25234", b)
}
func BenchmarkParseString7Blank(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "", b)
}
func BenchmarkParseString8Bool(b *testing.B) {
	sp := NewStreamProcessor()
	bParseString(sp, "true", b)
}

func initProcessRow(name string) (columns []Column, row []any) {

	if name == "float" {
		row = []any{5535.21414}
	} else if name == "decimal" {
		row = []any{[]byte{0x31, 0x30, 0x33, 0x32, 0x2e, 0x34, 0x34, 0x32, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}}
		row = []any{"5535.214140000"}
	} else if name == "int" {
		row = []any{int(48714719874194)}
	} else if name == "int64" {
		row = []any{int64(48714719874194)}
	} else if name == "string" {
		row = []any{g.RandString(g.AlphaNumericRunes, 1000)}
	} else if name == "timestamp" || name == "timestampz" {
		row = []any{time.Now()}
		// row = []any{"17-OCT-20 07.01.59.000000 PM"}
	} else if name == "bool" {
		row = []any{false}
	} else if name == "blank" {
		row = []any{""}
	} else {
		row = []any{
			"fritz", "larco", 55, 563525, 5535.21414, true, time.Now(),
		}
	}

	data := NewDataset(nil)
	fields := make([]string, len(row))
	for i := range row {
		fields[i] = g.F("col%d", i)
	}
	data.SetFields(fields)
	data.Append(row)
	data.InferColumnTypes()
	columns = data.Columns
	return
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessRow'
func BenchmarkProcessRow1(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		sp.ProcessRow(row)
	}
}

func BenchmarkProcessRow2(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastVal(i, val, &columns[i])
		}
	}
}
func BenchmarkProcessRow2b(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}

func BenchmarkProcessRow3(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastType(val, columns[i].Type)
		}
	}
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessRows'
func BenchmarkProcessRows(b *testing.B) {
	columns, row := initProcessRow("")
	ds := NewDatastream(columns)
	go func() {
		for range ds.Rows() {
		}
	}()
	for n := 0; n < b.N; n++ {
		ds.Push(row)
	}
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessVal'
func BenchmarkProcessValFloat(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("float")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValNumeric(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("decimal")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValInt(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("int")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValInt64(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("int64")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValString(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("string")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValBool(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("bool")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValTimestamp(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("timestamp")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}
func BenchmarkProcessValBlank(b *testing.B) {
	sp := NewStreamProcessor()
	columns, row := initProcessRow("blank")
	sp.ds = NewDatastream(columns)
	for n := 0; n < b.N; n++ {
		row = sp.CastRow(row, columns)
	}
}

func BenchmarkDecimalToString(b *testing.B) {
	val, _ := decimal.NewFromString("1234456.789")
	for n := 0; n < b.N; n++ {
		// val.String() // much slower
		val.NumDigits()
	}
}

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkCastToString'
func BenchmarkCastToStringTime(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("timestamp")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val)
		}
	}
}
func BenchmarkCastToStringFloat(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("float")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val)
		}
	}
}
func BenchmarkCastToStringNumeric(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("decimal")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val, "decimal")
		}
	}
}
func BenchmarkCastToStringInt(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val)
		}
	}
}
func BenchmarkCastToStringInt64(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int64")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val)
		}
	}
}
func BenchmarkCastToStringString(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("string")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToStringCSV(i, val)
		}
	}
}

func BenchmarkIsDate(b *testing.B) {
	t := time.Now()
	for n := 0; n < b.N; n++ {
		isDate(&t)
	}
}

func BenchmarkIsUTC(b *testing.B) {
	t := time.Now()
	for n := 0; n < b.N; n++ {
		isUTC(&t)
	}
}

func TestInterfVal(t *testing.T) {
	row := make([]any, 3)
	g.P(row[0])
	row[0] = float64(0)
	g.P(row[0])
	row[0] = nil
	g.P(row[0])
}

func TestParseDate(t *testing.T) {
	sp := NewStreamProcessor()
	val := "17-OCT-20 07.01.59.000000 PM"
	g.P(sp.ParseString(val))
	val = "17-OCT-20"
	g.P(sp.ParseString(val))
	val = `1/17/20`
	g.P(sp.ParseString(val))
	val = `0001-01-01 00:00:00.000`
	valT, err := sp.CastToTime(val)
	if assert.NoError(t, err) {
		g.P(valT)
		g.P(valT.IsZero())
		g.P(valT.Format(time.DateTime))
	}
	val = `0000-00-00 00:00:00.000`
	_, err = sp.CastToTime(val)
	assert.Error(t, err)
}

func TestParseDecimal(t *testing.T) {
	sp := NewStreamProcessor()
	val := "1.2"
	g.P(sp.ParseString(val))
	val = "1.2.3"
	g.P(sp.ParseString(val))
	iVal, err := cast.ToIntE("1.2")
	g.P(iVal)
	assert.Error(t, err)
}

func TestColumnTyping(t *testing.T) {
	maxStringLength := 1000

	type testCase struct {
		name         string
		column       Column
		columnTyping ColumnTyping

		expectedDecimalPrecision int
		expectedDecimalScale     int
		expectedStringLength     int
	}

	testCases := []testCase{
		// Decimal column typing tests
		{
			name:                     "decimal_sourced_precision_scale",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{}},
			expectedDecimalPrecision: 10,
			expectedDecimalScale:     2,
		},
		{
			name:                     "decimal_sourced_precision_scale_2",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{}},
			expectedDecimalPrecision: 10,
			expectedDecimalScale:     2,
		},
		{
			name:                     "decimal_min_precision_scale",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 5, DbScale: 1, Sourced: false},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{MinPrecision: g.Ptr(10), MinScale: g.Ptr(3)}},
			expectedDecimalPrecision: 24,
			expectedDecimalScale:     3,
		},
		{
			name:                     "decimal_max_precision_scale",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 50, DbScale: 15, Sourced: false},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{MaxPrecision: 20, MaxScale: 10}},
			expectedDecimalPrecision: 20,
			expectedDecimalScale:     10,
		},
		{
			name:                     "decimal_with_stats",
			column:                   Column{Name: "test", Type: DecimalType, Stats: ColumnStats{MaxLen: 8, MaxDecLen: 3}, Sourced: false},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{}},
			expectedDecimalPrecision: 24,
			expectedDecimalScale:     6,
		},
		{
			name:                     "decimal_zero_precision_scale",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 0, DbScale: 0, Sourced: false},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{}},
			expectedDecimalPrecision: 24,
			expectedDecimalScale:     6,
		},
		{
			name:                     "decimal_delta",
			column:                   Column{Name: "test", Type: DecimalType, DbPrecision: 0, DbScale: 19, Sourced: false},
			columnTyping:             ColumnTyping{Decimal: &DecimalColumnTyping{}},
			expectedDecimalPrecision: 38,
			expectedDecimalScale:     19,
		},

		// String column typing tests
		{
			name:                 "string_basic_length",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 50}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{}},
			expectedStringLength: 50,
		},
		{
			name:                 "string_length_factor",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 50}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{LengthFactor: 2}},
			expectedStringLength: 100,
		},
		{
			name:                 "string_length_factor_exceeds_max",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 600}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{LengthFactor: 2}},
			expectedStringLength: 1000, // should cap at maxStringLength
		},
		{
			name:                 "string_min_length",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 10}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{MinLength: 50}},
			expectedStringLength: 50,
		},
		{
			name:                 "string_max_length",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 200}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{MaxLength: 150}},
			expectedStringLength: 200, // original length since MaxLength doesn't override max
		},
		{
			name:                 "string_use_max",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 50}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{UseMax: true}},
			expectedStringLength: 1000, // should use maxStringLength
		},
		{
			name:                 "string_use_max_with_custom_max",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 50}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{UseMax: true, MaxLength: 2000}},
			expectedStringLength: 2000, // should use custom MaxLength
		},
		{
			name:                 "string_min_length_with_factor",
			column:               Column{Name: "test", Type: StringType, Stats: ColumnStats{MaxLen: 10}},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{LengthFactor: 2, MinLength: 50}},
			expectedStringLength: 50, // factor gives 20, but min is 50
		},

		// Sourced column precision tests
		{
			name:                 "string_sourced_precision",
			column:               Column{Name: "test", Type: StringType, DbPrecision: 100, Sourced: true},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{}},
			expectedStringLength: 100,
		},
		{
			name:                 "string_sourced_precision_with_factor",
			column:               Column{Name: "test", Type: StringType, DbPrecision: 50, Sourced: true},
			columnTyping:         ColumnTyping{String: &StringColumnTyping{LengthFactor: 2}},
			expectedStringLength: 100,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if sct := testCase.columnTyping.String; sct != nil {
				var length int
				if testCase.column.Sourced && testCase.column.DbPrecision > 0 {
					length = sct.Apply(testCase.column.DbPrecision, maxStringLength)
				} else {
					length = sct.Apply(testCase.column.Stats.MaxLen, maxStringLength)
				}
				assert.Equal(t, testCase.expectedStringLength, length)

			} else if dct := testCase.columnTyping.Decimal; dct != nil {
				precision, scale := dct.Apply(testCase.column)
				assert.Equal(t, testCase.expectedDecimalPrecision, precision)
				assert.Equal(t, testCase.expectedDecimalScale, scale)
			}
		})
	}

	// Keep the original hardcoded test for backward compatibility
	col := Column{Name: "test", Type: DecimalType, DbPrecision: 10, DbScale: 0, Sourced: true}
	ct := ColumnTyping{Decimal: &DecimalColumnTyping{}}
	precision, scale := ct.Decimal.Apply(col)
	assert.Equal(t, 10, precision)
	assert.Equal(t, 0, scale)
}

// Additional test for JSON column typing
func TestColumnTypingJSON(t *testing.T) {
	t.Run("json_as_text_false", func(t *testing.T) {
		col := Column{Name: "test", Type: JsonType}
		jct := JsonColumnTyping{AsText: false}
		jct.Apply(&col)
		assert.Equal(t, JsonType, col.Type)
	})

	t.Run("json_as_text_true", func(t *testing.T) {
		col := Column{Name: "test", Type: JsonType}
		jct := JsonColumnTyping{AsText: true}
		jct.Apply(&col)
		assert.Equal(t, TextType, col.Type)
	})
}

// Test for MaxDecimals method
func TestColumnTypingMaxDecimals(t *testing.T) {
	t.Run("nil_column_typing", func(t *testing.T) {
		var ct *ColumnTyping
		assert.Equal(t, -1, ct.MaxDecimals())
	})

	t.Run("nil_decimal_typing", func(t *testing.T) {
		ct := &ColumnTyping{}
		assert.Equal(t, -1, ct.MaxDecimals())
	})

	t.Run("max_scale_set", func(t *testing.T) {
		ct := &ColumnTyping{
			Decimal: &DecimalColumnTyping{MaxScale: 5},
		}
		assert.Equal(t, 5, ct.MaxDecimals())
	})

	t.Run("min_scale_set_no_max", func(t *testing.T) {
		ct := &ColumnTyping{
			Decimal: &DecimalColumnTyping{MinScale: g.Ptr(3)},
		}
		assert.Equal(t, 3, ct.MaxDecimals())
	})

	t.Run("both_scales_set", func(t *testing.T) {
		ct := &ColumnTyping{
			Decimal: &DecimalColumnTyping{
				MaxScale: 5,
				MinScale: g.Ptr(3),
			},
		}
		assert.Equal(t, 5, ct.MaxDecimals()) // MaxScale takes precedence
	})

	t.Run("no_scales_set", func(t *testing.T) {
		ct := &ColumnTyping{
			Decimal: &DecimalColumnTyping{},
		}
		assert.Equal(t, -1, ct.MaxDecimals())
	})
}

func TestDatasetSort(t *testing.T) {
	columns := NewColumnsFromFields("col1", "col2")
	data := NewDataset(columns)
	data.Append([]any{2, 3})
	data.Append([]any{1, 4})
	data.Append([]any{-1, 6})
	data.Append([]any{10, 1})
	g.P(data.Rows)
	data.Sort(0, true)
	g.P(data.Rows)
	data.Sort(1, false)
	g.P(data.Rows)
	// g.P(data.ColValuesStr(0))
}

func TestAddColumns(t *testing.T) {
	df := NewDataflow(0)
	df.Columns = NewColumnsFromFields("col1", "col2")
	assert.Equal(t, 2, len(df.Columns))
	newCols := NewColumnsFromFields("col2", "col3")
	df.AddColumns(newCols, false)
	assert.Equal(t, 3, len(df.Columns))
	g.Debug("%#v", df.Columns.Names())
}

func TestCleanName(t *testing.T) {
	names := []string{
		"great-one!9",
		"great-one!9",
		"great-one,9",
		"gag|hello",
		"Seller(s)",
		"1Seller(s) \n cool",
	}
	newNames := make([]string, len(names))

	for i, name := range names {
		newNames[i] = CleanName(name)
	}
	// g.P(newHeader)
	assert.Equal(t, "great_one_9", newNames[2])
	assert.Equal(t, "_1Seller_s_cool", newNames[5])
}

func TestParseString(t *testing.T) {
	sp := NewStreamProcessor()
	val := sp.ParseString("1697104406")
	assert.Equal(t, int64(1697104406), val)

	val = sp.ParseString("2024-04-24 14:49:58")
	g.P(val)
	g.P(cast.ToTime(val).Location().String() == "UTC")
	val = sp.ParseString("2024-04-24 13:49:58.000000 -03")
	g.P(val)
	g.P(cast.ToTime(val).Location().String() == "UTC")
	val = sp.ParseString("2024-05-05 09:10:09.000000 -07")
	g.P(val)
	g.P(cast.ToTime(val).Location().String() == "UTC")
}

func TestValidateNames(t *testing.T) {
	// Test 1: Column names shorter than max length - should remain unchanged
	t.Run("ColumnNamesShorterThanMaxLength", func(t *testing.T) {
		cols := NewColumnsFromFields("id", "name", "email")
		// Postgres has max_column_length of 63
		newCols := cols.ValidateNames(dbio.TypeDbPostgres)

		assert.Equal(t, "id", newCols[0].Name)
		assert.Equal(t, "name", newCols[1].Name)
		assert.Equal(t, "email", newCols[2].Name)
	})

	// Test 2: Column names exceeding max length - should be truncated
	t.Run("ColumnNamesExceedingMaxLength", func(t *testing.T) {
		longName := "this_is_a_very_long_column_name_that_exceeds_postgres_column_name_length_limit_of_63_characters"
		cols := NewColumnsFromFields("id", longName, "email")
		// Postgres has max_column_length of 63
		newCols := cols.ValidateNames(dbio.TypeDbPostgres)
		maxLength := cast.ToInt(dbio.TypeDbPostgres.GetTemplateValue("variable.max_column_length"))

		assert.Equal(t, "id", newCols[0].Name)
		assert.Equal(t, longName[:maxLength], newCols[1].Name) // Should be truncated to 63 chars
		assert.Equal(t, "email", newCols[2].Name)
	})

	// Test 3: Truncated names causing conflicts - should add suffix
	t.Run("TruncatedNamesWithConflicts", func(t *testing.T) {
		// Both columns will truncate to the same prefix
		col1 := "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567890"
		col2 := "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567890"
		cols := NewColumnsFromFields(col1, col2)
		// Postgres has max_column_length of 63
		newCols := cols.ValidateNames(dbio.TypeDbPostgres)

		// First column should be truncated without suffix
		assert.Equal(t, col1[:63], newCols[0].Name)

		// Second column should be truncated with a suffix
		// The suffix pattern should be original name (truncated to fit) + "_1"
		assert.Equal(t, "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567_1", newCols[1].Name)
		assert.Equal(t, 63, len(newCols[1].Name)) // Should still be 63 chars
	})

	// Test 4: Database with no max length defined - should return unchanged
	t.Run("DatabaseWithNoMaxLength", func(t *testing.T) {
		// Create a mock Type that doesn't have max_column_length defined
		mockType := dbio.Type("mock_type")

		longName := "this_is_a_very_long_column_name_that_would_normally_be_truncated"
		cols := NewColumnsFromFields("id", longName, "email")
		newCols := cols.ValidateNames(mockType)

		// Names should remain unchanged
		assert.Equal(t, "id", newCols[0].Name)
		assert.Equal(t, longName, newCols[1].Name)
		assert.Equal(t, "email", newCols[2].Name)
	})

	// Test 5: Multiple conflicts requiring incrementing suffixes
	t.Run("MultipleConflictsWithIncrementingSuffixes", func(t *testing.T) {
		// Create three identical columns that will cause conflicts
		prefix := "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567890"
		cols := NewColumnsFromFields(prefix, prefix, prefix)
		// Postgres has max_column_length of 63
		newCols := cols.ValidateNames(dbio.TypeDbPostgres)

		// First column should be truncated without suffix
		assert.Equal(t, prefix[:63], newCols[0].Name)

		// Second column should have _1 suffix
		assert.Equal(t, "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567_1", newCols[1].Name)

		// Third column should have _2 suffix
		assert.Equal(t, "abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_1234567_2", newCols[2].Name)
	})
}

func TestDecodeJSONIfBase64(t *testing.T) {
	// Test 1: Valid JSON - should return as-is
	t.Run("ValidJSON", func(t *testing.T) {
		validJSON := `{"key": "value", "number": 123}`
		result, err := DecodeJSONIfBase64(validJSON)
		assert.NoError(t, err)
		assert.Equal(t, validJSON, result)
	})

	// Test 2: Base64-encoded JSON - should decode
	t.Run("Base64EncodedJSON", func(t *testing.T) {
		originalJSON := `{"type": "service_account", "project_id": "my-project"}`
		base64JSON := base64.StdEncoding.EncodeToString([]byte(originalJSON))

		result, err := DecodeJSONIfBase64(base64JSON)
		assert.NoError(t, err)
		assert.Equal(t, originalJSON, result)
	})

	// Test 3: Complex nested JSON in base64
	t.Run("Base64EncodedComplexJSON", func(t *testing.T) {
		complexJSON := `{
  "type": "service_account",
  "project_id": "test-project",
  "private_key_id": "key123",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBg==\n-----END PRIVATE KEY-----\n",
  "client_email": "test@test.iam.gserviceaccount.com",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "nested": {
    "data": [1, 2, 3],
    "more": "values"
  }
}`
		base64JSON := base64.StdEncoding.EncodeToString([]byte(complexJSON))

		result, err := DecodeJSONIfBase64(base64JSON)
		assert.NoError(t, err)
		assert.JSONEq(t, complexJSON, result)
	})

	// Test 4: Invalid base64 - should return original
	t.Run("InvalidBase64", func(t *testing.T) {
		invalidBase64 := "this is not base64 !!@@##"
		result, err := DecodeJSONIfBase64(invalidBase64)
		assert.NoError(t, err)
		assert.Equal(t, invalidBase64, result)
	})

	// Test 5: Valid base64 but not JSON - should return original
	t.Run("Base64NotJSON", func(t *testing.T) {
		notJSON := "just some plain text"
		base64NotJSON := base64.StdEncoding.EncodeToString([]byte(notJSON))

		result, err := DecodeJSONIfBase64(base64NotJSON)
		assert.NoError(t, err)
		// Should return the base64 string since decoded content is not valid JSON
		assert.Equal(t, base64NotJSON, result)
	})

	// Test 6: Empty string - should return empty string
	t.Run("EmptyString", func(t *testing.T) {
		result, err := DecodeJSONIfBase64("")
		assert.NoError(t, err)
		assert.Equal(t, "", result)
	})

	// Test 7: JSON array in base64
	t.Run("Base64EncodedJSONArray", func(t *testing.T) {
		jsonArray := `[{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]`
		base64Array := base64.StdEncoding.EncodeToString([]byte(jsonArray))

		result, err := DecodeJSONIfBase64(base64Array)
		assert.NoError(t, err)
		assert.JSONEq(t, jsonArray, result)
	})

	// Test 8: JSON with special characters
	t.Run("Base64EncodedJSONWithSpecialChars", func(t *testing.T) {
		specialJSON := `{"message": "Hello\nWorld\t!", "emoji": "🎉", "quotes": "He said \"hi\""}`
		base64Special := base64.StdEncoding.EncodeToString([]byte(specialJSON))

		result, err := DecodeJSONIfBase64(base64Special)
		assert.NoError(t, err)
		assert.JSONEq(t, specialJSON, result)
	})
}
