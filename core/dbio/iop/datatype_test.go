package iop

import (
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

func initProcessRow(name string) (columns []Column, row []interface{}) {

	if name == "float" {
		row = []interface{}{5535.21414}
	} else if name == "decimal" {
		row = []interface{}{[]byte{0x31, 0x30, 0x33, 0x32, 0x2e, 0x34, 0x34, 0x32, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30}}
		row = []interface{}{"5535.214140000"}
	} else if name == "int" {
		row = []interface{}{int(48714719874194)}
	} else if name == "int64" {
		row = []interface{}{int64(48714719874194)}
	} else if name == "string" {
		row = []interface{}{g.RandString(g.AlphaNumericRunes, 1000)}
	} else if name == "timestamp" || name == "timestampz" {
		row = []interface{}{time.Now()}
		// row = []interface{}{"17-OCT-20 07.01.59.000000 PM"}
	} else if name == "bool" {
		row = []interface{}{false}
	} else if name == "blank" {
		row = []interface{}{""}
	} else {
		row = []interface{}{
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
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringFloat(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("float")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringNumeric(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("decimal")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val, "decimal")
		}
	}
}
func BenchmarkCastToStringInt(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringInt64(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("int64")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
		}
	}
}
func BenchmarkCastToStringString(b *testing.B) {
	sp := NewStreamProcessor()
	_, row := initProcessRow("string")
	for n := 0; n < b.N; n++ {
		for i, val := range row {
			row[i] = sp.CastToString(i, val)
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
	row := make([]interface{}, 3)
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
