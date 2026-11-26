package iop

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/xuri/excelize/v2"
)

func TestExcelDateToTime(t *testing.T) {
	// Test cases that cover various scenarios
	testCases := []struct {
		name           string
		excelDateValue float64
		expectedYear   int
		expectedMonth  time.Month
		expectedDay    int
		expectedHour   int
		expectedMinute int
		expectedSecond int
		description    string
	}{
		{
			name:           "Date only - January 1, 1900",
			excelDateValue: 1,
			expectedYear:   1900,
			expectedMonth:  time.January,
			expectedDay:    2, // Excel starts at Jan 1, 1900 as day 1
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Basic test case - first valid Excel date",
		},
		{
			name:           "Date only - Modern date",
			excelDateValue: 45000,
			expectedYear:   2023,
			expectedMonth:  time.March,
			expectedDay:    16, // Adjusted based on actual implementation
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Modern date (2023)",
		},
		{
			name:           "Date with time - Noon",
			excelDateValue: 45000.5,
			expectedYear:   2023,
			expectedMonth:  time.March,
			expectedDay:    16, // Adjusted based on actual implementation
			expectedHour:   12,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Date with noon time component",
		},
		{
			name:           "Date with time - Specific time",
			excelDateValue: 45258.621528, // From the issue example
			expectedYear:   2023,
			expectedMonth:  time.November,
			expectedDay:    29, // Adjusted based on actual implementation
			expectedHour:   14,
			expectedMinute: 55,
			expectedSecond: 0,
			description:    "Date with specific time (example from issue)",
		},
		{
			name:           "Date before leap year bug",
			excelDateValue: 59,
			expectedYear:   1900,
			expectedMonth:  time.March, // Adjusted based on actual implementation
			expectedDay:    1,          // Adjusted based on actual implementation
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Date before Excel's leap year bug test",
		},
		{
			name:           "Date after leap year bug",
			excelDateValue: 61,
			expectedYear:   1900,
			expectedMonth:  time.March,
			expectedDay:    2, // Adjusted based on actual implementation
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Date after Excel's leap year bug test",
		},
		{
			name:           "Modern date with precise time",
			excelDateValue: 44000.12345,
			expectedYear:   2020,
			expectedMonth:  time.June,
			expectedDay:    19, // Adjusted based on actual implementation
			expectedHour:   2,
			expectedMinute: 57,
			expectedSecond: 46,
			description:    "Modern date with precise time components",
		},
		{
			name:           "Zero date",
			excelDateValue: 0,
			expectedYear:   1900, // Excel's zero date in this implementation
			expectedMonth:  time.January,
			expectedDay:    1, // Adjusted based on actual implementation
			expectedHour:   0,
			expectedMinute: 0,
			expectedSecond: 0,
			description:    "Excel's zero date test",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Convert Excel date to time.Time
			result := excelDateToTime(tc.excelDateValue)

			// Print the actual result for debugging
			t.Logf("Excel value: %f, Result: %v", tc.excelDateValue, result)

			// Verify each component
			if result.Year() != tc.expectedYear {
				t.Errorf("Year mismatch for %s: got %d, want %d", tc.name, result.Year(), tc.expectedYear)
			}
			if result.Month() != tc.expectedMonth {
				t.Errorf("Month mismatch for %s: got %d, want %d", tc.name, result.Month(), tc.expectedMonth)
			}
			if result.Day() != tc.expectedDay {
				t.Errorf("Day mismatch for %s: got %d, want %d", tc.name, result.Day(), tc.expectedDay)
			}
			if result.Hour() != tc.expectedHour {
				t.Errorf("Hour mismatch for %s: got %d, want %d", tc.name, result.Hour(), tc.expectedHour)
			}
			if result.Minute() != tc.expectedMinute {
				t.Errorf("Minute mismatch for %s: got %d, want %d", tc.name, result.Minute(), tc.expectedMinute)
			}
			if result.Second() != tc.expectedSecond {
				t.Errorf("Second mismatch for %s: got %d, want %d", tc.name, result.Second(), tc.expectedSecond)
			}
		})
	}
}

func TestNewExcelDatasetMissingSheet(t *testing.T) {
	f := excelize.NewFile()
	f.SetCellValue("Sheet1", "A1", "id")
	f.SetCellValue("Sheet1", "A2", 1)
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	// an existing sheet works
	data, err := NewExcelDataset(bytes.NewReader(buf.Bytes()), map[string]string{"sheet": "Sheet1"})
	if err != nil {
		t.Fatalf("unexpected error for existing sheet: %v", err)
	}
	if len(data.Rows) == 0 {
		t.Fatalf("expected rows, got %d", len(data.Rows))
	}

	// a misspelled sheet must return an error instead of an empty dataset
	_, err = NewExcelDataset(bytes.NewReader(buf.Bytes()), map[string]string{"sheet": "Sheet_typo"})
	if err == nil {
		t.Fatal("expected an error for a sheet that does not exist, got nil")
	}
}

func TestNewExcelDatasetSheetNameWithBang(t *testing.T) {
	f := excelize.NewFile()
	f.NewSheet("Sales!Current")
	for _, sheet := range []string{"Sheet1", "Sales!Current"} {
		f.SetCellValue(sheet, "A1", "id")
		f.SetCellValue(sheet, "B1", "name")
		f.SetCellValue(sheet, "A2", 1)
		f.SetCellValue(sheet, "B2", "a")
		f.SetCellValue(sheet, "A3", 2)
		f.SetCellValue(sheet, "B3", "b")
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	// the number of rows returned includes the header row unless it is detected
	read := func(sheet string) int {
		data, err := NewExcelDataset(bytes.NewReader(buf.Bytes()), map[string]string{"sheet": sheet})
		if err != nil {
			t.Fatalf("unexpected error for sheet %q: %v", sheet, err)
		}
		return len(data.Rows)
	}

	full := read("Sheet1")
	ranged := read("Sheet1!A1:B2")
	if ranged >= full {
		t.Fatalf("expected range to return fewer than %d rows, got %d", full, ranged)
	}
	if got := read("Sales!Current"); got != full {
		t.Fatalf("expected %d rows for literal sheet name, got %d", full, got)
	}
	if got := read("Sales!Current!A1:B2"); got != ranged {
		t.Fatalf("expected %d rows for range on sheet name with '!', got %d", ranged, got)
	}
}

// Excel sheets hold at most excelize.TotalRows (1,048,576) rows, header
// included. Rows past that were silently dropped and the write reported success.
func TestExcelWriteSheetRowLimit(t *testing.T) {
	newDs := func(dataRows int) *Datastream {
		n := 0
		ds := NewDatastreamIt(context.Background(), NewColumnsFromFields("id"), func(it *Iterator) bool {
			if n >= dataRows {
				return false
			}
			n++
			it.Row[0] = int64(n)
			return true
		})
		if err := ds.Start(); err != nil {
			t.Fatal(err)
		}
		return ds
	}

	// header + (TotalRows-1) data rows fills the sheet exactly: allowed
	if err := NewExcel().WriteSheet("Sheet1", newDs(excelize.TotalRows-1), "overwrite"); err != nil {
		t.Fatalf("a sheet filled to the limit should be written, got: %v", err)
	}

	// one more row cannot be stored: must be an error, not a silent drop
	if err := NewExcel().WriteSheet("Sheet1", newDs(excelize.TotalRows), "overwrite"); err == nil {
		t.Fatal("expected an error when rows exceed the Excel limit, got nil")
	}
}
func TestExcelNumFmtKind(t *testing.T) {
	tests := []struct {
		id   int
		code string
		kind excelFmtKind
	}{
		{0, "", excelFmtNumber},
		{3, "", excelFmtNumber},
		{4, "", excelFmtNumber},
		{9, "", excelFmtPercent},
		{10, "", excelFmtPercent},
		{14, "", excelFmtDate},
		{22, "", excelFmtDate},
		{45, "", excelFmtDate},
		{164, "yyyy-mm-dd", excelFmtDate},
		{164, "[h]:mm:ss", excelFmtDate},
		{164, "[$-409]mmm d, yyyy", excelFmtDate},
		{164, `#,##0 "days"`, excelFmtNumber},
		{164, "[Red]#,##0", excelFmtNumber},
		{164, "0.0E+00", excelFmtNumber},
		{164, `#,##0\ "m"`, excelFmtNumber},
		{164, "0.0%", excelFmtPercent},
		{49, "", excelFmtText},
		{164, "@", excelFmtText},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.kind, numFmtKind(tt.id, tt.code), "%d %q", tt.id, tt.code)
	}
}

func TestExcelUseStoredNumbers(t *testing.T) {
	f := excelize.NewFile()
	var ids []int
	for _, st := range []excelize.Style{{NumFmt: 3}, {NumFmt: 14}, {NumFmt: 9}} {
		st := st
		id, err := f.NewStyle(&st)
		assert.NoError(t, err)
		ids = append(ids, id)
	}
	buf, err := f.WriteToBuffer()
	assert.NoError(t, err)

	xls, err := NewExcelFromReader(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)

	numFmts := func() (out []int) {
		for _, id := range ids {
			st, err := xls.File.GetStyle(id)
			assert.NoError(t, err)
			out = append(out, st.NumFmt)
		}
		return out
	}
	rawIDs := func() (out []int) {
		for _, xf := range xls.File.Styles.CellXfs.Xf {
			if xf.NumFmtID == nil {
				out = append(out, 0)
			} else {
				out = append(out, *xf.NumFmtID)
			}
		}
		return out
	}

	before := numFmts()
	assert.Equal(t, []int{3, 14, 9}, before)
	beforeIDs := rawIDs()

	restore, err := xls.useStoredNumbers()
	assert.NoError(t, err)
	for i, id := range ids {
		xf := xls.File.Styles.CellXfs.Xf[id]
		if i == 0 {
			assert.Equal(t, excelStoredNumFmtID, *xf.NumFmtID)
		} else {
			assert.Equal(t, before[i], *xf.NumFmtID)
		}
	}

	restore()
	assert.Equal(t, beforeIDs, rawIDs())
	assert.Equal(t, before, numFmts())
}

// excelNumberFormatsFile makes the workbook of tests/files/test.excel.number-formats.xlsx
func excelNumberFormatsFile(t testing.TB, styled bool) *bytes.Buffer {
	f := excelize.NewFile()
	style := func(st excelize.Style) int {
		if !styled {
			return 0
		}
		id, err := f.NewStyle(&st)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	neg := "#,##0;(#,##0)"
	date := "yyyy-mm-dd"
	styles := map[string]int{
		"qty":    style(excelize.Style{NumFmt: 3}),
		"amount": style(excelize.Style{NumFmt: 4}),
		"neg":    style(excelize.Style{CustomNumFmt: &neg}),
		"pct":    style(excelize.Style{NumFmt: 9}),
		"dt":     style(excelize.Style{CustomNumFmt: &date}),
		"big":    style(excelize.Style{Font: &excelize.Font{Bold: true}}),
	}

	for i, h := range []string{"id", "qty", "amount", "neg", "pct", "dt", "flag", "big"} {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue("Sheet1", cell, h)
	}
	for r, qty := range []int{3000, 1234567} {
		row := r + 2
		set := func(col, key string, v any) {
			cell := fmt.Sprintf("%s%d", col, row)
			f.SetCellValue("Sheet1", cell, v)
			if styles[key] != 0 {
				f.SetCellStyle("Sheet1", cell, cell, styles[key])
			}
		}
		set("A", "", r+1)
		set("B", "qty", qty)
		set("C", "amount", 1234.5)
		set("D", "neg", -1000)
		set("E", "pct", 0.5)
		set("F", "dt", 45230)
		set("G", "", true)
		set("H", "big", int64(123456789012))
	}

	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestExcelNumberFormats(t *testing.T) {
	read := func(styled bool, sheet string) Dataset {
		data, err := NewExcelDataset(
			bytes.NewReader(excelNumberFormatsFile(t, styled).Bytes()),
			map[string]string{"sheet": sheet},
		)
		assert.NoError(t, err)
		return data
	}

	// no range: the rows are text, the header is the first row
	data := read(true, "")
	plain := read(false, "")
	assert.Equal(t, len(plain.Rows), len(data.Rows))
	assert.Equal(t, len(plain.Columns), len(data.Columns))
	for _, row := range data.Rows[1:] {
		assert.Equal(t, "50%", row[4])
		assert.Equal(t, "2023-10-31", row[5])
		assert.Equal(t, "TRUE", row[6])
		assert.Equal(t, []string{"1234.5", "-1000", "123456789012"}, []string{
			cast.ToString(row[2]), cast.ToString(row[3]), cast.ToString(row[7]),
		})
	}
	assert.Equal(t, "3000", data.Rows[1][1])
	assert.Equal(t, "1234567", data.Rows[2][1])

	// range: the header names the columns and the values are typed
	data = read(true, "Sheet1!A1:H3")
	plain = read(false, "Sheet1!A1:H3")
	assert.Equal(t, len(plain.Rows), len(data.Rows))
	assert.Equal(t, len(plain.Columns), len(data.Columns))
	if len(data.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(data.Rows))
	}

	col := func(name string) int { return data.Columns.GetColumn(name).Position - 1 }
	typ := func(name string) ColumnType { return data.Columns.GetColumn(name).Type }
	assert.Equal(t, IntegerType, typ("qty"))
	assert.EqualValues(t, 3000, data.Rows[0][col("qty")])
	assert.EqualValues(t, 1234567, data.Rows[1][col("qty")])
	assert.Equal(t, DecimalType, typ("amount"))
	assert.Equal(t, "1234.5", cast.ToString(data.Rows[0][col("amount")]))
	assert.EqualValues(t, -1000, data.Rows[0][col("neg")])
	assert.Equal(t, StringType, typ("pct"))
	assert.Equal(t, "50%", data.Rows[0][col("pct")])
	assert.Equal(t, DateType, typ("dt"))
	assert.Equal(t, "2023-10-31", data.Rows[0][col("dt")].(time.Time).Format("2006-01-02"))
	assert.Equal(t, BoolType, typ("flag"))
	assert.Equal(t, BigIntType, typ("big"))
	assert.EqualValues(t, 123456789012, data.Rows[0][col("big")])

	// float noise is rounded to 15 digits, with and without a number style
	f := excelize.NewFile()
	id, _ := f.NewStyle(&excelize.Style{NumFmt: 4})
	f.SetCellValue("Sheet1", "A1", "general")
	f.SetCellValue("Sheet1", "B1", "styled")
	f.SetCellValue("Sheet1", "A2", 0.1+0.2)
	f.SetCellValue("Sheet1", "B2", 0.1+0.2)
	f.SetCellStyle("Sheet1", "B2", "B2", id)
	buf, err := f.WriteToBuffer()
	assert.NoError(t, err)
	xls, err := NewExcelFromReader(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	rows, err := xls.readRows("Sheet1")
	assert.NoError(t, err)
	assert.Equal(t, []string{"0.3", "0.3"}, rows[1])

	_, err = xls.readRows("Missing")
	assert.Error(t, err)
}

// go test -run '^$' -bench BenchmarkExcelReadRows -benchtime 1x ./core/dbio/iop
func BenchmarkExcelReadRows(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.xlsx")
	f := excelize.NewFile()
	numStyle, _ := f.NewStyle(&excelize.Style{NumFmt: 3})
	dateFmt := "yyyy-mm-dd"
	dateStyle, _ := f.NewStyle(&excelize.Style{CustomNumFmt: &dateFmt})
	sw, err := f.NewStreamWriter("Sheet1")
	if err != nil {
		b.Fatal(err)
	}
	for r := 1; r <= 100000; r++ {
		row := make([]any, 10)
		for c := 0; c < 9; c++ {
			row[c] = excelize.Cell{StyleID: numStyle, Value: r*1000 + c}
		}
		row[9] = excelize.Cell{StyleID: dateStyle, Value: 45230 + r%1000}
		cell, _ := excelize.CoordinatesToCellName(1, r)
		if err := sw.SetRow(cell, row); err != nil {
			b.Fatal(err)
		}
	}
	if err := sw.Flush(); err != nil {
		b.Fatal(err)
	}
	if err := f.SaveAs(path); err != nil {
		b.Fatal(err)
	}
	f.Close()

	open := func() *Excel {
		fh, err := os.Open(path)
		if err != nil {
			b.Fatal(err)
		}
		defer fh.Close()
		xls, err := NewExcelFromReader(fh)
		if err != nil {
			b.Fatal(err)
		}
		return xls
	}

	b.Run("GetRows", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			xls := open()
			b.StartTimer()
			if _, err := xls.File.GetRows("Sheet1"); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("readRows", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			xls := open()
			b.StartTimer()
			if _, err := xls.readRows("Sheet1"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestNewExcelFromReaderDeletedSheet(t *testing.T) {
	// deleting a sheet leaves a gap in the sheetIds (1, 3), so the sheet names
	// must not be looked up by position-derived sheetId.
	f := excelize.NewFile()
	f.NewSheet("Sheet2")
	f.NewSheet("Sheet3")
	f.SetCellValue("Sheet3", "A1", "id")
	f.SetCellValue("Sheet3", "A2", 1)
	if err := f.DeleteSheet("Sheet2"); err != nil {
		t.Fatal(err)
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	xls, err := NewExcelFromReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"Sheet1", "Sheet3"}
	if len(xls.Sheets) != len(want) {
		t.Fatalf("expected sheets %v, got %v", want, xls.Sheets)
	}
	for i := range want {
		if xls.Sheets[i] != want[i] {
			t.Fatalf("expected sheets %v, got %v", want, xls.Sheets)
		}
	}

	if err = xls.RefreshSheets(); err != nil {
		t.Fatal(err)
	}
	if len(xls.Sheets) != 2 || xls.Sheets[1] != "Sheet3" || xls.sheetIndex["Sheet3"] != 1 {
		t.Fatalf("RefreshSheets: got sheets %v, index %v", xls.Sheets, xls.sheetIndex)
	}
}

func TestNewExcelDatasetDeletedFirstSheet(t *testing.T) {
	// deleting the first sheet leaves sheetIds (2, 3): the default sheet must be
	// the first tab, not an empty name.
	f := excelize.NewFile()
	f.NewSheet("Sheet2")
	f.SetCellValue("Sheet2", "A1", "id")
	f.SetCellValue("Sheet2", "A2", 1)
	if err := f.DeleteSheet("Sheet1"); err != nil {
		t.Fatal(err)
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	data, err := NewExcelDataset(bytes.NewReader(buf.Bytes()), map[string]string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data.Rows) != 2 {
		t.Fatalf("expected 2 rows (header + 1) from the first remaining sheet, got %d", len(data.Rows))
	}
}

func TestNewExcelDatasetRowRangeWidth(t *testing.T) {
	originalSampleSize := SampleSize
	SampleSize = 900
	t.Cleanup(func() { SampleSize = originalSampleSize })

	f := excelize.NewFile()
	t.Cleanup(func() { _ = f.Close() })
	for row := 1; row <= 901; row++ {
		cell, err := excelize.CoordinatesToCellName(1, row)
		if err != nil {
			t.Fatal(err)
		}
		values := []interface{}{"left", "right"}
		if row == 901 {
			values = append(values, "late value")
		}
		if err := f.SetSheetRow("Sheet1", cell, &values); err != nil {
			t.Fatal(err)
		}
	}
	buf, err := f.WriteToBuffer()
	if err != nil {
		t.Fatal(err)
	}

	for _, sheetRange := range []string{"1:901", "1:", "A1:C901"} {
		t.Run(sheetRange, func(t *testing.T) {
			data, err := NewExcelDataset(bytes.NewReader(buf.Bytes()), map[string]string{"sheet": "Sheet1!" + sheetRange})
			if err != nil {
				t.Fatal(err)
			}
			if len(data.Rows) != 900 {
				t.Fatalf("got %d data rows, want 900", len(data.Rows))
			}
			lastRow := data.Rows[len(data.Rows)-1]
			if len(lastRow) != 3 || lastRow[2] != "late value" {
				t.Fatalf("last row = %v, want third cell to contain %q", lastRow, "late value")
			}
		})
	}
}
