package iop

import (
	"context"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/xuri/excelize/v2"
)

func NewExcelDataset(reader io.Reader, props map[string]string) (data Dataset, err error) {
	options := excelize.Options{
		Password:         props["password"],
		ShortDatePattern: props["short_date_pattern"],
		LongDatePattern:  props["long_date_pattern"],
		LongTimePattern:  props["long_time_pattern"],
	}
	xls, err := NewExcelFromReader(reader, options)
	if err != nil {
		err = g.Error(err, "Unable to open Excel File from reader")
		return data, err
	}
	xls.Props = props

	sheetName := props["sheet"]
	sheetRange := ""

	if sheetNameArr := strings.Split(sheetName, "!"); len(sheetNameArr) == 2 {
		sheetName = sheetNameArr[0]
		sheetRange = sheetNameArr[1]
	}
	if sheetName == "" {
		sheetName = xls.Sheets[0] // if sheet is blank, use first
	}

	if sheetRange != "" {
		data, err = xls.GetDatasetFromRange(sheetName, sheetRange)
		if err != nil {
			err = g.Error(err, "Unable to get range data for %s!%s", sheetName, sheetRange)
			return data, err
		}
	} else {
		data = xls.GetDataset(sheetName)
	}
	return
}

// Excel represent an Excel object pointing to its file
type Excel struct {
	spreadsheet
	File       *excelize.File
	Sheets     []string
	Path       string
	context    *g.Context
	sheetIndex map[string]int
}

// NewExcel creates a new excel file
func NewExcel() (xls *Excel) {

	xls = &Excel{
		File:    excelize.NewFile(),
		context: g.NewContext(context.Background()),
	}
	xls.spreadsheet.Props = map[string]string{}

	return
}

// NewExcelFromFile return a new Excel instance from a local file
func NewExcelFromFile(path string) (xls *Excel, err error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		err = g.Error(err, "Unable to open file: "+path)
		return
	}

	xls = &Excel{
		File:    f,
		Path:    path,
		context: g.NewContext(context.Background()),
	}
	xls.spreadsheet.Props = map[string]string{}

	xls.RefreshSheets()

	return
}

// NewExcelFromReader return a new Excel instance from a reader
func NewExcelFromReader(reader io.Reader, opts ...excelize.Options) (xls *Excel, err error) {
	f, err := excelize.OpenReader(reader, opts...)
	if err != nil {
		err = g.Error(err, "Unable to open reader")
		return
	}

	sheetMap := f.GetSheetMap()
	sheets := make([]string, len(sheetMap))
	for i := range sheets {
		sheets[i] = sheetMap[i+1]
	}

	xls = &Excel{
		File:    f,
		Sheets:  sheets,
		context: g.NewContext(context.Background()),
	}
	xls.spreadsheet.Props = map[string]string{}

	return
}

// RefreshSheets refresh sheet index data
func (xls *Excel) RefreshSheets() (err error) {

	sheetMap := xls.File.GetSheetMap()
	sheets := make([]string, len(sheetMap))
	sheetIndex := map[string]int{}
	for i := range sheets {
		sheets[i] = sheetMap[i+1]
		sheetIndex[sheetMap[i+1]] = i
	}
	xls.Sheets = sheets
	xls.sheetIndex = sheetIndex

	return
}

// GetDataset returns a dataset of the provided sheet
func (xls *Excel) GetDataset(sheet string) (data Dataset) {
	rows, _ := xls.File.GetRows(sheet)
	return xls.makeDatasetAuto(rows)
}

// GetDatasetFromRange returns a dataset of the provided sheet / range
// cellRange examples:
//   - `$AH$13:$AI$20` or `AH13:AI20` - standard cell range
//   - `A:E` - column range (all rows)
//   - `9:15` - row range (detects columns with data)
//   - `A4:C` - partial range (extends to last row)
func (xls *Excel) GetDatasetFromRange(sheet, cellRange string) (data Dataset, err error) {

	regexAlpha := *regexp.MustCompile(`[^a-zA-Z]`)
	regexNum := *regexp.MustCompile(`[^0-9]`)

	allRows, err := xls.File.GetRows(sheet)
	if err != nil {
		return data, g.Error(err, "could not get rows")
	}

	// Normalize range format (handles row-only and partial ranges)
	cellRange, err = xls.normalizeRange(cellRange, allRows)
	if err != nil {
		return data, g.Error(err, "could not normalize range")
	}

	rangeArr := strings.Split(cellRange, ":")
	if len(rangeArr) != 2 {
		err = g.Error(err, "Invalid range: "+cellRange)
		return
	}

	RangeStartCol := regexAlpha.ReplaceAllString(rangeArr[0], "")
	RangeEndCol := regexAlpha.ReplaceAllString(rangeArr[1], "")
	colStart := xls.TitleToNumber(RangeStartCol)
	colEnd := xls.TitleToNumber(RangeEndCol)
	rowStart := cast.ToInt(regexNum.ReplaceAllString(rangeArr[0], "")) - 1
	rowEnd := cast.ToInt(regexNum.ReplaceAllString(rangeArr[1], "")) - 1

	if rowStart == -1 {
		rowStart = 0
	}
	if rowEnd == -1 {
		rowEnd = len(allRows) - 1
	}

	if len(allRows) < rowEnd {
		err = g.Error(
			"Input row range is larger than file row range: %d < %d",
			len(allRows), rowEnd,
		)
		return
	} else if len(allRows[0]) < colEnd {
		err = g.Error(
			"Input col range is larger than file col range: %d < %d",
			len(allRows[0]), colEnd,
		)
		return
	}

	i := 0
	rangeRows := make([][]string, rowEnd-rowStart+1)
	for r := rowStart; r <= rowEnd; r++ {
		row0 := []string{}
		if r >= len(allRows) {
			continue
		}
		for c := colStart; c <= colEnd; c++ {
			if c >= len(allRows[r]) {
				continue
			}
			row0 = append(row0, strings.TrimSpace(allRows[r][c]))
		}
		rangeRows[i] = row0
		i++
	}

	data = xls.makeDatasetStr(rangeRows)

	return
}

// WriteToWriter write to a provided writer
func (xls *Excel) WriteToWriter(w io.Writer) (err error) {
	return xls.File.Write(w)
}

// WriteToFile write to a file
func (xls *Excel) WriteToFile(path string) (err error) {
	file, err := os.Create(path)
	if err != nil {
		return g.Error(err, "could not open %s for writing", path)
	}
	_, err = xls.File.WriteTo(file)
	if err != nil {
		return g.Error(err, "could not write to %s", path)
	}
	return
}

func (xls *Excel) createSheet(shtName string) (newShtName string) {
	// ensure not duplicate sheet names
	newShtName = shtName
	{
		ok := true
		i := 1
		for {
			_, ok = xls.sheetIndex[newShtName]
			if !ok {
				break
			}
			newShtName = g.F("%s%d", shtName, i)
			i++
		}
	}
	xls.File.NewSheet(newShtName)
	xls.RefreshSheets()
	return
}

// TitleToNumber provides a function to convert Excel sheet column title to
// int (this function doesn't do value check currently). For example convert
// AK and ak to column title 36:
//
//	excelize.TitleToNumber("AK")
//	excelize.TitleToNumber("ak")
func (xls *Excel) TitleToNumber(s string) int {
	weight := 0.0
	sum := 0
	for i := len(s) - 1; i >= 0; i-- {
		ch := int(s[i])
		if int(s[i]) >= int('a') && int(s[i]) <= int('z') {
			ch = int(s[i]) - 32
		}
		sum = sum + (ch-int('A')+1)*int(math.Pow(26, weight))
		weight++
	}
	return sum - 1
}

// NumberToTitle converts a 0-based column index to Excel column letters.
// For example: 0 -> "A", 25 -> "Z", 26 -> "AA"
func (xls *Excel) NumberToTitle(n int) string {
	result := ""
	for n >= 0 {
		result = string(rune('A'+n%26)) + result
		n = n/26 - 1
	}
	return result
}

// normalizeRange normalizes various range formats into standard Excel range notation.
// Supported formats:
//   - "9:15" (row-only) -> "A9:<lastCol>15" (detects last column with data)
//   - "A4:C" (partial) -> "A4:C<lastRow>" (extends to last row)
//   - "A4:C8" (standard) -> "A4:C8" (no change)
func (xls *Excel) normalizeRange(cellRange string, allRows [][]string) (string, error) {
	regexAlpha := regexp.MustCompile(`[^a-zA-Z]`)
	regexNum := regexp.MustCompile(`[^0-9]`)
	regexRowOnly := regexp.MustCompile(`^\d+:\d+$`)

	cellRange = strings.ReplaceAll(cellRange, "$", "")
	rangeArr := strings.Split(cellRange, ":")
	if len(rangeArr) != 2 {
		return "", g.Error("invalid range format: %s", cellRange)
	}

	startPart := rangeArr[0]
	endPart := rangeArr[1]

	// Check if it's a row-only range like "9:15"
	if regexRowOnly.MatchString(cellRange) {
		rowStart := cast.ToInt(startPart)
		rowEnd := cast.ToInt(endPart)

		if rowStart <= 0 || rowEnd <= 0 {
			return "", g.Error("invalid row numbers in range: %s", cellRange)
		}
		if rowStart > rowEnd {
			return "", g.Error("row range %d:%d is reversed", rowStart, rowEnd)
		}

		// Convert to 0-based indices
		rowStartIdx := rowStart - 1
		rowEndIdx := rowEnd - 1

		if rowStartIdx >= len(allRows) {
			return "", g.Error("row range %d:%d exceeds sheet bounds (max row: %d)", rowStart, rowEnd, len(allRows))
		}

		// Scan rows to find max column with data (limit to SampleSize rows for performance)
		maxCol := 0
		scannedRows := 0
		for r := rowStartIdx; r <= rowEndIdx && r < len(allRows); r++ {
			if len(allRows[r]) > maxCol {
				maxCol = len(allRows[r])
			}
			scannedRows++
			if scannedRows >= SampleSize {
				break
			}
		}

		if maxCol == 0 {
			return "", g.Error("no data found in row range %d:%d", rowStart, rowEnd)
		}

		// Convert to column letter (maxCol is count, so subtract 1 for 0-based index)
		endCol := xls.NumberToTitle(maxCol - 1)
		return g.F("A%d:%s%d", rowStart, endCol, rowEnd), nil
	}

	// Extract column and row parts
	_ = regexAlpha.ReplaceAllString(startPart, "") // startCol - not needed for normalization
	endCol := regexAlpha.ReplaceAllString(endPart, "")
	startRowStr := regexNum.ReplaceAllString(startPart, "")
	endRowStr := regexNum.ReplaceAllString(endPart, "")

	// Check for partial range like "A4:C" (has start row but no end row)
	if startRowStr != "" && endRowStr == "" && endCol != "" {
		startRow := cast.ToInt(startRowStr)
		if startRow <= 0 {
			return "", g.Error("invalid start row in range: %s", cellRange)
		}
		// Extend to last row
		return g.F("%s:%s%d", startPart, endCol, len(allRows)), nil
	}

	// Validate row order if both rows are specified
	if startRowStr != "" && endRowStr != "" {
		startRow := cast.ToInt(startRowStr)
		endRow := cast.ToInt(endRowStr)
		if startRow > endRow {
			return "", g.Error("row range %d:%d is reversed", startRow, endRow)
		}
	}

	// Standard format or column-only format (A:E), return as-is
	return cellRange, nil
}

// WriteSheet write a datastream into a sheet
// mode can be: `new`, `append` or `overwrite`. Default is `new`
func (xls *Excel) WriteSheet(shtName string, ds *Datastream, mode string) (err error) {

	if mode == "" || mode == "new" {
		// create sheet
		shtName = xls.createSheet(shtName)
	}

	rows := [][]string{}
	_, shtExists := xls.sheetIndex[shtName]
	if shtExists {
		rows, err = xls.File.GetRows(shtName)
		if err != nil {
			return g.Error(err, "could not get rows")
		}
	} else {
		shtName = xls.createSheet(shtName)
	}

	i := len(rows) + 1
	if mode == "overwrite" {
		i = 1
	}

	col := "A"

	// write header
	cellRange := g.F("%s%d", col, i)
	if mode != "append" {
		header := []interface{}{}
		for _, field := range ds.GetFields(false, true) {
			header = append(header, field)
		}
		xls.File.SetSheetRow(shtName, cellRange, &header)
		i++
	}

	for row := range ds.Rows() {
		cellRange = g.F("%s%d", col, i)
		xls.File.SetSheetRow(shtName, cellRange, &row)
		i++
	}

	// for overwrite, blank out the remaining rows
	// TODO: what about the remaining columns (on the right)
	if mode == "overwrite" && i < len(rows)+1 {
		for j := i; j < len(rows)+1; j++ {
			row := make([]interface{}, len(ds.Columns))
			cellRange = g.F("%s%d", col, j)
			xls.File.SetSheetRow(shtName, cellRange, &row)
		}
	}

	xls.RefreshSheets()

	return
}

// excelDateToTime convert Excel dates to time.Time
func excelDateToTime(excelDate float64) time.Time {
	// Excel dates are days since 1900-01-01, with a quirk for leap years
	// There's a leap year bug in Excel - it thinks 1900 was a leap year
	if excelDate > 60 {
		excelDate-- // Adjust for Excel's leap year bug
	}

	// Split into days and fractional day
	days := int(excelDate)
	frac := excelDate - float64(days)

	// Start date: 1900-01-01
	date := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

	// Add days
	date = date.AddDate(0, 0, days)

	// Add fractional day as hours, minutes, seconds
	seconds := int(math.Round(frac * 86400)) // 86400 seconds in a day
	hours := seconds / 3600
	seconds -= hours * 3600
	minutes := seconds / 60
	seconds -= minutes * 60

	return time.Date(date.Year(), date.Month(), date.Day(), hours, minutes, seconds, 0, time.UTC)
}
