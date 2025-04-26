package iop

import (
	"context"
	"io"
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

func NewExcelDataset(reader io.Reader, props map[string]string) (data Dataset, err error) {
	xls, err := NewExcelFromReader(reader)
	if err != nil {
		err = g.Error(err, "Unable to open Excel File from reader")
		return data, err
	}
	xls.Props = props

	sheetName := props["sheet"]
	sheetRange := ""

	if sheetName == "" {
		sheetName = xls.Sheets[0]
	} else if sheetNameArr := strings.Split(sheetName, "!"); len(sheetNameArr) == 2 {
		sheetName = sheetNameArr[0]
		sheetRange = sheetNameArr[1]
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
func NewExcelFromReader(reader io.Reader) (xls *Excel, err error) {
	f, err := excelize.OpenReader(reader)
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
	return xls.makeDatasetAuto(xls.File.GetRows(sheet))
}

// GetDatasetFromRange returns a dataset of the provided sheet / range
// cellRange example: `$AH$13:$AI$20` or `AH13:AI20` or `A:E`
func (xls *Excel) GetDatasetFromRange(sheet, cellRange string) (data Dataset, err error) {

	regexAlpha := *regexp.MustCompile(`[^a-zA-Z]`)
	regexNum := *regexp.MustCompile(`[^0-9]`)

	cellRange = strings.ReplaceAll(cellRange, "$", "")
	rangeArr := strings.Split(cellRange, ":")
	if len(rangeArr) != 2 {
		err = g.Error(err, "Invalid range: "+cellRange)
		return
	}

	RangeStartCol := regexAlpha.ReplaceAllString(rangeArr[0], "")
	RangeEndCol := regexAlpha.ReplaceAllString(rangeArr[1], "")
	colStart := excelize.TitleToNumber(RangeStartCol)
	colEnd := excelize.TitleToNumber(RangeEndCol)
	rowStart := cast.ToInt(regexNum.ReplaceAllString(rangeArr[0], "")) - 1
	rowEnd := cast.ToInt(regexNum.ReplaceAllString(rangeArr[1], "")) - 1

	allRows := xls.File.GetRows(sheet)

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
		rows = xls.File.GetRows(shtName)
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
