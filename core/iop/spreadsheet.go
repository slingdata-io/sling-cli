package iop

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/360EntSecGroup-Skylar/excelize"
	h "github.com/flarco/gutil"
	"github.com/spf13/cast"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

type spreadsheet struct {
	properties map[string]string
}

func (s *spreadsheet) makeDatasetAuto(rows [][]string) (data Dataset) {
	var blankCellCnt, trailingBlankRows int
	rowWidthDistro := map[int]int{}
	allRows := [][]string{}
	maxCount := 0
	// widthMostUsed := 0
	for _, row0 := range rows {
		blankCellCnt = 0
		row := make([]string, len(row0))
		for i, val := range row0 {
			val = strings.TrimSpace(val)
			if val == "" {
				blankCellCnt++
			} else {
				blankCellCnt = 0
			}
			row[i] = val
		}
		// h.P(row)

		rowWidthDistro[len(row)]++
		allRows = append(allRows, row)

		if blankCellCnt == len(row) {
			trailingBlankRows++
		} else {
			trailingBlankRows = 0
		}

		if rowWidthDistro[len(row)] > maxCount {
			maxCount = rowWidthDistro[len(row)]
			// widthMostUsed = len(row)
		}
	}

	// h.Debug("trailingBlankRows: %d", trailingBlankRows)
	data = NewDataset(nil)
	data.Sp.SetConfig(s.properties)

	for i, row0 := range allRows[:len(allRows)-trailingBlankRows] {
		if i == 0 {
			// assume first row is header row
			row0 = CleanHeaderRow(row0)
			data.SetFields(row0)
			continue
		}

		row := make([]interface{}, len(row0))
		for i, val := range row0 {
			row[i] = data.Sp.CastVal(i, val, data.Columns[i].Type)
		}
		data.Rows = append(data.Rows, row)

		if i == sampleSize {
			data.InferColumnTypes()
			for _, row := range data.Rows {
				row = data.Sp.CastRow(row, data.Columns)
			}
		}
	}
	if !data.Inferred {
		data.InferColumnTypes()
		for _, row := range data.Rows {
			row = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}
func (s *spreadsheet) makeDatasetStr(rangeRows [][]string) (data Dataset) {
	data = NewDataset(nil)
	data.Sp.SetConfig(s.properties)
	for i, row0 := range rangeRows {
		if i == 0 {
			// assume first row is header row
			row0 = CleanHeaderRow(row0)
			data.SetFields(row0)
			continue
		}

		row := make([]interface{}, len(row0))
		for i, val := range row0 {
			row[i] = data.Sp.CastVal(i, val, data.Columns[i].Type)
		}
		data.Append(row)

		if i == sampleSize {
			data.InferColumnTypes()
			for _, row := range data.Rows {
				row = data.Sp.CastRow(row, data.Columns)
			}
		}
	}

	if !data.Inferred {
		data.InferColumnTypes()
		for _, row := range data.Rows {
			row = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}

func (s *spreadsheet) makeDatasetInterf(rangeRows [][]interface{}) (data Dataset) {
	data = NewDataset(nil)
	data.Sp.SetConfig(s.properties)
	for i, row := range rangeRows {
		if i == 0 {
			// assume first row is header row
			row0 := make([]string, len(row))
			for i, val := range row {
				row0[i] = cast.ToString(val)
			}
			data.SetFields(CleanHeaderRow(row0))
			continue
		}

		row = data.Sp.CastRow(row, data.Columns)
		data.Append(row)

		if i == sampleSize {
			data.InferColumnTypes()
			for _, row := range data.Rows {
				row = data.Sp.CastRow(row, data.Columns)
			}
		}
	}

	if !data.Inferred {
		data.InferColumnTypes()
		for _, row := range data.Rows {
			row = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}

// Excel represent an Excel object pointing to its file
type Excel struct {
	spreadsheet
	File       *excelize.File
	Sheets     []string
	Path       string
	context    h.Context
	sheetIndex map[string]int
}

// GoogleSheet represent a Google Sheet object
type GoogleSheet struct {
	spreadsheet
	Sheets        []string
	SpreadsheetID string
	srv           *sheets.Service
	context       h.Context
	sheetObjects  map[string]*sheets.Sheet
}

// NewExcel creates a new excel file
func NewExcel() (xls *Excel) {

	xls = &Excel{
		File:    excelize.NewFile(),
		context: h.NewContext(context.Background()),
	}
	xls.spreadsheet.properties = map[string]string{}

	return
}

// NewExcelFromFile return a new Excel instance from a local file
func NewExcelFromFile(path string) (xls *Excel, err error) {
	f, err := excelize.OpenFile(path)
	if err != nil {
		err = h.Error(err, "Unable to open file: "+path)
		return
	}

	xls = &Excel{
		File:    f,
		Path:    path,
		context: h.NewContext(context.Background()),
	}
	xls.spreadsheet.properties = map[string]string{}

	xls.RefreshSheets()

	return
}

// NewExcelFromReader return a new Excel instance from a reader
func NewExcelFromReader(reader io.Reader) (xls *Excel, err error) {
	f, err := excelize.OpenReader(reader)
	if err != nil {
		err = h.Error(err, "Unable to open reader")
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
		context: h.NewContext(context.Background()),
	}
	xls.spreadsheet.properties = map[string]string{}

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
		err = h.Error(err, "Invalid range: "+cellRange)
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
		err = h.Error(
			err,
			"Input row range is larger than file row range: %d < %d",
			len(allRows), rowEnd,
		)
		return
	} else if len(allRows[0]) < colEnd {
		err = h.Error(
			err,
			"Input col range is larger than file col range: %d < %d",
			len(allRows[0]), colEnd,
		)
		return
	}

	i := 0
	rangeRows := make([][]string, rowEnd-rowStart+1)
	for r := rowStart; r <= rowEnd; r++ {
		row0 := []string{}
		for c := colStart; c <= colEnd; c++ {
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
		return h.Error(err, "could not open %s for writing", path)
	}
	_, err = xls.File.WriteTo(file)
	if err != nil {
		return h.Error(err, "could not write to %s", path)
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
			newShtName = h.F("%s%d", shtName, i)
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
	cellRange := h.F("%s%d", col, i)
	if mode != "append" {
		header := []interface{}{}
		for _, field := range ds.GetFields() {
			header = append(header, field)
		}
		xls.File.SetSheetRow(shtName, cellRange, &header)
		i++
	}

	for row := range ds.Rows {
		cellRange = h.F("%s%d", col, i)
		xls.File.SetSheetRow(shtName, cellRange, &row)
		i++
	}

	// for overwrite, blank out the remaining rows
	// TODO: what about the remaining columns (on the right)
	if mode == "overwrite" && i < len(rows)+1 {
		for j := i; j < len(rows)+1; j++ {
			row := make([]interface{}, len(ds.Columns))
			cellRange = h.F("%s%d", col, j)
			xls.File.SetSheetRow(shtName, cellRange, &row)
		}
	}

	xls.RefreshSheets()

	return
}

// NewGoogleSheet is a blank spreadsheet
// title is the new spreadsheet title
func NewGoogleSheet(props ...string) (ggs *GoogleSheet, err error) {

	ggs = &GoogleSheet{
		context:      h.NewContext(context.Background()),
		sheetObjects: map[string]*sheets.Sheet{},
	}

	ggs.spreadsheet.properties = map[string]string{}

	for k, v := range h.KVArrToMap(props...) {
		ggs.properties[k] = v
	}

	if ggs.properties["GSHEETS_CRED_FILE"] == "" {
		ggs.properties["GSHEETS_CRED_FILE"] = os.Getenv("GSHEETS_CRED_FILE")
	}
	if ggs.properties["GSHEETS_API_KEY"] == "" {
		ggs.properties["GSHEETS_API_KEY"] = os.Getenv("GSHEETS_API_KEY")
	}
	if ggs.properties["GSHEETS_TITLE"] == "" {
		ggs.properties["GSHEETS_TITLE"] = os.Getenv("GSHEETS_TITLE")
	}
	ggs.properties["GOOGLE_USER"] = os.Getenv("GOOGLE_USER")
	ggs.properties["GOOGLE_PASSWORD"] = os.Getenv("GOOGLE_PASSWORD")

	// https://developers.google.com/sheets/api/quickstart/go
	b, err := ioutil.ReadFile(ggs.properties["GSHEETS_CRED_FILE"])
	if err != nil {
		err = h.Error(err, "Unable to read client secret file: "+ggs.properties["GSHEETS_CRED_FILE"])
		return
	}

	scope := "https://www.googleapis.com/auth/spreadsheets"
	// scope = "https://www.googleapis.com/auth/spreadsheets.readonly"
	config, err := google.ConfigFromJSON(b, scope)
	if err != nil {
		err = h.Error(err, "Unable to parse client secret file to config: "+ggs.properties["GSHEETS_CRED_FILE"])
		return
	}
	// client := ggs.getGoogleClient(config)
	// if ggs.context.Err() != nil {
	// 	err = h.Error(ggs.context.Err(), "unable to create google client")
	// 	return
	// }

	// srv, err := sheets.New(client)
	// if err != nil {
	// 	err = h.Error(err, "Unable to retrieve Sheets client")
	// 	return
	// }

	// ggs.srv = srv

	// https://developers.google.com/sheets/api/quickstart/js

	token, err := ggs.getToken(config)
	if err != nil {
		err = h.Error(err, "Unable to obtain token")
		return
	}

	_ = option.WithTokenSource(config.TokenSource(ggs.context.Ctx, token))

	srv, err := sheets.NewService(
		ggs.context.Ctx,
		// option.WithAPIKey(ggs.properties["GSHEETS_API_KEY"]),
		option.WithScopes(sheets.SpreadsheetsScope),
		// option.WithScopes(sheets.SpreadsheetsReadonlyScope),
		option.WithTokenSource(config.TokenSource(ggs.context.Ctx, token)),
	)
	if err != nil {
		err = h.Error(err, "Unable to retrieve Sheets client")
		return
	}

	ggs.srv = srv

	return
}

// NewGoogleSheetFromURL return a new GoogleSheet instance from a provided url
func NewGoogleSheetFromURL(urlStr string, props ...string) (ggs *GoogleSheet, err error) {

	ggs, err = NewGoogleSheet(props...)
	if err != nil {
		err = h.Error(err, "Unable to initialize google sheets")
		return
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		err = h.Error(err, "could not parse google sheets url")
		return
	}

	pathArr := strings.Split(u.Path, "/")
	if len(pathArr) < 4 || strings.ToLower(pathArr[1]) != "spreadsheets" {
		err = fmt.Errorf("invalid google sheets url")
		err = h.Error(err, "invalid google sheets url")
		return
	}

	ggs.SpreadsheetID = pathArr[3]

	// get sheets
	err = ggs.RefreshSheets()
	if err != nil {
		err = h.Error(err, "Unable to load sheet properties")
		return
	}

	return
}

// RefreshSheets refreshes sheets data
func (ggs *GoogleSheet) RefreshSheets() (err error) {
	// https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#resource-spreadsheet
	// this loads all sheets data into memory. `IncludeGridData` doesn't work...
	h.Trace("refreshing sheets data")
	resp, err := ggs.srv.Spreadsheets.Get(ggs.SpreadsheetID).Fields("sheets").IncludeGridData(false).Do()
	if err != nil {
		err = h.Error(err, "Unable to get sheets from "+ggs.SpreadsheetID)
		return
	}
	for _, sheet := range resp.Sheets {
		// https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#sheetproperties
		// h.P(sheet.Data[0].MarshalJSON())
		// d, _ := sheet.Data[0].MarshalJSON()
		// h.P(len(sheet.Data[0].RowData))
		// h.P(sheet.Data[0].RowData[0])
		// h.P(len(sheet.Data[0].ColumnMetadata))
		ggs.Sheets = append(ggs.Sheets, sheet.Properties.Title)
		ggs.sheetObjects[sheet.Properties.Title] = sheet
	}
	return
}

// getRawRows will discard trailing empty rows (empty rows at end of data range)
// empty rows in the "middle" of the datarange will be left intact
func (ggs *GoogleSheet) getRawRows(sheet *sheets.Sheet) [][]string {
	var blankCellCnt, trailingBlankRows int
	rawRows := [][]string{}
	for _, rowData := range sheet.Data[0].RowData {
		blankCellCnt = 0
		row := make([]string, len(rowData.Values))
		for j, cell := range rowData.Values {
			row[j] = cell.FormattedValue
			if row[j] == "" {
				blankCellCnt++
			}
		}
		rawRows = append(rawRows, row)
		if blankCellCnt == len(row) {
			trailingBlankRows++
		} else {
			trailingBlankRows = 0
		}
	}
	return rawRows[:len(rawRows)-trailingBlankRows]
}

// GetDataset returns a dataset of the sheet
func (ggs *GoogleSheet) GetDataset(shtName string) (data Dataset, err error) {
	sheet, ok := ggs.sheetObjects[shtName]
	if !ok {
		err = h.Error("sheet %s not found", shtName)
		return
	}

	if len(sheet.Data) == 0 {
		err = h.Error("no data found for sheet %s", shtName)
		return
	}

	data = ggs.makeDatasetAuto(ggs.getRawRows(sheet))

	return
}

// GetDatasetFromRange returns a dataset from the specified range
func (ggs *GoogleSheet) GetDatasetFromRange(shtName, cellRange string) (data Dataset, err error) {
	if ggs == nil {
		return
	}

	readRange := h.F("%s!%s", shtName, cellRange)
	resp, err := ggs.srv.Spreadsheets.Values.Get(ggs.SpreadsheetID, readRange).Do()
	if err != nil {
		err = h.Error(err, "Unable to retrieve data from "+readRange)
		return
	}

	if len(resp.Values) == 0 {
		h.Warn("No data found for " + readRange)
		return
	}

	data = ggs.makeDatasetInterf(resp.Values)

	return
}

func (ggs *GoogleSheet) deleteSheet(shtName string) (err error) {

	// DeleteSheetRequest

	// create sheet
	batchUpdate := sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{&sheets.Request{
			DeleteSheet: &sheets.DeleteSheetRequest{
				SheetId: ggs.sheetObjects[shtName].Properties.SheetId,
			},
		}},
	}
	_, err = ggs.srv.Spreadsheets.BatchUpdate(ggs.SpreadsheetID, &batchUpdate).Do()

	if err != nil {
		return h.Error(err, "could not delete sheet: "+shtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return h.Error(err, "could not load sheets")
	}
	return
}

func (ggs *GoogleSheet) updateSheet(shtName string, properties *sheets.SheetProperties) (err error) {

	// update sheet
	batchUpdate := sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{&sheets.Request{
			UpdateSheetProperties: &sheets.UpdateSheetPropertiesRequest{
				Fields:     "title",
				Properties: properties,
			},
		}},
	}
	_, err = ggs.srv.Spreadsheets.BatchUpdate(ggs.SpreadsheetID, &batchUpdate).Do()

	if err != nil {
		return h.Error(err, "could not update sheet: "+shtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return h.Error(err, "could not load sheets")
	}
	return
}

func (ggs *GoogleSheet) createSheet(shtName string) (newShtName string, err error) {
	// ensure not duplicate sheet names
	newShtName = shtName
	{
		ok := true
		i := 1
		for {
			_, ok = ggs.sheetObjects[newShtName]
			if !ok {
				break
			}
			newShtName = h.F("%s%d", shtName, i)
			i++
		}
	}

	// create sheet
	batchUpdate := sheets.BatchUpdateSpreadsheetRequest{
		Requests: []*sheets.Request{&sheets.Request{
			AddSheet: &sheets.AddSheetRequest{
				Properties: &sheets.SheetProperties{
					Title: newShtName,
				},
			},
		}},
	}

	_, err = ggs.srv.Spreadsheets.BatchUpdate(ggs.SpreadsheetID, &batchUpdate).Do()

	if err != nil {
		return "", h.Error(err, "could not create new sheet: "+newShtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return newShtName, h.Error(err, "could not load sheets")
	}

	return
}

func (ggs *GoogleSheet) URL() string {
	return "https://docs.google.com/spreadsheets/d/" + ggs.SpreadsheetID
}
func (ggs *GoogleSheet) createSpreadsheet() (spreadsheetID string, err error) {
	rb := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title: ggs.properties["GSHEETS_TITLE"],
		},
	}
	resp, err := ggs.srv.Spreadsheets.Create(rb).Do()
	if err != nil {
		err = h.Error(err, "could not create new spreadsheet")
		return
	}
	ggs.SpreadsheetID = resp.SpreadsheetId
	ggs.RefreshSheets()
	return ggs.SpreadsheetID, nil
}

// WriteSheet write a datastream into a sheet
// mode can be: `new`, `append` or `overwrite`. Default is `new`
func (ggs *GoogleSheet) WriteSheet(shtName string, ds *Datastream, mode string) (err error) {

	if ggs.SpreadsheetID == "" {
		// create the spreadsheet
		_, err = ggs.createSpreadsheet()
		if err != nil {
			err = h.Error(err, "could not create new spreadsheet")
			return
		}
	}

	cellRange := "A1"
	if mode == "" || mode == "new" {
		// create sheet
		shtName, err = ggs.createSheet(shtName)
		if err != nil {
			return h.Error(err, "could not create new sheet: "+shtName)
		}
	}

	rows := [][]string{}
	sheet, shtExists := ggs.sheetObjects[shtName]
	if shtExists {
		rows = ggs.getRawRows(sheet)
	} else {
		shtName, err = ggs.createSheet(shtName)
		if err != nil {
			return h.Error(err, "could not create new sheet: "+shtName)
		}
	}

	if mode == "append" {
		rowCnt := len(rows)
		// colCnt := 0
		// if len(rows) > 0 {
		// 	colCnt = len(rows[0])
		// }
		cellRange = h.F("A%d", rowCnt+1)
	}

	data, err := ds.Collect(0)
	if err != nil {
		return h.Error(err, "could not collect stream")
	}

	outRows := data.Rows
	if mode != "append" {
		header := []interface{}{}
		for _, field := range data.GetFields() {
			header = append(header, field)
		}
		outRows = append([][]interface{}{header}, outRows...)
	}

	// h.Debug("cellRange: %s, outRows: %d, rows: %d", cellRange, len(outRows), len(rows))

	// for overwrite, blank out the remaining rows
	// TODO: what about the remaining columns (on the right)
	if mode == "overwrite" && len(outRows) < len(rows)+1 {
		for j := len(outRows); j <= len(rows); j++ {
			row := make([]interface{}, len(ds.Columns))
			for i := range row {
				row[i] = ""
			}
			outRows = append(outRows, row)
		}
	}

	shtRange := h.F("%s!%s", shtName, cellRange)
	rangeVals := sheets.ValueRange{
		Range:  shtRange,
		Values: outRows,
	}

	_, err = ggs.srv.Spreadsheets.Values.Update(ggs.SpreadsheetID, shtRange, &rangeVals).ValueInputOption("RAW").Do()
	if err != nil {
		return h.Error(err, "could not update sheet "+shtRange)
	}

	// if mode == "overwrite" {
	// 	// sheet is already created
	// 	if orgShtName != shtName {
	// 		// delete original sheet
	// 		err = ggs.deleteSheet(orgShtName)
	// 		if err != nil {
	// 			return h.Error(err, "could not delete sheet %s to overwrite. Remnant sheet %s !", orgShtName, shtName)
	// 		}

	// 		// rename new one to original
	// 		properties := ggs.sheetObjects[shtName].Properties
	// 		properties.Title = orgShtName
	// 		err = ggs.updateSheet(shtName, properties)
	// 		if err != nil {
	// 			return h.Error(err, "could not rename sheet %s to %s", shtName, orgShtName)
	// 		}
	// 	}
	// }

	err = ggs.RefreshSheets()
	if err != nil {
		err = nil
		h.LogError(err, "could not refresh sheets data")
	}

	return
}

// Retrieve a token, saves the token, then returns the generated client.
func (ggs *GoogleSheet) getGoogleClient(config *oauth2.Config) *http.Client {
	// The file token.json stores the user's access and refresh tokens, and is
	// created automatically when the authorization flow completes for the first
	// time.
	if ggs.properties["GOOGLE_USER"] != "" && ggs.properties["GOOGLE_PASSWORD"] != "" {
		tok, err := config.PasswordCredentialsToken(
			ggs.context.Ctx,
			ggs.properties["GOOGLE_USER"],
			ggs.properties["GOOGLE_PASSWORD"],
		)
		if err == nil {
			return config.Client(ggs.context.Ctx, tok)
		}
		// h.LogError(err, "unable to login using user/password (%s)", ggs.properties["GOOGLE_USER"])
	}

	tok, _ := ggs.getToken(config)
	return config.Client(ggs.context.Ctx, tok)
}

func (ggs *GoogleSheet) getToken(config *oauth2.Config) (*oauth2.Token, error) {

	tokFile := "/tmp/token.json"
	tok, err := ggs.tokenFromFile(tokFile)
	if err != nil {
		tok = ggs.getTokenFromWeb(config)
		if ggs.context.Err() == nil {
			ggs.saveToken(tokFile, tok)
		}
	}
	return tok, nil
}

// Retrieves a token from a local file.
func (ggs *GoogleSheet) tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func (ggs *GoogleSheet) saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		ggs.context.CaptureErr(err, "Unable to cache oauth token")
		return
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

// Request a token from the web, then returns the retrieved token.
// this is for CLI mode. For web server, there is web sever mode
// with callback
// see https://developers.google.com/sheets/api/quickstart/go
func (ggs *GoogleSheet) getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the authorization code: \n%s\n\nEnter Code:\n", authURL)

	var authCode string
	if _, err := fmt.Scanln(&authCode); err != nil {
		ggs.context.CaptureErr(err, "Unable to read authorization code")
		return nil
	}

	tok, err := config.Exchange(ggs.context.Ctx, authCode)
	if err != nil {
		ggs.context.CaptureErr(err, "Unable to retrieve token from web")
		return nil
	}
	return tok
}
