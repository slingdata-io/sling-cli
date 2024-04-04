package iop

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/json"
	"github.com/samber/lo"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// GoogleSheet represent a Google Sheet object
type GoogleSheet struct {
	spreadsheet
	Sheets        []string
	SpreadsheetID string
	srv           *sheets.Service
	context       g.Context
	sheetObjects  map[string]*sheets.Sheet
}

// NewGoogleSheet is a blank spreadsheet
// title is the new spreadsheet title
func NewGoogleSheet(props ...string) (ggs *GoogleSheet, err error) {

	ggs = &GoogleSheet{
		context:      g.NewContext(context.Background()),
		sheetObjects: map[string]*sheets.Sheet{},
	}

	ggs.spreadsheet.Props = map[string]string{}

	for k, v := range g.KVArrToMap(props...) {
		ggs.Props[k] = v
	}

	if _, ok := ggs.Props["KEY_BODY"]; !ok {
		return nil, g.Error("missing google credentials")
	}

	scope := "https://www.googleapis.com/auth/spreadsheets"
	// scope = "https://www.googleapis.com/auth/spreadsheets.readonly"
	config, err := google.ConfigFromJSON([]byte(ggs.Props["KEY_BODY"]), scope)
	if err != nil {
		err = g.Error(err, "Unable to parse client secret file to config")
		return
	}

	// client := ggs.getGoogleClient(config)
	// if ggs.context.Err() != nil {
	// 	err = g.Error(ggs.context.Err(), "unable to create google client")
	// 	return
	// }

	// srv, err := sheets.New(client)
	// if err != nil {
	// 	err = g.Error(err, "Unable to retrieve Sheets client")
	// 	return
	// }

	// ggs.srv = srv

	// https://developers.google.com/sheets/api/quickstart/js

	token, err := ggs.getToken(config)
	if err != nil {
		err = g.Error(err, "Unable to obtain token")
		return
	}

	_ = option.WithTokenSource(config.TokenSource(ggs.context.Ctx, token))

	srv, err := sheets.NewService(
		ggs.context.Ctx,
		// option.WithAPIKey(ggs.Props["API_KEY"]),
		option.WithScopes(sheets.SpreadsheetsScope),
		// option.WithScopes(sheets.SpreadsheetsReadonlyScope),
		option.WithTokenSource(config.TokenSource(ggs.context.Ctx, token)),
	)
	if err != nil {
		err = g.Error(err, "Unable to retrieve Sheets client")
		return
	}

	ggs.srv = srv

	return
}

// NewGoogleSheetFromURL return a new GoogleSheet instance from a provided url
func NewGoogleSheetFromURL(urlStr string, props ...string) (ggs *GoogleSheet, err error) {

	ggs, err = NewGoogleSheet(props...)
	if err != nil {
		err = g.Error(err, "Unable to initialize google sheets")
		return
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		err = g.Error(err, "could not parse google sheets url")
		return
	}

	pathArr := strings.Split(u.Path, "/")
	if len(pathArr) < 4 || strings.ToLower(pathArr[1]) != "spreadsheets" {
		err = g.Error("invalid google sheets url")
		err = g.Error(err, "invalid google sheets url")
		return
	}

	ggs.SpreadsheetID = pathArr[3]

	// get sheets
	err = ggs.RefreshSheets()
	if err != nil {
		err = g.Error(err, "Unable to load sheet Props")
		return
	}

	return
}

// RefreshSheets refreshes sheets data
func (ggs *GoogleSheet) RefreshSheets() (err error) {
	// https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#resource-spreadsheet
	// this loads all sheets data into memory. `IncludeGridData` doesn't work...
	g.Trace("refreshing sheets data")
	resp, err := ggs.srv.Spreadsheets.Get(ggs.SpreadsheetID).Fields("sheets").IncludeGridData(false).Do()
	if err != nil {
		err = g.Error(err, "Unable to get sheets from "+ggs.SpreadsheetID)
		return
	} else if len(resp.Sheets) == 0 {
		return g.Error("zero sheets returned")
	}

	for _, sheet := range resp.Sheets {
		// https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/sheets#sheetproperties
		// g.P(sheet.Data[0].MarshalJSON())
		// d, _ := sheet.Data[0].MarshalJSON()
		// g.P(len(sheet.Data[0].RowData))
		// g.P(sheet.Data[0].RowData[0])
		// g.P(len(sheet.Data[0].ColumnMetadata))
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
	if parts := strings.Split(shtName, "!"); len(parts) == 2 {
		return ggs.GetDatasetFromRange(parts[0], parts[1])
	} else if shtName == "" {
		shtName = ggs.Sheets[0]
	}

	sheet, ok := ggs.sheetObjects[shtName]
	if !ok {
		err = g.Error("sheet %s not found", shtName)
		return
	}

	if len(sheet.Data) == 0 {
		err = g.Error("no data found for sheet %s", shtName)
		return
	}

	g.Debug("reading sheet: %s", shtName)
	data = ggs.makeDatasetAuto(ggs.getRawRows(sheet))

	return
}

// GetDatasetFromRange returns a dataset from the specified range
func (ggs *GoogleSheet) GetDatasetFromRange(shtName, cellRange string) (data Dataset, err error) {
	if ggs == nil {
		return
	}

	readRange := g.F("%s!%s", shtName, cellRange)
	g.Debug("reading sheet: %s", readRange)
	resp, err := ggs.srv.Spreadsheets.Values.Get(ggs.SpreadsheetID, readRange).Do()
	if err != nil {
		err = g.Error(err, "Unable to retrieve data from "+readRange)
		return
	}

	if len(resp.Values) == 0 {
		g.Warn("No data found for " + readRange)
		return
	}

	data = ggs.makeDatasetInterf(resp.Values)

	return
}

func (ggs *GoogleSheet) DeleteSheet(shtName string) (err error) {

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
		return g.Error(err, "could not delete sheet: "+shtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return g.Error(err, "could not load sheets")
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
		return g.Error(err, "could not update sheet: "+shtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return g.Error(err, "could not load sheets")
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
			newShtName = g.F("%s%d", shtName, i)
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
		return "", g.Error(err, "could not create new sheet: "+newShtName)
	}

	// reload sheets
	err = ggs.RefreshSheets()
	if err != nil {
		return newShtName, g.Error(err, "could not load sheets")
	}

	return
}

func (ggs *GoogleSheet) URL() string {
	return "https://docs.google.com/spreadsheets/d/" + ggs.SpreadsheetID
}

func (ggs *GoogleSheet) createSpreadsheet() (spreadsheetID string, err error) {
	title := lo.Ternary(ggs.Props["SHEET"] != "", ggs.Props["SHEET"], "Sheet1")
	rb := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title: title,
		},
	}
	resp, err := ggs.srv.Spreadsheets.Create(rb).Do()
	if err != nil {
		err = g.Error(err, "could not create new spreadsheet")
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
			err = g.Error(err, "could not create new spreadsheet")
			return
		}
	}

	cellRange := "A1"
	if mode == "" || mode == "new" {
		// create sheet
		shtName, err = ggs.createSheet(shtName)
		if err != nil {
			return g.Error(err, "could not create new sheet: "+shtName)
		}
	}

	rows := [][]string{}
	sheet, shtExists := ggs.sheetObjects[shtName]
	if shtExists {
		rows = ggs.getRawRows(sheet)
	} else {
		shtName, err = ggs.createSheet(shtName)
		if err != nil {
			return g.Error(err, "could not create new sheet: "+shtName)
		}
	}

	if mode == "append" {
		rowCnt := len(rows)
		// colCnt := 0
		// if len(rows) > 0 {
		// 	colCnt = len(rows[0])
		// }
		cellRange = g.F("A%d", rowCnt+1)
	}

	data, err := ds.Collect(0)
	if err != nil {
		return g.Error(err, "could not collect stream")
	}

	outRows := data.Rows
	if mode != "append" {
		header := []interface{}{}
		for _, field := range data.GetFields() {
			header = append(header, field)
		}
		outRows = append([][]interface{}{header}, outRows...)
	}

	// g.Debug("cellRange: %s, outRows: %d, rows: %d", cellRange, len(outRows), len(rows))

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

	shtRange := g.F("%s!%s", shtName, cellRange)
	rangeVals := sheets.ValueRange{
		Range:  shtRange,
		Values: outRows,
	}

	_, err = ggs.srv.Spreadsheets.Values.Update(ggs.SpreadsheetID, shtRange, &rangeVals).ValueInputOption("RAW").Do()
	if err != nil {
		return g.Error(err, "could not update sheet "+shtRange)
	}

	// if mode == "overwrite" {
	// 	// sheet is already created
	// 	if orgShtName != shtName {
	// 		// delete original sheet
	// 		err = ggs.deleteSheet(orgShtName)
	// 		if err != nil {
	// 			return g.Error(err, "could not delete sheet %s to overwrite. Remnant sheet %s !", orgShtName, shtName)
	// 		}

	// 		// rename new one to original
	// 		Props := ggs.sheetObjects[shtName].Properties
	// 		Props.Title = orgShtName
	// 		err = ggs.updateSheet(shtName, Props)
	// 		if err != nil {
	// 			return g.Error(err, "could not rename sheet %s to %s", shtName, orgShtName)
	// 		}
	// 	}
	// }

	err = ggs.RefreshSheets()
	if err != nil {
		g.LogError(err, "could not refresh sheets data")
		err = nil
	}

	return
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
