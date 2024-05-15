package filesys

import (
	"bufio"
	"os"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestExcel(t *testing.T) {
	t.Parallel()
	_, err := iop.NewExcelFromFile("test/test.excel2.xlsx")
	assert.NoError(t, err)

	file, err := os.Open("test/test.excel2.xlsx")
	assert.NoError(t, err)
	xls, err := iop.NewExcelFromReader(bufio.NewReader(file))
	if assert.NoError(t, err) {
		return
	}

	data := xls.GetDataset(xls.Sheets[0])
	assert.Equal(t, 31, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))
	assert.EqualValues(t, 79000, data.Records()[0]["sale_amount"])

	data, err = xls.GetDatasetFromRange(xls.Sheets[0], "A:B")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))

	data, err = xls.GetDatasetFromRange(xls.Sheets[0], "$A$1:$H$29")
	assert.NoError(t, err)

	assert.Equal(t, 8, len(data.Columns))
	assert.Equal(t, 28, len(data.Rows))
	assert.Equal(t, "3387  AVALON RD", data.Records()[0]["property_address"])

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df, err := localFs.ReadDataflow("test/test.excel2.xlsx")
	assert.NoError(t, err)

	data, err = iop.MergeDataflow(df).Collect(0)
	assert.NoError(t, err)
	data0 := data

	assert.Equal(t, 1317, len(data.Rows))

	err = xls.WriteSheet("new", data.Stream(), "new")
	assert.NoError(t, err)
	err = xls.WriteSheet("new", data.Stream(), "append")
	assert.NoError(t, err)

	data = xls.GetDataset("new")
	assert.Equal(t, 2634, len(data.Rows))

	err = xls.WriteSheet("new", data0.Stream(), "overwrite")
	assert.NoError(t, err)

	data = xls.GetDataset("new")
	if !assert.Equal(t, 1317, len(data.Rows)) {
		return
	}

	err = xls.WriteToFile("test/test.excel5.xlsx")
	assert.NoError(t, err)

	xls2, err := iop.NewExcelFromFile("test/test.excel5.xlsx")
	assert.NoError(t, err)

	data, err = xls2.GetDatasetFromRange(xls2.Sheets[0], "A:B")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))

	os.RemoveAll("test/test.excel5.xlsx")

	df, err = iop.MakeDataFlow(data.Stream())
	assert.NoError(t, err)
	localFs.SetProp("COMPRESSION", "NONE")
	_, err = WriteDataflow(localFs, df, "test/test.excel6.xlsx")
	assert.NoError(t, err)

	xls3, err := iop.NewExcelFromFile("test/test.excel6.xlsx")
	assert.NoError(t, err)

	data = xls3.GetDataset(xls3.Sheets[0])
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))

	os.RemoveAll("test/test.excel6.xlsx")
}

func TestGoogleSheet(t *testing.T) {

	url := "https://docs.google.com/spreadsheets/d/1Wo7d_2oiYpWy1hYGqHIy0DSPWki24Xif3FnlRjNGzo4/edit#gid=0"
	ggs, err := iop.NewGoogleSheetFromURL(
		url, "GSHEETS_CRED_FILE="+os.Getenv("GSHEETS_CRED_FILE"),
	)
	if !assert.NoError(t, err) {
		return
	}
	assert.Greater(t, len(ggs.Sheets), 0)
	g.Debug("GetDatasetFromRange")
	data, err := ggs.GetDatasetFromRange("native_to_general", "A:B")
	assert.NoError(t, err)
	assert.EqualValues(t, len(data.Columns), 2)
	assert.Greater(t, len(data.Rows), 200)

	// ggs0, err := NewGoogleSheet("GSHEETS_CRED_FILE=" + os.Getenv("GSHEETS_CRED_FILE"))
	// if !assert.NoError(t, err) {
	// 	return
	// }
	// ggs0.Props["GSHEETS_TITLE"] = "sample title"
	// err = ggs0.WriteSheet("Sheet1", data.Stream(), "overwrite")
	// g.Debug(ggs0.URL())
	// assert.NoError(t, err)
	// return

	g.Debug("GetDataset")
	data0, err := ggs.GetDataset("native_to_general")
	assert.NoError(t, err)
	assert.EqualValues(t, len(data0.Columns), 7)
	assert.Greater(t, len(data0.Rows), 200)
	// g.P(len(data.Rows))

	err = ggs.WriteSheet("new", data0.Stream(), "new")
	assert.NoError(t, err)
	err = ggs.WriteSheet("new", data0.Stream(), "append")
	assert.NoError(t, err)

	g.Debug("GetDataset")
	data, err = ggs.GetDataset("new")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 400)

	err = ggs.WriteSheet("new", data0.Stream(), "overwrite")
	assert.NoError(t, err)

	g.Debug("GetDataset")
	data, err = ggs.GetDataset("new")
	assert.NoError(t, err)
	assert.Less(t, len(data.Rows), 400)

	err = ggs.DeleteSheet("new")
	assert.NoError(t, err)

	jsonBody, err := os.ReadFile(os.Getenv("GSHEETS_CRED_FILE"))
	assert.NoError(t, err)

	httpFs, err := NewFileSysClient(
		dbio.TypeFileHTTP,
		"GSHEET_CLIENT_JSON_BODY="+string(jsonBody),
		"GSHEET_SHEET_NAME=native_to_general",
	)
	assert.NoError(t, err)

	g.Debug("GetReader")
	df, err := httpFs.ReadDataflow(url)
	assert.NoError(t, err)

	data, err = iop.MergeDataflow(df).Collect(0)
	assert.NoError(t, err)
	assert.Greater(t, len(data0.Rows), 200)

	httpFs.SetProp("GSHEET_SHEET_NAME", "new")
	httpFs.SetProp("GSHEET_MODE", "new")
	_, err = httpFs.Write(url, data.Stream().NewCsvReader(0, 0))
	assert.NoError(t, err)

	ggs.RefreshSheets()
	err = ggs.DeleteSheet("new")
	assert.NoError(t, err)
}
