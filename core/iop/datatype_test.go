package iop

import (
	"bufio"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	h "github.com/flarco/gutil"
	"github.com/stretchr/testify/assert"
)

var (
	AWS_BUCKET = os.Getenv("AWS_BUCKET")
)

func TestS3(t *testing.T) {

	csvPath := "test/test1.1.csv"
	s3Path := "test/test1.1.csv"
	// s3PathPq := "test/test1.1.parquet"

	csv1 := CSV{Path: csvPath}

	s3 := S3{Bucket: AWS_BUCKET}

	err := s3.Delete("test")
	assert.NoError(t, err)
	// if err != nil {
	// 	return
	// }

	csvFile, err := os.Open(csvPath)
	assert.NoError(t, err)

	err = s3.Write(s3Path, csvFile)
	assert.NoError(t, err)

	reader, err := csv1.NewReader()
	assert.NoError(t, err)
	if err != nil {
		return
	}
	gzReader := Compress(reader)
	err = s3.Write(s3Path+".gz", gzReader)
	assert.NoError(t, err)

	s3Reader, err := s3.GetReader(s3Path)
	assert.NoError(t, err)

	csvFile.Seek(0, 0)
	csvReaderOut, err := ioutil.ReadAll(csvFile)
	s3ReaderOut, err := ioutil.ReadAll(s3Reader)
	csvFile.Close()
	assert.NoError(t, err)
	assert.Equal(t, string(csvReaderOut), string(s3ReaderOut))

	paths, err := s3.List("test/")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(paths))

	s3Reader, err = s3.GetReader(s3Path + ".gz")
	assert.NoError(t, err)

	gS3Reader, err := Decompress(s3Reader)
	assert.NoError(t, err)

	s3ReaderOut, err = ioutil.ReadAll(gS3Reader)
	assert.NoError(t, err)
	assert.Equal(t, string(csvReaderOut), strings.TrimSpace(string(s3ReaderOut)))

}

func bParseString(sp *StreamProcessor, val string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		sp.ParseString(val)
	}
}

// go test -run BenchmarkParseString -bench=.
// go test -benchmem -run='^$ github.com/slingdata/sling/core/iop' -bench '^BenchmarkParseString'
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
		// row = []interface{}{"5535.214140000"}
	} else if name == "int" {
		row = []interface{}{int(48714719874194)}
	} else if name == "int64" {
		row = []interface{}{int64(48714719874194)}
	} else if name == "string" {
		row = []interface{}{h.RandString(h.AplhanumericRunes, 1000)}
	} else if name == "timestamp" {
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
		fields[i] = h.F("col%d", i)
	}
	data.SetFields(fields)
	data.Append(row)
	data.InferColumnTypes()
	columns = data.Columns
	return
}

// go test -benchmem -run='^$ github.com/slingdata/sling/core/iop' -bench '^BenchmarkProcessRow'
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
			row[i] = sp.CastVal(i, val, columns[i].Type)
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

// go test -benchmem -run='^$ github.com/slingdata/sling/core/iop' -bench '^BenchmarkProcessVal'
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
	columns, row := initProcessRow("float")
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

// go test -benchmem -run='^$ github.com/slingdata/sling/core/iop' -bench '^BenchmarkCastToString'
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
func TestInterfVal(t *testing.T) {
	row := make([]interface{}, 3)
	h.P(row[0])
	row[0] = float64(0)
	h.P(row[0])
	row[0] = nil
	h.P(row[0])
}

func TestParseDate(t *testing.T) {
	sp := NewStreamProcessor()
	val := "17-OCT-20 07.01.59.000000 PM"
	h.P(sp.ParseString(val))
	val = "17-OCT-20"
	h.P(sp.ParseString(val))
}

func TestExcel(t *testing.T) {
	t.Parallel()
	xls, err := NewExcelFromFile("test/test.excel2.xlsx")
	assert.NoError(t, err)

	file, err := os.Open("test/test.excel2.xlsx")
	assert.NoError(t, err)
	xls, err = NewExcelFromReader(bufio.NewReader(file))
	assert.NoError(t, err)

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

	localFs, err := NewFileSysClient(LocalFileSys)
	assert.NoError(t, err)

	df, err := localFs.ReadDataflow("test/test.excel2.xlsx")
	assert.NoError(t, err)

	data, err = MergeDataflow(df).Collect(0)
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

	xls2, err := NewExcelFromFile("test/test.excel5.xlsx")
	assert.NoError(t, err)

	data, err = xls2.GetDatasetFromRange(xls2.Sheets[0], "A:B")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))

	os.RemoveAll("test/test.excel5.xlsx")

	df, err = MakeDataFlow(data.Stream())
	assert.NoError(t, err)
	localFs.SetProp("SLINGELT_COMPRESSION", "NONE")
	_, err = localFs.WriteDataflow(df, "test/test.excel6.xlsx")
	assert.NoError(t, err)

	xls3, err := NewExcelFromFile("test/test.excel6.xlsx")
	assert.NoError(t, err)

	data = xls3.GetDataset(xls3.Sheets[0])
	assert.NoError(t, err)
	assert.Equal(t, 2, len(data.Columns))
	assert.Equal(t, 1317, len(data.Rows))

	os.RemoveAll("test/test.excel6.xlsx")
}

func TestGoogleSheet(t *testing.T) {

	url := "https://docs.google.com/spreadsheets/d/1Wo7d_2oiYpWy1hYGqHIy0DSPWki24Xif3FnlRjNGzo4/edit#gid=0"
	ggs, err := NewGoogleSheetFromURL(
		url, "GSHEETS_CRED_FILE="+os.Getenv("GSHEETS_CRED_FILE"),
	)
	if !assert.NoError(t, err) {
		return
	}
	assert.Greater(t, len(ggs.Sheets), 0)
	h.Debug("GetDatasetFromRange")
	data, err := ggs.GetDatasetFromRange("native_to_general", "A:B")
	assert.NoError(t, err)
	assert.EqualValues(t, len(data.Columns), 2)
	assert.Greater(t, len(data.Rows), 200)

	// ggs0, err := NewGoogleSheet("GSHEETS_CRED_FILE=" + os.Getenv("GSHEETS_CRED_FILE"))
	// if !assert.NoError(t, err) {
	// 	return
	// }
	// ggs0.properties["GSHEETS_TITLE"] = "sample title"
	// err = ggs0.WriteSheet("Sheet1", data.Stream(), "overwrite")
	// h.Debug(ggs0.URL())
	// assert.NoError(t, err)
	// return

	h.Debug("GetDataset")
	data0, err := ggs.GetDataset("native_to_general")
	assert.NoError(t, err)
	assert.EqualValues(t, len(data0.Columns), 7)
	assert.Greater(t, len(data0.Rows), 200)
	// h.P(len(data.Rows))

	err = ggs.WriteSheet("new", data0.Stream(), "new")
	assert.NoError(t, err)
	err = ggs.WriteSheet("new", data0.Stream(), "append")
	assert.NoError(t, err)

	h.Debug("GetDataset")
	data, err = ggs.GetDataset("new")
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 400)

	err = ggs.WriteSheet("new", data0.Stream(), "overwrite")
	assert.NoError(t, err)

	h.Debug("GetDataset")
	data, err = ggs.GetDataset("new")
	assert.NoError(t, err)
	assert.Less(t, len(data.Rows), 400)

	err = ggs.deleteSheet("new")
	assert.NoError(t, err)

	jsonBody, err := ioutil.ReadFile(os.Getenv("GSHEETS_CRED_FILE"))
	assert.NoError(t, err)

	httpFs, err := NewFileSysClient(
		HTTPFileSys,
		"GSHEET_CLIENT_JSON_BODY="+string(jsonBody),
		"GSHEET_SHEET_NAME=native_to_general",
	)
	assert.NoError(t, err)

	h.Debug("GetReader")
	df, err := httpFs.ReadDataflow(url)
	assert.NoError(t, err)

	data, err = MergeDataflow(df).Collect(0)
	assert.NoError(t, err)
	assert.Greater(t, len(data0.Rows), 200)

	httpFs.SetProp("GSHEET_SHEET_NAME", "new")
	httpFs.SetProp("GSHEET_MODE", "new")
	_, err = httpFs.Write(url, data.Stream().NewCsvReader(0))
	assert.NoError(t, err)

	ggs.RefreshSheets()
	err = ggs.deleteSheet("new")
	assert.NoError(t, err)
}
