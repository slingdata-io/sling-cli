package filesys

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	arrowParquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/clbanning/mxj/v2"
	"github.com/flarco/g/net"
	"github.com/linkedin/goavro/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestFileSysLocalCsv(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List("./")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), "file://./fs_test.go")

	paths, err = fs.ListRecursive("test/test1/csv/*.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 2)
	assert.Contains(t, paths.URIs(), "file://test/test1/csv/test1.csv")

	paths, err = fs.ListRecursive("test/test1/csv/test?.?.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 1)
	assert.Contains(t, paths.URIs(), "file://test/test1/csv/test1.1.csv")

	// Test Delete, Write, Read
	testPath := "test/fs.test"
	testString := "abcde"
	Delete(fs, testPath)
	reader := strings.NewReader(testString)
	_, err = fs.Write(testPath, reader)
	assert.NoError(t, err)

	readers, err := fs.GetReaders(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(readers[0])
	assert.NoError(t, err)
	assert.Equal(t, testString, string(testBytes))

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive(".")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), "./"+testPath)

	// Test datastream
	fs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	fs.SetProp("header", "true")
	df, err := fs.ReadDataflow("test/test1/csv")
	g.AssertNoError(t, err)

	if t.Failed() {
		return
	}

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data.Rows))
	assert.NoError(t, df.Err())

	fs.SetProp("header", "false")
	df1, err := fs.ReadDataflow("test/test2/test2.1.noheader.csv")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 18, len(data1.Rows))

}

func TestFileSysLocalFormat(t *testing.T) {
	t.Parallel()
	iop.SampleSize = 4
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// clean up existing
	os.RemoveAll("test/test_write")

	columns := iop.NewColumns(
		iop.Columns{
			{Name: "col_big_int", Type: iop.BigIntType},
			{Name: "col_binary_type", Type: iop.BinaryType},
			{Name: "col_bool_type", Type: iop.BoolType},
			{Name: "col_date_type", Type: iop.DateType},
			{Name: "col_datetime_type", Type: iop.DatetimeType},
			{Name: "col_decimal_type", Type: iop.DecimalType},
			{Name: "col_integer_type", Type: iop.IntegerType},
			{Name: "col_json_type", Type: iop.JsonType},
			{Name: "col_small_int_type", Type: iop.SmallIntType},
			{Name: "col_string_type", Type: iop.StringType},
			{Name: "col_text_type", Type: iop.TextType},
			{Name: "col_timestamp_type", Type: iop.TimestampType},
			{Name: "col_timestampz_type", Type: iop.TimestampzType},
			{Name: "col_float_type", Type: iop.FloatType},
			{Name: "col_time_type", Type: iop.TimeType},
			{Name: "col_timez_type", Type: iop.TimezType},
		}...,
	)
	data := iop.NewDataset(columns)
	data.Inferred = true
	data.Append([]any{
		int64(9876554321),         // big_int_type
		[]byte("col_binary_type"), // binary_type
		true,                      // bool_type
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.Now().Location()), // date_type
		time.Date(2023, 2, 2, 2, 2, 2, 2, time.Now().Location()), // datetime_type
		123.45,           // decimal_type
		int64(898471313), // integer_type
		`{"message": "hello", "number": 123, "array": [1,2,3]}`, // json_type
		int32(1234),       // small_int_type
		"col_string_type", // string_type
		"col_text_type",   // text_type
		time.Now(),        // timestamp_type
		time.Date(2023, 3, 3, 3, 3, 3, 3, time.Now().Location()), // timestampz_type
		123.45,     // float_type
		"10:00:00", // time_type
		"10:00:00", // timez_type
	})

	for _, format := range []dbio.FileType{dbio.FileTypeJson, dbio.FileTypeJsonLines, dbio.FileTypeCsv, dbio.FileTypeParquet} {
		if t.Failed() {
			break
		}

		formatS := string(format)

		// file
		fs2, err := NewFileSysClient(dbio.TypeFileLocal, "FORMAT="+formatS)
		assert.NoError(t, err, formatS)
		fs.SetProp("header", "false")
		df2, _ := fs.ReadDataflow("test/test2/test2.1.noheader.csv")
		_, err = WriteDataflow(fs2, df2, g.F("test/test_write/%s.test", formatS))
		assert.NoError(t, err, formatS)
		df3, err := fs2.ReadDataflow(g.F("test/test_write/%s.test", formatS))
		g.LogError(err)
		assert.NoError(t, err, formatS)
		_, err = df3.Collect()
		assert.NoError(t, err, formatS)
		assert.Equal(t, cast.ToInt(df2.Count()), cast.ToInt(df3.Count()))

		// folder
		fs2, err = NewFileSysClient(dbio.TypeFileLocal, "FORMAT="+formatS, "FILE_MAX_ROWS=5")
		fs2.SetProp("header", "true")
		assert.NoError(t, err, formatS)
		fs.SetProp("header", "false")
		df2, _ = fs.ReadDataflow("test/test2/test2.1.noheader.csv")
		_, err = WriteDataflow(fs2, df2, g.F("test/test_write/%s.folder", formatS))
		assert.NoError(t, err, formatS)
		df3, err = fs2.ReadDataflow(g.F("test/test_write/%s.folder", formatS))
		assert.NoError(t, err, formatS)
		_, err = df3.Collect()
		assert.NoError(t, err, formatS)
		assert.Equal(t, cast.ToInt(df2.Count()), cast.ToInt(df3.Count()), formatS)

		// all data types
		fs3, err := NewFileSysClient(dbio.TypeFileLocal, "FORMAT="+formatS)
		assert.NoError(t, err, formatS)
		df4, err := iop.MakeDataFlow(data.Stream())
		assert.NoError(t, err, formatS)
		_, err = WriteDataflow(fs3, df4, g.F("test/test_write/%s.type.test", formatS))
		assert.NoError(t, err, formatS)
		df5, err := fs2.ReadDataflow(g.F("test/test_write/%s.type.test", formatS))
		assert.NoError(t, err, formatS)
		_, err = df5.Collect()
		assert.NoError(t, err, formatS)
		assert.Equal(t, len(data.Rows), cast.ToInt(df5.Count()))

	}

	if !t.Failed() {
		os.RemoveAll("test/test_write")
	}
}

func TestFileSysLocalJson(t *testing.T) {
	t.Parallel()
	iop.SampleSize = 4
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	fs.SetProp("flatten", "-1") // don't flatten at first
	df1, err := fs.ReadDataflow("test/test1/json")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	if assert.EqualValues(t, 2019, len(data1.Rows)) {
		assert.Equal(t, len(df1.Columns), len(data1.Rows[len(data1.Rows)-1]))
	}

	fs.SetProp("flatten", "0")
	df1, err = fs.ReadDataflow("test/test1/json")
	assert.NoError(t, err)

	data1, err = df1.Collect()
	assert.NoError(t, err)
	if assert.EqualValues(t, 2036, len(data1.Rows)) {
		lastRow := data1.Rows[len(data1.Rows)-1]
		assert.Equal(t, len(df1.Columns), len(lastRow), "cols: %s \n lastRow: %s", g.Marshal(df1.Columns.Names()), g.Marshal(lastRow))
	}

	fs.SetProp("flatten", "-1")
	df2, err := fs.ReadDataflow("test/test2/json")
	assert.NoError(t, err)

	data2, err := df2.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 20, len(data2.Rows))
	assert.EqualValues(t, 1, len(data2.Columns))

	fs.SetProp("flatten", "0")
	df2, err = fs.ReadDataflow("test/test2/json")
	assert.NoError(t, err)

	data2, err = df2.Collect()
	g.Debug("%#v", df2.Columns.Names())
	assert.NoError(t, err)
	assert.EqualValues(t, 20, len(data2.Rows))
	assert.GreaterOrEqual(t, 9, len(data2.Columns))

}

func TestFileSysLocalXml(t *testing.T) {

	fileBytes, err := os.ReadFile("test/test1/xml/test1.1.xml")
	assert.NoError(t, err)

	var payload any
	mv, err := mxj.NewMapXml(fileBytes)
	assert.NoError(t, err)
	payload = mv.Old()

	assert.NotNil(t, payload, "Payload should not be nil")
	g.Debug("payload: %s", g.Marshal(payload))

}

func TestFileSysLocalParquet(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data1.Rows))
	assert.EqualValues(t, 7, len(data1.Columns))
	// g.Info(g.Marshal(data1.Columns.Types()))

	df2, err := iop.MakeDataFlow(data1.Stream())
	assert.NoError(t, err)

	_, err = WriteDataflow(fs, df2, "file:///tmp/parquet.test.parquet")
	assert.NoError(t, err)

	// Verify column types in the new parquet file
	reader, err := os.Open("/tmp/parquet.test.parquet")
	assert.NoError(t, err)
	defer reader.Close()

	parquetReader, err := iop.NewParquetArrowReader(reader, nil)
	assert.NoError(t, err)

	columns := parquetReader.Columns()
	if !assert.Equal(t, 7, len(columns)) {
		return
	}

	expectedTypes := map[string]iop.ColumnType{
		"id":         iop.IntegerType,
		"first_name": iop.StringType,
		"last_name":  iop.StringType,
		"email":      iop.StringType,
		"rating":     iop.DecimalType,
		"date":       iop.DateType,
		"target":     iop.BoolType,
		"create_dt":  iop.DatetimeType,
	}

	for _, col := range columns {
		expectedType, exists := expectedTypes[col.Name]
		assert.True(t, exists, "Unexpected column: %s", col.Name)
		assert.Equal(t, expectedType, col.Type, "Mismatched type for column %s", col.Name)
	}

	// Verify data
	df3, err := fs.ReadDataflow("file:///tmp/parquet.test.parquet")
	assert.NoError(t, err)

	data3, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data3.Rows))
	assert.EqualValues(t, 7, len(data3.Columns))

}

func TestFileSysLocalIceberg(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Test reading Iceberg metadata
	path := "test/lineitem_iceberg"
	df, err := fs.ReadDataflow(path, iop.FileStreamConfig{Format: dbio.FileTypeIceberg})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 51793, len(data.Rows))

	expectedColumns := []struct {
		name     string
		dataType iop.ColumnType
	}{
		{"l_orderkey", iop.IntegerType},
		{"l_partkey", iop.IntegerType},
		{"l_suppkey", iop.IntegerType},
		{"l_linenumber", iop.IntegerType},
		{"l_quantity", iop.IntegerType},
		{"l_extendedprice", iop.DecimalType},
		{"l_discount", iop.DecimalType},
		{"l_tax", iop.DecimalType},
		{"l_returnflag", iop.TextType},
		{"l_linestatus", iop.TextType},
		{"l_shipdate", iop.DateType},
		{"l_commitdate", iop.DateType},
		{"l_receiptdate", iop.DateType},
		{"l_shipinstruct", iop.TextType},
		{"l_shipmode", iop.TextType},
		{"l_comment", iop.TextType},
	}

	if assert.Equal(t, len(expectedColumns), len(data.Columns), "Number of columns should match") {

		for i, expected := range expectedColumns {
			assert.Equal(t, expected.name, data.Columns[i].Name, "Column name should match")
			assert.Equal(t, expected.dataType, data.Columns[i].Type, "Column type should match for: %s", data.Columns[i].Name)
		}
	}
}

func TestFileSysLocalDelta(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Test reading Iceberg metadata
	path := "test/delta"
	df, err := fs.ReadDataflow(path, iop.FileStreamConfig{Format: dbio.FileTypeDelta})
	assert.NoError(t, err)

	data, err := df.Collect()
	assert.NoError(t, err)
	assert.Equal(t, 5, len(data.Rows))

	expectedColumns := []struct {
		name     string
		dataType iop.ColumnType
	}{
		{"first_name", iop.TextType},
		{"last_name", iop.TextType},
		{"country", iop.TextType},
		{"continent", iop.TextType},
	}

	if assert.Equal(t, len(expectedColumns), len(data.Columns), "Number of columns should match") {

		for i, expected := range expectedColumns {
			assert.Equal(t, expected.name, data.Columns[i].Name, "Column name should match")
			assert.Equal(t, expected.dataType, data.Columns[i].Type, "Column type should match for: %s", data.Columns[i].Name)
		}
	}
}

func TestFileSysLocalLargeParquet01(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("./test/dataset1M.csv")
	assert.NoError(t, err)

	filePath := "/tmp/test.1.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	config, err := parquet.NewWriterConfig()
	g.LogFatal(err)

	config.Schema = parquet.NewSchema("", iop.NewRecNode(df1.Columns, false))

	fw := parquet.NewWriter(f, config)

	g.Info("writing to %s with OldParquet", filePath)

	_ = arrowParquet.Types.FixedLenByteArray // to keep import
	numScale := iop.MakeDecNumScale(16)
	_ = numScale

	start := time.Now()
	for ds := range df1.StreamCh {
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				if batch.Count > 10000 {
					continue
				}
				rec := make([]parquet.Value, len(batch.Columns))

				for i, col := range batch.Columns {
					switch {
					case col.IsBool():
						row[i] = cast.ToBool(row[i]) // since is stored as string
					case col.IsDecimal():
						row[i] = cast.ToString(row[i])
						row[i] = iop.StringToDecimalByteArray(cast.ToString(row[i]), numScale, arrowParquet.Types.FixedLenByteArray, 16) // works for decimals with precision <= 16, very limited
					case col.IsDatetime() || col.IsDate():
						switch valT := row[i].(type) {
						case time.Time:
							if row[i] != nil {
								row[i] = valT.UnixNano()
							}
						}
					}
					if i < len(row) {
						rec[i] = parquet.ValueOf(row[i])
					}
				}

				_, err := fw.WriteRows([]parquet.Row{rec})
				g.LogFatal(err)
			}
		}
	}

	err = fw.Close()
	assert.NoError(t, err)

	duration := float64(int64(time.Since(start).Seconds()*100)) / 100
	g.Info("wrote %d in %f secs", df1.Count(), duration)
}

func TestFileSysLocalLargeParquet1(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("./test/dataset1M.csv")
	assert.NoError(t, err)

	filePath := "/tmp/test.1.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	pw, err := iop.NewParquetWriter(f, df1.Columns, &parquet.Snappy)
	g.LogFatal(err)

	g.Info("writing to %s with OldParquet", filePath)

	start := time.Now()
	for ds := range df1.StreamCh {
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				err = pw.WriteRow(row)
				g.LogFatal(err)
			}
		}
	}

	err = pw.Close()
	assert.NoError(t, err)

	duration := float64(int64(time.Since(start).Seconds()*100)) / 100
	g.Info("wrote %d in %f secs", df1.Count(), duration)
}

func TestFileSysLocalLargeParquet3(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("./test/dataset1M.csv")
	assert.NoError(t, err)

	filePath := "/tmp/test.3.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	pw, err := iop.NewParquetWriterMap(f, df1.Columns, &parquet.Snappy)
	g.LogFatal(err)

	g.Info("writing to %s with OldParquet", filePath)

	start := time.Now()
	for ds := range df1.StreamCh {
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				err = pw.WriteRec(row)
				g.LogFatal(err)
			}
		}
	}

	err = pw.Close()
	assert.NoError(t, err)

	duration := float64(int64(time.Since(start).Seconds()*100)) / 100
	g.Info("wrote %d in %f secs", df1.Count(), duration)
}

func BenchmarkFileSysLocalLargeParquet1(b *testing.B) {
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	g.LogError(err)

	df1, err := fs.ReadDataflow("./test/dataset1M.csv")
	g.LogError(err)

	filePath := "/tmp/test.1.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	pw, err := iop.NewParquetWriter(f, df1.Columns, &parquet.Snappy)
	g.LogFatal(err)

	g.Info("writing to %s with OldParquet", filePath)

	start := time.Now()
	var rows [][]any
	ds := <-df1.StreamCh
	batch := <-ds.BatchChan
	for row := range batch.Rows {
		if batch.Count > 100 {
			break
		}
		rows = append(rows, row)
	}

	for n := 0; n < b.N; n++ {
		for _, row := range rows {
			err = pw.WriteRow(row)
			g.LogFatal(err)
		}
	}

	err = pw.Close()
	g.LogError(err)

	duration := float64(int64(time.Since(start).Seconds()*100)) / 100
	g.Info("wrote %d in %f secs", df1.Count(), duration)
}

func TestFileSysLocalLargeParquet2(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("./test/dataset1M.csv")
	assert.NoError(t, err)

	filePath := "/tmp/test.2.parquet"
	f, err := os.Create(filePath)
	g.LogFatal(err)

	pw, err := iop.NewParquetArrowWriter(f, df1.Columns, compress.Codecs.Snappy)
	g.LogFatal(err)

	g.Info("writing to %s with NewParquetArrowWriter", filePath)

	start := time.Now()
	for ds := range df1.StreamCh {
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				err := pw.WriteRow(row)
				g.LogFatal(err)
			}
		}
	}

	err = pw.Close()
	assert.NoError(t, err)

	duration := float64(int64(time.Since(start).Seconds()*100)) / 100
	g.Info("wrote %d in %f secs", df1.Count(), duration)
}

func TestFileSysLocalSAS(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Dataset_Documentation/NHAMCS/sas/ed2021_sas.zip
	df1, err := fs.ReadDataflow("test/test1/sas7bdat/ed2021_sas.sas7bdat")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 16207, len(data1.Rows))
	assert.EqualValues(t, 912, len(data1.Columns))

	// g.Info(g.Marshal(data1.Columns.Types()))
	// g.Info(g.Marshal(data1.Rows[0]))
	// g.P(data1.Rows[0])
}

func TestFileSysLocalAvro(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	df1, err := fs.ReadDataflow("test/test1/avro/twitter.avro")
	assert.NoError(t, err)

	data1, err := df1.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, len(data1.Rows))
	assert.EqualValues(t, 3, len(data1.Columns))

	avroSchema := `
	{
	  "type": "record",
	  "name": "test_schema",
	  "fields": [
		{
		  "name": "time",
		  "type": "long"
		},
		{
		  "name": "customer",
		  "type": "string"
		}
	  ]
	}`

	// Writing OCF data
	var ocfFileContents bytes.Buffer
	writer, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:      &ocfFileContents,
		Schema: avroSchema,
	})
	if err != nil {
		fmt.Println(err)
	}
	err = writer.Append([]map[string]interface{}{
		{
			"time":     1617104831727,
			"customer": "customer1",
		},
		{
			"time":     1717104831727,
			"customer": "customer2",
		},
	})
	assert.NoError(t, err)
	// fmt.Println("ocfFileContents", ocfFileContents.String())

	reader := strings.NewReader(ocfFileContents.String())

	ds := iop.NewDatastream(nil)
	err = ds.ConsumeAvroReader(reader)
	assert.NoError(t, err)

	data, err := ds.Collect(0)
	assert.NoError(t, err)

	assert.EqualValues(t, 2, len(data.Rows))
	assert.EqualValues(t, 2, len(data.Columns))

}

func TestFileSysDOSpaces(t *testing.T) {
	fs, err := NewFileSysClient(
		dbio.TypeFileS3,
		// "ENDPOINT=https://nyc3.digitaloceanspaces.com",
		"ENDPOINT=nyc3.digitaloceanspaces.com",
		"ACCESS_KEY_ID="+os.Getenv("DOS_ACCESS_KEY_ID"),
		"SECRET_ACCESS_KEY="+os.Getenv("DOS_SECRET_ACCESS_KEY"),
		"METADATA="+g.Marshal(iop.Metadata{LoadedAt: iop.KeyValue{"loaded_at", time.Now().Unix()}, StreamURL: iop.KeyValue{"url", ""}}),
	)
	assert.NoError(t, err)

	// Test List
	paths, err := fs.List("s3://ocral/")
	assert.NoError(t, err)

	// Test Delete, Write, Read
	testPath := "s3://ocral/test/fs.test"
	testString := "abcde"
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)

	// writer, err := fs.GetWriter(testPath)
	// bw, err := Write(reader, writer)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("s3://ocral/")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("s3://ocral/test/")
	assert.NoError(t, err)

	// Test datastream
	df, err := fs.ReadDataflow("s3://ocral/test/")
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		data, err := ds.Collect(0)
		assert.NoError(t, err)
		assert.EqualValues(t, 18, len(data.Rows))
	}

	// Test concurrent wrinting from datastream

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "s3://ocral/test.fs.write"
	_, err = WriteDataflow(fs, df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	df2, err = localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	writeFolderPath = "s3://ocral/test.fs.write.json"
	_, err = WriteDataflow(fs, df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
	assert.Contains(t, data2.Columns.Names(), "loaded_at")
	assert.Contains(t, data2.Columns.Names(), "url")
}

func TestFileSysLarge(t *testing.T) {
	path := ""
	// path = "s3://ocral-data-1/LargeDataset.csv"
	// path = "s3://ocral-data-1"
	path = "s3://ocral-data-1/test.fs.write/part.01.0001.csv"
	path = "s3://ocral-data-1/test.fs.write/part"
	// path = "gs://flarco_us_bucket/test"
	// path = "gs://flarco_us_bucket/test/part"
	// path = "https://flarcostorage.blob.core.windows.net/testcont/test2"
	// path = "https://flarcostorage.blob.core.windows.net/testcont/test2/part"
	fs, err := NewFileSysClientFromURL(path)
	assert.NoError(t, err)

	paths, err := fs.List(path)
	assert.NoError(t, err)
	g.P(paths)

	return

	df, err := fs.ReadDataflow(path, iop.FileStreamConfig{Limit: 10000})
	assert.NoError(t, err)

	for ds := range df.StreamCh {
		_, err := ds.Collect(0)
		assert.NoError(t, err)
	}
	assert.NoError(t, df.Err())
}

func TestFileSysS3(t *testing.T) {
	t.Parallel()
	fs, err := NewFileSysClient(dbio.TypeFileS3)
	// fs, err := NewFileSysClient(S3cFileSys, "ENDPOINT=s3.amazonaws.com")
	assert.NoError(t, err)

	buckets, err := fs.Buckets()
	assert.NoError(t, err)
	assert.NotEmpty(t, buckets)

	// Test Delete, Write, Read
	testPath := "s3://ocral-data-1/test/fs.test"
	testString := "abcde"
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	paths, err := fs.List("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), "s3://ocral-data-1/test/")

	paths, err = fs.ListRecursive("s3://ocral-data-1/test/*.test")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 1)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("sling_test/csv/*.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 11)

	paths, err = fs.ListRecursive("sling_test/csv/part.??.001?.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 2)

	paths, err = fs.ListRecursive("test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("test/*.test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("s3://ocral-data-1/")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	// Test concurrent writing from datastream

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	localFs.SetProp("header", "true")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)

	writeFolderPath := "s3://ocral-data-1/test.fs.write"
	err = Delete(fs, writeFolderPath)
	assert.NoError(t, err)

	fs.SetProp("header", "true")
	fs.SetProp("FILE_MAX_BYTES", "20000")
	_, err = WriteDataflow(fs, df2, writeFolderPath)
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())
	assert.EqualValues(t, 1, len(df2.Streams))

	// eventual consistency
	time.Sleep(2 * time.Second) // wait to s3 files to write on AWS side
	df3, err := fs.ReadDataflow(writeFolderPath, iop.FileStreamConfig{Limit: 1})
	assert.NoError(t, err)

	data2, err := iop.MergeDataflow(df3).Collect(int(df3.Limit))
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(data2.Rows))
}

func TestFileSysAzure(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileAzure, "container=testcont")
	if !assert.NoError(t, err) {
		return
	}

	buckets, err := fs.Buckets()
	if !assert.NoError(t, err) {
		return
	}
	assert.NotEmpty(t, buckets)

	testString := "abcde"
	testPath := "https://flarcostorage.blob.core.windows.net/testcont/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	_ = bw
	// assert.EqualValues(t, 5, bw) // azure blob content-length is not returned. Need custom reader to capture length flowed
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	assert.NoError(t, err)

	paths, err := fs.ListRecursive("https://flarcostorage.blob.core.windows.net/testcont/test/*1")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("test/*1")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("sling_test/csv/*.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 11)

	paths, err = fs.ListRecursive("sling_test/csv/part.??.001?.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 2)

	assert.Equal(t, testString, string(testBytes))
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("https://flarcostorage.blob.core.windows.net")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	localFs.SetProp("header", "true")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	fs.SetProp("header", "true")
	writeFolderPath := "https://flarcostorage.blob.core.windows.net/testcont/test2"
	err = Delete(fs, writeFolderPath)
	assert.NoError(t, err)

	_, err = WriteDataflow(fs, df2, writeFolderPath+"/*.csv")
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	if assert.NoError(t, err) {
		data2, err := df3.Collect()
		assert.NoError(t, err)
		assert.EqualValues(t, 1036, len(data2.Rows))
	}

	df3, err = fs.ReadDataflow("https://flarcostorage.blob.core.windows.net/testcont/test2/part.01.0001.csv")
	if assert.NoError(t, err) {
		data2, err := df3.Collect()
		assert.NoError(t, err)
		assert.Greater(t, len(data2.Rows), 0)
	}
	// Delete(fs, writeFolderPath)
}

func TestFileSysGoogle(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(dbio.TypeFileGoogle, "BUCKET=flarco_us_bucket")
	if !assert.NoError(t, err) {
		return
	}

	buckets, err := fs.Buckets()
	assert.NoError(t, err)
	assert.NotEmpty(t, buckets)

	testString := "abcde"
	testPath := "gs://flarco_us_bucket/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.EqualValues(t, 5, bw)
	assert.NoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	paths, err := fs.ListRecursive("gs://flarco_us_bucket/test/*1")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("sling_test/csv/*.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 11)

	paths, err = fs.ListRecursive("sling_test/csv/part.??.001?.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 2)

	paths, err = fs.ListRecursive("test/*1")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive("gs://flarco_us_bucket/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	localFs.SetProp("header", "true")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := "gs://flarco_us_bucket/test"
	fs.SetProp("header", "true")
	_, err = WriteDataflow(fs, df2, writeFolderPath+"/*.csv")
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	if !assert.NoError(t, err) {
		return
	}

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))
}

func TestFileSysNormalizeURI(t *testing.T) {
	url := "sftp://sling.uri.test:2222/path/to/write/{stream_file_name}"
	fs, err := NewFileSysClient(dbio.TypeFileSftp, "URL="+url)
	assert.NoError(t, err)
	if t.Failed() {
		return
	}

	u := url
	assert.Equal(t, u, NormalizeURI(fs, u))

	u = "sftp://sling.uri.test:2222//path/to/write/{stream_file_name}"
	assert.Equal(t, u, NormalizeURI(fs, u))

	u = "path/to/write/{stream_file_name}"
	assert.Equal(t, "sftp://sling.uri.test:2222/path/to/write/{stream_file_name}", NormalizeURI(fs, u))

	u = "/path/to/write/{stream_file_name}"
	assert.Equal(t, "sftp://sling.uri.test:2222//path/to/write/{stream_file_name}", NormalizeURI(fs, u))
}

func TestFileSysSftp(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(
		dbio.TypeFileSftp,
		// "SSH_PRIVATE_KEY=/root/.ssh/id_rsa",
		"URL="+os.Getenv("SSH_TEST_PASSWD_URL"),
	)
	assert.NoError(t, err)
	if t.Failed() {
		return
	}

	root := os.Getenv("SSH_TEST_PASSWD_URL")
	rootU, err := net.NewURL(root)
	assert.NoError(t, err)
	root = g.F("sftp://%s:22", rootU.Hostname())

	testString := "abcde"
	testPath := root + "/tmp/test/test1"
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, 5, bw)

	reader2, err := fs.GetReader(testPath)
	if !assert.NoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	assert.NoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	paths, err := fs.ListRecursive(root + "/tmp/test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/tmp/test/*1")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 1)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/tmp/test/test?")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 1)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/tmp/test/test??")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 0)

	paths, err = fs.ListRecursive("tmp/test")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive("tmp/test/*1")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	err = Delete(fs, testPath)
	assert.NoError(t, err)

	paths, err = fs.ListRecursive(root + "/tmp/test")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	localFs.SetProp("header", "true")
	df2, err := localFs.ReadDataflow("test/test1/csv")
	assert.NoError(t, err)
	// assert.EqualValues(t, 3, len(df2.Streams))

	writeFolderPath := root + "/tmp/test"
	fs.SetProp("header", "true")
	_, err = WriteDataflow(fs, df2, writeFolderPath+"/*.csv")
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, df2.Count())

	df3, err := fs.ReadDataflow(writeFolderPath)
	assert.NoError(t, err)

	data2, err := df3.Collect()
	assert.NoError(t, err)
	assert.EqualValues(t, 1036, len(data2.Rows))

	err = Delete(fs, writeFolderPath)
	assert.NoError(t, err)
}

func TestFileSysFtp(t *testing.T) {
	t.Parallel()

	fs, err := NewFileSysClient(
		dbio.TypeFileFtp,
		"URL="+os.Getenv("FTP_TEST_URL"),
	)
	assert.NoError(t, err)
	if t.Failed() {
		return
	}

	root := os.Getenv("FTP_TEST_URL")
	rootU, err := net.NewURL(root)
	assert.NoError(t, err)
	root = "ftp://" + rootU.Hostname() + ":" + rootU.U.Port()

	csvBytes, err := os.ReadFile("test/test1/csv/test1.csv")
	if !g.AssertNoError(t, err) {
		return
	}
	testString := string(csvBytes)
	testPath := root + "/test/test1.csv"
	reader := strings.NewReader(testString)
	_, err = fs.Write(testPath, reader)
	g.AssertNoError(t, err)

	reader2, err := fs.GetReader(testPath)
	if !g.AssertNoError(t, err) {
		return
	}

	testBytes, err := io.ReadAll(reader2)
	g.AssertNoError(t, err)

	assert.Equal(t, testString, string(testBytes))

	// reconnect to list, or it will error
	fs, err = NewFileSysClient(
		dbio.TypeFileFtp,
		"URL="+os.Getenv("FTP_TEST_URL"),
	)
	assert.NoError(t, err)
	paths, err := fs.ListRecursive(root + "/")
	g.LogError(err)
	if !assert.NoError(t, err) {
		return
	}
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/*.csv")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/test/*.csv")
	assert.NoError(t, err)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/test/test?.csv")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 1)
	assert.Contains(t, paths.URIs(), testPath)

	paths, err = fs.ListRecursive(root + "/test/test??")
	assert.NoError(t, err)
	assert.Len(t, paths.URIs(), 0)

	paths, err = fs.ListRecursive(root + "/test/*.1csv")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	df3, err := fs.ReadDataflow(testPath)
	if !g.AssertNoError(t, err) {
		return
	}

	data2, err := df3.Collect()
	g.AssertNoError(t, err)
	assert.EqualValues(t, 1000, len(data2.Rows))

	// reconnect to list, or it will error
	fs, err = NewFileSysClient(
		dbio.TypeFileFtp,
		"URL="+os.Getenv("FTP_TEST_URL"),
	)
	g.AssertNoError(t, err)

	err = Delete(fs, testPath)
	g.AssertNoError(t, err)

	paths, err = fs.ListRecursive(root + "/test/*.csv")
	assert.NoError(t, err)
	assert.NotContains(t, paths.URIs(), testPath)

	// localFs, err := NewFileSysClient(dbio.TypeFileLocal)
	// assert.NoError(t, err)

	// localFs.SetProp("datetime_format", "02-01-2006 15:04:05.000")
	// df2, err := localFs.ReadDataflow("test/test1/csv")
	// assert.NoError(t, err)
	// // assert.EqualValues(t, 3, len(df2.Streams))

	// writeFolderPath := root + "/home/test/test"
	// _, err = WriteDataflow(fs,df2, writeFolderPath+"/*.csv")
	// assert.NoError(t, err)
	// assert.EqualValues(t, 1036, df2.Count())
}

func TestFileSysGoogleDrive(t *testing.T) {
	// Skip if no Google Drive credentials are provided
	if os.Getenv("GDRIVE_KEY_FILE") == "" && os.Getenv("GDRIVE_CLIENT_ID") == "" {
		t.Skip("Skipping Google Drive tests - no credentials provided")
		return
	}

	t.Parallel()

	// Create Google Drive filesystem client
	fs, err := NewFileSysClient(dbio.TypeFileGoogleDrive, "folder_id=1J0XayKhZ1Nw4q-awg2y32S18Yy-jLwuR", "GDRIVE_KEY_FILE="+os.Getenv("GDRIVE_KEY_FILE"))
	if !assert.NoError(t, err) {
		return
	}

	// Test 1: Basic Write and Read
	testPath := "gdrive:///test/fs_test.txt"
	testString := "Hello Google Drive!"

	// Delete if exists
	_ = Delete(fs, testPath)

	// Write file
	reader := strings.NewReader(testString)
	bw, err := fs.Write(testPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, len(testString), bw)

	// Read file back
	reader2, err := fs.GetReader(testPath)
	if assert.NoError(t, err) {
		data := make([]byte, len(testString))
		_, err = reader2.Read(data)
		assert.NoError(t, err)
		assert.Equal(t, testString, string(data))
	}

	// Test 2: List files
	paths, err := fs.List("gdrive:///test/")
	assert.NoError(t, err)
	found := false
	for _, node := range paths {
		if strings.Contains(node.URI, "fs_test.txt") {
			found = true
			assert.False(t, node.IsDir)
			assert.Greater(t, node.Size, uint64(0))
			break
		}
	}
	assert.True(t, found, "Should find the test file in listing")

	// Test 3: List recursive
	paths, err = fs.ListRecursive("gdrive://")
	assert.NoError(t, err)
	found = false
	for _, node := range paths {
		if strings.Contains(node.URI, "fs_test.txt") {
			found = true
			break
		}
	}
	assert.True(t, found, "Should find the test file in recursive listing")

	// Test 4: Create nested directories
	nestedPath := "gdrive:///test/nested/deep/file.txt"
	nestedContent := "Nested file content"
	reader = strings.NewReader(nestedContent)
	bw, err = fs.Write(nestedPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, len(nestedContent), bw)

	// Verify nested file exists
	reader2, err = fs.GetReader(nestedPath)
	if assert.NoError(t, err) {
		data := make([]byte, len(nestedContent))
		_, err = reader2.Read(data)
		assert.NoError(t, err)
		assert.Equal(t, nestedContent, string(data))
	}

	// Test 5: Overwrite existing file
	updatedContent := "Updated content"
	reader = strings.NewReader(updatedContent)
	bw, err = fs.Write(testPath, reader)
	assert.NoError(t, err)
	assert.EqualValues(t, len(updatedContent), bw)

	// Verify updated content
	reader2, err = fs.GetReader(testPath)
	if assert.NoError(t, err) {
		data := make([]byte, len(updatedContent))
		_, err = reader2.Read(data)
		assert.NoError(t, err)
		assert.Equal(t, updatedContent, string(data))
	}

	// Test 6: Delete files
	err = Delete(fs, testPath)
	assert.NoError(t, err)

	// Verify file is deleted
	time.Sleep(2 * time.Second) // Google Drive might have eventual consistency
	_, err = fs.GetReader(testPath)
	assert.Error(t, err, "Should get error when reading deleted file")

	// Clean up nested file
	err = Delete(fs, nestedPath)
	assert.NoError(t, err)
}

func TestFileSysHTTP(t *testing.T) {
	fs, err := NewFileSysClient(
		dbio.TypeFileHTTP, //"HTTP_USER=user", "HTTP_PASSWORD=password",
	)
	paths, err := fs.List("https://repo.anaconda.com/miniconda/")
	assert.NoError(t, err)
	assert.Greater(t, len(paths), 100)

	// paths, err = fs.List("https://privatefiles.ocral.org/")
	// assert.NoError(t, err)
	// g.P(paths)

	sampleCsv := "http://samplecsvs.s3.amazonaws.com/Sacramentorealestatetransactions.csv"
	// sampleCsv = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_25000.csv"
	sampleCsv = "https://www.stats.govt.nz/assets/Uploads/Business-price-indexes/Business-price-indexes-December-2019-quarter/Download-data/business-price-indexes-december-2019-quarter-csv.csv"
	// sampleCsv = "http://hci.stanford.edu/courses/cs448b/data/ipasn/cs448b_ipasn.csv"
	// sampleCsv = "https://perso.telecom-paristech.fr/eagan/class/igr204/data/BabyData.zip"
	sampleCsv = "https://people.sc.fsu.edu/~jburkardt/data/csv/freshman_kgs.csv"
	df, err := fs.ReadDataflow(sampleCsv)
	if !assert.NoError(t, err) {
		return
	}

	for _, ds := range df.Streams {
		data, err := ds.Collect(0)
		assert.NoError(t, err)
		assert.Greater(t, len(data.Rows), 0)
		// g.P(len(data.Rows))
		for _, row := range data.Rows {
			g.P(row)
		}
	}
}

func testManyCSV(t *testing.T) {
	fs, err := NewFileSysClient(dbio.TypeFileHTTP, "concurrencyLimit=5")
	nodes, err := fs.List("https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html")
	// paths, err := fs.List("https://www.stats.govt.nz/large-datasets/csv-files-for-download/")
	assert.NoError(t, err)

	// println(strings.Join(paths, "\n"))

	csvPaths := []string{}
	dss := []*iop.Datastream{}
	for _, path := range nodes.URIs() {
		if strings.HasSuffix(path, ".csv") {
			csvPaths = append(csvPaths, path)
			g.Debug("added csvPath %s", path)
			ds, err := fs.Self().GetDatastream(path)
			g.LogError(err, "could not parse "+path)
			if err == nil {
				dss = append(dss, ds)
			}
			// data, err := ds.Collect(0)
			// assert.NoError(t, err)
			// g.Debug("%d rows collected from %s", len(data.Rows), path)
		}
	}

	g.Debug("%d csvPaths", len(csvPaths))

	for i, ds := range dss {
		data, err := ds.Collect(0)
		g.Debug("%d rows collected from %s", len(data.Rows), csvPaths[i])
		if assert.NoError(t, err, "for "+csvPaths[i]) {
			assert.Greater(t, len(data.Rows), 0)
		}
	}
}

func TestCopyRecursive(t *testing.T) {
	// Setup local filesystem clients
	localFs1, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	localFs2, err := NewFileSysClient(dbio.TypeFileLocal)
	assert.NoError(t, err)

	// Setup S3 filesystem client
	s3Fs, err := NewFileSysClient(dbio.TypeFileS3)
	assert.NoError(t, err)

	azureFs, err := NewFileSysClient(dbio.TypeFileAzure, "container=testcont")
	assert.NoError(t, err)

	// Create test scenarios
	tests := []struct {
		name     string
		fromFs   FileSysClient
		toFs     FileSysClient
		fromPath string
		toPath   string
		wantErr  bool
	}{
		{
			name:     "local to local - single directory",
			fromFs:   localFs1,
			toFs:     localFs2,
			fromPath: "test/test1/csv",
			toPath:   "/tmp/test/test_copy/local_to_local",
			wantErr:  false,
		},
		{
			name:     "local to local - multiple files with pattern",
			fromFs:   localFs1,
			toFs:     localFs2,
			fromPath: "test/test1/csv/*.csv",
			toPath:   "/tmp/test/test_copy/local_to_local_pattern",
			wantErr:  false,
		},
		{
			name:     "local to s3",
			fromFs:   localFs1,
			toFs:     s3Fs,
			fromPath: "test/test1/csv",
			toPath:   "s3://ocral-data-1/test/test_copy/local_to_s3",
			wantErr:  false,
		},
		{
			name:     "s3 to local",
			fromFs:   s3Fs,
			toFs:     localFs1,
			fromPath: "s3://ocral-data-1/test/test_copy/local_to_s3",
			toPath:   "/tmp/test/test_copy/s3_to_local",
			wantErr:  false,
		},
		{
			name:     "s3 to azure",
			fromFs:   s3Fs,
			toFs:     azureFs,
			fromPath: "test/test_copy/local_to_s3",
			toPath:   "test/test_copy/s3_to_azure",
			wantErr:  false,
		},
		{
			name:     "azure to s3",
			fromFs:   azureFs,
			toFs:     s3Fs,
			fromPath: "test/test_copy/s3_to_azure",
			toPath:   "test/test_copy/azure_to_s3",
			wantErr:  false,
		},
		{
			name:     "invalid source path",
			fromFs:   localFs1,
			toFs:     localFs2,
			fromPath: "test/nonexistent/path",
			toPath:   "test/test_copy/invalid",
			wantErr:  true,
		},
	}

	// Run test cases
	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			tt.fromPath = NormalizeURI(tt.fromFs, tt.fromPath)
			tt.toPath = NormalizeURI(tt.toFs, tt.toPath)

			// Clean up destination before test
			_ = Delete(tt.toFs, tt.toPath)

			// Perform copy
			totalBytes, err := CopyRecursive(tt.fromFs, tt.toFs, tt.fromPath, tt.toPath)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Greater(t, totalBytes, int64(0))

			// Verify copy by listing files
			fromNodes, err := tt.fromFs.ListRecursive(tt.fromPath)
			assert.NoError(t, err)

			toNodes, err := tt.toFs.ListRecursive(tt.toPath)
			assert.NoError(t, err)

			// Count actual files (excluding directories)
			fromFileCount := 0
			for _, node := range fromNodes {
				if !node.IsDir {
					fromFileCount++
				}
			}

			toFileCount := 0
			for _, node := range toNodes {
				if !node.IsDir {
					toFileCount++
				}
			}

			// Verify file counts match
			assert.Equal(t, fromFileCount, toFileCount)

			// Verify content of a sample file
			if fromFileCount > 0 {
				// Get first file from source
				var firstSourceFile FileNode
				for _, node := range fromNodes {
					if !node.IsDir {
						firstSourceFile = node
						break
					}
				}

				// Read source content
				sourceReader, err := tt.fromFs.GetReader(firstSourceFile.URI)
				assert.NoError(t, err)
				sourceContent, err := io.ReadAll(sourceReader)
				assert.NoError(t, err)

				// Find corresponding destination file
				commonParent := tt.fromPath
				if strings.Contains(tt.fromPath, "*") || strings.Contains(tt.fromPath, "?") {
					commonParent = GetDeepestParent(tt.fromPath)
				}
				relPath := strings.TrimPrefix(firstSourceFile.URI, commonParent)
				relPath = strings.TrimPrefix(relPath, "/")
				destPath := tt.toPath + "/" + relPath

				// Read destination content
				destReader, err := tt.toFs.GetReader(destPath)
				if !assert.NoError(t, err) {
					return
				}
				destContent, err := io.ReadAll(destReader)
				assert.NoError(t, err)

				// Compare contents
				assert.Equal(t, len(sourceContent), len(destContent))
			}

		})

		if t.Failed() {
			return
		}
	}

	for _, tt := range tests {
		// Clean up
		_ = Delete(tt.toFs, tt.toPath)
	}
}
