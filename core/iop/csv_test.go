package iop

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	h "github.com/flarco/gutil"
	"github.com/stretchr/testify/assert"
)

func TestCSV(t *testing.T) {
	err := os.Remove("test2.csv")

	csv1 := CSV{Path: "test/test1.csv"}

	// Test streaming read & write
	ds, err := csv1.ReadStream()
	assert.NoError(t, err)
	if err != nil {
		return
	}

	csv2 := CSV{Path: "test2.csv"}
	_, err = csv2.WriteStream(ds)
	assert.NoError(t, err)

	// Test read & write
	data, err := ReadCsv("test2.csv")
	assert.NoError(t, err)

	assert.Len(t, data.Columns, 7)
	assert.Len(t, data.Rows, 1000)
	assert.Equal(t, data.Columns[6].Type, "decimal")
	assert.Equal(t, "AOCG,\"\n883", data.Records()[0]["first_name"])
	assert.Equal(t, "EKOZ,989", data.Records()[1]["last_name"])
	assert.EqualValues(t, 2.332, data.Records()[100]["rating"])
	assert.EqualValues(t, 28.686, data.Records()[1000-1]["rating"])
	if t.Failed() {
		return
	}

	err = os.Remove("test0.csv")

	err = data.WriteCsv("test0.csv")
	assert.NoError(t, err)

	err = os.Remove("test0.csv")
	err = os.Remove("test2.csv")

	// csv3 := CSV{
	// 	File:   data.Reader,
	// 	Fields: csv1.Fields,
	// }
	// stream, err = csv3.ReadStream()
	// assert.NoError(t, err)

	// csv2 = CSV{
	// 	Path:   "test2.csv",
	// 	Fields: csv1.Fields,
	// }
	// _, err = csv2.WriteStream(stream)
	// assert.NoError(t, err)
	// err = os.Remove("test2.csv")

}

// Revisit later
// func TestCSVDetectDeli(t *testing.T) {

// 	csv1 := CSV{Path: "test/test1.pipe.csv", detectDeli: true}
// 	reader, err := csv1.getReader()
// 	assert.NoError(t, err)

// 	deli := detectDelimiter(reader)
// 	println(string(deli))
// 	assert.Equal(t, '|', deli)

// }

func TestCleanHeaderRow(t *testing.T) {
	header := []string{
		"great-one!9",
		"great-one!9",
		"great-one!9",
		"gag|hello",
		"Seller(s)",
		"1Seller(s) \n cool",
	}
	newHeader := CleanHeaderRow(header)
	// h.P(newHeader)
	assert.Equal(t, "great_one_92", newHeader[2])
	assert.Equal(t, "_1seller_s____cool", newHeader[5])
}

func TestSplitCarrRet1(t *testing.T) {
	// An artificial input source.
	const input = "Now is the winter of our discontent,\r\nMade glorious summer by this sun of York.\r"
	scanner := bufio.NewScanner(strings.NewReader(input))
	// Set the split function for the scanning operation.
	// scanner.Split(bufio.ScanWords)
	scanner.Split(ScanCarrRet)
	// Count the words.
	count := 0
	for scanner.Scan() {
		h.P(string(scanner.Bytes()))
		count++
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading input:", err)
	}
	fmt.Printf("%d\n", count)
}
func TestSplitCarrRet2(t *testing.T) {
	// An artificial input source.
	const input = "Now is the winter of our discontent,\nMade glorious summer by this\r sun of York.\r\n"
	var reader io.Reader

	testBytes, reader, err := h.Peek(strings.NewReader(input), 0)
	assert.NoError(t, err)

	needsCleanUp := detectCarrRet(testBytes)
	h.P(needsCleanUp)
	b, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	assert.NotEqual(t, "", string(b))
	h.P(string(b))
}

func testManyCSV(t *testing.T) {
	fs, err := NewFileSysClient(HTTPFileSys, "concurencyLimit=5")
	paths, err := fs.List("https://people.sc.fsu.edu/~jburkardt/data/csv/csv.html")
	// paths, err := fs.List("https://www.stats.govt.nz/large-datasets/csv-files-for-download/")
	assert.NoError(t, err)

	// println(strings.Join(paths, "\n"))

	csvPaths := []string{}
	dss := []*Datastream{}
	for _, path := range paths {
		if strings.HasSuffix(path, ".csv") {
			csvPaths = append(csvPaths, path)
			h.Debug("added csvPath %s", path)
			ds, err := fs.Self().GetDatastream(path)
			h.LogError(err, "could not parse "+path)
			if err == nil {
				dss = append(dss, ds)
			}
			// data, err := ds.Collect(0)
			// assert.NoError(t, err)
			// h.Debug("%d rows collected from %s", len(data.Rows), path)
		}
	}

	h.Debug("%d csvPaths", len(csvPaths))

	for i, ds := range dss {
		data, err := Collect(ds)
		h.Debug("%d rows collected from %s", len(data.Rows), csvPaths[i])
		if assert.NoError(t, err, "for "+csvPaths[i]) {
			assert.Greater(t, len(data.Rows), 0)
		}
	}
}

func TestISO8601(t *testing.T) {
	s := "YYYY-MM-DDTHH:mm:ss.sZ"
	assert.Equal(t, "2006-01-02T15:04:05.000Z", iso8601ToGoLayout(s), s)

	s = "YYYY-MM"
	assert.Equal(t, "2006-01", iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.sZ09:00"
	assert.Equal(t, "2006-01-02T15:04:05.000Z0700", iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s Z09:00"
	assert.Equal(t, "2006-01-02T15:04:05.000 Z0700", iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s -04:00"
	assert.Equal(t, "2006-01-02T15:04:05.000 -0700", iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s+14:00"
	assert.Equal(t, "2006-01-02T15:04:05.000+0700", iso8601ToGoLayout(s), s)

	dateMap := GetISO8601DateMap(time.Unix(1494505756, 0))
	str := "/path/{YYYY}/{MM}/{DD}/{HH}:{mm}:{ss}"
	assert.Equal(t, "/path/2017/05/11/12:29:16", h.Rm(str, dateMap))
}

func TestSreamOptions(t *testing.T) {

	configMap := map[string]string{}

	consume := func() Dataset {
		file, err := os.Open("test/test1.csv")
		assert.NoError(t, err)
		ds := NewDatastream(nil)
		ds.SetConfig(configMap)
		// h.P(ds.Sp.config)
		err = ds.ConsumeReader(bufio.NewReader(file))
		assert.NoError(t, err)

		data, err := ds.Collect(0)
		assert.NoError(t, err)
		return data
	}

	configMap["EMPTY_FIELD_AS_NULL"] = "FALSE"
	data := consume()
	assert.Equal(t, "", data.Rows[9][1])
	assert.Equal(t, nil, data.Rows[20][0]) // this is an integer field, so nil is best instead of 0 (put by golang)

	configMap["EMPTY_FIELD_AS_NULL"] = "TRUE"
	data = consume()
	assert.Equal(t, nil, data.Rows[9][1])
	assert.Equal(t, "NULL", data.Rows[9][2])
	assert.Equal(t, " killsley9@feedburner.com ", data.Rows[9][3])
	assert.Equal(t, "19-02-2019 16:23:06.000", data.Rows[9][5])
	assert.Equal(t, "string", data.Columns[5].Type) // since timestamp is not recognized
	assert.Equal(t, nil, data.Rows[20][0])

	configMap["NULL_IF"] = "NULL"
	configMap["TRIM_SPACE"] = "TRUE"
	configMap["SKIP_BLANK_LINES"] = "TRUE"
	configMap["DATETIME_FORMAT"] = "DD-MM-YYYY HH:mm:ss.s"
	data = consume()
	// h.P(data.Columns[5])
	assert.Equal(t, "datetime", data.Columns[5].Type)
	assert.Equal(t, nil, data.Rows[9][2])
	assert.Equal(t, "killsley9@feedburner.com", data.Rows[9][3])
	assert.Equal(t, "Roger", data.Rows[20][1])

}
