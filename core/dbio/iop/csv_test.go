package iop

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flarco/g"
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
	assert.NoError(t, err)

	file, _ := os.Create("test0.csv")
	_, err = data.WriteCsv(file)
	assert.NoError(t, err)

	os.Remove("test0.csv")
	os.Remove("test2.csv")

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
	// g.P(newHeader)
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
		g.P(string(scanner.Bytes()))
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

	testBytes, reader, err := g.Peek(strings.NewReader(input), 0)
	assert.NoError(t, err)

	needsCleanUp := detectCarrRet(testBytes)
	g.P(needsCleanUp)
	b, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.NotEqual(t, "", string(b))
	g.P(string(b))
}

func TestISO8601(t *testing.T) {
	s := "YYYY-MM-DDTHH:mm:ss.sZ"
	assert.Equal(t, "2006-01-02T15:04:05.000Z", Iso8601ToGoLayout(s), s)

	s = "YYYY-MM"
	assert.Equal(t, "2006-01", Iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.sZ09:00"
	assert.Equal(t, "2006-01-02T15:04:05.000Z0700", Iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s Z09:00"
	assert.Equal(t, "2006-01-02T15:04:05.000 Z0700", Iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s -04:00"
	assert.Equal(t, "2006-01-02T15:04:05.000 -0700", Iso8601ToGoLayout(s), s)

	s = "YYYY-MM-DDTHH:mm:ss.s+14:00"
	assert.Equal(t, "2006-01-02T15:04:05.000+0700", Iso8601ToGoLayout(s), s)

	dateMap := GetISO8601DateMap(time.Unix(1494505756, 0).UTC())
	str := "/path/{YYYY}/{MM}/{DD}/{HH}:{mm}:{ss}"
	assert.Equal(t, "/path/2017/05/11/12:29:16", g.Rm(str, dateMap))
}

func TestSreamOptions(t *testing.T) {

	configMap := map[string]string{}

	consume := func() Dataset {
		file, err := os.Open("test/test1.csv")
		assert.NoError(t, err)
		ds := NewDatastream(nil)
		ds.SetConfig(configMap)
		// g.P(ds.Sp.config)
		err = ds.ConsumeCsvReader(bufio.NewReader(file))
		assert.NoError(t, err)

		data, err := ds.Collect(0)
		assert.NoError(t, err)
		return data
	}

	configMap["empty_field_as_null"] = "FALSE"
	data := consume()
	assert.Equal(t, "", data.Rows[9][1])
	assert.Equal(t, nil, data.Rows[20][0]) // this is an integer field, so nil is best instead of 0 (put by golang)

	configMap["empty_field_as_null"] = "TRUE"
	data = consume()
	assert.Equal(t, nil, data.Rows[9][1])
	assert.Equal(t, "NULL", data.Rows[9][2])
	assert.Equal(t, " killsley9@feedburner.com ", data.Rows[9][3])
	assert.Equal(t, "19-02-2019 16:23:06.000", data.Rows[9][5])
	assert.Equal(t, "string", data.Columns[5].Type) // since timestamp is not recognized
	assert.Equal(t, nil, data.Rows[20][0])

	configMap["null_if"] = "NULL"
	configMap["skip_blank_lines"] = "TRUE"
	configMap["datetime_format"] = "DD-MM-YYYY HH:mm:ss.s"
	data = consume()
	// g.P(data.Columns[5])
	assert.Equal(t, "datetime", data.Columns[5].Type)
	assert.Equal(t, nil, data.Rows[9][2])
	assert.Equal(t, "killsley9@feedburner.com", data.Rows[9][3])
	assert.Equal(t, "Roger", data.Rows[20][1])

}

func TestDetectDelimiter(t *testing.T) {
	testString := `col1,col2
cal,cal
cao;daf
"fa",da
ra<d|da`
	deli, numCols, err := detectDelimiter(",", []byte(testString))
	assert.NoError(t, err) // since delimiter is specified, will retun no error
	assert.Equal(t, string(','), string(deli))
	assert.Equal(t, 2, numCols)

	deli, _, err = detectDelimiter("\t", []byte(testString))
	assert.NoError(t, err)
	assert.Equal(t, string(','), string(deli))
	// assert.Equal(t, 1, numCols)

	deli, numCols, err = detectDelimiter("", []byte(testString))
	assert.NoError(t, err)
	assert.Equal(t, string(','), string(deli))
	assert.Equal(t, 2, numCols)

	testString = `col1|col2
cal|cal
cao|daf
"fa"|da
ra<d|da`

	deli, numCols, err = detectDelimiter("", []byte(testString))
	assert.NoError(t, err)
	assert.Equal(t, string('|'), string(deli))
	assert.Equal(t, 2, numCols)

	testString = `Obj;PropId;Value;TimeStamp;TimeStampISO
BB01;85;45,3828582763672;133245162327228051;2023-03-28T22:30:32Z
BB01;85;40,3816032409668;133245181140278467;2023-03-28T23:01:54Z
BB01;85;45,3858795166016;133245207233952957;2023-03-28T23:45:23Z
BB01;85;50,388298034668;133245209487304477;2023-03-28T23:49:08Z
BB01;85;45,3873443603516;133245215378614197;2023-03-28T23:58:57Z
BB01;85;40,3829345703125;133245217529463186;2023-03-29T00:02:32Z
BB01;85;35,3816719055176;133245220376169720;2023-03-29T00:07:17Z
BB01;85;40,3844985961914;133245230678878369;2023-03-29T00:24:27Z
BB01;85;45,3865814208984;133245234406821951;2023-03-29T00:30:40Z`

	deli, numCols, err = detectDelimiter("", []byte(testString))
	assert.NoError(t, err)
	assert.Equal(t, string(';'), string(deli))
	assert.Equal(t, 5, numCols)
}

func TestRecords1(t *testing.T) {
	row := make([]any, 10)
	start := time.Now()
	for i := 0; i < 10000000; i++ {
		row0 := []any{"1", 2, 3.6, 4, 5, 6, 7, 8, 9, i}
		copy(row, row0)
	}
	end := time.Now()
	g.Info("delta: %d us", end.UnixMicro()-start.UnixMicro())
}

func TestRecords2(t *testing.T) {
	row := make(chan []any)
	start := time.Now()
	go func() {
		for i := 0; i < 10000000; i++ {
			row0 := []any{"1", 2, 3.6, 4, 5, 6, 7, 8, 9, i}
			row <- row0
		}
		close(row)
	}()

	for val := range row {
		_ = val
	}

	end := time.Now()
	g.Info("delta: %d us", end.UnixMicro()-start.UnixMicro())
}

// TestRecords3 is the fastest, with Mutex
func TestRecords3(t *testing.T) {
	row := make([]any, 10)
	done := atomic.Bool{}
	mux := sync.Mutex{}
	start := time.Now()
	started := make(chan bool)

	go func(r []any) {
		started <- true

		for i := 0; i < 10000000; i++ {
			row0 := []any{"1", 2, 3.6, 4, 5, 6, 7, 8, 9, i}
			mux.Lock()
			copy(r, row0)
			mux.Unlock()
		}
		done.Store(true)
	}(row)

	<-started

	var j int

	for {
		if done.Load() {
			break
		}
		mux.Lock()
		j = row[9].(int)
		_ = j
		mux.Unlock()
	}

	end := time.Now()
	g.Info("delta: %d us", end.UnixMicro()-start.UnixMicro())
	g.Info("%#v", row)
}

func TestRecords4(t *testing.T) {
	row := make([]any, 10)
	done := atomic.Bool{}
	hasNew := atomic.Bool{}
	start := time.Now()
	started := make(chan bool)

	go func(r []any) {
		started <- true

		for i := 0; i < 10000000; i++ {
		redo:
			if hasNew.Load() {
				goto redo
			}

			row0 := []any{"1", 2, 3.6, 4, 5, 6, 7, 8, 9, i}
			copy(r, row0)
			hasNew.Store(true)
		}
		done.Store(true)
	}(row)

	<-started

	var j int

	for {
		if done.Load() {
			break
		} else if !hasNew.Load() {
			continue
		}
		j = row[9].(int)
		_ = j

		hasNew.Store(false)
	}

	end := time.Now()
	g.Info("delta: %d us", end.UnixMicro()-start.UnixMicro())
	g.Info("%#v", row)
}
