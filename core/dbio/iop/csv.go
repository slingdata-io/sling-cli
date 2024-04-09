package iop

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// CSV is a csv object
type CSV struct {
	Path            string
	NoHeader        bool
	Delimiter       rune
	Escape          string
	FieldsPerRecord int
	Columns         []Column
	File            *os.File
	Data            Dataset
	Reader          io.Reader
	Config          map[string]string
	NoDebug         bool
	bytes           int64
	noInfer         bool
	cleanup         bool
}

// CleanHeaderRow cleans the header row from incompatible characters
func CleanHeaderRow(header []string) []string {
	// replace any other chars than regex expression
	regexAllow := *regexp.MustCompile(`[^a-zA-Z0-9_]`)
	fieldMap := map[string]string{}

	transformer := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	for i, field := range header {
		field = strings.TrimSpace(field)
		field = strings.TrimSuffix(strings.TrimPrefix(field, `"`), `"`)
		field, _, _ = transform.String(transformer, field)

		// clean up undesired character
		field = regexAllow.ReplaceAllString(field, "`") // temporary so we can trim
		field = strings.TrimRight(strings.TrimLeft(field, "`"), "`")
		field = strings.ReplaceAll(field, "`", "_")

		// any header with numbers first, add underscore
		if regexFirstDigit.Match([]byte(field)) {
			field = "_" + field
		}
		if field == "" {
			field = "col"
		}

		// avoid duplicates
		j := 1
		newField := field
		for fieldMap[newField] != "" {
			newField = g.F("%s%d", field, j)
			j++
		}

		fieldMap[newField] = field
		header[i] = strings.ToLower(newField)
	}

	return header
}

// ReadCsv reads CSV and returns dataset
func ReadCsv(path string) (Dataset, error) {
	file, err := os.Open(path)
	if err != nil {
		return NewDataset(nil), err
	}

	csv1 := CSV{
		File: file,
	}

	ds, err := csv1.ReadStream()
	if err != nil {
		return NewDataset(nil), err
	}

	data, err := ds.Collect(0)

	return data, err
}

// ReadCsvStream reads CSV and returns datasream
func ReadCsvStream(path string) (ds *Datastream, err error) {

	csv1 := CSV{
		Path: path,
	}

	stat, err := os.Stat(path)
	if err != nil {
		err = g.Error(err, "Invalid path: "+path)
		return
	}

	csv1.bytes = stat.Size()

	return csv1.ReadStream()
}

// InferSchema returns a sample of n rows
func (c *CSV) InferSchema() error {
	data, err := c.Sample(1000)
	if err != nil {
		return err
	}

	data.InferColumnTypes()
	c.Columns = data.Columns

	return nil
}

// Sample returns a sample of n rows
func (c *CSV) Sample(n int) (Dataset, error) {
	var data Dataset

	ds, err := c.ReadStream()
	if err != nil {
		return data, err
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}
	count := 0
	for row := range ds.Rows() {
		count++
		if count > n {
			break
		}
		data.Rows = append(data.Rows, row)
	}

	c.File = nil

	return data, nil
}

// ReadStream returns the read CSV stream with Line 1 as header
func (c *CSV) Read() (data Dataset, err error) {
	ds, err := c.ReadStream()
	if err != nil {
		return data, g.Error(err, "Error reading csv stream")
	}

	c.Data, err = ds.Collect(0)
	if err != nil {
		return c.Data, g.Error(err, "Error collecting")
	}
	c.Data.NoDebug = c.NoDebug
	return c.Data, err
}

// SetFields sets the fields
func (c *CSV) SetFields(fields []string) {
	c.Columns = make([]Column, len(fields))

	for i, field := range fields {
		c.Columns[i] = Column{
			Name:     field,
			Position: i + 1,
			Type:     "",
		}
	}
}

func (c *CSV) getReader() (r csv.CsvReaderLike, err error) {
	var reader2, reader3, reader4 io.Reader

	if c.File == nil && c.Reader == nil {
		file, err := os.Open(c.Path)
		if err != nil {
			return r, g.Error(err, "cannot open: %#v", c.Path)
		}
		c.File = file
		c.Reader = bufio.NewReader(c.File)
	} else if c.File != nil {
		c.Reader = bufio.NewReader(c.File)
	}

	// decompress if gzip
	readerDecompr, err := AutoDecompress(c.Reader)
	if err != nil {
		return r, g.Error(err, "Decompress(c.Reader)")
	}

	testBytes, reader2, err := g.Peek(readerDecompr, 0)
	if err != nil {
		g.LogError(err, "could not Peek")
		err = nil
		reader2 = readerDecompr
	}

	// g.Trace("testBytes:\n%s", string(testBytes))

	if !c.cleanup {
		// detect \r chars that need cleanup
		c.cleanup = detectCarrRet(testBytes)
	}

	if c.cleanup {
		// needs clean up
		g.Warn("end of line carriage returns detected. Cleaning up")
		scanner := bufio.NewScanner(reader2)
		scanner.Split(ScanCarrRet)

		var pw *io.PipeWriter
		reader3, pw = io.Pipe()
		go func() {
			defer pw.Close()
			for scanner.Scan() {
				lineByte := append(scanner.Bytes(), '\n')
				// g.P(string(lineByte))
				if len(bytes.TrimSpace(lineByte)) != 0 {
					pw.Write(lineByte)
				}
			}
			if scanner.Err() != nil {
				g.LogError(g.Error(scanner.Err()))
			}
		}()
	} else {
		reader3 = reader2
	}

	numCols := c.FieldsPerRecord
	if c.Delimiter == 0 || numCols <= 0 {
		var deli rune
		deli, numCols, err = detectDelimiter(string(c.Delimiter), testBytes)

		if c.Delimiter == 0 {
			if err != nil {
				g.Debug(`could not detect delimiter. Using ","`)
				c.Delimiter = ','
			} else {
				if !c.NoDebug {
					g.Debug("delimiter auto-detected: %#v", string(deli))
				}
				c.Delimiter = deli
			}
		}
		if c.FieldsPerRecord <= 0 && !c.NoDebug {
			g.Trace("number-cols detected: %d", numCols)
		}
		err = nil
	}

	// inject dummy header if none present
	if c.NoHeader {
		if numCols == 0 {
			return nil, g.Error("Unable to detect number of columns since `header=false`. Need to pass property `fields_per_rec`")
		}
		header := strings.Join(CreateDummyFields(numCols), string(c.Delimiter))
		reader4 = io.MultiReader(strings.NewReader(header+"\n"), reader3)
	} else {
		reader4 = reader3
	}

	if c.Escape != "" {
		options := csv.CsvOptions{}
		options.Delimiter = byte(c.Delimiter)
		options.Escape = []byte(c.Escape)[0]
		return csv.NewCsv(options).NewReader(reader4), nil
	}

	reader5 := csv.NewReader(reader4)
	reader5.LazyQuotes = true
	reader5.ReuseRecord = true
	reader5.FieldsPerRecord = c.FieldsPerRecord
	// r.TrimLeadingSpace = true
	// r.TrailingComma = true
	reader5.Comma = c.Delimiter

	return reader5, err
}

// CreateDummyFields creates dummy columns for csvs with no header row
func CreateDummyFields(numCols int) (cols []string) {
	colFmt := "col_%03d"
	cols = make([]string, numCols)
	for i := 0; i < numCols; i++ {
		cols[i] = fmt.Sprintf(colFmt, i+1)
	}
	return cols
}

// ReadStream returns the read CSV stream with Line 1 as header
func (c *CSV) ReadStream() (ds *Datastream, err error) {
	return c.ReadStreamContext(context.Background())
}

// ReadStream returns the read CSV stream with Line 1 as header
func (c *CSV) ReadStreamContext(ctx context.Context) (ds *Datastream, err error) {

	ds = NewDatastreamContext(ctx, c.Columns)

	r, err := c.getReader()
	if err != nil {
		return ds, g.Error(err, "Error getting CSV reader")
	}

	row0, err := r.Read()
	if err == io.EOF {
		g.Warn("csv stream provided is empty (%s)", c.Path)
		ds.SetReady()
		ds.Close()
		return ds, nil
	} else if err != nil {
		return ds, g.Error(err, "could not read from Reader")
	}
	c.SetFields(CleanHeaderRow(row0))

	nextFunc := func(it *Iterator) bool {
		if it.Closed {
			return false
		}

		row, err := r.Read()
		if err == io.EOF {
			c.File.Close()
			return false
		} else if err != nil {
			if !strings.Contains(err.Error(), "file already closed") {
				it.Context.CaptureErr(g.Error(err, "Error reading file"))
			}
			return false
		}

		it.Row = make([]interface{}, len(row))
		var val interface{}
		for i, val0 := range row {
			if !it.ds.Columns[i].IsString() {
				val0 = strings.TrimSpace(val0)
				if val0 == "" {
					val = nil
				} else {
					val = val0
				}
			} else {
				val = val0
			}
			it.Row[i] = val
		}

		return true
	}

	ds = NewDatastreamIt(context.Background(), c.Columns, nextFunc)
	ds.SetConfig(c.Config)
	ds.NoDebug = c.NoDebug

	err = ds.Start()
	if err != nil {
		return ds, g.Error(err, "could start datastream")
	}
	return
}

// WriteStream to CSV file
func (c *CSV) WriteStream(ds *Datastream) (cnt uint64, err error) {

	if c.File == nil {
		file, err := os.Create(c.Path)
		if err != nil {
			return cnt, err
		}
		c.File = file
	}

	defer c.File.Close()

	w := csv.NewWriter(c.File)
	if c.Delimiter != 0 {
		w.Comma = c.Delimiter
	}

	// header row to lower case
	fields := []string{}
	for _, field := range ds.GetFields(true, true) {
		fields = append(fields, strings.ToLower(field))
	}
	_, err = w.Write(fields)
	if err != nil {
		return cnt, g.Error(err, "error write row to csv file")
	}

	for row0 := range ds.Rows() {
		cnt++
		row := make([]string, len(row0))
		for i, val := range row0 {
			row[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
		}
		_, err = w.Write(row)
		if err != nil {
			return cnt, g.Error(err, "error write row to csv file")
		}
		w.Flush()
	}
	return cnt, nil
}

// NewReader creates a Reader
func (c *CSV) NewReader() (*io.PipeReader, error) {
	pipeR, pipeW := io.Pipe()
	ds, err := c.ReadStream()
	if err != nil {
		return nil, err
	}

	go func() {
		w := csv.NewWriter(pipeW)

		_, err := w.Write(ds.GetFields(true, true))
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error writing ds.Fields"))
			ds.Context.Cancel()
			pipeW.Close()
		}

		for row0 := range ds.Rows() {
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
			}
			_, err := w.Write(row)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error w.Write(row)"))
				ds.Context.Cancel()
				break
			}
			w.Flush()
		}
		pipeW.Close()
	}()

	return pipeR, nil
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

// ScanCarrRet removes the \r runes that are without \n rightafter
func ScanCarrRet(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\r'); i >= 0 {
		if len(data) > i+1 && data[i+1] != '\n' {
			return i + 1, data[0:i], nil
		} else if len(data) > i+1 && data[i+1] == '\n' {
			return i + 2, dropCR(data[0 : i+1]), nil
		}
		return i + 1, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

func detectCarrRet(testBytes []byte) (needsCleanUp bool) {
	testBuf := bytes.NewBuffer(testBytes)
	line, err := testBuf.ReadString('\r')
	if err == io.EOF {
		return
	} else if err != nil {
		g.LogError(err)
		return
	}

	// get next line, \n should be in there
	line, err = testBuf.ReadString('\r')
	if err != io.EOF && err != nil {
		g.LogError(err)
		return
	}

	if !strings.Contains(line, "\n") {
		needsCleanUp = true
		return
	}
	return
}

func detectDelimiter(delimiter string, testBytes []byte) (bestDeli rune, numCols int, err error) {
	bestDeli = ','
	deliSuggested := false
	if delimiter != "" {
		bestDeli = []rune(delimiter)[0]
		deliSuggested = true
	}

	deliList := []rune{',', '\t', '|', ';'}
	if deliSuggested {
		deliList = append([]rune{bestDeli}, deliList...)
	}

	type stats struct {
		total float64
		count float64
		max   int
	}

	testCsvRowStats := make([]stats, len(deliList))
	eG := g.ErrorGroup{}

	// remove last line
	testString := string(testBytes)
	lines := strings.Split(testString, "\n")
	if len(lines) > 2 {
		// here we are cutting the lines in attempt to have a "complete" row
		// however, this backfires when cutting a line when quote is escaped
		testString = strings.Join(lines[:len(lines)-1], "\n")
	}

	errMap := map[rune]error{}
	for i, d := range deliList {
		var csvErr error
		var row []string
		csvR := csv.NewReader(strings.NewReader(testString))
		csvR.LazyQuotes = true
		csvR.Comma = d
		for {
			row, csvErr = csvR.Read()
			if csvErr == io.EOF {
				csvErr = nil
				break
			} else if csvErr != nil {
				// g.Trace("failed delimiter detection for %#v: %s", string(d), csvErr.Error())
				errMap[d] = csvErr
				eG.Capture(errMap[d])
				break
			}
			testCsvRowStats[i].total = testCsvRowStats[i].total + float64(len(row))
			testCsvRowStats[i].count++
			if len(row) > testCsvRowStats[i].max {
				testCsvRowStats[i].max = len(row)
			}
		}
	}

	var maxRowNumAvg float64
	for i, RowStats := range testCsvRowStats {
		d := deliList[i]
		avg := RowStats.total / RowStats.count
		if avg > maxRowNumAvg {
			maxRowNumAvg = avg
			bestDeli = d
			numCols = RowStats.max
		}
	}

	if numCols <= 1 || len(eG.Errors) == len(deliList) {
		err = errors.New("could not detect delimiter")
		return
	}

	return
}

// GetISO8601DateMap return a map of date parts for string formatting
func GetISO8601DateMap(t time.Time) map[string]interface{} {
	m := map[string]interface{}{}
	for _, v := range []string{"YYYY", "YY", "MMM", "MM", "DD", "HH", "hh", "mm", "ss"} {
		m[v] = t.Format(Iso8601ToGoLayout(v))
	}
	return m
}

// https://www.w3.org/QA/Tips/iso-date
// https://www.w3.org/TR/NOTE-datetime
// https://www.iso.org/iso-8601-date-and-time-format.html
func Iso8601ToGoLayout(dateFormat string) (goDateFormat string) {
	goDateFormat = strings.TrimSpace(dateFormat)
	goDateFormat = strings.ReplaceAll(goDateFormat, "YYYY", "2006")
	goDateFormat = strings.ReplaceAll(goDateFormat, "YY", "06")
	goDateFormat = strings.ReplaceAll(goDateFormat, "MMM", "Jan")
	goDateFormat = strings.ReplaceAll(goDateFormat, "MM", "01")
	goDateFormat = strings.ReplaceAll(goDateFormat, "DD", "02")
	goDateFormat = strings.ReplaceAll(goDateFormat, "HH", "15")
	goDateFormat = strings.ReplaceAll(goDateFormat, "hh", "03")
	goDateFormat = strings.ReplaceAll(goDateFormat, "mm", "04")
	goDateFormat = strings.ReplaceAll(goDateFormat, ".ss", ".000")
	goDateFormat = strings.ReplaceAll(goDateFormat, "ss", "05")
	goDateFormat = strings.ReplaceAll(goDateFormat, ".s", ".000")
	goDateFormat = strings.ReplaceAll(goDateFormat, "ISO8601", "2006-01-02T15:04:05Z")

	goDateFormat = regexp.MustCompile(`Z\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "Z0700")
	goDateFormat = regexp.MustCompile(`-\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "-0700")
	goDateFormat = regexp.MustCompile(`\+\d\d:?\d\d$`).ReplaceAllString(goDateFormat, "+0700")

	return
}
