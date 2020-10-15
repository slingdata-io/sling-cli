package iop

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/dustin/go-humanize"
	h "github.com/flarco/gutil"
	"github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	"github.com/spf13/cast"
)

var (
	// ShowProgress use the progress bar to show progress
	ShowProgress = false

	// RemoveTrailingDecZeros removes the trailing zeros in CastToString
	RemoveTrailingDecZeros = false
	sampleSize             = 900
)

// Dataflow is a collection of concurrent Datastreams
type Dataflow struct {
	Columns    []Column
	Buffer     [][]interface{}
	StreamCh   chan *Datastream
	Streams    []*Datastream
	Context    h.Context
	deferFuncs []func()
	Bytes      int64
	Ready      bool
	Inferred   bool
	FsURL      string
	closed     bool
	pbar       *pb.ProgressBar
	mux        sync.Mutex
}

// Datastream is a stream of rows
type Datastream struct {
	Columns       []Column
	Rows          chan []interface{}
	Buffer        [][]interface{}
	Count         uint64
	Context       h.Context
	Ready         bool
	Bytes         int64
	Sp            *StreamProcessor
	SafeInference bool
	NoTrace       bool
	Inferred      bool
	deferFuncs    []func()
	closed        bool
	empty         bool
	it            *Iterator
	config        *streamConfig
	df            *Dataflow
}

type streamConfig struct {
	trimSpace      bool
	emptyAsNull    bool
	header         bool
	compression    string // AUTO | ZIP | GZIP | SNAPPY | NONE
	nullIf         string
	datetimeFormat string
	skipBlankLines bool
	delimiter      rune
	fileMaxRows    int64
	decimalDiv     float64
}

// Iterator is the row provider for a datastream
type Iterator struct {
	Row      []interface{}
	Counter  uint64
	Context  h.Context
	Closed   bool
	ds       *Datastream
	nextFunc func(it *Iterator) bool
}

// StreamProcessor processes rows and values
type StreamProcessor struct {
	N                uint64
	dateLayoutCache  string
	stringTypeCache  map[int]string
	colStats         map[int]*ColumnStats
	unrecognizedDate string
	warn             bool
	parseFuncs       map[string]func(s string) (interface{}, error)
	decReplRegex     *regexp.Regexp
	ds               *Datastream
	dateLayouts      []string
	config           *streamConfig
	rowBlankValCnt   int
}

// Dataset is a query returned dataset
type Dataset struct {
	Result        *sqlx.Rows
	Columns       []Column
	Rows          [][]interface{}
	SQL           string
	Duration      float64
	Sp            *StreamProcessor
	Inferred      bool
	SafeInference bool
	NoTrace       bool
}

// Column represents a schemata column
type Column struct {
	Position    int    `json:"position"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	DbType      string `json:"db_type"`
	DbPrecision int    `json:"db_precision"`
	DbScale     int    `json:"db_scale"`
	Stats       ColumnStats
	ColType     *sql.ColumnType
	goType      reflect.Type
}

// Columns represent many columns
type Columns []Column

// ColumnStats holds statistics for a column
type ColumnStats struct {
	MinLen    int
	MaxLen    int
	MaxDecLen int
	Min       int64
	Max       int64
	NullCnt   int64
	IntCnt    int64
	DecCnt    int64
	BoolCnt   int64
	StringCnt int64
	DateCnt   int64
	TotalCnt  int64
	Checksum  uint64
}

func init() {
	if os.Getenv("SLING_SAMPLE_SIZE") != "" {
		sampleSize = cast.ToInt(os.Getenv("SLING_SAMPLE_SIZE"))
	}
	if os.Getenv("SLING_REMOVE_TRAILING_ZEROS") != "" {
		RemoveTrailingDecZeros = cast.ToBool(os.Getenv("SLING_REMOVE_TRAILING_ZEROS"))
	}
}

// MakeRowsChan returns a buffered channel with default size
func MakeRowsChan() chan []interface{} {
	return make(chan []interface{}, sampleSize*2)
}

// https://github.com/cheggaaa/pb/blob/master/v3/element.go
// calculates the RAM percent
var elementMem pb.ElementFunc = func(state *pb.State, args ...string) string {
	memRAM, err := mem.VirtualMemory()
	if err != nil {
		return ""
	}
	return h.F("| %d%% MEM", cast.ToInt(memRAM.UsedPercent))
}

// calculates the CPU percent
var elementCPU pb.ElementFunc = func(state *pb.State, args ...string) string {
	cpuPct, err := cpu.Percent(0, false)
	if err != nil || len(cpuPct) == 0 {
		return ""
	}
	return h.F("| %d%% CPU", cast.ToInt(cpuPct[0]))
}

var elementStatus pb.ElementFunc = func(state *pb.State, args ...string) string {
	status := cast.ToString(state.Get("status"))
	if status == "" {
		return ""
	}
	return h.F("| %s", status)
}

type argsHelper []string

func (args argsHelper) getOr(n int, value string) string {
	if len(args) > n {
		return args[n]
	}
	return value
}

func (args argsHelper) getNotEmptyOr(n int, value string) (v string) {
	if v = args.getOr(n, value); v == "" {
		return value
	}
	return
}

var elementCounters pb.ElementFunc = func(state *pb.State, args ...string) string {
	var f string
	if state.Total() > 0 {
		f = argsHelper(args).getNotEmptyOr(0, "%s / %s")
	} else {
		f = argsHelper(args).getNotEmptyOr(1, "%[1]s")
	}
	return fmt.Sprintf(
		f, humanize.Commaf(cast.ToFloat64(state.Value())),
		humanize.Commaf(cast.ToFloat64(state.Total())),
	)
}

// NewPBar creates a new progress bar
func NewPBar(d time.Duration) *pb.ProgressBar {
	pbar := new(pb.ProgressBar)
	pb.RegisterElement("status", elementStatus, true)
	pb.RegisterElement("counters", elementCounters, true)
	tmpl := `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{ status . }}`
	if h.IsDebugLow() {
		pb.RegisterElement("mem", elementMem, true)
		pb.RegisterElement("cpu", elementCPU, true)
		tmpl = `{{etime . "%s" | yellow }} {{counters . }} {{speed . "%s r/s" | green }} {{ mem . }} {{ cpu . }} {{ status . }}`
	}
	barTmpl := pb.ProgressBarTemplate(tmpl)
	pbar = barTmpl.New(0)
	pbar.SetRefreshRate(d)
	pbar.SetWidth(40)
	return pbar
}

// NewDataflow creates a new dataflow
func NewDataflow() (df *Dataflow) {

	df = &Dataflow{
		StreamCh:   make(chan *Datastream, 100),
		Streams:    []*Datastream{},
		Context:    h.NewContext(context.Background()),
		deferFuncs: []func(){},
	}

	if ShowProgress {
		// progress bar ticker
		df.pbar = NewPBar(time.Second)
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					cnt := df.Count()
					if cnt > 10000 {
						df.pbar.Start()
						df.pbar.SetCurrent(cast.ToInt64(cnt))
					}
				default:
					time.Sleep(100 * time.Millisecond)
					if df.IsEmpty() || df.Err() != nil {
						df.pbar.SetCurrent(cast.ToInt64(df.Count()))
						df.pbar.Finish()
						return
					}
				}
			}
		}()
	}

	return
}

// WriteCsv writes to a csv file
func (data *Dataset) WriteCsv(path string) error {
	file, err := os.Create(path)

	w := csv.NewWriter(file)
	defer w.Flush()

	err = w.Write(data.GetFields())
	if err != nil {
		return h.Error(err, "error write row to csv file")
	}

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = cast.ToString(val)
		}
		err := w.Write(rec)
		if err != nil {
			return h.Error(err, "error write row to csv file")
		}
	}
	return nil
}

//NewStreamProcessor returns a new StreamProcessor
func NewStreamProcessor() *StreamProcessor {
	sp := StreamProcessor{
		stringTypeCache: map[int]string{},
		colStats:        map[int]*ColumnStats{},
		decReplRegex:    regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`),
		config:          &streamConfig{emptyAsNull: true},
	}
	if os.Getenv("MAX_DECIMALS") != "" {
		sp.config.decimalDiv = cast.ToFloat64(math.Pow10(cast.ToInt(os.Getenv("MAX_DECIMALS"))))
	}
	sp.parseFuncs = map[string]func(s string) (interface{}, error){
		"int": func(s string) (interface{}, error) {
			return strconv.ParseInt(s, 10, 64)
		},
		"float": func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 64)
		},
		"time": func(s string) (interface{}, error) {
			return sp.ParseTime(s)
		},
		"bool": func(s string) (interface{}, error) {
			return cast.ToBoolE(s)
		},
	}
	sp.dateLayouts = []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02T15:04:05.000Z",
		"02-Jan-06",
		"02-Jan-06 15:04:05",
		"02-Jan-06 03:04:05 PM",
		"02-Jan-06 03.04.05.000000 PM",
		"2006-01-02T15:04:05-0700",
		time.RFC3339,
		"2006-01-02T15:04:05", // iso8601 without timezone
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC850,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		"2006-01-02 15:04:05.999999999 -0700 MST", // Time.String()
		"02 Jan 2006",
		"2006-01-02T15:04:05-0700", // RFC3339 without timezone hh:mm colon
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05Z07:00", // RFC3339 without T
		"2006-01-02 15:04:05Z0700",  // RFC3339 without T or timezone hh:mm colon
		"2006-01-02 15:04:05 MST",
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
		"01/02/2006 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006/01/02 15:04:05",
	}
	return &sp
}

// NewDataset return a new dataset
func NewDataset(columns []Column) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: columns,
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	return
}

// NewDatasetFromMap return a new dataset
func NewDatasetFromMap(m map[string]interface{}) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: []Column{},
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	if fieldsI, ok := m["headers"]; ok {
		fields := []string{}
		for _, f := range fieldsI.([]interface{}) {
			fields = append(fields, cast.ToString(f))
		}
		data.Columns = NewColumnsFromFields(fields)
	}
	if rowsI, ok := m["rows"]; ok {
		for _, rowI := range rowsI.([]interface{}) {
			data.Rows = append(data.Rows, rowI.([]interface{}))
		}
	}

	return
}

// NewDatastream return a new datastream
func NewDatastream(columns []Column) (ds *Datastream) {
	return NewDatastreamContext(context.Background(), columns)
}

// NewDatastreamIt with it
func NewDatastreamIt(ctx context.Context, columns []Column, nextFunc func(it *Iterator) bool) (ds *Datastream) {
	ds = NewDatastreamContext(ctx, columns)
	ds.it = &Iterator{
		Row:      make([]interface{}, len(columns)),
		nextFunc: nextFunc,
		Context:  h.NewContext(ctx),
		ds:       ds,
	}
	return
}

// NewDatastreamContext return a new datastream
func NewDatastreamContext(ctx context.Context, columns []Column) (ds *Datastream) {
	ds = &Datastream{
		Rows:       MakeRowsChan(),
		Columns:    columns,
		Context:    h.NewContext(ctx),
		Sp:         NewStreamProcessor(),
		config:     &streamConfig{emptyAsNull: true, header: true},
		deferFuncs: []func(){},
	}
	ds.Sp.ds = ds

	return
}

// SetEmpty sets the ds.Rows channel as empty
func (ds *Datastream) SetEmpty() {
	ds.empty = true
	if ds.df != nil && ds.df.IsEmpty() {
		h.Trace("executing defer functions")
		ds.df.mux.Lock()
		defer ds.df.mux.Unlock()
		for i, f := range ds.df.deferFuncs {
			f()
			ds.df.deferFuncs[i] = func() { return } // in case it gets called again
		}
	}
}

// SetConfig sets the ds.config values
func (ds *Datastream) SetConfig(configMap map[string]string) {
	ds.Sp.SetConfig(configMap)
	ds.config = ds.Sp.config
}

// SetConfig sets the data.Sp.config values
func (sp *StreamProcessor) SetConfig(configMap map[string]string) {
	if sp == nil {
		sp = NewStreamProcessor()
	}

	if configMap["DELIMITER"] != "" {
		sp.config.delimiter = []rune(cast.ToString(configMap["DELIMITER"]))[0]
	}

	if configMap["FILE_MAX_ROWS"] != "" {
		sp.config.fileMaxRows = cast.ToInt64(configMap["FILE_MAX_ROWS"])
	}

	if configMap["HEADER"] != "" {
		sp.config.header = cast.ToBool(configMap["HEADER"])
	} else {
		sp.config.header = true
	}

	if configMap["EMPTY_FIELD_AS_NULL"] != "" {
		sp.config.emptyAsNull = cast.ToBool(configMap["EMPTY_FIELD_AS_NULL"])
	}
	if configMap["NULL_IF"] != "" {
		sp.config.nullIf = configMap["NULL_IF"]
	}
	if configMap["TRIM_SPACE"] != "" {
		sp.config.trimSpace = cast.ToBool(configMap["TRIM_SPACE"])
	}
	if configMap["SKIP_BLANK_LINES"] != "" {
		sp.config.skipBlankLines = cast.ToBool(configMap["SKIP_BLANK_LINES"])
	}
	sp.config.compression = configMap["COMPRESSION"]

	if configMap["DATETIME_FORMAT"] != "" {
		sp.config.datetimeFormat = iso8601ToGoLayout(configMap["DATETIME_FORMAT"])
		// put in first
		sp.dateLayouts = append(
			[]string{sp.config.datetimeFormat},
			sp.dateLayouts...)
	}
}

// IsEmpty returns true is ds.Rows channel as empty
func (ds *Datastream) IsEmpty() bool {
	return ds.empty
}

// Push return the fields of the Data
func (ds *Datastream) Push(row []interface{}) {
	ds.Rows <- row
	ds.Count++
	if !ds.Ready {
		ds.Ready = true
	}
}

// IsClosed is true is ds is closed
func (ds *Datastream) IsClosed() bool {
	return ds.closed
}

// WaitReady waits until datastream is ready
func (ds *Datastream) WaitReady() error {
	for {
		select {
		case <-ds.Context.Ctx.Done():
			break
		default:
		}

		if ds.Context.Err() != nil {
			return ds.Context.Err()
		}

		if ds.Ready {
			break
		}

		// likelyhood of lock lessens. Unsure why
		// It seems that ds.Columns collides
		time.Sleep(50 * time.Millisecond)
	}

	return ds.Context.Ctx.Err()
}

// Defer runs a given function as close of Datastream
func (ds *Datastream) Defer(f func()) {
	ds.deferFuncs = append(ds.deferFuncs, f)
	if ds.closed { // mutex?
		for _, f := range ds.deferFuncs {
			f()
		}
	}
}

// Close closes the datastream
func (ds *Datastream) Close() {
	if !ds.closed {
		close(ds.Rows)

		for _, f := range ds.deferFuncs {
			f()
		}

		if ds.Sp.unrecognizedDate != "" {
			h.Warn("unrecognized date format (%s)", ds.Sp.unrecognizedDate)
		}
		ds.Buffer = nil
	}
	if ds.it != nil {
		ds.it.close()
	}
	ds.closed = true
}

// GetFields return the fields of the Data
func (ds *Datastream) GetFields() []string {
	fields := make([]string, len(ds.Columns))

	for j, column := range ds.Columns {
		fields[j] = column.Name
	}

	return fields
}

// SetFields sets the fields/columns of the Datastream
func (ds *Datastream) SetFields(fields []string) {
	if ds.Columns == nil || len(ds.Columns) != len(fields) {
		ds.Columns = make([]Column, len(fields))
	}

	for i, field := range fields {
		ds.Columns[i].Name = field
		ds.Columns[i].Position = i + 1
	}
}

func (ds *Datastream) setFields(fields []string) {
	if ds.Columns == nil || len(ds.Columns) != len(fields) {
		ds.Columns = make(Columns, len(fields))
	}

	for i, field := range fields {
		ds.Columns[i].Name = field
		ds.Columns[i].Position = i + 1
	}
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func IsDummy(columns []Column) bool {
	return Columns(columns).IsDummy()
}

// NewColumnsFromFields creates Columns from fields
func NewColumnsFromFields(fields []string) (cols Columns) {
	cols = make(Columns, len(fields))
	for i, field := range fields {
		cols[i].Name = field
		cols[i].Position = i + 1
	}
	return
}

// IsDummy returns true if the columns are injected by CreateDummyFields
func (cols Columns) IsDummy() bool {
	for _, col := range cols {
		if !strings.HasPrefix(col.Name, "col_") || len(col.Name) != 8 {
			return false
		}
	}
	return true
}

// Fields return the fields of the Data
func (cols Columns) Fields() []string {
	fields := make([]string, len(cols))
	for j, column := range cols {
		fields[j] = column.Name
	}
	return fields
}

// GetColumn returns the matched Col
func (cols Columns) GetColumn(name string) Column {
	colsMap := map[string]Column{}
	for _, col := range cols {
		colsMap[strings.ToLower(col.Name)] = col
	}
	return colsMap[strings.ToLower(name)]
}

// Normalize returns the normalized field name
func (cols Columns) Normalize(name string) string {
	return cols.GetColumn(name).Name
}

// Collect reads from one or more streams and return a dataset
func Collect(dss ...*Datastream) (data Dataset, err error) {
	var datas []Dataset

	if len(dss) == 0 {
		return
	}

	for i, ds := range dss {
		d, err := ds.Collect(0)
		if err != nil {
			return NewDataset(nil), h.Error(err, "Error collecting ds")
		}

		if i > 0 && len(datas[i-1].Columns) != len(d.Columns) {
			err := fmt.Errorf(
				"columns mismatch, %#v != %#v",
				datas[i-1].GetFields(), d.GetFields(),
			)
			return NewDataset(nil), h.Error(err, "Error appending ds")
		}
		datas = append(datas, d)
	}

	data.Result = nil
	data.Columns = datas[0].Columns
	data.Rows = [][]interface{}{}

	for _, d := range datas {
		data.Rows = append(data.Rows, d.Rows...)
	}

	return
}

// Collect reads a stream and return a dataset
// limit of 0 is unlimited
func (ds *Datastream) Collect(limit int) (Dataset, error) {
	data := NewDataset(ds.Columns)

	// wait for first ds to start streaming.
	// columns/buffer need to be populated
	for {
		if ds.Ready {
			break
		}

		if ds.Err() != nil {
			return data, ds.Err()
		}

		time.Sleep(50 * time.Millisecond)
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]interface{}{}
	limited := false

	for row := range ds.Rows {
		data.Rows = append(data.Rows, row)
		if limit > 0 && len(data.Rows) == limit {
			limited = true
			break
		}
	}

	if !limited {
		ds.SetEmpty()
	}

	return data, ds.Err()
}

// Err return the error if any
func (ds *Datastream) Err() (err error) {
	return ds.Context.Err()
}

// Err return the error if any
func (df *Dataflow) Err() (err error) {
	return df.Context.Err()
}

// IsClosed is true is ds is closed
func (df *Dataflow) IsClosed() bool {
	return df.closed
}

// Defer runs a given function as close of Dataflow
func (df *Dataflow) Defer(f func()) {
	df.mux.Lock()
	defer df.mux.Unlock()
	df.deferFuncs = append(df.deferFuncs, f)
}

// Close closes the df
func (df *Dataflow) Close() {
	if !df.closed {
		close(df.StreamCh)
	}
	df.closed = true
}

// SetEmpty sets all underlying datastreams empty
func (df *Dataflow) SetEmpty() {
	for _, ds := range df.Streams {
		ds.SetEmpty()
	}
}

// IsEmpty returns true is ds.Rows of all channels as empty
func (df *Dataflow) IsEmpty() bool {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		if ds != nil && ds.Ready {
			if !ds.IsEmpty() {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// SetColumns sets the columns
func (df *Dataflow) SetColumns(columns []Column) {
	df.Columns = columns
	// for i := range df.Streams {
	// 	df.Streams[i].Columns = columns
	// 	df.Streams[i].Inferred = true
	// }
}

// SyncColumns syncs two columns together
func SyncColumns(columns1 []Column, columns2 []Column) (columns []Column, err error) {
	if len(columns1) != len(columns2) {
		err = fmt.Errorf("mismatched columns %d != %d", len(columns1), len(columns2))
		return
	}

	columns = columns1

	for i := range columns2 {
		columns[i].Stats.TotalCnt += columns2[i].Stats.TotalCnt
		columns[i].Stats.NullCnt += columns2[i].Stats.NullCnt
		columns[i].Stats.StringCnt += columns2[i].Stats.StringCnt
		columns[i].Stats.IntCnt += columns2[i].Stats.IntCnt
		columns[i].Stats.DecCnt += columns2[i].Stats.DecCnt
		columns[i].Stats.BoolCnt += columns2[i].Stats.BoolCnt
		columns[i].Stats.DateCnt += columns2[i].Stats.DateCnt

		if columns[i].Stats.Min < columns2[i].Stats.Min {
			columns[i].Stats.Min = columns2[i].Stats.Min
		}
		if columns[i].Stats.Max > columns2[i].Stats.Max {
			columns[i].Stats.Max = columns2[i].Stats.Max
		}
		if columns[i].Stats.MaxLen > columns2[i].Stats.MaxLen {
			columns[i].Stats.MaxLen = columns2[i].Stats.MaxLen
		}
		if columns[i].Stats.MaxDecLen > columns2[i].Stats.MaxDecLen {
			columns[i].Stats.MaxDecLen = columns2[i].Stats.MaxDecLen
		}
	}
	return
}

// ResetStats resets the stats
func (df *Dataflow) ResetStats() {
	for i := range df.Columns {
		// set totals to 0
		df.Columns[i].Stats.TotalCnt = 0
		df.Columns[i].Stats.NullCnt = 0
		df.Columns[i].Stats.StringCnt = 0
		df.Columns[i].Stats.IntCnt = 0
		df.Columns[i].Stats.DecCnt = 0
		df.Columns[i].Stats.BoolCnt = 0
		df.Columns[i].Stats.DateCnt = 0
		df.Columns[i].Stats.Checksum = 0
	}
}

// SyncStats sync stream processor stats aggregated to the df.Columns
func (df *Dataflow) SyncStats() {
	df.ResetStats()

	for _, ds := range df.Streams {
		for i, colStat := range ds.Sp.colStats {
			if i+1 > len(df.Columns) {
				h.Debug("index %d is outside len of array (%d) in SyncStats", i, len(df.Columns))
				continue
			}
			df.Columns[i].Stats.TotalCnt = df.Columns[i].Stats.TotalCnt + colStat.TotalCnt
			df.Columns[i].Stats.NullCnt = df.Columns[i].Stats.NullCnt + colStat.NullCnt
			df.Columns[i].Stats.StringCnt = df.Columns[i].Stats.StringCnt + colStat.StringCnt
			df.Columns[i].Stats.IntCnt = df.Columns[i].Stats.IntCnt + colStat.IntCnt
			df.Columns[i].Stats.DecCnt = df.Columns[i].Stats.DecCnt + colStat.DecCnt
			df.Columns[i].Stats.BoolCnt = df.Columns[i].Stats.BoolCnt + colStat.BoolCnt
			df.Columns[i].Stats.DateCnt = df.Columns[i].Stats.DateCnt + colStat.DateCnt
			df.Columns[i].Stats.Checksum = df.Columns[i].Stats.Checksum + colStat.Checksum

			if colStat.Min < df.Columns[i].Stats.Min {
				df.Columns[i].Stats.Min = colStat.Min
			}
			if colStat.Max > df.Columns[i].Stats.Max {
				df.Columns[i].Stats.Max = colStat.Max
			}
			if colStat.MaxLen > df.Columns[i].Stats.MaxLen {
				df.Columns[i].Stats.MaxLen = colStat.MaxLen
			}
			if colStat.MaxDecLen > df.Columns[i].Stats.MaxDecLen {
				df.Columns[i].Stats.MaxDecLen = colStat.MaxDecLen
			}
		}
	}

	if !df.Inferred {
		df.Columns = InferFromStats(df.Columns, false, false)
		df.Inferred = true
	}
}

// Count returns the aggregate count
func (df *Dataflow) Count() (cnt uint64) {
	if df != nil && df.Ready {
		for _, ds := range df.Streams {
			if ds.Ready {
				cnt += ds.Count
			}
		}
	}
	return
}

// AddBytes add bytes as processed
func (df *Dataflow) AddBytes(b int64) {
	df.Bytes = df.Bytes + b
}

// Size is the number of streams
func (df *Dataflow) Size() int {
	return len(df.Streams)
}

// PushStreams waits until each datastream is ready, then adds them to the dataflow. Should be launched as a goroutine
func (df *Dataflow) PushStreams(dss ...*Datastream) {

	df.mux.Lock()
	df.Streams = append(df.Streams, dss...)
	df.mux.Unlock()

	pushed := map[int]bool{}
	pushCnt := 0
	for {
		for i, ds := range dss {
			if pushed[i] || df.closed {
				continue
			}

			if df.Err() != nil {
				df.Close()
				return
			}

			if ds.Err() != nil {
				df.Context.CaptureErr(ds.Err())
				df.Close()
				return
			}

			select {
			case <-df.Context.Ctx.Done():
				df.Close()
				return
			case <-ds.Context.Ctx.Done():
				df.Close()
				return
			default:
				// wait for first ds to start streaming.
				// columns/buffer need to be populated
				if ds.Ready {
					ds.df = df
					if !df.Ready {
						df.Columns = ds.Columns
						df.Buffer = ds.Buffer
						df.Ready = true
					}
					select {
					case df.StreamCh <- ds:
						df.AddBytes(ds.Bytes)
						pushed[i] = true
						pushCnt++
						h.Trace("pushed dss %d", i)
					default:
					}
				}
			}

			// likelyhood of lock lessens. Unsure why
			// It seems that ds.Columns collides
			time.Sleep(50 * time.Millisecond)
		}

		if pushCnt == len(dss) || df.closed {
			break
		}
	}
}

// WaitReady waits until dataflow is ready
func (df *Dataflow) WaitReady() error {
	// wait for first ds to start streaming.
	// columns need to be populated
	for {
		if df.Ready {
			break
		}

		if df.Context.Err() != nil {
			return df.Context.Err()
		}

		// likelyhood of lock lessens. Unsure why
		// It seems that df.Columns collides
		time.Sleep(50 * time.Millisecond)
	}

	return nil
}

func (it *Iterator) close() {
	it.Closed = true
}

func (it *Iterator) next() bool {
	select {
	case <-it.Context.Ctx.Done():
		return false
	default:
		next := it.nextFunc(it)
		if next {
			it.Counter++
		}
		return next
	}
}

// Start generates the stream
// Should cycle the Iter Func until done
func (ds *Datastream) Start() (err error) {
	if ds.it == nil {
		err = fmt.Errorf("iterator not defined")
		return h.Error(err, "need to define iterator")
	}

	if !ds.Inferred {
		for ds.it.next() {
			select {
			case <-ds.Context.Ctx.Done():
				if ds.Context.Err() != nil {
					err = h.Error(ds.Context.Err())
					return
				}
				break
			case <-ds.it.Context.Ctx.Done():
				if ds.it.Context.Err() != nil {
					err = h.Error(ds.it.Context.Err())
					ds.Context.CaptureErr(err, "Failed to scan")
					return
				}
				break
			default:
				row := ds.Sp.ProcessRow(ds.it.Row)
				ds.Buffer = append(ds.Buffer, row)
				if ds.it.Counter == cast.ToUint64(sampleSize) {
					break
				}
			}
		}

		// infer types
		sampleData := NewDataset(ds.Columns)
		sampleData.Rows = ds.Buffer
		sampleData.NoTrace = ds.NoTrace
		sampleData.SafeInference = ds.SafeInference
		sampleData.Sp.dateLayouts = ds.Sp.dateLayouts
		sampleData.InferColumnTypes()
		ds.Columns = sampleData.Columns
	}

	if ds.it.Context.Err() != nil {
		err = h.Error(ds.it.Context.Err(), "error in getting rows")
		return
	}

	go func() {
		var err error
		defer ds.Close()

		for _, row := range ds.Buffer {
			row = ds.Sp.CastRow(row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}
			ds.Push(row)
		}

		ds.Ready = true

		row := make([]interface{}, len(ds.Columns))
		rowPtrs := make([]interface{}, len(ds.Columns))
		for i := range row {
			// cast the interface place holders
			row[i] = ds.Sp.CastType(row[i], ds.Columns[i].Type)
			rowPtrs[i] = &row[i]
		}

		defer func() {
			// if any error occurs during iteration
			if ds.it.Context.Err() != nil {
				ds.Context.CaptureErr(h.Error(ds.it.Context.Err(), "error during iteration"))
			}
		}()

		for ds.it.next() {
			row = ds.Sp.CastRow(ds.it.Row, ds.Columns)
			if ds.config.skipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
				continue
			}

			select {
			case <-ds.Context.Ctx.Done():
				break
			case <-ds.it.Context.Ctx.Done():
				if ds.it.Context.Err() != nil {
					err = h.Error(ds.it.Context.Err())
					ds.Context.CaptureErr(err, "Failed to scan")
				}
				break
			default:
				ds.Push(row)
			}
		}
		if !ds.NoTrace {
			h.Trace("Got %d rows", ds.it.Counter)
		}
	}()

	return
}

// ConsumeReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeReader(reader io.Reader) (err error) {
	c := CSV{Reader: reader, NoHeader: !ds.config.header}

	r, err := c.getReader()
	if err != nil {
		err = h.Error(err, "could not get reader")
		ds.Context.CaptureErr(err)
		return err
	}

	row0, err := r.Read()
	if err == io.EOF {
		h.Warn("csv stream provided is empty")
		ds.Ready = true
		ds.Close()
		return nil
	} else if err != nil {
		err = h.Error(err, "could not parse header line")
		ds.Context.CaptureErr(err)
		return err
	}
	ds.SetFields(CleanHeaderRow(row0))

	nextFunc := func(it *Iterator) bool {

		row, err := r.Read()
		if err == io.EOF {
			it.ds.AddBytes(c.bytes)
			c.File.Close()
			return false
		} else if err != nil {
			it.Context.CaptureErr(h.Error(err, "Error reading file"))
			return false
		}

		it.Row = make([]interface{}, len(it.ds.Columns))
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

	ds.it = &Iterator{
		Row:      make([]interface{}, len(ds.Columns)),
		nextFunc: nextFunc,
		Context:  h.NewContext(ds.Context.Ctx),
		ds:       ds,
	}

	err = ds.Start()
	if err != nil {
		return h.Error(err, "could start datastream")
	}

	return
}

// ResetStats resets the stats
func (ds *Datastream) ResetStats() {
	for i := range ds.Columns {
		// set totals to 0
		ds.Columns[i].Stats.TotalCnt = 0
		ds.Columns[i].Stats.NullCnt = 0
		ds.Columns[i].Stats.StringCnt = 0
		ds.Columns[i].Stats.IntCnt = 0
		ds.Columns[i].Stats.DecCnt = 0
		ds.Columns[i].Stats.BoolCnt = 0
		ds.Columns[i].Stats.DateCnt = 0
		ds.Columns[i].Stats.Checksum = 0
	}
}

// AddBytes add bytes as processed
func (ds *Datastream) AddBytes(b int64) {
	ds.Bytes = ds.Bytes + b
}

// Records return rows of maps
func (ds *Datastream) Records() <-chan map[string]interface{} {
	chnl := make(chan map[string]interface{}, 1000)
	ds.WaitReady()
	go func() {
		defer close(chnl)

		for row := range ds.Rows {
			// get records
			rec := map[string]interface{}{}

			for i, field := range ds.GetFields() {
				rec[strings.ToLower(field)] = row[i]
			}
			chnl <- rec
		}
	}()

	return chnl
}

// Shape changes the column types as needed, to the provided columns var
// It will cast the already wrongly casted rows, and not recast the
// correctly casted rows
func (ds *Datastream) Shape(columns []Column) (nDs *Datastream, err error) {
	if len(columns) != len(ds.Columns) {
		err = h.Error(fmt.Errorf("number of columns do not match"))
		return ds, err
	}

	// we need to recast up to this marker
	counterMarker := ds.Count + 10

	diffCols := false
	for i := range columns {
		if columns[i].Type != ds.Columns[i].Type {
			diffCols = true
			ds.Columns[i].Type = columns[i].Type
		}
	}

	if !diffCols {
		return ds, nil
	}

	h.Trace("shaping ds...")
	nDs = NewDatastreamContext(ds.Context.Ctx, columns)

	go func() {
		defer close(nDs.Rows)
		nDs.Ready = true
		for row := range ds.Rows {
			if nDs.Count <= counterMarker {
				row = nDs.Sp.CastRow(row, nDs.Columns)
			}

			select {
			case <-nDs.Context.Ctx.Done():
				break
			default:
				nDs.Rows <- row
				nDs.Count++
			}
		}
		ds.SetEmpty()
	}()

	return
}

// Map applies the provided function to every row
// and returns the result
func (ds *Datastream) Map(transf func([]interface{}) []interface{}) (nDs *Datastream) {

	nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)

	go func() {
		defer close(nDs.Rows)
		for row := range ds.Rows {
			select {
			case <-nDs.Context.Ctx.Done():
				break
			default:
				nDs.Rows <- transf(row)
				nDs.Count++
			}
		}
		ds.SetEmpty()
	}()

	return
}

// MapParallel applies the provided function to every row in parallel and returns the result. Order is not maintained.
func (ds *Datastream) MapParallel(transf func([]interface{}) []interface{}, numWorkers int) (nDs *Datastream) {
	var wg sync.WaitGroup
	nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)

	transform := func(wDs *Datastream, wg *sync.WaitGroup) {
		defer wg.Done()
		for row := range wDs.Rows {
			select {
			case <-nDs.Context.Ctx.Done():
				break
			case <-wDs.Context.Ctx.Done():
				break
			default:
				nDs.Rows <- transf(row)
				nDs.Count++
			}
		}
	}

	wStreams := map[int]*Datastream{}
	for i := 0; i < numWorkers; i++ {
		wStream := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		wStreams[i] = wStream

		wg.Add(1)
		go transform(wStream, &wg)
	}

	go func() {
		wi := 0
		for row := range ds.Rows {
			select {
			case <-nDs.Context.Ctx.Done():
				break
			default:
				wStreams[wi].Push(row)
			}
			if wi == numWorkers-1 {
				wi = -1 // cycle through workers
			}
			wi++
		}

		for i := 0; i < numWorkers; i++ {
			close(wStreams[i].Rows)
			wStreams[i].closed = true
		}

		wg.Wait()
		close(nDs.Rows)
		nDs.closed = true
	}()

	return
}

// GetFields return the fields of the Data
func (data *Dataset) GetFields() []string {
	fields := make([]string, len(data.Columns))

	for j, column := range data.Columns {
		fields[j] = strings.ToLower(column.Name)
	}

	return fields
}

// SetFields sets the fields/columns of the Datastream
func (data *Dataset) SetFields(fields []string) {
	if data.Columns == nil || len(data.Columns) != len(fields) {
		data.Columns = make([]Column, len(fields))
	}

	for i, field := range fields {
		data.Columns[i].Name = field
		data.Columns[i].Position = i + 1
	}
}

// Append appends a new row
func (data *Dataset) Append(row []interface{}) {
	data.Rows = append(data.Rows, row)
}

// Stream returns a datastream of the dataset
func (data *Dataset) Stream() *Datastream {
	ds := NewDatastream(data.Columns)

	go func() {
		ds.Ready = true
		defer ds.Close()
		for _, row := range data.Rows {
			select {
			case <-ds.Context.Ctx.Done():
				break
			default:
				ds.Push(row)
			}
		}
		ds.SetEmpty()
	}()

	return ds
}

// FirstVal returns the first value from the first row
func (data *Dataset) FirstVal() interface{} {
	if len(data.Rows) > 0 && len(data.Rows[0]) > 0 {
		return data.Rows[0][0]
	}
	return nil
}

// FirstRow returns the first row
func (data *Dataset) FirstRow() []interface{} {
	if len(data.Rows) > 0 {
		return data.Rows[0]
	}
	return nil
}

// ColValues returns the values of a one column as array
func (data *Dataset) ColValues(col int) []interface{} {
	vals := make([]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = row[col]
	}
	return vals
}

// ColValuesStr returns the values of a one column as array or string
func (data *Dataset) ColValuesStr(col int) []string {
	vals := make([]string, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = cast.ToString(row[col])
	}
	return vals

}

// Records return rows of maps
func (data *Dataset) Records() []map[string]interface{} {
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields() {
			rec[field] = row[j]
		}
		records[i] = rec
	}
	return records
}

// ToJSONMap converst to a JSON object
func (data *Dataset) ToJSONMap() map[string]interface{} {
	return h.M("headers", data.GetFields(), "rows", data.Rows)
}

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() {
	var columns []Column

	if len(data.Rows) == 0 {
		h.Debug("skipping InferColumnTypes")
		return
	}

	for i, field := range data.GetFields() {
		columns = append(columns, Column{
			Name:     field,
			Position: i + 1,
			Type:     "string",
			Stats:    ColumnStats{},
		})
	}

	// h.Trace("InferColumnTypes with sample size %d", sampleSize)
	for i, row := range data.Rows {
		if i >= sampleSize {
			break
		}

		for j, val := range row {
			val = data.Sp.ParseString(strings.TrimSpace(cast.ToString(val)), j)
			columns[j].Stats.TotalCnt++

			l := len(cast.ToString(val))
			if l > columns[j].Stats.MaxLen {
				columns[j].Stats.MaxLen = l
			}
			if l < columns[j].Stats.MinLen {
				columns[j].Stats.MinLen = l
			}

			if val == nil || l == 0 {
				columns[j].Stats.NullCnt++
				continue
			}

			switch v := val.(type) {
			case time.Time:
				columns[j].Stats.DateCnt++
			case nil:
				columns[j].Stats.NullCnt++
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				columns[j].Stats.IntCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}
			case float32, float64:
				columns[j].Stats.DecCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}

				valStr := cast.ToString(val)
				if strings.Contains(valStr, ".") {
					decLen := len(strings.Split(cast.ToString(val), ".")[1])
					if decLen > columns[j].Stats.MaxDecLen {
						columns[j].Stats.MaxDecLen = decLen
					}
				}

			case bool:
				columns[j].Stats.BoolCnt++
			case string, []uint8:
				columns[j].Stats.StringCnt++

			default:
				_ = v
			}
		}
	}

	data.Columns = InferFromStats(columns, data.SafeInference, data.NoTrace)
	data.Inferred = true
}

// InferFromStats using the stats to infer data types
func InferFromStats(columns []Column, safe bool, noTrace bool) []Column {
	for j := range columns {
		if columns[j].Stats.StringCnt > 0 || columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.MaxLen > 255 {
				columns[j].Type = "text"
			} else {
				columns[j].Type = "string"
			}
			if safe {
				columns[j].Type = "text" // max out
			}
			columns[j].goType = reflect.TypeOf("string")
		} else if columns[j].Stats.BoolCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "bool"
			columns[j].goType = reflect.TypeOf(true)
		} else if columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			if columns[j].Stats.Min*10 < -2147483648 || columns[j].Stats.Max*10 > 2147483647 {
				columns[j].Type = "bigint"
			} else {
				columns[j].Type = "integer"
			}
			if safe {
				columns[j].Type = "bigint" // max out
			}
			columns[j].goType = reflect.TypeOf(int64(0))
		} else if columns[j].Stats.DateCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "datetime"
			columns[j].goType = reflect.TypeOf(time.Now())
		} else if columns[j].Stats.DecCnt+columns[j].Stats.IntCnt+columns[j].Stats.NullCnt == columns[j].Stats.TotalCnt {
			columns[j].Type = "decimal"
			columns[j].goType = reflect.TypeOf(float64(0.0))
		}
		if !noTrace {
			h.Trace(
				"%s - %s (maxLen: %d, nullCnt: %d, totCnt: %d, strCnt: %d, dtCnt: %d, intCnt: %d, decCnt: %d)",
				columns[j].Name, columns[j].Type,
				columns[j].Stats.MaxLen, columns[j].Stats.NullCnt,
				columns[j].Stats.TotalCnt, columns[j].Stats.StringCnt,
				columns[j].Stats.DateCnt, columns[j].Stats.IntCnt,
				columns[j].Stats.DecCnt,
			)
		}
	}
	return columns
}

// NewCsvBytesChnl returns a channel yield chunk of bytes of csv
func (ds *Datastream) NewCsvBytesChnl(chunkRowSize int) (dataChn chan *[]byte) {
	dataChn = make(chan *[]byte, 100)

	go func() {
		defer close(dataChn)
		for {
			reader := ds.NewCsvReader(chunkRowSize)
			data, err := ioutil.ReadAll(reader)
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "Error ioutil.ReadAll(reader)"))
				ds.Context.Cancel()
				return
			}
			dataChn <- &data
		}
	}()
	return dataChn
}

// NewCsvBufferReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvBufferReader(limit int) *bytes.Reader {
	reader := ds.NewCsvReader(limit)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		ds.Context.CaptureErr(h.Error(err, "Error ioutil.ReadAll(reader)"))
	}
	return bytes.NewReader(data)
}

// NewCsvBufferReaderChnl provides a channel of readers as the limit is reached
// data is read in memory, whereas NewCsvReaderChnl does not hold in memory
func (ds *Datastream) NewCsvBufferReaderChnl(limit int) (readerChn chan *bytes.Reader) {

	readerChn = make(chan *bytes.Reader) // not buffered, will block if receiver isn't ready

	go func() {
		defer close(readerChn)
		for !ds.IsEmpty() {
			readerChn <- ds.NewCsvBufferReader(limit)
		}
	}()

	return readerChn
}

// NewCsvReaderChnl provides a channel of readers as the limit is reached
func (ds *Datastream) NewCsvReaderChnl(limit int) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	pipeR, pipeW := io.Pipe()

	readerChn <- pipeR

	go func() {
		defer close(readerChn)

		c := 0 // local counter
		w := csv.NewWriter(pipeW)

		err := w.Write(ds.GetFields())
		if err != nil {
			ds.Context.CaptureErr(h.Error(err, "error writing header"))
			ds.Context.Cancel()
			pipeW.Close()
			return
		}

		for row0 := range ds.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
			}
			err := w.Write(row)
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "error writing row"))
				ds.Context.Cancel()
				pipeW.Close()
				return
			}
			w.Flush()

			if limit > 0 && c == limit {
				pipeW.Close() // close the prior reader?

				// new reader
				c = 0
				pipeR, pipeW = io.Pipe()
				w = csv.NewWriter(pipeW)

				err = w.Write(ds.GetFields())
				if err != nil {
					ds.Context.CaptureErr(h.Error(err, "error writing header"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}
				readerChn <- pipeR
			}
		}
		pipeW.Close()
	}()

	return readerChn
}

// NewCsvReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvReader(limit int) *io.PipeReader {
	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()
		c := 0 // local counter
		w := csv.NewWriter(pipeW)

		// header row to lower case
		fields := []string{}
		for _, field := range ds.GetFields() {
			fields = append(fields, strings.ToLower(field))
		}

		if ds.config.header {
			err := w.Write(fields)
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "error writing header"))
				ds.Context.Cancel()
			}
		}

		limited := false // need this to know if channel is emptied
		for row0 := range ds.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
			}
			err := w.Write(row)
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "error writing row"))
				ds.Context.Cancel()
				break
			}
			w.Flush()

			if limit > 0 && c == limit {
				limited = true
				break // close reader if row limit is reached
			}
		}

		if !limited {
			ds.SetEmpty()
		}

	}()

	return pipeR
}

// MakeDataFlow create a dataflow from datastreams
func MakeDataFlow(dss ...*Datastream) (df *Dataflow, err error) {

	if len(dss) == 0 {
		err = fmt.Errorf("Provided 0 datastreams for: %#v", dss)
		return
	}

	df = NewDataflow()

	go func() {
		defer df.Close()
		df.PushStreams(dss...)
	}()

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	return df, nil
}

// MergeDataflow merges the dataflow streams into one
func MergeDataflow(df *Dataflow) (ds *Datastream) {

	ds = NewDatastream(df.Columns)

	go func() {
		ds.Ready = true
		defer ds.Close()
		for ds0 := range df.StreamCh {
			for row := range ds0.Rows {
				select {
				case <-df.Context.Ctx.Done():
					break
				case <-ds.Context.Ctx.Done():
					break
				default:
					ds.Push(row)
				}
			}
		}
	}()

	return ds
}

// CastVal  casts the type of an interface based on its value
// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirect returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil).
func (sp *StreamProcessor) indirect(a interface{}) interface{} {
	if a == nil {
		return nil
	}
	if t := reflect.TypeOf(a); t.Kind() != reflect.Ptr {
		// Avoid creating a reflect.Value if it's not a pointer.
		return a
	}
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func (sp *StreamProcessor) toFloat64E(i interface{}) (float64, error) {
	i = sp.indirect(i)

	switch s := i.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case []uint8:
		return cast.ToFloat64(cast.ToString(s)), nil
	case string:
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unable to cast %#v of type %T to float64", i, i)
	}
}

// CastType casts the type of an interface
// CastType is used to cast the interface place holders?
func (sp *StreamProcessor) CastType(val interface{}, typ string) interface{} {
	var nVal interface{}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// nVal = cast.ToFloat64(val)
		nVal = val
	case "bool":
		// nVal = cast.ToBool(val)
		nVal = val
	case "datetime", "date", "timestamp":
		nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	return nVal
}

// GetType returns the type of an interface
func (sp *StreamProcessor) GetType(val interface{}) (typ string) {

	switch v := val.(type) {
	case time.Time:
		typ = "timestamp"
	case int8, int16, uint8, uint16:
		typ = "smallint"
	case int, int32, uint, uint32:
		typ = "integer"
	case int64, uint64:
		typ = "bigint"
	case float32, float64:
		typ = "decimal"
	case bool:
		typ = "bool"
	case string, []uint8:
		typ = "string"
	default:
		_ = v
		typ = "string"
	}
	return
}

// CastVal casts values with stats collection
// which degrades performance by ~10%
// go test -benchmem -run='^$ github.com/slingdata-io/sling/core/iop' -bench '^BenchmarkProcessVal'
func (sp *StreamProcessor) CastVal(i int, val interface{}, typ string) interface{} {
	cs, ok := sp.colStats[i]
	if !ok {
		sp.colStats[i] = &ColumnStats{}
		cs = sp.colStats[i]
	}

	var nVal interface{}
	var sVal string

	switch v := val.(type) {
	case godror.Number:
		val = sp.ParseString(cast.ToString(val), i)
	case []uint8:
		val = cast.ToString(val)
	case nil:
		cs.TotalCnt++
		cs.NullCnt++
		sp.rowBlankValCnt++
		return nil
	case string:
		sVal = val.(string)
		if sp.config.trimSpace {
			sVal = strings.TrimSpace(sVal)
			val = sVal
		}
		if sVal == "" {
			sp.rowBlankValCnt++
			if sp.config.emptyAsNull || !sp.ds.Columns[i].IsString() {
				cs.TotalCnt++
				cs.NullCnt++
				return nil
			}
		} else if sp.config.nullIf == sVal {
			cs.TotalCnt++
			cs.NullCnt++
			return nil
		}
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes", "":
		sVal = cast.ToString(val)
		if len(sVal) > cs.MaxLen {
			cs.MaxLen = len(sVal)
		}

		if cs.TotalCnt > 0 && cs.NullCnt == cs.TotalCnt && sp.ds != nil {
			// this is an attempt to cast correctly "uncasted" columns
			// (defaulting at string). This will not work in most db insert cases,
			// as the ds.Shape() function will change it back to the "string" type,
			// to match the target table column type. This takes priority.
			nVal = sp.ParseString(sVal)
			sp.ds.Columns[i].Type = sp.GetType(nVal)
			if !sp.ds.Columns[i].IsString() { // so we don't loop
				return sp.CastVal(i, nVal, sp.ds.Columns[i].Type)
			}
			cs.StringCnt++
			cs.Checksum = cs.Checksum + uint64(len(sVal))
			nVal = sVal
		} else {
			cs.StringCnt++
			cs.Checksum = cs.Checksum + uint64(len(sVal))
			nVal = sVal
		}
	case "smallint":
		iVal := cast.ToInt(val)
		if int64(iVal) > cs.Max {
			cs.Max = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			cs.Checksum = cs.Checksum + uint64(-iVal)
		} else {
			cs.Checksum = cs.Checksum + uint64(iVal)
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		nVal = iVal
	case "integer", "bigint":
		iVal := cast.ToInt64(val)
		if iVal > cs.Max {
			cs.Max = iVal
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			cs.Checksum = cs.Checksum + uint64(-iVal)
		} else {
			cs.Checksum = cs.Checksum + uint64(iVal)
		}
		nVal = iVal
	case "decimal", "float":
		fVal, _ := sp.toFloat64E(val)
		if int64(fVal) > cs.Max {
			cs.Max = int64(fVal)
		}
		if int64(fVal) < cs.Min {
			cs.Min = int64(fVal)
		}
		cs.DecCnt++
		if fVal < 0 {
			cs.Checksum = cs.Checksum + cast.ToUint64(-fVal)
		} else {
			cs.Checksum = cs.Checksum + cast.ToUint64(fVal)
		}
		// max 9 decimals for bigquery compatibility
		if sp.config.decimalDiv != 0 {
			nVal = math.Round(fVal*sp.config.decimalDiv) / sp.config.decimalDiv
		} else {
			nVal = val // use string to keep accuracy
		}
	case "bool":
		cs.BoolCnt++
		nVal = cast.ToBool(val)
		if nVal.(bool) {
			cs.Checksum++
		}
	case "datetime", "date", "timestamp":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			// sp.unrecognizedDate = h.F(
			// 	"N: %d, ind: %d, val: %s", sp.N, i, cast.ToString(val),
			// )
			// sp.warn = true
			nVal = val // keep string
			cs.StringCnt++
		} else if dVal.IsZero() {
			nVal = nil
			cs.NullCnt++
			sp.rowBlankValCnt++
		} else {
			nVal = dVal
			cs.DateCnt++
			cs.Checksum = cs.Checksum + uint64(dVal.Unix())
		}
	}
	cs.TotalCnt++
	return nVal
}

// CastToString to string
func (sp *StreamProcessor) oldCastToString(i int, val interface{}, valType ...string) string {
	switch v := val.(type) {
	case time.Time:
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.Format("2006-01-02 15:04:05.000")
	default:
		_ = v
		return cast.ToString(val)
	}
}

// CastToString to string
func (sp *StreamProcessor) CastToString(i int, val interface{}, valType ...string) string {
	typ := ""
	switch v := val.(type) {
	case time.Time:
		typ = "datetime"
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch typ {
	case "decimal", "float":
		if RemoveTrailingDecZeros {
			// attempt to remove trailing zeros, but is 10 times slower
			return sp.decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
		}
		return cast.ToString(val)
	case "datetime", "date", "timestamp":
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		}
		return tVal.Format("2006-01-02 15:04:05.000")
	default:
		return cast.ToString(val)
	}
}

// CastValWithoutStats casts the value without counting stats
func (sp *StreamProcessor) CastValWithoutStats(i int, val interface{}, typ string) interface{} {
	var nVal interface{}
	if nil == val {
		return nil
	}

	switch v := val.(type) {
	case godror.Number:
		val = sp.ParseString(cast.ToString(val), i)
	case []uint8:
		val = cast.ToString(val)
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// max 9 decimals for bigquery compatibility
		// nVal = math.Round(fVal*1000000000) / 1000000000
		nVal = val // use string to keep accuracy
	case "bool":
		nVal = cast.ToBool(val)
	case "datetime", "date", "timestamp":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			nVal = val // keep string
		} else if dVal.IsZero() {
			nVal = nil
		} else {
			nVal = dVal
		}
	default:
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	}

	return nVal
}

// CastToTime converts interface to time
func (sp *StreamProcessor) CastToTime(i interface{}) (t time.Time, err error) {
	i = sp.indirect(i)
	switch v := i.(type) {
	case nil:
		return
	case time.Time:
		return v, nil
	case string:
		return sp.ParseTime(i.(string))
	default:
		return cast.ToTimeE(i)
	}
}

// ParseTime parses a date string and returns time.Time
func (sp *StreamProcessor) ParseTime(i interface{}) (t time.Time, err error) {
	s := cast.ToString(i)
	if s == "" {
		return t, fmt.Errorf("blank val")
	}

	// date layouts to try out
	for _, layout := range sp.dateLayouts {
		// use cache to decrease parsing computation next iteration
		if sp.dateLayoutCache != "" {
			t, err = time.Parse(sp.dateLayoutCache, s)
			if err == nil {
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			sp.dateLayoutCache = layout
			return
		}
	}
	return
}

// ParseString return an interface
// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"
func (sp *StreamProcessor) ParseString(s string, jj ...int) interface{} {
	if s == "" {
		return nil
	}

	j := -1
	if len(jj) > 0 {
		j = jj[0]
	}

	stringTypeCache := sp.stringTypeCache[j]

	if stringTypeCache != "" {
		i, err := sp.parseFuncs[stringTypeCache](s)
		if err == nil {
			return i
		}
	}

	// int
	i, err := sp.parseFuncs["int"](s)
	if err == nil {
		sp.stringTypeCache[j] = "int"
		return i
	}

	// float
	f, err := sp.parseFuncs["float"](s)
	if err == nil {
		sp.stringTypeCache[j] = "float"
		return f
	}

	t, err := sp.parseFuncs["time"](s)
	if err == nil {
		sp.stringTypeCache[j] = "time"
		return t
	}

	// boolean
	// causes issues in SQLite and Oracle
	// b, err := sp.parseFuncs["bool"](s)
	// if err == nil {
	// 	sp.stringTypeCache[j] = "bool"
	// 	return b
	// }

	return s
}

// ProcessVal processes a value
func (sp *StreamProcessor) ProcessVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case godror.Number:
		nVal = sp.ParseString(cast.ToString(val))
	case []uint8:
		nVal = cast.ToString(val)
	default:
		nVal = val
		_ = v
	}
	return nVal

}

// ParseVal parses the value into its appropriate type
func (sp *StreamProcessor) ParseVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case time.Time:
		nVal = cast.ToTime(val)
	case nil:
		nVal = val
	case int:
		nVal = cast.ToInt64(val)
	case int8:
		nVal = cast.ToInt64(val)
	case int16:
		nVal = cast.ToInt64(val)
	case int32:
		nVal = cast.ToInt64(val)
	case int64:
		nVal = cast.ToInt64(val)
	case float32:
		nVal = cast.ToFloat32(val)
	case float64:
		nVal = cast.ToFloat64(val)
	case godror.Number:
		nVal = sp.ParseString(cast.ToString(val))
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		nVal = sp.ParseString(cast.ToString(val))
	default:
		nVal = sp.ParseString(cast.ToString(val))
		_ = v
		// fmt.Printf("%T\n", val)
	}
	return nVal
}

// CastRow casts each value of a row
func (sp *StreamProcessor) CastRow(row []interface{}, columns []Column) []interface{} {
	sp.N++
	// Ensure usable types
	sp.rowBlankValCnt = 0
	for i, val := range row {
		// fmt.Printf("| (%s) %#v", columns[i].Type, val)
		row[i] = sp.CastVal(i, val, columns[i].Type)
	}

	// debug a row, prev
	if sp.warn {
		h.Trace("%s -> %#v", sp.unrecognizedDate, row)
		sp.warn = false
	}

	return row
}

// ProcessRow processes a row
func (sp *StreamProcessor) ProcessRow(row []interface{}) []interface{} {
	// Ensure usable types
	for i, val := range row {
		row[i] = sp.ProcessVal(val)
	}
	return row
}

func (sp *StreamProcessor) processRec(rec map[string]interface{}) map[string]interface{} {
	// Ensure usable types
	for i, val := range rec {
		rec[i] = sp.ProcessVal(val)
	}
	return rec
}

func (sp *StreamProcessor) castRowInterf(row []interface{}) []interface{} {
	return row
}

// IsString returns whether the column is a string
func (col *Column) IsString() bool {
	switch col.Type {
	case "string", "text", "json", "time", "bytes", "":
		return true
	}
	return false
}

// IsInteger returns whether the column is an integer
func (col *Column) IsInteger() bool {
	return col.Type == "smallint" || col.Type == "bigint" || col.Type == "integer"
}

// IsDecimal returns whether the column is a decimal
func (col *Column) IsDecimal() bool {
	return col.Type == "float" || col.Type == "decimal"
}

// IsNumber returns whether the column is a decimal or an integer
func (col *Column) IsNumber() bool {
	return col.IsInteger() || col.IsDecimal()
}

// IsBool returns whether the column is a boolean
func (col *Column) IsBool() bool {
	return col.Type == "bool"
}

// IsDatetime returns whether the column is a datetime object
func (col *Column) IsDatetime() bool {
	return col.Type == "datetime" || col.Type == "date" || col.Type == "timestamp"
}

type ConnType int

const (
	ConnTypeNone ConnType = iota
	connTypeFileStart
	ConnTypeFileLocal
	ConnTypeFileHDFS
	ConnTypeFileS3
	ConnTypeFileAzure
	ConnTypeFileGoogle
	ConnTypeFileSftp
	ConnTypeFileHTTP
	connTypeFileEnd

	connTypeDbStart
	ConnTypeDbPostgres
	ConnTypeDbRedshift
	ConnTypeDbMySQL
	ConnTypeDbOracle
	ConnTypeDbBigQuery
	ConnTypeDbSnowflake
	ConnTypeDbSQLite
	ConnTypeDbSQLServer
	ConnTypeDbAzure
	connTypeDbEnd

	connTypeAPIStart
	ConnTypeAPIGit
	ConnTypeAPIGithub
	connTypeAPIEnd
)

// DataConn represents a data connection with properties
type DataConn struct {
	ID   string
	URL  string
	Vars map[string]interface{}
}

// NewDataConnFromMap loads a DataConn from a Map
func NewDataConnFromMap(m map[string]interface{}) (dc *DataConn) {
	dc = &DataConn{
		ID:   cast.ToString(m["id"]),
		URL:  cast.ToString(m["url"]),
		Vars: map[string]interface{}{},
	}
	vars, ok := m["vars"].(map[string]interface{})
	if ok {
		dc.Vars = vars
	}

	dc.SetFromEnv()
	return dc
}

// SetFromEnv set values from environment
func (dc *DataConn) SetFromEnv() {
	if newURL := os.Getenv(strings.TrimLeft(dc.URL, "$")); newURL != "" {
		dc.URL = newURL
	}

	for k, v := range dc.Vars {
		val := cast.ToString(v)
		if strings.HasPrefix(val, "$") {
			varKey := strings.TrimLeft(val, "$")
			if newVal := os.Getenv(varKey); newVal != "" {
				dc.Vars[k] = newVal
			} else {
				h.Warn("No env var value found for %s", val)
			}
		}
	}
}

// VarsS returns vars as map[string]string
func (dc *DataConn) VarsS() map[string]string {
	vars := map[string]string{}
	for k, v := range dc.Vars {
		vars[k] = cast.ToString(v)
	}
	return vars
}

// ToMap transforms DataConn to a Map
func (dc *DataConn) ToMap() map[string]interface{} {
	return h.M("id", dc.ID, "url", dc.URL, "vars", dc.Vars)
}

// GetType returns the connection type
func (dc *DataConn) GetType() ConnType {
	switch {
	case strings.HasPrefix(dc.URL, "postgres"):
		if strings.Contains(dc.URL, "redshift.amazonaws.com") {
			return ConnTypeDbRedshift
		}
		return ConnTypeDbPostgres
	case strings.HasPrefix(dc.URL, "redshift"):
		return ConnTypeDbRedshift
	case strings.HasPrefix(dc.URL, "sqlserver:"):
		if strings.Contains(dc.URL, "database.windows.net") {
			return ConnTypeDbAzure
		}
		return ConnTypeDbSQLServer
	case strings.HasPrefix(dc.URL, "mysql:"):
		return ConnTypeDbMySQL
	case strings.HasPrefix(dc.URL, "oracle:"):
		return ConnTypeDbOracle
	case strings.HasPrefix(dc.URL, "bigquery:"):
		return ConnTypeDbBigQuery
	case strings.HasPrefix(dc.URL, "snowflake"):
		return ConnTypeDbSnowflake
	case strings.HasPrefix(dc.URL, "fileSys :"):
		return ConnTypeDbSQLite
	case strings.HasPrefix(dc.URL, "s3:"):
		return ConnTypeFileS3
	case strings.HasPrefix(dc.URL, "hdfs:"):
		return ConnTypeFileHDFS
	case strings.Contains(dc.URL, ".core.windows.net"), strings.HasPrefix(dc.URL, "azure://"):
		if strings.Contains(dc.URL, "&sig=") && strings.Contains(dc.URL, "&spr=") {
			return ConnTypeNone // is SAS URL
		}
		return ConnTypeFileAzure
	case strings.HasPrefix(dc.URL, "gs://"):
		return ConnTypeFileGoogle
	case strings.HasPrefix(dc.URL, "sftp:/"):
		return ConnTypeFileSftp
	case strings.HasPrefix(dc.URL, "http:/"), strings.HasPrefix(dc.URL, "https:/"):
		if strings.HasSuffix(dc.URL, ".git") {
			return ConnTypeAPIGit
		}
		if strings.Contains(dc.URL, "github.com/") && len(strings.Split(dc.URL, "/")) == 5 {
			return ConnTypeAPIGithub
		}
		return ConnTypeFileHTTP
	case strings.Contains(dc.URL, `:\`):
		return ConnTypeFileLocal // Windows
	case strings.HasPrefix(dc.URL, `/`) && !strings.Contains(dc.URL, `:`):
		return ConnTypeFileLocal // Linux
	case strings.Contains(dc.URL, "://"):
		return ConnTypeNone
	default:
		return ConnTypeNone
	}
}

// ConnTypesDefPort are all the default ports
var ConnTypesDefPort = map[ConnType]int{
	ConnTypeDbPostgres:  5432,
	ConnTypeDbRedshift:  5439,
	ConnTypeDbMySQL:     3306,
	ConnTypeDbOracle:    1521,
	ConnTypeDbSQLServer: 1433,
	ConnTypeDbAzure:     1433,
}

// GetCredProps returns the credential properties
func (dc *DataConn) GetCredProps() (m map[string]interface{}, err error) {
	u, err := url.Parse(dc.URL)
	if err != nil {
		err = h.Error(err, "could not parse URL for "+dc.GetTypeKey())
		return
	}

	password, _ := u.User.Password()

	port := cast.ToInt(u.Port())
	if port == 0 {
		port = ConnTypesDefPort[dc.GetType()]
	}

	m = h.M(
		"type", dc.GetTypeKey(),
		"host", u.Hostname(),
		"user", u.User.Username(),
		"password", password,
		"port", port,
		"database", strings.ReplaceAll(u.Path, "/", ""),
		"url", u,
	)
	return
}

// GetTypeNameLong return the type long name
func (dc *DataConn) GetTypeNameLong() string {
	mapping := map[ConnType]string{
		ConnTypeFileLocal:   "FileSys - Local",
		ConnTypeFileHDFS:    "FileSys - HDFS",
		ConnTypeFileS3:      "FileSys - S3",
		ConnTypeFileAzure:   "FileSys - Azure",
		ConnTypeFileGoogle:  "FileSys - Google",
		ConnTypeFileSftp:    "FileSys - Sftp",
		ConnTypeFileHTTP:    "FileSys - HTTP",
		ConnTypeDbPostgres:  "DB - PostgreSQL",
		ConnTypeDbRedshift:  "DB - Redshift",
		ConnTypeDbMySQL:     "DB - MySQL",
		ConnTypeDbOracle:    "DB - Oracle",
		ConnTypeDbBigQuery:  "DB - BigQuery",
		ConnTypeDbSnowflake: "DB - Snowflake",
		ConnTypeDbSQLite:    "DB - SQLite",
		ConnTypeDbSQLServer: "DB - SQLServer",
		ConnTypeDbAzure:     "DB - Azure",
		ConnTypeAPIGit:      "API - Git",
		ConnTypeAPIGithub:   "API - Github",
	}
	return mapping[dc.GetType()]
}

// GetKind return the Kind name
func (dc *DataConn) GetKind() string {
	mapping := map[ConnType]string{
		ConnTypeFileLocal:   "file",
		ConnTypeFileHDFS:    "file",
		ConnTypeFileS3:      "file",
		ConnTypeFileAzure:   "file",
		ConnTypeFileGoogle:  "file",
		ConnTypeFileSftp:    "file",
		ConnTypeFileHTTP:    "file",
		ConnTypeDbPostgres:  "database",
		ConnTypeDbRedshift:  "database",
		ConnTypeDbMySQL:     "database",
		ConnTypeDbOracle:    "database",
		ConnTypeDbBigQuery:  "database",
		ConnTypeDbSnowflake: "database",
		ConnTypeDbSQLite:    "database",
		ConnTypeDbSQLServer: "database",
		ConnTypeDbAzure:     "database",
		ConnTypeAPIGit:      "api",
		ConnTypeAPIGithub:   "api",
	}
	return mapping[dc.GetType()]
}

// ConnTypesKeyMapping are all the connection types with their key
var ConnTypesKeyMapping = map[ConnType]string{
	ConnTypeFileLocal:   "local",
	ConnTypeFileHDFS:    "hdfs",
	ConnTypeFileS3:      "s3",
	ConnTypeFileAzure:   "azure",
	ConnTypeFileGoogle:  "gs",
	ConnTypeFileSftp:    "sftp",
	ConnTypeFileHTTP:    "http",
	ConnTypeDbPostgres:  "postgres",
	ConnTypeDbRedshift:  "redshift",
	ConnTypeDbMySQL:     "mysql",
	ConnTypeDbOracle:    "oracle",
	ConnTypeDbBigQuery:  "bigquery",
	ConnTypeDbSnowflake: "snowflake",
	ConnTypeDbSQLite:    "sqlite",
	ConnTypeDbSQLServer: "sqlserver",
	ConnTypeDbAzure:     "azuresql",
	ConnTypeAPIGit:      "git",
	ConnTypeAPIGithub:   "github",
}

// GetTypeKey return the type name
func (dc *DataConn) GetTypeKey() string {
	return ConnTypesKeyMapping[dc.GetType()]
}

// ConnTypesNameMapping are all the connection types with their key
var ConnTypesNameMapping = map[ConnType]string{
	ConnTypeFileLocal:   "Local Storage",
	ConnTypeFileHDFS:    "HDFS",
	ConnTypeFileS3:      "AWS S3",
	ConnTypeFileAzure:   "Azure Storage",
	ConnTypeFileGoogle:  "Google Storage",
	ConnTypeFileSftp:    "SFTP",
	ConnTypeFileHTTP:    "HTTP",
	ConnTypeDbPostgres:  "PostgreSQL",
	ConnTypeDbRedshift:  "Redshift",
	ConnTypeDbMySQL:     "MySQL",
	ConnTypeDbOracle:    "Oracle",
	ConnTypeDbBigQuery:  "BigQuery",
	ConnTypeDbSnowflake: "Snowflake",
	ConnTypeDbSQLite:    "SQLite",
	ConnTypeDbSQLServer: "SQL Server",
	ConnTypeDbAzure:     "Azure SQL",
	ConnTypeAPIGit:      "Git",
	ConnTypeAPIGithub:   "Github",
}

// GetTypeName return the type name
func (dc *DataConn) GetTypeName() string {
	return ConnTypesNameMapping[dc.GetType()]
}

// GetConnTypeMapping returns a key to name mapping of connection types
func GetConnTypeMapping() map[string]string {
	m := map[string]string{}
	for t, key := range ConnTypesKeyMapping {
		m[key] = ConnTypesNameMapping[t]
	}
	return m
}

// IsDbType returns true for database connections
func (dc *DataConn) IsDbType() bool {
	connType := dc.GetType()
	if connType > connTypeDbStart && connType < connTypeDbEnd {
		return true
	}
	return false
}

// IsFileType returns true for file connections
func (dc *DataConn) IsFileType() bool {
	connType := dc.GetType()
	if connType > connTypeFileStart && connType < connTypeFileEnd {
		return true
	}
	return false
}

// IsAPIType returns true for api connections
func (dc *DataConn) IsAPIType() bool {
	connType := dc.GetType()
	if connType > connTypeAPIStart && connType < connTypeAPIEnd {
		return true
	}
	return false
}
