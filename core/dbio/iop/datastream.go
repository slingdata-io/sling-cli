package iop

import (
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	arrowCompress "github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/flarco/g/json"
	jit "github.com/json-iterator/go"
	parquet "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress"
	"github.com/segmentio/ksuid"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"golang.org/x/text/transform"

	"github.com/samber/lo"
	"github.com/spf13/cast"
)

var (
	jsoniter = jit.ConfigCompatibleWithStandardLibrary
)

// Datastream is a stream of rows
type Datastream struct {
	Columns       Columns
	Buffer        [][]any
	BatchChan     chan *Batch
	Batches       []*Batch
	CurrentBatch  *Batch
	Count         uint64
	Context       *g.Context
	Ready         bool
	Bytes         atomic.Uint64
	Sp            *StreamProcessor
	SafeInference bool
	NoDebug       bool
	Inferred      bool
	deferFuncs    []func()
	closed        bool
	empty         bool
	it            *Iterator
	config        StreamConfig
	df            *Dataflow
	bwRows        chan []any // for correct byte written
	readyChn      chan struct{}
	schemaChgChan chan schemaChg
	bwCsv         *csv.Writer // for correct byte written
	ID            string
	Metadata      Metadata // map of column name to metadata type
	paused        bool
	pauseChan     chan struct{}
	unpauseChan   chan struct{}
}

type schemaChg struct {
	ChangedIndex int
	ChangedType  ColumnType
	Added        bool
	AddedCols    Columns
}

type FileStreamConfig struct {
	Limit            int               `json:"limit"`
	Select           []string          `json:"select"`
	SQL              string            `json:"sql"`
	Format           dbio.FileType     `json:"format"`
	IncrementalKey   string            `json:"incremental_key"`
	IncrementalValue string            `json:"incremental_value"`
	FileSelect       *[]string         `json:"file_select"`     // a list of files to include.
	DuckDBFilename   bool              `json:"duckdb_filename"` // stream URL
	Props            map[string]string `json:"props"`
}

func (sc *FileStreamConfig) ComputeWithDuckDB() bool {
	if val := os.Getenv("SLING_DUCKDB_COMPUTE"); val != "" {
		return cast.ToBool(val)
	}
	return true
}

func (sc *FileStreamConfig) ShouldUseDuckDB() bool {
	if val := sc.ComputeWithDuckDB(); !val {
		return val
	}
	return g.In(sc.Format, dbio.FileTypeIceberg, dbio.FileTypeDelta) || sc.SQL != ""
}

func (sc *FileStreamConfig) GetProp(key string) string {
	if sc.Props == nil {
		sc.Props = map[string]string{}
	}
	return sc.Props[key]
}

func (sc *FileStreamConfig) SetProp(key, val string) {
	if sc.Props == nil {
		sc.Props = map[string]string{}
	}
	sc.Props[key] = val
}

type KeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

type Metadata struct {
	StreamURL KeyValue `json:"stream_url"`
	LoadedAt  KeyValue `json:"loaded_at"`
	RowNum    KeyValue `json:"row_num"`
	RowID     KeyValue `json:"row_id"`
	ExecID    KeyValue `json:"exec_id"`
}

// AsMap return as map
func (m *Metadata) AsMap() map[string]any {
	m0 := g.M()
	g.JSONConvert(m, &m0)
	return m0
}

// Iterator is the row provider for a datastream
type Iterator struct {
	Row          []any
	Reprocess    chan []any
	IsCasted     bool
	RowIsCasted  bool
	Counter      uint64
	StreamRowNum uint64
	Context      *g.Context
	Closed       bool
	ds           *Datastream
	dsBufferI    int // -1 means ds is not buffered
	nextFunc     func(it *Iterator) bool
	limitCnt     uint64 // to not check for df limit each cycle

	prevStreamURL  any
	dsBufferStream []string

	incrementalVal  any
	incrementalCol  string
	incrementalColI int
}

// NewDatastream return a new datastream
func NewDatastream(columns Columns) (ds *Datastream) {
	return NewDatastreamContext(context.Background(), columns)
}

// NewDatastreamIt with it
func NewDatastreamIt(ctx context.Context, columns Columns, nextFunc func(it *Iterator) bool) (ds *Datastream) {
	ds = NewDatastreamContext(ctx, columns)
	ds.it = ds.NewIterator(columns, nextFunc)
	return
}

func (ds *Datastream) NewIterator(columns Columns, nextFunc func(it *Iterator) bool) *Iterator {
	it := &Iterator{
		Row:             make([]any, len(columns)),
		Reprocess:       make(chan []any, 1), // for reprocessing row
		nextFunc:        nextFunc,
		Context:         ds.Context,
		ds:              ds,
		dsBufferI:       -1,
		incrementalColI: -1,
	}

	if ds.config.Map["sling_incremental_col"] != "" {
		it.incrementalCol = ds.config.Map["sling_incremental_col"]
	}
	if ds.config.Map["sling_incremental_val"] != "" {
		it.incrementalVal = ds.config.Map["sling_incremental_val"]
	}

	return it
}

// NewDatastreamContext return a new datastream
func NewDatastreamContext(ctx context.Context, columns Columns) (ds *Datastream) {

	ds = &Datastream{
		ID:            g.NewTsID("ds"),
		BatchChan:     make(chan *Batch, 1000),
		Batches:       []*Batch{},
		Buffer:        make([][]any, 0, SampleSize),
		Columns:       columns,
		Context:       g.NewContext(ctx),
		Sp:            NewStreamProcessor(),
		config:        StreamConfig{EmptyAsNull: true, Header: true},
		deferFuncs:    []func(){},
		bwCsv:         csv.NewWriter(io.Discard),
		bwRows:        make(chan []any, 100),
		readyChn:      make(chan struct{}, 1),
		schemaChgChan: make(chan schemaChg, 1000),
		pauseChan:     make(chan struct{}),
		unpauseChan:   make(chan struct{}),
	}
	ds.Sp.ds = ds
	ds.it = ds.NewIterator(columns, func(it *Iterator) bool { return false })

	return
}

func (ds *Datastream) Df() *Dataflow {
	return ds.df
}

func (ds *Datastream) Limited(limit ...int) bool {
	if len(limit) > 0 && ds.Count >= uint64(limit[0]) {
		return true
	}
	if ds.df == nil || ds.df.Limit == 0 {
		return false
	}
	return ds.Count >= ds.df.Limit
}

func (ds *Datastream) processBwRows() {
	processBw := true
	if val := os.Getenv("SLING_PROCESS_BW"); val != "" {
		processBw = cast.ToBool(val)
	}

	done := false
	process := func() {
		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				g.Error("panic occurred! %s\n%s", r, string(debug.Stack()))
				g.Debug("soft-panic occurred: %s", r)
			}
		}()

		for row := range ds.bwRows {
			if processBw {
				ds.writeBwCsvSafe(ds.CastToStringSafeMask(row))
				ds.bwCsv.Flush()
			}
		}
		done = true
	}

	for {
		// loop so that if panic occurs, it continues
		process()
		if done {
			break
		}
	}
}

// SetReady sets the ds.ready
func (ds *Datastream) SetReady() {
	if !ds.Ready {
		ds.Ready = true
		go func() { ds.readyChn <- struct{}{} }()
	}
}

// SetEmpty sets the ds.Rows channel as empty
func (ds *Datastream) SetEmpty() {
	ds.empty = true
}

// SetConfig sets the ds.config values
func (ds *Datastream) SetConfig(configMap map[string]string) {
	// lower the keys
	for _, k := range lo.Keys(configMap) {
		configMap[strings.ToLower(k)] = configMap[k]
	}
	ds.Sp.SetConfig(configMap)
	ds.config = ds.Sp.Config

	// set columns if empty
	if len(ds.Columns) == 0 && len(ds.Sp.Config.Columns) > 0 {
		ds.Columns = ds.Sp.Config.Columns
	}

	// set metadata
	if metadata, ok := configMap["metadata"]; ok {
		ds.SetMetadata(metadata)
	}

	// parse constraint func after unmarshal
	for i := range ds.Columns {
		if ds.Columns[i].Constraint != nil {
			ds.Columns[i].Constraint.parse()
		}
	}
}

// GetConfig get config
func (ds *Datastream) GetConfig() (configMap map[string]string) {
	// lower the keys
	configMapI := g.M()
	g.JSONConvert(ds.Sp.Config, &configMapI)
	return g.ToMapString(configMapI)
}

// CastRowToString returns the row as string casted
func (ds *Datastream) CastRowToString(row []any) []string {
	rowStr := make([]string, len(row))
	for i, val := range row {
		rowStr[i] = ds.Sp.CastToString(i, val, ds.Columns[i].Type)
	}
	return rowStr
}

// CastToStringSafeMask returns the row as string mask casted ( evensafer)
func (ds *Datastream) CastToStringSafeMask(row []any) []string {
	rowStr := make([]string, len(row))
	for i, val := range row {
		rowStr[i] = ds.Sp.CastToStringSafeMask(i, val, ds.Columns[i].Type)
	}
	return rowStr
}

// writeBwCsv writes to the nullCsv
func (ds *Datastream) writeBwCsv(row []string) {
	bw, _ := ds.bwCsv.Write(row)
	ds.AddBytes(int64(bw))
}

func (ds *Datastream) writeBwCsvSafe(row []string) {
	var totalBytes int64

	for _, val := range row {
		totalBytes += int64(len(val)) // Calculate the byte length of the string
		totalBytes++                  // Add 1 for delimiter/separator
	}

	if len(row) > 0 {
		totalBytes-- // Remove last delimiter
		totalBytes++ // Add newline character
	}

	ds.AddBytes(totalBytes)
}

// Push return the fields of the Data
func (ds *Datastream) Push(row []any) {
	batch := ds.LatestBatch()

	if batch == nil {
		batch = ds.NewBatch(ds.Columns)
	}

	batch.Push(row)
}

// IsClosed is true is ds is closed
func (ds *Datastream) IsClosed() bool {
	return ds.closed
}

// WaitReady waits until datastream is ready
func (ds *Datastream) WaitReady() error {
	if ds.Ready {
		return ds.Context.Err()
	}

	select {
	case <-ds.readyChn:
		return ds.Context.Err()
	case <-ds.Context.Ctx.Done():
		return ds.Context.Err()
	}
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
	ds.Context.Lock()

	if !ds.closed {
		close(ds.bwRows)
		close(ds.BatchChan)

		if batch := ds.LatestBatch(); batch != nil {
			batch.Close()
		}

		for _, batch := range ds.Batches {
			select {
			case <-batch.closeChan: // clean up
			default:
			}
		}

	loop:
		for {
			select {
			case <-ds.pauseChan:
				<-ds.unpauseChan // wait for unpause
			case <-ds.readyChn:
			case <-ds.schemaChgChan:
			default:
				break loop
			}
		}

		for _, f := range ds.deferFuncs {
			f()
		}

		if ds.Sp.unrecognizedDate != "" {
			g.Warn("unrecognized date format (%s)", ds.Sp.unrecognizedDate)
		}

		ds.Buffer = nil // clear buffer
	}
	if ds.it != nil {
		ds.it.close()
	}
	ds.closed = true
	ds.Context.Unlock()

	select {
	case <-ds.readyChn:
	default:
	}
}

// SetColumns sets the columns
func (ds *Datastream) AddColumns(newCols Columns, overwrite bool) (added Columns) {
	mergedCols, colsAdded, colsChanged := ds.Columns.Merge(newCols, overwrite)
	ds.Columns = mergedCols.ValidateNames(ds.Sp.Config.TargetType)
	added = colsAdded.AddedCols

	ds.schemaChgChan <- colsAdded
	for _, change := range colsChanged {
		ds.schemaChgChan <- change
	}

	return added
}

// ChangeColumn applies a column type change
func (ds *Datastream) ChangeColumn(i int, newType ColumnType) {
	if ds == nil {
		return
	}

	oldType := ds.Columns[i].Type

	switch {
	// case ds.Columns[i].Sourced:
	// 	return
	case oldType == newType:
		return
	case oldType == TextType && newType == StringType:
		return
	}

	g.Debug("column type change for %s (%s to %s)", ds.Columns[i].Name, oldType, newType)
	setChangedType(&ds.Columns[i], newType)
	ds.schemaChgChan <- schemaChg{ChangedIndex: i, ChangedType: newType}
}

func setChangedType(col *Column, newType ColumnType) {
	oldType := col.Type
	// make type sticky
	nonStringToString := oldType != StringType && newType == StringType
	intToDecimal := oldType.IsInteger() && newType == DecimalType
	if nonStringToString || intToDecimal {
		col.Sourced = true
	}
	col.Type = newType
}

// GetFields return the fields of the Data
func (ds *Datastream) GetFields(args ...bool) []string {
	lower := false
	cleanUp := false
	if len(args) > 1 {
		lower = args[0]
		cleanUp = args[1]
	} else if len(args) > 0 {
		lower = args[0]
	}
	fields := make([]string, len(ds.Columns))

	for j, column := range ds.Columns {
		field := column.Name

		if lower {
			field = strings.ToLower(column.Name)
		}
		if cleanUp {
			field = CleanName(field) // clean up
		}

		fields[j] = field
	}

	return fields
}

func (ds *Datastream) SetIterator(it *Iterator) {
	ds.it = it
}

func (ds *Datastream) transformReader(reader io.Reader) (newReader io.Reader, decoded bool) {
	// decode File if requested
	if transformsPayload, ok := ds.Sp.Config.Map["transforms"]; ok {
		columnTransforms := makeColumnTransforms(transformsPayload)
		applied := []string{}

		if ts, ok := columnTransforms["*"]; ok {
			for _, t := range ts {
				switch t {
				case TransformDecodeLatin1.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeISO8859_1)
				case TransformDecodeLatin5.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeISO8859_5)
				case TransformDecodeLatin9.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeISO8859_15)
				case TransformDecodeWindows1250.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeWindows1250)
				case TransformDecodeWindows1252.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeWindows1252)
				case TransformDecodeUtf16.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF16)
				case TransformDecodeUtf8.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF8)
				case TransformDecodeUtf8Bom.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF8BOM)

				case TransformEncodeLatin1.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeISO8859_1)
				case TransformEncodeLatin5.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeISO8859_5)
				case TransformEncodeLatin9.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeISO8859_15)
				case TransformEncodeWindows1250.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeWindows1250)
				case TransformEncodeWindows1252.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeWindows1252)
				case TransformEncodeUtf16.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeUTF16)
				case TransformEncodeUtf8.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeUTF8)
				case TransformEncodeUtf8Bom.Name:
					newReader = transform.NewReader(reader, ds.Sp.transformers.EncodeUTF8BOM)

				default:
					continue
				}
				applied = append(applied, t) // delete from transforms, already applied
			}

			ts = lo.Filter(ts, func(t string, i int) bool {
				return !g.In(t, applied...)
			})
			columnTransforms["*"] = ts
		}

		if len(applied) > 0 {
			// re-apply transforms
			ds.Sp.applyTransforms(g.Marshal(columnTransforms))

			return newReader, true
		}
	} else {
		// auto-decode
		bReader, ok := reader.(*bufio.Reader)
		if !ok {
			bReader = bufio.NewReader(reader)
		}
		testBytes, err := bReader.Peek(3)
		if err == nil {
			// https://en.wikipedia.org/wiki/Byte_order_mark
			switch {
			case testBytes[0] == 255 && testBytes[1] == 254:
				// is UTF-16 (LE)
				g.Debug("auto-decoding UTF-16 (LE)")
				newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF16)
				return newReader, true
			case testBytes[0] == 254 && testBytes[1] == 255:
				// is UTF-16 (BE)
				g.Debug("auto-decoding UTF-16 (BE)")
				newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF16)
				return newReader, true
			case testBytes[0] == 239 && testBytes[1] == 187 && testBytes[2] == 191:
				// is UTF-8 with BOM
				g.Debug("auto-decoding UTF-8-BOM")
				newReader = transform.NewReader(reader, ds.Sp.transformers.DecodeUTF8BOM)
				return newReader, true
			}
			return bReader, true // since the reader advanced
		}
	}

	return reader, false
}

// SetFields sets the fields/columns of the Datastream
func (ds *Datastream) SetFields(fields []string) {
	if ds.Columns == nil || len(ds.Columns) != len(fields) {
		ds.Columns = make(Columns, len(fields))
	}

	for i, field := range fields {
		ds.Columns[i].Name = field
		ds.Columns[i].Position = i + 1
	}
}

// SetFileURI sets the FileURI of the columns of the Datastream
func (ds *Datastream) SetFileURI() {
	for i := range ds.Columns {
		ds.Columns[i].FileURI = cast.ToString(ds.Metadata.StreamURL.Value)
	}
}

// Collect reads a stream and return a dataset
// limit of 0 is unlimited
func (ds *Datastream) Collect(limit int) (Dataset, error) {
	data := NewDataset(ds.Columns)

	// wait for first ds to start streaming.
	// columns/buffer need to be populated
	err := ds.WaitReady()
	if err != nil {
		return data, g.Error(err)
	}

	data.Result = nil
	data.Columns = ds.Columns
	data.Rows = [][]any{}
	limited := false

	for row := range ds.Rows() {
		data.Rows = append(data.Rows, row)
		if limit > 0 && len(data.Rows) == limit {
			limited = true
			break
		}
	}

	if !limited {
		ds.SetEmpty()
	}

	ds.Buffer = nil // clear buffer
	if ds.Err() != nil {
		return data, g.Error(ds.Err())
	}
	return data, nil
}

// Err return the error if any
func (ds *Datastream) Err() (err error) {
	return ds.Context.Err()
}

// Start generates the stream
// Should cycle the Iter Func until done
func (ds *Datastream) Start() (err error) {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			ds.Context.CaptureErr(err)
		}
	}()

	if !ds.NoDebug {
		g.Trace("new ds.Start %s", ds.ID)
	}

	if ds.it == nil {
		err = g.Error("iterator not defined")
		return g.Error(err, "need to define iterator")
	}

	castAllColumns := len(ds.Sp.Config.Columns) == 1 && ds.Sp.Config.Columns[0].Name == "*"
	if SampleSize == 0 || castAllColumns {
		goto skipBuffer
	}

loop:
	for ds.it.next() {
		select {
		case <-ds.Context.Ctx.Done():
			if ds.Context.Err() != nil {
				err = g.Error(ds.Context.Err())
				return
			}
			break loop
		case <-ds.it.Context.Ctx.Done():
			if ds.it.Context.Err() != nil {
				err = g.Error(ds.it.Context.Err())
				ds.Context.CaptureErr(err, "Failed to scan")
				return
			}
			break loop
		default:
			if ds.it.Counter == 1 && !ds.NoDebug {
				g.Trace("first row of %s => %#v", ds.ID, ds.it.Row) // trace first row for debugging
			}

			row := ds.Sp.ProcessRow(ds.it.Row)
			ds.Buffer = append(ds.Buffer, row)
			if ds.it.Counter >= cast.ToUint64(SampleSize) {
				break loop
			}
		}
	}

skipBuffer:

	// infer types
	if !ds.Inferred && len(ds.Buffer) > 0 {
		sampleData := NewDataset(ds.Columns)
		sampleData.Rows = ds.Buffer
		sampleData.NoDebug = ds.NoDebug
		sampleData.SafeInference = ds.SafeInference
		sampleData.Sp.dateLayouts = ds.Sp.dateLayouts
		sampleData.Sp.Config = ds.Sp.Config
		sampleData.InferColumnTypes()
		ds.Columns = sampleData.Columns
		ds.Inferred = true
	} else if len(ds.Sp.Config.Columns) > 0 || !ds.config.ColumnCasing.IsEmpty() {
		ds.Columns = ds.Columns.Coerce(ds.Sp.Config.Columns, true, ds.config.ColumnCasing, ds.config.TargetType)
	}

	// set to have it loop process
	ds.it.dsBufferI = 0

	if ds.it.Context.Err() != nil {
		err = g.Error(ds.it.Context.Err(), "error in getting rows")
		return
	}

	// add metadata
	metaValuesMap := map[int]func(it *Iterator) any{}
	{
		// ensure there are no duplicates
		ensureName := func(name string) string {
			name = ds.config.ColumnCasing.Apply(name, ds.config.TargetType)
			colNames := lo.Keys(ds.Columns.FieldMap(true))
			for lo.Contains(colNames, strings.ToLower(name)) {
				name = name + "_"
			}
			return name
		}

		if ds.Metadata.LoadedAt.Key != "" && ds.Metadata.LoadedAt.Value != nil {
			ds.Metadata.LoadedAt.Key = ensureName(ds.Metadata.LoadedAt.Key)

			// handle timestamp value
			isTimestamp := false
			if tVal, err := cast.ToTimeE(ds.Metadata.LoadedAt.Value); err == nil {
				isTimestamp = true
				ds.Metadata.LoadedAt.Value = tVal
			} else {
				ds.Metadata.LoadedAt.Value = cast.ToInt64(ds.Metadata.LoadedAt.Value)
			}

			col := Column{
				Name:        ds.Metadata.LoadedAt.Key,
				Type:        lo.Ternary(isTimestamp, TimestampzType, IntegerType),
				Position:    len(ds.Columns) + 1,
				Description: "Sling.Metadata.LoadedAt",
				Metadata:    map[string]string{"sling_metadata": "loaded_at"},
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = func(it *Iterator) any {
				return ds.Metadata.LoadedAt.Value
			}
		}

		if ds.Metadata.StreamURL.Key != "" && ds.Metadata.StreamURL.Value != nil {
			ds.Metadata.StreamURL.Key = ensureName(ds.Metadata.StreamURL.Key)
			col := Column{
				Name:        ds.Metadata.StreamURL.Key,
				Type:        StringType,
				Position:    len(ds.Columns) + 1,
				Description: "Sling.Metadata.StreamURL",
				Metadata:    map[string]string{"sling_metadata": "stream_url"},
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = func(it *Iterator) any {
				return ds.Metadata.StreamURL.Value
			}
		}

		if ds.Metadata.RowNum.Key != "" {
			ds.Metadata.RowNum.Key = ensureName(ds.Metadata.RowNum.Key)
			col := Column{
				Name:        ds.Metadata.RowNum.Key,
				Type:        BigIntType,
				Position:    len(ds.Columns) + 1,
				Description: "Sling.Metadata.RowNum",
				Metadata:    map[string]string{"sling_metadata": "row_num"},
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = func(it *Iterator) any {
				return it.StreamRowNum
			}
		}

		if ds.Metadata.RowID.Key != "" {
			ds.Metadata.RowID.Key = ensureName(ds.Metadata.RowID.Key)
			col := Column{
				Name:        ds.Metadata.RowID.Key,
				Type:        StringType,
				Position:    len(ds.Columns) + 1,
				Description: "Sling.Metadata.RowID",
				Metadata:    map[string]string{"sling_metadata": "row_id"},
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = func(it *Iterator) any {
				for {
					uid, err := ksuid.NewRandom()
					if err == nil {
						return uid.String()
					}
				}
			}
		}

		if ds.Metadata.ExecID.Key != "" {
			ds.Metadata.ExecID.Key = ensureName(ds.Metadata.ExecID.Key)
			col := Column{
				Name:        ds.Metadata.ExecID.Key,
				Type:        StringType,
				Position:    len(ds.Columns) + 1,
				Description: "Sling.Metadata.ExecID",
				Metadata:    map[string]string{"sling_metadata": "exec_id"},
			}
			ds.Columns = append(ds.Columns, col)
			metaValuesMap[col.Position-1] = func(it *Iterator) any {
				return ds.Metadata.ExecID.Value
			}
		}
	}

	// setMetaValues sets mata column values
	setMetaValues := func(it *Iterator) []any { return it.Row }
	if len(metaValuesMap) > 0 {
		setMetaValues = func(it *Iterator) []any {
			for len(it.Row) < len(ds.Columns) {
				it.Row = append(it.Row, nil)
			}
			for i, f := range metaValuesMap {
				it.Row[i] = f(it)
			}
			return it.Row
		}
	}

	go ds.processBwRows()

	if !ds.NoDebug && ds.Metadata.StreamURL.Value != nil {
		g.Trace("%s.StreamURL => %s", ds.ID, ds.Metadata.StreamURL.Value)
	}

	ds.SetReady()

	go func() {
		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
				ds.Context.CaptureErr(err)
			}
		}()

		var err error
		defer ds.Close()

		if ds.CurrentBatch == nil {
			ds.CurrentBatch = ds.NewBatch(ds.Columns)
		}

		row := make([]any, len(ds.Columns))
		rowPtrs := make([]any, len(ds.Columns))
		for i := range row {
			// cast the interface place holders
			row[i] = ds.Sp.CastType(row[i], ds.Columns[i].Type)
			rowPtrs[i] = &row[i]
		}

		defer func() {
			// if any error occurs during iteration
			if ds.it.Context.Err() != nil {
				ds.Context.CaptureErr(ds.it.Context.Err())
			}
		}()

	loop:
		for ds.it.next() {

		schemaChgLoop:
			for {
				// reprocess row if needed (to expand it as needed)
				ds.it.Row = setMetaValues(ds.it)
				if ds.it.IsCasted || ds.it.RowIsCasted {
					row = ds.it.Row
				} else {
					row = ds.Sp.CastRow(ds.it.Row, ds.Columns)
				}
				if ds.config.SkipBlankLines && ds.Sp.rowBlankValCnt == len(row) {
					goto loop
				}

				if ds.Sp.skipCurrent {
					ds.Sp.skipCurrent = false // reset
					goto loop
				}

				if ds.Limited() {
					break loop
				}

				if df := ds.df; df != nil {
					select {
					case <-ds.pauseChan:
						<-ds.unpauseChan // wait for unpause
						goto schemaChgLoop

					// only consume channel if df exists
					case schemaChgVal := <-ds.schemaChgChan:
						ds.CurrentBatch.Close()

						if schemaChgVal.Added && df.OnColumnAdded != nil {
							// g.DebugLow("adding columns %s", g.Marshal(schemaChgVal.AddedCols.Types()))
							if _, ok := df.AddColumns(schemaChgVal.AddedCols, false, ds.ID); !ok {
								ds.schemaChgChan <- schemaChgVal // requeue to try adding again
							}
						} else if df.OnColumnChanged != nil {
							// g.DebugLow("changing column %s to %s", df.Columns[schemaChgVal.I].Name, schemaChgVal.Type)
							if !df.ChangeColumn(schemaChgVal.ChangedIndex, schemaChgVal.ChangedType, ds.ID) {
								ds.schemaChgChan <- schemaChgVal // requeue to try changing again
							}
						}
						goto schemaChgLoop
					default:
					}
				}

				select {
				case <-ds.pauseChan:
					<-ds.unpauseChan // wait for unpause
					goto schemaChgLoop

				default:
					if ds.CurrentBatch.closed {
						ds.CurrentBatch = ds.NewBatch(ds.Columns)
					}
					break schemaChgLoop
				}
			}

			select {
			case <-ds.Context.Ctx.Done():
				if ds.df != nil {
					ds.df.Context.CaptureErr(ds.Err())
				}
				break loop
			case <-ds.it.Context.Ctx.Done():
				if ds.it.Context.Err() != nil {
					err = g.Error(ds.it.Context.Err())
					ds.Context.CaptureErr(err, "Failed to scan")
					if ds.df != nil {
						ds.df.Context.CaptureErr(ds.Err())
					}
				}
				break loop
			default:
				ds.CurrentBatch.Push(row)
			}
		}

		// close batch
		ds.CurrentBatch.Close()

		ds.SetEmpty()

		if !ds.NoDebug {
			g.Trace("Pushed %d rows for %s", ds.it.Counter, ds.ID)
		}
	}()

	return
}

func (ds *Datastream) Rows() chan []any {
	rows := MakeRowsChan()

	go func() {
		defer close(rows)
		for batch := range ds.BatchChan {
			for row := range batch.Rows {
				rows <- row
			}
		}
	}()

	return rows
}

func (ds *Datastream) SetMetadata(jsonStr string) {
	if jsonStr != "" {
		streamValue := ds.Metadata.StreamURL.Value
		g.Unmarshal(jsonStr, &ds.Metadata)
		if ds.Metadata.StreamURL.Value == nil {
			ds.Metadata.StreamURL.Value = streamValue
		}
	}
}

// ConsumeJsonReader uses the provided reader to stream JSON
// This will put each JSON rec as one string value
// so payload can be processed downstream
func (ds *Datastream) ConsumeJsonReader(reader io.Reader) (err error) {
	reader2, err := AutoDecompress(reader)
	if err != nil {
		return g.Error(err, "Could not decompress reader")
	}

	// decode File if requested by transform
	if newReader, ok := ds.transformReader(reader2); ok {
		reader2 = newReader
	}

	decoder := json.NewDecoder(reader2)
	js := NewJSONStream(ds, decoder, ds.Sp.Config.Flatten, ds.Sp.Config.Jmespath)
	ds.it = ds.NewIterator(ds.Columns, js.NextFunc)

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}
	return
}

// ConsumeXmlReader uses the provided reader to stream XML
// This will put each XML rec as one string value
// so payload can be processed downstream
func (ds *Datastream) ConsumeXmlReader(reader io.Reader) (err error) {
	reader2, err := AutoDecompress(reader)
	if err != nil {
		return g.Error(err, "Could not decompress reader")
	}

	// decode File if requested by transform
	if newReader, ok := ds.transformReader(reader2); ok {
		reader2 = newReader
	}

	decoder := xml.NewDecoder(reader2)
	js := NewJSONStream(ds, decoder, ds.Sp.Config.Flatten, ds.Sp.Config.Jmespath)
	ds.it = ds.NewIterator(ds.Columns, js.NextFunc)

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}
	return
}

type ReaderReady struct {
	Reader io.Reader
	URI    string
}

func (ds *Datastream) ConsumeJsonReaderChl(readerChn chan *ReaderReady, isXML bool) (err error) {

	nextJSON := func(reader *ReaderReady) (*jsonStream, error) {

		// set URI
		ds.Metadata.StreamURL.Value = reader.URI

		// decompress if needed
		reader2, err := AutoDecompress(reader.Reader)
		if err != nil {
			return nil, g.Error(err, "could not auto-decompress")
		}

		// decode File if requested by transform
		if newReader, ok := ds.transformReader(reader2); ok {
			reader2 = newReader
		}

		var decoder decoderLike
		if isXML {
			decoder = xml.NewDecoder(reader2)
		} else {
			decoder = json.NewDecoder(reader2)
		}
		jsNew := NewJSONStream(ds, decoder, ds.Sp.Config.Flatten, ds.Sp.Config.Jmespath)

		return jsNew, nil
	}

	js, err := nextJSON(<-readerChn)
	if err != nil {
		return
	}

	nextFunc := func(it *Iterator) bool {

	processNext:
		hasNext := js.NextFunc(it)

		if !hasNext && it.Context.Err() == nil {
			// next reader
			for reader := range readerChn {
				if reader == nil {
					return false
				}

				jsNew, err := nextJSON(reader)
				if err != nil {
					it.ds.Context.CaptureErr(g.Error(err, "Error getting next reader"))
					return false
				}

				// keep column map & sp
				jsNew.ColumnMap = js.ColumnMap
				jsNew.sp = js.sp
				js = jsNew

				goto processNext
			}
		}

		// set stream url
		if it.dsBufferI == -1 {
			// still buffering
			streamURL := cast.ToString(ds.Metadata.StreamURL.Value)
			it.dsBufferStream = append(it.dsBufferStream, streamURL)
		}

		return hasNext
	}

	ds.it = ds.NewIterator(ds.Columns, nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}
	return
}

// ConsumeCsvReaderChl reads a channel of readers. Should be safe to use with
// header top row
func (ds *Datastream) ConsumeCsvReaderChl(readerChn chan *ReaderReady) (err error) {

	c := CSV{
		NoHeader:        !ds.config.Header,
		FieldsPerRecord: ds.config.FieldsPerRec,
		Escape:          ds.config.Escape,
		Quote:           ds.config.Quote,
		Delimiter:       rune(0),
		NoDebug:         ds.NoDebug,
	}

	if ds.config.Delimiter != "" {
		c.Delimiter = rune(ds.config.Delimiter[0])
	}

	nextCSV := func(reader *ReaderReady) (r csv.CsvReaderLike, err error) {
		c.Reader = reader.Reader

		// set URI
		ds.Metadata.StreamURL.Value = reader.URI

		// decompress if needed
		readerDecompr, err := AutoDecompress(c.Reader)
		if err != nil {
			return r, g.Error(err, "could not auto-decompress")
		}

		// decode File if requested by transform
		c.Reader = readerDecompr
		if newReader, ok := ds.transformReader(c.Reader); ok {
			c.Reader = newReader
		}

		r, err = c.getReader()
		if err != nil {
			err = g.Error(err, "could not get reader")
			ds.Context.CaptureErr(err)
		}
		return
	}

	var r csv.CsvReaderLike
	var row0 []string
	for reader := range readerChn {
		r, err = nextCSV(reader)
		if err != nil {
			return
		}

		row0, err = r.Read()
		if err == io.EOF {
			if ds.Metadata.StreamURL.Value != nil {
				g.Debug("csv stream provided is empty (%s)", ds.Metadata.StreamURL.Value)
			}

			// if first reader is empty, continue cycling until non-empty file
			continue
		} else if err != nil {
			err = g.Error(err, "could not parse header line")
			ds.Context.CaptureErr(err)
			return err
		}

		break // got header row
	}

	if err == io.EOF {
		// all readers are empty
		ds.SetReady()
		ds.Close()
		return nil
	} else if c.FieldsPerRecord == 0 || len(ds.Columns) != len(row0) {
		ds.SetFields(CleanHeaderRow(row0))
	}

	var colMap map[int]int
	nextFunc := func(it *Iterator) bool {

	processNext:
		row, err := r.Read()
		if err == io.EOF {
			c.File.Close()

			for reader := range readerChn {
				if reader == nil {
					return false
				}

				r, err = nextCSV(reader)
				if err != nil {
					it.ds.Context.CaptureErr(g.Error(err, "Error getting next reader"))
					return false
				}

				// analyze header for subsequent CSVs, since c.getReader() injects header line if missing
				row0, _ = r.Read()
				row0 = CleanHeaderRow(row0)

				// some files may have new columns
				fm := it.ds.Columns.FieldMap(true)
				toAdd := Columns{}
				for _, name := range row0 {
					if _, ok := fm[strings.ToLower(name)]; !ok {
						// field is not found, so we need to add
						toAdd = append(toAdd, Column{Name: name, Type: StringType, Position: len(it.ds.Columns) + 1})
					}
				}

				// add new columns, ensure all columns exist
				if len(toAdd) > 0 {
					it.ds.AddColumns(toAdd, false)
				}

				// set column mapping if in different order
				if g.Marshal(row0) != g.Marshal(it.ds.Columns.Names()) {
					fm := it.ds.Columns.FieldMap(true)
					colMap = map[int]int{}
					for incorrectI, name := range row0 {
						colMap[incorrectI] = fm[strings.ToLower(name)]
					}
				} else {
					colMap = nil
				}

				goto processNext
			}

			return false
		} else if err != nil {
			it.ds.Context.CaptureErr(g.Error(err, "Error reading file"))
			return false
		}

		if len(row) > len(it.ds.Columns) {
			it.addNewColumns(len(row))
		}

		if colMap != nil {
			// remake row in proper order. row has new structure
			correctRow := make([]string, len(it.ds.Columns))
			for incorrectI, correctI := range colMap {
				// Ensure the target index correctI is valid for correctRow (current schema length)
				if correctI < len(correctRow) {
					// Ensure the source index incorrectI is valid for the current data row's length
					if incorrectI < len(row) {
						correctRow[correctI] = row[incorrectI]
					}
				} else {
					// This would indicate a schema synchronization issue where colMap's target index
					// is out of bounds for the current datastream schema size (len(it.ds.Columns)).
					// This case should ideally not be hit if schema updates are perfectly synchronized.
					err = g.Error("CSV Schema inconsistency at row #%d (%s): colMap index %d is out of bounds for current schema length %d for column '%s'. Here are all the columns detected: %s", it.Counter+1, ds.Metadata.StreamURL.Value, correctI, len(correctRow), it.ds.Columns.Names()[correctI], g.Marshal(it.ds.Columns.Names()))
					it.ds.Context.CaptureErr(err)
					return false
				}
			}
			row = correctRow
		}

		it.Row = make([]any, len(row))
		var val any
		for i, val0 := range row {
			if i < len(it.ds.Columns) {
				if col := it.ds.Columns[i]; col.Type != "" && !col.IsString() {
					val0 = strings.TrimSpace(val0)
					if val0 == "" {
						val = nil
					} else {
						val = val0
					}
				} else {
					val = val0
				}
			} else {
				it.addNewColumns(len(row))
				val = val0
			}
			it.Row[i] = val
		}

		// set stream url
		if it.dsBufferI == -1 {
			// still buffering
			streamURL := cast.ToString(ds.Metadata.StreamURL.Value)
			it.dsBufferStream = append(it.dsBufferStream, streamURL)
		}

		return true
	}

	ds.it = ds.NewIterator(ds.Columns, nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeCsvReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeCsvReader(reader io.Reader) (err error) {
	c := CSV{
		Reader:          reader,
		NoHeader:        !ds.config.Header,
		FieldsPerRecord: ds.config.FieldsPerRec,
		Escape:          ds.config.Escape,
		Quote:           ds.config.Quote,
		Delimiter:       rune(0),
		NoDebug:         ds.NoDebug,
	}

	if ds.config.Delimiter != "" {
		c.Delimiter = rune(ds.config.Delimiter[0])
	}

	// decompress if needed
	readerDecompr, err := AutoDecompress(reader)
	if err != nil {
		err = g.Error(err, "could not AutoDecompress")
		ds.Context.CaptureErr(err)
		return err
	}

	// decode File if requested by transform
	c.Reader = readerDecompr
	if newReader, ok := ds.transformReader(readerDecompr); ok {
		c.Reader = newReader
	}

	r, err := c.getReader()
	if err != nil {
		err = g.Error(err, "could not get reader")
		ds.Context.CaptureErr(err)
		return err
	}

	// set delimiter
	// ds.config.Delimiter = string(c.Delimiter)

	row0, err := r.Read()
	if err == io.EOF {
		if ds.Metadata.StreamURL.Value != nil {
			g.Debug("csv stream provided is empty (%s)", ds.Metadata.StreamURL.Value)
		} else {
			g.Trace("csv stream is empty for %s", ds.ID)
		}
		err = nil
	} else if err != nil {
		err = g.Error(err, "could not parse header line")
		ds.Context.CaptureErr(err)
		return err
	}

	if c.FieldsPerRecord == 0 || len(ds.Columns) != len(row0) {
		ds.SetFields(CleanHeaderRow(row0))
	}

	nextFunc := func(it *Iterator) bool {

		row, err := r.Read()
		if err == io.EOF {
			c.File.Close()
			return false
		} else if err != nil {
			it.ds.Context.CaptureErr(g.Error(err, "Error reading file"))
			return false
		}

		if len(row) > len(it.ds.Columns) {
			it.addNewColumns(len(row))
		}

		it.Row = make([]any, len(row))
		var val any
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

	ds.it = ds.NewIterator(ds.Columns, nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeArrowReaderSeeker uses the provided reader to stream rows
func (ds *Datastream) ConsumeArrowReaderSeeker(reader *os.File) (err error) {
	selected := ds.Columns.Names()

	a, err := NewArrowReader(reader, selected)
	if err != nil {
		return g.Error(err, "could create arrow stream")
	}

	ds.Columns = a.Columns()
	ds.Inferred = ds.Columns.Sourced()
	ds.it = ds.NewIterator(ds.Columns, a.nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeParquetReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeParquetReaderSeeker(reader *os.File) (err error) {
	selected := ds.Columns.Names()

	// p, err := NewParquetStream(reader, Columns{}) // old version
	p, err := NewParquetArrowReader(reader, selected)
	if err != nil {
		return g.Error(err, "could create parquet stream")
	}

	ds.Columns = p.Columns()
	ds.Inferred = ds.Columns.Sourced()
	ds.it = ds.NewIterator(ds.Columns, p.nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeParquetReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeParquetReader(reader io.Reader) (err error) {
	// need to write to temp file prior
	parquetPath := path.Join(env.GetTempFolder(), g.NewTsID("parquet.temp")+".parquet")
	ds.Defer(func() { env.RemoveLocalTempFile(parquetPath) })

	file, err := os.Create(parquetPath)
	if err != nil {
		return g.Error(err, "Unable to create temp file: "+parquetPath)
	}

	g.Debug("downloading to temp file on disk: %s", parquetPath)
	bw, err := io.Copy(file, reader)
	if err != nil {
		return g.Error(err, "Unable to write to temp file: "+parquetPath)
	}
	g.Debug("wrote %d bytes to %s", bw, parquetPath)

	_, err = file.Seek(0, 0) // reset to beginning
	if err != nil {
		return g.Error(err, "Unable to seek to beginning of temp file: "+parquetPath)
	}

	return ds.ConsumeParquetReaderSeeker(file)
}

// ConsumeArrowReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeArrowReader(reader io.Reader) (err error) {
	// need to write to temp file prior
	arrowPath := path.Join(env.GetTempFolder(), g.NewTsID("arrow.temp")+".arrows")
	ds.Defer(func() { env.RemoveLocalTempFile(arrowPath) })

	file, err := os.Create(arrowPath)
	if err != nil {
		return g.Error(err, "Unable to create temp file: "+arrowPath)
	}

	g.Debug("downloading to temp file on disk: %s", arrowPath)
	bw, err := io.Copy(file, reader)
	if err != nil {
		return g.Error(err, "Unable to write to temp file: "+arrowPath)
	}
	g.Debug("wrote %d bytes to %s", bw, arrowPath)

	_, err = file.Seek(0, 0) // reset to beginning
	if err != nil {
		return g.Error(err, "Unable to seek to beginning of temp file: "+arrowPath)
	}

	return ds.ConsumeParquetReaderSeeker(file)
}

// ConsumeParquetReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeParquetReaderDuckDb(uri string, sc FileStreamConfig) (err error) {

	props := g.MapToKVArr(sc.Props)
	r, err := NewParquetReaderDuckDb(uri, props...)
	if err != nil {
		return g.Error(err, "could not create ParquetDuckDb")
	}

	sc.DuckDBFilename = ds.Metadata.StreamURL.Key != ""
	sql := r.MakeQuery(sc)
	ds, err = r.Duck.Stream(sql, g.M("datastream", ds, "filename", sc.DuckDBFilename))
	if err != nil {
		return g.Error(err, "could not read parquet rows")
	}

	ds.Inferred = true
	ds.Defer(func() { r.Close() })

	return
}

// ConsumeIcebergReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeIcebergReader(uri string, sc FileStreamConfig) (err error) {
	props := g.MapToKVArr(sc.Props)
	r, err := NewIcebergReader(uri, props...)
	if err != nil {
		return g.Error(err, "could not create IcebergDuckDb")
	}

	sql := r.MakeQuery(sc)
	ds, err = r.Duck.Stream(sql, g.M("datastream", ds))
	if err != nil {
		return g.Error(err, "could not read iceberg rows")
	}

	ds.Defer(func() { r.Close() })

	return
}

// ConsumeDeltaReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeDeltaReader(uri string, sc FileStreamConfig) (err error) {

	props := g.MapToKVArr(sc.Props)
	r, err := NewDeltaReader(uri, props...)
	if err != nil {
		return g.Error(err, "could not create DeltaReader")
	}

	sql := r.MakeQuery(sc)
	ds, err = r.Duck.Stream(sql, g.M("datastream", ds))
	if err != nil {
		return g.Error(err, "could not read delta rows")
	}

	ds.Inferred = true
	ds.Defer(func() { r.Close() })

	return
}

// ConsumeCsvReaderDuckDb uses the provided reader to stream rows
func (ds *Datastream) ConsumeCsvReaderDuckDb(uri string, sc FileStreamConfig) (err error) {

	props := g.MapToKVArr(sc.Props)
	r, err := NewCsvReaderDuckDb(uri, &ds.config, props...)
	if err != nil {
		return g.Error(err, "could not create CsvReaderDuckDb")
	}

	sc.DuckDBFilename = ds.Metadata.StreamURL.Key != ""
	sql := r.MakeQuery(sc)
	ds, err = r.Duck.Stream(sql, g.M("datastream", ds, "filename", sc.DuckDBFilename))
	if err != nil {
		return g.Error(err, "could not read csv rows")
	}

	ds.Inferred = true
	ds.Defer(func() { r.Close() })

	return
}

// ConsumeAvroReaderSeeker uses the provided reader to stream rows
func (ds *Datastream) ConsumeAvroReaderSeeker(reader io.ReadSeeker) (err error) {
	a, err := NewAvroStream(reader, Columns{})
	if err != nil {
		return g.Error(err, "could create avro stream")
	}

	ds.Columns = a.Columns()
	ds.Inferred = ds.Columns.Sourced()
	ds.it = ds.NewIterator(ds.Columns, a.nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeAvroReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeAvroReader(reader io.Reader) (err error) {
	// need to write to temp file prior
	avroPath := path.Join(env.GetTempFolder(), g.NewTsID("avro.temp")+".avro")
	ds.Defer(func() { env.RemoveLocalTempFile(avroPath) })

	file, err := os.Create(avroPath)
	if err != nil {
		return g.Error(err, "Unable to create temp file: "+avroPath)
	}

	g.Debug("downloading to temp file on disk: %s", avroPath)
	bw, err := io.Copy(file, reader)
	if err != nil {
		return g.Error(err, "Unable to write to temp file: "+avroPath)
	}
	g.Debug("wrote %d bytes to %s", bw, avroPath)

	_, err = file.Seek(0, 0) // reset to beginning
	if err != nil {
		return g.Error(err, "Unable to seek to beginning of temp file: "+avroPath)
	}

	return ds.ConsumeAvroReaderSeeker(file)
}

// ConsumeSASReaderSeeker uses the provided reader to stream rows
func (ds *Datastream) ConsumeSASReaderSeeker(reader io.ReadSeeker) (err error) {
	s, err := NewSASStream(reader, Columns{})
	if err != nil {
		return g.Error(err, "could create SAS stream")
	}

	ds.Columns = s.Columns()
	ds.Inferred = false
	ds.it = ds.NewIterator(ds.Columns, s.nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeSASReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeSASReader(reader io.Reader) (err error) {
	// need to write to temp file prior
	sasPath := path.Join(env.GetTempFolder(), g.NewTsID("sas.temp")+".sas7bdat")
	ds.Defer(func() { env.RemoveLocalTempFile(sasPath) })

	file, err := os.Create(sasPath)
	if err != nil {
		return g.Error(err, "Unable to create temp file: "+sasPath)
	}

	g.Debug("downloading to temp file on disk: %s", sasPath)
	bw, err := io.Copy(file, reader)
	if err != nil {
		return g.Error(err, "Unable to write to temp file: "+sasPath)
	}
	g.Debug("wrote %d bytes to %s", bw, sasPath)

	_, err = file.Seek(0, 0) // reset to beginning
	if err != nil {
		return g.Error(err, "Unable to seek to beginning of temp file: "+sasPath)
	}

	return ds.ConsumeSASReaderSeeker(file)
}

// ConsumeSASReaderSeeker uses the provided reader to stream rows
func (ds *Datastream) ConsumeExcelReaderSeeker(reader io.ReadSeeker, props map[string]string) (err error) {
	data, err := NewExcelDataset(reader, props)
	if err != nil {
		return g.Error(err, "could read Excel data")
	}

	rows := MakeRowsChan()
	nextFunc := func(it *Iterator) bool {
		for it.Row = range rows {
			return true
		}
		return false
	}

	go func() {
		defer close(rows)
		for _, row := range data.Rows {
			rows <- row
		}
	}()

	ds.it.IsCasted = true
	ds.Columns = data.Columns
	ds.Inferred = data.Inferred
	ds.Sp = data.Sp
	ds.Sp.ds = ds
	ds.it = ds.NewIterator(ds.Columns, nextFunc)
	ds.SetFileURI()

	err = ds.Start()
	if err != nil {
		return g.Error(err, "could start datastream")
	}

	return
}

// ConsumeSASReader uses the provided reader to stream rows
func (ds *Datastream) ConsumeExcelReader(reader io.Reader, props map[string]string) (err error) {
	// need to write to temp file prior
	excelPath := path.Join(env.GetTempFolder(), g.NewTsID("excel.temp")+".xlsx")
	ds.Defer(func() { env.RemoveLocalTempFile(excelPath) })

	file, err := os.Create(excelPath)
	if err != nil {
		return g.Error(err, "Unable to create temp file: "+excelPath)
	}

	g.Debug("downloading to temp file on disk: %s", excelPath)
	bw, err := io.Copy(file, reader)
	if err != nil {
		return g.Error(err, "Unable to write to temp file: "+excelPath)
	}
	g.Debug("wrote %d bytes to %s", bw, excelPath)

	_, err = file.Seek(0, 0) // reset to beginning
	if err != nil {
		return g.Error(err, "Unable to seek to beginning of temp file: "+excelPath)
	}

	return ds.ConsumeExcelReaderSeeker(file, props)
}

// AddBytes add bytes as processed
func (ds *Datastream) AddBytes(b int64) {
	ds.Bytes.Add(cast.ToUint64(b))
}

// Records return rows of maps
func (ds *Datastream) Records() <-chan map[string]any {
	chnl := make(chan map[string]any, 1000)
	ds.WaitReady()

	fields := ds.GetFields(true)

	go func() {
		defer close(chnl)

		for row := range ds.Rows() {
			// get records
			rec := map[string]any{}

			for i, field := range fields {
				rec[field] = row[i]
			}
			chnl <- rec
		}
	}()

	return chnl
}

// Chunk splits the datastream into chunk datastreams (in sequence)
func (ds *Datastream) Chunk(limit uint64) (chDs chan *Datastream) {
	chDs = make(chan *Datastream)
	if limit == 0 {
		limit = 200000
	}

	go func() {
		defer close(chDs)
		nDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		chDs <- nDs

		defer func() { nDs.Close() }()

	loop:
		for row := range ds.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Push(row)

				if nDs.Count == limit {
					nDs.Close()
					nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)
					chDs <- nDs
				}
			}
		}
		ds.SetEmpty()
	}()
	return
}

// Split splits the datastream into parallel datastreams
func (ds *Datastream) Split(numStreams ...int) (dss []*Datastream) {
	// TODO: Split freezes the flow in some situation, such as S3 -> PG.
	return []*Datastream{ds}

	conncurrency := lo.Ternary(
		os.Getenv("CONCURRENCY") != "",
		cast.ToInt(os.Getenv("CONCURRENCY")),
		runtime.NumCPU(),
	)
	if len(numStreams) > 0 {
		conncurrency = numStreams[0]
	}
	for i := 0; i < conncurrency; i++ {
		nDs := NewDatastreamContext(ds.Context.Ctx, ds.Columns)
		nDs.SetReady()
		dss = append(dss, nDs)
	}

	var nDs *Datastream
	go func() {
		defer ds.Close()
		i := 0
	loop:
		for row := range ds.Rows() {
			if i == len(dss) {
				i = 0 // cycle through datastreams
			}

			nDs = dss[i]
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Push(row)
			}
			i++
		}

		for _, nDs := range dss {
			go nDs.Close()
		}
		ds.SetEmpty()
	}()
	return
}

func (ds *Datastream) Pause() {
	if ds.Ready && !ds.closed {
		g.Trace("pausing %s", ds.ID)
		ds.pauseChan <- struct{}{}
		ds.paused = true
	}
}

func (ds *Datastream) TryPause() bool {
	if ds.Ready && !ds.paused {
		g.Trace("try pausing %s", ds.ID)
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case ds.pauseChan <- struct{}{}:
			ds.paused = true
			return true
		case <-timer.C:
			g.DebugLow("could not pause %s", ds.ID)
			return false
		}
	}
	return false
}

// Unpause unpauses all streams
func (ds *Datastream) Unpause() {
	if ds.paused {
		g.Trace("unpausing %s", ds.ID)
		ds.unpauseChan <- struct{}{}
		ds.paused = false
	}
}

// Shape changes the column types as needed, to the provided columns var
// It will cast the already wrongly casted rows, and not recast the
// correctly casted rows
func (ds *Datastream) Shape(columns Columns) (nDs *Datastream, err error) {
	batch := ds.LatestBatch()

	if batch == nil {
		batch = ds.NewBatch(ds.Columns)
	}

	err = batch.Shape(columns)

	return ds, err
}

// Map applies the provided function to every row
// and returns the result
func (ds *Datastream) Map(newColumns Columns, transf func([]any) []any) (nDs *Datastream) {

	rows := MakeRowsChan()
	nextFunc := func(it *Iterator) bool {
		for it.Row = range rows {
			return true
		}
		return false
	}
	nDs = NewDatastreamIt(ds.Context.Ctx, newColumns, nextFunc)
	ds.it.IsCasted = true
	ds.Inferred = true

	go func() {
		defer close(rows)
		for batch0 := range ds.BatchChan {
			for row := range batch0.Rows {
				rows <- transf(row)
			}
		}
	}()

	err := nDs.Start()
	if err != nil {
		ds.Context.CaptureErr(err)
	}

	return nDs
}

// MapParallel applies the provided function to every row in parallel and returns the result. Order is not maintained.
func (ds *Datastream) MapParallel(transf func([]any) []any, numWorkers int) (nDs *Datastream) {
	var wg sync.WaitGroup
	nDs = NewDatastreamContext(ds.Context.Ctx, ds.Columns)

	transform := func(wDs *Datastream, wg *sync.WaitGroup) {
		defer wg.Done()

	loop:
		for row := range wDs.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			case <-wDs.Context.Ctx.Done():
				break loop
			default:
				nDs.Rows() <- transf(row)
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

	loop:
		for row := range ds.Rows() {
			select {
			case <-nDs.Context.Ctx.Done():
				break loop
			default:
				wStreams[wi].Push(row)
			}
			if wi == numWorkers-1 {
				wi = -1 // cycle through workers
			}
			wi++
		}

		for i := 0; i < numWorkers; i++ {
			close(wStreams[i].Rows())
			wStreams[i].closed = true
		}

		wg.Wait()
		close(nDs.Rows())
		nDs.closed = true
	}()

	return
}

// NewCsvBytesChnl returns a channel yield chunk of bytes of csv
func (ds *Datastream) NewCsvBytesChnl(sc StreamConfig) (dataChn chan *[]byte) {
	dataChn = make(chan *[]byte, 100)

	go func() {
		defer close(dataChn)
		for {
			reader := ds.NewCsvReader(sc)
			data, err := io.ReadAll(reader)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error io.ReadAll(reader)"))
				ds.Context.Cancel()
				return
			}
			dataChn <- &data
		}
	}()
	return dataChn
}

// NewCsvBufferReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvBufferReader(sc StreamConfig) *bytes.Reader {
	reader := ds.NewCsvReader(sc)
	data, err := io.ReadAll(reader) // puts data in memory
	if err != nil {
		ds.Context.CaptureErr(g.Error(err, "Error io.ReadAll(reader)"))
	}
	return bytes.NewReader(data)
}

// NewCsvBufferReaderChnl provides a channel of readers as the limit is reached
// data is read in memory, whereas NewCsvReaderChnl does not hold in memory
func (ds *Datastream) NewCsvBufferReaderChnl(sc StreamConfig) (readerChn chan *bytes.Reader) {

	readerChn = make(chan *bytes.Reader) // not buffered, will block if receiver isn't ready

	go func() {
		defer close(readerChn)
		for !ds.empty {
			readerChn <- ds.NewCsvBufferReader(sc)
		}
	}()

	return readerChn
}

type BatchReader struct {
	Batch   *Batch
	Columns Columns
	Reader  io.Reader
	Counter int64
}

// NewCsvReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewCsvReaderChnl(sc StreamConfig) (readerChn chan *BatchReader) {
	readerChn = make(chan *BatchReader, 100)

	pipeR, pipeW := io.Pipe()

	tbw := int64(0)

	mux := ds.Context.Mux
	df := ds.Df()
	if df != nil {
		mux = df.Context.Mux
	}
	_ = mux

	go func() {
		var w *csv.Writer
		var br *BatchReader

		sp := NewStreamProcessor()
		sp.Config = sc

		defer close(readerChn)

		nextPipe := func(batch *Batch) error {

			pipeW.Close() // close the prior reader?
			tbw = 0       // reset

			// new reader
			pipeR, pipeW = io.Pipe()
			w = csv.NewWriterSize(pipeW, 40960*10)
			w.Comma = ','
			if sp.Config.Delimiter != "" {
				w.Comma = []rune(sp.Config.Delimiter)[0]
			}

			if sp.Config.Header {
				bw, err := w.Write(batch.Columns.Names(false, true))
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					err = g.Error(err, "error writing header")
					ds.Context.CaptureErr(g.Error(err, "error writing header"))
					ds.Context.Cancel()
					pipeW.Close()
					return err
				}
			}

			br = &BatchReader{batch, batch.Columns, pipeR, 0}
			readerChn <- br

			return nil
		}

		for batch := range ds.BatchChan {

			if batch.ColumnsChanged() || batch.IsFirst() {
				err := nextPipe(batch)
				if err != nil {
					ds.Context.CaptureErr(err)
					return
				}
			}

			for row0 := range batch.Rows {
				// g.PP(batch.Columns.MakeRec(row0))
				br.Counter++
				// convert to csv string
				row := make([]string, len(row0))
				for i, val := range row0 {
					row[i] = sp.CastToString(i, val, batch.Columns[i].Type)
				}
				mux.Lock()

				bw, err := w.Write(row)
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}
				w.Flush()
				mux.Unlock()

				if (sc.FileMaxRows > 0 && br.Counter >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					err = nextPipe(batch)
					if err != nil {
						ds.Context.CaptureErr(err)
						return
					}
				}
			}

			w.Flush()
		}

		pipeW.Close()

	}()

	return readerChn
}

func (ds *Datastream) NewJsonReaderChnl(sc StreamConfig) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	pipe := g.NewPipe()
	firstRec := true

	readerChn <- pipe.Reader
	tbw := int64(0)

	go func() {
		defer close(readerChn)

		c := int64(0) // local counter

		bw, _ := pipe.Writer.Write([]byte("["))
		tbw = tbw + cast.ToInt64(bw)

		for batch := range ds.BatchChan {
			fields := batch.Columns.Names()

			for row0 := range batch.Rows {
				c++

				rec := g.M()
				for i, val := range row0 {
					if sVal, ok := val.(string); ok && batch.Columns[i].Type.IsJSON() {
						if looksLikeJson(sVal) {
							var v any
							if err := g.Unmarshal(sVal, &v); err == nil {
								val = v
							}
						} else if sVal == "null" {
							val = nil
						}
					}
					rec[fields[i]] = val
				}

				b, err := json.Marshal(rec)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error marshaling rec"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				if !firstRec {
					bw, _ := pipe.Writer.Write([]byte{','}) // comma in between records
					tbw = tbw + cast.ToInt64(bw)
				} else {
					firstRec = false
				}

				bw, err := pipe.Writer.Write(b)
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				if (sc.FileMaxRows > 0 && c >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					pipe.Writer.Write([]byte("]")) // close bracket
					pipe.Writer.Close()            // close the prior reader?
					tbw = 0                        // reset

					// new reader
					c = 0
					firstRec = true
					pipe = g.NewPipe()
					readerChn <- pipe.Reader
					bw, _ := pipe.Writer.Write([]byte("["))
					tbw = tbw + cast.ToInt64(bw)
				}
			}
		}

		pipe.Writer.Write([]byte("]")) // close bracket
		pipe.Writer.Close()
	}()

	return readerChn
}

// NewJsonLinesReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewJsonLinesReaderChnl(sc StreamConfig) (readerChn chan *io.PipeReader) {
	readerChn = make(chan *io.PipeReader, 100)

	pipe := g.NewPipe()

	readerChn <- pipe.Reader
	tbw := int64(0)

	go func() {
		defer close(readerChn)

		c := int64(0) // local counter

		for batch := range ds.BatchChan {
			fields := batch.Columns.Names()

			for row0 := range batch.Rows {
				c++

				rec := g.M()
				for i, val := range row0 {
					if sVal, ok := val.(string); ok && batch.Columns[i].Type.IsJSON() {
						if looksLikeJson(sVal) {
							var v any
							if err := g.Unmarshal(sVal, &v); err == nil {
								val = v
							}
						} else if sVal == "null" {
							val = nil
						}
					}
					rec[fields[i]] = val
				}

				b, err := json.Marshal(rec)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error marshaling rec"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				bw, err := pipe.Writer.Write(append(b, '\n'))
				tbw = tbw + cast.ToInt64(bw)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipe.Writer.Close()
					return
				}

				if (sc.FileMaxRows > 0 && c >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					pipe.Writer.Close() // close the prior reader?
					tbw = 0             // reset

					// new reader
					c = 0
					pipe = g.NewPipe()
					readerChn <- pipe.Reader
				}
			}
		}
		pipe.Writer.Close()
	}()

	return readerChn
}

// NewArrowReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewArrowReaderChnl(sc StreamConfig) (readerChn chan *BatchReader) {
	readerChn = make(chan *BatchReader, 100)

	pipeR, pipeW := io.Pipe()

	tbw := int64(0)

	go func() {
		var aw *ArrowWriter
		var br *BatchReader
		var err error

		defer close(readerChn)

		nextPipe := func(batch *Batch) error {
			if aw != nil {
				err := aw.Close()
				if err != nil {
					return g.Error(err, "could not close arrow writer")
				}
			}

			if pipeW != nil {
				pipeW.Close() // close the prior writer after arrow writer is closed
			}
			tbw = 0 // reset

			// new reader
			pipeR, pipeW = io.Pipe()

			br = &BatchReader{batch, batch.Columns, pipeR, 0}
			readerChn <- br

			aw, err = NewArrowWriter(pipeW, batch.Columns)
			if err != nil {
				return g.Error(err, "could not create arrow writer")
			}

			return nil
		}

		for batch := range ds.BatchChan {
			if batch.ColumnsChanged() || batch.IsFirst() {
				err := nextPipe(batch)
				if err != nil {
					ds.Context.CaptureErr(err)
					return
				}
			}

			for row := range batch.Rows {
				err := aw.WriteRow(row)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}

				br.Counter++

				if (sc.FileMaxRows > 0 && br.Counter >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					err = nextPipe(batch)
					if err != nil {
						ds.Context.CaptureErr(err)
						return
					}
				}
			}
		}

		if aw != nil {
			err := aw.Close()
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "could not close final arrow writer"))
			}
		}
		if pipeW != nil {
			pipeW.Close()
		}

	}()

	return readerChn
}

// NewParquetArrowReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
// WARN: Not using this one since it doesn't write Decimals properly.
func (ds *Datastream) NewParquetArrowReaderChnl(sc StreamConfig) (readerChn chan *BatchReader) {
	readerChn = make(chan *BatchReader, 100)

	pipeR, pipeW := io.Pipe()

	tbw := int64(0)

	go func() {
		var pw *ParquetArrowWriter
		var br *BatchReader
		var err error

		defer close(readerChn)

		nextPipe := func(batch *Batch) error {
			if pw != nil {
				pw.Close()
			}

			pipeW.Close() // close the prior reader?
			tbw = 0       // reset

			// new reader
			pipeR, pipeW = io.Pipe()

			br = &BatchReader{batch, batch.Columns, pipeR, 0}
			readerChn <- br

			// default compression is snappy
			codec := arrowCompress.Codecs.Snappy

			switch sc.Compression {
			case SnappyCompressorType:
				codec = arrowCompress.Codecs.Snappy
			case ZStandardCompressorType:
				codec = arrowCompress.Codecs.Zstd
			case GzipCompressorType:
				codec = arrowCompress.Codecs.Gzip
			case NoneCompressorType:
				codec = arrowCompress.Codecs.Uncompressed
			}

			pw, err = NewParquetArrowWriter(pipeW, ds.Columns, codec)
			if err != nil {
				return g.Error(err, "could not create parquet writer")
			}

			return nil
		}

		for batch := range ds.BatchChan {
			if batch.ColumnsChanged() || batch.IsFirst() {
				err := nextPipe(batch)
				if err != nil {
					ds.Context.CaptureErr(err)
					return
				}
			}

			for row := range batch.Rows {
				err := pw.WriteRow(row)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}

				br.Counter++

				if (sc.FileMaxRows > 0 && br.Counter >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					err = nextPipe(batch)
					if err != nil {
						ds.Context.CaptureErr(err)
						return
					}
				}
			}
		}

		if pw != nil {
			pw.Close()
		}

		pipeW.Close()

	}()

	return readerChn
}

func (ds *Datastream) NewExcelReaderChnl(sc StreamConfig) (readerChn chan *BatchReader) {
	readerChn = make(chan *BatchReader, 100)
	xls := NewExcel()

	if sc.Sheet == "" {
		sc.Sheet = "Sheet1"
	}

	err := xls.WriteSheet(sc.Sheet, ds, "overwrite")
	if err != nil {
		ds.Context.CaptureErr(g.Error(err, "error writing sheet"))
		return
	}

	pipeR, pipeW := io.Pipe()

	go func() {
		defer close(readerChn)

		br := &BatchReader{ds.CurrentBatch, ds.Columns, pipeR, 0}
		readerChn <- br

		err = xls.WriteToWriter(pipeW)
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "error writing to excel file"))
		}

		pipeW.Close()
	}()

	return

}

// NewParquetReaderChnl provides a channel of readers as the limit is reached
// each channel flows as fast as the consumer consumes
func (ds *Datastream) NewParquetReaderChnl(sc StreamConfig) (readerChn chan *BatchReader) {
	readerChn = make(chan *BatchReader, 100)

	pipeR, pipeW := io.Pipe()

	tbw := int64(0)

	go func() {
		var pw *ParquetWriter
		var br *BatchReader
		var err error

		defer close(readerChn)

		nextPipe := func(batch *Batch) error {
			if pw != nil {
				pw.Close()
			}

			pipeW.Close() // close the prior reader?
			tbw = 0       // reset

			// new reader
			pipeR, pipeW = io.Pipe()

			br = &BatchReader{batch, batch.Columns, pipeR, 0}
			readerChn <- br

			// default compression is snappy
			var codec compress.Codec
			codec = &parquet.Snappy

			switch sc.Compression {
			case SnappyCompressorType:
				codec = &parquet.Snappy
			case ZStandardCompressorType:
				codec = &parquet.Zstd
			case GzipCompressorType:
				codec = &parquet.Gzip
			case NoneCompressorType:
				codec = &parquet.Uncompressed
			}

			pw, err = NewParquetWriterMap(pipeW, batch.Columns, codec)
			if err != nil {
				return g.Error(err, "could not create parquet writer")
			}

			return nil
		}

		for batch := range ds.BatchChan {
			if batch.ColumnsChanged() || batch.IsFirst() {
				err := nextPipe(batch)
				if err != nil {
					ds.Context.CaptureErr(err)
					return
				}
			}

			for row := range batch.Rows {

				err := pw.WriteRec(row)
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "error writing row"))
					ds.Context.Cancel()
					pipeW.Close()
					return
				}

				br.Counter++

				if (sc.FileMaxRows > 0 && br.Counter >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
					err = nextPipe(batch)
					if err != nil {
						ds.Context.CaptureErr(err)
						return
					}
				} else if sc.FileMaxRows == 0 && br.Counter == 10000000 {
					// memory can build up when writing a large dataset to a single parquet file
					// https://github.com/slingdata-io/sling-cli/issues/351
					g.Warn("writing a large dataset to a single parquet file can cause memory build-up. If memory consumption is high, try writing to multiple parquet files instead of one, with the file_max_rows target option.")
				}
			}
		}

		if pw != nil {
			pw.Close()
		}
		pipeW.Close()

	}()

	return readerChn
}

// NewCsvReader creates a Reader with limit. If limit == 0, then read all rows.
func (ds *Datastream) NewCsvReader(sc StreamConfig) *io.PipeReader {
	pipeR, pipeW := io.Pipe()

	tbw := int64(0)
	go func() {
		defer pipeW.Close()

		sp := NewStreamProcessor()
		sp.Config = sc // override with provided config

		// only process current batch
		var batch *Batch
		select {
		case batch = <-ds.BatchChan:
		default:
			batch = ds.LatestBatch()
		}
		if batch == nil {
			return
		}

		c := int64(0) // local counter
		w := csv.NewWriterSize(pipeW, 40960*10)
		w.Comma = ','
		if sp.Config.Delimiter != "" {
			w.Comma = []rune(sp.Config.Delimiter)[0]
		}

		if sp.Config.Header {
			bw, err := w.Write(batch.Columns.Names(false, true))
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing header"))
				ds.Context.Cancel()
			}
		}

		for row0 := range batch.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				row[i] = sp.CastToString(i, val, ds.Columns[i].Type)
			}
			bw, err := w.Write(row)
			tbw = tbw + cast.ToInt64(bw)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "error writing row"))
				ds.Context.Cancel()
				break
			}
			w.Flush()

			if (sc.FileMaxRows > 0 && c >= sc.FileMaxRows) || (sc.FileMaxBytes > 0 && tbw >= sc.FileMaxBytes) {
				return // close reader if limit is reached
			}
		}

	}()

	return pipeR
}

func (it *Iterator) Ds() *Datastream {
	return it.ds
}

func (it *Iterator) close() {
	it.Closed = true
}

func (it *Iterator) addNewColumns(newRowLen int) {
	mux := it.ds.Context.Mux
	if df := it.ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	mux.Lock()
	for j := len(it.ds.Columns); j < newRowLen; j++ {
		newColName := g.RandSuffix("col_", 3)
		it.ds.AddColumns(Columns{Column{
			Name:     newColName,
			Type:     StringType,
			Position: len(it.ds.Columns) + 1,
		}}, false)
	}
	mux.Unlock()
}

// BelowEqualIncrementalVal evaluates the incremental value against the incrementalCol
// this is used when the stream is a file with incremental mode
// (unable to filter at source like a database)
// it.incrementalVal and it.incrementalColI need to be set
func (it *Iterator) BelowEqualIncrementalVal() bool {

	if it.incrementalVal == nil || it.incrementalCol == "" {
		// no incremental val or col
		return false
	} else if it.incrementalColI == -1 {
		if col := it.ds.Columns.GetColumn(it.incrementalCol); col != nil {
			it.incrementalColI = col.Position - 1
		}
	}

	if it.incrementalColI == -1 || it.incrementalColI > len(it.ds.Columns) {
		// column index exceeds
		it.Context.CaptureErr(g.Error("unable to get it.incrementalCol: %d (len(it.ds.Columns) = %d)", it.incrementalColI, len(it.ds.Columns)))
		return false
	} else if it.incrementalColI > len(it.Row) {
		// column index exceeds row
		it.Context.CaptureErr(g.Error("unable to get it.incrementalCol: %d (len(it.Row) = %d)", it.incrementalColI, len(it.Row)))
		return false
	}

	incrementalCol := it.ds.Columns[it.incrementalColI]
	rowVal := it.Row[it.incrementalColI]

	if rowVal == nil {
		return true // consider as not above IncrementalVal
	}

again:
	switch iVal := it.incrementalVal.(type) {
	case string:
		switch {
		case incrementalCol.Type == "":
			it.incrementalVal = it.ds.Sp.ParseString(cast.ToString(it.incrementalVal))
			if _, ok := it.incrementalVal.(string); !ok {
				goto again // try other switch cases if not string
			}
		case incrementalCol.IsDatetime():
			incrementalValT, err := it.ds.Sp.ParseTime(iVal)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "unable to cast incremental value to time.Time: %s", iVal))
				it.incrementalVal = nil
				return false
			}
			it.incrementalVal = incrementalValT
			goto again
		case incrementalCol.IsInteger():
			incrementalValI, err := cast.ToInt64E(iVal)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "unable to cast incremental value to Int64: %s", iVal))
				it.incrementalVal = nil
				return false
			}
			it.incrementalVal = incrementalValI
		case incrementalCol.IsNumber():
			incrementalValF, err := cast.ToFloat64E(iVal)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "unable to cast incremental value to Float64: %s", iVal))
				it.incrementalVal = nil
				return false
			}
			it.incrementalVal = incrementalValF
		}
		return cast.ToString(iVal) >= cast.ToString(rowVal)
	case time.Time:
		rowValT, err := it.ds.Sp.ParseTime(rowVal)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "unable to cast row value to time.Time: %#v", rowVal))
			it.incrementalVal = nil
			return false
		}
		return iVal.UnixNano() >= rowValT.UnixNano()
	case int64:
		rowValI, err := cast.ToInt64E(rowVal)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "unable to cast row value to Int64: %#v", rowVal))
			it.incrementalVal = nil
			return false
		}
		return iVal >= rowValI
	case float64:
		rowValF, err := cast.ToFloat64E(rowVal)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "unable to cast row value to Float64: %#v", rowVal))
			it.incrementalVal = nil
			return false
		}
		return iVal >= rowValF
	default:
		return cast.ToString(iVal) >= cast.ToString(rowVal)
	}

	return false
}

func (it *Iterator) incrementStreamRowNum() {
	if it.dsBufferI == -1 {
		return
	}
	if it.prevStreamURL == it.ds.Metadata.StreamURL.Value {
		it.StreamRowNum++
	} else {
		it.prevStreamURL = it.ds.Metadata.StreamURL.Value
		it.StreamRowNum = 1
	}
}

func (it *Iterator) next() bool {
	it.RowIsCasted = false // reset RowIsCasted

	select {
	case <-it.Context.Ctx.Done():
		return false
	case it.Row = <-it.Reprocess:
		// g.DebugLow("Reprocess %s > %d", it.ds.ID, it.Counter)
		it.RowIsCasted = true // skip re-casting of single row
		return true
	default:
		if it.Closed {
			return false
		}

		// process ds Buffer, dsBufferI should be > -1
		if it.dsBufferI > -1 && it.dsBufferI < len(it.ds.Buffer) {
			it.Row = it.ds.Buffer[it.dsBufferI]
			if it.dsBufferI < len(it.dsBufferStream) && it.dsBufferStream[it.dsBufferI] != "" {
				it.ds.Metadata.StreamURL.Value = it.dsBufferStream[it.dsBufferI]
			}
			it.incrementStreamRowNum()
			it.dsBufferI++
			return true
		}

	processNext:
		next := it.nextFunc(it)
		if next {
			if it.BelowEqualIncrementalVal() {
				goto processNext
			}

			it.incrementStreamRowNum()
			it.Counter++

			// logic to improve perf but not checking if
			// df limit is reached each cycle
			if it.ds.df != nil && it.ds.df.Limit > 0 {
				it.limitCnt++

				if it.Counter > it.ds.df.Limit {
					return false
				} else if it.limitCnt >= 30 {
					// to check for limit reached each 30 rows
					it.limitCnt = 0
					if it.ds.df.Count() > it.ds.df.Limit {
						return false
					}
				}
			}
		}
		return next
	}
}

// WaitClosed waits until dataflow is closed
// hack to make sure all streams are pushed
func (ds *Datastream) WaitClosed() {
	for {
		if ds.closed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
