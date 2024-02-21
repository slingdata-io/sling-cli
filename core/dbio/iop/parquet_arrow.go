package iop

import (
	"encoding/binary"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/compress"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// ParquetArrowReader is a parquet reader object
type ParquetArrowReader struct {
	Path   string
	Reader *file.Reader
	Data   *Dataset

	selectedColIndices []int
	colMap             map[string]int
	nextRow            chan nextRow
	done               bool
}

type nextRow struct {
	row []any
	err error
}

func NewParquetArrowReader(reader *os.File, selected []string) (p *ParquetArrowReader, err error) {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			g.Warn("recovered from panic: %#v\n%s", r, string(debug.Stack()))
			err = g.Error("panic occurred! %#v", r)
		}
	}()

	r, err := file.NewParquetReader(reader)
	if err != nil {
		return p, g.Error(err, "could not open parquet reader")
	}

	p = &ParquetArrowReader{Reader: r, nextRow: make(chan nextRow, 10)}

	columns := p.Columns()
	p.colMap = columns.FieldMap(true)

	p.selectedColIndices = lo.Map(columns, func(c Column, i int) int { return i })

	if len(selected) > 0 {
		colMap := columns.FieldMap(true)
		p.selectedColIndices = []int{}
		for _, colName := range selected {
			if index, found := colMap[strings.ToLower(colName)]; found {
				p.selectedColIndices = append(p.selectedColIndices, index)
			} else {
				return p, g.Error("selected column '%s' not found", colName)
			}
		}
	}

	go p.readRowsLoop()

	return
}

func (p *ParquetArrowReader) Columns() Columns {
	s := p.Reader.MetaData().Schema
	cols := make(Columns, s.NumColumns())

	cTypeMap := map[schema.ConvertedType]ColumnType{
		// schema.ConvertedTypes.None:            StringType,
		schema.ConvertedTypes.UTF8:            StringType,
		schema.ConvertedTypes.Map:             JsonType,
		schema.ConvertedTypes.MapKeyValue:     JsonType,
		schema.ConvertedTypes.List:            JsonType,
		schema.ConvertedTypes.Enum:            StringType,
		schema.ConvertedTypes.Decimal:         DecimalType,
		schema.ConvertedTypes.Date:            DateType,
		schema.ConvertedTypes.TimeMillis:      StringType,
		schema.ConvertedTypes.TimeMicros:      StringType,
		schema.ConvertedTypes.TimestampMillis: DatetimeType,
		schema.ConvertedTypes.TimestampMicros: DatetimeType,
		schema.ConvertedTypes.Uint8:           IntegerType,
		schema.ConvertedTypes.Uint16:          IntegerType,
		schema.ConvertedTypes.Uint32:          BigIntType,
		schema.ConvertedTypes.Uint64:          BigIntType,
		schema.ConvertedTypes.Int8:            IntegerType,
		schema.ConvertedTypes.Int16:           IntegerType,
		schema.ConvertedTypes.Int32:           IntegerType,
		schema.ConvertedTypes.Int64:           BigIntType,
		schema.ConvertedTypes.JSON:            JsonType,
		schema.ConvertedTypes.BSON:            JsonType,
		schema.ConvertedTypes.Interval:        StringType,
		// schema.ConvertedTypes.NA:              StringType,
	}

	// lTypeMap := map[string]ColumnType{
	// 	schema.StringLogicalType{}.String():    StringType,
	// 	schema.MapLogicalType{}.String():       JsonType,
	// 	schema.ListLogicalType{}.String():      JsonType,
	// 	schema.EnumLogicalType{}.String():      StringType,
	// 	schema.DecimalLogicalType{}.String():   DecimalType,
	// 	schema.DateLogicalType{}.String():      DatetimeType,
	// 	schema.TimeLogicalType{}.String():      StringType,
	// 	schema.TimestampLogicalType{}.String(): DatetimeType,
	// 	schema.IntLogicalType{}.String():       BigIntType,
	// 	schema.UnknownLogicalType{}.String():   StringType,
	// 	schema.JSONLogicalType{}.String():      JsonType,
	// 	schema.BSONLogicalType{}.String():      JsonType,
	// 	schema.UUIDLogicalType{}.String():      StringType,
	// 	schema.IntervalLogicalType{}.String():  StringType,
	// 	schema.Float16LogicalType{}.String():   FloatType,
	// 	schema.NullLogicalType{}.String():      StringType,
	// 	schema.NoLogicalType{}.String():        StringType,
	// }

	pTypeMap := map[parquet.Type]ColumnType{
		parquet.Types.Boolean:           BoolType,
		parquet.Types.Int32:             IntegerType,
		parquet.Types.Int64:             BigIntType,
		parquet.Types.Int96:             BigIntType,
		parquet.Types.Float:             FloatType,
		parquet.Types.Double:            FloatType,
		parquet.Types.ByteArray:         StringType,
		parquet.Types.FixedLenByteArray: StringType,
	}

	for i := 0; i < s.NumColumns(); i++ {
		sCol := s.Column(i)
		col := Column{
			Name:     sCol.Name(),
			Position: i + 1,
			Metadata: map[string]string{},
		}

		pType := sCol.PhysicalType()
		lType := sCol.LogicalType()
		cType, decMeta := sCol.LogicalType().ToConvertedType()

		col.Metadata["primitiveType"] = pType.String()
		col.Metadata["logicalType"] = lType.String()
		col.Metadata["convertedType"] = cType.String()

		tsType, tsTypeOK := lType.(schema.TimestampLogicalType)
		if tsTypeOK && g.In(pType.String(), "INT32", "INT64", "INT96") {
			col.Type = DatetimeType
			col.Sourced = true
			col.DbType = tsType.String()
			payload, _ := tsType.MarshalJSON()
			m, _ := g.UnmarshalMap(string(payload))
			for k, v := range m {
				col.Metadata[k] = cast.ToString(v)
			}
		} else if colType, ok := cTypeMap[cType]; ok {
			col.Type = colType
			col.DbPrecision = cast.ToInt(decMeta.Precision)
			col.DbScale = cast.ToInt(decMeta.Scale)
			col.Sourced = true
			col.DbType = cType.String()
		} else if colType, ok := pTypeMap[pType]; ok {
			col.Type = colType
			col.Sourced = true
			col.DbType = pType.String()
		}

		// need to infer decimal length
		if col.Type == DecimalType && decMeta.Precision == 0 && decMeta.Scale == 0 {
			col.Sourced = false
		} else if col.Type == FloatType {
			col.Sourced = false
		}

		cols[i] = col
	}

	if len(p.selectedColIndices) > 0 {
		newCols := make(Columns, len(p.selectedColIndices))
		for i, ci := range p.selectedColIndices {
			newCols[i] = cols[ci]
		}
		return newCols
	}

	return cols
}

func (p *ParquetArrowReader) readRowsLoop() {
	count := 0
	columns := p.Columns()
	for r := 0; r < p.Reader.NumRowGroups(); r++ {
		rowGroup := p.Reader.RowGroup(r)
		rowGroupMeta := rowGroup.MetaData()

		scanners := make([]*ParquetArrowDumper, len(p.selectedColIndices))
		fields := make([]string, len(p.selectedColIndices))

		for i, colI := range p.selectedColIndices {
			_, err := rowGroupMeta.ColumnChunk(colI)
			if err != nil {
				p.nextRow <- nextRow{err: g.Error(err, "could not get ColumnChunk for column=%d", colI)}
				return
			}

			colReader, err := rowGroup.Column(colI)
			if err != nil {
				log.Fatalf("unable to fetch column=%d err=%s", colI, err)
				p.nextRow <- nextRow{err: g.Error(err, "unable to fetch column=%d", colI)}
				return
			}
			scanners[i] = NewParquetArrowDumper(colReader)
			fields[i] = colReader.Descriptor().Path()
		}

		for {
			done := false
			row := make([]any, len(p.selectedColIndices))
			for i, scanner := range scanners {
				col := columns[i]
				if val, ok := scanner.Next(); ok {
					switch col.Type {
					case DatetimeType:
						val, _ = convertTimestamp(col, val)
					}

					switch v := val.(type) {
					case parquet.ByteArray:
						if col.Type == DecimalType {
							val = DecimalByteArrayToString(v.Bytes(), col.DbPrecision, col.DbScale)
						} else {
							val = v.String()
						}
					case parquet.FixedLenByteArray:
						if col.Type == DecimalType {
							val = DecimalByteArrayToString(v.Bytes(), col.DbPrecision, col.DbScale)
						} else {
							val = v.String()
						}
					}

					row[i] = val
				} else {
					done = true
				}
			}

			if done {
				break
			}

			count++

			p.nextRow <- nextRow{row: row}

		}
	}

	p.done = true
	p.Reader.Close()
}

func (p *ParquetArrowReader) nextFunc(it *Iterator) bool {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			g.Warn("recovered from panic: %#v\n%s", r, string(debug.Stack()))
			err := g.Error("panic occurred! %#v", r)
			it.Context.CaptureErr(err)
		}
	}()

retry:
	select {
	case nextRow := <-p.nextRow:
		if err := nextRow.err; err != nil {
			it.Context.CaptureErr(g.Error(err, "could not read Parquet row"))
			return false
		}
		it.Row = nextRow.row
		return true
	default:
	}

	if !p.done {
		time.Sleep(10 * time.Millisecond)
		goto retry
	}

	return false
}

type ParquetArrowDumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16

	valueBuffer  interface{}
	valueBufferR reflect.Value
}

func NewParquetArrowDumper(ccReader file.ColumnChunkReader) *ParquetArrowDumper {
	const defaultBatchSize = 128
	batchSize := defaultBatchSize

	var valueBuffer interface{}
	switch ccReader.(type) {
	case *file.BooleanColumnChunkReader:
		valueBuffer = make([]bool, batchSize)
	case *file.Int32ColumnChunkReader:
		valueBuffer = make([]int32, batchSize)
	case *file.Int64ColumnChunkReader:
		valueBuffer = make([]int64, batchSize)
	case *file.Float32ColumnChunkReader:
		valueBuffer = make([]float32, batchSize)
	case *file.Float64ColumnChunkReader:
		valueBuffer = make([]float64, batchSize)
	case *file.Int96ColumnChunkReader:
		valueBuffer = make([]parquet.Int96, batchSize)
	case *file.ByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.ByteArray, batchSize)
	case *file.FixedLenByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.FixedLenByteArray, batchSize)
	}

	return &ParquetArrowDumper{
		reader:      ccReader,
		batchSize:   int64(batchSize),
		defLevels:   make([]int16, batchSize),
		repLevels:   make([]int16, batchSize),
		valueBuffer: valueBuffer,
	}
}

func (pad *ParquetArrowDumper) readNextBatch() {
	switch reader := pad.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values := pad.valueBuffer.([]bool)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.Int32ColumnChunkReader:
		values := pad.valueBuffer.([]int32)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.Int64ColumnChunkReader:
		values := pad.valueBuffer.([]int64)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.Float32ColumnChunkReader:
		values := pad.valueBuffer.([]float32)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.Float64ColumnChunkReader:
		values := pad.valueBuffer.([]float64)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.Int96ColumnChunkReader:
		values := pad.valueBuffer.([]parquet.Int96)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values := pad.valueBuffer.([]parquet.ByteArray)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values := pad.valueBuffer.([]parquet.FixedLenByteArray)
		pad.levelsBuffered, pad.valuesBuffered, _ = reader.ReadBatch(pad.batchSize, values, pad.defLevels, pad.repLevels)
	}

	pad.valueOffset = 0
	pad.levelOffset = 0
}

func (pad *ParquetArrowDumper) hasNext() bool {
	return pad.levelOffset < pad.levelsBuffered || pad.reader.HasNext()
}

func (pad *ParquetArrowDumper) Next() (interface{}, bool) {
	if pad.levelOffset == pad.levelsBuffered {
		if !pad.hasNext() {
			return nil, false
		}
		pad.readNextBatch()
		if pad.levelsBuffered == 0 {
			return nil, false
		}

		pad.valueBufferR = reflect.ValueOf(pad.valueBuffer)
	}

	defLevel := pad.defLevels[int(pad.levelOffset)]
	// repLevel := dump.repLevels[int(dump.levelOffset)]
	pad.levelOffset++

	if defLevel < pad.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	v := pad.valueBufferR.Index(pad.valueOffset).Interface()
	pad.valueOffset++

	return v, true
}

func convertTimestamp(col Column, val any) (newVal any, err error) {
	if int96, ok := val.(parquet.Int96); ok && col.Metadata["primitiveType"] == "INT96" {
		return int96ToTimestamp(int96, cast.ToString(col.Metadata["timeUnit"])), nil
	}

	valInt, err := cast.ToInt64E(val)
	if err != nil {
		return val, g.Error(err, "could not convert to int64 for timestamp")
	}

	switch cast.ToString(col.Metadata["timeUnit"]) {
	case "milliseconds":
		return time.UnixMilli(valInt), nil
	case "microseconds":
		return time.UnixMicro(valInt), nil
	case "nanoseconds":
		return time.Unix(valInt/1e9, (valInt % 1e9)), nil
	default:
		return val, g.Error("timeUnit is unknown for timestamp column: %s", col.Name)
	}
}

const julianUnixEpoch int64 = 2440588
const milliSecondsPerDay = 24 * 3600e3
const microSecondsPerDay = 24 * 3600e6
const nanoSecondsPerDay = 24 * 3600e9

var parseInt96AsTimestamp = false

func int96ToTimestamp(val parquet.Int96, unit string) (t time.Time) {
	nanos := binary.LittleEndian.Uint64(val[:8])
	jDays := binary.LittleEndian.Uint32(val[8:])

	switch unit {
	case "milliseconds":
		mSec := int64(nanos/1000000) + (int64(jDays)-julianUnixEpoch)*milliSecondsPerDay
		t = time.Unix(mSec/1e3, (mSec%1e3)*1e6).UTC()
	case "microseconds":
		uSec := int64(nanos/1000) + (int64(jDays)-julianUnixEpoch)*microSecondsPerDay
		t = time.Unix(uSec/1e6, (uSec%1e6)*1e3).UTC()
	case "nanoseconds":
		nanos = (uint64(jDays)-uint64(julianUnixEpoch))*uint64(nanoSecondsPerDay) + nanos
		t = time.Unix(0, int64(nanos)).UTC()
	}

	return t
}

var parquetMapPhysicalType = map[ColumnType]parquet.Type{
	BigIntType:     parquet.Types.Int64,
	BinaryType:     parquet.Types.ByteArray,
	BoolType:       parquet.Types.Boolean,
	DatetimeType:   parquet.Types.Int64,
	DateType:       parquet.Types.Int64,
	DecimalType:    parquet.Types.ByteArray,
	FloatType:      parquet.Types.Double,
	IntegerType:    parquet.Types.Int64,
	JsonType:       parquet.Types.ByteArray,
	SmallIntType:   parquet.Types.Int32,
	StringType:     parquet.Types.ByteArray,
	TextType:       parquet.Types.ByteArray,
	TimestampType:  parquet.Types.Int64,
	TimestampzType: parquet.Types.Int64,
	TimeType:       parquet.Types.ByteArray,
	TimezType:      parquet.Types.ByteArray,
}

type ParquetArrowWriter struct {
	Writer          *file.Writer
	columns         Columns
	rowGroup        file.BufferedRowGroupWriter
	colWriters      []file.ColumnChunkWriter
	colValuesBuffer [][]any
	decNumScale     []*big.Rat
}

func NewParquetArrowWriter(w io.Writer, columns Columns, codec compress.Compression) (p *ParquetArrowWriter, err error) {

	p = &ParquetArrowWriter{columns: columns, decNumScale: make([]*big.Rat, len(columns))}
	schema, err := p.makeSchema()
	if err != nil {
		return nil, g.Error(err, "could not make schema")
	} else if len(p.Columns()) == 0 {
		return nil, g.Error("no columns provided")
	}

	opts := make([]parquet.WriterProperty, 0)
	for i := range p.Columns() {
		opts = append(opts, parquet.WithCompressionFor(schema.Column(i).Name(), codec))
	}

	props := parquet.NewWriterProperties(opts...)
	p.Writer = file.NewParquetWriter(w, schema.Root(), file.WithWriterProps(props))

	err = p.AppendNewRowGroup()
	if err != nil {
		return nil, g.Error(err, "could not append row group")
	}

	return p, nil
}

func (p *ParquetArrowWriter) AppendNewRowGroup() (err error) {
	if p.rowGroup != nil {
		err = p.rowGroup.Close()
		if err != nil {
			return g.Error(err, "could not close rowGroup")
		}
	}

	p.rowGroup = p.Writer.AppendBufferedRowGroup()
	p.colValuesBuffer = make([][]any, len(p.Columns()))
	p.colWriters = make([]file.ColumnChunkWriter, len(p.Columns()))
	for i := range p.Columns() {
		p.colWriters[i], err = p.rowGroup.Column(i)
		if err != nil {
			return g.Error(err, "could not make colWriter from rowGroup")
		}
	}

	return nil
}

func (p *ParquetArrowWriter) Close() (err error) {

	// any remaining
	err = p.writeBuffer()
	if err != nil {
		return g.Error(err, "could not write writeBuffer")
	}

	// close things
	for i := range p.colWriters {
		err = p.colWriters[i].Close()
		if err != nil {
			return g.Error(err, "could not close colWriter")
		}

		// get bytes written
		_ = p.colWriters[i].TotalBytesWritten()
	}

	err = p.rowGroup.Close()
	if err != nil {
		return g.Error(err, "could not close rowGroup")
	}

	err = p.Writer.Close()
	if err != nil {
		return g.Error(err, "could not close writer")
	}

	return nil
}

func (p *ParquetArrowWriter) Columns() Columns {
	return p.columns
}

func (p *ParquetArrowWriter) makeSchema() (s *schema.Schema, err error) {
	rep := parquet.Repetitions.Undefined
	fields := make([]schema.Node, len(p.Columns()))
	for i, col := range p.Columns() {
		fields[i], _ = schema.NewPrimitiveNode(col.Name, rep, parquetMapPhysicalType[col.Type], -1, 12)

		pType := parquetMapPhysicalType[col.Type]

		var node schema.Node
		fieldID := int32(-1)
		switch {
		case col.Type == FloatType:
			node, err = schema.NewPrimitiveNode(col.Name, rep, pType, -1, -1)
		case col.Type == DecimalType:
			rep = parquet.Repetitions.Required
			col.DbPrecision = lo.Ternary(col.DbPrecision == 0, 28, col.DbPrecision)
			col.DbScale = lo.Ternary(col.DbScale == 0, 9, col.DbScale)
			p.columns[i] = col
			lType := schema.NewDecimalLogicalType(int32(col.DbPrecision), int32(col.DbScale))
			node, err = schema.NewPrimitiveNodeLogical(col.Name, rep, lType, pType, 16, fieldID)
			if col.DbScale > 0 {
				p.decNumScale[i] = MakeDecNumScale(col.DbScale)
			} else {
				p.decNumScale[i] = MakeDecNumScale(1)
			}
		case col.IsInteger():
			node, err = schema.NewPrimitiveNode(col.Name, rep, pType, -1, -1)
		case col.IsDatetime():
			lType := schema.NewTimestampLogicalType(true, schema.TimeUnitNanos)
			node, err = schema.NewPrimitiveNodeLogical(col.Name, rep, lType, pType, -1, fieldID)
		case col.IsBool():
			node = schema.NewBooleanNode(col.Name, rep, fieldID)
		// case col.Type == JsonType:
		// 	node, err = schema.NewPrimitiveNodeConverted(col.Name, rep, pType, schema.ConvertedTypes.JSON, -1, 0, 0, fieldID)
		default:
			node, err = schema.NewPrimitiveNodeConverted(col.Name, rep, pType, schema.ConvertedTypes.UTF8, -1, 0, 0, fieldID)
		}
		if err != nil {
			return nil, g.Error(err, "could not get create parquet schema node for %s", col.Name)
		}
		fields[i] = node
	}

	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	return schema.NewSchema(node), nil
}

func (p *ParquetArrowWriter) writeBuffer() (err error) {
	for i, col := range p.Columns() {
		err = p.writeColumnValues(&col, p.colWriters[i], p.colValuesBuffer[i])
		if err != nil {
			return g.Error(err, "could not write colValuesBuffer")
		}
	}
	p.colValuesBuffer = make([][]any, len(p.Columns())) // reset
	return nil
}

func (p *ParquetArrowWriter) WriteRow(row []any) (err error) {
	for c, val := range row {
		if c < len(p.colValuesBuffer) {
			p.colValuesBuffer[c] = append(p.colValuesBuffer[c], val)
		}
	}

	if len(p.colValuesBuffer[0]) == 100 {
		err = p.writeBuffer()
		if err != nil {
			return g.Error(err, "could not write writeBuffer")
		}

		// size at 128MB per row group
		if p.rowGroup.TotalBytesWritten() >= 128*1000*1000 {
			err = p.AppendNewRowGroup()
			if err != nil {
				return g.Error(err, "could not append new rowGroup")
			}
		}
	}
	return
}

func (p *ParquetArrowWriter) writeColumnValues(col *Column, writer file.ColumnChunkWriter, colValuesBatch []any) (err error) {
	if len(colValuesBatch) == 0 {
		return
	}

	var offset int64
	defLevels := make([]int16, len(colValuesBatch))
	for j := range defLevels {
		defLevels[j] = 0
	}
	defLevels = nil

	switch w := writer.(type) {
	case *file.Int32ColumnChunkWriter:
		values := make([]int32, len(colValuesBatch))
		for i, val := range colValuesBatch {
			values[i] = cast.ToInt32(val)
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.Int64ColumnChunkWriter:
		values := make([]int64, len(colValuesBatch))
		for i, val := range colValuesBatch {
			if col.IsDatetime() {
				tVal, _ := cast.ToTimeE(val)
				values[i] = cast.ToInt64(tVal.UnixNano())
			} else {
				values[i] = cast.ToInt64(val)
			}
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.Float32ColumnChunkWriter:
		values := make([]float32, len(colValuesBatch))
		for i, val := range colValuesBatch {
			values[i] = cast.ToFloat32(val)
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.Float64ColumnChunkWriter:
		values := make([]float64, len(colValuesBatch))
		for i, val := range colValuesBatch {
			values[i] = cast.ToFloat64(val)
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.BooleanColumnChunkWriter:
		values := make([]bool, len(colValuesBatch))
		for i, val := range colValuesBatch {
			values[i] = cast.ToBool(val)
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	// case *file.Int96ColumnChunkWriter:
	// 	values := make([]parquet.Int96, len(colValuesBatch))
	// 	for i, val := range colValuesBatch {
	// 	}
	// 	offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.ByteArrayColumnChunkWriter:
		values := make([]parquet.ByteArray, len(colValuesBatch))
		for i, val := range colValuesBatch {
			valS := cast.ToString(val)
			if col.Type == DecimalType {
				values[i] = StringToDecimalByteArray(valS, p.decNumScale[col.Position-1], parquet.Types.ByteArray, 16)
			} else {
				values[i] = []byte(cast.ToString(val))
			}
		}
		offset, err = w.WriteBatch(values, defLevels, nil)
	case *file.FixedLenByteArrayColumnChunkWriter:
		values := make([]parquet.FixedLenByteArray, len(colValuesBatch))

		for i, val := range colValuesBatch {
			valS := cast.ToString(val)
			if col.Type == DecimalType {
				values[i] = StringToDecimalByteArray(valS, p.decNumScale[col.Position-1], parquet.Types.FixedLenByteArray, 16)
			} else {
				values[i] = []byte(cast.ToString(val))
			}
		}

		offset, err = w.WriteBatch(values, defLevels, nil)
	default:
		err = g.Error("unimplemented ColumnChunkWriter: %#v", w)
		return
	}
	_ = offset

	return
}

// DecimalByteArrayToString converts bytes to decimal string
// from https://github.com/xitongsys/parquet-go/blob/8ca067b2bd324788a77bf61d4e1ef9a5f8b4b1d2/types/converter.go#L131
func DecimalByteArrayToString(dec []byte, precision int, scale int) string {
	sign := ""
	if dec[0] > 0x7f {
		sign = "-"
		for i := range dec {
			dec[i] = dec[i] ^ 0xff
		}
	}

	a := new(big.Int)
	a.SetBytes(dec)
	if sign == "-" {
		a = a.Add(a, big.NewInt(1))
	}
	sa := a.Text(10)

	if scale > 0 {
		ln := len(sa)
		if ln < scale+1 {
			sa = strings.Repeat("0", scale+1-ln) + sa
			ln = scale + 1
		}
		sa = sa[:ln-scale] + "." + sa[ln-scale:]
	}
	return sign + sa
}

func MakeDecNumScale(scale int) *big.Rat {
	numSca := big.NewRat(1, 1)
	for i := 0; i < scale; i++ {
		numSca.Mul(numSca, big.NewRat(10, 1))
	}
	return numSca
}

// StringToDecimalByteArray converts a string decimal to bytes
// improvised from https://github.com/xitongsys/parquet-go/blob/8ca067b2bd324788a77bf61d4e1ef9a5f8b4b1d2/types/types.go#L81
// This function is costly, and slows write dramatically. TODO: Find ways to optimize, if possible
func StringToDecimalByteArray(s string, numSca *big.Rat, pType parquet.Type, length int) (decBytes []byte) {

	num := new(big.Rat)
	num.SetString(s)
	num.Mul(num, numSca)

	if pType == parquet.Types.Int32 {
		tmp, _ := num.Float64()
		return []byte(cast.ToString(int32(tmp)))

	} else if pType == parquet.Types.Int64 {
		tmp, _ := num.Float64()
		return []byte(cast.ToString(int64(tmp)))

	} else if pType == parquet.Types.FixedLenByteArray {
		s = num.String()
		res := StrIntToBinary(s, "BigEndian", length, strings.HasPrefix(s, "-"))
		return []byte(res)
	} else {
		s = num.String()
		res := StrIntToBinary(s, "BigEndian", length, strings.HasPrefix(s, "-"))
		return []byte(res)
	}
}

// order=LittleEndian or BigEndian; length is byte num
func StrIntToBinary(num string, order string, length int, signed bool) string {
	bigNum := new(big.Int)
	bigNum.SetString(num, 10)
	if !signed {
		res := bigNum.Bytes()
		if len(res) < length {
			res = append(make([]byte, length-len(res)), res...)
		}
		if order == "LittleEndian" {
			for i, j := 0, len(res)-1; i < j; i, j = i+1, j-1 {
				res[i], res[j] = res[j], res[i]
			}
		}
		if length > 0 {
			res = res[len(res)-length:]
		}
		return string(res)
	}

	flag := bigNum.Cmp(big.NewInt(0))
	if flag == 0 {
		if length <= 0 {
			length = 1
		}
		return string(make([]byte, length))
	}

	bigNum = bigNum.SetBytes(bigNum.Bytes())
	bs := bigNum.Bytes()

	if len(bs) < length {
		bs = append(make([]byte, length-len(bs)), bs...)
	}

	upperBs := make([]byte, len(bs))
	upperBs[0] = byte(0x80)
	upper := new(big.Int)
	upper.SetBytes(upperBs)
	if flag > 0 {
		upper = upper.Sub(upper, big.NewInt(1))
	}

	if bigNum.Cmp(upper) > 0 {
		bs = append(make([]byte, 1), bs...)
	}

	if flag < 0 {
		modBs := make([]byte, len(bs)+1)
		modBs[0] = byte(0x01)
		mod := new(big.Int)
		mod.SetBytes(modBs)
		bs = mod.Sub(mod, bigNum).Bytes()
	}
	if length > 0 {
		bs = bs[len(bs)-length:]
	}
	if order == "LittleEndian" {
		for i, j := 0, len(bs)-1; i < j; i, j = i+1, j-1 {
			bs[i], bs[j] = bs[j], bs[i]
		}
	}
	return string(bs)
}

func decimalFixedLenByteArraySize(precision int) int {
	return int(math.Ceil((math.Log10(2) + float64(precision)) / math.Log10(256)))
}
