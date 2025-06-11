package iop

import (
	"context"
	"io"
	"math/big"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// ArrowReader is a arrow reader object using arrow v18
type ArrowReader struct {
	Path    string
	Reader  *ipc.FileReader
	File    *os.File
	Data    *Dataset
	Context *g.Context
	Memory  memory.Allocator

	selectedColIndices []int
	colMap             map[string]int
	nextRow            chan nextRow
	done               bool
	schema             *arrow.Schema
	columns            Columns
}

func NewArrowReader(reader *os.File, selected []string) (a *ArrowReader, err error) {
	ctx := g.NewContext(context.Background())

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			ctx.CaptureErr(err)
		}
	}()

	mem := memory.NewGoAllocator()

	// Create arrow file reader
	arrowReader, err := ipc.NewFileReader(reader, ipc.WithAllocator(mem))
	if err != nil {
		return a, g.Error(err, "could not create arrow reader")
	}

	a = &ArrowReader{
		Reader:  arrowReader,
		File:    reader,
		nextRow: make(chan nextRow, 10),
		Context: ctx,
		Memory:  mem,
	}

	// Get schema
	a.schema = arrowReader.Schema()

	// Convert arrow schema to columns
	a.columns = ArrowSchemaToColumns(a.schema)
	a.colMap = a.columns.FieldMap(true)

	a.selectedColIndices = lo.Map(a.columns, func(c Column, i int) int { return i })

	if len(selected) > 0 {
		colMap := a.columns.FieldMap(true)
		a.selectedColIndices = []int{}
		for _, colName := range selected {
			if index, found := colMap[strings.ToLower(colName)]; found {
				a.selectedColIndices = append(a.selectedColIndices, index)
			} else {
				return a, g.Error("selected column '%s' not found", colName)
			}
		}
	}

	go a.readRowsLoop()

	return
}

func (a *ArrowReader) Columns() Columns {
	if len(a.selectedColIndices) > 0 && len(a.selectedColIndices) < len(a.columns) {
		newCols := make(Columns, len(a.selectedColIndices))
		for i, ci := range a.selectedColIndices {
			newCols[i] = a.columns[ci]
		}
		return newCols
	}
	return a.columns
}

// ArrowSchemaToColumns converts arrow schema to Columns
func ArrowSchemaToColumns(schema *arrow.Schema) Columns {
	cols := make(Columns, len(schema.Fields()))

	for i, field := range schema.Fields() {
		col := Column{
			Name:     field.Name,
			Position: i + 1,
			Metadata: map[string]string{},
		}

		// Map arrow types to column types
		switch field.Type.ID() {
		case arrow.BOOL:
			col.Type = BoolType
			col.DbType = "BOOL"
		case arrow.INT8, arrow.INT16, arrow.INT32:
			col.Type = IntegerType
			col.DbType = field.Type.String()
		case arrow.INT64:
			col.Type = BigIntType
			col.DbType = "INT64"
		case arrow.UINT8, arrow.UINT16, arrow.UINT32:
			col.Type = IntegerType
			col.DbType = field.Type.String()
		case arrow.UINT64:
			col.Type = BigIntType
			col.DbType = "UINT64"
		case arrow.FLOAT32, arrow.FLOAT64:
			col.Type = FloatType
			col.DbType = field.Type.String()
		case arrow.DECIMAL128:
			col.Type = DecimalType
			if dt, ok := field.Type.(*arrow.Decimal128Type); ok {
				col.DbPrecision = int(dt.Precision)
				col.DbScale = int(dt.Scale)
			}
			col.DbType = "DECIMAL"
		case arrow.DATE32:
			col.Type = DateType
			col.DbType = "DATE"
		case arrow.TIMESTAMP:
			col.Type = DatetimeType
			col.DbType = "TIMESTAMP"
			if tsType, ok := field.Type.(*arrow.TimestampType); ok {
				col.Metadata["timeUnit"] = tsType.Unit.String()
			}
		case arrow.STRING, arrow.LARGE_STRING:
			col.Type = StringType
			col.DbType = "STRING"
		case arrow.BINARY, arrow.LARGE_BINARY:
			col.Type = BinaryType
			col.DbType = "BINARY"
		default:
			col.Type = StringType
			col.DbType = field.Type.String()
		}

		col.Sourced = true
		cols[i] = col
	}

	return cols
}

func (a *ArrowReader) readRowsLoop() {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			a.Context.CaptureErr(err)
		}
		a.done = true
		close(a.nextRow)
	}()

	// Read all records
	for i := 0; i < a.Reader.NumRecords(); i++ {
		record, err := a.Reader.ReadAt(int64(i))
		if err != nil {
			a.nextRow <- nextRow{err: g.Error(err, "could not read record")}
			return
		}

		// Process all rows in the record
		numRows := int(record.NumRows())
		numCols := int(record.NumCols())

		// Create selected columns slice
		selectedCols := make([]arrow.Array, len(a.selectedColIndices))
		for j, idx := range a.selectedColIndices {
			if idx < numCols {
				selectedCols[j] = record.Column(idx)
			}
		}

		// Process rows
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			row := make([]any, len(a.selectedColIndices))

			for j, arr := range selectedCols {
				if arr != nil && arr.Len() > rowIdx {
					val := GetValueFromArrowArray(arr, rowIdx)
					row[j] = val
				}
			}

			a.nextRow <- nextRow{row: row}
		}

		// Release the record after processing
		record.Release()
	}
}

func (a *ArrowReader) nextFunc(it *Iterator) bool {
retry:
	select {
	case nextRow, ok := <-a.nextRow:
		if !ok {
			// Channel is closed, no more rows
			return false
		}
		if err := nextRow.err; err != nil {
			it.Context.CaptureErr(g.Error(err, "could not read Arrow row"))
			return false
		}
		it.Row = nextRow.row
		return true
	default:
	}

	if !a.done {
		time.Sleep(10 * time.Millisecond)
		goto retry
	}

	return false
}

// ArrowWriter is an arrow writer object using arrow v18
type ArrowWriter struct {
	Writer       *ipc.FileWriter
	columns      Columns
	arrowSchema  *arrow.Schema
	mem          memory.Allocator
	builders     []array.Builder
	rowsBuffered int
}

func NewArrowWriter(w io.Writer, columns Columns) (a *ArrowWriter, err error) {
	a = &ArrowWriter{
		columns: columns,
		mem:     memory.NewGoAllocator(),
	}

	// Create arrow schema from columns
	a.arrowSchema = ColumnsToArrowSchema(columns)

	// Create arrow file writer
	writer, err := ipc.NewFileWriter(w, ipc.WithSchema(a.arrowSchema), ipc.WithAllocator(a.mem))
	if err != nil {
		return nil, g.Error(err, "could not create arrow writer")
	}
	a.Writer = writer

	// Initialize builders
	a.builders = make([]array.Builder, len(columns))
	for i, field := range a.arrowSchema.Fields() {
		a.builders[i] = a.createBuilder(field.Type)
	}

	return a, nil
}

func (a *ArrowWriter) createBuilder(dtype arrow.DataType) array.Builder {
	switch dtype.ID() {
	case arrow.BOOL:
		return array.NewBooleanBuilder(a.mem)
	case arrow.INT32:
		return array.NewInt32Builder(a.mem)
	case arrow.INT64:
		return array.NewInt64Builder(a.mem)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(a.mem)
	case arrow.DECIMAL128:
		return array.NewDecimal128Builder(a.mem, dtype.(*arrow.Decimal128Type))
	case arrow.DATE32:
		return array.NewDate32Builder(a.mem)
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(a.mem, dtype.(*arrow.TimestampType))
	case arrow.STRING:
		return array.NewStringBuilder(a.mem)
	case arrow.BINARY:
		return array.NewBinaryBuilder(a.mem, dtype.(*arrow.BinaryType))
	default:
		return array.NewStringBuilder(a.mem)
	}
}

func (a *ArrowWriter) WriteRow(row []any) error {
	for i, val := range row {
		if i >= len(a.builders) {
			continue
		}

		builder := a.builders[i]

		if val == nil {
			builder.AppendNull()
			continue
		}

		AppendToBuilder(builder, &a.columns[i], val)
	}

	a.rowsBuffered++

	// Write batch every 10000 rows
	if a.rowsBuffered >= 10000 {
		return a.flushBatch()
	}

	return nil
}

func (a *ArrowWriter) flushBatch() error {
	if a.rowsBuffered == 0 {
		return nil
	}

	// Build arrays
	arrays := make([]arrow.Array, len(a.builders))
	for i, builder := range a.builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record batch
	batch := array.NewRecord(a.arrowSchema, arrays, int64(a.rowsBuffered))
	defer batch.Release()

	// Write the batch
	err := a.Writer.Write(batch)
	if err != nil {
		return g.Error(err, "could not write batch")
	}

	// Reset builders
	for i := range a.builders {
		a.builders[i].Release()
		a.builders[i] = a.createBuilder(a.arrowSchema.Field(i).Type)
	}

	a.rowsBuffered = 0
	return nil
}

func (a *ArrowWriter) Close() error {
	// Flush any remaining rows
	err := a.flushBatch()
	if err != nil {
		return err
	}

	// Release builders
	for _, builder := range a.builders {
		builder.Release()
	}

	// Close the writer
	return a.Writer.Close()
}

func (a *ArrowWriter) Columns() Columns {
	return a.columns
}

func ColumnsToArrowSchema(columns Columns) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		var arrowType arrow.DataType

		switch col.Type {
		case BoolType:
			arrowType = arrow.FixedWidthTypes.Boolean
		case IntegerType, SmallIntType:
			arrowType = arrow.PrimitiveTypes.Int32
		case BigIntType:
			arrowType = arrow.PrimitiveTypes.Int64
		case FloatType:
			arrowType = arrow.PrimitiveTypes.Float64
		case DecimalType:
			col.DbPrecision = lo.Ternary(col.DbPrecision == 0, int(env.DdlMinDecLength), col.DbPrecision)
			col.DbScale = lo.Ternary(col.DbScale == 0, env.DdlMinDecScale, col.DbScale)
			arrowType = &arrow.Decimal128Type{Precision: int32(col.DbPrecision), Scale: int32(col.DbScale)}
		case DateType:
			arrowType = arrow.FixedWidthTypes.Date32
		case DatetimeType, TimestampType, TimestampzType:
			arrowType = arrow.FixedWidthTypes.Timestamp_ns
			switch col.Metadata["timeUnit"] {
			case "s":
				arrowType = arrow.FixedWidthTypes.Timestamp_s
			case "ms":
				arrowType = arrow.FixedWidthTypes.Timestamp_ms
			case "us":
				arrowType = arrow.FixedWidthTypes.Timestamp_us
			case "ns":
				arrowType = arrow.FixedWidthTypes.Timestamp_ns
			default:
				arrowType = arrow.FixedWidthTypes.Timestamp_us // iceberg does not support nano, micro as default
			}
		case StringType, TextType, JsonType:
			arrowType = arrow.BinaryTypes.String
		case BinaryType:
			arrowType = arrow.BinaryTypes.Binary
		default:
			arrowType = arrow.BinaryTypes.String
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
		}
	}

	return arrow.NewSchema(fields, nil)
}

func AppendToBuilder(builder array.Builder, col *Column, val interface{}) {

	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.BinaryBuilder:
		b.Append([]byte(cast.ToString(val)))
	case *array.BinaryDictionaryBuilder:
		b.Append([]byte(cast.ToString(val)))
	case *array.BinaryViewBuilder:
		b.Append([]byte(cast.ToString(val)))
	case *extensions.Bool8Builder:
		b.Append(cast.ToBool(val))
	case *array.BooleanBuilder:
		b.Append(cast.ToBool(val))
	case *array.Date32Builder:
		tVal, _ := cast.ToTimeE(val)
		days := int32(tVal.Unix() / 86400)
		b.Append(arrow.Date32(days))
	case *array.Date64Builder:
		tVal, _ := cast.ToTimeE(val)
		b.Append(arrow.Date64(tVal.UnixMilli()))
	case *array.DayTimeIntervalBuilder:
		// DayTimeInterval expects days and milliseconds
		tVal, _ := cast.ToTimeE(val)
		days := int32(tVal.Unix() / 86400)
		millis := int32((tVal.Unix() % 86400) * 1000)
		b.Append(arrow.DayTimeInterval{Days: days, Milliseconds: millis})
	case *array.Decimal128Builder:
		valStr := cast.ToString(val)
		// Clean value (remove currency symbols, commas)
		cleanValue := strings.ReplaceAll(strings.ReplaceAll(valStr, ",", ""), "$", "")

		// Convert to decimal128 using big.Rat for exact arithmetic
		rat := new(big.Rat)
		if _, ok := rat.SetString(cleanValue); ok {
			// Get numerator and denominator
			num := rat.Num()
			denom := rat.Denom()

			// Scale for decimal places - use default if not set
			scaleToUse := col.DbScale
			if scaleToUse == 0 {
				scaleToUse = env.DdlMinDecScale
			}
			scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleToUse)), nil)

			// Calculate scaled value: num * scale / denom
			scaledNum := new(big.Int).Mul(num, scale)
			scaledVal := new(big.Int).Div(scaledNum, denom)

			// Convert to Decimal128
			dec128 := decimal128.FromBigInt(scaledVal)
			b.Append(dec128)
		} else {
			b.AppendNull()
		}
	case *array.Decimal256Builder:
		valStr := cast.ToString(val)
		// Clean value (remove currency symbols, commas)
		cleanValue := strings.ReplaceAll(strings.ReplaceAll(valStr, ",", ""), "$", "")

		// Convert to decimal256 using big.Rat for exact arithmetic
		rat := new(big.Rat)
		if _, ok := rat.SetString(cleanValue); ok {
			// Get numerator and denominator
			num := rat.Num()
			denom := rat.Denom()

			// Scale for decimal places - use default if not set
			scaleToUse := col.DbScale
			if scaleToUse == 0 {
				scaleToUse = env.DdlMinDecScale
			}
			scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scaleToUse)), nil)

			// Calculate scaled value: num * scale / denom
			scaledNum := new(big.Int).Mul(num, scale)
			scaledVal := new(big.Int).Div(scaledNum, denom)

			// Convert to Decimal256
			dec256 := decimal256.FromBigInt(scaledVal)
			b.Append(dec256)
		} else {
			b.AppendNull()
		}
	case *array.DenseUnionBuilder:
		// Dense unions require type code and child index
		// For now, append to the first child (string type)
		b.Append(0)
		child := b.Child(0).(*array.StringBuilder)
		child.Append(cast.ToString(val))
	case *array.DurationBuilder:
		// Duration in nanoseconds
		durVal := cast.ToDuration(val)
		b.Append(arrow.Duration(durVal.Nanoseconds()))
	case *array.ExtensionBuilder:
		// Extension types delegate to their storage type
		// This is a generic handler - specific extensions may need custom logic
		b.Builder.AppendValueFromString(cast.ToString(val))
	case *array.FixedSizeBinaryBuilder:
		bytes := []byte(cast.ToString(val))
		// Pad or truncate to match fixed size
		fixedSize := b.Type().(*arrow.FixedSizeBinaryType).ByteWidth
		if len(bytes) < fixedSize {
			// Pad with zeros
			padded := make([]byte, fixedSize)
			copy(padded, bytes)
			b.Append(padded)
		} else {
			// Truncate if too long
			b.Append(bytes[:fixedSize])
		}
	case *array.FixedSizeBinaryDictionaryBuilder:
		bytes := []byte(cast.ToString(val))
		// Pad or truncate to match fixed size
		fixedSize := b.Type().(*arrow.FixedSizeBinaryType).ByteWidth
		if len(bytes) < fixedSize {
			// Pad with zeros
			padded := make([]byte, fixedSize)
			copy(padded, bytes)
			b.Append(padded)
		} else {
			// Truncate if too long
			b.Append(bytes[:fixedSize])
		}
	case *array.FixedSizeListBuilder:
		// Fixed size lists require exactly n values
		// For simplicity, we'll append the same value n times
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		listSize := b.Type().(*arrow.FixedSizeListType).Len()
		for i := 0; i < int(listSize); i++ {
			// Append the value to the value builder
			// This assumes a string value builder
			if sb, ok := valueBuilder.(*array.StringBuilder); ok {
				sb.Append(cast.ToString(val))
			} else {
				valueBuilder.AppendNull()
			}
		}
	case *array.Float16Builder:
		b.Append(float16.New(cast.ToFloat32(val)))
	case *array.Float32Builder:
		b.Append(cast.ToFloat32(val))
	case *array.Float64Builder:
		b.Append(cast.ToFloat64(val))
	case *array.Int16Builder:
		b.Append(cast.ToInt16(val))
	case *array.Int32Builder:
		b.Append(cast.ToInt32(val))
	case *array.Int64Builder:
		b.Append(cast.ToInt64(val))
	case *array.Int8Builder:
		b.Append(cast.ToInt8(val))
	case *array.LargeListBuilder:
		// Handle as single-element list
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		if sb, ok := valueBuilder.(*array.StringBuilder); ok {
			sb.Append(cast.ToString(val))
		} else {
			valueBuilder.AppendNull()
		}
	case *array.LargeListViewBuilder:
		// Handle as single-element list view
		valueBuilder := b.ValueBuilder()
		if sb, ok := valueBuilder.(*array.StringBuilder); ok {
			sb.Append(cast.ToString(val))
		} else {
			valueBuilder.AppendNull()
		}
		b.Append(true)
	case *array.LargeStringBuilder:
		b.Append(cast.ToString(val))
	case *array.ListBuilder:
		// Handle as single-element list
		b.Append(true)
		valueBuilder := b.ValueBuilder()
		if sb, ok := valueBuilder.(*array.StringBuilder); ok {
			sb.Append(cast.ToString(val))
		} else {
			valueBuilder.AppendNull()
		}
	case *array.ListViewBuilder:
		// Handle as single-element list view
		valueBuilder := b.ValueBuilder()
		if sb, ok := valueBuilder.(*array.StringBuilder); ok {
			sb.Append(cast.ToString(val))
		} else {
			valueBuilder.AppendNull()
		}
		b.Append(true)
	case *array.MapBuilder:
		// Maps need key-value pairs
		// For simplicity, use the value as both key and value
		b.Append(true)
		keyBuilder := b.KeyBuilder()
		itemBuilder := b.ItemBuilder()
		if kb, ok := keyBuilder.(*array.StringBuilder); ok {
			kb.Append(cast.ToString(val))
		} else {
			keyBuilder.AppendNull()
		}
		if ib, ok := itemBuilder.(*array.StringBuilder); ok {
			ib.Append(cast.ToString(val))
		} else {
			itemBuilder.AppendNull()
		}
	case *array.MonthDayNanoIntervalBuilder:
		// MonthDayNanoInterval expects months, days, and nanoseconds
		tVal, _ := cast.ToTimeE(val)
		// Simple approximation: use 0 months, convert to days and nanos
		days := int32(tVal.Unix() / 86400)
		nanos := int64((tVal.Unix() % 86400) * 1e9)
		b.Append(arrow.MonthDayNanoInterval{Months: 0, Days: days, Nanoseconds: nanos})
	case *array.MonthIntervalBuilder:
		// Convert to months (approximate)
		tVal, _ := cast.ToTimeE(val)
		months := int32(tVal.Unix() / (86400 * 30))
		b.Append(arrow.MonthInterval(months))
	case *array.NullBuilder:
		// Null builder always appends null
		b.AppendNull()
	case *array.NullDictionaryBuilder:
		// Null dictionary builder always appends null
		b.AppendNull()
	case *array.RunEndEncodedBuilder:
		// Run-end encoded arrays need the value builder
		// This is complex and rarely used, so we'll skip for now
		b.AppendNull()
	case *array.SparseUnionBuilder:
		// Sparse unions require type code
		// For now, append to the first child (string type)
		b.Append(0)
		child := b.Child(0)
		if sb, ok := child.(*array.StringBuilder); ok {
			sb.Append(cast.ToString(val))
		} else {
			child.AppendNull()
		}
	case *array.StringBuilder:
		b.Append(cast.ToString(val))
	case *array.StringViewBuilder:
		b.Append(cast.ToString(val))
	case *array.StructBuilder:
		// Struct builder needs to append to each field
		// For simplicity, convert value to string and append to all string fields
		b.Append(true)
		for i := 0; i < b.NumField(); i++ {
			fieldBuilder := b.FieldBuilder(i)
			if sb, ok := fieldBuilder.(*array.StringBuilder); ok {
				sb.Append(cast.ToString(val))
			} else {
				fieldBuilder.AppendNull()
			}
		}
	case *array.Time32Builder:
		tVal, _ := cast.ToTimeE(val)
		// Time32 can be seconds or milliseconds since midnight
		dt := b.Type().(*arrow.Time32Type)
		if dt.Unit == arrow.Second {
			seconds := int32(tVal.Hour()*3600 + tVal.Minute()*60 + tVal.Second())
			b.Append(arrow.Time32(seconds))
		} else { // Millisecond
			millis := int32((tVal.Hour()*3600+tVal.Minute()*60+tVal.Second())*1000 + tVal.Nanosecond()/1e6)
			b.Append(arrow.Time32(millis))
		}
	case *array.Time64Builder:
		tVal, _ := cast.ToTimeE(val)
		// Time64 can be microseconds or nanoseconds since midnight
		dt := b.Type().(*arrow.Time64Type)
		if dt.Unit == arrow.Microsecond {
			micros := int64((tVal.Hour()*3600+tVal.Minute()*60+tVal.Second())*1e6 + tVal.Nanosecond()/1000)
			b.Append(arrow.Time64(micros))
		} else { // Nanosecond
			nanos := int64((tVal.Hour()*3600+tVal.Minute()*60+tVal.Second())*1e9 + tVal.Nanosecond())
			b.Append(arrow.Time64(nanos))
		}
	case *array.TimestampBuilder:
		tVal, _ := cast.ToTimeE(val)
		switch col.Metadata["timeUnit"] {
		case "s":
			b.Append(arrow.Timestamp(tVal.Unix()))
		case "ms":
			b.Append(arrow.Timestamp(tVal.UnixMilli()))
		case "us":
			b.Append(arrow.Timestamp(tVal.UnixMicro()))
		case "ns":
			b.Append(arrow.Timestamp(tVal.UnixNano()))
		default:
			b.Append(arrow.Timestamp(tVal.UnixMicro())) // iceberg does not support nano, micro as default
		}
	case *array.Uint16Builder:
		b.Append(cast.ToUint16(val))
	case *array.Uint32Builder:
		b.Append(cast.ToUint32(val))
	case *array.Uint64Builder:
		b.Append(cast.ToUint64(val))
	case *array.Uint8Builder:
		b.Append(cast.ToUint8(val))
	case *extensions.UUIDBuilder:
		// UUID builder expects a 16-byte array
		uuidStr := cast.ToString(val)
		// Remove hyphens from UUID string
		uuidStr = strings.ReplaceAll(uuidStr, "-", "")
		if len(uuidStr) == 32 {
			// Convert hex string to bytes
			var uuidBytes [16]byte
			for i := 0; i < 16; i++ {
				byte, _ := strconv.ParseUint(uuidStr[i*2:i*2+2], 16, 8)
				uuidBytes[i] = uint8(byte)
			}
			b.Append(uuidBytes)
		} else {
			b.AppendNull()
		}
	}
}

// GetValueFromArrowArray extracts a value from an arrow array at the given index
func GetValueFromArrowArray(arr arrow.Array, idx int) any {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {

	case *array.Binary:
		return string(a.Value(idx))
	case *array.BinaryView:
		return string(a.Value(idx))
	case *array.Boolean:
		return a.Value(idx)
	case *array.Date32:
		days := a.Value(idx)
		return time.Unix(int64(days)*86400, 0).UTC()
	case *array.Date64:
		ms := a.Value(idx)
		return time.Unix(int64(ms)/1000, (int64(ms)%1000)*1000000).UTC()
	case *array.DayTimeInterval:
		val := a.Value(idx)
		// Return as a duration combining days and milliseconds
		return time.Duration(int64(val.Days)*86400*1e9 + int64(val.Milliseconds)*1e6)
	case *array.Decimal128:
		val := a.Value(idx)
		// Convert decimal128 to string representation
		dt := a.DataType().(*arrow.Decimal128Type)
		return decimal128ToString(val, int(dt.Precision), int(dt.Scale))
	case *array.Decimal256:
		val := a.Value(idx)
		// Convert decimal256 to string representation
		dt := a.DataType().(*arrow.Decimal256Type)
		return decimal256ToString(val, int(dt.Precision), int(dt.Scale))
	case *array.DenseUnion:
		// Get the child array and value
		typeCode := a.TypeCode(idx)
		childID := a.ChildID(idx)
		child := a.Field(int(typeCode))
		return GetValueFromArrowArray(child, int(childID))
	case *array.Duration:
		val := a.Value(idx)
		dt := a.DataType().(*arrow.DurationType)
		switch dt.Unit {
		case arrow.Second:
			return time.Duration(val) * time.Second
		case arrow.Millisecond:
			return time.Duration(val) * time.Millisecond
		case arrow.Microsecond:
			return time.Duration(val) * time.Microsecond
		case arrow.Nanosecond:
			return time.Duration(val)
		}
		return time.Duration(val)
	case *array.FixedSizeBinary:
		return a.Value(idx)
	case *array.FixedSizeList:
		// Return as a slice of values
		listSize := a.DataType().(*arrow.FixedSizeListType).Len()
		start := idx * int(listSize)
		values := make([]any, listSize)
		valueArray := a.ListValues()
		for i := 0; i < int(listSize); i++ {
			values[i] = GetValueFromArrowArray(valueArray, start+i)
		}
		return values
	case *array.Float16:
		return float64(a.Value(idx).Float32())
	case *array.Float32:
		return float64(a.Value(idx))
	case *array.Float64:
		return a.Value(idx)
	case *array.Int16:
		return int64(a.Value(idx))
	case *array.Int32:
		return int64(a.Value(idx))
	case *array.Int64:
		return a.Value(idx)
	case *array.Int8:
		return int64(a.Value(idx))
	case *array.LargeList:
		// Return as a slice of values
		if a.IsNull(idx) {
			return nil
		}
		start, end := a.ValueOffsets(idx)
		values := make([]any, end-start)
		valueArray := a.ListValues()
		for i := int64(0); i < end-start; i++ {
			values[i] = GetValueFromArrowArray(valueArray, int(start+i))
		}
		return values
	case *array.LargeListView:
		// Return as a slice of values
		if a.IsNull(idx) {
			return nil
		}
		start, size := a.ValueOffsets(idx)
		values := make([]any, size)
		valueArray := a.ListValues()
		for i := int64(0); i < size; i++ {
			values[i] = GetValueFromArrowArray(valueArray, int(start+i))
		}
		return values
	case *array.LargeString:
		return a.Value(idx)
	case *array.List:
		// Return as a slice of values
		if a.IsNull(idx) {
			return nil
		}
		start, end := a.ValueOffsets(idx)
		values := make([]any, end-start)
		valueArray := a.ListValues()
		for i := 0; i < int(end-start); i++ {
			values[i] = GetValueFromArrowArray(valueArray, int(start)+i)
		}
		return values
	case *array.ListView:
		// Return as a slice of values
		if a.IsNull(idx) {
			return nil
		}
		start, size := a.ValueOffsets(idx)
		values := make([]any, size)
		valueArray := a.ListValues()
		for i := 0; i < int(size); i++ {
			values[i] = GetValueFromArrowArray(valueArray, int(start)+i)
		}
		return values
	case *array.Map:
		// Return as a map
		if a.IsNull(idx) {
			return nil
		}
		start, end := a.ValueOffsets(idx)
		result := make(map[any]any)
		keyArray := a.Keys()
		itemArray := a.Items()
		for i := start; i < end; i++ {
			key := GetValueFromArrowArray(keyArray, int(i))
			value := GetValueFromArrowArray(itemArray, int(i))
			result[key] = value
		}
		return result
	case *array.MonthDayNanoInterval:
		val := a.Value(idx)
		// Return as a duration combining months, days and nanoseconds
		// Approximate: 1 month = 30 days
		days := int64(val.Months)*30 + int64(val.Days)
		return time.Duration(days*86400*1e9 + val.Nanoseconds)
	case *array.MonthInterval:
		val := a.Value(idx)
		// Return as days (approximate: 1 month = 30 days)
		return int32(val) * 30
	case *array.Null:
		return nil
	case *array.RunEndEncoded:
		// Get the physical index and return value from values array
		physicalIdx := a.GetPhysicalIndex(idx)
		return GetValueFromArrowArray(a.Values(), physicalIdx)
	case *array.SparseUnion:
		// Get the type code and return value from corresponding child
		typeCode := a.TypeCode(idx)
		child := a.Field(int(typeCode))
		return GetValueFromArrowArray(child, idx)
	case *array.String:
		return a.Value(idx)
	case *array.StringView:
		return a.Value(idx)
	case *array.Struct:
		// Return as a map of field names to values
		if a.IsNull(idx) {
			return nil
		}
		result := make(map[string]any)
		dt := a.DataType().(*arrow.StructType)
		for i := 0; i < a.NumField(); i++ {
			field := dt.Field(i)
			fieldArray := a.Field(i)
			result[field.Name] = GetValueFromArrowArray(fieldArray, idx)
		}
		return result
	case *array.Time32:
		val := a.Value(idx)
		dt := a.DataType().(*arrow.Time32Type)
		// Convert to time since midnight
		if dt.Unit == arrow.Second {
			seconds := int64(val)
			return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(seconds) * time.Second)
		} else { // Millisecond
			millis := int64(val)
			return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(millis) * time.Millisecond)
		}
	case *array.Time64:
		val := a.Value(idx)
		dt := a.DataType().(*arrow.Time64Type)
		// Convert to time since midnight
		if dt.Unit == arrow.Microsecond {
			micros := int64(val)
			return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond)
		} else { // Nanosecond
			nanos := int64(val)
			return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(nanos))
		}
	case *array.Timestamp:
		val := a.Value(idx)
		tsType := a.DataType().(*arrow.TimestampType)
		switch tsType.Unit {
		case arrow.Second:
			return time.Unix(int64(val), 0).UTC()
		case arrow.Millisecond:
			return time.UnixMilli(int64(val)).UTC()
		case arrow.Microsecond:
			return time.UnixMicro(int64(val)).UTC()
		case arrow.Nanosecond:
			return time.Unix(0, int64(val)).UTC()
		}
	case *array.Uint16:
		return int64(a.Value(idx))
	case *array.Uint32:
		return int64(a.Value(idx))
	case *array.Uint64:
		return a.Value(idx)
	case *array.Uint8:
		return int64(a.Value(idx))

	default:
		// For any other type, convert to string
		return arr.ValueStr(idx)
	}

	return nil
}

// decimal128ToString converts a Decimal128 value to its string representation
func decimal128ToString(val decimal128.Num, precision, scale int) string {
	// Convert back to big.Int
	bi := val.BigInt()

	// Convert to string
	str := bi.String()
	negative := false
	if str[0] == '-' {
		negative = true
		str = str[1:]
	}

	// Pad with zeros if necessary
	if len(str) <= scale {
		str = strings.Repeat("0", scale-len(str)+1) + str
	}

	// Insert decimal point
	if scale > 0 {
		str = str[:len(str)-scale] + "." + str[len(str)-scale:]
	}

	// Add negative sign back if needed
	if negative {
		str = "-" + str
	}

	return str
}

func decimal256ToString(val decimal256.Num, precision, scale int) string {
	// Convert back to big.Int
	bi := val.BigInt()

	// Convert to string
	str := bi.String()
	negative := false
	if str[0] == '-' {
		negative = true
		str = str[1:]
	}

	// Pad with zeros if necessary
	if len(str) <= scale {
		str = strings.Repeat("0", scale-len(str)+1) + str
	}

	// Insert decimal point
	if scale > 0 {
		str = str[:len(str)-scale] + "." + str[len(str)-scale:]
	}

	// Add negative sign back if needed
	if negative {
		str = "-" + str
	}

	return str
}
