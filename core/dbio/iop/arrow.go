package iop

import (
	"context"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/samber/lo"
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
	a.columns = a.arrowSchemaToColumns(a.schema)
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

// arrowSchemaToColumns converts arrow schema to Columns
func (a *ArrowReader) arrowSchemaToColumns(schema *arrow.Schema) Columns {
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
