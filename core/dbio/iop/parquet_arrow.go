package iop

import (
	"context"
	"io"
	"math/big"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/flarco/g"
	"github.com/samber/lo"
)

// ParquetArrowReader is a parquet reader object using arrow v18
type ParquetArrowReader struct {
	Path    string
	Reader  *pqarrow.FileReader
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

type nextRow struct {
	row []any
	err error
}

func NewParquetArrowReader(reader *os.File, selected []string) (p *ParquetArrowReader, err error) {
	ctx := g.NewContext(context.Background())

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			ctx.CaptureErr(err)
		}
	}()

	mem := memory.NewGoAllocator()

	// Create parquet file reader first
	pqFile, err := file.NewParquetReader(reader)
	if err != nil {
		return p, g.Error(err, "could not create parquet reader")
	}

	// Create arrow file reader
	pqReader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{Parallel: true}, mem)
	if err != nil {
		return p, g.Error(err, "could not open parquet reader")
	}

	p = &ParquetArrowReader{
		Reader:  pqReader,
		File:    reader,
		nextRow: make(chan nextRow, 10),
		Context: ctx,
		Memory:  mem,
	}

	// Get schema
	p.schema, err = pqReader.Schema()
	if err != nil {
		return p, g.Error(err, "could not get schema")
	}

	// Convert arrow schema to columns
	p.columns = ArrowSchemaToColumns(p.schema)
	p.colMap = p.columns.FieldMap(true)

	p.selectedColIndices = lo.Map(p.columns, func(c Column, i int) int { return i })

	if len(selected) > 0 {
		colMap := p.columns.FieldMap(true)
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
	if len(p.selectedColIndices) > 0 && len(p.selectedColIndices) < len(p.columns) {
		newCols := make(Columns, len(p.selectedColIndices))
		for i, ci := range p.selectedColIndices {
			newCols[i] = p.columns[ci]
		}
		return newCols
	}
	return p.columns
}

func (p *ParquetArrowReader) readRowsLoop() {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			p.Context.CaptureErr(err)
		}
		p.done = true
		close(p.nextRow)
	}()

	ctx := context.Background()

	// Read all data as a table
	table, err := p.Reader.ReadTable(ctx)
	if err != nil {
		p.nextRow <- nextRow{err: g.Error(err, "could not read table")}
		return
	}
	defer table.Release()

	// Process the entire table
	numCols := int(table.NumCols())
	numRows := int(table.NumRows())

	// Create selected columns slice
	selectedCols := make([]*arrow.Column, len(p.selectedColIndices))
	for i, idx := range p.selectedColIndices {
		if idx < numCols {
			selectedCols[i] = table.Column(idx)
		}
	}

	// Process rows
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		row := make([]any, len(p.selectedColIndices))

		for i, col := range selectedCols {
			if col != nil && col.Len() > rowIdx {
				val := p.getValueFromColumn(col, rowIdx, p.columns[p.selectedColIndices[i]])
				row[i] = val
			}
		}

		p.nextRow <- nextRow{row: row}
	}
}

// getValueFromColumn extracts a value from an arrow column at the given index
func (p *ParquetArrowReader) getValueFromColumn(col *arrow.Column, idx int, colMeta Column) any {
	if col == nil || col.Len() <= idx {
		return nil
	}

	// Process each chunk until we find the row
	chunkOffset := 0
	for i := 0; i < col.Data().Len(); i++ {
		chunk := col.Data().Chunk(i)
		if chunkOffset+chunk.Len() > idx {
			// Found the chunk containing our row
			localIdx := idx - chunkOffset
			return GetValueFromArrowArray(chunk, localIdx)
		}
		chunkOffset += chunk.Len()
	}

	return nil
}

func (p *ParquetArrowReader) nextFunc(it *Iterator) bool {
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

type ParquetArrowWriter struct {
	Writer        *pqarrow.FileWriter
	columns       Columns
	arrowSchema   *arrow.Schema
	mem           memory.Allocator
	builders      []array.Builder
	rowsBuffered  int
	decimalScales []*big.Rat
}

func NewParquetArrowWriter(w io.Writer, columns Columns, codec compress.Compression) (p *ParquetArrowWriter, err error) {
	p = &ParquetArrowWriter{
		columns:       columns,
		mem:           memory.NewGoAllocator(),
		decimalScales: make([]*big.Rat, len(columns)),
	}

	// Create arrow schema from columns
	p.arrowSchema = ColumnsToArrowSchema(columns)
	if err != nil {
		return nil, g.Error(err, "could not create arrow schema")
	}

	// Create parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithDictionaryDefault(true),
		parquet.WithVersion(parquet.V2_LATEST),
		parquet.WithCompression(codec),
		// parquet.WithBatchSize(1024*32),
		// parquet.WithDataPageSize(1024*1024),
		// parquet.WithMaxRowGroupLength(1024*1024),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	// Create the file writer
	p.Writer, err = pqarrow.NewFileWriter(p.arrowSchema, w, writerProps, arrowProps)
	if err != nil {
		return nil, g.Error(err, "could not create parquet writer")
	}

	// Initialize builders
	p.builders = make([]array.Builder, len(columns))
	for i, field := range p.arrowSchema.Fields() {
		p.builders[i] = p.createBuilder(field.Type)
	}

	return p, nil
}

func (p *ParquetArrowWriter) createBuilder(dtype arrow.DataType) array.Builder {
	switch dtype.ID() {
	case arrow.BOOL:
		return array.NewBooleanBuilder(p.mem)
	case arrow.INT32:
		return array.NewInt32Builder(p.mem)
	case arrow.INT64:
		return array.NewInt64Builder(p.mem)
	case arrow.FLOAT64:
		return array.NewFloat64Builder(p.mem)
	case arrow.DECIMAL128:
		return array.NewDecimal128Builder(p.mem, dtype.(*arrow.Decimal128Type))
	case arrow.DATE32:
		return array.NewDate32Builder(p.mem)
	case arrow.TIMESTAMP:
		return array.NewTimestampBuilder(p.mem, dtype.(*arrow.TimestampType))
	case arrow.STRING:
		return array.NewStringBuilder(p.mem)
	case arrow.BINARY:
		return array.NewBinaryBuilder(p.mem, dtype.(*arrow.BinaryType))
	default:
		return array.NewStringBuilder(p.mem)
	}
}

func (p *ParquetArrowWriter) WriteRow(row []any) error {
	for i, val := range row {
		if i >= len(p.builders) {
			continue
		}

		builder := p.builders[i]

		if val == nil {
			builder.AppendNull()
			continue
		}

		AppendToBuilder(builder, &p.columns[i], val)
	}

	p.rowsBuffered++

	// Write batch every 10000 rows
	if p.rowsBuffered >= 10000 {
		return p.flushBatch()
	}

	return nil
}

func (p *ParquetArrowWriter) flushBatch() error {
	if p.rowsBuffered == 0 {
		return nil
	}

	// Build arrays
	arrays := make([]arrow.Array, len(p.builders))
	for i, builder := range p.builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record batch
	batch := array.NewRecord(p.arrowSchema, arrays, int64(p.rowsBuffered))
	defer batch.Release()

	// Write the batch
	err := p.Writer.Write(batch)
	if err != nil {
		return g.Error(err, "could not write batch")
	}

	// Reset builders
	for i := range p.builders {
		p.builders[i].Release()
		p.builders[i] = p.createBuilder(p.arrowSchema.Field(i).Type)
	}

	p.rowsBuffered = 0
	return nil
}

func (p *ParquetArrowWriter) Close() error {
	// Flush any remaining rows
	err := p.flushBatch()
	if err != nil {
		return err
	}

	// Release builders
	for _, builder := range p.builders {
		builder.Release()
	}

	// Close the writer
	return p.Writer.Close()
}

func (p *ParquetArrowWriter) Columns() Columns {
	return p.columns
}

// Helper functions

func MakeDecNumScale(scale int) *big.Rat {
	numSca := big.NewRat(1, 1)
	for i := 0; i < scale; i++ {
		numSca.Mul(numSca, big.NewRat(10, 1))
	}
	return numSca
}

func StringToDecimalByteArray(s string, numSca *big.Rat, pType parquet.Type, length int) []byte {
	// This is now handled by arrow's decimal128 type
	return nil
}
