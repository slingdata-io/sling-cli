package iop

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestArrowReadWrite(t *testing.T) {
	// Create test columns
	columns := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
		{Name: "value", Type: FloatType, Position: 3},
		{Name: "active", Type: BoolType, Position: 4},
		{Name: "dec_val", Type: DecimalType, DbPrecision: 38, DbScale: 6, Position: 5},
	}

	// Create a buffer to write to
	buf := &bytes.Buffer{}

	// Create writer
	writer, err := NewArrowWriter(buf, columns)
	assert.NoError(t, err)

	// Write some test data
	testData := [][]any{
		{1, "Alice", 10.5, true, "50000"},
		{2, "Bob", 20.3, false, "123.456"},
		{3, "Charlie", 30.7, true, "999.999995"},
		{4, nil, 40.2, nil, "1"},
		{5, "David", 50.1, true, "0.001"},
		{6, "Eve", 60.9, false, nil}, // Test null values
	}

	for _, row := range testData {
		err := writer.WriteRow(row)
		assert.NoError(t, err)
	}

	// Close writer
	err = writer.Close()
	assert.NoError(t, err)

	// Create a temp file from buffer
	tmpFile, err := os.CreateTemp("", "test_arrow_*.arrow")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Open file for reading
	readFile, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer readFile.Close()

	// Create reader
	reader, err := NewArrowReader(readFile, nil)
	assert.NoError(t, err)

	// Verify columns
	readCols := reader.Columns()
	assert.Equal(t, len(columns), len(readCols))
	for i, col := range columns {
		assert.Equal(t, col.Name, readCols[i].Name)
		assert.Equal(t, col.Type, readCols[i].Type)
	}

	// Read data using datastream
	ds := NewDatastreamContext(context.Background(), reader.columns)
	it := ds.NewIterator(reader.columns, reader.nextFunc)

	rowCount := 0
	for it.next() {
		row := it.Row
		assert.Equal(t, len(columns), len(row))

		// Verify data matches what we wrote
		if rowCount < len(testData) {
			expected := testData[rowCount]
			for i, val := range expected {
				if val == nil {
					assert.Nil(t, row[i])
				} else {
					// Handle type conversions for integer values
					if intVal, ok := val.(int); ok {
						assert.Equal(t, int64(intVal), row[i])
					} else if decVal, ok := val.(string); ok && i == 4 {
						// For decimal, compare string representation
						dec, err := decimal.NewFromString(decVal)
						assert.NoError(t, err)

						// The reader gives us a string, so we parse it back to decimal
						resDec, err := decimal.NewFromString(row[i].(string))
						assert.NoError(t, err)

						assert.True(t, dec.Equal(resDec), "expected %s, got %s", dec.String(), resDec.String())
					} else {
						assert.Equal(t, val, row[i])
					}
				}
			}
		}
		rowCount++
	}

	assert.Equal(t, len(testData), rowCount)
}

func TestArrowReaderWithSelectedColumns(t *testing.T) {
	// Create test columns
	columns := Columns{
		{Name: "id", Type: IntegerType, Position: 1},
		{Name: "name", Type: StringType, Position: 2},
		{Name: "value", Type: FloatType, Position: 3},
	}

	// Create a buffer to write to
	buf := &bytes.Buffer{}

	// Create writer and write test data
	writer, err := NewArrowWriter(buf, columns)
	assert.NoError(t, err)

	testData := [][]any{
		{1, "Alice", 10.5},
		{2, "Bob", 20.3},
	}

	for _, row := range testData {
		err := writer.WriteRow(row)
		assert.NoError(t, err)
	}

	err = writer.Close()
	assert.NoError(t, err)

	// Create a temp file from buffer
	tmpFile, err := os.CreateTemp("", "test_arrow_selected_*.arrow")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	assert.NoError(t, err)
	err = tmpFile.Close()
	assert.NoError(t, err)

	// Open file for reading with selected columns
	readFile, err := os.Open(tmpFile.Name())
	assert.NoError(t, err)
	defer readFile.Close()

	// Create reader with only "id" and "value" columns
	reader, err := NewArrowReader(readFile, []string{"id", "value"})
	assert.NoError(t, err)

	// Verify selected columns
	readCols := reader.Columns()
	assert.Equal(t, 2, len(readCols))
	assert.Equal(t, "id", readCols[0].Name)
	assert.Equal(t, "value", readCols[1].Name)

	// Read data using datastream
	ds := NewDatastreamContext(context.Background(), reader.columns)
	it := ds.NewIterator(reader.columns, reader.nextFunc)

	rowCount := 0
	for it.next() {
		row := it.Row
		assert.Equal(t, 2, len(row)) // Only 2 selected columns

		if rowCount < len(testData) {
			// Handle type conversions for integer values
			assert.Equal(t, int64(testData[rowCount][0].(int)), row[0]) // id
			assert.Equal(t, testData[rowCount][2], row[1])              // value (skipping name)
		}
		rowCount++
	}

	assert.Equal(t, len(testData), rowCount)
}
