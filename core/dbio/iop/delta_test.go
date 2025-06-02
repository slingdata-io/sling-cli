package iop

import (
	"fmt"
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestDeltaReaderLocal(t *testing.T) {
	testFile := "test/delta"
	d, err := NewDeltaReader(testFile)
	assert.NoError(t, err)
	if !assert.NotNil(t, d) {
		return
	}

	testDeltaReader(t, d)

	// Test Close method
	err = d.Close()
	assert.NoError(t, err)
}

func TestDeltaReaderS3(t *testing.T) {
	t.Skip("DuckDB S3 has issues")
	s3Creds := os.Getenv("AWS_S3")
	if s3Creds == "" {
		assert.Fail(t, "AWS_S3 environment variable not set")
		return
	}

	fsProps := map[string]string{}
	err := yaml.Unmarshal([]byte(s3Creds), &fsProps)
	if !assert.NoError(t, err) {
		return
	}

	uri := fmt.Sprintf("s3://%s/sling_test/delta", fsProps["bucket"])

	props := g.MapToKVArr(map[string]string{"fs_props": g.Marshal(fsProps)})
	d, err := NewDeltaReader(uri, props...)
	assert.NoError(t, err)
	if !assert.NotNil(t, d) {
		return
	}

	testDeltaReader(t, d)

	// Test Close method
	err = d.Close()
	assert.NoError(t, err)
}

func testDeltaReader(t *testing.T, d *DeltaReader) {

	// Test Columns method
	columns, err := d.Columns()
	if !assert.NoError(t, err) {
		return
	}
	assert.NotEmpty(t, columns)

	// Check if columns are correctly parsed
	expectedColumns := []struct {
		name     string
		dataType ColumnType
	}{
		{"first_name", TextType},
		{"last_name", TextType},
		{"country", TextType},
		{"continent", TextType},
	}

	if assert.Equal(t, len(expectedColumns), len(columns), "Number of columns should match") {
		for i, expected := range expectedColumns {
			assert.Equal(t, expected.name, columns[i].Name, "Column name should match")
			assert.Equal(t, expected.dataType, columns[i].Type, "Column type should match for: %s", columns[i].Name)
		}
	}

	t.Run("Test MakeSelectQuery all and query execution", func(t *testing.T) {
		// Test MakeSelectQuery method with all fields
		allFields := []string{"*"}
		query := d.MakeQuery(FileStreamConfig{Select: allFields})
		expectedQuery := g.F("select * from delta_scan('%s')", d.URI)
		assert.Equal(t, expectedQuery, query, "Generated query should match expected query for all fields")

		// Test actual query execution
		ds, err := d.Duck.Stream(query)
		assert.NoError(t, err, "Streaming query should not produce an error")
		assert.NotNil(t, ds, "Datastream should not be nil")

		if ds != nil {
			data, err := ds.Collect(0)
			assert.NoError(t, err, "Collecting data should not produce an error")
			assert.NotEmpty(t, data.Rows, "Query should return non-empty result")
			assert.Equal(t, len(expectedColumns), len(data.Columns), "Result should have all columns")
			// Assert the number of rows based on your test data
			assert.Equal(t, 5, len(data.Rows), "Result should have 1000 rows")

			// Verify column names
			for i, expected := range expectedColumns {
				assert.Equal(t, expected.name, data.Columns[i].Name, "Column name should match")
			}
		}
	})

	t.Run("Test MakeSelectQuery limited and query execution", func(t *testing.T) {
		// Test MakeSelectQuery method
		fields := []string{"first_name", "last_name", "country"}
		limit := 10
		query := d.MakeQuery(FileStreamConfig{Select: fields, Limit: limit})
		expectedQuery := g.F("select \"first_name\",\"last_name\",\"country\" from delta_scan('%s') limit 10", d.URI)
		assert.Equal(t, expectedQuery, query, "Generated query should match expected query")

		// Test actual query execution
		ds, err := d.Duck.Stream(query)
		assert.NoError(t, err, "Streaming query should not produce an error")
		assert.NotNil(t, ds, "Datastream should not be nil")

		if ds != nil {
			data, err := ds.Collect(0)
			assert.NoError(t, err, "Collecting data should not produce an error")
			assert.NotEmpty(t, data.Rows, "Query should return non-empty result")
			assert.Equal(t, 3, len(data.Columns), "Result should have 3 columns")
			assert.Equal(t, 5, len(data.Rows), "Result should have at most 10 rows")
		}
	})

	t.Run("Test FormatQuery", func(t *testing.T) {
		// Test FormatQuery method
		inputSQL := "SELECT * FROM {stream_scanner} WHERE column1 > 10"
		expectedSQL := g.F("SELECT * FROM delta_scan('%s') WHERE column1 > 10", d.URI)

		formattedSQL := d.MakeQuery(FileStreamConfig{SQL: inputSQL})
		assert.Equal(t, expectedSQL, formattedSQL, "Formatted query should match expected query")
	})
}
