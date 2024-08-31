package iop

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestIcebergReaderLocal(t *testing.T) {
	testFile := "test/lineitem_iceberg"
	i, err := NewIcebergReader(testFile)
	assert.NoError(t, err)
	if !assert.NotNil(t, i) {
		return
	}

	testIcebergReader(t, i)

	// Test Close method
	err = i.Close()
	assert.NoError(t, err)
}

func TestIcebergReaderS3(t *testing.T) {
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

	uri := fmt.Sprintf("s3://%s/sling_test/lineitem_iceberg", fsProps["bucket"])

	props := g.MapToKVArr(map[string]string{"fs_props": g.Marshal(fsProps)})
	i, err := NewIcebergReader(uri, props...)
	assert.NoError(t, err)
	if !assert.NotNil(t, i) {
		return
	}

	testIcebergReader(t, i)

	// Test Close method
	err = i.Close()
	assert.NoError(t, err)
}

func testIcebergReader(t *testing.T, i *IcebergReader) {

	// Test Columns method
	columns, err := i.Columns()
	g.LogError(err)
	assert.NoError(t, err)
	assert.NotEmpty(t, columns)

	// Check if columns are correctly parsed
	expectedColumns := []struct {
		name     string
		dataType ColumnType
	}{
		{"l_orderkey", IntegerType},
		{"l_partkey", IntegerType},
		{"l_suppkey", IntegerType},
		{"l_linenumber", IntegerType},
		{"l_quantity", IntegerType},
		{"l_extendedprice", DecimalType},
		{"l_discount", DecimalType},
		{"l_tax", DecimalType},
		{"l_returnflag", StringType},
		{"l_linestatus", StringType},
		{"l_shipdate", DateType},
		{"l_commitdate", DateType},
		{"l_receiptdate", DateType},
		{"l_shipinstruct", StringType},
		{"l_shipmode", StringType},
		{"l_comment", StringType},
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
		limit := uint64(0) // No limit
		query := i.MakeSelectQuery(allFields, limit, "", nil)
		expectedQuery := fmt.Sprintf("select * from iceberg_scan('%s', allow_moved_paths = true)", i.URI)
		assert.Equal(t, expectedQuery, query, "Generated query should match expected query for all fields")

		// Test actual query execution
		ds, err := i.Duck.Stream(query)
		assert.NoError(t, err, "Streaming query should not produce an error")
		assert.NotNil(t, ds, "Datastream should not be nil")

		if ds != nil {
			data, err := ds.Collect(0) // Collect first 100 rows to avoid potential memory issues
			assert.NoError(t, err, "Collecting data should not produce an error")
			assert.NotEmpty(t, data.Rows, "Query should return non-empty result")
			assert.Equal(t, len(expectedColumns), len(data.Columns), "Result should have all columns")
			assert.Equal(t, 51793, len(data.Rows), "Result should have rows")

			// Verify column names
			for i, expected := range expectedColumns {
				assert.Equal(t, expected.name, data.Columns[i].Name, "Column name should match")
			}
		}
	})

	t.Run("Test MakeSelectQuery limited and query execution", func(t *testing.T) {
		// Test MakeSelectQuery method
		fields := []string{"l_orderkey", "l_quantity", "l_shipdate"}
		limit := uint64(10)
		query := i.MakeSelectQuery(fields, limit, "", nil)
		expectedQuery := fmt.Sprintf("select \"l_orderkey\",\"l_quantity\",\"l_shipdate\" from iceberg_scan('%s', allow_moved_paths = true) limit 10", i.URI)
		assert.Equal(t, expectedQuery, query, "Generated query should match expected query")

		// Test actual query execution
		ds, err := i.Duck.Stream(query)
		assert.NoError(t, err, "Streaming query should not produce an error")
		assert.NotNil(t, ds, "Datastream should not be nil")

		if ds != nil {
			data, err := ds.Collect(0)
			assert.NoError(t, err, "Collecting data should not produce an error")
			assert.NotEmpty(t, data.Rows, "Query should return non-empty result")
			assert.Equal(t, 3, len(data.Columns), "Result should have 3 columns")
			assert.Equal(t, 10, len(data.Rows), "Result should have at most 10 rows")
		}
	})

	t.Run("Test MakeSelectQuery with incremental key and value", func(t *testing.T) {
		// Test MakeSelectQuery method with incremental key and value
		fields := []string{"l_orderkey", "l_quantity", "l_shipdate", "l_commitdate"}
		limit := uint64(0) // No limit
		incrementalKey := "l_commitdate"
		incrementalValue := "'1996-01-01'"
		query := i.MakeSelectQuery(fields, limit, incrementalKey, incrementalValue)
		expectedQuery := fmt.Sprintf("select \"l_orderkey\",\"l_quantity\",\"l_shipdate\",\"l_commitdate\" from iceberg_scan('%s', allow_moved_paths = true) where \"l_commitdate\" > '1996-01-01'", i.URI)
		assert.Equal(t, expectedQuery, query, "Generated query should match expected query with incremental key and value")

		// Test actual query execution
		ds, err := i.Duck.Stream(query)
		assert.NoError(t, err, "Streaming query should not produce an error")
		assert.NotNil(t, ds, "Datastream should not be nil")

		if ds != nil {
			data, err := ds.Collect(0)
			assert.NoError(t, err, "Collecting data should not produce an error")
			assert.NotEmpty(t, data.Rows, "Query should return non-empty result")
			assert.Equal(t, 4, len(data.Columns), "Result should have 4 columns")

			// Verify column names
			expectedColumnNames := []string{"l_orderkey", "l_quantity", "l_shipdate", "l_commitdate"}
			for i, expectedName := range expectedColumnNames {
				assert.Equal(t, expectedName, data.Columns[i].Name, "Column name should match")
			}

			// Verify that all returned rows have l_commitdate > '1996-01-01'
			for _, row := range data.Rows {
				commitDate := cast.ToTime(row[3])
				assert.NoError(t, err, "Parsing commit date should not produce an error")
				if !assert.True(t, commitDate.After(time.Date(1996, 1, 1, 0, 0, 0, 0, time.UTC)), "All returned rows should have l_commitdate > '1996-01-01'") {
					break
				}
			}
		}
	})

}
