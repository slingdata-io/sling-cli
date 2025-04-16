package iop

import (
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestDataset_InferColumnTypes(t *testing.T) {
	tests := []struct {
		name           string
		dataset        Dataset
		expectedTypes  []ColumnType
		expectedMaxLen []int
	}{
		{
			name: "Empty dataset",
			dataset: Dataset{
				Columns: NewColumnsFromFields("col1", "col2"),
				Rows:    [][]any{},
				Sp:      NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{StringType, StringType},
		},
		{
			name: "All nulls",
			dataset: Dataset{
				Columns: NewColumnsFromFields("col1"),
				Rows:    [][]any{{nil}, {nil}, {nil}},
				Sp:      NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{StringType},
		},
		{
			name: "String values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("short_string", "long_string"),
				Rows: [][]any{
					{"abc", "this is a very long string that should be inferred as text type because it exceeds 255 characters. this is a very long string that should be inferred as text type because it exceeds 255 characters. this is a very long string that should be inferred as text type because it exceeds 255 characters."},
					{"def", "another long string"},
					{"ghi", "yet another long string"},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes:  []ColumnType{StringType, TextType},
			expectedMaxLen: []int{3, 296},
		},
		{
			name: "Integer values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("small_int", "big_int"),
				Rows: [][]any{
					{123, 9223372036854775807},
					{456, 9223372036854775806},
					{789, 9223372036854775805},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{IntegerType, BigIntType},
		},
		{
			name: "Decimal values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("decimal_col"),
				Rows: [][]any{
					{123.45},
					{456.78},
					{789.01},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{DecimalType},
		},
		{
			name: "Boolean values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("bool_col"),
				Rows: [][]any{
					{true},
					{false},
					{true},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{BoolType},
		},
		{
			name: "Date values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("date_col"),
				Rows: [][]any{
					{time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)},
					{time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC)},
					{time.Date(2022, 1, 3, 0, 0, 0, 0, time.UTC)},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{DateType},
		},
		{
			name: "Datetime values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("datetime_col", "datetimez_col"),
				Rows: [][]any{
					{time.Date(2022, 1, 1, 12, 30, 45, 0, time.UTC), time.Date(2022, 1, 1, 12, 30, 45, 0, time.FixedZone("EST", -5*60*60))},
					{time.Date(2022, 1, 2, 13, 31, 46, 0, time.UTC), time.Date(2022, 1, 2, 13, 31, 46, 0, time.FixedZone("EST", -5*60*60))},
					{time.Date(2022, 1, 3, 14, 32, 47, 0, time.UTC), time.Date(2022, 1, 3, 14, 32, 47, 0, time.FixedZone("EST", -5*60*60))},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{DatetimeType, TimestampzType},
		},
		{
			name: "JSON values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("json_col"),
				Rows: [][]any{
					{`{"key": "value"}`},
					{`{"array": [1, 2, 3]}`},
					{`{"nested": {"obj": true}}`},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{JsonType},
		},
		{
			name: "Mixed values with nulls",
			dataset: Dataset{
				Columns: NewColumnsFromFields("mixed_col"),
				Rows: [][]any{
					{123},
					{456.78},
					{nil},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{DecimalType},
		},
		{
			name: "Mixed string and numbers - should default to string",
			dataset: Dataset{
				Columns: NewColumnsFromFields("mixed_col"),
				Rows: [][]any{
					{123},
					{"abc"},
					{456},
				},
				Sp: NewStreamProcessor(),
			},
			expectedTypes: []ColumnType{StringType},
		},
		// New tests for CSV-like string data
		{
			name: "CSV-like string integers",
			dataset: Dataset{
				Columns: NewColumnsFromFields("int_as_string"),
				Rows: [][]any{
					{"123"},
					{"456"},
					{"789"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize these as integers even though they're strings
			expectedTypes: []ColumnType{IntegerType},
		},
		{
			name: "CSV-like string decimals",
			dataset: Dataset{
				Columns: NewColumnsFromFields("decimal_as_string"),
				Rows: [][]any{
					{"123.45"},
					{"456.78"},
					{"789.01"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize these as decimals even though they're strings
			expectedTypes: []ColumnType{DecimalType},
		},
		{
			name: "CSV-like string booleans",
			dataset: Dataset{
				Columns: NewColumnsFromFields("bool_as_string"),
				Rows: [][]any{
					{"true"},
					{"false"},
					{"true"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize these as booleans even though they're strings
			expectedTypes: []ColumnType{BoolType},
		},
		{
			name: "CSV-like string dates",
			dataset: Dataset{
				Columns: NewColumnsFromFields("date_as_string"),
				Rows: [][]any{
					{"2022-01-01"},
					{"2022-01-02"},
					{"2022-01-03"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize these as dates even though they're strings
			expectedTypes: []ColumnType{DateType},
		},
		{
			name: "CSV-like string timestamps",
			dataset: Dataset{
				Columns: NewColumnsFromFields("iso_timestamp", "datetime_tz", "regional_timestamp", "short_timestamp"),
				Rows: [][]any{
					{"2022-01-01T12:30:45Z", "2022-01-01 12:30:45-05:00", "01/02/2022 12:30:45", "2022-01-01 12:30"},
					{"2022-01-02T13:31:46Z", "2022-01-02 13:31:46-05:00", "01/03/2022 13:31:46", "2022-01-02 13:31"},
					{"2022-01-03T14:32:47Z", "2022-01-03 14:32:47-05:00", "01/04/2022 14:32:47", "2022-01-03 14:32"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize these as datetimes even though they're strings
			expectedTypes: []ColumnType{DatetimeType, TimestampzType, DatetimeType, DatetimeType},
		},
		{
			name: "CSV-like mixed valid and invalid timestamps",
			dataset: Dataset{
				Columns: NewColumnsFromFields("valid_mixed", "invalid_timestamp"),
				Rows: [][]any{
					{"2022-01-01T12:30:45Z", "not-a-date"},
					{"01/02/2022 12:30:45", "timestamp-without-numbers"},
					{"2022-01-03 14:32:47", "2022:01:03"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize valid mixed timestamp formats but invalid ones should be string
			expectedTypes: []ColumnType{DatetimeType, StringType},
		},
		{
			name: "CSV-like timestamp with nulls",
			dataset: Dataset{
				Columns: NewColumnsFromFields("sparse_timestamp"),
				Rows: [][]any{
					{nil},
					{""},
					{"2022-01-01T12:30:45Z"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should still identify as timestamp even with sparse data
			expectedTypes: []ColumnType{DatetimeType},
		},
		{
			name: "CSV-like mixed integers and empty strings",
			dataset: Dataset{
				Columns: NewColumnsFromFields("mixed_int_empty"),
				Rows: [][]any{
					{"123"},
					{""},
					{"789"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should recognize as integers with empty strings treated as nulls
			expectedTypes: []ColumnType{IntegerType},
		},
		{
			name: "CSV-like mixed types",
			dataset: Dataset{
				Columns: NewColumnsFromFields("col1", "col2", "col3", "col4"),
				Rows: [][]any{
					{"123", "abc", "123.45", "true"},
					{"456", "def", "456.78", "false"},
					{"", "", "", ""},
					{"789", "ghi", "789.01", "true"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should correctly identify each column type
			expectedTypes: []ColumnType{IntegerType, StringType, DecimalType, BoolType},
		},
		{
			name: "Invalid string numbers mixed with valid ones",
			dataset: Dataset{
				Columns: NewColumnsFromFields("mixed_valid_invalid"),
				Rows: [][]any{
					{"123"},
					{"456abc"}, // Invalid number
					{"789"},
				},
				Sp: NewStreamProcessor(),
			},
			// Should default to string type when mixed valid/invalid numbers
			expectedTypes: []ColumnType{StringType},
		},
		{
			name: "Sparse dataset with few values",
			dataset: Dataset{
				Columns: NewColumnsFromFields("sparse_col"),
				Rows: [][]any{
					{nil},
					{nil},
					{nil},
					{nil},
					{nil},
					{nil},
					{nil},
					{nil},
					{nil},
					{"123"}, // Just one value
				},
				Sp: NewStreamProcessor(),
			},
			// Should still correctly identify the type from the sparse data
			expectedTypes: []ColumnType{IntegerType},
		},
	}

	// Store original sample size and restore after tests
	originalSampleSize := SampleSize
	defer func() { SampleSize = originalSampleSize }()

	// Use a small sample size for testing
	SampleSize = 10

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run inference
			tt.dataset.InferColumnTypes()

			// Check types
			actualTypes := lo.Map(tt.dataset.Columns, func(col Column, _ int) ColumnType {
				return col.Type
			})
			assert.Equal(t, tt.expectedTypes, actualTypes, "Column types don't match expected types")

			// If max lengths were specified, verify them
			if tt.expectedMaxLen != nil {
				actualMaxLen := lo.Map(tt.dataset.Columns, func(col Column, _ int) int {
					return col.Stats.MaxLen
				})
				assert.Equal(t, tt.expectedMaxLen, actualMaxLen, "Max lengths don't match expected values")
			}

			// Verify that Inferred flag is set
			assert.True(t, tt.dataset.Inferred, "Dataset should be marked as inferred")
		})
	}
}

// Test to ensure empty strings are properly handled
func TestEmptyStringHandling(t *testing.T) {
	ds := Dataset{
		Columns: NewColumnsFromFields("col1", "col2", "col3"),
		Rows: [][]any{
			{"123", "", "abc"},
			{"", "456", "def"},
			{"789", "123", ""},
		},
		Sp: NewStreamProcessor(),
	}

	ds.InferColumnTypes()

	assert.Equal(t, IntegerType, ds.Columns[0].Type, "Column with empty strings but valid integers should be inferred as integer")
	assert.Equal(t, IntegerType, ds.Columns[1].Type, "Column with empty strings but valid integers should be inferred as integer")
	assert.Equal(t, StringType, ds.Columns[2].Type, "Column with empty strings and text should be inferred as string")

	// Check how empty strings are counted
	assert.True(t, ds.Columns[0].Stats.NullCnt > 0, "Empty strings should be counted as nulls")
	assert.True(t, ds.Columns[1].Stats.NullCnt > 0, "Empty strings should be counted as nulls")
	assert.True(t, ds.Columns[2].Stats.NullCnt > 0, "Empty strings should be counted as nulls")
}
