package database

import (
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

// getOptimizeTestConn returns a Postgres connection for testing.
func getOptimizeTestConn(t *testing.T) Connection {
	t.Helper()

	db := DBs["postgres"]
	conn, err := connect(db)
	if err != nil {
		t.Skip("POSTGRES connection not available: " + err.Error())
		return nil
	}
	return conn
}

// TestGetOptimizeTableStatements_WithinTypeExpansion tests that
// GetOptimizeTableStatements detects and handles within-type expansion
// (e.g., VARCHAR(100) -> VARCHAR(500), DECIMAL(10,2) -> DECIMAL(18,6))
func TestGetOptimizeTableStatements_WithinTypeExpansion(t *testing.T) {

	conn := getOptimizeTestConn(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	tests := []struct {
		name        string
		tableCols   iop.Columns // existing table columns (target)
		newCols     iop.Columns // incoming columns (source)
		expectAlter bool        // whether ALTER should be generated
		desc        string
	}{
		{
			name: "string_expansion",
			tableCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 100, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 500, Sourced: true},
			},
			expectAlter: true,
			desc:        "VARCHAR(100) -> VARCHAR(500) should expand",
		},
		{
			name: "string_no_shrink",
			tableCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 500, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 100, Sourced: true},
			},
			expectAlter: false,
			desc:        "VARCHAR(500) -> VARCHAR(100) should NOT shrink",
		},
		{
			name: "string_equal",
			tableCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 255, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 255, Sourced: true},
			},
			expectAlter: false,
			desc:        "VARCHAR(255) -> VARCHAR(255) should be no-op",
		},
		{
			name: "string_not_sourced",
			tableCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 100, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 500, Sourced: false},
			},
			expectAlter: false,
			desc:        "Non-sourced new column should not trigger expansion",
		},
		{
			name: "decimal_precision_expansion",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 2, Sourced: true},
			},
			expectAlter: true,
			desc:        "DECIMAL(10,2) -> DECIMAL(18,2) should expand precision",
		},
		{
			name: "decimal_scale_expansion",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 6, Sourced: true},
			},
			expectAlter: true,
			desc:        "DECIMAL(10,2) -> DECIMAL(10,6) should expand scale",
		},
		{
			name: "decimal_both_expansion",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 6, Sourced: true},
			},
			expectAlter: true,
			desc:        "DECIMAL(10,2) -> DECIMAL(18,6) should expand both",
		},
		{
			name: "decimal_no_shrink",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 6, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			expectAlter: false,
			desc:        "DECIMAL(18,6) -> DECIMAL(10,2) should NOT shrink",
		},
		{
			name: "decimal_equal",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			expectAlter: false,
			desc:        "DECIMAL(10,2) -> DECIMAL(10,2) should be no-op",
		},
		{
			name: "decimal_not_sourced",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 6, Sourced: false},
			},
			expectAlter: false,
			desc:        "Non-sourced new column should not trigger expansion",
		},
		{
			name: "decimal_mixed_expansion",
			tableCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 2, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 6, Sourced: true},
			},
			expectAlter: true,
			desc:        "DECIMAL(18,2) -> DECIMAL(10,6): scale grew, should expand to max(18,10),max(2,6)",
		},
		{
			name: "cross_type_int_to_bigint",
			tableCols: iop.Columns{
				{Name: "id", Type: iop.IntegerType, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "id", Type: iop.BigIntType, Sourced: true},
			},
			expectAlter: true,
			desc:        "INT -> BIGINT cross-type promotion should still work",
		},
		{
			name: "cross_type_int_to_decimal",
			tableCols: iop.Columns{
				{Name: "val", Type: iop.IntegerType, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "val", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
			},
			expectAlter: true,
			desc:        "INT -> DECIMAL cross-type promotion should still work",
		},
		{
			name: "multi_column_mixed",
			tableCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 100, Sourced: true},
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 2, Sourced: true},
				{Name: "id", Type: iop.IntegerType, Sourced: true},
			},
			newCols: iop.Columns{
				{Name: "name", Type: iop.StringType, DbPrecision: 500, Sourced: true},
				{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 6, Sourced: true},
				{Name: "id", Type: iop.IntegerType, Sourced: true},
			},
			expectAlter: true,
			desc:        "Multiple columns: string expansion + decimal expansion + no-change integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := Table{
				Columns: tt.tableCols,
			}

			ok, ddlParts, err := GetOptimizeTableStatements(conn, &table, tt.newCols, false)
			assert.NoError(t, err, tt.desc)

			if tt.expectAlter {
				assert.True(t, ok, "%s: expected ALTER to be generated", tt.desc)
				assert.NotEmpty(t, ddlParts, "%s: expected DDL parts", tt.desc)
			} else {
				assert.False(t, ok, "%s: expected no ALTER", tt.desc)
				assert.Empty(t, ddlParts, "%s: expected no DDL parts", tt.desc)
			}
		})
	}
}

// TestGetOptimizeTableStatements_DecimalMaxPrecision verifies that when
// decimal expansion happens, the resulting column uses max(old, new) for
// both precision and scale.
func TestGetOptimizeTableStatements_DecimalMaxPrecision(t *testing.T) {

	conn := getOptimizeTestConn(t)
	if conn == nil {
		return
	}
	defer conn.Close()

	// DECIMAL(18,2) -> DECIMAL(10,6): precision stays 18, scale expands to 6
	table := Table{
		Columns: iop.Columns{
			{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 2, Sourced: true},
		},
	}
	newCols := iop.Columns{
		{Name: "amount", Type: iop.DecimalType, DbPrecision: 10, DbScale: 6, Sourced: true},
	}

	ok, _, err := GetOptimizeTableStatements(conn, &table, newCols, false)
	assert.NoError(t, err)
	assert.True(t, ok, "expected ALTER for scale expansion")

	// Verify the table column was updated with max values
	assert.Equal(t, 18, table.Columns[0].DbPrecision, "precision should be max(18,10) = 18")
	assert.Equal(t, 6, table.Columns[0].DbScale, "scale should be max(2,6) = 6")
}
