package database

import (
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

func TestZerobusConn_URLParsing(t *testing.T) {
	conn := &ZerobusConn{
		URL: "zerobus://my-user:my-token@adb-123.databricks.com/main.default.customers",
	}
	err := conn.Init()
	assert.NoError(t, err)

	assert.Equal(t, "adb-123.databricks.com", conn.WorkspaceURL)
	assert.Equal(t, "main", conn.Catalog)
	assert.Equal(t, "default", conn.Schema)
	assert.Equal(t, "customers", conn.Table)
	assert.Equal(t, "my-token", conn.Token)
	assert.Equal(t, "my-user", conn.ClientID)
	assert.Equal(t, 10000, conn.BatchSize)
	assert.Equal(t, "none", conn.Compression)
	assert.Equal(t, 1000, conn.MaxInflightBatches)
}

func TestZerobusConn_CustomOptions(t *testing.T) {
	conn := &ZerobusConn{
		WorkspaceURL: "adb-123.databricks.com",
		Token:        "my-token",
	}
	conn.BaseConn.props = map[string]interface{}{
		"batch_size":           5000,
		"compression":          "zstd",
		"max_inflight_batches": 250,
	}
	err := conn.Init()
	assert.NoError(t, err)

	assert.Equal(t, 5000, conn.BatchSize)
	assert.Equal(t, "zstd", conn.Compression)
	assert.Equal(t, 250, conn.MaxInflightBatches)
}

func TestZerobusConn_BulkImportFlow_ArrowBatches(t *testing.T) {
	conn := &ZerobusConn{
		WorkspaceURL: "adb-test.cloud.databricks.com",
		Token:        "test-token",
		BatchSize:    2, // Small batch size to test multiple batch flushes
	}
	err := conn.Init()
	assert.NoError(t, err)

	// Create test columns
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
		{Name: "amount", Type: iop.DecimalType, DbPrecision: 18, DbScale: 2},
		{Name: "created_at", Type: iop.DatetimeType},
	}

	// Create test dataset
	now := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	data := iop.Dataset{
		Columns: cols,
		Rows: [][]any{
			{int64(1), "Alice", "123.45", now},
			{int64(2), "Bob", "67.89", now.Add(time.Hour)},
			{int64(3), "Charlie", "999.00", now.Add(2 * time.Hour)},
			{int64(4), nil, nil, nil}, // Test null handling
			{int64(5), "Eve", "0.01", now.Add(3 * time.Hour)},
		},
	}

	ds := data.Stream()
	df := iop.MakeDataFlow(ds)

	count, err := conn.BulkImportFlow("main.default.users", df)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), count)
}

func TestZerobus_IPCSerialization(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	schema := iop.ColumnsToArrowSchema(cols)

	// Test 1: Serialize schema to IPC (without data batches)
	schemaBytes, err := SerializeSchemaToIPC(schema)
	assert.NoError(t, err)
	assert.NotEmpty(t, schemaBytes)

	// Test 2: Serialize record batch with ZSTD compression
	now := time.Now()
	data := iop.Dataset{
		Columns: cols,
		Rows: [][]any{
			{int64(1), "Alice"},
			{int64(2), "Bob"},
		},
	}
	_ = now
	df := iop.MakeDataFlow(data.Stream())
	defer df.CleanUp()

	conn := &ZerobusConn{
		WorkspaceURL: "adb-123.databricks.com",
		Token:        "tok",
		Compression:  "zstd",
	}
	err = conn.Init()
	assert.NoError(t, err)

	count, err := conn.BulkImportFlow("main.default.test", df)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), count)
}

