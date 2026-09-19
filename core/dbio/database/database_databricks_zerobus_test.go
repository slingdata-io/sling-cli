package database

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/zerobus-sdk/go"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeZerobusStream struct {
	batches      [][]byte
	ingestErr    error
	flushErr     error
	closeErr     error
	ingestCalled int
	flushCalled  int
	closeCalled  int
}

func (f *fakeZerobusStream) IngestBatch(ipcBytes []byte) (int64, error) {
	f.ingestCalled++
	if f.ingestErr != nil {
		return -1, f.ingestErr
	}
	f.batches = append(f.batches, ipcBytes)
	return int64(f.ingestCalled), nil
}

func (f *fakeZerobusStream) Flush() error {
	f.flushCalled++
	return f.flushErr
}

func (f *fakeZerobusStream) Close() error {
	f.closeCalled++
	return f.closeErr
}

func (f *fakeZerobusStream) GetUnackedBatches() ([][]byte, error) {
	return f.batches, nil
}

func newTestDatabricksConn() *DatabricksConn {
	conn := &DatabricksConn{}
	conn.setContext(context.Background(), 1)
	return conn
}

func TestMapZerobusIPCCompression(t *testing.T) {
	none, err := mapZerobusIPCCompression("none")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionNone, none)
	assert.NotEqual(t, zerobus.IPCCompressionDefault, none)

	empty, err := mapZerobusIPCCompression("")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionNone, empty)

	lz4, err := mapZerobusIPCCompression("lz4")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionLZ4Frame, lz4)

	zstd, err := mapZerobusIPCCompression("zstd")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionZstd, zstd)

	_, err = mapZerobusIPCCompression("gzip")
	assert.Error(t, err)
}

func TestIsZerobusSchemaLag(t *testing.T) {
	assert.False(t, isZerobusSchemaLag(nil))
	assert.False(t, isZerobusSchemaLag(fmt.Errorf("invalid_client")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("Schema comparison failed: Client field 'json_data' does not exist in Delta schema")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("SCHEMA_VALIDATION_FAILED")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("FIELD_NOT_IN_TABLE")))
}

func TestZerobusValidateConfig(t *testing.T) {
	conn := newTestDatabricksConn()
	conn.CopyMethod = "zerobus"
	err := conn.validateZerobusConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus_endpoint")

	conn.ZerobusEndpoint = "https://123.zerobus.us-west-2.cloud.databricks.com"
	err = conn.validateZerobusConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client_id")

	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	assert.NoError(t, conn.validateZerobusConfig())
}

func TestColumnsToZerobusArrowSchema(t *testing.T) {
	cols := iop.Columns{
		{Name: "col_bool", Type: iop.BoolType},
		{Name: "col_tiny", Type: iop.SmallIntType, DbType: "tinyint"},
		{Name: "col_short", Type: iop.SmallIntType, DbType: "smallint"},
		{Name: "col_int", Type: iop.IntegerType},
		{Name: "col_int_tiny", Type: iop.IntegerType, DbType: "tinyint"},
		{Name: "col_bigint", Type: iop.BigIntType},
		{Name: "col_float", Type: iop.FloatType, DbType: "float"},
		{Name: "col_double", Type: iop.FloatType, DbType: "double"},
		{Name: "col_str", Type: iop.StringType},
		{Name: "col_dec", Type: iop.DecimalType, DbPrecision: 18, DbScale: 4},
		{Name: "col_bin", Type: iop.BinaryType},
		{Name: "col_date", Type: iop.DateType},
		{Name: "col_tsz", Type: iop.TimestampzType},
		{Name: "col_ntz", Type: iop.TimestampType, DbType: "timestamp_ntz"},
	}

	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)

	assert.Equal(t, "bool", schema.Field(0).Type.Name())
	assert.Equal(t, "int8", schema.Field(1).Type.Name())
	assert.Equal(t, "int16", schema.Field(2).Type.Name())
	assert.Equal(t, "int32", schema.Field(3).Type.Name())
	assert.Equal(t, "int8", schema.Field(4).Type.Name())
	assert.Equal(t, "int64", schema.Field(5).Type.Name())
	assert.Equal(t, "float32", schema.Field(6).Type.Name())
	assert.Equal(t, "float64", schema.Field(7).Type.Name())
	assert.Equal(t, "large_utf8", schema.Field(8).Type.Name())
	assert.Equal(t, "decimal(18, 4)", schema.Field(9).Type.String())
	assert.Equal(t, "large_binary", schema.Field(10).Type.Name())
	assert.Equal(t, "date32", schema.Field(11).Type.Name())
	assert.Equal(t, "timestamp[us, tz=UTC]", schema.Field(12).Type.String())
	assert.Equal(t, "timestamp[us]", schema.Field(13).Type.String())
}

func TestColumnsToZerobusArrowSchema_Unsupported(t *testing.T) {
	_, err := ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "arr", Type: iop.JsonType, DbType: "array<string>"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported Zerobus type")

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "m", Type: iop.JsonType, DbType: "map<string,int>"},
	})
	assert.Error(t, err)

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "s", Type: iop.JsonType, DbType: "struct<a:int>"},
	})
	assert.Error(t, err)

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "v", Type: iop.JsonType, DbType: "variant"},
	})
	assert.Error(t, err)
}

func TestColumnsToZerobusArrowSchema_Nullability(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType, Metadata: map[string]string{"is_nullable": "false"}},
		{Name: "name", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "true"}},
	}
	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)
	assert.False(t, schema.Field(0).Nullable)
	assert.True(t, schema.Field(1).Nullable)
}

func TestSerializeRecordToIPC_RoundTrip(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)

	schemaBytes, err := SerializeSchemaToIPC(schema)
	require.NoError(t, err)
	assert.NotEmpty(t, schemaBytes)

	mem := memory.NewGoAllocator()
	idB := array.NewInt64Builder(mem)
	nameB := array.NewLargeStringBuilder(mem)
	idB.AppendValues([]int64{1, 2}, nil)
	nameB.AppendValues([]string{"Alice", "Bob"}, nil)
	idA := idB.NewArray()
	nameA := nameB.NewArray()
	defer idA.Release()
	defer nameA.Release()
	rec := array.NewRecord(schema, []arrow.Array{idA, nameA}, 2)
	defer rec.Release()

	for _, compression := range []string{"none", "lz4", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			b, err := SerializeRecordToIPC(schema, rec, compression)
			require.NoError(t, err)
			assert.NotEmpty(t, b)

			r, err := ipc.NewReader(bytes.NewReader(b))
			require.NoError(t, err)
			defer r.Release()
			assert.True(t, r.Next())
			got := r.Record()
			assert.Equal(t, int64(2), got.NumRows())
			assert.False(t, r.Next())
			assert.NoError(t, r.Err())
		})
	}
}

func TestAlignZerobusSource(t *testing.T) {
	src := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	tgt := iop.Columns{
		{Name: "id", Type: iop.BigIntType, Metadata: map[string]string{"nullable": "false"}},
		{Name: "name", Type: iop.StringType},
		{Name: "note", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "true"}},
	}
	idx, err := alignZerobusSource(src, tgt)
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, -1}, idx)

	_, err = alignZerobusSource(src, iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "missing", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "false"}},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing non-null")

	_, err = alignZerobusSource(iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "extra", Type: iop.StringType},
	}, iop.Columns{{Name: "id", Type: iop.BigIntType}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extra column")
}

func TestCopyViaZerobus_FakeStream(t *testing.T) {
	origOpen := openZerobusStream
	origDescribe := zerobusDescribe
	t.Cleanup(func() {
		openZerobusStream = origOpen
		zerobusDescribe = origDescribe
	})

	fake := &fakeZerobusStream{}
	openZerobusStream = func(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
		assert.Equal(t, "https://123.zerobus.us-west-2.cloud.databricks.com", endpoint)
		assert.Equal(t, "https://dbc.cloud.databricks.com", workspaceURL)
		assert.Equal(t, "main.default.users", tableName)
		assert.NotEmpty(t, schemaIPC)
		assert.Equal(t, zerobus.IPCCompressionNone, opts.IPCCompression)
		return fake, func() {}, nil
	}
	zerobusDescribe = func(conn *DatabricksConn, tableFName string) (iop.Columns, error) {
		return iop.Columns{
			{Name: "id", Type: iop.BigIntType},
			{Name: "name", Type: iop.StringType},
		}, nil
	}

	conn := newTestDatabricksConn()
	conn.CopyMethod = "zerobus"
	conn.ZerobusEndpoint = "123.zerobus.us-west-2.cloud.databricks.com"
	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	conn.BatchSize = 2
	conn.IPCCompression = "none"
	conn.MaxInflightBatches = 1000
	conn.Catalog = "main"
	conn.Schema = "default"
	conn.SetProp("host", "dbc.cloud.databricks.com")

	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	data := iop.NewDataset(cols)
	data.Rows = [][]any{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
		{int64(4), nil},
		{int64(5), "Eve"},
	}
	df, err := iop.MakeDataFlow(data.Stream())
	require.NoError(t, err)

	table := Table{Database: "main", Schema: "default", Name: "users"}
	count, err := conn.CopyViaZerobus(table, df)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)
	assert.Equal(t, 3, fake.ingestCalled) // batch_size 2 → 2+2+1
	assert.Equal(t, 1, fake.flushCalled)
	assert.Equal(t, 1, fake.closeCalled)
}

func TestCopyViaZerobus_FlushError(t *testing.T) {
	origOpen := openZerobusStream
	origDescribe := zerobusDescribe
	t.Cleanup(func() {
		openZerobusStream = origOpen
		zerobusDescribe = origDescribe
	})

	fake := &fakeZerobusStream{flushErr: assert.AnError}
	openZerobusStream = func(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
		return fake, func() {}, nil
	}
	zerobusDescribe = func(conn *DatabricksConn, tableFName string) (iop.Columns, error) {
		return iop.Columns{
			{Name: "id", Type: iop.BigIntType},
		}, nil
	}

	conn := newTestDatabricksConn()
	conn.ZerobusEndpoint = "https://z.example"
	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	conn.BatchSize = 10
	conn.IPCCompression = "none"
	conn.SetProp("host", "dbc.cloud.databricks.com")

	data := iop.NewDataset(iop.Columns{{Name: "id", Type: iop.BigIntType}})
	data.Rows = [][]any{{int64(1)}}
	df, err := iop.MakeDataFlow(data.Stream())
	require.NoError(t, err)
	_, err = conn.CopyViaZerobus(Table{Name: "t", Schema: "s", Database: "c"}, df)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus flush failed")
	assert.Equal(t, 1, fake.closeCalled)
}

func TestCopyViaZerobus_MissingEndpoint(t *testing.T) {
	conn := newTestDatabricksConn()
	_, err := conn.CopyViaZerobus(Table{Name: "t"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus_endpoint")
}
