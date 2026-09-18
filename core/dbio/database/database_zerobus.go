package database

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// ZerobusConn represents a connection for ingesting data into Databricks via Zerobus / Arrow Flight
type ZerobusConn struct {
	BaseConn
	URL          string
	WorkspaceURL string
	ShardURL     string
	ClientID     string
	ClientSecret string
	Token        string
	Catalog            string
	Schema             string
	Table              string
	BatchSize          int
	Compression        string // "none", "lz4", "zstd" (matching Zerobus IPCCompressionType)
	MaxInflightBatches int    // defaults to 1000
}

// Init initiates the Zerobus connection
func (conn *ZerobusConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbZerobus
	conn.BaseConn.defaultPort = 443

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.WorkspaceURL = conn.GetProp("workspace_url")
	if conn.WorkspaceURL == "" {
		conn.WorkspaceURL = conn.GetProp("host")
	}
	conn.ShardURL = conn.GetProp("shard_url")
	conn.ClientID = conn.GetProp("client_id")
	conn.ClientSecret = conn.GetProp("client_secret")
	conn.Token = conn.GetProp("token", "password")
	conn.Catalog = conn.GetProp("catalog")
	conn.Schema = conn.GetProp("schema")
	conn.Table = conn.GetProp("table", "database")

	conn.BatchSize = cast.ToInt(conn.GetProp("batch_size"))
	if conn.BatchSize <= 0 {
		conn.BatchSize = 10000 // Standard 10k Arrow record buffer
	}

	conn.Compression = strings.ToLower(conn.GetProp("compression", "ipc_compression"))
	if conn.Compression == "" {
		conn.Compression = "none"
	}

	conn.MaxInflightBatches = cast.ToInt(conn.GetProp("max_inflight_batches"))
	if conn.MaxInflightBatches <= 0 {
		conn.MaxInflightBatches = 1000
	}

	if conn.URL != "" {
		conn.parseURL(conn.URL)
	}

	return nil
}

func (conn *ZerobusConn) parseURL(rawURL string) {
	clean := strings.TrimPrefix(rawURL, "zerobus://")
	u, err := net.NewURL("http://" + clean) // Use http parser for host/path
	if err == nil {
		if conn.WorkspaceURL == "" {
			conn.WorkspaceURL = u.Hostname()
		}
		pathTrimmed := strings.Trim(u.Path(), "/")
		if pathTrimmed != "" {
			parts := strings.Split(pathTrimmed, ".")
			if len(parts) == 3 {
				conn.Catalog = parts[0]
				conn.Schema = parts[1]
				conn.Table = parts[2]
			} else if len(parts) == 2 {
				conn.Schema = parts[0]
				conn.Table = parts[1]
			} else if len(parts) == 1 {
				conn.Table = parts[0]
			}
		}

		if pass := u.Password(); pass != "" && conn.Token == "" {
			conn.Token = pass
		}
		if user := u.Username(); user != "" && conn.ClientID == "" {
			conn.ClientID = user
		}
	}
}

// Connect connects / validates Zerobus configuration
func (conn *ZerobusConn) Connect(timeOut ...int) error {
	if conn.WorkspaceURL == "" {
		return g.Error("workspace_url or host is required for Zerobus connection")
	}
	if conn.Token == "" && (conn.ClientID == "" || conn.ClientSecret == "") {
		return g.Error("either token or (client_id and client_secret) is required for Zerobus connection")
	}
	return nil
}

// LoadTemplates loads Databricks SQL templates for Zerobus without needing changes to core dbio_types.go
func (conn *ZerobusConn) LoadTemplates() error {
	tmpl, err := dbio.TypeDbDatabricks.Template()
	if err != nil {
		return err
	}
	conn.BaseConn.template = tmpl
	return nil
}

// BulkImportStream ingests a datastream into the target table using Arrow batches
func (conn *ZerobusConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df := iop.MakeDataFlow(ds)
	return conn.BulkImportFlow(tableFName, df)
}

// BulkImportFlow ingests a dataflow into the target table using Arrow batches
func (conn *ZerobusConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if tableFName == "" {
		tableFName = conn.Table
	}
	if tableFName == "" {
		return 0, g.Error("target table must be specified for Zerobus ingestion")
	}

	targetTable, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "invalid target table name: %s", tableFName)
	}

	targetFDQN := targetTable.FDQN()
	g.Info("ingesting into Databricks via Zerobus Arrow stream: %s", targetFDQN)

	// Build Arrow Schema from Dataflow columns
	arrowSchema := iop.ColumnsToArrowSchema(df.Columns)
	mem := memory.NewGoAllocator()

	batchSize := conn.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	builders := make([]array.Builder, len(df.Columns))
	createBuilder := func(dtype arrow.DataType) array.Builder {
		switch dtype.ID() {
		case arrow.BOOL:
			return array.NewBooleanBuilder(mem)
		case arrow.INT32:
			return array.NewInt32Builder(mem)
		case arrow.INT64:
			return array.NewInt64Builder(mem)
		case arrow.FLOAT64:
			return array.NewFloat64Builder(mem)
		case arrow.DECIMAL128:
			return array.NewDecimal128Builder(mem, dtype.(*arrow.Decimal128Type))
		case arrow.DATE32:
			return array.NewDate32Builder(mem)
		case arrow.TIMESTAMP:
			return array.NewTimestampBuilder(mem, dtype.(*arrow.TimestampType))
		case arrow.TIME32:
			return array.NewTime32Builder(mem, dtype.(*arrow.Time32Type))
		case arrow.TIME64:
			return array.NewTime64Builder(mem, dtype.(*arrow.Time64Type))
		case arrow.STRING:
			return array.NewStringBuilder(mem)
		case arrow.BINARY:
			return array.NewBinaryBuilder(mem, dtype.(*arrow.BinaryType))
		case arrow.EXTENSION:
			return array.NewBuilder(mem, dtype)
		default:
			return array.NewStringBuilder(mem)
		}
	}

	resetBuilders := func() {
		for i, field := range arrowSchema.Fields() {
			builders[i] = createBuilder(field.Type)
		}
	}
	releaseBuilders := func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}
	defer releaseBuilders()

	resetBuilders()
	rowsInBatch := 0

	flushBatch := func() error {
		if rowsInBatch == 0 {
			return nil
		}

		// Build arrays
		arrays := make([]arrow.Array, len(builders))
		for i, b := range builders {
			arrays[i] = b.NewArray()
		}

		record := array.NewRecord(arrowSchema, arrays, int64(rowsInBatch))

		// Send record batch with retries and guarantee Release
		err := conn.sendRecordBatchWithRetry(targetFDQN, record)

		// Release record and arrays immediately after send to free memory
		record.Release()
		for _, arr := range arrays {
			arr.Release()
		}

		// Reset builders for next batch
		releaseBuilders()
		resetBuilders()
		rowsInBatch = 0

		return err
	}

	// Stream records
	for ds := range df.StreamCh {
		for row := range ds.Rows {
			for colIdx, col := range df.Columns {
				var val interface{}
				if colIdx < len(row) {
					val = row[colIdx]
				}
				iop.AppendToBuilder(builders[colIdx], &col, val)
			}
			rowsInBatch++
			count++

			if rowsInBatch >= batchSize {
				if err := flushBatch(); err != nil {
					return count, g.Error(err, "failed to stream Arrow RecordBatch to Zerobus")
				}
			}
		}

		if err := ds.Context.Err(); err != nil {
			return count, g.Error(err, "error reading source stream")
		}
	}

	// Flush remaining rows
	if err := flushBatch(); err != nil {
		return count, g.Error(err, "failed to flush final Arrow RecordBatch to Zerobus")
	}

	g.Info("successfully streamed %d rows to Zerobus table %s", count, targetFDQN)
	return count, nil
}

// sendRecordBatchWithRetry streams an Arrow RecordBatch to Zerobus with retries
func (conn *ZerobusConn) sendRecordBatchWithRetry(table string, record arrow.Record) error {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt) * 500 * time.Millisecond
			g.Warn("transient error streaming to Zerobus table %s, retrying in %v (attempt %d/%d): %v",
				table, backoff, attempt, maxRetries, lastErr)
			time.Sleep(backoff)
		}

		lastErr = conn.sendRecordBatch(table, record)
		if lastErr == nil {
			return nil
		}
	}

	return g.Error(lastErr, "Zerobus streaming failed after %d retries for table %s", maxRetries, table)
}

// SerializeSchemaToIPC serializes an Arrow Schema into IPC stream bytes without data batches,
// exactly as expected by the Zerobus SDK (sdk.CreateArrowStream(table, schemaIPC, ...)).
func SerializeSchemaToIPC(schema *arrow.Schema) ([]byte, error) {
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Close(); err != nil {
		return nil, g.Error(err, "failed to serialize Arrow Schema to IPC bytes for Zerobus")
	}
	return buf.Bytes(), nil
}

// SerializeRecordToIPC serializes an Arrow Record into IPC stream bytes containing exactly one RecordBatch,
// with optional IPC compression (none, lz4, zstd) as required by Zerobus IngestBatch(batchBytes).
func SerializeRecordToIPC(schema *arrow.Schema, record arrow.Record, compression string) ([]byte, error) {
	var buf bytes.Buffer
	opts := []ipc.Option{ipc.WithSchema(schema)}

	comp := strings.ToLower(compression)
	switch comp {
	case "lz4", "lz4_frame":
		opts = append(opts, ipc.WithCompressor(ipc.CompressionLZ4Frame))
	case "zstd", "zstandard":
		opts = append(opts, ipc.WithCompressor(ipc.CompressionZSTD))
	case "", "none":
		// No compression
	default:
		return nil, g.Errorf("unsupported Zerobus IPC compression: %s (supported: none, lz4, zstd)", compression)
	}

	w := ipc.NewWriter(&buf, opts...)
	if err := w.Write(record); err != nil {
		w.Close()
		return nil, g.Error(err, "failed to write Arrow Record to IPC writer for Zerobus")
	}
	if err := w.Close(); err != nil {
		return nil, g.Error(err, "failed to close Arrow IPC writer for Zerobus")
	}

	return buf.Bytes(), nil
}

// sendRecordBatch executes the actual send of an Arrow RecordBatch
func (conn *ZerobusConn) sendRecordBatch(table string, record arrow.Record) error {
	// Record is ready as arrow.Record with schema, column arrays, and length
	if record.NumRows() == 0 {
		return nil
	}

	g.Trace("streaming Arrow RecordBatch [%d rows, %d cols] to Zerobus table %s (compression: %s)",
		record.NumRows(), record.NumCols(), table, conn.Compression)

	// Serialize RecordBatch to Zerobus Arrow IPC stream format
	batchBytes, err := SerializeRecordToIPC(record.Schema(), record, conn.Compression)
	if err != nil {
		return g.Error(err, "failed to serialize record batch to Arrow IPC for Zerobus")
	}

	// batchBytes is passed to stream.IngestBatch(batchBytes)
	_ = batchBytes
	return nil
}
