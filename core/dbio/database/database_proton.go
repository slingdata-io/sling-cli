package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fatih/color"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/timeplus-io/proton-go-driver/v2"
	_ "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/flarco/g"
)

const (
	maxRetries        = 4
	retryDelay        = 5 * time.Second
	countWaitDuration = 5 * time.Second
)

var protonDataTypeMap = map[string]string{
	"int8":    "int8",
	"int16":   "int16",
	"int32":   "int32",
	"int64":   "int64",
	"uint8":   "uint8",
	"uint16":  "uint16",
	"uint32":  "uint32",
	"uint64":  "uint64",
	"float32": "float32",
	"float64": "float64",
	"string":  "string",
	"bool":    "bool",

	// Map types
	"map(string, string)":        "map(string, string)",
	"map(string, int32)":         "map(string, int32)",
	"map(string, int64)":         "map(string, int64)",
	"map(string, float64)":       "map(string, float64)",
	"map(string, array(string))": "map(string, array(string))",
	"map(int32, string)":         "map(int32, string)",
	"map(int64, string)":         "map(int64, string)",

	// Array types
	"array(int8)":    "array(int8)",
	"array(int16)":   "array(int16)",
	"array(int32)":   "array(int32)",
	"array(int64)":   "array(int64)",
	"array(uint8)":   "array(uint8)",
	"array(uint16)":  "array(uint16)",
	"array(uint32)":  "array(uint32)",
	"array(uint64)":  "array(uint64)",
	"array(float32)": "array(float32)",
	"array(float64)": "array(float64)",
	"array(string)":  "array(string)",
	"array(bool)":    "array(bool)",
	"array(boolean)": "array(boolean)",
}

// ProtonConn is a Proton connection
type ProtonConn struct {
	BaseConn
	URL              string
	Idempotent       bool
	IdempotentPrefix string
	ProtonConn       proton.Conn
	retryBackoff     *backoff.ExponentialBackOff
}

// Init initiates the object
func (conn *ProtonConn) Init() error {
	u, err := url.Parse(conn.URL)
	if err != nil {
		return g.Error(err, "could not parse Proton URL")
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "8463" // default Proton port
	}

	username := u.User.Username()
	password, _ := u.User.Password() // Password might be empty
	database := strings.TrimPrefix(u.Path, "/")

	conn.ProtonConn, err = proton.Open(&proton.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: proton.Auth{
			Database: database,
			Username: username,
			Password: password, // This might be an empty string
		},
		DialTimeout: 5 * time.Second,
		Compression: &proton.Compression{
			Method: proton.CompressionLZ4,
		},
	})
	if err != nil {
		return g.Error(err, "could not connect to proton")
	}

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbProton

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.retryBackoff = backoff.NewExponentialBackOff()
	conn.retryBackoff.MaxElapsedTime = 5 * time.Minute
	conn.retryBackoff.InitialInterval = 1 * time.Second
	return conn.BaseConn.Init()
}

func (conn *ProtonConn) Connect(timeOut ...int) (err error) {

	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected packet") {
			g.Info(color.MagentaString("Try using the `http_url` instead to connect to Proton via HTTP. See https://docs.slingdata.io/connections/database-connections/Proton"))
		}
	}

	return err
}

func (conn *ProtonConn) ConnString() string {

	if url := conn.GetProp("http_url"); url != "" {
		return url
	}

	return conn.BaseConn.ConnString()
}

// SetIdempotent enables or disables idempotent support
func (conn *ProtonConn) SetIdempotent(enabled bool, prefix string) {
	conn.Idempotent = enabled
	conn.IdempotentPrefix = prefix
}

// NewTransaction creates a new transaction
func (conn *ProtonConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {

	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	Tx := &BaseTransaction{Tx: tx, Conn: conn.Self(), context: &context}
	conn.tx = Tx

	// ProtonDB does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}

	return Tx, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *ProtonConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// allow custom SQL expression for partitioning
		partitionBy = g.F("partition by (%s)", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by %s", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{partition_by}", partitionBy)

	return strings.TrimSpace(sql), nil
}

// Define a helper function for retrying operations
func retryWithBackoff(operation func() error) error {
	b := backoff.NewExponentialBackOff()
	b.MaxElapsedTime = 5 * time.Minute // Set a maximum total retry time
	b.InitialInterval = 1 * time.Second

	return backoff.RetryNotify(operation, b, func(err error, duration time.Duration) {
		g.Warn("Operation failed, retrying in %v: %v", duration, err)
	})
}

// colClassification holds pre-computed column index lists for type conversion.
// Built once per column-change event and reused across all batches.
type colClassification struct {
	decimalCols              []int
	intCols                  []int
	int8Cols                 []int
	int16Cols                []int
	int32Cols                []int
	int64Cols                []int
	uint8Cols                []int
	uint16Cols               []int
	uint32Cols               []int
	uint64Cols               []int
	float32Cols              []int
	float64Cols              []int
	floatCols                []int
	stringCols               []int
	booleanCols              []int
	arrayStringCols          []int
	arrayBooleanCols         []int
	arrayInt8Cols            []int
	arrayInt16Cols           []int
	arrayInt32Cols           []int
	arrayInt64Cols           []int
	arrayUint8Cols           []int
	arrayUint16Cols          []int
	arrayUint32Cols          []int
	arrayUint64Cols          []int
	arrayFloat32Cols         []int
	arrayFloat64Cols         []int
	mapStringStringCols      []int
	mapStringInt32Cols       []int
	mapStringInt64Cols       []int
	mapStringUint32Cols      []int
	mapStringUint64Cols      []int
	mapStringFloat64Cols     []int
	mapStringFloat32Cols     []int
	mapStringArrayStringCols []int
	mapInt32StringCols       []int
	mapInt64StringCols       []int
}

// classifyColumns builds a colClassification from the target columns.
func classifyColumns(insFields iop.Columns) colClassification {
	var cc colClassification
	for i, col := range insFields {
		dbType := strings.ToLower(col.DbType)
		if strings.HasPrefix(dbType, "nullable(") {
			dbType = strings.TrimPrefix(dbType, "nullable(")
			dbType = strings.TrimSuffix(dbType, ")")
		}
		if strings.HasPrefix(dbType, "low_cardinality(") {
			dbType = strings.TrimPrefix(dbType, "low_cardinality(")
			dbType = strings.TrimSuffix(dbType, ")")
		}

		switch dbType {
		case "int8":
			cc.int8Cols = append(cc.int8Cols, i)
		case "int16":
			cc.int16Cols = append(cc.int16Cols, i)
		case "int32":
			cc.int32Cols = append(cc.int32Cols, i)
		case "int64":
			cc.int64Cols = append(cc.int64Cols, i)
		case "uint8":
			cc.uint8Cols = append(cc.uint8Cols, i)
		case "uint16":
			cc.uint16Cols = append(cc.uint16Cols, i)
		case "uint32":
			cc.uint32Cols = append(cc.uint32Cols, i)
		case "uint64":
			cc.uint64Cols = append(cc.uint64Cols, i)
		case "float32":
			cc.float32Cols = append(cc.float32Cols, i)
		case "float64":
			cc.float64Cols = append(cc.float64Cols, i)
		case "string":
			cc.stringCols = append(cc.stringCols, i)
		case "bool":
			cc.booleanCols = append(cc.booleanCols, i)
		case "array(string)":
			cc.arrayStringCols = append(cc.arrayStringCols, i)
		case "array(bool)", "array(boolean)":
			cc.arrayBooleanCols = append(cc.arrayBooleanCols, i)
		case "array(int64)":
			cc.arrayInt64Cols = append(cc.arrayInt64Cols, i)
		case "array(int32)":
			cc.arrayInt32Cols = append(cc.arrayInt32Cols, i)
		case "array(int16)":
			cc.arrayInt16Cols = append(cc.arrayInt16Cols, i)
		case "array(int8)":
			cc.arrayInt8Cols = append(cc.arrayInt8Cols, i)
		case "array(uint64)":
			cc.arrayUint64Cols = append(cc.arrayUint64Cols, i)
		case "array(uint32)":
			cc.arrayUint32Cols = append(cc.arrayUint32Cols, i)
		case "array(uint16)":
			cc.arrayUint16Cols = append(cc.arrayUint16Cols, i)
		case "array(uint8)":
			cc.arrayUint8Cols = append(cc.arrayUint8Cols, i)
		case "array(float32)":
			cc.arrayFloat32Cols = append(cc.arrayFloat32Cols, i)
		case "array(float64)":
			cc.arrayFloat64Cols = append(cc.arrayFloat64Cols, i)
		case "map(string, string)":
			cc.mapStringStringCols = append(cc.mapStringStringCols, i)
		case "map(string, int32)":
			cc.mapStringInt32Cols = append(cc.mapStringInt32Cols, i)
		case "map(string, int64)":
			cc.mapStringInt64Cols = append(cc.mapStringInt64Cols, i)
		case "map(string, uint32)":
			cc.mapStringUint32Cols = append(cc.mapStringUint32Cols, i)
		case "map(string, uint64)":
			cc.mapStringUint64Cols = append(cc.mapStringUint64Cols, i)
		case "map(string, float64)":
			cc.mapStringFloat64Cols = append(cc.mapStringFloat64Cols, i)
		case "map(string, float32)":
			cc.mapStringFloat32Cols = append(cc.mapStringFloat32Cols, i)
		case "map(string, array(string))":
			cc.mapStringArrayStringCols = append(cc.mapStringArrayStringCols, i)
		case "map(int32, string)":
			cc.mapInt32StringCols = append(cc.mapInt32StringCols, i)
		case "map(int64, string)":
			cc.mapInt64StringCols = append(cc.mapInt64StringCols, i)
		default:
			switch {
			case col.Type == iop.DecimalType:
				cc.decimalCols = append(cc.decimalCols, i)
			case col.Type == iop.SmallIntType:
				cc.intCols = append(cc.intCols, i)
			case col.Type.IsInteger():
				cc.int64Cols = append(cc.int64Cols, i)
			case col.Type == iop.FloatType:
				cc.floatCols = append(cc.floatCols, i)
			}
		}
	}
	return cc
}

// BulkImportStream inserts a stream into a table
func (conn *ProtonConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns
	var insFields iop.Columns
	var cc colClassification

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// set default schema
	conn.Exec(g.F("use `%s`", table.Schema))

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(100 * time.Millisecond)

			ds.Context.Lock()
			defer ds.Context.Unlock()

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for Postgres")
			}

			return nil
		}
	}

	batchCount := 0
	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}

			// recompute column validation and classification only on schema change
			insFields, err = conn.ValidateColumnNames(columns, batch.Columns.Names(), true)
			if err != nil {
				return count, g.Error(err, "columns mismatch")
			}
			cc = classifyColumns(insFields)
		}

		batchCount++
		err = conn.processBatch(tableFName, table, batch, insFields, cc, batchCount, &count, ds)
		if err != nil {
			if permanentErr, ok := err.(*backoff.PermanentError); ok {
				return count, g.Error(permanentErr.Err, "permanent error processing batch %d", batchCount)
			}
			return count, g.Error(err, "failed to process batch %d after retries", batchCount)
		}
	}

	ds.SetEmpty()

	g.Debug("%d ROWS COPIED", count)
	return count, nil
}

// processBatch handles the processing of a single batch.
// Proton does not support transactions, so we skip Begin/Commit.
// The retry loop only contains: PrepareBatch → Append rows → Send.
func (conn *ProtonConn) processBatch(tableFName string, table Table, batch *iop.Batch, insFields iop.Columns, cc colClassification, batchCount int, count *uint64, ds *iop.Datastream) error {
	batchRows := make([][]any, 0, batch.Count)
	for row := range batch.Rows {
		batchRows = append(batchRows, row)
	}

	idPrefix := fmt.Sprintf("%d_%d_batch_%s",
		time.Now().UnixNano(),
		batchCount,
		table.FullName())
	conn.SetIdempotent(true, idPrefix)

	insertStatement := conn.GenerateInsertStatement(
		table.FullName(),
		insFields,
		1,
	)

	operation := func() error {
		// Check context cancellation
		select {
		case <-ds.Context.Ctx.Done():
			return backoff.Permanent(g.Error(ds.Context.Ctx.Err(), "context cancelled"))
		default:
		}

		batched, err := conn.ProtonConn.PrepareBatch(ds.Context.Ctx, insertStatement)
		if err != nil {
			return g.Error(err, "could not prepare statement for table: %s, statement: %s", table.FullName(), insertStatement)
		}

		// Counter for successfully inserts within this batch
		var internalCount uint64
		for _, row := range batchRows {
			var eG g.ErrorGroup

			for _, colI := range cc.decimalCols {
				if row[colI] != nil {
					val, err := decimal.NewFromString(cast.ToString(row[colI]))
					if err == nil {
						row[colI] = val
					}
					eG.Capture(err)
				}
			}

			for _, colI := range cc.booleanCols {
				if row[colI] != nil {
					row[colI], err = cast.ToBoolE(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.stringCols {
				if row[colI] != nil {
					row[colI], err = cast.ToStringE(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.int8Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToInt8E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.int16Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToInt16E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.int32Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToInt32E(row[colI])
					eG.Capture(err)
				}
			}

			// set Int32 correctly
			for _, colI := range cc.intCols {
				if row[colI] != nil {
					row[colI], err = cast.ToIntE(row[colI])
					eG.Capture(err)
				}
			}

			// set Int64 correctly
			for _, colI := range cc.int64Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToInt64E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.uint8Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToUint8E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.uint16Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToUint16E(row[colI])
					eG.Capture(err)
				}
			}

			// set Int32 correctly
			for _, colI := range cc.uint32Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToUint32E(row[colI])
					eG.Capture(err)
				}
			}

			// set Int64 correctly
			for _, colI := range cc.uint64Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToUint64E(row[colI])
					eG.Capture(err)
				}
			}

			// set Float64 correctly
			for _, colI := range cc.floatCols {
				if row[colI] != nil {
					row[colI], err = cast.ToFloat64E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.float32Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToFloat32E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.float64Cols {
				if row[colI] != nil {
					row[colI], err = cast.ToFloat64E(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayStringCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayString(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayBooleanCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayBool(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayInt8Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayInt8(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayInt16Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayInt16(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayInt32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayInt32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayInt64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayInt64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayUint8Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayUint8(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayUint16Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayUint16(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayUint32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayUint32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayUint64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayUint64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayFloat32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayFloat32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.arrayFloat64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToArrayFloat64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringStringCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringString(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringInt32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringInt32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringInt64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringInt64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringUint32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringUint32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringUint64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringUint64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringFloat64Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringFloat64(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringFloat32Cols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringFloat32(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapStringArrayStringCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapStringArrayString(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapInt32StringCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapInt32String(row[colI])
					eG.Capture(err)
				}
			}

			for _, colI := range cc.mapInt64StringCols {
				if row[colI] != nil {
					row[colI], err = conn.convertToMapInt64String(row[colI])
					eG.Capture(err)
				}
			}

			if err = eG.Err(); err != nil {
				err = g.Error(err, "could not convert value for COPY into table %s", tableFName)
				ds.Context.CaptureErr(err)
				return backoff.Permanent(err) // Type conversion errors are permanent
			}

			// Do insert (batched is local to this closure, no concurrent access)
			err = batched.Append(row...)
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "could not insert into table %s, row: %#v", tableFName, row))
				return g.Error(err, "could not execute statement") // Network/temporary errors can retry
			}
			internalCount++
		}

		err = batched.Send()
		if err != nil {
			return g.Error(err, "could not send batch data")
		}

		// Update count only after successful send
		*count += internalCount
		return nil
	}

	return backoff.RetryNotify(operation,
		conn.retryBackoff,
		func(err error, duration time.Duration) {
			g.Warn("Batch %d failed, retrying in %v: %v", batchCount, duration, err)
		})
}

// ExecContext runs a sql query with context, returns `error`
func (conn *ProtonConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	err = retryWithBackoff(func() error {
		var execErr error
		result, execErr = conn.BaseConn.ExecContext(ctx, q, args...)
		return execErr
	})

	if err != nil {
		g.Error(err, "Failed to execute query after retries")
	}

	return result, err
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *ProtonConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	fields := cols.Names()
	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, field, n, c)
			qFields[i] = conn.Self().Quote(field)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	if conn.GetProp("http_url") != "" {
		table, _ := ParseTableName(tableName, conn.GetType())
		tableName = table.NameQ()
	}

	settings := ""
	if conn.Idempotent {
		if len(conn.IdempotentPrefix) > 0 {
			settings = fmt.Sprintf(" settings idempotent_id='%s'", conn.IdempotentPrefix)
		} else {
			// Use a default prefix with timestamp if not provided
			defaultPrefix := time.Now().Format("20060102150405")
			settings = fmt.Sprintf(" settings idempotent_id='%s'", defaultPrefix)
		}
	}

	statement := g.R(
		"insert into {table} ({fields}) {settings} values {values}",
		"table", tableName,
		"settings", settings,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *ProtonConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// proton does not support upsert with delete
	sqlTempl := `
	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from table({src_table}) src
	`
	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
	)

	return
}

func processProtonInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		if columns[i].Type == iop.DecimalType {
			sVal := cast.ToString(row[i])
			if sVal != "" {
				val, err := decimal.NewFromString(sVal)
				if !g.LogError(err, "could not convert value `%s` for Timeplus decimal", sVal) {
					row[i] = val
				}
			} else {
				row[i] = nil
			}
		} else if columns[i].Type == iop.FloatType {
			row[i] = cast.ToFloat64(row[i])
		}
	}
	return row
}

// GetCount returns count of records
func (conn *ProtonConn) GetCount(tableFName string) (uint64, error) {
	// Add another sleep, reason: after insert table we try to getcount directly to ensure no record missing
	// but proton seems not fully ready to get count.
	time.Sleep(countWaitDuration)
	var count uint64

	// Retry logic to handle occasional zero count, likely due to database latency or transactional delays.
	// Temporary workaround while investigating the root cause.
	for attempt := 0; attempt < maxRetries; attempt++ {
		sql := fmt.Sprintf(`select count(*) as cnt from table(%s)`, tableFName)
		data, err := conn.Self().Query(sql)
		if err != nil {
			g.LogError(err, "could not get row number")
			return 0, err
		}

		count = cast.ToUint64(data.Rows[0][0])
		if count > 0 {
			return count, nil
		}

		if attempt < maxRetries-1 {
			g.Debug("Got zero count for %s, retrying in %v (attempt %d/%d)",
				tableFName, countWaitDuration, attempt+1, maxRetries)
			time.Sleep(countWaitDuration)
		}
	}

	// Return 0 after max retries if no valid count is obtained
	return count, nil
}

func (conn *ProtonConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	nativeType, err = conn.BaseConn.GetNativeType(col)

	// remove nullable if part of pk
	if col.IsKeyType(iop.PrimaryKey) && strings.HasPrefix(nativeType, "nullable(") {
		nativeType = strings.TrimPrefix(nativeType, "nullable(")
		nativeType = strings.TrimSuffix(nativeType, ")")
	}

	// special case for _tp_time, Column _tp_time is reserved, expected type is non-nullable datetime64
	if col.Name == "_tp_time" {
		return "datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4)", nil
	}

	// special case for _tp_sn, Column _tp_sn is reserved, expected type is non-nullable int64
	if col.Name == "_tp_sn" {
		return "int64 CODEC(Delta(8), ZSTD(1))", nil
	}

	if col.DbType != "" {
		dbType := col.DbType

		// Check if the type is nullable
		isNullable := strings.HasPrefix(col.DbType, "nullable(")
		if isNullable {
			// Extract the inner type
			dbType = strings.TrimPrefix(dbType, "nullable(")
			dbType = strings.TrimSuffix(dbType, ")")
		}

		if mappedType, ok := protonDataTypeMap[dbType]; ok {
			if isNullable {
				return "nullable(" + mappedType + ")", nil
			}
			return mappedType, nil
		}

		// Check if the type is low cardinality
		isLowCardinality := strings.HasPrefix(col.DbType, "low_cardinality(")
		if isLowCardinality {
			dbType = strings.TrimPrefix(dbType, "low_cardinality(")
			dbType = strings.TrimSuffix(dbType, ")")
		}

		if mappedType, ok := protonDataTypeMap[dbType]; ok {
			if isLowCardinality {
				return "low_cardinality(" + mappedType + ")", nil
			}
			return mappedType, nil
		}
	}

	return nativeType, err
}

// Array types
func (conn *ProtonConn) convertToArrayString(value interface{}) ([]string, error) {

	if value == "" {
		return []string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayInt8(value interface{}) ([]int8, error) {

	if value == "" {
		return []int8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int8
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayInt16(value interface{}) ([]int16, error) {

	if value == "" {
		return []int16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int16
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayInt32(value interface{}) ([]int32, error) {

	if value == "" {
		return []int32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayInt64(value interface{}) ([]int64, error) {

	if value == "" {
		return []int64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []int64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint8(value interface{}) ([]uint8, error) {

	if value == "" {
		return []uint8{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint8
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint16(value interface{}) ([]uint16, error) {

	if value == "" {
		return []uint16{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint16
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayUint32(value interface{}) ([]uint32, error) {

	if value == "" {
		return []uint32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayUint64(value interface{}) ([]uint64, error) {

	if value == "" {
		return []uint64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []uint64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayFloat32(value interface{}) ([]float32, error) {

	if value == "" {
		return []float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []float32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}
func (conn *ProtonConn) convertToArrayFloat64(value interface{}) ([]float64, error) {

	if value == "" {
		return []float64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []float64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToArrayBool(value interface{}) ([]bool, error) {

	if value == "" {
		return []bool{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result []bool
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal array: %v", err)
	}

	return result, nil
}

// Map types
func (conn *ProtonConn) convertToMapStringUint64(value interface{}) (map[string]uint64, error) {

	if value == "" {
		return map[string]uint64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]uint64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringUint32(value interface{}) (map[string]uint32, error) {

	if value == "" {
		return map[string]uint32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]uint32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringInt32(value interface{}) (map[string]int32, error) {

	if value == "" {
		return map[string]int32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]int32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringInt64(value interface{}) (map[string]int64, error) {

	if value == "" {
		return map[string]int64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]int64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringFloat64(value interface{}) (map[string]float64, error) {

	if value == "" {
		return map[string]float64{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]float64
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringFloat32(value interface{}) (map[string]float32, error) {

	if value == "" {
		return map[string]float32{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]float32
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapInt32String(value interface{}) (map[int32]string, error) {

	if value == "" {
		return map[int32]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[int32]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapInt64String(value interface{}) (map[int64]string, error) {

	if value == "" {
		return map[int64]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[int64]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringArrayString(value interface{}) (map[string][]string, error) {

	if value == "" {
		return map[string][]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string][]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}

func (conn *ProtonConn) convertToMapStringString(value interface{}) (map[string]string, error) {

	if value == "" {
		return map[string]string{}, nil
	}

	str, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("expected string, got %T", value)
	}

	var result map[string]string
	err := json.Unmarshal([]byte(str), &result)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal map: %v", err)
	}

	return result, nil
}
