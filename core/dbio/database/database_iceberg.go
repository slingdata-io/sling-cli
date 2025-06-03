package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsv2config "github.com/aws/aws-sdk-go-v2/config"
	awsv2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// IcebergConn is an Iceberg connection
type IcebergConn struct {
	BaseConn
	URL     string
	Catalog catalog.Catalog
}

// Init initiates the object
func (conn *IcebergConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbIceberg

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	for _, key := range g.ArrStr("BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION", "DEFAULT_REGION", "SESSION_TOKEN", "ENDPOINT", "ROLE_ARN", "ROLE_SESSION_NAME", "PROFILE") {
		if conn.GetProp(key) == "" {
			conn.SetProp(key, conn.GetProp("AWS_"+key))
		}
	}

	return conn.BaseConn.Init()
}

// Connect connects to the Iceberg catalog
func (conn *IcebergConn) Connect(timeOut ...int) (err error) {
	if cast.ToBool(conn.GetProp("connected")) {
		return nil
	}

	// Determine catalog type from URL or properties
	catalogType := conn.GetProp("catalog_type")
	if catalogType == "" {
		catalogType = "rest" // default to REST catalog
	}

	switch strings.ToLower(catalogType) {
	case "rest":
		err = conn.connectREST()
	case "glue":
		err = conn.connectGlue()
	default:
		return g.Error("Unsupported catalog type: %s. Supported types are: rest, glue", catalogType)
	}

	if err != nil {
		return g.Error(err, "Failed to connect to Iceberg catalog")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	return nil
}

func (conn *IcebergConn) connectREST() error {
	restURI := conn.GetProp("rest_uri")
	if restURI == "" {
		return g.Error("rest_uri property is required for REST catalog")
	}

	catalogName := conn.GetProp("catalog_name")
	if catalogName == "" {
		catalogName = "iceberg"
	}

	opts := []rest.Option{}

	// Add authentication if provided
	if token := conn.GetProp("token"); token != "" {
		opts = append(opts, rest.WithOAuthToken(token))
	}

	// Add warehouse location if provided
	if warehouse := conn.GetProp("warehouse_location"); warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(warehouse))
	}

	// Add credential if provided
	if cred := conn.GetProp("credential"); cred != "" {
		opts = append(opts, rest.WithCredential(cred))
	}

	cat, err := rest.NewCatalog(
		context.Background(),
		catalogName,
		restURI,
		opts...,
	)
	if err != nil {
		return g.Error(err, "Failed to create REST catalog")
	}

	conn.Catalog = cat
	return nil
}

func (conn *IcebergConn) connectGlue() error {
	// Get AWS credentials from connection properties
	awsAccessKeyID := conn.GetProp("access_key_id")
	awsSecretAccessKey := conn.GetProp("secret_access_key")
	awsSessionToken := conn.GetProp("session_token")
	awsRegion := conn.GetProp("region")
	awsProfile := conn.GetProp("profile")

	if awsRegion == "" {
		return g.Error("AWS region not specified")
	}

	var awsCfg awsv2.Config
	var err error

	// Set credentials if provided
	if awsAccessKeyID != "" && awsSecretAccessKey != "" {
		g.Debug("Iceberg: Using static credentials (Key ID: %s)", awsAccessKeyID)

		// Create AWS config with static credentials
		awsCfg, err = awsv2config.LoadDefaultConfig(context.Background(),
			awsv2config.WithRegion(awsRegion),
			awsv2config.WithCredentialsProvider(
				awsv2creds.NewStaticCredentialsProvider(
					awsAccessKeyID,
					awsSecretAccessKey,
					awsSessionToken,
				),
			),
		)
		if err != nil {
			return g.Error(err, "Failed to create AWS config with static credentials")
		}
	} else if awsProfile != "" {
		g.Debug("Iceberg: Using AWS profile=%s region=%s", awsProfile, awsRegion)

		// Use specified profile from AWS credentials file
		awsCfg, err = awsv2config.LoadDefaultConfig(context.Background(),
			awsv2config.WithRegion(awsRegion),
			awsv2config.WithSharedConfigProfile(awsProfile),
		)
		if err != nil {
			return g.Error(err, "Failed to create AWS config with profile %s", awsProfile)
		}
	} else {
		g.Debug("Iceberg: Using default AWS credential chain")
		// Use default credential chain (env vars, IAM role, credential file, etc.)
		awsCfg, err = awsv2config.LoadDefaultConfig(context.Background(),
			awsv2config.WithRegion(awsRegion),
		)
		if err != nil {
			return g.Error(err, "Failed to create AWS config with default credentials")
		}
	}

	// Create Glue catalog with AWS config
	cat := glue.NewCatalog(glue.WithAwsConfig(awsCfg))
	conn.Catalog = cat

	return nil
}

// Close closes the connection
func (conn *IcebergConn) Close() error {
	if conn.Catalog == nil {
		return nil
	}

	// Iceberg catalog doesn't have a close method
	conn.Catalog = nil
	return conn.BaseConn.Close()
}

// NewTransaction creates a new transaction
func (conn *IcebergConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// Iceberg operations are transactional by nature when using table transactions
	// But we don't have traditional SQL transactions
	return nil, nil
}

// GetSchemas returns the list of namespaces (schemas)
func (conn *IcebergConn) GetSchemas() (data iop.Dataset, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return data, g.Error(err, "Could not reconnect")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))

	namespaces, err := conn.Catalog.ListNamespaces(conn.Context().Ctx, nil)
	if err != nil {
		return data, g.Error(err, "Failed to list namespaces")
	}

	for _, ns := range namespaces {
		// Convert namespace to schema name (join with dot)
		schemaName := strings.Join(ns, ".")
		data.Rows = append(data.Rows, []any{schemaName})
	}

	return data, nil
}

// GetDatabases returns databases (catalogs) for Iceberg connection
func (conn *IcebergConn) GetDatabases() (data iop.Dataset, err error) {
	// In Iceberg, the catalog is the database equivalent
	// Return the current catalog name
	data = iop.NewDataset(iop.NewColumnsFromFields("name"))
	catalogName := conn.GetProp("catalog_name")
	if catalogName == "" {
		catalogName = "iceberg"
	}
	data.Rows = append(data.Rows, []any{catalogName})
	return data, nil
}

// GetTables returns tables for given schema
func (conn *IcebergConn) GetTables(schema string) (data iop.Dataset, err error) {
	return conn.getTablesOrViews(schema, false)
}

// GetViews returns views for given schema
func (conn *IcebergConn) GetViews(schema string) (data iop.Dataset, err error) {
	// Iceberg doesn't have views in the traditional sense
	data = iop.NewDataset(iop.NewColumnsFromFields("table_name", "is_view"))
	return data, nil
}

// GetTablesAndViews returns tables and views for given schema
func (conn *IcebergConn) GetTablesAndViews(schema string) (data iop.Dataset, err error) {
	return conn.getTablesOrViews(schema, true)
}

func (conn *IcebergConn) getTablesOrViews(schema string, includeViews bool) (data iop.Dataset, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return data, g.Error(err, "Could not reconnect")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name", "is_view"))

	// Parse namespace from schema string
	var namespace table.Identifier
	if schema == "" {
		namespace = nil // List all tables
	} else {
		namespace = strings.Split(schema, ".")
	}

	// List tables in namespace
	iter := conn.Catalog.ListTables(conn.Context().Ctx, namespace)
	for tblID, err := range iter {
		if err != nil {
			return data, g.Error(err, "Failed to list tables")
		}
		// Get table name (last part of identifier)
		tableName := tblID[len(tblID)-1]
		data.Rows = append(data.Rows, []any{tableName, false})
	}

	return data, nil
}

// GetColumns returns the columns for a given table
func (conn *IcebergConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return columns, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	tableID := parseTableIdentifier(tableFName)

	// Load table
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID, nil)
	if err != nil {
		return columns, g.Error(err, "Failed to load table %s", tableFName)
	}

	// Get table schema
	schema := tbl.Schema()

	// Convert Iceberg schema to iop.Columns
	columns = make(iop.Columns, 0, len(schema.Fields()))

	for i, field := range schema.Fields() {
		col := iop.Column{
			Name:     field.Name,
			Position: i + 1,
			Type:     icebergTypeToIopType(field.Type),
			DbType:   field.Type.String(),
			Sourced:  true,
		}

		// Check if field is required (not nullable)
		if field.Required {
			col.Constraint = &iop.ColumnConstraint{
				Expression: "not null",
			}
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// StreamRowsContext streams the rows of a table or query
func (conn *IcebergConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, g.Error(err, "Could not reconnect")
	}

	opts := getQueryOptions(options)
	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val
	}

	limit := uint64(0) // infinite
	if val := cast.ToUint64(opts["limit"]); val > 0 {
		limit = val
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)
	conn.LogSQL(sql)

	// For Iceberg, we only support simple table scans (SELECT * FROM table)
	// Parse the SQL to extract table name
	tableName, err := parseSimpleSelectSQL(sql)
	if err != nil {
		return nil, g.Error(err, "Iceberg only supports simple SELECT * FROM table queries")
	}

	// Parse table identifier
	tableID := parseTableIdentifier(tableName)

	// Load table
	tbl, err := conn.Catalog.LoadTable(queryContext.Ctx, tableID, nil)
	if err != nil {
		return nil, g.Error(err, "Failed to load table %s", tableName)
	}

	// Get table schema and convert to columns if not provided
	if len(fetchedColumns) == 0 {
		schema := tbl.Schema()
		fetchedColumns = make(iop.Columns, 0, len(schema.Fields()))

		for i, field := range schema.Fields() {
			col := iop.Column{
				Name:     field.Name,
				Position: i + 1,
				Type:     icebergTypeToIopType(field.Type),
				DbType:   field.Type.String(),
				Sourced:  true,
				Table:    tableName,
			}
			fetchedColumns = append(fetchedColumns, col)
		}
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.NoDebug = !strings.Contains(sql, noDebugKey)

	// Create table scan
	scanOpts := []table.ScanOption{}
	if limit > 0 {
		scanOpts = append(scanOpts, table.WithLimit(int64(limit)))
	}

	// Add selected fields if specified
	if fieldNames := opts["selected_fields"]; fieldNames != nil {
		if fields, ok := fieldNames.([]string); ok {
			scanOpts = append(scanOpts, table.WithSelectedFields(fields...))
		}
	}

	scan := tbl.Scan(scanOpts...)

	// Create a function to fetch next row
	makeNextFunc := func() (nextFunc func(it *iop.Iterator) bool) {
		// Get Arrow record iterator
		_, recordIterator, err := scan.ToArrowRecords(queryContext.Ctx)
		if err != nil {
			queryContext.CaptureErr(g.Error(err, "Failed to create arrow record iterator"))
			return func(it *iop.Iterator) bool { return false }
		}

		var currentRecord arrow.Record
		var currentRowIdx int
		var records []arrow.Record
		var recordIdx int

		// Collect all records from the iterator
		for record, err := range recordIterator {
			if err != nil {
				queryContext.CaptureErr(g.Error(err, "Error reading arrow records"))
				break
			}
			records = append(records, record)
		}

		if len(records) > 0 {
			currentRecord = records[0]
		}

		return func(it *iop.Iterator) bool {
			if limit > 0 && it.Counter >= limit {
				return false
			}

			// Check if we need to fetch next record batch
			if currentRecord == nil || currentRowIdx >= int(currentRecord.NumRows()) {
				recordIdx++
				if recordIdx < len(records) {
					currentRecord = records[recordIdx]
					currentRowIdx = 0
				} else {
					return false
				}
			}

			// Convert current row to interface{} slice
			it.Row = make([]interface{}, currentRecord.NumCols())
			for colIdx := 0; colIdx < int(currentRecord.NumCols()); colIdx++ {
				col := currentRecord.Column(colIdx)
				it.Row[colIdx] = getArrowValue(col, currentRowIdx)
			}

			currentRowIdx++
			return true
		}
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, fetchedColumns, makeNextFunc())
	ds.NoDebug = strings.Contains(sql, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()

	if !ds.NoDebug {
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return ds, nil
}

// ExecContext executes a write operation
func (conn *IcebergConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	// Iceberg doesn't support SQL execution directly
	// This would need to be implemented with table operations
	return nil, g.Error("Iceberg does not support direct SQL execution. Use bulk import/export operations instead")
}

// Helper functions

func parseTableIdentifier(tableName string) table.Identifier {
	// Split by dots to create namespace and table parts
	parts := strings.Split(tableName, ".")
	return parts
}

func parseSimpleSelectSQL(sql string) (tableName string, err error) {
	// Very simple SQL parser for "SELECT * FROM table" queries
	sql = strings.TrimSpace(sql)
	sql = strings.ToUpper(sql)

	if !strings.HasPrefix(sql, "SELECT") {
		return "", g.Error("Only SELECT queries are supported")
	}

	// Find FROM clause
	fromIdx := strings.Index(sql, "FROM")
	if fromIdx == -1 {
		return "", g.Error("No FROM clause found")
	}

	// Extract table name after FROM
	afterFrom := strings.TrimSpace(sql[fromIdx+4:])
	parts := strings.Fields(afterFrom)
	if len(parts) == 0 {
		return "", g.Error("No table name found after FROM")
	}

	return parts[0], nil
}

func icebergTypeToIopType(icebergType iceberg.Type) iop.ColumnType {
	switch icebergType.Type() {
	case "boolean":
		return iop.BoolType
	case "int32", "int":
		return iop.IntegerType
	case "int64", "long":
		return iop.BigIntType
	case "float32", "float":
		return iop.FloatType
	case "float64", "double":
		return iop.FloatType
	case "decimal":
		return iop.DecimalType
	case "date":
		return iop.DateType
	case "timestamp", "timestamptz", "timestamp_ns", "timestamptz_ns":
		return iop.TimestampType
	case "time":
		return iop.TimeType
	case "string":
		return iop.StringType
	case "uuid":
		return iop.UUIDType
	case "binary", "fixed":
		return iop.BinaryType
	case "struct", "list", "map":
		return iop.JsonType
	default:
		return iop.StringType
	}
}

func getArrowValue(col arrow.Array, rowIdx int) interface{} {
	if col.IsNull(rowIdx) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(rowIdx)
	case *array.Int8:
		return int64(arr.Value(rowIdx))
	case *array.Int16:
		return int64(arr.Value(rowIdx))
	case *array.Int32:
		return int64(arr.Value(rowIdx))
	case *array.Int64:
		return arr.Value(rowIdx)
	case *array.Uint8:
		return int64(arr.Value(rowIdx))
	case *array.Uint16:
		return int64(arr.Value(rowIdx))
	case *array.Uint32:
		return int64(arr.Value(rowIdx))
	case *array.Uint64:
		return int64(arr.Value(rowIdx))
	case *array.Float32:
		return float64(arr.Value(rowIdx))
	case *array.Float64:
		return arr.Value(rowIdx)
	case *array.String:
		return arr.Value(rowIdx)
	case *array.Binary:
		return arr.Value(rowIdx)
	case *array.FixedSizeBinary:
		return arr.Value(rowIdx)
	case *array.Date32:
		// Convert days since epoch to time.Time
		days := arr.Value(rowIdx)
		return time.Unix(int64(days)*86400, 0).UTC()
	case *array.Date64:
		// Convert milliseconds since epoch to time.Time
		ms := arr.Value(rowIdx)
		return time.Unix(int64(ms)/1000, (int64(ms)%1000)*1000000).UTC()
	case *array.Timestamp:
		// Get the timestamp value and convert based on unit
		val := arr.Value(rowIdx)
		dt := arr.DataType().(*arrow.TimestampType)
		switch dt.Unit {
		case arrow.Second:
			return time.Unix(int64(val), 0).UTC()
		case arrow.Millisecond:
			return time.Unix(int64(val)/1000, (int64(val)%1000)*1000000).UTC()
		case arrow.Microsecond:
			return time.Unix(int64(val)/1000000, (int64(val)%1000000)*1000).UTC()
		case arrow.Nanosecond:
			return time.Unix(int64(val)/1000000000, int64(val)%1000000000).UTC()
		}
	default:
		// For complex types, convert to string representation
		return fmt.Sprintf("%v", col.ValueStr(rowIdx))
	}

	return nil
}
