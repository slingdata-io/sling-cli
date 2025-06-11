package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsv2config "github.com/aws/aws-sdk-go-v2/config"
	awsv2creds "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// IcebergConn is an Iceberg connection
type IcebergConn struct {
	BaseConn
	URL       string
	Warehouse string
	Catalog   catalog.Catalog
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
	conn.Warehouse = conn.GetProp("warehouse")
	if conn.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(conn.Warehouse))
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
		if strings.TrimSpace(err.Error()) == ":" {
			return g.Error("Failed to create REST catalog, check URI and credentials")
		}
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

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "is_view"))

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
		schemaName := tblID[len(tblID)-2]
		tableName := tblID[len(tblID)-1]
		data.Rows = append(data.Rows, []any{schemaName, tableName, false})
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
		if strings.Contains(err.Error(), "Table action can_get_metadata forbidden") {
			return columns, g.Error("%s, check table name", err.Error())
		}
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
			Type:     iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
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

func (conn *IcebergConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	if table.IsQuery() {
		return nil, g.Error("cannot get columns of a custom query in iceberg")
	}

	return conn.GetColumns(table.FullName(), fields...)
}

func (conn *IcebergConn) GetDataFiles(t Table) (dataFiles []iceberg.DataFile, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID, nil)
	if err != nil {
		return nil, g.Error(err, "could not load existing table: %s", t.FullName())
	}

	fs, err := tbl.FS(conn.Context().Ctx)
	if err != nil {
		return nil, g.Error(err, "could not load table FS => %s", t.FullName())
	}

	// Check each data file's statistics
	manifests, _ := tbl.CurrentSnapshot().Manifests(fs)
	for _, manifest := range manifests {
		// Only process data manifests (not delete manifests)
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}

		// Fetch manifest entries
		entries, err := manifest.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			dataFile := entry.DataFile()
			dataFiles = append(dataFiles, dataFile)
		}
	}

	return dataFiles, nil
}

// GetMaxValue gets the maximum value of the given column
func (conn *IcebergConn) GetMaxValue(t Table, colName string) (value any, maxCol iop.Column, err error) {

	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, maxCol, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID, nil)
	if err != nil {
		return 0, maxCol, g.Error(err, "could not load existing table: %s", t.FullName())
	}

	field, ok := tbl.Schema().FindFieldByNameCaseInsensitive(colName)
	if !ok {
		return nil, maxCol, g.Error("could not find column %s in table %s", colName, t.FullName())
	}

	dataFiles, err := conn.GetDataFiles(t)
	if err != nil {
		return nil, maxCol, g.Error(err, "could not get data files")
	}

	// Check each data file's statistics
	var globalMax iceberg.Literal

	for _, dataFile := range dataFiles {

		// Get upper bound values (max values) for each column
		upperBounds := dataFile.UpperBoundValues()

		// Check if we have statistics for our column
		for colId, maxBytes := range upperBounds {
			if colId != field.ID {
				continue
			}

			// Convert bytes to literal based on the field type
			maxLiteral, err := iceberg.LiteralFromBytes(field.Type, maxBytes)
			if err != nil {
				return nil, maxCol, err
			}

			// Update global max
			if globalMax == nil {
				globalMax = maxLiteral
			} else if maxLiteral.String() > globalMax.String() {
				globalMax = maxLiteral
			}
		}
	}

	// convert globalMax.String() to value according to field.Type
	maxCol = iop.Column{
		Name:    field.Name,
		Type:    iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
		DbType:  field.Type.String(),
		Sourced: true,
	}

	// cast value
	value = iop.NewStreamProcessor().CastVal(0, globalMax.String(), &maxCol)

	return
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

	tableSchema := cast.ToString(opts["table_schema"])
	tableName := cast.ToString(opts["table_name"])
	incrementalKey := cast.ToString(opts["incremental_key"])
	incrementalValue := cast.ToString(opts["incremental_value"])
	limit := cast.ToUint64(opts["limit"])

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)
	conn.LogSQL(sql)

	// Parse table identifier
	tableID := table.Identifier{tableSchema, tableName}

	// Load table
	tbl, err := conn.Catalog.LoadTable(queryContext.Ctx, tableID, nil)
	if err != nil {
		return nil, g.Error(err, "Failed to load table %s.%s", tableSchema, tableName)
	}

	// Get table schema and convert to columns if not provided
	if len(fetchedColumns) == 0 {
		schema := tbl.Schema()
		fetchedColumns = make(iop.Columns, 0, len(schema.Fields()))

		for i, field := range schema.Fields() {
			col := iop.Column{
				Name:     field.Name,
				Position: i + 1,
				Type:     iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
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

	if incrementalKey != "" && incrementalValue != "" {
		incrementalValue = strings.ReplaceAll(incrementalValue, "'", "") // clean
		// Find the field in the table schema to get proper field reference
		field, found := tbl.Schema().FindFieldByNameCaseInsensitive(incrementalKey)
		if !found {
			return nil, g.Error("incremental key field '%s' not found in table schema", incrementalKey)
		}

		// Create the greater than expression using field reference
		fieldRef := iceberg.Reference(field.Name)

		// Parse the incremental value to the appropriate type based on field type
		var literal iceberg.Literal

		// Try to parse as different types based on field type
		var greaterThanExpr iceberg.UnboundPredicate
		sp := iop.NewStreamProcessor()
		switch field.Type {
		case iceberg.PrimitiveTypes.String:
			literal = iceberg.NewLiteral(incrementalValue)
			greaterThanExpr = iceberg.GreaterThan(fieldRef, incrementalValue)
		case iceberg.PrimitiveTypes.Int32:
			if val, parseErr := cast.ToInt32E(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(val)
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as integer: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Int64:
			if val, parseErr := cast.ToInt64E(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(val)
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as long: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Float32:
			if val, parseErr := cast.ToFloat32E(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(val)
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as float: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Float64:
			if val, parseErr := cast.ToFloat64E(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(val)
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as double: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Date:
			if val, parseErr := sp.ParseTime(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(iceberg.Date(val.Unix() / 86400)) // Convert to days since epoch
				greaterThanExpr = iceberg.GreaterThan(fieldRef, iceberg.Date(val.Unix()/86400))
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as date: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.TimestampTz:
			if val, parseErr := sp.ParseTime(incrementalValue); parseErr == nil {
				literal = iceberg.NewLiteral(iceberg.Timestamp(val.UnixMicro()))
				greaterThanExpr = iceberg.GreaterThan(fieldRef, iceberg.Timestamp(val.UnixMicro()))
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as timestamp: %v", incrementalValue, parseErr)
			}
		default:
			// Default to string
			literal = iceberg.NewLiteral(incrementalValue)
			greaterThanExpr = iceberg.GreaterThan(fieldRef, incrementalValue)
		}
		_ = literal

		scanOpts = append(scanOpts, table.WithRowFilter(greaterThanExpr))
	}

	// Add selected fields if specified
	if fieldNames := opts["fields_array"]; fieldNames != nil {
		if fields, err := cast.ToStringSliceE(fieldNames); err == nil {
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
		var recordChan = make(chan arrow.Record, 1)
		var done = make(chan bool)

		// Stream records in a goroutine
		go func() {
			defer close(recordChan)
			defer close(done)
			for record, err := range recordIterator {
				if err != nil {
					queryContext.CaptureErr(g.Error(err, "Error reading arrow records"))
					return
				}
				recordChan <- record
			}
		}()

		return func(it *iop.Iterator) bool {
			if limit > 0 && it.Counter >= limit {
				return false
			}

			// Check if we need to fetch next record batch
			if currentRecord == nil || currentRowIdx >= int(currentRecord.NumRows()) {
				select {
				case record, ok := <-recordChan:
					if !ok {
						return false
					}
					currentRecord = record
					currentRowIdx = 0
				case <-done:
					return false
				}
			}

			// Convert current row to interface{} slice
			it.Row = make([]interface{}, currentRecord.NumCols())
			for colIdx := 0; colIdx < int(currentRecord.NumCols()); colIdx++ {
				col := currentRecord.Column(colIdx)
				it.Row[colIdx] = iop.GetValueFromArrowArray(col, currentRowIdx)
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

type icebergResult struct {
	TotalRows uint64
	res       driver.Result
}

func (r icebergResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r icebergResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

// ExecContext executes a write operation
func (conn *IcebergConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	switch {
	case strings.HasPrefix(sql, "create schema "):
		schema := strings.TrimPrefix(sql, "create schema ")
		return icebergResult{}, conn.CreateNamespaceIfNotExists(schema)
	case strings.HasPrefix(sql, "drop table "):
		table := strings.TrimPrefix(sql, "drop table ")
		return icebergResult{}, conn.DropTable(table)
	case strings.HasPrefix(sql, "drop view "):
		table := strings.TrimPrefix(sql, "drop view ")
		return icebergResult{}, conn.DropTable(table)
	case strings.Contains(sql, `"ddl_columns":`) && strings.Contains(sql, `"table":`):
		m, _ := g.UnmarshalMap(sql)
		var table Table
		var columns iop.Columns
		if err = g.JSONConvert(m["ddl_columns"], &columns); err != nil {
			return nil, g.Error(err, "could not convert ddl_columns")
		}
		if err = g.JSONConvert(m["table"], &table); err != nil {
			return nil, g.Error(err, "could not convert table for ")
		}

		if err = conn.CreateTable(table.FullName(), columns, ""); err != nil {
			return nil, g.Error(err, "could not create table")
		}

		return icebergResult{}, nil
	}

	// Iceberg doesn't support SQL execution directly
	// This would need to be implemented with table operations
	return nil, g.Error("Iceberg does not support direct SQL execution. Use bulk import/export operations instead:\n%s", sql)
}

func (conn *IcebergConn) CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error) {

	t, err := ParseTableName(tableName, conn.Type)
	if err != nil {
		return g.Error(err, "could create parse %s", tableName)
	}
	tableID := table.Identifier{t.Schema, t.Name}

	// First check if namespace exists (if schema is specified)
	if t.Schema != "" {
		if err := conn.CreateNamespaceIfNotExists(t.Schema); err != nil {
			return g.Error(err, "could create namespace %s", t.Schema)
		}
	}

	// Create Iceberg schema from datastream columns
	icebergSchema, err := conn.generateIcebergSchema(cols)
	if err != nil {
		return g.Error(err, "Failed to create Iceberg schema")
	}

	// Create table options
	createOpts := []catalog.CreateTableOpt{}

	// Add table properties including format-version: 2
	props := iceberg.Properties{
		"format-version":       "2",
		"write.format.default": "parquet",
		"created-by":           "sling-cli",
	}

	// Add any additional properties from connection
	if conn.Warehouse != "" {
		props["location"] = conn.Warehouse + "/" + strings.Join(tableID, "/")
	}

	createOpts = append(createOpts, catalog.WithProperties(props))

	// Create the table
	conn.LogSQL(g.F("create table %s (%s)", tableName, g.Marshal(icebergSchema)))

	_, err = conn.Catalog.CreateTable(conn.Context().Ctx, tableID, icebergSchema, createOpts...)
	if err != nil {
		return g.Error(err, "Failed to create table %s", tableName)
	}

	return nil
}

// GetCount returns -1 to skip validation
func (conn *IcebergConn) GetCount(tableFName string) (count int64, err error) {

	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	dataFiles, err := conn.GetDataFiles(t)
	if err != nil {
		return 0, g.Error(err, "could not get data files")
	}

	for _, dataFile := range dataFiles {
		count = count + dataFile.Count()
	}

	return count, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *IcebergConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	// Iceberg doesn't use traditional SQL DDL
	// Table creation is done through the catalog API
	return g.Marshal(g.M("table", table, "ddl_columns", data.Columns)), nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *IcebergConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	// Since Iceberg doesn't support SQL directly, just return the column name
	return conn.Self().Quote(srcCol.Name)
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *IcebergConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream inserts data into a table
func (conn *IcebergConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

func (conn *IcebergConn) TableExists(t Table) (exists bool, err error) {

	identifier := table.Identifier{t.Schema, t.Name}
	exists, err = conn.Catalog.CheckTableExists(conn.context.Ctx, identifier)
	if err != nil {
		return false, g.Error(err, "cannot check table existence: %s", t.FullName())
	}

	return
}

// DropTable drops given table.
func (conn *IcebergConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		t, err := ParseTableName(tableName, conn.Type)
		if err != nil {
			return g.Error(err, "cannot parse table name: %s", tableName)
		}

		exists, err := conn.TableExists(t)
		if err != nil {
			return g.Error(err, "cannot check table existence: %s", tableName)
		}

		identifier := table.Identifier{t.Schema, t.Name}
		if exists {
			err = conn.Catalog.DropTable(conn.context.Ctx, identifier)
			if err != nil {
				return g.Error(err, "cannot drop table")
			}
			g.Debug("table %s dropped", tableName)
		}
	}

	return nil
}

func (conn *IcebergConn) CreateNamespaceIfNotExists(schema string) (err error) {

	namespace := table.Identifier{schema}
	exists, nsErr := conn.Catalog.CheckNamespaceExists(conn.Context().Ctx, namespace)
	if nsErr != nil {
		// Some catalogs might not implement namespace checking, log and continue
		g.Debug("could not check if namespace exists: %v", nsErr)
	} else if !exists {
		// Try to create the namespace
		nsProps := iceberg.Properties{
			"created-by": "sling-cli",
		}
		if err = conn.Catalog.CreateNamespace(conn.Context().Ctx, namespace, nsProps); err != nil {
			return g.Error(err, "could not create namespace %s", schema)
		}
	}

	return nil
}

// SwapTable swaps two tables by renaming them
// 2025-06-09 => doesn't work, blank error
func (conn *IcebergConn) SwapTable(srcTable string, tgtTable string) (err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return g.Error(err, "Could not reconnect")
	}

	// Parse table names
	srcT, err := ParseTableName(srcTable, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse source table name: %s", srcTable)
	}

	tgtT, err := ParseTableName(tgtTable, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse target table name: %s", tgtTable)
	}

	// Create identifiers
	srcID := table.Identifier{srcT.Schema, srcT.Name}
	tgtID := table.Identifier{tgtT.Schema, tgtT.Name}

	// Create temporary table name
	tempName := tgtT.Name + "_tmp" + g.RandString(g.AlphaRunesLower, 2)
	tempID := table.Identifier{tgtT.Schema, tempName}

	// Drop temp table if exists
	tempT := Table{Schema: tgtT.Schema, Name: tempName}
	exists, err := conn.TableExists(tempT)
	if err != nil {
		return g.Error(err, "could not check temp table existence")
	}
	if exists {
		if err = conn.DropTable(tempT.FullName()); err != nil {
			return g.Error(err, "could not drop temp table %s", tempT.FullName())
		}
	}

	// Rename target table to temp
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, tgtID, tempID)
	if err != nil {
		return g.Error(err, "could not rename table %s to %s", tgtTable, tempT.FullName())
	}

	// Rename source table to target
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, srcID, tgtID)
	if err != nil {
		// Try to rollback
		conn.Catalog.RenameTable(conn.Context().Ctx, tempID, tgtID)
		return g.Error(err, "could not rename table %s to %s", srcTable, tgtTable)
	}

	// Rename temp table to source
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, tempID, srcID)
	if err != nil {
		// Try to rollback
		conn.Catalog.RenameTable(conn.Context().Ctx, tgtID, srcID)
		conn.Catalog.RenameTable(conn.Context().Ctx, tempID, tgtID)
		return g.Error(err, "could not rename table %s to %s", tempT.FullName(), srcTable)
	}

	g.Debug("successfully swapped tables %s and %s", srcTable, tgtTable)
	return nil
}

// BulkImportStream inserts a stream into a table using Arrow format.
// This method converts the incoming datastream to Apache Arrow format and uses
// the iceberg-go table.AppendTable API to write the data.
func (conn *IcebergConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return 0, g.Error(err, "Could not reconnect")
	}

	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID, nil)
	if err != nil {
		return 0, g.Error(err, "could not load existing table: %s", tableFName)
	}

	// Batch size for writing
	fileMaxRows := 500000
	if bSize := cast.ToInt(conn.GetProp("file_max_rows")); bSize > 0 {
		fileMaxRows = bSize
	}

	// Use snapshot properties
	snapshotProps := map[string]string{
		"operation": "append",
		"source":    "sling-cli",
	}

	// Process batches from the datastream
	for batch := range ds.BatchChan {
		// Create Arrow schema from batch columns (in case they changed)
		arrowSchema := iop.ColumnsToArrowSchema(batch.Columns)

		// Create memory allocator
		alloc := memory.NewGoAllocator()

		// Create record builder for this batch
		recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
		defer recordBuilder.Release()

		// Collect records for this batch
		records := []arrow.Record{}
		rowCount := 0

		for row := range batch.Rows {
			// Add row to record builder
			for i, val := range row {
				if i >= len(recordBuilder.Fields()) {
					break
				}
				iop.AppendToBuilder(recordBuilder.Field(i), &batch.Columns[i], val)
			}

			rowCount++
			count++

			// Create new record when batch size is reached
			if rowCount >= fileMaxRows {
				record := recordBuilder.NewRecord()
				records = append(records, record)
				rowCount = 0
				// Reset builder for next batch
				recordBuilder = array.NewRecordBuilder(alloc, arrowSchema)
				batch.Close() // close batch to write parquet file
			}
		}

		// Write final record if any rows remain
		if rowCount > 0 {
			record := recordBuilder.NewRecord()
			records = append(records, record)
		}

		// Skip if no records in this batch
		if len(records) == 0 {
			continue
		}

		// Create an Arrow table from records
		arrowTable := array.NewTableFromRecords(arrowSchema, records)
		defer arrowTable.Release()

		// Release individual records
		for _, rec := range records {
			rec.Release()
		}

		// Create a new transaction for this batch
		tx := tbl.NewTransaction()

		// Append the Arrow table to the Iceberg table
		err = tx.AppendTable(conn.Context().Ctx, arrowTable, cast.ToInt64(fileMaxRows), snapshotProps)
		if err != nil {
			return count, g.Error(err, "Failed to append data to Iceberg table %s", tableFName)
		}

		// Commit the transaction
		newTable, err := tx.Commit(conn.Context().Ctx)
		if err != nil {
			return count, g.Error(err, "Failed to commit data to Iceberg table %s", tableFName)
		}

		details := g.M("location", tbl.Location(), "snapshot_id", newTable.CurrentSnapshot().SnapshotID, "batch_rows", len(batch.Rows))
		g.Debug("committed iceberg snapshot", details)
	}

	return count, nil
}

// BulkExportStream reads table data in bulk
func (conn *IcebergConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// determine where clause to apply to scanOptions

	sql := table.Select()

	options := g.M("columns", table.Columns)

	parts := strings.Split(sql, "--iceberg-json=")
	if len(parts) == 2 {
		m, err := g.UnmarshalMap(parts[1])
		if err != nil {
			return nil, g.Error(err, "malformed iceberg-json payload")
		}

		for k, v := range m {
			options[k] = v
		}
	}

	// For bulk export, we can use the regular stream rows functionality
	return conn.Self().StreamRows(sql, options)
}

// GetSchemata obtain full schemata info for a schema and/or table
func (conn *IcebergConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	err := conn.Connect()
	if err != nil {
		return schemata, g.Error(err, "could not connect to get schemata")
	}

	schemaNames := []string{schemaName}
	if schemaName == "" {
		data, err := conn.GetSchemas()
		if err != nil {
			return schemata, g.Error(err, "could not get schemas")
		}
		schemaNames = data.ColValuesStr(0)
	}

	schemas := map[string]Schema{}
	ctx := g.NewContext(conn.context.Ctx, 10)

	// Use catalog name as database name
	currDatabase := conn.Warehouse
	if currDatabase == "" {
		currDatabase = "iceberg"
	}

	getOneSchemata := func(schemaName string, tables []string) error {
		defer ctx.Wg.Read.Done()

		var data iop.Dataset

		switch level {
		case SchemataLevelSchema:
			data.Columns = iop.NewColumnsFromFields("schema_name")
			data.Append([]any{schemaName})
		case SchemataLevelTable:
			data, err = conn.GetTablesAndViews(schemaName)
		case SchemataLevelColumn:
			// Get all tables in schema first
			tablesData, err := conn.GetTables(schemaName)
			if err != nil {
				return g.Error(err, "Could not get tables for schema %s", schemaName)
			}

			// Then get columns for each table
			data.Columns = iop.NewColumnsFromFields("schema_name", "table_name", "column_name", "data_type", "position", "is_view")
			for _, row := range tablesData.Rows {
				schemaName := cast.ToString(row[0])
				tableName := cast.ToString(row[1])

				if len(tables) > 0 {
					matched := false
					for _, table := range tables {
						if strings.EqualFold(table, tableName) {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				fullTableName := schemaName + "." + tableName
				columns, err := conn.GetColumns(fullTableName)
				if err != nil {
					g.Warn("Could not get columns for table %s: %v", fullTableName, err)
					continue
				}

				for _, col := range columns {
					data.Append([]any{
						schemaName,
						tableName,
						col.Name,
						col.DbType,
						col.Position,
						false, // is_view
					})
				}
			}
		}

		if err != nil {
			return g.Error(err, "Could not get schemata at %s level for schema=%s", level, schemaName)
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range data.Records() {
			schemaName := cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))

			// Skip if any names contain periods
			if strings.Contains(tableName, ".") ||
				strings.Contains(schemaName, ".") ||
				strings.Contains(columnName, ".") {
				continue
			}

			schema := Schema{
				Name:     schemaName,
				Database: currDatabase,
				Tables:   map[string]Table{},
			}

			if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
				schema = schemas[strings.ToLower(schema.Name)]
			}

			var table Table
			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				table = Table{
					Name:     tableName,
					Schema:   schemaName,
					Database: currDatabase,
					IsView:   cast.ToBool(rec["is_view"]),
					Columns:  iop.Columns{},
					Dialect:  dbio.TypeDbIceberg,
				}

				if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
					table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
				}
			}

			if level == SchemataLevelColumn {
				column := iop.Column{
					Name:     columnName,
					Type:     NativeTypeToGeneral(columnName, dataType, conn),
					Table:    tableName,
					Schema:   schemaName,
					Database: currDatabase,
					Position: cast.ToInt(rec["position"]),
					DbType:   dataType,
				}

				table.Columns = append(table.Columns, column)
			}

			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				schema.Tables[strings.ToLower(tableName)] = table
			}
			schemas[strings.ToLower(schema.Name)] = schema
		}

		schemata.Databases[strings.ToLower(currDatabase)] = Database{
			Name:    currDatabase,
			Schemas: schemas,
		}
		return nil
	}

	for _, schemaName := range schemaNames {
		g.Debug("getting schemata for %s %s", schemaName, g.Marshal(tableNames))

		ctx.Wg.Read.Add()
		go func() {
			err := getOneSchemata(schemaName, tableNames)
			ctx.CaptureErr(err)
		}()
	}

	ctx.Wg.Read.Wait()

	if err := ctx.Err(); err != nil {
		return schemata, g.Error(err)
	}

	return schemata, nil
}

// generateIcebergSchema creates an Iceberg schema from iop columns
func (conn *IcebergConn) generateIcebergSchema(columns iop.Columns) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, len(columns))

	for i, col := range columns {
		// Convert iop column type to Iceberg type
		icebergType := conn.iopTypeToIcebergPrimitiveType(col)

		// Create nested field with auto-assigned ID (starting from 1)
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     col.Name,
			Type:     icebergType,
			Required: false, // Default to nullable unless we have constraint info
		}

		// Check if column has NOT NULL constraint
		if col.Constraint != nil && strings.Contains(strings.ToLower(col.Constraint.Expression), "not null") {
			fields[i].Required = true
		}
	}

	// Create schema with ID 0 (initial schema)
	schema := iceberg.NewSchema(0, fields...)
	return schema, nil
}

// iopTypeToIcebergPrimitiveType converts iop column type to Iceberg primitive type
func (conn *IcebergConn) iopTypeToIcebergPrimitiveType(col iop.Column) iceberg.Type {
	switch col.Type {
	case iop.BoolType:
		return iceberg.PrimitiveTypes.Bool
	case iop.IntegerType, iop.SmallIntType:
		return iceberg.PrimitiveTypes.Int32
	case iop.BigIntType:
		return iceberg.PrimitiveTypes.Int64
	case iop.FloatType:
		return iceberg.PrimitiveTypes.Float64
	case iop.DecimalType:
		precision := lo.Ternary(col.DbPrecision > 0, col.DbPrecision, env.DdlMinDecLength)
		scale := lo.Ternary(col.DbScale > 0, col.DbScale, env.DdlMinDecScale)
		return iceberg.DecimalTypeOf(precision, scale)
	case iop.DateType:
		return iceberg.PrimitiveTypes.Date
	case iop.DatetimeType, iop.TimestampType:
		return iceberg.PrimitiveTypes.TimestampTz // arrow Timestamp converts to iceberg timestampz
	case iop.TimestampzType:
		return iceberg.PrimitiveTypes.TimestampTz
	case iop.TimeType:
		return iceberg.PrimitiveTypes.Time
	case iop.TimezType:
		// Iceberg doesn't have a specific time with timezone type, use Time
		return iceberg.PrimitiveTypes.Time
	case iop.StringType, iop.TextType:
		return iceberg.PrimitiveTypes.String
	case iop.UUIDType:
		return iceberg.PrimitiveTypes.UUID
	case iop.BinaryType:
		return iceberg.PrimitiveTypes.Binary
	case iop.JsonType:
		// JSON is typically stored as string in Iceberg
		return iceberg.PrimitiveTypes.String
	default:
		// Default to string for unknown types
		return iceberg.PrimitiveTypes.String
	}
}

// Helper functions

func parseTableIdentifier(tableName string) table.Identifier {
	// Split by dots to create namespace and table parts
	tableName = strings.ReplaceAll(tableName, `"`, "")
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
