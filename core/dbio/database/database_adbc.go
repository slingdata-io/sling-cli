package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// ArrowDBConn is an Arrow FlightSQL connection
type ArrowDBConn struct {
	BaseConn
	URL        string
	db         adbc.Database
	Conn       adbc.Connection
	driverType dbio.Type // Underlying database type for templates
}

// getDriverType maps ADBC driver names to corresponding database types
// This allows using driver-specific SQL templates
func getDriverType(driverName string) dbio.Type {
	mapping := map[string]dbio.Type{
		"postgresql": dbio.TypeDbPostgres,
		"postgres":   dbio.TypeDbPostgres,
		"mssql":      dbio.TypeDbSQLServer,
		"sqlserver":  dbio.TypeDbSQLServer,
		"snowflake":  dbio.TypeDbSnowflake,
		"sqlite":     dbio.TypeDbSQLite,
		"duckdb":     dbio.TypeDbDuckDb,
		"bigquery":   dbio.TypeDbBigQuery,
	}
	if t, ok := mapping[strings.ToLower(driverName)]; ok {
		return t
	}
	return dbio.TypeDbArrowDBC // Fallback to ADBC template
}

// getArrowStringValue extracts a string value from an Arrow array at the given index.
// It handles String, LargeString, Binary, and LargeBinary types, and creates a copy
// of the string to avoid referencing Arrow buffer memory which may be freed.
func getArrowStringValue(arr arrow.Array, idx int) string {
	if arr.IsNull(idx) {
		return ""
	}
	switch a := arr.(type) {
	case *array.String:
		return strings.Clone(a.Value(idx))
	case *array.LargeString:
		return strings.Clone(a.Value(idx))
	case *array.Binary:
		return string(a.Value(idx))
	case *array.LargeBinary:
		return string(a.Value(idx))
	default:
		val := iop.GetValueFromArrowArray(arr, idx)
		if val != nil {
			return cast.ToString(val)
		}
		return ""
	}
}

// Init initiates the connection
func (conn *ArrowDBConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbArrowDBC
	conn.BaseConn.defaultPort = 12345

	// Build ADBC-specific properties map
	// Filter out sling-specific properties and only pass ADBC driver properties
	adbcProps := map[string]string{}

	// List of sling-specific keys to exclude
	excludeKeys := map[string]bool{
		"type":           true,
		"driver_name":    true,
		"orig_prop_keys": true,
		"name":           true,
		"conn":           true,
		"database":       true,
		"schema":         true,
		"url":            true,
	}

	// Property mappings from sling format to ADBC driver format
	// Different drivers expect different property names
	propertyMappings := map[string]string{
		"adbc.postgresql.connection_string": "uri",
		"adbc.sqlserver.connection_string":  "uri",
		"adbc.mssql.connection_string":      "uri",
		"adbc.snowflake.connection_string":  "adbc.snowflake.sql.uri",
		"adbc.sqlite.connection_string":     "uri",
		"adbc.duckdb.connection_string":     "path",
	}

	for key, val := range conn.properties {
		// Skip excluded keys
		if excludeKeys[key] {
			continue
		}

		// Check if there's a property mapping
		if mappedKey, ok := propertyMappings[key]; ok {
			adbcProps[mappedKey] = val
			continue
		}

		// Include driver property and any adbc.* prefixed properties
		if key == "driver" || key == "driver_entrypoint" || key == "uri" || strings.HasPrefix(key, "adbc.") {
			adbcProps[key] = val
		}
	}

	// Resolve driver path if not explicitly provided
	if adbcProps["driver"] == "" {
		if driverPath := conn.resolveDriverPath(); driverPath != "" {
			adbcProps["driver"] = driverPath
			g.Debug("auto-detected ADBC driver: %s", driverPath)
		}
	}

	db, err := drivermgr.Driver{}.NewDatabase(adbcProps)
	if err != nil {
		return g.Error(err, "could not init new ADBC database")
	}

	conn.db = db
	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	// Determine driver type for template delegation
	driverName := conn.GetProp("driver_name")
	conn.driverType = getDriverType(driverName)

	if err := conn.BaseConn.Init(); err != nil {
		return err
	}

	// Reload templates with driver-specific overrides
	// (BaseConn.Init() loaded the default ADBC template)
	return conn.LoadTemplates()
}

// resolveDriverPath attempts to find the ADBC driver from dbc CLI installation
func (conn *ArrowDBConn) resolveDriverPath() string {
	// Check if explicit 'driver' property is set
	if driver := conn.GetProp("driver"); driver != "" {
		return driver
	}

	// Get driver name hint from properties
	driverName := conn.GetProp("driver_name")
	if driverName == "" {
		return ""
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	// Platform-specific extension and paths
	var ext string
	var driverPaths []string

	switch runtime.GOOS {
	case "darwin":
		ext = ".dylib"
		// macOS: dbc installs to ~/Library/Application Support/ADBC/Drivers
		driverPaths = []string{
			filepath.Join(home, "Library", "Application Support", "ADBC", "Drivers"),
			filepath.Join(home, ".dbc", "drivers"),
		}
	case "windows":
		ext = ".dll"
		// Windows: %LOCALAPPDATA%\ADBC\Drivers or ~/.dbc/drivers
		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData != "" {
			driverPaths = append(driverPaths, filepath.Join(localAppData, "ADBC", "Drivers"))
		}
		driverPaths = append(driverPaths, filepath.Join(home, ".dbc", "drivers"))
	default:
		ext = ".so"
		// Linux: ~/.local/share/ADBC/Drivers or ~/.dbc/drivers
		driverPaths = []string{
			filepath.Join(home, ".local", "share", "ADBC", "Drivers"),
			filepath.Join(home, ".dbc", "drivers"),
		}
	}

	// Look for driver file in each potential location
	for _, basePath := range driverPaths {
		// Try multiple patterns to find the driver
		patterns := []string{
			// Pattern: basePath/postgresql/libadbc_driver_postgresql.dylib
			filepath.Join(basePath, driverName, "*"+driverName+"*"+ext),
			// Pattern: basePath/postgresql-1.9.0/libadbc_driver_postgresql.dylib
			filepath.Join(basePath, driverName+"-*", "*"+driverName+"*"+ext),
			// Pattern: basePath/*/libadbc_driver_postgresql.dylib
			filepath.Join(basePath, "*", "*"+driverName+"*"+ext),
		}

		for _, pattern := range patterns {
			matches, _ := filepath.Glob(pattern)
			if len(matches) > 0 {
				return matches[0]
			}
		}
	}

	return ""
}

// Connect opens the ADBC connection
func (conn *ArrowDBConn) Connect(timeOut ...int) (err error) {
	// Re-initialize database if it was closed
	if conn.db == nil {
		if err := conn.Init(); err != nil {
			return g.Error(err, "could not re-initialize ADBC database")
		}
	}

	conn.Conn, err = conn.db.Open(conn.context.Ctx)
	if err != nil {
		return g.Error(err, "could not connect to ADBC database")
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.postConnect()

	return nil
}

// Close closes the ADBC connection and database
func (conn *ArrowDBConn) Close() error {
	var connErr, dbErr error

	if conn.Conn != nil {
		connErr = conn.Conn.Close()
		conn.Conn = nil
	}

	if conn.db != nil {
		dbErr = conn.db.Close()
		conn.db = nil
	}

	if !cast.ToBool(conn.GetProp("silent")) && cast.ToBool(conn.GetProp("connected")) {
		g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "false")

	if connErr != nil {
		return g.Error(connErr, "error closing ADBC connection")
	}
	if dbErr != nil {
		return g.Error(dbErr, "error closing ADBC database")
	}

	return nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *ArrowDBConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {

	var c Connection
	switch conn.driverType {
	case dbio.TypeDbPostgres:
		c, err = NewConn("postgres://")
	case dbio.TypeDbSQLServer:
		c, err = NewConn("sqlserver://")
	case dbio.TypeDbSnowflake:
		c, err = NewConn("snowflake://")
	case dbio.TypeDbSQLite:
		c, err = NewConn("sqlite://")
	case dbio.TypeDbDuckDb:
		c, err = NewConn("duckdb://")
	case dbio.TypeDbBigQuery:
		c, err = NewConn("bigquery://")
	}

	if err != nil {
		return "", g.Error(err, "could not init conn for generating DDL")
	}

	return c.GenerateDDL(table, data, temporary)
}

// GetTemplateValue returns the template value for the given path
// It first checks the driver-specific template, then falls back to ADBC template
func (conn *ArrowDBConn) GetTemplateValue(path string) string {
	// First try driver-specific template
	if conn.driverType != "" && conn.driverType != dbio.TypeDbArrowDBC {
		value := conn.driverType.GetTemplateValue(path)
		if value != "" {
			return value
		}
	}
	// Fall back to ADBC template
	return conn.Type.GetTemplateValue(path)
}

// GetNativeType returns the native column type from generic
func (conn *ArrowDBConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	var ct iop.ColumnTyping
	if val := conn.GetProp("column_typing"); val != "" {
		g.Unmarshal(val, &ct)
	}
	return col.GetNativeType(conn.driverType, ct)
}

func (conn *ArrowDBConn) Template() dbio.Template {
	return conn.template
}

func (conn *ArrowDBConn) Quote(field string) string {
	return conn.template.Quote(field)
}

func (conn *ArrowDBConn) Unquote(field string) string {
	return conn.template.Unquote(field)
}

// LoadTemplates loads the appropriate yaml template
// For ADBC, it merges the driver-specific template with the ADBC template
// Driver template is base, ADBC template overrides for ADBC-specific behavior
func (conn *ArrowDBConn) LoadTemplates() error {
	// Load ADBC template without base
	adbcTemplate, err := conn.Type.Template(false)
	if err != nil {
		return g.Error(err, "could not load ADBC template")
	}

	// If we have a driver type, start with driver template as base
	if conn.driverType != "" && conn.driverType != dbio.TypeDbArrowDBC {
		driverTemplate, err := conn.driverType.Template()
		if err != nil {
			g.Warn("could not load driver template for %s: %v", conn.driverType, err)
			conn.template = adbcTemplate
			return nil
		}

		// Start with driver template, then overlay ADBC-specific values
		// This allows driver SQL syntax to be used, with ADBC overrides where needed
		for k, v := range adbcTemplate.Core {
			driverTemplate.Core[k] = v
		}
		for k, v := range adbcTemplate.Metadata {
			driverTemplate.Metadata[k] = v
		}
		for k, v := range adbcTemplate.Analysis {
			driverTemplate.Analysis[k] = v
		}
		for k, v := range adbcTemplate.Function {
			driverTemplate.Function[k] = v
		}
		// for k, v := range adbcTemplate.GeneralTypeMap {
		// 	driverTemplate.GeneralTypeMap[k] = v
		// }
		// for k, v := range adbcTemplate.NativeTypeMap {
		// 	driverTemplate.NativeTypeMap[k] = v
		// }
		for k, v := range adbcTemplate.Variable {
			driverTemplate.Variable[k] = v
		}

		conn.template = driverTemplate

		return nil
	}

	// load with base
	conn.template, err = conn.Type.Template(true)
	if err != nil {
		return g.Error(err, "could not load ADBC template")
	}

	return nil
}

// adbcResult implements sql.Result for ADBC operations
type adbcResult struct {
	rowsAffected int64
}

func (r adbcResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r adbcResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// ExecContext executes a SQL statement (read-only operations)
func (conn *ArrowDBConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if conn.Conn == nil {
		return nil, g.Error("ADBC connection is not open")
	}

	stmt, err := conn.Conn.NewStatement()
	if err != nil {
		return nil, g.Error(err, "could not create ADBC statement")
	}
	defer stmt.Close()

	// Handle argument substitution if any
	if len(args) > 0 {
		for _, arg := range args {
			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", cast.ToString(val), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", cast.ToString(val), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			default:
				v := strings.ReplaceAll(cast.ToString(val), "'", "''")
				sql = strings.Replace(sql, "?", "'"+v+"'", 1)
			}
		}
	}

	conn.LogSQL(sql)

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, g.Error(err, "could not set SQL query")
	}

	rowsAffected, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, g.Error(err, "could not execute SQL")
	}

	return adbcResult{rowsAffected: rowsAffected}, nil
}

// StreamRowsContext streams query results as a datastream using Arrow record batches
func (conn *ArrowDBConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	if conn.Conn == nil {
		return nil, g.Error("ADBC connection is not open")
	}

	queryContext := g.NewContext(ctx)

	// Get options
	limit := uint64(0)
	if len(options) > 0 {
		if val, ok := options[0]["limit"]; ok {
			limit = cast.ToUint64(val)
		}
	}

	// Create and configure statement
	stmt, err := conn.Conn.NewStatement()
	if err != nil {
		return nil, g.Error(err, "could not create ADBC statement")
	}

	conn.LogSQL(sql)

	if err := stmt.SetSqlQuery(sql); err != nil {
		stmt.Close()
		return nil, g.Error(err, "could not set SQL query")
	}

	// Execute query
	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		stmt.Close()
		return nil, g.Error(err, "could not execute query")
	}

	// Convert Arrow schema to columns
	schema := reader.Schema()
	columns := iop.ArrowSchemaToColumns(schema)

	// Create the next function for streaming records
	makeNextFunc := func() func(it *iop.Iterator) bool {
		var currentRecord arrow.Record
		var previousRecord arrow.Record // Keep previous record until next iteration to prevent string memory corruption
		var currentRowIdx int
		var recordChan = make(chan arrow.Record, 10)

		// Stream records in a goroutine
		go func() {
			defer close(recordChan)
			defer reader.Release()
			defer stmt.Close()

			for reader.Next() {
				record := reader.Record()
				record.Retain() // Retain so it doesn't get freed
				select {
				case recordChan <- record:
				case <-queryContext.Ctx.Done():
					record.Release()
					return
				}
			}

			if err := reader.Err(); err != nil {
				queryContext.CaptureErr(g.Error(err, "error reading Arrow records"))
			}
		}()

		return func(it *iop.Iterator) bool {
			if limit > 0 && uint64(it.Counter) >= limit {
				return false
			}

			// Release the previous record (now safe since its data has been consumed)
			if previousRecord != nil {
				previousRecord.Release()
				previousRecord = nil
			}

			// Check if we need to fetch next record batch
			if currentRecord == nil || currentRowIdx >= int(currentRecord.NumRows()) {
				// Move current to previous (will be released on next iteration)
				previousRecord = currentRecord
				currentRecord = nil

				select {
				case record, ok := <-recordChan:
					if !ok {
						// Channel closed, no more records
						return false
					}
					currentRecord = record
					currentRowIdx = 0
				case <-queryContext.Ctx.Done():
					return false
				}
			}

			// Convert current row to interface{} slice
			// Copy string values since Arrow buffer memory may be reused
			it.Row = make([]interface{}, currentRecord.NumCols())
			for colIdx := 0; colIdx < int(currentRecord.NumCols()); colIdx++ {
				col := currentRecord.Column(colIdx)
				val := iop.GetValueFromArrowArray(col, currentRowIdx)
				// Copy string values to avoid referencing Arrow buffer memory
				if s, ok := val.(string); ok {
					val = strings.Clone(s)
				}
				it.Row[colIdx] = val
			}

			currentRowIdx++
			return true
		}
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, columns, makeNextFunc())
	ds.NoDebug = strings.Contains(sql, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()

	if !ds.NoDebug {
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could not start datastream")
	}

	return ds, nil
}

// GetSQLColumns returns columns for a SQL query using Arrow schema
// This avoids wrapping with LIMIT which may not work for all database types
func (conn *ArrowDBConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	if !table.IsQuery() {
		return conn.GetColumns(table.FullName())
	}

	// For ADBC, we can execute the query directly and get schema from Arrow
	// Use limit 0 approach by wrapping, but if that fails, execute directly
	sql := table.SQL
	if sql == "" {
		sql = table.Select()
	}

	// Execute and get columns from Arrow schema directly
	ds, err := conn.StreamRowsContext(conn.Context().Ctx, sql, g.M("limit", 1))
	if err != nil {
		return columns, g.Error(err, "GetSQLColumns Error")
	}

	err = ds.WaitReady()
	if err != nil {
		return columns, g.Error(err, "Datastream Error")
	}

	ds.Collect(0) // advance the datastream so it can close
	return ds.Columns, nil
}

// BulkExportFlow exports data as a dataflow
func (conn *ArrowDBConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	// Build the query
	sql := table.Select()
	if table.SQL != "" {
		sql = table.SQL
	}

	ds, err := conn.StreamRowsContext(conn.Context().Ctx, sql)
	if err != nil {
		return nil, g.Error(err, "could not stream rows")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return nil, g.Error(err, "could not create dataflow")
	}

	return df, nil
}

// GetDatabases returns the list of databases/catalogs
func (conn *ArrowDBConn) GetDatabases() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("name"))

	if conn.Conn == nil {
		return data, g.Error("ADBC connection is not open")
	}

	reader, err := conn.Conn.GetObjects(conn.Context().Ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	if err != nil {
		return data, g.Error(err, "could not get databases")
	}
	defer reader.Release()

	for reader.Next() {
		record := reader.Record()
		// The first column should be catalog_name
		if record.NumCols() > 0 {
			catalogCol := record.Column(0)
			for i := 0; i < int(record.NumRows()); i++ {
				catalogName := getArrowStringValue(catalogCol, i)
				if catalogName != "" {
					data.Append([]interface{}{catalogName})
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return data, g.Error(err, "error reading databases")
	}

	return data, nil
}

// GetSchemas returns the list of schemas
func (conn *ArrowDBConn) GetSchemas() (data iop.Dataset, err error) {
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))

	if conn.Conn == nil {
		return data, g.Error("ADBC connection is not open")
	}

	reader, err := conn.Conn.GetObjects(conn.Context().Ctx, adbc.ObjectDepthDBSchemas, nil, nil, nil, nil, nil)
	if err != nil {
		return data, g.Error(err, "could not get schemas")
	}
	defer reader.Release()

	// Parse the nested structure: catalog -> db_schemas
	for reader.Next() {
		record := reader.Record()
		schemas := conn.extractSchemasFromRecord(record)
		for _, schemaName := range schemas {
			data.Append([]interface{}{schemaName})
		}
	}

	if err := reader.Err(); err != nil {
		return data, g.Error(err, "error reading schemas")
	}

	return data, nil
}

// extractSchemasFromRecord extracts schema names from GetObjects record
func (conn *ArrowDBConn) extractSchemasFromRecord(record arrow.Record) []string {
	var schemas []string

	// GetObjects returns: catalog_name (string), catalog_db_schemas (list of structs)
	if record.NumCols() < 2 {
		return schemas
	}

	dbSchemasCol := record.Column(1)
	listCol, ok := dbSchemasCol.(*array.List)
	if !ok {
		return schemas
	}

	for i := 0; i < int(record.NumRows()); i++ {
		if listCol.IsNull(i) {
			continue
		}

		start, end := listCol.ValueOffsets(i)
		valuesArray := listCol.ListValues()
		structArray, ok := valuesArray.(*array.Struct)
		if !ok {
			continue
		}

		// First field should be db_schema_name
		if structArray.NumField() > 0 {
			schemaNameField := structArray.Field(0)
			for j := int(start); j < int(end); j++ {
				schemaName := getArrowStringValue(schemaNameField, j)
				if schemaName != "" {
					schemas = append(schemas, schemaName)
				}
			}
		}
	}

	return schemas
}

// GetSchemata returns the full database metadata
func (conn *ArrowDBConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (schemata Schemata, err error) {
	schemata = Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	if conn.Conn == nil {
		return schemata, g.Error("ADBC connection is not open")
	}

	// Determine object depth based on level
	var depth adbc.ObjectDepth
	switch level {
	case SchemataLevelSchema:
		depth = adbc.ObjectDepthDBSchemas
	case SchemataLevelTable:
		depth = adbc.ObjectDepthTables
	case SchemataLevelColumn:
		depth = adbc.ObjectDepthAll
	default:
		depth = adbc.ObjectDepthAll
	}

	// Prepare filters
	var schemaFilter *string
	if schemaName != "" {
		schemaFilter = &schemaName
	}

	var tableFilter *string
	if len(tableNames) > 0 && tableNames[0] != "" {
		tableFilter = &tableNames[0]
	}

	reader, err := conn.Conn.GetObjects(conn.Context().Ctx, depth, nil, schemaFilter, tableFilter, nil, nil)
	if err != nil {
		return schemata, g.Error(err, "could not get schemata")
	}
	defer reader.Release()

	currDatabase := conn.GetProp("database")
	if currDatabase == "" {
		currDatabase = "default"
	}

	schemas := map[string]Schema{}

	// Parse the nested GetObjects structure
	for reader.Next() {
		record := reader.Record()
		conn.parseGetObjectsRecord(record, currDatabase, level, schemas)
	}

	if err := reader.Err(); err != nil {
		return schemata, g.Error(err, "error reading schemata")
	}

	schemata.Databases[currDatabase] = Database{
		Name:    currDatabase,
		Schemas: schemas,
	}

	return schemata, nil
}

// parseGetObjectsRecord parses the nested GetObjects Arrow record
func (conn *ArrowDBConn) parseGetObjectsRecord(record arrow.Record, database string, level SchemataLevel, schemas map[string]Schema) {
	// GetObjects structure:
	// - catalog_name: string
	// - catalog_db_schemas: list<struct<db_schema_name, db_schema_tables>>

	if record.NumCols() < 2 {
		return
	}

	catalogNameCol := record.Column(0)
	dbSchemasCol := record.Column(1)

	listCol, ok := dbSchemasCol.(*array.List)
	if !ok {
		return
	}

	for catalogIdx := 0; catalogIdx < int(record.NumRows()); catalogIdx++ {
		catalogName := ""
		if catalogNameCol != nil {
			catalogName = getArrowStringValue(catalogNameCol, catalogIdx)
		}

		if listCol.IsNull(catalogIdx) {
			continue
		}

		start, end := listCol.ValueOffsets(catalogIdx)
		schemaArray := listCol.ListValues()
		structArray, ok := schemaArray.(*array.Struct)
		if !ok {
			continue
		}

		// Parse each schema
		for schemaIdx := int(start); schemaIdx < int(end); schemaIdx++ {
			schemaName := ""
			if structArray.NumField() > 0 {
				schemaName = getArrowStringValue(structArray.Field(0), schemaIdx)
			}

			if schemaName == "" {
				continue
			}

			schema := Schema{
				Name:     schemaName,
				Database: database,
				Tables:   map[string]Table{},
			}

			if existing, ok := schemas[strings.ToLower(schemaName)]; ok {
				schema = existing
			}

			// Parse tables if available (depth >= Tables)
			if level != SchemataLevelSchema && structArray.NumField() > 1 {
				tablesField := structArray.Field(1)
				conn.parseTablesFromField(tablesField, schemaIdx, database, schemaName, catalogName, level, &schema)
			}

			schemas[strings.ToLower(schemaName)] = schema
		}
	}
}

// parseTablesFromField parses table information from the nested structure
func (conn *ArrowDBConn) parseTablesFromField(tablesField arrow.Array, schemaIdx int, database, schemaName, _ string, level SchemataLevel, schema *Schema) {
	tablesListCol, ok := tablesField.(*array.List)
	if !ok {
		return
	}

	if tablesListCol.IsNull(schemaIdx) {
		return
	}

	tStart, tEnd := tablesListCol.ValueOffsets(schemaIdx)
	tablesArray := tablesListCol.ListValues()
	tablesStruct, ok := tablesArray.(*array.Struct)
	if !ok {
		return
	}

	for tableIdx := int(tStart); tableIdx < int(tEnd); tableIdx++ {
		tableName := ""
		tableType := ""

		if tablesStruct.NumField() > 0 {
			tableName = getArrowStringValue(tablesStruct.Field(0), tableIdx)
		}
		if tablesStruct.NumField() > 1 {
			tableType = getArrowStringValue(tablesStruct.Field(1), tableIdx)
		}

		if tableName == "" {
			continue
		}

		table := Table{
			Name:     tableName,
			Schema:   schemaName,
			Database: database,
			Dialect:  dbio.TypeDbArrowDBC,
			IsView:   strings.ToUpper(tableType) == "VIEW",
			Columns:  iop.Columns{},
		}

		if existing, ok := schema.Tables[strings.ToLower(tableName)]; ok {
			table = existing
		}

		// Parse columns if level is Column
		if level == SchemataLevelColumn && tablesStruct.NumField() > 3 {
			columnsField := tablesStruct.Field(3) // table_columns is typically the 4th field
			conn.parseColumnsFromField(columnsField, tableIdx, &table)
		}

		schema.Tables[strings.ToLower(tableName)] = table
	}
}

// parseColumnsFromField parses column information from the nested structure
func (conn *ArrowDBConn) parseColumnsFromField(columnsField arrow.Array, tableIdx int, table *Table) {
	columnsListCol, ok := columnsField.(*array.List)
	if !ok {
		return
	}

	if columnsListCol.IsNull(tableIdx) {
		return
	}

	cStart, cEnd := columnsListCol.ValueOffsets(tableIdx)
	columnsArray := columnsListCol.ListValues()
	columnsStruct, ok := columnsArray.(*array.Struct)
	if !ok {
		return
	}

	for colIdx := int(cStart); colIdx < int(cEnd); colIdx++ {
		columnName := ""
		ordinalPosition := 0
		dataType := ""

		// column_name
		if columnsStruct.NumField() > 0 {
			columnName = getArrowStringValue(columnsStruct.Field(0), colIdx)
		}
		// ordinal_position
		if columnsStruct.NumField() > 1 {
			if val := iop.GetValueFromArrowArray(columnsStruct.Field(1), colIdx); val != nil {
				ordinalPosition = cast.ToInt(val)
			}
		}
		// xdbc_type_name (data type) - typically at index 5 or via another field
		if columnsStruct.NumField() > 5 {
			dataType = getArrowStringValue(columnsStruct.Field(5), colIdx)
		}

		if columnName == "" {
			continue
		}

		col := iop.Column{
			Name:     columnName,
			Position: ordinalPosition,
			DbType:   dataType,
			Type:     iop.StringType, // Default, should be inferred from data type
			Sourced:  true,
		}

		table.Columns = append(table.Columns, col)
	}
}

// BulkImportFlow imports data from a dataflow using ADBC bulk ingestion
func (conn *ArrowDBConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if conn.Conn == nil {
		return 0, g.Error("ADBC connection is not open")
	}

	for ds := range df.StreamCh {
		if err = ds.WaitReady(); err != nil {
			return count, g.Error(err, "error waiting for datastream")
		}

		cnt, err := conn.BulkImportStream(tableFName, ds)
		if err != nil {
			return count, g.Error(err, "error importing stream")
		}
		count += cnt
	}

	if err = df.Err(); err != nil {
		return count, g.Error(err, "error in dataflow")
	}

	return count, nil
}

// BulkImportStream imports data from a datastream using ADBC bulk ingestion
func (conn *ArrowDBConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	if conn.Conn == nil {
		return 0, g.Error("ADBC connection is not open")
	}

	// Parse table name to get schema
	table, _ := ParseTableName(tableFName, conn.Type)

	// Get ingest mode from property, default to append
	ingestMode := conn.getIngestMode()

	g.Trace("arrow schema => %s", iop.ColumnsToArrowSchema(ds.Columns))

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			// Columns changed, will create new schema
			g.Debug("ADBC BulkImportStream: processing batch with %d rows, columns: %v", len(batch.Rows), batch.Columns.Names())
		}

		// Convert batch to Arrow record reader
		reader, err := conn.batchToRecordReader(batch)
		if err != nil {
			return count, g.Error(err, "error converting batch to Arrow")
		}

		// Ingest using ADBC
		opts := adbc.IngestStreamOptions{}
		if table.Schema != "" {
			opts.DBSchema = table.Schema
		}

		ingested, err := adbc.IngestStream(
			conn.Context().Ctx,
			conn.Conn,
			reader,
			table.Name,
			ingestMode,
			opts,
		)
		reader.Release()

		if err != nil {
			return count, g.Error(err, "error ingesting batch via ADBC")
		}

		count += uint64(ingested)
	}

	return count, nil
}

// getIngestMode returns the ADBC ingest mode based on the ingest_mode property
// Valid values: create, append, replace, create_append
// Default: append
func (conn *ArrowDBConn) getIngestMode() string {
	mode := strings.ToLower(conn.GetProp("ingest_mode"))
	switch mode {
	case "create":
		return adbc.OptionValueIngestModeCreate
	case "replace":
		return adbc.OptionValueIngestModeReplace
	case "create_append":
		return adbc.OptionValueIngestModeCreateAppend
	case "append", "":
		return adbc.OptionValueIngestModeAppend
	default:
		g.Warn("Unknown ingest_mode '%s', using 'append'", mode)
		return adbc.OptionValueIngestModeAppend
	}
}

// batchToRecordReader converts an iop.Batch to an Arrow RecordReader
// It consumes all rows from the batch channel
func (conn *ArrowDBConn) batchToRecordReader(batch *iop.Batch) (array.RecordReader, error) {
	// Create Arrow schema from columns
	schema := iop.ColumnsToArrowSchema(batch.Columns)

	// Create memory allocator
	mem := memory.NewGoAllocator()

	// Create record builder
	builder := array.NewRecordBuilder(mem, schema)

	// Consume all rows from the batch channel and append to builder
	rowCount := 0
	for row := range batch.Rows {
		for colIdx, col := range batch.Columns {
			var val interface{}
			if colIdx < len(row) {
				val = row[colIdx]
			}
			iop.AppendToBuilder(builder.Field(colIdx), &col, val)
		}
		rowCount++
	}

	// Build the record
	record := builder.NewRecord()
	builder.Release()

	if rowCount == 0 {
		// Return empty reader with schema
		record.Release()
		return array.NewRecordReader(schema, []arrow.Record{})
	}

	// Create a RecordReader from the single record
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		record.Release()
		return nil, g.Error(err, "error creating record reader")
	}

	// Note: record will be released when reader is released
	return reader, nil
}
