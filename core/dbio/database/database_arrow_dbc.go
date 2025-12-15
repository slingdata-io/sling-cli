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
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// ArrowDBConn is an Arrow FlightSQL connection
type ArrowDBConn struct {
	BaseConn
	URL  string
	db   adbc.Database
	Conn adbc.Connection
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

	return conn.BaseConn.Init()
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
	}

	if conn.db != nil {
		dbErr = conn.db.Close()
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
				catalogName := iop.GetValueFromArrowArray(catalogCol, i)
				if catalogName != nil {
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
				schemaName := iop.GetValueFromArrowArray(schemaNameField, j)
				if schemaName != nil {
					schemas = append(schemas, cast.ToString(schemaName))
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
		if val := iop.GetValueFromArrowArray(catalogNameCol, catalogIdx); val != nil {
			catalogName = cast.ToString(val)
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
				schemaNameField := structArray.Field(0)
				if val := iop.GetValueFromArrowArray(schemaNameField, schemaIdx); val != nil {
					schemaName = cast.ToString(val)
				}
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
			if val := iop.GetValueFromArrowArray(tablesStruct.Field(0), tableIdx); val != nil {
				tableName = cast.ToString(val)
			}
		}
		if tablesStruct.NumField() > 1 {
			if val := iop.GetValueFromArrowArray(tablesStruct.Field(1), tableIdx); val != nil {
				tableType = cast.ToString(val)
			}
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
			if val := iop.GetValueFromArrowArray(columnsStruct.Field(0), colIdx); val != nil {
				columnName = cast.ToString(val)
			}
		}
		// ordinal_position
		if columnsStruct.NumField() > 1 {
			if val := iop.GetValueFromArrowArray(columnsStruct.Field(1), colIdx); val != nil {
				ordinalPosition = cast.ToInt(val)
			}
		}
		// xdbc_type_name (data type) - typically at index 5 or via another field
		if columnsStruct.NumField() > 5 {
			if val := iop.GetValueFromArrowArray(columnsStruct.Field(5), colIdx); val != nil {
				dataType = cast.ToString(val)
			}
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
