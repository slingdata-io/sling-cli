package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// AzureTableConn is an Azure Table connection
type AzureTableConn struct {
	BaseConn
	URL           string
	Client        *aztables.ServiceClient
	AccountName   string
	AccountKey    string
	SASToken      string
	ConnectionStr string
}

// Init initiates the object
func (conn *AzureTableConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbAzureTable

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Init initiates the object
func (conn *AzureTableConn) getNewClient(timeOut ...int) (client *aztables.ServiceClient, err error) {
	serviceURL := fmt.Sprintf("https://%s.table.core.windows.net", conn.AccountName)

	// Try different authentication methods
	var cred azcore.TokenCredential
	var sharedKeyCred *aztables.SharedKeyCredential

	if conn.ConnectionStr != "" {
		// Use connection string
		client, err = aztables.NewServiceClientFromConnectionString(conn.ConnectionStr, nil)
		if err != nil {
			return nil, g.Error(err, "could not create client from connection string")
		}
		return client, nil
	} else if conn.SASToken != "" {
		// Use SAS token
		sasURL := serviceURL + "?" + strings.TrimPrefix(conn.SASToken, "?")
		client, err = aztables.NewServiceClientWithNoCredential(sasURL, nil)
		if err != nil {
			return nil, g.Error(err, "could not create client with SAS token")
		}
		return client, nil
	} else if conn.AccountKey != "" {
		// Use account key
		sharedKeyCred, err = aztables.NewSharedKeyCredential(conn.AccountName, conn.AccountKey)
		if err != nil {
			return nil, g.Error(err, "could not create shared key credential")
		}
		client, err = aztables.NewServiceClientWithSharedKey(serviceURL, sharedKeyCred, nil)
		if err != nil {
			return nil, g.Error(err, "could not create client with shared key")
		}
		return client, nil
	} else {
		// Try default Azure credential
		cred, err = azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, g.Error(err, "could not get Azure credential")
		}
		client, err = aztables.NewServiceClient(serviceURL, cred, nil)
		if err != nil {
			return nil, g.Error(err, "could not create client with default credential")
		}
		return client, nil
	}
}

// Connect connects to the database
func (conn *AzureTableConn) Connect(timeOut ...int) error {
	// Extract connection properties
	conn.AccountName = conn.GetProp("account_name")
	if conn.AccountName == "" {
		conn.AccountName = conn.GetProp("account")
	}
	conn.AccountKey = conn.GetProp("account_key")
	if conn.AccountKey == "" {
		conn.AccountKey = conn.GetProp("key")
	}
	conn.SASToken = conn.GetProp("sas_token")
	conn.ConnectionStr = conn.GetProp("conn_str")

	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	// Test connection by listing tables
	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, 5*time.Second)
	defer cancel()

	pager := conn.Client.NewListTablesPager(nil)
	if pager.More() {
		_, err = pager.NextPage(ctx)
		if err != nil {
			return g.Error(err, "Failed to list tables")
		}
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

func (conn *AzureTableConn) Close() error {
	conn.Client = nil
	g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	return nil
}

// NewTransaction creates a new transaction
func (conn *AzureTableConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// Azure Tables does not support transactions
	return nil, g.Error("transactions not supported in Azure Tables")
}

// GetTableColumns returns columns for a table
func (conn *AzureTableConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	// Sanitize table name for Azure Table Storage
	tableName := sanitizeAzureTableName(table.Name)
	// Azure Tables is schema-less, but we'll return a basic structure
	// with PartitionKey, RowKey, and Timestamp as mandatory fields
	columns = iop.Columns{
		{
			Name:     "PartitionKey",
			Type:     iop.TextType,
			Table:    table.Name,
			Schema:   table.Schema,
			Database: table.Database,
			Position: 1,
			DbType:   "string",
		},
		{
			Name:     "RowKey",
			Type:     iop.TextType,
			Table:    table.Name,
			Schema:   table.Schema,
			Database: table.Database,
			Position: 2,
			DbType:   "string",
		},
		{
			Name:     "Timestamp",
			Type:     iop.TimestampType,
			Table:    table.Name,
			Schema:   table.Schema,
			Database: table.Database,
			Position: 3,
			DbType:   "datetime",
		},
	}

	// Try to get a sample of data to infer additional columns
	tableClient := conn.Client.NewClient(tableName)
	top := int32(10)
	queryOptions := &aztables.ListEntitiesOptions{
		Top: &top,
	}

	pager := tableClient.NewListEntitiesPager(queryOptions)
	ctx := conn.BaseConn.Context().Ctx

	sampleData := []map[string]any{}
	for pager.More() && len(sampleData) < 10 {
		page, err := pager.NextPage(ctx)
		if err != nil {
			// Table might not exist or be empty, return basic columns
			return columns, nil
		}

		for _, entity := range page.Entities {
			entityMap := map[string]any{}
			// Entity is already a byte array from the SDK
			err = json.Unmarshal(entity, &entityMap)
			if err == nil {
				sampleData = append(sampleData, entityMap)
			}
		}
	}

	// Analyze sample data to find additional columns
	foundColumns := map[string]iop.ColumnType{}
	position := 4

	for _, entity := range sampleData {
		for key, value := range entity {
			// Skip system properties
			if key == "PartitionKey" || key == "RowKey" || key == "Timestamp" ||
				strings.HasPrefix(key, "odata.") || key == "Etag" ||
				strings.Contains(key, "@odata") {
				continue
			}

			if _, exists := foundColumns[key]; !exists {
				colType := inferAzureTableType(value)
				foundColumns[key] = colType

				columns = append(columns, iop.Column{
					Name:     key,
					Type:     colType,
					Table:    table.Name,
					Schema:   table.Schema,
					Database: table.Database,
					Position: position,
					DbType:   getAzureTableDbType(colType),
				})
				position++
			}
		}
	}

	return columns, nil
}

func inferAzureTableType(value any) iop.ColumnType {
	switch v := value.(type) {
	case string:
		// Try to parse as time
		if _, err := time.Parse(time.RFC3339, v); err == nil {
			return iop.TimestampType
		}
		return iop.TextType
	case float64:
		if v == float64(int64(v)) {
			return iop.BigIntType
		}
		return iop.FloatType
	case int, int32, int64:
		return iop.BigIntType
	case bool:
		return iop.BoolType
	case time.Time:
		return iop.TimestampType
	case []byte:
		return iop.BinaryType
	default:
		return iop.TextType
	}
}

func getAzureTableDbType(colType iop.ColumnType) string {
	switch colType {
	case iop.TextType:
		return "string"
	case iop.BigIntType:
		return "int64"
	case iop.IntegerType:
		return "int32"
	case iop.FloatType:
		return "double"
	case iop.BoolType:
		return "boolean"
	case iop.TimestampType:
		return "datetime"
	case iop.BinaryType:
		return "binary"
	default:
		return "string"
	}
}

func (conn *AzureTableConn) ExecContext(ctx context.Context, sql string, args ...any) (result sql.Result, err error) {
	// Azure Tables doesn't support SQL, but we can handle specific operations
	sqlLower := strings.ToLower(strings.TrimSpace(sql))

	// Handle DROP TABLE
	if strings.HasPrefix(sqlLower, "drop table") {
		// Extract table name from SQL
		parts := strings.Fields(sql)
		if len(parts) >= 3 {
			tableName := strings.Trim(parts[2], `"`)
			tableName = strings.ReplaceAll(tableName, `"."`, ".")
			// Remove schema prefix if present
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				tableName = parts[len(parts)-1]
			}

			// Azure Table names must be alphanumeric only
			tableName = sanitizeAzureTableName(tableName)

			// Delete the table
			tableClient := conn.Client.NewClient(tableName)
			_, err = tableClient.Delete(ctx, nil)
			if err != nil && !strings.Contains(err.Error(), "ResourceNotFound") {
				return nil, g.Error(err, "could not delete table %s", tableName)
			}
			return &AzureTableResult{rowsAffected: 0}, nil
		}
	}

	// Handle CREATE TABLE
	if strings.HasPrefix(sqlLower, "create table") {
		// Extract table name from SQL
		parts := strings.Fields(sql)
		if len(parts) >= 3 {
			tableName := strings.Trim(parts[2], `"`)
			tableName = strings.ReplaceAll(tableName, `"."`, ".")
			// Remove schema prefix if present
			if strings.Contains(tableName, ".") {
				parts := strings.Split(tableName, ".")
				tableName = parts[len(parts)-1]
			}

			// Azure Table names must be alphanumeric only
			tableName = sanitizeAzureTableName(tableName)

			// Create the table
			tableClient := conn.Client.NewClient(tableName)
			_, err = tableClient.CreateTable(ctx, nil)
			if err != nil && !strings.Contains(err.Error(), "TableAlreadyExists") {
				return nil, g.Error(err, "could not create table %s", tableName)
			}
			return &AzureTableResult{rowsAffected: 0}, nil
		}
	}

	return nil, g.Error("SQL operation not supported on AzureTableConn: %s", sql)
}

// AzureTableResult implements sql.Result interface
type AzureTableResult struct {
	rowsAffected int64
}

func (r *AzureTableResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r *AzureTableResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (conn *AzureTableConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	options, _ := g.UnmarshalMap(table.SQL)
	ds, err := conn.StreamRowsContext(conn.Context().Ctx, table.FullName(), options)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

func (conn *AzureTableConn) StreamRowsContext(ctx context.Context, tableName string, Opts ...map[string]any) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(Opts)
	Limit := int64(0) // infinite
	if val := cast.ToInt64(opts["limit"]); val > 0 {
		Limit = val
	}

	table, _ := ParseTableName(tableName, conn.Type)
	sanitizedName := sanitizeAzureTableName(table.Name)
	tableClient := conn.Client.NewClient(sanitizedName)

	// Build query options
	queryOptions := &aztables.ListEntitiesOptions{}

	// Handle filter if provided
	if filter, ok := opts["filter"]; ok {
		queryOptions.Filter = g.String(cast.ToString(filter))
	}

	// Handle incremental mode
	updateKey := cast.ToString(opts["update_key"])
	incrementalValue := cast.ToString(opts["value"])
	startValue := cast.ToString(opts["start_value"])
	endValue := cast.ToString(opts["end_value"])

	if updateKey != "" {
		if incrementalValue != "" {
			// Incremental mode
			filterStr := fmt.Sprintf("%s gt '%s'", updateKey, incrementalValue)
			queryOptions.Filter = &filterStr
		} else if startValue != "" && endValue != "" {
			// Backfill mode
			filterStr := fmt.Sprintf("%s ge '%s' and %s le '%s'", updateKey, startValue, updateKey, endValue)
			queryOptions.Filter = &filterStr
		}
	}

	// Handle select fields
	if fields, ok := opts["fields"]; ok {
		fieldsList := cast.ToStringSlice(fields)
		if len(fieldsList) > 0 {
			selectStr := strings.Join(fieldsList, ",")
			queryOptions.Select = &selectStr
		}
	}

	// Set limit for paging
	if Limit > 0 && Limit < 1000 {
		top := int32(Limit)
		queryOptions.Top = &top
	} else if Limit == 0 {
		top := int32(1000)
		queryOptions.Top = &top // Default page size
	}

	pager := tableClient.NewListEntitiesPager(queryOptions)

	// Create datastream
	ds = iop.NewDatastreamContext(ctx, nil)

	// Get columns
	columns, err := conn.GetTableColumns(&table)
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	ds.Columns = columns

	// Setup iterator
	counter := uint64(0)
	limit := cast.ToUint64(Limit)

	// Keep track of current page and position
	var currentEntities [][]byte
	var entityIndex int

	var nextFunc func(it *iop.Iterator) bool
	nextFunc = func(it *iop.Iterator) bool {
		if Limit > 0 && counter >= limit {
			return false
		} else if it.Context.Err() != nil {
			return false
		}

		// Continue processing entities from current page if we have any
		if currentEntities != nil && entityIndex < len(currentEntities) {
			entity := currentEntities[entityIndex]
			entityIndex++

			// Convert entity to row
			row := make([]any, len(ds.Columns))
			entityMap := map[string]any{}

			// Entity is already a byte array from the SDK
			err := json.Unmarshal(entity, &entityMap)
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "could not unmarshal entity"))
				// Try next entity
				return false
			}

			// Map values to columns
			for i, col := range ds.Columns {
				if val, ok := entityMap[col.Name]; ok {
					row[i] = convertAzureTableValue(val, col.Type)
				} else {
					row[i] = nil
				}
			}

			it.Row = row
			counter++
			return true
		}

		// Need to get next page
		if !pager.More() {
			return false
		}

		page, err := pager.NextPage(ctx)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "could not get next page"))
			return false
		}

		if len(page.Entities) == 0 {
			// Empty page, try next
			return nextFunc(it)
		}

		currentEntities = page.Entities
		entityIndex = 0
		// Process first entity from new page
		return nextFunc(it)
	}

	ds.SetIterator(ds.NewIterator(ds.Columns, nextFunc))
	ds.NoDebug = strings.Contains(tableName, noDebugKey)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	err = ds.Start()
	if err != nil {
		return ds, g.Error(err, "could start datastream")
	}

	return
}

func convertAzureTableValue(value any, targetType iop.ColumnType) any {
	if value == nil {
		return nil
	}

	// Handle OData type annotations
	if m, ok := value.(map[string]any); ok {
		if odataType, hasType := m["@odata.type"]; hasType {
			if val, hasValue := m["value"]; hasValue {
				switch odataType {
				case "Edm.DateTime":
					if strVal, ok := val.(string); ok {
						if t, err := time.Parse(time.RFC3339, strVal); err == nil {
							return t
						}
					}
				case "Edm.Int64":
					return cast.ToInt64(val)
				case "Edm.Double":
					return cast.ToFloat64(val)
				case "Edm.Binary":
					if strVal, ok := val.(string); ok {
						return []byte(strVal)
					}
				}
			}
		}
	}

	// Simple type conversion based on target type
	switch targetType {
	case iop.TimestampType:
		if strVal, ok := value.(string); ok {
			if t, err := time.Parse(time.RFC3339, strVal); err == nil {
				return t
			}
		}
		return value
	case iop.BigIntType:
		return cast.ToInt64(value)
	case iop.FloatType:
		return cast.ToFloat64(value)
	case iop.BoolType:
		return cast.ToBool(value)
	case iop.BinaryType:
		if strVal, ok := value.(string); ok {
			return []byte(strVal)
		}
		return value
	default:
		return cast.ToString(value)
	}
}

// TableExists checks if a table exists
func (conn *AzureTableConn) TableExists(table Table) (exists bool, err error) {
	// Sanitize table name for Azure Table Storage
	sanitizedName := sanitizeAzureTableName(table.Name)

	// List tables and check if our table exists
	ctx := conn.Context().Ctx
	pager := conn.Client.NewListTablesPager(nil)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return false, g.Error(err, "could not list tables")
		}

		for _, t := range page.Tables {
			if t.Name != nil && *t.Name == sanitizedName {
				return true, nil
			}
		}
	}

	return false, nil
}

// GetCount returns the row count for a table
func (conn *AzureTableConn) GetCount(tableFName string) (int64, error) {
	table, _ := ParseTableName(tableFName, conn.Type)
	sanitizedName := sanitizeAzureTableName(table.Name)

	// Azure Tables doesn't have a direct count query, we need to iterate
	ctx := conn.Context().Ctx
	tableClient := conn.Client.NewClient(sanitizedName)

	count := int64(0)
	pager := tableClient.NewListEntitiesPager(&aztables.ListEntitiesOptions{
		Select: g.Ptr("PartitionKey,RowKey"), // Only get minimal data
	})

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return 0, g.Error(err, "could not count entities")
		}
		count += int64(len(page.Entities))
	}

	return count, nil
}

// GetSchemas returns schemas
func (conn *AzureTableConn) GetSchemas() (data iop.Dataset, err error) {
	// Azure Tables doesn't have schemas, return a default one
	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]any{"default"})
	return data, nil
}

// GetTables returns tables
func (conn *AzureTableConn) GetTables(schema string) (data iop.Dataset, err error) {
	ctx := conn.Context().Ctx
	pager := conn.Client.NewListTablesPager(nil)

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return data, g.Error(err, "could not list tables")
		}

		for _, table := range page.Tables {
			if table.Name != nil {
				data.Append([]any{*table.Name})
			}
		}
	}

	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *AzureTableConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	database := Database{
		Name:    "azuretable",
		Schemas: map[string]Schema{},
	}

	schema := Schema{
		Name:     "default",
		Database: database.Name,
		Tables:   map[string]Table{},
	}

	tablesData, err := conn.GetTables(schema.Name)
	if err != nil {
		return schemata, g.Error(err, "Could not get tables")
	}

	for _, tableRow := range tablesData.Rows {
		tableName := cast.ToString(tableRow[0])

		table := Table{
			Name:     tableName,
			Schema:   schema.Name,
			Database: database.Name,
			IsView:   false,
			Dialect:  conn.GetType(),
		}

		if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
			if level == SchemataLevelColumn && (len(tableNames) == 0 || g.In(tableName, tableNames...)) {
				columns, err := conn.GetTableColumns(&table)
				if err != nil {
					g.Warn("could not get columns for table %s: %s", tableName, err)
				} else {
					table.Columns = columns
				}
			}
			schema.Tables[strings.ToLower(tableName)] = table
		}
	}

	database.Schemas[strings.ToLower(schema.Name)] = schema
	schemata.Databases[strings.ToLower(database.Name)] = database

	return schemata, nil
}

// BulkImportFlow imports data into Azure Tables
func (conn *AzureTableConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	// Create table client
	table, _ := ParseTableName(tableFName, conn.Type)
	sanitizedName := sanitizeAzureTableName(table.Name)
	tableClient := conn.Client.NewClient(sanitizedName)

	// Ensure table exists
	_, err = tableClient.CreateTable(conn.Context().Ctx, nil)
	if err != nil && !strings.Contains(err.Error(), "TableAlreadyExists") {
		return 0, g.Error(err, "could not create table")
	}

	// Process dataflow
	batchSize := 100 // Azure Tables supports max 100 entities per batch
	batch := make([]map[string]any, 0, batchSize)

	// Find PartitionKey and RowKey columns
	partitionKeyCol := -1
	rowKeyCol := -1
	for i, col := range df.Columns {
		if strings.EqualFold(col.Name, "PartitionKey") {
			partitionKeyCol = i
		} else if strings.EqualFold(col.Name, "RowKey") {
			rowKeyCol = i
		}
	}

	// If no PartitionKey or RowKey, we'll generate them
	// Don't use existing columns as PartitionKey/RowKey unless they're explicitly named as such

	processRow := func(row []any) error {
		entity := make(map[string]any)

		// Set PartitionKey
		if partitionKeyCol >= 0 && partitionKeyCol < len(row) {
			entity["PartitionKey"] = cast.ToString(row[partitionKeyCol])
		} else {
			// Use a default partition key for all entities
			entity["PartitionKey"] = "default"
		}

		// Set RowKey
		if rowKeyCol >= 0 && rowKeyCol < len(row) {
			entity["RowKey"] = cast.ToString(row[rowKeyCol])
		} else {
			// Generate unique row key
			entity["RowKey"] = g.NewTsID("")
		}

		// Add other properties
		for i, col := range df.Columns {
			if i == partitionKeyCol || i == rowKeyCol {
				continue // Already set
			}

			if i < len(row) && row[i] != nil {
				propName := col.Name
				propValue := convertToAzureTableProperty(row[i], col.Type)
				if propValue != nil {
					entity[propName] = propValue
				}
			}
		}

		batch = append(batch, entity)

		// Submit batch when full
		if len(batch) >= batchSize {
			err := submitBatch(tableClient, batch)
			if err != nil {
				return err
			}
			count += uint64(len(batch))
			batch = batch[:0] // Clear batch
		}

		return nil
	}

	// Process dataflow rows
	err = df.Err()
	if err != nil {
		return 0, g.Error(err, "dataflow error")
	}

	// Set concurrency limit
	df.Context.SetConcurrencyLimit(conn.Context().Wg.Limit)

	for stream := range df.StreamCh {
		err = stream.Start()
		if err != nil {
			return count, g.Error(err, "could not start stream")
		}

		for row := range stream.Rows() {
			err = processRow(row)
			if err != nil {
				return count, g.Error(err, "error processing row")
			}
		}

		if stream.Err() != nil {
			return count, g.Error(stream.Err(), "stream error")
		}
	}

	// Submit final batch
	if len(batch) > 0 {
		err = submitBatch(tableClient, batch)
		if err != nil {
			return count, g.Error(err, "error submitting final batch")
		}
		count += uint64(len(batch))
	}

	return count, nil
}

func submitBatch(tableClient *aztables.Client, entities []map[string]any) error {
	if len(entities) == 0 {
		return nil
	}

	// Azure Tables batch operations require all entities to have the same PartitionKey
	// Group by PartitionKey
	groups := make(map[string][]map[string]any)
	for _, entity := range entities {
		pk := cast.ToString(entity["PartitionKey"])
		groups[pk] = append(groups[pk], entity)
	}

	// Submit each group
	successCount := 0
	for _, group := range groups {
		for _, entity := range group {
			// Use upsert merge to handle existing entities
			// Convert map to marshalled bytes
			entityBytes, err := json.Marshal(entity)
			if err != nil {
				return g.Error(err, "error marshalling entity")
			}
			_, err = tableClient.UpsertEntity(context.Background(), entityBytes, &aztables.UpsertEntityOptions{
				UpdateMode: aztables.UpdateModeMerge,
			})
			if err != nil {
				return g.Error(err, "error upserting entity")
			}
			successCount++
		}
	}

	return nil
}

func convertToAzureTableProperty(value any, colType iop.ColumnType) any {
	if value == nil {
		return nil
	}

	switch colType {
	case iop.TimestampType, iop.DatetimeType, iop.DateType:
		switch v := value.(type) {
		case time.Time:
			return v.UTC()
		case string:
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				return t.UTC()
			}
			// Try other formats
			for _, format := range []string{
				"2006-01-02 15:04:05",
				"2006-01-02",
			} {
				if t, err := time.Parse(format, v); err == nil {
					return t.UTC()
				}
			}
		}
		return cast.ToString(value)
	case iop.BoolType:
		return cast.ToBool(value)
	case iop.BigIntType, iop.IntegerType:
		return cast.ToInt64(value)
	case iop.FloatType, iop.DecimalType:
		return cast.ToFloat64(value)
	case iop.BinaryType:
		switch v := value.(type) {
		case []byte:
			return v
		case string:
			return []byte(v)
		default:
			return []byte(cast.ToString(value))
		}
	default:
		return cast.ToString(value)
	}
}

// sanitizeAzureTableName ensures the table name conforms to Azure Table Storage naming rules
// Table names must be alphanumeric and 3-63 characters long
func sanitizeAzureTableName(name string) string {
	// Remove all non-alphanumeric characters
	re := regexp.MustCompile("[^a-zA-Z0-9]+")
	sanitized := re.ReplaceAllString(name, "")

	// Ensure it starts with a letter
	if len(sanitized) > 0 && !regexp.MustCompile("^[a-zA-Z]").MatchString(sanitized) {
		sanitized = "t" + sanitized
	}

	// Ensure minimum length
	if len(sanitized) < 3 {
		sanitized = "table" + sanitized
	}

	// Ensure maximum length
	if len(sanitized) > 63 {
		sanitized = sanitized[:63]
	}

	return sanitized
}
