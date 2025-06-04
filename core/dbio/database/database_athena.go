package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// AthenaConn is an Athena connection
type AthenaConn struct {
	BaseConn
	Client *athena.Client
	URL    string
}

// Init initiates the object
func (conn *AthenaConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbAthena

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	for _, key := range g.ArrStr("BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION", "DEFAULT_REGION", "SESSION_TOKEN", "ENDPOINT", "ROLE_ARN", "ROLE_SESSION_NAME", "PROFILE") {
		if conn.GetProp(key) == "" {
			conn.SetProp(key, conn.GetProp("AWS_"+key))
		}
	}

	return conn.BaseConn.Init()
}

func (conn *AthenaConn) getNewClient(timeOut ...int) (client *athena.Client, err error) {
	// Get AWS credentials from connection properties
	awsAccessKeyID := conn.GetProp("access_key_id")
	awsSecretAccessKey := conn.GetProp("secret_access_key")
	awsSessionToken := conn.GetProp("session_token")
	awsRegion := conn.GetProp("region")
	awsProfile := conn.GetProp("profile")

	if awsRegion == "" {
		return nil, g.Error("AWS region not specified")
	}

	ctx := context.Background()
	var cfg aws.Config

	// Configure options based on authentication method
	var configOptions []func(*awsconfig.LoadOptions) error

	// Add region to config options
	configOptions = append(configOptions, awsconfig.WithRegion(awsRegion))

	// Set timeout if provided
	if len(timeOut) > 0 && timeOut[0] > 0 {
		httpClient := &http.Client{
			Timeout: time.Duration(timeOut[0]) * time.Second,
		}
		configOptions = append(configOptions, awsconfig.WithHTTPClient(httpClient))
	}

	// Set credentials if provided
	if awsAccessKeyID != "" && awsSecretAccessKey != "" {
		g.Debug("Athena: Using static credentials (Key ID: %s)", awsAccessKeyID)
		credProvider := credentials.NewStaticCredentialsProvider(
			awsAccessKeyID,
			awsSecretAccessKey,
			awsSessionToken,
		)
		configOptions = append(configOptions, awsconfig.WithCredentialsProvider(credProvider))

		// Load config with static credentials
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return nil, g.Error(err, "Failed to create AWS config with static credentials")
		}
	} else if awsProfile != "" {
		g.Debug("Athena: Using AWS profile=%s region=%s", awsProfile, awsRegion)

		// Use specified profile from AWS credentials file
		configOptions = append(configOptions, awsconfig.WithSharedConfigProfile(awsProfile))

		// Load config with profile
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return nil, g.Error(err, "Failed to create AWS config with profile %s", awsProfile)
		}
	} else {
		g.Debug("Athena: Using default AWS credential chain")
		// Use default credential chain (env vars, IAM role, credential file, etc.)

		// Load config with default credential chain
		cfg, err = awsconfig.LoadDefaultConfig(ctx, configOptions...)
		if err != nil {
			return nil, g.Error(err, "Failed to create AWS config with default credentials")
		}
	}

	// Create and return a new Athena client
	return athena.NewFromConfig(cfg), nil
}

// Connect connects to the database
func (conn *AthenaConn) Connect(timeOut ...int) (err error) {
	if cast.ToBool(conn.GetProp("connected")) {
		return nil
	}

	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	// List available catalogs
	// output, err := conn.Client.ListDataCatalogs(context.Background(), &athena.ListDataCatalogsInput{})
	// if err != nil {
	// 	g.Warn("Could not list data catalogs: %v", err)
	// } else {
	// 	for _, dcs := range output.DataCatalogsSummary {
	// 		g.Warn("Available catalog: %s", g.PtrVal(dcs.CatalogName))
	// 	}
	// }

	// Get workgroup info
	wgOutput, err := conn.Client.GetWorkGroup(context.Background(), &athena.GetWorkGroupInput{WorkGroup: aws.String(conn.GetProp("workgroup"))})
	if err != nil {
		if strings.Contains(err.Error(), "Invalid refresh token provided") {
			return g.Error("could not connect. Please renew your session.\n%s", err.Error())
		} else {
			g.Warn("Could not get workgroup details: %v", err)
		}
	} else if wgOutput.WorkGroup != nil {
		g.Trace("connected to Athena. workgroup=%s catalog=%s", conn.GetProp("workgroup"), conn.GetProp("catalog"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	return nil
}

// NewTransaction creates a new transaction
func (conn *AthenaConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// Athena does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}
	// conn.tx = Tx

	return nil, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *AthenaConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	// Athena doesn't support temporary tables
	if temporary {
		temporary = false
	}

	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	// Add Athena-specific DDL modifications
	makeNativeType := func(col *iop.Column) (nativeType string) {
		switch col.Type {
		case iop.StringType, iop.TextType, iop.UUIDType:
			nativeType = "string"
		case iop.IntegerType, iop.SmallIntType, iop.BigIntType:
			nativeType = "int"
		case iop.FloatType:
			nativeType = "float"
		case iop.DecimalType:
			nativeType = "decimal(38,9)"
		case iop.DateType:
			nativeType = "date"
		case iop.TimestampType, iop.DatetimeType, iop.TimestampzType:
			nativeType = "timestamp"
		case iop.BoolType:
			nativeType = "boolean"
		default:
			nativeType = "string"
		}
		return
	}

	// Process partitioning - Athena uses Hive-style PARTITIONED BY
	partitionBy := ""
	partitionCols := []string{}
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// Use custom partition clause if defined
		for _, key := range keys {
			if col := data.Columns.GetColumn(key); col != nil {
				// For partitioning, we need both column name and type
				partitionCols = append(partitionCols, fmt.Sprintf("%s %s",
					conn.GetType().Quote(col.Name), makeNativeType(col)))
			}
		}
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		// Get columns marked as partition keys
		for _, col := range keyCols {
			// For partitioning, we need both column name and type
			partitionCols = append(partitionCols, fmt.Sprintf("%s %s",
				conn.GetType().Quote(col.Name), makeNativeType(&col)))
		}
	}
	if len(partitionCols) > 0 {
		partitionBy = fmt.Sprintf("partitioned by (%s)", strings.Join(partitionCols, ", "))
	}

	// Process bucketing/clustering if needed
	bucketBy := ""
	if keyCols := data.Columns.GetKeys(iop.ClusterKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		// Default to 10 buckets, but make it configurable
		numBuckets := 10
		if bucketCount := cast.ToInt(conn.GetProp("bucket_count")); bucketCount > 0 {
			numBuckets = bucketCount
		}
		bucketBy = fmt.Sprintf("CLUSTERED BY (%s) INTO %d BUCKETS",
			strings.Join(colNames, ", "), numBuckets)
	}

	// Process location if specified
	location := ""
	if loc := conn.GetProp("table_location"); loc != "" {
		location = fmt.Sprintf("LOCATION '%s'", loc)
	} else if loc := conn.GetProp("AWS_S3_LOCATION"); loc != "" {
		tablePath := strings.ToLower(strings.ReplaceAll(table.Name, ".", "/"))
		location = fmt.Sprintf("LOCATION '%s/%s'", loc, tablePath)
	}

	// Handle format and other properties
	tableProperties := []string{}

	// Default file format
	format := "PARQUET"
	if fmt := conn.GetProp("file_format"); fmt != "" {
		format = strings.ToUpper(fmt)
	}

	// Add all modifications to the SQL
	ddlParts := []string{}
	if partitionBy != "" {
		ddlParts = append(ddlParts, partitionBy)
	}
	if bucketBy != "" {
		ddlParts = append(ddlParts, bucketBy)
	}

	// Add row format if needed
	if format != "" {
		ddlParts = append(ddlParts, fmt.Sprintf("STORED AS %s", format))
	}

	if location != "" {
		ddlParts = append(ddlParts, location)
	}

	// Add table properties if specified
	if len(tableProperties) > 0 {
		propString := fmt.Sprintf("TBLPROPERTIES (%s)", strings.Join(tableProperties, ", "))
		ddlParts = append(ddlParts, propString)
	}

	// Append the modifications to the SQL
	if len(ddlParts) > 0 {
		// Make sure we have a closing parenthesis before adding clauses
		if !strings.Contains(sql, ")") {
			sql += ")"
		}

		// Find the last closing parenthesis and add our clauses after it
		lastParenPos := strings.LastIndex(sql, ")")
		if lastParenPos > 0 {
			sql = sql[:lastParenPos+1] + " " + strings.Join(ddlParts, " ") + sql[lastParenPos+1:]
		} else {
			// Just append if we can't find the closing parenthesis
			sql += " " + strings.Join(ddlParts, " ")
		}
	}

	return strings.TrimSpace(sql), nil
}

// Define a type that implements sql.Result for Athena
type athenaResult struct {
	rowsAffected int64
}

func (r athenaResult) LastInsertId() (int64, error) {
	// Athena doesn't support LastInsertId
	return 0, nil
}

func (r athenaResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// ExecContext executes the sql query
func (conn *AthenaConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	if strings.TrimSpace(sql) == "" {
		g.Warn("Empty Query")
		return
	}

	// Handle args if provided - replace placeholders
	if len(args) > 0 {
		for _, arg := range args {
			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%d", val), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%f", val), 1)
			case time.Time:
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			case []byte:
				if len(val) == 0 {
					sql = strings.Replace(sql, "?", "NULL", 1)
				} else {
					sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", string(val)), 1)
				}
			default:
				v := strings.ReplaceAll(cast.ToString(val), "\n", "\\n")
				v = strings.ReplaceAll(v, "'", "\\'")
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", v), 1)
			}
		}
	}

	conn.LogSQL(sql)

	// Create a struct to return rows affected
	result = athenaResult{}

	// Start the query
	startQueryInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(conn.GetProp("database")),
			Catalog:  aws.String(conn.GetProp("catalog")),
		},
		WorkGroup: aws.String(conn.GetProp("workgroup")),
	}

	// Set output location if provided
	if outputLocation := conn.GetProp("output_location"); outputLocation != "" {
		startQueryInput.ResultConfiguration = &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		}
	}

	// Execute the query
	resp, err := conn.Client.StartQueryExecution(ctx, startQueryInput)
	if err != nil {
		if strings.Contains(sql, noDebugKey) {
			err = g.Error(err, "Error executing query")
		} else {
			err = g.Error(err, "Error executing %s", env.Clean(conn.Props(), sql))
		}
		return nil, err
	}

	queryID := *resp.QueryExecutionId

	// Poll for query completion
	var queryExecution *athena.GetQueryExecutionOutput
	for {
		queryExecution, err = conn.Client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return nil, g.Error(err, "Failed to get query execution status")
		}

		state := queryExecution.QueryExecution.Status.State
		if state == types.QueryExecutionStateSucceeded {
			// Query succeeded - for non-select statements, we're done
			if queryExecution.QueryExecution.Statistics != nil {
				// athenaResult.RowsAffected = *queryExecution.QueryExecution.Statistics.DataManipulation.AffectedRows
			}
			break
		} else if state == types.QueryExecutionStateFailed ||
			state == types.QueryExecutionStateCancelled {
			errorMessage := ""
			if queryExecution.QueryExecution.Status.StateChangeReason != nil {
				errorMessage = *queryExecution.QueryExecution.Status.StateChangeReason
			}
			return nil, g.Error("Query execution failed: %s", errorMessage)
		}

		// Wait before polling again
		select {
		case <-ctx.Done():
			// Context cancelled
			conn.Client.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			return nil, g.Error("Query execution cancelled by context")
		case <-time.After(time.Second):
			// Continue polling
		}
	}

	return result, nil
}

// StreamRowsContext executes the query and streams the result into a datastream
func (conn *AthenaConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	opts := getQueryOptions(options)
	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val
	}

	Limit := uint64(0) // infinite
	if val := cast.ToUint64(opts["limit"]); val > 0 {
		Limit = val
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)
	conn.LogSQL(sql)

	// Start the query
	startQueryInput := &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(conn.GetProp("database")),
			Catalog:  aws.String(conn.GetProp("catalog")),
		},
		WorkGroup: aws.String(conn.GetProp("workgroup")),
	}

	// Set output location if provided
	if outputLocation := conn.GetProp("output_location"); outputLocation != "" {
		startQueryInput.ResultConfiguration = &types.ResultConfiguration{
			OutputLocation: aws.String(outputLocation),
		}
	}

	// Execute the query
	resp, err := conn.Client.StartQueryExecution(ctx, startQueryInput)
	if err != nil {
		if strings.Contains(sql, noDebugKey) {
			err = g.Error(err, "Error executing query")
		} else {
			err = g.Error(err, "Error executing %s", env.Clean(conn.Props(), sql))
		}
		return nil, err
	}

	queryID := *resp.QueryExecutionId

	// Poll for query completion
	g.Trace("athena query id: %s", queryID)
	var queryExecution *athena.GetQueryExecutionOutput
	for {
		queryExecution, err = conn.Client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return nil, g.Error(err, "Failed to get query execution status")
		}

		state := queryExecution.QueryExecution.Status.State
		if state == types.QueryExecutionStateSucceeded {
			// Query succeeded
			break
		} else if state == types.QueryExecutionStateFailed ||
			state == types.QueryExecutionStateCancelled {
			errorMessage := ""
			if queryExecution.QueryExecution.Status.StateChangeReason != nil {
				errorMessage = *queryExecution.QueryExecution.Status.StateChangeReason
			}
			return nil, g.Error("Query execution failed: %s", errorMessage)
		}

		// Wait before polling again
		select {
		case <-ctx.Done():
			// Context cancelled
			conn.Client.StopQueryExecution(ctx, &athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			return nil, g.Error("Query execution cancelled by context")
		case <-time.After(time.Second):
			// Continue polling
		}
	}

	// Get query results
	var queryResults *athena.GetQueryResultsOutput
	queryResults, err = conn.Client.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(queryID),
	})
	if err != nil {
		return nil, g.Error(err, "Failed to get query results")
	}
	// g.Warn(g.Marshal(queryResults))

	// Process column information
	if len(queryResults.ResultSet.ResultSetMetadata.ColumnInfo) > 0 {
		columnInfo := queryResults.ResultSet.ResultSetMetadata.ColumnInfo
		fetchedColumns = make(iop.Columns, len(columnInfo))

		for i, colInfo := range columnInfo {
			colType := iop.StringType // Default type
			dbType := *colInfo.Type

			// Map Athena types to corresponding iop types
			switch strings.ToLower(dbType) {
			case "integer", "int", "smallint", "tinyint":
				colType = iop.IntegerType
			case "bigint":
				colType = iop.BigIntType
			case "boolean":
				colType = iop.BoolType
			case "double", "float":
				colType = iop.FloatType
			case "decimal":
				colType = iop.DecimalType
			case "date":
				colType = iop.DateType
			case "timestamp":
				colType = iop.TimestampType
			case "binary", "varbinary":
				colType = iop.BinaryType
			case "array", "map", "struct", "json":
				colType = iop.JsonType
			}

			fetchedColumns[i] = iop.Column{
				Name:        *colInfo.Name,
				Type:        colType,
				Position:    i + 1,
				DbType:      dbType,
				DbPrecision: int(colInfo.Precision),
				DbScale:     int(colInfo.Scale),
				Sourced:     true,
			}
		}
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.NoDebug = !strings.Contains(sql, noDebugKey)

	// Create a function to fetch next row
	makeNextFunc := func() (nextFunc func(it *iop.Iterator) bool) {
		var tokenForNextPage *string = queryResults.NextToken
		var moreRows = true

		// first row is the column name, not sure why
		if rows := queryResults.ResultSet.Rows; len(rows) > 0 && len(rows[0].Data) > 0 && g.PtrVal(rows[0].Data[0].VarCharValue) == fetchedColumns[0].Name {
			queryResults.ResultSet.Rows = queryResults.ResultSet.Rows[1:]
		}

		return func(it *iop.Iterator) bool {
			if Limit > 0 && it.Counter >= Limit {
				return false
			}

			if !moreRows {
				return false
			}

			// If we don't have any rows left in the current page, fetch next page
			if len(queryResults.ResultSet.Rows) == 0 {
				if tokenForNextPage == nil {
					return false
				}

				// Except for the first row that we already fetched
				if it.Counter > 0 || len(queryResults.ResultSet.Rows) == 0 {
					input := &athena.GetQueryResultsInput{
						QueryExecutionId: aws.String(queryID),
						NextToken:        tokenForNextPage,
					}

					var err error
					g.Trace("getting next page with token: %s (queryID: %s)", g.PtrVal(tokenForNextPage), queryID)
					queryResults, err = conn.Client.GetQueryResults(queryContext.Ctx, input)
					if err != nil {
						queryContext.CaptureErr(g.Error(err, "Error getting next page of results"))
						return false
					}
					g.Trace("got queryResults %d rows (queryID: %s)", len(queryResults.ResultSet.Rows), queryID)

					// Skip header row if present
					if len(queryResults.ResultSet.Rows) > 0 {
						// Check if the first row contains column names
						firstRow := queryResults.ResultSet.Rows[0]
						isHeader := true
						for i, val := range firstRow.Data {
							if i < len(fetchedColumns) && (val.VarCharValue == nil || *val.VarCharValue != fetchedColumns[i].Name) {
								isHeader = false
								break
							}
						}

						if isHeader {
							queryResults.ResultSet.Rows = queryResults.ResultSet.Rows[1:]
						}
					}
				}

				// Update token for next page
				tokenForNextPage = queryResults.NextToken

				// If no more rows and no token for next page, we're done
				if len(queryResults.ResultSet.Rows) == 0 {
					if tokenForNextPage == nil {
						moreRows = false
					}
					return moreRows
				}
			}

			// Get the current row and advance
			row := queryResults.ResultSet.Rows[0]
			queryResults.ResultSet.Rows = queryResults.ResultSet.Rows[1:]

			// Convert the row data to the right format
			it.Row = make([]interface{}, len(row.Data))
			for i, cell := range row.Data {
				if cell.VarCharValue == nil {
					it.Row[i] = nil
					continue
				}

				// Convert based on column type
				value := g.PtrVal(cell.VarCharValue)
				if i < len(fetchedColumns) {
					switch fetchedColumns[i].Type {
					case iop.IntegerType, iop.BigIntType, iop.SmallIntType:
						it.Row[i], err = cast.ToInt64E(value)
					case iop.FloatType, iop.DecimalType:
						it.Row[i], err = cast.ToFloat64E(value)
					case iop.BoolType:
						it.Row[i], err = cast.ToBoolE(value)
					case iop.DateType, iop.DatetimeType, iop.TimestampType, iop.TimestampzType:
						it.Row[i], err = it.Ds().Sp.ParseTime(value)
					case iop.JsonType:
						it.Row[i] = value // Keep as string for JSON
					default:
						it.Row[i] = value
					}
					if err != nil {
						it.Row[i] = value
					}
				} else {
					it.Row[i] = value
				}
			}

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

// InsertBatchStream inserts a stream into a table in batch
func (conn *AthenaConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a Athena table using a file on the local filesystem.
func (conn *AthenaConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// Close closes the connection
func (conn *AthenaConn) Close() error {
	if conn.Client == nil {
		return nil
	}

	// no close method
	conn.Client = nil
	return conn.BaseConn.Close()
}

// Unload exports the sql query into an S3 bucket
func (conn *AthenaConn) Unload(ctx *g.Context, tables ...Table) (s3Path string, err error) {
	awsBucket := conn.GetProp("AWS_BUCKET")
	if awsBucket == "" {
		return "", g.Error("AWS_BUCKET must be set to unload data")
	}

	outputLocation := conn.GetProp("output_location")
	if outputLocation == "" {
		outputLocation = fmt.Sprintf("s3://%s/%s/", awsBucket, tempCloudStorageFolder)
		conn.SetProp("output_location", outputLocation)
	}

	if len(tables) == 0 {
		return "", g.Error("No tables provided for Unload")
	}

	table := tables[0]
	s3Path = fmt.Sprintf("%s%s_%s/",
		outputLocation,
		strings.ReplaceAll(table.Name, ".", "_"),
		time.Now().Format("20060102_150405"))

	// Build the SQL query for UNLOAD
	unloadSQL := fmt.Sprintf(`
	UNLOAD (%s)
	TO '%s' 
	WITH (
		format = 'CSV',
		field_delimiter = ',',
		compression = 'GZIP',
		header = true
	)
	`, table.Select(), s3Path)

	// Execute the UNLOAD query
	_, err = conn.ExecContext(ctx.Ctx, unloadSQL)
	if err != nil {
		return "", g.Error(err, "Failed to execute UNLOAD statement")
	}

	return s3Path, nil
}

// BulkExportStream reads in bulk
func (conn *AthenaConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {

	df, err := conn.BulkExportFlow(table)
	if err != nil {
		return ds, g.Error(err, "Could not export")
	}

	return iop.MergeDataflow(df), nil
}

// BulkExportFlow reads in bulk
func (conn *AthenaConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	if conn.GetProp("AWS_BUCKET") == "" {
		g.Warn("using cursor to export. Please set AWS creds for Sling to use the Athena UNLOAD function (for bigger datasets).")
		return conn.BaseConn.BulkExportFlow(table)
	}

	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	unloadCtx := g.NewContext(conn.Context().Ctx)
	s3Path, err := conn.Unload(unloadCtx, table)
	if err != nil {
		err = g.Error(err, "Could not unload.")
		return
	}

	fs, err := filesys.NewFileSysClientContext(unloadCtx.Ctx, dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	// set column coercion if specified
	if coerceCols, ok := getColumnsProp(conn); ok {
		cc, _ := getColumnCasingProp(conn)
		tgtType := dbio.Type(conn.GetProp("target_type"))
		columns.Coerce(coerceCols, true, cc, tgtType)
	}

	fs.SetProp("format", "csv")
	fs.SetProp("delimiter", ",")
	fs.SetProp("header", "true")
	fs.SetProp("null_if", `\N`)
	fs.SetProp("columns", g.Marshal(columns))
	fs.SetProp("metadata", conn.GetProp("metadata"))

	df, err = fs.ReadDataflow(s3Path)
	if err != nil {
		err = g.Error(err, "Could not read S3 Path for UNLOAD: "+s3Path)
		return
	}

	df.MergeColumns(columns, true) // overwrite types so we don't need to infer
	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(fs, s3Path)
		}
	})

	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *AthenaConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	settingMppBulkImportFlow(conn, iop.GzipCompressorType)
	if conn.GetProp("AWS_BUCKET") == "" {
		return count, g.Error("Need to set 'AWS_BUCKET' to copy to redshift")
	}

	s3Path := fmt.Sprintf(
		"s3://%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		tempCloudStorageFolder,
		tableFName,
	)

	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(s3Fs, s3Path)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+s3Path)
	}

	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(s3Fs, s3Path)
		}
	}) // cleanup

	g.Info("writing to s3 for redshift import")
	s3Fs.SetProp("null_as", `\N`)
	bw, err := filesys.WriteDataflow(s3Fs, df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "error writing to s3")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	_, err = conn.CopyFromS3(tableFName, s3Path, df.Columns)
	if err != nil {
		return df.Count(), g.Error(err, "error copying into redshift from s3")
	}

	return df.Count(), nil
}

// BulkImportStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *AthenaConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *AthenaConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	srcTgtPkEqual := strings.ReplaceAll(
		upsertMap["src_tgt_pk_equal"], "src.", srcTable+".",
	)
	srcTgtPkEqual = strings.ReplaceAll(
		srcTgtPkEqual, "tgt.", tgtTable+".",
	)

	// Athena doesn't support MERGE statement
	// Instead we'll use DELETE + INSERT pattern
	sqlTempl := `
	DELETE FROM {tgt_table}
	WHERE EXISTS (
		SELECT 1
		FROM {src_table}
		WHERE {src_tgt_pk_equal}
	);

	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table}
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"src_tgt_pk_equal", srcTgtPkEqual,
	)
	return
}

// CopyFromS3 uses the COPY INTO Table command from AWS S3
func (conn *AthenaConn) CopyFromS3(tableFName, s3Path string, columns iop.Columns) (count uint64, err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	AwsSessionToken := conn.GetProp("AWS_SESSION_TOKEN")
	if (AwsID == "" || AwsAccessKey == "") && (AwsSessionToken == "") {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' or 'AWS_SESSION_TOKEN' to copy to redshift from S3")
		return
	}

	AwsSessionTokenExpr := ""
	if AwsSessionToken != "" {
		AwsSessionTokenExpr = g.F(";token=%s", AwsSessionToken)
	}

	tgtColumns := conn.GetType().QuoteNames(columns.Names()...)

	g.Debug("copying into redshift from s3")
	g.Debug("url: " + s3Path)
	sql := g.R(
		conn.template.Core["copy_from_s3"],
		"tgt_table", tableFName,
		"tgt_columns", strings.Join(tgtColumns, ", "),
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
		"aws_session_token_expr", AwsSessionTokenExpr,
	)
	sql = conn.setEmptyAsNull(sql)

	_, err = conn.Exec(sql)
	if err != nil {
		return 0, g.Error(err)
	}

	return 0, nil
}
func (conn *AthenaConn) setEmptyAsNull(sql string) string {
	if !cast.ToBool(conn.GetProp("empty_as_null")) {
		sql = strings.ReplaceAll(sql, " EMPTYASNULL BLANKSASNULL", "")
	}
	return sql
}

// CastColumnForSelect casts to the correct target column type
func (conn *AthenaConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.IsString() && tgtCol.IsInteger():
		selectStr = g.F("cast(%s as bigint)", qName)
	case srcCol.IsString() && tgtCol.IsFloat():
		selectStr = g.F("cast(%s as double)", qName)
	case srcCol.IsString() && tgtCol.IsDecimal():
		// Use appropriate precision and scale if available
		precision := 38 // Default max precision for Athena
		scale := 18     // Default scale
		if tgtCol.DbPrecision > 0 {
			precision = tgtCol.DbPrecision
		}
		if tgtCol.DbScale > 0 {
			scale = tgtCol.DbScale
		}
		selectStr = g.F("cast(%s as decimal(%d,%d))", qName, precision, scale)
	case srcCol.IsString() && tgtCol.IsDate():
		selectStr = g.F("date(%s)", qName)
	case srcCol.IsString() && (tgtCol.IsDatetime()):
		selectStr = g.F("timestamp(%s)", qName)
	case srcCol.IsString() && tgtCol.IsBool():
		selectStr = g.F("cast(%s as boolean)", qName)
	case srcCol.IsNumber() && tgtCol.IsString():
		selectStr = g.F("cast(%s as varchar)", qName)
	case (srcCol.IsDate() || srcCol.IsDatetime()) && tgtCol.IsString():
		selectStr = g.F("cast(%s as varchar)", qName)
	case srcCol.IsDatetime() && tgtCol.IsDatetime():
		selectStr = g.F("timestamp(%s)", qName)
	case srcCol.IsDatetime() && tgtCol.IsDatetime():
		selectStr = g.F("timestamp(%s)", qName)
	case srcCol.IsDate() && (tgtCol.IsDatetime()):
		selectStr = g.F("timestamp(%s)", qName)
	case (srcCol.IsDatetime()) && tgtCol.IsDate():
		selectStr = g.F("date(%s)", qName)
	default:
		selectStr = qName
	}

	return selectStr
}

// GetDatabases returns databases for given connection
func (conn *AthenaConn) GetDatabases() (iop.Dataset, error) {
	// fields: [name]
	data := iop.NewDataset(iop.NewColumnsFromFields("name"))
	data.Rows = append(data.Rows, []any{conn.GetProp("catalog")})
	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *AthenaConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
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

	// We'll keep track of the current database - for Athena this is the catalog
	currDatabase := conn.GetProp("catalog")

	getOneSchemata := func(values map[string]interface{}) error {
		defer ctx.Wg.Read.Done()

		var data iop.Dataset
		switch level {
		case SchemataLevelSchema:
			data.Columns = iop.NewColumnsFromFields("schema_name")
			data.Append([]any{values["schema"]})
		case SchemataLevelTable:
			data, err = conn.GetTablesAndViews(schemaName)
		case SchemataLevelColumn:
			data, err = conn.SubmitTemplate(
				"single", conn.template.Metadata, "schemata",
				values,
			)
		}
		if err != nil {
			if strings.Contains(err.Error(), "TYPE_NOT_FOUND") || strings.Contains(err.Error(), "GENERIC_INTERNAL_ERROR") || strings.Contains(err.Error(), "AccessDenied") {
				g.Warn(g.ErrMsg(err))
				err = nil
			} else {
				return g.Error(err, "Could not get schemata at %s level for %s", level, g.Marshal(values))
			}
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range data.Records() {
			schemaName = cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))
			dataType = strings.Split(dataType, "(")[0]
			dataType = strings.Split(dataType, "<")[0]

			// if any of the names contains a period, skip. This messes with the keys
			if strings.Contains(tableName, ".") ||
				strings.Contains(schemaName, ".") ||
				strings.Contains(columnName, ".") {
				continue
			}

			switch v := rec["is_view"].(type) {
			case int64, float64:
				if cast.ToInt64(rec["is_view"]) == 0 {
					rec["is_view"] = false
				} else {
					rec["is_view"] = true
				}
			case string:
				if cast.ToBool(rec["is_view"]) {
					rec["is_view"] = true
				} else {
					rec["is_view"] = false
				}

			default:
				_ = fmt.Sprint(v)
				_ = rec["is_view"]
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
					Dialect:  dbio.TypeDbBigQuery,
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
					Position: cast.ToInt(data.Sp.ProcessVal(rec["position"])),
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
		g.Debug("getting schemata for %s", schemaName)
		values := g.M("schema", schemaName)

		if len(tableNames) > 0 && !(tableNames[0] == "" && len(tableNames) == 1) {
			tablesQ := []string{}
			for _, tableName := range tableNames {
				if strings.TrimSpace(tableName) == "" {
					continue
				}
				tablesQ = append(tablesQ, `'`+tableName+`'`)
			}
			if len(tablesQ) > 0 {
				values["tables"] = strings.Join(tablesQ, ", ")
			}
		}

		ctx.Wg.Read.Add()
		go func(values map[string]interface{}) {
			err := getOneSchemata(values)
			ctx.CaptureErr(err)
		}(values)
	}

	ctx.Wg.Read.Wait()

	if err := ctx.Err(); err != nil {
		return schemata, g.Error(err)
	}

	return schemata, nil
}
