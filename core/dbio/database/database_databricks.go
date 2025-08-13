package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// DatabricksConn is a Databricks connection
type DatabricksConn struct {
	BaseConn
	URL        string
	Catalog    string
	Schema     string
	Warehouse  string
	CopyMethod string
}

// Init initiates the object
func (conn *DatabricksConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDatabricks
	conn.CopyMethod = "stage"

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.Schema = conn.GetProp("schema")

	if m := conn.GetProp("copy_method"); m != "" {
		conn.CopyMethod = strings.ToLower(conn.GetProp("copy_method"))
	}

	if w := conn.GetProp("warehouse"); w != "" {
		conn.Warehouse = w
	}

	// disable internal log
	dbsqllog.SetLogLevel("disabled")

	return conn.BaseConn.Init()
}

// NewTransaction creates a new transaction
func (conn *DatabricksConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// context := g.NewContext(ctx)

	// _, err = conn.ExecContext(ctx, "BEGIN")
	// if err != nil {
	// 	return nil, g.Error(err, "could not begin Tx")
	// }

	// does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}
	// conn.tx = Tx

	return nil, nil
}

// GetURL returns the URL
func (conn *DatabricksConn) GetURL(newURL ...string) string {
	connURL := strings.TrimPrefix(conn.BaseConn.URL, "databricks://")

	u, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		g.LogError(err, "could not parse databricks URL")
		return connURL
	}

	paramKeyMapping := map[string]string{
		// Connection parameters as per Databricks Go SQL driver docs
		"catalog":          "catalog",        // Sets the initial catalog name in the session
		"schema":           "schema",         // Sets the initial schema name in the session
		"max_rows":         "maxRows",        // Maximum number of rows fetched per request (default: 10000)
		"timeout":          "timeout",        // Timeout in seconds for server query execution (default: no timeout)
		"user_agent_entry": "userAgentEntry", // Used to identify partners

		// Session parameters
		"ansi_mode": "ansi_mode", // Boolean for ANSI SQL specification adherence (default: false)
		"timezone":  "timezone",  // Timezone setting (default: UTC)
	}

	for key, libKey := range paramKeyMapping {
		if val := conn.GetProp(key); val != "" {
			u.SetParam(libKey, val)
		}
		if libKey != key {
			if val := conn.GetProp(libKey); val != "" {
				u.SetParam(libKey, val)
			}
		}
	}

	// reconstruct the url
	// https://docs.databricks.com/aws/en/dev-tools/go-sql-driver
	// token:<personal-access-token>@<server-hostname>:<port-number>/<http-path>?<param1=value1>&<param2=value2>
	// token:<access-token>@xxxxxxxxx.cloud.databricks.com/sql/1.0/warehouses/xxxxxxxx
	return strings.TrimPrefix(u.String(), "databricks://")
}

// Connect connects to the database
func (conn *DatabricksConn) Connect(timeOut ...int) error {
	err := conn.BaseConn.Connect(60)
	if err != nil {
		return err
	}

	// get current catalog
	data, err := conn.Query("select current_catalog()" + env.NoDebugKey)
	if err != nil {
		g.Warn("could not get catalog: %s", err.Error())
	} else {
		conn.Catalog = cast.ToString(data.Rows[0][0])
	}

	// get current schema
	if conn.Schema == "" {
		data, err = conn.Query("select current_schema()" + env.NoDebugKey)
		if err != nil {
			g.Warn("could not get schema: %s", err.Error())
		} else {
			conn.Schema = cast.ToString(data.Rows[0][0])
		}
	} else {
		_, err = conn.Exec("USE " + conn.Schema)
		if err != nil {
			g.Warn("could not set schema: %v", err)
		}
	}

	return nil
}

// BulkImportFlow inserts a flow of streams into a table
func (conn *DatabricksConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)

	if conn.GetProp("use_bulk") == "false" {
		// Fall back to base implementation which does batch inserts
		return conn.BaseConn.BulkImportFlow(tableFName, df)
	}

	switch conn.CopyMethod {
	case "aws":
		return conn.CopyViaS3(tableFName, df)
	}

	// Try volume-based loading as fallback
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: "+tableFName)
	}

	volume, err := conn.getOrCreateVolume(table.Schema)
	if err != nil {
		return 0, g.Error(err, "could not create volume")
	} else if volume != "" {
		return conn.CopyViaVolume(table, df)
	}

	if err == nil && volume == "" {
		g.Warn("Could not create internal volume, falling back to batch inserts")
	}
	return conn.BaseConn.BulkImportFlow(tableFName, df)
}

// ensureAWSSessionToken ensures we have the required AWS credentials, generating session tokens if needed
func (conn *DatabricksConn) ensureAWSSessionToken() error {
	awsAccessKey := conn.GetProp("AWS_ACCESS_KEY_ID")
	awsSecretKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	awsSessionToken := conn.GetProp("AWS_SESSION_TOKEN")

	// If we already have all three credentials, we're good
	if awsAccessKey != "" && awsSecretKey != "" && awsSessionToken != "" {
		return nil
	}

	// If we have long-term credentials but no session token, try to generate one using STS
	if awsAccessKey != "" && awsSecretKey != "" && awsSessionToken == "" {
		return conn.generateSessionToken()
	}

	// Try to use default AWS credential chain (IAM roles, profiles, etc.)
	if awsAccessKey == "" || awsSecretKey == "" {
		return conn.loadAWSCredentialsFromChain()
	}

	return nil
}

// generateSessionToken creates temporary credentials using AWS STS
func (conn *DatabricksConn) generateSessionToken() error {
	awsAccessKey := conn.GetProp("AWS_ACCESS_KEY_ID")
	awsSecretKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	region := conn.GetProp("AWS_REGION")
	if region == "" {
		region = conn.GetProp("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1" // Default region
	}

	// Create AWS config with static credentials
	cfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			awsAccessKey,
			awsSecretKey,
			"", // no session token for the static creds
		),
	}

	// Create STS client
	stsClient := sts.NewFromConfig(cfg)

	// Get session token (valid for 12 hours by default)
	sessionDuration := int32(43200) // 12 hours in seconds
	if durationStr := conn.GetProp("aws_session_duration"); durationStr != "" {
		if duration := cast.ToInt32(durationStr); duration > 0 {
			sessionDuration = duration
		}
	}

	input := &sts.GetSessionTokenInput{
		DurationSeconds: &sessionDuration,
	}

	result, err := stsClient.GetSessionToken(context.Background(), input)
	if err != nil {
		return g.Error(err, "Failed to generate AWS session token using STS")
	}

	// Set the temporary credentials - MUST use all three together
	conn.SetProp("AWS_ACCESS_KEY_ID", *result.Credentials.AccessKeyId)
	conn.SetProp("AWS_SECRET_ACCESS_KEY", *result.Credentials.SecretAccessKey)
	conn.SetProp("AWS_SESSION_TOKEN", *result.Credentials.SessionToken)

	g.Debug("successfully generated AWS session token, expires at: %v", result.Credentials.Expiration)
	return nil
}

// loadAWSCredentialsFromChain attempts to load credentials using the default AWS credential chain
func (conn *DatabricksConn) loadAWSCredentialsFromChain() error {
	g.Debug("Loading AWS credentials from default credential chain")

	// Load default AWS config (will use IAM roles, profiles, env vars, etc.)
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return g.Error(err, "Failed to load AWS configuration from credential chain")
	}

	// Get credentials
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return g.Error(err, "Failed to retrieve AWS credentials from credential chain")
	}

	// Set the credentials
	conn.SetProp("AWS_ACCESS_KEY_ID", creds.AccessKeyID)
	conn.SetProp("AWS_SECRET_ACCESS_KEY", creds.SecretAccessKey)
	if creds.SessionToken != "" {
		conn.SetProp("AWS_SESSION_TOKEN", creds.SessionToken)
	}

	// Set region if not already set
	if conn.GetProp("AWS_REGION") == "" && cfg.Region != "" {
		conn.SetProp("AWS_REGION", cfg.Region)
	}

	g.Debug("Successfully loaded AWS credentials from credential chain")
	return nil
}

// CopyViaS3 uses the Databricks COPY INTO command from AWS S3
func (conn *DatabricksConn) CopyViaS3(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)

	if conn.GetProp("AWS_BUCKET") == "" {
		err = g.Error("Need to set 'AWS_BUCKET' to copy to Databricks from S3")
		return
	}

	// Ensure we have a session token if using credentials
	err = conn.ensureAWSSessionToken()
	if err != nil {
		return 0, g.Error(err, "could not ensure AWS session token")
	}

	s3Path := fmt.Sprintf(
		"s3://%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		tempCloudStorageFolder,
		strings.ReplaceAll(tableFName, "`", ""),
	)

	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArrExclude("url")...)
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

	g.Info("writing to s3 for databricks import")

	// Set optimal format for Databricks - prefer parquet over CSV for better performance
	format := "parquet"
	if conn.GetProp("s3_file_format") != "" {
		format = conn.GetProp("s3_file_format")
	}

	if format == "parquet" {
		s3Fs.SetProp("format", "parquet")
		s3Fs.SetProp("compression", "snappy")
	} else {
		s3Fs.SetProp("format", "csv")
		s3Fs.SetProp("delimiter", ",")
		s3Fs.SetProp("header", "true")
		s3Fs.SetProp("null_as", "")
	}

	bw, err := filesys.WriteDataflow(s3Fs, df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	return df.Count(), conn.CopyFromS3(tableFName, s3Path, format)
}

// CopyFromS3 uses the Databricks COPY INTO command from AWS S3
func (conn *DatabricksConn) CopyFromS3(tableFName, s3Path, fileFormat string) (err error) {
	awsAccessKey := conn.GetProp("AWS_ACCESS_KEY_ID")
	awsSecretKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	awsSessionToken := conn.GetProp("AWS_SESSION_TOKEN")

	var sql string
	fileFormatUpper := strings.ToUpper(fileFormat)

	if awsAccessKey != "" && awsSecretKey != "" {
		// Use credentials in the COPY command
		if awsSessionToken != "" {
			sql = g.R(
				conn.template.Core["copy_from_s3_with_creds_and_token"],
				"table", tableFName,
				"s3_path", s3Path,
				"file_format", fileFormatUpper,
				"aws_access_key_id", awsAccessKey,
				"aws_secret_access_key", awsSecretKey,
				"aws_session_token", awsSessionToken,
			)
		} else {
			sql = g.R(
				conn.template.Core["copy_from_s3_with_creds"],
				"table", tableFName,
				"s3_path", s3Path,
				"file_format", fileFormatUpper,
				"aws_access_key_id", awsAccessKey,
				"aws_secret_access_key", awsSecretKey,
			)
		}
	} else {
		// Use IAM role or instance profile (credentials should be configured at cluster level)
		sql = g.R(
			conn.template.Core["copy_from_s3"],
			"table", tableFName,
			"s3_path", s3Path,
			"file_format", fileFormatUpper,
		)
	}

	g.Info("copying into databricks from s3")
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}

	return nil
}

// CopyToS3 exports a query to an S3 location using CREATE EXTERNAL TABLE
func (conn *DatabricksConn) CopyToS3(table Table) (s3Path string, err error) {
	if conn.GetProp("AWS_BUCKET") == "" {
		err = g.Error("Need to set 'AWS_BUCKET' to copy from Databricks to S3")
		return
	}

	// Ensure we have a session token if using credentials
	err = conn.ensureAWSSessionToken()
	if err != nil {
		return "", g.Error(err, "could not ensure AWS session token")
	}

	awsAccessKey := conn.GetProp("AWS_ACCESS_KEY_ID")
	awsSecretKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	awsSessionToken := conn.GetProp("AWS_SESSION_TOKEN")

	// Generate unique S3 path for export
	s3Path = fmt.Sprintf(
		"s3://%s/%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		tempCloudStorageFolder,
		strings.ReplaceAll(table.FullName(), "`", ""),
		cast.ToString(g.Now()),
	)

	// Set optimal format for Databricks export - prefer parquet over CSV for better performance
	format := "PARQUET"
	if conn.GetProp("s3_file_format") != "" {
		format = strings.ToUpper(conn.GetProp("s3_file_format"))
	}

	// Create S3 filesystem client to clean up any existing files
	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArrExclude("url")...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(s3Fs, s3Path)
	if err != nil {
		g.Warn("Could not delete existing files at %s: %v", s3Path, err)
	}

	// Generate a unique temporary table name for the export
	tempTableName := fmt.Sprintf("sling_export_%s_%d",
		strings.ReplaceAll(strings.ReplaceAll(table.Name, ".", "_"), "`", ""),
		g.Now())

	var sql string
	if awsAccessKey != "" && awsSecretKey != "" {
		// Use credentials in the CREATE TABLE command
		if awsSessionToken != "" {
			sql = g.R(
				conn.template.Core["export_to_s3_with_creds_and_token"],
				"temp_table", tempTableName,
				"sql", table.Select(),
				"s3_path", s3Path,
				"file_format", format,
				"aws_access_key_id", awsAccessKey,
				"aws_secret_access_key", awsSecretKey,
				"aws_session_token", awsSessionToken,
			)
		} else {
			sql = g.R(
				conn.template.Core["export_to_s3_with_creds"],
				"temp_table", tempTableName,
				"sql", table.Select(),
				"s3_path", s3Path,
				"file_format", format,
				"aws_access_key_id", awsAccessKey,
				"aws_secret_access_key", awsSecretKey,
			)
		}
	} else {
		// Use IAM role or instance profile (credentials should be configured at cluster level)
		sql = g.R(
			conn.template.Core["export_to_s3"],
			"temp_table", tempTableName,
			"sql", table.Select(),
			"s3_path", s3Path,
			"file_format", format,
		)
	}

	g.Info("exporting from databricks to s3 via CREATE EXTERNAL TABLE")
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return "", g.Error(err, "SQL Error during CREATE EXTERNAL TABLE export")
	}

	// Clean up the temporary table
	defer func() {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
		_, dropErr := conn.Exec(dropSQL)
		if dropErr != nil {
			g.Warn("Could not drop temporary table %s: %v", tempTableName, dropErr)
		}
	}()

	g.Debug("Exported to %s", s3Path)
	return s3Path, nil
}

// BulkImportStream inserts a stream into a table
func (conn *DatabricksConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

// BulkExportFlow reads in bulk
func (conn *DatabricksConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	df = iop.NewDataflowContext(conn.Context().Ctx)

	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}
	table.Columns = columns

	filePath := ""

	if conn.GetProp("use_bulk") == "false" {
		return conn.BaseConn.BulkExportFlow(table)
	}

	switch conn.CopyMethod {
	case "aws":
		s3Path, err := conn.CopyToS3(table)
		if err != nil {
			err = g.Error(err, "Could not copy to S3.")
			return df, err
		}
		filePath = s3Path

	default:
		// Try volume-based export as fallback
		volume, err := conn.getOrCreateVolume(table.Schema)
		if err != nil {
			return nil, g.Error(err, "could not create volume")
		} else if volume != "" {
			var unloadedFiles int
			filePath, unloadedFiles, err = conn.UnloadViaVolume(table)
			if err != nil {
				err = g.Error(err, "Could not unload to volume.")
				return df, err
			} else if unloadedFiles == 0 {
				// since no files, return empty dataflow
				data := iop.NewDataset(columns)
				return iop.MakeDataFlow(data.Stream())
			}
			filePath = "file://" + filePath // add scheme
		}
	}

	var fs filesys.FileSysClient
	if strings.HasPrefix(filePath, "s3://") {
		fs, err = filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArrExclude("url")...)
		if err != nil {
			err = g.Error(err, "Could not get fs client for S3")
			return df, err
		}

		// Set format based on what was exported
		format := "parquet"
		if conn.GetProp("s3_file_format") != "" {
			format = conn.GetProp("s3_file_format")
		}

		if format == "parquet" {
			fs.SetProp("format", "parquet")
			fs.SetProp("compression", "snappy")
		} else {
			fs.SetProp("format", "csv")
			fs.SetProp("delimiter", ",")
			fs.SetProp("header", "true")
			fs.SetProp("null_if", "")
		}
	} else {
		// Local file system
		fs, err = filesys.NewFileSysClientFromURL(filePath, conn.PropArrExclude("url")...)
		if err != nil {
			err = g.Error(err, "Could not get fs client")
			return df, err
		}

		// format is auto-detected, below if CSV
		fs.SetProp("delimiter", ",")
		fs.SetProp("header", "true")
		fs.SetProp("null_if", "\\N")
	}

	// set column coercion if specified
	if coerceCols, ok := getColumnsProp(conn); ok {
		cc, _ := getColumnCasingProp(conn)
		tgtType := dbio.Type(conn.GetProp("target_type"))
		columns.Coerce(coerceCols, true, cc, tgtType)
	}

	fs.SetProp("columns", g.Marshal(columns))
	fs.SetProp("metadata", conn.GetProp("metadata"))

	df, err = fs.ReadDataflow(filePath)
	if err != nil {
		err = g.Error(err, "Could not read "+filePath)
		return df, err
	}

	df.MergeColumns(columns, true) // overwrite types so we don't need to infer
	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(fs, filePath)
		}
	})

	return df, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *DatabricksConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	// Add Databricks-specific DDL modifications
	// Databricks uses Delta tables by default
	if !temporary && !strings.Contains(strings.ToLower(sql), "using") {
		sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
		sql += " USING DELTA"
	}

	// Add partitioning if specified
	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		colNames := conn.GetType().QuoteNames(keys...)
		partitionBy = g.F(" PARTITIONED BY (%s)", strings.Join(colNames, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F(" PARTITIONED BY (%s)", strings.Join(colNames, ", "))
	}

	if partitionBy != "" {
		sql += partitionBy
	}

	// Add clustering if specified
	clusterBy := ""
	if keys, ok := table.Keys[iop.ClusterKey]; ok {
		colNames := conn.GetType().QuoteNames(keys...)
		clusterBy = g.F(" CLUSTER BY (%s)", strings.Join(colNames, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.ClusterKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		clusterBy = g.F(" CLUSTER BY (%s)", strings.Join(colNames, ", "))
	}

	if clusterBy != "" {
		sql += clusterBy
	}

	return strings.TrimSpace(sql) + ";", nil
}

// GetColumnsFull returns full column information for a table
func (conn *DatabricksConn) GetColumnsFull(tableFName string) (data iop.Dataset, err error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return data, g.Error(err, "could not parse table name: "+tableFName)
	}

	data1, err := conn.SubmitTemplate(
		"single", conn.template.Metadata, "columns_full",
		g.M("schema", table.Schema, "table", table.Name),
	)
	if err != nil {
		return data1, err
	}

	data.SetFields([]string{"schema_name", "table_name", "column_name", "data_type", "position"})
	for i, rec := range data1.Records() {
		data.Append([]interface{}{table.Schema, table.Name, rec["col_name"], rec["data_type"], i + 1})
	}
	return data, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *DatabricksConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	// Databricks SQL casting
	switch {
	case srcCol.IsString() && tgtCol.IsInteger():
		selectStr = g.F("CAST(%s AS BIGINT)", qName)
	case srcCol.IsString() && tgtCol.IsFloat():
		selectStr = g.F("CAST(%s AS DOUBLE)", qName)
	case srcCol.IsString() && tgtCol.IsDecimal():
		selectStr = g.F("CAST(%s AS DECIMAL(38,9))", qName)
	case srcCol.IsString() && tgtCol.IsBool():
		selectStr = g.F("CAST(%s AS BOOLEAN)", qName)
	case srcCol.IsString() && tgtCol.IsDate():
		selectStr = g.F("CAST(%s AS DATE)", qName)
	case srcCol.IsString() && tgtCol.IsDatetime():
		selectStr = g.F("CAST(%s AS TIMESTAMP)", qName)
	case !srcCol.IsString() && tgtCol.IsString():
		selectStr = g.F("CAST(%s AS STRING)", qName)
	case srcCol.Type != tgtCol.Type:
		selectStr = g.F("CAST(%s AS %s)", qName, tgtCol.DbType)
	default:
		selectStr = qName
	}

	return selectStr
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *DatabricksConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// For Databricks MERGE VALUES clause, convert placeholder_fields (ph.) to source references (src.)
	srcValuesFields := strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src.")

	// Databricks supports MERGE INTO
	sqlTempl := `
	MERGE INTO {tgt_table} AS tgt
	USING {src_table} AS src
	ON {src_tgt_pk_equal}
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) VALUES ({src_values_fields})
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_values_fields", srcValuesFields,
	)

	return sql, nil
}

// getOrCreateVolume creates a volume if it doesn't exist, similar to Snowflake's getOrCreateStage
func (conn *DatabricksConn) getOrCreateVolume(schema string) (internalVolume string, err error) {
	internalVolume = conn.GetProp("internal_volume")

	if internalVolume == "" {
		if schema == "" {
			schema = conn.Schema
		}
		if schema == "" {
			return "", g.Error("schema is required to create temporary volume")
		}

		// Create volume name similar to how Snowflake creates stages
		volumeName := "sling_volume"
		volumeFullName := fmt.Sprintf("%s.%s.%s", conn.Catalog, schema, volumeName)

		sql := g.R(
			conn.template.Core["create_volume"],
			"volume_name", volumeFullName,
		)
		_, err := conn.Exec(sql + env.NoDebugKey)
		if err != nil {
			return "", g.Error(err, "could not create volume: %s", volumeFullName)
		}

		volumePath := fmt.Sprintf("/Volumes/%s/%s/%s", conn.Catalog, schema, volumeName)
		conn.SetProp("internal_volume", volumePath)
		internalVolume = volumeFullName
	}
	return internalVolume, nil
}

// VolumePUT uploads a local file to a Databricks volume using SQL commands
func (conn *DatabricksConn) VolumePUT(folderPath, localFilePath, volumePath string) error {
	sql := g.R(
		conn.template.Core["put_into_volume"],
		"local_file", localFilePath,
		"volume_path", volumePath,
	)

	ctx := driverctx.NewContextWithStagingInfo(
		conn.context.Ctx,
		[]string{folderPath},
	)

	_, err := conn.ExecContext(ctx, sql)
	if err != nil {
		return g.Error(err, "could not PUT file %s to volume path %s", localFilePath, volumePath)
	}

	// g.Debug("Successfully uploaded %s to %s", localFilePath, volumePath)
	return nil
}

// VolumeGET downloads a file from a Databricks volume to local filesystem
func (conn *DatabricksConn) VolumeGET(volumePath, folderPath, localFilePath string) error {
	sql := g.R(
		conn.template.Core["get_from_volume"],
		"volume_path", volumePath,
		"local_file", localFilePath,
	)

	ctx := driverctx.NewContextWithStagingInfo(
		conn.context.Ctx,
		[]string{folderPath},
	)
	_, err := conn.ExecContext(ctx, sql)
	if err != nil {
		return g.Error(err, "could not GET file %s from volume to %s", volumePath, localFilePath)
	}

	return nil
}

// VolumeList lists files in a Databricks volume path
func (conn *DatabricksConn) VolumeList(volumePath string) (data iop.Dataset, err error) {
	sql := g.R(
		conn.template.Core["list_volume"],
		"volume_path", volumePath,
	)

	data, err = conn.Query(sql)
	if err != nil {
		return data, g.Error(err, "could not LIST volume path %s", volumePath)
	}

	return data, nil
}

// VolumeDelete delete files in a Databricks volume path
func (conn *DatabricksConn) VolumeDelete(volumePaths ...string) (err error) {

	deleteContext := g.NewContext(conn.context.Ctx)

	for _, volumePath := range volumePaths {
		deleteContext.Wg.Write.Add()

		go func(volumePath string) {
			defer deleteContext.Wg.Write.Done()

			url := g.F("https://%s/api/2.0/fs/files%s", conn.GetProp("host"), volumePath)
			headers := map[string]string{"Authorization": "Bearer " + conn.GetProp("token")}
			_, respBytes, err := net.ClientDo("DELETE", url, nil, headers)
			if err != nil {
				deleteContext.CaptureErr(g.Error(err, "could not delete volume via API with for `%s` %s\nResponse: %s", volumePath, string(respBytes)))
			}

		}(volumePath)
	}

	deleteContext.Wg.Write.Wait()

	return deleteContext.Err()
}

// CopyFromVolume uses the Databricks COPY INTO command from volumes
func (conn *DatabricksConn) CopyFromVolume(tableFName, volumePath string, fileFormat dbio.FileType, columns iop.Columns) error {
	tgtColumns := make([]string, len(columns))
	for i, name := range columns.Names() {
		colName, _ := ParseColumnName(name, conn.GetType())
		tgtColumns[i] = conn.Quote(colName)
	}

	sql := g.R(
		conn.template.Core["copy_from_volume_"+fileFormat.String()],
		"tgt_columns", strings.Join(tgtColumns, ", "),
		"table", tableFName,
		"volume_path", volumePath,
	)

	data, err := conn.Query(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}
	g.Debug("\n" + data.PrettyTable())

	return nil
}

// CopyViaVolume uses Databricks volumes for bulk import, similar to Snowflake's CopyViaStage
func (conn *DatabricksConn) CopyViaVolume(table Table, df *iop.Dataflow) (count uint64, err error) {
	context := g.NewContext(conn.Context().Ctx)

	if conn.GetProp("internal_volume") == "" {
		return 0, g.Error("Prop internal_volume is required")
	}

	if conn.Schema == "" {
		if table.Schema == "" {
			return 0, g.Error("Prop schema is required for copy via volume")
		}
		conn.SetProp("schema", table.Schema)
	} else if table.Schema == "" {
		table.Schema = conn.GetProp("schema")
	}

	// get target columns
	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		return 0, g.Error(err, "could not get columns for %s", table.FullName())
	}

	columns, err = conn.ValidateColumnNames(columns, df.Columns.Names())
	if err != nil {
		return 0, g.Error(err, "could not validate columns prior to COPY from VOLUME for %s", table.FullName())
	}

	tableFName := table.FullName()

	// Write the dataflow to temp files
	folderPath := path.Join(env.GetTempFolder(), "databricks", "put", env.CleanTableName(tableFName), g.NowFileStr())

	// delete folder when done
	df.Defer(func() { env.RemoveAllLocalTempFile(folderPath) })

	fileReadyChn := make(chan filesys.FileReady, 10000)
	fileFormat := dbio.FileType(conn.GetProp("format"))
	if !g.In(fileFormat, dbio.FileTypeCsv, dbio.FileTypeParquet) {
		fileFormat = dbio.FileTypeCsv
		// fileFormat = dbio.FileTypeParquet // error-prone, type mismatch
	}

	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArrExclude("url")...)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Could not get fs client for Local"))
			return
		}

		config := iop.LoaderStreamConfig(true)
		config.TargetType = conn.GetType()
		config.Format = fileFormat
		config.Compression = iop.ZStandardCompressorType
		if val := strings.ToLower(conn.GetProp("COMPRESSION")); val != "" {
			config.Compression = iop.CompressorType(val)
		}
		config.FileMaxRows = cast.ToInt64(conn.GetProp("file_max_rows"))
		if config.FileMaxRows == 0 {
			config.FileMaxRows = 500000
		}

		switch fileFormat {
		case dbio.FileTypeCsv:
			config.Header = true
			config.Delimiter = ","
		case dbio.FileTypeParquet:
		}

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn, config)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error writing dataflow to disk: "+folderPath))
			return
		}
	}()

	// Get volume path for upload - format: /Volumes/catalog/schema/volume/path
	volumePrefix := conn.GetProp("internal_volume")
	volumeFolderPath := fmt.Sprintf("%s/%s/%s",
		volumePrefix, env.CleanTableName(tableFName), g.NowFileStr())

	// Clean up volume files when done
	volumeFilePaths := []string{}
	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			g.Debug("deleting temporary volume: %s", volumeFolderPath)
			err := conn.VolumeDelete(volumeFilePaths...)
			if err != nil {
				g.Warn("could not delete temporary volume files (%s): %s", volumeFolderPath, err.Error())
			}
		}
	})

	doPut := func(file filesys.FileReady) (volumeFilePath string) {
		defer func() { env.RemoveLocalTempFile(file.Node.Path()) }()
		os.Chmod(file.Node.Path(), 0777) // make file readable everywhere

		pathArr := strings.Split(file.Node.Path(), "/")
		fileName := pathArr[len(pathArr)-1]
		volumeFilePath = fmt.Sprintf("%s/%s", volumeFolderPath, fileName)

		err = conn.VolumePUT(folderPath, file.Node.Path(), volumeFilePath)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying to Databricks Volume: "+conn.GetProp("internal_volume")))
		}
		return volumeFilePath
	}

	doPutDone := func(file filesys.FileReady) {
		defer context.Wg.Write.Done()
		volumeFilePath := doPut(file)
		volumeFilePaths = append(volumeFilePaths, volumeFilePath) // for deletion
	}

	// Process files and upload to volume
	for file := range fileReadyChn {
		if df.Err() != nil || context.Err() != nil {
			break
		}

		context.Wg.Write.Add()
		go doPutDone(file)
	}

	context.Wg.Write.Wait()

	// Copy from volume to table
	if df.Err() == nil && context.Err() == nil {
		err = conn.CopyFromVolume(tableFName, volumeFolderPath, fileFormat, columns)
		if err != nil {
			return 0, g.Error(err, "Error with COPY INTO from volume")
		}
	}

	if context.Err() != nil {
		return 0, context.Err()
	}

	if df.Err() != nil {
		return 0, g.Error(df.Err())
	}

	return df.Count(), nil
}

// UnloadViaVolume exports data to a Databricks volume, similar to Snowflake's UnloadViaStage
func (conn *DatabricksConn) UnloadViaVolume(tables ...Table) (filePath string, unloadedFiles int, err error) {
	if conn.GetProp("internal_volume") == "" {
		return "", 0, g.Error("internal_volume is required for volume unload")
	}

	// Get volume path for export
	volumePrefix := conn.GetProp("internal_volume")
	volumeFolderPath := fmt.Sprintf("%s/%s/%s",
		volumePrefix, tempCloudStorageFolder, g.NowFileStr())

	unloadContext := g.NewContext(conn.Context().Ctx)
	fileFormat := dbio.FileType(conn.GetProp("format"))
	if !g.In(fileFormat, dbio.FileTypeCsv, dbio.FileTypeParquet) {
		// fileFormat = dbio.FileTypeCsv
		fileFormat = dbio.FileTypeParquet
	}

	// Write each table to temp file, then read to df
	localFolderPath := path.Join(env.GetTempFolder(), "databricks", "get", g.NowFileStr())
	if err = os.MkdirAll(localFolderPath, 0777); err != nil {
		return "", 0, g.Error(err, "could not create temp directory: %s", localFolderPath)
	}

	// Clean up volume files when done (similar to Snowflake's REMOVE)
	volumeFilePaths := []string{}
	defer func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			g.Debug("deleting temporary volume: %s", volumeFolderPath)
			err = conn.VolumeDelete(volumeFilePaths...)
			if err != nil {
				g.Warn("could not delete temporary volume files (%s): %s", volumeFolderPath, err.Error())
			}
		}
	}()

	unload := func(table Table, volumePartPath string) {
		defer unloadContext.Wg.Write.Done()

		selectOpts := SelectOptions{Fields: table.Columns.Names()}
		sql := g.R(
			conn.template.Core["export_to_volume_"+fileFormat.String()],
			"volume_path", volumePartPath,
			"sql", table.Select(selectOpts),
		)

		// convert variant to string
		if !table.IsQuery() {
			for _, col := range table.Columns {
				if g.In(strings.ToLower(col.DbType), "variant", "complex") {
					// replace only the first occurrence for straight select
					sql = strings.Replace(sql, conn.Quote(col.Name), conn.Quote(col.Name)+"::string", 1)
				}
			}
		}

		_, err := conn.Exec(sql)
		if err != nil {
			err = g.Error(err, "SQL Error for %s", volumePartPath)
			unloadContext.CaptureErr(err)
			return
		}
	}

	// Export each table to volume
	volumePathParts := []string{}
	for i, table := range tables {
		volumePathPart := fmt.Sprintf("%s/export_%02d", volumeFolderPath, i+1)
		unloadContext.Wg.Write.Add()
		go unload(table, volumePathPart)
		volumePathParts = append(volumePathParts, volumePathPart)
	}

	unloadContext.Wg.Write.Wait()
	err = unloadContext.Err()
	if err != nil {
		err = g.Error(err, "Could not unload to volume files")
		return
	}

	// Copy volume filePaths to local directory for reading using VolumeGET
	// List filePaths in volume
	for _, volumePathPart := range volumePathParts {
		data, err := conn.VolumeList(volumePathPart)
		if err != nil {
			err = g.Error(err, "Could not LIST volume path %s", volumeFolderPath)
			unloadContext.CaptureErr(err)
			return "", 0, err
		}
		for _, row := range data.Rows {
			volumeFilePaths = append(volumeFilePaths, cast.ToString(row[0]))
		}
	}

	// Copy files from volume to local temp directory using GET command
	for _, volumeFilePath := range volumeFilePaths {
		if !strings.HasSuffix(volumeFilePath, fileFormat.Ext()) {
			continue
		}
		volumeFilePathParts := strings.Split(volumeFilePath, "/")
		fileName := volumeFilePathParts[len(volumeFilePathParts)-1]
		localFilePath := path.Join(localFolderPath, fileName)

		// Use VolumeGET to download from volume to local
		err = conn.VolumeGET(volumeFilePath, localFolderPath, localFilePath)
		if err != nil {
			unloadContext.CaptureErr(g.Error(err, "Could not GET volume file %s to %s", volumeFilePath, localFilePath))
		} else {
			unloadedFiles++
		}
	}

	return localFolderPath, unloadedFiles, unloadContext.Err()
}
