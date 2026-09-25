package database

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/databricks/databricks-sql-go/driverctx"
	dbsqllog "github.com/databricks/databricks-sql-go/logger"
	zerobus "github.com/databricks/zerobus-sdk/go"
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
	URL         string
	Catalog     string
	Schema      string
	Warehouse   string
	CopyMethod  string
	TableFormat string

	ZerobusEndpoint    string
	ClientID           string
	ClientSecret       string
	BatchSize          int
	IPCCompression     string
	MaxInflightBatches int
}

// Init initiates the object
func (conn *DatabricksConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDatabricks
	conn.CopyMethod = "stage"
	conn.TableFormat = "delta"

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.Schema = conn.GetProp("schema")

	if m := conn.GetProp("copy_method"); m != "" {
		conn.CopyMethod = strings.ToLower(m)
	}

	if tf := conn.GetProp("table_format"); tf != "" {
		conn.TableFormat = strings.ToLower(tf)
	}
	if conn.CopyMethod == "zerobus" && conn.TableFormat != "delta" {
		g.Debug("copy_method: zerobus requires Delta tables; using table_format=delta instead of %s", conn.TableFormat)
		conn.TableFormat = "delta"
	}

	if w := conn.GetProp("warehouse"); w != "" {
		conn.Warehouse = w
	}

	conn.ZerobusEndpoint = conn.GetProp("zerobus_endpoint")
	conn.ClientID = conn.GetProp("client_id")
	conn.ClientSecret = conn.GetProp("client_secret")

	if conn.BatchSize <= 0 {
		conn.BatchSize = cast.ToInt(conn.GetProp("batch_size"))
		if conn.BatchSize <= 0 {
			conn.BatchSize = 10000
		}
	}
	if conn.IPCCompression == "" {
		conn.IPCCompression = strings.ToLower(conn.GetProp("ipc_compression", "compression"))
		if conn.IPCCompression == "" {
			conn.IPCCompression = "none"
		}
	}
	if conn.MaxInflightBatches <= 0 {
		conn.MaxInflightBatches = cast.ToInt(conn.GetProp("max_inflight_batches"))
		if conn.MaxInflightBatches <= 0 {
			conn.MaxInflightBatches = 1000
		}
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

	// update decimal columns precision/scale based on column_typing
	// this is needed especially for inferring the correct arrow parquet schema
	if err = applyColumnTypingToDf(conn, df); err != nil {
		return 0, g.Error(err, "invalid column_typing")
	}

	if conn.GetProp("use_bulk") == "false" {
		// Fall back to base implementation which does batch inserts
		return conn.BaseConn.BulkImportFlow(tableFName, df)
	}

	switch conn.CopyMethod {
	case "aws":
		return conn.CopyViaS3(tableFName, df)
	case "zerobus":
		table, err := ParseTableName(tableFName, conn.Type)
		if err != nil {
			return 0, g.Error(err, "could not parse table name: "+tableFName)
		}
		return conn.CopyViaZerobus(table, df)
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
	return loadAWSCredentialsFromChain(conn)
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
	g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

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
	// table format defaults to delta, set table_format=iceberg to use Iceberg
	if !strings.Contains(strings.ToLower(sql), "using") {
		sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
		sql += " USING " + strings.ToUpper(conn.TableFormat)
	}

	// Add partitioning if specified
	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		colNames := conn.Template().QuoteNames(keys...)
		partitionBy = g.F(" PARTITIONED BY (%s)", strings.Join(colNames, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.Template().QuoteNames(keyCols.Names()...)
		partitionBy = g.F(" PARTITIONED BY (%s)", strings.Join(colNames, ", "))
	}

	if partitionBy != "" {
		sql += partitionBy
	}

	// Add clustering if specified
	clusterBy := ""
	if keys, ok := table.Keys[iop.ClusterKey]; ok {
		colNames := conn.Template().QuoteNames(keys...)
		clusterBy = g.F(" CLUSTER BY (%s)", strings.Join(colNames, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.ClusterKey); len(keyCols) > 0 {
		colNames := conn.Template().QuoteNames(keyCols.Names()...)
		clusterBy = g.F(" CLUSTER BY (%s)", strings.Join(colNames, ", "))
	}

	if clusterBy != "" {
		sql += clusterBy
	}

	// column comments (Delta has no secondary CREATE INDEX)
	sql = appendColumnComments(strings.TrimSpace(sql), conn, table, data, temporary)

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

// GenerateMergeSQL generates the upsert SQL using the database default strategy.
func (conn *DatabricksConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, nil)
}

// GenerateMergeSQLWithStrategy generates the merge SQL using the specified strategy.
// Databricks supports all four merge strategies via native MERGE support.
func (conn *DatabricksConn) GenerateMergeSQLWithStrategy(srcTable string, tgtTable string, pkFields []string, strategy *MergeStrategy) (sql string, err error) {
	return conn.BaseConn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, strategy)
}

// getOrCreateVolume creates a volume if it doesn't exist, similar to Snowflake's getOrCreateStage
func (conn *DatabricksConn) getOrCreateVolume(schema string) (internalVolume string, err error) {
	var volume Table
	internalVolume = conn.GetProp("internal_volume")

	if internalVolume == "" {
		if schema == "" {
			schema = conn.Schema
		}
		if schema == "" {
			return "", g.Error("schema is required to create temporary volume")
		}

		// Create volume name similar to how Snowflake creates stages
		volume = Table{
			Database: conn.Catalog,
			Schema:   schema,
			Name:     "sling_volume",
			Dialect:  dbio.TypeDbDatabricks,
		}

		volumeFullName := volume.FDQN()
		sql := g.R(conn.template.Core["create_volume"], "volume_name", volumeFullName)
		if _, err = conn.Exec(sql + env.NoDebugKey); err != nil {
			return "", g.Error(err, "could not create volume: %s", volumeFullName)
		}
		conn.SetProp("internal_volume", volumeFullName)
	} else {
		volume, err = ParseTableName(internalVolume, dbio.TypeDbDatabricks)
		if err != nil {
			return "", g.Error(err, "invalid volume name, should be in format: `catalog_name`.`schema_name`.`volume_name`")
		}
	}

	volumePath := fmt.Sprintf("/Volumes/%s/%s/%s", volume.Database, volume.Schema, volume.Name)
	conn.SetProp("internal_volume_path", volumePath)

	return volumePath, nil
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

var (
	volumeFilesMaxRetries = 5
	volumeFilesRetryBase  = 500 * time.Millisecond
	volumeFilesMaxWait    = 8 * time.Second
)

func (conn *DatabricksConn) volumeFilesAPIURL(volumePath string) string {
	host := conn.GetProp("host")
	scheme := "https"
	switch {
	case strings.HasPrefix(host, "http://"):
		scheme = "http"
		host = strings.TrimPrefix(host, "http://")
	case strings.HasPrefix(host, "https://"):
		host = strings.TrimPrefix(host, "https://")
	case conn.GetProp("protocol") == "http", conn.GetProp("use_ssl") == "false", conn.GetProp("ssl") == "false":
		scheme = "http"
	}
	host = strings.TrimRight(host, "/")
	if !strings.HasPrefix(volumePath, "/") {
		volumePath = "/" + volumePath
	}
	return fmt.Sprintf("%s://%s/api/2.0/fs/files%s", scheme, host, volumePath)
}

func volumeFilesRetryWait(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		if retryAfter > 30*time.Second {
			return 30 * time.Second
		}
		return retryAfter
	}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 5 {
		attempt = 5
	}
	base := volumeFilesRetryBase * time.Duration(1<<uint(attempt))
	if base > volumeFilesMaxWait {
		base = volumeFilesMaxWait
	}
	jitterMax := int64(base / 2)
	if jitterMax < 1 {
		jitterMax = 1
	}
	return base/2 + time.Duration(rand.Int64N(jitterMax))
}

func parseRetryAfter(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(v)
	if err != nil || secs <= 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}

func isVolumeFilesRetryable(status int, body string) bool {
	if status == http.StatusTooManyRequests || status >= 500 {
		return true
	}
	upper := strings.ToUpper(body)
	return strings.Contains(upper, "RESOURCE_EXHAUSTED") ||
		strings.Contains(upper, "THROTTL") ||
		strings.Contains(upper, "TOO MANY REQUESTS")
}

func (conn *DatabricksConn) volumeDeleteFile(ctx context.Context, volumePath string) error {
	if volumePath == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	url := conn.volumeFilesAPIURL(volumePath)
	token := conn.GetProp("token")
	client := &http.Client{Timeout: 30 * time.Second}

	var lastErr error
	var retryAfter time.Duration
	for attempt := 0; attempt <= volumeFilesMaxRetries; attempt++ {
		if attempt > 0 {
			wait := volumeFilesRetryWait(attempt, retryAfter)
			retryAfter = 0
			g.Debug("volume delete throttled for %s, retrying in %s (attempt %d/%d)", volumePath, wait, attempt, volumeFilesMaxRetries)
			select {
			case <-ctx.Done():
				if lastErr != nil {
					return g.Error(lastErr, "could not delete volume file %s: %s", volumePath, ctx.Err())
				}
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return g.Error(err, "could not build volume delete request for %s", volumePath)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return g.Error(err, "could not delete volume file %s", volumePath)
			}
			continue
		}

		respBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body := string(respBytes)

		if resp.StatusCode == http.StatusNotFound || (resp.StatusCode >= 200 && resp.StatusCode < 300) {
			return nil
		}

		lastErr = g.Error("unexpected response %d deleting volume file %s: %s", resp.StatusCode, volumePath, body)
		if !isVolumeFilesRetryable(resp.StatusCode, body) {
			return lastErr
		}
		retryAfter = parseRetryAfter(resp.Header)
	}

	return g.Error(lastErr, "could not delete volume file %s after %d retries", volumePath, volumeFilesMaxRetries)
}

// VolumeDelete delete files in a Databricks volume path
func (conn *DatabricksConn) VolumeDelete(volumePaths ...string) (err error) {
	if len(volumePaths) == 0 {
		return nil
	}

	parent := context.Background()
	if conn.Context() != nil && conn.Context().Ctx != nil {
		parent = conn.Context().Ctx
	}
	// Cap parallelism so cleanup does not stampede S3 behind Volumes.
	deleteContext := g.NewContext(parent, 3)

	for _, volumePath := range volumePaths {
		if volumePath == "" {
			continue
		}
		deleteContext.Wg.Write.Add()
		go func(volumePath string) {
			defer deleteContext.Wg.Write.Done()
			// Use parent ctx so one exhausted failure does not cancel sibling backoff.
			if err := conn.volumeDeleteFile(parent, volumePath); err != nil {
				deleteContext.CaptureErr(g.Error(err, "could not delete volume file `%s`", volumePath))
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
	// The Arrow lane writes Parquet records, so the COPY runs with a parquet
	// file format; the row path keeps the `format` conn prop (CSV by default).
	fileFormat := stageFileFormat(df, dbio.FileType(conn.GetProp("format")))

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
	volumePrefix := conn.GetProp("internal_volume_path")
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
			df.Context.CaptureErr(g.Error(err, "Error copying to Databricks Volume: "+conn.GetProp("internal_volume_path")))
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
	volumePrefix := conn.GetProp("internal_volume_path")
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
			if delErr := conn.VolumeDelete(volumeFilePaths...); delErr != nil {
				g.Warn("could not delete temporary volume files (%s): %s", volumeFolderPath, delErr.Error())
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

func (conn *DatabricksConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {

	// table names need to be lower case for information schema lookup
	for i, tableName := range tableNames {
		tableNames[i] = strings.ToLower(tableName)
	}

	return conn.BaseConn.GetSchemata(level, schemaName, tableNames...)
}

type zerobusStream interface {
	IngestBatch(ipc []byte) (int64, error)
	Flush() error
	Close() error
	GetUnackedBatches() ([][]byte, error)
}

type zerobusStreamOpener func(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error)

var openZerobusStream zerobusStreamOpener = openZerobusStreamSDK

var zerobusDescribe = func(conn *DatabricksConn, tableFName string) (iop.Columns, error) {
	cols, err := conn.GetColumns(tableFName)
	if err != nil {
		return nil, g.Error(err, "create the table first or use copy_method: stage")
	}
	if len(cols) == 0 {
		return nil, g.Error("create the table first or use copy_method: stage")
	}
	return cols, nil
}

func openZerobusStreamSDK(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
	sdk, err := zerobus.NewZerobusSdk(endpoint, workspaceURL)
	if err != nil {
		return nil, nil, g.Error(err, "could not create Zerobus SDK")
	}
	stream, err := sdk.CreateArrowStream(tableName, schemaIPC, clientID, clientSecret, opts)
	if err != nil {
		sdk.Free()
		return nil, nil, g.Error(err, "could not create Zerobus Arrow stream for %s", tableName)
	}
	return stream, sdk.Free, nil
}

type zerobusPATHeaders struct {
	token, tableName string
}

func (p *zerobusPATHeaders) GetHeaders() (map[string]string, error) {
	return map[string]string{
		"authorization":                   "Bearer " + p.token,
		"x-databricks-zerobus-table-name": p.tableName,
	}, nil
}

func openZerobusStreamPAT(endpoint, workspaceURL, tableName string, schemaIPC []byte, token string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
	sdk, err := zerobus.NewZerobusSdk(endpoint, workspaceURL)
	if err != nil {
		return nil, nil, g.Error(err, "could not create Zerobus SDK")
	}
	stream, err := sdk.CreateArrowStreamWithHeadersProvider(tableName, schemaIPC, &zerobusPATHeaders{token: token, tableName: tableName}, opts)
	if err != nil {
		sdk.Free()
		return nil, nil, g.Error(err, "could not create Zerobus Arrow stream for %s", tableName)
	}
	return stream, sdk.Free, nil
}

func isZerobusSchemaLag(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "schema comparison failed") ||
		strings.Contains(s, "schema_validation_failed") ||
		strings.Contains(s, "does not exist in delta schema") ||
		strings.Contains(s, "field_not_in_table")
}

func (conn *DatabricksConn) openZerobusArrowStream(endpoint, workspaceURL, tableName string, schemaIPC []byte, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
	open := func() (zerobusStream, func(), error) {
		if conn.ClientID != "" && conn.ClientSecret != "" {
			return openZerobusStream(endpoint, workspaceURL, tableName, schemaIPC, conn.ClientID, conn.ClientSecret, opts)
		}
		return openZerobusStreamPAT(endpoint, workspaceURL, tableName, schemaIPC, conn.GetProp("token"), opts)
	}

	stream, free, err := open()
	if err == nil || conn.db == nil || !isZerobusSchemaLag(err) {
		return stream, free, err
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		g.Debug("zerobus Delta schema not yet visible for %s, retrying", tableName)
		time.Sleep(time.Second)
		stream, free, err = open()
		if err == nil || !isZerobusSchemaLag(err) {
			return stream, free, err
		}
	}
	return nil, nil, err
}

func (conn *DatabricksConn) databricksGETJSON(path string, dest any) error {
	workspaceURL, err := conn.unityCatalogURL()
	if err != nil {
		return err
	}
	token := conn.GetProp("token")
	if token == "" {
		return g.Error("databricks token is required")
	}
	req, err := http.NewRequest(http.MethodGet, workspaceURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return g.Error("databricks API %s returned %d: %s", path, resp.StatusCode, string(body))
	}
	if dest == nil {
		return nil
	}
	return json.Unmarshal(body, dest)
}

func (conn *DatabricksConn) resolveZerobusEndpoint() error {
	if strings.TrimSpace(conn.ZerobusEndpoint) != "" {
		return nil
	}
	if v := conn.GetProp("zerobus_endpoint"); v != "" {
		conn.ZerobusEndpoint = v
		return nil
	}
	if conn.GetProp("host") == "" || conn.GetProp("token") == "" {
		return g.Error("zerobus_endpoint is required for copy_method: zerobus (the shard URL, not the workspace host)")
	}

	var assignment struct {
		WorkspaceID json.Number `json:"workspace_id"`
		MetastoreID string      `json:"metastore_id"`
	}
	if err := conn.databricksGETJSON("/api/2.1/unity-catalog/current-metastore-assignment", &assignment); err != nil {
		return g.Error(err, "zerobus_endpoint is required for copy_method: zerobus (the shard URL, not the workspace host)")
	}

	var listing struct {
		Metastores []struct {
			MetastoreID string `json:"metastore_id"`
			Region      string `json:"region"`
			Cloud       string `json:"cloud"`
		} `json:"metastores"`
	}
	_ = conn.databricksGETJSON("/api/2.1/unity-catalog/metastores", &listing)

	region, cloud := "", "aws"
	for _, m := range listing.Metastores {
		if m.MetastoreID == assignment.MetastoreID || (assignment.MetastoreID == "" && m.Region != "") {
			region = m.Region
			if m.Cloud != "" {
				cloud = strings.ToLower(m.Cloud)
			}
			if m.MetastoreID == assignment.MetastoreID {
				break
			}
		}
	}
	workspaceID := assignment.WorkspaceID.String()
	if workspaceID == "" || region == "" {
		return g.Error("zerobus_endpoint is required for copy_method: zerobus (could not detect workspace_id/region)")
	}

	suffix := "cloud.databricks.com"
	host := conn.GetProp("host")
	switch {
	case strings.Contains(host, "azuredatabricks.net"):
		suffix = "azuredatabricks.net"
	case strings.Contains(host, "gcp.databricks.com"):
		suffix = "gcp.databricks.com"
	case cloud == "azure":
		suffix = "azuredatabricks.net"
	case cloud == "gcp":
		suffix = "gcp.databricks.com"
	}

	conn.ZerobusEndpoint = fmt.Sprintf("https://%s.zerobus.%s.%s", workspaceID, region, suffix)
	g.Debug("detected zerobus_endpoint=%s", conn.ZerobusEndpoint)
	return nil
}

func (conn *DatabricksConn) validateZerobusConfig() error {
	if err := conn.resolveZerobusEndpoint(); err != nil {
		return err
	}
	if conn.ZerobusEndpoint == "" {
		return g.Error("zerobus_endpoint is required for copy_method: zerobus (the shard URL, not the workspace host)")
	}
	if (conn.ClientID == "" || conn.ClientSecret == "") && conn.GetProp("token") == "" {
		return g.Error("client_id and client_secret (OAuth M2M) or token (PAT) are required for copy_method: zerobus")
	}
	return nil
}

func (conn *DatabricksConn) unityCatalogURL() (string, error) {
	host := conn.GetProp("host")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimRight(host, "/")
	if host == "" {
		return "", g.Error("databricks host is required for copy_method: zerobus")
	}
	return "https://" + host, nil
}

func (conn *DatabricksConn) zerobusTableName(table Table) string {
	catalog := table.Database
	if catalog == "" {
		catalog = conn.Catalog
	}
	schema := table.Schema
	if schema == "" {
		schema = conn.Schema
	}
	parts := []string{}
	if catalog != "" {
		parts = append(parts, catalog)
	}
	if schema != "" {
		parts = append(parts, schema)
	}
	parts = append(parts, table.Name)
	return strings.Join(parts, ".")
}

func mapZerobusIPCCompression(s string) (zerobus.IPCCompressionType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "none":
		return zerobus.IPCCompressionNone, nil
	case "lz4", "lz4_frame":
		return zerobus.IPCCompressionLZ4Frame, nil
	case "zstd", "zstandard":
		return zerobus.IPCCompressionZstd, nil
	default:
		return 0, g.Error("unsupported Zerobus IPC compression: %s (supported: none, lz4, zstd)", s)
	}
}

func normalizeZerobusEndpoint(endpoint string) string {
	endpoint = strings.TrimRight(endpoint, "/")
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	return "https://" + endpoint
}

// CopyViaZerobus streams Arrow RecordBatches into an existing Delta table.
func (conn *DatabricksConn) CopyViaZerobus(table Table, df *iop.Dataflow) (count uint64, err error) {
	if err = conn.validateZerobusConfig(); err != nil {
		return 0, err
	}

	workspaceURL, err := conn.unityCatalogURL()
	if err != nil {
		return 0, err
	}

	tableName := conn.zerobusTableName(table)
	if table.Name == "" {
		return 0, g.Error("target table must be specified for Zerobus ingestion")
	}

	tgtCols, err := zerobusDescribe(conn, table.FullName())
	if err != nil {
		return 0, err
	}
	for i := range tgtCols {
		if v := tgtCols[i].Metadata["is_nullable"]; v != "" {
			tgtCols[i].SetMetadata(string(iop.ColMetaNullable), v)
		}
	}

	srcIdx, err := alignZerobusSource(df.Columns, tgtCols)
	if err != nil {
		return 0, err
	}

	arrowSchema, err := ColumnsToZerobusArrowSchema(tgtCols)
	if err != nil {
		return 0, err
	}

	schemaIPC, err := SerializeSchemaToIPC(arrowSchema)
	if err != nil {
		return 0, err
	}

	codec, err := mapZerobusIPCCompression(conn.IPCCompression)
	if err != nil {
		return 0, err
	}

	opts := zerobus.DefaultArrowStreamConfigurationOptions()
	if conn.MaxInflightBatches > 0 {
		opts.MaxInflightBatches = uint64(conn.MaxInflightBatches)
	}
	opts.IPCCompression = codec

	g.Info("ingesting into Databricks via Zerobus Arrow stream: %s", tableName)

	endpoint := normalizeZerobusEndpoint(conn.ZerobusEndpoint)
	stream, free, err := conn.openZerobusArrowStream(endpoint, workspaceURL, tableName, schemaIPC, opts)
	if err != nil {
		return 0, err
	}
	if free != nil {
		defer free()
	}
	defer stream.Close()

	count, err = ingestZerobusFlow(conn, df, stream, arrowSchema, tgtCols, srcIdx)
	if err != nil {
		unacked, _ := stream.GetUnackedBatches()
		return 0, g.Error(err, "zerobus ingest failed, %d batches unacked", len(unacked))
	}

	if err = stream.Flush(); err != nil {
		unacked, _ := stream.GetUnackedBatches()
		return 0, g.Error(err, "zerobus flush failed, %d batches unacked", len(unacked))
	}

	// SQL warehouse snapshots can lag the Zerobus Delta commit.
	if count > 0 && conn.db != nil {
		_, _ = conn.Exec("REFRESH TABLE " + table.FullName() + env.NoDebugKey)
		deadline := time.Now().Add(30 * time.Second)
		var visible int64
		for {
			visible, err = conn.GetCount(table.FullName())
			if err == nil && uint64(visible) >= count {
				break
			}
			if time.Now().After(deadline) {
				if err != nil {
					return count, g.Error(err, "zerobus rows not yet visible in SQL warehouse for %s", table.FullName())
				}
				return count, g.Error("zerobus SQL warehouse count is %d after streaming %d rows into %s", visible, count, table.FullName())
			}
			time.Sleep(4 * time.Second)
		}
	}

	g.Info("successfully streamed %d rows to Zerobus table %s", count, tableName)
	return count, nil
}

func alignZerobusSource(src, tgt iop.Columns) ([]int, error) {
	srcMap := map[string]int{}
	for i, c := range src {
		srcMap[strings.ToLower(c.Name)] = i
	}

	used := map[string]bool{}
	srcIdx := make([]int, len(tgt))
	for i, tcol := range tgt {
		si, ok := srcMap[strings.ToLower(tcol.Name)]
		if !ok {
			if !columnZerobusNullable(tcol) {
				return nil, g.Error("source is missing non-null target column %s", tcol.Name)
			}
			srcIdx[i] = -1
			continue
		}
		used[strings.ToLower(tcol.Name)] = true
		srcIdx[i] = si
	}

	for _, c := range src {
		if !used[strings.ToLower(c.Name)] {
			return nil, g.Error("source has extra column %s not in target table", c.Name)
		}
	}
	return srcIdx, nil
}

func ingestZerobusFlow(conn *DatabricksConn, df *iop.Dataflow, stream zerobusStream, arrowSchema *arrow.Schema, tgtCols iop.Columns, srcIdx []int) (count uint64, err error) {
	mem := memory.NewGoAllocator()
	batchSize := conn.BatchSize
	if batchSize <= 0 {
		batchSize = 10000
	}

	builders := make([]array.Builder, len(tgtCols))
	createBuilder := func(dtype arrow.DataType) array.Builder {
		switch dtype.ID() {
		case arrow.BOOL:
			return array.NewBooleanBuilder(mem)
		case arrow.INT8:
			return array.NewInt8Builder(mem)
		case arrow.INT16:
			return array.NewInt16Builder(mem)
		case arrow.INT32:
			return array.NewInt32Builder(mem)
		case arrow.INT64:
			return array.NewInt64Builder(mem)
		case arrow.FLOAT32:
			return array.NewFloat32Builder(mem)
		case arrow.FLOAT64:
			return array.NewFloat64Builder(mem)
		case arrow.LARGE_STRING:
			return array.NewLargeStringBuilder(mem)
		case arrow.STRING:
			return array.NewStringBuilder(mem)
		case arrow.LARGE_BINARY:
			return array.NewBinaryBuilder(mem, arrow.BinaryTypes.LargeBinary)
		case arrow.BINARY:
			return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
		case arrow.DATE32:
			return array.NewDate32Builder(mem)
		case arrow.TIMESTAMP:
			return array.NewTimestampBuilder(mem, dtype.(*arrow.TimestampType))
		case arrow.DECIMAL128:
			return array.NewDecimal128Builder(mem, dtype.(*arrow.Decimal128Type))
		default:
			return array.NewLargeStringBuilder(mem)
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

		arrays := make([]arrow.Array, len(builders))
		for i, b := range builders {
			arrays[i] = b.NewArray()
		}
		record := array.NewRecord(arrowSchema, arrays, int64(rowsInBatch))

		batchBytes, err := SerializeRecordToIPC(arrowSchema, record, conn.IPCCompression)
		record.Release()
		for _, arr := range arrays {
			arr.Release()
		}
		releaseBuilders()
		resetBuilders()
		rowsInBatch = 0
		if err != nil {
			return err
		}

		_, err = stream.IngestBatch(batchBytes)
		return err
	}

	for ds := range df.StreamCh {
		for row := range ds.Rows() {
			for i, col := range tgtCols {
				var val interface{}
				si := srcIdx[i]
				if si >= 0 && si < len(row) {
					val = row[si]
				}
				appendToZerobusBuilder(builders[i], &col, val)
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

	if err := flushBatch(); err != nil {
		return count, g.Error(err, "failed to flush final Arrow RecordBatch to Zerobus")
	}
	return count, nil
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

// SerializeRecordToIPC serializes an Arrow Record into IPC stream bytes containing exactly one RecordBatch.
func SerializeRecordToIPC(schema *arrow.Schema, record arrow.Record, compression string) ([]byte, error) {
	var buf bytes.Buffer
	opts := []ipc.Option{ipc.WithSchema(schema)}

	switch strings.ToLower(compression) {
	case "lz4", "lz4_frame":
		opts = append(opts, ipc.WithLZ4())
	case "zstd", "zstandard":
		opts = append(opts, ipc.WithZstd())
	case "", "none":
	default:
		return nil, g.Error("unsupported Zerobus IPC compression: %s (supported: none, lz4, zstd)", compression)
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

func zerobusUnsupportedDbType(col iop.Column) error {
	dt := strings.ToLower(strings.TrimSpace(col.DbType))
	if dt == "" {
		return nil
	}
	if strings.HasPrefix(dt, "array") || strings.HasPrefix(dt, "map") ||
		strings.HasPrefix(dt, "struct") || strings.HasPrefix(dt, "variant") || dt == "object" {
		return g.Error("unsupported Zerobus type %s for column %s", col.DbType, col.Name)
	}
	return nil
}

func zerobusDecimalPrecisionScale(col iop.Column) (prec, scale int) {
	prec = col.DbPrecision
	scale = col.DbScale
	if prec <= 0 {
		dt := strings.ToLower(col.DbType)
		if i := strings.Index(dt, "("); i >= 0 {
			nums := strings.TrimSuffix(dt[i+1:], ")")
			parts := strings.Split(nums, ",")
			if len(parts) >= 1 {
				prec = cast.ToInt(strings.TrimSpace(parts[0]))
			}
			if len(parts) >= 2 {
				scale = cast.ToInt(strings.TrimSpace(parts[1]))
			}
		}
	}
	if prec <= 0 {
		prec = 38
	}
	if scale < 0 {
		scale = 0
	}
	return prec, scale
}

func columnZerobusNullable(col iop.Column) bool {
	if col.Metadata != nil {
		if v, ok := col.Metadata["is_nullable"]; ok {
			return v == "true" || strings.EqualFold(v, "yes")
		}
	}
	return col.IsNullable()
}

// ColumnsToZerobusArrowSchema maps Sling columns to the Arrow schema specified by
// Databricks Zerobus Arrow Flight ingestion.
func ColumnsToZerobusArrowSchema(columns iop.Columns) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))

	for i, col := range columns {
		if err := zerobusUnsupportedDbType(col); err != nil {
			return nil, err
		}

		var arrowType arrow.DataType

		switch col.Type {
		case iop.BoolType:
			arrowType = arrow.FixedWidthTypes.Boolean
		case iop.SmallIntType:
			if strings.EqualFold(col.DbType, "tinyint") || strings.EqualFold(col.DbType, "int8") || strings.EqualFold(col.DbType, "byte") {
				arrowType = arrow.PrimitiveTypes.Int8
			} else {
				arrowType = arrow.PrimitiveTypes.Int16
			}
		case iop.IntegerType:
			if strings.EqualFold(col.DbType, "tinyint") || strings.EqualFold(col.DbType, "int8") || strings.EqualFold(col.DbType, "byte") {
				arrowType = arrow.PrimitiveTypes.Int8
			} else if strings.EqualFold(col.DbType, "smallint") || strings.EqualFold(col.DbType, "int16") || strings.EqualFold(col.DbType, "short") {
				arrowType = arrow.PrimitiveTypes.Int16
			} else {
				arrowType = arrow.PrimitiveTypes.Int32
			}
		case iop.BigIntType:
			arrowType = arrow.PrimitiveTypes.Int64
		case iop.FloatType:
			if strings.EqualFold(col.DbType, "float") || strings.EqualFold(col.DbType, "float32") || strings.EqualFold(col.DbType, "real") {
				arrowType = arrow.PrimitiveTypes.Float32
			} else {
				arrowType = arrow.PrimitiveTypes.Float64
			}
		case iop.DecimalType:
			prec, scale := zerobusDecimalPrecisionScale(col)
			arrowType = &arrow.Decimal128Type{Precision: int32(prec), Scale: int32(scale)}
		case iop.DateType:
			arrowType = arrow.FixedWidthTypes.Date32
		case iop.TimestampzType:
			arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
		case iop.DatetimeType, iop.TimestampType:
			arrowType = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}
		case iop.BinaryType:
			arrowType = arrow.BinaryTypes.LargeBinary
		case iop.StringType, iop.TextType, iop.JsonType, iop.UUIDType:
			arrowType = arrow.BinaryTypes.LargeString
		default:
			arrowType = arrow.BinaryTypes.LargeString
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: columnZerobusNullable(col),
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func appendToZerobusBuilder(builder array.Builder, col *iop.Column, val interface{}) {
	if val == nil {
		builder.AppendNull()
		return
	}
	switch b := builder.(type) {
	case *array.LargeStringBuilder:
		b.Append(cast.ToString(val))
	default:
		iop.AppendToBuilder(builder, col, val)
	}
}
