package database

import (
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/youmark/pkcs8"

	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/snowflakedb/gosnowflake"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// SnowflakeConn is a Snowflake connection
type SnowflakeConn struct {
	BaseConn
	URL        string
	Warehouse  string
	CopyMethod string
	Mux        sync.Mutex
}

// Init initiates the object
func (conn *SnowflakeConn) Init() error {
	var sfLog = gosnowflake.GetLogger()
	sfLog.SetOutput(io.Discard)

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSnowflake
	conn.CopyMethod = "STAGE"

	if s := conn.GetProp("schema"); s != "" {
		conn.SetProp("schema", s)
	}

	if strings.EqualFold(conn.GetProp("authenticator"), "snowflake_jwt") &&
		(conn.GetProp("private_key_path") == "" && conn.GetProp("private_key") == "") {
		return g.Error(
			"did not provide property `private_key_path` or `private_key` with authenticator=snowflake_jwt. See https://docs.slingdata.io/connections/database-connections/snowflake",
		)
	}

	if m := conn.GetProp("copy_method"); m != "" {
		conn.CopyMethod = conn.GetProp("copy_method")
	}

	if val := cast.ToInt(conn.GetProp("max_chunk_download_workers")); val > 0 {
		gosnowflake.MaxChunkDownloadWorkers = val
	}

	if val := conn.GetProp("custom_json_decoder_enabled"); val != "" {
		gosnowflake.CustomJSONDecoderEnabled = cast.ToBool(val)
	}

	if keyPath := conn.GetProp("private_key_path"); keyPath != "" {
		if !g.PathExists(keyPath) {
			return g.Error("private_key_path does not exists (%s)", keyPath)
		}

		pemBytes, err := os.ReadFile(keyPath)
		if err != nil {
			return g.Error(err)
		}

		conn.SetProp("private_key", string(pemBytes))
	}

	if pk := conn.GetProp("private_key"); pk != "" {
		// if provided as file path
		if g.PathExists(pk) {
			pemBytes, err := os.ReadFile(pk)
			if err != nil {
				return g.Error(err)
			}
			pk = string(pemBytes)
		}

		encPK, err := getEncodedPrivateKey(pk, conn.GetProp("private_key_passphrase"))
		if err != nil {
			return g.Error(err, "could not get encoded private key")
		}
		conn.SetProp("encoded_private_key", encPK)
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

func (conn *SnowflakeConn) ConnString() string {
	connString := conn.URL

	if m := conn.GetProp("CopyMethod"); m != "" {
		connString = strings.ReplaceAll(connString, "CopyMethod="+m, "")
	}

	if epk := conn.GetProp("encoded_private_key"); epk != "" {
		connString = connString + "&authenticator=SNOWFLAKE_JWT&privateKey=" + epk
	}

	connString = strings.TrimSuffix(connString, "?")

	connString = strings.ReplaceAll(
		connString,
		"snowflake://",
		"",
	)

	return connString
}

// Connect connects to the database
func (conn *SnowflakeConn) Connect(timeOut ...int) error {
	err := conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}
	if cast.ToBool(conn.GetProp("POOL_USED")) {
		return nil
	}

	// Get Warehouse
	data, err := conn.Query("SHOW WAREHOUSES" + noDebugKey)
	if err != nil {
		return g.Error(err, "could not SHOW WAREHOUSES")
	}
	if len(data.Rows) > 0 {
		conn.SetProp("warehouse", cast.ToString(data.Rows[0][0]))
	}

	if val := conn.GetProp("database"); val != "" {
		_, err = conn.Exec("USE DATABASE " + val + noDebugKey)
	}
	if val := conn.GetProp("schema"); val != "" {
		_, err = conn.Exec("USE SCHEMA " + val + noDebugKey)
	}
	if val := conn.GetProp("role"); val != "" {
		_, err = conn.Exec("USE ROLE " + val + noDebugKey)
	}
	return err
}

func getEncodedPrivateKey(pemStr, passphrase string) (epk string, err error) {
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil {
		return "", g.Error("invalid private key data: no PEM block found or missing file")
	}

	key, err := pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
	if err != nil {
		return "", g.Error(err, "could not parse key")
	}

	privKeyPem, err := pkcs8.MarshalPrivateKey(key, nil, nil)
	if err != nil {
		return "", g.Error(err, "could not marshal key")
	}

	return base64.URLEncoding.EncodeToString(privKeyPem), nil
}

func (conn *SnowflakeConn) getOrCreateStage(schema string) string {
	internalStage := conn.GetProp("internal_stage")

createNew:
	if internalStage == "" {
		if schema == "" {
			schema = conn.GetProp("schema")
		}

		// use Table struct, but is a Snowflake Internal Stage
		defStaging := Table{
			Schema:  schema,
			Name:    "SLING_STAGING",
			Dialect: dbio.TypeDbSnowflake,
		}
		conn.Exec("USE SCHEMA " + defStaging.Schema + noDebugKey)
		_, err := conn.Exec("CREATE STAGE IF NOT EXISTS " + defStaging.FullName())
		if err != nil {
			g.Warn("Tried to create Internal Snowflake Stage but failed.\n" + g.ErrMsgSimple(err))
			return ""
		}
		conn.SetProp("schema", schema)
		conn.SetProp("internal_stage", defStaging.FullName())
	} else {
		defStaging, _ := ParseTableName(internalStage, dbio.TypeDbSnowflake)
		if defStaging.Schema != schema {
			// create new staging if schema is different
			internalStage = ""
			goto createNew
		}
		conn.Exec("USE SCHEMA " + defStaging.Schema + noDebugKey)
	}
	return conn.GetProp("internal_stage")
}

// GenerateDDL generates a DDL based on a dataset
func (conn *SnowflakeConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	clusterBy := ""
	if keys, ok := table.Keys[iop.ClusterKey]; ok {
		// allow custom SQL expression for clustering
		clusterBy = g.F("cluster by (%s)", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.ClusterKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		clusterBy = g.F("cluster by (%s)", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{cluster_by}", clusterBy)

	return strings.TrimSpace(sql), nil
}

// BulkExportFlow reads in bulk
func (conn *SnowflakeConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	df = iop.NewDataflowContext(conn.Context().Ctx)

	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	filePath := ""

	if conn.GetProp("use_bulk") != "false" {
		switch conn.CopyMethod {
		case "AZURE":
			filePath, err = conn.CopyToAzure(table)
			if err != nil {
				err = g.Error(err, "Could not copy to S3.")
				return
			}
		case "AWS":
			filePath, err = conn.CopyToS3(table)
			if err != nil {
				err = g.Error(err, "Could not copy to S3.")
				return
			}
		default:
			if stage := conn.getOrCreateStage(table.Schema); stage != "" {
				var unloaded int64
				filePath, unloaded, err = conn.UnloadViaStage(table)
				if err != nil {
					err = g.Error(err, "Could not unload to stage.")
					return
				} else if unloaded == 0 {
					// since no rows, return empty dataflow
					data := iop.NewDataset(columns)
					return iop.MakeDataFlow(data.Stream())
				}
				filePath = "file://" + filePath // add scheme
			} else {
				return conn.BaseConn.BulkExportFlow(table)
			}
		}
	} else {
		return conn.BaseConn.BulkExportFlow(table)
	}

	fs, err := filesys.NewFileSysClientFromURL(filePath, conn.PropArrExclude("url")...)
	if err != nil {
		err = g.Error(err, "Could not get fs client")
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
	df, err = fs.ReadDataflow(filePath)
	if err != nil {
		err = g.Error(err, "Could not read "+filePath)
		return
	}
	df.MergeColumns(columns, true) // overwrite types so we don't need to infer
	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(fs, filePath)
		}
	})

	return
}

// CopyToS3 exports a query to an S3 location
func (conn *SnowflakeConn) CopyToS3(tables ...Table) (s3Path string, err error) {

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID", "ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to S3 from snowflake")
		return
	}

	context := g.NewContext(conn.Context().Ctx)
	unload := func(table Table, s3PathPart string) {

		defer context.Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_s3"],
			"sql", table.Select(),
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = g.Error(err, fmt.Sprintf("SQL Error for %s", s3PathPart))
			context.CaptureErr(err)
		}

	}

	s3Bucket := conn.GetProp("AWS_BUCKET", "BUCKET")
	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArrExclude("url")...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", s3Bucket, tempCloudStorageFolder, cast.ToString(g.Now()))

	filesys.Delete(s3Fs, s3Path)
	for i, table := range tables {
		if context.Err() != nil {
			break
		}
		s3PathPart := fmt.Sprintf("%s/u%02d-", s3Path, i+1)
		context.Wg.Write.Add()
		go unload(table, s3PathPart)
	}

	context.Wg.Write.Wait()
	err = context.Err()

	if err == nil {
		g.Debug("Unloaded to %s", s3Path)
	}

	return s3Path, err
}

// CopyToAzure exports a query to an Azure location
func (conn *SnowflakeConn) CopyToAzure(tables ...Table) (azPath string, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy from snowflake to azure")
		return
	}

	azToken, err := getAzureToken(conn)
	if err != nil {
		return "", g.Error(err)
	}

	context := g.NewContext(conn.Context().Ctx)
	unload := func(table Table, azPathPart string) {

		defer context.Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_azure"],
			"sql", table.Select(),
			"azure_path", azPath,
			"azure_sas_token", azToken,
		)

		conn.SetProp("azure_sas_token", azToken)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = g.Error(err, fmt.Sprintf("SQL Error for %s", azPathPart))
			context.CaptureErr(err)
		}

	}

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArrExclude("url")...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	azPath = fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		tempCloudStorageFolder,
		cast.ToString(g.Now()),
	)

	filesys.Delete(azFs, azPath)
	for i, table := range tables {
		if context.Err() != nil {
			break
		}
		azPathPart := fmt.Sprintf("%s/u%02d-", azPath, i+1)
		context.Wg.Write.Add()
		go unload(table, azPathPart)
	}

	context.Wg.Write.Wait()
	err = context.Err()

	if err == nil {
		g.Debug("Unloaded to %s", azPath)
	}

	return azPath, err
}

// BulkImportFlow bulk import flow
func (conn *SnowflakeConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	// set OnSchemaChange
	if df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {
			// prevent any new writers
			conn.Mux.Lock()
			defer conn.Mux.Unlock()

			// wait till all current writers are done
			if qs := conn.Context().Wg.Write.GetQueueSize(); qs > 0 {
				conn.Context().Wg.Write.Wait()
			}

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for Snowflake")
			}
			return nil
		}
	}

	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)

	if conn.GetProp("use_bulk") != "false" {
		switch conn.CopyMethod {
		case "AWS":
			return conn.CopyViaAWS(tableFName, df)
		case "AZURE":
			return conn.CopyViaAzure(tableFName, df)
		default:
		}

		table, err := ParseTableName(tableFName, conn.Type)
		if err != nil {
			return 0, g.Error(err, "could not parse table name: "+tableFName)
		}

		stage := conn.getOrCreateStage(table.Schema)
		if stage != "" {
			return conn.CopyViaStage(table, df)
		}

		if err == nil && stage == "" {
			err = g.Error("Need to permit internal staging, or provide AWS/Azure creds")
			return 0, err
		}
	}

	for ds := range df.StreamCh {
		c, err := conn.BaseConn.InsertBatchStream(tableFName, ds)
		if err != nil {
			return 0, g.Error(err, "could not insert")
		}
		count += c
	}

	return count, nil
}

// BulkImportStream bulk import stream
func (conn *SnowflakeConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

// CopyViaAWS uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAWS(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)
	if conn.GetProp("AWS_BUCKET") == "" {
		err = g.Error("Need to set 'AWS_BUCKET' to copy to snowflake from S3")
		return
	}

	s3Path := fmt.Sprintf(
		"s3://%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		tempCloudStorageFolder,
		tableFName,
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

	g.Info("writing to s3 for snowflake import")
	s3Fs.SetProp("null_as", `\N`)
	bw, err := filesys.WriteDataflow(s3Fs, df, s3Path)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	return df.Count(), conn.CopyFromS3(tableFName, s3Path)
}

// CopyFromS3 uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromS3(tableFName, s3Path string) (err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	sql := g.R(
		conn.template.Core["copy_from_s3"],
		"table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	sql = conn.setEmptyAsNull(sql)

	g.Info("copying into snowflake from s3")
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}

	return nil
}

// CopyViaAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.ZStandardCompressorType)
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to snowflake from azure")
		return
	}

	azPath := fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		tempCloudStorageFolder,
		tableFName,
	)

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArrExclude("url")...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(azFs, azPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(azFs, azPath)
		}
	}) // cleanup

	g.Info("writing to azure for snowflake import")
	azFs.SetProp("null_as", `\N`)
	bw, err := filesys.WriteDataflow(azFs, df, azPath)
	if err != nil {
		return df.Count(), g.Error(err, "Error in FileSysWriteDataflow")
	}
	g.DebugLow("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

	return df.Count(), conn.CopyFromAzure(tableFName, azPath)
}

// CopyFromAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromAzure(tableFName, azPath string) (err error) {
	azToken, err := getAzureToken(conn)
	if err != nil {
		return g.Error(err)
	}

	sql := g.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
	)
	sql = conn.setEmptyAsNull(sql)

	g.Info("copying into snowflake from azure")
	g.Debug("url: " + azPath)
	conn.SetProp("azure_sas_token", azToken)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error")
	}

	return nil
}

func (conn *SnowflakeConn) UnloadViaStage(tables ...Table) (filePath string, unloaded int64, err error) {

	stageFolderPath := fmt.Sprintf(
		"@%s/%s/%s",
		conn.GetProp("internal_stage"),
		tempCloudStorageFolder,
		cast.ToString(g.Now()),
	)

	context := g.NewContext(conn.Context().Ctx)

	// Write the each stage file to temp file, read to ds
	folderPath := path.Join(env.GetTempFolder(), "snowflake", "get", g.NowFileStr())
	if err = os.MkdirAll(folderPath, 0777); err != nil {
		return "", 0, g.Error(err, "could not create temp directory: %s", folderPath)
	}

	unloadedRows := atomic.Int64{}
	unload := func(sql string, stagePartPath string) {

		defer context.Wg.Write.Done()

		unloadSQL := g.R(
			conn.template.Core["copy_to_stage"],
			"sql", sql,
			"stage_path", stagePartPath,
		)

		data, err := conn.Query(unloadSQL)
		g.LogError(err)
		if err != nil {
			err = g.Error(err, "SQL Error for %s", stagePartPath)
			context.CaptureErr(err)
		}

		if len(data.Rows) > 0 && len(data.Columns) > 0 {
			unloadedRows.Add(cast.ToInt64(data.Rows[0][0]))
		}

	}

	conn.Exec("REMOVE " + stageFolderPath)
	defer conn.Exec("REMOVE " + stageFolderPath)
	for i, table := range tables {
		stagePathPart := fmt.Sprintf("%s/%02d_", stageFolderPath, i+1)
		context.Wg.Write.Add()
		go unload(table.Select(), stagePathPart)
	}

	context.Wg.Write.Wait()
	err = context.Err()
	if err != nil {
		err = g.Error(err, "Could not unload to stage files")
		return
	}

	g.Debug("Unloaded %d rows to %s", unloadedRows.Load(), stageFolderPath)

	if unloadedRows.Load() == 0 {
		return "", 0, nil
	}

	// get file paths
	data, err := conn.Query("LIST " + stageFolderPath)
	if err != nil {
		err = g.Error(err, "Could not LIST for %s", stageFolderPath)
		context.CaptureErr(err)
		return
	}
	g.Trace("\n" + data.PrettyTable())

	// copies the folder level
	_, err = conn.StageGET(stageFolderPath, folderPath)
	if err != nil {
		err = g.Error(err, "Could not GET %s", stageFolderPath)
		context.CaptureErr(err)
		return
	}

	return folderPath, unloadedRows.Load(), context.Err()
}

// CopyViaStage uses the Snowflake COPY INTO Table command
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaStage(table Table, df *iop.Dataflow) (count uint64, err error) {

	context := g.NewContext(conn.Context().Ctx)

	if conn.GetProp("internal_stage") == "" {
		return 0, g.Error("Prop internal_stage is required")
	}

	if conn.GetProp("schema") == "" {
		if table.Schema == "" {
			return 0, g.Error("Prop schema is required")
		}
		conn.SetProp("schema", table.Schema)
	} else if table.Schema == "" {
		table.Schema = conn.GetProp("schema")
	}

	// get target columns
	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		return 0, g.Error("could not get columns for %s", table.FullName())
	}

	columns, err = conn.ValidateColumnNames(columns, df.Columns.Names())
	if err != nil {
		return 0, g.Error("could not validate columns prior to COPY from STAGE for %s", table.FullName())
	}

	tableFName := table.FullName()

	// Write the ds to a temp file
	folderPath := path.Join(env.GetTempFolder(), "snowflake", "put", env.CleanTableName(tableFName), g.NowFileStr())

	// delete folder when done
	df.Defer(func() { env.RemoveAllLocalTempFile(folderPath) })

	fileReadyChn := make(chan filesys.FileReady, 10000)
	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArrExclude("url")...)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Could not get fs client for Local"))
			return
		}

		config := iop.LoaderStreamConfig(true)
		config.TargetType = conn.GetType()
		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn, config)

		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error writing dataflow to disk: "+folderPath))
			return
		}

	}()

	// Import to staging
	stageFolderPath := g.F("@%s/%s/%s", conn.GetProp("internal_stage"), env.CleanTableName(tableFName), g.NowFileStr())
	conn.Exec("USE SCHEMA " + conn.GetProp("schema"))
	_, err = conn.Exec("REMOVE " + stageFolderPath)
	if err != nil {
		err = g.Error(err, "REMOVE: "+stageFolderPath)
		return
	}
	df.Defer(func() {
		if err != nil && strings.Contains(err.Error(), "transaction") {
			conn.tx = nil // clear any failed transactions
			conn.Exec("REMOVE " + stageFolderPath)
		}
	})

	doPut := func(file filesys.FileReady) (stageFilePath string) {
		defer func() { env.RemoveLocalTempFile(file.Node.Path()) }()
		os.Chmod(file.Node.Path(), 0777) // make file readeable everywhere
		err = conn.StagePUT(file.Node.URI, stageFolderPath)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying to Snowflake Stage: "+conn.GetProp("internal_stage")))
		}
		pathArr := strings.Split(file.Node.Path(), "/")
		fileName := pathArr[len(pathArr)-1]
		stageFilePath = g.F("%s/%s", stageFolderPath, fileName)
		return stageFilePath
	}

	doPutDone := func(file filesys.FileReady) {
		defer context.Wg.Write.Done()
		doPut(file)
	}

	doCopy := func(file filesys.FileReady) {
		defer context.Wg.Write.Done()
		stageFilePath := doPut(file)

		if df.Err() != nil {
			return
		}

		tgtColumns := make([]string, len(file.Columns))
		for i, name := range file.Columns.Names() {
			colName, _ := ParseColumnName(name, conn.GetType())
			tgtColumns[i] = conn.Quote(colName)
		}

		srcColumns := make([]string, len(file.Columns))
		for i := range file.Columns {
			srcColumns[i] = g.F("T.$%d", i+1)
		}

		sql := g.R(
			conn.template.Core["copy_from_stage"],
			"table", tableFName,
			"tgt_columns", strings.Join(tgtColumns, ", "),
			"src_columns", strings.Join(srcColumns, ", "),
			"stage_path", stageFilePath,
		)
		sql = conn.setEmptyAsNull(sql)

		_, err = conn.Exec(sql)
		if err != nil {
			err = g.Error(err, "Error with COPY INTO")
			df.Context.CaptureErr(err)
		}
	}
	_ = doCopy

	doCopyFolder := func() {

		if df.Err() != nil {
			return
		}

		tgtColumns := make([]string, len(columns))
		for i, name := range columns.Names() {
			tgtColumns[i] = conn.Quote(name)
		}

		srcColumns := make([]string, len(df.Columns))
		for i := range df.Columns {
			srcColumns[i] = g.F("T.$%d", i+1)
		}

		sql := g.R(
			conn.template.Core["copy_from_stage"],
			"table", tableFName,
			"tgt_columns", strings.Join(tgtColumns, ", "),
			"src_columns", strings.Join(srcColumns, ", "),
			"stage_path", stageFolderPath,
		)
		sql = conn.setEmptyAsNull(sql)

		data, err := conn.Query(sql)
		if err != nil {
			err = g.Error(err, "Error with COPY INTO")
			df.Context.CaptureErr(err)
		}
		g.Debug("\n" + data.PrettyTable("file", "status", "rows_loaded", "errors_seen"))
	}
	_ = doCopyFolder

	for file := range fileReadyChn {
		if df.Err() != nil || context.Err() != nil {
			break
		}

		conn.Mux.Lock() // to not collide with schema change
		context.Wg.Write.Add()
		go doPutDone(file) // when using doCopyFolder
		// go doCopy(file)
		conn.Mux.Unlock()
	}

	context.Wg.Write.Wait()

	doCopyFolder()

	if context.Err() != nil {
		return 0, context.Err()
	}

	if df.Err() != nil {
		return 0, g.Error(df.Err())
	}

	return df.Count(), nil
}

func (conn *SnowflakeConn) setEmptyAsNull(sql string) string {
	if cast.ToBool(conn.GetProp("empty_as_null")) {
		sql = strings.ReplaceAll(sql, "EMPTY_FIELD_AS_NULL = FALSE", "EMPTY_FIELD_AS_NULL = TRUE")
	}
	return sql
}

// StageGET Copies from a staging location to a local file or folder
func (conn *SnowflakeConn) StageGET(internalStagePath, folderPath string) (filePaths []string, err error) {
	query := g.F(
		"GET %s 'file://%s' overwrite=true parallel=%d",
		internalStagePath, folderPath, runtime.NumCPU(),
	)

	data, err := conn.Query(query)
	if err != nil {
		err = g.Error(err, "could not GET file %s", internalStagePath)
		return
	}

	g.Debug("\n" + data.PrettyTable())

	for _, row := range data.Rows {
		nameParts := strings.Split(cast.ToString(row[0]), "/")
		fileName := nameParts[len(nameParts)-1]
		filePaths = append(filePaths, g.F("%s/%s", folderPath, fileName))
	}

	return
}

// StagePUT Copies a local file or folder into a staging location
func (conn *SnowflakeConn) StagePUT(fileURI string, internalStagePath string) (err error) {
	query := g.F(
		"PUT '%s' %s PARALLEL=%d AUTO_COMPRESS=FALSE",
		fileURI, internalStagePath, runtime.NumCPU(),
	)

	data, err := conn.Query(query)
	if err != nil {
		err = g.Error(err, "could not PUT file %s", fileURI)
		return
	}

	g.Trace("\n" + data.PrettyTable())

	return
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *SnowflakeConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	merge into {tgt_table} tgt
	using (select {src_fields} from {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) values  ({src_fields_values})
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"src_fields_values", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *SnowflakeConn) GetColumnsFull(tableFName string) (data iop.Dataset, err error) {
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
		dataType, _, _ := parseSnowflakeDataType(rec)
		data.Append([]interface{}{rec["schema_name"], rec["table_name"], rec["column_name"], dataType, i + 1})
	}
	return data, nil
}

// GetDatabases returns the list of databases
func (conn *SnowflakeConn) GetDatabases() (data iop.Dataset, err error) {
	data1, err := conn.BaseConn.GetDatabases()
	if err != nil {
		return data1, err
	}

	return data1.Pick("name"), nil
}

// GetSchemas returns schemas
func (conn *SnowflakeConn) GetSchemas() (data iop.Dataset, err error) {
	// fields: [schema_name]
	data1, err := conn.BaseConn.GetSchemas()
	if err != nil {
		return data1, err
	}

	data = data1.Pick("name")
	data.Columns[0].Name = "schema_name" // rename column

	return data, nil
}

// GetTables returns tables
func (conn *SnowflakeConn) GetTables(schema string) (data iop.Dataset, err error) {
	// fields: [schema_name, table_name]
	data1, err := conn.BaseConn.GetTables(schema)
	if err != nil {
		return data1, err
	}
	data = data1.Pick("schema_name", "name")
	data.Columns[0].Name = "SCHEMA_NAME"
	data.Columns[1].Name = "TABLE_NAME"

	data.Columns = append(data.Columns, iop.Column{Name: "IS_VIEW", Type: iop.BoolType, Position: 3})

	for i := range data.Rows {
		data.Rows[i] = append(data.Rows[i], false)
	}

	return data, nil
}

// GetTables returns tables
func (conn *SnowflakeConn) GetViews(schema string) (data iop.Dataset, err error) {
	// fields: [schema_name, table_name]
	data1, err := conn.BaseConn.GetViews(schema)
	if err != nil {
		return data1, err
	}

	data = data1.Pick("schema_name", "name")
	data.Columns[0].Name = "SCHEMA_NAME"
	data.Columns[1].Name = "TABLE_NAME"

	data.Columns = append(data.Columns, iop.Column{Name: "IS_VIEW", Type: iop.BoolType, Position: 3})

	for i := range data.Rows {
		data.Rows[i] = append(data.Rows[i], true)
	}

	return data, nil
}

// GetTablesAndViews returns tables/views for given schema
func (conn *SnowflakeConn) GetTablesAndViews(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	dataTables, err := conn.GetTables(schema)
	if err != nil {
		return iop.Dataset{}, err
	}

	dataViews, err := conn.GetViews(schema)
	if err != nil {
		return iop.Dataset{}, err
	}

	// combine
	for _, row := range dataViews.Rows {
		dataTables.Append(row)
	}

	return dataTables, nil
}

func (conn *SnowflakeConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {

	values := make([]string, len(cols))
	qFields := make([]string, len(cols)) // quoted fields

	hasVariant := false
	for _, col := range cols {
		if col.DbType == "VARIANT" {
			hasVariant = true
		}
	}

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, col := range cols {
			c++
			values[i] = conn.bindVar(i+1, col.Name, n, c)
			qFields[i] = conn.Self().Quote(col.Name)
			if col.DbType == "VARIANT" {
				values[i] = "parse_json(" + values[i] + ")"
			}
		}
		if hasVariant {
			// use SELECT & UNION ALL when there is a variant column
			// since binding variants is not supported
			valuesStr += fmt.Sprintf("select %s union all\n", strings.Join(values, ", "))
		} else {
			valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
		}
	}

	template := lo.Ternary(
		hasVariant,
		"insert into {table} ({fields})  {values}",
		"insert into {table} ({fields}) values  {values}",
	)

	statement := g.R(
		template,
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(strings.TrimSuffix(valuesStr, ","), "union all\n"),
	)

	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// CastColumnForSelect casts to the correct target column type
func (conn *SnowflakeConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)
	srcDbType := strings.ToUpper(string(srcCol.DbType))
	tgtDbType := strings.ToUpper(string(tgtCol.DbType))

	switch {
	case srcCol.IsString() && srcDbType != "VARIANT" && tgtDbType == "VARIANT":
		selectStr = g.F("parse_json(%s::string)", qName)
	case srcCol.IsString() && srcDbType != "FIXED" && tgtDbType == "FIXED":
		selectStr = g.F("to_decimal(%s::string, %d, %d)", qName, ddlMaxDecLength, 10)
	case srcCol.IsString() && !tgtCol.IsString():
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case !srcCol.IsString() && tgtCol.IsString():
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.Type != iop.TimestampzType && tgtCol.Type == iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.Type == iop.TimestampzType && tgtCol.Type != iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	default:
		selectStr = qName
	}

	return selectStr
}

func parseSnowflakeDataType(rec map[string]any) (dataType string, precision, scale int) {
	dataType = "UNKNOWN"
	typeJSON := g.M()
	err := g.Unmarshal(cast.ToString(rec["data_type"]), &typeJSON)
	if err == nil {
		dataType = cast.ToString(typeJSON["type"])
		precision = cast.ToInt(typeJSON["precision"])
		scale = cast.ToInt(typeJSON["scale"])
		if dataType == "FIXED" && scale == 0 {
			dataType = "BIGINT"
		}
	}
	return
}
