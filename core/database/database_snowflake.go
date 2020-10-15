package database

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// SnowflakeConn is a Snowflake connection
type SnowflakeConn struct {
	BaseConn
	URL        string
	Warehouse  string
	CopyMethod string
}

// Init initiates the object
func (conn *SnowflakeConn) Init() error {

	if s := conn.GetProp("schema"); s != "" {
		conn.URL = strings.ReplaceAll(conn.URL, "schema="+s, "")
		conn.SetProp("schema", s)
	} else {
		conn.SetProp("schema", "public") // default schema
	}

	if m := conn.GetProp("CopyMethod"); m != "" {
		conn.URL = strings.ReplaceAll(conn.URL, "CopyMethod="+m, "")
		conn.CopyMethod = conn.GetProp("CopyMethod")
	}

	if strings.HasSuffix(conn.URL, "?") {
		conn.URL = conn.URL[0 : len(conn.URL)-1]
	}

	URL := strings.ReplaceAll(
		conn.URL,
		"snowflake://",
		"",
	)

	conn.BaseConn.URL = URL
	conn.BaseConn.Type = SnowflakeDbType

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()

}

// Connect connects to the database
func (conn *SnowflakeConn) Connect(timeOut ...int) error {
	err := conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}
	// Get Warehouse
	data, err := conn.Query("SHOW WAREHOUSES")
	if err != nil {
		return h.Error(err, "could not SHOW WAREHOUSES")
	}
	if len(data.Rows) > 0 {
		conn.SetProp("warehouse", cast.ToString(data.Rows[0][0]))
	}

	if conn.GetProp("schema") != "" {
		_, err = conn.Exec("USE SCHEMA " + conn.GetProp("schema"))
	}
	return err
}

func (conn *SnowflakeConn) getOrCreateStage(tableFName string) string {
	if conn.GetProp("internalStage") == "" {
		defStaging := "sling_staging"
		schema, _ := SplitTableFullName(tableFName)
		_, err := conn.Exec("USE SCHEMA " + schema)
		_, err = conn.Exec("CREATE STAGE IF NOT EXISTS " + defStaging)
		if err != nil {
			h.Debug(h.ErrMsg(err))
			return ""
		}
		conn.SetProp("schema", schema)
		conn.SetProp("internalStage", defStaging)
	}
	return conn.GetProp("internalStage")
}

// BulkExportFlow reads in bulk
func (conn *SnowflakeConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	df = iop.NewDataflow()

	columns, err := conn.GetSQLColumns(sqls...)
	if err != nil {
		err = h.Error(err, "Could not get columns.")
		return
	}

	filePath := ""
	switch conn.CopyMethod {
	case "AZURE":
		filePath, err = conn.CopyToAzure(sqls...)
		if err != nil {
			err = h.Error(err, "Could not copy to S3.")
			return
		}
	default:
		// default is AWS
		filePath, err = conn.CopyToS3(sqls...)
		if err != nil {
			err = h.Error(err, "Could not copy to S3.")
			return
		}
	}

	fs, err := iop.NewFileSysClientFromURL(filePath, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client")
		return
	}

	df, err = fs.ReadDataflow(filePath)
	if err != nil {
		err = h.Error(err, "Could not read "+filePath)
		return
	}
	df.SetColumns(columns)
	df.Inferred = true
	df.Defer(func() { fs.Delete(filePath) })

	return
}

// CopyToS3 exports a query to an S3 location
func (conn *SnowflakeConn) CopyToS3(sqls ...string) (s3Path string, err error) {

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = h.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to S3 from snowflake"))
		return
	}

	unload := func(sql string, s3PathPart string) {

		defer conn.Context().Wg.Write.Done()

		unloadSQL := h.R(
			conn.template.Core["copy_to_s3"],
			"sql", sql,
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = h.Error(err, fmt.Sprintf("SQL Error for %s", s3PathPart))
			conn.Context().CaptureErr(err)
		}

	}

	s3Bucket := conn.GetProp("AWS_BUCKET")
	s3Fs, err := iop.NewFileSysClient(iop.S3FileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", s3Bucket, filePathStorageSlug, cast.ToString(h.Now()))

	s3Fs.Delete(s3Path)
	for i, sql := range sqls {
		s3PathPart := fmt.Sprintf("%s/u%02d-", s3Path, i+1)
		conn.Context().Wg.Write.Add()
		go unload(sql, s3PathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		h.Debug("Unloaded to %s", s3Path)
	}

	return s3Path, err
}

// CopyToAzure exports a query to an Azure location
func (conn *SnowflakeConn) CopyToAzure(sqls ...string) (azPath string, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = h.Error(errors.New("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy from snowflake to azure"))
		return
	}

	azToken, err := conn.getAzureToken()
	if err != nil {
		return "", h.Error(err)
	}

	unload := func(sql string, azPathPart string) {

		defer conn.Context().Wg.Write.Done()

		unloadSQL := h.R(
			conn.template.Core["copy_to_azure"],
			"sql", sql,
			"azure_path", azPath,
			"azure_sas_token", azToken,
		)

		conn.SetProp("azure_sas_token", azToken)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			err = h.Error(err, fmt.Sprintf("SQL Error for %s", azPathPart))
			conn.Context().CaptureErr(err)
		}

	}

	azFs, err := iop.NewFileSysClient(iop.AzureFileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	azPath = fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		filePathStorageSlug,
		cast.ToString(h.Now()),
	)

	azFs.Delete(azPath)
	for i, sql := range sqls {
		azPathPart := fmt.Sprintf("%s/u%02d-", azPath, i+1)
		conn.Context().Wg.Write.Add()
		go unload(sql, azPathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		h.Debug("Unloaded to %s", azPath)
	}

	return azPath, err
}

// BulkImportFlow bulk import flow
func (conn *SnowflakeConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)

	switch conn.CopyMethod {
	case "AWS":
		return conn.CopyViaAWS(tableFName, df)
	case "AZURE":
		return conn.CopyViaAzure(tableFName, df)
	}

	_, err = exec.LookPath("snowsql")
	stage := conn.getOrCreateStage(tableFName)
	if err == nil && stage != "" {
		return conn.CopyViaStage(tableFName, df)
	}

	if conn.BaseConn.credsProvided("AWS") {
		return conn.CopyViaAWS(tableFName, df)
	} else if conn.BaseConn.credsProvided("AZURE") {
		return conn.CopyViaAzure(tableFName, df)
	}

	if err == nil && stage == "" {
		err = h.Error(fmt.Errorf("Need to permit internal staging, or provide AWS/Azure creds"))
		return 0, err
	}

	h.Debug("snowsql not found in path & AWS/Azure creds not provided. Using cursor")
	ds := iop.MergeDataflow(df)
	return conn.BaseConn.InsertBatchStream(tableFName, ds)
}

// BulkImportStream bulk import stream
func (conn *SnowflakeConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = h.Error(err, "Could not MakeDataFlow")
		return
	}
	return conn.BulkImportFlow(tableFName, df)
}

// CopyViaAWS uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAWS(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)
	if conn.GetProp("AWS_BUCKET") == "" {
		err = h.Error(errors.New("Need to set 'AWS_BUCKET' to copy to snowflake from S3"))
		return
	}

	s3Path := fmt.Sprintf(
		"s3://%s/%s/%s",
		conn.GetProp("AWS_BUCKET"),
		filePathStorageSlug,
		tableFName,
	)

	s3Fs, err := iop.NewFileSysClient(iop.S3FileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	err = s3Fs.Delete(s3Path)
	if err != nil {
		return count, h.Error(err, "Could not Delete: "+s3Path)
	}

	defer func() { s3Fs.Delete(s3Path) }() // cleanup

	h.Info("writing to s3 for snowflake import")
	bw, err := s3Fs.WriteDataflow(df, s3Path)
	if err != nil {
		return df.Count(), h.Error(err, "Error in FileSysWriteDataflow")
	}
	h.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	return df.Count(), conn.CopyFromS3(tableFName, s3Path)
}

// CopyFromS3 uses the Snowflake COPY INTO Table command from AWS S3
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromS3(tableFName, s3Path string) (err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = h.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3"))
		return
	}

	sql := h.R(
		conn.template.Core["copy_from_s3"],
		"table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)

	h.Info("copying into snowflake from s3")
	h.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return h.Error(err, "SQL Error:\n"+conn.CleanSQL(sql))
	}

	return nil
}

// CopyViaAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)
	if !conn.BaseConn.credsProvided("AZURE") {
		err = h.Error(errors.New("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to snowflake from azure"))
		return
	}

	azPath := fmt.Sprintf(
		"azure://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		filePathStorageSlug,
		tableFName,
	)

	azFs, err := iop.NewFileSysClient(iop.AzureFileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	err = azFs.Delete(azPath)
	if err != nil {
		return count, h.Error(err, "Could not Delete: "+azPath)
	}

	defer func() { azFs.Delete(azPath) }() // cleanup

	h.Info("writing to azure for snowflake import")
	bw, err := azFs.WriteDataflow(df, azPath)
	if err != nil {
		return df.Count(), h.Error(err, "Error in FileSysWriteDataflow")
	}
	h.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

	return df.Count(), conn.CopyFromAzure(tableFName, azPath)
}

func (conn *SnowflakeConn) getAzureToken() (azToken string, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = h.Error(errors.New("Need to set 'AZURE_SAS_SVC_URL' to copy to snowflake from azure"))
		return
	}

	azSasURLArr := strings.Split(azSasURL, "?")
	if len(azSasURLArr) != 2 {
		err = h.Error(
			fmt.Errorf("Invalid provided AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	azToken = azSasURLArr[1]
	return
}

// CopyFromAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyFromAzure(tableFName, azPath string) (err error) {
	azToken, err := conn.getAzureToken()
	if err != nil {
		return h.Error(err)
	}

	sql := h.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
	)

	h.Info("copying into snowflake from azure")
	h.Debug("url: " + azPath)
	conn.SetProp("azure_sas_token", azToken)
	_, err = conn.Exec(sql)
	if err != nil {
		return h.Error(err, "SQL Error:\n"+conn.CleanSQL(sql))
	}

	return nil
}

// CopyViaStage uses the Snowflake COPY INTO Table command
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func (conn *SnowflakeConn) CopyViaStage(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	if conn.GetProp("internalStage") == "" {
		return 0, h.Error(fmt.Errorf("Prop internalStage is required"), "")
	}

	if conn.GetProp("schema") == "" {
		schema, _ := SplitTableFullName(tableFName)
		if schema == "" {
			return 0, h.Error(fmt.Errorf("Prop schema is required"), "")
		}
		conn.SetProp("schema", schema)
	}

	// Write the ds to a temp file
	folderPath := fmt.Sprintf(
		"/tmp/snowflake.put.%d.%s.csv",
		time.Now().Unix(),
		h.RandString(h.AlphaRunes, 3),
	)

	// delete folder when done
	defer os.RemoveAll(folderPath)

	fileReadyChn := make(chan string, 10000)
	go func() {
		fs, err := iop.NewFileSysClient(iop.LocalFileSys, conn.PropArr()...)
		if err != nil {
			err = h.Error(err, "Could not get fs client for Local")
			return
		}

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn)

		if err != nil {
			err = h.Error(err, "Error writing dataflow to disk: "+folderPath)
			return
		}

	}()

	// Import to staging
	stageFolderPath := h.F("@%s/%s/%s", conn.GetProp("internalStage"), tableFName, h.NowFileStr())
	conn.Exec("USE SCHEMA " + conn.GetProp("schema"))
	_, err = conn.Exec("REMOVE " + stageFolderPath)
	if err != nil {
		err = h.Error(err, "REMOVE: "+stageFolderPath)
		return
	}
	defer conn.Exec("REMOVE " + stageFolderPath)

	doPut := func(filePath string) {
		defer os.Remove(filePath)
		defer conn.Context().Wg.Write.Done()
		err = conn.PutFile(filePath, stageFolderPath)
		if err != nil {
			df.Context.CaptureErr(h.Error(err, "Error copying to Snowflake Stage: "+conn.GetProp("internalStage")))
		}
	}

	for filePath := range fileReadyChn {
		conn.Context().Wg.Write.Add()
		go doPut(filePath)
	}

	conn.Context().Wg.Write.Wait()

	if df.Context.Err() != nil {
		return 0, h.Error(df.Context.Err())
	}

	// COPY INTO Table
	sql := h.R(
		conn.template.Core["copy_from_stage"],
		"table", tableFName,
		"stage_path", stageFolderPath,
	)

	_, err = conn.Exec(sql)
	if err != nil {
		err = h.Error(err, "Error with COPY INTO:\n"+sql)
		return
	}

	return df.Count(), nil
}

// PutFile Copies a local file or folder into a staging location
func (conn *SnowflakeConn) PutFile(fPath string, internalStagePath string) (err error) {
	var stderr, stdout bytes.Buffer

	query := h.F(
		"PUT file://%s %s PARALLEL=20",
		fPath, internalStagePath,
	)

	// Parse URL
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		return
	}

	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	database := strings.ReplaceAll(url.Path, "/", "")
	user := url.User.Username()
	if port == "" {
		port = "443"
	}

	proc := exec.Command(
		"snowsql",
		"--single-transaction",
		"--abort-detached-query",
		"-q", query,
	)

	proc.Env = []string{
		"LC_ALL=C.UTF-8",
		"LANG=C.UTF-8",
		"SNOWSQL_ACCOUNT=" + host,
		"SNOWSQL_PORT=" + port,
		"SNOWSQL_USER=" + user,
		"SNOWSQL_PWD=" + password,
		"SNOWSQL_DATABASE=" + database,
		// "SNOWSQL_WAREHOUSE=" + conn.GetProp("warehouse"),
		"SNOWSQL_SCHEMA=" + conn.GetProp("schema"),
	}

	proc.Stderr = &stderr
	proc.Stdout = &stdout

	// run and wait for finish
	cmdStr := strings.Join(proc.Args, " ")
	h.Trace("" + cmdStr)
	err = proc.Run()

	if err != nil || strings.TrimSpace(stderr.String()) != "" || proc.ProcessState.ExitCode() != 0 {

		err = h.Error(
			fmt.Errorf(stderr.String()),
			fmt.Sprintf(
				"SnowSQL Command -> %s\nSnowSQL Error  -> %s\n%s",
				cmdStr, stderr.String(), stdout.String(),
			),
		)
		return
	}

	h.Trace("\n%s", stdout.String())

	return
}

func selectFromDataset(data iop.Dataset, colIDs []int) (newData iop.Dataset) {
	newData = iop.NewDataset(data.Columns)
	newData.Rows = make([][]interface{}, len(data.Rows))

	for i, row := range data.Rows {
		newRow := make([]interface{}, len(colIDs))
		for j, c := range colIDs {
			if c+1 > len(row) {
				continue
			}
			newRow[j] = row[c]
		}
		newData.Rows[i] = newRow
	}
	return
}

// GetSchemas returns schemas
func (conn *SnowflakeConn) GetSchemas() (iop.Dataset, error) {
	colSelect := []int{1}
	data, err := conn.BaseConn.GetSchemas()
	return selectFromDataset(data, colSelect), err
}

// GetTables returns tables for given schema
func (conn *SnowflakeConn) GetTables(schema string) (iop.Dataset, error) {
	colSelect := []int{1}
	data, err := conn.BaseConn.GetTables(schema)
	return selectFromDataset(data, colSelect), err
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *SnowflakeConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = h.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	MERGE INTO {tgt_table} tgt
	USING (SELECT *	FROM {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) VALUES ({src_fields})
	`

	sql := h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placehold_fields"], "ph.", "src."),
	)
	txn, err := conn.Db().Begin()
	if err != nil {
		err = h.Error(err, "Could not begin transaction for upsert")
		return
	}
	res, err := txn.ExecContext(conn.Context().Ctx, sql)
	if err != nil {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	rowAffCnt, err = res.RowsAffected()
	if err != nil {
		rowAffCnt = -1
		cnt, _ := conn.GetCount(srcTable)
		if cnt > 0 {
			rowAffCnt = cast.ToInt64(cnt)
		}
	}

	err = txn.Commit()
	if err != nil {
		err = h.Error(err, "Could not commit upsert transaction")
		return
	}
	return
}

// CopyDirect copies directly from cloud files
// (without passing through sling)
func (conn *SnowflakeConn) CopyDirect(tableFName string, srcFile iop.DataConn) (cnt uint64, ok bool, err error) {
	props := h.MapToKVArr(srcFile.VarsS())
	fs, err := iop.NewFileSysClientFromURL(srcFile.URL, props...)
	if err != nil {
		err = h.Error(err, "Could not obtain client for: "+srcFile.URL)
		return
	}

	switch fs.FsType() {
	case iop.S3FileSys:
		ok = true
		err = conn.CopyFromS3(tableFName, srcFile.URL)
		if err != nil {
			err = h.Error(err, "could not load into database from S3")
		}
	case iop.AzureFileSys:
		ok = true
		err = conn.CopyFromAzure(tableFName, srcFile.URL)
		if err != nil {
			err = h.Error(err, "could not load into database from Azure")
		}
	case iop.GoogleFileSys:
	}

	if err != nil {
		// ok = false // try through sling?
	}
	return
}
