package database

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"

	"github.com/slingdata-io/sling-cli/core/dbio/filesys"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// RedshiftConn is a Redshift connection
type RedshiftConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *RedshiftConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbRedshift
	conn.BaseConn.defaultPort = 5439

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

func (conn *RedshiftConn) ConnString() string {
	return strings.ReplaceAll(conn.URL, "redshift://", "postgres://")
}

func isRedshift(URL string) (isRs bool) {
	db, err := sqlx.Open("postgres", URL)
	if err != nil {
		return isRs
	}
	res, err := db.Queryx("select version() v")
	if err != nil {
		return isRs
	}
	res.Next()
	row, err := res.SliceScan()
	if err != nil {
		return isRs
	}
	if strings.Contains(strings.ToLower(cast.ToString(row[0])), "redshift") {
		isRs = true
	}
	db.Close()
	return isRs
}

// GenerateDDL generates a DDL based on a dataset
func (conn *RedshiftConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	distKey := ""
	if keyCols := data.Columns.GetKeys(iop.DistributionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		distKey = g.F("distkey(%s)", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{dist_key}", distKey)

	sortKey := ""
	if keyCols := data.Columns.GetKeys(iop.SortKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		sortKey = g.F("compound sortkey(%s)", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{sort_key}", sortKey)

	return strings.TrimSpace(sql), nil
}

// getS3Props gets the properties for the S3 Filesystem,
// adding fallbacks for credentials for wider compatibility.
// See: https://github.com/slingdata-io/sling-cli/issues/571
func (conn *RedshiftConn) getS3Props() []string {
	s3Props := conn.PropArr()

	awsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	awsKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	awsToken := conn.GetProp("AWS_SESSION_TOKEN")

	if awsID != "" {
		s3Props = append(s3Props, "ACCESS_KEY_ID="+awsID)
	}
	if awsKey != "" {
		s3Props = append(s3Props, "SECRET_ACCESS_KEY="+awsKey)
	}
	if awsToken != "" {
		s3Props = append(s3Props, "SESSION_TOKEN="+awsToken)
	}

	// If no AWS credentials are provided, instruct S3 client to use environment credentials
	if awsID == "" && awsKey == "" {
		s3Props = append(s3Props, "USE_ENVIRONMENT=true")
	}
	return s3Props
}

// Unload unloads a query to S3
func (conn *RedshiftConn) Unload(ctx *g.Context, tables ...Table) (s3Path string, err error) {

	if conn.GetProp("AWS_BUCKET") == "" {
		return "", g.Error("need to set AWS_BUCKET")
	}

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	AwsSessionToken := conn.GetProp("AWS_SESSION_TOKEN")

	AwsSessionTokenExpr := ""
	if AwsSessionToken != "" {
		AwsSessionTokenExpr = g.F(";token=%s", AwsSessionToken)
	}

	g.Info("unloading from redshift to s3")
	queryContext := g.NewContext(ctx.Ctx)
	unload := func(table Table, s3PathPart string) {

		defer queryContext.Wg.Write.Done()

		// if it is limited, create temp table first, unload and drop
		matched, _ := regexp.MatchString(`limit\s+\d+`, strings.ToLower(table.Select()))
		if matched || table.limit > 0 {
			// create temp table
			tempTable := table.Clone()
			tempTable.Name = fmt.Sprintf("temp_unload_%d", time.Now().UnixNano())

			createSQL := g.F("create temporary table %s as %s", tempTable.Name, table.Select())

			// update unload SQL to use temp table
			unloadSQL := g.R(
				conn.template.Core["copy_to_s3"],
				"sql", g.F("select * from %s", tempTable.Name),
				"s3_path", s3PathPart,
				"aws_access_key_id", AwsID,
				"aws_secret_access_key", AwsAccessKey,
				"aws_session_token_expr", AwsSessionTokenExpr,
				"parallel", conn.GetProp("PARALLEL"),
			)

			dropTableSQL := g.F("drop table if exists %s", tempTable.Name)

			_, err = conn.ExecMulti(createSQL, unloadSQL, dropTableSQL)
			if err != nil {
				err = g.Error(err, "could not create temp table for unload")
				queryContext.CaptureErr(err)
				return
			}

		} else {

			sql := strings.ReplaceAll(strings.ReplaceAll(table.Select(), "\n", " "), "'", "''")

			unloadSQL := g.R(
				conn.template.Core["copy_to_s3"],
				"sql", sql,
				"s3_path", s3PathPart,
				"aws_access_key_id", AwsID,
				"aws_secret_access_key", AwsAccessKey,
				"aws_session_token_expr", AwsSessionTokenExpr,
				"parallel", conn.GetProp("PARALLEL"),
			)

			_, err = conn.Exec(unloadSQL)
			if err != nil {
				cleanSQL := strings.ReplaceAll(unloadSQL, AwsID, "*****")
				cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
				err = g.Error(err, fmt.Sprintf("SQL Error for %s:\n%s", s3PathPart, cleanSQL))
				queryContext.CaptureErr(err)
			}
		}

	}

	// Prepare properties for S3 client
	s3Props := conn.getS3Props()
	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, s3Props...)
	if err != nil {
		err = g.Error(err, "Unable to create S3 Client")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", conn.GetProp("AWS_BUCKET"), tempCloudStorageFolder, cast.ToString(g.Now()))

	filesys.Delete(s3Fs, s3Path)
	for i, table := range tables {
		s3PathPart := fmt.Sprintf("%s/u%02d-", s3Path, i+1)
		queryContext.Wg.Write.Add()
		go unload(table, s3PathPart)
	}

	queryContext.Wg.Write.Wait()
	err = queryContext.Err()

	if err == nil {
		g.Debug("Unloaded to %s", s3Path)
	}

	return s3Path, err
}

// BulkExportStream reads in bulk
func (conn *RedshiftConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {

	df, err := conn.BulkExportFlow(table)
	if err != nil {
		return ds, g.Error(err, "Could not export")
	}

	return iop.MergeDataflow(df), nil
}

// BulkExportFlow reads in bulk
func (conn *RedshiftConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	if conn.GetProp("AWS_BUCKET") == "" {
		g.Warn("using cursor to export. Please set AWS creds for Sling to use the Redshift UNLOAD function (for bigger datasets). See https://docs.slingdata.io/connections/database-connections/redshift")
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

	// Prepare properties for S3 client
	s3Props := conn.getS3Props()
	fs, err := filesys.NewFileSysClientContext(unloadCtx.Ctx, dbio.TypeFileS3, s3Props...)
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
func (conn *RedshiftConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
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

	// Prepare properties for S3 client
	s3Props := conn.getS3Props()
	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, s3Props...)
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

	// Close and re-establish connection to Redshift to avoid timeout
	connectTime := cast.ToTime(conn.GetProp("connect_time"))
	if time.Since(connectTime) > 10*time.Minute {
		g.Debug("re-establishing redshift connection before COPY command")
		conn.Close()
		err = conn.Connect()
		if err != nil {
			return df.Count(), g.Error(err, "error reconnecting to redshift before COPY command")
		}
	}

	_, err = conn.CopyFromS3(tableFName, s3Path, df.Columns)
	if err != nil {
		return df.Count(), g.Error(err, "error copying into redshift from s3")
	}

	return df.Count(), nil
}

// BulkImportStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *RedshiftConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

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

	sqlTempl := `
	delete from {tgt_table}
	using {src_table}
	where {src_tgt_pk_equal}
	;

	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from {src_table} src
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
func (conn *RedshiftConn) CopyFromS3(tableFName, s3Path string, columns iop.Columns) (count uint64, err error) {
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
		conn.WarnStlLoadErrors(err)
		return 0, g.Error(err)
	}

	return 0, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *RedshiftConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.Type != iop.TimestampzType && tgtCol.Type == iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.Type == iop.TimestampzType && tgtCol.Type != iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	default:
		selectStr = qName
	}

	return selectStr
}

func (conn *RedshiftConn) setEmptyAsNull(sql string) string {
	if !cast.ToBool(conn.GetProp("empty_as_null")) {
		sql = strings.ReplaceAll(sql, " EMPTYASNULL BLANKSASNULL", "")
	}
	return sql
}

func (conn *RedshiftConn) WarnStlLoadErrors(err error) {
	if strings.Contains(err.Error(), "stl_load_errors") {
		conn.Rollback()
		sql := conn.GetTemplateValue("core.stl_load_errors_check")
		data, _ := conn.Query(sql)
		if recs := data.Records(true); len(recs) == 1 {
			rec := g.M()
			for k, v := range recs[0] {
				rec[strings.TrimSpace(k)] = strings.TrimSpace(cast.ToString(v))
			}
			g.Warn("stl_load_errors => " + g.Marshal(rec))
			if strings.Contains(g.Marshal(rec), "String length exceeds DDL") {
				env.Println("")
				env.Println(env.MagentaString("If you'd like to dynamically multiply the DDL string length, try using `target_options.column_typing`"))
				env.Println("")
			}
		}
	}
}

func (conn *RedshiftConn) OptimizeTable(table *Table, newColumns iop.Columns, isTemp ...bool) (ok bool, err error) {
	// not supported.
	// when we attempt to create a temporary column and renaming,/
	// the COPY statement does not work.
	// need to drop the table and recreate it. TODO:
	return false, nil
}
