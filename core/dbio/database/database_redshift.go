package database

import (
	"fmt"
	"strings"

	"github.com/slingdata-io/sling-cli/core/dbio"

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
		colNames := quoteColNames(conn, keyCols.Names())
		distKey = g.F("distkey(%s)", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{dist_key}", distKey)

	sortKey := ""
	if keyCols := data.Columns.GetKeys(iop.SortKey); len(keyCols) > 0 {
		colNames := quoteColNames(conn, keyCols.Names())
		sortKey = g.F("compound sortkey(%s)", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{sort_key}", sortKey)

	return strings.TrimSpace(sql), nil
}

// Unload unloads a query to S3
func (conn *RedshiftConn) Unload(ctx *g.Context, tables ...Table) (s3Path string, err error) {

	if conn.GetProp("AWS_BUCKET") == "" {
		return "", g.Error("need to set AWS_BUCKET")
	}

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")

	g.Info("unloading from redshift to s3")
	unload := func(table Table, s3PathPart string) {

		defer conn.Context().Wg.Write.Done()

		sql := strings.ReplaceAll(strings.ReplaceAll(table.Select(0), "\n", " "), "'", "''")

		unloadSQL := g.R(
			conn.template.Core["copy_to_s3"],
			"sql", sql,
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
			"parallel", conn.GetProp("PARALLEL"),
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			cleanSQL := strings.ReplaceAll(unloadSQL, AwsID, "*****")
			cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
			err = g.Error(err, fmt.Sprintf("SQL Error for %s:\n%s", s3PathPart, cleanSQL))
			ctx.CaptureErr(err)
		}

	}

	s3Fs, err := filesys.NewFileSysClient(dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Unable to create S3 Client")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", conn.GetProp("AWS_BUCKET"), filePathStorageSlug, cast.ToString(g.Now()))

	filesys.Delete(s3Fs, s3Path)
	for i, table := range tables {
		s3PathPart := fmt.Sprintf("%s/u%02d-", s3Path, i+1)
		conn.Context().Wg.Write.Add()
		go unload(table, s3PathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

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
func (conn *RedshiftConn) BulkExportFlow(tables ...Table) (df *iop.Dataflow, err error) {
	if len(tables) == 0 {
		return df, g.Error("no table/query provided")
	} else if conn.GetProp("AWS_BUCKET") == "" {
		g.Debug("using cursor to export. Please set AWS creds for Sling to use to UNLOAD function (for bigger datasets). See https://docs.slingdata.io/connections/database-connections/redshift")
		return conn.BaseConn.BulkExportFlow(tables...)
	}

	columns, err := conn.GetSQLColumns(tables[0])
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	unloadCtx := g.NewContext(conn.Context().Ctx)
	s3Path, err := conn.Unload(&unloadCtx, tables...)
	if err != nil {
		err = g.Error(err, "Could not unload.")
		return
	}

	fs, err := filesys.NewFileSysClientContext(unloadCtx.Ctx, dbio.TypeFileS3, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	fs.SetProp("header", "false")
	fs.SetProp("format", "csv")
	fs.SetProp("columns", g.Marshal(columns))
	fs.SetProp("metadata", conn.GetProp("metadata"))
	df, err = fs.ReadDataflow(s3Path)
	if err != nil {
		err = g.Error(err, "Could not read S3 Path for UNLOAD: "+s3Path)
		return
	}
	df.MergeColumns(columns, true) // overwrite types so we don't need to infer
	df.Defer(func() { filesys.Delete(fs, s3Path) })

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
		filePathStorageSlug,
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

	df.Defer(func() { filesys.Delete(s3Fs, s3Path) }) // cleanup

	g.Info("writing to s3 for redshift import")
	s3Fs.SetProp("null_as", `\N`)
	bw, err := s3Fs.WriteDataflow(df, s3Path)
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
	DELETE FROM {tgt_table}
	USING {src_table}
	WHERE {src_tgt_pk_equal}
	;

	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
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
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	tgtColumns := quoteColNames(conn, columns.Names())

	g.Debug("copying into redshift from s3")
	g.Debug("url: " + s3Path)
	sql := g.R(
		conn.template.Core["copy_from_s3"],
		"tgt_table", tableFName,
		"tgt_columns", strings.Join(tgtColumns, ", "),
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
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
		selectStr = g.F("%s::%s as %s", qName, tgtCol.DbType, qName)
	case srcCol.Type == iop.TimestampzType && tgtCol.Type != iop.TimestampzType:
		selectStr = g.F("%s::%s as %s", qName, tgtCol.DbType, qName)
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
