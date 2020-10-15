package database

import (
	"errors"
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	h "github.com/flarco/gutil"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling/core/iop"
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
	conn.BaseConn.Type = RedshiftDbType
	conn.BaseConn.defaultPort = 5439

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
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

// Unload unloads a query to S3
func (conn *RedshiftConn) Unload(sqls ...string) (s3Path string, err error) {

	if conn.GetProp("AWS_BUCKET") == "" {
		return "", h.Error(err, "need to set AWS_BUCKET")
	}

	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")

	h.Info("unloading from redshift to s3")
	unload := func(sql string, s3PathPart string) {

		defer conn.Context().Wg.Write.Done()

		sql = strings.ReplaceAll(strings.ReplaceAll(sql, "\n", " "), "'", "''")

		unloadSQL := h.R(
			conn.template.Core["copy_to_s3"],
			"sql", sql,
			"s3_path", s3PathPart,
			"aws_access_key_id", AwsID,
			"aws_secret_access_key", AwsAccessKey,
			"parallel", conn.GetProp("SLING_PARALLEL"),
		)
		_, err = conn.Exec(unloadSQL)
		if err != nil {
			cleanSQL := strings.ReplaceAll(unloadSQL, AwsID, "*****")
			cleanSQL = strings.ReplaceAll(cleanSQL, AwsAccessKey, "*****")
			err = h.Error(err, fmt.Sprintf("SQL Error for %s:\n%s", s3PathPart, cleanSQL))
			conn.Context().CaptureErr(err)
		}

	}

	s3Fs, err := iop.NewFileSysClient(iop.S3FileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Unable to create S3 Client")
		return
	}

	s3Path = fmt.Sprintf("s3://%s/%s/stream/%s.csv", conn.GetProp("AWS_BUCKET"), filePathStorageSlug, cast.ToString(h.Now()))

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

// BulkExportStream reads in bulk
func (conn *RedshiftConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {

	df, err := conn.BulkExportFlow(sql)
	if err != nil {
		return ds, h.Error(err, "Could not export: \n"+sql)
	}

	return iop.MergeDataflow(df), nil
}

// BulkExportFlow reads in bulk
func (conn *RedshiftConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	columns, err := conn.GetSQLColumns(sqls...)
	if err != nil {
		err = h.Error(err, "Could not get columns.")
		return
	}

	s3Path, err := conn.Unload(sqls...)
	if err != nil {
		err = h.Error(err, "Could not unload.")
		return
	}

	fs, err := iop.NewFileSysClient(iop.S3FileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	df, err = fs.ReadDataflow(s3Path)
	if err != nil {
		err = h.Error(err, "Could not read "+s3Path)
		return
	}
	df.SetColumns(columns)
	df.Inferred = true
	df.Defer(func() { fs.Delete(s3Path) })

	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)
	if conn.GetProp("AWS_BUCKET") == "" {
		return count, h.Error(errors.New("Need to set 'AWS_BUCKET' to copy to redshift"))
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

	h.Info("writing to s3 for redshift import")
	bw, err := s3Fs.WriteDataflow(df, s3Path)
	if err != nil {
		return df.Count(), h.Error(err, "error writing to s3")
	}
	h.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), s3Path)

	_, err = conn.CopyFromS3(tableFName, s3Path)
	if err != nil {
		return df.Count(), h.Error(err, "error copygin into redshift from s3")
	}

	return df.Count(), nil
}

// BulkImportStream inserts a stream into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *RedshiftConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = h.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *RedshiftConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = h.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	DELETE FROM {tgt_table}
	USING {src_table}
	WHERE {src_tgt_pk_equal}
	`
	srcTgtPkEqual := strings.ReplaceAll(
		upsertMap["src_tgt_pk_equal"], "src.", srcTable+".",
	)
	srcTgtPkEqual = strings.ReplaceAll(
		srcTgtPkEqual, "tgt.", tgtTable+".",
	)
	sql := h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", srcTgtPkEqual,
	)

	txn, err := conn.Db().Begin()
	if err != nil {
		err = h.Error(err, "Could not begin transaction for upsert")
		return
	}

	_, err = txn.ExecContext(conn.Context().Ctx, sql)
	if err != nil {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	sqlTempl = `
	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
	`
	sql = h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)
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

// CopyFromS3 uses the COPY INTO Table command from AWS S3
func (conn *RedshiftConn) CopyFromS3(tableFName, s3Path string) (count uint64, err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = h.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3"))
		return
	}

	h.Info("copying into redshift from s3")
	h.Debug("url: " + s3Path)
	sql := h.R(
		conn.template.Core["copy_from_s3"],
		"tgt_table", tableFName,
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return 0, h.Error(err, "SQL Error:\n"+conn.CleanSQL(sql))
	}

	return 0, nil
}

// CopyDirect copies directly from cloud files
// (without passing through sling)
func (conn *RedshiftConn) CopyDirect(tableFName string, srcFile iop.DataConn) (cnt uint64, ok bool, err error) {
	props := h.MapToKVArr(srcFile.VarsS())
	fs, err := iop.NewFileSysClientFromURL(srcFile.URL, props...)
	if err != nil {
		err = h.Error(err, "Could not obtain client for: "+srcFile.URL)
		return
	}

	switch fs.FsType() {
	case iop.S3FileSys:
		ok = true
		cnt, err = conn.CopyFromS3(tableFName, srcFile.URL)
		if err != nil {
			err = h.Error(err, "could not load into database from S3")
		}
	}

	if err != nil {
		// ok = false // try through sling?
	}
	return
}
