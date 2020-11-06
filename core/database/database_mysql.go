package database

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os/exec"
	"strings"

	h "github.com/flarco/g"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/xo/dburl"
)

// MySQLConn is a Postgres connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MySQLConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = MySQLDbType
	conn.BaseConn.defaultPort = 3306

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	// InsertBatchStream is faster than LoadDataInFile
	conn.BaseConn.SetProp("allow_bulk_import", "false")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *MySQLConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	u, err := dburl.Parse(connURL)
	if err != nil {
		h.LogError(err, "could not parse MySQL URL")
		return connURL
	}

	// Add tcp explicitly...
	// https://github.com/go-sql-driver/mysql/issues/427#issuecomment-474034276
	URL := strings.ReplaceAll(
		connURL,
		"@"+u.Host,
		fmt.Sprintf("@tcp(%s)", u.Host),
	)

	// remove scheme
	URL = strings.ReplaceAll(
		URL,
		"mysql://",
		"",
	)

	// add parseTime
	if strings.Contains(URL, "?") {
		URL = URL + "&parseTime=true"
	} else {
		URL = URL + "?parseTime=true"
	}

	return URL
}

// BulkInsert
// Common Error: ERROR 3948 (42000) at line 1: Loading local data is disabled; this must be enabled on both the client and server sides
// Need to enable on serer side: https://stackoverflow.com/a/60027776
// mysql server needs to be launched with '--local-infile=1' flag
// mysql --local-infile=1 -h {host} -P {port} -u {user} -p{password} mysql -e "LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"

// BulkExportStream bulk Export
func (conn *MySQLConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		h.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.StreamRows(sql)
	}

	if conn.BaseConn.GetProp("allow_bulk_export") != "true" {
		return conn.BaseConn.StreamRows(sql)
	}

	stdOutReader, err := conn.LoadDataOutFile(sql)
	if err != nil {
		return ds, err
	}

	csv := iop.CSV{Reader: stdOutReader}
	ds, err = csv.ReadStream()

	return ds, err
}

// BulkImportStream bulk import stream
func (conn *MySQLConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		h.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	if conn.BaseConn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// needs to get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = h.Error(err, "could not shape stream")
		return
	}

	return conn.LoadDataInFile(tableFName, ds)
}

// LoadDataOutFile Bulk Export
// Possible error: ERROR 1227 (42000) at line 1: Access denied; you need (at least one of) the FILE privilege(s) for this operation
// File privilege needs to be granted to user
// also the --secure-file-priv option needs to be set properly for it to work.
// https://stackoverflow.com/questions/9819271/why-is-mysql-innodb-insert-so-slow to improve innodb insert speed
func (conn *MySQLConn) LoadDataOutFile(sql string) (stdOutReader io.Reader, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = h.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")
	query := h.R(
		`{sql} INTO OUTFILE '/dev/stdout'
		FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '"'
		LINES TERMINATED BY '\r\n'`,
		"sql", sql,
	)
	proc := exec.Command(
		"mysql",
		// "--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", strings.ReplaceAll(query, "\n", " "),
	)
	proc.Stderr = &stderr
	stdOutReader, err = proc.StdoutPipe()

	go func() {
		err := proc.Run()
		if err != nil {
			// bytes, _ := proc.CombinedOutput()
			cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
			err = h.Error(
				err,
				fmt.Sprintf(
					"MySQL Export Command -> %s\nMySQL Export Error  -> %s",
					cmdStr, stderr.String(),
				),
			)
			conn.Context().CaptureErr(err)
			h.LogError(err, "could not export from MySQL")
		}
	}()

	return stdOutReader, err
}

// LoadDataInFile Bulk Import
func (conn *MySQLConn) LoadDataInFile(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = h.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")

	loadQuery := h.R(`LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;`, "table", tableFName)
	proc := exec.Command(
		"mysql",
		"--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", loadQuery,
	)

	proc.Stderr = &stderr
	proc.Stdin = ds.NewCsvReader(0)

	err = proc.Run()
	if err != nil {
		cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
		err = h.Error(
			err,
			fmt.Sprintf(
				"MySQL Import Command -> %s\nMySQL Import Error  -> %s",
				cmdStr, stderr.String(),
			),
		)
		return ds.Count, err
	}

	return ds.Count, nil
}

//UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *MySQLConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = h.Error(err, "could not generate upsert variables")
		return
	}

	indexSQL := h.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", tgtTable,
		"cols", strings.Join(pkFields, ", "),
	)

	txn, err := conn.Db().BeginTx(conn.Context().Ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
	if err != nil {
		err = h.Error(err, "Could not begin transaction for upsert")
		return
	}
	_, err = txn.ExecContext(conn.Context().Ctx, indexSQL)
	if err != nil && !strings.Contains(err.Error(), "Duplicate") {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, indexSQL)
		return
	}

	sqlTemplate := `
	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	from {src_table} src
	ON DUPLICATE KEY UPDATE
		{set_fields}
	`

	sql := h.R(
		sqlTemplate,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", upsertMap["set_fields"],
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
	}

	err = txn.Commit()
	if err != nil {
		err = h.Error(err, "Could not commit upsert transaction")
		return
	}

	return
}
