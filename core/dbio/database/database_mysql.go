package database

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"

	"github.com/slingdata-io/sling-cli/core/dbio"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/xo/dburl"
)

// MySQLConn is a MySQL or MariaDB connection
type MySQLConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *MySQLConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbMySQL
	conn.BaseConn.defaultPort = 3306

	if strings.HasPrefix(conn.URL, "mariadb://") {
		conn.BaseConn.Type = dbio.TypeDbMariaDB
	}

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	// InsertBatchStream is faster than LoadDataInFile
	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "false")
	}

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

	connURL = strings.Replace(connURL, "mariadb://", "mysql://", 1)
	u, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse MySQL URL")
		return connURL
	}

	// add parseTime
	u.Query().Add("parseTime", "true")

	return u.DSN
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
		g.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.StreamRows(sql)
	} else if runtime.GOOS == "windows" {
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
		g.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	} else if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// needs to get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
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
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")
	query := g.R(
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
			err = g.Error(
				err,
				fmt.Sprintf(
					"MySQL Export Command -> %s\nMySQL Export Error  -> %s",
					cmdStr, stderr.String(),
				),
			)
			conn.Context().CaptureErr(err)
			g.LogError(err, "could not export from MySQL")
		}
	}()

	return stdOutReader, err
}

// LoadDataInFile Bulk Import
func (conn *MySQLConn) LoadDataInFile(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")

	loadQuery := g.R(`LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;`, "table", tableFName)
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
	proc.Stdin = ds.NewCsvReader(0, 0)

	err = proc.Run()
	if err != nil {
		cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
		err = g.Error(
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

// UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/
// GenerateUpsertSQL generates the upsert SQL
func (conn *MySQLConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	srcT, err := ParseTableName(srcTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not generate parse srcTable")
		return
	}

	tgtT, err := ParseTableName(tgtTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not generate parse tgtTable")
		return
	}

	// replace src & tgt to make compatible to MariaDB
	// see https://github.com/slingdata-io/sling-cli/issues/135
	upsertMap["src_tgt_pk_equal"] = strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "src.", srcT.NameQ()+".")
	upsertMap["src_tgt_pk_equal"] = strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", tgtT.NameQ()+".")

	sqlTemplate := `
	DELETE FROM {tgt_table}
	WHERE EXISTS (
			SELECT 1
			FROM {src_table}
			WHERE {src_tgt_pk_equal}
	)
	;

	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
	`

	sql = g.R(
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

	return
}
