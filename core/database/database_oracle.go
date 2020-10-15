package database

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"

	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// OracleConn is a Postgres connection
type OracleConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *OracleConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = OracleDbType
	conn.BaseConn.defaultPort = 1521

	conn.SetProp("allow_bulk_import", "true")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	// set MAX_DECIMALS to import for numeric types
	os.Setenv("MAX_DECIMALS", "9")

	return conn.BaseConn.Init()
}

// BulkImportStream bulk import stream
func (conn *OracleConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("sqlldr")
	if err != nil {
		err = h.Error(err, "sqlldr not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	if conn.GetProp("allow_bulk_import") != "true" {
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

	// logic to insert rows with values containing new line chars
	// addFilePath is additional rows to be inserted
	countTot, err := conn.SQLLoad(tableFName, ds)
	if err != nil {
		return 0, h.Error(err, "Error with SQLLoad")
	}

	return countTot, nil
}

// SQLLoad uses sqlldr to Bulk Import
// cat test1.csv | sqlldr system/oracle@oracle.host:1521/xe control=sqlldr.ctl log=/dev/stdout bad=/dev/stderr
// cannot import when newline in value. Need to scan for new lines.
func (conn *OracleConn) SQLLoad(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = h.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	ctlPath := fmt.Sprintf(
		"/tmp/oracle.%d.%s.sqlldr.ctl",
		time.Now().Unix(),
		h.RandString(h.AlphaRunes, 3),
	)

	// write to ctlPath
	ctlStr := h.R(
		conn.BaseConn.GetTemplateValue("core.sqlldr"),
		"table", tableFName,
		"columns", conn.getColumnsString(ds),
	)
	err = ioutil.WriteFile(
		ctlPath,
		[]byte(ctlStr),
		0755,
	)
	if err != nil {
		err = h.Error(err, "Error writing to "+ctlPath)
		return
	}

	password, _ := url.User.Password()
	hostPort := url.Host
	sid := strings.ReplaceAll(url.Path, "/", "")
	credHost := fmt.Sprintf(
		"%s/%s@%s/%s", url.User.Username(),
		password, hostPort, sid,
	)

	proc := exec.Command(
		"sqlldr",
		credHost,
		"control="+ctlPath,
		"discardmax=0",
		"errors=0",
		"data=/dev/stdin",
		"log=/dev/stdout",
		"bad=/dev/stderr",
	)

	stdIn, pu := sqlLoadCsvReader(ds)
	proc.Stderr = &stderr
	proc.Stdout = &stdout
	proc.Stdin = stdIn

	// run and wait for finish
	cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), credHost, "****")
	h.Trace(cmdStr)
	err = proc.Run()

	// Delete ctrl file
	defer os.Remove(ctlPath)

	if err != nil {
		err = h.Error(
			err,
			fmt.Sprintf(
				"Oracle Import Command -> %s\nOracle Import Error  -> %s\n%s",
				cmdStr, stderr.String(), stdout.String(),
			),
		)
	}

	if ds.Err() != nil {
		return ds.Count, h.Error(ds.Err(), "context error")
	}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	setCols := []string{}
	for c := range pu.cols {
		col := ds.Columns[c]
		expr := fmt.Sprintf(
			`REPLACE(REPLACE(%s, chr(13)), '~/N/~', chr(10))`,
			conn.Quote(col.Name),
		)
		setCols = append(
			setCols, fmt.Sprintf(`%s = %s`, conn.Quote(col.Name), expr),
		)
	}

	// do update statement if needed
	if len(setCols) > 0 {
		setColsStr := strings.Join(setCols, ", ")
		sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr)
		_, err = conn.Exec(sql)
		if err != nil {
			err = h.Error(err, "could not apply post update query")
			return
		}
	}
	return ds.Count, err
}

func (conn *OracleConn) getColumnsString(ds *iop.Datastream) string {
	columnsString := ""
	for _, col := range ds.Columns {
		expr := ""
		if col.Type == "datetime" || col.Type == "date" {
			expr = fmt.Sprintf(
				`"TO_DATE(:%s, 'YYYY-MM-DD HH24:MI:SS')"`,
				strings.ToUpper(col.Name),
			)
		} else if col.Type == "timestamp" {
			expr = fmt.Sprintf(
				`"TO_TIMESTAMP(:%s, 'YYYY-MM-DD HH24:MI:SS.FF3')"`,
				strings.ToUpper(col.Name),
			)
		} else if col.IsString() {
			// expr = fmt.Sprintf(
			// 	`"REPLACE(REPLACE(:%s, chr(13)), '~/N/~', chr(10))"`,
			// 	strings.ToUpper(col.Name),
			// )
		}
		columnsString += fmt.Sprintf("  %s %s,\n", col.Name, expr)
	}
	return strings.TrimSuffix(columnsString, ",\n")
}

// sqlLoadCsvReader creates a Reader with with a newline checker
// for SQLoad.
func sqlLoadCsvReader(ds *iop.Datastream) (*io.PipeReader, *struct{ cols map[int]int }) {
	pu := &struct{ cols map[int]int }{map[int]int{}}
	pipeR, pipeW := io.Pipe()

	go func() {
		c := uint64(0) // local counter
		w := csv.NewWriter(pipeW)

		err := w.Write(ds.GetFields())
		if err != nil {
			ds.Context.CaptureErr(h.Error(err, "Error writing ds.Fields"))
			ds.Context.Cancel()
			pipeW.Close()
		}

		for row0 := range ds.Rows {
			c++
			// convert to csv string
			row := make([]string, len(row0))
			for i, val := range row0 {
				if val == nil {
					row[i] = ""
					continue
				}

				valS := cast.ToString(val)
				if strings.Contains(valS, "\n") {
					valS = strings.ReplaceAll(valS, "\r", "")
					valS = strings.ReplaceAll(valS, "\n", `~/N/~`)
					pu.cols[i] = i
				}

				if ds.Columns[i].Type == "datetime" || ds.Columns[i].Type == "date" {
					// casting unsafely, but has been determined by ParseString
					// convert to Oracle Time format
					val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
					valS = val.(time.Time).Format("2006-01-02 15:04:05")
				} else if ds.Columns[i].Type == "timestamp" {
					// convert to Oracle Timestamp format
					val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
					valS = val.(time.Time).Format("2006-01-02 15:04:05.000")
				}
				row[i] = valS
			}

			err := w.Write(row)
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "Error w.Write(row)"))
				ds.Context.Cancel()
				break
			}
			w.Flush()

		}
		ds.SetEmpty()

		pipeW.Close()
	}()

	return pipeR, pu
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *OracleConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

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
	if err != nil && !strings.Contains(err.Error(), "already used") {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, indexSQL)
		return
	}

	sqlTempl := `
	MERGE INTO {tgt_table} tgt
	USING (SELECT * FROM {src_table}) src
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

// GenerateInsertStatement returns the proper INSERT statement
func (conn *OracleConn) GenerateInsertStatement(tableName string, fields []string, numRows int) string {

	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	intos := []string{}
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, strings.ReplaceAll(field, `"`, ""), n, c)
			qFields[i] = conn.Self().Quote(field)
		}

		// for Oracle
		intos = append(intos, h.R(
			"INTO {table} ({fields}) VALUES ({values})",
			"table", tableName,
			"fields", strings.Join(qFields, ", "),
			"values", strings.Join(values, ","),
		))
	}

	statement := h.R(
		`INSERT ALL {intosStr} SELECT 1 FROM DUAL`,
		"intosStr", strings.Join(intos, "\n"),
	)
	return statement
}
