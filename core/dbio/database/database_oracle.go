package database

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
	"github.com/xo/dburl"

	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
)

// OracleConn is a Postgres connection
type OracleConn struct {
	BaseConn
	URL     string
	version string
}

/*
Version	Initial Release	Date
11.2 (11g Release 2)	11.2.0.1	2009
12.1 (12c Release 1)	12.1.0.1	2013
12.2 (12c Release 2)	12.2.01	2016
18 (18c)	18.3.0	2018
19 (19c)	19.3.0	2019
21 (21c)	21.3.0	2021
*/

// Init initiates the object
func (conn *OracleConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbOracle
	conn.BaseConn.defaultPort = 1521
	conn.version = "11"

	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.SetProp("allow_bulk_import", "true")
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

func (conn *OracleConn) Version() int {
	parts := strings.Split(conn.version, ".")
	if len(parts) > 0 {
		v := cast.ToInt(parts[0])
		if v == 0 {
			v = 11
		}
		return v
	}
	return 11
}

// setTypeMap adjusts the type map depending on the version
func (conn *OracleConn) setTypeMap() {
	if conn.Version() >= 20 {
		conn.template.GeneralTypeMap["json"] = "json"
	}
}

func (conn *OracleConn) Connect(timeOut ...int) (err error) {
	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}

	// get version
	data, err := conn.Query(`select version from product_component_version` + noDebugKey)
	if err != nil {
		conn.version = "11.0"
	} else if len(data.Rows) > 0 {
		conn.version = cast.ToString(data.Rows[0][0])
	}

	conn.setTypeMap()

	return nil
}

func (conn *OracleConn) ConnString() string {

	propMapping := map[string]string{
		"sid":               "SID",
		"jdbc_str":          "connStr",
		"ssl":               "ssl",
		"ssl_verify":        "ssl verify",
		"wallet":            "wallet",
		"auth_type":         "AUTH TYPE",
		"auth_serv":         "AUTH SERV",
		"os_user":           "OS USER",
		"os_password":       "OS PASS",
		"domain":            "DOMAIN",
		"encryption":        "encryption",
		"data_integrity":    "data integrity",
		"unix_socket":       "unix socket",
		"timeout":           "TIMEOUT",
		"proxy_client_name": "proxy client name",
		"dba_privilege":     "dba privilege",
		"lob_fetch":         "lob fetch",
		"client_charset":    "client charset",
		"language":          "language",
		"territory":         "territory",
		"trace_file":        "trace file",
	}

	// infinite timeout by default
	options := map[string]string{"TIMEOUT": "0"}

	for key, new_key := range propMapping {
		if val := conn.GetProp(key); val != "" {
			options[new_key] = val
		}
	}

	connStr := go_ora.BuildUrl(
		conn.GetProp("host"),
		cast.ToInt(conn.GetProp("port")),
		conn.GetProp("service_name"),
		conn.GetProp("username"),
		conn.GetProp("password"),
		options,
	)

	if tns := conn.GetProp("tns"); tns != "" {
		connStr = go_ora.BuildJDBC(conn.GetProp("username"), conn.GetProp("password"), tns, options)
	}

	return connStr
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (conn *OracleConn) ExecMultiContext(ctx context.Context, qs ...string) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	eG := g.ErrorGroup{}

	for _, q := range qs {
		q2 := strings.TrimRight(strings.TrimSpace(strings.ToLower(q)), ";")
		cond1 := strings.HasPrefix(q2, "begin") && strings.HasSuffix(q2, "end")
		cond2 := strings.Contains(q2, "execute immediate")
		if cond1 || cond2 {
			return conn.Self().ExecContext(ctx, q)
		}

		for _, sql := range ParseSQLMultiStatements(q) {
			sql := strings.TrimSuffix(sql, ";")
			res, err := conn.Self().ExecContext(ctx, sql)
			if err != nil {
				eG.Capture(g.Error(err, "Error executing query"))
			} else {
				ra, _ := res.RowsAffected()
				g.Trace("RowsAffected: %d", ra)
				Res.rowsAffected = Res.rowsAffected + ra
			}
		}
	}

	err = eG.Err()
	result = Res

	return
}

func (conn *OracleConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	columns, err = conn.BaseConn.GetTableColumns(table, fields...)
	if err != nil {
		// try synonym
		conn.SetProp("get_synonym", "true")
		columns, err = conn.BaseConn.GetTableColumns(table, fields...)
		conn.SetProp("get_synonym", "false")
	}
	return
}

func (conn *OracleConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	ddl = strings.TrimSpace(ddl)

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	for _, index := range table.Indexes(data.Columns) {
		ddl = strings.ReplaceAll(
			ddl,
			"EXCEPTION",
			g.F("EXECUTE IMMEDIATE '%s';\nEXCEPTION", index.CreateDDL()),
		)
	}

	return ddl, nil
}

func (conn *OracleConn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error) {
	if cast.ToBool(conn.GetProp("get_synonym")) && name == "columns" {
		name = "columns_synonym"
	}
	return conn.BaseConn.SubmitTemplate(level, templateMap, name, values)
}

func (conn *OracleConn) sqlldrPath() string {
	if val := conn.GetProp("sqlldr_path"); val != "" {
		return val
	}
	return "sqlldr"
}

// BulkImportStream bulk import stream
func (conn *OracleConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath(conn.sqlldrPath())
	if err != nil {
		g.Debug("sqlldr not found in path. Using cursor...")
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

	// logic to insert rows with values containing new line chars
	// addFilePath is additional rows to be inserted
	countTot, err := conn.SQLLoad(tableFName, ds)
	if err != nil {
		return 0, g.Error(err, "Error with SQLLoad")
	}

	return countTot, nil
}

// SQLLoad uses sqlldr to Bulk Import
// cat test1.csv | sqlldr system/oracle@oracle.host:1521/xe control=sqlldr.ctl log=/dev/stdout bad=/dev/stderr
// cannot import when newline in value. Need to scan for new lines.
func (conn *OracleConn) SQLLoad(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer

	connURL := conn.ConnString()
	if su := conn.GetProp("ssh_url"); su != "" {
		connURL = su // use ssh url if specified
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	// write to ctlPath
	ctlPath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("oracle.%s.sqlldr", env.CleanTableName(tableFName)))+".ctl")
	ctlStr := g.R(
		conn.BaseConn.GetTemplateValue("core.sqlldr"),
		"table", tableFName,
		"columns", conn.getColumnsString(ds),
	)
	err = os.WriteFile(
		ctlPath,
		[]byte(ctlStr),
		0755,
	)
	if err != nil {
		err = g.Error(err, "Error writing to "+ctlPath)
		return
	}

	g.Debug("sqlldr ctl file content (%s):\n%s", ctlPath, ctlStr)

	password, _ := url.User.Password()
	hostPort := url.Host
	sid := strings.ReplaceAll(url.Path, "/", "")
	credHost := fmt.Sprintf(
		"%s/%s@%s/%s", url.User.Username(),
		password, hostPort, sid,
	)

	dataPath := "/dev/stdin"
	logPath := "/dev/stdout"

	// the columns that need post-updates
	postUpdates := cmap.New[int]()

	if runtime.GOOS == "windows" {
		dataPath = path.Join(env.GetTempFolder(), g.NewTsID(g.F("oracle.%s", env.CleanTableName(tableFName)))+".temp.csv")
		logPath = path.Join(env.GetTempFolder(), g.NewTsID(g.F("oracle.%s", env.CleanTableName(tableFName)))+".log")

		file, err := os.Create(dataPath)
		if err != nil {
			err = g.Error(err, "could not create temp file")
			return 0, err
		}

		g.Debug("writing to temp csv file: %s", dataPath)
		err = conn.writeCsv(ds, file, &postUpdates)
		if err != nil {
			err = g.Error(err, "could not write to temp file")
			return 0, err
		}

		defer func() {
			file.Close()
			env.RemoveLocalTempFile(dataPath)
		}()

		defer func() { env.RemoveLocalTempFile(logPath) }()
	}

retry:
	proc := exec.Command(
		conn.sqlldrPath(),
		"'"+credHost+"'",
		"control="+ctlPath,
		"discardmax=0",
		"errors=0",
		"data="+dataPath,
		"log="+logPath,
		"bad="+logPath,
	)

	ds.SetConfig(conn.Props())
	proc.Stderr = &stderr
	proc.Stdout = &stdout

	if runtime.GOOS != "windows" {
		proc.Stdin = conn.sqlLoadCsvReader(ds, &postUpdates)
	}

	// run and wait for finish
	cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), credHost, "****")
	g.Debug(cmdStr)
	err = proc.Run()

	// Delete ctrl file
	defer func() { env.RemoveLocalTempFile(ctlPath) }()

	if err != nil {
		if strings.Contains(err.Error(), "text file busy") {
			g.Warn("could not start sqlldr (%s), retrying...", err.Error())
			time.Sleep(1 * time.Second)
			goto retry
		}

		err = g.Error(
			err,
			fmt.Sprintf(
				"Oracle Import Command:\n%s\n\nControl File:\n%s\n\nOracle Import Error:%s\n%s",
				cmdStr, ctlStr, stderr.String(), stdout.String(),
			),
		)
		return ds.Count, err
	}

	if ds.Err() != nil {
		return ds.Count, g.Error(ds.Err(), "context error")
	}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	setCols := []string{}
	for _, c := range postUpdates.Items() {
		col := ds.Columns[c]
		colName := conn.Quote(col.Name)
		expr := fmt.Sprintf(
			`REPLACE(REPLACE(%s, chr(13)), '~/N/~', chr(10))`,
			conn.Quote(colName),
		)
		setCols = append(
			setCols, fmt.Sprintf(`%s = %s`, conn.Quote(colName), expr),
		)
	}

	// do update statement if needed
	if len(setCols) > 0 {
		setColsStr := strings.Join(setCols, ", ")
		sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr)
		_, err = conn.Exec(sql)
		if err != nil {
			err = g.Error(err, "could not apply post update query")
			return
		}
	}
	return ds.Count, err
}

func (conn *OracleConn) getColumnsString(ds *iop.Datastream) string {
	columnsString := ""
	for _, col := range ds.Columns {
		expr := ""
		colName := conn.Quote(col.Name)
		colNameEscaped := strings.ReplaceAll(colName, `"`, `\"`)
		if col.Type == iop.DateType {
			expr = fmt.Sprintf(
				`"TO_DATE(:%s, 'YYYY-MM-DD HH24:MI:SS')"`,
				colNameEscaped,
			)
		} else if col.Type == iop.DatetimeType || col.Type == iop.TimestampType {
			expr = fmt.Sprintf(
				`"TO_TIMESTAMP(:%s, 'YYYY-MM-DD HH24:MI:SS.FF6')"`,
				colNameEscaped,
			)
		} else if col.Type == iop.TimestampzType {
			expr = fmt.Sprintf(
				`"TO_TIMESTAMP_TZ(:%s, 'YYYY-MM-DD HH24:MI:SS.FF6 TZH:TZM')"`,
				colNameEscaped,
			)
		} else if col.IsString() {
			expr = g.F("char(400000) NULLIF %s=BLANKS", colName)
			if col.DbPrecision > 400000 {
				expr = g.F("char(%d) NULLIF %s=BLANKS", col.DbPrecision, colName)
			}
		}
		columnsString += fmt.Sprintf("  %s %s,\n", colName, expr)
	}
	return strings.TrimSuffix(columnsString, ",\n")
}

// sqlLoadCsvReader creates a Reader with with a newline checker
// for SQLoad.
func (conn *OracleConn) sqlLoadCsvReader(ds *iop.Datastream, pu *cmap.ConcurrentMap[string, int]) (r *io.PipeReader) {
	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()
		err := conn.writeCsv(ds, pipeW, pu)
		if err != nil {
			return
		}
	}()

	return pipeR
}

func (conn *OracleConn) writeCsv(ds *iop.Datastream, writer io.Writer, pu *cmap.ConcurrentMap[string, int]) (err error) {

	w := csv.NewWriter(writer)

	_, err = w.Write(ds.Columns.Names())
	if err != nil {
		ds.Context.CaptureErr(g.Error(err, "Error writing ds.Fields"))
		ds.Context.Cancel()
		return
	}

	for row0 := range ds.Rows() {
		// convert to csv string
		row := make([]string, len(row0))
		for i, val := range row0 {
			if val == nil {
				row[i] = ""
				continue
			}

			valS := ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
			if strings.Contains(valS, "\n") {
				valS = strings.ReplaceAll(valS, "\r", "")
				valS = strings.ReplaceAll(valS, "\n", `~/N/~`)
				pu.Set(cast.ToString(i), i)
			}

			if ds.Columns[i].Type == iop.DateType {
				// casting unsafely, but has been determined by ParseString
				// convert to Oracle Time format
				val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
				valS = val.(time.Time).Format("2006-01-02 15:04:05")
			} else if ds.Columns[i].Type == iop.DatetimeType || ds.Columns[i].Type == iop.TimestampType {
				// convert to Oracle Timestamp format
				val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
				valS = val.(time.Time).Format("2006-01-02 15:04:05.000000")
			} else if ds.Columns[i].Type == iop.TimestampzType {
				// convert to Oracle Timestamp format
				val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
				valS = val.(time.Time).Format("2006-01-02 15:04:05.000000 -07:00")
			}
			row[i] = valS
		}

		_, err = w.Write(row)
		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error w.Write(row)"))
			ds.Context.Cancel()
			return
		}
		w.Flush()

	}

	return
}

// GenerateMergeSQL generates the upsert SQL
func (conn *OracleConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateMergeExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	merge into {tgt_table} tgt
	using (select * from {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) values  ({src_fields})
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *OracleConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	fields := cols.Names()
	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	intos := []string{}
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			valField := strings.TrimPrefix(strings.ReplaceAll(field, `"`, ""), "_") // cannot start with "_"
			if len(valField) > 28 {
				valField = valField[:28]
			}
			values[i] = conn.bindVar(i+1, valField, n, c)
			qFields[i] = conn.Self().Quote(field)
		}

		// for Oracle
		intos = append(intos, g.R(
			"INTO {table} ({fields}) values  ({values})",
			"table", tableName,
			"fields", strings.Join(qFields, ", "),
			"values", strings.Join(values, ","),
		))
	}

	g.Trace("Count of Bind Vars: %d", c)
	statement := g.R(
		`INSERT ALL {intosStr} select 1 from DUAL`,
		"intosStr", strings.Join(intos, "\n"),
	)
	return statement
}

// CastColumnForSelect casts to the correct target column type
func (conn *OracleConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)
	srcDbType := strings.ToLower(srcCol.DbType)
	tgtDbType := strings.ToLower(tgtCol.DbType)
	tgtCol.DbPrecision = lo.Ternary(tgtCol.DbPrecision == 0, 4000, tgtCol.DbPrecision)

	switch {
	case srcDbType != "clob" && tgtDbType == "clob":
		selectStr = g.F("to_clob(%s)", qName)
	case srcDbType == "clob" && tgtCol.IsString() && tgtDbType != "clob":
		selectStr = g.F("cast(%s as varchar2(%d))", qName, tgtCol.DbPrecision)
	default:
		selectStr = qName
	}

	return selectStr
}
