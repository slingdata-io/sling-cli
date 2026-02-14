package database

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	"github.com/xo/dburl"
)

// StarRocksConn is a StarRocks connection
type StarRocksConn struct {
	BaseConn
	URL     string
	fePort  string
	version int
}

// Init initiates the object
func (conn *StarRocksConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbStarRocks
	conn.BaseConn.defaultPort = 9030

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	// InsertBatchStream is faster than LoadDataInFile
	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "false")
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

func (conn *StarRocksConn) Connect(timeOut ...int) (err error) {
	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}

	// get version
	data, err := conn.Query(`select @@version_comment` + noDebugKey)
	if err == nil && len(data.Rows) > 0 {
		version := cast.ToString(data.Rows[0][0])
		// convert to 3 digit int (3.2.3-a40e2f8 => 323)
		version = strings.Split(version, "-")[0]
		versionParts := strings.Split(version, ".")
		if len(versionParts) >= 3 {
			major := cast.ToInt(versionParts[0])
			minor := cast.ToInt(versionParts[1])
			patch := cast.ToInt(versionParts[2])
			conn.version = major*100 + minor*10 + patch
			g.Debug("starrocks version => %s (%d)", version, conn.version)
		}
	}

	return nil
}

// GetURL returns the processed URL
func (conn *StarRocksConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	connURL = strings.Replace(connURL, "starrocks://", "mysql://", 1)
	u, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse StarRocks URL")
		return connURL
	}

	// add parseTime
	u.Query().Add("parseTime", "true")

	return u.DSN
}

// NewTransaction creates a new transaction
func (conn *StarRocksConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// transactions are BETA in 3.5. only inserts are supported in 3.5, let's disable for now
	if conn.version >= 350 {
		g.Debug("transactions are in BETA in starrocks 3.5, disabling")
		return nil, nil
	}
	return conn.BaseConn.NewTransaction(ctx, options...)
}

func (conn *StarRocksConn) WaitAlterTable(table Table) (err error) {
	sql := g.R(conn.GetTemplateValue("core.show_alter_table"),
		"schema", table.Schema,
		"table", table.Name,
	)

	g.Debug("waiting for schema change to finish")
	for {
		time.Sleep(1 * time.Second)
		data, err := conn.Query(sql + noDebugKey)
		if err != nil {
			return g.Error(err, "could not fetch 'SHOW ALTER TABLE' to get schema change status.")
		} else if len(data.Rows) == 0 {
			return g.Error("did not get results from 'SHOW ALTER TABLE' to get schema change status.")
		}

		print(".")

		if g.In(cast.ToString(data.Records()[0]["state"]), "FINISHED", "CANCELLED") {
			break
		}
	}

	println("")

	return
}

func (conn *StarRocksConn) AddMissingColumns(table Table, newCols iop.Columns) (ok bool, err error) {
	ok, err = conn.BaseConn.AddMissingColumns(table, newCols)
	if err != nil {
		return
	}

	if ok {
		err = conn.WaitAlterTable(table)
		if err != nil {
			return ok, g.Error(err, "error while waiting for schema change")
		}
	}

	return
}

func (conn *StarRocksConn) OptimizeTable(table *Table, newColumns iop.Columns, isTemp ...bool) (ok bool, err error) {
	IsTemp := false
	if len(isTemp) > 0 {
		IsTemp = isTemp[0]
	}

	ok, ddlParts, err := GetOptimizeTableStatements(conn, table, newColumns, IsTemp)
	if err != nil {
		return
	}

	for _, ddlPart := range ddlParts {
		for _, sql := range ParseSQLMultiStatements(ddlPart) {
			_, err := conn.ExecMulti(sql)
			if err != nil {
				return ok, g.Error(err)
			}

			if strings.Contains(strings.ToLower(sql), "alter table") {
				err = conn.WaitAlterTable(*table)
				if err != nil {
					return ok, g.Error(err, "error while waiting for schema change")
				}
			}
		}
	}

	return
}

// ExecContext runs a sql query with context, returns `error`
func (conn *StarRocksConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
retry:
	result, err = conn.BaseConn.ExecContext(ctx, q, args...)
	if err != nil {
		if strings.Contains(err.Error(), "A schema change operation is in progress") {
			g.Debug("%s. Retrying in 5 sec.", err.Error())
			time.Sleep(5 * time.Second)
			goto retry
		}

		// Provide helpful error message for merge strategy failures on non-Primary Key tables
		err = conn.handleMergeStrategyError(q, err)
	}
	return
}

// handleMergeStrategyError checks if the error is related to merge strategies failing on
// non-Primary Key tables and provides a helpful error message
func (conn *StarRocksConn) handleMergeStrategyError(q string, err error) error {
	errStr := err.Error()

	// Check for DELETE/UPDATE with subquery errors on Duplicate Key tables
	// These errors indicate the user is trying to use merge strategies that require Primary Key tables
	isDuplicateKeyError := strings.Contains(errStr, "Where clause only supports compound predicate, binary predicate, is_null predicate and in predicate") ||
		strings.Contains(errStr, "Child of in predicate should be value") ||
		(strings.Contains(errStr, "DELETE") && strings.Contains(errStr, "subquery")) ||
		(strings.Contains(errStr, "UPDATE") && strings.Contains(errStr, "subquery"))

	// Check if the query is a merge-related operation (DELETE or UPDATE with subquery/FROM)
	qLower := strings.ToLower(q)
	isMergeQuery := (strings.Contains(qLower, "delete from") && (strings.Contains(qLower, "where") && (strings.Contains(qLower, " in (") || strings.Contains(qLower, "exists")))) ||
		(strings.Contains(qLower, "update") && strings.Contains(qLower, "from"))

	if isDuplicateKeyError && isMergeQuery {
		return g.Error(err, `StarRocks merge strategies (update, update_insert, delete_insert) require Primary Key tables.
Your target table appears to be a Duplicate Key table which does not support DELETE/UPDATE with subqueries.

To fix this, specify 'table_keys.primary' in your target_options to create a Primary Key table:

  target_options:
    table_keys:
      primary: [your_pk_column]

Alternatively, use 'merge_strategy: insert' or modes like 'full-refresh' or 'truncate' which work with all table types.
See: https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/`)
	}

	return err
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *StarRocksConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns
	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(ds.Columns)
	context := ds.Context

	// in case schema change is needed, cannot alter while inserting
	mux := ds.Context.Mux
	if df := ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	insertBatch := func(bColumns iop.Columns, rows [][]interface{}) error {
		defer context.Wg.Write.Done()

		mux.Lock()
		defer mux.Unlock()

		insCols, err := conn.ValidateColumnNames(columns, bColumns.Names())
		if err != nil {
			return g.Error(err, "columns mismatch")
		}
		valuesSlice := []string{}
		valCount := 0
		for _, row := range rows {
			rowVals := lo.Map(row, func(val any, i int) string {
				valCount++
				newVal := ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
				switch {
				case val == nil:
					return "NULL"
				case ds.Columns[i].Type.IsNumber():
					return newVal
				case ds.Columns[i].Type.IsBool():
					// return newVal
					return `"` + newVal + `"` // since we're storing bools as string
				case ds.Columns[i].Type == iop.BinaryType:
					return `X'` + hex.EncodeToString([]byte(newVal)) + `'`
				default:
					newVal = strings.ReplaceAll(newVal, `"`, `""`)
					newVal = strings.ReplaceAll(newVal, `\`, `\\`)
					return `"` + newVal + `"`
				}
			})
			valuesSlice = append(valuesSlice, "("+strings.Join(rowVals, ", ")+")")
		}

		sql := g.R(
			"insert into {table} ({fields}) values  {values} "+noDebugKey,
			"table", tableFName,
			"fields", strings.Join(insCols.Names(), ", "),
			"values", strings.Join(valuesSlice, ",\n"),
		)
		_, err = conn.ExecContext(ds.Context.Ctx, sql)
		if err != nil {
			batchErrStr := g.F("Batch Size: %d rows x %d cols = %d (%d vals)", len(rows), len(bColumns), len(rows)*len(bColumns), valCount)
			g.Trace(g.F(
				"%s\n%s \n%s \n%s",
				err.Error(), batchErrStr,
				fmt.Sprintf("Insert SQL: %s", sql),
				fmt.Sprintf("\n\nRows: %#v", lo.Map(rows, func(row []any, i int) string {
					return g.F("len(row[%d]) = %d", i, len(row))
				})),
			))
			context.CaptureErr(err)
			context.Cancel()
			return context.Err()
		}

		return nil
	}

	batchRows := [][]any{}
	var batch *iop.Batch

	for batch = range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			// make sure fields match
			mux.Lock()
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				err = g.Error(err, "could not get column list")
				return
			}
			mux.Unlock()
		}

		for row := range batch.Rows {
			batchRows = append(batchRows, row)
			count++
			if len(batchRows) == batchSize {
				context.Wg.Write.Add()
				select {
				case <-context.Ctx.Done():
					return count, context.Err()
				case <-ds.Context.Ctx.Done():
					return count, ds.Context.Err()
				default:
					insertBatch(batch.Columns, batchRows)
				}

				// reset
				batchRows = [][]interface{}{}
			}
		}

	}

	// remaining batch
	if len(batchRows) > 0 {
		g.Trace("remaining batchSize %d", len(batchRows))
		context.Wg.Write.Add()
		err = insertBatch(batch.Columns, batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), g.Error(err, "insertBatch")
		}
	}

	context.Wg.Write.Wait()

	return
}

// GenerateDDL generates a DDL based on a dataset
func (conn *StarRocksConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {
	primaryKeyCols := data.Columns.GetKeys(iop.PrimaryKey)
	dupKeyCols := data.Columns.GetKeys(iop.DuplicateKey)
	hashKeyCols := data.Columns.GetKeys(iop.HashKey)
	aggKeyCols := data.Columns.GetKeys(iop.AggregateKey)
	uniqueKeyCols := data.Columns.GetKeys(iop.UniqueKey)

	if len(hashKeyCols) == 0 {
		if len(primaryKeyCols) > 0 {
			hashKeyCols = primaryKeyCols
		} else if len(dupKeyCols) > 0 {
			hashKeyCols = dupKeyCols
		} else if len(aggKeyCols) > 0 {
			hashKeyCols = aggKeyCols
		} else if len(uniqueKeyCols) > 0 {
			hashKeyCols = uniqueKeyCols
		} else {
			return "", g.Error("did not provide primary-key, duplicate-key, aggregate-key or hash-key for creating StarRocks table")
		}
	}

	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	// replace keys
	var distroColNames []string
	var tableDistro string

	if len(primaryKeyCols) > 0 {
		tableDistro = "primary"
		distroColNames = conn.Template().QuoteNames(primaryKeyCols.Names()...)
	} else if len(dupKeyCols) > 0 {
		tableDistro = "duplicate"
		distroColNames = conn.Template().QuoteNames(dupKeyCols.Names()...)
	} else if len(aggKeyCols) > 0 {
		tableDistro = "aggregate"
		distroColNames = conn.Template().QuoteNames(aggKeyCols.Names()...)
	} else if len(uniqueKeyCols) > 0 {
		tableDistro = "unique"
		distroColNames = conn.Template().QuoteNames(uniqueKeyCols.Names()...)
	}

	// set hash key
	hashColNames := conn.Template().QuoteNames(hashKeyCols.Names()...)
	ddl = strings.ReplaceAll(ddl, "{hash_key}", strings.Join(hashColNames, ", "))

	// set table distribution type & keys
	distribution := ""
	if tableDistro != "" && len(distroColNames) > 0 {
		distribution = g.F("%s key(%s)", tableDistro, strings.Join(distroColNames, ", "))
	}
	ddl = strings.ReplaceAll(ddl, "{distribution}", distribution)

	return ddl, nil
}

// BulkImportFlow inserts a flow of streams into a table.
func (conn *StarRocksConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	// update decimal columns precision/scale based on column_typing
	// this is needed especially for inferring the correct arrow parquet schema
	if err = applyColumnTypingToDf(conn, df); err != nil {
		return 0, g.Error(err, "invalid column_typing")
	}

	useBulk := cast.ToBool(conn.GetProp("use_bulk"))
	if feURL := conn.GetProp("fe_url"); feURL != "" && useBulk {
		return conn.StreamLoad(feURL, tableFName, df)
	}

	if useBulk {
		g.Debug("WARN: Using INSERT mode which is meant for small datasets. Please set the `fe_url` for loading large datasets via Stream Load mode. See https://docs.slingdata.io/connections/database-connections/starrocks")
	}

	return conn.BaseConn.BulkImportFlow(tableFName, df)
}

// StreamLoad bulk loads
// https://docs.starrocks.io/docs/loading/StreamLoad/
// https://docs.starrocks.io/docs/sql-reference/sql-statements/data-manipulation/STREAM_LOAD/
func (conn *StarRocksConn) StreamLoad(feURL, tableFName string, df *iop.Dataflow) (count uint64, err error) {

	connURL, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		return count, g.Error(err, "invalid conn url")
	}

	user := connURL.U.User.Username()
	password, _ := connURL.U.User.Password()

	// if no user / password provided
	applyCreds := func(u *url.URL) string {
		urlStr := u.String()
		uUser := u.User.Username()
		uPassword, _ := u.User.Password()

		noCredPrefix := g.F("%s://%s", u.Scheme, u.Hostname())
		if strings.HasPrefix(urlStr, noCredPrefix) && uUser == "" {
			urlStr = strings.ReplaceAll(
				urlStr,
				u.Hostname(),
				g.F("%s:%s@%s", user, url.QueryEscape(password), u.Hostname()),
			)
		} else if uUser != "" && uPassword == "" && password != "" {
			urlStr = strings.ReplaceAll(
				urlStr,
				g.F("%s@%s", user, u.Hostname()),
				g.F("%s:%s@%s", user, url.QueryEscape(password), u.Hostname()),
			)
		}
		return urlStr
	}

	fu, err := net.NewURL(feURL)
	if err != nil {
		return count, g.Error(err, "invalid url for FE")
	}

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return count, g.Error(err, "could not parse table: %s", tableFName)
	}

	g.Info("importing into StarRocks via stream load")

	fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for Local")
		return
	}

	// set default format to csv
	if val := conn.GetProp("format"); val != "" {
		fs.SetProp("format", val)
	} else {
		fs.SetProp("format", "csv")
	}

	localPath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("starrocks.%s", env.CleanTableName(tableFName))))

	// TODO: use reader to fead HTTP directly. Need to get proper redirected URL first.
	// for ds := range df.StreamCh {
	// 	readerChn := ds.NewJsonReaderChnl(0, 0)
	// 	for reader := range readerChn {

	// 	}
	// }

	fileReadyChn := make(chan filesys.FileReady, 10)
	go func() {
		config := iop.LoaderStreamConfig(false)
		config.FileMaxRows = 250000
		if val := conn.GetProp("file_max_rows"); val != "" {
			config.FileMaxRows = cast.ToInt64(val)
		}
		if val := conn.GetProp("file_max_bytes"); val != "" {
			config.FileMaxBytes = cast.ToInt64(val)
		}

		if conn.version >= 327 && conn.GetProp("compression") != "" {
			config.Compression = iop.CompressorType(conn.GetProp("compression"))
		}
		_, err = fs.WriteDataflowReady(df, localPath, fileReadyChn, config)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "error writing dataflow to local storage: "+localPath))
			return
		}
	}()

	// col names must match ddl
	colNames := lo.Map(df.Columns.Names(), func(name string, i int) string {
		q := conn.template.Variable["quote_char"]
		return strings.ReplaceAll(conn.Quote(name), q, "")
	})

	// default is CSV
	headers := map[string]string{
		"expect":  "100-continue",
		"timeout": "300",

		"columns": strings.Join(colNames, ", "),

		"format":           "CSV",
		"column_separator": ",",
		"enclose":          `"`,
	}

	// putting compression does not work, returns `too many rows filtered` error
	// if conn.version >= 327 {
	// 	headers["compression"] = "zstd"
	// }

	if conn.GetProp("format") == "json" {
		headers = map[string]string{
			"expect":  "100-continue",
			"timeout": "300",
			"columns": strings.Join(colNames, ", "),

			"strict":            "true",
			"format":            "JSON",
			"strip_outer_array": "true",
		}
	}
	timeout := 330

	// set extra headers
	for _, key := range []string{"max_filter_ratio", "timezone", "strict_mode", "timeout", "compression"} {
		if val := conn.GetProp(key); val != "" {
			headers[key] = val
			if key == "timeout" {
				timeout = cast.ToInt(val) + 30
			}
		}
	}

	g.Debug("stream load headers => %s", g.Marshal(headers))

	var loadedRows uint64
	loadCtx := g.NewContext(conn.context.Ctx, 3)

	loadFromLocal := func(localFile filesys.FileReady, tableFName string) {
		defer loadCtx.Wg.Write.Done()
		g.Debug("loading %s [%s] %s", localFile.Node.Path(), humanize.Bytes(cast.ToUint64(localFile.BytesW)), localFile.BatchID)

		defer func() { env.RemoveLocalTempFile(localFile.Node.Path()) }()

		reader, err := os.Open(localFile.Node.Path())
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not open temp file: %s", localFile.Node.Path()))
		}

		apiURL := strings.TrimSuffix(applyCreds(fu.U), "/") + g.F("/api/%s/%s/_stream_load", table.Schema, table.Name)
		if conn.fePort != "" {
			// this is the fix to not freeze, call the redirected port directly
			apiURL = strings.ReplaceAll(apiURL, fu.U.Port(), conn.fePort)
		}

		resp, respBytes, err := net.ClientDo(http.MethodPut, apiURL, reader, headers, timeout)
		if resp != nil && resp.StatusCode >= 300 && resp.StatusCode <= 399 {
			redirectUrl, _ := resp.Location()
			if redirectUrl != nil {
				// g.Debug("FE url redirected to %s://%s", redirectUrl.Scheme, redirectUrl.Host)
				redirectUrlStr := strings.ReplaceAll(redirectUrl.String(), "127.0.0.1", fu.U.Hostname())
				redirectUrl, _ = url.Parse(redirectUrlStr)
				g.Warn("StarRocks redirected the API call to '%s://%s'. Please use that as your FE url.", redirectUrl.Scheme, redirectUrl.Host)
				conn.fePort = redirectUrl.Port()
				reader, _ = os.Open(localFile.Node.Path()) // re-open file since it would be closed
				_, respBytes, err = net.ClientDo(http.MethodPut, applyCreds(redirectUrl), reader, headers, timeout)
			}
		}

		respString := strings.ReplaceAll(string(respBytes), "127.0.0.1", fu.U.Hostname())
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error loading from %s into %s\n%s", localFile.Node.Path(), tableFName, respString))
			df.Context.Cancel()
		} else {
			respMap, _ := g.UnmarshalMap(respString)
			g.Debug("stream-load completed for %s => %s", localFile.Node.Path(), respString)
			if cast.ToString(respMap["Status"]) == "Fail" {
				df.Context.CaptureErr(g.Error("Failed loading from %s into %s\n%s", localFile.Node.Path(), tableFName, respString))
				df.Context.Cancel()
			}
			loadedRows = loadedRows + cast.ToUint64(respMap["NumberLoadedRows"])
		}
	}

	for localFile := range fileReadyChn {
		loadCtx.Wg.Write.Add()
		go loadFromLocal(localFile, tableFName)
		if df.Err() != nil {
			return df.Count(), df.Err()
		}
	}

	g.Debug("Done submitting data. Waiting for load completion.")
	loadCtx.Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), g.Error(df.Err(), "Error importing to StarRocks")
	}

	if loadedRows != df.Count() {
		return loadedRows, g.Error("loaded rows (%d) != stream count (%d). Records missing/mismatch. Aborting", loadedRows, df.Count())
	}

	return df.Count(), nil
}

// GetDatabases returns the list of databases
func (conn *StarRocksConn) GetDatabases() (data iop.Dataset, err error) {
	data1, err := conn.BaseConn.GetDatabases()
	if err != nil {
		return data1, err
	}

	data1.Columns[0].Name = "name" // rename

	return data1, nil
}
