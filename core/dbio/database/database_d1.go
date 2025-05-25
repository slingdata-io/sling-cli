package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// D1Conn is a Cloudflare SQLite connection
type D1Conn struct {
	SQLiteConn
	URL       string
	AccountID string
	Database  string
	UUID      string
	APIToken  string
	client    http.Client
}

// Init initiates the object
func (conn *D1Conn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbD1

	conn.AccountID = conn.GetProp("account_id")
	conn.Database = conn.GetProp("database")
	conn.APIToken = conn.GetProp("api_token")
	conn.client = http.Client{}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	conn.SetProp("use_bulk", "false") // no bulk import yet for D1

	return conn.BaseConn.Init()
}

func (conn *D1Conn) makeRequest(ctx context.Context, method, route string, body io.Reader) (resp *http.Response, err error) {
	tries := 0
	urlBase := "https://api.cloudflare.com/client/v4/accounts"
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + conn.APIToken,
	}

	route = strings.TrimPrefix(route, "/")
	URL := g.F("%s/%s/d1/database", urlBase, conn.AccountID)
	if conn.UUID != "" && route != "" {
		URL = g.F("%s/%s/d1/database/%s/%s", urlBase, conn.AccountID, conn.UUID, route)
	}

retry:
	tries++
	g.Trace("request #%d for %s @ %s", tries, method, URL)
	req, err := http.NewRequestWithContext(ctx, method, URL, body)
	if err != nil {
		return nil, g.Error(err, "could not make request for %s @ %s", method, URL)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err = conn.client.Do(req)
	if err != nil {
		err = g.Error(err, "could not perform request")
		return
	}

	// retry logic
	if (resp.StatusCode >= 502 || resp.StatusCode == 429) && tries <= 4 {
		delay := tries * 5
		g.Debug("d1 request failed %d: %s. Retrying in %d seconds.", resp.StatusCode, resp.Status, delay)
		time.Sleep(time.Duration(delay * int(time.Second)))
		goto retry
	}

	if resp.StatusCode >= 400 || resp.StatusCode < 200 {
		respBytes, _ := io.ReadAll(resp.Body)
		err = g.Error("Unexpected Response %d: %s (%s) => %s", resp.StatusCode, resp.Status, URL, string(respBytes))
		return
	}

	return
}

// Connect connects to the database
func (conn *D1Conn) Connect(timeOut ...int) (err error) {
	if cast.ToBool(conn.GetProp("connected")) {
		return nil
	}

	data, err := conn.GetDatabases()
	if err != nil {
		return g.Error(err, "could not list databases")
	} else if len(data.Rows) == 0 {
		return g.Error("no databases found")
	}

	available := []string{}
	for _, row := range data.Rows {
		name := cast.ToString(row[0])
		uuid := cast.ToString(row[1])
		available = append(available, name)
		if strings.EqualFold(name, conn.Database) {
			conn.UUID = uuid
		}
	}

	if conn.UUID == "" {
		return g.Error(`did not find database "%s" in %s`, conn.Database, g.Marshal(available))
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

// GetDatabases returns databases for given connection
func (conn *D1Conn) GetDatabases() (data iop.Dataset, err error) {
	type Response struct {
		Result []struct {
			UUID      string    `json:"uuid"`
			Name      string    `json:"name"`
			CreatedAt time.Time `json:"created_at"`
			Version   string    `json:"version"`
			NumTables int       `json:"num_tables"`
			FileSize  int64     `json:"file_size"`
		} `json:"result"`
	}

	resp, err := conn.makeRequest(conn.context.Ctx, "GET", "", nil)
	if err != nil {
		return data, g.Error(err, "could not make request")
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return data, g.Error(err, "could not read from request body")
	}

	var response Response
	if err = g.Unmarshal(string(respBytes), &response); err != nil {
		return data, g.Error(err, "could not unmarshal from request body")
	}

	if len(response.Result) == 0 {
		return data, g.Error("no databases found")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("name", "uuid"))
	for _, result := range response.Result {
		data.Rows = append(data.Rows, []any{result.Name, result.UUID})
	}

	return data, nil
}

type d1ExecResponse struct {
	Success bool  `json:"success"`
	Errors  []any `json:"errors"`
	Result  []struct {
		Meta struct {
			ServedBy    string  `json:"served_by"`
			Duration    float64 `json:"uuid"`
			Changes     int64   `json:"changes"`
			LastRowID   any     `json:"last_row_id"`
			ChangedDB   bool    `json:"changed_db"`
			SizeAfter   int64   `json:"size_after"`
			RowsRead    int64   `json:"rows_read"`
			RowsWritten int64   `json:"rows_written"`
		} `json:"meta"`
	} `json:"result"`
}

func (r d1ExecResponse) LastInsertId() (int64, error) {
	if len(r.Result) > 0 {
		return cast.ToInt64(r.Result[0].Meta.LastRowID), nil
	}
	return -1, nil
}

func (r d1ExecResponse) RowsAffected() (int64, error) {
	if len(r.Result) > 0 {
		return cast.ToInt64(r.Result[0].Meta.Changes), nil
	}
	return -1, nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *D1Conn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	if strings.TrimSpace(q) == "" {
		g.Warn("Empty Query")
		return
	}

	queryContext := g.NewContext(ctx)
	if args == nil {
		args = make([]any, 0)
	}
	payload := g.M("sql", q, "params", args)

	conn.LogSQL(q, args...)

	resp, err := conn.makeRequest(queryContext.Ctx, "POST", "/raw", strings.NewReader(g.Marshal(payload)))
	if err != nil {
		if strings.Contains(q, noDebugKey) {
			err = g.Error(err, "Error executing query")
		} else {
			err = g.Error(err, "Error executing %s", env.Clean(conn.Props(), q))
		}
		return
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, g.Error(err, "could not read from request body")
	}

	var response d1ExecResponse
	if err = g.Unmarshal(string(respBytes), &response); err != nil {
		return nil, g.Error(err, "could not unmarshal from request body")
	}

	return response, err
}

func (conn *D1Conn) StreamRowsContext(ctx context.Context, query string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	opts := getQueryOptions(options)
	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val
	}

	start := time.Now()
	if strings.TrimSpace(query) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)

	conn.LogSQL(query)

	payload := g.M("sql", query, "params", []string{})

	resp, err := conn.makeRequest(queryContext.Ctx, "POST", "/raw", strings.NewReader(g.Marshal(payload)))
	if err != nil {
		return ds, g.Error(err, "could not make request")
	}

	conn.Data.SQL = query
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.NoDebug = !strings.Contains(query, noDebugKey)

	// respBytes, _ := io.ReadAll(resp.Body)
	// g.Warn(string(respBytes))
	// return nil, g.Error("stopping")

	decoder := json.NewDecoder(resp.Body)

	// Read opening object
	if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
		return nil, g.Error(err, "invalid JSON structure: expected opening brace")
	}

	// this is to parse the response as it comes (it will not put all of it in memory)
	makeNextFunc := func() (F func(it *iop.Iterator) bool, err error) {
		// Find the "result" array
		for decoder.More() {
			t, err := decoder.Token()
			if err != nil {
				return nil, g.Error(err, "error reading JSON token")
			}

			// Example:
			// {"result":[{"results":{"columns":["schema_name","table_name","is_view"],"rows":[["main","_cf_KV","false"],["main","table_name","false"]]},"success":true,"meta":{"served_by":"v3-prod","duration":0.2294,"changes":0,"last_row_id":0,"changed_db":false,"size_after":16384,"rows_read":4,"rows_written":0}}],"errors":[],"messages":[],"success":true}
			if cast.ToString(t) == "result" {
				// Read the opening bracket of result array
				if t, err := decoder.Token(); err != nil || t != json.Delim('[') {
					return nil, g.Error(err, "invalid JSON structure: expected result array")
				}

				// Read the first result object
				if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
					return nil, g.Error(err, "invalid JSON structure: expected result object")
				}

				// Process the result object to find "results"
				for decoder.More() {
					t, err := decoder.Token()
					if err != nil {
						return nil, g.Error(err, "error reading result object")
					}

					if cast.ToString(t) == "results" {
						// Read the opening bracket of result array
						if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
							return nil, g.Error(err, "invalid JSON structure: expected result object inside results")
						}

						// Read the opening bracket of columns array
						t, err = decoder.Token()
						if err != nil {
							return nil, g.Error(err, "invalid JSON structure: expected columns array")
						} else if cast.ToString(t) != "columns" {
							return nil, g.Error("invalid JSON structure: expected columns array inside results")
						}

						var columns []string
						// Decode just the columns
						if err := decoder.Decode(&columns); err != nil {
							return nil, g.Error(err, "error decoding columns")
						}

						// Set up columns in datastream
						if g.Marshal(fetchedColumns.Names()) != g.Marshal(columns) {
							fetchedColumns = iop.NewColumnsFromFields(columns...)
						}

						// Read the opening bracket of rows array
						t, err = decoder.Token()
						if err != nil || cast.ToString(t) != "rows" {
							return nil, g.Error(err, "invalid JSON structure: expected rows array inside results")
						} else if cast.ToString(t) != "rows" {
							return nil, g.Error("invalid JSON structure: expected rows array inside results")
						}

						// Read opening bracket of rows array
						t, err := decoder.Token()
						if err != nil {
							return nil, g.Error(err, "invalid JSON structure: expected rows array")
						} else if t != json.Delim('[') {
							return nil, g.Error("invalid JSON structure: expected bracket inside rows array")
						}

						// Start streaming rows in a goroutine
						nextFunc := func(it *iop.Iterator) bool {
							// Stream each row
							if decoder.More() {
								var row []any
								if err := decoder.Decode(&row); err != nil {
									it.Context.CaptureErr(g.Error(err, "error decoding row"))
									return false
								}
								it.Row = row
								return true
							}
							return false
						}
						return nextFunc, nil
					}
				}
			}

			// Example:
			// {"errors":[{"code":7500,"message":"SQLITE_ERROR"}],"success":false,"messages":[],"result":[]}
			if cast.ToString(t) == "errors" {
				var errResp struct {
					Errors []struct {
						Code    int    `json:"code"`
						Message string `json:"message"`
					} `json:"errors"`
				}
				if err := decoder.Decode(&errResp); err != nil {
					return nil, g.Error(err, "error decoding error response")
				}
				if len(errResp.Errors) > 0 {
					return nil, g.Error(fmt.Sprintf("D1 error %d: %s", errResp.Errors[0].Code, errResp.Errors[0].Message))
				}
			}

		}

		return nil, g.Error("unable to create iterator. End of stream?")
	}

	nextFunc, err := makeNextFunc()
	if err != nil {
		return ds, err
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, fetchedColumns, nextFunc)
	ds.NoDebug = strings.Contains(query, noDebugKey)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *D1Conn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	err := conn.Connect()
	if err != nil {
		return schemata, g.Error(err, "could not get connect to get schemata")
	}

	data, err := conn.GetSchemas()
	if err != nil {
		return schemata, g.Error(err, "could not get schemas")
	}

	schemaNames := data.ColValuesStr(0)
	if schemaName != "" {
		schemaNames = []string{schemaName}
	}

	schemas := map[string]Schema{}
	ctx := g.NewContext(conn.context.Ctx, 5)
	currDatabase := "main"

	getOneSchemata := func(values map[string]interface{}) error {
		defer ctx.Wg.Read.Done()

		var data iop.Dataset
		var err error
		switch level {
		case SchemataLevelSchema:
			data.Columns = iop.NewColumnsFromFields("schema_name")
			data.Append([]any{values["schema"]})
		case SchemataLevelTable:
			data, err = conn.GetTablesAndViews(cast.ToString(values["schema"]))
		case SchemataLevelColumn:
			data, err = conn.SubmitTemplate(
				"single", conn.template.Metadata, "schemata",
				values,
			)
		}
		if err != nil {
			return g.Error(err, "Could not get schemata at %s level", level)
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range data.Records() {
			schemaName = cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))

			switch v := rec["is_view"].(type) {
			case int64, float64:
				if cast.ToInt64(rec["is_view"]) == 0 {
					rec["is_view"] = false
				} else {
					rec["is_view"] = true
				}
			case string:
				if cast.ToBool(rec["is_view"]) {
					rec["is_view"] = true
				} else {
					rec["is_view"] = false
				}

			default:
				_ = fmt.Sprint(v)
				_ = rec["is_view"]
			}

			schema := Schema{
				Name:     schemaName,
				Database: currDatabase,
				Tables:   map[string]Table{},
			}

			if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
				schema = schemas[strings.ToLower(schema.Name)]
			}

			var table Table
			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				table = Table{
					Name:     tableName,
					Schema:   schemaName,
					Database: currDatabase,
					IsView:   cast.ToBool(rec["is_view"]),
					Columns:  iop.Columns{},
					Dialect:  dbio.TypeDbSQLite,
				}

				if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
					table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
				}
			}

			if level == SchemataLevelColumn {
				column := iop.Column{
					Name:     columnName,
					Type:     NativeTypeToGeneral(columnName, dataType, conn),
					Table:    tableName,
					Schema:   schemaName,
					Database: currDatabase,
					Position: cast.ToInt(data.Sp.ProcessVal(rec["position"])),
					DbType:   dataType,
				}

				table.Columns = append(table.Columns, column)
			}

			if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
				schema.Tables[strings.ToLower(tableName)] = table
			}
			schemas[strings.ToLower(schema.Name)] = schema
		}

		schemata.Databases[strings.ToLower(currDatabase)] = Database{
			Name:    currDatabase,
			Schemas: schemas,
		}

		return nil
	}

	for _, schemaName := range schemaNames {
		g.Debug("getting schemata for %s", schemaName)
		values := g.M("schema", schemaName)

		if len(tableNames) > 0 && !(tableNames[0] == "" && len(tableNames) == 1) {
			tablesQ := []string{}
			for _, tableName := range tableNames {
				if strings.TrimSpace(tableName) == "" {
					continue
				}
				tablesQ = append(tablesQ, `'`+tableName+`'`)
			}
			if len(tablesQ) > 0 {
				values["tables"] = strings.Join(tablesQ, ", ")
			}
		}

		ctx.Wg.Read.Add()
		go func(values map[string]interface{}) {
			err := getOneSchemata(values)
			ctx.CaptureErr(err)
		}(values)
	}

	ctx.Wg.Read.Wait()

	if err := ctx.Err(); err != nil {
		return schemata, g.Error(err)
	}

	return schemata, nil
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *D1Conn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	_, indexTable := SplitTableFullName(tgtTable)

	pkFieldsQ := lo.Map(pkFields, func(f string, i int) string { return conn.Quote(f) })
	indexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+g.RandSuffix("_", 3)+"_idx",
		"table", indexTable,
		"cols", strings.Join(pkFieldsQ, ", "),
	)

	_, err = conn.Exec(indexSQL)
	if err != nil {
		err = g.Error(err, "could not create unique index")
		return
	}

	sqlTempl := `
	insert into {tgt_table} as tgt
		({insert_fields}) 
	select {src_fields}
	from {src_table} as src
	where true
	ON CONFLICT ({tgt_pk_fields})
	DO UPDATE 
	SET {set_fields}
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"tgt_pk_fields", upsertMap["tgt_pk_fields"],
		"set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}

func (conn *D1Conn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.InsertBatchStream(tableFName, ds)
}

func (conn *D1Conn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	var columns iop.Columns
	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(ds.Columns)

	// default 50 concurrent requests
	concurrency := 50
	if val := conn.GetProp("insert_concurrency"); val != "" {
		concurrency = cast.ToInt(val)
	}
	insertContext := g.NewContext(ds.Context.Ctx, concurrency)

	// in case schema change is needed, cannot alter while inserting
	mux := ds.Context.Mux
	if df := ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	insertBatch := func(bColumns iop.Columns, rows [][]interface{}) error {
		defer insertContext.Wg.Write.Done()

		insCols, err := conn.ValidateColumnNames(columns, bColumns.Names())
		if err != nil {
			return g.Error(err, "columns mismatch")
		}

		insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insCols, len(rows))
		vals := []interface{}{}
		for _, row := range rows {
			vals = append(vals, row...)
		}

		insertTemplate = insertTemplate + noDebugKey

		_, err = conn.ExecContext(ds.Context.Ctx, insertTemplate, vals...)
		if err != nil {
			batchErrStr := g.F("Batch Size: %d rows x %d cols = %d (%d vals)", len(rows), len(bColumns), len(rows)*len(bColumns), len(vals))
			if len(insertTemplate) > 3000 {
				insertTemplate = insertTemplate[:3000]
			}
			// g.Warn("\n\n%s\n\n", g.Marshal(rows))
			if len(rows) > 10 {
				rows = rows[:10]
			}
			g.Debug(g.F(
				"%s\n%s \n%s \n%s",
				err.Error(), batchErrStr,
				fmt.Sprintf("Insert: %s", insertTemplate),
				fmt.Sprintf("\n\nRows: %#v", lo.Map(rows, func(row []any, i int) string {
					return g.F("len(row[%d]) = %d", i, len(row))
				})),
			))
			insertContext.CaptureErr(err)
			return err
		}

		return nil
	}
	batchRows := [][]any{}
	var batch *iop.Batch

	for batch = range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			// make sure fields match
			mux.Lock()
			insertContext.Wg.Write.Wait() // wait for any pending queries
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
				insertContext.Wg.Write.Add()
				select {
				case <-insertContext.Ctx.Done():
					return count, insertContext.Err()
				case <-ds.Context.Ctx.Done():
					return count, ds.Context.Err()
				default:
					go insertBatch(batch.Columns, batchRows)
				}

				// reset
				batchRows = [][]interface{}{}
			}
		}

	}

	// remaining batch
	if len(batchRows) > 0 {
		g.Trace("remaining batchSize %d", len(batchRows))
		insertContext.Wg.Write.Add()
		err = insertBatch(batch.Columns, batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), g.Error(err, "insertBatch")
		}
	}

	insertContext.Wg.Write.Wait()

	return
}
