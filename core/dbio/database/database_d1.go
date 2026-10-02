package database

import (
	"bytes"
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
	apiURL    string
}

// Init initiates the object
func (conn *D1Conn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbD1

	conn.AccountID = conn.GetProp("account_id")
	conn.Database = conn.GetProp("database")
	conn.APIToken = conn.GetProp("api_token")
	conn.client = http.Client{}
	conn.apiURL = "https://api.cloudflare.com/client/v4/accounts"

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	conn.SetProp("use_bulk", "false") // no bulk import yet for D1

	return conn.BaseConn.Init()
}

func (conn *D1Conn) makeRequest(ctx context.Context, method, route string, body io.Reader) (resp *http.Response, err error) {
	tries := 0
	urlBase := conn.apiURL
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": "Bearer " + conn.APIToken,
	}

	route = strings.TrimPrefix(route, "/")
	URL := g.F("%s/%s/d1/database", urlBase, conn.AccountID)
	if conn.UUID != "" && route != "" {
		URL = g.F("%s/%s/d1/database/%s/%s", urlBase, conn.AccountID, conn.UUID, route)
	}

	// buffer body for potential retries (readers are single-use)
	var bodyBytes []byte
	if body != nil {
		bodyBytes, err = io.ReadAll(body)
		if err != nil {
			return nil, g.Error(err, "could not read request body for %s @ %s", method, URL)
		}
	}

retry:
	tries++
	g.Trace("request #%d for %s @ %s", tries, method, URL)

	var reqBody io.Reader
	if bodyBytes != nil {
		reqBody = bytes.NewReader(bodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, method, URL, reqBody)
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

	if resp.StatusCode >= 400 || resp.StatusCode < 200 {
		respBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		// retry logic for transient server errors / rate limits.
		// a memory limit reset (code 7429 with status 429) fails again with the same query.
		retryable := resp.StatusCode >= 502 || resp.StatusCode == 429
		if retryable && tries <= 4 && !strings.Contains(string(respBytes), d1MemoryLimitMsg) {
			delay := tries * 5
			g.Debug("d1 request failed %d: %s. Retrying in %d seconds.", resp.StatusCode, resp.Status, delay)
			time.Sleep(time.Duration(delay * int(time.Second)))
			goto retry
		}

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
	if resp.Body != nil {
		resp.Body.Close()
	}
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
			Duration    float64 `json:"duration"`
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
	if resp.Body != nil {
		resp.Body.Close()
	}
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
	limit := cast.ToUint64(opts["limit"])
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

	pager := newD1Pager(conn, queryContext.Ctx, query)
	if err = pager.fetch(); err != nil {
		return ds, g.Error(err, "could not make request")
	}

	conn.Data.SQL = query
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.NoDebug = !strings.Contains(query, noDebugKey)

	if g.Marshal(fetchedColumns.Names()) != g.Marshal(pager.columns) {
		fetchedColumns = iop.NewColumnsFromFields(pager.columns...)
	}

	nextFunc := func(it *iop.Iterator) bool {
		if limit > 0 && it.Counter >= limit {
			return false
		}
		row, err := pager.next()
		if err != nil {
			it.Context.CaptureErr(err)
			return false
		} else if row == nil {
			return false
		}
		it.Row = row
		return true
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, fetchedColumns, nextFunc)
	ds.NoDebug = strings.Contains(query, noDebugKey)
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())
	ds.Defer(pager.close)

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		pager.close()
		return ds, g.Error(err, "could start datastream")
	}

	return
}

const (
	d1DefaultPageSize = 5000
	d1MinPageSize     = 50
	d1MemoryLimitMsg  = "exceeded its memory limit"
)

// d1Pager reads a query result in pages. When one response is too large
// (e.g. rows with large BLOBs), D1 runs out of memory. It then fails with
// error 7429, 7500 (status 500) or a status 504. When a page fails
// this way, the pager halves the page size and tries again.
type d1Pager struct {
	conn     *D1Conn
	ctx      context.Context
	query    string
	pageSize int // 0 means one request, no pages
	offset   int
	pageRows int
	columns  []string
	body     io.ReadCloser
	decoder  *json.Decoder
}

func newD1Pager(conn *D1Conn, ctx context.Context, query string) *d1Pager {
	p := &d1Pager{conn: conn, ctx: ctx, query: strings.TrimRight(strings.TrimSpace(query), "; \n\t")}

	// only a select can be wrapped in a subquery
	lower := strings.ToLower(p.query)
	if strings.HasPrefix(lower, "select") || strings.HasPrefix(lower, "with") {
		p.pageSize = d1DefaultPageSize
		if val := cast.ToInt(conn.GetProp("page_size")); val > 0 {
			p.pageSize = val
		}
	}
	return p
}

// fetch requests the page at the current offset and moves the decoder to the first row
func (p *d1Pager) fetch() (err error) {
	p.close()
	p.pageRows = 0

	for {
		sql := p.query
		if p.pageSize > 0 {
			sql = g.F("select * from (\n%s\n) limit %d offset %d", p.query, p.pageSize, p.offset)
		}

		payload := g.M("sql", sql, "params", []string{})
		resp, err := p.conn.makeRequest(p.ctx, "POST", "/raw", strings.NewReader(g.Marshal(payload)))
		if err == nil {
			p.body = resp.Body
			break
		}

		if p.pageSize > d1MinPageSize && isD1ResponseTooLarge(err) {
			p.pageSize = max(p.pageSize/2, d1MinPageSize)
			g.Debug("d1 request failed at offset %d, retrying with page size %d", p.offset, p.pageSize)
			continue
		}
		return err
	}

	p.decoder = json.NewDecoder(p.body)
	if err = p.readHeader(); err != nil {
		p.close()
		return err
	}
	return nil
}

// next returns the next row, or nil at the end of the result
func (p *d1Pager) next() (row []any, err error) {
	for {
		if p.decoder.More() {
			if err = p.decoder.Decode(&row); err != nil {
				return nil, g.Error(err, "error decoding row")
			}
			p.pageRows++
			return row, nil
		}

		// a page that is not full is the last one
		if p.pageSize == 0 || p.pageRows < p.pageSize {
			p.close()
			return nil, nil
		}

		p.offset += p.pageRows
		if err = p.fetch(); err != nil {
			return nil, g.Error(err, "could not fetch rows at offset %d", p.offset)
		}
	}
}

func isD1ResponseTooLarge(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "Unexpected Response 500") ||
		strings.Contains(msg, "Unexpected Response 504") ||
		strings.Contains(msg, d1MemoryLimitMsg)
}

func (p *d1Pager) close() {
	if p.body != nil {
		p.body.Close()
		p.body = nil
	}
}

// readHeader parses the response up to the rows array, as it comes (not all in memory).
// Example:
// {"result":[{"results":{"columns":["schema_name","table_name","is_view"],"rows":[["main","_cf_KV","false"],["main","table_name","false"]]},"success":true,"meta":{...}}],"errors":[],"messages":[],"success":true}
func (p *d1Pager) readHeader() error {
	decoder := p.decoder

	if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
		return g.Error(err, "invalid JSON structure: expected opening brace")
	}

	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return g.Error(err, "error reading JSON token")
		}

		if cast.ToString(t) == "result" {
			// Read the opening bracket of result array
			if t, err := decoder.Token(); err != nil || t != json.Delim('[') {
				return g.Error(err, "invalid JSON structure: expected result array")
			}

			// Read the first result object
			if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
				return g.Error(err, "invalid JSON structure: expected result object")
			}

			// Process the result object to find "results"
			for decoder.More() {
				t, err := decoder.Token()
				if err != nil {
					return g.Error(err, "error reading result object")
				}

				if cast.ToString(t) != "results" {
					continue
				}

				if t, err := decoder.Token(); err != nil || t != json.Delim('{') {
					return g.Error(err, "invalid JSON structure: expected result object inside results")
				}

				t, err = decoder.Token()
				if err != nil {
					return g.Error(err, "invalid JSON structure: expected columns array")
				} else if cast.ToString(t) != "columns" {
					return g.Error("invalid JSON structure: expected columns array inside results")
				}

				var columns []string
				if err := decoder.Decode(&columns); err != nil {
					return g.Error(err, "error decoding columns")
				}
				if p.columns == nil {
					p.columns = columns
				}

				t, err = decoder.Token()
				if err != nil || cast.ToString(t) != "rows" {
					return g.Error(err, "invalid JSON structure: expected rows array inside results")
				}

				t, err = decoder.Token()
				if err != nil {
					return g.Error(err, "invalid JSON structure: expected rows array")
				} else if t != json.Delim('[') {
					return g.Error("invalid JSON structure: expected bracket inside rows array")
				}
				return nil
			}
		}

		// Example:
		// {"errors":[{"code":7500,"message":"SQLITE_ERROR"}],"success":false,"messages":[],"result":[]}
		if cast.ToString(t) == "errors" {
			var errs []struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			}
			if err := decoder.Decode(&errs); err != nil {
				return g.Error(err, "error decoding error response")
			}
			if len(errs) > 0 {
				return g.Error(fmt.Sprintf("D1 error %d: %s", errs[0].Code, errs[0].Message))
			}
		}
	}

	return g.Error("unable to create iterator. End of stream?")
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

// GenerateMergeSQL generates the upsert SQL using the database default strategy (update_insert).
func (conn *D1Conn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, nil)
}

// GenerateMergeSQLWithStrategy generates the merge SQL using the specified strategy.
// D1 (SQLite-based) supports all four merge strategies.
// For update_insert strategy, creates a unique index on PK fields to enable ON CONFLICT.
func (conn *D1Conn) GenerateMergeSQLWithStrategy(srcTable string, tgtTable string, pkFields []string, strategy *MergeStrategy) (sql string, err error) {
	return conn.SQLiteConn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, strategy)
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
