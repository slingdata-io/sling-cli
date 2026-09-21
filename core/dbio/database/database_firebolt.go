package database

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

const fireboltDriverName = "firebolt"

// Firebolt protocol constants. Each engine accepts SQL over HTTP: the request
// body is a single SQL statement, results come back as JSONLines_Compact so
// they can be streamed without buffering the whole result set.
const (
	fireboltOutputFormat  = "JSONLines_Compact"
	fireboltNullSuffix    = " null"
	fireboltUpdateParams  = "Firebolt-Update-Parameters"
	fireboltRemoveParams  = "Firebolt-Remove-Parameters"
	fireboltQueryParamsKV = "query_parameters"
)

func init() {
	sql.Register(fireboltDriverName, &fireboltDriver{})
}

// FireboltConn is a Firebolt connection
type FireboltConn struct {
	BaseConn
	URL string
}

// Init initiates the connection
func (conn *FireboltConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbFirebolt
	conn.BaseConn.defaultPort = 3473

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// ---------------------------------------------------------------- driver

type fireboltDriver struct{}

func (d *fireboltDriver) Open(dsn string) (driver.Conn, error) {
	return newFireboltConn(dsn)
}

type fireboltConn struct {
	client  *http.Client
	baseURL string // scheme://host:port
	params  map[string]string
	mux     sync.Mutex
}

func newFireboltConn(dsn string) (*fireboltConn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, g.Error(err, "could not parse Firebolt connection URL")
	}

	host := u.Hostname()
	if host == "" {
		return nil, g.Error("Firebolt host is required")
	}
	port := u.Port()
	if port == "" {
		port = "3473"
	}

	values := u.Query()
	secure := cast.ToBool(values.Get("secure"))
	scheme := "http"
	if secure {
		scheme = "https"
	}

	conn := &fireboltConn{
		client:  fireboltHTTPClient(secure, secure && cast.ToBool(values.Get("skip_verify"))),
		baseURL: fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(host, port)),
		params:  map[string]string{},
	}

	if database := strings.Trim(u.Path, "/"); database != "" {
		conn.params["database"] = database
	}

	return conn, nil
}

// fireboltClients caches one HTTP client per TLS setting. The pool holds several
// physical connections per sling connection (a suite run peaks at ~10 concurrent
// sockets to the engine), and http.Client is safe for concurrent use, so a single
// client and its TCP pool serve them all rather than one Transport per session.
var (
	fireboltClientsMu sync.Mutex
	fireboltClients   = map[string]*http.Client{}
)

func fireboltHTTPClient(secure, skipVerify bool) *http.Client {
	key := fmt.Sprintf("%t|%t", secure, skipVerify)

	fireboltClientsMu.Lock()
	defer fireboltClientsMu.Unlock()

	if client, ok := fireboltClients[key]; ok {
		return client
	}

	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        16,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	if skipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client := &http.Client{Transport: transport}
	fireboltClients[key] = client

	return client
}

func (c *fireboltConn) Prepare(query string) (driver.Stmt, error) {
	return &fireboltStmt{conn: c, query: query}, nil
}

func (c *fireboltConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

func (c *fireboltConn) Close() error {
	// The HTTP client is shared process-wide (see fireboltHTTPClient), so a
	// driver connection must not tear down its idle sockets: that would close
	// the sockets of every other session in the pool. Idle connections expire
	// on their own via the transport's IdleConnTimeout.
	return nil
}

func (c *fireboltConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fireboltConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.ReadOnly {
		return nil, g.Error("Firebolt does not support read-only transactions")
	}
	// BEGIN TRANSACTION replies with the transaction id to pass on every
	// following request of the same transaction.
	if _, err := c.execContext(ctx, "BEGIN TRANSACTION", nil); err != nil {
		return nil, g.Error(err, "could not begin Firebolt transaction")
	}
	c.mux.Lock()
	txID := c.params["transaction_id"]
	c.mux.Unlock()
	if txID == "" {
		return nil, g.Error("Firebolt did not return a transaction_id")
	}
	return &fireboltTx{conn: c}, nil
}

func (c *fireboltConn) Ping(ctx context.Context) error {
	_, err := c.execContext(ctx, "SELECT 1", nil)
	return err
}

// CheckNamedValue converts any value sling hands over into a Firebolt parameter
// value. Maps and slices (JSON columns) are marshalled to JSON text.
func (c *fireboltConn) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case nil, string, []byte, bool, time.Time, int64, float64:
		return nil
	case json.RawMessage:
		nv.Value = string(nv.Value.(json.RawMessage))
		return nil
	}

	val, err := driver.DefaultParameterConverter.ConvertValue(nv.Value)
	if err != nil {
		// fall back to JSON text for structured values
		bytes, jErr := json.Marshal(nv.Value)
		if jErr != nil {
			return err
		}
		nv.Value = string(bytes)
		return nil
	}
	nv.Value = val
	return nil
}

func (c *fireboltConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.execContext(ctx, query, args)
}

func (c *fireboltConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.queryContext(ctx, query, args)
}

func (c *fireboltConn) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Firebolt only accepts one statement per request
	for _, sql := range ParseSQLMultiStatements(query, dbio.TypeDbFirebolt) {
		if strings.TrimSpace(sql) == "" {
			continue
		}
		resp, err := c.do(ctx, sql, args)
		if err != nil {
			return nil, err
		}
		_, err = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, g.Error(err, "error reading Firebolt response")
		}
	}
	return fireboltResult{}, nil
}

func (c *fireboltConn) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	resp, err := c.do(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return newFireboltRows(c, resp)
}

// do sends one statement and returns the streaming response.
func (c *fireboltConn) do(ctx context.Context, query string, args []driver.NamedValue) (*http.Response, error) {
	c.mux.Lock()
	values := url.Values{"output_format": {fireboltOutputFormat}}
	for k, v := range c.params {
		values.Set(k, v)
	}
	c.mux.Unlock()

	if len(args) > 0 {
		paramsJSON, err := fireboltQueryParameters(args)
		if err != nil {
			return nil, err
		}
		values.Set(fireboltQueryParamsKV, paramsJSON)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/?"+values.Encode(), strings.NewReader(query))
	if err != nil {
		return nil, g.Error(err, "could not build Firebolt request")
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	req.Header.Set("User-Agent", "sling")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, g.Error(err, "could not reach Firebolt engine")
	}

	c.applySessionHeaders(resp.Header)

	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return nil, g.Error("Firebolt SQL Error: %s", fireboltErrorFrom(body))
	}

	return resp, nil
}

// applySessionHeaders keeps track of the connection parameters the engine asks
// the client to send on the following requests (database, transaction_id,
// transaction_sequence_id, ...). Both headers repeat once per parameter, so
// every value must be read: `header.Get` would only return the first one and
// leave a stale transaction_sequence_id behind.
func (c *fireboltConn) applySessionHeaders(header http.Header) {
	updates := header.Values(fireboltUpdateParams)
	removes := header.Values(fireboltRemoveParams)
	if len(updates) == 0 && len(removes) == 0 {
		return
	}

	c.mux.Lock()
	defer c.mux.Unlock()
	for _, raw := range updates {
		for _, pair := range strings.Split(raw, ",") {
			kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
			if len(kv) == 2 && kv[0] != "" {
				c.params[kv[0]] = kv[1]
			}
		}
	}
	for _, raw := range removes {
		for _, key := range strings.Split(raw, ",") {
			delete(c.params, strings.TrimSpace(key))
		}
	}
}

// fireboltTx runs statements inside an explicit Firebolt transaction.
type fireboltTx struct {
	conn *fireboltConn
	done bool
}

func (tx *fireboltTx) Commit() error   { return tx.finish("COMMIT TRANSACTION") }
func (tx *fireboltTx) Rollback() error { return tx.finish("ROLLBACK TRANSACTION") }

func (tx *fireboltTx) finish(statement string) error {
	if tx.done {
		return nil
	}
	tx.done = true
	_, err := tx.conn.execContext(context.Background(), statement, nil)
	return err
}

type fireboltStmt struct {
	conn  *fireboltConn
	query string
}

func (s *fireboltStmt) Close() error  { return nil }
func (s *fireboltStmt) NumInput() int { return -1 } // placeholders are server-side

func (s *fireboltStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), fireboltNamedValues(args))
}

func (s *fireboltStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), fireboltNamedValues(args))
}

func (s *fireboltStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.execContext(ctx, s.query, args)
}

func (s *fireboltStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.queryContext(ctx, s.query, args)
}

func fireboltNamedValues(args []driver.Value) []driver.NamedValue {
	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return named
}

type fireboltResult struct{}

func (r fireboltResult) LastInsertId() (int64, error) { return 0, nil }
func (r fireboltResult) RowsAffected() (int64, error) { return 0, nil }

// fireboltQueryParameters renders `$1, $2, ...` bind variables as the
// query_parameters JSON array the engine expects.
func fireboltQueryParameters(args []driver.NamedValue) (string, error) {
	type param struct {
		Name  string `json:"name"`
		Value any    `json:"value"`
	}

	params := make([]param, len(args))
	for i, arg := range args {
		ordinal := arg.Ordinal
		if ordinal == 0 {
			ordinal = i + 1
		}
		value, err := fireboltParamValue(arg.Value)
		if err != nil {
			return "", err
		}
		params[i] = param{Name: g.F("$%d", ordinal), Value: value}
	}

	bytes, err := json.Marshal(params)
	if err != nil {
		return "", g.Error(err, "could not encode Firebolt query parameters")
	}
	return string(bytes), nil
}

// fireboltParamValue converts a Go value into the string form the engine
// accepts as a query parameter (null is sent as JSON null).
func fireboltParamValue(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case string:
		return v, nil
	case []byte:
		return "\\x" + hex.EncodeToString(v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case time.Time:
		// date-only values (midnight UTC) are sent as dates, so a date column
		// does not receive a time part
		if v.Truncate(24 * time.Hour).Equal(v) {
			return v.Format("2006-01-02"), nil
		}
		if _, offset := v.Zone(); offset != 0 {
			return v.Format("2006-01-02 15:04:05.000000-07:00"), nil
		}
		return v.Format("2006-01-02 15:04:05.000000"), nil
	case json.RawMessage:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	}

	// structured values (maps, slices) are sent as JSON text
	if bytes, err := json.Marshal(value); err == nil {
		return string(bytes), nil
	}
	return nil, g.Error("unsupported Firebolt parameter type: %T", value)
}

// ---------------------------------------------------------------- rows

// fireboltRows streams the JSONLines_Compact response of one statement.
type fireboltRows struct {
	conn   *fireboltConn
	resp   *http.Response
	reader *bufio.Reader

	columns  []string
	dbTypes  []string
	nullable []bool
	lengths  []int64
	precs    []int64
	scales   []int64

	chunk [][]any
	pos   int
	done  bool
	err   error
}

// fireboltMessages are the JSONLines_Compact message types
type fireboltMessage struct {
	MessageType    string          `json:"message_type"`
	ResultColumns  []fireboltCol   `json:"result_columns"`
	Data           [][]any         `json:"data"`
	Errors         []fireboltErr   `json:"errors"`
	QueryID        string          `json:"query_id"`
	QueryLabel     *string         `json:"query_label"`
	RequestID      string          `json:"request_id"`
	Statistics     json.RawMessage `json:"statistics"`
	UpdateEndpoint string          `json:"update_endpoint"`
}

type fireboltCol struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type fireboltErr struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

func newFireboltRows(conn *fireboltConn, resp *http.Response) (*fireboltRows, error) {
	rows := &fireboltRows{
		conn:   conn,
		resp:   resp,
		reader: bufio.NewReaderSize(resp.Body, 1024*1024),
	}

	// the START message carries the column list and must be read eagerly so the
	// driver can report columns before iterating
	for {
		msg, err := rows.readMessage()
		if err != nil {
			resp.Body.Close()
			return nil, err
		} else if msg == nil {
			resp.Body.Close()
			return nil, g.Error("Firebolt returned no result columns")
		}

		switch msg.MessageType {
		case "START":
			rows.setColumns(msg.ResultColumns)
			return rows, nil
		case "FINISH_WITH_ERRORS", "ERROR":
			resp.Body.Close()
			return nil, g.Error("Firebolt SQL Error: %s", fireboltErrors(msg.Errors))
		case "FINISH_SUCCESSFULLY":
			resp.Body.Close()
			return rows, nil
		}
	}
}

func (r *fireboltRows) setColumns(cols []fireboltCol) {
	r.columns = make([]string, len(cols))
	r.dbTypes = make([]string, len(cols))
	r.nullable = make([]bool, len(cols))
	r.lengths = make([]int64, len(cols))
	r.precs = make([]int64, len(cols))
	r.scales = make([]int64, len(cols))

	for i, col := range cols {
		dbType := col.Type
		if strings.HasSuffix(dbType, fireboltNullSuffix) {
			dbType = strings.TrimSuffix(dbType, fireboltNullSuffix)
			r.nullable[i] = true
		}

		r.columns[i] = col.Name
		r.dbTypes[i] = dbType

		if precision, scale, ok := fireboltDecimalSize(dbType); ok {
			r.precs[i], r.scales[i] = precision, scale
		}
	}
}

func (r *fireboltRows) Columns() []string { return r.columns }

func (r *fireboltRows) Close() error {
	if !r.done {
		r.done = true
		r.resp.Body.Close()
	}
	return nil
}

func (r *fireboltRows) Next(dest []driver.Value) error {
	for {
		if r.err != nil {
			return r.err
		} else if r.pos < len(r.chunk) {
			row := r.chunk[r.pos]
			r.pos++
			for i := range dest {
				if i >= len(row) {
					dest[i] = nil
					continue
				}
				value, err := fireboltRowValue(row[i], r.dbTypes[i])
				if err != nil {
					r.err = err
					return err
				}
				dest[i] = value
			}
			return nil
		} else if r.done {
			return io.EOF
		}

		msg, err := r.readMessage()
		if err != nil {
			r.err = err
			return err
		} else if msg == nil {
			r.done = true
			return io.EOF
		}

		switch msg.MessageType {
		case "DATA":
			r.chunk = msg.Data
			r.pos = 0
		case "FINISH_WITH_ERRORS", "ERROR":
			r.err = g.Error("Firebolt SQL Error: %s", fireboltErrors(msg.Errors))
			r.done = true
			return r.err
		case "FINISH_SUCCESSFULLY":
			r.done = true
		}
	}
}

func (r *fireboltRows) readMessage() (*fireboltMessage, error) {
	for {
		line, err := r.reader.ReadBytes('\n')
		if len(line) > 0 {
			line = []byte(strings.TrimSpace(string(line)))
			if len(line) == 0 {
				continue
			}
			msg := &fireboltMessage{}
			if err := json.Unmarshal(line, msg); err != nil {
				return nil, g.Error(err, "could not parse Firebolt response: %s", g.F("%.200s", line))
			}
			return msg, nil
		}
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, g.Error(err, "error reading Firebolt response")
		}
	}
}

func (r *fireboltRows) ColumnTypeDatabaseTypeName(index int) string {
	return r.dbTypes[index]
}

func (r *fireboltRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.nullable[index], true
}

func (r *fireboltRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if r.lengths[index] > 0 {
		return r.lengths[index], true
	}
	return 0, false
}

func (r *fireboltRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	precision, scale = r.precs[index], r.scales[index]
	return precision, scale, precision > 0
}

// ---------------------------------------------------------------- values

// fireboltRowValue converts a JSON value from the result stream into the Go
// value sling expects for the given Firebolt type.
func fireboltRowValue(raw any, dbType string) (driver.Value, error) {
	if raw == nil {
		return nil, nil
	}

	switch fireboltBaseType(dbType) {
	case "bigint", "long", "hugeint", "integer", "int", "smallint", "tinyint":
		switch v := raw.(type) {
		case string:
			return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		case float64:
			return int64(v), nil
		case bool:
			return cast.ToInt64(v), nil
		}
	case "double", "double precision", "real", "float":
		switch v := raw.(type) {
		case string:
			return strconv.ParseFloat(strings.TrimSpace(v), 64)
		case float64:
			return v, nil
		case bool:
			return cast.ToFloat64(v), nil
		}
	case "numeric", "decimal":
		// decimals arrive as strings to preserve precision
		return fireboltToString(raw), nil
	case "boolean", "bool":
		switch v := raw.(type) {
		case bool:
			return v, nil
		case string:
			return cast.ToBool(v), nil
		case float64:
			return v != 0, nil
		}
	case "date":
		return fireboltParseTime(fireboltToString(raw), "2006-01-02")
	case "timestamp", "timestampntz":
		return fireboltParseTime(fireboltToString(raw), fireboltTimestampLayouts...)
	case "timestamptz":
		return fireboltParseTime(fireboltToString(raw), fireboltTimestampzLayouts...)
	case "bytea":
		return fireboltParseBytes(fireboltToString(raw))
	case "json", "array", "struct", "geography":
		return fireboltJSONText(raw), nil
	}

	switch v := raw.(type) {
	case string, bool, float64, nil:
		return v, nil
	}
	return fireboltToString(raw), nil
}

var fireboltTimestampLayouts = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
}

var fireboltTimestampzLayouts = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05-07",
	"2006-01-02T15:04:05.999999999Z07:00",
}

func fireboltParseTime(value string, layouts ...string) (driver.Value, error) {
	value = strings.TrimSpace(value)
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	// leave unparseable values untouched so nothing is silently lost
	return value, nil
}

func fireboltParseBytes(value string) (driver.Value, error) {
	value = strings.TrimSpace(value)
	if hexValue, ok := strings.CutPrefix(value, "\\x"); ok {
		bytes, err := hex.DecodeString(hexValue)
		if err != nil {
			return nil, g.Error(err, "could not decode Firebolt bytea value")
		}
		return bytes, nil
	}
	return []byte(value), nil
}

func fireboltJSONText(raw any) string {
	if text, ok := raw.(string); ok {
		return text
	} else if bytes, err := json.Marshal(raw); err == nil {
		return string(bytes)
	}
	return fmt.Sprint(raw)
}

func fireboltToString(raw any) string {
	if text, ok := raw.(string); ok {
		return text
	}
	return fmt.Sprint(raw)
}

// fireboltBaseType returns the type name without its parameters, so
// `numeric(38, 9)` and `array(integer null)` resolve to `numeric` / `array`.
func fireboltBaseType(dbType string) string {
	base, _, _ := strings.Cut(strings.ToLower(strings.TrimSpace(dbType)), "(")
	return strings.TrimSpace(base)
}

func fireboltDecimalSize(dbType string) (precision, scale int64, ok bool) {
	lowered := strings.ToLower(strings.TrimSpace(dbType))
	if !strings.HasPrefix(lowered, "numeric(") && !strings.HasPrefix(lowered, "decimal(") {
		return 0, 0, false
	}
	inner, _, found := strings.Cut(strings.TrimPrefix(strings.TrimPrefix(lowered, "numeric("), "decimal("), ")")
	if !found {
		return 0, 0, false
	}
	parts := strings.Split(inner, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	precision, errP := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	scale, errS := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if errP != nil || errS != nil {
		return 0, 0, false
	}
	return precision, scale, true
}

// fireboltErrorFrom extracts the error descriptions from a non-200 response.
func fireboltErrorFrom(body []byte) string {
	msg := fireboltMessage{}
	if err := json.Unmarshal(body, &msg); err == nil && len(msg.Errors) > 0 {
		return fireboltErrors(msg.Errors)
	}
	return strings.TrimSpace(string(body))
}

func fireboltErrors(errs []fireboltErr) string {
	descriptions := make([]string, len(errs))
	for i, e := range errs {
		descriptions[i] = e.Description
	}
	return strings.Join(descriptions, "; ")
}
