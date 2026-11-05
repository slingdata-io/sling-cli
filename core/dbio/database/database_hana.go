package database

import (
	"bytes"
	"context"
	"database/sql"
	"net/url"
	"strings"
	"time"

	"github.com/SAP/go-hdb/driver"
	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// HanaConn is a SAP HANA connection
type HanaConn struct {
	BaseConn
	URL string
}

// Init initiates the connection
func (conn *HanaConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbHana
	conn.BaseConn.defaultPort = dbio.TypeDbHana.DefPort()

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the go-hdb DSN. go-hdb rejects unknown query keys, so the DSN
// is built only from the keys that it supports.
func (conn *HanaConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	u, err := url.Parse(connURL)
	if err != nil {
		return connURL
	}
	u.Scheme = "hdb"
	u.Path = ""

	if user := conn.GetProp("user", "username"); user != "" {
		u.User = url.UserPassword(user, conn.GetProp("password"))
	}

	q := url.Values{}
	setParam := func(key string, props ...string) {
		if val := conn.GetProp(props...); val != "" {
			q.Set(key, val)
		}
	}
	setParam(driver.DSNDatabaseName, driver.DSNDatabaseName, "database")
	setParam(driver.DSNDefaultSchema, driver.DSNDefaultSchema, "schema")
	setParam(driver.DSNTimeout, driver.DSNTimeout, "timeout")
	setParam(driver.DSNPingInterval, driver.DSNPingInterval, "ping_interval")
	setParam(driver.DSNTLSServerName, driver.DSNTLSServerName, "tls_server_name")
	setParam(driver.DSNTLSRootCAFile, driver.DSNTLSRootCAFile, "tls_root_ca_file")
	setParam(driver.DSNTLSInsecureSkipVerify, driver.DSNTLSInsecureSkipVerify, "tls_skip_verify")

	// HANA Cloud requires TLS: a server name turns it on in go-hdb.
	// Use the original host: an SSH tunnel changes the URL host to 127.0.0.1.
	if cast.ToBool(conn.GetProp("encrypt")) && q.Get(driver.DSNTLSServerName) == "" {
		host := conn.GetProp("host")
		if origURL, err := url.Parse(conn.BaseConn.URL); host == "" && err == nil {
			host = origURL.Hostname()
		}
		q.Set(driver.DSNTLSServerName, host)
	}

	u.RawQuery = q.Encode()
	return u.String()
}

// NewTransaction creates a new transaction
func (conn *HanaConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {
	// HANA rejects explicit isolation levels other than its supported ones
	return conn.BaseConn.NewTransaction(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
}

// BulkImportStream inserts with go-hdb bulk exec (see GenerateInsertStatement)
func (conn *HanaConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BaseConn.InsertBatchStream(tableFName, ds)
}

// GenerateInsertStatement returns a single-row insert. go-hdb runs it as one
// bulk operation when it gets the values of many rows.
func (conn *HanaConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	return conn.BaseConn.GenerateInsertStatement(tableName, cols, 1)
}

// GenerateMergeSQL generates the upsert SQL using the default strategy
func (conn *HanaConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.BaseConn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, nil)
}

// processHanaInsertRow converts values that go-hdb cannot bind to the target column type
func processHanaInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		if row[i] == nil || i >= len(columns) {
			continue
		}

		dbType := strings.ToUpper(columns[i].DbType)
		switch {
		case g.In(dbType, "NVARCHAR", "VARCHAR", "NCHAR", "CHAR", "ALPHANUM", "SHORTTEXT", "NCLOB", "CLOB", "TEXT", "BINTEXT"):
			switch v := row[i].(type) {
			case string, []byte:
			case time.Time:
				row[i] = v.Format(time.RFC3339Nano)
			case map[string]any, []any:
				row[i] = g.Marshal(v)
			default:
				row[i] = cast.ToString(v)
			}
		case g.In(dbType, "BOOLEAN"):
			if _, ok := row[i].(bool); !ok {
				row[i] = cast.ToBool(row[i])
			}
		case g.In(dbType, "VARBINARY", "BINARY", "BLOB"):
			if v, ok := row[i].(string); ok {
				row[i] = []byte(v)
			}
		}
	}
	return row
}

// scanHanaRow scans a row. go-hdb returns a LOB locator for an `any` target,
// so LOB columns are scanned into a NullLob.
func scanHanaRow(rows *sqlx.Rows, colTypes []ColumnType) ([]any, error) {
	dests := make([]any, len(colTypes))
	for i, ct := range colTypes {
		switch strings.ToUpper(ct.DatabaseTypeName) {
		case "NCLOB", "CLOB", "TEXT", "BINTEXT", "BLOB":
			dests[i] = &driver.NullLob{Lob: driver.NewLob(nil, new(bytes.Buffer))}
		default:
			dests[i] = new(any)
		}
	}

	if err := rows.Scan(dests...); err != nil {
		return nil, err
	}

	row := make([]any, len(dests))
	for i, dest := range dests {
		switch d := dest.(type) {
		case *driver.NullLob:
			if !d.Valid {
				continue
			}
			buf := d.Lob.Writer().(*bytes.Buffer)
			if strings.ToUpper(colTypes[i].DatabaseTypeName) == "BLOB" {
				row[i] = buf.Bytes()
			} else {
				row[i] = buf.String()
			}
		case *any:
			row[i] = *d
		}
	}
	return row, nil
}
