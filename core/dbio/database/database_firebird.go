package database

import (
	"strings"

	_ "github.com/nakagami/firebirdsql"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/xo/dburl"

	"github.com/flarco/g"
)

// FirebirdConn is a Firebird connection
type FirebirdConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *FirebirdConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbFirebird
	conn.BaseConn.defaultPort = 3050

	// Turn off Bulk export/import for now due to hanging issues
	conn.BaseConn.SetProp("allow_bulk_export", "false")
	conn.BaseConn.SetProp("allow_bulk_import", "false")
	
	// Set batch limit to 1 to force single-row inserts
	conn.BaseConn.SetProp("batch_limit", "1")

	// Firebird doesn't support schemas, but we use 'main' as default
	conn.BaseConn.SetProp("default_schema", "main")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *FirebirdConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	u, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse Firebird URL")
		return connURL
	}

	// param keys from https://github.com/nakagami/firebirdsql
	paramKeyMapping := map[string]string{
		"auth_plugin_name": "auth_plugin_name",
		"role":             "role",
		"timezone":         "timezone",
		"wire_crypt":       "wire_crypt",
		"charset":          "charset",
		"timeout":          "timeout",
	}

	query := u.Query()
	for key, libKey := range paramKeyMapping {
		if val := conn.GetProp(key); val != "" {
			query.Set(libKey, val)
		}
		if libKey != key {
			if val := conn.GetProp(libKey); val != "" {
				query.Set(libKey, val)
			}
		}
	}

	// reconstruct the url
	u.RawQuery = query.Encode()
	u, err = dburl.Parse(u.String())
	if err != nil {
		g.LogError(err, "could not parse Firebird URL")
		return connURL
	}

	return u.DSN
}

// GenerateDDL generates a DDL based on a dataset
func (conn *FirebirdConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	// Firebird doesn't support schemas, so strip schema from table name
	ddl = strings.ReplaceAll(ddl, table.FDQN(), table.NameQ())

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	for _, index := range table.Indexes(data.Columns) {
		indexDDL := strings.ReplaceAll(index.CreateDDL(), table.FDQN(), table.NameQ()) // doesn't like FDQN
		ddl = ddl + ";\n" + indexDDL
	}

	return ddl, nil
}

// GetSchemas returns a list of schemas
func (conn *FirebirdConn) GetSchemas() (iop.Dataset, error) {
	// Firebird doesn't support schemas, return only 'main'
	return conn.BaseConn.GetSchemas()
}

// CreateSchema creates a schema
func (conn *FirebirdConn) CreateSchema(schema string) error {
	// Firebird doesn't support schemas, ignore
	return nil
}

// DropSchema drops a schema
func (conn *FirebirdConn) DropSchema(schema string) error {
	// Firebird doesn't support schemas, ignore
	return nil
}

// GetSQLColumns returns the columns for a table
func (conn *FirebirdConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	// For Firebird, we need to use just the table name without schema
	_, tableName := SplitTableFullName(table.Name)
	table.Name = tableName
	return conn.BaseConn.GetSQLColumns(table)
}

// GetColumns returns the columns for a table
func (conn *FirebirdConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	// For Firebird, we need to use just the table name without schema
	_, tableName := SplitTableFullName(tableFName)

	// Strip quotes from table name for metadata queries
	tableNameUnquoted := strings.Trim(tableName, `"`)

	return conn.BaseConn.GetColumns(tableNameUnquoted, fields...)
}

// DropTable drops a table
func (conn *FirebirdConn) DropTable(tableNames ...string) error {
	for _, tableName := range tableNames {
		// Strip schema from table name
		_, tblName := SplitTableFullName(tableName)
		sql := g.R(
			conn.GetTemplateValue("core.drop_table"),
			"table", conn.Quote(tblName),
		)
		_, err := conn.Exec(sql)
		if err != nil {
			return g.Error(err, "Error for "+sql)
		}
	}
	return nil
}

// CreateTable creates a table
func (conn *FirebirdConn) CreateTable(tableName string, cols iop.Columns, tableDDL string) error {
	// Strip schema from table name
	_, tblName := SplitTableFullName(tableName)

	// Replace table name in DDL with just the table name
	if tableDDL != "" {
		tableDDL = strings.ReplaceAll(tableDDL, conn.Quote("main")+"."+conn.Quote(tblName), conn.Quote(tblName))
	}

	return conn.BaseConn.CreateTable(tblName, cols, tableDDL)
}

// GenerateInsertStatement returns the proper INSERT statement for Firebird
func (conn *FirebirdConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	// Strip schema from table name since Firebird doesn't support schemas
	_, tblName := SplitTableFullName(tableName)
	
	// Firebird doesn't support multi-row inserts, so we always generate for single row
	// The actual batching will be handled by executing multiple single-row inserts
	return conn.BaseConn.GenerateInsertStatement(conn.Quote(tblName), cols, 1)
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *FirebirdConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// Strip schema from table name since Firebird doesn't support schemas
	_, tblName := SplitTableFullName(tableFName)
	
	// Use just the table name without schema
	// Call the original InsertBatchStream which should work with our 
	// GenerateInsertStatement override that returns single-row statements
	return InsertBatchStream(conn, conn.tx, conn.Quote(tblName), ds)
}
