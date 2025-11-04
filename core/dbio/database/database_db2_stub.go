//go:build !db2

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"

	"github.com/flarco/g"
)

// DB2Conn is a DB2 connection (stub implementation when DB2 support is not enabled)
type DB2Conn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *DB2Conn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDB2
	conn.BaseConn.defaultPort = 50000

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Connect connects to the database
func (conn *DB2Conn) Connect(timeOut ...int) error {
	return g.Error("DB2 support is not enabled in this build. Please rebuild with the 'db2' build tag and ensure DB2 CLI libraries are installed.")
}

// Close closes the connection
func (conn *DB2Conn) Close() error {
	return conn.BaseConn.Close()
}

// GetSchemas returns schemas for the DB2 database
func (conn *DB2Conn) GetSchemas() (iop.Dataset, error) {
	return conn.BaseConn.GetSchemas()
}

// GetTables returns tables for a schema
func (conn *DB2Conn) GetTables(schema string) (iop.Dataset, error) {
	return conn.BaseConn.GetTables(schema)
}

// GetViews returns views for a schema
func (conn *DB2Conn) GetViews(schema string) (iop.Dataset, error) {
	return conn.BaseConn.GetViews(schema)
}

// GetColumns returns columns for a table
func (conn *DB2Conn) GetColumns(tableName string, fields ...string) (iop.Columns, error) {
	return conn.BaseConn.GetColumns(tableName, fields...)
}

// BulkImportStream bulk imports a stream into a table
func (conn *DB2Conn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BaseConn.InsertBatchStream(tableFName, ds)
}

// BulkExportStream bulk exports a table to a stream
func (conn *DB2Conn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	return conn.BaseConn.BulkExportStream(table)
}

// GenerateDDL generates DDL for creating a table
func (conn *DB2Conn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {
	return conn.BaseConn.GenerateDDL(table, data, temporary)
}

// GenerateMergeSQL generates a MERGE statement for upserting data
func (conn *DB2Conn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.BaseConn.GenerateMergeSQL(srcTable, tgtTable, pkFields)
}

// GenerateUpsertSQL is an alias for GenerateMergeSQL
func (conn *DB2Conn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.GenerateMergeSQL(srcTable, tgtTable, pkFields)
}

// DropTable drops one or more tables in DB2
func (conn *DB2Conn) DropTable(tableNames ...string) (err error) {
	return conn.BaseConn.DropTable(tableNames...)
}
