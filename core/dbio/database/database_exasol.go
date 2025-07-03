package database

import (
	"fmt"
	"os"
	"path"
	"strings"

	_ "github.com/exasol/exasol-driver-go"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// ExasolConn is a Exasol connection
type ExasolConn struct {
	BaseConn
	URL string
}

// Init initiates the connection
func (conn *ExasolConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbExasol
	conn.BaseConn.defaultPort = 8563

	// exasol driver is auto-registered on import

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *ExasolConn) GetURL(newURL ...string) string {
	if len(newURL) > 0 {
		conn.BaseConn.URL = newURL[0]
	}
	// Exasol driver requires specific connection string format
	// DSN format: "exa:<host>:<port>;user=<username>;password=<password>;autocommit=0"

	// Extract connection properties
	host := cast.ToString(conn.GetProp("host"))
	port := cast.ToString(conn.GetProp("port"))
	username := cast.ToString(conn.GetProp("username"))
	password := cast.ToString(conn.GetProp("password"))
	schema := cast.ToString(conn.GetProp("schema"))

	// Use default port if not specified
	if port == "" {
		port = "8563"
	}

	if host == "" && conn.BaseConn.URL != "" {
		return conn.BaseConn.URL // if only url was provided
	}

	// Build the connection string
	connURL := fmt.Sprintf("exa:%s:%s;user=%s;password=%s;autocommit=0", host, port, username, password)

	// Add schema if specified
	if schema != "" {
		connURL += fmt.Sprintf(";schema=%s", schema)
	}

	return connURL
}

// Connect connects to the database
func (conn *ExasolConn) Connect(timeOut ...int) error {
	err := conn.BaseConn.Connect(timeOut...)
	if err != nil {
		// Provide more context for common connection errors
		errStr := strings.ToLower(err.Error())
		if strings.Contains(errStr, "no such host") || strings.Contains(errStr, "connection refused") {
			return g.Error(err, "Failed to connect to Exasol. Please verify host and port are correct.")
		} else if strings.Contains(errStr, "authentication failed") || strings.Contains(errStr, "invalid credentials") {
			return g.Error(err, "Authentication failed. Please verify username and password.")
		}
		return g.Error(err, "Failed to connect to Exasol")
	}

	// Set schema if provided
	if schema := cast.ToString(conn.GetProp("schema")); schema != "" {
		_, err = conn.Exec(fmt.Sprintf("OPEN SCHEMA %s", conn.Quote(schema)))
		if err != nil {
			return g.Error(err, "Failed to set schema")
		}
	}

	return nil
}

// GenerateDDL generates DDL for Exasol
func (conn *ExasolConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {
	// Use the template system for DDL generation
	sql, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, err
	}

	// Add any Exasol-specific modifications if needed
	return sql, nil
}

// BulkImportStream performs bulk import for Exasol
func (conn *ExasolConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// For now, use standard insert mode
	return conn.BaseConn.InsertBatchStream(tableFName, ds)
}

// BulkImportFlow performs bulk import for Exasol using temporary CSV files
func (conn *ExasolConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	context := g.NewContext(conn.Context().Ctx)

	// Parse table name
	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		return 0, g.Error(err, "could not parse table name")
	}

	// Get target columns
	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		return 0, g.Error(err, "could not get columns for %s", table.FullName())
	}

	columns, err = conn.ValidateColumnNames(columns, df.Columns.Names())
	if err != nil {
		return 0, g.Error(err, "could not validate columns prior to IMPORT for %s", table.FullName())
	}

	// Create temporary folder for CSV files
	folderPath := path.Join(env.GetTempFolder(), "exasol", "import", env.CleanTableName(tableFName), g.NowFileStr())

	// Delete folder when done
	df.Defer(func() { env.RemoveAllLocalTempFile(folderPath) })

	// Channel to receive files as they're written
	fileReadyChn := make(chan filesys.FileReady, 10000)

	// Write dataflow to CSV files
	go func() {
		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArrExclude("url")...)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Could not get fs client for Local"))
			return
		}

		config := iop.LoaderStreamConfig(true)
		config.TargetType = conn.GetType()
		config.Format = dbio.FileTypeCsv
		config.FileMaxRows = cast.ToInt64(conn.GetProp("file_max_rows"))
		if config.FileMaxRows == 0 {
			config.FileMaxRows = 500000
		}
		config.Header = true
		config.Delimiter = ","

		_, err = fs.WriteDataflowReady(df, folderPath, fileReadyChn, config)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error writing dataflow to disk: "+folderPath))
			return
		}
	}()

	// Build column list once
	colNames := []string{}
	for _, col := range columns {
		colNames = append(colNames, conn.Quote(col.Name))
	}

	// Process files as they become ready
	processFile := func(file filesys.FileReady) {
		defer context.Wg.Write.Done()
		defer func() { env.RemoveLocalTempFile(file.Node.Path()) }()

		// Make file readable
		os.Chmod(file.Node.Path(), 0644)

		// Build and execute IMPORT statement for this file
		importSQL := fmt.Sprintf(`IMPORT INTO %s (%s) FROM LOCAL CSV FILE '%s'
  COLUMN SEPARATOR = ','
  COLUMN DELIMITER = '"'
  ROW SEPARATOR = 'LF'
  SKIP = 1`,
			tableFName,
			strings.Join(colNames, ", "),
			file.Node.Path(),
		)

		g.Debug("Executing Exasol IMPORT: %s", importSQL)

		_, err := conn.Exec(importSQL)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error executing IMPORT INTO for file: %s", file.Node.Path()))
		}
	}

	// Wait for all files to be written and imported
	for file := range fileReadyChn {
		context.Wg.Write.Add()
		go processFile(file)
	}

	// Wait for all files to be processed
	context.Wg.Write.Wait()

	if df.Err() != nil {
		return count, df.Err()
	}

	// Count is tracked by the dataflow
	count = df.Count()

	return count, nil
}

// BulkExportStream performs bulk export for Exasol
func (conn *ExasolConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// For now, use standard select streaming
	sql := conn.GetTemplateValue("core.select_all")
	sql = strings.ReplaceAll(sql, "{table}", table.FDQN())
	return conn.BaseConn.StreamRows(sql)
}
