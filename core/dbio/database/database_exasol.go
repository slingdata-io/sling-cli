package database

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	_ "github.com/exasol/exasol-driver-go"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/flarco/g/net"
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

	// Property mapping for Exasol connection options
	propMapping := map[string]string{
		"validateservercertificate":   "validateservercertificate",
		"validate_server_certificate": "validateservercertificate",

		"encryption": "encryption",

		"connecttimeout":     "connecttimeout",
		"connect_timeout":    "connecttimeout",
		"connection_timeout": "connecttimeout",

		"logintimeout":  "logintimeout",
		"login_timeout": "logintimeout",

		"compression": "compression",

		"fetchsize":  "fetchsize",
		"fetch_size": "fetchsize",

		"autocommit": "autocommit",

		"clientname":  "clientname",
		"client_name": "clientname",

		"clientversion":  "clientversion",
		"client_version": "clientversion",

		"certificatefingerprint":  "certificatefingerprint",
		"certificate_fingerprint": "certificatefingerprint",

		"feedbackinterval":  "feedbackinterval",
		"feedback_interval": "feedbackinterval",

		"querytimeout":  "querytimeout",
		"query_timeout": "querytimeout",

		"resultsetmaxrows":   "resultsetmaxrows",
		"resultset_max_rows": "resultsetmaxrows",

		"sessionidprefix":   "sessionidprefix",
		"session_id_prefix": "sessionidprefix",

		"snapshottransactions":  "snapshottransactions",
		"snapshot_transactions": "snapshottransactions",
	}

	// makeDSN converts a net.URL to Exasol DSN format
	makeDSN := func(U *net.URL) string {
		// Extract connection properties from URL
		host := U.Hostname()
		port := cast.ToString(U.Port())
		username := U.Username()
		password := U.Password()

		// Use connection properties as fallback
		if host == "" {
			host = cast.ToString(conn.GetProp("host"))
		}
		if port == "" || port == "0" {
			port = cast.ToString(conn.GetProp("port"))
		}
		if username == "" {
			username = cast.ToString(conn.GetProp("username"))
		}
		if password == "" {
			password = cast.ToString(conn.GetProp("password"))
		}

		// Use default port if not specified
		if port == "" || port == "0" {
			port = "8563"
		}

		// Build the connection string
		// Exasol driver requires specific connection string format
		// DSN format: "exa:<host>:<port>;user=<username>;password=<password>;autocommit=0"
		connURL := fmt.Sprintf("exa:%s:%s;user=%s;password=%s", host, port, username, password)

		// Add autocommit (default to 0 if not specified)
		autocommit := cast.ToString(conn.GetProp("autocommit"))
		if autocommit == "" {
			autocommit = "0"
		}
		connURL += fmt.Sprintf(";autocommit=%s", autocommit)

		// Add schema if specified
		schema := cast.ToString(conn.GetProp("schema"))
		if schema == "" && U.Path() != "" {
			// Extract schema from URL path
			schema = strings.Trim(U.Path(), "/")
		}
		if schema != "" {
			connURL += fmt.Sprintf(";schema=%s", schema)
		}

		// Add additional connection options from URL parameters and properties
		urlParams := U.Query()
		for origKey, mappedKey := range propMapping {
			var val string
			// First check URL parameters
			if urlVal, exists := urlParams[origKey]; exists && urlVal != "" {
				val = urlVal
			} else if urlVal, exists := urlParams[mappedKey]; exists && urlVal != "" {
				val = urlVal
			} else if propVal := conn.GetProp(origKey); propVal != "" {
				val = propVal
			}

			if val != "" && origKey != "autocommit" { // autocommit already handled above
				// Only add if not already present
				if !strings.Contains(connURL, mappedKey+"=") {
					connURL += fmt.Sprintf(";%s=%s", mappedKey, val)
				}
			}
		}

		return connURL
	}

	// Parse URL to extract and set parameters
	U, _ := net.NewURL(conn.BaseConn.URL)
	for key, newKey := range propMapping {
		if val := conn.GetProp(key); val != "" {
			U.SetParam(newKey, val)
		}
	}

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
		if strings.HasPrefix(conn.BaseConn.URL, "exasol://") {
			// translate to DSN format using makeDSN helper
			U, _ = net.NewURL(conn.BaseConn.URL)
			return makeDSN(U)
		}
		return conn.BaseConn.URL // if only url was provided
	}

	// If we have host info, use makeDSN helper to build DSN
	if host != "" {
		// Create a URL object with the connection details
		urlStr := fmt.Sprintf("exasol://%s:%s@%s:%s", username, password, host, port)
		if schema != "" {
			urlStr += "/" + schema
		}
		U, _ := net.NewURL(urlStr)
		return makeDSN(U)
	}

	return conn.BaseConn.URL
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

	// Write dataflow to CSV files with proper timestamp formatting
	go func() {
		defer close(fileReadyChn)

		// Process each datastream from the dataflow
		for ds := range df.StreamCh {
			// Create CSV files with proper timestamp formatting
			err := conn.writeDataflowToCSV(ds, folderPath, fileReadyChn)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Error writing dataflow to CSV files"))
				return
			}
		}
	}()

	// Build column list
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

// writeDataflowToCSV writes a datastream to CSV files with proper timestamp formatting for Exasol
func (conn *ExasolConn) writeDataflowToCSV(ds *iop.Datastream, folderPath string, fileReadyChn chan filesys.FileReady) error {
	fileMaxRows := cast.ToInt64(conn.GetProp("file_max_rows"))
	if fileMaxRows == 0 {
		fileMaxRows = 500000
	}

	fileNum := 1
	rowCount := int64(0)
	var currentFile *os.File
	var csvWriter *csv.Writer

	// Create folder if it doesn't exist
	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		return g.Error(err, "could not create folder: "+folderPath)
	}

	// Function to create a new CSV file
	createNewFile := func() error {
		if currentFile != nil {
			csvWriter.Flush()
			currentFile.Close()
		}

		fileName := fmt.Sprintf("part.%02d.%04d.csv", fileNum, fileNum)
		filePath := path.Join(folderPath, fileName)

		currentFile, err = os.Create(filePath)
		if err != nil {
			return g.Error(err, "could not create CSV file: "+filePath)
		}

		csvWriter = csv.NewWriter(currentFile)

		// Write header
		if _, err := csvWriter.Write(ds.Columns.Names()); err != nil {
			return g.Error(err, "could not write CSV header")
		}

		// Signal that file is ready for processing
		fileNode := filesys.FileNode{
			URI: "file://" + filePath,
		}
		fileReadyChn <- filesys.FileReady{
			Node: fileNode,
		}

		fileNum++
		rowCount = 0
		return nil
	}

	// Create first file
	if err := createNewFile(); err != nil {
		return err
	}

	// Process rows
	for row := range ds.Rows() {
		// Check if we need a new file
		if rowCount >= fileMaxRows {
			if err := createNewFile(); err != nil {
				return err
			}
		}

		// Format row values for Exasol
		formattedRow := make([]string, len(row))
		for i, val := range row {
			if val == nil {
				formattedRow[i] = ""
				continue
			}

			col := ds.Columns[i]
			switch col.Type {
			case iop.DateType:
				if t, ok := val.(time.Time); ok {
					formattedRow[i] = t.Format("2006-01-02 15:04:05")
				} else {
					formattedRow[i] = ds.Sp.CastToString(i, val, col.Type)
				}
			case iop.DatetimeType, iop.TimestampType:
				if t, ok := val.(time.Time); ok {
					formattedRow[i] = t.Format("2006-01-02 15:04:05.000000")
				} else {
					formattedRow[i] = ds.Sp.CastToString(i, val, col.Type)
				}
			case iop.TimestampzType:
				if t, ok := val.(time.Time); ok {
					// Convert to UTC for Exasol timestamp format
					formattedRow[i] = t.UTC().Format("2006-01-02 15:04:05.000000")
				} else {
					formattedRow[i] = ds.Sp.CastToString(i, val, col.Type)
				}
			default:
				formattedRow[i] = ds.Sp.CastToString(i, val, col.Type)
			}
		}

		if _, err := csvWriter.Write(formattedRow); err != nil {
			return g.Error(err, "could not write CSV row")
		}

		rowCount++
	}

	// Close last file
	if currentFile != nil {
		csvWriter.Flush()
		currentFile.Close()
	}

	return nil
}

// BulkExportStream performs bulk export for Exasol
func (conn *ExasolConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// For now, use standard select streaming
	sql := conn.GetTemplateValue("core.select_all")
	sql = strings.ReplaceAll(sql, "{table}", table.FDQN())
	return conn.BaseConn.StreamRows(sql)
}
