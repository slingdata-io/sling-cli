package database

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/exasol/exasol-driver-go"
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
// This method uses the official Exasol driver's configuration builder (exasol.NewConfig)
// to properly handle special characters in passwords and other connection parameters.
// This ensures passwords with backslashes, semicolons, etc. are correctly escaped.
func (conn *ExasolConn) GetURL(newURL ...string) string {
	if len(newURL) > 0 {
		conn.BaseConn.URL = newURL[0]
	}

	// If a complete URL is already provided and it's not an exasol:// scheme, return as-is
	if conn.BaseConn.URL != "" && !strings.HasPrefix(conn.BaseConn.URL, "exasol://") {
		return conn.BaseConn.URL
	}

	// Extract connection properties
	host := cast.ToString(conn.GetProp("host"))
	port := cast.ToInt(conn.GetProp("port"))
	username := cast.ToString(conn.GetProp("username"))
	password := cast.ToString(conn.GetProp("password"))
	schema := cast.ToString(conn.GetProp("schema"))

	// If we have an exasol:// URL, extract properties from it
	if strings.HasPrefix(conn.BaseConn.URL, "exasol://") {
		if U, err := net.NewURL(conn.BaseConn.URL); err == nil {
			if host == "" {
				host = U.Hostname()
			}
			if port == 0 {
				port = cast.ToInt(U.Port())
			}
			if username == "" {
				username = U.Username()
			}
			if password == "" {
				password = U.Password()
			}
			if schema == "" && U.Path() != "" {
				schema = strings.Trim(U.Path(), "/")
			}

			// Extract additional parameters from URL query parameters
			for param, values := range U.U.Query() {
				if len(values) > 0 && conn.GetProp(param) == "" {
					// Only set if not already set through connection properties
					conn.SetProp(param, values[0])
				}
			}
		}
	}

	// Use default port if not specified
	if port == 0 {
		port = 8563
	}

	// Validate required parameters
	if host == "" || username == "" {
		// If we don't have enough info, return the original URL
		return conn.BaseConn.URL
	}

	// Use Exasol driver's configuration builder for proper escaping
	config := exasol.NewConfig(username, password).
		Host(host).
		Port(port)

	// Add schema if specified
	if schema != "" {
		config = config.Schema(schema)
	}

	// Property mapping for Exasol connection options
	// Only include properties that are actually supported by the Exasol driver
	propMapping := map[string]string{
		"validateservercertificate":   "validateservercertificate",
		"validate_server_certificate": "validateservercertificate",
		"encryption":                  "encryption",
		"compression":                 "compression",
		"fetchsize":                   "fetchsize",
		"fetch_size":                  "fetchsize",
		"autocommit":                  "autocommit",
		"clientname":                  "clientname",
		"client_name":                 "clientname",
		"clientversion":               "clientversion",
		"client_version":              "clientversion",
		"certificatefingerprint":      "certificatefingerprint",
		"certificate_fingerprint":     "certificatefingerprint",
		"querytimeout":                "querytimeout",
		"query_timeout":               "querytimeout",
		"resultsetmaxrows":            "resultsetmaxrows",
		"resultset_max_rows":          "resultsetmaxrows",
	}

	// Apply connection options using the builder pattern
	for origKey, mappedKey := range propMapping {
		if val := conn.GetProp(origKey); val != "" {
			switch mappedKey {
			case "autocommit":
				if autocommit, err := strconv.ParseBool(val); err == nil {
					config = config.Autocommit(autocommit)
				} else if val == "1" {
					config = config.Autocommit(true)
				} else if val == "0" {
					config = config.Autocommit(false)
				}
			case "compression":
				if compression, err := strconv.ParseBool(val); err == nil {
					config = config.Compression(compression)
				} else if val == "1" {
					config = config.Compression(true)
				} else if val == "0" {
					config = config.Compression(false)
				}
			case "encryption":
				if encryption, err := strconv.ParseBool(val); err == nil {
					config = config.Encryption(encryption)
				} else if val == "1" {
					config = config.Encryption(true)
				} else if val == "0" {
					config = config.Encryption(false)
				}
			case "validateservercertificate":
				if validate, err := strconv.ParseBool(val); err == nil {
					config = config.ValidateServerCertificate(validate)
				} else if val == "1" {
					config = config.ValidateServerCertificate(true)
				} else if val == "0" {
					config = config.ValidateServerCertificate(false)
				}
			case "certificatefingerprint":
				config = config.CertificateFingerprint(val)
			case "fetchsize":
				if fetchsize, err := strconv.Atoi(val); err == nil {
					config = config.FetchSize(fetchsize)
				}
			case "querytimeout":
				if timeout, err := strconv.Atoi(val); err == nil {
					config = config.QueryTimeout(timeout)
				}
			case "resultsetmaxrows":
				if maxrows, err := strconv.Atoi(val); err == nil {
					config = config.ResultSetMaxRows(maxrows)
				}
			case "clientname":
				config = config.ClientName(val)
			case "clientversion":
				config = config.ClientVersion(val)
			}
		}
	}

	// Return the properly constructed DSN using the Exasol driver
	return config.String()
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
