package database

import (
	"context"
	"database/sql"
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
		Host(host).Port(port).Autocommit(false)

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
				if autocommit, err := cast.ToBoolE(val); err == nil {
					config = config.Autocommit(autocommit)
				} else if val == "1" {
					config = config.Autocommit(true)
				} else if val == "0" {
					config = config.Autocommit(false)
				}
			case "compression":
				if compression, err := cast.ToBoolE(val); err == nil {
					config = config.Compression(compression)
				} else if val == "1" {
					config = config.Compression(true)
				} else if val == "0" {
					config = config.Compression(false)
				}
			case "encryption":
				if encryption, err := cast.ToBoolE(val); err == nil {
					config = config.Encryption(encryption)
				} else if val == "1" {
					config = config.Encryption(true)
				} else if val == "0" {
					config = config.Encryption(false)
				}
			case "validateservercertificate":
				if validate, err := cast.ToBoolE(val); err == nil {
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

// NewTransaction creates a new transaction
func (conn *ExasolConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	if val := conn.GetProp("autocommit"); cast.ToBool(val) {
		return nil, nil // transactions not supported with auto-commit
	}

	return conn.BaseConn.NewTransaction(ctx, options...)
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
	// Exasol has issues with parameterized batch inserts in prepared statements
	// Use CSV-based import for all bulk operations
	df := iop.NewDataflow()

	// Set the columns on the dataflow from the datastream
	df.Columns = ds.Columns

	// Send the datastream through the channel and close it
	go func() {
		defer close(df.StreamCh)
		df.StreamCh <- ds
	}()

	return conn.BulkImportFlow(tableFName, df)
}

// BulkImportFlow performs bulk import for Exasol using temporary CSV files
func (conn *ExasolConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	context := g.NewContext(conn.Context().Ctx, 1) // 1 file load at a time

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

	// Build column list - only include columns that exist in the datastream
	colNames := []string{}
	dataStreamColMap := make(map[string]bool)
	for _, col := range df.Columns {
		dataStreamColMap[strings.ToLower(col.Name)] = true
	}

	for _, col := range columns {
		// Only include columns that exist in the datastream
		if dataStreamColMap[strings.ToLower(col.Name)] {
			colNames = append(colNames, conn.Quote(col.Name))
		}
	}

	// Process files as they become ready
	processFile := func(file filesys.FileReady) {
		defer context.Wg.Write.Done()
		defer func() { env.RemoveLocalTempFile(file.Node.Path()) }()

		// Make file readable
		os.Chmod(file.Node.Path(), 0644)

		// Build column format specifications for timestamp/date columns
		// We need to build ranges like: (1..3, 4 FORMAT='YYYY-MM-DD HH24:MI:SS.FF6', 5..7)
		var columnSpecs []string
		colIndex := 1
		rangeStart := 1

		// Track which columns need formatting and build ranges
		for _, col := range columns {
			// Only include columns that exist in the datastream
			if dataStreamColMap[strings.ToLower(col.Name)] {
				// Find the corresponding datastream column to get type information
				var dsCol *iop.Column
				for _, dCol := range df.Columns {
					if strings.EqualFold(dCol.Name, col.Name) {
						dsCol = &dCol
						break
					}
				}

				needsFormat := false
				var formatStr string
				if dsCol != nil {
					switch dsCol.Type {
					case iop.DateType:
						needsFormat = true
						formatStr = "YYYY-MM-DD HH24:MI:SS"
					case iop.DatetimeType, iop.TimestampType:
						needsFormat = true
						formatStr = "YYYY-MM-DD HH24:MI:SS.FF6"
					case iop.TimestampzType:
						needsFormat = true
						formatStr = "YYYY-MM-DD HH24:MI:SS.FF6Z"
					}
				}

				if needsFormat {
					// Close any previous range if needed
					if rangeStart < colIndex {
						if rangeStart == colIndex-1 {
							columnSpecs = append(columnSpecs, fmt.Sprintf("%d", rangeStart))
						} else {
							columnSpecs = append(columnSpecs, fmt.Sprintf("%d..%d", rangeStart, colIndex-1))
						}
					}

					// Add the formatted column
					columnSpecs = append(columnSpecs, fmt.Sprintf("%d FORMAT='%s'", colIndex, formatStr))

					// Next range starts after this column
					rangeStart = colIndex + 1
				}

				colIndex++
			}
		}

		// Add final range if there are remaining columns
		if rangeStart < colIndex {
			if rangeStart == colIndex-1 {
				columnSpecs = append(columnSpecs, fmt.Sprintf("%d", rangeStart))
			} else {
				columnSpecs = append(columnSpecs, fmt.Sprintf("%d..%d", rangeStart, colIndex-1))
			}
		}

		// Build column specifications string
		columnSpecsStr := ""
		if len(columnSpecs) > 0 {
			columnSpecsStr = "\n  (" + strings.Join(columnSpecs, ", ") + ")"
		}

		// Build and execute IMPORT statement for this file
		// Note: Exasol's CSV parser requires proper escaping of quotes and newlines
		// The CSV files we generate use standard RFC 4180 format with doubled quotes for escaping
		importSQL := fmt.Sprintf(`IMPORT INTO %s (%s) FROM LOCAL CSV FILE '%s'%s
  COLUMN SEPARATOR = ','
  COLUMN DELIMITER = '"'
  ROW SEPARATOR = 'LF'
  SKIP = 1
  ENCODING = 'UTF-8'`,
			tableFName,
			strings.Join(colNames, ", "),
			file.Node.Path(),
			columnSpecsStr,
		)

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
	var currentFilePath string
	filesCreated := []string{}

	// Create folder if it doesn't exist
	err := os.MkdirAll(folderPath, 0755)
	if err != nil {
		return g.Error(err, "could not create folder: "+folderPath)
	}

	// Function to create a new CSV file
	createNewFile := func() error {
		// Close previous file if exists
		if currentFile != nil {
			csvWriter.Flush()
			currentFile.Close()

			// Signal that the previous file is ready
			fileNode := filesys.FileNode{
				URI: "file://" + currentFilePath,
			}
			fileReadyChn <- filesys.FileReady{
				Node: fileNode,
			}
		}

		fileName := fmt.Sprintf("part.%02d.%04d.csv", fileNum, fileNum)
		currentFilePath = path.Join(folderPath, fileName)
		filesCreated = append(filesCreated, currentFilePath)

		currentFile, err = os.Create(currentFilePath)
		if err != nil {
			return g.Error(err, "could not create CSV file: "+currentFilePath)
		}

		csvWriter = csv.NewWriter(currentFile)

		// Write header
		if _, err := csvWriter.Write(ds.Columns.Names()); err != nil {
			return g.Error(err, "could not write CSV header")
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
					// Handle zero time (0001-01-01) as empty
					if t.IsZero() {
						formattedRow[i] = ""
					} else {
						formattedRow[i] = t.Format("2006-01-02 15:04:05")
					}
				} else {
					formattedRow[i] = ds.Sp.CastToStringCSV(i, val, col.Type)
				}
			case iop.DatetimeType, iop.TimestampType:
				if t, ok := val.(time.Time); ok {
					// Handle zero time (0001-01-01) as empty
					if t.IsZero() {
						formattedRow[i] = ""
					} else {
						formattedRow[i] = t.Format("2006-01-02 15:04:05.000000")
					}
				} else {
					formattedRow[i] = ds.Sp.CastToStringCSV(i, val, col.Type)
				}
			case iop.TimestampzType:
				if t, ok := val.(time.Time); ok {
					// Handle zero time (0001-01-01) which Exasol cannot convert between timezones
					if t.IsZero() {
						formattedRow[i] = ""
					} else {
						// Convert to UTC for Exasol timestamp format and add Z suffix
						formattedRow[i] = t.UTC().Format("2006-01-02 15:04:05.000000Z")
					}
				} else {
					formattedRow[i] = ds.Sp.CastToStringCSV(i, val, col.Type)
				}
			default:
				formattedRow[i] = ds.Sp.CastToStringCSV(i, val, col.Type)
			}
		}

		if _, err := csvWriter.Write(formattedRow); err != nil {
			return g.Error(err, "could not write CSV row")
		}

		rowCount++
	}

	// Close and signal last file
	if currentFile != nil {
		csvWriter.Flush()
		currentFile.Close()

		// Signal that the last file is ready
		fileNode := filesys.FileNode{
			URI: "file://" + currentFilePath,
		}
		fileReadyChn <- filesys.FileReady{
			Node: fileNode,
		}
	}

	return nil
}

// GenerateMergeSQL generates the upsert SQL for Exasol using the database default strategy (update_insert).
func (conn *ExasolConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, nil)
}

// GenerateMergeSQLWithStrategy generates the merge SQL for Exasol using the specified strategy.
// Exasol supports all four merge strategies via native MERGE support.
func (conn *ExasolConn) GenerateMergeSQLWithStrategy(srcTable string, tgtTable string, pkFields []string, strategy *MergeStrategy) (sql string, err error) {
	return conn.BaseConn.GenerateMergeSQLWithStrategy(srcTable, tgtTable, pkFields, strategy)
}
