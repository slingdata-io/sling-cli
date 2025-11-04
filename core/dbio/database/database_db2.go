//go:build db2

package database

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/xo/dburl"

	_ "github.com/ibmdb/go_ibm_db" // DB2 driver
)

// DB2Conn is a DB2 connection
type DB2Conn struct {
	BaseConn
	URL           string
	cliDriverPath string
	version       string
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
	// Ensure DB2 CLI tools are available
	cliDriverPath, err := conn.EnsureBinDB2CLI()
	if err != nil {
		return g.Error(err, "DB2 CLI driver not found")
	}
	conn.cliDriverPath = cliDriverPath

	// Set environment variables if CLI driver is found
	if cliDriverPath != "" {
		// Set DB2_HOME if not already set
		if os.Getenv("DB2_HOME") == "" {
			os.Setenv("DB2_HOME", cliDriverPath)
		}

		// Set LD_LIBRARY_PATH on Linux to include DB2 libraries
		if runtime.GOOS == "linux" {
			libPath := filepath.Join(cliDriverPath, "lib")
			if currentLdPath := os.Getenv("LD_LIBRARY_PATH"); currentLdPath != "" {
				os.Setenv("LD_LIBRARY_PATH", libPath+":"+currentLdPath)
			} else {
				os.Setenv("LD_LIBRARY_PATH", libPath)
			}
		}
	}

	// Connect to database using BaseConn
	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return g.Error(err, "could not connect to DB2")
	}

	// Set current schema if provided
	if currentSchema := conn.GetProp("currentSchema"); currentSchema != "" {
		_, err = conn.Exec(g.F("SET CURRENT SCHEMA = %s", currentSchema))
		if err != nil {
			return g.Error(err, "could not set current schema")
		}
	}

	// Get DB2 version
	data, err := conn.Query("SELECT service_level FROM TABLE(sysproc.env_get_inst_info()) AS instanceinfo" + noDebugKey)
	if err != nil {
		g.Warn("could not get DB2 version: %s", err.Error())
		conn.version = "11.0"
	} else if len(data.Rows) > 0 {
		conn.version = cast.ToString(data.Rows[0][0])
		g.Debug("DB2 version: %s", conn.version)
	}

	return nil
}

// ConnString builds the DB2 connection string from properties
func (conn *DB2Conn) ConnString() string {
	// The go_ibm_db driver expects format: HOSTNAME=host;DATABASE=dbname;PORT=port;UID=username;PWD=password

	var parts []string

	// Required parameters
	if host := conn.GetProp("host"); host != "" {
		parts = append(parts, g.F("HOSTNAME=%s", host))
	}

	if database := conn.GetProp("database"); database != "" {
		parts = append(parts, g.F("DATABASE=%s", database))
	}

	if port := conn.GetProp("port"); port != "" {
		parts = append(parts, g.F("PORT=%s", port))
	}

	if username := conn.GetProp("username"); username != "" {
		parts = append(parts, g.F("UID=%s", username))
	}

	if password := conn.GetProp("password"); password != "" {
		parts = append(parts, g.F("PWD=%s", password))
	}

	// Optional parameters
	if sslConnection := conn.GetProp("sslConnection"); sslConnection == "true" {
		parts = append(parts, "SECURITY=SSL")
	}

	if currentSchema := conn.GetProp("currentSchema"); currentSchema != "" {
		parts = append(parts, g.F("CurrentSchema=%s", currentSchema))
	}

	if connectTimeout := conn.GetProp("connectTimeout"); connectTimeout != "" {
		parts = append(parts, g.F("ConnectTimeout=%s", connectTimeout))
	}

	if authenticationType := conn.GetProp("authenticationType"); authenticationType != "" {
		parts = append(parts, g.F("Authentication=%s", authenticationType))
	}

	return strings.Join(parts, ";")
}

// Close closes the connection
func (conn *DB2Conn) Close() error {
	return conn.BaseConn.Close()
}

// GetSchemas returns schemas for the DB2 database
func (conn *DB2Conn) GetSchemas() (iop.Dataset, error) {
	// Use template-based approach from BaseConn
	// The db2.yaml template contains the DB2-specific schema query
	return conn.BaseConn.GetSchemas()
}

// GetTables returns tables for a schema
func (conn *DB2Conn) GetTables(schema string) (iop.Dataset, error) {
	// Use template-based approach from BaseConn
	// The db2.yaml template contains the DB2-specific tables query
	return conn.BaseConn.GetTables(schema)
}

// GetViews returns views for a schema
func (conn *DB2Conn) GetViews(schema string) (iop.Dataset, error) {
	// Use template-based approach from BaseConn
	// The db2.yaml template contains the DB2-specific views query
	return conn.BaseConn.GetViews(schema)
}

// GetColumns returns columns for a table
func (conn *DB2Conn) GetColumns(tableName string, fields ...string) (iop.Columns, error) {
	// Use template-based approach from BaseConn
	// The db2.yaml template contains the DB2-specific columns query
	return conn.BaseConn.GetColumns(tableName, fields...)
}

// BulkImportStream bulk imports a stream into a table using DB2 IMPORT command
func (conn *DB2Conn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// Check if db2 CLI is available
	db2PathStr, err := conn.db2ImportPath()
	if err != nil {
		g.Debug("db2 not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// Check if user explicitly disabled bulk import
	if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// Verify db2 binary exists
	_, err = exec.LookPath(db2PathStr)
	if err != nil {
		g.Debug("db2 not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// Get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	// Execute DB2 IMPORT
	count, err = conn.DB2ImportFile(tableFName, ds)
	if err != nil {
		return 0, g.Error(err, "Error with DB2ImportFile")
	}

	return count, nil
}

// BulkExportStream bulk exports a table to a stream
func (conn *DB2Conn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// TODO: Implement DB2 EXPORT utility for bulk export if needed
	// For now, use standard SELECT streaming
	return conn.BaseConn.BulkExportStream(table)
}

// GenerateDDL generates DDL for creating a table
func (conn *DB2Conn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {
	// Use BaseConn's DDL generation which uses the db2.yaml template
	// The template already has DB2-specific type mappings and syntax
	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return "", g.Error(err, "could not generate DDL")
	}

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	for _, index := range table.Indexes(data.Columns) {
		ddl = ddl + ";\n" + index.CreateDDL()
	}

	return ddl, nil
}

// GenerateMergeSQL generates a MERGE statement for upserting data
// DB2 supports the MERGE statement with similar syntax to SQL Server and Oracle
func (conn *DB2Conn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	// DB2 MERGE statement syntax:
	// MERGE INTO target_table AS t
	// USING source_table AS s
	// ON (t.key1 = s.key1 AND t.key2 = s.key2)
	// WHEN MATCHED THEN
	//   UPDATE SET t.col1 = s.col1, t.col2 = s.col2
	// WHEN NOT MATCHED THEN
	//   INSERT (col1, col2, ...) VALUES (s.col1, s.col2, ...)

	if len(pkFields) == 0 {
		return "", g.Error("no primary key fields provided for upsert")
	}

	// Use BaseConn's helper to generate merge expressions
	upsertMap, err := conn.BaseConn.GenerateMergeExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		return "", g.Error(err, "could not generate merge expressions")
	}

	// Build the MERGE statement using DB2 syntax
	sqlTempl := `
	merge into {tgt_table} tgt
	using (select * from {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) values ({src_fields})
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return sql, nil
}

// GenerateUpsertSQL is an alias for GenerateMergeSQL
func (conn *DB2Conn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	return conn.GenerateMergeSQL(srcTable, tgtTable, pkFields)
}

// DropTable drops one or more tables in DB2
func (conn *DB2Conn) DropTable(tableNames ...string) (err error) {
	for _, tableName := range tableNames {
		sql := g.F("DROP TABLE %s", tableName)
		_, err = conn.Exec(sql)
		if err != nil {
			// Ignore "does not exist" errors
			if strings.Contains(strings.ToLower(err.Error()), "does not exist") ||
				strings.Contains(strings.ToLower(err.Error()), "not found") ||
				strings.Contains(err.Error(), "SQL0204N") { // DB2 error code for object not found
				g.Debug("table %s does not exist, skipping drop", tableName)
				continue
			}
			return g.Error(err, "could not drop table %s", tableName)
		}
		g.Debug("dropped table %s", tableName)
	}
	return nil
}

// EnsureBinDB2CLI ensures DB2 CLI driver exists and is accessible.
// If missing, downloads and installs from IBM public URLs.
func (conn *DB2Conn) EnsureBinDB2CLI() (cliDriverPath string, err error) {
	// 1. Check environment variable first
	if envPath := os.Getenv("DB2_CLI_PATH"); envPath != "" {
		// Validate it exists
		if !g.PathExists(envPath) {
			return "", g.Error("DB2_CLI_PATH provided but path does not exist: %s", envPath)
		}

		// Validate it has the db2 binary
		binPath := filepath.Join(envPath, "bin", "db2")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}

		if !g.PathExists(binPath) {
			return "", g.Error("DB2_CLI_PATH provided but db2 binary not found at: %s", binPath)
		}

		g.Debug("using DB2 CLI driver from DB2_CLI_PATH: %s", envPath)
		return envPath, nil
	}

	// 2. Check common installation paths
	var commonPaths []string
	if runtime.GOOS == "windows" {
		commonPaths = []string{
			"C:\\Program Files\\IBM\\clidriver",
			"C:\\IBM\\clidriver",
			"C:\\Program Files (x86)\\IBM\\clidriver",
		}
	} else {
		// Linux/Unix/macOS
		commonPaths = []string{
			"/opt/ibm/clidriver",
			"/usr/local/clidriver",
			filepath.Join(os.Getenv("HOME"), "sqllib"),
			"/home/db2inst1/sqllib",
		}
	}

	// 3. Check each common path
	for _, commonPath := range commonPaths {
		binPath := filepath.Join(commonPath, "bin", "db2")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}

		if g.PathExists(binPath) {
			g.Debug("found DB2 CLI driver at: %s", commonPath)
			return commonPath, nil
		}
	}

	// 4. Auto-download if not found
	folderPath := path.Join(env.HomeBinDir(), "db2")

	// Check if already downloaded to sling's bin directory
	binPath := filepath.Join(folderPath, "bin", "db2")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	if g.PathExists(binPath) {
		g.Debug("found DB2 CLI driver in sling bin directory: %s", folderPath)
		return folderPath, nil
	}

	// Determine download URL based on platform
	var downloadURL string
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "linux/amd64":
		downloadURL = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/linuxx64_odbc_cli.tar.gz"
	case "darwin/amd64":
		downloadURL = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/macos64_odbc_cli.tar.gz"
	case "darwin/arm64":
		downloadURL = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/macarm64_odbc_cli.tar.gz"
	case "windows/amd64":
		downloadURL = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/ntx64_odbc_cli.zip"
	default:
		return "", g.Error("unsupported platform for DB2 CLI auto-download: %s/%s. Please download manually from: https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/", runtime.GOOS, runtime.GOARCH)
	}

	// Create folder
	err = os.MkdirAll(folderPath, 0755)
	if err != nil {
		return "", g.Error(err, "failed to create DB2 CLI directory: %s", folderPath)
	}

	// Download archive
	archivePath := filepath.Join(env.GetTempFolder(), "db2_cli_download")
	if strings.HasSuffix(downloadURL, ".zip") {
		archivePath += ".zip"
	} else {
		archivePath += ".tar.gz"
	}

	g.Info("downloading DB2 CLI driver from %s", downloadURL)
	err = net.DownloadFile(downloadURL, archivePath)
	if err != nil {
		return "", g.Error(err, "failed to download DB2 CLI driver")
	}
	defer os.Remove(archivePath)

	// Extract archive
	g.Info("extracting DB2 CLI driver to %s", folderPath)
	if strings.HasSuffix(archivePath, ".zip") {
		// Use Unzip for Windows
		_, err = iop.Unzip(archivePath, folderPath)
		if err != nil {
			return "", g.Error(err, "failed to extract DB2 CLI driver")
		}
	} else {
		// Use tar extraction for Linux/macOS
		// Extract with --strip-components=1 to remove the top-level 'clidriver' directory
		cmd := exec.Command("tar", "-xzf", archivePath, "-C", folderPath, "--strip-components=1")
		output, err := cmd.CombinedOutput()
		if err != nil {
			return "", g.Error(err, "failed to extract DB2 CLI driver. Output: %s", string(output))
		}
	}

	// Verify installation
	if !g.PathExists(binPath) {
		return "", g.Error("DB2 CLI driver downloaded but db2 binary not found at: %s", binPath)
	}

	g.Info("DB2 CLI driver installed successfully at: %s", folderPath)
	return folderPath, nil
}

// db2Path returns the path to the db2 command-line tool
func (conn *DB2Conn) db2Path() (string, error) {
	if val := conn.GetProp("db2_path"); val != "" {
		return val, nil
	}

	// Ensure CLI driver is available
	if conn.cliDriverPath == "" {
		cliDriverPath, err := conn.EnsureBinDB2CLI()
		if err != nil {
			return "", err
		}
		conn.cliDriverPath = cliDriverPath
	}

	binPath := filepath.Join(conn.cliDriverPath, "bin", "db2")
	if runtime.GOOS == "windows" {
		binPath += ".exe"
	}

	return binPath, nil
}

// db2ImportPath returns the path to the db2 executable for running IMPORT/LOAD commands
func (conn *DB2Conn) db2ImportPath() (string, error) {
	// For DB2, the import/load commands are executed through the same db2 binary
	return conn.db2Path()
}

// DB2ImportFile uses DB2 IMPORT command to bulk load data from a CSV file
func (conn *DB2Conn) DB2ImportFile(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer

	// Get connection URL
	connURL := conn.URL
	if su := conn.GetProp("ssh_url"); su != "" {
		connURL = su // use ssh url if specified
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		err = g.Error(err, "Error parsing connection URL")
		return
	}

	// Parse connection details
	password, _ := url.User.Password()
	host := url.Host
	port := url.Port()
	if port == "" {
		port = cast.ToString(conn.GetType().DefPort())
	}
	// Remove port from host if present
	if strings.Contains(host, ":") {
		host = strings.Split(host, ":")[0]
	}
	database := strings.TrimPrefix(url.Path, "/")
	user := url.User.Username()

	// Create temporary CSV file
	csvPath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("db2.%s", env.CleanTableName(tableFName)))+".csv")
	msgPath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("db2.%s", env.CleanTableName(tableFName)))+".msg")

	defer func() {
		env.RemoveLocalTempFile(csvPath)
		env.RemoveLocalTempFile(msgPath)
	}()

	// Write data to CSV file
	g.Debug("writing to temp csv file: %s", csvPath)
	cnt, err := writeCsvForDB2(csvPath, ds)
	if err != nil {
		err = g.Error(err, "could not write to temp CSV file")
		return 0, err
	}

	if cnt == 0 {
		g.Debug("no rows to import")
		return 0, nil
	}

	// Build column positions for METHOD P clause
	columnPositions := make([]string, len(ds.Columns))
	for i := range ds.Columns {
		columnPositions[i] = cast.ToString(i + 1)
	}

	// Build column names list
	columnNames := make([]string, len(ds.Columns))
	for i, col := range ds.Columns {
		columnNames[i] = conn.Quote(col.Name)
	}

	// Build the DB2 IMPORT command
	// We'll use db2 CLI with a compound command to connect, import, and terminate
	importCmd := fmt.Sprintf(
		`IMPORT FROM '%s' OF DEL MODIFIED BY COLDEL, CHARDEL" CODEPAGE=1208 METHOD P (%s) MESSAGES '%s' INSERT INTO %s (%s)`,
		csvPath,
		strings.Join(columnPositions, ", "),
		msgPath,
		tableFName,
		strings.Join(columnNames, ", "),
	)

	// Build connection command
	connectCmd := fmt.Sprintf(`CONNECT TO %s USER %s USING %s`, database, user, password)
	terminateCmd := `TERMINATE`

	// Get db2 binary path
	db2PathStr, err := conn.db2ImportPath()
	if err != nil {
		return 0, g.Error(err, "could not get db2 path")
	}

retry:
	// Execute connect
	procConnect := exec.Command(db2PathStr, connectCmd)
	procConnect.Stderr = &stderr
	procConnect.Stdout = &stdout

	// Mask password in debug output
	maskedConnectCmd := strings.ReplaceAll(connectCmd, password, "****")
	g.Debug("%s %s", db2PathStr, maskedConnectCmd)

	err = procConnect.Run()
	if err != nil {
		if strings.Contains(err.Error(), "text file busy") {
			g.Warn("could not start db2 (%s), retrying...", err.Error())
			time.Sleep(1 * time.Second)
			goto retry
		}
		err = g.Error(
			err,
			fmt.Sprintf(
				"DB2 Connect Command -> %s %s\nDB2 Connect Error -> %s\n%s",
				db2PathStr, maskedConnectCmd, stderr.String(), stdout.String(),
			),
		)
		return 0, err
	}

	// Execute import
	stderr.Reset()
	stdout.Reset()
	procImport := exec.Command(db2PathStr, importCmd)
	procImport.Stderr = &stderr
	procImport.Stdout = &stdout

	g.Debug("%s %s", db2PathStr, importCmd)

	err = procImport.Run()

	// Read messages file for additional info
	msgContent := ""
	if g.PathExists(msgPath) {
		msgBytes, _ := os.ReadFile(msgPath)
		msgContent = string(msgBytes)
	}

	// Parse output for row count
	// DB2 IMPORT output typically contains "Number of rows inserted: X"
	countRegex := regexp.MustCompile(`(?i)(\d+)\s+(?:rows?\s+(?:inserted|read|committed))`)
	res := countRegex.FindStringSubmatch(stdout.String() + msgContent)
	if len(res) >= 2 {
		count = cast.ToUint64(res[1])
	}

	if err != nil {
		err = g.Error(
			err,
			fmt.Sprintf(
				"DB2 Import Command -> %s %s\nDB2 Import Error -> %s\n%s\n%s",
				db2PathStr, importCmd, stderr.String(), stdout.String(), msgContent,
			),
		)
		// Don't return yet, try to terminate connection
	}

	// Execute terminate
	stderr.Reset()
	stdout.Reset()
	procTerminate := exec.Command(db2PathStr, terminateCmd)
	procTerminate.Stderr = &stderr
	procTerminate.Stdout = &stdout

	g.Debug("%s %s", db2PathStr, terminateCmd)

	terminateErr := procTerminate.Run()
	if terminateErr != nil {
		g.Warn("DB2 terminate command failed: %s", terminateErr.Error())
	}

	// Check for datastream errors
	if ds.Err() != nil {
		return count, g.Error(ds.Err(), "datastream error")
	}

	// Return any import error
	if err != nil {
		return count, err
	}

	return count, nil
}

// writeCsvForDB2 writes a datastream to a CSV file in DB2-compatible format
// Uses UTF-8 encoding, comma delimiter, and double-quote string delimiter
func writeCsvForDB2(filePath string, ds *iop.Datastream) (cnt uint64, err error) {
	file, err := os.Create(filePath)
	if err != nil {
		return 0, g.Error(err, "could not create CSV file")
	}
	defer file.Close()

	// Create CSV writer with standard settings
	w := csv.NewWriter(file)
	w.Comma = ','

	// Write header row
	err = w.Write(ds.Columns.Names())
	if err != nil {
		return 0, g.Error(err, "could not write header to CSV file")
	}

	// Get timestamp layout for DB2
	timestampLayout := dbio.TypeDbDB2.GetTemplateValue("variable.timestamp_layout")
	if timestampLayout == "" {
		timestampLayout = "2006-01-02 15:04:05.000000"
	}
	ds.Sp.SetConfig(map[string]string{"datetime_format": timestampLayout})

	// Write data rows
	for row0 := range ds.Rows() {
		cnt++
		row := make([]string, len(row0))
		for i, val := range row0 {
			if val == nil {
				row[i] = ""
				continue
			}

			// Handle specific data types
			if ds.Columns[i].Type == iop.DateType {
				// Convert to DB2 date format
				val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
				if t, ok := val.(time.Time); ok {
					row[i] = t.Format("2006-01-02")
				} else {
					row[i] = ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
				}
			} else if ds.Columns[i].Type == iop.DatetimeType || ds.Columns[i].Type == iop.TimestampType {
				// Convert to DB2 timestamp format
				val = ds.Sp.CastValWithoutStats(i, val, ds.Columns[i].Type)
				if t, ok := val.(time.Time); ok {
					row[i] = t.Format(timestampLayout)
				} else {
					row[i] = ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
				}
			} else {
				// Use standard CSV casting
				row[i] = ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
			}
		}

		err = w.Write(row)
		if err != nil {
			return cnt, g.Error(err, "could not write row to CSV file")
		}

		w.Flush()
		if w.Error() != nil {
			return cnt, g.Error(w.Error(), "could not flush CSV writer")
		}
	}

	w.Flush()
	if w.Error() != nil {
		return cnt, g.Error(w.Error(), "could not flush CSV writer")
	}

	return cnt, nil
}
