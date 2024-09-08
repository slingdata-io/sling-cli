package iop

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/flarco/g/process"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

var (
	DuckDbVersion      = "1.0.0"
	DuckDbUseTempFile  = false
	duckDbReadOnlyHint = "/* -readonly */"
	duckDbSOFMarker    = "___start_of_duckdb_result___"
	duckDbEOFMarker    = "___end_of_duckdb_result___"
)

// DuckDb is a Duck DB compute layer
type DuckDb struct {
	Context       *g.Context
	Proc          *process.Proc
	StdinWriter   io.Writer
	isInteractive bool
	extensions    []string
	secrets       []string
}

// NewDuckDb creates a new DuckDb instance with the given context and properties
func NewDuckDb(ctx context.Context, props ...string) *DuckDb {
	Ctx := g.NewContext(ctx)
	duck := &DuckDb{Context: &Ctx}

	m := g.KVArrToMap(props...)
	for k, v := range m {
		duck.SetProp(k, v)
	}

	return duck
}

// SetProp sets a property for the DuckDb instance
func (duck *DuckDb) SetProp(key string, value string) {
	duck.Context.Map.Set(key, value)
}

// GetProp retrieves a property value for the DuckDb instance
func (duck *DuckDb) GetProp(key string) string {
	val, _ := duck.Context.Map.Get(key)
	return cast.ToString(val)
}

// Props returns all properties of the DuckDb instance as a map
func (duck *DuckDb) Props() map[string]string {
	props := map[string]string{}
	m := duck.Context.Map.Items()
	for k, v := range m {
		props[k] = cast.ToString(v)
	}
	return props
}

// AddExtension adds an extension to the DuckDb instance if it's not already present
func (duck *DuckDb) AddExtension(extension string) {
	if !lo.Contains(duck.extensions, extension) {
		duck.extensions = append(duck.extensions, extension)
	}
}

// PrepareFsSecretAndURI prepares the secret configuration from the fs_props and modifies the URI if necessary
// for different storage types (S3, Google Cloud Storage, Azure Blob Storage).
// It returns the modified URI string.
//
// The function handles the following storage types:
// - Local files: Removes the "file://" prefix
// - S3: Configures AWS credentials and handles Cloudflare R2 storage
// - Google Cloud Storage: Sets up GCS credentials
// - Azure Blob Storage: Configures Azure connection string or account name
//
// It uses the DuckDb instance's properties to populate the secret configuration.
func (duck *DuckDb) PrepareFsSecretAndURI(uri string) string {
	scheme := dbio.Type(strings.Split(uri, "://")[0])

	fsProps := map[string]string{}
	g.Unmarshal(duck.GetProp("fs_props"), &fsProps)

	// uppercase all the keys
	for _, k := range lo.Keys(fsProps) {
		fsProps[strings.ToUpper(k)] = fsProps[k]
		if k != strings.ToUpper(k) {
			delete(fsProps, k)
		}
	}

	var secretKeyMap map[string]string
	secretProps := []string{}
	scopeScheme := scheme.String()

	switch scheme {
	case dbio.TypeFileLocal:
		return strings.TrimPrefix(uri, "file://")

	case dbio.TypeFileS3:
		secretKeyMap = map[string]string{
			"ACCESS_KEY_ID":     "KEY_ID",
			"SECRET_ACCESS_KEY": "SECRET",
			"BUCKET":            "SCOPE",
			"REGION":            "REGION",
			"SESSION_TOKEN":     "SESSION_TOKEN",
			"ENDPOINT":          "ENDPOINT",
		}

		if strings.Contains(fsProps["endpoint"], "r2.cloudflarestorage.com") {
			accountID := strings.Split(fsProps["endpoint"], ".")[0]
			secretProps = append(secretProps, "ACCOUNT_ID "+accountID)
			secretProps = append(secretProps, "TYPE R2")
			scopeScheme = "r2"
			uri = strings.ReplaceAll(uri, "s3://", "r2://")
		} else {
			secretProps = append(secretProps, "TYPE S3")
		}

	case dbio.TypeFileGoogle:
		secretKeyMap = map[string]string{
			"ACCESS_KEY_ID":     "KEY_ID",
			"SECRET_ACCESS_KEY": "SECRET",
		}
		secretProps = append(secretProps, "TYPE GCS")
		scopeScheme = "gcs"
		uri = strings.ReplaceAll(uri, "gs://", "gcs://")

	case dbio.TypeFileAzure:
		secretKeyMap = map[string]string{
			"CONN_STR": "CONNECTION_STRING",
			"ACCOUNT":  "ACCOUNT_NAME",
		}
		secretProps = append(secretProps, "TYPE AZURE")
	}

	// populate secret props and make secret sql
	if len(secretProps) > 0 {
		for slingKey, duckdbKey := range secretKeyMap {
			if val := fsProps[slingKey]; val != "" {
				if duckdbKey == "SCOPE" {
					val = scopeScheme + "://" + val
				}
				duckdbVal := "'" + val + "'" // add quotes
				secretProps = append(secretProps, g.F("%s %s", duckdbKey, duckdbVal))
			}
		}

		secretSQL := g.R(
			"create or replace secret {name} ({key_vals})",
			"name", scopeScheme,
			"key_vals", strings.Join(secretProps, ",\n  "),
		)

		duck.secrets = append(duck.secrets, secretSQL)
	}

	return uri
}

// getLoadExtensionSQL generates SQL statements to load extensions
func (duck *DuckDb) getLoadExtensionSQL() (sql string) {
	for _, extension := range duck.extensions {
		sql += fmt.Sprintf(";INSTALL %s; LOAD %s;", extension, extension)
	}
	return
}

// getCreateSecretSQL generates SQL statements to create secrets
func (duck *DuckDb) getCreateSecretSQL() (sql string) {
	for _, secret := range duck.secrets {
		sql += fmt.Sprintf(";%s;", secret)
	}
	return
}

// Open initializes the DuckDb connection
func (duck *DuckDb) Open(timeOut ...int) (err error) {

	bin, err := EnsureBinDuckDB(duck.GetProp("duckdb_version"))
	if err != nil {
		return g.Error(err, "could not get duckdb binary")
	}

	duck.Proc, err = process.NewProc(bin)
	if err != nil {
		return g.Error(err, "could not create process for duckdb")
	}

	duck.Proc.HideCmdInErr = true
	args := []string{"-csv", "-nullvalue", `\N\`}
	duck.Proc.Env = g.KVArrToMap(os.Environ()...)

	if path := duck.GetProp("path"); path != "" {
		args = append(args, path)
	}

	if motherduckToken := duck.GetProp("motherduck_token"); motherduckToken != "" {
		duck.Proc.Env["motherduck_token"] = motherduckToken
	}

	duck.Proc.SetArgs(args...)

	duck.SetProp("connected", "true")

	_, err = duck.Exec("select 1" + env.NoDebugKey)
	if err != nil {
		if strings.Contains(err.Error(), " version") {
			g.Warn("To having sling use a different DuckDB version, set DUCKDB_VERSION=<version>")
		}
		return g.Error(err, "could not init connection")
	}

	// default extensions
	duck.AddExtension("json")

	// set as interactive
	duck.isInteractive = cast.ToBool(duck.GetProp("interactive"))

	return nil
}

// Close closes the connection
func (duck *DuckDb) Close() error {
	if duck.Proc.Exited() {
		return nil
	}

	// submit quit command?
	if duck.isInteractive && duck.Proc.Cmd != nil {
		// need to submit to stdin: ".quit"
		duck.StdinWriter.Write([]byte(".quit\n"))
	}

	// kill timer
	timer := time.NewTimer(5 * time.Second)
	done := make(chan struct{})
	go func() {
		if duck.Proc.Cmd != nil {
			duck.Proc.Cmd.Process.Wait()
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timer.C:
		if duck.Proc.Cmd != nil {
			duck.Proc.Cmd.Process.Kill()
		}
	}
	duck.Proc.Cmd = nil

	return nil
}

// Exec executes a SQL query and returns the result
func (duck *DuckDb) Exec(sql string, args ...interface{}) (result sql.Result, err error) {

	result, err = duck.ExecContext(duck.Context.Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// ExecMultiContext executes multiple SQL queries with context and returns the result
func (duck *DuckDb) ExecMultiContext(ctx context.Context, sqls ...string) (result sql.Result, err error) {
	return duck.ExecContext(ctx, strings.Join(sqls, ";\n"))
}

type duckDbResult struct {
	TotalChanges int64
}

func (r duckDbResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r duckDbResult) RowsAffected() (int64, error) {
	return r.TotalChanges, nil
}

// startAndSubmitSQL starts the duckdb process if needed and submits a sql query to it
func (duck *DuckDb) startAndSubmitSQL(sql string, showChanges bool) (err error) {

	// Set up the process
	if !duck.isInteractive {
		err = duck.Proc.Start()
		if err != nil {
			return g.Error(err, "Failed to execute SQL")
		}
	}

	extensionsSQL := duck.getLoadExtensionSQL()
	secretSQL := duck.getCreateSecretSQL()

	// submit sql to stdin
	stdinLines := []string{
		extensionsSQL + ";",
		secretSQL + ";",
		g.R("select '{v}' AS marker;", "v", duckDbSOFMarker),
		".changes on",
		sql + ";",
		".changes off",
		g.R("select '{v}' AS {v};\n", "v", duckDbEOFMarker),
	}

	if !showChanges {
		stdinLines = []string{
			extensionsSQL + ";",
			secretSQL + ";",
			g.R("select '{v}' AS marker;", "v", duckDbSOFMarker),
			sql + ";",
			g.R("select '{v}' AS {v};\n", "v", duckDbEOFMarker),
		}
	}

	// combine props with fs_props for proper masking
	propsCombined := map[string]string{}
	g.Unmarshal(duck.GetProp("fs_props"), &propsCombined)
	for k, v := range duck.Props() {
		propsCombined[k] = v
	}
	env.LogSQL(propsCombined, sql)

	sqls := strings.Join(stdinLines, "\n")
	// g.Warn(sqls)

	_, err = duck.Proc.StdinWriter.Write([]byte(sqls))
	if err != nil {
		return g.Error(err, "Failed to write to stdin")
	}

	return nil
}

// closeStdinAndWait closes the stdin if the process is not interactive
func (duck *DuckDb) closeStdinAndWait() (err error) {

	err = duck.Proc.CloseStdin()
	if err != nil {
		return g.Error(err, "Failed to close stdin")
	}

	err = duck.Proc.Wait()
	if err != nil {
		return g.Error(err, "Failed to execute SQL")
	}

	return nil
}

// ExecContext executes a SQL query with context and returns the result
func (duck *DuckDb) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if duck.GetProp("connected") != "true" {
		if err = duck.Open(); err != nil {
			return nil, g.Error(err, "Could not open DuckDB connection")
		}
	}

	result = duckDbResult{}

	if len(args) > 0 {
		for i, arg := range args {
			ph := g.F("$%d", i+1)

			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, ph, fmt.Sprintf("%d", val), 1)
			case float32, float64:
				sql = strings.Replace(sql, ph, fmt.Sprintf("%f", val), 1)
			case time.Time:
				sql = strings.Replace(sql, ph, fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, ph, "NULL", 1)
			default:
				v := strings.ReplaceAll(cast.ToString(val), "\n", "\\n")
				v = strings.ReplaceAll(v, "'", "\\'")
				sql = strings.Replace(sql, ph, fmt.Sprintf("'%s'", v), 1)
			}
		}
	}

	// start and submit sql
	duck.Proc.Capture = true
	err = duck.startAndSubmitSQL(sql, true)
	if err != nil {
		return result, g.Error(err, "Failed to submit SQL")
	}

	// Extract total changes from stdout
	result, err = duck.waitForExec(ctx)
	if err != nil {
		return result, g.Error(err, "Failed to execute SQL")
	}

	// close if needed
	duck.Proc.Capture = false
	if !duck.isInteractive {
		err = duck.closeStdinAndWait()
		if err != nil {
			if strings.Contains(err.Error(), "exit code = -1") && strings.Contains(err.Error(), duckDbEOFMarker) {
				err = nil // some weird error, returning exit code -1 with stdout
			} else {
				return result, g.Error(err, "Failed to close stdin")
			}
		}
	}

	return result, nil
}

// waitForExec waits for the execution of a SQL query and returns the result
func (duck *DuckDb) waitForExec(ctx context.Context) (result sql.Result, err error) {
	// Extract total changes from stdout
	var lastLength int

	// Extract total changes from stdout
	// This loop continuously reads the output from the DuckDB process
	// It looks for lines containing "total_changes:" to update the result
	// and stops when it encounters the EOF marker
	for {
		select {
		case <-ctx.Done():
			return result, g.Error("sql execution cancelled")
		default:
			currentLength := duck.Proc.Stdout.Len()
			if currentLength > lastLength {
				output := duck.Proc.Stdout.Bytes()[lastLength:currentLength]
				lines := strings.Split(string(output), "\n")
				for _, line := range lines {
					if strings.Contains(line, "total_changes:") {
						parts := strings.Fields(line)
						if len(parts) >= 4 {
							totalChanges, err := strconv.ParseUint(parts[3], 10, 64)
							if err == nil {
								result = duckDbResult{TotalChanges: cast.ToInt64(totalChanges)}
							}
						}
					} else if strings.Contains(line, duckDbEOFMarker) {
						goto done
					}
				}
				lastLength = currentLength
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
done:
	return
}

// Query runs a sql query, returns `Dataset`
func (duck *DuckDb) Query(sql string, options ...map[string]interface{}) (data Dataset, err error) {
	return duck.QueryContext(context.Background(), sql, options...)
}

// QueryContext runs a sql query with context, returns `Dataset`
func (duck *DuckDb) QueryContext(ctx context.Context, sql string, options ...map[string]interface{}) (data Dataset, err error) {
	ds, err := duck.StreamContext(ctx, sql, options...)
	if err != nil {
		return data, g.Error(err, "could not export data")
	}

	data, err = ds.Collect(0)
	if err != nil {
		return data, g.Error(err, "could not collect data")
	}

	ds.Close() // ensure defers are run

	return
}

// Stream runs a sql query, returns `Datastream`
func (duck *DuckDb) Stream(sql string, options ...map[string]interface{}) (ds *Datastream, err error) {
	return duck.StreamContext(duck.Context.Ctx, sql, options...)
}

// StreamContext runs a sql query with context, returns `Datastream`
func (duck *DuckDb) StreamContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *Datastream, err error) {
	if duck.GetProp("connected") != "true" {
		if err = duck.Open(); err != nil {
			return nil, g.Error(err, "Could not open DuckDB connection")
		}
	}

	queryCtx := g.NewContext(ctx)

	opts := g.M()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	}

	columns, err := duck.Describe(sql)
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}

	// do not capture stdout, use scanner instead
	// Create a pipe for stdout, stderr handling
	stdOutReader, stdOutWriter := io.Pipe()
	stderrBuf := &bytes.Buffer{}

	// Set up the scanner to write to the pipe
	started := false
	done := false
	duck.Proc.SetScanner(func(stderr bool, line string) {
		if done {
			return
		} else if strings.Contains(line, duckDbEOFMarker) {
			stdOutWriter.Close()
			done = true
			return
		}

		if stderr {
			stderrBuf.WriteString(line + "\n")
		} else if !started && strings.Contains(line, duckDbSOFMarker) {
			started = true
		} else if line != "" && started {
			_, err := stdOutWriter.Write([]byte(line + "\n"))
			if err != nil {
				queryCtx.CaptureErr(g.Error(err, "Failed to write to stdout pipe"))
			}
		}
	})

	// start and submit sql
	err = duck.startAndSubmitSQL(sql, false)
	if err != nil {
		return nil, g.Error(err, "Failed to submit SQL")
	}

	// so that lists are treated as TEXT and not JSON
	// lists / arrays do not conform to JSON spec and can error out
	transforms := map[string][]string{"*": {"duckdb_list_to_text"}}

	ds = NewDatastreamContext(queryCtx.Ctx, columns)

	if cds, ok := opts["datastream"]; ok {
		// if provided, use it
		ds = cds.(*Datastream)
		ds.Columns = columns
	}

	ds.Inferred = true
	ds.NoDebug = strings.Contains(sql, env.NoDebugKey)
	ds.SetConfig(duck.Props())
	ds.SetConfig(map[string]string{"delimiter": ",", "header": "true", "transforms": g.Marshal(transforms), "null_if": `\N\`})

	ds.Defer(func() {
		if !duck.isInteractive {
			duck.closeStdinAndWait()
		}
		duck.Proc.SetScanner(nil)
	})

	err = ds.ConsumeCsvReader(stdOutReader)
	if err != nil {
		ds.Close()
		return ds, g.Error(err, "could not read output stream")
	}

	if errOutS := stderrBuf.String(); strings.Contains(errOutS, "Error: ") {
		return ds, g.Error(errOutS)
	}

	return
}

// Import inserts a stream into a table
// It takes a table name and a Datastream as input, and returns the number of rows imported and any error encountered
// The function handles the import process by writing data to a temporary CSV file or using stdin,
// then executing an INSERT statement to load the data into the specified table
func (duck *DuckDb) Import(tableFName string, ds *Datastream) (count uint64, err error) {
	defer ds.Close()

	return ds.Count, nil
}

// Quote quotes a column name
func (duck *DuckDb) Quote(col string) (qName string) {
	qName = `"` + col + `"`
	return
}

// EnsureBinDuckDB ensures duckdb binary exists
// if missing, downloads and uses
func EnsureBinDuckDB(version string) (binPath string, err error) {

	if version == "" {
		if val := os.Getenv("DUCKDB_VERSION"); val != "" {
			version = val
		} else {
			version = DuckDbVersion
		}
	}

	// use specified path to duckdb binary
	if envPath := os.Getenv("DUCKDB_PATH"); envPath != "" {
		if !g.PathExists(envPath) {
			return "", g.Error("duckdb binary not found: %s", envPath)
		}
		return envPath, nil
	}

	if useTempFile := os.Getenv("DUCKDB_USE_TMP_FILE"); useTempFile != "" {
		DuckDbUseTempFile = cast.ToBool(useTempFile)
	} else if g.In(version, "0.8.0", "0.8.1") {
		// there is a bug with stdin stream in 0.8.1.
		// Out of Memory Error
		DuckDbUseTempFile = true
	}

	folderPath := path.Join(env.HomeBinDir(), "duckdb", version)
	extension := lo.Ternary(runtime.GOOS == "windows", ".exe", "")
	binPath = path.Join(folderPath, "duckdb"+extension)
	found := g.PathExists(binPath)

	checkVersion := func() (bool, error) {

		out, err := exec.Command(binPath, "-version").Output()
		if err != nil {
			return false, g.Error(err, "could not get version for duckdb")
		}

		if strings.HasPrefix(string(out), "v"+version) {
			return true, nil
		}

		return false, nil
	}

	// TODO: check version if found
	if found {
		ok, err := checkVersion()
		if err != nil {
			return "", g.Error(err, "error checking version for duckdb")
		}
		found = ok // so it can re-download if mismatch
	}

	if !found {
		// we need to download it ourselves
		var downloadURL string
		zipPath := path.Join(g.UserHomeDir(), "duckdb.zip")
		defer os.Remove(zipPath)

		switch runtime.GOOS + "/" + runtime.GOARCH {

		case "windows/amd64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-windows-amd64.zip"

		case "windows/386":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-windows-i386.zip"

		case "darwin/386", "darwin/arm", "darwin/arm64", "darwin/amd64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-osx-universal.zip"

		case "linux/386":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-i386.zip"

		case "linux/amd64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-amd64.zip"

		case "linux/aarch64":
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-aarch64.zip"

		default:
			return "", g.Error("OS %s/%s not handled", runtime.GOOS, runtime.GOARCH)
		}

		downloadURL = g.R(downloadURL, "version", version)

		g.Info("downloading duckdb %s for %s/%s", version, runtime.GOOS, runtime.GOARCH)
		err = net.DownloadFile(downloadURL, zipPath)
		if err != nil {
			return "", g.Error(err, "Unable to download duckdb binary")
		}

		paths, err := Unzip(zipPath, folderPath)
		if err != nil {
			return "", g.Error(err, "Error unzipping duckdb zip")
		}

		if !g.PathExists(binPath) {
			return "", g.Error("cannot find %s, paths are: %s", binPath, g.Marshal(paths))
		}
	}

	_, err = checkVersion()
	if err != nil {
		return "", g.Error(err, "error checking version for duckdb")
	}

	return binPath, nil
}

// DuckDBTypeToColumnType converts a DuckDB type to a ColumnType
func DuckDBTypeToColumnType(duckDBType string) ColumnType {
	// Normalize the input by converting to uppercase and trimming spaces
	duckDBType = strings.TrimSpace(strings.ToUpper(duckDBType))

	// Extract the base type for types with parameters
	baseType := strings.Split(duckDBType, "(")[0]

	switch baseType {
	case "BIGINT", "INT8", "LONG", "UBIGINT":
		return BigIntType
	case "BIT", "BITSTRING":
		return BinaryType
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		return BinaryType
	case "BOOLEAN", "BOOL", "LOGICAL":
		return BoolType
	case "DATE":
		return DateType
	case "DECIMAL", "NUMERIC":
		return DecimalType
	case "DOUBLE", "FLOAT8":
		return FloatType
	case "FLOAT", "FLOAT4", "REAL":
		return FloatType
	case "HUGEINT", "UHUGEINT":
		return BigIntType
	case "INTEGER", "INT4", "INT", "SIGNED", "UINTEGER":
		return IntegerType
	case "INTERVAL":
		return StringType // No direct mapping, using StringType
	case "SMALLINT", "INT2", "SHORT", "USMALLINT":
		return SmallIntType
	case "TIME":
		return TimeType
	case "TIMESTAMP WITH TIME ZONE":
		return TimestampzType
	case "TIMESTAMP":
		return TimestampType
	case "TINYINT", "INT1", "UTINYINT":
		return SmallIntType
	case "ARRAY", "LIST", "MAP", "STRUCT", "UNION":
		return JsonType
	case "UUID":
		return StringType // No direct mapping, using StringType
	case "VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING":
		return StringType
	default:
		return StringType // Default to StringType for unknown types
	}
}

// Describe returns the columns of a query
func (duck *DuckDb) Describe(query string) (columns Columns, err error) {
	// prevent infinite loop
	if strings.HasPrefix(query, "describe ") {
		return columns, nil
	}

	data, err := duck.Query("describe " + query + env.NoDebugKey)
	if err != nil {
		return nil, g.Error(err, "could not describe query")
	}

	for k, rec := range data.Records() {
		var col Column

		col.Name = cast.ToString(rec["column_name"])
		col.DbType = cast.ToString(rec["column_type"])
		col.Type = DuckDBTypeToColumnType(cast.ToString(rec["column_type"]))
		col.Position = k + 1

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return nil, g.Error("no columns found")
	}

	return
}

func (duck *DuckDb) MakeScanQuery(scanFunc, uri string, fsc FileStreamConfig) (sql string) {

	where := ""
	incrementalWhereCond := "1=1"

	if fsc.IncrementalValue == "" {
		fsc.IncrementalValue = "null"
	}

	if fsc.IncrementalKey != "" && fsc.IncrementalValue != "" {
		incrementalWhereCond = g.F("%s > %s", dbio.TypeDbDuckDb.Quote(fsc.IncrementalKey), fsc.IncrementalValue)
		where = g.F("where %s", incrementalWhereCond)
	}

	streamScanner := dbio.TypeDbDuckDb.GetTemplateValue("function." + scanFunc)
	if fsc.SQL != "" {
		return g.R(
			g.R(fsc.SQL, "stream_scanner", streamScanner),
			"incremental_where_cond", incrementalWhereCond,
			"incremental_value", fsc.IncrementalValue,
			"uri", uri,
		)
	}

	fields := fsc.Select
	if len(fields) == 0 || fields[0] == "*" {
		fields = []string{"*"}
	} else {
		fields = dbio.TypeDbDuckDb.QuoteNames(fields...)
	}

	selectStreamScanner := dbio.TypeDbDuckDb.GetTemplateValue("core.select_stream_scanner")
	if selectStreamScanner == "" {
		selectStreamScanner = "select {fields} from {stream_scanner} {where}"
	}

	sql = strings.TrimSpace(g.R(
		g.R(selectStreamScanner, "stream_scanner", streamScanner),
		"fields", strings.Join(fields, ","),
		"uri", uri,
		"where", where,
	))

	if fsc.Limit > 0 {
		sql += fmt.Sprintf(" limit %d", fsc.Limit)
	}

	return sql
}
