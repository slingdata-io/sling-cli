package iop

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
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
	"github.com/labstack/echo/v4"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

var (
	DuckDbVersion      = "1.3.2"
	DuckDbUseTempFile  = false
	duckDbReadOnlyHint = "/* -readonly */"
	duckDbSOFMarker    = "___start_of_duckdb_result___"
	duckDbEOFMarker    = "___end_of_duckdb_result___"
	DuckDbURISeparator = "|-|+|"
)

// DuckDb is a Duck DB compute layer
type DuckDb struct {
	Context     *g.Context
	Proc        *process.Proc
	extensions  []string
	secrets     []DuckDbSecret
	initialized bool
	query       *duckDbQuery // only one active query at a time
}

type duckDbQuery struct {
	SQL     string
	Context *g.Context
	reader  *io.PipeReader
	writer  *io.PipeWriter
	err     error
	started bool
	done    bool
}

// NewDuckDb creates a new DuckDb instance with the given context and properties
func NewDuckDb(ctx context.Context, props ...string) *DuckDb {
	duck := &DuckDb{Context: g.NewContext(ctx)}

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
		duck.initialized = false // to re-initialize
	}
}

type DuckDbSecret struct {
	Type  DuckDbSecretType  `json:"type"`
	Name  string            `json:"name"`
	Props map[string]string `json:"props"`
}

type DuckDbSecretType string

const (
	DuckDbSecretTypeUnknown DuckDbSecretType = ""
	DuckDbSecretTypeS3      DuckDbSecretType = "s3"
	DuckDbSecretTypeR2      DuckDbSecretType = "r2"
	DuckDbSecretTypeGCS     DuckDbSecretType = "gcs"
	DuckDbSecretTypeAzure   DuckDbSecretType = "azure"
	DuckDbSecretTypeIceberg DuckDbSecretType = "iceberg"
)

func NewDuckDbSecret(name string, secretType DuckDbSecretType, props map[string]string) DuckDbSecret {

	secret := DuckDbSecret{
		Type:  secretType,
		Name:  name,
		Props: map[string]string{},
	}

	// lowercase keys
	for k, v := range props {
		secret.Props[strings.ToLower(k)] = cast.ToString(v)
	}

	return secret
}

func (dds *DuckDbSecret) AddProp(key, value string) {
	dds.Props[key] = value
}

func (dds *DuckDbSecret) Render() string {

	props := []string{}
	for k, v := range dds.Props {
		if g.In(k, "provider") {
			props = append(props, g.F("%s %s", strings.ToUpper(k), v))
		} else {
			props = append(props, g.F("%s '%s'", strings.ToUpper(k), v)) // add quotes
		}
	}

	return g.F("CREATE SECRET IF NOT EXISTS %s (TYPE %s, %s)",
		dds.Name, dds.Type, strings.Join(props, ", "))
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

	if scheme == "https" && strings.Contains(uri, ".core.windows.net/") {
		scheme = dbio.TypeFileAzure
	}

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
	var secretType DuckDbSecretType
	secretProps := []string{}
	scopeScheme := scheme.String()

	switch scheme {
	case dbio.TypeFileLocal:
		return strings.ReplaceAll(uri, "file://", "")

	case dbio.TypeFileS3, dbio.TypeFileR2:
		secretKeyMap = map[string]string{
			"ACCESS_KEY_ID":     "KEY_ID",
			"SECRET_ACCESS_KEY": "SECRET",
			"BUCKET":            "SCOPE",
			"REGION":            "REGION",
			"SESSION_TOKEN":     "SESSION_TOKEN",
			"ENDPOINT":          "ENDPOINT",
			"PROFILE":           "PROFILE",
			"USE_SSL":           "USE_SSL",
			"URL_STYLE":         "URL_STYLE",
		}

		if strings.Contains(fsProps["ENDPOINT"], "r2.cloudflarestorage.com") {
			accountID := strings.Split(fsProps["ENDPOINT"], ".")[0]
			secretProps = append(secretProps, g.F("ACCOUNT_ID '%s'", accountID))
			// Add ACCOUNT_ID to fsProps so it gets picked up later
			fsProps["ACCOUNT_ID"] = accountID
			secretKeyMap["ACCOUNT_ID"] = "ACCOUNT_ID"
			scopeScheme = "r2"
			uri = strings.ReplaceAll(uri, "s3://", "r2://")
			secretType = DuckDbSecretTypeR2
		} else {
			secretType = DuckDbSecretTypeS3
		}

		// set endpoint
		if endpoint := fsProps["ENDPOINT"]; endpoint != "" {
			if _, ok := fsProps["USE_SSL"]; !ok && strings.HasPrefix(endpoint, "http://") {
				fsProps["USE_SSL"] = "false" // default is true
			}
			// clean up endpoint scheme
			fsProps["ENDPOINT"] = strings.TrimPrefix(endpoint, "https://")
			fsProps["ENDPOINT"] = strings.TrimPrefix(endpoint, "http://")
		}

		// add default provider chain (https://duckdb.org/docs/extensions/httpfs/s3api.html#credential_chain-provider)
		s3DefaultSecret := NewDuckDbSecret(
			"s3_default", DuckDbSecretTypeS3,
			map[string]string{"provider": "credential_chain", "chain": "env;config"},
		)
		duck.AddSecret(s3DefaultSecret)

	case dbio.TypeFileGoogle:
		secretKeyMap = map[string]string{
			"ACCESS_KEY_ID":     "KEY_ID",
			"SECRET_ACCESS_KEY": "SECRET",
		}
		secretType = DuckDbSecretTypeGCS
		scopeScheme = "gcs"
		uri = strings.ReplaceAll(uri, "gs://", "gcs://")

	case dbio.TypeFileAzure:
		secretKeyMap = map[string]string{
			"CONN_STR":                "CONNECTION_STRING",
			"ACCOUNT":                 "ACCOUNT_NAME",
			"ACCOUNT_KEY":             "ACCOUNT_KEY",
			"SAS_TOKEN":               "SAS_TOKEN",
			"CLIENT_SECRET":           "CLIENT_SECRET",
			"HTTP_PROXY":              "HTTP_PROXY",
			"PROXY_USER_NAME":         "PROXY_USER_NAME",
			"PROXY_PASSWORD":          "PROXY_PASSWORD",
			"CLIENT_CERTIFICATE_PATH": "CLIENT_CERTIFICATE_PATH",
			"TENANT_ID":               "TENANT_ID",
			"CLIENT_ID":               "CLIENT_ID",
			"PROVIDER":                "PROVIDER",
		}
		secretType = DuckDbSecretTypeAzure

		if strings.Contains(uri, ".blob.core.windows.net/") {
			uri = strings.ReplaceAll(uri, "https://", "az://")
		}
		if strings.Contains(uri, ".dfs.core.windows.net/") {
			uri = strings.ReplaceAll(uri, "https://", "abfss://")
		}

		// need to provide CONNECTION_STRING
		if fsProps["SAS_SVC_URL"] != "" && fsProps["CONN_STR"] == "" && g.IsDebug() {
			g.Warn("to use duckdb to read from Azure, need to provide 'conn_str' instead of 'sas_svc_url'. See https://docs.slingdata.io/connections/file-connections/azure")
		}

		// add default provider chain (https://duckdb.org/docs/extensions/azure.html#credential_chain-provider)
		azureDefaultSecret := NewDuckDbSecret(
			"azure_default", DuckDbSecretTypeAzure,
			map[string]string{
				"provider":     "credential_chain",
				"chain":        "default;env;managed_identity;cli",
				"account_name": fsProps["ACCOUNT"],
			},
		)
		duck.AddSecret(azureDefaultSecret)
	}

	// populate secret props and make secret sql
	if len(secretProps) > 0 {
		props := map[string]string{}
		for slingKey, duckdbKey := range secretKeyMap {
			if val := fsProps[slingKey]; val != "" {
				if duckdbKey == "SCOPE" {
					val = scopeScheme + "://" + val
				}
				props[duckdbKey] = val
			}
		}
		secret := NewDuckDbSecret(g.F("%s_secret", secretType), secretType, props)
		duck.AddSecret(secret)
	}

	return uri
}

// AddSecret registers a new secret in session (also adds needed extension)
func (duck *DuckDb) AddSecret(secret DuckDbSecret) {
	duck.secrets = append(duck.secrets, secret)
	duck.initialized = false // to re-initialize

	// add extension
	switch secret.Type {
	case DuckDbSecretTypeS3:
		duck.AddExtension("aws")
		duck.AddExtension("httpfs")
	case DuckDbSecretTypeR2:
		duck.AddExtension("aws")
		duck.AddExtension("httpfs")
	case DuckDbSecretTypeAzure:
		duck.AddExtension("azure")
	case DuckDbSecretTypeGCS:
		duck.AddExtension("httpfs")
	}
}

// getLoadExtensionSQL generates SQL statements to load extensions
func (duck *DuckDb) getLoadExtensionSQL() (sql string) {
	for _, extension := range duck.extensions {
		if cast.ToBool(os.Getenv("DUCKDB_USE_INSTALLED_EXTENSIONS")) {
			sql += fmt.Sprintf("LOAD %s;", extension)
		} else {
			sql += fmt.Sprintf("INSTALL %s; LOAD %s;", extension, strings.TrimSuffix(extension, "from community"))
		}
	}
	return
}

// getCreateSecretSQL generates SQL statements to create secrets
func (duck *DuckDb) getCreateSecretSQL() (sql string) {
	for _, s := range duck.secrets {
		secret := s.Render()
		env.LogSQL(nil, secret+env.NoDebugKey)
		sql += fmt.Sprintf("%s;", secret)
	}
	return
}

// Open initializes the DuckDb connection
func (duck *DuckDb) Open(timeOut ...int) (err error) {
	if cast.ToBool(duck.GetProp("connected")) {
		return nil
	}

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

	if cast.ToBool(duck.GetProp("read_only")) {
		args = append(args, "-readonly")
	}

	if workingDir := duck.GetProp("working_dir"); workingDir != "" {
		duck.Proc.WorkDir = workingDir
	}

	if instance := duck.GetProp("instance"); instance != "" {
		args = append(args, instance)
	}

	if motherduckToken := duck.GetProp("motherduck_token"); motherduckToken != "" {
		duck.Proc.Env["motherduck_token"] = motherduckToken
		args = append(args, "md:"+duck.GetProp("database"))
	}

	// default extensions
	duck.AddExtension("json")

	duck.Proc.SetArgs(args...)

	// open process
	err = duck.Proc.Start()
	if err != nil {
		return g.Error(err, "Failed to start duckDB process")
	}

	// start the scanner
	duck.initScanner()

	duck.SetProp("connected", "true")

	_, err = duck.Exec("select 1" + env.NoDebugKey)
	if err != nil {
		if strings.Contains(err.Error(), " version") {
			g.Warn("To having sling use a different DuckDB version, set DUCKDB_VERSION=<version>")
		}
		return g.Error(err, "could not init connection")
	}

	return nil
}

// Close closes the connection
func (duck *DuckDb) Close() error {
	if duck.Proc == nil || duck.Proc.Exited() {
		return nil
	}

	duck.SetProp("connected", "false")

	// submit quit command?
	if duck.Proc.Cmd != nil {
		// need to submit to stdin: ".quit"
		duck.Proc.StdinWriter.Write([]byte(".quit\n"))

		// close stdin
		if err := duck.closeStdinAndWait(); err != nil {
			return err
		}
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
			g.Debug("killing pid %s", duck.Proc.Cmd.Process.Pid)
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

// SubmitSQL submits a sql query to duckdb via stdin
func (duck *DuckDb) SubmitSQL(sql string, showChanges bool) (err error) {

	// combine props with fs_props for proper masking in logging
	propsCombined := map[string]string{}
	g.Unmarshal(duck.GetProp("fs_props"), &propsCombined)
	for k, v := range duck.Props() {
		propsCombined[k] = v
	}

	extensionSecretSQL := ""
	if !duck.initialized {
		extensionsSQL := duck.getLoadExtensionSQL()
		secretSQL := duck.getCreateSecretSQL()
		extensionSecretSQL = extensionSecretSQL + strings.Trim(extensionsSQL, ";") + ";"
		extensionSecretSQL = extensionSecretSQL + strings.Trim(secretSQL, ";") + ";"
		duck.initialized = true // commented out for now
		env.LogSQL(propsCombined, extensionSecretSQL+env.NoDebugKey)
	}

	queryID := g.RandSuffix("", 3) // for debugging

	// submit sql to stdin
	sqlLines := []string{
		extensionSecretSQL,
		// preserve_insertion_order=false reduces the memory load.
		// See https://www.reddit.com/r/DuckDB/comments/1jnw1ed/got_outofmemory_while_etl_30gb_parquet_files_on_s3/
		"set preserve_insertion_order = false;",
		g.R("select '{v}' AS marker_{id};", "v", duckDbSOFMarker, "id", queryID),
		".changes on",
		sql + ";",
		".changes off",
		g.R("select '{v}' AS {v};\n", "v", duckDbEOFMarker),
	}

	if !showChanges {
		sqlLines = []string{
			extensionSecretSQL,
			g.R("select '{v}' AS marker_{id};", "v", duckDbSOFMarker, "id", queryID),
			sql + ";",
			g.R("select '{v}' AS {v};\n", "v", duckDbEOFMarker),
		}
	}
	env.LogSQL(propsCombined, sql)

	sqls := strings.Join(sqlLines, "\n")
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
		return g.Error(err, "Process errored")
	}

	return nil
}

// ExecContext executes a SQL query with context and returns the result
func (duck *DuckDb) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if !cast.ToBool(duck.GetProp("connected")) {
		if err = duck.Open(); err != nil {
			return nil, g.Error(err, "Could not open DuckDB connection")
		}
	}

	// one query at a time
	duck.Context.Lock()
	defer duck.Context.Unlock()

	dq := duck.newQuery(ctx, sql)

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
	err = duck.SubmitSQL(sql, true)
	if err != nil {
		return result, g.Error(err, "Failed to submit SQL")
	}

	// Extract total changes from stdout
	result, err = duck.waitForResult(dq)
	if err != nil {
		return result, g.Error(err, "Failed to execute SQL")
	}

	return result, nil
}

func (duck *DuckDb) newQuery(ctx context.Context, sql string) (query *duckDbQuery) {
	stdOutReader, stdOutWriter := io.Pipe() // new pipe
	duck.query = &duckDbQuery{
		SQL:     sql,
		Context: g.NewContext(ctx),
		reader:  stdOutReader,
		writer:  stdOutWriter,
	}
	return duck.query
}

// waitForResult waits for the execution of a SQL query and returns the result
func (duck *DuckDb) waitForResult(dq *duckDbQuery) (result sql.Result, err error) {

	stdOutReaderB := bufio.NewReader(dq.reader)

	// Extract total changes from stdout
	// This loop continuously reads the output from the DuckDB process
	// It looks for lines containing "total_changes:" to update the result
	// and stops when it encounters the EOF marker
	var line string
	for {
		time.Sleep(10 * time.Millisecond)

		if dq.err != nil {
			return result, dq.err
		}

		if dq.done {
			return result, nil
		}

		if !dq.started {
			continue
		}

		// try peeking
		_, err := stdOutReaderB.Peek(0)
		if err != nil {
			if err == io.EOF {
				// No more data available, but EOF marker not found yet
				goto next
			}
			return result, g.Error(err, "could not read output from stdout")
		}

		for {
			// read all available lines
			line, err = stdOutReaderB.ReadString('\n')
			if len(line) == 0 && err != nil {
				if err == io.EOF {
					// No more data available, but EOF marker not found yet
					goto next
				}
				return result, g.Error(err, "could not read output from stdout")
			}

			// g.Warn("line | %s", line)
			if strings.Contains(line, "total_changes:") {
				parts := strings.Fields(line)
				if len(parts) >= 4 {
					totalChanges, err := strconv.ParseUint(parts[3], 10, 64)
					if err == nil {
						result = duckDbResult{TotalChanges: cast.ToInt64(totalChanges)}
					}
				}
			} else if strings.Contains(line, duckDbEOFMarker) {
				return result, nil
			}
		}
	next:
	}
}

// Query runs a sql query, returns `Dataset`
func (duck *DuckDb) Query(sql string, options ...map[string]interface{}) (data Dataset, err error) {
	return duck.QueryContext(context.Background(), sql, options...)
}

// QueryContext runs a sql query with context, returns `Dataset`
func (duck *DuckDb) QueryContext(ctx context.Context, sql string, options ...map[string]interface{}) (data Dataset, err error) {

	ds, err := duck.StreamContext(ctx, sql, options...)
	if err != nil {
		return data, g.Error(err, "could not get datastream")
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
	if !cast.ToBool(duck.GetProp("connected")) {
		if err = duck.Open(); err != nil {
			return nil, g.Error(err, "Could not open DuckDB connection")
		}
	}

	queryCtx := g.NewContext(ctx)

	opts := g.M()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	}

	columns, describeErr := duck.Describe(sql)

	// one query at a time
	duck.Context.Lock()

	// new datastream
	ds = NewDatastreamContext(queryCtx.Ctx, columns)

	// Create a pipe for stdout, stderr handling
	dq := duck.newQuery(queryCtx.Ctx, sql)

	// start and submit sql
	err = duck.SubmitSQL(sql, false)
	if err != nil {
		duck.Context.Unlock() // release lock
		return nil, g.Error(err, "Failed to submit SQL")
	}

	// so that lists are treated as TEXT and not JSON
	// lists / arrays do not conform to JSON spec and can error out
	transforms := map[string][]string{"*": {TransformDuckdbListToText.Name}}

	// add any specified transforms
	fsProps := map[string]string{}
	g.Unmarshal(duck.GetProp("fs_props"), &fsProps)
	if transformsPayload, ok := fsProps["transforms"]; ok {
		columnTransforms := map[string][]string{}
		g.Unmarshal(transformsPayload, &columnTransforms)
		if val, ok := columnTransforms["*"]; ok {
			columnTransforms["*"] = append(val, transforms["*"]...)
		} else {
			columnTransforms["*"] = transforms["*"]
		}
		transforms = columnTransforms
	}

	if cds, ok := opts["datastream"]; ok {
		// if provided, use it
		ds = cds.(*Datastream)
		ds.Columns = columns
	}

	// handle filename, always last column
	if cast.ToBool(opts["filename"]) {
		// rename to _sling_stream_url
		ds.Columns[len(ds.Columns)-1].Name = ds.Metadata.StreamURL.Key
		ds.Metadata.StreamURL.Key = "" // so it is not added again
	}

	ds.Inferred = true
	ds.NoDebug = strings.Contains(sql, env.NoDebugKey)
	ds.SetConfig(duck.Props())
	ds.SetConfig(map[string]string{"delimiter": ",", "header": "true", "transforms": g.Marshal(transforms), "null_if": `\N\`})

	ds.Defer(func() {
		duck.Context.Mux.TryLock()
		duck.Context.Unlock() // release lock
	})

	err = ds.ConsumeCsvReader(dq.reader)
	if err != nil {
		ds.Close()
		return ds, g.Error(err, "could not read output stream")
	}

	if dq.err != nil {
		return ds, dq.err
	} else if describeErr != nil {
		// should never occur, since if Describe fails, SubmitSQL should fail
		// to get better error. but just in case it does, log error
		g.LogError(describeErr)
	}

	return
}

// initScanner is set only once
func (duck *DuckDb) initScanner() {

	errString := strings.Builder{}
	var errTimer, eofTimer *time.Timer

	var stdOutWriter *io.PipeWriter
	resetWriter := func() {
		if stdOutWriter != nil {
			stdOutWriter.Close()
		}
		stdOutWriter = nil // set as nil until next query start
		duck.query.done = true
	}

	duck.Proc.SetScanner(func(stderr bool, line string) {
		// g.Warn("stderr:%v done:%v | %s", stderr, duck.query.done, line)

		if duck.query == nil || duck.query.done {
			return
		}

		select {
		case <-duck.query.Context.Ctx.Done():
			resetWriter()
			return
		default:
		}

		if stderr {
			// the issue is we don't know when the error message ends.
			// so we have a timer to use as a debouce, to concat the error lines
			if errTimer != nil {
				errTimer.Stop() // cancel timer for new one
			}

			// it can occur that the error outputs right after the EOF marker
			if eofTimer != nil {
				eofTimer.Stop()
				eofTimer = nil
			}

			errString.WriteString(line)
			errTimer = time.AfterFunc(25*time.Millisecond, func() {
				suffix := g.F("For query => %s", duck.query.SQL)
				duck.query.err = g.Error(errString.String() + "\n" + suffix)
				errString.Reset()
				resetWriter() // in case writer is active
			})
		} else {
			if strings.Contains(line, duckDbEOFMarker) {
				eofTimer = time.AfterFunc(25*time.Millisecond, func() {
					resetWriter() // since result set ended
				})
			} else if strings.Contains(line, duckDbSOFMarker) {
				stdOutWriter = duck.query.writer
				duck.query.started = true
			} else if stdOutWriter != nil {
				_, err := stdOutWriter.Write([]byte(line + "\n"))
				if err != nil {
					duck.query.err = g.Error(err, "Failed to write to stdout pipe")
					resetWriter() // since we errored
				}
			}
		}
	})

}

type DuckDbCopyOptions struct {
	Format             dbio.FileType
	Compression        CompressorType
	PartitionFields    []PartitionLevel // part_year, part_month, part_day, etc.
	PartitionKey       string
	WritePartitionCols bool
	FileSizeBytes      int64
}

func (duck *DuckDb) GenerateCopyStatement(fromTable, toLocalPath string, options DuckDbCopyOptions) (sql string, err error) {
	type partExpression struct {
		alias      string
		expression string
	}

	var partExpressions []partExpression

	// validate PartitionLevels
	for _, pl := range options.PartitionFields {
		if !pl.IsValid() {
			return sql, g.Error("invalid partition level: %s", pl)
		} else if options.PartitionKey == "" {
			return "", g.Error("missing partition key")
		}

		pe := partExpression{
			alias:      g.F("%s_%s", dbio.TypeDbDuckDb.Unquote(options.PartitionKey), pl),
			expression: g.F("date_part('%s', %s)", pl, options.PartitionKey),
		}

		switch pl {
		case PartitionLevelYear:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%Y")
		case PartitionLevelYearMonth:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%Y-%m")
		case PartitionLevelMonth:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%m")
		case PartitionLevelWeek:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%V")
		case PartitionLevelDay:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%d")
		case PartitionLevelHour:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%H")
		case PartitionLevelMinute:
			pe.expression = g.F("strftime(%s, '%s')", options.PartitionKey, "%M")
		default:
			return sql, g.Error("invalid partition field: %s", pl)
		}

		partExpressions = append(partExpressions, pe)
	}

	// validate compression
	fileExtensionExpr := ""
	if options.Format == dbio.FileTypeParquet {
		if g.In(options.Compression, "", "none", "auto") {
			options.Compression = SnappyCompressorType
		}
	}
	if options.Format == dbio.FileTypeCsv {
		if g.In(options.Compression, GzipCompressorType) {
			fileExtensionExpr = g.F("file_extension 'csv.gz',")
		}
	}

	fileSizeBytesExpr := ""
	if options.FileSizeBytes > 0 {
		fileSizeBytesExpr = g.F("per_thread_output true, file_size_bytes %d,", options.FileSizeBytes)
	}

	if len(partExpressions) > 0 {
		partSqlColumns := make([]string, len(partExpressions))
		partSqlExpressions := make([]string, len(partExpressions))
		for i, partExpression := range partExpressions {
			aliasQ := dbio.TypeDbDuckDb.Quote(partExpression.alias)
			partSqlColumns[i] = aliasQ
			partSqlExpressions[i] = g.F("%s as %s", partExpression.expression, aliasQ)
		}

		sql = g.R(
			dbio.TypeDbDuckDb.GetTemplateValue("core.export_to_local_partitions"),
			"table", fromTable,
			"local_path", toLocalPath,
			"format", string(options.Format),
			"file_size_bytes_expr", fileSizeBytesExpr,
			"file_extension_expr", fileExtensionExpr,
			"partition_expressions", strings.Join(partSqlExpressions, ", "),
			"partition_columns", strings.Join(partSqlColumns, ", "),
			"compression", string(options.Compression),
			"write_partition_columns", cast.ToString(options.WritePartitionCols),
		)
	} else {
		sql = g.R(
			dbio.TypeDbDuckDb.GetTemplateValue("core.export_to_local"),
			"table", fromTable,
			"local_path", toLocalPath,
			"format", string(options.Format),
			"file_size_bytes_expr", fileSizeBytesExpr,
			"file_extension_expr", fileExtensionExpr,
			"compression", string(options.Compression),
		)
	}

	return
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
			downloadURL = "https://github.com/duckdb/duckdb/releases/download/v{version}/duckdb_cli-linux-arm64.zip"

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

// Describe returns the columns of a query
func (duck *DuckDb) Describe(query string) (columns Columns, err error) {
	// prevent infinite loop
	queryStriped, _ := StripSQLComments(query)
	queryL := strings.TrimSpace(strings.ToLower(queryStriped))
	if !(strings.HasPrefix(queryL, "with") || strings.HasPrefix(queryL, "select")) {
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
		col.Type = NativeTypeToGeneral(col.Name, col.DbType, dbio.TypeDbDuckDb)
		col.Position = k + 1
		col.Sourced = true

		// fill in precision/scale if decimal
		if dbType := strings.ToLower(col.DbType); strings.HasPrefix(dbType, "decimal(") {
			dbType = strings.ReplaceAll(dbType, " ", "")
			precScale := strings.ReplaceAll(dbType, "decimal", "")
			precScale = strings.Trim(precScale, "()")
			precScaleParts := strings.Split(precScale, ",")
			col.DbPrecision = cast.ToInt(precScaleParts[0])
			if len(precScaleParts) > 1 {
				col.DbScale = cast.ToInt(precScaleParts[1])
			}
		}

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return nil, g.Error("no columns found for: %s", query)
	} else {
		g.Trace("duckdb describe got %d column(s)", len(columns))
	}

	return
}

func (duck *DuckDb) GetScannerFunc(format dbio.FileType) (scanFunc string) {

	switch format {
	case dbio.FileTypeDelta:
		scanFunc = "delta_scanner"
	case dbio.FileTypeIceberg:
		scanFunc = "iceberg_scanner"
	case dbio.FileTypeParquet:
		scanFunc = "parquet_scanner"
	case dbio.FileTypeCsv:
		scanFunc = "csv_scanner"
	}

	return
}

type HttpStreamPart struct {
	Index    int
	FromExpr string
	Columns  Columns
}

func (duck *DuckDb) DataflowToHttpStream(df *Dataflow, sc StreamConfig) (streamPartChn chan HttpStreamPart, err error) {
	// create fromExprChn channel
	streamPartChn = make(chan HttpStreamPart)

	// start local http server
	port, err := g.GetPort("localhost:0")
	if err != nil {
		err = g.Error(err, "could not acquire local port for duckdb http import")
		return streamPartChn, err
	}
	// create reader channel to pass between handler and main flow
	readerCh := make(chan io.Reader)
	doneCh := make(chan bool)

	// determine content type and format based on sc.Format
	contentType := "text/csv"
	format := dbio.FileTypeCsv // default to CSV
	if sc.Format == dbio.FileTypeArrow {
		contentType = "application/vnd.apache.arrow.stream"
		format = dbio.FileTypeArrow
	}

	// create http server to serve data
	importContext := g.NewContext(duck.Context.Ctx)
	httpURL := g.F("http://localhost:%d/data", port)
	server := echo.New()
	{
		server.HidePort = true
		server.HideBanner = true
		// server.Use(middleware.Logger())
		server.Add(http.MethodGet, "/data", func(c echo.Context) (err error) {
			reader := <-readerCh
			if reader != nil {
				defer func() { doneCh <- true }()
				return c.Stream(200, contentType, reader)
			}
			return c.NoContent(http.StatusOK)
		})

		server.Add(http.MethodHead, "/data", func(c echo.Context) error {
			c.Response().Header().Set("Content-Type", contentType)
			return c.NoContent(http.StatusOK)
		})

		// start server in background, wait for it to start
		importContext.Wg.Read.Add()
		go func() {
			g.Debug("started %s for duckdb direct stream", httpURL)
			importContext.Wg.Read.Done()
			if err := server.Start(g.F("localhost:%d", port)); err != http.ErrServerClosed {
				g.Error(err, "duckdb import http server error")
			}
		}()
	}

	// wait for local server startup
	importContext.Wg.Read.Wait()
	time.Sleep(100 * time.Millisecond)
	df.Defer(func() { server.Shutdown(importContext.Ctx) })

	df.SetBatchLimit(sc.BatchLimit)
	ds := MergeDataflow(df)

	go func() {
		defer close(streamPartChn)
		var partIndex int

		if format == dbio.FileTypeArrow {
			// Use Arrow format
			for batchR := range ds.NewArrowReaderChnl(sc) {
				g.Trace("processing duckdb arrow batch %s", batchR.Batch.ID())

				// Stream data directly without buffering all in memory
				// Create a pipe to stream data through
				pipeR, pipeW := io.Pipe()

				// Use read_arrow for Arrow format
				fromExpr := g.F(`read_arrow('%s')`, httpURL)

				streamPartChn <- HttpStreamPart{
					Index:    partIndex,
					FromExpr: fromExpr,
					Columns:  batchR.Columns,
				}

				// Stream data through pipe
				go func() {
					defer pipeW.Close()
					_, err := io.Copy(pipeW, batchR.Reader)
					if err != nil {
						g.LogError(g.Error(err, "failed to stream arrow batch"))
					}
				}()

				readerCh <- pipeR
				<-doneCh

				partIndex++
			}
		} else {
			// Default to CSV format
			for batchR := range ds.NewCsvReaderChnl(sc) {
				g.Trace("processing duckdb batch %s", batchR.Batch.ID())

				// Stream data directly without buffering all in memory
				// Create a pipe to stream data through
				pipeR, pipeW := io.Pipe()

				// can use this as a from table
				fromExpr := g.F(`read_csv('%s', delim=',', header=True, columns=%s, max_line_size=2000000, parallel=false, quote='"', escape='"', nullstr='\N', auto_detect=false)`, httpURL, duck.GenerateCsvColumns(batchR.Columns))

				streamPartChn <- HttpStreamPart{
					Index:    partIndex,
					FromExpr: fromExpr,
					Columns:  batchR.Columns,
				}

				// Stream data through pipe
				go func() {
					defer pipeW.Close()
					_, err := io.Copy(pipeW, batchR.Reader)
					if err != nil {
						g.LogError(g.Error(err, "failed to stream csv batch"))
					}
				}()

				readerCh <- pipeR
				<-doneCh

				partIndex++
			}
		}
	}()

	return streamPartChn, nil
}

func (duck *DuckDb) DefaultCsvConfig() (config StreamConfig) {
	config = DefaultStreamConfig()
	config.FileMaxRows = 250000
	config.Header = true
	config.Delimiter = ","
	config.Escape = `"`
	config.Quote = `"`
	config.NullAs = `\N`
	config.DatetimeFormat = dbio.TypeDbDuckDb.GetTemplateValue("variable.timestampz_layout")
	return config
}

func (duck *DuckDb) GenerateCsvColumns(columns Columns) (colStr string) {
	// {'FlightDate': 'DATE', 'UniqueCarrier': 'VARCHAR', 'OriginCityName': 'VARCHAR', 'DestCityName': 'VARCHAR'}

	colsArr := make([]string, len(columns))
	for i, col := range columns {
		nativeType, err := col.GetNativeType(dbio.TypeDbDuckDb, ColumnTyping{})
		if err != nil {
			g.Warn(err.Error())
		}

		// CSVs cannot handle binary data, so let's set to varchar.
		// https://duckdb.org/docs/sql/functions/blob.html
		if nativeType == "binary" {
			nativeType = "varchar"
		}

		colsArr[i] = g.F("'%s':'%s'", col.Name, nativeType)
	}

	return "{" + strings.Join(colsArr, ", ") + "}"
}

func (duck *DuckDb) MakeScanQuery(format dbio.FileType, uri string, fsc FileStreamConfig) (sql string) {

	where := ""
	incrementalWhereCond := "1=1"

	uris := strings.Split(uri, DuckDbURISeparator)
	workDir := duck.GetProp("working_dir")
	for i, val := range uris {
		if workDir != "" {
			val = strings.TrimPrefix(strings.TrimPrefix(val, workDir), "/")
		}
		uris[i] = g.F("'%s'", val) // add quotes
	}

	// reserved word to use for timestamp comparison (when listing)
	const slingLoadedAtColumn = "_sling_loaded_at"
	if fsc.IncrementalKey != "" && fsc.IncrementalKey != slingLoadedAtColumn &&
		fsc.IncrementalValue != "" {
		incrementalWhereCond = g.F("%s > %s", dbio.TypeDbDuckDb.Quote(fsc.IncrementalKey), fsc.IncrementalValue)
		where = g.F("where %s", incrementalWhereCond)
	}

	if format == dbio.FileTypeNone {
		g.Warn("duck.MakeScanQuery: format is empty, cannot determine stream_scanner")
	}

	duckdbFilenameStr := ""
	if fsc.DuckDBFilename {
		duckdbFilenameStr = g.F(", filename = true")
	}

	streamScanner := dbio.TypeDbDuckDb.GetTemplateValue("function." + duck.GetScannerFunc(format))
	if fsc.SQL != "" {
		sql = g.R(
			g.R(fsc.SQL, "stream_scanner", streamScanner),
			"incremental_where_cond", incrementalWhereCond,
			"incremental_value", fsc.IncrementalValue,
			"uri", uri,
			"uris", strings.Join(uris, ", "),
			"filename_expr", duckdbFilenameStr,
		)

		if fsc.Limit > 0 {
			sql = g.F("select * from ( %s ) as t limit %d", sql, fsc.Limit)
		}
		return sql
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
		"uris", strings.Join(uris, ", "),
		"where", where,
		"filename_expr", duckdbFilenameStr,
	))

	if fsc.Limit > 0 {
		sql += fmt.Sprintf(" limit %d", fsc.Limit)
	}

	return sql
}

// StripSQLComments removes all SQL comments (-- or /* */) from the provided SQL string
func StripSQLComments(sql string) (string, error) {
	inQuote := false
	inCommentLine := false
	inCommentMulti := false
	result := strings.Builder{}

	for i := 0; i < len(sql); i++ {
		// Check for comment start/end sequences
		if !inQuote {
			// Check for start of line comment (--)
			if i < len(sql)-1 && sql[i] == '-' && sql[i+1] == '-' && !inCommentMulti {
				inCommentLine = true
				i++ // Skip the second '-'
				continue
			}

			// Check for start of multi-line comment (/*)
			if i < len(sql)-1 && sql[i] == '/' && sql[i+1] == '*' && !inCommentLine {
				inCommentMulti = true
				i++ // Skip the '*'
				continue
			}

			// Check for end of multi-line comment (*/)
			if i < len(sql)-1 && sql[i] == '*' && sql[i+1] == '/' && inCommentMulti {
				inCommentMulti = false
				i++ // Skip the '/'
				continue
			}
		}

		// Handle quotes
		if sql[i] == '\'' && !inCommentLine && !inCommentMulti {
			// Check for escaped quote ('')
			if i < len(sql)-1 && sql[i+1] == '\'' {
				result.WriteByte(sql[i])
				result.WriteByte(sql[i+1])
				i++ // Skip the second quote
				continue
			}
			inQuote = !inQuote
		}

		// End of line comment ends at newline
		if sql[i] == '\n' && inCommentLine {
			inCommentLine = false
		}

		// Only write character if not in a comment
		if !inCommentLine && !inCommentMulti {
			result.WriteByte(sql[i])
		}
	}

	return result.String(), nil
}
