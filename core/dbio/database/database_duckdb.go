package database

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/env"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// DuckDbConn is a Duck DB connection
type DuckDbConn struct {
	BaseConn
	URL string

	isInteractive     bool
	cmdInteractive    *exec.Cmd      // For interactive mode
	stdInInteractive  io.WriteCloser // For interactive mode
	stdOutInteractive *bufio.Scanner // For interactive mode
	stdErrInteractive *duckDbBuffer  // For interactive mode
}

var DuckDbVersion = "0.10.0"
var DuckDbUseTempFile = false
var DuckDbFileContext = map[string]*g.Context{} // so that collision doesn't happen
var DuckDbFileCmd = map[string]*exec.Cmd{}
var duckDbReadOnlyHint = "/* -readonly */"

// Init initiates the object
func (conn *DuckDbConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDuckDb
	if strings.HasPrefix(conn.URL, "motherduck") {
		conn.BaseConn.Type = dbio.TypeDbMotherDuck
	}

	if _, ok := DuckDbFileContext[conn.URL]; !ok {
		c := g.NewContext(conn.Context().Ctx)
		DuckDbFileContext[conn.URL] = &c
	}

	conn.isInteractive = cast.ToBool(conn.GetProp("interactive"))
	if conn.BaseConn.Type == dbio.TypeDbMotherDuck && conn.GetProp("interactive") == "" {
		conn.isInteractive = true // default interactive true for motherduck
	}

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *DuckDbConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	URL := strings.ReplaceAll(
		connURL,
		"duckdb://",
		"",
	)
	URL = strings.ReplaceAll(
		URL,
		"motherduck://",
		"md:",
	)
	return URL
}

func (conn *DuckDbConn) dbPath() (string, error) {
	dbPathU, err := net.NewURL(conn.GetURL())
	if err != nil {
		err = g.Error(err, "could not get duckdb file path")
		return "", err
	}
	dbPath := strings.ReplaceAll(conn.GetURL(), "?"+dbPathU.U.RawQuery, "")
	return dbPath, nil
}

func (conn *DuckDbConn) Connect(timeOut ...int) (err error) {
	connURL := conn.GetURL()

	dbPath, err := conn.dbPath()
	if err != nil {
		return g.Error(err, "could not get db path")
	} else if conn.GetType() != dbio.TypeDbMotherDuck && !g.PathExists(dbPath) {
		g.Warn("The file %s does not exist, however it will be created if needed.", dbPath)
	}

	connPool.Mux.Lock()
	dbConn, poolOk := connPool.DuckDbs[connURL]
	connPool.Mux.Unlock()

	if poolOk {
		conn.cmdInteractive = dbConn.cmdInteractive
		conn.stdInInteractive = dbConn.stdInInteractive
		conn.stdOutInteractive = dbConn.stdOutInteractive
		conn.stdErrInteractive = dbConn.stdErrInteractive
	}

	usePool = os.Getenv("USE_POOL") == "TRUE"
	if usePool && !poolOk {
		connPool.Mux.Lock()
		connPool.DuckDbs[connURL] = conn
		connPool.Mux.Unlock()
	}

	conn.SetProp("connected", "true")

	// init extensions
	conn.Exec("INSTALL json; LOAD json;" + noDebugKey)

	if conn.GetType() == dbio.TypeDbMotherDuck {
		conn.Exec("SET autoinstall_known_extensions=1; SET autoload_known_extensions=1;" + noDebugKey)
	}

	return nil
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
			return "", g.Error("duckdb binary not found")
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

	folderPath := path.Join(g.UserHomeDir(), "duckdb", version)
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

		paths, err := iop.Unzip(zipPath, folderPath)
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

// ExecContext runs a sql query with context, returns `error`
func (conn *DuckDbConn) ExecMultiContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	return conn.ExecContext(ctx, sql, args...)
}

type duckDbResult struct {
	TotalRows uint64
	res       driver.Result
}

// ducbDbBuffer syncs reset/writes
// from https://stackoverflow.com/a/59946981
type duckDbBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *duckDbBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *duckDbBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func (b *duckDbBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *duckDbBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Bytes()
}

func (conn *DuckDbConn) getCmd(sql string, readOnly bool) (cmd *exec.Cmd, sqlPath string, err error) {

	bin, err := EnsureBinDuckDB(conn.GetProp("duckdb_version"))
	if err != nil {
		return cmd, "", g.Error(err, "could not get duckdb binary")
	}

	sqlPath, err = writeTempSQL(sql)
	if err != nil {
		return cmd, "", g.Error(err, "could not create temp sql file for duckdb")
	}

	dbPath, err := conn.dbPath()
	if err != nil {
		os.Remove(sqlPath)
		err = g.Error(err, "could not get duckdb file path")
		return
	}

	cmd = exec.Command(bin)
	if readOnly {
		cmd.Args = append(cmd.Args, "-readonly")
	}

	if conn.isInteractive {
		cmd.Args = append(cmd.Args, "-csv", "-cmd", "PRAGMA threads=4", dbPath)
		if conn.cmdInteractive == nil || (conn.cmdInteractive.ProcessState != nil && conn.cmdInteractive.ProcessState.Exited()) {
			conn.stdInInteractive, err = cmd.StdinPipe()
			if err != nil {
				err = g.Error(err, "could not get conn.stdInInteractive")
				return
			}

			stdOutInteractive, err := cmd.StdoutPipe()
			if err != nil {
				return cmd, sqlPath, g.Error(err, "could not get conn.stdOutInteractive")
			}

			conn.stdErrInteractive = &duckDbBuffer{}
			cmd.Stderr = conn.stdErrInteractive

			conn.stdOutInteractive = bufio.NewScanner(stdOutInteractive)
			conn.stdOutInteractive.Split(bufio.ScanLines)

			conn.cmdInteractive = cmd

			go func() {
				err := conn.cmdInteractive.Run()
				conn.cmdInteractive = nil
				if err != nil {
					if stdErr := conn.stdErrInteractive.String(); stdErr != "" {
						conn.Context().CaptureErr(g.Error(stdErr))
					} else {
						conn.Context().CaptureErr(g.Error(err, "error running duckdb interactive command"))
					}
				}
			}()
		} else {
			cmd = conn.cmdInteractive
		}
	} else {
		cmd.Args = append(cmd.Args, dbPath, g.F(`.read %s`, sqlPath))
	}

	// set token in env
	if motherduckToken := conn.GetProp("motherduck_token"); motherduckToken != "" {
		cmd.Env = append(os.Environ(), "motherduck_token="+motherduckToken)
	}

	return cmd, sqlPath, nil
}

func (r duckDbResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r duckDbResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

func (conn *DuckDbConn) submitToCmdStdin(ctx context.Context, sql string) (stdOutReader io.ReadCloser, stderrBuf *bytes.Buffer, err error) {
	// submit to stdin
	sql = strings.TrimLeft(strings.TrimSpace(sql), ";")
	_, err = io.Copy(conn.stdInInteractive, strings.NewReader(sql+" ;\n"))
	if err != nil {
		err = g.Error(err, "could not submit SQL via stdin to duckdb process")
		return
	}

	// we use PRAGMA version; to signal to end of the result
	_, err = io.Copy(conn.stdInInteractive, strings.NewReader("PRAGMA version ;\n"))
	if err != nil {
		err = g.Error(err, "could not submit 'PRAGMA version' via stdin to duckdb process")
		return
	}

	// scan stdout / stderr
	// create new reader which is scanned and controlled
	// detect end with 'PRAGMA version'
	pragmaVersionHeaders := []byte("library_version,source_id")
	var stdOutPipeW *io.PipeWriter

	stdOutReader, stdOutPipeW = io.Pipe()
	stderrBuf = bytes.NewBuffer([]byte{})

	go func() {
		defer stdOutPipeW.Close()

		var pragmaVersionLine []byte
		isPragmaVersionHeaders := false
		for conn.stdOutInteractive.Scan() {
			lineByte := append(conn.stdOutInteractive.Bytes(), '\n')
			if isPragmaVersionHeaders && bytes.HasPrefix(lineByte, []byte{'v'}) {
				stderrBuf.Write(conn.stdErrInteractive.Bytes())
				conn.stdErrInteractive.Reset()
				return // end reader since it matches 'PRAGMA version' output
			} else if !isPragmaVersionHeaders && bytes.HasPrefix(lineByte, pragmaVersionHeaders) {
				pragmaVersionLine = lineByte
				isPragmaVersionHeaders = true
			} else if len(bytes.TrimSpace(lineByte)) != 0 {
				if isPragmaVersionHeaders {
					// in case it is not the 'PRAGMA version' output
					stdOutPipeW.Write(append(pragmaVersionLine, '\n'))
					isPragmaVersionHeaders = false
				}
				stdOutPipeW.Write(lineByte)
			}
		}
		if conn.stdOutInteractive.Err() != nil {
			g.LogError(g.Error(conn.stdOutInteractive.Err()))
		}
	}()

	return
}

func (conn *DuckDbConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	var stderr bytes.Buffer

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

	readOnly := cast.ToBool(conn.GetProp("read_only"))
	if strings.Contains(sql, duckDbReadOnlyHint) {
		readOnly = true
	}
	cmd, sqlPath, err := conn.getCmd(sql, readOnly)
	if err != nil {
		return result, g.Error(err, "could not get cmd duckdb")
	}

	conn.LogSQL(sql, args...)

	var out []byte
	DuckDbFileCmd[conn.URL] = cmd
	fileContext := DuckDbFileContext[conn.URL]
	fileContext.Mux.Lock()
	if conn.isInteractive {
		stdOutReader, stderrBuf, err := conn.submitToCmdStdin(ctx, sql)
		if err != nil {
			return result, g.Error(err, "could not exec SQL for duckdb via stdin")
		}
		out, _ = io.ReadAll(stdOutReader)
		outE := stderrBuf.Bytes()
		stderr = *bytes.NewBuffer(outE)

	} else {

		cmd.Stderr = &stderr

		out, err = cmd.Output()

		os.Remove(sqlPath) // delete sql temp file
	}
	// time.Sleep(400 * time.Millisecond) // so that cmd releases process
	fileContext.Mux.Unlock()

	if err != nil || strings.Contains(stderr.String(), "Error: ") {
		errText := g.F("could not exec SQL for duckdb: %s\n%s\n%s", string(out), stderr.String(), sql)
		if strings.Contains(errText, "version number") {
			errText = "Please set the DuckDB version with environment variable DUCKDB_VERSION. Example: DUCKDB_VERSION=0.6.0\n" + errText
		} else if err == nil {
			err = g.Error("DuckDB Error")
		}
		return result, g.Error(err, errText)
	} else if cmd.ProcessState != nil && cmd.ProcessState.ExitCode() != 0 {
		errText := g.F("could not exec SQL for duckdb: %s\n%s\n%s", string(out), stderr.String(), sql)
		if strings.Contains(errText, "version number") {
			errText = "Please set the DuckDB version with environment variable DUCKDB_VERSION. Example: DUCKDB_VERSION=0.6.0\n" + errText
		}
		if conn.GetType() == dbio.TypeDbMotherDuck && string(out)+stderr.String() == "" && cmd.ProcessState.ExitCode() == 1 {
			errText = "Perhaps your Motherduck token needs to be renewed?"
		}
		err = g.Error("exit code is %d", cmd.ProcessState.ExitCode())
		return result, g.Error(err, errText)

	}

	result = duckDbResult{}

	return
}

func (conn *DuckDbConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {

	// https://duckdb.org/docs/sql/statements/copy
	// copySQL := g.F("COPY ( %s ) TO '/dev/stdout' ( FORMAT CSV, header true )", sql) // works, but types are not preserved. BIGINT can be Messed up in javascript.
	// copySQL := g.F("COPY ( %s ) TO '/dev/stdout' ( FORMAT JSON )", sql) // works, but need to flatten, and becomes out of order
	// copySQL := g.F("COPY ( %s ) TO '/dev/stdout' ( FORMAT PARQUET, compression uncompressed, FIELD_IDS 'auto' )", sql) // does not work, maybe use better parquet lib
	copySQL := sql
	readOnly := cast.ToBool(conn.GetProp("read_only"))
	if strings.Contains(sql, duckDbReadOnlyHint) {
		readOnly = true
	}
	cmd, sqlPath, err := conn.getCmd(copySQL, readOnly)
	if err != nil {
		return ds, g.Error(err, "could not get cmd duckdb")
	}
	defer func() { os.Remove(sqlPath) }()

	opts := getQueryOptions(options)
	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val

		// set as sourced
		for i := range fetchedColumns {
			fetchedColumns[i].Sourced = true
		}
	}

	conn.LogSQL(sql)

	cmd.Args = append(cmd.Args, "-csv")

	DuckDbFileCmd[conn.URL] = cmd
	fileContext := DuckDbFileContext[conn.URL]
	fileContext.Mux.Lock()

	var stdOutReader, stdErrReader io.ReadCloser
	var stdErrReaderB *bufio.Reader
	var stderrBuf *bytes.Buffer
	if conn.isInteractive {
		stdOutReader, stderrBuf, err = conn.submitToCmdStdin(ctx, sql)
		if err != nil {
			return ds, g.Error(err, "could not exec SQL for duckdb via stdin")
		}
	} else {
		stdOutReader, err = cmd.StdoutPipe()
		if err != nil {
			return ds, g.Error(err, "could not get stdout for duckdb")
		}

		stdErrReader, err = cmd.StderrPipe()
		if err != nil {
			return ds, g.Error(err, "could not get stderr for duckdb")
		}

		stdErrReaderB = bufio.NewReader(stdErrReader)

		err = cmd.Start()
		if err != nil {
			return ds, g.Error(err, "could not exec SQL for duckdb")
		}

		DuckDbFileCmd[conn.URL] = cmd
	}

	// so that lists are treated as TEXT and not JSON
	// lists / arrays do not conform to JSON spec and can error out
	transforms := map[string][]string{"*": {"duckdb_list_to_text"}}

	ds = iop.NewDatastream(fetchedColumns)
	ds.SafeInference = true
	ds.SetConfig(conn.Props())
	ds.SetConfig(map[string]string{"delimiter": ",", "header": "true", "transforms": g.Marshal(transforms)})
	ds.Defer(func() { fileContext.Mux.Unlock() })

	// ds.SetConfig(map[string]string{"flatten": "true"})
	// err = ds.ConsumeJsonReader(stdOutReader)
	// err = ds.ConsumeParquetReader(stdOutReader)
	err = ds.ConsumeCsvReader(stdOutReader)
	if err != nil {
		ds.Close()
		return ds, g.Error(err, "could not read output stream")
	}

	var errOut []byte
	if conn.isInteractive {
		errOut = stderrBuf.Bytes()
	} else {
		if size := stdErrReaderB.Buffered(); size > 0 {
			errOut, err = io.ReadAll(stdErrReader)
		}
	}

	if err != nil {
		return ds, g.Error(err, "could not read error stream")
	} else if errOutS := string(errOut); strings.Contains(errOutS, "Error: ") {
		return ds, g.Error(errOutS)
	}

	return
}

// Close closes the connection
func (conn *DuckDbConn) Close() error {
	fileContext := DuckDbFileContext[conn.URL]
	if cmd, ok := DuckDbFileCmd[conn.URL]; ok {
		cmd.Process.Kill()
		fileContext.Mux.TryLock() // in case it is already unlocked
		fileContext.Mux.Unlock()
	}

	fileContext.Mux.Lock()

	// submit quit command
	if conn.isInteractive && conn.cmdInteractive != nil {
		conn.submitToCmdStdin(conn.context.Ctx, ".quit")
	}

	// kill timer
	timer := time.NewTimer(5 * time.Second)
	done := make(chan struct{})
	go func() {
		if conn.cmdInteractive != nil {
			conn.cmdInteractive.Process.Wait()
		}
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-timer.C:
		if conn.cmdInteractive != nil {
			conn.cmdInteractive.Process.Kill()
		}
	}
	conn.cmdInteractive = nil

	fileContext.Unlock()

	return nil
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *DuckDbConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *DuckDbConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// BulkImportStream inserts a stream into a table
func (conn *DuckDbConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	defer ds.Close()
	var columns iop.Columns

	// FIXME: batching works better when transactions are closed
	// seems, when the appender is closed, the transaction is closed as well
	conn.Commit()

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get list of columns from table")
			}

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		// write to temp CSV
		csvPath := path.Join(env.GetTempFolder(), g.NewTsID("duckdb.temp")+".csv")

		// set header false
		cfgMap := ds.GetConfig()
		cfgMap["header"] = "true"
		cfgMap["delimiter"] = ","
		cfgMap["datetime_format"] = "2006-01-02 15:04:05.000000-07:00"
		ds.SetConfig(cfgMap)

		if runtime.GOOS == "windows" || DuckDbUseTempFile || conn.isInteractive {
			// set batch rows limit, so we don't run out of space for massive datasets
			batch.Limit = 1000000
			if val := cast.ToInt64(conn.GetProp("file_max_rows")); val > 0 {
				batch.Limit = val
			}

			fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal)
			if err != nil {
				err = g.Error(err, "could not obtain client for temp file")
				return 0, err
			}

			_, err = fs.Write("file://"+csvPath, ds.NewCsvReader(0, 0))
			if err != nil {
				err = g.Error(err, "could not write to temp file")
				return 0, err
			}
		} else {
			csvPath = "/dev/stdin"
		}

		columnNames := lo.Map(columns.Names(), func(col string, i int) string {
			return `"` + col + `"`
		})

		sqlLines := []string{
			g.F(`insert into %s (%s) select * from read_csv('%s', delim=',', header=True, columns=%s, max_line_size=134217728, parallel=false);`, table.FDQN(), strings.Join(columnNames, ", "), csvPath, conn.generateCsvColumns(ds.Columns)),
		}

		var out []byte
		var stdOutReader io.ReadCloser
		stderrBuf := bytes.NewBuffer([]byte{})
		sql := strings.Join(sqlLines, ";\n")

		conn.LogSQL(sql)

		cmd, sqlPath, err := conn.getCmd(sql, cast.ToBool(conn.GetProp("read_only")))
		if err != nil {
			os.Remove(csvPath)
			return count, g.Error(err, "could not get cmd duckdb")
		}

		DuckDbFileCmd[conn.URL] = cmd
		fileContext := DuckDbFileContext[conn.URL]
		fileContext.Mux.Lock()
		if conn.isInteractive {
			stdOutReader, stderrBuf, err = conn.submitToCmdStdin(conn.Context().Ctx, sql)
			if err != nil {
				return count, g.Error(err, "could not exec import SQL for duckdb via stdin")
			}

			out, _ = io.ReadAll(stdOutReader)
		} else {

			if csvPath == "/dev/stdin" {
				cmd.Stdin = ds.NewCsvReader(0, 0)
			}

			cmd.Stderr = stderrBuf

			out, err = cmd.Output()
		}
		fileContext.Mux.Unlock()

		if csvPath != "/dev/stdin" {
			os.Remove(csvPath) // delete csv file
		}
		os.Remove(sqlPath) // delete sql temp file

		stderrVal := stderrBuf.String()
		if err != nil {
			return count, g.Error(err, "could not ingest for duckdb: %s\n%s", string(out), stderrVal)
		} else if strings.Contains(stderrVal, "Error: ") {
			return count, g.Error("DuckDB Error: %s\n%s", string(out), stderrVal)
		} else if strings.Contains(stderrVal, "expected") {
			return count, g.Error("could not ingest for duckdb: %s\n%s", string(out), stderrVal)
		}
	}

	return ds.Count, nil
}

func (conn *DuckDbConn) generateCsvColumns(columns iop.Columns) (colStr string) {
	// {'FlightDate': 'DATE', 'UniqueCarrier': 'VARCHAR', 'OriginCityName': 'VARCHAR', 'DestCityName': 'VARCHAR'}

	colsArr := make([]string, len(columns))
	for i, col := range columns {
		nativeType, err := conn.GetNativeType(col)
		if err != nil {
			g.Warn(err.Error())
		}
		colsArr[i] = g.F("'%s':'%s'", col.Name, nativeType)
	}

	return "{" + strings.Join(colsArr, ", ") + "}"
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *DuckDbConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// _, indexTable := SplitTableFullName(tgtTable)

	// indexSQL := g.R(
	// 	conn.GetTemplateValue("core.create_unique_index"),
	// 	"index", strings.Join(pkFields, "_")+"_idx",
	// 	"table", indexTable,
	// 	"cols", strings.Join(pkFields, ", "),
	// )

	// _, err = conn.Exec(indexSQL)
	// if err != nil {
	// 	err = g.Error(err, "could not create unique index")
	// 	return
	// }

	// V0.7
	// sqlTempl := `
	// INSERT INTO {tgt_table} as tgt
	// 	({insert_fields})
	// SELECT {src_fields}
	// FROM {src_table} as src
	// WHERE true
	// ON CONFLICT ({pk_fields})
	// DO UPDATE
	// SET {set_fields}
	// `

	sqlTempl := `
	DELETE FROM {tgt_table} tgt
	USING {src_table} src
	WHERE {src_tgt_pk_equal}
	;

	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_fields", upsertMap["src_fields"],
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"insert_fields", upsertMap["insert_fields"],
	)

	return
}
