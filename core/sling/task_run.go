package sling

import (
	"context"
	"net/http"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/slingdata-io/sling-cli/core"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// connPool a way to cache connections to that they don't have to reconnect
// for each replication steps
var connPool = map[string]database.Connection{}

var start time.Time
var slingLoadedAtColumn = "_sling_loaded_at"
var slingStreamURLColumn = "_sling_stream_url"
var slingRowNumColumn = "_sling_row_num"
var slingRowIDColumn = "_sling_row_id"

func init() {
	// we need a webserver to get the pprof webserver
	if cast.ToBool(os.Getenv("SLING_PPROF")) {
		go func() {
			g.Trace("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}
}

// Execute runs a Sling task.
// This may be a file/db to file/db transfer
func (t *TaskExecution) Execute() error {
	env.SetLogger()

	done := make(chan struct{})
	now := time.Now()
	t.StartTime = &now
	t.lastIncrement = now

	if t.Context == nil {
		ctx := g.NewContext(context.Background())
		t.Context = &ctx
	}

	// get stats of process at beginning
	t.ProcStatsStart = g.GetProcStats(os.Getpid())

	// set defaults
	t.Config.SetDefault()

	// print for debugging
	g.Trace("using Config:\n%s", g.Pretty(t.Config))
	go func() {
		defer close(done)
		defer t.PBar.Finish()

		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				t.Err = g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
			}
		}()

		t.Status = ExecStatusRunning

		if t.Err != nil {
			return
		}

		// update into store
		StoreUpdate(t)

		g.DebugLow("Sling version: %s (%s %s)", core.Version, runtime.GOOS, runtime.GOARCH)
		g.DebugLow("type is %s", t.Type)
		g.Debug("using source options: %s", g.Marshal(t.Config.Source.Options))
		g.Debug("using target options: %s", g.Marshal(t.Config.Target.Options))

		switch t.Type {
		case DbSQL:
			t.Err = t.runDbSQL()
		case FileToDB:
			t.Err = t.runFileToDB()
		case DbToDb:
			t.Err = t.runDbToDb()
		case DbToFile:
			t.Err = t.runDbToFile()
		case FileToFile:
			t.Err = t.runFileToFile()
		default:
			t.SetProgress("task execution configuration is invalid")
			t.Err = g.Error("Cannot Execute. Task Type is not specified")
		}

		// update into store
		StoreUpdate(t)
	}()

	select {
	case <-done:
		t.Cleanup()
	case <-t.Context.Ctx.Done():
		go t.Cleanup()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
		if t.Err == nil {
			t.Err = g.Error("Execution interrupted")
		}
	}

	if t.Err == nil {
		t.SetProgress("execution succeeded")
		t.Status = ExecStatusSuccess
	} else {
		t.SetProgress("execution failed")
		t.Status = ExecStatusError
		if err := t.df.Context.Err(); err != nil && err.Error() != t.Err.Error() {
			eG := g.ErrorGroup{}
			eG.Add(err)
			eG.Add(t.Err)
			t.Err = g.Error(eG.Err(), "execution failed")
		} else {
			t.Err = g.Error(t.Err, "execution failed")
		}
	}

	now2 := time.Now()
	t.EndTime = &now2

	// show help text
	if eh := ErrorHelper(t.Err); eh != "" && !t.Config.ReplicationMode {
		env.Println("")
		env.Println(env.MagentaString(eh))
		env.Println("")
	}

	// update into store
	StoreUpdate(t)

	return t.Err
}

func (t *TaskExecution) getSrcDBConn(ctx context.Context) (conn database.Connection, err error) {

	// sets metadata
	metadata := t.setGetMetadata()

	options := t.sourceOptionsMap()
	options["METADATA"] = g.Marshal(metadata)

	srcProps := append(
		g.MapToKVArr(t.Config.SrcConn.DataS()),
		g.MapToKVArr(g.ToMapString(options))...,
	)

	// look for conn in cache
	if conn, ok := connPool[t.Config.SrcConn.Hash()]; ok {
		return conn, nil
	}

	conn, err = database.NewConnContext(ctx, t.Config.SrcConn.URL(), srcProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	// cache connection is using replication from CLI
	if t.isUsingPool() {
		connPool[t.Config.SrcConn.Hash()] = conn
	}

	// set read_only if sqlite / duckdb since it's a source
	if g.In(conn.GetType(), dbio.TypeDbSQLite, dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck) {
		conn.SetProp("read_only", "true")
	}

	return
}

func (t *TaskExecution) getTgtDBConn(ctx context.Context) (conn database.Connection, err error) {
	// look for conn in cache
	if conn, ok := connPool[t.Config.TgtConn.Hash()]; ok {
		return conn, nil
	}

	options := g.M()
	g.Unmarshal(g.Marshal(t.Config.Target.Options), &options)
	tgtProps := append(
		g.MapToKVArr(t.Config.TgtConn.DataS()), g.MapToKVArr(g.ToMapString(options))...,
	)

	// Connection context should be different than task context
	conn, err = database.NewConnContext(ctx, t.Config.TgtConn.URL(), tgtProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	// cache connection is using replication from CLI
	if t.isUsingPool() {
		connPool[t.Config.TgtConn.Hash()] = conn
	}

	// set bulk
	if val := t.Config.Target.Options.UseBulk; val != nil && !*val {
		conn.SetProp("use_bulk", "false")
		conn.SetProp("allow_bulk_import", "false")
	}
	return
}

func (t *TaskExecution) runDbSQL() (err error) {

	start = time.Now()

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		defer tgtConn.Close()
	}

	t.SetProgress("executing sql on target database")
	result, err := tgtConn.Exec(t.Config.Target.Object)
	if err != nil {
		err = g.Error(err, "Could not complete sql execution on %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	rowAffCnt, err := result.RowsAffected()
	if err == nil {
		t.SetProgress("%d rows affected", rowAffCnt)
	}

	return
}

func (t *TaskExecution) runDbToFile() (err error) {

	start = time.Now()

	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	t.SetProgress("connecting to source database (%s)", srcConn.GetType())
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.SrcConn.Info().Name, srcConn.GetType())
		return
	}

	if !t.isUsingPool() {
		defer srcConn.Close()
	}

	t.SetProgress("reading from source database")
	defer t.Cleanup()
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	t.SetProgress("wrote %d rows [%s r/s] to %s", cnt, getRate(cnt), t.getTargetObjectValue())

	err = t.df.Err()
	return

}

func (t *TaskExecution) runFolderToDB() (err error) {
	/*
		This will take a URL as a folder path
		1. list the files/folders in it (not recursive)
		2a. run runFileToDB for each of the files, naming the target table respectively
		2b. OR run runFileToDB for each of the files, to the same target able, assume each file has same structure
		3. keep list of file inserted in Job.Settings (view handleExecutionHeartbeat in server_ws.go).

	*/
	return
}

func (t *TaskExecution) runFileToDB() (err error) {

	start = time.Now()

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	if t.usingCheckpoint() {
		t.SetProgress("getting checkpoint value")
		if t.Config.Source.UpdateKey == "." {
			t.Config.Source.UpdateKey = slingLoadedAtColumn
		}
		varMap := map[string]string{} // should always be number
		t.Config.IncrementalVal, err = getIncrementalValue(t.Config, tgtConn, varMap)
		if err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
	}

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.usingCheckpoint() && t.Config.IncrementalVal != "" {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalVal), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "could not read from file")
		return
	}
	defer t.df.Close()

	// set schema if needed
	t.Config.Target.Object = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Object)
	t.Config.Target.Options.TableTmp = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Options.TableTmp)

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "could not write to database")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s]", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt))

	if err != nil {
		err = g.Error(t.df.Err(), "error in transfer")
	}
	return
}

func (t *TaskExecution) runFileToFile() (err error) {

	start = time.Now()

	if t.Config.Options.StdIn && t.Config.SrcConn.Type.IsUnknown() {
		t.SetProgress("reading from stream (stdin)")
	} else {
		t.SetProgress("reading from source file system (%s)", t.Config.SrcConn.Type)
	}
	t.df, err = t.ReadFromFile(t.Config)
	if err != nil {
		if strings.Contains(err.Error(), "Provided 0 files") {
			if t.usingCheckpoint() && t.Config.IncrementalVal != "" {
				t.SetProgress("no new files found since latest timestamp (%s)", time.Unix(cast.ToInt64(t.Config.IncrementalVal), 0))
			} else {
				t.SetProgress("no files found")
			}
			return nil
		} else if len(t.df.Streams) == 1 && t.df.Streams[0].IsClosed() {
			return nil
		}
		err = g.Error(err, "Could not ReadFromFile")
		return
	}
	defer t.df.Close()

	if t.Config.Options.StdOut {
		t.SetProgress("writing to target stream (stdout)")
	} else {
		t.SetProgress("writing to target file system (%s)", t.Config.TgtConn.Type)
	}
	defer t.Cleanup()
	cnt, err := t.WriteToFile(t.Config, t.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	t.SetProgress("wrote %d rows to %s [%s r/s]", cnt, t.getTargetObjectValue(), getRate(cnt))

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error in runFileToFile")
	}
	return
}

func (t *TaskExecution) runDbToDb() (err error) {
	start = time.Now()
	if t.Config.Mode == Mode("") {
		t.Config.Mode = FullRefreshMode
	}

	// Initiate connections
	srcConn, err := t.getSrcDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	tgtConn, err := t.getTgtDBConn(t.Context.Ctx)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	t.SetProgress("connecting to source database (%s)", srcConn.GetType())
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.SrcConn.Info().Name, srcConn.GetType())
		return
	}

	t.SetProgress("connecting to target database (%s)", tgtConn.GetType())
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", t.Config.TgtConn.Info().Name, tgtConn.GetType())
		return
	}

	if !t.isUsingPool() {
		t.AddCleanupTaskLast(func() { srcConn.Close() })
		t.AddCleanupTaskLast(func() { tgtConn.Close() })
	}

	// set schema if needed
	t.Config.Target.Object = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Object)
	t.Config.Target.Options.TableTmp = setSchema(cast.ToString(t.Config.Target.Data["schema"]), t.Config.Target.Options.TableTmp)

	// check if table exists by getting target columns
	pullTargetTableColumns(t.Config, tgtConn, false)

	// get watermark
	if t.usingCheckpoint() {
		t.SetProgress("getting checkpoint value")
		t.Config.IncrementalVal, err = getIncrementalValue(t.Config, tgtConn, srcConn.Template().Variable)
		if err != nil {
			err = g.Error(err, "Could not get incremental value")
			return err
		}
	}

	t.SetProgress("reading from source database")
	t.df, err = t.ReadFromDB(t.Config, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer t.df.Close()

	// to DirectLoad if possible
	if t.df.FsURL != "" {
		data := g.M("url", t.df.FsURL)
		for k, v := range srcConn.Props() {
			data[k] = v
		}
		t.Config.Source.Data["SOURCE_FILE"] = g.M("data", data)
	}

	t.SetProgress("writing to target database [mode: %s]", t.Config.Mode)
	defer t.Cleanup()
	cnt, err := t.WriteToDb(t.Config, t.df, tgtConn)
	if err != nil {
		err = g.Error(err, "Could not WriteToDb")
		return
	}

	bytesStr := ""
	if val := t.GetBytesString(); val != "" {
		bytesStr = "[" + val + "]"
	}
	elapsed := int(time.Since(start).Seconds())
	t.SetProgress("inserted %d rows into %s in %d secs [%s r/s] %s", cnt, t.getTargetObjectValue(), elapsed, getRate(cnt), bytesStr)

	if t.df.Err() != nil {
		err = g.Error(t.df.Err(), "Error running runDbToDb")
	}
	return
}
