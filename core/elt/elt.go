package elt

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"github.com/flarco/dbio/filesys"

	"github.com/flarco/dbio"

	"github.com/dustin/go-humanize"
	"github.com/slingdata-io/sling/core/env"

	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling/core/dbt"
	"github.com/spf13/cast"
)

// AllowedProps allowed properties
var AllowedProps = map[string]string{
	"sheet": "Provided for Excel source files. Default is first sheet",
	"range": "Optional for Excel source file. Default is largest table range",
}

var start time.Time
var PermitTableSchemaOptimization = false

// IsStalled determines if the task has stalled (no row increment)
func (j *Task) IsStalled(window float64) bool {
	if strings.Contains(j.Progress, "pre-sql") || strings.Contains(j.Progress, "post-sql") {
		return false
	}
	return time.Since(j.lastIncrement).Seconds() > window
}

// GetCount return the current count of rows processed
func (j *Task) GetCount() (count uint64) {
	if j.StartTime == nil {
		return
	}

	return j.df.Count()
}

// GetRate return the speed of flow (rows / sec)
// secWindow is how many seconds back to measure (0 is since beginning)
func (j *Task) GetRate(secWindow int) (rate int) {
	var secElapsed float64
	if j.StartTime == nil || j.StartTime.IsZero() {
		return
	} else if j.EndTime == nil || j.EndTime.IsZero() {
		st := *j.StartTime
		if secWindow <= 0 {
			secElapsed = time.Since(st).Seconds()
			rate = cast.ToInt(math.Round(cast.ToFloat64(j.df.Count()) / secElapsed))
		} else {
			rate = cast.ToInt(math.Round(cast.ToFloat64((j.df.Count() - j.prevCount) / cast.ToUint64(secWindow))))
			if j.prevCount < j.df.Count() {
				j.lastIncrement = time.Now()
			}
			j.prevCount = j.df.Count()
		}
	} else {
		st := *j.StartTime
		et := *j.EndTime
		secElapsed = cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
		rate = cast.ToInt(math.Round(cast.ToFloat64(j.df.Count()) / secElapsed))
	}
	return
}

// Execute runs a Sling task.
// This may be a file/db to file/db transfer
func (j *Task) Execute() error {
	env.InitLogger()

	done := make(chan struct{})
	now := time.Now()
	j.StartTime = &now
	j.lastIncrement = now

	if j.Ctx == nil {
		j.Ctx = context.Background()
	}

	go func() {
		defer close(done)
		j.Status = ExecStatusRunning

		if j.Err != nil {
			return
		} else if j.Type == DbSQL {
			j.Err = j.runDbSQL()
		} else if j.Type == DbDbt {
			j.Err = j.runDbDbt()
		} else if j.Type == FileToDB {
			j.Err = j.runFileToDB()
		} else if j.Type == DbToDb {
			j.Err = j.runDbToDb()
		} else if j.Type == DbToFile {
			j.Err = j.runDbToFile()
		} else if j.Type == FileToFile {
			j.Err = j.runFileToFile()
		} else {
			j.SetProgress("task execution configuration is invalid")
			j.Err = fmt.Errorf("Cannot Execute. Task Type is not specified")
		}
	}()

	select {
	case <-done:
	case <-j.Ctx.Done():
		// need to add cancelling mechanism here
		// if context is cancelled, need to cleanly cancel task
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
		if j.Err == nil {
			j.Err = fmt.Errorf("Execution interrupted")
		}
	}

	if j.Err == nil {
		j.SetProgress("execution succeeded")
		j.Status = ExecStatusSuccess
	} else {
		j.SetProgress("execution failed")
		j.Status = ExecStatusError
	}

	now2 := time.Now()
	j.EndTime = &now2

	return j.Err
}

func (j *Task) runDbSQL() (err error) {

	start = time.Now()

	j.SetProgress("connecting to target database")
	tgtProps := g.MapToKVArr(j.Cfg.TgtConn.DataS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer tgtConn.Close()

	j.SetProgress("executing sql on target database")
	result, err := tgtConn.ExecContext(j.Ctx, j.Cfg.Target.PostSQL)
	if err != nil {
		err = g.Error(err, "Could not complete sql execution on %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	rowAffCnt, err := result.RowsAffected()
	if err == nil {
		j.SetProgress("%d rows affected", rowAffCnt)
	}

	return
}

func (j *Task) runDbDbt() (err error) {
	dbtConfig := g.Marshal(j.Cfg.Target.Dbt)
	dbtObj, err := dbt.NewDbt(dbtConfig)
	if err != nil {
		return g.Error(err, "could not init dbt task")
	}

	dbtObj.Session.SetScanner(func(stderr bool, text string) {
		switch stderr {
		case true, false:
			j.SetProgress(text)
		}
	})

	if dbtObj.Profile == "" {
		dbtObj.Profile = j.Cfg.TgtConn.ID
	}

	err = dbtObj.Init(j.Cfg.TgtConn)
	if err != nil {
		return g.Error(err, "could not initialize dbt project")
	}

	err = dbtObj.Run()
	if err != nil {
		err = g.Error(err, "could not run dbt task")
	}

	return
}

func (j *Task) runDbToFile() (err error) {

	start = time.Now()
	srcProps := g.MapToKVArr(j.Cfg.SrcConn.DataS())
	srcConn, err := database.NewConnContext(j.Ctx, j.Cfg.SrcConn.URL, srcProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	j.SetProgress("connecting to source database")
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", j.Cfg.SrcConn.ID, srcConn.GetType())
		return
	}

	defer srcConn.Close()

	j.SetProgress("reading from source database")
	j.df, err = j.ReadFromDB(&j.Cfg, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target file system")
	cnt, err := j.WriteToFile(&j.Cfg, j.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	j.SetProgress("wrote %d rows [%s r/s]", cnt, getRate(cnt))

	err = j.df.Context.Err()
	return

}

func (j *Task) runFileToDB() (err error) {

	start = time.Now()

	j.SetProgress("connecting to target database")
	tgtProps := g.MapToKVArr(j.Cfg.TgtConn.DataS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer tgtConn.Close()

	j.SetProgress("reading from source file system")
	j.df, err = j.ReadFromFile(&j.Cfg)
	if err != nil {
		err = g.Error(err, "could not read from file")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target database")
	cnt, err := j.WriteToDb(&j.Cfg, j.df, tgtConn)
	if err != nil {
		err = g.Error(err, "could not write to database")
		if j.Cfg.Target.TmpTableCreated {
			// need to drop residue
			tgtConn.DropTable(j.Cfg.Target.TableTmp)
		}
		return
	}

	elapsed := int(time.Since(start).Seconds())
	j.SetProgress("inserted %d rows in %d secs [%s r/s]", cnt, elapsed, getRate(cnt))

	if err != nil {
		err = g.Error(j.df.Err(), "error in transfer")
	}
	return
}

func (j *Task) runFileToFile() (err error) {

	start = time.Now()

	j.SetProgress("reading from source file system")
	j.df, err = j.ReadFromFile(&j.Cfg)
	if err != nil {
		err = g.Error(err, "Could not ReadFromFile")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target file system")
	cnt, err := j.WriteToFile(&j.Cfg, j.df)
	if err != nil {
		err = g.Error(err, "Could not WriteToFile")
		return
	}

	j.SetProgress("wrote %d rows [%s r/s]", cnt, getRate(cnt))

	if j.df.Err() != nil {
		err = g.Error(j.df.Err(), "Error in runFileToFile")
	}
	return
}

func (j *Task) runDbToDb() (err error) {
	start = time.Now()
	if j.Cfg.Target.Mode == Mode("") {
		j.Cfg.Target.Mode = AppendMode
	}

	// Initiate connections
	srcProps := g.MapToKVArr(j.Cfg.SrcConn.DataS())
	srcConn, err := database.NewConnContext(j.Ctx, j.Cfg.SrcConn.URL, srcProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize source connection")
		return
	}

	tgtProps := g.MapToKVArr(j.Cfg.TgtConn.DataS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = g.Error(err, "Could not initialize target connection")
		return
	}

	j.SetProgress("connecting to source database")
	err = srcConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", j.Cfg.SrcConn.ID, srcConn.GetType())
		return
	}

	j.SetProgress("connecting to target database")
	err = tgtConn.Connect()
	if err != nil {
		err = g.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer srcConn.Close()
	defer tgtConn.Close()

	// check if table exists by getting target columns
	j.Cfg.Target.Columns, _ = tgtConn.GetSQLColumns("select * from " + j.Cfg.Target.Table)

	if j.Cfg.Target.Mode == "upsert" {
		j.SetProgress("getting checkpoint value")
		j.Cfg.UpsertVal, err = getUpsertValue(&j.Cfg, tgtConn, srcConn.Template().Variable)
		if err != nil {
			err = g.Error(err, "Could not getUpsertValue")
			return err
		}
	}

	j.SetProgress("reading from source database")
	j.df, err = j.ReadFromDB(&j.Cfg, srcConn)
	if err != nil {
		err = g.Error(err, "Could not ReadFromDB")
		return
	}
	defer j.df.Close()

	// to DirectLoad if possible
	vars := map[string]interface{}{}
	for k, v := range g.KVArrToMap(srcConn.PropArr()...) {
		vars[k] = v
	}
	j.Cfg.Source.Data["SOURCE_FILE"] = g.Map{
		"url":  j.df.FsURL,
		"vars": vars,
	}

	j.SetProgress("writing to target database")
	cnt, err := j.WriteToDb(&j.Cfg, j.df, tgtConn)
	if err != nil {
		err = g.Error(err, "Could not WriteToDb")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	j.SetProgress("inserted %d rows in %d secs [%s r/s]", cnt, elapsed, getRate(cnt))

	if j.df.Context.Err() != nil {
		err = g.Error(j.df.Context.Err(), "Error running runDbToDb")
	}
	return
}

// ReadFromDB reads from a source database
func (j *Task) ReadFromDB(cfg *Config, srcConn database.Connection) (df *iop.Dataflow, err error) {

	fieldsStr := "*"
	sql := g.F(`select %s from %s`, fieldsStr, cfg.Source.Table)

	if cfg.Source.SQL != "" {
		// for upsert, need to put `{upsert_where_cond}` for proper selecting
		sql, err = GetSQLText(cfg.Source.SQL)
		if err != nil {
			err = g.Error(err, "Could not get GetSQLText for: "+cfg.Source.SQL)
			return
		}
	} else if cfg.Target.Mode != "drop" && len(j.Cfg.Target.Columns) > 0 {
		// since we are not dropping and table exists, we need to only select the matched columns
		srcData, _ := srcConn.Query(g.F("select * from %s where 1=0", cfg.Source.Table))

		if len(srcData.Columns) > 0 {
			commFields := database.CommonColumns(
				database.ColumnNames(srcData.Columns),
				database.ColumnNames(j.Cfg.Target.Columns),
			)
			if len(commFields) == 0 {
				err = g.Error("src table and tgt table have no columns with same names. Column names must match")
				return
			}
			fieldsStr = strings.Join(commFields, ", ")
		}
	}

	// Get source columns
	cfg.Source.Columns, err = srcConn.GetSQLColumns(g.R(sql, "upsert_where_cond", "1=0"))
	if err != nil {
		err = g.Error(err, "Could not obtain source columns")
		return
	}

	if cfg.Target.Mode == "upsert" {
		// select only records that have been modified after last max value
		upsertWhereCond := "1=1"
		if cfg.UpsertVal != "" {
			upsertWhereCond = g.R(
				"{update_key} >= {value}",
				"update_key", srcConn.Quote(cfg.Source.Columns.Normalize(cfg.Target.UpdateKey)),
				"value", cfg.UpsertVal,
			)
		}

		if cfg.Source.SQL != "" {
			if !strings.Contains(sql, "{upsert_where_cond}") {
				err = g.Error("For upsert loading with custom SQL, need to include where clause placeholder {upsert_where_cond}. e.g: select * from my_table where col2='A' AND {upsert_where_cond}")
				return
			}
			sql = g.R(sql, "upsert_where_cond", upsertWhereCond)
		} else {
			sql = g.R(
				`select {fields} from {table} where {upsert_where_cond}`,
				"fields", fieldsStr,
				"table", cfg.Source.Table,
				"upsert_where_cond", upsertWhereCond,
			)
		}
	} else if cfg.Source.Limit > 0 {
		sql = g.R(
			srcConn.Template().Core["limit"],
			"fields", fieldsStr,
			"table", cfg.Source.Table,
			"limit", cast.ToString(cfg.Source.Limit),
		)
	}

	df, err = srcConn.BulkExportFlow(sql)
	if err != nil {
		err = g.Error(err, "Could not BulkStream: "+sql)
		return
	}

	return
}

// ReadFromFile reads from a source file
func (j *Task) ReadFromFile(cfg *Config) (df *iop.Dataflow, err error) {

	var stream *iop.Datastream

	if cfg.SrcConn.URL != "" {
		fs, err := filesys.NewFileSysClientFromURLContext(j.Ctx, cfg.SrcConn.URL, g.MapToKVArr(cfg.SrcConn.DataS())...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for: "+cfg.SrcConn.URL)
			return df, err
		}

		df, err = fs.ReadDataflow(cfg.SrcConn.URL)
		if err != nil {
			err = g.Error(err, "Could not FileSysReadDataflow for: "+cfg.SrcConn.URL)
			return df, err
		}
	} else {
		stream, err = filesys.MakeDatastream(bufio.NewReader(os.Stdin))
		if err != nil {
			err = g.Error(err, "Could not MakeDatastream")
			return
		}
		df, err = iop.MakeDataFlow(stream)
		if err != nil {
			err = g.Error(err, "Could not MakeDataFlow for Stdin")
			return
		}
	}

	return
}

// WriteToFile writes to a target file
func (j *Task) WriteToFile(cfg *Config, df *iop.Dataflow) (cnt uint64, err error) {
	var stream *iop.Datastream
	var bw int64

	if cfg.TgtConn.URL != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtConn.URL = g.Rm(cfg.TgtConn.URL, dateMap)
		fs, err := filesys.NewFileSysClientFromURLContext(j.Ctx, cfg.TgtConn.URL, g.MapToKVArr(cfg.TgtConn.DataS())...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for: "+cfg.TgtConn.URL)
			return cnt, err
		}

		bw, err = fs.WriteDataflow(df, cfg.TgtConn.URL)
		if err != nil {
			err = g.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.StdOut {
		stream = iop.MergeDataflow(df)
		reader := stream.NewCsvReader(0)
		bufStdout := bufio.NewWriter(os.Stdout)
		defer bufStdout.Flush()
		bw, err = filesys.Write(reader, bufStdout)
		if err != nil {
			err = g.Error(err, "Could not write to Stdout")
			return
		}
		cnt = stream.Count
	} else {
		err = g.Error("target for output is not specified")
		return
	}

	g.Debug(
		"wrote %s: %d rows [%s r/s]",
		humanize.Bytes(cast.ToUint64(bw)), cnt, getRate(cnt),
	)

	return
}

// WriteToDb writes to a target DB
// create temp table
// load into temp table
// insert / upsert / replace into target table
func (j *Task) WriteToDb(cfg *Config, df *iop.Dataflow, tgtConn database.Connection) (cnt uint64, err error) {
	if cfg.Target.TableTmp == "" {
		cfg.Target.TableTmp = cfg.Target.Table + "_tmp" + g.RandString(g.NumericRunes, 1) + strings.ToLower(g.RandString(g.AplhanumericRunes, 1))
	}
	if cfg.Target.Mode == "" {
		cfg.Target.Mode = "append"
	}

	// pre SQL
	if cfg.Target.PreSQL != "" {
		j.SetProgress("executing pre-sql")
		sql, err := GetSQLText(cfg.Target.PreSQL)
		if err != nil {
			err = g.Error(err, "could not get pre-sql body")
			return cnt, err
		}
		_, err = tgtConn.Exec(sql)
		if err != nil {
			err = g.Error(err, "could not execute pre-sql on target")
			return cnt, err
		}
	}

	// Drop & Create the temp table
	err = tgtConn.DropTable(cfg.Target.TableTmp)
	if err != nil {
		err = g.Error(err, "could not drop table "+cfg.Target.TableTmp)
		return
	}
	sampleData := iop.NewDataset(df.Columns)
	sampleData.Rows = df.Buffer
	sampleData.SafeInference = true
	_, err = createTableIfNotExists(tgtConn, sampleData, cfg.Target.TableTmp, "")
	if err != nil {
		err = g.Error(err, "could not create temp table "+cfg.Target.TableTmp)
		return
	}
	cfg.Target.TmpTableCreated = true
	defer tgtConn.DropTable(cfg.Target.TableTmp)

	// TODO: if srcFile is from a cloud storage, and targetDB
	// supports direct loading (RedShift, Snowflake or Azure)
	// do direct loading (without passing through our box)
	// risk is potential data loss, since we cannot validate counts
	srcFile := &dbio.DataConn{}
	if sf, ok := j.Cfg.Source.Data["SOURCE_FILE"]; ok {
		srcFile = dbio.NewDataConnFromMap(sf.(g.Map))
	}
	cnt, ok, err := tgtConn.CopyDirect(cfg.Target.TableTmp, *srcFile)
	if ok {
		df.SetEmpty() // this executes deferred functions (such as file residue removal
		if err != nil {
			err = g.Error(err, "could not directly load into database")
			return
		}
		g.Debug("copied directly from cloud storage")
		cnt, _ = tgtConn.GetCount(cfg.Target.TableTmp)
		// perhaps do full analysis to validate quality
	} else {
		j.SetProgress("streaming inserts")
		cnt, err = tgtConn.BulkImportFlow(cfg.Target.TableTmp, df)
		if err != nil {
			err = g.Error(err, "could not insert into "+cfg.Target.Table)
			return
		}
		tCnt, _ := tgtConn.GetCount(cfg.Target.TableTmp)
		if cnt != tCnt {
			err = g.Error("inserted in temp table but table count (%d) != stream count (%d). Records missing. Aborting", tCnt, cnt)
			return
		}
		// aggregate stats from stream processors
		df.SyncStats()

		// Checksum Comparison, data quality
		err = tgtConn.CompareChecksums(cfg.Target.TableTmp, df.Columns)
		if err != nil {
			g.Debug(err.Error())
		}
	}

	// need to contain the final write in a transcation
	err = tgtConn.Begin()
	if err != nil {
		err = g.Error(err, "could not opne transcation to write to final table")
		return
	}

	defer tgtConn.Rollback() // rollback in case of error

	if cnt > 0 {
		if cfg.Target.Mode == "drop" {
			// drop, (create if not exists) and insert directly
			err = tgtConn.DropTable(cfg.Target.Table)
			if err != nil {
				err = g.Error(err, "could not drop table "+cfg.Target.Table)
				return
			}
			j.SetProgress("dropped table " + cfg.Target.Table)
		}

		// create table if not exists
		sample := iop.NewDataset(df.Columns)
		sample.Rows = df.Buffer
		sample.Inferred = true // already inferred with SyncStats
		created, err := createTableIfNotExists(
			tgtConn,
			sample,
			cfg.Target.Table,
			cfg.Target.TableDDL,
		)
		if err != nil {
			err = g.Error(err, "could not create table "+cfg.Target.Table)
			return cnt, err
		} else if created {
			j.SetProgress("created table %s", cfg.Target.Table)
		}

		// TODO: put corrective action here, based on StreamProcessor's result
		// --> use StreamStats to generate new DDL, create the target
		// table with the new DDL, then insert into.
		// IF! Target table exists, and the DDL is insufficient, then
		// have setting called PermitTableSchemaOptimization, which
		// allows sling elt to alter the final table to fit the data
		// if table doesn't exists, then easy-peasy, create it.
		// change logic in createTableIfNotExists ot use StreamStats
		// OptimizeTable creates the table if it's missing
		// **Hole in this**: will truncate data points, since it is based
		// only on new data being inserted... would need a complete
		// stats of the target table to properly optimize.
		if !created && PermitTableSchemaOptimization {
			err = tgtConn.OptimizeTable(cfg.Target.Table, df.Columns)
			if err != nil {
				err = g.Error(err, "could not optimize table schema")
				return cnt, err
			}
		}
	}

	// Put data from tmp to final
	if cnt == 0 {
		j.SetProgress("0 rows inserted. Nothing to do.")
	} else if cfg.Target.Mode == "drop (need to optimize temp table in place)" {
		// use swap
		err = tgtConn.SwapTable(cfg.Target.TableTmp, cfg.Target.Table)
		if err != nil {
			err = g.Error(err, "could not swap tables %s to %s", cfg.Target.TableTmp, cfg.Target.Table)
			return 0, err
		}

		err = tgtConn.DropTable(cfg.Target.TableTmp)
		if err != nil {
			err = g.Error(err, "could not drop table "+cfg.Target.TableTmp)
			return
		}
		j.SetProgress("dropped old table of " + cfg.Target.Table)

	} else if cfg.Target.Mode == "append" || cfg.Target.Mode == "drop" {
		// create if not exists and insert directly
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			return 0, err
		}
	} else if cfg.Target.Mode == "truncate" {
		// truncate (create if not exists) and insert directly
		truncSQL := g.R(
			tgtConn.GetTemplateValue("core.truncate_table"),
			"table", cfg.Target.Table,
		)
		_, err = tgtConn.Exec(truncSQL)
		if err != nil {
			err = g.Error(err, "Could not truncate table: "+cfg.Target.Table)
			return
		}
		j.SetProgress("truncated table " + cfg.Target.Table)

		// insert
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
	} else if cfg.Target.Mode == "upsert" {
		// insert in temp
		// create final if not exists
		// delete from final and insert
		// or update (such as merge or ON CONFLICT)
		rowAffCnt, err := tgtConn.Upsert(cfg.Target.TableTmp, cfg.Target.Table, cfg.Target.PrimaryKey)
		if err != nil {
			err = g.Error(err, "Could not upsert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
		j.SetProgress("%d INSERTS / UPDATES", rowAffCnt)
	}

	// post SQL
	if cfg.Target.PostSQL != "" {
		j.SetProgress("executing post-sql")
		sql, err := GetSQLText(cfg.Target.PostSQL)
		if err != nil {
			err = g.Error(err, "Error executing Target.PostSQL. Could not get GetSQLText for: "+cfg.Target.PostSQL)
			return cnt, err
		}
		_, err = tgtConn.Exec(sql)
		if err != nil {
			err = g.Error(err, "Error executing Target.PostSQL")
			return cnt, err
		}
	}

	err = tgtConn.Commit()
	if err != nil {
		err = g.Error(err, "could not commit")
		return cnt, err
	}

	err = df.Context.Err()
	return
}

func createTableIfNotExists(conn database.Connection, data iop.Dataset, tableName string, tableDDL string) (created bool, err error) {

	// check table existence
	exists, err := conn.TableExists(tableName)
	if err != nil {
		return false, g.Error(err, "Error checking table "+tableName)
	} else if exists {
		return false, nil
	}

	if tableDDL == "" {
		tableDDL, err = conn.GenerateDDL(tableName, data)
		if err != nil {
			return false, g.Error(err, "Could not generate DDL for "+tableName)
		}
	}

	_, err = conn.Exec(tableDDL)
	if err != nil {
		errorFilterTableExists := conn.GetTemplateValue("variable.error_filter_table_exists")
		if errorFilterTableExists != "" && strings.Contains(err.Error(), errorFilterTableExists) {
			return false, g.Error(err, "Error creating table %s as it already exists", tableName)
		}
		return false, g.Error(err, "Error creating table "+tableName)
	}

	return true, nil
}

func insertFromTemp(cfg *Config, tgtConn database.Connection) (err error) {
	// insert
	tmpColumns, err := tgtConn.GetColumns(cfg.Target.TableTmp)
	if err != nil {
		err = g.Error(err, "could not get column list for "+cfg.Target.TableTmp)
		return
	}
	tgtColumns, err := tgtConn.GetColumns(cfg.Target.Table)
	if err != nil {
		err = g.Error(err, "could not get column list for "+cfg.Target.Table)
		return
	}

	// if tmpColumns are dummy fields, simply match the target column names
	if iop.IsDummy(tmpColumns) && len(tmpColumns) == len(tgtColumns) {
		for i, col := range tgtColumns {
			tmpColumns[i].Name = col.Name
		}
	}

	// TODO: need to validate the source table types are casted
	// into the target column type
	tgtFields, err := tgtConn.ValidateColumnNames(
		database.ColumnNames(tgtColumns),
		database.ColumnNames(tmpColumns),
		true,
	)
	if err != nil {
		err = g.Error(err, "columns mismatched")
		return
	}

	srcFields := tgtConn.CastColumnsForSelect(tmpColumns, tgtColumns)

	sql := g.R(
		tgtConn.Template().Core["insert_from_table"],
		"tgt_table", cfg.Target.Table,
		"src_table", cfg.Target.TableTmp,
		"tgt_fields", strings.Join(tgtFields, ", "),
		"src_fields", strings.Join(srcFields, ", "),
	)
	_, err = tgtConn.Exec(sql)
	if err != nil {
		err = g.Error(err, "Could not execute SQL: "+sql)
		return
	}
	g.Debug("inserted rows into `%s` from temp table `%s`", cfg.Target.Table, cfg.Target.TableTmp)
	return
}

func getUpsertValue(cfg *Config, tgtConn database.Connection, srcConnVarMap map[string]string) (val string, err error) {
	// get table columns type for table creation if not exists
	// in order to get max value
	// does table exists?
	// get max value from key_field
	sql := g.F(
		"select max(%s) as max_val from %s",
		cfg.Target.UpdateKey,
		cfg.Target.Table,
	)

	data, err := tgtConn.Query(sql)
	if err != nil {
		if strings.Contains(err.Error(), "exist") {
			// table does not exists, will be create later
			// set val to blank for full load
			return "", nil
		}
		err = g.Error(err, "could not get max value for "+cfg.Target.UpdateKey)
		return
	}
	if len(data.Rows) == 0 {
		// table is empty
		// set val to blank for full load
		return "", nil
	}

	value := data.Rows[0][0]
	colType := data.Columns[0].Type
	if strings.Contains("datetime,date,timestamp", colType) {
		val = g.R(
			srcConnVarMap["timestamp_layout_str"],
			"value", cast.ToTime(value).Format(srcConnVarMap["timestamp_layout"]),
		)
	} else if strings.Contains("datetime,date", colType) {
		val = g.R(
			srcConnVarMap["date_layout_str"],
			"value", cast.ToTime(value).Format(srcConnVarMap["date_layout"]),
		)
	} else if strings.Contains("integer,bigint,decimal", colType) {
		val = cast.ToString(value)
	} else {
		val = strings.ReplaceAll(cast.ToString(value), `'`, `''`)
		val = `'` + val + `'`
	}

	return
}

func getRate(cnt uint64) string {
	return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
}

// GetSQLText process source sql file / text
func GetSQLText(sqlStr string) (string, error) {
	sql := sqlStr

	_, err := os.Stat(sqlStr)
	if err == nil {
		bytes, err := ioutil.ReadFile(sqlStr)
		if err != nil {
			return "", g.Error(err, "Could not ReadFile: "+sqlStr)
		}
		sql = string(bytes)
	}

	return sql, nil
}
