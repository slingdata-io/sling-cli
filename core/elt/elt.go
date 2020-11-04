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

	"github.com/dustin/go-humanize"
	"github.com/slingdata-io/sling/core/env"

	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/database"
	"github.com/slingdata-io/sling/core/dbt"
	"github.com/slingdata-io/sling/core/iop"
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
	tgtProps := h.MapToKVArr(j.Cfg.TgtConn.VarsS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = h.Error(err, "Could not initialize target connection")
		return
	}

	err = tgtConn.Connect()
	if err != nil {
		err = h.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer tgtConn.Close()

	j.SetProgress("executing sql on target database")
	result, err := tgtConn.ExecContext(j.Ctx, j.Cfg.TgtPostSQL)
	if err != nil {
		err = h.Error(err, "Could not complete sql execution on %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	rowAffCnt, err := result.RowsAffected()
	if err == nil {
		j.SetProgress("%d rows affected", rowAffCnt)
	}

	return
}

func (j *Task) runDbDbt() (err error) {
	dbtConfig := h.Marshal(j.Cfg.TgtPostDbt)
	dbtObj, err := dbt.NewDbt(dbtConfig)
	if err != nil {
		return h.Error(err, "could not init dbt task")
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
		return h.Error(err, "could not initialize dbt project")
	}

	err = dbtObj.Run()
	if err != nil {
		err = h.Error(err, "could not run dbt task")
	}

	return
}

func (j *Task) runDbToFile() (err error) {

	start = time.Now()
	srcProps := h.MapToKVArr(j.Cfg.SrcConn.VarsS())
	srcConn, err := database.NewConnContext(j.Ctx, j.Cfg.SrcConn.URL, srcProps...)
	if err != nil {
		err = h.Error(err, "Could not initialize source connection")
		return
	}

	j.SetProgress("connecting to source database")
	err = srcConn.Connect()
	if err != nil {
		err = h.Error(err, "Could not connect to: %s (%s)", j.Cfg.SrcConn.ID, srcConn.GetType())
		return
	}

	defer srcConn.Close()

	j.SetProgress("reading from source database")
	j.df, err = j.ReadFromDB(&j.Cfg, srcConn)
	if err != nil {
		err = h.Error(err, "Could not ReadFromDB")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target file system")
	cnt, err := j.WriteToFile(&j.Cfg, j.df)
	if err != nil {
		err = h.Error(err, "Could not WriteToFile")
		return
	}

	j.SetProgress("wrote %d rows [%s r/s]", cnt, getRate(cnt))

	err = j.df.Context.Err()
	return

}

func (j *Task) runFileToDB() (err error) {

	start = time.Now()

	j.SetProgress("connecting to target database")
	tgtProps := h.MapToKVArr(j.Cfg.TgtConn.VarsS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = h.Error(err, "Could not initialize target connection")
		return
	}

	err = tgtConn.Connect()
	if err != nil {
		err = h.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer tgtConn.Close()

	j.SetProgress("reading from source file system")
	j.df, err = j.ReadFromFile(&j.Cfg)
	if err != nil {
		err = h.Error(err, "could not read from file")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target database")
	cnt, err := j.WriteToDb(&j.Cfg, j.df, tgtConn)
	if err != nil {
		err = h.Error(err, "could not write to database")
		if j.Cfg.TmpTableCreated {
			// need to drop residue
			tgtConn.DropTable(j.Cfg.TgtTableTmp)
		}
		return
	}

	elapsed := int(time.Since(start).Seconds())
	j.SetProgress("inserted %d rows in %d secs [%s r/s]", cnt, elapsed, getRate(cnt))

	if err != nil {
		err = h.Error(j.df.Err(), "error in transfer")
	}
	return
}

func (j *Task) runFileToFile() (err error) {

	start = time.Now()

	j.SetProgress("reading from source file system")
	j.df, err = j.ReadFromFile(&j.Cfg)
	if err != nil {
		err = h.Error(err, "Could not ReadFromFile")
		return
	}
	defer j.df.Close()

	j.SetProgress("writing to target file system")
	cnt, err := j.WriteToFile(&j.Cfg, j.df)
	if err != nil {
		err = h.Error(err, "Could not WriteToFile")
		return
	}

	j.SetProgress("wrote %d rows [%s r/s]", cnt, getRate(cnt))

	if j.df.Err() != nil {
		err = h.Error(j.df.Err(), "Error in runFileToFile")
	}
	return
}

func (j *Task) runDbToDb() (err error) {
	start = time.Now()
	if j.Cfg.Mode == "" {
		j.Cfg.Mode = "append"
	}

	// Initiate connections
	srcProps := h.MapToKVArr(j.Cfg.SrcConn.VarsS())
	srcConn, err := database.NewConnContext(j.Ctx, j.Cfg.SrcConn.URL, srcProps...)
	if err != nil {
		err = h.Error(err, "Could not initialize source connection")
		return
	}

	tgtProps := h.MapToKVArr(j.Cfg.TgtConn.VarsS())
	tgtConn, err := database.NewConnContext(j.Ctx, j.Cfg.TgtConn.URL, tgtProps...)
	if err != nil {
		err = h.Error(err, "Could not initialize target connection")
		return
	}

	j.SetProgress("connecting to source database")
	err = srcConn.Connect()
	if err != nil {
		err = h.Error(err, "Could not connect to: %s (%s)", j.Cfg.SrcConn.ID, srcConn.GetType())
		return
	}

	j.SetProgress("connecting to target database")
	err = tgtConn.Connect()
	if err != nil {
		err = h.Error(err, "Could not connect to: %s (%s)", j.Cfg.TgtConn.ID, tgtConn.GetType())
		return
	}

	defer srcConn.Close()
	defer tgtConn.Close()

	// check if table exists by getting target columns
	j.Cfg.TgtColumns, _ = tgtConn.GetSQLColumns("select * from " + j.Cfg.TgtTable)

	if j.Cfg.Mode == "upsert" {
		j.SetProgress("getting checkpoint value")
		j.Cfg.UpsertVal, err = getUpsertValue(&j.Cfg, tgtConn, srcConn.Template().Variable)
		if err != nil {
			err = h.Error(err, "Could not getUpsertValue")
			return err
		}
	}

	j.SetProgress("reading from source database")
	j.df, err = j.ReadFromDB(&j.Cfg, srcConn)
	if err != nil {
		err = h.Error(err, "Could not ReadFromDB")
		return
	}
	defer j.df.Close()

	// to DirectLoad if possible
	vars := map[string]interface{}{}
	for k, v := range h.KVArrToMap(srcConn.PropArr()...) {
		vars[k] = v
	}
	j.Cfg.SrcFile = iop.DataConn{
		URL:  j.df.FsURL,
		Vars: vars,
	}

	j.SetProgress("writing to target database")
	cnt, err := j.WriteToDb(&j.Cfg, j.df, tgtConn)
	if err != nil {
		err = h.Error(err, "Could not WriteToDb")
		return
	}

	elapsed := int(time.Since(start).Seconds())
	j.SetProgress("inserted %d rows in %d secs [%s r/s]", cnt, elapsed, getRate(cnt))

	if j.df.Context.Err() != nil {
		err = h.Error(j.df.Context.Err(), "Error running runDbToDb")
	}
	return
}

// ReadFromDB reads from a source database
func (j *Task) ReadFromDB(cfg *Config, srcConn database.Connection) (df *iop.Dataflow, err error) {

	fieldsStr := "*"
	sql := h.F(`select %s from %s`, fieldsStr, cfg.SrcTable)

	if cfg.SrcSQL != "" {
		// for upsert, need to put `{upsert_where_cond}` for proper selecting
		sql, err = GetSQLText(cfg.SrcSQL)
		if err != nil {
			err = h.Error(err, "Could not get GetSQLText for: "+cfg.SrcSQL)
			return
		}
	} else if cfg.Mode != "drop" && len(j.Cfg.TgtColumns) > 0 {
		// since we are not dropping and table exists, we need to only select the matched columns
		srcData, _ := srcConn.Query(h.F("select * from %s where 1=0", cfg.SrcTable))

		if len(srcData.Columns) > 0 {
			commFields := database.CommonColumns(
				database.ColumnNames(srcData.Columns),
				database.ColumnNames(j.Cfg.TgtColumns),
			)
			if len(commFields) == 0 {
				err = h.Error("src table and tgt table have no columns with same names. Column names must match")
				return
			}
			fieldsStr = strings.Join(commFields, ", ")
		}
	}

	// Get source columns
	cfg.SrcColumns, err = srcConn.GetSQLColumns(h.R(sql, "upsert_where_cond", "1=0"))
	if err != nil {
		err = h.Error(err, "Could not obtain source columns")
		return
	}

	if cfg.Mode == "upsert" {
		// select only records that have been modified after last max value
		upsertWhereCond := "1=1"
		if cfg.UpsertVal != "" {
			upsertWhereCond = h.R(
				"{update_key} >= {value}",
				"update_key", srcConn.Quote(cfg.SrcColumns.Normalize(cfg.UpdateKey)),
				"value", cfg.UpsertVal,
			)
		}

		if cfg.SrcSQL != "" {
			if !strings.Contains(sql, "{upsert_where_cond}") {
				err = h.Error("For upsert loading with custom SQL, need to include where clause placeholder {upsert_where_cond}. e.g: select * from my_table where col2='A' AND {upsert_where_cond}")
				return
			}
			sql = h.R(sql, "upsert_where_cond", upsertWhereCond)
		} else {
			sql = h.R(
				`select {fields} from {table} where {upsert_where_cond}`,
				"fields", fieldsStr,
				"table", cfg.SrcTable,
				"upsert_where_cond", upsertWhereCond,
			)
		}
	} else if cfg.Limit > 0 {
		sql = h.R(
			srcConn.Template().Core["limit"],
			"fields", fieldsStr,
			"table", cfg.SrcTable,
			"limit", cast.ToString(cfg.Limit),
		)
	}

	df, err = srcConn.BulkExportFlow(sql)
	if err != nil {
		err = h.Error(err, "Could not BulkStream: "+sql)
		return
	}

	return
}

// ReadFromFile reads from a source file
func (j *Task) ReadFromFile(cfg *Config) (df *iop.Dataflow, err error) {

	var stream *iop.Datastream

	if cfg.SrcFile.URL != "" {
		fs, err := iop.NewFileSysClientFromURLContext(j.Ctx, cfg.SrcFile.URL, h.MapToKVArr(cfg.SrcFile.VarsS())...)
		if err != nil {
			err = h.Error(err, "Could not obtain client for: "+cfg.SrcFile.URL)
			return df, err
		}

		df, err = fs.ReadDataflow(cfg.SrcFile.URL)
		if err != nil {
			err = h.Error(err, "Could not FileSysReadDataflow for: "+cfg.SrcFile.URL)
			return df, err
		}
	} else {
		stream, err = iop.MakeDatastream(bufio.NewReader(os.Stdin))
		if err != nil {
			err = h.Error(err, "Could not MakeDatastream")
			return
		}
		df, err = iop.MakeDataFlow(stream)
		if err != nil {
			err = h.Error(err, "Could not MakeDataFlow for Stdin")
			return
		}
	}

	return
}

// WriteToFile writes to a target file
func (j *Task) WriteToFile(cfg *Config, df *iop.Dataflow) (cnt uint64, err error) {
	var stream *iop.Datastream
	var bw int64

	if cfg.TgtFile.URL != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtFile.URL = h.Rm(cfg.TgtFile.URL, dateMap)
		fs, err := iop.NewFileSysClientFromURLContext(j.Ctx, cfg.TgtFile.URL, h.MapToKVArr(cfg.TgtFile.VarsS())...)
		if err != nil {
			err = h.Error(err, "Could not obtain client for: "+cfg.TgtFile.URL)
			return cnt, err
		}

		bw, err = fs.WriteDataflow(df, cfg.TgtFile.URL)
		if err != nil {
			err = h.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.StdOut {
		stream = iop.MergeDataflow(df)
		reader := stream.NewCsvReader(0)
		bufStdout := bufio.NewWriter(os.Stdout)
		defer bufStdout.Flush()
		bw, err = iop.Write(reader, bufStdout)
		if err != nil {
			err = h.Error(err, "Could not write to Stdout")
			return
		}
		cnt = stream.Count
	} else {
		err = h.Error("target for output is not specified")
		return
	}

	h.Debug(
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
	if cfg.TgtTableTmp == "" {
		cfg.TgtTableTmp = cfg.TgtTable + "_tmp" + h.RandString(h.NumericRunes, 1) + strings.ToLower(h.RandString(h.AplhanumericRunes, 1))
	}
	if cfg.Mode == "" {
		cfg.Mode = "append"
	}

	// pre SQL
	if cfg.TgtPreSQL != "" {
		j.SetProgress("executing pre-sql")
		sql, err := GetSQLText(cfg.TgtPreSQL)
		if err != nil {
			err = h.Error(err, "could not get pre-sql body")
			return cnt, err
		}
		_, err = tgtConn.Exec(sql)
		if err != nil {
			err = h.Error(err, "could not execute pre-sql on target")
			return cnt, err
		}
	}

	// Drop & Create the temp table
	err = tgtConn.DropTable(cfg.TgtTableTmp)
	if err != nil {
		err = h.Error(err, "could not drop table "+cfg.TgtTableTmp)
		return
	}
	sampleData := iop.NewDataset(df.Columns)
	sampleData.Rows = df.Buffer
	sampleData.SafeInference = true
	_, err = createTableIfNotExists(tgtConn, sampleData, cfg.TgtTableTmp, "")
	if err != nil {
		err = h.Error(err, "could not create temp table "+cfg.TgtTableTmp)
		return
	}
	cfg.TmpTableCreated = true
	defer tgtConn.DropTable(cfg.TgtTableTmp)

	// TODO: if srcFile is from a cloud storage, and targetDB
	// supports direct loading (RedShift, Snowflake or Azure)
	// do direct loading (without passing through our box)
	// risk is potential data loss, since we cannot validate counts
	cnt, ok, err := tgtConn.CopyDirect(cfg.TgtTableTmp, cfg.SrcFile)
	if ok {
		df.SetEmpty() // this executes deferred functions (such as file residue removal
		if err != nil {
			err = h.Error(err, "could not directly load into database")
			return
		}
		h.Debug("copied directly from cloud storage")
		cnt, _ = tgtConn.GetCount(cfg.TgtTableTmp)
		// perhaps do full analysis to validate quality
	} else {
		j.SetProgress("streaming inserts")
		cnt, err = tgtConn.BulkImportFlow(cfg.TgtTableTmp, df)
		if err != nil {
			err = h.Error(err, "could not insert into "+cfg.TgtTable)
			return
		}
		tCnt, _ := tgtConn.GetCount(cfg.TgtTableTmp)
		if cnt != tCnt {
			err = h.Error("inserted in temp table but table count (%d) != stream count (%d). Records missing. Aborting", tCnt, cnt)
			return
		}
		// aggregate stats from stream processors
		df.SyncStats()

		// Checksum Comparison, data quality
		err = tgtConn.CompareChecksums(cfg.TgtTableTmp, df.Columns)
		if err != nil {
			h.Debug(err.Error())
		}
	}

	if cnt > 0 {
		if cfg.Mode == "drop" {
			// drop, (create if not exists) and insert directly
			err = tgtConn.DropTable(cfg.TgtTable)
			if err != nil {
				err = h.Error(err, "could not drop table "+cfg.TgtTable)
				return
			}
			j.SetProgress("dropped table " + cfg.TgtTable)
		}

		// create table if not exists
		sample := iop.NewDataset(df.Columns)
		sample.Rows = df.Buffer
		sample.Inferred = true // already inferred with SyncStats
		created, err := createTableIfNotExists(
			tgtConn,
			sample,
			cfg.TgtTable,
			cfg.TgtTableDDL,
		)
		if err != nil {
			err = h.Error(err, "could not create table "+cfg.TgtTable)
			return cnt, err
		} else if created {
			j.SetProgress("created table %s", cfg.TgtTable)
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
			err = tgtConn.OptimizeTable(cfg.TgtTable, df.Columns)
			if err != nil {
				err = h.Error(err, "could not optimize table schema")
				return cnt, err
			}
		}
	}

	// Put data from tmp to final
	if cnt == 0 {
		j.SetProgress("0 rows inserted. Nothing to do.")
	} else if cfg.Mode == "drop (need to optimize temp table in place)" {
		// use swap
		err = tgtConn.SwapTable(cfg.TgtTableTmp, cfg.TgtTable)
		if err != nil {
			err = h.Error(err, "could not swap tables %s to %s", cfg.TgtTableTmp, cfg.TgtTable)
			return 0, err
		}

		err = tgtConn.DropTable(cfg.TgtTableTmp)
		if err != nil {
			err = h.Error(err, "could not drop table "+cfg.TgtTableTmp)
			return
		}
		j.SetProgress("dropped old table of " + cfg.TgtTable)

	} else if cfg.Mode == "append" || cfg.Mode == "drop" {
		// create if not exists and insert directly
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = h.Error(err, "Could not insert from temp")
			return 0, err
		}
	} else if cfg.Mode == "truncate" {
		// truncate (create if not exists) and insert directly
		truncSQL := h.R(
			tgtConn.GetTemplateValue("core.truncate_table"),
			"table", cfg.TgtTable,
		)
		_, err = tgtConn.Exec(truncSQL)
		if err != nil {
			err = h.Error(err, "Could not truncate table: "+cfg.TgtTable)
			return
		}
		j.SetProgress("truncated table " + cfg.TgtTable)

		// insert
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = h.Error(err, "Could not insert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
	} else if cfg.Mode == "upsert" {
		// insert in temp
		// create final if not exists
		// delete from final and insert
		// or update (such as merge or ON CONFLICT)
		rowAffCnt, err := tgtConn.Upsert(cfg.TgtTableTmp, cfg.TgtTable, strings.Split(cfg.PrimaryKey, ","))
		if err != nil {
			err = h.Error(err, "Could not upsert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
		j.SetProgress("%d INSERTS / UPDATES", rowAffCnt)
	}

	// post SQL
	if cfg.TgtPostSQL != "" {
		j.SetProgress("executing post-sql")
		sql, err := GetSQLText(cfg.TgtPostSQL)
		if err != nil {
			err = h.Error(err, "Error executing TgtPostSQL. Could not get GetSQLText for: "+cfg.TgtPostSQL)
			return cnt, err
		}
		_, err = tgtConn.Exec(sql)
		if err != nil {
			err = h.Error(err, "Error executing TgtPostSQL")
			return cnt, err
		}
	}

	err = df.Context.Err()
	return
}

func createTableIfNotExists(conn database.Connection, data iop.Dataset, tableName string, tableDDL string) (created bool, err error) {

	if tableDDL == "" {
		tableDDL, err = conn.GenerateDDL(tableName, data)
		if err != nil {
			return false, h.Error(err, "Could not generate DDL for "+tableName)
		}
	}

	_, err = conn.Exec(tableDDL)
	if err != nil {
		errorFilterTableExists := conn.GetTemplateValue("variable.error_filter_table_exists")
		if errorFilterTableExists != "" && strings.Contains(err.Error(), errorFilterTableExists) {
			h.Debug("did not create table %s as it already exists", tableName)
			return false, nil
		} else {
			return false, h.Error(err, "Error creating table "+tableName)
		}
	}

	return true, nil
}

func insertFromTemp(cfg *Config, tgtConn database.Connection) (err error) {
	// insert
	tmpColumns, err := tgtConn.GetColumns(cfg.TgtTableTmp)
	if err != nil {
		err = h.Error(err, "could not get column list for "+cfg.TgtTableTmp)
		return
	}
	tgtColumns, err := tgtConn.GetColumns(cfg.TgtTable)
	if err != nil {
		err = h.Error(err, "could not get column list for "+cfg.TgtTable)
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
		err = h.Error(err, "columns mismatched")
		return
	}

	srcFields := tgtConn.CastColumnsForSelect(tmpColumns, tgtColumns)

	sql := h.R(
		tgtConn.Template().Core["insert_from_table"],
		"tgt_table", cfg.TgtTable,
		"src_table", cfg.TgtTableTmp,
		"tgt_fields", strings.Join(tgtFields, ", "),
		"src_fields", strings.Join(srcFields, ", "),
	)
	_, err = tgtConn.Exec(sql)
	if err != nil {
		err = h.Error(err, "Could not execute SQL: "+sql)
		return
	}
	h.Debug("inserted rows into `%s` from temp table `%s`", cfg.TgtTable, cfg.TgtTableTmp)
	return
}

func getUpsertValue(cfg *Config, tgtConn database.Connection, srcConnVarMap map[string]string) (val string, err error) {
	// get table columns type for table creation if not exists
	// in order to get max value
	// does table exists?
	// get max value from key_field
	sql := h.F(
		"select max(%s) as max_val from %s",
		cfg.UpdateKey,
		cfg.TgtTable,
	)

	data, err := tgtConn.Query(sql)
	if err != nil {
		if strings.Contains(err.Error(), "exist") {
			// table does not exists, will be create later
			// set val to blank for full load
			return "", nil
		}
		err = h.Error(err, "could not get max value for "+cfg.UpdateKey)
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
		val = h.R(
			srcConnVarMap["timestamp_layout_str"],
			"value", cast.ToTime(value).Format(srcConnVarMap["timestamp_layout"]),
		)
	} else if strings.Contains("datetime,date", colType) {
		val = h.R(
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
			return "", h.Error(err, "Could not ReadFile: "+sqlStr)
		}
		sql = string(bytes)
	}

	return sql, nil
}
