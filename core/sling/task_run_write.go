package sling

import (
	"bufio"
	"context"
	"database/sql"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// WriteToFile writes to a target file
func (t *TaskExecution) WriteToFile(cfg *Config, df *iop.Dataflow) (cnt uint64, err error) {
	var bw int64
	defer t.PBar.Finish()

	if uri := cfg.TgtConn.URL(); uri != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtConn.Set(g.M("url", g.Rm(uri, dateMap)))

		// construct props by merging with options
		options := g.M()
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)
		props := append(
			g.MapToKVArr(cfg.TgtConn.DataS()),
			g.MapToKVArr(g.ToMapString(options))...,
		)

		fs, err := filesys.NewFileSysClientFromURLContext(t.Context.Ctx, uri, props...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for: %s", cfg.TgtConn.Type)
			return cnt, err
		}

		// apply column casing
		applyColumnCasingToDf(df, fs.FsType(), t.Config.Target.Options.ColumnCasing)

		bw, err = fs.WriteDataflow(df, uri)
		if err != nil {
			err = g.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.Options.StdOut {
		// apply column casing
		applyColumnCasingToDf(df, dbio.TypeFileLocal, t.Config.Target.Options.ColumnCasing)

		limit := cast.ToUint64(cfg.Source.Limit())
		options := map[string]string{"delimiter": ","}
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)

		for stream := range df.StreamCh {
			stream.SetConfig(options)
			for batchR := range stream.NewCsvReaderChnl(cast.ToInt(limit), 0) {
				if limit > 0 && cnt >= limit {
					return
				}

				if len(batchR.Columns) != len(df.Columns) {
					err = g.Error(err, "number columns have changed, not compatible with stdout.")
					return
				}
				bufStdout := bufio.NewWriter(os.Stdout)
				bw, err = filesys.Write(batchR.Reader, bufStdout)
				bufStdout.Flush()
				if err != nil {
					err = g.Error(err, "Could not write to Stdout")
					return
				} else if err = stream.Context.Err(); err != nil {
					err = g.Error(err, "encountered stream error")
					return
				}
				cnt = cnt + uint64(batchR.Counter)
			}
		}
	} else {
		err = g.Error("target for output is not specified")
		return
	}

	g.DebugLow(
		"wrote %s: %d rows [%s r/s]",
		humanize.Bytes(cast.ToUint64(bw)), cnt, getRate(cnt),
	)

	return
}

// WriteToDb writes to a target DB
// create temp table
// load into temp table
// insert / incremental / replace into target table
func (t *TaskExecution) WriteToDb(cfg *Config, df *iop.Dataflow, tgtConn database.Connection) (cnt uint64, err error) {
	defer t.PBar.Finish()

	// detect empty
	if len(df.Columns) == 0 {
		err = g.Error("no stream columns detected")
		return
	}

	targetTable, err := database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
	if err != nil {
		return 0, g.Error(err, "could not parse object table name")
	}
	targetTable.DDL = cfg.Target.Options.TableDDL
	targetTable.DDL = g.R(targetTable.DDL, "object_name", targetTable.Raw, "table", targetTable.Raw)
	targetTable.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

	// check table ddl
	if targetTable.DDL != "" && !strings.Contains(targetTable.DDL, targetTable.Raw) {
		err = g.Error("The Table DDL provided needs to contains the exact object table name: %s\nProvided:\n%s", targetTable.Raw, targetTable.DDL)
		return
	}

	var tableTmp database.Table
	if cfg.Target.Options.TableTmp == "" {
		tableTmp, err = database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
		if err != nil {
			return 0, g.Error(err, "could not parse object table name")
		}
		suffix := lo.Ternary(tgtConn.GetType().DBNameUpperCase(), "_TMP", "_tmp")
		if g.In(tgtConn.GetType(), dbio.TypeDbOracle) {
			if len(tableTmp.Name) > 24 {
				tableTmp.Name = tableTmp.Name[:24] // max is 30 chars
			}

			// some weird column / commit error, not picking up latest columns
			suffix2 := g.RandString(g.NumericRunes, 1) + g.RandString(g.AplhanumericRunes, 1)
			suffix2 = lo.Ternary(
				tgtConn.GetType().DBNameUpperCase(),
				strings.ToUpper(suffix2),
				strings.ToLower(suffix2),
			)
			suffix = suffix + suffix2
		}

		tableTmp.Name = tableTmp.Name + suffix
		cfg.Target.Options.TableTmp = tableTmp.FullName()
	} else {
		tableTmp, err = database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
		if err != nil {
			return 0, g.Error(err, "could not parse temp table name")
		}
	}

	// set DDL
	tableTmp.DDL = strings.Replace(targetTable.DDL, targetTable.Raw, tableTmp.FullName(), 1)
	tableTmp.Raw = tableTmp.FullName()
	err = tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)
	if err != nil {
		err = g.Error(err, "could not set keys for "+tableTmp.FullName())
		return
	}

	// create schema if not exist
	_, err = createSchemaIfNotExists(tgtConn, tableTmp.Schema)
	if err != nil {
		err = g.Error(err, "Error checking & creating schema "+tableTmp.Schema)
		return
	}

	// Drop & Create the temp table
	err = tgtConn.DropTable(tableTmp.FullName())
	if err != nil {
		err = g.Error(err, "could not drop table "+tableTmp.FullName())
		return
	}

	if paused := df.Pause(); !paused { // to create DDL and set column change functions
		err = g.Error(err, "could not pause streams to infer columns")
		return
	}

	// apply column casing
	applyColumnCasingToDf(df, tgtConn.GetType(), t.Config.Target.Options.ColumnCasing)

	sampleData := iop.NewDataset(df.Columns)
	sampleData.Rows = df.Buffer
	sampleData.Inferred = df.Inferred
	if !sampleData.Inferred {
		sampleData.SafeInference = true
		sampleData.InferColumnTypes()
		df.Columns = sampleData.Columns
	}

	// set table keys
	tableTmp.Columns = sampleData.Columns
	err = tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)
	if err != nil {
		err = g.Error(err, "could not set keys for "+tableTmp.FullName())
		return
	}

	_, err = createTableIfNotExists(tgtConn, sampleData, tableTmp)
	if err != nil {
		err = g.Error(err, "could not create temp table "+tableTmp.FullName())
		return
	}
	cfg.Target.TmpTableCreated = true
	df.Columns = sampleData.Columns

	t.AddCleanupTaskFirst(func() {
		if cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			return
		}

		conn, err := t.getTgtDBConn(context.Background())
		if err == nil {
			g.LogError(conn.DropTable(tableTmp.FullName()))
		}
	})

	err = tgtConn.BeginContext(df.Context.Ctx)
	if err != nil {
		err = g.Error(err, "could not open transaction to write to temp table")
		return
	}

	adjustColumnType := cfg.Target.Options.AdjustColumnType != nil && *cfg.Target.Options.AdjustColumnType

	// set OnColumnChanged
	if adjustColumnType {
		df.OnColumnChanged = func(col iop.Column) error {
			tableTmp.Columns, err = tgtConn.GetSQLColumns(tableTmp)
			if err != nil {
				return g.Error(err, "could not get table columns for schema change")
			}

			// preseve keys
			tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

			ok, err := tgtConn.OptimizeTable(&tableTmp, iop.Columns{col}, true)
			if err != nil {
				return g.Error(err, "could not change table schema")
			} else if ok {
				cfg.Target.columns = tableTmp.Columns
			} else {
				// revert to old type
				col.Type = df.Columns[col.Position-1].Type
			}
			df.Columns.Merge(iop.Columns{col}, true)

			return nil
		}
	}

	// set OnColumnAdded
	if *cfg.Target.Options.AddNewColumns {
		df.OnColumnAdded = func(col iop.Column) error {

			// sleep to allow transaction to close
			// time.Sleep(100 * time.Millisecond)

			// df.Context.Lock()
			// defer df.Context.Unlock()

			ok, err := tgtConn.AddMissingColumns(tableTmp, iop.Columns{col})
			if err != nil {
				return g.Error(err, "could not add missing columns")
			} else if ok {
				_, err = pullTargetTempTableColumns(t.Config, tgtConn, true)
				if err != nil {
					return g.Error(err, "could not get table columns")
				}
			}
			return nil
		}
	}

	df.Unpause() // to create DDL and set column change functions
	t.SetProgress("streaming data")
	cnt, err = tgtConn.BulkImportFlow(tableTmp.FullName(), df)
	if err != nil {
		tgtConn.Rollback()
		if cast.ToBool(os.Getenv("SLING_CLI")) && cfg.sourceIsFile() {
			err = g.Error(err, "could not insert into %s.", tableTmp.FullName())
		} else {
			err = g.Error(err, "could not insert into "+tableTmp.FullName())
		}
		return
	}

	tgtConn.Commit()
	t.PBar.Finish()

	tCnt, _ := tgtConn.GetCount(tableTmp.FullName())
	if cnt != tCnt {
		err = g.Error("inserted in temp table but table count (%d) != stream count (%d). Records missing/mismatch. Aborting", tCnt, cnt)
		return
	} else if tCnt == 0 && len(sampleData.Rows) > 0 {
		err = g.Error("Loaded 0 records while sample data has %d records. Exiting.", len(sampleData.Rows))
		return
	}

	// pre SQL
	if preSQL := cfg.Target.Options.PreSQL; preSQL != "" {
		t.SetProgress("executing pre-sql")
		preSQL, err = GetSQLText(preSQL)
		if err != nil {
			err = g.Error(err, "could not get pre-sql body")
			return cnt, err
		}

		fMap, err := t.Config.GetFormatMap()
		if err != nil {
			err = g.Error(err, "could not get format map for pre-sql")
			return cnt, err
		}

		_, err = tgtConn.ExecMulti(g.Rm(preSQL, fMap))
		if err != nil {
			err = g.Error(err, "could not execute pre-sql on target")
			return cnt, err
		}
	}

	if cnt == 0 && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY_TABLES")) {
		g.Warn("No data or records found in stream. Nothing to do. To allow Sling to create empty tables, set SLING_ALLOW_EMPTY_TABLES=TRUE")
		return
	} else if cnt > 0 {
		// FIXME: find root cause of why columns don't synch while streaming
		df.SyncColumns()

		// aggregate stats from stream processors
		df.Inferred = !cfg.sourceIsFile() // re-infer is source is file
		df.SyncStats()

		// Checksum Comparison, data quality. Limit to 10k, cause sums get too high
		if df.Count() <= 10000 {
			err = tgtConn.CompareChecksums(tableTmp.FullName(), df.Columns)
			if err != nil {
				if os.Getenv("ERROR_ON_CHECKSUM_FAILURE") != "" {
					return
				}
				g.DebugLow(g.ErrMsgSimple(err))
			}
		}
	}

	// need to contain the final write in a transcation after data is loaded
	txOptions := sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false}
	switch tgtConn.GetType() {
	case dbio.TypeDbSnowflake, dbio.TypeDbDuckDb:
		txOptions = sql.TxOptions{}
	case dbio.TypeDbClickhouse, dbio.TypeDbOracle:
		txOptions = sql.TxOptions{Isolation: sql.LevelDefault}
	}
	err = tgtConn.BeginContext(df.Context.Ctx, &txOptions)
	if err != nil {
		err = g.Error(err, "could not open transcation to write to final table")
		return
	}

	defer tgtConn.Rollback() // rollback in case of error

	{
		if cfg.Mode == FullRefreshMode {
			// drop, (create if not exists) and insert directly
			err = tgtConn.DropTable(targetTable.FullName())
			if err != nil {
				err = g.Error(err, "could not drop table "+targetTable.FullName())
				return cnt, err
			}
		}

		// create table if not exists
		sample := iop.NewDataset(df.Columns)
		sample.Rows = df.Buffer
		sample.Inferred = true // already inferred with SyncStats

		created, err := createTableIfNotExists(tgtConn, sample, targetTable)
		if err != nil {
			err = g.Error(err, "could not create table "+targetTable.FullName())
			return cnt, err
		} else if created {
			t.SetProgress("created table %s", targetTable.FullName())
		}

		if !created && cfg.Mode != FullRefreshMode {
			if *cfg.Target.Options.AddNewColumns {
				ok, err := tgtConn.AddMissingColumns(targetTable, sample.Columns)
				if err != nil {
					return cnt, g.Error(err, "could not add missing columns")
				} else if ok {
					targetTable.Columns, err = pullTargetTableColumns(t.Config, tgtConn, true)
					if err != nil {
						return cnt, g.Error(err, "could not get table columns")
					}
				}
			}

			if adjustColumnType {

				targetTable.Columns, err = tgtConn.GetSQLColumns(targetTable)
				if err != nil {
					return cnt, g.Error(err, "could not get table columns for optimization")
				}

				// preseve keys
				targetTable.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

				ok, err := tgtConn.OptimizeTable(&targetTable, sample.Columns, false)
				if err != nil {
					return cnt, g.Error(err, "could not optimize table schema")
				} else if ok {
					cfg.Target.columns = targetTable.Columns
					for i := range df.Columns {
						df.Columns[i].Type = targetTable.Columns[i].Type
						df.Columns[i].DbType = targetTable.Columns[i].DbType
						for _, ds := range df.StreamMap {
							if len(ds.Columns) == len(df.Columns) {
								ds.Columns[i].Type = targetTable.Columns[i].Type
								ds.Columns[i].DbType = targetTable.Columns[i].DbType
							}
						}
					}
				}
			}
		}
	}

	// Put data from tmp to final
	if cnt == 0 {
		t.SetProgress("0 rows inserted. Nothing to do.")
	} else if cfg.Mode == "drop (need to optimize temp table in place)" {
		// use swap
		err = tgtConn.SwapTable(tableTmp.FullName(), targetTable.FullName())
		if err != nil {
			err = g.Error(err, "could not swap tables %s to %s", tableTmp.FullName(), targetTable.FullName())
			return 0, err
		}

	} else if (cfg.Mode == IncrementalMode && len(t.Config.Source.PrimaryKey()) == 0) || cfg.Mode == SnapshotMode || cfg.Mode == FullRefreshMode {
		// create if not exists and insert directly
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			return 0, err
		}
	} else if cfg.Mode == TruncateMode {
		// truncate (create if not exists) and insert directly
		truncSQL := g.R(
			tgtConn.GetTemplateValue("core.truncate_table"),
			"table", targetTable.FullName(),
		)
		_, err = tgtConn.Exec(truncSQL)
		if err != nil {
			err = g.Error(err, "Could not truncate table: "+targetTable.FullName())
			return
		}
		t.SetProgress("truncated table " + targetTable.FullName())

		// insert
		err = insertFromTemp(cfg, tgtConn)
		if err != nil {
			err = g.Error(err, "Could not insert from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
	} else if cfg.Mode == IncrementalMode || cfg.Mode == BackfillMode {
		// insert in temp
		// create final if not exists
		// delete from final and insert
		// or update (such as merge or ON CONFLICT)
		rowAffCnt, err := tgtConn.Upsert(tableTmp.FullName(), targetTable.FullName(), cfg.Source.PrimaryKey())
		if err != nil {
			err = g.Error(err, "Could not incremental from temp")
			// data is still in temp table at this point
			// need to decide whether to drop or keep it for future use
			return 0, err
		}
		if rowAffCnt > 0 {
			g.DebugLow("%d TOTAL INSERTS / UPDATES", rowAffCnt)
		}
	}

	// post SQL
	if postSQL := cfg.Target.Options.PostSQL; postSQL != "" {
		t.SetProgress("executing post-sql")

		postSQL, err = GetSQLText(postSQL)
		if err != nil {
			err = g.Error(err, "Error executing Target.PostSQL. Could not get getSQLText for: "+cfg.Target.Options.PostSQL)
			return cnt, err
		}

		fMap, err := t.Config.GetFormatMap()
		if err != nil {
			err = g.Error(err, "could not get format map for post-sql")
			return cnt, err
		}

		_, err = tgtConn.ExecMulti(g.Rm(postSQL, fMap))
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

	err = df.Err()
	return
}
