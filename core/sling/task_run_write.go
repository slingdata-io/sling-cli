package sling

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
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
	setStage("5 - load-into-final")

	if uri := cfg.TgtConn.URL(); uri != "" {
		dateMap := iop.GetISO8601DateMap(time.Now())
		cfg.TgtConn.Set(g.M("url", g.Rm(uri, dateMap)))

		if len(df.Buffer) == 0 && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY")) {
			g.Warn("No data or records found in stream. Nothing to do. To allow Sling to create empty files, set SLING_ALLOW_EMPTY=TRUE")
			return
		}

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

		bw, err = filesys.WriteDataflow(fs, df, uri)
		if err != nil {
			err = g.Error(err, "Could not FileSysWriteDataflow")
			return cnt, err
		}
		cnt = df.Count()
	} else if cfg.Options.StdOut {
		// apply column casing
		applyColumnCasingToDf(df, dbio.TypeFileLocal, t.Config.Target.Options.ColumnCasing)

		limit := cast.ToUint64(cfg.Source.Limit())

		// store as dataset
		if cfg.Options.Dataset {
			df.Limit = limit
			data, err := df.Collect()
			if err != nil {
				err = g.Error(err, "Could not collect dataflow")
				return cnt, err
			}
			t.data = &data
			cnt = cast.ToUint64(len(data.Rows))
			return cnt, nil
		}

		options := map[string]string{"delimiter": ","}
		g.Unmarshal(g.Marshal(cfg.Target.Options), &options)

		for stream := range df.StreamCh {
			// stream.SetConfig(options)
			// c := iop.CSV{File: os.Stdout}
			// cnt, err = c.WriteStream(stream)
			// if err != nil {
			// 	err = g.Error(err, "Could not write to Stdout")
			// 	return
			// }

			// continue

			stream.SetConfig(options)
			sc := df.StreamConfig()
			sc.FileMaxRows = cast.ToInt64(limit)
			for batchR := range stream.NewCsvReaderChnl(sc) {
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
	setStage("6 - closing")

	return
}

// WriteToDb writes to a target DB
// create temp table
// load into temp table
// insert / incremental / replace into target table
func (t *TaskExecution) WriteToDb(cfg *Config, df *iop.Dataflow, tgtConn database.Connection) (cnt uint64, err error) {
	defer t.PBar.Finish()

	// Detect empty columns
	if len(df.Columns) == 0 {
		err = g.Error("no stream columns detected")
		return 0, err
	}

	// Initialize target and temp tables
	targetTable, err := initializeTargetTable(cfg, tgtConn)
	if err != nil {
		return 0, err
	}

	tableTmp, err := initializeTempTable(cfg, tgtConn, targetTable)
	if err != nil {
		return 0, err
	}

	setStage("4 - prepare-temp")

	// Ensure schema exists
	if err := ensureSchemaExists(tgtConn, tableTmp.Schema); err != nil {
		return 0, err
	}

	// Drop temp table if exists
	if err := dropTableIfExists(tgtConn, tableTmp.FullName()); err != nil {
		return 0, err
	}

	// Pause dataflow to set up DDL and handlers
	if paused := df.Pause(); !paused {
		err = g.Error(err, "could not pause streams to infer columns")
		return 0, err
	}

	// Prepare dataflow
	sampleData, err := prepareDataflow(t, df, tgtConn)
	if err != nil {
		return 0, err
	}

	// Set table keys
	tableTmp.Columns = sampleData.Columns
	if err := tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys); err != nil {
		err = g.Error(err, "could not set keys for "+tableTmp.FullName())
		return 0, err
	}

	// Create temp table
	if err := createTable(t, tgtConn, tableTmp, sampleData, true); err != nil {
		err = g.Error(err, "could not create table "+tableTmp.FullName())
		return 0, err
	}

	cfg.Target.Options.TableDDL = g.String(tableTmp.DDL)
	cfg.Target.TmpTableCreated = true
	df.Columns = sampleData.Columns
	setStage("4 - load-into-temp")

	// Add cleanup task for temp table
	t.AddCleanupTaskFirst(func() {
		if cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			return
		}

		conn := tgtConn
		if tgtConn.Context().Err() != nil {
			conn, err = t.getTgtDBConn(context.Background())
			if err == nil {
				conn.Connect()
			}
		}
		g.LogError(conn.DropTable(tableTmp.FullName()))
		conn.Close()
	})

	// Begin transaction for temp table operations
	if err := tgtConn.BeginContext(df.Context.Ctx); err != nil {
		err = g.Error(err, "could not open transaction to write to temp table")
		return 0, err
	}

	// Configure column handlers
	if err := configureColumnHandlers(t, cfg, df, tgtConn, tableTmp); err != nil {
		err = g.Error(err, "could not configure column handlers")
		return 0, err
	}

	df.Unpause() // Resume dataflow
	t.SetProgress("streaming data")

	// Set batch limit if specified
	if batchLimit := cfg.Target.Options.BatchLimit; batchLimit != nil {
		df.SetBatchLimit(*batchLimit)
	}

	// Bulk import data into temp table
	cnt, err = tgtConn.BulkImportFlow(tableTmp.FullName(), df)
	if err != nil {
		tgtConn.Rollback()
		if cast.ToBool(os.Getenv("SLING_CLI")) && cfg.sourceIsFile() {
			err = g.Error(err, "could not insert into %s.", tableTmp.FullName())
		} else {
			err = g.Error(err, "could not insert into "+tableTmp.FullName())
		}
		return 0, err
	}

	if err := tgtConn.Commit(); err != nil {
		err = g.Error(err, "could not commit transaction")
		return 0, err
	}

	t.PBar.Finish()

	// Validate data
	tCnt, err := tgtConn.GetCount(tableTmp.FullName())
	if err != nil {
		err = g.Error(err, "could not get count for temp table "+tableTmp.FullName())
		return 0, err
	} else {
		if cnt != tCnt {
			err = g.Error("inserted in temp table but table count (%d) != stream count (%d). Records missing/mismatch. Aborting", tCnt, cnt)
			return 0, err
		} else if tCnt == 0 && len(sampleData.Rows) > 0 {
			err = g.Error("Loaded 0 records while sample data has %d records. Exiting.", len(sampleData.Rows))
			return 0, err
		}
	}

	// Execute pre-SQL
	if err := executeSQL(t, tgtConn, cfg.Target.Options.PreSQL, "pre"); err != nil {
		err = g.Error(err, "Error executing %s-sql", "pre")
		return 0, err
	}

	// Handle empty data case
	if cnt == 0 && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY_TABLES")) && !cast.ToBool(os.Getenv("SLING_ALLOW_EMPTY")) {
		g.Warn("No data or records found in stream. Nothing to do. To allow Sling to create empty tables, set SLING_ALLOW_EMPTY=TRUE")
		return 0, nil
	} else if cnt > 0 {
		// FIXME: find root cause of why columns don't sync while streaming
		df.SyncColumns()

		// Aggregate stats from stream processors
		df.Inferred = !cfg.sourceIsFile() // Re-infer if source is file
		df.SyncStats()

		// Checksum Comparison, data quality. Limit to env var SLING_CHECKSUM_ROWS, cause sums get too high
		if val := cast.ToUint64(os.Getenv("SLING_CHECKSUM_ROWS")); val > 0 && df.Count() <= val {
			err = tgtConn.CompareChecksums(tableTmp.FullName(), df.Columns)
			if err != nil {
				return
			}
		}
	}

	// need to contain the final write in a transcation after data is loaded

	txOptions := determineTxOptions(tgtConn.GetType())
	if err := tgtConn.BeginContext(df.Context.Ctx, &txOptions); err != nil {
		err = g.Error(err, "could not open transaction to write to final table")
		return 0, err
	}

	defer tgtConn.Rollback() // rollback in case of error

	setStage("5 - prepare-final")

	// Prepare final table operations
	if cnt, err = prepareFinal(t, cfg, tgtConn, targetTable, df, cnt); err != nil {
		err = g.Error(err, "Error preparing final table")
		return 0, err
	}

	// Put data from tmp to final
	setStage("5 - load-into-final")

	// Transfer data from temp to final table
	if cnt == 0 {
		t.SetProgress("0 rows inserted. Nothing to do.")
	} else if err := transferData(t, cfg, tgtConn, tableTmp, targetTable); err != nil {
		err = g.Error(err, "Error transferring data from temp to final table")
		return 0, err
	}

	// Execute post-SQL
	if err := executeSQL(t, tgtConn, cfg.Target.Options.PostSQL, "post"); err != nil {
		err = g.Error(err, "Error executing %s-sql", "post")
		return 0, err
	}

	// Commit transaction
	if err := tgtConn.Commit(); err != nil {
		err = g.Error(err, "could not commit final transaction")
		return 0, err
	}

	// Set progress as finished
	if err := df.Err(); err != nil {
		setStage("6 - closing")
		return cnt, err
	}

	setStage("6 - closing")

	return cnt, nil
}

func determineTxOptions(dbType dbio.Type) sql.TxOptions {
	switch dbType {
	case dbio.TypeDbSnowflake, dbio.TypeDbDuckDb:
		return sql.TxOptions{}
	case dbio.TypeDbClickhouse, dbio.TypeDbProton, dbio.TypeDbOracle:
		return sql.TxOptions{Isolation: sql.LevelDefault}
	default:
		return sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false}
	}
}

func initializeTargetTable(cfg *Config, tgtConn database.Connection) (database.Table, error) {
	targetTable, err := database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
	if err != nil {
		return database.Table{}, g.Error(err, "could not parse object table name")
	}

	if cfg.Target.Options.TableDDL != nil {
		targetTable.DDL = *cfg.Target.Options.TableDDL
	}
	targetTable.DDL = g.R(targetTable.DDL, "object_name", targetTable.Raw, "table", targetTable.Raw)
	targetTable.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys)

	// check table ddl
	if targetTable.DDL != "" && !strings.Contains(targetTable.DDL, targetTable.Raw) {
		err = g.Error("The Table DDL provided needs to contains the exact object table name: %s\nProvided:\n%s", targetTable.Raw, targetTable.DDL)
		return database.Table{}, err
	}

	return targetTable, nil
}

func initializeTempTable(cfg *Config, tgtConn database.Connection, targetTable database.Table) (database.Table, error) {
	var tableTmp database.Table
	var err error

	if cfg.Target.Options.TableTmp == "" {
		tableTmp, err = database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
		if err != nil {
			return database.Table{}, g.Error(err, "could not parse object table name")
		}
		suffix := lo.Ternary(tgtConn.GetType().DBNameUpperCase(), "_TMP", "_tmp")
		if g.In(tgtConn.GetType(), dbio.TypeDbOracle) {
			if len(tableTmp.Name) > 24 {
				tableTmp.Name = tableTmp.Name[:24] // Max 30 chars
			}

			// some weird column / commit error, not picking up latest columns
			suffix2 := g.RandString(g.NumericRunes, 1) + g.RandString(g.AlphaNumericRunes, 1)
			suffix2 = lo.Ternary(
				tgtConn.GetType().DBNameUpperCase(),
				strings.ToUpper(suffix2),
				strings.ToLower(suffix2),
			)
			suffix += suffix2
		}

		tableTmp.Name += suffix
		cfg.Target.Options.TableTmp = tableTmp.FullName()
	} else {
		tableTmp, err = database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
		if err != nil {
			return database.Table{}, g.Error(err, "could not parse temp table name")
		}
	}

	// Set DDL for temp table
	tableTmp.DDL = strings.Replace(targetTable.DDL, targetTable.Raw, tableTmp.FullName(), 1)
	tableTmp.Raw = tableTmp.FullName()
	if err := tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys); err != nil {
		return database.Table{}, g.Error(err, "could not set keys for "+tableTmp.FullName())
	}

	return tableTmp, nil
}

func ensureSchemaExists(tgtConn database.Connection, schemaName string) error {
	if _, err := createSchemaIfNotExists(tgtConn, schemaName); err != nil {
		return g.Error(err, "Error checking & creating schema "+schemaName)
	}
	return nil
}

func dropTableIfExists(tgtConn database.Connection, tableName string) error {
	if err := tgtConn.DropTable(tableName); err != nil {
		return g.Error(err, "could not drop table "+tableName)
	}
	return nil
}

func createTable(t *TaskExecution, tgtConn database.Connection, table database.Table, sampleData iop.Dataset, isTemp bool) error {
	created, err := createTableIfNotExists(tgtConn, sampleData, &table, isTemp)
	if err != nil {
		return g.Error(err, "could not create table "+table.FullName())
	}
	if created {
		t.SetProgress("created table %s", table.FullName())
	}
	return nil
}

func configureColumnHandlers(t *TaskExecution, cfg *Config, df *iop.Dataflow, tgtConn database.Connection, tableTmp database.Table) error {
	adjustColumnType := cfg.Target.Options.AdjustColumnType != nil && *cfg.Target.Options.AdjustColumnType

	// set OnColumnChanged
	if adjustColumnType {
		df.OnColumnChanged = func(col iop.Column) error {
			var err error
			tableTmp.Columns, err = tgtConn.GetSQLColumns(tableTmp)
			if err != nil {
				return g.Error(err, "could not get table columns for schema change")
			}

			// preserve keys
			if err := tableTmp.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys); err != nil {
				return g.Error(err, "could not set keys for "+tableTmp.FullName())
			}

			ok, err := tgtConn.OptimizeTable(&tableTmp, iop.Columns{col}, true)
			if err != nil {
				return g.Error(err, "could not change table schema")
			} else if ok {
				cfg.Target.Columns = tableTmp.Columns
			} else {
				// revert to old type
				col.Type = df.Columns[col.Position-1].Type
			}
			df.Columns.Merge(iop.Columns{col}, true)

			return nil
		}
	}

	// set OnColumnAdded handler if adding new columns is enabled
	if cfg.Target.Options.AddNewColumns != nil && *cfg.Target.Options.AddNewColumns {
		df.OnColumnAdded = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(300 * time.Millisecond)

			// df.Context.Lock()
			// defer df.Context.Unlock()

			ok, err := tgtConn.AddMissingColumns(tableTmp, iop.Columns{col})
			if err != nil {
				return g.Error(err, "could not add missing columns")
			}
			if ok {
				if _, err := pullTargetTempTableColumns(t.Config, tgtConn, true); err != nil {
					return g.Error(err, "could not get table columns")
				}
			}
			return nil
		}
	}

	return nil
}

func prepareDataflow(t *TaskExecution, df *iop.Dataflow, tgtConn database.Connection) (iop.Dataset, error) {
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

	return sampleData, nil
}

func prepareFinal(
	t *TaskExecution,
	cfg *Config,
	tgtConn database.Connection,
	targetTable database.Table,
	df *iop.Dataflow,
	cnt uint64,
) (uint64, error) {

	// Handle Full Refresh Mode: Drop the target table if it exists
	if cfg.Mode == FullRefreshMode {
		if err := tgtConn.DropTable(targetTable.FullName()); err != nil {
			return cnt, g.Error(err, "could not drop table "+targetTable.FullName())
		}
	}

	// Create the target table if it does not exist
	sample := iop.NewDataset(df.Columns)
	sample.Rows = df.Buffer
	sample.Inferred = true // already inferred with SyncStats

	created, err := createTableIfNotExists(tgtConn, sample, &targetTable, false)
	if err != nil {
		return cnt, g.Error(err, "could not create table "+targetTable.FullName())
	}
	if created {
		t.SetProgress("created table %s", targetTable.FullName())
	}

	// If the table wasn't created and we're not in Full Refresh Mode, handle schema updates
	if !created && cfg.Mode != FullRefreshMode {
		// Add missing columns if the option is enabled
		if cfg.Target.Options.AddNewColumns != nil && *cfg.Target.Options.AddNewColumns {
			if ok, err := tgtConn.AddMissingColumns(targetTable, sample.Columns); err != nil {
				return cnt, g.Error(err, "could not add missing columns")
			} else if ok {
				if targetTable.Columns, err = pullTargetTableColumns(cfg, tgtConn, true); err != nil {
					return cnt, g.Error(err, "could not get table columns")
				}
			}
		}

		// Adjust column types if the option is enabled
		if cfg.Target.Options.AdjustColumnType != nil && *cfg.Target.Options.AdjustColumnType {
			if targetTable.Columns, err = tgtConn.GetSQLColumns(targetTable); err != nil {
				return cnt, g.Error(err, "could not get table columns for optimization")
			}

			// Preserve keys after fetching columns
			if err := targetTable.SetKeys(cfg.Source.PrimaryKey(), cfg.Source.UpdateKey, cfg.Target.Options.TableKeys); err != nil {
				return cnt, g.Error(err, "could not set keys for "+targetTable.FullName())
			}

			ok, err := tgtConn.OptimizeTable(&targetTable, sample.Columns, false)
			if err != nil {
				return cnt, g.Error(err, "could not optimize table schema")
			}
			if ok {
				cfg.Target.Columns = targetTable.Columns
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

	return cnt, nil
}

func transferData(t *TaskExecution, cfg *Config, tgtConn database.Connection, tableTmp, targetTable database.Table) error {
	if cfg.Mode == "drop (need to optimize temp table in place)" {
		// Use swap
		return transferBySwappingTables(tgtConn, tableTmp, targetTable)
	}

	if (cfg.Mode == IncrementalMode && len(cfg.Source.PrimaryKey()) == 0) || cfg.Mode == SnapshotMode || cfg.Mode == FullRefreshMode {
		// Create if not exists and insert directly
		if err := insertFromTemp(cfg, tgtConn); err != nil {
			err = g.Error(err, "Could not insert from temp")
			return err
		}
		return nil
	}

	if cfg.Mode == TruncateMode {
		// Truncate (create if not exists) and insert directly
		if err := truncateTable(t, tgtConn, targetTable.FullName()); err != nil {
			return err
		}
		if err := insertFromTemp(cfg, tgtConn); err != nil {
			err = g.Error(err, "Could not insert from temp")
			return err
		}
		return nil
	}

	if cfg.Mode == IncrementalMode || cfg.Mode == BackfillMode {
		// Insert in temp, create final if not exists, delete from final and insert or update
		if err := performUpsert(tgtConn, tableTmp, targetTable, cfg); err != nil {
			err = g.Error(err, "Could not perform upsert from temp")
			return err
		}
		return nil
	}

	var err error
	err = g.Error(err, "unsupported transfer mode: %s", cfg.Mode)
	return err
}

func transferBySwappingTables(tgtConn database.Connection, tableTmp, targetTable database.Table) error {
	g.Debug("Swapping temporary table %s with target table %s", tableTmp.FullName(), targetTable.FullName())
	if err := tgtConn.SwapTable(tableTmp.FullName(), targetTable.FullName()); err != nil {
		err = g.Error(err, "could not swap tables %s to %s", tableTmp.FullName(), targetTable.FullName())
		return err
	}
	return nil
}

func truncateTable(t *TaskExecution, tgtConn database.Connection, tableName string) error {
	truncSQL := g.R(
		tgtConn.GetTemplateValue("core.truncate_table"),
		"table", tableName,
	)
	if _, err := tgtConn.Exec(truncSQL); err != nil {
		err = g.Error(err, "Could not truncate table ", tableName)
		return err
	}

	t.SetProgress("truncated table " + tableName)
	return nil
}

func performUpsert(tgtConn database.Connection, tableTmp, targetTable database.Table, cfg *Config) error {
	tgtPrimaryKey := cfg.Source.PrimaryKey()
	if cc := cfg.Target.Options.ColumnCasing; cc != nil && *cc != SourceColumnCasing {
		for i, pk := range tgtPrimaryKey {
			tgtPrimaryKey[i] = applyColumnCasing(pk, *cc == SnakeColumnCasing, tgtConn.GetType())
		}
	}
	g.Debug("Performing upsert from temporary table %s to target table %s with primary keys %v",
		tableTmp.FullName(), targetTable.FullName(), tgtPrimaryKey)
	rowAffCnt, err := tgtConn.Upsert(tableTmp.FullName(), targetTable.FullName(), tgtPrimaryKey)
	if err != nil {
		err = g.Error(err, "Could not perform upsert from temp")
		return err
	}
	if rowAffCnt > 0 {
		g.DebugLow("%d TOTAL INSERTS / UPDATES", rowAffCnt)
	}
	return nil
}

func executeSQL(t *TaskExecution, tgtConn database.Connection, sqlStatements *string, stage string) error {
	if sqlStatements == nil || *sqlStatements == "" {
		return nil
	}

	t.SetProgress(fmt.Sprintf("executing %s-sql", stage))
	if _, err := tgtConn.ExecMulti(*sqlStatements); err != nil {
		err = g.Error(err, "Error executing %s-sql", stage)
		return err
	}
	return nil
}
