package sling

import (
	"math"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

func createSchemaIfNotExists(conn database.Connection, schemaName string) (created bool, err error) {
	if g.In(conn.GetType(), dbio.TypeDbSQLite, dbio.TypeDbD1) {
		return
	}

	// check schema existence
	schemasData, err := conn.GetSchemas()
	if err != nil {
		return false, g.Error(err, "Error getting schemas")
	}

	schemas := schemasData.ColValuesStr(0)
	if schemaName == "" {
		schemaName = conn.GetProp("schema")
		if schemaName == "" {
			return false, g.Error("did not specify schema. Please specify schema in object name.")
		}

		// qualify
		dummy, _ := database.ParseTableName(schemaName+".dummy", conn.GetType())
		schemaName = dummy.Schema
	}

	if !lo.Contains(schemas, schemaName) {
		sql := g.R(
			conn.Template().Value("core.create_schema"),
			"schema", conn.Quote(schemaName),
		)
		_, err = conn.Exec(sql)
		if err != nil {
			return false, g.Error(err, "Error creating schema %s", conn.Quote(schemaName))
		}
		created = true
	}

	return created, nil
}

func createTableIfNotExists(conn database.Connection, data iop.Dataset, table *database.Table, temp bool) (created bool, err error) {

	// check table existence
	exists, err := conn.TableExists(*table)
	if err != nil {
		return false, g.Error(err, "Error checking table "+table.FullName())
	} else if exists {
		return false, nil
	}

	// create schema if not exist
	_, err = createSchemaIfNotExists(conn, table.Schema)
	if err != nil {
		return false, g.Error(err, "Error checking & creating schema "+table.Schema)
	}

	table.DDL, err = conn.GenerateDDL(*table, data, temp)
	if err != nil {
		return false, g.Error(err, "Could not generate DDL for "+table.FullName())
	}

	_, err = conn.ExecMulti(table.DDL)
	if err != nil {
		errorFilterTableExists := conn.GetTemplateValue("variable.error_filter_table_exists")
		if errorFilterTableExists != "" && strings.Contains(err.Error(), errorFilterTableExists) {
			return false, g.Error(err, "Error creating table %s as it already exists", table.FullName())
		}
		return false, g.Error(err, "Error creating table "+table.FullName())
	}

	return true, nil
}

func pullTargetTableColumns(cfg *Config, tgtConn database.Connection, force bool) (cols iop.Columns, err error) {
	if len(cfg.Target.columns) == 0 || force {
		cfg.Target.columns, err = tgtConn.GetColumns(cfg.Target.Object)
		if err != nil {
			err = g.Error(err, "could not get column list for "+cfg.Target.Object)
			return
		}
	}
	return cfg.Target.columns, nil
}

func pullTargetTempTableColumns(cfg *Config, tgtConn database.Connection, force bool) (cols iop.Columns, err error) {
	if len(cfg.Target.columns) == 0 || force {
		cfg.Target.columns, err = tgtConn.GetColumns(cfg.Target.Options.TableTmp)
		if err != nil {
			err = g.Error(err, "could not get column list for "+cfg.Target.Options.TableTmp)
			return
		}
	}
	return cfg.Target.columns, nil
}

func insertFromTemp(cfg *Config, tgtConn database.Connection) (err error) {
	// insert
	tmpColumns, err := tgtConn.GetColumns(cfg.Target.Options.TableTmp)
	if err != nil {
		err = g.Error(err, "could not get column list for "+cfg.Target.Options.TableTmp)
		return
	}

	tgtColumns, err := pullTargetTableColumns(cfg, tgtConn, true)
	if err != nil {
		err = g.Error(err, "could not get column list for "+cfg.Target.Object)
		return
	}

	// if tmpColumns are dummy fields, simply match the target column names
	if iop.IsDummy(tmpColumns) && len(tmpColumns) == len(tgtColumns) {
		for i, col := range tgtColumns {
			tmpColumns[i].Name = col.Name
		}
	}

	tgtCols, err := tgtConn.ValidateColumnNames(tgtColumns, tmpColumns.Names())
	if err != nil {
		err = g.Error(err, "columns mismatched")
		return
	}

	tgtFields := tgtConn.GetType().QuoteNames(tgtCols.Names()...)
	srcFields := tgtConn.CastColumnsForSelect(tmpColumns, tgtColumns)

	srcTable, err := database.ParseTableName(cfg.Target.Options.TableTmp, tgtConn.GetType())
	if err != nil {
		err = g.Error(err, "unable to parse tmp table name")
		return
	}

	tgtTable, err := database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
	if err != nil {
		err = g.Error(err, "unable to parse tmp table name")
		return
	}

	sql := g.R(
		tgtConn.Template().Core["insert_from_table"],
		"tgt_table", tgtTable.FullName(),
		"src_table", srcTable.FullName(),
		"tgt_fields", strings.Join(tgtFields, ", "),
		"src_fields", strings.Join(srcFields, ", "),
	)
	_, err = tgtConn.Exec(sql)
	if err != nil {
		err = g.Error(err, "Could not execute SQL: "+sql)
		return
	}
	g.Debug("inserted rows into %s from temp table %s", cfg.Target.Object, cfg.Target.Options.TableTmp)
	return
}

var (
	getIncrementalValueViaState = func(*TaskExecution) (err error) {
		g.Warn("use the official release of sling-cli to use incremental via sling state")
		return nil
	}

	setIncrementalValueViaState = func(*TaskExecution) (err error) {
		g.Warn("use the official release of sling-cli to use incremental via sling state")
		return nil
	}
)

func getIncrementalValueViaDB(cfg *Config, tgtConn database.Connection, srcConnType dbio.Type) (err error) {
	// check if already set from override
	if cfg.IncrementalValStr != "" {
		if cfg.IncrementalVal == nil {
			cfg.IncrementalVal = strings.Trim(cfg.IncrementalValStr, "\"'")
		}
		return
	}

	// get table columns type for table creation if not exists
	// in order to get max value
	// does table exists?
	// get max value from key_field
	table, err := database.ParseTableName(cfg.Target.Object, tgtConn.GetType())
	if err != nil {
		err = g.Error(err, "could not parse target table name: %s", cfg.Target.Object)
		return
	}

	tgtUpdateKey := cfg.Source.UpdateKey
	if cc := cfg.Target.Options.ColumnCasing; cc != nil {
		tgtUpdateKey = cc.Apply(tgtUpdateKey, tgtConn.GetType())
	}

	// get target columns to match update-key
	// in case column casing needs adjustment
	targetCols, _ := pullTargetTableColumns(cfg, tgtConn, false)
	if updateCol := targetCols.GetColumn(tgtUpdateKey); updateCol != nil && updateCol.Name != "" {
		tgtUpdateKey = updateCol.Name // overwrite with correct casing
	} else if len(targetCols) == 0 {
		return // target columns does not exist
	}

	// get target columns to match update-key
	// in case column casing needs adjustment
	var maxCol iop.Column
	cfg.IncrementalVal, maxCol, err = tgtConn.GetMaxValue(table, tgtUpdateKey)
	if err != nil {
		return g.Error(err, "could not get incremental value")
	}

	// oracle's DATE type is mapped to datetime, but needs to use the TO_DATE function
	if maxCol.DbType == "DATE" && tgtConn.GetType() == dbio.TypeDbOracle {
		maxCol.Type = iop.DateType // force date type
	}

	cfg.IncrementalValStr = iop.FormatValue(cfg.IncrementalVal, maxCol.Type, srcConnType)

	return
}

func getRate(cnt uint64) string {
	return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
}

// GetSQLText process source sql file / text
func GetSQLText(sqlStringPath string) (string, error) {
	if g.PathExists(sqlStringPath) || strings.HasPrefix(sqlStringPath, "file://") {
		sqlStringPath = strings.TrimPrefix(sqlStringPath, "file://")
		bytes, err := os.ReadFile(sqlStringPath)
		if err != nil {
			return "", g.Error(err, "Could not ReadFile: "+sqlStringPath)
		}
		return string(bytes), nil
	}

	return sqlStringPath, nil
}

func setStage(value string) {
	env.SetTelVal("stage", value)
}
