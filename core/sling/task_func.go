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
	"github.com/spf13/cast"
)

func createSchemaIfNotExists(conn database.Connection, schemaName string) (created bool, err error) {
	if conn.GetType() == dbio.TypeDbSQLite {
		return
	}

	// check schema existence
	schemasData, err := conn.GetSchemas()
	if err != nil {
		return false, g.Error(err, "Error getting schemas")
	}

	schemas := schemasData.ColValuesStr(0)
	schemas = lo.Map(schemas, func(v string, i int) string { return strings.ToLower(v) })
	schemaName = strings.ToLower(schemaName)
	if schemaName == "" {
		schemaName = strings.ToLower(conn.GetProp("schema"))
		if schemaName == "" {
			return false, g.Error("did not specify schema. Please specify schema in object name.")
		}
	}

	if !lo.Contains(schemas, schemaName) {
		_, err = conn.Exec(g.F("create schema %s", conn.Quote(schemaName)))
		if err != nil {
			return false, g.Error(err, "Error creating schema %s", conn.Quote(schemaName))
		}
		created = true
	}

	return created, nil
}

func createTableIfNotExists(conn database.Connection, data iop.Dataset, table database.Table) (created bool, err error) {

	// check table existence
	exists, err := database.TableExists(conn, table.FullName())
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

	if table.DDL == "" {
		table.DDL, err = conn.GenerateDDL(table, data, false)
		if err != nil {
			return false, g.Error(err, "Could not generate DDL for "+table.FullName())
		}
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

func pullSourceTableColumns(cfg *Config, srcConn database.Connection, table string) (cols iop.Columns, err error) {
	cfg.Source.columns, err = srcConn.GetColumns(table)
	if err != nil {
		err = g.Error(err, "could not get column list for "+table)
		return
	}
	return cfg.Source.columns, nil
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

	// TODO: need to validate the source table types are casted
	// into the target column type
	tgtFields, err := tgtConn.ValidateColumnNames(
		tgtColumns.Names(),
		tmpColumns.Names(),
		true,
	)
	if err != nil {
		err = g.Error(err, "columns mismatched")
		return
	}

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

func getIncrementalValue(cfg *Config, tgtConn database.Connection, srcConnVarMap map[string]string) (val string, err error) {
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
	if cc := cfg.Target.Options.ColumnCasing; cc != nil && *cc != SourceColumnCasing {
		tgtUpdateKey = applyColumnCasing(tgtUpdateKey, *cc == SnakeColumnCasing, tgtConn.GetType())
	}

	// get target columns to match update-key
	// in case column casing needs adjustment
	targetCols, _ := pullTargetTableColumns(cfg, tgtConn, false)
	if updateCol := targetCols.GetColumn(tgtUpdateKey); updateCol.Name != "" {
		tgtUpdateKey = updateCol.Name // overwrite with correct casing
	}

	sql := g.F(
		"select max(%s) as max_val from %s",
		tgtConn.Quote(tgtUpdateKey, false),
		table.FDQN(),
	)

	data, err := tgtConn.Query(sql)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "exist") ||
			strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "unknown") ||
			strings.Contains(errMsg, "no such table") ||
			strings.Contains(errMsg, "invalid object") {
			// table does not exists, will be create later
			// set val to blank for full load
			return "", nil
		}
		err = g.Error(err, "could not get max value for "+tgtUpdateKey)
		return
	}
	if len(data.Rows) == 0 {
		// table is empty
		// set val to blank for full load
		return "", nil
	}

	value := data.Rows[0][0]
	colType := data.Columns[0].Type
	if colType.IsDatetime() {
		val = g.R(
			srcConnVarMap["timestamp_layout_str"],
			"value", cast.ToTime(value).Format(srcConnVarMap["timestamp_layout"]),
		)
	} else if colType == iop.DateType {
		val = g.R(
			srcConnVarMap["date_layout_str"],
			"value", cast.ToTime(value).Format(srcConnVarMap["date_layout"]),
		)
	} else if colType.IsNumber() {
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

// getSQLText process source sql file / text
func getSQLText(sqlStringPath string) (string, error) {
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
