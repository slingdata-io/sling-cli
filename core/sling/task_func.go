package sling

import (
	"io/ioutil"
	"math"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/samber/lo"
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
		schemaName = conn.GetProp("schema")
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

func createTableIfNotExists(conn database.Connection, data iop.Dataset, tableName string, tableDDL string) (created bool, err error) {

	table, err := database.ParseTableName(tableName, conn.GetType())
	if err != nil {
		return false, g.Error(err, "could not parse table name: "+tableName)
	}

	// check table existence
	exists, err := database.TableExists(conn, tableName)
	if err != nil {
		return false, g.Error(err, "Error checking table "+tableName)
	} else if exists {
		return false, nil
	}

	// create schema if not exist
	_, err = createSchemaIfNotExists(conn, table.Schema)
	if err != nil {
		return false, g.Error(err, "Error checking & creating schema "+table.Schema)
	}

	if tableDDL == "" {
		tableDDL, err = conn.GenerateDDL(tableName, data, false)
		if err != nil {
			return false, g.Error(err, "Could not generate DDL for "+tableName)
		}
	}

	_, err = conn.ExecMulti(tableDDL)
	if err != nil {
		errorFilterTableExists := conn.GetTemplateValue("variable.error_filter_table_exists")
		if errorFilterTableExists != "" && strings.Contains(err.Error(), errorFilterTableExists) {
			return false, g.Error(err, "Error creating table %s as it already exists", tableName)
		}
		return false, g.Error(err, "Error creating table "+tableName)
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
	g.Debug("inserted rows into `%s` from temp table `%s`", cfg.Target.Object, cfg.Target.Options.TableTmp)
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

	sql := g.F(
		"select max(%s) as max_val from %s",
		tgtConn.Quote(cfg.Source.UpdateKey),
		table.FDQN(),
	)

	data, err := tgtConn.Query(sql)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "exist") ||
			strings.Contains(strings.ToLower(err.Error()), "not found") {
			// table does not exists, will be create later
			// set val to blank for full load
			return "", nil
		}
		err = g.Error(err, "could not get max value for "+cfg.Source.UpdateKey)
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
func getSQLText(filePath string) (string, error) {
	filePath = strings.TrimPrefix(filePath, "file://")
	_, err := os.Stat(filePath)
	if err != nil {
		return "", g.Error(err, "Could not find file -> "+filePath)
	}
	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", g.Error(err, "Could not ReadFile: "+filePath)
	}

	return string(bytes), nil
}
