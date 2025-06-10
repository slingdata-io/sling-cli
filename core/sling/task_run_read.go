package sling

import (
	"bufio"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// ReadFromDB reads from a source database
func (t *TaskExecution) ReadFromDB(cfg *Config, srcConn database.Connection) (df *iop.Dataflow, err error) {

	setStage("3 - prepare-dataflow")

	selectFields := []string{"*"}
	sTable, err := t.GetSourceTable()
	if err != nil {
		err = g.Error(err, "Could not parse source stream text")
		return t.df, err
	}

	// get source columns
	st := sTable
	st.SQL = g.R(st.SQL, "incremental_where_cond", "1=1") // so we get the columns, and not change the orig SQL
	st.SQL = g.R(st.SQL, "incremental_value", "null")     // so we get the columns, and not change the orig SQL
	sTable.Columns, err = srcConn.GetSQLColumns(st)
	if err != nil {
		err = g.Error(err, "Could not get source columns")
		return t.df, err
	}

	if len(cfg.Source.Select) > 0 {
		selectFields = lo.Map(cfg.Source.Select, func(f string, i int) string {
			// lookup column name
			col := sTable.Columns.GetColumn(srcConn.GetType().Unquote(f))
			if col != nil {
				return col.Name
			}
			return f
		})

		excluded := lo.Filter(cfg.Source.Select, func(f string, i int) bool {
			return strings.HasPrefix(f, "-")
		})

		if len(excluded) > 0 {
			if len(excluded) != len(cfg.Source.Select) {
				return t.df, g.Error("All specified select columns must be excluded with prefix '-'. Cannot do partial exclude.")
			}

			includedCols := lo.Filter(sTable.Columns, func(c iop.Column, i int) bool {
				for _, exField := range excluded {
					exField = srcConn.GetType().Unquote(strings.TrimPrefix(exField, "-"))
					if strings.EqualFold(c.Name, exField) {
						return false
					}
				}
				return true
			})

			if len(includedCols) == 0 {
				return t.df, g.Error("All available columns were excluded")
			}
			selectFields = iop.Columns(includedCols).Names()
		}
	}

	if t.isIncrementalWithUpdateKey() || t.hasStateWithUpdateKey() || t.Config.Mode == BackfillMode {
		// default true value
		incrementalWhereCond := "1=1"

		// get source columns to match update-key
		// in case column casing needs adjustment
		updateCol := sTable.Columns.GetColumn(cfg.Source.UpdateKey)
		if updateCol == nil {
			return df, g.Error("did not find update_key: %s", cfg.Source.UpdateKey)
		} else if updateCol.Name != "" {
			cfg.Source.UpdateKey = updateCol.Name // overwrite with correct casing
		}

		// select only records that have been modified after last max value
		if cfg.IncrementalValStr != "" {
			incrementalWhereCond = g.R(
				srcConn.GetTemplateValue("core.incremental_where"),
				"update_key", srcConn.Quote(cfg.Source.UpdateKey),
				"value", cfg.IncrementalValStr,
				"gt", lo.Ternary(t.Config.IncrementalGTE, ">=", ">"),
			)
		} else {
			// allows the use of coalesce in custom SQL using {incremental_value}
			// this will be null when target table does not exists
			cfg.IncrementalValStr = "null"
		}

		if t.Config.Mode == BackfillMode {
			rangeArr := strings.Split(*cfg.Source.Options.Range, ",")
			startValue := rangeArr[0]
			endValue := rangeArr[1]

			// oracle's DATE type is mapped to datetime, but needs to use the TO_DATE function
			isOracleDate := updateCol.DbType == "DATE" && srcConn.GetType() == dbio.TypeDbOracle

			if updateCol.IsDate() || isOracleDate {
				timestampTemplate := srcConn.GetTemplateValue("variable.date_layout_str")
				startValue = g.R(timestampTemplate, "value", startValue)
				endValue = g.R(timestampTemplate, "value", endValue)
			} else if updateCol.Type == iop.TimestampzType {
				timestampTemplate := srcConn.GetTemplateValue("variable.timestampz_layout_str")
				startValue = g.R(timestampTemplate, "value", startValue)
				endValue = g.R(timestampTemplate, "value", endValue)
			} else if updateCol.IsDatetime() {
				timestampTemplate := srcConn.GetTemplateValue("variable.timestamp_layout_str")
				startValue = g.R(timestampTemplate, "value", startValue)
				endValue = g.R(timestampTemplate, "value", endValue)
			} else if updateCol.IsString() {
				startValue = `'` + startValue + `'`
				endValue = `'` + endValue + `'`
			}

			incrementalWhereCond = g.R(
				srcConn.GetTemplateValue("core.backfill_where"),
				"update_key", srcConn.Quote(cfg.Source.UpdateKey),
				"start_value", startValue,
				"end_value", endValue,
			)
		}

		if sTable.SQL == "" {
			key := lo.Ternary(
				cfg.Source.Limit() > 0,
				lo.Ternary(
					cfg.Source.Offset() > 0,
					"core.incremental_select_limit_offset",
					"core.incremental_select_limit",
				),
				"core.incremental_select",
			)

			sFields := lo.Map(selectFields, func(sf string, i int) string {
				col := sTable.Columns.GetColumn(srcConn.GetType().Unquote(sf))
				if col != nil {
					return srcConn.GetType().Quote(col.Name) // apply quotes if match
				}
				return sf
			})

			sTable.SQL = g.R(
				srcConn.GetTemplateValue(key),
				"fields", strings.Join(sFields, ", "),
				"table", sTable.FDQN(),
				"incremental_where_cond", incrementalWhereCond,
				"update_key", srcConn.Quote(cfg.Source.UpdateKey),
				"incremental_value", cfg.IncrementalValStr,
				"table_name", sTable.Name,
				"table_schema", sTable.Schema,
				"fields_array", g.Marshal(sFields),
			)
		} else {
			if g.In(t.Config.Mode, IncrementalMode, BackfillMode) && !(strings.Contains(sTable.SQL, "{incremental_where_cond}") || strings.Contains(sTable.SQL, "{incremental_value}")) {
				err = g.Error("Since using %s mode + custom SQL, with an `update_key`, the SQL text needs to contain a placeholder: {incremental_where_cond} or {incremental_value}. See https://docs.slingdata.io for help.", t.Config.Mode)
				return t.df, err
			}

			sTable.SQL = g.R(
				sTable.SQL,
				"incremental_where_cond", incrementalWhereCond,
				"update_key", srcConn.Quote(cfg.Source.UpdateKey),
				"incremental_value", cfg.IncrementalValStr,
			)
		}

		// fill in the where clause
		cfg.Source.Where = g.R(
			cfg.Source.Where,
			"incremental_where_cond", incrementalWhereCond,
			"update_key", srcConn.Quote(cfg.Source.UpdateKey),
			"incremental_value", cfg.IncrementalValStr,
		)
	}

	if srcConn.GetType() == dbio.TypeDbBigTable {
		srcConn.SetProp("start_time", t.Config.IncrementalValStr)
	}

	sTable.SQL = g.R(sTable.SQL, "incremental_where_cond", "1=1") // if running non-incremental mode
	sTable.SQL = g.R(sTable.SQL, "incremental_value", "null")     // if running non-incremental mode

	// construct select statement for selected fields or where condition
	if len(selectFields) > 1 || selectFields[0] != "*" || cfg.Source.Where != "" || cfg.Source.Limit() > 0 {
		if sTable.SQL != "" {
			// If sTable.SQL is already a query (e.g. from incremental template or custom SQL),
			// it means the field selection (cfg.Source.Select) is assumed to be handled by its construction.
			selectFields = []string{"*"}
		}

		sTable.SQL = sTable.Select(database.SelectOptions{
			Fields: selectFields,
			Where:  cfg.Source.Where,
			Limit:  cfg.Source.Limit(),
			Offset: cfg.Source.Offset(),
		})
	}

	// set constraints
	for _, col := range cfg.ColumnsPrepared() {
		if c := sTable.Columns.GetColumn(col.Name); c != nil {
			sTable.Columns[c.Position-1].Constraint = col.Constraint
		}
	}

	df, err = srcConn.BulkExportFlow(sTable)
	if err != nil {
		err = g.Error(err, "Could not BulkExportFlow")
		return t.df, err
	}

	err = t.setColumnKeys(df)
	if err != nil {
		err = g.Error(err, "Could not set column keys")
		return t.df, err
	}

	g.Trace("%#v", df.Columns.Types())
	setStage("3 - dataflow-stream")

	return
}

// ReadFromFile reads from a source file
func (t *TaskExecution) ReadFromFile(cfg *Config) (df *iop.Dataflow, err error) {

	setStage("3 - prepare-dataflow")

	// sets metadata
	metadata := t.setGetMetadata()

	var stream *iop.Datastream
	options := t.getOptionsMap()
	options["METADATA"] = g.Marshal(metadata)

	if t.Config.HasIncrementalVal() && !t.Config.IsFileStreamWithStateAndParts() {
		// file stream incremental mode
		if t.Config.Source.UpdateKey == slingLoadedAtColumn {
			options["SLING_FS_TIMESTAMP"] = t.Config.IncrementalValStr
			g.Debug(`file stream using file_sys_timestamp=%#v and update_key=%s`, t.Config.IncrementalValStr, t.Config.Source.UpdateKey)
		} else {
			options["SLING_INCREMENTAL_COL"] = t.Config.Source.UpdateKey
			options["SLING_INCREMENTAL_VAL"] = strings.TrimSuffix(strings.TrimPrefix(t.Config.IncrementalValStr, "'"), "'") // remove quotes
			g.Debug(`file stream using incremental_val=%#v and update_key=%s`, t.Config.IncrementalValStr, t.Config.Source.UpdateKey)
		}
	}

	if uri := cfg.SrcConn.URL(); uri != "" {
		// construct props by merging with options
		props := append(
			g.MapToKVArr(cfg.SrcConn.DataS()),
			g.MapToKVArr(g.ToMapString(options))...,
		)

		fs, err := filesys.NewFileSysClientFromURLContext(t.Context.Ctx, uri, props...)
		if err != nil {
			err = g.Error(err, "Could not obtain client for %s ", cfg.SrcConn.Type)
			return t.df, err
		}

		fsCfg := iop.FileStreamConfig{
			Select:           cfg.Source.Select,
			Limit:            cfg.Source.Limit(),
			SQL:              cfg.Source.Query,
			FileSelect:       cfg.Source.Files,
			IncrementalKey:   cfg.Source.UpdateKey,
			IncrementalValue: cfg.IncrementalValStr,
		}

		// format the uri if it has placeholders
		// determine uri if it has part fields, find first parent folder
		if t.Config.IsFileStreamWithStateAndParts() {
			mask := filesys.GetDeepestParent(uri) // mask is without glob symbols

			// if backfill mode, generate the range of uris to read from
			if t.Config.Mode == BackfillMode {
				rangeArr := strings.Split(*cfg.Source.Options.Range, ",")
				start, err := cast.ToTimeE(rangeArr[0])
				if err != nil {
					return df, g.Error(err, "invalid start timestamp value: %s", rangeArr[0])
				}
				end, err := cast.ToTimeE(rangeArr[1])
				if err != nil {
					return df, g.Error(err, "invalid end timestamp value: %s", rangeArr[1])
				}

				rangeURIs, err := iop.GeneratePartURIsFromRange(mask, cfg.Source.UpdateKey, start, end)
				if err != nil {
					return df, g.Error(err, "could not generate uris from range")
				}

				fsCfg.FileSelect = g.Ptr(rangeURIs)

				// set as end value
				cfg.IncrementalVal = end

			} else if cfg.IncrementalVal != nil {
				valueTime, err := cast.ToTimeE(cfg.IncrementalVal)
				if err != nil {
					return df, g.Error(err, "could not parse time incremental value: %#v", cfg.IncrementalVal)
				}

				uri = g.Rm(uri, iop.GetISO8601DateMap(valueTime))
				uri = g.Rm(uri, iop.GetPartitionDateMap(cfg.Source.UpdateKey, valueTime))
			} else {
				uri, err = filesys.GetFirstDatePartURI(fs, mask)
				if err != nil {
					return t.df, g.Error(err, "could not get first partition path")
				}

				// extract current incremental value
				cfg.IncrementalVal, err = iop.ExtractPartitionTimeValue(mask, uri)
				if err != nil {
					return t.df, g.Error(err, "could not extract time partition  incremental value")
				}
			}
			cfg.SrcConn.Data["url"] = uri // set compiled uri

			// unset fsCfg.IncrementalValue to not further filter.
			// since we're filtering at folder level
			fsCfg.IncrementalValue = ""
		}

		if ffmt := cfg.Source.Options.Format; ffmt != nil {
			fsCfg.Format = *ffmt
		}
		df, err = fs.ReadDataflow(uri, fsCfg)
		if err != nil {
			err = g.Error(err, "Could not FileSysReadDataflow for %s", cfg.SrcConn.Type)
			return t.df, err
		}
	} else {
		stream, err = filesys.MakeDatastream(bufio.NewReader(os.Stdin), g.ToMapString(options))
		if err != nil {
			err = g.Error(err, "Could not MakeDatastream")
			return t.df, err
		}
		df, err = iop.MakeDataFlow(stream.Split()...)
		if err != nil {
			err = g.Error(err, "Could not MakeDataFlow for Stdin")
			return t.df, err
		}
	}

	if len(df.Streams) == 0 {
		streamName := lo.Ternary(cfg.SrcConn.URL() == "", "stdin", cfg.SrcConn.URL())
		return df, g.Error("Could not read stream (%s)", streamName)
	} else if len(df.Columns) == 0 && !df.Streams[0].IsClosed() {
		return df, g.Error("Could not read columns")
	}

	err = t.setColumnKeys(df)
	if err != nil {
		err = g.Error(err, "Could not set column keys")
		return t.df, err
	}

	g.Trace("%#v", df.Columns.Types())
	setStage("3 - dataflow-stream")

	return
}

// ReadFromApi reads from a source api
func (t *TaskExecution) ReadFromApi(cfg *Config, srcConn *api.APIConnection) (df *iop.Dataflow, err error) {
	setStage("3 - prepare-dataflow")

	if cfg.Source.Options.Flatten == nil {
		cfg.Source.Options.Flatten = 1 // flatten level 1 by default
	}

	sCfg := api.APIStreamConfig{
		Flatten:     cfg.Source.Flatten(),
		JmesPath:    g.PtrVal(cfg.Source.Options.JmesPath),
		Select:      cfg.Source.Select,
		Limit:       cfg.Source.Limit(),
		Metadata:    t.setGetMetadata(),
		Mode:        strings.ToLower(string(cfg.Mode)),
		DsConfigMap: t.getOptionsMap(),
	}
	df, err = srcConn.ReadDataflow(cfg.StreamName, sCfg)
	if err != nil {
		err = g.Error(err, "Could not ReadDataflow for %s", cfg.SrcConn.Type)
		return t.df, err
	}

	return df, err
}

// setColumnKeys sets the column keys
func (t *TaskExecution) setColumnKeys(df *iop.Dataflow) (err error) {
	eG := g.ErrorGroup{}

	if t.Config.Source.HasPrimaryKey() {
		// set true PK only when StarRocks, we don't want to create PKs on target table implicitly
		if t.Config.Source.Type == dbio.TypeDbStarRocks {
			eG.Capture(df.Columns.SetKeys(iop.PrimaryKey, t.Config.Source.PrimaryKey()...))
		}
		eG.Capture(df.Columns.SetMetadata(iop.PrimaryKey.MetadataKey(), "source", t.Config.Source.PrimaryKey()...))
	}

	if t.Config.Source.HasUpdateKey() {
		eG.Capture(df.Columns.SetMetadata(iop.UpdateKey.MetadataKey(), "source", t.Config.Source.UpdateKey))
	}

	if tkMap := t.Config.Target.Options.TableKeys; tkMap != nil {
		for tableKey, keys := range tkMap {
			// ignore error if column is not found, it is set again later
			// see https://github.com/slingdata-io/sling-cli/issues/532
			_ = df.Columns.SetKeys(tableKey, keys...)
		}
	}

	return eG.Err()
}
