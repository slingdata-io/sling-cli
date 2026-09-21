package sling

import (
	"bufio"
	"os"
	"strconv"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// ReadFromDB reads from a source database
func (t *TaskExecution) ReadFromDB(cfg *Config, srcConn database.Connection) (df *iop.Dataflow, err error) {

	setStage("3 - prepare-dataflow")

	selectFields := []string{"*"}
	sTable, err := t.GetSourceTable(srcConn)
	if err != nil {
		err = g.Error(err, "Could not parse source stream text")
		return t.df, err
	}

	// get source columns
	st := sTable

	// so we get the columns, and not change the orig SQL
	st.SQL = g.R(st.SQL, "incremental_where_cond", "1=1")
	st.SQL = g.R(st.SQL, "incremental_value", "null")
	st.SQL = g.R(st.SQL, "start_value", "null")
	st.SQL = g.R(st.SQL, "end_value", "null")
	st.SQL = g.R(st.SQL, "fields", "*")

	sTable.Columns, err = srcConn.GetSQLColumns(st)
	if err != nil {
		err = g.Error(err, "Could not get source columns")
		return t.df, err
	}

	// Fetch extended column metadata when schema migration is enabled
	if sm := database.NewSchemaMigrator(nil); sm.IsEnabled() {
		sTable.Columns, err = sm.GetExtendedColumnMetadata(srcConn, sTable, sTable.Columns)
		if err != nil {
			err = g.Error(err, "Could not get extended column metadata for schema migration")
			return
		}
	}

	cfg.Source.table = sTable

	if len(cfg.Source.Select) > 0 {
		// Normalize select expressions
		rawSelect := lo.Map(cfg.Source.Select, func(f string, i int) string {
			original, alias, isExclude, _ := iop.ParseSelectExpr(f)
			if isExclude {
				return f // exclusions handled by ApplySelect / exclude-only path
			}
			// Pass through `*` and globs untouched so ApplySelect can expand.
			if original == "*" || strings.Contains(original, "*") {
				return f
			}
			// Lookup the original column for case correction.
			col := sTable.Columns.GetColumn(srcConn.Unquote(original))
			if col != nil {
				if alias != "" {
					return col.Name + " as " + alias
				}
				return col.Name
			}
			return f
		})

		excluded := lo.Filter(cfg.Source.Select, func(f string, i int) bool {
			return strings.HasPrefix(f, "-")
		})

		if len(excluded) == len(cfg.Source.Select) && len(excluded) > 0 {
			// exclude-only select: emit all source columns minus excluded
			includedCols := lo.Filter(sTable.Columns, func(c iop.Column, i int) bool {
				colNameLower := strings.ToLower(c.Name)
				for _, exField := range excluded {
					exField = srcConn.Unquote(strings.TrimPrefix(exField, "-"))
					exFieldLower := strings.ToLower(exField)
					// Use glob matching to support patterns like "address_*"
					if iop.MatchesSelectGlob(colNameLower, exFieldLower) {
						return false
					}
				}
				return true
			})

			if len(includedCols) == 0 {
				return t.df, g.Error("All available columns were excluded")
			}
			selectFields = iop.Columns(includedCols).Names()
		} else if len(excluded) > 0 && len(excluded) != len(cfg.Source.Select) {
			// mixed include/exclude was historically rejected.
			hasWildcardInclude := false
			for _, expr := range cfg.Source.Select {
				if strings.HasPrefix(expr, "-") {
					continue
				}
				field, _, _, _ := iop.ParseSelectExpr(expr)
				if field == "*" || strings.Contains(field, "*") {
					hasWildcardInclude = true
					break
				}
			}
			if !hasWildcardInclude {
				return t.df, g.Error("All specified select columns must be excluded with prefix '-'. Cannot do partial exclude.")
			}
			// expand against the source column list to get the final list
			expanded, expErr := iop.ApplySelectExprs(iop.Columns(sTable.Columns).Names(), rawSelect)
			if expErr != nil {
				return t.df, g.Error(expErr, "could not apply select")
			}
			if len(expanded) == 0 {
				return t.df, g.Error("select expression produced no columns")
			}
			selectFields = expanded
		} else {
			// include-only (with or without `*`/globs).
			expanded, expErr := iop.ApplySelectExprs(iop.Columns(sTable.Columns).Names(), rawSelect)
			if expErr != nil {
				return t.df, g.Error(expErr, "could not apply select")
			}
			if len(expanded) == 0 {
				return t.df, g.Error("select expression produced no columns")
			}
			selectFields = expanded
		}
	}

	// geometry columns of some sources are unusable through their drivers;
	// make them return hex WKB so duckdb can parse them at export
	selectFields = sTable.GeometryWKBFields(selectFields)

	if t.isIncrementalChangeTracking() {
		sqlServerConn, ok := srcConn.(*database.MsSQLServerConn)
		if !ok {
			return t.df, g.Error("change tracking is currently only supported for SQL Server sources, got: %s", srcConn.GetType())
		}

		// Change tracking requires a physical SQL Server table
		if sTable.Name == "" || sTable.IsQuery() {
			return t.df, g.Error("change tracking requires a physical SQL Server table, got: %s", t.Config.Source.Stream)
		}

		// Source limit is not supported with change tracking to prevent partial syncs and watermark loss
		if cfg.Source.Limit() > 0 {
			return t.df, g.Error("source limit is not supported with change tracking as it causes partial syncs and watermark loss")
		}

		// 1. Resolve & verify Primary Key from SQL Server
		detectedPKs, err := sqlServerConn.GetTablePKColumns(sTable)
		if err != nil {
			return t.df, g.Error(err, "failed to discover primary key for table %s", sTable.FDQN())
		}
		if len(detectedPKs) == 0 {
			return t.df, g.Error("SQL Server Change Tracking requires a primary key, but none was found for table %s", sTable.FDQN())
		}

		configuredPKs := cfg.Source.PrimaryKey()
		if len(configuredPKs) > 0 {
			detectedMap := make(map[string]bool)
			for _, k := range detectedPKs {
				detectedMap[strings.ToLower(k)] = true
			}
			for _, k := range configuredPKs {
				if !detectedMap[strings.ToLower(k)] {
					return t.df, g.Error("configured primary key %v does not match SQL Server primary key %v for table %s", configuredPKs, detectedPKs, sTable.FDQN())
				}
			}
			if len(configuredPKs) != len(detectedPKs) {
				return t.df, g.Error("configured primary key %v does not match SQL Server primary key %v for table %s", configuredPKs, detectedPKs, sTable.FDQN())
			}
		}
		pks := detectedPKs
		cfg.Source.PrimaryKeyI = pks

		// Resolve table columns if not yet populated
		if len(sTable.Columns) == 0 {
			cols, err := sqlServerConn.GetColumns(sTable.FDQN())
			if err != nil {
				return t.df, g.Error(err, "failed to get columns for table %s", sTable.FDQN())
			}
			if len(cols) == 0 {
				return t.df, g.Error("table %s has no columns", sTable.FDQN())
			}
			sTable.Columns = cols
		}

		// Spatial columns (geometry, geography) are not supported with Change Tracking
		for _, col := range sTable.Columns {
			if col.Type.IsGeometry() || strings.EqualFold(col.Type.String(), "geometry") || strings.EqualFold(col.Type.String(), "geography") {
				return t.df, g.Error("SQL Server Change Tracking does not currently support spatial column '%s' in table %s", col.Name, sTable.FDQN())
			}
		}

		// 2. Get change tracking versions from SQL Server (validates CT enabled on DB and table)
		currVer, minValidVer, err := sqlServerConn.GetChangeTrackingVersions(sTable)
		if err != nil {
			return t.df, err
		}

		// 3. Determine last sync version
		hasLastVer := cfg.IncrementalValStr != "" && cfg.IncrementalValStr != "null"
		var lastVer int64 = 0
		if hasLastVer {
			trimmed := strings.Trim(cfg.IncrementalValStr, "\"' \t\r\n")
			parsed, parseErr := strconv.ParseInt(trimmed, 10, 64)
			if parseErr != nil || parsed < 0 {
				return t.df, g.Error("invalid change tracking watermark value %q: must be a valid non-negative integer", cfg.IncrementalValStr)
			}
			lastVer = parsed
		}

		isSnapshot := false
		if !hasLastVer {
			isSnapshot = true
		} else if lastVer > currVer {
			if cfg.Source.AutoFullRefresh() {
				g.Warn("change tracking version %d for table %s is newer than current database version %d (database may have been restored or reset); performing auto full refresh snapshot", lastVer, sTable.FDQN(), currVer)
				g.Warn("change tracking auto full refresh performs an insert/upsert snapshot; deleted rows from source during retention expiration/reset are not automatically deleted from target. A manual full refresh or truncate is recommended if source deletes occurred.")
				isSnapshot = true
			} else {
				return t.df, g.Error("change tracking version %d for table %s is newer than current database version %d (database may have been restored or change tracking was reset). A full refresh is required", lastVer, sTable.FDQN(), currVer)
			}
		} else if lastVer < minValidVer {
			if cfg.Source.AutoFullRefresh() {
				g.Warn("change tracking version %d for table %s is older than minimum valid version %d; performing auto full refresh snapshot", lastVer, sTable.FDQN(), minValidVer)
				g.Warn("change tracking auto full refresh performs an insert/upsert snapshot; deleted rows from source during retention expiration/reset are not automatically deleted from target. A manual full refresh or truncate is recommended if source deletes occurred.")
				isSnapshot = true
			} else {
				return t.df, g.Error("change tracking version %d for table %s is older than minimum valid version %d (retention expired). A full refresh is required", lastVer, sTable.FDQN(), minValidVer)
			}
		}

		if isSnapshot {
			t.SetProgress("reading snapshot for change tracking (version: %d)", currVer)
			sTable.SQL = sqlServerConn.BuildChangeTrackingSnapshotSQL(sTable, pks, selectFields, currVer, cfg.Source.Where)
		} else {
			t.SetProgress("reading change tracking delta (versions %d -> %d)", lastVer, currVer)
			sTable.SQL = sqlServerConn.BuildChangeTrackingSelectSQL(sTable, pks, selectFields, lastVer, currVer, cfg.Source.Where)
		}

		// Clear Where and reset selectFields since sTable.SQL already handled them
		cfg.Source.Where = ""
		selectFields = []string{"*"}

		// Stage pending watermark version (promoted only upon successful write)
		t.pendingCTVersion = currVer

		// Target merge strategy validation and defaulting
		if cfg.Target.Options == nil {
			cfg.Target.Options = &TargetOptions{}
		}
		if cfg.Target.Options.MergeStrategy == nil {
			strategy := database.MergeStrategyChangeCapture
			cfg.Target.Options.MergeStrategy = &strategy
		} else if *cfg.Target.Options.MergeStrategy != database.MergeStrategyChangeCapture && *cfg.Target.Options.MergeStrategy != database.MergeStrategyChangeCaptureSoft {
			return t.df, g.Error("change tracking requires target merge_strategy 'change_capture' or 'change_capture_soft', got: %s", *cfg.Target.Options.MergeStrategy)
		}
	} else if t.isIncrementalWithUpdateKey() || t.hasStateWithUpdateKey() || t.Config.Mode == BackfillMode || t.Config.IsFullRefreshWithRange() || t.Config.IsTruncateWithRange() || t.Config.IsIncrementalWithRange() {
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
		if incValStr := cfg.IncrementalValStr; incValStr != "" {
			if srcConn.GetType().IsNoSQL() {
				// escape double quote since this uses JSON
				incValStr = strings.ReplaceAll(incValStr, `"`, `\"`)
			}

			incrementalWhereCond = g.R(
				srcConn.GetTemplateValue("core.incremental_where"),
				"update_key", srcConn.Quote(cfg.Source.UpdateKey),
				"value", incValStr,
				"gt", lo.Ternary(t.Config.IncrementalGTE, ">=", ">"),
			)
		} else {
			// allows the use of coalesce in custom SQL using {incremental_value}
			// this will be null when target table does not exists
			cfg.IncrementalValStr = "null"
		}

		if t.Config.Mode == BackfillMode || t.Config.IsFullRefreshWithRange() || t.Config.IsTruncateWithRange() || t.Config.IsIncrementalWithRange() {
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
				original, alias, _, _ := iop.ParseSelectExpr(sf)
				col := sTable.Columns.GetColumn(srcConn.Unquote(original))
				if col != nil {
					if alias != "" {
						return srcConn.Quote(col.Name) + " as " + srcConn.Quote(alias)
					}
					return srcConn.Quote(col.Name) // apply quotes if match
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

	// if {fields} placeholder is used, replace it with selected fields to avoid double wrapping
	if strings.Contains(sTable.SQL, "{fields}") {
		sFields := lo.Map(selectFields, func(sf string, i int) string {
			original, alias, _, _ := iop.ParseSelectExpr(sf)
			col := sTable.Columns.GetColumn(srcConn.Unquote(original))
			if col != nil {
				if alias != "" {
					return srcConn.Quote(col.Name) + " as " + srcConn.Quote(alias)
				}
				return srcConn.Quote(col.Name) // apply quotes if match
			}
			return sf
		})
		sTable.SQL = g.R(sTable.SQL, "fields", strings.Join(sFields, ", "))
		// Reset selectFields to prevent Select() from wrapping the query
		selectFields = []string{"*"}
	}

	// For definition-only mode, inject WHERE 1=0 to avoid reading data
	if cfg.Mode == DefinitionOnlyMode {
		cfg.Source.Where = "1=0"
	}

	// construct select statement for selected fields or where condition
	if !t.isIncrementalChangeTracking() && (len(selectFields) > 1 || selectFields[0] != "*" || cfg.Source.Where != "" || cfg.Source.Limit() > 0) {
		if sTable.SQL != "" && !cfg.SrcConn.Type.IsNoSQL() && !strings.Contains(sTable.SQL, "{fields}") {
			// If sTable.SQL is already a query (e.g. from incremental template or custom SQL),
			// it means the field selection (cfg.Source.Select) is assumed to be handled by its construction.
			selectFields = []string{"*"}
		}

		sTable.SQL = sTable.Select(database.SelectOptions{
			Fields: selectFields,
			Where:  cfg.Source.Where,
			Limit:  lo.Ternary(cfg.Source.Limit() > 0, g.Ptr(cfg.Source.Limit()), nil),
			Offset: cfg.Source.Offset(),
		})
	}

	// set constraints
	cols, _ := cfg.ColumnsPrepared()
	for _, col := range cols {
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
	options := t.getSourceOptionsMap()
	options["METADATA"] = g.Marshal(metadata)

	if t.Config.HasIncrementalVal() && !t.Config.IsFileStreamWithStateAndParts() {
		// file stream incremental mode
		if g.In(t.Config.Source.UpdateKey, env.ReservedFields.LoadedAt, env.ReservedFields.SyncedAt) {
			options["SLING_FS_TIMESTAMP"] = strings.TrimSuffix(strings.TrimPrefix(t.Config.IncrementalValStr, "'"), "'") // remove quotes
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
			g.MapToKVArr(g.CastToMapString(options))...,
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

		// limit when definition-only
		if cfg.Mode == DefinitionOnlyMode {
			fsCfg.SchemaOnly = true
			fsCfg.Limit = iop.SampleSize
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

				fsCfg.FileSelect = rangeURIs

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
		stream, err = filesys.MakeDatastream(bufio.NewReader(os.Stdin), g.CastToMapString(options))
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
		Jq:          g.PtrVal(cfg.Source.Options.Jq),
		Select:      cfg.Source.Select,
		PrimaryKey:  cfg.Source.PrimaryKey(),
		Limit:       cfg.Source.Limit(),
		Metadata:    t.setGetMetadata(),
		Mode:        strings.ToLower(string(cfg.Mode)),
		Range:       g.PtrVal(t.Config.Source.Options.Range),
		DsConfigMap: t.getSourceOptionsMap(),
	}

	if cfg.Mode == DefinitionOnlyMode {
		sCfg.SchemaOnly = true
		sCfg.Limit = iop.SampleSize
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
