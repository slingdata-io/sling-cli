package main

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/integrii/flaggy"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	dbioEnv "github.com/slingdata-io/sling-cli/core/dbio/env"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/slingdata-io/sling-cli/core/store"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	projectID = os.Getenv("SLING_PROJECT")
	headers   = map[string]string{
		"Content-Type":     "application/json",
		"Sling-Project-ID": projectID,
	}
	updateMessage = ""
	updateVersion = ""
)

func init() {
	// init sqlite
	store.InitDB()
}

func processRun(c *g.CliSC) (ok bool, err error) {
	ok = true
	cfg := &sling.Config{}
	replicationCfgPath := ""
	taskCfgStr := ""
	showExamples := false
	selectStreams := []string{}
	iterate := 1
	itNumber := 1

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			telemetryMap["error"] = g.F("panic occurred! %#v\n%s", r, string(debug.Stack()))
		}
	}()

	// determine if stdin data is piped
	// https://stackoverflow.com/a/26567513
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cfg.Options.StdIn = true
	}

	for k, v := range c.Vals {
		switch k {
		case "replication":
			telemetryMap["run_mode"] = "replication"
			replicationCfgPath = cast.ToString(v)
		case "config":
			telemetryMap["run_mode"] = "task"
			taskCfgStr = cast.ToString(v)
		case "src-conn":
			cfg.Source.Conn = cast.ToString(v)
		case "src-stream", "src-table", "src-sql", "src-file":
			cfg.StreamName = cast.ToString(v)
			cfg.Source.Stream = cast.ToString(v)
			if strings.Contains(cfg.Source.Stream, "://") {
				if _, ok := c.Vals["src-conn"]; !ok { // src-conn not specified
					cfg.Source.Conn = cfg.Source.Stream
				}
			}
		case "src-options":
			payload := cast.ToString(v)
			options, err := parsePayload(payload, true)
			if err != nil {
				return ok, g.Error(err, "invalid source options -> %s", payload)
			}

			err = g.JSONConvert(options, &cfg.Source.Options)
			if err != nil {
				return ok, g.Error(err, "invalid source options -> %s", payload)
			}
		case "tgt-conn":
			cfg.Target.Conn = cast.ToString(v)

		case "primary-key":
			cfg.Source.PrimaryKeyI = strings.Split(cast.ToString(v), ",")

		case "update-key":
			cfg.Source.UpdateKey = cast.ToString(v)

		case "limit":
			if cfg.Source.Options == nil {
				cfg.Source.Options = &sling.SourceOptions{}
			}
			cfg.Source.Options.Limit = g.Int(cast.ToInt(v))

		case "iterate":
			if cast.ToString(v) == "infinite" {
				iterate = -1
			} else if val := cast.ToInt(v); val > 0 {
				iterate = val
			} else {
				return ok, g.Error("invalid value for `iterate`")
			}
		case "range":
			if cfg.Source.Options == nil {
				cfg.Source.Options = &sling.SourceOptions{}
			}
			cfg.Source.Options.Range = g.String(cast.ToString(v))

		case "tgt-object", "tgt-table", "tgt-file":
			cfg.Target.Object = cast.ToString(v)
			if strings.Contains(cfg.Target.Object, "://") {
				if _, ok := c.Vals["tgt-conn"]; !ok { // tgt-conn not specified
					cfg.Target.Conn = cfg.Target.Object
				}
			}
		case "tgt-options":
			payload := cast.ToString(v)
			options, err := parsePayload(payload, true)
			if err != nil {
				return ok, g.Error(err, "invalid target options -> %s", payload)
			}

			err = g.JSONConvert(options, &cfg.Target.Options)
			if err != nil {
				return ok, g.Error(err, "invalid target options -> %s", payload)
			}
		case "env":
			payload := cast.ToString(v)
			env, err := parsePayload(payload, false)
			if err != nil {
				return ok, g.Error(err, "invalid env variable map -> %s", payload)
			}

			err = g.JSONConvert(env, &cfg.Env)
			if err != nil {
				return ok, g.Error(err, "invalid env variable map -> %s", payload)
			}
		case "stdout":
			cfg.Options.StdOut = cast.ToBool(v)
		case "mode":
			cfg.Mode = sling.Mode(cast.ToString(v))
		case "select":
			cfg.Source.Select = strings.Split(cast.ToString(v), ",")
		case "streams":
			selectStreams = strings.Split(cast.ToString(v), ",")
		case "debug":
			cfg.Options.Debug = cast.ToBool(v)
			if cfg.Options.Debug {
				os.Setenv("DEBUG", "LOW")
				env.SetLogger()
			}
		case "examples":
			showExamples = cast.ToBool(v)
		}
	}

	if showExamples {
		println(examples)
		return ok, nil
	}

	if replicationCfgPath != "" && taskCfgStr != "" {
		return ok, g.Error("cannot provide replication and task configuration. Choose one.")
	}

	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_CLI_ARGS", g.Marshal(os.Args[1:]))
	if os.Getenv("SLING_EXEC_ID") == "" {
		// set exec id if none provided
		os.Setenv("SLING_EXEC_ID", sling.NewExecID())
	}

	// check for update, and print note
	go checkUpdate(false)
	defer printUpdateAvailable()

	for {
		if replicationCfgPath != "" {
			//  run replication
			err = runReplication(replicationCfgPath, cfg, selectStreams...)
			if err != nil {
				return ok, g.Error(err, "failure running replication (see docs @ https://docs.slingdata.io/sling-cli)")
			}
		} else {
			// run task
			if taskCfgStr != "" {
				err = cfg.Unmarshal(taskCfgStr)
				if err != nil {
					return ok, g.Error(err, "could not parse task configuration (see docs @ https://docs.slingdata.io/sling-cli)")
				}
			}

			// run as replication is stream is wildcard
			if cfg.HasWildcard() {
				rc := cfg.AsReplication()
				replicationCfgPath = path.Join(dbioEnv.GetTempFolder(), g.NewTsID("replication.temp")+".json")
				err = os.WriteFile(replicationCfgPath, []byte(g.Marshal(rc)), 0775)
				if err != nil {
					return ok, g.Error(err, "could not write temp replication: %s", replicationCfgPath)
				}
				defer os.Remove(replicationCfgPath)
				continue // run replication
			}

			err = runTask(cfg, nil)
			if err != nil {
				return ok, g.Error(err, "failure running task (see docs @ https://docs.slingdata.io/sling-cli)")
			}
		}
		if iterate > 0 && itNumber >= iterate {
			break
		}

		itNumber++
		time.Sleep(1 * time.Second) // sleep for one second
		println()
		g.Info("Iteration #%d", itNumber)
	}

	return ok, nil
}

func runTask(cfg *sling.Config, replication *sling.ReplicationConfig) (err error) {
	var task *sling.TaskExecution

	taskMap := g.M()
	taskOptions := g.M()
	setTM := func() {

		if task != nil {
			if task.Config.Source.Options == nil {
				task.Config.Source.Options = &sling.SourceOptions{}
			}
			if task.Config.Target.Options == nil {
				task.Config.Target.Options = &sling.TargetOptions{}
			}
			taskOptions["src_has_primary_key"] = task.Config.Source.HasPrimaryKey()
			taskOptions["src_has_update_key"] = task.Config.Source.HasUpdateKey()
			taskOptions["src_flatten"] = task.Config.Source.Options.Flatten
			taskOptions["src_format"] = task.Config.Source.Options.Format
			taskOptions["src_transforms"] = task.Config.Source.Options.Transforms
			taskOptions["tgt_file_max_rows"] = task.Config.Target.Options.FileMaxRows
			taskOptions["tgt_file_max_bytes"] = task.Config.Target.Options.FileMaxBytes
			taskOptions["tgt_format"] = task.Config.Target.Options.Format
			taskOptions["tgt_use_bulk"] = task.Config.Target.Options.UseBulk
			taskOptions["tgt_add_new_columns"] = task.Config.Target.Options.AddNewColumns
			taskOptions["tgt_adjust_column_type"] = task.Config.Target.Options.AdjustColumnType
			taskOptions["tgt_column_casing"] = task.Config.Target.Options.ColumnCasing

			taskMap["md5"] = task.Config.MD5()
			taskMap["type"] = task.Type
			taskMap["mode"] = task.Config.Mode
			taskMap["status"] = task.Status
			taskMap["source_md5"] = task.Config.Source.MD5()
			taskMap["source_type"] = task.Config.SrcConn.Type
			taskMap["target_md5"] = task.Config.Target.MD5()
			taskMap["target_type"] = task.Config.TgtConn.Type
		}

		if projectID != "" {
			telemetryMap["project_id"] = projectID
		}

		if cfg.Options.StdIn && cfg.SrcConn.Type.IsUnknown() {
			taskMap["source_type"] = "stdin"
		}
		if cfg.Options.StdOut {
			taskMap["target_type"] = "stdout"
		}

		telemetryMap["task_options"] = g.Marshal(taskOptions)
		telemetryMap["task"] = g.Marshal(taskMap)
	}

	// track usage
	defer func() {
		taskStats := g.M()

		if task != nil {
			if task.Config.Source.Options == nil {
				task.Config.Source.Options = &sling.SourceOptions{}
			}
			if task.Config.Target.Options == nil {
				task.Config.Target.Options = &sling.TargetOptions{}
			}

			inBytes, outBytes := task.GetBytes()
			taskStats["start_time"] = task.StartTime
			taskStats["end_time"] = task.EndTime
			taskStats["rows_count"] = task.GetCount()
			taskStats["rows_in_bytes"] = inBytes
			taskStats["rows_out_bytes"] = outBytes

			taskMap["md5"] = task.Config.MD5()
			taskMap["type"] = task.Type
			taskMap["mode"] = task.Config.Mode
			taskMap["status"] = task.Status

			if err != nil {
				taskMap["tgt_table_ddl"] = task.Config.Target.Options.TableDDL
			}
		}

		if err != nil {
			telemetryMap["error"] = getErrString(err)
		}

		telemetryMap["task_stats"] = g.Marshal(taskStats)
		telemetryMap["task_options"] = g.Marshal(taskOptions)
		telemetryMap["task"] = g.Marshal(taskMap)

		// telemetry
		Track("run")
	}()

	err = cfg.Prepare()
	if err != nil {
		err = g.Error(err, "could not set task configuration")
		return
	}

	// try to get project_id
	setProjectID(cfg.Env["SLING_CONFIG_PATH"])
	cfg.Env["SLING_PROJECT_ID"] = projectID

	// set logging
	if val := cfg.Env["SLING_LOGGING"]; val != "" {
		os.Setenv("SLING_LOGGING", val)
	}

	task = sling.NewTask(os.Getenv("SLING_EXEC_ID"), cfg)
	task.Replication = replication

	if cast.ToBool(cfg.Env["SLING_DRY_RUN"]) || cast.ToBool(os.Getenv("SLING_DRY_RUN")) {
		return nil
	}

	// insert into store for history keeping
	sling.StoreInsert(task)

	if task.Err != nil {
		err = g.Error(task.Err)
		return
	}

	// set context
	task.Context = &ctx

	// run task
	setTM()
	err = task.Execute()
	if err != nil {
		return g.Error(err)
	}

	return nil
}

func runReplication(cfgPath string, cfgOverwrite *sling.Config, selectStreams ...string) (err error) {
	startTime := time.Now()

	replication, err := sling.LoadReplicationConfig(cfgPath)
	if err != nil {
		return g.Error(err, "Error parsing replication config")
	}

	err = replication.ProcessWildcards()
	if err != nil {
		return g.Error(err, "could not process streams using wildcard")
	}

	// clean up selectStreams
	matchedStreams := map[string]*sling.ReplicationStreamConfig{}
	for _, selectStream := range selectStreams {
		for key, val := range replication.MatchStreams(selectStream) {
			key = replication.Normalize(key)
			matchedStreams[key] = val
		}
	}

	g.Trace("len(selectStreams) = %d, len(matchedStreams) = %d, len(replication.Streams) = %d", len(selectStreams), len(matchedStreams), len(replication.Streams))
	streamCnt := lo.Ternary(len(selectStreams) > 0, len(matchedStreams), len(replication.Streams))
	g.Info("Sling Replication [%d streams] | %s -> %s", streamCnt, replication.Source, replication.Target)

	if err = testStreamCnt(streamCnt, lo.Keys(matchedStreams), lo.Keys(replication.Streams)); err != nil {
		return err
	}

	if streamCnt == 0 {
		g.Warn("Did not match any streams. Exiting.")
		return
	}

	streamsOrdered := replication.StreamsOrdered()
	eG := g.ErrorGroup{}
	succcess := 0
	errors := make([]error, len(streamsOrdered))

	counter := 0
	for i, name := range streamsOrdered {
		if interrupted {
			break
		}

		_, matched := matchedStreams[replication.Normalize(name)]
		if len(selectStreams) > 0 && !matched {
			g.Trace("skipping stream %s since it is not selected", name)
			continue
		}
		counter++

		stream := replication.Streams[name]
		if stream == nil {
			stream = &sling.ReplicationStreamConfig{}
		}
		sling.SetStreamDefaults(stream, replication)

		if stream.Object == "" {
			return g.Error("need to specify `object`. Please see https://docs.slingdata.io/sling-cli for help.")
		}

		// config overwrite
		if cfgOverwrite != nil {
			if string(cfgOverwrite.Mode) != "" && stream.Mode != cfgOverwrite.Mode {
				g.Debug("stream mode overwritten: %s => %s", stream.Mode, cfgOverwrite.Mode)
				stream.Mode = cfgOverwrite.Mode
			}
			if string(cfgOverwrite.Source.UpdateKey) != "" && stream.UpdateKey != cfgOverwrite.Source.UpdateKey {
				g.Debug("stream update_key overwritten: %s => %s", stream.UpdateKey, cfgOverwrite.Source.UpdateKey)
				stream.UpdateKey = cfgOverwrite.Source.UpdateKey
			}
			if cfgOverwrite.Source.PrimaryKeyI != nil && stream.PrimaryKeyI != cfgOverwrite.Source.PrimaryKeyI {
				g.Debug("stream primary_key overwritten: %#v => %#v", stream.PrimaryKeyI, cfgOverwrite.Source.PrimaryKeyI)
				stream.PrimaryKeyI = cfgOverwrite.Source.PrimaryKeyI
			}
		}

		cfg := sling.Config{
			Source: sling.Source{
				Conn:        replication.Source,
				Stream:      name,
				Select:      stream.Select,
				PrimaryKeyI: stream.PrimaryKey(),
				UpdateKey:   stream.UpdateKey,
			},
			Target: sling.Target{
				Conn:   replication.Target,
				Object: stream.Object,
			},
			Mode:            stream.Mode,
			ReplicationMode: true,
			Env:             g.ToMapString(replication.Env),
			StreamName:      name,
		}

		// so that the next stream does not retain previous pointer values
		g.Unmarshal(g.Marshal(stream.SourceOptions), &cfg.Source.Options)
		g.Unmarshal(g.Marshal(stream.TargetOptions), &cfg.Target.Options)

		if stream.SQL != "" {
			cfg.Source.Stream = stream.SQL
		}

		println()

		if stream.Disabled {
			g.Debug("[%d / %d] skipping stream %s since it is disabled", counter, streamCnt, name)
			continue
		} else {
			g.Info("[%d / %d] running stream %s", counter, streamCnt, name)
		}

		telemetryMap = g.M("begin_time", time.Now().UnixMicro(), "run_mode", "replication") // reset map
		telemetryMap["replication_md5"] = replication.MD5()
		err = runTask(&cfg, &replication)
		if err != nil {
			g.Info(env.RedString(err.Error()))
			if eh := sling.ErrorHelper(err); eh != "" {
				env.Println("")
				env.Println(env.MagentaString(eh))
				env.Println("")
			}

			errors[i] = g.Error(err, "error for stream %s", name)
			eG.Capture(err, streamsOrdered[i])
		} else {
			succcess++
		}
	}

	println()
	delta := time.Since(startTime)

	successStr := env.GreenString(g.F("%d Successes", succcess))
	failureStr := g.F("%d Failures", len(eG.Errors))
	if len(eG.Errors) > 0 {
		failureStr = env.RedString(failureStr)
	} else {
		failureStr = env.GreenString(failureStr)
	}

	g.Info("Sling Replication Completed in %s | %s -> %s | %s | %s\n", g.DurationString(delta), replication.Source, replication.Target, successStr, failureStr)

	return eG.Err()
}

func processConns(c *g.CliSC) (ok bool, err error) {
	ok = true

	ef := env.LoadSlingEnvFile()
	ec := connection.EnvConns{EnvFile: &ef}

	telemetryMap["task_start_time"] = time.Now()
	defer func() {
		telemetryMap["task_status"] = lo.Ternary(err != nil, "error", "success")
		telemetryMap["task_end_time"] = time.Now()
	}()

	switch c.UsedSC() {
	case "unset":
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))
		if name == "" {
			flaggy.ShowHelp("")
			return ok, nil
		}

		err := ec.Unset(name)
		if err != nil {
			return ok, g.Error(err, "could not unset %s", name)
		}
		g.Info("connection `%s` has been removed from %s", name, ec.EnvFile.Path)
	case "set":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return ok, nil
		}

		kvArr := []string{cast.ToString(c.Vals["value properties..."])}
		kvMap := map[string]interface{}{}
		for k, v := range g.KVArrToMap(append(kvArr, flaggy.TrailingArguments...)...) {
			k = strings.ToLower(k)
			kvMap[k] = v
		}
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))

		err := ec.Set(name, kvMap)
		if err != nil {
			return ok, g.Error(err, "could not set %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}
		g.Info("connection `%s` has been set in %s. Please test with `sling conns test %s`", name, ec.EnvFile.Path, name)
	case "exec":
		telemetryMap["task"] = g.Marshal(g.M("type", sling.ConnExec))

		name := cast.ToString(c.Vals["name"])
		conn, ok := ec.GetConnEntry(name)
		if !ok {
			return ok, g.Error("did not find connection %s", name)
		}

		telemetryMap["conn_type"] = conn.Connection.Type.String()

		if !conn.Connection.Type.IsDb() {
			return ok, g.Error("cannot execute SQL query on a non-database connection (%s)", conn.Connection.Type)
		}

		start := time.Now()
		dbConn, err := conn.Connection.AsDatabase()
		if err != nil {
			return ok, g.Error(err, "cannot create database connection (%s)", conn.Connection.Type)
		}

		err = dbConn.Connect()
		if err != nil {
			return ok, g.Error(err, "cannot connect to database (%s)", conn.Connection.Type)
		}

		queries := append([]string{cast.ToString(c.Vals["queries..."])}, flaggy.TrailingArguments...)

		var totalAffected int64
		for i, query := range queries {
			if len(queries) > 1 {
				if strings.HasPrefix(query, "file://") {
					g.Info("executing query #%d (%s)", i+1, query)
				} else {
					g.Info("executing query #%d", i+1)
				}
			} else {
				g.Info("executing query")
			}

			query, err = sling.GetSQLText(query)
			if err != nil {
				return ok, g.Error(err, "cannot get query")
			}

			sQuery, err := database.ParseTableName(query, conn.Connection.Type)
			if err != nil {
				return ok, g.Error(err, "cannot parse query")
			}

			if len(database.ParseSQLMultiStatements(query)) == 1 && (!sQuery.IsQuery() || strings.Contains(query, "select")) {

				data, err := dbConn.Query(sQuery.Select(100))
				if err != nil {
					return ok, g.Error(err, "cannot execute query")
				}

				println(g.PrettyTable(data.GetFields(), data.Rows))

			} else {
				result, err := dbConn.ExecMulti(query)
				if err != nil {
					return ok, g.Error(err, "cannot execute query")
				}

				affected, _ := result.RowsAffected()
				totalAffected = totalAffected + affected
			}
		}

		end := time.Now()
		if totalAffected > 0 {
			g.Info("Successful! Duration: %d seconds (%d affected records)", end.Unix()-start.Unix(), totalAffected)
		} else {
			g.Info("Successful! Duration: %d seconds.", end.Unix()-start.Unix())
		}

	case "list":
		println(ec.List())

	case "test":
		telemetryMap["task"] = g.Marshal(g.M("type", sling.ConnTest))
		name := cast.ToString(c.Vals["name"])
		if conn, ok := ec.GetConnEntry(name); ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
			telemetryMap["conn_keys"] = lo.Keys(conn.Connection.Data)
		}

		ok, err = ec.Test(name)
		if err != nil {
			return ok, g.Error(err, "could not test %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		} else if ok {
			g.Info("success!") // successfully connected
		}
	case "discover":
		telemetryMap["task"] = g.Marshal(g.M("type", sling.ConnDiscover))
		name := cast.ToString(c.Vals["name"])
		conn, ok := ec.GetConnEntry(name)
		if ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
		}

		opt := &connection.DiscoverOptions{
			Pattern:     cast.ToString(c.Vals["selector"]),
			ColumnLevel: cast.ToBool(c.Vals["columns"]),
			Recursive:   cast.ToBool(c.Vals["recursive"]),
		}

		files, schemata, err := ec.Discover(name, opt)
		if err != nil {
			return ok, g.Error(err, "could not discover %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}

		if tables := lo.Values(schemata.Tables()); len(tables) > 0 {

			sort.Slice(tables, func(i, j int) bool {
				val := func(t database.Table) string {
					return t.FDQN()
				}
				return val(tables[i]) < val(tables[j])
			})

			if opt.ColumnLevel {
				println(iop.Columns(lo.Values(schemata.Columns())).PrettyTable(true))
			} else {
				header := []string{"#", "Schema", "Name", "Type", "Columns"}
				rows := lo.Map(tables, func(table database.Table, i int) []any {
					tableType := lo.Ternary(table.IsView, "view", "table")
					if table.Dialect.DBNameUpperCase() {
						tableType = strings.ToUpper(tableType)
					}
					return []any{i + 1, table.Schema, table.Name, tableType, len(table.Columns)}
				})
				println(g.PrettyTable(header, rows))
			}
		} else if len(files) > 0 {
			if opt.ColumnLevel {
				println(iop.Columns(lo.Values(files.Columns())).PrettyTable(true))
			} else {

				files.Sort()

				header := []string{"#", "Name", "Type", "Size", "Last Updated (UTC)"}
				rows := lo.Map(files, func(file dbio.FileNode, i int) []any {
					fileType := lo.Ternary(file.IsDir, "directory", "file")

					lastUpdated := "-"
					if file.Updated > 100 {
						updated := time.Unix(file.Updated, 0)
						delta := strings.Split(g.DurationString(time.Since(updated)), " ")[0]
						lastUpdated = g.F("%s (%s ago)", updated.UTC().Format("2006-01-02 15:04:05"), delta)
					}

					size := "-"
					if !file.IsDir || file.Size > 0 {
						size = humanize.IBytes(file.Size)
					}

					return []any{i + 1, file.Path(), fileType, size, lastUpdated}
				})

				println(g.PrettyTable(header, rows))

				if len(files) > 0 && !(opt.Recursive || opt.Pattern != "") {
					g.Info("Those are non-recursive folder or file names (at the root level). Please use the --selector flag to list sub-folders, or --recursive")
				}
			}
		}

	case "":
		return false, nil
	}
	return ok, nil
}

func printUpdateAvailable() {
	if updateVersion != "" {
		println(updateMessage)
	}
}

func parsePayload(payload string, validate bool) (options map[string]any, err error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return map[string]any{}, nil
	}

	// try json
	options, err = g.UnmarshalMap(payload)
	if err == nil {
		return options, nil
	}

	// try yaml
	err = yaml.Unmarshal([]byte(payload), &options)
	if err != nil {
		return options, g.Error(err, "could not parse options")
	}

	// validate, check for typos
	if validate {
		for k := range options {
			if strings.Contains(k, ":") {
				return options, g.Error("invalid key: %s. Try adding a space after the colon.", k)
			}
		}
	}

	return options, nil
}

// setProjectID attempts to get the first sha of the repo
func setProjectID(cfgPath string) {
	if cfgPath == "" {
		return
	}

	cfgPath, _ = filepath.Abs(cfgPath)

	if fs, err := os.Stat(cfgPath); err == nil && !fs.IsDir() {
		// get first sha
		cmd := exec.Command("git", "rev-list", "--max-parents=0", "HEAD")
		cmd.Dir = filepath.Dir(cfgPath)
		out, err := cmd.Output()
		if err == nil {
			projectID = strings.TrimSpace(string(out))
		}
	}
}

func testStreamCnt(streamCnt int, matchedStreams, inputStreams []string) error {
	if expected := os.Getenv("SLING_STREAM_CNT"); expected != "" {

		if strings.HasPrefix(expected, ">") {
			atLeast := cast.ToInt(strings.TrimPrefix(expected, ">"))
			if streamCnt <= atLeast {
				return g.Error("Expected at least %d streams, got %d => %s", atLeast, streamCnt, g.Marshal(append(matchedStreams, inputStreams...)))
			}
			return nil
		}

		if streamCnt != cast.ToInt(expected) {
			return g.Error("Expected %d streams, got %d => %s", cast.ToInt(expected), streamCnt, g.Marshal(append(matchedStreams, inputStreams...)))
		}
	}
	return nil
}
