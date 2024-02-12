package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/integrii/flaggy"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"

	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/slingdata-io/sling-cli/core/store"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	isDocker  = os.Getenv("SLING_PACKAGE") == "DOCKER"
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
	cfgStr := ""
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
			replicationCfgPath = cast.ToString(v)
		case "config":
			cfgStr = cast.ToString(v)
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
			selectStreams = strings.Split(cast.ToString(v), ",")
		case "debug":
			cfg.Options.Debug = cast.ToBool(v)
			if cfg.Options.Debug {
				os.Setenv("DEBUG", "LOW")
			}
		case "examples":
			showExamples = cast.ToBool(v)
		}
	}

	if showExamples {
		println(examples)
		return ok, nil
	}

	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_CLI_ARGS", g.Marshal(os.Args[1:]))

	// check for update, and print note
	go checkUpdate()
	defer printUpdateAvailable()

	for {
		if replicationCfgPath != "" {
			//  run replication
			err = runReplication(replicationCfgPath, selectStreams...)
			if err != nil {
				return ok, g.Error(err, "failure running replication (see docs @ https://docs.slingdata.io/sling-cli)")
			}
		} else {
			// run task
			if cfgStr != "" {
				err = cfg.Unmarshal(cfgStr)
				if err != nil {
					return ok, g.Error(err, "could not parse task configuration (see docs @ https://docs.slingdata.io/sling-cli)")
				}
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
	telemetryMap["run_mode"] = "task"
	var task *sling.TaskExecution

	// track usage
	defer func() {
		if task != nil {
			if task.Config.Source.Options == nil {
				task.Config.Source.Options = &sling.SourceOptions{}
			}
			if task.Config.Target.Options == nil {
				task.Config.Target.Options = &sling.TargetOptions{}
			}
			inBytes, outBytes := task.GetBytes()
			telemetryMap["task_type"] = task.Type
			telemetryMap["task_mode"] = task.Config.Mode
			telemetryMap["task_status"] = task.Status
			telemetryMap["task_source_type"] = task.Config.SrcConn.Type
			telemetryMap["task_target_type"] = task.Config.TgtConn.Type
			telemetryMap["task_start_time"] = task.StartTime
			telemetryMap["task_end_time"] = task.EndTime
			telemetryMap["task_rows_count"] = task.GetCount()
			telemetryMap["task_rows_in_bytes"] = inBytes
			telemetryMap["task_rows_out_bytes"] = outBytes
			telemetryMap["task_options"] = g.Marshal(g.M(
				"src_has_primary_key", task.Config.Source.HasPrimaryKey(),
				"src_has_update_key", task.Config.Source.HasUpdateKey(),
				"src_flatten", task.Config.Source.Options.Flatten,
				"src_format", task.Config.Source.Options.Format,
				"src_transforms", task.Config.Source.Options.Transforms,
				"tgt_file_max_rows", task.Config.Target.Options.FileMaxRows,
				"tgt_file_max_bytes", task.Config.Target.Options.FileMaxBytes,
				"tgt_format", task.Config.Target.Options.Format,
				"tgt_use_bulk", task.Config.Target.Options.UseBulk,
				"tgt_add_new_columns", task.Config.Target.Options.AddNewColumns,
				"tgt_adjust_column_type", task.Config.Target.Options.AdjustColumnType,
				"tgt_column_casing", task.Config.Target.Options.ColumnCasing,
			))
		}

		if projectID != "" {
			telemetryMap["project_id"] = projectID
		}

		if cfg.Options.StdIn && cfg.SrcConn.Type.IsUnknown() {
			telemetryMap["task_source_type"] = "stdin"
		}
		if cfg.Options.StdOut {
			telemetryMap["task_target_type"] = "stdout"
		}

		if err != nil {
			telemetryMap["error"] = getErrString(err)
		}

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

	task = sling.NewTask(0, cfg)
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
	err = task.Execute()
	if err != nil {
		return g.Error(err)
	}

	return nil
}

func runReplication(cfgPath string, selectStreams ...string) (err error) {
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
	selectStreams = lo.Filter(selectStreams, func(v string, i int) bool {
		return replication.HasStream(v)
	})

	streamCnt := lo.Ternary(len(selectStreams) > 0, len(selectStreams), len(replication.Streams))
	g.Info("Sling Replication [%d streams] | %s -> %s", streamCnt, replication.Source, replication.Target)

	eG := g.ErrorGroup{}
	counter := 0
	for _, name := range replication.StreamsOrdered() {
		if interrupted {
			break
		}

		if len(selectStreams) > 0 && !g.IsMatched(selectStreams, name) {
			g.Debug("skipping stream %s since it is not selected", name)
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
			g.Info("[%d / %d] skipping stream %s since it is disabled", counter, streamCnt, name)
			continue
		} else {
			g.Info("[%d / %d] running stream %s", counter, streamCnt, name)
		}

		telemetryMap["run_mode"] = "replication"
		err = runTask(&cfg, &replication)
		if err != nil {
			err = g.Error(err, "error for stream %s", name)
			g.LogError(err)
			eG.Capture(err)
		}
		telemetryMap = g.M("begin_time", time.Now().UnixMicro()) // reset map
	}

	println()
	delta := time.Since(startTime)
	g.Info("Sling Replication Completed in %s | %s -> %s", g.DurationString(delta), replication.Source, replication.Target)

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

	case "list":
		println(ec.List())

	case "test":
		name := cast.ToString(c.Vals["name"])
		if conn, ok := ec.GetConnEntry(name); ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
		}

		ok, err = ec.Test(name)
		if err != nil {
			return ok, g.Error(err, "could not test %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		} else if ok {
			g.Info("success!") // successfully connected
		}
	case "discover":
		name := cast.ToString(c.Vals["name"])
		if conn, ok := ec.GetConnEntry(name); ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
		}

		opt := connection.DiscoverOptions{
			Schema:    cast.ToString(c.Vals["schema"]),
			Folder:    cast.ToString(c.Vals["folder"]),
			Filter:    cast.ToString(c.Vals["filter"]),
			Recursive: cast.ToBool(c.Vals["recursive"]),
		}

		var streamNames []string
		streamNames, err = ec.Discover(name, opt)
		if err != nil {
			return ok, g.Error(err, "could not discover %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}

		g.Info("Found %d streams:", len(streamNames))
		for _, sn := range streamNames {
			println(g.F(" - %s", sn))
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
