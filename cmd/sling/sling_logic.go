package main

import (
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/integrii/flaggy"
	"github.com/samber/lo"
	"gopkg.in/yaml.v2"

	"github.com/flarco/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	masterServerURL = os.Getenv("SLING_MASTER_URL")
	isDocker        = os.Getenv("SLING_PACKAGE") == "DOCKER"
	apiKey          = os.Getenv("SLING_API_KEY")
	projectID       = os.Getenv("SLING_PROJECT")
	headers         = map[string]string{
		"Content-Type":     "application/json",
		"Sling-API-Key":    apiKey,
		"Sling-Project-ID": projectID,
	}
	updateMessage = ""
	updateVersion = ""
)

func init() {
	if masterServerURL == "" {
		masterServerURL = "https://api.slingdata.io"
	}
}

func processRun(c *g.CliSC) (ok bool, err error) {
	ok = true
	cfg := &sling.Config{}
	replicationCfgPath := ""
	cfgStr := ""
	showExamples := false
	selectStreams := []string{}
	// saveAsJob := false

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
			cfg.Source.Stream = cast.ToString(v)
			if strings.Contains(cfg.Source.Stream, "://") {
				if _, ok := c.Vals["src-conn"]; !ok { // src-conn not specified
					cfg.Source.Conn = cfg.Source.Stream
				}
			}
		case "src-options":
			payload := cast.ToString(v)
			options, err := parseOptions(payload)
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

		case "tgt-object", "tgt-table", "tgt-file":
			cfg.Target.Object = cast.ToString(v)
			if strings.Contains(cfg.Target.Object, "://") {
				if _, ok := c.Vals["tgt-conn"]; !ok { // tgt-conn not specified
					cfg.Target.Conn = cfg.Target.Object
				}
			}
		case "tgt-options":
			payload := cast.ToString(v)
			options, err := parseOptions(payload)
			if err != nil {
				return ok, g.Error(err, "invalid target options -> %s", payload)
			}

			err = g.JSONConvert(options, &cfg.Target.Options)
			if err != nil {
				return ok, g.Error(err, "invalid target options -> %s", payload)
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

	// check for update, and print note
	go checkUpdate()
	defer printUpdateAvailable()

	if replicationCfgPath != "" {
		err = runReplication(replicationCfgPath, selectStreams...)
		if err != nil {
			return ok, g.Error(err, "failure running replication (see docs @ https://docs.slingdata.io/sling-cli)")
		}
		return ok, nil

	}

	if cfgStr != "" {
		err = cfg.Unmarshal(cfgStr)
		if err != nil {
			return ok, g.Error(err, "could not parse task configuration (see docs @ https://docs.slingdata.io/sling-cli)")
		}
	}

	// run task
	telemetryMap["run_mode"] = "task"
	err = runTask(cfg)
	if err != nil {
		return ok, g.Error(err, "failure running task (see docs @ https://docs.slingdata.io/sling-cli)")
	}

	return ok, nil
}

func runTask(cfg *sling.Config) (err error) {
	var task *sling.TaskExecution

	// track usage
	defer func() {
		if task != nil {
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

	task = sling.NewTask(0, cfg)
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
		return strings.TrimSpace(v) != ""
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
			g.Debug("skipping stream `%s` since it is not selected", name)
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
				Columns:     stream.Columns,
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
		}

		// so that the next stream does not retain previous pointer values
		g.Unmarshal(g.Marshal(stream.SourceOptions), &cfg.Source.Options)
		g.Unmarshal(g.Marshal(stream.TargetOptions), &cfg.Target.Options)

		if stream.SQL != "" {
			cfg.Source.Stream = stream.SQL
		}

		println()

		if stream.Disabled {
			g.Info("[%d / %d] skipping stream `%s` since it is disabled", counter, streamCnt, name)
			continue
		} else {
			g.Info("[%d / %d] running stream `%s`", counter, streamCnt, name)
		}

		telemetryMap["run_mode"] = "replication"
		err = runTask(&cfg)
		if err != nil {
			err = g.Error(err, "error for stream `%s`", name)
			g.LogError(err)
			eG.Capture(err)
		}
		telemetryMap = g.M("begin_time", time.Now().UnixMicro()) // reset map
	}

	return eG.Err()
}

func processConns(c *g.CliSC) (ok bool, err error) {
	ok = true

	ef := env.LoadSlingEnvFile()
	ec := connection.EnvConns{EnvFile: &ef}

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
		ok, err = ec.Test(name)
		if err != nil {
			return ok, g.Error(err, "could not test %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		} else if ok {
			g.Info("success!") // successfully connected
		}

		if conn, ok := ec.GetConnEntry(name); ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
		}
	case "discover":
		name := cast.ToString(c.Vals["name"])
		opt := connection.DiscoverOptions{
			Schema: cast.ToString(c.Vals["schema"]),
			Folder: cast.ToString(c.Vals["folder"]),
			Filter: cast.ToString(c.Vals["filter"]),
		}

		streamNames, err := ec.Discover(name, opt)
		if err != nil {
			return ok, g.Error(err, "could not discover %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}

		g.Info("Found %d streams:", len(streamNames))
		for _, sn := range streamNames {
			println(g.F(" - %s", sn))
		}

		if conn, ok := ec.GetConnEntry(name); ok {
			telemetryMap["conn_type"] = conn.Connection.Type.String()
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

func parseOptions(payload string) (options map[string]any, err error) {
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
	for k := range options {
		if strings.Contains(k, ":") {
			return options, g.Error("invalid key: %s. Try adding a space after the colon.", k)
		}
	}

	return options, nil
}
