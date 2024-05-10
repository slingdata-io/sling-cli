package main

import (
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/samber/lo"
	"gopkg.in/yaml.v2"

	"github.com/shirou/gopsutil/v3/mem"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	projectID     = os.Getenv("SLING_PROJECT_ID")
	updateMessage = ""
	updateVersion = ""
	rowCount      = int64(0)
	totalBytes    = uint64(0)
)

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
			env.SetTelVal("error", g.F("panic occurred! %#v\n%s", r, string(debug.Stack())))
		}
	}()

	// determine if stdin data is piped
	// https://stackoverflow.com/a/26567513
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		cfg.Options.StdIn = true
	}

	env.SetTelVal("stage", "0 - init")
	env.SetTelVal("run_mode", "cli")

	for k, v := range c.Vals {
		switch k {
		case "replication":
			env.SetTelVal("run_mode", "replication")
			replicationCfgPath = cast.ToString(v)
		case "config":
			env.SetTelVal("run_mode", "task")
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
			if cfg.Options.Debug && os.Getenv("DEBUG") == "" {
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
				replicationCfgPath = path.Join(env.GetTempFolder(), g.NewTsID("replication.temp")+".json")
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

	// test count/bytes if need
	err = testOutput(rowCount, totalBytes)

	return ok, err
}

func runTask(cfg *sling.Config, replication *sling.ReplicationConfig) (err error) {
	var task *sling.TaskExecution

	taskMap := g.M()
	taskOptions := g.M()
	env.SetTelVal("stage", "1 - task-creation")
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
			env.SetTelVal("project_id", projectID)
		}

		if cfg.Options.StdIn && cfg.SrcConn.Type.IsUnknown() {
			taskMap["source_type"] = "stdin"
		}
		if cfg.Options.StdOut {
			taskMap["target_type"] = "stdout"
		}

		env.SetTelVal("task_options", g.Marshal(taskOptions))
		env.SetTelVal("task", g.Marshal(taskMap))
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
			taskStats["rows_out_bytes"] = outBytes

			if memRAM, _ := mem.VirtualMemory(); memRAM != nil {
				taskStats["mem_used"] = memRAM.Used
				taskStats["mem_total"] = memRAM.Total
			}

			taskMap["md5"] = task.Config.MD5()
			taskMap["type"] = task.Type
			taskMap["mode"] = task.Config.Mode
			taskMap["status"] = task.Status

			if err != nil {
				taskMap["tgt_table_ddl"] = task.Config.Target.Options.TableDDL
			}
		}

		if err != nil {
			env.SetTelVal("error", getErrString(err))
		}

		env.SetTelVal("task_stats", g.Marshal(taskStats))
		env.SetTelVal("task_options", g.Marshal(taskOptions))
		env.SetTelVal("task", g.Marshal(taskMap))

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

	rowCount = rowCount + int64(task.GetCount())
	inBytes, outBytes := task.GetBytes()
	if inBytes == 0 {
		totalBytes = totalBytes + outBytes
	} else {
		totalBytes = totalBytes + inBytes
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

		stream := sling.ReplicationStreamConfig{}
		if replication.Streams[name] != nil {
			stream = *replication.Streams[name]
		}
		sling.SetStreamDefaults(&stream, replication)

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

		env.TelMap = g.M("begin_time", time.Now().UnixMicro(), "run_mode", "replication") // reset map
		env.SetTelVal("replication_md5", replication.MD5())
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
		if err == nil && projectID == "" {
			projectID = strings.TrimSpace(string(out))
		}
	}
}

func testOutput(rowCnt int64, totalBytes uint64) error {
	if expected := os.Getenv("SLING_ROW_CNT"); expected != "" {

		if strings.HasPrefix(expected, ">") {
			atLeast := cast.ToInt64(strings.TrimPrefix(expected, ">"))
			if rowCnt <= atLeast {
				return g.Error("Expected at least %d rows, got %d", atLeast+1, rowCnt)
			}
			return nil
		}

		if rowCnt != cast.ToInt64(expected) {
			return g.Error("Expected %d rows, got %d", cast.ToInt(expected), rowCnt)
		}
	}

	if expected := os.Getenv("SLING_TOTAL_BYTES"); expected != "" {

		if strings.HasPrefix(expected, ">") {
			atLeast := cast.ToUint64(strings.TrimPrefix(expected, ">"))
			if totalBytes <= atLeast {
				return g.Error("Expected at least %d bytes, got %d", atLeast+1, totalBytes)
			}
			return nil
		}

		if totalBytes != cast.ToUint64(expected) {
			return g.Error("Expected %d bytes, got %d", cast.ToInt(expected), totalBytes)
		}
	}

	return nil
}
