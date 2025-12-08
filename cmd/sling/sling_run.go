package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"golang.org/x/term"

	"gopkg.in/yaml.v2"

	"github.com/samber/lo"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

var (
	projectID         = os.Getenv("SLING_PROJECT_ID")
	updateMessage     = ""
	updateVersion     = ""
	rowCount          = int64(0)
	totalBytes        = uint64(0)
	constraintFails   = uint64(0)
	lookupReplication = func(id string) (r sling.ReplicationConfig, e error) { return }

	runReplication func(string, *sling.Config, ...string) error = replicationRun
)

func processRun(c *g.CliSC) (ok bool, err error) {
	ok = true
	cfg := &sling.Config{
		Source: sling.Source{Options: &sling.SourceOptions{}},
		Target: sling.Target{Options: &sling.TargetOptions{}},
	}

	var replicationCfgPath, pipelineCfgPath, directoryPath string

	showExamples := false
	selectStreams := []string{}

	// set function here due to scoping
	sling.HookRunReplication = runReplication

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
		case "pipeline":
			env.SetTelVal("run_mode", "pipeline")
			pipelineCfgPath = cast.ToString(v)
		case "directory":
			env.SetTelVal("run_mode", "directory")
			directoryPath = cast.ToString(v)
		case "path":
			filePath := cast.ToString(v)
			fileInfo, err := os.Stat(filePath)
			if err != nil {
				return true, g.Error(err, "error accessing path: %s", filePath)
			}

			if fileInfo.IsDir() {
				env.SetTelVal("run_mode", "directory")
				directoryPath = filePath
			} else {
				dirPath, err := os.Getwd()
				if err != nil {
					return true, g.Error(err, "unable to determine working dir. Try using flags")
				}
				runFile := sling.RunFile{File: g.InfoToFileItem(dirPath, filePath, fileInfo)}
				valid, err := runFile.IsValid()
				if err != nil {
					return true, g.Error(err, "could not load file: %s", filePath)
				} else if valid {
					switch runFile.Type {
					case sling.RunFileReplication:
						env.SetTelVal("run_mode", "replication")
						replicationCfgPath = filePath
					case sling.RunFilePipeline:
						env.SetTelVal("run_mode", "pipeline")
						pipelineCfgPath = filePath
					}
				} else {
					return true, g.Error("file is not a valid replication or pipeline: %s", filePath)
				}
			}

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
			cfg.Source.Options.Limit = g.Int(cast.ToInt(v))

		case "offset":
			cfg.Source.Options.Offset = g.Int(cast.ToInt(v))

		case "range":
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
		case "columns":
			payload := cast.ToString(v)
			err = yaml.Unmarshal([]byte(payload), &cfg.Target.Columns)
			if err != nil {
				return ok, g.Error(err, "invalid columns -> %s", payload)
			}
		case "transforms":
			payload := cast.ToString(v)
			err = yaml.Unmarshal([]byte(payload), &cfg.Transforms)
			if err != nil {
				return ok, g.Error(err, "invalid transforms -> %s", payload)
			}
		case "select":
			cfg.Source.Select = strings.Split(cast.ToString(v), ",")
		case "where":
			cfg.Source.Where = cast.ToString(v)
		case "streams":
			selectStreams = strings.Split(cast.ToString(v), ",")
		case "examples":
			showExamples = cast.ToBool(v)
		}
	}

	if cast.ToBool(c.Vals["trace"]) {
		cfg.Options.Debug = true
		os.Setenv("DEBUG", "TRACE")
		env.InitLogger()
	} else if cast.ToBool(c.Vals["debug"]) {
		cfg.Options.Debug = true
		os.Setenv("DEBUG", "LOW")
		env.InitLogger()
	}

	if showExamples {
		println(examples)
		return ok, nil
	}

	if taskCfgStr := os.Getenv("SLING_TASK_CONFIG"); taskCfgStr != "" {
		err = cfg.Unmarshal(taskCfgStr)
		if err != nil {
			return ok, g.Error(err, "could not parse task configuration")
		}
	}

	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_CLI_ARGS", g.Marshal(os.Args[1:]))

	// set run mode
	if os.Getenv("SLING_RUN_MODE") == "" {
		os.Setenv(
			"SLING_RUN_MODE",
			lo.Ternary(pipelineCfgPath != "", "pipeline", "replication"),
		)
	}

runReplication:
	defer connection.CloseAll()

	if !env.IsThreadChild {
		text := "Sling CLI | https://slingdata.io"
		if env.NoColor {
			g.Info(text)
		} else {
			g.Info(env.CyanString(text))
		}

		// check for update, and print note
		go checkUpdate()
		defer printUpdateAvailable()
	}

	if directoryPath != "" {
		err = runDirectory(directoryPath)
		if err != nil {
			return ok, g.Error(err, "failure running directory (see docs @ https://docs.slingdata.io)")
		}
	} else if pipelineCfgPath != "" {
		err = runPipeline(pipelineCfgPath)
		if err != nil {
			return ok, g.Error(err, "failure running pipeline (see docs @ https://docs.slingdata.io)")
		}
	} else if replicationCfgPath != "" {
		//  run replication
		err = runReplication(replicationCfgPath, cfg, selectStreams...)
		if err != nil {
			return ok, g.Error(err, "failure running replication (see docs @ https://docs.slingdata.io/sling-cli)")
		}
	} else {
		// run task, add replication config for md5
		rc := cfg.AsReplication()

		// run as replication is stream is wildcard
		if cfg.HasWildcard() {
			replicationCfgPath = path.Join(env.GetTempFolder(), g.NewTsID("replication.temp")+".json")
			err = os.WriteFile(replicationCfgPath, []byte(g.Marshal(rc)), 0775)
			if err != nil {
				return ok, g.Error(err, "could not write temp replication: %s", replicationCfgPath)
			}
			defer os.Remove(replicationCfgPath)
			goto runReplication // run replication
		}

		// set global timeout
		setTimeout(os.Getenv("SLING_TIMEOUT"))

		err = runTask(cfg, &rc)
		if err != nil {
			return ok, g.Error(err, "failure running task (see docs @ https://docs.slingdata.io/sling-cli)")
		}
	}

	// test count/bytes if need
	err = testOutput(rowCount, totalBytes, constraintFails)

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
			taskOptions["tgt_file_max_rows"] = task.Config.Target.Options.FileMaxRows
			taskOptions["tgt_file_max_bytes"] = task.Config.Target.Options.FileMaxBytes
			taskOptions["tgt_format"] = task.Config.Target.Options.Format
			taskOptions["tgt_use_bulk"] = task.Config.Target.Options.UseBulk
			taskOptions["tgt_add_new_columns"] = task.Config.Target.Options.AddNewColumns
			taskOptions["tgt_adjust_column_type"] = task.Config.Target.Options.AdjustColumnType
			taskOptions["tgt_column_casing"] = task.Config.Target.Options.ColumnCasing

			taskMap["exec_id"] = task.ExecID
			taskMap["md5"] = task.Config.MD5()
			taskMap["type"] = task.Type
			taskMap["mode"] = task.Config.Mode
			taskMap["transforms"] = task.Config.Transforms
			taskMap["status"] = task.Status
			taskMap["source_md5"] = task.Config.SrcConnMD5()
			taskMap["source_type"] = task.Config.SrcConn.Type
			taskMap["target_md5"] = task.Config.TgtConnMD5()
			taskMap["target_type"] = task.Config.TgtConn.Type
			taskMap["stream_id"] = task.Config.StreamID()
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

	// set working directory path
	if val := cfg.Env["SLING_WORK_PATH"]; val != "" {
		if err = os.Chdir(val); err != nil {
			err = g.Error(err, "could not set working directory: %s", val)
			return
		}
	} else {
		cfg.Env["SLING_WORK_PATH"], _ = os.Getwd()
	}

	// set logging
	if val := cfg.Env["SLING_LOGGING"]; val != "" {
		os.Setenv("SLING_LOGGING", val)
	}

	task = sling.NewTask(os.Getenv("SLING_EXEC_ID"), cfg)
	task.Replication = replication

	if cast.ToBool(cfg.Env["SLING_DRY_RUN"]) || cast.ToBool(os.Getenv("SLING_DRY_RUN")) {
		return nil
	} else if replication.FailErr != "" {
		task.Status = sling.ExecStatusError
		task.Err = g.Error(replication.FailErr)
	}

	// set log sink if not pipeline mode
	if sling.IsReplicationRunMode() {
		env.LogSink = func(ll *g.LogLine) {
			ll.Group = g.F("%s,%s", task.ExecID, task.Config.StreamID())
			task.AppendOutput(ll)
		}
	}

	task.StateSet() // set into store

	if task.Err != nil {
		err = g.Error(task.Err)
		return
	}

	// set context
	task.Context = ctx

	// set into store after
	defer task.StateSet()

	// run task
	setTM()
	err = task.Execute()

	if err != nil {

		if replication != nil && (len(replication.Tasks) > 1 || projectID != "") {
			// print error right after stream run if there are multiple runs
			// so it's easy to find. otherwise will print at end
			fmt.Fprintf(os.Stderr, "%s\n", env.RedString(g.ErrMsgSimple(err)))
		}

		// show help text
		if eh := sling.ErrorHelper(err); eh != "" {
			env.Println("")
			env.Println(env.MagentaString(eh))
			env.Println("")
		}

		return g.Error(err)
	}

	rowCount = rowCount + int64(task.GetCount())
	inBytes, outBytes := task.GetBytes()
	if inBytes == 0 {
		totalBytes = totalBytes + outBytes
	} else {
		totalBytes = totalBytes + inBytes
	}

	// add up constraints
	if df := task.Df(); df != nil {
		for _, col := range df.Columns {
			if c := col.Constraint; c != nil && c.FailCnt > 0 {
				constraintFails = constraintFails + c.FailCnt
			}
		}
	}

	return nil
}

func replicationRun(cfgPath string, cfgOverwrite *sling.Config, selectStreams ...string) (err error) {
	startTime := time.Now()

	replication, err := sling.LoadReplicationConfigFromFile(cfgPath)
	if err != nil {
		if sling.IsJSONorYAML(cfgPath) {
			replication, err = sling.LoadReplicationConfig(cfgPath) // is JSON
		} else if r, e := lookupReplication(cfgPath); r.OriginalCfg() != "" {
			replication, err = r, e
		}
		if err != nil {
			return g.Error(err, "Error parsing replication config")
		}
	}

	// load SLING_TIMEOUT if specified in replication env
	timeoutR := replication.Env["SLING_TIMEOUT"]
	timeoutE := os.Getenv("SLING_TIMEOUT")

	setTimeout(cast.ToString(timeoutR), timeoutE)

	err = replication.Compile(cfgOverwrite, selectStreams...)
	if err != nil {
		return g.Error(err, "Error compiling replication config")
	}

	if len(replication.Tasks) == 0 {
		g.Warn("Did not match any streams. Exiting.")
		return
	}

	isThreadChild := cast.ToBool(os.Getenv("SLING_THREAD_CHILD"))

	eG := g.ErrorGroup{}
	successes := 0
	replication.Context = ctx

	// get final stream count
	streamCnt := 0
	for _, cfg := range replication.Tasks {
		if cfg.ReplicationStream.Disabled {
			continue
		}
		streamCnt++
	}

	if streamCnt > 1 {
		g.Info("Sling Replication [%d streams] | %s -> %s", streamCnt, replication.Source, replication.Target)
	}

	// set pipeline store data
	if store := sling.GetPipelineStoreEnv(); len(store) > 0 && sling.IsPipelineRunMode() {
		if state, _ := replication.RuntimeState(); state != nil {
			maps.Copy(state.Store, store)
		}
	}

	// run start hooks if not thread child
	if !isThreadChild {
		if err = replication.ExecuteReplicationHook(sling.HookStageStart); err != nil {
			return g.Error(err, "error executing start hooks")
		}
	}

	counter := 0
	cleanedForChunkLoad := map[string]bool{}

	for _, cfg := range replication.Tasks {
		if interrupted {
			break
		}

		// if we're chunking and if truncate/full-refresh
		// truncate or drop right now, the first task will re-create it
		cleaned := cleanedForChunkLoad[cfg.Target.Object]
		if !isThreadChild && !cleaned && (cfg.IsFullRefreshWithChunking() || cfg.IsTruncateWithChunking()) {
			if err = cfg.ClearTableForChunkLoadWithRange(); err != nil {
				return g.Error(err, "could not clear table in target conn for chunk loading")
			}
			cleanedForChunkLoad[cfg.Target.Object] = true
		}

		if sling.IsReplicationRunMode() {
			env.LogSink = nil // clear log sink if replication mode
		}

		if cfg.ReplicationStream.Disabled {
			println()
			g.Debug("skipping stream %s since it is disabled", cfg.StreamName)
			continue
		} else if streamCnt == 1 {
			g.Info("Sling Replication | %s -> %s | %s", replication.Source, replication.Target, env.BlueString(cfg.StreamName))
		} else {
			println()
			counter++
			g.Info("[%d / %d] running stream %s", counter, streamCnt, env.BlueString(cfg.StreamName))
		}

		env.TelMap = g.M("begin_time", time.Now().UnixMicro(), "run_mode", "replication") // reset map
		env.SetTelVal("replication_md5", replication.MD5())
		err = runTask(cfg, &replication)
		if err != nil {
			eG.Capture(err, cfg.StreamName)

			// if a connection issue, stop
			if e, ok := err.(*g.ErrType); ok && strings.Contains(e.Debug(), "Could not connect to ") {
				replication.FailErr = g.ErrMsg(e)
			}
		} else {
			successes++
		}
	}

	// run end hooks if not thread child
	if !isThreadChild {
		if err = replication.ExecuteReplicationHook(sling.HookStageEnd); err != nil {
			eG.Capture(err, "end-hooks")
		}
	} else {
		iop.CloseQueues() // close queues to clean up if needed in child for next dependent
	}

	println()
	delta := time.Since(startTime)

	successStr := env.GreenString(g.F("%d Successes", successes))
	failureStr := g.F("%d Failures", len(eG.Errors))
	if len(eG.Errors) > 0 {
		failureStr = env.RedString(failureStr)
	} else {
		failureStr = env.GreenString(failureStr)
	}

	if streamCnt > 1 {
		g.Info("Sling Replication Completed in %s | %s -> %s | %s | %s\n", g.DurationString(delta), replication.Source, replication.Target, successStr, failureStr)
	}

	if state, _ := replication.RuntimeState(); state != nil && sling.IsPipelineRunMode() {
		sling.SetPipelineStoreEnv(state.Store)
	}

	return eG.Err()
}

func runPipeline(pipelineCfgPath string) (err error) {
	g.Debug("Sling version: %s (%s %s)", core.Version, runtime.GOOS, runtime.GOARCH)

	pipeline, err := sling.LoadPipelineConfigFromFile(pipelineCfgPath)
	if err != nil {
		return g.Error(err, "could not load pipeline: %s", pipelineCfgPath)
	}

	// load SLING_TIMEOUT if specified in pipeline env
	timeoutR := pipeline.Env["SLING_TIMEOUT"]
	timeoutE := os.Getenv("SLING_TIMEOUT")

	setTimeout(cast.ToString(timeoutR), timeoutE)

	pipelineMap := g.M()

	// track usage
	defer func() {
		steps := []map[string]any{}
		for _, s := range pipeline.GetSteps() {
			steps = append(steps, s.PayloadMap())
		}

		pipelineMap["md5"] = pipeline.MD5
		pipelineMap["steps"] = steps

		if err != nil {
			env.SetTelVal("error", getErrString(err))
		}
		env.SetTelVal("pipeline", g.Marshal(pipelineMap))

		// telemetry
		Track("run")
	}()

	pipeline.Context = ctx
	err = pipeline.Execute()

	return
}

func runDirectory(directoryPath string) (err error) {
	if !g.PathExists(directoryPath) {
		return g.Error("directory does not exist: %s", directoryPath)
	}

	files, err := g.ListDirRecursive(directoryPath)
	if err != nil {
		return g.Error(err, "could not list files from %s", directoryPath)
	}

	// collect replication or pipeline
	runFiles := sling.RunFiles{}
	for _, file := range files {
		if strings.HasSuffix(file.Name, ".yaml") || strings.HasSuffix(file.Name, ".yml") {
			runFile := sling.RunFile{File: file}
			valid, err := runFile.IsValid()
			if err != nil {
				return g.Error(err, "could not load file: %s", file.RelPath)
			} else if valid {
				runFiles = append(runFiles, runFile)
			}
		}
	}

	// get terminal width, fallback to 80 if unable to determine
	width := 80
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil {
		width = int(float64(w) * 0.75)
	}

	// change working dir
	if err = os.Chdir(directoryPath); err != nil {
		return g.Error(err, "could not change working directory to %s", directoryPath)
	}

	// run them
	startTime := time.Now()
	eG := g.ErrorGroup{}
	successes := 0
	for _, runFile := range runFiles.Order() {
		ctx = g.NewContext(context.Background()) // reset master context

		fileName := g.F("%s (%s)", runFile.File.RelPath, runFile.Type)
		remainingWidth := width - len(fileName) - 2 // subtract 2 for spaces around filename
		leftSigns := remainingWidth / 2
		rightSigns := remainingWidth - leftSigns // accounts for odd numbers
		env.Println(env.GreenString(g.F("%s %s %s", strings.Repeat("=", leftSigns), fileName, strings.Repeat("=", rightSigns))))

		switch runFile.Type {
		case sling.RunFilePipeline:
			err = runPipeline(runFile.File.RelPath)
		case sling.RunFileReplication:
			err = runReplication(runFile.File.RelPath, nil)
		}
		if eG.Capture(err, runFile.File.RelPath) {
			g.LogError(err)
		} else {
			successes++
		}
	}

	delta := time.Since(startTime)
	env.Println(env.GreenString(strings.Repeat("=", width)))
	successStr := env.GreenString(g.F("%d Successes", successes))
	failureStr := g.F("%d Failures", len(eG.Errors))
	if len(eG.Errors) > 0 {
		failureStr = env.RedString(failureStr)
	} else {
		failureStr = env.GreenString(failureStr)
	}

	g.Info("Sling Run Completed in %s | %s | %s\n", g.DurationString(delta), successStr, failureStr)

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
	projectID = os.Getenv("SLING_PROJECT_ID")

	if cfgPath == "" && !strings.HasPrefix(cfgPath, "{") {
		return
	}

	cfgPath, _ = filepath.Abs(cfgPath)

	if fs, err := os.Stat(cfgPath); err == nil && !fs.IsDir() {
		if projectID == "" {
			projectID = g.GetRootCommit(filepath.Dir(cfgPath))
		}
	}
}

func testOutput(rowCnt int64, totalBytes, constraintFails uint64) error {
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

	if expected := os.Getenv("SLING_CONSTRAINT_FAILS"); expected != "" {

		if strings.HasPrefix(expected, ">") {
			atLeast := cast.ToUint64(strings.TrimPrefix(expected, ">"))
			if constraintFails <= atLeast {
				return g.Error("Expected at least %d constraint failures, got %d", atLeast+1, constraintFails)
			}
			return nil
		}

		if constraintFails != cast.ToUint64(expected) {
			return g.Error("Expected %d constraint failures, got %d", cast.ToInt(expected), constraintFails)
		}
	}

	return nil
}

func setTimeout(values ...string) (deadline time.Time) {
	for _, timeout := range values {
		if timeout == "" && timeout != "0" {
			continue
		}
		// only process first non-empty value
		duration := time.Duration(cast.ToFloat64(timeout) * float64(time.Minute))
		parent, cancel := context.WithTimeout(context.Background(), duration)
		_ = cancel

		ctx = g.NewContext(parent) // overwrite global context
		time.AfterFunc(duration-time.Second, func() {
			filePath := dumpRuntimeStack()
			if cast.ToBool(os.Getenv("SLING_TIMEOUT_STACK")) {
				// Print all goroutine stacks before panicking
				content, _ := os.ReadFile(filePath)
				env.Println(string(content))
				panic(g.F("SLING_TIMEOUT = %s mins reached!", timeout))
			} else {
				g.Warn("SLING_TIMEOUT = %s mins reached!", timeout)
			}
		})

		// set deadline for status setting later
		g.Debug("setting timeout for %s minutes", timeout)
		deadline = time.Now().Add(duration)
		ctx.Map.Set("timeout-deadline", deadline.Unix())
		break
	}
	return deadline
}

func dumpRuntimeStack() string {
	buf := make([]byte, 1<<20) // 1MB buffer
	stackLen := runtime.Stack(buf, true)

	folderPath := path.Join(env.GetTempFolder(), "sling", "runtime")
	filePath := path.Join(folderPath, g.NowFileStr())
	if os.MkdirAll(folderPath, 0755) == nil {
		if os.WriteFile(filePath, buf[:stackLen], 0755) == nil {
			os.Setenv("SLING_RUNTIME_DUMP_FILE", filePath)
			return filePath
		}
	}
	return ""
}
