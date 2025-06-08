package main

import (
	"context"
	"embed"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/slingdata-io/sling-cli/core/store"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
)

//go:embed resource/*
var slingResources embed.FS
var (
	examples    = ``
	ctx         = g.NewContext(context.Background())
	telemetry   = true
	interrupted = false
	machineID   = ""
)

func init() {
	env.InitLogger()
	store.InitDB()

	if os.Getenv("SLING_EXEC_ID") == "" {
		os.Setenv("SLING_EXEC_ID", sling.NewExecID()) // set exec id if none provided
	}
}

var cliRunFlags = []g.Flag{
	{
		Name:        "replication",
		ShortName:   "r",
		Type:        "string",
		Description: "The replication config file to use (JSON or YAML).\n",
	},
	{
		Name:        "pipeline",
		ShortName:   "p",
		Type:        "string",
		Description: "The pipeline config file to use (JSON or YAML).\n",
	},
	{
		Name:        "config",
		ShortName:   "c",
		Type:        "string",
		Description: "The task config string or file to use (JSON or YAML). [deprecated]",
	},
	{
		Name:        "src-conn",
		ShortName:   "",
		Type:        "string",
		Description: "The source database / storage connection (name, conn string or URL).",
	},
	{
		Name:        "src-stream",
		ShortName:   "",
		Type:        "string",
		Description: "The source table (schema.table), local / cloud file path.\n                       Can also be the path of sql file or in-line text to use as query. Use `file://` for local paths.",
	},
	{
		Name:        "src-options",
		Type:        "string",
		Description: "in-line options to further configure source (JSON or YAML).\n",
	},
	{
		Name:        "tgt-conn",
		ShortName:   "",
		Type:        "string",
		Description: "The target database connection (name, conn string or URL).",
	},
	{
		Name:        "tgt-object",
		ShortName:   "",
		Type:        "string",
		Description: "The target table (schema.table) or local / cloud file path. Use `file://` for local paths.",
	},
	{
		Name:        "tgt-options",
		Type:        "string",
		Description: "in-line options to further configure target (JSON or YAML).\n",
	},
	{
		Name:        "select",
		ShortName:   "s",
		Type:        "string",
		Description: "Select or exclude specific columns from the source stream. (comma separated). Use '-' prefix to exclude.",
	},
	{
		Name:        "where",
		Type:        "string",
		Description: "Specify the WHERE clause to filter (if not providing custom SQL)",
	},
	{
		Name:        "transforms",
		ShortName:   "",
		Type:        "string",
		Description: "An object/map, or array/list of built-in transforms to apply to records (JSON or YAML).",
	},
	{
		Name:        "columns",
		ShortName:   "",
		Type:        "string",
		Description: "An object/map to specify the type that a column should be cast as (JSON or YAML).",
	},
	{
		Name:        "streams",
		ShortName:   "",
		Type:        "string",
		Description: "Only run specific streams from a replication. (comma separated)",
	},
	{
		Name:        "stdout",
		ShortName:   "",
		Type:        "bool",
		Description: "Output the stream to standard output (STDOUT).",
	},
	{
		Name:        "env",
		ShortName:   "",
		Type:        "string",
		Description: "in-line environment variable object/map to pass in (JSON or YAML).",
	},
	{
		Name:        "mode",
		ShortName:   "m",
		Type:        "string",
		Description: "The target load mode to use: backfill, incremental, truncate, snapshot, full-refresh.\n                       Default is full-refresh. For incremental, must provide `update-key` and `primary-key` values.\n                       All modes load into a new temp table on tgtConn prior to final load.",
	},
	{
		Name:        "limit",
		ShortName:   "l",
		Type:        "string",
		Description: "The maximum number of rows to pull.",
	},
	{
		Name:        "offset",
		ShortName:   "o",
		Type:        "string",
		Description: "The number of rows to offset by.",
	},
	{
		Name:        "range",
		ShortName:   "",
		Type:        "string",
		Description: "The range to use for backfill mode, separated by a single comma. Example: `2021-01-01,2021-02-01` or `1,10000`",
	},
	{
		Name:        "primary-key",
		ShortName:   "",
		Type:        "string",
		Description: "The primary key to use for incremental. For composite key, put comma delimited values.",
	},
	{
		Name:        "update-key",
		ShortName:   "",
		Type:        "string",
		Description: "The update key to use for incremental.\n",
	},
	{
		Name:        "debug",
		ShortName:   "d",
		Type:        "bool",
		Description: "Set logging level to DEBUG.",
	},
	{
		Name:        "trace",
		Type:        "bool",
		Description: "Set logging level to TRACE (do not use in production).",
	},
	{
		Name:        "examples",
		ShortName:   "e",
		Type:        "bool",
		Description: "Shows some examples.",
	},
}

var cliRun = &g.CliSC{
	Name:                  "run",
	Description:           "Execute a run",
	AdditionalHelpPrepend: "\nSee more examples and configuration details at https://docs.slingdata.io/sling-cli/",
	Flags:                 cliRunFlags,
	ExecProcess:           processRun,
}

var cliInteractive = &g.CliSC{
	Name:        "it",
	Description: "launch interactive mode",
	ExecProcess: slingPrompt,
}

var cliUpdate = &g.CliSC{
	Name:        "update",
	Description: "Update Sling to the latest version",
	ExecProcess: updateCLI,
}

var cliConns = &g.CliSC{
	Name:                  "conns",
	Singular:              "local connection",
	Description:           "Manage and interact with local connections",
	AdditionalHelpPrepend: "\nSee more details at https://docs.slingdata.io/sling-cli/",
	SubComs: []*g.CliSC{
		{
			Name:        "discover",
			Description: "list available streams in connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "pattern",
					ShortName:   "p",
					Type:        "string",
					Description: "filter stream name by glob pattern (e.g. schema.prefix_*, dir/*.csv, dir/**/*.json, */*/*.parquet)",
				},
				{
					Name:        "recursive",
					ShortName:   "",
					Type:        "bool",
					Description: "List all files recursively.",
				},
				{
					Name:        "columns",
					ShortName:   "",
					Type:        "bool",
					Description: "Show column level metadata.",
				},
				{
					Name:        "debug",
					ShortName:   "d",
					Type:        "bool",
					Description: "Set logging level to DEBUG.",
				},
				{
					Name:        "trace",
					Type:        "bool",
					Description: "Set logging level to TRACE (do not use in production).",
				},
			},
		},
		{
			Name:        "list",
			Description: "list local connections detected",
		},
		{
			Name:        "test",
			Description: "test a local connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to test",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "debug",
					ShortName:   "d",
					Type:        "bool",
					Description: "Set logging level to DEBUG.",
				},
				{
					Name:        "trace",
					Type:        "bool",
					Description: "Set logging level to TRACE (do not use in production).",
				},
				{
					Name:        "endpoints",
					Type:        "string",
					Description: "The endpoint(s) to test, for API connections only",
				},
			},
		},
		{
			Name:        "unset",
			Description: "remove a connection from the sling env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to remove",
				},
			},
		},
		{
			Name:        "set",
			Description: "set a connection in the sling env file",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to set",
				},
				{
					Name:        "key=value properties...",
					ShortName:   "",
					Type:        "string",
					Description: "The key=value properties to set. See https://docs.slingdata.io/sling-cli/environment#set-connections",
				},
			},
		},
		{
			Name:        "exec",
			Description: "execute a SQL query on a Database connection",
			PosFlags: []g.Flag{
				{
					Name:        "name",
					ShortName:   "",
					Type:        "string",
					Description: "The name of the connection to set",
				},
				{
					Name:        "queries...",
					ShortName:   "",
					Type:        "string",
					Description: "The SQL queries to execute. Can be in-line text or a file",
				},
			},
			Flags: []g.Flag{
				{
					Name:        "debug",
					ShortName:   "d",
					Type:        "bool",
					Description: "Set logging level to DEBUG.",
				},
				{
					Name:        "trace",
					Type:        "bool",
					Description: "Set logging level to TRACE (do not use in production).",
				},
			},
		},
	},
	ExecProcess: processConns,
}

var cliCloud = &g.CliSC{
	Name:                  "cloud",
	Singular:              "cloud",
	Description:           "Manage and trigger replications on the cloud",
	AdditionalHelpPrepend: "\nSee more details at https://docs.slingdata.io/sling-cli/",
	SubComs: []*g.CliSC{
		{
			Name:        "login",
			Description: "Authenticate into Sling Cloud",
		},
		{
			Name:        "status",
			Description: "Get status of replications",
		},
		{
			Name:        "deploy",
			Description: "deploy one or more replications to the cloud",
			PosFlags: []g.Flag{
				{
					Name:        "replication",
					ShortName:   "r",
					Type:        "string",
					Description: "The file or folder path of YAML / JSON replication file(s)",
				},
			},
		},
		{
			Name:        "run",
			Description: "Execute a run on the cloud",
			Flags:       cliRunFlags,
		},
	},
	ExecProcess: processCloud,
}

func init() {

	if val := os.Getenv("SLING_DISABLE_TELEMETRY"); val != "" {
		telemetry = !cast.ToBool(val)
	}

	// collect examples
	examplesBytes, _ := slingResources.ReadFile("resource/examples.sh")
	examples = string(examplesBytes)

	cliConns.Make().Add()
	cliRun.Make().Add()
	cliUpdate.Make().Add()

	if projectID == "" {
		projectID = os.Getenv("SLING_PROJECT_ID")
	}
}

func Track(event string, props ...map[string]interface{}) {
	if !telemetry || core.Version == "dev" {
		return
	}

	machineID = store.GetMachineID()
	if projectID != "" {
		machineID = g.MD5(projectID) // hashed
	}

	properties := g.M(
		"package", getSlingPackage(),
		"emit_time", time.Now().UnixMicro(),
		"user_id", machineID,
	)

	for k, v := range core.TelProps {
		properties[k] = v
	}

	for k, v := range env.TelMap {
		properties[k] = v
	}

	if len(props) > 0 {
		for k, v := range props[0] {
			properties[k] = v
		}
	}

	properties["command"] = g.CliObj.Name
	if g.CliObj != nil {
		if g.CliObj.UsedSC() != "" {
			properties["sub-command"] = g.CliObj.UsedSC()
		}
	}

	if env.PlausibleURL != "" {
		propsPayload := g.Marshal(properties)
		payload := map[string]string{
			"name":     event,
			"url":      "http://events.slingdata.io/sling-cli",
			"props":    propsPayload,
			"referrer": "http://" + getSlingPackage(),
		}
		h := map[string]string{
			"Content-Type": "application/json",
			"User-Agent":   g.F("sling-cli/%s (%s) %s", core.Version, runtime.GOOS, machineID),
		}
		body := strings.NewReader(g.Marshal(payload))
		net.ClientDo(http.MethodPost, env.PlausibleURL, body, h, 5)
	}
}

func main() {

	exitCode := 11
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	kill := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(kill, syscall.SIGTERM)

	if val := os.Getenv("SLING_SHOW_PROGRESS"); val != "" {
		sling.ShowProgress = cast.ToBool(os.Getenv("SLING_SHOW_PROGRESS"))
	}
	database.UseBulkExportFlowCSV = cast.ToBool(os.Getenv("SLING_BULK_EXPORT_FLOW_CSV"))

	exit := func() {
		time.Sleep(50 * time.Millisecond) // so logger can flush
		os.Exit(exitCode)
	}

	go func() {
		select {
		case <-kill:
			env.Println("\nkilling process...")
			exitCode = 111
			exit()
		case <-interrupt:
			g.SentryClear()
			if cliRun.Sc.Used {
				env.Println("\ninterrupting...")
				interrupted = true
				ctx.Cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				}
			}
			exit()
			return
		case <-done:
			exit()
		}
	}()

	exitCode = cliInit(done)
	if !interrupted {
		g.SentryFlush(time.Second * 2)
	}
	time.Sleep(50 * time.Millisecond) // so logger can flush

	os.Exit(exitCode)
}

func cliInit(done chan struct{}) int {
	defer close(done)

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			info := string(debug.Stack())
			env.SetTelVal("error", g.F("panic occurred! %#v\n%s", r, info))
			g.Warn(g.F("panic occurred! %#v\n%s", r, info))
		}
	}()

	// Set your program's name and description.  These appear in help output.
	flaggy.SetName("sling")
	flaggy.SetDescription("An Extract-Load tool | https://slingdata.io")
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target.\nVersion " + core.Version

	flaggy.SetVersion(core.Version)
	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.ShowHelpOnUnexpectedDisable()
	flaggy.Parse()

	setSentry()
	ok, err := g.CliProcess()

	if err != nil || env.TelMap["error"] != nil {
		if err == nil && env.TelMap["error"] != nil {
			err = g.Error(cast.ToString(env.TelMap["error"]))
		}

		if g.In(g.CliObj.Name, "conns", "update") || env.TelMap["error"] == nil {
			env.SetTelVal("error", getErrString(err))

			eventName := g.CliObj.Name
			if g.CliObj.UsedSC() != "" {
				eventName = g.CliObj.Name + "_" + g.CliObj.UsedSC()
			}
			Track(eventName)
		}

		g.PrintFatal(err)
		return 1
	} else if !ok {
		flaggy.ShowHelp("")
	}

	switch {
	case g.CliObj.Name == "conns" && g.In(g.CliObj.UsedSC(), "test", "discover", "exec"):
		Track("conns_" + g.CliObj.UsedSC())
	case g.CliObj.Name == "update":
		Track("update")
	}

	return 0
}

func getErrString(err error) (errString string) {
	if err != nil {
		errString = err.Error()
		E, ok := err.(*g.ErrType)
		if ok && E.Debug() != "" {
			errString = E.Debug()
		}
	}
	return
}

func setSentry() {

	if telemetry {
		g.SentryRelease = g.F("sling@%s", core.Version)
		g.SentryDsn = env.SentryDsn
		g.SentryConfigureFunc = func(se *g.SentryEvent) bool {
			if se.Exception.Err == "context canceled" {
				return false
			}

			// set transaction
			taskMap, _ := g.UnmarshalMap(cast.ToString(env.TelMap["task"]))
			sourceType := lo.Ternary(taskMap["source_type"] == nil, "unknown", cast.ToString(taskMap["source_type"]))
			targetType := lo.Ternary(taskMap["target_type"] == nil, "unknown", cast.ToString(taskMap["target_type"]))
			se.Event.Transaction = "cli"

			// format telMap
			telMap, _ := g.UnmarshalMap(g.Marshal(env.TelMap))
			if val, ok := telMap["task"]; ok {
				telMap["task"], _ = g.UnmarshalMap(cast.ToString(val))
			}
			if val, ok := telMap["task_options"]; ok {
				telMap["task_options"], _ = g.UnmarshalMap(cast.ToString(val))
			}
			if val, ok := telMap["task_stats"]; ok {
				telMap["task_stats"], _ = g.UnmarshalMap(cast.ToString(val))
			}
			delete(telMap, "error")
			bars := "--------------------------------------------------------"
			se.Event.Message = se.Exception.Debug() + "\n\n" + bars + "\n\n" + g.Pretty(telMap)

			e := se.Event.Exception[0]
			se.Event.Exception[0].Type = e.Stacktrace.Frames[len(e.Stacktrace.Frames)-1].Function
			se.Event.Exception[0].Value = g.F("%s [%s]", se.Exception.LastCaller(), se.Exception.CallerStackMD5())

			se.Scope.SetUser(sentry.User{ID: machineID})
			if g.CliObj.Name == "conns" {
				se.Scope.SetTag("run_mode", "conns")
				se.Scope.SetTag("target_type", targetType)
			} else {
				se.Scope.SetTag("source_type", sourceType)
				se.Scope.SetTag("target_type", targetType)
				se.Scope.SetTag("stage", cast.ToString(env.TelMap["stage"]))
				se.Scope.SetTag("run_mode", cast.ToString(env.TelMap["run_mode"]))
				if val := cast.ToString(taskMap["mode"]); val != "" {
					se.Scope.SetTag("mode", val)
				}
				if val := cast.ToString(taskMap["type"]); val != "" {
					se.Scope.SetTag("type", val)
				}
				se.Scope.SetTag("package", getSlingPackage())
				if projectID != "" {
					se.Scope.SetTag("project_id", projectID)
				}
			}

			return true
		}
		g.SentryInit()
	}
}
