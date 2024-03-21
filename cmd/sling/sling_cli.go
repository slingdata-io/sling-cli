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

	"github.com/denisbrodbeck/machineid"
	"github.com/getsentry/sentry-go"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
)

//go:embed *
var slingFolder embed.FS
var examples = ``
var ctx = g.NewContext(context.Background())
var telemetryMap = g.M("begin_time", time.Now().UnixMicro(), "run_mode", "cli")
var telemetry = true
var interrupted = false
var machineID = ""
var sentryOptions = sentry.ClientOptions{
	// Either set your DSN here or set the SENTRY_DSN environment variable.
	Dsn: "https://abb36e36341a4a2fa7796b6f9a0b3766@o881232.ingest.sentry.io/5835484",
	// Either set environment and release here or set the SENTRY_ENVIRONMENT
	// and SENTRY_RELEASE environment variables.
	Environment: lo.Ternary(core.Version == "dev", "Development", "Production"),
	Release:     "sling@" + core.Version,
	// Enable printing of SDK debug messages.
	// Useful when getting started or trying to figure something out.
	Debug: false,
}

func init() {
	env.InitLogger()
}

var cliRun = &g.CliSC{
	Name:                  "run",
	Description:           "Execute a run",
	AdditionalHelpPrepend: "\nSee more examples and configuration details at https://docs.slingdata.io/sling-cli/",
	Flags: []g.Flag{
		{
			Name:        "config",
			ShortName:   "c",
			Type:        "string",
			Description: "The task config string or file to use (JSON or YAML).",
		},
		{
			Name:        "replication",
			ShortName:   "r",
			Type:        "string",
			Description: "The replication config file to use (JSON or YAML).\n",
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
			Description: "in-line environment variable map to pass in (JSON or YAML).",
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
			Name:        "iterate",
			ShortName:   "",
			Type:        "string",
			Description: "Have sling run continuously a number of times (useful for backfilling).\n                       Accepts an integer above 0, or 'infinite' to run indefinitely. If the run fails, sling will exit",
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
			Name:        "examples",
			ShortName:   "e",
			Type:        "bool",
			Description: "Shows some examples.",
		},
	},
	ExecProcess: processRun,
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
	Description:           "Manage local connections in the sling env file",
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
					Name:        "selector",
					ShortName:   "s",
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
					Name:        "query",
					ShortName:   "",
					Type:        "string",
					Description: "The SQL query to execute. Can be in-line text or a file",
				},
			},
		},
	},
	ExecProcess: processConns,
}

func init() {

	if val := os.Getenv("SLING_DISABLE_TELEMETRY"); val != "" {
		telemetry = !cast.ToBool(val)
	}

	// collect examples
	examplesBytes, _ := slingFolder.ReadFile("examples.sh")
	examples = string(examplesBytes)

	// cliInteractive.Make().Add()
	// cliAuth.Make().Add()
	// cliCloud.Make().Add()
	cliConns.Make().Add()
	// cliProject.Make().Add()
	cliRun.Make().Add()
	cliUpdate.Make().Add()
	// cliUi.Make().Add()

	if telemetry {
		if projectID == "" {
			projectID = os.Getenv("GITHUB_REPOSITORY_ID")
		}
		machineID, _ = machineid.ProtectedID("sling")
		if projectID != "" {
			machineID = g.MD5(projectID) // hashed
		}
		sentry.Init(sentryOptions)
	}
}

func Track(event string, props ...map[string]interface{}) {
	if !telemetry || core.Version == "dev" {
		return
	}

	properties := g.M(
		"application", "sling-cli",
		"version", core.Version,
		"package", getSlingPackage(),
		"os", runtime.GOOS+"/"+runtime.GOARCH,
		"emit_time", time.Now().UnixMicro(),
		"user_id", machineID,
	)

	for k, v := range telemetryMap {
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
		resp, respBytes, _ := net.ClientDo(http.MethodPost, env.PlausibleURL, body, h, 5)
		if resp != nil {
			g.Trace("post event response: %s\n%s", resp.Status, string(respBytes))
		}
	}
}

func main() {

	exitCode := 11
	done := make(chan struct{})
	interrupt := make(chan os.Signal, 1)
	kill := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	signal.Notify(kill, syscall.SIGTERM)

	sling.ShowProgress = os.Getenv("SLING_SHOW_PROGRESS") != "false"
	database.UseBulkExportFlowCSV = cast.ToBool(os.Getenv("SLING_BULK_EXPORT_FLOW_CSV"))

	go func() {
		defer close(done)
		exitCode = cliInit()
	}()

	exit := func() {
		time.Sleep(50 * time.Millisecond) // so logger can flush
		os.Exit(exitCode)
	}

	select {
	case <-done:
		exit()
	case <-kill:
		env.Println("\nkilling process...")
		exitCode = 111
		exit()
	case <-interrupt:
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
	}
}

func cliInit() int {

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			telemetryMap["error"] = g.F("panic occurred! %#v\n%s", r, string(debug.Stack()))
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

	ok, err := g.CliProcess()

	if time.Now().UnixMicro()%20 == 0 {
		defer SlingMedia.PrintFollowUs()
	}

	if err != nil || telemetryMap["error"] != nil {
		if err == nil && telemetryMap["error"] != nil {
			err = g.Error(cast.ToString(telemetryMap["error"]))
		}

		if g.In(g.CliObj.Name, "conns", "update") || telemetryMap["error"] == nil {
			telemetryMap["error"] = getErrString(err)

			eventName := g.CliObj.Name
			if g.CliObj.UsedSC() != "" {
				eventName = g.CliObj.Name + "_" + g.CliObj.UsedSC()
			}
			Track(eventName)
		}

		// sentry details
		if telemetry && core.Version != "dev" {
			evt := sentry.NewEvent()
			evt.Environment = sentryOptions.Environment
			evt.Release = sentryOptions.Release
			evt.Level = sentry.LevelError
			evt.Exception = []sentry.Exception{
				{
					Type: err.Error(),
					// Value:      err.Error(),
					Stacktrace: sentry.ExtractStacktrace(err),
				},
			}

			// set transaction
			taskMap, _ := g.UnmarshalMap(cast.ToString(telemetryMap["task"]))
			sourceType := lo.Ternary(taskMap["source_type"] == nil, "unknown", cast.ToString(taskMap["source_type"]))
			targetType := lo.Ternary(taskMap["target_type"] == nil, "unknown", cast.ToString(taskMap["target_type"]))
			evt.Transaction = g.F("%s - %s", sourceType, targetType)
			if g.CliObj.Name == "conns" {
				targetType = lo.Ternary(telemetryMap["conn_type"] == nil, "unknown", cast.ToString(telemetryMap["conn_type"]))
				evt.Transaction = g.F(targetType)
			}

			taskType := lo.Ternary(taskMap["type"] == nil, "unknown", cast.ToString(taskMap["type"]))
			E, ok := err.(*g.ErrType)
			if ok {
				lastCaller := E.LastCaller()
				evt.Exception[0].Type = lastCaller                            // the title
				evt.Exception[0].Value = g.F("%s (%s)", lastCaller, taskType) // the subtitle

				if arr := strings.Split(lastCaller, " "); len(arr) > 1 {
					evt.Exception[0].Type = strings.TrimPrefix(lastCaller, arr[0]+" ")
				}

				evt.Exception[0].Stacktrace = sentry.ExtractStacktrace(err)

				evt.Message = E.Debug()
			}

			if eh := sling.ErrorHelper(err); eh != "" {
				evt.Message = evt.Message + "\n\n" + eh
			}

			sentry.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetUser(sentry.User{ID: machineID})
				if g.CliObj.Name == "conns" {
					scope.SetTag("target_type", targetType)
				} else {
					scope.SetTag("source_type", sourceType)
					scope.SetTag("target_type", targetType)
					scope.SetTag("run_mode", cast.ToString(telemetryMap["run_mode"]))
					if val := cast.ToString(taskMap["mode"]); val != "" {
						scope.SetTag("mode", val)
					}
					if val := cast.ToString(taskMap["type"]); val != "" {
						scope.SetTag("type", val)
					}
					scope.SetTag("package", getSlingPackage())
					if projectID != "" {
						scope.SetTag("project_id", projectID)
					}
				}
			})

			sentry.CaptureEvent(evt)
			// sentry.CaptureException(err)
			sentry.Flush(2 * time.Second)
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
