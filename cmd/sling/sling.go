package main

import (
	"context"
	"embed"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/flarco/sling/core"
	"github.com/flarco/sling/core/env"
	"github.com/flarco/sling/core/sling"
	"github.com/getsentry/sentry-go"
	"github.com/rudderlabs/analytics-go"

	"github.com/flarco/dbio/database"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/spf13/cast"
)

//go:embed *
var slingFolder embed.FS
var examples = ``
var ctx, cancel = context.WithCancel(context.Background())

var cliRun = &g.CliSC{
	Name:        "run",
	Description: "Execute an ad-hoc task",
	Flags: []g.Flag{
		{
			Name:        "config",
			ShortName:   "c",
			Type:        "string",
			Description: "The config string or file to use (JSON or YAML).\n",
		},
		{
			Name:        "src-conn",
			ShortName:   "",
			Type:        "string",
			Description: "The source database / API connection (name, conn string or URL).",
		},
		{
			Name:        "src-stream",
			ShortName:   "",
			Type:        "string",
			Description: "The source table (schema.table), local / cloud file path or API supported object name.\n                       Can also be the path of sql file or in-line text to use as query. Use `file://` for local paths.",
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
			Name:        "stdout",
			ShortName:   "",
			Type:        "bool",
			Description: "Output the stream to standard output (STDOUT).",
		},
		{
			Name:        "mode",
			ShortName:   "m",
			Type:        "string",
			Description: "The target load mode to use: append, upsert, truncate, drop.\n                       Default is append. For upsert, must provide `update-key` and `primary-key` values.\n                       All modes load into a new temp table on tgtConn prior to final load.",
		},
		{
			Name:        "primary-key",
			ShortName:   "",
			Type:        "string",
			Description: "The primary key to use for upsert. For composite key, put comma delimited values.",
		},
		{
			Name:        "update-key",
			ShortName:   "",
			Type:        "string",
			Description: "The update key to use for upsert.\n",
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

var cliUi = &g.CliSC{
	Name:        "ui",
	Description: "Open the Sling app in a browser",
	ExecProcess: slingUiServer,
}

var cliConns = &g.CliSC{
	Name:        "conns",
	Singular:    "local connection",
	Description: "Manage local connections",
	SubComs: []*g.CliSC{
		// {
		// 	Name:        "add",
		// 	Description: "add new connection",
		// },
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
	},
	ExecProcess: processConns,
}

var cliAuth = &g.CliSC{
	Name:        "auth",
	Description: "Sling cloud authentication",
	SubComs: []*g.CliSC{
		{
			Name:        "login",
			Description: "Log in from existing sling cloud account",
			ExecProcess: processAuth,
		},
		{
			Name:        "logout",
			Description: "Log out the currently logged in user",
			ExecProcess: processAuth,
		},
		{
			Name:        "signup",
			Description: "Create a new sling cloud account",
			ExecProcess: processAuth,
		},
		{
			Name:        "token",
			Description: "Show the current auth token",
			ExecProcess: processAuth,
		},
	},
}

var cliProjectTasksSubComs = []*g.CliSC{
	{
		Name:        "list",
		Description: "List project tasks",
		ExecProcess: processProjectTasks,
	},
	{
		Name:        "show",
		Description: "Show a singular task's details",
		PosFlags: []g.Flag{
			{
				Name:        "task-id",
				Type:        "string",
				Description: "The task id to lookup",
			},
		},
		ExecProcess: processProjectTasks,
	},
	{
		Name:        "toggle-active",
		Description: "Enable / disable task schedule",
		ExecProcess: processProjectTasks,
		PosFlags: []g.Flag{
			{
				Name:        "selector",
				ShortName:   "s",
				Type:        "string",
				Description: "The selector string to filter affected records",
			},
		},
	},
	{
		Name:        "trigger",
		Description: "Send one or more tasks to cloud queue for execution, immediately",
		ExecProcess: processProjectTasks,
		PosFlags: []g.Flag{
			{
				Name:        "selector",
				ShortName:   "s",
				Type:        "string",
				Description: "The selector string to filter affected records",
			},
		},
		Flags: []g.Flag{
			{
				Name:        "dry-run",
				Type:        "bool",
				Description: "Returns a list of tasks to be executed. Good for validation.",
			},
		},
	},
	{
		Name:        "terminate",
		Description: "Terminate one or more tasks currently executing",
		ExecProcess: processProjectTasks,
		PosFlags: []g.Flag{
			{
				Name:        "selector",
				ShortName:   "s",
				Type:        "string",
				Description: "The selector string to filter affected records",
			},
		},
		Flags: []g.Flag{
			{
				Name:        "dry-run",
				Type:        "bool",
				Description: "Returns a list of tasks to be executed. Good for validation.",
			},
		},
	},
	{
		Name:        "generate",
		Description: "Auto-generate task files from a database schemata",
		ExecProcess: processProjectTasksGenerate,
	},
}

var cliProjectWorkersSubComs = []*g.CliSC{
	{
		Name:        "list",
		Description: "List registered project self-hosted workers",
		ExecProcess: processProjectWorkersList,
	},
	{
		Name:        "attach",
		Description: "Register & attach a self-hosted worker to project",
		ExecProcess: processProjectWorkersAttach,
	},
	{
		Name:        "detach",
		Description: "Detach a self-hosted worker from project",
		ExecProcess: processProjectWorkersDetach,
	},
}

var cliProjectConnsSubComs = []*g.CliSC{
	{
		Name:        "set",
		Description: "Set (create or update) a project connection",
		ExecProcess: processProjectConns,
	},
	{
		Name:        "unset",
		Description: "Unset (delete) a project connection",
		PosFlags: []g.Flag{
			{
				Name:        "name",
				ShortName:   "",
				Type:        "string",
				Description: "The name of the connection to test",
			},
		},
		ExecProcess: processProjectConns,
	},
	{
		Name:        "list",
		Description: "List the project connections. Only the name & type will be shown.",
		ExecProcess: processProjectConns,
	},
	{
		Name:        "test",
		Description: "Test connectivity to a project connection",
		PosFlags: []g.Flag{
			{
				Name:        "name",
				ShortName:   "",
				Type:        "string",
				Description: "The name of the connection to test",
			},
		},
		ExecProcess: processProjectConns,
	},
}

var cliProjectVarsSubComs = []*g.CliSC{
	{
		Name:        "set",
		Description: "Set (create or update) a project environment variable",
		ExecProcess: processProjectVars,
	},
	{
		Name:        "unset",
		Description: "Unset (delete) a project environment variable",
		PosFlags: []g.Flag{
			{
				Name:        "name",
				ShortName:   "",
				Type:        "string",
				Description: "The name of the connection to test",
			},
		},
		ExecProcess: processProjectVars,
	},
	{
		Name:        "list",
		Description: "List the project's environment variables. Only the name will be shown.",
		ExecProcess: processProjectVars,
	},
}

var cliProject = &g.CliSC{
	Name:        "project",
	Description: "Manage a sling cloud project",
	SubComs: []*g.CliSC{
		{
			Name:        "init",
			Description: "Initiate a new project",
		},
		{
			Name:        "tasks",
			Description: "Manage the project tasks",
			SubComs:     cliProjectTasksSubComs,
		},
		{
			Name:        "validate",
			Description: "Validates project tasks and environment. Is ran prior to deployment",
			ExecProcess: processProjectValidate,
		},
		{
			Name:        "deploy",
			Description: "Deploy a sling project",
			ExecProcess: processProjectDeploy,
		},
		{
			Name:        "conns",
			Description: "Manage project connections",
			SubComs:     cliProjectConnsSubComs,
		},
		{
			Name:        "vars",
			Description: "Manage project environment variables",
			SubComs:     cliProjectVarsSubComs,
		},
		{
			Name:        "workers",
			Description: "Manage self-hosted workers",
			SubComs:     cliProjectWorkersSubComs,
		},
		{
			Name:        "history",
			Description: "See project activity history",
			ExecProcess: processProjectHistory,
		},
		{
			Name:        "logs",
			Description: "See project task execution logs",
			PosFlags: []g.Flag{
				{
					Name:        "exec-id",
					ShortName:   "",
					Type:        "string",
					Description: "The ID of the task execution",
				},
			},
			ExecProcess: processProjectLogs,
		},
	},
}

func init() {
	// we need a webserver to get the pprof webserver
	if os.Getenv("SLING_PPROF") == "TRUE" {
		go func() {
			g.Trace("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}

	// collect examples
	examplesBytes, _ := slingFolder.ReadFile("examples.sh")
	examples = string(examplesBytes)

	// cliInteractive.Make().Add()
	// cliAuth.Make().Add()
	cliConns.Make().Add()
	// cliProject.Make().Add()
	cliRun.Make().Add()
	cliUpdate.Make().Add()
	// cliUi.Make().Add()

	sentry.Init(sentry.ClientOptions{
		// Either set your DSN here or set the SENTRY_DSN environment variable.
		Dsn: "https://abb36e36341a4a2fa7796b6f9a0b3766@o881232.ingest.sentry.io/5835484",
		// Either set environment and release here or set the SENTRY_ENVIRONMENT
		// and SENTRY_RELEASE environment variables.
		Environment: "Production",
		Release:     "sling@" + core.Version,
		// Enable printing of SDK debug messages.
		// Useful when getting started or trying to figure something out.
		Debug: false,
	})
}

func getRsClient() analytics.Client {
	return analytics.New("1uXKxEPgIB9HXkTduvfwXmFak2l", "https://liveflarccszw.dataplane.rudderstack.com")
}

func Track(event string, props ...g.Map) {
	if val := os.Getenv("SLING_SEND_ANON_USAGE"); val != "" {
		if !cast.ToBool(val) {
			return
		}
	}

	rsClient := getRsClient()
	properties := analytics.NewProperties().
		Set("application", "sling-cli").
		Set("version", core.Version).
		Set("os", runtime.GOOS)

	if len(props) > 0 {
		for k, v := range props[0] {
			properties.Set(k, v)
		}
	}

	rsClient.Enqueue(analytics.Track{
		UserId:     "sling",
		Event:      event,
		Properties: properties,
	})
	rsClient.Close()
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

	select {
	case <-done:
		os.Exit(exitCode)
	case <-kill:
		println("\nkilling process...")
		os.Exit(111)
	case <-interrupt:
		if cliRun.Sc.Used {
			println("\ninterrupting...")
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		}
		os.Exit(exitCode)
		return
	}
}

func cliInit() int {
	env.InitLogger()

	// Set your program's name and description.  These appear in help output.
	flaggy.SetName("sling")
	flaggy.SetDescription("An Extract-Load tool.")
	flaggy.DefaultParser.ShowHelpOnUnexpected = true
	flaggy.DefaultParser.AdditionalHelpPrepend = "Slings data from a data source to a data target.\nVersion " + core.Version

	flaggy.SetVersion(core.Version)
	for _, cli := range g.CliArr {
		flaggy.AttachSubcommand(cli.Sc, 1)
	}

	flaggy.Parse()

	ok, err := g.CliProcess()
	if ok {
		if err != nil {
			sentry.CaptureException(err)
		}
		g.LogFatal(err)
	} else {
		flaggy.ShowHelp("")
		// Track("ShowHelp")
	}
	return 0
}
