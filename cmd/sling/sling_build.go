package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling/build"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

var cliBuildFlags = []g.Flag{
	{
		Name:        "target",
		ShortName:   "t",
		Type:        "string",
		Description: "Target connection (required if no sling_build.yml).",
	},
	{
		Name:        "select",
		ShortName:   "s",
		Type:        "string",
		Description: "Model selector (glob, tag:xxx, +model for upstream).",
	},
	{
		Name:        "exclude",
		Type:        "string",
		Description: "Exclude models matching pattern.",
	},
	{
		Name:        "full-refresh",
		ShortName:   "f",
		Type:        "bool",
		Description: "Force full-refresh for all models.",
	},
	{
		Name:        "schema",
		Type:        "string",
		Description: "Override dev schema (forces dev mode, cannot combine with --prod).",
	},
	{
		Name:        "prod",
		Type:        "bool",
		Description: "Force prod mode (overrides yml mode: dev).",
	},
	{
		Name:        "vars",
		Type:        "string",
		Description: "Variables as YAML/JSON string.",
	},
	{
		Name:        "compile",
		ShortName:   "c",
		Type:        "bool",
		Description: "Compile only — show SQL + DAG, don't execute.",
	},
	{
		Name:        "list",
		ShortName:   "l",
		Type:        "bool",
		Description: "List selected models and exit.",
	},
	{
		Name:        "fail-fast",
		ShortName:   "x",
		Type:        "bool",
		Description: "Stop on first failure (in-flight models finish).",
	},
	{
		Name:        "no-seeds",
		Type:        "bool",
		Description: "Skip seed loading.",
	},
	{
		Name:        "range",
		Type:        "string",
		Description: "Backfill range for incremental models: 'start,end[,step]'. E.g. '2024-01-01,2024-12-31,1mo'. Does not advance SLING_STATE.",
	},
	{
		Name:        "threads",
		Type:        "string",
		Description: "Parallel model executions (default: 4).",
	},
	{
		Name:        "recursive",
		ShortName:   "R",
		Type:        "bool",
		Description: "Recursively discover sling_build.yml in immediate subdirectories.",
	},
	{
		Name:        "test",
		Type:        "bool",
		Description: "Run declarative data tests only (skip materialization).",
	},
	{
		Name:        "json",
		Type:        "bool",
		Description: "Emit machine-readable JSON for --compile / --list.",
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
		Description: "Set logging level to TRACE.",
	},
}

var cliBuild = &g.CliSC{
	Name:                  "build",
	Description:           "Build and execute SQL models",
	AdditionalHelpPrepend: "\nA lightweight SQL model builder with dependency resolution, Jinja templating, and incremental materializations.",
	ExecuteWithoutFlags:   true,
	Flags:                 cliBuildFlags,
	PosFlags: []g.Flag{
		{
			Name:        "path",
			Type:        "string",
			Description: "The project directory path (default: current directory).\n",
			Required:    false,
		},
	},
	ExecProcess: processBuild,
}

func init() {
	cliBuild.Make().Add()
}

func processBuild(c *g.CliSC) (ok bool, err error) {
	ok = true

	opts := build.BuildOptions{
		Threads: build.DefaultThreads,
	}

	projectPath := "."
	compileMode := false

	for k, v := range c.Vals {
		switch k {
		case "path":
			if p := cast.ToString(v); p != "" {
				projectPath = p
			}
		case "target":
			opts.Target = cast.ToString(v)
		case "select":
			if s := cast.ToString(v); s != "" {
				opts.Select = strings.Split(s, ",")
			}
		case "exclude":
			if s := cast.ToString(v); s != "" {
				opts.Exclude = strings.Split(s, ",")
			}
		case "full-refresh":
			opts.FullRefresh = cast.ToBool(v)
		case "schema":
			opts.Schema = cast.ToString(v)
		case "prod":
			opts.Prod = cast.ToBool(v)
		case "vars":
			if varsStr := cast.ToString(v); varsStr != "" {
				varsMap := make(map[string]any)
				if err := yaml.Unmarshal([]byte(varsStr), &varsMap); err != nil {
					return ok, g.Error(err, "could not parse --vars")
				}
				opts.Vars = varsMap
			}
		case "compile":
			compileMode = cast.ToBool(v)
		case "list":
			opts.List = cast.ToBool(v)
		case "fail-fast":
			opts.FailFast = cast.ToBool(v)
		case "no-seeds":
			opts.NoSeeds = cast.ToBool(v)
		case "range":
			if s := cast.ToString(v); s != "" {
				opts.Range = g.String(s)
			}
		case "threads":
			if t := cast.ToInt(v); t > 0 {
				opts.Threads = t
			}
		case "recursive":
			opts.Recursive = cast.ToBool(v)
		case "test":
			opts.Test = cast.ToBool(v)
		case "json":
			opts.JSON = cast.ToBool(v)
		case "debug":
			if cast.ToBool(v) {
				os.Setenv("DEBUG", "LOW")
				env.InitLogger()
			}
		case "trace":
			if cast.ToBool(v) {
				os.Setenv("DEBUG", "TRACE")
				env.InitLogger()
			}
		}
	}

	opts.Compile = compileMode

	os.Setenv("SLING_CLI", "TRUE")
	if os.Getenv("SLING_RUN_MODE") == "" {
		os.Setenv("SLING_RUN_MODE", "build")
	}

	// Print the same startup marker as `sling run` so the agent can detect
	// that the process is alive before the first build-status heartbeat.
	if !env.IsThreadChild {
		if env.NoColor {
			g.Info(env.Marker)
		} else {
			g.Info(env.CyanString(env.Marker))
		}
	}

	// Validate flag combinations
	if opts.Prod && opts.Schema != "" {
		return ok, g.Error("cannot combine --prod and --schema")
	}

	// If there's no sling_build.yml at the path and the user gave us nothing to
	// work with (no --target, no -r), show help instead of walking the tree.
	// This avoids slurping every .sql file under cwd as "models".
	if opts.Target == "" && !opts.Recursive {
		if _, found := build.FindConfigFile(projectPath); !found {
			flaggy.ShowHelp("")
			return ok, nil
		}
	}

	// Build and compile
	b, err := build.NewBuild(projectPath, opts)
	if err != nil {
		return ok, g.Error(err, "could not load build project")
	}

	if err := b.Compile(); err != nil {
		return ok, g.Error(err, "could not compile build project")
	}

	if opts.List {
		if opts.JSON {
			b.PrintListJSON()
		} else {
			b.PrintListOutput()
		}
		return ok, nil
	}

	if compileMode {
		if opts.JSON {
			b.PrintCompileJSON()
		} else {
			b.PrintCompileOutput()
		}
		return ok, nil
	}

	// Execute the build
	if err := b.Execute(); err != nil {
		return ok, g.Error(err, "build execution failed")
	}
	if err := testOutput(int64(b.ExecRows), b.ExecBytes, 0); err != nil {
		return ok, err
	}
	return ok, nil
}

// askPrompt writes label and reads one answer. It returns an error on EOF so
// callers that loop on empty input cannot spin when stdin closes.
func askPrompt(reader *bufio.Reader, label string) (string, error) {
	fmt.Print(label)
	input, err := reader.ReadString('\n')
	if err != nil && input == "" {
		return "", g.Error(err, "could not read input")
	}
	return strings.TrimSpace(input), nil
}

// isInteractive reports whether stdin is a TTY.
func isInteractive() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
