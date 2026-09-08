package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling/project"
	"github.com/slingdata-io/sling-cli/core/sling/validate"
	"github.com/spf13/cast"
)

var cliValidate = &g.CliSC{
	Name:                  "validate",
	Description:           "Validate Sling files",
	AdditionalHelpPrepend: "\nDetect the file kind and check the structure. Compile is the default, which confirms the file is ready to run. Compile needs connections and replaces ${VAR}. Use --parse-only for a syntax check. With no paths, validate the project from the current directory.",
	Flags: []g.Flag{
		{Name: "quiet", ShortName: "q", Type: "bool", Description: "Exit code only. No output."},
		{Name: "parse-only", Type: "bool", Description: "Skip compilation step, parse structure only. Do not replace ${VAR}."},
		{Name: "detailed", Type: "bool", Description: "Show one table per kind (streams, steps, endpoints, connections)."},
		{Name: "ndjson", Type: "bool", Description: "One JSON object per line."},
		{Name: "output", ShortName: "o", Type: "string", Description: "Output format: json. Human table is the TTY default."},
		{Name: "json", Type: "bool", Description: "Emit one JSON object keyed by path."},
		{Name: "debug", ShortName: "d", Type: "bool", Description: "Set logging level to DEBUG."},
		{Name: "trace", Type: "bool", Description: "Set logging level to TRACE."},
	},
	PosFlags: []g.Flag{
		{
			Name:        "paths...",
			Type:        "string",
			Description: "Files or folders to validate.",
			Required:    false,
		},
	},
	ExecProcess: processValidate,
}

func init() {
	cliValidate.Make().Add()
}

func processValidate(c *g.CliSC) (ok bool, err error) {
	ok = true

	if cast.ToBool(c.Vals["trace"]) {
		os.Setenv("DEBUG", "TRACE")
		env.InitLogger()
	} else if cast.ToBool(c.Vals["debug"]) {
		os.Setenv("DEBUG", "LOW")
		env.InitLogger()
	}

	paths := collectValidatePaths(c)
	if len(paths) == 0 {
		// Bare invocation inside a project validates the project root.
		wd, wdErr := os.Getwd()
		if wdErr == nil {
			if root, findErr := project.FindRoot(wd); findErr == nil && root != "" {
				g.Debug("validating project root %s", root)
				paths = []string{root}
			}
		}
	}
	if len(paths) == 0 {
		flaggy.ShowHelp("")
		return ok, nil
	}

	opts := validate.Options{
		Compile:  !cast.ToBool(c.Vals["parse-only"]),
		Quiet:    cast.ToBool(c.Vals["quiet"]),
		NDJSON:   cast.ToBool(c.Vals["ndjson"]),
		JSON:     cast.ToBool(c.Vals["json"]),
		Detailed: cast.ToBool(c.Vals["detailed"]),
	}

	output := strings.ToLower(strings.TrimSpace(cast.ToString(c.Vals["output"])))
	switch output {
	case "", "text":
	case "json":
		opts.JSON = true
	default:
		return ok, g.Error("invalid --output %q; expected json", output)
	}

	results := validate.ParsePaths(paths, opts)
	setValidateTel(opts, results)
	text, err := validate.GetOutput(results, opts)
	if err != nil {
		return ok, g.Error(err, "could not render validate output")
	}
	if text != "" {
		fmt.Fprint(os.Stdout, text+"\n")
	}

	if validate.AnyFailed(results) {
		return ok, validateFailErr(results)
	}
	return ok, nil
}

func setValidateTel(opts validate.Options, results []validate.FileResult) {
	kinds := map[string]int{}
	fail := 0
	for _, r := range results {
		k := string(r.Kind)
		if k == "" {
			k = "unknown"
		}
		kinds[k]++
		if !r.OK {
			fail++
		}
	}
	env.SetTelVal("validate", g.Marshal(g.M(
		"compile", opts.Compile,
		"file_count", len(results),
		"fail_count", fail,
		"kinds", kinds,
	)))
}

func collectValidatePaths(c *g.CliSC) []string {
	paths := []string{}
	if v := strings.TrimSpace(cast.ToString(c.Vals["paths..."])); v != "" {
		paths = append(paths, v)
	}
	paths = append(paths, flaggy.TrailingArguments...)
	return paths
}

func validateFailErr(results []validate.FileResult) error {
	n := 0
	var first validate.FileResult
	for _, r := range results {
		if !r.OK {
			n++
			if first.Path == "" {
				first = r
			}
		}
	}
	if n == 1 {
		if first.Error != "" {
			return g.Error("%s: %s", first.Path, first.Error)
		}
		return g.Error("%s: parse failed", first.Path)
	}
	return g.Error("%d files failed to parse", n)
}
