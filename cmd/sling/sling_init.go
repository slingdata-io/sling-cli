package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling/project"
	"github.com/spf13/cast"
)

var initFlags = []g.Flag{
	{Name: "source", Type: "string", Description: "Source connection name"},
	{Name: "target", ShortName: "t", Type: "string", Description: "Target connection name"},
	{Name: "name", Type: "string", Description: "Project name (default: folder name)"},
	{Name: "yes", ShortName: "y", Type: "bool", Description: "Overwrite existing files without a prompt"},
	{Name: "force", Type: "bool", Description: "Allow init inside an existing project"},
	{Name: "test", Type: "bool", Description: "Test source and target connections"},
	{Name: "debug", ShortName: "d", Type: "bool", Description: "Set logging level to DEBUG."},
}

var cliInitProject = &g.CliSC{
	Name:                  "init",
	Description:           "Create a Sling project in the current folder",
	AdditionalHelpPrepend: "\nSee more details at https://docs.slingdata.io/",
	Flags:                 initFlags,
	ExecProcess:           processInit,
}

func init() {
	cliInitProject.Make().Add()
}

func processInit(c *g.CliSC) (ok bool, err error) {
	if cast.ToBool(c.Vals["debug"]) {
		os.Setenv("DEBUG", "LOW")
		env.InitLogger()
	}
	ok = true
	return ok, runInit(initOpts(c), cast.ToBool(c.Vals["test"]))
}

func initOpts(c *g.CliSC) project.Options {
	return project.Options{
		Source: cast.ToString(c.Vals["source"]),
		Target: cast.ToString(c.Vals["target"]),
		Name:   cast.ToString(c.Vals["name"]),
		Yes:    cast.ToBool(c.Vals["yes"]),
		Force:  cast.ToBool(c.Vals["force"]),
	}
}

func runInit(opts project.Options, testConns bool) error {
	if err := resolveInitConns(&opts, testConns); err != nil {
		return err
	}

	res, err := project.Init(opts)
	if err != nil {
		return err
	}

	for _, f := range res.Files {
		g.Info("wrote `%s`", f)
	}
	fmt.Println()
	fmt.Println("  Next: Add .sql models to schema folders and run 'sling build'")
	fmt.Println("  Run a job locally with 'sling run -j <key>'. Schedules fire on the platform once this folder is linked.")
	fmt.Println()

	if os.Getenv("SLING_PROJECT_TOKEN") != "" {
		fmt.Println("To see the platform jobs for this folder, run `sling platform jobs list`.")
	}
	return nil
}

// resolveInitConns fills in missing source/target via interactive prompts and
// optionally tests both connections.
func resolveInitConns(opts *project.Options, testConns bool) error {
	opts.Source = strings.TrimSpace(opts.Source)
	opts.Target = strings.TrimSpace(opts.Target)

	if opts.Source == "" || opts.Target == "" {
		if err := promptMissingConns(opts); err != nil {
			return err
		}
	}

	if testConns {
		entries := connection.GetLocalConns()
		for _, name := range []string{opts.Source, opts.Target} {
			if err := testNamedConn(entries, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// promptMissingConns lists local connections and asks for any of source/target
// not already provided via flags.
func promptMissingConns(opts *project.Options) error {
	entries := connection.GetLocalConns()
	if len(entries) == 0 {
		return g.Error("no connections found. Run `sling assist` or `sling conns set --type` to add a connection.")
	}
	if !isInteractive() {
		return g.Error("source and target are required; pass --source and --target with --yes")
	}

	fmt.Println("\n  Available connections:")
	for i, conn := range entries {
		fmt.Printf("    %d. %s (%s)\n", i+1, conn.Name, conn.Connection.Type.String())
	}
	fmt.Println()

	reader := bufio.NewReader(os.Stdin)
	var err error
	if opts.Source == "" {
		if opts.Source, err = askPrompt(reader, "  ? Source connection: "); err != nil {
			return err
		}
	}
	if opts.Target == "" {
		if opts.Target, err = askPrompt(reader, "  ? Target connection: "); err != nil {
			return err
		}
	}

	if opts.Source == "" || opts.Target == "" {
		return g.Error("source and target are required")
	}
	return nil
}

// testNamedConn tests one connection looked up by name in the given entries.
func testNamedConn(entries connection.ConnEntries, name string) error {
	conn := entries.Get(name)
	if conn.Name == "" {
		return g.Error("connection %s not found", name)
	}
	ok, err := conn.Connection.Test()
	conn.Connection.Close()
	if err != nil {
		return g.Error(err, "connection %s failed", name)
	}
	if !ok {
		return g.Error("connection %s failed", name)
	}
	return nil
}
