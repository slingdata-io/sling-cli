package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling/assist"
	"github.com/spf13/cast"
)

var cliAssistFlags = []g.Flag{
	{Name: "id", Type: "string", Description: "investigate a failure by its id (an id prefix works)"},
	{Name: "out", ShortName: "o", Type: "string", Description: "emit prompt instead of launching: FILE, or - for stdout"},
	{Name: "agent", Type: "string", Description: "override profile.agent for this run only"},
	{Name: "model", Type: "string", Description: "pass --model to the harness for this run only"},
	{Name: "name", Type: "string", Description: "history slug override"},
	{Name: "non-interactive", Type: "bool", Description: "skip interactive prompts (use defaults / launch headless)"},
	{Name: "resume", Type: "string", Description: "resume a past session (omit id to pick)"},
}

var cliAssistSetupFlags = []g.Flag{
	{Name: "doctor", Type: "bool", Description: "print the health/install report and exit"},
	{Name: "agent", Type: "string", Description: "preferred agent for this setup"},
	{Name: "non-interactive", Type: "bool", Description: "skip interactive prompts (use defaults)"},
	{Name: "install", Type: "string", Description: "comma-separated components (e.g. mcp,skills)"},
	{Name: "uninstall", Type: "bool", Description: "remove all Sling skills + MCP wiring"},
	{Name: "reconfigure", Type: "bool", Description: "re-prompt the profile form before installing"},
	{Name: "scope", Type: "string", Description: "user (default) or project"},
	{Name: "clients", Type: "string", Description: "with --uninstall: comma-separated client names"},
}

// cliAssist is the `sling assist` top-level command.
var cliAssist = &g.CliSC{
	Name:        "assist",
	Description: "Get AI help to build, run and debug replications, pipelines, API Specs, etc.",
	AdditionalHelpPrepend: "\n" +
		"  sling assist                     # first run: setup; after: greet with choices\n" +
		"  sling assist \"backfill orders\"  # positional ask\n" +
		"  sling assist --id <id>           # investigate a failure\n  sling assist --resume            # pick a past session\n" +
		"  sling assist --resume <id>       # resume that session\n" +
		"  sling assist setup               # re-run setup / change harness\n" +
		"  sling assist setup --doctor      # report only\n" +
		"  sling assist error <sig>         # look up an error signature\n" +
		"  sling assist report --id <id>    # review a redacted failure report\n" +
		"  sling assist report --id <id> --github  # open a prefilled GitHub issue\n" +
		"  sling assist report --id <id> --email   # send to support (confirm first)\n" +
		"  sling assist --out - | --out F   # emit prompt, no launch\n" +
		"  sling assist --agent claude      # one-run override\n" +
		"  sling assist --model sonnet      # pass --model to the harness\n" +
		"See https://docs.slingdata.io/sling-cli/assist",
	ExecuteWithoutFlags: true,
	Flags:               cliAssistFlags,
	SubComs: []*g.CliSC{
		{
			Name:                "setup",
			Description:         "install skills + MCP, or reconfigure / uninstall",
			ExecuteWithoutFlags: true,
			Flags:               cliAssistSetupFlags,
		},
		{
			Name:        "error",
			Description: "look up guidance for an error signature",
			Flags: []g.Flag{
				{Name: "json", Type: "bool", Description: "emit machine-readable JSON"},
			},
			PosFlags: []g.Flag{
				{Name: "signature", Type: "string", Description: "8-char error signature from a failed run"},
			},
		},
		{
			Name:        "report",
			Description: "compose a redacted issue report from a failed run",
			Flags: []g.Flag{
				{Name: "id", Type: "string", Description: "failure id (a unique prefix works)"},
				{Name: "title", Type: "string", Description: "override the report title"},
				{Name: "description", Type: "string", Description: "custom context shown above the error in the report"},
				{Name: "github", Type: "bool", Description: "open a prefilled GitHub issue after confirm"},
				{Name: "email", Type: "bool", Description: "send to support after confirm"},
				{Name: "submit", Type: "bool", Description: "skip the confirm prompt (for agents)"},
			},
		},
	},
	ExecProcess: processAssist,
}

func init() {
	cliAssist.Make().Add()
}

// processAssist is the dispatcher. Auto-refresh runs once at the top so every
// subcommand below sees a fresh canonical bundle. If skills are already
// installed, AutoRefresh updates and prunes them. If none are installed,
// the user must run `sling assist setup`.
func processAssist(c *g.CliSC) (ok bool, err error) {
	ok = true
	if notice, refreshErr := assist.AutoRefresh(context.Background()); refreshErr == nil && notice != "" {
		fmt.Fprintln(os.Stderr, notice)
	}

	switch c.UsedSC() {
	case "setup":
		return ok, runAssistSetup(c)
	case "error":
		return ok, runAssistError(c)
	case "report":
		return ok, runAssistReport(c)
	}
	return ok, runAssistFlags(c)
}

// runAssistFlags is the flags-only path: --resume, else first-run
// setup or probe+launch / --out.
func runAssistFlags(c *g.CliSC) error {
	vals := flatVals(c)

	resumeSet, resumeID := resumeFromArgs(os.Args)
	if resumeSet {
		return runAssistResume(c, resumeID)
	}

	_, profileExists, err := assist.LoadProfile()
	if err != nil {
		return err
	}
	if !profileExists && strings.TrimSpace(cast.ToString(vals["out"])) == "" {
		return runAssistSetup(c)
	}
	return runAssistSession(c)
}

func runAssistSession(c *g.CliSC) error {
	vals := flatVals(c)
	// Trailing args are the ask (flaggy cannot share PosFlags with SubComs).
	ask := ""
	if len(flaggy.TrailingArguments) > 0 {
		ask = strings.TrimSpace(strings.Join(flaggy.TrailingArguments, " "))
	}
	opts := assist.SessionOptions{
		Ask:      ask,
		ExecID:   strings.TrimSpace(cast.ToString(vals["id"])),
		Name:     cast.ToString(vals["name"]),
		Agent:    cast.ToString(vals["agent"]),
		Model:    cast.ToString(vals["model"]),
		Headless: cast.ToBool(vals["non-interactive"]),
	}
	applyAssistOut(&opts, cast.ToString(vals["out"]))
	_, err := assist.Session(opts)
	if code, ok := assist.ExitCodeOf(err); ok {
		os.Exit(code)
	}
	return err
}

// applyAssistOut maps --out onto the session emit options.
// "-" means stdout; any other value is a file path.
func applyAssistOut(opts *assist.SessionOptions, out string) {
	out = strings.TrimSpace(out)
	switch out {
	case "":
	case "-":
		opts.Print = true
	default:
		opts.OutputFile = out
	}
}

func runAssistResume(c *g.CliSC, id string) error {
	vals := flatVals(c)
	if id == "" {
		e, err := assist.PickHistoryEntry()
		if err != nil {
			if errors.Is(err, assist.ErrUserAborted) {
				return nil
			}
			return err
		}
		id = e.ID
	}
	opts := assist.SessionOptions{
		ResumeID:  id,
		ResumeSet: true,
		Agent:     cast.ToString(vals["agent"]),
		Model:     cast.ToString(vals["model"]),
		Headless:  cast.ToBool(vals["non-interactive"]),
	}
	applyAssistOut(&opts, cast.ToString(vals["out"]))
	_, err := assist.Session(opts)
	if code, ok := assist.ExitCodeOf(err); ok {
		os.Exit(code)
	}
	return err
}

// padAssistResumeFlag lets flaggy accept a bare `--resume` (picker) as `--resume=`.
func padAssistResumeFlag(args []string) []string {
	assist := false
	for _, a := range args[1:] {
		if a == "assist" {
			assist = true
			break
		}
		if strings.HasPrefix(a, "-") {
			continue
		}
		break
	}
	if !assist {
		return args
	}
	out := make([]string, 0, len(args)+1)
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--resume" {
			if i+1 >= len(args) || strings.HasPrefix(args[i+1], "-") {
				out = append(out, "--resume=")
				continue
			}
		}
		out = append(out, a)
	}
	return out
}

func resumeFromArgs(args []string) (present bool, id string) {
	for i := 1; i < len(args); i++ {
		a := args[i]
		if a == "--resume" {
			if i+1 < len(args) && args[i+1] != "" && !strings.HasPrefix(args[i+1], "-") {
				return true, args[i+1]
			}
			return true, ""
		}
		if strings.HasPrefix(a, "--resume=") {
			return true, strings.TrimPrefix(a, "--resume=")
		}
	}
	return false, ""
}

func parseScope(v string) assist.Scope {
	if strings.EqualFold(v, "project") {
		return assist.ScopeProject
	}
	return assist.ScopeUser
}

// runAssistSetup is the unified setup entry point. One command does everything
// install/doctor/uninstall used to do — doctor runs implicitly each call to
// detect current state, then we branch by flag intent or interactive choice.
func runAssistSetup(c *g.CliSC) error {
	vals := flatVals(c)

	// --uninstall is the only path that skips doctor (we're tearing it all
	// down anyway; running doctor first would just be noise).
	if cast.ToBool(vals["uninstall"]) {
		return runSetupUninstallAll(vals)
	}

	// Doctor always runs first; we use its result both to render state for
	// the user and to decide what the implicit "next action" should be.
	// Scope must match install scope so project installs aren't reported broken.
	report, err := assist.Doctor(context.Background(), assist.DoctorOptions{
		Scope: parseScope(cast.ToString(vals["scope"])),
	})
	if err != nil {
		return err
	}
	_, profileExists, _ := assist.LoadProfile()

	// --doctor: print and exit.
	if cast.ToBool(vals["doctor"]) {
		fmt.Fprint(os.Stdout, report.Render())
		if !report.OK {
			return g.Error("doctor reported failures")
		}
		return nil
	}

	// --install <components>: skip the picker, install whatever was named.
	if comps := cast.ToString(vals["install"]); comps != "" {
		// Split "mcp,skills" into lower-case names.
		var parts []string
		for _, p := range strings.Split(comps, ",") {
			p = strings.TrimSpace(strings.ToLower(p))
			if p != "" {
				parts = append(parts, p)
			}
		}
		return runSetupInstall(vals, parts, profileExists, report)
	}

	// Non-interactive without an explicit verb → behave like first-run install
	// of all components (back-compat with how the old `install --non-interactive`
	// behaved).
	if cast.ToBool(vals["non-interactive"]) {
		return runSetupInstall(vals, allComponents(), profileExists, report)
	}

	// Subsequent run (profile exists): show doctor output, then drop into the
	// interactive "what do you want to do?" form.
	if profileExists {
		fmt.Fprint(os.Stdout, report.Render())
		fmt.Fprintln(os.Stdout, "")
		action, err := assist.RunSetupActionForm(report)
		if err != nil {
			return err
		}
		switch action {
		case assist.SetupActionRefresh:
			return runSetupInstall(vals, allComponents(), profileExists, report)
		case assist.SetupActionInstallMissing:
			comps := report.MissingComponents()
			if len(comps) == 0 {
				comps = allComponents()
			}
			return runSetupInstall(vals, comps, profileExists, report)
		case assist.SetupActionUninstall:
			return runSetupUninstallAll(vals)
		case assist.SetupActionReconfigure:
			vals["reconfigure"] = true
			return runSetupInstall(vals, allComponents(), profileExists, report)
		case assist.SetupActionExit:
			return nil
		}
		return nil
	}

	// First run (no profile): harness confirm (agents + bundled opencode fallback).
	if len(assist.DetectedClients()) == 0 && len(assist.RankedCLIAgents()) == 0 {
		return g.Error("no AI agent on $PATH; install one of: claude, codex, gemini, cursor, opencode, pi, grok")
	}
	prefill := assist.DefaultProfile()
	prefill.Agent = assist.RecommendedAgent()
	result, err := assist.RunHarnessConfirmForm(prefill)
	if err != nil {
		if errors.Is(err, assist.ErrUserAborted) {
			return nil
		}
		return err
	}
	prefill.Agent = result.Agent
	prefill.HintInErrors = result.HintInErrors
	prefill.DefaultInstallScope = result.Scope
	if err := assist.SaveProfile(prefill); err != nil {
		return err
	}
	vals["agent"] = result.Agent
	vals["scope"] = result.Scope
	return runSetupInstall(vals, result.Components, true, report)
}

// runSetupInstall runs the profile form (if needed) then installs the
// requested components. components is the canonical set; we translate it
// to SkillsOnly/MCPOnly for the existing Install API.
func runSetupInstall(vals map[string]any, components []string, _ bool, _ *assist.DoctorReport) error {
	opts := assist.InstallOptions{
		Reconfigure:    cast.ToBool(vals["reconfigure"]),
		Scope:          parseScope(cast.ToString(vals["scope"])),
		NonInteractive: cast.ToBool(vals["non-interactive"]),
		DefaultAgent:   cast.ToString(vals["agent"]),
	}
	hasSkills := containsString(components, "skills")
	hasMCP := containsString(components, "mcp")
	if !hasSkills && !hasMCP {
		return g.Error("no components selected; pass --install mcp,skills or pick at least one in the form")
	}
	opts.SkillsOnly = hasSkills && !hasMCP
	opts.MCPOnly = hasMCP && !hasSkills

	// Interactive first-run (or --reconfigure): show the profile form before
	// falling through to Install().
	if !opts.NonInteractive {
		prof, exists, _ := assist.LoadProfile()
		needForm := !exists || opts.Reconfigure
		if needForm {
			if len(assist.DetectedClients()) == 0 && len(assist.RankedCLIAgents()) == 0 {
				return g.Error("no AI agent on $PATH; install one of: claude, codex, gemini, cursor, opencode, pi, grok")
			}
			prefill := prof
			if !exists {
				prefill = assist.DefaultProfile()
				prefill.Agent = assist.RecommendedAgent()
			}
			result, err := assist.RunInstallForm(prefill)
			if err != nil {
				if errors.Is(err, assist.ErrUserAborted) {
					return nil
				}
				return err
			}
			prefill.Agent = result.Agent
			prefill.HintInErrors = result.HintInErrors
			prefill.DefaultInstallScope = result.Scope
			if err := assist.SaveProfile(prefill); err != nil {
				return err
			}
			opts.Reconfigure = false
			opts.DefaultAgent = ""
			opts.Scope = parseScope(result.Scope)
		}
	}

	res, err := assist.Install(context.Background(), opts)
	if err != nil {
		return err
	}
	// Install summary: profile, canonical skills, then each wired client.
	if res.ProfileWritten {
		fmt.Fprintf(os.Stdout, "%s Wrote AI profile to %s\n",
			env.GreenString("✓"), env.CyanString(env.HomeDirEnvFile))
	}
	fmt.Fprintf(os.Stdout, "%s Wrote canonical skills to %s\n",
		env.GreenString("✓"), env.CyanString(res.CanonicalSkillsDir))
	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintln(os.Stdout, env.BlueString("Wired clients:"))
	yesNo := func(v bool) string {
		if v {
			return env.GreenString("yes")
		}
		return env.DarkGrayString("no")
	}
	for _, row := range res.WiredClients {
		mark := env.GreenString("✓")
		if !row.WroteSkills && !row.WroteMCP {
			mark = env.YellowString("⊘")
		}
		auth := row.Authed.YesNo()
		authOut := env.DarkGrayString(auth)
		if row.Authed == assist.AuthOK {
			authOut = env.GreenString(auth)
		}
		fmt.Fprintf(os.Stdout, "  %s %-8s skills=%s mcp=%s authed=%s %s\n",
			mark, row.Name, yesNo(row.WroteSkills), yesNo(row.WroteMCP), authOut,
			env.DarkGrayString(row.Notes))
	}
	fmt.Fprintln(os.Stdout, "")
	fmt.Fprintf(os.Stdout, "Run %s to verify.\n", env.CyanString("`sling assist setup --doctor`"))
	return nil
}

// runSetupUninstallAll wipes everything (skills + mcp from every detected
// client + the canonical bundle). No interactive form — `--uninstall` is the
// blunt instrument; per-client/per-component selection isn't worth a separate
// surface.
func runSetupUninstallAll(vals map[string]any) error {
	opts := assist.UninstallOptions{
		All:   true,
		Scope: parseScope(cast.ToString(vals["scope"])),
	}
	if v := cast.ToString(vals["clients"]); v != "" {
		opts.IncludeClients = strings.Split(v, ",")
	}
	if err := assist.Uninstall(context.Background(), opts); err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, env.GreenString("✓ Removed Sling skills + MCP wiring from detected clients."))
	return nil
}

func allComponents() []string { return []string{"skills", "mcp"} }

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func runAssistReport(c *g.CliSC) error {
	vals := flatVals(c)
	id := strings.TrimSpace(cast.ToString(vals["id"]))
	if id == "" {
		return g.Error("usage: sling assist report --id <exec_id>")
	}
	return assist.RunReport(assist.ReportCmd{
		ExecID:      id,
		Title:       strings.TrimSpace(cast.ToString(vals["title"])),
		Description: strings.TrimSpace(cast.ToString(vals["description"])),
		GitHub:      cast.ToBool(vals["github"]),
		Email:       cast.ToBool(vals["email"]),
		Submit:      cast.ToBool(vals["submit"]),
	})
}

func runAssistError(c *g.CliSC) error {
	vals := flatVals(c)
	sig := strings.TrimSpace(cast.ToString(vals["signature"]))
	if sig == "" {
		return g.Error("usage: sling assist error <signature>")
	}
	result, err := assist.LookupError(sig)
	if err != nil {
		return err
	}
	if cast.ToBool(vals["json"]) {
		fmt.Println(g.Marshal(result))
		return nil
	}
	fmt.Printf("error_signature: %s\n", result.Signature)
	fmt.Printf("status:          %s\n", result.Status)
	if result.Title != "" {
		fmt.Printf("title:           %s\n", result.Title)
	}
	if result.Guidance != "" {
		fmt.Println()
		fmt.Println(result.Guidance)
	}
	if result.DocsURL != "" {
		fmt.Printf("\nDocs: %s\n", result.DocsURL)
	}
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("  sling assist               # open assist (offers to investigate failures)\n")
	if result.Status == "unknown" || result.Status == "pending" {
		fmt.Printf("  sling assist report --id <exec_id>   # share a redacted report\n")
	}
	return nil
}

// flatVals returns the val map from the active subcommand. CliSC stores per-
// subcommand flag values in c.Vals; we just pass that through.
func flatVals(c *g.CliSC) map[string]any {
	out := map[string]any{}
	for k, v := range c.Vals {
		out[k] = v
	}
	return out
}
