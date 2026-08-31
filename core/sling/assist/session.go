package assist

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/flarco/g"
	"github.com/google/uuid"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/env"
	"golang.org/x/term"
)

// assistOut is stdout for --out - / nested-launch / the open card. Tests swap it.
var assistOut io.Writer = os.Stdout

const (
	modeAsk  = "ask"
	modeOpen = "open"
)

// SessionOptions is the flags-only `sling assist` invocation.
type SessionOptions struct {
	Ask            string
	Name           string
	Agent          string
	Model          string
	Print          bool
	OutputFile     string
	Headless       bool
	ExecID         string // --id: investigate this failure
	ResumeID       string
	ResumeSet      bool // --resume present (empty id → picker already resolved)
	NonInteractive map[string]string
}

// NestedLaunch reports an already-running CLI agent (or non-TTY stdin).
// When true, print the prompt instead of spawning another agent.
func NestedLaunch() bool {
	if os.Getenv("CLAUDECODE") != "" {
		return true
	}
	if os.Getenv("CURSOR_TRACE_ID") != "" {
		return true
	}
	if os.Getenv("OPENCODE") != "" || os.Getenv("OPENCODE_SESSION") != "" {
		return true
	}
	return false
}

// Session probes local state, renders the five-section prompt, then prints or launches.
func Session(opts SessionOptions) (string, error) {
	if opts.ResumeID != "" {
		return resumeSession(opts)
	}

	if len(opts.NonInteractive) > 0 {
		if v := opts.NonInteractive["Intention"]; v != "" && opts.Ask == "" {
			opts.Ask = v
		}
		if v := opts.NonInteractive["intention"]; v != "" && opts.Ask == "" {
			opts.Ask = v
		}
	}
	opts.Ask = strings.TrimSpace(opts.Ask)

	if opts.Headless && opts.Ask == "" && opts.ExecID == "" {
		return "", g.Error(`no ask given; pass it as an argument: sling assist "<what you want>"`)
	}

	// Failure details enter the prompt only when the ask targets the
	// failure: the investigate pick, or the empty-ask fallback.
	ctx := Probe(ProbeOptions{
		Ask:            opts.Ask,
		ExecID:         opts.ExecID,
		IncludeFailure: opts.Ask == "",
	})
	if opts.ExecID != "" && ctx.TargetExec == nil {
		if _, err := ResolveLocalExec(opts.ExecID); err != nil {
			return "", g.Error(err, "see ~/.sling/assist/errors/")
		}
	}
	mode := modeAsk
	if shouldOpenScreen(opts) {
		ask, investigate, ok := runOpenScreen(ctx)
		if !ok {
			return "", nil
		}
		opts.Ask = ask
		ctx = Probe(ProbeOptions{Ask: opts.Ask, IncludeFailure: investigate})
		mode = modeOpen
	}

	prompt, err := RenderPrompt(ctx)
	if err != nil {
		return "", err
	}

	if opts.Print {
		fmt.Fprint(assistOut, prompt)
		return prompt, nil
	}
	if opts.OutputFile != "" {
		if err := os.WriteFile(opts.OutputFile, []byte(prompt), 0o644); err != nil {
			return "", g.Error(err, "write --out %s", opts.OutputFile)
		}
		return prompt, nil
	}
	if NestedLaunch() || (!ttyCheck(os.Stdin) && !opts.Headless) {
		fmt.Fprint(assistOut, prompt)
		return prompt, nil
	}

	if err := EnsureAssistReady(); err != nil {
		return "", err
	}
	prof, _, _ := LoadProfile()
	resolvedAgent, agentErr := ResolveAgent(opts.Agent, prof)
	if agentErr != nil {
		return "", agentErr
	}

	a := AnswersFile{
		Name:         opts.Name,
		Task:         mode,
		SlingVersion: core.Version,
		Created:      time.Now().UTC(),
		Agent:        resolvedAgent,
		Cwd:          mustGetwd(),
		Answers:      map[string]any{"intention": opts.Ask, "ask": opts.Ask},
	}
	if a.Name == "" {
		if s := slugify(opts.Ask); s != "" && s != "entry" {
			a.Name = s
		} else {
			a.Name = "assist"
		}
	}
	now := time.Now().UTC()
	harnessID := newHarnessSessionID(resolvedAgent)
	m := Meta{
		Task:             mode,
		Agent:            resolvedAgent,
		Model:            opts.Model,
		HarnessSessionID: harnessID,
		LaunchedAt:       &now,
	}
	id, err := SaveEntry(a, prompt, m)
	if err != nil {
		return prompt, err
	}
	if err := AutoTrim(); err != nil {
		g.Debug("assist: auto-trim: %s", err.Error())
	}
	if resolvedAgent == "" {
		return id, nil
	}
	promptPath := filepath.Join(HistoryDir(), id, "prompt.md")
	g.Info("submitting prompt to agent %s: %s",
		env.CyanString(resolvedAgent), env.DarkGrayString(collapseHome(promptPath)))
	snap := snapshotHarnessFiles(resolvedAgent)
	err = LaunchAgent(LaunchOptions{
		Agent:      resolvedAgent,
		Prompt:     prompt,
		PromptPath: promptPath,
		Model:      opts.Model,
		SessionID:  harnessID,
	})
	if harnessID == "" {
		if hid := discoverHarnessSessionID(resolvedAgent, snap); hid != "" {
			e, lerr := LoadEntry(id)
			if lerr == nil {
				e.Meta.HarnessSessionID = hid
				if serr := e.saveMeta(); serr != nil {
					g.Debug("assist: save harness session id: %s", serr.Error())
				}
			}
		}
	}
	if err != nil {
		var ae *AgentExitError
		if errors.As(err, &ae) {
			return id, ae
		}
		return id, g.Error(err, "agent launch failed")
	}
	return id, nil
}

func resumeSession(opts SessionOptions) (string, error) {
	e, err := LoadEntry(opts.ResumeID)
	if err != nil {
		return "", g.Error("unknown session %q; run `sling assist --resume` to pick one", opts.ResumeID)
	}

	promptPath := filepath.Join(e.Path, "prompt.md")
	promptBytes, _ := os.ReadFile(promptPath)
	prompt := string(promptBytes)

	if opts.Print {
		fmt.Fprint(assistOut, prompt)
		return prompt, nil
	}
	if opts.OutputFile != "" {
		if err := os.WriteFile(opts.OutputFile, promptBytes, 0o644); err != nil {
			return "", g.Error(err, "write --out %s", opts.OutputFile)
		}
		return prompt, nil
	}

	if err := EnsureAssistReady(); err != nil {
		return "", err
	}

	agent := e.Answers.Agent
	if e.Meta.Agent != "" {
		agent = e.Meta.Agent
	}
	if opts.Agent != "" && opts.Agent != agent {
		return "", g.Error("session %q was launched with agent %q; cannot resume with --agent %s", e.ID, agent, opts.Agent)
	}

	hid := e.Meta.HarnessSessionID
	if hid == "" {
		hid = discoverHarnessSessionID(agent, nil)
		if hid != "" {
			e.Meta.HarnessSessionID = hid
			if serr := e.saveMeta(); serr != nil {
				g.Debug("assist: save harness session id: %s", serr.Error())
			}
		}
	}
	if hid == "" {
		return "", g.Error("session %q has no harness session id; cannot resume", e.ID)
	}

	if err := LaunchResume(agent, hid, opts.Model); err != nil {
		var ae *AgentExitError
		if errors.As(err, &ae) {
			return e.ID, ae
		}
		return e.ID, g.Error(err, "agent resume failed")
	}
	return e.ID, nil
}

// assistIn is stdin for the open screen. Tests swap it.
var assistIn io.Reader = os.Stdin

// ttyCheck is the TTY probe. Tests swap it.
var ttyCheck = isTTY

func shouldOpenScreen(opts SessionOptions) bool {
	if strings.TrimSpace(opts.Ask) != "" || opts.ExecID != "" {
		return false
	}
	if opts.Print || opts.OutputFile != "" || opts.Headless || NestedLaunch() {
		return false
	}
	return ttyCheck(os.Stdin)
}

func runOpenScreen(p PromptContext) (ask string, investigate, ok bool) {
	width := 80
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		width = w
	}
	opts := p.suggestions()
	if assistIn == os.Stdin {
		// Real terminal: the huh form draws its own list, no banner.
		return pickOpenAsk(opts)
	}
	fmt.Fprint(assistOut, renderOpenCard(p, width))
	return readOpenAsk(assistIn, assistOut, opts)
}

// pickOpenAsk is the huh picker: a select over the suggestion rows, then a
// text area when the user picks the describe-it row.
func pickOpenAsk(opts []suggestion) (ask string, investigate, ok bool) {
	if len(opts) == 0 {
		return "", false, false
	}
	sel := make([]huh.Option[int], 0, len(opts))
	for i, s := range opts {
		sel = append(sel, huh.NewOption(s.Label, i))
	}
	pick := 0
	free := ""
	isFree := func() bool { return pick >= 0 && pick < len(opts) && strings.TrimSpace(opts[pick].Ask) == "" }

	form := huh.NewForm(
		huh.NewGroup(
			// No Height: huh sizes the viewport to all options, so a
			// wrapped row never scrolls the list.
			huh.NewSelect[int]().
				Title("What would you like to do?").
				Options(sel...).
				Value(&pick),
		),
		huh.NewGroup(
			huh.NewText().
				Title("Describe what you want").
				Placeholder("e.g. backfill orders from postgres into snowflake").
				Lines(5).
				CharLimit(2000).
				Value(&free),
		).WithHideFunc(func() bool { return !isFree() }),
	).WithTheme(huh.ThemeCharm())

	if err := form.Run(); err != nil {
		if !errors.Is(err, huh.ErrUserAborted) {
			g.Debug("assist: open picker: %s", err.Error())
		}
		fmt.Fprintln(assistOut, `No ask given. Run: sling assist "<what you want>"`)
		return "", false, false
	}

	if pick < 0 || pick >= len(opts) {
		return "", false, false
	}
	s := opts[pick]
	if strings.TrimSpace(s.Ask) != "" {
		return s.Ask, s.Investigate, true
	}
	free = strings.TrimSpace(free)
	if free == "" {
		fmt.Fprintln(assistOut, `No ask given. Run: sling assist "<what you want>"`)
		return "", false, false
	}
	return free, false, true
}

func renderOpenCard(p PromptContext, width int) string {
	if width <= 0 {
		width = 80
	}
	var b strings.Builder
	b.WriteString(openSummary(p))
	b.WriteByte('\n')
	for i, s := range p.suggestions() {
		fmt.Fprintf(&b, "  %d. %s\n", i+1, s.Label)
	}
	b.WriteByte('\n')
	return wrapToWidth(strings.TrimRight(b.String(), "\n")+"\n", width)
}

func openSummary(p PromptContext) string {
	n := userConnectionCount(p.Connections)
	noun := "connections"
	if n == 1 {
		noun = "connection"
	}
	var line1 string
	switch {
	case p.HasProject && p.ProjectName != "":
		line1 = fmt.Sprintf("On project %s · %d %s", p.ProjectName, n, noun)
	case p.HasProject:
		line1 = fmt.Sprintf("On this project · %d %s", n, noun)
	default:
		line1 = fmt.Sprintf("No project in this folder · %d %s", n, noun)
	}
	if run, ok := latestRun(p.RecentRuns); ok {
		label := run.ID
		if obj := run.displayObject(); obj != "" {
			label += " " + obj
		}
		s := fmt.Sprintf("%s [%s]", label, run.Status)
		if !run.When.IsZero() {
			rt := relTime(run.When)
			if rt == "just now" {
				s += "  just now"
			} else {
				s += "  " + rt + " ago"
			}
		}
		return line1 + "\n  last run: " + s + "\n"
	}
	return line1 + "\n"
}

func readOpenAsk(r io.Reader, w io.Writer, opts []suggestion) (ask string, investigate, ok bool) {
	br := bufio.NewReader(r)
	empty := 0
	fmt.Fprint(w, "> ")
	for {
		line, err := br.ReadString('\n')
		if err != nil && strings.TrimSpace(line) == "" {
			fmt.Fprintln(w, `No ask given. Run: sling assist "<what you want>"`)
			return "", false, false
		}
		line = strings.TrimSpace(line)
		if line == "" {
			empty++
			if empty >= 2 {
				fmt.Fprintln(w, `No ask given. Run: sling assist "<what you want>"`)
				return "", false, false
			}
			fmt.Fprintln(w, `Type a number or describe what you want.`)
			fmt.Fprint(w, "> ")
			continue
		}
		empty = 0
		if n, convErr := strconv.Atoi(line); convErr == nil && n >= 1 && n <= len(opts) {
			s := opts[n-1]
			if strings.TrimSpace(s.Ask) != "" {
				return s.Ask, s.Investigate, true
			}
			fmt.Fprintln(w, "Describe it:")
			fmt.Fprint(w, "> ")
			continue
		}
		return line, false, true
	}
}

// isTTY reports whether the file descriptor is connected to a terminal.
func isTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

// ResolveAgent picks the agent to launch, in order:
// 1. --agent override (must be a known CLI agent and detected on disk).
// 2. profile.Agent (when not "auto").
// 3. profile.Agent == "auto" → first detected CLI agent on $PATH.
func ResolveAgent(override string, prof Profile) (string, error) {
	if override != "" {
		c := LookupClient(override)
		if c == nil {
			return "", g.Error("unknown agent %q", override)
		}
		if c.Kind() != KindCLIAgent {
			return "", g.Error("agent %q is not a launchable CLI agent (it's an install target)", override)
		}
		if !c.Detect() {
			return "", g.Error("agent %q not detected; run `sling assist setup` to set it up", override)
		}
		return override, nil
	}
	if prof.Agent != "" && prof.Agent != "auto" {
		c := LookupClient(prof.Agent)
		if c == nil {
			return "", g.Error("profile names unknown agent %q; run `sling assist setup`", prof.Agent)
		}
		if c.Kind() != KindCLIAgent {
			return "", g.Error("profile names non-launchable agent %q; run `sling assist setup` or use --agent", prof.Agent)
		}
		return prof.Agent, nil
	}
	for _, c := range CLIAgents() {
		if c.Detect() && commandOnPath(agentBinary(c.Name())) {
			return c.Name(), nil
		}
	}
	return "", g.Error("no AI agent on $PATH; run `sling assist setup` or pass --agent")
}

type agentLaunchPlan struct {
	Args     []string
	UseStdin bool
}

func agentBinary(agent string) string {
	if agent == "cursor" {
		return "cursor-agent"
	}
	return agent
}

func assignsHarnessSessionID(agent string) bool {
	return agent == "claude" || agent == "grok"
}

func newHarnessSessionID(agent string) string {
	if !assignsHarnessSessionID(agent) {
		return ""
	}
	return uuid.NewString()
}

func agentLaunchArgs(agent, promptPath, model, sessionID string) agentLaunchPlan {
	var p agentLaunchPlan
	switch agent {
	case "codex":
		p = agentLaunchPlan{Args: []string{"exec", "-"}, UseStdin: true}
	case "gemini":
		p = agentLaunchPlan{Args: []string{"-p", "-"}, UseStdin: true}
	case "grok":
		// -p/--prompt-file are single-turn. Seed the interactive session
		// with a positional prompt that @-references the file.
		p = agentLaunchPlan{
			Args:     []string{fmt.Sprintf("Read and execute the task in @%s", promptPath)},
			UseStdin: false,
		}
	case "pi":
		p = agentLaunchPlan{Args: []string{"-p"}, UseStdin: true}
	case "opencode":
		p = agentLaunchPlan{
			Args:     []string{"run", "--file", promptPath, "Read and execute the attached task"},
			UseStdin: false,
		}
	case "claude":
		p = agentLaunchPlan{
			Args:     []string{fmt.Sprintf("Read and execute the task in @%s", promptPath)},
			UseStdin: false,
		}
	case "cursor":
		p = agentLaunchPlan{Args: []string{promptPath}, UseStdin: false}
	default:
		p = agentLaunchPlan{UseStdin: true}
	}
	p.Args = withModelAndSession(agent, p.Args, model, sessionID)
	return p
}

func agentResumeArgs(agent, harnessID, model string) agentLaunchPlan {
	var p agentLaunchPlan
	switch agent {
	case "claude":
		p = agentLaunchPlan{Args: []string{"--resume", harnessID}}
	case "grok":
		p = agentLaunchPlan{Args: []string{"--resume", harnessID}}
	case "codex":
		p = agentLaunchPlan{Args: []string{"resume", harnessID}}
	case "gemini":
		p = agentLaunchPlan{Args: []string{"--resume", harnessID}}
	case "cursor":
		p = agentLaunchPlan{Args: []string{"--resume=" + harnessID}}
	case "opencode":
		p = agentLaunchPlan{Args: []string{"--session", harnessID}}
	case "pi":
		p = agentLaunchPlan{Args: []string{"--session", harnessID}}
	default:
		p = agentLaunchPlan{Args: []string{"--resume", harnessID}}
	}
	p.Args = withModelAndSession(agent, p.Args, model, "")
	return p
}

func withModelAndSession(agent string, args []string, model, sessionID string) []string {
	flags := []string{}
	switch agent {
	case "claude":
		if sessionID != "" {
			flags = append(flags, "--session-id", sessionID)
		}
		if model != "" {
			flags = append(flags, "--model", model)
		}
		return append(flags, args...)
	case "grok":
		if sessionID != "" {
			flags = append(flags, "--session-id", sessionID)
		}
		if model != "" {
			flags = append(flags, "--model", model)
		}
		return append(flags, args...)
	case "codex":
		if model == "" {
			return args
		}
		if len(args) == 0 {
			return []string{"-m", model}
		}
		out := make([]string, 0, len(args)+2)
		out = append(out, args[0], "-m", model)
		out = append(out, args[1:]...)
		return out
	default:
		if model != "" {
			flags = append(flags, "--model", model)
		}
		return append(flags, args...)
	}
}

// LaunchOptions is one agent exec (first run or resume).
type LaunchOptions struct {
	Agent      string
	Prompt     string
	PromptPath string
	Model      string
	SessionID  string // pre-assigned harness id (claude, grok)
}

// LaunchAgent execs the given CLI agent with the prompt.
func LaunchAgent(opts LaunchOptions) error {
	plan := agentLaunchArgs(opts.Agent, opts.PromptPath, opts.Model, opts.SessionID)
	return startAgent(opts.Agent, plan, opts.Prompt)
}

// LaunchResume execs the harness resume command. No prompt is sent.
func LaunchResume(agent, harnessSessionID, model string) error {
	if strings.TrimSpace(harnessSessionID) == "" {
		return g.Error("missing harness session id")
	}
	plan := agentResumeArgs(agent, harnessSessionID, model)
	return startAgent(agent, plan, "")
}

func startAgent(agent string, plan agentLaunchPlan, prompt string) error {
	binary, err := lookPath(agent)
	if err != nil {
		return err
	}

	args := plan.Args
	useStdin := plan.UseStdin

	procAttr := &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Env:   os.Environ(),
	}
	if useStdin {
		pr, pw, err := os.Pipe()
		if err != nil {
			return err
		}
		go func() {
			defer pw.Close()
			_, _ = pw.Write([]byte(prompt))
		}()
		procAttr.Files = []*os.File{pr, os.Stdout, os.Stderr}
		argv := append([]string{binary}, args...)
		proc, err := os.StartProcess(binary, argv, procAttr)
		if err != nil {
			return err
		}
		state, err := proc.Wait()
		pr.Close()
		if err != nil {
			return err
		}
		return finishAgentExit(state, agent)
	}

	argv := append([]string{binary}, args...)
	proc, err := os.StartProcess(binary, argv, procAttr)
	if err != nil {
		return err
	}
	state, err := proc.Wait()
	if err != nil {
		return err
	}
	return finishAgentExit(state, agent)
}

// AgentExitError is returned when a launched CLI agent exits non-zero.
type AgentExitError struct {
	ExitCode int
	Agent    string
}

func (e *AgentExitError) Error() string {
	if e == nil {
		return "agent exited with error"
	}
	if e.Agent != "" {
		return fmt.Sprintf("agent %q exited with code %d", e.Agent, e.ExitCode)
	}
	return fmt.Sprintf("agent exited with code %d", e.ExitCode)
}

// ExitCodeOf returns (code, true) when err is or wraps an AgentExitError.
func ExitCodeOf(err error) (int, bool) {
	var ae *AgentExitError
	if errors.As(err, &ae) && ae != nil {
		return ae.ExitCode, true
	}
	return 0, false
}

func finishAgentExit(state *os.ProcessState, agent string) error {
	if state.Success() {
		return nil
	}
	code := 1
	if ws, ok := state.Sys().(syscall.WaitStatus); ok {
		code = ws.ExitStatus()
	}
	return &AgentExitError{ExitCode: code, Agent: agent}
}

func lookPath(name string) (string, error) {
	bin := agentBinary(name)
	if bin == "opencode" {
		p, err := EnsureBinOpenCode()
		if err != nil {
			return "", g.Error(err, "agent %q not found on $PATH", name)
		}
		return p, nil
	}
	p, err := exec.LookPath(bin)
	if err != nil {
		return "", g.Error(err, "agent %q not found on $PATH", name)
	}
	return p, nil
}

func harnessSessionRoot(agent string) string {
	home := userHome()
	switch agent {
	case "claude":
		return filepath.Join(home, ".claude", "projects")
	case "codex":
		return filepath.Join(home, ".codex", "sessions")
	case "gemini":
		return filepath.Join(home, ".gemini", "tmp")
	case "cursor":
		return filepath.Join(home, ".cursor")
	case "opencode":
		dataHome := os.Getenv("XDG_DATA_HOME")
		if dataHome == "" {
			dataHome = filepath.Join(home, ".local", "share")
		}
		return filepath.Join(dataHome, "opencode")
	case "pi":
		return filepath.Join(home, ".pi", "agent", "sessions")
	case "grok":
		return filepath.Join(home, ".grok", "sessions")
	default:
		return ""
	}
}

func snapshotHarnessFiles(agent string) map[string]time.Time {
	root := harnessSessionRoot(agent)
	out := map[string]time.Time{}
	if root == "" {
		return out
	}
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			rel = p
		}
		out[rel] = info.ModTime()
		return nil
	})
	return out
}

func discoverHarnessSessionID(agent string, before map[string]time.Time) string {
	root := harnessSessionRoot(agent)
	if root == "" {
		return ""
	}
	if before == nil {
		before = map[string]time.Time{}
	}
	var bestPath string
	var bestTime time.Time
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			rel = p
		}
		prev, known := before[rel]
		if known && !info.ModTime().After(prev) {
			return nil
		}
		id := idFromSessionPath(p)
		if id == "" {
			return nil
		}
		if bestPath == "" || info.ModTime().After(bestTime) {
			bestPath = p
			bestTime = info.ModTime()
		}
		return nil
	})
	if bestPath == "" {
		return ""
	}
	return idFromSessionPath(bestPath)
}

func idFromSessionPath(p string) string {
	base := filepath.Base(p)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if base == "" || strings.HasPrefix(base, ".") {
		return ""
	}
	switch strings.ToLower(base) {
	case "meta", "index", "config", "settings":
		return ""
	}
	return base
}
