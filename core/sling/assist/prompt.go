package assist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"
	"unicode/utf8"

	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling/project"
	"github.com/slingdata-io/sling-cli/core/sling/validate"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// maxPromptTokens is a soft cap for the assembled --out prompt (~4 chars/token).
const maxPromptTokens = 1500

// maxErrorExcerptLines caps the sanitized failure excerpt in Context.
const maxErrorExcerptLines = 15

const maxProbeConnections = 10

// canonicalFolders are the wave-5 project dirs scanned for file counts.
var canonicalFolders = []string{"replications", "pipelines", "models", "specs"}

// ProbeConn is name/type/source from ConnEntries.List() (no payload fields).
type ProbeConn struct {
	Name   string
	Type   string
	Source string
}

// PromptContext is the assembled state for the five-section agent prompt.
type PromptContext struct {
	Version         string
	Cwd             string
	ProjectName     string
	ProjectRoot     string
	HasProject      bool
	FileCounts      map[string]int
	Connections     []ProbeConn
	ConnectionExtra int
	RecentRuns      []LocalExec
	TargetExec      *LocalExec // set by --id: investigate this failure
	MCPWired        bool
	Signature       string
	Lookup          *ErrorLookupResult
	ErrorExcerpt    string
	RunLogExcerpt   string
	Ask             string
	Suggestions     []string
	Route           string
	Agent           string
}

type promptView struct {
	Rules     string
	State     string
	Context   string
	Ask       string
	Objective string
}

type promptBundle struct {
	Skeleton  string `yaml:"_skeleton"`
	Rules     string `yaml:"_rules"`
	Objective string `yaml:"_objective"`
}

func loadPromptBundle() (promptBundle, error) {
	var b promptBundle
	if err := yaml.Unmarshal(PromptsYAML, &b); err != nil {
		return b, err
	}
	if strings.TrimSpace(b.Skeleton) == "" {
		return b, fmt.Errorf("prompts.yaml missing _skeleton")
	}
	if strings.TrimSpace(b.Rules) == "" {
		return b, fmt.Errorf("prompts.yaml missing _rules")
	}
	if strings.TrimSpace(b.Objective) == "" {
		return b, fmt.Errorf("prompts.yaml missing _objective")
	}
	return b, nil
}

// ProbeOptions is the user-facing input to Probe.
// IncludeFailure pulls signature/lookup/excerpt into Context. Set it only
// when the ask targets the failure (investigate pick, empty-ask fallback,
// landing card); other asks keep the one-line recent-runs summary.
type ProbeOptions struct {
	Ask            string
	IncludeFailure bool
	ExecID         string // target this exec instead of the latest failure
}

// Probe gathers local state for the agent prompt.
// It lists connections via ConnEntries.List() only (name/type/source).
func Probe(opts ProbeOptions) PromptContext {
	cwd := workDir()
	p := PromptContext{
		Version:    core.Version,
		Cwd:        cwd,
		Ask:        strings.TrimSpace(opts.Ask),
		FileCounts: map[string]int{},
	}
	p.ProjectName, p.ProjectRoot, p.HasProject = detectProject(cwd)
	scanRoot := cwd
	if p.ProjectRoot != "" {
		scanRoot = p.ProjectRoot
	}
	p.FileCounts = countCanonicalFiles(scanRoot)
	p.Connections, p.ConnectionExtra = capProbeConnections(listProbeConnections())
	p.RecentRuns = recentRunsForPrompt()
	p.MCPWired = probeMCPWired()
	if prof, ok, _ := LoadProfile(); ok {
		p.Agent = prof.Agent
	}
	if opts.ExecID != "" {
		if le, ok := LookupLocalExec(opts.ExecID); ok {
			p.TargetExec = &le
			p.Signature, p.Lookup, p.ErrorExcerpt = failureDetails(le)
			p.RunLogExcerpt = runLogExcerpt(le)
		}
	} else if opts.IncludeFailure {
		if lead, ok := leadingFailure(p.RecentRuns); ok {
			p.Signature, p.Lookup, p.ErrorExcerpt = failureDetails(lead)
			p.RunLogExcerpt = runLogExcerpt(lead)
		}
	}
	for _, s := range p.suggestions() {
		p.Suggestions = append(p.Suggestions, s.Label)
	}
	p.Route = Route(p)
	return p
}

// Route picks the prompt ladder rung. Pure Go, no LLM.
// Order: ask > recent failed run > zero connections > no project in cwd > default.
// Free function: PromptContext already has a Route field.
func Route(p PromptContext) string {
	if p.TargetExec != nil {
		return "failed_run"
	}
	if strings.TrimSpace(p.Ask) != "" {
		return "ask"
	}
	if _, ok := leadingFailure(p.RecentRuns); ok {
		return "failed_run"
	}
	if userConnectionCount(p.Connections) == 0 {
		return "zero_connections"
	}
	if !p.HasProject {
		return "no_project"
	}
	return "default"
}

const (
	suggestionElseLabel = "Something else — just describe it"
	maxSuggestionRows   = 4
	maxSignalRows       = 3
)

// suggestion is one open-screen row. Label is shown; Ask is the launch text.
// Investigate marks the failed-run row: only that pick puts failure details
// (signature, lookup, excerpt) into the prompt Context.
type suggestion struct {
	Label       string
	Ask         string // empty → free text ("something else")
	Investigate bool
}

func (p PromptContext) suggestions() []suggestion {
	var signals []suggestion
	if fail, ok := leadingFailure(p.RecentRuns); ok {
		label := failedRunLabel(fail)
		signals = append(signals, suggestion{Label: label, Ask: label, Investigate: true})
	}
	if userConnectionCount(p.Connections) == 0 {
		signals = append(signals, suggestion{
			Label: "Add your first connection",
			Ask:   "Add a connection",
		})
	}
	if !p.HasProject {
		signals = append(signals, suggestion{
			Label: "Scaffold a project (`sling init`)",
			Ask:   "Scaffold a Sling project in this folder",
		})
	}
	out := signals
	if len(out) < maxSignalRows {
		out = append(out, suggestion{
			Label: "Create or update a replication / pipeline / model / API spec",
			Ask:   "Help me create or update a Sling config (replication, pipeline, model, or API spec). First ask me which one and which connections it involves.",
		})
	}
	if len(out) > maxSignalRows {
		out = out[:maxSignalRows]
	}
	out = append(out, suggestion{Label: suggestionElseLabel})
	if len(out) > maxSuggestionRows {
		out = append(out[:maxSuggestionRows-1], suggestion{Label: suggestionElseLabel})
	}
	return out
}

func failedRunLabel(r LocalExec) string {
	idObj := strings.TrimSpace(r.ID + " " + r.displayObject())
	idObj = strings.Join(strings.Fields(idObj), " ")
	s := "Investigate the failed run " + idObj
	if r.When.IsZero() {
		return s
	}
	rt := relTime(r.When)
	if rt == "just now" {
		return s + " (just now)"
	}
	return s + " (" + rt + " ago)"
}

func userConnectionCount(conns []ProbeConn) int {
	n := 0
	for _, c := range conns {
		if c.Source == "built-in" {
			continue
		}
		n++
	}
	return n
}

func leadingFailure(runs []LocalExec) (LocalExec, bool) {
	for _, r := range runs {
		if r.Status == "err" {
			return r, true
		}
	}
	return LocalExec{}, false
}

func listProbeConnections() []ProbeConn {
	entries := connection.GetLocalConns(true)
	_, rows := entries.List()
	out := make([]ProbeConn, 0, len(rows))
	for _, row := range rows {
		if len(row) < 3 {
			continue
		}
		out = append(out, ProbeConn{
			Name:   cast.ToString(row[0]),
			Type:   cast.ToString(row[1]),
			Source: cast.ToString(row[2]),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := connSourceRank(out[i].Source), connSourceRank(out[j].Source)
		if si != sj {
			return si < sj
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func connSourceRank(source string) int {
	switch source {
	case "sling env yaml":
		return 0
	case "built-in":
		return 1
	default:
		return 2
	}
}

func capProbeConnections(all []ProbeConn) ([]ProbeConn, int) {
	if len(all) <= maxProbeConnections {
		return all, 0
	}
	return all[:maxProbeConnections], len(all) - maxProbeConnections
}

func detectProject(cwd string) (name, root string, has bool) {
	if cwd == "" {
		return "", "", false
	}
	if r, err := project.FindRoot(cwd); err == nil && r != "" {
		m, err := project.Load(r)
		n := filepath.Base(r)
		if err == nil && strings.TrimSpace(m.Name) != "" {
			n = m.Name
		}
		return n, r, true
	}
	if hasCanonicalFolders(cwd) {
		return filepath.Base(cwd), cwd, true
	}
	return "", cwd, false
}

func hasCanonicalFolders(dir string) bool {
	if project.HasManifest(dir) {
		return true
	}
	for _, n := range canonicalFolders {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			return true
		}
	}
	return false
}

func countCanonicalFiles(root string) map[string]int {
	out := map[string]int{}
	for _, folder := range canonicalFolders {
		out[folder] = scanCanonicalFolder(root, folder)
	}
	return out
}

func scanCanonicalFolder(root, folder string) int {
	dir := filepath.Join(root, folder)
	n := 0
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(p))
		if ext != ".yaml" && ext != ".yml" && ext != ".sql" {
			return nil
		}
		body, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil
		}
		if validate.DetectFileKind(body, p) != validate.KindUnknown {
			n++
		}
		return nil
	})
	return n
}

func recentRunsForPrompt() []LocalExec {
	execs, err := ListLocalExecs()
	if err != nil || len(execs) == 0 {
		return nil
	}
	sort.SliceStable(execs, func(i, j int) bool {
		iFail := execs[i].Status == "err"
		jFail := execs[j].Status == "err"
		if iFail != jFail {
			return iFail
		}
		return execs[i].When.After(execs[j].When)
	})
	if len(execs) > 3 {
		execs = execs[:3]
	}
	return execs
}

func probeMCPWired() bool {
	report, err := Doctor(context.Background())
	if err != nil || report == nil || report.Matrix == nil {
		return false
	}
	for _, row := range report.Matrix.Rows {
		if row.Label != "MCP" {
			continue
		}
		for _, st := range row.Cells {
			if st == CellOK {
				return true
			}
		}
	}
	return false
}

// maxRunLogLines caps the run-log tail that enters the prompt.
const maxRunLogLines = 40

// runLogExcerpt returns the tail of the captured run log for this exec.
// Empty when nothing was captured or it only mirrors error.txt.
func runLogExcerpt(le LocalExec) string {
	b, err := os.ReadFile(filepath.Join(le.LogDir, "stderr.log"))
	if err != nil {
		return ""
	}
	errB, _ := os.ReadFile(filepath.Join(le.LogDir, "error.txt"))
	if string(b) == string(errB) {
		return "" // pre-capture snapshot: stderr.log duplicates error.txt
	}
	return capExcerptLines(sanitizeLogForPrompt(string(b), 0), maxRunLogLines)
}

func failureDetails(le LocalExec) (sig string, lookup *ErrorLookupResult, excerpt string) {
	metaPath := filepath.Join(le.LogDir, "meta.json")
	if b, err := os.ReadFile(metaPath); err == nil {
		doc := map[string]any{}
		if json.Unmarshal(b, &doc) == nil {
			sig = cast.ToString(doc["error_signature"])
			if sig == "" {
				sig = cast.ToString(doc["error_pattern_id"])
			}
		}
	}
	if sig != "" {
		if r, err := LookupError(sig); err == nil {
			lookup = &r
		}
	}
	errPath := filepath.Join(le.LogDir, "error.txt")
	if b, err := os.ReadFile(errPath); err == nil {
		excerpt = capExcerptLines(sanitizeLogForPrompt(string(b), 0), maxErrorExcerptLines)
	}
	return sig, lookup, excerpt
}

func capExcerptLines(s string, n int) string {
	s = strings.TrimRight(s, "\n")
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

// Render fills the five-section skeleton. Caps at ~maxPromptTokens.
func (p PromptContext) Render() (string, error) {
	bundle, err := loadPromptBundle()
	if err != nil {
		return "", err
	}
	p = p.shrinkContextForBudget()
	view := promptView{
		Rules:     strings.TrimSpace(bundle.Rules),
		State:     p.renderState(),
		Context:   p.renderContext(),
		Ask:       p.renderAsk(),
		Objective: p.renderObjective(bundle),
	}
	tmpl, err := template.New("skeleton").Parse(bundle.Skeleton)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, view); err != nil {
		return "", err
	}
	out := strings.TrimRight(buf.String(), "\n") + "\n"
	return capPromptTokens(out), nil
}

// RenderPrompt is the exported wrapper for (PromptContext).Render.
func RenderPrompt(p PromptContext) (string, error) {
	return p.Render()
}

func (p PromptContext) renderState() string {
	var b strings.Builder
	fmt.Fprintf(&b, "- Sling version: %s\n", p.Version)
	fmt.Fprintf(&b, "- cwd: %s\n", p.Cwd)
	switch {
	case p.HasProject && p.ProjectName != "":
		fmt.Fprintf(&b, "- project: %s (%s)\n", p.ProjectName, p.ProjectRoot)
	case p.HasProject:
		fmt.Fprintf(&b, "- project: %s\n", p.ProjectRoot)
	default:
		b.WriteString("- project: (none in cwd)\n")
	}
	if p.MCPWired {
		b.WriteString("- MCP wired: yes\n")
	} else {
		b.WriteString("- MCP wired: no\n")
	}
	if p.Agent != "" {
		fmt.Fprintf(&b, "- preferred agent: %s\n", p.Agent)
	}
	return strings.TrimRight(b.String(), "\n")
}

func (p PromptContext) renderContext() string {
	var b strings.Builder
	b.WriteString("- connections: ")
	b.WriteString(formatProbeConnections(p.Connections, p.ConnectionExtra))
	b.WriteByte('\n')
	b.WriteString("- files: ")
	b.WriteString(formatFileCounts(p.FileCounts))
	b.WriteByte('\n')
	b.WriteString("- recent runs: ")
	b.WriteString(formatRecentRuns(p.RecentRuns))
	b.WriteByte('\n')
	if p.Signature != "" {
		fmt.Fprintf(&b, "- error signature: %s\n", p.Signature)
		if p.Lookup != nil && p.Lookup.Title != "" {
			fmt.Fprintf(&b, "- error lookup: %s (%s)\n", p.Lookup.Title, p.Lookup.Status)
		}
	}
	if strings.TrimSpace(p.ErrorExcerpt) != "" {
		b.WriteString("- error excerpt:\n")
		for _, line := range strings.Split(p.ErrorExcerpt, "\n") {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	if strings.TrimSpace(p.RunLogExcerpt) != "" {
		b.WriteString("- run log (tail):\n")
		for _, line := range strings.Split(p.RunLogExcerpt, "\n") {
			fmt.Fprintf(&b, "  %s\n", line)
		}
	}
	if strings.TrimSpace(p.Ask) == "" && len(p.Suggestions) > 0 {
		b.WriteString("- suggestions: ")
		b.WriteString(strings.Join(p.Suggestions, "; "))
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

func formatProbeConnections(conns []ProbeConn, extra int) string {
	if len(conns) == 0 {
		return "(none — run `sling conns set --type <type>` with ${VAR} refs; never ask for credentials in chat)"
	}
	parts := make([]string, 0, len(conns))
	for _, c := range conns {
		if c.Type != "" {
			parts = append(parts, fmt.Sprintf("%s (%s)", c.Name, c.Type))
		} else {
			parts = append(parts, c.Name)
		}
	}
	s := strings.Join(parts, ", ")
	if extra > 0 {
		s += fmt.Sprintf(" and %d more — run `sling conns list`", extra)
	}
	return s
}

func formatFileCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(canonicalFolders))
	total := 0
	for _, k := range canonicalFolders {
		n := counts[k]
		total += n
		parts = append(parts, fmt.Sprintf("%s %d", k, n))
	}
	if total == 0 {
		return "(none)"
	}
	return strings.Join(parts, ", ")
}

func formatRecentRuns(runs []LocalExec) string {
	if len(runs) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(runs))
	for _, r := range runs {
		label := r.ID
		if obj := r.displayObject(); obj != "" {
			label = r.ID + " " + obj
		}
		parts = append(parts, fmt.Sprintf("%s [%s]", label, r.Status))
	}
	return strings.Join(parts, "; ")
}

func (p PromptContext) renderAsk() string {
	if strings.TrimSpace(p.Ask) != "" {
		return p.Ask
	}
	if p.TargetExec != nil {
		return failedRunLabel(*p.TargetExec)
	}
	return "(none — ask the user what they want; the suggestions are in Context)"
}

func (p PromptContext) renderObjective(bundle promptBundle) string {
	return strings.TrimSpace(bundle.Objective)
}

func estimateTokens(s string) int {
	n := len([]rune(s))
	return (n + 3) / 4
}

const truncNote = "[...truncated to ~1500 tokens...]"

func (p PromptContext) shrinkContextForBudget() PromptContext {
	p.ErrorExcerpt = capExcerptLines(p.ErrorExcerpt, maxErrorExcerptLines)
	p.RunLogExcerpt = capExcerptLines(p.RunLogExcerpt, maxRunLogLines)
	for n := maxErrorExcerptLines; n > 3 && estimateTokens(p.renderContext()) > maxPromptTokens/2; n -= 4 {
		p.ErrorExcerpt = capExcerptLines(p.ErrorExcerpt, n)
		p.RunLogExcerpt = capExcerptLines(p.RunLogExcerpt, n)
	}
	if estimateTokens(p.renderContext()) > maxPromptTokens/2 && len(p.Connections) > 3 {
		extra := len(p.Connections) - 3
		p.Connections = p.Connections[:3]
		p.ConnectionExtra += extra
	}
	if estimateTokens(p.renderContext()) > maxPromptTokens/2 && len(p.Suggestions) > 1 {
		p.Suggestions = p.Suggestions[len(p.Suggestions)-1:]
	}
	return p
}

func capPromptTokens(s string) string {
	if estimateTokens(s) <= maxPromptTokens {
		return s
	}
	const ctxH = "# Context\n"
	const askH = "\n# Ask\n"
	ctxAt := strings.Index(s, ctxH)
	askAt := strings.Index(s, askH)
	if ctxAt < 0 || askAt <= ctxAt {
		return s
	}
	prefix := s[:ctxAt+len(ctxH)]
	ctxBody := s[ctxAt+len(ctxH) : askAt]
	suffix := s[askAt:]
	budget := maxPromptTokens * 4
	keep := budget - len([]rune(prefix)) - len([]rune(suffix)) - len([]rune(truncNote)) - 2
	if keep < 80 {
		return prefix + truncNote + "\n" + suffix
	}
	runes := []rune(ctxBody)
	if len(runes) <= keep {
		return s
	}
	cut := keep
	for cut > 0 && runes[cut-1] != '\n' {
		cut--
	}
	if cut < keep/2 {
		cut = keep
	}
	return prefix + string(runes[:cut]) + "\n" + truncNote + "\n" + suffix
}

// LandingKind is the bare-`sling` TTY card.
type LandingKind string

const (
	LandingFresh     LandingKind = "fresh"
	LandingNoProject LandingKind = "no_project"
	LandingProject   LandingKind = "project"
)

// IsFreshInstall is true when env.yaml is the seeded default and there is
// no assist history (sessions or failure snapshots). Does not create dirs.
func IsFreshInstall() bool {
	home := slingHome()
	// Any session or error snapshot under ~/.sling/assist means not fresh.
	if home != "" {
		for _, rel := range []string{
			filepath.Join("assist", "history"),
			filepath.Join("assist", "errors"),
		} {
			entries, err := os.ReadDir(filepath.Join(home, rel))
			if err != nil {
				continue
			}
			for _, e := range entries {
				if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
					return false
				}
			}
		}
	}
	path := envFilePath()
	// Missing env.yaml is fresh. A user connection or assist profile is not.
	if path == "" {
		return true
	}
	if _, err := os.Stat(path); err != nil {
		return true
	}
	ef := env.LoadEnvFile(path)
	if len(ef.Connections) > 0 {
		return false
	}
	if _, ok := ef.Env[assistEnvKey]; ok {
		return false
	}
	for _, v := range ef.Env {
		if v == nil {
			continue
		}
		if strings.TrimSpace(fmt.Sprint(v)) != "" {
			return false
		}
	}
	return true
}

// ClassifyLanding picks the card. Fresh wins so a default home never
// falls through to the probe states.
func ClassifyLanding(p PromptContext, fresh bool) LandingKind {
	if fresh {
		return LandingFresh
	}
	if p.HasProject {
		return LandingProject
	}
	return LandingNoProject
}

// SuggestedCommand is the wave-7 ladder mapped to one runnable command.
func (p PromptContext) SuggestedCommand() string {
	switch Route(p) {
	case "failed_run":
		if sig := strings.TrimSpace(p.Signature); sig != "" {
			return "sling assist error " + sig
		}
		return "sling assist"
	case "zero_connections":
		return "sling assist"
	case "no_project":
		return "sling init"
	default:
		return "sling assist"
	}
}

// RenderLanding prints the TTY card for one probe state. width 0 → 80.
func RenderLanding(kind LandingKind, p PromptContext, width int) string {
	if width <= 0 {
		width = 80
	}
	var body string
	switch kind {
	case LandingFresh:
		// All three steps are `sling assist` so the card never points at `conns set`.
		body = `Welcome to sling. Three steps to your first data flow:

  1. Set up your agent      sling assist
  2. Add a connection       sling assist   (or edit ~/.sling/env.yaml)
  3. Move some data         sling assist

Docs: https://docs.slingdata.io
`
	case LandingNoProject:
		// Conn count plus a pointer at `sling init`.
		n := userConnectionCount(p.Connections)
		noun := "connections"
		if n == 1 {
			noun = "connection"
		}
		body = fmt.Sprintf("%d %s configured (`sling conns list`). No project here — `sling init` scaffolds one.\n", n, noun)
	default:
		// Project card: name, linked?, file counts, last run, next command.
		name := strings.TrimSpace(p.ProjectName)
		if name == "" {
			if p.ProjectRoot != "" {
				name = filepath.Base(p.ProjectRoot)
			} else {
				name = "(unnamed)"
			}
		}
		linked := "not linked"
		if strings.TrimSpace(p.ProjectRoot) != "" {
			// Linked when sling_project.yml (or .sling.json) has a project id.
			if m, err := project.Load(p.ProjectRoot); err == nil && m.Linked() {
				linked = "linked"
			}
		}
		var b strings.Builder
		fmt.Fprintf(&b, "On project %s (%s)\n", name, linked)
		fmt.Fprintf(&b, "  files: %s\n", formatFileCounts(p.FileCounts))
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
			fmt.Fprintf(&b, "  last run: %s\n", s)
		} else {
			b.WriteString("  last run: none\n")
		}
		fmt.Fprintf(&b, "\n  Next: %s\n", p.SuggestedCommand())
		body = b.String()
	}
	return wrapToWidth(strings.TrimRight(body, "\n")+"\n", width)
}

// relTime is a short age phrase: "just now", "3m", "2h", "1d".
func relTime(t time.Time) string {
	d := time.Since(t)
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

func latestRun(runs []LocalExec) (LocalExec, bool) {
	var best LocalExec
	ok := false
	for _, r := range runs {
		if !ok || r.When.After(best.When) {
			best, ok = r, true
		}
	}
	return best, ok
}

func wrapToWidth(s string, width int) string {
	if width <= 0 {
		width = 80
	}
	// Keep `backtick commands` as one token so a wrapped card stays copyable.
	leadingSpaces := func(line string) string {
		i := 0
		for i < len(line) && line[i] == ' ' {
			i++
		}
		return line[:i]
	}
	wrapTokens := func(in string) []string {
		var words []string
		var cur strings.Builder
		inTick := false
		for _, r := range in {
			switch {
			case r == '`':
				inTick = !inTick
				cur.WriteRune(r)
			case r == ' ' && !inTick:
				if cur.Len() > 0 {
					words = append(words, cur.String())
					cur.Reset()
				}
			default:
				cur.WriteRune(r)
			}
		}
		if cur.Len() > 0 {
			words = append(words, cur.String())
		}
		return words
	}
	chunkRunes := func(in string, w int) []string {
		runes := []rune(in)
		var out []string
		for len(runes) > w {
			out = append(out, string(runes[:w]))
			runes = runes[w:]
		}
		if len(runes) > 0 {
			out = append(out, string(runes))
		}
		return out
	}
	wrapLine := func(line string) []string {
		if utf8.RuneCountInString(line) <= width {
			return []string{line}
		}
		indent := leadingSpaces(line)
		body := strings.TrimLeft(line, " ")
		avail := width - len(indent)
		if avail < 20 {
			avail = 20
			indent = ""
		}
		words := wrapTokens(body)
		if len(words) == 0 {
			return []string{line}
		}
		var lines []string
		cur := indent + words[0]
		for _, w := range words[1:] {
			trial := cur + " " + w
			if utf8.RuneCountInString(trial) <= width {
				cur = trial
				continue
			}
			lines = append(lines, cur)
			cur = indent + w
			if utf8.RuneCountInString(cur) > width {
				lines = append(lines, chunkRunes(cur, width)...)
				cur = indent
			}
		}
		if strings.TrimSpace(cur) != "" {
			lines = append(lines, cur)
		}
		return lines
	}

	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, wrapLine(line)...)
	}
	return strings.Join(out, "\n") + "\n"
}
