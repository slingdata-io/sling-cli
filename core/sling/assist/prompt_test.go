package assist

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func sampleLookup() *ErrorLookupResult {
	r, err := LookupError("97d84811")
	if err != nil {
		return &ErrorLookupResult{Signature: "97d84811", Status: "unknown", Title: "No published guidance yet"}
	}
	return &r
}

func snapshotCtx(route string) PromptContext {
	base := PromptContext{
		Version:     "dev",
		Cwd:         "/work",
		ProjectName: "demo",
		ProjectRoot: "/work",
		HasProject:  true,
		FileCounts:  map[string]int{"replications": 1, "pipelines": 0, "models": 0, "specs": 0},
		Connections: []ProbeConn{
			{Name: "MY_PG", Type: "PostgreSQL", Source: "sling env yaml"},
			{Name: "MY_SF", Type: "Snowflake", Source: "sling env yaml"},
		},
		MCPWired: true,
		Agent:    "claude",
		Route:    route,
	}
	switch route {
	case "ask":
		base.Ask = "backfill orders"
	case "failed_run":
		base.Signature = "97d84811"
		base.Lookup = sampleLookup()
		base.ErrorExcerpt = "column missing: email_verified"
		base.RecentRuns = []LocalExec{{ID: "exec_fail1", Status: "err", ConfigPath: "./r.yaml"}}
	case "zero_connections":
		base.Connections = nil
		base.HasProject = true
	case "no_project":
		base.HasProject = false
		base.ProjectName = ""
		base.ProjectRoot = "/tmp"
		base.FileCounts = map[string]int{"replications": 0, "pipelines": 0, "models": 0, "specs": 0}
	case "default":
		base.Ask = ""
	}
	for _, s := range base.suggestions() {
		base.Suggestions = append(base.Suggestions, s.Label)
	}
	return base
}

func TestPrintSnapshots(t *testing.T) {
	rungs := []string{"ask", "failed_run", "zero_connections", "no_project", "default"}
	for _, rung := range rungs {
		t.Run(rung, func(t *testing.T) {
			got, err := RenderPrompt(snapshotCtx(rung))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(got, "# Rules") || !strings.Contains(got, "# State") || !strings.Contains(got, "# Context") {
				t.Fatalf("missing section headings:\n%s", got)
			}
			if strings.Contains(got, "super-secret") || strings.Contains(got, "password") {
				t.Fatalf("prompt leaked a secret-shaped value:\n%s", got)
			}
			path := filepath.Join("testdata", "print_"+rung+".golden")
			if os.Getenv("UPDATE_GOLDEN") == "1" {
				if err := os.MkdirAll("testdata", 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden %s: %v (set UPDATE_GOLDEN=1 to write)", path, err)
			}
			if got != string(want) {
				t.Errorf("golden mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", rung, got, want)
			}
		})
	}
}

func TestRouteLadder(t *testing.T) {
	ask := PromptContext{Ask: "do it", RecentRuns: []LocalExec{{Status: "err"}}}
	if Route(ask) != "ask" {
		t.Fatalf("ask rung lost to failure: %s", Route(ask))
	}
	fail := PromptContext{RecentRuns: []LocalExec{{Status: "err"}}}
	if Route(fail) != "failed_run" {
		t.Fatalf("got %s", Route(fail))
	}
	zero := PromptContext{Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}}, HasProject: true}
	if Route(zero) != "zero_connections" {
		t.Fatalf("got %s", Route(zero))
	}
	noProj := PromptContext{Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}}}
	if Route(noProj) != "no_project" {
		t.Fatalf("got %s", Route(noProj))
	}
	def := PromptContext{
		HasProject:  true,
		Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}},
	}
	if Route(def) != "default" {
		t.Fatalf("got %s", Route(def))
	}
}

func TestProbeListsNamesNotSecrets(t *testing.T) {
	dir := withTempHomeDir(t)
	envPath := filepath.Join(dir, "env.yaml")
	body := "connections:\n  MY_PG:\n    type: postgres\n    url: postgresql://user:super-secret-pass@localhost/db\n"
	if err := os.WriteFile(envPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	p := Probe(ProbeOptions{Ask: "sync users"})
	out, err := RenderPrompt(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "# Rules") || !strings.Contains(out, "# State") || !strings.Contains(out, "# Context") {
		t.Fatalf("missing headings:\n%s", out)
	}
	if !strings.Contains(out, "MY_PG") {
		t.Fatalf("expected connection name MY_PG:\n%s", out)
	}
	if strings.Contains(out, "super-secret-pass") {
		t.Fatalf("secret leaked into prompt:\n%s", out)
	}
	if strings.Contains(out, "postgresql://") {
		t.Fatalf("connection URL leaked into prompt:\n%s", out)
	}
}

func TestFailureDetailsGatedByIntent(t *testing.T) {
	withTempHomeDir(t)
	WriteFailureSnapshot(FailureSnapshot{
		ExecID:     "exec_gate1",
		ErrMsg:     "column missing: email_verified",
		ConfigPath: "./r.yaml",
	})

	// A normal ask keeps the one-line run summary only.
	p := Probe(ProbeOptions{Ask: "create a replication"})
	out, err := RenderPrompt(p)
	if err != nil {
		t.Fatal(err)
	}
	if p.Signature != "" || p.ErrorExcerpt != "" {
		t.Fatalf("failure details leaked into ask mode: sig=%q excerpt=%q", p.Signature, p.ErrorExcerpt)
	}
	if strings.Contains(out, "error excerpt") || strings.Contains(out, "error signature") {
		t.Fatalf("ask-mode prompt contains failure details:\n%s", out)
	}
	if !strings.Contains(out, "exec_gate1") || !strings.Contains(out, "[err]") {
		t.Fatalf("ask-mode prompt lost the recent-runs summary:\n%s", out)
	}

	// The investigate intent pulls the full details in.
	p = Probe(ProbeOptions{Ask: "Investigate the failed run exec_gate1 ./r.yaml", IncludeFailure: true})
	out, err = RenderPrompt(p)
	if err != nil {
		t.Fatal(err)
	}
	if p.Signature == "" {
		t.Fatal("investigate mode missing signature")
	}
	if !strings.Contains(out, "error excerpt") || !strings.Contains(out, "column missing: email_verified") {
		t.Fatalf("investigate prompt missing failure details:\n%s", out)
	}
}

func TestPromptAssemblyDoesNotCallExpandEnvVars(t *testing.T) {
	files := []string{"prompt.go", "session.go"}
	for _, name := range files {
		b, err := os.ReadFile(name)
		if err != nil {
			// a rename must fail the guard, not skip it
			t.Fatalf("cannot read %s: %s", name, err)
		}
		if bytes.Contains(b, []byte("ExpandEnvVars")) {
			t.Errorf("%s must not call ExpandEnvVars", name)
		}
		if bytes.Contains(b, []byte(".Data")) && bytes.Contains(b, []byte("Connection")) {
			t.Errorf("%s must not read connection payload fields", name)
		}
	}
}

func TestConnectionCap(t *testing.T) {
	all := make([]ProbeConn, 12)
	for i := range all {
		all[i] = ProbeConn{Name: "C" + strings.Repeat("X", 1), Type: "PostgreSQL"}
		all[i].Name = "CONN_" + string(rune('A'+i))
	}
	got, extra := capProbeConnections(all)
	if len(got) != maxProbeConnections || extra != 2 {
		t.Fatalf("len=%d extra=%d", len(got), extra)
	}
	s := formatProbeConnections(got, extra)
	if !strings.Contains(s, "and 2 more — run `sling conns list`") {
		t.Fatalf("cap suffix missing: %s", s)
	}
}

func TestErrorExcerptCaps15Lines(t *testing.T) {
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, "line")
	}
	got := capExcerptLines(strings.Join(lines, "\n"), maxErrorExcerptLines)
	if n := strings.Count(got, "\n") + 1; n != 15 {
		t.Fatalf("lines=%d want 15", n)
	}
}

func TestNestedLaunchPrintsInsteadOfSpawn(t *testing.T) {
	withTempHomeDir(t)
	t.Setenv("CLAUDECODE", "1")
	buf := &bytes.Buffer{}
	prev := assistOut
	assistOut = buf
	defer func() { assistOut = prev }()

	out, err := Session(SessionOptions{Ask: "backfill orders"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "# Rules") {
		t.Fatalf("stdout missing Rules:\n%s", buf.String())
	}
	if !strings.Contains(out, "backfill orders") {
		t.Fatalf("ask missing from prompt:\n%s", out)
	}
}

func TestAPISpecsSkillMentionsAgentBrowser(t *testing.T) {
	b, err := SkillsFS.ReadFile("skills/sling-api-specs/SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte("agent-browser")) {
		t.Fatal("sling-api-specs/SKILL.md must mention agent-browser")
	}
	if bytes.Contains(b, []byte("sling assist --browse")) {
		t.Fatal("sling-api-specs/SKILL.md still mentions sling assist --browse")
	}
}

func TestPipelineSkillsTeachStateResult(t *testing.T) {
	// Skills must teach the runtime shape, not the rejected one.
	skill, err := SkillsFS.ReadFile("skills/sling-pipelines/SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	steps, err := SkillsFS.ReadFile("skills/sling-pipelines/STEPS.md")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(skill, []byte("state.count_query.result[0]")) {
		t.Fatal("SKILL.md must teach state.<id>.result[0]")
	}
	if bytes.Contains(skill, []byte("state.count_query[0]")) {
		t.Fatal("SKILL.md still teaches the rejected state.<id>[0] shape")
	}
	if !bytes.Contains(steps, []byte("state.my_query.result[0]")) {
		t.Fatal("STEPS.md must teach state.<id>.result[0]")
	}
	if bytes.Contains(steps, []byte("state.my_query[0]")) {
		t.Fatal("STEPS.md still teaches the rejected state.<id>[0] shape")
	}
}

func TestSkillsExpressionsParse(t *testing.T) {
	// Every {…} expression in the skill bundle must pass the real parser.
	eval := iop.NewEvaluator([]string{
		"env", "state", "secrets", "auth", "response", "request", "sync",
		"context", "record", "queue", "source", "target", "stream", "object",
		"timestamp", "store", "execution", "loop", "run",
	})
	var failed []string
	err := fs.WalkDir(SkillsFS, "skills", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".md") && !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}
		body, err := SkillsFS.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(body)
		for _, span := range incorrectExampleSpans(text) {
			text = text[:span[0]] + strings.Repeat(" ", span[1]-span[0]) + text[span[1]:]
		}
		for _, expr := range extractBraceExprs(text) {
			if err := eval.Check(expr); err != nil {
				failed = append(failed, path+": "+err.Error())
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) > 0 {
		t.Fatalf("skills teach expressions the runtime rejects:\n%s", strings.Join(failed, "\n"))
	}
}

func extractBraceExprs(s string) []string {
	var out []string
	for i := 0; i < len(s); i++ {
		if s[i] != '{' {
			continue
		}
		if i+1 < len(s) && (s[i+1] == '{' || s[i+1] == '#' || s[i+1] == '%') {
			continue
		}
		depth := 1
		j := i + 1
		for j < len(s) && depth > 0 {
			switch s[j] {
			case '{':
				depth++
			case '}':
				depth--
			}
			j++
		}
		if depth != 0 {
			continue
		}
		expr := strings.TrimSpace(s[i+1 : j-1])
		if !looksLikeRuntimeExpr(expr) {
			i = j - 1
			continue
		}
		out = append(out, expr)
		i = j - 1
	}
	return out
}

func incorrectExampleSpans(s string) [][2]int {
	var out [][2]int
	for _, marker := range []string{"# ❌", "❌ Incorrect", "Incorrect - single quotes"} {
		i := 0
		for {
			j := strings.Index(s[i:], marker)
			if j < 0 {
				break
			}
			start := i + j
			end := strings.Index(s[start:], "\n```")
			if end < 0 {
				end = len(s) - start
			}
			out = append(out, [2]int{start, start + end})
			i = start + len(marker)
		}
	}
	return out
}

func looksLikeRuntimeExpr(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" || strings.HasPrefix(expr, "#") || strings.HasPrefix(expr, "%") {
		return false
	}
	// Python / JSON dicts in skill examples: {'a': 1}, {"id": 1}
	if strings.HasPrefix(expr, "'") || (strings.HasPrefix(expr, `"`) && strings.Contains(expr, ":")) {
		return false
	}
	if strings.ContainsAny(expr, "()") {
		return true
	}
	if strings.Contains(expr, " + ") || strings.Contains(expr, " - ") ||
		strings.Contains(expr, " == ") || strings.Contains(expr, " != ") {
		return true
	}
	return false
}

func TestGatherFirstIntroDoNotAskWhenResolved(t *testing.T) {
	names := []string{
		"sling-replications/SKILL.md",
		"sling-pipelines/SKILL.md",
		"sling-build/SKILL.md",
		"sling-api-specs/SKILL.md",
		"sling/CONNECTIONS.md",
	}
	needle := []byte("If every row resolves, do not ask")
	for _, name := range names {
		b, err := SkillsFS.ReadFile("skills/" + name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !bytes.Contains(b, needle) {
			t.Errorf("%s missing Gather first intro", name)
		}
	}
}

func TestSuggestionPriorityFullStack(t *testing.T) {
	p := PromptContext{
		HasProject:  false,
		Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}},
		RecentRuns:  []LocalExec{{ID: "exec1", Status: "err", ConfigPath: "./r.yaml"}},
	}
	got := p.suggestions()
	if len(got) != 4 {
		t.Fatalf("len=%d want 4: %+v", len(got), got)
	}
	if !strings.Contains(got[0].Label, "Investigate the failed run") {
		t.Fatalf("first: %s", got[0].Label)
	}
	if got[1].Label != "Add your first connection" {
		t.Fatalf("second: %s", got[1].Label)
	}
	if !strings.Contains(got[2].Label, "Scaffold a project") {
		t.Fatalf("third: %s", got[2].Label)
	}
	if got[3].Label != suggestionElseLabel {
		t.Fatalf("last: %s", got[3].Label)
	}
	for _, s := range got {
		if strings.Contains(s.Label, "Create or update") {
			t.Fatal("filler must drop when three signals fill the cap")
		}
	}
}

func TestSuggestionFillerWhenSlotsRemain(t *testing.T) {
	p := PromptContext{
		HasProject:  true,
		Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}},
	}
	got := p.suggestions()
	if len(got) != 2 {
		t.Fatalf("len=%d want 2: %+v", len(got), got)
	}
	if !strings.Contains(got[0].Label, "Create or update") {
		t.Fatalf("filler first: %s", got[0].Label)
	}
	if got[1].Label != suggestionElseLabel {
		t.Fatalf("else last: %s", got[1].Label)
	}
}

func TestAskModeObjectiveHasGatherFirst(t *testing.T) {
	out, err := RenderPrompt(snapshotCtx("ask"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "  1. ") {
		t.Fatalf("ask mode must not print a menu:\n%s", out)
	}
	if !strings.Contains(out, "Gather first") {
		t.Fatalf("Objective missing Gather first:\n%s", out)
	}
	if !strings.Contains(out, "Load the matching Sling skill") {
		t.Fatalf("Objective missing skill rule:\n%s", out)
	}
	if strings.Contains(out, "- suggestions:") {
		t.Fatalf("ask mode must not list suggestions:\n%s", out)
	}
}

func TestEmptyAskFallbackSuggestionsLine(t *testing.T) {
	out, err := RenderPrompt(snapshotCtx("default"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "- suggestions:") {
		t.Fatalf("empty-ask Context missing suggestions:\n%s", out)
	}
	if !strings.Contains(out, suggestionElseLabel) {
		t.Fatalf("missing else row:\n%s", out)
	}
	if !strings.Contains(out, "ask the user what they want") {
		t.Fatalf("missing empty-ask fallback:\n%s", out)
	}
}

func TestTruncationNeverCutsObjective(t *testing.T) {
	excerpt := strings.Repeat("error line that is fairly long for the cap test\n", 80)
	conns := make([]ProbeConn, 10)
	for i := range conns {
		conns[i] = ProbeConn{Name: "CONN_" + strings.Repeat("X", 40) + string(rune('A'+i)), Type: "PostgreSQL", Source: "sling env yaml"}
	}
	p := PromptContext{
		Version:      "dev",
		Cwd:          "/work",
		HasProject:   true,
		ProjectName:  "demo",
		ProjectRoot:  "/work",
		FileCounts:   map[string]int{"replications": 1, "pipelines": 0, "models": 0, "specs": 0},
		Connections:  conns,
		ErrorExcerpt: excerpt,
		Ask:          "backfill orders",
		Suggestions: []string{
			strings.Repeat("Investigate the failed run a-very-long-id ./r.yaml (3m ago)", 8),
			suggestionElseLabel,
		},
	}
	out, err := RenderPrompt(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "# Objective") {
		t.Fatalf("Objective heading cut:\n%s", out)
	}
	if !strings.Contains(out, "Gather first") {
		t.Fatalf("Objective body cut:\n%s", out)
	}
	if !strings.Contains(out, "Load the matching Sling skill") {
		t.Fatalf("Objective skill rule cut:\n%s", out)
	}
}

func TestSessionPrintTwiceStable(t *testing.T) {
	withTempHomeDir(t)
	opts := SessionOptions{Ask: "backfill orders", Print: true}
	buf1, buf2 := &bytes.Buffer{}, &bytes.Buffer{}
	prev := assistOut
	defer func() { assistOut = prev }()
	assistOut = buf1
	a, err := Session(opts)
	if err != nil {
		t.Fatal(err)
	}
	assistOut = buf2
	b, err := Session(opts)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || buf1.String() != buf2.String() {
		t.Fatalf("print not stable\n---1---\n%s\n---2---\n%s", buf1.String(), buf2.String())
	}
	for _, heading := range []string{"# Rules", "# State", "# Context"} {
		if !strings.Contains(a, heading) {
			t.Fatalf("missing %s", heading)
		}
	}
}

func TestRelTime(t *testing.T) {
	now := time.Now()
	cases := []struct {
		t    time.Time
		want string
	}{
		{now.Add(-10 * time.Second), "just now"},
		{now.Add(-3 * time.Minute), "3m"},
		{now.Add(-2 * time.Hour), "2h"},
		{now.Add(-25 * time.Hour), "1d"},
	}
	for _, tc := range cases {
		if got := relTime(tc.t); got != tc.want {
			t.Errorf("relTime(%v)=%q want %q", tc.t, got, tc.want)
		}
	}
}

func TestRenderOpenCardFailedRunRow(t *testing.T) {
	p := PromptContext{
		HasProject:  true,
		ProjectName: "demo",
		Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}},
		RecentRuns: []LocalExec{{
			ID:         "exec_fail1",
			Status:     "err",
			ConfigPath: "./r.yaml",
			When:       time.Now().Add(-3 * time.Minute),
		}},
	}
	got := renderOpenCard(p, 80)
	if !strings.Contains(got, "On project demo") {
		t.Fatalf("missing project line:\n%s", got)
	}
	if !strings.Contains(got, "Investigate the failed run exec_fail1 ./r.yaml (3m ago)") {
		t.Fatalf("missing failed-run row:\n%s", got)
	}
	if !strings.Contains(got, "3m ago") {
		t.Fatalf("missing rel-time:\n%s", got)
	}
	if !strings.Contains(got, suggestionElseLabel) {
		t.Fatalf("missing else row:\n%s", got)
	}
}

func TestRenderOpenCardZeroConnRow(t *testing.T) {
	p := PromptContext{
		HasProject:  true,
		ProjectName: "demo",
		Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}},
	}
	got := renderOpenCard(p, 80)
	if !strings.Contains(got, "Add your first connection") {
		t.Fatalf("missing zero-conn row:\n%s", got)
	}
	if !strings.Contains(got, "0 connections") {
		t.Fatalf("missing conn count:\n%s", got)
	}
}

func TestResolveOpenPick(t *testing.T) {
	p := PromptContext{
		HasProject:  false,
		Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}},
		RecentRuns:  []LocalExec{{ID: "exec1", Status: "err", ConfigPath: "./r.yaml"}},
	}
	opts := p.suggestions()
	var buf bytes.Buffer
	ask, investigate, ok := readOpenAsk(strings.NewReader("1\n"), &buf, opts)
	if !ok {
		t.Fatal("pick 1 aborted")
	}
	if !strings.Contains(ask, "Investigate the failed run") {
		t.Fatalf("ask=%q", ask)
	}
	if !investigate {
		t.Fatal("investigate pick must set the investigate flag")
	}

	ask, investigate, ok = readOpenAsk(strings.NewReader("2\n"), &buf, opts)
	if !ok || ask != "Add a connection" || investigate {
		t.Fatalf("pick 2: ok=%v ask=%q investigate=%v", ok, ask, investigate)
	}

	ask, investigate, ok = readOpenAsk(strings.NewReader("3\n"), &buf, opts)
	if !ok || ask != "Scaffold a Sling project in this folder" || investigate {
		t.Fatalf("pick 3: ok=%v ask=%q investigate=%v", ok, ask, investigate)
	}

	ask, investigate, ok = readOpenAsk(strings.NewReader("backfill orders\n"), &buf, opts)
	if !ok || ask != "backfill orders" || investigate {
		t.Fatalf("free text: ok=%v ask=%q investigate=%v", ok, ask, investigate)
	}
}

func TestReadOpenAskEmptyLineExit(t *testing.T) {
	opts := []suggestion{{Label: suggestionElseLabel}}
	var buf bytes.Buffer
	ask, _, ok := readOpenAsk(strings.NewReader("\n\n"), &buf, opts)
	if ok || ask != "" {
		t.Fatalf("ok=%v ask=%q", ok, ask)
	}
	if !strings.Contains(buf.String(), `sling assist "<what you want>"`) {
		t.Fatalf("missing hint:\n%s", buf.String())
	}
}

func TestSessionOpenScreenDoesNotLaunchBeforePick(t *testing.T) {
	withTempHomeDir(t)
	prevTTY, prevIn, prevOut := ttyCheck, assistIn, assistOut
	t.Cleanup(func() {
		ttyCheck = prevTTY
		assistIn = prevIn
		assistOut = prevOut
	})
	ttyCheck = func(*os.File) bool { return true }
	assistIn = strings.NewReader("\n\n")
	buf := &bytes.Buffer{}
	assistOut = buf

	id, err := Session(SessionOptions{})
	if err != nil {
		t.Fatalf("empty abort should exit 0: %v", err)
	}
	if id != "" {
		t.Fatalf("launched session %q", id)
	}
	if !strings.Contains(buf.String(), suggestionElseLabel) {
		t.Fatalf("card missing:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), `sling assist "<what you want>"`) {
		t.Fatalf("missing hint:\n%s", buf.String())
	}
}

func TestSessionHeadlessNoAskErrors(t *testing.T) {
	withTempHomeDir(t)
	_, err := Session(SessionOptions{Headless: true})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no ask given") {
		t.Fatalf("got %v", err)
	}
}

func TestSessionModeAskWrittenToMeta(t *testing.T) {
	dir := withTempHomeDir(t)
	if err := os.MkdirAll(dir+"/bin", 0o755); err != nil {
		t.Fatal(err)
	}
	stub := dir + "/bin/claude"
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+"/bin"+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := SaveProfile(Profile{Agent: "claude"}); err != nil {
		t.Fatal(err)
	}

	id, err := Session(SessionOptions{Ask: "backfill orders", Headless: true, Agent: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := LoadEntry(id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Meta.Task != modeAsk {
		t.Fatalf("Meta.Task=%q want %q", e.Meta.Task, modeAsk)
	}
	if e.Answers.Task != modeAsk {
		t.Fatalf("Answers.Task=%q want %q", e.Answers.Task, modeAsk)
	}
}

func TestSessionOpenModeWrittenToMeta(t *testing.T) {
	dir := withTempHomeDir(t)
	if err := os.MkdirAll(dir+"/bin", 0o755); err != nil {
		t.Fatal(err)
	}
	stub := dir + "/bin/claude"
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+"/bin"+string(os.PathListSeparator)+os.Getenv("PATH"))
	if err := SaveProfile(Profile{Agent: "claude"}); err != nil {
		t.Fatal(err)
	}

	prevTTY, prevIn, prevOut := ttyCheck, assistIn, assistOut
	t.Cleanup(func() {
		ttyCheck = prevTTY
		assistIn = prevIn
		assistOut = prevOut
	})
	ttyCheck = func(*os.File) bool { return true }
	assistIn = strings.NewReader("Add a connection\n")
	assistOut = &bytes.Buffer{}

	id, err := Session(SessionOptions{Agent: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	e, err := LoadEntry(id)
	if err != nil {
		t.Fatal(err)
	}
	if e.Meta.Task != modeOpen {
		t.Fatalf("Meta.Task=%q want %q", e.Meta.Task, modeOpen)
	}
	if e.Answers.Task != modeOpen {
		t.Fatalf("Answers.Task=%q want %q", e.Answers.Task, modeOpen)
	}
}

func TestRunOpenScreenUsesReaderWhenStdinSwapped(t *testing.T) {
	prev := assistIn
	t.Cleanup(func() { assistIn = prev })
	assistIn = strings.NewReader("Add a connection\n")

	p := PromptContext{Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}}}
	prevOut := assistOut
	t.Cleanup(func() { assistOut = prevOut })
	buf := &bytes.Buffer{}
	assistOut = buf

	ask, investigate, ok := runOpenScreen(p)
	if !ok || ask != "Add a connection" || investigate {
		t.Fatalf("ok=%v ask=%q investigate=%v", ok, ask, investigate)
	}
	if !strings.Contains(buf.String(), suggestionElseLabel) {
		t.Fatalf("numbered card missing:\n%s", buf.String())
	}
}

func TestPickOpenAskNoOptions(t *testing.T) {
	if ask, _, ok := pickOpenAsk(nil); ok || ask != "" {
		t.Fatalf("ok=%v ask=%q", ok, ask)
	}
}

func landingCtx(kind LandingKind) PromptContext {
	base := PromptContext{
		Version:     "dev",
		Cwd:         "/work",
		ProjectName: "demo",
		ProjectRoot: "/work",
		HasProject:  true,
		FileCounts:  map[string]int{"replications": 2, "pipelines": 1, "models": 3, "specs": 0},
		Connections: []ProbeConn{
			{Name: "MY_PG", Type: "PostgreSQL", Source: "sling env yaml"},
			{Name: "MY_SF", Type: "Snowflake", Source: "sling env yaml"},
			{Name: "LOCAL", Type: "Local File System", Source: "built-in"},
		},
		Route: "default",
	}
	switch kind {
	case LandingFresh:
		base.HasProject = false
		base.ProjectName = ""
		base.ProjectRoot = ""
		base.Connections = nil
		base.FileCounts = map[string]int{"replications": 0, "pipelines": 0, "models": 0, "specs": 0}
	case LandingNoProject:
		base.HasProject = false
		base.ProjectName = ""
		base.ProjectRoot = "/tmp"
		base.Route = "no_project"
		base.FileCounts = map[string]int{"replications": 0, "pipelines": 0, "models": 0, "specs": 0}
	case LandingProject:
		base.RecentRuns = []LocalExec{
			{ID: "exec_old", Status: "err", ConfigPath: "./old.yaml", When: time.Now().Add(-48 * time.Hour)},
			{ID: "exec_new", Status: "ok", ConfigPath: "./r.yaml", When: time.Now().Add(-2 * time.Hour)},
		}
	}
	return base
}

func TestClassifyLanding(t *testing.T) {
	if got := ClassifyLanding(PromptContext{HasProject: true}, true); got != LandingFresh {
		t.Fatalf("fresh wins over project: %s", got)
	}
	if got := ClassifyLanding(PromptContext{HasProject: true}, false); got != LandingProject {
		t.Fatalf("got %s", got)
	}
	if got := ClassifyLanding(PromptContext{}, false); got != LandingNoProject {
		t.Fatalf("got %s", got)
	}
}

func TestSuggestedCommandLadder(t *testing.T) {
	fail := PromptContext{RecentRuns: []LocalExec{{Status: "err"}}, Signature: "97d84811"}
	if got := fail.SuggestedCommand(); got != "sling assist error 97d84811" {
		t.Fatalf("got %q", got)
	}
	failNoSig := PromptContext{RecentRuns: []LocalExec{{Status: "err"}}}
	if got := failNoSig.SuggestedCommand(); got != "sling assist" {
		t.Fatalf("got %q", got)
	}
	zero := PromptContext{Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}}, HasProject: true}
	if got := zero.SuggestedCommand(); got != "sling assist" {
		t.Fatalf("got %q", got)
	}
	noProj := PromptContext{Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}}}
	if got := noProj.SuggestedCommand(); got != "sling init" {
		t.Fatalf("got %q", got)
	}
	def := PromptContext{HasProject: true, Connections: []ProbeConn{{Name: "MY_PG", Source: "sling env yaml"}}}
	if got := def.SuggestedCommand(); got != "sling assist" {
		t.Fatalf("got %q", got)
	}
}

func TestRenderLandingThreeStates(t *testing.T) {
	cases := []struct {
		kind LandingKind
		want []string
		skip []string
	}{
		{
			kind: LandingFresh,
			want: []string{
				"Welcome to sling",
				"sling assist",
				"https://docs.slingdata.io",
			},
			skip: []string{"conns set", "sling init"},
		},
		{
			kind: LandingNoProject,
			want: []string{
				"2 connections configured",
				"sling conns list",
				"sling init",
			},
			skip: []string{"conns set", "Welcome to sling"},
		},
		{
			kind: LandingProject,
			want: []string{
				"On project demo",
				"not linked",
				"replications 2",
				"pipelines 1",
				"models 3",
				"last run: exec_new ./r.yaml [ok]",
				"Next: sling assist",
			},
			skip: []string{"conns set", "Welcome to sling"},
		},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			got := RenderLanding(tc.kind, landingCtx(tc.kind), 80)
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("missing %q\n%s", w, got)
				}
			}
			for _, s := range tc.skip {
				if strings.Contains(got, s) {
					t.Errorf("must not contain %q\n%s", s, got)
				}
			}
			if n := strings.Count(got, "conns set"); n != 0 {
				t.Errorf("conns set leaked")
			}
		})
	}
}

func TestRenderLandingFailedRunSuggestsError(t *testing.T) {
	p := landingCtx(LandingProject)
	p.RecentRuns = []LocalExec{{ID: "exec_fail", Status: "err", ConfigPath: "./r.yaml", When: time.Now()}}
	p.Signature = "97d84811"
	p.Route = "failed_run"
	got := RenderLanding(LandingProject, p, 80)
	if !strings.Contains(got, "Next: sling assist error 97d84811") {
		t.Fatalf("missing error suggestion:\n%s", got)
	}
	if !strings.Contains(got, "last run: exec_fail ./r.yaml [err]") {
		t.Fatalf("missing last run:\n%s", got)
	}
}

func TestRenderLandingZeroConnectionsNoProject(t *testing.T) {
	p := PromptContext{
		Connections: []ProbeConn{{Name: "LOCAL", Source: "built-in"}},
	}
	got := RenderLanding(LandingNoProject, p, 80)
	if !strings.Contains(got, "0 connections configured") {
		t.Fatalf("got:\n%s", got)
	}
	if !strings.Contains(got, "sling init") {
		t.Fatalf("missing project init:\n%s", got)
	}
}

func TestRenderLandingRespectsWidth(t *testing.T) {
	got := RenderLanding(LandingNoProject, landingCtx(LandingNoProject), 40)
	for i, line := range strings.Split(strings.TrimRight(got, "\n"), "\n") {
		if n := utf8.RuneCountInString(line); n > 40 {
			t.Errorf("line %d len %d > 40: %q", i, n, line)
		}
	}
}

func TestFreshLandingPointsOnlyAtAssist(t *testing.T) {
	got := RenderLanding(LandingFresh, PromptContext{}, 80)
	if strings.Count(got, "sling assist") < 3 {
		t.Fatalf("expected 3 assist pointers:\n%s", got)
	}
	if strings.Contains(got, "conns set") {
		t.Fatalf("conns set leaked:\n%s", got)
	}
}

func TestIsFreshInstall(t *testing.T) {
	dir := withTempHomeDir(t)

	if !IsFreshInstall() {
		t.Fatal("empty home should be fresh")
	}

	envPath := filepath.Join(dir, "env.yaml")
	body := "# Environment Credentials for Sling CLI\n# See https://docs.slingdata.io/sling-cli/environment\n\nconnections:\n\n\nvariables:\n"
	if err := os.WriteFile(envPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if !IsFreshInstall() {
		t.Fatal("seeded default env.yaml should be fresh")
	}

	if err := os.WriteFile(envPath, []byte("connections:\n  MY_PG:\n    type: postgres\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if IsFreshInstall() {
		t.Fatal("user connection should not be fresh")
	}

	if err := os.WriteFile(envPath, []byte("connections:\n\nenv:\n  SLING_ASSIST:\n    agent: claude\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if IsFreshInstall() {
		t.Fatal("assist profile should not be fresh")
	}

	if err := os.WriteFile(envPath, []byte("connections:\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	hist := filepath.Join(dir, "assist", "history", "sess1")
	if err := os.MkdirAll(hist, 0o755); err != nil {
		t.Fatal(err)
	}
	if IsFreshInstall() {
		t.Fatal("assist history should not be fresh")
	}
}

func TestLatestRunPicksNewestNotFailure(t *testing.T) {
	oldFail := LocalExec{ID: "old", Status: "err", When: time.Now().Add(-time.Hour)}
	newOK := LocalExec{ID: "new", Status: "ok", When: time.Now()}
	got, ok := latestRun([]LocalExec{oldFail, newOK})
	if !ok || got.ID != "new" {
		t.Fatalf("got %+v ok=%v", got, ok)
	}
}
