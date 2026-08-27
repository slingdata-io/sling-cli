package evals

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/sling/assist"
	"github.com/spf13/cast"
)

// TestEvalAssist is the opt-in eval entry.
//
//	go test -v -run TestEvalAssist -- --arms mock
//	go test -v -run TestEvalAssist -- --tags smoke --trials 1
//	go test -v -run TestEvalAssist -- --baseline results/main-latest.jsonl
//
// Missing arm binary or API key skips that arm. The suite stays green.
func TestEvalAssist(t *testing.T) {
	// Opt-in: only run when the caller passed flags after --.
	if !hasDashDash(os.Args) {
		t.Skip("opt-in: pass flags after -- (e.g. --arms mock)")
	}
	bin, err := FindSlingBin()
	if err != nil {
		t.Skipf("skip TestEvalAssist: %v", err)
	}
	bin, err = EnsureFreshSlingBin(bin)
	if err != nil {
		t.Fatalf("stale sling binary: %v", err)
	}
	t.Logf("sling binary: %s", slingBinaryStamp(bin))

	flags := ParseFlags(os.Args)
	cases, err := LoadCases(casesDir())
	if err != nil {
		t.Fatalf("load cases: %v", err)
	}
	selected := SelectCases(cases, flags)
	if len(selected) == 0 {
		t.Fatalf("no cases selected")
	}

	runID := NewRunID()
	w, err := NewResultsWriter(resultsDir(), runID)
	if err != nil {
		t.Fatalf("results: %v", err)
	}

	s := &Suite{
		Flags:  flags,
		Bin:    bin,
		Writer: w,
		Logf:   t.Logf,
	}
	sum, err := s.RunSuite(selected)
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	sumPath := filepath.Join(resultsDir(), runID+".summary.json")
	if b, err := json.MarshalIndent(sum, "", "  "); err == nil {
		_ = os.WriteFile(sumPath, b, 0o644)
	}
	t.Log("\n" + FormatSummary(*sum))
	t.Logf("jsonl=%s", w.Path())

	// Mock invariants are the local gate.
	if contains(flags.Arms, "mock") {
		assertMockInvariants(t, w.Trials(), selected)
	}

	if sum.Verdict == "fail" && hasLiveArm(flags.Arms) {
		// Live-arm gate only when a real arm actually ran (no skip).
		if liveRan(w.Trials()) {
			t.Errorf("suite verdict=fail (threshold=%.2f)", sum.Threshold)
		}
	}
}

func hasDashDash(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return true
		}
	}
	return false
}

func hasLiveArm(arms []string) bool {
	for _, a := range arms {
		if isLiveArm(a) {
			return true
		}
	}
	return false
}

func liveRan(trials []TrialResult) bool {
	for _, tr := range trials {
		if isLiveArm(tr.Arm) && tr.SkipReason == "" {
			return true
		}
	}
	return false
}

func assertMockInvariants(t *testing.T, trials []TrialResult, cases []Case) {
	t.Helper()
	byCase := map[string][]TrialResult{}
	for _, tr := range trials {
		if tr.Arm != "mock" {
			continue
		}
		byCase[tr.Case] = append(byCase[tr.Case], tr)
	}
	for _, c := range cases {
		trs := byCase[c.ID]
		if len(trs) == 0 {
			t.Errorf("%s: no mock trials", c.ID)
			continue
		}
		var sawPassable, passableOK, sawSens, sensOK bool
		for _, tr := range trs {
			switch tr.Invariant {
			case "passable":
				sawPassable = true
				if tr.Error != "" {
					t.Errorf("%s passable error: %s", c.ID, tr.Error)
					continue
				}
				if !tr.Pass {
					t.Errorf("%s passable failed graders=%s", c.ID, graderDump(tr.Graders))
				} else {
					passableOK = true
				}
			case "sensitivity":
				sawSens = true
				if tr.Error != "" && tr.Error != "no mutants shipped" {
					t.Errorf("%s sensitivity error: %s", c.ID, tr.Error)
					continue
				}
				if tr.Pass {
					sensOK = true
				} else {
					t.Errorf("%s sensitivity did not fail a required grader (mutant is invisible) graders=%s", c.ID, graderDump(tr.Graders))
				}
			}
		}
		if !sawPassable {
			t.Errorf("%s: missing passable trial", c.ID)
		}
		if !sawSens {
			t.Errorf("%s: missing sensitivity trial", c.ID)
		}
		_ = passableOK
		_ = sensOK
	}
}

func graderDump(gs []GraderResult) string {
	var parts []string
	for _, g := range gs {
		mark := "✗"
		if g.Skip {
			mark = "skip"
		} else if g.Pass {
			mark = "✓"
		}
		parts = append(parts, g.Name+"="+mark)
		if g.Detail != "" && mark == "✗" {
			parts = append(parts, "("+g.Detail+")")
		}
	}
	return strings.Join(parts, " ")
}

// TestFixtureSkillChecksum asserts seeded skills match the embedded bundle.
func TestFixtureSkillChecksum(t *testing.T) {
	for _, name := range []string{"home_claude", "home_grok", "home_opencode2", "home_codex"} {
		home := t.TempDir()
		if err := SeedHome(home, name, false); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		root := filepath.Join(home, ".agents", "skills")
		n := 0
		err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return err
			}
			rel, err := filepath.Rel(root, p)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			want, err := assist.MD5OfEmbeddedSkill(rel)
			if err != nil {
				t.Errorf("%s: embedded %s: %v", name, rel, err)
				return nil
			}
			got, err := assist.MD5OfFile(p)
			if err != nil {
				t.Errorf("%s: read %s: %v", name, rel, err)
				return nil
			}
			if got != want {
				t.Errorf("%s: %s md5=%s want embedded %s", name, rel, got, want)
			}
			n++
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Errorf("%s: no skill files seeded", name)
		}
	}
}

// TestHomeWithClaudeSeedExists is the fixture bar when tmux is absent.
func TestHomeWithClaudeSeedExists(t *testing.T) {
	p := filepath.Join(evalsDir(), "../assist/fixtures/home_with_claude")
	ents, err := os.ReadDir(p)
	if err != nil {
		t.Fatalf("home_with_claude missing: %v", err)
	}
	if len(ents) == 0 {
		t.Fatal("home_with_claude is empty")
	}
}

func TestParseFlags(t *testing.T) {
	f := ParseFlags([]string{"go", "test", "--", "--arms", "mock", "--tags", "smoke", "--trials", "1", "--max-suite-usd", "9"})
	if len(f.Arms) != 1 || f.Arms[0] != "mock" {
		t.Fatalf("arms=%v", f.Arms)
	}
	if f.Trials != 1 {
		t.Fatalf("trials=%d", f.Trials)
	}
	if f.MaxSuiteUSD != 9 {
		t.Fatalf("budget=%v", f.MaxSuiteUSD)
	}
	if !contains(f.Tags, "smoke") {
		t.Fatalf("tags=%v", f.Tags)
	}
}

func TestArmSkipWhenUnavailable(t *testing.T) {
	a := CheckArm("claude")
	if a.Skip == "" {
		// Binary + key present on this machine; skip-policy still exists.
		t.Logf("claude available at %s", a.Binary)
		return
	}
	if !strings.Contains(a.Skip, "missing") {
		t.Errorf("skip reason should mention missing: %s", a.Skip)
	}
}

func TestGatingFieldNotTags(t *testing.T) {
	off := false
	on := true
	c := Case{Tier: TierCore, Gating: &off, Tags: []string{"replication"}}
	if c.IsGating() {
		t.Fatal("gating: false must not gate")
	}
	c2 := Case{Tier: TierSmoke, Gating: &on, Tags: []string{}}
	if !c2.IsGating() {
		t.Fatal("gating: true must gate")
	}
	if c.HasTag("smoke") || c2.HasTag("flaky") {
		t.Fatal("tier/gating must not leak into tags")
	}
}

func TestLoadCaseExpectedIsMap(t *testing.T) {
	c, err := loadCase(filepath.Join(casesDir(), "e.01.repl_pg_duckdb_incremental", "case.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Graders.Required) < 5 {
		t.Fatalf("required=%d", len(c.Graders.Required))
	}
	var found bool
	for _, g := range c.Graders.Required {
		if g.kind() != "expected" {
			continue
		}
		found = true
		t.Logf("expected type %T value=%#v", g["expected"], g["expected"])
		if _, ok := asMap(g["expected"]); !ok {
			t.Fatalf("expected is not a map: %T", g["expected"])
		}
	}
	if !found {
		t.Fatal("no expected grader")
	}
}

func TestPrincipalCasesExist(t *testing.T) {
	cases, err := LoadCases(casesDir())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Case{}
	for _, c := range cases {
		got[c.ID] = c
	}
	for _, gone := range []string{
		"e.08.repl_mysql_snapshot",
		"e.19.build_incremental_ref",
		"e.20.build_compile_list",
		"e.21.build_update_materialization",
	} {
		if _, ok := got[gone]; ok {
			t.Errorf("removed case still present: %s", gone)
		}
	}
	if _, ok := got["e.48.cdc_outcome"]; ok {
		t.Error("e.48 must not ship without a wal_level=logical fixture")
	}
	must := []string{
		"e.01.repl_pg_duckdb_incremental",
		"e.09.repl_pg_ch_multistream",
		"e.18.build_staging_view",
		"e.22.spec_simple_rest",
		"e.27.build_tpch_layers",
		"e.33.repl_incremental_outcome",
		"e.38.spec_cursor_stop",
		"e.46.repl_cdc_create",
		"e.49.debug_real_run",
	}
	for _, id := range must {
		if _, ok := got[id]; !ok {
			t.Errorf("missing case %s", id)
		}
	}
	smoke := 0
	for _, c := range got {
		if c.Tier == TierSmoke {
			smoke++
			if !contains(c.Arms, "noskills") {
				t.Errorf("%s smoke case missing noskills arm", c.ID)
			}
		}
		if c.Gating == nil {
			t.Errorf("%s missing gating", c.ID)
		}
		if c.HasTag("smoke") || c.HasTag("flaky") {
			t.Errorf("%s still has smoke/flaky tag", c.ID)
		}
	}
	if smoke < 8 {
		t.Errorf("smoke cases=%d want >=8", smoke)
	}
}

func TestNoSkillsSeedOmitsSkills(t *testing.T) {
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(home, ".agents", "skills")); !os.IsNotExist(err) {
		t.Fatal("noskills seed must omit skill files")
	}
}

func TestTaskIDMapping(t *testing.T) {
	c := Case{Task: "replication_create"}
	if c.TaskID() != "replication.create" {
		t.Fatalf("got %s", c.TaskID())
	}
	c.Task = "replication.create"
	if c.TaskID() != "replication.create" {
		t.Fatalf("got %s", c.TaskID())
	}
	c.Task = "debug"
	if c.TaskID() != "debug" {
		t.Fatalf("got %s", c.TaskID())
	}
}

func TestLinkHostAuthClaude(t *testing.T) {
	host := t.TempDir()
	sandbox := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)

	cred := filepath.Join(host, ".claude", ".credentials.json")
	if err := os.MkdirAll(filepath.Dir(cred), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cred, []byte(`{"mcpOAuth":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	hostJSON := map[string]any{
		"oauthAccount":           map[string]any{"emailAddress": "dev@example.com"},
		"userID":                 "user-1",
		"hasCompletedOnboarding": true,
		"mcpServers":             map[string]any{"other": "no"},
	}
	b, _ := json.Marshal(hostJSON)
	if err := os.WriteFile(filepath.Join(host, ".claude.json"), b, 0o600); err != nil {
		t.Fatal(err)
	}

	destJSON := filepath.Join(sandbox, ".claude.json")
	if err := os.WriteFile(destJSON, []byte(`{"mcpServers":{"sling":{"command":"sling"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := LinkHostAuth(sandbox, "claude"); err != nil {
		t.Fatal(err)
	}

	link := filepath.Join(sandbox, ".claude", ".credentials.json")
	got, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("credentials should be a symlink: %v", err)
	}
	if got != cred {
		t.Fatalf("symlink=%s want %s", got, cred)
	}

	var doc map[string]any
	body, err := os.ReadFile(destJSON)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	oauth, _ := doc["oauthAccount"].(map[string]any)
	if oauth["emailAddress"] != "dev@example.com" {
		t.Fatalf("oauth not merged: %#v", doc["oauthAccount"])
	}
	mcp, _ := doc["mcpServers"].(map[string]any)
	if _, ok := mcp["sling"]; !ok {
		t.Fatalf("fixture MCP was dropped: %#v", doc["mcpServers"])
	}
	if _, ok := mcp["other"]; ok {
		t.Fatal("host MCP must not replace fixture MCP")
	}
}

func TestLinkHostAuthGrok(t *testing.T) {
	host := t.TempDir()
	sandbox := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)

	src := filepath.Join(host, ".grok", "auth.json")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte(`{"token":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := LinkHostAuth(sandbox, "grok"); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(filepath.Join(sandbox, ".grok", "auth.json"))
	if err != nil {
		t.Fatalf("auth.json should be a symlink: %v", err)
	}
	if got != src {
		t.Fatalf("symlink=%s want %s", got, src)
	}
}

func TestLinkHostAuthMissingHostFiles(t *testing.T) {
	t.Setenv("EVAL_HOST_HOME", t.TempDir())
	if err := LinkHostAuth(t.TempDir(), "claude"); err != nil {
		t.Fatal(err)
	}
	if err := LinkHostAuth(t.TempDir(), "grok"); err != nil {
		t.Fatal(err)
	}
}

func TestCheckArmUsesHostLogin(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	t.Setenv("XAI_API_KEY", "")
	t.Setenv("GROK_API_KEY", "")
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)

	if CheckArm("claude").Skip == "" {
		t.Fatal("claude must skip without key or host login")
	}
	if CheckArm("grok").Skip == "" {
		t.Fatal("grok must skip without key or host login")
	}

	if err := os.WriteFile(filepath.Join(host, ".claude.json"), []byte(`{"oauthAccount":{"emailAddress":"dev@example.com"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(host, ".grok"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(host, ".grok", "auth.json"), []byte(`{"token":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ca := CheckArm("claude")
	if _, err := exec.LookPath("claude"); err != nil {
		if !strings.Contains(ca.Skip, "missing binary") {
			t.Fatalf("claude skip=%q", ca.Skip)
		}
	} else if ca.Skip != "" {
		t.Fatalf("host oauth should unlock claude: %s", ca.Skip)
	}

	ga := CheckArm("grok")
	if _, err := exec.LookPath("grok"); err != nil {
		if !strings.Contains(ga.Skip, "missing binary") {
			t.Fatalf("grok skip=%q", ga.Skip)
		}
	} else if ga.Skip != "" {
		t.Fatalf("host auth.json should unlock grok: %s", ga.Skip)
	}
}

func TestAgentEnvClaudeUsesHostHome(t *testing.T) {
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	if err := os.MkdirAll(filepath.Join(host, ".claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	sandbox := t.TempDir()
	work := t.TempDir()
	envv := agentEnv(sandbox, work, "/bin/sling", "claude")
	gotHome := ""
	for _, kv := range envv {
		if strings.HasPrefix(kv, "HOME=") {
			gotHome = strings.TrimPrefix(kv, "HOME=")
		}
		if strings.HasPrefix(kv, "CLAUDE_CONFIG_DIR=") {
			t.Fatalf("CLAUDE_CONFIG_DIR must stay unset so ~/.claude.json is used: %s", kv)
		}
		if strings.HasPrefix(kv, "SLING_HOME_DIR=") {
			if !strings.HasPrefix(strings.TrimPrefix(kv, "SLING_HOME_DIR="), sandbox) {
				t.Fatalf("SLING_HOME_DIR left the sandbox: %s", kv)
			}
		}
	}
	if gotHome != host {
		t.Fatalf("claude HOME=%s want host %s", gotHome, host)
	}
}

func TestAgentEnvGrokKeepsSandboxHome(t *testing.T) {
	sandbox := t.TempDir()
	envv := agentEnv(sandbox, t.TempDir(), "/bin/sling", "grok")
	for _, kv := range envv {
		if strings.HasPrefix(kv, "HOME=") && strings.TrimPrefix(kv, "HOME=") != sandbox {
			t.Fatalf("grok must keep sandbox HOME, got %s", kv)
		}
	}
}

func TestAgentEvalArgsAllowsInspectTools(t *testing.T) {
	args := agentEvalArgs("claude", "/tmp/prompt.md", "/tmp/work", 0)
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "Glob") || !strings.Contains(joined, "Grep") {
		t.Fatalf("missing Glob/Grep: %v", args)
	}
	if !strings.Contains(joined, "--mcp-config") || !strings.Contains(joined, "--strict-mcp-config") {
		t.Fatalf("claude must pin trial MCP: %v", args)
	}
	if !strings.Contains(joined, evalAgentSystemPrompt[:40]) {
		t.Fatal("missing eval system prompt")
	}
	gargs := agentEvalArgs("grok", "/tmp/prompt.md", "/tmp/work", 0)
	gjoin := strings.Join(gargs, " ")
	if !strings.Contains(gjoin, "--prompt-file") || !strings.Contains(gjoin, "--always-approve") {
		t.Fatalf("grok args=%v", gargs)
	}
	if len(gargs) > 0 && gargs[0] == "-p" {
		t.Fatal("grok -p consumes --prompt-file as the prompt text")
	}
}

func TestHostHasAuthEnvStillCounts(t *testing.T) {
	t.Setenv("EVAL_HOST_HOME", t.TempDir())
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("XAI_API_KEY", "xai-test")
	if !HostHasClaudeAuth() {
		t.Fatal("ANTHROPIC_API_KEY must count")
	}
	if !HostHasGrokAuth() {
		t.Fatal("XAI_API_KEY must count")
	}
}

func TestCompareMustMatchMode(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
defaults:
  mode: incremental
  primary_key: [id]
  update_key: updated_at
  object: public.orders
streams:
  public.orders:
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    primary_key: [id]
    update_key: updated_at
    object: public.orders
`)
	res := CompareCompiled(actual, expected, MatchPolicy{
		MustMatch: []string{"source", "target", "streams.*.mode", "streams.*.primary_key", "streams.*.update_key", "streams.*.object"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
}

func TestCompareMustNotExistHooks(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: full-refresh
    object: public.orders
`)
	bad := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
hooks:
  start:
    - type: query
      query: select 1
streams:
  public.orders:
    mode: full-refresh
    object: public.orders
    transforms:
      - upper
`)
	res := CompareCompiled(bad, expected, MatchPolicy{
		MustNotExist: []string{"hooks", "streams.*.transforms"},
	})
	fails := 0
	for _, r := range res {
		if !r.Pass {
			fails++
		}
	}
	if fails < 1 {
		t.Fatal("expected hooks/transforms to fail must_not_exist")
	}
}

func TestComparePrimaryKeyAsSet(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    primary_key: [id, tenant_id]
    object: public.orders
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    primary_key: [tenant_id, id]
    object: public.orders
`)
	res := CompareCompiled(actual, expected, MatchPolicy{MustMatch: []string{"streams.*.primary_key"}})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("set compare failed: %s", r.Message)
		}
	}
}

func TestCompareOrderedSelectMismatch(t *testing.T) {
	// select is a set key — order should not matter. Use object (string) for ordered.
	expected := mustCompileYAML(t, `
source: POSTGRES
target: LOCAL
streams:
  public.orders:
    mode: full-refresh
    object: /tmp/a.csv
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: LOCAL
streams:
  public.orders:
    mode: full-refresh
    object: /tmp/b.csv
`)
	res := CompareCompiled(actual, expected, MatchPolicy{MustMatch: []string{"streams.*.object"}})
	ok := false
	for _, r := range res {
		if !r.Pass && strings.Contains(r.Message, "mismatch") {
			ok = true
		}
	}
	if !ok {
		t.Fatal("expected object mismatch")
	}
}

func TestCompareMismatchMessageNamesPath(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    object: public.orders
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: full-refresh
    object: public.orders
`)
	res := CompareCompiled(actual, expected, MatchPolicy{MustMatch: []string{"streams.*.mode"}})
	found := false
	for _, r := range res {
		if !r.Pass && strings.Contains(r.Path, "mode") {
			found = true
			if !strings.Contains(r.Message, "incremental") && r.Want != "incremental" {
				t.Errorf("message should show both values: %s want=%v got=%v", r.Message, r.Want, r.Got)
			}
		}
	}
	if !found {
		t.Fatal("expected mode mismatch")
	}
}

func TestCompareStreamNameVariance(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    object: public.orders
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: DUCKDB
streams:
  orders:
    mode: incremental
    object: public.orders
`)
	res := CompareCompiled(actual, expected, MatchPolicy{MustMatch: []string{"streams.*.mode", "streams.*.object"}})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("singleton/object align failed: %s %s", r.Path, r.Message)
		}
	}
}

func TestE09ExpectedSelfCompare(t *testing.T) {
	p := filepath.Join(casesDir(), "e.09.repl_pg_ch_multistream", "expected.yaml")
	m, err := CompileReplicationMap(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("compiled=%s", stringify(m))
	res := CompareCompiled(m, m, MatchPolicy{
		MustMatch: []string{"streams.*.mode", "streams.*.object"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
}

func TestCompareEndpointSyncAnyName(t *testing.T) {
	want := map[string]any{
		"endpoints": map[string]any{
			"items": map[string]any{"sync": []any{"updated_since"}},
		},
	}
	got := map[string]any{
		"endpoints": map[string]any{
			"orders": map[string]any{"sync": []any{"updated_since"}},
		},
	}
	res := CompareCompiled(got, want, MatchPolicy{MustMatch: []string{"endpoints.*.sync"}})
	for _, r := range res {
		if !r.Pass {
			t.Fatalf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
	nosync := map[string]any{"endpoints": map[string]any{"items": map[string]any{}}}
	bad := CompareCompiled(nosync, want, MatchPolicy{MustMatch: []string{"endpoints.*.sync"}})
	failed := false
	for _, r := range bad {
		if !r.Pass {
			failed = true
		}
	}
	if !failed {
		t.Fatal("missing sync must fail")
	}
}

func TestCompareSelectAllowsExclude(t *testing.T) {
	expected := mustCompileYAML(t, `
source: MYSQL
target: POSTGRES
streams:
  mysql.users:
    mode: full-refresh
    object: public.users
    select: [id, email, name]
`)
	actual := mustCompileYAML(t, `
source: MYSQL
target: POSTGRES
streams:
  mysql.users:
    mode: full-refresh
    object: public.users
    select: [id, email, name, "-password"]
`)
	res := CompareCompiled(actual, expected, MatchPolicy{
		MustMatch: []string{"streams.*.select"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
}

func TestCompareDefaultsFoldIntoWildcardStreams(t *testing.T) {
	// Agent wrote defaults.mode + finance.* (no per-stream mode). Expected
	// lists expanded streams with mode full-refresh. Compiled compare must
	// fold defaults so streams.*.mode matches (H7 / e.09).
	expected := mustCompileYAML(t, `
source: POSTGRES
target: CLICKHOUSE
defaults:
  mode: full-refresh
  object: public.{stream_table}
streams:
  finance.invoices:
    mode: full-refresh
    object: public.invoices
  finance.payments:
    mode: full-refresh
    object: public.payments
  public.products:
    mode: snapshot
    object: public.products
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: CLICKHOUSE
defaults:
  mode: full-refresh
  object: public.{stream_table}
streams:
  finance.*:
  public.products:
    mode: snapshot
`)
	res := CompareCompiled(actual, expected, MatchPolicy{
		MustMatch: []string{"source", "target", "streams.*.mode"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
}

func TestCompareGlobStreamNameMatchesExpanded(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: CLICKHOUSE
defaults:
  mode: full-refresh
  object: public.{stream_table}
streams:
  finance.invoices: {}
  finance.payments: {}
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: CLICKHOUSE
defaults:
  mode: full-refresh
  object: public.{stream_table}
streams:
  finance.*: {}
`)
	res := CompareCompiled(actual, expected, MatchPolicy{
		MustMatch: []string{"source", "target", "streams.*.mode"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
	mutant := mustCompileYAML(t, `
source: POSTGRES
target: CLICKHOUSE
streams:
  finance.invoices:
    mode: incremental
    object: public.invoices
`)
	bad := CompareCompiled(mutant, expected, MatchPolicy{
		MustMatch: []string{"streams.*.mode"},
	})
	var failed bool
	for _, r := range bad {
		if !r.Pass {
			failed = true
		}
	}
	if !failed {
		t.Fatal("incremental mutant must fail streams.*.mode")
	}
}

func mustCompileYAML(t *testing.T, body string) map[string]any {
	t.Helper()
	m, err := CompileReplicationYAML([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestGraderFileExistsAndAbsent(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "out.yaml")
	if err := os.WriteFile(good, []byte("source: X\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "out.yaml"}
	r := gradeFileExists(ctx, "{artifact}")
	if !r.Pass {
		t.Fatalf("exists: %s", r.Detail)
	}
	r = gradeFileAbsent(ctx, "missing.yaml")
	if !r.Pass {
		t.Fatalf("absent: %s", r.Detail)
	}
	r = gradeFileAbsent(ctx, "out.yaml")
	if r.Pass {
		t.Fatal("file_absent should fail when file exists")
	}
}

func TestGraderYAMLValid(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ok.yaml"), []byte("a: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte(":\n  - [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir}
	if r := gradeYAMLValid(ctx, "ok.yaml"); !r.Pass {
		t.Fatalf("ok: %s", r.Detail)
	}
	if r := gradeYAMLValid(ctx, "bad.yaml"); r.Pass {
		t.Fatal("bad yaml should fail")
	}
}

func TestGraderReplicationParseCompile(t *testing.T) {
	dir := t.TempDir()
	body := []byte(`
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    object: public.orders
    primary_key: [id]
    update_key: updated_at
`)
	if err := os.WriteFile(filepath.Join(dir, "ok.yaml"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("not: a: replication\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "ok.yaml"}
	if r := gradeSling(ctx, "replication parse {artifact}"); !r.Pass {
		t.Fatalf("parse: %s", r.Detail)
	}
	if r := gradeSling(ctx, "replication compile {artifact}"); !r.Pass {
		t.Fatalf("compile: %s", r.Detail)
	}
	ctx.Artifact = "bad.yaml"
	if r := gradeSling(ctx, "replication parse {artifact}"); r.Pass {
		t.Fatal("bad parse should fail")
	}
}

func TestGraderExpectedGoodAndBad(t *testing.T) {
	dir := t.TempDir()
	caseDir := t.TempDir()
	expected := `
source: POSTGRES
target: DUCKDB
streams:
  public.orders:
    mode: incremental
    object: public.orders
    primary_key: [id]
    update_key: updated_at
`
	if err := os.WriteFile(filepath.Join(caseDir, "expected.yaml"), []byte(expected), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "out.yaml"), []byte(expected), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "out.yaml", CaseDir: caseDir}
	spec := map[string]any{
		"file":           "expected.yaml",
		"compare":        "compiled",
		"must_match":     []any{"source", "target", "streams.*.mode", "streams.*.primary_key"},
		"must_not_exist": []any{"hooks"},
	}
	if r := gradeExpected(ctx, spec); !r.Pass {
		t.Fatalf("good expected: %s", r.Detail)
	}
	mut := `
source: POSTGRES
target: DUCKDB
hooks:
  start: [{type: query, query: select 1}]
streams:
  public.orders:
    mode: full-refresh
    object: public.orders
`
	if err := os.WriteFile(filepath.Join(dir, "out.yaml"), []byte(mut), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := gradeExpected(ctx, spec); r.Pass {
		t.Fatal("mutant should fail expected")
	}
}

func TestGraderTranscript(t *testing.T) {
	ctx := GradeContext{Transcript: "connection WAREHOUSE_PROD does not exist"}
	if r := gradeTranscriptContains(ctx, "WAREHOUSE_PROD"); !r.Pass {
		t.Fatal(r.Detail)
	}
	if r := gradeTranscriptContains(ctx, "nope"); r.Pass {
		t.Fatal("should fail")
	}
	if r := gradeTranscriptAbsent(ctx, "sling run"); !r.Pass {
		t.Fatal(r.Detail)
	}
	ctx.Transcript = "I will sling run -r out.yaml now"
	if r := gradeTranscriptAbsent(ctx, "sling run"); r.Pass {
		t.Fatal("should detect run")
	}
}

func TestTranscriptAbsentIgnoresProse(t *testing.T) {
	// e.49-style: the agent names the forbidden command in prose.
	prose := `{"type":"text","text":"sling run --parse is a sling command the Ask told me not to run"}`
	ctx := GradeContext{Transcript: prose}
	if r := gradeTranscriptAbsent(ctx, "sling run"); !r.Pass {
		t.Fatalf("prose must not fail transcript_absent: %s", r.Detail)
	}
	if r := gradeTranscriptAbsentRaw(ctx, "sling run"); r.Pass {
		t.Fatal("raw grep should still see the prose")
	}
	tool := `{"type":"tool_use","name":"Bash","input":{"command":"sling run -r bad.yaml"}}`
	ctx.Transcript = tool
	if r := gradeTranscriptAbsent(ctx, "sling run"); r.Pass {
		t.Fatal("executed Bash command must fail transcript_absent")
	}
}

func TestGraderSkeleton(t *testing.T) {
	dir := t.TempDir()
	good := `
steps:
  - type: query
    connection: POSTGRES
    query: select 1
  - type: check
    check: true
`
	bad := `
steps:
  - type: delete
    location: LOCAL/x
  - type: query
    connection: POSTGRES
    query: select 1
`
	if err := os.WriteFile(filepath.Join(dir, "ok.yaml"), []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "ok.yaml"}
	skel := map[string]any{
		"must_contain_steps": []any{
			map[string]any{"type": "query", "connection": "POSTGRES"},
			map[string]any{"type": "check"},
		},
		"must_not_contain_steps": []any{
			map[string]any{"type": "delete"},
		},
	}
	if r := gradeSkeleton(ctx, skel); !r.Pass {
		t.Fatalf("good skeleton: %s", r.Detail)
	}
	if err := os.WriteFile(filepath.Join(dir, "ok.yaml"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := gradeSkeleton(ctx, skel); r.Pass {
		t.Fatal("delete step should fail")
	}
}

func TestGraderSkeletonFindsNestedCopy(t *testing.T) {
	dir := t.TempDir()
	body := `
steps:
  - type: list
    location: data/*.csv
  - type: group
    loop: state.list.result
    steps:
      - type: copy
        from: "{loop.value.path}"
        to: AWS_S3_TEST/out/
`
	if err := os.WriteFile(filepath.Join(dir, "p.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "p.yaml"}
	skel := map[string]any{
		"must_contain_steps": []any{
			map[string]any{"type": "list"},
			map[string]any{"type": "copy"},
		},
	}
	if r := gradeSkeleton(ctx, skel); !r.Pass {
		t.Fatalf("nested copy should match: %s", r.Detail)
	}
}

func TestGraderYQ(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "out.yaml"), []byte("streams:\n  a: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "out.yaml"}
	if r := gradeYQ(ctx, ".streams | length == 1"); !r.Pass {
		t.Fatal(r.Detail)
	}
	if r := gradeYQ(ctx, ".streams | length == 2"); r.Pass {
		t.Fatal("should fail length")
	}
}

func TestGraderSQLFallback(t *testing.T) {
	dir := t.TempDir()
	caseDir := t.TempDir()
	art := `
source: POSTGRES
target: DUCKDB
streams:
  custom_orders:
    object: public.custom_orders
    sql: select o.id from public.orders o join public.customers c on c.id = o.customer_id
`
	if err := os.WriteFile(filepath.Join(dir, "out.yaml"), []byte(art), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(caseDir, "expected.sql"), []byte("select o.id from public.orders o join public.customers c on c.id = o.customer_id"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "out.yaml", CaseDir: caseDir, ConnDown: map[string]bool{"POSTGRES": true}}
	spec := map[string]any{
		"connection":   "POSTGRES",
		"actual_path":  "streams.custom_orders.sql",
		"expected_sql": "expected.sql",
		"compare":      "rows",
	}
	if r := gradeSQLEquiv(ctx, spec); !r.Pass {
		t.Fatalf("fallback should pass: %s", r.Detail)
	}
	// Missing table ref.
	if err := os.WriteFile(filepath.Join(dir, "out.yaml"), []byte(`
source: POSTGRES
target: DUCKDB
streams:
  custom_orders:
    object: public.custom_orders
    sql: select 1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := gradeSQLEquiv(ctx, spec); r.Pass {
		t.Fatal("fallback should fail when refs missing")
	}
}

func TestGraderAPISpecTestHitsEndpoints(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]}`))
	}))
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	good := []byte(fmt.Sprintf(`
name: eval_live
endpoints:
  users:
    request:
      url: %s/users
      method: GET
    response:
      records:
        jmespath: "data"
        primary_key: [id]
`, srv.URL))
	if err := os.WriteFile(filepath.Join(dir, "good.yaml"), good, 0o644); err != nil {
		t.Fatal(err)
	}
	dead := []byte(`
name: eval_dead
endpoints:
  users:
    request:
      url: http://127.0.0.1:1/users
      method: GET
    response:
      records:
        jmespath: "data"
        primary_key: [id]
`)
	if err := os.WriteFile(filepath.Join(dir, "dead.yaml"), dead, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := GradeContext{WorkDir: dir, Artifact: "good.yaml"}
	if r := gradeSling(ctx, "api_spec parse {artifact}"); !r.Pass || r.Skip {
		t.Fatalf("parse good: %+v", r)
	}
	if r := gradeSling(ctx, "api_spec test {artifact}"); !r.Pass || r.Skip {
		t.Fatalf("test good must hit endpoint and return records: %+v", r)
	}
	if !strings.Contains(gradeSling(ctx, "api_spec test {artifact}").Detail, "records") {
		t.Fatal("test detail should report record count")
	}

	ctx.Artifact = "dead.yaml"
	if r := gradeSling(ctx, "api_spec parse {artifact}"); !r.Pass {
		t.Fatalf("parse dead should still pass: %+v", r)
	}
	if r := gradeSling(ctx, "api_spec test {artifact}"); r.Pass || r.Skip {
		t.Fatalf("test dead must fail (not rubber-stamp parse): %+v", r)
	}

	ctx.SkipExecute = true
	if r := gradeSling(ctx, "api_spec test {artifact}"); !r.Skip {
		t.Fatalf("skip execute: %+v", r)
	}
	ctx.SkipExecute = false
	ctx.ConnDown = map[string]bool{"DUMMYJSON": true}
	if r := gradeSling(ctx, "api_spec test {artifact}"); !r.Skip {
		t.Fatalf("lane-b conn down should skip: %+v", r)
	}
	ctx.ConnDown = map[string]bool{"MOCK_API": true}
	ctx.Artifact = "dead.yaml"
	if r := gradeSling(ctx, "api_spec test {artifact}"); r.Skip {
		t.Fatalf("MOCK_API down must fail, not skip: %+v", r)
	}
}

func TestWeightedScoreAndTrialPass(t *testing.T) {
	gs := []GraderResult{
		{Name: "file_exists", Pass: true},
		{Name: "expected", Pass: true},
		{Name: "yq", Pass: false, Optional: true},
		{Name: "judge:x", Pass: false, Judge: true},
	}
	if !TrialPasses(gs) {
		t.Fatal("required passed so trial should pass")
	}
	s := WeightedScore(gs)
	// 1+1+0+0 / 1+1+0.5+0.5 = 2/3
	if s < 0.66 || s > 0.67 {
		t.Fatalf("score=%v", s)
	}
	gs[1].Pass = false
	if TrialPasses(gs) {
		t.Fatal("required fail should fail trial")
	}
}

func TestSkippedRequiredDoesNotFailTrial(t *testing.T) {
	gs := []GraderResult{
		{Name: "file_exists", Pass: true},
		{Name: "dry_run", Pass: true, Skip: true, Detail: "SKIPPED (conn down: POSTGRES)"},
	}
	if !TrialPasses(gs) {
		t.Fatal("skipped required should not fail")
	}
}

func TestParseJudgeOutput(t *testing.T) {
	raw := `here is json [{"question":"Does the config do everything?","verdict":"pass","critique":"matches intention"}] done`
	got := parseJudgeOutput([]string{"Does the config do everything?"}, raw)
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	if !got[0].Judge || !got[0].Pass {
		t.Fatalf("%+v", got[0])
	}
	if got[0].Critique == "" {
		t.Fatal("missing critique")
	}
}

func TestFirstScalarSkipsCSVHeader(t *testing.T) {
	if got := firstScalar("?column?\ntrue\n"); got != true && cast.ToString(got) != "true" {
		t.Fatalf("want true, got %#v", got)
	}
	if got := firstScalar("cast_min_order_date__as_varchar\n1992-01-01\n"); cast.ToString(got) != "1992-01-01" {
		t.Fatalf("want 1992-01-01, got %#v", got)
	}
	if got := firstScalar("count_star()\n1\n"); toFloatMust(got) != 1 {
		t.Fatalf("want 1, got %#v", got)
	}
}

func toFloatMust(v any) float64 {
	n, _ := toFloat(v)
	return n
}

func TestValuesEqualDefaultMethodAndAliases(t *testing.T) {
	if ok, _ := valuesEqual(nil, "GET", false); !ok {
		t.Fatal("omitted method must equal GET")
	}
	if ok, _ := valuesEqual("Mock API", "MOCK_API", false); !ok {
		t.Fatal("spec name must be case/space insensitive")
	}
	if ok, _ := valuesEqual("full-refresh", "table", false); !ok {
		t.Fatal("table and full-refresh are aliases")
	}
}

func TestLooksLikeInfra(t *testing.T) {
	if !looksLikeInfra("Conflicting lock is held in eval-home") {
		t.Fatal("lock should be infra")
	}
	if looksLikeInfra("relation public.users does not exist") {
		t.Fatal("missing table is not infra")
	}
	if !looksLikeInfra("fork/exec duckdb: malformed Mach-o file") {
		t.Fatal("corrupt engine binary should be infra")
	}
}

func TestNeedsSlingStateCDCOnly(t *testing.T) {
	if !needsSlingState(Case{Tags: []string{"cdc"}}) {
		t.Fatal("cdc tag must opt in")
	}
	if needsSlingState(Case{Tags: []string{"replication"}}) {
		t.Fatal("non-cdc cases must not get SLING_STATE")
	}
	envv := withCaseEnv(nil, t.TempDir(), Case{Tags: []string{"replication"}})
	if hasEnvKey(envv, "SLING_STATE") {
		t.Fatal("SLING_STATE leaked onto a non-cdc case")
	}
	envv = withCaseEnv(nil, t.TempDir(), Case{Tags: []string{"cdc"}})
	if !hasEnvKey(envv, "SLING_STATE") {
		t.Fatal("cdc case missing SLING_STATE")
	}
	for _, kv := range envv {
		if strings.HasPrefix(kv, "SLING_STATE=") && strings.Contains(kv, "file://") {
			t.Fatalf("SLING_STATE must be LOCAL/path, got %s", kv)
		}
	}
}

func TestArmTimeoutMultiplier(t *testing.T) {
	if armTimeoutMultiplier("opencode2") != 2 {
		t.Fatal("opencode2 should be 2x")
	}
	if armTimeoutMultiplier("codex") != 2 {
		t.Fatal("codex should be 2x")
	}
	if armTimeout("opencode2", 3*time.Minute) != 6*time.Minute {
		t.Fatal(armTimeout("opencode2", 3*time.Minute))
	}
	if armTimeout("codex", 3*time.Minute) != 6*time.Minute {
		t.Fatal(armTimeout("codex", 3*time.Minute))
	}
	if armTimeout("claude", 3*time.Minute) != 3*time.Minute {
		t.Fatal("claude stays 1x")
	}
}

func TestTimeoutExcludedFromPassRate(t *testing.T) {
	trials := []TrialResult{
		{Case: "e.13", Arm: "opencode2", Trial: 1, Pass: false, Timeout: true, Score: 0},
	}
	m := AggregateCase("e.13", "opencode2", trials, true)
	if m.Trials != 0 || m.Timeouts != 1 || m.SkipReason != "timeout" {
		t.Fatalf("%+v", m)
	}
}

func TestPersistTrialWork(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "out.yaml"), []byte("source: PG\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = os.MkdirAll(filepath.Join(src, ".agents"), 0o755)
	_ = os.WriteFile(filepath.Join(src, ".agents", "skip.md"), []byte("no"), 0o644)
	persistTrialWork(dst, src)
	if _, err := os.Stat(filepath.Join(dst, "out.yaml")); err != nil {
		t.Fatal("yaml should be copied")
	}
	if _, err := os.Stat(filepath.Join(dst, ".agents", "skip.md")); err == nil {
		t.Fatal("skill files must not be copied")
	}
}

func TestParseJudgeOutputClaudeEnvelope(t *testing.T) {
	inner := `[{"question":"Does the config do everything the intention asked, and nothing more?","verdict":"pass","critique":"ok"}]`
	raw, _ := json.Marshal(map[string]any{
		"type":           "result",
		"result":         inner,
		"total_cost_usd": 0.01,
	})
	got := parseJudgeOutput([]string{"Does the config do everything the intention asked, and nothing more?"}, string(raw))
	if len(got) != 1 || got[0].Skip || !got[0].Pass {
		t.Fatalf("envelope parse failed: %+v", got)
	}
}

func TestJudgeNeverGates(t *testing.T) {
	gs := []GraderResult{
		{Name: "file_exists", Pass: true},
		{Name: "judge:x", Pass: false, Judge: true, Critique: "no"},
	}
	if !TrialPasses(gs) {
		t.Fatal("judge fail must not fail the trial")
	}
}

func TestSkipJudgeWhenUnavailable(t *testing.T) {
	got := skipJudge([]string{"q1", "q2"}, "missing binary: claude")
	if len(got) != 2 {
		t.Fatal(got)
	}
	for _, g := range got {
		if !g.Skip || !g.Judge {
			t.Fatalf("%+v", g)
		}
	}
}

func TestPassAtKAndPassHatK(t *testing.T) {
	trials := []TrialResult{
		{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1},
		{Case: "e.01", Arm: "mock", Trial: 2, Pass: false, Score: 0.2},
		{Case: "e.01", Arm: "mock", Trial: 3, Pass: false, Score: 0.1},
	}
	m := AggregateCase("e.01", "mock", trials, true)
	if !m.PassAtK {
		t.Fatal("pass@k should be true")
	}
	if m.PassHatK {
		t.Fatal("pass^k should be false")
	}
	if m.Passed != 1 || m.Trials != 3 {
		t.Fatalf("passed=%d trials=%d", m.Passed, m.Trials)
	}

	all := []TrialResult{
		{Case: "e.02", Arm: "mock", Trial: 1, Pass: true, Score: 1},
		{Case: "e.02", Arm: "mock", Trial: 2, Pass: true, Score: 1},
	}
	m = AggregateCase("e.02", "mock", all, true)
	if !m.PassHatK {
		t.Fatal("all passed → pass^k")
	}
}

func TestPairedDiffFlips(t *testing.T) {
	base := []TrialResult{
		{Case: "e.01", Arm: "claude", Trial: 1, Pass: true, Score: 0.9},
		{Case: "e.02", Arm: "claude", Trial: 1, Pass: false, Score: 0.2},
		{Case: "e.03", Arm: "claude", Trial: 1, Pass: true, Score: 0.8},
	}
	cur := []TrialResult{
		{Case: "e.01", Arm: "claude", Trial: 1, Pass: false, Score: 0.3}, // pass→fail
		{Case: "e.02", Arm: "claude", Trial: 1, Pass: true, Score: 0.9},  // fail→pass
		{Case: "e.03", Arm: "claude", Trial: 1, Pass: true, Score: 0.7},  // stable
	}
	flips := PairedDiff(cur, base, map[string]bool{"e.01": true, "e.02": true, "e.03": true})
	if len(flips) != 2 {
		t.Fatalf("flips=%d %+v", len(flips), flips)
	}
	var pf, fp bool
	for _, f := range flips {
		if f.Case == "e.01" && f.From == "pass" && f.To == "fail" {
			pf = true
		}
		if f.Case == "e.02" && f.From == "fail" && f.To == "pass" {
			fp = true
		}
	}
	if !pf || !fp {
		t.Fatalf("missing transitions: %+v", flips)
	}
	if !SmokeFlipFail(flips, map[string]bool{"e.01": true}) {
		t.Fatal("smoke pass→fail should fail the gate")
	}
}

func TestJSONLRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := NewResultsWriter(dir, "testrun")
	if err != nil {
		t.Fatal(err)
	}
	tr := TrialResult{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1, Graders: []GraderResult{{Name: "x", Pass: true}}}
	if err := w.Append(tr); err != nil {
		t.Fatal(err)
	}
	got, err := LoadJSONL(w.Path())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Case != "e.01" || !got[0].Pass {
		t.Fatalf("roundtrip: %+v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "testrun.jsonl")); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateSuiteThreshold(t *testing.T) {
	trials := []TrialResult{
		{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1},
		{Case: "e.02", Arm: "mock", Trial: 1, Pass: false, Score: 0},
	}
	sum := AggregateSuite("r", trials, map[string]bool{"e.01": true, "e.02": true}, 0.90)
	if sum.Verdict != "fail" {
		t.Fatalf("verdict=%s", sum.Verdict)
	}
	sum = AggregateSuite("r", trials[:1], map[string]bool{"e.01": true}, 0.90)
	if sum.Verdict != "pass" {
		t.Fatalf("verdict=%s", sum.Verdict)
	}
}

func TestBaselineJSONLFixtures(t *testing.T) {
	base, err := LoadJSONL(filepath.Join(evalsDir(), "testdata", "baseline_a.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	cur, err := LoadJSONL(filepath.Join(evalsDir(), "testdata", "baseline_b.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	flips := PairedDiff(cur, base, map[string]bool{
		"e.01.repl_pg_duckdb_incremental": true,
		"e.11.repl_unknown_conn":          true,
	})
	if len(flips) != 1 {
		t.Fatalf("flips=%+v", flips)
	}
	if flips[0].Case != "e.01.repl_pg_duckdb_incremental" || flips[0].From != "pass" || flips[0].To != "fail" {
		t.Fatalf("%+v", flips[0])
	}
	if !SmokeFlipFail(flips, map[string]bool{"e.01.repl_pg_duckdb_incremental": true}) {
		t.Fatal("smoke flip should fail gate")
	}
}

func TestGatingFalseExcludedFromGate(t *testing.T) {
	trials := []TrialResult{
		{Case: "e.report", Arm: "mock", Trial: 1, Pass: false, Score: 0},
		{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1},
	}
	sum := AggregateSuite("r", trials, map[string]bool{"e.report": false, "e.01": true}, 0.90)
	if sum.Verdict != "pass" {
		t.Fatalf("gating=false fail must not drop verdict: %s", sum.Verdict)
	}
	if sum.GatingN != 1 || sum.GatingPass != 1 || sum.GatingRate != 1 {
		t.Fatalf("gating math=%+v", sum)
	}
	if _, ok := sum.TierRates["core"]; ok {
		t.Fatal("non-gating fail must not move tier_rates")
	}
	out := FormatSummary(sum)
	if !strings.Contains(out, "gating=1/1") {
		t.Fatalf("summary missing gating-only rate:\n%s", out)
	}
}

func TestCompareSelectExclusionOnly(t *testing.T) {
	expected := mustCompileYAML(t, `
source: POSTGRES
target: LOCAL
streams:
  eval_ecom.raw_customers:
    mode: full-refresh
    object: eval_ecom.raw_customers
    select: [customer_id, email, country_code]
`)
	actual := mustCompileYAML(t, `
source: POSTGRES
target: LOCAL
streams:
  eval_ecom.raw_customers:
    mode: full-refresh
    object: eval_ecom.raw_customers
    select: ["-address"]
`)
	res := CompareCompiled(actual, expected, MatchPolicy{
		MustMatch: []string{"streams.*.select"},
	})
	for _, r := range res {
		if !r.Pass {
			t.Errorf("%s: %s want=%v got=%v", r.Path, r.Message, r.Want, r.Got)
		}
	}
}

func TestParseJudgeOutputStructuredOutput(t *testing.T) {
	raw := `{"type":"result","result":"{\"result\":[{\"question\":\"Does the config do everything the intention asked, and nothing more?\",\"verdict\":\"pass\",\"critique\":\"ok\"}]}","structured_output":{"result":[{"question":"Does the config do everything the intention asked, and nothing more?","verdict":"pass","critique":"ok"}]}}`
	got := parseJudgeOutput([]string{"Does the config do everything the intention asked, and nothing more?"}, raw)
	if len(got) != 1 || got[0].Skip || !got[0].Pass {
		t.Fatalf("structured_output parse failed: %+v", got)
	}
}

func TestParseJudgeOutputLiveFixture(t *testing.T) {
	p := filepath.Join(evalsDir(), "testdata", "judge_response_sonnet.json")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Skip(err)
	}
	got := parseJudgeOutput([]string{"Did the agent ask one focused question instead of guessing a connection?"}, string(b))
	if len(got) != 1 || got[0].Skip || !got[0].Pass {
		t.Fatalf("live fixture parse failed: %+v", got)
	}
}

func TestDefaultTrialCountIsTwo(t *testing.T) {
	if (Case{}).TrialCount(0) != 2 {
		t.Fatal("default trials must be 2")
	}
	if (Case{Trials: 3}).TrialCount(0) != 3 {
		t.Fatal("explicit trials must win")
	}
}

func TestSeedHomeOverlaysHostConns(t *testing.T) {
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	if err := os.MkdirAll(filepath.Join(host, ".sling"), 0o755); err != nil {
		t.Fatal(err)
	}
	hostEnv := []byte("connections:\n  POSTGRES:\n    type: postgres\n    url: postgresql://host/db\n  DUCKDB:\n    type: duckdb\n    instance: /tmp/host.duckdb\n")
	if err := os.WriteFile(filepath.Join(host, ".sling", "env.yaml"), hostEnv, 0o644); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	if err := SeedHome(home, "home_claude", false); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(home, ".sling", "env.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yamlUnmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	conns, ok := asMap(doc["connections"])
	if !ok {
		t.Fatalf("connections missing: %s", body)
	}
	pg, _ := asMap(conns["POSTGRES"])
	if castToString(pg["url"]) != "postgresql://host/db" {
		t.Fatalf("POSTGRES not overlaid: %#v", conns["POSTGRES"])
	}
	dd, _ := asMap(conns["DUCKDB"])
	if inst := castToString(dd["instance"]); inst != "eval.duckdb" {
		t.Fatalf("DUCKDB must stay isolated, got %q", inst)
	}
}

func TestRenderPromptPreservesCommas(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skipf("need sling binary: %v", err)
	}
	work := t.TempDir()
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", false); err != nil {
		t.Fatal(err)
	}
	envv := trialEnv(home, work, bin)
	c := Case{
		Task: "replication.create",
		Intention: "Create a replication from MYSQL to POSTGRES for mysql.users. " +
			"Select id, email, name. Backfill range 2024-01-01,2024-12-31. " +
			"Write ./out.yaml.",
		EditPath: "out.yaml",
	}
	out := filepath.Join(work, "prompt.md")
	if err := renderPrompt(bin, work, envv, c, out); err != nil {
		t.Fatalf("render: %v", err)
	}
	body, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"id, email, name", "2024-01-01,2024-12-31"} {
		if !strings.Contains(text, want) {
			t.Errorf("prompt.md missing %q\n---\n%s", want, text)
		}
	}
	if strings.Contains(text, "id  email  name") || strings.Contains(text, "2024-01-01 2024-12-31") {
		t.Errorf("commas were stripped from the rendered intention")
	}
}

func TestRunTimedKillsProcessGroup(t *testing.T) {
	script := filepath.Join(t.TempDir(), "hang.sh")
	body := "#!/bin/sh\n" +
		"echo PARENT:$$\n" +
		"(sleep 60; echo CHILD_DONE) &\n" +
		"echo CHILD:$!\n" +
		"wait\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	out, err := RunTimed("/bin/sh", []string{script}, "", nil, 400*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil || !strings.Contains(err.Error(), "timeout after") {
		t.Fatalf("want timeout error, got %v out=%q", err, out)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("elapsed %s, timeout did not fail fast", elapsed)
	}

	parent, child := parsePidLine(out, "PARENT:"), parsePidLine(out, "CHILD:")
	if parent == 0 || child == 0 {
		t.Fatalf("missing pids in output %q", out)
	}
	// Give the kernel a tick to reap.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !pidAlive(parent) && !pidAlive(child) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("pids still alive parent=%d (%v) child=%d (%v)", parent, pidAlive(parent), child, pidAlive(child))
}

func parsePidLine(out, prefix string) int {
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		n, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, prefix)))
		return n
	}
	return 0
}

func TestEvalCasesValid(t *testing.T) {
	reg, err := LoadFixtureRegistry(registryPath())
	if err != nil {
		t.Fatal(err)
	}
	cases, err := LoadCases(casesDir())
	if err != nil {
		t.Fatalf("load shipped cases: %v", err)
	}
	for _, c := range cases {
		for _, e := range ValidateCase(c, reg) {
			t.Errorf("%s: %s", c.ID, e)
		}
	}

	tmp := t.TempDir()
	write := func(name, body string) string {
		dir := filepath.Join(tmp, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, "case.yaml")
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}

	if _, err := loadCase(write("e.bad.keys", "id: e.bad.keys\nunknown_key: 1\ntier: core\ngating: true\n")); err == nil {
		t.Fatal("unknown key must fail load")
	}

	p := write("e.bad.nogating", "id: e.bad.nogating\ntier: core\ntask: debug\nintention: x\n")
	c, err := loadCase(p)
	if err != nil {
		t.Fatal(err)
	}
	errs := ValidateCase(c, reg)
	if !hasErrSub(errs, "gating") {
		t.Fatalf("want missing gating, got %v", errs)
	}

	p = write("e.bad.grader", "id: e.bad.grader\ntier: core\ngating: true\ntask: debug\nintention: x\ngraders:\n  required:\n    - not_a_grader: 1\n")
	c, err = loadCase(p)
	if err != nil {
		t.Fatal(err)
	}
	if !hasErrSub(ValidateCase(c, reg), "unknown grader") {
		t.Fatal("want unknown grader")
	}
}

func hasErrSub(errs []string, sub string) bool {
	for _, e := range errs {
		if strings.Contains(e, sub) {
			return true
		}
	}
	return false
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "case.yaml")
	if err := os.WriteFile(p, []byte("id: x\nnot_a_field: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCase(p); err == nil {
		t.Fatal("expected unknown field error")
	}
}

func TestUsedConnectionsIsExplicit(t *testing.T) {
	c := Case{Intention: "replicate POSTGRES to DUCKDB", Connections: []string{"MYSQL"}}
	got := c.UsedConnections()
	if len(got) != 1 || got[0] != "MYSQL" {
		t.Fatalf("used=%v", got)
	}
	c.Connections = nil
	if len(c.UsedConnections()) != 0 {
		t.Fatal("empty connections must not scan intention")
	}
}

func TestSelectTierSmokeDoesNotPullDeep(t *testing.T) {
	all := []Case{
		{ID: "e.s", Tier: TierSmoke, Tags: []string{"a"}},
		{ID: "e.d", Tier: TierDeep, Tags: []string{"a"}},
	}
	got := SelectCases(all, Flags{Tiers: []string{TierSmoke}})
	if len(got) != 1 || got[0].ID != "e.s" {
		t.Fatalf("got=%v", got)
	}
}

func TestPairedDiffRemovedNotFail(t *testing.T) {
	base := []TrialResult{
		{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1},
		{Case: "e.gone", Arm: "mock", Trial: 1, Pass: true, Score: 1},
	}
	cur := []TrialResult{
		{Case: "e.01", Arm: "mock", Trial: 1, Pass: true, Score: 1},
	}
	flips := PairedDiff(cur, base, map[string]bool{"e.01": true, "e.gone": true})
	var removed, pf bool
	for _, f := range flips {
		if f.Case == "e.gone" && f.To == "removed" {
			removed = true
		}
		if f.To == "fail" {
			pf = true
		}
	}
	if !removed {
		t.Fatalf("missing removed flip: %+v", flips)
	}
	if pf {
		t.Fatal("removed must not be fail")
	}
	if SmokeFlipFail(flips, map[string]bool{"e.gone": true}) {
		t.Fatal("removed must not fail the smoke gate")
	}
}

func TestSmokeFlipStillBlocks(t *testing.T) {
	base := []TrialResult{{Case: "e.01", Arm: "claude", Trial: 1, Pass: true, Score: 1}}
	cur := []TrialResult{{Case: "e.01", Arm: "claude", Trial: 1, Pass: false, Score: 0}}
	flips := PairedDiff(cur, base, map[string]bool{"e.01": true})
	if !SmokeFlipFail(flips, map[string]bool{"e.01": true}) {
		t.Fatal("smoke pass→fail must block")
	}
}

func TestFormatSummarySkillsDelta(t *testing.T) {
	sum := SuiteSummary{
		Arms: map[string]ArmSummary{"claude": {Cases: 1, PassAt1: 1}, "noskills": {Cases: 1, PassAt1: 0}},
		Cases: []CaseMetrics{
			{Case: "e.01", Arm: "claude", Tier: TierSmoke, PassAtK: true, Gating: true, Trials: 1},
			{Case: "e.01", Arm: "noskills", Tier: TierSmoke, PassAtK: false, Gating: true, Trials: 1},
		},
		SkillsDelta: skillsDeltaLine([]CaseMetrics{
			{Case: "e.01", Arm: "claude", Tier: TierSmoke, PassAtK: true, Trials: 1},
			{Case: "e.01", Arm: "noskills", Tier: TierSmoke, PassAtK: false, Trials: 1},
		}),
		Verdict: "pass",
	}
	out := FormatSummary(sum)
	if !strings.Contains(out, "skills-delta") || !strings.Contains(out, "claude") || !strings.Contains(out, "noskills") {
		t.Fatalf("summary missing skills-delta:\n%s", out)
	}
}

func TestGraderQueryEqualsAndTolerance(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", true); err != nil {
		t.Fatal(err)
	}
	envv := trialEnv(home, dir, bin)
	ctx := GradeContext{WorkDir: dir, SlingBin: bin, Env: envv}
	r := gradeQuery(ctx, map[string]any{
		"connection": "DUCKDB",
		"sql":        "select 2+2",
		"equals":     4,
	})
	if !r.Pass {
		t.Fatalf("equals: %+v", r)
	}
	r = gradeQuery(ctx, map[string]any{
		"connection": "DUCKDB",
		"sql":        "select 10",
		"equals":     11,
		"tolerance":  2,
	})
	if !r.Pass {
		t.Fatalf("tolerance: %+v", r)
	}
	r = gradeQuery(ctx, map[string]any{
		"connection": "DUCKDB",
		"sql":        "select 1",
		"equals":     99,
	})
	if r.Pass {
		t.Fatal("wrong scalar must fail")
	}
	r = gradeQuery(ctx, map[string]any{
		"connection":   "DUCKDB",
		"sql":          "select 5",
		"equals_query": "select 2+3",
	})
	if !r.Pass {
		t.Fatalf("equals_query: %+v", r)
	}
}

func TestGraderRowsEqual(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", true); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, CaseDir: dir, SlingBin: bin, Env: trialEnv(home, dir, bin)}
	r := gradeRowsEqual(ctx, map[string]any{
		"connection":   "DUCKDB",
		"actual_sql":   "select 1 as a, 2 as b",
		"expected_sql": "select 1 as a, 2 as b",
		"order_by":     "a",
	})
	if !r.Pass {
		t.Fatalf("rows match: %+v", r)
	}
	r = gradeRowsEqual(ctx, map[string]any{
		"connection":   "DUCKDB",
		"actual_sql":   "select 1 as a",
		"expected_sql": "select 9 as a",
	})
	if r.Pass {
		t.Fatal("wrong rows must fail")
	}
}

func TestGraderDAGAndBuildCompile(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sling_build.yml"), []byte("target: DUCKDB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "staging"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "staging", "stg_a.sql"), []byte("select 1 as id\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "staging", "stg_b.sql"), []byte("select * from {{ ref('stg_a') }}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", true); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "sling_build.yml", SlingBin: bin, Env: trialEnv(home, dir, bin)}
	if r := gradeSling(ctx, "build compile ."); !r.Pass {
		t.Fatalf("compile good: %+v", r)
	}
	if r := gradeDAG(ctx, map[string]any{"model": "stg_b", "depends_on": []any{"stg_a"}}); !r.Pass {
		t.Fatalf("dag: %+v", r)
	}
	if r := gradeDAG(ctx, map[string]any{"model": "stg_b", "depends_on": []any{"missing"}}); r.Pass {
		t.Fatal("missing edge must fail")
	}
	// cycle
	if err := os.WriteFile(filepath.Join(dir, "staging", "stg_a.sql"), []byte("select * from {{ ref('stg_b') }}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := gradeSling(ctx, "build compile ."); r.Pass {
		t.Fatal("cycle must fail compile")
	}
}

func TestGraderTestsPass(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skip(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "sling_build.yml"), []byte("target: DUCKDB\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "staging"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "staging", "stg.sql"), []byte("select 1 as id\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	if err := SeedHome(home, "home_claude", true); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, SlingBin: bin, Env: trialEnv(home, dir, bin)}
	if r := gradeTestsPass(ctx, map[string]any{"target": "DUCKDB"}); r.Pass {
		t.Fatalf("no tests must fail: %+v", r)
	}
	if err := os.WriteFile(filepath.Join(dir, "staging", "stg.sql"), []byte("/**\nmode: table\ntests:\n  - not_null: [id]\n**/\nselect 1 as id\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if r := gradeSling(ctx, "build run . --target DUCKDB"); !r.Pass {
		t.Fatalf("build run: %+v", r)
	}
	if r := gradeTestsPass(ctx, map[string]any{"target": "DUCKDB"}); !r.Pass {
		t.Fatalf("tests_pass: %+v", r)
	}
}

func TestFixtures(t *testing.T) {
	bin, err := FindSlingBin()
	if err != nil {
		t.Skip(err)
	}
	p, err := NewProvisioner(bin, os.Environ(), t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Reset(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := p.Ensure([]string{"tpch_duckdb", "tpch_postgres", "tpch_clickhouse", "ecom_dirty"}); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if !p.Ready("tpch_duckdb") {
		t.Fatal("tpch_duckdb must provision")
	}
	ok, err := p.readyCheck(p.Registry["tpch_duckdb"])
	if err != nil || !ok {
		t.Fatalf("tpch_duckdb ready_check: ok=%v err=%v", ok, err)
	}
	got := firstScalar(mustExec(t, p, "DUCKDB", "select count(*) from orders"))
	if !scalarsEqual(got, 15000, nil) {
		t.Fatalf("orders count=%v", got)
	}
	for _, name := range []string{"tpch_postgres", "tpch_clickhouse", "ecom_dirty"} {
		if p.Ready(name) {
			ok, err := p.readyCheck(p.Registry[name])
			if err != nil || !ok {
				t.Errorf("%s ready after provision: ok=%v err=%v", name, ok, err)
			}
		} else if p.Skipped(name) == "" {
			t.Errorf("%s not ready and not skipped", name)
		} else {
			t.Logf("%s skipped: %s", name, p.Skipped(name))
		}
	}
	p2, err := NewProvisioner(bin, os.Environ(), t.Logf)
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	if err := p2.Ensure([]string{"tpch_duckdb"}); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(t0); d > 5*time.Second {
		t.Fatalf("second ready_check took %s", d)
	}
	if err := p.Reset(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tpchDuckPath()); !os.IsNotExist(err) {
		t.Fatal("reset must drop tpch.duckdb")
	}
	_ = start
}

func mustExec(t *testing.T, p *Provisioner, conn, sql string) string {
	t.Helper()
	out, err := p.execSQL(conn, sql)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestInventoryAndFixtureServer(t *testing.T) {
	for _, gone := range []string{"e.08", "e.19", "e.20", "e.21"} {
		ents, _ := os.ReadDir(casesDir())
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), gone) {
				t.Errorf("folder %s should be gone", e.Name())
			}
		}
	}
	exp, err := os.ReadFile(filepath.Join(casesDir(), "e.09.repl_pg_ch_multistream", "expected.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(exp), "snapshot") {
		t.Fatal("e.09 expected must include a snapshot stream")
	}
	for _, id := range []string{
		"e.18.build_staging_view", "e.22.spec_simple_rest", "e.27.build_tpch_layers",
		"e.33.repl_incremental_outcome", "e.38.spec_cursor_stop", "e.43.spec_real_dummyjson",
		"e.46.repl_cdc_create", "e.47.cdc_debug", "e.49.debug_real_run",
	} {
		if _, err := os.Stat(filepath.Join(casesDir(), id, "case.yaml")); err != nil {
			t.Errorf("missing %s", id)
		}
	}
	cases, err := LoadCases(casesDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Tier == TierSmoke && !contains(c.Arms, "noskills") {
			t.Errorf("%s smoke missing noskills", c.ID)
		}
	}
	if _, err := os.Stat(filepath.Join(fixturesDir(), "data", "mock_api.yaml")); err != nil {
		t.Fatal("mock_api.yaml must exist (not dangling)")
	}
	body, _ := os.ReadFile(filepath.Join(fixturesDir(), "home_claude", ".sling", "env.yaml"))
	if strings.Contains(string(body), "MOCK_API_SELECT") {
		t.Fatal("MOCK_API_SELECT must be gone")
	}

	srv, err := StartFixtureServer()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	req, _ := http.NewRequest("GET", srv.URL+"/users?offset=0&limit=50", nil)
	req.Header.Set("Authorization", "Bearer "+fixtureToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var page map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	data, _ := page["data"].([]any)
	if len(data) != 50 {
		t.Fatalf("page size=%d", len(data))
	}
	// all 250
	n := 0
	for off := 0; off < 300; off += 50 {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/users?offset=%d&limit=50", srv.URL, off), nil)
		req.Header.Set("Authorization", "Bearer "+fixtureToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var p map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()
		rows, _ := p["data"].([]any)
		n += len(rows)
		if len(rows) == 0 {
			break
		}
	}
	if n != 250 {
		t.Fatalf("users=%d", n)
	}

	// cursor exhaust
	cur := ""
	orders := 0
	for i := 0; i < 20; i++ {
		u := srv.URL + "/orders"
		if cur != "" {
			u += "?cursor=" + cur
		}
		req, _ := http.NewRequest("GET", u, nil)
		req.Header.Set("Authorization", "Bearer "+fixtureToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var p map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()
		rows, _ := p["data"].([]any)
		orders += len(rows)
		if p["next_cursor"] == nil {
			break
		}
		cur = fmt.Sprint(p["next_cursor"])
	}
	if orders != 1000 {
		t.Fatalf("orders=%d", orders)
	}

	// oauth2
	resp, err = http.Post(srv.URL+"/oauth/token", "application/x-www-form-urlencoded", strings.NewReader("client_id="+fixtureClientID+"&client_secret="+fixtureClientSecret))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("oauth status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// 429 then 200
	req, _ = http.NewRequest("GET", srv.URL+"/flaky", nil)
	req.Header.Set("Authorization", "Bearer "+fixtureToken)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 429 {
		t.Fatalf("flaky1=%d", resp.StatusCode)
	}
	resp.Body.Close()
	req, _ = http.NewRequest("GET", srv.URL+"/flaky", nil)
	req.Header.Set("Authorization", "Bearer "+fixtureToken)
	resp, _ = http.DefaultClient.Do(req)
	if resp.StatusCode != 200 {
		t.Fatalf("flaky2=%d", resp.StatusCode)
	}
	resp.Body.Close()

	// downed server fails api_spec test
	dir := t.TempDir()
	spec := fmt.Sprintf(`name: x
authentication:
  type: static
  headers:
    Authorization: "Bearer %s"
endpoints:
  users:
    request:
      url: %s/users
      method: GET
    response:
      records:
        jmespath: data
        primary_key: [id]
`, fixtureToken, srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "s.yaml"), []byte(spec), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "s.yaml", FixtureBaseURL: srv.URL, FixtureToken: fixtureToken}
	if r := gradeAPISpec(ctx, "test", filepath.Join(dir, "s.yaml"), 0, "users"); !r.Pass {
		t.Fatalf("live server test: %+v", r)
	}
	srv.Close()
	if r := gradeAPISpec(ctx, "test", filepath.Join(dir, "s.yaml"), 0, "users"); r.Skip || r.Pass {
		t.Fatalf("downed server must fail: %+v", r)
	}

	// infinite pagination mutant times out
	bad := fmt.Sprintf(`name: x
authentication:
  type: static
  headers:
    Authorization: "Bearer %s"
endpoints:
  orders:
    request:
      url: "%s/orders"
      method: GET
      parameters:
        cursor: "{state.cursor}"
    pagination:
      next_state:
        cursor: "{state.cursor}"
    response:
      records:
        jmespath: "data"
`, fixtureToken, "http://127.0.0.1:1")
	// use a live server again
	srv2, err := StartFixtureServer()
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	bad = strings.ReplaceAll(bad, "http://127.0.0.1:1", srv2.URL)
	if err := os.WriteFile(filepath.Join(dir, "loop.yaml"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx2 := GradeContext{WorkDir: dir, Artifact: "loop.yaml", FixtureBaseURL: srv2.URL, FixtureToken: fixtureToken, APITimeout: 2 * time.Second, APIPageCap: 3}
	r := gradeAPISpec(ctx2, "test", filepath.Join(dir, "loop.yaml"), 0, "orders")
	if r.Pass {
		t.Fatal("missing stop_condition must not pass")
	}
	if !strings.Contains(r.Detail, "timeout") && !strings.Contains(r.Detail, "cap") && !strings.Contains(r.Detail, "stop_condition") {
		t.Fatalf("want timeout, page cap, or missing stop_condition: %+v", r)
	}
}

func writeFakeSling(t *testing.T, dir, body string) string {
	t.Helper()
	p := filepath.Join(dir, "sling")
	if err := os.WriteFile(p, []byte("#!/bin/bash\n"+body+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestParseInsertedRows(t *testing.T) {
	n, ok := parseInsertedRows("inserted 100 rows into main.orders in 2 secs")
	if !ok || n != 100 {
		t.Fatalf("got %d ok=%v", n, ok)
	}
	n, ok = parseInsertedRows("0 rows inserted. Nothing to do.")
	if !ok || n != 0 {
		t.Fatalf("zero form: got %d ok=%v", n, ok)
	}
	if _, ok := parseInsertedRows("done"); ok {
		t.Fatal("empty output must not parse")
	}
}

func TestOutcomeRunTwiceDelta(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeSling(t, dir, `
if [[ "$1" == "run" ]]; then
  if [[ -f .run1 ]]; then
    echo "inserted 100 rows into main.orders in 1 secs"
    exit 0
  fi
  echo "inserted 15000 rows into main.orders in 1 secs"
  touch .run1
  exit 0
fi
if [[ "$1" == "conns" && "$2" == "exec" ]]; then
  if [[ "$3" == "DUCKDB" ]]; then
    printf "count\n100\n"
    exit 0
  fi
  exit 0
fi
exit 0
`)
	if err := os.WriteFile(filepath.Join(dir, "orders_repl.yaml"), []byte("source: POSTGRES\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "orders_repl.yaml", SlingBin: bin}
	spec := map[string]any{
		"first_run":  "sling run -r {artifact}",
		"insert":     map[string]any{"connection": "POSTGRES", "sql": "insert into eval_tpch.orders select 1"},
		"second_run": "sling run -r {artifact}",
		"rows_moved": map[string]any{"equals": 100, "tolerance": 5},
		"query":      map[string]any{"connection": "DUCKDB", "sql": "select count(*) from main.orders", "equals": 100},
	}
	if r := gradeOutcome(ctx, spec); !r.Pass || r.Skip {
		t.Fatalf("run-twice delta must pass: %+v", r)
	}

	// Full-refresh second run moves the whole table.
	bin2 := writeFakeSling(t, t.TempDir(), `
if [[ "$1" == "run" ]]; then
  echo "inserted 15100 rows into main.orders in 1 secs"
  exit 0
fi
exit 0
`)
	ctx.SlingBin = bin2
	ctx.WorkDir = t.TempDir()
	if r := gradeOutcome(ctx, spec); r.Pass || r.Skip {
		t.Fatalf("full-refresh second run must fail rows_moved: %+v", r)
	}
	if !strings.Contains(gradeOutcome(ctx, spec).Detail, "moved") {
		t.Fatalf("want rows_moved detail: %+v", gradeOutcome(ctx, spec))
	}
}

func TestAPISpecIncrementalSecondRun(t *testing.T) {
	srv, err := StartFixtureServer()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	dir := t.TempDir()
	good := fmt.Sprintf(`name: MOCK_API
authentication:
  type: static
  headers:
    Authorization: "Bearer %s"
endpoints:
  users:
    request:
      url: "%s/users"
      method: GET
      parameters:
        updated_since: '{coalesce(sync.updated_since, "2024-01-01T00:00:00Z")}'
        limit: "1000"
    response:
      records:
        jmespath: "data"
        primary_key: [id]
      processors:
        - expression: "record.updated_at"
          output: "state.updated_since"
          aggregation: maximum
    sync:
      - updated_since
`, fixtureToken, srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "good.yaml"), []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	nosync := fmt.Sprintf(`name: MOCK_API
authentication:
  type: static
  headers:
    Authorization: "Bearer %s"
endpoints:
  users:
    request:
      url: "%s/users"
      method: GET
    response:
      records:
        jmespath: "data"
        primary_key: [id]
`, fixtureToken, srv.URL)
	if err := os.WriteFile(filepath.Join(dir, "nosync.yaml"), []byte(nosync), 0o644); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{WorkDir: dir, Artifact: "good.yaml", FixtureBaseURL: srv.URL, FixtureToken: fixtureToken, APITimeout: 15 * time.Second, APIPageCap: 50}
	r := gradeAPISpecOpts(ctx, "test", filepath.Join(dir, "good.yaml"), "users", nil, map[string]any{"equals": 0})
	if !r.Pass || r.Skip {
		t.Fatalf("incremental second run must return 0: %+v", r)
	}
	if !strings.Contains(r.Detail, "then") {
		t.Fatalf("detail should show both runs: %+v", r)
	}
	r = gradeAPISpecOpts(ctx, "test", filepath.Join(dir, "nosync.yaml"), "users", nil, map[string]any{"equals": 0})
	if r.Pass || r.Skip {
		t.Fatalf("spec without sync must fail then_sync equals 0: %+v", r)
	}
}

func TestExpectedUsersSubtreeVsSeed(t *testing.T) {
	seedPath := filepath.Join(casesDir(), "e.24.spec_update_add_endpoint", "seed", "spec.yaml")
	mutPath := filepath.Join(casesDir(), "e.24.spec_update_add_endpoint", "mutants", "m1.yaml")
	seed, err := YAMLMap(seedPath)
	if err != nil {
		t.Fatal(err)
	}
	mut, err := YAMLMap(mutPath)
	if err != nil {
		t.Fatal(err)
	}
	ok := CompareCompiled(seed, seed, MatchPolicy{MustMatch: []string{"endpoints.users"}})
	for _, r := range ok {
		if !r.Pass {
			t.Fatalf("seed vs seed: %s %s", r.Path, r.Message)
		}
	}
	bad := CompareCompiled(mut, seed, MatchPolicy{MustMatch: []string{"endpoints.users"}})
	failed := false
	for _, r := range bad {
		if !r.Pass {
			failed = true
		}
	}
	if !failed {
		t.Fatal("mutant users subtree must fail vs seed")
	}

	dir := t.TempDir()
	if err := copyFile(seedPath, filepath.Join(dir, "spec.yaml")); err != nil {
		t.Fatal(err)
	}
	ctx := GradeContext{
		WorkDir:  dir,
		Artifact: "spec.yaml",
		CaseDir:  filepath.Join(casesDir(), "e.24.spec_update_add_endpoint"),
	}
	r := gradeExpected(ctx, map[string]any{
		"file": "seed/spec.yaml", "compare": "raw", "must_match": []any{"endpoints.users"},
	})
	if !r.Pass {
		t.Fatalf("unchanged users vs seed: %+v", r)
	}
	if err := copyFile(mutPath, filepath.Join(dir, "spec.yaml")); err != nil {
		t.Fatal(err)
	}
	r = gradeExpected(ctx, map[string]any{
		"file": "seed/spec.yaml", "compare": "raw", "must_match": []any{"endpoints.users"},
	})
	if r.Pass {
		t.Fatal("changed users must fail vs seed")
	}
}

func TestRunHookAllowFailCapturesStderr(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeSling(t, dir, `echo "column updated_atz does not exist" >&2; exit 1`)
	hooks := []any{map[string]any{
		"sling":      "run -r bad.yaml",
		"allow_fail": true,
		"stderr":     "run.stderr",
	}}
	if err := runHookCmds(bin, dir, os.Environ(), hooks, map[string]bool{}); err != nil {
		t.Fatalf("allow_fail must continue: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "run.stderr"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "updated_atz") {
		t.Fatalf("stderr file: %q", b)
	}

	dir2 := t.TempDir()
	strict := []any{map[string]any{"sling": "run -r bad.yaml"}}
	if err := runHookCmds(bin, dir2, os.Environ(), strict, map[string]bool{}); err == nil {
		t.Fatal("setup without allow_fail must return the error")
	}
}

func TestREADMEHygieneRule(t *testing.T) {
	b, err := os.ReadFile(filepath.Join(evalsDir(), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(b)
	for _, want := range []string{"canonical schema", "outcome grader", "mutant", "negative"} {
		if !strings.Contains(strings.ToLower(text), strings.ToLower(want)) {
			t.Errorf("README missing %q", want)
		}
	}
}

func TestCheckArmOpenCode2(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("GEMINI_API_KEY", "")
	t.Setenv("GOOGLE_API_KEY", "")
	t.Setenv("XAI_API_KEY", "")
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("OPENCODE2_PATH", filepath.Join(t.TempDir(), "missing-opencode2"))

	a := CheckArm("opencode2")
	if a.Skip == "" || !strings.Contains(a.Skip, "missing binary") {
		t.Fatalf("want missing binary skip, got %+v", a)
	}

	stub := filepath.Join(t.TempDir(), "opencode2")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENCODE2_PATH", stub)
	a = CheckArm("opencode2")
	if a.Skip == "" || !strings.Contains(a.Skip, "missing login") {
		t.Fatalf("want missing login skip, got %+v", a)
	}

	auth := filepath.Join(host, ".local", "share", "opencode", "auth.json")
	if err := os.MkdirAll(filepath.Dir(auth), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(auth, []byte(`{"anthropic":{"type":"api"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a = CheckArm("opencode2")
	if a.Skip != "" {
		t.Fatalf("want available, skip=%q", a.Skip)
	}
	if a.Binary != stub {
		t.Fatalf("binary=%s want %s", a.Binary, stub)
	}
}

func TestLinkHostAuthOpenCode2(t *testing.T) {
	host := t.TempDir()
	sandbox := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)

	if err := LinkHostAuth(sandbox, "opencode2"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(sandbox, ".local", "share", "opencode", "auth.json")); !os.IsNotExist(err) {
		t.Fatal("absent host auth file must be a no-op")
	}

	src := filepath.Join(host, ".local", "share", "opencode", "auth.json")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte(`{"anthropic":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LinkHostAuth(sandbox, "opencode2"); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(filepath.Join(sandbox, ".local", "share", "opencode", "auth.json"))
	if err != nil {
		t.Fatalf("auth.json should be a symlink: %v", err)
	}
	if got != src {
		t.Fatalf("symlink=%s want %s", got, src)
	}
}

func TestSeedTrialMCPWritesProjectConfig(t *testing.T) {
	work := t.TempDir()
	bin := "/opt/sling/sling"
	if err := seedTrialMCP(work, bin, "claude"); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(work, ".mcp.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), bin) || !strings.Contains(string(body), "serve") {
		t.Fatalf("claude mcp missing trial bin: %s", body)
	}
	if err := seedTrialMCP(work, bin, "grok"); err != nil {
		t.Fatal(err)
	}
	gt, err := os.ReadFile(filepath.Join(work, ".grok", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gt), bin) {
		t.Fatalf("grok mcp missing trial bin: %s", gt)
	}
}

func TestRewritePromptMCPWired(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "prompt.md")
	if err := os.WriteFile(p, []byte("- MCP wired: no\n- preferred agent: claude\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := rewritePromptMCPWired(p, true); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	if !strings.Contains(string(b), "- MCP wired: yes") {
		t.Fatalf("want wired yes, got %s", b)
	}
}

func TestPreflightSkipsIsolatedDuckDB(t *testing.T) {
	down := PreflightConns("/bin/false", []string{"DUCKDB", "POSTGRES"}, nil)
	if down["DUCKDB"] {
		t.Fatal("DUCKDB must not be preflighted (locks eval.duckdb)")
	}
	if !down["POSTGRES"] {
		t.Fatal("live conns still preflight")
	}
}

func TestAgentEvalArgsOpenCode2(t *testing.T) {
	args := agentEvalArgs("opencode2", "/tmp/prompt.md", "/tmp/work", 0)
	joined := strings.Join(args, " ")
	for _, want := range []string{"run", "--standalone", "--agent eval", "prompt.md"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %v", want, args)
		}
	}
	if strings.Contains(joined, "/tmp/prompt.md") {
		t.Fatal("absolute prompt path becomes external_directory; use relative prompt.md")
	}
	if strings.Contains(joined, "--auto") {
		t.Fatal("--auto would approve ask-able actions; permissions must be explicit")
	}
	if strings.Contains(joined, evalAgentSystemPrompt[:20]) {
		t.Fatal("system prompt must live in seeded opencode.json, not argv")
	}
}

func TestSeedHomeOpenCode2(t *testing.T) {
	home := t.TempDir()
	if err := SeedHome(home, "home_opencode2", false); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(home, ".config", "opencode", "opencode.json")
	body, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("json: %v\n%s", err, body)
	}
	perms, ok := doc["permissions"].([]any)
	if !ok || len(perms) == 0 {
		t.Fatalf("permissions must be a non-empty array: %#v", doc["permissions"])
	}
	var lastShellAllow bool
	for _, p := range perms {
		m, _ := asMap(p)
		if castToString(m["action"]) != "shell" {
			continue
		}
		if castToString(m["resource"]) == "sling *" && castToString(m["effect"]) == "allow" {
			lastShellAllow = true
		} else {
			lastShellAllow = false
		}
	}
	if !lastShellAllow {
		t.Fatal("last shell rule for sling * must be allow")
	}
	agents, _ := asMap(doc["agents"])
	eval, _ := asMap(agents["eval"])
	if castToString(eval["system"]) != evalAgentSystemPrompt {
		t.Fatalf("agents.eval.system mismatch:\n%s", castToString(eval["system"]))
	}
	if _, err := os.Stat(filepath.Join(home, ".agents", "skills")); err != nil {
		t.Fatalf("skills overlay missing: %v", err)
	}

	home2 := t.TempDir()
	t.Setenv("EVAL_OPENCODE2_MODEL", "openai/gpt-4.1")
	if err := SeedHome(home2, "home_opencode2", false); err != nil {
		t.Fatal(err)
	}
	body, err = os.ReadFile(filepath.Join(home2, ".config", "opencode", "opencode.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatal(err)
	}
	if castToString(doc["model"]) != "openai/gpt-4.1" {
		t.Fatalf("EVAL_OPENCODE2_MODEL not applied: %v", doc["model"])
	}
}

func TestAgentEnvOpenCode2Isolation(t *testing.T) {
	host := t.TempDir()
	t.Setenv("HOME", host)
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("OPENCODE", "1")
	t.Setenv("OPENCODE_SESSION", "ses_parent")
	t.Setenv("OPENCODE_TERMINAL", "1")
	sandbox := t.TempDir()
	work := t.TempDir()
	envv := agentEnv(sandbox, work, "/bin/sling", "opencode2")
	got := map[string]string{}
	for _, kv := range envv {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
	}
	if got["HOME"] != sandbox {
		t.Fatalf("HOME=%s want sandbox %s", got["HOME"], sandbox)
	}
	if got["HOME"] == host {
		t.Fatal("host HOME must be absent")
	}
	if got["PWD"] != work {
		t.Fatalf("PWD=%s want work %s", got["PWD"], work)
	}
	if got["XDG_CONFIG_HOME"] != filepath.Join(sandbox, ".config") {
		t.Fatalf("XDG_CONFIG_HOME=%s", got["XDG_CONFIG_HOME"])
	}
	if got["XDG_DATA_HOME"] != filepath.Join(sandbox, ".local", "share") {
		t.Fatalf("XDG_DATA_HOME=%s", got["XDG_DATA_HOME"])
	}
	if got["XDG_STATE_HOME"] != filepath.Join(sandbox, ".local", "state") {
		t.Fatalf("XDG_STATE_HOME=%s", got["XDG_STATE_HOME"])
	}
	if got["OPENCODE_DB"] != filepath.Join(work, "opencode.db") {
		t.Fatalf("OPENCODE_DB=%s", got["OPENCODE_DB"])
	}
	for _, leak := range []string{"OPENCODE", "OPENCODE_SESSION", "OPENCODE_TERMINAL"} {
		if _, ok := got[leak]; ok {
			t.Fatalf("parent session env leaked: %s=%s", leak, got[leak])
		}
	}
}

func TestSeedHomeOpenCode2WritesProjectConfig(t *testing.T) {
	home := t.TempDir()
	work := t.TempDir()
	if err := SeedHome(home, "home_opencode2", false); err != nil {
		t.Fatal(err)
	}
	if err := seedOpenCode2Project(home, work); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(work, ".opencode", "opencode.json")); err != nil {
		t.Fatalf("project .opencode/opencode.json missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(work, "opencode.json")); !os.IsNotExist(err) {
		t.Fatal("root opencode.json would be picked up as a sling build seed")
	}
}

func TestParseAgentTelemetryOpenCode2(t *testing.T) {
	golden := `{"model":"anthropic/claude-sonnet-4-5","cost":0.042,"turns":7}`
	cost, turns, model := parseAgentTelemetry("opencode2", golden)
	if cost != 0.042 || turns != 7 || model != "anthropic/claude-sonnet-4-5" {
		t.Fatalf("got cost=%v turns=%d model=%q", cost, turns, model)
	}
	// Nested usage + steps aliases.
	nested := `noise
{"model":"anthropic/claude-sonnet-4-5","usage":{"cost_usd":0.01},"steps":3}
`
	cost, turns, model = parseAgentTelemetry("opencode2", nested)
	if cost != 0.01 || turns != 3 || model != "anthropic/claude-sonnet-4-5" {
		t.Fatalf("nested: cost=%v turns=%d model=%q", cost, turns, model)
	}
	// Untracked cost falls back to 0 and does not panic.
	cost, turns, model = parseAgentTelemetry("opencode2", "plain text, no json")
	if cost != 0 || turns != 0 || model != "" {
		t.Fatalf("fallback: cost=%v turns=%d model=%q", cost, turns, model)
	}
}

func TestCheckArmCodex(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("CODEX_API_KEY", "")
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("CODEX_PATH", filepath.Join(t.TempDir(), "missing-codex"))

	a := CheckArm("codex")
	if a.Skip == "" || !strings.Contains(a.Skip, "missing binary") {
		t.Fatalf("want missing binary skip, got %+v", a)
	}

	stub := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_PATH", stub)
	a = CheckArm("codex")
	if a.Skip == "" || !strings.Contains(a.Skip, "missing login") {
		t.Fatalf("want missing login skip, got %+v", a)
	}

	auth := filepath.Join(host, ".codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(auth), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(auth, []byte(`{"tokens":{"access_token":"x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	a = CheckArm("codex")
	if a.Skip != "" {
		t.Fatalf("want available, skip=%q", a.Skip)
	}
	if a.Binary != stub {
		t.Fatalf("binary=%s want %s", a.Binary, stub)
	}
}

func TestLinkHostAuthCodex(t *testing.T) {
	host := t.TempDir()
	sandbox := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)

	if err := LinkHostAuth(sandbox, "codex"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(sandbox, ".codex", "auth.json")); !os.IsNotExist(err) {
		t.Fatal("absent host auth file must be a no-op")
	}

	src := filepath.Join(host, ".codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(src), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src, []byte(`{"tokens":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LinkHostAuth(sandbox, "codex"); err != nil {
		t.Fatal(err)
	}
	got, err := os.Readlink(filepath.Join(sandbox, ".codex", "auth.json"))
	if err != nil {
		t.Fatalf("auth.json should be a symlink: %v", err)
	}
	if got != src {
		t.Fatalf("symlink=%s want %s", got, src)
	}
}

func TestAgentEvalArgsCodex(t *testing.T) {
	args := agentEvalArgs("codex", "/tmp/prompt.md", "/tmp/work", 0)
	joined := strings.Join(args, " ")
	for _, want := range []string{"exec", "--json", "--skip-git-repo-check", "--sandbox workspace-write"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing %q in %v", want, args)
		}
	}
	if strings.Contains(joined, " -m ") || strings.HasPrefix(joined, "-m ") || strings.Contains(joined, " --model ") {
		t.Fatalf("must not pin a model: %v", args)
	}
	for i, a := range args {
		if a == "-m" || a == "--model" {
			t.Fatalf("must not pass %s at %d: %v", a, i, args)
		}
	}
	if !strings.Contains(joined, evalAgentSystemPrompt[:40]) {
		t.Fatal("eval system prompt must be prepended to the exec prompt")
	}
}

func TestSeedHomeCodex(t *testing.T) {
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("EVAL_CODEX_MODEL", "")
	if err := os.MkdirAll(filepath.Join(host, ".codex"), 0o755); err != nil {
		t.Fatal(err)
	}
	hostCfg := `
model = "host-model"
model_provider = "ZAI"
model_reasoning_effort = "high"
model_catalog_json = "~/.codex/models.json"

[model_providers.ZAI]
name = "ZAI"
base_url = "https://example.test/v1"
env_key = "ZAI_API_KEY"

[mcp_servers.hostleak]
command = "leaked"

[projects."/tmp/secret"]
trust_level = "trusted"
`
	if err := os.WriteFile(filepath.Join(host, ".codex", "config.toml"), []byte(hostCfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(host, ".codex", "models.json"), []byte(`{"models":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}

	home := t.TempDir()
	if err := SeedHome(home, "home_codex", false); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(home, ".codex", "config.toml")
	doc, err := readTOMLMap(cfgPath)
	if err != nil {
		t.Fatalf("toml: %v", err)
	}
	if jsonMapString(doc, "approval_policy") != "never" {
		t.Fatalf("approval_policy=%v", doc["approval_policy"])
	}
	mcp, _ := asMap(doc["mcp_servers"])
	slingSrv, ok := asMap(mcp["sling"])
	if !ok {
		t.Fatalf("mcp_servers.sling missing: %#v", doc["mcp_servers"])
	}
	if jsonMapString(slingSrv, "default_tools_approval_mode") != "approve" {
		t.Fatalf("mcp default_tools_approval_mode=%v", slingSrv["default_tools_approval_mode"])
	}
	mcpEnv, _ := asMap(slingSrv["env"])
	if !strings.Contains(fmt.Sprint(mcpEnv["SLING_HOME_DIR"]), ".sling") {
		t.Fatalf("mcp env SLING_HOME_DIR missing: %#v", slingSrv["env"])
	}
	if _, ok := mcp["hostleak"]; ok {
		t.Fatal("host mcp_servers must not merge")
	}
	if _, ok := doc["projects"]; ok {
		t.Fatal("host projects must not merge")
	}
	if jsonMapString(doc, "model") != "host-model" {
		t.Fatalf("host model not merged: %v", doc["model"])
	}
	providers, _ := asMap(doc["model_providers"])
	zai, _ := asMap(providers["ZAI"])
	if jsonMapString(zai, "name") != "ZAI" {
		t.Fatalf("model_providers not merged: %#v", doc["model_providers"])
	}
	sw, _ := asMap(doc["sandbox_workspace_write"])
	wantRoot := filepath.Join(home, ".sling")
	hasRoot := false
	switch roots := sw["writable_roots"].(type) {
	case []any:
		for _, r := range roots {
			if fmt.Sprint(r) == wantRoot {
				hasRoot = true
			}
		}
	case []string:
		for _, r := range roots {
			if r == wantRoot {
				hasRoot = true
			}
		}
	}
	if !hasRoot {
		t.Fatalf("writable_roots missing sandbox .sling: %#v", sw["writable_roots"])
	}
	agents, err := os.ReadFile(filepath.Join(home, ".codex", "AGENTS.md"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(agents)) != evalAgentSystemPrompt {
		t.Fatalf("AGENTS.md mismatch:\n%s", agents)
	}
	if _, err := os.Stat(filepath.Join(home, ".codex", "models.json")); err != nil {
		t.Fatalf("models.json not copied: %v", err)
	}

	home2 := t.TempDir()
	t.Setenv("EVAL_CODEX_MODEL", "pinned-model")
	if err := SeedHome(home2, "home_codex", false); err != nil {
		t.Fatal(err)
	}
	doc, err = readTOMLMap(filepath.Join(home2, ".codex", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if jsonMapString(doc, "model") != "pinned-model" {
		t.Fatalf("EVAL_CODEX_MODEL not applied: %v", doc["model"])
	}

	t.Setenv("EVAL_CODEX_MODEL", "")
	emptyHost := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", emptyHost)
	home3 := t.TempDir()
	if err := SeedHome(home3, "home_codex", false); err != nil {
		t.Fatal(err)
	}
	doc, err = readTOMLMap(filepath.Join(home3, ".codex", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if jsonMapString(doc, "approval_policy") != "never" {
		t.Fatalf("fixture defaults lost: %#v", doc)
	}
	if jsonMapString(doc, "model") != "" {
		t.Fatalf("missing host config must not invent a model: %v", doc["model"])
	}
}

func TestAgentEnvCodexIsolation(t *testing.T) {
	host := t.TempDir()
	t.Setenv("HOME", host)
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("CODEX_HOME", filepath.Join(host, ".codex-host"))
	sandbox := t.TempDir()
	work := t.TempDir()
	envv := agentEnv(sandbox, work, "/bin/sling", "codex")
	got := map[string]string{}
	nCodex := 0
	for _, kv := range envv {
		k, v, _ := strings.Cut(kv, "=")
		got[k] = v
		if k == "CODEX_HOME" {
			nCodex++
		}
	}
	if got["HOME"] != sandbox {
		t.Fatalf("HOME=%s want sandbox %s", got["HOME"], sandbox)
	}
	want := filepath.Join(sandbox, ".codex")
	if got["CODEX_HOME"] != want {
		t.Fatalf("CODEX_HOME=%s want %s", got["CODEX_HOME"], want)
	}
	if nCodex != 1 {
		t.Fatalf("host CODEX_HOME leaked, count=%d", nCodex)
	}
	if got["CODEX_HOME"] == filepath.Join(host, ".codex-host") {
		t.Fatal("host CODEX_HOME must be dropped")
	}
}

func TestParseAgentTelemetryCodex(t *testing.T) {
	var logs []string
	codexCostLog = func(format string, args ...any) {
		logs = append(logs, fmt.Sprintf(format, args...))
	}
	t.Cleanup(func() {
		codexCostLog = func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, format+"\n", args...)
		}
	})
	codexCostOnce = sync.Once{}

	golden := `Reading additional input from stdin...
{"type":"thread.started","thread_id":"t1"}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"done"}}
{"type":"turn.completed","usage":{"input_tokens":10,"cached_input_tokens":0,"cache_write_input_tokens":0,"output_tokens":5,"reasoning_output_tokens":1}}
{"type":"turn.completed","usage":{"input_tokens":1,"output_tokens":1}}
`
	cost, turns, model := parseAgentTelemetry("codex", golden)
	if cost != 0 || turns != 2 {
		t.Fatalf("got cost=%v turns=%d model=%q", cost, turns, model)
	}
	if model != "" {
		t.Fatalf("phase 0 JSONL has no model field, got %q", model)
	}
	if len(logs) != 1 {
		t.Fatalf("one-shot log count=%d logs=%v", len(logs), logs)
	}
	cost, turns, model = parseAgentTelemetry("codex", golden)
	if len(logs) != 1 {
		t.Fatalf("one-shot log fired again: %v", logs)
	}
	_ = cost
	_ = turns
	_ = model
}

func TestExecutedCommandsCodexStderrPrefix(t *testing.T) {
	skillCat := `{"type":"item.completed","item":{"id":"item_1","type":"command_execution","command":"cat SKILL.md","aggregated_output":"Validate with sling run -r file.yaml --dry-run\n","status":"completed"}}`
	ctx := GradeContext{Transcript: "Reading additional input from stdin...\n" + skillCat + "\n"}
	if r := gradeTranscriptAbsent(ctx, "sling run"); !r.Pass {
		t.Fatalf("skill cat must not fail transcript_absent: %s", r.Detail)
	}
	real := `{"type":"item.completed","item":{"id":"item_2","type":"command_execution","command":"sling run -r x.yaml","aggregated_output":"ok","status":"completed"}}`
	ctx.Transcript = "Reading additional input from stdin...\n" + real + "\n"
	if r := gradeTranscriptAbsent(ctx, "sling run"); r.Pass {
		t.Fatal("real sling run must fail transcript_absent")
	}
}

func TestCompileGraderRequiresObject(t *testing.T) {
	missing := []byte(`
source: MYSQL
target: POSTGRES
streams:
  mysql.users:
    mode: full-refresh
`)
	if _, err := CompileReplicationYAML(missing); err == nil {
		t.Fatal("missing object must fail compile grader")
	} else if !strings.Contains(err.Error(), "object") {
		t.Fatalf("want object error, got %v", err)
	}
	withDefaults := []byte(`
source: MYSQL
target: POSTGRES
defaults:
  object: public.{stream_table}
streams:
  mysql.users:
    mode: full-refresh
`)
	if _, err := CompileReplicationYAML(withDefaults); err != nil {
		t.Fatalf("defaults.object must pass: %v", err)
	}
}

func TestOutcomePreBlockOnE33(t *testing.T) {
	c, err := loadCase(filepath.Join(casesDir(), "e.33.repl_incremental_outcome", "case.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, spec := range c.Graders.Required {
		raw, ok := spec["outcome"]
		if !ok {
			continue
		}
		m, ok := asStringMap(raw)
		if !ok {
			t.Fatal("outcome must be a map")
		}
		pre, ok := asStringMap(m["pre"])
		if !ok {
			t.Fatal("e.33 outcome must have pre")
		}
		if cast.ToString(pre["connection"]) != "POSTGRES" {
			t.Fatalf("pre.connection=%v", pre["connection"])
		}
		if !strings.Contains(cast.ToString(pre["sql"]), "o_orderkey >= 900000000") {
			t.Fatalf("pre.sql=%v", pre["sql"])
		}
		found = true
	}
	if !found {
		t.Fatal("e.33 missing outcome grader")
	}
}

func TestMCPWiredForArm(t *testing.T) {
	if mcpWiredForArm("mock") {
		t.Fatal("mock MCP must be no")
	}
	for _, arm := range []string{"claude", "grok", "opencode2", "codex"} {
		if !mcpWiredForArm(arm) {
			t.Fatalf("%s MCP must be yes after default_tools_approval_mode=approve", arm)
		}
	}
}

func TestCountCodexMCPCalls(t *testing.T) {
	body := `Reading additional input from stdin...
{"type":"item.started","item":{"id":"item_2","type":"mcp_tool_call","status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_2","type":"mcp_tool_call","error":{"message":"MCP tool call requires approval, but approval policy is never"},"status":"failed"}}
{"type":"item.completed","item":{"id":"item_3","type":"mcp_tool_call","status":"failed"}}
`
	n, failed := countCodexMCPCalls(body)
	if n != 2 || failed != 2 {
		t.Fatalf("total=%d failed=%d", n, failed)
	}
	ok := `{"type":"item.completed","item":{"id":"item_1","type":"mcp_tool_call","status":"completed"}}`
	n, failed = countCodexMCPCalls(ok)
	if n != 1 || failed != 0 {
		t.Fatalf("ok total=%d failed=%d", n, failed)
	}
}

func TestEvalAgentSystemPromptHasDiscoveryRules(t *testing.T) {
	for _, want := range []string{
		"Never end with only a question",
		"If a named connection does not exist",
		"Delete scratch files",
		"Prefer skills, docs, and --help",
		"discover their columns",
		"search every configured DB connection",
	} {
		if !strings.Contains(evalAgentSystemPrompt, want) {
			t.Fatalf("prompt missing %q", want)
		}
	}
	if strings.Contains(evalAgentSystemPrompt, "Do not query live schemas") {
		t.Fatal("blanket schema ban must be dropped")
	}
}

func TestPatchCodexConfigMCPEnvToken(t *testing.T) {
	host := t.TempDir()
	t.Setenv("EVAL_HOST_HOME", host)
	t.Setenv("EVAL_CODEX_MODEL", "")
	_ = os.MkdirAll(filepath.Join(host, ".codex"), 0o755)

	readEnv := func(home string) map[string]any {
		doc, err := readTOMLMap(filepath.Join(home, ".codex", "config.toml"))
		if err != nil {
			t.Fatal(err)
		}
		mcp, _ := asMap(doc["mcp_servers"])
		slingSrv, ok := asMap(mcp["sling"])
		if !ok {
			t.Fatal("mcp_servers.sling missing")
		}
		env, _ := asMap(slingSrv["env"])
		return env
	}

	t.Setenv("SLING_CLI_TOKEN", "tok123")
	home := t.TempDir()
	if err := SeedHome(home, "home_codex", false); err != nil {
		t.Fatal(err)
	}
	if got := readEnv(home)["SLING_CLI_TOKEN"]; got != "tok123" {
		t.Fatalf("SLING_CLI_TOKEN=%v, want tok123", got)
	}

	t.Setenv("SLING_CLI_TOKEN", "")
	home2 := t.TempDir()
	if err := SeedHome(home2, "home_codex", false); err != nil {
		t.Fatal(err)
	}
	if _, ok := readEnv(home2)["SLING_CLI_TOKEN"]; ok {
		t.Fatal("SLING_CLI_TOKEN must be absent when host has no token")
	}
}

func TestClaudeMCPJSONTokenEnv(t *testing.T) {
	readSling := func() map[string]any {
		path := filepath.Join(t.TempDir(), ".mcp.json")
		if err := writeClaudeMCPJSON(path, "/opt/sling/sling"); err != nil {
			t.Fatal(err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		doc := map[string]any{}
		if err := json.Unmarshal(b, &doc); err != nil {
			t.Fatal(err)
		}
		servers, _ := asMap(doc["mcpServers"])
		slingSrv, _ := asMap(servers["sling"])
		return slingSrv
	}

	t.Setenv("SLING_CLI_TOKEN", "tok123")
	slingSrv := readSling()
	env, _ := asMap(slingSrv["env"])
	if env["SLING_CLI_TOKEN"] != "tok123" {
		t.Fatalf("env=%#v, want SLING_CLI_TOKEN=tok123", slingSrv["env"])
	}

	t.Setenv("SLING_CLI_TOKEN", "")
	slingSrv = readSling()
	if _, ok := slingSrv["env"]; ok {
		t.Fatal("empty token must not write an env map (it would shadow the inherited value)")
	}
}

func TestSeedTrialMCPTokenEnvGrokOpenCode2(t *testing.T) {
	work := t.TempDir()
	bin := "/opt/sling/sling"

	t.Setenv("SLING_CLI_TOKEN", "tok123")
	if err := seedTrialMCP(work, bin, "grok"); err != nil {
		t.Fatal(err)
	}
	gt, err := os.ReadFile(filepath.Join(work, ".grok", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gt), `env = { SLING_CLI_TOKEN = "tok123" }`) {
		t.Fatalf("grok mcp missing token env: %s", gt)
	}

	ocPath := filepath.Join(work, ".opencode", "opencode.json")
	if err := os.MkdirAll(filepath.Dir(ocPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ocPath, []byte(`{"mcp":{"servers":{}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := seedTrialMCP(work, bin, "opencode2"); err != nil {
		t.Fatal(err)
	}
	ob, err := os.ReadFile(ocPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := map[string]any{}
	if err := json.Unmarshal(ob, &doc); err != nil {
		t.Fatal(err)
	}
	mcp, _ := asMap(doc["mcp"])
	servers, _ := asMap(mcp["servers"])
	slingSrv, _ := asMap(servers["sling"])
	env, _ := asMap(slingSrv["environment"])
	if env["SLING_CLI_TOKEN"] != "tok123" {
		t.Fatalf("opencode2 environment=%#v, want SLING_CLI_TOKEN=tok123", slingSrv["environment"])
	}

	// Empty token: no env keys written for either arm.
	t.Setenv("SLING_CLI_TOKEN", "")
	work2 := t.TempDir()
	if err := seedTrialMCP(work2, bin, "grok"); err != nil {
		t.Fatal(err)
	}
	gt2, err := os.ReadFile(filepath.Join(work2, ".grok", "config.toml"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(gt2), "SLING_CLI_TOKEN") {
		t.Fatalf("grok mcp must not carry an empty token: %s", gt2)
	}
}

func TestEvalPromptRefusalFirst(t *testing.T) {
	refusal := strings.Index(evalAgentSystemPrompt, "If a named connection does not exist")
	probable := strings.Index(evalAgentSystemPrompt, "choose the most probable option")
	if refusal < 0 || probable < 0 {
		t.Fatalf("prompt missing rules: %q", evalAgentSystemPrompt)
	}
	if refusal > probable {
		t.Fatal("refusal rule must precede the choose-the-most-probable rule")
	}
}

func TestLicenseGateBlocked(t *testing.T) {
	fixture := `{"type":"item.completed","item":{"type":"mcp_tool_call","error":{"message":"unable to validate CLI Pro token. Please see https://docs.slingdata.io/sling-cli/cli-pro to enable "},"status":"failed"}}`
	if !licenseGateBlocked(fixture) {
		t.Fatal("Pro-token error must be detected")
	}
	if licenseGateBlocked(`{"type":"item.completed","item":{"type":"mcp_tool_call","status":"completed"}}`) {
		t.Fatal("clean transcript must not be flagged")
	}
}

func TestPersistTrialWorkExcludesMCPJSON(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	// .mcp.json carries SLING_CLI_TOKEN; it must never reach results/.
	if err := os.WriteFile(filepath.Join(src, ".mcp.json"), []byte(`{"mcpServers":{"sling":{"env":{"SLING_CLI_TOKEN":"tok"}}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	persistTrialWork(dst, src)
	if _, err := os.Stat(filepath.Join(dst, ".mcp.json")); err == nil {
		t.Fatal(".mcp.json must not be persisted (token leak)")
	}
}

func TestResultsWriterReplace(t *testing.T) {
	dir := t.TempDir()
	w, err := NewResultsWriter(dir, "testrun")
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append(TrialResult{Case: "e.1", Arm: "codex", Trial: 1, Pass: false})
	_ = w.Append(TrialResult{Case: "e.1", Arm: "codex", Trial: 2, Pass: false})
	_ = w.Append(TrialResult{Case: "e.2", Arm: "codex", Trial: 1, Pass: true})

	if err := w.Replace(TrialResult{Case: "e.1", Arm: "codex", Trial: 2, Pass: true, Retried: true}); err != nil {
		t.Fatal(err)
	}
	rows, err := LoadJSONL(w.Path())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d", len(rows))
	}
	got := rows[1]
	if !got.Pass || !got.Retried || got.Run != "testrun" {
		t.Fatalf("replaced row wrong: %+v", got)
	}
	if err := w.Replace(TrialResult{Case: "e.9", Arm: "codex", Trial: 1}); err == nil {
		t.Fatal("expected error for missing row")
	}
}

func TestParseFlagsRetryFailed(t *testing.T) {
	f := ParseFlags([]string{"--arms", "codex"})
	if !f.RetryFailed {
		t.Fatal("retry-failed must default on")
	}
	f = ParseFlags([]string{"--retry-failed=false"})
	if f.RetryFailed {
		t.Fatal("retry-failed=false must stick")
	}
}
