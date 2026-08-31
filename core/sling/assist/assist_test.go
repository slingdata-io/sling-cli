// Package tests: bugs, harness gaps, and extensibility seams.

package assist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core"
)

func TestTryRefreshLockExclusiveAndTokenUnlock(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, ".refresh-lock")

	unlock1, ok := tryRefreshLock(lockPath)
	if !ok {
		t.Fatal("first lock should succeed")
	}
	if _, ok := tryRefreshLock(lockPath); ok {
		t.Fatal("second lock should fail while first is held")
	}
	unlock1()
	unlock2, ok := tryRefreshLock(lockPath)
	if !ok {
		t.Fatal("lock after unlock should succeed")
	}
	unlock2()
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lock file should be removed after unlock, err=%v", err)
	}
}

func TestTryRefreshLockReclaimsStale(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, ".refresh-lock")
	if err := os.WriteFile(lockPath, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := time.Now().Add(-refreshLockStale - time.Minute)
	if err := os.Chtimes(lockPath, stale, stale); err != nil {
		t.Fatal(err)
	}
	unlock, ok := tryRefreshLock(lockPath)
	if !ok {
		t.Fatal("stale lock should be reclaimable")
	}
	unlock()
}

func TestPruneRetiredSkillsRemovesStaleDirs(t *testing.T) {
	withTempHomeDir(t)
	root := CanonicalSkillsDir()
	stale := filepath.Join(root, "sling-hooks")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "SKILL.md"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(root, "sling-pipelines")
	if err := os.MkdirAll(keep, 0o755); err != nil {
		t.Fatal(err)
	}

	pruneRetiredSkills(context.Background(), ScopeUser)

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("retired skill dir %s should be removed", stale)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("current skill dir %s should remain: %v", keep, err)
	}
}

func TestUninstallMarksStampSoAutoRefreshSkips(t *testing.T) {
	withTempHomeDir(t)
	// Simulate a prior install stamp, then uninstall skills.
	if err := os.MkdirAll(AssistDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(VersionFilePath(), []byte("0.0.0-old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Uninstall(context.Background(), UninstallOptions{NonInteractive: true}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(VersionFilePath())
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) != versionUninstalled {
		t.Fatalf("stamp = %q, want %q", got, versionUninstalled)
	}
	notice, err := AutoRefresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if notice != "" {
		t.Fatalf("AutoRefresh after uninstall should no-op, got notice %q", notice)
	}
}

func TestAutoRefreshNoopsWhenNeverInstalled(t *testing.T) {
	withTempHomeDir(t)
	// No skills on disk → never installed; must not write the bundle.
	notice, err := AutoRefresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if notice != "" {
		t.Fatalf("AutoRefresh with no skills should no-op, got %q", notice)
	}
	for _, name := range listSkillNames() {
		if _, err := os.Stat(canonicalSkillPath(name)); !os.IsNotExist(err) {
			t.Fatalf("should not install %s when no skills exist", name)
		}
	}
}

func TestAutoRefreshNoopsWhenStampStaleButNoSkills(t *testing.T) {
	withTempHomeDir(t)
	if err := os.MkdirAll(AssistDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(VersionFilePath(), []byte("0.0.0-old"), 0o644); err != nil {
		t.Fatal(err)
	}
	notice, err := AutoRefresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if notice != "" {
		t.Fatalf("AutoRefresh with no skills should no-op, got %q", notice)
	}
	for _, name := range listSkillNames() {
		if _, err := os.Stat(canonicalSkillPath(name)); !os.IsNotExist(err) {
			t.Fatalf("should not install %s from a stale stamp", name)
		}
	}
}

func TestAutoRefreshHealsDriftWhenStampCurrent(t *testing.T) {
	withTempHomeDir(t)
	names := listSkillNames()
	if len(names) == 0 {
		t.Fatal("no embedded skills")
	}
	if err := writeCanonicalBundle(names); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(AssistDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(VersionFilePath(), []byte(core.Version), 0o644); err != nil {
		t.Fatal(err)
	}

	name := names[0]
	skillPath := canonicalSkillPath(name)
	if err := os.WriteFile(skillPath, []byte("drifted-content"), 0o644); err != nil {
		t.Fatal(err)
	}

	notice, err := AutoRefresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if notice == "" {
		t.Fatal("expected refresh notice after drift")
	}

	got, err := os.ReadFile(skillPath)
	if err != nil {
		t.Fatal(err)
	}
	want, err := SkillsFS.ReadFile(filepath.ToSlash(filepath.Join("skills", name, "SKILL.md")))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("skill %s still drifted after AutoRefresh", name)
	}
}

func TestAutoRefreshPrunesRetiredWhenStampCurrent(t *testing.T) {
	withTempHomeDir(t)
	names := listSkillNames()
	if err := writeCanonicalBundle(names); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(AssistDir(), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(VersionFilePath(), []byte(core.Version), 0o644); err != nil {
		t.Fatal(err)
	}

	stale := filepath.Join(CanonicalSkillsDir(), "sling-hooks")
	if err := os.MkdirAll(stale, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "SKILL.md"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	notice, err := AutoRefresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if notice == "" {
		t.Fatal("expected refresh notice after prune")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("retired skill dir %s should be removed", stale)
	}
}

func TestAgentExitErrorExitCodeOf(t *testing.T) {
	err := &AgentExitError{ExitCode: 42, Agent: "claude"}
	code, ok := ExitCodeOf(err)
	if !ok || code != 42 {
		t.Fatalf("ExitCodeOf = %d, %v", code, ok)
	}
	if _, ok := ExitCodeOf(fmt.Errorf("other")); ok {
		t.Fatal("non-agent error should not match")
	}
}

func TestClaudeProjectMCPPath(t *testing.T) {
	c := &claudeClient{}
	got := c.mcpPath(ScopeProject)
	if got != filepath.Join(".", ".mcp.json") && got != ".mcp.json" {
		// filepath.Join(".", ".mcp.json") is "./.mcp.json" on Unix
		if !strings.HasSuffix(got, ".mcp.json") || strings.Contains(got, ".claude.json") {
			t.Fatalf("project mcp path = %q", got)
		}
	}
	if strings.Contains(c.mcpPath(ScopeProject), ".claude.json") {
		t.Fatalf("project scope must not use .claude.json: %q", c.mcpPath(ScopeProject))
	}
}

func TestVSCodeMCPUsesServersKey(t *testing.T) {
	// Unit-level: path helper for project scope.
	c := &vscodeClient{}
	p := c.vscodeMCPPath(ScopeProject)
	if !strings.Contains(p, ".vscode") || !strings.HasSuffix(p, "mcp.json") {
		t.Fatalf("project vscode mcp path = %q", p)
	}
}

func TestAgentLaunchArgsPerAgent(t *testing.T) {
	path := "/tmp/prompt.md"
	cases := []struct {
		agent    string
		wantArgs []string
		stdin    bool
	}{
		{"codex", []string{"exec", "-"}, true},
		{"gemini", []string{"-p", "-"}, true},
		{"claude", []string{"Read and execute the task in @" + path}, false},
		{"cursor", []string{path}, false},
		{"grok", []string{"Read and execute the task in @" + path}, false},
		{"pi", []string{"-p"}, true},
		{"opencode", []string{"run", "--file", path, "Read and execute the attached task"}, false},
		{"unknown-cli", nil, true},
	}
	for _, tc := range cases {
		p := agentLaunchArgs(tc.agent, path, "", "")
		if p.UseStdin != tc.stdin {
			t.Errorf("%s UseStdin=%v want %v", tc.agent, p.UseStdin, tc.stdin)
		}
		if len(p.Args) != len(tc.wantArgs) {
			t.Errorf("%s args=%v want %v", tc.agent, p.Args, tc.wantArgs)
			continue
		}
		for i := range tc.wantArgs {
			if p.Args[i] != tc.wantArgs[i] {
				t.Errorf("%s args[%d]=%q want %q", tc.agent, i, p.Args[i], tc.wantArgs[i])
			}
		}
	}
}

func TestResolveAgentOverrideWins(t *testing.T) {
	// Need a detectable agent dir so override succeeds.
	home := t.TempDir()
	prevHome := os.Getenv("HOME")
	os.Setenv("HOME", home)
	t.Cleanup(func() { os.Setenv("HOME", prevHome) })
	// claude Detect() requires the binary on $PATH
	if err := os.MkdirAll(filepath.Join(home, ".claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Put claude on PATH via a stub.
	bin := filepath.Join(home, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	stub := filepath.Join(bin, "claude")
	if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	prevPath := os.Getenv("PATH")
	os.Setenv("PATH", bin+string(os.PathListSeparator)+prevPath)
	t.Cleanup(func() { os.Setenv("PATH", prevPath) })

	got, err := ResolveAgent("claude", Profile{Agent: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	if got != "claude" {
		t.Fatalf("override should win: got %q", got)
	}
}

func TestResolveAgentUnknownOverride(t *testing.T) {
	_, err := ResolveAgent("not-an-agent", Profile{})
	if err == nil || !strings.Contains(err.Error(), "unknown agent") {
		t.Fatalf("err = %v", err)
	}
}

func TestResolveAgentProfileNonLaunchable(t *testing.T) {
	_, err := ResolveAgent("", Profile{Agent: "vscode"})
	if err == nil || !strings.Contains(err.Error(), "non-launchable") {
		t.Fatalf("err = %v", err)
	}
}

func TestSlugify(t *testing.T) {
	if got := slugify("Hello World!"); got != "hello-world" {
		t.Fatalf("got %q", got)
	}
	if got := slugify("   "); got != "entry" {
		t.Fatalf("empty → entry, got %q", got)
	}
	if got := slugify("a / b"); got != "a-b" {
		t.Fatalf("separator runs must collapse, got %q", got)
	}
}

func TestSlugifyCapsLength(t *testing.T) {
	long := "Help me create or update a Sling config (replication, pipeline, model, or API spec). First ask me which one."
	got := slugify(long)
	if len(got) > maxSlugLen {
		t.Fatalf("slug %q is %d chars, want <= %d", got, len(got), maxSlugLen)
	}
	if strings.HasSuffix(got, "-") || strings.HasPrefix(got, "-") {
		t.Fatalf("slug must not have dangling separators: %q", got)
	}
	// Cut on a word boundary: every kept word is whole.
	want := "help-me-create-or-update-a-sling-config"
	if got != want {
		t.Fatalf("slug = %q, want %q", got, want)
	}
}

func TestSaveEntryIDStaysShort(t *testing.T) {
	withTempHomeDir(t)
	a := AnswersFile{
		Name: slugify("Help me create or update a Sling config (replication, pipeline, model, or API spec)"),
		Task: "open",
	}
	id, err := SaveEntry(a, "prompt", Meta{Task: "open"})
	if err != nil {
		t.Fatal(err)
	}
	// <timestamp 19> + "_" + slug
	if len(id) > 20+maxSlugLen {
		t.Fatalf("id %q is %d chars", id, len(id))
	}
	if _, err := LoadEntry(id); err != nil {
		t.Fatalf("round-trip: %v", err)
	}
}

func TestFilterEntriesQuery(t *testing.T) {
	entries := []Entry{
		{ID: "one", Answers: AnswersFile{Task: "replication.create", Agent: "claude", Name: "pg-to-sf"}},
		{ID: "two", Answers: AnswersFile{Task: "pipeline.create", Agent: "codex", Name: "daily"}},
	}
	got := filterEntries(entries, "pg-to")
	if len(got) != 1 || got[0].ID != "one" {
		t.Fatalf("got %+v", got)
	}
	got = filterEntries(entries, "codex")
	if len(got) != 1 || got[0].ID != "two" {
		t.Fatalf("got %+v", got)
	}
	got = filterEntries(entries, "")
	if len(got) != 2 {
		t.Fatalf("empty query len=%d", len(got))
	}
}

func TestSessionPrintWithoutSetup(t *testing.T) {
	withTempHomeDir(t)
	_, err := Session(SessionOptions{Print: true, Ask: "from PG"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSessionLaunchWithoutSetupFails(t *testing.T) {
	withTempHomeDir(t)
	_, err := Session(SessionOptions{Headless: true, Ask: "from PG"})
	if err == nil {
		t.Fatal("expected setup error")
	}
	if !strings.Contains(err.Error(), "sling assist setup") {
		t.Fatalf("want setup hint, got: %v", err)
	}
}

func TestPickerRendersOldTaskAndNewMode(t *testing.T) {
	entries := []Entry{
		{ID: "old", Answers: AnswersFile{Task: "replication.create", Name: "pg-to-sf", Created: time.Now()}},
		{ID: "open1", Answers: AnswersFile{Task: "open", Name: "backfill", Created: time.Now()}},
		{ID: "ask1", Answers: AnswersFile{Task: "ask", Name: "hello", Created: time.Now()}},
	}
	got := newPickerModel(entries).View()
	for _, w := range []string{"replication", "open", "ask"} {
		if !strings.Contains(got, w) {
			t.Errorf("missing %q\n%s", w, got)
		}
	}
}

func TestDoctorReportToJSONTyped(t *testing.T) {
	withTempHomeDir(t)
	r, err := Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("nil report")
	}
	if r.SlingVersion == "" {
		t.Fatal("missing sling_version")
	}
	if len(r.Findings) == 0 {
		t.Fatal("expected findings")
	}
	// Findings must not use glyph-prefixed prose as the only structure.
	for _, f := range r.Findings {
		if f.ID == "" {
			t.Fatalf("finding missing id: %+v", f)
		}
		if strings.HasPrefix(f.Summary, "✓") || strings.HasPrefix(f.Summary, "✗") {
			t.Fatalf("summary still has glyph: %q", f.Summary)
		}
	}
	body, err := r.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, ok := decoded["findings"]; !ok {
		t.Fatalf("json missing findings: %s", body)
	}
	if _, ok := decoded["sling_version"]; !ok {
		t.Fatalf("json missing sling_version: %s", body)
	}
	// Lines must not appear in JSON (json:"-").
	if _, ok := decoded["Lines"]; ok {
		t.Fatal("Lines should not be in JSON")
	}
	if _, ok := decoded["lines"]; ok {
		t.Fatal("lines should not be in JSON")
	}
}

func TestDoctorContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Doctor(ctx)
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestCellStateJSON(t *testing.T) {
	m := DoctorMatrix{
		Clients: []string{"claude"},
		Rows: []MatrixRow{{
			Label: "MCP",
			Cells: map[string]CellState{"claude": CellOK},
		}},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"ok"`) {
		t.Fatalf("cell state not stringified: %s", b)
	}
}

func TestCheckResultRender(t *testing.T) {
	r := checkSkill(CellFail, "sling", "redirect missing")
	line := r.Render("claude")
	if !strings.HasPrefix(line, "✗") {
		t.Fatalf("glyph: %q", line)
	}
	if !strings.Contains(line, "claude") || !strings.Contains(line, "sling") {
		t.Fatalf("line: %q", line)
	}
}

func TestPathsOverrideAffectsSlingHome(t *testing.T) {
	dir := t.TempDir()
	restore := SetPaths(Paths{SlingHome: dir, UserHome: dir, CWD: dir})
	defer restore()
	if !strings.HasPrefix(LogsRoot(), dir) {
		t.Fatalf("LogsRoot=%q not under %q", LogsRoot(), dir)
	}
	if !strings.HasPrefix(AssistDir(), dir) {
		t.Fatalf("AssistDir=%q not under %q", AssistDir(), dir)
	}
	if !strings.HasPrefix(ErrorsDir(), dir) {
		t.Fatalf("ErrorsDir=%q not under %q", ErrorsDir(), dir)
	}
	if userHome() != dir {
		t.Fatalf("userHome=%q want %q", userHome(), dir)
	}
}

func TestInstallRespectsCanceledContext(t *testing.T) {
	withTempHomeDir(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Install(ctx, InstallOptions{NonInteractive: true, DefaultAgent: "claude"})
	if err == nil {
		t.Fatal("expected cancel error")
	}
}

func TestCodexMCPSectionLineAnchored(t *testing.T) {
	// Mention in a comment or string must not count.
	if hasCodexMCPSection(`# see [mcp_servers.sling] docs
name = "x"
`) {
		t.Fatal("comment mention should not count")
	}
	body := `
[mcp_servers.other]
command = "x"

[mcp_servers.sling]
command = "sling"
args = ["serve", "mcp"]

[mcp_servers.sling.env]
FOO = "bar"
`
	if !hasCodexMCPSection(body) {
		t.Fatal("expected section present")
	}
	out := removeCodexMCP(body)
	if hasCodexMCPSection(out) {
		t.Fatalf("header still present after remove:\n%s", out)
	}
	if strings.Contains(out, "[mcp_servers.sling.env]") {
		t.Fatalf("orphan subtable left behind:\n%s", out)
	}
	if !strings.Contains(out, "[mcp_servers.other]") {
		t.Fatalf("sibling section removed:\n%s", out)
	}
}

func TestBackupPreservesSourceMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret.json")
	if err := os.WriteFile(path, []byte(`{"a":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := backupBeforeEdit(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path + backupSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("backup mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestResumePrintsSavedPrompt(t *testing.T) {
	withTempHomeDir(t)
	a := AnswersFile{
		Name:    "edit-me",
		Task:    "replication.update",
		Created: time.Now().UTC(),
		Agent:   "claude",
		Answers: map[string]any{"ask": "add incremental mode"},
	}
	id, err := SaveEntry(a, "saved prompt body\n", Meta{Task: a.Task, Agent: "claude", HarnessSessionID: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	old := assistOut
	var buf bytes.Buffer
	assistOut = &buf
	t.Cleanup(func() { assistOut = old })
	_, err = Session(SessionOptions{ResumeID: id, Print: true})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "saved prompt body") {
		t.Fatalf("got %q", buf.String())
	}
}

func TestProjectRootFindsGit(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	restore := SetPaths(Paths{SlingHome: dir, UserHome: dir, CWD: sub})
	defer restore()
	if got := projectRoot(); got != dir {
		t.Fatalf("projectRoot = %q, want %q", got, dir)
	}
}

func TestSyncCanonicalSkillPrunesStale(t *testing.T) {
	withTempHomeDir(t)
	// Write bundle then plant a stale file under a skill dir.
	skills := listSkillNames()
	if len(skills) == 0 {
		t.Fatal("no embedded skills")
	}
	name := skills[0]
	if _, err := syncCanonicalSkill(name); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(CanonicalSkillsDir(), name, "STALE_DO_NOT_KEEP.md")
	if err := os.WriteFile(stale, []byte("gone"), 0o644); err != nil {
		t.Fatal(err)
	}
	changed, err := syncCanonicalSkill(name)
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected change when pruning stale file")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale file still present: %v", err)
	}
}

func TestClaudeLaunchArgsAssignSessionAndModel(t *testing.T) {
	path := "/tmp/prompt.md"
	sid := "550e8400-e29b-41d4-a716-446655440000"
	p := agentLaunchArgs("claude", path, "sonnet", sid)
	joined := strings.Join(p.Args, " ")
	if !strings.Contains(joined, "--session-id "+sid) {
		t.Fatalf("missing session-id: %v", p.Args)
	}
	if !strings.Contains(joined, "--model sonnet") {
		t.Fatalf("missing model: %v", p.Args)
	}
}

func TestCodexLaunchArgsInsertModelAfterSubcommand(t *testing.T) {
	p := agentLaunchArgs("codex", "/tmp/p.md", "sonnet", "")
	if len(p.Args) < 3 || p.Args[0] != "exec" || p.Args[1] != "-m" || p.Args[2] != "sonnet" {
		t.Fatalf("codex args=%v", p.Args)
	}
}

func TestResumeArgsHaveNoPrompt(t *testing.T) {
	cases := []struct {
		agent string
		id    string
		want  []string
	}{
		{"claude", "u1", []string{"--resume", "u1"}},
		{"grok", "u2", []string{"--resume", "u2"}},
		{"codex", "u3", []string{"resume", "u3"}},
		{"gemini", "u4", []string{"--resume", "u4"}},
		{"cursor", "u5", []string{"--resume=u5"}},
		{"opencode", "u6", []string{"--session", "u6"}},
		{"pi", "u7", []string{"--session", "u7"}},
	}
	for _, tc := range cases {
		p := agentResumeArgs(tc.agent, tc.id, "")
		if strings.Join(p.Args, " ") != strings.Join(tc.want, " ") {
			t.Errorf("%s args=%v want %v", tc.agent, p.Args, tc.want)
		}
		if p.UseStdin {
			t.Errorf("%s resume should not use stdin", tc.agent)
		}
	}
	p := agentResumeArgs("claude", "u1", "sonnet")
	if !strings.Contains(strings.Join(p.Args, " "), "--model sonnet") {
		t.Fatalf("claude resume missing model: %v", p.Args)
	}
	p = agentResumeArgs("codex", "u3", "sonnet")
	if len(p.Args) < 4 || p.Args[0] != "resume" || p.Args[1] != "-m" || p.Args[2] != "sonnet" {
		t.Fatalf("codex resume args=%v", p.Args)
	}
}

func TestDiscoverHarnessSessionIDNewFile(t *testing.T) {
	home := t.TempDir()
	restore := SetPaths(Paths{SlingHome: home, UserHome: home, CWD: home})
	t.Cleanup(restore)
	root := filepath.Join(home, ".codex", "sessions")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(root, "old.jsonl")
	if err := os.WriteFile(old, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := snapshotHarnessFiles("codex")
	newID := "019dd4bf-0929-7ea0-b227-1f51085e7d71"
	if err := os.WriteFile(filepath.Join(root, newID+".jsonl"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := discoverHarnessSessionID("codex", before)
	if got != newID {
		t.Fatalf("got %q want %q", got, newID)
	}
}

func TestAgentBinaryCursorIsAgent(t *testing.T) {
	if agentBinary("cursor") != "cursor-agent" {
		t.Fatal(agentBinary("cursor"))
	}
	if agentBinary("claude") != "claude" {
		t.Fatal(agentBinary("claude"))
	}
}

func TestDoctorHonorsScope(t *testing.T) {
	withTempHomeDir(t)
	// Doctor with ScopeProject should not panic and should return a report.
	r, err := Doctor(context.Background(), DoctorOptions{Scope: ScopeProject})
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("nil report")
	}
}

func TestEnsureAssistReadyRequiresProfile(t *testing.T) {
	withTempHomeDir(t)
	err := EnsureAssistReady()
	if err == nil {
		t.Fatal("expected error when assist not set up")
	}
	if !strings.Contains(err.Error(), "sling assist setup") {
		t.Fatalf("error should point at setup: %v", err)
	}
}
