// Client adapter tests: JSONC preserve, profile YAML comments, backups.

package assist

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/tidwall/gjson"
)

// withTempHomeDir overrides env.HomeDir for the duration of the test. The
// package-level var is set during init() from $SLING_HOME_DIR — for tests we
// need both updated so envFilePath() picks up the temp dir.
func withTempHomeDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	// Clear the nested-launch markers. Without this, a test run from inside
	// a CLI agent makes NestedLaunch() true and Session prints instead of launching.
	for _, k := range []string{"CLAUDECODE", "CURSOR_TRACE_ID", "OPENCODE", "OPENCODE_SESSION"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
	prev := env.HomeDir
	env.HomeDir = dir
	// Override path seam so client adapters (userHome) and Sling dirs (slingHome)
	// both resolve under the temp tree.
	restore := SetPaths(Paths{SlingHome: dir, UserHome: dir, CWD: dir})
	t.Cleanup(func() {
		env.HomeDir = prev
		restore()
	})
	return dir
}

// TestJSONPreservesComments verifies that a JSONC document with `//` and `/* */`
// comments survives a round-trip through setJSONPath/deleteJSONPath. This is
// the bug that bit us with VS Code's settings.json, where the previous map
// rewrite silently stripped every comment in the file.
func TestJSONPreservesComments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")

	original := `{
  // top comment about the user's editor
  "editor.fontSize": 14,
  /* block comment
     spanning two lines */
  "editor.fontFamily": "JetBrains Mono", // trailing comment
  "files.exclude": {
    "**/.git": true // hide git
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := setJSONPath(path, `chat\.instructionsFilesLocations.-1`, "/Users/me/.agents/skills"); err != nil {
		t.Fatalf("setJSONPath: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := string(got)

	wantSubstrings := []string{
		"// top comment about the user's editor",
		"/* block comment",
		"spanning two lines */",
		"// trailing comment",
		"// hide git",
		`"editor.fontSize": 14`,
		`"editor.fontFamily": "JetBrains Mono"`,
		`"chat.instructionsFilesLocations"`,
		`"/Users/me/.agents/skills"`,
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(out, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, out)
		}
	}
}

// TestJSONDeletePreservesComments verifies that deleting a key still keeps
// surrounding comments intact.
func TestJSONDeletePreservesComments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")

	original := `{
  // keep this comment
  "editor.fontSize": 14,
  "mcpServers": {
    "sling": { "command": "sling", "args": ["serve", "mcp"] },
    "other": { "command": "other" }
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := deleteJSONPath(path, "mcpServers.sling"); err != nil {
		t.Fatalf("deleteJSONPath: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)

	if !strings.Contains(out, "// keep this comment") {
		t.Errorf("expected comment to be preserved\n--- got ---\n%s", out)
	}
	if strings.Contains(out, `"sling"`) {
		t.Errorf("expected sling entry to be deleted\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, `"other"`) {
		t.Errorf("expected sibling entry preserved\n--- got ---\n%s", out)
	}
}

// TestSaveProfilePreservesYAMLComments verifies that adding the
// env.SLING_ASSIST entry to an existing env.yaml doesn't blow away the user's
// comments and unrelated keys. This is the analogue of
// TestJSONPreservesComments for YAML.
func TestSaveProfilePreservesYAMLComments(t *testing.T) {
	homeDir := withTempHomeDir(t)

	envFile := filepath.Join(homeDir, "env.yaml")
	original := `# Sling environment file — managed by you.
# These connections are used by replications and pipelines.

connections:
  # Production warehouse
  PG_PROD:
    type: postgres
    host: db.example.com
    user: app
  # Staging warehouse
  PG_STAGE:
    type: postgres
    host: stage.db.example.com

# Variables shared across runs
variables:
  region: us-west-2
`
	if err := os.WriteFile(envFile, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	prof := DefaultProfile()
	prof.Agent = "claude"
	if err := SaveProfile(prof); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}

	got, _ := os.ReadFile(envFile)
	out := string(got)

	wantSubstrings := []string{
		"# Sling environment file — managed by you.",
		"# These connections are used by replications and pipelines.",
		"# Production warehouse",
		"# Staging warehouse",
		"PG_PROD:",
		"PG_STAGE:",
		"region: us-west-2",
		"SLING_ASSIST:",
		"agent: claude",
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(out, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, out)
		}
	}
	// Legacy `variables:` migrates to `env:` on save — the block contents
	// (region: us-west-2) survive, but the heading comment attached to the
	// renamed key is dropped along with the old key.
	if strings.Contains(out, "variables:") {
		t.Errorf("expected legacy variables: block to be renamed to env:\n--- got ---\n%s", out)
	}

	// Idempotency: a second save should leave comments intact and not duplicate
	// the SLING_ASSIST entry.
	prof.HintInErrors = false
	if err := SaveProfile(prof); err != nil {
		t.Fatalf("second SaveProfile: %v", err)
	}
	got, _ = os.ReadFile(envFile)
	out = string(got)

	if strings.Count(out, "SLING_ASSIST:") != 1 {
		t.Errorf("expected exactly one SLING_ASSIST entry, got\n%s", out)
	}
	if !strings.Contains(out, "# Production warehouse") {
		t.Errorf("comments lost on second save\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, "hint_in_errors: false") {
		t.Errorf("SLING_ASSIST entry did not update on second save\n--- got ---\n%s", out)
	}
}

// TestJSONLeadingCommentBanner reproduces the real-world bug where VS Code's
// settings.json has a `// Place your settings...` banner *before* the opening
// `{`. sjson can't parse that prefix and rebuilds the document as a single
// compact line, blowing away the user's config. Our fix strips the leading
// non-JSON content before handing the buffer to sjson, so the body survives.
func TestJSONLeadingCommentBanner(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")

	// Mimics the real shape of VS Code's user settings file — leading banner,
	// many keys, multi-line.
	original := `// Place your settings in this file to overwrite the default settings
{
  "editor.tabSize": 2,
  "editor.detectIndentation": false,
  "editor.guides.indentation": false,
  "editor.formatOnSave": true,
  "workbench.colorTheme": "Default Dark Modern",
  "files.autoSave": "afterDelay",
  "git.autofetch": true,
  "terminal.integrated.fontSize": 13
}
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := setJSONPath(path, `chat\.instructionsFilesLocations.-1`, "/Users/me/.agents/skills"); err != nil {
		t.Fatalf("setJSONPath: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)

	// Must still contain every original key.
	wantKeys := []string{
		`"editor.tabSize"`,
		`"editor.detectIndentation"`,
		`"editor.guides.indentation"`,
		`"editor.formatOnSave"`,
		`"workbench.colorTheme"`,
		`"files.autoSave"`,
		`"git.autofetch"`,
		`"terminal.integrated.fontSize"`,
		`"chat.instructionsFilesLocations"`,
	}
	for _, k := range wantKeys {
		if !strings.Contains(out, k) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", k, out)
		}
	}

	// And the file should not have collapsed to one line.
	if strings.Count(out, "\n") < 5 {
		t.Errorf("file collapsed to a single line, only %d newlines\n--- got ---\n%s",
			strings.Count(out, "\n"), out)
	}

	// Leading banner must survive — it carries useful context the user wrote
	// (or VS Code wrote on their behalf).
	if !strings.HasPrefix(out, "// Place your settings in this file") {
		t.Errorf("leading banner was dropped\n--- got ---\n%s", out)
	}
}

// TestJSONBackupCreatedBeforeEdit verifies setJSONPath writes <path>.backup
// before mutating the original. The user can always recover from .backup if
// something goes wrong on the next install.
func TestJSONBackupCreatedBeforeEdit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "settings.json")

	original := `{
  "editor.fontSize": 14,
  "workbench.colorTheme": "Default Dark Modern"
}`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := setJSONPath(path, `chat\.instructionsFilesLocations.-1`, "/x"); err != nil {
		t.Fatalf("setJSONPath: %v", err)
	}

	bk, err := os.ReadFile(path + ".backup")
	if err != nil {
		t.Fatalf("expected backup file at %s: %v", path+".backup", err)
	}
	if string(bk) != original {
		t.Errorf("backup didn't match original\n--- backup ---\n%s\n--- want ---\n%s", string(bk), original)
	}
}

// TestJSONNoBackupWhenSourceMissing ensures we don't create an empty
// <path>.backup when the file we're about to edit doesn't exist yet.
func TestJSONNoBackupWhenSourceMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fresh.json")

	if err := setJSONPath(path, "mcpServers.sling", map[string]any{"command": "sling"}); err != nil {
		t.Fatalf("setJSONPath: %v", err)
	}
	if _, err := os.Stat(path + ".backup"); !os.IsNotExist(err) {
		t.Errorf("expected no backup for fresh-file write, got err=%v", err)
	}
}

// TestJSONDestructiveEditRefused simulates an edit that drops top-level keys
// (e.g. a mangled sjson rewrite) and verifies the helper refuses to commit it.
// We trigger this by directly invoking the validator with mocked before/after
// snapshots.
func TestJSONDestructiveEditRefused(t *testing.T) {
	before := []byte(`{
  "a": 1,
  "b": 2,
  "c": 3,
  "d": 4
}`)
	// Mimic an sjson-trip-on-banner result: the whole body collapsed into a
	// single key, single line.
	after := []byte(`{"only":"survivor"}`)

	if err := validateEditNotDestructive(before, after, 0); err == nil {
		t.Errorf("expected validateEditNotDestructive to reject a destructive edit")
	}

	// And a single-key delete on the same document should be allowed when
	// allowKeyDelta=1.
	deleted := []byte(`{
  "a": 1,
  "b": 2,
  "c": 3
}`)
	if err := validateEditNotDestructive(before, deleted, 1); err != nil {
		t.Errorf("expected single-key delete to be allowed: %v", err)
	}
}

// TestSaveProfileFreshFile covers the empty-file path: when env.yaml doesn't
// exist yet, SaveProfile should create it with just env.SLING_ASSIST.
func TestSaveProfileFreshFile(t *testing.T) {
	homeDir := withTempHomeDir(t)

	prof := DefaultProfile()
	prof.Agent = "codex"
	if err := SaveProfile(prof); err != nil {
		t.Fatalf("SaveProfile: %v", err)
	}

	got, _ := os.ReadFile(filepath.Join(homeDir, "env.yaml"))
	out := string(got)

	if !strings.Contains(out, "SLING_ASSIST:") {
		t.Errorf("missing SLING_ASSIST entry in fresh file\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, "agent: codex") {
		t.Errorf("missing agent: codex\n--- got ---\n%s", out)
	}
}

// ---- opencode / pi / grok adapters ----

// TestNewClientsRegistered verifies the three additions are in the canonical
// list, are launchable CLI agents, and resolve by name.
func TestNewClientsRegistered(t *testing.T) {
	for _, name := range []string{"opencode", "pi", "grok"} {
		c := LookupClient(name)
		if c == nil {
			t.Fatalf("LookupClient(%q) returned nil", name)
		}
		if c.Kind() != KindCLIAgent {
			t.Errorf("%s: expected KindCLIAgent, got %v", name, c.Kind())
		}
		found := false
		for _, a := range CLIAgents() {
			if a.Name() == name {
				found = true
			}
		}
		if !found {
			t.Errorf("%s missing from CLIAgents()", name)
		}
	}
}

// TestOpencodeMCPRoundTrip covers opencode's distinctive MCP shape: a `mcp`
// (not `mcpServers`) block whose entries carry `type: local` and a single
// argv array rather than command/args.
func TestOpencodeMCPRoundTrip(t *testing.T) {
	home := withTempHomeDir(t)
	ctx := context.Background()
	c := &opencodeClient{}

	path := filepath.Join(home, ".config", "opencode", "opencode.json")
	if got := c.configPath(ScopeUser); got != path {
		t.Fatalf("configPath = %s, want %s", got, path)
	}

	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellFail {
		t.Errorf("expected fail before write, got %v", res.State)
	}
	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("WriteMCP: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	out := string(data)
	if got := gjson.GetBytes(data, "mcp.sling.type").String(); got != "local" {
		t.Errorf("type = %q, want \"local\"\n--- got ---\n%s", got, out)
	}
	// command must be a flat argv array, not the command/args pair.
	if got := gjson.GetBytes(data, "mcp.sling.command").String(); got != `["sling","serve","mcp"]` {
		t.Errorf("command = %q, want [\"sling\",\"serve\",\"mcp\"]\n--- got ---\n%s", got, out)
	}
	if !gjson.GetBytes(data, "mcp.sling.enabled").Bool() {
		t.Errorf("expected enabled: true\n--- got ---\n%s", out)
	}
	if gjson.GetBytes(data, "mcp.sling.args").Exists() {
		t.Errorf("opencode entry should not use args key\n--- got ---\n%s", out)
	}
	if got := gjson.GetBytes(data, "mcp.agent-browser.type").String(); got != "local" {
		t.Errorf("agent-browser type = %q, want \"local\"\n--- got ---\n%s", got, out)
	}
	if !strings.Contains(gjson.GetBytes(data, "mcp.agent-browser.command").Raw, `"mcp"`) {
		t.Errorf("agent-browser command missing mcp\n--- got ---\n%s", out)
	}

	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellOK {
		t.Errorf("expected ok after write, got %v (%s)", res.State, res.Note)
	}
	if err := c.RemoveMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("RemoveMCP: %v", err)
	}
	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellFail {
		t.Errorf("expected fail after remove, got %v", res.State)
	}
}

// TestOpencodeMCPPreservesSiblings ensures we only touch mcp.sling and leave
// the user's other opencode settings (and their comments) alone.
func TestOpencodeMCPPreservesSiblings(t *testing.T) {
	home := withTempHomeDir(t)
	ctx := context.Background()
	path := filepath.Join(home, ".config", "opencode", "opencode.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	original := `{
  // my opencode setup
  "$schema": "https://opencode.ai/config.json",
  "theme": "tokyonight",
  "mcp": {
    "other": { "type": "local", "command": ["other"] }
  }
}`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &opencodeClient{}
	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("WriteMCP: %v", err)
	}
	out, _ := os.ReadFile(path)
	for _, sub := range []string{"// my opencode setup", `"tokyonight"`, `"other"`, `"sling"`, `"agent-browser"`, `"$schema"`} {
		if !strings.Contains(string(out), sub) {
			t.Errorf("expected %q preserved\n--- got ---\n%s", sub, string(out))
		}
	}

	if err := c.RemoveMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("RemoveMCP: %v", err)
	}
	out, _ = os.ReadFile(path)
	if strings.Contains(string(out), `"sling"`) {
		t.Errorf("sling entry not removed\n--- got ---\n%s", string(out))
	}
	if strings.Contains(string(out), `"agent-browser"`) {
		t.Errorf("agent-browser entry not removed\n--- got ---\n%s", string(out))
	}
	if !strings.Contains(string(out), `"other"`) {
		t.Errorf("sibling MCP entry lost\n--- got ---\n%s", string(out))
	}
}

// TestOpencodeConfigDirXDG verifies XDG_CONFIG_HOME wins over ~/.config.
func TestOpencodeConfigDirXDG(t *testing.T) {
	home := withTempHomeDir(t)
	if got, want := opencodeConfigDir(), filepath.Join(home, ".config", "opencode"); got != want {
		t.Errorf("default configDir = %s, want %s", got, want)
	}
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "xdg"))
	if got, want := opencodeConfigDir(), filepath.Join(home, "xdg", "opencode"); got != want {
		t.Errorf("XDG configDir = %s, want %s", got, want)
	}
}

// TestPiMCPRoundTrip covers pi's mcp.json (separate from settings.json) with
// the standard mcpServers command/args shape.
func TestPiMCPRoundTrip(t *testing.T) {
	home := withTempHomeDir(t)
	ctx := context.Background()
	c := &piClient{}

	path := filepath.Join(home, ".pi", "agent", "mcp.json")
	if got := c.mcpPath(ScopeUser); got != path {
		t.Fatalf("mcpPath = %s, want %s", got, path)
	}

	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("WriteMCP: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// pi uses the standard command/args split (not opencode's argv array).
	if got := gjson.GetBytes(data, "mcpServers.sling.command").String(); got != "sling" {
		t.Errorf("command = %q, want \"sling\"\n--- got ---\n%s", got, string(data))
	}
	if got := gjson.GetBytes(data, "mcpServers.sling.args").String(); got != `["serve","mcp"]` {
		t.Errorf("args = %q, want [\"serve\",\"mcp\"]\n--- got ---\n%s", got, string(data))
	}
	if got := gjson.GetBytes(data, "mcpServers.agent-browser.args").String(); got != `["mcp","--tools","core"]` {
		t.Errorf("agent-browser args = %q\n--- got ---\n%s", got, string(data))
	}
	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellOK {
		t.Errorf("expected ok after write, got %v (%s)", res.State, res.Note)
	}

	if err := c.RemoveMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("RemoveMCP: %v", err)
	}
	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellFail {
		t.Errorf("expected fail after remove, got %v", res.State)
	}
}

// TestPiAgentDirEnvOverride verifies PI_CODING_AGENT_DIR takes precedence over
// the ~/.pi/agent default.
func TestPiAgentDirEnvOverride(t *testing.T) {
	home := withTempHomeDir(t)
	if got, want := piAgentDir(), filepath.Join(home, ".pi", "agent"); got != want {
		t.Errorf("default agentDir = %s, want %s", got, want)
	}
	custom := filepath.Join(home, "custom-pi")
	t.Setenv("PI_CODING_AGENT_DIR", custom)
	if got := piAgentDir(); got != custom {
		t.Errorf("env agentDir = %s, want %s", got, custom)
	}
	if got, want := (&piClient{}).mcpPath(ScopeUser), filepath.Join(custom, "mcp.json"); got != want {
		t.Errorf("mcpPath = %s, want %s", got, want)
	}
}

// TestGrokMCPRoundTrip covers grok's TOML config: [mcp_servers.sling] written
// into ~/.grok/config.toml, same table shape as codex.
func TestGrokMCPRoundTrip(t *testing.T) {
	home := withTempHomeDir(t)
	ctx := context.Background()
	c := &grokClient{}

	path := filepath.Join(home, ".grok", "config.toml")
	if got := c.configPath(ScopeUser); got != path {
		t.Fatalf("configPath = %s, want %s", got, path)
	}

	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellFail {
		t.Errorf("expected fail before write, got %v", res.State)
	}
	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("WriteMCP: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	for _, sub := range []string{"[mcp_servers.sling]", `command = "sling"`, `args = ["serve", "mcp"]`, "[mcp_servers.agent-browser]"} {
		if !strings.Contains(string(data), sub) {
			t.Errorf("expected %q in config.toml\n--- got ---\n%s", sub, string(data))
		}
	}
	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellOK {
		t.Errorf("expected ok after write, got %v (%s)", res.State, res.Note)
	}

	// Idempotent: a second write must not duplicate the section.
	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("second WriteMCP: %v", err)
	}
	data, _ = os.ReadFile(path)
	if n := strings.Count(string(data), "[mcp_servers.sling]"); n != 1 {
		t.Errorf("expected 1 sling section, got %d\n--- got ---\n%s", n, string(data))
	}
	if n := strings.Count(string(data), "[mcp_servers.agent-browser]"); n != 1 {
		t.Errorf("expected 1 agent-browser section, got %d\n--- got ---\n%s", n, string(data))
	}

	if err := c.RemoveMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("RemoveMCP: %v", err)
	}
	if res := c.CheckMCP(ctx, ScopeUser); res.State != CellFail {
		t.Errorf("expected fail after remove, got %v", res.State)
	}
}

// TestGrokMCPPreservesOtherSections verifies we only own the sling table and
// leave the user's other grok config intact.
func TestGrokMCPPreservesOtherSections(t *testing.T) {
	home := withTempHomeDir(t)
	ctx := context.Background()
	path := filepath.Join(home, ".grok", "config.toml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	original := `model = "grok-4"

[mcp_servers.filesystem]
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem"]

[skills]
paths = ["~/.agents/skills"]
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	c := &grokClient{}
	if err := c.WriteMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("WriteMCP: %v", err)
	}
	out, _ := os.ReadFile(path)
	for _, sub := range []string{`model = "grok-4"`, "[mcp_servers.filesystem]", "[skills]", "[mcp_servers.sling]", "[mcp_servers.agent-browser]"} {
		if !strings.Contains(string(out), sub) {
			t.Errorf("expected %q preserved\n--- got ---\n%s", sub, string(out))
		}
	}

	if err := c.RemoveMCP(ctx, ScopeUser); err != nil {
		t.Fatalf("RemoveMCP: %v", err)
	}
	out, _ = os.ReadFile(path)
	if strings.Contains(string(out), "[mcp_servers.sling]") {
		t.Errorf("sling section not removed\n--- got ---\n%s", string(out))
	}
	if strings.Contains(string(out), "[mcp_servers.agent-browser]") {
		t.Errorf("agent-browser section not removed\n--- got ---\n%s", string(out))
	}
	if !strings.Contains(string(out), "[mcp_servers.filesystem]") {
		t.Errorf("sibling MCP section lost\n--- got ---\n%s", string(out))
	}
	if !strings.Contains(string(out), "[skills]") {
		t.Errorf("skills section lost\n--- got ---\n%s", string(out))
	}
}

// TestNewClientsSkillsAreCanonical verifies opencode/pi/grok are all no-op on
// WriteSkills (they read ~/.agents/skills/ natively) and that CheckSkills
// tracks the canonical bundle rather than a per-client redirect.
func TestNewClientsSkillsAreCanonical(t *testing.T) {
	withTempHomeDir(t)
	ctx := context.Background()
	skills := []string{"sling", "sling-replications"}

	clients := []Client{&opencodeClient{}, &piClient{}, &grokClient{}}
	for _, c := range clients {
		if err := c.WriteSkills(ctx, skills, ScopeUser); err != nil {
			t.Fatalf("%s WriteSkills: %v", c.Name(), err)
		}
		// Bundle not written yet → every skill reports missing.
		for _, res := range c.CheckSkills(ctx, skills, ScopeUser) {
			if res.State != CellFail {
				t.Errorf("%s: expected fail before bundle write, got %v", c.Name(), res.State)
			}
		}
	}

	if err := writeCanonicalBundle(skills); err != nil {
		t.Fatalf("writeCanonicalBundle: %v", err)
	}
	for _, c := range clients {
		results := c.CheckSkills(ctx, skills, ScopeUser)
		if len(results) != len(skills) {
			t.Errorf("%s: got %d results, want %d", c.Name(), len(results), len(skills))
		}
		for _, res := range results {
			if res.State != CellOK {
				t.Errorf("%s: expected ok after bundle write, got %v (%s)", c.Name(), res.State, res.Note)
			}
		}
		// RemoveSkills must not touch the canonical bundle — it's shared.
		if err := c.RemoveSkills(ctx, skills, ScopeUser); err != nil {
			t.Fatalf("%s RemoveSkills: %v", c.Name(), err)
		}
		if !g.PathExists(canonicalSkillPath("sling")) {
			t.Errorf("%s RemoveSkills deleted the shared canonical bundle", c.Name())
		}
	}
}

// TestNewClientsProjectScope verifies project-scope paths resolve under
// projectRoot() (absolute), not the bare relative "./..." form that scattered
// files when CWD was a subdirectory.
func TestNewClientsProjectScope(t *testing.T) {
	root := projectRoot()
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"opencode", (&opencodeClient{}).configPath(ScopeProject), filepath.Join(root, "opencode.json")},
		{"pi", (&piClient{}).mcpPath(ScopeProject), filepath.Join(root, ".pi", "mcp.json")},
		{"grok", (&grokClient{}).configPath(ScopeProject), filepath.Join(root, ".grok", "config.toml")},
		{"claude", (&claudeClient{}).mcpPath(ScopeProject), filepath.Join(root, ".mcp.json")},
		{"vscode", (&vscodeClient{}).vscodeMCPPath(ScopeProject), filepath.Join(root, ".vscode", "mcp.json")},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s project path = %s, want %s", tc.name, tc.got, tc.want)
		}
		if !filepath.IsAbs(tc.got) {
			t.Errorf("%s project path should be absolute, got %s", tc.name, tc.got)
		}
	}
}

// TestNewClientsDetect is PATH-only: a config dir without a binary is not enough.
func TestNewClientsDetect(t *testing.T) {
	home := withTempHomeDir(t)
	bin := filepath.Join(home, "empty-bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)

	cases := []Client{&opencodeClient{}, &piClient{}, &grokClient{}, &geminiClient{}}
	for _, c := range cases {
		if c.Detect() {
			t.Errorf("%s: detected with empty PATH", c.Name())
		}
	}
	for _, name := range []string{"opencode", "pi", "grok", "gemini"} {
		stub := filepath.Join(bin, name)
		if err := os.WriteFile(stub, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range cases {
		if !c.Detect() {
			t.Errorf("%s: not detected after stub on PATH", c.Name())
		}
	}
}

func clearAuthEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN",
		"OPENAI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY",
		"GOOGLE_APPLICATION_CREDENTIALS", "ANTHROPIC_AUTH_TOKEN",
		"CURSOR_API_KEY", "AIDER_API_KEY", "OPENCODE_API_KEY",
		"PI_API_KEY", "OPENROUTER_API_KEY", "XAI_API_KEY", "GROK_API_KEY",
		"GROK_CODE_XAI_API_KEY", "CLAUDE_CONFIG_DIR",
		"CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY",
		"XDG_DATA_HOME", "PI_CODING_AGENT_DIR",
	} {
		t.Setenv(k, "")
	}
}

func TestAuthStateFakeConfigTrees(t *testing.T) {
	clearAuthEnv(t)
	dir := withTempHomeDir(t)

	wantClaudeEmpty := AuthNone
	if runtime.GOOS == "darwin" {
		wantClaudeEmpty = AuthUnknown // Keychain may hold /login tokens
	}
	if st := (&claudeClient{}).AuthState(); st != wantClaudeEmpty {
		t.Fatalf("claude empty = %s, want %s", st, wantClaudeEmpty)
	}
	if st := (&grokClient{}).AuthState(); st != AuthNone {
		t.Fatalf("grok empty = %s, want none", st)
	}

	cred := filepath.Join(dir, ".claude", ".credentials.json")
	if err := os.MkdirAll(filepath.Dir(cred), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cred, []byte(`{"oauth":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := (&claudeClient{}).AuthState(); st != AuthOK {
		t.Fatalf("claude credentials file = %s, want ok", st)
	}

	oauth := filepath.Join(dir, ".claude.json")
	_ = os.Remove(cred)
	if err := os.WriteFile(oauth, []byte(`{"oauthAccount":{"uuid":"x"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := (&claudeClient{}).AuthState(); st != AuthOK {
		t.Fatalf("claude oauth json = %s, want ok", st)
	}

	grokAuth := filepath.Join(dir, ".grok", "auth.json")
	if err := os.MkdirAll(filepath.Dir(grokAuth), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(grokAuth, []byte(`{"token":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := (&grokClient{}).AuthState(); st != AuthOK {
		t.Fatalf("grok auth.json = %s, want ok", st)
	}

	codexAuth := filepath.Join(dir, ".codex", "auth.json")
	if err := os.MkdirAll(filepath.Dir(codexAuth), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(codexAuth, []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if st := (&codexClient{}).AuthState(); st != AuthOK {
		t.Fatalf("codex auth.json = %s, want ok", st)
	}

	if st := (&vscodeClient{}).AuthState(); st != AuthUnknown {
		t.Fatalf("vscode = %s, want unknown", st)
	}
}

func TestAuthStateEnvKeys(t *testing.T) {
	withTempHomeDir(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	if st := (&claudeClient{}).AuthState(); st != AuthOK {
		t.Fatalf("claude env = %s, want ok", st)
	}
}

func TestRankedCLIAgentsAuthFirst(t *testing.T) {
	clearAuthEnv(t)
	dir := withTempHomeDir(t)
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"claude", "codex"} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin)
	t.Setenv("XDG_CONFIG_HOME", "")
	if err := os.MkdirAll(filepath.Join(dir, ".claude"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".claude", ".credentials.json"), []byte(`{"ok":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ranked := RankedCLIAgents()
	if len(ranked) < 2 {
		t.Fatalf("expected claude+codex, got %+v", ranked)
	}
	if ranked[0].Name != "claude" || ranked[0].Auth != AuthOK {
		t.Fatalf("want claude/ok first, got %+v", ranked[0])
	}
	if ranked[1].Name != "codex" {
		t.Fatalf("want codex second, got %+v", ranked[1])
	}
	for _, a := range ranked {
		if a.Bundled {
			t.Fatalf("PATH agents present: bundled opencode should not appear: %+v", a)
		}
	}
}

func TestRankedCLIAgentsBundledOnlyWhenNoneOnPath(t *testing.T) {
	clearAuthEnv(t)
	dir := withTempHomeDir(t)
	t.Setenv("PATH", filepath.Join(dir, "empty-bin"))
	t.Setenv("XDG_CONFIG_HOME", "")
	if err := os.MkdirAll(filepath.Join(dir, ".gemini"), 0o755); err != nil {
		t.Fatal(err)
	}
	ranked := RankedCLIAgents()
	if len(ranked) == 0 {
		t.Fatal("expected bundled opencode when nothing is on PATH")
	}
	if ranked[0].Name != "opencode" || !ranked[0].Bundled {
		t.Fatalf("want bundled opencode, got %+v", ranked[0])
	}
	for _, a := range ranked {
		if a.Name == "gemini" {
			t.Fatalf("gemini config dir without binary must not rank: %+v", a)
		}
	}
}

func TestDoctorMatrixOmitsOffPathAgents(t *testing.T) {
	clearAuthEnv(t)
	dir := withTempHomeDir(t)
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bin, "claude"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	if err := os.MkdirAll(filepath.Join(dir, ".gemini"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := SaveProfile(DefaultProfile()); err != nil {
		t.Fatal(err)
	}
	r, err := Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Matrix == nil {
		t.Fatal("expected matrix")
	}
	for _, c := range r.Matrix.Clients {
		if c == "gemini" {
			t.Fatal("gemini not on PATH must not appear in the matrix")
		}
	}
	var hasAuth bool
	for _, row := range r.Matrix.Rows {
		if row.Label == "agent on PATH" {
			t.Fatal("PATH row should be gone")
		}
		if row.Label == "auth" {
			hasAuth = true
		}
	}
	if !hasAuth {
		t.Fatal("expected auth row")
	}
	out := r.Render()
	if strings.Contains(out, "binary not on $PATH") {
		t.Fatalf("PATH-missing note should not render:\n%s", out)
	}
	if strings.Contains(out, "GEMINI") {
		t.Fatalf("gemini column should not render:\n%s", out)
	}
}

func TestNestedLaunchEnvMarkers(t *testing.T) {
	t.Setenv("CLAUDECODE", "")
	t.Setenv("CURSOR_TRACE_ID", "")
	t.Setenv("OPENCODE", "")
	t.Setenv("OPENCODE_SESSION", "")
	if NestedLaunch() {
		t.Fatal("empty env should not nest")
	}
	t.Setenv("CLAUDECODE", "1")
	if !NestedLaunch() {
		t.Fatal("CLAUDECODE should nest")
	}
	t.Setenv("CLAUDECODE", "")
	t.Setenv("CURSOR_TRACE_ID", "abc")
	if !NestedLaunch() {
		t.Fatal("CURSOR_TRACE_ID should nest")
	}
	t.Setenv("CURSOR_TRACE_ID", "")
	t.Setenv("OPENCODE_SESSION", "s1")
	if !NestedLaunch() {
		t.Fatal("OPENCODE_SESSION should nest")
	}
}

func TestCursorAuthEnvKeyWins(t *testing.T) {
	clearAuthEnv(t)
	resetCursorStatusCache()
	t.Cleanup(resetCursorStatusCache)
	t.Setenv("CURSOR_API_KEY", "key-123")
	if st := (&cursorClient{}).AuthState(); st != AuthOK {
		t.Fatalf("CURSOR_API_KEY = %s, want ok", st)
	}
}

// Cursor keeps browser-login tokens in the OS keychain, so AuthState shells
// out to `cursor-agent status --format json`. When that CLI is absent the
// probe must not claim the user is signed out.
func TestCursorAuthWithoutCLI(t *testing.T) {
	clearAuthEnv(t)
	resetCursorStatusCache()
	t.Cleanup(resetCursorStatusCache)
	t.Setenv("PATH", t.TempDir())
	st := (&cursorClient{}).AuthState()
	if _, ok := cursorStatusAuth(); ok {
		t.Fatal("no cursor-agent on PATH, yet the probe answered")
	}
	want := AuthNone
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		want = AuthUnknown
	}
	if st != want {
		t.Fatalf("AuthState = %s, want %s", st, want)
	}
}

func TestCursorStatusAuthParsesCLI(t *testing.T) {
	clearAuthEnv(t)
	resetCursorStatusCache()
	t.Cleanup(resetCursorStatusCache)
	dir := t.TempDir()
	stub := filepath.Join(dir, "cursor-agent")
	// echo is a shell builtin: the stub's PATH holds only this dir.
	script := "#!/bin/sh\necho '{\"status\":\"authenticated\",\"isAuthenticated\":true}'\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)

	authed, ok := cursorStatusAuth()
	if !ok || !authed {
		t.Fatalf("authed=%v answered=%v", authed, ok)
	}
	if st := (&cursorClient{}).AuthState(); st != AuthOK {
		t.Fatalf("AuthState = %s, want ok", st)
	}

	// Logged out: the CLI still exits 0, so the field decides.
	script = "#!/bin/sh\necho '{\"status\":\"unauthenticated\",\"isAuthenticated\":false}'\n"
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	resetCursorStatusCache()
	authed, ok = cursorStatusAuth()
	if !ok || authed {
		t.Fatalf("logged out: authed=%v answered=%v", authed, ok)
	}
	if st := (&cursorClient{}).AuthState(); st != AuthNone {
		t.Fatalf("AuthState = %s, want none", st)
	}
}
