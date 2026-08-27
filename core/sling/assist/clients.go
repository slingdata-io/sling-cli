package assist

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"gopkg.in/yaml.v3"
)

// ClientKind distinguishes launchable CLI agents from install-only UI surfaces.
// Only Kind() == CLIAgent clients are picked by the assist launcher.
type ClientKind int

const (
	KindCLIAgent ClientKind = iota
	KindUISurface
)

// Client is the per-tool install adapter contract. Each adapter takes care of
// its own redirect/translation of the canonical skills bundle and its own MCP
// config shape. Install/uninstall/doctor iterate over Detected() clients.
//
// Methods that touch the filesystem accept context.Context for cancellation
// (submission/network work will share this seam). Implementations should
// honor ctx.Err() at entry when doing I/O.
type Client interface {
	Name() string
	Kind() ClientKind
	Detect() bool
	WriteSkills(ctx context.Context, skillNames []string, scope Scope) error
	RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error
	WriteMCP(ctx context.Context, scope Scope) error
	RemoveMCP(ctx context.Context, scope Scope) error
	// CheckSkills returns one typed result per skill (no glyph prefixes).
	CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult
	// CheckMCP returns a typed MCP wiring status.
	CheckMCP(ctx context.Context, scope Scope) CheckResult
	// AuthState reports offline credential-file / env-key presence. Never launches a binary.
	AuthState() AuthStatus
}

// Scope is `--scope user` (default) or `--scope project`.
type Scope int

const (
	ScopeUser Scope = iota
	ScopeProject
)

// AllClients returns the canonical ordered list of supported clients.
// Order matters for `agent: auto` resolution and for stable output.
func AllClients() []Client {
	return []Client{
		&claudeClient{},
		&codexClient{},
		&geminiClient{},
		&cursorClient{},
		&opencodeClient{},
		&piClient{},
		&grokClient{},
		&vscodeClient{},
	}
}

// CLIAgents returns only the launchable CLI-agent clients (excludes vscode).
func CLIAgents() []Client {
	out := []Client{}
	for _, c := range AllClients() {
		if c.Kind() == KindCLIAgent {
			out = append(out, c)
		}
	}
	return out
}

// DetectedClients returns the subset of AllClients() whose Detect() returned true.
func DetectedClients() []Client {
	out := []Client{}
	for _, c := range AllClients() {
		if c.Detect() {
			out = append(out, c)
		}
	}
	return out
}

// LookupClient finds a client by name; returns nil if no match.
func LookupClient(name string) Client {
	name = strings.ToLower(strings.TrimSpace(name))
	for _, c := range AllClients() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

// canonicalSkillPath returns the absolute path to a skill's SKILL.md inside
// the canonical bundle.
func canonicalSkillPath(skill string) string {
	return filepath.Join(CanonicalSkillsDir(), skill, "SKILL.md")
}

// writeRedirectFile writes a 1-line `@<absolute_canonical_path>` redirect file.
// If symlinks are preferred (Unix), we still write a stub file so doctor's
// "resolves to canonical" check is uniform across platforms.
func writeRedirectFile(redirectPath, canonicalPath string) error {
	if err := os.MkdirAll(filepath.Dir(redirectPath), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(redirectPath))
	}
	body := fmt.Sprintf("@%s\n", canonicalPath)
	return os.WriteFile(redirectPath, []byte(body), 0o644)
}

// readRedirectTarget parses a 1-line `@<path>` redirect; returns "" if the
// file isn't a redirect (or isn't readable).
func readRedirectTarget(redirectPath string) string {
	data, err := os.ReadFile(redirectPath)
	if err != nil {
		return ""
	}
	line := strings.TrimSpace(string(data))
	if !strings.HasPrefix(line, "@") {
		return ""
	}
	// drop the @, take only the first line if there are several
	target := strings.TrimSpace(strings.SplitN(line[1:], "\n", 2)[0])
	return target
}

// listSkillNames walks the embedded skills FS and returns canonical names
// (one per top-level directory). Skill bundles with multiple files (like
// sling-api-specs) still come back as one name.
func listSkillNames() []string {
	entries, err := SkillsFS.ReadDir("skills")
	if err != nil {
		return nil
	}
	out := []string{}
	for _, e := range entries {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out
}

// writeCanonicalBundle copies the embedded skills tree to ~/.agents/skills/.
// Existing files are overwritten — skills are Sling-owned (see design doc).
// Stale files removed from the embed are deleted from disk.
func writeCanonicalBundle(skillNames []string) error {
	for _, name := range skillNames {
		if _, err := syncCanonicalSkill(name); err != nil {
			return err
		}
	}
	return nil
}

// syncCanonicalSkill writes one skill from the embed FS onto the canonical
// disk tree and removes on-disk files under that skill that are no longer
// embedded. Shared by writeCanonicalBundle and AutoRefresh.
func syncCanonicalSkill(name string) (changed bool, err error) {
	root := CanonicalSkillsDir()
	embeddedRoot := filepath.ToSlash(filepath.Join("skills", name))
	wantFiles := map[string]bool{} // path relative to root, slash-separated

	err = fs.WalkDir(SkillsFS, embeddedRoot, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(p, "skills/")
		wantFiles[rel] = true
		dst := filepath.Join(root, filepath.FromSlash(rel))
		want, rerr := SkillsFS.ReadFile(p)
		if rerr != nil {
			return g.Error(rerr, "read embedded %s", p)
		}
		got, _ := os.ReadFile(dst)
		if bytes.Equal(want, got) {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return g.Error(err, "mkdir %s", filepath.Dir(dst))
		}
		if err := os.WriteFile(dst, want, 0o644); err != nil {
			return g.Error(err, "write %s", dst)
		}
		changed = true
		return nil
	})
	if err != nil {
		return changed, err
	}

	// Prune files on disk that are no longer in the embed (stale supporting docs).
	skillDir := filepath.Join(root, name)
	if g.PathExists(skillDir) {
		_ = filepath.WalkDir(skillDir, func(p string, d fs.DirEntry, werr error) error {
			if werr != nil || d.IsDir() {
				return werr
			}
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				return nil
			}
			rel = filepath.ToSlash(rel)
			if !wantFiles[rel] {
				if rmErr := os.Remove(p); rmErr == nil {
					changed = true
				}
			}
			return nil
		})
	}
	return changed, nil
}

// skillMatchesEmbedded compares every embedded file for a skill against disk.
// Returns ok=false with a short detail when any file is missing or drifted.
func skillMatchesEmbedded(name string) (ok bool, detail string, err error) {
	root := CanonicalSkillsDir()
	embeddedRoot := filepath.ToSlash(filepath.Join("skills", name))
	var mismatches []string
	err = fs.WalkDir(SkillsFS, embeddedRoot, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(p, "skills/")
		dst := filepath.Join(root, filepath.FromSlash(rel))
		want, rerr := SkillsFS.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		got, rerr := os.ReadFile(dst)
		if rerr != nil {
			mismatches = append(mismatches, rel+": missing")
			return nil
		}
		if !bytes.Equal(want, got) {
			mismatches = append(mismatches, rel+": drifted")
		}
		return nil
	})
	if err != nil {
		return false, "", err
	}
	if len(mismatches) > 0 {
		return false, strings.Join(mismatches, "; "), nil
	}
	return true, "", nil
}

// removeCanonicalBundle removes ~/.agents/skills/sling-* directories — only
// our own skills, never anyone else's.
func removeCanonicalBundle(skillNames []string) error {
	root := CanonicalSkillsDir()
	var errs []string
	for _, name := range skillNames {
		p := filepath.Join(root, name)
		if err := os.RemoveAll(p); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", name, err))
		}
	}
	if len(errs) > 0 {
		return g.Error("remove canonical skills: %s", strings.Join(errs, "; "))
	}
	return nil
}

// slingMCPEntry is the canonical Sling MCP server descriptor written into
// every client's MCP config. Centralized so each adapter renders the same
// shape and a future schema bump is one-line.
func slingMCPEntry() map[string]any {
	return map[string]any{
		"command": "sling",
		"args":    []any{"serve", "mcp"},
	}
}

// checkCanonicalSkills is the CheckSkills implementation shared by every
// client that reads ~/.agents/skills/ natively (codex, gemini, opencode, pi,
// grok) — there's no per-client redirect to verify, only the bundle itself.
func checkCanonicalSkills(skillNames []string) []CheckResult {
	out := []CheckResult{}
	for _, name := range skillNames {
		if g.PathExists(canonicalSkillPath(name)) {
			out = append(out, checkSkill(CellOK, name, "canonical"))
		} else {
			out = append(out, checkSkill(CellFail, name, "missing in canonical bundle"))
		}
	}
	return out
}

// checkMCPServersKey reports whether <path> has sling and agent-browser
// entries under the given top-level object key ("mcpServers", "servers", "mcp").
func checkMCPServersKey(path, key string) CheckResult {
	doc, err := jsonReadOrEmpty(path)
	if err != nil {
		return checkFail(err.Error())
	}
	servers, _ := doc[key].(map[string]any)
	if servers == nil {
		return checkFail("no " + key + " block")
	}
	if _, ok := servers["sling"]; !ok {
		return checkFail("sling MCP entry missing")
	}
	if _, ok := servers[agentBrowserMCPName]; !ok {
		return checkFail("agent-browser MCP entry missing")
	}
	return checkOK("sling and agent-browser MCP present")
}

// ---- claude ----

type claudeClient struct{}

func (c *claudeClient) Name() string     { return "claude" }
func (c *claudeClient) Kind() ClientKind { return KindCLIAgent }
func (c *claudeClient) Detect() bool     { return commandOnPath("claude") }

func (c *claudeClient) skillsRoot(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".claude", "skills")
	}
	return filepath.Join(userHome(), ".claude", "skills")
}

func (c *claudeClient) mcpPath(scope Scope) string {
	// Project-scoped MCP lives in .mcp.json at the project root (Claude Code
	// convention). User/local scope stays in ~/.claude.json.
	if scope == ScopeProject {
		return projectPath(".mcp.json")
	}
	return filepath.Join(userHome(), ".claude.json")
}

func (c *claudeClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	root := c.skillsRoot(scope)
	for _, name := range skillNames {
		canonical := canonicalSkillPath(name)
		redirect := filepath.Join(root, name, "SKILL.md")
		if err := writeRedirectFile(redirect, canonical); err != nil {
			return err
		}
	}
	return nil
}

func (c *claudeClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	root := c.skillsRoot(scope)
	for _, name := range skillNames {
		_ = os.RemoveAll(filepath.Join(root, name))
	}
	return nil
}

func (c *claudeClient) WriteMCP(ctx context.Context, scope Scope) error {
	return writeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *claudeClient) RemoveMCP(ctx context.Context, scope Scope) error {
	return removeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *claudeClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	out := []CheckResult{}
	root := c.skillsRoot(scope)
	for _, name := range skillNames {
		redirect := filepath.Join(root, name, "SKILL.md")
		canonical := canonicalSkillPath(name)
		target := readRedirectTarget(redirect)
		switch {
		case target == "":
			out = append(out, checkSkill(CellFail, name, "redirect missing"))
		case target != canonical:
			out = append(out, checkSkill(CellFail, name, fmt.Sprintf("points at %s (expected %s)", target, canonical)))
		case !g.PathExists(canonical):
			out = append(out, checkSkill(CellFail, name, "redirect target not found"))
		default:
			out = append(out, checkSkill(CellOK, name, ""))
		}
	}
	return out
}

func (c *claudeClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	return checkMCPServersKey(c.mcpPath(scope), "mcpServers")
}

// ---- codex (reads ~/.agents/skills/ natively) ----

type codexClient struct{}

func (c *codexClient) Name() string     { return "codex" }
func (c *codexClient) Kind() ClientKind { return KindCLIAgent }
func (c *codexClient) Detect() bool     { return commandOnPath("codex") }

func (c *codexClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	// Codex reads ~/.agents/skills/ natively — nothing extra to do.
	return nil
}
func (c *codexClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}

func (c *codexClient) codexConfigPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".codex", "config.toml")
	}
	return filepath.Join(userHome(), ".codex", "config.toml")
}

func (c *codexClient) WriteMCP(ctx context.Context, scope Scope) error {
	path := c.codexConfigPath(scope)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	body, _ := os.ReadFile(path)
	merged := upsertCodexMCP(string(body))
	return writeBytesPreserveMode(path, []byte(merged), 0o600)
}

func (c *codexClient) RemoveMCP(ctx context.Context, scope Scope) error {
	path := c.codexConfigPath(scope)
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	out := removeCodexMCP(string(body))
	return writeBytesPreserveMode(path, []byte(out), 0o600)
}

func (c *codexClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	return checkCanonicalSkills(skillNames)
}

func (c *codexClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	body, err := os.ReadFile(c.codexConfigPath(scope))
	if err != nil {
		return checkFail("config.toml not present")
	}
	if hasCodexMCPSection(string(body)) && hasCodexNamedMCPSection(string(body), agentBrowserMCPName) {
		return checkOK("sling and agent-browser MCP present")
	}
	if !hasCodexMCPSection(string(body)) {
		return checkFail("sling MCP entry missing")
	}
	return checkFail("agent-browser MCP entry missing")
}

const codexMCPHeader = "[mcp_servers.sling]"

func codexMCPHeaderNamed(name string) string {
	return "[mcp_servers." + name + "]"
}

// isCodexSlingTable reports whether a trimmed TOML header line is our sling
// table or a nested subtable ([mcp_servers.sling.env], etc.).
func isCodexSlingTable(trim string) bool {
	return isCodexNamedMCPTable(trim, "sling")
}

func isCodexNamedMCPTable(trim, name string) bool {
	header := codexMCPHeaderNamed(name)
	return trim == header || strings.HasPrefix(trim, header[:len(header)-1]+".")
}

// hasCodexMCPSection reports whether body has a real [mcp_servers.sling]
// table header (line-anchored), not a mention inside a string or comment.
func hasCodexMCPSection(body string) bool {
	return hasCodexNamedMCPSection(body, "sling")
}

func hasCodexNamedMCPSection(body, name string) bool {
	header := codexMCPHeaderNamed(name)
	for _, line := range strings.Split(body, "\n") {
		if strings.TrimSpace(line) == header {
			return true
		}
	}
	return false
}

func upsertCodexNamedMCP(body, name, command string, args []string) string {
	header := codexMCPHeaderNamed(name)
	block := header + "\ncommand = " + tomlQuote(command) + "\nargs = " + tomlStringArray(args) + "\n"
	if !hasCodexNamedMCPSection(body, name) {
		if body != "" && !strings.HasSuffix(body, "\n") {
			body += "\n"
		}
		return body + "\n" + block
	}
	lines := strings.Split(body, "\n")
	out := []string{}
	skipping := false
	injected := false
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if trim == header {
			out = append(out, strings.TrimRight(block, "\n"))
			skipping = true
			injected = true
			continue
		}
		if skipping {
			if strings.HasPrefix(trim, "[") {
				if isCodexNamedMCPTable(trim, name) {
					continue
				}
				skipping = false
			} else {
				continue
			}
		}
		out = append(out, line)
	}
	if !injected {
		out = append(out, "", strings.TrimRight(block, "\n"))
	}
	return strings.Join(out, "\n")
}

// upsertCodexMCP injects/replaces sling and agent-browser MCP tables.
func upsertCodexMCP(body string) string {
	body = upsertCodexNamedMCP(body, "sling", "sling", []string{"serve", "mcp"})
	return upsertCodexNamedMCP(body, agentBrowserMCPName, agentBrowserBin(), []string{"mcp", "--tools", "core"})
}

func removeCodexNamedMCP(body, name string) string {
	lines := strings.Split(body, "\n")
	out := []string{}
	skipping := false
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if isCodexNamedMCPTable(trim, name) {
			skipping = true
			continue
		}
		if skipping {
			if strings.HasPrefix(trim, "[") {
				if isCodexNamedMCPTable(trim, name) {
					continue
				}
				skipping = false
			} else {
				continue
			}
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// removeCodexMCP strips sling and agent-browser MCP tables (and nested subtables).
func removeCodexMCP(body string) string {
	body = removeCodexNamedMCP(body, "sling")
	return removeCodexNamedMCP(body, agentBrowserMCPName)
}

// ---- gemini (reads ~/.agents/skills/ as alias) ----

type geminiClient struct{}

func (c *geminiClient) Name() string     { return "gemini" }
func (c *geminiClient) Kind() ClientKind { return KindCLIAgent }
func (c *geminiClient) Detect() bool     { return commandOnPath("gemini") }
func (c *geminiClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}
func (c *geminiClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}

func (c *geminiClient) settingsPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".gemini", "settings.json")
	}
	return filepath.Join(userHome(), ".gemini", "settings.json")
}

func (c *geminiClient) WriteMCP(ctx context.Context, scope Scope) error {
	return writeJSONMCP(c.settingsPath(scope), "mcpServers")
}

func (c *geminiClient) RemoveMCP(ctx context.Context, scope Scope) error {
	return removeJSONMCP(c.settingsPath(scope), "mcpServers")
}

func (c *geminiClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	return checkCanonicalSkills(skillNames)
}

func (c *geminiClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	return checkMCPServersKey(c.settingsPath(scope), "mcpServers")
}

// ---- cursor (translates to .mdc) ----

type cursorClient struct{}

func (c *cursorClient) Name() string     { return "cursor" }
func (c *cursorClient) Kind() ClientKind { return KindCLIAgent }
func (c *cursorClient) Detect() bool     { return commandOnPath(agentBinary("cursor")) }

func (c *cursorClient) rulesDir(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".cursor", "rules")
	}
	return filepath.Join(userHome(), ".cursor", "rules")
}

func (c *cursorClient) mcpPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".cursor", "mcp.json")
	}
	return filepath.Join(userHome(), ".cursor", "mcp.json")
}

// translateSkillToMDC produces a cursor `.mdc` file body from a SKILL.md.
// Cursor's `.mdc` shape is YAML frontmatter (with `description`,
// `globs`, `alwaysApply`) + Markdown body — same shape as SKILL.md, so we
// pass through with a lightly-rewritten frontmatter.
func translateSkillToMDC(skillBody []byte) []byte {
	body := string(skillBody)
	// SKILL.md frontmatter looks like:
	//   ---
	//   name: sling
	//   description: ...
	//   ---
	// Cursor's .mdc wants:
	//   ---
	//   description: ...
	//   alwaysApply: false
	//   ---
	if !strings.HasPrefix(body, "---") {
		return []byte("---\nalwaysApply: false\n---\n\n" + body)
	}
	parts := strings.SplitN(body, "\n---", 2)
	if len(parts) != 2 {
		return []byte("---\nalwaysApply: false\n---\n\n" + body)
	}
	header := strings.TrimPrefix(parts[0], "---\n")
	rest := strings.TrimPrefix(parts[1], "\n")

	var fm map[string]any
	_ = yaml.Unmarshal([]byte(header), &fm)
	if fm == nil {
		fm = map[string]any{}
	}
	out := map[string]any{
		"description": fm["description"],
		"alwaysApply": false,
	}
	yamlBytes, _ := yaml.Marshal(out)
	return []byte("---\n" + string(yamlBytes) + "---\n\n" + rest)
}

func (c *cursorClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	dir := c.rulesDir(scope)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return g.Error(err, "mkdir %s", dir)
	}
	for _, name := range skillNames {
		src := canonicalSkillPath(name)
		body, err := os.ReadFile(src)
		if err != nil {
			return g.Error(err, "read %s", src)
		}
		mdc := translateSkillToMDC(body)
		dst := filepath.Join(dir, name+".mdc")
		if err := os.WriteFile(dst, mdc, 0o644); err != nil {
			return g.Error(err, "write %s", dst)
		}
	}
	return nil
}

func (c *cursorClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	dir := c.rulesDir(scope)
	for _, name := range skillNames {
		_ = os.Remove(filepath.Join(dir, name+".mdc"))
	}
	return nil
}

func (c *cursorClient) WriteMCP(ctx context.Context, scope Scope) error {
	return writeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *cursorClient) RemoveMCP(ctx context.Context, scope Scope) error {
	return removeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *cursorClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	out := []CheckResult{}
	for _, name := range skillNames {
		mdc := filepath.Join(c.rulesDir(scope), name+".mdc")
		if g.PathExists(mdc) {
			out = append(out, checkSkill(CellOK, name, "mdc present"))
		} else {
			out = append(out, checkSkill(CellFail, name, "mdc missing"))
		}
	}
	return out
}

func (c *cursorClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	return checkMCPServersKey(c.mcpPath(scope), "mcpServers")
}

// ---- opencode (reads ~/.agents/skills/ natively) ----

type opencodeClient struct{}

func (c *opencodeClient) Name() string     { return "opencode" }
func (c *opencodeClient) Kind() ClientKind { return KindCLIAgent }
func (c *opencodeClient) Detect() bool {
	return commandOnPath("opencode") || g.PathExists(BundledOpenCodePath())
}

// opencodeConfigDir is opencode's global config dir: $XDG_CONFIG_HOME/opencode
// falling back to ~/.config/opencode.
func opencodeConfigDir() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "opencode")
	}
	return filepath.Join(userHome(), ".config", "opencode")
}

func (c *opencodeClient) configPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath("opencode.json")
	}
	return filepath.Join(opencodeConfigDir(), "opencode.json")
}

func (c *opencodeClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	// opencode globs ~/.agents/skills/<name>/SKILL.md natively.
	return nil
}
func (c *opencodeClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}

// opencodeMCPEntry is opencode's own MCP shape: `type: local` plus a single
// argv array (not command/args like everyone else).
func opencodeMCPEntry() map[string]any {
	return map[string]any{
		"type":    "local",
		"command": []any{"sling", "serve", "mcp"},
		"enabled": true,
	}
}

func (c *opencodeClient) WriteMCP(ctx context.Context, scope Scope) error {
	if err := setJSONPath(c.configPath(scope), "mcp.sling", opencodeMCPEntry()); err != nil {
		return err
	}
	return setJSONPath(c.configPath(scope), "mcp.agent-browser", opencodeAgentBrowserMCPEntry())
}

func (c *opencodeClient) RemoveMCP(ctx context.Context, scope Scope) error {
	if err := deleteJSONPath(c.configPath(scope), "mcp.sling"); err != nil {
		return err
	}
	return deleteJSONPath(c.configPath(scope), "mcp.agent-browser")
}

func (c *opencodeClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	return checkCanonicalSkills(skillNames)
}

func (c *opencodeClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	return checkMCPServersKey(c.configPath(scope), "mcp")
}

// ---- pi (reads ~/.agents/skills/ natively) ----

type piClient struct{}

func (c *piClient) Name() string     { return "pi" }
func (c *piClient) Kind() ClientKind { return KindCLIAgent }
func (c *piClient) Detect() bool     { return commandOnPath("pi") }

// piAgentDir is pi's agent config dir — $PI_CODING_AGENT_DIR when set,
// otherwise ~/.pi/agent.
func piAgentDir() string {
	if d := os.Getenv("PI_CODING_AGENT_DIR"); d != "" {
		return d
	}
	return filepath.Join(userHome(), ".pi", "agent")
}

// mcpPath: pi keeps MCP servers in a dedicated mcp.json, separate from
// settings.json. Project scope is .pi/mcp.json at the repo root.
func (c *piClient) mcpPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".pi", "mcp.json")
	}
	return filepath.Join(piAgentDir(), "mcp.json")
}

func (c *piClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	// pi discovers ~/.agents/skills/<name>/SKILL.md natively.
	return nil
}
func (c *piClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}

func (c *piClient) WriteMCP(ctx context.Context, scope Scope) error {
	return writeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *piClient) RemoveMCP(ctx context.Context, scope Scope) error {
	return removeJSONMCP(c.mcpPath(scope), "mcpServers")
}

func (c *piClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	return checkCanonicalSkills(skillNames)
}

func (c *piClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	return checkMCPServersKey(c.mcpPath(scope), "mcpServers")
}

// ---- grok (TOML config, reads ~/.agents/skills/ for AGENTS.md compat) ----

type grokClient struct{}

func (c *grokClient) Name() string     { return "grok" }
func (c *grokClient) Kind() ClientKind { return KindCLIAgent }
func (c *grokClient) Detect() bool     { return commandOnPath("grok") }

func (c *grokClient) configPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".grok", "config.toml")
	}
	return filepath.Join(userHome(), ".grok", "config.toml")
}

func (c *grokClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	// grok discovers ~/.agents/skills/ as part of its AGENTS.md compatibility.
	return nil
}
func (c *grokClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	return nil
}

func (c *grokClient) WriteMCP(ctx context.Context, scope Scope) error {
	path := c.configPath(scope)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "mkdir %s", filepath.Dir(path))
	}
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	body, _ := os.ReadFile(path)
	// grok's config.toml uses the same [mcp_servers.<name>] shape as codex.
	merged := upsertCodexMCP(string(body))
	return writeBytesPreserveMode(path, []byte(merged), 0o600)
}

func (c *grokClient) RemoveMCP(ctx context.Context, scope Scope) error {
	path := c.configPath(scope)
	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := backupBeforeEdit(path); err != nil {
		return err
	}
	return writeBytesPreserveMode(path, []byte(removeCodexMCP(string(body))), 0o600)
}

func (c *grokClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	return checkCanonicalSkills(skillNames)
}

func (c *grokClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	body, err := os.ReadFile(c.configPath(scope))
	if err != nil {
		return checkFail("config.toml not present")
	}
	if hasCodexMCPSection(string(body)) && hasCodexNamedMCPSection(string(body), agentBrowserMCPName) {
		return checkOK("sling and agent-browser MCP present")
	}
	if !hasCodexMCPSection(string(body)) {
		return checkFail("sling MCP entry missing")
	}
	return checkFail("agent-browser MCP entry missing")
}

// ---- vscode (UI surface, install-only) ----

type vscodeClient struct{}

func (c *vscodeClient) Name() string     { return "vscode" }
func (c *vscodeClient) Kind() ClientKind { return KindUISurface }

// vscodeUserDir returns the platform-specific VS Code user-config dir, or "".
// We probe Code, Code-Insiders, and VSCodium in that order.
func vscodeUserDir() string {
	for _, name := range []string{"Code", "Code - Insiders", "VSCodium"} {
		if p := vscodeUserDirNamed(name); p != "" {
			return p
		}
	}
	return ""
}

func vscodeUserDirNamed(productName string) string {
	var base string
	switch runtime.GOOS {
	case "darwin":
		base = filepath.Join(userHome(), "Library", "Application Support", productName, "User")
	case "windows":
		base = filepath.Join(os.Getenv("APPDATA"), productName, "User")
	default:
		base = filepath.Join(userHome(), ".config", productName, "User")
	}
	if g.PathExists(base) {
		return base
	}
	return ""
}

func (c *vscodeClient) Detect() bool { return vscodeUserDir() != "" }

func (c *vscodeClient) settingsPath() string {
	dir := vscodeUserDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "settings.json")
}

func (c *vscodeClient) WriteSkills(ctx context.Context, skillNames []string, scope Scope) error {
	path := c.settingsPath()
	if path == "" {
		return nil
	}
	// VS Code uses literal dotted keys, so the path must escape the dots.
	const key = `chat\.instructionsFilesLocations`
	canonical := CanonicalSkillsDir()
	_, body, err := jsonReadRaw(path)
	if err != nil {
		return err
	}
	// Already present → no-op (avoid an unnecessary backup churn + write).
	for _, e := range gjsonGetArrayStrings(body, key) {
		if e == canonical {
			return nil
		}
	}
	// Route through setJSONPath so we get the backup + sanity-check guard.
	return setJSONPath(path, key+".-1", canonical)
}

func (c *vscodeClient) RemoveSkills(ctx context.Context, skillNames []string, scope Scope) error {
	path := c.settingsPath()
	if path == "" {
		return nil
	}
	const key = `chat\.instructionsFilesLocations`
	canonical := CanonicalSkillsDir()
	_, body, err := jsonReadRaw(path)
	if err != nil {
		return err
	}
	cur := gjsonGetArrayStrings(body, key)
	idx := -1
	for i, e := range cur {
		if e == canonical {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	return deleteJSONPath(path, fmt.Sprintf("%s.%d", key, idx))
}

// vscodeMCPPath returns the VS Code mcp.json for the given scope.
// User: <UserData>/User/mcp.json; project: ./.vscode/mcp.json.
// VS Code reads MCP from mcp.json (servers.*), not settings.json.
func (c *vscodeClient) vscodeMCPPath(scope Scope) string {
	if scope == ScopeProject {
		return projectPath(".vscode", "mcp.json")
	}
	dir := vscodeUserDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "mcp.json")
}

func (c *vscodeClient) WriteMCP(ctx context.Context, scope Scope) error {
	path := c.vscodeMCPPath(scope)
	if path == "" {
		return nil
	}
	// VS Code mcp.json uses top-level "servers", not "mcpServers".
	return writeJSONMCP(path, "servers")
}

func (c *vscodeClient) RemoveMCP(ctx context.Context, scope Scope) error {
	path := c.vscodeMCPPath(scope)
	if path == "" {
		return nil
	}
	_ = deleteJSONPath(path, "servers.sling")
	_ = deleteJSONPath(path, "servers.agent-browser")
	// Clean the obsolete flat settings.json key from an earlier buggy path
	// (github.copilot.chat.mcp.servers.sling) so doctor/settings stay tidy.
	if settings := c.settingsPath(); settings != "" {
		_ = deleteJSONPath(settings, `github\.copilot\.chat\.mcp\.servers\.sling`)
	}
	return nil
}

func (c *vscodeClient) CheckSkills(ctx context.Context, skillNames []string, scope Scope) []CheckResult {
	path := c.settingsPath()
	if path == "" {
		return []CheckResult{checkNA("no user config dir found")}
	}
	doc, err := jsonReadOrEmpty(path)
	if err != nil {
		return []CheckResult{checkFail(err.Error())}
	}
	canonical := CanonicalSkillsDir()
	has := false
	if locs, ok := doc["chat.instructionsFilesLocations"].([]any); ok {
		for _, e := range locs {
			if s, _ := e.(string); s == canonical {
				has = true
				break
			}
		}
	}
	// One aggregate result for all skills (vscode wires the canonical dir once).
	if has {
		return []CheckResult{checkOK("chat.instructionsFilesLocations includes canonical")}
	}
	return []CheckResult{checkFail("chat.instructionsFilesLocations missing canonical")}
}

func (c *vscodeClient) CheckMCP(ctx context.Context, scope Scope) CheckResult {
	path := c.vscodeMCPPath(scope)
	if path == "" {
		return checkNA("no user config dir found")
	}
	doc, err := jsonReadOrEmpty(path)
	if err != nil {
		return checkFail(err.Error())
	}
	servers, _ := doc["servers"].(map[string]any)
	if servers == nil {
		return checkFail("no servers block in mcp.json")
	}
	if _, ok := servers["sling"]; !ok {
		return checkFail("servers.sling missing in mcp.json")
	}
	if _, ok := servers[agentBrowserMCPName]; !ok {
		return checkFail("servers.agent-browser missing in mcp.json")
	}
	return checkOK("mcp.json servers.sling and agent-browser present")
}

// commandOnPath returns true if `name` resolves to an executable on $PATH.
// Uses exec.LookPath so Windows PATHEXT / .exe resolution works.
func commandOnPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// AuthStatus is an offline probe of whether a CLI agent looks signed in.
// Values: ok (credential file or env key present), none, unknown.
type AuthStatus string

const (
	AuthOK      AuthStatus = "ok"
	AuthNone    AuthStatus = "none"
	AuthUnknown AuthStatus = "unknown"
)

func fileNonEmpty(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Size() > 0
}

func envNonEmpty(keys ...string) bool {
	for _, k := range keys {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return false
}

func claudeJSONHasOAuth(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var doc map[string]any
	if json.Unmarshal(b, &doc) != nil {
		return false
	}
	v, ok := doc["oauthAccount"]
	if !ok || v == nil {
		return false
	}
	m, ok := v.(map[string]any)
	return ok && len(m) > 0
}

func (c *claudeClient) AuthState() AuthStatus {
	if envNonEmpty("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN") {
		return AuthOK
	}
	if envNonEmpty("CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY") {
		return AuthOK
	}
	credDir := filepath.Join(userHome(), ".claude")
	if d := strings.TrimSpace(os.Getenv("CLAUDE_CONFIG_DIR")); d != "" {
		credDir = d
	}
	if fileNonEmpty(filepath.Join(credDir, ".credentials.json")) {
		return AuthOK
	}
	if claudeJSONHasOAuth(filepath.Join(userHome(), ".claude.json")) {
		return AuthOK
	}
	// macOS stores /login tokens in Keychain, not a file.
	if runtime.GOOS == "darwin" {
		return AuthUnknown
	}
	return AuthNone
}

func (c *codexClient) AuthState() AuthStatus {
	if envNonEmpty("OPENAI_API_KEY") {
		return AuthOK
	}
	home := userHome()
	if fileNonEmpty(filepath.Join(home, ".codex", "auth.json")) {
		return AuthOK
	}
	if fileNonEmpty(filepath.Join(home, ".codex", "config.toml")) {
		return AuthUnknown
	}
	return AuthNone
}

func (c *geminiClient) AuthState() AuthStatus {
	if envNonEmpty("GEMINI_API_KEY", "GOOGLE_API_KEY") {
		return AuthOK
	}
	if p := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); p != "" && fileNonEmpty(p) {
		return AuthOK
	}
	home := userHome()
	if fileNonEmpty(filepath.Join(home, ".gemini", "oauth_creds.json")) ||
		fileNonEmpty(filepath.Join(home, ".gemini", "google_accounts.json")) {
		return AuthOK
	}
	return AuthNone
}

func (c *cursorClient) AuthState() AuthStatus {
	if envNonEmpty("CURSOR_API_KEY") {
		return AuthOK
	}
	// Browser login stores credentials in the OS keychain, not a file we can
	// read, so ask the CLI. `cursor-agent status --format json` reports
	// isAuthenticated and always exits 0 — trust the field, not the code.
	if st, ok := cursorStatusAuth(); ok {
		if st {
			return AuthOK
		}
		return AuthNone
	}
	if runtime.GOOS == "darwin" || runtime.GOOS == "windows" {
		return AuthUnknown
	}
	return AuthNone
}

// cursorStatusOnce caches the probe: doctor asks several times per run and
// each call spawns the CLI.
var cursorStatusOnce struct {
	sync.Once
	authed, answered bool
}

// cursorStatusAuth runs `cursor-agent status --format json`.
// Returns (authenticated, true) when the CLI answered, (false, false) otherwise.
func cursorStatusAuth() (bool, bool) {
	cursorStatusOnce.Do(func() {
		cursorStatusOnce.authed, cursorStatusOnce.answered = probeCursorStatus()
	})
	return cursorStatusOnce.authed, cursorStatusOnce.answered
}

// resetCursorStatusCache clears the memoized probe. Tests use it when they
// swap the stub on PATH.
func resetCursorStatusCache() {
	cursorStatusOnce.Once = sync.Once{}
	cursorStatusOnce.authed, cursorStatusOnce.answered = false, false
}

func probeCursorStatus() (bool, bool) {
	bin, err := exec.LookPath(agentBinary("cursor"))
	if err != nil {
		return false, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "status", "--format", "json").Output()
	if err != nil && len(out) == 0 {
		return false, false
	}
	var doc struct {
		IsAuthenticated *bool `json:"isAuthenticated"`
	}
	if json.Unmarshal(out, &doc) != nil || doc.IsAuthenticated == nil {
		return false, false
	}
	return *doc.IsAuthenticated, true
}

func (c *opencodeClient) AuthState() AuthStatus {
	if envNonEmpty("OPENCODE_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY") {
		return AuthOK
	}
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		dataHome = filepath.Join(userHome(), ".local", "share")
	}
	if fileNonEmpty(filepath.Join(dataHome, "opencode", "auth.json")) {
		return AuthOK
	}
	if fileNonEmpty(filepath.Join(userHome(), ".opencode", "auth.json")) {
		return AuthOK
	}
	return AuthNone
}

func (c *piClient) AuthState() AuthStatus {
	if envNonEmpty("PI_API_KEY", "OPENROUTER_API_KEY") {
		return AuthOK
	}
	if fileNonEmpty(filepath.Join(piAgentDir(), "auth.json")) {
		return AuthOK
	}
	return AuthNone
}

func (c *grokClient) AuthState() AuthStatus {
	if envNonEmpty("XAI_API_KEY", "GROK_API_KEY", "GROK_CODE_XAI_API_KEY") {
		return AuthOK
	}
	if fileNonEmpty(filepath.Join(userHome(), ".grok", "auth.json")) {
		return AuthOK
	}
	return AuthNone
}

func (c *vscodeClient) AuthState() AuthStatus {
	return AuthUnknown
}

// YesNo is the install-summary token: yes / no / —.
func (s AuthStatus) YesNo() string {
	switch s {
	case AuthOK:
		return "yes"
	case AuthNone:
		return "no"
	default:
		return "—"
	}
}

// cell is the doctor-matrix cell. Missing auth is not a failure.
func (s AuthStatus) cell() CellState {
	switch s {
	case AuthOK:
		return CellOK
	case AuthNone:
		return CellEmpty
	default:
		return CellNA
	}
}

// RankedAgent is a detected CLI agent with install + auth state for the confirm form.
type RankedAgent struct {
	Name    string
	Auth    AuthStatus
	Detect  bool
	Bundled bool // true when this is the downloadable opencode fallback
	Score   int  // lower is better
}

// RankedCLIAgents lists CLI agents on $PATH: authenticated first, then unknown, then none.
// Bundled OpenCode is appended only when no CLI agent is on $PATH (and a release asset exists).
func RankedCLIAgents() []RankedAgent {
	out := []RankedAgent{}
	for _, c := range CLIAgents() {
		if !c.Detect() {
			continue
		}
		auth := c.AuthState()
		score := 2
		switch auth {
		case AuthOK:
			score = 0
		case AuthUnknown:
			score = 1
		}
		out = append(out, RankedAgent{
			Name:   c.Name(),
			Auth:   auth,
			Detect: true,
			Score:  score,
		})
	}
	if len(out) == 0 {
		if _, err := OpenCodeAssetName(runtime.GOOS, runtime.GOARCH); err == nil {
			out = append(out, RankedAgent{
				Name:    "opencode",
				Auth:    AuthNone,
				Detect:  false,
				Bundled: true,
				Score:   3,
			})
		}
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Score < out[i].Score {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// pathRanked splits RankedCLIAgents into on-PATH agents and optional bundled OpenCode.
func pathRanked() (agents []RankedAgent, bundled *RankedAgent) {
	for _, a := range RankedCLIAgents() {
		if a.Bundled {
			cp := a
			bundled = &cp
			continue
		}
		agents = append(agents, a)
	}
	return agents, bundled
}

// RecommendedAgent is the first ranked CLI agent (authenticated on PATH, else the only one).
func RecommendedAgent() string {
	ranked := RankedCLIAgents()
	if len(ranked) == 0 {
		return "auto"
	}
	return ranked[0].Name
}
