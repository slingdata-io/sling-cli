package assist

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core"
)

// InstallOptions controls `sling assist setup` (install path).
type InstallOptions struct {
	Reconfigure    bool
	SkillsOnly     bool
	MCPOnly        bool
	Scope          Scope
	NonInteractive bool
	DefaultAgent   string // used only on non-interactive first-run
}

// InstallResult summarizes what install touched.
type InstallResult struct {
	ProfileWritten     bool
	CanonicalSkillsDir string
	WiredClients       []ClientResult
	SkippedClients     []string // not detected
}

// ClientResult is one row of the install summary.
type ClientResult struct {
	Name        string
	Kind        ClientKind
	WroteSkills bool
	WroteMCP    bool
	Authed      AuthStatus
	Notes       string
}

func requestedAgent(opts InstallOptions) string {
	agent := strings.ToLower(strings.TrimSpace(opts.DefaultAgent))
	if agent != "" {
		return agent
	}
	if prof, exists, err := LoadProfile(); err == nil && exists {
		return strings.ToLower(strings.TrimSpace(prof.Agent))
	}
	return ""
}

func anyUsableCLIAgent() bool {
	for _, c := range CLIAgents() {
		if c.Detect() {
			return true
		}
	}
	return false
}

// maybeEnsureOpenCode downloads opencode only when the user picked it, or when
// no other CLI agent is usable (bundled fallback). System binaries still win.
func maybeEnsureOpenCode(opts InstallOptions) error {
	agent := requestedAgent(opts)
	switch {
	case agent == "opencode":
		// user picked opencode
	case agent != "" && agent != "auto":
		return nil
	case anyUsableCLIAgent():
		return nil
	}
	if _, err := EnsureBinOpenCode(); err != nil {
		return err
	}
	return ApplyHarnessProviderConfig()
}

// Install is idempotent install/refresh. Honors ctx between clients.
func Install(ctx context.Context, opts InstallOptions) (*InstallResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	res := &InstallResult{
		CanonicalSkillsDir: CanonicalSkillsDir(),
	}

	if err := maybeEnsureOpenCode(opts); err != nil {
		return nil, err
	}
	if !opts.SkillsOnly {
		if err := maybeEnsureAgentBrowser(opts); err != nil {
			return nil, err
		}
	}

	detected := DetectedClients()
	if len(detected) == 0 {
		return nil, g.Error("no AI agent on $PATH; install one of: claude, codex, gemini, cursor, opencode, pi, grok")
	}

	prof, exists, err := LoadProfile()
	if err != nil {
		return nil, err
	}
	if !exists || opts.Reconfigure {
		prof = DefaultProfile()
		switch {
		case opts.DefaultAgent != "":
			prof.Agent = opts.DefaultAgent
		default:
			for _, c := range detected {
				if c.Kind() == KindCLIAgent {
					prof.Agent = c.Name()
					break
				}
			}
			if prof.Agent == "" {
				prof.Agent = "auto"
			}
		}
		if err := SaveProfile(prof); err != nil {
			return nil, err
		}
		res.ProfileWritten = true
	}

	skillNames := listSkillNames()
	if !opts.MCPOnly {
		if err := writeCanonicalBundle(skillNames); err != nil {
			return nil, err
		}
		pruneRetiredSkills(ctx, opts.Scope)
	}

	var wireErrs []string
	for _, c := range detected {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		row := ClientResult{Name: c.Name(), Kind: c.Kind(), Authed: c.AuthState()}
		if !opts.MCPOnly {
			if err := c.WriteSkills(ctx, skillNames, opts.Scope); err != nil {
				row.Notes = fmt.Sprintf("skills: %v", err)
				wireErrs = append(wireErrs, fmt.Sprintf("%s skills: %v", c.Name(), err))
			} else {
				row.WroteSkills = true
			}
		}
		if !opts.SkillsOnly {
			if err := c.WriteMCP(ctx, opts.Scope); err != nil {
				if row.Notes != "" {
					row.Notes += "; "
				}
				row.Notes += fmt.Sprintf("mcp: %v", err)
				wireErrs = append(wireErrs, fmt.Sprintf("%s mcp: %v", c.Name(), err))
			} else {
				row.WroteMCP = true
			}
		}
		res.WiredClients = append(res.WiredClients, row)
	}
	for _, c := range AllClients() {
		if !c.Detect() {
			res.SkippedClients = append(res.SkippedClients, c.Name())
		}
	}

	if len(wireErrs) > 0 {
		// Do not stamp on partial failure — retry on next install.
		return res, g.Error("install incomplete: %s", strings.Join(wireErrs, "; "))
	}

	if err := os.WriteFile(VersionFilePath(), []byte(core.Version), 0o644); err != nil {
		return res, g.Error(err, "could not stamp %s", VersionFilePath())
	}

	return res, nil
}

// UninstallOptions controls `sling assist setup --uninstall`.
type UninstallOptions struct {
	All            bool
	SkillsOnly     bool
	MCPOnly        bool
	Scope          Scope
	NonInteractive bool
	IncludeClients []string // empty = all detected
}

// versionUninstalled prevents AutoRefresh from resurrecting after uninstall.
const versionUninstalled = "uninstalled"

// Uninstall removes sling skills/MCP only (never other tools' entries).
func Uninstall(ctx context.Context, opts UninstallOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	skillNames := listSkillNames()

	pickClient := func(name string) bool {
		if len(opts.IncludeClients) == 0 {
			return true
		}
		for _, n := range opts.IncludeClients {
			if n == name {
				return true
			}
		}
		return false
	}

	var errs []string
	for _, c := range AllClients() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !c.Detect() {
			continue
		}
		if !pickClient(c.Name()) {
			continue
		}
		if !opts.MCPOnly {
			if err := c.RemoveSkills(ctx, skillNames, opts.Scope); err != nil {
				errs = append(errs, fmt.Sprintf("%s skills: %v", c.Name(), err))
			}
		}
		if !opts.SkillsOnly {
			if err := c.RemoveMCP(ctx, opts.Scope); err != nil {
				errs = append(errs, fmt.Sprintf("%s mcp: %v", c.Name(), err))
			}
		}
	}
	if !opts.MCPOnly {
		if err := removeCanonicalBundle(skillNames); err != nil {
			errs = append(errs, err.Error())
		}
		// Mark uninstalled so AutoRefresh will not re-wire on upgrade.
		if err := os.MkdirAll(AssistDir(), 0o755); err != nil {
			errs = append(errs, fmt.Sprintf("mkdir assist: %v", err))
		} else if err := os.WriteFile(VersionFilePath(), []byte(versionUninstalled), 0o644); err != nil {
			errs = append(errs, fmt.Sprintf("stamp uninstalled: %v", err))
		}
	}
	if len(errs) > 0 {
		return g.Error("uninstall completed with errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// AutoRefresh updates installed skills to match the embedded bundle.
// It also removes retired skill directories.
// This function runs on each `sling assist` command.
// If no current skills exist, it does not install them.
// If the user ran uninstall, it does not install them again.
// Returns a notice line when something changed.
func AutoRefresh(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}

	stamp, err := os.ReadFile(VersionFilePath())
	if err != nil && !os.IsNotExist(err) {
		return "", g.Error(err, "could not read %s", VersionFilePath())
	}
	s := strings.TrimSpace(string(stamp))
	if s == versionUninstalled {
		return "", nil
	}

	if !anyCurrentSkillsInstalled() {
		// Remove leftover retired dirs. Do not install the current bundle.
		pruneRetiredSkills(ctx, ScopeUser)
		return "", nil
	}

	lockPath := filepath.Join(AssistDir(), ".refresh-lock")
	unlock, ok := tryRefreshLock(lockPath)
	if !ok {
		return "", nil
	}
	defer unlock()

	skillNames := listSkillNames()
	changed := []string{}
	var refreshErrs []string
	for _, name := range skillNames {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		didChange, err := syncCanonicalSkill(name)
		if err != nil {
			refreshErrs = append(refreshErrs, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		if didChange {
			changed = append(changed, name)
		}
	}
	pruned := pruneRetiredSkills(ctx, ScopeUser)

	// Files already match the embed and the stamp is current. Skip re-wire.
	if len(changed) == 0 && len(pruned) == 0 && s == core.Version && len(refreshErrs) == 0 {
		return "", nil
	}

	clients := []string{}
	for _, c := range DetectedClients() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := c.WriteSkills(ctx, skillNames, ScopeUser); err != nil {
			refreshErrs = append(refreshErrs, fmt.Sprintf("%s: %v", c.Name(), err))
			continue
		}
		clients = append(clients, c.Name())
	}

	// Only stamp on full success so partial failure retries next time.
	if len(refreshErrs) > 0 {
		return "", g.Error("auto-refresh incomplete: %s", strings.Join(refreshErrs, "; "))
	}
	if err := os.WriteFile(VersionFilePath(), []byte(core.Version), 0o644); err != nil {
		return "", g.Error(err, "could not stamp %s", VersionFilePath())
	}

	if len(changed) == 0 && len(pruned) == 0 {
		return "", nil
	}
	return fmt.Sprintf("sling: refreshed AI skills for v%s (%s)", core.Version, strings.Join(clients, ", ")), nil
}

const refreshLockStale = 5 * time.Minute

// tryRefreshLock acquires an exclusive lock file; unlock removes only if we still own it.
func tryRefreshLock(lockPath string) (func(), bool) {
	token := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())

	create := func() (*os.File, error) {
		return os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	}

	f, err := create()
	if err != nil {
		if !os.IsExist(err) {
			return nil, false
		}
		info, statErr := os.Stat(lockPath)
		if statErr != nil || time.Since(info.ModTime()) < refreshLockStale {
			return nil, false
		}
		// Stale lock: reclaim (racing peer loses on O_EXCL).
		_ = os.Remove(lockPath)
		f, err = create()
		if err != nil {
			return nil, false
		}
	}
	_, _ = f.Write([]byte(token))
	_ = f.Close()

	unlock := func() {
		got, err := os.ReadFile(lockPath)
		if err == nil && string(got) == token {
			_ = os.Remove(lockPath)
		}
	}
	return unlock, true
}

// retiredSkillNames are Sling-owned skill names from earlier bundle versions
// that no longer exist in the embed. Pruned on install/refresh so agents stop
// picking up their stale content.
var retiredSkillNames = []string{
	"sling-hooks", "sling-transforms", "sling-troubleshooting",
	"sling-connections", "sling-project", "sling-monitors",
}

// pruneRetiredSkills removes retired skills from the canonical bundle and from
// per-skill client redirects. Best-effort: failures only debug-log.
// vscode is skipped — RemoveSkills unwires the whole bundle,
// and it references the canonical dir, so the canonical prune covers it.
// Returns the retired names that were present on disk and removed.
func pruneRetiredSkills(ctx context.Context, scope Scope) []string {
	root := CanonicalSkillsDir()
	pruned := []string{}
	for _, name := range retiredSkillNames {
		p := filepath.Join(root, name)
		if !g.PathExists(p) {
			continue
		}
		if err := os.RemoveAll(p); err != nil {
			g.Debug("assist: prune retired skill %s: %s", name, err.Error())
			continue
		}
		pruned = append(pruned, name)
	}
	for _, c := range DetectedClients() {
		if ctx.Err() != nil {
			return pruned
		}
		if c.Name() == "vscode" {
			continue
		}
		if err := c.RemoveSkills(ctx, retiredSkillNames, scope); err != nil {
			g.Debug("assist: prune retired skills for %s: %s", c.Name(), err.Error())
		}
	}
	return pruned
}

// anyCurrentSkillsInstalled reports whether at least one embedded skill
// is present in ~/.agents/skills/. Absence means the user has not run setup.
func anyCurrentSkillsInstalled() bool {
	for _, name := range listSkillNames() {
		if g.PathExists(canonicalSkillPath(name)) {
			return true
		}
	}
	return false
}

// MD5OfFile returns the MD5 hex of a file.
func MD5OfFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:]), nil
}

// MD5OfEmbeddedSkill returns the MD5 of an embedded skill file.
func MD5OfEmbeddedSkill(rel string) (string, error) {
	data, err := SkillsFS.ReadFile(filepath.ToSlash(filepath.Join("skills", rel)))
	if err != nil {
		return "", err
	}
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:]), nil
}

// SetupAction is what RunSetupActionForm returns. Drives the post-doctor
// branching in `sling assist setup` for users who already have a profile.
type SetupAction string

const (
	SetupActionRefresh        SetupAction = "refresh"         // re-install everything (idempotent)
	SetupActionInstallMissing SetupAction = "install_missing" // install only the failing components
	SetupActionReconfigure    SetupAction = "reconfigure"     // re-prompt the profile form, then install
	SetupActionUninstall      SetupAction = "uninstall"       // wipe everything
	SetupActionExit           SetupAction = "exit"            // do nothing
)

// ErrUserAborted is returned by interactive forms when the user declines.
var ErrUserAborted = errors.New("user aborted")

// RunSetupActionForm runs after doctor has printed its report.
func RunSetupActionForm(report *DoctorReport) (SetupAction, error) {
	missingLabel := "Install missing components"
	hasFailures := report != nil && !report.OK
	opts := []huh.Option[string]{}
	if hasFailures {
		opts = append(opts, huh.NewOption(missingLabel+" (recommended)", string(SetupActionInstallMissing)))
		opts = append(opts, huh.NewOption("Re-install everything (refresh)", string(SetupActionRefresh)))
	} else {
		opts = append(opts, huh.NewOption("Re-install everything (refresh)", string(SetupActionRefresh)))
	}
	opts = append(opts,
		huh.NewOption("Reconfigure (change preferred agent / scope)", string(SetupActionReconfigure)),
		huh.NewOption("Uninstall everything", string(SetupActionUninstall)),
		huh.NewOption("Exit (do nothing)", string(SetupActionExit)),
	)

	chosen := opts[0].Value
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("What would you like to do?").
				Description("Doctor already ran — pick your next action.").
				Options(opts...).
				Value(&chosen),
		),
	).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return SetupActionExit, g.Error(err, "setup form aborted")
	}
	return SetupAction(chosen), nil
}

// HarnessConfirmResult is the first-run / setup confirm form.
type HarnessConfirmResult struct {
	Agent        string
	HintInErrors bool
	Scope        string
	Components   []string
}

func agentAuthLabel(a RankedAgent) string {
	if a.Bundled {
		choice := HarnessProviderChoice()
		if choice.Disclosure != "" {
			return fmt.Sprintf("%s (bundled fallback) — %s", a.Name, choice.Disclosure)
		}
		return fmt.Sprintf("%s (bundled fallback, keyed provider)", a.Name)
	}
	auth := "no auth"
	switch a.Auth {
	case AuthOK:
		auth = "auth ok"
	case AuthUnknown:
		auth = "auth unknown"
	}
	return fmt.Sprintf("%s (detected, %s)", a.Name, auth)
}

func harnessAgentDescription(ranked []RankedAgent) string {
	base := "Detected agents with install and auth state. Authenticated first."
	hasBundled := false
	for _, a := range ranked {
		if a.Bundled {
			hasBundled = true
			break
		}
	}
	if !hasBundled {
		return base
	}
	choice := HarnessProviderChoice()
	if choice.Disclosure != "" {
		return base + " OpenCode downloads only when no other agent is usable. Keyed alternative: set ANTHROPIC_API_KEY or OPENAI_API_KEY. " + choice.Disclosure + "."
	}
	return base + " OpenCode downloads only when no other agent is usable."
}

// RunHarnessConfirmForm lists detected agents with install+auth state.
func RunHarnessConfirmForm(prefill Profile) (*HarnessConfirmResult, error) {
	agent, err := resolveSetupAgent(prefill.Agent)
	if err != nil {
		return nil, err
	}

	res := &HarnessConfirmResult{
		Agent:        agent,
		HintInErrors: prefill.HintInErrors,
		Scope:        "user",
		Components:   []string{"skills", "mcp"},
	}
	if prefill.DefaultInstallScope != "" {
		res.Scope = prefill.DefaultInstallScope
	}

	fields := []huh.Field{
		huh.NewMultiSelect[string]().
			Title("What would you like to install?").
			Description("Skills are markdown guides; MCP wires the Sling MCP server into each client.").
			Options(
				huh.NewOption("Skills (canonical bundle + per-client redirects)", "skills").Selected(true),
				huh.NewOption("MCP server registration", "mcp").Selected(true),
			).
			Value(&res.Components),
		huh.NewConfirm().
			Title("Show AI hint in run errors?").
			Description("Append `sling assist error <sig>` to failed `sling run` / `conns test` output.").
			Value(&res.HintInErrors),
		huh.NewSelect[string]().
			Title("Install scope").
			Description("Where to wire skills + MCP. `user` writes to ~/.<client>/; `project` writes to ./.<client>/").
			Options(
				huh.NewOption("user (recommended)", "user"),
				huh.NewOption("project", "project"),
			).
			Value(&res.Scope),
	}
	if opts, ranked, ok := setupAgentSelectOptions(); ok {
		fields = append([]huh.Field{
			huh.NewSelect[string]().
				Title("Preferred agent").
				Description(harnessAgentDescription(ranked)).
				Options(opts...).
				Filtering(filteringFor(opts)).
				Value(&res.Agent),
		}, fields...)
	}

	form := huh.NewForm(huh.NewGroup(fields...)).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return nil, g.Error(err, "setup form aborted")
	}
	if len(res.Components) == 0 {
		return nil, g.Error("no components selected")
	}
	return res, nil
}

// InstallFormResult is what the first-run install form returns.
type InstallFormResult struct {
	Agent        string
	HintInErrors bool
	Scope        string
}

// RunInstallForm shows the first-run interactive form and returns the user's
// choices.
func RunInstallForm(prefill Profile) (*InstallFormResult, error) {
	agent, err := resolveSetupAgent(prefill.Agent)
	if err != nil {
		return nil, err
	}

	res := &InstallFormResult{
		Agent:        agent,
		HintInErrors: prefill.HintInErrors,
		Scope:        "user",
	}
	if prefill.DefaultInstallScope != "" {
		res.Scope = prefill.DefaultInstallScope
	}

	fields := []huh.Field{
		huh.NewConfirm().
			Title("Show AI hint in run errors?").
			Description("Append `sling assist error <sig>` to failed `sling run` / `conns test` output.").
			Value(&res.HintInErrors),
		huh.NewSelect[string]().
			Title("Install scope").
			Description("Where to wire skills + MCP. `user` writes to ~/.<client>/; `project` writes to ./.<client>/").
			Options(
				huh.NewOption("user (recommended)", "user"),
				huh.NewOption("project", "project"),
			).
			Value(&res.Scope),
	}
	if opts, ranked, ok := setupAgentSelectOptions(); ok {
		fields = append([]huh.Field{
			huh.NewSelect[string]().
				Title("Preferred agent").
				Description(harnessAgentDescription(ranked)).
				Options(opts...).
				Filtering(filteringFor(opts)).
				Value(&res.Agent),
		}, fields...)
	}

	form := huh.NewForm(huh.NewGroup(fields...)).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return nil, g.Error(err, "install form aborted")
	}
	return res, nil
}

// resolveSetupAgent picks the agent for setup: the only one on PATH, else
// the authenticated one (via a later picker), else bundled OpenCode after confirm.
func resolveSetupAgent(current string) (string, error) {
	agents, bundled := pathRanked()
	switch {
	case len(agents) == 0:
		if bundled == nil {
			return "", g.Error("no AI agent on $PATH; install one of: claude, codex, gemini, cursor, opencode, pi, grok")
		}
		if err := confirmInstallOpenCode(); err != nil {
			return "", err
		}
		return bundled.Name, nil
	case len(agents) == 1:
		return agents[0].Name, nil
	default:
		if current != "" {
			for _, a := range agents {
				if a.Name == current {
					return current, nil
				}
			}
		}
		return agents[0].Name, nil
	}
}

// setupAgentSelectOptions is the picker for two or more PATH agents.
func setupAgentSelectOptions() ([]huh.Option[string], []RankedAgent, bool) {
	agents, _ := pathRanked()
	if len(agents) < 2 {
		return nil, agents, false
	}
	opts := make([]huh.Option[string], 0, len(agents))
	for i, a := range agents {
		label := agentAuthLabel(a)
		if i == 0 {
			label += " — recommended"
		}
		opts = append(opts, huh.NewOption(label, a.Name))
	}
	return opts, agents, true
}

func confirmInstallOpenCode() error {
	ok := false
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("No AI agent on $PATH").
				Description("Sling can install OpenCode and use it as the agent.").
				Affirmative("Install OpenCode").
				Negative("Cancel").
				Value(&ok),
		),
	).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return g.Error(err, "setup form aborted")
	}
	if !ok {
		return ErrUserAborted
	}
	return nil
}

func EnsureAssistReady() error {
	prof, exists, err := LoadProfile()
	if err != nil {
		return err
	}
	if !exists {
		return g.Error("Sling assist is not set up yet. Run:\n\n  sling assist setup\n")
	}
	if _, err := ResolveAgent("", prof); err != nil {
		return g.Error("no AI agent ready for assist. Run:\n\n  sling assist setup\n\n(%s)", err.Error())
	}
	return nil
}

func filteringFor(opts []huh.Option[string]) bool {
	return len(opts) >= 6
}
