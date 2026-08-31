package evals

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/sling/assist"
	"gopkg.in/yaml.v3"
)

// liveArms are the real (non-mock, non-noskills) agent harnesses.
var liveArms = []string{"claude", "grok", "opencode2", "codex"}

func isLiveArm(name string) bool {
	return contains(liveArms, name)
}

func evalsDirFromCaller() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		wd, _ := os.Getwd()
		return wd
	}
	return filepath.Dir(file)
}

// Flags are TestEvalAssist CLI flags (args after --).
type Flags struct {
	Arms          []string
	Tags          []string
	Tiers         []string
	Cases         []string
	Trials        int
	Baseline      string
	MaxSuiteUSD   float64
	Parallel      int
	ResetFixtures bool
	RetryFailed   bool
}

// DefaultFlags returns suite defaults.
func DefaultFlags() Flags {
	return Flags{
		Arms:        []string{"mock", "claude", "grok", "opencode2", "codex"},
		Trials:      0, // use per-case
		MaxSuiteUSD: 15,
		Parallel:    4,
		RetryFailed: true,
	}
}

// ParseFlags reads os.Args after --.
func ParseFlags(args []string) Flags {
	f := DefaultFlags()
	dash := false
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			dash = true
			continue
		}
		if !dash && !strings.HasPrefix(a, "--") {
			continue
		}
		name, val, hasVal := splitArg(a)
		next := func() string {
			if hasVal {
				return val
			}
			if i+1 < len(args) {
				i++
				return args[i]
			}
			return ""
		}
		switch name {
		case "--arms":
			f.Arms = splitCSV(next())
		case "--tags":
			f.Tags = splitCSV(next())
		case "--cases":
			f.Cases = splitCSV(next())
		case "--trials":
			fmt.Sscanf(next(), "%d", &f.Trials)
		case "--baseline":
			f.Baseline = next()
		case "--max-suite-usd":
			fmt.Sscanf(next(), "%f", &f.MaxSuiteUSD)
		case "--parallel":
			fmt.Sscanf(next(), "%d", &f.Parallel)
		case "--tier", "--tiers":
			f.Tiers = splitCSV(next())
		case "--reset-fixtures":
			f.ResetFixtures = true
			if hasVal && (val == "false" || val == "0") {
				f.ResetFixtures = false
			}
		case "--retry-failed":
			f.RetryFailed = true
			if hasVal && (val == "false" || val == "0") {
				f.RetryFailed = false
			}
		}
	}
	if f.Parallel <= 0 {
		f.Parallel = 4
	}
	if f.MaxSuiteUSD <= 0 {
		f.MaxSuiteUSD = 15
	}
	return f
}

func splitArg(a string) (name, val string, hasVal bool) {
	if i := strings.Index(a, "="); i >= 0 {
		return a[:i], a[i+1:], true
	}
	return a, "", false
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ArmAvail describes whether an arm can run.
type ArmAvail struct {
	Name   string
	Skip   string // empty = available
	Binary string
}

func CheckArm(name string) ArmAvail {
	switch name {
	case "mock", "noskills":
		return ArmAvail{Name: name}
	case "claude":
		bin, err := exec.LookPath("claude")
		if err != nil {
			return ArmAvail{Name: name, Skip: "missing binary: claude"}
		}
		if !HostHasClaudeAuth() {
			return ArmAvail{Name: name, Skip: "missing login (ANTHROPIC_API_KEY or ~/.claude)"}
		}
		return ArmAvail{Name: name, Binary: bin}
	case "grok":
		bin, err := exec.LookPath("grok")
		if err != nil {
			return ArmAvail{Name: name, Skip: "missing binary: grok"}
		}
		if !HostHasGrokAuth() {
			return ArmAvail{Name: name, Skip: "missing login (XAI_API_KEY or ~/.grok/auth.json)"}
		}
		return ArmAvail{Name: name, Binary: bin}
	case "opencode2":
		bin, err := findOpenCode2Bin()
		if err != nil {
			return ArmAvail{Name: name, Skip: "missing binary: opencode2"}
		}
		if !HostHasOpenCode2Auth() {
			return ArmAvail{Name: name, Skip: "missing login (provider API key or opencode auth)"}
		}
		return ArmAvail{Name: name, Binary: bin}
	case "codex":
		bin, err := findCodexBin()
		if err != nil {
			return ArmAvail{Name: name, Skip: "missing binary: codex"}
		}
		if !HostHasCodexAuth() {
			return ArmAvail{Name: name, Skip: "missing login (OPENAI_API_KEY/CODEX_API_KEY or ~/.codex/auth.json)"}
		}
		return ArmAvail{Name: name, Binary: bin}
	default:
		return ArmAvail{Name: name, Skip: "unknown arm"}
	}
}

// findOpenCode2Bin locates the opencode2 binary. OPENCODE2_PATH overrides PATH
// (same idea as SLING_BIN). Beta has no standalone GitHub assets — do not download.
func findOpenCode2Bin() (string, error) {
	if v := os.Getenv("OPENCODE2_PATH"); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", err
		}
		st, err := os.Stat(abs)
		if err != nil || st.IsDir() {
			return "", fmt.Errorf("OPENCODE2_PATH=%s: missing", abs)
		}
		return abs, nil
	}
	return exec.LookPath("opencode2")
}

// findCodexBin locates the codex binary. CODEX_PATH overrides PATH.
// Do not download — the host must already have Codex CLI installed.
func findCodexBin() (string, error) {
	if v := os.Getenv("CODEX_PATH"); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", err
		}
		st, err := os.Stat(abs)
		if err != nil || st.IsDir() {
			return "", fmt.Errorf("CODEX_PATH=%s: missing", abs)
		}
		return abs, nil
	}
	return exec.LookPath("codex")
}

// evalAgentSystemPrompt keeps the live agent inside the workdir.
// Claude keeps the host HOME for keychain login, so this block is required.
// One constant: claude --append-system-prompt, grok --rules, opencode2
// agents.eval.system, and the codex exec prompt (plus $CODEX_HOME/AGENTS.md).
const evalAgentSystemPrompt = `You are in an automated eval. Work only in the current directory.
Do not explore parent directories, other git repos, or the host home.
Check named connections first. If a named connection does not exist, it is a hard stop: refuse, ask one focused question, and write no file.
For every other gap (missing tables, unstated options), resolve via discovery; if still ambiguous, choose the most probable option, state the assumption, and write the artifact.
When the ask names a table but no connection, search every configured DB connection for it (connection discover). Use the connection where the table exists, and discover their columns. Treat a table as absent only when no configured connection has it; then still write the artifact using the requested names.
Never end with only a question unless a named connection is missing.
Do not wait for confirmation before writing the file.
Delete scratch files and directories you created before finishing.
Prefer skills, docs, and --help over reverse-engineering binaries. Time-box exploration.`

// agentEvalArgs is the plan's argv matrix.
func agentEvalArgs(arm, promptPath, work string, budget float64) []string {
	switch arm {
	case "claude":
		args := []string{
			"-p", fmt.Sprintf("Read and execute the task in @%s", promptPath),
			"--output-format", "json",
			"--max-turns", "20",
			"--allowedTools", "Read,Write,Edit,Glob,Grep,Bash(sling *),mcp__sling__*",
			"--permission-mode", "acceptEdits",
			"--mcp-config", filepath.Join(work, ".mcp.json"),
			"--strict-mcp-config",
			"--append-system-prompt", evalAgentSystemPrompt,
		}
		if budget > 0 {
			args = append(args, "--max-budget-usd", fmt.Sprintf("%.2f", budget))
		}
		return args
	case "grok":
		return []string{
			"--prompt-file", promptPath,
			"--always-approve",
			"--max-turns", "25",
			"--output-format", "json",
			"--rules", evalAgentSystemPrompt,
		}
	case "opencode2":
		// System prompt lives on agents.eval.system in the seeded opencode.json.
		// Relative prompt.md: RunTimed already sets cwd to the trial workdir.
		// An absolute /var/folders/... path is treated as external_directory
		// if the process inherits the host repo as the OpenCode project.
		// --format json is the v2 run flag. No --max-turns on run;
		// the eval agent sets steps: 20 and RunTimed is the backstop.
		return []string{
			"run",
			"--standalone",
			"--agent", "eval",
			"--format", "json",
			"Read and execute the task in prompt.md",
		}
	case "codex":
		// Prepend evalAgentSystemPrompt: $CODEX_HOME/AGENTS.md is not a
		// reliable channel for `codex exec` (e.34). Model/provider come
		// from the whitelist-copied config — never pass -m.
		// No turn cap on exec; RunTimed is the backstop.
		return []string{
			"exec",
			"--json",
			"--skip-git-repo-check",
			"--sandbox", "workspace-write",
			"--color", "never",
			evalAgentSystemPrompt + "\n\nRead and execute the task in prompt.md",
		}
	}
	return nil
}

// FindSlingBin locates the built sling binary.
func FindSlingBin() (string, error) {
	if v := os.Getenv("SLING_BIN"); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(abs); err != nil {
			return "", fmt.Errorf("SLING_BIN=%s: %w", abs, err)
		}
		return abs, nil
	}
	cands := []string{
		filepath.Join(evalsDir(), "../../cmd/sling/sling"),
		filepath.Join(evalsDir(), "../../cmd/sling/sling.exe"),
		"./sling",
	}
	for _, c := range cands {
		abs, err := filepath.Abs(c)
		if err != nil {
			continue
		}
		if _, err := os.Stat(abs); err == nil {
			return abs, nil
		}
	}
	return "", fmt.Errorf("sling binary not found (build with: cd cmd/sling && go build .)")
}

// PreflightConns runs `sling conns test` for each name. Down conns are skipped, not failed.
func PreflightConns(bin string, names []string, env []string) map[string]bool {
	down := map[string]bool{}
	for _, n := range names {
		// Isolated file-local conns (DUCKDB, LOCAL, …) must not be opened
		// by the harness — a leftover handle locks the trial's eval.duckdb
		// and races sling build (H6).
		if isolatedConnNames[n] {
			continue
		}
		c := exec.Command(bin, "conns", "test", n)
		c.Env = env
		if err := c.Run(); err != nil {
			down[n] = true
		}
	}
	return down
}

// Suite is the eval run coordinator.
type Suite struct {
	Flags       Flags
	Bin         string
	Writer      *ResultsWriter
	Logf        func(string, ...any)
	Server      *FixtureServer
	Provisioner *Provisioner
	spent       float64
	spentMu     sync.Mutex
}

func (s *Suite) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

func (s *Suite) addCost(c float64) float64 {
	s.spentMu.Lock()
	defer s.spentMu.Unlock()
	s.spent += c
	return s.spent
}

func (s *Suite) overBudget() bool {
	s.spentMu.Lock()
	defer s.spentMu.Unlock()
	return s.spent >= s.Flags.MaxSuiteUSD
}

// SelectCases filters loaded cases by flags.
func SelectCases(all []Case, f Flags) []Case {
	var out []Case
	wantCase := map[string]bool{}
	for _, id := range f.Cases {
		wantCase[id] = true
	}
	for _, c := range all {
		if len(wantCase) > 0 {
			ok := wantCase[c.ID]
			if !ok {
				// prefix match e.01
				for id := range wantCase {
					if strings.HasPrefix(c.ID, id) {
						ok = true
						break
					}
				}
			}
			if !ok {
				continue
			}
		}
		if len(f.Tags) > 0 && !hasAnyTag(c.Tags, f.Tags) {
			continue
		}
		if len(f.Tiers) > 0 && !contains(f.Tiers, c.Tier) {
			continue
		}
		out = append(out, c)
	}
	return out
}

func hasAnyTag(have, want []string) bool {
	set := map[string]bool{}
	for _, t := range have {
		set[t] = true
	}
	for _, t := range want {
		if set[t] {
			return true
		}
	}
	return false
}

// RunSuite executes the selected cases.
func (s *Suite) RunSuite(cases []Case) (*SuiteSummary, error) {
	if s.Server == nil {
		srv, err := StartFixtureServer()
		if err != nil {
			return nil, fmt.Errorf("fixture server: %w", err)
		}
		s.Server = srv
		defer srv.Close()
	}
	if s.Server != nil {
		_ = os.Setenv("EVAL_MOCK_API_URL", s.Server.URL)
	}
	if s.Provisioner == nil && s.Bin != "" {
		p, err := NewProvisioner(s.Bin, os.Environ(), s.logf)
		if err != nil {
			s.logf("fixture registry: %v", err)
		} else {
			s.Provisioner = p
			if s.Flags.ResetFixtures {
				if err := p.Reset(); err != nil {
					s.logf("reset-fixtures: %v", err)
				}
			}
			if err := p.Ensure(collectFixtureNames(cases)); err != nil {
				s.logf("provision: %v", err)
			}
		}
	}
	if s.Flags.MaxSuiteUSD <= 0 {
		s.Flags.MaxSuiteUSD = suiteCapForTiers(cases)
	}
	if wantsJudge(cases) {
		if note := smokeJudge(); note != "" {
			s.logf("judge smoke: %s", note)
		}
	}

	type job struct {
		c   Case
		arm string
	}
	var jobs []job
	armAvail := map[string]ArmAvail{}
	for _, name := range s.Flags.Arms {
		armAvail[name] = CheckArm(name)
	}
	for _, c := range cases {
		for _, arm := range s.Flags.Arms {
			// Case may restrict arms (except mock/noskills always allowed when requested).
			if arm != "mock" && arm != "noskills" && len(c.Arms) > 0 && !contains(c.Arms, arm) {
				continue
			}
			jobs = append(jobs, job{c: c, arm: arm})
		}
	}

	sem := make(chan struct{}, s.Flags.Parallel)
	var wg sync.WaitGroup
	for _, j := range jobs {
		j := j
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.runCaseArm(j.c, j.arm, armAvail[j.arm])
		}()
	}
	wg.Wait()

	if s.Flags.RetryFailed && s.Flags.Parallel > 1 {
		s.retryFailedSequentially(cases, armAvail)
	}

	gating := map[string]bool{}
	meta := map[string]CaseMeta{}
	for _, c := range cases {
		gating[c.ID] = c.IsGating()
		meta[c.ID] = CaseMeta{Gating: c.IsGating(), Tier: c.Tier}
	}
	sum := AggregateSuiteMeta(s.Writer.RunID(), s.Writer.Trials(), meta, 0.90, DefaultTierThresholds)
	sum.Judge = judgeAvailability(s.Writer.Trials())
	sum.Binary = slingBinaryStamp(s.Bin)
	sum.SchemaReset = schemaResetNote(cases)
	if s.Flags.Baseline != "" {
		base, err := LoadJSONL(s.Flags.Baseline)
		if err != nil {
			s.logf("baseline load: %v", err)
		} else {
			flips := PairedDiff(s.Writer.Trials(), base, gating)
			sum.Flips = flips
			if SmokeFlipFail(flips, gating) {
				sum.Verdict = "fail"
			}
			// Attach flip text per arm.
			byArm := map[string][]string{}
			for _, fl := range flips {
				mark := "✓→✗"
				if fl.From == "fail" {
					mark = "✗→✓"
				}
				byArm[fl.Arm] = append(byArm[fl.Arm], fl.Case+" "+mark)
			}
			for arm, row := range sum.Arms {
				row.Flips = strings.Join(byArm[arm], ", ")
				sum.Arms[arm] = row
			}
		}
	}
	return &sum, nil
}

func (s *Suite) runCaseArm(c Case, arm string, avail ArmAvail) {
	if avail.Skip != "" {
		s.logf("SKIPPED arm %s: %s", arm, avail.Skip)
		_ = s.Writer.Append(TrialResult{
			Case: c.ID, Arm: arm, Trial: 1, SkipReason: avail.Skip,
		})
		return
	}
	if s.overBudget() {
		s.logf("SKIPPED %s/%s: suite budget", c.ID, arm)
		_ = s.Writer.Append(TrialResult{
			Case: c.ID, Arm: arm, Trial: 1, SkipReason: "max-suite-usd",
		})
		return
	}

	if arm == "mock" {
		s.runMockInvariants(c)
		return
	}

	k := c.TrialCount(s.Flags.Trials)
	for i := 1; i <= k; i++ {
		if s.overBudget() {
			_ = s.Writer.Append(TrialResult{
				Case: c.ID, Arm: arm, Trial: i, SkipReason: "max-suite-usd",
			})
			return
		}
		tr := s.runTrial(c, arm, i, avail, "", "")
		if tr.InfraError && !tr.Pass {
			s.logf("%s %s t%d infra_error, retrying once", c.ID, arm, i)
			retry := s.runTrial(c, arm, i, avail, "", "")
			retry.Trial = i
			if retry.Error == "" {
				retry.Error = "retried after infra_error"
			}
			tr = retry
		}
		_ = s.Writer.Append(tr)
		s.addCost(tr.CostUSD)
	}
}

func (s *Suite) runMockInvariants(c Case) {
	// Passable: plant expected (or nothing if file_absent is the required outcome).
	tr := s.runTrial(c, "mock", 1, ArmAvail{Name: "mock"}, "passable", c.ExpectedPath())
	tr.Invariant = "passable"
	_ = s.Writer.Append(tr)

	// Sensitivity: each mutant must fail at least one required grader.
	muts := c.MutantPaths()
	if len(muts) == 0 {
		_ = s.Writer.Append(TrialResult{
			Case: c.ID, Arm: "mock", Trial: 2, Invariant: "sensitivity",
			Pass: false, Error: "no mutants shipped",
		})
		return
	}
	for i, m := range muts {
		tr := s.runTrial(c, "mock", i+2, ArmAvail{Name: "mock"}, "sensitivity", m)
		tr.Invariant = "sensitivity"
		_ = s.Writer.Append(tr)
	}
}

// retryFailedSequentially re-runs failed live trials once, one at a time.
// Parallel runs lose trials to scheduler contention (shared fixtures,
// timeouts); the retry result replaces the parallel one so the verdict
// reflects agent quality, not contention.
func (s *Suite) retryFailedSequentially(cases []Case, armAvail map[string]ArmAvail) {
	byID := map[string]Case{}
	for _, c := range cases {
		byID[c.ID] = c
	}
	retried := map[string]bool{} // case|arm|trial already retried
	for _, tr := range s.Writer.Trials() {
		if tr.Pass || tr.SkipReason != "" || tr.Arm == "mock" || tr.Invariant != "" {
			continue
		}
		if tr.Retried {
			continue
		}
		key := tr.Case + "|" + tr.Arm + "|" + fmt.Sprint(tr.Trial)
		if retried[key] {
			continue
		}
		c, ok := byID[tr.Case]
		if !ok {
			continue
		}
		if s.overBudget() {
			s.logf("RETRY-SKIP %s %s t%d: suite budget", tr.Case, tr.Arm, tr.Trial)
			return
		}
		retried[key] = true
		s.logf("RETRY %s %s t%d (sequential)", tr.Case, tr.Arm, tr.Trial)
		retry := s.runTrial(c, tr.Arm, tr.Trial, armAvail[tr.Arm], "", "")
		retry.Retried = true
		if retry.Error == "" && !retry.Pass {
			retry.Error = "sequential retry still failing"
		}
		_ = s.Writer.Replace(retry)
		s.addCost(retry.CostUSD)
	}
}

func suiteCapForTiers(cases []Case) float64 {	capn := DefaultFlags().MaxSuiteUSD
	seen := map[string]bool{}
	for _, c := range cases {
		if seen[c.Tier] {
			continue
		}
		seen[c.Tier] = true
		if v, ok := DefaultTierCostCaps[c.Tier]; ok && v > capn {
			capn = v
		}
	}
	return capn
}

func laneBSkip(c Case) string {
	switch {
	case strings.Contains(c.ID, "spec_real_github"):
		if os.Getenv("GITHUB_TOKEN") == "" {
			return "SKIPPED (missing GITHUB_TOKEN)"
		}
		if !githubTokenOK() {
			return "SKIPPED (bad credential)"
		}
	case strings.Contains(c.ID, "spec_real_omdb"):
		if os.Getenv("OMDB_API_KEY") == "" {
			return "SKIPPED (missing OMDB_API_KEY)"
		}
	case strings.Contains(c.ID, "spec_real_dummyjson"):
		resp, err := httpGet("https://dummyjson.com/test")
		if err != nil || resp >= 500 {
			return "SKIPPED (dummyjson unavailable)"
		}
	}
	return ""
}

func githubTokenOK() bool {
	token := os.Getenv("GITHUB_TOKEN")
	if token == "" {
		return false
	}
	req, err := http.NewRequest(http.MethodGet, "https://api.github.com/user", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "sling-eval")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func httpGet(url string) (int, error) {
	c := exec.Command("curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "--max-time", "5", url)
	out, err := c.Output()
	if err != nil {
		return 0, err
	}
	n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
	return n, nil
}

func (s *Suite) runTrial(c Case, arm string, n int, avail ArmAvail, invariant, plant string) TrialResult {
	start := time.Now()
	tr := TrialResult{Case: c.ID, Arm: arm, Trial: n, Invariant: invariant}
	if skip := laneBSkip(c); skip != "" && arm != "mock" {
		tr.SkipReason = skip
		return tr
	}

	home, err := os.MkdirTemp("", "eval-home-")
	if err != nil {
		tr.Error = err.Error()
		return tr
	}
	defer os.RemoveAll(home)
	work, err := os.MkdirTemp("", "eval-work-")
	if err != nil {
		tr.Error = err.Error()
		return tr
	}
	defer os.RemoveAll(work)

	seedName := "home_claude"
	switch arm {
	case "grok":
		seedName = "home_grok"
	case "opencode2":
		seedName = "home_opencode2"
	case "codex":
		seedName = "home_codex"
	case "noskills":
		seedName = "home_claude"
	}
	if err := SeedHome(home, seedName, arm == "noskills"); err != nil {
		tr.Error = "seed: " + err.Error()
		return tr
	}
	if err := seedDuckDBEngine(home); err != nil {
		s.logf("%s: seed duckdb engine: %v", c.ID, err)
	}
	if err := LinkHostAuth(home, arm); err != nil {
		tr.Error = "auth: " + err.Error()
		return tr
	}
	if err := seedCaseFiles(c, work); err != nil {
		tr.Error = "seed files: " + err.Error()
		return tr
	}
	if err := seedReferencedSuiteFiles(c, work); err != nil {
		tr.Error = "seed suite files: " + err.Error()
		return tr
	}
	if contains(c.Fixtures, "tpch_duckdb") {
		if err := copyFile(tpchDuckPath(), filepath.Join(work, "eval.duckdb")); err != nil && arm != "mock" {
			// Copy is best-effort; query graders skip if the file is empty.
			s.logf("%s: copy tpch duckdb: %v", c.ID, err)
		}
	}

	envv := withCaseEnv(trialEnv(home, work, s.Bin), work, c)
	if isLiveArm(arm) {
		// Project-local skills so the agent still sees the shipped bundle
		// when Claude keeps the host HOME (keychain login needs it).
		// grok, opencode2, and codex scan project .agents/skills natively.
		_ = writeEmbeddedSkills(filepath.Join(work, ".agents", "skills"))
		_ = writeEmbeddedSkills(filepath.Join(work, ".claude", "skills"))
	}
	if arm == "opencode2" {
		if err := seedOpenCode2Project(home, work); err != nil {
			tr.Error = "opencode2 project: " + err.Error()
			return tr
		}
	}
	if arm != "mock" {
		if err := seedTrialMCP(work, s.Bin, arm); err != nil {
			tr.Error = "mcp: " + err.Error()
			return tr
		}
	}
	down := map[string]bool{}
	if arm != "mock" {
		down = PreflightConns(s.Bin, c.UsedConnections(), envv)
		// Isolated conns (DUCKDB, MOCK_API) must not skip the trial.
		for name, isDown := range down {
			if isDown && !isolatedConnNames[name] {
				msg := fmt.Sprintf("SKIPPED (conn down: %s)", name)
				s.logf("%s %s", c.ID, msg)
				tr.SkipReason = msg
				tr.DurationS = time.Since(start).Seconds()
				return tr
			}
		}
	} else {
		// Mock: mark conns down so L5 graders skip instead of fail.
		for _, name := range c.UsedConnections() {
			down[name] = true
		}
		// If a live conn is actually up, un-mark so L5 can run.
		live := PreflightConns(s.Bin, c.UsedConnections(), envv)
		for _, name := range c.UsedConnections() {
			if !live[name] {
				down[name] = true
			} else {
				delete(down, name)
			}
		}
	}

	// Per-trial drop of declared schemas on shared conns.
	if arm != "mock" {
		if note := resetTrialSchemas(s.Bin, envv, c, down); note != "" {
			s.logf("%s %s t%d schema_reset: %s", c.ID, arm, n, note)
		}
	}

	// Setup hooks (best-effort; skip on mock when conn is down).
	if err := runHookCmds(s.Bin, work, envv, c.Setup, down); err != nil && arm != "mock" {
		tr.Error = "setup: " + err.Error()
		tr.DurationS = time.Since(start).Seconds()
		return tr
	}

	promptPath := filepath.Join(work, "prompt.md")
	if err := renderPrompt(s.Bin, work, envv, c, promptPath); err != nil {
		tr.Error = "render: " + err.Error()
		tr.DurationS = time.Since(start).Seconds()
		return tr
	}
	// Doctor probes the host profile, not the trial. Rewrite the MCP line
	// so the prompt matches what this arm actually has (H1).
	if err := rewritePromptMCPWired(promptPath, mcpWiredForArm(arm)); err != nil {
		s.logf("%s: rewrite MCP wired: %v", c.ID, err)
	}
	if b, err := os.ReadFile(promptPath); err == nil {
		sum := md5.Sum(b)
		tr.PromptMD5 = hex.EncodeToString(sum[:])
	}
	tr.SkillsVersion = skillStamp(home)

	transcript := ""
	if arm == "mock" {
		tlog, err := runMockAgent(c, work, plant, invariant)
		if err != nil {
			tr.Error = "mock: " + err.Error()
			tr.DurationS = time.Since(start).Seconds()
			return tr
		}
		transcript = tlog
	} else {
		tlog, cost, turns, model, err := runAgent(avail, arm, promptPath, work, withCaseEnv(agentEnv(home, work, s.Bin, arm), work, c), c)
		transcript = tlog
		tr.CostUSD = cost
		tr.Turns = turns
		tr.Model = model
		if arm == "codex" && tr.Model == "" {
			tr.Model = codexConfigModel(home)
		}
		if err != nil {
			tr.Error = "agent: " + err.Error()
			if strings.Contains(err.Error(), "timeout after") {
				tr.Timeout = true
			}
		}
		if arm == "codex" {
			if n, failed := countCodexMCPCalls(transcript); n > 0 && failed == n {
				codexMCPFailOnce.Do(func() {
					codexMCPFailLog("codex MCP unavailable: all %d mcp_tool_call items failed", n)
				})
				tr.InfraError = true
			}
		}
		if licenseGateBlocked(transcript) {
			licenseGateOnce.Do(func() {
				licenseGateLog("arm-side Pro token missing — live api tests were blocked")
			})
			tr.InfraError = true
		}
	}

	// Reap leftover helpers (DuckDB MCP child, sling-spawned duckdb)
	// before graders open eval.duckdb.
	if arm != "mock" {
		reapTrialChildren(home, work)
	}

	// Persist transcript + small workdir text files for triage.
	trialDir := filepath.Join(s.Writer.path, "..", s.Writer.runID, c.ID, arm, fmt.Sprintf("t%d", n))
	_ = os.MkdirAll(trialDir, 0o755)
	_ = os.WriteFile(filepath.Join(trialDir, "transcript.log"), []byte(transcript), 0o644)
	persistTrialWork(filepath.Join(trialDir, "work"), work)

	baseURL := ""
	if s.Server != nil {
		baseURL = s.Server.URL
	}
	gc := GradeContext{
		WorkDir:        work,
		Artifact:       c.Artifact,
		CaseDir:        c.CaseDir,
		Transcript:     transcript,
		SlingBin:       s.Bin,
		Env:            envv,
		ConnDown:       down,
		SkipExecute:    arm == "mock",
		FixtureBaseURL: baseURL,
		FixtureToken:   fixtureToken,
		APIPageCap:     50,
		APITimeout:     20 * time.Second,
	}
	graders := RunGraders(gc, c.Graders.Required, c.Graders.Optional)

	// Judge: trend only, never gates. Skip when no judge binary.
	if len(c.Graders.Judge) > 0 {
		artBody := ""
		if b, err := os.ReadFile(gc.artifactPath()); err == nil {
			artBody = string(b)
		}
		jv := RunJudge(c.Graders.Judge, c.Intention, artBody, transcript, trialDir)
		graders = append(graders, jv...)
	}

	tr.Graders = graders
	tr.Score = WeightedScore(graders)
	tr.Pass = TrialPasses(graders)
	tr.Binary = slingBinaryStamp(s.Bin)
	if isInfraFailure(tr.Error, graders) {
		tr.InfraError = true
	}
	if invariant == "sensitivity" {
		// Sensitivity passes the *invariant* when the trial FAILS required graders.
		tr.Pass = !TrialPasses(graders)
	}
	tr.DurationS = time.Since(start).Seconds()

	if err := runHookCmds(s.Bin, work, envv, c.Teardown, down); err != nil {
		s.logf("%s %s t%d teardown: %v", c.ID, arm, n, err)
	}
	return tr
}

func trialEnv(home, work, bin string) []string {
	envv := dropEnvKeys(os.Environ(),
		"HOME", "SLING_HOME_DIR", "CLAUDE_CONFIG_DIR",
		"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME",
	)
	envv = append(envv,
		"HOME="+home,
		"SLING_HOME_DIR="+filepath.Join(home, ".sling"),
		"NO_COLOR=1",
		"PATH="+filepath.Join(home, "bin")+string(os.PathListSeparator)+filepath.Dir(bin)+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	if os.Getenv("GITHUB_TOKEN") != "" && os.Getenv("GITHUB_USERNAME") == "" {
		envv = append(envv, "GITHUB_USERNAME=sling-eval")
	}
	if u := os.Getenv("EVAL_MOCK_API_URL"); u != "" {
		envv = append(envv, "EVAL_MOCK_API_URL="+u)
	}
	return envv
}

// withCaseEnv adds opt-in case env. SLING_STATE changes engine behavior for
// every replication mode, so it is never suite-global.
func withCaseEnv(envv []string, work string, c Case) []string {
	for k, v := range c.Env {
		envv = append(envv, k+"="+os.ExpandEnv(v))
	}
	if needsSlingState(c) && !hasEnvKey(envv, "SLING_STATE") {
		dir := filepath.Join(work, ".sling-state")
		_ = os.MkdirAll(dir, 0o755)
		envv = append(envv, "SLING_STATE=LOCAL/"+filepath.ToSlash(dir))
	}
	return envv
}

func needsSlingState(c Case) bool {
	if c.HasTag("cdc") {
		return true
	}
	if c.Env != nil {
		if _, ok := c.Env["SLING_STATE"]; ok {
			return true
		}
	}
	return false
}

func hasEnvKey(envv []string, key string) bool {
	prefix := key + "="
	for _, kv := range envv {
		if strings.HasPrefix(kv, prefix) {
			return true
		}
	}
	return false
}

func isInfraFailure(agentErr string, graders []GraderResult) bool {
	if looksLikeInfra(agentErr) {
		return true
	}
	for _, g := range graders {
		if g.Pass || g.Skip {
			continue
		}
		if looksLikeInfra(g.Detail) {
			return true
		}
	}
	return false
}

func looksLikeInfra(s string) bool {
	low := strings.ToLower(s)
	return strings.Contains(low, "conflicting lock") ||
		strings.Contains(low, "could not set lock") ||
		strings.Contains(low, "connection reset") ||
		strings.Contains(low, "broken pipe") ||
		strings.Contains(low, "i/o timeout") ||
		strings.Contains(low, "malformed mach-o") ||
		strings.Contains(low, "exec format error") ||
		strings.Contains(low, "text file busy")
}

// reapTrialChildren kills leftover processes whose cwd is the trial workdir
// or whose binary lives under the trial eval-home (DuckDB helper, MCP).
func reapTrialChildren(home, work string) {
	if home == "" || work == "" {
		return
	}
	out, err := exec.Command("ps", "-ax", "-o", "pid=,command=").Output()
	if err != nil {
		return
	}
	homeAbs, _ := filepath.Abs(home)
	workAbs, _ := filepath.Abs(work)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		pidStr, cmd, _ := strings.Cut(line, " ")
		pid, err := strconv.Atoi(strings.TrimSpace(pidStr))
		if err != nil || pid <= 1 {
			continue
		}
		if !strings.Contains(cmd, homeAbs) && !strings.Contains(cmd, workAbs) &&
			!strings.Contains(cmd, filepath.Join(home, ".sling", "bin")) {
			continue
		}
		// Don't kill the current process or its parent.
		if pid == os.Getpid() || pid == os.Getppid() {
			continue
		}
		p, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		_ = p.Signal(os.Interrupt)
		time.Sleep(50 * time.Millisecond)
		_ = p.Kill()
	}
}

func dropEnvKeys(envv []string, keys ...string) []string {
	drop := map[string]bool{}
	for _, k := range keys {
		drop[k] = true
	}
	out := make([]string, 0, len(envv))
	for _, kv := range envv {
		name, _, _ := strings.Cut(kv, "=")
		if drop[name] {
			continue
		}
		out = append(out, kv)
	}
	return out
}

// agentEnv is the child env for live arms.
// Grok and opencode2 keep the sandbox HOME (file auth, no keychain).
// Claude Code stores OAuth in the macOS keychain under the host HOME,
// so a file symlink is not enough — restore the host HOME for Claude.
func agentEnv(home, work, bin, arm string) []string {
	envv := trialEnv(home, work, bin)
	switch arm {
	case "claude":
		host := hostHome()
		out := dropEnvKeys(envv, "HOME", "CLAUDE_CONFIG_DIR")
		out = append(out, "HOME="+host)
		return out
	case "opencode2":
		// Pin XDG + OPENCODE_DB so config, DB, and service state stay inside
		// the sandbox. Combined with --standalone this is parallel-safe.
		// Drop parent-session vars (OPENCODE, OPENCODE_SESSION, …) and force
		// PWD=work so a suite launched from inside OpenCode does not inherit
		// the host repo as the project (that made the temp workdir "external").
		out := dropEnvKeys(envv,
			"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME", "OPENCODE_DB",
			"PWD", "OPENCODE", "OPENCODE_SESSION", "OPENCODE_TERMINAL",
			"OPENCODE_SERVER", "OPENCODE_CONFIG",
		)
		out = append(out,
			"PWD="+work,
			"XDG_CONFIG_HOME="+filepath.Join(home, ".config"),
			"XDG_DATA_HOME="+filepath.Join(home, ".local", "share"),
			"XDG_STATE_HOME="+filepath.Join(home, ".local", "state"),
			"OPENCODE_DB="+filepath.Join(work, "opencode.db"),
		)
		return out
	case "codex":
		out := dropEnvKeys(envv, "CODEX_HOME")
		out = append(out, "CODEX_HOME="+filepath.Join(home, ".codex"))
		return out
	default:
		return envv
	}
}

func renderPrompt(bin, work string, envv []string, c Case, out string) error {
	ask := c.Intention
	if c.EditPath != "" {
		ask = "Update " + c.EditPath + ": " + ask
	}
	args := []string{"assist", "--out", out, ask}
	cmd := exec.Command(bin, args...)
	cmd.Dir = work
	cmd.Env = envv
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w", buf.String(), err)
	}
	if _, err := os.Stat(out); err != nil {
		return fmt.Errorf("render wrote no file: %w", err)
	}
	return nil
}

func runMockAgent(c Case, work, plant, invariant string) (string, error) {
	var transcript string
	if invariant == "sensitivity" {
		transcript = c.Mock.MutantTranscript
	} else {
		transcript = c.Mock.Transcript
	}
	if transcript == "" && invariant != "sensitivity" {
		// Default passable transcript mentions any transcript_contains needle.
		for _, g := range c.Graders.Required {
			if v := g["transcript_contains"]; v != nil {
				transcript = castToString(v)
			}
		}
	}
	if plant == "" {
		// file_absent passable: write no artifact.
		return transcript + "\nmock planted nothing\n", nil
	}
	dst := filepath.Join(work, c.Artifact)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return transcript, err
	}
	info, err := os.Stat(plant)
	if err != nil {
		return transcript, err
	}
	if info.IsDir() {
		if err := copyTree(plant, dstDir(dst, c.Artifact)); err != nil {
			return transcript, err
		}
	} else {
		if err := copyFile(plant, dst); err != nil {
			return transcript, err
		}
	}
	if invariant == "sensitivity" && transcript == "" {
		// Mutant that should mention a run invocation for e.26.
		for _, g := range c.Graders.Required {
			if v := g["transcript_absent"]; v != nil {
				transcript = "sling run -r out.yaml\n"
			}
		}
	}
	return transcript + "\nmock planted " + c.Artifact + " from " + plant + "\n", nil
}

func dstDir(dst, artifact string) string {
	if artifact == "." || artifact == "./" {
		return filepath.Dir(dst)
	}
	// If artifact is a file name, copy dir contents next to it.
	if strings.Contains(artifact, string(os.PathSeparator)) || strings.HasSuffix(artifact, "/") {
		return dst
	}
	return filepath.Dir(dst)
}

func runAgent(avail ArmAvail, arm, promptPath, work string, envv []string, c Case) (transcript string, cost float64, turns int, model string, err error) {
	args := agentEvalArgs(arm, promptPath, work, c.BudgetUSD)
	if arm == "codex" {
		for _, kv := range envv {
			if strings.HasPrefix(kv, "SLING_HOME_DIR=") {
				prompt := args[len(args)-1]
				args = append(append(args[:len(args)-1], "--add-dir", strings.TrimPrefix(kv, "SLING_HOME_DIR=")), prompt)
				break
			}
		}
	}
	timeout := armTimeout(arm, c.TimeoutDur())
	if arm == "codex" {
		// Keep stderr off the graded transcript (codex prints a stdin
		// banner there). JSONL stays on stdout.
		transcript, err = RunTimedStdout(avail.Binary, args, work, envv, timeout)
	} else {
		transcript, err = RunTimed(avail.Binary, args, work, envv, timeout)
	}
	cost, turns, model = parseAgentTelemetry(arm, transcript)
	return
}

func armTimeoutMultiplier(arm string) float64 {
	switch arm {
	case "opencode2", "codex":
		return 2
	default:
		return 1
	}
}

func armTimeout(arm string, base time.Duration) time.Duration {
	m := armTimeoutMultiplier(arm)
	if m <= 1 {
		return base
	}
	return time.Duration(float64(base) * m)
}

func parseAgentTelemetry(arm, transcript string) (cost float64, turns int, model string) {
	switch arm {
	case "claude":
		return parseClaudeTelemetry(transcript)
	case "opencode2":
		return parseOpenCode2Telemetry(transcript)
	case "codex":
		return parseCodexTelemetry(transcript)
	default:
		return 0, 0, ""
	}
}

func parseClaudeTelemetry(transcript string) (cost float64, turns int, model string) {
	// Claude --output-format json: look for total_cost_usd.
	if i := strings.LastIndex(transcript, `"total_cost_usd"`); i >= 0 {
		rest := transcript[i:]
		fmt.Sscanf(rest, `"total_cost_usd": %f`, &cost)
	}
	if i := strings.Index(transcript, `"num_turns"`); i >= 0 {
		rest := transcript[i:]
		fmt.Sscanf(rest, `"num_turns": %d`, &turns)
	}
	if i := strings.Index(transcript, `"model"`); i >= 0 {
		rest := transcript[i:]
		var m string
		fmt.Sscanf(rest, `"model": %q`, &m)
		model = m
	}
	return
}

// opencode2CostLog is swapped in tests. Logs once per process when cost is absent.
var opencode2CostLog = func(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

var opencode2CostOnce sync.Once

func parseOpenCode2Telemetry(transcript string) (cost float64, turns int, model string) {
	foundCost := false
	extract := func(raw []byte) {
		var m map[string]any
		if json.Unmarshal(raw, &m) != nil {
			return
		}
		if v := jsonMapString(m, "model"); v != "" {
			model = v
		}
		if c, ok := jsonMapFloat(m, "cost", "cost_usd", "total_cost_usd"); ok {
			cost = c
			foundCost = true
		}
		if u, ok := asMap(m["usage"]); ok {
			if c, ok := jsonMapFloat(u, "cost", "cost_usd"); ok {
				cost = c
				foundCost = true
			}
		}
		if t, ok := jsonMapInt(m, "turns", "steps", "num_turns"); ok {
			turns = t
		}
	}

	trimmed := strings.TrimSpace(transcript)
	extract([]byte(trimmed))
	if i := strings.Index(transcript, "{"); i >= 0 {
		if j := strings.LastIndex(transcript, "}"); j > i {
			extract([]byte(transcript[i : j+1]))
		}
	}
	for _, line := range strings.Split(transcript, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "{") {
			extract([]byte(line))
		}
	}
	if !foundCost {
		opencode2CostOnce.Do(func() {
			opencode2CostLog("opencode2 cost is not tracked (no structured cost in transcript)")
		})
	}
	return
}

// codexCostLog is swapped in tests. Logs once per process when cost is absent.
var codexCostLog = func(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
}

var codexCostOnce sync.Once

var (
	codexMCPFailOnce sync.Once
	codexMCPFailLog  = func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
)

var (
	codexTokenWarnOnce sync.Once
	codexTokenWarnLog  = func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
)

// licenseGateBlocked reports that the arm-side MCP server hit the Pro
// license gate, so live api_spec tests silently degraded to guesses.
var (
	licenseGateOnce sync.Once
	licenseGateLog  = func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
)

func licenseGateBlocked(transcript string) bool {
	return strings.Contains(transcript, "unable to validate CLI Pro token")
}

func parseCodexTelemetry(transcript string) (cost float64, turns int, model string) {
	codexCostOnce.Do(func() {
		codexCostLog("codex cost is not tracked (tokens only; no USD in transcript)")
	})
	for _, line := range strings.Split(transcript, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if v := jsonMapString(m, "model"); v != "" {
			model = v
		}
		if item, ok := asMap(m["item"]); ok {
			if v := jsonMapString(item, "model"); v != "" {
				model = v
			}
		}
		if jsonMapString(m, "type") == "turn.completed" {
			turns++
		}
	}
	return 0, turns, model
}

func jsonMapString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func jsonMapFloat(m map[string]any, keys ...string) (float64, bool) {
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch n := v.(type) {
		case float64:
			return n, true
		case json.Number:
			f, err := n.Float64()
			if err == nil {
				return f, true
			}
		case string:
			f, err := strconv.ParseFloat(n, 64)
			if err == nil {
				return f, true
			}
		}
	}
	return 0, false
}

func jsonMapInt(m map[string]any, keys ...string) (int, bool) {
	for _, k := range keys {
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch n := v.(type) {
		case float64:
			return int(n), true
		case json.Number:
			i, err := n.Int64()
			if err == nil {
				return int(i), true
			}
		case int:
			return n, true
		}
	}
	return 0, false
}

func runHookCmds(bin, work string, envv []string, hooks []any, down map[string]bool) error {
	for _, h := range hooks {
		m, ok := asMap(h)
		if !ok {
			continue
		}
		cmd := castToString(m["sling"])
		if cmd == "" {
			continue
		}
		fields := strings.Fields(cmd)
		// Skip conns exec when that conn is down.
		if len(fields) >= 3 && fields[0] == "conns" {
			if down[fields[2]] {
				continue
			}
		}
		c := exec.Command(bin, fields...)
		c.Dir = work
		c.Env = envv
		var stdout, stderr bytes.Buffer
		c.Stdout = &stdout
		c.Stderr = &stderr
		err := c.Run()
		if rel := castToString(m["stderr"]); rel != "" {
			body := stderr.Bytes()
			if len(bytes.TrimSpace(body)) == 0 {
				body = append(append([]byte{}, stdout.Bytes()...), stderr.Bytes()...)
			}
			_ = os.WriteFile(filepath.Join(work, rel), body, 0o644)
		}
		if rel := castToString(m["stdout"]); rel != "" {
			_ = os.WriteFile(filepath.Join(work, rel), stdout.Bytes(), 0o644)
		}
		if err != nil {
			if hookAllowFail(m["allow_fail"]) {
				continue
			}
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = strings.TrimSpace(stdout.String())
			}
			if msg != "" {
				return fmt.Errorf("%s: %w", msg, err)
			}
			return err
		}
	}
	return nil
}

// seedReferencedSuiteFiles copies suite-level files the intention names
// into the trial workdir. e.36 asks for fixtures/data/ecom/*.csv but
// declares no fixtures: — same class as e.15.
func seedReferencedSuiteFiles(c Case, work string) error {
	intention := strings.ToLower(c.Intention)
	if !strings.Contains(intention, "fixtures/data/ecom") {
		return nil
	}
	src := filepath.Join(fixturesDir(), "data", "ecom")
	dst := filepath.Join(work, "fixtures", "data", "ecom")
	if _, err := os.Stat(src); err != nil {
		return err
	}
	return copyTree(src, dst)
}

func schemaResetNote(cases []Case) string {
	seen := map[string]bool{}
	var parts []string
	for _, c := range cases {
		for _, sch := range c.ResetSchemas {
			if seen[sch] {
				continue
			}
			seen[sch] = true
			parts = append(parts, sch)
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "per-trial drop of " + strings.Join(parts, ",")
}

// resetTrialSchemas drops case-declared schemas on shared (non-isolated)
// connections so build leftovers do not leak across trials.
func resetTrialSchemas(bin string, envv []string, c Case, down map[string]bool) string {
	schemas := c.ResetSchemas
	if len(schemas) == 0 {
		return ""
	}
	var notes []string
	for _, conn := range c.UsedConnections() {
		if isolatedConnNames[conn] || down[conn] {
			continue
		}
		q := dropSchemaSQL(conn, schemas)
		if q == "" {
			continue
		}
		cmd := exec.Command(bin, "conns", "exec", conn, q)
		cmd.Env = envv
		var buf bytes.Buffer
		cmd.Stdout = &buf
		cmd.Stderr = &buf
		if err := cmd.Run(); err != nil {
			notes = append(notes, conn+": "+strings.TrimSpace(buf.String()))
			continue
		}
		notes = append(notes, conn+" ok")
	}
	return strings.Join(notes, "; ")
}

func dropSchemaSQL(conn string, schemas []string) string {
	var parts []string
	switch strings.ToUpper(conn) {
	case "CLICKHOUSE":
		for _, s := range schemas {
			parts = append(parts, "drop database if exists "+s)
		}
	default:
		// POSTGRES, MYSQL, and other SQL conns.
		for _, s := range schemas {
			parts = append(parts, "drop schema if exists "+s+" cascade")
		}
	}
	return strings.Join(parts, "; ")
}

func seedCaseFiles(c Case, work string) error {
	if c.EditPath != "" {
		src := filepath.Join(c.CaseDir, "seed", filepath.Base(c.EditPath))
		if _, err := os.Stat(src); err == nil {
			dst := filepath.Join(work, c.EditPath)
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			if err := copyFile(src, dst); err != nil {
				return err
			}
		}
	}
	seedDir := filepath.Join(c.CaseDir, "seed")
	if st, err := os.Stat(seedDir); err == nil && st.IsDir() {
		if err := copyTree(seedDir, work); err != nil {
			return err
		}
	}
	for rel, body := range c.SeedFiles {
		dst := filepath.Join(work, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, []byte(body), 0o644); err != nil {
			return err
		}
	}
	return nil
}

// SeedHome copies fixtures/home_* and overlays embedded skills.
func SeedHome(home, fixture string, noSkills bool) error {
	src := filepath.Join(fixturesDir(), fixture)
	if _, err := os.Stat(src); err != nil {
		return fmt.Errorf("seed dir missing: %s", src)
	}
	if err := copyTree(src, home); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(home, "bin"), 0o755); err != nil {
		return err
	}
	if err := injectHostConns(filepath.Join(home, ".sling", "env.yaml")); err != nil {
		return err
	}
	if noSkills {
		_ = os.RemoveAll(filepath.Join(home, ".agents", "skills"))
		_ = os.RemoveAll(filepath.Join(home, ".claude", "skills"))
		return nil
	}
	if fixture == "home_opencode2" {
		if err := patchOpenCode2Config(home); err != nil {
			return err
		}
	}
	if fixture == "home_codex" {
		if err := patchCodexConfig(home); err != nil {
			return err
		}
	}
	return writeEmbeddedSkills(filepath.Join(home, ".agents", "skills"))
}

// patchOpenCode2Config writes evalAgentSystemPrompt into agents.eval.system
// and applies EVAL_OPENCODE2_MODEL so the fixture does not duplicate the Go constant.
func patchOpenCode2Config(home string) error {
	p := filepath.Join(home, ".config", "opencode", "opencode.json")
	b, err := os.ReadFile(p)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return fmt.Errorf("opencode.json: %w", err)
	}
	if m := os.Getenv("EVAL_OPENCODE2_MODEL"); m != "" {
		doc["model"] = m
	}
	agents, _ := asMap(doc["agents"])
	if agents == nil {
		agents = map[string]any{}
		doc["agents"] = agents
	}
	eval, _ := asMap(agents["eval"])
	if eval == nil {
		eval = map[string]any{"description": "Automated eval agent"}
		agents["eval"] = eval
	}
	eval["system"] = evalAgentSystemPrompt
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, append(out, '\n'), 0o644)
}

var codexHostConfigKeys = []string{
	"model", "model_provider", "model_reasoning_effort", "model_verbosity", "model_catalog_json",
}

func patchCodexConfig(home string) error {
	p := filepath.Join(home, ".codex", "config.toml")
	doc, err := readTOMLMap(p)
	if err != nil {
		return err
	}
	hostCfg := filepath.Join(hostCodexHome(), "config.toml")
	if host, err := readTOMLMap(hostCfg); err == nil {
		for _, k := range codexHostConfigKeys {
			if v, ok := host[k]; ok {
				doc[k] = v
			}
		}
		if v, ok := host["model_providers"]; ok {
			doc["model_providers"] = v
		}
	}
	if m := os.Getenv("EVAL_CODEX_MODEL"); m != "" {
		doc["model"] = m
	}
	sw, _ := asMap(doc["sandbox_workspace_write"])
	if sw == nil {
		sw = map[string]any{}
		doc["sandbox_workspace_write"] = sw
	}
	sw["writable_roots"] = []any{filepath.Join(home, ".sling")}
	mcp, _ := asMap(doc["mcp_servers"])
	if mcp == nil {
		mcp = map[string]any{}
		doc["mcp_servers"] = mcp
	}
	slingSrv, _ := asMap(mcp["sling"])
	if slingSrv == nil {
		slingSrv = map[string]any{"command": "sling", "args": []any{"serve", "mcp"}}
		mcp["sling"] = slingSrv
	}
	mcpEnv, _ := asMap(slingSrv["env"])
	if mcpEnv == nil {
		mcpEnv = map[string]any{}
	}
	mcpEnv["HOME"] = home
	mcpEnv["SLING_HOME_DIR"] = filepath.Join(home, ".sling")
	if tok := os.Getenv("SLING_CLI_TOKEN"); tok != "" {
		mcpEnv["SLING_CLI_TOKEN"] = tok
	} else {
		codexTokenWarnOnce.Do(func() {
			codexTokenWarnLog("codex: SLING_CLI_TOKEN not set; agent-side api_spec test will fail")
		})
	}
	slingSrv["env"] = mcpEnv
	// Auto-approve sling MCP tools. With approval_policy=never, Codex
	// otherwise fails every mcp_tool_call ("requires approval, but
	// approval policy is never"). Knob: default_tools_approval_mode.
	slingSrv["default_tools_approval_mode"] = "approve"
	slingSrv["tool_timeout_sec"] = 60
	mcp["sling"] = slingSrv
	if catalogPointsAtModelsJSON(fmt.Sprint(doc["model_catalog_json"])) {
		src := filepath.Join(hostCodexHome(), "models.json")
		if fileNonEmpty(src) {
			if err := copyFile(src, filepath.Join(home, ".codex", "models.json")); err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "AGENTS.md"), []byte(evalAgentSystemPrompt+"\n"), 0o644); err != nil {
		return err
	}
	return writeTOMLMap(p, doc)
}

func catalogPointsAtModelsJSON(s string) bool {
	s = strings.TrimSpace(strings.Trim(s, `"`))
	if s == "" || s == "<nil>" {
		return false
	}
	return filepath.Base(s) == "models.json"
}

func readTOMLMap(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	doc := map[string]any{}
	if err := toml.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return doc, nil
}

func writeTOMLMap(path string, doc map[string]any) error {
	var buf bytes.Buffer
	enc := toml.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

func codexConfigModel(home string) string {
	doc, err := readTOMLMap(filepath.Join(home, ".codex", "config.toml"))
	if err != nil {
		return ""
	}
	return jsonMapString(doc, "model")
}

// seedOpenCode2Project copies the seeded config into the trial workdir so v2
// treats that directory as the project root (not the host git repo).
// Use .opencode/opencode.json, not a root opencode.json — sling build would
// otherwise treat a root JSON file as a seed (e.18 saw public.opencode).
func seedOpenCode2Project(home, work string) error {
	src := filepath.Join(home, ".config", "opencode", "opencode.json")
	dst := filepath.Join(work, ".opencode", "opencode.json")
	return copyFile(src, dst)
}

// seedTrialMCP writes project-scope MCP config that points at the trial
// sling binary so live agents can validate/discover without host MCP.
func seedTrialMCP(work, bin, arm string) error {
	if err := writeClaudeMCPJSON(filepath.Join(work, ".mcp.json"), bin); err != nil {
		return err
	}
	switch arm {
	case "grok":
		return writeGrokMCPTOML(filepath.Join(work, ".grok", "config.toml"), bin)
	case "opencode2":
		return patchOpenCode2MCPCommand(filepath.Join(work, ".opencode", "opencode.json"), bin)
	case "codex":
		// MCP lives in the seeded $CODEX_HOME/config.toml as command = "sling".
		// trialEnv prepends the trial binary dir to PATH.
		return nil
	}
	return nil
}

func writeClaudeMCPJSON(path, bin string) error {
	slingSrv := map[string]any{
		"command": bin,
		"args":    []any{"serve", "mcp"},
	}
	// Claude Code merges env over the inherited env; pin the token so the
	// MCP server is not at the mercy of the CLI's inheritance default.
	if tok := os.Getenv("SLING_CLI_TOKEN"); tok != "" {
		slingSrv["env"] = map[string]any{"SLING_CLI_TOKEN": tok}
	}
	doc := map[string]any{
		"mcpServers": map[string]any{
			"sling": slingSrv,
		},
	}
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

func writeGrokMCPTOML(path, bin string) error {
	body := fmt.Sprintf("[mcp_servers.sling]\ncommand = %q\nargs = [\"serve\", \"mcp\"]\n", bin)
	// Grok supports env = { KEY = "value" } in the server table
	// (docs/user-guide/07-mcp-servers.md).
	if tok := os.Getenv("SLING_CLI_TOKEN"); tok != "" {
		body += fmt.Sprintf("env = { SLING_CLI_TOKEN = %q }\n", tok)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(body), 0o644)
}

func patchOpenCode2MCPCommand(path, bin string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		return err
	}
	mcp, _ := asMap(doc["mcp"])
	if mcp == nil {
		mcp = map[string]any{}
		doc["mcp"] = mcp
	}
	servers, _ := asMap(mcp["servers"])
	if servers == nil {
		servers = map[string]any{}
		mcp["servers"] = servers
	}
	slingSrv := map[string]any{
		"type":    "local",
		"command": []any{bin, "serve", "mcp"},
	}
	// OpenCode merges the server environment map over the inherited env.
	if tok := os.Getenv("SLING_CLI_TOKEN"); tok != "" {
		slingSrv["environment"] = map[string]any{"SLING_CLI_TOKEN": tok}
	}
	servers["sling"] = slingSrv
	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// mcpWiredForArm reports whether this arm can actually call Sling MCP.
// Mock has none. Codex MCP is auto-approved via default_tools_approval_mode.
func mcpWiredForArm(arm string) bool {
	switch arm {
	case "mock":
		return false
	default:
		return true
	}
}

// countCodexMCPCalls counts completed mcp_tool_call items in a Codex JSONL transcript.
func countCodexMCPCalls(transcript string) (total, failed int) {
	for _, line := range strings.Split(transcript, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		if jsonMapString(m, "type") != "item.completed" {
			continue
		}
		item, ok := asMap(m["item"])
		if !ok || jsonMapString(item, "type") != "mcp_tool_call" {
			continue
		}
		total++
		if jsonMapString(item, "status") == "failed" {
			failed++
		}
	}
	return
}

// rewritePromptMCPWired replaces the Doctor-probed MCP line with the
// trial-actual state. Live arms seed project MCP; mock has none.
func rewritePromptMCPWired(promptPath string, wired bool) error {
	b, err := os.ReadFile(promptPath)
	if err != nil {
		return err
	}
	want := "- MCP wired: no"
	if wired {
		want = "- MCP wired: yes"
	}
	s := string(b)
	s = strings.ReplaceAll(s, "- MCP wired: yes", want)
	s = strings.ReplaceAll(s, "- MCP wired: no", want)
	return os.WriteFile(promptPath, []byte(s), 0o644)
}

// isolatedConnNames stay as fixture stubs so file DBs do not share host state.
var isolatedConnNames = map[string]bool{
	"DUCKDB": true, "SQLITE": true, "LOCAL": true, "MOCK_API": true,
}

var suiteConnNames = []string{
	"POSTGRES", "MYSQL", "CLICKHOUSE", "DUCKDB", "SQLITE", "LOCAL", "AWS_S3_TEST", "MOCK_API",
}

// injectHostConns copies live suite connection payloads into the sandbox env.yaml.
// Process env vars win, then ~/.sling/env.yaml. File-local conns stay isolated.
func injectHostConns(envPath string) error {
	body, err := os.ReadFile(envPath)
	if err != nil {
		return err
	}
	var doc map[string]any
	if err := yamlUnmarshal(body, &doc); err != nil {
		return err
	}
	if doc == nil {
		doc = map[string]any{}
	}
	conns := map[string]any{}
	if m, ok := asMap(doc["connections"]); ok {
		for k, v := range m {
			conns[strings.ToUpper(k)] = v
		}
	}
	for _, n := range suiteConnNames {
		if v, ok := doc[n]; ok {
			conns[n] = v
			delete(doc, n)
		}
	}

	hostConns := loadHostSlingConns()
	changed := false
	for _, n := range suiteConnNames {
		if isolatedConnNames[n] {
			continue
		}
		if v := os.Getenv(n); v != "" {
			conns[n] = connPayload(v)
			changed = true
			continue
		}
		if v, ok := hostConns[n]; ok {
			conns[n] = v
			changed = true
		}
	}
	if spec := filepath.Join(fixturesDir(), "data", "mock_api.yaml"); fileNonEmpty(spec) {
		if m, ok := asMap(conns["MOCK_API"]); ok {
			m["spec"] = "file://" + spec
			if secrets, ok := asMap(m["secrets"]); ok {
				if u := os.Getenv("EVAL_MOCK_API_URL"); u != "" {
					secrets["base_url"] = u
				}
				m["secrets"] = secrets
			}
			conns["MOCK_API"] = m
			changed = true
		}
	}
	if !changed && doc["connections"] != nil {
		return nil
	}
	doc["connections"] = conns
	out, err := yamlMarshal(doc)
	if err != nil {
		return err
	}
	return os.WriteFile(envPath, out, 0o644)
}

func loadHostSlingConns() map[string]any {
	out := map[string]any{}
	p := filepath.Join(hostHome(), ".sling", "env.yaml")
	b, err := os.ReadFile(p)
	if err != nil {
		return out
	}
	var doc map[string]any
	if yamlUnmarshal(b, &doc) != nil {
		return out
	}
	if m, ok := asMap(doc["connections"]); ok {
		for k, v := range m {
			out[strings.ToUpper(k)] = v
		}
	}
	return out
}

func connPayload(v string) any {
	var m map[string]any
	if yamlUnmarshal([]byte(v), &m) == nil && (m["type"] != nil || m["url"] != nil) {
		return m
	}
	if strings.Contains(v, "://") {
		return map[string]any{"url": v}
	}
	return v
}

func writeEmbeddedSkills(root string) error {
	return fs.WalkDir(assist.SkillsFS, "skills", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("skills", path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		dst := filepath.Join(root, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o755)
		}
		data, err := assist.SkillsFS.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dst, data, 0o644)
	})
}

func skillStamp(home string) string {
	root := filepath.Join(home, ".agents", "skills")
	var parts []string
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		sum, err := assist.MD5OfEmbeddedSkill(filepath.ToSlash(rel))
		if err != nil {
			return nil
		}
		parts = append(parts, rel+"="+sum[:8])
		return nil
	})
	return strings.Join(parts, ",")
}

// seedDuckDBEngine copies the host DuckDB CLI into the trial eval-home so
// trials never download mid-run and a reaped download cannot leave a
// truncated Mach-O.
func seedDuckDBEngine(home string) error {
	version := iop.DuckDbVersion
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}
	destDir := filepath.Join(home, ".sling", "bin", "duckdb", version)
	dest := filepath.Join(destDir, "duckdb"+ext)
	if duckDBBinOK(dest) {
		return nil
	}
	src := hostDuckDBBin(version, ext)
	if src == "" {
		return nil
	}
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	if err := copyFile(src, dest); err != nil {
		return err
	}
	return os.Chmod(dest, 0o755)
}

func hostDuckDBBin(version, ext string) string {
	cands := []string{
		filepath.Join(hostHome(), ".sling", "bin", "duckdb", version, "duckdb"+ext),
	}
	if h := os.Getenv("HOME"); h != "" && h != hostHome() {
		cands = append(cands, filepath.Join(h, ".sling", "bin", "duckdb", version, "duckdb"+ext))
	}
	for _, p := range cands {
		if duckDBBinOK(p) {
			return p
		}
	}
	return ""
}

func duckDBBinOK(p string) bool {
	if !fileNonEmpty(p) {
		return false
	}
	out, err := exec.Command(p, "-version").CombinedOutput()
	if err != nil {
		return false
	}
	return strings.HasPrefix(strings.TrimSpace(string(out)), "v")
}

const persistWorkMaxBytes = 256 * 1024

var persistWorkExt = map[string]bool{
	".yaml": true, ".yml": true, ".json": true, ".sql": true,
	".md": true, ".txt": true, ".csv": true,
}

func persistTrialWork(dest, work string) {
	_ = filepath.Walk(work, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.Size() > persistWorkMaxBytes {
			return nil
		}
		if !persistWorkExt[strings.ToLower(filepath.Ext(p))] {
			return nil
		}
		rel, err := filepath.Rel(work, p)
		if err != nil {
			return nil
		}
		if strings.HasPrefix(rel, ".agents") || strings.HasPrefix(rel, ".claude") ||
			strings.HasPrefix(rel, ".opencode") || strings.HasPrefix(rel, ".grok") {
			return nil
		}
		// .mcp.json carries SLING_CLI_TOKEN; never persist it.
		if rel == ".mcp.json" {
			return nil
		}
		_ = copyFile(p, filepath.Join(dest, rel))
		return nil
	})
}

func slingBinaryStamp(bin string) string {
	if bin == "" {
		return ""
	}
	st, err := os.Stat(bin)
	if err != nil {
		return bin
	}
	ver := slingBinVersion(bin)
	git := slingGitDescribe()
	parts := []string{bin, fmt.Sprintf("mtime=%s", st.ModTime().UTC().Format(time.RFC3339)), fmt.Sprintf("size=%d", st.Size())}
	if ver != "" {
		parts = append(parts, "version="+ver)
	}
	if git != "" {
		parts = append(parts, "git="+git)
	}
	return strings.Join(parts, " ")
}

func slingBinVersion(bin string) string {
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func slingGitDescribe() string {
	root := filepath.Join(evalsDir(), "../..")
	out, err := exec.Command("git", "-C", root, "describe", "--always", "--dirty", "--abbrev=12").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// EnsureFreshSlingBin rebuilds cmd/sling when the binary is older than any
// core/ source file. A live run against a stale binary must not happen silently.
func EnsureFreshSlingBin(bin string) (string, error) {
	if os.Getenv("EVAL_SKIP_REBUILD") == "1" {
		return bin, nil
	}
	root := filepath.Join(evalsDir(), "../..")
	newest, err := newestSourceMTime(filepath.Join(root, "core"))
	if err != nil {
		return bin, err
	}
	st, err := os.Stat(bin)
	needRebuild := err != nil || st.ModTime().Before(newest)
	if !needRebuild {
		return bin, nil
	}
	cmdDir := filepath.Join(root, "cmd", "sling")
	cmd := exec.Command("go", "build", ".")
	cmd.Dir = cmdDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return bin, fmt.Errorf("rebuild sling (binary older than core/): %w\n%s", err, out)
	}
	fresh, err := FindSlingBin()
	if err != nil {
		return bin, err
	}
	return fresh, nil
}

func newestSourceMTime(dir string) (time.Time, error) {
	var newest time.Time
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		switch strings.ToLower(filepath.Ext(p)) {
		case ".go", ".yaml", ".yml", ".md", ".sql":
		default:
			return nil
		}
		if info.ModTime().After(newest) {
			newest = info.ModTime()
		}
		return nil
	})
	return newest, err
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		return copyFile(p, target)
	})
}

func copyFile(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func contains(ss []string, n string) bool {
	for _, s := range ss {
		if s == n {
			return true
		}
	}
	return false
}

func castToString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

func hookAllowFail(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "1" || s == "yes"
	default:
		return false
	}
}

func yamlUnmarshal(b []byte, v any) error { return yaml.Unmarshal(b, v) }
func yamlMarshal(v any) ([]byte, error)   { return yaml.Marshal(v) }

// RunTimed starts name+args in a new process group and kills the whole
// group when timeout elapses. CommandContext only signals the parent.
// Stdout and stderr are merged (legacy graders parse the combined log).
func RunTimed(name string, args []string, dir string, envv []string, timeout time.Duration) (stdout string, err error) {
	return runTimed(name, args, dir, envv, timeout, true)
}

// RunTimedStdout is RunTimed but keeps stderr off the returned transcript.
func RunTimedStdout(name string, args []string, dir string, envv []string, timeout time.Duration) (stdout string, err error) {
	return runTimed(name, args, dir, envv, timeout, false)
}

func runTimed(name string, args []string, dir string, envv []string, timeout time.Duration, mergeErr bool) (stdout string, err error) {
	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = envv
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	if mergeErr {
		cmd.Stderr = &outBuf
	} else {
		cmd.Stderr = &errBuf
	}
	setProcGroup(cmd)
	if err := cmd.Start(); err != nil {
		return "", err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return outBuf.String(), err
	case <-timer.C:
		if cmd.Process != nil {
			killProcGroup(cmd.Process.Pid)
		}
		<-done
		return outBuf.String(), fmt.Errorf("timeout after %s", timeout)
	}
}
