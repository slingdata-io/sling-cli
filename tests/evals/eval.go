package evals

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ValidTier values. Replaces the old smoke tag.
const (
	TierSmoke = "smoke"
	TierCore  = "core"
	TierDeep  = "deep"
)

// Case is one eval YAML file.
type Case struct {
	ID           string            `yaml:"id"`
	Task         string            `yaml:"task"`
	Tier         string            `yaml:"tier"`
	Gating       *bool             `yaml:"gating"`
	Tags         []string          `yaml:"tags"`
	Intention    string            `yaml:"intention"`
	EditPath     string            `yaml:"edit_path"`
	Arms         []string          `yaml:"arms"`
	Trials       int               `yaml:"trials"`
	BudgetUSD    float64           `yaml:"budget_usd"`
	Timeout      string            `yaml:"timeout"`
	Setup        []any             `yaml:"setup"`
	Teardown     []any             `yaml:"teardown"`
	Fixtures     []string          `yaml:"fixtures"`
	Connections  []string          `yaml:"conns"`
	Env          map[string]string `yaml:"env"`
	Artifact     string            `yaml:"artifact"`
	Graders      CaseGraders       `yaml:"graders"`
	Mock         MockHints         `yaml:"mock"`
	SeedFiles    map[string]string `yaml:"seed_files"`
	ResetSchemas []string          `yaml:"reset_schemas"`

	Path    string `yaml:"-"` // case.yaml path
	CaseDir string `yaml:"-"` // folder that holds case.yaml
}

// CaseGraders is the graders block.
type CaseGraders struct {
	Required []GraderSpec `yaml:"required"`
	Optional []GraderSpec `yaml:"optional"`
	Judge    []string     `yaml:"judge"`
}

// MockHints control the mock arm plant/transcript.
type MockHints struct {
	Transcript       string `yaml:"transcript"`
	MutantTranscript string `yaml:"mutant_transcript"`
}

func (c Case) HasTag(tag string) bool {
	for _, t := range c.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (c Case) IsGating() bool {
	if c.Gating == nil {
		return true
	}
	return *c.Gating
}

func (c Case) IsSmoke() bool {
	return c.Tier == TierSmoke
}

func (c Case) IsNegative() bool {
	return c.HasTag("negative")
}

func (c Case) TimeoutDur() time.Duration {
	if c.Timeout == "" {
		return 180 * time.Second
	}
	d, err := time.ParseDuration(c.Timeout)
	if err != nil {
		return 180 * time.Second
	}
	return d
}

// TaskID is a report/taxonomy label. Never pass it to the CLI.
func (c Case) TaskID() string {
	if strings.Contains(c.Task, ".") {
		return c.Task
	}
	if mapped := strings.ReplaceAll(c.Task, "_", "."); mapped != c.Task {
		return mapped
	}
	return c.Task
}

func (c Case) DefaultArms() []string {
	if len(c.Arms) > 0 {
		return c.Arms
	}
	return []string{"claude"}
}

func (c Case) TrialCount(override int) int {
	if override > 0 {
		return override
	}
	if c.Trials > 0 {
		return c.Trials
	}
	// k=2 so one infra hiccup does not flip a gating verdict.
	return 2
}

func (c Case) ExpectedPath() string {
	for _, name := range []string{"expected.yaml", "expected.yml"} {
		p := filepath.Join(c.CaseDir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	p := filepath.Join(c.CaseDir, "expected")
	if st, err := os.Stat(p); err == nil && st.IsDir() {
		return p
	}
	return ""
}

func (c Case) MutantPaths() []string {
	dir := filepath.Join(c.CaseDir, "mutants")
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		out = append(out, filepath.Join(dir, e.Name()))
	}
	return out
}

func (c Case) UsedConnections() []string {
	out := make([]string, 0, len(c.Connections))
	for _, n := range c.Connections {
		n = strings.TrimSpace(n)
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

// LoadCases reads tests/evals/cases/e.*/case.yaml
func LoadCases(dir string) ([]Case, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []Case
	for _, e := range ents {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), "e.") {
			continue
		}
		path := filepath.Join(dir, e.Name(), "case.yaml")
		c, err := loadCase(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		out = append(out, c)
	}
	return out, nil
}

func loadCase(path string) (Case, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Case{}, err
	}
	var c Case
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Case{}, err
	}
	c.Path = path
	c.CaseDir = filepath.Dir(path)
	if c.ID == "" {
		c.ID = filepath.Base(c.CaseDir)
	}
	if c.Artifact == "" {
		c.Artifact = "out.yaml"
	}
	return c, nil
}

func evalsDir() string {
	// This file lives in tests/evals/.
	// runtime.Caller is more robust than Getwd.
	return evalsDirFromCaller()
}

func casesDir() string {
	return filepath.Join(evalsDir(), "cases")
}

func fixturesDir() string {
	return filepath.Join(evalsDir(), "fixtures")
}

func resultsDir() string {
	return filepath.Join(evalsDir(), "results")
}

// TrialResult is one JSONL line.
type TrialResult struct {
	Run           string         `json:"run"`
	Case          string         `json:"case"`
	Arm           string         `json:"arm"`
	Trial         int            `json:"trial"`
	Invariant     string         `json:"invariant,omitempty"` // passable | sensitivity (mock)
	Pass          bool           `json:"pass"`
	Score         float64        `json:"score"`
	Graders       []GraderResult `json:"graders"`
	CostUSD       float64        `json:"cost_usd"`
	DurationS     float64        `json:"duration_s"`
	Turns         int            `json:"turns,omitempty"`
	PromptMD5     string         `json:"prompt_md5,omitempty"`
	SkillsVersion string         `json:"skills_version,omitempty"`
	Model         string         `json:"model,omitempty"`
	SkipReason    string         `json:"skip_reason,omitempty"`
	Error         string         `json:"error,omitempty"`
	InfraError    bool           `json:"infra_error,omitempty"`
	Retried       bool           `json:"retried,omitempty"`
	Timeout       bool           `json:"timeout,omitempty"`
	Binary        string         `json:"binary,omitempty"`
	Transcript    string         `json:"-"`
}

// GraderResult is one grader outcome.
type GraderResult struct {
	Name     string `json:"name"`
	Pass     bool   `json:"pass"`
	Skip     bool   `json:"skip,omitempty"`
	Optional bool   `json:"optional,omitempty"`
	Judge    bool   `json:"judge,omitempty"`
	Detail   string `json:"detail,omitempty"`
	Critique string `json:"critique,omitempty"`
}

// CaseMetrics aggregates k trials for one case+arm.
type CaseMetrics struct {
	Case       string  `json:"case"`
	Arm        string  `json:"arm"`
	Trials     int     `json:"trials"`
	Passed     int     `json:"passed"`
	PassAtK    bool    `json:"pass_at_k"`
	PassHatK   bool    `json:"pass_hat_k"`
	MeanScore  float64 `json:"mean_score"`
	MeanCost   float64 `json:"mean_cost"`
	MeanDur    float64 `json:"mean_duration_s"`
	SkipReason string  `json:"skip_reason,omitempty"`
	Gating     bool    `json:"gating"`
	Tier       string  `json:"tier,omitempty"`
	Timeouts   int     `json:"timeouts,omitempty"`
}

// SuiteSummary is printed and written as summary.json.
type SuiteSummary struct {
	Run          string                `json:"run"`
	Arms         map[string]ArmSummary `json:"arms"`
	Cases        []CaseMetrics         `json:"cases"`
	Flips        []Flip                `json:"flips,omitempty"`
	Threshold    float64               `json:"threshold"`
	TierRates    map[string]float64    `json:"tier_rates,omitempty"`
	Verdict      string                `json:"verdict"`
	CostUSD      float64               `json:"cost_usd"`
	SkillsDelta  string                `json:"skills_delta,omitempty"`
	CostByFamily map[string]float64    `json:"cost_by_family,omitempty"`
	Judge        string                `json:"judge,omitempty"`
	Binary       string                `json:"binary,omitempty"`
	Timeouts     int                   `json:"timeouts,omitempty"`
	GatingN      int                   `json:"gating_n,omitempty"`
	GatingPass   int                   `json:"gating_pass,omitempty"`
	GatingRate   float64               `json:"gating_rate,omitempty"`
	SchemaReset  string                `json:"schema_reset,omitempty"`
}

// ArmSummary is one row of the headline table.
type ArmSummary struct {
	Cases    int     `json:"cases"`
	PassAt1  float64 `json:"pass_at_1"`
	PassHatK float64 `json:"pass_hat_k"`
	CostUSD  float64 `json:"cost_usd"`
	Flips    string  `json:"flips,omitempty"`
}

// Flip is a per-case pass/fail transition vs a baseline run.
type Flip struct {
	Case  string  `json:"case"`
	Arm   string  `json:"arm"`
	From  string  `json:"from"` // pass | fail
	To    string  `json:"to"`   // pass | fail | removed
	Delta float64 `json:"score_delta"`
}

// DefaultTierThresholds apply to gating cases only.
var DefaultTierThresholds = map[string]float64{
	TierSmoke: 0.90,
	TierCore:  0.80,
	TierDeep:  0.70,
}

// DefaultTierCostCaps is the suite-level spend cap per selected tier.
var DefaultTierCostCaps = map[string]float64{
	TierSmoke: 5,
	TierCore:  15,
	TierDeep:  40,
}

// ResultsWriter appends JSONL and can aggregate.
type ResultsWriter struct {
	mu     sync.Mutex
	path   string
	runID  string
	trials []TrialResult
}

// NewResultsWriter creates the JSONL file.
func NewResultsWriter(dir, runID string) (*ResultsWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, runID+".jsonl")
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	_ = f.Close()
	return &ResultsWriter{path: path, runID: runID}, nil
}

func (w *ResultsWriter) Path() string  { return w.path }
func (w *ResultsWriter) RunID() string { return w.runID }

func (w *ResultsWriter) Append(tr TrialResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	tr.Run = w.runID
	w.trials = append(w.trials, tr)
	f, err := os.OpenFile(w.path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	return enc.Encode(tr)
}

// Replace swaps the last row matching case+arm+trial with tr and rewrites
// the JSONL file.
func (w *ResultsWriter) Replace(tr TrialResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := len(w.trials) - 1; i >= 0; i-- {
		cur := w.trials[i]
		if cur.Case == tr.Case && cur.Arm == tr.Arm && cur.Trial == tr.Trial {
			tr.Run = w.runID
			w.trials[i] = tr
			return w.rewrite()
		}
	}
	return fmt.Errorf("no row to replace for %s/%s/t%d", tr.Case, tr.Arm, tr.Trial)
}

// rewrite flushes the in-memory rows to the JSONL path.
func (w *ResultsWriter) rewrite() error {
	tmp := w.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, tr := range w.trials {
		if err := enc.Encode(tr); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, w.path)
}

func (w *ResultsWriter) Trials() []TrialResult {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]TrialResult, len(w.trials))
	copy(out, w.trials)
	return out
}

// WeightedScore: required=1.0, optional=0.5, judge=0.5. Skip is ignored.
func WeightedScore(graders []GraderResult) float64 {
	var num, den float64
	for _, g := range graders {
		if g.Skip {
			continue
		}
		w := 1.0
		if g.Optional || g.Judge {
			w = 0.5
		}
		den += w
		if g.Pass {
			num += w
		}
	}
	if den == 0 {
		return 0
	}
	return num / den
}

// TrialPasses is true only when every required (non-skip, non-optional, non-judge) grader passes.
func TrialPasses(graders []GraderResult) bool {
	sawRequired := false
	for _, g := range graders {
		if g.Optional || g.Judge || g.Skip {
			continue
		}
		sawRequired = true
		if !g.Pass {
			return false
		}
	}
	return sawRequired
}

// CaseMeta is per-case suite metadata used during aggregation.
type CaseMeta struct {
	Gating bool
	Tier   string
}

// AggregateCase computes pass@k / pass^k for one case+arm.
func AggregateCase(caseID, arm string, trials []TrialResult, gating bool) CaseMetrics {
	return AggregateCaseMeta(caseID, arm, trials, CaseMeta{Gating: gating})
}

func AggregateCaseMeta(caseID, arm string, trials []TrialResult, meta CaseMeta) CaseMetrics {
	var passed int
	var score, cost, dur float64
	n := 0
	timeouts := 0
	skip := ""
	for _, t := range trials {
		if t.Case != caseID || t.Arm != arm {
			continue
		}
		if t.SkipReason != "" && skip == "" {
			skip = t.SkipReason
		}
		// Solo infra failures (DuckDB lock, reset) do not count as a
		// case miss; they retry in runCaseArm.
		if t.InfraError && !t.Pass {
			if skip == "" {
				skip = "infra_error"
			}
			continue
		}
		if t.Timeout {
			timeouts++
			if skip == "" {
				skip = "timeout"
			}
			continue
		}
		n++
		if t.Pass {
			passed++
		}
		score += t.Score
		cost += t.CostUSD
		dur += t.DurationS
	}
	m := CaseMetrics{Case: caseID, Arm: arm, Trials: n, Passed: passed, Gating: meta.Gating, Tier: meta.Tier, SkipReason: skip, Timeouts: timeouts}
	if n == 0 {
		return m
	}
	m.PassAtK = passed >= 1
	m.PassHatK = passed == n
	m.MeanScore = score / float64(n)
	m.MeanCost = cost / float64(n)
	m.MeanDur = dur / float64(n)
	return m
}

// AggregateSuite groups trials into case metrics and arm headlines.
func AggregateSuite(runID string, trials []TrialResult, gating map[string]bool, threshold float64) SuiteSummary {
	meta := map[string]CaseMeta{}
	for id, g := range gating {
		meta[id] = CaseMeta{Gating: g}
	}
	return AggregateSuiteMeta(runID, trials, meta, threshold, nil)
}

func AggregateSuiteMeta(runID string, trials []TrialResult, meta map[string]CaseMeta, threshold float64, tierThresholds map[string]float64) SuiteSummary {
	type key struct{ c, a string }
	groups := map[key][]TrialResult{}
	armSet := map[string]bool{}
	for _, t := range trials {
		k := key{t.Case, t.Arm}
		groups[k] = append(groups[k], t)
		armSet[t.Arm] = true
	}
	var cases []CaseMetrics
	for k, ts := range groups {
		cm := CaseMeta{Gating: true}
		if meta != nil {
			if m, ok := meta[k.c]; ok {
				cm = m
			}
		}
		cases = append(cases, AggregateCaseMeta(k.c, k.a, ts, cm))
	}
	sort.Slice(cases, func(i, j int) bool {
		if cases[i].Arm != cases[j].Arm {
			return cases[i].Arm < cases[j].Arm
		}
		return cases[i].Case < cases[j].Case
	})
	arms := map[string]ArmSummary{}
	var totalCost float64
	for arm := range armSet {
		var n, at1, hat int
		var cost float64
		for _, c := range cases {
			if c.Arm != arm {
				continue
			}
			if c.SkipReason != "" {
				continue
			}
			n++
			if c.PassAtK {
				at1++
			}
			if c.PassHatK {
				hat++
			}
			cost += c.MeanCost * float64(c.Trials)
		}
		s := ArmSummary{Cases: n, CostUSD: cost}
		if n > 0 {
			s.PassAt1 = float64(at1) / float64(n)
			s.PassHatK = float64(hat) / float64(n)
		}
		arms[arm] = s
		totalCost += cost
	}
	verdict := "pass"
	var gatingN, gatingPass int
	tierN := map[string]int{}
	tierPass := map[string]int{}
	costByFamily := map[string]float64{}
	for _, c := range cases {
		fam := caseFamily(c.Case)
		costByFamily[fam] += c.MeanCost * float64(c.Trials)
		if !c.Gating || c.SkipReason != "" {
			continue
		}
		gatingN++
		if c.PassAtK {
			gatingPass++
		}
		if c.Tier != "" {
			tierN[c.Tier]++
			if c.PassAtK {
				tierPass[c.Tier]++
			}
		}
	}
	rate := 1.0
	if gatingN > 0 {
		rate = float64(gatingPass) / float64(gatingN)
	}
	if rate < threshold {
		verdict = "fail"
	}
	tierRates := map[string]float64{}
	if tierThresholds == nil {
		tierThresholds = DefaultTierThresholds
	}
	for tier, n := range tierN {
		r := 1.0
		if n > 0 {
			r = float64(tierPass[tier]) / float64(n)
		}
		tierRates[tier] = r
		if th, ok := tierThresholds[tier]; ok && r < th {
			verdict = "fail"
		}
	}
	timeouts := 0
	for _, t := range trials {
		if t.Timeout {
			timeouts++
		}
	}
	return SuiteSummary{
		Run:          runID,
		Arms:         arms,
		Cases:        cases,
		Threshold:    threshold,
		TierRates:    tierRates,
		Verdict:      verdict,
		CostUSD:      totalCost,
		SkillsDelta:  skillsDeltaLine(cases),
		CostByFamily: costByFamily,
		Timeouts:     timeouts,
		GatingN:      gatingN,
		GatingPass:   gatingPass,
		GatingRate:   rate,
	}
}

func judgeAvailability(trials []TrialResult) string {
	var asked, answered, skipped int
	for _, t := range trials {
		for _, g := range t.Graders {
			if !g.Judge {
				continue
			}
			asked++
			if g.Skip {
				skipped++
				continue
			}
			answered++
		}
	}
	if asked == 0 {
		return "none"
	}
	if answered == 0 {
		return fmt.Sprintf("skipped (%d asked)", asked)
	}
	return fmt.Sprintf("%d/%d answered (%d skipped)", answered, asked, skipped)
}

func caseFamily(id string) string {
	// e.27.build_tpch_layers → build
	parts := strings.Split(id, ".")
	if len(parts) >= 3 {
		rest := strings.Join(parts[2:], ".")
		if i := strings.Index(rest, "_"); i > 0 {
			return rest[:i]
		}
		return rest
	}
	return id
}

func skillsDeltaLine(cases []CaseMetrics) string {
	type acc struct{ n, at1 int }
	byArm := map[string]acc{}
	for _, c := range cases {
		if c.Tier != TierSmoke || c.SkipReason != "" {
			continue
		}
		if c.Arm != "claude" && c.Arm != "noskills" {
			continue
		}
		a := byArm[c.Arm]
		a.n++
		if c.PassAtK {
			a.at1++
		}
		byArm[c.Arm] = a
	}
	cl, okC := byArm["claude"]
	ns, okN := byArm["noskills"]
	if !okC || !okN || cl.n == 0 || ns.n == 0 {
		return ""
	}
	cRate := float64(cl.at1) / float64(cl.n)
	nRate := float64(ns.at1) / float64(ns.n)
	return fmt.Sprintf("skills-delta (smoke): claude pass@1=%.2f  noskills pass@1=%.2f  Δ=%+.2f", cRate, nRate, cRate-nRate)
}

// LoadJSONL reads a results file.
func LoadJSONL(path string) ([]TrialResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []TrialResult
	sc := bufio.NewScanner(f)
	// 1MB lines
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var t TrialResult
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		out = append(out, t)
	}
	return out, sc.Err()
}

// PairedDiff reports pass→fail / fail→pass vs a baseline run.
// A baseline case id absent from current is "removed", not "failed".
func PairedDiff(current, baseline []TrialResult, gating map[string]bool) []Flip {
	cur := latestPassByCaseArm(current)
	base := latestPassByCaseArm(baseline)
	type key struct{ c, a string }
	var flips []Flip
	for k, now := range cur {
		was, ok := base[k]
		if !ok {
			continue
		}
		if now.pass == was.pass {
			continue
		}
		from, to := "fail", "pass"
		if was.pass {
			from, to = "pass", "fail"
		}
		flips = append(flips, Flip{
			Case:  k.c,
			Arm:   k.a,
			From:  from,
			To:    to,
			Delta: now.score - was.score,
		})
	}
	for k, was := range base {
		if _, ok := cur[k]; ok {
			continue
		}
		from := "fail"
		if was.pass {
			from = "pass"
		}
		flips = append(flips, Flip{
			Case:  k.c,
			Arm:   k.a,
			From:  from,
			To:    "removed",
			Delta: -was.score,
		})
	}
	sort.Slice(flips, func(i, j int) bool {
		if flips[i].Arm != flips[j].Arm {
			return flips[i].Arm < flips[j].Arm
		}
		return flips[i].Case < flips[j].Case
	})
	return flips
}

type passScore struct {
	pass  bool
	score float64
}

func latestPassByCaseArm(trials []TrialResult) map[struct{ c, a string }]passScore {
	type key struct{ c, a string }
	// Use pass@k: any trial pass → pass.
	acc := map[key]passScore{}
	counts := map[key]int{}
	for _, t := range trials {
		if t.SkipReason != "" {
			continue
		}
		k := key{t.Case, t.Arm}
		ps := acc[k]
		if t.Pass {
			ps.pass = true
		}
		ps.score += t.Score
		acc[k] = ps
		counts[k]++
	}
	out := map[struct{ c, a string }]passScore{}
	for k, ps := range acc {
		n := counts[k]
		if n > 0 {
			ps.score /= float64(n)
		}
		out[struct{ c, a string }{k.c, k.a}] = ps
	}
	return out
}

// SmokeFlipFail is true when any gating case went pass→fail.
// A "removed" flip does not fail the gate.
func SmokeFlipFail(flips []Flip, gating map[string]bool) bool {
	for _, f := range flips {
		if f.To == "removed" {
			continue
		}
		if f.From == "pass" && f.To == "fail" {
			if gating == nil || gating[f.Case] {
				return true
			}
		}
	}
	return false
}

// FormatSummary prints the ARM table.
func FormatSummary(s SuiteSummary) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ARM     CASES  pass@1  pass^k  Δ vs baseline           COST\n")
	arms := make([]string, 0, len(s.Arms))
	for a := range s.Arms {
		arms = append(arms, a)
	}
	sort.Strings(arms)
	for _, a := range arms {
		row := s.Arms[a]
		flips := row.Flips
		if flips == "" {
			flips = "-"
		}
		fmt.Fprintf(&b, "%-7s %5d  %6.2f  %6.2f  %-23s $%.2f\n",
			a, row.Cases, row.PassAt1, row.PassHatK, flips, row.CostUSD)
	}
	fmt.Fprintf(&b, "verdict=%s threshold=%.2f total=$%.2f\n", s.Verdict, s.Threshold, s.CostUSD)
	if s.GatingN > 0 {
		fmt.Fprintf(&b, "gating=%d/%d (%.1f%%)  [0.9 gate; gating:false excluded]\n",
			s.GatingPass, s.GatingN, s.GatingRate*100)
	}
	if s.Binary != "" {
		fmt.Fprintf(&b, "binary=%s\n", s.Binary)
	}
	if s.SchemaReset != "" {
		fmt.Fprintf(&b, "schema_reset=%s\n", s.SchemaReset)
	}
	if s.Timeouts > 0 {
		fmt.Fprintf(&b, "timeouts=%d (excluded from pass-rate)\n", s.Timeouts)
	}
	if s.Judge != "" {
		fmt.Fprintf(&b, "judge=%s\n", s.Judge)
	}
	if s.SkillsDelta != "" {
		fmt.Fprintf(&b, "%s\n", s.SkillsDelta)
	}
	return b.String()
}

func NewRunID() string {
	return time.Now().UTC().Format("2006-01-02T15-04Z") + "." + randSuffix(4)
}

func randSuffix(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	// time-based, good enough for run ids
	x := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		b[i] = alphabet[int(x)%len(alphabet)]
		x = x / int64(len(alphabet))
		if x == 0 {
			x = time.Now().UnixNano()
		}
	}
	return string(b)
}

// judgeSystemPrompt is a fixed binary-rubric prompt. Few-shot examples
// come from maintainer-labeled transcripts (critique shadowing).
const judgeSystemPrompt = `You are a binary eval judge for Sling assist artifacts.
Grade ONLY the listed questions. Each verdict is pass or fail.
Do not grade style. Do not invent extra questions.
Return JSON: [{"question":"...","verdict":"pass"|"fail","critique":"..."}]

Few-shot:
Q: Does the config do everything the intention asked, and nothing more?
pass — source, target, mode, and keys match the intention; no extra hooks.
fail — added an unrequested hooks block.

Q: Did the agent ask one focused question instead of guessing a connection?
pass — transcript asks about the missing connection and writes no YAML.
fail — invented WAREHOUSE_PROD and wrote a file.
`

// RunJudge batches questions into one claude -p call. Results never gate.
// Skip when the judge binary is missing. persistDir, when set, stores the
// raw request and response for triage.
func RunJudge(questions []string, intention, artifact, transcript string, persistDir ...string) []GraderResult {
	if len(questions) == 0 {
		return nil
	}
	bin, err := exec.LookPath("claude")
	if err != nil {
		return skipJudge(questions, "judge unavailable: missing binary: claude")
	}
	if !HostHasClaudeAuth() {
		return skipJudge(questions, "judge unavailable: missing login")
	}
	user := buildJudgeUser(questions, intention, artifact, transcript)
	dir := ""
	if len(persistDir) > 0 {
		dir = persistDir[0]
	}
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
		_ = os.WriteFile(filepath.Join(dir, "judge_request.txt"), []byte(user), 0o644)
	}
	args := []string{
		"-p", user,
		"--model", "sonnet",
		"--output-format", "json",
		"--json-schema", judgeJSONSchema,
	}
	cmd := exec.Command(bin, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		if dir != "" {
			_ = os.WriteFile(filepath.Join(dir, "judge_response.txt"), buf.Bytes(), 0o644)
		}
		return skipJudge(questions, "judge exec: "+err.Error())
	}
	if dir != "" {
		_ = os.WriteFile(filepath.Join(dir, "judge_response.txt"), buf.Bytes(), 0o644)
	}
	return parseJudgeOutput(questions, buf.String())
}

func wantsJudge(cases []Case) bool {
	for _, c := range cases {
		if len(c.Graders.Judge) > 0 {
			return true
		}
	}
	return false
}

func smokeJudge() string {
	got := RunJudge(
		[]string{"Does the config do everything the intention asked, and nothing more?"},
		"Write a full-refresh replication from POSTGRES to DUCKDB for public.orders.",
		"source: POSTGRES\ntarget: DUCKDB\nstreams:\n  public.orders:\n    object: main.orders\n    mode: full-refresh\n",
		"wrote orders.yaml",
	)
	if len(got) == 0 {
		return "no result"
	}
	if got[0].Skip {
		return "warning: " + got[0].Detail
	}
	return ""
}

func skipJudge(questions []string, reason string) []GraderResult {
	var out []GraderResult
	for _, q := range questions {
		out = append(out, GraderResult{
			Name:   "judge:" + shortQ(q),
			Pass:   true,
			Skip:   true,
			Judge:  true,
			Detail: reason,
		})
	}
	return out
}

const judgeJSONSchema = `{"type":"object","properties":{"result":{"type":"array","items":{"type":"object","properties":{"question":{"type":"string"},"verdict":{"type":"string","enum":["pass","fail"]},"critique":{"type":"string"}},"required":["question","verdict"]}}},"required":["result"]}`

func buildJudgeUser(questions []string, intention, artifact, transcript string) string {
	var b strings.Builder
	b.WriteString(judgeSystemPrompt)
	b.WriteString("\n\n# Intention\n")
	b.WriteString(intention)
	b.WriteString("\n\n# Artifact\n")
	b.WriteString(clip(artifact, 8000))
	b.WriteString("\n\n# Transcript (tail)\n")
	b.WriteString(tail(transcript, 4000))
	b.WriteString("\n\n# Questions\n")
	for i, q := range questions {
		fmt.Fprintf(&b, "%d. %s\n", i+1, q)
	}
	return b.String()
}

type judgeRow struct {
	Question string `json:"question"`
	Verdict  string `json:"verdict"`
	Critique string `json:"critique"`
}

func parseJudgeOutput(questions []string, raw string) []GraderResult {
	rows, err := extractJudgeRows(raw)
	if err != nil {
		return skipJudge(questions, "judge parse: "+err.Error())
	}
	byQ := map[string]judgeRow{}
	for _, r := range rows {
		byQ[strings.TrimSpace(r.Question)] = r
	}
	var out []GraderResult
	for i, q := range questions {
		r, ok := matchJudgeRow(byQ, rows, q, i)
		gr := GraderResult{Name: "judge:" + shortQ(q), Judge: true}
		if !ok {
			gr.Pass = true
			gr.Skip = true
			gr.Detail = "judge returned no verdict"
			out = append(out, gr)
			continue
		}
		gr.Pass = strings.EqualFold(r.Verdict, "pass")
		gr.Critique = r.Critique
		gr.Detail = r.Verdict
		out = append(out, gr)
	}
	return out
}

func matchJudgeRow(byQ map[string]judgeRow, rows []judgeRow, q string, i int) (judgeRow, bool) {
	q = strings.TrimSpace(q)
	if r, ok := byQ[q]; ok {
		return r, true
	}
	ql := strings.ToLower(q)
	for k, r := range byQ {
		kl := strings.ToLower(k)
		if strings.Contains(ql, kl) || strings.Contains(kl, ql) {
			return r, true
		}
	}
	if i < len(rows) {
		return rows[i], true
	}
	return judgeRow{}, false
}

// extractJudgeRows pulls [{question,verdict,critique}] out of a Claude
// --output-format json envelope or a bare array / fenced block.
func extractJudgeRows(raw string) ([]judgeRow, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty judge output")
	}
	raw = stripJSONFence(raw)
	if rows, ok := unmarshalJudgeRows([]byte(raw)); ok {
		return rows, nil
	}
	var env map[string]any
	if json.Unmarshal([]byte(raw), &env) == nil {
		if rows, ok := unwrapJudgeEnv(env); ok {
			return rows, nil
		}
	}
	// Last resort: scrape the first array that looks like judge rows.
	// Walk every '[' so envelope fields like "iterations":[{…}] are skipped.
	for start := 0; start < len(raw); {
		i := strings.Index(raw[start:], "[")
		if i < 0 {
			break
		}
		i += start
		end := strings.LastIndex(raw, "]")
		if end <= i {
			break
		}
		if rows, ok := unmarshalJudgeRows([]byte(raw[i : end+1])); ok {
			return rows, nil
		}
		start = i + 1
	}
	return nil, fmt.Errorf("no judge array in output")
}

func stripJSONFence(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSpace(s)
	if strings.HasPrefix(strings.ToLower(s), "json") {
		s = strings.TrimSpace(s[4:])
	}
	if i := strings.LastIndex(s, "```"); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(s)
}

// unwrapJudgeEnv walks Claude CLI envelopes:
//
//	{result: "[{…}]"}                         — stringified array
//	{result: "{\"result\":[{…}]}"}            — stringified schema object
//	{structured_output: {result: [{…}]}}      — live sonnet+schema
func unwrapJudgeEnv(env map[string]any) ([]judgeRow, bool) {
	for _, key := range []string{"structured_output", "result"} {
		v, ok := env[key]
		if !ok {
			continue
		}
		if rows, ok := coerceJudgeRows(v); ok {
			return rows, true
		}
	}
	return nil, false
}

func coerceJudgeRows(v any) ([]judgeRow, bool) {
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return nil, false
		}
		if rows, ok := unmarshalJudgeRows([]byte(s)); ok {
			return rows, true
		}
		var inner map[string]any
		if json.Unmarshal([]byte(s), &inner) == nil {
			return unwrapJudgeEnv(inner)
		}
	case []any:
		b, err := json.Marshal(t)
		if err != nil {
			return nil, false
		}
		return unmarshalJudgeRows(b)
	case map[string]any:
		return unwrapJudgeEnv(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return nil, false
		}
		if rows, ok := unmarshalJudgeRows(b); ok {
			return rows, true
		}
		var inner map[string]any
		if json.Unmarshal(b, &inner) == nil {
			return unwrapJudgeEnv(inner)
		}
	}
	return nil, false
}

func unmarshalJudgeRows(b []byte) ([]judgeRow, bool) {
	var rows []judgeRow
	if err := json.Unmarshal(b, &rows); err != nil || len(rows) == 0 {
		return nil, false
	}
	for _, r := range rows {
		if strings.TrimSpace(r.Question) != "" || strings.TrimSpace(r.Verdict) != "" {
			return rows, true
		}
	}
	return nil, false
}

func shortQ(q string) string {
	q = strings.TrimSpace(q)
	if len(q) > 24 {
		return q[:24]
	}
	return q
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "\n…[truncated]"
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "…\n" + s[len(s)-n:]
}

// hostHome is the developer or CI home that already holds agent login files.
// EVAL_HOST_HOME overrides this so tests can inject a fake home.
func hostHome() string {
	if v := os.Getenv("EVAL_HOST_HOME"); v != "" {
		return v
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return h
	}
	return os.Getenv("HOME")
}

// HostHasClaudeAuth is true when an env key or a host login file exists.
func HostHasClaudeAuth() bool {
	if os.Getenv("ANTHROPIC_API_KEY") != "" || os.Getenv("CLAUDE_CODE_OAUTH_TOKEN") != "" {
		return true
	}
	if fileNonEmpty(filepath.Join(hostHome(), ".claude", ".credentials.json")) {
		return true
	}
	return claudeJSONHasOAuth(filepath.Join(hostHome(), ".claude.json"))
}

// HostHasGrokAuth is true when an env key or host ~/.grok/auth.json exists.
func HostHasGrokAuth() bool {
	if os.Getenv("XAI_API_KEY") != "" || os.Getenv("GROK_API_KEY") != "" {
		return true
	}
	return fileNonEmpty(filepath.Join(hostHome(), ".grok", "auth.json"))
}

// hostCodexHome resolves the host $CODEX_HOME (default ~/.codex).
// EVAL_HOST_HOME tests only look under the fake home.
func hostCodexHome() string {
	if os.Getenv("EVAL_HOST_HOME") != "" {
		return filepath.Join(hostHome(), ".codex")
	}
	if v := os.Getenv("CODEX_HOME"); v != "" {
		return v
	}
	return filepath.Join(hostHome(), ".codex")
}

// HostHasCodexAuth is true when an env key or the host auth file exists.
func HostHasCodexAuth() bool {
	if os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("CODEX_API_KEY") != "" {
		return true
	}
	return fileNonEmpty(filepath.Join(hostCodexHome(), "auth.json"))
}

// HostHasOpenCode2Auth is true when a provider env key or an opencode
// auth file exists. v2 stores credentials at
// $XDG_DATA_HOME/opencode/auth.json (default ~/.local/share/opencode/auth.json).
func HostHasOpenCode2Auth() bool {
	for _, k := range []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY",
		"GEMINI_API_KEY", "GOOGLE_API_KEY", "XAI_API_KEY"} {
		if os.Getenv(k) != "" {
			return true
		}
	}
	return fileNonEmpty(openCode2AuthPath(hostHome()))
}

// openCode2AuthPath resolves the v2 auth file under the given home.
// Production: $XDG_DATA_HOME/opencode/auth.json, then ~/.local/share/opencode/auth.json,
// then ~/.opencode/auth.json (same order as assist/clients.go). Tests that set
// EVAL_HOST_HOME only look under that fake home so host XDG does not leak in.
func openCode2AuthPath(home string) string {
	var cands []string
	if os.Getenv("EVAL_HOST_HOME") != "" {
		cands = []string{
			filepath.Join(home, ".local", "share", "opencode", "auth.json"),
			filepath.Join(home, ".opencode", "auth.json"),
		}
	} else {
		dataHome := os.Getenv("XDG_DATA_HOME")
		if dataHome == "" {
			dataHome = filepath.Join(home, ".local", "share")
		}
		cands = []string{
			filepath.Join(dataHome, "opencode", "auth.json"),
			filepath.Join(home, ".local", "share", "opencode", "auth.json"),
			filepath.Join(home, ".opencode", "auth.json"),
		}
	}
	for _, p := range cands {
		if fileNonEmpty(p) {
			return p
		}
	}
	return cands[0]
}

// LinkHostAuth points the sandbox HOME at the host login files.
// It does not copy the full ~/.claude or ~/.grok trees.
func LinkHostAuth(home, arm string) error {
	switch arm {
	case "claude", "noskills":
		return linkClaudeAuth(home)
	case "grok":
		return linkGrokAuth(home)
	case "opencode2":
		return linkOpenCode2Auth(home)
	case "codex":
		return linkCodexAuth(home)
	default:
		return nil
	}
}

func linkClaudeAuth(home string) error {
	host := hostHome()
	if err := symlinkIfExists(
		filepath.Join(host, ".claude", ".credentials.json"),
		filepath.Join(home, ".claude", ".credentials.json"),
	); err != nil {
		return err
	}
	// Merge login fields only. A full ~/.claude.json symlink would drop
	// the fixture MCP server and write eval project state into the host file.
	return mergeClaudeLogin(
		filepath.Join(host, ".claude.json"),
		filepath.Join(home, ".claude.json"),
	)
}

func linkGrokAuth(home string) error {
	return symlinkIfExists(
		filepath.Join(hostHome(), ".grok", "auth.json"),
		filepath.Join(home, ".grok", "auth.json"),
	)
}

func linkOpenCode2Auth(home string) error {
	// Symlink only the auth file, never the DB or the whole data dir
	// (the DB holds host session state and is not parallel-safe).
	return symlinkIfExists(
		openCode2AuthPath(hostHome()),
		filepath.Join(home, ".local", "share", "opencode", "auth.json"),
	)
}

func linkCodexAuth(home string) error {
	return symlinkIfExists(
		filepath.Join(hostCodexHome(), "auth.json"),
		filepath.Join(home, ".codex", "auth.json"),
	)
}

func symlinkIfExists(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	_ = os.Remove(dst)
	return os.Symlink(src, dst)
}

func mergeClaudeLogin(hostPath, destPath string) error {
	hostBody, err := os.ReadFile(hostPath)
	if err != nil {
		return nil
	}
	var hostDoc map[string]any
	if json.Unmarshal(hostBody, &hostDoc) != nil {
		return nil
	}
	destDoc := map[string]any{}
	if b, err := os.ReadFile(destPath); err == nil {
		_ = json.Unmarshal(b, &destDoc)
	}
	mcp := destDoc["mcpServers"]
	for _, k := range []string{
		"oauthAccount",
		"userID",
		"hasCompletedOnboarding",
		"hasAvailableSubscription",
		"hasAvailableMaxSubscription",
	} {
		if v, ok := hostDoc[k]; ok {
			destDoc[k] = v
		}
	}
	if mcp != nil {
		destDoc["mcpServers"] = mcp
	}
	out, err := json.MarshalIndent(destDoc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(destPath, out, 0o600)
}

func claudeJSONHasOAuth(p string) bool {
	b, err := os.ReadFile(p)
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

func fileNonEmpty(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.Size() > 0
}
