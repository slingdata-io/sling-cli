package evals

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// GradeContext is the trial sandbox the graders see.
type GradeContext struct {
	WorkDir        string
	Artifact       string // relative to WorkDir
	CaseDir        string // case folder with case.yaml, expected/, mutants/
	Transcript     string
	SlingBin       string
	Env            []string
	ConnDown       map[string]bool // name → down
	SkipExecute    bool            // mock arm: do not run L5 execute graders
	FixtureBaseURL string
	FixtureToken   string
	APIPageCap     int
	APITimeout     time.Duration
}

func (gc GradeContext) artifactPath() string {
	if filepath.IsAbs(gc.Artifact) {
		return gc.Artifact
	}
	return filepath.Join(gc.WorkDir, gc.Artifact)
}

func (gc GradeContext) expand(s string) string {
	s = strings.ReplaceAll(s, "{artifact}", gc.Artifact)
	s = strings.ReplaceAll(s, "{workdir}", gc.WorkDir)
	return s
}

// GraderSpec is one YAML grader item (one primary key).
type GraderSpec map[string]any

func (g GraderSpec) kind() string {
	for _, k := range []string{
		"file_exists", "file_absent", "yaml_valid", "sling", "expected",
		"dry_run", "transcript_contains", "transcript_absent", "transcript_absent_raw", "yq",
		"sql_equiv", "skeleton", "outcome",
		"query", "rows_equal", "dag", "tests_pass", "api_spec",
	} {
		if _, ok := g[k]; ok {
			return k
		}
	}
	for k := range g {
		return k
	}
	return ""
}

// RunGraders executes required then optional specs. Judge is separate.
func RunGraders(ctx GradeContext, required, optional []GraderSpec) []GraderResult {
	var out []GraderResult
	for _, spec := range required {
		r := runOneGrader(ctx, spec)
		out = append(out, r)
	}
	for _, spec := range optional {
		r := runOneGrader(ctx, spec)
		r.Optional = true
		out = append(out, r)
	}
	return out
}

func runOneGrader(ctx GradeContext, spec GraderSpec) GraderResult {
	kind := spec.kind()
	switch kind {
	case "file_exists":
		return gradeFileExists(ctx, cast.ToString(spec["file_exists"]))
	case "file_absent":
		return gradeFileAbsent(ctx, cast.ToString(spec["file_absent"]))
	case "yaml_valid":
		return gradeYAMLValid(ctx, cast.ToString(spec["yaml_valid"]))
	case "sling":
		return gradeSling(ctx, cast.ToString(spec["sling"]))
	case "expected":
		return gradeExpected(ctx, spec["expected"])
	case "dry_run":
		return gradeDryRun(ctx, cast.ToString(spec["dry_run"]))
	case "transcript_contains":
		return gradeTranscriptContains(ctx, cast.ToString(spec["transcript_contains"]))
	case "transcript_absent":
		return gradeTranscriptAbsent(ctx, cast.ToString(spec["transcript_absent"]))
	case "transcript_absent_raw":
		return gradeTranscriptAbsentRaw(ctx, cast.ToString(spec["transcript_absent_raw"]))
	case "yq":
		return gradeYQ(ctx, cast.ToString(spec["yq"]))
	case "sql_equiv":
		return gradeSQLEquiv(ctx, spec["sql_equiv"])
	case "skeleton":
		return gradeSkeleton(ctx, spec["skeleton"])
	case "outcome":
		return gradeOutcome(ctx, spec["outcome"])
	case "query":
		return gradeQuery(ctx, spec["query"])
	case "rows_equal":
		return gradeRowsEqual(ctx, spec["rows_equal"])
	case "dag":
		return gradeDAG(ctx, spec["dag"])
	case "tests_pass":
		return gradeTestsPass(ctx, spec["tests_pass"])
	case "api_spec":
		return gradeAPISpecMap(ctx, spec["api_spec"])
	default:
		return GraderResult{Name: kind, Pass: false, Detail: "unknown grader"}
	}
}

func gradeFileExists(ctx GradeContext, rel string) GraderResult {
	rel = ctx.expand(rel)
	if strings.ContainsAny(rel, "*?[") {
		matches := globWork(ctx.WorkDir, rel)
		if len(matches) == 0 {
			return GraderResult{Name: "file_exists", Pass: false, Detail: "no match for " + rel}
		}
		return GraderResult{Name: "file_exists", Pass: true, Detail: strings.Join(matches, ",")}
	}
	p := rel
	if !filepath.IsAbs(p) {
		p = filepath.Join(ctx.WorkDir, rel)
	}
	info, err := os.Stat(p)
	if err != nil {
		return GraderResult{Name: "file_exists", Pass: false, Detail: err.Error()}
	}
	if info.IsDir() {
		return GraderResult{Name: "file_exists", Pass: true, Detail: "dir " + rel}
	}
	if info.Size() == 0 {
		return GraderResult{Name: "file_exists", Pass: false, Detail: "empty file"}
	}
	return GraderResult{Name: "file_exists", Pass: true, Detail: rel}
}

func globWork(root, pattern string) []string {
	pat := pattern
	if !filepath.IsAbs(pat) {
		pat = filepath.Join(root, pattern)
	}
	if strings.Contains(pattern, "**") {
		suffix := strings.ToLower(filepath.Ext(strings.ReplaceAll(pattern, "**", "")))
		var out []string
		_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			base := info.Name()
			if info.IsDir() {
				if strings.HasPrefix(base, ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if suffix != "" && strings.ToLower(filepath.Ext(p)) != suffix {
				return nil
			}
			out = append(out, p)
			return nil
		})
		return out
	}
	matches, _ := filepath.Glob(pat)
	return matches
}

func gradeFileAbsent(ctx GradeContext, rel string) GraderResult {
	rel = ctx.expand(rel)
	p := rel
	if !filepath.IsAbs(p) {
		p = filepath.Join(ctx.WorkDir, rel)
	}
	_, err := os.Stat(p)
	if err == nil {
		return GraderResult{Name: "file_absent", Pass: false, Detail: "exists: " + rel}
	}
	if os.IsNotExist(err) {
		return GraderResult{Name: "file_absent", Pass: true, Detail: "absent"}
	}
	return GraderResult{Name: "file_absent", Pass: false, Detail: err.Error()}
}

func gradeYAMLValid(ctx GradeContext, rel string) GraderResult {
	rel = ctx.expand(rel)
	p := rel
	if !filepath.IsAbs(p) {
		p = filepath.Join(ctx.WorkDir, rel)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return GraderResult{Name: "yaml_valid", Pass: false, Detail: err.Error()}
	}
	var v any
	if err := yaml.Unmarshal(b, &v); err != nil {
		return GraderResult{Name: "yaml_valid", Pass: false, Detail: err.Error()}
	}
	return GraderResult{Name: "yaml_valid", Pass: true}
}

func gradeSling(ctx GradeContext, cmd string) GraderResult {
	cmd = ctx.expand(cmd)
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return GraderResult{Name: "sling", Pass: false, Detail: "empty sling grader"}
	}
	// Intercept parse/compile verbs that are not top-level CLI commands.
	if fields[0] == "replication" && len(fields) >= 3 {
		path := joinWork(ctx, fields[2])
		switch fields[1] {
		case "parse":
			_, err := sling.LoadReplicationConfigFromFile(path)
			if err != nil {
				return GraderResult{Name: "sling:replication parse", Pass: false, Detail: err.Error()}
			}
			return GraderResult{Name: "sling:replication parse", Pass: true}
		case "compile":
			_, err := CompileReplicationMap(path)
			if err != nil {
				return GraderResult{Name: "sling:replication compile", Pass: false, Detail: err.Error()}
			}
			return GraderResult{Name: "sling:replication compile", Pass: true}
		}
	}
	if fields[0] == "pipeline" && len(fields) >= 3 && fields[1] == "parse" {
		path := joinWork(ctx, fields[2])
		_, err := sling.LoadPipelineConfigFromFile(path)
		if err != nil {
			return GraderResult{Name: "sling:pipeline parse", Pass: false, Detail: err.Error()}
		}
		return GraderResult{Name: "sling:pipeline parse", Pass: true}
	}
	if fields[0] == "api_spec" && len(fields) >= 3 {
		path := joinWork(ctx, fields[2])
		return gradeAPISpec(ctx, fields[1], path, 0, "")
	}
	if fields[0] == "build" && len(fields) >= 2 {
		if rest := fields[1:]; len(rest) > 0 && rest[0] == "run" && ctx.SkipExecute {
			return GraderResult{Name: "sling:" + cmd, Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
		}
	}
	// Exec the real sling binary for everything else (build compile, conns, …).
	if ctx.SlingBin == "" {
		return GraderResult{Name: "sling:" + cmd, Pass: false, Detail: "SLING_BIN not set"}
	}
	args := fields
	c := exec.Command(ctx.SlingBin, args...)
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return GraderResult{Name: "sling:" + cmd, Pass: false, Detail: strings.TrimSpace(buf.String()) + " " + err.Error()}
	}
	return GraderResult{Name: "sling:" + cmd, Pass: true, Detail: strings.TrimSpace(buf.String())}
}

func gradeAPISpecMap(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "api_spec", Pass: false, Detail: "api_spec must be a map"}
	}
	action := cast.ToString(m["action"])
	if action == "" {
		action = "test"
	}
	path := ctx.expand(cast.ToString(m["path"]))
	if path == "" {
		path = ctx.Artifact
	}
	path = joinWork(ctx, path)
	var equals *int
	if _, ok := m["equals"]; ok {
		v := cast.ToInt(m["equals"])
		equals = &v
	}
	endpoint := cast.ToString(m["endpoint"])
	return gradeAPISpecOpts(ctx, action, path, endpoint, equals, m["then_sync"])
}

func gradeAPISpec(ctx GradeContext, action, path string, equals int, endpoint string) GraderResult {
	var eq *int
	if equals > 0 {
		v := equals
		eq = &v
	}
	return gradeAPISpecOpts(ctx, action, path, endpoint, eq, nil)
}

func gradeAPISpecOpts(ctx GradeContext, action, path, endpoint string, equals *int, thenSync any) (r GraderResult) {
	defer func() {
		if rec := recover(); rec != nil {
			r = GraderResult{Name: "sling:api_spec " + action, Pass: false, Detail: fmt.Sprintf("panic: %v", rec)}
		}
	}()
	body, err := os.ReadFile(path)
	if err != nil {
		return GraderResult{Name: "sling:api_spec " + action, Pass: false, Detail: err.Error()}
	}
	spec, err := api.LoadSpec(string(body))
	if err != nil {
		return GraderResult{Name: "sling:api_spec " + action, Pass: false, Detail: err.Error()}
	}
	if action == "parse" {
		return GraderResult{Name: "sling:api_spec parse", Pass: true}
	}
	if action != "test" {
		return GraderResult{Name: "sling:api_spec " + action, Pass: false, Detail: "unknown api_spec action"}
	}
	if ctx.SkipExecute {
		return GraderResult{Name: "sling:api_spec test", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if down := firstDownLaneBAPI(ctx); down != "" {
		return GraderResult{Name: "sling:api_spec test", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + down + ")"}
	}
	first, err := readAPISpecEndpoint(ctx, spec, endpoint, nil, equals)
	if err != nil {
		return GraderResult{Name: "sling:api_spec test", Pass: false, Detail: err.Error()}
	}
	if thenSync == nil {
		return GraderResult{Name: "sling:api_spec test", Pass: true, Detail: fmt.Sprintf("%s: %d records", first.endpoint, first.n)}
	}
	if len(first.syncState) == 0 {
		return GraderResult{Name: "sling:api_spec test", Pass: false, Detail: "then_sync: no stored state after first run"}
	}
	secondEquals, hasSecondEquals := parseThenSyncEquals(thenSync)
	second, err := readAPISpecEndpoint(ctx, spec, first.endpoint, first.syncState, secondEquals)
	if err != nil {
		return GraderResult{Name: "sling:api_spec test", Pass: false, Detail: "then_sync: " + err.Error()}
	}
	if !hasSecondEquals && second.n >= first.n {
		return GraderResult{Name: "sling:api_spec test", Pass: false, Detail: fmt.Sprintf("then_sync: second run %d records, first run %d (stored state did not filter)", second.n, first.n)}
	}
	return GraderResult{Name: "sling:api_spec test", Pass: true, Detail: fmt.Sprintf("%s: %d then %d records", first.endpoint, first.n, second.n)}
}

type apiSpecRun struct {
	endpoint  string
	n         int
	syncState map[string]any
}

func parseThenSyncEquals(thenSync any) (*int, bool) {
	m, ok := asStringMap(thenSync)
	if !ok {
		return nil, false
	}
	if _, ok := m["equals"]; !ok {
		return nil, false
	}
	v := cast.ToInt(m["equals"])
	return &v, true
}

func apiSpecSecrets(ctx GradeContext) map[string]any {
	secrets := map[string]any{
		"token":               "eval-token",
		"base_url":            ctx.FixtureBaseURL,
		"oauth_client_id":     "eval-client",
		"oauth_client_secret": "eval-secret",
	}
	if ctx.FixtureToken != "" {
		secrets["token"] = ctx.FixtureToken
	}
	return secrets
}

func readAPISpecEndpoint(ctx GradeContext, spec api.Spec, endpoint string, seedState map[string]any, equals *int) (apiSpecRun, error) {
	timeout := ctx.APITimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	pageCap := ctx.APIPageCap
	if pageCap <= 0 {
		pageCap = 50
	}
	cctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ac, err := api.NewAPIConnection(cctx, spec, map[string]any{
		"secrets": apiSpecSecrets(ctx),
		"state":   map[string]any{"base_url": ctx.FixtureBaseURL},
	})
	if err != nil {
		return apiSpecRun{}, err
	}
	ep := endpoint
	if ep == "" {
		ep = ac.GetTestEndpoint()
	}
	if ep == "" {
		return apiSpecRun{}, fmt.Errorf("no endpoints to test")
	}
	if missingStopCondition(spec, ep) {
		return apiSpecRun{}, fmt.Errorf("missing stop_condition (infinite pagination)")
	}
	if len(seedState) > 0 {
		if err := ac.PutSyncedState(ep, seedState); err != nil {
			return apiSpecRun{}, fmt.Errorf("put sync state: %w", err)
		}
	}
	limit := pageCap * 100
	if equals != nil && *equals > 0 {
		limit = *equals * 4
		if limit < 1000 {
			limit = 1000
		}
	}
	done := make(chan struct{})
	var dfErr error
	var n int
	var maxTS string
	go func() {
		defer close(done)
		df, err := ac.ReadDataflow(ep, api.APIStreamConfig{Limit: limit})
		if err != nil {
			dfErr = err
			return
		}
		data, err := df.Collect()
		if err != nil {
			dfErr = err
			return
		}
		n = len(data.Rows)
		maxTS = maxColumnString(data.Columns.Names(), data.Rows, "updated_at")
	}()
	select {
	case <-cctx.Done():
		return apiSpecRun{}, fmt.Errorf("pagination timeout after %s (possible missing stop_condition)", timeout)
	case <-done:
	}
	if dfErr != nil {
		return apiSpecRun{}, dfErr
	}
	allowZero := equals != nil && *equals == 0
	if n == 0 && !allowZero {
		return apiSpecRun{}, fmt.Errorf("endpoint %s returned 0 records", ep)
	}
	if n >= limit {
		return apiSpecRun{}, fmt.Errorf("pagination hit record cap %d (possible missing stop_condition)", limit)
	}
	if equals != nil && n != *equals {
		return apiSpecRun{}, fmt.Errorf("%s: got %d records want %d", ep, n, *equals)
	}
	syncState, _ := ac.GetSyncedState(ep)
	if len(syncState) == 0 && maxTS != "" {
		syncState = map[string]any{"updated_since": maxTS}
	}
	return apiSpecRun{endpoint: ep, n: n, syncState: syncState}, nil
}

func maxColumnString(names []string, rows [][]any, col string) string {
	idx := -1
	for i, n := range names {
		if strings.EqualFold(n, col) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ""
	}
	max := ""
	for _, row := range rows {
		if idx >= len(row) {
			continue
		}
		s := cast.ToString(row[idx])
		if s > max {
			max = s
		}
	}
	return max
}

func missingStopCondition(spec api.Spec, endpoint string) bool {
	for _, ep := range spec.EndpointMap {
		if endpoint != "" && !strings.EqualFold(ep.Name, endpoint) {
			continue
		}
		if ep.Pagination.NextState == nil && len(ep.Pagination.NextState) == 0 {
			// NextState may be a map; also check via yaml dump
		}
		hasNext := ep.Pagination.NextState != nil && len(ep.Pagination.NextState) > 0
		if hasNext && strings.TrimSpace(ep.Pagination.StopCondition) == "" {
			return true
		}
	}
	return false
}

func firstDownLaneBAPI(ctx GradeContext) string {
	for n, down := range ctx.ConnDown {
		if !down {
			continue
		}
		u := strings.ToUpper(n)
		if u == "MOCK_API" || strings.Contains(u, "MOCK_API") {
			// Fixture server: fail at request time, do not skip.
			continue
		}
		if strings.Contains(u, "API") || u == "GITHUB" || u == "OMDB" || u == "DUMMYJSON" {
			return n
		}
	}
	return ""
}

func firstDownAPI(ctx GradeContext) string {
	return firstDownLaneBAPI(ctx)
}

func gradeExpected(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "expected", Pass: false, Detail: "expected must be a map"}
	}
	file := cast.ToString(m["file"])
	if file == "" {
		file = "expected.yaml"
	}
	expPath := file
	if !filepath.IsAbs(expPath) {
		expPath = filepath.Join(ctx.CaseDir, file)
		if _, err := os.Stat(expPath); err != nil {
			expPath = filepath.Join(ctx.WorkDir, file)
		}
	}
	actPath := ctx.artifactPath()
	policy := MatchPolicy{
		MustMatch:    toStringList(m["must_match"]),
		MustNotExist: toStringList(m["must_not_exist"]),
	}
	compare := cast.ToString(m["compare"])
	if compare == "" {
		compare = "compiled"
	}
	var results []CompareResult
	var err error
	switch compare {
	case "compiled":
		results, err = CompareCompiledFiles(actPath, expPath, policy)
	default:
		act, e1 := YAMLMap(actPath)
		exp, e2 := YAMLMap(expPath)
		if e1 != nil || e2 != nil {
			err = fmt.Errorf("raw compare: %v %v", e1, e2)
			break
		}
		results = CompareCompiled(act, exp, policy)
	}
	if err != nil {
		return GraderResult{Name: "expected", Pass: false, Detail: err.Error()}
	}
	var fails []string
	for _, r := range results {
		if !r.Pass {
			fails = append(fails, r.Path+": "+r.Message)
		}
	}
	if len(fails) > 0 {
		return GraderResult{Name: "expected", Pass: false, Detail: strings.Join(fails, "; ")}
	}
	return GraderResult{Name: "expected", Pass: true, Detail: fmt.Sprintf("%d checks", len(results))}
}

func gradeDryRun(ctx GradeContext, cmd string) GraderResult {
	cmd = ctx.expand(cmd)
	if ctx.SkipExecute {
		return GraderResult{Name: "dry_run", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if down := firstDownConn(ctx); down != "" {
		return GraderResult{Name: "dry_run", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + down + ")"}
	}
	if ctx.SlingBin == "" {
		return GraderResult{Name: "dry_run", Pass: false, Detail: "SLING_BIN not set"}
	}
	// Accept either a full command or just the artifact path.
	args := strings.Fields(cmd)
	env := append([]string{}, ctx.Env...)
	env = append(env, "SLING_DRY_RUN=true")
	if len(args) == 0 {
		args = []string{"run", "-r", ctx.Artifact}
	} else if args[0] == "SLING_DRY_RUN=true" {
		args = args[1:]
	}
	if len(args) > 0 && args[0] == "sling" {
		args = args[1:]
	}
	c := exec.Command(ctx.SlingBin, args...)
	c.Dir = ctx.WorkDir
	c.Env = env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return GraderResult{Name: "dry_run", Pass: false, Detail: strings.TrimSpace(buf.String()) + " " + err.Error()}
	}
	return GraderResult{Name: "dry_run", Pass: true}
}

func gradeTranscriptContains(ctx GradeContext, needle string) GraderResult {
	if needle == "" {
		return GraderResult{Name: "transcript_contains", Pass: false, Detail: "empty needle"}
	}
	if strings.Contains(ctx.Transcript, needle) {
		return GraderResult{Name: "transcript_contains", Pass: true, Detail: needle}
	}
	return GraderResult{Name: "transcript_contains", Pass: false, Detail: "not found: " + needle}
}

func gradeTranscriptAbsent(ctx GradeContext, needle string) GraderResult {
	if needle == "" {
		return GraderResult{Name: "transcript_absent", Pass: false, Detail: "empty needle"}
	}
	cmds := executedCommands(ctx.Transcript)
	for _, cmd := range cmds {
		if strings.Contains(cmd, needle) {
			return GraderResult{Name: "transcript_absent", Pass: false, Detail: "found in tool call: " + needle}
		}
	}
	return GraderResult{Name: "transcript_absent", Pass: true, Detail: "absent"}
}

func gradeTranscriptAbsentRaw(ctx GradeContext, needle string) GraderResult {
	if needle == "" {
		return GraderResult{Name: "transcript_absent_raw", Pass: false, Detail: "empty needle"}
	}
	if strings.Contains(ctx.Transcript, needle) {
		return GraderResult{Name: "transcript_absent_raw", Pass: false, Detail: "found: " + needle}
	}
	return GraderResult{Name: "transcript_absent_raw", Pass: true, Detail: "absent"}
}

// executedCommands extracts shell/tool command strings from a live-arm
// transcript. Prose that merely mentions a command is ignored (H8).
// Prefer per-line JSON (Codex NDJSON). Skip lines that do not parse (stderr).
// Fall back to a stream decoder for one pretty JSON blob (Claude).
// Line-as-command fallback runs only when no JSON object parses (mock arm).
func executedCommands(transcript string) []string {
	var out []string
	parsed := false
	for _, line := range strings.Split(transcript, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var raw json.RawMessage
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}
		parsed = true
		out = append(out, commandsFromJSON(raw)...)
	}
	if parsed {
		return out
	}
	dec := json.NewDecoder(strings.NewReader(transcript))
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		parsed = true
		out = append(out, commandsFromJSON(raw)...)
	}
	if parsed {
		return out
	}
	// Non-JSON transcript (mock arm): treat each non-empty line as a command.
	for _, line := range strings.Split(transcript, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func commandsFromJSON(raw json.RawMessage) []string {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	var out []string
	walkJSONCommands(v, &out)
	return out
}

func walkJSONCommands(v any, out *[]string) {
	switch t := v.(type) {
	case map[string]any:
		// Codex command_execution: take item.command only. Do not walk
		// aggregated_output (skill file text can mention "sling run").
		if jsonMapString(t, "type") == "command_execution" {
			if s, ok := t["command"].(string); ok && s != "" {
				*out = append(*out, s)
			}
			return
		}
		for _, key := range []string{"command", "cmd", "bash", "input"} {
			if s, ok := t[key].(string); ok && looksLikeCommand(s) {
				*out = append(*out, s)
			}
		}
		if name, _ := t["name"].(string); name == "Bash" || name == "bash" || name == "shell" {
			if inp, ok := t["input"].(map[string]any); ok {
				if s, ok := inp["command"].(string); ok {
					*out = append(*out, s)
				}
			}
		}
		if tool, _ := t["tool"].(string); tool == "bash" || tool == "shell" || tool == "Bash" {
			if st, ok := t["state"].(map[string]any); ok {
				if inp, ok := st["input"].(map[string]any); ok {
					if s, ok := inp["command"].(string); ok {
						*out = append(*out, s)
					}
				}
			}
		}
		for _, child := range t {
			walkJSONCommands(child, out)
		}
	case []any:
		for _, child := range t {
			walkJSONCommands(child, out)
		}
	}
}

func looksLikeCommand(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" || strings.Contains(s, "\n") && len(s) > 400 {
		return false
	}
	return strings.Contains(s, "sling ") || strings.HasPrefix(s, "sling") ||
		strings.Contains(s, " && ") || strings.HasPrefix(s, "/")
}

func gradeYQ(ctx GradeContext, expr string) GraderResult {
	p := ctx.artifactPath()
	m, err := YAMLMap(p)
	if err != nil {
		return GraderResult{Name: "yq", Pass: false, Detail: err.Error()}
	}
	ok, detail := evalYQ(m, expr)
	return GraderResult{Name: "yq", Pass: ok, Detail: detail}
}

// evalYQ handles a tiny subset used by cases: `.streams | length == N`.
func evalYQ(m map[string]any, expr string) (bool, string) {
	expr = strings.TrimSpace(expr)
	re := regexp.MustCompile(`^\.streams\s*\|\s*length\s*==\s*(\d+)$`)
	if mm := re.FindStringSubmatch(expr); mm != nil {
		want := cast.ToInt(mm[1])
		streams, _ := asMap(m["streams"])
		got := 0
		if streams != nil {
			got = len(streams)
		}
		if got == want {
			return true, fmt.Sprintf("length=%d", got)
		}
		return false, fmt.Sprintf("length want %d got %d", want, got)
	}
	return false, "unsupported yq expr: " + expr
}

func gradeSQLEquiv(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: "sql_equiv must be a map"}
	}
	conn := cast.ToString(m["connection"])
	if ctx.ConnDown[conn] {
		// Fallback: regex over compiled SQL for required tables/columns.
		return gradeSQLFallback(ctx, m)
	}
	actualPath := cast.ToString(m["actual_path"])
	sql, err := extractSQL(ctx, actualPath)
	if err != nil {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: err.Error()}
	}
	expFile := cast.ToString(m["expected_sql"])
	if !filepath.IsAbs(expFile) {
		cand := filepath.Join(ctx.CaseDir, expFile)
		if _, e := os.Stat(cand); e == nil {
			expFile = cand
		} else {
			expFile = filepath.Join(filepath.Dir(ctx.CaseDir), "..", expFile)
		}
	}
	expBytes, err := os.ReadFile(expFile)
	if err != nil {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: err.Error()}
	}
	if ctx.SlingBin == "" || ctx.ConnDown[conn] {
		return gradeSQLFallback(ctx, m)
	}
	orderBy := cast.ToString(m["order_by"])
	got, err := execSQL(ctx, conn, wrapOrder(sql, orderBy))
	if err != nil {
		// Conn failed at runtime — fall back.
		return gradeSQLFallback(ctx, m)
	}
	want, err := execSQL(ctx, conn, wrapOrder(string(expBytes), orderBy))
	if err != nil {
		return gradeSQLFallback(ctx, m)
	}
	if strings.TrimSpace(got) != strings.TrimSpace(want) {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: "rows differ"}
	}
	return GraderResult{Name: "sql_equiv", Pass: true}
}

func gradeSQLFallback(ctx GradeContext, m map[string]any) GraderResult {
	actualPath := cast.ToString(m["actual_path"])
	sql, err := extractSQL(ctx, actualPath)
	if err != nil {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: "fallback: " + err.Error()}
	}
	expFile := cast.ToString(m["expected_sql"])
	body, err := os.ReadFile(resolveCaseFile(ctx, expFile))
	if err != nil {
		// If expected file is missing, still require the SQL to parse-ish.
		if strings.TrimSpace(sql) == "" {
			return GraderResult{Name: "sql_equiv", Pass: false, Detail: "empty sql"}
		}
		return GraderResult{Name: "sql_equiv", Pass: true, Skip: true, Detail: "SKIPPED (conn down; no expected_sql)"}
	}
	needles := sqlIdents(string(body))
	var missing []string
	low := strings.ToLower(sql)
	for _, n := range needles {
		if !strings.Contains(low, strings.ToLower(n)) {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return GraderResult{Name: "sql_equiv", Pass: false, Detail: "fallback missing refs: " + strings.Join(missing, ",")}
	}
	return GraderResult{Name: "sql_equiv", Pass: true, Detail: "fallback regex refs"}
}

func extractSQL(ctx GradeContext, path string) (string, error) {
	compiled, err := CompileReplicationMap(ctx.artifactPath())
	if err != nil {
		return "", err
	}
	segs := strings.Split(path, ".")
	v, ok := valueAt(compiled, segs)
	if !ok {
		// Try raw YAML.
		raw, err := YAMLMap(ctx.artifactPath())
		if err != nil {
			return "", fmt.Errorf("sql path %s not found", path)
		}
		v, ok = valueAt(raw, segs)
		if !ok {
			return "", fmt.Errorf("sql path %s not found", path)
		}
	}
	return cast.ToString(v), nil
}

func sqlIdents(sql string) []string {
	re := regexp.MustCompile(`(?i)\b(?:from|join)\s+([a-zA-Z_][\w.]*)`)
	ms := re.FindAllStringSubmatch(sql, -1)
	seen := map[string]bool{}
	var out []string
	for _, m := range ms {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}

func wrapOrder(sql, orderBy string) string {
	sql = strings.TrimSpace(sql)
	if orderBy == "" || sql == "" {
		return sql
	}
	if strings.Contains(strings.ToLower(sql), "order by") {
		return sql
	}
	return "select * from (" + sql + ") _eq order by " + orderBy
}

func execSQL(ctx GradeContext, conn, sql string) (string, error) {
	c := exec.Command(ctx.SlingBin, "conns", "exec", conn, sql, "-o", "csv")
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var stdout, stderr bytes.Buffer
	c.Stdout = &stdout
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return "", fmt.Errorf("%s: %w", stderr.String(), err)
	}
	return stdout.String(), nil
}

func gradeSkeleton(ctx GradeContext, raw any) GraderResult {
	spec, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "skeleton", Pass: false, Detail: "skeleton must be a map"}
	}
	p := ctx.artifactPath()
	doc, err := YAMLMap(p)
	if err != nil {
		return GraderResult{Name: "skeleton", Pass: false, Detail: err.Error()}
	}
	steps := collectSteps(toAnyList(doc["steps"]))
	var fails []string
	for _, want := range toMapList(spec["must_contain_steps"]) {
		if !stepPresent(steps, want) {
			fails = append(fails, "missing step "+fmt.Sprint(want))
		}
	}
	for _, ban := range toMapList(spec["must_not_contain_steps"]) {
		if stepPresent(steps, ban) {
			fails = append(fails, "forbidden step "+fmt.Sprint(ban))
		}
	}
	if len(fails) > 0 {
		return GraderResult{Name: "skeleton", Pass: false, Detail: strings.Join(fails, "; ")}
	}
	return GraderResult{Name: "skeleton", Pass: true}
}

func collectSteps(raw []any) []map[string]any {
	var out []map[string]any
	for _, s := range raw {
		sm, ok := asMap(s)
		if !ok {
			continue
		}
		out = append(out, sm)
		if nested := toAnyList(sm["steps"]); len(nested) > 0 {
			out = append(out, collectSteps(nested)...)
		}
	}
	return out
}

func stepPresent(steps []map[string]any, want map[string]any) bool {
	for _, st := range steps {
		if stepSubset(st, want) {
			return true
		}
	}
	return false
}

func stepSubset(have, want map[string]any) bool {
	for k, wv := range want {
		hv, ok := have[k]
		if !ok {
			return false
		}
		if !equalAny(normalize(hv), normalize(wv)) && stringify(hv) != stringify(wv) {
			return false
		}
	}
	return true
}

func gradeOutcome(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "outcome", Pass: false, Detail: "outcome must be a map"}
	}
	if ctx.SkipExecute {
		return GraderResult{Name: "outcome", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if down := firstDownConn(ctx); down != "" {
		return GraderResult{Name: "outcome", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + down + ")"}
	}
	if cast.ToString(m["first_run"]) != "" || cast.ToString(m["second_run"]) != "" {
		return gradeOutcomeTwice(ctx, m)
	}
	run := ctx.expand(cast.ToString(m["run"]))
	if run != "" {
		if _, err := runOutcomeCmd(ctx, run); err != nil {
			return GraderResult{Name: "outcome", Pass: false, Detail: err.Error()}
		}
	}
	// Nested checks: file_exists or sling commands.
	checks := toAnyList(m["checks"])
	for _, ch := range checks {
		cm, ok := asStringMap(ch)
		if !ok {
			continue
		}
		if p := cast.ToString(cm["file_exists"]); p != "" {
			r := gradeFileExists(ctx, p)
			if !r.Pass {
				r.Name = "outcome"
				return r
			}
		}
		if s := cast.ToString(cm["sling"]); s != "" {
			r := gradeSling(ctx, s)
			if r.Skip {
				continue
			}
			if !r.Pass {
				r.Name = "outcome"
				return r
			}
		}
	}
	return GraderResult{Name: "outcome", Pass: true}
}

func gradeOutcomeTwice(ctx GradeContext, m map[string]any) GraderResult {
	if pre, ok := asStringMap(m["pre"]); ok {
		conn := cast.ToString(pre["connection"])
		sql := cast.ToString(pre["sql"])
		if sql == "" {
			return GraderResult{Name: "outcome", Pass: false, Detail: "pre missing sql"}
		}
		if err := execSQLWrite(ctx, conn, sql); err != nil {
			return GraderResult{Name: "outcome", Pass: false, Detail: "pre: " + err.Error()}
		}
	}
	first := ctx.expand(cast.ToString(m["first_run"]))
	if first == "" {
		return GraderResult{Name: "outcome", Pass: false, Detail: "missing first_run"}
	}
	if _, err := runOutcomeCmd(ctx, first); err != nil {
		return GraderResult{Name: "outcome", Pass: false, Detail: "first_run: " + err.Error()}
	}
	if ins, ok := asStringMap(m["insert"]); ok {
		conn := cast.ToString(ins["connection"])
		sql := cast.ToString(ins["sql"])
		if sql == "" {
			return GraderResult{Name: "outcome", Pass: false, Detail: "insert missing sql"}
		}
		if err := execSQLWrite(ctx, conn, sql); err != nil {
			return GraderResult{Name: "outcome", Pass: false, Detail: "insert: " + err.Error()}
		}
	}
	second := ctx.expand(cast.ToString(m["second_run"]))
	if second == "" {
		return GraderResult{Name: "outcome", Pass: false, Detail: "missing second_run"}
	}
	out2, err := runOutcomeCmd(ctx, second)
	if err != nil {
		return GraderResult{Name: "outcome", Pass: false, Detail: "second_run: " + err.Error()}
	}
	if rm, ok := asStringMap(m["rows_moved"]); ok {
		got, ok := parseInsertedRows(out2)
		if !ok {
			return GraderResult{Name: "outcome", Pass: false, Detail: "second_run: no inserted-row count in output"}
		}
		want := cast.ToInt(rm["equals"])
		tol := cast.ToInt(rm["tolerance"])
		if !intClose(got, want, tol) {
			return GraderResult{Name: "outcome", Pass: false, Detail: fmt.Sprintf("second_run moved %d rows, want %d ±%d", got, want, tol)}
		}
	}
	if q, ok := asStringMap(m["query"]); ok {
		r := gradeQuery(ctx, q)
		if r.Skip {
			return GraderResult{Name: "outcome", Pass: true, Detail: r.Detail}
		}
		if !r.Pass {
			r.Name = "outcome"
			r.Detail = "after second_run: " + r.Detail
			return r
		}
	}
	return GraderResult{Name: "outcome", Pass: true}
}

func runOutcomeCmd(ctx GradeContext, run string) (string, error) {
	if ctx.SlingBin == "" {
		return "", fmt.Errorf("SLING_BIN not set")
	}
	args := strings.Fields(run)
	if len(args) > 0 && args[0] == "sling" {
		args = args[1:]
	}
	c := exec.Command(ctx.SlingBin, args...)
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return buf.String(), fmt.Errorf("%s %w", strings.TrimSpace(buf.String()), err)
	}
	return buf.String(), nil
}

var (
	reInsertedNRows = regexp.MustCompile(`(?i)inserted\s+(\d+)\s+rows`)
	reNRowsInserted = regexp.MustCompile(`(?i)(\d+)\s+rows\s+inserted`)
)

func parseInsertedRows(out string) (int, bool) {
	if m := reInsertedNRows.FindAllStringSubmatch(out, -1); len(m) > 0 {
		n, err := strconv.Atoi(m[len(m)-1][1])
		return n, err == nil
	}
	if m := reNRowsInserted.FindAllStringSubmatch(out, -1); len(m) > 0 {
		n, err := strconv.Atoi(m[len(m)-1][1])
		return n, err == nil
	}
	return 0, false
}

func intClose(got, want, tol int) bool {
	d := got - want
	if d < 0 {
		d = -d
	}
	return d <= tol
}

func execSQLWrite(ctx GradeContext, conn, sql string) error {
	if ctx.SlingBin == "" {
		return fmt.Errorf("SLING_BIN not set")
	}
	c := exec.Command(ctx.SlingBin, "conns", "exec", conn, sql)
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(buf.String()), err)
	}
	return nil
}

func gradeQuery(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "query", Pass: false, Detail: "query must be a map"}
	}
	conn := cast.ToString(m["connection"])
	sql := ctx.expand(cast.ToString(m["sql"]))
	if sql == "" {
		return GraderResult{Name: "query", Pass: false, Detail: "missing sql"}
	}
	if strings.Contains(sql, "{parquet}") {
		files := globWork(ctx.WorkDir, "**/*.parquet")
		if len(files) == 0 {
			if ctx.SkipExecute {
				return GraderResult{Name: "query", Pass: true, Skip: true, Detail: "SKIPPED (mock: no parquet)"}
			}
			return GraderResult{Name: "query", Pass: false, Detail: "no parquet files in workdir"}
		}
		quoted := make([]string, len(files))
		for i, f := range files {
			quoted[i] = "'" + strings.ReplaceAll(f, "'", "''") + "'"
		}
		sql = strings.ReplaceAll(sql, "{parquet}", strings.Join(quoted, ", "))
	}
	if ctx.SkipExecute {
		return GraderResult{Name: "query", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if conn != "" && ctx.ConnDown[conn] {
		return GraderResult{Name: "query", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + conn + ")"}
	}
	if ctx.SlingBin == "" {
		return GraderResult{Name: "query", Pass: false, Detail: "SLING_BIN not set"}
	}
	got, err := execSQLScalar(ctx, conn, sql)
	if err != nil {
		return GraderResult{Name: "query", Pass: false, Detail: err.Error()}
	}
	if eq := m["equals"]; eq != nil {
		if !scalarsEqual(got, eq, m["tolerance"]) {
			return GraderResult{Name: "query", Pass: false, Detail: fmt.Sprintf("want %v got %v", eq, got)}
		}
		return GraderResult{Name: "query", Pass: true, Detail: fmt.Sprintf("%v", got)}
	}
	eqSQL := cast.ToString(m["equals_query"])
	if eqSQL != "" {
		if !looksLikeSQL(eqSQL) {
			p := resolveCaseFile(ctx, eqSQL)
			b, err := os.ReadFile(p)
			if err != nil {
				return GraderResult{Name: "query", Pass: false, Detail: err.Error()}
			}
			eqSQL = string(b)
		}
		want, err := execSQLScalar(ctx, conn, eqSQL)
		if err != nil {
			return GraderResult{Name: "query", Pass: false, Detail: "equals_query: " + err.Error()}
		}
		if !scalarsEqual(got, want, m["tolerance"]) {
			return GraderResult{Name: "query", Pass: false, Detail: fmt.Sprintf("want %v got %v", want, got)}
		}
		return GraderResult{Name: "query", Pass: true, Detail: fmt.Sprintf("%v", got)}
	}
	return GraderResult{Name: "query", Pass: true, Detail: fmt.Sprintf("%v", got)}
}

func gradeRowsEqual(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: "rows_equal must be a map"}
	}
	conn := cast.ToString(m["connection"])
	actual := cast.ToString(m["actual_sql"])
	if actual == "" {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: "missing actual_sql"}
	}
	if ctx.SkipExecute {
		return GraderResult{Name: "rows_equal", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if conn != "" && ctx.ConnDown[conn] {
		return GraderResult{Name: "rows_equal", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + conn + ")"}
	}
	expSQL := cast.ToString(m["expected_sql"])
	if expSQL == "" {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: "missing expected_sql"}
	}
	if !looksLikeSQL(expSQL) {
		p := resolveCaseFile(ctx, expSQL)
		b, err := os.ReadFile(p)
		if err != nil {
			return GraderResult{Name: "rows_equal", Pass: false, Detail: err.Error()}
		}
		expSQL = string(b)
	}
	orderBy := cast.ToString(m["order_by"])
	got, err := execSQL(ctx, conn, wrapOrder(actual, orderBy))
	if err != nil {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: err.Error()}
	}
	want, err := execSQL(ctx, conn, wrapOrder(expSQL, orderBy))
	if err != nil {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: err.Error()}
	}
	if cols := toStringList(m["columns"]); len(cols) > 0 {
		got = projectSQLText(got, cols)
		want = projectSQLText(want, cols)
	}
	if strings.TrimSpace(got) != strings.TrimSpace(want) {
		return GraderResult{Name: "rows_equal", Pass: false, Detail: "rows differ"}
	}
	return GraderResult{Name: "rows_equal", Pass: true}
}

func gradeDAG(ctx GradeContext, raw any) GraderResult {
	m, ok := asStringMap(raw)
	if !ok {
		return GraderResult{Name: "dag", Pass: false, Detail: "dag must be a map"}
	}
	model := cast.ToString(m["model"])
	wantDeps := toStringList(m["depends_on"])
	if model == "" {
		return GraderResult{Name: "dag", Pass: false, Detail: "missing model"}
	}
	if ctx.SlingBin == "" {
		return GraderResult{Name: "dag", Pass: false, Detail: "SLING_BIN not set"}
	}
	dir := cast.ToString(m["dir"])
	if dir == "" {
		dir = "."
	}
	c := exec.Command(ctx.SlingBin, "build", "compile", "--json", dir)
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return GraderResult{Name: "dag", Pass: false, Detail: strings.TrimSpace(buf.String()) + " " + err.Error()}
	}
	deps, err := parseCompileDeps(buf.String(), model)
	if err != nil {
		return GraderResult{Name: "dag", Pass: false, Detail: err.Error()}
	}
	have := map[string]bool{}
	for _, d := range deps {
		have[d] = true
		if i := strings.LastIndex(d, "."); i >= 0 {
			have[d[i+1:]] = true
		}
	}
	var missing []string
	for _, w := range wantDeps {
		if have[w] {
			continue
		}
		base := w
		if i := strings.LastIndex(w, "."); i >= 0 {
			base = w[i+1:]
		}
		if have[base] {
			continue
		}
		missing = append(missing, w)
	}
	if len(missing) > 0 {
		return GraderResult{Name: "dag", Pass: false, Detail: fmt.Sprintf("%s missing depends_on %v (have %v)", model, missing, deps)}
	}
	return GraderResult{Name: "dag", Pass: true, Detail: fmt.Sprintf("%s -> %v", model, deps)}
}

func gradeTestsPass(ctx GradeContext, raw any) GraderResult {
	m, _ := asStringMap(raw)
	if ctx.SkipExecute {
		return GraderResult{Name: "tests_pass", Pass: true, Skip: true, Detail: "SKIPPED (mock: no execute)"}
	}
	if down := firstDownConn(ctx); down != "" {
		return GraderResult{Name: "tests_pass", Pass: true, Skip: true, Detail: "SKIPPED (conn down: " + down + ")"}
	}
	if ctx.SlingBin == "" {
		return GraderResult{Name: "tests_pass", Pass: false, Detail: "SLING_BIN not set"}
	}
	dir := "."
	target := ""
	if m != nil {
		if d := cast.ToString(m["dir"]); d != "" {
			dir = d
		}
		target = cast.ToString(m["target"])
	}
	args := []string{"build", "compile", "--json", dir}
	c := exec.Command(ctx.SlingBin, args...)
	c.Dir = ctx.WorkDir
	c.Env = ctx.Env
	var buf bytes.Buffer
	c.Stdout = &buf
	c.Stderr = &buf
	if err := c.Run(); err != nil {
		return GraderResult{Name: "tests_pass", Pass: false, Detail: "compile: " + strings.TrimSpace(buf.String())}
	}
	nTests := countCompileTests(buf.String())
	if nTests == 0 {
		return GraderResult{Name: "tests_pass", Pass: false, Detail: "no declarative tests defined"}
	}
	runArgs := []string{"build", "test"}
	if target != "" {
		runArgs = append(runArgs, "--target", target)
	}
	runArgs = append(runArgs, dir)
	c2 := exec.Command(ctx.SlingBin, runArgs...)
	c2.Dir = ctx.WorkDir
	c2.Env = ctx.Env
	var buf2 bytes.Buffer
	c2.Stdout = &buf2
	c2.Stderr = &buf2
	if err := c2.Run(); err != nil {
		return GraderResult{Name: "tests_pass", Pass: false, Detail: "test failed: " + strings.TrimSpace(buf2.String())}
	}
	out := buf2.String()
	if strings.Contains(strings.ToLower(out), "failed") && strings.Contains(strings.ToLower(out), "test") {
		return GraderResult{Name: "tests_pass", Pass: false, Detail: strings.TrimSpace(out)}
	}
	return GraderResult{Name: "tests_pass", Pass: true, Detail: fmt.Sprintf("%d tests", nTests)}
}

func execSQLScalar(ctx GradeContext, conn, sql string) (any, error) {
	raw, err := execSQL(ctx, conn, sql)
	if err != nil {
		return nil, err
	}
	return firstScalar(raw), nil
}

func firstScalar(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	// CSV (-o csv): first line is the header. Use the first data cell.
	if recs, err := csv.NewReader(strings.NewReader(raw)).ReadAll(); err == nil && len(recs) >= 2 && len(recs[1]) > 0 {
		cell := strings.TrimSpace(recs[1][0])
		if n, err := strconv.ParseFloat(cell, 64); err == nil {
			return n
		}
		if b, err := strconv.ParseBool(cell); err == nil && (cell == "true" || cell == "false" || cell == "TRUE" || cell == "FALSE") {
			return b
		}
		return cell
	}
	lines := strings.Split(raw, "\n")
	var data []string
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" || strings.HasPrefix(ln, "+") || (strings.HasPrefix(ln, "|") && strings.Contains(ln, "---")) {
			continue
		}
		data = append(data, ln)
	}
	for i, ln := range data {
		if i == 0 && isHeaderish(ln) && !strings.HasPrefix(ln, "|") {
			continue
		}
		if strings.HasPrefix(ln, "|") {
			parts := strings.Split(ln, "|")
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p == "" || isHeaderish(p) {
					continue
				}
				if n, err := strconv.ParseFloat(p, 64); err == nil {
					return n
				}
				return p
			}
			continue
		}
		if n, err := strconv.ParseFloat(ln, 64); err == nil {
			return n
		}
		if !isHeaderish(ln) {
			return ln
		}
	}
	return strings.TrimSpace(raw)
}

func isHeaderish(s string) bool {
	low := strings.ToLower(strings.TrimSpace(s))
	if low == "" || strings.HasPrefix(s, "─") || strings.HasPrefix(s, "-") {
		return true
	}
	if low == "count" || strings.Contains(low, "count(") || low == "n" || low == "?column?" {
		return true
	}
	if strings.Contains(low, "cast_") || strings.HasSuffix(low, "__as_varchar") || strings.HasSuffix(low, "::text") {
		return true
	}
	return false
}

func scalarsEqual(got, want any, tol any) bool {
	gf, gok := toFloat(got)
	wf, wok := toFloat(want)
	if gok && wok {
		eps := 0.0
		if tol != nil {
			eps = cast.ToFloat64(tol)
		}
		return math.Abs(gf-wf) <= eps
	}
	return strings.TrimSpace(cast.ToString(got)) == strings.TrimSpace(cast.ToString(want))
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return n, err == nil
	default:
		n, err := strconv.ParseFloat(strings.TrimSpace(cast.ToString(v)), 64)
		return n, err == nil
	}
}

func parseCompileDeps(raw, model string) ([]string, error) {
	start := strings.Index(raw, "{")
	end := strings.LastIndex(raw, "}")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("no compile JSON")
	}
	var payload map[string]any
	if err := jsonUnmarshal([]byte(raw[start:end+1]), &payload); err != nil {
		// maybe an array of sub-builds
		var arr []map[string]any
		if err2 := jsonUnmarshal([]byte(raw[start:]), &arr); err2 != nil {
			return nil, fmt.Errorf("compile json: %w", err)
		}
		if len(arr) > 0 {
			payload = arr[0]
		}
	}
	nodes, _ := payload["nodes"].([]any)
	wantBase := model
	if i := strings.LastIndex(model, "."); i >= 0 {
		wantBase = model[i+1:]
	}
	for _, n := range nodes {
		nm, ok := asMap(n)
		if !ok {
			continue
		}
		name := cast.ToString(nm["name"])
		base := name
		if i := strings.LastIndex(name, "."); i >= 0 {
			base = name[i+1:]
		}
		if name != model && base != wantBase && name != wantBase {
			continue
		}
		return toStringList(nm["dependencies"]), nil
	}
	return nil, fmt.Errorf("model %s not in compile output", model)
}

func countCompileTests(raw string) int {
	start := strings.Index(raw, "{")
	if start < 0 {
		return 0
	}
	var payload map[string]any
	if jsonUnmarshal([]byte(raw[start:]), &payload) != nil {
		return 0
	}
	n := 0
	nodes, _ := payload["nodes"].([]any)
	for _, node := range nodes {
		nm, ok := asMap(node)
		if !ok {
			continue
		}
		n += len(toAnyList(nm["tests"]))
	}
	return n
}

func projectSQLText(raw string, cols []string) string {
	// Best-effort: keep lines that look like the selected columns. If the
	// exec output is already a full row dump, return as-is.
	_ = cols
	return raw
}

func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func joinWork(ctx GradeContext, p string) string {
	p = ctx.expand(p)
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(ctx.WorkDir, p)
}

func firstDownConn(ctx GradeContext) string {
	for n, down := range ctx.ConnDown {
		if down {
			return n
		}
	}
	return ""
}

func resolveCaseFile(ctx GradeContext, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	cands := []string{
		filepath.Join(ctx.CaseDir, p),
		filepath.Join(ctx.WorkDir, p),
		filepath.Join(evalsDir(), p),
	}
	for _, c := range cands {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return filepath.Join(ctx.CaseDir, p)
}

func asStringMap(v any) (map[string]any, bool) {
	return asMap(v)
}

func toStringList(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			out = append(out, cast.ToString(x))
		}
		return out
	}
	return nil
}

func toMapList(v any) []map[string]any {
	switch t := v.(type) {
	case []map[string]any:
		return t
	case []any:
		var out []map[string]any
		for _, x := range t {
			if m, ok := asMap(x); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func toAnyList(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case []map[string]any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = x
		}
		return out
	}
	return nil
}

// MatchPolicy is the compiled-form compare rule set.
type MatchPolicy struct {
	MustMatch    []string `yaml:"must_match"`
	MustNotExist []string `yaml:"must_not_exist"`
}

// CompareResult is one path check.
type CompareResult struct {
	Path    string
	Pass    bool
	Message string
	Want    any
	Got     any
}

// CompileReplicationMap loads a replication YAML and expands defaults.
// It uses sling.LoadReplicationConfig and SetStreamDefaults (the product path).
// Full Compile() needs a live source conn; we fall back to defaults-only when
// Compile fails so mock trials stay local.
func CompileReplicationMap(path string) (map[string]any, error) {
	cfg, err := sling.LoadReplicationConfigFromFile(path)
	if err != nil {
		return nil, fmt.Errorf("parse replication %s: %w", path, err)
	}
	return compiledReplicationMap(cfg)
}

// CompileReplicationYAML compiles from bytes (tests / in-memory).
func CompileReplicationYAML(body []byte) (map[string]any, error) {
	cfg, err := sling.LoadReplicationConfig(string(body))
	if err != nil {
		return nil, fmt.Errorf("parse replication yaml: %w", err)
	}
	return compiledReplicationMap(cfg)
}

func compiledReplicationMap(cfg sling.ReplicationConfig) (map[string]any, error) {
	// SetStreamDefaults is the product default-expansion used by Compile().
	// We do not call Compile() here: it opens the source connection.
	// After defaults merge, each stream must resolve an object — same
	// check as Compile(), without opening a connection.
	for _, name := range cfg.StreamsOrdered() {
		stream := cfg.Streams[name]
		if stream == nil {
			stream = &sling.ReplicationStreamConfig{}
			cfg.Streams[name] = stream
		}
		sling.SetStreamDefaults(name, stream, cfg)
		if strings.TrimSpace(stream.Object) == "" {
			return nil, fmt.Errorf("need to specify `object` for stream `%s`", name)
		}
	}
	return streamsToMap(cfg), nil
}

func streamsToMap(cfg sling.ReplicationConfig) map[string]any {
	out := map[string]any{
		"source":  cfg.Source,
		"target":  cfg.Target,
		"streams": map[string]any{},
	}
	if !cfg.Hooks.IsEmpty() {
		out["hooks"] = hookMapToAny(cfg.Hooks)
	}
	streams := map[string]any{}
	for _, name := range cfg.StreamsOrdered() {
		s := cfg.Streams[name]
		if s == nil {
			streams[name] = map[string]any{}
			continue
		}
		streams[name] = streamToMap(s)
	}
	out["streams"] = streams
	return out
}

func tasksToMap(cfg sling.ReplicationConfig) map[string]any {
	out := map[string]any{
		"source":  cfg.Source,
		"target":  cfg.Target,
		"streams": map[string]any{},
	}
	if !cfg.Hooks.IsEmpty() {
		out["hooks"] = hookMapToAny(cfg.Hooks)
	}
	streams := map[string]any{}
	for _, task := range cfg.Tasks {
		if task == nil {
			continue
		}
		sm := map[string]any{
			"mode":   string(task.Mode),
			"object": task.Target.Object,
		}
		if pk := task.Source.PrimaryKey(); len(pk) > 0 {
			sm["primary_key"] = pk
		}
		if task.Source.UpdateKey != "" {
			sm["update_key"] = task.Source.UpdateKey
		}
		if len(task.Source.Select) > 0 {
			sm["select"] = task.Source.Select
		}
		if task.Source.Query != "" {
			sm["sql"] = task.Source.Query
		}
		if task.Transforms != nil {
			sm["transforms"] = task.Transforms
		}
		if task.Target.Options != nil && len(task.Target.Options.TableKeys) > 0 {
			sm["target_options"] = map[string]any{"table_keys": task.Target.Options.TableKeys}
		}
		if task.Target.Options != nil && task.Target.Options.FileMaxRows != nil {
			to, _ := sm["target_options"].(map[string]any)
			if to == nil {
				to = map[string]any{}
			}
			to["file_max_rows"] = *task.Target.Options.FileMaxRows
			sm["target_options"] = to
		}
		streams[task.StreamName] = sm
	}
	out["streams"] = streams
	return out
}

func streamToMap(s *sling.ReplicationStreamConfig) map[string]any {
	sm := map[string]any{}
	if s.Mode != "" {
		sm["mode"] = string(s.Mode)
	}
	if s.Object != "" {
		sm["object"] = s.Object
	}
	if pk := s.PrimaryKey(); len(pk) > 0 {
		sm["primary_key"] = pk
	}
	if s.UpdateKey != "" {
		sm["update_key"] = s.UpdateKey
	}
	if len(s.Select) > 0 {
		sm["select"] = s.Select
	}
	if s.SQL != "" {
		sm["sql"] = s.SQL
	}
	if s.Where != "" {
		sm["where"] = s.Where
	}
	if s.Transforms != nil {
		sm["transforms"] = s.Transforms
	}
	if !s.Hooks.IsEmpty() {
		sm["hooks"] = hookMapToAny(s.Hooks)
	}
	if s.SourceOptions != nil && s.SourceOptions.Range != nil && *s.SourceOptions.Range != "" {
		sm["source_options"] = map[string]any{"range": *s.SourceOptions.Range}
	}
	if s.TargetOptions != nil {
		to := map[string]any{}
		if len(s.TargetOptions.TableKeys) > 0 {
			to["table_keys"] = s.TargetOptions.TableKeys
		}
		if s.TargetOptions.FileMaxRows != nil {
			to["file_max_rows"] = *s.TargetOptions.FileMaxRows
		}
		if len(to) > 0 {
			sm["target_options"] = to
		}
	}
	return sm
}

func hookMapToAny(h sling.HookMap) any {
	m := map[string]any{}
	if len(h.Start) > 0 {
		m["start"] = h.Start
	}
	if len(h.End) > 0 {
		m["end"] = h.End
	}
	if len(h.Pre) > 0 {
		m["pre"] = h.Pre
	}
	if len(h.Post) > 0 {
		m["post"] = h.Post
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

// CompareCompiled walks two compiled maps with the match policy.
func CompareCompiled(actual, expected map[string]any, policy MatchPolicy) []CompareResult {
	var out []CompareResult
	for _, glob := range policy.MustMatch {
		pairs := alignGlob(actual, expected, glob)
		if len(pairs) == 0 {
			out = append(out, CompareResult{
				Path:    glob,
				Pass:    false,
				Message: "must_match: no path matched on expected",
			})
			continue
		}
		for _, p := range pairs {
			ok, msg := valuesEqual(p.got, p.want, isSetKey(glob))
			out = append(out, CompareResult{
				Path:    p.path,
				Pass:    ok,
				Message: msg,
				Want:    p.want,
				Got:     p.got,
			})
		}
	}
	for _, glob := range policy.MustNotExist {
		hits := collectExisting(actual, glob)
		if len(hits) == 0 {
			out = append(out, CompareResult{Path: glob, Pass: true, Message: "absent"})
			continue
		}
		for _, h := range hits {
			out = append(out, CompareResult{
				Path:    h.path,
				Pass:    false,
				Message: "must_not_exist: path is present",
				Got:     h.val,
			})
		}
	}
	return out
}

// CompareCompiledFiles compiles both YAML files then applies the policy.
func CompareCompiledFiles(actualPath, expectedPath string, policy MatchPolicy) ([]CompareResult, error) {
	actual, err := CompileReplicationMap(actualPath)
	if err != nil {
		return nil, err
	}
	expected, err := CompileReplicationMap(expectedPath)
	if err != nil {
		return nil, err
	}
	return CompareCompiled(actual, expected, policy), nil
}

type aligned struct {
	path string
	got  any
	want any
}

type existing struct {
	path string
	segs []string
	val  any
}

func alignGlob(actual, expected map[string]any, glob string) []aligned {
	wantHits := walkGlob(expected, strings.Split(glob, "."), "")
	if len(wantHits) == 0 {
		return nil
	}
	// Align streams.* by name, then by object, then by singleton.
	if strings.HasPrefix(glob, "streams.") {
		return alignStreamHits(actual, wantHits, glob)
	}
	if strings.Contains(glob, "*") {
		return alignAnyGlob(actual, wantHits, glob)
	}
	var out []aligned
	for _, w := range wantHits {
		got, ok := valueAt(actual, strings.Split(w.path, "."))
		if !ok {
			out = append(out, aligned{path: w.path, want: w.val, got: nil})
			continue
		}
		out = append(out, aligned{path: w.path, want: w.val, got: got})
	}
	return out
}

func alignAnyGlob(actual map[string]any, wantHits []existing, glob string) []aligned {
	gotHits := walkGlob(actual, strings.Split(glob, "."), "")
	var out []aligned
	for _, w := range wantHits {
		got := any(nil)
		for _, g := range gotHits {
			ok, _ := valuesEqual(g.val, w.val, isSetKey(glob))
			if ok {
				got = g.val
				break
			}
		}
		if got == nil && len(gotHits) > 0 {
			got = gotHits[0].val
		}
		out = append(out, aligned{path: w.path, want: w.val, got: got})
	}
	return out
}

func alignStreamHits(actual map[string]any, wantHits []existing, glob string) []aligned {
	actStreams, _ := actual["streams"].(map[string]any)
	if actStreams == nil {
		actStreams = map[string]any{}
	}
	suffix := ""
	parts := strings.Split(glob, ".")
	if len(parts) > 2 {
		suffix = strings.Join(parts[2:], ".")
	}
	var out []aligned
	used := map[string]bool{}
	for _, w := range wantHits {
		// segs: ["streams", "<stream name>", ...fields]
		if len(w.segs) < 2 {
			out = append(out, aligned{path: w.path, want: w.val})
			continue
		}
		name := w.segs[1]
		if sm, ok := actStreams[name]; ok {
			got := valueFrom(sm, suffix)
			out = append(out, aligned{path: w.path, want: w.val, got: got})
			used[name] = true
			continue
		}
		// Glob name: actual `finance.*` covers expected `finance.invoices`.
		// Do not mark the glob used — one wildcard covers many streams.
		if partner := streamByGlobName(actStreams, name, used); partner != "" {
			got := valueFrom(actStreams[partner], suffix)
			out = append(out, aligned{path: w.path, want: w.val, got: got})
			continue
		}
		// Match by object when names differ (orders vs public.orders).
		if partner := streamByObject(actStreams, wantObject(w.val, name), used); partner != "" {
			got := valueFrom(actStreams[partner], suffix)
			out = append(out, aligned{path: w.path, want: w.val, got: got})
			used[partner] = true
			continue
		}
		// Singleton fallback.
		if len(actStreams) == 1 && len(wantHits) == 1 {
			for k, sm := range actStreams {
				got := valueFrom(sm, suffix)
				out = append(out, aligned{path: w.path, want: w.val, got: got})
				used[k] = true
				break
			}
			continue
		}
		// Same-value fallback: actual may use a glob name (finance.*) while
		// expected lists expanded streams. Compare the field only.
		if allEqualWants(wantHits) && len(actStreams) > 0 && allActualFieldEq(actStreams, suffix, w.val) {
			out = append(out, aligned{path: w.path, want: w.val, got: w.val})
			continue
		}
		out = append(out, aligned{path: w.path, want: w.val, got: nil})
	}
	return out
}

func allEqualWants(hits []existing) bool {
	if len(hits) == 0 {
		return false
	}
	for _, h := range hits[1:] {
		ok, _ := valuesEqual(h.val, hits[0].val, false)
		if !ok {
			return false
		}
	}
	return true
}

func allActualFieldEq(streams map[string]any, suffix string, want any) bool {
	if len(streams) == 0 {
		return false
	}
	for _, sm := range streams {
		got := valueFrom(sm, suffix)
		ok, _ := valuesEqual(got, want, false)
		if !ok {
			return false
		}
	}
	return true
}

func wantObject(val any, streamName string) string {
	if m, ok := val.(map[string]any); ok {
		if o, ok := m["object"].(string); ok && o != "" {
			return o
		}
	}
	return streamName
}

func streamByGlobName(streams map[string]any, wantName string, used map[string]bool) string {
	for name := range streams {
		if used[name] || !strings.Contains(name, "*") {
			continue
		}
		if globNameMatch(name, wantName) {
			return name
		}
	}
	return ""
}

func globNameMatch(pattern, name string) bool {
	if pattern == name {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	// Single-star suffix/prefix only (finance.*, *.orders).
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(name, strings.TrimPrefix(pattern, "*"))
	}
	parts := strings.SplitN(pattern, "*", 2)
	if len(parts) != 2 {
		return false
	}
	return strings.HasPrefix(name, parts[0]) && strings.HasSuffix(name, parts[1])
}

func streamByObject(streams map[string]any, object string, used map[string]bool) string {
	if object == "" {
		return ""
	}
	for name, sm := range streams {
		if used[name] {
			continue
		}
		if m, ok := sm.(map[string]any); ok {
			if o, _ := m["object"].(string); o == object || name == object {
				return name
			}
		}
		if name == object {
			return name
		}
	}
	return ""
}

func valueFrom(node any, suffix string) any {
	if suffix == "" {
		return node
	}
	m, ok := asMap(node)
	if !ok {
		return nil
	}
	v, _ := valueAt(m, strings.Split(suffix, "."))
	return v
}

func collectExisting(root map[string]any, glob string) []existing {
	hits := walkGlob(root, strings.Split(glob, "."), "")
	var out []existing
	for _, h := range hits {
		if isEmptyValue(h.val) {
			continue
		}
		out = append(out, h)
	}
	return out
}

func walkGlob(node any, segs []string, prefix string) []existing {
	return walkGlobSegs(node, segs, nil)
}

func walkGlobSegs(node any, segs, acc []string) []existing {
	if len(segs) == 0 {
		return []existing{{path: strings.Join(acc, "."), val: node, segs: append([]string{}, acc...)}}
	}
	seg := segs[0]
	rest := segs[1:]
	if seg == "*" {
		m, ok := asMap(node)
		if !ok {
			return nil
		}
		var out []existing
		for _, k := range sortedKeys(m) {
			out = append(out, walkGlobSegs(m[k], rest, append(acc, k))...)
		}
		return out
	}
	m, ok := asMap(node)
	if !ok {
		return nil
	}
	child, ok := m[seg]
	if !ok {
		return nil
	}
	return walkGlobSegs(child, rest, append(acc, seg))
}

func valueAt(m map[string]any, segs []string) (any, bool) {
	var cur any = m
	for _, s := range segs {
		mm, ok := asMap(cur)
		if !ok {
			return nil, false
		}
		v, ok := mm[s]
		if !ok {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

func asMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, vv := range m {
			out[cast.ToString(k)] = vv
		}
		return out, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Map {
		out := make(map[string]any, rv.Len())
		for _, k := range rv.MapKeys() {
			out[cast.ToString(k.Interface())] = rv.MapIndex(k).Interface()
		}
		return out, true
	}
	return nil, false
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// isSetKey: arrays compare as sets when the path ends in a key list.
func isSetKey(glob string) bool {
	last := glob
	if i := strings.LastIndex(glob, "."); i >= 0 {
		last = glob[i+1:]
	}
	switch last {
	case "primary_key", "select", "tags", "files":
		return true
	}
	return false
}

// equalIgnoringDefaults treats omitted GET and alias modes as equal.
// Only applies to scalar strings / nil — never to maps or slices.
func equalIgnoringDefaults(got, want any) bool {
	if isMapOrSlice(got) || isMapOrSlice(want) {
		return false
	}
	gs := strings.TrimSpace(strings.ToLower(cast.ToString(got)))
	ws := strings.TrimSpace(strings.ToLower(cast.ToString(want)))
	if gs == ws && gs != "" {
		return true
	}
	// request.method GET is the default; agents often omit it.
	if (got == nil || gs == "") && ws == "get" {
		return true
	}
	if (want == nil || ws == "") && gs == "get" {
		return true
	}
	// spec name: MOCK_API vs "Mock API"
	if gs != "" && ws != "" && strings.ReplaceAll(gs, " ", "_") == strings.ReplaceAll(ws, " ", "_") {
		return true
	}
	if (gs == "table" && ws == "full-refresh") || (gs == "full-refresh" && ws == "table") {
		return true
	}
	return false
}

func isMapOrSlice(v any) bool {
	if v == nil {
		return false
	}
	switch v.(type) {
	case map[string]any, []any, []string, map[any]any:
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Map || rv.Kind() == reflect.Slice
}

func valuesEqual(got, want any, asSet bool) (bool, string) {
	if equalIgnoringDefaults(got, want) {
		return true, "ok"
	}
	if asSet {
		gs := toStringSlice(got)
		ws := toStringSlice(want)
		if gs == nil && ws == nil {
			if equalAny(got, want) {
				return true, "ok"
			}
			return false, fmt.Sprintf("mismatch: want %v got %v", want, got)
		}
		if selectSetsEqual(gs, ws) {
			return true, "ok"
		}
		return false, fmt.Sprintf("set mismatch: want %v got %v", dropExcludeCols(ws), dropExcludeCols(gs))
	}
	if equalAny(got, want) {
		return true, "ok"
	}
	return false, fmt.Sprintf("mismatch: want %v got %v", want, got)
}

func dropExcludeCols(ss []string) []string {
	if ss == nil {
		return nil
	}
	var out []string
	for _, s := range ss {
		if strings.HasPrefix(s, "-") {
			continue
		}
		out = append(out, s)
	}
	return out
}

// selectSetsEqual treats a pure-exclusion select (["-address"]) as matching
// a positive pin that does not name the excluded column (e.34).
func selectSetsEqual(got, want []string) bool {
	gPos, gEx := splitSelect(got)
	wPos, wEx := splitSelect(want)
	if setEqual(gPos, wPos) {
		return true // extra -col exclusions are ignored (dropExcludeCols)
	}
	// Agent wrote only exclusions; pin listed the keep-list. Accept when
	// every excluded name is absent from the pin (the intention's "exclude X").
	if len(gPos) == 0 && len(gEx) > 0 && len(wPos) > 0 {
		for _, ex := range gEx {
			if containsFold(wPos, ex) {
				return false
			}
		}
		return true
	}
	if len(wPos) == 0 && len(wEx) > 0 && len(gPos) > 0 {
		for _, ex := range wEx {
			if containsFold(gPos, ex) {
				return false
			}
		}
		return true
	}
	return false
}

func splitSelect(ss []string) (pos, ex []string) {
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "-") {
			ex = append(ex, strings.TrimPrefix(s, "-"))
			continue
		}
		pos = append(pos, s)
	}
	return pos, ex
}

func containsFold(ss []string, want string) bool {
	for _, s := range ss {
		if strings.EqualFold(s, want) {
			return true
		}
	}
	return false
}

func toStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, x := range t {
			out = append(out, cast.ToString(x))
		}
		return out
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice {
		out := make([]string, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out = append(out, cast.ToString(rv.Index(i).Interface()))
		}
		return out
	}
	if s, ok := v.(string); ok && s != "" {
		return []string{s}
	}
	return nil
}

func setEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	am := map[string]int{}
	for _, x := range a {
		am[x]++
	}
	for _, x := range b {
		am[x]--
		if am[x] < 0 {
			return false
		}
	}
	for _, n := range am {
		if n != 0 {
			return false
		}
	}
	return true
}

func equalAny(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Numeric / bool looseness via stringify when types differ.
	if reflect.DeepEqual(normalize(a), normalize(b)) {
		return true
	}
	return stringify(a) == stringify(b)
}

func normalize(v any) any {
	switch t := v.(type) {
	case map[any]any:
		m, _ := asMap(t)
		out := map[string]any{}
		for k, vv := range m {
			out[k] = normalize(vv)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for k, vv := range t {
			out[k] = normalize(vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = normalize(x)
		}
		return out
	default:
		return v
	}
}

func stringify(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int, int64, int32, uint, uint64:
		return fmt.Sprintf("%d", t)
	case float64:
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, err := yaml.Marshal(v)
		if err != nil {
			return fmt.Sprint(v)
		}
		return strings.TrimSpace(string(b))
	}
}

func isEmptyValue(v any) bool {
	if v == nil {
		return true
	}
	switch t := v.(type) {
	case string:
		return t == ""
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	case []string:
		return len(t) == 0
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array:
		return rv.Len() == 0
	}
	return false
}

// YAMLMap loads a YAML file into a generic map.
func YAMLMap(path string) (map[string]any, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}
