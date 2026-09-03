package validate

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/flarco/g"
	"github.com/robfig/cron/v3"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/slingdata-io/sling-cli/core/sling/build"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// Options controls parse behavior.
type Options struct {
	Compile  bool
	Quiet    bool
	NDJSON   bool
	JSON     bool
	Detailed bool
}

// FileResult is one validated path.
type FileResult struct {
	Path     string   `json:"path,omitempty"`
	Kind     Kind     `json:"kind"`
	OK       bool     `json:"ok"`
	Compiled bool     `json:"compiled"`
	Parsed   any      `json:"parsed,omitempty"`
	Error    string   `json:"error,omitempty"`
	Warnings []string `json:"warnings,omitempty"`
}

// ParsePaths parses each path. Directories are walked. Parse errors
// stay in the result list so all files are reported.
func ParsePaths(paths []string, opts Options) []FileResult {
	var results []FileResult
	for _, p := range paths {
		results = append(results, parseOnePath(p, opts)...)
	}
	return results
}

// ParseFile parses one explicit file. Directories are rejected.
func ParseFile(path string, opts Options) FileResult {
	abs, err := filepath.Abs(path)
	if err != nil {
		return FileResult{Path: displayPath(path, path), OK: false, Error: err.Error()}
	}
	info, err := os.Stat(abs)
	if err != nil {
		return FileResult{Path: displayPath(path, abs), OK: false, Kind: KindUnknown, Error: err.Error()}
	}
	if info.IsDir() {
		return FileResult{Path: displayPath(path, abs), OK: false, Kind: KindUnknown, Error: "path is a directory"}
	}
	return parseExplicitFile(path, abs, opts)
}

func parseOnePath(userPath string, opts Options) []FileResult {
	abs, err := filepath.Abs(userPath)
	if err != nil {
		return []FileResult{{Path: userPath, OK: false, Kind: KindUnknown, Error: err.Error()}}
	}
	info, err := os.Stat(abs)
	if err != nil {
		return []FileResult{{Path: displayPath(userPath, abs), OK: false, Kind: KindUnknown, Error: err.Error()}}
	}
	if info.IsDir() {
		return walkDir(userPath, abs, opts)
	}
	return []FileResult{parseExplicitFile(userPath, abs, opts)}
}

func parseExplicitFile(userPath, abs string, opts Options) FileResult {
	body, err := os.ReadFile(abs)
	if err != nil {
		return FileResult{Path: displayPath(userPath, abs), OK: false, Kind: KindUnknown, Error: err.Error()}
	}
	kind := DetectFileKind(body, abs)
	return parseBody(displayPath(userPath, abs), abs, kind, body, opts)
}

func walkDir(userPath, absDir string, opts Options) []FileResult {
	files, err := g.ListDirRecursive(absDir)
	if err != nil {
		return []FileResult{{Path: displayPath(userPath, absDir), OK: false, Kind: KindUnknown, Error: err.Error()}}
	}

	var results []FileResult
	for _, file := range files {
		if file.IsDir {
			continue
		}
		name := strings.ToLower(file.Name)
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}

		body, err := os.ReadFile(file.FullPath)
		if err != nil {
			disp := walkDisplayPath(userPath, absDir, file.FullPath)
			results = append(results, FileResult{Path: disp, OK: false, Kind: KindUnknown, Error: err.Error()})
			continue
		}

		kind := DetectFileKind(body, file.FullPath)
		disp := walkDisplayPath(userPath, absDir, file.FullPath)
		if kind == KindUnknown && !matchesWalkPattern(file.FullPath) && !inCanonicalFolder(file.FullPath) {
			g.Debug("skipping unknown yaml in folder walk: %s", disp)
			continue
		}
		results = append(results, parseBody(disp, file.FullPath, kind, body, opts))
	}
	return results
}

func parseBody(display, abs string, kind Kind, body []byte, opts Options) FileResult {
	res := FileResult{Path: display, Kind: kind}
	if kind == KindUnknown {
		res.Error = "unknown kind"
		return res
	}

	if kind == KindProject {
		return parseProjectFile(res, abs, body, opts)
	}

	useCompile := opts.Compile && (kind == KindReplication || kind == KindPipeline || kind == KindBuild)
	var (
		parsed   any
		err      error
		warnings []string
	)
	if useCompile {
		parsed, warnings, err = parseCompile(kind, abs, body)
	} else {
		parsed, err = parseDTO(kind, body)
	}
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.OK = true
	res.Compiled = useCompile
	res.Parsed = parsed
	res.Warnings = append(res.Warnings, warnings...)
	if kind == KindPipeline {
		res.Warnings = append(res.Warnings, pipelineStepPathWarnings(abs, parsed)...)
	}
	if kind == KindReplication {
		res.Warnings = append(res.Warnings, unknownConnWarnings(parsed)...)
	}
	return res
}

func parseProjectFile(res FileResult, abs string, body []byte, opts Options) FileResult {
	parsed, warnings, errs := validateProject(abs, body, opts)
	res.Warnings = warnings
	if len(errs) > 0 {
		res.Error = strings.Join(errs, "; ")
		return res
	}
	res.OK = true
	res.Parsed = parsed
	return res
}

// 5-field cron, or a descriptor such as @daily / @every 1h.
var (
	cronSpecParser       = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronDescriptorParser = cron.NewParser(cron.Descriptor)
)

func validateProject(abs string, body []byte, opts Options) (parsed any, warnings, errs []string) {
	var raw struct {
		Name      string         `yaml:"name"`
		ProjectID string         `yaml:"project_id"`
		Jobs      map[string]any `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(body, &raw); err != nil {
		return nil, nil, []string{fmt.Sprintf("invalid project yaml: %v", err)}
	}

	dir := filepath.Dir(abs)
	for _, key := range sortedKeys(raw.Jobs) {
		jm, ok := asMap(raw.Jobs[key])
		if !ok {
			errs = append(errs, fmt.Sprintf("job %q must be a mapping", key))
			continue
		}
		file := strings.TrimSpace(cast.ToString(jm["file"]))
		if file == "" {
			errs = append(errs, fmt.Sprintf("job %q is missing required key 'file'", key))
			continue
		}
		if err := validateModeIn(jm, "job "+key); err != nil {
			errs = append(errs, err.Error())
		}

		schedules := toStringSlice(jm["schedules"])
		for _, sched := range schedules {
			if err := validateCron(sched); err != nil {
				errs = append(errs, fmt.Sprintf("job %q has an invalid schedule %q: %v", key, sched, err))
			}
		}
		if len(schedules) > 1 {
			warnings = append(warnings, fmt.Sprintf("job %q has %d schedules; only the first cron fires", key, len(schedules)))
		}

		absFile := file
		if !filepath.IsAbs(absFile) {
			absFile = filepath.Join(dir, file)
		}
		info, err := os.Stat(absFile)
		if err != nil {
			errs = append(errs, fmt.Sprintf("job %q file %s does not exist", key, file))
			continue
		}
		if info.IsDir() {
			errs = append(errs, fmt.Sprintf("job %q file %s is a directory", key, file))
			continue
		}
		fileBody, err := os.ReadFile(absFile)
		if err != nil {
			errs = append(errs, fmt.Sprintf("job %q file %s: %s", key, file, err.Error()))
			continue
		}
		kind := DetectFileKind(fileBody, absFile)
		if kind != KindReplication && kind != KindPipeline {
			errs = append(errs, fmt.Sprintf("job %q file %s is a %s; expected a replication or pipeline", key, file, kind))
			continue
		}
		jobRes := parseBody(file, absFile, kind, fileBody, opts)
		if !jobRes.OK {
			errs = append(errs, fmt.Sprintf("job %q file %s does not parse: %s", key, file, jobRes.Error))
			continue
		}
		if kind != KindReplication {
			continue
		}
		streams, ok := asMap(asMapOrEmpty(jobRes.Parsed)["streams"])
		if !ok {
			continue
		}
		for _, name := range toStringSlice(jm["streams"]) {
			if _, found := streams[name]; !found {
				warnings = append(warnings, fmt.Sprintf("job %q stream %q is not in %s", key, name, file))
			}
		}
	}

	dto := ProjectDTO{Name: raw.Name, ProjectID: raw.ProjectID, Jobs: raw.Jobs}
	return Redact(dtoToMap(dto)), warnings, errs
}

func validateCron(spec string) error {
	expr := strings.TrimSpace(spec)
	if expr == "" {
		return fmt.Errorf("schedule is empty")
	}
	if strings.HasPrefix(expr, "@every") {
		parts := strings.Split(expr, " ")
		if len(parts) == 2 && strings.HasSuffix(parts[1], "d") {
			if days := cast.ToInt(strings.TrimSuffix(parts[1], "d")); days > 0 {
				expr = fmt.Sprintf("@every %dh", days*24)
			}
		}
	}
	if _, err := cronSpecParser.Parse(expr); err == nil {
		return nil
	} else if _, err2 := cronDescriptorParser.Parse(expr); err2 == nil {
		return nil
	} else {
		return err
	}
}

func matchesWalkPattern(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	if isManifestName(filepath.Base(path)) || isBuildConfigName(filepath.Base(path)) || isEnvFileName(filepath.Base(path)) {
		return true
	}
	yaml := strings.HasSuffix(base, ".yaml") || strings.HasSuffix(base, ".yml")
	if !yaml {
		return false
	}
	return strings.HasPrefix(base, "replication") || strings.HasPrefix(base, "pipeline")
}

func inCanonicalFolder(path string) bool {
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		switch part {
		case "replications", "pipelines", "specs", "models":
			return true
		}
	}
	return false
}

func displayPath(userPath, abs string) string {
	if userPath == "" {
		return filepath.ToSlash(abs)
	}
	if filepath.IsAbs(userPath) {
		return filepath.ToSlash(abs)
	}
	return filepath.ToSlash(filepath.Clean(userPath))
}

func walkDisplayPath(userPath, absDir, absFile string) string {
	rel, err := filepath.Rel(absDir, absFile)
	if err != nil {
		return filepath.ToSlash(absFile)
	}
	if filepath.IsAbs(userPath) {
		return filepath.ToSlash(filepath.Join(absDir, rel))
	}
	return filepath.ToSlash(filepath.Join(filepath.Clean(userPath), rel))
}

func AnyFailed(results []FileResult) bool {
	for _, r := range results {
		if !r.OK {
			return true
		}
	}
	return false
}

func parseCompile(kind Kind, absPath string, body []byte) (any, []string, error) {
	env.LoadDotEnvSlingFrom(filepath.Dir(absPath))

	switch kind {
	case KindReplication:
		cfg, err := sling.LoadReplicationConfigFromFile(absPath)
		if err != nil {
			return nil, nil, err
		}
		if err = cfg.Compile(nil); err != nil {
			return nil, nil, err
		}
		return Redact(compiledReplication(cfg)), nil, nil
	case KindPipeline:
		pipeline, err := sling.LoadPipelineConfigFromFile(absPath)
		if err != nil {
			return nil, nil, err
		}
		if err := validatePipelineSteps(pipeline.Steps, ""); err != nil {
			return nil, nil, err
		}
		dto := PipelineDTO{Steps: pipeline.Steps, Env: pipeline.Env}
		return Redact(dtoToMap(dto)), nil, nil
	case KindBuild:
		projDir := absPath
		if isBuildConfigName(filepath.Base(absPath)) {
			projDir = filepath.Dir(absPath)
		}
		project, err := build.LoadProject(projDir, build.BuildOptions{SkipUnresolvedCheck: true})
		if err != nil {
			return nil, nil, err
		}
		var warnings []string
		for _, name := range project.UnresolvedConfigVars() {
			warnings = append(warnings, fmt.Sprintf("sling_build.yml uses ${%s} but %s is not set", name, name))
		}
		return Redact(compiledBuild(project)), warnings, nil
	default:
		parsed, err := parseDTO(kind, body)
		return parsed, nil, err
	}
}

func compiledReplication(cfg sling.ReplicationConfig) map[string]any {
	streams := map[string]any{}
	b, err := json.Marshal(cfg.Streams)
	if err == nil {
		_ = json.Unmarshal(b, &streams)
	}
	return dtoToMap(ReplicationDTO{
		Source:   cfg.Source,
		Target:   cfg.Target,
		Hooks:    cfg.Hooks,
		Defaults: cfg.Defaults,
		Streams:  streams,
		Env:      cfg.Env,
	})
}

func compiledBuild(project *build.BuildProject) map[string]any {
	models := make([]string, 0, len(project.Models))
	for name := range project.Models {
		models = append(models, name)
	}
	sort.Strings(models)
	seeds := make([]string, 0, len(project.Seeds))
	for name := range project.Seeds {
		seeds = append(seeds, name)
	}
	sort.Strings(seeds)

	out := map[string]any{
		"dir":    project.Dir,
		"models": models,
		"seeds":  seeds,
		"mode":   project.Mode,
	}
	if project.Config != nil {
		out["target"] = project.Config.Target
		out["defaults"] = project.Config.Defaults
		out["vars"] = project.Config.Vars
	}
	return out
}
