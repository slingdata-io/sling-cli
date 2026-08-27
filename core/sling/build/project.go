package build

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// BuildProject represents a sling build project discovered from a directory.
type BuildProject struct {
	Dir            string
	Config         *BuildConfig            // from sling_build.yml (nil if missing)
	Models         map[string]*Model       // keyed by unique model name
	Seeds          map[string]*Seed        // keyed by unique seed name
	Macros         []*MacroFile            // collected .macros.sql files
	Mode           string                  // "dev" or "prod"
	SchemaOverride string                  // dev mode schema
	DefaultSchema  string                  // default schema for root-level files (default: "public")
	ChildConfigs   map[string]*BuildConfig // child sling_build.yml configs keyed by relative dir
	SubProjects    []*BuildProject         // independent build projects (when no root yml)
	Recursive      bool                    // CLI --recursive: keep immediate child projects
}

// Model represents a SQL model file in the project.
type Model struct {
	Name              string      // e.g., "dim_customers"
	FilePath          string      // absolute path
	RelPath           string      // relative path from project root
	Schema            string      // derived from folder or override
	Prefix            string      // underscore-joined nested folder names
	FullTableName     string      // schema.prefix_name (current mode)
	ProdFullTableName string      // schema.prefix_name (always prod-mode, for SQL matching)
	RawSQL            string      // raw file content (frontmatter stripped)
	CompiledSQL       string      // after Jinja rendering
	PreStatements     []string    // SQL statements before the model query (from multi-statement splitting)
	PostStatements    []string    // SQL statements after the model query (from multi-statement splitting)
	Config            ModelConfig // from YAML frontmatter or config() block
	HasFrontmatter    bool        // true if config was set via YAML frontmatter (config() becomes no-op)
	Style             Style       // detected incremental pattern: StyleDbt or StyleSling (populated at load)
	Refs              []string    // ref() dependencies
	Sources           []string    // src() references
	DependsOn         []string    // all DAG dependencies (refs + bare refs + auto-detected)
	startHooks        sling.Hooks // parsed start hooks (populated at execution time)
	endHooks          sling.Hooks // parsed end hooks (populated at execution time)
}

// ModelConfig holds configuration extracted from the config() block in a SQL model.
type ModelConfig struct {
	Mode          string        `yaml:"mode,omitempty"`
	Materialized  string        `yaml:"materialized,omitempty"` // dbt alias for mode
	UniqueKey     any           `yaml:"unique_key,omitempty"`   // string or []string
	MergeStrategy string        `yaml:"merge_strategy,omitempty"`
	UpdateKey     string        `yaml:"update_key,omitempty"`
	Tags          []string      `yaml:"tags,omitempty"`
	Hooks         sling.HookMap `yaml:"hooks,omitempty"`
	PreHook       string        `yaml:"pre_hook,omitempty"`  // deprecated: kept for validation only
	PostHook      string        `yaml:"post_hook,omitempty"` // deprecated: kept for validation only
	Schema        string        `yaml:"schema,omitempty"`
	Enabled       *bool         `yaml:"enabled,omitempty"`
	Engine        string        `yaml:"engine,omitempty"`
	Range         *RangeConfig  `yaml:"range,omitempty"`
	DropCascade   *bool         `yaml:"drop_cascade,omitempty"` // default false; CASCADE on DROP when true
	Rewrite       *bool         `yaml:"rewrite,omitempty"`      // default true; set false to skip bare-name rewrite
	Tests         []any         `yaml:"tests,omitempty"`        // declarative data tests
}

// RangeConfig holds the range block from model front-matter. It drives the
// unified incremental / lookback / paged-backfill behavior (sling style only).
type RangeConfig struct {
	Start    string `yaml:"start,omitempty"`    // literal value; parsed lazily at execution time
	Advance  string `yaml:"advance,omitempty"`  // duration (5m, 5h, 5d, 5w, 1mo, 1y) — per-run forward advance
	Lookback string `yaml:"lookback,omitempty"` // duration
}

// HasAdvance returns true if paged-advance mode is enabled.
func (r *RangeConfig) HasAdvance() bool { return r != nil && r.Advance != "" }

// HasLookback returns true if a lookback window is configured.
func (r *RangeConfig) HasLookback() bool { return r != nil && r.Lookback != "" }

// durationRegex matches a positive integer followed by a unit: ms, s, m, h, d, w, mo, y.
// Longer units must come first in the alternation so "mo" wins over "m".
var durationRegex = regexp.MustCompile(`^(\d+)(ms|mo|s|m|h|d|w|y)$`)

// parseBuildDuration parses a duration literal as used in range.advance / range.lookback.
// Supported units: ms, s, m, h, d (24h), w (7d), mo (30d), y (365d).
// The return is approximate for calendar units — phase 4 will re-parse if calendar
// accuracy is needed (e.g., month-by-month stepping via time.AddDate).
func parseBuildDuration(s string) (time.Duration, error) {
	m := durationRegex.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, g.Error("invalid duration '%s': expected <int><unit> with unit in ms,s,m,h,d,w,mo,y", s)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, g.Error(err, "invalid duration '%s'", s)
	}
	unit := m[2]
	switch unit {
	case "ms":
		return time.Duration(n) * time.Millisecond, nil
	case "s":
		return time.Duration(n) * time.Second, nil
	case "m":
		return time.Duration(n) * time.Minute, nil
	case "h":
		return time.Duration(n) * time.Hour, nil
	case "d":
		return time.Duration(n) * 24 * time.Hour, nil
	case "w":
		return time.Duration(n) * 7 * 24 * time.Hour, nil
	case "mo":
		return time.Duration(n) * 30 * 24 * time.Hour, nil
	case "y":
		return time.Duration(n) * 365 * 24 * time.Hour, nil
	}
	return 0, g.Error("invalid duration unit '%s'", unit)
}

// validateModel runs load-time validation on a Model's configuration.
// It is called from addModel() after frontmatter parse and style detection.
func validateModel(m *Model) error {
	if err := applyModeAliases(&m.Config, m.Name); err != nil {
		return err
	}

	// mode: incremental requires update_key
	if m.Config.Mode == "incremental" && m.Config.UpdateKey == "" {
		return g.Error("model '%s': mode 'incremental' requires update_key", m.Name)
	}

	r := m.Config.Range
	if r == nil {
		return nil
	}

	if r.Start != "" && r.Advance == "" {
		return g.Error("model '%s': range.start requires range.advance", m.Name)
	}
	if (r.Advance != "" || r.Lookback != "") && m.Config.Mode != "incremental" {
		return g.Error("model '%s': range.* requires mode: incremental", m.Name)
	}
	if r.Advance != "" && m.Config.UpdateKey == "" {
		return g.Error("model '%s': range.advance requires update_key", m.Name)
	}
	if r.Advance != "" {
		if _, err := parseBuildDuration(r.Advance); err != nil {
			return g.Error(err, "model '%s': invalid range.advance", m.Name)
		}
	}
	if r.Lookback != "" {
		if _, err := parseBuildDuration(r.Lookback); err != nil {
			return g.Error(err, "model '%s': invalid range.lookback", m.Name)
		}
	}

	// range.* features require owning the WHERE clause (sling style)
	if (r.Advance != "" || r.Lookback != "") && m.Style == StyleDbt {
		return g.Error("model '%s': range.* requires {incremental_where_cond} (sling style); is_incremental() is not compatible with range.*", m.Name)
	}

	return nil
}

// Seed represents a seed file (CSV, JSON, Parquet) in the project.
type Seed struct {
	Name              string // e.g., "country_codes"
	FilePath          string // absolute path
	RelPath           string // relative path from project root
	Schema            string
	Prefix            string
	FullTableName     string // schema.prefix_name (current mode)
	ProdFullTableName string // schema.prefix_name (always prod-mode, for SQL matching)
	Format            string // csv, json, parquet
}

// BuildConfig represents the contents of sling_build.yml.
type BuildConfig struct {
	Target     string         `yaml:"target"`
	Dev        *DevConfig     `yaml:"dev,omitempty"`
	DbtProject any            `yaml:"dbt_project,omitempty"`
	Vars       map[string]any `yaml:"vars,omitempty"`
	Defaults   BuildDefaults  `yaml:"defaults,omitempty"`
}

// DevConfig holds dev-mode settings in sling_build.yml.
// When present, dev mode is the default (override with --prod).
type DevConfig struct {
	Target string `yaml:"target,omitempty"` // optional, falls back to top-level target
	Schema string `yaml:"schema"`           // mandatory for dev mode
}

// DbtProjectConfig holds dbt project compatibility settings.
type DbtProjectConfig struct {
	ModelsPath string `yaml:"models_path,omitempty"` // default: "models"
	SeedsPath  string `yaml:"seeds_path,omitempty"`  // default: "seeds"
}

// BuildDefaults holds default settings for models.
type BuildDefaults struct {
	Mode          string        `yaml:"mode,omitempty"`
	Schema        string        `yaml:"schema,omitempty"`
	Tags          []string      `yaml:"tags,omitempty"` // additive across nesting
	UniqueKey     any           `yaml:"unique_key,omitempty"`
	UpdateKey     string        `yaml:"update_key,omitempty"`
	MergeStrategy string        `yaml:"merge_strategy,omitempty"`
	Enabled       *bool         `yaml:"enabled,omitempty"`
	Hooks         sling.HookMap `yaml:"hooks,omitempty"` // additive across nesting
	DropCascade   *bool         `yaml:"drop_cascade,omitempty"`
}

// BuildOptions holds CLI-provided overrides.
type BuildOptions struct {
	Target      string
	Schema      string
	Prod        bool
	Vars        map[string]any
	FullRefresh bool
	Select      []string
	Exclude     []string
	Compile     bool
	Threads     int
	FailFast    bool
	List        bool
	NoSeeds     bool
	Range       *string // CLI --range: "start,end[,step]"
	Recursive   bool    // CLI --recursive/-R: discover sling_build.yml in immediate subdirectories
	Test        bool    // CLI --test: run data tests only (no materialization)
	JSON        bool    // CLI --json: machine-readable compile/list output
}

// DefaultThreads is the default parallelism for model execution.
const DefaultThreads = 4

// ValidModes are the recognized materialization modes.
var ValidModes = map[string]bool{
	"full-refresh": true,
	"view":         true,
	"truncate":     true,
	"incremental":  true,
	"append":       true,
	"snapshot":     true, // deprecated alias for append
}

// normalizeMode maps aliases and deprecated names to canonical modes.
// Returns the canonical mode and an optional warning message.
func normalizeMode(mode string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "":
		return "", ""
	case "snapshot":
		return "append", "'snapshot' mode is deprecated and means append-only insert; use 'append'. 'snapshot' will mean SCD2 in a future release"
	case "table":
		return "full-refresh", ""
	case "ephemeral":
		return "ephemeral", "ephemeral models are not supported in sling build; materialize as a view or table instead"
	default:
		return strings.ToLower(strings.TrimSpace(mode)), ""
	}
}

// mapMaterialized converts a dbt materialized= value to a sling mode.
func mapMaterialized(materialized string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(materialized)) {
	case "table":
		return "full-refresh", nil
	case "view":
		return "view", nil
	case "incremental":
		return "incremental", nil
	case "ephemeral":
		return "", g.Error("materialized='ephemeral' is not supported in sling build; use view or table")
	case "materialized_view", "materializedview":
		return "", g.Error("materialized='%s' is not supported in sling build; use view or table", materialized)
	case "":
		return "", nil
	default:
		return "", g.Error("unknown materialized value '%s'; expected table, view, or incremental", materialized)
	}
}

// applyModeAliases resolves mode/materialized aliases on a ModelConfig.
// Prefer explicit mode over materialized. Warns on deprecated names.
func applyModeAliases(cfg *ModelConfig, modelName string) error {
	if cfg.Materialized != "" {
		mapped, err := mapMaterialized(cfg.Materialized)
		if err != nil {
			return g.Error(err, "model '%s'", modelName)
		}
		// Explicit mode wins over materialized
		if cfg.Mode == "" {
			cfg.Mode = mapped
		}
		cfg.Materialized = "" // consumed
	}
	if cfg.Mode != "" {
		canonical, warn := normalizeMode(cfg.Mode)
		if warn != "" {
			g.Warn("model '%s': %s", modelName, warn)
		}
		if canonical == "ephemeral" {
			return g.Error("model '%s': ephemeral models are not supported; use view or table", modelName)
		}
		cfg.Mode = canonical
	}
	return nil
}

// ConfigFileName is the standard config file name.
const ConfigFileName = "sling_build.yml"

// seedExtensions are recognized seed file extensions.
var seedExtensions = map[string]string{
	".csv":     "csv",
	".json":    "json",
	".parquet": "parquet",
}

// LoadProject loads a build project from the given directory.
func LoadProject(dir string, opts ...BuildOptions) (*BuildProject, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, g.Error(err, "could not resolve project directory")
	}

	info, err := os.Stat(absDir)
	if err != nil {
		return nil, g.Error(err, "could not access project directory: %s", absDir)
	}
	if !info.IsDir() {
		return nil, g.Error("path is not a directory: %s", absDir)
	}

	var cliOpts BuildOptions
	if len(opts) > 0 {
		cliOpts = opts[0]
	}

	project := &BuildProject{
		Dir:           absDir,
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		DefaultSchema: "public",
		ChildConfigs:  make(map[string]*BuildConfig),
		Recursive:     cliOpts.Recursive,
	}

	// Load root config if present
	rootConfigPath := filepath.Join(absDir, ConfigFileName)
	if _, err := os.Stat(rootConfigPath); err == nil {
		cfg, err := loadConfig(rootConfigPath)
		if err != nil {
			return nil, g.Error(err, "could not load %s", rootConfigPath)
		}
		project.Config = cfg
	}

	// Discover nested sling_build.yml files (only when --recursive is set)
	if cliOpts.Recursive {
		if err := discoverNestedConfigs(project); err != nil {
			return nil, g.Error(err, "could not discover nested configs")
		}
	}

	// If no root config but children have configs, these are independent builds
	if project.Config == nil && len(project.ChildConfigs) > 0 {
		return loadIndependentBuilds(project, cliOpts)
	}

	// Apply CLI overrides
	applyCliOverrides(project, cliOpts)

	// Discover files
	if err := discoverFiles(project); err != nil {
		return nil, g.Error(err, "could not discover files")
	}

	// Warn about macro name shadows
	warnMacroShadows(project)

	// Validate unique names
	if err := validateUniqueNames(project); err != nil {
		return nil, err
	}

	return project, nil
}

// loadConfig reads and parses a sling_build.yml file.
func loadConfig(path string) (*BuildConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, g.Error(err, "could not read config file")
	}

	cfg := &BuildConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, g.Error(err, "could not parse config file")
	}

	return cfg, nil
}

// mergeHookMaps produces a HookMap whose slices are the ordered concatenation
// of parent then child slices for each stage. All six stages are merged so
// this helper stays useful if more stages get wired into build execution.
func mergeHookMaps(parent, child sling.HookMap) sling.HookMap {
	appendAny := func(a, b []any) []any {
		if len(a) == 0 {
			return b
		}
		if len(b) == 0 {
			return a
		}
		out := make([]any, 0, len(a)+len(b))
		out = append(out, a...)
		out = append(out, b...)
		return out
	}
	return sling.HookMap{
		Start:     appendAny(parent.Start, child.Start),
		End:       appendAny(parent.End, child.End),
		Pre:       appendAny(parent.Pre, child.Pre),
		Post:      appendAny(parent.Post, child.Post),
		PreMerge:  appendAny(parent.PreMerge, child.PreMerge),
		PostMerge: appendAny(parent.PostMerge, child.PostMerge),
	}
}

// mergeConfigs merges a child config into a parent, returning a new merged config.
// vars are deep-merged, defaults are shallow-merged, all other fields replaced by child.
func mergeConfigs(parent, child *BuildConfig) *BuildConfig {
	if parent == nil {
		return child
	}
	if child == nil {
		return parent
	}

	merged := &BuildConfig{
		Target:     parent.Target,
		Dev:        parent.Dev,
		DbtProject: parent.DbtProject,
		Defaults:   parent.Defaults,
	}

	// Deep merge vars
	merged.Vars = make(map[string]any)
	for k, v := range parent.Vars {
		merged.Vars[k] = v
	}
	for k, v := range child.Vars {
		merged.Vars[k] = v
	}

	// Child overrides non-empty fields
	if child.Target != "" {
		merged.Target = child.Target
	}
	if child.Dev != nil {
		merged.Dev = child.Dev
	}
	if child.DbtProject != nil {
		merged.DbtProject = child.DbtProject
	}

	// Field-by-field defaults merge.
	// Scalars: child replaces if set. Tags: union+dedupe. Hooks: append (parent first).
	if child.Defaults.Mode != "" {
		merged.Defaults.Mode = child.Defaults.Mode
	}
	if child.Defaults.Schema != "" {
		merged.Defaults.Schema = child.Defaults.Schema
	}
	if child.Defaults.UniqueKey != nil {
		merged.Defaults.UniqueKey = child.Defaults.UniqueKey
	}
	if child.Defaults.UpdateKey != "" {
		merged.Defaults.UpdateKey = child.Defaults.UpdateKey
	}
	if child.Defaults.MergeStrategy != "" {
		merged.Defaults.MergeStrategy = child.Defaults.MergeStrategy
	}
	if child.Defaults.Enabled != nil {
		merged.Defaults.Enabled = child.Defaults.Enabled
	}
	if child.Defaults.DropCascade != nil {
		merged.Defaults.DropCascade = child.Defaults.DropCascade
	}
	if len(parent.Defaults.Tags)+len(child.Defaults.Tags) > 0 {
		merged.Defaults.Tags = lo.Uniq(append(append([]string(nil), parent.Defaults.Tags...), child.Defaults.Tags...))
	}
	merged.Defaults.Hooks = mergeHookMaps(parent.Defaults.Hooks, child.Defaults.Hooks)

	return merged
}

// discoverNestedConfigs finds child sling_build.yml files in subdirectories.
func discoverNestedConfigs(project *BuildProject) error {
	entries, err := os.ReadDir(project.Dir)
	if err != nil {
		return g.Error(err, "could not read directory")
	}

	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}

		childDir := filepath.Join(project.Dir, entry.Name())
		childConfigPath := filepath.Join(childDir, ConfigFileName)

		if _, err := os.Stat(childConfigPath); err == nil {
			cfg, err := loadConfig(childConfigPath)
			if err != nil {
				return g.Error(err, "could not load %s", childConfigPath)
			}
			project.ChildConfigs[entry.Name()] = cfg
		}
	}

	return nil
}

// loadIndependentBuilds creates sub-projects when no root yml exists but children do.
func loadIndependentBuilds(project *BuildProject, cliOpts BuildOptions) (*BuildProject, error) {
	for childDir, childCfg := range project.ChildConfigs {
		subDir := filepath.Join(project.Dir, childDir)
		subProject := &BuildProject{
			Dir:           subDir,
			Config:        childCfg,
			Models:        make(map[string]*Model),
			Seeds:         make(map[string]*Seed),
			DefaultSchema: "public",
			ChildConfigs:  make(map[string]*BuildConfig),
		}

		applyCliOverrides(subProject, cliOpts)

		if err := discoverFiles(subProject); err != nil {
			return nil, g.Error(err, "could not discover files in %s", subDir)
		}

		warnMacroShadows(subProject)

		if err := validateUniqueNames(subProject); err != nil {
			return nil, err
		}

		project.SubProjects = append(project.SubProjects, subProject)
	}

	return project, nil
}

// applyCliOverrides applies CLI flags over project config.
func applyCliOverrides(project *BuildProject, opts BuildOptions) {
	// Determine mode from config: dev block present → dev, otherwise prod
	project.Mode = "prod"
	if project.Config != nil && project.Config.Dev != nil {
		project.Mode = "dev"
		project.SchemaOverride = project.Config.Dev.Schema
	}

	// CLI overrides
	if opts.Prod {
		project.Mode = "prod"
		project.SchemaOverride = ""
	}
	if opts.Schema != "" {
		project.Mode = "dev"
		project.SchemaOverride = opts.Schema
	}

	// Resolve target: in dev mode, use dev.target if set
	if project.Config != nil && project.Mode == "dev" && project.Config.Dev != nil && project.Config.Dev.Target != "" {
		project.Config.Target = project.Config.Dev.Target
	}

	// --target overrides everything
	if opts.Target != "" && project.Config != nil {
		project.Config.Target = opts.Target
	} else if opts.Target != "" && project.Config == nil {
		project.Config = &BuildConfig{Target: opts.Target}
	}

	// Merge vars
	if project.Config != nil && len(opts.Vars) > 0 {
		if project.Config.Vars == nil {
			project.Config.Vars = make(map[string]any)
		}
		for k, v := range opts.Vars {
			project.Config.Vars[k] = v
		}
	}
}

// getDbtProjectConfig parses the dbt_project field which can be bool or DbtProjectConfig.
func getDbtProjectConfig(cfg *BuildConfig) *DbtProjectConfig {
	if cfg == nil || cfg.DbtProject == nil {
		return nil
	}

	switch v := cfg.DbtProject.(type) {
	case bool:
		if v {
			return &DbtProjectConfig{
				ModelsPath: "models",
				SeedsPath:  "seeds",
			}
		}
		return nil
	case map[string]any:
		dbtCfg := &DbtProjectConfig{
			ModelsPath: "models",
			SeedsPath:  "seeds",
		}
		if mp, ok := v["models_path"]; ok {
			dbtCfg.ModelsPath = cast.ToString(mp)
		}
		if sp, ok := v["seeds_path"]; ok {
			dbtCfg.SeedsPath = cast.ToString(sp)
		}
		return dbtCfg
	}

	return nil
}

// discoverFiles walks the project directory and populates Models and Seeds.
func discoverFiles(project *BuildProject) error {
	dbtCfg := getDbtProjectConfig(project.Config)

	if dbtCfg != nil {
		// dbt_project mode: scan models_path for .sql, seeds_path for seed files
		modelsDir := filepath.Join(project.Dir, dbtCfg.ModelsPath)
		seedsDir := filepath.Join(project.Dir, dbtCfg.SeedsPath)

		if err := walkForModels(project, modelsDir, modelsDir); err != nil {
			return err
		}
		if err := walkForSeeds(project, seedsDir, seedsDir); err != nil {
			return err
		}
	} else {
		// Flat mode: walk project root, classify by extension
		if err := walkFlat(project); err != nil {
			return err
		}
	}

	return nil
}

// skipNestedBuildDir reports whether dir is a nested project that parent
// discovery must ignore. Immediate children stay in the walk when -R is set
// so ChildConfigs still apply. Deeper nested projects and any nested project
// without -R are skipped (e.31 leftover probe/ dirs).
func skipNestedBuildDir(project *BuildProject, dir string) bool {
	if project == nil || dir == "" || dir == project.Dir {
		return false
	}
	if _, err := os.Stat(filepath.Join(dir, ConfigFileName)); err != nil {
		return false
	}
	if !project.Recursive {
		return true
	}
	rel, err := filepath.Rel(project.Dir, dir)
	if err != nil || rel == "." {
		return false
	}
	if !strings.Contains(rel, string(os.PathSeparator)) {
		return false
	}
	return true
}

// walkFlat discovers models and seeds in flat directory structure.
func walkFlat(project *BuildProject) error {
	return filepath.Walk(project.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip hidden files/dirs
		if strings.HasPrefix(info.Name(), ".") {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip directories (we'll process their contents)
		if info.IsDir() {
			if skipNestedBuildDir(project, path) {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip config files
		if info.Name() == ConfigFileName {
			return nil
		}

		relPath, err := filepath.Rel(project.Dir, path)
		if err != nil {
			return g.Error(err, "could not get relative path")
		}

		ext := strings.ToLower(filepath.Ext(info.Name()))

		// Collect .macros.sql files
		if strings.HasSuffix(strings.ToLower(info.Name()), ".macros.sql") {
			relDir := filepath.ToSlash(filepath.Dir(relPath))
			return collectMacro(project, path, relDir)
		}

		if ext == ".sql" {
			return addModel(project, path, relPath)
		}

		if format, ok := seedExtensions[ext]; ok {
			return addSeed(project, path, relPath, format)
		}

		return nil
	})
}

// walkForModels discovers .sql model files under the given root.
func walkForModels(project *BuildProject, walkRoot, baseDir string) error {
	if _, err := os.Stat(walkRoot); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(walkRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			if skipNestedBuildDir(project, path) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(info.Name(), ".") || info.Name() == ConfigFileName {
			return nil
		}

		if strings.HasSuffix(strings.ToLower(info.Name()), ".macros.sql") {
			relPath, err := filepath.Rel(baseDir, path)
			if err != nil {
				return g.Error(err, "could not get relative path for macro")
			}
			relDir := filepath.ToSlash(filepath.Dir(relPath))
			return collectMacro(project, path, relDir)
		}

		ext := strings.ToLower(filepath.Ext(info.Name()))
		if ext == ".sql" {
			relPath, err := filepath.Rel(baseDir, path)
			if err != nil {
				return g.Error(err, "could not get relative path")
			}
			return addModel(project, path, relPath)
		}

		return nil
	})
}

// walkForSeeds discovers seed files under the given root.
func walkForSeeds(project *BuildProject, walkRoot, baseDir string) error {
	if _, err := os.Stat(walkRoot); os.IsNotExist(err) {
		return nil
	}

	return filepath.Walk(walkRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			if skipNestedBuildDir(project, path) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(info.Name(), ".") || info.Name() == ConfigFileName {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(info.Name()))
		if format, ok := seedExtensions[ext]; ok {
			relPath, err := filepath.Rel(baseDir, path)
			if err != nil {
				return g.Error(err, "could not get relative path")
			}
			return addSeed(project, path, relPath, format)
		}

		return nil
	})
}

// parseYAMLFrontmatter extracts YAML frontmatter from the leading comment of a
// SQL model file. The comment may use any of these styles:
//
//	/** name: my graph **/                       (doc-block, plain YAML)
//	/* {"name": "my graph"} */                   (block comment, JSON/flow)
//	-- {name: my graph}                          (single-line, flow)
//	-- { \n --   name: my graph \n -- }          (multi-line line-comment, flow)
//
// For the /** ... **/ style any valid YAML mapping is accepted (canonical form).
// For other styles, the comment must contain a YAML/JSON mapping starting with
// '{' so plain prose comments aren't misread as frontmatter (e.g. a "-- Pre-
// statement: setup" line that incidentally parses as a YAML map).
//
// On success the YAML is parsed into ModelConfig and the SQL after the comment
// is returned. If no frontmatter is detected the original SQL is returned
// unchanged.
func parseYAMLFrontmatter(sql string) (config ModelConfig, remainingSQL string, hasFrontmatter bool, err error) {
	trimmed := strings.TrimLeft(sql, " \t\r\n")
	if trimmed == "" {
		return ModelConfig{}, sql, false, nil
	}

	var yamlContent string
	var afterComment string
	// requireBraceObject is true for comment styles that don't have a strong
	// frontmatter signal (-- and /* */). Those need explicit '{' to be treated
	// as frontmatter, otherwise they're just regular comments.
	var requireBraceObject bool
	var detected bool

	switch {
	case strings.HasPrefix(trimmed, "/**"):
		// /** ... **/ doc-block: canonical, plain YAML allowed.
		closingIdx := strings.Index(trimmed[3:], "**/")
		if closingIdx < 0 {
			return ModelConfig{}, sql, false, nil
		}
		yamlContent = trimmed[3 : 3+closingIdx]
		afterComment = trimmed[3+closingIdx+3:]
		detected = true
		requireBraceObject = false

	case strings.HasPrefix(trimmed, "/*"):
		// /* ... */ block comment: must contain a {} object to be frontmatter.
		closingIdx := strings.Index(trimmed[2:], "*/")
		if closingIdx < 0 {
			return ModelConfig{}, sql, false, nil
		}
		yamlContent = trimmed[2 : 2+closingIdx]
		afterComment = trimmed[2+closingIdx+2:]
		detected = true
		requireBraceObject = true

	case strings.HasPrefix(trimmed, "--"):
		// Consecutive `-- ...` lines: must contain a {} object to be frontmatter.
		lines := strings.Split(trimmed, "\n")
		var contentLines []string
		consumed := 0
		for i, line := range lines {
			ltrim := strings.TrimLeft(line, " \t")
			if !strings.HasPrefix(ltrim, "--") {
				consumed = i
				break
			}
			body := strings.TrimPrefix(ltrim, "--")
			body = strings.TrimPrefix(body, " ")
			contentLines = append(contentLines, body)
			consumed = i + 1
		}
		yamlContent = strings.Join(contentLines, "\n")
		afterComment = strings.Join(lines[consumed:], "\n")
		detected = true
		requireBraceObject = true
	}

	if !detected {
		return ModelConfig{}, sql, false, nil
	}

	contentStripped := strings.TrimSpace(yamlContent)
	if contentStripped == "" {
		// Empty /** **/ → frontmatter present but empty config (preserves
		// existing behavior). Empty /* */ or -- block → not frontmatter.
		if requireBraceObject {
			return ModelConfig{}, sql, false, nil
		}
		return ModelConfig{}, stripLeadingNewline(afterComment), true, nil
	}

	if requireBraceObject && !strings.HasPrefix(contentStripped, "{") {
		// Looks like a regular comment — pass through unchanged.
		return ModelConfig{}, sql, false, nil
	}

	// Validate that the content is actually a YAML mapping (not a scalar or
	// list). For required-brace styles a parse failure means "regular comment",
	// not frontmatter.
	var asMap map[string]any
	if mapErr := yaml.Unmarshal([]byte(yamlContent), &asMap); mapErr != nil {
		if requireBraceObject {
			return ModelConfig{}, sql, false, nil
		}
		return ModelConfig{}, sql, false, g.Error(mapErr, "could not parse YAML frontmatter")
	}
	if asMap == nil {
		// Scalar/null — treat as regular comment for require-brace styles, or
		// empty config for /** **/ style.
		if requireBraceObject {
			return ModelConfig{}, sql, false, nil
		}
		return ModelConfig{}, stripLeadingNewline(afterComment), true, nil
	}

	cfg := ModelConfig{}
	if err := yaml.Unmarshal([]byte(yamlContent), &cfg); err != nil {
		return ModelConfig{}, sql, false, g.Error(err, "could not parse YAML frontmatter")
	}

	if cfg.PreHook != "" || cfg.PostHook != "" {
		return ModelConfig{}, sql, false, g.Error("pre_hook/post_hook are not supported in sling build frontmatter. Use hooks.start/hooks.end instead.\nSee https://docs.slingdata.io/concepts/sling-build for details")
	}

	return cfg, stripLeadingNewline(afterComment), true, nil
}

// stripLeadingNewline removes a single leading \n or \r\n.
func stripLeadingNewline(s string) string {
	if strings.HasPrefix(s, "\r\n") {
		return s[2:]
	}
	if strings.HasPrefix(s, "\n") {
		return s[1:]
	}
	return s
}

// addModel creates a Model from a file and adds it to the project.
func addModel(project *BuildProject, absPath, relPath string) error {
	rawSQL, err := os.ReadFile(absPath)
	if err != nil {
		return g.Error(err, "could not read model file: %s", absPath)
	}

	schema, prefix, name, fullTableName := resolveTableName(relPath, project.Mode, project.SchemaOverride, project.DefaultSchema)
	_, _, _, prodFullTableName := resolveTableName(relPath, "prod", "", project.DefaultSchema)

	// Apply merged defaults (root + child sling_build.yml) for this file's location.
	defaults := effectiveDefaults(project, relPath)
	modelConfig := ModelConfig{
		Mode:          defaults.Mode,
		Schema:        defaults.Schema,
		Tags:          append([]string(nil), defaults.Tags...),
		UniqueKey:     defaults.UniqueKey,
		UpdateKey:     defaults.UpdateKey,
		MergeStrategy: defaults.MergeStrategy,
		Enabled:       defaults.Enabled,
		Hooks:         defaults.Hooks,
		DropCascade:   defaults.DropCascade,
	}

	// Parse YAML frontmatter (canonical config declaration)
	sqlContent := string(rawSQL)
	hasFrontmatter := false
	fmConfig, remaining, hasFM, fmErr := parseYAMLFrontmatter(sqlContent)
	if fmErr != nil {
		return g.Error(fmErr, "model '%s'", name)
	}
	if hasFM {
		hasFrontmatter = true
		sqlContent = remaining
		// Frontmatter overrides defaults (non-zero fields only).
		// Tags and Hooks layer additively on top of defaults instead of replacing.
		if fmConfig.Mode != "" {
			modelConfig.Mode = fmConfig.Mode
		}
		if fmConfig.UniqueKey != nil {
			modelConfig.UniqueKey = fmConfig.UniqueKey
		}
		if fmConfig.MergeStrategy != "" {
			modelConfig.MergeStrategy = fmConfig.MergeStrategy
		}
		if fmConfig.UpdateKey != "" {
			modelConfig.UpdateKey = fmConfig.UpdateKey
		}
		if len(fmConfig.Tags) > 0 {
			modelConfig.Tags = lo.Uniq(append(modelConfig.Tags, fmConfig.Tags...))
		}
		modelConfig.Hooks = mergeHookMaps(modelConfig.Hooks, fmConfig.Hooks)
		if fmConfig.Schema != "" {
			modelConfig.Schema = fmConfig.Schema
		}
		if fmConfig.Enabled != nil {
			modelConfig.Enabled = fmConfig.Enabled
		}
		if fmConfig.Engine != "" {
			modelConfig.Engine = fmConfig.Engine
		}
		if fmConfig.Range != nil {
			modelConfig.Range = fmConfig.Range
		}
		if fmConfig.DropCascade != nil {
			modelConfig.DropCascade = fmConfig.DropCascade
		}
		if fmConfig.Rewrite != nil {
			modelConfig.Rewrite = fmConfig.Rewrite
		}
		if len(fmConfig.Tests) > 0 {
			modelConfig.Tests = fmConfig.Tests
		}
		if fmConfig.Materialized != "" {
			modelConfig.Materialized = fmConfig.Materialized
		}
	}

	// Schema override from defaults or frontmatter — recompute FullTableName.
	// (Before this change, a frontmatter `schema:` was silently ignored at the
	// table-name level; only modelConfig.Schema was set.)
	// ProdFullTableName is intentionally NOT rewritten: it exists specifically
	// as the prod-mode reference for SQL ref() matching.
	if modelConfig.Schema != "" {
		schema = modelConfig.Schema
		qualifiedName := name
		if prefix != "" {
			qualifiedName = prefix + "_" + name
		}
		fullTableName = schema + "." + qualifiedName
	}

	// Detect incremental pattern (dbt-style vs sling-native). Errors at load time
	// if the model mixes both patterns.
	style, styleErr := detectModelStyle(sqlContent)
	if styleErr != nil {
		return g.Error(styleErr, "model '%s'", name)
	}

	model := &Model{
		Name:              name,
		FilePath:          absPath,
		RelPath:           relPath,
		Schema:            schema,
		Prefix:            prefix,
		FullTableName:     fullTableName,
		ProdFullTableName: prodFullTableName,
		RawSQL:            sqlContent,
		Config:            modelConfig,
		HasFrontmatter:    hasFrontmatter,
		Style:             style,
	}

	if err := validateModel(model); err != nil {
		return err
	}

	if existing, ok := project.Models[name]; ok {
		return g.Error("duplicate model name '%s': found in both '%s' and '%s'", name, existing.RelPath, relPath)
	}

	project.Models[name] = model
	return nil
}

// addSeed creates a Seed from a file and adds it to the project.
func addSeed(project *BuildProject, absPath, relPath, format string) error {
	schema, prefix, name, fullTableName := resolveTableName(relPath, project.Mode, project.SchemaOverride, project.DefaultSchema)
	_, _, _, prodFullTableName := resolveTableName(relPath, "prod", "", project.DefaultSchema)

	// Seeds only honor defaults.schema from the merged config — no tags,
	// enabled, hooks, or unique_key semantics apply to seeds today.
	if defaults := effectiveDefaults(project, relPath); defaults.Schema != "" {
		schema = defaults.Schema
		qualifiedName := name
		if prefix != "" {
			qualifiedName = prefix + "_" + name
		}
		fullTableName = schema + "." + qualifiedName
	}

	seed := &Seed{
		Name:              name,
		FilePath:          absPath,
		RelPath:           relPath,
		Schema:            schema,
		Prefix:            prefix,
		FullTableName:     fullTableName,
		ProdFullTableName: prodFullTableName,
		Format:            format,
	}

	if existing, ok := project.Seeds[name]; ok {
		return g.Error("duplicate seed name '%s': found in both '%s' and '%s'", name, existing.RelPath, relPath)
	}

	project.Seeds[name] = seed
	return nil
}

// resolveTableName determines the schema, prefix, name, and full table name from a relative path.
func resolveTableName(relPath, mode, schemaOverride, defaultSchema string) (schema, prefix, name, fullTableName string) {
	// Normalize path separators
	relPath = filepath.ToSlash(relPath)

	// Split path into parts
	parts := strings.Split(relPath, "/")

	// Extract filename and remove extension
	fileName := parts[len(parts)-1]
	ext := filepath.Ext(fileName)
	name = strings.TrimSuffix(fileName, ext)

	// Get directory parts (excluding filename)
	dirParts := parts[:len(parts)-1]

	if mode == "dev" {
		// Dev mode: all folder parts become prefix, use override schema
		schema = schemaOverride
		if len(dirParts) > 0 {
			prefix = strings.Join(dirParts, "_")
		}
	} else {
		// Prod mode: 1st folder = schema, remaining = prefix
		if len(dirParts) == 0 {
			// Root-level file
			schema = defaultSchema
		} else {
			schema = dirParts[0]
			if len(dirParts) > 1 {
				prefix = strings.Join(dirParts[1:], "_")
			}
		}
	}

	// Build full table name
	qualifiedName := name
	if prefix != "" {
		qualifiedName = prefix + "_" + name
	}
	fullTableName = schema + "." + qualifiedName

	return
}

// validateUniqueNames checks that there are no duplicate names across models and seeds.
func validateUniqueNames(project *BuildProject) error {
	seen := make(map[string]string) // name -> "model" or "seed"

	for name := range project.Models {
		if existing, ok := seen[name]; ok {
			return g.Error("duplicate name '%s': found as both %s and model", name, existing)
		}
		seen[name] = "model"
	}

	for name := range project.Seeds {
		if existing, ok := seen[name]; ok {
			return g.Error("duplicate name '%s': found as both %s and seed", name, existing)
		}
		seen[name] = "seed"
	}

	return nil
}

// effectiveDefaults returns the merged BuildDefaults that apply to the file at relPath.
// It is the single source of truth for "what defaults apply to this file" and is
// reusable by both addModel and addSeed.
func effectiveDefaults(project *BuildProject, relPath string) BuildDefaults {
	relDir := filepath.Dir(relPath)
	if relDir == "." {
		relDir = ""
	}
	cfg := project.GetEffectiveConfig(relDir)
	if cfg == nil {
		return BuildDefaults{}
	}
	return cfg.Defaults
}

// GetEffectiveConfig returns the merged config for the project, applying child overrides.
func (p *BuildProject) GetEffectiveConfig(dir string) *BuildConfig {
	if dir == "" || dir == "." {
		return p.Config
	}

	parts := strings.Split(dir, string(filepath.Separator))
	if len(parts) > 0 {
		if childCfg, ok := p.ChildConfigs[parts[0]]; ok {
			return mergeConfigs(p.Config, childCfg)
		}
	}

	return p.Config
}

// AllNames returns a sorted list of all model and seed names.
func (p *BuildProject) AllNames() []string {
	names := make([]string, 0, len(p.Models)+len(p.Seeds))
	for name := range p.Models {
		names = append(names, name)
	}
	for name := range p.Seeds {
		names = append(names, name)
	}
	return lo.Uniq(names)
}

// LookupFullTableName returns the full table name for a given model or seed name.
func (p *BuildProject) LookupFullTableName(name string) (string, bool) {
	if m, ok := p.Models[name]; ok {
		return m.FullTableName, true
	}
	if s, ok := p.Seeds[name]; ok {
		return s.FullTableName, true
	}
	return "", false
}

// prodNameEntry maps a prod-mode name to its model/seed name and current-mode FullTableName.
type prodNameEntry struct {
	Name          string // model or seed name
	FullTableName string // current-mode full table name
}

// BuildProdNameIndex builds a lookup map from lowercased prod FullTableName (and unqualified name)
// to the model/seed entry. Qualified names take priority over unqualified for the same key.
func (p *BuildProject) BuildProdNameIndex() map[string]prodNameEntry {
	index := make(map[string]prodNameEntry)

	// First pass: add unqualified names (lower priority)
	for _, m := range p.Models {
		key := strings.ToLower(m.Name)
		index[key] = prodNameEntry{Name: m.Name, FullTableName: m.FullTableName}
	}
	for _, s := range p.Seeds {
		key := strings.ToLower(s.Name)
		index[key] = prodNameEntry{Name: s.Name, FullTableName: s.FullTableName}
	}

	// Second pass: add qualified prod names (higher priority, overwrites unqualified if same key)
	for _, m := range p.Models {
		key := strings.ToLower(m.ProdFullTableName)
		index[key] = prodNameEntry{Name: m.Name, FullTableName: m.FullTableName}
	}
	for _, s := range p.Seeds {
		key := strings.ToLower(s.ProdFullTableName)
		index[key] = prodNameEntry{Name: s.Name, FullTableName: s.FullTableName}
	}

	return index
}

// =============================================================================
// Seed Loading
// =============================================================================

// LoadSeed loads a seed file into the target database using the existing
// sling task infrastructure. This gets CSV/JSON/Parquet parsing, type inference,
// bulk loading, and all 30+ connectors for free. Seeds always use full-refresh.
func LoadSeed(seed *Seed, connName string, fullRefresh bool) error {
	_ = fullRefresh // seeds always full-refresh; kept for call-site clarity

	// Build source connection using file:// prefix for the directory
	sourceDir := filepath.Dir(seed.FilePath)
	sourceFile := filepath.Base(seed.FilePath)

	cfg := &sling.Config{
		Source: sling.Source{
			Conn:    "file://" + sourceDir,
			Stream:  sourceFile,
			Options: &sling.SourceOptions{},
		},
		Target: sling.Target{
			Conn:   connName,
			Object: seed.FullTableName,
		},
		Mode: sling.FullRefreshMode,
	}

	task := sling.NewTask("", cfg)
	if task.Err != nil {
		return g.Error(task.Err, "could not create task for seed '%s'", seed.Name)
	}

	if err := task.Execute(); err != nil {
		return g.Error(err, "could not load seed '%s' into %s", seed.Name, seed.FullTableName)
	}

	return nil
}

// MakeSeedConfig creates a sling.Config for loading a seed file
// without executing it. Useful for testing and compile mode.
func MakeSeedConfig(seed *Seed, connName string) *sling.Config {
	sourceDir := filepath.Dir(seed.FilePath)
	sourceFile := filepath.Base(seed.FilePath)

	return &sling.Config{
		Source: sling.Source{
			Conn:    "file://" + sourceDir,
			Stream:  sourceFile,
			Options: &sling.SourceOptions{},
		},
		Target: sling.Target{
			Conn:   connName,
			Object: seed.FullTableName,
		},
		Mode: sling.FullRefreshMode,
	}
}
