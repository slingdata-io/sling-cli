// Package assist implements `sling assist`: profile, skills/MCP install, prompt, resume.
package assist

import (
	"embed"
	"os"
	"path/filepath"
	"sync"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
	"gopkg.in/yaml.v3"
)

//go:embed all:skills
var SkillsFS embed.FS

//go:embed prompts.yaml
var PromptsYAML []byte

// SchemaVersion is the on-disk layout version.
const SchemaVersion = 1

// HistoryMaxEntries caps entries under ~/.sling/assist/history/.
const HistoryMaxEntries = 100

// ExecsMaxEntries caps failure snapshots under ~/.sling/assist/errors/.
const ExecsMaxEntries = 100

// Paths is the injectable path seam (tests use SetPaths).
type Paths struct {
	SlingHome string
	UserHome  string
	CWD       string
}

var (
	pathsMu       sync.RWMutex
	pathsOverride *Paths // nil = live OS
)

// CurrentPaths returns the active path set (override or live).
func CurrentPaths() Paths {
	pathsMu.RLock()
	ov := pathsOverride
	pathsMu.RUnlock()
	if ov != nil {
		return *ov
	}
	cwd, _ := os.Getwd()
	return Paths{
		SlingHome: env.HomeDir,
		UserHome:  g.UserHomeDir(),
		CWD:       cwd,
	}
}

// SetPaths installs a path override; restore undoes it.
func SetPaths(p Paths) (restore func()) {
	pathsMu.Lock()
	prev := pathsOverride
	cp := p
	pathsOverride = &cp
	pathsMu.Unlock()
	return func() {
		pathsMu.Lock()
		pathsOverride = prev
		pathsMu.Unlock()
	}
}

func slingHome() string {
	return CurrentPaths().SlingHome
}

func userHome() string {
	return CurrentPaths().UserHome
}

func workDir() string {
	p := CurrentPaths()
	if p.CWD != "" {
		return p.CWD
	}
	cwd, _ := os.Getwd()
	return cwd
}

// projectRootMarkers identify a project root when walking up for --scope project.
var projectRootMarkers = []string{
	".git",
	"go.mod",
	"package.json",
	"pyproject.toml",
	"Cargo.toml",
	"sling_build.yml",
	".sling",
}

// projectRoot walks up from workDir for a marker; falls back to workDir.
func projectRoot() string {
	dir := workDir()
	if dir == "" {
		return dir
	}
	start := dir
	for {
		for _, m := range projectRootMarkers {
			if g.PathExists(filepath.Join(dir, m)) {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return start
		}
		dir = parent
	}
}

func projectPath(elem ...string) string {
	return filepath.Join(append([]string{projectRoot()}, elem...)...)
}

const assistEnvKey = "SLING_ASSIST"

// Profile is stored under env.SLING_ASSIST in ~/.sling/env.yaml.
type Profile struct {
	Agent               string `yaml:"agent" json:"agent"`                                           // claude | codex | … | auto
	HintInErrors        bool   `yaml:"hint_in_errors" json:"hint_in_errors"`                         // run-error footer
	DefaultInstallScope string `yaml:"default_install_scope,omitempty" json:"default_install_scope"` // user | project
}

// DefaultProfile returns sane first-run defaults.
func DefaultProfile() Profile {
	return Profile{
		Agent:               "auto",
		HintInErrors:        true,
		DefaultInstallScope: "user",
	}
}

func envFilePath() string {
	return env.GetEnvFilePath(slingHome())
}

// LoadProfile reads env.SLING_ASSIST. Missing key → (Profile{}, false, nil).
func LoadProfile() (p Profile, exists bool, err error) {
	path := envFilePath()
	if _, statErr := os.Stat(path); statErr != nil {
		if os.IsNotExist(statErr) {
			return Profile{}, false, nil
		}
		return Profile{}, false, g.Error(statErr, "could not stat %s", path)
	}
	ef := env.LoadEnvFile(path)
	raw, ok := ef.Env[assistEnvKey]
	if !ok || raw == nil {
		return Profile{}, false, nil
	}
	m, err := castToStringMap(raw)
	if err != nil {
		return Profile{}, false, g.Error(err, "env.%s is not a mapping", assistEnvKey)
	}
	if len(m) == 0 {
		return Profile{}, false, nil
	}
	p, err = profileFromMap(m)
	if err != nil {
		return Profile{}, false, err
	}
	return p, true, nil
}

// castToStringMap normalizes YAML maps (map[string]any or map[any]any).
func castToStringMap(v any) (map[string]any, error) {
	switch m := v.(type) {
	case map[string]any:
		return m, nil
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, vv := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, g.Error("non-string key %v", k)
			}
			out[ks] = vv
		}
		return out, nil
	case nil:
		return map[string]any{}, nil
	default:
		return nil, g.Error("unexpected type %T", v)
	}
}

// SaveProfile writes env.SLING_ASSIST via EnvFile (preserves other keys/comments).
func SaveProfile(p Profile) error {
	path := envFilePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return g.Error(err, "could not create %s", filepath.Dir(path))
	}
	ef := env.LoadEnvFile(path)
	ef.Path = path
	m, err := profileToMap(p)
	if err != nil {
		return err
	}
	if ef.Env == nil {
		ef.Env = map[string]any{}
	}
	ef.Env[assistEnvKey] = m
	return ef.WriteEnvFile()
}

func profileToMap(p Profile) (map[string]any, error) {
	b, err := yaml.Marshal(p)
	if err != nil {
		return nil, g.Error(err, "could not marshal profile")
	}
	m := map[string]any{}
	if uerr := yaml.Unmarshal(b, &m); uerr != nil {
		return nil, g.Error(uerr, "could not re-parse profile")
	}
	return m, nil
}

func profileFromMap(m map[string]any) (Profile, error) {
	b, err := yaml.Marshal(m)
	if err != nil {
		return Profile{}, g.Error(err, "could not marshal SLING_ASSIST block")
	}
	var p Profile
	if uerr := yaml.Unmarshal(b, &p); uerr != nil {
		return Profile{}, g.Error(uerr, "could not parse SLING_ASSIST block")
	}
	return p, nil
}

// AssistDir returns ~/.sling/assist/.
func AssistDir() string {
	d := filepath.Join(slingHome(), "assist")
	_ = os.MkdirAll(d, 0o755)
	return d
}

// HistoryDir returns ~/.sling/assist/history/.
func HistoryDir() string {
	d := filepath.Join(AssistDir(), "history")
	_ = os.MkdirAll(d, 0o755)
	return d
}

// ErrorsDir returns ~/.sling/assist/errors/ (legacy snapshot root).
// New snapshots live under ExecutionsDir(); readers scan both.
func ErrorsDir() string {
	d := filepath.Join(AssistDir(), "errors")
	_ = os.MkdirAll(d, 0o755)
	return d
}

// ExecutionsDir returns ~/.sling/assist/errors/executions/ (failure snapshots).
func ExecutionsDir() string {
	d := filepath.Join(ErrorsDir(), "executions")
	_ = os.MkdirAll(d, 0o755)
	return d
}

// VersionFilePath returns ~/.sling/assist/version.
func VersionFilePath() string {
	return filepath.Join(AssistDir(), "version")
}

// CanonicalSkillsDir returns ~/.agents/skills/ (shared skill source of truth).
func CanonicalSkillsDir() string {
	d := filepath.Join(userHome(), ".agents", "skills")
	_ = os.MkdirAll(d, 0o755)
	return d
}
