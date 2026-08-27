package validate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// ReplicationDTO is a replication file without engine-only fields.
type ReplicationDTO struct {
	Source   any            `json:"source" yaml:"source"`
	Target   any            `json:"target" yaml:"target"`
	Hooks    any            `json:"hooks,omitempty" yaml:"hooks,omitempty"`
	Defaults any            `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Streams  map[string]any `json:"streams" yaml:"streams"`
	Env      map[string]any `json:"env,omitempty" yaml:"env,omitempty"`
}

// PipelineDTO is a pipeline file without engine-only fields.
type PipelineDTO struct {
	Steps []any          `json:"steps" yaml:"steps"`
	Env   map[string]any `json:"env,omitempty" yaml:"env,omitempty"`
}

// EnvDTO is an env.yaml file. Values are not interpolated.
type EnvDTO struct {
	Connections map[string]any `json:"connections,omitempty" yaml:"connections,omitempty"`
	Env         map[string]any `json:"env,omitempty" yaml:"env,omitempty"`
	Variables   map[string]any `json:"variables,omitempty" yaml:"variables,omitempty"`
}

// APISpecDTO is an API spec without engine-only fields.
type APISpecDTO struct {
	Name             string         `json:"name" yaml:"name"`
	Description      string         `json:"description,omitempty" yaml:"description,omitempty"`
	Authentication   any            `json:"authentication,omitempty" yaml:"authentication,omitempty"`
	Defaults         any            `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Endpoints        map[string]any `json:"endpoints,omitempty" yaml:"endpoints,omitempty"`
	DynamicEndpoints any            `json:"dynamic_endpoints,omitempty" yaml:"dynamic_endpoints,omitempty"`
	Queues           any            `json:"queues,omitempty" yaml:"queues,omitempty"`
}

// BuildDTO is a sling_build.yml file.
type BuildDTO struct {
	Target     string         `json:"target,omitempty" yaml:"target,omitempty"`
	Dev        any            `json:"dev,omitempty" yaml:"dev,omitempty"`
	DbtProject any            `json:"dbt_project,omitempty" yaml:"dbt_project,omitempty"`
	Vars       map[string]any `json:"vars,omitempty" yaml:"vars,omitempty"`
	Defaults   any            `json:"defaults,omitempty" yaml:"defaults,omitempty"`
}

// ProjectDTO is a sling_project.yml manifest.
type ProjectDTO struct {
	Name      string         `json:"name,omitempty" yaml:"name,omitempty"`
	ProjectID string         `json:"project_id,omitempty" yaml:"project_id,omitempty"`
	Jobs      map[string]any `json:"jobs,omitempty" yaml:"jobs,omitempty"`
}

var knownStepTypes = map[string]bool{
	"query":       true,
	"http":        true,
	"check":       true,
	"copy":        true,
	"list":        true,
	"write":       true,
	"read":        true,
	"replication": true,
	"command":     true,
	"group":       true,
	"delete":      true,
	"log":         true,
	"inspect":     true,
	"store":       true,
	"set":         true,
	"routine":     true,
	"read_cdc":    true,
	"build":       true,
}

func parseDTO(kind Kind, body []byte) (any, error) {
	switch kind {
	case KindReplication:
		return parseReplicationDTO(body)
	case KindPipeline:
		return parsePipelineDTO(body)
	case KindEnv:
		return parseEnvDTO(body)
	case KindAPISpec:
		return parseAPISpecDTO(body)
	case KindBuild:
		return parseBuildDTO(body)
	case KindMonitor, KindRoutine:
		root := map[string]any{}
		if err := yaml.Unmarshal(body, &root); err != nil {
			return nil, fmt.Errorf("invalid yaml: %w", err)
		}
		return Redact(root), nil
	default:
		return nil, fmt.Errorf("unknown kind")
	}
}

// parseReplicationDTO parses a replication file and checks required keys.
func parseReplicationDTO(body []byte) (any, error) {
	var dto ReplicationDTO
	if err := yaml.Unmarshal(body, &dto); err != nil {
		return nil, fmt.Errorf("invalid replication yaml: %w", err)
	}
	if strings.TrimSpace(cast.ToString(dto.Source)) == "" {
		return nil, fmt.Errorf("replication is missing required key 'source'")
	}
	if strings.TrimSpace(cast.ToString(dto.Target)) == "" {
		return nil, fmt.Errorf("replication is missing required key 'target'")
	}
	if dto.Streams == nil {
		return nil, fmt.Errorf("replication is missing required key 'streams'")
	}
	if err := validateStreams(dto.Streams); err != nil {
		return nil, err
	}
	if err := validateModeIn(dto.Defaults, "defaults"); err != nil {
		return nil, err
	}
	return Redact(dtoToMap(dto)), nil
}

// parsePipelineDTO parses a pipeline file and checks step mappings and types.
func parsePipelineDTO(body []byte) (any, error) {
	var dto PipelineDTO
	if err := yaml.Unmarshal(body, &dto); err != nil {
		return nil, fmt.Errorf("invalid pipeline yaml: %w", err)
	}
	if dto.Steps == nil {
		return nil, fmt.Errorf("pipeline is missing required key 'steps'")
	}
	if err := validatePipelineSteps(dto.Steps, ""); err != nil {
		return nil, err
	}
	return Redact(dtoToMap(dto)), nil
}

// validatePipelineSteps checks each step mapping and type, including
// nested steps under group and routine.
func validatePipelineSteps(steps []any, base string) error {
	for i, step := range steps {
		where := fmt.Sprintf("pipeline step %d", i)
		if base != "" {
			where = fmt.Sprintf("%s.%d", base, i)
		}
		m, ok := asMap(step)
		if !ok {
			return fmt.Errorf("%s must be a mapping", where)
		}
		typ := strings.TrimSpace(cast.ToString(m["type"]))
		if typ == "" {
			if inferred := stepType(m); inferred != "-" {
				typ = inferred
			}
		}
		if typ != "" && !knownStepTypes[typ] {
			return fmt.Errorf("%s has unknown type %q", where, typ)
		}
		if typ != "group" && typ != "routine" {
			continue
		}
		nested := asSlice(m["steps"])
		if nested == nil {
			if inner, ok := asMap(m[typ]); ok {
				nested = asSlice(inner["steps"])
			}
		}
		if err := validatePipelineSteps(nested, where+".steps"); err != nil {
			return err
		}
	}
	return nil
}

// parseEnvDTO parses an env.yaml file. Values are not interpolated.
func parseEnvDTO(body []byte) (any, error) {
	var dto EnvDTO
	if err := yaml.Unmarshal(body, &dto); err != nil {
		return nil, fmt.Errorf("invalid env yaml: %w", err)
	}
	return Redact(dtoToMap(dto)), nil
}

// parseAPISpecDTO parses an API spec file and checks required keys.
func parseAPISpecDTO(body []byte) (any, error) {
	var dto APISpecDTO
	if err := yaml.Unmarshal(body, &dto); err != nil {
		return nil, fmt.Errorf("invalid api spec yaml: %w", err)
	}
	if strings.TrimSpace(dto.Name) == "" {
		return nil, fmt.Errorf("api spec is missing required key 'name'")
	}
	if dto.Endpoints == nil && dto.DynamicEndpoints == nil {
		return nil, fmt.Errorf("api spec is missing 'endpoints' or 'dynamic_endpoints'")
	}
	for name, ep := range dto.Endpoints {
		if ep == nil {
			continue
		}
		if _, ok := ep.(map[string]any); !ok {
			return nil, fmt.Errorf("api spec endpoint %q must be a mapping", name)
		}
	}
	if _, err := api.LoadSpec(string(body)); err != nil {
		return nil, fmt.Errorf("api spec is invalid: %s", err.Error())
	}
	return Redact(dtoToMap(dto)), nil
}

// parseBuildDTO parses a sling_build.yml file into its DTO.
func parseBuildDTO(body []byte) (any, error) {
	var dto BuildDTO
	if err := yaml.Unmarshal(body, &dto); err != nil {
		return nil, fmt.Errorf("invalid build yaml: %w", err)
	}
	return Redact(dtoToMap(dto)), nil
}

// unknownConnWarnings reports source/target names that look like connections
// but are not in the local connection list. Templates ({var}) and URLs are skipped.
func unknownConnWarnings(parsed any) []string {
	m := asMapOrEmpty(parsed)
	var out []string
	for _, key := range []string{"source", "target"} {
		name := strings.TrimSpace(cast.ToString(m[key]))
		if name == "" || strings.Contains(name, "{") || strings.Contains(name, "://") {
			continue
		}
		if connection.GetLocalConns().Get(name).Name == "" {
			out = append(out, fmt.Sprintf("%s connection %q is not defined", key, name))
		}
	}
	return out
}

// validateStreams checks that each stream is a mapping or null with a valid mode.
func validateStreams(streams map[string]any) error {
	for name, val := range streams {
		if val == nil {
			continue
		}
		m, ok := asMap(val)
		if !ok {
			return fmt.Errorf("stream %q must be a mapping or null", name)
		}
		if err := validateModeIn(m, "stream "+name); err != nil {
			return err
		}
	}
	return nil
}

// validateModeIn checks that the 'mode' key in v is a known mode, when set.
func validateModeIn(v any, where string) error {
	m, ok := asMap(v)
	if !ok {
		return nil
	}
	mode := strings.TrimSpace(cast.ToString(m["mode"]))
	if mode == "" {
		return nil
	}
	if !isKnownMode(mode) {
		return fmt.Errorf("%s has unknown mode %q", where, mode)
	}
	return nil
}

// isKnownMode reports whether mode matches an entry in sling.AllMode.
func isKnownMode(mode string) bool {
	for _, am := range sling.AllMode {
		if string(am.Value) == mode {
			return true
		}
	}
	return false
}

// pipelineStepPathWarnings reports replication/build step paths that do not
// resolve. Paths may resolve at run time from another root, so these are
// warnings, never errors.
func pipelineStepPathWarnings(absPipeline string, parsed any) (out []string) {
	m, ok := asMap(parsed)
	if !ok {
		return nil
	}
	baseDir := filepath.Dir(absPipeline)
	for i, step := range asSlice(m["steps"]) {
		sm, ok := asMap(step)
		if !ok {
			continue
		}
		typ := stepType(sm)
		var path string
		switch typ {
		case "replication":
			path = firstNonEmpty(cast.ToString(sm["path"]), cast.ToString(sm["replication"]))
		case "build":
			path = firstNonEmpty(cast.ToString(sm["path"]), cast.ToString(sm["build"]))
		default:
			continue
		}
		path = strings.TrimSpace(path)
		if path == "" || strings.Contains(path, "{") {
			continue // empty, or resolved at run time from an expression
		}
		if stepPathExists(baseDir, path) {
			continue
		}
		out = append(out, fmt.Sprintf("step %d (%s) path %s does not exist", i, typ, path))
	}
	return out
}

// stepPathExists looks relative to the pipeline file, then the working dir.
func stepPathExists(baseDir, path string) bool {
	if filepath.IsAbs(path) {
		_, err := os.Stat(path)
		return err == nil
	}
	if _, err := os.Stat(filepath.Join(baseDir, path)); err == nil {
		return true
	}
	_, err := os.Stat(path)
	return err == nil
}
