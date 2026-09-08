package validate

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/slingdata-io/sling-cli/core/sling/build"
	"gopkg.in/yaml.v3"
)

// Kind is a Sling YAML document kind.
type Kind string

const (
	KindPipeline    Kind = "pipeline"
	KindReplication Kind = "replication"
	KindAPISpec     Kind = "api_spec"
	KindMonitor     Kind = "monitor"
	KindRoutine     Kind = "routine"
	KindEnv         Kind = "env"
	KindBuild       Kind = "build"
	KindProject     Kind = "project"
	KindUnknown     Kind = "unknown"
)

// DetectFileKind classifies a YAML document from parsed root keys.
// Content rules match VS Code detectSchemaType order. Path rules run
// only when content matches nothing. sling_build.yml (or a directory
// that contains one) is the first path exception. env.yaml is the
// content-unknown fallback.
func DetectFileKind(body []byte, path string) Kind {
	if kind := detectKindFromContent(body); kind != KindUnknown {
		return kind
	}
	return detectKindFromPath(path)
}

func detectKindFromContent(body []byte) Kind {
	if len(bytes.TrimSpace(body)) == 0 {
		return KindUnknown
	}

	root := map[string]any{}
	if err := yaml.Unmarshal(body, &root); err != nil || root == nil {
		return KindUnknown
	}

	if hasKey(root, "steps") {
		return KindPipeline
	}
	if hasKey(root, "source") && hasKey(root, "target") && hasKey(root, "streams") {
		return KindReplication
	}
	if hasKey(root, "name") && (hasKey(root, "endpoints") || hasKey(root, "dynamic_endpoints")) {
		return KindAPISpec
	}
	if hasKey(root, "connection") && hasKey(root, "objects") {
		return KindMonitor
	}
	if hasKey(root, "routines") {
		return KindRoutine
	}
	if hasKey(root, "connections") {
		return KindEnv
	}
	return KindUnknown
}

// detectKindFromPath falls back to file/folder name conventions when content rules match nothing.
func detectKindFromPath(path string) Kind {
	base := filepath.Base(path)
	if isManifestName(base) {
		return KindProject
	}
	if isBuildConfigName(base) {
		return KindBuild
	}
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		if _, ok := build.FindConfigFile(path); ok {
			return KindBuild
		}
	}
	if isEnvFileName(base) {
		return KindEnv
	}
	return KindUnknown
}

func hasKey(m map[string]any, key string) bool {
	_, ok := m[key]
	return ok
}

// isManifestName reports whether the file name is a sling_project manifest.
func isManifestName(name string) bool {
	n := strings.ToLower(name)
	return n == "sling_project.yml" || n == "sling_project.yaml"
}

// isBuildConfigName reports whether the file name is a sling_build config.
func isBuildConfigName(name string) bool {
	return build.IsConfigFileName(name)
}

// isEnvFileName reports whether the file name is an env.yaml connections file.
func isEnvFileName(name string) bool {
	n := strings.ToLower(name)
	return n == "env.yaml" || n == "env.yml"
}
