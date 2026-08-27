package project

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/flarco/g"
	"gopkg.in/yaml.v3"
)

const (
	// ManifestFileName is the canonical project file.
	ManifestFileName = "sling_project.yml"
	// LegacyFileName is the older JSON project file.
	LegacyFileName = ".sling.json"
)

// Manifest is a local Sling project file.
// Unknown YAML/JSON keys are ignored.
type Manifest struct {
	Name      string             `yaml:"name,omitempty" json:"name,omitempty"`
	ProjectID string             `yaml:"project_id,omitempty" json:"id,omitempty"`
	Paths     []string           `yaml:"paths,omitempty" json:"paths,omitempty"`
	Jobs      map[string]JobSpec `yaml:"jobs,omitempty" json:"jobs,omitempty"`
	Root      string             `yaml:"-" json:"-"`
	Path      string             `yaml:"-" json:"-"`
}

// Parse unmarshals a sling_project.yml body. Extra keys are ignored.
func Parse(body []byte) (Manifest, error) {
	var m Manifest
	if err := yaml.Unmarshal(body, &m); err != nil {
		return m, g.Error(err, "could not parse %s", ManifestFileName)
	}
	return m, nil
}

// HasManifest reports whether dir has sling_project.yml or .sling.json.
func HasManifest(dir string) bool {
	if dir == "" {
		return false
	}
	if g.PathExists(filepath.Join(dir, ManifestFileName)) {
		return true
	}
	return g.PathExists(filepath.Join(dir, LegacyFileName))
}

// FindRoot walks up from start and returns the nearest folder with a manifest.
func FindRoot(start string) (string, error) {
	if start == "" {
		wd, err := os.Getwd()
		if err != nil {
			return "", g.Error(err, "could not get working directory")
		}
		start = wd
	}
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", g.Error(err, "could not resolve path")
	}
	for {
		if HasManifest(dir) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", g.Error("no sling project found")
		}
		dir = parent
	}
}

// Load reads sling_project.yml first, then .sling.json.
func Load(folderPath string) (Manifest, error) {
	var m Manifest
	if folderPath == "" {
		return m, g.Error("folder path is empty")
	}
	abs, err := filepath.Abs(folderPath)
	if err != nil {
		return m, g.Error(err, "could not resolve folder")
	}

	ymlPath := filepath.Join(abs, ManifestFileName)
	jsonPath := filepath.Join(abs, LegacyFileName)

	switch {
	case g.PathExists(ymlPath):
		body, err := os.ReadFile(ymlPath)
		if err != nil {
			return m, g.Error(err, "could not read file %s", ymlPath)
		}
		if err = yaml.Unmarshal(body, &m); err != nil {
			return m, g.Error(err, "could not parse file %s", ymlPath)
		}
		m.Path = ymlPath
	case g.PathExists(jsonPath):
		body, err := os.ReadFile(jsonPath)
		if err != nil {
			return m, g.Error(err, "could not read file %s", jsonPath)
		}
		if err = json.Unmarshal(body, &m); err != nil {
			return m, g.Error(err, "could not parse file %s", jsonPath)
		}
		m.Path = jsonPath
	default:
		return m, g.Error("did not find %s or %s", ManifestFileName, LegacyFileName)
	}

	m.Root = abs
	return m, nil
}

// Linked reports whether the manifest points at a platform project.
func (m Manifest) Linked() bool {
	return strings.TrimSpace(m.ProjectID) != ""
}

// LinkProject is one platform project a token can attach to.
type LinkProject struct {
	ID   string
	Name string
}

// ResolveLinkProjectID picks the platform project id to attach a folder to.
// A token-scoped id wins. Otherwise one listed project is used, or pick() when
// more than one exists.
func ResolveLinkProjectID(tokenID string, listed []LinkProject, pick func([]LinkProject) (string, error)) (string, error) {
	if id := strings.TrimSpace(tokenID); id != "" {
		return id, nil
	}
	if len(listed) == 0 {
		return "", g.Error("no platform projects found for this token")
	}
	if len(listed) == 1 {
		return listed[0].ID, nil
	}
	if pick == nil {
		return "", g.Error("multiple platform projects; use a project-scoped token or pick one")
	}
	return pick(listed)
}

// SetProjectID writes project_id into sling_project.yml and keeps comments.
func SetProjectID(folderPath, projectID string) error {
	if strings.TrimSpace(projectID) == "" {
		return g.Error("project id is empty")
	}
	abs, err := filepath.Abs(folderPath)
	if err != nil {
		return g.Error(err, "could not resolve folder")
	}
	ymlPath := filepath.Join(abs, ManifestFileName)

	var doc yaml.Node
	if g.PathExists(ymlPath) {
		body, err := os.ReadFile(ymlPath)
		if err != nil {
			return g.Error(err, "could not read file %s", ymlPath)
		}
		if err = yaml.Unmarshal(body, &doc); err != nil {
			return g.Error(err, "could not parse file %s", ymlPath)
		}
	}
	if err = setYAMLMapString(&doc, "project_id", projectID); err != nil {
		return err
	}
	if mapping := yamlMapping(&doc); mapping != nil && yamlMapValue(mapping, "name") == "" {
		name := filepath.Base(abs)
		if m, loadErr := Load(abs); loadErr == nil && m.Name != "" {
			name = m.Name
		}
		_ = setYAMLMapString(&doc, "name", name)
	}

	var buf strings.Builder
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err = enc.Encode(&doc); err != nil {
		_ = enc.Close()
		return g.Error(err, "could not marshal %s", ManifestFileName)
	}
	if err = enc.Close(); err != nil {
		return g.Error(err, "could not finalize %s", ManifestFileName)
	}
	if err = os.WriteFile(ymlPath, []byte(buf.String()), 0644); err != nil {
		return g.Error(err, "could not write %s", ymlPath)
	}

	jsonPath := filepath.Join(abs, LegacyFileName)
	if g.PathExists(jsonPath) {
		m, loadErr := Load(abs)
		if loadErr == nil {
			m.ProjectID = projectID
			body := []byte(g.Pretty(map[string]any{
				"id":    m.ProjectID,
				"name":  m.Name,
				"paths": m.Paths,
			}))
			_ = os.WriteFile(jsonPath, body, 0644)
		}
	}

	return nil
}

func yamlMapping(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind == yaml.MappingNode {
		return n
	}
	return nil
}

func yamlMapValue(m *yaml.Node, key string) string {
	if m == nil {
		return ""
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1].Value
		}
	}
	return ""
}

func setYAMLMapString(doc *yaml.Node, key, value string) error {
	if doc == nil {
		return g.Error("yaml document is nil")
	}
	if doc.Kind == 0 {
		doc.Kind = yaml.DocumentNode
		doc.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
	}
	m := yamlMapping(doc)
	if m == nil {
		return g.Error("sling_project.yml root is not a mapping")
	}
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i].Value == key {
			m.Content[i+1].Kind = yaml.ScalarNode
			m.Content[i+1].Tag = "!!str"
			m.Content[i+1].Value = value
			return nil
		}
	}
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value},
	)
	return nil
}
