package env

import (
	"bytes"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/flarco/g"
	cmap "github.com/orcaman/concurrent-map/v2"
	"gopkg.in/yaml.v3"
)

// envVarRefRe matches a whole-string ${VAR} ref. Unset refs stay literal after g.Rmd.
var envVarRefRe = regexp.MustCompile(`^\$\{([A-Z_][A-Z0-9_]*)\}$`)

type EnvFile struct {
	Connections map[string]map[string]any `json:"connections,omitempty" yaml:"connections,omitempty"`
	Env         map[string]any            `json:"env,omitempty" yaml:"env,omitempty"`
	Variables   map[string]any            `json:"variables,omitempty" yaml:"variables,omitempty"` // legacy

	Path       string `json:"-" yaml:"-"`
	TopComment string `json:"-" yaml:"-"`
	Body       string `json:"-" yaml:"-"`
}

func (ef *EnvFile) WriteEnvFile() (err error) {
	output, err := ef.marshalEnvFileBytes()
	if err != nil {
		return err
	}

	// fix windows path
	ef.Path = strings.ReplaceAll(ef.Path, `\`, `/`)
	err = os.WriteFile(ef.Path, output, 0644)
	if err != nil {
		return g.Error(err, "could not write YAML file")
	}

	return
}

// MarshalBody returns the EnvFile as a formatted YAML string
func (ef *EnvFile) MarshalBody() (string, error) {
	output, err := ef.marshalEnvFileBytes()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func (ef *EnvFile) freshRoot() *yaml.Node {
	root := &yaml.Node{
		Kind: yaml.DocumentNode,
		Content: []*yaml.Node{{
			Kind: yaml.MappingNode,
		}},
	}
	if ef.TopComment != "" {
		root.Content[0].HeadComment = strings.TrimRight(ef.TopComment, "\n")
	}
	return root
}

// marshalEnvFileBytes renders the EnvFile as YAML, preserving comments, key
// order, and unmanaged top-level keys from the file at ef.Path.
func (ef *EnvFile) marshalEnvFileBytes() ([]byte, error) {
	original, err := ef.loadRootNode()
	if err != nil {
		return nil, err
	}

	newRoot, err := ef.structToRootNode(original)
	if err != nil {
		return nil, err
	}

	merged := mergeNode(original, newRoot, interpEnvMap(ef.Path))
	annotateEnvVarRefComments(merged)

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(merged); err != nil {
		_ = enc.Close()
		return nil, g.Error(err, "could not marshal into YAML")
	}
	if err := enc.Close(); err != nil {
		return nil, g.Error(err, "could not finalize YAML encoder")
	}
	return buf.Bytes(), nil
}

// structToRootNode marshals the EnvFile struct into a DocumentNode, mirroring
// unmanaged top-level keys from original so the merge doesn't drop them.
func (ef *EnvFile) structToRootNode(original *yaml.Node) (*yaml.Node, error) {
	b, err := yaml.Marshal(ef)
	if err != nil {
		return nil, g.Error(err, "could not marshal env file")
	}
	var doc yaml.Node
	if uerr := yaml.Unmarshal(b, &doc); uerr != nil {
		return nil, g.Error(uerr, "could not re-parse env file node")
	}
	if doc.Kind == 0 {
		doc = yaml.Node{
			Kind:    yaml.DocumentNode,
			Content: []*yaml.Node{{Kind: yaml.MappingNode}},
		}
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		doc.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
	}

	managed := map[string]struct{}{
		"connections": {}, "variables": {}, "env": {},
	}
	if original != nil && len(original.Content) > 0 && original.Content[0].Kind == yaml.MappingNode {
		newMap := doc.Content[0]
		newKeys := map[string]struct{}{}
		for i := 0; i < len(newMap.Content); i += 2 {
			newKeys[newMap.Content[i].Value] = struct{}{}
		}
		origMap := original.Content[0]
		for i := 0; i < len(origMap.Content); i += 2 {
			k := origMap.Content[i].Value
			if _, isManaged := managed[k]; isManaged {
				continue
			}
			if _, alreadyInNew := newKeys[k]; alreadyInNew {
				continue
			}
			keyCopy := *origMap.Content[i]
			valCopy := *origMap.Content[i+1]
			newMap.Content = append(newMap.Content, &keyCopy, &valCopy)
		}
	}

	return &doc, nil
}

var dotEnvMap = cmap.New[string]()

// LoadDotEnvSling reads a `.env.sling` file from the current working directory
// and injects its key=value pairs into os environment variables.
// Existing env vars are not overwritten.
func LoadDotEnvSling() map[string]string {
	cwd, err := os.Getwd()
	if err != nil {
		return dotEnvMap.Items()
	}
	return LoadDotEnvSlingFrom(cwd)
}

// LoadDotEnvSlingFrom reads a `.env.sling` file from the specified directory
// and injects its key=value pairs into os environment variables.
// Existing env vars are not overwritten.
func LoadDotEnvSlingFrom(dir string) map[string]string {
	dotEnvPath := path.Join(dir, ".env.sling")
	bytes, err := os.ReadFile(dotEnvPath)
	if err != nil {
		return dotEnvMap.Items() // file doesn't exist or can't be read
	}

	for key, val := range ParseDotEnv(string(bytes)) {
		// don't overwrite existing env vars
		if _, exists := os.LookupEnv(key); !exists {
			dotEnvMap.Set(key, val)
			os.Setenv(key, val)
		}
	}
	return dotEnvMap.Items()
}

// ParseDotEnv parses a .env file content into key-value pairs.
// It supports single-line and multi-line values enclosed in matching quotes (' or ").
func ParseDotEnv(content string) map[string]string {
	result := map[string]string{}
	lines := strings.Split(content, "\n")

	for i := 0; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, val, found := strings.Cut(line, "=")
		if !found {
			continue
		}

		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)

		// check for quoted multi-line values
		if len(val) >= 1 && (val[0] == '\'' || val[0] == '"') {
			quote := val[0]

			// check if closing quote is on the same line
			if len(val) >= 2 && val[len(val)-1] == quote {
				// single-line quoted value
				val = val[1 : len(val)-1]
			} else {
				// multi-line: accumulate lines until we find the closing quote
				var buf strings.Builder
				buf.WriteString(val[1:]) // content after opening quote
				for i++; i < len(lines); i++ {
					raw := lines[i]
					trimmed := strings.TrimRight(raw, " \t")
					if len(trimmed) > 0 && trimmed[len(trimmed)-1] == quote {
						buf.WriteByte('\n')
						buf.WriteString(trimmed[:len(trimmed)-1])
						break
					}
					buf.WriteByte('\n')
					buf.WriteString(raw)
				}
				val = buf.String()
			}
		}

		result[key] = val
	}
	return result
}

func UnsetEnvKeys(keys []string) {
	for _, key := range keys {
		os.Unsetenv(key)
	}
}

func LoadEnvFile(path string) (ef EnvFile) {
	body, _ := os.ReadFile(path)
	ef, _ = loadEnvFile(string(body), path)
	return ef
}

// loadEnvFile parses YAML env-file content from `body`, expanding ${VAR}
// references against the current process environment (plus SLING_HOME_DIR
// when a path is provided), and exports scalar entries from `env:` into
// os.Environ. `path` is recorded on the returned EnvFile when non-empty.
func loadEnvFile(body, path string) (ef EnvFile, err error) {
	ef.Body = body
	ef.Path = path

	if body == "" {
		ef.Connections = map[string]map[string]any{}
		ef.Env = map[string]any{}
		return ef, nil
	}

	// expand variables
	envMap := interpEnvMap(path)
	ef.Body = g.Rmd(ef.Body, envMap)

	if err = yaml.Unmarshal([]byte(ef.Body), &ef); err != nil {
		err = g.Error(err, "error parsing yaml string")
	}

	if ef.Connections == nil {
		ef.Connections = map[string]map[string]any{}
	}

	if len(ef.Env) == 0 {
		if len(ef.Variables) == 0 {
			ef.Env = map[string]any{}
		} else {
			ef.Env = ef.Variables // support legacy
			ef.Variables = nil
		}
	}

	for k, v := range ef.Env {
		if _, found := envMap[k]; !found {
			// non-scalar values (e.g. SLING_ASSIST) are read from ef.Env, not os.Getenv
			switch v.(type) {
			case map[string]any, map[any]any, []any:
				continue
			}
			os.Setenv(k, g.CastToString(v))
		}
	}
	return ef, err
}

func GetEnvFilePath(dir string) string {
	return CleanWindowsPath(path.Join(dir, "env.yaml"))
}

// processEnv is the current process environment as a map.
func processEnv() map[string]any {
	out := map[string]any{}
	for _, kv := range os.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			continue
		}
		out[k] = v
	}
	return out
}

// MergeDeclaredEnv starts from the process environment and overlays
// declared pipeline/replication env keys. Bare {env.X} then renders
// even when the YAML has no env: block.
func MergeDeclaredEnv(declared map[string]any) map[string]any {
	out := processEnv()
	for k, v := range declared {
		out[k] = v
	}
	return out
}

// interpEnvMap is the same substitution map loadEnvFile uses for g.Rmd.
func interpEnvMap(path string) map[string]any {
	envMap := processEnv()
	if path != "" {
		if _, ok := envMap["SLING_HOME_DIR"]; !ok {
			envMap["SLING_HOME_DIR"] = HomeDir
		}
	}
	return envMap
}

// keepOnDiskScalar is true when newVal is origVal or origVal after env expansion.
// Load interpolates ${VAR}; write must keep the on-disk ref, not the secret.
func keepOnDiskScalar(origVal, newVal string, envMap map[string]any) bool {
	if origVal == newVal {
		return true
	}
	if envMap == nil || !strings.Contains(origVal, "${") {
		return false
	}
	return g.Rmd(origVal, envMap) == newVal
}

// mergeNode deep-merges newNode into original, keeping original's comments and
// key order. Adapted from pulumi/pulumi's yamlutil.editNodes (Apache 2.0).
func mergeNode(original, newNode *yaml.Node, envMap map[string]any) *yaml.Node {
	if original == nil {
		out := *newNode
		return &out
	}
	if newNode == nil {
		out := *original
		return &out
	}
	if original.Kind != newNode.Kind {
		out := *newNode
		return &out
	}

	ret := *original
	if original.Kind == yaml.ScalarNode && keepOnDiskScalar(original.Value, newNode.Value, envMap) {
		return &ret
	}
	ret.Tag = newNode.Tag
	ret.Value = newNode.Value

	switch original.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		minLen := len(newNode.Content)
		if len(original.Content) < minLen {
			minLen = len(original.Content)
		}
		content := make([]*yaml.Node, 0, len(newNode.Content))
		for i := 0; i < minLen; i++ {
			content = append(content, mergeNode(original.Content[i], newNode.Content[i], envMap))
		}
		content = append(content, newNode.Content[minLen:]...)
		ret.Content = content
	case yaml.MappingNode:
		ret.Content = mergeMappingContent(original, newNode, envMap)
	case yaml.ScalarNode, yaml.AliasNode:
		ret.Content = newNode.Content
	}
	return &ret
}

// mergeMappingContent merges two mapping nodes: original keys keep their
// position and comments; new-only keys append at the end; dropped keys are
// removed.
func mergeMappingContent(original, newNode *yaml.Node, envMap map[string]any) []*yaml.Node {
	origIdx := map[string]int{}
	newIdx := map[string]int{}
	var origOrder, newOnly []string

	for i := 0; i < len(original.Content); i += 2 {
		k := original.Content[i].Value
		origIdx[k] = i
		origOrder = append(origOrder, k)
	}
	for i := 0; i < len(newNode.Content); i += 2 {
		k := newNode.Content[i].Value
		newIdx[k] = i
		if _, ok := origIdx[k]; !ok {
			newOnly = append(newOnly, k)
		}
	}

	content := make([]*yaml.Node, 0, len(newNode.Content))
	for _, k := range origOrder {
		ni, present := newIdx[k]
		if !present {
			continue
		}
		oi := origIdx[k]
		key := mergeNode(original.Content[oi], newNode.Content[ni], envMap)
		val := mergeNode(original.Content[oi+1], newNode.Content[ni+1], envMap)
		content = append(content, key, val)
	}
	for _, k := range newOnly {
		ni := newIdx[k]
		key := *newNode.Content[ni]
		val := *newNode.Content[ni+1]
		content = append(content, &key, &val)
	}
	return content
}

// loadRootNode parses ef.Path into a yaml.Node tree, returning a fresh root
// if the file is missing, empty, or not a mapping at the top.
func (ef *EnvFile) loadRootNode() (*yaml.Node, error) {
	root := &yaml.Node{}
	if ef.Path == "" {
		return ef.freshRoot(), nil
	}
	data, rerr := os.ReadFile(ef.Path)
	if rerr != nil || len(bytes.TrimSpace(data)) == 0 {
		return ef.freshRoot(), nil
	}
	if uerr := yaml.Unmarshal(data, root); uerr != nil {
		return nil, g.Error(uerr, "could not parse %s", ef.Path)
	}
	if root.Kind == 0 {
		return ef.freshRoot(), nil
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		root.Content = []*yaml.Node{{Kind: yaml.MappingNode}}
	}
	return root, nil
}

// IsEnvVarRef is true when s is a whole-string ${VAR} reference.
func IsEnvVarRef(s string) bool {
	return envVarRefRe.MatchString(strings.TrimSpace(s))
}

// EnvVarRefName returns VAR from ${VAR}. It returns "" when s is not a ref.
func EnvVarRefName(s string) string {
	m := envVarRefRe.FindStringSubmatch(strings.TrimSpace(s))
	if len(m) != 2 {
		return ""
	}
	return m[1]
}

// ConnLocation is a deep-link into env.yaml for one connection.
type ConnLocation struct {
	Path       string       `json:"path"`
	Line       int          `json:"line"`
	Connection string       `json:"connection"`
	Missing    []MissingRef `json:"missing"`
}

// MissingRef is one ${VAR} field that still needs a value (or an env var).
type MissingRef struct {
	Key  string `json:"key"`
	Var  string `json:"var"`
	Line int    `json:"line"`
}

// LookupConnection re-parses ef.Path and returns line numbers for
// connections.<NAME> and each ${VAR} field under it.
func (ef *EnvFile) LookupConnection(name string) (ConnLocation, error) {
	loc := ConnLocation{
		Path:       ef.Path,
		Connection: strings.ToUpper(name),
		Missing:    []MissingRef{},
	}
	root, err := ef.loadRootNode()
	if err != nil {
		return loc, err
	}

	conns := mappingChild(root, "connections")
	if conns == nil {
		return loc, g.Error("connections block not found in %s", ef.Path)
	}

	keyNode, valNode := mappingChildFold(conns, name)
	if keyNode == nil {
		return loc, g.Error("connection %s not found in %s", name, ef.Path)
	}
	loc.Line = keyNode.Line
	collectMissingRefs(valNode, "", &loc.Missing)
	return loc, nil
}

func mappingChild(n *yaml.Node, key string) *yaml.Node {
	n = mappingRoot(n)
	if n == nil {
		return nil
	}
	for i := 0; i < len(n.Content)-1; i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

func mappingChildFold(n *yaml.Node, key string) (keyNode, valNode *yaml.Node) {
	n = mappingRoot(n)
	if n == nil {
		return nil, nil
	}
	for i := 0; i < len(n.Content)-1; i += 2 {
		if strings.EqualFold(n.Content[i].Value, key) {
			return n.Content[i], n.Content[i+1]
		}
	}
	return nil, nil
}

func mappingRoot(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind != yaml.MappingNode {
		return nil
	}
	return n
}

func collectMissingRefs(n *yaml.Node, prefix string, out *[]MissingRef) {
	if n == nil {
		return
	}
	switch n.Kind {
	case yaml.ScalarNode:
		if !IsEnvVarRef(n.Value) {
			return
		}
		*out = append(*out, MissingRef{
			Key:  prefix,
			Var:  EnvVarRefName(n.Value),
			Line: n.Line,
		})
	case yaml.MappingNode:
		for i := 0; i < len(n.Content)-1; i += 2 {
			k := n.Content[i].Value
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			collectMissingRefs(n.Content[i+1], path, out)
		}
	case yaml.SequenceNode:
		for _, child := range n.Content {
			collectMissingRefs(child, prefix, out)
		}
	}
}

// annotateEnvVarRefComments writes EnvVarRefComment on connection ${VAR}
// scalars that have no trailing comment. Original comments stay.
func annotateEnvVarRefComments(root *yaml.Node) {
	conns := mappingChild(root, "connections")
	if conns == nil {
		return
	}
	annotateMappingRefs(conns)
}

func annotateMappingRefs(n *yaml.Node) {

	// EnvVarRefComment is the trailing comment written next to scaffolded ${VAR} refs.
	const EnvVarRefComment = "replace with the value, or set the env var (CI)"

	n = mappingRoot(n)
	if n == nil {
		return
	}
	for i := 0; i < len(n.Content)-1; i += 2 {
		val := n.Content[i+1]
		switch val.Kind {
		case yaml.ScalarNode:
			if IsEnvVarRef(val.Value) && strings.TrimSpace(val.LineComment) == "" {
				val.LineComment = EnvVarRefComment
			}
		case yaml.MappingNode:
			annotateMappingRefs(val)
		}
	}
}
