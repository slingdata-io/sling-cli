package env

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/flarco/g"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/slingdata-io/sling-cli/core/secrets"
	"gopkg.in/yaml.v3"
)

// envVarRefRe matches a whole-string ${VAR} ref. Unset refs stay literal after g.Rmd.
var envVarRefRe = regexp.MustCompile(`^\$\{([A-Z_][A-Z0-9_]*)\}$`)

// envKeyRe matches a safe YAML mapping key (connection name).
var envKeyRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// envVarKeyRe matches a safe environment variable name (env: key).
var envVarKeyRe = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

type EnvFile struct {
	Connections map[string]map[string]any `json:"connections,omitempty" yaml:"connections,omitempty"`
	Env         map[string]any            `json:"env,omitempty" yaml:"env,omitempty"`
	Variables   map[string]any            `json:"variables,omitempty" yaml:"variables,omitempty"` // legacy
	Workbench   *WorkbenchConfig          `json:"workbench,omitempty" yaml:"workbench,omitempty"`

	// SecretProviders holds named secret manager instances (see core/secrets).
	// Connection values that are secret references use them.
	SecretProviders map[string]map[string]any `json:"secret_providers,omitempty" yaml:"secret_providers,omitempty"`

	Path       string `json:"-" yaml:"-"`
	Repaired   bool   `json:"-" yaml:"-"` // indentation was repaired on read
	TopComment string `json:"-" yaml:"-"`
	Body       string `json:"-" yaml:"-"`
}

// WorkbenchConfig is the `workbench:` block of env.yaml: the settings of
// `sling serve workbench`. A command-line flag wins over the file.
type WorkbenchConfig struct {
	// Host is the listen address. The default is 127.0.0.1.
	Host string `json:"host,omitempty" yaml:"host,omitempty"`
	// Port is the listen port. The default is 7879; 0 picks a free port.
	Port int `json:"port,omitempty" yaml:"port,omitempty"`
	// Token is required when Host is not loopback.
	Token string `json:"token,omitempty" yaml:"token,omitempty"`
	// ProjectsRoot limits the Open-folder dialog to one folder tree.
	ProjectsRoot string `json:"projects_root,omitempty" yaml:"projects_root,omitempty"`
	// Shell allows interactive shell terminals. The default is true.
	Shell *bool `json:"shell,omitempty" yaml:"shell,omitempty"`
	// WorkerIdle stops a project worker that has no sessions and no running
	// work after this duration, for example "15m". The default is 15m.
	WorkerIdle string `json:"worker_idle,omitempty" yaml:"worker_idle,omitempty"`
	// PathExtra is prepended to PATH for shells, runs and agent CLIs, so a
	// launchd or systemd service finds the tools the user's shell does.
	PathExtra string `json:"path_extra,omitempty" yaml:"path_extra,omitempty"`
	// Env holds extra environment variables for workers.
	Env map[string]any `json:"env,omitempty" yaml:"env,omitempty"`
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
	if err := ef.CheckFile(); err != nil {
		return nil, err
	}
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
		"connections": {}, "variables": {}, "env": {}, "workbench": {}, "secret_providers": {},
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
		// don't overwrite existing env vars; the real process env wins
		if _, exists := os.LookupEnv(key); exists {
			if _, fromFile := dotEnvMap.Get(key); !fromFile {
				g.Debug("env: .env.sling key %s is hidden by the process environment", key)
			}
			continue
		}
		dotEnvMap.Set(key, val)
		os.Setenv(key, val)
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
	repaired := string(repairEnvYAML([]byte(body)))
	ef.Repaired = repaired != body
	body = repaired
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

// ExpandEntry expands ${VAR} refs in all string values of props, also inside
// strings and nested lists and maps, with the same rules as loadEnvFile.
// It returns a deep copy: the input map is not modified.
func ExpandEntry(props map[string]any) map[string]any {
	expanded, _ := expandValue(props, interpEnvMap("")).(map[string]any)
	return expanded
}

// expandValue deep-copies val, running g.Rmd over every string with envMap,
// the same interpolation loadEnvFile applies to the whole file body.
func expandValue(val any, envMap map[string]any) any {
	switch v := val.(type) {
	case string:
		return g.Rmd(v, envMap)
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = expandValue(item, envMap)
		}
		return out
	case map[any]any:
		out := make(map[any]any, len(v))
		for key, item := range v {
			out[key] = expandValue(item, envMap)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = expandValue(item, envMap)
		}
		return out
	default:
		return val
	}
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
	if rerr != nil && !os.IsNotExist(rerr) {
		return nil, g.Error(rerr, "could not read %s", ef.Path)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return ef.freshRoot(), nil
	}
	data = repairEnvYAML(data)
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

// CheckFile returns an error when the file at ef.Path does not fully parse
// into EnvFile. A struct write from a partial parse drops the entries that did
// not parse. A missing or empty file is valid.
func (ef *EnvFile) CheckFile() error {
	data, err := os.ReadFile(ef.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return g.Error(err, "could not read %s", ef.Path)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := checkEnvYAML(repairEnvYAML(data)); err != nil {
		return g.Error("%s is not valid YAML. Fix it before sling changes the file: %s", ef.Path, g.ErrMsgSimple(err))
	}
	return nil
}

// oddSpaces look like a space but YAML does not read them as whitespace.
const oddSpaces = "\u00a0\u2007\u202f"

// checkEnvYAML returns an error when b does not parse into EnvFile, or when a
// mapping key starts with a space character (see checkEnvKeys).
func checkEnvYAML(b []byte) error {
	if err := yaml.Unmarshal(b, &EnvFile{}); err != nil {
		return err
	}
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return err
	}
	return checkEnvKeys(&root)
}

// checkEnvKeys returns an error when a mapping key starts with a space
// character: an indentation error that still parses.
func checkEnvKeys(root *yaml.Node) error {
	var badKey *yaml.Node
	var walk func(n *yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil || badKey != nil {
			return
		}
		for i, c := range n.Content {
			if n.Kind == yaml.MappingNode && i%2 == 0 {
				if r, _ := utf8.DecodeRuneInString(c.Value); unicode.IsSpace(r) {
					badKey = c
					return
				}
			}
			walk(c)
		}
	}
	walk(root)
	if badKey != nil {
		return g.Error("line %d: key %q starts with a space character", badKey.Line, badKey.Value)
	}
	return nil
}

// repairEnvYAML turns tab and non-breaking-space indentation into spaces when
// b does not pass checkEnvYAML and the repaired body does. Tab widths 2, 4 and
// 8 are tried in order. A repair that changes a value is refused. Otherwise b
// is returned unchanged.
func repairEnvYAML(b []byte) []byte {
	if !bytes.ContainsAny(b, "\t"+oddSpaces) || checkEnvYAML(b) == nil {
		return b
	}
	for _, width := range []int{2, 4, 8} {
		if fixed := respaceIndent(b, width); checkEnvYAML(fixed) == nil && valuesKept(b, fixed) {
			return fixed
		}
	}
	return b
}

// valuesKept is true when each line of each scalar in fixed is also in orig,
// so a repair only moved indentation. Escaped or folded scalars fail it.
func valuesKept(orig, fixed []byte) bool {
	var root yaml.Node
	if err := yaml.Unmarshal(fixed, &root); err != nil {
		return false
	}
	var walk func(n *yaml.Node) bool
	walk = func(n *yaml.Node) bool {
		if n.Kind == yaml.ScalarNode {
			for _, line := range strings.Split(n.Value, "\n") {
				if line != "" && !bytes.Contains(orig, []byte(line)) {
					return false
				}
			}
		}
		for _, c := range n.Content {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	return walk(&root)
}

// keyOddSpaceRe matches a plain key colon followed by an odd space.
var keyOddSpaceRe = regexp.MustCompile(`^((?:- )?[A-Za-z0-9_.-]+:)[\x{00a0}\x{2007}\x{202f}]`)

// respaceIndent rewrites the leading whitespace of each line as spaces, with
// tabs expanded to tabWidth stops. An odd space after a plain key colon
// becomes a space.
func respaceIndent(b []byte, tabWidth int) []byte {
	var sb strings.Builder
	for _, line := range strings.SplitAfter(string(b), "\n") {
		col, i := 0, 0
	indent:
		for i < len(line) {
			r, size := utf8.DecodeRuneInString(line[i:])
			switch {
			case r == ' ' || strings.ContainsRune(oddSpaces, r):
				col++
			case r == '\t':
				col += tabWidth - col%tabWidth
			default:
				break indent
			}
			i += size
		}
		rest := keyOddSpaceRe.ReplaceAllString(line[i:], "$1 ")
		sb.WriteString(strings.Repeat(" ", col))
		sb.WriteString(rest)
	}
	return []byte(sb.String())
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
	root, err := ef.loadRootNode()
	if err != nil {
		return ConnLocation{Path: ef.Path, Connection: strings.ToUpper(name), Missing: []MissingRef{}}, err
	}
	return lookupConnectionInRoot(root, name, ef.Path)
}

// LookupConnectionBody is LookupConnection against a body string. path is
// recorded on the result for display only. No ${VAR} interpolation happens.
func LookupConnectionBody(body, name, path string) (ConnLocation, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(repairEnvYAML([]byte(body)), &root); err != nil {
		return ConnLocation{Path: path, Connection: strings.ToUpper(name), Missing: []MissingRef{}}, g.Error(err, "could not parse env file body")
	}
	if root.Kind == 0 {
		root = yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode}}}
	}
	return lookupConnectionInRoot(&root, name, path)
}

func lookupConnectionInRoot(root *yaml.Node, name, path string) (ConnLocation, error) {
	loc := ConnLocation{
		Path:       path,
		Connection: strings.ToUpper(name),
		Missing:    []MissingRef{},
	}

	conns := mappingChild(root, "connections")
	if conns == nil {
		return loc, g.Error("connections block not found in %s", path)
	}

	keyNode, valNode := mappingChildFold(conns, name)
	if keyNode == nil {
		return loc, g.Error("connection %s not found in %s", name, path)
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

// ValidateKey returns an error if key is not a safe YAML mapping key
// (^[A-Za-z_][A-Za-z0-9_-]*$, no leading/trailing whitespace).
func ValidateKey(key string) error {
	if key == "" {
		return g.Error("name is blank")
	}
	if strings.TrimSpace(key) != key {
		return g.Error("name %q must not have leading or trailing whitespace", key)
	}
	if !envKeyRe.MatchString(key) {
		return g.Error("invalid name %q: must match %s", key, envKeyRe.String())
	}
	return nil
}

// ValidateEnvKey returns an error if key is not a safe environment variable
// name (^[A-Z_][A-Z0-9_]*$).
func ValidateEnvKey(key string) error {
	if key == "" {
		return g.Error("env var name is blank")
	}
	if !envVarKeyRe.MatchString(key) {
		return g.Error("invalid env var name %q: must match %s", key, envVarKeyRe.String())
	}
	return nil
}

// ExpandRef expands a whole-string ${VAR} against the process environment.
// An unset var keeps the ref as-is.
func ExpandRef(s string) string {
	name := EnvVarRefName(s)
	if name == "" {
		return s
	}
	if v, ok := os.LookupEnv(name); ok {
		return v
	}
	return s
}

// BodySha returns the sha256 hex digest of an env file body. Clients send it
// back on save so a stale buffer cannot overwrite a newer file.
func BodySha(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

// ConnectionEditorEnabled reports whether the GUI connection editor is
// enabled. SLING_DISABLE_CONNECTION_EDITOR=1 turns it off, which also disables
// the EnvironmentSet removal guard (a client that does not know about
// allow_removals cannot act on that refusal).
func ConnectionEditorEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SLING_DISABLE_CONNECTION_EDITOR"))) {
	case "1", "true", "yes", "on":
		return false
	}
	return true
}

// RawConnections parses the file at ef.Path into connection prop maps, without
// ${VAR} interpolation. Load/ReadConnections expand refs against the process
// environment; this does not, so callers that hand values to a UI (or write
// them back) can never observe a resolved secret.
func (ef *EnvFile) RawConnections() (map[string]map[string]any, error) {
	root, err := ef.loadRootNode()
	if err != nil {
		return nil, err
	}
	return rawConnectionsFromRoot(root), nil
}

// ParseEnvFileConnections parses an env.yaml body into raw connection prop
// maps, without ${VAR} interpolation.
func ParseEnvFileConnections(body string) (map[string]map[string]any, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(repairEnvYAML([]byte(body)), &root); err != nil {
		return nil, g.Error(err, "could not parse env file body")
	}
	if root.Kind == 0 {
		return map[string]map[string]any{}, nil
	}
	return rawConnectionsFromRoot(&root), nil
}

func rawConnectionsFromRoot(root *yaml.Node) map[string]map[string]any {
	out := map[string]map[string]any{}
	conns := mappingChild(root, "connections")
	if conns == nil {
		return out
	}
	for i := 0; i < len(conns.Content)-1; i += 2 {
		props := map[string]any{}
		if err := conns.Content[i+1].Decode(&props); err != nil {
			// non-mapping entry (e.g. `MY_PG:` with no fields); keep it empty
			props = map[string]any{}
		}
		out[conns.Content[i].Value] = props
	}
	return out
}

// ParseEnvFileKeys parses a raw env.yaml body and returns its connection names
// and env keys (legacy `variables:` included). No ${VAR} interpolation.
func ParseEnvFileKeys(body string) (connNames, envKeys []string, err error) {
	var root yaml.Node
	if err := yaml.Unmarshal(repairEnvYAML([]byte(body)), &root); err != nil {
		return nil, nil, g.Error(err, "could not parse env file body")
	}
	if root.Kind == 0 {
		return nil, nil, nil
	}

	for name := range rawConnectionsFromRoot(&root) {
		connNames = append(connNames, name)
	}
	seen := map[string]struct{}{}
	for _, block := range []string{"env", "variables"} {
		n := mappingChild(&root, block)
		if n == nil {
			continue
		}
		for i := 0; i < len(n.Content)-1; i += 2 {
			seen[n.Content[i].Value] = struct{}{}
		}
	}
	for k := range seen {
		envKeys = append(envKeys, k)
	}
	sort.Strings(connNames)
	sort.Strings(envKeys)
	return connNames, envKeys, nil
}

// RemovedKeys lists connections and env vars (prefixed `env.`) present in
// oldBody but not in newBody. Used to refuse a raw-editor save that would drop
// credentials the previous body had, unless the client confirms.
func RemovedKeys(oldBody, newBody string) ([]string, error) {
	oldConns, oldEnv, err := ParseEnvFileKeys(oldBody)
	if err != nil {
		return nil, g.Error(err, "could not parse current env.yaml")
	}
	newConns, newEnv, err := ParseEnvFileKeys(newBody)
	if err != nil {
		// let the save path produce the parse error
		return nil, nil
	}

	inNew := func(list []string, key string) bool {
		for _, k := range list {
			if strings.EqualFold(k, key) {
				return true
			}
		}
		return false
	}

	var removed []string
	for _, k := range oldConns {
		if !inNew(newConns, k) {
			removed = append(removed, k)
		}
	}
	for _, k := range oldEnv {
		if !inNew(newEnv, k) {
			removed = append(removed, "env."+k)
		}
	}
	sort.Strings(removed)
	return removed, nil
}

// ConnectionNames returns the connection keys present in the raw file at
// ef.Path, sorted.
func (ef *EnvFile) ConnectionNames() ([]string, error) {
	raw, err := ef.RawConnections()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// EnvKeys returns the keys under `env:` (legacy `variables:` included) in the
// raw file at ef.Path, sorted. No ${VAR} interpolation.
func (ef *EnvFile) EnvKeys() ([]string, error) {
	root, err := ef.loadRootNode()
	if err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	for _, block := range []string{"env", "variables"} {
		n := mappingChild(root, block)
		if n == nil {
			continue
		}
		for i := 0; i < len(n.Content)-1; i += 2 {
			seen[n.Content[i].Value] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

// RawEnv returns the `env:` values (legacy `variables:` included) from the raw
// file at ef.Path, without ${VAR} interpolation.
func (ef *EnvFile) RawEnv() (map[string]any, error) {
	root, err := ef.loadRootNode()
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	for _, block := range []string{"env", "variables"} {
		n := mappingChild(root, block)
		if n == nil {
			continue
		}
		for i := 0; i < len(n.Content)-1; i += 2 {
			key := n.Content[i].Value
			var v any
			if err := n.Content[i+1].Decode(&v); err != nil {
				continue
			}
			out[key] = v
		}
	}
	return out, nil
}

// SetConnectionNode merges props into one connection entry of the file at
// ef.Path and saves, through EnvFileEditor: only the changed lines change. It
// never reads ef.Connections, so expanded ${VAR} values held in the struct
// cannot leak to disk.
//
// envUpdates, when non-empty, are written under `env:` in the same save (one
// atomic write; used for secret promotion). Existing env values are replaced:
// callers that must protect hand-set values (EnvFileConns.SetValidated) check
// first.
func (ef *EnvFile) SetConnectionNode(name string, props map[string]any, envUpdates map[string]any) error {
	e, err := LoadEnvEditor(ef.Path)
	if err != nil {
		return err
	}
	if err := e.Set(name, props, EditOptions{AllowOverwrite: true, EnvUpdates: envUpdates, AllowEnvOverwrite: true}); err != nil {
		return err
	}
	return e.Save("")
}

// DeleteConnectionNode removes one connection entry and its head comment from
// the file at ef.Path and saves. Comments after the entry stay.
func (ef *EnvFile) DeleteConnectionNode(name string) error {
	e, err := LoadEnvEditor(ef.Path)
	if err != nil {
		return err
	}
	if err := e.Delete(name); err != nil {
		return err
	}
	return e.Save("")
}

// SetSecretProviderNode replaces one secret_providers entry of the file at
// ef.Path, writes envUpdates under `env:` in the same edit, and saves.
func (ef *EnvFile) SetSecretProviderNode(name string, props, envUpdates map[string]any) error {
	e, err := LoadEnvEditor(ef.Path)
	if err != nil {
		return err
	}
	if len(envUpdates) > 0 {
		if err := e.SetEnv(envUpdates, true); err != nil {
			return err
		}
	}
	if err := e.SetProvider(name, props); err != nil {
		return err
	}
	return e.Save("")
}

// DeleteSecretProviderNode removes one secret_providers entry of the file at
// ef.Path and saves.
func (ef *EnvFile) DeleteSecretProviderNode(name string) error {
	e, err := LoadEnvEditor(ef.Path)
	if err != nil {
		return err
	}
	if err := e.DeleteProvider(name); err != nil {
		return err
	}
	return e.Save("")
}

// SetEnvNodes sets keys under the `env:` block (legacy `variables:` when `env:`
// is absent) of the file at ef.Path and saves. Existing keys are only
// replaced when allowOverwrite is true or the value is unchanged.
func (ef *EnvFile) SetEnvNodes(updates map[string]any, allowOverwrite bool) error {
	e, err := LoadEnvEditor(ef.Path)
	if err != nil {
		return err
	}
	if err := e.SetEnv(updates, allowOverwrite); err != nil {
		return err
	}
	return e.Save("")
}

// effectiveEnvKey returns the block that holds env vars: `env:` when present,
// otherwise the legacy `variables:` block (which loadEnvFile treats as env).
func effectiveEnvKey(root *yaml.Node) string {
	if mappingChild(root, "env") != nil {
		return "env"
	}
	if mappingChild(root, "variables") != nil {
		return "variables"
	}
	return "env"
}

// anyToNode renders any value as a yaml.Node.
func anyToNode(v any) (*yaml.Node, error) {
	b, err := yaml.Marshal(v)
	if err != nil {
		return nil, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) == 0 {
		return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}, nil
	}
	return doc.Content[0], nil
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

// annotateEnvVarRefComments writes envVarRefComment on connection ${VAR}
// scalars that have no trailing comment. Original comments stay.
func annotateEnvVarRefComments(root *yaml.Node) {
	conns := mappingChild(root, "connections")
	if conns == nil {
		return
	}
	annotateMappingRefs(conns)
}

func annotateMappingRefs(n *yaml.Node) {
	n = mappingRoot(n)
	if n == nil {
		return
	}
	for i := 0; i < len(n.Content)-1; i += 2 {
		val := n.Content[i+1]
		switch val.Kind {
		case yaml.ScalarNode:
			if IsEnvVarRef(val.Value) && strings.TrimSpace(val.LineComment) == "" {
				val.LineComment = envVarRefComment
			}
		case yaml.MappingNode:
			annotateMappingRefs(val)
		}
	}
}

// minSecretValueLen skips short values, so "on" or "5432" do not redact log text.
const minSecretValueLen = 4

// secretValues is the set of resolved secret values (from secret references).
// Log output replaces each value with ***, whatever key holds it.
type secretValues struct {
	mu   sync.RWMutex
	set  map[string]struct{}
	vals []string // longest first
}

// resolvedSecrets holds the values that the secret resolver returned.
var resolvedSecrets = &secretValues{set: map[string]struct{}{}}

// AddSecretValue records a resolved secret value for redaction.
func AddSecretValue(v string) { resolvedSecrets.Add(v) }

func (s *secretValues) Add(v string) {
	v = strings.TrimSpace(v)
	if len(v) < minSecretValueLen {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.set[v]; ok {
		return
	}
	s.set[v] = struct{}{}
	s.vals = append(s.vals, v)
	// longest first, so a value that contains another is replaced whole
	sort.Slice(s.vals, func(i, j int) bool { return len(s.vals[i]) > len(s.vals[j]) })
}

func (s *secretValues) Empty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.vals) == 0
}

// Redact replaces each value in line with ***.
func (s *secretValues) Redact(line string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, v := range s.vals {
		if strings.Contains(line, v) {
			line = strings.ReplaceAll(line, v, "***")
		}
	}
	return line
}

// RedactLogLine returns ll, or a copy with the format args applied and the
// values redacted. Map args and caller markers stay: formatters skip them.
func (s *secretValues) RedactLogLine(ll *g.LogLine) *g.LogLine {
	if ll == nil || s.Empty() {
		return ll
	}
	out := *ll
	var formatArgs, keep []any
	for _, arg := range ll.Args {
		switch a := arg.(type) {
		case map[string]any:
			keep = append(keep, arg)
		case string:
			if strings.HasPrefix(a, "_DEBUG_CALLER_START=") {
				keep = append(keep, arg)
				continue
			}
			formatArgs = append(formatArgs, arg)
		default:
			formatArgs = append(formatArgs, arg)
		}
	}
	text := g.F(ll.Text, formatArgs...)
	redacted := s.Redact(text)
	if redacted == text {
		return ll
	}
	out.Text = redacted
	out.Args = keep
	return &out
}

// secretKeyParts mark a key as secret when the key, without "_" and "-",
// contains one of them.
var secretKeyParts = []string{
	"password", "passwd", "passphrase", "secret", "token", "credential",
	"privatekey", "accesskey", "accountkey", "apikey", "keybody", "sastoken",
	"connstr", "connectionstring", "authstring", "authorization",
}

// IsSecretPath tells if a resolved value at a key path is secret. Log output
// shows the other values. A top-level URL can hold a password, thus it is
// secret. All values under `secrets:` are secret. Under `inputs:` (API
// specs), a URL is not secret.
func IsSecretPath(path []string) bool {
	if len(path) == 0 || strings.EqualFold(path[0], "secrets") {
		return true
	}
	key := strings.ToLower(path[len(path)-1])
	words := strings.FieldsFunc(key, func(r rune) bool { return r == '_' || r == '-' || r == '.' })
	joined := strings.Join(words, "")
	for _, part := range secretKeyParts {
		if strings.Contains(joined, part) {
			return true
		}
	}
	for _, w := range words {
		switch w {
		case "key", "dsn":
			return true
		case "url", "uri", "headers", "tunnel":
			if len(path) == 1 {
				return true
			}
		}
	}
	return false
}

// fromKeyAliases maps key names of common secret layouts (AWS RDS rotation
// secrets) to connection keys, for `from:`.
var fromKeyAliases = map[string]string{
	"username": "user",
	"dbname":   "database",
}

// SecretResolver returns the resolver for the secret_providers of the home
// env.yaml and ENV_YAML.
func SecretResolver() (*secrets.Resolver, error) {
	return secretResolvers.For(nil)
}

// SecretResolverFor is SecretResolver with the providers of another env.yaml
// (e.g. a platform project) on top.
func SecretResolverFor(providers map[string]map[string]any) (*secrets.Resolver, error) {
	return secretResolvers.For(providers)
}

var secretResolvers = &secretResolverCache{resolvers: map[string]*secrets.Resolver{}}

// secretResolverCache keeps one resolver per provider config. An edit to
// secret_providers gives a new resolver, without a restart.
type secretResolverCache struct {
	mu        sync.Mutex
	homeStamp string // path, size and mtime of the home env.yaml
	home      map[string]map[string]any
	baseDir   string
	resolvers map[string]*secrets.Resolver // by config hash
}

func (c *secretResolverCache) For(extra map[string]map[string]any) (*secrets.Resolver, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.loadHome()
	raw := map[string]map[string]any{}
	maps.Copy(raw, c.home)
	if content := os.Getenv("ENV_YAML"); content != "" {
		if ef, err := LoadSlingEnvFileBody(content); err == nil {
			maps.Copy(raw, ef.SecretProviders)
		}
	}
	maps.Copy(raw, extra)

	key := g.Marshal(raw) + "|" + c.baseDir // map keys marshal sorted
	if r, ok := c.resolvers[key]; ok {
		return r, nil
	}

	cfg, err := secrets.ParseConfig(raw)
	if err != nil {
		return nil, g.Error(err, "invalid secret_providers")
	}
	r := secrets.NewResolver(secrets.Options{
		Config:     cfg,
		CacheTTL:   5 * time.Minute,
		OnValue:    AddSecretValue,
		IsSecret:   IsSecretPath,
		BaseDir:    c.baseDir,
		KeyAliases: fromKeyAliases,
	})
	c.resolvers[key] = r
	return r, nil
}

// loadHome reads the home env.yaml again only when the file changes.
func (c *secretResolverCache) loadHome() {
	path := GetEnvFilePath(HomeDir)
	stamp := ""
	if fi, err := os.Stat(path); err == nil {
		stamp = g.F("%s|%d|%d", path, fi.Size(), fi.ModTime().UnixNano())
	}
	if stamp == c.homeStamp {
		return
	}
	c.homeStamp, c.home, c.baseDir = stamp, nil, ""
	if stamp != "" {
		c.home = LoadEnvFile(path).SecretProviders
		c.baseDir = filepath.Dir(path)
	}
}

// ResolveSecretEnv returns envMap with its secret references resolved. It is
// for the env: block of a replication or pipeline.
func ResolveSecretEnv(ctx context.Context, envMap map[string]any) (map[string]any, error) {
	if !secrets.HasRef(envMap) {
		return envMap, nil
	}
	r, err := SecretResolver()
	if err != nil {
		return nil, err
	}
	v, err := r.ResolveValue(ctx, envMap)
	if err != nil {
		return nil, g.Error(err, "could not resolve env")
	}
	return v.(map[string]any), nil
}
