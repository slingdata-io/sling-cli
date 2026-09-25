package env

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/flarco/g"
	cmap "github.com/orcaman/concurrent-map/v2"
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

	Path       string `json:"-" yaml:"-"`
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
		"connections": {}, "variables": {}, "env": {}, "workbench": {},
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
	if rerr != nil && !os.IsNotExist(rerr) {
		return nil, g.Error(rerr, "could not read %s", ef.Path)
	}
	if len(bytes.TrimSpace(data)) == 0 {
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
	if err := yaml.Unmarshal(data, &EnvFile{}); err != nil {
		return g.Error("%s is not valid YAML. Fix it before sling changes the file: %s", ef.Path, g.ErrMsgSimple(err))
	}
	return nil
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
	if err := yaml.Unmarshal([]byte(body), &root); err != nil {
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
	if err := yaml.Unmarshal([]byte(body), &root); err != nil {
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
	if err := yaml.Unmarshal([]byte(body), &root); err != nil {
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

// SetConnectionNode writes one connection's props into the YAML node tree at
// ef.Path and saves. It never reads ef.Connections, so expanded ${VAR} values
// held in the struct cannot leak to disk. Existing scalars under the entry that
// are ${VAR} refs are kept as refs when the incoming value is identical.
//
// envUpdates, when non-empty, are written under `env:` in the same save (one
// atomic write; used for secret promotion). Existing env values are replaced:
// callers that must protect hand-set values (EnvFileConns.SetValidated) check
// first.
func (ef *EnvFile) SetConnectionNode(name string, props map[string]any, envUpdates map[string]any) error {
	root, err := ef.loadRootNode()
	if err != nil {
		return err
	}

	conns := ensureMappingChild(root, "connections")
	valNode, err := anyToNode(props)
	if err != nil {
		return g.Error(err, "could not render connection %s", name)
	}

	keyNode, existingVal := mappingChildFold(conns, name)
	if keyNode == nil {
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name}
		conns.Content = append(conns.Content, key, valNode)
	} else {
		// mergeNode (nil envMap) keeps identical on-disk scalars — including
		// ${VAR} refs — and otherwise takes the incoming value verbatim. Do not
		// pass interpEnvMap here: the write must not depend on the writer's
		// process environment.
		merged := mergeNode(existingVal, valNode, nil)
		for i := 0; i < len(conns.Content)-1; i += 2 {
			if conns.Content[i] == keyNode {
				conns.Content[i+1] = merged
				break
			}
		}
	}

	if len(envUpdates) > 0 {
		if err := setEnvNodes(root, envUpdates, true); err != nil {
			return err
		}
	}

	annotateEnvVarRefComments(root)
	return ef.saveRootNode(root)
}

// DeleteConnectionNode removes one connection entry (and the comments attached
// to it) from the node tree at ef.Path and saves. A trailing comment that is
// attached to the last entry of the `connections:` block is kept: it is moved
// onto the preceding entry (or the block itself when there is none), so
// deleting the last connection does not silently eat it.
func (ef *EnvFile) DeleteConnectionNode(name string) error {
	root, err := ef.loadRootNode()
	if err != nil {
		return err
	}

	conns := mappingChild(root, "connections")
	if conns == nil {
		return g.Error("connections block not found in %s", ef.Path)
	}

	idx := -1
	for i := 0; i < len(conns.Content)-1; i += 2 {
		if strings.EqualFold(conns.Content[i].Value, name) {
			idx = i
			break
		}
	}
	if idx < 0 {
		return g.Error("did not find connection `%s`", name)
	}

	keyNode, valNode := conns.Content[idx], conns.Content[idx+1]
	if idx == len(conns.Content)-2 {
		// last entry: comments attached as FootComment inside its subtree
		// describe the end of the block, not the entry
		notes := collectFootComments(keyNode, valNode)
		if len(notes) > 0 {
			if idx == 0 {
				// no neighbor to carry it: keep it on the block header
				connsKey, _ := mappingChildFold(mappingRoot(root), "connections")
				if connsKey != nil {
					connsKey.FootComment = joinComment(connsKey.FootComment, notes)
				}
			} else {
				target := deepestLastNode(conns.Content[idx-1])
				if target != nil {
					target.FootComment = joinComment(target.FootComment, notes)
				}
			}
		}
	}

	conns.Content = append(conns.Content[:idx], conns.Content[idx+2:]...)
	return ef.saveRootNode(root)
}

// SetEnvNodes sets keys under the `env:` block (legacy `variables:` when `env:`
// is absent) in the node tree at ef.Path and saves. Existing keys are only
// replaced when allowOverwrite is true or the value is unchanged.
func (ef *EnvFile) SetEnvNodes(updates map[string]any, allowOverwrite bool) error {
	root, err := ef.loadRootNode()
	if err != nil {
		return err
	}
	if err := setEnvNodes(root, updates, allowOverwrite); err != nil {
		return err
	}
	return ef.saveRootNode(root)
}

// setEnvNodes splices updates into the effective env block of root.
func setEnvNodes(root *yaml.Node, updates map[string]any, allowOverwrite bool) error {
	if len(updates) == 0 {
		return nil
	}

	blockKey := effectiveEnvKey(root)
	block := ensureMappingChild(root, blockKey)

	existing := map[string]*yaml.Node{}
	for i := 0; i < len(block.Content)-1; i += 2 {
		existing[block.Content[i].Value] = block.Content[i]
	}

	keys := make([]string, 0, len(updates))
	for k := range updates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := ValidateEnvKey(k); err != nil {
			return err
		}
		valNode, err := anyToNode(updates[k])
		if err != nil {
			return g.Error(err, "could not render env var %s", k)
		}
		if keyNode, ok := existing[k]; ok {
			cur := block.Content[indexOfNode(block, keyNode)+1]
			if !allowOverwrite && !nodesEqualScalar(cur, valNode) {
				return g.Error("env var %s already exists in env.yaml; pass allow_overwrite to update it", k)
			}
			block.Content[indexOfNode(block, keyNode)+1] = valNode
			continue
		}
		key := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: k}
		block.Content = append(block.Content, key, valNode)
		existing[k] = key
	}
	return nil
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

// ensureMappingChild returns the mapping value for key, creating it when
// absent.
func ensureMappingChild(root *yaml.Node, key string) *yaml.Node {
	root = mappingRoot(root)
	if root == nil {
		return nil
	}
	if n := mappingChild(root, key); n != nil {
		if n.Kind != yaml.MappingNode {
			n.Kind = yaml.MappingNode
			n.Tag = "!!map"
			n.Value = ""
			n.Content = nil
		}
		return n
	}
	keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
	valNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	root.Content = append(root.Content, keyNode, valNode)
	return valNode
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

// saveRootNode encodes root and writes it to ef.Path.
func (ef *EnvFile) saveRootNode(root *yaml.Node) error {
	if ef.Path == "" {
		return g.Error("env file path is not set")
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(root); err != nil {
		_ = enc.Close()
		return g.Error(err, "could not marshal into YAML")
	}
	if err := enc.Close(); err != nil {
		return g.Error(err, "could not finalize YAML encoder")
	}
	ef.Path = strings.ReplaceAll(ef.Path, `\`, `/`)
	if err := os.WriteFile(ef.Path, buf.Bytes(), 0644); err != nil {
		return g.Error(err, "could not write YAML file")
	}
	return nil
}

// collectFootComments gathers FootComment text from the key node and the value
// subtree, in document order.
func collectFootComments(keyNode, valNode *yaml.Node) []string {
	var out []string
	var walk func(*yaml.Node)
	walk = func(n *yaml.Node) {
		if n == nil {
			return
		}
		if n.FootComment != "" {
			out = append(out, n.FootComment)
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(keyNode)
	walk(valNode)
	return out
}

// deepestLastNode returns the last node in document order under n.
func deepestLastNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	for n.Kind == yaml.MappingNode || n.Kind == yaml.SequenceNode || n.Kind == yaml.DocumentNode {
		if len(n.Content) == 0 {
			return n
		}
		n = n.Content[len(n.Content)-1]
	}
	return n
}

func joinComment(existing string, notes []string) string {
	out := existing
	for _, n := range notes {
		if n == "" {
			continue
		}
		if out == "" {
			out = n
		} else {
			out = out + "\n" + n
		}
	}
	return out
}

func indexOfNode(m *yaml.Node, key *yaml.Node) int {
	for i := 0; i < len(m.Content)-1; i += 2 {
		if m.Content[i] == key {
			return i
		}
	}
	return -1
}

func nodesEqualScalar(a, b *yaml.Node) bool {
	return a.Kind == b.Kind && a.Tag == b.Tag && a.Value == b.Value
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
