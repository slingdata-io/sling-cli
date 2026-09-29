package connection

import (
	"context"
	"encoding/json"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/secrets"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// MergeConnProps copies existing and applies incoming. Nested maps merge.
// Keys that incoming does not pass stay on the result.
func MergeConnProps(existing, incoming map[string]any) map[string]any {
	out := copyAnyMap(existing)
	if out == nil {
		out = map[string]any{}
	}
	for k, v := range incoming {
		if vMap := asAnyMap(v); vMap != nil {
			if eMap := asAnyMap(out[k]); eMap != nil {
				out[k] = MergeConnProps(eMap, vMap)
				continue
			}
		}
		out[k] = v
	}
	return out
}

// EnvVarNameOf builds the canonical env var name for a connection field.
func EnvVarNameOf(connName, key string) string {
	name := sanitizeEnvVarPart(connName)
	prop := sanitizeEnvVarPart(key)
	return name + "_" + prop
}

func sanitizeEnvVarPart(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// EnvVarRef builds ${<NAME>_<PROP>} for a connection field.
func EnvVarRef(connName, key string) string {
	return "${" + EnvVarNameOf(connName, key) + "}"
}

// PromoteLiteralSecrets replaces literal secret values in props with ${VAR}
// refs, recording the values to write under `env:` in envUpdates. It returns
// the promoted prop paths, e.g. []string{"password", "secrets.client_id"}.
//
// Secret fields are: top-level keys in env.SecretKeys, and every leaf under a
// nested `secrets` map (same rule RejectLiteralSecrets applies when refusing).
// Values that are already ${VAR} refs are left alone, and so are values that
// equal the expansion of the ref already on disk for that field (existing),
// so retyping an unchanged secret does not add a plaintext copy to env:.
func PromoteLiteralSecrets(connName string, props, existing map[string]any, envUpdates map[string]any) (promoted []string) {
	if props == nil || envUpdates == nil {
		return nil
	}

	for _, k := range env.SecretKeys {
		v, ok := props[k]
		if !ok || !isLiteralSecret(v) {
			continue
		}
		if ref, ok := existingRefFor(existing[k], cast.ToString(v)); ok {
			props[k] = ref
			continue
		}
		envUpdates[EnvVarNameOf(connName, k)] = cast.ToString(v)
		props[k] = EnvVarRef(connName, k)
		promoted = append(promoted, k)
	}

	secrets := asAnyMap(props["secrets"])
	if secrets == nil {
		return promoted
	}
	existingSecrets := asAnyMap(existing["secrets"])
	keys := lo.Keys(secrets)
	sort.Strings(keys)
	for _, k := range keys {
		if !isLiteralSecret(secrets[k]) {
			continue
		}
		if ref, ok := existingRefFor(existingSecrets[k], cast.ToString(secrets[k])); ok {
			secrets[k] = ref
			continue
		}
		envKey := EnvVarNameOf(connName, k)
		if _, taken := envUpdates[envKey]; taken {
			// leaf name collides with another promoted secret; use the path
			envKey = EnvVarNameOf(connName, "secrets."+k)
		}
		envUpdates[envKey] = cast.ToString(secrets[k])
		secrets[k] = "${" + envKey + "}"
		promoted = append(promoted, "secrets."+k)
	}
	return promoted
}

// existingRefFor returns the on-disk ref for a field when literal equals that
// ref's expansion.
func existingRefFor(existing any, literal string) (string, bool) {
	ref, ok := existing.(string)
	if !ok || !env.IsEnvVarRef(ref) {
		return "", false
	}
	if literal == "" || env.ExpandRef(ref) != literal {
		return "", false
	}
	return ref, true
}

// PreserveRefs keeps an on-disk ${VAR} ref for any incoming literal value that
// equals that ref's expansion. The GUI never receives expanded values, but the
// CLI contract does allow setting a props map with resolved values; keeping the
// ref keeps the file free of plaintext secrets.
func PreserveRefs(existing, incoming map[string]any) {
	if existing == nil || incoming == nil {
		return
	}
	keys := lo.Keys(incoming)
	sort.Strings(keys)
	for _, k := range keys {
		if nested := asAnyMap(incoming[k]); nested != nil {
			PreserveRefs(asAnyMap(existing[k]), nested)
			continue
		}
		oldRef, ok := existing[k].(string)
		if !ok || !env.IsEnvVarRef(oldRef) {
			continue
		}
		val, ok := incoming[k].(string)
		if !ok || env.IsEnvVarRef(val) {
			continue
		}
		if env.ExpandRef(oldRef) == val {
			incoming[k] = oldRef
		}
	}
}

// NormalizeConnProps parses secrets/inputs YAML strings into maps.
func NormalizeConnProps(kv map[string]any) error {
	if kv == nil {
		return nil
	}
	for _, field := range []string{"secrets", "inputs"} {
		v, ok := kv[field]
		if !ok {
			continue
		}
		s, isStr := v.(string)
		if !isStr || strings.TrimSpace(s) == "" {
			continue
		}
		parsed, err := g.UnmarshalYAMLMap(s)
		if err != nil {
			return g.Error(err, "could not parse %s string", field)
		}
		kv[field] = parsed
	}
	return nil
}

// RejectLiteralSecrets refuses secret fields (and nested secrets values)
// whose value is not a ${VAR} ref.
func RejectLiteralSecrets(name string, kv map[string]any) error {
	if kv == nil {
		return nil
	}
	if err := NormalizeConnProps(kv); err != nil {
		return err
	}

	var literals []string
	for _, k := range env.SecretKeys {
		if v, ok := kv[k]; ok && isLiteralSecret(v) {
			literals = append(literals, k)
		}
	}

	// nested secrets are keyed separately, so they cannot repeat the above
	if secrets := asAnyMap(kv["secrets"]); secrets != nil {
		nested := lo.Keys(secrets)
		sort.Strings(nested)
		for _, k := range nested {
			if isLiteralSecret(secrets[k]) {
				literals = append(literals, "secrets."+k)
			}
		}
	}

	if len(literals) == 0 {
		return nil
	}
	example := EnvVarRef(name, "PASSWORD")
	return g.Error("secret field(s) %s must be an env-var ref such as %s or a secret reference such as op://vault/item/field. Do not pass secret values.", strings.Join(literals, ", "), example)
}

func isLiteralSecret(v any) bool {
	if v == nil {
		return false
	}
	if asAnyMap(v) != nil {
		// nested map: checked by the caller per-key
		return false
	}
	s := strings.TrimSpace(cast.ToString(v))
	if s == "" {
		return false
	}
	return !env.IsEnvVarRef(s) && !secrets.IsRef(s)
}

// ValidateConnProps checks that props can be written as a connection entry:
// a `url` parses (and implies the type when absent), or a valid `type` is
// present. SetValidated applies it before the write; the GUI save path, which
// writes through its own env editor, calls it directly.
func ValidateConnProps(name string, props map[string]any) error {
	// parse url
	if url := cast.ToString(props["url"]); url != "" {
		conn, uErr := NewConnectionFromURL(name, url)
		if uErr != nil {
			return g.Error(uErr, "could not parse url")
		}
		if _, ok := props["type"]; !ok {
			props["type"] = conn.Type.String()
		}
	}

	t, found := props["type"]
	if _, typeOK := dbio.ValidateType(cast.ToString(t)); found && !typeOK {
		return g.Error("invalid type (%s)", cast.ToString(t))
	} else if !found {
		return g.Error("need to specify valid `type` key or provide `url`")
	}
	return nil
}

// UnsetEnvRef is a ${VAR} value that g.Rmd did not substitute (var not set).
type UnsetEnvRef struct {
	Key string
	Var string
}

// FindUnsetEnvRefs walks connection data for whole-string ${VAR} values.
func FindUnsetEnvRefs(data map[string]any) []UnsetEnvRef {
	var out []UnsetEnvRef
	walkUnsetRefs(data, "", &out)
	return out
}

func walkUnsetRefs(v any, prefix string, out *[]UnsetEnvRef) {
	if v == nil {
		return
	}
	if m := asAnyMap(v); m != nil {
		keys := lo.Keys(m)
		// stable-ish: not required, but keeps errors readable
		for _, k := range keys {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			walkUnsetRefs(m[k], path, out)
		}
		return
	}
	s := strings.TrimSpace(cast.ToString(v))
	if !env.IsEnvVarRef(s) {
		return
	}
	*out = append(*out, UnsetEnvRef{Key: prefix, Var: env.EnvVarRefName(s)})
}

// FormatUnsetRefError names each unset var and its env.yaml line.
func FormatUnsetRefError(refs []UnsetEnvRef, loc env.ConnLocation) error {
	file := filepath.Base(loc.Path)
	if file == "" || file == "." {
		file = "env.yaml"
	}
	lineFor := func(r UnsetEnvRef) int {
		for _, m := range loc.Missing {
			if m.Var == r.Var || m.Key == r.Key {
				return m.Line
			}
		}
		return 0
	}
	msgs := make([]string, 0, len(refs))
	for _, r := range refs {
		if line := lineFor(r); line > 0 {
			msgs = append(msgs, g.F("env var %s is not set (%s:%d)", r.Var, file, line))
		} else {
			msgs = append(msgs, g.F("env var %s is not set", r.Var))
		}
	}
	return g.Error(strings.Join(msgs, "; "))
}

// ScrubConnProps returns key names and ref/status only. No secret values.
func ScrubConnProps(kv map[string]any) []map[string]any {
	out := []map[string]any{}
	keys := lo.Keys(kv)
	sort.Strings(keys)
	for _, k := range keys {
		v := kv[k]
		if nested := asAnyMap(v); nested != nil {
			nKeys := lo.Keys(nested)
			sort.Strings(nKeys)
			for _, nk := range nKeys {
				out = append(out, scrubEntry(k+"."+nk, nested[nk]))
			}
			continue
		}
		out = append(out, scrubEntry(k, v))
	}
	return out
}

func scrubEntry(key string, v any) map[string]any {
	s := strings.TrimSpace(cast.ToString(v))
	entry := g.M("key", key)
	if env.IsEnvVarRef(s) {
		entry["ref"] = s
		return entry
	}
	entry["set"] = s != ""
	return entry
}

// ParsePropsInput reads a YAML or JSON property map from stdin/payload text.
func ParsePropsInput(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, g.Error("stdin is empty")
	}
	var m map[string]any
	if strings.HasPrefix(raw, "{") {
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, g.Error(err, "could not parse JSON properties")
		}
		return lowercaseKeys(m), nil
	}
	if err := yaml.Unmarshal([]byte(raw), &m); err != nil {
		return nil, g.Error(err, "could not parse YAML properties")
	}
	return lowercaseKeys(m), nil
}

func lowercaseKeys(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	for k, v := range m {
		key := strings.ToLower(k)
		if nested := asAnyMap(v); nested != nil {
			out[key] = lowercaseKeys(nested)
			continue
		}
		out[key] = v
	}
	return out
}

func copyAnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if nested := asAnyMap(v); nested != nil {
			out[k] = copyAnyMap(nested)
			continue
		}
		out[k] = v
	}
	return out
}

func asAnyMap(v any) map[string]any {
	switch t := v.(type) {
	case map[string]any:
		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[cast.ToString(k)] = val
		}
		return out
	case map[string]string:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = val
		}
		return out
	default:
		return nil
	}
}

// The canonical key order of new connection entries written to env.yaml:
// `type` first, then the property order of core/dbio/templates/_properties.yaml
// for the entry's type, then the remaining keys (alphabetically, applied by the
// env package when this registration is absent). The hook lives in core/env,
// which cannot import core/dbio; registering it here means every binary that
// reads connections (the CLI, the platform agent, the workbench) writes new
// entries in the order the templates define.
func init() {
	env.TemplateKeyOrder = templateKeyOrder
}

var (
	templateOrderOnce sync.Once
	templateOrder     map[string][]string
)

// templateKeyOrder returns the template property order of props["type"], or
// nil when the type is unknown.
func templateKeyOrder(props map[string]any) []string {
	connType := strings.ToLower(cast.ToString(props["type"]))
	if connType == "" {
		return nil
	}
	templateOrderOnce.Do(loadTemplateOrder)
	return templateOrder[connType]
}

// loadTemplateOrder reads the property order of every type in
// _properties.yaml once. Order matters: the file is parsed as a node tree,
// because a map parse would lose it.
func loadTemplateOrder() {
	templateOrder = map[string][]string{}
	body, err := dbio.ReadTemplateFile("_properties.yaml")
	if err != nil {
		return
	}
	var root yaml.Node
	if err := yaml.Unmarshal(body, &root); err != nil {
		return
	}
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return
	}
	types := root.Content[0]
	for i := 0; i < len(types.Content)-1; i += 2 {
		name := strings.ToLower(types.Content[i].Value)
		typeNode := types.Content[i+1]
		if typeNode.Kind != yaml.MappingNode {
			continue
		}
		for j := 0; j < len(typeNode.Content)-1; j += 2 {
			if typeNode.Content[j].Value != "properties" || typeNode.Content[j+1].Kind != yaml.MappingNode {
				continue
			}
			props := typeNode.Content[j+1]
			order := make([]string, 0, len(props.Content)/2)
			for k := 0; k < len(props.Content)-1; k += 2 {
				order = append(order, props.Content[k].Value)
			}
			templateOrder[name] = order
			break
		}
	}
}

func init() {
	secrets.Register("sling", func(secrets.ProviderConfig) (secrets.Provider, error) {
		return slingProvider{}, nil
	})

	iop.ResolveSecret = func(ref string) (string, error) {
		if !secrets.IsRef(ref) {
			return "", g.Error("not a secret reference: use a form such as op://vault/item/field or ref+awssecrets://name#/key")
		}
		r, err := env.SecretResolver()
		if err != nil {
			return "", err
		}
		v, err := r.Resolve(context.Background(), ref)
		if err != nil {
			return "", err
		}
		return cast.ToString(v), nil
	}
}

// HasSecretRef is true when the connection data holds a secret reference or `from:`.
func (c *Connection) HasSecretRef() bool {
	_, hasFrom := c.Data["from"]
	return hasFrom || secrets.HasRef(c.Data)
}

// Resolved returns c when c.Data has no secret reference. Else it returns a
// copy with resolved data and a rebuilt URL. c.Data keeps the references.
func (c *Connection) Resolved(ctx context.Context) (*Connection, error) {
	if !c.HasSecretRef() {
		return c, nil
	}
	r, err := env.SecretResolver()
	if err != nil {
		return nil, err
	}

	data := maps.Clone(c.Data)
	if c.derivedURL() {
		delete(data, "url") // setURL builds it again from the resolved values
	}
	out, err := r.ResolveEntry(ctx, data)
	if err != nil {
		return nil, err
	}

	t := c.Type
	if t.IsUnknown() {
		t = SchemeType(cast.ToString(out["url"]))
		for _, key := range []string{"type", "engine"} { // engine: AWS RDS secrets
			if kt, ok := dbio.ValidateType(cast.ToString(out[key])); ok {
				t = kt
				break
			}
		}
	}

	nc, err := NewConnection(c.Name, t, out)
	if err != nil {
		return nil, g.Error(err, "could not build connection %s from resolved secrets", c.Name)
	}
	nc.context = c.context
	return &nc, nil
}

// ResolveType sets the type of a connection with references and no `type`.
// It reads the secrets, so call it only when the connection is used.
func (c *Connection) ResolveType(ctx context.Context) error {
	if !c.Type.IsUnknown() || !c.HasSecretRef() {
		return nil
	}
	rc, err := c.Resolved(ctx)
	if err != nil {
		return err
	}
	if rc.Type.IsUnknown() {
		return g.Error("could not find the type of connection %s. Add `type` to the connection, or a `type` key to the secret", c.Name)
	}
	c.Type = rc.Type
	return nil
}

// derivedURL is true when setURL built c.Data["url"] from other keys that
// hold references. That URL contains reference text and must be built again.
func (c *Connection) derivedURL() bool {
	u := cast.ToString(c.Data["url"])
	if u == "" {
		return false
	}
	for k, v := range c.Data {
		s, ok := v.(string)
		if k == "url" || !ok || !secrets.ContainsRef(s) {
			continue
		}
		escaped := strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
		if strings.Contains(u, s) || strings.Contains(u, escaped) {
			return true
		}
	}
	return false
}

// slingProvider reads values that sling already has:
//
//	ref+sling://connections/<name>[/<field>[/<nested>...]]
//	ref+sling://env/<KEY>
//
// A connection with no field comes back as JSON, so `from:` can copy it.
// References in the value resolve too.
type slingProvider struct{}

func (p slingProvider) Get(ctx context.Context, ref secrets.Ref) ([]byte, error) {
	scope, rest, _ := strings.Cut(strings.Trim(ref.Path, "/"), "/")
	if rest == "" {
		return nil, g.Error("use ref+sling://connections/<name>/<field> or ref+sling://env/<KEY>")
	}

	switch strings.ToLower(scope) {
	case "connections":
		return p.connection(ctx, rest)
	case "env":
		return p.env(ctx, rest)
	default:
		return nil, g.Error("unknown sling scope %q. Use connections or env", scope)
	}
}

// connection returns a field of the local connection, or the whole connection.
func (slingProvider) connection(ctx context.Context, path string) ([]byte, error) {
	name, field, _ := strings.Cut(path, "/")
	entry := GetLocalConns().Get(name)
	if entry.Name == "" {
		return nil, g.Error("did not find connection %s", name)
	}
	conn, err := entry.Connection.Resolved(ctx)
	if err != nil {
		return nil, err
	}

	data := maps.Clone(conn.Data)
	data["url"] = conn.URL()
	delete(data, "from")
	if field == "" {
		return json.Marshal(data)
	}

	key, nested, _ := strings.Cut(field, "/")
	var value any
	found := false
	for k, v := range data {
		if strings.EqualFold(k, key) {
			value, found = v, true
			break
		}
	}
	if !found {
		return nil, g.Error("connection %s has no field %q", entry.Name, key)
	}

	if nested != "" {
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if value, err = (secrets.Ref{Pointer: "/" + nested}).Select(b); err != nil {
			return nil, err
		}
	}
	if s, ok := value.(string); ok {
		return []byte(s), nil
	}
	return json.Marshal(value)
}

// env returns a process env var (this includes the env: of env.yaml).
func (slingProvider) env(ctx context.Context, key string) ([]byte, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return nil, g.Error("env var %s is not set", key)
	}
	if !secrets.HasRef(value) {
		return []byte(value), nil
	}
	r, err := env.SecretResolver()
	if err != nil {
		return nil, err
	}
	resolved, err := r.ResolveValue(ctx, value)
	if err != nil {
		return nil, err
	}
	return []byte(cast.ToString(resolved)), nil
}
