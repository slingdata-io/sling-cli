package connection

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
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
// Values that are already ${VAR} refs are left alone.
func PromoteLiteralSecrets(connName string, props map[string]any, envUpdates map[string]any) (promoted []string) {
	if props == nil || envUpdates == nil {
		return nil
	}

	for _, k := range env.SecretKeys {
		v, ok := props[k]
		if !ok || !isLiteralSecret(v) {
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
	keys := lo.Keys(secrets)
	sort.Strings(keys)
	for _, k := range keys {
		if !isLiteralSecret(secrets[k]) {
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
	return g.Error("secret field(s) %s must be an env-var ref such as %s. Do not pass secret values.", strings.Join(literals, ", "), example)
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
	return !env.IsEnvVarRef(s)
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
