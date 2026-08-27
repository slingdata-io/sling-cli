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

// EnvVarRef builds ${<NAME>_<PROP>} for a connection field.
func EnvVarRef(connName, key string) string {
	name := strings.ToUpper(strings.TrimSpace(connName))
	prop := strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(key), "-", "_"))
	return "${" + name + "_" + prop + "}"
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
