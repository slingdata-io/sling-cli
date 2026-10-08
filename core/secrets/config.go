package secrets

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// ProviderConfig is one entry of secret_providers, or a zero-config default.
type ProviderConfig struct {
	Name  string
	Kind  string
	Props map[string]any // ${VAR} already expanded

	baseDir string              // folder of the env.yaml, for relative paths
	timeout time.Duration       // per fetch
	redact  func(string) string // hides resolved values in provider errors
}

// Get returns Props[key], else the first set env var of envKeys.
func (c ProviderConfig) Get(key string, envKeys ...string) string {
	if v, ok := c.Props[key]; ok && v != nil {
		if s := strings.TrimSpace(fmt.Sprint(v)); s != "" {
			return s
		}
	}
	for _, k := range envKeys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// Bool is Get parsed as a boolean.
func (c ProviderConfig) Bool(key string, envKeys ...string) bool {
	switch strings.ToLower(c.Get(key, envKeys...)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// Decode copies Props into a typed struct of the provider (JSON tags).
func (c ProviderConfig) Decode(out any) error {
	b, err := json.Marshal(c.Props)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("invalid config for secret provider %q: %w", c.Name, err)
	}
	return nil
}

// Redact hides known secret values in s. Use it on CLI stderr and HTTP bodies.
func (c ProviderConfig) Redact(s string) string {
	if c.redact == nil {
		return s
	}
	return c.redact(s)
}

// BaseDir is the folder that relative paths resolve against.
func (c ProviderConfig) BaseDir() string { return c.baseDir }

// Config is the secret_providers block.
type Config map[string]ProviderConfig

// ParseConfig reads the raw secret_providers block. Each entry needs a `type`.
func ParseConfig(raw map[string]map[string]any) (Config, error) {
	cfg := Config{}
	names := make([]string, 0, len(raw))
	for name := range raw {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		props := map[string]any{}
		for k, v := range raw[name] {
			props[strings.ToLower(k)] = v
		}
		typ := strings.TrimSpace(fmt.Sprint(props["type"]))
		if props["type"] == nil || typ == "" {
			return nil, fmt.Errorf("secret provider %q has no type", name)
		}
		kind, ok := lookupKind(typ)
		if !ok {
			return nil, fmt.Errorf("secret provider %q has unknown type %q. Known types: %s", name, typ, strings.Join(Kinds(), ", "))
		}
		delete(props, "type")
		for k, v := range props {
			if s, ok := v.(string); ok && ContainsRef(s) {
				return nil, fmt.Errorf("secret provider %q: key %q cannot be a secret reference. Use ${VAR}", name, k)
			}
		}
		cfg[name] = ProviderConfig{Name: name, Kind: kind, Props: props}
	}
	return cfg, nil
}

// Merge returns a copy of c with the entries of other on top.
func (c Config) Merge(other Config) Config {
	out := Config{}
	for k, v := range c {
		out[k] = v
	}
	for k, v := range other {
		out[k] = v
	}
	return out
}

// ofKind returns the entry names of one kind, sorted.
func (c Config) ofKind(kind string) []string {
	var names []string
	for name, pc := range c {
		if pc.Kind == kind {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}
