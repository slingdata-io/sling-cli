package validate

import (
	"regexp"
	"strings"
	"sync"

	"github.com/slingdata-io/sling-cli/core/env"
)

var envRefRe = regexp.MustCompile(`^\s*\$\{[A-Za-z_][A-Za-z0-9_]*\}\s*$`)

var (
	secretKeyOnce sync.Once
	secretKeySet  map[string]struct{}
)

// secretKeysLower returns the lowercased set of known secret key names, built once.
func secretKeysLower() map[string]struct{} {
	secretKeyOnce.Do(func() {
		secretKeySet = map[string]struct{}{}
		for _, k := range env.SecretKeys {
			secretKeySet[strings.ToLower(k)] = struct{}{}
		}
		secretKeySet["authentication"] = struct{}{}
	})
	return secretKeySet
}

// isEnvRef reports whether s is exactly a ${VAR} reference.
func isEnvRef(s string) bool {
	return envRefRe.MatchString(s)
}

// Redact replaces secret-shaped values with "***". ${VAR} refs pass through.
func Redact(v any) any {
	return redactValue(v, false)
}

// redactValue walks v recursively, masking strings under force or when the
// parent key is secret-shaped.
func redactValue(v any, force bool) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		keys := secretKeysLower()
		for k, val := range x {
			lk := strings.ToLower(k)
			if lk == "authentication" {
				out[k] = redactAuth(val)
				continue
			}
			if lk == "secrets" {
				out[k] = redactValue(val, true)
				continue
			}
			_, secret := keys[lk]
			out[k] = redactValue(val, force || secret)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = redactValue(item, force)
		}
		return out
	case string:
		if force {
			if isEnvRef(x) || x == "" {
				return x
			}
			return "***"
		}
		return x
	default:
		if force && v != nil {
			if s, ok := v.(string); ok && isEnvRef(s) {
				return s
			}
			return "***"
		}
		return v
	}
}

// redactAuth masks authentication values but keeps the map shape and the
// non-secret "type" field so output stays an object.
func redactAuth(v any) any {
	if s, ok := v.(string); ok && isEnvRef(s) {
		return s
	}
	if v == nil {
		return v
	}
	if m, ok := v.(map[string]any); ok {
		out, _ := redactValue(m, true).(map[string]any)
		for k, val := range m {
			if strings.EqualFold(k, "type") {
				if s, ok := val.(string); ok {
					out[k] = s
				}
			}
		}
		return out
	}
	return "***"
}
