package validate

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/spf13/cast"
)

// Shared value-coercion helpers used across parsing and rendering.

// asMap coerces v to map[string]any, converting map[any]any keys when needed.
func asMap(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[cast.ToString(k)] = val
		}
		return out, true
	default:
		return nil, false
	}
}

// asMapOrEmpty is asMap but returns an empty map instead of false on mismatch.
func asMapOrEmpty(v any) map[string]any {
	if m, ok := asMap(v); ok {
		return m
	}
	return map[string]any{}
}

// asSlice coerces v to []any, wrapping []string items when needed.
func asSlice(v any) []any {
	switch t := v.(type) {
	case []any:
		return t
	case []string:
		out := make([]any, len(t))
		for i, s := range t {
			out[i] = s
		}
		return out
	}
	return nil
}

// toStringSlice coerces a list or scalar to []string. Blank strings yield nil.
func toStringSlice(v any) []string {
	switch t := v.(type) {
	case []string:
		return t
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			out = append(out, cast.ToString(item))
		}
		return out
	case string:
		if strings.TrimSpace(t) == "" {
			return nil
		}
		return []string{t}
	}
	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// firstNonEmpty returns the first value that is not blank after trimming.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// dtoToMap round-trips a DTO through JSON so nested values become plain maps.
func dtoToMap(v any) map[string]any {
	b, err := json.Marshal(v)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		return map[string]any{"error": err.Error()}
	}
	return m
}
