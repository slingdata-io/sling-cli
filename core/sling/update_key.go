package sling

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cast"
	"gopkg.in/yaml.v3"
)

// UpdateKey normalizes scalars and slices internally to []string
type UpdateKey []string

// UnmarshalYAML implements the yaml.Unmarshaler interface for gopkg.in/yaml.v3
func (u *UpdateKey) UnmarshalYAML(value *yaml.Node) error {
	switch value.Kind {
	case yaml.ScalarNode:
		var single string
		if err := value.Decode(&single); err != nil {
			return err
		}
		parsed, err := ParseUpdateKey(single)
		if err != nil {
			return err
		}
		*u = parsed
		return nil

	case yaml.SequenceNode:
		var slice []string
		if err := value.Decode(&slice); err != nil {
			return err
		}
		*u = slice
		return u.Normalize()

	default:
		return fmt.Errorf("invalid type for update_key at line %d: expected string or array", value.Line)
	}
}

// MarshalYAML implements the yaml.Marshaler interface
func (u UpdateKey) MarshalYAML() (interface{}, error) {
	if len(u) == 1 {
		return u[0], nil // backwards-compatible output as string when only 1 key is present
	}
	return []string(u), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (u *UpdateKey) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*u = []string{}
		return nil
	}

	if trimmed[0] == '"' {
		var single string
		if err := stdjson.Unmarshal(trimmed, &single); err != nil {
			return err
		}
		parsed, err := ParseUpdateKey(single)
		if err != nil {
			return err
		}
		*u = parsed
		return nil
	}

	if trimmed[0] == '[' {
		var slice []string
		if err := stdjson.Unmarshal(trimmed, &slice); err != nil {
			return err
		}
		*u = slice
		return u.Normalize()
	}

	return fmt.Errorf("invalid type for update_key: expected string or array")
}

// MarshalJSON implements the json.Marshaler interface
func (u UpdateKey) MarshalJSON() ([]byte, error) {
	if len(u) == 1 {
		return stdjson.Marshal(u[0])
	}
	return stdjson.Marshal([]string(u))
}

// Normalize cleans up whitespace, filters out empty entries, treats empty arrays as unset,
// and validates that there are no duplicate column names.
func (u *UpdateKey) Normalize() error {
	if u == nil || len(*u) == 0 {
		if u != nil {
			*u = []string{}
		}
		return nil
	}

	seen := make(map[string]bool)
	cleaned := make([]string, 0, len(*u))

	for _, item := range *u {
		col := strings.TrimSpace(item)
		if col == "" {
			continue
		}
		colLower := strings.ToLower(col)
		if seen[colLower] {
			return fmt.Errorf("duplicate column '%s' in update_key", col)
		}
		seen[colLower] = true
		cleaned = append(cleaned, col)
	}

	*u = cleaned
	return nil
}

// NormalizeAndValidate runs Normalize (trims whitespace, removes empty entries, deduplicates)
// and returns any validation error. Note: this mutates the receiver.
func (u *UpdateKey) NormalizeAndValidate() error {
	return u.Normalize()
}

// First returns the first key or empty string if empty
func (u UpdateKey) First() string {
	if len(u) > 0 {
		return u[0]
	}
	return ""
}

// Columns returns the key columns as []string
func (u UpdateKey) Columns() []string {
	if u == nil {
		return []string{}
	}
	return []string(u)
}

// String returns a comma-delimited representation of the update keys
func (u UpdateKey) String() string {
	return strings.Join(u, ", ")
}

// IsEmpty returns true if there are no keys
func (u UpdateKey) IsEmpty() bool {
	return len(u) == 0
}

// ParseUpdateKey parses various input types (string, comma-separated string, []string, []any) into UpdateKey
func ParseUpdateKey(val any) (UpdateKey, error) {
	var key UpdateKey
	switch v := val.(type) {
	case nil:
		return UpdateKey{}, nil
	case UpdateKey:
		key = make(UpdateKey, len(v))
		copy(key, v)
	case []string:
		key = make(UpdateKey, len(v))
		copy(key, v)
	case string:
		if strings.Contains(v, ",") {
			parts := strings.Split(v, ",")
			key = make(UpdateKey, 0, len(parts))
			for _, p := range parts {
				key = append(key, strings.TrimSpace(p)) // trim each part explicitly
			}
		} else if strings.TrimSpace(v) != "" {
			key = UpdateKey{v}
		} else {
			key = UpdateKey{}
		}
	case []any:
		key = make(UpdateKey, 0, len(v))
		for _, item := range v {
			str := cast.ToString(item)
			key = append(key, str)
		}
	default:
		str := cast.ToString(v)
		if strings.TrimSpace(str) != "" {
			return ParseUpdateKey(str)
		}
		return UpdateKey{}, nil
	}

	if err := key.Normalize(); err != nil {
		return nil, err
	}
	return key, nil
}

