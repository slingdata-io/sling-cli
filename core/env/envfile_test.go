package env

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseDotEnv(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected map[string]string
	}{
		{
			name:    "simple key=value",
			content: "FOO=bar",
			expected: map[string]string{
				"FOO": "bar",
			},
		},
		{
			name:    "single-line single-quoted JSON",
			content: `KEY='{"a": "b"}'`,
			expected: map[string]string{
				"KEY": `{"a": "b"}`,
			},
		},
		{
			name:    "single-line double-quoted JSON",
			content: `KEY="{\"a\": \"b\"}"`,
			expected: map[string]string{
				"KEY": `{\"a\": \"b\"}`,
			},
		},
		{
			name: "multi-line single-quoted JSON",
			content: `KEY='{
  "a": "b"
}'`,
			expected: map[string]string{
				"KEY": "{\n  \"a\": \"b\"\n}",
			},
		},
		{
			name: "multi-line double-quoted value",
			content: "KEY=\"hello\nworld\"",
			expected: map[string]string{
				"KEY": "hello\nworld",
			},
		},
		{
			name: "multi-line with multiple keys",
			content: `BEFORE=hello
JSON_VAL='{
  "key": "value",
  "num": 42
}'
AFTER=world`,
			expected: map[string]string{
				"BEFORE":   "hello",
				"JSON_VAL": "{\n  \"key\": \"value\",\n  \"num\": 42\n}",
				"AFTER":    "world",
			},
		},
		{
			name: "comments and blank lines are skipped",
			content: `# this is a comment
FOO=bar

# another comment
BAZ=qux`,
			expected: map[string]string{
				"FOO": "bar",
				"BAZ": "qux",
			},
		},
		{
			name:    "value with equals sign",
			content: `CONN=postgres://user:pass@host/db?sslmode=require`,
			expected: map[string]string{
				"CONN": "postgres://user:pass@host/db?sslmode=require",
			},
		},
		{
			name: "multi-line with nested braces",
			content: `CONFIG='{
  "database": {
    "host": "localhost",
    "port": 5432
  }
}'`,
			expected: map[string]string{
				"CONFIG": "{\n  \"database\": {\n    \"host\": \"localhost\",\n    \"port\": 5432\n  }\n}",
			},
		},
		{
			name:    "unquoted value",
			content: `KEY=some value here`,
			expected: map[string]string{
				"KEY": "some value here",
			},
		},
		{
			name:    "empty value",
			content: `KEY=`,
			expected: map[string]string{
				"KEY": "",
			},
		},
		{
			name: "double-quoted value with single quotes inside",
			content: `KEY="{'a': 'b'}"`,
			expected: map[string]string{
				"KEY": "{'a': 'b'}",
			},
		},
		{
			name: "multi-line double-quoted with single quotes inside",
			content: "KEY=\"{\n  'a': 'b'\n}\"",
			expected: map[string]string{
				"KEY": "{\n  'a': 'b'\n}",
			},
		},
		{
			name:    "line without equals is skipped",
			content: "NOPE",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseDotEnv(tt.content)
			assert.Equal(t, tt.expected, result)
		})
	}
}
