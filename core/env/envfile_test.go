package env

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// TestWriteEnvFilePreservesComments verifies that mutating Connections and
// writing back via WriteEnvFile keeps user comments and unrelated top-level
// keys intact. This is the analogue of the AI-block preservation test: same
// Node graft path, exercised through the conns set/unset code path.
func TestWriteEnvFilePreservesComments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")

	original := `# Sling environment file — managed by you.
# These connections move data between systems.

connections:
  # Production warehouse
  PG_PROD:
    type: postgres
    host: db.example.com
    user: app
  # Staging warehouse
  PG_STAGE:
    type: postgres
    host: stage.db.example.com

# Variables shared across runs
variables:
  region: us-west-2

# Custom block we don't manage — must survive untouched.
custom_section:
  retain: yes
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := LoadEnvFile(path)
	ef.Connections["NEW_PG"] = map[string]any{
		"type": "postgres",
		"host": "new.db.example.com",
		"user": "app",
	}
	if err := ef.WriteEnvFile(); err != nil {
		t.Fatalf("WriteEnvFile: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)

	wantSubstrings := []string{
		"# Sling environment file — managed by you.",
		"# These connections move data between systems.",
		"# Production warehouse",
		"# Staging warehouse",
		"# Custom block we don't manage — must survive untouched.",
		"custom_section:",
		"retain: yes",
		"PG_PROD:",
		"PG_STAGE:",
		"NEW_PG:",
		"region: us-west-2",
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(out, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, out)
		}
	}
	// Legacy `variables:` block migrates to `env:` on save; the block contents
	// survive but the heading comment attached to the renamed key does not.
	if strings.Contains(out, "variables:") {
		t.Errorf("expected legacy variables: block to be renamed to env:\n--- got ---\n%s", out)
	}
	if !strings.Contains(out, "env:") {
		t.Errorf("expected env: block after legacy migration\n--- got ---\n%s", out)
	}
}

// TestWriteEnvFilePreservesInnerConnectionComments verifies that comments
// *inside* a connection mapping (above and trailing individual fields) survive
// mutating an unrelated field. This is the harder case the recursive merge is
// designed for: per-field HeadComment/LineComment inside nested mappings.
func TestWriteEnvFilePreservesInnerConnectionComments(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")

	original := `connections:
  PG_PROD:
    type: postgres
    # primary writer
    host: db.example.com # main DSN
    user: app
    # rotated quarterly
    password: ${PG_PASSWORD}
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := LoadEnvFile(path)
	// Mutate an unrelated field — the user changes.
	ef.Connections["PG_PROD"]["user"] = "app_v2"
	if err := ef.WriteEnvFile(); err != nil {
		t.Fatalf("WriteEnvFile: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)

	for _, sub := range []string{
		"# primary writer",
		"# main DSN",
		"# rotated quarterly",
		"user: app_v2",
	} {
		if !strings.Contains(out, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, out)
		}
	}
}

// TestWriteEnvFileAssistRoundTrip verifies that the assist profile, stored as
// env.SLING_ASSIST (an inline map), gets serialized into env: and round-trips
// cleanly across Load/Save. The assist package owns the typed shape; from
// EnvFile's POV it's just a nested map under env.
func TestWriteEnvFileAssistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")

	ef := LoadEnvFile(path)
	ef.Path = path
	if ef.Env == nil {
		ef.Env = map[string]any{}
	}
	ef.Env["SLING_ASSIST"] = map[string]any{
		"agent":          "claude",
		"hint_in_errors": true,
	}
	if err := ef.WriteEnvFile(); err != nil {
		t.Fatalf("WriteEnvFile: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)

	for _, sub := range []string{"env:", "SLING_ASSIST:", "agent: claude", "hint_in_errors: true"} {
		if !strings.Contains(out, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, out)
		}
	}

	// Round-trip: load again, mutate, save, ensure env: stays a single block.
	ef2 := LoadEnvFile(path)
	raw, ok := ef2.Env["SLING_ASSIST"]
	if !ok {
		t.Fatalf("expected env.SLING_ASSIST after reload, got %+v", ef2.Env)
	}
	m, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("expected env.SLING_ASSIST to be map[string]any, got %T", raw)
	}
	if m["agent"] != "claude" {
		t.Fatalf("expected agent=claude after reload, got %+v", m)
	}
	m["hint_in_errors"] = false
	ef2.Env["SLING_ASSIST"] = m
	if err := ef2.WriteEnvFile(); err != nil {
		t.Fatalf("second WriteEnvFile: %v", err)
	}
	got, _ = os.ReadFile(path)
	out = string(got)
	// Match the start-of-line env: header, regardless of whether it's the very
	// first line (no preceding newline) or further down.
	headers := strings.Count(out, "\nenv:") + boolToInt(strings.HasPrefix(out, "env:"))
	if headers != 1 {
		t.Errorf("expected exactly one env: block, got\n%s", out)
	}
	if !strings.Contains(out, "hint_in_errors: false") {
		t.Errorf("SLING_ASSIST block did not update on second save\n--- got ---\n%s", out)
	}
}

// TestWriteEnvFileKeepsOnDiskEnvRefs fails if write materializes a ${VAR}
// that loadEnvFile interpolated from the process environment.
func TestWriteEnvFileKeepsOnDiskEnvRefs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	const leak = "hunter2-LEAK-TEST"
	t.Setenv("MY_PG_PASSWORD", leak)

	original := `connections:
  MY_PG:
    type: postgres
    host: localhost
    password: ${MY_PG_PASSWORD}
    port: 5432
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := LoadEnvFile(path)
	gotPass, _ := ef.Connections["MY_PG"]["password"].(string)
	if gotPass != leak {
		t.Fatalf("load should interpolate password, got %q", gotPass)
	}

	ef.Connections["MY_PG"]["port"] = "5433"
	if err := ef.WriteEnvFile(); err != nil {
		t.Fatalf("WriteEnvFile: %v", err)
	}

	got, _ := os.ReadFile(path)
	out := string(got)
	if !strings.Contains(out, "${MY_PG_PASSWORD}") {
		t.Errorf("expected on-disk ${MY_PG_PASSWORD} ref\n--- got ---\n%s", out)
	}
	if strings.Contains(out, leak) {
		t.Errorf("merge wrote interpolated secret %q\n--- got ---\n%s", leak, out)
	}
	if !strings.Contains(out, "5433") {
		t.Errorf("expected updated port\n--- got ---\n%s", out)
	}
}

func TestLookupConnectionLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  MY_PG:
    type: postgres
    host: localhost
    password: ${MY_PG_PASSWORD}
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := LoadEnvFile(path)
	loc, err := ef.LookupConnection("MY_PG")
	if err != nil {
		t.Fatalf("LookupConnection: %v", err)
	}
	if loc.Path != path {
		t.Errorf("path: got %q want %q", loc.Path, path)
	}
	if loc.Connection != "MY_PG" {
		t.Errorf("connection: got %q", loc.Connection)
	}
	if loc.Line != 2 {
		t.Errorf("connection line: got %d want 2\n%s", loc.Line, original)
	}
	if len(loc.Missing) != 1 {
		t.Fatalf("missing: got %#v", loc.Missing)
	}
	if loc.Missing[0].Key != "password" {
		t.Errorf("missing key: got %q", loc.Missing[0].Key)
	}
	if loc.Missing[0].Var != "MY_PG_PASSWORD" {
		t.Errorf("missing var: got %q", loc.Missing[0].Var)
	}
	if loc.Missing[0].Line != 5 {
		t.Errorf("missing line: got %d want 5", loc.Missing[0].Line)
	}
}

func TestIsEnvVarRef(t *testing.T) {
	if !IsEnvVarRef("${MY_PG_PASSWORD}") {
		t.Error("expected ${MY_PG_PASSWORD} to be a ref")
	}
	if IsEnvVarRef("${my_pg_password}") {
		t.Error("lowercase var names are not refs")
	}
	if IsEnvVarRef("mypass") {
		t.Error("plaintext is not a ref")
	}
	if EnvVarRefName("${MY_PG_PASSWORD}") != "MY_PG_PASSWORD" {
		t.Error("EnvVarRefName")
	}
}

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
			name:    "multi-line double-quoted value",
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
			name:    "double-quoted value with single quotes inside",
			content: `KEY="{'a': 'b'}"`,
			expected: map[string]string{
				"KEY": "{'a': 'b'}",
			},
		},
		{
			name:    "multi-line double-quoted with single quotes inside",
			content: "KEY=\"{\n  'a': 'b'\n}\"",
			expected: map[string]string{
				"KEY": "{\n  'a': 'b'\n}",
			},
		},
		{
			name:     "line without equals is skipped",
			content:  "NOPE",
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

func TestMergeDeclaredEnv(t *testing.T) {
	t.Setenv("MERGE_DECLARED_PROBE", "from-process")
	t.Setenv("MERGE_DECLARED_OVERRIDE", "from-process")

	got := MergeDeclaredEnv(map[string]any{
		"MERGE_DECLARED_OVERRIDE": "from-yaml",
		"MERGE_DECLARED_ONLY":     7,
	})

	assert.Equal(t, "from-process", got["MERGE_DECLARED_PROBE"])
	assert.Equal(t, "from-yaml", got["MERGE_DECLARED_OVERRIDE"])
	assert.Equal(t, 7, got["MERGE_DECLARED_ONLY"])

	empty := MergeDeclaredEnv(nil)
	assert.Equal(t, "from-process", empty["MERGE_DECLARED_PROBE"])
	assert.NotContains(t, empty, "MERGE_DECLARED_ONLY")
}
