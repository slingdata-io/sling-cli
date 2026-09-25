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

func TestValidateKeyAndEnvKey(t *testing.T) {
	for _, key := range []string{"MY_PG", "my-pg", "_PRIVATE", "PG1"} {
		if err := ValidateKey(key); err != nil {
			t.Errorf("ValidateKey(%q) unexpected error: %v", key, err)
		}
	}
	for _, key := range []string{"", " MY_PG", "MY_PG ", "MY PG", "1PG", "MY.PG"} {
		if err := ValidateKey(key); err == nil {
			t.Errorf("ValidateKey(%q) expected error", key)
		}
	}

	for _, key := range []string{"MY_PG_PASSWORD", "_X", "A1"} {
		if err := ValidateEnvKey(key); err != nil {
			t.Errorf("ValidateEnvKey(%q) unexpected error: %v", key, err)
		}
	}
	for _, key := range []string{"", "my_var", "MY-VAR", "MY.VAR", "1VAR"} {
		if err := ValidateEnvKey(key); err == nil {
			t.Errorf("ValidateEnvKey(%q) expected error", key)
		}
	}
}

func TestSetConnectionNodePreservesRestOfFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `# Sling environment file — managed by you.

connections:
  # Production warehouse
  PG_PROD:
    type: postgres
    host: db.example.com
    user: app
  PG_STAGE:
    type: postgres
    host: stage.db.example.com

# Variables shared across runs
env:
  region: us-west-2

# Custom block — must survive untouched.
custom_section:
  retain: yes
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	err := ef.SetConnectionNode("PG_PROD", map[string]any{
		"type": "postgres",
		"host": "new.db.example.com",
		"user": "app",
		"port": 5432,
	}, map[string]any{"PG_PROD_TOKEN": "tok"})
	if err != nil {
		t.Fatalf("SetConnectionNode: %v", err)
	}

	out, _ := os.ReadFile(path)
	got := string(out)

	for _, sub := range []string{
		"# Sling environment file — managed by you.",
		"# Production warehouse",
		"PG_STAGE:",
		"stage.db.example.com",
		"# Variables shared across runs",
		"region: us-west-2",
		"# Custom block — must survive untouched.",
		"custom_section:",
		"host: new.db.example.com",
		"port: 5432",
		"PG_PROD_TOKEN: tok",
	} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, got)
		}
	}
	if strings.Contains(got, "host: db.example.com") {
		t.Errorf("old host value survived\n--- got ---\n%s", got)
	}

	// every original line except the edited field must survive verbatim, in order
	j := 0
	outLines := strings.Split(got, "\n")
	for _, line := range strings.Split(original, "\n") {
		if strings.TrimSpace(line) == "" || strings.Contains(line, "host: db.example.com") {
			continue
		}
		found := false
		for ; j < len(outLines); j++ {
			if outLines[j] == line {
				found = true
				j++
				break
			}
		}
		if !found {
			t.Errorf("original line did not survive in order: %q\n--- got ---\n%s", line, got)
		}
	}

	// a brand-new connection appends under connections:
	if err := ef.SetConnectionNode("PG_NEW", map[string]any{"type": "postgres", "host": "n"}, nil); err != nil {
		t.Fatalf("SetConnectionNode: %v", err)
	}
	out, _ = os.ReadFile(path)
	got = string(out)
	if !strings.Contains(got, "PG_NEW:") || !strings.Contains(got, "PG_STAGE:") {
		t.Errorf("new connection missing or staging dropped\n--- got ---\n%s", got)
	}
	// the unmanaged block must be after connections: (order preserved)
	if strings.Index(got, "custom_section:") < strings.Index(got, "PG_NEW:") {
		t.Errorf("custom block moved before connections\n--- got ---\n%s", got)
	}
}

// TestSetConnectionNodeNeverExpandsRefs is the regression guard for the GUI
// write path: the file on disk keeps ${VAR}, and the resolved value is never
// written, whatever the writing process's environment looks like.
func TestSetConnectionNodeNeverExpandsRefs(t *testing.T) {
	const leak = "hunter2-LEAK-TEST"

	cases := []struct {
		name    string
		mutate  func(t *testing.T)
		writeAt func(t *testing.T)
	}{
		{
			name:    "var still set at write",
			mutate:  func(t *testing.T) { t.Setenv("MY_PG_PASSWORD", leak) },
			writeAt: func(t *testing.T) { t.Setenv("MY_PG_PASSWORD", leak) },
		},
		{
			name:    "var unset before write",
			mutate:  func(t *testing.T) { t.Setenv("MY_PG_PASSWORD", leak) },
			writeAt: func(t *testing.T) { os.Unsetenv("MY_PG_PASSWORD") },
		},
		{
			name:    "process env differs from load time",
			mutate:  func(t *testing.T) { t.Setenv("MY_PG_PASSWORD", leak) },
			writeAt: func(t *testing.T) { t.Setenv("MY_PG_PASSWORD", "other-value") },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "env.yaml")
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

			tc.mutate(t)
			ef := LoadEnvFile(path) // expands in memory, as the CLI does
			tc.writeAt(t)

			// the GUI payload carries the on-disk ref, not the resolved value
			err := ef.SetConnectionNode("MY_PG", map[string]any{
				"type":     "postgres",
				"host":     "localhost",
				"password": "${MY_PG_PASSWORD}",
				"port":     5433,
			}, nil)
			if err != nil {
				t.Fatalf("SetConnectionNode: %v", err)
			}

			out, _ := os.ReadFile(path)
			got := string(out)
			if !strings.Contains(got, "${MY_PG_PASSWORD}") {
				t.Errorf("expected on-disk ${MY_PG_PASSWORD} ref\n--- got ---\n%s", got)
			}
			if strings.Contains(got, leak) {
				t.Errorf("resolved secret written to disk\n--- got ---\n%s", got)
			}
			if !strings.Contains(got, "5433") {
				t.Errorf("expected updated port\n--- got ---\n%s", got)
			}
		})
	}
}

// TestSetConnectionNodeKeepsUnchangedRefs verifies that an untouched ref field
// stays a ref (the incoming value is identical, so no expansion is involved).
func TestSetConnectionNodeKeepsUnchangedRefs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  MY_PG:
    type: postgres
    password: ${MY_PG_PASSWORD}
    ssh_private_key: ${MY_PG_SSH_PRIVATE_KEY}
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	err := ef.SetConnectionNode("MY_PG", map[string]any{
		"type":            "postgres",
		"password":        "${MY_PG_PASSWORD}",
		"ssh_private_key": "${MY_PG_SSH_PRIVATE_KEY}",
	}, nil)
	if err != nil {
		t.Fatalf("SetConnectionNode: %v", err)
	}

	out, _ := os.ReadFile(path)
	got := string(out)
	if !strings.Contains(got, "${MY_PG_PASSWORD}") || !strings.Contains(got, "${MY_PG_SSH_PRIVATE_KEY}") {
		t.Errorf("refs were not kept\n--- got ---\n%s", got)
	}
}

func TestDeleteConnectionNodePreservesNeighborsAndTrailingComment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `# top
connections:
  PG_A:
    type: postgres
  # keep me (block trailing)
  PG_B:
    type: mysql
    # deeper trailing

env:
  region: us-west-2
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	if err := ef.DeleteConnectionNode("PG_A"); err != nil {
		t.Fatalf("DeleteConnectionNode: %v", err)
	}
	out, _ := os.ReadFile(path)
	got := string(out)
	if strings.Contains(got, "PG_A") {
		t.Errorf("PG_A not removed\n--- got ---\n%s", got)
	}
	for _, sub := range []string{"PG_B:", "# keep me (block trailing)", "region: us-west-2"} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected %q to survive\n--- got ---\n%s", sub, got)
		}
	}

	// deleting the LAST entry keeps the block trailing comment
	if err := ef.DeleteConnectionNode("PG_B"); err != nil {
		t.Fatalf("DeleteConnectionNode: %v", err)
	}
	out, _ = os.ReadFile(path)
	got = string(out)
	if strings.Contains(got, "PG_B") {
		t.Errorf("PG_B not removed\n--- got ---\n%s", got)
	}
	for _, sub := range []string{"connections:", "env:", "region: us-west-2"} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected %q to survive\n--- got ---\n%s", sub, got)
		}
	}
	if !strings.Contains(got, "deeper trailing") {
		t.Errorf("block trailing comment was eaten\n--- got ---\n%s", got)
	}

	// missing connection is an error
	if err := ef.DeleteConnectionNode("NOPE"); err == nil {
		t.Error("expected error for missing connection")
	}
}

func TestConnectionNamesAndEnvKeysRaw(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  MY_PG:
    type: postgres
    password: ${SOME_UNSET_REF}
  my_mongo:
    type: mongodb
variables:
  LEGACY_VAR: ${SOME_UNSET_REF}
  OTHER: x
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	names, err := ef.ConnectionNames()
	if err != nil {
		t.Fatalf("ConnectionNames: %v", err)
	}
	assert.Equal(t, []string{"MY_PG", "my_mongo"}, names)

	keys, err := ef.EnvKeys()
	if err != nil {
		t.Fatalf("EnvKeys: %v", err)
	}
	assert.Equal(t, []string{"LEGACY_VAR", "OTHER"}, keys)

	// raw parse must not expand ${VAR}
	raw, err := ef.RawConnections()
	if err != nil {
		t.Fatalf("RawConnections: %v", err)
	}
	assert.Equal(t, "${SOME_UNSET_REF}", raw["MY_PG"]["password"])
}

func TestParseEnvFileConnectionsKeepsRefs(t *testing.T) {
	t.Setenv("PARSE_TEST_PASSWORD", "hunter2-PARSE-TEST")
	raw, err := ParseEnvFileConnections(`connections:
  MY_PG:
    type: postgres
    password: ${PARSE_TEST_PASSWORD}
`)
	if err != nil {
		t.Fatalf("ParseEnvFileConnections: %v", err)
	}
	if got := raw["MY_PG"]["password"]; got != "${PARSE_TEST_PASSWORD}" {
		t.Fatalf("expected raw ref, got %v", got)
	}
}

func TestSetEnvNodes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  MY_PG:
    type: postgres
env:
  EXISTING: one
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	// new key + unchanged existing value: allowed
	err := ef.SetEnvNodes(map[string]any{"EXISTING": "one", "NEW_KEY": "two"}, false)
	if err != nil {
		t.Fatalf("SetEnvNodes: %v", err)
	}
	out, _ := os.ReadFile(path)
	got := string(out)
	if !strings.Contains(got, "NEW_KEY: two") || !strings.Contains(got, "EXISTING: one") {
		t.Errorf("env updates missing\n--- got ---\n%s", got)
	}

	// changing an existing value requires allowOverwrite
	err = ef.SetEnvNodes(map[string]any{"EXISTING": "changed"}, false)
	if err == nil {
		t.Fatal("expected refusal to overwrite existing env var")
	}
	if err = ef.SetEnvNodes(map[string]any{"EXISTING": "changed"}, true); err != nil {
		t.Fatalf("SetEnvNodes with overwrite: %v", err)
	}
	out, _ = os.ReadFile(path)
	got = string(out)
	if !strings.Contains(got, "EXISTING: changed") {
		t.Errorf("env var not updated\n--- got ---\n%s", got)
	}
	if !strings.Contains(got, "MY_PG:") {
		t.Errorf("connections block lost\n--- got ---\n%s", got)
	}

	// invalid env var name
	if err = ef.SetEnvNodes(map[string]any{"lower": "x"}, true); err == nil {
		t.Error("expected error for lowercase env var name")
	}
}

func TestSetEnvNodesLegacyVariablesBlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  MY_PG:
    type: postgres
variables:
  LEGACY: one
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}

	ef := EnvFile{Path: path}
	if err := ef.SetEnvNodes(map[string]any{"NEW_ONE": "x"}, false); err != nil {
		t.Fatalf("SetEnvNodes: %v", err)
	}
	out, _ := os.ReadFile(path)
	got := string(out)
	if !strings.Contains(got, "variables:") || !strings.Contains(got, "NEW_ONE: x") {
		t.Errorf("expected writes to land in the legacy variables block\n--- got ---\n%s", got)
	}
	if strings.Contains(got, "\nenv:") {
		t.Errorf("should not create a second env block\n--- got ---\n%s", got)
	}
}

func TestDeleteConnectionNodeOnlyEntryKeepsComment(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "env.yaml")
	original := `connections:
  ONLY:
    type: postgres
  # block trailing
env:
  A: b
`
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	ef := EnvFile{Path: path}
	if err := ef.DeleteConnectionNode("ONLY"); err != nil {
		t.Fatalf("DeleteConnectionNode: %v", err)
	}
	out, _ := os.ReadFile(path)
	got := string(out)
	if strings.Contains(got, "ONLY") {
		t.Errorf("entry not removed\n--- got ---\n%s", got)
	}
	if !strings.Contains(got, "# block trailing") {
		t.Errorf("trailing comment was eaten\n--- got ---\n%s", got)
	}
	if !strings.Contains(got, "A: b") {
		t.Errorf("env block lost\n--- got ---\n%s", got)
	}
}

func TestWriteRefusesInvalidEnvFile(t *testing.T) {
	cases := map[string]string{
		"tab":        "connections:\n  PG1:\n\t host: h\n    type: postgres\n",
		"bad conn":   "connections:\n  PG1:\n    type: postgres\n  PG2: [bad]\n",
		"conns list": "connections:\n  - PG1\nenv:\n  KEEP: me\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "env.yaml")
			assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))

			ef := LoadEnvFile(path)
			ef.Connections["PG3"] = map[string]any{"type": "postgres"}
			assert.ErrorContains(t, ef.WriteEnvFile(), "Fix it before sling changes the file")
			assert.ErrorContains(t, ef.CheckFile(), "Fix it before sling changes the file")

			after, _ := os.ReadFile(path)
			assert.Equal(t, body, string(after))
		})
	}
}

func TestRepairEnvYAML(t *testing.T) {
	const nb = " "
	cases := []struct {
		name, body string
		repaired   bool
	}{
		{"nbsp everywhere", "connections:\n" + nb + nb + "MSSQL:\n" + nb + nb + nb + nb + "type:" + nb + "sqlserver\n" + nb + nb + nb + nb + "host:" + nb + "TEST101\n", true},
		{"nbsp indent only", "connections:\n" + nb + nb + "MSSQL:\n" + nb + nb + nb + nb + "type: sqlserver\n" + nb + nb + nb + nb + "host: TEST101\n", true},
		{"tabs only", "connections:\n\tMSSQL:\n\t\ttype: sqlserver\n\t\thost: TEST101\n", true},
		{"spaces then tab", "connections:\r\n  MSSQL:\r\n\ttype: sqlserver\r\n\thost: TEST101\r\n", true},
		{"valid with nbsp in value", "connections:\n  MSSQL:\n    type: sqlserver\n    host: TEST101\n    password: 'a" + nb + "b'\n", false},
		{"unrepairable", "connections:\n  MSSQL:\n\t host: TEST101\n    type: sqlserver\n", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := string(repairEnvYAML([]byte(c.body)))
			if !c.repaired {
				assert.Equal(t, c.body, got)
				return
			}
			ef, err := loadEnvFile(c.body, "")
			assert.NoError(t, err)
			assert.Equal(t, "sqlserver", ef.Connections["MSSQL"]["type"])
			assert.Equal(t, "TEST101", ef.Connections["MSSQL"]["host"])
			assert.NotContains(t, got, nb)
			assert.NotContains(t, got, "\t")
		})
	}
}

func TestWriteRepairsEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "env.yaml")
	body := "connections:\n  KEEP:\n    type: postgres\n    host: h\n"
	assert.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	ef := LoadEnvFile(path)
	assert.NoError(t, ef.CheckFile())
	ef.Connections["NEW"] = map[string]any{"type": "postgres", "host": "n"}
	assert.NoError(t, ef.WriteEnvFile())

	after, _ := os.ReadFile(path)
	assert.NotContains(t, string(after), " ")
	reloaded := LoadEnvFile(path)
	assert.Equal(t, "h", reloaded.Connections["KEEP"]["host"])
	assert.Equal(t, "n", reloaded.Connections["NEW"]["host"])
}
