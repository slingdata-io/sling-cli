package connection

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
)

func writeEnvFile(t *testing.T, body string) (path string, ec *EnvFileConns) {
	t.Helper()
	dir := t.TempDir()
	path = filepath.Join(dir, "env.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path, &EnvFileConns{Name: "project env.yaml", EnvFile: &env.EnvFile{Path: path}}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestEnvVarRefRenders(t *testing.T) {
	cases := map[string]string{
		"MY_PG|password":           "${MY_PG_PASSWORD}",
		"my-pg|ssh-key":            "${MY_PG_SSH_KEY}",
		" MY_PG | Password ":       "${MY_PG_PASSWORD}",
		"MY_API|secrets.client_id": "${MY_API_SECRETS_CLIENT_ID}",
	}
	for in, want := range cases {
		parts := strings.Split(in, "|")
		if got := EnvVarRef(parts[0], parts[1]); got != want {
			t.Errorf("EnvVarRef(%q, %q) = %q, want %q", parts[0], parts[1], got, want)
		}
	}
}

// TestNewConnectionFromEntry is the one-path guarantee: the single-entry
// builder and ReadConnectionsEnv produce the same Connection for the same
// env.yaml entry. Refs expand with the env file rules (also inside strings)
// and nested values survive into Data.
func TestNewConnectionFromEntry(t *testing.T) {
	t.Setenv("H", "myhost")
	t.Setenv("P", "secretpw")

	body := `
connections:
  MY_MSSQL:
    type: sqlserver
    host: ${H}.corp
    password: ${P}
    port: 1433
    user: sa
    database: mydb
    bcp_extra_args:
      - -b
      - "5000"
`

	rawProps, err := env.ParseEnvFileConnections(body)
	if err != nil {
		t.Fatal(err)
	}
	entry, ok := rawProps["MY_MSSQL"]
	if !ok {
		t.Fatal("MY_MSSQL not parsed from body")
	}

	// the loader path
	envMap := map[string]any{}
	for name, props := range rawProps {
		envMap[name] = props
	}
	conns, err := ReadConnectionsEnv(envMap)
	if err != nil {
		t.Fatal(err)
	}
	viaLoader, ok := conns["MY_MSSQL"]
	if !ok {
		t.Fatal("ReadConnectionsEnv did not load MY_MSSQL")
	}

	// the native single-entry path
	conn, err := NewConnectionFromEntry("MY_MSSQL", entry)
	if err != nil {
		t.Fatal(err)
	}

	if conn.Type != viaLoader.Type {
		t.Errorf("type mismatch: NewConnectionFromEntry gave %s, ReadConnectionsEnv gave %s", conn.Type, viaLoader.Type)
	}
	if !reflect.DeepEqual(conn.Data, viaLoader.Data) {
		t.Errorf("Data mismatch:\nNewConnectionFromEntry: %s\nReadConnectionsEnv:    %s", g.Marshal(conn.Data), g.Marshal(viaLoader.Data))
	}

	// refs expanded, also inside strings
	if conn.Data["host"] != "myhost.corp" {
		t.Errorf(`host = %v, want "myhost.corp"`, conn.Data["host"])
	}
	if conn.Data["password"] != "secretpw" {
		t.Errorf(`password = %v, want "secretpw"`, conn.Data["password"])
	}

	// nested list survived with item types intact
	args, ok := conn.Data["bcp_extra_args"].([]any)
	if !ok {
		t.Fatalf("bcp_extra_args = %T, want []interface{}", conn.Data["bcp_extra_args"])
	}
	if len(args) != 2 || args[0] != "-b" || args[1] != "5000" {
		t.Errorf("bcp_extra_args = %v, want [-b 5000]", args)
	}

	// the input entry is untouched: ExpandEntry returns a deep copy
	if entry["host"] != "${H}.corp" || entry["password"] != "${P}" {
		t.Errorf("input entry was mutated: %v", g.Marshal(entry))
	}
}

func TestSetValidatedPreservesRestOfFile(t *testing.T) {
	path, ec := writeEnvFile(t, `# Sling environment file — managed by you.

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

# Custom block we don't manage
custom_section:
  retain: yes
`)

	err := ec.SetValidated("PG_PROD", map[string]any{
		"type": "postgres",
		"host": "new.db.example.com",
		"user": "app",
		"port": 5432,
	}, SetOptions{AllowOverwrite: true})
	if err != nil {
		t.Fatalf("SetValidated: %v", err)
	}

	got := readFile(t, path)
	for _, sub := range []string{
		"# Sling environment file — managed by you.",
		"# Production warehouse",
		"PG_STAGE:",
		"stage.db.example.com",
		"# Variables shared across runs",
		"region: us-west-2",
		"# Custom block we don't manage",
		"custom_section:",
		"host: new.db.example.com",
		"port: 5432",
	} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected output to contain %q\n--- got ---\n%s", sub, got)
		}
	}

	// new connection (no overwrite flag needed)
	if err := ec.SetValidated("PG_NEW", map[string]any{
		"type": "postgres",
		"host": "n",
	}, SetOptions{RejectLiteralSecrets: true}); err != nil {
		t.Fatalf("SetValidated new: %v", err)
	}
	got = readFile(t, path)
	if !strings.Contains(got, "PG_NEW:") || !strings.Contains(got, "PG_STAGE:") {
		t.Errorf("new entry missing or neighbor dropped\n--- got ---\n%s", got)
	}
}

func TestSetValidatedOverwriteGuard(t *testing.T) {
	path, ec := writeEnvFile(t, `connections:
  MY_PG:
    type: postgres
    host: localhost
`)
	err := ec.SetValidated("MY_PG", map[string]any{"host": "other"}, SetOptions{})
	if err == nil {
		t.Fatal("expected refusal without AllowOverwrite")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(readFile(t, path), "other") {
		t.Error("file changed despite refusal")
	}

	if err := ec.SetValidated("MY_PG", map[string]any{"host": "other"}, SetOptions{AllowOverwrite: true}); err != nil {
		t.Fatalf("SetValidated with overwrite: %v", err)
	}
	got := readFile(t, path)
	if !strings.Contains(got, "host: other") {
		t.Errorf("host not updated\n--- got ---\n%s", got)
	}
	if !strings.Contains(got, "type: postgres") {
		t.Errorf("omitted keys should be kept (MergeConnProps contract)\n--- got ---\n%s", got)
	}
}

func TestSetValidatedRequireExisting(t *testing.T) {
	_, ec := writeEnvFile(t, `connections:
  MY_PG:
    type: postgres
`)
	if err := ec.SetValidated("NOPE", map[string]any{"type": "postgres"}, SetOptions{RequireExisting: true}); err == nil {
		t.Fatal("expected error for missing connection")
	}
}

// TestSetValidatedKeepsRefsWhenEnvDrifts is the regression guard for the GUI
// write path: the expanded value handed back by a caller must not replace the
// on-disk ref, whatever the process environment looks like at write time.
func TestSetValidatedKeepsRefsWhenEnvDrifts(t *testing.T) {
	const leak = "hunter2-LEAK-TEST"

	body := `connections:
  MY_PG:
    type: postgres
    host: localhost
    password: ${MY_PG_PASSWORD}
    port: 5432
`

	t.Run("expanded incoming value keeps ref", func(t *testing.T) {
		t.Setenv("MY_PG_PASSWORD", leak)
		path, ec := writeEnvFile(t, body)

		err := ec.SetValidated("MY_PG", map[string]any{
			"type":     "postgres",
			"host":     "localhost",
			"password": leak,
			"port":     5433,
		}, SetOptions{AllowOverwrite: true, RejectLiteralSecrets: true})
		if err != nil {
			t.Fatalf("SetValidated: %v", err)
		}
		got := readFile(t, path)
		if !strings.Contains(got, "${MY_PG_PASSWORD}") {
			t.Errorf("expected ref kept\n--- got ---\n%s", got)
		}
		if strings.Contains(got, leak) {
			t.Errorf("secret leaked\n--- got ---\n%s", got)
		}
		if !strings.Contains(got, "5433") {
			t.Errorf("port not updated\n--- got ---\n%s", got)
		}
	})

	// When the process env drifted, the incoming literal cannot be matched to
	// the on-disk ref. The write must then be refused (never materialized).
	t.Run("drifted env refuses literal", func(t *testing.T) {
		t.Setenv("MY_PG_PASSWORD", leak)
		path, ec := writeEnvFile(t, body)
		t.Setenv("MY_PG_PASSWORD", "different")

		err := ec.SetValidated("MY_PG", map[string]any{
			"type":     "postgres",
			"password": leak,
		}, SetOptions{AllowOverwrite: true, RejectLiteralSecrets: true})
		if err == nil {
			t.Fatal("expected literal secret to be refused")
		}
		got := readFile(t, path)
		if strings.Contains(got, leak) {
			t.Errorf("secret leaked\n--- got ---\n%s", got)
		}
		if !strings.Contains(got, "${MY_PG_PASSWORD}") {
			t.Errorf("ref lost\n--- got ---\n%s", got)
		}
	})

	t.Run("unset ref still accepted", func(t *testing.T) {
		os.Unsetenv("MY_PG_PASSWORD")
		path, ec := writeEnvFile(t, body)
		err := ec.SetValidated("MY_PG", map[string]any{
			"type":     "postgres",
			"password": "${MY_PG_PASSWORD}",
			"port":     5433,
		}, SetOptions{AllowOverwrite: true, RejectLiteralSecrets: true})
		if err != nil {
			t.Fatalf("SetValidated: %v", err)
		}
		got := readFile(t, path)
		if !strings.Contains(got, "${MY_PG_PASSWORD}") {
			t.Errorf("expected ref kept\n--- got ---\n%s", got)
		}
	})
}

func TestSetValidatedRefusesLiteralSecret(t *testing.T) {
	_, ec := writeEnvFile(t, `connections:
  MY_PG:
    type: postgres
`)
	err := ec.SetValidated("MY_API", map[string]any{
		"type":     "postgres",
		"password": "hunter2",
	}, SetOptions{RejectLiteralSecrets: true})
	if err == nil {
		t.Fatal("expected literal secret to be refused")
	}
	if !strings.Contains(err.Error(), "${") {
		t.Fatalf("error should point at the ref form: %v", err)
	}
}

func TestPromoteLiteralSecrets(t *testing.T) {
	props := map[string]any{
		"type":     "postgres",
		"host":     "localhost",
		"password": "hunter2",
		"secrets": map[string]any{
			"client_id":     "cid",
			"client_secret": "csec",
			"access_token":  "${ALREADY_A_REF}",
		},
	}
	envUpdates := map[string]any{}
	promoted := PromoteLiteralSecrets("MY_PG", props, nil, envUpdates)

	if props["password"] != "${MY_PG_PASSWORD}" {
		t.Errorf("password not promoted: %v", props["password"])
	}
	if envUpdates["MY_PG_PASSWORD"] != "hunter2" {
		t.Errorf("password not stored: %v", envUpdates)
	}
	secrets := props["secrets"].(map[string]any)
	if secrets["client_id"] != "${MY_PG_CLIENT_ID}" {
		t.Errorf("client_id not promoted: %v", secrets["client_id"])
	}
	if secrets["client_secret"] != "${MY_PG_CLIENT_SECRET}" {
		t.Errorf("client_secret not promoted: %v", secrets["client_secret"])
	}
	if secrets["access_token"] != "${ALREADY_A_REF}" {
		t.Errorf("existing ref must be left alone: %v", secrets["access_token"])
	}
	if envUpdates["MY_PG_ACCESS_TOKEN"] != nil {
		t.Errorf("existing ref must not be stored: %v", envUpdates)
	}
	for _, want := range []string{"password", "secrets.client_id", "secrets.client_secret"} {
		if !strings.Contains(strings.Join(promoted, ","), want) {
			t.Errorf("promoted list missing %s: %v", want, promoted)
		}
	}

	// nil envUpdates is a no-op
	if got := PromoteLiteralSecrets("MY_PG", map[string]any{"password": "x"}, nil, nil); got != nil {
		t.Errorf("expected no-op, got %v", got)
	}
}

func TestSetValidatedWritesEnvAndConnectionAtomically(t *testing.T) {
	path, ec := writeEnvFile(t, `connections:
  OTHER:
    type: postgres
env:
  KEEP: me
`)
	props := map[string]any{"type": "postgres", "password": "hunter2"}
	envUpdates := map[string]any{}
	PromoteLiteralSecrets("MY_PG", props, nil, envUpdates)

	err := ec.SetValidated("MY_PG", props, SetOptions{
		RejectLiteralSecrets: true,
		EnvUpdates:           envUpdates,
	})
	if err != nil {
		t.Fatalf("SetValidated: %v", err)
	}
	got := readFile(t, path)
	for _, sub := range []string{"MY_PG:", "password: ${MY_PG_PASSWORD}", "MY_PG_PASSWORD: hunter2", "KEEP: me", "OTHER:"} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected %q\n--- got ---\n%s", sub, got)
		}
	}

	// a second save with a different value refuses to clobber the env var
	err = ec.SetValidated("MY_PG", map[string]any{"password": "${MY_PG_PASSWORD}"}, SetOptions{
		RejectLiteralSecrets: true,
		EnvUpdates:           map[string]any{"MY_PG_PASSWORD": "newer"},
		AllowOverwrite:       true,
	})
	if err == nil {
		t.Fatal("expected refusal to overwrite existing env var")
	}
	if !strings.Contains(err.Error(), "allow_overwrite") {
		t.Fatalf("unexpected error: %v", err)
	}

	// with AllowEnvOverwrite it goes through
	err = ec.SetValidated("MY_PG", map[string]any{"password": "${MY_PG_PASSWORD}"}, SetOptions{
		RejectLiteralSecrets: true,
		EnvUpdates:           map[string]any{"MY_PG_PASSWORD": "newer"},
		AllowOverwrite:       true,
		AllowEnvOverwrite:    true,
	})
	if err != nil {
		t.Fatalf("SetValidated with AllowEnvOverwrite: %v", err)
	}
	got = readFile(t, path)
	if !strings.Contains(got, "MY_PG_PASSWORD: newer") {
		t.Errorf("env var not updated\n--- got ---\n%s", got)
	}
}

func TestSetValidatedRefusesInvalidType(t *testing.T) {
	_, ec := writeEnvFile(t, "")
	err := ec.SetValidated("MY_CONN", map[string]any{"type": "not-a-real-type"}, SetOptions{})
	if err == nil {
		t.Fatal("expected invalid type error")
	}
	if !strings.Contains(err.Error(), "invalid type") {
		t.Fatalf("unexpected error: %v", err)
	}

	// url-only connections derive their type
	if err := ec.SetValidated("MY_PG", map[string]any{"url": "postgres://user:pass@localhost:5432/db"}, SetOptions{}); err != nil {
		t.Fatalf("SetValidated url: %v", err)
	}
	if _, found := ec.Get("MY_PG"); !found {
		t.Fatal("url connection not written")
	}
}

func TestSetValidatedKeepsQuotedAndBlockValues(t *testing.T) {
	path, ec := writeEnvFile(t, `connections:
  MY_SFTP:
    type: sftp
    host: one.example.com
    password: "${MY_SFTP_PASSWORD}"
    sslmode: "require"
    private_key: |
      -----BEGIN OPENSSH PRIVATE KEY-----
      b3BlbnNzaC1rZXktdjEA
      -----END OPENSSH PRIVATE KEY-----
`)
	if err := ec.SetValidated("MY_SFTP", map[string]any{
		"host": "two.example.com",
	}, SetOptions{AllowOverwrite: true, RejectLiteralSecrets: true}); err != nil {
		t.Fatalf("SetValidated: %v", err)
	}
	got := readFile(t, path)
	for _, sub := range []string{
		"password: \"${MY_SFTP_PASSWORD}\"",
		"sslmode: \"require\"",
		"host: two.example.com",
		"private_key: |",
		"-----BEGIN OPENSSH PRIVATE KEY-----",
		"b3BlbnNzaC1rZXktdjEA",
		"-----END OPENSSH PRIVATE KEY-----",
	} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected %q\n--- got ---\n%s", sub, got)
		}
	}
	if strings.Contains(got, "host: one.example.com") {
		t.Errorf("host not updated\n--- got ---\n%s", got)
	}
}

// SetValidated with Replace treats props as the full entry: keys that props
// does not pass are removed from the stored entry (plan 8.2). Without Replace
// they are kept, the CLI contract.
func TestSetValidatedReplace(t *testing.T) {
	path, ec := writeEnvFile(t, `connections:
  PG:
    type: postgres
    host: db.example.com
    port: 5432
    sslmode: require
`)
	// merge keeps the omitted keys
	if err := ec.SetValidated("PG", g.M("type", "postgres", "host", "new.example.com"), SetOptions{AllowOverwrite: true}); err != nil {
		t.Fatal(err)
	}
	merged := readFile(t, path)
	if !strings.Contains(merged, "port: 5432") || !strings.Contains(merged, "sslmode: require") {
		t.Errorf("merge lost the omitted keys:\n%s", merged)
	}

	// replace removes them
	if err := ec.SetValidated("PG", g.M("type", "postgres", "host", "new.example.com"), SetOptions{Replace: true, AllowOverwrite: true}); err != nil {
		t.Fatal(err)
	}
	replaced := readFile(t, path)
	if strings.Contains(replaced, "port:") || strings.Contains(replaced, "sslmode:") {
		t.Errorf("replace kept the dropped keys:\n%s", replaced)
	}
	if !strings.Contains(replaced, "host: new.example.com") {
		t.Errorf("replace lost the passed keys:\n%s", replaced)
	}

	// a stored ${VAR} ref survives a Replace that passes its expansion back
	refPath, ec := writeEnvFile(t, "connections:\n  PG:\n    type: postgres\n    host: h\n    password: ${PG_PASSWORD}\n")
	t.Setenv("PG_PASSWORD", "hunter2")
	if err := ec.SetValidated("PG", g.M("type", "postgres", "host", "h2", "password", "hunter2"), SetOptions{Replace: true, AllowOverwrite: true}); err != nil {
		t.Fatal(err)
	}
	final := readFile(t, refPath)
	if !strings.Contains(final, "password: ${PG_PASSWORD}") || strings.Contains(final, "hunter2") {
		t.Errorf("replace expanded the stored ref:\n%s", final)
	}
}

// RenameValidated re-keys an entry, keeping its position and comments, and
// refuses an existing target (case-insensitively).
func TestRenameValidated(t *testing.T) {
	path, ec := writeEnvFile(t, `# the file
connections:
  # the production warehouse
  PG_PROD:
    type: postgres
    host: db.example.com
  PG_STAGE:
    type: postgres
    host: stage.example.com
env:
  K: v
`)
	if err := ec.RenameValidated("PG_PROD", "PG_MAIN"); err != nil {
		t.Fatal(err)
	}
	renamed := readFile(t, path)
	if !strings.Contains(renamed, "PG_MAIN:") || strings.Contains(renamed, "PG_PROD") {
		t.Errorf("rename did not re-key the entry:\n%s", renamed)
	}
	if !strings.Contains(renamed, "# the production warehouse") {
		t.Errorf("rename lost the entry comment:\n%s", renamed)
	}
	if !strings.Contains(renamed, "PG_STAGE:") {
		t.Errorf("rename touched the other entries:\n%s", renamed)
	}

	// target exists -> error
	if err := ec.RenameValidated("PG_MAIN", "pg_stage"); err == nil {
		t.Error("expected an already-exists error")
	} else if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %v", err)
	}

	// source missing -> error
	if err := ec.RenameValidated("NOPE", "PG_X"); err == nil {
		t.Error("expected a not-found error")
	}

	// case-only rename is allowed
	if err := ec.RenameValidated("PG_MAIN", "pg_main"); err != nil {
		t.Errorf("case-only rename: %v", err)
	}
	if !strings.Contains(readFile(t, path), "PG_MAIN:") {
		t.Error("case-only rename did not apply")
	}
}

// TestEnvFileConnsSetKeepsFile is the `sling conns set` / `conns unset` path:
// each command changes only the lines of its entry, byte for byte.
func TestEnvFileConnsSetKeepsFile(t *testing.T) {
	t.Setenv("PG_PASS", "hunter2-LEAK-TEST")
	original := `# Sling env file
# maintained by hand

connections:

  # production postgres
  PG_PROD:
    type: postgres
    host: "db.example.com"      # primary
    port: 5432
    password: ${PG_PASS}        # from vault

  DUCK:
    type: duckdb
    instance: /tmp/a.db

# shared variables
env:
  SLING_THREADS: 4     # inline
`
	path, ec := writeEnvFile(t, original)
	ef := env.LoadEnvFile(path) // expands ${PG_PASS} in memory, as the CLI does
	ec.EnvFile = &ef

	// add: the new entry goes after DUCK, with the blank line style of the file
	if err := ec.Set("new_pg", map[string]any{"type": "postgres", "host": "h2"}); err != nil {
		t.Fatalf("Set new: %v", err)
	}
	added := strings.Replace(original, "    instance: /tmp/a.db\n", "    instance: /tmp/a.db\n\n  NEW_PG:\n    type: postgres\n    host: h2\n", 1)
	if got := readFile(t, path); got != added {
		t.Fatalf("add changed other lines\n--- got ---\n%s\n--- want ---\n%s", got, added)
	}

	// update: CLI values are strings; only the port line changes
	if err := ec.Set("PG_PROD", map[string]any{"port": "5433", "host": "db.example.com"}); err != nil {
		t.Fatalf("Set update: %v", err)
	}
	updated := strings.Replace(added, "    port: 5432\n", "    port: 5433\n", 1)
	if got := readFile(t, path); got != updated {
		t.Fatalf("update changed other lines\n--- got ---\n%s\n--- want ---\n%s", got, updated)
	}

	// the struct in memory follows the file, with refs expanded (MCP reads it)
	if got := ec.EnvFile.Connections["PG_PROD"]["password"]; got != "hunter2-LEAK-TEST" {
		t.Errorf("in-memory password = %v", got)
	}
	if got := ec.EnvFile.Connections["NEW_PG"]["host"]; got != "h2" {
		t.Errorf("in-memory NEW_PG host = %v", got)
	}

	// unset: the file is back to the update state without NEW_PG
	if err := ec.Unset("NEW_PG"); err != nil {
		t.Fatalf("Unset: %v", err)
	}
	want := strings.Replace(original, "    port: 5432\n", "    port: 5433\n", 1)
	if got := readFile(t, path); got != want {
		t.Fatalf("unset changed other lines\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
	if _, ok := ec.EnvFile.Connections["NEW_PG"]; ok {
		t.Error("NEW_PG still in memory after Unset")
	}
	if strings.Contains(readFile(t, path), "hunter2-LEAK-TEST") {
		t.Error("expanded secret written to disk")
	}

	// errors leave the file as it is
	before := readFile(t, path)
	if err := ec.Set("BAD", map[string]any{"type": "nope"}); err == nil {
		t.Error("expected an invalid type error")
	}
	if err := ec.Unset("MISSING"); err == nil {
		t.Error("expected a missing connection error")
	}
	if got := readFile(t, path); got != before {
		t.Errorf("a failed command changed the file\n%s", got)
	}
}
