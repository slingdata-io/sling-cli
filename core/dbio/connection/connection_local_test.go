package connection

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	promoted := PromoteLiteralSecrets("MY_PG", props, envUpdates)

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
	if got := PromoteLiteralSecrets("MY_PG", map[string]any{"password": "x"}, nil); got != nil {
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
	PromoteLiteralSecrets("MY_PG", props, envUpdates)

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
  MY_PG:
    type: postgres
    password: "${MY_PG_PASSWORD}"
    sslmode: "require"
`)
	if err := ec.SetValidated("MY_PG", map[string]any{
		"type": "postgres",
		"host": "localhost",
	}, SetOptions{AllowOverwrite: true, RejectLiteralSecrets: true}); err != nil {
		t.Fatalf("SetValidated: %v", err)
	}
	got := readFile(t, path)
	for _, sub := range []string{"password: \"${MY_PG_PASSWORD}\"", "sslmode: \"require\"", "host: localhost"} {
		if !strings.Contains(got, sub) {
			t.Errorf("expected %q\n--- got ---\n%s", sub, got)
		}
	}
}
