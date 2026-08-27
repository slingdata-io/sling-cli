package env

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestCleanRedactsCreateStageCredentials(t *testing.T) {
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	props := map[string]string{
		"AWS_SECRET_ACCESS_KEY": secret,
	}
	line := "CREATE STAGE s CREDENTIALS=(AWS_SECRET_KEY='" + secret + "')"
	got := Clean(props, line)
	if strings.Contains(got, secret) {
		t.Fatalf("CREATE CREDENTIALS not redacted: %q", got)
	}
	if !strings.Contains(got, "***") {
		t.Fatalf("expected *** in %q", got)
	}
}

func TestCleanRedactsPasswordWithoutRegistry(t *testing.T) {
	// exasol has no _properties.yaml entry; password still redacts.
	secret := "exasol-super-secret"
	props := map[string]string{
		"type":     "exasol",
		"password": secret,
	}
	line := "CREATE USER foo IDENTIFIED BY '" + secret + "'"
	got := Clean(props, line)
	if strings.Contains(got, secret) {
		t.Fatalf("floor password not redacted: %q", got)
	}
	if !strings.Contains(got, "***") {
		t.Fatalf("expected *** in %q", got)
	}
}

func TestCleanRedactsNonFloorSecretKey(t *testing.T) {
	jsonSecret := `{"private_key":"not-a-real-key"}`
	got := Clean(map[string]string{
		"gcp_credentials_json": jsonSecret,
		"host":                 "db.example.com",
	}, "create with "+jsonSecret+" host=db.example.com")
	if strings.Contains(got, "not-a-real-key") {
		t.Fatalf("registered secret key not redacted: %q", got)
	}
	if !strings.Contains(got, "db.example.com") {
		t.Fatalf("non-secret host was redacted: %q", got)
	}
}

func TestCleanSkipsEmptySecretValues(t *testing.T) {
	line := "CREATE TABLE t (id int)"
	got := Clean(map[string]string{"password": ""}, line)
	if got != line {
		t.Fatalf("empty password mangled SQL: %q", got)
	}
}

func TestCleanConnDataRedactsNestedSecretNotInRegistry(t *testing.T) {
	secret := "nested-api-key-DISTINCTIVE"
	line := "Authorization: Bearer " + secret
	got := CleanConnData(map[string]any{
		"type": "api",
		"spec": "stripe",
		"secrets": map[string]any{
			"api_key": secret,
		},
	}, line)
	if strings.Contains(got, secret) {
		t.Fatalf("nested api_key leaked: %q", got)
	}
	if !strings.Contains(got, "***") {
		t.Fatalf("expected *** in %q", got)
	}
}

func TestCleanConnDataKeepsShortSecretValues(t *testing.T) {
	line := "Sling Replication | aws_s3 -> postgres | test/parquet/test1.parquet"
	got := CleanConnData(map[string]any{
		"type":     "postgres",
		"password": "postgres",
	}, line)
	if got != line {
		t.Fatalf("short password redacted an unrelated line: %q", got)
	}
}

func TestCleanConnDataKeepsShortNestedSecrets(t *testing.T) {
	line := "Sling CLI | https://slingdata.io"
	got := CleanConnData(map[string]any{
		"type": "api",
		"secrets": map[string]any{
			"subdomain": "slingdata",
		},
	}, line)
	if got != line {
		t.Fatalf("short nested secret redacted an unrelated line: %q", got)
	}
}

func TestCleanConnDataRedactsLongSecret(t *testing.T) {
	secret := "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	got := CleanConnData(map[string]any{
		"type":              "s3",
		"secret_access_key": secret,
	}, "using key "+secret)
	if strings.Contains(got, secret) {
		t.Fatalf("long secret not redacted: %q", got)
	}
}

// TestSecretKeysCoverTemplates fails when a connector adds `secret: true` to
// core/dbio/templates/_properties.yaml but not to SecretKeys.
func TestSecretKeysCoverTemplates(t *testing.T) {
	data, err := os.ReadFile("../dbio/templates/_properties.yaml")
	if err != nil {
		t.Skipf("templates not readable: %v", err)
	}

	have := map[string]struct{}{}
	for _, k := range SecretKeys {
		have[strings.ToLower(k)] = struct{}{}
	}

	// A property name is the last `key:` line at or above a `secret: true`.
	keyRe := regexp.MustCompile(`^\s+([a-z0-9_]+):\s*$`)
	lines := strings.Split(string(data), "\n")
	var missing []string
	for i, ln := range lines {
		if strings.TrimSpace(ln) != "secret: true" {
			continue
		}
		for j := i - 1; j >= 0; j-- {
			m := keyRe.FindStringSubmatch(lines[j])
			if m == nil {
				continue
			}
			name := m[1]
			if name == "properties" {
				break
			}
			if _, ok := have[name]; !ok {
				missing = append(missing, name)
			}
			break
		}
	}
	if len(missing) > 0 {
		t.Fatalf("YAML secret keys absent from SecretKeys: %v", missing)
	}
}
