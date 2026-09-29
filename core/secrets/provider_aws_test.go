package secrets

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// awsTestEnv sets static credentials and hides the user's AWS files.
func awsTestEnv(t *testing.T) {
	t.Helper()
	empty := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_CONFIG_FILE", empty)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", empty)
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIDTEST")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secretkeytest")
	t.Setenv("AWS_SESSION_TOKEN", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

func awsResolver(t *testing.T, kind, endpoint string) *Resolver {
	t.Helper()
	cfg, err := ParseConfig(map[string]map[string]any{
		"aws": {"type": kind, "endpoint": endpoint},
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewResolver(Options{Config: cfg})
}

func TestAWSSecretsManager(t *testing.T) {
	awsTestEnv(t)
	var gotTarget string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTarget = r.Header.Get("X-Amz-Target")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"Name":"prod/postgres","SecretString":"{\"username\":\"etl\",\"password\":\"pg-pass-123\"}"}`))
	}))
	defer srv.Close()

	r := awsResolver(t, "aws_secrets_manager", srv.URL)
	v, err := r.Resolve(context.Background(), "ref+awssecrets://prod/postgres?version_stage=AWSCURRENT#/password")
	if err != nil {
		t.Fatal(err)
	}
	if v != "pg-pass-123" {
		t.Fatalf("got %v", v)
	}
	if gotTarget != "secretsmanager.GetSecretValue" {
		t.Fatalf("target %q", gotTarget)
	}
	if gotBody["SecretId"] != "prod/postgres" || gotBody["VersionStage"] != "AWSCURRENT" {
		t.Fatalf("body %v", gotBody)
	}
}

func TestAWSSecretsManagerError(t *testing.T) {
	awsTestEnv(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"ResourceNotFoundException","message":"Secrets Manager can't find the specified secret."}`))
	}))
	defer srv.Close()

	r := awsResolver(t, "awssecrets", srv.URL)
	_, err := r.Resolve(context.Background(), "ref+awssecrets://missing")
	if err == nil {
		t.Fatal("expected error")
	}
	if want := `could not resolve secret reference "ref+awssecrets://missing" (provider aws, awssecrets)`; !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not contain %q", err, want)
	}
}

func TestAWSSSM(t *testing.T) {
	awsTestEnv(t)
	var gotTarget string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTarget = r.Header.Get("X-Amz-Target")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &gotBody)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = w.Write([]byte(`{"Parameter":{"Name":"/prod/pg/password","Type":"SecureString","Value":"ssm-pass-456"}}`))
	}))
	defer srv.Close()

	r := awsResolver(t, "aws_ssm", srv.URL)
	v, err := r.Resolve(context.Background(), "ref+awsssm://prod/pg/password?region=eu-west-1")
	if err != nil {
		t.Fatal(err)
	}
	if v != "ssm-pass-456" {
		t.Fatalf("got %v", v)
	}
	if gotTarget != "AmazonSSM.GetParameter" {
		t.Fatalf("target %q", gotTarget)
	}
	if gotBody["Name"] != "/prod/pg/password" || gotBody["WithDecryption"] != true {
		t.Fatalf("body %v", gotBody)
	}
}

func TestAWSRegionFromARN(t *testing.T) {
	l := newAWSLoader(ProviderConfig{})
	t.Setenv("AWS_REGION", "us-east-1")
	for path, want := range map[string]string{
		"arn:aws:secretsmanager:eu-west-2:123456789012:secret:prod/pg-a1b2c3": "eu-west-2",
		"arn:aws-us-gov:ssm:us-gov-west-1:123456789012:parameter/prod/pg":     "us-gov-west-1",
		"prod/pg": "us-east-1",
	} {
		if got := l.region(Ref{Path: path}); got != want {
			t.Fatalf("%s: got %q, want %q", path, got, want)
		}
	}
	ref := Ref{Path: "arn:aws:secretsmanager:eu-west-2:1:secret:x", Params: map[string][]string{"region": {"ap-south-1"}}}
	if got := l.region(ref); got != "ap-south-1" {
		t.Fatalf("?region= must win, got %q", got)
	}
}
