package secrets

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGCPSecretManager(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		data := base64.StdEncoding.EncodeToString([]byte("gcp-pass-000\n"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"projects/acme/secrets/pg-password/versions/3","payload":{"data":"` + data + `"}}`))
	}))
	defer srv.Close()

	cfg, err := ParseConfig(map[string]map[string]any{
		"gcp": {"type": "gcp_secret_manager", "endpoint": srv.URL + "/", "no_auth": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	r := NewResolver(Options{Config: cfg})
	for _, tc := range []struct{ ref, path string }{
		{"ref+gcpsecrets://acme/pg-password?version=3", "/v1/projects/acme/secrets/pg-password/versions/3:access"},
		{"ref+gcpsecrets://projects/acme/secrets/pg-password", "/v1/projects/acme/secrets/pg-password/versions/latest:access"},
		{"ref+gcpsecrets:////secretmanager.googleapis.com/projects/acme/secrets/pg-password/versions/2", "/v1/projects/acme/secrets/pg-password/versions/2:access"},
		{"ref+gcpsecrets://projects/acme/locations/us-central1/secrets/pg-password/versions/5", "/v1/projects/acme/locations/us-central1/secrets/pg-password/versions/5:access"},
	} {
		v, err := r.Resolve(context.Background(), tc.ref)
		if err != nil {
			t.Fatal(err)
		}
		if v != "gcp-pass-000" {
			t.Fatalf("got %q", v)
		}
		if gotPath != tc.path {
			t.Fatalf("%s: path %q, want %q", tc.ref, gotPath, tc.path)
		}
	}
}

func TestGCPSecretManagerLocation(t *testing.T) {
	p := &gcpSecretsProvider{}
	ref, err := ParseRef("ref+gcpsecrets://projects/acme/locations/europe-west1/secrets/pg")
	if err != nil {
		t.Fatal(err)
	}
	name, location, err := p.versionName(ref)
	if err != nil {
		t.Fatal(err)
	}
	if name != "projects/acme/locations/europe-west1/secrets/pg/versions/latest" || location != "europe-west1" {
		t.Fatalf("name %q, location %q", name, location)
	}
}

func TestGCPSecretManagerBadPath(t *testing.T) {
	r := NewResolver(Options{})
	_, err := r.Resolve(context.Background(), "ref+gcpsecrets://only-project")
	if err == nil || !strings.Contains(err.Error(), "<project>/<name>") {
		t.Fatalf("error %v", err)
	}
}
