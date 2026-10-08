package secrets

import (
	"context"
	"encoding/base64"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func k8sSecretJSON() string {
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	return `{"kind":"Secret","data":{"password":"` + b64("k8s-pw") + `","user":"` + b64("etl") + `"}}`
}

func TestK8sProviderInCluster(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer sa-token" || r.URL.Path != "/api/v1/namespaces/prod/secrets/pg" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		io.WriteString(w, k8sSecretJSON())
	}))
	defer srv.Close()

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	caFile := filepath.Join(dir, "ca.crt")
	os.WriteFile(tokenFile, []byte("sa-token\n"), 0600)
	os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600)

	p := restTestProvider(t, "k8s", map[string]any{"host": srv.URL, "token_file": tokenFile, "ca_file": caFile})
	if got := restTestGet(t, p, "ref+k8s://v1/Secret/prod/pg/password"); got != "k8s-pw" {
		t.Fatalf("key: %s", got)
	}
	if got := restTestGet(t, p, "ref+kubernetes://v1/Secret/prod/pg"); got != `{"password":"k8s-pw","user":"etl"}` {
		t.Fatalf("whole: %s", got)
	}
	r, _ := ParseRef("ref+k8s://v1/Secret/prod/pg/missing")
	if _, err := p.Get(context.Background(), r); err == nil || strings.Contains(err.Error(), "k8s-pw") {
		t.Fatalf("missing key: %v", err)
	}
}

func TestK8sProviderKubectl(t *testing.T) {
	dir := t.TempDir()
	argsFile := filepath.Join(dir, "args")
	script := "#!/bin/sh\necho \"$@\" > " + argsFile + "\ncat <<'EOF'\n" + k8sSecretJSON() + "\nEOF\n"
	if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("KUBERNETES_SERVICE_HOST", "")

	p := restTestProvider(t, "k8s", map[string]any{"kubeconfig": "/tmp/kc", "context": "dev"})
	if got := restTestGet(t, p, "ref+k8s://v1/Secret/prod/pg/user"); got != "etl" {
		t.Fatalf("got %s", got)
	}
	args, _ := os.ReadFile(argsFile)
	if strings.TrimSpace(string(args)) != "get secret pg -n prod -o json --kubeconfig /tmp/kc --context dev" {
		t.Fatalf("args: %s", args)
	}
}
