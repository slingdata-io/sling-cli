package secrets

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestVaultProvider(t *testing.T) {
	var logins, reads int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/approle/login" && r.Header.Get("X-Vault-Token") != "tok1" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.Header.Get("X-Vault-Namespace") != "team" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch r.URL.Path {
		case "/v1/auth/approle/login":
			atomic.AddInt32(&logins, 1)
			body := readJSON(r)
			if body["role_id"] != "r1" || body["secret_id"] != "s1" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]any{"auth": map[string]any{"client_token": "tok1"}})
		case "/v1/sys/internal/ui/mounts/secret/prod/pg":
			writeJSON(w, map[string]any{"data": map[string]any{"path": "secret/", "type": "kv", "options": map[string]any{"version": "2"}}})
		case "/v1/sys/internal/ui/mounts/kv1/app":
			writeJSON(w, map[string]any{"data": map[string]any{"path": "kv1/", "type": "kv", "options": map[string]any{"version": "1"}}})
		case "/v1/secret/data/prod/pg":
			if atomic.AddInt32(&reads, 1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests) // retried
				return
			}
			if r.URL.Query().Get("version") != "" && r.URL.Query().Get("version") != "3" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]any{"data": map[string]any{"data": map[string]any{"password": "pw2"}, "metadata": map[string]any{}}})
		case "/v1/kv1/app":
			writeJSON(w, map[string]any{"data": map[string]any{"password": "pw1"}})
		case "/v1/other/data/app":
			writeJSON(w, map[string]any{"data": map[string]any{"data": map[string]any{"k": "v"}}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	props := map[string]any{"address": srv.URL, "namespace": "team", "auth": "approle", "role_id": "r1", "secret_id": "s1"}
	p := restTestProvider(t, "vault", props).(*vaultProvider)
	p.client.backoff = time.Millisecond

	if got := restTestGet(t, p, "ref+vault://secret/prod/pg"); got != `{"password":"pw2"}` {
		t.Fatalf("kv2: %s", got)
	}
	if got := restTestGet(t, p, "ref+vault://secret/prod/pg?version=3"); got != `{"password":"pw2"}` {
		t.Fatalf("kv2 version: %s", got)
	}
	if got := restTestGet(t, p, "ref+vault://kv1/app"); got != `{"password":"pw1"}` {
		t.Fatalf("kv1: %s", got)
	}
	// mount lookup fails (404): ?kv=2 puts data/ after the first segment
	if got := restTestGet(t, p, "ref+vault://other/app?kv=2"); got != `{"k":"v"}` {
		t.Fatalf("forced kv2: %s", got)
	}
	if logins != 1 {
		t.Fatalf("logins = %d, want 1", logins)
	}
	if reads < 2 {
		t.Fatalf("429 was not retried")
	}

	if v := restTestResolve(t, "vault", props, "ref+vault://secret/prod/pg#/password"); v != "pw2" {
		t.Fatalf("resolve: %v", v)
	}

	t.Setenv("VAULT_ADDR", "")
	if _, err := factoryOf("vault")(ProviderConfig{Name: "t", Kind: "vault", Props: map[string]any{}}); err == nil {
		t.Fatal("want error with no address")
	}
}

func TestOpenBaoProvider(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "bao-tok" || r.Header.Get("X-Vault-Namespace") != "ns1" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if r.URL.Path == "/v1/secret/data/app" {
			writeJSON(w, map[string]any{"data": map[string]any{"data": map[string]any{"api_key": "abcd1234"}}})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	t.Setenv("BAO_ADDR", srv.URL)
	t.Setenv("BAO_TOKEN", "bao-tok")
	t.Setenv("BAO_NAMESPACE", "ns1")
	t.Setenv("VAULT_ADDR", "")
	t.Setenv("VAULT_TOKEN", "")

	p := restTestProvider(t, "openbao", map[string]any{})
	if got := restTestGet(t, p, "ref+openbao://secret/app?kv=2"); got != `{"api_key":"abcd1234"}` {
		t.Fatalf("got %s", got)
	}
}

func TestVaultLeaseRenewal(t *testing.T) {
	renewed := make(chan map[string]any, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/database/creds/etl":
			writeJSON(w, map[string]any{
				"lease_id": "database/creds/etl/abc", "lease_duration": 1, "renewable": true,
				"data": map[string]any{"username": "v-etl", "password": "dyn-pw"},
			})
		case "/v1/sys/leases/renew":
			renewed <- readJSON(r)
			writeJSON(w, map[string]any{"lease_duration": 0, "renewable": false}) // max TTL reached
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := restTestProvider(t, "vault", map[string]any{"address": srv.URL, "token": "tok"})
	defer p.(*vaultProvider).Close()
	if got := restTestGet(t, p, "ref+vault://database/creds/etl?kv=1"); !strings.Contains(got, "dyn-pw") {
		t.Fatalf("unexpected value %s", got)
	}

	select {
	case body := <-renewed:
		if body["lease_id"] != "database/creds/etl/abc" {
			t.Fatalf("unexpected renew body %v", body)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("lease was not renewed")
	}
}
