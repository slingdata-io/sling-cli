package secrets

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestInfisicalProvider(t *testing.T) {
	var logins int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/auth/universal-auth/login":
			atomic.AddInt32(&logins, 1)
			body := readJSON(r)
			if body["clientId"] != "cid" || body["clientSecret"] != "csec" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeJSON(w, map[string]any{"accessToken": "inf-tok"})
		case r.URL.Path == "/api/v3/secrets/raw/DB_PASS":
			q := r.URL.Query()
			if r.Header.Get("Authorization") != "Bearer inf-tok" || q.Get("workspaceId") != "proj" || q.Get("environment") != "dev" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			writeJSON(w, map[string]any{"secret": map[string]any{"secretValue": "val@" + q.Get("secretPath")}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := restTestProvider(t, "infisical", map[string]any{"site_url": srv.URL, "client_id": "cid", "client_secret": "csec"})
	if got := restTestGet(t, p, "ref+infisical://proj/dev/app/db/DB_PASS"); got != "val@/app/db" {
		t.Fatalf("path form: %s", got)
	}
	if got := restTestGet(t, p, "ref+infisical://proj/dev/DB_PASS"); got != "val@/" {
		t.Fatalf("root folder: %s", got)
	}
	if got := restTestGet(t, p, "ref+infisical://DB_PASS?project=proj&environment=dev&path=/x"); got != "val@/x" {
		t.Fatalf("vals form: %s", got)
	}
	if logins != 1 {
		t.Fatalf("logins = %d", logins)
	}
}
