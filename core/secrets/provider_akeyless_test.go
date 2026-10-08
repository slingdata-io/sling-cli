package secrets

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAkeylessProvider(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := readJSON(r)
		switch r.URL.Path {
		case "/auth":
			if body["access-id"] != "p-123" || body["access-key"] != "k" || body["access-type"] != "access_key" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeJSON(w, map[string]any{"token": "ak-tok"})
		case "/get-secret-value":
			names, _ := body["names"].([]any)
			if body["token"] != "ak-tok" || len(names) != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeJSON(w, map[string]any{names[0].(string): "ak-val"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := restTestProvider(t, "akeyless", map[string]any{"gateway_url": srv.URL, "access_id": "p-123", "access_key": "k"})
	if got := restTestGet(t, p, "ref+akeyless:///prod/db/password"); got != "ak-val" {
		t.Fatalf("got %s", got)
	}
	if got := restTestGet(t, p, "ref+akeyless://prod/db/password"); got != "ak-val" {
		t.Fatalf("no leading slash: %s", got)
	}
}
