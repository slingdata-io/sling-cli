package secrets

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTSSDelineaProvider(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/SecretServer/oauth2/token":
			r.ParseForm()
			if r.Form.Get("grant_type") != "password" || r.Form.Get("username") != "u" || r.Form.Get("password") != "p" || r.Form.Get("domain") != "corp" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]any{"access_token": "tss-tok"})
		case "/SecretServer/api/v1/secrets/42":
			if r.Header.Get("Authorization") != "Bearer tss-tok" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			writeJSON(w, map[string]any{"items": []any{
				map[string]any{"slug": "username", "fieldName": "Username", "itemValue": "admin"},
				map[string]any{"slug": "password", "fieldName": "Password", "itemValue": "tss-pw"},
			}})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	props := map[string]any{"server_url": srv.URL + "/SecretServer", "username": "u", "password": "p", "domain": "corp"}
	if v := restTestResolve(t, "delinea", props, "ref+tss://42#/password"); v != "tss-pw" {
		t.Fatalf("pointer: %v", v)
	}
	p := restTestProvider(t, "tss", props)
	if got := restTestGet(t, p, "ref+tss://42/username"); got != "admin" {
		t.Fatalf("field: %s", got)
	}
}
