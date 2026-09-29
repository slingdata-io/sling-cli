package secrets

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConjurProvider(t *testing.T) {
	rawToken := `{"protected":"abc","payload":"xyz"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.EscapedPath() {
		case "/authn/acct/host%2Fapp/authenticate":
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPost || string(body) != "key123" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			io.WriteString(w, rawToken)
		case "/secrets/acct/variable/prod%2Fdb%2Fpassword":
			want := `Token token="` + base64.StdEncoding.EncodeToString([]byte(rawToken)) + `"`
			if r.Header.Get("Authorization") != want {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			io.WriteString(w, "conjur-pw")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p := restTestProvider(t, "conjur", map[string]any{"appliance_url": srv.URL, "account": "acct", "login": "host/app", "api_key": "key123"})
	for _, ref := range []string{"ref+conjur://prod/db/password", "ref+conjur://acct:variable:prod/db/password"} {
		if got := restTestGet(t, p, ref); got != "conjur-pw" {
			t.Fatalf("%s: got %s", ref, got)
		}
	}
	if _, err := p.Get(context.Background(), Ref{Path: "other:variable:prod/db/password"}); err == nil || !strings.Contains(err.Error(), "not the configured account") {
		t.Fatalf("error %v", err)
	}
}
