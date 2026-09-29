package secrets

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestDopplerProvider(t *testing.T) {
	var downloads int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer dp.st.x" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		switch r.URL.Path {
		case "/v3/configs/config/secret":
			writeJSON(w, map[string]any{"value": map[string]any{"raw": q.Get("project") + "/" + q.Get("config") + "/" + q.Get("name")}})
		case "/v3/configs/config/secrets/download":
			atomic.AddInt32(&downloads, 1)
			if q.Get("format") != "json" || q.Get("project") != "web" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeJSON(w, map[string]any{"A": "1111", "B": "2222"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	t.Setenv("DOPPLER_PROJECT", "")
	t.Setenv("DOPPLER_CONFIG", "")
	p := restTestProvider(t, "doppler", map[string]any{"api_url": srv.URL, "token": "dp.st.x", "project": "web", "config": "prd"})
	if got := restTestGet(t, p, "ref+doppler://api/dev/KEY"); got != "api/dev/KEY" {
		t.Fatalf("full form: %s", got)
	}
	if got := restTestGet(t, p, "ref+doppler://KEY"); got != "web/prd/KEY" {
		t.Fatalf("short form: %s", got)
	}

	a, _ := ParseRef("ref+doppler://A")
	b, _ := ParseRef("ref+doppler://web/prd/B")
	got, err := p.(batchGetter).GetMany(context.Background(), []Ref{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if string(got[a.Key()]) != "1111" || string(got[b.Key()]) != "2222" || downloads != 1 {
		t.Fatalf("GetMany = %v, downloads %d", got, downloads)
	}
}
