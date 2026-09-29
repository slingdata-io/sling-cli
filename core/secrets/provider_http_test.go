package secrets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// restTestProvider builds a provider of kind from props, with no resolver.
func restTestProvider(t *testing.T, kind string, props map[string]any) Provider {
	t.Helper()
	p, err := factoryOf(kind)(ProviderConfig{Name: "t", Kind: kind, Props: props})
	if err != nil {
		t.Fatalf("factory %s: %v", kind, err)
	}
	return p
}

func restTestGet(t *testing.T, p Provider, ref string) string {
	t.Helper()
	r, err := ParseRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Get(context.Background(), r)
	if err != nil {
		t.Fatalf("get %s: %v", ref, err)
	}
	return string(b)
}

// restTestResolve resolves ref through a resolver with one named provider.
func restTestResolve(t *testing.T, kind string, props map[string]any, ref string) any {
	t.Helper()
	raw := map[string]any{"type": kind}
	for k, v := range props {
		raw[k] = v
	}
	cfg, err := ParseConfig(map[string]map[string]any{"p": raw})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewResolver(Options{Config: cfg}).Resolve(context.Background(), ref)
	if err != nil {
		t.Fatalf("resolve %s: %v", ref, err)
	}
	return v
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request) map[string]any {
	m := map[string]any{}
	json.NewDecoder(r.Body).Decode(&m)
	return m
}

func TestHTTPJSONProvider(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable) // retried
			return
		}
		if r.Header.Get("Authorization") != "Bearer cfg-tok" || r.URL.Path != "/sling/mydb" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		writeJSON(w, map[string]any{"password": "http-pw"})
	}))
	defer srv.Close()

	host := strings.TrimPrefix(srv.URL, "http://")
	props := map[string]any{"headers": map[string]any{"Authorization": "Bearer cfg-tok"}}
	p := restTestProvider(t, "httpjson", props).(*httpJSONProvider)
	p.client.backoff = time.Millisecond
	if got := restTestGet(t, p, "ref+httpjson://"+host+"/sling/mydb?insecure=true"); got != `{"password":"http-pw"}`+"\n" {
		t.Fatalf("got %q", got)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
	if v := restTestResolve(t, "httpjson", props, "ref+httpjson://"+host+"/sling/mydb?insecure=true#/password"); v != "http-pw" {
		t.Fatalf("resolve: %v", v)
	}
}
