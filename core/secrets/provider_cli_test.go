package secrets

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeCLI writes an executable script named bin into a temp dir, and puts
// the dir first on PATH. The script writes its argv, stdin and some env vars
// to <dir>/log, then runs body.
func fakeCLI(t *testing.T, bin, body string) (logPath string) {
	t.Helper()
	dir := t.TempDir()
	logPath = filepath.Join(dir, "log")
	script := "#!/bin/sh\n" +
		`echo "ARGS:$*" >> "` + logPath + `"` + "\n" +
		`echo "ENV:OP=$OP_SERVICE_ACCOUNT_TOKEN BWS=$BWS_ACCESS_TOKEN BW=$BW_SESSION" >> "` + logPath + `"` + "\n" +
		body + "\n"
	if err := os.WriteFile(filepath.Join(dir, bin), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return logPath
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func cliRef(t *testing.T, s string) Ref {
	t.Helper()
	ref, err := ParseRef(s)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func TestOpCLIRead(t *testing.T) {
	log := fakeCLI(t, "op", `printf 'tok-value'`)
	t.Setenv("OP_CONNECT_HOST", "")
	p, _ := newOPProvider(ProviderConfig{Name: "op", Kind: "op", Props: map[string]any{"token": "sa-token-123", "account": "acme"}})

	raw, err := p.Get(context.Background(), cliRef(t, "op://Data/MotherDuck/token?attribute=otp"))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "tok-value" {
		t.Fatalf("got %q", raw)
	}
	l := readLog(t, log)
	if !strings.Contains(l, "ARGS:read --no-newline op://Data/MotherDuck/token?attribute=otp --account acme") {
		t.Fatalf("argv: %s", l)
	}
	if !strings.Contains(l, "OP=sa-token-123") {
		t.Fatalf("token not in child env: %s", l)
	}
	if strings.Contains(strings.SplitN(l, "\n", 2)[0], "sa-token-123") {
		t.Fatal("token is in argv")
	}
}

func TestOpCLIInject(t *testing.T) {
	// the fake op inject replaces each {{ op://... }} with a value, as op does
	log := fakeCLI(t, "op", `cat | sed -e 's#{{ op://v/a/f }}#line1\
line2#' -e 's#{{ op://v/b/f }}#bval#'`)
	p, _ := newOPProvider(ProviderConfig{Name: "op", Kind: "op", Props: map[string]any{}})
	a, b := cliRef(t, "op://v/a/f"), cliRef(t, "op://v/b/f")

	got, err := p.(batchGetter).GetMany(context.Background(), []Ref{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if string(got[a.Key()]) != "line1\nline2" || string(got[b.Key()]) != "bval" {
		t.Fatalf("got %q / %q", got[a.Key()], got[b.Key()])
	}
	if !strings.Contains(readLog(t, log), "ARGS:inject") {
		t.Fatal("op inject not called")
	}
}

func TestOpMissingCLI(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	t.Setenv("OP_CONNECT_HOST", "")
	p, _ := newOPProvider(ProviderConfig{Name: "op", Kind: "op", Props: map[string]any{}})
	_, err := p.Get(context.Background(), cliRef(t, "op://v/i/f"))
	if opSDKFactory == nil && (err == nil || !strings.Contains(err.Error(), "OP_CONNECT_HOST")) {
		t.Fatalf("want install hint, got %v", err)
	}
}

func TestOpConnect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer ct-123" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		filter := r.URL.Query().Get("filter")
		switch r.URL.Path {
		case "/v1/vaults":
			if filter != `name eq "Prod"` {
				t.Errorf("vault filter %q", filter)
			}
			w.Write([]byte(`[{"id":"vault1"}]`))
		case "/v1/vaults/vault1/items":
			if filter != `title eq "PG"` {
				t.Errorf("item filter %q", filter)
			}
			w.Write([]byte(`[{"id":"item1"}]`))
		case "/v1/vaults/vault1/items/item1":
			w.Write([]byte(`{"id":"item1",
				"sections":[{"id":"s1","label":"creds"}],
				"fields":[
					{"id":"password","label":"password","value":"top-level"},
					{"id":"f2","label":"password","value":"in-section","section":{"id":"s1"}}
				]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	t.Setenv("PATH", t.TempDir()) // no op CLI: Connect is next
	p, _ := newOPProvider(ProviderConfig{Name: "op", Kind: "op", Props: map[string]any{
		"connect_host": srv.URL, "connect_token": "ct-123",
	}})
	ctx := context.Background()

	raw, err := p.Get(ctx, cliRef(t, "op://Prod/PG/password"))
	if err != nil || string(raw) != "top-level" {
		t.Fatalf("got %q, %v", raw, err)
	}
	raw, err = p.Get(ctx, cliRef(t, "op://Prod/PG/creds/password"))
	if err != nil || string(raw) != "in-section" {
		t.Fatalf("got %q, %v", raw, err)
	}
	if _, err = p.Get(ctx, cliRef(t, "op://Prod/PG/nope")); err == nil {
		t.Fatal("want error for a missing field")
	}
}

func TestBWSGet(t *testing.T) {
	log := fakeCLI(t, "bws", `echo '{"id":"x","key":"k","value":"bws-secret"}'`)
	p, _ := newBWSProvider(ProviderConfig{Name: "bws", Kind: "bws", Props: map[string]any{"token": "bws-tok-1", "server_url": "https://vault.example"}})
	raw, err := p.Get(context.Background(), cliRef(t, "ref+bws://1234-uuid"))
	if err != nil || string(raw) != "bws-secret" {
		t.Fatalf("got %q, %v", raw, err)
	}
	l := readLog(t, log)
	if !strings.Contains(l, "ARGS:secret get 1234-uuid --output json --server-url https://vault.example") || !strings.Contains(l, "BWS=bws-tok-1") {
		t.Fatalf("log: %s", l)
	}
}

func TestBitwardenBWGet(t *testing.T) {
	log := fakeCLI(t, "bw", `echo '{"notes":"n1","login":{"username":"u1","password":"p1"},"fields":[{"name":"api","value":"a1"}]}'`)
	p, _ := newBWProvider(ProviderConfig{Name: "bw", Kind: "bw", Props: map[string]any{"session": "sess-1"}})
	ctx := context.Background()
	for ref, want := range map[string]string{
		"ref+bw://item1/password": "p1",
		"ref+bw://item1/username": "u1",
		"ref+bw://item1/notes":    "n1",
		"ref+bw://item1/api":      "a1",
	} {
		raw, err := p.Get(ctx, cliRef(t, ref))
		if err != nil || string(raw) != want {
			t.Fatalf("%s: got %q, %v", ref, raw, err)
		}
	}
	raw, err := p.Get(ctx, cliRef(t, "ref+bw://item1"))
	if err != nil || !json.Valid(raw) {
		t.Fatalf("whole item: %q, %v", raw, err)
	}
	if _, err := p.Get(ctx, cliRef(t, "ref+bw://item1/missing")); err == nil {
		t.Fatal("want error for a missing field")
	}
	l := readLog(t, log)
	if !strings.Contains(l, "ARGS:get item item1") || !strings.Contains(l, "BW=sess-1") {
		t.Fatalf("log: %s", l)
	}
}

func TestKeeperNoConfig(t *testing.T) {
	t.Setenv("KSM_CONFIG", "")
	p, _ := newKeeperProvider(ProviderConfig{Name: "keeper", Kind: "keeper", Props: map[string]any{}})
	_, err := p.Get(context.Background(), cliRef(t, "keeper://abc/field/password"))
	if err == nil || !strings.Contains(err.Error(), "KSM_CONFIG") {
		t.Fatalf("got %v", err)
	}
}

func TestKeeperNotation(t *testing.T) {
	for in, want := range map[string]string{
		"keeper://abc/field/password":       "keeper://abc/field/password",
		"ref+keeper://My Title/field/login": "keeper://My Title/field/login",
	} {
		if got := keeperNotation(cliRef(t, in)); got != want {
			t.Fatalf("%s: got %q", in, got)
		}
	}
}

func TestSopsDecryptOnce(t *testing.T) {
	log := fakeCLI(t, "sops", `echo '{"pg":{"password":"s3cret"}}'`)
	base := t.TempDir()
	p, _ := newSOPSProvider(ProviderConfig{Name: "sops", Kind: "sops", Props: map[string]any{}, baseDir: base})
	a := cliRef(t, "ref+sops://secrets.enc.json#/pg/password")
	b := cliRef(t, "ref+sops://secrets.enc.json?x=1#/pg")

	got, err := p.(batchGetter).GetMany(context.Background(), []Ref{a, b})
	if err != nil {
		t.Fatal(err)
	}
	v, err := a.Select(got[a.Key()])
	if err != nil || v != "s3cret" {
		t.Fatalf("got %v, %v", v, err)
	}
	l := readLog(t, log)
	if strings.Count(l, "ARGS:") != 1 {
		t.Fatalf("want one decrypt, log: %s", l)
	}
	want := "ARGS:-d --input-type json --output-type json " + filepath.Join(base, "secrets.enc.json")
	if !strings.Contains(l, want) {
		t.Fatalf("argv: %s", l)
	}
}

func TestSopsDotenv(t *testing.T) {
	fakeCLI(t, "sops", `printf 'PG_PASSWORD="abc def"\n# comment\nTOKEN=xyz\n'`)
	p, _ := newSOPSProvider(ProviderConfig{Name: "sops", Kind: "sops", Props: map[string]any{}})
	ref := cliRef(t, "ref+sops:///tmp/app.env#PG_PASSWORD")
	raw, err := p.Get(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	v, err := ref.Select(raw)
	if err != nil || v != "abc def" {
		t.Fatalf("got %v, %v", v, err)
	}
}

func TestSopsViaResolver(t *testing.T) {
	fakeCLI(t, "sops", `printf 'pg:\n  password: yaml-pass\n'`)
	r := NewResolver(Options{BaseDir: t.TempDir()})
	v, err := r.Resolve(context.Background(), "ref+sops://secrets.enc.yaml#/pg/password")
	if err != nil || v != "yaml-pass" {
		t.Fatalf("got %v, %v", v, err)
	}
}
