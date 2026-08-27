package assist

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func isolateOpenCodeEnv(t *testing.T) {
	t.Helper()
	home := withTempHomeDir(t)
	bin := filepath.Join(home, "empty-bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("OPENCODE_PATH", "")
	t.Setenv("OPENCODE_VERSION", OpenCodeVersion)
	t.Setenv("XDG_CONFIG_HOME", "")
	t.Setenv("ANTHROPIC_API_KEY", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("GEMINI_API_KEY", "")
	t.Setenv("GOOGLE_API_KEY", "")
	t.Setenv("XAI_API_KEY", "")
	t.Cleanup(func() { openCodeTestDownloadURL = "" })
}

func zipOpenCodeStub(t *testing.T, version string) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	zw := zip.NewWriter(buf)
	h := &zip.FileHeader{Name: "opencode", Method: zip.Deflate}
	h.SetMode(0o755)
	w, err := zw.CreateHeader(h)
	if err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"" + version + "\"; exit 0; fi\n" +
		"echo stub\n"
	if _, err := io.WriteString(w, script); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func serveOpenCodeZip(t *testing.T, version string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	payload := zipOpenCodeStub(t, version)
	hits := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/zip")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

func TestOpenCodeAssetNameLinuxX64(t *testing.T) {
	prevAVX, prevMusl := openCodeHasAVX2, openCodeIsMusl
	t.Cleanup(func() {
		openCodeHasAVX2, openCodeIsMusl = prevAVX, prevMusl
	})

	cases := []struct {
		avx2, musl bool
		want       string
	}{
		{true, false, "opencode-linux-x64.tar.gz"},
		{false, false, "opencode-linux-x64-baseline.tar.gz"},
		{true, true, "opencode-linux-x64-musl.tar.gz"},
		{false, true, "opencode-linux-x64-baseline-musl.tar.gz"},
	}
	for _, tc := range cases {
		openCodeHasAVX2 = func() bool { return tc.avx2 }
		openCodeIsMusl = func() bool { return tc.musl }
		got, err := OpenCodeAssetName("linux", "amd64")
		if err != nil {
			t.Fatalf("avx2=%v musl=%v: %v", tc.avx2, tc.musl, err)
		}
		if got != tc.want {
			t.Errorf("avx2=%v musl=%v: got %s want %s", tc.avx2, tc.musl, got, tc.want)
		}
	}
}

func TestOpenCodeAssetNamePinnedPlatforms(t *testing.T) {
	prevAVX, prevMusl := openCodeHasAVX2, openCodeIsMusl
	t.Cleanup(func() {
		openCodeHasAVX2, openCodeIsMusl = prevAVX, prevMusl
	})
	openCodeHasAVX2 = func() bool { return true }
	openCodeIsMusl = func() bool { return false }

	cases := map[string]string{
		"darwin/arm64":  "opencode-darwin-arm64.zip",
		"darwin/amd64":  "opencode-darwin-x64.zip",
		"windows/amd64": "opencode-windows-x64.zip",
		"windows/arm64": "opencode-windows-arm64.zip",
		"linux/arm64":   "opencode-linux-arm64.tar.gz",
	}
	for plat, want := range cases {
		parts := strings.Split(plat, "/")
		got, err := OpenCodeAssetName(parts[0], parts[1])
		if err != nil {
			t.Fatalf("%s: %v", plat, err)
		}
		if got != want {
			t.Errorf("%s: got %s want %s", plat, got, want)
		}
	}
	if _, err := OpenCodeAssetName("js", "wasm"); err == nil {
		t.Fatal("expected error for js/wasm")
	}
}

func TestEnsureBinOpenCodeInstallFromZip(t *testing.T) {
	isolateOpenCodeEnv(t)
	srv, hits := serveOpenCodeZip(t, OpenCodeVersion)
	openCodeTestDownloadURL = srv.URL + "/opencode.zip"

	bin, err := EnsureBinOpenCode()
	if err != nil {
		t.Fatalf("EnsureBinOpenCode: %v", err)
	}
	want := BundledOpenCodePath()
	if bin != want {
		t.Fatalf("bin = %s, want %s", bin, want)
	}
	info, err := os.Stat(bin)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("perm = %o, want 0755", info.Mode().Perm())
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("--version: %v (%s)", err, out)
	}
	if !versionMatches(string(out), OpenCodeVersion) {
		t.Fatalf("version %q does not match %s", out, OpenCodeVersion)
	}
	if hits.Load() != 1 {
		t.Fatalf("download hits = %d, want 1", hits.Load())
	}

	bin2, err := EnsureBinOpenCode()
	if err != nil {
		t.Fatal(err)
	}
	if bin2 != bin {
		t.Fatalf("second call bin = %s", bin2)
	}
	if hits.Load() != 1 {
		t.Fatalf("stale dir should not re-download; hits = %d", hits.Load())
	}
}

func TestEnsureBinOpenCodeStaleRedownload(t *testing.T) {
	isolateOpenCodeEnv(t)
	want := BundledOpenCodePath()
	if err := os.MkdirAll(filepath.Dir(want), 0o755); err != nil {
		t.Fatal(err)
	}
	stale := "#!/bin/sh\necho 0.0.1\n"
	if err := os.WriteFile(want, []byte(stale), 0o755); err != nil {
		t.Fatal(err)
	}

	srv, hits := serveOpenCodeZip(t, OpenCodeVersion)
	openCodeTestDownloadURL = srv.URL + "/opencode.zip"

	bin, err := EnsureBinOpenCode()
	if err != nil {
		t.Fatalf("EnsureBinOpenCode: %v", err)
	}
	if bin != want {
		t.Fatalf("bin = %s, want %s", bin, want)
	}
	if hits.Load() != 1 {
		t.Fatalf("stale binary should re-download; hits = %d", hits.Load())
	}
	out, err := exec.Command(bin, "--version").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !versionMatches(string(out), OpenCodeVersion) {
		t.Fatalf("after re-download version = %q", out)
	}
}

func TestEnsureBinOpenCodePathOverride(t *testing.T) {
	isolateOpenCodeEnv(t)
	dir := t.TempDir()
	asDir := filepath.Join(dir, "as-dir")
	if err := os.MkdirAll(asDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENCODE_PATH", asDir)
	if _, err := EnsureBinOpenCode(); err == nil {
		t.Fatal("expected error for directory OPENCODE_PATH")
	}

	file := filepath.Join(dir, "opencode")
	if err := os.WriteFile(file, []byte("x"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENCODE_PATH", file)
	got, err := EnsureBinOpenCode()
	if err != nil {
		t.Fatal(err)
	}
	if got != file {
		t.Fatalf("got %s want %s", got, file)
	}
}

func TestEnsureBinOpenCodeSystemPathWins(t *testing.T) {
	isolateOpenCodeEnv(t)
	home := withTempHomeDir(t)
	binDir := filepath.Join(home, "sys-bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	sys := filepath.Join(binDir, "opencode")
	if err := os.WriteFile(sys, []byte("#!/bin/sh\necho sys\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir)

	hits := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(500)
	}))
	t.Cleanup(srv.Close)
	openCodeTestDownloadURL = srv.URL + "/opencode.zip"

	got, err := EnsureBinOpenCode()
	if err != nil {
		t.Fatal(err)
	}
	if got != sys {
		t.Fatalf("got %s want system %s", got, sys)
	}
	if hits.Load() != 0 {
		t.Fatalf("system binary should skip download; hits = %d", hits.Load())
	}
}

func TestHarnessProviderChoiceZenDisclosure(t *testing.T) {
	isolateOpenCodeEnv(t)
	c := HarnessProviderChoice()
	if c.Kind != "zen-free" {
		t.Fatalf("kind = %s", c.Kind)
	}
	if c.Model != ZenFreeModel {
		t.Fatalf("model = %s", c.Model)
	}
	if c.Disclosure != ZenFreeDisclosure {
		t.Fatalf("disclosure = %q", c.Disclosure)
	}
	cfg := HarnessProviderConfig()
	if cfg["model"] != ZenFreeModel {
		t.Fatalf("config model = %v", cfg["model"])
	}
}

func TestHarnessProviderChoiceKeyedWins(t *testing.T) {
	isolateOpenCodeEnv(t)
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	c := HarnessProviderChoice()
	if c.Kind != "anthropic" {
		t.Fatalf("kind = %s", c.Kind)
	}
	if c.Disclosure != "" {
		t.Fatalf("keyed provider should have no zen disclosure, got %q", c.Disclosure)
	}
	if !strings.Contains(c.Model, "anthropic/") {
		t.Fatalf("model = %s", c.Model)
	}
}

func TestApplyHarnessProviderConfigWritesModel(t *testing.T) {
	isolateOpenCodeEnv(t)
	if err := ApplyHarnessProviderConfig(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(opencodeConfigDir(), "opencode.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), ZenFreeModel) {
		t.Fatalf("missing model in %s", body)
	}
	if err := setJSONPath(path, "model", "keep/me"); err != nil {
		t.Fatal(err)
	}
	if err := ApplyHarnessProviderConfig(); err != nil {
		t.Fatal(err)
	}
	body, _ = os.ReadFile(path)
	if !strings.Contains(string(body), "keep/me") {
		t.Fatalf("should not clobber existing model: %s", body)
	}
}

func TestDoctorFreeModelUnavailable(t *testing.T) {
	isolateOpenCodeEnv(t)
	if err := SaveProfile(Profile{Agent: "opencode", HintInErrors: true}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	prev := zenModelsURL
	zenModelsURL = srv.URL
	t.Cleanup(func() { zenModelsURL = prev })

	r, err := Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, f := range r.Findings {
		if f.ID == "opencode.zen" {
			found = true
			if f.OK {
				t.Fatalf("want fail, got %+v", f)
			}
			if !strings.Contains(f.Summary, "free model unavailable") {
				t.Fatalf("summary = %q", f.Summary)
			}
		}
	}
	if !found {
		t.Fatalf("missing opencode.zen finding: %+v", r.Findings)
	}
	if r.OK {
		t.Fatal("doctor should fail when free model is unavailable")
	}
}

func TestDoctorFreeModelAvailable(t *testing.T) {
	isolateOpenCodeEnv(t)
	if err := SaveProfile(Profile{Agent: "opencode", HintInErrors: true}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"id":"big-pickle"}]}`))
	}))
	t.Cleanup(srv.Close)
	prev := zenModelsURL
	zenModelsURL = srv.URL
	t.Cleanup(func() { zenModelsURL = prev })

	r, err := Doctor(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, f := range r.Findings {
		if f.ID == "opencode.zen" {
			found = true
			if !f.OK {
				t.Fatalf("want ok, got %+v", f)
			}
		}
	}
	if !found {
		t.Fatalf("missing opencode.zen finding: %+v", r.Findings)
	}
}

func TestRankedCLIAgentsIncludesBundledOpenCode(t *testing.T) {
	isolateOpenCodeEnv(t)
	ranked := RankedCLIAgents()
	if len(ranked) == 0 {
		t.Fatal("expected bundled opencode fallback")
	}
	last := ranked[len(ranked)-1]
	if !last.Bundled || last.Name != "opencode" {
		t.Fatalf("want bundled opencode last, got %+v", last)
	}
	label := agentAuthLabel(last)
	if !strings.Contains(label, ZenFreeDisclosure) {
		t.Fatalf("bundled label missing disclosure: %s", label)
	}
	if !strings.Contains(harnessAgentDescription(ranked), "ANTHROPIC_API_KEY") {
		t.Fatal("form description should mention keyed-provider alternative")
	}
}

func TestOpenCodeDownloadURLNeverLatest(t *testing.T) {
	prevAVX, prevMusl := openCodeHasAVX2, openCodeIsMusl
	t.Cleanup(func() {
		openCodeHasAVX2, openCodeIsMusl = prevAVX, prevMusl
	})
	openCodeHasAVX2 = func() bool { return true }
	openCodeIsMusl = func() bool { return false }
	openCodeTestDownloadURL = ""
	u, err := openCodeDownloadURL(OpenCodeVersion)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(u, "/latest") {
		t.Fatalf("must not use releases/latest: %s", u)
	}
	if !strings.Contains(u, "/v"+OpenCodeVersion+"/") {
		t.Fatalf("missing pinned tag: %s", u)
	}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" && !strings.HasSuffix(u, "opencode-darwin-arm64.zip") {
		t.Fatalf("unexpected darwin arm64 url: %s", u)
	}
}
