package assist

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func isolateAgentBrowserEnv(t *testing.T) {
	t.Helper()
	home := withTempHomeDir(t)
	bin := filepath.Join(home, "empty-bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin)
	t.Setenv("AGENT_BROWSER_PATH", "")
	t.Setenv("AGENT_BROWSER_VERSION", AgentBrowserVersion)
	t.Setenv("AGENT_BROWSER_SKIP_DOWNLOAD", "")
	t.Setenv("AGENT_BROWSER_SKIP_CHROME", "1")
	t.Cleanup(func() { agentBrowserTestDownloadURL = "" })
}

func stubAgentBrowserScript(version string) string {
	return "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo \"" + version + "\"; exit 0; fi\n" +
		"if [ \"$1\" = \"install\" ]; then echo chrome-stub; exit 0; fi\n" +
		"echo stub\n"
}

func serveAgentBrowserBin(t *testing.T, version string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	payload := []byte(stubAgentBrowserScript(version))
	hits := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(srv.Close)
	return srv, hits
}

func TestAgentBrowserAssetNamePinnedPlatforms(t *testing.T) {
	cases := map[string]string{
		"darwin/arm64":  "agent-browser-darwin-arm64",
		"darwin/amd64":  "agent-browser-darwin-x64",
		"linux/arm64":   "agent-browser-linux-arm64",
		"linux/amd64":   "agent-browser-linux-x64",
		"windows/amd64": "agent-browser-win32-x64.exe",
		"windows/arm64": "agent-browser-win32-x64.exe",
	}
	for plat, want := range cases {
		if strings.HasPrefix(plat, "linux/") && linuxMuslPresent() {
			continue
		}
		parts := strings.Split(plat, "/")
		got, err := AgentBrowserAssetName(parts[0], parts[1])
		if err != nil {
			t.Fatalf("%s: %v", plat, err)
		}
		if got != want {
			t.Errorf("%s: got %s want %s", plat, got, want)
		}
	}
}

func TestAgentBrowserAssetNameUnsupported(t *testing.T) {
	if _, err := AgentBrowserAssetName("plan9", "amd64"); err == nil {
		t.Fatal("expected error for plan9")
	}
}

func TestEnsureBinAgentBrowserDownloadsOnce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stub is a POSIX shell script")
	}
	isolateAgentBrowserEnv(t)
	srv, hits := serveAgentBrowserBin(t, AgentBrowserVersion)
	agentBrowserTestDownloadURL = srv.URL

	p1, err := EnsureBinAgentBrowser()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(p1, agentBrowserBinName()) {
		t.Fatalf("path=%s", p1)
	}
	p2, err := EnsureBinAgentBrowser()
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Fatalf("path changed %s -> %s", p1, p2)
	}
	if hits.Load() != 1 {
		t.Fatalf("downloads=%d want 1", hits.Load())
	}
}

func TestEnsureBinAgentBrowserPathEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("stub is a POSIX shell script")
	}
	isolateAgentBrowserEnv(t)
	dir := t.TempDir()
	stub := filepath.Join(dir, "agent-browser")
	if err := os.WriteFile(stub, []byte(stubAgentBrowserScript(AgentBrowserVersion)), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_BROWSER_PATH", stub)
	got, err := EnsureBinAgentBrowser()
	if err != nil {
		t.Fatal(err)
	}
	if got != stub {
		t.Fatalf("got %s want %s", got, stub)
	}
}

func TestEnsureBinAgentBrowserSkipDownload(t *testing.T) {
	isolateAgentBrowserEnv(t)
	t.Setenv("AGENT_BROWSER_SKIP_DOWNLOAD", "1")
	got, err := EnsureBinAgentBrowser()
	if err != nil {
		t.Fatal(err)
	}
	if got != "agent-browser" {
		t.Fatalf("got %s", got)
	}
}

func TestAgentBrowserSkillEmbedded(t *testing.T) {
	names := listSkillNames()
	found := false
	for _, n := range names {
		if n == "agent-browser" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("listSkillNames missing agent-browser: %v", names)
	}
	stub, err := SkillsFS.ReadFile("skills/agent-browser/SKILL.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stub), "CORE.md") {
		t.Fatal("SKILL.md must point at CORE.md")
	}
	if !strings.Contains(string(stub), "sling assist setup") {
		t.Fatal("SKILL.md must tell the agent about sling assist setup")
	}
	core, err := SkillsFS.ReadFile("skills/agent-browser/CORE.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(core), "The core loop") {
		t.Fatal("CORE.md missing official core loop")
	}
}

func TestAgentBrowserMCPEntryUsesResolvedBin(t *testing.T) {
	isolateAgentBrowserEnv(t)
	t.Setenv("AGENT_BROWSER_PATH", "/tmp/custom-agent-browser")
	entry := agentBrowserMCPEntry()
	if entry["command"] != "/tmp/custom-agent-browser" {
		t.Fatalf("command=%v", entry["command"])
	}
	args, _ := entry["args"].([]any)
	if len(args) != 3 || args[0] != "mcp" || args[1] != "--tools" || args[2] != "core" {
		t.Fatalf("args=%v", args)
	}
}
