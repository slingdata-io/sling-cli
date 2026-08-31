package assist

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/env"
)

// AgentBrowserVersion is the pinned CLI release. Override with AGENT_BROWSER_VERSION.
// Pin checked against https://github.com/vercel-labs/agent-browser/releases (v0.34.0, 2026-08-11).
const AgentBrowserVersion = "0.34.0"

const agentBrowserGitHubBase = "https://github.com/vercel-labs/agent-browser/releases/download/v{version}/{asset}"

const agentBrowserMCPName = "agent-browser"

// agentBrowserTestDownloadURL replaces the GitHub asset URL in tests.
var agentBrowserTestDownloadURL string

type agentBrowserInstall struct {
	version string
}

func newAgentBrowserInstall() *agentBrowserInstall {
	return &agentBrowserInstall{version: agentBrowserVersion()}
}

func agentBrowserVersion() string {
	if val := strings.TrimSpace(os.Getenv("AGENT_BROWSER_VERSION")); val != "" {
		return strings.TrimPrefix(val, "v")
	}
	return AgentBrowserVersion
}

func agentBrowserBinName() string {
	if runtime.GOOS == "windows" {
		return "agent-browser.exe"
	}
	return "agent-browser"
}

func (a *agentBrowserInstall) dest() string {
	return filepath.Join(env.HomeBinDir(), "agent-browser", a.version)
}

func (a *agentBrowserInstall) bundledPath() string {
	return filepath.Join(a.dest(), agentBrowserBinName())
}

// BundledAgentBrowserPath is ~/.sling/bin/agent-browser/<version>/agent-browser[.exe].
func BundledAgentBrowserPath() string {
	return newAgentBrowserInstall().bundledPath()
}

func (a *agentBrowserInstall) assetName(goos, goarch string) (string, error) {
	var osName, arch string
	switch goos {
	case "darwin":
		osName = "darwin"
	case "linux":
		osName = "linux"
	case "windows":
		osName = "win32"
	default:
		return "", g.Error("agent-browser is not available for %s/%s", goos, goarch)
	}
	switch goarch {
	case "amd64":
		arch = "x64"
	case "arm64":
		arch = "arm64"
	default:
		return "", g.Error("agent-browser is not available for %s/%s", goos, goarch)
	}
	if goos == "windows" && goarch == "arm64" {
		// Upstream publishes win32-x64 only; x64 binary runs under emulation.
		arch = "x64"
	}
	if goos == "linux" && linuxMuslPresent() {
		return fmt.Sprintf("agent-browser-linux-musl-%s", arch), nil
	}
	name := fmt.Sprintf("agent-browser-%s-%s", osName, arch)
	if goos == "windows" {
		name += ".exe"
	}
	return name, nil
}

// AgentBrowserAssetName is the GitHub asset for goos/goarch (pinned layout, not /latest).
func AgentBrowserAssetName(goos, goarch string) (string, error) {
	return newAgentBrowserInstall().assetName(goos, goarch)
}

func (a *agentBrowserInstall) downloadURL() (string, error) {
	if agentBrowserTestDownloadURL != "" {
		return agentBrowserTestDownloadURL, nil
	}
	asset, err := a.assetName(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	return g.R(agentBrowserGitHubBase, "version", a.version, "asset", asset), nil
}

func (a *agentBrowserInstall) versionOK(binPath string) (bool, error) {
	out, err := exec.Command(binPath, "--version").CombinedOutput()
	if err != nil {
		return false, g.Error(err, "could not get version for agent-browser: %s", strings.TrimSpace(string(out)))
	}
	s := strings.TrimSpace(string(out))
	return strings.Contains(s, a.version), nil
}

// agentBrowserBin is the command written into MCP configs.
// Order: AGENT_BROWSER_PATH, $PATH, bundled binary, then the bare name.
func agentBrowserBin() string {
	if envPath := strings.TrimSpace(os.Getenv("AGENT_BROWSER_PATH")); envPath != "" {
		return envPath
	}
	if p, err := exec.LookPath("agent-browser"); err == nil {
		return p
	}
	bundled := BundledAgentBrowserPath()
	if g.PathExists(bundled) {
		return bundled
	}
	return "agent-browser"
}

func agentBrowserMCPEntry() map[string]any {
	return map[string]any{
		"command": agentBrowserBin(),
		"args":    []any{"mcp", "--tools", "core"},
	}
}

func opencodeAgentBrowserMCPEntry() map[string]any {
	return map[string]any{
		"type":    "local",
		"command": []any{agentBrowserBin(), "mcp", "--tools", "core"},
		"enabled": true,
	}
}

func skipAgentBrowserDownload() bool {
	return os.Getenv("AGENT_BROWSER_SKIP_DOWNLOAD") == "1"
}

// EnsureBinAgentBrowser returns a usable agent-browser binary.
// Order: AGENT_BROWSER_PATH, $PATH, then a versioned download under ~/.sling/bin/agent-browser/<version>/.
func EnsureBinAgentBrowser() (binPath string, err error) {
	return newAgentBrowserInstall().ensure()
}

func (a *agentBrowserInstall) ensure() (binPath string, err error) {
	if envPath := strings.TrimSpace(os.Getenv("AGENT_BROWSER_PATH")); envPath != "" {
		if !g.PathExists(envPath) {
			return "", g.Error("agent-browser binary not found: %s", envPath)
		}
		if stat, _ := os.Stat(envPath); stat != nil && stat.IsDir() {
			return "", g.Error("AGENT_BROWSER_PATH provided is a directory, should be a file: %s", envPath)
		}
		return envPath, nil
	}

	if p, err := exec.LookPath("agent-browser"); err == nil {
		return p, nil
	}

	if skipAgentBrowserDownload() {
		return "agent-browser", nil
	}

	folderPath := a.dest()
	binPath = a.bundledPath()
	found := g.PathExists(binPath)
	if found {
		ok, verr := a.versionOK(binPath)
		if verr != nil {
			found = false
		} else {
			found = ok
		}
	}

	if !found {
		downloadURL, uerr := a.downloadURL()
		if uerr != nil {
			return "", uerr
		}

		if err = os.MkdirAll(folderPath, 0755); err != nil {
			return "", g.Error(err, "could not create agent-browser folder")
		}

		tmpPath := binPath + ".download"
		defer os.Remove(tmpPath)

		g.Info("downloading agent-browser %s for %s/%s", a.version, runtime.GOOS, runtime.GOARCH)
		if err = net.DownloadFile(downloadURL, tmpPath); err != nil {
			return "", g.Error(err, "unable to download agent-browser binary")
		}
		if err = os.Rename(tmpPath, binPath); err != nil {
			return "", g.Error(err, "could not move agent-browser binary to %s", binPath)
		}
		if err = os.Chmod(binPath, 0755); err != nil {
			return "", g.Error(err, "could not make agent-browser executable")
		}
	}

	ok, err := a.versionOK(binPath)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", g.Error("agent-browser at %s does not report version %s", binPath, a.version)
	}
	return binPath, nil
}

func maybeEnsureAgentBrowser(opts InstallOptions) error {
	return newAgentBrowserInstall().maybeEnsure(opts)
}

func (a *agentBrowserInstall) maybeEnsure(opts InstallOptions) error {
	if skipAgentBrowserDownload() {
		return nil
	}
	bin, err := a.ensure()
	if err != nil {
		return err
	}
	return a.maybeInstallChrome(bin, opts)
}

func (a *agentBrowserInstall) chromeLikelyPresent() bool {
	if g.PathExists(filepath.Join(userHome(), ".agent-browser", "browsers")) {
		return true
	}
	for _, name := range []string{"google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"} {
		if _, err := exec.LookPath(name); err == nil {
			return true
		}
	}
	switch runtime.GOOS {
	case "darwin":
		return g.PathExists("/Applications/Google Chrome.app")
	case "windows":
		for _, p := range []string{
			filepath.Join(os.Getenv("PROGRAMFILES"), "Google", "Chrome", "Application", "chrome.exe"),
			filepath.Join(os.Getenv("PROGRAMFILES(X86)"), "Google", "Chrome", "Application", "chrome.exe"),
		} {
			if p != "" && g.PathExists(p) {
				return true
			}
		}
	}
	return false
}

func (a *agentBrowserInstall) maybeInstallChrome(bin string, opts InstallOptions) error {
	if os.Getenv("AGENT_BROWSER_SKIP_CHROME") == "1" {
		return nil
	}
	if a.chromeLikelyPresent() {
		return nil
	}
	yes := os.Getenv("SLING_AGENT_BROWSER_YES") == "1"
	if opts.NonInteractive && !yes {
		g.Info("Chrome for Testing is not installed. After setup, run: %s install", bin)
		return nil
	}
	if !yes && !env.IsInteractiveTerminal() {
		g.Info("Chrome for Testing is not installed. After setup, run: %s install", bin)
		return nil
	}
	g.Info("downloading Chrome for Testing via agent-browser install")
	cmd := exec.Command(bin, "install")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		g.Warn("agent-browser install failed: %v (MCP is still wired; run `%s install` later)", err, bin)
	}
	return nil
}

func tomlQuote(s string) string {
	return strconv.Quote(s)
}

func tomlStringArray(ss []string) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = tomlQuote(s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func writeJSONMCP(path, serversKey string) error {
	if err := setJSONPath(path, serversKey+".sling", slingMCPEntry()); err != nil {
		return err
	}
	return setJSONPath(path, serversKey+".agent-browser", agentBrowserMCPEntry())
}

func removeJSONMCP(path, serversKey string) error {
	if err := deleteJSONPath(path, serversKey+".sling"); err != nil {
		return err
	}
	return deleteJSONPath(path, serversKey+".agent-browser")
}
