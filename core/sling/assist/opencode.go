package assist

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"golang.org/x/sys/cpu"
)

// OpenCodeVersion is the pinned CLI release. Override with OPENCODE_VERSION.
// Pin checked against https://github.com/anomalyco/opencode/releases (v1.18.18, 2026-08-13).
const OpenCodeVersion = "1.18.18"

// ZenFreeDisclosure is shown when the harness uses a free Zen model.
const ZenFreeDisclosure = "free-model prompts may be used for training"

// ZenFreeModel is the default free OpenCode Zen model id (`provider/model`).
const ZenFreeModel = "opencode/big-pickle"

const openCodeGitHubBase = "https://github.com/anomalyco/opencode/releases/download/v{version}/{asset}"

// openCodeTestDownloadURL replaces the GitHub asset URL in tests (httptest zip).
var openCodeTestDownloadURL string

// zenModelsURL is the Zen catalog. Tests point this at httptest.
var zenModelsURL = "https://opencode.ai/zen/v1/models"

var zenHTTPClient = &http.Client{Timeout: 5 * time.Second}

var (
	openCodeHasAVX2 = func() bool { return cpu.X86.HasAVX2 }
	openCodeIsMusl  = linuxMuslPresent
)

type openCodeInstall struct {
	version string
}

func newOpenCodeInstall() *openCodeInstall {
	return &openCodeInstall{version: openCodeVersion()}
}

func openCodeVersion() string {
	if val := strings.TrimSpace(os.Getenv("OPENCODE_VERSION")); val != "" {
		return strings.TrimPrefix(val, "v")
	}
	return OpenCodeVersion
}

func openCodeBinName() string {
	if runtime.GOOS == "windows" {
		return "opencode.exe"
	}
	return "opencode"
}

func (o *openCodeInstall) dest() string {
	return filepath.Join(env.HomeBinDir(), "opencode", o.version)
}

func (o *openCodeInstall) bundledPath() string {
	return filepath.Join(o.dest(), openCodeBinName())
}

// BundledOpenCodePath is ~/.sling/bin/opencode/<version>/opencode[.exe].
func BundledOpenCodePath() string {
	return newOpenCodeInstall().bundledPath()
}

func linuxMuslPresent() bool {
	matches, _ := filepath.Glob("/lib/ld-musl-*")
	return len(matches) > 0
}

func (o *openCodeInstall) assetName(goos, goarch string) (string, error) {
	var osName, arch string
	switch goos {
	case "darwin", "linux", "windows":
		osName = goos
	default:
		return "", g.Error("opencode is not available for %s/%s", goos, goarch)
	}
	switch goarch {
	case "amd64":
		arch = "x64"
	case "arm64":
		arch = "arm64"
	default:
		return "", g.Error("opencode is not available for %s/%s", goos, goarch)
	}

	ext := "zip"
	if goos == "linux" {
		ext = "tar.gz"
	}

	suffix := ""
	if arch == "x64" && !openCodeHasAVX2() {
		suffix += "-baseline"
	}
	if goos == "linux" && openCodeIsMusl() {
		suffix += "-musl"
	}
	return fmt.Sprintf("opencode-%s-%s%s.%s", osName, arch, suffix, ext), nil
}

// OpenCodeAssetName is the GitHub asset for goos/goarch (pinned layout, not /latest).
func OpenCodeAssetName(goos, goarch string) (string, error) {
	return newOpenCodeInstall().assetName(goos, goarch)
}

func (o *openCodeInstall) downloadURL() (string, error) {
	if openCodeTestDownloadURL != "" {
		return openCodeTestDownloadURL, nil
	}
	asset, err := o.assetName(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}
	return g.R(openCodeGitHubBase, "version", o.version, "asset", asset), nil
}

func openCodeDownloadURL(version string) (string, error) {
	return (&openCodeInstall{version: version}).downloadURL()
}

func versionMatches(out, version string) bool {
	s := strings.TrimSpace(out)
	return strings.HasPrefix(s, version) || strings.HasPrefix(s, "v"+version)
}

func (o *openCodeInstall) versionOK(binPath string) (bool, error) {
	out, err := exec.Command(binPath, "--version").CombinedOutput()
	if err != nil {
		return false, g.Error(err, "could not get version for opencode: %s", strings.TrimSpace(string(out)))
	}
	return versionMatches(string(out), o.version), nil
}

func (o *openCodeInstall) findBin(folder string) string {
	want := openCodeBinName()
	direct := filepath.Join(folder, want)
	if g.PathExists(direct) {
		return direct
	}
	var found string
	_ = filepath.Walk(folder, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		if info.Name() == want || info.Name() == "opencode" {
			found = p
			return filepath.SkipAll
		}
		return nil
	})
	return found
}

func (o *openCodeInstall) extract(archive, dest string) error {
	if strings.HasSuffix(archive, ".tar.gz") || strings.HasSuffix(archive, ".tgz") {
		if err := g.ExtractTarGz(archive, dest); err != nil {
			return g.Error(err, "error extracting opencode archive")
		}
		return nil
	}
	if _, err := iop.Unzip(archive, dest); err != nil {
		return g.Error(err, "error unzipping opencode archive")
	}
	return nil
}

// EnsureBinOpenCode returns a usable opencode binary.
// Order: OPENCODE_PATH, $PATH, then a versioned download under ~/.sling/bin/opencode/<version>/.
func EnsureBinOpenCode() (binPath string, err error) {
	return newOpenCodeInstall().ensure()
}

func (o *openCodeInstall) ensure() (binPath string, err error) {
	if envPath := os.Getenv("OPENCODE_PATH"); envPath != "" {
		if !g.PathExists(envPath) {
			return "", g.Error("opencode binary not found: %s", envPath)
		}
		if stat, _ := os.Stat(envPath); stat != nil && stat.IsDir() {
			return "", g.Error("OPENCODE_PATH provided is a directory, should be a file: %s", envPath)
		}
		return envPath, nil
	}

	if p, err := exec.LookPath("opencode"); err == nil {
		return p, nil
	}

	folderPath := o.dest()
	binPath = o.bundledPath()
	found := g.PathExists(binPath)
	if found {
		ok, verr := o.versionOK(binPath)
		if verr != nil {
			found = false
		} else {
			found = ok
		}
	}

	if !found {
		downloadURL, uerr := o.downloadURL()
		if uerr != nil {
			return "", uerr
		}

		ext := ".zip"
		if strings.Contains(downloadURL, ".tar.gz") {
			ext = ".tar.gz"
		}
		archivePath := filepath.Join(os.TempDir(), g.F("opencode-%s%s", o.version, ext))
		defer os.Remove(archivePath)

		g.Info("downloading opencode %s for %s/%s", o.version, runtime.GOOS, runtime.GOARCH)
		if err = net.DownloadFile(downloadURL, archivePath); err != nil {
			return "", g.Error(err, "unable to download opencode binary")
		}

		if err = os.MkdirAll(folderPath, 0755); err != nil {
			return "", g.Error(err, "could not create opencode folder")
		}

		if err = o.extract(archivePath, folderPath); err != nil {
			return "", err
		}

		foundBin := o.findBin(folderPath)
		if foundBin == "" {
			return "", g.Error("cannot find opencode binary at %s after extraction", binPath)
		}
		if foundBin != binPath {
			if err = os.Rename(foundBin, binPath); err != nil {
				return "", g.Error(err, "could not move opencode binary to %s", binPath)
			}
		}
		if !g.PathExists(binPath) {
			return "", g.Error("cannot find opencode binary at %s after extraction", binPath)
		}
		if err = os.Chmod(binPath, 0755); err != nil {
			return "", g.Error(err, "could not make opencode executable")
		}
	}

	ok, err := o.versionOK(binPath)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", g.Error("opencode at %s does not report version %s", binPath, o.version)
	}
	return binPath, nil
}

// ProviderChoice is one harness LLM setup (Zen free vs keyed provider).
type ProviderChoice struct {
	Kind       string         // zen-free | anthropic | openai | google | xai
	Model      string         // provider/model
	Provider   map[string]any // opencode.json `provider` map; nil if unused
	Disclosure string         // set for zen-free
}

func envFirst(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return k
		}
	}
	return ""
}

func keyedProvider(kind, model, envKey string) ProviderChoice {
	return ProviderChoice{
		Kind:  kind,
		Model: model,
		Provider: map[string]any{
			kind: map[string]any{
				"options": map[string]any{
					"apiKey": "{env:" + envKey + "}",
				},
			},
		},
	}
}

// HarnessProviderChoice picks a keyed provider env if set, else free Zen.
func HarnessProviderChoice() ProviderChoice {
	if k := envFirst("ANTHROPIC_API_KEY"); k != "" {
		return keyedProvider("anthropic", "anthropic/claude-sonnet-4-5", k)
	}
	if k := envFirst("OPENAI_API_KEY"); k != "" {
		return keyedProvider("openai", "openai/gpt-4o", k)
	}
	if k := envFirst("GEMINI_API_KEY", "GOOGLE_API_KEY"); k != "" {
		return keyedProvider("google", "google/gemini-2.0-flash", k)
	}
	if k := envFirst("XAI_API_KEY"); k != "" {
		return keyedProvider("xai", "xai/grok-3", k)
	}
	return ProviderChoice{
		Kind:       "zen-free",
		Model:      ZenFreeModel,
		Disclosure: ZenFreeDisclosure,
	}
}

// HarnessProviderConfig is the opencode.json fragment for the harness choice.
func HarnessProviderConfig() map[string]any {
	c := HarnessProviderChoice()
	out := map[string]any{"model": c.Model}
	if len(c.Provider) > 0 {
		out["provider"] = c.Provider
	}
	return out
}

// ApplyHarnessProviderConfig writes model (and provider) when missing.
func ApplyHarnessProviderConfig() error {
	path := filepath.Join(opencodeConfigDir(), "opencode.json")
	doc, err := jsonReadOrEmpty(path)
	if err != nil {
		return err
	}
	cfg := HarnessProviderConfig()
	if _, ok := doc["model"]; !ok {
		if err := setJSONPath(path, "model", cfg["model"]); err != nil {
			return err
		}
	}
	if _, has := doc["provider"]; !has {
		if p, ok := cfg["provider"]; ok {
			if err := setJSONPath(path, "provider", p); err != nil {
				return err
			}
		}
	}
	return nil
}

func opencodeRelevant() bool {
	if (&opencodeClient{}).Detect() {
		return true
	}
	prof, exists, err := LoadProfile()
	if err != nil || !exists {
		return false
	}
	return prof.Agent == "opencode"
}

var zenFreeModelTokens = []string{
	"big-pickle",
	"mimo-v2.5-free",
	"hy3-free",
	"nemotron-3-ultra-free",
	"nemotron-3.5-lightning-free",
	"muse-spark-1.2-contributor-free",
}

// ProbeZenFreeModel reports whether the free Zen catalog is reachable.
func ProbeZenFreeModel() (ok bool, detail string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, zenModelsURL, nil)
	if err != nil {
		return false, err.Error()
	}
	resp, err := zenHTTPClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	s := strings.ToLower(string(body))
	for _, tok := range zenFreeModelTokens {
		if strings.Contains(s, strings.ToLower(tok)) {
			return true, tok
		}
	}
	return false, "no free model in catalog"
}

func (r *DoctorReport) addZenFinding() {
	if r == nil || !opencodeRelevant() {
		return
	}
	if HarnessProviderChoice().Kind != "zen-free" {
		return
	}
	ok, detail := ProbeZenFreeModel()
	if ok {
		r.AddFinding(DoctorFinding{
			ID:      "opencode.zen",
			OK:      true,
			Summary: "free model available",
			Detail:  detail,
		})
		return
	}
	r.AddFinding(DoctorFinding{
		ID:      "opencode.zen",
		OK:      false,
		Summary: "free model unavailable",
		Detail:  detail,
		Hint:    "set ANTHROPIC_API_KEY or OPENAI_API_KEY for a keyed provider",
	})
}
