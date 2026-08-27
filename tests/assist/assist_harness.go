// Package assist_tests is the tmux-driven harness for `sling assist` interactive flows.
//
// File naming note: this file is intentionally NOT *_test.go because Go's test
// machinery would scope it to a single package. We want to import it from
// assist_test.go (same package) so the harness helpers are reusable across
// cases without being public API.
//
// Invocation (from repo root, after building the binary):
//
//	cd cmd/sling && go build -o sling .
//	SLING_BIN=$PWD/sling go test -v ./tests/assist
//
// Or from this directory with a relative binary:
//
//	SLING_BIN=../../cmd/sling/sling go test -v .
//
// Cases live in ./cases/*.yaml; fixtures in ./fixtures/.
package assist_tests

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/sling/assist"
	"gopkg.in/yaml.v3"
)

// Case is one tmux-driven test scenario, loaded from cases/<NN>.<name>.yaml.
type Case struct {
	ID   int    // parsed from filename "NN.<name>.yaml"
	Path string // absolute path to the YAML file
	Name string `yaml:"name"`
	Seed string `yaml:"seed"`
	Env  map[string]string `yaml:"env"`
	// MockAgents overrides default stub agent scripts ($HOME/bin/<name>).
	MockAgents map[string]MockAgent `yaml:"mock_agents"`
	// NoMockAgents disables the harness's default stub-agent install. Use
	// for cases that assert "no AI agent on $PATH" — otherwise the stubs
	// trip codex's PATH-based Detect().
	NoMockAgents bool `yaml:"no_mock_agents"`
	TimeoutS     int  `yaml:"timeout_s"`
	// Order is the sequence of tmux steps the harness drives. Named "order"
	// (not "steps") to avoid colliding with pipeline/replication step
	// terminology used elsewhere in this repo's YAML configs.
	Order       []Step       `yaml:"order"`
	AssertFiles []FileAssert `yaml:"assert_files"`
	// AssertExitCode, when set, checks $? of the last shell command after
	// all order steps complete (via echo of a unique marker).
	AssertExitCode *int `yaml:"assert_exit_code"`
}

// MockAgent overrides the default stub binary content for one agent.
type MockAgent struct {
	ExitCode int    `yaml:"exit_code"`
	Stdout   string `yaml:"stdout"`
}

// Step is one ordered action in the case script. The YAML uses one key per
// step type — exactly one of the fields below is populated.
type Step struct {
	Run               string   `yaml:"run"`
	SendKey           string   `yaml:"send_key"`
	SendKeys          []string `yaml:"send_keys"`
	Type              string   `yaml:"type"`
	WaitFor           string   `yaml:"wait_for"`
	ExpectContains    string   `yaml:"expect_contains"`
	ExpectNotContains string   `yaml:"expect_not_contains"`
	ExpectScreen      string   `yaml:"expect_screen"`
	ExpectExit        *int     `yaml:"expect_exit"`
	SleepMs           int      `yaml:"sleep_ms"`
	TimeoutMs         int      `yaml:"timeout_ms"`
}

// FileAssert is a post-run filesystem check.
type FileAssert struct {
	Path               string   `yaml:"path"`
	Contains           []string `yaml:"contains"`
	MD5MatchesEmbedded bool     `yaml:"md5_matches_embedded"`
	IsRedirectTo       string   `yaml:"is_redirect_to"`
	// NotEmpty only checks the file is non-empty (prefer this over a no-op
	// md5 flag when embed comparison is not needed).
	NotEmpty bool `yaml:"not_empty"`
}

// LoadCases discovers all cases/*.yaml in the test data dir.
func LoadCases(dir string) ([]Case, error) {
	out := []Case{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".yaml") {
			continue
		}
		if e.IsDir() {
			continue
		}
		path := filepath.Join(dir, name)
		c := Case{Path: path}
		// Parse ID prefix "NN."
		num := 0
		fmt.Sscanf(name, "%d.", &num)
		c.ID = num
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		// Reject known-removed schema keys so silent no-ops cannot return.
		raw := map[string]any{}
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if _, ok := raw["mock_platform"]; ok {
			return nil, fmt.Errorf("%s: mock_platform is not supported (platform investigate is not wired); remove the key", name)
		}
		if err := yaml.Unmarshal(data, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		if c.Name == "" {
			c.Name = strings.TrimSuffix(name, ".yaml")
		}
		out = append(out, c)
	}
	return out, nil
}

// TmuxSession owns one detached tmux session. Each case gets a fresh one.
type TmuxSession struct {
	t        *testing.T
	Name     string // session name, e.g. "sling-ai-01"
	HomeDir  string // ephemeral $HOME for this session
	BinDir   string // $HOME/bin (mock agents installed here)
	Logger   *bytes.Buffer
	keyDelay time.Duration
}

// minTmuxVersion is required for `tmux new-session -e KEY=VAL`.
const minTmuxVersion = "3.2"

// requireTmux checks tmux is on PATH and is new enough for -e env flags.
func requireTmux(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux not available — skipping interactive assist tests")
	}
	out, err := exec.Command("tmux", "-V").Output()
	if err != nil {
		t.Skipf("tmux -V failed: %v", err)
	}
	// "tmux 3.5a" / "tmux 3.2" → major.minor
	ver := strings.TrimSpace(string(out))
	fields := strings.Fields(ver)
	if len(fields) < 2 {
		t.Skipf("unrecognized tmux version output: %q", ver)
	}
	if !tmuxVersionAtLeast(fields[1], minTmuxVersion) {
		t.Skipf("tmux %s < %s required for new-session -e", fields[1], minTmuxVersion)
	}
}

// tmuxVersionAtLeast compares dotted version prefixes (ignores letter suffixes).
func tmuxVersionAtLeast(have, want string) bool {
	hp := parseVersionPrefix(have)
	wp := parseVersionPrefix(want)
	for i := 0; i < len(wp); i++ {
		var h int
		if i < len(hp) {
			h = hp[i]
		}
		if h > wp[i] {
			return true
		}
		if h < wp[i] {
			return false
		}
	}
	return true
}

var versionNum = regexp.MustCompile(`\d+`)

func parseVersionPrefix(s string) []int {
	parts := versionNum.FindAllString(s, -1)
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, _ := strconv.Atoi(p)
		out = append(out, n)
		if len(out) >= 3 {
			break
		}
	}
	return out
}

// NewTmuxSession spawns a fresh detached tmux session with a pinned 120x40
// pane size, the given env, and a tmp $HOME. Caller MUST call Kill() in a
// defer to clean up (unless deliberately retaining the session).
func NewTmuxSession(t *testing.T, name string, homeDir string, envOverrides map[string]string) *TmuxSession {
	t.Helper()
	if !strings.HasPrefix(homeDir, os.TempDir()) {
		t.Fatalf("refusing to run TmuxSession with HOME=%q outside temp dir", homeDir)
	}
	binDir := filepath.Join(homeDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &TmuxSession{
		t:        t,
		Name:     name,
		HomeDir:  homeDir,
		BinDir:   binDir,
		Logger:   &bytes.Buffer{},
		keyDelay: 50 * time.Millisecond,
	}

	// Build the env list. Start clean — we only forward what the harness
	// needs, plus per-case overrides. PATH order: $HOME/bin (mock agents +
	// sling shim) → /usr/bin:/bin (coreutils). We deliberately exclude
	// /opt/homebrew/bin and similar so any system-installed `sling` cannot
	// shadow our shim if PATH is reset by a profile somewhere.
	//
	// NO_COLOR must be non-empty (https://no-color.org/); empty string is
	// ignored by most libraries.
	env := map[string]string{
		"HOME":           homeDir,
		"SLING_HOME_DIR": filepath.Join(homeDir, ".sling"),
		"PATH":           binDir + string(os.PathListSeparator) + "/usr/bin:/bin",
		"TERM":           "xterm-256color",
		"SHELL":          "/bin/bash",
		"PS1":            "$ ",
		"NO_COLOR":       "1",
		"SLING_LOGGING":  "NO_COLOR",
		"SLING_ASSIST_HINT": "true",
		"AGENT_BROWSER_SKIP_DOWNLOAD": "1",
	}
	for k, v := range envOverrides {
		env[k] = v
	}
	envArgs := []string{}
	for k, v := range env {
		envArgs = append(envArgs, "-e", fmt.Sprintf("%s=%s", k, v))
	}

	// Write a tiny rcfile that pins PATH inside the shell. macOS's
	// /etc/bashrc otherwise resets PATH for interactive shells, which
	// would shadow our $HOME/bin/sling shim with the system-installed
	// sling. The rc re-exports HOME/PATH/PS1 from the env tmux already
	// gave us.
	rcPath := filepath.Join(homeDir, ".harness-bashrc")
	rcBody := fmt.Sprintf(`export PATH=%q
export HOME=%q
export SLING_HOME_DIR=%q
export PS1='$ '
export NO_COLOR=1
export SLING_LOGGING=NO_COLOR
`, env["PATH"], homeDir, env["SLING_HOME_DIR"])
	if err := os.WriteFile(rcPath, []byte(rcBody), 0o644); err != nil {
		t.Fatalf("write rcfile: %v", err)
	}

	args := append([]string{"new-session", "-d", "-s", name, "-x", "120", "-y", "40"}, envArgs...)
	args = append(args, "bash", "--noprofile", "--rcfile", rcPath, "-i")
	if err := exec.Command("tmux", args...).Run(); err != nil {
		t.Fatalf("tmux new-session: %v", err)
	}
	// give tmux a tick to spin up the shell
	time.Sleep(200 * time.Millisecond)
	return s
}

// Kill ends the tmux session unconditionally.
func (s *TmuxSession) Kill() {
	_ = exec.Command("tmux", "kill-session", "-t", s.Name).Run()
}

// SendLine sends a literal command followed by Enter.
func (s *TmuxSession) SendLine(line string) {
	s.t.Helper()
	if err := exec.Command("tmux", "send-keys", "-t", s.Name, "-l", line).Run(); err != nil {
		s.t.Fatalf("tmux send-keys -l: %v", err)
	}
	s.SendKey("Enter")
}

// SendKey sends one named tmux key (e.g. Enter, Up, C-c).
func (s *TmuxSession) SendKey(key string) {
	s.t.Helper()
	if err := exec.Command("tmux", "send-keys", "-t", s.Name, key).Run(); err != nil {
		s.t.Fatalf("tmux send-keys %s: %v", key, err)
	}
	time.Sleep(s.keyDelay)
}

// SendType sends a literal string (no key interpretation), no Enter.
func (s *TmuxSession) SendType(text string) {
	s.t.Helper()
	if err := exec.Command("tmux", "send-keys", "-t", s.Name, "-l", text).Run(); err != nil {
		s.t.Fatalf("tmux send-keys -l type: %v", err)
	}
	time.Sleep(s.keyDelay)
}

// Capture returns the current pane content as a single string.
func (s *TmuxSession) Capture() string {
	out, err := exec.Command("tmux", "capture-pane", "-t", s.Name, "-p", "-J").Output()
	if err != nil {
		s.t.Logf("capture-pane: %v", err)
		return ""
	}
	return string(out)
}

// CaptureStable polls capture-pane until two consecutive snapshots match
// (Bubble Tea integration-test idiom) or timeout elapses. Returns the final
// snapshot.
func (s *TmuxSession) CaptureStable(timeout time.Duration) string {
	deadline := time.Now().Add(timeout)
	last := s.Capture()
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		now := s.Capture()
		if now == last {
			return now
		}
		last = now
	}
	return last
}

// WaitFor polls capture-pane until the substring appears or timeout elapses.
// Returns the matching snapshot, or the empty string on timeout.
func (s *TmuxSession) WaitFor(substr string, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out := s.Capture()
		if strings.Contains(out, substr) {
			return out, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return s.Capture(), false
}

// CheckExitCode echoes $? with a unique marker and waits for the *expanded*
// form so we do not race the typed command line.
func (s *TmuxSession) CheckExitCode(want int, timeout time.Duration) error {
	// Unique marker avoids matching the echoed command line (which contains
	// the literal $? before expansion).
	marker := fmt.Sprintf("__ASSIST_EXIT_%d__", want)
	// Force expansion: print only after $? is substituted by the shell.
	s.SendLine(fmt.Sprintf(`printf '%%s\n' "__ASSIST_EXIT_$?__"`))
	out, ok := s.WaitFor(marker, timeout)
	if !ok {
		return fmt.Errorf("expect_exit %d: marker %q not found\nGot:\n%s", want, marker, out)
	}
	return nil
}

// WriteMockAgent installs a stub script at $HOME/bin/<agent>. The stub echoes
// `[stub <agent>] <args>` then exits with the configured exit_code.
func (s *TmuxSession) WriteMockAgent(name string, ma MockAgent) {
	s.t.Helper()
	stdout := ma.Stdout
	if stdout == "" {
		stdout = fmt.Sprintf("[stub %s] received prompt", name)
	}
	exitCode := ma.ExitCode
	body := fmt.Sprintf(`#!/usr/bin/env bash
echo "%s"
# Drain stdin only when the caller piped data in (stdin is not a tty).
# When we inherit the tmux terminal stdin (argv-mode launch), reading
# would block forever, so skip.
if [ ! -t 0 ] && [ ! -c /dev/stdin ]; then
  cat - >/dev/null 2>&1 || true
fi
exit %d
`, strings.ReplaceAll(stdout, `"`, `\"`), exitCode)
	path := filepath.Join(s.BinDir, name)
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		s.t.Fatalf("write mock %s: %v", name, err)
	}
}

// WriteDefaultMockAgents installs success-stubs for every CLI agent name so
// $PATH lookups don't fall through to a system-installed agent.
func (s *TmuxSession) WriteDefaultMockAgents() {
	for _, name := range []string{"claude", "codex", "gemini", "cursor", "opencode", "pi", "grok"} {
		s.WriteMockAgent(name, MockAgent{ExitCode: 0})
	}
}

// ApplyFileAsserts runs the assert_files block after the case has finished.
func ApplyFileAsserts(t *testing.T, homeDir string, asserts []FileAssert) {
	t.Helper()
	for _, fa := range asserts {
		path := strings.ReplaceAll(fa.Path, "$HOME", homeDir)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("assert_files: missing %s: %v", path, err)
			continue
		}
		body := string(data)
		if fa.NotEmpty && len(data) == 0 {
			t.Errorf("assert_files %s: expected non-empty file", path)
		}
		for _, want := range fa.Contains {
			if !strings.Contains(body, want) {
				t.Errorf("assert_files %s: missing substring %q\nGot: %s", path, want, body)
			}
		}
		if fa.IsRedirectTo != "" {
			expected := strings.ReplaceAll(fa.IsRedirectTo, "$HOME", homeDir)
			line := strings.TrimSpace(body)
			if !strings.HasPrefix(line, "@") || !strings.Contains(line, expected) {
				t.Errorf("assert_files %s: expected redirect to %s, got %q", path, expected, body)
			}
		}
		if fa.MD5MatchesEmbedded {
			// File must live under ~/.agents/skills/<rel> so we can map to
			// the embedded skills FS path <rel>.
			skillsRoot := filepath.Join(homeDir, ".agents", "skills")
			rel, err := filepath.Rel(skillsRoot, path)
			if err != nil || strings.HasPrefix(rel, "..") {
				t.Errorf("assert_files md5_matches_embedded: %s is not under ~/.agents/skills/", path)
				continue
			}
			rel = filepath.ToSlash(rel)
			wantMD5, err := assist.MD5OfEmbeddedSkill(rel)
			if err != nil {
				t.Errorf("assert_files md5_matches_embedded: embedded %s: %v", rel, err)
				continue
			}
			gotMD5 := MD5OfBytes(data)
			if gotMD5 != wantMD5 {
				t.Errorf("assert_files md5_matches_embedded: %s md5=%s want embedded %s", path, gotMD5, wantMD5)
			}
		}
	}
}

// MD5OfBytes is a small helper exposed for case authors / debug.
func MD5OfBytes(b []byte) string {
	sum := md5.Sum(b)
	return hex.EncodeToString(sum[:])
}

// SafeHomeDir returns a tmp dir under os.TempDir() with the right shape for
// the harness. Bails the test if it can't create the dir.
func SafeHomeDir(t *testing.T, prefix string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", prefix+"-")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// HardFailIfNotInTemp panics if path doesn't live under os.TempDir(). Used as
// a belt-and-braces guard in case a test mishandles a path.
func HardFailIfNotInTemp(t *testing.T, path string) {
	t.Helper()
	if !strings.HasPrefix(path, os.TempDir()) {
		t.Fatalf("refusing operation on %q (outside %s)", path, os.TempDir())
	}
}

// BinaryPath returns the absolute path to the freshly built sling binary.
// Default: ./sling relative to the process CWD (usually tests/assist/ when
// invoked as `go test .` from that dir, or set SLING_BIN to an absolute path).
//
// Recommended: SLING_BIN=$PWD/cmd/sling/sling go test -v ./tests/assist
func BinaryPath() (string, error) {
	bin := os.Getenv("SLING_BIN")
	if bin == "" {
		// Prefer sibling cmd/sling/sling when tests run from repo root via
		// go test ./tests/assist (CWD is package dir = tests/assist).
		candidates := []string{"./sling", "../../cmd/sling/sling", "../cmd/sling/sling"}
		for _, c := range candidates {
			abs, err := filepath.Abs(c)
			if err != nil {
				continue
			}
			if _, err := os.Stat(abs); err == nil {
				return abs, nil
			}
		}
		bin = "./sling"
	}
	abs, err := filepath.Abs(bin)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(abs); err != nil {
		return "", fmt.Errorf("sling binary not found at %s (build with: cd cmd/sling && go build -o sling .): %w", abs, err)
	}
	return abs, nil
}
