package assist_tests

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode"
)

// TestAssist is the entry point for the tmux-driven interactive harness. It
// mirrors TestCLI's flag conventions:
//
//	go test -v -run TestAssist                      # all cases
//	go test -v -run TestAssist -- "1,2,3"           # cases 01, 02, 03
//	go test -v -run TestAssist -- "5+"              # case 05 onward
//	go test -v -run TestAssist -- --debug "1-4"
//	go test -v -run TestAssist -- --keep-tmux       # leave tmux session alive on failure only
//
// Prerequisites: tmux ≥ 3.2, and a built sling binary (see BinaryPath).
// Cases: tests/assist/cases/*.yaml
func TestAssist(t *testing.T) {
	requireTmux(t)

	// --- argv parsing (matches TestCLI conventions) ---
	keepTmux := false
	debug := false
	selector := ""
	for _, arg := range os.Args {
		switch arg {
		case "--debug", "-d":
			debug = true
		case "--keep-tmux":
			keepTmux = true
		}
		if arg != "" && unicode.IsDigit(rune(arg[0])) {
			selector = arg
		}
	}

	bin, err := BinaryPath()
	if err != nil {
		t.Fatalf("missing sling binary (run `go build .` in cmd/sling first): %v", err)
	}
	if debug {
		t.Logf("using sling binary: %s", bin)
	}

	// Cases live next to this package: tests/assist/cases/
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	casesDir := filepath.Join(wd, "cases")
	cases, err := LoadCases(casesDir)
	if err != nil {
		t.Fatalf("load cases: %v", err)
	}
	if len(cases) == 0 {
		t.Fatalf("no cases found in %s", casesDir)
	}

	wantedIDs := parseSelector(selector)
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })

	for _, c := range cases {
		if len(wantedIDs) > 0 && !wantedIDs[c.ID] {
			continue
		}
		c := c
		testName := fmt.Sprintf("%02d_%s", c.ID, sanitize(c.Name))
		t.Run(testName, func(t *testing.T) {
			runCase(t, c, bin, keepTmux, debug)
		})
	}
}

// runCase orchestrates one tmux-driven scenario.
func runCase(t *testing.T, c Case, bin string, keepTmux, debug bool) {
	t.Helper()

	homeDir := SafeHomeDir(t, fmt.Sprintf("sling-ai-%02d", c.ID))
	HardFailIfNotInTemp(t, homeDir)

	sessionName := fmt.Sprintf("sling-ai-%02d", c.ID)
	var session *TmuxSession
	// On success always clean up. --keep-tmux retains session + home only
	// when the case fails (for interactive debugging).
	t.Cleanup(func() {
		failed := t.Failed()
		if session != nil && !(keepTmux && failed) {
			session.Kill()
		}
		if !(keepTmux && failed) {
			_ = os.RemoveAll(homeDir)
		}
		if keepTmux && failed {
			t.Logf("session left alive: tmux attach -t %s", sessionName)
			t.Logf("home dir left intact: %s", homeDir)
		}
	})

	// Apply seed before tmux starts so the shell sees a fully-baked $HOME.
	if c.Seed != "" {
		seedPath := filepath.Join("fixtures", c.Seed)
		if _, err := os.Stat(seedPath); err == nil {
			if err := copyTreeInto(seedPath, homeDir); err != nil {
				t.Fatalf("seed: %v", err)
			}
		} else {
			t.Fatalf("seed dir missing: %s", seedPath)
		}
	}

	envOverrides := map[string]string{}
	for k, v := range c.Env {
		envOverrides[k] = strings.ReplaceAll(v, "$HOME", homeDir)
	}

	// Make the sling binary directly available as `sling` via a tiny shim
	// in $HOME/bin. Mock agents go in the same dir so they shadow any
	// system-installed claude/codex.
	if err := writeShim(filepath.Join(homeDir, "bin", "sling"), bin); err != nil {
		t.Fatalf("shim: %v", err)
	}

	session = NewTmuxSession(t, sessionName, homeDir, envOverrides)

	// Default mock agents. Per-case overrides land on top.
	if !c.NoMockAgents {
		session.WriteDefaultMockAgents()
	}
	for name, ma := range c.MockAgents {
		session.WriteMockAgent(name, ma)
	}

	timeout := 30 * time.Second
	if c.TimeoutS > 0 {
		timeout = time.Duration(c.TimeoutS) * time.Second
	}
	deadline := time.Now().Add(timeout)

	// Run each step in order. Anything that captures the screen does so
	// after a stable-read settle.
	for i, step := range c.Order {
		if time.Now().After(deadline) {
			t.Fatalf("step %d: case timeout exceeded", i+1)
		}
		execStep(t, session, step, debug)
	}

	// Case-level exit code assertion (after the last interactive step).
	if c.AssertExitCode != nil {
		if err := session.CheckExitCode(*c.AssertExitCode, 5*time.Second); err != nil {
			t.Error(err)
		}
	}

	// Filesystem assertions (after the session has done its work).
	ApplyFileAsserts(t, homeDir, c.AssertFiles)
}

// execStep runs one step with reasonable defaults.
func execStep(t *testing.T, s *TmuxSession, step Step, debug bool) {
	t.Helper()
	switch {
	case step.Run != "":
		s.SendLine(step.Run)
		s.CaptureStable(2 * time.Second)
	case step.SendKey != "":
		s.SendKey(step.SendKey)
	case len(step.SendKeys) > 0:
		for _, k := range step.SendKeys {
			s.SendKey(k)
		}
	case step.Type != "":
		s.SendType(step.Type)
	case step.WaitFor != "":
		// Default 15s — setup/install and agent launch regularly exceed 5s
		// under load; cases can override via timeout_ms.
		dur := 15 * time.Second
		if step.TimeoutMs > 0 {
			dur = time.Duration(step.TimeoutMs) * time.Millisecond
		}
		out, ok := s.WaitFor(step.WaitFor, dur)
		if !ok {
			t.Errorf("wait_for %q timed out\nFinal screen:\n%s", step.WaitFor, out)
		}
	case step.ExpectContains != "":
		out, ok := s.WaitFor(step.ExpectContains, 15*time.Second)
		if !ok {
			t.Errorf("expect_contains %q not found\nGot:\n%s", step.ExpectContains, out)
		}
	case step.ExpectNotContains != "":
		out := s.CaptureStable(500 * time.Millisecond)
		if strings.Contains(out, step.ExpectNotContains) {
			t.Errorf("expect_not_contains %q matched\nGot:\n%s", step.ExpectNotContains, out)
		}
	case step.ExpectExit != nil:
		// Wait for the *expanded* marker, not the typed command line.
		if err := s.CheckExitCode(*step.ExpectExit, 5*time.Second); err != nil {
			t.Error(err)
		}
	case step.SleepMs > 0:
		// Prefer wait_for anchors; sleep is a last resort for UI settle.
		time.Sleep(time.Duration(step.SleepMs) * time.Millisecond)
	}
	if debug {
		t.Logf("step done — screen tail:\n%s", tailScreen(s.Capture(), 10))
	}
}

func tailScreen(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) <= n {
		return s
	}
	return strings.Join(lines[len(lines)-n:], "\n")
}

func parseSelector(sel string) map[int]bool {
	out := map[int]bool{}
	if sel == "" {
		return out
	}
	for _, tok := range strings.Split(sel, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		switch {
		case strings.HasSuffix(tok, "+"):
			start := atoi(strings.TrimSuffix(tok, "+"))
			for i := start; i < start+1000; i++ {
				out[i] = true
			}
		case strings.Contains(tok, "-"):
			parts := strings.SplitN(tok, "-", 2)
			a := atoi(parts[0])
			b := atoi(parts[1])
			for i := a; i <= b; i++ {
				out[i] = true
			}
		default:
			if n := atoi(tok); n > 0 {
				out[n] = true
			}
		}
	}
	return out
}

func atoi(s string) int {
	n := 0
	fmt.Sscanf(s, "%d", &n)
	return n
}

func sanitize(s string) string {
	out := strings.Builder{}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out.WriteRune(r)
		case r == ' ' || r == '_' || r == '-':
			out.WriteRune('_')
		}
	}
	return out.String()
}

// writeShim drops a tiny shell shim that exec's the real sling binary. We
// use a shim rather than a symlink so $HOME/bin/sling works inside the tmux
// session even if the underlying path is unusual.
func writeShim(path, target string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	body := fmt.Sprintf("#!/usr/bin/env bash\nexec %q \"$@\"\n", target)
	return os.WriteFile(path, []byte(body), 0o755)
}

// copyTreeInto recursively copies src/* into dst.
func copyTreeInto(src, dst string) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, info.Mode())
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode())
	})
}

func TestTmuxVersionAtLeast(t *testing.T) {
	if !tmuxVersionAtLeast("3.5a", "3.2") {
		t.Fatal("3.5a should be >= 3.2")
	}
	if tmuxVersionAtLeast("2.9", "3.2") {
		t.Fatal("2.9 should be < 3.2")
	}
	if !tmuxVersionAtLeast("3.2", "3.2") {
		t.Fatal("equal should pass")
	}
}

func TestParseSelector(t *testing.T) {
	got := parseSelector("1,3-5,10+")
	for _, id := range []int{1, 3, 4, 5, 10, 11} {
		if !got[id] {
			t.Errorf("missing %d", id)
		}
	}
	if got[2] {
		t.Error("2 should not be selected")
	}
}
