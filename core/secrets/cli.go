package secrets

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// cliRunner runs one vendor CLI. It never uses a shell: each argument is
// passed as is, so a reference path cannot inject a command.
type cliRunner struct {
	bin    string   // name on PATH, or a path
	hint   string   // install hint for a missing binary
	env    []string // extra KEY=VALUE for the child
	redact func(string) string
}

func newCLIRunner(cfg ProviderConfig, bin, hint string) *cliRunner {
	if p := cfg.Get("cli_path"); p != "" {
		bin = p
	}
	return &cliRunner{bin: bin, hint: hint, redact: cfg.Redact}
}

// setEnv adds KEY=VALUE to the child env when value is set. Use it to pass
// tokens: never pass a secret through argv.
func (c *cliRunner) setEnv(key, value string) {
	if value != "" {
		c.env = append(c.env, key+"="+value)
	}
}

// Installed is true when the binary is on PATH.
func (c *cliRunner) Installed() bool {
	_, err := exec.LookPath(c.bin)
	return err == nil
}

// Run executes the CLI and returns stdout.
func (c *cliRunner) Run(ctx context.Context, stdin []byte, args ...string) ([]byte, error) {
	path, err := exec.LookPath(c.bin)
	if err != nil {
		msg := fmt.Sprintf("%q is not installed", c.bin)
		if c.hint != "" {
			msg += ". " + c.hint
		}
		return nil, errors.New(msg)
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = append(os.Environ(), c.env...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s timed out", c.bin)
		}
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		if c.redact != nil {
			msg = c.redact(msg)
		}
		return nil, fmt.Errorf("%s failed: %s", c.bin, truncate(msg, 500))
	}
	return stdout.Bytes(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
