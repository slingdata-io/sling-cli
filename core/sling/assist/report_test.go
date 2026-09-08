package assist

import (
	"encoding/base64"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio"
)

func writeReportFixture(t *testing.T, id string) string {
	t.Helper()
	withTempHomeDir(t)
	errMsg := "could not read /Users/alice/secret/file.csv from host"
	WriteFailureSnapshot(FailureSnapshot{
		ExecID:     id,
		ErrMsg:     errMsg,
		ConfigPath: "/Users/alice/secret/repl.yaml",
		SignMeta:   SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbSnowflake},
		RunLog: strings.Join([]string{
			"opened https://db.internal.example.com/sync",
			"path=/Users/alice/secret/file.csv",
			"failed to copy rows",
		}, "\n"),
	})
	dir := findLocalExecDir(id)
	if dir == "" {
		t.Fatal("fixture dir missing")
	}
	return dir
}

func TestComposeReportGoldenDraft(t *testing.T) {
	writeReportFixture(t, "exec_report1")
	d, err := ComposeReport("exec_report1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Title, " (postgres→snowflake)") {
		t.Fatalf("title = %q", d.Title)
	}
	md := d.BodyMarkdown()
	descAt := strings.Index(md, "## Description")
	cfgAt := strings.Index(md, "## Replication Configuration")
	logAt := strings.Index(md, "## Log Output")
	if descAt < 0 || cfgAt < 0 || logAt < 0 || !(descAt < cfgAt && cfgAt < logAt) {
		t.Fatalf("markdown section order:\n%s", md)
	}
	if strings.Contains(d.LogExcerpt, "/Users/") {
		t.Fatalf("path leaked in LogExcerpt: %q", d.LogExcerpt)
	}
	if strings.Contains(d.LogExcerpt, "db.internal.example.com") {
		t.Fatalf("hostname leaked in LogExcerpt: %q", d.LogExcerpt)
	}
	if !strings.Contains(d.LogExcerpt, "<path>") && !strings.Contains(d.LogExcerpt, "<url>") {
		t.Fatalf("expected placeholders in LogExcerpt: %q", d.LogExcerpt)
	}
	if strings.Contains(d.Config, "/Users/") {
		t.Fatalf("path leaked in Config: %q", d.Config)
	}
	if strings.Contains(md, d.SignatureID) {
		t.Fatalf("signature must not appear in markdown body")
	}
}

func TestComposeReportKeepsDebugStack(t *testing.T) {
	withTempHomeDir(t)
	errMsg := strings.Join([]string{
		"~ could not connect",
		"--- database.go:123 Connect ---",
		"~ failed to ping",
		"--- task_run.go:140 Execute ---",
		"connection refused",
	}, "\n")
	WriteFailureSnapshot(FailureSnapshot{
		ExecID:   "exec_stack1",
		ErrMsg:   errMsg,
		SignMeta: SignMeta{SourceType: dbio.TypeDbPostgres, TargetType: dbio.TypeDbSnowflake},
	})
	d, err := ComposeReport("exec_stack1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(d.Description, "--- database.go:123 Connect ---") {
		t.Fatalf("debug stack stripped from Description:\n%s", d.Description)
	}
	if !strings.Contains(d.Description, "--- task_run.go:140 Execute ---") {
		t.Fatalf("debug stack stripped from Description:\n%s", d.Description)
	}
	if strings.Contains(d.Description, "could not connect failed to ping") {
		t.Fatalf("debug stack collapsed to one line:\n%s", d.Description)
	}
	md := d.BodyMarkdown()
	if !strings.Contains(md, "--- task_run.go:140 Execute ---") {
		t.Fatalf("debug stack stripped from body:\n%s", md)
	}
}

func TestComposeReportRedactsPathAndHost(t *testing.T) {
	writeReportFixture(t, "exec_redact1")
	d, err := ComposeReport("exec_redact1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(d.LogExcerpt, "/Users/alice") {
		t.Fatalf("raw path in excerpt: %q", d.LogExcerpt)
	}
	if strings.Contains(d.LogExcerpt, "db.internal.example.com") {
		t.Fatalf("raw host in excerpt: %q", d.LogExcerpt)
	}
}

func TestComposeReportPrefixID(t *testing.T) {
	writeReportFixture(t, "exec_prefix_abc")
	if _, err := ComposeReport("exec_pre"); err != nil {
		t.Fatal(err)
	}
}

func TestGitHubIssueURLBudget(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 400; i++ {
		b.WriteString("line ")
		b.WriteString(strings.Repeat("x", 80))
		b.WriteByte('\n')
	}
	d := ReportDraft{
		Title:       "boom (postgres→snowflake)",
		Description: "kept description",
		Config:      "repl.yaml",
		LogExcerpt:  b.String(),
		Version:     "1.4.24",
		OS:          "Mac",
	}
	u := d.GitHubIssueURL()
	if len(u) > githubIssueURLMax {
		t.Fatalf("url len %d > %d", len(u), githubIssueURLMax)
	}
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	if q.Get("template") != "" {
		t.Fatalf("template param must be absent for body prefill, got %q", q.Get("template"))
	}
	body := q.Get("body")
	if !strings.Contains(body, "kept description") {
		t.Fatalf("description trimmed from body")
	}
	if !strings.Contains(body, "Sling version: 1.4.24") {
		t.Fatalf("version missing from body")
	}
	if !strings.Contains(body, "OS: Mac") {
		t.Fatalf("os missing from body")
	}
	if !strings.Contains(body, "```") {
		t.Fatal("logs stripped entirely from body")
	}
}

func TestContactFormURLNoTrim(t *testing.T) {
	d := ReportDraft{
		Title:       "boom",
		Description: "kept",
		Config:      strings.Repeat("col: value\n", 4000),
		LogExcerpt:  strings.Repeat("log line\n", 4000),
		Version:     "1.4.24",
		OS:          "Mac",
	}
	u := d.ContactFormURL()
	if !strings.HasPrefix(u, contactFormBaseURL+"?issue=") {
		t.Fatalf("bad prefix: %q", u[:60])
	}
	// Over-limit bodies must stay complete; delivery falls back to a file.
	if len(u) <= contactFormURLMax {
		t.Fatalf("expected over-limit url, got %d", len(u))
	}
	enc := strings.TrimPrefix(u, contactFormBaseURL+"?issue=")
	decoded, err := base64.RawURLEncoding.DecodeString(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(decoded), "log line") || !strings.Contains(string(decoded), "col: value") {
		t.Fatal("body trimmed")
	}
}

func TestDeliverContactFormOversizedWritesFile(t *testing.T) {
	d := ReportDraft{
		Title:       "boom",
		Description: "kept",
		Config:      strings.Repeat("col: value\n", 4000),
		LogExcerpt:  "log",
		Version:     "1.4.24",
		OS:          "Mac",
	}
	// Writes to stdout; failure surfaces as an error return.
	if err := deliverContactForm(d, false); err != nil {
		t.Fatal(err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDeliverGitHubIssueOversizedFallsBackToEmail(t *testing.T) {
	d := ReportDraft{
		Title:             "boom",
		Description:       "kept",
		CustomDescription: strings.Repeat("context ", 4000),
		Config:            "cfg",
		LogExcerpt:        "log",
		Version:           "1.4.24",
		OS:                "Mac",
	}
	if n := len(d.GitHubIssueURL()); n <= githubIssueURLMax {
		t.Fatalf("fixture not over github budget: %d", n)
	}
	out := captureStdout(t, func() {
		if err := deliverGitHubIssue(d, false); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, "sending via email instead") {
		t.Fatalf("missing email fallback:\n%s", out)
	}
	if strings.Contains(out, githubIssueBaseURL) {
		t.Fatalf("still printed a GitHub URL:\n%s", out)
	}
}

func TestDeliverGitHubIssueFitsStaysOnGitHub(t *testing.T) {
	d := ReportDraft{
		Title:       "boom",
		Description: "kept",
		Config:      "cfg",
		LogExcerpt:  "log",
		Version:     "1.4.24",
		OS:          "Mac",
	}
	u := d.GitHubIssueURL()
	if len(u) > githubIssueURLMax {
		t.Fatalf("fixture over github budget: %d", len(u))
	}
	out := captureStdout(t, func() {
		if err := deliverGitHubIssue(d, false); err != nil {
			t.Fatal(err)
		}
	})
	if !strings.Contains(out, githubIssueBaseURL) {
		t.Fatalf("missing GitHub URL:\n%s", out)
	}
	if strings.Contains(out, "sending via email instead") {
		t.Fatalf("should not fall back to email:\n%s", out)
	}
}

func TestGitHubIssueURLDropdownAndEscape(t *testing.T) {
	d := ReportDraft{
		Title:       "hash # and café",
		Description: "line1\nline2 #frag",
		Config:      "a=b",
		LogExcerpt:  "ok",
		OS:          "FreeBSD",
		Version:     "dev",
	}
	u := d.GitHubIssueURL()
	parsed, err := url.Parse(u)
	if err != nil {
		t.Fatal(err)
	}
	q := parsed.Query()
	body := q.Get("body")
	if !strings.Contains(body, "line1\nline2 #frag") {
		t.Fatalf("description unescape = %q", body)
	}
	if !strings.Contains(body, "OS: FreeBSD") {
		t.Fatalf("non-dropdown os should stay in body text, got %q", body)
	}
	if q.Get("title") != "hash # and café" {
		t.Fatalf("title unescape = %q", q.Get("title"))
	}
}

func TestComposeReportConnTestHasNoConfig(t *testing.T) {
	withTempHomeDir(t)
	WriteFailureSnapshot(FailureSnapshot{
		ExecID:   "exec_conn1",
		ErrMsg:   "could not connect",
		ConnName: "MY_PG",
		SignMeta: SignMeta{SourceType: dbio.TypeDbPostgres},
	})
	dir := findLocalExecDir("exec_conn1")
	if dir == "" {
		t.Fatal("fixture dir missing")
	}
	if _, err := os.Stat(filepath.Join(dir, "config.snapshot.yaml")); !os.IsNotExist(err) {
		t.Fatal("conns test must not write config.snapshot.yaml")
	}
	d, err := ComposeReport("exec_conn1")
	if err != nil {
		t.Fatal(err)
	}
	if d.ConnName != "MY_PG" {
		t.Fatalf("ConnName=%q", d.ConnName)
	}
	if d.Config != "" {
		t.Fatalf("config text must be empty for conns test, got %q", d.Config)
	}
	md := d.BodyMarkdown()
	if !strings.Contains(md, "Connection: MY_PG") {
		t.Fatalf("missing connection line:\n%s", md)
	}
	if strings.Contains(md, "## Replication Configuration") {
		t.Fatalf("conns test must not include config section:\n%s", md)
	}
	if strings.Contains(md, "Target:") {
		t.Fatalf("conns test must not invent a target:\n%s", md)
	}
}

func TestHandleReportComposeRedacts(t *testing.T) {
	writeReportFixture(t, "exec_mcp1")
	d, err := ComposeReport("exec_mcp1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(findLocalExecDir("exec_mcp1"), "stderr.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "/Users/alice") {
		t.Fatal("fixture missing raw path")
	}
	if strings.Contains(d.LogExcerpt, "/Users/alice") {
		t.Fatalf("compose leaked path: %q", d.LogExcerpt)
	}
}

func TestComposeReportFromRedactsSecrets(t *testing.T) {
	d := ReportParts{
		ExecID:    "exec_platform1",
		ErrorText: "could not read /Users/alice/secret/file.csv from host",
		LogText: strings.Join([]string{
			"opened https://user:s3cretpass@db.internal.example.com/sync",
			"path=/Users/alice/secret/file.csv",
			"failed to copy rows",
		}, "\n"),
		ConfigBody:        "source: postgres\nurl: https://user:s3cretpass@db.internal.example.com/prod\n",
		Version:           "1.4.24",
		OS:                "Linux",
		SourceType:        "postgres",
		TargetType:        "snowflake",
		CustomDescription: "reproduced on first stream",
	}.ComposeReport()
	if d.SignatureID == "" {
		t.Fatal("missing signature")
	}
	if !strings.Contains(d.Title, "postgres→snowflake") {
		t.Fatalf("title = %q", d.Title)
	}
	md := d.BodyMarkdown()
	for _, secret := range []string{"/Users/alice", "db.internal.example.com", "s3cretpass"} {
		if strings.Contains(d.LogExcerpt, secret) {
			t.Fatalf("secret %q leaked in LogExcerpt: %q", secret, d.LogExcerpt)
		}
		if strings.Contains(d.Config, secret) {
			t.Fatalf("secret %q leaked in Config: %q", secret, d.Config)
		}
		if strings.Contains(md, secret) {
			t.Fatalf("secret %q leaked in markdown", secret)
		}
		if strings.Contains(d.GitHubIssueURL(), secret) {
			t.Fatalf("secret %q leaked in GitHub URL", secret)
		}
	}
	if !strings.Contains(md, "reproduced on first stream") {
		t.Fatalf("custom description missing:\n%s", md)
	}
}

func TestComposeReportFromContactFormFits(t *testing.T) {
	small := ReportParts{
		ExecID:    "exec_small",
		ErrorText: "boom",
		LogText:   "log",
		Version:   "1.4.24",
		OS:        "Mac",
	}.ComposeReport()
	if !small.ContactFormFits() {
		t.Fatal("small report should fit")
	}
	big := ReportDraft{
		Title:       "boom",
		Description: "kept",
		Config:      strings.Repeat("col: value\n", 4000),
		LogExcerpt:  strings.Repeat("log line\n", 4000),
		Version:     "1.4.24",
		OS:          "Mac",
	}
	if big.ContactFormFits() {
		t.Fatal("oversized report should not fit")
	}
	if n := len(big.GitHubIssueURL()); n > githubIssueURLMax {
		t.Fatalf("github url len %d > %d", n, githubIssueURLMax)
	}
}
