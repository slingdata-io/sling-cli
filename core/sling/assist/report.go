package assist

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/charmbracelet/huh"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

const (
	githubIssueURLMax = 8000
	// Cloudflare rejects request lines over ~16KB. Stay well under it.
	contactFormURLMax  = 12000
	githubIssueBaseURL = "https://github.com/slingdata-io/sling-cli/issues/new"
	contactFormBaseURL = "https://slingdata.io/contact/"
	maxLogExcerptBytes = 64 * 1024
)

// ReportDraft is the composed, redacted report. Both routes consume it.
type ReportDraft struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Config      string `json:"config"`
	LogExcerpt  string `json:"log_excerpt"`
	Version     string `json:"version"`
	OS          string `json:"os"`
	SourceType  string `json:"source_type"`
	TargetType  string `json:"target_type"`
	SignatureID string `json:"signature_id"`
	Skeleton    string `json:"skeleton"`
	ExecID      string `json:"exec_id"`
	ConnName    string `json:"conn_name,omitempty"`
	// CustomDescription is caller-supplied context, shown above the error.
	CustomDescription string `json:"custom_description,omitempty"`
}

// ComposeReport builds a redacted report from a local failure snapshot.
func ComposeReport(execID string) (ReportDraft, error) {
	le, err := ResolveLocalExec(execID)
	if err != nil {
		return ReportDraft{}, err
	}

	errText := readSnapshotFile(le.LogDir, "error.txt")
	runLog := readSnapshotFile(le.LogDir, "stderr.log")
	meta := map[string]any{}
	if b, err := os.ReadFile(filepath.Join(le.LogDir, "meta.json")); err == nil {
		_ = json.Unmarshal(b, &meta)
	}

	src := cast.ToString(meta["source_type"])
	tgt := cast.ToString(meta["target_type"])
	label := cast.ToString(meta["error_short_label"])
	configPath := cast.ToString(meta["config_path"])
	if configPath == "" {
		configPath = cast.ToString(meta["object"]) // legacy snapshots
	}
	connName := cast.ToString(meta["conn_name"])
	sigID := strings.ToLower(cast.ToString(meta["error_signature"]))

	skel := Skeleton(errText)
	if sigID == "" || len(sigID) != CompositeIDLen {
		sig := SignError(errText, SignMeta{SourceType: dbio.Type(src), TargetType: dbio.Type(tgt)})
		sigID = sig.ID
		if label == "" {
			label = sig.ShortLabel
		}
		if src == "" {
			src = string(sig.Meta.SourceType)
		}
		if tgt == "" {
			tgt = string(sig.Meta.TargetType)
		}
	}
	if label == "" {
		label = ShortLabel(skel)
	}

	d := ReportDraft{
		Title:       reportTitle(label, src, tgt),
		Description: reportDescription(redactForReport(errText)),
		Config:      reportConfig(configPath, connName, le.LogDir),
		LogExcerpt:  reportLogExcerpt(redactForReport(runLog)),
		Version:     core.Version,
		OS:          reportOSName(),
		SourceType:  src,
		TargetType:  tgt,
		SignatureID: sigID,
		Skeleton:    skel,
		ExecID:      le.ID,
		ConnName:    connName,
	}
	return d, nil
}

// ReportParts is raw evidence collected outside a local snapshot.
// ComposeReport redacts it the same way the exec-id ComposeReport
// redacts on-disk files.
type ReportParts struct {
	ExecID            string
	ErrorText         string
	LogText           string
	ConfigBody        string
	Version           string
	OS                string
	SourceType        string
	TargetType        string
	ConnName          string
	CustomDescription string
}

// ComposeReport builds a redacted ReportDraft from already-collected fields.
func (p ReportParts) ComposeReport() ReportDraft {
	src := p.SourceType
	tgt := p.TargetType
	skel := Skeleton(p.ErrorText)
	sig := SignError(p.ErrorText, SignMeta{SourceType: dbio.Type(src), TargetType: dbio.Type(tgt)})
	label := sig.ShortLabel
	if src == "" {
		src = string(sig.Meta.SourceType)
	}
	if tgt == "" {
		tgt = string(sig.Meta.TargetType)
	}
	version := p.Version
	if version == "" {
		version = core.Version
	}
	osName := p.OS
	if osName == "" {
		osName = reportOSName()
	}
	config := strings.TrimSpace(redactForReport(p.ConfigBody))
	if config == "" {
		config = "(not captured)"
	}
	return ReportDraft{
		Title:             reportTitle(label, src, tgt),
		Description:       reportDescription(redactForReport(p.ErrorText)),
		Config:            config,
		LogExcerpt:        reportLogExcerpt(redactForReport(p.LogText)),
		Version:           version,
		OS:                osName,
		SourceType:        src,
		TargetType:        tgt,
		SignatureID:       sig.ID,
		Skeleton:          skel,
		ExecID:            p.ExecID,
		ConnName:          p.ConnName,
		CustomDescription: p.CustomDescription,
	}
}

func reportTitle(label, src, tgt string) string {
	if label == "" {
		label = "unknown_error"
	}
	return fmt.Sprintf("%s (%s→%s)", label, typeToken(dbio.Type(src)), typeToken(dbio.Type(tgt)))
}

func reportDescription(redactedErr string) string {
	redactedErr = strings.TrimSpace(redactedErr)
	if redactedErr == "" {
		return "(no error message captured)"
	}
	// Keep the debug message stack (caller frames + wrap messages), not a
	// collapsed one-line summary. GitHub URL budget trims logs/config first.
	return redactedErr
}

func reportConfig(configPath, connName, logDir string) string {
	if connName != "" {
		return ""
	}
	snap := filepath.Join(logDir, "config.snapshot.yaml")
	body := ""
	if b, err := os.ReadFile(snap); err == nil {
		body = strings.TrimSpace(redactForReport(string(b)))
	}
	// Fall back to the config path on disk (snapshots written before
	// config.snapshot.yaml existed).
	if body == "" && isConfigFilePath(configPath) {
		if b, err := os.ReadFile(configPath); err == nil && int64(len(b)) <= 64*1024 {
			body = strings.TrimSpace(redactForReport(string(b)))
		}
	}
	if body != "" {
		return body
	}
	return "(not captured)"
}

func isConfigFilePath(path string) bool {
	return g.In(strings.ToLower(filepath.Ext(path)), ".yaml", ".yml", ".json")
}

func reportLogExcerpt(redacted string) string {
	redacted = strings.TrimRight(redacted, "\n")
	if redacted == "" {
		return "(no log captured)"
	}
	if len(redacted) <= maxLogExcerptBytes {
		return redacted
	}
	s := redacted[len(redacted)-maxLogExcerptBytes:]
	if i := strings.IndexByte(s, '\n'); i >= 0 && i < 200 {
		s = s[i+1:]
	}
	return "[...truncated...]\n" + s
}

func reportOSName() string {
	switch runtime.GOOS {
	case "linux":
		return "Linux"
	case "darwin":
		return "Mac"
	case "windows":
		return "Windows"
	default:
		return runtime.GOOS
	}
}

func readSnapshotFile(dir, name string) string {
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return ""
	}
	return string(b)
}

// redactForReport scrubs secrets, then replaces only URLs and file paths.
// Identifiers, numbers and timestamps stay readable for debugging.
func redactForReport(s string) string {
	s = env.ScrubLine(s)
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		line = reQuotedURL.ReplaceAllStringFunc(line, replaceURL)
		line = reQuotedPath.ReplaceAllString(line, "<path>")
		line = reURL.ReplaceAllStringFunc(line, replaceURL)
		line = reVersionBanner.ReplaceAllString(line, "")
		line = replacePathKeepLead(reUnixPath, line)
		line = replacePathKeepLead(reWinPath, line)
		line = replacePathKeepLead(reHomePath, line)
		lines[i] = line
	}
	return strings.Join(lines, "\n")
}

// BodyMarkdown renders sections that match the GitHub issue template fields.
func (d ReportDraft) BodyMarkdown() string {
	var b strings.Builder
	b.WriteString("## Description\n\n")
	if custom := strings.TrimSpace(d.CustomDescription); custom != "" {
		b.WriteString(custom)
		b.WriteString("\n\n")
	}
	b.WriteString("Error:\n\n```\n")
	b.WriteString(strings.TrimSpace(d.Description))
	b.WriteString("\n```\n\n")
	fmt.Fprintf(&b, "Exec ID: %s\n", d.ExecID)
	fmt.Fprintf(&b, "Sling version: %s\n", d.Version)
	fmt.Fprintf(&b, "OS: %s\n", d.OS)
	if d.ConnName != "" {
		fmt.Fprintf(&b, "Connection: %s\n", d.ConnName)
	}
	if d.SourceType != "" {
		fmt.Fprintf(&b, "Source: %s\n", typeToken(dbio.Type(d.SourceType)))
	}
	if d.TargetType != "" {
		fmt.Fprintf(&b, "Target: %s\n", typeToken(dbio.Type(d.TargetType)))
	}
	if d.ConnName == "" {
		b.WriteString("\n## Replication Configuration\n\n")
		b.WriteString("```yaml\n")
		b.WriteString(strings.TrimRight(d.Config, "\n"))
		b.WriteString("\n```\n")
	}
	b.WriteString("\n## Log Output\n\n")
	b.WriteString("```\n")
	b.WriteString(strings.TrimRight(d.LogExcerpt, "\n"))
	b.WriteString("\n```\n")
	return b.String()
}

// trimToBudget shrinks logs, then config, until build stays within max bytes.
// Never trims the description.
func (d ReportDraft) trimToBudget(max int, build func(logs, config string) string) string {
	logs := d.LogExcerpt
	config := d.Config
	u := build(logs, config)
	for len(u) > max && logsHasMore(logs) {
		logs = dropFirstLine(logs)
		u = build(logs, config)
	}
	if len(u) > max && len(logs) > 0 {
		// One remaining line still too long: keep the tail.
		keep := len(logs) / 2
		for keep > 32 && len(build(logs[len(logs)-keep:], config)) > max {
			keep = keep / 2
		}
		if keep < len(logs) {
			logs = logs[len(logs)-keep:]
		}
		u = build(logs, config)
	}
	for len(u) > max && config != "" && config != "(not captured)" && config != "(truncated)" {
		next := dropFirstLine(config)
		if next == config || next == "" {
			if len(config) > 64 {
				config = config[len(config)/2:]
			} else {
				config = "(truncated)"
			}
		} else {
			config = next
		}
		if strings.TrimSpace(config) == "" {
			config = "(truncated)"
		}
		u = build(logs, config)
	}
	return u
}

// GitHubIssueURL builds a prefilled new-issue URL. Issue forms (.yml) ignore
// query params except title, so the full report goes into body against the
// blank issue form.
func (d ReportDraft) GitHubIssueURL() string {
	return d.trimToBudget(githubIssueURLMax, func(logs, config string) string {
		trimmed := d
		trimmed.LogExcerpt = logs
		trimmed.Config = config
		q := url.Values{}
		q.Set("title", d.Title)
		q.Set("body", trimmed.BodyMarkdown())
		return githubIssueBaseURL + "?" + q.Encode()
	})
}

// ContactFormURL builds the contact-page URL with the full report body
// base64 encoded in the issue param. The form collects name/email and
// Turnstile. Never trims logs or config; oversized bodies go to a file
// instead (see deliverContactForm).
func (d ReportDraft) ContactFormURL() string {
	return contactFormBaseURL + "?issue=" + base64.RawURLEncoding.EncodeToString([]byte(d.BodyMarkdown()))
}

// ContactFormFits reports whether ContactFormURL stays under the Cloudflare
// request-line budget. When false, callers should return BodyMarkdown for
// manual paste instead of the prefilled URL.
func (d ReportDraft) ContactFormFits() bool {
	return len(d.ContactFormURL()) <= contactFormURLMax
}

// deliverGitHubIssue opens a prefilled GitHub issue when the URL fits.
// When it still exceeds githubIssueURLMax after trimming, it falls back
// to the email contact form (or a temp file if that is also oversize).
func deliverGitHubIssue(d ReportDraft, interactive bool) error {
	u := d.GitHubIssueURL()
	if len(u) > githubIssueURLMax {
		fmt.Fprintln(os.Stdout, "report too long for a GitHub issue URL; sending via email instead.")
		return deliverContactForm(d, interactive)
	}
	fmt.Fprintln(os.Stdout, u)
	if interactive {
		if err := OpenBrowser(u); err != nil {
			g.Warn("could not open browser: %s", err.Error())
		}
	}
	return nil
}

// deliverContactForm opens the prefilled contact page when the URL fits.
// When it exceeds the Cloudflare request-line limit, it writes the full
// report to a temp file for manual email instead.
func deliverContactForm(d ReportDraft, interactive bool) error {
	u := d.ContactFormURL()
	if len(u) > contactFormURLMax {
		return writeReportFile(d)
	}
	fmt.Fprintln(os.Stdout, u)
	if interactive {
		if err := OpenBrowser(u); err != nil {
			g.Warn("could not open browser: %s", err.Error())
		}
		fmt.Fprintln(os.Stdout, "complete the name/email fields in the form, then submit.")
	}
	return nil
}

// writeReportFile saves the full report body for manual email as attachment.
func writeReportFile(d ReportDraft) error {
	f, err := os.CreateTemp("", "sling-report-*.md")
	if err != nil {
		return g.Error("could not write report file: %s", err.Error())
	}
	defer f.Close()
	if _, err := f.WriteString(d.BodyMarkdown()); err != nil {
		return g.Error("could not write report file: %s", err.Error())
	}
	fmt.Fprintln(os.Stdout, "report too long for the web form.")
	fmt.Fprintf(os.Stdout, "attach this file and email it to %s:\n  %s\n", "support@slingdata.io", f.Name())
	return nil
}

func logsHasMore(s string) bool {
	return strings.Contains(s, "\n")
}

func dropFirstLine(s string) string {
	i := strings.IndexByte(s, '\n')
	if i < 0 {
		return ""
	}
	return s[i+1:]
}

// OpenBrowser opens url in the default browser. Always also print the URL.
func OpenBrowser(rawURL string) error {
	if browserEnv := strings.TrimSpace(os.Getenv("BROWSER")); browserEnv != "" {
		return exec.Command(browserEnv, rawURL).Start()
	}
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "windows":
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler", rawURL}
	case "darwin":
		cmd = "open"
		args = []string{rawURL}
	default:
		cmd = "xdg-open"
		args = []string{rawURL}
	}
	return exec.Command(cmd, args...).Start()
}

// ReportCmd is the `sling assist report` entry.
type ReportCmd struct {
	ExecID      string
	Title       string // optional override
	Description string // optional custom context, shown above the error
	GitHub      bool
	Email       bool
	Submit      bool // skip the confirm prompt (for agents)
}

// RunReport prints the redacted draft, then optionally sends it.
func RunReport(opts ReportCmd) error {
	d, err := ComposeReport(opts.ExecID)
	if err != nil {
		return err
	}
	if opts.Title != "" {
		d.Title = opts.Title
	}
	if opts.Description != "" {
		d.CustomDescription = opts.Description
	}
	fmt.Fprintln(os.Stdout, d.Title)
	fmt.Fprintln(os.Stdout, "")
	body := d.BodyMarkdown()
	fmt.Fprint(os.Stdout, body)
	if !strings.HasSuffix(body, "\n") {
		fmt.Fprintln(os.Stdout)
	}

	if opts.GitHub && opts.Email {
		return g.Error("use only one of --github or --email")
	}

	route := ""
	switch {
	case opts.GitHub:
		route = "github"
	case opts.Email:
		route = "email"
	}

	if !env.IsInteractiveTerminal() {
		if route == "github" {
			return deliverGitHubIssue(d, false)
		}
		if route == "email" {
			// Form flow: the user confirms by submitting the contact form.
			return deliverContactForm(d, false)
		}
		return nil
	}

	if route == "" {
		picked, err := pickReportRoute()
		if err != nil {
			return err
		}
		if picked == "cancel" || picked == "" {
			return nil
		}
		route = picked
	}

	if !opts.Submit {
		ok, err := confirmSendReport()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	return sendReport(d, route)
}

func sendReport(d ReportDraft, route string) error {
	switch route {
	case "github":
		return deliverGitHubIssue(d, true)
	case "email":
		// The contact form's Turnstile check and name/email fields validate
		// the submission.
		return deliverContactForm(d, true)
	default:
		return g.Error("unknown report route %q", route)
	}
}

func pickReportRoute() (string, error) {
	route := "cancel"
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewSelect[string]().
				Title("How do you want to send this report?").
				Options(
					huh.NewOption("GitHub issue (public, needs account)", "github"),
					huh.NewOption("Email to support", "email"),
					huh.NewOption("Cancel", "cancel"),
				).
				Value(&route),
		),
	).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return "", err
	}
	return route, nil
}

func confirmSendReport() (bool, error) {
	ok := false
	form := huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("Send this report? [y/N]").
				Affirmative("Yes").
				Negative("No").
				Value(&ok),
		),
	).WithTheme(huh.ThemeCharm())
	if err := form.Run(); err != nil {
		return false, err
	}
	return ok, nil
}
