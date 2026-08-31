package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
)

func TestAssistBrowseFlagRemoved(t *testing.T) {
	for _, f := range cliAssistFlags {
		if f.Name == "browse" || f.Name == "cdp" || f.Name == "setup" || f.Name == "doctor" || f.Name == "task" || f.Name == "path" {
			t.Fatalf("removed flag still present: %s", f.Name)
		}
	}
	foundResume, foundModel := false, false
	for _, f := range cliAssistFlags {
		if f.Name == "resume" {
			foundResume = true
		}
		if f.Name == "model" {
			foundModel = true
		}
	}
	if !foundResume || !foundModel {
		t.Fatal("missing --resume or --model on sling assist")
	}
	hasSetup, setupHasDoctor := false, false
	for _, sc := range cliAssist.SubComs {
		if sc.Name == "setup" {
			hasSetup = true
			for _, f := range sc.Flags {
				if f.Name == "doctor" {
					setupHasDoctor = true
				}
			}
		}
		if sc.Name == "investigate" || sc.Name == "history" {
			t.Fatalf("removed subcommand still present: %s", sc.Name)
		}
	}
	hasReport := false
	for _, sc := range cliAssist.SubComs {
		if sc.Name == "report" {
			hasReport = true
		}
	}
	if !hasSetup {
		t.Fatal("missing setup subcommand")
	}
	if !hasReport {
		t.Fatal("missing report subcommand")
	}
	if !setupHasDoctor {
		t.Fatal("missing --doctor on sling assist setup")
	}
}

func TestPadAssistResumeFlag(t *testing.T) {
	got := padAssistResumeFlag([]string{"sling", "assist", "--resume"})
	if len(got) < 3 || got[2] != "--resume=" {
		t.Fatalf("bare resume: %v", got)
	}
	got = padAssistResumeFlag([]string{"sling", "assist", "--resume", "--out"})
	if got[2] != "--resume=" {
		t.Fatalf("resume before flag: %v", got)
	}
	got = padAssistResumeFlag([]string{"sling", "assist", "--resume", "sess1"})
	if got[2] != "--resume" || got[3] != "sess1" {
		t.Fatalf("resume with id: %v", got)
	}
}

func TestResumeFromArgs(t *testing.T) {
	ok, id := resumeFromArgs([]string{"sling", "assist", "--resume="})
	if !ok || id != "" {
		t.Fatalf("empty resume: %v %q", ok, id)
	}
	ok, id = resumeFromArgs([]string{"sling", "assist", "--resume", "abc"})
	if !ok || id != "abc" {
		t.Fatalf("id: %v %q", ok, id)
	}
	ok, _ = resumeFromArgs([]string{"sling", "assist", "--out"})
	if ok {
		t.Fatal("resume not set")
	}
}

// parseKVList is kept in this test file so TestCLI still compiles after
// the production helper was removed in the assist redesign.
func parseKVList(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range splitKVPairs(s) {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])
		v := unquoteKV(strings.TrimSpace(kv[1]))
		if strings.HasPrefix(v, "@") && !strings.HasPrefix(v, "@@") {
			if b, err := os.ReadFile(v[1:]); err == nil {
				v = string(b)
			}
		}
		out[k] = v
	}
	return out
}

func splitKVPairs(s string) []string {
	var parts []string
	var b strings.Builder
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && i+1 < len(s) {
				b.WriteByte(c)
				i++
				b.WriteByte(s[i])
				continue
			}
			if c == quote {
				quote = 0
			}
			b.WriteByte(c)
			continue
		}
		if c == '"' || c == '\'' {
			quote = c
			b.WriteByte(c)
			continue
		}
		if c == ',' {
			parts = append(parts, b.String())
			b.Reset()
			continue
		}
		b.WriteByte(c)
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	return parts
}

func unquoteKV(v string) string {
	if len(v) < 2 {
		return v
	}
	if (v[0] == '"' && v[len(v)-1] == '"') || (v[0] == '\'' && v[len(v)-1] == '\'') {
		inner := v[1 : len(v)-1]
		inner = strings.ReplaceAll(inner, `\"`, `"`)
		inner = strings.ReplaceAll(inner, `\'`, `'`)
		return inner
	}
	return v
}

func TestParseKVListQuotedComma(t *testing.T) {
	got := parseKVList(`Intention="Select id, email, name. Range 2024-01-01,2024-12-31",Path=./out.yaml`)
	if got["Intention"] != "Select id, email, name. Range 2024-01-01,2024-12-31" {
		t.Fatalf("Intention=%q", got["Intention"])
	}
	if got["Path"] != "./out.yaml" {
		t.Fatalf("Path=%q", got["Path"])
	}
}

func TestParseKVListAtFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "intention.txt")
	body := "Load fixtures/data/*.csv and range 2024-01-01,2024-12-31"
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got := parseKVList("Intention=@" + p + ",Path=./out.yaml")
	if got["Intention"] != body {
		t.Fatalf("Intention=%q", got["Intention"])
	}
	if got["Path"] != "./out.yaml" {
		t.Fatalf("Path=%q", got["Path"])
	}
}

func TestParseKVListUnquotedStillSplits(t *testing.T) {
	got := parseKVList("A=1,B=2")
	if got["A"] != "1" || got["B"] != "2" {
		t.Fatalf("%v", got)
	}
}

func TestOverlaySpecConn(t *testing.T) {
	dir := t.TempDir()
	specPath := filepath.Join(dir, "draft.yaml")
	if err := os.WriteFile(specPath, []byte("name: draft\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	apiConn, err := connection.NewConnection("MY_API", dbio.TypeApi, g.M("type", "api", "spec", "baseline"))
	if err != nil {
		t.Fatal(err)
	}
	otherConn, err := connection.NewConnection("LOCAL", dbio.TypeFileLocal, g.M("type", "file"))
	if err != nil {
		t.Fatal(err)
	}
	entries := connection.ConnEntries{
		{Name: "MY_API", Connection: apiConn},
		{Name: "LOCAL", Connection: otherConn},
	}

	out, err := overlaySpecConn(entries, "MY_API", "draft.yaml", dir)
	if err != nil {
		t.Fatal(err)
	}

	got := out.Get("MY_API").Connection.Data["spec"]
	want := "file://" + specPath
	if got != want {
		t.Fatalf("overlay spec=%q want %q", got, want)
	}
	// original entries stay untouched
	if entries.Get("MY_API").Connection.Data["spec"] != "baseline" {
		t.Fatalf("original entry was mutated")
	}
	if out.Get("LOCAL").Connection.Data["spec"] != nil {
		t.Fatalf("unrelated entry changed")
	}

	// missing file errors
	if _, err := overlaySpecConn(entries, "MY_API", "nope.yaml", dir); err == nil {
		t.Fatal("expected error for missing spec file")
	}
	// unknown connection errors
	if _, err := overlaySpecConn(entries, "NOPE", specPath, dir); err == nil {
		t.Fatal("expected error for unknown connection")
	}
}
