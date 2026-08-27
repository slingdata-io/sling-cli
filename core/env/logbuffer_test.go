package env

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/rs/zerolog"
)

func line(s string) *g.LogLine { return &g.LogLine{Level: 1, Text: s} }

func TestLogCaptureOffByDefault(t *testing.T) {
	logs.Stop()
	logs.Capture(line("dropped"))
	if got := logs.Recent(); got != "" {
		t.Fatalf("want empty when off, got %q", got)
	}
}

func TestLogCaptureKeepsOrder(t *testing.T) {
	logs.Start()
	defer logs.Stop()
	for i := 1; i <= 3; i++ {
		logs.Capture(line(fmt.Sprintf("l%d", i)))
	}
	lines := strings.Split(logs.Recent(), "\n")
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d: %q", len(lines), lines)
	}
	for i, l := range lines {
		if !strings.HasSuffix(l, fmt.Sprintf("l%d", i+1)) {
			t.Fatalf("line %d out of order: %q", i, l)
		}
	}
}

func TestLogCaptureWrapsKeepingNewest(t *testing.T) {
	logs.Start()
	defer logs.Stop()
	total := recentLogLines + 10
	for i := 0; i < total; i++ {
		logs.Capture(line(fmt.Sprintf("l%d", i)))
	}
	got := logs.Recent()
	lines := strings.Split(got, "\n")
	if len(lines) != recentLogLines {
		t.Fatalf("want %d lines, got %d", recentLogLines, len(lines))
	}
	// Oldest 10 evicted; newest retained, still in order.
	if !strings.HasSuffix(lines[0], fmt.Sprintf("l%d", total-recentLogLines)) {
		t.Fatalf("wrong first line: %q", lines[0])
	}
	if !strings.HasSuffix(lines[len(lines)-1], fmt.Sprintf("l%d", total-1)) {
		t.Fatalf("wrong last line: %q", lines[len(lines)-1])
	}
	if strings.Contains(got, "l0\n") {
		t.Fatal("evicted line l0 still present")
	}
}

func TestLogCaptureSkipsBlank(t *testing.T) {
	logs.Start()
	defer logs.Stop()
	logs.Capture(line("   "))
	logs.Capture(nil)
	if got := logs.Recent(); got != "" {
		t.Fatalf("want empty, got %q", got)
	}
}

func TestLogCaptureRestartClears(t *testing.T) {
	logs.Start()
	logs.Capture(line("old"))
	logs.Start() // restart must drop prior content
	defer logs.Stop()
	if got := logs.Recent(); got != "" {
		t.Fatalf("restart must clear, got %q", got)
	}
}

func TestLogBufferWriteFileRespectsLevel(t *testing.T) {
	dir := t.TempDir()
	debugPath := filepath.Join(dir, "debug.log")
	tracePath := filepath.Join(dir, "trace.log")
	t.Setenv("SLING_DEBUG_FILE", debugPath)
	t.Setenv("SLING_TRACE_FILE", tracePath)

	lb := &logBuffer{}
	lb.SetupFiles()
	defer lb.CloseFiles()

	lb.WriteFile(&g.LogLine{Level: int8(zerolog.TraceLevel), Text: "trace-only"})
	lb.WriteFile(&g.LogLine{Level: int8(zerolog.ErrorLevel), Text: "an-error"})
	lb.CloseFiles()

	debug, err := os.ReadFile(debugPath)
	if err != nil {
		t.Fatalf("read debug: %v", err)
	}
	trace, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	if strings.Contains(string(debug), "trace-only") {
		t.Error("trace line must not reach the debug file")
	}
	if !strings.Contains(string(debug), "an-error") {
		t.Error("error line missing from debug file")
	}
	for _, want := range []string{"trace-only", "an-error"} {
		if !strings.Contains(string(trace), want) {
			t.Errorf("trace file missing %q", want)
		}
	}
}

func TestLogBufferInstancesAreIndependent(t *testing.T) {
	a, b := &logBuffer{}, &logBuffer{}
	a.Start()
	defer a.Stop()
	a.Capture(line("only-in-a"))
	if got := b.Recent(); got != "" {
		t.Fatalf("second buffer must stay empty, got %q", got)
	}
}

func TestPrintFatalCapturesToLogBuffer(t *testing.T) {
	logs.Start()
	defer logs.Stop()

	old := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w
	PrintFatal(g.Error("connection refused", "could not connect"))
	_ = w.Close()
	os.Stderr = old
	_, _ = r.Read(make([]byte, 64*1024))

	got := logs.Recent()
	if !strings.Contains(got, "fatal:") {
		t.Fatalf("missing fatal prefix: %q", got)
	}
	if !strings.Contains(got, "could not connect") && !strings.Contains(got, "connection refused") {
		t.Fatalf("missing error text: %q", got)
	}
	if strings.Contains(got, "0001-01-01") {
		t.Fatalf("zero timestamp on captured fatal: %q", got)
	}
}
