package env

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/rs/zerolog"
	"github.com/slingdata-io/sling-cli/core"
)

// recentLogLines is how many log lines the failure buffer keeps.
const recentLogLines = 5000

// logBuffer owns run logging: the file sinks (debug/trace) and a bounded
// in-memory tail of the log. The tail feeds the assist failure snapshot, so a
// failed run keeps the lines that led to the error.
//
// The ring is off until Start runs. next is the next write slot; the buffer
// wraps once full, so an old line is overwritten in place instead of shifting
// the whole slice.
type logBuffer struct {
	mu sync.Mutex

	// file sinks
	debugFile *os.File
	traceFile *os.File
	headerOn  bool

	// in-memory tail
	on    bool
	lines []string
	next  int
	full  bool
}

// logs is the process-wide run log.
var logs = &logBuffer{}

// Start begins buffering log lines for the failure snapshot.
func (lb *logBuffer) Start() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.on = true
	lb.lines = make([]string, recentLogLines)
	lb.next, lb.full = 0, false
}

// Stop stops buffering and drops what was kept.
func (lb *logBuffer) Stop() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.on = false
	lb.lines = nil
	lb.next, lb.full = 0, false
}

// Recent returns the buffered tail, oldest first.
func (lb *logBuffer) Recent() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	n := lb.next
	if lb.full {
		n = recentLogLines
	}
	if n == 0 {
		return ""
	}

	// Size the builder up front: one Grow beats repeated reallocation.
	size := n - 1 // newline separators
	for i := 0; i < n; i++ {
		size += len(lb.at(i))
	}

	var b strings.Builder
	b.Grow(size)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(lb.at(i))
	}
	return b.String()
}

// at returns the i-th line, oldest first. Call with the lock held.
func (lb *logBuffer) at(i int) string {
	if !lb.full {
		return lb.lines[i]
	}
	return lb.lines[(lb.next+i)%recentLogLines]
}

func (lb *logBuffer) shortExecID() string {
	val := ExecID
	if len(val) > 8 {
		val = val[len(val)-8:]
	}
	return val
}

// formatLogLine formats a log line for file output (no colors)
func (lb *logBuffer) formatLogLine(ll *g.LogLine) string {
	var levelPrefix string

	switch zerolog.Level(ll.Level) {
	case zerolog.TraceLevel:
		levelPrefix = "TRC "
	case zerolog.DebugLevel:
		levelPrefix = "DBG "
	case zerolog.InfoLevel:
		levelPrefix = "INF "
	case zerolog.WarnLevel:
		levelPrefix = "WRN "
	case zerolog.ErrorLevel:
		levelPrefix = "ERR "
	default:
		levelPrefix = ""
	}

	timeText := ll.Time.Format("2006-01-02 15:04:05")

	// Filter out map arguments and special strings (used internally by g library for context logging)
	filteredArgs := []any{}
	for _, arg := range ll.Args {
		switch arg.(type) {
		case map[string]any:
			// Skip map arguments - they're context fields, not format args
			continue
		default:
			if s, ok := arg.(string); ok && strings.HasPrefix(s, "_DEBUG_CALLER_START=") {
				// Skip internal caller tracking string
				continue
			}
			filteredArgs = append(filteredArgs, arg)
		}
	}

	text := g.F(ll.Text, filteredArgs...)

	// Strip any ANSI codes from the text
	text = lb.stripANSI(text)

	return fmt.Sprintf("%s | %s %s%s\n", lb.shortExecID(), timeText, levelPrefix, text)
}

// stripANSI removes ANSI escape codes from a string
func (lb *logBuffer) stripANSI(text string) string {
	// Match ANSI escape sequences: ESC[ followed by any number of params and a letter
	// This handles color codes like \x1b[32m, \x1b[0m, \x1b[90m, etc.
	result := strings.Builder{}
	i := 0
	for i < len(text) {
		if i+1 < len(text) && text[i] == '\x1b' && text[i+1] == '[' {
			// Skip the escape sequence
			j := i + 2
			for j < len(text) && ((text[j] >= '0' && text[j] <= '9') || text[j] == ';') {
				j++
			}
			if j < len(text) && text[j] >= 'A' && text[j] <= 'z' {
				j++ // Skip the final letter
			}
			i = j
		} else {
			result.WriteByte(text[i])
			i++
		}
	}
	return result.String()
}

// Capture appends one rendered line, overwriting the oldest once full.
func (lb *logBuffer) Capture(ll *g.LogLine) {
	if ll == nil {
		return
	}
	lb.mu.Lock()
	defer lb.mu.Unlock()
	if !lb.on {
		return
	}
	// Check the message before formatting: formatLogLine prepends the exec id
	// and timestamp, which would make a blank message look non-blank.
	if strings.TrimSpace(ll.Text) == "" {
		return
	}
	// formatLogLine interpolates Args and adds the level/time prefix.
	text := strings.TrimRight(lb.stripANSI(lb.formatLogLine(ll)), "\n")
	if strings.TrimSpace(text) == "" {
		return
	}
	lb.lines[lb.next] = text
	lb.next++
	if lb.next == recentLogLines {
		lb.next, lb.full = 0, true
	}
}

// CloseFiles closes any open log files.
func (lb *logBuffer) CloseFiles() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.closeFiles()
}

// closeFiles closes the sinks. Call with the lock held.
func (lb *logBuffer) closeFiles() {
	if lb.debugFile != nil {
		lb.debugFile.Close()
		lb.debugFile = nil
	}
	if lb.traceFile != nil {
		lb.traceFile.Close()
		lb.traceFile = nil
	}
}

// cleanupOldLogFiles removes old .log files from the directory, keeping the latest `keep` files.
// Files are sorted by name (which sorts chronologically for date-based filenames).
func (lb *logBuffer) cleanupOldLogFiles(dir string, keep int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		g.Warn("could not read log directory for cleanup: %s", err.Error())
		return
	}

	var logFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			logFiles = append(logFiles, entry.Name())
		}
	}

	sort.Strings(logFiles)

	if len(logFiles) > keep {
		for _, name := range logFiles[:len(logFiles)-keep] {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				g.Warn("could not remove old log file %s: %s", name, err.Error())
			}
		}
	}
}

// SetupFiles opens the debug and trace sinks from the environment.
// SLING_DEBUG_FILE and SLING_TRACE_FILE name a file each; SLING_LOG_DIR gives
// date-based rotation when SLING_DEBUG_FILE is unset.
func (lb *logBuffer) SetupFiles() {
	if IsThreadChild {
		return // don't write log from child processes
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.closeFiles() // for re-initialization

	// setup env from env.yaml and .env.sling
	LoadSlingEnvFile()
	LoadDotEnvSling()

	if debugPath := os.Getenv("SLING_DEBUG_FILE"); debugPath != "" {
		f, err := os.OpenFile(debugPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			g.Warn("could not open debug log file: %s", err.Error())
		} else {
			lb.debugFile = f
		}
	}

	// Date-based rotation, only when SLING_DEBUG_FILE was not set.
	if logDir := os.Getenv("SLING_LOG_DIR"); logDir != "" && lb.debugFile == nil {
		if strings.HasPrefix(logDir, "~/") {
			logDir = filepath.Join(g.UserHomeDir(), logDir[2:])
		}
		if err := os.MkdirAll(logDir, 0755); err != nil {
			g.Warn("could not create log directory: %s", err.Error())
		} else {
			logPath := filepath.Join(logDir, "sling_debug_"+time.Now().Format("2006_01_02")+".log")
			f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				g.Warn("could not open log file: %s", err.Error())
			} else {
				lb.debugFile = f
				lb.cleanupOldLogFiles(logDir, 15)
			}
		}
	}

	if tracePath := os.Getenv("SLING_TRACE_FILE"); tracePath != "" {
		f, err := os.OpenFile(tracePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			g.Warn("could not open trace log file: %s", err.Error())
		} else {
			lb.traceFile = f
		}
	}
}

// WriteFile writes the log entry to the configured log file(s).
func (lb *logBuffer) WriteFile(ll *g.LogLine) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	writeHeader := func(logFile *os.File) {
		// Write session header
		wd, _ := os.Getwd()
		header := fmt.Sprintf(
			"\n%s\n== %s | version: %s | exec_id: %s\n== dir: %s | command: %s\n%s\n",
			strings.Repeat("=", 100),
			time.Now().Format("2006-01-02 15:04:05"),
			core.Version,
			ExecID,
			wd,
			strings.Join(os.Args, " "),
			strings.Repeat("=", 80),
		)
		logFile.WriteString(header)
	}

	if lb.debugFile == nil && lb.traceFile == nil {
		return
	}

	if !lb.headerOn {
		if lb.debugFile != nil {
			writeHeader(lb.debugFile)
		}
		if lb.traceFile != nil {
			writeHeader(lb.traceFile)
		}
		lb.headerOn = true
	}

	// Level 9 is raw output from a child process. Write it through with the
	// ANSI codes stripped, to both files: Print output is important.
	if ll.Level == 9 {
		text := lb.stripANSI(ll.Text)
		if strings.TrimSpace(text) == "" {
			return
		}
		text = lb.shortExecID() + " | " + text
		if !strings.HasSuffix(text, "\n") {
			text = text + "\n"
		}
		if lb.traceFile != nil {
			lb.traceFile.WriteString(text)
		}
		if lb.debugFile != nil {
			lb.debugFile.WriteString(text)
		}
		return
	}

	line := lb.formatLogLine(ll)

	if lb.traceFile != nil {
		lb.traceFile.WriteString(line) // all levels
	}
	// zerolog levels: Trace=-1, Debug=0, Info=1, Warn=2, Error=3
	if lb.debugFile != nil && zerolog.Level(ll.Level) >= zerolog.DebugLevel {
		lb.debugFile.WriteString(line)
	}
}

// Process is the DebugLevel hook: buffer the tail, then fan out to LogSink.
func (lb *logBuffer) Process(ll *g.LogLine) {
	lb.Capture(ll)

	if LogSink != nil {
		LogSink(ll)
	}
}

// --- package-level API -------------------------------------------------

// StartLogCapture begins buffering log lines for the failure snapshot.
func StartLogCapture() { logs.Start() }

// StopLogCapture stops buffering and drops what was kept.
func StopLogCapture() { logs.Stop() }

// RecentLogs returns the buffered tail, oldest first.
func RecentLogs() string { return logs.Recent() }

// CloseFileLogging closes any open log files.
func CloseFileLogging() { logs.CloseFiles() }

// setupFileLogging opens the log sinks from the environment.
func setupFileLogging() { logs.SetupFiles() }

func writeToLogFile(ll *g.LogLine) { logs.WriteFile(ll) }

func processLogEntry(ll *g.LogLine) { logs.Process(ll) }

// SecretKeys is the full set of connection property names whose values
// are secrets. Clean and parse.Redact redact these. Keep it in sync with
// `secret: true` in core/dbio/templates/_properties.yaml.
var SecretKeys = []string{
	"access_key_id",
	"account_key",
	"aws_access_key_id",
	"aws_secret_access_key",
	"aws_session_token",
	"azure_account_key",
	"azure_sas_token",
	"conn_str",
	"gcp_credentials_json",
	"gcs_secret_access_key",
	"password",
	"rest_oauth_client_secret",
	"rest_token",
	"s3_secret_access_key",
	"s3_session_token",
	"sas_svc_url",
	"sas_token",
	"secret_access_key",
	"session_token",
	"ssh_passphrase",
	"ssh_private_key",
	"token",
}

var (
	secretKeysOnce sync.Once
	secretKeyCache map[string]struct{}
)

func secretKeysLower() map[string]struct{} {
	secretKeysOnce.Do(func() {
		secretKeyCache = make(map[string]struct{}, len(SecretKeys))
		for _, k := range SecretKeys {
			secretKeyCache[strings.ToLower(k)] = struct{}{}
		}
	})
	return secretKeyCache
}

// ScrubLine redacts secrets from every local connection in Env.
func ScrubLine(line string) string {
	if Env == nil {
		return line
	}
	for _, data := range Env.Connections {
		line = CleanConnData(data, line)
	}
	return line
}
