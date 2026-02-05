package env

import (
	"embed"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	"github.com/kardianos/osext"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

var (
	Marker         = "Sling CLI | https://slingdata.io"
	HomeDir        = os.Getenv("SLING_HOME_DIR")
	HomeDirEnvFile = ""
	Env            = &EnvFile{}
	PlausibleURL   = ""
	SentryDsn      = ""
	NoColor        = g.In(os.Getenv("SLING_LOGGING"), "NO_COLOR", "JSON")
	LogSink        func(*g.LogLine)
	TelMap         = g.M("begin_time", time.Now().UnixMicro())
	TelMux         = sync.Mutex{}
	HomeDirs       = map[string]string{}
	envMux         = sync.Mutex{}
	NoDebugKey     = " /* nD */"
	Executable     = ""
	IsThreadChild  = cast.ToBool(os.Getenv("SLING_THREAD_CHILD"))
	ExecID         = os.Getenv("SLING_EXEC_ID")
	AgentID        = os.Getenv("SLING_AGENT_ID")
	IsAgentMode    = AgentID != ""

	// File logging
	debugLogFile *os.File
	traceLogFile *os.File
	logFileMux   sync.Mutex
	GetOAuthMap  = func() map[string]map[string]any {
		return map[string]map[string]any{}
	}
	ExecFolder      = func() string { return path.Join(HomeDir, "executions", ExecID) }
	QueueFolder     = func() string { return path.Join(ExecFolder(), "queues") }
	RuntimeFolder   = func() string { return path.Join(ExecFolder(), "runtime") }
	RuntimeFilePath = func(name string) string {
		name = strings.ReplaceAll(name, "\\", "_")
		name = strings.ReplaceAll(name, "/", "_")
		name = strings.ReplaceAll(name, ":", "_")
		os.MkdirAll(RuntimeFolder(), 0755) // make folder
		return path.Join(RuntimeFolder(), g.F("%s.json", name))
	}
	setupOtel = func() {}

	ReservedFields = struct {
		LoadedAt  string
		SyncedAt  string
		SyncedOp  string
		DeletedAt string
		StreamURL string
		RowNum    string
		RowID     string
		ExecID    string
	}{
		LoadedAt:  "_sling_loaded_at",
		SyncedAt:  "_sling_synced_at",
		SyncedOp:  "_sling_synced_op",
		DeletedAt: "_sling_deleted_at",
		StreamURL: "_sling_stream_url",
		RowNum:    "_sling_row_num",
		RowID:     "_sling_row_id",
		ExecID:    "_sling_exec_id",
	}
)

const (
	DdlDefDecLength = 20
	DdlMinDecLength = 24
	DdlMaxDecScale  = 24
	DdlMaxDecLength = 38
	DdlMinDecScale  = 6
)

//go:embed *
var envFolder embed.FS

func init() {

	HomeDir = SetHomeDir("sling")
	HomeDirEnvFile = GetEnvFilePath(HomeDir)
	Executable, _ = osext.Executable()

	// create env file if not exists
	os.MkdirAll(HomeDir, 0755)
	if HomeDir != "" && !g.PathExists(HomeDirEnvFile) {
		defaultEnvBytes, _ := envFolder.ReadFile("default.env.yaml")
		os.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}

	if content := os.Getenv("SLING_ENV_YAML"); content != "" {
		os.Setenv("ENV_YAML", content)
	}

	// other sources of creds
	SetHomeDir("dbnet")  // https://github.com/dbnet-io/dbnet
	SetHomeDir("dbrest") // https://github.com/dbrest-io/dbrest

	if SentryDsn == "" {
		SentryDsn = os.Getenv("SENTRY_DSN")
	}

	// legacy env var for ERROR_ON_CHECKSUM_FAILURE
	if val := os.Getenv("ERROR_ON_CHECKSUM_FAILURE"); val != "" {
		os.Setenv("SLING_CHECKSUM_ROWS", "10000")
	}

	TelMap["parent"] = g.Marshal(process.GetParent())
}

func HomeBinDir() string {
	return path.Join(HomeDir, "bin")
}

// IsInteractiveTerminal checks if the current process is running in an interactive terminal
func IsInteractiveTerminal() bool {
	return isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())
}

func SetTelVal(key string, value any) {
	TelMux.Lock()
	TelMap[key] = value
	TelMux.Unlock()
}

func SetLogger() {
	g.SetZeroLogLevel(zerolog.InfoLevel)
	NoColor = g.In(os.Getenv("SLING_LOGGING"), "NO_COLOR", "JSON")
	g.DisableColor = NoColor

	if os.Getenv("_DEBUG_CALLER_LEVEL") != "" {
		g.CallerLevel = cast.ToInt(os.Getenv("_DEBUG_CALLER_LEVEL"))
	}
	if os.Getenv("DEBUG") == "TRACE" {
		g.SetZeroLogLevel(zerolog.TraceLevel)
		g.SetLogLevel(g.TraceLevel)
	} else if os.Getenv("DEBUG") != "" {
		g.SetZeroLogLevel(zerolog.DebugLevel)
		g.SetLogLevel(g.DebugLevel)
		if os.Getenv("DEBUG") == "LOW" {
			g.SetLogLevel(g.LowDebugLevel)
		}
	}

	outputOut := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: g.ZLogFormatMessage}
	outputErr := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: g.ZLogFormatMessage}
	outputOut.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	outputErr.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}

	if os.Getenv("SLING_LOGGING") == "NO_COLOR" {
		NoColor = true
		outputOut.NoColor = true
		outputErr.NoColor = true
		outputOut.FormatLevel = g.ZLogFormatMessage
		outputErr.FormatLevel = g.ZLogFormatMessage
		g.ZLogOut = zerolog.New(outputOut).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	} else if os.Getenv("SLING_LOGGING") == "JSON" {
		NoColor = true
		zerolog.LevelFieldName = "lvl"
		zerolog.MessageFieldName = "msg"
		g.ZLogOut = zerolog.New(os.Stdout).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "3:04PM", FormatLevel: g.ZLogFormatLevel, FormatMessage: g.ZLogFormatMessage}
		if g.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: g.ZLogFormatMessage}
		}
		g.ZLogOut = zerolog.New(outputErr).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	}
}

// InitLogger initializes the Logger
func InitLogger() {
	// reset log hook at g.DebugLevel
	g.SetLogHook(
		g.NewLogHook(
			g.DebugLevel,
			func(ll *g.LogLine) { processLogEntry(ll) },
		),
	)

	// add log hook at TraceLevel to capture all log levels for file logging
	g.AddLogHook(
		g.NewLogHook(
			g.TraceLevel,
			func(ll *g.LogLine) {
				// Write to log file(s) if configured
				writeToLogFile(ll)
			},
		),
	)

	SetLogger()
	setupFileLogging()
	setupOtel()
}

// setupFileLogging initializes file logging based on SLING_DEBUG_FILE and SLING_TRACE_FILE env vars
func setupFileLogging() {
	logFileMux.Lock()
	defer logFileMux.Unlock()

	// Close existing files if any (for re-initialization)
	if debugLogFile != nil {
		debugLogFile.Close()
		debugLogFile = nil
	}
	if traceLogFile != nil {
		traceLogFile.Close()
		traceLogFile = nil
	}

	// Open debug log file
	if debugPath := os.Getenv("SLING_DEBUG_FILE"); debugPath != "" {
		f, err := os.OpenFile(debugPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			g.Warn("could not open debug log file: %s", err.Error())
		} else {
			debugLogFile = f
		}
	}

	// Open trace log file
	if tracePath := os.Getenv("SLING_TRACE_FILE"); tracePath != "" {
		f, err := os.OpenFile(tracePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			g.Warn("could not open trace log file: %s", err.Error())
		} else {
			traceLogFile = f
		}
	}
}

// CloseFileLogging closes any open log files
func CloseFileLogging() {
	logFileMux.Lock()
	defer logFileMux.Unlock()

	if debugLogFile != nil {
		debugLogFile.Close()
		debugLogFile = nil
	}
	if traceLogFile != nil {
		traceLogFile.Close()
		traceLogFile = nil
	}
}

// stripANSI removes ANSI escape codes from a string
func stripANSI(text string) string {
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

// formatLogLine formats a log line for file output (no colors)
func formatLogLine(ll *g.LogLine) string {
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
	text = stripANSI(text)

	return fmt.Sprintf("%s %s%s\n", timeText, levelPrefix, text)
}

// writeToLogFile writes the log entry to configured log file(s)
func writeToLogFile(ll *g.LogLine) {
	logFileMux.Lock()
	defer logFileMux.Unlock()

	// Skip if no log files configured
	if debugLogFile == nil && traceLogFile == nil {
		return
	}

	level := zerolog.Level(ll.Level)

	// Handle Print/Println entries (level 9) - these are raw output from child processes
	// Write them directly with ANSI codes stripped, but only if they have content
	if ll.Level == 9 {
		text := stripANSI(ll.Text)
		if strings.TrimSpace(text) == "" {
			return
		}
		// Ensure text ends with newline
		if !strings.HasSuffix(text, "\n") {
			text = text + "\n"
		}
		// Write to both files (Print output is considered important)
		if traceLogFile != nil {
			traceLogFile.WriteString(text)
		}
		if debugLogFile != nil {
			debugLogFile.WriteString(text)
		}
		return
	}

	line := formatLogLine(ll)

	// Write to trace file (all levels)
	if traceLogFile != nil {
		traceLogFile.WriteString(line)
	}

	// Write to debug file (debug level and above)
	// zerolog levels: Trace=-1, Debug=0, Info=1, Warn=2, Error=3
	if debugLogFile != nil && level >= zerolog.DebugLevel {
		debugLogFile.WriteString(line)
	}
}

func Print(text string) {
	fmt.Fprintf(os.Stderr, "%s", text)
	processLogEntry(&g.LogLine{Level: 9, Text: text})
	writeToLogFile(&g.LogLine{Level: 9, Text: text})
}

func Println(text string) {
	text = text + "\n"
	Print(text)
}

func LoadSlingEnvFile() (ef EnvFile) {
	ef = LoadEnvFile(HomeDirEnvFile)
	Env = &ef
	Env.TopComment = "# Environment Credentials for Sling CLI\n# See https://docs.slingdata.io/sling-cli/environment\n"
	return
}

func LoadSlingEnvFileBody(body string) (ef EnvFile, err error) {
	if body == "" {
		return EnvFile{
			Connections: map[string]map[string]any{},
			Env:         map[string]any{},
		}, nil
	}

	ef.Body = body

	// expand variables
	envMap := map[string]any{}
	for _, tuple := range os.Environ() {
		key := strings.Split(tuple, "=")[0]
		val := strings.TrimPrefix(tuple, key+"=")
		envMap[key] = val
	}
	ef.Body = g.Rmd(ef.Body, envMap)

	err = yaml.Unmarshal([]byte(ef.Body), &ef)

	if ef.Connections == nil {
		ef.Connections = map[string]map[string]any{}
	}

	if len(ef.Env) == 0 {
		if len(ef.Variables) == 0 {
			ef.Env = map[string]any{}
		} else {
			ef.Env = ef.Variables // support legacy
		}
	}

	for k, v := range ef.Env {
		if _, found := envMap[k]; !found {
			os.Setenv(k, g.CastToString(v))
		}
	}

	return
}

func GreenString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorGreen, text)
}

func YellowString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorYellow, text)
}

func RedString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorRed, text)
}

func BlueString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorBlue, text)
}

func CyanString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorCyan, text)
}

func MagentaString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorMagenta, text)
}

func DarkGrayString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorDarkGray, text)
}

func GetHomeDirConnsMap() (connsMap map[string]map[string]any, err error) {
	defer envMux.Unlock()
	envMux.Lock()
	connsMap = map[string]map[string]any{}
	for _, homeDir := range HomeDirs {
		envFilePath := GetEnvFilePath(homeDir)
		if g.PathExists(envFilePath) {
			m := g.M()
			g.JSONConvert(LoadEnvFile(envFilePath), &m)
			cm, _ := readConnectionsMap(m)
			for k, v := range cm {
				connsMap[k] = v
			}
		}
	}
	return connsMap, nil
}

func readConnectionsMap(env map[string]any) (conns map[string]map[string]any, err error) {
	conns = map[string]map[string]any{}

	if connections, ok := env["connections"]; ok {
		switch connectionsV := connections.(type) {
		case map[string]any, map[interface{}]interface{}:
			connMap := cast.ToStringMap(connectionsV)
			for name, v := range connMap {
				switch v.(type) {
				case map[string]any, map[interface{}]interface{}:
					conns[strings.ToLower(name)] = cast.ToStringMap(v)
				default:
					g.Warn("did not handle %s", name)
				}
			}
		default:
			g.Warn("did not handle connections profile type %T", connections)
		}
	}
	return
}

func UseDuckDbCompute() bool {
	if val := os.Getenv("SLING_DUCKDB_COMPUTE"); val != "" && !cast.ToBool(val) {
		return false
	}
	return true
}

func GetTempFolder() string {
	tempDir := os.TempDir()
	if val := os.Getenv("SLING_TEMP_DIR"); val != "" {
		tempDir = val
	}
	tempDir = strings.TrimRight(strings.TrimRight(tempDir, "/"), "\\")
	return CleanWindowsPath(tempDir)
}

func CleanTableName(tableName string) string {
	return strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(tableName, `"`, ``), "`", ""))
}

func CleanWindowsPath(path string) string {
	return strings.ReplaceAll(path, `\`, `/`)
}

func processLogEntry(ll *g.LogLine) {
	// Existing LogSink functionality
	if LogSink != nil {
		LogSink(ll)
	}
}

// RemoveLocalTempFile deletes the local file
func RemoveLocalTempFile(localPath string) {
	if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
		os.Remove(localPath)
	}
}

// RemoveAllLocalTempFile deletes the local folder
func RemoveAllLocalTempFile(localPath string) {
	if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
		os.RemoveAll(localPath)
	}
}

func WriteTempSQL(sql string, filePrefix ...string) (sqlPath string, err error) {
	sqlPath = path.Join(GetTempFolder(), g.NewTsID(filePrefix...)+".sql")

	err = os.WriteFile(sqlPath, []byte(sql), 0777)
	if err != nil {
		return "", g.Error(err, "could not create temp sql")
	}

	return
}

func LogSQL(props map[string]string, query string, args ...any) {
	noColor := g.In(os.Getenv("SLING_LOGGING"), "NO_COLOR", "JSON")

	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	// wrap args
	contextArgs := g.M()
	connIdSuffix := ""
	if connID := props["sling_conn_id"]; connID != "" {
		contextArgs["conn"] = connID
		// use connection name
		connArr := strings.Split(connID, "-")
		connIdSuffix = strings.TrimPrefix(connID, connArr[0]+"-")
		connIdSuffix = strings.TrimPrefix(connIdSuffix, connArr[1]+"-")
		connIdSuffix = DarkGrayString(" [" + connIdSuffix + "]")
	}
	if len(args) > 0 {
		contextArgs["query_args"] = args
	}
	if strings.Contains(query, NoDebugKey) {
		if !noColor {
			query = CyanString(query)
		}
		g.Trace(query, contextArgs)
	} else {
		if !noColor {
			query = CyanString(Clean(props, query))
		}
		if !cast.ToBool(props["silent"]) {
			g.Debug(query + connIdSuffix)
		}
	}
}

// Clean removes creds from a log line
func Clean(props map[string]string, line string) string {
	line = strings.TrimSpace(line)
	sqlLower := strings.ToLower(line)

	startsWith := func(p string) bool { return strings.HasPrefix(sqlLower, p) }

	switch {
	case startsWith("drop "), startsWith("create "), startsWith("insert into"), startsWith("select count"):
		return line
	case startsWith("alter table "), startsWith("update "), startsWith("alter table "), startsWith("update "):
		return line
	case startsWith("select *"):
		return line
	}

	for k, v := range props {
		if strings.TrimSpace(v) == "" {
			continue
		} else if g.In(k, "password", "access_key_id", "secret_access_key", "session_token", "aws_access_key_id", "aws_secret_access_key", "ssh_private_key", "ssh_passphrase", "sas_svc_url", "conn_str") {
			line = strings.ReplaceAll(line, v, "***")
		}
	}
	return line
}
