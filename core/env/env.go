package env

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/flarco/g"
	"github.com/flarco/g/process"
	"github.com/kardianos/osext"
	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/segmentio/ksuid"
	"github.com/spf13/cast"
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
	envMux         = sync.Mutex{}
	NoDebugKey     = " /* nD */"
	Executable     = ""
	IsThreadChild  = cast.ToBool(os.Getenv("SLING_THREAD_CHILD"))
	ExecID         = g.Getenv("SLING_EXEC_ID", NewExecID())
	RunnerID       = g.Getenv("SLING_RUNNER_ID", os.Getenv("SLING_AGENT_ID"))
	IsRunnerMode   = RunnerID != ""

	GetOAuthMap = func() map[string]map[string]any {
		return map[string]map[string]any{}
	}
	ExecFolder      = func() string { return filepath.Join(HomeDir, "executions", ExecID) }
	QueueFolder     = func() string { return filepath.Join(ExecFolder(), "queues") }
	RuntimeFolder   = func() string { return filepath.Join(ExecFolder(), "runtime") }
	RuntimeFilePath = func(name string) string {
		name = strings.ReplaceAll(name, "\\", "_")
		name = strings.ReplaceAll(name, "/", "_")
		name = strings.ReplaceAll(name, ":", "_")
		os.MkdirAll(RuntimeFolder(), 0755) // make folder
		return filepath.Join(RuntimeFolder(), g.F("%s.json", name))
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
		CDCSeq    string
	}{
		LoadedAt:  "_sling_loaded_at",
		SyncedAt:  "_sling_synced_at",
		SyncedOp:  "_sling_synced_op",
		DeletedAt: "_sling_deleted_at",
		StreamURL: "_sling_stream_url",
		RowNum:    "_sling_row_num",
		RowID:     "_sling_row_id",
		ExecID:    "_sling_exec_id",
		CDCSeq:    "_sling_cdc_seq",
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
	Executable, _ = osext.Executable()

	if content := os.Getenv("SLING_ENV_YAML"); content != "" {
		os.Setenv("ENV_YAML", content)
	}

	LoadHomeDir()

	if SentryDsn == "" {
		SentryDsn = os.Getenv("SENTRY_DSN")
	}

	// legacy env var for ERROR_ON_CHECKSUM_FAILURE
	if val := os.Getenv("ERROR_ON_CHECKSUM_FAILURE"); val != "" {
		os.Setenv("SLING_CHECKSUM_ROWS", "10000")
	}

	TelMap["parent"] = g.Marshal(process.GetParent())

	// we need a webserver to get the pprof webserver
	if cast.ToBool(os.Getenv("SLING_PPROF")) {
		go func() {
			g.Debug("Starting pprof webserver @ localhost:6060")
			g.LogError(http.ListenAndServe("localhost:6060", nil))
		}()
	}
}

func LoadHomeDir() {
	envKey := "SLING_HOME_DIR"
	HomeDir = CleanWindowsPath(os.Getenv(envKey))
	if HomeDir == "" {
		HomeDir = CleanWindowsPath(filepath.Join(g.UserHomeDir(), ".sling"))
		os.Setenv(envKey, HomeDir)
	}

	HomeDirEnvFile = GetEnvFilePath(HomeDir)

	// create env file if not exists
	os.MkdirAll(HomeDir, 0755)
	if HomeDir != "" && !g.PathExists(HomeDirEnvFile) {
		defaultEnvBytes, _ := envFolder.ReadFile("default.env.yaml")
		os.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}
}

func HomeBinDir() string {
	return filepath.Join(HomeDir, "bin")
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

func NewExecID() string {
	uid, err := ksuid.NewRandom()
	execID := g.NewTsID("exec")
	if err == nil {
		execID = uid.String()
	}

	return execID
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

	formatMsg := func(i interface{}) string {
		return ScrubLine(g.ZLogFormatMessage(i))
	}
	formatErr := func(i interface{}) string {
		return ScrubLine(fmt.Sprintf("%s", i))
	}
	outputOut := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: formatMsg}
	outputErr := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: formatMsg}
	outputOut.FormatErrFieldValue = formatErr
	outputErr.FormatErrFieldValue = formatErr

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
		outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "3:04PM", FormatLevel: g.ZLogFormatLevel, FormatMessage: formatMsg}
		if g.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05", FormatLevel: g.ZLogFormatLevel, FormatMessage: formatMsg}
		}
		outputErr.FormatErrFieldValue = formatErr
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
func Print(text string) {
	fmt.Fprintf(os.Stderr, "%s", text)
	ll := &g.LogLine{Level: 9, Text: text, Time: time.Now()}
	processLogEntry(ll)
	writeToLogFile(ll)
}

func Println(text string) {
	text = text + "\n"
	Print(text)
}

// PrintFatal prints the fatal error (same text as g.PrintFatal) and captures
// it in the run-log buffer via Println, so stderr.log includes it.
func PrintFatal(E error, args ...interface{}) {
	makeErrStrings := func(payload string) string {
		cancelledCount := 0
		payload = strings.ReplaceAll(payload, "---\n\n---", "---\n---")
		errParts := strings.Split(payload, "\n\n")
		errStrings := []string{}
		errHash := map[string]struct{}{}
		for _, errPart := range errParts {
			if _, ok := errHash[errPart]; !ok && errPart != "context canceled" {
				if ps := strings.Split(errPart, "\n"); ps[len(ps)-1] == "context canceled" {
					cancelledCount++
				}
				errStrings = append(errStrings, errPart)
			}
			errHash[errPart] = struct{}{}
		}

		if cancelledCount == len(errStrings) {
			return "cancelled"
		}
		return strings.Join(errStrings, "\n\n")
	}

	prefix := "fatal:\n"
	if E != nil {
		err, ok := E.(*g.ErrType)
		if !ok {
			err = g.NewError(3, E, args...).(*g.ErrType)
		}

		eG, ok := E.(*g.ErrorGroup)
		if ok {
			if !g.IsDebugLow() {
				Println(RedString(prefix + eG.Error()))
			} else {
				Println(RedString(prefix + eG.Debug()))
			}
		} else {
			if !g.IsDebugLow() {
				joined := makeErrStrings(err.Error())
				Println(RedString(prefix + joined))
			} else {
				joined := makeErrStrings(err.Err)
				output := g.F("%s\n%s", strings.Join(err.Stack(), "\n"), joined)
				Println(RedString(prefix + output))
			}
		}
	}
}

func LoadSlingEnvFile() (ef EnvFile) {
	ef = LoadEnvFile(HomeDirEnvFile)
	Env = &ef
	Env.TopComment = "# Environment Credentials for Sling CLI\n# See https://docs.slingdata.io/sling-cli/environment\n"
	return
}

func LoadSlingEnvFileBody(body string) (ef EnvFile, err error) {
	return loadEnvFile(body, "")
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
	envFilePath := GetEnvFilePath(HomeDir)
	if g.PathExists(envFilePath) {
		m := g.M()
		g.JSONConvert(LoadEnvFile(envFilePath), &m)
		cm, _ := readConnectionsMap(m)
		for k, v := range cm {
			connsMap[k] = v
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
		query = Clean(props, query)
		if !noColor {
			query = CyanString(query)
		}
		if !cast.ToBool(props["silent"]) {
			g.Debug(query + connIdSuffix)
		}
	}
}

// Skip short values so passwords like "postgres" don't redact unrelated log text.
const minSecretLen = 12

func redactable(val string) bool {
	val = strings.TrimSpace(val)
	return len(val) >= minSecretLen && !IsEnvVarRef(val)
}

// Clean removes creds from a log line. CREATE/INSERT/etc. still redact:
// Redshift COPY/UNLOAD embeds keys in those statements.
func Clean(props map[string]string, line string) string {
	line = strings.TrimSpace(line)
	keys := secretKeysLower()
	for k, v := range props {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if _, ok := keys[strings.ToLower(k)]; ok {
			line = strings.ReplaceAll(line, v, "***")
		}
	}
	return line
}

// expandEnvVars replaces $KEY or ${KEY} with its environment variable value
// only if the variable is present in the environment.
// If not present, $KEY or ${KEY} will remain in the config text.
func ExpandEnvVars(text string) string {
	for key, value := range g.KVArrToMap(os.Environ()...) {
		text = strings.ReplaceAll(text, "$"+key+"", value)
		text = strings.ReplaceAll(text, "${"+key+"}", value)
	}
	return text
}

// CleanConnData redacts registry secrets and every nested secrets: value.
// Nested API keys are secret even when the key name is not in the registry.
func CleanConnData(data map[string]any, line string) string {

	asStringAnyMap := func(v any) map[string]any {
		switch m := v.(type) {
		case map[string]any:
			return m
		case map[any]any:
			out := map[string]any{}
			for k, val := range m {
				out[cast.ToString(k)] = val
			}
			return out
		default:
			return nil
		}
	}

	if data == nil {
		return line
	}
	flat := map[string]string{}
	var nested map[string]any
	for k, v := range data {
		if strings.EqualFold(k, "secrets") {
			if m := asStringAnyMap(v); m != nil {
				nested = m
			}
			continue
		}
		if val := cast.ToString(v); redactable(val) {
			flat[k] = val
		}
	}
	line = Clean(flat, line)
	for _, v := range nested {
		val := strings.TrimSpace(cast.ToString(v))
		if !redactable(val) {
			continue
		}
		line = strings.ReplaceAll(line, val, "***")
	}
	return line
}
