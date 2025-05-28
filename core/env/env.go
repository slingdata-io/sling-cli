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
	g.DisableColor = !cast.ToBool(os.Getenv("SLING_LOGGING_COLOR"))

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

	outputOut := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	outputErr := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
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
		g.ZLogOut = zerolog.New(outputOut).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	} else if os.Getenv("SLING_LOGGING") == "JSON" {
		NoColor = true
		zerolog.LevelFieldName = "lvl"
		zerolog.MessageFieldName = "msg"
		g.ZLogOut = zerolog.New(os.Stdout).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "3:04PM"}
		if g.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
		}
		g.ZLogOut = zerolog.New(outputErr).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	}
}

// InitLogger initializes the g Logger
func InitLogger() {

	// set log hook
	g.SetLogHook(
		g.NewLogHook(
			g.DebugLevel,
			func(ll *g.LogLine) { processLogEntry(ll) },
		),
	)

	SetLogger()
}

func Print(text string) {
	fmt.Fprintf(os.Stderr, "%s", text)
	processLogEntry(&g.LogLine{Level: 9, Text: text})
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
			Connections: map[string]map[string]interface{}{},
			Variables:   map[string]interface{}{},
		}, nil
	}
	err = yaml.Unmarshal([]byte(body), &ef)
	return
}

func GreenString(text string) string {
	if NoColor {
		return text
	}
	return g.Colorize(g.ColorGreen, text)
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

func readConnectionsMap(env map[string]interface{}) (conns map[string]map[string]any, err error) {
	conns = map[string]map[string]any{}

	if connections, ok := env["connections"]; ok {
		switch connectionsV := connections.(type) {
		case map[string]interface{}, map[interface{}]interface{}:
			connMap := cast.ToStringMap(connectionsV)
			for name, v := range connMap {
				switch v.(type) {
				case map[string]interface{}, map[interface{}]interface{}:
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
	contextArgs := g.M("conn", props["sling_conn_id"])
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
			g.Debug(query)
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
