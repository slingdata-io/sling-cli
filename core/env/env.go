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
	"github.com/rs/zerolog"
	"github.com/spf13/cast"
)

var (
	HomeDir        = os.Getenv("SLING_HOME_DIR")
	HomeDirEnvFile = ""
	Env            = &EnvFile{}
	PlausibleURL   = ""
	SentryDsn      = ""
	NoColor        = g.In(os.Getenv("SLING_LOGGING"), "NO_COLOR", "JSON")
	LogSink        func(t string)
	TelMap         = g.M("begin_time", time.Now().UnixMicro())
	TelMux         = sync.Mutex{}
	HomeDirs       = map[string]string{}
	envMux         = sync.Mutex{}
)

//go:embed *
var envFolder embed.FS

func init() {

	HomeDir = SetHomeDir("sling")
	HomeDirEnvFile = GetEnvFilePath(HomeDir)

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
}

func HomeBinDir() string {
	return path.Join(HomeDir, "bin")
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
			func(le *g.LogEntry) { processLogEntry(le) },
		),
	)

	SetLogger()
}

func Print(text string) {
	fmt.Fprintf(os.Stderr, "%s", text)
	processLogEntry(&g.LogEntry{Level: 99, Text: text})
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
	return cleanWindowsPath(tempDir)
}

func cleanWindowsPath(path string) string {
	return strings.ReplaceAll(path, `\`, `/`)
}

func processLogEntry(le *g.LogEntry) {
	// construct log line like zerolog
	var timeText, levelPrefix string

	switch le.Level {
	case zerolog.TraceLevel:
		levelPrefix = "\x1b[35mTRC\x1b[0m "
	case zerolog.DebugLevel:
		levelPrefix = "\x1b[33mDBG\x1b[0m "
	case zerolog.InfoLevel:
		levelPrefix = "\x1b[32mINF\x1b[0m "
	case zerolog.WarnLevel:
		levelPrefix = "\x1b[31mWRN\x1b[0m "
	}

	if !le.Time.IsZero() {
		timeText = g.F(
			"\x1b[90m%s\x1b[0m ",
			le.Time.Format("2006-01-02 15:04:05"),
		)
	}

	msg := g.F(timeText+levelPrefix+le.Text, le.Args...)

	if LogSink != nil {
		LogSink(msg)
	}
}
