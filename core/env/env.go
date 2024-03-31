package env

import (
	"bufio"
	"embed"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/flarco/g"
	"github.com/rs/zerolog"
	env "github.com/slingdata-io/sling-cli/core/dbio/env"
	"github.com/spf13/cast"
)

var (
	HomeDir        = os.Getenv("SLING_HOME_DIR")
	HomeDirEnvFile = ""
	Env            = &env.EnvFile{}
	PlausibleURL   = ""
	SentryDsn      = ""
	NoColor        = g.In(os.Getenv("SLING_LOGGING"), "NO_COLOR", "JSON")
	OsStdErr       *os.File
	StderrR        io.ReadCloser
	StdErrW        *os.File
	StdErrChn      chan string
	TelMap         = g.M("begin_time", time.Now().UnixMicro())
)

//go:embed *
var envFolder embed.FS

func init() {

	HomeDir = env.SetHomeDir("sling")
	HomeDirEnvFile = env.GetEnvFilePath(HomeDir)

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
	env.SetHomeDir("dbnet")  // https://github.com/dbnet-io/dbnet
	env.SetHomeDir("dbrest") // https://github.com/dbrest-io/dbrest

	if SentryDsn == "" {
		SentryDsn = os.Getenv("SENTRY_DSN")
	}
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
	outputErr := zerolog.ConsoleWriter{Out: StdErrW, TimeFormat: "2006-01-02 15:04:05"}
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
		outputErr = zerolog.ConsoleWriter{Out: StdErrW, TimeFormat: "3:04PM"}
		if g.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: StdErrW, TimeFormat: "2006-01-02 15:04:05"}
		}
		g.ZLogOut = zerolog.New(outputErr).With().Timestamp().Logger()
		g.ZLogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	}
}

// InitLogger initializes the g Logger
func InitLogger() {

	// capture stdErr
	// OsStdErr = os.Stderr
	// StdErrW = os.Stderr
	StderrR, StdErrW, _ = os.Pipe()
	// os.Stderr = StdErrW

	StderrR2 := io.TeeReader(StderrR, os.Stderr)

	SetLogger()

	if StderrR != nil {
		StderrReader := bufio.NewReader(StderrR2)

		go func() {
			buf := make([]byte, 4*1024)
			for {
				nr, err := StderrReader.Read(buf)
				if err == nil && nr > 0 {
					text := string(buf[0:nr])
					if StdErrChn != nil {
						StdErrChn <- text
					}
				}
			}
		}()
	}
}

func Print(text string) { fmt.Fprintf(StdErrW, "%s", text) }

func Println(text string) { fmt.Fprintf(StdErrW, "%s\n", text) }

func LoadSlingEnvFile() (ef env.EnvFile) {
	ef = env.LoadEnvFile(HomeDirEnvFile)
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
