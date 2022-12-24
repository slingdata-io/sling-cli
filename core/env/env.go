package env

import (
	"embed"
	"fmt"
	"io/ioutil"
	"os"

	env "github.com/flarco/dbio/env"
	"github.com/flarco/g"
	"github.com/rs/zerolog"
	"github.com/spf13/cast"
)

var (
	HomeDir         = os.Getenv("SLING_HOME_DIR")
	DbNetDir        = os.Getenv("DBNET_HOME_DIR")
	HomeDirEnvFile  = ""
	DbNetDirEnvFile = ""
	Env             = &env.EnvFile{}
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
		ioutil.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}

	// other sources of creds
	env.SetHomeDir("dbnet")  // https://github.com/dbnet-io/dbnet
	env.SetHomeDir("dbrest") // https://github.com/dbrest-io/dbrest
}

// InitLogger initializes the g Logger
func InitLogger() {
	g.SetZeroLogLevel(zerolog.InfoLevel)
	g.DisableColor = !cast.ToBool(os.Getenv("SLING_LOGGING_COLOR"))

	if os.Getenv("_DEBUG_CALLER_LEVEL") != "" {
		g.CallerLevel = cast.ToInt(os.Getenv("_DEBUG_CALLER_LEVEL"))
	}
	if os.Getenv("_DEBUG") == "TRACE" {
		g.SetZeroLogLevel(zerolog.TraceLevel)
		g.SetLogLevel(g.TraceLevel)
	} else if os.Getenv("_DEBUG") != "" {
		g.SetZeroLogLevel(zerolog.DebugLevel)
		g.SetLogLevel(g.DebugLevel)
		if os.Getenv("_DEBUG") == "LOW" {
			g.SetLogLevel(g.LowDebugLevel)
		}
	}

	// fmt.Printf("g.LogLevel = %d\n", g.GetLogLevel())
	// fmt.Printf("g.zerolog = %d\n", zerolog.GlobalLevel())

	outputOut := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "2006-01-02 15:04:05"}
	outputErr := zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
	outputOut.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	outputErr.FormatErrFieldValue = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	// if os.Getenv("ZLOG") != "PROD" {
	// 	zlog.Logger = zerolog.New(outputErr).With().Timestamp().Logger()
	// }

	if os.Getenv("G_LOGGING") == "TASK" {
		outputOut.NoColor = true
		outputErr.NoColor = true
		g.LogOut = zerolog.New(outputOut).With().Timestamp().Logger()
		g.LogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	} else if os.Getenv("G_LOGGING") == "MASTER" || os.Getenv("G_LOGGING") == "WORKER" {
		zerolog.LevelFieldName = "lvl"
		zerolog.MessageFieldName = "msg"
		g.LogOut = zerolog.New(os.Stdout).With().Timestamp().Logger()
		g.LogErr = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "3:04PM"}
		if g.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
		}
		g.LogOut = zerolog.New(outputErr).With().Timestamp().Logger()
		g.LogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	}
}

func WriteSlingEnvFile(ef env.EnvFile) (err error) {
	return env.WriteEnvFile(HomeDirEnvFile, ef)
}

func LoadSlingEnvFile() (ef env.EnvFile) {
	ef = env.LoadEnvFile(HomeDirEnvFile)
	Env = &ef
	return
}
