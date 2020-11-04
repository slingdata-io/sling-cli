package env

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	h "github.com/flarco/gutil"
	"github.com/flarco/gutil/net"
	"github.com/rs/zerolog"
	"github.com/spf13/cast"
)

var DisableSendAnonUsage = false

var envVars = []string{
	"SLING_PARALLEL", "AWS_BUCKET", "AWS_ACCESS_KEY_ID",
	"AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_ENDPOINT", "AWS_REGION",
	"SLING_COMPRESSION", "SLING_FILE_ROW_LIMIT", "SLING_SAMPLE_SIZE",
	"GC_BUCKET", "GC_CRED_FILE", "GSHEETS_CRED_FILE",
	"GC_CRED_JSON_BODY", "GC_CRED_JSON_BODY_ENC", "GC_CRED_API_KEY",
	"AZURE_ACCOUNT", "AZURE_KEY", "AZURE_CONTAINER", "AZURE_SAS_SVC_URL",
	"AZURE_CONN_STR", "SSH_TUNNEL", "SSH_PRIVATE_KEY", "SSH_PUBLIC_KEY",
	"SLING_CONCURENCY_LIMIT",

	"SLING_SMTP_HOST", "SLING_SMTP_PORT", "SLING_SMTP_USERNAME", "SLING_SMTP_PASSWORD", "SLING_SMTP_FROM_EMAIL", "SLING_SMTP_REPLY_EMAIL",

	"SFTP_USER", "SFTP_PASSWORD", "SFTP_HOST", "SFTP_PORT",
	"SSH_PRIVATE_KEY", "SFTP_PRIVATE_KEY", "SFTP_URL",

	"HTTP_USER", "HTTP_PASSWORD", "GSHEET_CLIENT_JSON_BODY",
	"GSHEET_SHEET_NAME", "GSHEET_MODE",

	"DIGITALOCEAN_ACCESS_TOKEN", "GITHUB_ACCESS_TOKEN",
	"SURVEYMONKEY_ACCESS_TOKEN",

	"SLING_SEND_ANON_USAGE", "SLING_HOME",
}

// EnvVars are the variables we are using
func EnvVars() (vars map[string]string) {
	vars = map[string]string{}
	// get default from environment
	for _, k := range envVars {
		if vars[k] == "" {
			vars[k] = os.Getenv(k)
		}
	}

	// default as true
	for _, k := range []string{} {
		if vars[k] == "" {
			vars[k] = "true"
		}
	}

	if vars["SLING_CONCURENCY_LIMIT"] == "" {
		vars["SLING_CONCURENCY_LIMIT"] = "10"
	}

	if vars["SLING_SAMPLE_SIZE"] == "" {
		vars["SLING_SAMPLE_SIZE"] = "900"
	}

	if bodyEnc := vars["GC_CRED_JSON_BODY_ENC"]; bodyEnc != "" {
		body, _ := url.QueryUnescape(bodyEnc)
		vars["GC_CRED_JSON_BODY"] = body
	}
	return
}

// LogEvent logs to Graylog
func LogEvent(m map[string]interface{}) {
	if DisableSendAnonUsage {
		return
	}

	URL := "https://logapi.slingdata.io/log/event/prd"
	if os.Getenv("SLING_ENV") == "STG" {
		URL = "https://logapi.slingdata.io/log/event/stg"
	}

	jsonBytes, err := json.Marshal(m)
	if err != nil {
		if h.IsDebugLow() {
			h.LogError(err)
		}
	}

	_, _, err = net.ClientDo(
		"POST",
		URL,
		bytes.NewBuffer(jsonBytes),
		map[string]string{"Content-Type": "application/json"},
		1,
	)

	if err != nil {
		if h.IsDebugLow() {
			h.LogError(err)
		}
	}
}

// InitLogger initializes the Gutil Logger
func InitLogger() {
	h.SetZeroLogLevel(zerolog.InfoLevel)
	h.DisableColor = !cast.ToBool(os.Getenv("SLING_LOGGING_COLOR"))

	if val := os.Getenv("SLING_SEND_ANON_USAGE"); val != "" {
		DisableSendAnonUsage = cast.ToBool(val)
	}

	if os.Getenv("SLING_DEBUG_CALLER_LEVEL") != "" {
		h.CallerLevel = cast.ToInt(os.Getenv("SLING_DEBUG_CALLER_LEVEL"))
	}
	if os.Getenv("SLING_DEBUG") == "TRACE" {
		h.SetZeroLogLevel(zerolog.TraceLevel)
		h.SetLogLevel(h.TraceLevel)
	} else if os.Getenv("SLING_DEBUG") != "" {
		h.SetZeroLogLevel(zerolog.DebugLevel)
		h.SetLogLevel(h.DebugLevel)
		if os.Getenv("SLING_DEBUG") == "LOW" {
			h.SetLogLevel(h.LowDebugLevel)
		}
	}

	// fmt.Printf("gutil.LogLevel = %d\n", h.GetLogLevel())
	// fmt.Printf("gutil.zerolog = %d\n", zerolog.GlobalLevel())

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

	if os.Getenv("SLING_LOGGING") == "TASK" {
		outputOut.NoColor = true
		outputErr.NoColor = true
		h.LogOut = zerolog.New(outputOut).With().Timestamp().Logger()
		h.LogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	} else if os.Getenv("SLING_LOGGING") == "MASTER" || os.Getenv("SLING_LOGGING") == "WORKER" {
		zerolog.LevelFieldName = "lvl"
		zerolog.MessageFieldName = "msg"
		h.LogOut = zerolog.New(os.Stdout).With().Timestamp().Logger()
		h.LogErr = zerolog.New(os.Stdout).With().Timestamp().Logger()
	} else {
		outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "3:04PM"}
		if h.IsDebugLow() {
			outputErr = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "2006-01-02 15:04:05"}
		}
		h.LogOut = zerolog.New(outputErr).With().Timestamp().Logger()
		h.LogErr = zerolog.New(outputErr).With().Timestamp().Logger()
	}
}

// Env returns the environment variables to propogate
func Env() map[string]interface{} {
	return h.M(
		"SLING_SEND_ANON_USAGE", cast.ToString(!DisableSendAnonUsage),
	)
}
