package env

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
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

var (
	HomeDir         = os.Getenv("SLING_HOME_DIR")
	DbNetDir        = os.Getenv("DBNET_HOME_DIR")
	HomeDirEnvFile  = ""
	DbNetDirEnvFile = ""
)

func init() {
	if HomeDir == "" {
		HomeDir = g.UserHomeDir() + "/.sling"
		os.Setenv("SLING_HOME_DIR", HomeDir)
	}
	if DbNetDir == "" {
		DbNetDir = g.UserHomeDir() + "/.dbnet"
	}
	os.MkdirAll(HomeDir, 0755)

	HomeDirEnvFile = HomeDir + "/env.yaml"
	DbNetDirEnvFile = DbNetDir + "/env.yaml"

	// os.Setenv("DBIO_PROFILE_PATHS", g.F("%s,%s", HomeDirEnvFile, DbNetDirEnvFile))
}

// flatten and rename all children properly
func flatten(key string, val interface{}) (m map[string]string) {
	m = map[string]string{}
	switch v := val.(type) {
	case map[string]interface{}:
		for k2, v2 := range v {
			k2 = g.F("%s_%s", key, k2)
			for k3, v3 := range flatten(k2, v2) {
				m[k3] = v3
			}
		}
	case map[interface{}]interface{}:
		for k2, v2 := range v {
			k2 = g.F("%s_%s", key, k2)
			for k3, v3 := range flatten(cast.ToString(k2), v2) {
				m[k3] = v3
			}
		}
	default:
		m[key] = cast.ToString(v)
	}
	return m
}

func NormalizeEnvFile(filePath string) (env map[string]interface{}, err error) {
	if !g.PathExists(filePath) {
		err = g.Error("%s does not exists", filePath)
		return
	}

	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		err = g.Error(err, "could not read from env file")
		return
	}

	envMap := g.M()
	err = yaml.Unmarshal(bytes, &envMap)
	if err != nil {
		err = g.Error(err, "Error parsing env file bytes")
		return
	}

	// flatten and rename all children properly
	env = map[string]interface{}{}
	for key, val := range envMap {
		switch v := val.(type) {
		case map[string]interface{}:
			m := map[string]interface{}{}
			for k2, v2 := range v {
				m[strings.ToLower(k2)] = v2
			}
			env[strings.ToUpper(key)] = m

		case map[interface{}]interface{}:
			m := map[string]interface{}{}
			for k2, v2 := range v {
				m[strings.ToLower(cast.ToString(k2))] = v2
			}
			env[strings.ToUpper(key)] = m
		default:
			env[strings.ToUpper(key)] = v
		}
	}
	return
}
func FlattenEnvFile(filePath string) (env map[string]string, err error) {
	if !g.PathExists(filePath) {
		err = g.Error("%s does not exists", filePath)
		return
	}

	bytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		err = g.Error(err, "could not read from env file")
		return
	}

	envMap := g.M()
	err = yaml.Unmarshal(bytes, &envMap)
	if err != nil {
		err = g.Error(err, "Error parsing env file bytes")
		return
	}

	// flatten and rename all children properly
	env = map[string]string{}
	for key, val := range envMap {
		for k, v := range flatten(key, val) {
			k = strings.ToUpper(k)
			env[k] = v
		}
	}

	return
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
		if g.IsDebugLow() {
			g.LogError(err)
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
		if g.IsDebugLow() {
			g.LogError(err)
		}
	}
}

// InitLogger initializes the g Logger
func InitLogger() {
	g.SetZeroLogLevel(zerolog.InfoLevel)
	g.DisableColor = !cast.ToBool(os.Getenv("SLING_LOGGING_COLOR"))

	if val := os.Getenv("SLING_SEND_ANON_USAGE"); val != "" {
		DisableSendAnonUsage = cast.ToBool(val)
	}

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

// Env returns the environment variables to propogate
func Env() map[string]interface{} {
	return g.M(
		"SLING_SEND_ANON_USAGE", cast.ToString(!DisableSendAnonUsage),
	)
}

type Conn struct {
	Name        string
	Description string
	Source      string
	Connection  connection.Connection
}

func GetLocalConns() []Conn {
	connsMap := map[string]Conn{}

	// get dbt connections
	dbtConns, err := connection.ReadDbtConnections()
	if !g.LogError(err) {
		for _, conn := range dbtConns {
			c := Conn{
				Name:        strings.ToUpper(conn.Info().Name),
				Description: conn.Type.NameLong(),
				Source:      "dbt profiles yaml",
				Connection:  conn,
			}
			connsMap[c.Name] = c
		}
	}

	if g.PathExists(DbNetDirEnvFile) {
		profileConns, err := connection.ReadConnections(DbNetDirEnvFile)
		if !g.LogError(err) {
			for _, conn := range profileConns {
				c := Conn{
					Name:        strings.ToUpper(conn.Info().Name),
					Description: conn.Type.NameLong(),
					Source:      "dbnet env yaml",
					Connection:  conn,
				}
				connsMap[c.Name] = c
			}
		}
	}

	if g.PathExists(HomeDirEnvFile) {
		profileConns, err := connection.ReadConnections(HomeDirEnvFile)
		if !g.LogError(err) {
			for _, conn := range profileConns {
				c := Conn{
					Name:        strings.ToUpper(conn.Info().Name),
					Description: conn.Type.NameLong(),
					Source:      "sling env yaml",
					Connection:  conn,
				}
				connsMap[c.Name] = c
			}
		}
	}

	// Environment variables
	for key, val := range g.KVArrToMap(os.Environ()...) {
		if !strings.Contains(val, ":/") || strings.Contains(val, "{") {
			continue
		}

		key = strings.ToUpper(key)
		conn, err := connection.NewConnectionFromURL(key, val)
		if err != nil {
			e := g.F("could not parse %s: %s", key, g.ErrMsgSimple(err))
			g.Warn(e)
			continue
		}

		if conn.Type.NameLong() == "" || conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP {
			continue
		}

		c := Conn{
			Name:        conn.Info().Name,
			Description: conn.Type.NameLong(),
			Source:      "env variable",
			Connection:  conn,
		}
		if exC, ok := connsMap[c.Name]; ok {
			g.Warn(
				"conn credentials of %s from %s overwritten by env var %s",
				exC.Name, exC.Source, c.Name,
			)
		}
		connsMap[c.Name] = c
	}

	connArr := lo.Values(connsMap)
	sort.Slice(connArr, func(i, j int) bool {
		return cast.ToString(connArr[i].Name) < cast.ToString(connArr[j].Name)
	})
	return connArr
}
