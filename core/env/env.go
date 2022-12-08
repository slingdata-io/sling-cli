package env

import (
	"embed"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/g"
	"github.com/rs/zerolog"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

var (
	HomeDir         = os.Getenv("SLING_HOME_DIR")
	DbNetDir        = os.Getenv("DBNET_HOME_DIR")
	HomeDirEnvFile  = ""
	DbNetDirEnvFile = ""
	Env             = &EnvFile{}
)

//go:embed *
var envFolder embed.FS

type EnvFile struct {
	Connections map[string]map[string]interface{} `json:"connections,omitempty" yaml:"connections,omitempty"`
	Variables   map[string]interface{}            `json:"variables,omitempty" yaml:"variables,omitempty"`
	Worker      map[string]string                 `json:"worker,omitempty" yaml:"worker,omitempty"`
}

func init() {
	if HomeDir == "" {
		HomeDir = path.Join(g.UserHomeDir(), ".sling")
		os.Setenv("SLING_HOME_DIR", HomeDir)
	}
	if DbNetDir == "" {
		DbNetDir = path.Join(g.UserHomeDir(), ".dbnet")
	}
	os.MkdirAll(HomeDir, 0755)

	HomeDirEnvFile = path.Join(HomeDir, "env.yaml")
	DbNetDirEnvFile = path.Join(DbNetDir, "env.yaml")

	// os.Setenv("PROFILE_PATHS", g.F("%s,%s", HomeDirEnvFile, DbNetDirEnvFile))
	// create env file if not exists
	if !g.PathExists(HomeDirEnvFile) {
		defaultEnvBytes, _ := envFolder.ReadFile("default.env.yaml")
		ioutil.WriteFile(HomeDirEnvFile, defaultEnvBytes, 0644)
	}

	// load init env file
	LoadSlingEnvFile()
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

func WriteSlingEnvFile(ef EnvFile) (err error) {
	return WriteEnvFile(HomeDirEnvFile, ef)
}

func WriteEnvFile(path string, ef EnvFile) (err error) {
	connsMap := yaml.MapSlice{}

	// order connections names
	names := lo.Keys(ef.Connections)
	sort.Strings(names)
	for _, name := range names {
		keyMap := ef.Connections[name]
		// order connection keys (type first)
		cMap := yaml.MapSlice{}
		keys := lo.Keys(keyMap)
		sort.Strings(keys)
		if v, ok := keyMap["type"]; ok {
			cMap = append(cMap, yaml.MapItem{Key: "type", Value: v})
		}

		for _, k := range keys {
			if k == "type" {
				continue // already put first
			}
			k = cast.ToString(k)
			cMap = append(cMap, yaml.MapItem{Key: k, Value: keyMap[k]})
		}

		// add to connection map
		connsMap = append(connsMap, yaml.MapItem{Key: name, Value: cMap})
	}

	efMap := yaml.MapSlice{
		{Key: "connections", Value: connsMap},
		{Key: "variables", Value: ef.Variables},
		{Key: "worker", Value: ef.Worker},
	}

	envBytes, err := yaml.Marshal(efMap)
	if err != nil {
		return g.Error(err, "could not marshal into YAML")
	}

	output := []byte("# Environment Credentials for Sling CLI\n# See https://docs.slingdata.io/sling-cli/environment\n" + string(envBytes))

	err = ioutil.WriteFile(HomeDirEnvFile, formatYAML(output), 0644)
	if err != nil {
		return g.Error(err, "could not write YAML file")
	}

	return
}

func formatYAML(input []byte) []byte {
	newOutput := []byte{}
	pIndent := 0
	indent := 0
	inIndent := true
	prevC := byte('-')
	for _, c := range input {
		add := false
		if c == ' ' && inIndent {
			indent++
			add = true
		} else if c == '\n' {
			pIndent = indent
			indent = 0
			add = true
			inIndent = true
		} else if prevC == '\n' {
			newOutput = append(newOutput, '\n') // add extra space
			add = true
		} else if prevC == ' ' && pIndent > indent && inIndent {
			newOutput = append(newOutput, '\n') // add extra space
			for i := 0; i < indent; i++ {
				newOutput = append(newOutput, ' ')
			}
			add = true
			inIndent = false
		} else {
			add = true
			inIndent = false
		}

		if add {
			newOutput = append(newOutput, c)
		}
		prevC = c
	}
	return newOutput
}

func LoadSlingEnvFile() (ef EnvFile) {
	ef = LoadEnvFile(HomeDirEnvFile)
	Env = &ef
	return
}

func LoadEnvFile(path string) (ef EnvFile) {
	bytes, _ := ioutil.ReadFile(path)
	err := yaml.Unmarshal(bytes, &ef)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		_ = err
	}

	if ef.Connections == nil {
		ef.Connections = map[string]map[string]interface{}{}
	}

	if ef.Variables == nil {
		ef.Variables = map[string]interface{}{}
	}

	if ef.Worker == nil {
		ef.Worker = map[string]string{}
	}

	// set env vars
	envMap := map[string]string{}
	for _, tuple := range os.Environ() {
		key := strings.Split(tuple, "=")[0]
		val := strings.TrimPrefix(tuple, key+"=")
		envMap[key] = val
	}

	for k, v := range ef.Variables {
		if _, found := envMap[k]; !found {
			os.Setenv(k, cast.ToString(v))
		}
	}
	return ef
}

type Conn struct {
	Name        string
	Description string
	Source      string
	Connection  connection.Connection
}

var (
	localConns   []Conn
	localConnsTs time.Time
)

func GetLocalConns() []Conn {
	if time.Since(localConnsTs).Seconds() < 10 {
		return localConns // cachine to not re-read from disk. once every 10s
	}

	connsMap := map[string]Conn{}

	// TODO: add local disk connection
	// conn, _ := connection.NewConnection("LOCAL_DISK", dbio.TypeFileLocal, g.M("url", "file://."))
	// c := Conn{
	// 	Name:        "LOCAL_DISK",
	// 	Description: dbio.TypeFileLocal.NameLong(),
	// 	Source:      "built-in",
	// 	Connection:  conn,
	// }
	// connsMap[c.Name] = c

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
			c.Name = strings.ReplaceAll(c.Name, "/", "_")
			c.Connection.Name = strings.ReplaceAll(c.Connection.Name, "/", "_")
			connsMap[c.Name] = c
		}
	}

	if g.PathExists(DbNetDirEnvFile) {
		m := g.M()
		g.JSONConvert(LoadEnvFile(DbNetDirEnvFile), &m)
		profileConns, err := connection.ReadConnections(m)
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
		m := g.M()
		g.JSONConvert(LoadSlingEnvFile(), &m)
		profileConns, err := connection.ReadConnections(m)
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

	localConnsTs = time.Now()
	localConns = connArr

	return connArr
}
