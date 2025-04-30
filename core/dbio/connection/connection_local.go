package connection

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type ConnEntry struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Source      string     `json:"source"`
	Connection  Connection `json:"connection"`
}

type ConnEntries []ConnEntry

func (ce ConnEntries) Names() (names []string) {
	for _, conn := range ce {
		names = append(names, conn.Name)
	}
	return names
}

func (ce ConnEntries) Get(name string) ConnEntry {
	for _, conn := range ce {
		if strings.EqualFold(conn.Name, name) {
			return conn
		}
	}
	return ConnEntry{}
}

func (ce ConnEntries) List() (fields []string, rows [][]any) {
	fields = []string{"Conn Name", "Conn Type", "Source"}
	for _, conn := range ce {
		rows = append(rows, []any{conn.Name, conn.Description, conn.Source})
	}
	return fields, rows
}

func (ce ConnEntries) Discover(name string, opt *DiscoverOptions) (nodes filesys.FileNodes, schemata database.Schemata, endpoints api.Endpoints, err error) {
	conn := ce.Get(name)
	if name == "" {
		return nodes, schemata, endpoints, g.Error("Invalid Connection name: %s. Make sure it is created. See https://docs.slingdata.io/sling-cli/environment", name)
	}

	defer conn.Connection.Close()
	_, nodes, schemata, endpoints, err = conn.Connection.Discover(opt)
	return
}

func (ce ConnEntries) Test(name string) (ok bool, err error) {
	conn := ce.Get(name)
	if conn.Name == "" {
		return ok, g.Error("Invalid Connection name: %s. Make sure it is created. See https://docs.slingdata.io/sling-cli/environment", name)
	}
	defer conn.Connection.Close()
	ok, err = conn.Connection.Test()
	return
}

var (
	localConns   ConnEntries
	localConnsTs time.Time
)

func GetLocalConns(force ...bool) ConnEntries {
	if len(force) > 0 && force[0] {
		// force refresh
	} else if time.Since(localConnsTs).Seconds() < 10 {
		return localConns // caching to not re-read from disk. once every 10s
	}

	connsMap := map[string]ConnEntry{}

	// local disk connection
	conn, _ := NewConnection("LOCAL", dbio.TypeFileLocal, g.M("type", "file"))
	c := ConnEntry{
		Name:        "LOCAL",
		Description: dbio.TypeFileLocal.NameLong(),
		Source:      "built-in",
		Connection:  conn,
	}
	connsMap[c.Name] = c

	// get dbt connections
	dbtConns, err := ReadDbtConnections()
	if !g.LogError(err) {
		for _, conn := range dbtConns {
			c := ConnEntry{
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

	for name, homeDir := range env.HomeDirs {
		envFilePath := env.GetEnvFilePath(homeDir)
		if g.PathExists(envFilePath) {
			m := g.M()
			g.JSONConvert(env.LoadEnvFile(envFilePath), &m)
			profileConns, err := ReadConnections(m)
			if !g.LogError(err) {
				for _, conn := range profileConns {
					c := ConnEntry{
						Name:        strings.ToUpper(conn.Info().Name),
						Description: conn.Type.NameLong(),
						Source:      name + " env yaml",
						Connection:  conn,
					}
					connsMap[c.Name] = c
				}
			}
		}

	}

	// env.yaml as an Environment variable
	if content := os.Getenv("ENV_YAML"); content != "" {
		ef, err := env.LoadSlingEnvFileBody(content)
		if err != nil {
			g.LogError(g.Error(err, "could not parse ENV_YAML content"))
		} else {
			m := g.M()
			g.JSONConvert(ef, &m)
			profileConns, err := ReadConnections(m)
			if !g.LogError(err) {
				for _, conn := range profileConns {
					c := ConnEntry{
						Name:        strings.ToUpper(conn.Info().Name),
						Description: conn.Type.NameLong(),
						Source:      "env-var env yaml",
						Connection:  conn,
					}
					connsMap[c.Name] = c
				}
			}
		}
	}

	// Environment variables
	for key, val := range g.KVArrToMap(os.Environ()...) {
		var conn Connection

		// try to decode base64
		payload := g.M()
		err := json.Unmarshal([]byte(val), &payload)
		if eVal, ok := payload["base64"]; ok && err == nil && eVal != nil {
			dVal, err := base64.StdEncoding.DecodeString(val)
			if err == nil {
				val = string(dVal)
			}
		} else if strings.HasPrefix(val, "base64:") && strings.HasSuffix(val, ":46esab") {
			val = strings.TrimPrefix(strings.TrimSuffix(val, ":46esab"), "base64:")
			dVal, err := base64.StdEncoding.DecodeString(val)
			if err == nil {
				val = string(dVal)
			}
		}

		// embedded JSON/YAML payload
		payload = g.M()
		err = yaml.Unmarshal([]byte(val), &payload)
		if cType, ok := payload["type"]; ok && err == nil {
			conn, err = NewConnectionFromMap(g.M("name", key, "type", cType, "data", payload))
			if err != nil {
				e := g.F("could not parse env payload %s: %s", key, g.ErrMsgSimple(err))
				g.Warn(e)
				continue
			}
		} else {
			// Parse URL
			if !strings.Contains(val, "://") || strings.Contains(val, "{") || strings.Contains(val, "[") {
				continue
			}

			key = strings.ToUpper(key)
			conn, err = NewConnectionFromURL(key, val)
			if err != nil {
				e := g.F("could not parse %s: %s", key, g.ErrMsgSimple(err))
				g.Warn(e)
				continue
			}
		}

		if conn.Type.NameLong() == "" || conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP || string(conn.Info().Type) == "https" {
			continue
		}

		c := ConnEntry{
			Name:        conn.Info().Name,
			Description: conn.Type.NameLong(),
			Source:      "env variable",
			Connection:  conn,
		}
		if exC, ok := connsMap[c.Name]; ok {
			g.Debug(
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

func LocalFileConnEntry() ConnEntry {
	c, _ := NewConnection("LOCAL", "file", nil)
	return ConnEntry{
		Name:       "LOCAL",
		Source:     "built-in",
		Connection: c,
	}
}

type EnvFileConns struct {
	Name    string
	EnvFile *env.EnvFile
}

func (ec *EnvFileConns) Set(name string, kvMap map[string]any) (err error) {

	if name == "" {
		return g.Error("name is blank")
	}

	// parse url
	if url := cast.ToString(kvMap["url"]); url != "" {
		conn, err := NewConnectionFromURL(name, url)
		if err != nil {
			return g.Error(err, "could not parse url")
		}
		if _, ok := kvMap["type"]; !ok {
			kvMap["type"] = conn.Type.String()
		}
	}

	t, found := kvMap["type"]
	if _, typeOK := dbio.ValidateType(cast.ToString(t)); found && !typeOK {
		return g.Error("invalid type (%s)", cast.ToString(t))
	} else if !found {
		return g.Error("need to specify valid `type` key or provide `url`.")
	}

	ef := ec.EnvFile
	ef.Connections[name] = kvMap
	err = ef.WriteEnvFile()
	if err != nil {
		return g.Error(err, "could not write env file")
	}

	return
}

func (ec *EnvFileConns) Unset(name string) (err error) {
	if name == "" {
		return g.Error("name is blank")
	}

	ef := ec.EnvFile
	_, ok := ef.Connections[name]
	if !ok {
		return g.Error("did not find connection `%s`", name)
	}

	delete(ef.Connections, name)
	err = ef.WriteEnvFile()
	if err != nil {
		return g.Error(err, "could not write env file")
	}

	return
}

func (ec *EnvFileConns) ConnectionEntries() (entries ConnEntries, err error) {
	m := g.M()
	if err = g.JSONConvert(ec.EnvFile, &m); err != nil {
		return entries, g.Error(err)
	}

	connsMap := map[string]ConnEntry{}
	profileConns, err := ReadConnections(m)
	for _, conn := range profileConns {
		c := ConnEntry{
			Name:        strings.ToUpper(conn.Info().Name),
			Description: conn.Type.NameLong(),
			Source:      ec.Name,
			Connection:  conn,
		}
		if c.Source == "" {
			c.Source = "provided env.yaml"
		}
		connsMap[c.Name] = c
	}

	entries = lo.Values(connsMap)
	sort.Slice(entries, func(i, j int) bool {
		return cast.ToString(entries[i].Name) < cast.ToString(entries[j].Name)
	})

	return
}

// GetConnEntry get connection from envfile (may be provided via body)
func (ec *EnvFileConns) GetConnEntry(name string) (conn ConnEntry, ok bool) {
	if ec.EnvFile == nil {
		return
	}

	entries, err := ec.ConnectionEntries()
	if err != nil {
		return
	}

	conn = entries.Get(name)
	return conn, conn.Name != ""
}
