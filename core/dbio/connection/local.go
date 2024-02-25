package connection

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jedib0t/go-pretty/table"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/env"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type ConnEntry struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Source      string     `json:"source"`
	Connection  Connection `json:"connection"`
}

var (
	localConns   []ConnEntry
	localConnsTs time.Time
)

func GetLocalConns(force ...bool) []ConnEntry {
	if len(force) > 0 && force[0] {
		// force refresh
	} else if time.Since(localConnsTs).Seconds() < 10 {
		return localConns // cachine to not re-read from disk. once every 10s
	}

	connsMap := map[string]ConnEntry{}

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
		m := g.M()
		err := yaml.Unmarshal([]byte(content), &m)
		if err != nil {
			g.LogError(g.Error(err, "could not parse ENV_YAML content"))
		} else {
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

		if conn.Type.NameLong() == "" || conn.Info().Type == dbio.TypeUnknown || conn.Info().Type == dbio.TypeFileHTTP {
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

type EnvConns struct {
	EnvFile *env.EnvFile
}

func (ec *EnvConns) Set(name string, kvMap map[string]any) (err error) {

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

func (ec *EnvConns) Unset(name string) (err error) {
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

func (ec *EnvConns) List() string {
	conns := GetLocalConns(true)
	T := table.NewWriter()
	T.AppendHeader(table.Row{"Conn Name", "Conn Type", "Source"})
	for _, conn := range conns {
		T.AppendRow(table.Row{conn.Name, conn.Description, conn.Source})
	}
	return T.Render()
}

type DiscoverOptions struct {
	Stream    string
	Filter    string
	Schema    string
	Folder    string
	Recursive bool
	discover  bool
}

func (ec *EnvConns) Discover(name string, opt DiscoverOptions) (streamNames []string, schemata database.Schemata, err error) {
	opt.discover = true
	_, streamNames, schemata, err = ec.testDiscover(name, opt)
	return
}

func (ec *EnvConns) Test(name string) (ok bool, err error) {
	ok, _, _, err = ec.testDiscover(name, DiscoverOptions{})
	return
}

func (ec *EnvConns) GetConnEntry(name string) (conn ConnEntry, ok bool) {
	conns := map[string]ConnEntry{}
	for _, conn := range GetLocalConns() {
		conns[strings.ToLower(conn.Name)] = conn
	}

	conn, ok = conns[strings.ToLower(name)]
	return
}

func (ec *EnvConns) testDiscover(name string, opt DiscoverOptions) (ok bool, streamNames []string, schemata database.Schemata, err error) {
	discover := opt.discover
	stream := opt.Stream
	schema := opt.Schema
	folder := opt.Folder
	filter := opt.Filter
	recursive := opt.Recursive

	conn, ok1 := ec.GetConnEntry(name)
	if !ok1 || name == "" {
		return ok, streamNames, schemata, g.Error("Invalid Connection name: %s", name)
	}

	switch {

	case conn.Connection.Type.IsDb():
		dbConn, err := conn.Connection.AsDatabase()
		if err != nil {
			return ok, streamNames, schemata, g.Error(err, "could not initiate %s", name)
		}
		err = dbConn.Connect()
		if err != nil {
			return ok, streamNames, schemata, g.Error(err, "could not connect to %s", name)
		}
		if discover {
			if stream != "" {
				table, err := database.ParseTableName(stream, dbConn.GetType())
				if err != nil {
					return ok, streamNames, schemata, g.Error(err, "could not parse table name %s", stream)
				}
				schema = table.Schema
				stream = table.Name
			}
			schemata, err = dbConn.GetSchemata(schema, stream)
			if err != nil {
				return ok, streamNames, schemata, g.Error(err, "could not discover %s", name)
			}
			for _, table := range schemata.Tables() {
				streamNames = append(streamNames, table.FullName())
			}
		}

	case conn.Connection.Type.IsFile():
		fileClient, err := conn.Connection.AsFile()
		if err != nil {
			return ok, streamNames, schemata, g.Error(err, "could not initiate %s", name)
		}
		err = fileClient.Init(context.Background())
		if err != nil {
			return ok, streamNames, schemata, g.Error(err, "could not connect to %s", name)
		}

		url := conn.Connection.URL()
		if folder != "" {
			if !strings.HasPrefix(folder, string(fileClient.FsType())+"://") {
				return ok, streamNames, schemata, g.Error("need to use proper URL for folder path. Example -> %s/my-folder", url)
			}
			url = folder
		}

		if recursive {
			streamNames, err = fileClient.ListRecursive(url)
		} else {
			streamNames, err = fileClient.List(url)
		}
		if err != nil {
			return ok, streamNames, schemata, g.Error(err, "could not connect to %s", name)
		}

	default:
		return ok, streamNames, schemata, g.Error("Unhandled connection type: %s", conn.Connection.Type)
	}

	if discover {
		// sort alphabetically
		sort.Slice(streamNames, func(i, j int) bool {
			return streamNames[i] < streamNames[j]
		})

		filters := strings.Split(filter, ",")
		streamNames = lo.Filter(streamNames, func(n string, i int) bool {
			return filter == "" || g.IsMatched(filters, n)
		})
		if len(streamNames) > 0 && conn.Connection.Type.IsFile() &&
			folder == "" {
			g.Warn("Those are non-recursive folder or file names (at the root level). Please use --folder flag to list sub-folders")
			println()
		}
	} else {
		ok = true
	}

	return
}
