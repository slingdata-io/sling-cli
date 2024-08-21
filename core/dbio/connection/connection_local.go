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
	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
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

func (ce ConnEntries) Get(name string) ConnEntry {
	for _, conn := range ce {
		if strings.EqualFold(conn.Name, name) {
			return conn
		}
	}
	return ConnEntry{}
}

var (
	localConns   ConnEntries
	localConnsTs time.Time
)

func GetLocalConns(force ...bool) ConnEntries {
	if len(force) > 0 && force[0] {
		// force refresh
	} else if time.Since(localConnsTs).Seconds() < 10 {
		return localConns // cachine to not re-read from disk. once every 10s
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

func (ec *EnvConns) List() (fields []string, rows [][]any) {
	conns := GetLocalConns(true)
	fields = []string{"Conn Name", "Conn Type", "Source"}
	for _, conn := range conns {
		rows = append(rows, []any{conn.Name, conn.Description, conn.Source})
	}
	return fields, rows
}

type DiscoverOptions struct {
	Pattern     string `json:"pattern,omitempty"`
	ColumnLevel bool   `json:"column_level,omitempty"` // get column level
	Recursive   bool   `json:"recursive,omitempty"`
	Discover    bool
}

func (ec *EnvConns) Discover(name string, opt *DiscoverOptions) (nodes dbio.FileNodes, schemata database.Schemata, err error) {
	opt.Discover = true
	_, nodes, schemata, err = ec.testDiscover(name, opt)
	return
}

func (ec *EnvConns) Test(name string) (ok bool, err error) {
	ok, _, _, err = ec.testDiscover(name, &DiscoverOptions{})
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

func (ec *EnvConns) testDiscover(name string, opt *DiscoverOptions) (ok bool, nodes dbio.FileNodes, schemata database.Schemata, err error) {

	patterns := []string{}
	globPatterns := []glob.Glob{}

	parsePattern := func() {
		if opt.Pattern != "" {
			patterns = []string{}
			globPatterns = []glob.Glob{}
			for _, f := range strings.Split(opt.Pattern, ",") {
				patterns = append(patterns, f)
				gc, err := glob.Compile(f)
				if err == nil {
					globPatterns = append(globPatterns, gc)
				}
			}
		}
	}

	parsePattern()

	if opt.Pattern != "" && len(patterns) == 1 {
		if strings.Contains(opt.Pattern, "**") || strings.Contains(opt.Pattern, "*/*") {
			opt.Recursive = true
		}
	}

	conn, ok1 := ec.GetConnEntry(name)
	if !ok1 || name == "" {
		return ok, nodes, schemata, g.Error("Invalid Connection name: %s. Make sure it is created. See https://docs.slingdata.io/sling-cli/environment", name)
	}

	switch {

	case conn.Connection.Type.IsDb():
		dbConn, err := conn.Connection.AsDatabase()
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not initiate %s", name)
		}
		err = dbConn.Connect()
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", name)
		}
		defer dbConn.Close()

		if opt.Discover {
			var table database.Table
			if opt.Pattern != "" {
				table, _ = database.ParseTableName(opt.Pattern, dbConn.GetType())
				if strings.Contains(table.Schema, "*") {
					table.Schema = ""
				}
				if strings.Contains(table.Name, "*") {
					table.Name = ""
				}
			}
			g.Debug("database discover inputs: %s", g.Marshal(g.M("pattern", opt.Pattern, "schema", table.Schema, "table", table.Name, "column_level", opt.ColumnLevel)))

			schemata, err = dbConn.GetSchemata(table.Schema, table.Name)
			if err != nil {
				return ok, nodes, schemata, g.Error(err, "could not discover %s", name)
			}

			if opt.ColumnLevel {
				g.Debug("unfiltered nodes returned: %d", len(schemata.Columns()))
				if len(schemata.Columns()) <= 10 {
					g.Debug(g.Marshal(lo.Keys(schemata.Columns())))
				}
			} else {
				g.Debug("unfiltered nodes returned: %d", len(schemata.Tables()))
				if len(schemata.Tables()) <= 10 {
					g.Debug(g.Marshal(lo.Keys(schemata.Tables())))
				}
			}

			// apply filter
			if len(patterns) > 0 {
				schemata = schemata.Filtered(opt.ColumnLevel, patterns...)
			}
		}

	case conn.Connection.Type.IsFile():
		fileClient, err := conn.Connection.AsFile()
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not initiate %s", name)
		}
		err = fileClient.Init(context.Background())
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", name)
		}
		defer fileClient.Close()

		url := conn.Connection.URL()
		if opt.Pattern != "" {
			url = opt.Pattern
		}

		if strings.Contains(url, "*") || strings.Contains(url, "?") {
			opt.Pattern = url
			url = filesys.GetDeepestParent(url)
			parsePattern()
		}

		g.Debug("file discover inputs: %s", g.Marshal(g.M("pattern", opt.Pattern, "url", url, "column_level", opt.ColumnLevel, "recursive", opt.Recursive)))
		if opt.Recursive {
			nodes, err = fileClient.ListRecursive(url)
		} else {
			nodes, err = fileClient.List(url)
		}
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", name)
		}
		g.Debug("unfiltered nodes returned: %d", len(nodes))
		if len(nodes) <= 10 {
			g.Debug(g.Marshal(nodes.Paths()))
		}

		// apply filter
		if opt.Discover {
			// sort alphabetically
			nodes.Sort()
			nodes = lo.Filter(nodes, func(n dbio.FileNode, i int) bool {
				if len(patterns) == 0 || !(strings.Contains(opt.Pattern, "*") || strings.Contains(opt.Pattern, "?")) {
					return true
				}
				for _, gf := range globPatterns {
					if gf.Match(n.Path()) {
						return true
					}
				}
				return false
			})

			// if single file, get columns of file content
			if opt.ColumnLevel {
				ctx := g.NewContext(fileClient.Context().Ctx, 5)

				getColumns := func(i int) {
					defer ctx.Wg.Read.Done()
					node := nodes[i]

					df, err := fileClient.ReadDataflow(node.URI, filesys.FileStreamConfig{Limit: 100})
					if err != nil {
						ctx.CaptureErr(g.Error(err, "could not read file content of %s", node.URI))
						return
					}

					// discard rows, just need columns
					for stream := range df.StreamCh {
						for range stream.Rows() {
						}
					}

					// get columns
					nodes[i].Columns = df.Columns
				}

				for i := range nodes {
					ctx.Wg.Read.Add()
					go getColumns(i)

					if i+1 >= 15 {
						g.Warn("limiting the number of read ops for files (15 files already read)")
						break
					}
				}
				ctx.Wg.Read.Wait()

				if err = ctx.Err(); err != nil {
					return ok, nodes, schemata, g.Error(err, "could not read files")
				}
			}
		}

	default:
		return ok, nodes, schemata, g.Error("Unhandled connection type: %s", conn.Connection.Type)
	}

	ok = true

	return
}

func EnvFileConnectionEntries(ef env.EnvFile, sourceName string) (entries ConnEntries, err error) {
	m := g.M()
	if err = g.JSONConvert(ef, &m); err != nil {
		return entries, g.Error(err)
	}

	connsMap := map[string]ConnEntry{}
	profileConns, err := ReadConnections(m)
	for _, conn := range profileConns {
		c := ConnEntry{
			Name:        strings.ToUpper(conn.Info().Name),
			Description: conn.Type.NameLong(),
			Source:      sourceName,
			Connection:  conn,
		}
		connsMap[c.Name] = c
	}

	entries = lo.Values(connsMap)
	sort.Slice(entries, func(i, j int) bool {
		return cast.ToString(entries[i].Name) < cast.ToString(entries[j].Name)
	})

	return
}
