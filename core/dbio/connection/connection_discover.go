package connection

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (c *Connection) Test() (ok bool, err error) {
	os.Setenv("SLING_TEST_MODE", "true")

	switch {
	case c.Type.IsDb():
		dbConn, err := c.AsDatabase()
		if err != nil {
			return ok, g.Error(err, "could not initiate %s", c.Name)
		}
		err = dbConn.Connect(10)
		if err != nil {
			return ok, g.Error(err, "could not connect to %s", c.Name)
		}
	case c.Type.IsFile():
		fileClient, err := c.AsFile()
		if err != nil {
			return ok, g.Error(err, "could not initiate %s", c.Name)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		err = fileClient.Init(ctx)
		if err != nil {
			return ok, g.Error(err, "could not connect to %s", c.Name)
		}

		url := c.URL()

		g.Debug("file test inputs: %s", g.Marshal(g.M("url", url)))
		nodes, err := fileClient.List(url)
		if err != nil {
			return ok, g.Error(err, "could not connect to %s", c.Name)
		}
		g.Debug("unfiltered nodes returned: %d", len(nodes))
		if len(nodes) <= 10 {
			g.Debug(g.Marshal(nodes.Paths()))
		}
	case c.Type.IsAPI():
		apiClient, err := c.AsAPI()
		if err != nil {
			return ok, g.Error(err, "could not initiate %s", c.Name)
		}

		// set testing mode to limit requests
		apiClient.Context.Map.Set("testing", true)

		if err := apiClient.Authenticate(); err != nil {
			return ok, g.Error(err, "could not authenticate to %s", c.Name)
		}

		for i, endpoint := range apiClient.Spec.Endpoints {
			// set limits for testing
			endpoint.Response.Records.Limit = 10
			apiClient.Spec.Endpoints[i] = endpoint

			g.Info("testing endpoint: %#v", endpoint.Name)
			df, err := apiClient.ReadDataflow(endpoint.Name)
			if err != nil {
				return ok, g.Error(err, "error testing endpoint: %s", endpoint.Name)
			}

			data, err := df.Collect()
			if err != nil {
				return ok, g.Error(err, "could collect data from endpoint: %s", endpoint.Name)
			}

			g.Debug("   got %d records from endpoint: %s", len(data.Rows), endpoint.Name)
		}

	}

	return true, nil
}

type DiscoverOptions struct {
	Pattern   string                 `json:"pattern,omitempty"`
	Level     database.SchemataLevel `json:"level,omitempty"`
	Recursive bool                   `json:"recursive,omitempty"`
}

func (c *Connection) Discover(opt *DiscoverOptions) (ok bool, nodes filesys.FileNodes, schemata database.Schemata, endpoints api.Endpoints, err error) {

	patterns := []string{}
	globPatterns := []glob.Glob{}

	parsePattern := func() {
		if opt.Pattern != "" {
			patterns = []string{}
			globPatterns = []glob.Glob{}
			for _, f := range strings.Split(opt.Pattern, "|") {
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

	switch {
	case c.Type.IsDb():
		dbConn, err := c.AsDatabase()
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not initiate %s", c.Name)
		}
		err = dbConn.Connect(10)
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not connect to %s", c.Name)
		}

		var table database.Table
		level := database.SchemataLevelSchema
		if opt.Pattern != "" {
			level = database.SchemataLevelTable
			table, _ = database.ParseTableName(opt.Pattern, c.Type)
			if strings.Contains(table.Schema, "*") || strings.Contains(table.Schema, "?") {
				table.Schema = ""
			}
			if strings.Contains(table.Name, "*") || strings.Contains(table.Name, "?") {
				table.Name = ""
			}
		}

		if string(opt.Level) == "" {
			opt.Level = level
		}

		g.Debug("database discover inputs: %s", g.Marshal(g.M("pattern", opt.Pattern, "schema", table.Schema, "table", table.Name, "level", opt.Level)))

		schemata, err = dbConn.GetSchemata(opt.Level, table.Schema, table.Name)
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not discover %s", c.Name)
		}

		if opt.Level == database.SchemataLevelColumn {
			g.Debug("unfiltered column records returned: %d", len(schemata.Columns()))
			if len(schemata.Columns()) <= 15 {
				g.Debug(g.Marshal(lo.Keys(schemata.Columns())))
			}
		} else {
			g.Debug("unfiltered table records returned: %d", len(schemata.Tables()))
			if len(schemata.Tables()) <= 15 {
				g.Debug(g.Marshal(lo.Keys(schemata.Tables())))
			}
		}

		// apply filter if table is not specified
		if len(patterns) > 0 && table.Name == "" {
			schemata = schemata.Filtered(opt.Level == database.SchemataLevelColumn, patterns...)
		}

	case c.Type.IsFile():
		fileClient, err := c.AsFile()
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not initiate %s", c.Name)
		}

		parent, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()

		err = fileClient.Init(parent)
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not connect to %s", c.Name)
		}

		url := c.URL()
		if opt.Pattern != "" {
			url = opt.Pattern
		}

		if strings.Contains(url, "*") || strings.Contains(url, "?") {
			opt.Pattern = url
			url = filesys.GetDeepestParent(url)
			parsePattern()
		}

		g.Debug("file discover inputs: %s", g.Marshal(g.M("pattern", opt.Pattern, "url", url, "column_level", opt.Level, "recursive", opt.Recursive)))
		if opt.Recursive {
			nodes, err = fileClient.ListRecursive(url)
		} else {
			nodes, err = fileClient.List(url)
		}
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not connect to %s", c.Name)
		}
		g.Debug("unfiltered nodes returned: %d", len(nodes))
		if len(nodes) <= 20 {
			g.Debug(g.Marshal(nodes.Paths()))
		}

		// apply filter
		// sort alphabetically
		nodes.Sort()
		nodes = lo.Filter(nodes, func(n filesys.FileNode, i int) bool {
			if len(patterns) == 0 || !(strings.Contains(opt.Pattern, "*") || strings.Contains(opt.Pattern, "?")) {
				return true
			}
			for _, gf := range globPatterns {
				if gf.Match(strings.TrimSuffix(n.Path(), "/")) {
					return true
				}
			}
			return false
		})

		// if single file, get columns of file content
		if opt.Level == database.SchemataLevelColumn {
			ctx := g.NewContext(fileClient.Context().Ctx, 5)

			getColumns := func(i int) {
				defer ctx.Wg.Read.Done()
				node := nodes[i]

				df, err := fileClient.ReadDataflow(node.URI, iop.FileStreamConfig{Limit: 100})
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
				return ok, nodes, schemata, endpoints, g.Error(err, "could not read files")
			}
		}

	case c.Type.IsAPI():
		apiConn, err := c.AsAPI()
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not initiate %s", c.Name)
		}

		endpoints, err = apiConn.ListEndpoints(opt.Pattern)
		if err != nil {
			return ok, nodes, schemata, endpoints, g.Error(err, "could not list endpoints from: %s", c.Name)
		}

	default:
		return ok, nodes, schemata, endpoints, g.Error("Unhandled connection type: %s", c.Type)
	}

	ok = true

	return
}
