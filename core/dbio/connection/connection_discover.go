package connection

import (
	"context"
	"sort"
	"strings"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

func (c *Connection) Test() (ok bool, err error) {

	switch {
	case c.Type.IsDb():
		dbConn, err := c.AsDatabase()
		if err != nil {
			return ok, g.Error(err, "could not initiate %s", c.Name)
		}
		err = dbConn.Connect()
		if err != nil {
			return ok, g.Error(err, "could not connect to %s", c.Name)
		}
		defer dbConn.Close()
	case c.Type.IsFile():
		fileClient, err := c.AsFile()
		if err != nil {
			return ok, g.Error(err, "could not initiate %s", c.Name)
		}
		err = fileClient.Init(context.Background())
		if err != nil {
			return ok, g.Error(err, "could not connect to %s", c.Name)
		}
		defer fileClient.Close()

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
	}

	return
}

type DiscoverOptions struct {
	Pattern     string `json:"pattern,omitempty"`
	ColumnLevel bool   `json:"column_level,omitempty"` // get column level
	Recursive   bool   `json:"recursive,omitempty"`
}

func (c *Connection) Discover(opt *DiscoverOptions) (ok bool, nodes dbio.FileNodes, schemata database.Schemata, err error) {

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
			return ok, nodes, schemata, g.Error(err, "could not initiate %s", c.Name)
		}
		err = dbConn.Connect()
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", c.Name)
		}
		defer dbConn.Close()

		var table database.Table
		if opt.Pattern != "" {
			table, _ = database.ParseTableName(opt.Pattern, c.Type)
			if strings.Contains(table.Schema, "*") || strings.Contains(table.Schema, "?") {
				table.Schema = ""
			}
			if strings.Contains(table.Name, "*") || strings.Contains(table.Name, "?") {
				table.Name = ""
			}
		}
		g.Debug("database discover inputs: %s", g.Marshal(g.M("pattern", opt.Pattern, "schema", table.Schema, "table", table.Name, "column_level", opt.ColumnLevel)))

		schemata, err = dbConn.GetSchemata(table.Schema, table.Name)
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not discover %s", c.Name)
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

	case c.Type.IsFile():
		fileClient, err := c.AsFile()
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not initiate %s", c.Name)
		}
		err = fileClient.Init(context.Background())
		if err != nil {
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", c.Name)
		}
		defer fileClient.Close()

		url := c.URL()
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
			return ok, nodes, schemata, g.Error(err, "could not connect to %s", c.Name)
		}
		g.Debug("unfiltered nodes returned: %d", len(nodes))
		if len(nodes) <= 10 {
			g.Debug(g.Marshal(nodes.Paths()))
		}

		// apply filter
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

	default:
		return ok, nodes, schemata, g.Error("Unhandled connection type: %s", c.Type)
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
