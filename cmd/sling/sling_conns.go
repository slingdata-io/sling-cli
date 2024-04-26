package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
)

func processConns(c *g.CliSC) (ok bool, err error) {
	ok = true

	ef := env.LoadSlingEnvFile()
	ec := connection.EnvConns{EnvFile: &ef}
	asJSON := os.Getenv("SLING_OUTPUT") == "json"

	env.SetTelVal("task_start_time", time.Now())
	defer func() {
		env.SetTelVal("task_status", lo.Ternary(err != nil, "error", "success"))
		env.SetTelVal("task_end_time", time.Now())
	}()

	switch c.UsedSC() {
	case "unset":
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))
		if name == "" {
			flaggy.ShowHelp("")
			return ok, nil
		}

		err := ec.Unset(name)
		if err != nil {
			return ok, g.Error(err, "could not unset %s", name)
		}
		g.Info("connection `%s` has been removed from %s", name, ec.EnvFile.Path)
	case "set":
		if len(c.Vals) == 0 {
			flaggy.ShowHelp("")
			return ok, nil
		}

		kvArr := []string{cast.ToString(c.Vals["value properties..."])}
		kvMap := map[string]interface{}{}
		for k, v := range g.KVArrToMap(append(kvArr, flaggy.TrailingArguments...)...) {
			k = strings.ToLower(k)
			kvMap[k] = v
		}
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))

		err := ec.Set(name, kvMap)
		if err != nil {
			return ok, g.Error(err, "could not set %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}
		g.Info("connection `%s` has been set in %s. Please test with `sling conns test %s`", name, ec.EnvFile.Path, name)
	case "exec":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnExec)))

		name := cast.ToString(c.Vals["name"])
		conn, ok := ec.GetConnEntry(name)
		if !ok {
			return ok, g.Error("did not find connection %s", name)
		}

		env.SetTelVal("conn_type", conn.Connection.Type.String())

		if !conn.Connection.Type.IsDb() {
			return ok, g.Error("cannot execute SQL query on a non-database connection (%s)", conn.Connection.Type)
		}

		start := time.Now()
		dbConn, err := conn.Connection.AsDatabase()
		if err != nil {
			return ok, g.Error(err, "cannot create database connection (%s)", conn.Connection.Type)
		}

		err = dbConn.Connect()
		if err != nil {
			return ok, g.Error(err, "cannot connect to database (%s)", conn.Connection.Type)
		}

		queries := append([]string{cast.ToString(c.Vals["queries..."])}, flaggy.TrailingArguments...)

		var totalAffected int64
		for i, query := range queries {

			query, err = sling.GetSQLText(query)
			if err != nil {
				return ok, g.Error(err, "cannot get query")
			}

			sQuery, err := database.ParseTableName(query, conn.Connection.Type)
			if err != nil {
				return ok, g.Error(err, "cannot parse query")
			}

			if len(database.ParseSQLMultiStatements(query)) == 1 && (!sQuery.IsQuery() || strings.Contains(query, "select") || g.In(conn.Connection.Type, dbio.TypeDbPrometheus, dbio.TypeDbMongoDB)) {

				data, err := dbConn.Query(sQuery.Select(100))
				if err != nil {
					return ok, g.Error(err, "cannot execute query")
				}

				if asJSON {
					fmt.Println(g.Marshal(g.M("fields", data.GetFields(), "rows", data.Rows)))
				} else {
					fmt.Println(g.PrettyTable(data.GetFields(), data.Rows))
				}

				totalAffected = cast.ToInt64(len(data.Rows))
			} else {
				if len(queries) > 1 {
					if strings.HasPrefix(query, "file://") {
						g.Info("executing query #%d (%s)", i+1, query)
					} else {
						g.Info("executing query #%d", i+1)
					}
				} else {
					g.Info("executing query")
				}

				result, err := dbConn.ExecMulti(query)
				if err != nil {
					return ok, g.Error(err, "cannot execute query")
				}

				affected, _ := result.RowsAffected()
				totalAffected = totalAffected + affected
			}
		}

		end := time.Now()
		if totalAffected > 0 {
			g.Info("successful! duration: %d seconds (%d affected records)", end.Unix()-start.Unix(), totalAffected)
		} else {
			g.Info("successful! duration: %d seconds.", end.Unix()-start.Unix())
		}

		if err := testRowCnt(totalAffected); err != nil {
			return ok, err
		}

	case "list":
		fields, rows := ec.List()
		if asJSON {
			fmt.Println(g.Marshal(g.M("fields", fields, "rows", rows)))
		} else {
			fmt.Println(g.PrettyTable(fields, rows))
		}

	case "test":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnTest)))
		name := cast.ToString(c.Vals["name"])
		if conn, ok := ec.GetConnEntry(name); ok {
			env.SetTelVal("conn_type", conn.Connection.Type.String())
			env.SetTelVal("conn_keys", lo.Keys(conn.Connection.Data))
		}

		ok, err = ec.Test(name)
		if err != nil {
			return ok, g.Error(err, "could not test %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		} else if ok {
			g.Info("success!") // successfully connected
		}
	case "discover":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnDiscover)))
		name := cast.ToString(c.Vals["name"])
		conn, ok := ec.GetConnEntry(name)
		if ok {
			env.SetTelVal("conn_type", conn.Connection.Type.String())
		}

		opt := &connection.DiscoverOptions{
			Pattern:     cast.ToString(c.Vals["pattern"]),
			ColumnLevel: cast.ToBool(c.Vals["columns"]),
			Recursive:   cast.ToBool(c.Vals["recursive"]),
		}

		files, schemata, err := ec.Discover(name, opt)
		if err != nil {
			return ok, g.Error(err, "could not discover %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}

		if tables := lo.Values(schemata.Tables()); len(tables) > 0 {

			sort.Slice(tables, func(i, j int) bool {
				val := func(t database.Table) string {
					return t.FDQN()
				}
				return val(tables[i]) < val(tables[j])
			})

			if opt.ColumnLevel {
				columns := iop.Columns(lo.Values(schemata.Columns()))
				if asJSON {
					fmt.Println(columns.JSON(true))
				} else {
					fmt.Println(columns.PrettyTable(true))
				}
			} else {
				fields := []string{"#", "Schema", "Name", "Type", "Columns"}
				rows := lo.Map(tables, func(table database.Table, i int) []any {
					tableType := lo.Ternary(table.IsView, "view", "table")
					if table.Dialect.DBNameUpperCase() {
						tableType = strings.ToUpper(tableType)
					}
					return []any{i + 1, table.Schema, table.Name, tableType, len(table.Columns)}
				})
				if asJSON {
					fmt.Println(g.Marshal(g.M("fields", fields, "rows", rows)))
				} else {
					fmt.Println(g.PrettyTable(fields, rows))
				}
			}
		} else if len(files) > 0 {
			if opt.ColumnLevel {
				columns := iop.Columns(lo.Values(files.Columns()))
				if asJSON {
					fmt.Println(columns.JSON(true))
				} else {
					fmt.Println(columns.PrettyTable(true))
				}
			} else {

				files.Sort()

				fields := []string{"#", "Name", "Type", "Size", "Last Updated (UTC)"}
				rows := lo.Map(files, func(file dbio.FileNode, i int) []any {
					fileType := lo.Ternary(file.IsDir, "directory", "file")

					lastUpdated := "-"
					if file.Updated > 100 {
						updated := time.Unix(file.Updated, 0)
						delta := strings.Split(g.DurationString(time.Since(updated)), " ")[0]
						lastUpdated = g.F("%s (%s ago)", updated.UTC().Format("2006-01-02 15:04:05"), delta)
					}

					size := "-"
					if !file.IsDir || file.Size > 0 {
						size = humanize.IBytes(file.Size)
					}

					return []any{i + 1, file.Path(), fileType, size, lastUpdated}
				})

				if asJSON {
					fmt.Println(g.Marshal(g.M("fields", fields, "rows", rows)))
				} else {
					fmt.Println(g.PrettyTable(fields, rows))
				}

				if len(files) > 0 && !(opt.Recursive || opt.Pattern != "") {
					g.Info("Those are non-recursive folder or file names (at the root level). Please use the --pattern flag to list sub-folders, or --recursive")
				}
			}
		} else {
			g.Info("no result.")
		}

	case "":
		return false, nil
	}
	return ok, nil
}
