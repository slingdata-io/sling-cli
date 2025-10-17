package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/integrii/flaggy"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
)

var (
	connsDiscover = func(*g.CliSC) error { return g.Error("please use the official build of Sling CLI to use this command") }
	connsCheck    = func(*g.CliSC) error { return g.Error("please use the official build of Sling CLI to use this command") }
)

func processConns(c *g.CliSC) (ok bool, err error) {
	ok = true

	ef := env.LoadSlingEnvFile()
	ec := connection.EnvFileConns{EnvFile: &ef}
	asJSON := os.Getenv("SLING_OUTPUT") == "json"

	entries := connection.GetLocalConns()
	defer connection.CloseAll()

	env.SetTelVal("task_start_time", time.Now())
	defer func() {
		env.SetTelVal("task_status", lo.Ternary(err != nil, "error", "success"))
		env.SetTelVal("task_end_time", time.Now())
	}()

	if cast.ToBool(c.Vals["trace"]) {
		os.Setenv("DEBUG", "TRACE")
		env.InitLogger()
	} else if cast.ToBool(c.Vals["debug"]) {
		os.Setenv("DEBUG", "LOW")
		env.InitLogger()
	}

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
		conn := entries.Get(name)
		if conn.Name == "" {
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

			if len(database.ParseSQLMultiStatements(query)) == 1 && (!sQuery.IsQuery() || (strings.Contains(strings.ToLower(query), "select") && !strings.Contains(strings.ToLower(query), "insert")) || g.In(conn.Connection.Type, dbio.TypeDbPrometheus, dbio.TypeDbMongoDB, dbio.TypeDbElasticsearch)) {

				sql := sQuery.Select(database.SelectOptions{Limit: g.Ptr(100)})
				if sQuery.IsQuery() || sQuery.IsProcedural() {
					sql = sQuery.Raw
				}
				data, err := dbConn.Query(sql)
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

		if err := testOutput(totalAffected, 0, 0); err != nil {
			return ok, err
		}

	case "list":
		fields, rows := entries.List()
		if asJSON {
			fmt.Println(g.Marshal(g.M("fields", fields, "rows", rows)))
		} else {
			fmt.Println(g.PrettyTable(fields, rows))
		}

	case "test":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnTest)))
		name := cast.ToString(c.Vals["name"])

		if conn := entries.Get(name); conn.Name != "" {
			env.SetTelVal("conn_type", conn.Connection.Type.String())
			env.SetTelVal("conn_keys", lo.Keys(conn.Connection.Data))
		}

		// for testing specific endpoints
		if endpoints := cast.ToString(c.Vals["endpoints"]); endpoints != "" {
			os.Setenv("SLING_TEST_ENDPOINTS", endpoints)
		}

		ok, err = entries.Test(name)
		if err != nil {
			err = g.Error(err, "could not test %s", name)
		}

		if asJSON {
			fmt.Println(g.Marshal(g.M("success", err == nil, "error", g.ErrMsg(err))))
			return
		}

		if err != nil {
			return ok, err
		} else if ok {
			g.Info("success!") // successfully connected
		}
	case "discover":
		return ok, connsDiscover(c)

	case "check":
		return ok, connsCheck(c)

	case "":
		return false, nil
	}
	return ok, nil
}
