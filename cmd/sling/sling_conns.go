package main

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

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

var (
	connsDiscover = func(*g.CliSC) error { return g.Error("please use the official build of Sling CLI to use this command") }
	connsCheck    = func(*g.CliSC) error { return g.Error("please use the official build of Sling CLI to use this command") }
)

func processConns(c *g.CliSC) (ok bool, err error) {
	ok = true

	if homeDir := cast.ToString(c.Vals["home-dir"]); homeDir != "" {
		os.Setenv("SLING_HOME_DIR", homeDir)
		env.LoadHomeDir()
	}

	ef := env.LoadSlingEnvFile()
	ec := connection.EnvFileConns{EnvFile: &ef}
	// resolved per-subcommand below; default falls back to SLING_OUTPUT
	var asJSON, asArrow, asCSV bool

	entries := connection.GetLocalConns(true)
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
		name := strings.ToUpper(cast.ToString(c.Vals["name"]))
		if name == "" {
			flaggy.ShowHelp("")
			return ok, nil
		}

		setOutput, outErr := ResolveOutputFormat(c, "json")
		if outErr != nil {
			return ok, outErr
		}
		asJSON = setOutput == "json"

		kvMap := map[string]any{}
		if cast.ToBool(c.Vals["stdin"]) {
			stat, _ := os.Stdin.Stat()
			if stat != nil && stat.Mode()&os.ModeCharDevice != 0 {
				return ok, g.Error("stdin is a terminal; pipe a YAML or JSON property map")
			}
			raw, readErr := io.ReadAll(os.Stdin)
			if readErr != nil {
				return ok, g.Error(readErr, "could not read stdin")
			}
			stdinMap, parseErr := connection.ParsePropsInput(string(raw))
			if parseErr != nil {
				return ok, parseErr
			}
			kvMap = stdinMap
		}

		kvArr := []string{cast.ToString(c.Vals["value properties..."])}
		for k, v := range g.KVArrToMap(append(kvArr, flaggy.TrailingArguments...)...) {
			k = strings.ToLower(k)
			if k == "" {
				continue
			}
			kvMap[k] = v
		}
		if t := strings.TrimSpace(cast.ToString(c.Vals["type"])); t != "" {
			kvMap["type"] = strings.ToLower(t)
		}

		if err = connection.RejectLiteralSecrets(name, kvMap); err != nil {
			return ok, err
		}

		err = ec.Set(name, kvMap)
		if err != nil {
			return ok, g.Error(err, "could not set %s (See https://docs.slingdata.io/sling-cli/environment)", name)
		}

		loc, locErr := ec.EnvFile.LookupConnection(name)
		if locErr != nil {
			loc = env.ConnLocation{Path: ec.EnvFile.Path, Connection: name, Missing: []env.MissingRef{}}
		}

		if asJSON {
			fmt.Println(g.Marshal(loc))
			return ok, nil
		}

		g.Info("connection `%s` has been set in %s:%d", name, loc.Path, loc.Line)
		if len(loc.Missing) > 0 {
			g.Info("set the env var(s), then: sling conns test %s", name)
		} else {
			g.Info("next: sling conns test %s", name)
		}
	case "exec":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnExec)))

		var output string
		output, err = ResolveOutputFormat(c, "json", "csv", "arrow")
		if err != nil {
			return ok, err
		}
		asJSON = output == "json"
		asCSV = output == "csv"
		asArrow = output == "arrow"

		// --limit: default 100, "0" means no limit. String type so we can
		// distinguish "not provided" (use default) from explicit 0.
		limit := uint64(100)
		if raw := strings.TrimSpace(cast.ToString(c.Vals["limit"])); raw != "" {
			n, parseErr := cast.ToUint64E(raw)
			if parseErr != nil {
				return ok, g.Error("invalid --limit %q; expected a non-negative integer", raw)
			}
			limit = n
		}
		queryOpts := g.M("limit", limit)

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

				// Limit handling:
				//  - limit > 0: wrap the SQL with the dialect's limit_sql template via
				//    sQuery.Select(...) so the database truncates server-side. This
				//    works for both bare tables and raw SELECT queries.
				//  - limit == 0: unlimited; for raw queries fall back to sQuery.Raw so
				//    we don't wrap with `LIMIT 0`.
				//  - procedural calls (stored procs, etc.) cannot be wrapped, so they
				//    always use sQuery.Raw and ignore the limit.
				var selectOpts database.SelectOptions
				if limit > 0 {
					n := int(limit)
					selectOpts.Limit = &n
				}
				sql := sQuery.Select(selectOpts)
				if sQuery.IsProcedural() || (limit == 0 && sQuery.IsQuery()) {
					sql = sQuery.Raw
				}

				if asArrow || asCSV {
					// Streaming path: pull rows from StreamRowsContext and write them
					// directly to stdout (Arrow IPC stream or CSV). Logs go to stderr
					// so the output stays clean. Memory stays bounded for large queries.
					ds, err := dbConn.Self().StreamRowsContext(dbConn.Context().Ctx, sql, queryOpts)
					if err != nil {
						return ok, g.Error(err, "cannot execute query")
					}
					if err := ds.WaitReady(); err != nil {
						return ok, g.Error(err, "datastream not ready")
					}

					var rowCount int64
					if asArrow {
						aw, err := iop.NewArrowStreamWriter(os.Stdout, ds.Columns)
						if err != nil {
							return ok, g.Error(err, "could not create arrow writer")
						}
						for row := range ds.Rows() {
							if err := aw.WriteRow(row); err != nil {
								aw.Close()
								return ok, g.Error(err, "could not write arrow row")
							}
							rowCount++
						}
						if err := ds.Err(); err != nil {
							aw.Close()
							return ok, g.Error(err, "error while streaming rows")
						}
						if err := aw.Close(); err != nil {
							return ok, g.Error(err, "could not close arrow writer")
						}
					} else { // asCSV
						w := csv.NewWriter(os.Stdout)
						if err := w.Write(ds.Columns.Names()); err != nil {
							return ok, g.Error(err, "could not write csv header")
						}
						rec := make([]string, len(ds.Columns))
						for row := range ds.Rows() {
							for i, val := range row {
								if i >= len(ds.Columns) {
									break
								}
								rec[i] = ds.Sp.CastToStringCSV(i, val, ds.Columns[i].Type)
							}
							if err := w.Write(rec); err != nil {
								return ok, g.Error(err, "could not write csv row")
							}
							rowCount++
						}
						w.Flush()
						if err := w.Error(); err != nil {
							return ok, g.Error(err, "csv writer error")
						}
						if err := ds.Err(); err != nil {
							return ok, g.Error(err, "error while streaming rows")
						}
					}
					totalAffected = rowCount
				} else {
					data, err := dbConn.Query(sql, queryOpts)
					if err != nil {
						return ok, g.Error(err, "cannot execute query")
					}

					// Render []byte cells. Drivers (especially MySQL/clickhouse)
					// return DECIMAL, DATE, VARCHAR, JSON etc. as []byte too —
					// for those we decode as text. Only true binary columns
					// (Oracle RAW/BLOB, Snowflake BINARY, etc.) get rendered
					// as 0x<hex>, replacing the Go slice notation `[222 173 ...]`.
					for ri, row := range data.Rows {
						for ci, val := range row {
							b, isBytes := val.([]byte)
							if !isBytes {
								continue
							}
							if ci < len(data.Columns) && data.Columns[ci].Type.IsBinary() {
								data.Rows[ri][ci] = "0x" + strings.ToUpper(hex.EncodeToString(b))
							} else {
								data.Rows[ri][ci] = string(b)
							}
						}
					}

					if asJSON {
						fmt.Println(g.Marshal(g.M("fields", data.GetFields(), "rows", data.Rows)))
					} else {
						fmt.Println(g.PrettyTable(data.GetFields(), data.Rows))
					}

					totalAffected = cast.ToInt64(len(data.Rows))
				}
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
		if os.Getenv("SLING_OUTPUT") == "json" {
			fmt.Println(g.Marshal(g.M("fields", fields, "rows", rows)))
		} else {
			fmt.Println(g.PrettyTable(fields, rows))
		}

	case "test":
		env.SetTelVal("task", g.Marshal(g.M("type", sling.ConnTest)))
		name := cast.ToString(c.Vals["name"])

		conn := entries.Get(name)
		if conn.Name != "" {
			env.SetTelVal("conn_type", conn.Connection.Type.String())
			env.SetTelVal("conn_keys", lo.Keys(conn.Connection.Data))
			if g.IsDebugLow() {
				g.Debug("connection %s properties: %s", name, g.Marshal(conn.Connection.Data))
			}
		}

		refData := conn.Connection.Data
		if len(refData) == 0 {
			if cdata, ok := ef.Connections[strings.ToUpper(name)]; ok {
				refData = cdata
			}
		}
		if refs := connection.FindUnsetEnvRefs(refData); len(refs) > 0 {
			loc, _ := ef.LookupConnection(strings.ToUpper(name))
			err = connection.FormatUnsetRefError(refs, loc)
			if os.Getenv("SLING_OUTPUT") == "json" {
				fmt.Println(g.Marshal(g.M("success", false, "error", g.ErrMsg(err))))
				return
			}
			return ok, err
		}

		// for testing specific endpoints
		if endpoints := cast.ToString(c.Vals["endpoints"]); endpoints != "" {
			os.Setenv("SLING_TEST_ENDPOINTS", endpoints)
		}

		ok, err = entries.Test(name)
		if err != nil {
			err = g.Error(err, "could not test %s", name)
		}

		if os.Getenv("SLING_OUTPUT") == "json" {
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

// ResolveOutputFormat resolves the output format for `conns` subcommands.
func ResolveOutputFormat(c *g.CliSC, allowed ...string) (string, error) {
	output := strings.ToLower(strings.TrimSpace(cast.ToString(c.Vals["output"])))
	if output == "" {
		output = strings.ToLower(strings.TrimSpace(os.Getenv("SLING_OUTPUT")))
	}
	if output == "" || output == "text" {
		return "", nil
	}
	for _, a := range allowed {
		if output == a {
			return output, nil
		}
	}
	return "", g.Error("invalid --output %q; expected one of: text, %s", output, strings.Join(allowed, ", "))
}
