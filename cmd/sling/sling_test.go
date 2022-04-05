package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/flarco/sling/core/env"

	"github.com/flarco/sling/core/sling"

	d "github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

type testDB struct {
	name     string
	URL      string
	table    string
	conn     d.Connection
	propStrs []string
}

var (
	testFile1Bytes []byte
)

var DBs = []*testDB{
	// {
	// 	// https://github.com/mattn/go-sqlite3
	// 	name:  "SQLite",
	// 	URL:   "file:./test.db",
	// 	table: "main.test1",
	// },

	{
		// https://github.com/lib/pq
		name:  "Postgres",
		URL:   "$POSTGRES_URL",
		table: "public.test1",
	},

	{
		// https://github.com/godror/godror
		name:  "Oracle",
		URL:   "$ORACLE_URL",
		table: "system.test1",
	},

	// {
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "MySQL",
	// 	URL:   "$MYSQL_URL",
	// 	table: "mysql.test1",
	// },

	// {
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "SQLServer",
	// 	URL:   "$MSSQL_URL",
	// 	table: "dbo.test1",
	// },

	// {
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "AzureSQL",
	// 	URL:   "$AZURESQL_URL",
	// 	table: "dbo.test1",
	// },

	// {
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "Snowflake",
	// 	URL:   "$SNOWFLAKE_URL",
	// 	table: "sling.test1",
	// },

	// {
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "BigQuery",
	// 	URL:   "$BIGQUERY_URL",
	// 	table: "public.test1",
	// },

	// {
	// 	// https://github.com/lib/pq
	// 	name:  "Redshift",
	// 	URL:   "$REDSHIFT_URL",
	// 	table: "public.test1",
	// },
}

func init() {
	env.InitLogger()
	iop.RemoveTrailingDecZeros = true
	sling.PermitTableSchemaOptimization = false
	os.Setenv("SLING_FILE_ROW_LIMIT", "0")
	for _, db := range DBs {
		if db.URL == "" {
			log.Fatal("No Env Var URL for " + db.name)
		} else if db.name == "SQLite" {
			os.Remove(strings.ReplaceAll(db.URL, "file:", ""))
		}
		if db.name == "Redshift" {
			os.Setenv("SLING_PARALLEL", "FALSE")
		}
	}
}

func TestOne(t *testing.T) {
	return
	cfg := sling.Config{}
	cfg.SetDefault()
	err := cfg.Unmarshal("/tmp/test.json")
	task := sling.NewTask(0, cfg)
	if g.AssertNoError(t, task.Err) {
		err = task.Execute()
		g.AssertNoError(t, err)
	}
}

func TestTasks(t *testing.T) {
	files, _ := g.ListDir("tests/tasks")
	for _, file := range files {
		cfg := sling.Config{}
		cfg.SetDefault()
		err := cfg.Unmarshal(file.FullPath)
		task := sling.NewTask(0, cfg)
		if g.AssertNoError(t, task.Err) {
			err = task.Execute()
			g.AssertNoError(t, err)
		}
	}
}

func TestInToDb(t *testing.T) {
	csvFile := "tests/files/test1.csv"
	csvFileUpsert := "tests/files/test1.upsert.csv"
	testFile1, err := os.Open(csvFile)
	if err != nil {
		g.LogError(err)
		g.AssertNoError(t, err)
		return
	}

	tReader, err := iop.AutoDecompress(bufio.NewReader(testFile1))
	g.AssertNoError(t, err)
	testFile1Bytes, err = ioutil.ReadAll(tReader)
	testFile1.Close()

	for _, tgtDB := range DBs {
		println()
		g.Debug(">>>>>> Tranferring from CSV(%s) to %s", csvFile, tgtDB.name)

		cfgMap := g.M(
			"source", g.M(
				"conn", "file://"+csvFile,
				"stream", "file://"+csvFile,
			),
			"target", g.M(
				"conn", tgtDB.URL,
				"object", tgtDB.table,
				"mode", sling.DropMode,
			),
		)
		config, err := sling.NewConfig(g.Marshal(cfgMap))
		g.AssertNoError(t, err)

		task := sling.NewTask(0, config)
		err = task.Execute()
		if err != nil {
			g.LogError(err)
			g.AssertNoError(t, err)
			return
		}

		cfgMap = g.M(
			"source", g.M(
				"conn", "file://"+csvFileUpsert,
				"stream", "file://"+csvFileUpsert,
			),
			"target", g.M(
				"conn", tgtDB.URL,
				"object", tgtDB.table+"_upsert",
				"mode", sling.TruncateMode,
			),
		)
		config, err = sling.NewConfig(g.Marshal(cfgMap))
		g.AssertNoError(t, err)

		taskUpsert := sling.NewTask(0, config)
		err = taskUpsert.Execute()
		g.LogError(err)
		if err != nil {
			g.AssertNoError(t, err)
			return
		}
	}
}

func TestDbToDb(t *testing.T) {
	for _, srcDB := range DBs {
		for _, tgtDB := range DBs {
			if srcDB.name == "SQLite" && tgtDB.name == "SQLite" {
				continue
			} else if srcDB.name == "Postgres" || tgtDB.name == "Postgres" {
				// OK
			} else {
				continue
			}

			println()
			g.Debug(">>>>>> Tranferring from %s to %s", srcDB.name, tgtDB.name)

			cfgMap := g.M(
				"source", g.M(
					"conn", srcDB.URL,
					"stream", srcDB.table,
				),
				"target", g.M(
					"conn", tgtDB.URL,
					"object", tgtDB.table+"_copy",
					"mode", sling.DropMode,
				),
			)
			config, err := sling.NewConfig(g.Marshal(cfgMap))
			g.AssertNoError(t, err)

			task := sling.NewTask(0, config)
			err = task.Execute()
			if g.LogError(err) {
				g.AssertNoError(t, err)
				return
			}

			cfgMap = g.M(
				"source", g.M(
					"conn", srcDB.URL,
					"stream", srcDB.table+"_upsert",
				),
				"target", g.M(
					"conn", tgtDB.URL,
					"object", tgtDB.table+"_copy",
					"primary_key", []string{"id"},
					"update_key", "create_dt",
					"mode", sling.UpsertMode,
				),
			)
			config, err = sling.NewConfig(g.Marshal(cfgMap))
			g.AssertNoError(t, err)

			taskUpsert := sling.NewTask(0, config)
			err = taskUpsert.Execute()
			if err != nil {
				g.AssertNoError(t, err)
				return
			}
		}
	}
}

func TestDbToOut(t *testing.T) {

	for _, srcDB := range DBs {
		filePath2 := fmt.Sprintf("tests/files/%s.out.csv", srcDB.name)
		println()
		g.Debug(">>>>>> Tranferring from %s to CSV (%s)", srcDB.name, filePath2)

		srcTable := srcDB.table
		srcTableCopy := srcDB.table + "_copy"

		cfgMap := g.M(
			"source", g.M(
				"conn", srcDB.URL,
				"stream", g.F("select * from %s order by id", srcTableCopy),
			),
			"target", g.M(
				"object", "file://"+filePath2,
			),
		)
		config, err := sling.NewConfig(g.Marshal(cfgMap))
		g.AssertNoError(t, err)

		task := sling.NewTask(0, config)
		err = task.Execute()
		if !g.AssertNoError(t, err) {
			g.LogError(err)
			return
		}

		testFile1, err := os.Open("tests/files/test1.result.csv")
		g.AssertNoError(t, err)
		testFile1Bytes, err = ioutil.ReadAll(testFile1)

		testFile2, err := os.Open(filePath2)
		g.AssertNoError(t, err)
		testFile2Bytes, err := ioutil.ReadAll(testFile2)

		if srcDB.name != "SQLite" {
			// SQLite uses int for bool, so it will not match
			equal := assert.Equal(t, string(testFile1Bytes), string(testFile2Bytes))

			if equal {
				err = os.RemoveAll(filePath2)
				g.AssertNoError(t, err)

				conn, err := d.NewConn(srcDB.URL)
				g.AssertNoError(t, err)

				err = conn.Connect()
				g.AssertNoError(t, err)

				err = conn.DropTable(srcTable)
				g.AssertNoError(t, err)

				err = conn.DropTable(srcTableCopy)
				g.AssertNoError(t, err)

				err = conn.Close()
				g.AssertNoError(t, err)
			}
		} else {
			testFile1Lines := len(strings.Split(string(testFile1Bytes), "\n"))
			testFile2Lines := len(strings.Split(string(testFile2Bytes), "\n"))
			equal := assert.Equal(t, testFile1Lines, testFile2Lines)

			if equal {
				err = os.RemoveAll(filePath2)
				os.Remove(strings.ReplaceAll(srcDB.URL, "file:", ""))
			} else {
				g.Debug("Not equal for " + srcDB.name)
			}
		}
	}
}

func TestCfgPath(t *testing.T) {

	testCfg := func(path string) (err error) {
		cfg, err := sling.NewConfig(path)
		if g.LogError(err) {
			g.AssertNoError(t, err)
			return
		}

		assert.EqualValues(t, "testing", cfg.SrcConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Source.Stream)
		assert.EqualValues(t, "testing", cfg.SrcConn.URL())
		assert.EqualValues(t, "testing", cfg.TgtConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Target.Object)
		assert.EqualValues(t, "testing", cfg.TgtConn.URL())
		assert.EqualValues(t, "testing", cfg.Target.Mode)
		assert.EqualValues(t, 111, cfg.Source.Limit)
		assert.EqualValues(t, "testing", cfg.Target.Options.TableDDL)
		assert.EqualValues(t, "testing", cfg.Target.Options.TableTmp)
		assert.EqualValues(t, "testing", cfg.Target.Options.PostSQL)

		return err
	}

	err := testCfg("tests/test1.yaml")
	g.AssertNoError(t, err)

	err = testCfg("tests/test1.json")
	g.AssertNoError(t, err)
}

func testTask(t *testing.T) {
	config := sling.Config{}
	config.SetDefault()
	config.Source.Conn = "s3://ocral/rudderstack/rudder-logs/1uXKxCrhN2WGAt2fojy6k2fqDSb/06-27-2021"
	// config.Source.Conn = "s3://ocral/rudderstack/rudder-logs/1uXKxCrhN2WGAt2fojy6k2fqDSb/06-27-2021/1624811693.1uXKxCrhN2WGAt2fojy6k2fqDSb.9939afc4-a80f-4f3e-921e-fc24e8e7ff43.json.gz"
	// config.Source.Conn = "file:///tmp/csvTest/"
	// config.Source.Conn = "file:///tmp/csvTest/part2.csv.gz"
	config.Target.Conn = "PG_BIONIC_URL"
	config.Target.Object = "public.sling_cli_events"
	config.Target.Mode = sling.DropMode
	config.Target.Options.UseBulk = true
	err := config.Prepare()
	if g.AssertNoError(t, err) {

		task := sling.NewTask(0, config)
		g.AssertNoError(t, task.Err)

		// run task
		err = task.Execute()
		g.AssertNoError(t, err)
	}
}
