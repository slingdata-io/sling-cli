package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"

	"github.com/slingdata-io/sling-cli/core/sling"

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
	// return
	path := "tests/tasks/task.06.json"
	pathArr := strings.Split(path, "/")
	file := g.FileItem{FullPath: path, RelPath: path, Name: pathArr[len(pathArr)-1]}
	runOneTask(t, file)
	if t.Failed() {
		return
	}

	// path = "/tmp/temp.json"
	// file = g.FileItem{FullPath: path, RelPath: path}
	// runOneTask(t, file)
}

func TestTasks(t *testing.T) {
	defFilePath := "tests/tasks.tsv"
	folderPath := "tests/tasks"

	// generate files
	os.RemoveAll(folderPath) // clean up
	os.MkdirAll(folderPath, 0777)

	data, err := iop.ReadCsv(defFilePath)
	if !g.AssertNoError(t, err) {
		return
	}

	for i, rec := range data.Records() {
		if g.In(rec["source_conn"], "GITHUB_DBIO", "NOTION") {
			g.Warn("skipping since source_conn is %s", rec["source_conn"])
			continue
		}

		options, _ := g.UnmarshalMap(cast.ToString(rec["options"]))
		sourceOptions, _ := g.UnmarshalMap(cast.ToString(rec["source_options"]))
		targetOptions, _ := g.UnmarshalMap(cast.ToString(rec["target_options"]))
		env, _ := g.UnmarshalMap(cast.ToString(rec["env"]))
		primaryKey := []string{}
		if val := cast.ToString(rec["source_primary_key"]); val != "" {
			primaryKey = strings.Split(val, ",")
		}

		task := g.M(
			"source", g.M(
				"conn", cast.ToString(rec["source_conn"]),
				"stream", cast.ToString(rec["source_stream"]),
				"primary_key", primaryKey,
				"update_key", cast.ToString(rec["source_update_key"]),
				"options", sourceOptions,
			),
			"target", g.M(
				"conn", cast.ToString(rec["target_conn"]),
				"object", cast.ToString(rec["target_object"]),
				"options", targetOptions,
			),
			"mode", cast.ToString(rec["mode"]),
			"options", options,
			"env", env,
		)
		taskPath := filepath.Join(folderPath, g.F("task.%02d.json", i+1))
		taskBytes := []byte(g.Marshal(task))
		err = os.WriteFile(taskPath, taskBytes, 0777)
		g.AssertNoError(t, err)
	}

	files, _ := g.ListDir(folderPath)
	for i, file := range files {
		_ = i
		// if !g.In(i+1, 24, 25) {
		// 	continue
		// }
		// if i+1 < 14 {
		// 	continue
		// }
		runOneTask(t, file)

		if t.Failed() {
			break
		}
	}
}

func runOneTask(t *testing.T, file g.FileItem) {
	os.Setenv("ERROR_ON_CHECKSUM_FAILURE", "1") // so that it errors when checksums don't match
	println()

	bars := "---------------------------"
	g.Info("%s Testing %s %s", bars, file.RelPath, bars)
	cfg := &sling.Config{}
	err := cfg.Unmarshal(file.FullPath)
	if !g.AssertNoError(t, err) {
		return
	}

	task := sling.NewTask(0, cfg)
	if !g.AssertNoError(t, task.Err) {
		return
	}

	// validate object name
	if name, ok := task.Config.Env["validation_object"]; ok {
		if !assert.Equal(t, name, task.Config.Target.Object) {
			return
		}
	}

	// g.PP(task)
	if g.AssertNoError(t, task.Err) {
		err = task.Execute()
		if !g.AssertNoError(t, err) {
			return
		}
	} else {
		return
	}

	// validate count
	if task.Config.Mode == sling.FullRefreshMode && task.Config.TgtConn.Type.IsDb() {
		g.Debug("getting count")
		conn, err := task.Config.TgtConn.AsDatabase()
		if g.AssertNoError(t, err) {
			count, err := conn.GetCount(task.Config.Target.Object)
			g.AssertNoError(t, err)
			assert.EqualValues(t, task.GetCount(), count)
			conn.Close()
		}
	}

	// validate file
	if val, ok := task.Config.Env["validation_file"]; ok {
		valFile := strings.TrimPrefix(val, "file://")
		dataFile, err := iop.ReadCsv(valFile)
		if !g.AssertNoError(t, err) {
			return
		}
		orderByStr := ""
		if len(task.Config.Source.PrimaryKey) > 0 {
			orderByStr = strings.Join(task.Config.Source.PrimaryKey, ", ")
		}
		sql := g.F("select * from %s order by %s", task.Config.Target.Object, orderByStr)
		conn, _ := task.Config.TgtConn.AsDatabase()
		dataDB, err := conn.Query(sql)
		g.AssertNoError(t, err)
		conn.Close()
		valCols := strings.Split(task.Config.Env["validation_cols"], ",")

		if g.AssertNoError(t, err) {
			for _, valColS := range valCols {
				valCol := cast.ToInt(valColS)
				valuesFile := dataFile.ColValues(valCol)
				valuesDb := dataDB.ColValues(valCol)
				// g.P(dataDB.ColValues(0))
				// g.P(dataDB.ColValues(1))
				// g.P(valuesDb)
				if assert.Equal(t, len(valuesFile), len(valuesDb)) {
					for i := range valuesDb {
						valDb := dataDB.Sp.ParseString(cast.ToString(valuesDb[i]))
						valFile := dataDB.Sp.ParseString(cast.ToString(valuesFile[i]))
						if !assert.EqualValues(t, valFile, valDb, g.F("row %d, col %d (%s), in test %s", i+1, valCol, dataDB.Columns[valCol].Name, file.Name)) {
							return
						}
					}
				}
			}
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
			),
			"mode", sling.FullRefreshMode,
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
				"object", tgtDB.table+"_incremental",
			),
			"mode", sling.TruncateMode,
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
				),
				"mode", sling.FullRefreshMode,
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
					"stream", srcDB.table+"_incremental",
				),
				"target", g.M(
					"conn", tgtDB.URL,
					"object", tgtDB.table+"_copy",
					"primary_key", []string{"id"},
					"update_key", "create_dt",
				),
				"mode", sling.IncrementalMode,
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
		assert.EqualValues(t, "testing", cfg.Mode)
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

func testOneTask(t *testing.T) {
	os.Setenv("SLING_CLI", "TRUE")
	config := &sling.Config{}
	cfgStr := `
source:
  conn: do_spaces
  stream: 's3://ocral/rudderstack/rudder-logs/1uXKxCrhN2WGAt2fojy6k2fqDSb/02-17-2022'
  options:
    flatten: true
options:
  stdout: true`
	err := config.Unmarshal(cfgStr)
	if g.AssertNoError(t, err) {
		err = config.Prepare()
		if g.AssertNoError(t, err) {

			task := sling.NewTask(0, config)
			g.AssertNoError(t, task.Err)

			// run task
			err = task.Execute()
			g.AssertNoError(t, err)
		}
	}
}

func TestOneReplication(t *testing.T) {
	sling.ShowProgress = false
	os.Setenv("_DEBUG", "LOW")
	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_LOADED_AT_COLUMN", "TRUE")
	os.Setenv("CONCURENCY_LIMIT", "2")
	replicationCfgPath := "/Users/fritz/Downloads/mlops.slack.replication.local.yml"
	err := runReplication(replicationCfgPath)
	if g.AssertNoError(t, err) {
		return
	}
}
