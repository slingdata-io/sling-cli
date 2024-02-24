package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"

	"github.com/slingdata-io/sling-cli/core/sling"

	"github.com/flarco/g"
	d "github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

type testDB struct {
	name string
	conn d.Connection
}

type testConn struct {
	name   string
	schema string
	noBulk bool
}

var dbConnMap = map[dbio.Type]testConn{
	dbio.TypeDbPostgres:   {name: "postgres"},
	dbio.TypeDbRedshift:   {name: "redshift"},
	dbio.TypeDbStarRocks:  {name: "starrocks", noBulk: true},
	dbio.TypeDbMySQL:      {name: "mysql", schema: "mysql"},
	dbio.TypeDbMariaDB:    {name: "mariadb", schema: "mariadb"},
	dbio.TypeDbOracle:     {name: "oracle", schema: "system"},
	dbio.TypeDbBigTable:   {name: "bigtable"},
	dbio.TypeDbBigQuery:   {name: "bigquery"},
	dbio.TypeDbSnowflake:  {name: "snowflake"},
	dbio.TypeDbSQLite:     {name: "sqlite"},
	dbio.TypeDbDuckDb:     {name: "duckdb"},
	dbio.TypeDbMotherDuck: {name: "motherduck"},
	dbio.TypeDbSQLServer:  {name: "sqlserver"},
	dbio.TypeDbAzure:      {name: "azuresql"},
	dbio.TypeDbAzureDWH:   {name: "azuredwh"},
	dbio.TypeDbClickhouse: {name: "clickhouse"},
}

func init() {
	env.InitLogger()
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
		// if g.In(i+1, 12) {
		// 	continue // broken ssh connection, skip oracle
		// }
		// if i+1 < 13 {
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

	if doDelete := task.Config.Env["delete_duck_db"]; cast.ToBool(doDelete) {
		os.Remove(strings.TrimPrefix(task.Config.TgtConn.URL(), "duckdb://"))
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
		g.Debug("getting count for test validation")
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
		orderByStr := "1"
		if len(task.Config.Source.PrimaryKey()) > 0 {
			orderByStr = strings.Join(task.Config.Source.PrimaryKey(), ", ")
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
						if !assert.EqualValues(t, valFile, valDb, g.F("row %d, col %d (%s), in test %s => %#v vs %#v", i+1, valCol, dataDB.Columns[valCol].Name, file.Name, valDb, valFile)) {
							return
						}
					}
				}
			}
		}
	}
}

func TestOptions(t *testing.T) {
	testCases := []struct {
		input       string
		shouldError bool
		expected    map[string]any
	}{
		{
			input:       ``,
			shouldError: false,
		},
		{
			input:       `.`,
			shouldError: true,
		},
		{
			input:    `{"use_bulk":false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `{"use_bulk": false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `{use_bulk: false}`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:    `use_bulk: false`,
			expected: map[string]any{"use_bulk": false},
		},
		{
			input:       `{use_bulk:false}`,
			shouldError: true,
		},
		{
			input:       `use_bulk:false`,
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		options, err := parsePayload(testCase.input, true)
		msg := g.F("with input: `%s`", testCase.input)
		if testCase.shouldError {
			assert.Error(t, err, msg)
		} else {
			assert.NoError(t, err, msg)
			if testCase.expected != nil {
				assert.Equal(t, testCase.expected, options, msg)
			}
		}
	}
}

func TestCfgPath(t *testing.T) {

	testCfg := func(path string) (err error) {
		cfg, err := sling.NewConfig(path)
		if !g.AssertNoError(t, err) {
			return
		}

		assert.EqualValues(t, "testing", cfg.SrcConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Source.Stream)
		assert.EqualValues(t, "testing", cfg.SrcConn.URL())
		assert.EqualValues(t, "testing", cfg.TgtConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Target.Object)
		assert.EqualValues(t, "testing", cfg.TgtConn.URL())
		assert.EqualValues(t, "testing", cfg.Mode)
		assert.EqualValues(t, 111, cfg.Source.Limit())
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

func Test1Task(t *testing.T) {
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

func Test1Replication(t *testing.T) {
	sling.ShowProgress = false
	os.Setenv("DEBUG", "LOW")
	os.Setenv("SLING_CLI", "TRUE")
	os.Setenv("SLING_LOADED_AT_COLUMN", "TRUE")
	os.Setenv("CONCURENCY_LIMIT", "2")
	replicationCfgPath := g.UserHomeDir() + "/Downloads/mlops.slack.replication.local.yml"
	err := runReplication(replicationCfgPath)
	if g.AssertNoError(t, err) {
		return
	}
}

func TestExtract(t *testing.T) {
	core.Version = "v1.0.43"

	checkUpdate()
	assert.NotEmpty(t, updateVersion)

	printUpdateAvailable()

	err := ExtractTarGz(g.UserHomeDir()+"/Downloads/sling/sling_1.0.44_darwin_all.tar.gz", g.UserHomeDir()+"/Downloads/sling")
	g.AssertNoError(t, err)
}

func testSuite(dbType dbio.Type, t *testing.T) {
	conn, ok := dbConnMap[dbType]
	if !assert.True(t, ok) {
		return
	}

	templateFilePath := "tests/suite.template.tsv"
	tempFilePath := g.F("/tmp/tests.%s.tsv", dbType.String())
	folderPath := g.F("tests/suite/%s", dbType.String())
	testSchema := lo.Ternary(conn.schema == "", "sling_test", conn.schema)
	testTable := g.F("%s_test1k", dbType.String())

	// generate files
	os.RemoveAll(folderPath) // clean up
	os.MkdirAll(folderPath, 0777)

	// replace vars
	filebytes, err := os.ReadFile(templateFilePath)
	g.LogFatal(err)

	fileContent := string(filebytes)
	fileContent = strings.ReplaceAll(fileContent, "[conn]", conn.name)
	fileContent = strings.ReplaceAll(fileContent, "[schema]", testSchema)
	fileContent = strings.ReplaceAll(fileContent, "[table]", testTable)

	err = os.WriteFile(tempFilePath, []byte(fileContent), 0777)
	g.LogFatal(err)

	data, err := iop.ReadCsv(tempFilePath)
	if !g.AssertNoError(t, err) {
		return
	}

	for i, rec := range data.Records() {
		options, _ := g.UnmarshalMap(cast.ToString(rec["options"]))
		sourceOptions, _ := g.UnmarshalMap(cast.ToString(rec["source_options"]))
		targetOptions, _ := g.UnmarshalMap(cast.ToString(rec["target_options"]))
		env, _ := g.UnmarshalMap(cast.ToString(rec["env"]))
		primaryKey := []string{}
		if val := cast.ToString(rec["source_primary_key"]); val != "" {
			primaryKey = strings.Split(val, ",")
		}

		if conn.noBulk {
			targetOptions["use_bulk"] = false
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
		taskPath := filepath.Join(folderPath, g.F("%02d.%s.json", i+1, rec["test_name"]))
		taskBytes := []byte(g.Pretty(task))
		err = os.WriteFile(taskPath, taskBytes, 0777)
		g.AssertNoError(t, err)
	}

	time.Sleep(500 * time.Millisecond)
	files, _ := g.ListDir(folderPath)
	for _, file := range files {
		if t.Failed() {
			break
		}
		runOneTask(t, file)
	}
}

func TestSuitePostgres(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbPostgres, t)
}

// func TestSuiteRedshift(t *testing.T) {
// 	t.Parallel()
// 	testSeries(dbio.TypeDbRedshift, t)
// }

func TestSuiteStarRocks(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbStarRocks, t)
}

func TestSuiteMySQL(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbMySQL, t)
}

func TestSuiteMariaDB(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbMariaDB, t)
}

func TestSuiteOracle(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbOracle, t)
}

// func TestSuiteBigTable(t *testing.T) {
// 	t.Parallel()
// 	testSeries(dbio.TypeDbBigTable, t)
// }

func TestSuiteBigQuery(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbBigQuery, t)
}

func TestSuiteSnowflake(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbSnowflake, t)
}

func TestSuiteSQLite(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbSQLite, t)
}

func TestSuiteDuckDb(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbDuckDb, t)
}

func TestSuiteMotherDuck(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbMotherDuck, t)
}

func TestSuiteSQLServer(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbSQLServer, t)
}

// func TestSuiteAzure(t *testing.T) {
// 	t.Parallel()
// 	testSeries(dbio.TypeDbAzure, t)
// }

// func TestSuiteAzureDWH(t *testing.T) {
// 	t.Parallel()
// 	testSeries(dbio.TypeDbAzureDWH, t)
// }

func TestSuiteClickhouse(t *testing.T) {
	t.Parallel()
	testSuite(dbio.TypeDbClickhouse, t)
}
