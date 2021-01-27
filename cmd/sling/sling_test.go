package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/slingdata-io/sling/core/dbt"
	"github.com/slingdata-io/sling/core/env"

	"github.com/slingdata-io/sling/core/elt"

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
	// &testDB{
	// 	// https://github.com/mattn/go-sqlite3
	// 	name:  "SQLite",
	// 	URL:   "file:./test.db",
	// 	table: "main.test1",
	// },

	&testDB{
		// https://github.com/lib/pq
		name:  "Postgres",
		URL:   "$POSTGRES_URL",
		table: "public.test1",
	},

	&testDB{
		// https://github.com/godror/godror
		name:  "Oracle",
		URL:   "$ORACLE_URL",
		table: "system.test1",
	},

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "MySQL",
	// 	URL:   "$MYSQL_URL",
	// 	table: "mysql.test1",
	// },

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "SQLServer",
	// 	URL:   "$MSSQL_URL",
	// 	table: "dbo.test1",
	// },

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "AzureSQL",
	// 	URL:   "$AZURESQL_URL",
	// 	table: "dbo.test1",
	// },

	// &testDB{
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "Snowflake",
	// 	URL:   "$SNOWFLAKE_URL",
	// 	table: "sling.test1",
	// },

	// &testDB{
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "BigQuery",
	// 	URL:   "$BIGQUERY_URL",
	// 	table: "public.test1",
	// },

	// &testDB{
	// 	// https://github.com/lib/pq
	// 	name:  "Redshift",
	// 	URL:   "$REDSHIFT_URL",
	// 	table: "public.test1",
	// },
}

func init() {
	env.InitLogger()
	iop.RemoveTrailingDecZeros = true
	elt.PermitTableSchemaOptimization = false
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

func TestInToDb(t *testing.T) {
	csvFile := "tests/test1.csv"
	csvFileUpsert := "tests/test1.upsert.csv"
	testFile1, err := os.Open(csvFile)
	if err != nil {
		assert.NoError(t, err)
		return
	}

	tReader, err := iop.Decompress(bufio.NewReader(testFile1))
	assert.NoError(t, err)
	testFile1Bytes, err = ioutil.ReadAll(tReader)
	testFile1.Close()

	for _, tgtDB := range DBs {
		println()
		g.Debug(">>>>>> Tranferring from CSV(%s) to %s", csvFile, tgtDB.name)

		config := elt.Config{}
		config.Source.URL = csvFile
		config.Target.URL = tgtDB.URL
		config.Target.Table = tgtDB.table
		config.Target.Mode = elt.DropMode
		task := elt.NewTask(0, config)
		err = task.Execute()
		if err != nil {
			assert.NoError(t, err)
			return
		}

		config = elt.Config{}
		config.Source.URL = csvFileUpsert
		config.Target.URL = tgtDB.URL
		config.Target.Table = tgtDB.table + "_upsert"
		config.Target.Mode = elt.TruncateMode
		taskUpsert := elt.NewTask(0, config)
		err = taskUpsert.Execute()
		g.LogError(err)
		if err != nil {
			assert.NoError(t, err)
			return
		}
	}
}

func TestDbToDb(t *testing.T) {
	var err error
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
			config := elt.Config{}
			config.Source.URL = srcDB.URL
			config.Source.Table = srcDB.table
			config.Target.URL = tgtDB.URL
			config.Target.Table = tgtDB.table + "_copy"
			config.Target.Mode = elt.DropMode
			task := elt.NewTask(0, config)
			err = task.Execute()
			if g.LogError(err) {
				assert.NoError(t, err)
				return
			}

			config = elt.Config{}
			config.Source.URL = srcDB.URL
			config.Source.Table = srcDB.table + "_upsert"
			config.Target.URL = tgtDB.URL
			config.Target.Table = tgtDB.table + "_copy"
			config.Target.Mode = elt.UpsertMode
			config.Target.UpdateKey = "create_dt"
			config.Target.PrimaryKey = []string{"id"}
			taskUpsert := elt.NewTask(0, config)
			err = taskUpsert.Execute()
			if err != nil {
				assert.NoError(t, err)
				return
			}
		}
	}
}

func TestDbToOut(t *testing.T) {

	for _, srcDB := range DBs {
		filePath2 := fmt.Sprintf("tests/%s.out.csv", srcDB.name)
		println()
		g.Debug(">>>>>> Tranferring from %s to CSV (%s)", srcDB.name, filePath2)

		srcTable := srcDB.table
		srcTableCopy := srcDB.table + "_copy"

		config := elt.Config{}
		config.Source.URL = srcDB.URL
		config.Source.SQL = g.F("select * from %s order by id", srcTableCopy)
		config.Target.URL = filePath2

		task := elt.NewTask(0, config)
		err := task.Execute()
		if !assert.NoError(t, err) {
			return
		}

		testFile1, err := os.Open("tests/test1.result.csv")
		assert.NoError(t, err)
		testFile1Bytes, err = ioutil.ReadAll(testFile1)

		testFile2, err := os.Open(filePath2)
		assert.NoError(t, err)
		testFile2Bytes, err := ioutil.ReadAll(testFile2)

		if srcDB.name != "SQLite" {
			// SQLite uses int for bool, so it will not match
			equal := assert.Equal(t, string(testFile1Bytes), string(testFile2Bytes))

			if equal {
				err = os.RemoveAll(filePath2)
				assert.NoError(t, err)

				conn, err := d.NewConn(srcDB.URL)
				assert.NoError(t, err)

				err = conn.Connect()
				assert.NoError(t, err)

				err = conn.DropTable(srcTable)
				assert.NoError(t, err)

				err = conn.DropTable(srcTableCopy)
				assert.NoError(t, err)

				err = conn.Close()
				assert.NoError(t, err)
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

func TestDbt(t *testing.T) {

	for _, db := range DBs {
		schema, _ := d.SplitTableFullName(db.table)
		switch db.name {
		case "Postgres", "Snowflake", "BigQuery":
			println()
			g.Debug(">>>>>> DBT (%s)", db.name)

			dbtConfig := &dbt.Dbt{
				Version: "0.18",
				Profile: db.URL,
				RepoURL: "https://github.com/fishtown-analytics/dbt-starter-project",
				Schema:  schema,
				Models:  "+my_second_dbt_model",
			}

			config := elt.Config{}
			config.Target.URL = db.URL
			config.Target.Dbt = dbtConfig
			task := elt.NewTask(0, config)
			err := task.Execute()
			if !assert.NoError(t, err) {
				g.LogError(err)
				return
			}
		default:
			continue
		}
	}

}

func TestCfgPath(t *testing.T) {

	testCfg := func(path string) (err error) {
		cfg, err := elt.NewConfig(path)
		if g.LogError(err) {
			assert.NoError(t, err)
			return
		}

		assert.EqualValues(t, "testing", cfg.SrcConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Source.Table)
		assert.EqualValues(t, "testing", cfg.SrcConn.URL())
		assert.EqualValues(t, "testing", cfg.Source.SQL)
		assert.EqualValues(t, "testing", cfg.TgtConn.Info().Name)
		assert.EqualValues(t, "testing", cfg.Target.Table)
		assert.EqualValues(t, "testing", cfg.TgtConn.URL())
		assert.EqualValues(t, "testing", cfg.Target.Mode)
		assert.EqualValues(t, 111, cfg.Source.Limit)
		assert.EqualValues(t, "testing", cfg.Target.TableDDL)
		assert.EqualValues(t, "testing", cfg.Target.TableTmp)
		assert.EqualValues(t, "testing", cfg.Target.PostSQL)

		return err
	}

	err := testCfg("tests/test1.yaml")
	assert.NoError(t, err)

	err = testCfg("tests/test1.json")
	assert.NoError(t, err)
}

func testTask(t *testing.T) {
	config := elt.Config{}
	config.Source.URL = "SNOWFLAKE_URL"
	config.Source.Table = "public.test5"
	config.Target.URL = "POSTGRES_URL"
	config.Target.Table = "public.test5"
	config.Target.Mode = elt.DropMode
	task := elt.NewTask(0, config)
	assert.NoError(t, task.Err)

	// run task
	err := task.Execute()
	assert.NoError(t, err)
}
