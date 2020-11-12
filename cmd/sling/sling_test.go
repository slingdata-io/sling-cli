package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"

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
		URL:   "POSTGRES_URL",
		table: "public.test1",
	},

	&testDB{
		// https://github.com/godror/godror
		name:  "Oracle",
		URL:   "ORACLE_URL",
		table: "system.test1",
	},

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "MySQL",
	// 	URL:   "MYSQL_URL",
	// 	table: "mysql.test1",
	// },

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "SQLServer",
	// 	URL:   "MSSQL_URL",
	// 	table: "dbo.test1",
	// },

	// &testDB{
	// 	// https://github.com/denisenkom/go-mssqldb
	// 	name:  "AzureSQL",
	// 	URL:   "AZURESQL_URL",
	// 	table: "dbo.test1",
	// },

	// &testDB{
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "Snowflake",
	// 	URL:   "SNOWFLAKE_URL",
	// 	table: "sling.test1",
	// },

	// &testDB{
	// 	// https://github.com/snowflakedb/gosnowflake
	// 	name:  "BigQuery",
	// 	URL:   "BIGQUERY_URL",
	// 	table: "public.test1",
	// },

	// &testDB{
	// 	// https://github.com/lib/pq
	// 	name:  "Redshift",
	// 	URL:   os.Getenv("REDSHIFT_URL"),
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

		task := elt.NewTask(0, elt.Config{
			SrcFileObj: csvFile,
			TgtConnObj: tgtDB.URL,
			TgtTable:   tgtDB.table,
			Mode:       "drop",
		})
		err = task.Execute()
		if err != nil {
			assert.NoError(t, err)
			return
		}

		taskUpsert := elt.NewTask(0, elt.Config{
			SrcFileObj: csvFileUpsert,
			TgtConnObj: tgtDB.URL,
			TgtTable:   tgtDB.table + "_upsert",
			Mode:       "truncate",
		})
		err = taskUpsert.Execute()
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
			task := elt.NewTask(0, elt.Config{
				SrcConnObj: srcDB.URL,
				SrcTable:   srcDB.table,
				TgtConnObj: tgtDB.URL,
				TgtTable:   tgtDB.table + "_copy",
				Mode:       "drop",
			})
			err = task.Execute()
			if err != nil {
				assert.NoError(t, err)
				return
			}

			taskUpsert := elt.NewTask(0, elt.Config{
				SrcConnObj: srcDB.URL,
				SrcTable:   srcDB.table + "_upsert",
				TgtConnObj: tgtDB.URL,
				TgtTable:   tgtDB.table + "_copy",
				Mode:       "upsert",
				UpdateKey:  "create_dt",
				PrimaryKey: "id",
			})
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
		task := elt.NewTask(0, elt.Config{
			SrcConnObj: srcDB.URL,
			SrcSQL:     g.F("select * from %s order by id", srcTableCopy),
			TgtFileObj: filePath2,
		})
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

	dbtConfig := `
	dbt_version: "0.18"
	repo_url: "https://github.com/fishtown-analytics/dbt-starter-project"
	schema: {schema}
	expr: "+my_second_dbt_model"`

	for _, db := range DBs {
		schema, _ := d.SplitTableFullName(db.table)
		switch db.name {
		case "Postgres", "Snowflake", "BigQuery":
			println()
			g.Debug(">>>>>> DBT (%s)", db.name)

			dbtMap, _ := g.UnmarshalMap(g.Deindent(g.R(dbtConfig, "schema", schema)))
			task := elt.NewTask(0, elt.Config{
				TgtConnObj: db.URL,
				TgtPostDbt: dbtMap,
			})
			err := task.Execute()
			if !assert.NoError(t, err) {
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
		assert.NoError(t, err)

		assert.EqualValues(t, "testing", cfg.SrcConn.ID)
		assert.EqualValues(t, "testing", cfg.SrcTable)
		assert.EqualValues(t, "testing", cfg.SrcFile.URL)
		assert.EqualValues(t, "testing", cfg.SrcSQL)
		assert.EqualValues(t, "testing", cfg.TgtConn.ID)
		assert.EqualValues(t, "testing", cfg.TgtTable)
		assert.EqualValues(t, "testing", cfg.TgtFile.URL)
		assert.EqualValues(t, "testing", cfg.Mode)
		assert.EqualValues(t, 111, cfg.Limit)
		assert.EqualValues(t, "testing", cfg.TgtTableDDL)
		assert.EqualValues(t, "testing", cfg.TgtTableTmp)
		assert.EqualValues(t, "testing", cfg.TgtPostSQL)

		return err
	}

	err := testCfg("tests/test1.yaml")
	assert.NoError(t, err)

	err = testCfg("tests/test1.json")
	assert.NoError(t, err)
}

func testTask(t *testing.T) {
	task := elt.NewTask(0, elt.Config{
		SrcConnObj: "SNOWFLAKE_URL",
		SrcTable:   "public.test5",
		TgtConnObj: "POSTGRES_URL",
		TgtTable:   "public.test5",
		Mode:       "drop",
	})
	assert.NoError(t, task.Err)

	// run task
	err := task.Execute()
	assert.NoError(t, err)
}
