package database

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	zerobus "github.com/databricks/zerobus-sdk/go"
	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xo/dburl"
	"syreclabs.com/go/faker"
)

var (
	PostgresURL = os.Getenv("POSTGRES_URL")
	SQLiteURL   = "./test.db"
)

type person struct {
	FirstName string `gorm:"primary_key" json:"first_name"`
	LastName  string `json:"last_name"`
	Email     string `json:"email"`
}

type place struct {
	Country string `json:"country" gorm:"index:idx_country_city"`
	City    string `json:"city" gorm:"index:idx_country_city"`
	Telcode int64  `json:"telcode"`
}

type transact struct {
	Datetime            time.Time `json:"date" `
	Description         string    `json:"description"`
	OriginalDescription string    `json:"original_description"`
	Amount              float64   `json:"amount"`
	TransactionType     string    `json:"transaction_type"`
	Category            string    `json:"category"`
	AccountName         string    `json:"account_name"`
	Labels              string    `json:"labels"`
	Notes               string    `json:"notes"`
}

type testDB struct {
	conn          Connection
	name          string
	URL           string
	schema        string
	transactDDL   string
	personDDL     string
	placeDDL      string
	placeIndex    string
	placeVwDDL    string
	placeVwSelect string
	propStrs      []string
}

var DBs = map[string]*testDB{
	"postgres": {
		name:        "postgres",
		URL:         os.Getenv("POSTGRES_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE public.person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view public.place_vw as select * from place where telcode = 65`,
		placeVwSelect: "select place.country,\n    place.city,\n    place.telcode\n   from place\n  where (place.telcode = 65);",
	},

	"sqlite3": {
		name:   "sqlite3",
		URL:    "sqlite://./test.db?_journal=WAL",
		schema: "main",

		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "CREATE VIEW place_vw as select * from place where telcode = 65",
	},

	"duckdb": {
		name:   "duckdb",
		URL:    "duckdb:///tmp/test.d.db?interactive=true",
		schema: "main",

		transactDDL: `CREATE TABLE transact (date_time date, description varchar, original_description varchar, amount decimal(10,5), transaction_type varchar, category varchar, account_name varchar, labels varchar, notes varchar )`,
		personDDL:   `CREATE TABLE person (first_name varchar, last_name varchar, email varchar, CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE \"place\" (\"country\" varchar,\"city\" varchar,\"telcode\" bigint )",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "CREATE VIEW place_vw as select * from place where telcode = 65",
	},

	"montherduck": {
		name: "montherduck",
		URL:  "motherduck://my_db?interactive=true&motherduck_token=" + os.Getenv("MOTHERDUCK_TOKEN"),
	},

	"mysql": {
		name:          "mysql",
		URL:           os.Getenv("MYSQL_URL"),
		schema:        "mysql",
		transactDDL:   `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:     `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:      "CREATE TABLE `place` (\n  `country` varchar(255) DEFAULT NULL,\n  `city` varchar(255) DEFAULT NULL,\n  `telcode` decimal(10,0) DEFAULT NULL,\n  KEY `idx_country_city` (`country`,`city`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci",
		placeIndex:    `select 1`, //`CREATE INDEX idx_country_city ON place(country, city)`,
		placeVwDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
		placeVwSelect: "CREATE ALGORITHM=UNDEFINED DEFINER=`admin`@`%` SQL SECURITY DEFINER VIEW `place_vw` AS select `place`.`country` AS `country`,`place`.`city` AS `city`,`place`.`telcode` AS `telcode` from `place` where (`place`.`telcode` = 65)",
	},

	"azuresql": {
		name:        "azuresql",
		URL:         os.Getenv("AZURESQL_URL"),
		schema:      "dbo",
		transactDDL: `CREATE TABLE dbo.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE dbo.place\n(\n    \"country\" varchar(255) NULL,\n    \"city\" varchar(255) NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create view place_vw as select * from place where telcode = 65`,
		placeVwSelect: "create view place_vw as select * from place where telcode = 65",
	},

	"azuredwh": {
		name:        "azuredwh",
		URL:         os.Getenv("AZUREDWH_URL"),
		schema:      "dbo",
		transactDDL: `CREATE TABLE dbo.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY NONCLUSTERED (first_name) NOT ENFORCED  )`,
		placeDDL:    "CREATE TABLE dbo.place\n(\n    \"country\" varchar(255) NULL,\n    \"city\" varchar(255) NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create view place_vw as select * from place where telcode = 65`,
		placeVwSelect: "CREATE VIEW [place_vw]\r\nAS select * from place where telcode = 65;",
	},

	"sqlserver": {
		name:        "sqlserver",
		URL:         os.Getenv("MSSQL_URL"),
		schema:      "dbo",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE dbo.place\n(\n    \"country\" varchar(255) NULL,\n    \"city\" varchar(255) NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create view place_vw as select * from place where telcode = 65`,
		placeVwSelect: "create view place_vw as select * from place where telcode = 65",
	},

	"oracle": {
		name:        "oracle",
		URL:         os.Getenv("ORACLE_URL"),
		schema:      "SYSTEM",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE \"SYSTEM\".\"PLACE\" \n   (\t\"COUNTRY\" VARCHAR2(255), \n\t\"CITY\" VARCHAR2(255), \n\t\"TELCODE\" NUMBER(*,0)\n   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n  TABLESPACE \"SYSTEM\"",
		placeIndex: `CREATE INDEX idx_country_city 
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW system.place_vw as select * from place where telcode = 65",
		placeVwSelect: "select \"COUNTRY\",\"CITY\",\"TELCODE\" from place where telcode = 65",
	},

	"redshift": {
		name:        "redshift",
		URL:         os.Getenv("REDSHIFT_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE public.person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view public.place_vw as select * from place where telcode = 65`,
		placeVwSelect: "select place.country,\n    place.city,\n    place.telcode\n   from place\n  where (place.telcode = 65);",
		propStrs: []string{
			"AWS_BUCKET=" + os.Getenv("AWS_BUCKET"),
		},
	},

	"bigquery": {
		name:        "bigquery",
		URL:         os.Getenv("BIGQUERY_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time datetime, description string, original_description string, amount float64, transaction_type string, category string, account_name string, labels string, notes string )`,
		personDDL:   `CREATE TABLE public.person (first_name string, last_name string, email string )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    country string,\n    city string,\n    telcode int64\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "create or replace view public.place_vw as select * from `proven-cider-633.public.place` where telcode = 65",
		placeVwSelect: "select place.country,\n    place.city,\n    place.telcode\n   from place\n  where (place.telcode = 65);",
		propStrs: []string{
			"PROJECT=proven-cider-633",
			"schema=public",
			"GOOGLE_APPLICATION_CREDENTIALS=/__/devbox/slingelt-prod-10fbedc838ea.json",
		},
	},

	"bigtable": {
		name: "bigtable",
		URL:  os.Getenv("BIGTABLE_URL"),
		propStrs: []string{
			"PROJECT=proven-cider-633",
			"INSTANCE=test-instance-1",
			"GOOGLE_APPLICATION_CREDENTIALS=/__/devbox/slingelt-prod-10fbedc838ea.json",
		},
	},

	"snowflake": {
		name:        "snowflake",
		URL:         os.Getenv("SNOWFLAKE_URL"),
		schema:      "PUBLIC",
		transactDDL: `CREATE TABLE public.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE public.person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "create or replace TABLE PLACE (\n\tCOUNTRY VARCHAR(16777216),\n\tCITY VARCHAR(16777216),\n\tTELCODE NUMBER(38,0)\n);",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view public.place_vw as select * from place where telcode = 65`,
		placeVwSelect: "create or replace view PLACE_VW(\n\tCOUNTRY,\n\tCITY,\n\tTELCODE\n) as select * from place where telcode = 65;",
		// propStrs: []string{
		// 	"schema=public",
		// 	"warehouse=COMPUTE_WH",
		// 	"internalStage=stage_loading",
		// 	"AWS_BUCKET=" + os.Getenv("AWS_BUCKET"),
		// },
	},

	"clickhouse": {
		name:        "clickhouse",
		URL:         os.Getenv("CLICKHOUSE_URL"),
		schema:      "default",
		transactDDL: `CREATE TABLE default.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) ) engine=Memory`,
		personDDL:   `CREATE TABLE default.person (first_name varchar(255), last_name varchar(255), email varchar(255)) engine=Memory`,
		placeDDL:    "CREATE TABLE default.place\n(\n    `country` String,\n    `city` Nullable(String),\n    `telcode` Int64\n)\nENGINE = Memory",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view default.place_vw as select * from place where telcode = 65`,
		placeVwSelect: "CREATE VIEW default.place_vw\n(\n    `country` String,\n    `city` Nullable(String),\n    `telcode` Int64\n) AS\nSELECT *\nFROM default.place\nWHERE telcode = 65",
	},
}

var connsMap map[string]map[string]any

func TestPostgres(t *testing.T) {
	t.Parallel()
	db := DBs["postgres"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestClickhouse(t *testing.T) {
	t.Parallel()
	db := DBs["clickhouse"]
	conn, err := connect(db)
	g.AssertNoError(t, err)
	assert.NotEmpty(t, conn)
	DBTest(t, db, conn)
}

func TestSQLite(t *testing.T) {
	t.Parallel()
	dbPath := strings.ReplaceAll(DBs["sqlite3"].URL, "file:", "")
	os.Remove(dbPath)
	db := DBs["sqlite3"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
	os.Remove(dbPath)
}

func TestDuckDB(t *testing.T) {
	db := DBs["duckdb"]
	os.Remove(strings.TrimPrefix(db.URL, "file://"))
	conn, err := connect(db)
	g.AssertNoError(t, err)
	// data, err := conn.Query("describe place")
	// g.PP(data.Records())
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestMySQL(t *testing.T) {
	t.Parallel()
	db := DBs["mysql"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestSnowflake(t *testing.T) {
	t.Parallel()
	db := DBs["snowflake"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestOracle(t *testing.T) {
	db := DBs["oracle"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestRedshift(t *testing.T) {
	db := DBs["redshift"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func TestSqlServer(t *testing.T) {
	t.Parallel()
	db := DBs["sqlserver"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}

	return

	db = DBs["azuresql"]
	conn, err = NewConn(db.URL)
	g.AssertNoError(t, err)

	err = conn.Connect()
	g.AssertNoError(t, err)

	db = DBs["azuredwh"]
	conn, err = NewConn(db.URL)
	g.AssertNoError(t, err)

	err = conn.Connect()
	g.AssertNoError(t, err)
}

func TestBigQuery(t *testing.T) {
	t.Parallel()
	db := DBs["bigquery"]
	conn, err := connect(db)
	if g.AssertNoError(t, err) {
		DBTest(t, db, conn)
	}
}

func connect(db *testDB) (conn Connection, err error) {
	connsMap, _ = env.GetHomeDirConnsMap()

	if ce, ok := connsMap[db.name]; ok {
		if db.URL == "" {
			db.URL = cast.ToString(ce["url"])
		}
	}

	if val := os.Getenv("ALLOW_BULK_IMPORT"); val != "" {
		val = strings.ToLower(cast.ToString(val))
		db.propStrs = append(db.propStrs, g.F("ALLOW_BULK_IMPORT=%s", val))
	}

	conn, err = NewConn(db.URL, db.propStrs...)
	if err != nil {
		return
	}
	err = conn.Connect()
	return
}

func DBTest(t *testing.T, db *testDB, conn Connection) {
	defer conn.Close()
	if t.Failed() {
		return
	}

	g.Info("Testing " + conn.GetType().String())

	err := conn.DropTable(db.schema+".person", db.schema+".place", db.schema+".transact", "person", "place", "transact")
	g.AssertNoError(t, err)

	err = conn.DropView(db.schema + ".place_vw")
	g.AssertNoError(t, err)

	// gConn, err := conn.GetGormConn()
	// g.AssertNoError(t, err)
	// gConn.SingularTable(true)
	// gConn.AutoMigrate(&person{}, &place{}, &transact{})

	conn.MustExec(db.transactDDL)
	conn.MustExec(db.personDDL)
	conn.MustExec(db.placeDDL)
	conn.MustExec(db.placeVwDDL)
	if !strings.Contains("redshift,bigquery,snowflake,clickhouse", db.name) {
		conn.MustExec(db.placeIndex)
	}
	personColumns, err := conn.GetColumns(db.schema + ".person")
	g.AssertNoError(t, err)
	placeColumns, err := conn.GetColumns(db.schema + ".place")
	g.AssertNoError(t, err)
	transactColumns, err := conn.GetColumns(db.schema + ".transact")
	g.AssertNoError(t, err)

	insCols, err := conn.ValidateColumnNames(personColumns, []string{"first_name", "last_name", "email"})
	g.AssertNoError(t, err)
	personInsertStatement := conn.GenerateInsertStatement(
		db.schema+".person",
		insCols,
		1,
	)

	insCols, err = conn.ValidateColumnNames(placeColumns, []string{"country", "city", "telcode"})
	g.AssertNoError(t, err)
	placeInsertStatement := conn.GenerateInsertStatement(
		db.schema+".place",
		insCols,
		1,
	)

	insCols, err = conn.ValidateColumnNames(transactColumns, []string{"date_time", "description", "amount"})
	g.AssertNoError(t, err)
	transactInsertStatement := conn.GenerateInsertStatement(
		db.schema+".transact",
		insCols,
		1,
	)

	conn.MustExec(personInsertStatement, "Jason", "Moiron", "jmoiron@jmoiron.net")
	conn.MustExec(personInsertStatement, "John", "Doe", "johndoeDNE@gmail.net")
	conn.MustExec(placeInsertStatement, "United States", "New York", 1)
	conn.MustExec(placeInsertStatement, "Hong Kong", nil, 852)
	conn.MustExec(placeInsertStatement, "Singapore", nil, 65)
	conn.MustExec(transactInsertStatement, cast.ToTime("2019-10-10"), "test\" \nproduct", 65.657)
	conn.MustExec(transactInsertStatement, cast.ToTime("2020-10-10"), "new \nproduct", 5.657)

	// Test Streaming
	// streamRec, err := conn.StreamRecords(`select * from person`)
	// g.AssertNoError(t, err)

	// recs := []map[string]interface{}{}
	// for rec := range streamRec {
	// 	recs = append(recs, rec)
	// }
	// assert.Len(t, recs, 2)

	stream, err := conn.StreamRows(g.F(`select * from %s.person`, db.schema))
	g.AssertNoError(t, err)

	rows := [][]interface{}{}
	for row := range stream.Rows() {
		rows = append(rows, row)
	}
	assert.Len(t, rows, 2)

	data, err := conn.Query(g.F(`select * from %s.person`, db.schema))
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 2)

	data, err = conn.Query(g.F(`select * from %s.place`, db.schema))
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 3)

	data, err = conn.Query(g.F(`select * from %s.transact`, db.schema))
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []interface{}{65.657, 5.567, 5.657}, cast.ToFloat64(data.Records()[0]["amount"]))

	// GetSchemas
	data, err = conn.GetSchemas()
	g.AssertNoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables(db.schema)
	g.AssertNoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews(db.schema)
	g.AssertNoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetColumns
	columns, err := conn.GetColumns(db.schema + ".person")
	g.AssertNoError(t, err)
	assert.Len(t, columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "varchar2", "character varying", "varchar", "text", "string", "string"}, strings.ToLower(columns[0].DbType))

	// GetPrimaryKeys
	if !strings.Contains("redshift,bigquery,snowflake,clickhouse", db.name) {
		data, err = conn.GetPrimaryKeys(db.schema + ".person")
		g.AssertNoError(t, err)
		assert.Len(t, data.Rows, 1)
		assert.Equal(t, "first_name", strings.ToLower(cast.ToString(data.Records()[0]["column_name"])))
	}

	// GetIndexes
	if !strings.Contains("redshift,bigquery,azuredwh,snowflake,sqlite3,clickhouse,duckdb", db.name) {
		data, err = conn.GetIndexes(db.schema + ".place")
		g.AssertNoError(t, err)
		assert.Len(t, data.Rows, 2)
		assert.Equal(t, "city", strings.ToLower(cast.ToString(data.Records()[1]["column_name"])))
	}

	// GetColumnsFull
	data, err = conn.GetColumnsFull(db.schema + ".place")
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 3)
	dType := strings.ToLower(cast.ToString(data.Records()[2]["data_type"]))
	assert.Contains(t, []string{"bigint", "number", "decimal", "int64", "fixed", "int64"}, dType)

	// GetDDL of table
	if !strings.Contains("redshift,bigquery,sqlserver,azuresql,azuredwh,duckdb", db.name) {
		ddl, err := conn.GetDDL(db.schema + ".place")
		g.AssertNoError(t, err)
		assert.Equal(t, db.placeDDL, ddl)
	}

	// GetDDL of view
	if !strings.Contains("redshift,bigquery,duckdb", db.name) {
		ddl, err := conn.GetDDL(db.schema + ".place_vw")
		g.AssertNoError(t, err)
		assert.Equal(t, db.placeVwSelect, ddl)
	}

	// load Csv from test file
	csv1 := iop.CSV{Path: "test/test1.csv"}

	stream, err = csv1.ReadStream()
	g.AssertNoError(t, err)

	csvTableName := db.schema + ".test1"
	err = conn.DropTable(csvTableName)
	g.AssertNoError(t, err)

	csvTable, err := ParseTableName(csvTableName, conn.GetType())
	g.AssertNoError(t, err)

	sampleData := iop.NewDataset(stream.Columns)
	sampleData.Rows = stream.Buffer
	ddl, err := conn.GenerateDDL(csvTable, sampleData, false)
	g.AssertNoError(t, err)
	ok := assert.NotEmpty(t, ddl)

	if ok {
		_, err = conn.ExecMulti(ddl)
		if !g.AssertNoError(t, err) {
			return
		}

		// import to database
		conn.SetProp("AWS_BUCKET", os.Getenv("AWS_BUCKET"))
		// err = conn.Begin()
		g.AssertNoError(t, err)
		_, err = conn.BulkImportStream(csvTableName, stream)
		if !g.AssertNoError(t, err) {
			return
		}

		// select back to assert equality
		count, err := conn.GetCount(csvTableName)
		g.AssertNoError(t, err)
		assert.Equal(t, 1000, cast.ToInt(count))

		// err = conn.Commit()
		g.AssertNoError(t, err)
	}

	if t.Failed() {
		return
	}

	// Test Schemata
	schemata, err := conn.GetSchemata(SchemataLevelColumn, db.schema, "")
	g.AssertNoError(t, err)
	sData := schemata.Database().Schemas[strings.ToLower(db.schema)]
	assert.Equal(t, strings.ToLower(db.schema), strings.ToLower(sData.Name))
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	personTable := sData.Tables["person"]
	assert.Len(t, personTable.Columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "varchar2", "character varying", "varchar", "text", "string", "character varying(255)", "string"}, strings.ToLower(personTable.ColumnsMap()["email"].DbType))
	assert.Equal(t, true, sData.Tables["place_vw"].IsView)
	// assert.EqualValues(t, int64(3), conn.Schemata().Tables[db.schema+".person"].ColumnsMap["email"].Position)
	if t.Failed() {
		return
	}

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1":         db.schema + ".place",
		"t2":         db.schema + ".place",
		"t1_field":   "country",
		"t1_fields1": "country",
		"t1_filter":  "1=1",
		"t2_field":   "country",
		"t2_fields1": "country",
		"t2_filter":  "1=1",
		"conds":      `lower(t1.country) = lower(t2.country)`,
	}
	data, err = conn.RunAnalysis("table_join_match", values)
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []interface{}{0.0, int64(0), "0"}, data.Records()[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, cast.ToFloat64(data.Records()[1]["match_rate"]))

	// RunAnalysisTable field_stat
	m := g.M("tables", []string{db.schema + ".person", db.schema + ".place"})
	data, err = conn.RunAnalysis("table_count", m)
	g.AssertNoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []int{2, 3}, cast.ToInt(data.Records()[0]["cnt"]))
	assert.Contains(t, []int{2, 3}, cast.ToInt(data.Records()[1]["cnt"]))

	// RunAnalysisField field_stat_deep
	table, _ := ParseTableName(db.schema+".person", conn.GetType())
	m = g.M("schema", table.Schema, "table", table.Name)
	data, err = conn.RunAnalysis("field_stat_deep", m)
	if g.AssertNoError(t, err) {
		assert.Len(t, data.Rows, 3)
		assert.EqualValues(t, 2, cast.ToInt(data.Records()[0]["tot_cnt"]))
		assert.EqualValues(t, 0, cast.ToInt(data.Records()[1]["f_dup_cnt"]))
	}
	if t.Failed() {
		return
	}

	// Not used
	// columns, err = conn.GetColumnStats(db.schema + ".transact")
	// g.AssertNoError(t, err)
	// assert.Len(t, columns, 9)
	// assert.Contains(t, []string{"date", "datetime"}, columns[0].Type)
	// assert.EqualValues(t, "original_description", strings.ToLower(columns[2].Name))
	// assert.Contains(t, []string{"string", "text"}, columns[2].Type)
	// assert.EqualValues(t, "decimal", columns[3].Type)

	// Extract / Load Test
	if !strings.Contains("redshift,bigquery,sqlite3,sqlserver,azuresql,azuredwh,clickhouse,duckdb", db.name) {
		ELTest(t, db, csvTableName)
		if t.Failed() {
			return
		}
	}

	// Drop all tables
	err = conn.DropTable("person", "place", "transact", "test1")
	g.AssertNoError(t, err)

	if !strings.Contains("redshift,bigquery,sqlite3,azuredwh,clickhouse,duckdb,oracle", db.name) {
		// test sleep function
		sleepSQL := g.R(
			conn.GetTemplateValue("function.sleep"),
			"seconds", "1",
		)
		dd, err := conn.Query(sleepSQL)
		g.AssertNoError(t, err)
		assert.Greater(t, dd.Duration, 1.0)

		// Test cancel query
		cancelDone := make(chan bool)
		ctx, cancel := context.WithCancel(conn.Context().Ctx)
		go func() {
			_, err := conn.QueryContext(ctx, sleepSQL)
			assert.Error(t, err)
			cancelDone <- true
		}()

		time.Sleep(100 * time.Millisecond)
		cancel()
		<-cancelDone // wait for cancel to be done
	}

}

func ELTest(t *testing.T, db *testDB, srcTable string) {
	tgtTable := srcTable + "2"

	// var srcConn, tgtConn PostgresConn
	srcConn, err := NewConn(db.URL, db.propStrs...)
	g.AssertNoError(t, err)
	tgtConn, err := NewConn(db.URL, db.propStrs...)
	g.AssertNoError(t, err)

	err = srcConn.Connect()
	g.AssertNoError(t, err)

	err = tgtConn.Connect()
	g.AssertNoError(t, err)

	sTable, _ := ParseTableName(srcTable, srcConn.GetType())
	tTable, _ := ParseTableName(tgtTable, tgtConn.GetType())

	ddl, err := srcConn.GetDDL(srcTable)
	g.AssertNoError(t, err)
	assert.NotEmpty(t, ddl)
	newDdl := strings.Replace(ddl, sTable.Name, tTable.Name, 1)
	if strings.Contains("oracle,snowflake", db.name) {
		newDdl = strings.Replace(
			ddl, strings.ToUpper(sTable.Name),
			strings.ToUpper(tTable.Name), 1,
		)
	}

	err = tgtConn.DropTable(tgtTable)
	g.AssertNoError(t, err)

	_, err = tgtConn.Exec(newDdl)
	g.AssertNoError(t, err)

	stream, err := srcConn.StreamRows(`select * from ` + srcTable)
	g.AssertNoError(t, err)

	if g.AssertNoError(t, err) {
		_, err = tgtConn.InsertBatchStream(tgtTable, stream)
		g.AssertNoError(t, err)

		m := g.M("tables", []string{srcTable, tgtTable})
		data, err := tgtConn.RunAnalysis("table_count", m)
		if g.AssertNoError(t, err) {
			assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
		}
	}

	// use Bulk
	_, err = tgtConn.Query("delete from " + tgtTable)
	g.AssertNoError(t, err)

	stream, err = srcConn.BulkExportStream(Table{SQL: `select * from ` + srcTable, Dialect: srcConn.GetType()})
	g.AssertNoError(t, err)

	if err == nil {
		_, err = tgtConn.BulkImportStream(tgtTable, stream)
		g.AssertNoError(t, err)

		m := g.M("tables", []string{srcTable, tgtTable})
		data, err := tgtConn.RunAnalysis("table_count", m)
		if g.AssertNoError(t, err) {
			assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
		}
	}

	err = tgtConn.DropTable(tgtTable)
	g.AssertNoError(t, err)

	srcConn.Close()
	tgtConn.Close()

}

func tInsertStreamLarge(t *testing.T, conn Connection, data iop.Dataset, tableName string) {
	start := time.Now()
	getRate := func(cnt uint64) string {
		return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
	}

	df, err := iop.MakeDataFlow(data.Stream())
	g.AssertNoError(t, err)

	cnt, err := conn.BulkImportFlow(tableName, df)
	g.AssertNoError(t, err)

	g.Debug("inserted %d rows [%s r/s]", cnt, getRate(cnt))
}

func tSelectStreamLarge(t *testing.T, conn Connection, tableName string, dfMult int) (count int64) {
	start := time.Now()
	getRate := func(cnt int64) string {
		return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
	}
	table, _ := ParseTableName(tableName, conn.GetType())
	UseBulkExportFlowCSV = false
	df, err := conn.BulkExportFlow(table)
	if !g.AssertNoError(t, err) {
		return
	}
	for ds := range df.StreamCh {
		for range ds.Rows() {
			// do nothing
		}
		ds.SetEmpty()
		g.AssertNoError(t, ds.Err())
	}
	count = cast.ToInt64(df.Count())
	assert.True(t, df.IsEmpty())
	df.Close()
	g.Debug("selected %d rows [%s r/s]", count, getRate(count))
	return count
}

// generate large dataset or use cache
func generateLargeDataset(path string, numRows int) (data iop.Dataset) {

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		// path exists
		data, err = iop.ReadCsv(path)
		if numRows == len(data.Rows) {
			return data
		}
		os.Remove(path)
	}

	type FakeField struct {
		name string
		gen  func() interface{}
	}

	fieldsFunc := []*FakeField{
		{"name", func() interface{} { return faker.Name().Name() }},
		{"url", func() interface{} { return faker.Internet().Url() }},
		{"date_time", func() interface{} { return faker.Date().Forward(100 * time.Minute).Format("2006-01-02 15:04:05") }},
		{"address", func() interface{} { return faker.Address().SecondaryAddress() }},
		{"price", func() interface{} { return faker.Commerce().Price() }},
		{"my_int", func() interface{} { return faker.Number().NumberInt64(5) }},
		{"email", func() interface{} { return faker.Internet().Email() }},
		{"creditcardexpirydate", func() interface{} { return faker.Date().Forward(1000000 * time.Minute).Format("2006-01-02") }},
		{"latitude", func() interface{} { return faker.Address().Latitude() }},
		{"longitude", func() interface{} { return faker.Address().Longitude() }},
	}

	makeRow := func() (row []interface{}) {
		row = make([]interface{}, len(fieldsFunc))
		c := 0
		for _, ff := range fieldsFunc {
			row[c] = ff.gen()
			c++
		}

		return row
	}

	getFields := func() (fields []string) {
		fields = make([]string, len(fieldsFunc))
		i := 0
		for _, ff := range fieldsFunc {
			fields[i] = ff.name
			i++
		}
		return
	}

	data = iop.NewDataset(nil)
	data.Rows = make([][]interface{}, numRows)
	data.SetFields(getFields())

	for i := 0; i < 50; i++ {
		data.Rows[i] = makeRow()
	}

	for i := 50; i < numRows; i++ {
		data.Rows[i] = data.Rows[i%50]
	}

	file, _ := os.Create(path)
	_, err := data.WriteCsv(file)
	if err != nil {
		log.Fatal(g.Error(err, "Could not create file: "+path))
	}

	return data

}

func TestLargeDataset(t *testing.T) {

	numRows := 100 * 1000
	data := generateLargeDataset("/tmp/LargeDataset.csv", numRows)
	data.InferColumnTypes()

	dbs := []*testDB{
		DBs["postgres"],
		DBs["mysql"],
		DBs["oracle"],
		DBs["clickhouse"],
		// DBs["redshift"],
		DBs["sqlserver"],
		// DBs["azuresql"],
		// DBs["azuredwh"],
		DBs["snowflake"],
		DBs["bigquery"],
		DBs["sqlite3"],
		DBs["duckdb"],
	}
	// test snowflake Azure and AWS
	DBs["snowflake_aws"] = &testDB{
		name:   "snowflake-aws",
		URL:    os.Getenv("SNOWFLAKE_URL") + "&copy_method=AWS",
		schema: "PUBLIC",
	}
	DBs["snowflake_azure"] = &testDB{
		name:   "snowflake-azure",
		URL:    os.Getenv("SNOWFLAKE_URL") + "&copy_method=AZURE",
		schema: "PUBLIC",
	}

	// dbs = []*testDB{DBs["sqlite3"], DBs["duckdb"]}

	ctx := g.NewContext(context.Background(), 5)
	doTest := func(db *testDB) {
		defer ctx.Wg.Write.Done()
		os.Setenv("FILE_MAX_ROWS", "13000")
		conn, err := connect(db)
		ok := g.AssertNoError(t, err)
		if !ok {
			return
		}
		defer conn.Close()
		g.Info("START - testing large file for %s", db.name)
		defer g.Info("END - testing large file for %s", db.name)

		tableName := db.schema + ".test1"
		err = conn.DropTable(tableName)
		g.AssertNoError(t, err)

		table, err := ParseTableName(tableName, conn.GetType())
		g.AssertNoError(t, err)

		ddl, err := conn.GenerateDDL(table, data, false)
		g.AssertNoError(t, err)

		_, err = conn.ExecMulti(ddl)
		g.AssertNoError(t, err)
		tInsertStreamLarge(t, conn, data, tableName)

		cnt, err := conn.GetCount(tableName)
		g.AssertNoError(t, err)
		if !assert.EqualValues(t, numRows, cnt, "Got %d", cnt) {
			return
		}

		dfMult := 1
		cnt = tSelectStreamLarge(t, conn, tableName, dfMult)
		assert.EqualValues(t, numRows*dfMult, cnt)

		err = conn.DropTable(tableName)
		g.AssertNoError(t, err)

	}

	testSnowflake := func() {
		defer ctx.Wg.Write.Done()
		ctx.Wg.Write.Add()
		doTest(DBs["snowflake"])
		ctx.Wg.Write.Add()
		doTest(DBs["snowflake_aws"])
		ctx.Wg.Write.Add()
		doTest(DBs["snowflake_azure"])
	}

	for _, db := range dbs {
		ctx.Wg.Write.Add()
		if db.name == "snowflake" {
			go testSnowflake()
		} else {
			go doTest(db)
		}
	}
	ctx.Wg.Write.Wait()
}

func TestURL(t *testing.T) {
	url, err := dburl.Parse(os.Getenv("SNOWFLAKE_URL"))
	if err != nil {
		return
	}
	g.P(url)
	// g.P(url.Query())
}

func TestExport(t *testing.T) {
	db := DBs["bigquery"]
	conn, err := NewConn(db.URL)
	g.AssertNoError(t, err)
	table, _ := ParseTableName(`proven-cider-633.pg_home.bank_mint_transactions`, conn.GetType())
	_, err = conn.BulkExportFlow(table)
	g.AssertNoError(t, err)
}

func TestMultiStatement(t *testing.T) {
	db := DBs["oracle"]
	conn, err := NewConn(db.URL)
	g.AssertNoError(t, err)
	sql := `select 1 from dual ;
	-- ;
	select 2 as n, ';' as t, ' '' /*' as t2 from dual
	/* ;
	 hello
	*/;
	select 'a' done from dual
	;

	`
	_, err = conn.Exec(sql)
	g.AssertNoError(t, err)

	db = DBs["bigquery"]
	conn, err = NewConn(db.URL)
	g.AssertNoError(t, err)
	_, err = conn.Exec("select 1 as a; select 2 /* hey; */ as a; -- sselect")
	g.AssertNoError(t, err)
}

func TestPasswordSSH(t *testing.T) {
	// with password
	dbURL := "POSTGRES_URL"
	// sshURL := "ssh://user:hello@bionic:2222"
	sshURL := os.Getenv("SSH_TEST_PASSWD_URL")
	conn, err := NewConn(dbURL, "SSH_TUNNEL="+sshURL)
	g.AssertNoError(t, err)
	err = conn.Connect()
	g.AssertNoError(t, err)
	conn.Close()
}

func TestPrivateKeySSH(t *testing.T) {
	// with private key
	dbURLs := []string{
		"POSTGRES_URL",
		"MSSQL_URL",
		"MYSQL_URL",
		"ORACLE_URL",
	}
	for _, dbURL := range dbURLs {
		// sshURL := "ssh://user@bionic:2222"
		sshURL := os.Getenv("SSH_TEST_PRVKEY_URL")
		conn, err := NewConn(
			dbURL, "SSH_TUNNEL="+sshURL,
			"SSH_PRIVATE_KEY=/root/.ssh/id_rsa",
		)
		g.AssertNoError(t, err)
		err = conn.Connect()
		g.AssertNoError(t, err)
		conn.Close()
	}
}

func testOracleClob(t *testing.T) {
	conn, err := NewConn("ORACLE_URL")
	g.AssertNoError(t, err)
	err = conn.Connect()
	g.AssertNoError(t, err)

	// sql := `select *	from dba_hist_sqltext`
	sql := `select * from SYS.METASTYLESHEET where rownum < 10`
	data, err := conn.Query(sql)
	g.AssertNoError(t, err)
	g.P(data.Rows[0])
}

func TestCastColumnsForSelect(t *testing.T) {
	db := DBs["postgres"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	err = conn.DropTable(`public.tgt1`, `public.src1`)
	_, err = conn.Exec(`
	create table public.tgt1 (
		"CoL1" varchar(100),
		col1b varchar(100),
		col2 int,
		col3 timestamp,
		col3b timestamp,
		col4 numeric(10,3),
		"COL5" varchar(40)
	)
	`)
	g.AssertNoError(t, err)

	_, err = conn.Exec(`
	create table public.src1 (
		col1 varchar(50),
		col3 timestamp,
		col2 bigint,
		col4 float,
		col5 varchar(50)
	)
	`)
	g.AssertNoError(t, err)

	tgtColumns, err := conn.GetColumns(`public.tgt1`)
	g.AssertNoError(t, err)
	srcColumns, err := conn.GetColumns(`public.src1`)
	g.AssertNoError(t, err)

	assert.EqualValues(t, 7, len(tgtColumns))
	assert.EqualValues(t, 5, len(srcColumns))

	tgtFields, err := conn.ValidateColumnNames(
		tgtColumns,
		srcColumns.Names(),
	)
	g.AssertNoError(t, err)

	assert.EqualValues(t, 5, len(tgtFields))
	assert.EqualValues(t, `"CoL1"`, tgtFields[0])

	g.P(tgtFields)

	srcFields := conn.CastColumnsForSelect(srcColumns, tgtColumns)
	assert.EqualValues(t, len(tgtFields), len(srcFields))
	g.P(srcFields)

	err = conn.DropTable(`public.tgt1`, `public.src1`)
	g.AssertNoError(t, err)
}

func TestGetSQLColumnsLarge(t *testing.T) {
	conn, err := NewConn(os.Getenv("PG_BIONIC_URL"))
	// conn, err := NewConn(DBs["bigquery"].URL)
	if !g.AssertNoError(t, err) {
		return
	}

	err = conn.Connect()
	g.AssertNoError(t, err)

	// sql := `select * from public.ccxt_price_second limit 500000`
	sql := Table{SQL: `select * from crypto.ccxt_price_second limit 500000`}
	cols, err := conn.GetSQLColumns(sql)
	g.AssertNoError(t, err)
	g.P(cols)
}

func TestSchema(t *testing.T) {
	db := DBs["snowflake"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	data, err := conn.GetSchemas()
	g.AssertNoError(t, err)
	g.P(data)
}

func TestQuery(t *testing.T) {
	db := DBs["bigquery"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	// data, err := conn.Query(`select * from main.test1`)
	// g.AssertNoError(t, err)
	// assert.Len(t, data.Rows, 1000)

	// data, err := conn.Query(`select count(*) from main.test1`)
	// g.AssertNoError(t, err)
	// g.P(data.Rows)

	// rows := conn.Db().QueryRowx(`select count(*) from main.test1`)
	// g.AssertNoError(t, rows.Err())
	// values, err := rows.SliceScan()
	// g.AssertNoError(t, err)
	// g.P(values)

	count, err := conn.GetCount("public.test1")
	g.AssertNoError(t, err)
	assert.Equal(t, 1000, cast.ToInt(count))
}

func TestDecimal(t *testing.T) {
	db := DBs["postgres"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	err = conn.DropTable(`public.table1`)
	g.AssertNoError(t, err)

	_, err = conn.Exec(`
	create table public.table1 (
		col1 numeric(30,9)
	)
	`)
	g.AssertNoError(t, err)

	_, err = conn.Exec(`insert into public.table1 values(1032.442)`)
	g.AssertNoError(t, err)
	_, err = conn.Exec(`commit`)
	g.AssertNoError(t, err)

	result, err := conn.Db().Queryx(`select col1 from public.table1`)
	g.AssertNoError(t, err)
	result.Next()
	var val interface{}
	result.Scan(&val)
	g.P(val)
	g.P(cast.ToString(val))

	decReplRegex := regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`)
	nVal := decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
	g.P(nVal)
	err = conn.DropTable(`public.table1`)

}

func TestStageSnowflake(t *testing.T) {
	db := DBs["snowflake"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	fileName := "test1.1.csv.gz"
	filePath, _ := filepath.Abs("test/test1.1.csv.gz")
	_, err = conn.Exec(g.F("PUT file://%s @~", filePath))
	g.AssertNoError(t, err)

	data, err := conn.Query("LIST @~")
	g.AssertNoError(t, err)
	g.P(data.Records())
	names := data.ColValuesStr(0)
	found := false
	for _, name := range names {
		if name == fileName {
			found = true
		}
	}
	assert.True(t, found)

	// Not implemented yet: https://github.com/snowflakedb/gosnowflake/search?q=SNOW-206124
	// _, err = conn.Exec(g.F("GET @~/%s file:///tmp/", fileName))
	// g.AssertNoError(t, err)

	// assert.True(t, g.PathExists("/tmp/test1.1.csv.gz"))

	_, err = conn.Exec(g.F("REMOVE @~/%s", fileName))
	g.AssertNoError(t, err)
}

func testSnowflakeAuth(t *testing.T) {
	url := "snowflake://&authenticator=externalbrowser"
	conn, err := NewConn(url)
	// conn, err := NewConn(DBs["bigquery"].URL)
	if !g.AssertNoError(t, err) {
		return
	}

	err = conn.Connect()
	g.AssertNoError(t, err)

	// data, err := conn.Query(`show columns in database "BIDB"`)
	// g.AssertNoError(t, err)
	// g.Debug("got %d columns", len(data.Rows))

	schemata, err := conn.GetSchemata(SchemataLevelColumn, "", "")
	g.AssertNoError(t, err)
	g.Debug("found %d tables totalling %d columns", len(schemata.Tables()), len(schemata.Columns()))

}

func TestSchemataAll(t *testing.T) {
	db := DBs["bigquery"]
	conn, err := connect(db)

	if !g.AssertNoError(t, err) {
		return
	}

	err = conn.Connect()
	g.AssertNoError(t, err)

	schemata, err := conn.GetSchemata(SchemataLevelColumn, "public", "place_vw")
	// schemata, err := GetSchemataAll(conn)
	g.AssertNoError(t, err)
	g.P(schemata)
	_ = schemata

}

func TestBigTable(t *testing.T) {
	// https://console.cloud.google.com/bigtable/instances?project=proven-cider-633

	db := DBs["bigtable"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	tableName := "test_table3"
	numRows := 10 * 1000

	// // drop if exists
	// err = conn.DropTable(tableName)
	// if !g.AssertNoError(t, err) {
	// 	return
	// }

	// // create table
	g.Debug("Creating table")
	query := BigTableQuery{
		Action:         BTCreateTable,
		Table:          tableName,
		ColumnFamilies: []string{"default"},
	}
	_, err = conn.Exec(g.Marshal(query))
	if !g.AssertNoError(t, err) {
		return
	}

	// tables, err := conn.GetTables("")
	// if !g.AssertNoError(t, err) {
	// 	return
	// }
	// g.P(tables.Rows)

	// columns, err := conn.GetColumns(tableName)
	// if !g.AssertNoError(t, err) {
	// 	return
	// }
	// g.P(columns.Names())

	// // insert data
	g.Debug("Inserting data")
	data := generateLargeDataset("/tmp/MediumDataset.csv", numRows)
	count, err := conn.InsertBatchStream(tableName, data.Stream())
	assert.Equal(t, numRows, count)
	if !g.AssertNoError(t, err) {
		return
	}

	// read table
	g.Debug("Reading data")
	table, _ := ParseTableName(tableName, conn.GetType())
	ds, err := conn.BulkExportStream(table)
	if !g.AssertNoError(t, err) {
		return
	}

	data1, err := ds.Collect(0)
	// g.PP(data1.Columns)
	g.P(data1.Columns.Names())
	g.P(data1.Rows[0])
	assert.EqualValues(t, numRows, len(data1.Rows))
	g.AssertNoError(t, err)

}

func TestConcurrentDuckDb(t *testing.T) {

	db := DBs["duckdb"]
	conn1, err := connect(db)
	g.AssertNoError(t, err)
	conn2, err := connect(db)
	g.AssertNoError(t, err)

	c := g.NewContext(context.Background())
	c.Wg.Read.Add()
	go func() {
		defer c.Wg.Read.Done()
		data, err := conn1.Query("select 1 as a")
		g.AssertNoError(t, err)
		g.PP(data.Records())
	}()

	c.Wg.Read.Add()
	go func() {
		defer c.Wg.Read.Done()
		data, err := conn2.Query("select 1 as b")
		g.AssertNoError(t, err)
		g.PP(data.Records())
	}()

	c.Wg.Read.Wait()

}

func TestParseURL(t *testing.T) {
	u, err := url.Parse("sqlserver://myuser:mypass@host.ip?database=master")
	g.AssertNoError(t, err)
	g.Info(g.Marshal(u))
}

func TestInteractiveDuckDb(t *testing.T) {
	var err error

	// db := DBs["montherduck"]
	db := DBs["duckdb"]
	conn, err := connect(db)
	g.AssertNoError(t, err)

	assert.True(t, cast.ToBool(conn.GetProp("interactive")))

	data, err := conn.Query("select 1 as a union all select 2 as a")
	g.AssertNoError(t, err)
	assert.NotEmpty(t, data.Records())

	data, err = conn.Query("select 1 ,,,")
	assert.Error(t, err)

	data, err = conn.Query("select 33333 33 33!")
	assert.Error(t, err)

	_, err = conn.Exec("select 33333 33 33!")
	assert.Error(t, err)
}

func TestInteractiveMotherDuck(t *testing.T) {
	cmd := exec.Command("~/duckdb/0.8.1/duckdb", "-csv", "-cmd", "pragma version", "md:")
	cmd.Env = append(os.Environ(), "motherduck_token="+os.Getenv("MOTHERDUCK_TOKEN"))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatalln("Error creating the stdin pipe :", err)
	}

	stdOutReader, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalln("Error creating the stdout pipe :", err)
	}
	_ = stdOutReader

	stdErrReader, err := cmd.StderrPipe()
	if err != nil {
		log.Fatalln("Error creating the stderr pipe :", err)
	}

	go io.Copy(os.Stdout, stdOutReader)
	go io.Copy(os.Stderr, stdErrReader)

	go func() {
		io.Copy(stdin, strings.NewReader("use my_db;\n"))
		io.Copy(stdin, strings.NewReader("PRAGMA version;\n"))
		io.Copy(stdin, strings.NewReader("select Count(1) from main.test1;\n"))
		// io.Copy(stdin, strings.NewReader("select 2,,, as a;\n"))
		io.Copy(stdin, strings.NewReader("PRAGMA version;\n"))
		io.Copy(stdin, strings.NewReader(".quit\n"))
		// io.Copy(stdin, strings.NewReader(".quit\n"))
		// io.Copy(stdin, strings.NewReader("set -m\n"))
		// io.Copy(stdin, strings.NewReader("/usr/bin/python3\n"))
		// io.Copy(stdin, strings.NewReader("print('hey')\n"))
	}()

	g.Info("start")
	err = cmd.Run()
	if err != nil {
		log.Fatalln("Error while running :", err)
	}
}

func newTestRedshiftConn(t *testing.T) *RedshiftConn {
	t.Helper()
	conn, err := NewConnContext(
		context.Background(),
		"redshift://testuser:testpass@testhost.example.com:5439/testdb",
	)
	if err != nil {
		t.Fatalf("could not create redshift conn: %s", err)
	}
	rs, ok := conn.(*RedshiftConn)
	if !ok {
		t.Fatalf("expected *RedshiftConn, got %T", conn)
	}
	return rs
}

// ensureAWSCredentials should short-circuit when explicit credentials are provided,
// without attempting to load from the AWS credential chain.
func TestRedshiftEnsureAWSCredentialsExplicit(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")

	ok, err := conn.ensureAWSCredentials()
	assert.NoError(t, err)
	assert.True(t, ok)
}

// ensureAWSCredentials should honor USE_ENVIRONMENT=false and not attempt the chain.
func TestRedshiftEnsureAWSCredentialsOptedOut(t *testing.T) {
	for _, val := range []string{"false", "FALSE", "0", "no"} {
		conn := newTestRedshiftConn(t)
		conn.SetProp("USE_ENVIRONMENT", val)

		ok, err := conn.ensureAWSCredentials()
		assert.NoError(t, err, val)
		assert.False(t, ok, val)
	}
}

func TestRedshiftMakeCopyCredentialString(t *testing.T) {
	t.Run("static credentials with session token", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
		conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")
		conn.SetProp("AWS_SESSION_TOKEN", "sessiontoken")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t,
			"credentials 'aws_access_key_id=AKIAEXAMPLE;aws_secret_access_key=secretkey;token=sessiontoken'",
			cred,
		)
	})

	t.Run("iam role arn", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/MyRole")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t,
			"iam_role 'arn:aws:iam::123456789012:role/MyRole'",
			cred,
		)
	})

	t.Run("iam role default", func(t *testing.T) {
		conn := newTestRedshiftConn(t)
		conn.SetProp("AWS_ROLE_ARN", "default")

		cred := conn.makeCopyCredentialString()
		assert.Equal(t, "iam_role default", cred)
	})
}

// getS3Props should include the region and propagate explicit credentials.
func TestRedshiftGetS3Props(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")
	conn.SetProp("AWS_REGION", "eu-west-1")

	props := conn.getS3Props()
	joined := strings.Join(props, " ")

	assert.Contains(t, joined, "ACCESS_KEY_ID=AKIAEXAMPLE")
	assert.Contains(t, joined, "SECRET_ACCESS_KEY=secretkey")
	assert.Contains(t, joined, "REGION=eu-west-1")
}

// redactCredentials should mask all AWS secrets, and leave the SQL intact
// when a credential prop is unset (an empty value must not match everywhere).
func TestRedshiftRedactCredentials(t *testing.T) {
	conn := newTestRedshiftConn(t)
	conn.SetProp("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
	conn.SetProp("AWS_SECRET_ACCESS_KEY", "secretkey")
	conn.SetProp("AWS_SESSION_TOKEN", "sessiontoken")

	sql := "unload ('select 1') to 's3://b/p' credentials 'aws_access_key_id=AKIAEXAMPLE;aws_secret_access_key=secretkey;token=sessiontoken'"
	clean := conn.redactCredentials(sql)

	assert.NotContains(t, clean, "AKIAEXAMPLE")
	assert.NotContains(t, clean, "secretkey")
	assert.NotContains(t, clean, "sessiontoken")
	assert.Contains(t, clean, "s3://b/p")

	// AWS_ROLE_ARN is unset here, so the SQL must not be mangled
	conn2 := newTestRedshiftConn(t)
	conn2.SetProp("USE_ENVIRONMENT", "false")
	assert.Equal(t, sql, conn2.redactCredentials(sql))
}

// env.Clean should mask the session token under either property name.
func TestCleanRedactsSessionToken(t *testing.T) {
	props := map[string]string{
		"aws_session_token":     "tokenABC",
		"aws_secret_access_key": "secretXYZ",
	}
	line := "copy tbl from 's3://b/p' credentials 'aws_secret_access_key=secretXYZ;token=tokenABC'"
	clean := env.Clean(props, line)

	assert.NotContains(t, clean, "tokenABC")
	assert.NotContains(t, clean, "secretXYZ")
}

// The change_capture_soft soft-mark guard must be NULL-safe on every dialect.
// Target rows loaded before CDC started have a NULL _sling_synced_op, and
// `_sling_synced_op != 'D'` evaluates to NULL (not TRUE) for those rows, so a
// bare comparison silently skips them and deletes are never recorded.
func TestSoftMergeGuardIsNullSafe(t *testing.T) {
	types := []dbio.Type{
		dbio.TypeDbPostgres, dbio.TypeDbRedshift, dbio.TypeDbSnowflake,
		dbio.TypeDbBigQuery, dbio.TypeDbSQLServer, dbio.TypeDbDuckDb,
		dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbClickhouse,
		dbio.TypeDbSQLite, dbio.TypeDbOracle, dbio.TypeDbDatabricks,
		dbio.TypeDbStarRocks, dbio.TypeDbD1, dbio.TypeDbExasol,
	}

	for _, ty := range types {
		tmpl, err := ty.Template()
		if !assert.NoError(t, err, ty) {
			continue
		}

		sql := tmpl.Core["merge_change_capture_soft"]
		if strings.TrimSpace(sql) == "" || sql == "null" {
			continue // dialect does not support the strategy
		}

		// locate the soft-mark statement (the one setting _sling_synced_op = 'D')
		var mark string
		for _, stmt := range strings.Split(sql, ";") {
			up := strings.ReplaceAll(strings.ToUpper(stmt), `"`, "")
			if strings.Contains(up, "UPDATE") && strings.Contains(up, "_SLING_SYNCED_OP = 'D'") {
				mark = stmt
				break
			}
		}

		if assert.NotEmpty(t, mark, "%s: no soft-mark statement found", ty) {
			assert.Contains(t, strings.ToUpper(mark), "COALESCE",
				"%s: soft-mark guard is not NULL-safe:\n%s", ty, mark)
		}
	}
}

type fakeZerobusStream struct {
	batches      [][]byte
	ingestErr    error
	flushErr     error
	closeErr     error
	ingestCalled int
	flushCalled  int
	closeCalled  int
}

func (f *fakeZerobusStream) IngestBatch(ipcBytes []byte) (int64, error) {
	f.ingestCalled++
	if f.ingestErr != nil {
		return -1, f.ingestErr
	}
	f.batches = append(f.batches, ipcBytes)
	return int64(f.ingestCalled), nil
}

func (f *fakeZerobusStream) Flush() error {
	f.flushCalled++
	return f.flushErr
}

func (f *fakeZerobusStream) Close() error {
	f.closeCalled++
	return f.closeErr
}

func (f *fakeZerobusStream) GetUnackedBatches() ([][]byte, error) {
	return f.batches, nil
}

func newTestDatabricksConn() *DatabricksConn {
	conn := &DatabricksConn{}
	conn.setContext(context.Background(), 1)
	return conn
}

func TestVolumeDeleteRetryOn429(t *testing.T) {
	origRetries, origBase := volumeFilesMaxRetries, volumeFilesRetryBase
	volumeFilesMaxRetries = 4
	volumeFilesRetryBase = time.Millisecond
	defer func() {
		volumeFilesMaxRetries = origRetries
		volumeFilesRetryBase = origBase
	}()

	t.Run("retries then succeeds", func(t *testing.T) {
		var deletes int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodDelete, r.Method)
			n := atomic.AddInt32(&deletes, 1)
			if n < 3 {
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte(`{"error_code":"RESOURCE_EXHAUSTED","message":"AWS S3 is throttling requests; try again later."}`))
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		conn := newTestDatabricksConn()
		conn.SetProp("host", strings.TrimPrefix(server.URL, "http://"))
		conn.SetProp("protocol", "http")
		conn.SetProp("token", "tok")

		err := conn.VolumeDelete("/Volumes/workspace/default/sling_volume/sling_temp/file.parquet")
		require.NoError(t, err)
		assert.GreaterOrEqual(t, atomic.LoadInt32(&deletes), int32(3))
	})

	t.Run("404 is success", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		conn := newTestDatabricksConn()
		conn.SetProp("host", strings.TrimPrefix(server.URL, "http://"))
		conn.SetProp("protocol", "http")
		conn.SetProp("token", "tok")

		err := conn.VolumeDelete("/Volumes/workspace/default/sling_volume/gone.parquet")
		assert.NoError(t, err)
	})

	t.Run("403 is not retried", func(t *testing.T) {
		var deletes int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&deletes, 1)
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte(`{"error_code":"PERMISSION_DENIED"}`))
		}))
		defer server.Close()

		conn := newTestDatabricksConn()
		conn.SetProp("host", strings.TrimPrefix(server.URL, "http://"))
		conn.SetProp("protocol", "http")
		conn.SetProp("token", "tok")

		err := conn.VolumeDelete("/Volumes/workspace/default/sling_volume/denied.parquet")
		assert.Error(t, err)
		assert.Equal(t, int32(1), atomic.LoadInt32(&deletes))
	})

	t.Run("exhausted 429", func(t *testing.T) {
		var deletes int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&deletes, 1)
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"error_code":"RESOURCE_EXHAUSTED"}`))
		}))
		defer server.Close()

		conn := newTestDatabricksConn()
		conn.SetProp("host", strings.TrimPrefix(server.URL, "http://"))
		conn.SetProp("protocol", "http")
		conn.SetProp("token", "tok")

		err := conn.VolumeDelete("/Volumes/workspace/default/sling_volume/busy.parquet")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "retries")
		assert.Equal(t, int32(volumeFilesMaxRetries+1), atomic.LoadInt32(&deletes))
	})
}

func TestMapZerobusIPCCompression(t *testing.T) {
	none, err := mapZerobusIPCCompression("none")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionNone, none)
	assert.NotEqual(t, zerobus.IPCCompressionDefault, none)

	empty, err := mapZerobusIPCCompression("")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionNone, empty)

	lz4, err := mapZerobusIPCCompression("lz4")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionLZ4Frame, lz4)

	zstd, err := mapZerobusIPCCompression("zstd")
	require.NoError(t, err)
	assert.Equal(t, zerobus.IPCCompressionZstd, zstd)

	_, err = mapZerobusIPCCompression("gzip")
	assert.Error(t, err)
}

func TestIsZerobusSchemaLag(t *testing.T) {
	assert.False(t, isZerobusSchemaLag(nil))
	assert.False(t, isZerobusSchemaLag(fmt.Errorf("invalid_client")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("Schema comparison failed: Client field 'json_data' does not exist in Delta schema")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("SCHEMA_VALIDATION_FAILED")))
	assert.True(t, isZerobusSchemaLag(fmt.Errorf("FIELD_NOT_IN_TABLE")))
}

func TestZerobusValidateConfig(t *testing.T) {
	conn := newTestDatabricksConn()
	conn.CopyMethod = "zerobus"
	err := conn.validateZerobusConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus_endpoint")

	conn.ZerobusEndpoint = "https://123.zerobus.us-west-2.cloud.databricks.com"
	err = conn.validateZerobusConfig()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "client_id")

	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	assert.NoError(t, conn.validateZerobusConfig())
}

func TestColumnsToZerobusArrowSchema(t *testing.T) {
	cols := iop.Columns{
		{Name: "col_bool", Type: iop.BoolType},
		{Name: "col_tiny", Type: iop.SmallIntType, DbType: "tinyint"},
		{Name: "col_short", Type: iop.SmallIntType, DbType: "smallint"},
		{Name: "col_int", Type: iop.IntegerType},
		{Name: "col_int_tiny", Type: iop.IntegerType, DbType: "tinyint"},
		{Name: "col_bigint", Type: iop.BigIntType},
		{Name: "col_float", Type: iop.FloatType, DbType: "float"},
		{Name: "col_double", Type: iop.FloatType, DbType: "double"},
		{Name: "col_str", Type: iop.StringType},
		{Name: "col_dec", Type: iop.DecimalType, DbPrecision: 18, DbScale: 4},
		{Name: "col_bin", Type: iop.BinaryType},
		{Name: "col_date", Type: iop.DateType},
		{Name: "col_tsz", Type: iop.TimestampzType},
		{Name: "col_ntz", Type: iop.TimestampType, DbType: "timestamp_ntz"},
	}

	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)

	assert.Equal(t, "bool", schema.Field(0).Type.Name())
	assert.Equal(t, "int8", schema.Field(1).Type.Name())
	assert.Equal(t, "int16", schema.Field(2).Type.Name())
	assert.Equal(t, "int32", schema.Field(3).Type.Name())
	assert.Equal(t, "int8", schema.Field(4).Type.Name())
	assert.Equal(t, "int64", schema.Field(5).Type.Name())
	assert.Equal(t, "float32", schema.Field(6).Type.Name())
	assert.Equal(t, "float64", schema.Field(7).Type.Name())
	assert.Equal(t, "large_utf8", schema.Field(8).Type.Name())
	assert.Equal(t, "decimal(18, 4)", schema.Field(9).Type.String())
	assert.Equal(t, "large_binary", schema.Field(10).Type.Name())
	assert.Equal(t, "date32", schema.Field(11).Type.Name())
	assert.Equal(t, "timestamp[us, tz=UTC]", schema.Field(12).Type.String())
	assert.Equal(t, "timestamp[us]", schema.Field(13).Type.String())
}

func TestColumnsToZerobusArrowSchema_Unsupported(t *testing.T) {
	_, err := ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "arr", Type: iop.JsonType, DbType: "array<string>"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported Zerobus type")

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "m", Type: iop.JsonType, DbType: "map<string,int>"},
	})
	assert.Error(t, err)

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "s", Type: iop.JsonType, DbType: "struct<a:int>"},
	})
	assert.Error(t, err)

	_, err = ColumnsToZerobusArrowSchema(iop.Columns{
		{Name: "v", Type: iop.JsonType, DbType: "variant"},
	})
	assert.Error(t, err)
}

func TestColumnsToZerobusArrowSchema_Nullability(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType, Metadata: map[string]string{"is_nullable": "false"}},
		{Name: "name", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "true"}},
	}
	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)
	assert.False(t, schema.Field(0).Nullable)
	assert.True(t, schema.Field(1).Nullable)
}

func TestSerializeRecordToIPC_RoundTrip(t *testing.T) {
	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	schema, err := ColumnsToZerobusArrowSchema(cols)
	require.NoError(t, err)

	schemaBytes, err := SerializeSchemaToIPC(schema)
	require.NoError(t, err)
	assert.NotEmpty(t, schemaBytes)

	mem := memory.NewGoAllocator()
	idB := array.NewInt64Builder(mem)
	nameB := array.NewLargeStringBuilder(mem)
	idB.AppendValues([]int64{1, 2}, nil)
	nameB.AppendValues([]string{"Alice", "Bob"}, nil)
	idA := idB.NewArray()
	nameA := nameB.NewArray()
	defer idA.Release()
	defer nameA.Release()
	rec := array.NewRecord(schema, []arrow.Array{idA, nameA}, 2)
	defer rec.Release()

	for _, compression := range []string{"none", "lz4", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			b, err := SerializeRecordToIPC(schema, rec, compression)
			require.NoError(t, err)
			assert.NotEmpty(t, b)

			r, err := ipc.NewReader(bytes.NewReader(b))
			require.NoError(t, err)
			defer r.Release()
			assert.True(t, r.Next())
			got := r.Record()
			assert.Equal(t, int64(2), got.NumRows())
			assert.False(t, r.Next())
			assert.NoError(t, r.Err())
		})
	}
}

func TestAlignZerobusSource(t *testing.T) {
	src := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	tgt := iop.Columns{
		{Name: "id", Type: iop.BigIntType, Metadata: map[string]string{"nullable": "false"}},
		{Name: "name", Type: iop.StringType},
		{Name: "note", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "true"}},
	}
	idx, err := alignZerobusSource(src, tgt)
	require.NoError(t, err)
	assert.Equal(t, []int{0, 1, -1}, idx)

	_, err = alignZerobusSource(src, iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "missing", Type: iop.StringType, Metadata: map[string]string{"is_nullable": "false"}},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing non-null")

	_, err = alignZerobusSource(iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "extra", Type: iop.StringType},
	}, iop.Columns{{Name: "id", Type: iop.BigIntType}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extra column")
}

func TestCopyViaZerobus_FakeStream(t *testing.T) {
	origOpen := openZerobusStream
	origDescribe := zerobusDescribe
	t.Cleanup(func() {
		openZerobusStream = origOpen
		zerobusDescribe = origDescribe
	})

	fake := &fakeZerobusStream{}
	openZerobusStream = func(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
		assert.Equal(t, "https://123.zerobus.us-west-2.cloud.databricks.com", endpoint)
		assert.Equal(t, "https://dbc.cloud.databricks.com", workspaceURL)
		assert.Equal(t, "main.default.users", tableName)
		assert.NotEmpty(t, schemaIPC)
		assert.Equal(t, zerobus.IPCCompressionNone, opts.IPCCompression)
		return fake, func() {}, nil
	}
	zerobusDescribe = func(conn *DatabricksConn, tableFName string) (iop.Columns, error) {
		return iop.Columns{
			{Name: "id", Type: iop.BigIntType},
			{Name: "name", Type: iop.StringType},
		}, nil
	}

	conn := newTestDatabricksConn()
	conn.CopyMethod = "zerobus"
	conn.ZerobusEndpoint = "123.zerobus.us-west-2.cloud.databricks.com"
	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	conn.BatchSize = 2
	conn.IPCCompression = "none"
	conn.MaxInflightBatches = 1000
	conn.Catalog = "main"
	conn.Schema = "default"
	conn.SetProp("host", "dbc.cloud.databricks.com")

	cols := iop.Columns{
		{Name: "id", Type: iop.BigIntType},
		{Name: "name", Type: iop.StringType},
	}
	data := iop.NewDataset(cols)
	data.Rows = [][]any{
		{int64(1), "Alice"},
		{int64(2), "Bob"},
		{int64(3), "Charlie"},
		{int64(4), nil},
		{int64(5), "Eve"},
	}
	df, err := iop.MakeDataFlow(data.Stream())
	require.NoError(t, err)

	table := Table{Database: "main", Schema: "default", Name: "users"}
	count, err := conn.CopyViaZerobus(table, df)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), count)
	assert.Equal(t, 3, fake.ingestCalled) // batch_size 2 → 2+2+1
	assert.Equal(t, 1, fake.flushCalled)
	assert.Equal(t, 1, fake.closeCalled)
}

func TestCopyViaZerobus_FlushError(t *testing.T) {
	origOpen := openZerobusStream
	origDescribe := zerobusDescribe
	t.Cleanup(func() {
		openZerobusStream = origOpen
		zerobusDescribe = origDescribe
	})

	fake := &fakeZerobusStream{flushErr: assert.AnError}
	openZerobusStream = func(endpoint, workspaceURL, tableName string, schemaIPC []byte, clientID, clientSecret string, opts *zerobus.ArrowStreamConfigurationOptions) (zerobusStream, func(), error) {
		return fake, func() {}, nil
	}
	zerobusDescribe = func(conn *DatabricksConn, tableFName string) (iop.Columns, error) {
		return iop.Columns{
			{Name: "id", Type: iop.BigIntType},
		}, nil
	}

	conn := newTestDatabricksConn()
	conn.ZerobusEndpoint = "https://z.example"
	conn.ClientID = "id"
	conn.ClientSecret = "secret"
	conn.BatchSize = 10
	conn.IPCCompression = "none"
	conn.SetProp("host", "dbc.cloud.databricks.com")

	data := iop.NewDataset(iop.Columns{{Name: "id", Type: iop.BigIntType}})
	data.Rows = [][]any{{int64(1)}}
	df, err := iop.MakeDataFlow(data.Stream())
	require.NoError(t, err)
	_, err = conn.CopyViaZerobus(Table{Name: "t", Schema: "s", Database: "c"}, df)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus flush failed")
	assert.Equal(t, 1, fake.closeCalled)
}

func TestCopyViaZerobus_MissingEndpoint(t *testing.T) {
	conn := newTestDatabricksConn()
	_, err := conn.CopyViaZerobus(Table{Name: "t"}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zerobus_endpoint")
}

func icebergCdcCols() iop.Columns {
	return iop.NewColumnsFromFields("id", "val", "_sling_synced_op", "_sling_cdc_seq")
}

func icebergPkOf(split icebergMergeSplit) (upsertIDs, deleteIDs []any) {
	for _, row := range split.Upserts {
		upsertIDs = append(upsertIDs, row[0])
	}
	for _, row := range split.Deletes {
		deleteIDs = append(deleteIDs, row[0])
	}
	return
}

func newTestIcebergConn(t *testing.T) *IcebergConn {
	t.Helper()
	conn := &IcebergConn{}
	conn.setContext(context.Background(), 1)
	require.NoError(t, conn.Init())
	return conn
}

func icebergConnFor(catalog dbio.IcebergCatalogType, storage icebergStorageKind) *IcebergConn {
	conn := &IcebergConn{CatalogType: catalog}
	conn.setContext(context.Background(), 1)
	switch storage {
	case icebergStorageS3:
		conn.Warehouse = "s3://bucket/wh"
	case icebergStorageGCS:
		conn.Warehouse = "gs://bucket/wh"
	case icebergStorageAzure:
		conn.Warehouse = "abfss://c@acct.dfs.core.windows.net/wh"
	}
	return conn
}

func TestIcebergDedupCDCHigherSeqWins(t *testing.T) {
	st := MergeStrategyChangeCapture
	cols := icebergCdcCols()
	rows := [][]any{
		{1, "a", "U", int64(1)},
		{1, "b", "U", int64(3)},
		{1, "c", "U", int64(2)},
	}
	split, err := newTestIcebergConn(t).dedupMergeRows(cols, rows, []string{"id"}, &st)
	require.NoError(t, err)
	require.Len(t, split.Upserts, 1)
	assert.Equal(t, "b", split.Upserts[0][1])
	assert.Equal(t, uint64(3), split.Count)
}

func TestIcebergDedupCDCUThenD(t *testing.T) {
	st := MergeStrategyChangeCapture
	cols := icebergCdcCols()
	rows := [][]any{
		{1, "a", "U", int64(1)},
		{1, "a", "D", int64(2)},
	}
	split, err := newTestIcebergConn(t).dedupMergeRows(cols, rows, []string{"id"}, &st)
	require.NoError(t, err)
	upsertIDs, deleteIDs := icebergPkOf(split)
	assert.Empty(t, upsertIDs)
	assert.Equal(t, []any{1}, deleteIDs)
}

func TestIcebergDedupCDCDThenU(t *testing.T) {
	st := MergeStrategyChangeCapture
	cols := icebergCdcCols()
	rows := [][]any{
		{1, "a", "D", int64(1)},
		{1, "b", "U", int64(2)},
	}
	split, err := newTestIcebergConn(t).dedupMergeRows(cols, rows, []string{"id"}, &st)
	require.NoError(t, err)
	upsertIDs, deleteIDs := icebergPkOf(split)
	assert.Equal(t, []any{1}, upsertIDs)
	assert.Equal(t, []any{1}, deleteIDs)
	assert.Equal(t, "b", split.Upserts[0][1])
}

func TestIcebergDedupSoftDelete(t *testing.T) {
	st := MergeStrategyChangeCaptureSoft
	cols := icebergCdcCols()
	rows := [][]any{
		{1, "a", "D", int64(5)},
	}
	split, err := newTestIcebergConn(t).dedupMergeRows(cols, rows, []string{"id"}, &st)
	require.NoError(t, err)
	require.Len(t, split.Upserts, 1)
	require.Len(t, split.Deletes, 1)
	assert.Equal(t, "D", split.Upserts[0][2])
}

func TestIcebergDedupIncrementalAllPKsInBothSets(t *testing.T) {
	cols := iop.NewColumnsFromFields("id", "val")
	rows := [][]any{
		{1, "a"},
		{2, "b"},
		{1, "a2"}, // last row wins
	}
	split, err := newTestIcebergConn(t).dedupMergeRows(cols, rows, []string{"id"}, nil)
	require.NoError(t, err)
	require.Len(t, split.Upserts, 2)
	require.Len(t, split.Deletes, 2)

	got := map[any]any{}
	for _, row := range split.Upserts {
		got[row[0]] = row[1]
	}
	assert.Equal(t, "a2", got[1])
	assert.Equal(t, "b", got[2])
}

func TestIcebergDedupMergeWithoutPK(t *testing.T) {
	_, err := newTestIcebergConn(t).dedupMergeRows(icebergCdcCols(), [][]any{{1, "a", "I", int64(1)}}, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "primary-key")
}

func TestIcebergMergeEngineRouter(t *testing.T) {
	tests := []struct {
		name        string
		needMerge   bool
		catalog     dbio.IcebergCatalogType
		storage     icebergStorageKind
		goHas       bool
		want        icebergMergeEngine
		wantErr     bool
		neverDuckDB bool
	}{
		{"append no merge", false, dbio.IcebergCatalogTypeREST, icebergStorageS3, true, icebergMergeEngineAppend, false, false},
		{"sql catalog go", true, dbio.IcebergCatalogTypeSQL, icebergStorageS3, true, icebergMergeEngineGo, false, true},
		{"sql catalog old fork", true, dbio.IcebergCatalogTypeSQL, icebergStorageS3, false, "", true, true},
		{"azure go", true, dbio.IcebergCatalogTypeREST, icebergStorageAzure, true, icebergMergeEngineGo, false, true},
		{"azure old fork", true, dbio.IcebergCatalogTypeREST, icebergStorageAzure, false, "", true, true},
		{"rest old fork duckdb", true, dbio.IcebergCatalogTypeREST, icebergStorageS3, false, icebergMergeEngineDuckDB, false, false},
		{"s3tables old fork duckdb", true, dbio.IcebergCatalogTypeS3Tables, icebergStorageS3, false, icebergMergeEngineDuckDB, false, false},
		{"glue go never duckdb", true, dbio.IcebergCatalogTypeGlue, icebergStorageS3, true, icebergMergeEngineGo, false, true},
		{"glue old fork error", true, dbio.IcebergCatalogTypeGlue, icebergStorageS3, false, "", true, true},
		{"gcs rest old fork duckdb", true, dbio.IcebergCatalogTypeREST, icebergStorageGCS, false, icebergMergeEngineDuckDB, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := icebergConnFor(tt.catalog, tt.storage).mergeEngineWith(tt.needMerge, tt.goHas)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "Iceberg merge is not supported")
				assert.NotEqual(t, icebergMergeEngineDuckDB, got)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			if tt.neverDuckDB {
				assert.NotEqual(t, icebergMergeEngineDuckDB, got)
			}
		})
	}
}

func TestIcebergStorageKind(t *testing.T) {
	assert.Equal(t, icebergStorageS3, icebergConnFor("", icebergStorageS3).icebergStorageKind())
	assert.Equal(t, icebergStorageGCS, icebergConnFor("", icebergStorageGCS).icebergStorageKind())
	assert.Equal(t, icebergStorageAzure, icebergConnFor("", icebergStorageAzure).icebergStorageKind())

	azure := &IcebergConn{}
	azure.setContext(context.Background(), 1)
	azure.SetProp("azure_account_name", "acct")
	assert.Equal(t, icebergStorageAzure, azure.icebergStorageKind())

	s3 := &IcebergConn{Warehouse: "warehouse1"}
	s3.setContext(context.Background(), 1)
	s3.SetProp("s3_region", "us-east-1")
	assert.Equal(t, icebergStorageS3, s3.icebergStorageKind())
}

func TestIcebergGoHasRowDelta(t *testing.T) {
	assert.True(t, icebergGoHasRowDelta(), "apache iceberg-go v0.6+ should expose NewRowDelta")
}

func TestIcebergDuckMergeSQLFromTemplates(t *testing.T) {
	conn := newTestIcebergConn(t)
	tgt := `iceberg_catalog."sling_test"."t"`
	src := "sling_merge_src"
	cols := iop.NewColumnsFromFields("id", "val")

	sqls, err := conn.duckMergeSQL(tgt, src, cols, []string{"id"}, MergeStrategyNone)
	require.NoError(t, err)
	joined := strings.Join(sqls, "\n")
	assert.Contains(t, joined, `DELETE FROM iceberg_catalog."sling_test"."t"`)
	assert.Contains(t, joined, `INSERT INTO iceberg_catalog."sling_test"."t"`)
	assert.Contains(t, joined, `src."id" = tgt."id"`)

	sqls, err = conn.duckMergeSQL(tgt, src, cols, []string{"id"}, MergeStrategyInsert)
	require.NoError(t, err)
	assert.Contains(t, strings.Join(sqls, "\n"), "WHERE NOT EXISTS")

	sqls, err = conn.duckMergeSQL(tgt, src, icebergCdcCols(), []string{"id"}, MergeStrategyChangeCapture)
	require.NoError(t, err)
	joined = strings.Join(sqls, "\n")
	assert.Contains(t, joined, "_sling_cdc_seq")
	assert.Contains(t, joined, "_sling_synced_op != 'D'")

	sqls, err = conn.duckMergeSQL(tgt, src, icebergCdcCols(), []string{"id"}, MergeStrategyChangeCaptureSoft)
	require.NoError(t, err)
	joined = strings.Join(sqls, "\n")
	assert.Contains(t, joined, "_sling_synced_op = 'D'")
	assert.Contains(t, joined, "CURRENT_TIMESTAMP")

	_, err = conn.duckMergeSQL(tgt, src, cols, nil, MergeStrategyDeleteInsert)
	require.Error(t, err)
}

func newTestLanceDBConn(t *testing.T, props map[string]string) *LanceDBConn {
	t.Helper()
	conn := &LanceDBConn{}
	conn.setContext(context.Background(), 1)
	for k, v := range props {
		conn.SetProp(k, v)
	}
	return conn
}

// The namespace root comes from `path`, with `instance` as an alias. It must
// never reach DuckDB as the process's database file.
func TestLanceDBConnNamespaceRoot(t *testing.T) {
	err := newTestLanceDBConn(t, nil).Init()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "'path'")

	conn := newTestLanceDBConn(t, map[string]string{"instance": "/data/lancedb"})
	require.NoError(t, conn.Init())
	assert.Equal(t, "/data/lancedb", conn.Path)
	assert.Empty(t, conn.GetProp("instance"))
	assert.Equal(t, "ATTACH IF NOT EXISTS '/data/lancedb' AS lancedb (TYPE lance)", conn.buildAttachSQL())
}

// The DuckDB CLI cannot open an in-memory database read-only, and the lance
// extension rejects a READ_ONLY attach, so the property is refused up front.
func TestLanceDBConnReadOnlyRefused(t *testing.T) {
	conn := newTestLanceDBConn(t, map[string]string{"path": "/data/lancedb", "read_only": "true"})
	err := conn.Init()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read_only")
}

func TestLanceDBConnAttachSQLQuoting(t *testing.T) {
	conn := newTestLanceDBConn(t, map[string]string{"path": "/data/my'db"})
	require.NoError(t, conn.Init())
	assert.Equal(t, "ATTACH IF NOT EXISTS '/data/my''db' AS lancedb (TYPE lance)", conn.buildAttachSQL())
}

// Only the schemes the lance extension serves are mapped to a secret family;
// everything else is passed through so the extension reports it.
func TestLanceDBConnObjectStore(t *testing.T) {
	tests := []struct {
		path        string
		scheme      string
		scope       string
		objectStore bool
	}{
		{path: "/data/lancedb", scheme: "", scope: "/data/lancedb"},
		{path: "./data/lancedb", scheme: "", scope: "./data/lancedb"},
		{path: "file:///data/lancedb", scheme: "file", scope: "file:///"},
		{path: "s3://my-bucket/lancedb", scheme: "s3", scope: "s3://my-bucket/", objectStore: true},
		{path: "s3a://my-bucket/nested/lancedb", scheme: "s3", scope: "s3a://my-bucket/", objectStore: true},
		{path: "gs://my-bucket/lancedb", scheme: "gs", scope: "gs://my-bucket/", objectStore: true},
		{
			path:        "abfss://container@acct.dfs.core.windows.net/lancedb",
			scheme:      "az",
			scope:       "abfss://container@acct.dfs.core.windows.net/",
			objectStore: true,
		},
		{path: "r2://my-bucket/lancedb", scheme: "r2", scope: "r2://my-bucket/", objectStore: true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			conn := newTestLanceDBConn(t, map[string]string{"path": tt.path})
			require.NoError(t, conn.Init())
			assert.Equal(t, tt.scheme, conn.objectStoreScheme())
			assert.Equal(t, tt.objectStore, conn.isObjectStore())
			assert.Equal(t, tt.scope, conn.lanceScope())
		})
	}
}

// A secret is built for the object stores whose credentials sling can supply.
// The keys are the ones the lance extension's secret provider accepts, and the
// scope covers the bucket so that every dataset below it matches.
func TestLanceDBConnSecrets(t *testing.T) {
	conn := newTestLanceDBConn(t, map[string]string{
		"path":                 "s3://my-bucket/lancedb",
		"s3_access_key_id":     "AKIAEXAMPLE",
		"s3_secret_access_key": "secret",
		"s3_session_token":     "token",
		"s3_region":            "us-east-1",
		"s3_endpoint":          "http://localhost:9000",
	})
	require.NoError(t, conn.Init())

	secret, ok := conn.makeSecret()
	require.True(t, ok)
	assert.Equal(t, "lance_secret", secret.Name)
	assert.Equal(t, iop.DuckDbSecretType("lance"), secret.Type)
	assert.Equal(t, "s3://my-bucket/", secret.Props["scope"])
	assert.Equal(t, "config", secret.Props["provider"])
	assert.Equal(t, "AKIAEXAMPLE", secret.Props["access_key_id"])
	assert.Equal(t, "secret", secret.Props["secret_access_key"])
	assert.Equal(t, "token", secret.Props["session_token"])
	assert.Equal(t, "us-east-1", secret.Props["region"])
	assert.Equal(t, "http://localhost:9000", secret.Props["endpoint"])
	assert.Equal(t, "true", secret.Props["allow_http"])

	// without explicit keys, the upstream credential chain is used
	fallback := newTestLanceDBConn(t, map[string]string{"path": "s3://my-bucket/lancedb"})
	require.NoError(t, fallback.Init())
	secret, ok = fallback.makeSecret()
	require.True(t, ok)
	assert.Equal(t, "credential_chain", secret.Props["provider"])
	assert.NotContains(t, secret.Props, "access_key_id")

	azure := newTestLanceDBConn(t, map[string]string{
		"path":                "az://my-container/lancedb",
		"azure_account_name":  "acct",
		"azure_account_key":   "a2V5",
		"azure_sas_token":     "?sv=2024",
		"azure_tenant_id":     "tenant",
		"azure_client_id":     "client",
		"azure_client_secret": "client-secret",
	})
	require.NoError(t, azure.Init())
	secret, ok = azure.makeSecret()
	require.True(t, ok)
	assert.Equal(t, "az://my-container/", secret.Props["scope"])
	assert.Equal(t, "acct", secret.Props["account_name"])
	assert.Equal(t, "a2V5", secret.Props["account_key"])

	// SAS is only used when there is no account key
	sas := newTestLanceDBConn(t, map[string]string{
		"path":               "abfss://container@acct.dfs.core.windows.net/lancedb",
		"azure_account_name": "acct",
		"azure_sas_token":    "?sv=2024",
	})
	require.NoError(t, sas.Init())
	secret, ok = sas.makeSecret()
	require.True(t, ok)
	assert.Equal(t, "abfss://container@acct.dfs.core.windows.net/", secret.Props["scope"])
	assert.Equal(t, "sv=2024", secret.Props["sas_token"])
	assert.NotContains(t, secret.Props, "account_key")

	gcs := newTestLanceDBConn(t, map[string]string{
		"path":         "gs://my-bucket/lancedb",
		"gcs_key_file": "/tmp/gcs.json",
	})
	require.NoError(t, gcs.Init())
	secret, ok = gcs.makeSecret()
	require.True(t, ok)
	assert.Equal(t, "config", secret.Props["provider"])
	assert.Equal(t, "/tmp/gcs.json", secret.Props["google_application_credentials"])
}

// Object stores outside the s3 / gs / az families resolve their own
// credentials, so no secret is registered for them.
func TestLanceDBConnSecretSkippedForOtherStores(t *testing.T) {
	for _, path := range []string{"oss://my-bucket/lancedb", "hf://datasets/org/repo", "r2://my-bucket/lancedb"} {
		conn := newTestLanceDBConn(t, map[string]string{"path": path})
		require.NoError(t, conn.Init())
		_, ok := conn.makeSecret()
		assert.False(t, ok, path)
	}
}

// dBase fixtures under test/dbf:
//
//	TEST.DBF + TEST.FPT
//	  FoxPro (0x32) with every field type, a memo field and one deleted record.
//	  https://github.com/Valentin-Kaiser/go-dbase (BSD-3-Clause)
//	expense categories.dbf
//	  FoxPro, table name containing a space.
//	  https://github.com/Valentin-Kaiser/go-dbase (BSD-3-Clause)
//	dbase_03.dbf
//	  dBase III (0x03): 14 records, blank numeric / date fields.
//	dbase_8b.dbf + dbase_8b.dbt
//	  dBase IV (0x8B) with a `.dbt` memo, which the reader cannot open.
//	  https://github.com/infused/dbf (MIT), dbase_03.dbf and dbase_8b.dbf
//	nullable.dbf
//	  written with the reader library, holding nullable and variable length
//	  varchar / varbinary fields, with a null record.
const dbfTestDir = "test/dbf"

func dbfConn(t *testing.T, path string) Connection {
	t.Helper()

	conn, err := NewConn("dbase://" + path)
	require.NoError(t, err)
	require.NoError(t, conn.Connect())
	return conn
}

func TestDbaseConnectionURL(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/TEST.DBF")

	assert.Equal(t, dbio.TypeDbDBase, conn.GetType())
	assert.Equal(t, "main", conn.GetProp("schema"))
	assert.Equal(t, dbfTestDir+"/TEST.DBF", conn.GetProp("path"))
}

func TestDbasePathFromURL(t *testing.T) {
	for url, expected := range map[string]string{
		"dbase:///data/tables":             "/data/tables",
		"dbase://./tables":                 "./tables",
		"dbase://tables":                   "tables",
		"dbase://relative/tables":          "relative/tables",
		"dbase:///data/my%20tables/x.dbf":  "/data/my tables/x.dbf",
		"DBF:///data/tables":               "/data/tables",
		"/data/tables":                     "/data/tables",
		"dbase:///data/100%25%20tables":    "/data/100% tables",
		"dbase:///data/tables?x=1&y=2":     "/data/tables?x=1&y=2",
		"dbase:///data/back%2Fslash/x.dbf": "/data/back/slash/x.dbf",
		"dbase://":                         "",
	} {
		assert.Equal(t, expected, DbasePathFromURL(url), url)
	}
}

func TestDbaseColumns(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/dbase_03.dbf")

	columns, err := conn.GetColumns(`"dbase_03"`)
	require.NoError(t, err)
	require.Len(t, columns, 31)

	for _, tc := range []struct {
		position int // Point_ID is defined twice, so columns are checked by position
		name     string
		colType  iop.ColumnType
		dbType   string
		prec     int
		scale    int
	}{
		{1, "Point_ID", iop.TextType, "character(12)", 0, 0},
		{8, "Comments", iop.TextType, "character(60)", 0, 0},
		{9, "Date_Visit", iop.DateType, "date", 0, 0},
		{11, "Max_PDOP", iop.DecimalType, "numeric(5,1)", 5, 1},
		{20, "Unfilt_Pos", iop.BigIntType, "numeric", 10, 0},
		{24, "GPS_Second", iop.DecimalType, "numeric(12,3)", 12, 3},
		{28, "Std_Dev", iop.DecimalType, "numeric(16,6)", 16, 6},
		{31, "Point_ID1", iop.BigIntType, "numeric", 9, 0}, // the repeated name is suffixed
	} {
		col := columns[tc.position-1]
		assert.Equal(t, tc.name, col.Name)
		assert.Equal(t, tc.position, col.Position)
		assert.Equal(t, tc.colType, col.Type, tc.name)
		assert.Equal(t, tc.dbType, col.DbType, tc.name)
		assert.Equal(t, tc.prec, col.DbPrecision, tc.name)
		assert.Equal(t, tc.scale, col.DbScale, tc.name)
		assert.Equal(t, "dbase_03", col.Table, tc.name)
	}

	// sling cannot tell repeated column names apart, so the second one is
	// suffixed as the file readers do with repeated headers
	assert.Nil(t, columns.GetColumn("Point_ID2"))
	data, err := conn.Query(`select "Point_ID", "Point_ID1" from "dbase_03" limit 2`)
	require.NoError(t, err)
	assert.Equal(t, []any{"0507121", int64(401)}, data.Rows[0])
	assert.Equal(t, []any{"0507122", int64(402)}, data.Rows[1])
}

func TestDbaseRows(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/dbase_03.dbf")

	data, err := conn.Query(`select * from "dbase_03"`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 14)

	// decimal values are kept as strings, as sling does for every connection
	row := data.Rows[0]
	assert.Equal(t, "0507121", row[0])
	assert.Equal(t, "CMP", row[1])
	assert.Equal(t, time.Date(2005, 7, 12, 0, 0, 0, 0, time.UTC), row[8])
	assert.Equal(t, "5.2", row[10])
	assert.Equal(t, int64(2), row[19])
	assert.Equal(t, "226625", row[23])
	assert.Equal(t, "1131.323", row[24])
	assert.Equal(t, "0.897088", row[27])

	// a blank number is stored as spaces and is not a zero
	assert.Nil(t, data.Rows[1][27])
	assert.Nil(t, data.Rows[13][27])
	// a blank character field is an empty string
	assert.Equal(t, "", data.Rows[0][4])
}

func TestDbaseFoxProFieldTypes(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/TEST.DBF")

	columns, err := conn.GetColumns(`"TEST"`)
	require.NoError(t, err)
	require.Len(t, columns, 16)

	for _, tc := range []struct {
		name    string
		colType iop.ColumnType
		dbType  string
	}{
		{"PRODUCTID", iop.IntegerType, "integer"},
		{"PRODNAME", iop.TextType, "character(20)"},
		{"PRICE", iop.DecimalType, "currency"},
		{"DOUBLE", iop.DecimalType, "double"},
		{"DATE", iop.DateType, "date"},
		{"DATETIME", iop.DatetimeType, "datetime"},
		{"INTEGER", iop.DecimalType, "float(4,2)"},
		{"FLOAT", iop.IntegerType, "integer"},
		{"ACTIVE", iop.BoolType, "logical"},
		{"DESC", iop.TextType, "memo(4)"},
		{"TAX", iop.DecimalType, "numeric(8,2)"},
		{"INSTOCK", iop.BigIntType, "numeric"},
		{"BLOB", iop.BinaryType, "blob(4)"},
		{"VARBIN_NIL", iop.BinaryType, "varbinary(10)"},
		{"VAR_NIL", iop.TextType, "varchar(254)"},
		{"VAR", iop.TextType, "varchar(10)"},
	} {
		col := columns.GetColumn(tc.name)
		require.NotNil(t, col, tc.name)
		assert.Equal(t, tc.colType, col.Type, tc.name)
		assert.Equal(t, tc.dbType, col.DbType, tc.name)
	}

	// the table holds 3 records, one of which is deleted
	count, err := conn.GetCount(`"TEST"`)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	data, err := conn.Query(`select * from "TEST"`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 2)

	row := data.Rows[0]
	assert.Equal(t, int64(1), row[0])
	assert.Equal(t, "TEST PRODUCT", row[1])
	assert.Equal(t, "12.3456", row[2])
	assert.Equal(t, "78.9", row[3])
	assert.Equal(t, time.Date(2022, 4, 10, 0, 0, 0, 0, time.UTC), row[4])
	assert.Equal(t, "true", row[8])                // booleans are kept as strings, as sling does
	assert.Equal(t, "PRODUCT DESCRIPTION", row[9]) // memo, read from the .fpt file
	assert.Equal(t, "19.99", row[10])
	assert.Equal(t, int64(1), row[11])
	assert.Nil(t, row[12]) // blank blob
	assert.Equal(t, []byte{17, 34, 51, 68, 85, 102, 119, 136, 153, 170}, row[13])
	assert.Equal(t, "Test value with variable length", row[14])
	assert.Equal(t, "", row[15])

	// the second record has a shorter varbinary
	assert.Equal(t, []byte{170, 187, 204}, data.Rows[1][13])
}

func TestDbaseTablesInFolder(t *testing.T) {
	conn := dbfConn(t, dbfTestDir)

	tables, err := conn.GetTables("main")
	require.NoError(t, err)
	assert.ElementsMatch(t,
		[]string{"TEST", "dbase_03", "dbase_8b", "expense categories", "nullable", "test1k_dbase"},
		tables.ColValuesStr(1),
	)

	schemata, err := conn.GetSchemata(SchemataLevelColumn, "main")
	require.NoError(t, err)
	assert.Len(t, schemata.Tables(), 6)
	assert.Equal(t, "expense categories", schemata.Tables()["main.main.expense categories"].Name)
	assert.Len(t, schemata.Tables()["main.main.expense categories"].Columns, 3)

	// the file extension is not part of the table name, and names are matched
	// without case
	exists, err := conn.TableExists(Table{Name: "test.dbf", Dialect: conn.GetType()})
	require.NoError(t, err)
	assert.False(t, exists)

	exists, err = conn.TableExists(Table{Name: "Test", Dialect: conn.GetType()})
	require.NoError(t, err)
	assert.True(t, exists)

	count, err := conn.GetCount(`"expense categories"`)
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	data, err := conn.Query(`select * from "expense categories"`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 5)
	assert.Equal(t, []any{int64(1), "Meals", int64(500)}, data.Rows[0])
}

func TestDbaseSingleFileRoot(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/TEST.DBF")

	tables, err := conn.GetTables("main")
	require.NoError(t, err)
	assert.Equal(t, []string{"TEST"}, tables.ColValuesStr(1))

	data, err := conn.Query(`select "PRODNAME" from "TEST"`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 2)
	assert.Equal(t, "TEST PRODUCT", data.Rows[0][0])
}

func TestDbaseStatements(t *testing.T) {
	conn := dbfConn(t, dbfTestDir)

	for _, tc := range []struct {
		sql   string
		rows  int
		cols  int
		first string // first column of the first row
	}{
		{`select * from "dbase_03" limit 2`, 2, 31, "0507121"},
		{`select * from "dbase_03" limit 3 offset 12`, 2, 31, "05071232"},
		{`select "Point_ID", "Max_PDOP" from "dbase_03"`, 14, 2, "0507121"},
		{`select "Point_ID" as "pid" from "dbase_03"`, 14, 1, "0507121"},
		{`select * from "dbase_03" where 1=0`, 0, 31, ""},
		{`select * from "main"."main"."dbase_03" limit 1`, 1, 31, "0507121"},
		// a derived table, as generated when a limit is applied to a query
		{"select * from (\n  select \"Point_ID\" from \"dbase_03\" limit 4\n) as t limit 2 offset 1", 2, 1, "0507122"},
	} {
		data, err := conn.Query(tc.sql)
		require.NoError(t, err, tc.sql)
		assert.Len(t, data.Rows, tc.rows, tc.sql)
		assert.Len(t, data.Columns, tc.cols, tc.sql)
		if tc.rows > 0 {
			assert.Equal(t, tc.first, data.Rows[0][0], tc.sql)
		}
	}

	// the statement sling generates for a table read with a limit and offset
	table := Table{Name: "dbase_03", Schema: "main", Database: "main", Dialect: conn.GetType()}
	data, err := conn.Query(table.Select(SelectOptions{
		Fields: []string{"Point_ID"},
		Limit:  g.Ptr(3),
		Offset: 2,
	}))
	require.NoError(t, err)
	require.Len(t, data.Rows, 3)
	assert.Equal(t, []string{"Point_ID"}, data.Columns.Names())
	assert.Equal(t, "0507123", data.Rows[0][0])

	// a projection over a derived table
	data, err = conn.Query("select \"Max_PDOP\" from (select * from \"dbase_03\") t")
	require.NoError(t, err)
	require.Len(t, data.Rows, 14)
	assert.Equal(t, "Max_PDOP", data.Columns[0].Name)

	// dBase has no query engine: anything else is rejected
	for _, sql := range []string{
		`select * from "dbase_03" where "Max_PDOP" > 5`,
		`select count(*) from "dbase_03"`,
		`select * from "dbase_03" order by "Point_ID"`,
		`select * from "dbase_03" join "TEST" on 1=1`,
		`select "Point_ID" * 2 from "dbase_03"`,
		`select * from "nope"`,
		`insert into "dbase_03" values (1)`,
		`select 1`,
		`select * from "dbase_03" limit abc`,
	} {
		_, err := conn.Query(sql)
		assert.Error(t, err, sql)
	}

	// the missing table is reported with the available ones
	_, err = conn.Query(`select * from "nope"`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dbase_03")

	// writing is not supported
	_, err = conn.ExecContext(context.Background(), `insert into "dbase_03" values (1)`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
}

func TestDbaseUnsupportedMemoFile(t *testing.T) {
	conn := dbfConn(t, dbfTestDir)

	// the columns are readable, the records are not
	columns, err := conn.GetColumns(`"dbase_8b"`)
	require.NoError(t, err)
	require.Len(t, columns, 6)
	assert.Equal(t, iop.TextType, columns.GetColumn("MEMO").Type)

	_, err = conn.Query(`select * from "dbase_8b"`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dbase_8b.dbt")
}

func TestDbaseVariableLengthFields(t *testing.T) {
	conn := dbfConn(t, dbfTestDir+"/nullable.dbf")

	columns, err := conn.GetColumns(`"nullable"`)
	require.NoError(t, err)
	require.Len(t, columns, 5)

	data, err := conn.Query(`select * from "NULLABLE"`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 3)

	// a value shorter than the field is stored with its length in the last
	// byte of the field, flagged in the record's null flag field
	assert.Equal(t, []any{int64(1), "alpha", "notes one", []byte{1, 2, 3}, "12.34"}, data.Rows[0])
	// a value marked as null, and one that fills its field entirely
	assert.Equal(t, []any{int64(2), nil, "second", nil, "0"}, data.Rows[1])
	assert.Equal(t, []any{int64(3), "full length name 123", "third", []byte{1, 2, 3, 4, 5, 6, 7, 8}, "7"}, data.Rows[2])
}

func TestDbaseTrimSpacesProp(t *testing.T) {
	path, err := filepath.Abs(dbfTestDir + "/TEST.DBF")
	require.NoError(t, err)
	_, err = os.Stat(path)
	require.NoError(t, err)

	conn, err := NewConn("dbase://" + path)
	require.NoError(t, err)
	conn.SetProp("trim_spaces", "false")
	require.NoError(t, conn.Connect())

	data, err := conn.Query(`select "PRODNAME" from "TEST" limit 1`)
	require.NoError(t, err)
	require.Len(t, data.Rows, 1)
	assert.Equal(t, "TEST PRODUCT        ", data.Rows[0][0])
}
