package database

import (
	"context"
	"io"
	"log"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
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
	env.SetHomeDir("sling")
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
