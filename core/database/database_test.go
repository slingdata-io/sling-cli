package database

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	h "github.com/flarco/gutil"
	"github.com/slingdata/sling/core/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/xo/dburl"
	"syreclabs.com/go/faker"
	// "github.com/gobuffalo/packr"
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
	"postgres": &testDB{
		name:        "postgres",
		URL:         os.Getenv("POSTGRES_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view place_vw as select * from place where telcode = 65`,
		placeVwSelect: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
	},

	"sqlite3": &testDB{
		name:   "sqlite3",
		URL:    "file:./test.db",
		schema: "main",

		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE \"place\" (\"country\" varchar(255),\"city\" varchar(255),\"telcode\" bigint )",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "CREATE VIEW place_vw as select * from place where telcode = 65",
	},

	"mysql": &testDB{
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

	"azuresql": &testDB{
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

	"azuredwh": &testDB{
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

	"sqlserver": &testDB{
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

	"oracle": &testDB{
		name:        "oracle",
		URL:         "ORACLE_URL",
		schema:      "system",
		transactDDL: `CREATE TABLE transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "\n  CREATE TABLE \"SYSTEM\".\"PLACE\" \n   (\t\"COUNTRY\" VARCHAR2(255), \n\t\"CITY\" VARCHAR2(255), \n\t\"TELCODE\" NUMBER(*,0)\n   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING\n  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645\n  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)\n  TABLESPACE \"SYSTEM\" ",
		placeIndex: `CREATE INDEX idx_country_city 
		ON place(country, city)`,
		placeVwDDL:    "CREATE VIEW place_vw as select * from place where telcode = 65",
		placeVwSelect: "select \"COUNTRY\",\"CITY\",\"TELCODE\" from place where telcode = 65",
	},

	"redshift": &testDB{
		name:        "redshift",
		URL:         os.Getenv("REDSHIFT_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE public.person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    \"country\" text NULL,\n    \"city\" text NULL,\n    \"telcode\" bigint NULL\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view public.place_vw as select * from place where telcode = 65`,
		placeVwSelect: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
		propStrs: []string{
			"AWS_BUCKET=" + os.Getenv("AWS_BUCKET"),
		},
	},

	"bigquery": &testDB{
		name:        "bigquery",
		URL:         os.Getenv("BIGQUERY_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time datetime, description string, original_description string, amount numeric, transaction_type string, category string, account_name string, labels string, notes string )`,
		personDDL:   `CREATE TABLE public.person (first_name string, last_name string, email string )`,
		placeDDL:    "CREATE TABLE public.place\n(\n    country string,\n    city string,\n    telcode int64\n)",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    "create or replace view public.place_vw as select * from `proven-cider-633.public.place` where telcode = 65",
		placeVwSelect: " SELECT place.country,\n    place.city,\n    place.telcode\n   FROM place\n  WHERE (place.telcode = 65);",
		propStrs: []string{
			"PROJECT_ID=proven-cider-633",
			"schema=public",
			"GC_CRED_FILE=/__/devbox/slingelt-prod-10fbedc838ea.json",
		},
	},

	"snowflake": &testDB{
		name:        "snowflake",
		URL:         os.Getenv("SNOWFLAKE_URL"),
		schema:      "public",
		transactDDL: `CREATE TABLE public.transact (date_time date, description varchar(255), original_description varchar(255), amount decimal(10,5), transaction_type varchar(255), category varchar(255), account_name varchar(255), labels varchar(255), notes varchar(255) )`,
		personDDL:   `CREATE TABLE public.person (first_name varchar(255), last_name varchar(255), email varchar(255), CONSTRAINT person_first_name PRIMARY KEY (first_name) )`,
		placeDDL:    "create or replace TABLE PLACE (\n\tCOUNTRY VARCHAR(16777216),\n\tCITY VARCHAR(16777216),\n\tTELCODE NUMBER(38,0)\n);",
		placeIndex: `CREATE INDEX idx_country_city
		ON place(country, city)`,
		placeVwDDL:    `create or replace view public.place_vw as select * from place where telcode = 65`,
		placeVwSelect: "\ncreate or replace view PLACE_VW as select * from place where telcode = 65;\n",
		// propStrs: []string{
		// 	"schema=public",
		// 	"warehouse=COMPUTE_WH",
		// 	"internalStage=stage_loading",
		// 	"AWS_BUCKET=" + os.Getenv("AWS_BUCKET"),
		// },
	},
}

func TestPostgres(t *testing.T) {
	t.Parallel()
	db := DBs["postgres"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func TestSQLite(t *testing.T) {
	t.Parallel()
	dbPath := strings.ReplaceAll(DBs["sqlite3"].URL, "file:", "")
	os.Remove(dbPath)
	db := DBs["sqlite3"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
	os.Remove(dbPath)
}

func TestMySQL(t *testing.T) {
	t.Parallel()
	db := DBs["mysql"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func TestSnowflake(t *testing.T) {
	t.Parallel()
	db := DBs["snowflake"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func TestOracle(t *testing.T) {
	db := DBs["oracle"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func TestRedshift(t *testing.T) {
	db := DBs["redshift"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func TestSqlServer(t *testing.T) {
	t.Parallel()
	db := DBs["sqlserver"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)

	return

	db = DBs["azuresql"]
	conn, err = NewConn(db.URL)
	assert.NoError(t, err)

	err = conn.Connect()
	assert.NoError(t, err)

	db = DBs["azuredwh"]
	conn, err = NewConn(db.URL)
	assert.NoError(t, err)

	err = conn.Connect()
	assert.NoError(t, err)
}

func TestBigQuery(t *testing.T) {
	t.Parallel()
	db := DBs["bigquery"]
	conn, err := connect(db)
	assert.NoError(t, err)
	DBTest(t, db, conn)
}

func connect(db *testDB) (conn Connection, err error) {
	conn, err = NewConn(db.URL)
	if err != nil {
		return
	}
	err = conn.Connect()
	return
}

func DBTest(t *testing.T, db *testDB, conn Connection) {
	h.Info("Testing " + conn.GetType().String())

	err := conn.DropTable(db.schema+".person", db.schema+".place", db.schema+".transact")
	assert.NoError(t, err)

	err = conn.DropView(db.schema + ".place_vw")
	assert.NoError(t, err)

	// gConn, err := conn.GetGormConn()
	// assert.NoError(t, err)
	// gConn.SingularTable(true)
	// gConn.AutoMigrate(&person{}, &place{}, &transact{})

	conn.MustExec(db.transactDDL)
	conn.MustExec(db.personDDL)
	conn.MustExec(db.placeDDL)
	conn.MustExec(db.placeVwDDL)
	if !strings.Contains("redshift,bigquery,snowflake", db.name) {
		conn.MustExec(db.placeIndex)
	}

	personColumns, err := conn.GetColumns(db.schema + ".person")
	assert.NoError(t, err)
	placeColumns, err := conn.GetColumns(db.schema + ".place")
	assert.NoError(t, err)
	transactColumns, err := conn.GetColumns(db.schema + ".transact")
	assert.NoError(t, err)

	insFields, err := conn.ValidateColumnNames(ColumnNames(personColumns), []string{"first_name", "last_name", "email"}, true)
	assert.NoError(t, err)
	personInsertStatement := conn.GenerateInsertStatement(
		"person",
		insFields,
		1,
	)

	insFields, err = conn.ValidateColumnNames(ColumnNames(placeColumns), []string{"country", "city", "telcode"}, true)
	assert.NoError(t, err)
	placeInsertStatement := conn.GenerateInsertStatement(
		"place",
		insFields,
		1,
	)

	insFields, err = conn.ValidateColumnNames(ColumnNames(transactColumns), []string{"date_time", "description", "amount"}, true)
	assert.NoError(t, err)
	transactInsertStatement := conn.GenerateInsertStatement(
		"transact",
		insFields,
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
	// assert.NoError(t, err)

	// recs := []map[string]interface{}{}
	// for rec := range streamRec {
	// 	recs = append(recs, rec)
	// }
	// assert.Len(t, recs, 2)

	stream, err := conn.StreamRows(`select * from person`)
	assert.NoError(t, err)

	rows := [][]interface{}{}
	for row := range stream.Rows {
		rows = append(rows, row)
	}
	assert.Len(t, rows, 2)

	data, err := conn.Query(`select * from person`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)

	data, err = conn.Query(`select * from place`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)

	data, err = conn.Query(`select * from transact`)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []interface{}{65.657, 5.567, 5.657}, cast.ToFloat64(data.Records()[0]["amount"]))

	// GetSchemas
	data, err = conn.GetSchemas()
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetTables
	data, err = conn.GetTables(db.schema)
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)

	// GetViews
	data, err = conn.GetViews(db.schema)
	assert.NoError(t, err)
	assert.Greater(t, len(data.Rows), 0)
	assert.Greater(t, data.Duration, 0.0)

	// GetColumns
	columns, err := conn.GetColumns(db.schema + ".person")
	assert.NoError(t, err)
	assert.Len(t, columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "VARCHAR2", "character varying", "varchar", "TEXT", "STRING"}, columns[0].DbType)

	// GetPrimaryKeys
	if !strings.Contains("redshift,bigquery,snowflake", db.name) {
		data, err = conn.GetPrimaryKeys(db.schema + ".person")
		assert.NoError(t, err)
		assert.Len(t, data.Rows, 1)
		assert.Equal(t, "first_name", strings.ToLower(cast.ToString(data.Records()[0]["column_name"])))
	}

	// GetIndexes
	if !strings.Contains("redshift,bigquery,azuredwh,snowflake,sqlite3", db.name) {
		data, err = conn.GetIndexes(db.schema + ".place")
		assert.NoError(t, err)
		assert.Len(t, data.Rows, 2)
		assert.Equal(t, "city", strings.ToLower(cast.ToString(data.Records()[1]["column_name"])))
	}

	// GetColumnsFull
	data, err = conn.GetColumnsFull(db.schema + ".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.Contains(t, []string{"bigint", "NUMBER", "decimal", "INT64"}, data.Records()[2]["data_type"])

	// GetDDL of table
	if !strings.Contains("redshift,bigquery,sqlserver,azuresql,azuredwh", db.name) {
		ddl, err := conn.GetDDL(db.schema + ".place")
		assert.NoError(t, err)
		assert.Equal(t, db.placeDDL, ddl)
	}

	// GetDDL of view
	if !strings.Contains("redshift,bigquery", db.name) {
		ddl, err := conn.GetDDL(db.schema + ".place_vw")
		assert.NoError(t, err)
		assert.Equal(t, db.placeVwSelect, ddl)
	}

	// load Csv from test file
	csv1 := iop.CSV{Path: "test/test1.csv"}

	stream, err = csv1.ReadStream()
	assert.NoError(t, err)

	csvTable := db.schema + ".test1"
	err = conn.DropTable(csvTable)
	assert.NoError(t, err)

	sampleData := iop.NewDataset(stream.Columns)
	sampleData.Rows = stream.Buffer
	ddl, err := conn.GenerateDDL(csvTable, sampleData)
	assert.NoError(t, err)
	ok := assert.NotEmpty(t, ddl)

	if ok {
		_, err = conn.Exec(ddl)
		assert.NoError(t, err)

		// import to database
		conn.SetProp("AWS_BUCKET", os.Getenv("AWS_BUCKET"))
		_, err = conn.BulkImportStream(csvTable, stream)
		assert.NoError(t, err)

		// select back to assert equality
		count, err := conn.GetCount(csvTable)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1000), count)
	}

	// Test Schemata
	sData, err := conn.GetSchemaObjects(db.schema)
	assert.NoError(t, err)
	assert.Equal(t, db.schema, sData.Name)
	assert.Contains(t, sData.Tables, "person")
	assert.Contains(t, sData.Tables, "place_vw")
	assert.Contains(t, conn.Schemata().Tables, db.schema+".person")
	assert.Len(t, sData.Tables["person"].Columns, 3)
	assert.Contains(t, []string{"text", "varchar(255)", "VARCHAR2", "character varying", "varchar", "TEXT", "STRING"}, sData.Tables["person"].ColumnsMap["email"].Type)
	assert.Equal(t, true, sData.Tables["place_vw"].IsView)
	assert.EqualValues(t, int64(3), conn.Schemata().Tables[db.schema+".person"].ColumnsMap["email"].Position)

	// RunAnalysis field_stat
	values := map[string]interface{}{
		"t1":         db.schema + ".place",
		"t2":         db.schema + ".place",
		"t1_field":   "t1.country",
		"t1_fields1": "country",
		"t1_filter":  "1=1",
		"t2_field":   "t2.country",
		"t2_fields1": "country",
		"t2_filter":  "1=1",
		"conds":      `lower(t1.country) = lower(t2.country)`,
	}
	data, err = conn.RunAnalysis("table_join_match", values)
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []interface{}{0.0, int64(0), "0"}, data.Records()[0]["t1_null_cnt"])
	assert.Equal(t, 100.0, cast.ToFloat64(data.Records()[1]["match_rate"]))

	// RunAnalysisTable field_stat
	data, err = conn.RunAnalysisTable("table_count", db.schema+".person", db.schema+".place")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 2)
	assert.Contains(t, []int{2, 3}, cast.ToInt(data.Records()[0]["cnt"]))
	assert.Contains(t, []int{2, 3}, cast.ToInt(data.Records()[1]["cnt"]))

	// RunAnalysisField field_stat_deep
	data, err = conn.RunAnalysisField("field_stat_deep", db.schema+".person")
	assert.NoError(t, err)
	assert.Len(t, data.Rows, 3)
	assert.EqualValues(t, 2, cast.ToInt(data.Records()[0]["tot_cnt"]))
	assert.EqualValues(t, 0, cast.ToInt(data.Records()[1]["f_dup_cnt"]))

	// Not used
	// columns, err = conn.GetColumnStats(db.schema + ".transact")
	// assert.NoError(t, err)
	// assert.Len(t, columns, 9)
	// assert.Contains(t, []string{"date", "datetime"}, columns[0].Type)
	// assert.EqualValues(t, "original_description", strings.ToLower(columns[2].Name))
	// assert.Contains(t, []string{"string", "text"}, columns[2].Type)
	// assert.EqualValues(t, "decimal", columns[3].Type)

	// Extract / Load Test
	if !strings.Contains("redshift,bigquery,sqlite3,sqlserver,azuresql,azuredwh", db.name) {
		ELTest(t, db, csvTable)
	}

	// Drop all tables
	err = conn.DropTable("person", "place", "transact", "test1")
	assert.NoError(t, err)

	if !strings.Contains("redshift,bigquery,sqlite3,azuredwh", db.name) {
		// test sleep function
		sleepSQL := h.R(
			conn.GetTemplateValue("function.sleep"),
			"seconds", "1",
		)
		dd, err := conn.Query(sleepSQL)
		assert.NoError(t, err)
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

	err = conn.Close()
	assert.NoError(t, err)
}

func ELTest(t *testing.T, db *testDB, srcTable string) {
	tgtTable := srcTable + "2"
	_, sTable := SplitTableFullName(srcTable)
	_, tTable := SplitTableFullName(tgtTable)

	// var srcConn, tgtConn PostgresConn
	srcConn, err := NewConn(db.URL, db.propStrs...)
	assert.NoError(t, err)
	tgtConn, err := NewConn(db.URL, db.propStrs...)
	assert.NoError(t, err)

	err = srcConn.Connect()
	assert.NoError(t, err)

	err = tgtConn.Connect()
	assert.NoError(t, err)

	ddl, err := srcConn.GetDDL(srcTable)
	assert.NoError(t, err)
	newDdl := strings.Replace(ddl, sTable, tTable, 1)
	if strings.Contains("oracle,snowflake", db.name) {
		newDdl = strings.Replace(
			ddl, strings.ToUpper(sTable),
			strings.ToUpper(tTable), 1,
		)
	}

	err = tgtConn.DropTable(tgtTable)
	assert.NoError(t, err)

	_, err = tgtConn.Exec(newDdl)
	assert.NoError(t, err)

	stream, err := srcConn.StreamRows(`select * from ` + srcTable)
	assert.NoError(t, err)

	if assert.NoError(t, err) {
		_, err = tgtConn.InsertBatchStream(tgtTable, stream)
		assert.NoError(t, err)

		data, err := tgtConn.RunAnalysisTable("table_count", srcTable, tgtTable)
		if assert.NoError(t, err) {
			assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
		}
	}

	// use Bulk
	_, err = tgtConn.Query("truncate table " + tgtTable)
	assert.NoError(t, err)

	stream, err = srcConn.BulkExportStream(`select * from ` + srcTable)
	assert.NoError(t, err)

	if err == nil {
		_, err = tgtConn.BulkImportStream(tgtTable, stream)
		assert.NoError(t, err)

		data, err := tgtConn.RunAnalysisTable("table_count", srcTable, tgtTable)
		if assert.NoError(t, err) {
			assert.Equal(t, data.Records()[0]["cnt"], data.Records()[1]["cnt"])
		}
	}

	err = tgtConn.DropTable(tgtTable)
	assert.NoError(t, err)

	srcConn.Close()
	tgtConn.Close()

}

func tInsertStreamLarge(t *testing.T, conn Connection, data iop.Dataset, tableName string) {
	start := time.Now()
	getRate := func(cnt uint64) string {
		return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
	}

	df, err := iop.MakeDataFlow(data.Stream())
	assert.NoError(t, err)

	cnt, err := conn.BulkImportFlow(tableName, df)
	assert.NoError(t, err)

	h.Debug("inserted %d rows [%s r/s]", cnt, getRate(cnt))
}

func tSelectStreamLarge(t *testing.T, conn Connection, tableName string, dfMult int) (count uint64) {
	start := time.Now()
	getRate := func(cnt uint64) string {
		return humanize.Commaf(math.Round(cast.ToFloat64(cnt) / time.Since(start).Seconds()))
	}

	sql := "select * from " + tableName
	sqls := []string{}
	for i := 0; i < dfMult; i++ {
		sqls = append(sqls, sql)
	}
	UseBulkExportFlowCSV = false
	df, err := conn.BulkExportFlow(sqls...)
	if !assert.NoError(t, err) {
		return
	}
	for ds := range df.StreamCh {
		for range ds.Rows {
			// do nothing
		}
		ds.SetEmpty()
		assert.NoError(t, ds.Err())
	}
	count = df.Count()
	assert.True(t, df.IsEmpty())
	df.Close()
	h.Debug("selected %d rows [%s r/s]", count, getRate(count))
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
		&FakeField{"name", func() interface{} { return faker.Name().Name() }},
		&FakeField{"url", func() interface{} { return faker.Internet().Url() }},
		&FakeField{"date_time", func() interface{} { return faker.Date().Forward(100 * time.Minute).Format("2006-01-02 15:04:05") }},
		&FakeField{"address", func() interface{} { return faker.Address().SecondaryAddress() }},
		&FakeField{"price", func() interface{} { return faker.Commerce().Price() }},
		&FakeField{"my_int", func() interface{} { return faker.Number().NumberInt64(5) }},
		&FakeField{"email", func() interface{} { return faker.Internet().Email() }},
		&FakeField{"creditcardexpirydate", func() interface{} { return faker.Date().Forward(1000000 * time.Minute).Format("2006-01-02") }},
		&FakeField{"latitude", func() interface{} { return faker.Address().Latitude() }},
		&FakeField{"longitude", func() interface{} { return faker.Address().Longitude() }},
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

	err := data.WriteCsv(path)
	if err != nil {
		log.Fatal(h.Error(err, "Could not create file: "+path))
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
		// DBs["redshift"],
		DBs["sqlserver"],
		// DBs["azuresql"],
		// DBs["azuredwh"],
		DBs["snowflake"],
		DBs["bigquery"],
	}
	// test snowflake Azure and AWS
	DBs["snowflake_aws"] = &testDB{
		URL:    os.Getenv("SNOWFLAKE_URL") + "&CopyMethod=AWS",
		schema: "public",
	}
	DBs["snowflake_azure"] = &testDB{
		URL:    os.Getenv("SNOWFLAKE_URL") + "&CopyMethod=AZURE",
		schema: "public",
	}

	// dbs = []*testDB{DBs["snowflake_aws"], DBs["snowflake_azure"]}

	ctx := h.NewContext(context.Background(), 20)
	doTest := func(db *testDB) {
		defer ctx.Wg.Write.Done()
		os.Setenv("SLINGELT_FILE_ROW_LIMIT", "13000")
		conn, err := connect(db)
		ok := assert.NoError(t, err)
		if !ok {
			return
		}
		defer conn.Close()
		iop.ShowProgress = false

		tableName := db.schema + ".test1"
		err = conn.DropTable(tableName)
		assert.NoError(t, err)

		ddl, err := conn.GenerateDDL(tableName, data)
		assert.NoError(t, err)

		_, err = conn.Exec(ddl)
		assert.NoError(t, err)
		tInsertStreamLarge(t, conn, data, tableName)

		cnt, err := conn.GetCount(tableName)
		assert.NoError(t, err)
		if !assert.EqualValues(t, numRows, cnt, "Got %d", cnt) {
			return
		}

		// return

		dfMult := 1
		cnt = tSelectStreamLarge(t, conn, tableName, dfMult)
		assert.EqualValues(t, numRows*dfMult, cnt)

		err = conn.DropTable(tableName)
		assert.NoError(t, err)

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

func TestEnvURL(t *testing.T) {
	os.Setenv("SLINGELT_DB_PG_HELLO", DBs["postgres"].URL)
	slingEnv := GetSlingEnv()
	assert.Contains(t, slingEnv, "SLINGELT_DB_PG_HELLO")
	conn, err := NewConn("PG_HELLO")
	assert.NoError(t, err)
	err = conn.Connect()
	assert.NoError(t, err)
}

func TestURL(t *testing.T) {
	url, err := dburl.Parse(os.Getenv("SNOWFLAKE_URL"))
	if err != nil {
		return
	}
	h.P(url)
	// h.P(url.Query())
}

func TestExport(t *testing.T) {
	db := DBs["bigquery"]
	conn, err := NewConn(db.URL)
	assert.NoError(t, err)
	_, err = conn.BulkExportFlow("select * from `proven-cider-633.pg_home.bank_mint_transactions`")
	assert.NoError(t, err)
}

func TestPasswordSSH(t *testing.T) {
	// with password
	dbURL := "POSTGRES_URL"
	// sshURL := "ssh://user:hello@bionic:2222"
	sshURL := os.Getenv("SSH_TEST_PASSWD_URL")
	conn, err := NewConn(dbURL, "SSH_TUNNEL="+sshURL)
	assert.NoError(t, err)
	err = conn.Connect()
	assert.NoError(t, err)
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
		assert.NoError(t, err)
		err = conn.Connect()
		assert.NoError(t, err)
		conn.Close()
	}
}

func testOracleClob(t *testing.T) {
	conn, err := NewConn("ORACLE_URL")
	assert.NoError(t, err)
	err = conn.Connect()
	assert.NoError(t, err)

	// sql := `SELECT *	FROM dba_hist_sqltext`
	sql := `SELECT * FROM SYS.METASTYLESHEET where rownum < 10`
	data, err := conn.Query(sql)
	assert.NoError(t, err)
	h.P(data.Rows[0])
}

func TestDatatypes(t *testing.T) {

	now := time.Now()
	now = time.Date(now.Year(), now.Month(), now.Day(), 10, 11, 13, 0, now.Location())
	testValsMap := map[string]interface{}{
		"bigint":    741868284,
		"binary":    []byte(`{"key":{"subkey":[1,2]}, "another":"value"}`),
		"bit":       1,
		"bool":      true,
		"date":      time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()),
		"datetime":  now,
		"decimal":   14876.411,
		"integer":   716274,
		"json":      `{"key":{"subkey":[1,2]}, "another":"value"}`,
		"smallint":  112,
		"string":    "this is my string",
		"text":      "this is my string",
		"timestamp": now,
		"float":     4414.11,
		"time":      time.Date(0, 1, 1, 10, 11, 13, 0, now.Location()),
	}

	dbs := []*testDB{
		// DBs["postgres"], // OK
		// DBs["mysql"], // OK
		// DBs["oracle"], // OK
		DBs["redshift"],
		// DBs["sqlserver"], // OK
		// // DBs["azuresql"],
		// // DBs["azuredwh"],
		// DBs["snowflake"], // OK, bool returns 1 for true
		// DBs["bigquery"], // Almost OK, date/time require proper format, Bool fails to insert as go typed
	}

	TypesNativeFile, err := h.PkgerFile("templates/types_native_to_general.tsv")
	assert.NoError(t, err)

	TypesNativeCSV := iop.CSV{Reader: bufio.NewReader(TypesNativeFile)}

	data, err := TypesNativeCSV.Read()
	assert.NoError(t, err)

	for _, db := range dbs {
		conn, err := NewConn(db.URL, db.propStrs...)
		assert.NoError(t, err)

		err = conn.Connect()
		if !assert.NoError(t, err) {
			continue
		}

		colsDDL := []string{}
		insertCols := []string{}
		testVals := []interface{}{}
		for _, rec := range data.Records() {
			if rec["database"] == conn.GetType() {
				colDDL := cast.ToString(rec["ddl_string"])
				if strings.TrimSpace(colDDL) != "" {
					colsDDL = append(colsDDL, colDDL)
				}
				testVal, ok := testValsMap[cast.ToString(rec["general_type"])]
				if !ok {
					h.LogFatal(fmt.Errorf(cast.ToString(rec["general_type"]) + " not found"))
				}

				colNameArr := strings.Split(colDDL, " ")
				if cast.ToString(rec["test"]) == "TRUE" && len(colNameArr) > 0 {
					testVals = append(testVals, testVal)
					insertCols = append(insertCols, colNameArr[0])
				}
			}
		}

		tableName := db.schema + ".test1"
		conn.DropTable(tableName)
		ddl := h.F(
			"create table %s (%s)",
			tableName, strings.Join(colsDDL, ", "),
		)
		_, err = conn.Exec(ddl)
		if !assert.NoError(t, err) {
			return
		}

		columns, err := conn.GetColumns(tableName)
		assert.NoError(t, err)

		insertCols, err = conn.ValidateColumnNames(ColumnNames(columns), insertCols, true)
		assert.NoError(t, err)
		insertStatement := conn.GenerateInsertStatement(
			tableName,
			insertCols,
			1,
		)
		conn.MustExec(insertStatement, testVals...)

		columns, err = conn.GetColumnStats(tableName)
		assert.NoError(t, err)
		assert.Greater(t, len(columns), 0)
		for _, col := range columns {
			h.Debug("%s - %#v", col.Name, col.Stats)
		}

		data, err = conn.Query(h.F(
			`select %s from %s`,
			strings.Join(insertCols, ", "),
			tableName,
		))
		for i, val := range data.Rows[0] {
			assert.EqualValues(t, testVals[i], val, insertCols[i])
		}
		conn.DropTable(tableName)
	}
}

func TestCastColumnsForSelect(t *testing.T) {
	db := DBs["postgres"]
	conn, err := connect(db)
	assert.NoError(t, err)

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
	assert.NoError(t, err)

	_, err = conn.Exec(`
	create table public.src1 (
		col1 varchar(50),
		col3 timestamp,
		col2 bigint,
		col4 float,
		col5 varchar(50)
	)
	`)
	assert.NoError(t, err)

	tgtColumns, err := conn.GetColumns(`public.tgt1`)
	assert.NoError(t, err)
	srcColumns, err := conn.GetColumns(`public.src1`)
	assert.NoError(t, err)

	assert.EqualValues(t, 7, len(tgtColumns))
	assert.EqualValues(t, 5, len(srcColumns))

	tgtFields, err := conn.ValidateColumnNames(
		ColumnNames(tgtColumns),
		ColumnNames(srcColumns),
		true,
	)
	assert.NoError(t, err)

	assert.EqualValues(t, 5, len(tgtFields))
	assert.EqualValues(t, `"CoL1"`, tgtFields[0])

	h.P(tgtFields)

	srcFields := conn.CastColumnsForSelect(srcColumns, tgtColumns)
	assert.EqualValues(t, len(tgtFields), len(srcFields))
	h.P(srcFields)

	err = conn.DropTable(`public.tgt1`, `public.src1`)
}

func TestDecimal(t *testing.T) {
	db := DBs["postgres"]
	conn, err := connect(db)
	assert.NoError(t, err)

	err = conn.DropTable(`public.table1`)
	assert.NoError(t, err)

	_, err = conn.Exec(`
	create table public.table1 (
		col1 numeric(30,9)
	)
	`)
	assert.NoError(t, err)

	_, err = conn.Exec(`insert into public.table1 values(1032.442)`)
	assert.NoError(t, err)
	_, err = conn.Exec(`commit`)
	assert.NoError(t, err)

	result, err := conn.Db().Queryx(`select col1 from public.table1`)
	assert.NoError(t, err)
	result.Next()
	var val interface{}
	result.Scan(&val)
	h.P(val)
	h.P(cast.ToString(val))

	decReplRegex := regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`)
	nVal := decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
	h.P(nVal)
	err = conn.DropTable(`public.table1`)

}
