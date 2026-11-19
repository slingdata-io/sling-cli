package database

import (
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestParseTableName(t *testing.T) {
	/*
		schema.table
		"ScheMa".table
		"ScheMa Name".table
		schema."Table Name"
		"ScheMa Name"."Table Name"
	*/
	type testCase struct {
		input   string
		dialect dbio.Type
		output  Table
	}
	cases := []testCase{
		{
			input:   `schema.table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "SCHEMA", Name: "TABLE"},
		},
		{
			input:   `schema.*`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "SCHEMA", Name: "*"},
		},
		{
			input:   `*`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Name: "*"},
		},
		{
			input:   `"ScheMa".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa", Name: "TABLE"},
		},
		{
			input:   `"ScheMa Name".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa Name", Name: "TABLE"},
		},
		{
			input:   "`db-4`.table",
			dialect: dbio.TypeDbMySQL,
			output:  Table{Schema: "db-4", Name: "table"},
		},
		{
			input:   "DB-4.table",
			dialect: dbio.TypeDbMySQL,
			output:  Table{Schema: "DB-4", Name: "table"},
		},
		{
			input:   "`DB-4`.table",
			dialect: dbio.TypeDbMySQL,
			output:  Table{Schema: "DB-4", Name: "table"},
		},
		{
			input:   "schema.`Table Name`",
			dialect: dbio.TypeDbMySQL,
			output:  Table{Schema: "schema", Name: "Table Name"},
		},
		{
			input:   `"ScheMa Name"."Table Name"`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa Name", Name: "Table Name"},
		},
		{
			input:   `ScheMa-Name."Table Name"`,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{Schema: "ScheMa-Name", Name: "Table Name"},
		},
		{
			input:   `select 1 from table `,
			dialect: dbio.TypeDbSnowflake,
			output:  Table{SQL: `select 1 from table`},
		},
	}

	for _, c := range cases {
		table, err := ParseTableName(c.input, c.dialect)
		if !assert.NoError(t, err, c) {
			return
		}
		assert.Equal(t, c.output.Name, table.Name, c)
		assert.Equal(t, c.output.Schema, table.Schema, c)
		assert.Equal(t, c.output.Database, table.Database, c)
		assert.Equal(t, c.output.SQL, table.SQL, c)
	}
}

func TestRegexMatch(t *testing.T) {

	table, err := ParseTableName(`with () select id, created, type from "dbo"."biz" where "created" >= '2021-01-01 01:40:00' order by "created" asc`, dbio.TypeDbSQLServer)
	if !assert.NoError(t, err) {
		return
	}
	sql := table.Select(SelectOptions{Limit: g.Ptr(10)})
	assert.True(t, strings.HasPrefix(sql, `with () select id`), sql)
}

func TestParseColumnName(t *testing.T) {
	type testCase struct {
		input   string
		dialect dbio.Type
		output  string
	}
	cases := []testCase{
		{
			input:   `schema.table.col1`,
			dialect: dbio.TypeDbSnowflake,
			output:  "COL1",
		},
		{
			input:   `schema.*`,
			dialect: dbio.TypeDbSnowflake,
			output:  "*",
		},
		{
			input:   `*`,
			dialect: dbio.TypeDbSnowflake,
			output:  "*",
		},
		{
			input:   `"ScheMa".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  "TABLE",
		},
		{
			input:   `table`,
			dialect: dbio.TypeDbSnowflake,
			output:  "TABLE",
		},
		{
			input:   `table`,
			dialect: dbio.TypeDbMySQL,
			output:  "table",
		},
		{
			input:   `TABLE`,
			dialect: dbio.TypeDbMySQL,
			output:  "table",
		},
		{
			input:   `TaBLE`,
			dialect: dbio.TypeDbMySQL,
			output:  "TaBLE",
		},
		{
			input:   `"ScheMa Name".table`,
			dialect: dbio.TypeDbSnowflake,
			output:  "TABLE",
		},
		{
			input:   "`table-4`",
			dialect: dbio.TypeDbMySQL,
			output:  "table-4",
		},
		{
			input:   "TABLE-4",
			dialect: dbio.TypeDbMySQL,
			output:  "table-4",
		},
		{
			input:   "TABLe-4",
			dialect: dbio.TypeDbMySQL,
			output:  "TABLe-4",
		},
		{
			input:   "schema.`Table Name`",
			dialect: dbio.TypeDbMySQL,
			output:  "Table Name",
		},
		{
			input:   `"ScheMa Name"."Table Name"`,
			dialect: dbio.TypeDbSnowflake,
			output:  "Table Name",
		},
		{
			input:   `ScheMa-Name.Table-Name`,
			dialect: dbio.TypeDbSnowflake,
			output:  "Table-Name",
		},
	}

	for _, c := range cases {
		column, err := ParseColumnName(c.input, c.dialect)
		if !assert.NoError(t, err, c) {
			return
		}
		assert.Equal(t, c.output, column, c)
	}
}

func TestParseSQLMultiStatements(t *testing.T) {
	type testCase struct {
		name     string
		input    string
		dialect  dbio.Type
		expected []string
	}

	cases := []testCase{
		{
			name:     "simple single statement",
			input:    "SELECT * FROM users",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "simple multiple statements",
			input:    "SELECT * FROM users; INSERT INTO logs VALUES (1);",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users", "INSERT INTO logs VALUES (1)"},
		},
		{
			name:     "with trailing whitespace",
			input:    "SELECT * FROM users;  \n  ",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users"},
		},
		{
			name:     "statements with comments",
			input:    "SELECT * FROM users; -- Get all users\nINSERT INTO logs VALUES (1); /* Add log */",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users", "-- Get all users\nINSERT INTO logs VALUES (1)", "/* Add log */"},
		},
		{
			name:     "semicolon in quoted string",
			input:    "SELECT * FROM users WHERE name = 'user;name';",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users WHERE name = 'user;name'"},
		},
		{
			name:     "semicolon in comments",
			input:    "SELECT * FROM users /* ; */ WHERE id = 1;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users /* ; */ WHERE id = 1"},
		},
		{
			name:     "sql server with trailing semicolon",
			input:    "SELECT * FROM users;",
			dialect:  dbio.TypeDbSQLServer,
			expected: []string{"SELECT * FROM users;"},
		},
		{
			name:     "begin end block",
			input:    "BEGIN UPDATE users SET active = 1; INSERT INTO logs VALUES (1); END;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"BEGIN UPDATE users SET active = 1; INSERT INTO logs VALUES (1); END;"},
		},
		{
			name:     "declare begin end block",
			input:    "DECLARE v_x NUMBER; BEGIN v_x := 1; INSERT INTO logs VALUES (v_x); END;",
			dialect:  dbio.TypeDbOracle,
			expected: []string{"DECLARE v_x NUMBER; BEGIN v_x := 1; INSERT INTO logs VALUES (v_x); END;"},
		},
		{
			name:     "prepare execute statement",
			input:    "PREPARE stmt AS SELECT * FROM users; EXECUTE stmt;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"PREPARE stmt AS SELECT * FROM users; EXECUTE stmt;"},
		},
		{
			name:     "create procedure",
			input:    "CREATE PROCEDURE get_users() BEGIN SELECT * FROM users; END;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"CREATE PROCEDURE get_users() BEGIN SELECT * FROM users; END;"},
		},
		{
			name:     "create function",
			input:    "CREATE FUNCTION get_user_count() RETURNS INT BEGIN RETURN (SELECT COUNT(*) FROM users); END;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"CREATE FUNCTION get_user_count() RETURNS INT BEGIN RETURN (SELECT COUNT(*) FROM users); END;"},
		},
		{
			name:     "single quotes with escape",
			input:    "SELECT * FROM users WHERE name = 'O''Connor'; SELECT * FROM logs;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users WHERE name = 'O''Connor'", "SELECT * FROM logs"},
		},
		{
			name:     "empty statements should be skipped",
			input:    ";;SELECT * FROM users;;;;SELECT * FROM logs;;;",
			dialect:  dbio.TypeDbPostgres,
			expected: []string{"SELECT * FROM users", "SELECT * FROM logs"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := ParseSQLMultiStatements(c.input, c.dialect)
			assert.Equal(t, c.expected, result)
		})
	}
}

func TestGetSchemataAll(t *testing.T) {
	ef := env.LoadSlingEnvFile()

	url := cast.ToStringMap(ef.Connections["POSTGRES"])["url"]
	if url == nil {
		t.Skip("POSTGRES env var not set")
	}

	conn, err := NewConn(cast.ToString(url))
	if !assert.NoError(t, err) {
		return
	}
	defer conn.Close()

	schemata, err := GetSchemataAll(conn)
	if !assert.NoError(t, err) {
		return
	}

	// Count all tables from all databases
	tableCount := 0
	for _, db := range schemata.Databases {
		tableCount += len(db.Tables())
	}

	assert.Greater(t, tableCount, 0, "expected at least one table across all databases")
}

func TestAddPrimaryKeyToDDL(t *testing.T) {
	// Verifies that primary key is placed correctly in column definitions
	// when table_ddl contains WITH clause or other suffixes after ({col_types})

	type testCase struct {
		name     string
		dialect  dbio.Type
		ddl      string
		pkCols   []string
		expected string
	}

	cases := []testCase{
		{
			name:     "simple DDL without WITH clause",
			dialect:  dbio.TypeDbSQLServer,
			ddl:      `create table "dbo"."test" ("col1" nvarchar(10), "col2" nvarchar(6))`,
			pkCols:   []string{"col1", "col2"},
			expected: `create table "dbo"."test" ("col1" nvarchar(10), "col2" nvarchar(6), primary key ("col1", "col2"))`,
		},
		{
			name:     "DDL with WITH clause (GitHub issue #694)",
			dialect:  dbio.TypeDbSQLServer,
			ddl:      `create table "dbo"."test" ("col1" nvarchar(10), "col2" nvarchar(6)) WITH (data_compression=page)`,
			pkCols:   []string{"col1", "col2"},
			expected: `create table "dbo"."test" ("col1" nvarchar(10), "col2" nvarchar(6), primary key ("col1", "col2")) WITH (data_compression=page)`,
		},
		{
			name:     "DDL with multiple WITH options",
			dialect:  dbio.TypeDbSQLServer,
			ddl:      `create table "dbo"."test" ("col1" int, "col2" int) WITH (PAD_INDEX = ON, FILLFACTOR = 90)`,
			pkCols:   []string{"col1"},
			expected: `create table "dbo"."test" ("col1" int, "col2" int, primary key ("col1")) WITH (PAD_INDEX = ON, FILLFACTOR = 90)`,
		},
		{
			name:     "DDL with nested parentheses in column type",
			dialect:  dbio.TypeDbSQLServer,
			ddl:      `create table "dbo"."test" ("col1" decimal(10,2), "col2" varchar(100)) WITH (LOCK_ESCALATION = TABLE)`,
			pkCols:   []string{"col1"},
			expected: `create table "dbo"."test" ("col1" decimal(10,2), "col2" varchar(100), primary key ("col1")) WITH (LOCK_ESCALATION = TABLE)`,
		},
		{
			name:     "Postgres DDL without suffix",
			dialect:  dbio.TypeDbPostgres,
			ddl:      `create table if not exists "public"."test" ("col1" integer, "col2" text)`,
			pkCols:   []string{"col1"},
			expected: `create table if not exists "public"."test" ("col1" integer, "col2" text, primary key ("col1"))`,
		},
		{
			name:     "Postgres DDL with PARTITION BY clause",
			dialect:  dbio.TypeDbPostgres,
			ddl:      `create table if not exists "public"."test" ("col1" integer, "col2" date) PARTITION BY RANGE (col2)`,
			pkCols:   []string{"col1"},
			expected: `create table if not exists "public"."test" ("col1" integer, "col2" date, primary key ("col1")) PARTITION BY RANGE (col2)`,
		},
		{
			name:     "Oracle DDL with named constraint",
			dialect:  dbio.TypeDbOracle,
			ddl:      `create table "SCHEMA"."TEST" ("COL1" NUMBER, "COL2" VARCHAR2(100))`,
			pkCols:   []string{"COL1"},
			expected: `create table "SCHEMA"."TEST" ("COL1" NUMBER, "COL2" VARCHAR2(100), constraint test_pkey primary key ("COL1"))`,
		},
		{
			name:     "no primary key columns",
			dialect:  dbio.TypeDbSQLServer,
			ddl:      `create table "dbo"."test" ("col1" int, "col2" int) WITH (FILLFACTOR = 90)`,
			pkCols:   []string{},
			expected: `create table "dbo"."test" ("col1" int, "col2" int) WITH (FILLFACTOR = 90)`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table := &Table{
				Name:    "test",
				Dialect: c.dialect,
			}

			// Create columns with primary key flag
			var cols iop.Columns
			for _, name := range c.pkCols {
				col := iop.Column{Name: name}
				col.SetMetadata(iop.PrimaryKey.MetadataKey(), "true")
				cols = append(cols, col)
			}

			result, err := table.AddPrimaryKeyToDDL(c.ddl, cols)
			assert.NoError(t, err)
			assert.Equal(t, c.expected, result)
		})
	}
}

func TestTrimSQLComments(t *testing.T) {
	type testCase struct {
		name     string
		input    string
		expected string
		hasError bool
	}

	cases := []testCase{
		{
			name:     "no comments",
			input:    "SELECT * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
			hasError: false,
		},
		{
			name:     "line comment at end",
			input:    "SELECT * FROM users -- This is a comment",
			expected: "SELECT * FROM users ",
			hasError: false,
		},
		{
			name:     "line comment in middle",
			input:    "SELECT * -- Get all users\nFROM users",
			expected: "SELECT * \nFROM users",
			hasError: false,
		},
		{
			name:     "block comment at end",
			input:    "SELECT * FROM users /* This is a block comment */",
			expected: "SELECT * FROM users ",
			hasError: false,
		},
		{
			name:     "block comment in middle",
			input:    "SELECT * /* Get all users */ FROM users",
			expected: "SELECT *  FROM users",
			hasError: false,
		},
		{
			name:     "mixed comments",
			input:    "SELECT * /* Block comment */ FROM users -- Line comment\nWHERE id = 1",
			expected: "SELECT *  FROM users \nWHERE id = 1",
			hasError: false,
		},
		{
			name:     "comment inside quoted string",
			input:    "SELECT * FROM users WHERE comment = '-- Not a comment'",
			expected: "SELECT * FROM users WHERE comment = '-- Not a comment'",
			hasError: false,
		},
		{
			name:     "escaped quotes",
			input:    "SELECT * FROM users WHERE name = 'O''Connor' -- Comment",
			expected: "SELECT * FROM users WHERE name = 'O''Connor' ",
			hasError: false,
		},
		{
			name:     "unterminated quote",
			input:    "SELECT * FROM users WHERE name = 'O",
			expected: "",
			hasError: true,
		},
		{
			name:     "unterminated block comment",
			input:    "SELECT * FROM users /* Comment without end",
			expected: "",
			hasError: true,
		},
		{
			name:     "nested-looking comments",
			input:    "SELECT * /* outer /* inner */ comment */ FROM users",
			expected: "SELECT *  comment */ FROM users",
			hasError: false,
		},
		{
			name:     "empty input",
			input:    "",
			expected: "",
			hasError: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result, err := TrimSQLComments(c.input)
			if c.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, c.expected, result)
			}
		})
	}
}

func TestSelectSQLServerOrderBy(t *testing.T) {
	query := "select id, name from dbo.t order by id"

	table, err := ParseTableName(query, dbio.TypeDbSQLServer)
	assert.NoError(t, err)

	sql := table.Select(SelectOptions{Limit: g.Ptr(10)})
	assert.Contains(t, sql, "select top 10 * from (")
	assert.Contains(t, sql, "order by id offset 0 rows")

	sql = table.Select(SelectOptions{Limit: g.Ptr(10), Fields: []string{"id"}})
	assert.Contains(t, sql, "top 10")
	assert.Contains(t, sql, "order by id offset 0 rows\n) t")

	sql = table.Select(SelectOptions{Fields: []string{"id"}})
	assert.Contains(t, sql, "order by id offset 0 rows\n) t")

	// no wrap, no change
	assert.Equal(t, query, table.Select())

	// postgres is unchanged
	table, err = ParseTableName(query, dbio.TypeDbPostgres)
	assert.NoError(t, err)
	sql = table.Select(SelectOptions{Limit: g.Ptr(10), Fields: []string{"id"}})
	assert.NotContains(t, sql, "offset 0 rows")
	assert.Contains(t, sql, "order by id")
}

func TestTableSelectLimitOffset(t *testing.T) {
	cases := []struct {
		name     string
		dialect  dbio.Type
		offset   int
		where    string
		equal    string
		prefix   string
		suffix   string
		contains string
	}{
		{name: "postgres limit", dialect: dbio.TypeDbPostgres, equal: `select * from "public"."t" limit 5`},
		{name: "postgres offset", dialect: dbio.TypeDbPostgres, offset: 10, equal: `select * from "public"."t" limit 5 offset 10`},
		{name: "postgres where", dialect: dbio.TypeDbPostgres, offset: 10, where: "id > 1", equal: `select * from "public"."t" where (id > 1) limit 5 offset 10`},
		{name: "duckdb limit", dialect: dbio.TypeDbDuckDb, equal: `select * from "public"."t" limit 5`},
		{name: "duckdb offset", dialect: dbio.TypeDbDuckDb, offset: 10, equal: `select * from "public"."t" limit 5 offset 10`},
		{name: "duckdb where", dialect: dbio.TypeDbDuckDb, offset: 10, where: "id > 1", equal: `select * from "public"."t" where (id > 1) limit 5 offset 10`},
		{name: "odbc offset", dialect: dbio.TypeDbODBC, offset: 10, suffix: " limit 5 offset 10"},
		{name: "adbc offset", dialect: dbio.TypeDbArrowDBC, offset: 10, suffix: " limit 5 offset 10"},
		{name: "athena limit", dialect: dbio.TypeDbAthena, suffix: " limit 5"},
		{name: "athena offset", dialect: dbio.TypeDbAthena, offset: 10, suffix: " offset 10 limit 5"},
		{name: "trino limit", dialect: dbio.TypeDbTrino, where: "id > 1", suffix: " where (id > 1) limit 5"},
		{name: "trino offset", dialect: dbio.TypeDbTrino, offset: 10, where: "id > 1", suffix: " where (id > 1) offset 10 limit 5"},
		{name: "sqlserver limit", dialect: dbio.TypeDbSQLServer, prefix: "select top 5 "},
		{name: "sqlserver offset", dialect: dbio.TypeDbSQLServer, offset: 10, contains: "offset 10 rows) as t"},
		{name: "oracle offset", dialect: dbio.TypeDbOracle, offset: 10, contains: "offset 10 rows fetch next 5 rows only"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			table := Table{Name: "t", Schema: "public", Dialect: c.dialect}
			sql := table.Select(SelectOptions{Limit: g.Ptr(5), Offset: c.offset, Where: c.where})
			if c.equal != "" {
				assert.Equal(t, c.equal, sql)
			}
			if c.prefix != "" {
				assert.True(t, strings.HasPrefix(sql, c.prefix), sql)
			}
			if c.suffix != "" {
				assert.True(t, strings.HasSuffix(sql, c.suffix), sql)
			}
			if c.contains != "" {
				assert.Contains(t, sql, c.contains)
			}
		})
	}
}

func TestTableSelectOffsetAllDialects(t *testing.T) {
	// these dialects return before they read the template
	noTemplate := []dbio.Type{
		dbio.TypeDbPrometheus, dbio.TypeDbIceberg, dbio.TypeDbDynamoDB, dbio.TypeDbMongoDB,
		dbio.TypeDbElasticsearch, dbio.TypeDbOpenSearch, dbio.TypeDbAzureTable,
	}

	types := []dbio.Type{
		dbio.TypeDbAzureDWH, dbio.Type("db2"), dbio.Type("firebird"),
		dbio.Type("hive"), dbio.Type("spark"), dbio.Type("vertica"),
	}
	for _, td := range dbio.AllType {
		if td.Value.IsDb() {
			types = append(types, td.Value)
		}
	}

	for _, dialect := range types {
		if g.In(dialect, noTemplate...) {
			continue
		}
		if _, err := dialect.Template(); err != nil {
			continue
		}
		t.Run(string(dialect), func(t *testing.T) {
			table := Table{Name: "t", Schema: "public", Dialect: dialect}
			sql := table.Select(SelectOptions{Limit: g.Ptr(5), Offset: 10})
			assert.Contains(t, sql, "10", "offset is lost for %s: %s", dialect, sql)
		})
	}
}

func TestTableFinalizeAggregateFields(t *testing.T) {
	newTable := func(dialect dbio.Type) Table {
		return Table{
			Name:    "agg",
			Schema:  "db",
			Dialect: dialect,
			Columns: iop.Columns{
				{Name: "id", DbType: "UInt32"},
				{Name: "min_ts", DbType: "AggregateFunction(min, Nullable(DateTime64(6, 'Asia/Jakarta')))"},
				{Name: "cnt", DbType: "AggregateFunction(count)"},
				{Name: "mx", DbType: "SimpleAggregateFunction(max, UInt32)"},
			},
		}
	}
	table := newTable(dbio.TypeDbClickhouse)

	all := []string{"id", "finalizeAggregation(`min_ts`) as `min_ts`", "finalizeAggregation(`cnt`) as `cnt`", "mx"}
	assert.Equal(t, all, table.FinalizeAggregateFields([]string{"*"}))
	assert.Equal(t, all, table.FinalizeAggregateFields(nil))

	assert.Equal(t,
		[]string{"id", "finalizeAggregation(`min_ts`) as `first_ts`"},
		table.FinalizeAggregateFields([]string{"id", "min_ts as first_ts"}),
	)
	assert.Equal(t,
		[]string{"finalizeAggregation(`min_ts`) as `min_ts`"},
		table.FinalizeAggregateFields([]string{"MIN_TS"}),
	)
	assert.Equal(t, []string{"-cnt"}, table.FinalizeAggregateFields([]string{"-cnt"}))

	// no AggregateFunction column
	plain := newTable(dbio.TypeDbClickhouse)
	plain.Columns = plain.Columns[:1]
	assert.Equal(t, []string{"*"}, plain.FinalizeAggregateFields([]string{"*"}))
	assert.Nil(t, plain.FinalizeAggregateFields(nil))

	// other dialects are unchanged
	pg := newTable(dbio.TypeDbPostgres)
	assert.Equal(t, []string{"*"}, pg.FinalizeAggregateFields([]string{"*"}))

	sql := table.Select(SelectOptions{Fields: table.FinalizeAggregateFields([]string{"*"})})
	assert.Equal(t, "select `id`, finalizeAggregation(`min_ts`) as `min_ts`, finalizeAggregation(`cnt`) as `cnt`, `mx` from `db`.`agg`", sql)
}

func TestSelectFirebirdLimit(t *testing.T) {
	fb := dbio.Type("firebird")

	table, err := ParseTableName("TBL", fb)
	assert.NoError(t, err)

	sql := table.Select(SelectOptions{Limit: g.Ptr(5)})
	assert.Contains(t, sql, "select first 5")
	assert.Contains(t, sql, `from "TBL"`)
	assert.NotContains(t, sql, "limit 5")
	assert.NotContains(t, sql, `"main"`)

	sql = table.Select(SelectOptions{Limit: g.Ptr(5), Offset: 10})
	assert.Contains(t, sql, "first 5 skip 10")
	assert.NotContains(t, sql, "limit")

	table, err = ParseTableName("select a from TBL", fb)
	assert.NoError(t, err)
	sql = table.Select(SelectOptions{Limit: g.Ptr(5)})
	assert.Contains(t, sql, "select first 5 skip 0 * from (")
}

func TestTableDropUnsupportedSchema(t *testing.T) {
	fb := dbio.Type("firebird")

	table, _ := ParseTableName("dbo.TBL", fb)
	assert.Equal(t, "DBO", table.DropUnsupportedSchema())
	assert.Equal(t, `"TBL"`, table.FullName())

	table, _ = ParseTableName("TBL", fb)
	assert.Empty(t, table.DropUnsupportedSchema())

	table, _ = ParseTableName("select 1 from rdb$database", fb)
	assert.Empty(t, table.DropUnsupportedSchema())

	for _, typ := range []dbio.Type{dbio.TypeDbPostgres, dbio.TypeDbSQLite} {
		table, _ = ParseTableName("main.tbl", typ)
		assert.Empty(t, table.DropUnsupportedSchema())
		assert.Equal(t, "main", table.Schema)
	}
}

func TestFilterTablesEmptySchema(t *testing.T) {
	newSchemata := func(schema string, names ...string) Schemata {
		tables := map[string]Table{}
		for _, name := range names {
			tables[strings.ToLower(name)] = Table{
				Name:   name,
				Schema: schema,
				Columns: iop.Columns{
					{Name: "id", Table: name, Schema: schema, Type: iop.IntegerType},
					{Name: "val", Table: name, Schema: schema, Type: iop.StringType},
				},
			}
		}
		return Schemata{Databases: map[string]Database{
			"": {Schemas: map[string]Schema{strings.ToLower(schema): {Name: schema, Tables: tables}}},
		}}
	}

	s := newSchemata("", "TBL_A", "TBL_B", "OTHER")

	ns := s.Filtered(false, "TBL_*")
	assert.Len(t, ns.Tables(), 2)
	ns = s.Filtered(false, "*")
	assert.Len(t, ns.Tables(), 3)

	ns = s.Filtered(true, "TBL_A.*")
	cols := ns.Columns()
	assert.Len(t, cols, 2)
	for _, col := range cols {
		assert.Equal(t, "TBL_A", col.Table)
	}

	// schema-qualified keys are unchanged
	pg := newSchemata("public", "t1", "t2")
	ns = pg.Filtered(false, "public.*")
	assert.Len(t, ns.Tables(), 2)
	ns = pg.Filtered(false, "public.t1")
	assert.Len(t, ns.Tables(), 1)
	ns = pg.Filtered(true, "public.t1.*")
	assert.Len(t, ns.Columns(), 2)
}
