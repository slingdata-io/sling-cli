package database

import (
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio"
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
	sql := table.Select(SelectOptions{Limit: 10})
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
