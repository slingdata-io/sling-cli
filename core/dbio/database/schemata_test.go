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
	table, err := ParseTableName(`select id, created, type from "dbo"."biz" where "created" >= '2021-01-01 01:40:00' order by "created" asc`, dbio.TypeDbSQLServer)
	if !assert.NoError(t, err) {
		return
	}
	sql := table.Select(10)
	assert.True(t, strings.HasSuffix(sql, ` order by "created" asc`))
	sql = table.Select(0, "id")
	assert.True(t, strings.HasSuffix(sql, ` order by "created" asc`))

	table, err = ParseTableName(`with () select id, created, type from "dbo"."biz" where "created" >= '2021-01-01 01:40:00' order by "created" asc`, dbio.TypeDbSQLServer)
	if !assert.NoError(t, err) {
		return
	}
	sql = table.Select(10)
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
