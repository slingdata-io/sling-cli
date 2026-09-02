package dbio

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplainSQLPrefixKeepsCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"
	got, err := TypeDbPostgres.ExplainSQL(sql)
	require.NoError(t, err)
	assert.Equal(t, "explain "+sql, got)
	assert.True(t, strings.HasPrefix(strings.ToLower(got), "explain with"))
	assert.NotContains(t, got, "from (\n")
}

func TestExplainSQLNestedCTEAndSubquery(t *testing.T) {
	sql := `
WITH a AS (
  SELECT id FROM users WHERE active
), b AS (
  SELECT a.id, o.total FROM a JOIN (SELECT * FROM orders) o ON o.user_id = a.id
)
SELECT * FROM b
`
	got, err := TypeDbSnowflake.ExplainSQL(sql)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(got)), "explain using tabular"))
	assert.Contains(t, got, "WITH a AS")
}

func TestExplainSQLDialects(t *testing.T) {
	sql := "select 1"
	cases := []struct {
		typ    Type
		substr string
	}{
		{TypeDbPostgres, "explain select 1"},
		{TypeDbMySQL, "explain select 1"},
		{TypeDbSQLite, "explain query plan select 1"},
		{TypeDbD1, "explain query plan select 1"},
		{TypeDbOracle, "explain plan for select 1"},
		{TypeDbSnowflake, "explain using tabular select 1"},
		{TypeDbDatabricks, "explain formatted select 1"},
	}
	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			got, err := tc.typ.ExplainSQL(sql)
			require.NoError(t, err)
			assert.Equal(t, tc.substr, strings.TrimSpace(got))
		})
	}
}

func TestExplainSQLServerWrapsCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"
	got, err := TypeDbSQLServer.ExplainSQL(sql)
	require.NoError(t, err)
	assert.Contains(t, got, "select top 0 * from (")
	assert.Contains(t, got, sql)
	assert.Contains(t, got, ") as _sling_explain")
	assert.False(t, strings.HasPrefix(strings.ToLower(strings.TrimSpace(got)), "explain"))
}

func TestExplainSQLBigQueryWrapsCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS x) SELECT * FROM cte"
	got, err := TypeDbBigQuery.ExplainSQL(sql)
	require.NoError(t, err)
	assert.Contains(t, got, "select * from (")
	assert.Contains(t, got, sql)
	assert.Contains(t, got, "where false")
}

func TestExplainSQLStripsSemicolonAndSkipsDouble(t *testing.T) {
	got, err := TypeDbPostgres.ExplainSQL("select 1;")
	require.NoError(t, err)
	assert.Equal(t, "explain select 1", got)

	already := "EXPLAIN SELECT 1"
	got, err = TypeDbPostgres.ExplainSQL(already)
	require.NoError(t, err)
	assert.Equal(t, already, got)
}

func TestExplainSQLUnsupported(t *testing.T) {
	_, err := TypeDbMongoDB.ExplainSQL("select 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not supported")

	_, err = TypeDbPostgres.ExplainSQL("  ;  ")
	require.Error(t, err)
}

func TestExplainTemplatePresentForSQLDatabases(t *testing.T) {
	noExplain := map[Type]bool{
		TypeDbMongoDB:       true,
		TypeDbElasticsearch: true,
		TypeDbAzureTable:    true,
		TypeDbBigTable:      true,
		TypeDbPrometheus:    true,
	}
	for _, td := range AllType {
		if !td.Value.IsDb() {
			continue
		}
		val := strings.TrimSpace(td.Value.GetTemplateValue("core.explain"))
		if noExplain[td.Value] {
			assert.Empty(t, val, "%s should not have core.explain", td.Value)
			continue
		}
		assert.NotEmpty(t, val, "%s missing core.explain", td.Value)
		assert.Contains(t, val, "{sql}", "%s core.explain must include {sql}", td.Value)
	}
}
