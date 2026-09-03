package build

import (
	"fmt"
	"testing"

	"github.com/flarco/g"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBuildSampleProject(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	assert.NotNil(t, b.Project)
	assert.Equal(t, "POSTGRES", b.GetTarget())
}

func TestNewBuildNoTarget(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{})
	require.NoError(t, err)

	// Compile should fail because no target override and yml has target
	// Actually sample_project has target: POSTGRES in sling_build.yml
	err = b.Compile()
	assert.NoError(t, err) // should succeed since yml has target
}

func TestCompileSampleProject(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)

	err = b.Compile()
	require.NoError(t, err)

	// DAG should be built
	assert.NotNil(t, b.DAG)
	assert.NotEmpty(t, b.DAG.Order)

	// All models + seeds should be selected (no selector)
	assert.Len(t, b.Selected, 8) // 6 models + 2 seeds

	// Verify DAG contains all nodes
	assert.Contains(t, b.DAG.Nodes, "stg_orders")
	assert.Contains(t, b.DAG.Nodes, "stg_customers")
	assert.Contains(t, b.DAG.Nodes, "dim_customers")
	assert.Contains(t, b.DAG.Nodes, "fct_orders")
	assert.Contains(t, b.DAG.Nodes, "revenue")
	assert.Contains(t, b.DAG.Nodes, "raw")
	assert.Contains(t, b.DAG.Nodes, "country_codes")
	assert.Contains(t, b.DAG.Nodes, "status_map")

	// Verify dependencies resolved correctly
	fctOrders := b.DAG.Nodes["fct_orders"]
	assert.Contains(t, fctOrders.Dependencies, "stg_orders")

	dimCustomers := b.DAG.Nodes["dim_customers"]
	assert.Contains(t, dimCustomers.Dependencies, "stg_customers")

	revenue := b.DAG.Nodes["revenue"]
	assert.Contains(t, revenue.Dependencies, "fct_orders")
}

func TestCompileWithSelector(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{
		Target: "POSTGRES",
		Select: []string{"stg_*"},
	})
	require.NoError(t, err)

	err = b.Compile()
	require.NoError(t, err)

	// Should only select stg_orders and stg_customers
	assert.Len(t, b.Selected, 2)
	assert.Contains(t, b.Selected, "stg_orders")
	assert.Contains(t, b.Selected, "stg_customers")
}

func TestCompileWithExclude(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{
		Target:  "POSTGRES",
		Exclude: []string{"raw"},
	})
	require.NoError(t, err)

	err = b.Compile()
	require.NoError(t, err)

	// Should have all except 'raw'
	assert.Len(t, b.Selected, 7)
	assert.NotContains(t, b.Selected, "raw")
}

func TestCompileWithUpstreamSelector(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{
		Target: "POSTGRES",
		Select: []string{"+revenue"},
	})
	require.NoError(t, err)

	err = b.Compile()
	require.NoError(t, err)

	// +revenue should include revenue + all upstream: fct_orders, stg_orders
	assert.Contains(t, b.Selected, "revenue")
	assert.Contains(t, b.Selected, "fct_orders")
	assert.Contains(t, b.Selected, "stg_orders")
}

func TestCompileNoTargetError(t *testing.T) {
	dir := t.TempDir()

	b, err := NewBuild(dir, BuildOptions{})
	require.NoError(t, err)

	err = b.Compile()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "No target specified")
}

func TestGetModelMode(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// dim_customers has config(mode='view')
	dimCustomers := b.Project.Models["dim_customers"]
	assert.Equal(t, "view", b.GetModelMode(dimCustomers))

	// fct_orders has config(mode='incremental')
	fctOrders := b.Project.Models["fct_orders"]
	assert.Equal(t, "incremental", b.GetModelMode(fctOrders))

	// stg_orders has no config mode, should use project defaults
	stgOrders := b.Project.Models["stg_orders"]
	assert.Equal(t, "full-refresh", b.GetModelMode(stgOrders))

	// raw has no config mode, should use project defaults
	raw := b.Project.Models["raw"]
	assert.Equal(t, "full-refresh", b.GetModelMode(raw))
}

func TestGetModelModeFullRefreshOverride(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES", FullRefresh: true})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// Even view models should be full-refresh when --full-refresh is set
	dimCustomers := b.Project.Models["dim_customers"]
	assert.Equal(t, "full-refresh", b.GetModelMode(dimCustomers))

	fctOrders := b.Project.Models["fct_orders"]
	assert.Equal(t, "full-refresh", b.GetModelMode(fctOrders))
}

func TestCompileDevMode(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{
		Target: "POSTGRES",
		Schema: "dev_test",
	})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// All models should be in dev_test schema
	for _, model := range b.Project.Models {
		assert.Equal(t, "dev_test", model.Schema, "model %s should use dev schema", model.Name)
	}
}

func TestCompileMultiTarget(t *testing.T) {
	dir := getTestFixturePath("multi_target_project")

	b, err := NewBuild(dir, BuildOptions{Recursive: true})
	require.NoError(t, err)

	// Multi-target projects have sub-projects
	assert.Len(t, b.Project.SubProjects, 2)

	// Compile should succeed (sub-projects are handled separately)
	err = b.Compile()
	assert.NoError(t, err)
}

func TestCompileNestedConfig(t *testing.T) {
	dir := getTestFixturePath("nested_yml_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES", Recursive: true})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// stg_orders should have mode=truncate (from child config)
	stgOrders := b.Project.Models["stg_orders"]
	assert.Equal(t, "truncate", b.GetModelMode(stgOrders))

	// dim_customers should have mode=full-refresh (from root config)
	dimCustomers := b.Project.Models["dim_customers"]
	assert.Equal(t, "full-refresh", b.GetModelMode(dimCustomers))
}

func TestPrintCompileOutput(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// Just verify it doesn't panic — output goes to stdout
	b.PrintCompileOutput()
}

func TestCompileEmptyProject(t *testing.T) {
	dir := t.TempDir()

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	assert.NotNil(t, b.DAG)
	assert.Len(t, b.Selected, 0)
}

func TestGetTarget(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	// Target from yml
	b, err := NewBuild(dir, BuildOptions{})
	require.NoError(t, err)
	assert.Equal(t, "POSTGRES", b.GetTarget())

	// CLI target overrides yml
	b, err = NewBuild(dir, BuildOptions{Target: "CLICKHOUSE"})
	require.NoError(t, err)
	assert.Equal(t, "CLICKHOUSE", b.GetTarget())

	// Empty project, target from options
	b, err = NewBuild(t.TempDir(), BuildOptions{Target: "MY_DB"})
	require.NoError(t, err)
	assert.Equal(t, "MY_DB", b.GetTarget())
}

func TestCompileDbtCompatProject(t *testing.T) {
	dir := getTestFixturePath("dbt_compat_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	assert.Len(t, b.Selected, 2) // 1 model + 1 seed
	assert.Contains(t, b.Selected, "stg_orders")
	assert.Contains(t, b.Selected, "country_codes")
}

func TestSplitModelSQL_SingleSelect(t *testing.T) {
	sql := `SELECT id, name FROM customers WHERE active = true`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 0 {
		t.Errorf("expected 0 pre-statements, got %d", len(pre))
	}
	if model == "" {
		t.Error("model query is empty")
	}
	if len(post) != 0 {
		t.Errorf("expected 0 post-statements, got %d", len(post))
	}
	t.Logf("model: %s", model)
}

func TestSplitModelSQL_PreAndPost(t *testing.T) {
	sql := `CREATE TEMP TABLE raw_orders AS SELECT * FROM source_orders;
CREATE INDEX idx_raw ON raw_orders(id);

SELECT id, customer_name, total FROM raw_orders;

DROP TABLE IF EXISTS raw_orders;
ANALYZE staging.dim_orders;`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}

	if len(pre) != 2 {
		t.Errorf("expected 2 pre-statements, got %d", len(pre))
	}
	if model == "" {
		t.Error("model query is empty")
	}
	if len(post) != 2 {
		t.Errorf("expected 2 post-statements, got %d", len(post))
	}

	t.Logf("pre[0]: %s", pre[0])
	t.Logf("pre[1]: %s", pre[1])
	t.Logf("model: %s", model)
	t.Logf("post[0]: %s", post[0])
	t.Logf("post[1]: %s", post[1])
}

func TestSplitModelSQL_PreOnly(t *testing.T) {
	sql := `CREATE TEMP TABLE tmp AS SELECT 1 AS x;
SELECT * FROM tmp;`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}

	if len(pre) != 1 {
		t.Errorf("expected 1 pre-statement, got %d", len(pre))
	}
	if model == "" {
		t.Error("model query is empty")
	}
	if len(post) != 0 {
		t.Errorf("expected 0 post-statements, got %d", len(post))
	}
}

func TestSplitModelSQL_PostOnly(t *testing.T) {
	sql := `SELECT * FROM orders;
DROP TABLE IF EXISTS tmp;`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}

	if len(pre) != 0 {
		t.Errorf("expected 0 pre-statements, got %d", len(pre))
	}
	if model == "" {
		t.Error("model query is empty")
	}
	if len(post) != 1 {
		t.Errorf("expected 1 post-statement, got %d", len(post))
	}
}

func TestSplitModelSQL_ZeroQueries(t *testing.T) {
	sql := `CREATE TABLE t (id INT);
DROP TABLE t;`

	_, _, _, err := SplitModelSQL(sql, "postgres")
	if err == nil {
		t.Fatal("expected error for zero queries")
	}
	t.Logf("expected error: %v", err)
}

func TestSplitModelSQL_MultipleQueries(t *testing.T) {
	sql := `SELECT 1;
SELECT 2;`

	_, _, _, err := SplitModelSQL(sql, "postgres")
	if err == nil {
		t.Fatal("expected error for multiple queries")
	}
	t.Logf("expected error: %v", err)
}

func TestSplitModelSQL_CTE(t *testing.T) {
	sql := `WITH cte AS (SELECT id FROM raw_data)
SELECT * FROM cte WHERE id > 0`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 0 || len(post) != 0 {
		t.Errorf("CTE should have no pre/post: pre=%d, post=%d", len(pre), len(post))
	}
	if model == "" {
		t.Error("model is empty")
	}
	t.Logf("model: %s", model)
}

func TestSplitModelSQL_UnionAll(t *testing.T) {
	sql := `SELECT 1 AS id UNION ALL SELECT 2 AS id`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 0 || len(post) != 0 {
		t.Errorf("UNION should have no pre/post: pre=%d, post=%d", len(pre), len(post))
	}
	if model == "" {
		t.Error("model is empty")
	}
	t.Logf("model: %s", model)
}

func TestSplitModelSQL_CreateTableAsSelect(t *testing.T) {
	// CREATE TABLE AS SELECT should be classified as DDL, not query
	sql := `CREATE TEMP TABLE staging AS SELECT * FROM raw;
SELECT id, name FROM staging;`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 1 {
		t.Errorf("CTAS should be a pre-statement, got %d pre", len(pre))
	}
	if model == "" {
		t.Error("model is empty")
	}
	if len(post) != 0 {
		t.Errorf("expected 0 post, got %d", len(post))
	}
	t.Logf("pre[0]: %s", pre[0])
	t.Logf("model: %s", model)
}

func TestSplitModelSQL_SemicolonInStringLiteral(t *testing.T) {
	// The semicolon inside the string literal should NOT cause a split
	sql := `SELECT 'hello; world' AS greeting`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 0 || len(post) != 0 {
		t.Errorf("semicolon in string should not split: pre=%d, post=%d", len(pre), len(post))
	}
	if model == "" {
		t.Error("model is empty")
	}
}

func TestSplitModelSQL_SemicolonInComment(t *testing.T) {
	sql := `-- this is a comment; with semicolon
SELECT 1`

	pre, model, post, err := SplitModelSQL(sql, "postgres")
	if err != nil {
		t.Fatalf("SplitModelSQL failed: %v", err)
	}
	if len(pre) != 0 || len(post) != 0 {
		t.Errorf("semicolon in comment should not split: pre=%d, post=%d", len(pre), len(post))
	}
	if model == "" {
		t.Error("model is empty")
	}
}

func TestStyleCheck(t *testing.T) {
	sql := `{%- config(mode='incremental', unique_key='id', merge_strategy='delete+insert', update_key='created_at') -%}

SELECT
    id,
    name,
    created_at
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
{% endif %}`
	style, err := detectModelStyle(sql)
	fmt.Println("style:", style, "err:", err)
	fmt.Println("StyleDbt:", StyleDbt, "StyleSling:", StyleSling)
}

func TestCompileDatabaseUnsupportedDialect(t *testing.T) {
	project, err := LoadProject(getTestFixturePath("database_project"), BuildOptions{Prod: true})
	require.NoError(t, err)

	b := &Build{Project: project, Options: BuildOptions{Target: "POSTGRES", Prod: true}}
	err = b.Compile()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not support database.schema.table")
}

// The platform scheduler gates a build run on the "compiled" flag surviving the
// agent-to-master JSON round-trip. Dropping it errors with "no model compiled".
func TestJSONPayloadCarriesCompiled(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	payload := b.CompileJSONPayload()
	require.Contains(t, payload, "compiled")
	assert.Equal(t, true, payload["compiled"])

	var got *BuildConfig
	require.NoError(t, g.JSONConvert(payload, &got))
	assert.True(t, got.Compiled)
	assert.NotEmpty(t, got.Nodes)

	// a build that never compiled must stay false
	var nilBuild *Build
	assert.Equal(t, false, nilBuild.Compiled().JSONPayload()["compiled"])
}
