package build

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Macro Tests
// =============================================================================

func TestMacroApplies(t *testing.T) {
	tests := []struct {
		name     string
		macroDir string
		modelDir string
		want     bool
	}{
		{"root macro applies to root model", "", "", true},
		{"root macro applies to staging model", "", "staging", true},
		{"root macro applies to deeply nested model", "", "marts/core/sub", true},
		{"staging macro applies to staging model", "staging", "staging", true},
		{"staging macro applies to staging child", "staging", "staging/sub", true},
		{"staging macro does not apply to marts model", "staging", "marts", false},
		{"staging macro does not apply to root model", "staging", "", false},
		{"marts/core macro applies to marts/core model", "marts/core", "marts/core", true},
		{"marts/core macro applies to marts/core/sub", "marts/core", "marts/core/sub", true},
		{"marts/core macro does not apply to marts model", "marts/core", "marts", false},
		{"marts macro does not apply to marts_v2 model", "marts", "marts_v2", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := macroApplies(tt.macroDir, tt.modelDir)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDirDepth(t *testing.T) {
	assert.Equal(t, 0, dirDepth(""))
	assert.Equal(t, 1, dirDepth("staging"))
	assert.Equal(t, 2, dirDepth("marts/core"))
	assert.Equal(t, 3, dirDepth("marts/core/sub"))
}

func TestGetMacrosForModel(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/utils.macros.sql",
				Dir:      "",
				RawSQL:   "{% macro global_fn() %}GLOBAL{% endmacro %}",
			},
			{
				FilePath: "/test/staging/helpers.macros.sql",
				Dir:      "staging",
				RawSQL:   "{% macro staging_fn() %}STAGING{% endmacro %}",
			},
			{
				FilePath: "/test/marts/marts_utils.macros.sql",
				Dir:      "marts",
				RawSQL:   "{% macro marts_fn() %}MARTS{% endmacro %}",
			},
		},
	}

	// Staging model should get global + staging macros
	stagingModel := &Model{
		Name:    "stg_orders",
		RelPath: "staging/stg_orders.sql",
	}
	result := GetMacrosForModel(project, stagingModel)
	assert.Contains(t, result, "global_fn")
	assert.Contains(t, result, "staging_fn")
	assert.NotContains(t, result, "marts_fn")

	// Marts model should get global + marts macros
	martsModel := &Model{
		Name:    "dim_customers",
		RelPath: "marts/core/dim_customers.sql",
	}
	result = GetMacrosForModel(project, martsModel)
	assert.Contains(t, result, "global_fn")
	assert.Contains(t, result, "marts_fn")
	assert.NotContains(t, result, "staging_fn")

	// Root model should get only global macros
	rootModel := &Model{
		Name:    "raw",
		RelPath: "raw.sql",
	}
	result = GetMacrosForModel(project, rootModel)
	assert.Contains(t, result, "global_fn")
	assert.NotContains(t, result, "staging_fn")
	assert.NotContains(t, result, "marts_fn")
}

func TestGetMacrosForModelOrdering(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/staging/b_helpers.macros.sql",
				Dir:      "staging",
				RawSQL:   "STAGING_B",
			},
			{
				FilePath: "/test/utils.macros.sql",
				Dir:      "",
				RawSQL:   "GLOBAL",
			},
			{
				FilePath: "/test/staging/a_helpers.macros.sql",
				Dir:      "staging",
				RawSQL:   "STAGING_A",
			},
		},
	}

	model := &Model{
		Name:    "stg_orders",
		RelPath: "staging/stg_orders.sql",
	}

	result := GetMacrosForModel(project, model)

	// Root macros should come before staging macros
	globalIdx := strings.Index(result, "GLOBAL")
	stagingAIdx := strings.Index(result, "STAGING_A")
	stagingBIdx := strings.Index(result, "STAGING_B")

	assert.True(t, globalIdx < stagingAIdx, "global macros should come before staging macros")
	assert.True(t, stagingAIdx < stagingBIdx, "same-dir macros should be sorted by filename")
}

func TestGetMacrosForModelEmpty(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
	}

	model := &Model{
		Name:    "test",
		RelPath: "staging/test.sql",
	}

	result := GetMacrosForModel(project, model)
	assert.Empty(t, result)
}

func TestCollectMacro(t *testing.T) {
	// Create a temp macro file
	dir := t.TempDir()
	macroPath := filepath.Join(dir, "test.macros.sql")
	content := "{% macro test_fn() %}TEST{% endmacro %}"
	require.NoError(t, os.WriteFile(macroPath, []byte(content), 0644))

	project := &BuildProject{
		Dir:           dir,
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		DefaultSchema: "public",
	}

	err := collectMacro(project, macroPath, "")
	require.NoError(t, err)

	assert.Len(t, project.Macros, 1)
	assert.Equal(t, macroPath, project.Macros[0].FilePath)
	assert.Equal(t, "", project.Macros[0].Dir)
	assert.Equal(t, content, project.Macros[0].RawSQL)
}

func TestMacroDiscoveryFlatMode(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	project, err := LoadProject(dir)
	require.NoError(t, err)

	// Should discover macro files
	assert.Len(t, project.Macros, 2, "should discover utils.macros.sql and staging_helpers.macros.sql")

	// Model and seed counts should be unchanged
	assert.Len(t, project.Models, 6)
	assert.Len(t, project.Seeds, 2)

	// Verify macro dirs
	macroDirs := make(map[string]bool)
	for _, m := range project.Macros {
		macroDirs[m.Dir] = true
	}
	assert.True(t, macroDirs[""], "should have root-level macro")
	assert.True(t, macroDirs["staging"], "should have staging-level macro")
}

func TestMacroCompilationEndToEnd(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/utils.macros.sql",
				Dir:      "",
				RawSQL: `{% macro cents_to_dollars(column_name) %}
    ({{ column_name }} / 100.0)
{% endmacro %}`,
			},
		},
	}

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RelPath:       "staging/test_model.sql",
		RawSQL:        "SELECT id, {{ cents_to_dollars('amount_cents') }} as amount_dollars FROM orders",
	}
	project.Models["test_model"] = model

	te := NewTemplateEngine(project, nil)
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "(amount_cents / 100.0)")
	assert.Contains(t, result, "as amount_dollars")
}

func TestMacroScopeIsolation(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/staging/helpers.macros.sql",
				Dir:      "staging",
				RawSQL:   "{% macro staging_only() %}STAGING_RESULT{% endmacro %}",
			},
		},
	}

	// Staging model can use the staging-scoped macro
	stagingModel := &Model{
		Name:          "stg_test",
		Schema:        "staging",
		FullTableName: "staging.stg_test",
		RelPath:       "staging/stg_test.sql",
		RawSQL:        "SELECT {{ staging_only() }} as val",
	}
	project.Models["stg_test"] = stagingModel

	te := NewTemplateEngine(project, nil)
	result, err := te.CompileModel(stagingModel, nil)
	require.NoError(t, err)
	assert.Contains(t, result, "STAGING_RESULT")

	// Marts model cannot use the staging-scoped macro
	martsModel := &Model{
		Name:          "dim_test",
		Schema:        "marts",
		FullTableName: "marts.dim_test",
		RelPath:       "marts/dim_test.sql",
		RawSQL:        "SELECT {{ staging_only() }} as val",
	}
	project.Models["dim_test"] = martsModel

	_, err = te.CompileModel(martsModel, nil)
	assert.Error(t, err, "marts model should not have access to staging-scoped macro")
}

func TestMacroWithMultipleArgs(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/utils.macros.sql",
				Dir:      "",
				RawSQL: `{% macro safe_divide(numerator, denominator) %}
    CASE WHEN {{ denominator }} = 0 THEN NULL ELSE {{ numerator }}::float / {{ denominator }} END
{% endmacro %}`,
			},
		},
	}

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RelPath:       "staging/test_model.sql",
		RawSQL:        "SELECT {{ safe_divide('revenue', 'num_orders') }} as avg_order_value",
	}
	project.Models["test_model"] = model

	te := NewTemplateEngine(project, nil)
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "CASE WHEN num_orders = 0 THEN NULL")
	assert.Contains(t, result, "revenue::float / num_orders")
}

func TestMacroWithExistingModelRefs(t *testing.T) {
	project := &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Macros: []*MacroFile{
			{
				FilePath: "/test/utils.macros.sql",
				Dir:      "",
				RawSQL:   "{% macro add_prefix(val) %}prefix_{{ val }}{% endmacro %}",
			},
		},
	}

	project.Models["stg_orders"] = &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RelPath:       "staging/stg_orders.sql",
		RawSQL:        "SELECT 1 as id",
	}

	// Model that uses both macros and refs
	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RelPath:       "staging/test_model.sql",
		RawSQL:        "SELECT {{ add_prefix('hello') }} FROM {{ ref('stg_orders') }}",
	}
	project.Models["test_model"] = model

	te := NewTemplateEngine(project, nil)
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "prefix_hello")
	assert.Contains(t, result, "FROM staging.stg_orders")
	assert.Contains(t, model.Refs, "stg_orders")
}

func TestMacroSampleProjectCompiles(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)

	err = b.Compile()
	require.NoError(t, err)

	// Macros are present but not called by existing models — should compile fine
	assert.NotNil(t, b.DAG)
	assert.Len(t, b.Selected, 8) // 6 models + 2 seeds, unchanged
}

// =============================================================================
// SQL Parser Tests
// =============================================================================

func TestSQLParserSimpleFrom(t *testing.T) {
	sql := `SELECT * FROM staging.orders`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
}

func TestSQLParserMultipleFromJoin(t *testing.T) {
	sql := `SELECT o.*, c.name
FROM staging.orders o
JOIN staging.customers c ON o.customer_id = c.id
LEFT JOIN public.products p ON o.product_id = p.id`

	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.customers")
	assert.Contains(t, refs, "public.products")
	assert.Len(t, refs, 3)
}

func TestSQLParserUnqualifiedTable(t *testing.T) {
	sql := `SELECT * FROM orders`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "orders")
}

func TestSQLParserQuotedIdentifiers(t *testing.T) {
	sql := `SELECT * FROM "my schema"."my table"`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "my schema.my table")
}

func TestSQLParserBacktickQuotes(t *testing.T) {
	sql := "SELECT * FROM `staging`.`orders`"
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
}

func TestSQLParserSkipsCTEs(t *testing.T) {
	sql := `WITH cte_orders AS (
    SELECT * FROM staging.raw_orders
),
cte_customers AS (
    SELECT * FROM staging.raw_customers
)
SELECT o.*, c.name
FROM cte_orders o
JOIN cte_customers c ON o.customer_id = c.id`

	refs := ExtractTableReferences(sql)
	// Should contain the real tables, not the CTEs
	assert.Contains(t, refs, "staging.raw_orders")
	assert.Contains(t, refs, "staging.raw_customers")
	assert.NotContains(t, refs, "cte_orders")
	assert.NotContains(t, refs, "cte_customers")
}

func TestSQLParserSkipsJinja(t *testing.T) {
	sql := `SELECT * FROM {{ ref('stg_orders') }}
JOIN {{ src('raw_db.accounts') }} ON 1=1`

	refs := ExtractTableReferences(sql)
	// Jinja expressions should be stripped, not matched as tables
	assert.NotContains(t, refs, "ref")
	assert.NotContains(t, refs, "src")
	// __JINJA__ placeholder might be captured but should be filtered
	for _, ref := range refs {
		assert.NotContains(t, ref, "JINJA")
	}
}

func TestSQLParserDeduplicates(t *testing.T) {
	sql := `SELECT * FROM staging.orders
UNION ALL
SELECT * FROM staging.orders`

	refs := ExtractTableReferences(sql)
	// Count occurrences of staging.orders
	count := 0
	for _, ref := range refs {
		if ref == "staging.orders" {
			count++
		}
	}
	assert.Equal(t, 1, count, "should deduplicate")
}

func TestSQLParserThreePartName(t *testing.T) {
	sql := `SELECT * FROM mydb.staging.orders`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "mydb.staging.orders")
}

func TestSQLParserSubqueryNotMatched(t *testing.T) {
	sql := `SELECT * FROM staging.orders WHERE id IN (SELECT order_id FROM staging.line_items)`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.line_items")
}

func TestSQLParserLeftOuterJoin(t *testing.T) {
	sql := `SELECT * FROM staging.orders o LEFT OUTER JOIN staging.customers c ON o.id = c.order_id`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.customers")
}

func TestSQLParserCrossJoin(t *testing.T) {
	sql := `SELECT * FROM staging.orders CROSS JOIN staging.dates`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.dates")
}

func TestSQLParserEmptySQL(t *testing.T) {
	refs := ExtractTableReferences("")
	assert.Empty(t, refs)
}

func TestSQLParserNoFrom(t *testing.T) {
	sql := `SELECT 1 as id, 'test' as name`
	refs := ExtractTableReferences(sql)
	assert.Empty(t, refs)
}

func TestSQLParserSkipsReservedWords(t *testing.T) {
	// Ensure we don't match keywords that might appear after FROM-like constructs
	sql := `SELECT * FROM staging.orders WHERE EXISTS (SELECT 1 FROM staging.items)`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.items")
	assert.NotContains(t, refs, "exists")
}

func TestSQLParserMultilineSQL(t *testing.T) {
	sql := `
SELECT
    o.id,
    o.name
FROM
    staging.orders o
INNER JOIN
    staging.customers c
    ON o.customer_id = c.id
WHERE
    o.status = 'active'
`
	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "staging.orders")
	assert.Contains(t, refs, "staging.customers")
}

func TestSQLParserMixedRefsAndAutoDetect(t *testing.T) {
	// SQL that has both Jinja refs (stripped) and real table references
	sql := `SELECT * FROM {{ ref('stg_orders') }}
JOIN raw_db.accounts ON 1=1`

	refs := ExtractTableReferences(sql)
	assert.Contains(t, refs, "raw_db.accounts")
}

// =============================================================================
// Template Engine Tests
// =============================================================================

func newTestProject() *BuildProject {
	return &BuildProject{
		Dir:           "/test",
		Mode:          "prod",
		DefaultSchema: "public",
		Models: map[string]*Model{
			"stg_orders": {
				Name:              "stg_orders",
				Schema:            "staging",
				FullTableName:     "staging.stg_orders",
				ProdFullTableName: "staging.stg_orders",
				RawSQL:            "SELECT 1 as id, 'order_1' as name",
			},
			"stg_customers": {
				Name:              "stg_customers",
				Schema:            "staging",
				FullTableName:     "staging.stg_customers",
				ProdFullTableName: "staging.stg_customers",
				RawSQL:            "SELECT 1 as id, 'customer_1' as name",
			},
		},
		Seeds: map[string]*Seed{
			"country_codes": {
				Name:              "country_codes",
				Schema:            "staging",
				FullTableName:     "staging.country_codes",
				ProdFullTableName: "staging.country_codes",
				Format:            "csv",
			},
		},
	}
}

func newTestProjectDev() *BuildProject {
	return &BuildProject{
		Dir:            "/test",
		Mode:           "dev",
		SchemaOverride: "dev_fritz",
		DefaultSchema:  "public",
		Models: map[string]*Model{
			"stg_orders": {
				Name:              "stg_orders",
				Schema:            "dev_fritz",
				FullTableName:     "dev_fritz.stg_orders",
				ProdFullTableName: "staging.stg_orders",
				RawSQL:            "SELECT 1 as id, 'order_1' as name",
			},
			"stg_customers": {
				Name:              "stg_customers",
				Schema:            "dev_fritz",
				FullTableName:     "dev_fritz.stg_customers",
				ProdFullTableName: "staging.stg_customers",
				RawSQL:            "SELECT 1 as id, 'customer_1' as name",
			},
		},
		Seeds: map[string]*Seed{
			"country_codes": {
				Name:              "country_codes",
				Schema:            "dev_fritz",
				FullTableName:     "dev_fritz.country_codes",
				ProdFullTableName: "staging.country_codes",
				Format:            "csv",
			},
		},
	}
}

func TestCompileSimpleModel(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := project.Models["stg_orders"]
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT 1 as id, 'order_1' as name", result)
	assert.Empty(t, model.Refs)
	assert.Empty(t, model.Sources)
	assert.Empty(t, model.DependsOn)
}

func TestCompileWithRef(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_customers",
		Schema:        "marts",
		FullTableName: "marts.dim_customers",
		RawSQL:        "SELECT * FROM {{ ref('stg_customers') }}",
	}
	project.Models["dim_customers"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM staging.stg_customers", result)
	assert.Contains(t, model.Refs, "stg_customers")
	assert.Contains(t, model.DependsOn, "stg_customers")
}

func TestCompileWithSeedRef(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_countries",
		Schema:        "marts",
		FullTableName: "marts.dim_countries",
		RawSQL:        "SELECT * FROM {{ ref('country_codes') }}",
	}
	project.Models["dim_countries"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM staging.country_codes", result)
	assert.Contains(t, model.Refs, "country_codes")
}

func TestCompileWithSrc(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT * FROM {{ src('raw_db.accounts') }}",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM raw_db.accounts", result)
	assert.Contains(t, model.Sources, "raw_db.accounts")
	assert.Empty(t, model.DependsOn) // src() does not create DAG edges
}

func TestCompileWithSrcTwoArgs(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT * FROM {{ src('raw_db', 'accounts') }}",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM raw_db.accounts", result)
	assert.Contains(t, model.Sources, "raw_db.accounts")
	assert.Empty(t, model.DependsOn)
}

func TestCompileWithSource(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT * FROM {{ source('raw_db', 'accounts') }}",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM raw_db.accounts", result)
	assert.Contains(t, model.Sources, "raw_db.accounts")
	assert.Empty(t, model.DependsOn)
}

func TestCompileWithSourceOneArg(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT * FROM {{ source('raw_db.accounts') }}",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM raw_db.accounts", result)
	assert.Contains(t, model.Sources, "raw_db.accounts")
	assert.Empty(t, model.DependsOn)
}

func TestCompileWithThis(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_orders",
		Schema:        "marts",
		FullTableName: "marts.fct_orders",
		RawSQL:        "SELECT MAX(updated_at) FROM {{ this }}",
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT MAX(updated_at) FROM marts.fct_orders", result)
}

func TestCompileWithIsIncremental(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_orders",
		Schema:        "marts",
		FullTableName: "marts.fct_orders",
		RawSQL: `SELECT id FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}`,
	}
	project.Models["fct_orders"] = model

	// Not incremental
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.NotContains(t, result, "WHERE updated_at")
	assert.Contains(t, result, "SELECT id FROM staging.stg_orders")

	// Reset refs for re-compile
	model.Refs = nil
	model.DependsOn = nil

	// Incremental
	result, err = te.CompileModel(model, &IncrementalContext{IsIncremental: true})
	require.NoError(t, err)
	assert.Contains(t, result, "WHERE updated_at > (SELECT MAX(updated_at) FROM marts.fct_orders)")
}

func TestCompileWithConfig(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_orders",
		Schema:        "marts",
		FullTableName: "marts.fct_orders",
		RawSQL: `{%- config(mode='incremental', unique_key='id', merge_strategy='delete+insert', update_key='updated_at') -%}
SELECT id, name FROM {{ ref('stg_orders') }}`,
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// Config block should produce no output
	assert.Equal(t, "SELECT id, name FROM staging.stg_orders", result)

	// Config should be extracted
	assert.Equal(t, "incremental", model.Config.Mode)
	assert.Equal(t, "id", model.Config.UniqueKey)
	assert.Equal(t, "delete+insert", model.Config.MergeStrategy)
	assert.Equal(t, "updated_at", model.Config.UpdateKey)
}

func TestCompileWithVars(t *testing.T) {
	project := newTestProject()
	vars := map[string]any{
		"start_date":  "2024-01-01",
		"environment": "dev",
	}
	te := NewTemplateEngine(project, vars)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT * FROM orders WHERE created_at >= '{{ start_date }}' AND env = '{{ environment }}'",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "2024-01-01")
	assert.Contains(t, result, "dev")
}

func TestCompileWithAtRef(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_customers",
		Schema:        "marts",
		FullTableName: "marts.dim_customers",
		RawSQL:        "SELECT * FROM @stg_customers",
	}
	project.Models["dim_customers"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// @model_name resolves to the model's full table name
	assert.Equal(t, "SELECT * FROM staging.stg_customers", result)
	assert.Contains(t, model.DependsOn, "stg_customers")
}

func TestCompileWithAtRefSeed(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_countries",
		Schema:        "marts",
		FullTableName: "marts.dim_countries",
		RawSQL:        "SELECT * FROM @country_codes",
	}
	project.Models["dim_countries"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM staging.country_codes", result)
	assert.Contains(t, model.DependsOn, "country_codes")
}

func TestCompileWithAtRefNoMatch(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT @rowcount, @unknown_var FROM @stg_orders",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// @rowcount and @unknown_var are not models, left as-is
	// @stg_orders is a model, gets resolved
	assert.Contains(t, result, "@rowcount")
	assert.Contains(t, result, "@unknown_var")
	assert.Contains(t, result, "staging.stg_orders")
}

func TestCompileWithDoubleAtSkipped(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL:        "SELECT @@version, * FROM @stg_orders",
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// @@version should not be touched
	assert.Contains(t, result, "@@version")
	assert.Contains(t, result, "staging.stg_orders")
}

func TestCompileWithBareRefRemoved(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_customers",
		Schema:        "marts",
		FullTableName: "marts.dim_customers",
		RawSQL:        "SELECT * FROM {{ stg_customers }}",
	}
	project.Models["dim_customers"] = model

	// Bare Jinja variable {{ stg_customers }} no longer resolves to a full table name.
	// gonja renders undefined variables as empty string, so the SQL will be broken.
	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	// The variable is no longer registered, so it renders as empty
	assert.Equal(t, "SELECT * FROM", result)
}

func TestCompileRefNotFound(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "bad_model",
		Schema:        "staging",
		FullTableName: "staging.bad_model",
		RawSQL:        "SELECT * FROM {{ ref('nonexistent_model') }}",
	}
	project.Models["bad_model"] = model

	_, err := te.CompileModel(model, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent_model")
}

func TestCompileWithConfigView(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "dim_view",
		Schema:        "marts",
		FullTableName: "marts.dim_view",
		RawSQL: `{%- config(mode='view') -%}
SELECT * FROM {{ ref('stg_customers') }}`,
	}
	project.Models["dim_view"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT * FROM staging.stg_customers", result)
	assert.Equal(t, "view", model.Config.Mode)
}

func TestCompileWithConfigUnknownModeRejected(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "bad_mode",
		Schema:        "marts",
		FullTableName: "marts.bad_mode",
		RawSQL: `{%- config(mode='vew') -%}
SELECT 1`,
	}
	project.Models["bad_mode"] = model

	_, err := te.CompileModel(model, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown mode 'vew'")
}

func TestCompileWithConfigEnabled(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "disabled_model",
		Schema:        "staging",
		FullTableName: "staging.disabled_model",
		RawSQL: `{%- config(enabled=false) -%}
SELECT 1`,
	}
	project.Models["disabled_model"] = model

	_, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.NotNil(t, model.Config.Enabled)
	assert.False(t, *model.Config.Enabled)
}

func TestCompileMultipleRefs(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_combined",
		Schema:        "marts",
		FullTableName: "marts.fct_combined",
		RawSQL: `SELECT o.*, c.name as customer_name
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.id = c.id`,
	}
	project.Models["fct_combined"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "FROM staging.stg_orders o")
	assert.Contains(t, result, "JOIN staging.stg_customers c")
	assert.Len(t, model.Refs, 2)
	assert.Contains(t, model.Refs, "stg_orders")
	assert.Contains(t, model.Refs, "stg_customers")
	assert.Len(t, model.DependsOn, 2)
}

func TestCompileWithJinjaControlFlow(t *testing.T) {
	project := newTestProject()
	vars := map[string]any{
		"include_archived": true,
	}
	te := NewTemplateEngine(project, vars)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL: `SELECT * FROM {{ ref('stg_orders') }}
{% if include_archived %}
WHERE status IN ('active', 'archived')
{% else %}
WHERE status = 'active'
{% endif %}`,
	}
	project.Models["test_model"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Contains(t, result, "WHERE status IN ('active', 'archived')")
	assert.NotContains(t, result, "WHERE status = 'active'")
}

func TestCompileAllModels(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	err := te.CompileAll(nil)
	require.NoError(t, err)

	for _, model := range project.Models {
		assert.NotEmpty(t, model.CompiledSQL, "model %s should have compiled SQL", model.Name)
	}
}

func TestCompileDuplicateRef(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
		RawSQL: `SELECT * FROM {{ ref('stg_orders') }}
UNION ALL
SELECT * FROM {{ ref('stg_orders') }}`,
	}
	project.Models["test_model"] = model

	_, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// Duplicate refs should only appear once
	assert.Len(t, model.Refs, 1)
	assert.Len(t, model.DependsOn, 1)
}

// =============================================================================
// YAML Frontmatter Compile Tests
// =============================================================================

func TestCompileWithYAMLFrontmatter(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:           "fct_orders",
		Schema:         "marts",
		FullTableName:  "marts.fct_orders",
		HasFrontmatter: true,
		Config: ModelConfig{
			Mode:          "incremental",
			UniqueKey:     "id",
			MergeStrategy: "delete+insert",
			UpdateKey:     "updated_at",
		},
		RawSQL: "SELECT id, name FROM {{ ref('stg_orders') }}",
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT id, name FROM staging.stg_orders", result)
	assert.Equal(t, "incremental", model.Config.Mode)
	assert.Equal(t, "id", model.Config.UniqueKey)
	assert.Equal(t, "delete+insert", model.Config.MergeStrategy)
	assert.Equal(t, "updated_at", model.Config.UpdateKey)
}

func TestCompileFrontmatterOverridesJinjaConfig(t *testing.T) {
	// When YAML frontmatter is present, jinja config() should be a no-op
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:           "fct_orders",
		Schema:         "marts",
		FullTableName:  "marts.fct_orders",
		HasFrontmatter: true,
		Config: ModelConfig{
			Mode:      "incremental",
			UniqueKey: "id",
		},
		// This jinja config should be ignored because frontmatter takes precedence
		RawSQL: `{%- config(mode='full-refresh', unique_key='other_id') -%}
SELECT id FROM {{ ref('stg_orders') }}`,
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	// SQL should be rendered (config block produces empty string)
	assert.Equal(t, "SELECT id FROM staging.stg_orders", result)

	// Config should retain frontmatter values, NOT jinja overrides
	assert.Equal(t, "incremental", model.Config.Mode)
	assert.Equal(t, "id", model.Config.UniqueKey)
}

func TestCompileJinjaConfigFallback(t *testing.T) {
	// When no frontmatter, jinja config() works as before (dbt compat)
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:           "fct_orders",
		Schema:         "marts",
		FullTableName:  "marts.fct_orders",
		HasFrontmatter: false,
		RawSQL: `{%- config(mode='incremental', unique_key='id') -%}
SELECT id FROM {{ ref('stg_orders') }}`,
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)

	assert.Equal(t, "SELECT id FROM staging.stg_orders", result)
	assert.Equal(t, "incremental", model.Config.Mode)
	assert.Equal(t, "id", model.Config.UniqueKey)
}

func TestContainsSemicolon(t *testing.T) {
	tests := []struct {
		sql      string
		expected bool
	}{
		{"SELECT 1", false},
		{"SELECT 1;", true},
		{"SELECT 'a;b'", false},
		{"SELECT 1 -- comment; here\nFROM t", false},
		{"SELECT 1; SELECT 2", true},
		{"SELECT /* ; */ 1", false},
		{"SELECT 1; -- trailing", true},
	}

	for _, tt := range tests {
		got := containsSemicolon(tt.sql)
		if got != tt.expected {
			t.Errorf("containsSemicolon(%q) = %v, want %v", tt.sql, got, tt.expected)
		}
	}
}

// =============================================================================
// RewriteTableReferences Tests
// =============================================================================

func TestRewriteTableReferences_ProdMode(t *testing.T) {
	project := newTestProject()

	// In prod mode, prod name == current name, so rewriting is a no-op
	sql := "SELECT * FROM staging.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM staging.stg_orders", rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_DevMode(t *testing.T) {
	project := newTestProjectDev()

	sql := "SELECT * FROM staging.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM dev_fritz.stg_orders", rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_DevModeMultiple(t *testing.T) {
	project := newTestProjectDev()

	sql := `SELECT o.*, c.name
FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.id = c.id`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Contains(t, rewritten, "FROM dev_fritz.stg_orders o")
	assert.Contains(t, rewritten, "JOIN dev_fritz.stg_customers c")
	assert.Contains(t, deps, "stg_orders")
	assert.Contains(t, deps, "stg_customers")
}

func TestRewriteTableReferences_NoMatch(t *testing.T) {
	project := newTestProjectDev()

	// External table not in project — should not be rewritten
	sql := "SELECT * FROM external_db.raw_events"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM external_db.raw_events", rewritten)
	assert.Empty(t, deps)
}

func TestRewriteTableReferences_StringsNotRewritten(t *testing.T) {
	project := newTestProjectDev()

	// Table name inside string literal should NOT be rewritten
	sql := "SELECT 'staging.stg_orders' as table_name FROM staging.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Contains(t, rewritten, "'staging.stg_orders'") // string preserved
	assert.Contains(t, rewritten, "FROM dev_fritz.stg_orders") // real ref rewritten
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_CommentsNotRewritten(t *testing.T) {
	project := newTestProjectDev()

	sql := `-- FROM staging.stg_orders
SELECT * FROM staging.stg_customers`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Contains(t, rewritten, "-- FROM staging.stg_orders") // comment preserved
	assert.Contains(t, rewritten, "FROM dev_fritz.stg_customers")
	assert.Contains(t, deps, "stg_customers")
}

func TestRewriteTableReferences_CTENotRewritten(t *testing.T) {
	project := newTestProjectDev()

	// CTE name matching a model should not be rewritten when referenced
	sql := `WITH stg_orders AS (
    SELECT 1 as id
)
SELECT * FROM stg_orders`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	// The CTE reference should not be rewritten
	assert.Contains(t, rewritten, "FROM stg_orders")
	assert.NotContains(t, rewritten, "dev_fritz")
	assert.Empty(t, deps)
}

func TestRewriteTableReferences_UnqualifiedName(t *testing.T) {
	project := newTestProjectDev()

	// Bare table name without schema should match by model name
	sql := "SELECT * FROM stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM dev_fritz.stg_orders", rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_SelfNotRewritten(t *testing.T) {
	project := newTestProjectDev()

	// Self-references should not be rewritten
	sql := "SELECT * FROM staging.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "stg_orders")

	assert.Equal(t, "SELECT * FROM staging.stg_orders", rewritten)
	assert.Empty(t, deps)
}

func TestRewriteTableReferences_DoubleQuoted(t *testing.T) {
	project := newTestProjectDev()

	sql := `SELECT * FROM "staging"."stg_orders"`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	// Quotes should be preserved in the replacement
	assert.Equal(t, `SELECT * FROM "dev_fritz"."stg_orders"`, rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_Backticked(t *testing.T) {
	project := newTestProjectDev()

	sql := "SELECT * FROM `staging`.`stg_orders`"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	// Backticks should be preserved in the replacement
	assert.Equal(t, "SELECT * FROM `dev_fritz`.`stg_orders`", rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_UnquotedPreserved(t *testing.T) {
	project := newTestProjectDev()

	sql := "SELECT * FROM staging.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	// No quotes in original, no quotes in replacement
	assert.Equal(t, "SELECT * FROM dev_fritz.stg_orders", rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_MixedQuotesAndPlain(t *testing.T) {
	project := newTestProjectDev()

	sql := `SELECT * FROM "staging"."stg_orders" o JOIN staging.stg_customers c ON o.id = c.id`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Contains(t, rewritten, `"dev_fritz"."stg_orders"`)
	assert.Contains(t, rewritten, "dev_fritz.stg_customers")
	assert.Contains(t, deps, "stg_orders")
	assert.Contains(t, deps, "stg_customers")
}

func TestRewriteTableReferences_QuotedProdMode(t *testing.T) {
	project := newTestProject()

	// In prod mode, quoted identifiers should still be preserved (identity rewrite)
	sql := `SELECT * FROM "staging"."stg_orders"`
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, `SELECT * FROM "staging"."stg_orders"`, rewritten)
	assert.Contains(t, deps, "stg_orders")
}

func TestRewriteTableReferences_SeedRewritten(t *testing.T) {
	project := newTestProjectDev()

	sql := "SELECT * FROM staging.country_codes"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM dev_fritz.country_codes", rewritten)
	assert.Contains(t, deps, "country_codes")
}

func TestRewriteTableReferences_RefOutputNotDoubleRewritten(t *testing.T) {
	project := newTestProjectDev()

	// In dev mode, ref() already returns the dev name.
	// The rewriter should NOT match it again (dev name is not in the prod-name index).
	sql := "SELECT * FROM dev_fritz.stg_orders"
	rewritten, deps, _ := RewriteTableReferences(sql, project, "some_model")

	assert.Equal(t, "SELECT * FROM dev_fritz.stg_orders", rewritten)
	assert.Empty(t, deps) // no match since dev name != prod name
}

func TestRefStemAndSchemaTable(t *testing.T) {
	project := loadMartsEventsProject(t, BuildOptions{Prod: true})
	te := NewTemplateEngine(project, nil)
	model := &Model{Name: "downstream", RawSQL: `select * from {{ ref("events") }}`}
	sql, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "analytics.events")
	assert.Contains(t, model.DependsOn, "events")
	assert.Contains(t, model.Refs, "events")

	model = &Model{Name: "downstream2", RawSQL: `select * from {{ ref("analytics.events") }}`}
	sql, err = te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "analytics.events")
	assert.Contains(t, model.DependsOn, "events")
}

func TestRefOldPrefixedNameNotFound(t *testing.T) {
	project := loadMartsEventsProject(t, BuildOptions{Prod: true})
	te := NewTemplateEngine(project, nil)
	model := &Model{Name: "downstream", RawSQL: `select * from {{ ref("plausible_events") }}`}
	_, err := te.CompileModel(model, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestRewriteTableReferencesProdQualified(t *testing.T) {
	project := loadMartsEventsProject(t, BuildOptions{Prod: true})
	rewritten, deps, err := RewriteTableReferences("select * from analytics.events", project, "downstream")
	require.NoError(t, err)
	assert.Equal(t, "select * from analytics.events", rewritten)
	assert.Contains(t, deps, "events")
}

func TestRefThreePart(t *testing.T) {
	project := loadDatabaseProject(t, BuildOptions{Prod: true})
	te := NewTemplateEngine(project, nil)

	model := &Model{Name: "downstream", RawSQL: `select * from {{ ref("dim_customers") }}`}
	sql, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "ANALYTICS_DB.marts.dim_customers")

	// `this` renders the three-part name
	revenue := project.Models["revenue"]
	revenue.RawSQL = "select * from {{ this }}"
	sql, err = te.CompileModel(revenue, nil)
	require.NoError(t, err)
	assert.Contains(t, sql, "FIN_DB.marts.revenue")
}

func TestRewriteTableReferencesThreePart(t *testing.T) {
	project := loadDatabaseProject(t, BuildOptions{Prod: true})

	rewritten, deps, err := RewriteTableReferences("select * from marts.dim_customers", project, "downstream")
	require.NoError(t, err)
	assert.Equal(t, "select * from ANALYTICS_DB.marts.dim_customers", rewritten)
	assert.Contains(t, deps, "dim_customers")
}

// =============================================================================
// protectLiterals Tests
// =============================================================================

func TestProtectLiterals(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantSafe bool // whether protected string contains no raw literals
	}{
		{"single-quoted string", "SELECT 'hello' FROM t", true},
		{"line comment", "SELECT 1 -- comment\nFROM t", true},
		{"block comment", "SELECT /* block */ 1 FROM t", true},
		{"jinja expression", "SELECT {{ var }} FROM t", true},
		{"jinja block", "{% if true %}SELECT 1{% endif %}", true},
		{"escaped quote", "SELECT 'it''s' FROM t", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protected, placeholders := protectLiterals(tt.sql)
			assert.True(t, len(placeholders) > 0)
			restored := restoreLiterals(protected, placeholders)
			assert.Equal(t, tt.sql, restored) // round-trip must be lossless
		})
	}
}

// =============================================================================
// preprocessAtRefs Tests
// =============================================================================

func TestPreprocessAtRefs(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "staging",
		FullTableName: "staging.test_model",
	}

	result, err := te.preprocessAtRefs("SELECT * FROM @stg_orders o JOIN @stg_customers c ON o.id = c.id", model)
	require.NoError(t, err)

	assert.Contains(t, result, "staging.stg_orders")
	assert.Contains(t, result, "staging.stg_customers")
	assert.Contains(t, model.DependsOn, "stg_orders")
	assert.Contains(t, model.DependsOn, "stg_customers")
}

func TestPreprocessAtRefsDevMode(t *testing.T) {
	project := newTestProjectDev()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "test_model",
		Schema:        "dev_fritz",
		FullTableName: "dev_fritz.test_model",
	}

	result, err := te.preprocessAtRefs("SELECT * FROM @stg_orders", model)
	require.NoError(t, err)

	// Should resolve to dev-mode FullTableName
	assert.Contains(t, result, "dev_fritz.stg_orders")
	assert.Contains(t, model.DependsOn, "stg_orders")
}

// =============================================================================
// IncrementalContext placeholder tests (Phase 3.5)
// =============================================================================

func TestCompileModel_SlingStyle_Placeholder_Default(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RawSQL:        "SELECT * FROM raw WHERE {{ incremental_where_cond() }}",
	}
	project.Models["stg_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.Contains(t, result, "WHERE 1=1")
	assert.NotContains(t, result, "incremental_where_cond")
}

func TestCompileModel_SlingStyle_Placeholder_Custom(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RawSQL:        "SELECT * FROM raw WHERE {{ incremental_where_cond() }}",
	}
	project.Models["stg_orders"] = model

	ctx := &IncrementalContext{WhereCond: `"created_at" > '2024-01-01'`, IsIncremental: true}
	result, err := te.CompileModel(model, ctx)
	require.NoError(t, err)
	assert.Contains(t, result, `"created_at" > '2024-01-01'`)
	assert.NotContains(t, result, "incremental_where_cond")
}

func TestCompileModel_SlingStyle_IncrementalValue(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RawSQL:        "SELECT {{ incremental_value() }} AS watermark FROM raw",
	}
	project.Models["stg_orders"] = model

	ctx := &IncrementalContext{Value: "'2024-01-01'", IsIncremental: true}
	result, err := te.CompileModel(model, ctx)
	require.NoError(t, err)
	assert.Contains(t, result, "'2024-01-01' AS watermark")
	assert.NotContains(t, result, "incremental_value")
}

func TestCompileModel_DbtStyle_IsIncrementalTrue(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_orders",
		Schema:        "marts",
		FullTableName: "marts.fct_orders",
		RawSQL: `SELECT id FROM raw
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}`,
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, &IncrementalContext{IsIncremental: true})
	require.NoError(t, err)
	assert.Contains(t, result, "WHERE updated_at > (SELECT MAX(updated_at) FROM marts.fct_orders)")
}

func TestCompileModel_DbtStyle_IsIncrementalFalse(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "fct_orders",
		Schema:        "marts",
		FullTableName: "marts.fct_orders",
		RawSQL: `SELECT id FROM raw
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}`,
	}
	project.Models["fct_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.NotContains(t, result, "WHERE updated_at")
}

func TestCompileModel_NilContext_UsesDefault(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RawSQL:        "SELECT * FROM raw WHERE {{ incremental_where_cond() }}",
	}
	project.Models["stg_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	// nil context → DefaultIncrementalContext → WhereCond="1=1"
	assert.Contains(t, result, "WHERE 1=1")
}

func TestDetectModelStyle_LegacyPlaceholderErrors(t *testing.T) {
	_, err := detectModelStyle("SELECT * FROM raw WHERE {incremental_where_cond}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "{{ incremental_where_cond() }}")

	_, err = detectModelStyle("SELECT {incremental_value} AS watermark FROM raw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "{{ incremental_value() }}")
}

func TestDetectModelStyle_SlingJinja(t *testing.T) {
	style, err := detectModelStyle("SELECT * FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	assert.Equal(t, StyleSling, style)
}

func TestCompileModel_ViewModel_NoPlaceholders_NoOp(t *testing.T) {
	project := newTestProject()
	te := NewTemplateEngine(project, nil)

	model := &Model{
		Name:          "stg_orders",
		Schema:        "staging",
		FullTableName: "staging.stg_orders",
		RawSQL:        "SELECT * FROM raw_orders",
	}
	project.Models["stg_orders"] = model

	result, err := te.CompileModel(model, nil)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM raw_orders", result)
}
