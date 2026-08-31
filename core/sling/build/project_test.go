package build

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestFixturePath(name string) string {
	// Tests run from core/sling/build/, fixtures are at tests/build/
	// We need to find the project root first
	wd, _ := os.Getwd()
	// Walk up to find go.mod
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Fallback to relative path
			return filepath.Join("../../../tests/build", name)
		}
		dir = parent
	}
	return filepath.Join(dir, "tests/build", name)
}

func TestResolveTableName(t *testing.T) {
	tests := []struct {
		name           string
		relPath        string
		mode           string
		schemaOverride string
		defaultSchema  string
		wantSchema     string
		wantPrefix     string
		wantName       string
		wantFull       string
	}{
		{
			name:          "prod mode - first level folder is schema",
			relPath:       "staging/stg_orders.sql",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "staging",
			wantPrefix:    "",
			wantName:      "stg_orders",
			wantFull:      "staging.stg_orders",
		},
		{
			name:          "prod mode - nested folder becomes prefix",
			relPath:       "marts/core/dim_customers.sql",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "marts",
			wantPrefix:    "core",
			wantName:      "dim_customers",
			wantFull:      "marts.core_dim_customers",
		},
		{
			name:          "prod mode - deeply nested folders",
			relPath:       "marts/core/finance/revenue.sql",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "marts",
			wantPrefix:    "core_finance",
			wantName:      "revenue",
			wantFull:      "marts.core_finance_revenue",
		},
		{
			name:          "prod mode - root level file uses default schema",
			relPath:       "raw.sql",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "public",
			wantPrefix:    "",
			wantName:      "raw",
			wantFull:      "public.raw",
		},
		{
			name:           "dev mode - all folders become prefix",
			relPath:        "staging/stg_orders.sql",
			mode:           "dev",
			schemaOverride: "dev_fritz",
			defaultSchema:  "public",
			wantSchema:     "dev_fritz",
			wantPrefix:     "staging",
			wantName:       "stg_orders",
			wantFull:       "dev_fritz.staging_stg_orders",
		},
		{
			name:           "dev mode - nested folders all become prefix",
			relPath:        "marts/core/dim_customers.sql",
			mode:           "dev",
			schemaOverride: "dev_fritz",
			defaultSchema:  "public",
			wantSchema:     "dev_fritz",
			wantPrefix:     "marts_core",
			wantName:       "dim_customers",
			wantFull:       "dev_fritz.marts_core_dim_customers",
		},
		{
			name:           "dev mode - root level file",
			relPath:        "raw.sql",
			mode:           "dev",
			schemaOverride: "dev_fritz",
			defaultSchema:  "public",
			wantSchema:     "dev_fritz",
			wantPrefix:     "",
			wantName:       "raw",
			wantFull:       "dev_fritz.raw",
		},
		{
			name:          "csv seed file",
			relPath:       "staging/country_codes.csv",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "staging",
			wantPrefix:    "",
			wantName:      "country_codes",
			wantFull:      "staging.country_codes",
		},
		{
			name:          "json seed file in nested dir",
			relPath:       "seeds/status_map.json",
			mode:          "prod",
			defaultSchema: "public",
			wantSchema:    "seeds",
			wantPrefix:    "",
			wantName:      "status_map",
			wantFull:      "seeds.status_map",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, prefix, name, fullTableName := resolveTableName(tt.relPath, tt.mode, tt.schemaOverride, tt.defaultSchema)
			assert.Equal(t, tt.wantSchema, schema, "schema")
			assert.Equal(t, tt.wantPrefix, prefix, "prefix")
			assert.Equal(t, tt.wantName, name, "name")
			assert.Equal(t, tt.wantFull, fullTableName, "fullTableName")
		})
	}
}

func TestDiscoverFilesFlatMode(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	project := &BuildProject{
		Dir:           dir,
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Mode:          "prod",
		DefaultSchema: "public",
		ChildConfigs:  make(map[string]*BuildConfig),
	}
	project.Config, _ = loadConfig(filepath.Join(dir, ConfigFileName))

	err := discoverFiles(project)
	require.NoError(t, err)

	// Check models
	expectedModels := []string{"stg_orders", "stg_customers", "dim_customers", "fct_orders", "revenue", "raw"}
	assert.Len(t, project.Models, len(expectedModels))
	for _, name := range expectedModels {
		assert.Contains(t, project.Models, name, "expected model: %s", name)
	}

	// Check seeds
	expectedSeeds := []string{"country_codes", "status_map"}
	assert.Len(t, project.Seeds, len(expectedSeeds))
	for _, name := range expectedSeeds {
		assert.Contains(t, project.Seeds, name, "expected seed: %s", name)
	}

	// Check specific model properties
	stgOrders := project.Models["stg_orders"]
	assert.Equal(t, "staging", stgOrders.Schema)
	assert.Equal(t, "", stgOrders.Prefix)
	assert.Equal(t, "staging.stg_orders", stgOrders.FullTableName)
	assert.NotEmpty(t, stgOrders.RawSQL)

	dimCustomers := project.Models["dim_customers"]
	assert.Equal(t, "marts", dimCustomers.Schema)
	assert.Equal(t, "core", dimCustomers.Prefix)
	assert.Equal(t, "marts.core_dim_customers", dimCustomers.FullTableName)

	revenue := project.Models["revenue"]
	assert.Equal(t, "marts", revenue.Schema)
	assert.Equal(t, "finance", revenue.Prefix)
	assert.Equal(t, "marts.finance_revenue", revenue.FullTableName)

	raw := project.Models["raw"]
	assert.Equal(t, "public", raw.Schema)
	assert.Equal(t, "", raw.Prefix)
	assert.Equal(t, "public.raw", raw.FullTableName)

	// Check seed properties
	countryCodes := project.Seeds["country_codes"]
	assert.Equal(t, "staging", countryCodes.Schema)
	assert.Equal(t, "csv", countryCodes.Format)
	assert.Equal(t, "staging.country_codes", countryCodes.FullTableName)

	statusMap := project.Seeds["status_map"]
	assert.Equal(t, "seeds", statusMap.Schema)
	assert.Equal(t, "json", statusMap.Format)

	// Check macros are collected (not counted as models)
	assert.Len(t, project.Macros, 2) // utils.macros.sql + staging_helpers.macros.sql
}

func TestDiscoverFilesDbtProjectMode(t *testing.T) {
	dir := getTestFixturePath("dbt_compat_project")

	project := &BuildProject{
		Dir:           dir,
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		Mode:          "prod",
		DefaultSchema: "public",
		ChildConfigs:  make(map[string]*BuildConfig),
	}
	project.Config, _ = loadConfig(filepath.Join(dir, ConfigFileName))

	err := discoverFiles(project)
	require.NoError(t, err)

	// Check models - should only find SQL in models/ dir
	assert.Len(t, project.Models, 1)
	assert.Contains(t, project.Models, "stg_orders")

	// Check model naming: staging/ is first folder under models/, so schema=staging
	stgOrders := project.Models["stg_orders"]
	assert.Equal(t, "staging", stgOrders.Schema)
	assert.Equal(t, "staging.stg_orders", stgOrders.FullTableName)

	// Check seeds - should only find seeds in seeds/ dir
	assert.Len(t, project.Seeds, 1)
	assert.Contains(t, project.Seeds, "country_codes")

	countryCodes := project.Seeds["country_codes"]
	assert.Equal(t, "staging", countryCodes.Schema)
	assert.Equal(t, "csv", countryCodes.Format)
}

func TestLoadProjectSampleProject(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	project, err := LoadProject(dir)
	require.NoError(t, err)

	assert.Equal(t, "prod", project.Mode)
	assert.NotNil(t, project.Config)
	assert.Equal(t, "POSTGRES", project.Config.Target)
	assert.Len(t, project.Models, 6)
	assert.Len(t, project.Seeds, 2)
	assert.NotEmpty(t, project.Macros)
}

func TestLoadProjectDbtCompat(t *testing.T) {
	dir := getTestFixturePath("dbt_compat_project")

	project, err := LoadProject(dir)
	require.NoError(t, err)

	assert.NotNil(t, project.Config)
	assert.Equal(t, true, project.Config.DbtProject)
	assert.Len(t, project.Models, 1)
	assert.Len(t, project.Seeds, 1)
}

func TestLoadProjectWithCliOverrides(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	// Test dev mode override via --schema
	project, err := LoadProject(dir, BuildOptions{Schema: "dev_test"})
	require.NoError(t, err)

	assert.Equal(t, "dev", project.Mode)
	assert.Equal(t, "dev_test", project.SchemaOverride)

	// In dev mode, all models should use the override schema
	for _, model := range project.Models {
		assert.Equal(t, "dev_test", model.Schema, "model %s should use dev schema", model.Name)
	}
	for _, seed := range project.Seeds {
		assert.Equal(t, "dev_test", seed.Schema, "seed %s should use dev schema", seed.Name)
	}
}

func TestLoadProjectWithProdOverride(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	// Test --prod flag
	project, err := LoadProject(dir, BuildOptions{Prod: true})
	require.NoError(t, err)

	assert.Equal(t, "prod", project.Mode)
}

func TestFindConfigFile(t *testing.T) {
	dir := t.TempDir()
	_, ok := FindConfigFile(dir)
	assert.False(t, ok)

	ymlPath := filepath.Join(dir, "sling_build.yml")
	require.NoError(t, os.WriteFile(ymlPath, []byte("target: POSTGRES\n"), 0644))
	found, ok := FindConfigFile(dir)
	require.True(t, ok)
	assert.Equal(t, ymlPath, found)

	yamlPath := filepath.Join(dir, "sling_build.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte("target: DUCKDB\n"), 0644))
	found, ok = FindConfigFile(dir)
	require.True(t, ok)
	assert.Equal(t, ymlPath, found, "prefer .yml when both exist")
}

func TestLoadProjectYAMLConfig(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sling_build.yaml"), []byte("target: DUCKDB\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "stg_ok.sql"), []byte("select 1 as id\n"), 0644))

	project, err := LoadProject(dir)
	require.NoError(t, err)
	require.NotNil(t, project.Config)
	assert.Equal(t, "DUCKDB", project.Config.Target)
	require.NotNil(t, project.Models["stg_ok"])
}

func TestNestedProjectIsolation(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "sling_build.yml"), []byte("target: POSTGRES\n"), 0644))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "staging"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "staging", "stg_ok.sql"), []byte("select 1 as id\n"), 0644))

	probe := filepath.Join(dir, "probe")
	require.NoError(t, os.MkdirAll(filepath.Join(probe, "staging"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(probe, "sling_build.yml"), []byte("target: DUCKDB\n"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(probe, "staging", "stg_broken.sql"), []byte("select not_a_column from nowhere\n"), 0644))

	project, err := LoadProject(dir)
	require.NoError(t, err)
	require.NotNil(t, project.Models["stg_ok"])
	require.Nil(t, project.Models["stg_broken"], "nested probe project must not join parent discovery")

	rec, err := LoadProject(dir, BuildOptions{Recursive: true})
	require.NoError(t, err)
	require.NotNil(t, rec.Models["stg_ok"])
	require.NotNil(t, rec.Models["stg_broken"], "-R keeps immediate child models")
}

func TestNestedConfigInheritance(t *testing.T) {
	dir := getTestFixturePath("nested_yml_project")

	// Nested sling_build.yml overrides require --recursive
	project, err := LoadProject(dir, BuildOptions{Recursive: true})
	require.NoError(t, err)

	assert.NotNil(t, project.Config)
	assert.Equal(t, "POSTGRES", project.Config.Target)
	assert.Equal(t, "full-refresh", project.Config.Defaults.Mode)

	// The staging child config should override defaults.mode to "truncate"
	assert.Contains(t, project.ChildConfigs, "staging")
	assert.Equal(t, "truncate", project.ChildConfigs["staging"].Defaults.Mode)

	// Models in staging should get the overridden mode
	stgOrders := project.Models["stg_orders"]
	require.NotNil(t, stgOrders)
	assert.Equal(t, "truncate", stgOrders.Config.Mode)

	// Models in marts should get the root default mode
	dimCustomers := project.Models["dim_customers"]
	require.NotNil(t, dimCustomers)
	assert.Equal(t, "full-refresh", dimCustomers.Config.Mode)
}

func TestMultiTargetIndependentBuilds(t *testing.T) {
	dir := getTestFixturePath("multi_target_project")

	// Independent sub-projects require --recursive to discover child ymls
	project, err := LoadProject(dir, BuildOptions{Recursive: true})
	require.NoError(t, err)

	// Should have no root config
	assert.Nil(t, project.Config)

	// Should have sub-projects
	assert.Len(t, project.SubProjects, 2)

	// Verify each sub-project
	targets := make(map[string]bool)
	for _, sub := range project.SubProjects {
		require.NotNil(t, sub.Config)
		targets[sub.Config.Target] = true
		assert.Len(t, sub.Models, 1) // each has one model
	}

	assert.True(t, targets["POSTGRES"])
	assert.True(t, targets["CLICKHOUSE"])
}

func TestMergeConfigs(t *testing.T) {
	parent := &BuildConfig{
		Target: "POSTGRES",
		Dev:    &DevConfig{Schema: "dev_schema"},
		Vars: map[string]any{
			"start_date":  "2024-01-01",
			"environment": "prod",
		},
		Defaults: BuildDefaults{
			Mode: "full-refresh",
		},
	}

	child := &BuildConfig{
		Vars: map[string]any{
			"environment": "staging", // override
			"new_var":     "value",   // new
		},
		Defaults: BuildDefaults{
			Mode: "truncate", // override
		},
	}

	merged := mergeConfigs(parent, child)

	assert.Equal(t, "POSTGRES", merged.Target)       // inherited
	assert.Equal(t, "dev_schema", merged.Dev.Schema) // inherited
	assert.Equal(t, "truncate", merged.Defaults.Mode)
	assert.Equal(t, "2024-01-01", merged.Vars["start_date"]) // inherited
	assert.Equal(t, "staging", merged.Vars["environment"])   // overridden
	assert.Equal(t, "value", merged.Vars["new_var"])         // new
}

func TestMergeConfigsNil(t *testing.T) {
	cfg := &BuildConfig{Target: "PG"}
	assert.Equal(t, cfg, mergeConfigs(nil, cfg))
	assert.Equal(t, cfg, mergeConfigs(cfg, nil))
}

func TestValidateUniqueNames(t *testing.T) {
	// No duplicates - should pass
	project := &BuildProject{
		Models: map[string]*Model{"model_a": {Name: "model_a"}},
		Seeds:  map[string]*Seed{"seed_a": {Name: "seed_a"}},
	}
	assert.NoError(t, validateUniqueNames(project))

	// Model-seed collision - should fail
	project = &BuildProject{
		Models: map[string]*Model{"same_name": {Name: "same_name"}},
		Seeds:  map[string]*Seed{"same_name": {Name: "same_name"}},
	}
	err := validateUniqueNames(project)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate name")
}

func TestLoadProjectEmptyDir(t *testing.T) {
	dir := t.TempDir()

	project, err := LoadProject(dir)
	require.NoError(t, err)

	assert.Nil(t, project.Config)
	assert.Len(t, project.Models, 0)
	assert.Len(t, project.Seeds, 0)
}

func TestLoadProjectNonExistentDir(t *testing.T) {
	_, err := LoadProject("/nonexistent/path")
	assert.Error(t, err)
}

func TestGetDbtProjectConfig(t *testing.T) {
	// false
	cfg := &BuildConfig{DbtProject: false}
	assert.Nil(t, getDbtProjectConfig(cfg))

	// true - uses defaults
	cfg = &BuildConfig{DbtProject: true}
	dbtCfg := getDbtProjectConfig(cfg)
	require.NotNil(t, dbtCfg)
	assert.Equal(t, "models", dbtCfg.ModelsPath)
	assert.Equal(t, "seeds", dbtCfg.SeedsPath)

	// Custom paths
	cfg = &BuildConfig{DbtProject: map[string]any{
		"models_path": "custom_models",
		"seeds_path":  "custom_seeds",
	}}
	dbtCfg = getDbtProjectConfig(cfg)
	require.NotNil(t, dbtCfg)
	assert.Equal(t, "custom_models", dbtCfg.ModelsPath)
	assert.Equal(t, "custom_seeds", dbtCfg.SeedsPath)

	// nil
	assert.Nil(t, getDbtProjectConfig(nil))
	assert.Nil(t, getDbtProjectConfig(&BuildConfig{}))
}

func TestLookupFullTableName(t *testing.T) {
	project := &BuildProject{
		Models: map[string]*Model{
			"stg_orders": {Name: "stg_orders", FullTableName: "staging.stg_orders"},
		},
		Seeds: map[string]*Seed{
			"country_codes": {Name: "country_codes", FullTableName: "staging.country_codes"},
		},
	}

	name, ok := project.LookupFullTableName("stg_orders")
	assert.True(t, ok)
	assert.Equal(t, "staging.stg_orders", name)

	name, ok = project.LookupFullTableName("country_codes")
	assert.True(t, ok)
	assert.Equal(t, "staging.country_codes", name)

	_, ok = project.LookupFullTableName("nonexistent")
	assert.False(t, ok)
}

// =============================================================================
// Seed Tests
// =============================================================================

func TestMakeSeedConfigCSV(t *testing.T) {
	seed := &Seed{
		Name:          "country_codes",
		FilePath:      "/tmp/project/staging/country_codes.csv",
		RelPath:       "staging/country_codes.csv",
		Schema:        "staging",
		FullTableName: "staging.country_codes",
		Format:        "csv",
	}

	cfg := MakeSeedConfig(seed, "POSTGRES")

	assert.Equal(t, "file:///tmp/project/staging", cfg.Source.Conn)
	assert.Equal(t, "country_codes.csv", cfg.Source.Stream)
	assert.Equal(t, "POSTGRES", cfg.Target.Conn)
	assert.Equal(t, "staging.country_codes", cfg.Target.Object)
	assert.Equal(t, sling.FullRefreshMode, cfg.Mode)
}

func TestMakeSeedConfigJSON(t *testing.T) {
	seed := &Seed{
		Name:          "status_map",
		FilePath:      "/tmp/project/seeds/status_map.json",
		RelPath:       "seeds/status_map.json",
		Schema:        "seeds",
		FullTableName: "seeds.status_map",
		Format:        "json",
	}

	cfg := MakeSeedConfig(seed, "CLICKHOUSE")

	assert.Equal(t, "file:///tmp/project/seeds", cfg.Source.Conn)
	assert.Equal(t, "status_map.json", cfg.Source.Stream)
	assert.Equal(t, "CLICKHOUSE", cfg.Target.Conn)
	assert.Equal(t, "seeds.status_map", cfg.Target.Object)
	assert.Equal(t, sling.FullRefreshMode, cfg.Mode)
}

func TestMakeSeedConfigParquet(t *testing.T) {
	seed := &Seed{
		Name:          "large_data",
		FilePath:      "/data/warehouse/large_data.parquet",
		RelPath:       "warehouse/large_data.parquet",
		Schema:        "warehouse",
		FullTableName: "warehouse.large_data",
		Format:        "parquet",
	}

	cfg := MakeSeedConfig(seed, "SNOWFLAKE")

	assert.Equal(t, "file:///data/warehouse", cfg.Source.Conn)
	assert.Equal(t, "large_data.parquet", cfg.Source.Stream)
	assert.Equal(t, "SNOWFLAKE", cfg.Target.Conn)
	assert.Equal(t, "warehouse.large_data", cfg.Target.Object)
	assert.Equal(t, sling.FullRefreshMode, cfg.Mode)
}

func TestMakeSeedConfigSourceOptions(t *testing.T) {
	seed := &Seed{
		Name:          "test_seed",
		FilePath:      "/tmp/test_seed.csv",
		RelPath:       "test_seed.csv",
		Schema:        "public",
		FullTableName: "public.test_seed",
		Format:        "csv",
	}

	cfg := MakeSeedConfig(seed, "MY_DB")

	// Source options should be initialized (non-nil)
	assert.NotNil(t, cfg.Source.Options)
}

// =============================================================================
// YAML Frontmatter Tests
// =============================================================================

func TestParseYAMLFrontmatter_Basic(t *testing.T) {
	sql := `/**
mode: incremental
unique_key: id
update_key: updated_at
**/
SELECT * FROM orders`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, "id", cfg.UniqueKey)
	assert.Equal(t, "updated_at", cfg.UpdateKey)
	assert.Equal(t, "SELECT * FROM orders", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_AllFields(t *testing.T) {
	sql := `/**
mode: incremental
unique_key:
  - id
  - tenant_id
merge_strategy: delete+insert
update_key: updated_at
tags:
  - daily
  - finance
hooks:
  start:
    - type: log
      message: "Starting model build"
  end:
    - type: log
      message: "Model build complete"
schema: analytics
enabled: false
engine: MergeTree()
**/
SELECT 1`

	cfg, _, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, []interface{}{"id", "tenant_id"}, cfg.UniqueKey)
	assert.Equal(t, "delete+insert", cfg.MergeStrategy)
	assert.Equal(t, "updated_at", cfg.UpdateKey)
	assert.Equal(t, []string{"daily", "finance"}, cfg.Tags)
	assert.Len(t, cfg.Hooks.Start, 1)
	assert.Len(t, cfg.Hooks.End, 1)
	assert.Equal(t, "analytics", cfg.Schema)
	assert.NotNil(t, cfg.Enabled)
	assert.False(t, *cfg.Enabled)
	assert.Equal(t, "MergeTree()", cfg.Engine)
}

func TestParseYAMLFrontmatter_PreHookError(t *testing.T) {
	sql := `/**
mode: full-refresh
pre_hook: "CREATE TEMP TABLE tmp AS SELECT 1"
**/
SELECT 1`

	_, _, _, err := parseYAMLFrontmatter(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre_hook/post_hook are not supported")
	assert.Contains(t, err.Error(), "hooks.start/hooks.end")
}

func TestParseYAMLFrontmatter_PostHookError(t *testing.T) {
	sql := `/**
mode: full-refresh
post_hook: "DROP TABLE IF EXISTS tmp"
**/
SELECT 1`

	_, _, _, err := parseYAMLFrontmatter(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre_hook/post_hook are not supported")
}

func TestParseYAMLFrontmatter_WithHooks(t *testing.T) {
	sql := `/**
mode: incremental
unique_key: id
hooks:
  start:
    - type: query
      connection: postgres
      query: "REFRESH MATERIALIZED VIEW upstream_mv"
    - type: log
      message: "Starting model build"
  end:
    - type: check
      check: execution.status.error == 0
    - type: log
      message: "Model build complete"
**/
SELECT * FROM {{ ref('stg_orders') }}`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, "id", cfg.UniqueKey)
	assert.False(t, cfg.Hooks.IsEmpty())
	assert.Len(t, cfg.Hooks.Start, 2)
	assert.Len(t, cfg.Hooks.End, 2)
	assert.Contains(t, remaining, "SELECT * FROM")
}

func TestParseYAMLFrontmatter_NoFrontmatter(t *testing.T) {
	sql := `SELECT * FROM orders`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, ModelConfig{}, cfg)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_JinjaConfigNotFrontmatter(t *testing.T) {
	sql := `{%- config(mode='incremental') -%}
SELECT * FROM orders`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_UnclosedDelimiter(t *testing.T) {
	sql := `/**
mode: incremental
SELECT * FROM orders`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_RegularComment(t *testing.T) {
	// A regular /* ... */ comment should NOT be treated as frontmatter
	sql := `/* this is a regular comment */
SELECT * FROM orders`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_LeadingWhitespace(t *testing.T) {
	sql := `
/**
mode: view
**/
SELECT 1`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "view", cfg.Mode)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_InvalidYAML(t *testing.T) {
	sql := `/**
mode: [invalid yaml
**/
SELECT 1`

	_, _, _, err := parseYAMLFrontmatter(sql)
	assert.Error(t, err)
}

func TestParseYAMLFrontmatter_EmptyFrontmatter(t *testing.T) {
	sql := `/**
**/
SELECT 1`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, ModelConfig{}, cfg)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_InlineOpening(t *testing.T) {
	// /** on same line as first YAML key
	sql := `/** mode: view
**/
SELECT 1`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "view", cfg.Mode)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(remaining))
}

// =============================================================================
// Flexible Frontmatter Format Tests
// =============================================================================

func TestParseYAMLFrontmatter_LineCommentInlineFlow(t *testing.T) {
	sql := `-- {mode: view, schema: analytics}
SELECT 1`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "view", cfg.Mode)
	assert.Equal(t, "analytics", cfg.Schema)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_LineCommentMultilineFlow(t *testing.T) {
	sql := `-- {
--   mode: incremental,
--   unique_key: id,
--   update_key: updated_at
-- }
SELECT * FROM raw_orders`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, "id", cfg.UniqueKey)
	assert.Equal(t, "updated_at", cfg.UpdateKey)
	assert.Equal(t, "SELECT * FROM raw_orders", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_BlockCommentJSON(t *testing.T) {
	sql := `/* {"mode": "view", "schema": "analytics"} */
SELECT 1`

	cfg, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "view", cfg.Mode)
	assert.Equal(t, "analytics", cfg.Schema)
	assert.Equal(t, "SELECT 1", strings.TrimSpace(remaining))
}

func TestParseYAMLFrontmatter_BlockCommentMultilineFlow(t *testing.T) {
	sql := `/* {
  "mode": "incremental",
  "unique_key": "id"
} */
SELECT 1`

	cfg, _, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, "id", cfg.UniqueKey)
}

func TestParseYAMLFrontmatter_LineCommentRegularComment(t *testing.T) {
	// A `--` line comment without a {} object should NOT be detected as frontmatter.
	sql := `-- Pre-statement: create a temp table for staging
CREATE TEMP TABLE tmp AS SELECT 1 AS id;`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_LineCommentMultipleRegular(t *testing.T) {
	sql := `-- This is a comment
-- explaining the next query
SELECT 1`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_BlockCommentNoBraces(t *testing.T) {
	// Plain prose in /* */ should remain a regular comment.
	sql := `/* this is a regular comment about the query */
SELECT 1`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_LineCommentSimpleName(t *testing.T) {
	// Single-key flow object on one line — common minimal frontmatter.
	sql := `-- {schema: marts}
SELECT 1`

	cfg, _, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "marts", cfg.Schema)
}

func TestParseYAMLFrontmatter_LineCommentWithHooks(t *testing.T) {
	sql := `-- {
--   mode: incremental,
--   unique_key: id,
--   tags: [daily, finance]
-- }
SELECT 1`

	cfg, _, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.True(t, hasFM)
	assert.Equal(t, "incremental", cfg.Mode)
	assert.Equal(t, []string{"daily", "finance"}, cfg.Tags)
}

func TestParseYAMLFrontmatter_LineCommentInvalidFlowFallsBack(t *testing.T) {
	// `-- { not valid yaml` shouldn't crash; should be treated as regular comment.
	sql := `-- { not valid yaml here
SELECT 1`

	_, remaining, hasFM, err := parseYAMLFrontmatter(sql)
	require.NoError(t, err)
	assert.False(t, hasFM)
	assert.Equal(t, sql, remaining)
}

func TestParseYAMLFrontmatter_BlockCommentPreHookErrors(t *testing.T) {
	sql := `/* {"mode": "full-refresh", "pre_hook": "x"} */
SELECT 1`

	_, _, _, err := parseYAMLFrontmatter(sql)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pre_hook/post_hook are not supported")
}

func TestAddModelWithFrontmatter(t *testing.T) {
	dir := t.TempDir()
	sqlContent := `/**
mode: incremental
unique_key: id
update_key: updated_at
**/
SELECT * FROM raw_orders`

	modelPath := filepath.Join(dir, "orders.sql")
	require.NoError(t, os.WriteFile(modelPath, []byte(sqlContent), 0644))

	project := &BuildProject{
		Dir:           dir,
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		ChildConfigs:  make(map[string]*BuildConfig),
	}

	err := addModel(project, modelPath, "orders.sql")
	require.NoError(t, err)

	model := project.Models["orders"]
	require.NotNil(t, model)
	assert.True(t, model.HasFrontmatter)
	assert.Equal(t, "incremental", model.Config.Mode)
	assert.Equal(t, "id", model.Config.UniqueKey)
	assert.Equal(t, "updated_at", model.Config.UpdateKey)
	assert.Equal(t, "SELECT * FROM raw_orders", strings.TrimSpace(model.RawSQL))
}

func TestSeedInBuildProject(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	// Seeds should be at depth 0 in DAG
	countryCodes := b.DAG.Nodes["country_codes"]
	require.NotNil(t, countryCodes)
	assert.NotNil(t, countryCodes.Seed)
	assert.Nil(t, countryCodes.Model)
	assert.Equal(t, 0, countryCodes.Depth)
	assert.Empty(t, countryCodes.Dependencies)

	statusMap := b.DAG.Nodes["status_map"]
	require.NotNil(t, statusMap)
	assert.NotNil(t, statusMap.Seed)
	assert.Equal(t, 0, statusMap.Depth)

	// Seeds should appear in execution order
	levels := b.DAG.GetExecutionLevels()
	require.NotEmpty(t, levels)

	// Level 0 should contain seeds and models with no deps
	level0 := levels[0]
	assert.Contains(t, level0, "country_codes")
	assert.Contains(t, level0, "status_map")
}

// =============================================================================
// Phase 2: RangeConfig + validateModel tests
// =============================================================================

// addModelFromFrontmatter is a test helper that writes a model SQL file containing
// the given YAML frontmatter and body, then calls addModel() on a fresh project.
// Returns the resulting error (nil on success) and the Model if it was added.
func addModelFromFrontmatter(t *testing.T, frontmatter, body string) (*Model, error) {
	t.Helper()
	dir := t.TempDir()
	content := "/**\n" + frontmatter + "\n**/\n" + body
	modelPath := filepath.Join(dir, "m.sql")
	require.NoError(t, os.WriteFile(modelPath, []byte(content), 0644))

	project := &BuildProject{
		Dir:           dir,
		Mode:          "prod",
		DefaultSchema: "public",
		Models:        make(map[string]*Model),
		Seeds:         make(map[string]*Seed),
		ChildConfigs:  make(map[string]*BuildConfig),
	}

	if err := addModel(project, modelPath, "m.sql"); err != nil {
		return nil, err
	}
	return project.Models["m"], nil
}

func TestFrontmatterMaterializedAlias(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"materialized: table\nunique_key: id",
		"SELECT 1 as id")
	require.NoError(t, err)
	assert.Equal(t, "full-refresh", m.Config.Mode)
}

func TestFrontmatterSnapshotRenamedToAppend(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: snapshot",
		"SELECT 1 as id")
	require.NoError(t, err)
	assert.Equal(t, "append", m.Config.Mode)
}

func TestFrontmatterUnknownModeRejected(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremenal",
		"SELECT 1 as id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown mode 'incremenal'")
	assert.Contains(t, err.Error(), "full-refresh")
}

func TestFrontmatterValidModesAccepted(t *testing.T) {
	for mode := range ValidModes {
		frontmatter := "mode: " + mode
		if mode == "incremental" {
			frontmatter += "\nunique_key: id\nupdate_key: updated_at"
		}
		m, err := addModelFromFrontmatter(t, frontmatter, "SELECT 1 as id, 2 as updated_at")
		require.NoError(t, err, "mode %s should be accepted", mode)
		assert.Equal(t, mode, m.Config.Mode)
	}
}

func TestLoadConfigUnknownDefaultsModeRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ConfigFileName)
	require.NoError(t, os.WriteFile(path,
		[]byte("target: MY_PG\ndefaults:\n  mode: bogus\n"), 0644))

	_, err := loadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown defaults.mode 'bogus'")
}

func TestLoadConfigDefaultsModeAliasNormalized(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ConfigFileName)
	require.NoError(t, os.WriteFile(path,
		[]byte("target: MY_PG\ndefaults:\n  mode: table\n"), 0644))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "full-refresh", cfg.Defaults.Mode)
}

func TestFrontmatterDataTests(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: full-refresh\ntests:\n  - not_null: [id]\n  - unique: id\n  - expr: sum(id) > 0",
		"SELECT 1 as id")
	require.NoError(t, err)
	require.Len(t, m.Config.Tests, 3)
}

func TestFrontmatterRewriteFalse(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: full-refresh\nrewrite: false",
		"SELECT * FROM external_orders")
	require.NoError(t, err)
	require.NotNil(t, m.Config.Rewrite)
	assert.False(t, *m.Config.Rewrite)
}

func TestFrontmatterDropCascade(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: full-refresh\ndrop_cascade: true",
		"SELECT 1 as id")
	require.NoError(t, err)
	require.NotNil(t, m.Config.DropCascade)
	assert.True(t, *m.Config.DropCascade)
}

func TestRangeConfig_Valid_AbsentMeansPlainIncremental(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Nil(t, m.Config.Range)
	assert.Equal(t, StyleSling, m.Style)
}

func TestRangeConfig_Valid_AdvanceOnly(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  advance: 7d",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	require.NotNil(t, m.Config.Range)
	assert.Equal(t, "7d", m.Config.Range.Advance)
	assert.True(t, m.Config.Range.HasAdvance())
	assert.False(t, m.Config.Range.HasLookback())
}

func TestRangeConfig_Valid_StartAndAdvance(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  start: '2020-01-01'\n  advance: 1mo",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	require.NotNil(t, m.Config.Range)
	assert.Equal(t, "2020-01-01", m.Config.Range.Start)
	assert.Equal(t, "1mo", m.Config.Range.Advance)
}

func TestRangeConfig_Valid_AdvanceAndLookback(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  advance: 7d\n  lookback: 2d",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	require.NotNil(t, m.Config.Range)
	assert.Equal(t, "7d", m.Config.Range.Advance)
	assert.Equal(t, "2d", m.Config.Range.Lookback)
	assert.True(t, m.Config.Range.HasLookback())
}

func TestRangeConfig_Valid_LookbackOnly(t *testing.T) {
	m, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  lookback: 3h",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.NoError(t, err)
	require.NotNil(t, m.Config.Range)
	assert.Equal(t, "", m.Config.Range.Advance)
	assert.Equal(t, "3h", m.Config.Range.Lookback)
}

func TestRangeConfig_Invalid_StartWithoutAdvance(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  start: '2020-01-01'",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "range.start requires range.advance")
}

func TestRangeConfig_Invalid_AdvanceWithoutUpdateKey(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nrange:\n  advance: 7d",
		"SELECT id FROM raw WHERE {{ incremental_where_cond() }}")
	require.Error(t, err)
	// incremental mode without update_key fires first
	assert.Contains(t, err.Error(), "update_key")
}

func TestRangeConfig_Invalid_RangeWithoutIncrementalMode(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: view\nrange:\n  advance: 7d",
		"SELECT id FROM raw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "range.* requires mode: incremental")
}

func TestRangeConfig_Invalid_BadAdvanceDuration(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  advance: seven_days",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid range.advance")
}

func TestRangeConfig_Invalid_BadLookbackDuration(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  lookback: later",
		"SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid range.lookback")
}

func TestIncrementalWithoutUpdateKey(t *testing.T) {
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id",
		"SELECT id FROM raw")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mode 'incremental' requires update_key")
}

func TestParseBuildDuration(t *testing.T) {
	// All valid units should parse without error.
	cases := map[string]time.Duration{
		"500ms": 500 * time.Millisecond,
		"30s":   30 * time.Second,
		"15m":   15 * time.Minute,
		"3h":    3 * time.Hour,
		"2d":    2 * 24 * time.Hour,
		"1w":    7 * 24 * time.Hour,
		"1mo":   30 * 24 * time.Hour,
		"1y":    365 * 24 * time.Hour,
	}
	for input, expected := range cases {
		got, err := parseBuildDuration(input)
		require.NoError(t, err, "input=%s", input)
		assert.Equal(t, expected, got, "input=%s", input)
	}

	// Invalid inputs
	for _, bad := range []string{"", "foo", "5", "d5", "5x", "-1d"} {
		_, err := parseBuildDuration(bad)
		assert.Error(t, err, "expected error for input=%q", bad)
	}
}

func TestValidateModel_RangeWithDbtStyleErrors(t *testing.T) {
	// A model using is_incremental() + range.advance errors at load.
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at\nrange:\n  advance: 7d",
		`SELECT id, updated_at FROM raw
{% if is_incremental() %}WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }}){% endif %}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "range.* requires incremental_where_cond()")
}

func TestValidateModel_MixedStyleErrors(t *testing.T) {
	// A model containing both is_incremental() AND {{ incremental_where_cond() }} errors.
	_, err := addModelFromFrontmatter(t,
		"mode: incremental\nunique_key: id\nupdate_key: updated_at",
		`SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}
{% if is_incremental() %}AND updated_at > (SELECT MAX(updated_at) FROM {{ this }}){% endif %}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot mix is_incremental() and incremental_where_cond()")
}

// =============================================================================
// Expanded BuildDefaults — tests for layered defaults, additive merging,
// schema overrides, and enabled:false DAG skip.
// =============================================================================

func TestMergeConfigsAdditiveTagsAndHooks(t *testing.T) {
	parent := &BuildConfig{
		Defaults: BuildDefaults{
			Tags: []string{"a", "b"},
			Hooks: sling.HookMap{
				Start: []any{map[string]any{"type": "log", "message": "parent_start"}},
				End:   []any{map[string]any{"type": "log", "message": "parent_end"}},
			},
		},
	}
	child := &BuildConfig{
		Defaults: BuildDefaults{
			Tags: []string{"b", "c"}, // "b" duplicates parent — should be deduped
			Hooks: sling.HookMap{
				Start: []any{map[string]any{"type": "log", "message": "child_start"}},
			},
		},
	}

	merged := mergeConfigs(parent, child)

	// Tags: union + dedupe, parent order preserved first
	assert.Equal(t, []string{"a", "b", "c"}, merged.Defaults.Tags)

	// Hooks: parent first, child appended
	require.Len(t, merged.Defaults.Hooks.Start, 2)
	assert.Equal(t, "parent_start", merged.Defaults.Hooks.Start[0].(map[string]any)["message"])
	assert.Equal(t, "child_start", merged.Defaults.Hooks.Start[1].(map[string]any)["message"])

	// End only had parent entry — should survive
	require.Len(t, merged.Defaults.Hooks.End, 1)
	assert.Equal(t, "parent_end", merged.Defaults.Hooks.End[0].(map[string]any)["message"])
}

func TestMergeHookMapsHandlesEmpty(t *testing.T) {
	// Both empty
	result := mergeHookMaps(sling.HookMap{}, sling.HookMap{})
	assert.True(t, result.IsEmpty())

	// Parent empty, child has entries
	child := sling.HookMap{Start: []any{"a"}}
	result = mergeHookMaps(sling.HookMap{}, child)
	assert.Equal(t, []any{"a"}, result.Start)

	// Parent has entries, child empty
	parent := sling.HookMap{End: []any{"b"}}
	result = mergeHookMaps(parent, sling.HookMap{})
	assert.Equal(t, []any{"b"}, result.End)
}

// buildDefaultsProject returns a fresh project rooted at a tempdir with the given
// root + child configs and models. Each model value is raw SQL content (with or
// without frontmatter). Returns a loaded project in prod mode.
func buildDefaultsProject(t *testing.T, rootCfg string, childCfgs map[string]string, models map[string]string, seeds map[string]string) *BuildProject {
	t.Helper()
	dir := t.TempDir()

	if rootCfg != "" {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "sling_build.yml"), []byte(rootCfg), 0644))
	}
	for subDir, cfg := range childCfgs {
		subPath := filepath.Join(dir, subDir)
		require.NoError(t, os.MkdirAll(subPath, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(subPath, "sling_build.yml"), []byte(cfg), 0644))
	}
	for relPath, content := range models {
		fullPath := filepath.Join(dir, relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
	}
	for relPath, content := range seeds {
		fullPath := filepath.Join(dir, relPath)
		require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0755))
		require.NoError(t, os.WriteFile(fullPath, []byte(content), 0644))
	}

	// Recursive so child sling_build.yml defaults are discovered
	project, err := LoadProject(dir, BuildOptions{Recursive: true})
	require.NoError(t, err)
	return project
}

func TestEffectiveDefaultsNested(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  schema: raw
`,
		map[string]string{
			"staging": `defaults:
  mode: truncate
`,
		},
		map[string]string{
			"staging/orders.sql": "SELECT 1 as id",
			"marts/fct.sql":      "SELECT 2 as id",
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	assert.Equal(t, "truncate", orders.Config.Mode, "child defaults.mode should apply")
	assert.Equal(t, "raw", orders.Schema, "root defaults.schema should override folder")
	// In prod mode, a single folder becomes the schema, so prefix is empty.
	assert.Equal(t, "raw.orders", orders.FullTableName)

	fct := project.Models["fct"]
	require.NotNil(t, fct)
	assert.Equal(t, "", fct.Config.Mode, "marts has no child defaults.mode")
	assert.Equal(t, "raw", fct.Schema, "root defaults.schema applies everywhere")
	assert.Equal(t, "raw.fct", fct.FullTableName)
}

func TestDefaultsSchemaOverride(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  schema: analytics
`,
		nil,
		map[string]string{
			"staging/orders.sql": "SELECT 1 as id",
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	assert.Equal(t, "analytics", orders.Schema)
	assert.Equal(t, "analytics.orders", orders.FullTableName)

	// ProdFullTableName is intentionally not rewritten — used for ref() matching.
	assert.Equal(t, "staging.orders", orders.ProdFullTableName)
}

func TestDefaultsEnabledFalseSkippedInDAG(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
`,
		nil,
		map[string]string{
			"staging/stg_active.sql":   "SELECT 1",
			"staging/stg_disabled.sql": "/**\nenabled: false\n**/\nSELECT 2",
		},
		nil,
	)

	// Both models are loaded into project.Models
	assert.Contains(t, project.Models, "stg_active")
	assert.Contains(t, project.Models, "stg_disabled")

	// Confirm enabled state was parsed from frontmatter
	disabled := project.Models["stg_disabled"]
	require.NotNil(t, disabled.Config.Enabled)
	assert.False(t, *disabled.Config.Enabled)

	// Build the DAG — disabled model should NOT be a node.
	// No SQL refs between models, so DependsOn is empty and no compile needed.
	dag, err := BuildDAG(project)
	require.NoError(t, err)

	assert.Contains(t, dag.Nodes, "stg_active")
	assert.NotContains(t, dag.Nodes, "stg_disabled")
}

func TestDefaultsEnabledFalseViaDefaults(t *testing.T) {
	// Enabled:false set via defaults (not frontmatter) on a child folder.
	project := buildDefaultsProject(t,
		`target: POSTGRES
`,
		map[string]string{
			"archive": `defaults:
  enabled: false
`,
		},
		map[string]string{
			"staging/stg_active.sql": "SELECT 1",
			"archive/old_model.sql":  "SELECT 2",
		},
		nil,
	)

	oldModel := project.Models["old_model"]
	require.NotNil(t, oldModel)
	require.NotNil(t, oldModel.Config.Enabled)
	assert.False(t, *oldModel.Config.Enabled)

	dag, err := BuildDAG(project)
	require.NoError(t, err)
	assert.Contains(t, dag.Nodes, "stg_active")
	assert.NotContains(t, dag.Nodes, "old_model")
}

func TestFrontmatterSchemaOverridesFolder(t *testing.T) {
	// Regression test for the latent bug fixed alongside defaults.schema:
	// a frontmatter `schema:` must override the folder-derived Schema + FullTableName.
	project := buildDefaultsProject(t,
		`target: POSTGRES
`,
		nil,
		map[string]string{
			"staging/orders.sql": "/**\nschema: custom\n**/\nSELECT 1",
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	assert.Equal(t, "custom", orders.Schema)
	assert.Equal(t, "custom.orders", orders.FullTableName)
	// Prod name is untouched — ref() matches against this.
	assert.Equal(t, "staging.orders", orders.ProdFullTableName)
}

func TestDefaultsHooksMergedWithFrontmatter(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  hooks:
    start:
      - type: log
        message: default_start
`,
		nil,
		map[string]string{
			"staging/orders.sql": `/**
hooks:
  start:
    - type: log
      message: fm_start
**/
SELECT 1`,
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	require.Len(t, orders.Config.Hooks.Start, 2, "defaults + frontmatter hooks should merge")
	assert.Equal(t, "default_start", orders.Config.Hooks.Start[0].(map[string]any)["message"])
	assert.Equal(t, "fm_start", orders.Config.Hooks.Start[1].(map[string]any)["message"])
}

func TestDefaultsTagsMergedWithFrontmatter(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  tags: [a, b]
`,
		nil,
		map[string]string{
			"staging/orders.sql": `/**
tags: [b, c]
**/
SELECT 1`,
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	assert.Equal(t, []string{"a", "b", "c"}, orders.Config.Tags)
}

func TestDefaultsAppliedToSeedSchema(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  schema: raw
`,
		nil,
		nil,
		map[string]string{
			"staging/country_codes.csv": "code,name\nUS,United States\n",
		},
	)

	seed := project.Seeds["country_codes"]
	require.NotNil(t, seed)
	assert.Equal(t, "raw", seed.Schema)
	assert.Equal(t, "raw.country_codes", seed.FullTableName)
}

func TestDefaultsUniqueKeyAndMergeStrategy(t *testing.T) {
	project := buildDefaultsProject(t,
		`target: POSTGRES
defaults:
  mode: incremental
  unique_key: id
  update_key: updated_at
  merge_strategy: delete+insert
`,
		nil,
		map[string]string{
			"staging/orders.sql": "SELECT id, updated_at FROM raw WHERE {{ incremental_where_cond() }}",
		},
		nil,
	)

	orders := project.Models["orders"]
	require.NotNil(t, orders)
	assert.Equal(t, "incremental", orders.Config.Mode)
	assert.Equal(t, "id", orders.Config.UniqueKey)
	assert.Equal(t, "updated_at", orders.Config.UpdateKey)
	assert.Equal(t, "delete+insert", orders.Config.MergeStrategy)
}
