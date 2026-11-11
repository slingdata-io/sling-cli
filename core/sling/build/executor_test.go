package build

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSQLResult struct {
	n   int64
	err error
}

func (m mockSQLResult) LastInsertId() (int64, error) { return 0, nil }
func (m mockSQLResult) RowsAffected() (int64, error) { return m.n, m.err }

func TestExecutionResultRowsClamp(t *testing.T) {
	assert.Equal(t, uint64(0), rowsFromResult(nil))
	assert.Equal(t, uint64(0), rowsFromResult(mockSQLResult{n: -1}))
	assert.Equal(t, uint64(0), rowsFromResult(mockSQLResult{n: 5, err: errors.New("unsupported")}))
	assert.Equal(t, uint64(0), rowsFromResult(mockSQLResult{n: 0}))
	assert.Equal(t, uint64(42), rowsFromResult(mockSQLResult{n: 42}))

	rows, err := rowsFromExec(nil, errors.New("boom"))
	assert.Equal(t, uint64(0), rows)
	assert.Error(t, err)

	rows, err = rowsFromExec(mockSQLResult{n: -1}, nil)
	assert.Equal(t, uint64(0), rows)
	assert.NoError(t, err)
}

func TestCountIfColumnStoreSkipped(t *testing.T) {
	e := &Executor{}
	assert.Equal(t, uint64(7), e.countIfColumnStore("analytics.t", 7))
}

func TestNewExecutor(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	exec, err := NewExecutor(b)
	require.NoError(t, err)
	assert.Equal(t, "POSTGRES", exec.ConnName)
	assert.NotNil(t, exec.Build)
	assert.NotNil(t, exec.failedSet)
}

func TestNewExecutorNoTarget(t *testing.T) {
	dir := t.TempDir()

	b, err := NewBuild(dir, BuildOptions{})
	require.NoError(t, err)

	_, err = NewExecutor(b)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no target connection specified")
}

func TestGetUniqueKeysString(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: "id"}}
	keys := getUniqueKeys(model)
	assert.Equal(t, []string{"id"}, keys)
}

func TestGetUniqueKeysStringSlice(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: []string{"id", "name"}}}
	keys := getUniqueKeys(model)
	assert.Equal(t, []string{"id", "name"}, keys)
}

func TestGetUniqueKeysInterfaceSlice(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: []interface{}{"id", "name"}}}
	keys := getUniqueKeys(model)
	assert.Equal(t, []string{"id", "name"}, keys)
}

func TestGetUniqueKeysNil(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: nil}}
	keys := getUniqueKeys(model)
	assert.Nil(t, keys)
}

func TestGetUniqueKeysEmptyString(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: ""}}
	keys := getUniqueKeys(model)
	assert.Nil(t, keys)
}

func backtick(s string) string { return "`" + s + "`" }

func allKeysModel() *Model {
	return &Model{Config: ModelConfig{
		UniqueKey:   "id",
		OrderBy:     "(d, id)",
		PartitionBy: "toYYYYMM(ts)",
		PrimaryKey:  "d",
		TTL:         "ts + INTERVAL 2 YEAR",
		Settings: ModelSettings{
			Table: SettingValues{"storage_policy": "p"},
			Query: SettingValues{"max_memory_usage": 100},
		},
	}}
}

func TestCHTableDefault(t *testing.T) {
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY tuple()", newCHTable(nil, backtick, true).clause())
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY tuple()", newCHTable(&Model{}, backtick, true).clause())

	model := &Model{Config: ModelConfig{UniqueKey: "id"}}
	assert.Equal(t,
		"ENGINE = MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1",
		newCHTable(model, backtick, true).clause())

	model = &Model{Config: ModelConfig{Engine: "ReplacingMergeTree(ts)"}}
	assert.Equal(t, "ENGINE = ReplacingMergeTree(ts) ORDER BY tuple()", newCHTable(model, nil, true).clause())

	model = &Model{Config: ModelConfig{UniqueKey: []string{"order_key", "line_number"}}}
	clause := newCHTable(model, nil, true).clause()
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY (order_key, line_number) SETTINGS allow_nullable_key = 1", clause)
	assert.NotContains(t, strings.ToUpper(clause), "TUPLE()")
}

func TestCHTableClauseOrder(t *testing.T) {
	model := allKeysModel()
	assert.Equal(t,
		"ENGINE = MergeTree() PARTITION BY toYYYYMM(ts) PRIMARY KEY d ORDER BY (d, id) TTL ts + INTERVAL 2 YEAR SETTINGS allow_nullable_key = 1, storage_policy = 'p'",
		newCHTable(model, backtick, true).clause())
	assert.Len(t, model.Config.Settings.Table, 1, "the model settings stay unchanged")
}

func TestCHTableAllowNullableOverride(t *testing.T) {
	model := &Model{Config: ModelConfig{
		OrderBy:  "(d, id)",
		Settings: ModelSettings{Table: SettingValues{"allow_nullable_key": 0}},
	}}
	clause := newCHTable(model, backtick, true).clause()
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY (d, id) SETTINGS allow_nullable_key = 0", clause)
	assert.Equal(t, 1, strings.Count(clause, "allow_nullable_key"))
}

func TestCHTableProton(t *testing.T) {
	model := allKeysModel()
	assert.Equal(t,
		"ENGINE = MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1",
		newCHTable(model, backtick, false).clause())
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY tuple()", newCHTempTable(model, false).clause())
}

func TestCHTableTemp(t *testing.T) {
	assert.Equal(t,
		"ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = 'p'",
		newCHTempTable(allKeysModel(), true).clause())
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY tuple()", newCHTempTable(&Model{}, true).clause())
	assert.Equal(t, "ENGINE = MergeTree() ORDER BY tuple()", newCHTempTable(nil, true).clause())
}

func TestSettingValuesSQL(t *testing.T) {
	assert.Equal(t, "", SettingValues{}.sql())
	assert.Equal(t, "", SettingValues(nil).sql())
	assert.Equal(t, `a = 'a\'b\\c', b = 3`, SettingValues{"b": 3, "a": `a'b\c`}.sql())
	assert.Equal(t, "i = 3, i64 = 4, u64 = 5", SettingValues{"i": 3, "i64": int64(4), "u64": uint64(5)}.sql())
	assert.Equal(t, "big = 10000000000, half = 0.5", SettingValues{"big": float64(1e10), "half": 0.5}.sql())
	assert.Equal(t, "f = 0, t = 1", SettingValues{"t": true, "f": false}.sql())
}

func TestSettingValuesWire(t *testing.T) {
	v := SettingValues{"s": "a'b", "n": 3, "f": 0.5, "t": true, "z": false}
	got := v.wire()
	assert.Equal(t, map[string]string{"s": "a'b", "n": "3", "f": "0.5", "t": "1", "z": "0"}, got)

	got["s"] = "changed"
	assert.Equal(t, "a'b", v.wire()["s"], "each call returns a new map")
	assert.Empty(t, SettingValues(nil).wire())
}

type settingsKey struct{}

type recordedCall struct {
	method   string
	sql      string
	settings map[string]string
}

// recordingConn is a target that records each statement.
type recordingConn struct {
	database.Connection // nil; a call to a method that is not overridden panics
	dbType              dbio.Type
	calls               []recordedCall
	exists              bool
}

func (c *recordingConn) GetType() dbio.Type             { return c.dbType }
func (c *recordingConn) Quote(f string) string          { return c.dbType.Quote(f) }
func (c *recordingConn) Context() *g.Context            { return g.NewContext(context.Background()) }
func (c *recordingConn) GetCount(string) (int64, error) { return 5, nil }
func (c *recordingConn) TableExists(database.Table) (bool, error) {
	return c.exists, nil
}
func (c *recordingConn) GetTemplateValue(path string) string { return c.dbType.GetTemplateValue(path) }

func (c *recordingConn) record(method string, ctx context.Context, q string) {
	settings, _ := ctx.Value(settingsKey{}).(map[string]string)
	c.calls = append(c.calls, recordedCall{method, q, settings})
}

func (c *recordingConn) Exec(q string, args ...any) (sql.Result, error) {
	c.record("Exec", context.Background(), q)
	return mockSQLResult{n: 1}, nil
}

func (c *recordingConn) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	c.record("ExecContext", ctx, q)
	return mockSQLResult{n: 1}, nil
}

func (c *recordingConn) ExecMulti(qs ...string) (sql.Result, error) {
	c.record("ExecMulti", context.Background(), strings.Join(qs, ";"))
	return mockSQLResult{n: 1}, nil
}

func (c *recordingConn) ExecMultiContext(ctx context.Context, qs ...string) (sql.Result, error) {
	c.record("ExecMultiContext", ctx, strings.Join(qs, ";"))
	return mockSQLResult{n: 1}, nil
}

func (c *recordingConn) QuerySettingsContext(ctx context.Context, settings map[string]string) context.Context {
	return context.WithValue(ctx, settingsKey{}, settings)
}

// find returns the calls whose SQL starts with prefix.
func (c *recordingConn) find(prefix string) (out []recordedCall) {
	for _, call := range c.calls {
		if strings.HasPrefix(call.sql, prefix) {
			out = append(out, call)
		}
	}
	return out
}

// plainConn hides QuerySettingsContext, as ProtonConn does.
type plainConn struct{ database.Connection }

func newRecordingExecutor(dbType dbio.Type) (*Executor, *recordingConn) {
	conn := &recordingConn{dbType: dbType}
	return &Executor{DbConn: conn, RunID: "r1"}, conn
}

func queryModel(query SettingValues) *Model {
	return &Model{
		Name:          "t",
		Schema:        "s",
		FullTableName: "s.t",
		Config: ModelConfig{
			UniqueKey: "id",
			Settings:  ModelSettings{Table: SettingValues{"storage_policy": "p"}, Query: query},
		},
	}
}

func TestExecModelQuerySettings(t *testing.T) {
	want := map[string]string{"max_memory_usage": "100"}
	e, conn := newRecordingExecutor(dbio.TypeDbClickhouse)
	model := queryModel(SettingValues{"max_memory_usage": 100})

	_, err := e.createTableAs("s.t", "SELECT 1 AS id", model)
	require.NoError(t, err)
	_, err = e.createTempTable(model, "s.tmp", "SELECT 1 AS id")
	require.NoError(t, err)
	_, err = e.insertSelect(model, "SELECT 1")
	require.NoError(t, err)

	ctas := conn.find("CREATE TABLE `s`.`t` ")
	require.Len(t, ctas, 1)
	assert.Equal(t, "ExecContext", ctas[0].method)
	assert.Equal(t, want, ctas[0].settings)
	assert.Equal(t,
		"CREATE TABLE `s`.`t` ENGINE = MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1, storage_policy = 'p' AS (SELECT 1 AS id)",
		ctas[0].sql)

	tmp := conn.find("CREATE TABLE `s`.`tmp`")
	require.Len(t, tmp, 1)
	assert.Equal(t, "ExecContext", tmp[0].method)
	assert.Equal(t, want, tmp[0].settings)
	assert.Equal(t,
		"CREATE TABLE `s`.`tmp` ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = 'p' AS (SELECT 1 AS id)",
		tmp[0].sql)

	ins := conn.find("INSERT INTO `s`.`t` SELECT 1")
	require.Len(t, ins, 1)
	assert.Equal(t, "ExecContext", ins[0].method)
	assert.Equal(t, want, ins[0].settings)

	// append into an existing column store table: tmp CTAS, then INSERT ... SELECT
	e, conn = newRecordingExecutor(dbio.TypeDbClickhouse)
	conn.exists = true
	model.CompiledSQL = "SELECT 1 AS id"
	_, err = e.executeAppend(model)
	require.NoError(t, err)
	tmpName := "`s`.`_sling_build_tmp_t_r1`"
	tmpCTAS := conn.find("CREATE TABLE " + tmpName)
	require.Len(t, tmpCTAS, 1)
	assert.Equal(t, "ExecContext", tmpCTAS[0].method)
	assert.Equal(t, want, tmpCTAS[0].settings)
	appendIns := conn.find("INSERT INTO `s`.`t` SELECT * FROM " + tmpName)
	require.Len(t, appendIns, 1)
	assert.Equal(t, "ExecContext", appendIns[0].method)
	assert.Equal(t, want, appendIns[0].settings)

	// merge string with two statements
	e, conn = newRecordingExecutor(dbio.TypeDbClickhouse)
	_, err = e.execModelMulti(model, "ALTER TABLE a DELETE WHERE 1; INSERT INTO a SELECT 1")
	require.NoError(t, err)
	require.Len(t, conn.calls, 1)
	assert.Equal(t, "ExecMultiContext", conn.calls[0].method)
	assert.Equal(t, want, conn.calls[0].settings)

	// a model with no query settings sends the plain calls and no settings
	e, conn = newRecordingExecutor(dbio.TypeDbClickhouse)
	plain := queryModel(nil)
	_, err = e.createTableAs("s.t", "SELECT 1 AS id", plain)
	require.NoError(t, err)
	_, err = e.createTempTable(plain, "s.tmp", "SELECT 1 AS id")
	require.NoError(t, err)
	_, err = e.insertSelect(plain, "SELECT 1")
	require.NoError(t, err)
	_, err = e.execModelMulti(plain, "SELECT 1; SELECT 2")
	require.NoError(t, err)
	_, err = e.execModelMulti(nil, "SELECT 3")
	require.NoError(t, err)
	require.Len(t, conn.calls, 5)
	for i, call := range conn.calls {
		assert.Nil(t, call.settings, "call %d", i)
		assert.NotContains(t, call.method, "Context", "call %d", i)
	}
}

func TestExecModelNonClickhouse(t *testing.T) {
	model := queryModel(SettingValues{"max_memory_usage": 100})

	e, conn := newRecordingExecutor(dbio.TypeDbPostgres)
	_, err := e.createTableAs("s.t", "SELECT 1", model)
	require.NoError(t, err)
	require.Len(t, conn.calls, 1)
	assert.Equal(t, "Exec", conn.calls[0].method)
	assert.Equal(t, `CREATE TABLE "s"."t" AS (SELECT 1)`, conn.calls[0].sql)
	assert.Nil(t, conn.calls[0].settings)

	// Proton has no QuerySettingsContext: plain Exec with today's clause
	rec := &recordingConn{dbType: dbio.TypeDbProton}
	e = &Executor{DbConn: plainConn{rec}, RunID: "r1"}
	_, err = e.createTableAs("s.t", "SELECT 1", model)
	require.NoError(t, err)
	_, err = e.createTempTable(model, "s.tmp", "SELECT 1")
	require.NoError(t, err)
	require.Len(t, rec.calls, 2)
	for _, call := range rec.calls {
		assert.Equal(t, "Exec", call.method)
		assert.Nil(t, call.settings)
	}
	assert.Equal(t,
		"CREATE TABLE `s`.`t` ENGINE = MergeTree() ORDER BY (`id`) SETTINGS allow_nullable_key = 1 AS (SELECT 1)",
		rec.calls[0].sql)
	assert.Equal(t, "CREATE TABLE `s`.`tmp` ENGINE = MergeTree() ORDER BY tuple() AS (SELECT 1)", rec.calls[1].sql)
}

func TestInsertSelectClickhouseNoParens(t *testing.T) {
	model := queryModel(nil)

	e, conn := newRecordingExecutor(dbio.TypeDbClickhouse)
	_, err := e.insertSelect(model, "SELECT 1")
	require.NoError(t, err)
	require.Len(t, conn.calls, 1)
	assert.Equal(t, "INSERT INTO `s`.`t` SELECT 1", conn.calls[0].sql)

	e, conn = newRecordingExecutor(dbio.TypeDbPostgres)
	_, err = e.insertSelect(model, "SELECT 1")
	require.NoError(t, err)
	require.Len(t, conn.calls, 1)
	assert.Equal(t, `INSERT INTO "s"."t" (SELECT 1)`, conn.calls[0].sql)
}

func TestFormatDurationMilliseconds(t *testing.T) {
	d := 250 * time.Millisecond
	assert.Equal(t, "250ms", formatDuration(d))
}

func TestFormatDurationSeconds(t *testing.T) {
	d := 1500 * time.Millisecond
	assert.Equal(t, "1.5s", formatDuration(d))
}

func TestFormatDurationZero(t *testing.T) {
	d := time.Duration(0)
	assert.Equal(t, "0ms", formatDuration(d))
}

func TestExecutionResultTracking(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	exec, err := NewExecutor(b)
	require.NoError(t, err)

	// Add results manually
	exec.Results = append(exec.Results, ExecutionResult{
		Name:     "stg_orders",
		NodeType: "model",
		Mode:     "full-refresh",
		Duration: 200 * time.Millisecond,
	})
	exec.Results = append(exec.Results, ExecutionResult{
		Name:     "fct_orders",
		NodeType: "model",
		Mode:     "incremental",
		Duration: 500 * time.Millisecond,
		Err:      assert.AnError,
	})

	assert.Len(t, exec.Results, 2)
	assert.Nil(t, exec.Results[0].Err)
	assert.NotNil(t, exec.Results[1].Err)
}

func TestGetMergeStrategyDeleteInsert(t *testing.T) {
	model := &Model{Config: ModelConfig{MergeStrategy: "delete+insert"}}
	strategy := getMergeStrategy(model, false)
	assert.Equal(t, database.MergeStrategyDeleteInsert, strategy)
}

func TestGetMergeStrategyUpdateInsert(t *testing.T) {
	model := &Model{Config: ModelConfig{MergeStrategy: "update+insert"}}
	strategy := getMergeStrategy(model, false)
	assert.Equal(t, database.MergeStrategyUpdateInsert, strategy)
}

func TestGetMergeStrategyInsert(t *testing.T) {
	model := &Model{Config: ModelConfig{MergeStrategy: "insert"}}
	strategy := getMergeStrategy(model, false)
	assert.Equal(t, database.MergeStrategyInsert, strategy)
}

func TestGetMergeStrategyDefault(t *testing.T) {
	// Empty strategy defaults to delete+insert
	model := &Model{Config: ModelConfig{}}
	strategy := getMergeStrategy(model, false)
	assert.Equal(t, database.MergeStrategyDeleteInsert, strategy)
}

func TestGetMergeStrategyClickHouseOverride(t *testing.T) {
	// ClickHouse forces delete+insert regardless of user setting
	model := &Model{Config: ModelConfig{MergeStrategy: "update+insert"}}
	strategy := getMergeStrategy(model, true)
	assert.Equal(t, database.MergeStrategyDeleteInsert, strategy)

	// delete+insert is kept as-is for ClickHouse
	model2 := &Model{Config: ModelConfig{MergeStrategy: "delete+insert"}}
	strategy2 := getMergeStrategy(model2, true)
	assert.Equal(t, database.MergeStrategyDeleteInsert, strategy2)

	// Empty also defaults to delete+insert for ClickHouse
	model3 := &Model{Config: ModelConfig{}}
	strategy3 := getMergeStrategy(model3, true)
	assert.Equal(t, database.MergeStrategyDeleteInsert, strategy3)
}

func TestGetTempTableName(t *testing.T) {
	model := &Model{Name: "fct_orders", Schema: "marts"}

	// Always schema-qualified with run ID for concurrent-run isolation
	tempName := getTempTableName(model, "abc123")
	assert.Equal(t, "marts._sling_build_tmp_fct_orders_abc123", tempName)

	tempName2 := getTempTableName(model, "xyz789")
	assert.Equal(t, "marts._sling_build_tmp_fct_orders_xyz789", tempName2)
	assert.NotEqual(t, tempName, tempName2)
}

func TestGetTempTableNameThreePart(t *testing.T) {
	model := &Model{Name: "fct_orders", Schema: "marts", Database: "ANALYTICS_DB"}

	tempName := getTempTableName(model, "abc123")
	assert.Equal(t, "ANALYTICS_DB.marts._sling_build_tmp_fct_orders_abc123", tempName)
}

func TestNormalizeMode(t *testing.T) {
	m, warn := normalizeMode("snapshot")
	assert.Equal(t, "append", m)
	assert.NotEmpty(t, warn)

	m, warn = normalizeMode("table")
	assert.Equal(t, "full-refresh", m)
	assert.Empty(t, warn)

	m, _ = normalizeMode("append")
	assert.Equal(t, "append", m)
}

func TestMapMaterialized(t *testing.T) {
	m, err := mapMaterialized("table")
	require.NoError(t, err)
	assert.Equal(t, "full-refresh", m)

	m, err = mapMaterialized("view")
	require.NoError(t, err)
	assert.Equal(t, "view", m)

	m, err = mapMaterialized("incremental")
	require.NoError(t, err)
	assert.Equal(t, "incremental", m)

	_, err = mapMaterialized("ephemeral")
	assert.Error(t, err)
}

func TestCompileDataTestNotNull(t *testing.T) {
	sql, label, err := compileDataTest(
		map[string]any{"not_null": []any{"id", "name"}},
		`"public"."orders"`,
		func(s string) string { return `"` + s + `"` },
	)
	require.NoError(t, err)
	assert.Contains(t, label, "not_null")
	assert.Contains(t, sql, `"id" IS NULL`)
	assert.Contains(t, sql, `"name" IS NULL`)
}

func TestCompileDataTestUnique(t *testing.T) {
	sql, label, err := compileDataTest(
		map[string]any{"unique": "id"},
		`"public"."orders"`,
		func(s string) string { return `"` + s + `"` },
	)
	require.NoError(t, err)
	assert.Contains(t, label, "unique")
	assert.Contains(t, sql, "GROUP BY")
	assert.Contains(t, sql, "HAVING count(*) > 1")
}

func TestCompileDataTestExpr(t *testing.T) {
	sql, label, err := compileDataTest(
		map[string]any{"expr": "sum(amount) >= 0"},
		`"public"."orders"`,
		func(s string) string { return `"` + s + `"` },
	)
	require.NoError(t, err)
	assert.Contains(t, label, "expr")
	assert.Contains(t, sql, "sum(amount) >= 0")
}

func TestContainsSemicolonDollarQuote(t *testing.T) {
	// Semicolon inside dollar-quoted body should not count
	sql := `CREATE FUNCTION f() RETURNS void AS $$ BEGIN PERFORM 1; END; $$ LANGUAGE plpgsql`
	assert.False(t, containsSemicolon(sql))

	// Real statement separator outside dollar quotes
	sql2 := `SELECT 1; SELECT 2`
	assert.True(t, containsSemicolon(sql2))
}

func TestIncrementalRequiresUniqueKey(t *testing.T) {
	// Model with incremental mode but no unique_key should error
	model := &Model{
		Name:          "bad_model",
		FullTableName: "public.bad_model",
		Config:        ModelConfig{Mode: "incremental"},
		CompiledSQL:   "SELECT 1",
	}

	dir := getTestFixturePath("sample_project")
	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	exec, err := NewExecutor(b)
	require.NoError(t, err)

	_, err = exec.executeIncremental(model)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no unique_key defined")
}

func TestIsDownstreamOfFailed(t *testing.T) {
	dir := getTestFixturePath("sample_project")

	b, err := NewBuild(dir, BuildOptions{Target: "POSTGRES"})
	require.NoError(t, err)
	require.NoError(t, b.Compile())

	exec, err := NewExecutor(b)
	require.NoError(t, err)

	// No failures yet — fct_orders depends on stg_orders
	assert.False(t, exec.isDownstreamOfFailed("fct_orders"))

	// Mark stg_orders as failed
	exec.failedSet["stg_orders"] = true

	// Now fct_orders should be downstream of a failure
	assert.True(t, exec.isDownstreamOfFailed("fct_orders"))

	// stg_customers is independent, should not be affected
	assert.False(t, exec.isDownstreamOfFailed("stg_customers"))
}

// =============================================================================
// Range tests
// =============================================================================

func quote(s string) string { return `"` + s + `"` }

func TestRangeChunk_WhereCond_Unbounded(t *testing.T) {
	c := RangeChunk{Lower: "", Upper: ""}
	assert.Equal(t, "1=1", c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_LowerOnly_Exclusive(t *testing.T) {
	c := RangeChunk{Lower: "'2024-01-01'", Upper: "", LowerInclusive: false}
	assert.Equal(t, `"col" > '2024-01-01'`, c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_LowerOnly_Inclusive(t *testing.T) {
	c := RangeChunk{Lower: "'2024-01-01'", Upper: "", LowerInclusive: true}
	assert.Equal(t, `"col" >= '2024-01-01'`, c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_UpperOnly(t *testing.T) {
	c := RangeChunk{Lower: "", Upper: "'2024-12-31'"}
	assert.Equal(t, `"col" < '2024-12-31'`, c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_Bounded_Exclusive(t *testing.T) {
	c := RangeChunk{Lower: "'2024-01-01'", Upper: "'2024-02-01'", LowerInclusive: false}
	assert.Equal(t, `"col" > '2024-01-01' AND "col" < '2024-02-01'`, c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_Bounded_Inclusive(t *testing.T) {
	c := RangeChunk{Lower: "'2024-01-01'", Upper: "'2024-02-01'", LowerInclusive: true}
	assert.Equal(t, `"col" >= '2024-01-01' AND "col" < '2024-02-01'`, c.WhereCond("col", quote))
}

func TestRangeChunk_WhereCond_NullLower_TreatedUnbounded(t *testing.T) {
	c := RangeChunk{Lower: "null", Upper: "'2024-02-01'"}
	assert.Equal(t, `"col" < '2024-02-01'`, c.WhereCond("col", quote))
}

func TestParseCLIRange_StartEnd(t *testing.T) {
	start, end, step, hasStep, err := parseCLIRange("2024-01-01,2024-12-31")
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01", start)
	assert.Equal(t, "2024-12-31", end)
	assert.Equal(t, "", step)
	assert.False(t, hasStep)
}

func TestParseCLIRange_StartEndStep(t *testing.T) {
	start, end, step, hasStep, err := parseCLIRange("2024-01-01,2024-04-01,1mo")
	require.NoError(t, err)
	assert.Equal(t, "2024-01-01", start)
	assert.Equal(t, "2024-04-01", end)
	assert.Equal(t, "1mo", step)
	assert.True(t, hasStep)
}

func TestParseCLIRange_TooFewParts(t *testing.T) {
	_, _, _, _, err := parseCLIRange("2024-01-01")
	assert.Error(t, err)
}

func TestParseCLIRange_TooManyParts(t *testing.T) {
	_, _, _, _, err := parseCLIRange("a,b,c,d")
	assert.Error(t, err)
}

func TestParseCLIRange_EmptyStart(t *testing.T) {
	_, _, _, _, err := parseCLIRange(",2024-12-31")
	assert.Error(t, err)
}

func TestParseCLIRange_BadStep(t *testing.T) {
	_, _, _, _, err := parseCLIRange("2024-01-01,2024-12-31,notaduration")
	assert.Error(t, err)
}

func TestSplitCLIRange_NoStep_SingleChunk(t *testing.T) {
	r, err := splitCLIRange("2024-01-01,2024-12-31", dbio.TypeDbDuckDb, "")
	require.NoError(t, err)
	require.Len(t, r.Chunks, 1)
	assert.True(t, r.FromCLI)
	assert.False(t, r.UpdateState)
	assert.Equal(t, "2024-01-01", r.Chunks[0].LowerRaw)
	assert.Equal(t, "2024-12-31", r.Chunks[0].UpperRaw)
	assert.True(t, r.Chunks[0].LowerInclusive)
}

func TestSplitCLIRange_WithStep_MultipleChunks(t *testing.T) {
	// 1mo = 30d; Jan1→Apr1 = 91 days → 4 chunks (ceiling)
	r, err := splitCLIRange("2024-01-01,2024-04-01,1mo", dbio.TypeDbDuckDb, iop.TimestampType)
	require.NoError(t, err)
	assert.Greater(t, len(r.Chunks), 1)
	for _, c := range r.Chunks {
		assert.True(t, c.LowerInclusive)
	}
}

func TestSplitCLIRange_WithStep_FinalChunkClamped(t *testing.T) {
	// 2 full months + remainder
	r, err := splitCLIRange("2024-01-01,2024-03-15,1mo", dbio.TypeDbDuckDb, iop.TimestampType)
	require.NoError(t, err)
	assert.Equal(t, 3, len(r.Chunks))
	// Last chunk upper should be clamped to end (2024-03-15)
	lastChunk := r.Chunks[len(r.Chunks)-1]
	assert.Contains(t, lastChunk.UpperRaw, "2024-03-15")
}

func TestSplitCLIRange_WithStep_BadStartTime(t *testing.T) {
	_, err := splitCLIRange("not-a-date,2024-04-01,1mo", dbio.TypeDbDuckDb, iop.TimestampType)
	assert.Error(t, err)
}

func TestQuoteValue_Nil(t *testing.T) {
	result := quoteValue(nil, "", dbio.TypeDbDuckDb)
	assert.Equal(t, "null", result)
}

func TestQuoteValue_StringFallback(t *testing.T) {
	result := quoteValue("hello", iop.StringType, dbio.TypeDbDuckDb)
	assert.Contains(t, result, "hello")
}

func TestQuoteValue_TimestampForDuckDB(t *testing.T) {
	// iop.FormatValue wraps timestamp values in quotes for DuckDB
	result := quoteValue("2024-01-15", iop.StringType, dbio.TypeDbDuckDb)
	assert.NotEmpty(t, result)
}

func TestParseValueAsTime_ISO8601(t *testing.T) {
	t1, err := parseValueAsTime("2024-01-15T00:00:00Z", iop.TimestampType)
	require.NoError(t, err)
	assert.Equal(t, 2024, t1.Year())
	assert.Equal(t, 1, int(t1.Month()))
	assert.Equal(t, 15, t1.Day())
}

func TestParseValueAsTime_ISO8601_DateOnly(t *testing.T) {
	t1, err := parseValueAsTime("2024-03-01", iop.DateType)
	require.NoError(t, err)
	assert.Equal(t, 2024, t1.Year())
	assert.Equal(t, 3, int(t1.Month()))
}

func TestParseValueAsTime_Empty(t *testing.T) {
	_, err := parseValueAsTime("", iop.TimestampType)
	assert.Error(t, err)
}

func TestParseValueAsTime_NotDatetime(t *testing.T) {
	_, err := parseValueAsTime("123", iop.IntegerType)
	assert.Error(t, err)
}

// stripANSI removes ANSI color codes from a string for substring matching.
var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;]*[mGKHJA-Z]`)

func stripANSI(s string) string { return ansiEscapeRe.ReplaceAllString(s, "") }

func TestFormatChunkProgressLine_OK(t *testing.T) {
	c := RangeChunk{
		Lower:          "'2024-01-01'",
		Upper:          "'2024-02-01'",
		LowerRaw:       "2024-01-01",
		UpperRaw:       "2024-02-01",
		LowerInclusive: true,
	}
	line := stripANSI(formatChunkProgressLine(3, 12, c, "created_at", 1400*time.Millisecond, false))
	assert.Contains(t, line, "chunk 3/12")
	assert.Contains(t, line, "created_at=[2024-01-01, 2024-02-01)")
	assert.Contains(t, line, "OK")
	assert.Contains(t, line, "1.4s")
}

func TestFormatChunkProgressLine_Failed(t *testing.T) {
	c := RangeChunk{
		Lower:          "'2024-02-01'",
		Upper:          "'2024-03-01'",
		LowerRaw:       "2024-02-01",
		UpperRaw:       "2024-03-01",
		LowerInclusive: true,
	}
	line := stripANSI(formatChunkProgressLine(2, 3, c, "ts", 250*time.Millisecond, true))
	assert.Contains(t, line, "chunk 2/3")
	assert.Contains(t, line, "FAIL")
	assert.Contains(t, line, "250ms")
}

func TestFormatResumeCommand_WithStep(t *testing.T) {
	failed := RangeChunk{LowerRaw: "2024-02-01"}
	last := RangeChunk{UpperRaw: "2024-04-01"}
	assert.Equal(t, "2024-02-01,2024-04-01,1mo", formatResumeCommand(failed, last, "1mo"))
}

func TestProgressPrefixModeParensColored(t *testing.T) {
	old := env.NoColor
	env.NoColor = false
	t.Cleanup(func() { env.NoColor = old })

	prefix := progressPrefix(1, 2, "analytics.gitbook_insights_questions", "full-refresh")
	assert.Equal(t, "[1/2] analytics.gitbook_insights_questions (full-refresh) ", stripANSI(prefix))
	// Closing paren must sit inside the dark-gray span, not after the reset.
	assert.Contains(t, prefix, "\x1b[90m(full-refresh)\x1b[0m")
}

func TestFormatResumeCommand_WithoutStep(t *testing.T) {
	failed := RangeChunk{LowerRaw: "2024-02-01"}
	last := RangeChunk{UpperRaw: "2024-04-01"}
	assert.Equal(t, "2024-02-01,2024-04-01", formatResumeCommand(failed, last, ""))
}

func TestModelLogIsolationAcrossThreads(t *testing.T) {
	e := &Executor{modelLogs: make(map[string]g.LogLines)}
	e.attachLogSink()
	defer e.detachLogSink()

	const n = 20
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		unbind := e.bindModel("gitbook_insights_questions")
		defer unbind()
		for i := 0; i < n; i++ {
			env.LogSink(&g.LogLine{Text: "gitbook line %d", Args: []any{i}})
		}
	}()
	go func() {
		defer wg.Done()
		unbind := e.bindModel("gsc_indexed_questions")
		defer unbind()
		for i := 0; i < n; i++ {
			env.LogSink(&g.LogLine{Text: "gsc line %d", Args: []any{i}})
		}
	}()
	wg.Wait()

	// Unscoped logs must not leak into a model drain.
	env.LogSink(&g.LogLine{Text: "parent summary"})

	gitbook := e.drainLogs("gitbook_insights_questions")
	gsc := e.drainLogs("gsc_indexed_questions")
	parent := e.drainLogs("")

	require.Len(t, gitbook, n)
	require.Len(t, gsc, n)
	require.Len(t, parent, 1)
	assert.Equal(t, "parent summary", parent[0].Text)

	for _, ll := range gitbook {
		assert.Equal(t, "gitbook line %d", ll.Text)
		assert.Contains(t, ll.Group, "gitbook_insights_questions")
	}
	for _, ll := range gsc {
		assert.Equal(t, "gsc line %d", ll.Text)
		assert.Contains(t, ll.Group, "gsc_indexed_questions")
	}

	// Draining one model must not steal the other's leftover (already drained).
	assert.Empty(t, e.drainLogs("gitbook_insights_questions"))
	assert.Empty(t, e.drainLogs("gsc_indexed_questions"))
}

func TestDrainLogsDoesNotStealOtherModel(t *testing.T) {
	e := &Executor{modelLogs: make(map[string]g.LogLines)}
	e.attachLogSink()
	defer e.detachLogSink()

	started := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		unbind := e.bindModel("model_a")
		defer unbind()
		env.LogSink(&g.LogLine{Text: "a-1"})
		close(started)
		<-release
		env.LogSink(&g.LogLine{Text: "a-2"})
	}()

	<-started
	unbindB := e.bindModel("model_b")
	env.LogSink(&g.LogLine{Text: "b-1"})
	bLines := e.drainLogs("model_b")
	unbindB()
	close(release)
	wg.Wait()

	aLines := e.drainLogs("model_a")
	require.Len(t, bLines, 1)
	assert.Equal(t, "b-1", bLines[0].Text)
	require.Len(t, aLines, 2)
	assert.Equal(t, "a-1", aLines[0].Text)
	assert.Equal(t, "a-2", aLines[1].Text)
}
