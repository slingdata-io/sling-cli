package build

import (
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

func TestGetEngineClauseDefault(t *testing.T) {
	model := &Model{Config: ModelConfig{}}
	clause := getEngineClause(model)
	assert.Equal(t, "ENGINE = MergeTree()", clause)
}

func TestGetEngineClauseCustom(t *testing.T) {
	model := &Model{Config: ModelConfig{Engine: "ReplacingMergeTree(updated_at)"}}
	clause := getEngineClause(model)
	assert.Equal(t, "ENGINE = ReplacingMergeTree(updated_at)", clause)
}

func TestGetOrderByClauseNoKeys(t *testing.T) {
	model := &Model{Config: ModelConfig{}}
	clause := getOrderByClause(model, nil)
	assert.Equal(t, "ORDER BY tuple()", clause)
}

func TestGetOrderByClauseSingleKey(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: "id"}}
	clause := getOrderByClause(model, nil)
	assert.Equal(t, "ORDER BY (id)", clause)
}

func TestGetOrderByClauseMultipleKeys(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: []string{"id", "name"}}}
	clause := getOrderByClause(model, nil)
	assert.Equal(t, "ORDER BY (id, name)", clause)
}

func TestClickHouseCreateTableAllowsNullableKeys(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: []string{"order_key", "line_number"}}}
	clause := getOrderByClause(model, nil)
	assert.Equal(t, "ORDER BY (order_key, line_number)", clause)
	assert.NotContains(t, strings.ToUpper(clause), "TUPLE()")
}

func TestGetOrderByClauseQuoted(t *testing.T) {
	model := &Model{Config: ModelConfig{UniqueKey: "id"}}
	clause := getOrderByClause(model, func(s string) string { return `"` + s + `"` })
	assert.Equal(t, `ORDER BY ("id")`, clause)
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
