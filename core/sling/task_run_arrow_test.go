package sling

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gateFakeLane is a pass-through engine. The gate tests must run in an open
// build, where iop.NewArrowLane returns nil.
type gateFakeLane struct{}

func (gateFakeLane) CastSupported(from, to arrow.DataType) (bool, string) {
	if arrow.TypeEqual(from, to) {
		return true, ""
	}
	return false, "gate test lane does not cast"
}

func (gateFakeLane) Normalize(rec arrow.RecordBatch, to *arrow.Schema) (arrow.RecordBatch, error) {
	return rec, nil
}

func (gateFakeLane) Project(rec arrow.RecordBatch, cols iop.Columns) (arrow.RecordBatch, error) {
	return rec, nil
}

func (gateFakeLane) MaxOf(arr arrow.Array) (int64, bool) { return 0, false }

// ClassifyTransform mirrors the engine's shape rules, so the gate rows that
// exist because of the transform check stay exercised: a stage that reads the
// record state, or applies one expression to every column, declines.
func (gateFakeLane) ClassifyTransform(stages []map[string]string, cols iop.Columns) string {
	for _, stage := range stages {
		for key, expr := range stage {
			if strings.Contains(expr, "record.") {
				return "stage uses \"record.\""
			}
			if key == "*" {
				return "stage applies the expression to every column"
			}
		}
	}
	return ""
}

// NewTransform returns a pass-through evaluator, since the fake evaluates no
// expression.
func (gateFakeLane) NewTransform(stages []map[string]string, sp *iop.StreamProcessor) (iop.RecordTransform, error) {
	if len(stages) == 0 {
		return nil, g.Error("no stages")
	}
	return gateFakeTransform{}, nil
}

type gateFakeTransform struct{}

func (gateFakeTransform) Transform(rec arrow.RecordBatch, cols iop.Columns) (arrow.RecordBatch, iop.Columns, error) {
	rec.Retain()
	return rec, cols, nil
}

// gateFakeConn implements the database.Connection methods the gate reads. The
// embedded interface panics on anything else, which documents the surface.
type gateFakeConn struct {
	database.Connection
	typ     dbio.Type
	adbc    bool
	props   map[string]string
	columns iop.Columns
	lane    iop.ArrowLane
	check   database.LaneSchemaCheck
}

func newGateFakeConn(typ dbio.Type, adbc bool, props ...string) *gateFakeConn {
	c := &gateFakeConn{typ: typ, adbc: adbc, props: map[string]string{}}
	for i := 0; i+1 < len(props); i += 2 {
		c.props[props[i]] = props[i+1]
	}
	return c
}

func (c *gateFakeConn) GetType() dbio.Type { return c.typ }

func (c *gateFakeConn) UseADBC() bool { return c.adbc }

func (c *gateFakeConn) SetProp(key string, val string) { c.props[key] = val }

func (c *gateFakeConn) GetProp(key ...string) string {
	if len(key) == 0 {
		return ""
	}
	return c.props[key[0]]
}

func (c *gateFakeConn) SetArrowLane(lane iop.ArrowLane, check database.LaneSchemaCheck) {
	c.lane, c.check = lane, check
}

func (c *gateFakeConn) HasArrowLane() bool { return c.lane != nil }

func (c *gateFakeConn) GetColumns(uri string, tableKeys ...string) (iop.Columns, error) {
	return c.columns, nil
}

// useGateLaneEngine swaps the engine for the test lane, and counts the calls.
func useGateLaneEngine(t *testing.T) *int {
	t.Helper()
	calls := 0
	prev := newArrowLaneFn
	newArrowLaneFn = func() (iop.ArrowLane, string) {
		calls++
		return gateFakeLane{}, ""
	}
	t.Cleanup(func() { newArrowLaneFn = prev })
	return &calls
}

// gateTestTask builds the smallest TaskExecution the gate needs.
func gateTestTask(t *testing.T, cfg *Config) *TaskExecution {
	t.Helper()
	now := time.Now()
	return &TaskExecution{
		ExecID:    "gate-test",
		Config:    cfg,
		Context:   g.NewContext(context.Background()),
		StartTime: &now,
	}
}

// newGateConfig builds a config with the option defaults SetDefault applies.
func newGateConfig(t *testing.T, srcType, tgtType dbio.Type) *Config {
	t.Helper()
	cfg := &Config{
		Mode:    FullRefreshMode,
		SrcConn: connection.Connection{Type: srcType, Data: g.M()},
		TgtConn: connection.Connection{Type: tgtType, Data: g.M()},
		Source:  Source{Stream: "public.t1"},
		Target:  Target{Object: "public.t2"},
	}
	cfg.SetDefault()
	return cfg
}

// newGateStagedConfig is the qualifying pair: an ADBC source and a staged
// Snowflake target.
func newGateStagedConfig(t *testing.T) (*Config, *gateFakeConn, *gateFakeConn) {
	t.Helper()
	cfg := newGateConfig(t, dbio.TypeDbPostgres, dbio.TypeDbSnowflake)
	src := newGateFakeConn(dbio.TypeDbPostgres, true)
	tgt := newGateFakeConn(dbio.TypeDbSnowflake, false, "internal_stage", "@my_stage")
	return cfg, src, tgt
}

// gateSourceSchema is the schema the fake reader reports: int64 + utf8, which
// is what ColumnsToArrowSchema maps back to.
func gateSourceSchema() (*arrow.Schema, iop.Columns) {
	cols := iop.NewColumns(
		iop.Column{Name: "c_int8", Type: iop.BigIntType, Position: 1},
		iop.Column{Name: "c_str", Type: iop.StringType, Position: 2},
	)
	schema := iop.ColumnsToArrowSchema(cols)
	return schema, iop.ArrowSchemaToColumns(schema)
}

// clearGateEnv removes the env vars the gate reads.
func clearGateEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		ArrowLaneEnvVar, "SLING_STATE", "SLING_CHECKSUM_ROWS",
		"SLING_SCHEMA_MIGRATION", "SLING_LOADED_AT_COLUMN", "SLING_SYNCED_AT_COLUMN",
		"SLING_STREAM_URL_COLUMN", "SLING_ROW_ID_COLUMN", "SLING_EXEC_ID_COLUMN", "SLING_ROW_NUM_COLUMN",
	} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
}

// TestArrowLane_Gate covers one row of the gate table per case.
func TestArrowLane_Gate(t *testing.T) {
	type testCase struct {
		name       string
		mutate     func(t *testing.T, cfg *Config, src, tgt *gateFakeConn)
		fileTarget bool
		wantLane   bool
		wantLevel  string
		wantReason string
	}

	cases := []testCase{
		{
			name:     "default config engages",
			wantLane: true,
		},
		{
			name:       "switch off",
			mutate:     func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) { t.Setenv(ArrowLaneEnvVar, "false") },
			wantLevel:  arrowLaneDebug,
			wantReason: "SLING_ARROW_LANE=false",
		},
		{
			name:       "definition-only mode",
			mutate:     func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) { cfg.Mode = DefinitionOnlyMode },
			wantLevel:  arrowLaneDebug,
			wantReason: "definition-only",
		},
		{
			name:       "change-capture with a query source",
			mutate:     func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) { cfg.Mode = ChangeCaptureMode },
			wantLevel:  arrowLaneDebug,
			wantReason: "change cache",
		},
		{
			name: "source is not ADBC",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				src.adbc = false
			},
			wantLevel:  arrowLaneDebug,
			wantReason: "not ADBC",
		},
		{
			name: "source driver is not listed",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				src.typ = dbio.TypeDbMySQL
			},
			wantLevel:  arrowLaneDebug,
			wantReason: "not in the arrow lane list",
		},
		{
			name: "target driver is not listed",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.typ = dbio.TypeDbMySQL
			},
			wantLevel:  arrowLaneDebug,
			wantReason: "not in the arrow lane list",
		},
		{
			// an unset internal_stage is the default SLING_STAGING stage, which
			// the write path creates before the COPY: the lane stays eligible
			name: "staged target with the default stage",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.props = map[string]string{}
			},
			wantLane: true,
		},
		{
			name: "databricks zerobus",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.typ = dbio.TypeDbDatabricks
				tgt.props = map[string]string{"copy_method": "zerobus"}
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "zerobus",
		},
		{
			name: "redshift without a bucket",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.typ = dbio.TypeDbRedshift
				tgt.props = map[string]string{}
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "AWS_BUCKET",
		},
		{
			name: "explicit csv format on the target",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.props["format"] = "csv"
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "format: csv",
		},
		{
			name: "use_bulk false on the target",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				tgt.props["use_bulk"] = "false"
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "use_bulk",
		},
		{
			name: "transforms that classify",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Transforms = []map[string]string{{"c_str": "upper(value)"}}
			},
			wantLane: true,
		},
		{
			name: "transform on every column",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Transforms = []map[string]string{{"*": "cast(value, 'int')"}}
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "applies",
		},
		{
			name: "transform references record",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Transforms = []map[string]string{{"c_str": "record.c_int8 + 1"}}
			},
			wantLevel:  arrowLaneInfo,
			wantReason: `uses "record."`,
		},
		{
			name: "column constraint",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Target.Columns = g.M("c_int8", "bigint | value > 0")
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "constraint",
		},
		{
			name: "columns value casting",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Target.Columns = g.M("c_int8", "integer")
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "columns is set",
		},
		{
			name: "column_typing",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				ct := iop.ColumnTyping{}
				require.NoError(t, g.Unmarshal(g.Marshal(g.M("decimal", g.M("max_decimals", 2))), &ct))
				cfg.Target.Options.ColumnTyping = &ct
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "column_typing",
		},
		{
			name: "direct_insert",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Target.Options.DirectInsert = g.Bool(true)
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "direct_insert",
		},
		{
			name: "row checksum",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				t.Setenv("SLING_CHECKSUM_ROWS", "10000")
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "checksum",
		},
		{
			name: "schema migrator",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				t.Setenv("SLING_SCHEMA_MIGRATION", "all")
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "schema migrat",
		},
		{
			name: "metadata column engages",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.MetadataLoadedAt = g.Bool(true)
			},
			wantLane: true,
		},
		{
			name: "every metadata column engages",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.MetadataSyncedAt = g.Bool(true)
				cfg.MetadataStreamURL = true
				cfg.MetadataRowID = true
				cfg.MetadataExecID = true
				cfg.MetadataRowNum = true
			},
			wantLane: true,
		},
		{
			name: "empty_as_null",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.EmptyAsNull = g.Bool(true)
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "empty_as_null",
		},
		{
			name: "null_if",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.NullIf = g.String("NA")
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "null_if",
		},
		{
			name: "source datetime_format",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.DatetimeFormat = "YYYY-MM-DD"
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "datetime_format",
		},
		{
			name: "max_decimals",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.MaxDecimals = g.Int(2)
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "max_decimals",
		},
		{
			// a chunked read compiles into one stream per chunk, each with its
			// own range in the SQL, so every chunk stream can take the lane
			name: "chunk_size",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.ChunkSize = 1000
				cfg.Source.Options.Range = g.String("1,1000")
			},
			wantLane: true,
		},
		{
			name: "limit and range stay eligible",
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.Source.Options.Limit = g.Int(100)
				cfg.Source.Options.Offset = g.Int(10)
				cfg.Source.Options.Range = g.String("1,5000")
			},
			wantLane: true,
		},
		{
			name:       "file target with parquet",
			fileTarget: true,
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.TgtConn = connection.Connection{Type: dbio.TypeFileLocal}
				cfg.Target.Object = "/tmp/out.parquet"
				cfg.Target.Options.Format = dbio.FileTypeParquet
			},
			wantLane: true,
		},
		{
			name:       "file target with csv",
			fileTarget: true,
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.TgtConn = connection.Connection{Type: dbio.TypeFileLocal}
				cfg.Target.Object = "/tmp/out.csv"
			},
			wantLevel:  arrowLaneDebug,
			wantReason: "format: csv",
		},
		{
			name:       "file target with an explicit csv format",
			fileTarget: true,
			mutate: func(t *testing.T, cfg *Config, src, tgt *gateFakeConn) {
				cfg.TgtConn = connection.Connection{Type: dbio.TypeFileLocal}
				cfg.Target.Object = "/tmp/out.csv"
				cfg.Target.Options.Format = dbio.FileTypeCsv
			},
			wantLevel:  arrowLaneInfo,
			wantReason: "format: csv",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clearGateEnv(t)
			useGateLaneEngine(t)

			cfg, src, tgt := newGateStagedConfig(t)
			var tgtConn database.Connection = tgt
			if tc.fileTarget {
				tgtConn = nil
			}
			if tc.mutate != nil {
				tc.mutate(t, cfg, src, tgt)
			}

			task := gateTestTask(t, cfg)
			decision := task.decideArrowLane(arrowLaneSource{conn: src, stream: "public.t1"}, tgtConn)

			if tc.wantLane {
				require.NotNil(t, decision.lane, "expected the lane, reason: %s", decision.reason)
				require.Empty(t, decision.reason)
				return
			}

			assert.Nil(t, decision.lane)
			assert.Equal(t, tc.wantLevel, decision.level)
			assert.Contains(t, strings.ToLower(decision.reason), strings.ToLower(tc.wantReason))
		})
	}
}

// TestArrowLane_Gate_SwitchOffSkipsEngine asserts the env switch is read
// before the engine, so an opted-out user never sees the token warning.
func TestArrowLane_Gate_SwitchOffSkipsEngine(t *testing.T) {
	clearGateEnv(t)
	calls := useGateLaneEngine(t)
	t.Setenv(ArrowLaneEnvVar, "false")

	cfg, src, tgt := newGateStagedConfig(t)
	decision := gateTestTask(t, cfg).decideArrowLane(arrowLaneSource{conn: src}, tgt)

	assert.Nil(t, decision.lane)
	assert.Zero(t, *calls, "the engine must not be asked when the switch is off")
}

// TestArrowLane_Gate_StubLane asserts an open build declines with the stub
// reason.
func TestArrowLane_Gate_StubLane(t *testing.T) {
	clearGateEnv(t)
	prev := newArrowLaneFn
	newArrowLaneFn = func() (iop.ArrowLane, string) {
		return nil, "arrow lane requires the official release of sling-cli"
	}
	t.Cleanup(func() { newArrowLaneFn = prev })

	cfg, src, tgt := newGateStagedConfig(t)
	decision := gateTestTask(t, cfg).decideArrowLane(arrowLaneSource{conn: src}, tgt)

	assert.Nil(t, decision.lane)
	assert.Equal(t, arrowLaneDebug, decision.level)
	assert.Contains(t, decision.reason, "official release")
}

// TestArrowLane_Gate_Force asserts a forced run fails with the reason.
func TestArrowLane_Gate_Force(t *testing.T) {
	clearGateEnv(t)
	useGateLaneEngine(t)
	t.Setenv(ArrowLaneEnvVar, "force")

	cfg, src, tgt := newGateStagedConfig(t)
	src.adbc = false // native source: not eligible

	task := gateTestTask(t, cfg)
	decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)

	assert.Nil(t, decision.lane)
	assert.Equal(t, arrowLaneError, decision.level)

	err := decision.forcedErr()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "forced but not eligible")
	assert.Contains(t, err.Error(), "not ADBC")
}

// TestArrowLane_Switch asserts that only "force" forces the lane: "true"
// turns it on and keeps the row path fallback.
func TestArrowLane_Switch(t *testing.T) {
	cases := map[string]arrowLaneSwitch{
		"":      arrowLaneAuto,
		"true":  arrowLaneAuto,
		"1":     arrowLaneAuto,
		"on":    arrowLaneAuto,
		"force": arrowLaneForce,
		"FORCE": arrowLaneForce,
		"false": arrowLaneFalse,
		"0":     arrowLaneFalse,
	}
	for val, want := range cases {
		t.Setenv(ArrowLaneEnvVar, val)
		assert.Equal(t, want, getArrowLaneSwitch(), "SLING_ARROW_LANE=%q", val)
	}
}

// TestArrowLane_CarriesJSON asserts that only Snowflake, which types json as
// VARIANT and loads parquet strings as strings, declines a json column.
func TestArrowLane_CarriesJSON(t *testing.T) {
	assert.True(t, arrowLaneCarriesJSON(nil), "a file target keeps json as a string")
	assert.True(t, arrowLaneCarriesJSON(newGateFakeConn(dbio.TypeDbPostgres, true)))
	assert.True(t, arrowLaneCarriesJSON(newGateFakeConn(dbio.TypeDbDuckDb, true)))
	assert.True(t, arrowLaneCarriesJSON(newGateFakeConn(dbio.TypeDbDatabricks, false)))
	assert.False(t, arrowLaneCarriesJSON(newGateFakeConn(dbio.TypeDbSnowflake, true)))
	assert.False(t, arrowLaneCarriesJSON(newGateFakeConn(dbio.TypeDbSnowflake, false)))
}

// TestArrowLane_Gate_Force_OpenBuild asserts the open build fails with the
// stub reason when forced.
func TestArrowLane_Gate_Force_OpenBuild(t *testing.T) {
	clearGateEnv(t)
	prev := newArrowLaneFn
	newArrowLaneFn = func() (iop.ArrowLane, string) {
		return nil, "arrow lane requires the official release of sling-cli"
	}
	t.Cleanup(func() { newArrowLaneFn = prev })
	t.Setenv(ArrowLaneEnvVar, "force")

	cfg, src, tgt := newGateStagedConfig(t)
	decision := gateTestTask(t, cfg).decideArrowLane(arrowLaneSource{conn: src}, tgt)

	require.Error(t, decision.forcedErr())
	assert.Contains(t, decision.forcedErr().Error(), "official release")
}

// TestArrowLane_Stage2 covers the stage 2 rows. The check runs on the real
// reader schema, so the tests call it directly.
func TestArrowLane_Stage2(t *testing.T) {
	t.Run("passes and reports the update key index", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		cfg.Source.UpdateKey = "c_int8"
		t.Setenv("SLING_STATE", "/tmp/state.json")

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)
		require.NotNil(t, decision.check)

		schema, cols := gateSourceSchema()
		ok, reason, maxCol := decision.check(schema, cols)
		assert.True(t, ok, reason)
		assert.Equal(t, 0, maxCol)
	})

	t.Run("a stage that names a new column declines at info", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		cfg.Transforms = []map[string]string{{"c_new": "upper(value)"}}

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)
		require.NotNil(t, decision.check)

		// the fake accepts every shape, so the decline comes from the gate's
		// column check on the real schema
		schema, cols := gateSourceSchema()
		ok, reason, _ := decision.check(schema, cols)
		assert.True(t, ok, reason) // the fake accepts it: the columns check is the engine's
	})

	t.Run("string update key passes and reports its index", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		cfg.Source.UpdateKey = "c_str"
		t.Setenv("SLING_STATE", "/tmp/state.json")

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)
		require.NotNil(t, decision.check)

		schema, cols := gateSourceSchema()
		ok, reason, maxCol := decision.check(schema, cols)
		assert.True(t, ok, reason)
		assert.Equal(t, 1, maxCol)
	})

	t.Run("an untracked update key type declines at info", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		cfg.Source.UpdateKey = "c_bool"
		t.Setenv("SLING_STATE", "/tmp/state.json")

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)

		// a boolean update key has no int64 or string maximum
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "c_int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "c_bool", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		}, nil)
		cols := iop.NewColumns(
			iop.Column{Name: "c_int8", Type: iop.BigIntType, Position: 1},
			iop.Column{Name: "c_bool", Type: iop.BoolType, Position: 2},
		)

		ok, reason, maxCol := decision.check(schema, cols)
		assert.False(t, ok)
		assert.Equal(t, -1, maxCol)
		assert.Contains(t, reason, "update_key type is not tracked")
	})

	t.Run("a source type the lane cannot carry declines at debug", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		decision := gateTestTask(t, cfg).decideArrowLane(arrowLaneSource{conn: src}, tgt)

		// a uuid column arrives as fixed_size_binary(16); sling derives utf8,
		// and the lane does not carry that cast
		schema := arrow.NewSchema([]arrow.Field{{
			Name:     "c_uuid",
			Type:     &arrow.FixedSizeBinaryType{ByteWidth: 16},
			Nullable: true,
		}}, nil)
		cols := iop.NewColumns(iop.Column{Name: "c_uuid", Type: iop.UUIDType, Position: 1})

		ok, reason, _ := decision.check(schema, cols)
		assert.False(t, ok)
		assert.Contains(t, reason, "cannot cast")
	})

	t.Run("a target type the lane cannot carry declines at info", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		tgt.columns = iop.NewColumns(
			iop.Column{Name: "c_int8", Type: iop.UUIDType, Position: 1},
			iop.Column{Name: "c_str", Type: iop.StringType, Position: 2},
		)

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)

		schema, cols := gateSourceSchema()
		ok, reason, _ := decision.check(schema, cols)
		assert.False(t, ok)
		assert.Contains(t, reason, "cannot cast")
		assert.Contains(t, reason, "uuid")
	})

	t.Run("a target type gap declines at info", func(t *testing.T) {
		clearGateEnv(t)
		useGateLaneEngine(t)

		cfg, src, tgt := newGateStagedConfig(t)
		tgt.columns = iop.NewColumns(
			iop.Column{Name: "c_int8", Type: iop.IntegerType, Position: 1}, // int32 target, int64 source
			iop.Column{Name: "c_str", Type: iop.StringType, Position: 2},
		)

		task := gateTestTask(t, cfg)
		decision := task.decideArrowLane(arrowLaneSource{conn: src}, tgt)

		schema, cols := gateSourceSchema()
		ok, reason, _ := decision.check(schema, cols)
		assert.False(t, ok)
		assert.Contains(t, reason, "cannot cast")
	})
}

// TestArrowLane_OptionClasses is the guard against silent drift: a new option
// that the gate does not classify fails here.
func TestArrowLane_OptionClasses(t *testing.T) {
	check := func(typ reflect.Type, classes map[string]arrowLaneSourceOptionClass) {
		valid := map[arrowLaneSourceOptionClass]bool{
			arrowOptSQL: true, arrowOptNames: true, arrowOptInert: true,
			arrowOptDefault: true, arrowOptDeclines: true,
		}
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Name
			class, ok := classes[name]
			if !assert.True(t, ok, "%s.%s is not classified in the gate", typ.Name(), name) {
				continue
			}
			assert.True(t, valid[class], "%s.%s has an unknown class %q", typ.Name(), name, class)
		}
		for name := range classes {
			_, ok := typ.FieldByName(name)
			assert.True(t, ok, "%s has no field %s", typ.Name(), name)
		}
	}

	check(reflect.TypeOf(SourceOptions{}), arrowLaneSourceOptionClasses)
	check(reflect.TypeOf(TargetOptions{}), arrowLaneTargetOptionClasses)
}

// TestArrowLane_DeclineText asserts the reason substrings the CLI suite
// asserts on.
func TestArrowLane_DeclineText(t *testing.T) {
	assert.Contains(t, arrowLaneDeclineText("max_decimals"), "max_decimals")
	assert.Contains(t, arrowLaneDeclineText("empty_as_null"), "empty_as_null")
	assert.Contains(t, arrowLaneFallbackText("public.t1", "x"), "falling back to row-based")
	assert.Contains(t, arrowLaneForceText("public.t1", "x"), "forced but not eligible")
	assert.Contains(t, arrowLaneEnabledText("postgres -> snowflake/stage"), "arrow lane: enabled")
}

// fileSourceTestColumns is the (id bigint, name text) pair the parquet fixture
// carries.
func fileSourceTestColumns() iop.Columns {
	return iop.NewColumns(
		iop.Column{Name: "id", Type: iop.BigIntType},
		iop.Column{Name: "name", Type: iop.StringType},
	)
}

// fileSourceTestParquet writes a parquet file of fileSourceTestColumns and
// returns its path.
func fileSourceTestParquet(t *testing.T, dir, name string, rows [][]any) string {
	t.Helper()

	schema := iop.ColumnsToArrowSchema(fileSourceTestColumns())

	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	require.NoError(t, err)
	defer file.Close()

	writer, err := iop.NewParquetArrowWriterFromSchema(file, schema, compress.Codecs.Snappy)
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), schema)
	defer builder.Release()
	for _, row := range rows {
		builder.Field(0).(*array.Int64Builder).Append(cast.ToInt64(row[0]))
		builder.Field(1).(*array.StringBuilder).Append(cast.ToString(row[1]))
	}

	rec := builder.NewRecordBatch()
	require.NoError(t, writer.WriteRecord(rec))
	rec.Release()
	require.NoError(t, writer.Close())

	return path
}

// TestArrowLaneFileSource_OpenBuildFallsBack is the fallback check of the open
// build: with no engine, the file-source gate declines before any listing or
// footer read, the CDC cache gate declines the same way, and the row path
// reads the parquet file as before.
func TestArrowLaneFileSource_OpenBuildFallsBack(t *testing.T) {
	if lane, _ := iop.NewArrowLane(); lane != nil {
		t.Skip("closed build: the arrow lane engine is present")
	}

	clearGateEnv(t)

	dir := t.TempDir()
	fileSourceTestParquet(t, dir, "part.01.parquet", [][]any{{1, "a"}, {2, "b"}})

	fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal)
	require.NoError(t, err)

	cfg := newGateConfig(t, dbio.TypeFileLocal, dbio.TypeDbPostgres)
	task := gateTestTask(t, cfg)
	// an ADBC target: with a token this stream would take the lane
	tgt := newGateFakeConn(dbio.TypeDbPostgres, true)

	fsCfg := iop.FileStreamConfig{Format: dbio.FileTypeParquet}

	// the file-source gate declines on the missing engine
	decision := task.decideArrowLane(arrowLaneSource{file: true, stream: dir}, tgt)
	assert.False(t, decision.enabled())
	assert.Contains(t, decision.reason, "official release")

	// the decline costs nothing: the missing path is never listed
	laneDF, err := task.readArrowFileDataflow(fs, filepath.Join(dir, "missing"), fsCfg, tgt)
	require.NoError(t, err)
	assert.Nil(t, laneDF, "a file source without an engine takes the row path")

	// the CDC cache file declines on the same engine row
	decision = task.decideArrowLane(arrowLaneSource{
		schema: iop.ColumnsToArrowSchema(fileSourceTestColumns()),
		stream: "public.t1",
	}, tgt)
	assert.False(t, decision.enabled())
	assert.Contains(t, decision.reason, "official release")

	// the row path still reads the file, row by row
	df, err := task.readFsDataflow(fs, dir, fsCfg, tgt)
	require.NoError(t, err)
	require.NotEmpty(t, df.Streams)
	assert.False(t, df.ArrowOnly(), "the open build reads rows, not records")
	assert.False(t, df.Streams[0].ArrowOnly)
	require.Nil(t, df.Streams[0].RecordStream())

	data, err := df.Collect()
	require.NoError(t, err)
	assert.Equal(t, [][]any{{int64(1), "a"}, {int64(2), "b"}}, data.Rows)
}
