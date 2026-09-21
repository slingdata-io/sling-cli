package sling

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestGetRate(t *testing.T) {
	now := time.Now()
	now2 := time.Now()
	df := iop.Dataflow{}
	task := TaskExecution{
		StartTime: &now,
		EndTime:   &now2,
		//df:        &df,
	}
	rate, _ := task.GetRate(10)

	st := *task.StartTime
	et := *task.EndTime

	g.P(et.UnixNano())
	g.P(st.UnixNano())
	g.P(df.Count())
	g.P(rate)

	g.P(et.UnixNano() - st.UnixNano())

	secElapsed := cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
	g.P(secElapsed)
	g.P(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	rate = cast.ToInt64(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	g.P(rate)
}

func TestColumnCasing(t *testing.T) {
	df := iop.NewDataflow(0)

	normalizeCasing := iop.NormalizeColumnCasing
	sourceCasing := iop.SourceColumnCasing
	snakeCasing := iop.SnakeColumnCasing
	targetCasing := iop.TargetColumnCasing

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &sourceCasing)
	assert.Equal(t, "myCol", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"}, iop.Column{Name: "hey-hey"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &snakeCasing)
	assert.Equal(t, "MY_COL", df.Columns[0].Name)
	assert.Equal(t, "HEY_HEY", df.Columns[1].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"}, iop.Column{Name: "hey-hey"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &normalizeCasing)
	assert.Equal(t, "myCol", df.Columns[0].Name)
	assert.Equal(t, "hey-hey", df.Columns[1].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &targetCasing)
	assert.Equal(t, "MYCOL", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &targetCasing)
	assert.Equal(t, "dhl_originaltracking_number", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &snakeCasing)
	assert.Equal(t, "dhl_original_tracking_number", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &normalizeCasing)
	assert.Equal(t, "DHL OriginalTracking-Number", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "HAPPY"})
	applyColumnCasingToDf(df, dbio.TypeDbDuckDb, &normalizeCasing)
	assert.Equal(t, "happy", df.Columns[0].Name)
	applyColumnCasingToDf(df, dbio.TypeDbSnowflake, &normalizeCasing)
	assert.Equal(t, "HAPPY", df.Columns[0].Name)
}

func TestGetFormatMapAPISourceStreamTable(t *testing.T) {
	cfg := &Config{
		Source:     Source{Conn: "MOCK_API", Stream: "users"},
		Target:     Target{Conn: "DUCKDB", Object: "main.{stream_table}"},
		StreamName: "users",
	}
	cfg.SrcConn.Type = dbio.TypeApi
	cfg.TgtConn.Type = dbio.TypeDbDuckDb
	cfg.initEvaluator()

	m, err := cfg.GetFormatMap()
	if err != nil {
		t.Fatal(err)
	}
	if got := cast.ToString(m["stream_table"]); got != "users" {
		t.Fatalf("stream_table=%q want users", got)
	}
	if got := cast.ToString(m["stream_name"]); got != "users" {
		t.Fatalf("stream_name=%q want users", got)
	}

	cfg.Target.Object = "main.{stream_table}"
	cfg.Target.Options = &TargetOptions{}
	if err := cfg.FormatTargetObjectName(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(cfg.Target.Object, "{stream_table}") {
		t.Fatalf("object still has placeholder: %s", cfg.Target.Object)
	}
	if cfg.Target.Options.TableTmp == "" {
		t.Fatal("expected duckdb temp table")
	}
	if strings.Contains(cfg.Target.Options.TableTmp, "{stream_table}") {
		t.Fatalf("temp table still has placeholder: %s", cfg.Target.Options.TableTmp)
	}
	if !strings.Contains(strings.ToLower(cfg.Target.Options.TableTmp), "users") {
		t.Fatalf("temp table should include endpoint name, got %s", cfg.Target.Options.TableTmp)
	}
}

func TestChangeTrackingConfig(t *testing.T) {
	// 1. Boolean true
	cfg1 := &Config{
		Mode: IncrementalMode,
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
			Options: &SourceOptions{
				ChangeTracking: true,
			},
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfg1.SrcConn.Type = dbio.TypeDbSQLServer
	cfg1.TgtConn.Type = dbio.TypeDbPostgres
	assert.True(t, cfg1.Source.IsChangeTracking())
	assert.True(t, cfg1.IsChangeTracking())
	assert.False(t, cfg1.Source.AutoFullRefresh())
	assert.False(t, cfg1.AutoFullRefresh())
	// DetermineType should pass without update_key when ChangeTracking is true
	_, err1 := cfg1.DetermineType()
	assert.NoError(t, err1)

	// 2. Map with auto_full_refresh
	cfg2 := &Config{
		Mode: IncrementalMode,
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
			Options: &SourceOptions{
				ChangeTracking: map[string]any{"auto_full_refresh": true},
			},
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfg2.SrcConn.Type = dbio.TypeDbSQLServer
	cfg2.TgtConn.Type = dbio.TypeDbPostgres
	assert.True(t, cfg2.Source.IsChangeTracking())
	assert.True(t, cfg2.IsChangeTracking())
	assert.True(t, cfg2.Source.AutoFullRefresh())
	assert.True(t, cfg2.AutoFullRefresh())
	_, err2 := cfg2.DetermineType()
	assert.NoError(t, err2)

	// 3. Direct auto_full_refresh field in source_options
	cfg3 := &Config{
		Mode: IncrementalMode,
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
			Options: &SourceOptions{
				ChangeTracking:  true,
				AutoFullRefresh: true,
			},
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfg3.SrcConn.Type = dbio.TypeDbSQLServer
	cfg3.TgtConn.Type = dbio.TypeDbPostgres
	assert.True(t, cfg3.Source.IsChangeTracking())
	assert.True(t, cfg3.AutoFullRefresh())

	// 4. Incremental without ChangeTracking and without update_key should fail validation
	cfg4 := &Config{
		Mode: IncrementalMode,
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfg4.SrcConn.Type = dbio.TypeDbSQLServer
	cfg4.TgtConn.Type = dbio.TypeDbPostgres
	assert.False(t, cfg4.Source.IsChangeTracking())
	assert.False(t, cfg4.IsChangeTracking())
	_, err4 := cfg4.DetermineType()
	assert.Error(t, err4)

	// 5. Non-SQL Server with ChangeTracking should fail validation in DetermineType
	cfgNonSQLServer := &Config{
		Mode: IncrementalMode,
		Source: Source{
			Conn:   "PG",
			Stream: "public.orders",
			Options: &SourceOptions{
				ChangeTracking: true,
			},
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfgNonSQLServer.SrcConn.Type = dbio.TypeDbPostgres
	cfgNonSQLServer.TgtConn.Type = dbio.TypeDbPostgres
	_, errNonSQLServer := cfgNonSQLServer.DetermineType()
	assert.Error(t, errNonSQLServer)
	assert.Contains(t, errNonSQLServer.Error(), "change tracking is only supported for SQL Server sources")

	// 6. Map with enabled: false should not be change tracking
	optsMapDisabled := &SourceOptions{
		ChangeTracking: map[string]any{"enabled": false},
	}
	assert.False(t, optsMapDisabled.IsChangeTracking())

	optsStringFalse := &SourceOptions{
		ChangeTracking: "false",
	}
	assert.False(t, optsStringFalse.IsChangeTracking())

	// 7. Non-incremental mode with change tracking should fail
	cfgWrongMode := &Config{
		Mode: FullRefreshMode,
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
			Options: &SourceOptions{
				ChangeTracking: true,
			},
		},
	}
	cfgWrongMode.SrcConn.Type = dbio.TypeDbSQLServer
	_, errWrongMode := cfgWrongMode.DetermineType()
	assert.Error(t, errWrongMode)
	assert.Contains(t, errWrongMode.Error(), "change tracking is only supported with mode 'incremental'")

	// 8. Empty mode with change tracking defaults to incremental
	cfgEmptyMode := &Config{
		Source: Source{
			Conn:   "MSSQL",
			Stream: "dbo.orders",
			Options: &SourceOptions{
				ChangeTracking: true,
			},
		},
		Target: Target{
			Conn:   "POSTGRES",
			Object: "public.orders",
		},
	}
	cfgEmptyMode.SrcConn.Type = dbio.TypeDbSQLServer
	cfgEmptyMode.TgtConn.Type = dbio.TypeDbPostgres
	jobType, errEmptyMode := cfgEmptyMode.DetermineType()
	assert.NoError(t, errEmptyMode)
	assert.Equal(t, IncrementalMode, cfgEmptyMode.Mode)
	assert.Equal(t, DbToDb, jobType)

	// 9. SetDefaults propagation
	tgtOpts := &SourceOptions{}
	srcOpts := &SourceOptions{
		ChangeTracking:  true,
		AutoFullRefresh: true,
	}
	tgtOpts.SetDefaults(*srcOpts)
	assert.Equal(t, true, tgtOpts.ChangeTracking)
	assert.Equal(t, true, tgtOpts.AutoFullRefresh)
}
