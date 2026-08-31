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
