package sling

import (
	"math"
	"testing"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
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

func TestConfig(t *testing.T) {

	cfgStr := `{
		"post_dbt": {
			"conn": "ORACLE_SLING",
			"expr": "my_first_dbt_model",
			"name": "DBT_PROJ_1",
			"folder": "/",
			"version": "0.18.0",
			"repo_url": "https://github.com/fishtown-analytics/dbt-starter-project"
		},
		"tgt_conn": "ORACLE_SLING"
	}`
	_, err := NewConfig(cfgStr)
	assert.NoError(t, err)

}

func TestColumnCasing(t *testing.T) {
	df := iop.NewDataflow(0)

	sourceCasing := SourceColumnCasing
	snakeCasing := SnakeColumnCasing
	targetCasing := TargetColumnCasing

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasing(df, dbio.TypeDbSnowflake, &sourceCasing)
	assert.Equal(t, "myCol", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"}, iop.Column{Name: "hey-hey"})
	applyColumnCasing(df, dbio.TypeDbSnowflake, &snakeCasing)
	assert.Equal(t, "MY_COL", df.Columns[0].Name)
	assert.Equal(t, "HEY_HEY", df.Columns[1].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "myCol"})
	applyColumnCasing(df, dbio.TypeDbSnowflake, &targetCasing)
	assert.Equal(t, "MYCOL", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasing(df, dbio.TypeDbDuckDb, &targetCasing)
	assert.Equal(t, "dhl_originaltracking_number", df.Columns[0].Name)

	df.Columns = iop.NewColumns(iop.Column{Name: "DHL OriginalTracking-Number"})
	applyColumnCasing(df, dbio.TypeDbDuckDb, &snakeCasing)
	assert.Equal(t, "dhl_original_tracking_number", df.Columns[0].Name)
}
