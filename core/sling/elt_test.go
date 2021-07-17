package sling_test

import (
	"github.com/slingdata-io/sling/core/sling"
	"math"
	"testing"
	"time"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestGetRate(t *testing.T) {
	now := time.Now()
	now2 := time.Now()
	df := iop.Dataflow{}
	task := sling.Task{
		StartTime: &now,
		EndTime:   &now2,
		//df:        &df,
	}
	rate := task.GetRate(10)

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
	rate = cast.ToInt(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
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
	_, err := sling.NewConfig(cfgStr)
	assert.NoError(t, err)

}
