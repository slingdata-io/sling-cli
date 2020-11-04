package elt

import (
	"math"
	"testing"
	"time"

	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestGetRate(t *testing.T) {
	now := time.Now()
	now2 := time.Now()
	df := iop.Dataflow{}
	task := Task{
		StartTime: &now,
		EndTime:   &now2,
		df:        &df,
	}
	rate := task.GetRate(10)

	st := *task.StartTime
	et := *task.EndTime

	h.P(et.UnixNano())
	h.P(st.UnixNano())
	h.P(df.Count())
	h.P(rate)

	h.P(et.UnixNano() - st.UnixNano())

	secElapsed := cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
	h.P(secElapsed)
	h.P(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	rate = cast.ToInt(math.Round(cast.ToFloat64(df.Count()) / secElapsed))
	h.P(rate)
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
	cfg, err := NewConfig(cfgStr)
	assert.NoError(t, err)

	println(cfg.TgtPostDbt)

}
