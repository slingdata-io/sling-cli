package elt

import (
	"math"
	"testing"
	"time"

	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
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
