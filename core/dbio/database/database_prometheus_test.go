package database

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePromAPI records the ranges requested through QueryRange.
type fakePromAPI struct {
	v1.API
	mu     sync.Mutex
	ranges []v1.Range
}

func (f *fakePromAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ranges)
}

func (f *fakePromAPI) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	f.mu.Lock()
	f.ranges = append(f.ranges, r)
	f.mu.Unlock()
	sample := &model.SampleStream{
		Metric: model.Metric{"job": "test"},
		Values: []model.SamplePair{{Timestamp: model.TimeFromUnix(r.Start.Unix()), Value: 1}},
	}
	return model.Matrix{sample}, nil, nil
}

func TestPrometheusChunkedRelativeDurations(t *testing.T) {
	// each range spans 24h, so it must be split into at least 24 hourly chunks
	for _, c := range []struct{ start, end string }{
		{"now-2d", "now-1d"},
		{"now-1w", "now-6d"},
		{"now-1M", "now-30d"},
	} {
		start := c.start
		api := &fakePromAPI{}
		conn := &PrometheusConn{Client: api}
		ctx := g.NewContext(context.Background())
		conn.BaseConn.context = ctx
		query := `up#{"start":"` + start + `","end":"` + c.end + `"}`

		qctx := g.NewContext(context.Background())
		ds, err := conn.StreamRowsChunked(qctx, query, map[string]interface{}{})
		require.NoError(t, err, start)
		go ds.Collect(0) // drain so the producer keeps going

		// one chunk per hour of the requested range
		require.Eventually(t, func() bool { return api.count() >= 24 }, 20*time.Second, 50*time.Millisecond, start)
		qctx.Cancel()
	}
}

func TestParseRelativeOffset(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"-30m", -30 * time.Minute},
		{"-12h", -12 * time.Hour},
		{"-7d", -7 * 24 * time.Hour},
		{"-1w", -7 * 24 * time.Hour},
		{"-2M", -2 * 24 * 31 * time.Hour},
		{"+1d", 24 * time.Hour},
	}
	for _, c := range cases {
		got, err := parseRelativeOffset(c.in)
		assert.NoError(t, err, c.in)
		assert.Equal(t, c.want, got, c.in)
	}

	_, err := parseRelativeOffset("-abc")
	assert.Error(t, err)
}
