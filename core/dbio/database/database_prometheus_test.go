package database

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakePromAPI records the ranges of QueryRange. Like Prometheus, it returns
// one point at each Start + k*Step, up to and including End.
type fakePromAPI struct {
	v1.API
	empty  bool
	mu     sync.Mutex
	ranges []v1.Range
}

func newFakePromConn(api *fakePromAPI) *PrometheusConn {
	conn := &PrometheusConn{Client: api}
	conn.BaseConn.context = g.NewContext(context.Background())
	return conn
}

func (f *fakePromAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.ranges)
}

// times returns the evaluation times of all recorded ranges, in order.
func (f *fakePromAPI) times() (times []time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.ranges {
		for ts := r.Start; !ts.After(r.End); ts = ts.Add(r.Step) {
			times = append(times, ts)
		}
	}
	return
}

func (f *fakePromAPI) QueryRange(ctx context.Context, query string, r v1.Range, opts ...v1.Option) (model.Value, v1.Warnings, error) {
	f.mu.Lock()
	f.ranges = append(f.ranges, r)
	f.mu.Unlock()
	if f.empty {
		return model.Matrix{}, nil, nil
	}
	sample := &model.SampleStream{Metric: model.Metric{"job": "test"}}
	for ts := r.Start; !ts.After(r.End); ts = ts.Add(r.Step) {
		sample.Values = append(sample.Values, model.SamplePair{Timestamp: model.TimeFromUnixNano(ts.UnixNano()), Value: 1})
	}
	return model.Matrix{sample}, nil, nil
}

// collect reads the whole datastream, and fails the test if it does not finish.
// It does not use ds.Collect, which writes into ds.Columns while the stream runs.
func collect(t *testing.T, ds *iop.Datastream) iop.Dataset {
	t.Helper()
	done := make(chan iop.Dataset, 1)
	go func() {
		assert.NoError(t, ds.WaitReady())
		data := iop.Dataset{Columns: ds.Columns}
		for row := range ds.Rows() {
			data.Rows = append(data.Rows, row)
		}
		assert.NoError(t, ds.Err())
		done <- data
	}()
	select {
	case data := <-done:
		return data
	case <-time.After(20 * time.Second):
		t.Fatal("datastream did not finish")
		return iop.Dataset{}
	}
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
		conn := newFakePromConn(api)
		query := `up#{"start":"` + start + `","end":"` + c.end + `"}`

		qctx := g.NewContext(context.Background())
		ds, err := conn.StreamRowsChunked(qctx, query, map[string]interface{}{})
		require.NoError(t, err, start)
		go func() { // drain so the producer keeps going
			for range ds.Rows() {
			}
		}()

		require.Eventually(t, func() bool { return api.count() >= 24 }, 20*time.Second, 50*time.Millisecond, start)
		qctx.Cancel()
	}
}

func TestPromChunkReaderGrid(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(70*24*time.Hour + 5*time.Minute)

	for _, c := range []struct {
		step, size time.Duration
	}{
		{15 * time.Second, time.Hour},
		{time.Minute, time.Hour},
		{7 * time.Minute, 7 * time.Hour},
		{time.Hour, 60 * time.Hour},
		{24 * time.Hour, 60 * 24 * time.Hour},
	} {
		api := &fakePromAPI{}
		reader := newPromChunkReader(context.Background(), newFakePromConn(api), "up", start, end, c.step)
		reader.delay = 0
		assert.Equal(t, c.size, reader.size, c.step)

		rows := 0
		for !reader.Done() {
			chunkRows, err := reader.Read()
			require.NoError(t, err, c.step)
			rows += len(chunkRows)
		}

		// the chunks must give the same points as one full range query, with no repeats
		var want []time.Time
		for ts := start; !ts.After(end); ts = ts.Add(c.step) {
			want = append(want, ts)
		}
		assert.Equal(t, want, api.times(), c.step)
		assert.Equal(t, len(want), rows, c.step)
	}
}

func TestPrometheusChunkedStepDays(t *testing.T) {
	api := &fakePromAPI{}
	conn := newFakePromConn(api)
	query := `up#{"start":"now-2M","end":"now","step":"1d"}`

	ds, err := conn.StreamRowsChunked(g.NewContext(context.Background()), query, map[string]interface{}{})
	require.NoError(t, err)
	data := collect(t, ds)

	// 2M is 62 days: 63 daily points in 2 chunks of at most 60 days
	assert.Equal(t, 2, api.count())
	assert.Len(t, data.Rows, 63)

	seen := map[any]bool{}
	tsIdx := data.Columns.GetColumn("timestamp").Position - 1
	for _, row := range data.Rows {
		assert.False(t, seen[row[tsIdx]], "duplicate timestamp %v", row[tsIdx])
		seen[row[tsIdx]] = true
	}
}

func TestPrometheusChunkedEmpty(t *testing.T) {
	api := &fakePromAPI{empty: true}
	conn := newFakePromConn(api)
	opts := map[string]interface{}{"columns": iop.NewColumnsFromFields("job", "timestamp", "value")}

	ds, err := conn.StreamRowsChunked(g.NewContext(context.Background()), `up#{"start":"now-3h"}`, opts)
	require.NoError(t, err)
	data := collect(t, ds)

	assert.Equal(t, 4, api.count()) // 3 hourly chunks, plus the point at end
	assert.Empty(t, data.Rows)
	assert.Equal(t, []string{"job", "timestamp", "value"}, data.Columns.Names())
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
