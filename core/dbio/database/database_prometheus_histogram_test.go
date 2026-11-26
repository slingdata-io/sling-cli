package database

import (
	"context"
	"testing"
	"time"

	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

type histogramPromAPI struct {
	v1.API
}

func (histogramPromAPI) QueryRange(_ context.Context, _ string, r v1.Range, _ ...v1.Option) (model.Value, v1.Warnings, error) {
	return model.Matrix{&model.SampleStream{
		Histograms: []model.SampleHistogramPair{{
			Timestamp: model.TimeFromUnixNano(r.Start.UnixNano()),
			Histogram: &model.SampleHistogram{
				Count: 3,
				Sum:   5,
				Buckets: model.HistogramBuckets{
					{Boundaries: 3, Lower: -1, Upper: 1, Count: 1},
					{Boundaries: 0, Lower: 1, Upper: 3, Count: 2},
				},
			},
		}},
	}}, nil, nil
}

func TestPromChunkReaderHistogramBoundaries(t *testing.T) {
	start := time.Unix(100, 0)
	conn := &PrometheusConn{Client: histogramPromAPI{}}
	reader := newPromChunkReader(context.Background(), conn, "test_histogram", start, start, time.Minute)

	rows, err := reader.Read()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	index, ok := reader.columns.FieldMap(true)["bucket_boundaries"]
	require.True(t, ok)
	require.Equal(t, []any{3, 0}, []any{rows[0][index], rows[1][index]})
}
