package iop

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startFakeStreamConsumer simulates the part of ds.Start()'s row loop
// this test cares about: it consumes ds.pauseChan / ds.unpauseChan
// exactly like the real loop. rowDelay simulates how long it takes the
// stream to enter its row loop after being attached to the dataflow
// (e.g. HTTP RTT + decode time for a slow API source).
func startFakeStreamConsumer(t *testing.T, ds *Datastream, rowDelay time.Duration, stop <-chan struct{}) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-time.After(rowDelay):
		case <-stop:
			return
		}
		for {
			select {
			case <-stop:
				return
			case <-ds.pauseChan:
				select {
				case <-ds.unpauseChan:
				case <-stop:
					return
				}
			}
		}
	}()
	return done
}

// TestPauseSurvivesSlowToStartRowLoop reproduces the race that broke
// the API source: a datastream is attached to the dataflow (df.Ready
// == true) but its row loop hasn't begun yet because the HTTP response
// is still in flight. The pre-fix Pause() gave up after a random 1-4s
// window; the fix extends the budget to PauseTimeout() (default 60s).
//
// Simulated row-loop start delay is 5s, well past the old 1-4s race
// window. With the old code this test would fail; with the fix it
// passes.
func TestPauseSurvivesSlowToStartRowLoop(t *testing.T) {
	df := NewDataflow()
	cols := NewColumnsFromFields("id", "name")

	ds := NewDatastreamContext(context.Background(), cols)
	ds.Ready = true

	df.Columns = cols
	df.Streams = append(df.Streams, ds)
	df.Ready = true

	stop := make(chan struct{})
	defer close(stop)
	startFakeStreamConsumer(t, ds, 5*time.Second, stop)

	start := time.Now()
	paused := df.Pause()
	elapsed := time.Since(start)

	assert.True(t, paused, "Pause() should succeed even when row loop starts late")
	assert.GreaterOrEqual(t, elapsed, 5*time.Second,
		"Pause() should keep retrying until the row loop comes up")

	df.Unpause()
}

// TestPauseTimesOutWhenStreamNeverStarts asserts that if the row loop
// never begins, Pause() returns false within PauseTimeout() — false,
// not silently true — so callers can surface a real error. We shrink
// the timeout via SLING_PAUSE_TIMEOUT to keep the test fast.
func TestPauseTimesOutWhenStreamNeverStarts(t *testing.T) {
	t.Setenv("SLING_PAUSE_TIMEOUT", "1")

	df := NewDataflow()
	cols := NewColumnsFromFields("id")
	ds := NewDatastreamContext(context.Background(), cols)
	ds.Ready = true
	df.Columns = cols
	df.Streams = append(df.Streams, ds)
	df.Ready = true

	start := time.Now()
	paused := df.Pause()
	elapsed := time.Since(start)

	assert.False(t, paused, "Pause() must return false when no row loop ever starts")
	assert.GreaterOrEqual(t, elapsed, 1*time.Second,
		"Pause() should wait the configured timeout before giving up")
	assert.Less(t, elapsed, 3*time.Second,
		"Pause() should not wait significantly longer than the timeout")
}

// TestPauseRespectsDataflowContextCancel ensures Pause() returns
// promptly when the dataflow context is canceled, rather than waiting
// out PauseTimeout(). This matters for shutdown paths.
func TestPauseRespectsDataflowContextCancel(t *testing.T) {
	t.Setenv("SLING_PAUSE_TIMEOUT", "30")

	ctx, cancel := context.WithCancel(context.Background())
	df := NewDataflowContext(ctx)
	cols := NewColumnsFromFields("id")
	ds := NewDatastreamContext(ctx, cols)
	ds.Ready = true
	df.Columns = cols
	df.Streams = append(df.Streams, ds)
	df.Ready = true

	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	paused := df.Pause()
	elapsed := time.Since(start)

	assert.False(t, paused, "Pause() must return false when context is canceled")
	assert.Less(t, elapsed, 2*time.Second,
		"Pause() should return promptly on context cancel, not wait for the full timeout")
}

// TestPauseSucceedsWhenStreamAlreadyRunning is the happy path: the
// stream is already in its row loop when Pause() is called. Should
// return true essentially immediately. This is the file-source case
// (rows start flowing as soon as the consumer is wired up).
func TestPauseSucceedsWhenStreamAlreadyRunning(t *testing.T) {
	df := NewDataflow()
	cols := NewColumnsFromFields("id")
	ds := NewDatastreamContext(context.Background(), cols)
	ds.Ready = true
	df.Columns = cols
	df.Streams = append(df.Streams, ds)
	df.Ready = true

	stop := make(chan struct{})
	defer close(stop)
	startFakeStreamConsumer(t, ds, 0, stop) // already pumping

	// give the consumer goroutine a moment to enter the select
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	paused := df.Pause()
	elapsed := time.Since(start)

	assert.True(t, paused, "Pause() should succeed when row loop is already running")
	assert.Less(t, elapsed, 500*time.Millisecond,
		"Pause() should be near-instant for an already-running stream")

	df.Unpause()
}

// TestPauseWhileStreamCloses pauses and unpauses the dataflow while the
// stream loop ends and closes the stream.
func TestPauseWhileStreamCloses(t *testing.T) {
	ds := intRowsDatastream(t, 3000)
	df, err := MakeDataFlow(ds)
	require.NoError(t, err)

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range ds.Rows() {
		}
	}()

	paused := false
	for !ds.IsClosed() {
		if df.Pause() {
			paused = true
		}
		df.Unpause()
	}
	<-drained

	assert.True(t, paused)
	assert.True(t, ds.IsClosed())
	require.NoError(t, ds.Err())
}

// TestDataflowStatsSnapshotStreamsAppend reads the dataflow stats while
// streams are appended and writers add egress bytes. The test appends under
// df.mux: PushStreamChan calls ds.AddColumns, which races with the stream
// and is out of scope.
func TestDataflowStatsSnapshotStreamsAppend(t *testing.T) {
	enableStatus(t)
	const nStreams, perStream = 3, 2000
	cols := NewColumns(Column{Name: "id", Type: BigIntType})

	df := NewDataflow()
	df.SetReady()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = df.StatsSnapshot()
				_ = df.Count()
				_ = df.DsTotalBytes()
			}
		}
	}()
	for range 2 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 1000 {
				df.AddEgressBytes(1)
			}
		}()
	}

	for range nStreams {
		i := 0
		ds := NewDatastreamIt(context.Background(), cols.Clone(), func(it *Iterator) bool {
			if i >= perStream {
				return false
			}
			i++
			it.Row = []any{int64(i)}
			return true
		})
		require.NoError(t, ds.Start())

		df.mux.Lock()
		ds.setDf(df)
		df.Streams = append(df.Streams, ds)
		df.mux.Unlock()

		data, err := ds.Collect(0)
		require.NoError(t, err)
		assert.Len(t, data.Rows, perStream)
	}

	close(stop)
	readers.Wait()

	snaps := df.StatsSnapshot()
	require.Len(t, snaps, nStreams)
	for _, snap := range snaps {
		assert.Equal(t, uint64(perStream), snap.Count)
		require.Len(t, snap.Columns, 1)
		assert.Equal(t, int64(perStream), snap.Columns[0].Stats.TotalCnt)
	}
	_, egress := df.Bytes()
	assert.Equal(t, uint64(2000), egress)
}
