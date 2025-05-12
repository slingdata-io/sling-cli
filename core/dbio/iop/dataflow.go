package iop

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// Dataflow is a collection of concurrent Datastreams
type Dataflow struct {
	Columns         Columns
	Buffer          [][]interface{}
	StreamCh        chan *Datastream
	Streams         []*Datastream
	Context         *g.Context
	Limit           uint64
	EgressBytes     uint64
	deferFuncs      []func()
	Ready           bool
	Inferred        bool
	FsURL           string
	OnColumnChanged func(col Column) error
	OnColumnAdded   func(col Column) error
	readyChn        chan struct{}
	StreamMap       map[string]*Datastream
	closed          bool
	mux             sync.Mutex
	SchemaVersion   int // for column type version
}

// NewDataflow creates a new dataflow
func NewDataflow(limit ...int) (df *Dataflow) {
	return NewDataflowContext(context.Background(), limit...)
}

func NewDataflowContext(ctx context.Context, limit ...int) (df *Dataflow) {

	Limit := uint64(0) // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = cast.ToUint64(limit[0])
	}

	df = &Dataflow{
		StreamCh:      make(chan *Datastream, 1),
		Streams:       []*Datastream{},
		Context:       g.NewContext(ctx),
		Limit:         Limit,
		StreamMap:     map[string]*Datastream{},
		deferFuncs:    []func(){},
		readyChn:      make(chan struct{}),
		OnColumnAdded: func(col Column) error { return nil },
	}

	// df.OnColumnAdded = func(col Column) (err error) {
	// 	eG := g.ErrorGroup{}
	// 	for _, ds := range df.Streams {
	// 		eG.Capture(ds.OnColumnAdded(col))
	// 	}
	// 	return eG.Err()
	// }

	return
}

// Err return the error if any
func (df *Dataflow) Err() (err error) {
	eG := g.ErrorGroup{}
	for _, ds := range df.Streams {
		if ds.Err() != nil {
			eG.Add(ds.Context.ErrGroup.Errors...)
		}
	}

	if err = df.Context.Err(); err != nil {
		eG.Add(df.Context.ErrGroup.Errors...)
	}

	return eG.Err()
}

// IsClosed is true is ds is closed
func (df *Dataflow) IsClosed() bool {
	return df.closed
}

// CleanUp refers the defer functions
func (df *Dataflow) CleanUp() {
	g.Trace("executing defer functions")
	df.mux.Lock()
	defer df.mux.Unlock()
	for i, f := range df.deferFuncs {
		f()
		df.deferFuncs[i] = func() {} // in case it gets called again
	}
}

// StreamConfig get the first Sp config
func (df *Dataflow) StreamConfig() (cfg StreamConfig) {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		return ds.config
	}
	return DefaultStreamConfig()
}

// SetConfig set the Sp config
func (df *Dataflow) SetConfig(cfg StreamConfig) {
	// don't overwrite transforms if not provided
	if cfg.transforms == nil {
		cfg.transforms = df.StreamConfig().transforms
	}

	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		ds.Sp.Config = cfg
	}
}

// SetBatchLimit set the ds.Batch.Limit
func (df *Dataflow) SetBatchLimit(limit int64) {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		ds.Sp.Config.BatchLimit = limit
		if ds.CurrentBatch != nil {
			ds.CurrentBatch.Limit = limit
		}
	}
}

// Defer runs a given function as close of Dataflow
func (df *Dataflow) Defer(f func()) {
	df.mux.Lock()
	defer df.mux.Unlock()
	if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
		df.deferFuncs = append(df.deferFuncs, f)
	}
}

// Close closes the df
func (df *Dataflow) Close() {
	if !df.closed {
		close(df.StreamCh)
	}
	df.closed = true
}

// BufferDataset return the buffer as a dataset
func (df *Dataflow) BufferDataset() Dataset {
	data := NewDataset(df.Columns)
	data.Rows = df.Buffer
	data.Inferred = df.Inferred
	return data
}

// Pause pauses all streams
func (df *Dataflow) Pause(exceptDs ...string) bool {
	if df.Ready {

		timer := time.NewTimer(time.Duration(g.RandInt(3000)+1000) * time.Millisecond)
		for {
			df.mux.Lock()
			// try to pause all datastreams, or none
			pauseMap := map[string]bool{}
			for _, ds := range df.Streams {
				if !lo.Contains(exceptDs, ds.ID) && !ds.closed {
					pauseMap[ds.ID] = ds.TryPause()
				}
			}

			pauseSlice := lo.Values(pauseMap)
			if len(lo.Filter(pauseSlice, func(v bool, i int) bool { return v })) == len(pauseSlice) {
				df.mux.Unlock()
				break // only exit if all datastreams are paused
			} else if len(pauseSlice) == 0 {
				df.mux.Unlock()
				break
			}

			// unpause paused since could not do distributed pause, and wait a bit
			for _, ds := range df.Streams {
				if paused, ok := pauseMap[ds.ID]; ok && paused {
					ds.Unpause()
				}
			}
			df.mux.Unlock()
			time.Sleep(time.Duration(g.RandInt(100)) * time.Millisecond)

			select {
			case <-timer.C:
				return false
			default:
			}
		}
	}

	return true
}

// Unpause unpauses all streams
func (df *Dataflow) Unpause(exceptDs ...string) {
	df.mux.Lock()
	defer df.mux.Unlock()

	if df.Ready {
		for _, ds := range df.Streams {
			if !lo.Contains(exceptDs, ds.ID) {
				ds.Unpause()
			}
		}
	}
}

// SetReady sets the df.ready
func (df *Dataflow) SetReady() {
	if !df.Ready {
		df.mux.Lock()
		defer df.mux.Unlock()

		// set inferred
		df.Inferred = true
		for _, ds := range df.Streams {
			if !ds.Inferred {
				df.Inferred = false
			}
		}

		df.Ready = true
		go func() { df.readyChn <- struct{}{} }()
	}
}

// SetEmpty sets all underlying datastreams empty
func (df *Dataflow) SetEmpty() {
	for _, ds := range df.Streams {
		ds.SetEmpty()
	}
}

// IsEmpty returns true is ds.Rows of all channels as empty
func (df *Dataflow) IsEmpty() bool {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		if ds != nil && ds.Ready {
			if !ds.empty {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// SetColumns sets the columns
func (df *Dataflow) MergeColumns(columns []Column, inferred bool) (processOk bool) {
	df.mux.Lock()
	mergedCols, colsAdded, colsChanged := df.Columns.Merge(columns, false)

	df.Columns = mergedCols
	df.Inferred = inferred
	df.mux.Unlock()

	if len(colsAdded.AddedCols) > 0 {
		_, ok := df.AddColumns(colsAdded.AddedCols, false)
		if !ok {
			return false
		}
	}

	for _, changed := range colsChanged {
		if !df.ChangeColumn(changed.ChangedIndex, changed.ChangedType) {
			return false
		}
	}
	return true
}

// SetColumns sets the columns
func (df *Dataflow) AddColumns(newCols Columns, overwrite bool, exceptDs ...string) (added Columns, processOk bool) {
	df.mux.Lock()
	mergedCols, colsAdded, _ := df.Columns.Merge(newCols, overwrite)

	df.Columns = mergedCols
	added = colsAdded.AddedCols
	df.mux.Unlock()

	if len(added) > 0 {
		if !df.Pause(exceptDs...) {
			return added, false
		}

		// lock for operation
		df.Context.Lock()

		// wait for current batches to close
		df.CloseCurrentBatches()

		for _, addedCol := range added {
			if err := df.OnColumnAdded(addedCol); err != nil {
				df.Context.CaptureErr(err)
			} else {
				df.incrementVersion()
			}
		}

		df.Context.Unlock()

		df.Unpause(exceptDs...)
	}
	return added, true
}

// SetColumns sets the columns
func (df *Dataflow) ChangeColumn(i int, newType ColumnType, exceptDs ...string) bool {
	if df.OnColumnChanged == nil {
		g.DebugLow("df.OnColumnChanged is not defined")
		return false
	}

	if !df.Pause(exceptDs...) {
		return false
	}

	// lock for operation
	df.Context.Lock()

	// wait for current batches to close
	df.CloseCurrentBatches()

	setChangedType(&df.Columns[i], newType)
	if err := df.OnColumnChanged(df.Columns[i]); err != nil {
		df.Context.CaptureErr(err)
	} else {
		df.incrementVersion()
	}

	df.Context.Unlock()

	df.Unpause(exceptDs...)

	return true
}

func (df *Dataflow) incrementVersion() {
	df.mux.Lock()
	defer df.mux.Unlock()

	df.SchemaVersion++ // increment version
	for _, ds0 := range df.Streams {
		if len(ds0.Columns) == len(df.Columns) {
			for i := range df.Columns {
				ds0.Columns[i].Type = df.Columns[i].Type
			}
		}
	}
}

func (df *Dataflow) CloseCurrentBatches() {

	for _, ds := range df.Streams {
		if batch := ds.LatestBatch(); batch != nil {
			batch.Close()
		}
	}
}

// MakeStreamCh determines whether to merge all the streams into one
// or keep them separate. If data is small per stream, it's best to merge
// For example, Bigquery has limits on number of operations can be called within a time limit
func (df *Dataflow) MakeStreamCh(forceMerge bool) (streamCh chan *Datastream) {
	streamCh = make(chan *Datastream, df.Context.Wg.Limit)
	totalBufferRows := 0
	totalCnt := 0
	minBufferRows := SampleSize
	df.mux.Lock()
	for _, ds := range df.Streams {
		if ds.Ready && len(ds.Buffer) < minBufferRows {
			minBufferRows = len(ds.Buffer)
			totalBufferRows = totalBufferRows + len(ds.Buffer)
			totalCnt++
		}
	}
	df.mux.Unlock()

	avgBufferRows := cast.ToFloat64(totalBufferRows) / cast.ToFloat64(totalCnt)

	go func() {
		defer close(streamCh)

		// buffer should be at least 90% full on average, 80% full at minimum
		if forceMerge || avgBufferRows < 0.9*cast.ToFloat64(SampleSize) || cast.ToFloat64(minBufferRows) < 0.8*cast.ToFloat64(SampleSize) {
			streamCh <- MergeDataflow(df)
		} else {
			for ds := range df.StreamCh {
				streamCh <- ds
			}
		}
	}()

	return
}

// SyncColumns a workaround to synch the ds.Columns to the df.Columns
func (df *Dataflow) SyncColumns() {
	df.mux.Lock()
	defer df.mux.Unlock()
	for _, ds := range df.Streams {
		colMap := df.Columns.FieldMap(true)
		for i, col := range ds.Columns {
			maxLen := col.Stats.MaxLen       // old max length
			minLen := col.Stats.MinLen       // old min length
			maxDecLen := col.Stats.MaxDecLen // old max dec length

			// sync stats
			if cs, ok := ds.Sp.colStats[i]; ok {
				ds.Columns[i].Stats = *cs
			}

			// keep old values
			if maxLen > ds.Columns[i].Stats.MaxLen {
				ds.Columns[i].Stats.MaxLen = maxLen
			}
			if minLen > 0 && ds.Columns[i].Stats.MinLen == 0 {
				ds.Columns[i].Stats.MinLen = minLen
			}
			if maxDecLen > ds.Columns[i].Stats.MaxDecLen {
				ds.Columns[i].Stats.MaxDecLen = maxDecLen
			}

			colName := strings.ToLower(col.Name)
			if _, ok := colMap[colName]; !ok {
				col.Position = len(df.Columns)
				df.Columns = append(df.Columns, col)
			}
		}
	}
}

// SyncStats sync stream processor stats aggregated to the df.Columns
func (df *Dataflow) SyncStats() {

	df.mux.Lock()
	defer df.mux.Unlock()

	dfColMap := df.Columns.FieldMap(true)

	// for some reason, df.Columns remains the same as the first ds.Columns
	// need to recreate them, reassign from dfCols
	dfCols := df.Columns.Clone()
	for i, col := range dfCols {
		dfCols[i].Stats = ColumnStats{MaxLen: col.Stats.MaxLen} // keep manual column length spec
	}

	for _, ds := range df.Streams {
		for j, col := range ds.Columns {
			i, ok := dfColMap[strings.ToLower(col.Name)]
			if !ok {
				g.DebugLow("Warning: column '%s' not found in df.SyncStats", col.Name)
				continue
			}

			colStats, ok := ds.Sp.colStats[j]
			if !ok {
				continue
			}

			dfCols[i].Stats.TotalCnt = dfCols[i].Stats.TotalCnt + colStats.TotalCnt
			dfCols[i].Stats.NullCnt = dfCols[i].Stats.NullCnt + colStats.NullCnt
			dfCols[i].Stats.StringCnt = dfCols[i].Stats.StringCnt + colStats.StringCnt
			dfCols[i].Stats.JsonCnt = dfCols[i].Stats.JsonCnt + colStats.JsonCnt
			dfCols[i].Stats.IntCnt = dfCols[i].Stats.IntCnt + colStats.IntCnt
			dfCols[i].Stats.DecCnt = dfCols[i].Stats.DecCnt + colStats.DecCnt
			dfCols[i].Stats.BoolCnt = dfCols[i].Stats.BoolCnt + colStats.BoolCnt
			dfCols[i].Stats.DateCnt = dfCols[i].Stats.DateCnt + colStats.DateCnt
			dfCols[i].Stats.DateTimeCnt = dfCols[i].Stats.DateTimeCnt + colStats.DateTimeCnt
			dfCols[i].Stats.DateTimeZCnt = dfCols[i].Stats.DateTimeZCnt + colStats.DateTimeZCnt
			dfCols[i].Stats.Checksum = dfCols[i].Stats.Checksum + colStats.Checksum

			if colStats.Min < dfCols[i].Stats.Min {
				dfCols[i].Stats.Min = colStats.Min
			}
			if colStats.Max > dfCols[i].Stats.Max {
				dfCols[i].Stats.Max = colStats.Max
			}
			if colStats.MaxLen > dfCols[i].Stats.MaxLen {
				dfCols[i].Stats.MaxLen = colStats.MaxLen
			}
			if colStats.MaxDecLen > dfCols[i].Stats.MaxDecLen {
				dfCols[i].Stats.MaxDecLen = colStats.MaxDecLen
			}

			{ // preserve original stats
				if col.Stats.Max > dfCols[i].Stats.Max {
					dfCols[i].Stats.Max = col.Stats.Max
				}
				if col.Stats.MaxLen > dfCols[i].Stats.MaxLen {
					dfCols[i].Stats.MaxLen = col.Stats.MaxLen
				}
				if col.Stats.MaxDecLen > dfCols[i].Stats.MaxDecLen {
					dfCols[i].Stats.MaxDecLen = col.Stats.MaxDecLen
				}
				if col.Stats.MinLen > 0 && dfCols[i].Stats.MinLen == 0 {
					dfCols[i].Stats.MinLen = col.Stats.MinLen
				}
			}

			switch {
			case col.Type.IsDecimal() || col.Type.IsFloat():
				// skip string comparison. may cause issues for clickhouse
				dfCols[i].Stats.LastVal = colStats.LastVal
			default:
				if cast.ToString(colStats.LastVal) > cast.ToString(dfCols[i].Stats.LastVal) {
					dfCols[i].Stats.LastVal = colStats.LastVal
				}
			}

			if col.Constraint != nil {
				dfCols[i].Constraint.FailCnt = dfCols[i].Constraint.FailCnt + col.Constraint.FailCnt
			}
		}
	}

	// reassign from dfCols
	df.Columns = dfCols

	if !df.Inferred {
		df.Columns = InferFromStats(df.Columns, false, false)
		df.Inferred = true
	}
}

// Count returns the aggregate count
func (df *Dataflow) Count() (cnt uint64) {
	if df != nil && df.Ready {
		for _, ds := range df.Streams {
			if ds.Ready {
				cnt += ds.Count
			}
		}
	}
	return
}

// AddEgressBytes add egress bytes
func (df *Dataflow) AddEgressBytes(bytes uint64) {
	df.EgressBytes = df.EgressBytes + bytes
}

func (df *Dataflow) Bytes() (inBytes, outBytes uint64) {
	return df.DsTotalBytes(), df.EgressBytes
}

func (df *Dataflow) DsTotalBytes() (bytes uint64) {
	if df != nil && df.Ready {
		for _, ds := range df.Streams {
			if ds.Ready {
				bytes += ds.Bytes.Load()
			}
		}
	}
	return
}

// Size is the number of streams
func (df *Dataflow) Size() int {
	return len(df.Streams)
}

func (df *Dataflow) PushStreamChan(dsCh chan *Datastream) {
	defer df.Close()

	pushCnt := 0

	defer func() { g.Trace("pushed %d datastreams", pushCnt) }()

	for ds := range dsCh {

		if df.closed {
			break
		}

		if df.Err() != nil {
			return
		}

		if ds.Err() != nil {
			df.Context.CaptureErr(ds.Err())
			return
		}

		select {
		case <-df.Context.Ctx.Done():
			return
		case <-ds.Context.Ctx.Done():
			if err := ds.Err(); err != nil {
				df.Context.CaptureErr(g.Error(err, "datastream error"))
			}
			return
		case <-ds.readyChn:
			// wait for first ds to start streaming.
			// columns/buffer need to be populated
			if len(df.Streams) > 0 {
				// add new columns two-way if not exist
				g.Debug("%s => %s", ds.Metadata.StreamURL.Value, g.Marshal(ds.Columns.Types()))
				ok := df.MergeColumns(ds.Columns, false)
				if !ok {
					// Could not run MergeColumns process
					g.Warn("could not merge columns...")
				}

				// add new columns two-way if not exist
				df.mux.Lock()
				ds.AddColumns(df.Columns, false)
				df.mux.Unlock()
			} else {
				if len(df.Columns) == 0 {
					df.Columns = ds.Columns
				}
				df.Buffer = ds.Buffer
			}

			// push stream, keep retrying
		tryPush:
			df.mux.Lock()
			ds.df = df
			select {
			case df.StreamCh <- ds:
				df.StreamMap[ds.ID] = ds
				df.Streams = append(df.Streams, ds)
				df.mux.Unlock()
			default:
				df.mux.Unlock()
				time.Sleep(1 * time.Millisecond)
				if df.closed {
					return
				}
				goto tryPush
			}

			pushCnt++
			g.Trace("%d datastreams pushed [%s]", pushCnt, ds.ID)
			if df.Limit > 0 && df.Count() >= df.Limit {
				g.Debug("reached dataflow limit of %d", df.Limit)
				df.SetReady()
				return
			} else if df.Count() >= uint64(SampleSize) {
				df.SetReady()
			} else if len(df.StreamCh) == cap(df.StreamCh) {
				df.SetReady()
			}
		}
	}

	df.SetReady()

}

// WaitReady waits until dataflow is ready
func (df *Dataflow) WaitReady() error {
	// wait for first ds to start streaming.
	// columns need to be populated
	select {
	case <-df.readyChn:
		return df.Err()
	case <-df.Context.Ctx.Done():
		if err := df.Err(); err != nil {
			return g.Error(df.Err(), "dataflow error while waiting for ready state")
		}
		return nil
	}
}

// WaitClosed waits until dataflow is closed
// hack to make sure all streams are pushed
func (df *Dataflow) WaitClosed() {
	for {
		if df.closed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Collect reads from one or more streams and return a dataset
func (df *Dataflow) Collect() (data Dataset, err error) {
	var datas []Dataset

	data = NewDataset(df.Columns)
	for ds := range df.StreamCh {
		d, err := ds.Collect(int(df.Limit))
		if err != nil {
			return NewDataset(nil), g.Error(err, "Error collecting ds")
		}

		datas = append(datas, d)
		data.AddColumns(d.Columns, false)
	}

	data.Result = nil
	data.Rows = [][]interface{}{}

	for _, d := range datas {
		// augment row size as needed
		for i := range d.Rows {
			for len(d.Rows[i]) < len(data.Columns) {
				d.Rows[i] = append(d.Rows[i], nil)
			}
		}
		data.Rows = append(data.Rows, d.Rows...)
	}

	if err = df.Err(); err != nil {
		err = g.Error(err)
	}

	return
}

// MakeDataFlow create a dataflow from datastreams
func MakeDataFlow(dss ...*Datastream) (df *Dataflow, err error) {

	if len(dss) == 0 {
		err = g.Error("Provided 0 datastreams for: %#v", dss)
		return
	}

	df = NewDataflow()
	dsCh := make(chan *Datastream)

	go func() {
		defer close(dsCh)
		for _, ds := range dss {
			dsCh <- ds
		}
	}()

	go df.PushStreamChan(dsCh)

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, g.Error(err)
	}

	return df, nil
}

// MergeDataflow merges the dataflow streams into one
func MergeDataflow(df *Dataflow) (dsN *Datastream) {

	rows := MakeRowsChan()
	nextFunc := func(it *Iterator) bool {
		for it.Row = range rows {
			return true
		}
		return false
	}
	dsN = NewDatastreamIt(df.Context.Ctx, df.Columns, nextFunc)
	dsN.it.IsCasted = true
	dsN.Inferred = true

	go func() {
		defer close(rows)
		for ds := range df.StreamCh {
			dsN.Sp.Config = ds.Sp.Config // copy config
			for batch := range ds.BatchChan {
				if !dsN.Columns.IsSimilarTo(df.Columns) {
					dsN.AddColumns(df.Columns, false)
					dsN.NewBatch(dsN.Columns)
				}

				shaper, err := batch.Columns.MakeShaper(dsN.Columns)
				if err != nil {
					dsN.Context.CaptureErr(g.Error(err, "could not MakeShaper"))
					dsN.Close()
					return
				}
				if shaper == nil {
					shaper = &Shaper{
						Func:       func(row []any) []any { return row },
						SrcColumns: batch.Columns,
						TgtColumns: dsN.Columns,
						ColMap:     map[int]int{},
					}
				}

				for row := range batch.Rows {
					rows <- shaper.Func(row)
				}
				if dsN.CurrentBatch != nil {
					dsN.CurrentBatch.Close()
				}
			}

			ds.Buffer = nil // clear buffer
		}
	}()

	err := dsN.Start()
	if err != nil {
		df.Context.CaptureErr(err)
		dsN.Close()
	}

	return dsN
}
