package iop

import (
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
)

// RowChunkSize is the number of rows that the producer sends to the consumer at once.
// RowChunkWait is the time after which the producer sends a chunk that is not full.
var (
	RowChunkSize = 256
	RowChunkWait = 50 * time.Millisecond
)

type Batch struct {
	id          int
	Columns     Columns
	Previous    *Batch
	Count       int64
	Limit       int64
	MaxDuration time.Duration
	started     time.Time
	ds          *Datastream
	closed      bool
	transforms  []func(row []any) []any
	context     *g.Context

	// The producer adds rows to pending, and sends full chunks to the consumer.
	// Close gives the pending rows to the consumer as the tail.
	chunks       chan [][]any
	closing      chan struct{} // closed by Close, so that a flush that waits stops
	mu           sync.Mutex    // for pending, tail and closed
	flushMu      sync.Mutex    // one flush at a time, Close waits for it
	pending      [][]any
	pendingSince time.Time
	tail         [][]any
	flushTimer   *time.Timer
}

// NewBatch create new batch with fixed columns
// should be used each time column type changes, or columns are added
func (ds *Datastream) NewBatch(columns Columns) *Batch {
	if ds.ArrowOnly {
		// A sink took the row branch on an Arrow stream. Do not panic: the
		// error names the wiring bug and ends the run with a useful message.
		ds.Context.CaptureErr(g.Error("arrow lane: row consumer on an Arrow stream"))
		chunks := make(chan [][]any)
		close(chunks)
		return &Batch{
			Columns: columns,
			chunks:  chunks,
			closing: make(chan struct{}),
			closed:  true,
			context: g.NewContext(ds.Context.Ctx),
		}
	}

	batch := &Batch{
		id:          len(ds.Batches),
		Columns:     columns,
		Previous:    ds.LatestBatch(),
		ds:          ds,
		Limit:       ds.Sp.Config.BatchLimit,
		MaxDuration: ds.Sp.Config.BatchMaxDuration,
		started:     time.Now(),
		transforms:  []func(row []any) []any{},
		context:     g.NewContext(ds.Context.Ctx),
		chunks:      make(chan [][]any, 2),
		closing:     make(chan struct{}),
	}

	if batch.Previous != nil && !batch.Previous.closed {
		batch.Previous.Close() // close previous batch
	}
	ds.Batches = append(ds.Batches, batch)
	ds.CurrentBatch = batch
	if !ds.closed {
		ds.BatchChan <- batch
	}
	if !ds.NoDebug {
		g.Trace("new batch %s", batch.ID())
	}
	return batch
}

func (ds *Datastream) LatestBatch() *Batch {
	if len(ds.Batches) > 0 {
		return ds.Batches[len(ds.Batches)-1]
	}
	return nil
}

func (b *Batch) ID() string {
	if b == nil || b.ds == nil {
		return ""
	}
	return g.F("%s-%d", b.ds.ID, b.id)
}

func (b *Batch) Ds() *Datastream {
	if b == nil {
		return nil
	}
	return b.ds
}

func (b *Batch) IsFirst() bool {
	if b == nil {
		return false
	}
	return b.id == 0
}

// Close ends the batch. The consumer gets all the rows that the producer pushed.
func (b *Batch) Close() {
	if b == nil {
		return
	}
	b.context.Lock()
	defer b.context.Unlock()

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	close(b.closing)
	b.mu.Unlock()

	b.flushMu.Lock() // a flush in progress stops
	b.mu.Lock()
	b.tail, b.pending = b.pending, nil
	b.mu.Unlock()
	if b.flushTimer != nil {
		b.flushTimer.Stop()
	}
	close(b.chunks)
	b.flushMu.Unlock()

	b.ds.sendBwRows(b.tail)
	if !b.ds.NoDebug {
		g.Trace("closed %s", b.ID())
	}
}

// Rows gives the rows of the batch to the consumer, with the batch transforms.
func (b *Batch) Rows() iter.Seq[[]any] {
	return func(yield func([]any) bool) {
		send := func(rows [][]any) bool {
			for i, row := range rows {
				for _, f := range b.transforms {
					row = f(row)
				}
				for len(row) < len(b.Columns) {
					row = append(row, nil)
				}
				rows[i] = row
				atomic.AddUint64(&b.ds.Count, 1)
				if !yield(row) {
					return false
				}
			}
			return true
		}

		for chunk := range b.chunks {
			if !send(chunk) {
				return
			}
		}
		send(b.tail) // Close sets the tail before it closes chunks
	}
}

func (b *Batch) Closed() bool {
	return b != nil && b.closed
}

func (b *Batch) ColumnsChanged() bool {
	if pB := b.Previous; pB != nil {
		if len(pB.Columns) != len(b.Columns) {
			return true
		}
		for i := range b.Columns {
			if b.Columns[i].Type != pB.Columns[i].Type {
				return true
			} else if b.Columns[i].Name != pB.Columns[i].Name {
				return true
			}
		}
	}
	return false
}

func (b *Batch) Shape(tgtColumns Columns, pause ...bool) (err error) {
	doPause := true
	if len(pause) > 0 {
		doPause = pause[0]
	}
	srcColumns := b.Columns

	if len(tgtColumns) < len(srcColumns) {
		return g.Error("number of target columns is smaller than number of source columns")
	}

	// determine diff, and match order of target columns
	tgtColNames := lo.Map(tgtColumns, func(c Column, i int) string { return strings.ToLower(c.Name) })
	diffCols := len(tgtColumns) != len(srcColumns)
	colMap := map[int]int{}
	for s, col := range srcColumns {
		t := lo.IndexOf(tgtColNames, strings.ToLower(col.Name))
		if t == -1 {
			return g.Error("column %s not found in target columns. available => %s", col.Name, g.Marshal(tgtColNames))
		}
		colMap[s] = t
		diffCols = s != t ||
			!strings.EqualFold(tgtColumns[t].Name, srcColumns[s].Name)
	}

	if !diffCols {
		return nil
	}

	mapRowCol := func(srcRow []any) []any {
		tgtRow := make([]any, len(tgtColumns))
		for len(srcRow) < len(tgtRow) {
			srcRow = append(srcRow, nil)
		}
		for s, t := range colMap {
			tgtRow[t] = srcRow[s]
		}

		return tgtRow
	}

	if doPause {
		b.ds.Pause()
	}
	b.Columns = tgtColumns // should match the target columns
	b.AddTransform(mapRowCol)
	g.DebugLow("%s | added mapRowCol, len(b.transforms) = %d", b.ID(), len(b.transforms))
	if doPause {
		b.ds.Unpause()
	}

	return nil
}

func (b *Batch) AddTransform(transf func(row []any) []any) {
	b.transforms = append(b.transforms, transf)
}

func (b *Batch) Push(row []any) {
	select {
	case <-b.ds.pauseChan:
		<-b.ds.unpauseChan // wait for unpause
	default:
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		b.ds.it.Reprocess <- row
		return
	}
	if len(b.pending) == 0 {
		b.pendingSince = time.Now()
		if b.flushTimer == nil {
			b.flushTimer = time.AfterFunc(RowChunkWait, b.flushIdle)
		} else {
			b.flushTimer.Reset(RowChunkWait)
		}
	}
	b.pending = append(b.pending, row)
	b.Count++
	full := len(b.pending) >= RowChunkSize
	b.mu.Unlock()

	atomic.AddUint64(&b.ds.pushed, 1)
	b.ds.Sp.commitChecksum()

	// Start the wall-clock on the first row so setup delays between
	// NewBatch and the first Push don't burn the max-duration budget.
	if b.Count == 1 {
		b.started = time.Now()
	}

	if full {
		b.flush()
	}

	if b.Limit > 0 && b.Count == b.Limit {
		b.Close()
	} else if b.MaxDuration > 0 && time.Since(b.started) >= b.MaxDuration {
		if !b.ds.NoDebug {
			g.Debug("closing batch %s by max_duration=%s rows=%d", b.ID(), b.MaxDuration, b.Count)
		}
		b.Close()
	}
}

// flush sends the pending rows to the consumer. It waits for the consumer.
func (b *Batch) flush() {
	for {
		b.flushMu.Lock()
		b.mu.Lock()
		chunk := b.pending
		b.mu.Unlock()
		if len(chunk) == 0 {
			b.flushMu.Unlock()
			return
		}

		select {
		case b.chunks <- chunk:
			b.mu.Lock()
			b.pending = make([][]any, 0, RowChunkSize)
			b.mu.Unlock()
			b.flushMu.Unlock()
			b.ds.sendBwRows(chunk)
			return
		case <-b.closing: // Close gives the pending rows as the tail
			b.flushMu.Unlock()
			return
		case <-b.ds.Context.Ctx.Done():
			b.flushMu.Unlock()
			b.ds.Close()
			return
		case <-b.ds.pauseChan:
			b.flushMu.Unlock()
			<-b.ds.unpauseChan // wait for unpause, then try again
		}
	}
}

// flushIdle sends the pending rows of a slow producer, if the consumer waits.
func (b *Batch) flushIdle() {
	if !b.flushMu.TryLock() {
		return // the producer sends them
	}
	defer b.flushMu.Unlock()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || len(b.pending) == 0 {
		return
	} else if wait := RowChunkWait - time.Since(b.pendingSince); wait > 0 {
		b.flushTimer.Reset(wait)
		return
	}

	select {
	case b.chunks <- b.pending:
		go b.ds.sendBwRows(b.pending)
		b.pending = make([][]any, 0, RowChunkSize)
	default:
		b.flushTimer.Reset(RowChunkWait) // the consumer is busy
	}
}
