package iop

import (
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
)

type Batch struct {
	id         int
	Columns    Columns
	Rows       chan []any
	Previous   *Batch
	Count      int64
	Limit      int64
	ds         *Datastream
	closed     bool
	closeChan  chan struct{}
	transforms []func(row []any) []any
	context    *g.Context
}

// NewBatch create new batch with fixed columns
// should be used each time column type changes, or columns are added
func (ds *Datastream) NewBatch(columns Columns) *Batch {
	batch := &Batch{
		id:         len(ds.Batches),
		Columns:    columns,
		Rows:       MakeRowsChan(),
		Previous:   ds.LatestBatch(),
		ds:         ds,
		Limit:      ds.Sp.Config.BatchLimit,
		closeChan:  make(chan struct{}),
		transforms: []func(row []any) []any{},
		context:    g.NewContext(ds.Context.Ctx),
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

func (b *Batch) Close() {
	if b == nil {
		return
	}
	b.context.Lock()
	defer b.context.Unlock()
	if !b.closed {
		timer := time.NewTimer(4 * time.Millisecond)
		select {
		case b.closeChan <- struct{}{}:
		case <-timer.C:
		}
		b.closed = true
		close(b.Rows)
		if !b.ds.NoDebug {
			g.Trace("closed %s", b.ID())
		}
	}
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
			return g.Error("column %s not found in target columns", col.Name)
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

	newRow := row

	for _, f := range b.transforms {
		newRow = f(newRow) // allows transformations
	}

	for len(newRow) < len(b.Columns) {
		newRow = append(newRow, nil)
	}

	b.context.Lock()
	if b.closed {
		b.context.Unlock()
		b.ds.it.Reprocess <- row
		return
	}
	b.context.Unlock()

	select {
	case <-b.ds.Context.Ctx.Done():
		b.ds.Close()
	case <-b.ds.pauseChan:
		<-b.ds.unpauseChan // wait for unpause
		b.ds.it.Reprocess <- row
	case <-b.closeChan:
		b.ds.it.Reprocess <- row
	case v := <-b.ds.schemaChgChan:
		b.ds.it.Reprocess <- row
		b.ds.schemaChgChan <- v
	case b.Rows <- newRow:
		b.Count++
		b.ds.Count++
		b.ds.bwRows <- newRow
		b.ds.Sp.commitChecksum()

		if b.Limit > 0 && b.Count == b.Limit {
			b.Close()
		}
	}
}
