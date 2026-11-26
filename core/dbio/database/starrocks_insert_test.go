package database

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarRocksInsertBatchStreamBatchLimit(t *testing.T) {
	columns := iop.NewColumnsFromFields("a", "b", "c", "d", "e", "f")
	conn, err := NewConn("starrocks://root:@localhost:9030/sys")
	require.NoError(t, err)
	batchValues, err := strconv.Atoi(conn.GetTemplateValue("variable.batch_values"))
	require.NoError(t, err)
	templateCap := batchValues / len(columns)
	require.GreaterOrEqual(t, templateCap, 4)
	for _, test := range []struct {
		name  string
		limit int64
		rows  int
		want  []int
	}{
		{name: "one row per insert", limit: 1, rows: 4, want: []int{1, 1, 1, 1}},
		{name: "template cap", limit: int64(templateCap + 1), rows: templateCap + 1, want: []int{templateCap, 1}},
		{name: "no limit", rows: 4, want: []int{4}},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, err := NewConn("starrocks://root:@localhost:9030/sys")
			require.NoError(t, err)
			sr := conn.(*StarRocksConn)
			sr.SetProp("connected", "true")
			var metadata Connection = starRocksInsertMetadata{Connection: sr, columns: columns}
			sr.instance = &metadata
			tx := &starRocksInsertRecorder{}
			sr.setTx(tx)

			ds := iop.NewDatastream(columns)
			ds.StartBwProcessor()
			ds.Sp.Config.BatchLimit = test.limit
			for range test.rows {
				batch := ds.NewBatch(columns)
				batch.Push([]any{"a", "b", "c", "d", "e", "f"})
				batch.Close()
			}
			ds.Close()

			count, err := sr.InsertBatchStream("test_table", ds)
			require.NoError(t, err)
			assert.EqualValues(t, test.rows, count)
			assert.Equal(t, test.want, tx.insertSizes)
		})
	}
}

type starRocksInsertMetadata struct {
	Connection
	columns iop.Columns
}

func (m starRocksInsertMetadata) GetTableColumns(_ *Table, _ ...string) (iop.Columns, error) {
	return m.columns, nil
}

type starRocksInsertRecorder struct {
	Transaction
	insertSizes []int
}

func (r *starRocksInsertRecorder) ExecContext(_ context.Context, query string, _ ...interface{}) (sql.Result, error) {
	r.insertSizes = append(r.insertSizes, 1+strings.Count(query, "),\n("))
	return nil, nil
}
