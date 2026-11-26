package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"slices"
	"strings"
	"testing"

	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func TestOracleInsertBatchStreamLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int64
		want  []int
	}{
		{"one row per statement", 1, []int{1, 1, 1, 1}},
		{"bind cap takes precedence", 500, []int{333, 1}},
		{"no configured limit", 0, []int{333, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oracle, err := NewConn("oracle://localhost:1521/test")
			if err != nil {
				t.Fatal(err)
			}
			columns := iop.NewColumnsFromFields("a", "b", "c", "d", "e", "f")
			ds := iop.NewDatastream(columns)
			batch := ds.NewBatch(columns)
			rowCount := 334
			if tc.limit == 1 {
				rowCount = 4
			}
			for i := 0; i < rowCount; i++ {
				batch.Push([]any{i, i, i, i, i, i})
			}
			ds.Close()

			// A queued batch can already contain rows when target options are applied.
			ds.Sp.Config.BatchLimit = tc.limit
			batch.Limit = tc.limit
			recorder := &oracleInsertRecorder{}
			db := sql.OpenDB(recorder)
			defer db.Close()
			conn := &oracleBatchTestConn{Connection: oracle, columns: columns, db: db}

			count, err := InsertBatchStream(conn, nil, "target", ds)
			if err != nil || count != uint64(rowCount) {
				t.Fatalf("inserted %d of %d rows: %v", count, rowCount, err)
			}
			if !slices.Equal(recorder.rows, tc.want) {
				t.Errorf("rows per INSERT ALL = %v, want %v", recorder.rows, tc.want)
			}
		})
	}
}

type oracleBatchTestConn struct {
	Connection
	columns iop.Columns
	db      *sql.DB
}

func (c *oracleBatchTestConn) GetColumns(string, ...string) (iop.Columns, error) {
	return c.columns, nil
}

func (c *oracleBatchTestConn) Prepare(query string) (*sql.Stmt, error) {
	return c.db.Prepare(query)
}

type oracleInsertRecorder struct {
	driver.Connector
	driver.Conn
	rows []int
}

func (r *oracleInsertRecorder) Connect(_ context.Context) (driver.Conn, error) { return r, nil }
func (r *oracleInsertRecorder) Close() error                                   { return nil }

func (r *oracleInsertRecorder) Prepare(query string) (driver.Stmt, error) {
	r.rows = append(r.rows, strings.Count(query, "INTO target "))
	return oracleInsertTestStmt{}, nil
}

type oracleInsertTestStmt struct{ driver.Stmt }

func (oracleInsertTestStmt) Close() error  { return nil }
func (oracleInsertTestStmt) NumInput() int { return -1 }
func (oracleInsertTestStmt) Exec(args []driver.Value) (driver.Result, error) {
	return driver.RowsAffected(len(args) / 6), nil
}
