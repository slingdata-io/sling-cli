package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/flarco/g"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

type Transaction interface {
	Connection() Connection
	Context() *g.Context
	Commit() (err error)
	Rollback() (err error)
	Prepare(query string) (stmt *sql.Stmt, err error)
	QueryContext(ctx context.Context, q string, args ...interface{}) (result *sqlx.Rows, err error)
	ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error)
	ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error)
}

// BaseTransaction is a database transaction
type BaseTransaction struct {
	Tx      *sqlx.Tx
	Conn    Connection
	context *g.Context
	log     []string
}

type Result struct {
	rowsAffected int64
}

func (r Result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// Connection return the connection
func (t *BaseTransaction) Connection() Connection {
	return t.Conn
}

// Commit commits connection wide transaction
func (t *BaseTransaction) Context() *g.Context {
	return t.context
}

func (t *BaseTransaction) Commit() (err error) {
	select {
	case <-t.context.Ctx.Done():
		t.Rollback()
		err = t.context.Err()
		return
	default:
		g.Trace("commiting")
		err = t.Tx.Commit()
		if err != nil {
			if strings.Contains(err.Error(), "idle") {
				return nil
			}
			err = g.Error(err, "could not commit Tx")
		}
	}
	return
}

// Rollback rolls back connection wide transaction
func (t *BaseTransaction) Rollback() (err error) {
	if t == nil || t.Tx == nil {
		return
	}
	g.Trace("rolling back")
	err = t.Tx.Rollback()
	if err != nil {
		err = g.Error(err, "could not rollback Tx")
	}
	return
}

// Prepare prepares the statement
func (t *BaseTransaction) Prepare(query string) (stmt *sql.Stmt, err error) {
	stmt, err = t.Tx.PrepareContext(t.context.Ctx, query)
	if err != nil {
		err = g.Error(err, "could not prepare Tx: %s", query)
	}
	return
}

// Exec runs a sql query, returns `error`
func (t *BaseTransaction) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.ExecContext(t.context.Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// QueryContext queries rows
func (t *BaseTransaction) QueryContext(ctx context.Context, q string, args ...interface{}) (result *sqlx.Rows, err error) {
	t.log = append(t.log, q)

	t.Conn.Base().LogSQL(q, args...)
	result, err = t.Tx.QueryxContext(ctx, q, args...)
	if err != nil {
		err = g.Error(err, "Error executing query")
	}
	return
}

// ExecContext runs a sql query with context, returns `error`
func (t *BaseTransaction) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	if strings.TrimSpace(q) == "" {
		err = g.Error("Empty Query")
		return
	}

	t.Conn.Base().LogSQL(q, args...)

	t.log = append(t.log, q)
	result, err = t.Tx.ExecContext(ctx, q, args...)
	if err != nil {
		if strings.Contains(q, noDebugKey) && !g.IsDebugLow() {
			err = g.Error(err, "Error executing query")
		} else {
			err = g.Error(err, "Error executing: "+CleanSQL(t.Conn, q))
		}
	}

	return
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (t *BaseTransaction) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	eG := g.ErrorGroup{}
	for _, sql := range ParseSQLMultiStatements(q, t.Conn.GetType()) {
		res, err := t.ExecContext(ctx, sql, args...)
		if err != nil {
			eG.Capture(g.Error(err, "Error executing query"))
		} else {
			ra, _ := res.RowsAffected()
			g.Trace("RowsAffected: %d", ra)
			Res.rowsAffected = Res.rowsAffected + ra
		}
	}

	err = eG.Err()
	result = Res

	return
}

// DisableTrigger disables a trigger
func (t *BaseTransaction) DisableTrigger(tableName, triggerName string) (err error) {
	template := t.Conn.GetTemplateValue("core.disable_trigger")
	sql := g.R(
		template,
		"table", tableName,
		"trigger", triggerName,
	)
	if sql == "" {
		return
	}
	_, err = t.Exec(sql)
	if err != nil {
		return g.Error(err, "could not disable trigger %s on %s", triggerName, tableName)
	}
	return
}

// EnableTrigger enables a trigger
func (t *BaseTransaction) EnableTrigger(tableName, triggerName string) (err error) {
	template := t.Conn.GetTemplateValue("core.enable_trigger")
	sql := g.R(
		template,
		"table", tableName,
		"trigger", triggerName,
	)
	if sql == "" {
		return
	}
	_, err = t.Exec(sql)
	if err != nil {
		return g.Error(err, "could not enable trigger %s on %s", triggerName, tableName)
	}
	return
}

// UpsertStream inserts a stream into a table in batch
func (t *BaseTransaction) UpsertStream(tableFName string, ds *iop.Datastream, pk []string) (count uint64, err error) {

	// create tmp table first
	return
}

// InsertStream inserts a stream into a table
func (t *BaseTransaction) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertStream(t.Conn, t, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not insert into %s", tableFName)
	}
	return
}

// InsertBatchStream inserts a stream into a table in batch
func (t *BaseTransaction) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertBatchStream(t.Conn, t, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not batch insert into %s", tableFName)
	}
	return
}

// Upsert does an upsert from source table into target table
func (t *BaseTransaction) Upsert(sourceTable, targetTable string, pkFields []string) (count uint64, err error) {
	cnt, err := Upsert(t.Conn, t, sourceTable, targetTable, pkFields)
	if err != nil {
		err = g.Error(err, "Could not upsert from %s into %s", sourceTable, targetTable)
	}
	count = cast.ToUint64(cnt)
	return
}

// InsertStream inserts a stream
func InsertStream(conn Connection, tx *BaseTransaction, tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// make sure fields match
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}
	insFields, err := conn.ValidateColumnNames(columns.Names(), ds.Columns.Names(), true)
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, 1)

	var stmt *sql.Stmt
	if tx != nil {
		stmt, err = tx.Prepare(insertTemplate)
	} else {
		stmt, err = conn.Prepare(insertTemplate)
	}

	if err != nil {
		err = g.Error(err, "Could not prepate statement")
		return
	}
	for row := range ds.Rows() {
		count++
		// Do insert
		_, err = stmt.ExecContext(ds.Context.Ctx, row...)
		if err != nil {
			return count, g.Error(
				err,
				fmt.Sprintf("Insert: %s\nFor Row: %#v", insertTemplate, row),
			)
		}
	}

	return count, nil
}

// InsertBatchStream inserts a stream into a table in batch
func InsertBatchStream(conn Connection, tx Transaction, tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	context := ds.Context

	// in case schema change is needed, cannot alter while inserting
	mux := ds.Context.Mux
	if df := ds.Df(); df != nil {
		mux = df.Context.Mux
	}
	_ = mux

	insertBatch := func(bColumns iop.Columns, rows [][]interface{}) {
		var err error
		defer context.Wg.Write.Done()

		mux.Lock()
		defer mux.Unlock()

		insFields, err := conn.ValidateColumnNames(columns.Names(), bColumns.Names(), true)
		if err != nil {
			err = g.Error(err, "columns mismatch")
			context.CaptureErr(err)
			return
		}

		insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, len(rows))
		// conn.Base().AddLog(insertTemplate)
		// open statement
		var stmt *sql.Stmt
		if tx != nil {
			stmt, err = tx.Prepare(insertTemplate)
		} else {
			stmt, err = conn.Prepare(insertTemplate)
		}
		if err != nil {
			err = g.Error(err, "Error in PrepareContext")
			context.CaptureErr(err)
			return
		}

		vals := []interface{}{}
		for _, row := range rows {
			if conn.GetType() == dbio.TypeDbClickhouse {
				row = processClickhouseInsertRow(bColumns, row)
			} else if conn.GetType() == dbio.TypeDbTrino {
				row = processTrinoInsertRow(bColumns, row)
			}
			vals = append(vals, row...)
		}

		// Do insert
		_, err = stmt.ExecContext(ds.Context.Ctx, vals...)
		if err != nil {
			batchErrStr := g.F("Batch Size: %d rows x %d cols = %d (%d vals)", len(rows), len(bColumns), len(rows)*len(bColumns), len(vals))
			if len(insertTemplate) > 3000 {
				insertTemplate = insertTemplate[:3000]
			}
			// g.Warn("\n\n%s\n\n", g.Marshal(rows))
			if len(rows) > 10 {
				rows = rows[:10]
			}
			g.Debug(g.F(
				"%s\n%s \n%s \n%s",
				err.Error(), batchErrStr,
				fmt.Sprintf("Insert: %s", insertTemplate),
				fmt.Sprintf("\n\nRows: %#v", lo.Map(rows, func(row []any, i int) string {
					return g.F("len(row[%d]) = %d", i, len(row))
				})),
			))
			context.CaptureErr(err)
			return
		}

		// close statement
		err = stmt.Close()
		if err != nil {
			err = g.Error(
				err,
				fmt.Sprintf("stmt.Close: %s", insertTemplate),
			)
			context.CaptureErr(err)
		}
	}

	g.Trace("batchRows")

	var batch *iop.Batch
	var batchSize int

	batchRows := [][]interface{}{}

	for batch = range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			// make sure fields match
			mux.Lock()
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				err = g.Error(err, "could not get column list")
				return
			}
			mux.Unlock()

			// err = batch.Shape(columns)
			// if err != nil {
			// 	return count, g.Error(err, "could not shape batch stream")
			// }
		}

		if conn.GetType() == dbio.TypeDbClickhouse {
			batchSize = 1
		} else {
			batchSize = cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(columns)
		}

		for row := range batch.Rows {
			batchRows = append(batchRows, row)
			count++
			if len(batchRows) == batchSize {
				context.Wg.Write.Add()
				select {
				case <-context.Ctx.Done():
					return count, context.Err()
				case <-ds.Context.Ctx.Done():
					return count, ds.Context.Err()
				default:
					insertBatch(batch.Columns, batchRows)
				}

				// reset
				batchRows = [][]interface{}{}
			}
		}

		// insert
		if len(batchRows) > 0 {
			context.Wg.Write.Add()
			insertBatch(batch.Columns, batchRows)
			batchRows = [][]interface{}{}
		}
	}

	// remaining batch
	if len(batchRows) > 0 {
		g.Trace("remaining batchSize %d", len(batchRows))
		context.Wg.Write.Add()
		insertBatch(batch.Columns, batchRows)
	}

	context.Wg.Write.Wait()
	err = context.Err()
	ds.SetEmpty()

	if err != nil {
		ds.Context.Cancel()
		return count - cast.ToUint64(batchSize), g.Error(err, "insertBatch")
	}

	if ds.Err() != nil {
		return count, g.Error(ds.Err(), "context error")
	}

	return count, nil
}

// Upsert upserts from source table into target table
func Upsert(conn Connection, tx Transaction, sourceTable, targetTable string, pkFields []string) (count int64, err error) {

	srcTable, err := ParseTableName(sourceTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not parse source table name")
		return
	}

	tgtTable, err := ParseTableName(targetTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not parse target table name")
		return
	}

	q, err := conn.GenerateUpsertSQL(srcTable.FullName(), tgtTable.FullName(), pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert sql")
		return
	}

	var result sql.Result
	if tx != nil {
		result, err = tx.ExecMultiContext(tx.Context().Ctx, q)
	} else {
		result, err = conn.ExecMulti(q)
	}
	if err != nil {
		err = g.Error(err, "Could not upsert")
		return
	}

	count, err = result.RowsAffected()
	if err != nil || (count == 0 && conn.GetType() == dbio.TypeDbClickhouse) {
		count = -1
	}

	return
}

type ManualTransaction struct {
	Conn    Connection
	context *g.Context
	log     []string
}

func (t *ManualTransaction) Context() *g.Context {
	return t.context
}

func (t *ManualTransaction) Commit() (err error) {
	_, err = t.Conn.Exec("COMMIT")
	if err != nil {
		return g.Error(err, "could not commit")
	}
	return
}

func (t *ManualTransaction) Rollback() (err error) {
	_, err = t.Conn.Exec("ROLLBACK")
	if err != nil {
		return g.Error(err, "could not commit")
	}
	return
}

func (t *ManualTransaction) Prepare(query string) (stmt *sql.Stmt, err error) {
	return
}

func (t *ManualTransaction) QueryContext(ctx context.Context, q string, args ...interface{}) (result *sqlx.Rows, err error) {
	_, err = t.Conn.QueryContext(ctx, q)
	return
}

func (t *ManualTransaction) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.Conn.ExecContext(ctx, q)
	if err != nil {
		err = g.Error(err, "could not execute query")
	}
	return
}

func (t *ManualTransaction) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.Conn.ExecMultiContext(ctx, q)
	if err != nil {
		err = g.Error(err, "could not execute multiple queries")
	}
	return
}

type BlankTransaction struct {
	Conn    Connection
	context *g.Context
	log     []string
}

func (t *BlankTransaction) Connection() Connection {
	return t.Conn
}

func (t *BlankTransaction) Context() *g.Context {
	return t.context
}

func (t *BlankTransaction) Commit() (err error) {
	return
}

func (t *BlankTransaction) Rollback() (err error) {
	return
}

func (t *BlankTransaction) Prepare(query string) (stmt *sql.Stmt, err error) {
	return
}

func (t *BlankTransaction) QueryContext(ctx context.Context, q string, args ...interface{}) (result *sqlx.Rows, err error) {
	_, err = t.Conn.QueryContext(ctx, q)
	return
}

func (t *BlankTransaction) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.Conn.ExecContext(ctx, q)
	if err != nil {
		err = g.Error(err, "could not execute query")
	}
	return
}

func (t *BlankTransaction) ExecMultiContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	result, err = t.Conn.ExecMultiContext(ctx, q)
	if err != nil {
		err = g.Error(err, "could not execute multiple queries")
	}
	return
}
