package database

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"github.com/jaswdr/faker"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
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
			err = g.Error(err, "Error executing: "+env.Clean(t.Conn.Props(), q))
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

// Merge does an upsert from source table into target table
func (t *BaseTransaction) Merge(sourceTable, targetTable string, pkFields []string) (count uint64, err error) {
	cnt, err := Merge(t.Conn, t, sourceTable, targetTable, pkFields)
	if err != nil {
		err = g.Error(err, "Could not merge from %s into %s", sourceTable, targetTable)
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
	insCols, err := conn.ValidateColumnNames(columns, ds.Columns.Names())
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insCols, 1)

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

	// Check for identity columns when target is SQL Server (schema migration scenario)
	// If inserting explicit values into identity columns, we need IDENTITY_INSERT ON
	if conn.GetType().IsSQLServer() {
		hasIdentity := false
		for _, col := range ds.Columns {
			if col.IsAutoIncrement() {
				hasIdentity = true
				break
			}
		}
		if hasIdentity {
			_, err = conn.Exec(g.F("SET IDENTITY_INSERT %s ON", tableFName))
			if err != nil {
				g.Debug("could not enable IDENTITY_INSERT: %v", err)
				// Don't fail - the insert might still work if the identity column isn't in the insert list
				err = nil
			} else {
				defer func() {
					conn.Exec(g.F("SET IDENTITY_INSERT %s OFF", tableFName))
				}()
			}
		}
	}

	insertBatch := func(bColumns iop.Columns, rows [][]interface{}) {
		var err error
		defer context.Wg.Write.Done()

		mux.Lock()
		defer mux.Unlock()

		insCols, err := conn.ValidateColumnNames(columns, bColumns.Names())
		if err != nil {
			err = g.Error(err, "columns mismatch")
			context.CaptureErr(err)
			return
		}

		insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insCols, len(rows))
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
			switch {
			case conn.GetType() == dbio.TypeDbClickhouse:
				row = processClickhouseInsertRow(bColumns, row)
			case conn.GetType() == dbio.TypeDbTrino:
				row = processTrinoInsertRow(bColumns, row)
			case conn.GetType() == dbio.TypeDbProton:
				row = processProtonInsertRow(bColumns, row)
			case conn.GetType().IsMySQLLike():
				row = processMySqlLikeInsertRow(bColumns, row)
			case conn.GetType().IsSQLServer():
				// row = processSQLServerInsertRow(bColumns, row)
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

			// set db type for casting (especially bools)
			for i := range batch.Columns {
				if col := columns.GetColumn(batch.Columns[i].Name); col != nil {
					batch.Columns[i].DbType = col.DbType
				}
			}
			mux.Unlock()

			// err = batch.Shape(columns)
			// if err != nil {
			// 	return count, g.Error(err, "could not shape batch stream")
			// }
		}

		if g.In(conn.GetType(), dbio.TypeDbClickhouse, dbio.TypeDbProton) {
			batchSize = 1
		} else if conn.GetType() == dbio.TypeDbSnowflake && strings.Contains(strings.Join(columns.DbTypes(), " "), "VARIANT") {
			// https://github.com/snowflakedb/gosnowflake/blob/099708d318689634a558f705ccc19b3b7b278972/structured_type_write_test.go#L12
			// variant binding is not currently supported, so we'll use SELECT and UNION ALL with PARSE_JSON
			batchSize = 50
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

// Merge upserts from source table into target table using the database's default strategy.
// This is a backward-compatible wrapper that calls MergeWithStrategy with nil strategy.
func Merge(conn Connection, tx Transaction, sourceTable, targetTable string, pkFields []string) (count int64, err error) {
	return MergeWithStrategy(conn, tx, sourceTable, targetTable, pkFields, nil)
}

// MergeWithStrategy upserts from source table into target table using the specified merge strategy.
// If strategy is nil, uses the database's default merge strategy from templates.
func MergeWithStrategy(conn Connection, tx Transaction, sourceTable, targetTable string, pkFields []string, strategy *MergeStrategy) (count int64, err error) {

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

	q, err := conn.Self().GenerateMergeSQLWithStrategy(srcTable.FullName(), tgtTable.FullName(), pkFields, strategy)
	if err != nil {
		err = g.Error(err, "could not generate merge sql")
		return
	}

	var result sql.Result
	if tx != nil {
		result, err = tx.ExecMultiContext(tx.Context().Ctx, q)
	} else {
		result, err = conn.ExecMulti(q)
	}
	if err != nil {
		err = g.Error(err, "Could not merge")
		return
	}

	if result != nil {
		count, err = result.RowsAffected()
		if err != nil || (count == 0 && conn.GetType() == dbio.TypeDbClickhouse) {
			count = -1
		}
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

type IsolationLevel string

const (
	IsolationLevelDefault         IsolationLevel = "default"
	IsolationLevelReadUncommitted IsolationLevel = "read_uncommitted"
	IsolationLevelReadCommitted   IsolationLevel = "read_committed"
	IsolationLevelWriteCommitted  IsolationLevel = "write_committed"
	IsolationLevelRepeatableRead  IsolationLevel = "repeatable_read"
	IsolationLevelSnapshot        IsolationLevel = "snapshot"
	IsolationLevelSerializable    IsolationLevel = "serializable"
	IsolationLevelLinearizable    IsolationLevel = "linearizable"
)

func (il IsolationLevel) AsSqlIsolationLevel() sql.IsolationLevel {
	switch il {
	case IsolationLevelDefault:
		return sql.LevelDefault
	case IsolationLevelReadUncommitted:
		return sql.LevelReadUncommitted
	case IsolationLevelReadCommitted:
		return sql.LevelReadCommitted
	case IsolationLevelWriteCommitted:
		return sql.LevelWriteCommitted
	case IsolationLevelRepeatableRead:
		return sql.LevelRepeatableRead
	case IsolationLevelSnapshot:
		return sql.LevelSnapshot
	case IsolationLevelSerializable:
		return sql.LevelSerializable
	case IsolationLevelLinearizable:
		return sql.LevelLinearizable
	}
	return sql.LevelDefault
}

type Operation string

const (
	OperationMerge        Operation = "merge"
	OperationDropTable    Operation = "drop_table"
	OperationCreateIndex  Operation = "create_index"
	OperationGenerateData Operation = "generate_data"
)

// QueryOperation uses the operation input to render/generate a query
// based on a template. This should be connection agnostic.
func QueryOperation(conn Connection, operation Operation, params map[string]any) (query string, err error) {

	var eG g.ErrorGroup

	getStringOrErr := func(key string) string {
		value := cast.ToString(params[key])
		if value == "" {
			eG.Add(g.Error("missing %s input", key))
		}
		return value
	}

	getStringSliceOrErr := func(key string) []string {
		value := cast.ToStringSlice(params[key])
		if len(value) == 0 {
			eG.Add(g.Error("missing %s input", key))
		}
		return value
	}

	switch operation {
	case OperationMerge:
		srcTable := getStringOrErr("source_table")
		tgtTable := getStringOrErr("target_table")
		// strategy := getStringOrErr("strategy") // TODO: add strategy
		primaryKey := getStringSliceOrErr("primary_key")

		if err = eG.Err(); err != nil {
			return
		}

		query, err = conn.Self().GenerateMergeSQL(srcTable, tgtTable, primaryKey)
		if err != nil {
			err = g.Error(err, "could not generate merge sql")
			return
		}

	case OperationDropTable:
		table, err := ParseTableName(cast.ToString(params["table"]), conn.GetType())
		if err != nil {
			return "", g.Error(err, "invalid table name: %s", params["table"])
		}
		query = g.R(conn.GetTemplateValue("core.drop_table"), "table", table.FullName())

	case OperationCreateIndex:
		table, err := ParseTableName(cast.ToString(params["table"]), conn.GetType())
		if err != nil {
			return "", g.Error(err, "invalid table name: %s", params["table"])
		}
		index := getStringOrErr("index")
		columns := getStringSliceOrErr("columns")

		if err = eG.Err(); err != nil {
			return "", err
		}

		inputs := g.M("index", index, "table", table.FullName(), "cols", strings.Join(columns, ", "))
		query = g.Rm(conn.GetTemplateValue("core.create_index"), inputs)

	case OperationGenerateData:
		query, err = generateDataOperation(conn, params)
	}

	return
}

// generateDataOperation generates a table with fake data based on provided columns and types
func generateDataOperation(conn Connection, params map[string]any) (query string, err error) {

	// generateFakeValue generates a fake value based on the column type
	generateFakeValue := func(fake faker.Faker, colType iop.ColumnType) any {
		switch colType {
		case iop.BigIntType:
			return fake.IntBetween(1000000, 9999999999)
		case iop.IntegerType:
			return fake.IntBetween(1, 1000000)
		case iop.SmallIntType:
			return fake.IntBetween(1, 32767)
		case iop.DecimalType, iop.FloatType:
			return cast.ToFloat64(fake.Float64(2, 0, 10000))
		case iop.BoolType:
			return fake.Bool()
		case iop.DateType:
			return fake.Time().Time(time.Now()).Format("2006-01-02")
		case iop.DatetimeType, iop.TimestampType:
			return fake.Time().Time(time.Now()).Format("2006-01-02 15:04:05")
		case iop.TimestampzType:
			return fake.Time().Time(time.Now()).Format("2006-01-02 15:04:05-07:00")
		case iop.TimeType:
			return fake.Time().Time(time.Now()).Format("15:04:05")
		case iop.TimezType:
			return fake.Time().Time(time.Now()).Format("15:04:05-07:00")
		case iop.UUIDType:
			return uuid.New().String()
		case iop.JsonType:
			return g.Marshal(g.M("id", fake.Int(), "name", fake.Person().Name(), "active", fake.Bool()))
		case iop.StringType:
			return fake.Lorem().Sentence(g.RandInt(20))
		case iop.TextType:
			whiteSpace := g.RandString("\t\n,.", 2)
			return fake.Lorem().Sentence(g.RandInt(50)) + whiteSpace + fake.Lorem().Sentence(g.RandInt(50))
		case iop.BinaryType:
			return []byte(fake.Lorem().Text(20))

		// custom types
		case iop.ColumnType("email"):
			return fake.Internet().Email()
		case iop.ColumnType("city"):
			return fake.Address().City()
		case iop.ColumnType("address"):
			return fake.Address().Address()
		case iop.ColumnType("country"):
			return fake.Address().Country()
		case iop.ColumnType("first_name"):
			return fake.Person().FirstName()
		case iop.ColumnType("last_name"):
			return fake.Person().LastName()
		case iop.ColumnType("name"):
			return fake.Person().Name()

		default:
			// Default to string for unknown types
			return fake.Person().Name()
		}
	}

	// Extract parameters
	fullTableName, _ := params["table"].(string)
	numRows := cast.ToInt(params["rows"])

	var columnsMap map[string]iop.ColumnType
	if err = g.JSONConvert(params["columns"], &columnsMap); err != nil {
		return "", g.Error(err, "invalid columns map provided")
	}

	if fullTableName == "" {
		return "", g.Error("table is required")
	}
	if len(columnsMap) == 0 {
		return "", g.Error("columns map is required")
	}
	if numRows <= 0 {
		numRows = 100 // default to 100 rows
	}

	// Sort column names alphabetically
	columnNames := lo.Keys(columnsMap)
	sort.Strings(columnNames)

	// Create iop.Columns with sorted names and general types
	columns := make(iop.Columns, len(columnNames))
	for i, colName := range columnNames {
		col := iop.Column{
			Name:     colName,
			Type:     columnsMap[colName],
			Position: i + 1,
		}
		if g.In(col.Type, iop.DecimalType, iop.FloatType) {
			col.DbPrecision = env.DdlMinDecLength
			col.DbScale = env.DdlMinDecScale
		}
		columns[i] = col
	}

	// Generate fake data based on column types
	fake := faker.New()
	rows := make([][]any, numRows)

	for i := 0; i < numRows; i++ {
		row := make([]any, len(columns))
		for j, col := range columns {
			row[j] = generateFakeValue(fake, col.Type)
		}
		rows[i] = row
	}

	// Create dataset
	dataset := iop.NewDataset(columns)
	dataset.Rows = rows

	// Generate DDL for table creation
	table, err := ParseTableName(fullTableName, conn.GetType())
	if err != nil {
		return "", g.Error(err, "invalid table name: %s", fullTableName)
	}

	// Generate DROP TABLE statement
	dropSQL := g.R(conn.GetTemplateValue("core.drop_table"), "table", fullTableName)

	_, err = conn.Self().Exec(dropSQL)
	if err != nil {
		return "", g.Error(err, "failed to drop existing table %s", table.FullName())
	}

	// Generate CREATE TABLE DDL
	createSQL, err := conn.Self().GenerateDDL(table, dataset, false)
	if err != nil {
		return "", g.Error(err, "failed to generate DDL")
	}

	_, err = conn.Self().Exec(createSQL)
	if err != nil {
		return "", g.Error(err, "failed to create table %s", table.FullName())
	}

	// Insert data using InsertBatchStream
	_, err = conn.Self().InsertBatchStream(fullTableName, dataset.Stream())
	if err != nil {
		return "", g.Error(err, "failed to insert fake data")
	}

	// Return dummy query
	query = g.F("select 'created table %s with %d columns, and inserted %d rows' as result", table.FullName(), len(columnsMap), numRows)

	return query, nil
}
