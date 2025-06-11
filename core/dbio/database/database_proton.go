package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	_ "github.com/timeplus-io/proton-go-driver/v2"

	"github.com/flarco/g"
)

// ProtonConn is a Proton connection
type ProtonConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *ProtonConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbProton

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

func (conn *ProtonConn) Connect(timeOut ...int) (err error) {

	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected packet") {
			g.Info(color.MagentaString("Try using the `http_url` instead to connect to Proton via HTTP. See https://docs.slingdata.io/connections/database-connections/Proton"))
		}
	}

	return err
}

func (conn *ProtonConn) ConnString() string {

	if url := conn.GetProp("http_url"); url != "" {
		return url
	}

	return conn.BaseConn.ConnString()
}

// NewTransaction creates a new transaction
func (conn *ProtonConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {

	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	Tx := &BaseTransaction{Tx: tx, Conn: conn.Self(), context: context}
	conn.tx = Tx

	// ProtonDB does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}

	return Tx, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *ProtonConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// allow custom SQL expression for partitioning
		partitionBy = g.F("partition by (%s)", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by %s", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{partition_by}", partitionBy)

	return strings.TrimSpace(sql), nil
}

// BulkImportStream inserts a stream into a table
func (conn *ProtonConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// set default schema
	conn.Exec(g.F("use `%s`", table.Schema))

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(100 * time.Millisecond)

			ds.Context.Lock()
			defer ds.Context.Unlock()

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for Postgres")
			}

			return nil
		}
	}

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}
		}

		err = func() error {
			// COPY needs a transaction
			if conn.Tx() == nil {
				err = conn.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})
				if err != nil {
					return g.Error(err, "could not begin")
				}
				defer conn.Rollback()
			}

			insFields, err := conn.ValidateColumnNames(columns, batch.Columns.Names())
			if err != nil {
				return g.Error(err, "columns mismatch")
			}

			insertStatement := conn.GenerateInsertStatement(
				table.FullName(),
				insFields,
				1,
			)

			stmt, err := conn.Prepare(insertStatement)
			if err != nil {
				g.Trace("%s: %#v", table, columns.Names())
				return g.Error(err, "could not prepare statement")
			}

			decimalCols := []int{}
			intCols := []int{}
			int64Cols := []int{}
			floatCols := []int{}
			for i, col := range batch.Columns {
				switch {
				case col.Type == iop.DecimalType:
					decimalCols = append(decimalCols, i)
				case col.Type == iop.SmallIntType:
					intCols = append(intCols, i)
				case col.Type.IsInteger():
					int64Cols = append(int64Cols, i)
				case col.Type == iop.FloatType:
					floatCols = append(floatCols, i)
				}
			}

			for row := range batch.Rows {
				var eG g.ErrorGroup

				// set decimals correctly
				for _, colI := range decimalCols {
					if row[colI] != nil {
						val, err := decimal.NewFromString(cast.ToString(row[colI]))
						if err == nil {
							row[colI] = val
						}
						eG.Capture(err)
					}
				}

				// set Int32 correctly
				for _, colI := range intCols {
					if row[colI] != nil {
						row[colI], err = cast.ToIntE(row[colI])
						eG.Capture(err)
					}
				}

				// set Int64 correctly
				for _, colI := range int64Cols {
					if row[colI] != nil {
						row[colI], err = cast.ToInt64E(row[colI])
						eG.Capture(err)
					}
				}

				// set Float64 correctly
				for _, colI := range floatCols {
					if row[colI] != nil {
						row[colI], err = cast.ToFloat64E(row[colI])
						eG.Capture(err)
					}
				}

				if err = eG.Err(); err != nil {
					err = g.Error(err, "could not convert value for COPY into table %s", tableFName)
					ds.Context.CaptureErr(err)
					return err
				}

				count++
				// Do insert
				ds.Context.Lock()
				_, err := stmt.Exec(row...)
				time.Sleep(1 * time.Millisecond)
				ds.Context.Unlock()
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
					g.Trace("error for row: %#v", row)
					return g.Error(err, "could not execute statement")
				}
			}

			err = stmt.Close()
			if err != nil {
				return g.Error(err, "could not close statement")
			}

			err = conn.Commit()
			if err != nil {
				return g.Error(err, "could not commit transaction")
			}
			// time.Sleep(100 * time.Millisecond)

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data")
		}
	}

	ds.SetEmpty()

	g.Debug("%d ROWS COPIED", count)
	// time.Sleep(100 * time.Millisecond)
	return count, nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *ProtonConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	const maxRetries = 3
	retries := 0

	for {
		result, err = conn.BaseConn.ExecContext(ctx, q, args...)
		if err == nil {
			return
		}

		retries++
		if retries >= maxRetries {
			return
		}

		g.Info("Error (%s). Sleep 5 sec and retry", err.Error())
		time.Sleep(5 * time.Second)
	}
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *ProtonConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	fields := cols.Names()
	values := make([]string, len(fields))
	qFields := make([]string, len(fields)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, field := range fields {
			c++
			values[i] = conn.bindVar(i+1, field, n, c)
			qFields[i] = conn.Self().Quote(field)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	if conn.GetProp("http_url") != "" {
		table, _ := ParseTableName(tableName, conn.GetType())
		tableName = table.NameQ()
	}

	statement := g.R(
		"insert into {table} ({fields}) values {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *ProtonConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	// proton does not support upsert with delete
	sqlTempl := `
	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from table({src_table}) src
	`
	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)

	return
}

func processProtonInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		if columns[i].Type == iop.DecimalType {
			sVal := cast.ToString(row[i])
			if sVal != "" {
				val, err := decimal.NewFromString(sVal)
				if !g.LogError(err, "could not convert value `%s` for Timeplus decimal", sVal) {
					row[i] = val
				}
			} else {
				row[i] = nil
			}
		} else if columns[i].Type == iop.FloatType {
			row[i] = cast.ToFloat64(row[i])
		}
	}
	return row
}

// GetCount returns count of records
func (conn *ProtonConn) GetCount(tableFName string) (int64, error) {
	// wait for a while before getting count, otherwise newly added row can be 0
	time.Sleep(3300 * time.Millisecond)
	sql := fmt.Sprintf(`select count(*) as cnt from table(%s)`, tableFName)
	data, err := conn.Self().Query(sql)
	if err != nil {
		g.LogError(err, "could not get row number")
		return 0, err
	}
	return cast.ToInt64(data.Rows[0][0]), nil
}

func (conn *ProtonConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	nativeType, err = conn.BaseConn.GetNativeType(col)

	// remove nullable if part of pk
	if col.IsKeyType(iop.PrimaryKey) && strings.HasPrefix(nativeType, "nullable(") {
		nativeType = strings.TrimPrefix(nativeType, "nullable(")
		nativeType = strings.TrimSuffix(nativeType, ")")
	}

	// special case for _tp_time, Column _tp_time is reserved, expected type is non-nullable datetime64
	if col.Name == "_tp_time" {
		return "datetime64(3, 'UTC')", nil
	}

	return nativeType, err
}
