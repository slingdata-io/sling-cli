package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"

	"github.com/flarco/g"
)

// ClickhouseConn is a Clikchouse connection
type ClickhouseConn struct {
	BaseConn
	URL string
}

// Init initiates the object
func (conn *ClickhouseConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbClickhouse

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

func (conn *ClickhouseConn) Connect(timeOut ...int) (err error) {

	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		if strings.Contains(err.Error(), "unexpected packet") {
			g.Info(env.MagentaString("Try using the `http_url` instead to connect to Clickhouse via HTTP. See https://docs.slingdata.io/connections/database-connections/clickhouse"))
		}
	}

	return err
}

func (conn *ClickhouseConn) ConnString() string {

	if url := conn.GetProp("http_url"); url != "" {
		return url
	}

	return conn.BaseConn.ConnString()
}

// NewTransaction creates a new transaction
func (conn *ClickhouseConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {

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

	return Tx, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *ClickhouseConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	orderBy := "tuple()"
	primaryKey := ""
	if keyCols := data.Columns.GetKeys(iop.PrimaryKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		primaryKey = g.F("primary key (%s)", strings.Join(colNames, ", "))
		orderBy = strings.Join(colNames, ", ")
	}
	ddl = g.R(ddl, "primary_key", primaryKey, "order_by", orderBy)

	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// allow custom SQL expression for partitioning
		partitionBy = g.F("partition by (%s)", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by %s", strings.Join(colNames, ", "))
	}
	ddl = strings.ReplaceAll(ddl, "{partition_by}", partitionBy)

	return strings.TrimSpace(ddl), nil
}

// BulkImportStream inserts a stream into a table
func (conn *ClickhouseConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
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

			insCols, err := conn.ValidateColumnNames(columns, batch.Columns.Names())
			if err != nil {
				return g.Error(err, "columns mismatch")
			}

			insertStatement := conn.GenerateInsertStatement(
				table.FullName(),
				insCols,
				1,
			)

			stmt, err := conn.Prepare(insertStatement)
			if err != nil {
				g.Trace("%s: %#v", table, columns.Names())
				return g.Error(err, "could not prepare statement")
			}

			var decimalCols, intCols, jsonCols, int64Cols, floatCols []int
			for i, col := range batch.Columns {
				switch {
				case col.Type == iop.DecimalType:
					decimalCols = append(decimalCols, i)
				case col.Type == iop.SmallIntType:
					intCols = append(intCols, i)
				case col.Type == iop.JsonType:
					// jsonCols = append(jsonCols, i) // disable cause it gives issues (2025-05-01)
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

				// set JSON correctly
				for _, colI := range jsonCols {
					if row[colI] != nil {
						sVal := cast.ToString(row[colI])
						mVal, _ := g.UnmarshalMap(sVal)
						val := chcol.NewJSON()
						err := val.Scan(mVal)
						if !g.LogError(err, "could not convert value `%s` for clickhouse JSON", sVal) {
							row[colI] = val
						} else {
							row[colI] = nil
						}
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

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data")
		}
	}

	ds.SetEmpty()

	g.Trace("COPY %d ROWS", count)
	return count, nil
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *ClickhouseConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	values := make([]string, len(cols))
	qFields := make([]string, len(cols)) // quoted fields

	valuesStr := ""
	c := 0
	for n := 0; n < numRows; n++ {
		for i, col := range cols {
			c++
			values[i] = conn.bindVar(i+1, col.Name, n, c)
			// disabled due to issues (2025-05-01)
			// if col.Type == iop.JsonType {
			// 	values[i] = values[i] + "::JSON" // cast to JSOn
			// }
			qFields[i] = conn.Self().Quote(col.Name)
		}
		valuesStr += fmt.Sprintf("(%s),", strings.Join(values, ", "))
	}

	if conn.GetProp("http_url") != "" {
		table, _ := ParseTableName(tableName, conn.GetType())
		tableName = table.NameQ()
	}

	statement := g.R(
		"insert into {table} ({fields}) values  {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *ClickhouseConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	alter table {tgt_table}
	delete where ({tgt_pk_fields}) in (
			select {src_pk_fields}
			from {src_table} src
	)
	;

	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from {src_table} src
	`
	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"tgt_pk_fields", upsertMap["tgt_pk_fields"],
		"src_pk_fields", upsertMap["src_pk_fields"],
	)

	return
}

var count = 0

func processClickhouseInsertRow(columns iop.Columns, row []any) []any {
	for i := range row {
		col := columns[i]
		switch {
		case col.Type == iop.DecimalType:
			sVal := cast.ToString(row[i])
			if sVal != "" {
				val, err := decimal.NewFromString(sVal)
				if !g.LogError(err, "could not convert value `%s` for clickhouse decimal", sVal) {
					row[i] = val
				}
			} else {
				row[i] = nil
			}
		// case col.Type == iop.JsonType:
		// 	sVal := cast.ToString(row[i])
		// 	val := chcol.NewJSON()
		// 	err := val.Scan(sVal)
		// 	if !g.LogError(err, "could not convert value `%s` for clickhouse JSON", sVal) {
		// 		row[i] = val
		// 	} else {
		// 		row[i] = nil
		// 	}
		case col.Type == iop.FloatType:
			row[i] = cast.ToFloat64(row[i])
		}
	}
	count++
	return row
}

func (conn *ClickhouseConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	nativeType, err = conn.BaseConn.GetNativeType(col)

	// remove Nullable if part of pk
	if col.IsKeyType(iop.PrimaryKey) && strings.HasPrefix(nativeType, "Nullable(") {
		nativeType = strings.TrimPrefix(nativeType, "Nullable(")
		nativeType = strings.TrimSuffix(nativeType, ")")
	}

	return nativeType, err
}
