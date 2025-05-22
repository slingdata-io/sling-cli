package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"google.golang.org/api/option"
)

// BigTableConn is a Google Big Query connection
type BigTableConn struct {
	BaseConn
	URL        string
	Client     *bigtable.Client
	ProjectID  string
	InstanceID string
	Location   string
}

// Init initiates the object
func (conn *BigTableConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbBigTable

	u, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		return g.Error(err, "could not parse bigtable url")
	}

	conn.ProjectID = conn.GetProp("project")
	if conn.ProjectID == "" {
		conn.ProjectID = u.U.Host
	}

	conn.InstanceID = conn.GetProp("instance")
	if conn.InstanceID == "" {
		conn.InstanceID = strings.ReplaceAll(u.Path(), "/", "")
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	err = conn.BaseConn.Init()
	if err != nil {
		err = g.Error(err, "could not initialize connection")
		return err
	}

	// Google BigQuery has limits
	// https://cloud.google.com/bigquery/quotas
	conn.Context().SetConcurrencyLimit(5)
	// conn.SetProp("FILE_MAX_ROWS", "1000000") // hard code?

	if conn.GetProp("GC_KEY_FILE") == "" {
		conn.SetProp("GC_KEY_FILE", conn.GetProp("keyfile")) // dbt style
	}

	// set MAX_DECIMALS to fix bigquery import for numeric types
	conn.SetProp("MAX_DECIMALS", "9")

	return nil
}

func (conn *BigTableConn) getNewClient(timeOut ...int) (client *bigtable.Client, err error) {
	var authOption option.ClientOption
	var credJsonBody string

	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}

	if val := conn.GetProp("GC_KEY_BODY"); val != "" {
		credJsonBody = val
		authOption = option.WithCredentialsJSON([]byte(val))
	} else if val := conn.GetProp("GC_KEY_FILE"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else if val := conn.GetProp("GC_CRED_API_KEY"); val != "" {
		authOption = option.WithAPIKey(val)
	} else if val := conn.GetProp("GOOGLE_APPLICATION_CREDENTIALS"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else {
		err = g.Error("no Google credentials provided")
		return
	}

	if conn.ProjectID == "" && credJsonBody != "" {
		m := g.M()
		g.Unmarshal(credJsonBody, &m)
		conn.ProjectID = cast.ToString(m["project_id"])
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()
	return bigtable.NewClient(ctx, conn.ProjectID, conn.InstanceID, authOption)
}

func (conn *BigTableConn) getNewAdminClient(timeOut ...int) (client *bigtable.AdminClient, err error) {
	var authOption option.ClientOption
	var credJsonBody string

	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}

	if val := conn.GetProp("GC_KEY_BODY"); val != "" {
		credJsonBody = val
		authOption = option.WithCredentialsJSON([]byte(val))
	} else if val := conn.GetProp("GC_KEY_FILE"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else if val := conn.GetProp("GC_CRED_API_KEY"); val != "" {
		authOption = option.WithAPIKey(val)
	} else if val := conn.GetProp("GOOGLE_APPLICATION_CREDENTIALS"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else {
		err = g.Error("no Google credentials provided")
		return
	}

	if conn.ProjectID == "" && credJsonBody != "" {
		m := g.M()
		g.Unmarshal(credJsonBody, &m)
		conn.ProjectID = cast.ToString(m["project_id"])
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()
	return bigtable.NewAdminClient(ctx, conn.ProjectID, conn.InstanceID, authOption)
}

// Connect connects to the database
func (conn *BigTableConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	_, err = conn.GetTables("")
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

// Close closes the connection
func (conn *BigTableConn) Close() error {
	err := conn.Client.Close()
	if err != nil {
		return err
	}
	return conn.BaseConn.Close()
}

// NewTransaction creates a new transaction
func (conn *BigTableConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {

	// BT does not support transactions at the moment
	Tx := &BlankTransaction{Conn: conn.Self(), context: g.NewContext(ctx)}
	conn.tx = Tx

	return nil, nil
}

type BigTableAction string

const BTCreateTable BigTableAction = "create_table"
const BTTableInfo BigTableAction = "table_info"
const BTDeleteTable BigTableAction = "delete_table"
const BTCreateColumnFamily BigTableAction = "create_column_family"

type BigTableQuery struct {
	Action         BigTableAction `json:"action"`
	Table          string         `json:"table"`
	ColumnFamilies []string       `json:"column_family"`
}

func (conn *BigTableConn) ExecContext(ctx context.Context, payload string, args ...interface{}) (result sql.Result, err error) {

	query := BigTableQuery{}
	err = g.Unmarshal(payload, &query)
	if err != nil {
		err = g.Error(err, "could not parse query payload")
		return
	}

	if query.Action == "" {
		err = g.Error("invalid query payload, no action provided")
		return
	}

	// get admin client
	client, err := conn.getNewAdminClient()
	if err != nil {
		err = g.Error(err, "could not get admin client")
		return
	}
	defer client.Close()

	queryContext := g.NewContext(ctx)
	if query.Table != "" && query.Action == BTCreateTable {
		err = client.CreateTable(queryContext.Ctx, query.Table)
		if err != nil {
			err = g.Error(err, "could not create table")
			return
		}
	}

	if query.Table != "" && len(query.ColumnFamilies) > 0 && (query.Action == BTCreateTable || query.Action == BTCreateColumnFamily) {
		for _, columnFamily := range query.ColumnFamilies {
			err = client.CreateColumnFamily(queryContext.Ctx, query.Table, columnFamily)
			if err != nil {
				err = g.Error(err, "could not create column family")
				return
			}
		}
	}

	if query.Table != "" && query.Action == BTDeleteTable {
		err = client.DeleteTable(queryContext.Ctx, query.Table)
		if err != nil {
			err = g.Error(err, "could not create table")
			return
		}
	}

	return
}

// GetTables returns tables for given schema
func (conn *BigTableConn) GetViews(schema string) (data iop.Dataset, err error) {
	return
}

func (conn *BigTableConn) GetTables(schema string) (data iop.Dataset, err error) {
	// fields: [table_name]

	// get admin client
	client, err := conn.getNewAdminClient()
	if err != nil {
		err = g.Error(err, "could not get admin client")
		return
	}
	defer client.Close()

	queryContext := g.NewContext(conn.context.Ctx)
	tables, err := client.Tables(queryContext.Ctx)
	if err != nil {
		err = g.Error(err, "could not list tables")
		return
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("table_name"))
	for _, table := range tables {
		data.Rows = append(data.Rows, []interface{}{table})
	}
	return
}

func (conn *BigTableConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]
	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	data.Append([]interface{}{"bigtable"})
	return data, nil
}

func (conn *BigTableConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (schemata Schemata, err error) {
	schemata = Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	tableData, err := conn.GetTables("")
	if err != nil {
		err = g.Error(err, "could not get tables")
		return
	}

	schema := Schema{Name: "", Tables: map[string]Table{}}
	for _, row := range tableData.Rows {
		table := cast.ToString(row[0])
		schema.Tables[strings.ToLower(table)] = Table{
			Name:     table,
			Database: "default",
			Dialect:  dbio.TypeDbBigTable,
		}
	}

	schemata.Databases["default"] = Database{
		Name:    "default",
		Schemas: map[string]Schema{"": schema},
	}

	return
}

func (conn *BigTableConn) GetColumnsFull(tableFName string) (iop.Dataset, error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return iop.Dataset{}, g.Error(err, "could not parse table name: "+tableFName)
	}

	// ds, err := conn.StreamRowsContext(conn.context.Ctx, table.Name, g.M("limit", 1000))
	// if err != nil {
	// 	return iop.Dataset{}, g.Error(err, "could not get sample data: "+tableFName)
	// }

	// for range ds.Rows { // drain channel
	// }

	// data := iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "column_name", "data_type", "position"))
	// for i, col := range ds.Columns {
	// 	data.Append([]interface{}{"", table.Name, col.Name, col.Type, i + 1})
	// }

	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "column_name", "data_type", "position"))
	cols, err := conn.GetColumns(table.Name)
	if err != nil {
		return iop.Dataset{}, g.Error(err, "could not get columns for table: "+table.Name)
	}
	for i, col := range cols {
		data.Append([]interface{}{"", table.Name, col.Name, col.Type, i + 1})
	}

	return data, nil
}

// GetTables returns tables for given schema
func (conn *BigTableConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	return conn.GetColumns(table.FullName())
}

func (conn *BigTableConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	// fields: [table_name]

	// get admin client
	client, err := conn.getNewAdminClient()
	if err != nil {
		err = g.Error(err, "could not get admin client")
		return
	}
	defer client.Close()

	queryContext := g.NewContext(conn.context.Ctx)
	info, err := client.TableInfo(queryContext.Ctx, tableFName)
	if err != nil {
		err = g.Error(err, "could not list table info")
		return
	}

	colNames := lo.Map(info.FamilyInfos, func(fi bigtable.FamilyInfo, i int) string {
		return fi.Name
	})

	columns = iop.NewColumnsFromFields(colNames...)

	return
}

func (conn *BigTableConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	ds, err := conn.StreamRowsContext(conn.context.Ctx, table.Name)
	if err != nil {
		return df, g.Error(err, "could start datastream")
	}

	df, err = iop.MakeDataFlow(ds)
	if err != nil {
		return df, g.Error(err, "could start dataflow")
	}

	return
}

func (conn *BigTableConn) StreamRowsContext(ctx context.Context, table string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	opts := getQueryOptions(options)
	Limit := uint64(0) // infinite
	if val := cast.ToUint64(opts["limit"]); val > 0 {
		Limit = val
	}

	start := time.Now()
	if strings.TrimSpace(table) == "" {
		g.Warn("Empty Table name")
		return ds, nil
	}

	queryContext := g.NewContext(ctx)

	tbl := conn.Client.Open(table)

	filter := bigtable.PassAllFilter()
	startTime := cast.ToTime(opts["start_time"])
	if startTime.IsZero() {
		if valS := conn.GetProp("start_time"); valS != "" {
			valI, err := cast.ToIntE(valS)
			if err != nil {
				// is string
				startTime = cast.ToTime(valS)
			} else {
				// is unix timestamp
				startTime = cast.ToTime(valI)
			}
		}
	}
	if !startTime.IsZero() {
		filter = bigtable.TimestampRangeFilter(startTime, time.Now())
	}

	conn.Data.SQL = table
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.NoDebug = !strings.Contains(table, noDebugKey)

	recChan := make(chan bigtable.Row)
	doneChan := make(chan struct{})

	go func() {
		defer close(recChan)
		defer func() { doneChan <- struct{}{} }()

		it := func(rec bigtable.Row) bool {
			// https://github.com/GoogleCloudPlatform/golang-samples/blob/782bee75d41682444d6af5e6ff4e08347829fb40/bigtable/filters/filters.go#L185
			select {
			case <-ds.Context.Ctx.Done():
				return false
			default:
				recChan <- rec
				return true
			}
		}

		// keys, _ := tbl.SampleRowKeys(queryContext.Ctx)

		err = tbl.ReadRows(
			queryContext.Ctx,
			bigtable.RowRange{},
			it,
			bigtable.RowFilter(filter),
		)
		if err != nil {
			if strings.Contains(table, noDebugKey) && !g.IsDebugLow() {
				err = g.Error(err, "Query Error")
			} else {
				err = g.Error(err, "Query Error for:\n"+table)
			}
			ds.Context.CaptureErr(err)
			ds.Context.Cancel()
		}

	}()

	ColumnMap := map[string]*iop.Column{}
	nextFunc := func(it *iop.Iterator) bool {
		if Limit > 0 && it.Counter >= Limit {
			return false
		}

		var row bigtable.Row

		select {
		case row = <-recChan:
		case <-doneChan:
			return false
		}

		if row == nil {
			return false
		}

		// process row
		keys := lo.Keys(row)
		sort.Strings(keys)

		ds := it.Ds()
		it.Row = make([]interface{}, len(ds.Columns))

		for _, columnFamily := range keys {
			cols := row[columnFamily]
			var colTimestamp time.Time
			for _, col := range cols {
				// colName := col.Column[strings.IndexByte(col.Column, ':')+1:]
				colName := strings.ReplaceAll(col.Column, ":", "_")
				col0, ok := ColumnMap[colName]
				if !ok {
					col0 = &iop.Column{
						Name:     colName,
						Type:     iop.StringType,
						Position: len(ColumnMap) + 1,
					}

					// add column
					ds.Columns = append(ds.Columns, *col0)
					ColumnMap[col0.Name] = col0

					it.Row = append(it.Row, nil)
				}
				it.Row[col0.Position-1] = string(col.Value)

				// timestamp
				if t := col.Timestamp.Time(); !col.Timestamp.Time().IsZero() {
					if colTimestamp.IsZero() || t.After(colTimestamp) {
						colTimestamp = t
					}
				}
			}

			if !colTimestamp.IsZero() {
				colName := "_bigtable_timestamp"
				col1, ok := ColumnMap[colName]
				if !ok {
					col1 = &iop.Column{
						Name:     colName,
						Type:     iop.BigIntType,
						Position: len(ColumnMap) + 1,
					}

					// add column
					ds.Columns = append(ds.Columns, *col1)
					ColumnMap[col1.Name] = col1

					it.Row = append(it.Row, nil)
				}
				it.Row[col1.Position-1] = colTimestamp.Unix()
			}

			colName := "_bigtable_key"
			col1, ok := ColumnMap[colName]
			if !ok {
				col1 = &iop.Column{
					Name:     colName,
					Type:     iop.StringType,
					Position: len(ColumnMap) + 1,
				}

				// add column
				ds.Columns = append(ds.Columns, *col1)
				ColumnMap[col1.Name] = col1

				it.Row = append(it.Row, nil)
			}
			it.Row[col1.Position-1] = row.Key()
		}

		return true
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoDebug = strings.Contains(table, noDebugKey)
	// ds.Inferred = !InferDBStream
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BigTableConn) InsertBatchStream(table string, ds *iop.Datastream) (count uint64, err error) {
	context := ds.Context
	tbl := conn.Client.Open(table)

	keyI := int32(0)

	insertBatch := func(rows [][]interface{}) error {
		defer context.Wg.Write.Done()

		muts := make([]*bigtable.Mutation, len(rows))
		rowKeys := make([]string, len(rows))

		// Make column families
		defaultColFamily := "default"
		colFamilies := map[string]map[string]int{defaultColFamily: {}}
		for i, col := range ds.Columns {
			colNameArr := strings.Split(col.Name, "__")
			if len(colNameArr) > 1 && false { // skip this logic, just use default
				colFamily := colNameArr[0]
				colName := strings.Join(colNameArr[1:], "__")
				colFamilies[colFamily][colName] = i
			} else {
				colFamilies[defaultColFamily][col.Name] = i
			}
		}

		for i, row := range rows {
			atomic.AddInt32(&keyI, 1)
			muts[i] = bigtable.NewMutation()
			rowKeys[i] = fmt.Sprintf("%d", keyI)
			for colFamily, colMap := range colFamilies {
				for colName, index := range colMap {
					muts[i].Set(colFamily, colName, bigtable.Now(), []byte(cast.ToString(row[index])))
				}
			}
		}

		rowErrs, err := tbl.ApplyBulk(context.Ctx, rowKeys, muts)
		if err != nil {
			err = g.Error(err, "could not ApplyBulk")
			ds.Context.CaptureErr(err)
			return err
		} else if rowErrs != nil {
			eG := g.ErrorGroup{}
			for i, rowErr := range rowErrs {
				eG.Capture(g.Error(rowErr, "Error writing row"))
				if i > 10 {
					break
				}
			}
			ds.Context.CaptureErr(eG.Err())
			return eG.Err()
		}

		return nil
	}

	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values"))
	batchRows := [][]interface{}{}
	g.Trace("batchRows")
	for row := range ds.Rows() {
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
				insertBatch(batchRows)
			}

			batchRows = [][]interface{}{}
		}
	}

	// remaining batch
	if len(batchRows) > 0 {
		g.Trace("remaining batchSize %d", len(batchRows))
		context.Wg.Write.Add()
		err = insertBatch(batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), g.Error(err, "insertBatch")
		}
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
	return
}
