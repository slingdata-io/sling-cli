package database

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/gocql/gocql"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"gopkg.in/inf.v0"
)

// ScyllaDBConn is a native ScyllaDB/CQL connection type.
type ScyllaDBConn struct {
	BaseConn
	URL      string
	keyspace string
	session  *gocql.Session
}

type scyllaResult struct{}

func (r scyllaResult) LastInsertId() (int64, error) { return 0, nil }

// no support for rows affected in scylladb
func (r scyllaResult) RowsAffected() (int64, error) { return 0, nil }

// Init initiates the object.
func (conn *ScyllaDBConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbScyllaDB

	// ScyllaDB/Cassandra batches are not a generic bulk-load primitive. Keep
	// Sling's bulk flags off and use the native prepared insert path below.
	conn.BaseConn.SetProp("allow_bulk_export", "false")
	conn.BaseConn.SetProp("use_bulk", "false")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

// Connect opens a native gocql session.
func (conn *ScyllaDBConn) Connect(timeOut ...int) (err error) {
	to := time.Duration(15) * time.Second
	if len(timeOut) > 0 {
		to = time.Duration(timeOut[0]) * time.Second
	}
	url, err := url.Parse(conn.BaseConn.URL)
	if err != nil {
		return g.Error(err, "Failed to parse URL")
	}
	hosts := []string{url.Host}
	if configHosts := conn.GetProp("hosts"); configHosts != "" {
		hosts = append(hosts, strings.Split(configHosts, ",")...)
	}
	cluster := gocql.NewCluster(hosts...)
	if port, err := strconv.Atoi(url.Port()); err == nil {
		cluster.Port = port
	}
	if keyspace := conn.GetProp("keyspace", "database", "schema"); keyspace != "" {
		conn.keyspace = keyspace
		cluster.Keyspace = keyspace
	}
	cluster.Timeout = to
	cluster.ConnectTimeout = to
	cluster.DisableInitialHostLookup = true
	if username, password := conn.GetProp("username"), conn.GetProp("password"); username != "" || password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	}

	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return g.Error(err, "Failed to make TLS config")
	}
	if tlsConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{Config: tlsConfig}
	}

	ctx, cancel := context.WithTimeout(conn.Context().Ctx, cluster.Timeout)
	defer cancel()

	session, err := cluster.CreateSession()
	if err != nil {
		return g.Error(err, "Could not open ScyllaDB session")
	}

	if err = session.QueryWithContext(ctx, "SELECT now() FROM system.local").Exec(); err != nil {
		session.Close()
		return g.Error(err, "Could not ping ScyllaDB")
	}

	conn.session = session
	conn.postConnect()

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	return nil
}

// Close closes the native gocql session and marks the Sling connection closed.
func (conn *ScyllaDBConn) Close() error {
	if conn.session != nil {
		conn.session.Close()
		conn.session = nil
	}
	conn.SetProp("connected", "false")
	return nil
}

func (conn *ScyllaDBConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// does not support transaction
	return
}

func (conn *ScyllaDBConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	if pkCols := data.Columns.GetKeys(iop.PrimaryKey); len(pkCols) == 0 {
		// scylladb doesn't support tables without primary key
		err = makeColumnPk(data.Columns)
		if err != nil {
			return "", g.Error(err)
		}
	}
	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	return strings.TrimSpace(ddl), nil
}

// Makes a first matching column primary key.
func makeColumnPk(columns iop.Columns) error {
	for i, col := range columns {
		if strings.ToLower(col.Name) == "id" {
			columns[i].SetMetadata(string(iop.PrimaryKey.MetadataKey()), "true")
			return nil
		}
	}

	for i, col := range columns {
		if strings.Contains(strings.ToLower(col.Name), "id") {
			columns[i].SetMetadata(string(iop.PrimaryKey.MetadataKey()), "true")
			return nil
		}
	}

	if len(columns) > 0 {
		columns[0].SetMetadata(string(iop.PrimaryKey.MetadataKey()), "true")
		return nil
	}

	return g.Error("no column suitable to make primary key")
}

// ExecContext executes a CQL statement.
func (conn *ScyllaDBConn) ExecContext(ctx context.Context, cql string, args ...interface{}) (sql.Result, error) {
	err := reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return nil, err
	}

	if strings.TrimSpace(cql) == "" {
		g.Warn("Empty Query")
		return scyllaResult{}, nil
	}

	conn.LogSQL(cql, args...)
	if err := conn.session.QueryWithContext(ctx, cql, args...).Exec(); err != nil {
		return scyllaResult{}, g.Error(err, "CQL Error for:\n%s", cql)
	}
	return scyllaResult{}, nil
}

func (conn *ScyllaDBConn) GetCount(tableFName string) (int64, error) {
	err := reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return 0, err
	}
	cql := g.F(`select count(*) from %s`, tableFName)
	var cnt int64
	err = conn.session.Query(cql).Scan(&cnt)
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

// StreamRowsContext streams the rows of a CQL query through gocql paging.
func (conn *ScyllaDBConn) StreamRowsContext(ctx context.Context, cql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return nil, err
	}
	if strings.TrimSpace(cql) == "" {
		g.Warn("Empty Query")
		return &iop.Datastream{}, nil
	}

	queryCtx := g.NewContext(ctx)
	conn.LogSQL(cql)
	limit := cast.ToUint64(getQueryOptions(options)["limit"])
	iter := conn.session.QueryWithContext(queryCtx.Ctx, cql).Iter()
	colInfo := iter.Columns()
	columns := conn.columnsFromGocqlInfo(colInfo)

	nextFunc := func(it *iop.Iterator) bool {
		if limit > 0 && it.Counter >= limit {
			return false
		}

		rowData := setupRowDataForNulls(colInfo)
		if !iter.MapScan(rowData) {
			if closeErr := iter.Close(); closeErr != nil {
				it.Context.CaptureErr(g.Error(closeErr, "Error during ScyllaDB iteration for cql %s", cql))
			}
			return false
		}

		row := make([]any, len(colInfo))
		for i := range colInfo {
			val := rowData[colInfo[i].Name]
			rv := reflect.ValueOf(val)
			if rv.Kind() == reflect.Ptr {
				if rv.IsNil() {
					row[i] = nil
				} else {
					row[i] = rv.Elem().Interface()
				}
			} else {
				row[i] = val
			}
			if colInfo[i].TypeInfo.Type() == gocql.TypeDecimal {
				if dec, ok := val.(*inf.Dec); ok && dec != nil {
					row[i] = cast.ToFloat64(dec.String())
				}
			}
		}
		it.Row = row
		return true
	}

	ds = iop.NewDatastreamIt(queryCtx.Ctx, columns, nextFunc)
	ds.Inferred = !InferDBStream
	ds.SetMetadata(conn.GetProp("METADATA"))
	ds.SetConfig(conn.Props())

	if err = ds.Start(); err != nil {
		queryCtx.Cancel()
		return ds, g.Error(err, "could not start ScyllaDB datastream")
	}

	return ds, nil
}

// BulkExportStream exports from ScyllaDB through the regular native cursor path.
func (conn *ScyllaDBConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	return conn.StreamRows(table.Select(), g.M("columns", table.Columns))
}

func (conn *ScyllaDBConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	cql := g.F(`select * from "%s"."%s" limit 1`, table.Schema, table.Name)
	iter := conn.session.QueryWithContext(conn.context.Ctx, cql).Iter()
	colInfo := iter.Columns()
	columns = conn.columnsFromGocqlInfo(colInfo)
	err = iter.Close()
	if err != nil {
		err = g.Error(err, "could not get columns list with cql: %s", cql)
	}
	return
}

// GetSQLColumns override, because scylladb doesn't suport inner queries
func (conn *ScyllaDBConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	if !table.IsQuery() {
		return conn.GetColumns(table.FullName())
	}

	ds, err := conn.StreamRows(table.SQL, g.M("limit", 1))
	if err != nil {
		return columns, g.Error(err, "GetSQLColumns Error")
	}

	err = ds.WaitReady()
	if err != nil {
		err = g.Error(err, "Datastream Error ")
		return columns, err
	}

	ds.Collect(0) // advance the datastream so it can close
	return ds.Columns, nil
}

// BulkImportStream imports into ScyllaDB using native gocql INSERT statements.
func (conn *ScyllaDBConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	columns, err := conn.GetTableColumns(&Table{Name: tableFName})
	if err != nil {
		return count, g.Error(err, "could not get column list")
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		return count, g.Error(err, "could not shape stream")
	}

	return conn.InsertStream(tableFName, ds)
}

func (conn *ScyllaDBConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.InsertStream(tableFName, ds)
}

func (conn *ScyllaDBConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	if err := reconnectIfClosed(conn); err != nil {
		return count, err
	}

	if len(ds.Columns) == 0 {
		return count, g.Error("cannot insert into ScyllaDB with no columns")
	}

	cql := conn.GenerateInsertStatement(tableFName, ds.Columns, 1)
	g.Trace("ScyllaDB insert query: %s", cql)
	query := conn.session.QueryWithContext(ds.Context.Ctx, cql)

	for row := range ds.Rows() {
		if len(row) != len(ds.Columns) {
			return count, g.Error("row has %d fields, expected %d", len(row), len(ds.Columns))
		}

		args := make([]any, len(row))
		for i, val := range row {
			args[i] = normalizeInsertValue(val, ds.Columns[i])
		}

		err = query.Bind(args...).Exec()
		if err != nil {
			return count, g.Error(err, "could not insert row into ScyllaDB table %s", tableFName)
		}
		count++
	}

	if err = ds.Err(); err != nil {
		return count, g.Error(err, "datastream error during ScyllaDB insert")
	}

	return count, nil
}

// GenerateInsertStatement generates a CQL INSERT statement.
func (conn *ScyllaDBConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {
	names := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, col := range cols {
		names[i] = fmt.Sprintf(`"%s"`, col.Name)
		placeholders[i] = "?"
	}
	return fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, tableName, strings.Join(names, ", "), strings.Join(placeholders, ", "))
}

func (conn *ScyllaDBConn) GetSchemas() (iop.Dataset, error) {
	cql := `SELECT keyspace_name
		FROM system_schema.keyspaces;`
	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	var name string
	iter := conn.session.Query(cql).Iter()
	for iter.Scan(&name) {
		data.Append([]any{name})
	}

	return data, nil
}

// CurrentDatabase returns the configured keyspace.
func (conn *ScyllaDBConn) CurrentDatabase() (string, error) {
	if conn.keyspace != "" {
		return conn.keyspace, nil
	}
	if keyspace := conn.GetProp("keyspace", "database", "schema"); keyspace != "" {
		return keyspace, nil
	}
	return "", nil
}

// Scylladb doesn't support functions used in comparisons so need to write its own
func (conn *ScyllaDBConn) CompareChecksums(tableName string, columns iop.Columns) (err error) {
	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err = g.Error(g.F("panic occurred! %#v\n%s", r, string(debug.Stack())))
		}
	}()

	table, err := ParseTableName(tableName, conn.GetType())
	if err != nil {
		return g.Error(err, "could not parse table name")
	}

	tColumns, err := conn.GetColumns(table.FullName())
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	// make sure columns exist in table, get common columns into cols
	cols, err := conn.ValidateColumnNames(tColumns, columns.Names())
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}
	fieldsMap := g.ArrMapString(cols.Names(), false)
	g.Debug("comparing checksums %s", g.Marshal(tColumns.Types()))

	exprs := []string{}
	for _, col := range columns {
		if _, ok := fieldsMap[col.Name]; !ok {
			continue // making sure it is a common column
		}
		exprs = append(exprs, g.F("\"%s\"", col.Name))
	}

	cql := g.F(
		"select %s from %s;",
		strings.Join(exprs, ", "),
		tableName,
	)

	ds, err := conn.StreamRowsContext(conn.context.Ctx, cql)
	data, err := ds.Collect(0)
	if err != nil {
		return g.Error(err, "error running CompareChecksums query")
	} else if len(data.Rows) == 0 {
		return g.Error("error running CompareChecksums query. No Rows returns")
	} else if len(data.Rows[0]) != len(data.Columns) {
		return g.Error("error running CompareChecksums query. Row vs Column size mismatch (%d != %d)", len(data.Rows[0]), len(data.Columns))
	}
	summedColumns := sumColumns(data)

	eg := g.ErrorGroup{}
	for _, col := range columns {
		actual := summedColumns[col.Name]
		expected := col.Stats.Checksum
		tCol := tColumns.GetColumn(col.Name)

		if actual != expected {
			eg.Add(g.Error("checksum failure for %s [%s | %s] (sling-side vs db-side): %d != %d", col.Name, col.Type, tCol.DbType, actual, expected))
		}
	}

	return eg.Err()
}

func sumColumns(data iop.Dataset) map[string]uint64 {
	colMap := make(map[string]uint64)
	for i, col := range data.Columns {
		var sum uint64
		values := data.ColValues(i)
		for _, val := range values {
			if val == nil {
				continue
			}
			switch {
			case col.Type.IsJSON():
				sum += uint64(len(strings.Replace(g.F("%s", val), " ", "", -1)))
			case col.IsNumber(), col.Type == iop.BigIntType:
				sum += uint64(math.Abs(cast.ToFloat64(val)))
			case col.IsString():
				sum += uint64(len(cast.ToString(val)))
			case col.IsBool():
				sum += uint64(lo.Ternary(cast.ToBool(val), 4, 5))
			case col.IsDate(), col.IsDatetime():
				t := cast.ToTime(val)
				sum += uint64(t.UnixMicro())
			case col.Type == iop.TimestampType, col.Type == iop.TimestampzType:
				sum += cast.ToUint64(val)
			}
		}
		colMap[col.Name] = sum
	}
	return colMap
}

func (conn *ScyllaDBConn) columnsFromGocqlInfo(colInfo []gocql.ColumnInfo) iop.Columns {
	columns := make(iop.Columns, len(colInfo))
	for i, col := range colInfo {
		dbTy, ty := cqlTypeToIopType(col.TypeInfo)
		columns[i] = iop.Column{
			Name:     col.Name,
			Type:     ty,
			DbType:   dbTy,
			Position: i + 1,
		}
	}
	return columns
}

func cqlTypeToIopType(dbType gocql.TypeInfo) (string, iop.ColumnType) {
	if dbType == nil {
		return "nil", iop.JsonType
	}
	switch dbType.Type() {
	case gocql.TypeCustom:
		return "custom", iop.JsonType
	case gocql.TypeAscii:
		return "ascii", iop.TextType
	case gocql.TypeBigInt:
		return "bigint", iop.BigIntType
	case gocql.TypeBlob:
		return "blob", iop.BinaryType
	case gocql.TypeBoolean:
		return "bool", iop.BoolType
	case gocql.TypeCounter:
		return "counter", iop.BigIntType
	case gocql.TypeDecimal:
		return "decimal", iop.DecimalType
	case gocql.TypeDouble:
		return "double", iop.FloatType
	case gocql.TypeFloat:
		return "float", iop.FloatType
	case gocql.TypeInt:
		return "int", iop.IntegerType
	case gocql.TypeText:
		return "text", iop.TextType
	case gocql.TypeTimestamp:
		return "timestamp", iop.TimestampType
	case gocql.TypeUUID:
		return "uuid", iop.UUIDType
	case gocql.TypeVarchar:
		return "varchar", iop.StringType
	case gocql.TypeVarint:
		return "varint", iop.IntegerType
	case gocql.TypeTimeUUID:
		return "uuid", iop.UUIDType
	case gocql.TypeInet:
		return "inet", iop.StringType
	case gocql.TypeDate:
		return "date", iop.DateType
	case gocql.TypeTime:
		return "time", iop.TimeType
	case gocql.TypeSmallInt:
		return "smallint", iop.SmallIntType
	case gocql.TypeTinyInt:
		return "tinyint", iop.SmallIntType
	case gocql.TypeDuration:
		return "duration", iop.TimeType
	case gocql.TypeList:
		return "list", iop.JsonType
	case gocql.TypeMap:
		return "map", iop.JsonType
	case gocql.TypeSet:
		return "set", iop.JsonType
	case gocql.TypeUDT:
		return "udt", iop.JsonType
	case gocql.TypeTuple:
		return "tuple", iop.JsonType
	}
	return "missing", iop.JsonType
}

// parses the value to match targeted column type if possible
func normalizeInsertValue(val any, col iop.Column) any {
	if val == nil {
		return nil
	}
	if col.Type == iop.BoolType {
		return cast.ToBool(val)
	}
	if col.Type == iop.DecimalType && g.GetType(val) == "string" {
		d := inf.Dec{}
		val, _ = d.SetString(val.(string))
		return val
	}
	if col.Type == iop.JsonType {
		switch val.(type) {
		case string, []byte, map[string]any, []any:
			return val
		default:
			return g.Marshal(val)
		}
	}
	return val
}

// assigns non default types so scylladb driver assigns nils instead of zero values
func setupRowDataForNulls(colInfo []gocql.ColumnInfo) map[string]any {
	rowData := map[string]any{}
	for i := range colInfo {
		name := colInfo[i].Name
		switch colInfo[i].TypeInfo.Type() {
		case gocql.TypeAscii, gocql.TypeVarchar, gocql.TypeText, gocql.TypeInet:
			rowData[name] = new(*string)
			break
		case gocql.TypeBoolean:
			rowData[name] = new(*bool)
			break
		case gocql.TypeDecimal:
			rowData[name] = new(*inf.Dec)
			break
		case gocql.TypeDouble:
			rowData[name] = new(*float64)
			break
		case gocql.TypeFloat:
			rowData[name] = new(*float32)
			break
		case gocql.TypeInt, gocql.TypeSmallInt, gocql.TypeTinyInt, gocql.TypeDuration, gocql.TypeCounter, gocql.TypeVarint:
			rowData[name] = new(*int64)
			break
		case gocql.TypeTimestamp, gocql.TypeDate:
			rowData[name] = new(*time.Time)
			break
		case gocql.TypeTime:
			rowData[name] = new(*time.Duration)
			break
		}
	}
	return rowData
}

var _ Connection = (*ScyllaDBConn)(nil)
