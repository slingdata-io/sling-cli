package database

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
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

func (r scyllaResult) RowsAffected() (int64, error) { return 0, nil }

func (conn *ScyllaDBConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbScyllaDB

	conn.BaseConn.SetProp("allow_bulk_export", "false")
	conn.BaseConn.SetProp("use_bulk", "false")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	return conn.BaseConn.Init()
}

func (conn *ScyllaDBConn) Connect(timeOut ...int) (err error) {
	to := 15 * time.Second
	if len(timeOut) > 0 && timeOut[0] > 0 {
		to = time.Duration(timeOut[0]) * time.Second
	}
	if v := conn.GetProp("timeout"); v != "" {
		if secs := cast.ToInt(v); secs > 0 {
			to = time.Duration(secs) * time.Second
		}
	}
	connectTo := to
	if v := conn.GetProp("connect_timeout"); v != "" {
		if secs := cast.ToInt(v); secs > 0 {
			connectTo = time.Duration(secs) * time.Second
		}
	}

	u, err := url.Parse(conn.BaseConn.URL)
	if err != nil {
		return g.Error(err, "Failed to parse URL")
	}

	hosts := []string{}
	seen := map[string]bool{}
	addHost := func(h string) {
		h = strings.TrimSpace(h)
		if h == "" {
			return
		}
		if hostOnly, _, err := net.SplitHostPort(h); err == nil {
			h = hostOnly
		}
		if !seen[h] {
			seen[h] = true
			hosts = append(hosts, h)
		}
	}
	addHost(u.Hostname())
	if configHosts := conn.GetProp("hosts"); configHosts != "" {
		for _, h := range strings.Split(configHosts, ",") {
			addHost(h)
		}
	}
	if len(hosts) == 0 {
		return g.Error("no ScyllaDB hosts configured (set host or hosts)")
	}

	cluster := gocql.NewCluster(hosts...)
	if portStr := u.Port(); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			cluster.Port = port
		}
	} else if p := cast.ToInt(conn.GetProp("port")); p > 0 {
		cluster.Port = p
	}

	if keyspace := conn.GetProp("keyspace", "database", "schema"); keyspace != "" {
		conn.keyspace = keyspace
		cluster.Keyspace = keyspace
	}

	cluster.Timeout = to
	cluster.ConnectTimeout = connectTo

	disableLookup := true // default true for docker/k8s unreachable broadcast IPs
	if v := conn.GetProp("disable_initial_host_lookup"); v != "" {
		disableLookup = cast.ToBool(v)
	}
	cluster.DisableInitialHostLookup = disableLookup

	if username, password := conn.GetProp("username"), conn.GetProp("password"); username != "" || password != "" {
		if username == "" {
			username = conn.GetProp("user")
		}
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	}

	if c := conn.GetProp("consistency"); c != "" {
		if cons, err := gocql.ParseConsistencyWrapper(c); err == nil {
			cluster.Consistency = cons
		} else {
			return g.Error(err, "invalid consistency %q (use ONE, LOCAL_ONE, QUORUM, LOCAL_QUORUM, ALL, ...)", c)
		}
	}

	if n := cast.ToInt(conn.GetProp("num_conns")); n > 0 {
		cluster.NumConns = n
	}
	if n := cast.ToInt(conn.GetProp("page_size")); n > 0 {
		cluster.PageSize = n
	}

	if localDC := conn.GetProp("local_dc", "datacenter"); localDC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.DCAwareRoundRobinPolicy(localDC),
		)
	}

	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return g.Error(err, "Failed to make TLS config")
	}
	if tlsConfig != nil {
		cluster.SslOpts = &gocql.SslOptions{
			Config:                 tlsConfig,
			EnableHostVerification: !tlsConfig.InsecureSkipVerify,
		}
	}

	ctx, cancel := context.WithTimeout(conn.Context().Ctx, cluster.ConnectTimeout)
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

func (conn *ScyllaDBConn) Close() error {
	if conn.session != nil {
		conn.session.Close()
		conn.session = nil
	}
	conn.SetProp("connected", "false")
	return nil
}

func (conn *ScyllaDBConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	return
}

func (conn *ScyllaDBConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	if pkCols := data.Columns.GetKeys(iop.PrimaryKey); len(pkCols) == 0 {
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

func (conn *ScyllaDBConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	return conn.StreamRows(table.Select(), g.M("columns", table.Columns))
}

func (conn *ScyllaDBConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	if table.Schema != "" && table.Name != "" && !table.IsQuery() {
		cql := g.F(
			`select column_name, type from system_schema.columns where keyspace_name = '%s' and table_name = '%s'`,
			table.Schema, table.Name,
		)
		iter := conn.session.QueryWithContext(conn.context.Ctx, cql).Iter()
		var name, typ string
		i := 0
		for iter.Scan(&name, &typ) {
			dbTy, colTy := nativeCQLTypeStringToIop(typ)
			columns = append(columns, iop.Column{
				Name:     name,
				Type:     colTy,
				DbType:   dbTy,
				Position: i + 1,
			})
			i++
		}
		if closeErr := iter.Close(); closeErr != nil {
			return columns, g.Error(closeErr, "could not get columns list with cql: %s", cql)
		}
		if len(columns) > 0 {
			return columns, nil
		}
	}

	// fallback via SELECT * LIMIT 1
	schema, name := table.Schema, table.Name
	if schema == "" || name == "" {
		parsed, perr := ParseTableName(table.FullName(), conn.GetType())
		if perr == nil {
			schema, name = parsed.Schema, parsed.Name
		}
	}
	if schema == "" {
		schema = conn.keyspace
	}
	cql := g.F(`select * from "%s"."%s" limit 1`, schema, name)
	iter := conn.session.QueryWithContext(conn.context.Ctx, cql).Iter()
	colInfo := iter.Columns()
	columns = conn.columnsFromGocqlInfo(colInfo)
	err = iter.Close()
	if err != nil {
		err = g.Error(err, "could not get columns list with cql: %s", cql)
	}
	return
}

// GetSQLColumns: no inner-query support
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

func (conn *ScyllaDBConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		return count, g.Error(err, "could not parse table name %s", tableFName)
	}
	if table.Schema == "" {
		table.Schema = conn.keyspace
	}

	columns, err := conn.GetTableColumns(&table)
	if err != nil {
		return count, g.Error(err, "could not get column list")
	}
	if len(columns) == 0 {
		columns = ds.Columns
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		return count, g.Error(err, "could not shape stream")
	}
	ds.Columns = columns

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

	// prefer live table types for decimal/bool marshaling
	insertCols := ds.Columns
	if table, perr := ParseTableName(tableFName, conn.GetType()); perr == nil {
		if table.Schema == "" {
			table.Schema = conn.keyspace
		}
		if tCols, terr := conn.GetTableColumns(&table); terr == nil && len(tCols) > 0 {
			byName := map[string]iop.Column{}
			for _, c := range tCols {
				byName[strings.ToLower(c.Name)] = c
			}
			merged := make(iop.Columns, len(insertCols))
			for i, c := range insertCols {
				if tc, ok := byName[strings.ToLower(c.Name)]; ok {
					c.Type = tc.Type
					c.DbType = tc.DbType
				}
				merged[i] = c
			}
			insertCols = merged
		}
	}

	cql := conn.GenerateInsertStatement(tableFName, insertCols, 1)
	g.Trace("ScyllaDB insert query: %s", cql)

	concurrency := 16
	if v := conn.GetProp("insert_concurrency"); v != "" {
		if n := cast.ToInt(v); n > 0 {
			concurrency = n
		}
	}
	insertCtx := g.NewContext(ds.Context.Ctx, concurrency)
	var rowCount atomic.Uint64

	for row := range ds.Rows() {
		if insertCtx.Err() != nil {
			break
		}
		if len(row) != len(insertCols) {
			insertCtx.CaptureErr(g.Error("row has %d fields, expected %d", len(row), len(insertCols)))
			break
		}

		args := make([]any, len(row))
		for i, val := range row {
			args[i] = normalizeInsertValue(val, insertCols[i])
		}

		insertCtx.Wg.Write.Add()
		go func(args []any) {
			defer insertCtx.Wg.Write.Done()
			if err := conn.session.Query(cql).WithContext(ds.Context.Ctx).Bind(args...).Exec(); err != nil {
				insertCtx.CaptureErr(g.Error(err, "could not insert row into ScyllaDB table %s", tableFName))
				return
			}
			rowCount.Add(1)
		}(args)
	}

	insertCtx.Wg.Write.Wait()
	count = rowCount.Load()

	if err = insertCtx.Err(); err != nil {
		return count, err
	}
	if err = ds.Err(); err != nil {
		return count, g.Error(err, "datastream error during ScyllaDB insert")
	}

	return count, nil
}

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

func (conn *ScyllaDBConn) CurrentDatabase() (string, error) {
	if conn.keyspace != "" {
		return conn.keyspace, nil
	}
	if keyspace := conn.GetProp("keyspace", "database", "schema"); keyspace != "" {
		return keyspace, nil
	}
	return "", nil
}

func (conn *ScyllaDBConn) CompareChecksums(tableName string, columns iop.Columns) (err error) {
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
				sum += uint64(len(strings.ReplaceAll(g.F("%s", val), " ", "")))
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

func nativeCQLTypeStringToIop(typ string) (string, iop.ColumnType) {
	base := strings.ToLower(strings.TrimSpace(typ))
	if i := strings.Index(base, "<"); i > 0 {
		base = base[:i]
	}
	switch base {
	case "ascii":
		return "ascii", iop.TextType
	case "bigint":
		return "bigint", iop.BigIntType
	case "blob":
		return "blob", iop.BinaryType
	case "boolean":
		return "bool", iop.BoolType
	case "counter":
		return "counter", iop.BigIntType
	case "date":
		return "date", iop.DateType
	case "decimal":
		return "decimal", iop.DecimalType
	case "double":
		return "double", iop.FloatType
	case "float":
		return "float", iop.FloatType
	case "inet":
		return "inet", iop.StringType
	case "int":
		return "int", iop.IntegerType
	case "smallint":
		return "smallint", iop.SmallIntType
	case "text":
		return "text", iop.TextType
	case "time":
		return "time", iop.TimeType
	case "timestamp":
		return "timestamp", iop.TimestampType
	case "timeuuid", "uuid":
		return "uuid", iop.UUIDType
	case "tinyint":
		return "tinyint", iop.SmallIntType
	case "varchar":
		return "varchar", iop.StringType
	case "varint":
		return "varint", iop.IntegerType
	case "duration":
		return "duration", iop.TimeType
	case "list", "set", "map", "tuple", "udt", "frozen", "vector":
		return base, iop.JsonType
	default:
		return typ, iop.StringType
	}
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

func normalizeInsertValue(val any, col iop.Column) any {
	if val == nil {
		return nil
	}
	if col.Type == iop.BoolType || strings.EqualFold(col.DbType, "boolean") || strings.EqualFold(col.DbType, "bool") {
		return cast.ToBool(val)
	}
	// gocql decimal only accepts string or *inf.Dec
	if col.Type == iop.DecimalType || strings.EqualFold(col.DbType, "decimal") || strings.EqualFold(col.DbType, "varint") {
		switch v := val.(type) {
		case *inf.Dec:
			return v
		case inf.Dec:
			return &v
		default:
			d := new(inf.Dec)
			if _, ok := d.SetString(cast.ToString(val)); ok {
				return d
			}
			return cast.ToString(val)
		}
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

// setupRowDataForNulls uses pointer types so gocql yields nil not zero values
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
