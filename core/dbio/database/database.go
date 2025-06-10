package database

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"

	"github.com/slingdata-io/sling-cli/core/dbio/filesys"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/microsoft/go-mssqldb/azuread"
	_ "github.com/microsoft/go-mssqldb/integratedauth/krb5"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	_ "github.com/snowflakedb/gosnowflake"

	_ "github.com/flarco/bigquery"
	// _ "github.com/solcates/go-sql-bigquery"
	"github.com/spf13/cast"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// InferDBStream may need to be `true`, since precision and scale is not guaranteed.
// If `false`, will use the database stream source schema
var InferDBStream = false

func init() {
	if val := os.Getenv("SLING_INFER_DB_STREAM"); val != "" {
		InferDBStream = cast.ToBool(val)
	}
}

// Connection is the Base interface for Connections
type Connection interface {
	Base() *BaseConn
	BaseURL() string
	Begin(options ...*sql.TxOptions) error
	BeginContext(ctx context.Context, options ...*sql.TxOptions) error
	BulkExportFlow(table Table) (*iop.Dataflow, error)
	BulkExportStream(table Table) (*iop.Datastream, error)
	BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error)
	BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	CastColumnForSelect(srcColumn iop.Column, tgtColumn iop.Column) string
	CastColumnsForSelect(srcColumns iop.Columns, tgtColumns iop.Columns) []string
	Close() error
	Commit() error
	CompareChecksums(tableName string, columns iop.Columns) (err error)
	Connect(timeOut ...int) error
	ConnString() string
	Context() *g.Context
	CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error)
	CreateTemporaryTable(tableName string, cols iop.Columns) (err error)
	CurrentDatabase() (string, error)
	Db() *sqlx.DB
	DbX() *DbX
	DropTable(...string) error
	DropView(...string) error
	Exec(sql string, args ...interface{}) (result sql.Result, err error)
	ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error)
	ExecMulti(sqls ...string) (result sql.Result, err error)
	ExecMultiContext(ctx context.Context, sqls ...string) (result sql.Result, err error)
	GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error)
	GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string
	GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error)
	GetAnalysis(string, map[string]interface{}) (string, error)
	GetColumns(tableFName string, fields ...string) (iop.Columns, error)
	GetColumnsFull(string) (iop.Dataset, error)
	GetColumnStats(tableName string, fields ...string) (columns iop.Columns, err error)
	GetCount(string) (int64, error)
	GetDatabases() (iop.Dataset, error)
	GetMaxValue(t Table, colName string) (value any, col iop.Column, err error)
	GetDDL(string) (string, error)
	GetGormConn(config *gorm.Config) (*gorm.DB, error)
	GetIndexes(string) (iop.Dataset, error)
	GetNativeType(col iop.Column) (nativeType string, err error)
	GetPrimaryKeys(string) (iop.Dataset, error)
	GetProp(...string) string
	GetSchemas() (iop.Dataset, error)
	GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error)
	GetSQLColumns(table Table) (columns iop.Columns, err error)
	GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error)
	GetTablesAndViews(string) (iop.Dataset, error)
	GetTables(string) (iop.Dataset, error)
	GetTemplateValue(path string) (value string)
	GetType() dbio.Type
	GetURL(newURL ...string) string
	GetViews(string) (iop.Dataset, error)
	Info() ConnInfo
	Init() error
	InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	Kill() error
	LoadTemplates() error
	MustExec(sql string, args ...interface{}) (result sql.Result)
	NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error)
	OptimizeTable(table *Table, columns iop.Columns, isTemp ...bool) (ok bool, err error)
	Prepare(query string) (stmt *sql.Stmt, err error)
	ProcessTemplate(level, text string, values map[string]interface{}) (sql string, err error)
	Props() map[string]string
	PropsArr() []string
	Query(sql string, options ...map[string]interface{}) (iop.Dataset, error)
	QueryContext(ctx context.Context, sql string, options ...map[string]interface{}) (iop.Dataset, error)
	Quote(field string) string
	RenameTable(table string, newTable string) (err error)
	Rollback() error
	RunAnalysis(string, map[string]interface{}) (iop.Dataset, error)
	Schemata() Schemata
	Self() Connection
	SetProp(string, string)
	StreamRecords(sql string) (<-chan map[string]interface{}, error)
	StreamRows(sql string, options ...map[string]interface{}) (*iop.Datastream, error)
	StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error)
	SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error)
	SwapTable(srcTable string, tgtTable string) (err error)
	TableExists(table Table) (exists bool, err error)
	Template() dbio.Template
	Tx() Transaction
	Unquote(string) string
	Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error)
	ValidateColumnNames(tgtCols iop.Columns, colNames []string) (newCols iop.Columns, err error)
	AddMissingColumns(table Table, newCols iop.Columns) (ok bool, err error)
}

type ConnInfo struct {
	Host      string
	Port      int
	Database  string
	User      string
	Password  string
	Schema    string
	Warehouse string
	Role      string
	URL       *net.URL
}

// BaseConn is a database connection
type BaseConn struct {
	Connection
	URL         string
	Type        dbio.Type // the type of database for sqlx: postgres, mysql, sqlite
	db          *sqlx.DB
	tx          Transaction
	Data        iop.Dataset
	defaultPort int
	instance    *Connection
	context     *g.Context
	template    dbio.Template
	schemata    Schemata
	properties  map[string]string
	sshClient   *iop.SSHClient
	Log         []string
}

// Pool is a pool of connections
type Pool struct {
	Dbs     map[string]*sqlx.DB
	DuckDbs map[string]*DuckDbConn
	Mux     sync.Mutex
}

var (
	// UseBulkExportFlowCSV to use BulkExportFlowCSV
	UseBulkExportFlowCSV = false

	ddlDefDecLength = 20

	ddlMinDecLength = 24
	ddlMaxDecScale  = 24

	ddlMaxDecLength = 38
	ddlMinDecScale  = 6

	tempCloudStorageFolder = "sling_temp"

	noDebugKey = env.NoDebugKey

	connPool = Pool{Dbs: map[string]*sqlx.DB{}, DuckDbs: map[string]*DuckDbConn{}}
	usePool  = os.Getenv("USE_POOL") == "TRUE"
)

func init() {
	if os.Getenv("SLING_TEMP_CLOUD_FOLDER") != "" || os.Getenv("FILEPATH_SLUG") != "" {
		tempCloudStorageFolder = os.Getenv("SLING_TEMP_CLOUD_FOLDER")
		if tempCloudStorageFolder == "" {
			tempCloudStorageFolder = os.Getenv("FILEPATH_SLUG") // legacy
		}
	}

}

// NewConn return the most proper connection for a given database
func NewConn(URL string, props ...string) (Connection, error) {
	return NewConnContext(context.Background(), URL, props...)
}

// NewConnContext return the most proper connection for a given database with context
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewConnContext(ctx context.Context, URL string, props ...string) (Connection, error) {
	var conn Connection

	// URL is actually just DB Name in this case
	// see TestEnvURL
	OrigURL := URL
	if newURL := os.Getenv(strings.TrimLeft(URL, "$")); newURL != "" {
		URL = newURL
	}

	if !strings.Contains(URL, ":") {
		err := g.Error("could not detect URL")
		return nil, g.Error(err, "invalid URL")
	}

	// Add / Extract Props from URL
	u, err := net.NewURL(URL)
	if err == nil {
		vars := env.Vars()
		for k, v := range u.Query() {
			props = append(props, g.F("%s=%s", k, v))
			if _, ok := vars[strings.ToUpper(k)]; ok {
				u.PopParam(k)
			}
		}
		URL = u.String() // update url

		// issue with some drivers not parsing special characters in go escaped format
		if u.Password() != "" {
			passwordEncOld := strings.Replace(u.U.User.String(), u.Username()+":", "", 1)
			passwordEncNew := url.QueryEscape(u.Password())
			URL = strings.Replace(URL, ":"+passwordEncOld+"@", ":"+passwordEncNew+"@", 1)
		}
	} else {
		return nil, g.Error(err, "could not parse URL")
	}

	concurrency := 10
	if strings.HasPrefix(URL, "postgres") {
		if strings.Contains(URL, "redshift.amazonaws.com") {
			conn = &RedshiftConn{URL: URL}
		} else {
			conn = &PostgresConn{URL: URL}
		}
	} else if strings.HasPrefix(URL, "redshift") {
		conn = &RedshiftConn{URL: URL}
	} else if strings.HasPrefix(URL, "athena") {
		conn = &AthenaConn{URL: URL}
	} else if strings.HasPrefix(URL, "trino") {
		conn = &TrinoConn{URL: URL}
	} else if strings.HasPrefix(URL, "sqlserver:") {
		conn = &MsSQLServerConn{URL: URL}
	} else if strings.HasPrefix(URL, "starrocks:") {
		conn = &StarRocksConn{URL: URL}
	} else if strings.HasPrefix(URL, "mysql:") {
		conn = &MySQLConn{URL: URL}
	} else if strings.HasPrefix(URL, "mongo") {
		conn = &MongoDBConn{URL: URL}
	} else if strings.HasPrefix(URL, "elasticsearch") {
		conn = &ElasticsearchConn{URL: URL}
	} else if strings.HasPrefix(URL, "prometheus") {
		conn = &PrometheusConn{URL: URL}
	} else if strings.HasPrefix(URL, "mariadb:") {
		conn = &MySQLConn{URL: URL}
	} else if strings.HasPrefix(URL, "oracle:") {
		conn = &OracleConn{URL: URL}
		// concurrency = 2
	} else if strings.HasPrefix(URL, "bigquery:") {
		conn = &BigQueryConn{URL: URL}
	} else if strings.HasPrefix(URL, "bigtable:") {
		conn = &BigTableConn{URL: URL}
	} else if strings.HasPrefix(URL, "clickhouse:") {
		conn = &ClickhouseConn{URL: URL}
	} else if strings.HasPrefix(URL, "proton:") {
		conn = &ProtonConn{URL: URL}
	} else if strings.HasPrefix(URL, "snowflake") {
		conn = &SnowflakeConn{URL: URL}
	} else if strings.HasPrefix(URL, "d1") {
		conn = &D1Conn{URL: URL}
	} else if strings.HasPrefix(URL, "sqlite:") {
		conn = &SQLiteConn{URL: URL}
	} else if strings.HasPrefix(URL, "duckdb:") || strings.HasPrefix(URL, "motherduck:") {
		conn = &DuckDbConn{URL: URL}
	} else if strings.HasPrefix(URL, "ducklake:") {
		conn = &DuckLakeConn{DuckDbConn: DuckDbConn{URL: URL}}
	} else if strings.HasPrefix(URL, "iceberg:") {
		conn = &IcebergConn{URL: URL}
	} else {
		conn = &BaseConn{URL: URL}
	}

	conn.Base().setContext(ctx, concurrency)

	// Add / Extract provided Props
	for _, propStr := range props {
		arr := strings.Split(propStr, "=")
		if len(arr) == 1 && arr[0] != "" {
			conn.SetProp(arr[0], "")
		} else if len(arr) == 2 {
			conn.SetProp(arr[0], arr[1])
		} else if len(arr) > 2 {
			val := strings.Join(arr[1:], "=")
			conn.SetProp(arr[0], val)
		}
	}

	// Init
	conn.SetProp("orig_url", OrigURL)
	conn.SetProp("orig_prop_keys", g.Marshal(lo.Keys(conn.Base().properties))) // used when caching conn

	if val := conn.GetProp("concurrency"); val != "" {
		concurrency = cast.ToInt(val)
		conn.Base().setContext(ctx, concurrency)
	}

	err = conn.Init()

	// set sling_conn_id
	conn.SetProp("sling_conn_id", g.RandSuffix(g.F("conn-%s-", conn.GetType()), 3))

	return conn, err
}

func getDriverName(conn Connection) (driverName string) {
	dbType := conn.GetType()
	switch dbType {
	case dbio.TypeDbPostgres, dbio.TypeDbRedshift:
		driverName = "postgres"
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks:
		driverName = "mysql"
	case dbio.TypeDbOracle:
		driverName = "oracle"
	case dbio.TypeDbBigQuery:
		driverName = "bigquery"
	case dbio.TypeDbSnowflake:
		driverName = "snowflake"
	case dbio.TypeDbSQLite:
		driverName = "sqlite3"
	case dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbDuckLake:
		driverName = "duckdb"
	case dbio.TypeDbSQLServer, dbio.TypeDbAzure:
		driverName = "sqlserver"
	case dbio.TypeDbTrino:
		driverName = "trino"
	case dbio.TypeDbProton:
		driverName = "proton"
	default:
		driverName = dbType.String()
	}

	// for custom specified driver
	if driver := conn.GetProp("driver"); driver != "" {
		return driver
	}

	return
}

func getDialector(conn Connection) (driverDialector gorm.Dialector) {
	switch conn.GetType() {
	case dbio.TypeDbPostgres, dbio.TypeDbRedshift:
		driverDialector = postgres.Open(conn.Self().BaseURL())
	case dbio.TypeDbSQLite:
		driverDialector = sqlite.Open(conn.Self().GetURL())
	default:
		g.LogError(g.Error("No Gorm Dialector found for %s", conn.GetType()))
	}
	return
}

// BaseURL returns the base Conn
func (conn *BaseConn) Base() *BaseConn {
	return conn
}

// BaseURL returns the base URL with default port
func (conn *BaseConn) BaseURL() string {
	return conn.URL
}

// ConnString returns the connection string needed for connection
func (conn *BaseConn) ConnString() string {
	if val := conn.GetProp("CONN_STR"); val != "" {
		return val
	}
	return conn.URL
}

// GetURL returns the processed URL
func (conn *BaseConn) GetURL(newURL ...string) string {
	if len(newURL) > 0 {
		return newURL[0]
	}
	return conn.URL
}

// Init initiates the connection object & add default port if missing
func (conn *BaseConn) Init() (err error) {
	if conn.instance == nil {
		instance := Connection(conn)
		conn.instance = &instance
	}

	// sets all the available env vars as default if not already set
	for k, v := range env.Vars() {
		if conn.GetProp(k) == "" {
			conn.SetProp(k, v)
		}
	}

	if conn.defaultPort != 0 {
		connU, err := url.Parse(conn.URL)
		if err != nil {
			return g.Error(err, "could not parse connection URL")
		}
		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			conn.URL = strings.ReplaceAll(
				conn.URL, g.F("@%s", connHost),
				g.F("@%s:%d", connHost, conn.defaultPort),
			)
		}
	}

	if string(conn.Type) == "" {
		return g.Error("Could not determine database type.")
	}

	err = conn.LoadTemplates()
	if err != nil {
		return err
	}

	conn.schemata = Schemata{
		Databases: map[string]Database{},
	}

	conn.SetProp("connected", "false")
	return nil
}

func (conn *BaseConn) setContext(ctx context.Context, concurrency int) {
	c := g.NewContext(ctx, concurrency)
	if conn.context != nil {
		c.Map = conn.context.Map
	}
	conn.context = c
}

// Self returns the respective connection Instance
// This is useful to refer back to a subclass method
// from the superclass level. (Aka overloading)
func (conn *BaseConn) Self() Connection {
	return *conn.instance
}

// Db returns the sqlx db object
func (conn *BaseConn) Db() *sqlx.DB {
	return conn.db
}

// DbX returns the DbX object
func (conn *BaseConn) DbX() *DbX {
	return &DbX{db: conn.db}
}

// Tx returns the current sqlx tx object
func (conn *BaseConn) Tx() Transaction {
	return conn.tx
}

// GetType returns the type db object
func (conn *BaseConn) GetType() dbio.Type {
	return conn.Type
}

// Context returns the db context
func (conn *BaseConn) Context() *g.Context {
	return conn.context
}

// Schemata returns the Schemata object
func (conn *BaseConn) Schemata() Schemata {
	return conn.schemata
}

// Template returns the Template object
func (conn *BaseConn) Template() dbio.Template {
	return conn.template
}

// GetProp returns the value of a property
func (conn *BaseConn) GetProp(key ...string) string {
	conn.context.Mux.Lock()
	defer conn.context.Mux.Unlock()
	for _, k := range key {
		if val, ok := conn.properties[strings.ToLower(k)]; ok {
			return val
		}
	}
	return ""
}

// SetProp sets the value of a property
func (conn *BaseConn) SetProp(key string, val string) {
	conn.context.Mux.Lock()
	if conn.properties == nil {
		conn.properties = map[string]string{}
	}
	conn.properties[strings.ToLower(key)] = val
	conn.context.Mux.Unlock()
}

// PropArr returns an array of properties
func (conn *BaseConn) PropArr() []string {
	return conn.PropArrExclude() // don't exclude any key
}

func (conn *BaseConn) PropArrExclude(exclude ...string) []string {
	props := []string{}
	conn.context.Mux.Lock()
	for k, v := range conn.properties {
		if g.In(k, exclude...) {
			continue
		}
		props = append(props, g.F("%s=%s", k, v))
	}
	conn.context.Mux.Unlock()
	return props
}

// ReplaceProps used when reusing a connection
// since the provided props can change, this is used
// to delete old original props and set new ones
func (conn *BaseConn) ReplaceProps(newProps map[string]string) {
	oldPropKeys := []string{}

	g.Unmarshal(conn.GetProp("orig_prop_keys"), oldPropKeys)

	conn.context.Mux.Lock()
	for _, k := range oldPropKeys {
		delete(conn.properties, k)
	}

	// set new props
	for k, v := range newProps {
		conn.properties[k] = v
	}

	conn.context.Mux.Unlock()
}

// Props returns a map properties
func (conn *BaseConn) Props() map[string]string {
	m := map[string]string{}
	conn.context.Mux.Lock()
	for k, v := range conn.properties {
		m[k] = v
	}
	conn.context.Mux.Unlock()
	return m
}

// Kill kill the database connection
func (conn *BaseConn) Kill() error {
	conn.context.Cancel()
	conn.SetProp("connected", "false")
	return nil
}

// Connect connects to the database
func (conn *BaseConn) Connect(timeOut ...int) (err error) {
	var tryNum int

	to := 15
	if len(timeOut) > 0 && timeOut[0] != 0 {
		to = timeOut[0]
	}

	usePool = os.Getenv("USE_POOL") == "TRUE"
	// g.Trace("conn.Type: %s", conn.Type)
	// g.Trace("conn.URL: " + conn.Self().GetURL())
	if conn.Type == "" {
		return g.Error("Invalid URL? conn.Type needs to be specified")
	}

	connURL := conn.Self().ConnString()

	// start SSH Tunnel with SSH_TUNNEL prop
	if sshURL := conn.GetProp("SSH_TUNNEL"); sshURL != "" {

		connU, err := url.Parse(connURL)
		if err != nil {
			return g.Error(err, "could not parse connection URL for SSH forwarding")
		}

		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			connPort = conn.defaultPort
			connURL = strings.ReplaceAll(
				connURL, g.F("@%s", connHost),
				g.F("@%s:%d", connHost, connPort),
			)
		}

		localPort, err := iop.OpenTunnelSSH(connHost, connPort, sshURL, conn.GetProp("SSH_PRIVATE_KEY"), conn.GetProp("SSH_PASSPHRASE"))
		if err != nil {
			return g.Error(err, "could not connect to ssh tunnel server")
		}

		connURL = strings.ReplaceAll(
			connURL, g.F("@%s:%d", connHost, connPort),
			g.F("@127.0.0.1:%d", localPort),
		)
		g.Trace("new connection URL: " + conn.Self().GetURL(connURL))
		conn.SetProp("ssh_url", connURL) // set ssh url for 3rd party bulk loading
	}

	if conn.db == nil {
		connURL = conn.Self().GetURL(connURL)
		connPool.Mux.Lock()
		db, poolOk := connPool.Dbs[connURL]
		connPool.Mux.Unlock()
		g.Trace("connURL -> %s", connURL)

		if !usePool || !poolOk {
			db, err = sqlx.Open(getDriverName(conn), connURL)
			if err != nil {
				return g.Error(err, "Could not connect to DB: "+getDriverName(conn))
			}
		} else {
			conn.SetProp("POOL_USED", cast.ToString(poolOk))
		}

		conn.db = db

	retry:
		tryNum++

		// 15 sec timeout
		pingCtx, cancel := context.WithTimeout(conn.Context().Ctx, time.Duration(to)*time.Second)
		_ = cancel // lint complaint

		if conn.Type != dbio.TypeDbBigQuery {
			err = conn.db.PingContext(pingCtx)
			if err != nil {
				msg := ""
				switch conn.Type {
				case dbio.TypeDbPostgres, dbio.TypeDbRedshift:
					if val := conn.GetProp("sslmode"); !strings.EqualFold(val, "require") {
						msg = " (try adding `sslmode=require` or `sslmode=disable`)"
					}
				case dbio.TypeDbSnowflake:
					if strings.Contains(err.Error(), "000605") {
						// retry, snowflake server-side error
						// https://github.com/snowflakedb/gosnowflake/blob/master/restful.go#L30
						time.Sleep(5 * time.Second)
						g.Warn("Snowflake server-side error when trying to connect: %s\nRetrying connection.", err.Error())
						if tryNum < 5 {
							goto retry
						}
					}
				}
				return g.Error(err, "could not connect to database"+env.Clean(conn.Props(), msg))
			}
		}

		if !cast.ToBool(conn.GetProp("silent")) {
			g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
		}

		// add to pool after successful connection
		if usePool && !poolOk {
			connPool.Mux.Lock()
			connPool.Dbs[connURL] = db
			connPool.Mux.Unlock()

			// expire the connection from pool after 10 minutes of
			timer := time.NewTimer(time.Duration(10*60) * time.Second)
			go func() {
				select {
				case <-timer.C:
					connPool.Mux.Lock()
					delete(connPool.Dbs, connURL)
					connPool.Mux.Unlock()
				}
			}()
		}
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	return nil
}

func reconnectIfClosed(conn Connection) (err error) {
	// g.Warn("connected => %s", conn.GetProp("connected"))
	if conn.GetProp("connected") != "true" {
		if !cast.ToBool(conn.GetProp("silent")) {
			g.Debug("connection was closed, reconnecting")
		}
		err = conn.Self().Connect()
		if err != nil {
			err = g.Error(err, "Could not connect")
			return
		}
	}
	return
}

// Close closes the connection
func (conn *BaseConn) Close() error {
	var err error
	if conn.db != nil {
		err = conn.db.Close()
		conn.db = nil
		if err == nil && !cast.ToBool(conn.GetProp("silent")) {
			g.Debug(`closed "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
		}
	}
	if conn.sshClient != nil {
		conn.sshClient.Close()
		conn.sshClient = nil
	}
	conn.SetProp("connected", "false")
	return err
}

// LogSQL logs a query for debugging
func (conn *BaseConn) LogSQL(query string, args ...any) {
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")

	conn.Log = append(conn.Log, query)
	if len(conn.Log) > 9000 {
		conn.Log = conn.Log[1:]
	}

	env.LogSQL(conn.Props(), query, args...)
}

// GetGormConn returns the gorm db connection
func (conn *BaseConn) GetGormConn(config *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(getDialector(conn), config)
}

// GetTemplateValue returns the value of the path
func (conn *BaseConn) GetTemplateValue(path string) (value string) {
	return conn.Type.GetTemplateValue(path)
}

// LoadTemplates loads the appropriate yaml template
func (conn *BaseConn) LoadTemplates() (err error) {
	conn.template, err = conn.Type.Template()
	return
}

// StreamRecords the records of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRecords(sql string) (<-chan map[string]interface{}, error) {

	ds, err := conn.Self().StreamRows(sql)
	if err != nil {
		err = g.Error(err, "error in StreamRowsContext")
	}
	return ds.Records(), err
}

// BulkExportStream streams the rows in bulk
func (conn *BaseConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	g.Trace("BulkExportStream not implemented for %s", conn.Type)
	return conn.Self().StreamRows(table.Select(), g.M("columns", table.Columns))
}

// BulkImportStream import the stream rows in bulk
func (conn *BaseConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	g.Trace("BulkImportStream not implemented for %s", conn.Type)
	return conn.Self().InsertBatchStream(tableFName, ds)
}

// StreamRows the rows of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRows(sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	return conn.Self().StreamRowsContext(conn.Context().Ctx, sql, options...)
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BaseConn) StreamRowsContext(ctx context.Context, query string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	opts := getQueryOptions(options)

	Limit := uint64(0) // infinite
	if val := cast.ToUint64(opts["limit"]); val > 0 {
		Limit = val
	}

	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val
	}

	start := time.Now()
	if strings.TrimSpace(query) == "" {
		return ds, g.Error("Empty Query")
	}

	queryContext := g.NewContext(ctx)

	conn.LogSQL(query)
	var result *sqlx.Rows
	if conn.tx != nil {
		result, err = conn.tx.QueryContext(queryContext.Ctx, query)
	} else {
		result, err = conn.db.QueryxContext(queryContext.Ctx, query)
	}

	if err != nil && err.Error() == "EOF" {
		// for clickhouse
		err = nil
	} else if err != nil {
		queryContext.Cancel()
		if strings.Contains(query, noDebugKey) && !g.IsDebugLow() {
			return ds, g.Error(err, "SQL Error")
		}
		return ds, g.Error(err, "SQL Error for:\n"+query)
	}

	var colTypes []ColumnType
	if result != nil {
		dbColTypes, err := getColumnTypes(result)
		if err != nil {
			queryContext.Cancel()
			return ds, g.Error(err, "could not get column types")
		}

		colTypes = lo.Map(dbColTypes, func(ct *sql.ColumnType, i int) ColumnType {
			nullable, _ := ct.Nullable()
			length, _ := ct.Length()
			precision, scale, _ := ct.DecimalSize()

			if length == math.MaxInt64 {
				length = math.MaxInt32
			}

			dataType := ct.DatabaseTypeName()

			if conn.Type == dbio.TypeDbSnowflake {
				if dataType == "FIXED" && scale == 0 {
					dataType = "BIGINT"
				}
			}

			colType := ColumnType{
				Name:             ct.Name(),
				DatabaseTypeName: dataType,
				FetchedColumn:    fetchedColumns.GetColumn(ct.Name()),
				Length:           cast.ToInt(length),
				Precision:        cast.ToInt(precision),
				Scale:            cast.ToInt(scale),
				Nullable:         nullable,
				CT:               ct,
			}

			// if !strings.Contains(query, noDebugKey) {
			// 	g.Warn("%#v", colType)
			// }

			return colType
		})
	}

	conn.Data.Result = result
	conn.Data.SQL = query
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.Columns = SQLColumns(colTypes, conn) // type mapping logic !
	conn.Data.NoDebug = !strings.Contains(query, noDebugKey)

	g.Trace("query responded in %f secs", conn.Data.Duration)

	nextFunc := func(it *iop.Iterator) bool {
		if result == nil {
			return false
		} else if err = result.Err(); err != nil {
			// if any error occurs during iteration
			it.Context.CaptureErr(g.Error(err, "error during row iteration"))
			result.Close()
			return false
		} else if Limit > 0 && it.Counter >= Limit {
			result.Next()
			result.Close()
			return false
		}

		next := result.Next()
		if next {
			// add row
			it.Row, err = result.SliceScan()
			if err != nil {
				it.Context.CaptureErr(g.Error(err, "failed to scan"))
			} else {
				return true
			}
		}

		// Check for context cancellation when Next() returns false
		if ctxErr := queryContext.Ctx.Err(); ctxErr != nil {
			it.Context.CaptureErr(g.Error(ctxErr, "query context canceled"))
		} else if err = result.Err(); err != nil {
			// Double check result.Err() in case error occurred between previous check
			it.Context.CaptureErr(g.Error(err, "error after row iteration"))
		}

		result.Close()
		return false
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoDebug = strings.Contains(query, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()
	if !ds.NoDebug {
		// don't set metadata for internal queries
		ds.SetMetadata(conn.GetProp("METADATA"))
		conn.setTransforms(ds.Columns)
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}
	return
}

func (conn *BaseConn) setTransforms(columns iop.Columns) {
	colTransforms := map[string][]string{}

	// merge from existing
	if transforms := conn.GetProp("transforms"); transforms != "" {
		colTransformsExisting := map[string][]string{}
		g.Unmarshal(transforms, &colTransformsExisting)
		for k, v := range colTransformsExisting {
			if _, ok := colTransforms[k]; !ok {
				colTransforms[k] = v
			}
		}
	}

	// add new
	for _, col := range columns {
		key := strings.ToLower(col.Name)
		vals := colTransforms[key]
		switch conn.Type {
		case dbio.TypeDbAzure, dbio.TypeDbSQLServer, dbio.TypeDbAzureDWH:
			if strings.ToLower(col.DbType) == "uniqueidentifier" {
				if !lo.Contains(vals, "parse_uuid") {
					g.Debug(`setting transform "parse_ms_uuid" for column "%s"`, col.Name)
					colTransforms[key] = append([]string{"parse_ms_uuid"}, vals...)
				}
			}
		case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks:
			if strings.ToLower(col.DbType) == "bit" {
				// only add transform parse_bit if binary_to_hex or binary_to_decimal is not specified
				if !lo.Contains(vals, "binary_to_hex") && !lo.Contains(vals, "binary_to_decimal") {
					g.Debug(`setting transform "parse_bit" for column "%s"`, col.Name)
					colTransforms[key] = append([]string{"parse_bit"}, vals...)
				}
			}
		}
	}

	if len(colTransforms) > 0 {
		conn.SetProp("transforms", g.Marshal(colTransforms))
	}
}

// NewTransaction creates a new transaction
func (conn *BaseConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (Transaction, error) {
	context := g.NewContext(ctx)

	if len(options) == 0 {
		options = []*sql.TxOptions{&sql.TxOptions{}}
	}

	if conn.Db() == nil {
		return &BlankTransaction{}, nil
	}

	tx, err := conn.Db().BeginTxx(context.Ctx, options[0])
	if err != nil {
		return nil, g.Error(err, "could not begin Tx")
	}

	// a cloned connection object
	// this will enable to use transaction across all sub functions
	// URL := conn.GetProp("orig_url")
	// c, err := NewConnContext(context.Ctx, URL, conn.PropArr()...)
	// if err != nil {
	// 	return nil, g.Error(err, "could not clone conn object")
	// }

	// Tx := &BaseTransaction{Tx: tx, Conn: c, context: &context}
	// conn.tx = Tx

	// err = c.Connect()
	// if err != nil {
	// 	return nil, g.Error(err, "could not connect cloned conn object")
	// }

	Tx := &BaseTransaction{Tx: tx, Conn: conn.Self(), context: context}
	conn.tx = Tx

	return Tx, nil
}

// Begin starts a connection wide transaction
func (conn *BaseConn) Begin(options ...*sql.TxOptions) (err error) {
	return conn.BeginContext(conn.Context().Ctx, options...)
}

// BeginContext starts a connection wide transaction
func (conn *BaseConn) BeginContext(ctx context.Context, options ...*sql.TxOptions) (err error) {
	if conn.Db() == nil {
		return
	}

	g.Trace("begin")
	tx, err := conn.Self().NewTransaction(ctx, options...)
	if err != nil {
		return g.Error(err, "could not create transaction")
	}
	if tx != nil {
		conn.tx = tx
	}
	return
}

// Commit commits a connection wide transaction
func (conn *BaseConn) Commit() (err error) {
	if conn.tx == nil {
		return
	}

	tx := conn.tx
	select {
	case <-conn.tx.Context().Ctx.Done():
		err = conn.tx.Context().Err()
		conn.Rollback()
		return
	default:
		conn.tx = nil
		err = tx.Commit()
		// conn.tx.Connection().Close()
		if err != nil {
			return g.Error(err, "Could not commit")
		}
	}

	return nil
}

// Rollback rolls back a connection wide transaction
func (conn *BaseConn) Rollback() (err error) {
	if tx := conn.tx; tx != nil {
		conn.tx = nil
		err = tx.Rollback()
		// conn.tx.Connection().Close()
	}
	if err != nil {
		return g.Error(err, "Could not rollback")
	}
	return nil
}

// Prepare prepares the statement
func (conn *BaseConn) Prepare(query string) (stmt *sql.Stmt, err error) {
	if conn.tx != nil {
		stmt, err = conn.tx.Prepare(query)
	} else if conn.db != nil {
		stmt, err = conn.db.PrepareContext(conn.Context().Ctx, query)
	} else {
		err = g.Error("no connection instance")
	}
	if err != nil {
		err = g.Error(err, "could not prepare statement")
	}
	return
}

// Exec runs a sql query, returns `error`
func (conn *BaseConn) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	result, err = conn.Self().ExecContext(conn.Context().Ctx, sql, args...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// ExecMulti runs mutiple sql queries, returns `error`
func (conn *BaseConn) ExecMulti(sqls ...string) (result sql.Result, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	result, err = conn.Self().ExecMultiContext(conn.Context().Ctx, sqls...)
	if err != nil {
		err = g.Error(err, "Could not execute SQL")
	}
	return
}

// ExecContext runs a sql query with context, returns `error`
func (conn *BaseConn) ExecContext(ctx context.Context, q string, args ...interface{}) (result sql.Result, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	if strings.TrimSpace(q) == "" {
		g.Warn("Empty Query")
		return
	}

	if conn.tx != nil {
		result, err = conn.tx.ExecContext(ctx, q, args...)
		q = q + noDebugKey // just to not show twice the sql in error since tx does
	} else if conn.db != nil {
		conn.LogSQL(q, args...)
		result, err = conn.db.ExecContext(ctx, q, args...)
	} else {
		err = g.Error("no connection instance")
	}
	if err != nil {
		if strings.Contains(q, noDebugKey) {
			err = g.Error(err, "Error executing query [tx: %t]", conn.tx != nil)
		} else {
			err = g.Error(err, "Error executing [tx: %t] %s", conn.tx != nil, env.Clean(conn.Props(), q))
		}
	}
	return
}

// ExecMultiContext runs multiple sql queries with context, returns `error`
func (conn *BaseConn) ExecMultiContext(ctx context.Context, qs ...string) (result sql.Result, err error) {

	Res := Result{rowsAffected: 0}

	eG := g.ErrorGroup{}
	for _, q := range qs {
		for _, sql := range ParseSQLMultiStatements(q, conn.Type) {
			res, err := conn.Self().ExecContext(ctx, sql)
			if err != nil {
				eG.Capture(g.Error(err, "Error executing query"))
			} else {
				ra, _ := res.RowsAffected()
				g.Trace("RowsAffected: %d", ra)
				Res.rowsAffected = Res.rowsAffected + ra
			}
			delay := cast.ToInt64(conn.GetTemplateValue("variable.multi_exec_delay"))
			time.Sleep(time.Duration(delay) * time.Second)
		}
	}

	err = eG.Err()
	result = Res

	return
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func (conn *BaseConn) MustExec(sql string, args ...interface{}) (result sql.Result) {
	res, err := conn.Self().Exec(sql, args...)
	if err != nil {
		g.LogFatal(err, "fatal query error")
	}
	return res
}

// Query runs a sql query, returns `result`, `error`
func (conn *BaseConn) Query(sql string, options ...map[string]interface{}) (data iop.Dataset, err error) {
	return conn.Self().QueryContext(conn.Context().Ctx, sql, options...)
}

// QueryContext runs a sql query with ctx, returns `result`, `error`
func (conn *BaseConn) QueryContext(ctx context.Context, sql string, options ...map[string]interface{}) (data iop.Dataset, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		err = g.Error(err, "Could not reconnect")
		return
	}

	for _, sql := range ParseSQLMultiStatements(sql, conn.Type) {

		ds, err := conn.Self().StreamRowsContext(ctx, sql, options...)
		if err != nil {
			err = g.Error(err, "Error with StreamRows")
			return iop.Dataset{SQL: sql}, err
		}

		if len(data.Columns) == 0 {
			data, err = ds.Collect(0)
			if err != nil {
				return iop.Dataset{SQL: sql}, err
			}
		} else if len(data.Columns) == len(ds.Columns) {
			dataNew, err := ds.Collect(0)
			if err != nil {
				return iop.Dataset{SQL: sql}, err
			}
			data.Append(dataNew.Rows...)
		} else {
			return iop.Dataset{SQL: sql}, g.Error("Multi-statement queries returned different number of columns")
		}

	}

	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration
	g.Trace("query returned %d rows", len(data.Rows))
	return data, err
}

// SplitTableFullName retrusn the schema / table name
func SplitTableFullName(tableName string) (string, string) {
	var (
		schema string
		table  string
	)

	a := strings.Split(tableName, ".")
	if len(a) == 2 {
		schema = a[0]
		table = a[1]
	} else if len(a) == 1 {
		schema = ""
		table = a[0]
	}
	return schema, table
}

func (conn *BaseConn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error) {
	template, ok := templateMap[name]
	if !ok {
		err = g.Error("Could not find template %s", name)
		return
	}

	template = strings.TrimSpace(template) + noDebugKey
	sql, err := conn.ProcessTemplate(level, template, values)
	if err != nil {
		err = g.Error(err, "error processing template")
		return
	}
	return conn.Self().Query(sql)
}

// GetCount returns count of records
func (conn *BaseConn) GetCount(tableFName string) (int64, error) {
	sql := fmt.Sprintf(`select count(*) cnt from %s`, tableFName)
	data, err := conn.Self().Query(sql)
	if err != nil {
		return 0, err
	} else if len(data.Rows) == 0 || len(data.Rows[0]) == 0 {
		return 0, nil
	}
	return cast.ToInt64(data.Rows[0][0]), nil
}

// GetSchemas returns schemas
func (conn *BaseConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]
	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "schemas",
		g.M(),
	)
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *BaseConn) GetObjects(schema string, objectType string) (iop.Dataset, error) {
	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "objects",
		g.M("schema", schema, "object_type", objectType),
	)
}

// CurrentDatabase returns the name of the current database
func (conn *BaseConn) CurrentDatabase() (dbName string, err error) {
	data, err := conn.SubmitTemplate("single", conn.template.Metadata, "current_database", g.M())
	if err != nil {
		err = g.Error(err, "could not get current database")
	} else {
		dbName = cast.ToString(data.FirstVal())
	}

	return
}

// GetDatabases returns databases for given connection
func (conn *BaseConn) GetDatabases() (iop.Dataset, error) {
	// fields: [name]
	return conn.SubmitTemplate("single", conn.template.Metadata, "databases", g.M())
}

// GetTablesAndViews returns tables/views for given schema
func (conn *BaseConn) GetTablesAndViews(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	dataTables, err := conn.SubmitTemplate(
		"single", conn.template.Metadata, "tables",
		g.M("schema", schema),
	)
	if err != nil {
		return iop.Dataset{}, err
	}

	dataViews, err := conn.SubmitTemplate(
		"single", conn.template.Metadata, "views",
		g.M("schema", schema),
	)
	if err != nil {
		return iop.Dataset{}, err
	}

	// combine
	for _, row := range dataViews.Rows {
		dataTables.Append(row)
	}

	return dataTables, nil
}

// GetTables returns tables for given schema
func (conn *BaseConn) GetTables(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "tables",
		g.M("schema", schema),
	)
}

// GetViews returns views for given schema
func (conn *BaseConn) GetViews(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "views",
		g.M("schema", schema),
	)
}

// CommonColumns return common columns
func CommonColumns(colNames1 []string, colNames2 []string) (commCols []string) {
	commCols = []string{}
	cols2 := g.ArrMapString(colNames2, true)
	for _, col := range colNames1 {
		if _, ok := cols2[strings.ToLower(col)]; ok {
			commCols = append(commCols, col)
		}
	}
	return
}

// SQLColumns returns the columns from database ColumnType
func SQLColumns(colTypes []ColumnType, conn Connection) (columns iop.Columns) {
	columns = make(iop.Columns, len(colTypes))

	for i, colType := range colTypes {
		col := iop.Column{
			Name:     strings.ReplaceAll(colType.Name, ".", "_"),
			Position: i + 1,
			Type:     NativeTypeToGeneral(colType.Name, colType.DatabaseTypeName, conn),
			DbType:   colType.DatabaseTypeName,
			Sourced:  colType.IsSourced(),
		}

		// use pre-fetched column types for embedded databases since they rely
		// on output of external processes
		if fc := colType.FetchedColumn; fc != nil {
			if g.In(conn.GetType(), dbio.TypeDbDuckDb, dbio.TypeDbMotherDuck, dbio.TypeDbSQLite, dbio.TypeDbD1) && fc.Type != "" {
				col.Type = fc.Type
				col.Sourced = fc.Sourced
			}
			col.Constraint = fc.Constraint
		}

		if colType.Length > 0 {
			col.Stats.MaxLen = colType.Length
		}
		col.Stats.MaxDecLen = 0

		// if length is provided, set as string if less than 4000
		if col.Type == iop.TextType && colType.Length > 0 && colType.Length <= 4000 {
			col.Type = iop.StringType // set as string
		}

		// mark types that don't depend on length, precision or scale as sourced (inferred)
		if !g.In(col.Type, iop.DecimalType) {
			col.Sourced = true
		}

		if colType.IsSourced() || col.Sourced {
			if col.IsString() && g.In(conn.GetType(), dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbSnowflake, dbio.TypeDbOracle, dbio.TypeDbPostgres, dbio.TypeDbRedshift) {
				col.Sourced = true
				if colType.Length > 0 {
					col.DbPrecision = colType.Length
				}
			}
		}

		if col.IsString() {
			if col.DbPrecision == 0 && colType.Length > 0 && colType.Length < 999999 {
				col.DbPrecision = colType.Length
				col.Stats.MaxDecLen = colType.Length
				col.Sourced = true
			}
		}

		if col.IsDecimal() {
			if col.DbPrecision == 0 && colType.Precision < 99 {
				col.DbPrecision = colType.Precision
			}
			if col.DbScale == 0 && colType.Scale < 30 {
				col.DbScale = colType.Scale
				col.Stats.MaxDecLen = colType.Scale
			}
			if col.DbPrecision > 0 {
				col.Sourced = true
			}
		}

		// parse length, precision, scale manually specified in mapping
		col.SetLengthPrecisionScale()

		g.Trace(`database col => "%s" native_type=%s generic_type=%s length=%d precision=%d scale=%d sourced=%v`, col.Name, col.DbType, col.Type, col.Stats.MaxLen, col.DbPrecision, col.DbScale, col.Sourced)
		// g.Trace(`   colType=%#v`, colType)

		columns[i] = col

		// g.Warn("%s -> %s (%s) (sourced: %v)", col.Name, col.Type, col.DbType, col.Sourced)
	}
	return columns
}

func NativeTypeToGeneral(name, dbType string, conn Connection) (colType iop.ColumnType) {
	return iop.NativeTypeToGeneral(name, dbType, conn.GetType())
}

// GetSQLColumns return columns from a sql query result
func (conn *BaseConn) GetSQLColumns(table Table) (columns iop.Columns, err error) {
	if !table.IsQuery() {
		return conn.GetColumns(table.FullName())
	}

	limitSQL := table.Select(SelectOptions{Limit: 1})
	if table.IsProcedural() {
		limitSQL = table.Raw // don't wrap in limit
	}

	// get column types
	g.Trace("GetSQLColumns: %s", limitSQL)
	limitSQL = limitSQL + " /* GetSQLColumns */ " + noDebugKey
	ds, err := conn.Self().StreamRows(limitSQL, g.M("limit", 1))
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

// TableExists returns true if the table exists
func (conn *BaseConn) TableExists(table Table) (exists bool, err error) {

	colData, err := conn.SubmitTemplate(
		"single", conn.Template().Metadata, "columns",
		g.M("schema", table.Schema, "table", table.Name),
	)
	if err != nil && !strings.Contains(err.Error(), "does not exist") {
		return false, g.Error(err, "could not check table existence: "+table.FullName())
	}

	if len(colData.Rows) > 0 {
		exists = true
	}
	return exists, nil
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *BaseConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	if table.IsQuery() {
		return conn.GetSQLColumns(*table)
	}

	columns = iop.Columns{}
	colData, err := conn.Self().SubmitTemplate(
		"single", conn.template.Metadata, "columns",
		g.M("schema", table.Schema, "table", table.Name),
	)
	if err != nil {
		return columns, g.Error(err, "could not get list of columns for %s", table.FullName())
	} else if len(colData.Rows) == 0 {
		return columns, g.Error("did not find any columns for %s. Perhaps it does not exists, or user does not have read permission.", table.FullName())
	}

	// if fields provided, check if exists in table
	colMap := map[string]map[string]any{}
	for _, rec := range colData.Records() {
		colName := cast.ToString(rec["column_name"])
		colMap[strings.ToLower(colName)] = rec
	}

	var colTypes []ColumnType

	// if fields provided, filter, keep order
	if len(fields) > 0 {
		for _, field := range fields {
			rec, ok := colMap[strings.ToLower(field)]
			if !ok {
				err = g.Error(
					"provided field '%s' not found in table %s",
					strings.ToLower(field), table.FullName(),
				)
				return
			}

			if conn.Type == dbio.TypeDbSnowflake {
				rec["data_type"], rec["precision"], rec["scale"] = parseSnowflakeDataType(rec)
			}

			colTypes = append(colTypes, ColumnType{
				Name:             cast.ToString(rec["column_name"]),
				DatabaseTypeName: cast.ToString(rec["data_type"]),
				Precision:        cast.ToInt(rec["precision"]),
				Length:           cast.ToInt(cast.ToInt(rec["maximum_length"])),
				Scale:            cast.ToInt(rec["scale"]),
				Sourced:          true,
			})
		}
	} else {
		colTypes = lo.Map(colData.Records(), func(rec map[string]interface{}, i int) ColumnType {

			if conn.Type == dbio.TypeDbSnowflake {
				rec["data_type"], rec["precision"], rec["scale"] = parseSnowflakeDataType(rec)
			}

			return ColumnType{
				Name:             cast.ToString(rec["column_name"]),
				DatabaseTypeName: cast.ToString(rec["data_type"]),
				Precision:        cast.ToInt(rec["precision"]),
				Scale:            cast.ToInt(rec["scale"]),
				Length:           cast.ToInt(cast.ToInt(rec["maximum_length"])),
			}
		})

	}

	columns = SQLColumns(colTypes, conn)

	// add table info
	for i := range columns {
		columns[i].Database = table.Database
		columns[i].Schema = table.Schema
		columns[i].Table = table.Name
	}

	table.Columns = columns

	if len(columns) == 0 {
		err = g.Error("unable to obtain columns for " + table.FullName())
	}

	return
}

func (conn *BaseConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return columns, g.Error(err, "could not parse table name: "+tableFName)
	}

	return conn.Self().GetTableColumns(&table, fields...)
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *BaseConn) GetColumnsFull(tableFName string) (iop.Dataset, error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return iop.Dataset{}, g.Error(err, "could not parse table name: "+tableFName)
	}

	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "columns_full",
		g.M("schema", table.Schema, "table", table.Name),
	)
}

// GetPrimaryKeys returns primark keys for given table.
func (conn *BaseConn) GetPrimaryKeys(tableFName string) (iop.Dataset, error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return iop.Dataset{}, g.Error(err, "could not parse table name: "+tableFName)
	}

	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "primary_keys",
		g.M("schema", table.Schema, "table", table.Name),
	)
}

// GetIndexes returns indexes for given table.
func (conn *BaseConn) GetIndexes(tableFName string) (iop.Dataset, error) {
	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return iop.Dataset{}, g.Error(err, "could not parse table name: "+tableFName)
	}

	return conn.SubmitTemplate(
		"single", conn.template.Metadata, "indexes",
		g.M("schema", table.Schema, "table", table.Name),
	)
}

// GetMaxValue get the max value of a column
func (conn *BaseConn) GetMaxValue(table Table, colName string) (value any, maxCol iop.Column, err error) {
	sql := g.F(
		"select max(%s) as max_val from %s",
		conn.Quote(colName),
		table.FDQN(),
	)

	data, err := conn.Query(sql)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "exist") ||
			strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "unknown") ||
			strings.Contains(errMsg, "no such table") ||
			strings.Contains(errMsg, "invalid object") {
			// table does not exists, will be create later
			// set val to blank for full load
			return nil, maxCol, nil
		}
		err = g.Error(err, "could not get max value for "+colName)
		return
	}

	if len(data.Rows) == 0 || len(data.Rows[0]) == 0 {
		// table is empty
		// set val to blank for full load
		return nil, maxCol, nil
	}

	maxCol = data.Columns[0]
	value = lo.Ternary(cast.ToString(data.Rows[0][0]) == "", nil, data.Rows[0][0])

	return value, maxCol, nil
}

// GetDDL returns DDL for given table.
func (conn *BaseConn) GetDDL(tableFName string) (string, error) {

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return "", g.Error(err, "could not parse table name: "+tableFName)
	}

	ddlCol := cast.ToInt(conn.template.Variable["ddl_col"])
	ddlArr := []string{}
	data, err := conn.SubmitTemplate(
		"single", conn.template.Metadata, "ddl_view",
		g.M("schema", table.Schema, "table", table.Name),
	)

	for _, row := range data.Rows {
		ddlArr = append(ddlArr, cast.ToString(row[ddlCol]))
	}

	ddl := strings.TrimSpace(strings.Join(ddlArr, "\n"))
	if err == nil && ddl != "" {
		return ddl, err
	}

	data, err = conn.SubmitTemplate(
		"single", conn.template.Metadata, "ddl_table",
		g.M("schema", table.Schema, "table", table.Name),
	)
	if err != nil {
		return "", err
	}

	for _, row := range data.Rows {
		ddlArr = append(ddlArr, cast.ToString(row[ddlCol]))
	}

	ddl = strings.TrimSpace(strings.Join(ddlArr, "\n"))
	return ddl, nil
}

// CreateTemporaryTable creates a temp table based on provided columns
func (conn *BaseConn) CreateTemporaryTable(tableName string, cols iop.Columns) (err error) {
	table, err := ParseTableName(tableName, conn.Type)
	if err != nil {
		return g.Error(err, "Could not parse table name: "+tableName)
	}

	// generate ddl
	tableDDL, err := conn.GenerateDDL(table, cols.Dataset(), true)
	if err != nil {
		return g.Error(err, "Could not generate DDL for "+tableName)
	}

	// execute ddl
	_, err = conn.ExecMulti(tableDDL)
	if err != nil {
		return g.Error(err, "Could not create table "+tableName)
	}

	return
}

// CreateTable creates a new table based on provided columns
// `tableName` should have 'schema.table' format
func (conn *BaseConn) CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error) {

	table, err := ParseTableName(tableName, conn.Type)
	if err != nil {
		return g.Error(err, "Could not parse table name: "+tableName)
	}

	// check table existence
	exists, err := conn.TableExists(table)
	if err != nil {
		return g.Error(err, "Error checking table "+tableName)
	} else if exists {
		return nil
	}

	// generate ddl
	if tableDDL == "" {
		tableDDL, err = conn.GenerateDDL(table, cols.Dataset(), false)
		if err != nil {
			return g.Error(err, "Could not generate DDL for "+tableName)
		}
	}

	// execute ddl
	_, err = conn.ExecMulti(tableDDL)
	if err != nil {
		return g.Error(err, "Could not create table "+tableName)
	}

	return
}

// DropTable drops given table.
func (conn *BaseConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		sql := g.R(conn.template.Core["drop_table"], "table", tableName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errMsg := strings.ToLower(err.Error())
			errIgnoreWord := strings.ToLower(conn.Template().Variable["error_ignore_drop_table"])
			if !(errIgnoreWord != "" && strings.Contains(errMsg, errIgnoreWord)) {
				return g.Error(err, "Error for "+sql)
			}
			g.Debug("table %s does not exist", tableName)
		} else {
			g.Debug("table %s dropped", tableName)
		}
	}
	return nil
}

// DropView drops given view.
func (conn *BaseConn) DropView(viewNames ...string) (err error) {

	for _, viewName := range viewNames {
		sql := g.R(conn.template.Core["drop_view"], "view", viewName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_view"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return g.Error(err, "Error for "+sql)
			}
			g.Debug("view %s does not exist", viewName)
		} else {
			g.Debug("view %s dropped", viewName)
		}
	}
	return nil
}

// Import imports `data` into `tableName`
func (conn *BaseConn) Import(data iop.Dataset, tableName string) error {

	return nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *BaseConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {

	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	values := g.M()
	if schemaName != "" {
		values["schema"] = schemaName
	}
	if len(tableNames) > 0 && !(tableNames[0] == "" && len(tableNames) == 1) {
		tablesQ := []string{}
		for _, tableName := range tableNames {
			if strings.TrimSpace(tableName) == "" {
				continue
			}
			tablesQ = append(tablesQ, `'`+tableName+`'`)
		}
		if len(tablesQ) > 0 {
			values["tables"] = strings.Join(tablesQ, ", ")
		}
	}

	currDatabase := conn.Type.String()
	currDbData, err := conn.SubmitTemplate("single", conn.template.Metadata, "current_database", g.M())
	if err == nil {
		currDatabase = cast.ToString(currDbData.FirstVal())
	}

	var data iop.Dataset
	switch level {
	case SchemataLevelSchema:
		data, err = conn.Self().GetSchemas()
	case SchemataLevelTable:
		data, err = conn.Self().GetTablesAndViews(schemaName)
	case SchemataLevelColumn:
		data, err = conn.SubmitTemplate(
			"single", conn.template.Metadata, "schemata",
			values,
		)
	}
	if err != nil {
		return schemata, g.Error(err, "Could not get schemata at %s level", level)
	}

	schemas := map[string]Schema{}
	for _, rec := range data.Records() {
		schemaName = cast.ToString(rec["schema_name"])
		tableName := cast.ToString(rec["table_name"])
		columnName := cast.ToString(rec["column_name"])
		dataType := cast.ToString(rec["data_type"])

		// if any of the names contains a period, skip. This messes with the keys
		if strings.Contains(tableName, ".") ||
			strings.Contains(schemaName, ".") ||
			strings.Contains(columnName, ".") {
			continue
		}

		switch rec["is_view"].(type) {
		case int64, float64:
			if cast.ToInt64(rec["is_view"]) == 0 {
				rec["is_view"] = false
			} else {
				rec["is_view"] = true
			}
		default:
			rec["is_view"] = cast.ToBool(rec["is_view"])
		}

		schema := Schema{
			Name:     schemaName,
			Database: currDatabase,
			Tables:   map[string]Table{},
		}

		if _, ok := schemas[strings.ToLower(schema.Name)]; ok {
			schema = schemas[strings.ToLower(schema.Name)]
		}

		var table Table
		if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
			table = Table{
				Name:     tableName,
				Schema:   schemaName,
				Database: currDatabase,
				IsView:   cast.ToBool(rec["is_view"]),
				Columns:  iop.Columns{},
				Dialect:  conn.GetType(),
			}

			if _, ok := schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]; ok {
				table = schemas[strings.ToLower(schema.Name)].Tables[strings.ToLower(tableName)]
			}
		}

		if level == SchemataLevelColumn {
			column := iop.Column{
				Name:     columnName,
				Type:     NativeTypeToGeneral(columnName, dataType, conn),
				Table:    tableName,
				Schema:   schemaName,
				Database: currDatabase,
				Position: cast.ToInt(data.Sp.ProcessVal(rec["position"])),
				DbType:   dataType,
			}

			table.Columns = append(table.Columns, column)
		}

		if g.In(level, SchemataLevelTable, SchemataLevelColumn) {
			schema.Tables[strings.ToLower(tableName)] = table
		}

		schemas[strings.ToLower(schema.Name)] = schema
	}

	schemata.Databases[strings.ToLower(currDatabase)] = Database{
		Name:    currDatabase,
		Schemas: schemas,
	}

	return schemata, nil
}

// RunAnalysis runs an analysis
func (conn *BaseConn) RunAnalysis(analysisName string, values map[string]interface{}) (data iop.Dataset, err error) {
	sql, err := conn.Self().GetAnalysis(analysisName, values)
	if err != nil {
		err = g.Error(err, "could not run analysis %s", analysisName)
		return
	}
	return conn.Self().Query(sql)
}

// ProcessTemplate processes a template SQL text at a given level
func (conn *BaseConn) ProcessTemplate(level, text string, values map[string]interface{}) (sql string, err error) {

	sqls := []string{}

	getColumns := func(columns []string) (cols iop.Columns) {
		cols = iop.NewColumnsFromFields(columns...)
		if len(cols) == 0 {
			// get all columns then
			var table Table
			tableFName := g.F("%s.%s", values["schema"], values["table"])
			table, err = ParseTableName(tableFName, conn.GetType())
			if err != nil {
				err = g.Error(err, "could not parse table name")
				return
			}

			cols, err = conn.GetColumns(table.FullName())
			if err != nil {
				err = g.Error(err, "could not obtain table columns")
			}
		}
		return
	}

	text, err = g.ExecuteTemplate(text, values)
	if err != nil {
		err = g.Error(err, "error execute template")
		return
	}

	switch level {
	case "single":
		sql = g.Rm(text, values)
	case "schema-union":
		schemas, ok := values["schemas"]
		if !ok {
			err = g.Error("missing 'schemas' key")
		} else {
			for _, schema := range cast.ToSlice(schemas) {
				values["schema"] = schema
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "table-union":
		tableFNames, ok := values["tables"]
		if !ok {
			err = g.Error("missing 'tables' key")
		} else {
			for _, tableFName := range cast.ToStringSlice(tableFNames) {
				table, err := ParseTableName(tableFName, conn.Type)
				if err != nil {
					return "", g.Error(err, "could not parse table name: "+tableFName)
				}
				values["schema"] = table.Schema
				values["table"] = table.Name
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "column-union":
		columns, ok := values["columns"]
		if !ok {
			err = g.Error("missing 'columns' key")
		} else {
			for _, col := range getColumns(cast.ToStringSlice(columns)) {
				values["column"] = col.Name
				values["field"] = col.Name
				values["type"] = col.Type
				sqls = append(sqls, g.Rm(text, values))
			}
			sql = strings.Join(sqls, "\nUNION ALL\n")
		}
	case "column-expres":
		columns, ok1 := values["columns"]
		expr, ok2 := values["expr"]
		if !ok1 || !ok2 {
			err = g.Error("missing 'columns' or 'expr' key")
		} else {
			if !strings.Contains(cast.ToString(expr), "{column}") {
				err = g.Error("\"expr\" value must contain '{column}'")
				return
			}
			exprs := []string{}
			for _, col := range getColumns(cast.ToStringSlice(columns)) {
				values["column"] = col.Name
				values["field"] = col.Name
				values["type"] = col.Type
				exprVal := g.Rm(cast.ToString(expr), values)
				exprs = append(exprs, exprVal)
			}
			values["columns_sql"] = strings.Join(exprs, ", ")
			sql = g.Rm(text, values)
		}
	}

	return
}

// GetAnalysis runs an analysis
func (conn *BaseConn) GetAnalysis(analysisName string, values map[string]interface{}) (sql string, err error) {
	template, ok := conn.template.Analysis[analysisName]
	if !ok {
		err = g.Error("did not find Analysis: " + analysisName)
		return
	}

	switch analysisName {
	case "table_count":
		sql, err = conn.ProcessTemplate("table-union", template, values)
	case "field_chars", "field_stat", "field_stat_group", "field_stat_deep":
		if _, ok := values["columns"]; !ok {
			values["columns"] = values["fields"]
		}
		sql, err = conn.ProcessTemplate("column-union", template, values)
	case "distro_field_date_wide", "fill_cnt_group_field":
		sql, err = conn.ProcessTemplate("column-expres", template, values)
	default:
		sql, err = conn.ProcessTemplate("single", template, values)
	}

	if err != nil {
		err = g.Error(err, "could not get analysis %s", analysisName)
		return
	}

	return
}

// CastColumnForSelect casts to the correct target column type
func (conn *BaseConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) string {
	return conn.Self().Quote(srcCol.Name)
}

// CastColumnsForSelect cast the source columns into the target Column types
func (conn *BaseConn) CastColumnsForSelect(srcColumns iop.Columns, tgtColumns iop.Columns) []string {
	selectExprs := []string{}

	tgtFields := map[string]iop.Column{}
	// compare type, length/precision, scale
	for _, tgtCol := range tgtColumns {
		tgtFields[strings.ToLower(tgtCol.Name)] = tgtCol
	}

	for _, srcCol := range srcColumns {
		tgtCol, ok := tgtFields[strings.ToLower(srcCol.Name)]
		if !ok {
			continue
		}

		// don't normalize name, leave as is
		selectExpr := conn.Self().Quote(srcCol.Name)

		if !strings.EqualFold(srcCol.DbType, tgtCol.DbType) {
			g.Debug(
				"inserting %s [%s] into %s [%s]",
				srcCol.Name, srcCol.DbType, tgtCol.Name, tgtCol.DbType,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		} else if srcCol.DbPrecision > tgtCol.DbPrecision {
			g.Debug(
				"target precision / length is smaller when inserting %s [%s(%d)] into %s [%s(%d)]",
				srcCol.Name, srcCol.DbType, srcCol.DbPrecision,
				tgtCol.Name, tgtCol.DbType, tgtCol.DbPrecision,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		}

		// add alias
		qName := conn.Self().Quote(srcCol.Name)
		selectExprs = append(selectExprs, g.F("%s as %s", selectExpr, qName))
	}

	return selectExprs
}

// ValidateColumnNames verifies that source fields are present in the target table
// It will return quoted field names as `newColNames`, the same length as `colNames`
func (conn *BaseConn) ValidateColumnNames(tgtCols iop.Columns, colNames []string) (newCols iop.Columns, err error) {

	mismatches := []string{}
	for _, colName := range colNames {
		newCol := tgtCols.GetColumn(colName)
		if newCol == nil || newCol.Name == "" {
			// src field is missing in tgt field
			mismatches = append(mismatches, g.F("source field '%s' is missing in target table", colName))
			continue
		}
		newCols = append(newCols, *newCol)
	}

	if len(mismatches) > 0 {
		err = g.Error("column names mismatch: %s", strings.Join(mismatches, "\n"))
	}

	g.Trace("insert target fields: " + strings.Join(newCols.Names(), ", "))

	return
}

// InsertStream inserts a stream into a table
func (conn *BaseConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertStream(conn.Self(), nil, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not insert into %s", tableFName)
	}
	return
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BaseConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	count, err = InsertBatchStream(conn.Self(), conn.tx, tableFName, ds)
	if err != nil {
		err = g.Error(err, "Could not batch insert into %s", tableFName)
	}
	return
}

// bindVar return proper bind var according to https://jmoiron.github.io/sqlx/#bindvars
func (conn *BaseConn) bindVar(i int, field string, n int, c int) string {
	return g.R(
		conn.template.Variable["bind_string"],
		"i", cast.ToString(i),
		"field", field,
		"n", cast.ToString(n),
		"c", cast.ToString(c),
	)
}

// Unquote removes quotes to the field name
func (conn *BaseConn) Unquote(field string) string {
	return conn.Type.Unquote(field)
}

// Quote adds quotes to the field name
func (conn *BaseConn) Quote(field string) string {
	return conn.Type.Quote(field)
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *BaseConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {

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

	statement := g.R(
		"insert into {table} ({fields}) values  {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	g.Trace("insert statement: "+strings.Split(statement, ") values  ")[0]+")"+" x %d", numRows)
	return statement
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *BaseConn) Upsert(srcTable string, tgtTable string, primKeys []string) (rowAffCnt int64, err error) {
	var cnt int64
	if conn.tx != nil {
		cnt, err = Upsert(conn.Self(), conn.tx, srcTable, tgtTable, primKeys)
	} else {
		cnt, err = Upsert(conn.Self(), nil, srcTable, tgtTable, primKeys)
	}
	if err != nil {
		err = g.Error(err, "could not upsert")
	}
	return cast.ToInt64(cnt), err
}

// SwapTable swaps two table
func (conn *BaseConn) SwapTable(srcTable string, tgtTable string) (err error) {

	tgtTableTemp := tgtTable + "_tmp" + g.RandString(g.AlphaRunesLower, 2)
	conn.DropTable(tgtTableTemp)

	sql := g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTable,
		"new_table", tgtTableTemp,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+tgtTable)
	}

	sql = g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", srcTable,
		"new_table", tgtTable,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+srcTable)
	}

	sql = g.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTableTemp,
		"new_table", srcTable,
	)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "could not rename table "+tgtTableTemp)
	}

	return
}

// GetNativeType returns the native column type from generic
func (conn *BaseConn) GetNativeType(col iop.Column) (nativeType string, err error) {
	var ct iop.ColumnTyping
	if val := conn.GetProp("column_typing"); val != "" {
		g.Unmarshal(val, &ct)
	}
	return col.GetNativeType(conn.GetType(), ct)
}

// GenerateDDL genrate a DDL based on a dataset
func (conn *BaseConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

	if !data.Inferred || data.SafeInference {
		if len(data.Columns) > 0 && data.Columns[0].Stats.TotalCnt == 0 && data.Columns[0].Type == "" {
			g.Warn("Generating DDL from 0 rows. Will use string for unknown types.")
		}
		data.InferColumnTypes()
	}
	columnsDDL := []string{}
	columns := data.Columns

	// re-order columns for starrocks (keys first)
	if g.In(conn.GetType(), dbio.TypeDbStarRocks) {
		orderedColumns := iop.Columns{}

		for _, col := range columns {
			if col.IsKeyType(iop.PrimaryKey) || col.IsKeyType(iop.DuplicateKey) || col.IsKeyType(iop.HashKey) || col.IsKeyType(iop.AggregateKey) || col.IsKeyType(iop.UniqueKey) {
				orderedColumns = append(orderedColumns, col)
			}
		}

		for _, col := range columns {
			if !g.In(col.Name, orderedColumns.Names()...) {
				orderedColumns = append(orderedColumns, col)
			}
		}

		columns = orderedColumns
	}

	// if temporary, and SQL Server, set factor for nvarchar
	if temporary && conn.GetType().IsSQLServer() {
		// modify column typing to increase factor for temp table
		// see https://github.com/slingdata-io/sling-cli/issues/554
		origColTyping := conn.GetProp("column_typing")
		defer func() {
			conn.SetProp("column_typing", origColTyping) // set back after DDL generation
		}()

		var ct iop.ColumnTyping
		if origColTyping != "" {
			g.Unmarshal(origColTyping, &ct)
		}

		cts := ct.String
		if cts == nil {
			cts = &iop.StringColumnTyping{}
		}
		if cts.LengthFactor < 2 {
			cts.LengthFactor = 2 // set to allow buffer for replacement
			cts.MinLength = 50   // set to allow buffer for replacement
			cts.Note = "for temporary table buffer"
		}
		ct.String = cts

		// marshal for DDL generation
		conn.SetProp("column_typing", g.Marshal(ct))
	}

	for _, col := range columns {
		// convert from general type to native type
		nativeType, err := conn.Self().GetNativeType(col)
		if err != nil {
			return "", g.Error(err, "no native mapping")
		}

		if !data.NoDebug {
			g.Trace("%s - %s %s", col.Name, col.Type, g.Marshal(col.Stats))
		}

		columnDDL := conn.Self().Quote(col.Name) + " " + nativeType
		columnsDDL = append(columnsDDL, columnDDL)
	}

	createTemplate := conn.template.Core["create_table"]
	if ctt := conn.template.Core["create_temporary_table"]; ctt != "" && temporary {
		createTemplate = ctt
	}

	if table.DDL != "" {
		createTemplate = table.DDL
	}

	ddl := g.R(
		createTemplate,
		"table", table.FullName(),
		"col_types", strings.Join(columnsDDL, ",\n  "),
	)

	return ddl, nil
}

// BulkImportFlow imports the streams rows in bulk concurrently using channels
func (conn *BaseConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	// g.Trace("BulkImportFlow not implemented for %s", conn.GetType())
	df.Context.SetConcurrencyLimit(conn.Context().Wg.Limit)
	// df.Context.SetConcurrencyLimit(1) // safer for now, fails with too many files

	doImport := func(tableFName string, ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()

		// cast bool columns (mysql, sqlite)
		// ds = conn.castDsBoolColumns(ds)

		var cnt uint64
		if conn.GetProp("use_bulk") == "false" {
			cnt, err = conn.Self().InsertBatchStream(tableFName, ds)
		} else {
			cnt, err = conn.Self().BulkImportStream(tableFName, ds)
		}
		count += cnt
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not bulk import"))
		} else if err = ds.Err(); err != nil {
			df.Context.CaptureErr(g.Error(err, "could not bulk import"))
		}
	}

	// concurrent imports does not work very well
	for ds := range df.StreamCh {
		df.Context.Wg.Write.Add()
		doImport(tableFName, ds)
	}

	// safer for now, fails with too many files
	// df.Context.Wg.Write.Add()
	// doImport(tableFName, iop.MergeDataflow(df))

	df.Context.Wg.Write.Wait()

	return count, df.Err()
}

// BulkExportFlow creates a dataflow from a sql query
func (conn *BaseConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {

	g.Trace("BulkExportFlow not implemented for %s", conn.GetType())
	if UseBulkExportFlowCSV {
		return conn.BulkExportFlowCSV(table)
	}

	df = iop.NewDataflowContext(conn.Context().Ctx)

	dsCh := make(chan *iop.Datastream)

	go func() {
		defer close(dsCh)
		dss := []*iop.Datastream{}
		for _, table := range []Table{table} {
			ds, err := conn.Self().BulkExportStream(table)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "Error running query"))
				return
			}
			// split the ds into multiple streams (output will not be in order)
			dss = append(dss, ds.Split()...)
		}

		for _, ds := range dss {
			dsCh <- ds
		}

	}()

	go df.PushStreamChan(dsCh)

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, g.Error(err)
	}

	return df, nil
}

// BulkExportFlowCSV creates a dataflow from a sql query, using CSVs
func (conn *BaseConn) BulkExportFlowCSV(table Table) (df *iop.Dataflow, err error) {

	columns, err := conn.Self().GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	exportCtx := g.NewContext(conn.Context().Ctx)
	df = iop.NewDataflowContext(exportCtx.Ctx)
	dsCh := make(chan *iop.Datastream)

	unload := func(table Table, pathPart string) {
		defer df.Context.Wg.Read.Done()
		defer close(dsCh)
		fileReadyChn := make(chan filesys.FileReady, 10000)
		ds, err := conn.Self().BulkExportStream(table)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error running query"))
			df.Context.Cancel()
			return
		}

		fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
		if err != nil {
			exportCtx.CaptureErr(g.Error(err, "Unable to create Local file sys Client"))
			ds.Context.Cancel()
			return
		}

		sqlDf, err := iop.MakeDataFlow(ds)
		if err != nil {
			exportCtx.CaptureErr(g.Error(err, "Unable to create data flow"))
			ds.Context.Cancel()
			return
		}

		go func() {
			_, err := fs.Self().WriteDataflowReady(sqlDf, pathPart, fileReadyChn, iop.DefaultStreamConfig())
			if err != nil {
				exportCtx.CaptureErr(g.Error(err, "Unable to write to file: "+pathPart))
				ds.Context.Cancel()
				return
			}
		}()

		for file := range fileReadyChn {
			// when the file is ready, push to dataflow
			nDs, err := iop.ReadCsvStream(file.Node.Path())
			if err != nil {
				exportCtx.CaptureErr(g.Error(err, "Unable to read stream: "+file.Node.Path()))
				ds.Context.Cancel()
				df.Context.Cancel()
				return
			}
			nDs.Defer(func() { env.RemoveAllLocalTempFile(file.Node.Path()) })
			dsCh <- nDs
		}
	}

	folderPath := path.Join(env.GetTempFolder(), "sling", "stream", string(conn.GetType()), g.NowFileStr())

	go func() {
		defer df.Close()
		for i, table := range []Table{table} {
			pathPart := fmt.Sprintf("%s/sql%02d", folderPath, i+1)
			df.Context.Wg.Read.Add()
			go unload(table, pathPart)
		}

		// wait until all nDs are pushed to close
		df.Context.Wg.Read.Wait()
	}()

	go df.PushStreamChan(dsCh)

	// wait for first ds to start streaming.
	err = df.WaitReady()
	if err != nil {
		return df, g.Error(err)
	}

	df.AddColumns(columns, true) // overwrite types so we don't need to infer
	df.Inferred = true

	return
}

// GenerateUpsertSQL returns a sql for upsert
func (conn *BaseConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTemplate := conn.Template().Core["upsert"]
	if sqlTemplate == "" {
		return "", g.Error("Did not find upsert in template for %s", conn.GetType())
	}

	sql = g.R(
		sqlTemplate,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"pk_fields", upsertMap["pk_fields"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)

	return
}

// GenerateUpsertExpressions returns a map with needed expressions
func (conn *BaseConn) GenerateUpsertExpressions(srcTable string, tgtTable string, pkFields []string) (exprs map[string]string, err error) {

	srcColumns, err := conn.GetColumns(srcTable)
	if err != nil {
		err = g.Error(err, "could not get columns for "+srcTable)
		return
	}
	tgtColumns, err := conn.GetColumns(tgtTable)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	pkCols, err := conn.ValidateColumnNames(tgtColumns, pkFields)
	if err != nil {
		err = g.Error(err, "PK columns mismatch")
		return
	}

	var pkEqualFields, srcPkFields, tgtPkFields []string
	pkFields = conn.Type.QuoteNames(pkCols.Names()...)
	pkFieldMap := map[string]string{}
	for _, pkField := range pkCols.Names() {
		// don't normalize, use raw name
		srcCol := srcColumns.GetColumn(pkField)
		tgtCol := tgtColumns.GetColumn(pkField)
		srcField := conn.Quote(srcCol.Name)
		tgtField := conn.Quote(tgtCol.Name)

		srcPkFields = append(srcPkFields, srcField)
		tgtPkFields = append(tgtPkFields, tgtField)

		pkEqualField := g.F("src.%s = tgt.%s", srcField, tgtField)
		pkEqualFields = append(pkEqualFields, pkEqualField)
		pkFieldMap[pkField] = ""
	}

	tgtCols, err := conn.ValidateColumnNames(tgtColumns, srcColumns.Names())
	if err != nil {
		err = g.Error(err, "columns mismatch")
		return
	}

	tgtFields := conn.Type.QuoteNames(tgtCols.Names()...)
	setFields := []string{}
	insertFields := []string{}
	placeholderFields := []string{}
	for _, tgtColName := range tgtCols.Names() {
		srcCol := g.PtrVal(srcColumns.GetColumn(tgtColName)) // should be found
		tgtCol := tgtColumns.GetColumn(tgtColName)
		if tgtCol == nil {
			return nil, g.Error("did not find target column: %s (has %s)", tgtColName, g.Marshal(tgtColumns.Names()))
		}

		// don't normalize, use raw name
		srcColNameQ := conn.Quote(srcCol.Name)
		tgtColNameQ := conn.Quote(tgtCol.Name)

		colExpr := conn.Self().CastColumnForSelect(srcCol, *tgtCol)

		insertFields = append(insertFields, tgtColNameQ)

		phExpr := strings.ReplaceAll(colExpr, srcColNameQ, g.F("ph.%s", srcColNameQ))
		placeholderFields = append(placeholderFields, phExpr)
		if _, ok := pkFieldMap[tgtCol.Name]; !ok {
			// is not a pk field
			setSrcExpr := strings.ReplaceAll(colExpr, srcColNameQ, g.F("src.%s", srcColNameQ))
			setField := g.F("%s = %s", tgtColNameQ, setSrcExpr)
			setFields = append(setFields, setField)
		}
	}

	// cast into the correct type
	srcFields := conn.Self().CastColumnsForSelect(srcColumns, tgtColumns)

	exprs = map[string]string{
		"src_tgt_pk_equal":   strings.Join(pkEqualFields, " and "),
		"src_upd_pk_equal":   strings.ReplaceAll(strings.Join(pkEqualFields, ", "), "tgt.", "upd."),
		"src_fields":         strings.Join(srcFields, ", "),
		"tgt_fields":         strings.Join(tgtFields, ", "),
		"insert_fields":      strings.Join(insertFields, ", "),
		"pk_fields":          strings.Join(pkFields, ", "),
		"src_pk_fields":      strings.Join(srcPkFields, ", "),
		"tgt_pk_fields":      strings.Join(tgtPkFields, ", "),
		"set_fields":         strings.Join(setFields, ", "),
		"placeholder_fields": strings.Join(placeholderFields, ", "),
	}

	return
}

// GetColumnStats analyzes the table and returns the column statistics
func (conn *BaseConn) GetColumnStats(tableName string, fields ...string) (columns iop.Columns, err error) {

	table, err := ParseTableName(tableName, conn.GetType())
	if err != nil {
		return columns, g.Error(err, "could not parse table name")
	}

	tableColumns, err := conn.Self().GetColumns(table.FullName())
	if err != nil {
		err = g.Error(err, "could not obtain columns data")
		return
	}

	generalTypes := map[string]string{}
	statFields := []string{}
	for _, col := range tableColumns {
		generalTypes[strings.ToLower(col.Name)] = string(col.Type)
		statFields = append(statFields, col.Name)
	}

	if len(fields) == 0 {
		fields = statFields
	}

	// run analysis field_stat_len
	m := g.M("table", table.FullName(), "fields", fields)
	data, err := conn.Self().RunAnalysis("field_stat_len", m)
	if err != nil {
		err = g.Error(err, "could not analyze table")
		return
	}

	columns = iop.Columns{}
	for _, rec := range data.Records() {
		colName := cast.ToString(rec["field"])
		column := iop.Column{
			Name: colName,
			Type: iop.ColumnType(generalTypes[strings.ToLower(colName)]),
			Stats: iop.ColumnStats{
				Min:       cast.ToInt64(rec["f_min"]),
				Max:       cast.ToInt64(rec["f_max"]),
				MaxDecLen: cast.ToInt(rec["f_max_dec_len"]),
				MinLen:    cast.ToInt(rec["f_min_len"]),
				MaxLen:    cast.ToInt(rec["f_max_len"]),
				NullCnt:   cast.ToInt64(rec["f_null_cnt"]),
				TotalCnt:  cast.ToInt64(rec["tot_cnt"]),
			},
		}

		if column.Stats.MaxDecLen == 0 {
			column.Stats.MaxDecLen = ddlMinDecScale
		}
		columns = append(columns, column)
	}

	return

}

// GetOptimizeTableStatements analyzes the table and alters the table with
// the columns data type based on its analysis result
// if table is missing, it is created with a new DDl
// Hole in this: will truncate data points, since it is based
// only on new data being inserted... would need a complete
// stats of the target table to properly optimize.
func GetOptimizeTableStatements(conn Connection, table *Table, newColumns iop.Columns, isTemp bool) (ok bool, ddlParts []string, err error) {
	if missing := table.Columns.GetMissing(newColumns...); len(missing) > 0 {
		return false, ddlParts, g.Error("missing columns: %#v\ntable.Columns: %#v\nnewColumns: %#v", missing.Names(), table.Columns.Names(), newColumns.Names())
	} else if g.In(conn.GetType(), dbio.TypeDbSQLite, dbio.TypeDbD1) {
		return false, ddlParts, nil
	}

	newColumnsMap := lo.KeyBy(newColumns, func(c iop.Column) string {
		return strings.ToLower(c.Name)
	})

	var oldCols, colsChanging iop.Columns
	for i, col := range table.Columns {
		newCol, ok := newColumnsMap[strings.ToLower(col.Name)]
		if !ok {
			continue
		} else if col.Type == newCol.Type {
			continue
		}
		msg := g.F("optimizing existing '%s' (%s) vs new '%s' (%s) => ", col.Name, col.Type, newCol.Name, newCol.Type)
		switch {
		case col.Type.IsDecimal() && newCol.Type.IsDecimal():
			continue
		case col.Type.IsDatetime() && newCol.Type.IsDatetime():
			newCol.Type = iop.TimestampType
		case col.Type.IsDatetime() && newCol.Type.IsDate():
			newCol.Type = iop.TimestampType
		case col.Type.IsInteger() && newCol.Type.IsDecimal():
			newCol.Type = iop.DecimalType
		case col.Type.IsInteger() && newCol.Type.IsFloat():
			newCol.Type = iop.FloatType
		case col.Type.IsDecimal() && newCol.Type.IsInteger():
			newCol.Type = iop.DecimalType
		case col.Type.IsInteger() && newCol.Type == iop.BigIntType:
			newCol.Type = iop.BigIntType
		case col.Type == iop.BigIntType && newCol.Type.IsInteger():
			newCol.Type = iop.BigIntType
		case col.Type == iop.SmallIntType && newCol.Type == iop.IntegerType:
			newCol.Type = iop.IntegerType
		case col.Type == iop.IntegerType && newCol.Type == iop.SmallIntType:
			newCol.Type = iop.IntegerType
		case isTemp && col.IsString() && newCol.HasNulls() && (newCol.IsDatetime() || newCol.IsDate() || newCol.IsNumber() || newCol.IsBool()):
			// use new type
		case col.Type == iop.TextType || newCol.Type == iop.TextType:
			newCol.Type = iop.TextType
		default:
			newCol.Type = iop.StringType
		}

		if col.Type == newCol.Type {
			continue
		}

		g.Debug(msg + string(newCol.Type))

		oldNativeType, err := conn.GetNativeType(col)
		if err != nil {
			return false, ddlParts, g.Error(err, "no native mapping for `%s`", newCol.Type)
		}

		newNativeType, err := conn.GetNativeType(newCol)
		if err != nil {
			return false, ddlParts, g.Error(err, "no native mapping for `%s`", newCol.Type)
		}

		if oldNativeType == newNativeType {
			continue
		}

		table.Columns[i].Type = newCol.Type
		table.Columns[i].DbType = newNativeType
		colsChanging = append(colsChanging, table.Columns[i])
		oldCols = append(oldCols, col)
	}

	if len(colsChanging) == 0 {
		return false, ddlParts, nil
	}

	// if column is part of index, drop it
	for _, index := range table.Indexes(colsChanging) {
		ddlParts = append(ddlParts, index.DropDDL())
	}

	for index, col := range colsChanging {
		// to safely modify the column type
		colNameTemp := g.RandSuffix(col.Name+"_", 3)

		// for starrocks update
		pKey := table.Columns.GetKeys(iop.PrimaryKey).Names()
		if len(pKey) == 0 {
			pKey = table.Columns.GetKeys(iop.UniqueKey).Names()
		}
		if len(pKey) == 0 && g.In(conn.GetType(), dbio.TypeDbStarRocks) {
			err = g.Error("unable to modify starrocks schema without primary key or unique key")
			return
		}
		for i, key := range pKey {
			pKey[i] = conn.Self().Quote(key)
		}

		// add new column with new type
		ddlParts = append(ddlParts, g.R(
			conn.GetTemplateValue("core.add_column"),
			"table", table.FullName(),
			"column", conn.Self().Quote(colNameTemp),
			"type", col.DbType,
		))

		// update set to cast old values
		oldColCasted := conn.Self().CastColumnForSelect(oldCols[index], col)
		if oldColCasted == conn.Self().Quote(col.Name) {
			oldColCasted = g.R(
				conn.GetTemplateValue("function.cast_as"),
				"field", conn.Self().Quote(col.Name),
				"type", col.DbType,
			)
		}

		// for starrocks
		fields := append(table.Columns.Names(), colNameTemp)
		fields = conn.GetType().QuoteNames(fields...) // add quotes
		updatedFields := append(
			conn.GetType().QuoteNames(table.Columns.Names()...), // add quotes
			oldColCasted)

		ddlParts = append(ddlParts, g.R(
			conn.GetTemplateValue("core.update"),
			"table", table.FullName(),
			"set_fields", g.R(
				"{temp_column} = {old_column_casted}",
				"temp_column", conn.Self().Quote(colNameTemp),
				"old_column_casted", oldColCasted,
			),
			"fields", strings.Join(fields, ", "),
			"updated_fields", strings.Join(updatedFields, ", "),
			"pk_fields_equal", "1=1",
		))

		// drop old column
		ddlParts = append(ddlParts, g.R(
			conn.GetTemplateValue("core.drop_column"),
			"table", table.FullName(),
			"column", conn.Self().Quote(col.Name),
		))

		// rename new column to old name
		tableName := table.FullName()
		oldColName := conn.Self().Quote(colNameTemp)
		newColName := conn.Self().Quote(col.Name)

		if g.In(conn.GetType(), dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH) {
			tableName = conn.Unquote(table.FullName())
			oldColName = colNameTemp
			newColName = col.Name
		}

		// for starrocks
		otherNames := lo.Filter(table.Columns.Names(), func(name string, i int) bool {
			return !strings.EqualFold(name, col.Name)
		})
		fields = append(otherNames, col.Name)
		fields = conn.GetType().QuoteNames(fields...) // add quotes
		updatedFields = append(otherNames, colNameTemp)
		updatedFields = conn.GetType().QuoteNames(updatedFields...) // add quotes

		ddlParts = append(ddlParts, g.R(
			conn.GetTemplateValue("core.rename_column"),
			"table", tableName,
			"column", oldColName,
			"new_column", newColName,
			"new_type", col.DbType,
			"fields", strings.Join(fields, ", "),
			"updated_fields", strings.Join(updatedFields, ", "),
		))
	}

	// re-create index
	for _, index := range table.Indexes(colsChanging) {
		ddlParts = append(ddlParts, index.CreateDDL())
	}

	return true, ddlParts, nil
}

func (conn *BaseConn) OptimizeTable(table *Table, newColumns iop.Columns, isTemp ...bool) (ok bool, err error) {
	IsTemp := false
	if len(isTemp) > 0 {
		IsTemp = isTemp[0]
	}
	ok, ddlParts, err := GetOptimizeTableStatements(conn, table, newColumns, IsTemp)
	if err != nil {
		return ok, err
	}

	_, err = conn.ExecMulti(ddlParts...)
	if err != nil {
		return false, g.Error(err, "could not alter columns on table "+table.FullName())
	}

	return ok, nil
}

// CompareChecksums compares the checksum values from the database side
// to the checkum values from the StreamProcessor
func (conn *BaseConn) CompareChecksums(tableName string, columns iop.Columns) (err error) {

	// recover from panic
	defer func() {
		if r := recover(); r != nil {
			err = g.Error(g.F("panic occurred! %#v\n%s", r, string(debug.Stack())))
		}
	}()

	if g.In(conn.Type, dbio.TypeDbIceberg) {
		g.Warn("cannot compare checksums for %s", conn.Type)
		return nil
	}

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
	fieldsMap := g.ArrMapString(cols.Names(), true)
	g.Debug("comparing checksums %s", g.Marshal(tColumns.Types()))

	exprs := []string{}
	colMap := lo.KeyBy(columns, func(c iop.Column) string { return strings.ToLower(c.Name) })
	colChecksum := map[string]uint64{}
	exprMap := map[string]string{}
	for _, col := range columns {
		colChecksum[strings.ToLower(col.Name)] = col.Stats.Checksum
		if _, ok := fieldsMap[strings.ToLower(col.Name)]; !ok {
			continue // making sure it is a common column
		}

		expr := ""
		switch {
		case col.Type == iop.JsonType:
			expr = conn.GetTemplateValue("function.checksum_json")
		case col.IsString():
			expr = conn.GetTemplateValue("function.checksum_string")
		case col.IsInteger():
			expr = conn.GetTemplateValue("function.checksum_integer")
		case col.IsFloat():
			expr = conn.GetTemplateValue("function.checksum_decimal")
		case col.IsDecimal():
			expr = conn.GetTemplateValue("function.checksum_decimal")
		case col.IsDate():
			expr = conn.GetTemplateValue("function.checksum_date")
			if expr == "" {
				expr = conn.GetTemplateValue("function.checksum_datetime")
			}
		case col.IsDatetime():
			expr = conn.GetTemplateValue("function.checksum_datetime")
		case col.IsBool():
			expr = conn.GetTemplateValue("function.checksum_boolean")
		default:
			expr = "0"
		}
		colName := fieldsMap[strings.ToLower(col.Name)]
		expr = g.R(expr, "field", conn.Self().Quote(cast.ToString(colName)))
		exprs = append(exprs, g.F("sum(%s) as %s", expr, conn.Self().Quote(cast.ToString(colName))))
		exprMap[strings.ToLower(col.Name)] = g.F("sum(%s)", expr)
	}

	sql := g.F(
		"select %s from %s ",
		strings.Join(exprs, ", "),
		tableName,
	)

	data, err := conn.Self().Query(sql)
	if err != nil {
		return g.Error(err, "error running CompareChecksums query")
	} else if len(data.Rows) == 0 {
		return g.Error("error running CompareChecksums query. No Rows returns")
	} else if len(data.Rows[0]) != len(data.Columns) {
		return g.Error("error running CompareChecksums query. Row vs Column size mismatch (%d != %d)", len(data.Rows[0]), len(data.Columns))
	}

	eg := g.ErrorGroup{}
	for i, col := range data.Columns {
		refCol := colMap[strings.ToLower(col.Name)]
		checksum1 := colChecksum[strings.ToLower(col.Name)]
		checksum2, err2 := cast.ToUint64E(data.Rows[0][i])
		if refCol.Stats.TotalCnt == 0 || err2 != nil {
			// skip
		} else if checksum1 != checksum2 {
			if refCol.Type != col.Type {
				// don't compare
			} else if refCol.IsString() && g.In(conn.GetType(), dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH) && checksum2 >= checksum1 {
				// datalength can return higher counts since it counts bytes
			} else if refCol.IsDatetime() && conn.GetType() == dbio.TypeDbSQLite && checksum1/1000 == checksum2 {
				// sqlite can only handle timestamps up to milliseconds
			} else if refCol.IsDatetime() && conn.GetType() == dbio.TypeDbD1 && checksum1/1000 == checksum2 {
				// d1 can only handle timestamps up to milliseconds
			} else if refCol.IsInteger() && conn.GetType() == dbio.TypeDbD1 && checksum1 > 4796827514234845000 && checksum1-checksum2 < 500 && checksum1-checksum2 > 0 {
				// some weird sum issue with big numbers in d1
			} else if checksum1 > 1500000000000 && ((checksum2-checksum1) == 1 || (checksum1-checksum2) == 1) {
				// something micro seconds are off by 1 msec
			} else {
				eg.Add(g.Error("checksum failure for %s [%s | %s] (sling-side vs db-side): %d != %d -- (%s)\n%s", col.Name, col.Type, col.DbType, checksum1, checksum2, exprMap[strings.ToLower(col.Name)], g.Marshal(data.Records(true)[0])))
			}
		}
	}

	return eg.Err()
}

// Info returns connection information
func (conn *BaseConn) Info() (ci ConnInfo) {
	U, err := net.NewURL(conn.GetURL())
	if err != nil {
		return
	}

	ci = ConnInfo{
		Host:      U.Hostname(),
		Port:      U.Port(),
		Database:  strings.ReplaceAll(U.Path(), "/", ""),
		User:      U.Username(),
		Password:  U.Password(),
		Schema:    U.GetParam("schema"),
		Warehouse: U.GetParam("warehouse"),
		Role:      U.GetParam("role"),
		URL:       U,
	}
	return
}

func (conn *BaseConn) credsProvided(provider string) bool {
	if provider == "AWS" {
		if conn.GetProp("AWS_SECRET_ACCESS_KEY", "SECRET_ACCESS_KEY") != "" || conn.GetProp("AWS_ACCESS_KEY_ID", "ACCESS_KEY_ID") != "" {
			return true
		}
	}
	if provider == "AZURE" {
		if conn.GetProp("AZURE_ACCOUNT") != "" || conn.GetProp("AZURE_CONTAINER") != "" || (conn.GetProp("AZURE_SAS_SVC_URL") == "" && conn.GetProp("AZURE_CONN_STR") == "") {
			return true
		}
	}
	return false
}

func (conn *BaseConn) makeTlsConfig() (tlsConfig *tls.Config, err error) {
	if cast.ToBool(conn.GetProp("tls")) {
		tlsConfig = &tls.Config{}
	}

	// legacy only handles files
	certificate := conn.GetProp("cert_file")
	certificateKey := conn.GetProp("cert_key_file")
	certificateCA := conn.GetProp("cert_ca_file")

	// processValue handles files or values
	processValue := func(val string) (string, error) {
		// check if file exists
		if _, err := os.Stat(val); err == nil {
			bytes, err := os.ReadFile(val)
			if err != nil {
				return "", g.Error(err, "Failed to load file: %s", val)
			}
			val = string(bytes)
		}

		// check if it is a pem encoded certificate
		val = strings.TrimSpace(val)
		if strings.HasPrefix(val, "-----BEGIN") {
			return val, nil
		}

		// not a file or pem encoded certificate
		return "", g.Error("invalid certificate value or file path")
	}

	if val := conn.GetProp("certificate"); val != "" {
		certificate, err = processValue(val)
		if err != nil {
			return nil, g.Error(err, "Failed to load client certificate")
		}
		conn.SetProp("certificate", certificate) // overwrite
	}

	if val := conn.GetProp("certificate_key"); val != "" {
		certificateKey, err = processValue(val)
		if err != nil {
			return nil, g.Error(err, "Failed to load client certificate key")
		}
		conn.SetProp("certificate_key", certificateKey) // overwrite
	}

	if val := conn.GetProp("certificate_ca"); val != "" {
		certificateCA, err = processValue(val)
		if err != nil {
			return nil, g.Error(err, "Failed to load CA certificate")
		}
		conn.SetProp("certificate_ca", certificateCA) // overwrite
	}

	if certificateKey != "" && certificate != "" {
		cert, err := tls.LoadX509KeyPair(certificate, certificateKey)
		if err != nil {
			return nil, g.Error(err, "Failed to load client certificate")
		}

		tlsConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}

		if certificateCA != "" {
			caCert, err := os.ReadFile(certificateCA)
			if err != nil {
				return nil, g.Error(err, "Failed to load CA certificate")
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			ok := tlsConfig.RootCAs.AppendCertsFromPEM(caCert)
			if !ok {
				return nil, g.Error("Failed to parse PEM file")
			}
		}
	}

	return
}

// settingMppBulkImportFlow sets settings for MPP databases type
// for BulkImportFlow
func settingMppBulkImportFlow(conn Connection, compressor iop.CompressorType) {
	if val := conn.GetProp("FILE_MAX_ROWS"); val == "" {
		conn.SetProp("FILE_MAX_ROWS", "500000")
	}

	if val := conn.GetProp("FILE_MAX_BYTES"); val == "" {
		conn.SetProp("FILE_MAX_BYTES", "50000000")
	}

	compr := strings.ToLower(conn.GetProp("COMPRESSION"))
	if g.In(compr, "", string(iop.AutoCompressorType)) {
		conn.SetProp("COMPRESSION", string(compressor))
	}

	conn.SetProp("PARALLEL", "true")
}

func (conn *BaseConn) AddMissingColumns(table Table, newCols iop.Columns) (ok bool, err error) {
	cols, err := conn.GetColumns(table.FullName())
	if err != nil {
		err = g.Error(err, "could not obtain table columns for adding %s", g.Marshal(newCols.Names()))
		return
	}

	missing := cols.GetMissing(newCols...)

	// generate alter commands
	for _, col := range missing {
		nativeType, err := conn.GetNativeType(col)
		if err != nil {
			return false, g.Error(err, "no native mapping")
		}
		sql := g.R(
			conn.Template().Core["add_column"],
			"table", table.FullName(),
			"column", conn.Self().Quote(col.Name),
			"type", nativeType,
		)

		g.Debug("adding new column: %s", col.Name)
		_, err = conn.Exec(sql)
		if err != nil {
			return false, g.Error(err, "could not add column %s to table %s", col.Name, table.FullName())
		}

		if g.In(conn.GetType(), dbio.TypeDbBigQuery) {
			// avoid rate limit error
			time.Sleep(2 * time.Second)
		}
	}

	return len(missing) > 0, nil
}

// TestPermissions tests the needed permissions in a given connection
func TestPermissions(conn Connection, tableName string) (err error) {

	type testObj struct {
		Title string
		SQL   string
		Skip  []dbio.Type
	}

	col := iop.Column{Name: "col1", Type: "integer"}
	nativeType := conn.Template().GeneralTypeMap[string(col.Type)]

	// drop table if exists
	err = conn.DropTable(tableName)
	if err != nil {
		return g.Error(err, "failed testing permissions: Drop table")
	}

	tests := []testObj{

		// Create table
		{
			Title: "Create a test table",
			SQL: g.R(
				conn.GetTemplateValue("core.create_table"),
				"table", tableName,
				"col_types", col.Name+" "+nativeType,
			),
		},

		// Insert
		{
			Title: "Insert into a test table",
			SQL: g.R(
				conn.GetTemplateValue("core.insert"),
				"table", tableName,
				"fields", col.Name,
				"values", "1",
			),
		},

		// Update
		{
			Title: "Update the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.update"),
				"table", tableName,
				"set_fields", g.F("%s = 2", col.Name),
				"pk_fields_equal", "1=1",
			),
		},

		// Delete
		{
			Title: "Delete from the test table",
			SQL: g.R(
				"delete from {table} where 1=0",
				"table", tableName,
			),
		},

		// Truncate
		{
			Title: "Truncate the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.truncate_table"),
				"table", tableName,
			),
		},

		// Drop table
		{
			Title: "Drop the test table",
			SQL: g.R(
				conn.GetTemplateValue("core.drop_table"),
				"table", tableName,
			),
		},
	}

	for _, test := range tests {
		for _, skipType := range test.Skip {
			if conn.GetType() == skipType {
				continue
			}
		}

		_, err = conn.Exec(test.SQL)
		if err != nil {
			return g.Error(err, "failed testing permissions: %s", test.Title)
		}
	}

	return
}

func CopyFromS3(conn Connection, tableFName, s3Path string) (err error) {
	AwsID := conn.GetProp("AWS_ACCESS_KEY_ID")
	AwsAccessKey := conn.GetProp("AWS_SECRET_ACCESS_KEY")
	if AwsID == "" || AwsAccessKey == "" {
		err = g.Error("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to copy to snowflake from S3")
		return
	}

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		return g.Error(err, "could not parse table name")
	}

	sql := g.R(
		conn.Template().Core["copy_from_s3"],
		"table", table.FullName(),
		"s3_path", s3Path,
		"aws_access_key_id", AwsID,
		"aws_secret_access_key", AwsAccessKey,
	)

	g.Debug("copying into %s from s3", conn.GetType())
	g.Debug("url: " + s3Path)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error:\n"+env.Clean(conn.Props(), sql))
	}

	return nil
}

func getAzureToken(conn Connection) (azToken string, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL' to copy to snowflake from azure")
		return
	}

	azSasURLArr := strings.Split(azSasURL, "?")
	if len(azSasURLArr) != 2 {
		err = g.Error(
			g.Error("Invalid provided AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	azToken = azSasURLArr[1]
	return
}

// CopyFromAzure uses the Snowflake COPY INTO Table command from Azure
// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
func CopyFromAzure(conn Connection, tableFName, azPath string) (err error) {
	azToken, err := getAzureToken(conn)
	if err != nil {
		return g.Error(err)
	}

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		return g.Error(err, "could not parse table name")
	}

	sql := g.R(
		conn.Template().Core["copy_from_azure"],
		"table", table.FullName(),
		"azure_path", azPath,
		"azure_sas_token", azToken,
	)

	g.Debug("copying into %s from azure", conn.GetType())
	g.Debug("url: " + azPath)
	conn.SetProp("azure_sas_token", azToken)
	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "SQL Error:\n"+env.Clean(conn.Props(), sql))
	}

	return nil
}

func Clone(conn Connection) (newConn Connection, err error) {
	props := g.MapToKVArr(conn.Base().Props())
	newConn, err = NewConn(conn.GetURL(), props...)
	if err != nil {
		err = g.Error(err, "could not clone database connection")
	}
	return
}

func getQueryOptions(options []map[string]interface{}) (opts map[string]interface{}) {
	opts = g.M()
	if len(options) > 0 && options[0] != nil {
		opts = options[0]
	}
	return opts
}

// ChangeColumnTypeViaAdd swaps a new column with the old in order to change the type
// need to use this with snowflake when changing from date to string, or number to string
func ChangeColumnTypeViaAdd(conn Connection, table Table, col iop.Column) (err error) {
	// Add new Column with _ suffix with correct type
	newCol := iop.Column{
		Name:     col.Name + "_",
		Position: col.Position,
		Type:     col.Type,
	}

	err = conn.Begin()
	if err != nil {
		err = g.Error(err, "could not being tx for column new type")
		return
	}

	_, err = conn.AddMissingColumns(table, iop.Columns{newCol})
	if err != nil {
		err = g.Error(err, "could not add new column")
		return
	}

	// UPDATE set new_col value equal old column value
	colAsString := g.R(
		conn.GetTemplateValue("function.cast_to_string"),
		"field", conn.Quote(col.Name),
	)
	sql := g.F("update %s set %s = %s", table.FDQN(), conn.Quote(newCol.Name), colAsString)
	_, err = conn.Exec(sql)
	if err != nil {
		err = g.Error(err, "could not set new column value")
		return
	}

	// Drop old column
	sql = g.R(
		conn.GetTemplateValue("core.drop_column"),
		"table", table.FDQN(),
		"column", conn.Quote(col.Name),
	)
	_, err = conn.Exec(sql)
	if err != nil {
		err = g.Error(err, "could not drop old column")
		return
	}

	// rename new column name to old column name
	sql = g.R(
		conn.GetTemplateValue("core.rename_column"),
		"table", table.FDQN(),
		"column", conn.Quote(col.Name),
		"new_column", conn.Quote(newCol.Name),
	)
	_, err = conn.Exec(sql)
	if err != nil {
		err = g.Error(err, "could not rename new column to old")
		return
	}

	err = conn.Commit()
	if err != nil {
		err = g.Error(err, "could not commit column new type")
		return
	}

	return
}
