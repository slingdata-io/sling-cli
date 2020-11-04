package database

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/slingdata-io/sling/core/env"

	_ "github.com/denisenkom/go-mssqldb"
	h "github.com/flarco/gutil"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/slingdata-io/sling/core/local"
	_ "github.com/snowflakedb/gosnowflake"
	_ "github.com/solcates/go-sql-bigquery"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// DbType is an string type for enum for the database Type
type DbType string

const (
	// PostgresDbType is postgres
	PostgresDbType DbType = "postgres"
	// RedshiftDbType is redshift
	RedshiftDbType DbType = "redshift"
	// MySQLDbType is mysql
	MySQLDbType DbType = "mysql"
	// OracleDbType is oracle
	OracleDbType DbType = "oracle"
	// BigQueryDbType is big query
	BigQueryDbType DbType = "bigquery"
	// SnowflakeDbType is snowflake
	SnowflakeDbType DbType = "snowflake"
	// SQLiteDbType is sqlite
	SQLiteDbType DbType = "sqlite"
	// SQLServerDbType is MS SQL Server
	SQLServerDbType DbType = "sqlserver"
	// AzureSQLDbType is MS SQL Server on Azure
	AzureSQLDbType DbType = "azuresql"
	// AzureDWHDbType is MS SQL Server on Azure
	AzureDWHDbType DbType = "azuredwh"
)

// Connection is the Base interface for Connections
type Connection interface {
	Self() Connection
	Init() error
	Connect(timeOut ...int) error
	Kill() error
	Close() error
	GetType() DbType
	GetGormConn(config *gorm.Config) (*gorm.DB, error)
	LoadTemplates() error
	GetURL(newURL ...string) string
	CopyDirect(tableFName string, srcFile iop.DataConn) (cnt uint64, ok bool, err error)
	StreamRows(sql string, limit ...int) (*iop.Datastream, error)
	StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error)
	BulkExportStream(sql string) (*iop.Datastream, error)
	BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	BulkExportFlow(sqls ...string) (*iop.Dataflow, error)
	BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error)
	Begin() error
	Commit() error
	Rollback() error
	Query(sql string, limit ...int) (iop.Dataset, error)
	QueryContext(ctx context.Context, sql string, limit ...int) (iop.Dataset, error)
	GenerateDDL(tableFName string, data iop.Dataset) (string, error)
	Quote(string) string
	Unquote(string) string
	GenerateInsertStatement(tableName string, fields []string, numRows int) string
	GenerateUpsertExpressions(srcTable string, tgtTable string, pkFields []string) (exprs map[string]string, err error)
	DropTable(...string) error
	DropView(...string) error
	InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error)
	Db() *sqlx.DB
	Exec(sql string, args ...interface{}) (result sql.Result, err error)
	ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error)
	MustExec(sql string, args ...interface{}) (result sql.Result)
	Schemata() Schemata
	Template() Template
	SetProp(string, string)
	GetProp(string) string
	PropArr() []string
	GetTemplateValue(path string) (value string)
	Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error)
	SwapTable(srcTable string, tgtTable string) (err error)
	RenameTable(table string, newTable string) (err error)
	Context() *h.Context
	setContext(ctx context.Context)

	StreamRecords(sql string) (<-chan map[string]interface{}, error)
	GetDDL(string) (string, error)
	GetSchemaObjects(string) (Schema, error)
	GetSchemas() (iop.Dataset, error)
	GetTables(string) (iop.Dataset, error)
	GetViews(string) (iop.Dataset, error)
	GetSQLColumns(sqls ...string) (columns []iop.Column, err error)
	GetColumns(tableFName string, fields ...string) ([]iop.Column, error)
	GetColumnStats(tableName string, fields ...string) (columns []iop.Column, err error)
	GetPrimaryKeys(string) (iop.Dataset, error)
	GetIndexes(string) (iop.Dataset, error)
	GetColumnsFull(string) (iop.Dataset, error)
	GetCount(string) (uint64, error)
	RunAnalysis(string, map[string]interface{}) (iop.Dataset, error)
	RunAnalysisTable(string, ...string) (iop.Dataset, error)
	RunAnalysisField(string, string, ...string) (iop.Dataset, error)
	CastColumnForSelect(srcColumn iop.Column, tgtColumn iop.Column) string
	CastColumnsForSelect(srcColumns []iop.Column, tgtColumns []iop.Column) []string
	ValidateColumnNames(tgtColName []string, colNames []string, quote bool) (newColNames []string, err error)
	OptimizeTable(tableName string, columns []iop.Column) (err error)
	CompareChecksums(tableName string, columns []iop.Column) (err error)
	BaseURL() string
}

// BaseConn is a database connection
type BaseConn struct {
	Connection
	URL         string
	Type        DbType // the type of database for sqlx: postgres, mysql, sqlite
	db          *sqlx.DB
	tx          *sqlx.Tx
	Data        iop.Dataset
	defaultPort int
	instance    *Connection
	context     h.Context
	template    Template
	schemata    Schemata
	properties  map[string]string
	sshClient   *iop.SSHClient
}

// Table represents a schemata table
type Table struct {
	Name       string `json:"name"`
	FullName   string `json:"full_name"`
	IsView     bool   `json:"is_view"` // whether is a view
	Columns    []iop.Column
	ColumnsMap map[string]*iop.Column
}

// Schema represents a schemata schema
type Schema struct {
	Name   string `json:"name"`
	Tables map[string]Table
}

// Schemata contains the full schema for a connection
type Schemata struct {
	Schemas map[string]Schema
	Tables  map[string]*Table // all tables with full name lower case (schema.table)
}

// Template is a database YAML template
type Template struct {
	Core           map[string]string
	Metadata       map[string]string
	Analysis       map[string]string
	Function       map[string]string `yaml:"function"`
	GeneralTypeMap map[string]string `yaml:"general_type_map"`
	NativeTypeMap  map[string]string `yaml:"native_type_map"`
	NativeStatsMap map[string]bool   `yaml:"native_stat_map"`
	Variable       map[string]string
}

// Pool is a pool of connections
type Pool struct {
	Dbs map[string]*sqlx.DB
	Mux sync.Mutex
}

var (
	// UseBulkExportFlowCSV to use BulkExportFlowCSV
	UseBulkExportFlowCSV = false

	sampleSize = 900

	ddlDefDecScale  = 6
	ddlDefDecLength = 20

	ddlMaxDecLength = 30
	ddlMaxDecScale  = 9

	ddlMinDecScale = 4

	filePathStorageSlug = "temp"

	connPool = Pool{Dbs: map[string]*sqlx.DB{}}
)

func init() {
	if os.Getenv("SLING_SAMPLE_SIZE") != "" {
		sampleSize = cast.ToInt(os.Getenv("SLING_SAMPLE_SIZE"))
	}
	if os.Getenv("SLING_FILEPATH_SLUG") != "" {
		filePathStorageSlug = os.Getenv("SLING_FILEPATH_SLUG")
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
	if newURL := os.Getenv(strings.TrimLeft(URL, "$")); newURL != "" {
		URL = newURL
	} else if home, err := local.GetHome(); err != nil && home != nil {
		conns, err := home.Profile.ListConnections(true)
		h.LogError(err)
		for _, dc := range conns {
			if dc.ID == URL {
				URL = dc.URL
				props = h.MapToKVArr(dc.VarsS())
			}
		}
	}

	if !strings.Contains(URL, ":") {
		err := fmt.Errorf("could not detect URL")
		return nil, h.Error(err, "invalid URL")
	}

	// Add / Extract Props from URL
	u, err := url.Parse(URL)
	if err == nil {
		for _, propStr := range strings.Split(u.RawQuery, `&`) {
			props = append(props, propStr)
			for _, p := range env.EnvVars() {
				if strings.HasPrefix(propStr, p+"=") {
					URL = strings.ReplaceAll(URL, propStr, "")
				}
			}
		}
	} else {
		return nil, h.Error(err, "could not parse URL")
	}

	if strings.HasPrefix(URL, "postgres") {
		if strings.Contains(URL, "redshift.amazonaws.com") {
			conn = &RedshiftConn{URL: URL}
		} else {
			conn = &PostgresConn{URL: URL}
		}
	} else if strings.HasPrefix(URL, "redshift") {
		conn = &RedshiftConn{URL: URL}
	} else if strings.HasPrefix(URL, "sqlserver:") {
		conn = &MsSQLServerConn{URL: URL}
	} else if strings.HasPrefix(URL, "mysql:") {
		conn = &MySQLConn{URL: URL}
	} else if strings.HasPrefix(URL, "oracle:") {
		conn = &OracleConn{URL: URL}
	} else if strings.HasPrefix(URL, "bigquery:") {
		conn = &BigQueryConn{URL: URL}
	} else if strings.HasPrefix(URL, "snowflake") {
		conn = &SnowflakeConn{URL: URL}
	} else if strings.HasPrefix(URL, "file:") {
		conn = &SQLiteConn{URL: URL}
	} else {
		conn = &BaseConn{URL: URL}
	}

	// Add / Extract provided Props
	for _, propStr := range props {
		h.Trace("setting connection prop -> " + propStr)
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
	conn.setContext(ctx)
	err = conn.Init()

	return conn, err
}

// GetSlingEnv return sling Env Vars
func GetSlingEnv() map[string]string {
	slingEnvs := map[string]string{}

	for _, env := range os.Environ() {
		key := strings.Split(env, "=")[0]
		value := strings.ReplaceAll(env, key+"=", "")

		keyUpper := strings.ToUpper(key)
		if strings.HasPrefix(keyUpper, "SLING_") {
			slingEnvs[keyUpper] = value
		}
	}

	return slingEnvs
}

func getDriverName(dbType DbType) (driverName string) {
	switch dbType {
	case PostgresDbType, RedshiftDbType:
		driverName = "postgres"
	case MySQLDbType:
		driverName = "mysql"
	case OracleDbType:
		driverName = "godror"
	case BigQueryDbType:
		driverName = "bigquery"
	case SnowflakeDbType:
		driverName = "snowflake"
	case SQLiteDbType:
		driverName = "sqlite3"
	case SQLServerDbType, AzureSQLDbType, AzureDWHDbType:
		driverName = "sqlserver"
	default:
		driverName = dbType.String()
	}
	return
}

func getDialector(conn Connection) (driverDialector gorm.Dialector) {
	switch conn.GetType() {
	case PostgresDbType, RedshiftDbType:
		driverDialector = postgres.Open(conn.BaseURL())
	}
	return
}

// BaseURL returns the base URL with default port
func (conn *BaseConn) BaseURL() string {
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
		var instance Connection
		instance = conn
		conn.instance = &instance
	}

	// sets all the available env vars as default if not already set
	for k, v := range env.EnvVars() {
		if conn.GetProp(k) == "" {
			conn.SetProp(k, v)
		}
	}

	if conn.defaultPort != 0 {
		connU, err := url.Parse(conn.URL)
		if err != nil {
			return h.Error(err, "could not parse connection URL")
		}
		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			conn.URL = strings.ReplaceAll(
				conn.URL, h.F("@%s", connHost),
				h.F("@%s:%d", connHost, conn.defaultPort),
			)
		}
	}

	err = conn.LoadTemplates()
	if err != nil {
		return err
	}
	conn.SetProp("connected", "false")
	return nil
}

func (conn *BaseConn) setContext(ctx context.Context) {
	conn.context = h.NewContext(ctx)
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

// String returns the db type string
func (t DbType) String() string {
	return string(t)
}

// GetType returns the type db object
func (conn *BaseConn) GetType() DbType {
	return conn.Type
}

// Context returns the db context
func (conn *BaseConn) Context() *h.Context {
	return &conn.context
}

// Schemata returns the Schemata object
func (conn *BaseConn) Schemata() Schemata {
	return conn.schemata
}

// Template returns the Template object
func (conn *BaseConn) Template() Template {
	return conn.template
}

// GetProp returns the value of a property
func (conn *BaseConn) GetProp(key string) string {
	return conn.properties[strings.ToUpper(key)]
}

// SetProp sets the value of a property
func (conn *BaseConn) SetProp(key string, val string) {
	if conn.properties == nil {
		conn.properties = map[string]string{}
	}
	conn.properties[strings.ToUpper(key)] = val
}

// PropArr returns an array of properties
func (conn *BaseConn) PropArr() []string {
	props := []string{}
	for k, v := range conn.properties {
		props = append(props, h.F("%s=%s", k, v))
	}
	return props
}

// Kill kill the database connection
func (conn *BaseConn) Kill() error {
	conn.context.Cancel()
	conn.SetProp("connected", "false")
	return nil
}

// Connect connects to the database
func (conn *BaseConn) Connect(timeOut ...int) (err error) {
	conn.schemata = Schemata{
		Schemas: map[string]Schema{},
		Tables:  map[string]*Table{},
	}

	to := 15
	if len(timeOut) > 0 && timeOut[0] != 0 {
		to = timeOut[0]
	}

	h.Trace("conn.Type: %s", conn.Type)
	h.Trace("conn.URL: " + conn.Self().GetURL())
	if conn.Type == "" {
		return errors.New("Invalid URL? conn.Type needs to be specified")
	}

	connURL := conn.URL
	// start SSH Tunnel with SSH_TUNNEL prop
	if sshURL := conn.GetProp("SSH_TUNNEL"); sshURL != "" {
		sshU, err := url.Parse(sshURL)
		if err != nil {
			return h.Error(err, "could not parse SSH_TUNNEL URL")
		}

		connU, err := url.Parse(connURL)
		if err != nil {
			return h.Error(err, "could not parse connection URL for SSH forwarding")
		}

		sshHost := sshU.Hostname()
		sshPort := cast.ToInt(sshU.Port())
		if sshPort == 0 {
			sshPort = 22
		}
		sshUser := sshU.User.Username()
		sshPassword, _ := sshU.User.Password()

		connHost := connU.Hostname()
		connPort := cast.ToInt(connU.Port())
		if connPort == 0 {
			connPort = conn.defaultPort
			connURL = strings.ReplaceAll(
				connURL, h.F("@%s", connHost),
				h.F("@%s:%d", connHost, connPort),
			)
		}

		conn.sshClient = &iop.SSHClient{
			Host:       sshHost,
			Port:       sshPort,
			User:       sshUser,
			Password:   sshPassword,
			TgtHost:    connHost,
			TgtPort:    connPort,
			PrivateKey: conn.GetProp("SSH_PRIVATE_KEY"),
		}

		localPort, err := conn.sshClient.OpenPortForward()
		if err != nil {
			return h.Error(err, "could not connect to ssh server")
		}

		connURL = strings.ReplaceAll(
			connURL, h.F("@%s:%d", connHost, connPort),
			h.F("@127.0.0.1:%d", localPort),
		)
		h.Trace("new connection URL: " + conn.Self().GetURL(connURL))
	}

	if conn.Type != BigQueryDbType {
		connURL = conn.Self().GetURL(connURL)

		connPool.Mux.Lock()
		db, ok := connPool.Dbs[connURL]
		connPool.Mux.Unlock()

		if !ok || true { // do not use pool for now. Db is closed after use.
			db, err = sqlx.Open(getDriverName(conn.Type), connURL)
			if err != nil {
				return h.Error(err, "Could not connect to DB: "+getDriverName(conn.Type))
			}
		}

		conn.db = db

		// 15 sec timeout
		pingCtx, cancel := context.WithTimeout(conn.Context().Ctx, time.Duration(to)*time.Second)
		_ = cancel // lint complaint

		err = conn.db.PingContext(pingCtx)
		if err != nil {
			return h.Error(err, "Could not ping DB")
		}

		// add to pool after successful connection
		if !ok {
			connPool.Mux.Lock()
			connPool.Dbs[connURL] = db
			connPool.Mux.Unlock()

			// expire the connection from pool after 10 minutes
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

	h.Info(`connected to %s`, conn.Type)
	return nil
}

// Close closes the connection
func (conn *BaseConn) Close() error {
	var err error
	if conn.db != nil {
		err = conn.db.Close()
	}
	if conn.sshClient != nil {
		conn.sshClient.Close()
	}
	return err
}

// GetGormConn returns the gorm db connection
func (conn *BaseConn) GetGormConn(config *gorm.Config) (*gorm.DB, error) {
	return gorm.Open(getDialector(conn), config)
}

// GetTemplateValue returns the value of the path
func (conn *BaseConn) GetTemplateValue(path string) (value string) {

	prefixes := map[string]map[string]string{
		"core.":             conn.template.Core,
		"analysis.":         conn.template.Analysis,
		"function.":         conn.template.Function,
		"metadata.":         conn.template.Metadata,
		"general_type_map.": conn.template.GeneralTypeMap,
		"native_type_map.":  conn.template.NativeTypeMap,
		"variable.":         conn.template.Variable,
	}

	for prefix, dict := range prefixes {
		if strings.HasPrefix(path, prefix) {
			key := strings.Replace(path, prefix, "", 1)
			value = dict[key]
			break
		}
	}

	return value
}

// ToData convert is dataset
func (template Template) ToData() (data iop.Dataset) {
	columns := []string{"key", "value"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns))
	data.Rows = append(data.Rows, []interface{}{"core", template.Core})
	data.Rows = append(data.Rows, []interface{}{"analysis", template.Analysis})
	data.Rows = append(data.Rows, []interface{}{"function", template.Function})
	data.Rows = append(data.Rows, []interface{}{"metadata", template.Metadata})
	data.Rows = append(data.Rows, []interface{}{"general_type_map", template.GeneralTypeMap})
	data.Rows = append(data.Rows, []interface{}{"native_type_map", template.NativeTypeMap})
	data.Rows = append(data.Rows, []interface{}{"variable", template.Variable})

	return
}

// LoadTemplates loads the appropriate yaml template
func (conn *BaseConn) LoadTemplates() error {
	conn.template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
		NativeTypeMap:  map[string]string{},
		NativeStatsMap: map[string]bool{},
		Variable:       map[string]string{},
	}

	baseTemplateFile, err := h.PkgerFile("templates/base.yaml")
	if err != nil {
		return h.Error(err, `cannot read templates/base.yaml`)
	}

	baseTemplateBytes, err := ioutil.ReadAll(baseTemplateFile)
	if err != nil {
		return h.Error(err, "ioutil.ReadAll(baseTemplateFile)")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &conn.template); err != nil {
		return h.Error(err, "yaml.Unmarshal")
	}

	fName := "templates/" + conn.Type.String() + ".yaml"
	templateFile, err := h.PkgerFile(fName)
	if err != nil {
		return h.Error(err, `cannot read `+fName)
	}

	templateBytes, err := ioutil.ReadAll(templateFile)
	if err != nil {
		return h.Error(err, "ioutil.ReadAll(templateFile) for "+conn.Type)
	}

	template := Template{}
	err = yaml.Unmarshal([]byte(templateBytes), &template)
	if err != nil {
		return h.Error(err, "yaml.Unmarshal")
	}

	for key, val := range template.Core {
		conn.template.Core[key] = val
	}

	for key, val := range template.Analysis {
		conn.template.Analysis[key] = val
	}

	for key, val := range template.Function {
		conn.template.Function[key] = val
	}

	for key, val := range template.Metadata {
		conn.template.Metadata[key] = val
	}

	for key, val := range template.Variable {
		conn.template.Variable[key] = val
	}

	TypesNativeFile, err := h.PkgerFile("templates/types_native_to_general.tsv")
	if err != nil {
		return h.Error(err, `cannot open types_native_to_general`)
	}

	TypesNativeCSV := iop.CSV{Reader: bufio.NewReader(TypesNativeFile)}
	TypesNativeCSV.Delimiter = '\t'
	TypesNativeCSV.NoTrace = true

	data, err := TypesNativeCSV.Read()
	if err != nil {
		return h.Error(err, `TypesNativeCSV.Read()`)
	}

	for _, rec := range data.Records() {
		if rec["database"] == conn.Type.String() {
			nt := strings.TrimSpace(cast.ToString(rec["native_type"]))
			gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
			s := strings.TrimSpace(cast.ToString(rec["stats_allowed"]))
			conn.template.NativeTypeMap[nt] = cast.ToString(gt)
			conn.template.NativeStatsMap[nt] = cast.ToBool(s)
		}
	}

	TypesGeneralFile, err := h.PkgerFile("templates/types_general_to_native.tsv")
	if err != nil {
		return h.Error(err, `cannot open types_general_to_native`)
	}

	TypesGeneralCSV := iop.CSV{Reader: bufio.NewReader(TypesGeneralFile)}
	TypesGeneralCSV.Delimiter = '\t'
	TypesGeneralCSV.NoTrace = true

	data, err = TypesGeneralCSV.Read()
	if err != nil {
		return h.Error(err, `TypesGeneralCSV.Read()`)
	}

	for _, rec := range data.Records() {
		gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
		conn.template.GeneralTypeMap[gt] = cast.ToString(rec[conn.Type.String()])
	}

	return nil
}

// StreamRecords the records of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRecords(sql string) (<-chan map[string]interface{}, error) {

	ds, err := conn.Self().StreamRows(sql)
	if err != nil {
		err = h.Error(err, "error in StreamRowsContext")
	}
	return ds.Records(), nil
}

// BulkExportStream streams the rows in bulk
func (conn *BaseConn) BulkExportStream(sql string) (ds *iop.Datastream, err error) {
	h.Trace("BulkExportStream not implemented for %s", conn.Type)
	return conn.Self().StreamRows(sql)
}

// BulkImportStream import the stream rows in bulk
func (conn *BaseConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	h.Trace("BulkImportStream not implemented for %s", conn.Type)
	return conn.Self().InsertBatchStream(tableFName, ds)
}

// StreamRows the rows of a sql query, returns `result`, `error`
func (conn *BaseConn) StreamRows(sql string, limit ...int) (ds *iop.Datastream, err error) {
	return conn.Self().StreamRowsContext(conn.Context().Ctx, sql, limit...)
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BaseConn) StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error) {
	Limit := uint64(0) // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = cast.ToUint64(limit[0])
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, errors.New("Empty Query")
	}

	noTrace := strings.Contains(sql, "\n\n-- nT --")
	if !noTrace {
		h.Debug(sql)
	}

	queryContext := h.NewContext(ctx)

	var result *sqlx.Rows
	if conn.tx != nil {
		result, err = conn.tx.QueryxContext(queryContext.Ctx, sql)
	} else {
		result, err = conn.db.QueryxContext(queryContext.Ctx, sql)
	}
	if err != nil {
		queryContext.Cancel()
		return ds, h.Error(err, "SQL Error for:\n"+sql)
	}

	colTypes, err := result.ColumnTypes()
	if err != nil {
		queryContext.Cancel()
		return ds, h.Error(err, "result.ColumnTypes()")
	}

	conn.Data.Result = result
	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.Columns = SQLColumns(colTypes, conn.template.NativeTypeMap)
	conn.Data.NoTrace = noTrace

	h.Trace("query responded in %f secs", conn.Data.Duration)

	nextFunc := func(it *iop.Iterator) bool {
		if Limit > 0 && it.Counter == Limit {
			result.Close()
			return false
		}

		next := result.Next()
		if next {
			// add row
			it.Row, err = result.SliceScan()
			if err != nil {
				it.Context.CaptureErr(h.Error(err, "failed to scan"))
			} else {
				return true
			}
		}

		result.Close()

		// if any error occurs during iteration
		if result.Err() != nil {
			it.Context.CaptureErr(h.Error(result.Err(), "error during iteration"))
		}
		return false
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoTrace = noTrace
	ds.Inferred = true

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, h.Error(err, "could start datastream")
	}
	return
}

// Begin starts a connection wide transaction
func (conn *BaseConn) Begin() (err error) {
	if conn.db == nil {
		return
	}
	conn.tx, err = conn.db.BeginTxx(conn.Context().Ctx, &sql.TxOptions{})
	if err != nil {
		err = h.Error(err, "could not begin Tx")
	}
	return
}

// Commit commits connection wide transaction
func (conn *BaseConn) Commit() (err error) {
	if conn.tx == nil {
		return
	}
	err = conn.tx.Commit()
	conn.tx = nil
	if err != nil {
		err = h.Error(err, "could not commit Tx")
	}
	return
}

// Rollback rolls back connection wide transaction
func (conn *BaseConn) Rollback() (err error) {
	if conn.tx == nil {
		return
	}
	err = conn.tx.Rollback()
	conn.tx = nil
	if err != nil {
		err = h.Error(err, "could not rollback Tx")
	}
	return
}

// Exec runs a sql query, returns `error`
func (conn *BaseConn) Exec(sql string, args ...interface{}) (result sql.Result, err error) {
	if conn.GetProp("connected") != "true" {
		err = conn.Self().Connect()
		if err != nil {
			err = h.Error(err, "Could not connect")
			return
		}
	}
	result, err = conn.Self().ExecContext(conn.Context().Ctx, sql, args...)
	if err != nil {
		err = h.Error(err, "Could not execute SQL")
	}
	return
}

// ExecContext runs a sql query with context, returns `error`
func (conn *BaseConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	if strings.TrimSpace(sql) == "" {
		err = h.Error(errors.New("Empty Query"))
		return
	}

	noTrace := strings.Contains(sql, "\n\n-- nT --")
	if !noTrace {
		h.Debug(conn.CleanSQL(sql), args...)
	}

	if conn.tx != nil {
		result, err = conn.tx.ExecContext(ctx, sql, args...)
	} else {
		result, err = conn.db.ExecContext(ctx, sql, args...)
	}
	if err != nil {
		err = h.Error(err, "Error executing: "+conn.CleanSQL(sql))
	}
	return
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func (conn *BaseConn) MustExec(sql string, args ...interface{}) (result sql.Result) {
	res, err := conn.Self().Exec(sql, args...)
	if err != nil {
		h.LogFatal(err, "query error for: "+sql)
	}
	return res
}

// Query runs a sql query, returns `result`, `error`
func (conn *BaseConn) Query(sql string, limit ...int) (data iop.Dataset, err error) {
	if conn.GetProp("connected") != "true" {
		err = conn.Self().Connect()
		if err != nil {
			err = h.Error(err, "Could not connect")
			return
		}
	}

	ds, err := conn.Self().StreamRows(sql, limit...)
	if err != nil {
		err = h.Error(err, "Error with StreamRows")
		return iop.Dataset{SQL: sql}, err
	}

	data, err = ds.Collect(0)
	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration

	return data, err
}

// QueryContext runs a sql query with ctx, returns `result`, `error`
func (conn *BaseConn) QueryContext(ctx context.Context, sql string, limit ...int) (iop.Dataset, error) {

	ds, err := conn.Self().StreamRowsContext(ctx, sql, limit...)
	if err != nil {
		return iop.Dataset{SQL: sql}, err
	}

	data, err := ds.Collect(0)
	data.SQL = sql
	data.Duration = conn.Data.Duration // Collect does not time duration

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

// GetCount returns count of records
func (conn *BaseConn) GetCount(tableFName string) (uint64, error) {
	sql := fmt.Sprintf(`select count(*) cnt from %s`, tableFName)
	data, err := conn.Self().Query(sql)
	if err != nil {
		return 0, err
	}
	return cast.ToUint64(data.Rows[0][0]), nil
}

// GetSchemas returns schemas
func (conn *BaseConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]
	sql := conn.template.Metadata["schemas"] + "\n\n-- nT --"
	return conn.Self().Query(sql)
}

// GetObjects returns objects (tables or views) for given schema
// `objectType` can be either 'table', 'view' or 'all'
func (conn *BaseConn) GetObjects(schema string, objectType string) (iop.Dataset, error) {
	sql := h.R(conn.template.Metadata["objects"], "schema", schema, "object_type", objectType) + "\n\n-- nT --"
	return conn.Self().Query(sql)
}

// GetTables returns tables for given schema
func (conn *BaseConn) GetTables(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	sql := h.R(conn.template.Metadata["tables"], "schema", schema) + "\n\n-- nT --"
	return conn.Self().Query(sql)
}

// GetViews returns views for given schema
func (conn *BaseConn) GetViews(schema string) (iop.Dataset, error) {
	// fields: [table_name]
	sql := h.R(conn.template.Metadata["views"], "schema", schema) + "\n\n-- nT --"
	return conn.Self().Query(sql)
}

// ColumnNames return column names of columns array
func ColumnNames(columns []iop.Column) (colNames []string) {
	colNames = make([]string, len(columns))
	for i := range columns {
		colNames[i] = columns[i].Name
	}
	return
}

// CommonColumns return common columns
func CommonColumns(colNames1 []string, colNames2 []string) (commCols []string) {
	commCols = []string{}
	cols2 := h.ArrMapString(colNames2, true)
	for _, col := range colNames1 {
		if _, ok := cols2[strings.ToLower(col)]; ok {
			commCols = append(commCols, col)
		}
	}
	return
}

// SQLColumns returns the columns from database ColumnType
func SQLColumns(colTypes []*sql.ColumnType, NativeTypeMap map[string]string) (columns []iop.Column) {
	columns = make([]iop.Column, len(colTypes))

	for i, colType := range colTypes {
		dbType := strings.ToLower(colType.DatabaseTypeName())
		dbType = strings.Split(dbType, "(")[0]

		Type := dbType
		if _, ok := NativeTypeMap[dbType]; ok {
			Type = NativeTypeMap[dbType]
		} else if Type != "" {
			h.Warn("type '%s' not mapped for col '%s': %#v", dbType, colType.Name(), colType)
			Type = "string" // default as string
		}

		columns[i] = iop.Column{
			Name:     colType.Name(),
			Position: i + 1,
			Type:     Type,
			DbType:   dbType,
			ColType:  colType,
		}
		// h.Trace("%s -> %s (%s)", colType.Name(), Type, dbType)
	}
	return columns
}

// GetSQLColumns return columns from a sql query result
func (conn *BaseConn) GetSQLColumns(sqls ...string) (columns []iop.Column, err error) {
	sql := ""
	if len(sqls) > 0 {
		sql = sqls[0]
	} else {
		err = h.Error("no query provided")
		return
	}

	// add 1=0
	sql = h.R(conn.GetTemplateValue("core.column_names"), "sql", sql)

	// get column types
	h.Trace("GetSQLColumns: %s", sql)
	if conn.Db() != nil {
		rows, err := conn.Db().Queryx(sql)
		if err != nil {
			err = h.Error(err, "SQL Error for:\n"+sql)
			return columns, err
		}
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			err = h.Error(err, "could not get column types")
			return columns, err
		}
		rows.Close()
		h.Trace("GetSQLColumns: got %d columns", len(colTypes))

		columns = SQLColumns(colTypes, conn.Template().NativeTypeMap)
	} else {
		ds, err := conn.Self().StreamRows(sql)
		if err != nil {
			err = h.Error(err, "SQL Error for:\n"+sql)
			return columns, err
		}
		columns = ds.Columns
	}

	columns = iop.Columns(columns)
	return
}

// GetColumns returns columns for given table. `tableFName` should
// include schema and table, example: `schema1.table2`
// fields should be `column_name|data_type`
func (conn *BaseConn) GetColumns(tableFName string, fields ...string) (columns []iop.Column, err error) {
	columns = []iop.Column{}
	sql := getMetadataTableFName(conn, "columns", tableFName)

	colData, err := conn.Self().Query(sql)
	if err != nil {
		return columns, h.Error(err, "could not get list of columns for table: "+tableFName)
	}

	// if fields provided, check if exists in table
	colMap := map[string]string{}
	fieldMap := map[string]string{}
	for _, rec := range colData.Records() {
		colName := cast.ToString(rec["column_name"])
		colMap[strings.ToLower(colName)] = colName
	}
	for _, field := range fields {
		_, ok := colMap[strings.ToLower(field)]
		if !ok {
			err = h.Error(
				"provided field '%s' not found in table %s",
				strings.ToLower(field), tableFName,
			)
			return
		}
		fieldMap[strings.ToLower(field)] = colMap[strings.ToLower(field)]
	}

	for i, rec := range colData.Records() {
		dType := cast.ToString(rec["data_type"])
		dType = strings.Split(strings.ToLower(dType), "(")[0]
		generalType, ok := conn.Template().NativeTypeMap[dType]
		if !ok {
			err = h.Error(
				"No general type mapping defined for col '%s', with type '%s' for '%s'",
				rec["column_name"],
				dType,
				conn.GetType(),
			)
			return
		}

		column := iop.Column{
			Position:    i + 1,
			Name:        cast.ToString(rec["column_name"]),
			Type:        generalType,
			DbType:      cast.ToString(rec["data_type"]),
			DbPrecision: cast.ToInt(rec["precision"]),
			DbScale:     cast.ToInt(rec["scale"]),
		}
		if len(fields) > 0 {
			_, ok := fieldMap[strings.ToLower(column.Name)]
			if !ok {
				continue
			}
		}
		columns = append(columns, column)
	}

	if len(columns) == 0 {
		err = h.Error("unable to obtain columns for " + tableFName)
	}

	return
}

// GetColumnsFull returns columns for given table. `tableName` should
// include schema and table, example: `schema1.table2`
// fields should be `schema_name|table_name|table_type|column_name|data_type|column_id`
func (conn *BaseConn) GetColumnsFull(tableFName string) (iop.Dataset, error) {
	sql := getMetadataTableFName(conn, "columns_full", tableFName)
	return conn.Self().Query(sql)
}

// GetPrimaryKeys returns primark keys for given table.
func (conn *BaseConn) GetPrimaryKeys(tableFName string) (iop.Dataset, error) {
	sql := getMetadataTableFName(conn, "primary_keys", tableFName)
	return conn.Self().Query(sql)
}

// GetIndexes returns indexes for given table.
func (conn *BaseConn) GetIndexes(tableFName string) (iop.Dataset, error) {
	sql := getMetadataTableFName(conn, "indexes", tableFName)
	return conn.Self().Query(sql)
}

// GetDDL returns DDL for given table.
func (conn *BaseConn) GetDDL(tableFName string) (string, error) {
	schema, table := SplitTableFullName(tableFName)
	ddlCol := cast.ToInt(conn.template.Variable["ddl_col"])
	sqlTable := h.R(
		conn.template.Metadata["ddl_table"],
		"schema", schema,
		"table", table,
	) + "\n\n-- nT --"
	sqlView := h.R(
		conn.template.Metadata["ddl_view"],
		"schema", schema,
		"table", table,
	) + "\n\n-- nT --"

	data, err := conn.Self().Query(sqlView)
	if err != nil {
		return "", err
	}

	if len(data.Rows) == 0 || cast.ToString(data.Rows[0][ddlCol]) == "" {
		data, err = conn.Self().Query(sqlTable)
		if err != nil {
			return "", err
		}
	}

	if len(data.Rows) == 0 {
		return "", nil
	}

	return cast.ToString(data.Rows[0][ddlCol]), nil
}

func getMetadataTableFName(conn *BaseConn, template string, tableFName string) string {
	schema, table := SplitTableFullName(tableFName)
	sql := h.R(
		conn.template.Metadata[template],
		"schema", schema,
		"table", table,
	)
	sql = sql + "\n\n-- nT --"
	return sql
}

// DropTable drops given table.
func (conn *BaseConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		sql := h.R(conn.template.Core["drop_table"], "table", tableName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_table"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return h.Error(err, "Error for "+sql)
			}
			h.Debug("table %s does not exist", tableName)
		} else {
			h.Debug("table %s dropped", tableName)
		}
	}
	return nil
}

// DropView drops given view.
func (conn *BaseConn) DropView(viewNames ...string) (err error) {

	for _, viewName := range viewNames {
		sql := h.R(conn.template.Core["drop_view"], "view", viewName)
		_, err = conn.Self().Exec(sql)
		if err != nil {
			errIgnoreWord := conn.template.Variable["error_ignore_drop_view"]
			if !(errIgnoreWord != "" && strings.Contains(cast.ToString(err), errIgnoreWord)) {
				return h.Error(err, "Error for "+sql)
			}
			h.Debug("view %s does not exist", viewName)
		} else {
			h.Debug("view %s dropped", viewName)
		}
	}
	return nil
}

// Import imports `data` into `tableName`
func (conn *BaseConn) Import(data iop.Dataset, tableName string) error {

	return nil
}

// GetSchemaObjects obtain full schemata info
func (conn *BaseConn) GetSchemaObjects(schemaName string) (Schema, error) {

	schema := Schema{
		Name:   "",
		Tables: map[string]Table{},
	}

	sql := h.R(conn.template.Metadata["schemata"], "schema", schemaName) + "\n\n-- nT --"
	schemaData, err := conn.Self().Query(sql)
	if err != nil {
		return schema, h.Error(err, "Could not GetSchemaObjects for "+schemaName)
	}

	schema.Name = schemaName

	for _, rec := range schemaData.Records() {
		tableName := strings.ToLower(cast.ToString(rec["table_name"]))

		switch v := rec["is_view"].(type) {
		case int64, float64:
			if cast.ToInt64(rec["is_view"]) == 0 {
				rec["is_view"] = false
			} else {
				rec["is_view"] = true
			}
		case string:
			if cast.ToBool(rec["is_view"]) {
				rec["is_view"] = true
			} else {
				rec["is_view"] = false
			}

		default:
			_ = fmt.Sprint(v)
			_ = rec["is_view"]
		}

		table := Table{
			Name:       tableName,
			IsView:     cast.ToBool(rec["is_view"]),
			Columns:    []iop.Column{},
			ColumnsMap: map[string]*iop.Column{},
		}

		if _, ok := schema.Tables[tableName]; ok {
			table = schema.Tables[tableName]
		}

		column := iop.Column{
			Position: cast.ToInt(schemaData.Sp.ProcessVal(rec["position"])),
			Name:     cast.ToString(rec["column_name"]),
			Type:     cast.ToString(rec["data_type"]),
			DbType:   cast.ToString(rec["data_type"]),
		}

		table.Columns = append(table.Columns, column)
		table.ColumnsMap[strings.ToLower(column.Name)] = &column

		conn.schemata.Tables[schemaName+"."+tableName] = &table
		schema.Tables[tableName] = table

	}

	conn.schemata.Schemas[schemaName] = schema

	return schema, nil
}

// RunAnalysis runs an analysis
func (conn *BaseConn) RunAnalysis(analysisName string, values map[string]interface{}) (iop.Dataset, error) {
	sql := h.Rm(
		conn.template.Analysis[analysisName],
		values,
	)
	return conn.Self().Query(sql)
}

// RunAnalysisTable runs a table level analysis
func (conn *BaseConn) RunAnalysisTable(analysisName string, tableFNames ...string) (iop.Dataset, error) {

	if len(tableFNames) == 0 {
		return iop.NewDataset(nil), errors.New("Need to provied tables for RunAnalysisTable")
	}

	sqls := []string{}

	for _, tableFName := range tableFNames {
		schema, table := SplitTableFullName(tableFName)
		sql := h.R(
			conn.GetTemplateValue("analysis."+analysisName),
			"schema", schema,
			"table", table,
		)
		sqls = append(sqls, sql)
	}

	sql := strings.Join(sqls, "\nUNION ALL\n")
	return conn.Self().Query(sql)
}

// RunAnalysisField runs a field level analysis
func (conn *BaseConn) RunAnalysisField(analysisName string, tableFName string, fields ...string) (iop.Dataset, error) {
	schema, table := SplitTableFullName(tableFName)

	sqls := []string{}

	if len(fields) == 0 {
		// get fields
		columns, err := conn.GetColumns(tableFName)
		if err != nil {
			return iop.NewDataset(nil), err
		}

		fields = ColumnNames(columns)
	}

	for _, field := range fields {
		sql := h.R(
			conn.template.Analysis[analysisName],
			"schema", schema,
			"table", table,
			"field", field,
		)
		sqls = append(sqls, sql)
	}

	sql := strings.Join(sqls, "\nUNION ALL\n")
	return conn.Self().Query(sql)
}

// CastColumnForSelect casts to the correct target column type
func (conn *BaseConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) string {
	return conn.Self().Quote(srcCol.Name)
}

// CastColumnsForSelect cast the source columns into the target Column types
func (conn *BaseConn) CastColumnsForSelect(srcColumns []iop.Column, tgtColumns []iop.Column) []string {
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

		selectExpr := conn.Self().Quote(srcCol.Name)

		if srcCol.DbType != tgtCol.DbType {
			h.Debug(
				"inserting %s [%s] into %s [%s]",
				srcCol.Name, srcCol.DbType, tgtCol.Name, tgtCol.DbType,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		} else if srcCol.DbPrecision > tgtCol.DbPrecision {
			h.Debug(
				"target precision / length is smaller when inserting %s [%s(%d)] into %s [%s(%d)]",
				srcCol.Name, srcCol.DbType, srcCol.DbPrecision,
				tgtCol.Name, tgtCol.DbType, tgtCol.DbPrecision,
			)
			selectExpr = conn.Self().CastColumnForSelect(srcCol, tgtCol)
		}

		selectExprs = append(selectExprs, selectExpr)
	}

	return selectExprs
}

// ValidateColumnNames verifies that source fields are present in the target table
// It will return quoted field names as `newColNames`, the same length as `colNames`
func (conn *BaseConn) ValidateColumnNames(tgtColNames []string, colNames []string, quote bool) (newColNames []string, err error) {

	tgtFields := map[string]string{}
	for _, colName := range tgtColNames {
		colName = conn.Self().Unquote(colName)
		if quote {
			tgtFields[strings.ToLower(colName)] = conn.Self().Quote(colName)
		} else {
			tgtFields[strings.ToLower(colName)] = colName
		}
	}

	mismatches := []string{}
	for _, colName := range colNames {
		newColName, ok := tgtFields[strings.ToLower(colName)]
		if !ok {
			// src field is missing in tgt field
			mismatches = append(mismatches, h.F("source field '%s' is missing in target table", colName))
			continue
		}
		if quote {
			newColName = conn.Self().Quote(newColName)
		} else {
			newColName = conn.Self().Unquote(newColName)
		}
		newColNames = append(newColNames, newColName)
	}

	if len(mismatches) > 0 {
		err = h.Error("column names mismatch: %s", strings.Join(mismatches, "\n"))
	}

	h.Trace("insert target fields: " + strings.Join(newColNames, ", "))

	return
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BaseConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	// make sure fields match
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}

	batchSize := cast.ToInt(conn.GetTemplateValue("variable.batch_values")) / len(columns)

	ds, err = ds.Shape(columns)
	if err != nil {
		err = h.Error(err, "could not shape stream")
		return
	}

	insFields, err := conn.ValidateColumnNames(ColumnNames(columns), ds.GetFields(), true)
	if err != nil {
		err = h.Error(err, "columns mismatch")
		return
	}

	txOptions := sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false}
	switch conn.GetType() {
	case SnowflakeDbType:
		txOptions = sql.TxOptions{}
	}

	tx, err := conn.Db().BeginTx(ds.Context.Ctx, &txOptions)
	if err != nil {
		err = h.Error(err, "Could not begin transaction")
		return
	}

	insertBatch := func(rows [][]interface{}) error {
		var err error
		defer conn.Context().Wg.Write.Done()
		insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, len(rows))

		// open statement
		stmt, err := tx.PrepareContext(ds.Context.Ctx, insertTemplate)
		if err != nil {
			err = h.Error(err, "Error in PrepareContext")
			conn.Context().CaptureErr(err)
			conn.Context().Cancel()
			tx.Rollback()
			return conn.Context().Err()
		}

		vals := []interface{}{}
		for _, row := range rows {
			vals = append(vals, row...)
		}

		// Do insert
		_, err = stmt.ExecContext(ds.Context.Ctx, vals...)
		if err != nil {
			batchErrStr := h.F("Batch Size: %d rows x %d cols = %d", len(rows), len(rows[0]), len(rows)*len(rows[0]))
			if len(insertTemplate) > 1000 {
				insertTemplate = insertTemplate[:1000]
			}
			if len(rows) > 10 {
				rows = rows[:10]
			}
			println(h.F(
				"%s \n%s",
				batchErrStr,
				fmt.Sprintf("Insert: %s", insertTemplate),
				// fmt.Sprintf("\n\nRows: %#v", rows),
			))
			conn.Context().CaptureErr(err)
			conn.Context().Cancel()
			tx.Rollback()
			return conn.Context().Err()
		}

		// close statement
		err = stmt.Close()
		if err != nil {
			err = h.Error(
				err,
				fmt.Sprintf("stmt.Close: %s", insertTemplate),
			)
			conn.Context().CaptureErr(err)
			conn.Context().Cancel()
			tx.Rollback()
		}
		return conn.Context().Err()
	}

	batchRows := [][]interface{}{}
	h.Trace("batchRows")
	for row := range ds.Rows {
		batchRows = append(batchRows, row)
		count++
		if len(batchRows) == batchSize {
			select {
			case <-conn.Context().Ctx.Done():
				return count, conn.Context().Err()
			case <-ds.Context.Ctx.Done():
				return count, ds.Context.Err()
			default:
				conn.Context().Wg.Write.Add()
				go insertBatch(batchRows)
			}

			batchRows = [][]interface{}{}
		}
	}

	// remaining batch
	h.Trace("remaining batch")
	if len(batchRows) > 0 {
		conn.Context().Wg.Write.Add()
		err = insertBatch(batchRows)
		if err != nil {
			return count - cast.ToUint64(len(batchRows)), h.Error(err, "insertBatch")
		}
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()
	ds.SetEmpty()

	if err != nil {
		ds.Context.Cancel()
		return count - cast.ToUint64(batchSize), h.Error(err, "insertBatch")
	}

	if ds.Err() != nil {
		return count, h.Error(ds.Err(), "context error")
	}

	err = tx.Commit()
	if err != nil {
		ds.Context.Cancel()
		return 0, h.Error(err, "could not commit transaction")
	}

	return count, nil
}

// bindVar return proper bind var according to https://jmoiron.github.io/sqlx/#bindvars
func (conn *BaseConn) bindVar(i int, field string, n int, c int) string {
	return h.R(
		conn.template.Variable["bind_string"],
		"i", cast.ToString(i),
		"field", field,
		"n", cast.ToString(n),
		"c", cast.ToString(c),
	)
}

// Unquote removes quotes to the field name
func (conn *BaseConn) Unquote(field string) string {
	q := conn.template.Variable["quote_char"]
	return strings.ReplaceAll(field, q, "")
}

// Quote adds quotes to the field name
func (conn *BaseConn) Quote(field string) string {
	q := conn.template.Variable["quote_char"]
	field = conn.Self().Unquote(field)
	return q + field + q
}

// GenerateInsertStatement returns the proper INSERT statement
func (conn *BaseConn) GenerateInsertStatement(tableName string, fields []string, numRows int) string {

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

	statement := h.R(
		"INSERT INTO {table} ({fields}) VALUES {values}",
		"table", tableName,
		"fields", strings.Join(qFields, ", "),
		"values", strings.TrimSuffix(valuesStr, ","),
	)
	h.Trace("insert statement: " + strings.Split(statement, ") VALUES ")[0] + ")")
	return statement
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *BaseConn) Upsert(srcTable string, tgtTable string, primKeys []string) (rowAffCnt int64, err error) {
	err = fmt.Errorf("Upsert is not implemented for %s", conn.GetType())
	err = h.Error(err, "")
	return
}

// SwapTable swaps two table
func (conn *BaseConn) SwapTable(srcTable string, tgtTable string) (err error) {

	tgtTableTemp := tgtTable + "_tmp" + h.RandString(h.AlphaRunesLower, 2)
	conn.DropTable(tgtTableTemp)

	tx, err := conn.db.Begin()
	if err != nil {
		err = h.Error(err, "Could not begin transaction")
		return
	}

	sql := h.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTable,
		"new_table", tgtTableTemp,
	)
	_, err = tx.Exec(sql)
	if err != nil {
		return h.Error(err, "could not rename table "+tgtTable)
	}

	sql = h.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", srcTable,
		"new_table", tgtTable,
	)
	_, err = tx.Exec(sql)
	if err != nil {
		return h.Error(err, "could not rename table "+srcTable)
	}

	sql = h.R(
		conn.GetTemplateValue("core.rename_table"),
		"table", tgtTableTemp,
		"new_table", srcTable,
	)
	_, err = tx.Exec(sql)
	if err != nil {
		return h.Error(err, "could not rename table "+tgtTableTemp)
	}
	err = tx.Commit()
	if err != nil {
		return h.Error(err, "could not commit")
	}

	return
}

// InsertStream inserts a stream into a table
func (conn *BaseConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	// make sure fields match
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}
	insFields, err := conn.ValidateColumnNames(ColumnNames(columns), ds.GetFields(), true)
	if err != nil {
		err = h.Error(err, "columns mismatch")
		return
	}

	insertTemplate := conn.Self().GenerateInsertStatement(tableFName, insFields, 1)

	tx, err := conn.db.Begin()
	if err != nil {
		err = h.Error(err, "Could not begin transaction")
		return
	}
	for row := range ds.Rows {
		count++
		// Do insert
		_, err := tx.Exec(insertTemplate, row...)
		if err != nil {
			tx.Rollback()
			return count, h.Error(
				err,
				fmt.Sprintf("Insert: %s\nFor Row: %#v", insertTemplate, row),
			)
		}
	}
	tx.Commit()

	return count, nil
}

func (conn *BaseConn) getNativeType(col iop.Column) (nativeType string, err error) {

	nativeType, ok := conn.template.GeneralTypeMap[col.Type]
	if !ok {
		err = fmt.Errorf(
			"No native type mapping defined for col '%s', with type '%s' ('%s') for '%s'",
			col.Name,
			col.Type,
			col.DbType,
			conn.Type,
		)
		// return "", h.Error(err)
		h.Warn(err.Error() + ". Using 'string'")
		err = nil
		nativeType = conn.template.GeneralTypeMap["string"]
	}

	// Add precision as needed
	if strings.HasSuffix(nativeType, "()") {
		length := col.Stats.MaxLen * 2
		if col.Type == "string" {
			if length < 255 {
				length = 255
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		} else if col.Type == "integer" {
			if length < ddlDefDecLength {
				length = ddlDefDecLength
			}
			nativeType = strings.ReplaceAll(
				nativeType,
				"()",
				fmt.Sprintf("(%d)", length),
			)
		}
	} else if strings.HasSuffix(nativeType, "(,)") {
		length := col.Stats.MaxLen * 2
		scale := col.Stats.MaxDecLen + 2
		length = ddlMaxDecLength // max out
		scale = ddlMaxDecScale   // max out
		if strings.Contains("float,smallint,integer,bigint,decimal", col.Type) {
			if length < ddlMaxDecScale {
				length = ddlMaxDecScale
			} else if length > ddlMaxDecLength {
				length = ddlMaxDecLength
			}
			if scale < ddlMinDecScale {
				scale = ddlMinDecScale
			} else if scale > ddlMaxDecScale {
				scale = ddlMaxDecScale
			}
		}
		nativeType = strings.ReplaceAll(
			nativeType,
			"(,)",
			fmt.Sprintf("(%d,%d)", length, scale),
		)
	}

	return
}

func (conn *BaseConn) generateColumnDDL(col iop.Column, nativeType string) (columnDDL string) {

	columnDDL = h.R(
		conn.GetTemplateValue("core.modify_column"),
		"column", conn.Self().Quote(col.Name),
		"type", nativeType,
	)

	return
}

// GenerateDDL genrate a DDL based on a dataset
func (conn *BaseConn) GenerateDDL(tableFName string, data iop.Dataset) (string, error) {

	if !data.Inferred || data.SafeInference {
		data.InferColumnTypes()
	}
	columnsDDL := []string{}

	if !data.NoTrace && len(data.Rows) > 0 {
		h.Trace("%#v", data.Rows[len(data.Rows)-1])
	}

	if len(data.Columns) > 0 && data.Columns[0].Stats.TotalCnt == 0 && data.Columns[0].Type == "" {
		h.Warn("Generating DDL from 0 rows. Will use string for unknown types.")
	}

	for _, col := range data.Columns {
		// convert from general type to native type
		nativeType, err := conn.getNativeType(col)
		if err != nil {
			return "", h.Error(err, "no native mapping")
		}

		if !data.NoTrace {
			h.Trace(
				"%s - %s (maxLen: %d, nullCnt: %d, totCnt: %d, strCnt: %d, dtCnt: %d, intCnt: %d, decCnt: %d)",
				col.Name, col.Type,
				col.Stats.MaxLen, col.Stats.NullCnt,
				col.Stats.TotalCnt, col.Stats.StringCnt,
				col.Stats.DateCnt, col.Stats.IntCnt,
				col.Stats.DecCnt,
			)
		}

		columnDDL := col.Name + " " + nativeType
		columnsDDL = append(columnsDDL, columnDDL)
	}

	ddl := h.R(
		conn.template.Core["create_table"],
		"table", tableFName,
		"col_types", strings.Join(columnsDDL, ",\n"),
	)

	return ddl, nil
}

// BulkImportFlow imports the streams rows in bulk concurrently using channels
func (conn *BaseConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {

	h.Trace("BulkImportFlow not implemented for %s", conn.GetType())

	doImport := func(tableFName string, ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()

		cnt, err := conn.Self().BulkImportStream(tableFName, ds)
		count += cnt
		if err != nil {
			df.Context.CaptureErr(h.Error(err, "could not bulk import"))
		}
	}

	for ds := range df.StreamCh {
		df.Context.Wg.Write.Add()
		go doImport(tableFName, ds)
	}

	df.Context.Wg.Write.Wait()

	return count, df.Context.Err()
}

// BulkExportFlow creates a dataflow from a sql query
func (conn *BaseConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	h.Trace("BulkExportFlow not implemented for %s", conn.GetType())
	if UseBulkExportFlowCSV {
		return conn.BulkExportFlowCSV(sqls...)
	}
	columns, err := conn.Self().GetSQLColumns(sqls...)
	if err != nil {
		err = h.Error(err, "Could not get columns.")
		return
	}

	df = iop.NewDataflow()
	df.Context = h.NewContext(conn.Context().Ctx)

	go func() {
		defer df.Close()
		dss := []*iop.Datastream{}

		for _, sql := range sqls {
			ds, err := conn.Self().BulkExportStream(sql)
			if err != nil {
				df.Context.CaptureErr(h.Error(err, "Error running query"))
				return
			}
			dss = append(dss, ds)
		}

		df.PushStreams(dss...)

	}()

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	df.SetColumns(columns)
	df.Inferred = true

	return df, nil
}

// BulkExportFlowCSV creates a dataflow from a sql query, using CSVs
func (conn *BaseConn) BulkExportFlowCSV(sqls ...string) (df *iop.Dataflow, err error) {

	columns, err := conn.Self().GetSQLColumns(sqls...)
	if err != nil {
		err = h.Error(err, "Could not get columns.")
		return
	}

	df = iop.NewDataflow()

	unload := func(sql string, pathPart string) {
		defer df.Context.Wg.Read.Done()
		fileReadyChn := make(chan string, 10000)
		ds, err := conn.Self().BulkExportStream(sql)
		if err != nil {
			df.Context.CaptureErr(h.Error(err, "Error running query"))
			conn.Context().Cancel()
			return
		}

		fs, err := iop.NewFileSysClient(iop.LocalFileSys, conn.PropArr()...)
		if err != nil {
			conn.Context().CaptureErr(h.Error(err, "Unable to create Local file sys Client"))
			ds.Context.Cancel()
			return
		}

		sqlDf, err := iop.MakeDataFlow(ds)
		if err != nil {
			conn.Context().CaptureErr(h.Error(err, "Unable to create data flow"))
			ds.Context.Cancel()
			return
		}

		go func() {
			bw, err := fs.Self().WriteDataflowReady(sqlDf, pathPart, fileReadyChn)
			if err != nil {
				conn.Context().CaptureErr(h.Error(err, "Unable to write to file: "+pathPart))
				ds.Context.Cancel()
				return
			}
			sqlDf.AddBytes(bw)
		}()

		for filePath := range fileReadyChn {
			// when the file is ready, push to dataflow
			nDs, err := iop.ReadCsvStream(filePath)
			if err != nil {
				conn.Context().CaptureErr(h.Error(err, "Unable to read stream: "+filePath))
				ds.Context.Cancel()
				df.Context.Cancel()
				return
			}
			nDs.Defer(func() { os.RemoveAll(filePath) })
			df.PushStreams(nDs)
		}
	}

	path := fmt.Sprintf("/tmp/%s/stream/%s/%s.csv", filePathStorageSlug, conn.GetType(), cast.ToString(h.Now()))

	go func() {
		defer df.Close()
		for i, sql := range sqls {
			pathPart := fmt.Sprintf("%s/sql%02d", path, i+1)
			df.Context.Wg.Read.Add()
			go unload(sql, pathPart)
		}

		// wait until all nDs are pushed to close
		df.Context.Wg.Read.Wait()
	}()

	// wait for first ds to start streaming.
	err = df.WaitReady()
	if err != nil {
		return df, h.Error(err)
	}

	h.Debug("Unloading to %s", path)
	df.SetColumns(columns)
	df.Inferred = true

	return
}

// CleanSQL removes creds from the query
func (conn *BaseConn) CleanSQL(sql string) string {
	for _, v := range conn.properties {
		if strings.TrimSpace(v) == "" {
			continue
		}
		sql = strings.ReplaceAll(sql, v, "***")
	}
	return sql
}

// GenerateUpsertExpressions returns a map with needed expressions
func (conn *BaseConn) GenerateUpsertExpressions(srcTable string, tgtTable string, pkFields []string) (exprs map[string]string, err error) {

	srcColumns, err := conn.GetColumns(srcTable)
	if err != nil {
		err = h.Error(err, "could not get columns for "+srcTable)
		return
	}
	srcCols := ColumnNames(srcColumns)
	tgtColumns, err := conn.GetColumns(tgtTable)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}

	pkFields, err = conn.ValidateColumnNames(ColumnNames(tgtColumns), pkFields, true)
	if err != nil {
		err = h.Error(err, "PK columns mismatch")
		return
	}
	pkFieldMap := map[string]string{}
	pkEqualFields := []string{}
	for _, pkField := range pkFields {
		pkEqualField := h.F("src.%s = tgt.%s", pkField, pkField)
		pkEqualFields = append(pkEqualFields, pkEqualField)
		pkFieldMap[pkField] = ""
	}

	srcFields, err := conn.ValidateColumnNames(ColumnNames(tgtColumns), srcCols, true)
	if err != nil {
		err = h.Error(err, "columns mismatch")
		return
	}

	tgtFields := srcFields
	setFields := []string{}
	insertFields := []string{}
	placeholdFields := []string{}
	for _, colName := range srcFields {
		insertFields = append(insertFields, colName)
		placeholdFields = append(placeholdFields, h.F("ph.%s", colName))
		if _, ok := pkFieldMap[colName]; !ok {
			// is not a pk field
			setField := h.F("%s = src.%s", colName, colName)
			setFields = append(setFields, setField)
		}
	}

	// cast into the correct type
	srcFields = conn.Self().CastColumnsForSelect(srcColumns, tgtColumns)

	exprs = map[string]string{
		"src_tgt_pk_equal": strings.Join(pkEqualFields, " and "),
		"src_upd_pk_equal": strings.ReplaceAll(strings.Join(pkEqualFields, ", "), "tgt.", "upd."),
		"src_fields":       strings.Join(srcFields, ", "),
		"tgt_fields":       strings.Join(tgtFields, ", "),
		"insert_fields":    strings.Join(insertFields, ", "),
		"pk_fields":        strings.Join(pkFields, ", "),
		"set_fields":       strings.Join(setFields, ", "),
		"placehold_fields": strings.Join(placeholdFields, ", "),
	}

	return
}

// GetColumnStats analyzes the table and returns the column statistics
func (conn *BaseConn) GetColumnStats(tableName string, fields ...string) (columns []iop.Column, err error) {

	tableColumns, err := conn.Self().GetColumns(tableName)
	if err != nil {
		err = h.Error(err, "could not obtain columns data")
		return
	}

	generalTypes := map[string]string{}
	statFields := []string{}
	for _, col := range tableColumns {
		generalType := col.Type
		colName := col.Name
		generalTypes[strings.ToLower(colName)] = generalType
		statFields = append(statFields, colName)
	}

	if len(fields) == 0 {
		fields = statFields
	}

	// run analysis field_stat_len
	data, err := conn.Self().RunAnalysisField("field_stat_len", tableName, fields...)
	if err != nil {
		err = h.Error(err, "could not analyze table")
		return
	}

	columns = []iop.Column{}
	for _, rec := range data.Records() {
		colName := cast.ToString(rec["field"])
		column := iop.Column{
			Name: colName,
			Type: generalTypes[strings.ToLower(colName)],
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
			column.Stats.MaxDecLen = ddlDefDecScale
		}
		columns = append(columns, column)
	}

	return

}

// OptimizeTable analyzes the table and alters the table with
// the columns data type based on its analysis result
// if table is missing, it is created with a new DDl
// Hole in this: will truncate data points, since it is based
// only on new data being inserted... would need a complete
// stats of the target table to properly optimize.
func (conn *BaseConn) OptimizeTable(tableName string, newColStats []iop.Column) (err error) {

	// stats are already provided with columns
	tgtColStats, err := conn.Self().GetColumnStats(tableName)
	if err != nil {
		return h.Error(err, "unable to run column stats")
	}

	colStats, err := iop.SyncColumns(newColStats, tgtColStats)
	if err != nil {
		return h.Error(err, "unable to sync columns data")
	}

	columns := iop.InferFromStats(colStats, false, false)

	// compare, and alter or recreate as needed
	colDDLs := []string{}
	for _, col := range columns {
		nativeType, err := conn.getNativeType(col)
		if err != nil {
			return h.Error(err, "no native mapping")
		}

		switch col.Type {
		case "string", "text", "json", "bytes":
			// alter field to resize column
			colDDL := h.R(
				conn.GetTemplateValue("core.modify_column"),
				"column", conn.Self().Quote(col.Name),
				"type", nativeType,
			)
			colDDLs = append(colDDLs, colDDL)
		default:
			// alter field to resize column
			colDDL := h.R(
				conn.GetTemplateValue("core.modify_column"),
				"column", conn.Self().Quote(col.Name),
				"type", nativeType,
			)
			colDDLs = append(colDDLs, colDDL)
		}

	}

	ddl := h.R(
		conn.GetTemplateValue("core.alter_columns"),
		"table", tableName,
		"col_ddls", strings.Join(colDDLs, ", "),
	)
	_, err = conn.Exec(ddl)
	if err != nil {
		err = h.Error(err, "could not alter columns on table "+tableName)
	}

	return
}

// CompareChecksums compares the checksum values from the database side
// to the checkum values from the StreamProcessor
func (conn *BaseConn) CompareChecksums(tableName string, columns []iop.Column) (err error) {
	tColumns, err := conn.GetColumns(tableName)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}

	// make sure columns exist in table, get common columns into fields
	fields, err := conn.ValidateColumnNames(ColumnNames(tColumns), ColumnNames(columns), false)
	if err != nil {
		err = h.Error(err, "columns mismatch")
		return
	}
	fieldsMap := h.ArrMapString(fields, true)
	h.Debug("comparing checksums %#v vs %#v: %#v", ColumnNames(tColumns), ColumnNames(columns), fields)

	exprs := []string{}
	colChecksum := map[string]uint64{}
	for _, col := range columns {
		colChecksum[strings.ToLower(col.Name)] = col.Stats.Checksum
		if _, ok := fieldsMap[strings.ToLower(col.Name)]; !ok {
			continue // making sure it is a common column
		}

		expr := ""
		switch {
		case col.IsString():
			expr = conn.GetTemplateValue("function.checksum_string")
		case col.IsInteger():
			expr = conn.GetTemplateValue("function.checksum_integer")
		case col.IsDecimal():
			expr = conn.GetTemplateValue("function.checksum_decimal")
		case col.IsDatetime():
			expr = conn.GetTemplateValue("function.checksum_datetime")
		case col.IsBool():
			expr = conn.GetTemplateValue("function.checksum_boolean")
		default:
			expr = "0"
		}
		colName := fieldsMap[strings.ToLower(col.Name)]
		expr = h.R(expr, "field", conn.Self().Quote(cast.ToString(colName)))
		exprs = append(exprs, h.F("sum(%s) as %s", expr, col.Name))
	}

	sql := h.F(
		"select %s from %s",
		strings.Join(exprs, ", "),
		tableName,
	)

	data, err := conn.Self().Query(sql)
	if err != nil {
		return h.Error(err, "error running CompareChecksums query")
	}
	// h.P(data.Rows[0])

	eg := h.ErrorGroup{}
	for i, col := range data.Columns {
		checksum1 := colChecksum[strings.ToLower(col.Name)]
		checksum2 := cast.ToUint64(data.Rows[0][i])
		if checksum1 != checksum2 {
			eg.Add(fmt.Errorf("checksum failure for %s: %d != %d", col.Name, checksum1, checksum2))
		}
	}

	return eg.Err()
}

func (conn *BaseConn) credsProvided(provider string) bool {
	if provider == "AWS" {
		if conn.GetProp("AWS_SECRET_ACCESS_KEY") != "" || conn.GetProp("AWS_ACCESS_KEY_ID") != "" {
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

// CopyDirect copies directly from cloud files
// (without passing through sling)
func (conn *BaseConn) CopyDirect(tableFName string, srcFile iop.DataConn) (cnt uint64, ok bool, err error) {
	ok = false
	return
}

// settingMppBulkImportFlow sets settings for MPP databases type
// for BulkImportFlow
func settingMppBulkImportFlow(conn Connection) {
	if cast.ToInt(conn.GetProp("SLING_FILE_ROW_LIMIT")) == 0 {
		conn.SetProp("SLING_FILE_ROW_LIMIT", "500000")
	}

	conn.SetProp("SLING_COMPRESSION", "GZIP")

	conn.SetProp("SLING_PARALLEL", "true")
}

// ToData converts schema objects to tabular format
func (schema *Schema) ToData() (data iop.Dataset) {
	columns := []string{"schema_name", "table_name", "is_view", "column_id", "column_name", "column_type"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns))

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			row := []interface{}{schema.Name, table.Name, table.IsView, col.Position, col.Name, col.DbType}
			data.Rows = append(data.Rows, row)
		}
	}
	return
}

// TestPermissions tests the needed permissions in a given connection
func TestPermissions(conn Connection, tableName string) (err error) {

	type testObj struct {
		Title string
		SQL   string
		Skip  []DbType
	}

	col := iop.Column{Name: "col1", Type: "integer"}
	nativeType := conn.Template().GeneralTypeMap[col.Type]

	// drop table if exists
	err = conn.DropTable(tableName)
	if err != nil {
		return h.Error(err, "failed testing permissions: Drop table")
	}

	tests := []testObj{

		// Create table
		testObj{
			Title: "Create a test table",
			SQL: h.R(
				conn.GetTemplateValue("core.create_table"),
				"table", tableName,
				"col_types", col.Name+" "+nativeType,
			),
		},

		// Insert
		testObj{
			Title: "Insert into a test table",
			SQL: h.R(
				conn.GetTemplateValue("core.insert"),
				"table", tableName,
				"fields", col.Name,
				"values", "1",
			),
		},

		// Update
		testObj{
			Title: "Update the test table",
			SQL: h.R(
				conn.GetTemplateValue("core.update"),
				"table", tableName,
				"set_fields", h.F("%s = 2", col.Name),
				"pk_fields_equal", "1=1",
			),
		},

		// Delete
		testObj{
			Title: "Delete from the test table",
			SQL: h.R(
				"delete from {table} where 1=0",
				"table", tableName,
			),
		},

		// Truncate
		testObj{
			Title: "Truncate the test table",
			SQL: h.R(
				conn.GetTemplateValue("core.truncate_table"),
				"table", tableName,
			),
		},

		// Drop table
		testObj{
			Title: "Drop the test table",
			SQL: h.R(
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
			return h.Error(err, "failed testing permissions: %s", test.Title)
		}
	}

	return
}
