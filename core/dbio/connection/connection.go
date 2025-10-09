package connection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"gopkg.in/yaml.v2"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio/api"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/spf13/cast"
)

// Info is the connection type
type Info struct {
	Name     string
	Type     dbio.Type
	LongType string
	Database string
	Data     map[string]interface{}
}

// ConnectionInt is a connection
type ConnectionInt interface {
	// Self() Connection
	Close() error
	Context() g.Context
	Info() Info
	URL() string
	DataS(lowerCase ...bool) map[string]string
	ToMap() map[string]interface{}
	AsDatabase() (database.Connection, error)
	AsFile() (filesys.FileSysClient, error)
	Set(map[string]interface{})
}

// Connection is the base connection struct
type Connection struct {
	Name    string                 `json:"name,omitempty"`
	Type    dbio.Type              `json:"type,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
	context *g.Context             `json:"-"`

	File     filesys.FileSysClient
	Database database.Connection
	API      *api.APIConnection
}

// NewConnection creates a new connection
func NewConnection(Name string, t dbio.Type, Data map[string]interface{}) (conn Connection, err error) {
	// prevent infinite recursion
	if localConnsExclude != "" && strings.EqualFold(Name, localConnsExclude) {
		return
	}

	conn = Connection{
		Name:    strings.TrimLeft(Name, "$"),
		Type:    t,
		Data:    g.AsMap(Data, true),
		context: g.NewContext(context.Background()),
	}

	err = conn.setURL()
	if err != nil {
		return conn, g.Error(err, "could not set URL for %s: %s", conn.Type, Name)
	}

	return conn, err
}

// NewConnectionFromURL creates a new connection from a url
func NewConnectionFromURL(Name, URL string) (conn Connection, err error) {
	return NewConnection(Name, "", g.M("url", URL))
}

// NewConnectionFromMap loads a Connection from a Map
func NewConnectionFromMap(m map[string]interface{}) (c Connection, err error) {
	data := g.AsMap(m["data"])
	name := cast.ToString(m["name"])
	Type := cast.ToString(m["type"])

	if name == "" {
		name = cast.ToString(data["name"])
	}
	if Type == "" {
		Type = cast.ToString(data["type"])
	}

	c, err = NewConnection(
		name,
		dbio.Type(Type),
		g.AsMap(m["data"]),
	)

	if c.Type == "" {
		c.Type = SchemeType(c.URL())
	}

	return
}

// NewConnectionFromDbt loads a Connection from a DBT Profile
func NewConnectionFromDbt(name string) (c Connection, err error) {
	conns, err := ReadDbtConnections()
	if err != nil {
		err = g.Error(err)
		return
	}

	if conn, ok := conns[name]; ok {
		return conn, nil
	}
	return c, g.Error("dbt connection '%s' was not found", name)
}

// NewConnectionFromProfiles loads a Connection from YAML Profiles
func NewConnectionFromProfiles(name string) (c Connection, err error) {
	profileConns := map[string]Connection{}
	for _, path := range strings.Split(os.Getenv("PROFILE_PATHS"), ",") {
		if strings.TrimSpace(path) == "" {
			continue
		}
		conns, err := ReadConnectionsFromFile(path)
		if err != nil {
			err = g.Error(err)
			return c, err
		}

		for k, v := range conns {
			profileConns[k] = v
		}
	}

	if conn, ok := profileConns[name]; ok {
		return conn, nil
	}
	return
}

// Info returns connection information
func (c *Connection) Info() Info {
	name := c.Name
	if strings.Contains(name, "://") {
		name = strings.Split(name, "://")[0] + "://" // avoid leaking sensitive info
	}
	return Info{
		Name:     name,
		Type:     c.Type,
		LongType: c.Type.NameLong(),
		Database: c.DataS(true)["database"],
		Data:     c.Data,
	}
}

func (c *Connection) Hash() string {
	parts := []string{c.Name, c.Type.Name()}
	keys := lo.Keys(c.Data)
	sort.Strings(keys)
	for _, key := range keys {
		value := g.F("%s=%s", key, g.Marshal(c.Data[key]))
		parts = append(parts, value)
	}

	return g.MD5(parts...)
}

// ToMap transforms DataConn to a Map
func (c *Connection) ToMap() map[string]interface{} {
	data := g.M()
	g.JSONConvert(c.Data, &data) // so that pointers are not modified downstream
	return g.M("name", c.Name, "type", c.Type, "data", data)
}

// ToMap transforms DataConn to a Map
func (c *Connection) Copy() *Connection {
	nc, _ := NewConnectionFromMap(c.ToMap())
	return &nc
}

// Set sets key/values from a map
func (c *Connection) Set(m map[string]interface{}) {
	for k, v := range m {
		c.Data[k] = v
	}
	c.setURL()
}

// DataS returns data as map[string]string
func (c *Connection) DataS(lowerCase ...bool) map[string]string {
	lc := false
	if len(lowerCase) > 0 {
		lc = lowerCase[0]
	}
	data := map[string]string{"name": strings.ToLower(c.Name)}
	for k, v := range c.Data {
		val, err := cast.ToStringE(v)
		if err != nil {
			val = g.Marshal(v) // in case it's an array or object
		}

		if lc {
			data[strings.ToLower(k)] = val
		} else {
			data[k] = val
		}
	}
	return data
}

func (c *Connection) DataSWithExtra(extra map[string]any) map[string]string {
	data := c.DataS()
	for k, v := range extra {
		val, err := cast.ToStringE(v)
		if err != nil {
			val = g.Marshal(v) // in case it's an array or object
		}
		data[k] = val
	}
	return data
}

// Context returns the context
func (c *Connection) Context() *g.Context {
	return c.context
}

// URL returns the url string
func (c *Connection) URL() string {
	url := cast.ToString(c.Data["url"])
	if c.Type.IsFile() && url == "" {
		url = c.Type.String() + "://"
		switch c.Type {
		case dbio.TypeFileLocal:
			url = "file://"
		case dbio.TypeFileSftp:
			url = g.F("%s://%s:%s", c.Type.String(), c.Data["host"], cast.ToString(c.Data["port"]))
		case dbio.TypeFileFtp:
			url = g.F("%s://%s:%s", c.Type.String(), c.Data["host"], cast.ToString(c.Data["port"]))
		case dbio.TypeFileS3:
			url = g.F("%s://%s", c.Type.String(), c.Data["bucket"])
		case dbio.TypeFileGoogle:
			url = g.F("%s://%s", c.Type.String(), c.Data["bucket"])
		case dbio.TypeFileGoogleDrive:
			url = "gdrive://"
		case dbio.TypeFileAzure:
			url = g.F("https://%s.blob.core.windows.net/%s", c.Data["account"], c.Data["container"])
		case dbio.TypeFileAzureABFS:
			filesystem := c.Data["filesystem"]
			parent := c.Data["parent"]
			if filesystem == "" {
				filesystem = c.Data["container"] // fallback for compatibility
			}
			// Check if custom endpoint is provided or if account contains full domain
			if endpoint := c.Data["endpoint"]; endpoint != "" {
				url = g.F("https://%s/%s", endpoint, filesystem)
			} else if account := cast.ToString(c.Data["account"]); strings.Contains(account, ".") {
				// Account already contains full domain (e.g., onelake.dfs.fabric.microsoft.com)
				url = g.F("https://%s/%s", account, filesystem)
			} else {
				// Standard Azure Storage account
				url = g.F("https://%s.dfs.core.windows.net/%s", c.Data["account"], filesystem)
			}
			if parent != "" {
				url = g.F("%s/%s", url, parent)
			}
		}
	}

	switch c.Type {
	case dbio.TypeDbDuckDb:
		// fix windows path
		url = strings.ReplaceAll(url, `\`, `/`)
	}

	return url
}

// Close closes the connection
func (c *Connection) Close() error {
	// remove from cache
	defer connCache.Remove(c.Hash())

	if c.Database != nil {
		return c.Database.Close()
	}

	if c.File != nil {
		return c.File.Close()
	}

	if c.API != nil {
		return c.API.Close()
	}
	return nil
}

var connCache = cmap.New[*Connection]()

type AsConnOptions struct {
	UseCache bool
	Extra    map[string]any
}

func (c *Connection) AsDatabase(options ...AsConnOptions) (dc database.Connection, err error) {
	return c.AsDatabaseContext(c.Context().Ctx, options...)
}

func (c *Connection) AsDatabaseContext(ctx context.Context, options ...AsConnOptions) (dc database.Connection, err error) {
	if !c.Type.IsDb() {
		return nil, g.Error("not a database type: %s", c.Type)
	}

	// default cache to true
	opt := AsConnOptions{UseCache: true, Extra: g.M()}
	if len(options) > 0 {
		opt = options[0]
	}

	// build input data

	if cc, ok := connCache.Get(c.Hash()); ok && opt.UseCache {
		if cc.Database != nil {
			return cc.Database, nil
		}
	}

	data := c.DataSWithExtra(opt.Extra)
	c.Database, err = database.NewConnContext(
		ctx, c.URL(), g.MapToKVArr(data)...,
	)
	if err != nil {
		return
	}
	connCache.Set(c.Hash(), c) // cache

	return c.Database, nil
}

func (c *Connection) AsFile(options ...AsConnOptions) (fc filesys.FileSysClient, err error) {
	return c.AsFileContext(c.Context().Ctx, options...)
}

func (c *Connection) AsFileContext(ctx context.Context, options ...AsConnOptions) (fc filesys.FileSysClient, err error) {
	if !c.Type.IsFile() {
		return nil, g.Error("not a file system type: %s", c.Type)
	}

	// default cache to true
	opt := AsConnOptions{UseCache: true, Extra: g.M()}
	if len(options) > 0 {
		opt = options[0]
	}

	// default cache to true
	if cc, ok := connCache.Get(c.Hash()); ok && opt.UseCache {
		if cc.File != nil {
			return cc.File, nil
		}
	}

	// build input data
	data := c.DataSWithExtra(opt.Extra)
	c.File, err = filesys.NewFileSysClientFromURLContext(
		ctx, c.URL(), g.MapToKVArr(data)...,
	)
	if err != nil {
		return
	}
	connCache.Set(c.Hash(), c) // cache

	return c.File, nil
}

func (c *Connection) AsAPI(options ...AsConnOptions) (ac *api.APIConnection, err error) {
	return c.AsAPIContext(c.Context().Ctx, options...)
}

func (c *Connection) AsAPIContext(ctx context.Context, options ...AsConnOptions) (ac *api.APIConnection, err error) {
	if !c.Type.IsAPI() {
		return nil, g.Error("not a api connection type: %s", c.Type)
	}

	// load spec
	spec, err := LoadAPISpec(cast.ToString(c.Data["spec"]))
	if err != nil {
		return nil, g.Error(err, "could not load spec")
	}

	// default cache to true
	opt := AsConnOptions{UseCache: true, Extra: g.M()}
	if len(options) > 0 {
		opt = options[0]
	}

	// add connection name
	data := maps.Clone(c.Data)
	data["name"] = strings.ToLower(c.Name)
	for k, v := range opt.Extra {
		data[k] = v
	}

	// default cache to true
	if cc, ok := connCache.Get(c.Hash()); ok && opt.UseCache {
		if cc.API != nil {
			return cc.API, nil
		}
	}

	c.API, err = api.NewAPIConnection(ctx, spec, data)
	if err != nil {
		return
	}
	connCache.Set(c.Hash(), c) // cache

	return c.API, nil
}

func (c *Connection) setFromEnv() {
	if c.Name == "" && strings.HasPrefix(c.URL(), "$") {
		c.Name = strings.TrimLeft(c.URL(), "$")
	}

	if newURL := os.Getenv(strings.TrimLeft(c.URL(), "$")); newURL != "" {
		c.Data["url"] = newURL
	}

	for k, v := range c.Data {
		val := cast.ToString(v)
		if strings.HasPrefix(val, "$") {
			varKey := strings.TrimLeft(val, "$")
			if newVal := os.Getenv(varKey); newVal != "" {
				c.Data[k] = newVal
			} else {
				g.Warn("No env var value found for %s", val)
			}
		}
	}
}

// ConnSetDatabase returns a new connection with the specified
// database name
func (c *Connection) ConnSetDatabase(dbName string) *Connection {
	data := g.AsMap(c.Data)
	if dbName != "" {
		data["database"] = dbName
	}
	c2, _ := NewConnection(c.Name, c.Type, data)
	return &c2
}

func (c *Connection) setURL() (err error) {
	c.setFromEnv()

	// setIfMissing sets a default value if key is not present
	setIfMissing := func(key string, val interface{}) {
		if v, ok := c.Data[key]; !ok || v == "" {
			c.Data[key] = val
		}
	}

	// if URL is provided, extract properties from it
	if strings.HasPrefix(c.URL(), "file://") {
		c.Type = dbio.TypeFileLocal
		setIfMissing("type", c.Type)
	} else if c.URL() != "" {
		U, err := net.NewURL(c.URL())
		if err != nil {
			// this does not return the full error since that can leak passwords
			// only in TRACE debug mode
			g.Trace(err.Error())
			return g.Error("could not parse provided credentials / url")
		}

		c.Type = SchemeType(c.URL())
		setIfMissing("type", c.Type)

		if c.Type.IsDb() {
			// set props from URL
			pathValue := strings.ReplaceAll(U.Path(), "/", "")
			setIfMissing("schema", U.PopParam("schema"))

			if !g.In(c.Type, dbio.TypeDbMotherDuck, dbio.TypeDbDuckDb, dbio.TypeDbDuckLake, dbio.TypeDbSQLite, dbio.TypeDbD1, dbio.TypeDbBigQuery) {
				setIfMissing("host", U.Hostname())
				setIfMissing("username", U.Username())
				setIfMissing("password", U.Password())
				setIfMissing("port", U.Port(c.Info().Type.DefPort()))
			}

			if g.In(c.Type, dbio.TypeDbPostgres, dbio.TypeDbRedshift) {
				setIfMissing("sslmode", U.PopParam("sslmode"))
				setIfMissing("role", U.PopParam("role"))
			}

			switch c.Type {
			case dbio.TypeDbSnowflake:
				setIfMissing("warehouse", U.PopParam("warehouse"))
			case dbio.TypeDbBigQuery:
				setIfMissing("location", U.PopParam("location"))
				setIfMissing("project", U.Hostname())
			case dbio.TypeDbBigTable:
				setIfMissing("project", U.Hostname())
				setIfMissing("instance", pathValue)
			case dbio.TypeDbSQLite, dbio.TypeDbDuckDb:
				setIfMissing("instance", U.Path())
				setIfMissing("schema", "main")
			case dbio.TypeDbMotherDuck:
				setIfMissing("schema", "main")
			case dbio.TypeDbD1:
				setIfMissing("schema", "main")
				setIfMissing("host", U.Hostname())
				setIfMissing("password", U.Password())
			case dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbFabric:
				setIfMissing("instance", pathValue)
				setIfMissing("database", U.PopParam("database"))
			case dbio.TypeDbOracle:
				setIfMissing("sid", pathValue)
			}

			// set database
			setIfMissing("database", pathValue)

			// pass query params
			for k, v := range U.Query() {
				setIfMissing(k, v)
			}
		}
		if g.In(c.Type, dbio.TypeFileSftp, dbio.TypeFileFtp) {
			setIfMissing("user", U.Username())
			setIfMissing("host", U.Hostname())
			setIfMissing("password", U.Password())
			setIfMissing("port", U.Port(c.Info().Type.DefPort()))
		}
		if c.Type == dbio.TypeFileS3 || c.Type == dbio.TypeFileGoogle {
			setIfMissing("bucket", U.U.Host)
		}
		if c.Type == dbio.TypeFileGoogleDrive {
			setIfMissing("folder_id", U.U.Host)
		}
		if c.Type == dbio.TypeFileAzure {
			account := strings.ReplaceAll(U.U.Host, ".blob.core.windows.net", "")
			account = strings.ReplaceAll(account, ".dfs.core.windows.net", "")
			setIfMissing("account", account)
			setIfMissing("container", strings.ReplaceAll(U.U.Path, "/", ""))
		}
		if c.Type == dbio.TypeFileAzureABFS {
			switch {
			case strings.HasPrefix(c.URL(), "http"):
				account := strings.ReplaceAll(U.U.Host, ".dfs.core.windows.net", "")
				account = strings.ReplaceAll(account, ".dfs.fabric.microsoft.com", "")
				setIfMissing("account", account)
				// Store the full endpoint if it's not standard Azure
				if strings.Contains(U.U.Host, ".dfs.fabric.microsoft.com") {
					setIfMissing("endpoint", U.U.Host)
				}
				// Extract filesystem (first path segment only)
				pathParts := strings.Split(strings.TrimPrefix(U.U.Path, "/"), "/")
				if len(pathParts) > 0 {
					setIfMissing("filesystem", pathParts[0])
				}
				if len(pathParts) > 1 {
					setIfMissing("parent", pathParts[1])
				}
			case strings.HasPrefix(c.URL(), "abfs"):
				account := strings.ReplaceAll(U.U.Host, ".dfs.core.windows.net", "")
				account = strings.ReplaceAll(account, ".dfs.fabric.microsoft.com", "")
				setIfMissing("account", account)

				// Store the full endpoint if it's not standard Azure
				if strings.Contains(U.U.Host, ".dfs.fabric.microsoft.com") {
					setIfMissing("endpoint", U.U.Host)
				}

				setIfMissing("filesystem", U.Username())
				pathParts := strings.Split(strings.TrimPrefix(U.U.Path, "/"), "/")
				if len(pathParts) > 0 {
					setIfMissing("parent", pathParts[0])
				}
			}
		}
	}

	template := ""

	// checkData checks c.Data for any missing keys
	checkData := func(keys ...string) error {
		eG := g.ErrorGroup{}
		for _, k := range keys {
			if _, ok := c.Data[k]; !ok {
				eG.Add(errors.New(g.F("Property value not provided: %s", k)))
			}
		}
		if err = eG.Err(); err != nil {
			return g.Error(err)
		}
		return nil
	}

	switch c.Type {
	case dbio.TypeDbOracle:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("database", c.Data["sid"])
		setIfMissing("database", c.Data["service_name"])
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("client_charset", "UTF8")
		if tns, ok := c.Data["tns"]; ok && cast.ToString(tns) != "" {
			if !strings.HasPrefix(cast.ToString(tns), "(") {
				c.Data["tns"] = "(" + cast.ToString(tns) + ")"
			}
			template = "oracle://{username}:{password}@{tns}"
		} else {
			template = "oracle://{username}:{password}@{host}:{port}/{database}"
		}

		if _, ok := c.Data["jdbc_str"]; ok {
			template = "oracle://"
		}
	case dbio.TypeDbPostgres:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("sslmode", "disable")
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("database", c.Data["dbname"])
		template = "postgresql://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
		if _, ok := c.Data["role"]; ok {
			template = template + "&role={role}"
		}
		if _, ok := c.Data["statement_timeout"]; ok {
			template = template + "&statement_timeout={statement_timeout}"
		}
	case dbio.TypeDbRedshift:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("sslmode", "disable")
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("database", c.Data["dbname"])
		template = "redshift://{username}:{password}@{host}:{port}/{database}?sslmode={sslmode}"
	case dbio.TypeDbStarRocks:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "starrocks://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeDbMySQL:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "mysql://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeDbMariaDB:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "mariadb://{username}:{password}@{host}:{port}/{database}"
	case dbio.TypeDbBigQuery:
		setIfMissing("dataset", c.Data["schema"])
		setIfMissing("schema", c.Data["dataset"])
		setIfMissing("location", "US")
		template = "bigquery://{project}/{location}/{dataset}?"
		if val, ok := c.Data["key_file"]; ok {
			c.Data["keyfile"] = val
			delete(c.Data, "key_file")
		}
		if _, ok := c.Data["keyfile"]; ok {
			template = template + "&credentialsFile={keyfile}"
		}
	case dbio.TypeDbMongoDB:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = "mongodb://{username}:{password}@{host}:{port}"
	case dbio.TypeDbElasticsearch:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())

		// parse http url
		if httpUrlStr, ok := c.Data["http_url"]; ok {
			u, err := url.Parse(cast.ToString(httpUrlStr))
			if err != nil {
				g.Warn("invalid http_url: %s", err.Error())
			} else {
				setIfMissing("host", u.Hostname())
			}
		}

		if cloudID := cast.ToString(c.Data["cloud_id"]); cloudID != "" {
			template = "elasticsearch://"
		} else {
			template = "elasticsearch://{username}:{password}@{host}:{port}"
		}
	case dbio.TypeDbPrometheus:
		setIfMissing("api_key", "")
		setIfMissing("port", c.Type.DefPort())

		// parse http url
		if httpUrlStr, ok := c.Data["http_url"]; ok {
			u, err := url.Parse(cast.ToString(httpUrlStr))
			if err != nil {
				g.Warn("invalid http_url: %s", err.Error())
			} else {
				setIfMissing("host", u.Hostname())
			}
		}

		template = "prometheus://{host}"
	case dbio.TypeDbAzureTable:
		setIfMissing("account_name", c.Data["account"])
		setIfMissing("account_key", "")
		template = "azuretable://{account_name}"
	case dbio.TypeDbBigTable:
		template = "bigtable://{project}/{instance}?"
		if _, ok := c.Data["keyfile"]; ok {
			template = template + "&credentialsFile={keyfile}"
		}
	case dbio.TypeDbSnowflake:
		// setIfMissing("schema", "public")
		// template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?schema={schema}&warehouse={warehouse}"
		setIfMissing("username", c.Data["user"])
		setIfMissing("host", c.Data["account"])
		setIfMissing("password", "") // make password optional, especially when using a private key
		c.Data["host"] = strings.ReplaceAll(cast.ToString(c.Data["host"]), ".snowflakecomputing.com", "")
		template = "snowflake://{username}:{password}@{host}.snowflakecomputing.com:443/{database}?"
		if _, ok := c.Data["warehouse"]; ok {
			template = template + "&warehouse={warehouse}"
		}
		if _, ok := c.Data["role"]; ok {
			template = template + "&role={role}"
		}
		// if _, ok := c.Data["schema"]; ok {
		// 	template = template + "&schema={schema}"
		// }
		if _, ok := c.Data["authenticator"]; ok {
			template = template + "&authenticator={authenticator}"
		}
		if _, ok := c.Data["passcode"]; ok {
			template = template + "&passcode={passcode}"
		}
	case dbio.TypeDbDatabricks:
		setIfMissing("token", c.Data["password"])
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("warehouse_id", c.Data["warehouse_id"]) // Default as per documentation
		setIfMissing("max_rows", 10000)                      // Default as per documentation

		// databricks uses a custom connection string format
		// "token:<my_token>@hostname:port/http_path?catalog=hive_metastore&schema=default&timeout=60&maxRows=100&timezone=America/Sao_Paulo&ansi_mode=true"
		if httpPath := cast.ToString(c.Data["http_path"]); httpPath != "" {
			setIfMissing("http_path", httpPath)
			template = "databricks://token:{token}@{host}:{port}{http_path}"
		} else {
			template = "databricks://token:{token}@{host}:{port}/sql/1.0/warehouses/{warehouse_id}"
		}
	case dbio.TypeDbD1:
		setIfMissing("account_id", c.Data["host"])
		setIfMissing("api_token", c.Data["password"])
		template = "d1://user:{api_token}@{account_id}/{database}"
	case dbio.TypeDbSQLite:
		if val, ok := c.Data["instance"]; ok {
			dbURL, err := net.NewURL(cast.ToString(val))
			if err == nil && g.In(dbURL.U.Scheme, "s3", "http", "https") {
				setIfMissing("http_url", dbURL.String())
				c.Data["instance"] = dbURL.Path()
			} else {
				c.Data["instance"] = strings.ReplaceAll(cast.ToString(val), `\`, `/`) // windows path fix
			}
		}
		template = "sqlite://{instance}?cache=shared&mode=rwc&_journal_mode=WAL&_synchronous=NORMAL"
	case dbio.TypeDbDuckDb:
		if val, ok := c.Data["instance"]; ok {
			dbURL, err := net.NewURL(cast.ToString(val))
			if err == nil && g.In(dbURL.U.Scheme, "s3", "http", "https") {
				setIfMissing("http_url", dbURL.String())
				c.Data["instance"] = dbURL.Path()
			} else {
				c.Data["instance"] = strings.ReplaceAll(cast.ToString(val), `\`, `/`) // windows path fix
			}
		}
		setIfMissing("schema", "main")
		template = "duckdb://{instance}?schema={schema}"
	case dbio.TypeDbDuckLake:
		if val, ok := c.Data["instance"]; ok {
			dbURL, err := net.NewURL(cast.ToString(val))
			if err == nil && g.In(dbURL.U.Scheme, "s3", "http", "https") {
				setIfMissing("http_url", dbURL.String())
				c.Data["instance"] = dbURL.Path()
			} else {
				c.Data["instance"] = strings.ReplaceAll(cast.ToString(val), `\`, `/`) // windows path fix
			}
		}

		setIfMissing("schema", "main")

		// Storage credentials for various backends
		// S3
		setIfMissing("s3_access_key_id", c.Data["s3_access_key_id"])
		setIfMissing("s3_secret_access_key", c.Data["s3_secret_access_key"])
		setIfMissing("s3_session_token", c.Data["s3_session_token"])
		setIfMissing("s3_region", c.Data["s3_region"])
		setIfMissing("s3_endpoint", c.Data["s3_endpoint"])

		// Azure
		setIfMissing("azure_account_name", c.Data["azure_account_name"])
		setIfMissing("azure_account_key", c.Data["azure_account_key"])
		setIfMissing("azure_sas_token", c.Data["azure_sas_token"])
		setIfMissing("azure_tenant_id", c.Data["azure_tenant_id"])
		setIfMissing("azure_client_id", c.Data["azure_client_id"])
		setIfMissing("azure_client_secret", c.Data["azure_client_secret"])
		setIfMissing("azure_connection_string", c.Data["azure_connection_string"])

		// GCS
		setIfMissing("gcs_key_file", c.Data["gcs_key_file"])
		setIfMissing("gcs_project_id", c.Data["gcs_project_id"])
		setIfMissing("gcs_access_key_id", c.Data["gcs_access_key_id"])
		setIfMissing("gcs_secret_access_key", c.Data["gcs_secret_access_key"])

		// Catalog configuration
		setIfMissing("catalog_type", c.Data["catalog_type"])
		setIfMissing("catalog_conn_string", c.Data["catalog_conn_string"])
		setIfMissing("data_path", c.Data["data_path"])

		// Build the ducklake URL based on catalog configuration
		// Default to simple ducklake:// if no specific catalog URL is provided
		template = "ducklake://"
	case dbio.TypeDbMotherDuck:
		setIfMissing("schema", "main")
		setIfMissing("interactive", true)
		template = "motherduck://{database}?interactive={interactive}&motherduck_token={motherduck_token}"
	case dbio.TypeDbSQLServer, dbio.TypeDbAzure, dbio.TypeDbAzureDWH, dbio.TypeDbFabric:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("app_name", "sling")

		template = "sqlserver://{username}:{password}@{host}"
		if c.Type == dbio.TypeDbFabric {
			template = "fabric://{username}:{password}@{host}"
		}

		_, port_ok := c.Data["port"]
		_, instance_ok := c.Data["instance"]

		switch {
		case port_ok:
			template += ":{port}"
		case instance_ok:
			template += "/{instance}"
		default:
			template += ":{port}"
			setIfMissing("port", c.Type.DefPort())
		}

		template = template + "?"
		if _, ok := c.Data["database"]; ok {
			template = template + "&database={database}"
		}

	case dbio.TypeDbTrino:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())

		// parse http url
		if httpUrlStr, ok := c.Data["http_url"]; ok {
			u, err := net.NewURL(cast.ToString(httpUrlStr))
			if err != nil {
				g.Warn("invalid http_url: %s", err.Error())
			} else {
				setIfMissing("host", u.Hostname())
			}
			setIfMissing("catalog", u.GetParam("catalog"))
			setIfMissing("schema", u.GetParam("schema"))
		}

		template = "trino://{username}:{password}@{host}:{port}?catalog={catalog}"
		if _, ok := c.Data["schema"]; ok {
			template = template + "&schema={schema}"
		}
	case dbio.TypeDbClickhouse:
		setIfMissing("username", c.Data["user"])
		setIfMissing("username", "") // clickhouse can work without a user
		setIfMissing("password", "")
		setIfMissing("schema", c.Data["database"])
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("secure", "false")
		setIfMissing("skip_verify", "false")

		// parse http url
		if httpUrlStr, ok := c.Data["http_url"]; ok {
			u, err := url.Parse(cast.ToString(httpUrlStr))
			if err != nil {
				g.Warn("invalid http_url: %s", err.Error())
			} else {
				setIfMissing("host", u.Hostname())
			}
			setIfMissing("database", "default")
		}

		template = "clickhouse://{username}:{password}@{host}:{port}/{database}?secure={secure}&skip_verify={skip_verify}"
	case dbio.TypeDbProton:
		setIfMissing("username", c.Data["user"])
		setIfMissing("username", "") // proton can work without a user
		setIfMissing("password", "")
		setIfMissing("schema", c.Data["database"])
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("secure", "false")
		setIfMissing("skip_verify", "false")

		// parse http url
		if httpUrlStr, ok := c.Data["http_url"]; ok {
			u, err := url.Parse(cast.ToString(httpUrlStr))
			if err != nil {
				g.Warn("invalid http_url: %s", err.Error())
			} else {
				setIfMissing("host", u.Hostname())
			}
			setIfMissing("database", "default")
		}

		template = "proton://{username}:{password}@{host}:{port}/{database}?secure={secure}&skip_verify={skip_verify}"
	case dbio.TypeDbExasol:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("schema", "")

		template = "exasol://{username}:{password}@{host}:{port}/?"
		if _, ok := c.Data["schema"]; ok && c.Data["schema"] != "" {
			template = template + "&schema={schema}"
		}
	case dbio.TypeDbArrowFlight:
		setIfMissing("username", c.Data["user"])
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		setIfMissing("schema", "")

		template = "flightsql://{username}:{password}@{host}:{port}/?"
		if _, ok := c.Data["schema"]; ok && c.Data["schema"] != "" {
			template = template + "&schema={schema}"
		}
	case dbio.TypeFileSftp, dbio.TypeFileFtp:
		setIfMissing("password", "")
		setIfMissing("port", c.Type.DefPort())
		template = c.Type.String() + "://{user}:{password}@{host}:{port}/"
		if path := cast.ToString(c.Data["path"]); path != "" {
			template = template + path
		}
	case dbio.TypeFileS3, dbio.TypeFileGoogle, dbio.TypeFileGoogleDrive, dbio.TypeFileAzure, dbio.TypeFileAzureABFS,
		dbio.TypeFileLocal:
		return nil
	case dbio.TypeDbIceberg:
		setIfMissing("catalog_type", c.Data["catalog_type"]) // rest, glue, s3tables, sql

		// SQL catalog specific properties
		if c.Data["catalog_type"] == "sql" {
			setIfMissing("sql_catalog_name", c.Data["sql_catalog_name"])
			setIfMissing("sql_catalog_init", "true")
		}

		if connName := c.Data["sql_catalog_conn"]; connName != nil {
			// create database object and pass for connection
			exclude := LocalConnsExclude(c.Name)
			entry := GetLocalConns(exclude).Get(cast.ToString(connName))
			if entry.Name != "" {
				c.Data["sql_conn_payload"] = g.M(
					"url", entry.Connection.URL(),
					"data", entry.Connection.DataS(),
				)
			}
		}

		template = "iceberg://{catalog_type}"
	case dbio.TypeDbAthena:
		// use dbt inputs
		{
			setIfMissing("aws_region", c.Data["region_name"])
			setIfMissing("aws_profile", c.Data["aws_profile_name"])
			setIfMissing("workgroup", c.Data["work_group"])
			setIfMissing("data_location", c.Data["s3_data_dir"])
			setIfMissing("staging_location", c.Data["s3_staging_dir"])
		}

		setIfMissing("aws_access_key_id", c.Data["user"])
		setIfMissing("aws_access_key_id", c.Data["access_key_id"])
		setIfMissing("aws_secret_access_key", nil)
		setIfMissing("aws_secret_access_key", c.Data["secret_access_key"])
		setIfMissing("aws_region", c.Data["region"])
		setIfMissing("aws_profile", nil)
		setIfMissing("aws_session_token", nil)
		setIfMissing("workgroup", "primary")
		setIfMissing("catalog", "AwsDataCatalog") // standard default aws catalog
		setIfMissing("database", "")
		template = "athena://{aws_region}"
	default:
		if c.Type.IsUnknown() {
			g.Trace("no type detected for %s", c.Name)
		}
		return nil
	}

	keys := g.MatchesGroup(template, "{([a-zA-Z]+)}", 0)
	if err := checkData(keys...); err != nil {
		return g.Error(err, "required keys not provided")
	}

	// set URL is missing
	urlData := g.M()
	for k, v := range c.Data {
		urlData[k] = v
	}
	urlData["password"] = url.QueryEscape(cast.ToString(urlData["password"]))
	setIfMissing("url", g.Rm(template, urlData))

	return nil
}

// CloseAll closes all cached connections
func CloseAll() {
	for _, conn := range connCache.Items() {
		conn.Close()
	}
}

// CopyDirect copies directly from cloud files
// (without passing through dbio)
func CopyDirect(conn database.Connection, tableFName string, srcFile Connection) (cnt uint64, ok bool, err error) {
	if !srcFile.Info().Type.IsFile() {
		return 0, false, nil
	}

	fs, err := srcFile.AsFile()
	if err != nil {
		err = g.Error(err, "Could not obtain client for: "+srcFile.URL())
		return
	}

	switch fs.FsType() {
	case dbio.TypeFileS3:
		ok = true
		err = database.CopyFromS3(conn, tableFName, srcFile.URL())
		if err != nil {
			err = g.Error(err, "could not load into database from S3")
		}
	case dbio.TypeFileAzure, dbio.TypeFileAzureABFS:
		ok = true
		err = database.CopyFromAzure(conn, tableFName, srcFile.URL())
		if err != nil {
			err = g.Error(err, "could not load into database from Azure")
		}
	case dbio.TypeFileGoogle:
	}

	if err != nil {
		// ok = false // try through dbio?
	}
	return
}

func ReadDbtConnections() (conns map[string]Connection, err error) {
	conns = map[string]Connection{}
	envVarRegex := `{{ *env_var\(['"]+([0-9a-zA-Z_-]+)['"]+\) *}}`

	profileDir := strings.TrimSuffix(os.Getenv("DBT_PROFILES_DIR"), "/")
	if profileDir == "" {
		profileDir = path.Join(g.UserHomeDir(), ".dbt")
	}
	path := path.Join(profileDir, "profiles.yml")
	if !g.PathExists(path) {
		return
	}

	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "error reading from yaml: %s", path)
		return
	}

	bytes, err := io.ReadAll(file)
	if err != nil {
		err = g.Error(err, "error reading bytes from yaml: %s", path)
		return
	}

	type ProfileConn struct {
		Target  string                            `json:"target" yaml:"target"`
		Outputs map[string]map[string]interface{} `json:"outputs" yaml:"outputs"`
	}

	dbtProfile := map[string]ProfileConn{}
	err = yaml.Unmarshal(bytes, &dbtProfile)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		return
	}

	for pName, pc := range dbtProfile {
		for target, data := range pc.Outputs {
			connName := strings.ToUpper(pName + "/" + target)
			data["dbt"] = true

			// expand env_var
			// https://docs.getdbt.com/reference/dbt-jinja-functions/env_var
			for key, val := range data {
				if valS, ok := val.(string); ok {
					for _, match := range g.Matches(valS, envVarRegex) {
						matchVal := match.Full
						envVarKey := match.Group[0]
						envVarVal, ok := os.LookupEnv(envVarKey)
						if ok {
							valS = strings.ReplaceAll(valS, matchVal, envVarVal)
						} else {
							g.Warn("Unable to expand env_var '%s' for dbt profile %s / %s, key %s", envVarKey, pName, target, key)
						}
					}
					data[key] = valS
				}
			}

			conn, err := NewConnectionFromMap(
				g.M("name", connName, "data", data, "type", data["type"]),
			)
			if err != nil {
				g.Trace("could not load dbt connection %s, connName: %s", data["type"], connName, err.Error())
				continue
			}

			conns[connName] = conn
			// g.Trace("found connection from dbt profiles YAML: " + connName)
		}
	}

	return
}

// ReadConnections loads the connections
func ReadConnectionsEnv(env map[string]interface{}) (conns map[string]Connection, err error) {

	conns = map[string]Connection{}
	// look for _TYPE suffix
	for k, val := range env {
		switch v := val.(type) {

		case map[string]interface{}:
			if ct, ok := v["type"]; ok {
				if connType, ok := dbio.ValidateType(cast.ToString(ct)); ok {
					connName := k
					data := v
					conn, err := NewConnectionFromMap(
						g.M("name", connName, "data", data, "type", connType.String()),
					)
					if err != nil {
						err = g.Error(err, "error loading connection %s", connName)
						return conns, err
					}
					conns[connName] = conn
				}
			} else if u, ok := v["url"]; ok {
				U, err := net.NewURL(cast.ToString(u))
				if err != nil {
					g.Warn("could not parse url value of %s", k)
					continue
				}

				if connType := SchemeType(U.String()); !connType.IsUnknown() {
					connName := k
					data := v
					conn, err := NewConnectionFromMap(
						g.M("name", connName, "data", data, "type", connType.String()),
					)
					if err != nil {
						err = g.Error(err, "error loading connection %s", connName)
						return conns, err
					}
					conns[connName] = conn
				}
			}

		case string:
			if !strings.Contains(v, ":/") || strings.Contains(v, "{") {
				continue // not a url
			}

			U, err := net.NewURL(v)
			if err != nil {
				g.Warn("could not parse url value of %s", k)
				continue
			}

			if connType := SchemeType(U.String()); !connType.IsUnknown() {
				connName := k
				data := v
				conn, err := NewConnectionFromMap(
					g.M("name", connName, "data", data, "type", connType.String()),
				)
				if err != nil {
					err = g.Error(err, "error loading connection %s", connName)
					return conns, err
				}
				conns[connName] = conn
			}
		}
	}
	return
}

func ReadConnectionsFromFile(path string) (conns map[string]Connection, err error) {
	conns = map[string]Connection{}

	if !g.PathExists(path) {
		return
	}

	env := map[string]interface{}{}
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "error reading from yaml")
		return
	}

	bytes, err := io.ReadAll(file)
	if err != nil {
		err = g.Error(err, "error reading bytes from yaml")
		return
	}

	err = yaml.Unmarshal(bytes, env)
	if err != nil {
		err = g.Error(err, "error parsing yaml string")
		return
	}

	return ReadConnections(env)
}

func ReadConnections(env map[string]interface{}) (conns map[string]Connection, err error) {
	conns = map[string]Connection{}

	if connections, ok := env["connections"]; ok {
		switch connectionsV := connections.(type) {
		case map[string]interface{}, map[interface{}]interface{}:
			connMap := cast.ToStringMap(connectionsV)
			for name, v := range connMap {
				switch v.(type) {
				case map[string]interface{}, map[interface{}]interface{}:
					data := cast.ToStringMap(v)
					if n := cast.ToString(data["name"]); n != "" {
						data["name"] = name
					}

					conn, err := NewConnectionFromMap(g.M("name", name, "data", data, "type", data["type"]))
					if err != nil {
						g.Warn("could not load connection %s: %s", name, g.ErrMsgSimple(err))
						continue
					}
					if err = conn.setURL(); err != nil {
						g.Warn("could not load connection %s: %s", name, g.ErrMsgSimple(err))
						continue
					}

					conns[name] = conn
				default:
					g.Warn("did not handle %s", name)
				}
			}
		default:
			g.Warn("did not handle connections profile type %T", connections)
		}
	}
	return
}

func (i *Info) IsURL() bool {
	return strings.Contains(i.Name, "://")
}

// SchemeType returns the correct scheme of the url
func SchemeType(url string) dbio.Type {
	if t, _, _, err := filesys.ParseURLType(url); err == nil {
		return t
	}

	scheme := strings.Split(url, "://")[0]
	t, _ := dbio.ValidateType(scheme)
	return t
}

func ParseLocation(location string) (conn Connection, objectExpr string, err error) {
	stateConnParts := strings.Split(location, "/")
	if len(stateConnParts) == 1 {
		err = g.Error("invalid location value: '%s'. See docs for help", location)
		return
	}

	connName := stateConnParts[0]
	if strings.TrimSpace(connName) == "" {
		err = g.Error("invalid location value: '%s'. See docs for help", location)
		return
	}

	objectExpr = strings.TrimPrefix(location, connName+"/")

	entry := GetLocalConns().Get(connName)
	if entry.Name == "" {
		err = g.Error("did not find connection: %s", connName)
		return
	}

	return entry.Connection, objectExpr, nil
}

// LoadAPISpec loads the spec from the spec location
func LoadAPISpec(specIdentifier string) (spec api.Spec, err error) {

	if specIdentifier == "" {
		return spec, g.Error("invalid or missing spec")
	}

	// maps a github/gitlab url to raw
	// https://github.com/slingdata-io/sling-cli/blob/main/examples/example.go
	// https://raw.githubusercontent.com/slingdata-io/sling-cli/refs/heads/main/examples/example.go
	transformToRawGitURL := func(gitURL string) (string, error) {
		// Parse the URL
		parsedURL, err := url.Parse(gitURL)
		if err != nil {
			return "", err
		}

		// Check if it's a GitHub Gist
		if parsedURL.Host == "gist.github.com" {
			// Gist URL format: https://gist.github.com/username/gistid
			pathParts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
			if len(pathParts) < 2 {
				return "", errors.New("invalid GitHub Gist URL format")
			}

			// Extract username and gistID
			username := pathParts[0]
			gistID := pathParts[1]

			// Filename would be the third component if specified
			filename := ""
			if len(pathParts) > 2 {
				filename = pathParts[2]
			}

			// If no specific file is requested, we'll get all raw files
			if filename == "" {
				// Format: https://gist.githubusercontent.com/username/gistid/raw
				return fmt.Sprintf("https://gist.githubusercontent.com/%s/%s/raw", username, gistID), nil
			}

			// Format: https://gist.githubusercontent.com/username/gistid/raw/filename
			return fmt.Sprintf("https://gist.githubusercontent.com/%s/%s/raw/%s", username, gistID, filename), nil
		}

		// Determine the host and set raw domain
		var rawHost string
		isGitHub := false
		switch parsedURL.Host {
		case "github.com":
			rawHost = "raw.githubusercontent.com"
			isGitHub = true
		case "gitlab.com":
			rawHost = "gitlab.com"
		default:
			// return original
			return gitURL, nil
		}

		// Split the path into components
		pathParts := strings.Split(strings.Trim(parsedURL.Path, "/"), "/")
		if len(pathParts) < 4 || pathParts[2] != "blob" {
			return "", errors.New("invalid Git repository file URL format")
		}

		// Extract components
		owner := pathParts[0]
		repo := pathParts[1]
		branch := pathParts[3]
		filePath := strings.Join(pathParts[4:], "/")

		// Construct the raw URL based on platform
		var rawURL *url.URL
		if isGitHub {
			// GitHub format
			rawURL = &url.URL{
				Scheme: "https",
				Host:   rawHost,
				Path:   "/" + owner + "/" + repo + "/refs/heads/" + branch + "/" + filePath,
			}
		} else {
			// GitLab format
			rawURL = &url.URL{
				Scheme: "https",
				Host:   rawHost,
				Path:   "/" + owner + "/" + repo + "/-/raw/" + branch + "/" + filePath,
			}
		}

		return rawURL.String(), nil
	}

	var specBody string
	switch {
	// load from location
	case strings.HasPrefix(specIdentifier, "file://"):
		specPath := strings.TrimPrefix(specIdentifier, "file://")
		bytes, err := os.ReadFile(specPath)
		if err != nil {
			return spec, g.Error(err, "could not read api spec from: %s", specPath)
		}
		specBody = string(bytes)
	case strings.HasPrefix(specIdentifier, "https://"), strings.HasPrefix(specIdentifier, "http://"):
		// download raw http
		specURL, err := transformToRawGitURL(specIdentifier)
		if err != nil {
			return spec, g.Error(err, "could not make git spec URL for download: %s", specIdentifier)
		}
	redirect:
		resp, respBytes, err := net.ClientDo("GET", specURL, nil, nil)
		if err != nil {
			return spec, g.Error(err, "could not download spec from: %s", specURL)
		} else if resp != nil && resp.StatusCode >= 300 && resp.StatusCode <= 399 {
			redirectUrl, _ := resp.Location()
			if redirectUrl != nil {
				specURL = redirectUrl.String()
				goto redirect
			}
		}
		specBody = string(respBytes)
	case strings.Contains(specIdentifier, "/"):
		// connect to location and download
		specConn, specPath, err := ParseLocation(specIdentifier)
		if err != nil {
			return spec, g.Error(err, "could not read api spec from location: %s", specIdentifier)
		}
		fc, err := specConn.AsFile()
		if err != nil {
			return spec, g.Error(err, "could not connection to location: %s", specIdentifier)
		}

		reader, err := fc.GetReader(specPath)
		if err != nil {
			return spec, g.Error(err, "could not read from file: %s", specPath)
		}

		bytes, err := io.ReadAll(reader)
		if err != nil {
			return spec, g.Error(err, "could not read api spec from: %s", specPath)
		}
		specBody = string(bytes)
	default:
		specBody, err = api.FetchSpec(specIdentifier)
		if err != nil {
			return spec, g.Error(err, "could not fetch api spec: %s", specIdentifier)
		}
	}

	// load spec
	spec, err = api.LoadSpec(specBody)
	if err != nil {
		return spec, g.Error(err, "could not load spec from %s", specIdentifier)
	}

	return
}
