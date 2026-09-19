package database

import (
	"context"
	"database/sql"
	"iter"
	"maps"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	_ "github.com/apache/iceberg-go/io/gocloud" // S3 / GCS / Azure blob IO for iceberg-go v0.6+
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/utils"
	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	awsv2config "github.com/aws/aws-sdk-go-v2/config"
	awsv2creds "github.com/aws/aws-sdk-go-v2/credentials"
	awsv2glue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// IcebergConn is an Iceberg connection
type IcebergConn struct {
	BaseConn
	URL            string
	CatalogType    dbio.IcebergCatalogType
	CatalogSQLConn Connection
	Catalog        catalog.Catalog
	Warehouse      string
	duck           *iop.DuckDb
}

// Init initiates the object
func (conn *IcebergConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbIceberg

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	conn.CatalogType = dbio.IcebergCatalogType(conn.GetProp("catalog_type"))

	return conn.BaseConn.Init()
}

// Connect connects to the Iceberg catalog
func (conn *IcebergConn) Connect(timeOut ...int) (err error) {
	if cast.ToBool(conn.GetProp("connected")) {
		return nil
	}

	switch conn.CatalogType {
	case dbio.IcebergCatalogTypeREST:
		err = conn.connectREST()
	case dbio.IcebergCatalogTypeGlue:
		err = conn.connectGlue()
	case dbio.IcebergCatalogTypeSQL:
		err = conn.connectSQL()
	default:
		return g.Error("Unsupported catalog type: %s. Supported types are: rest, glue, sql", conn.CatalogType)
	}

	if err != nil {
		return g.Error(err, "Failed to connect to Iceberg catalog")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	return nil
}

func (conn *IcebergConn) connectREST() error {

	catalogName := conn.GetProp("catalog_name")
	if catalogName == "" {
		catalogName = "iceberg"
	}

	opts := []rest.Option{}

	// Add warehouse location if provided
	conn.Warehouse = conn.GetProp("rest_warehouse")
	if conn.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(conn.Warehouse))

		// check fo s3table
		if conn.isS3TablesViaREST() {
			// extract region
			parts := strings.Split(conn.Warehouse, ":")
			if len(parts) != 6 {
				return g.Error("invalid s3tables ARN: %s", conn.Warehouse)
			}

			// configure for s3tables via REST
			region := parts[3]
			if r := conn.GetProp("s3_region"); r != "" && r != region {
				return g.Error("ARN region does not match `s3_region` property: %s != %s", r, region)
			}
			opts = append(opts, rest.WithSigV4RegionSvc(region, "s3tables"))

			conn.SetProp("s3_region", region)
			conn.SetProp("rest_uri", g.F("https://s3tables.%s.amazonaws.com/iceberg", region))
		}
	}

	restURI := conn.GetProp("rest_uri")
	if restURI == "" {
		return g.Error("rest_uri property is required for REST catalog")
	}

	if cast.ToBool(conn.GetProp("rest_sigv4_enable")) {
		opts = append(opts, rest.WithSigV4())
		region := conn.GetProp("rest_sigv4_region")
		service := conn.GetProp("rest_sigv4_service")
		if region != "" {
			opts = append(opts, rest.WithSigV4RegionSvc(region, service))
		}
	}

	if tlsConfig, err := conn.makeTlsConfig(); tlsConfig != nil && err == nil {
		opts = append(opts, rest.WithTLSConfig(tlsConfig))
	} else if err != nil {
		return g.Error(err, "invalid TLS config")
	}

	if metaLoc := conn.GetProp("rest_metadata_location"); metaLoc != "" {
		opts = append(opts, rest.WithMetadataLocation(metaLoc))
	}

	if prefix := conn.GetProp("rest_prefix"); prefix != "" {
		opts = append(opts, rest.WithPrefix(prefix))
	}

	props := map[string]string{}
	if extra := conn.GetProp("rest_extra_props"); extra != "" {
		if err := g.Unmarshal(extra, &props); err != nil {
			return g.Error(err, "could not unmarshal rest_extra_props")
		}
	}

	maps.Copy(props, conn.applyIcebergS3Props())

	if len(props) > 0 {
		g.Debug("using additional props for iceberg REST: %s", g.Marshal(lo.Keys(props)))
		opts = append(opts, rest.WithAdditionalProps(props))
	}

	// Pass through S3 properties for REST access
	// awsProps := map[string]string{}
	// for key, value := range conn.properties {
	// 	switch key {
	// 	case "s3_access_key_id", "s3_secret_access_key", "s3_session_token", "s3_region", "s3_endpoint", "s3_profile":
	// 		newKey := strings.TrimPrefix(key, "s3_")
	// 		awsProps[newKey] = value
	// 	}
	// }

	// if len(awsProps) > 0 {
	// 	cfg, err := iop.MakeAwsConfig(conn.context.Ctx, awsProps)
	// 	if err != nil {
	// 		return g.Error(err, "failed to create AWS config for iceberg")
	// 	}
	// 	g.Trace("using S3 props for iceberg REST: %s", g.Marshal(lo.Keys(awsProps)))
	// 	opts = append(opts, rest.WithAwsConfig(cfg))
	// }

	// Add authentication if provided
	if token := conn.GetProp("rest_token"); token != "" {
		opts = append(opts, rest.WithOAuthToken(token))
	}

	if authURI := conn.GetProp("rest_oauth_server_uri"); authURI != "" {
		au, err := net.NewURL(authURI)
		if err != nil {
			return g.Error(err, "invalid rest_auth_uri")
		}
		opts = append(opts, rest.WithAuthURI(au.U))
	}

	if scope := conn.GetProp("rest_oauth_scope"); scope != "" {
		opts = append(opts, rest.WithScope(scope))
	}

	// Add rest_credential if provided
	if clientID := conn.GetProp("rest_oauth_client_id"); clientID != "" {
		clientSecret := conn.GetProp("rest_oauth_client_secret")
		creds := clientID + ":" + clientSecret
		opts = append(opts, rest.WithCredential(creds))
	}

	cat, err := rest.NewCatalog(
		context.Background(),
		catalogName,
		restURI,
		opts...,
	)
	if err != nil {
		if strings.TrimSpace(err.Error()) == ":" {
			return g.Error("Failed to create REST catalog, check URI and credentials")
		}
		return g.Error(err, "Failed to create REST catalog")
	}

	conn.Catalog = cat
	if err := conn.attachIcebergAwsConfig(); err != nil {
		return err
	}
	return nil
}

func (conn *IcebergConn) isS3TablesViaREST() bool {
	return strings.HasPrefix(conn.Warehouse, "arn:aws:s3tables")
}

// applyIcebergS3Props maps s3_* connection keys to iceberg-go properties and
// AWS env vars (gocloud IO still reads AWS_REGION / AWS_S3_ENDPOINT).
func (conn *IcebergConn) applyIcebergS3Props() iceberg.Properties {
	props := iceberg.Properties{}
	if v := conn.GetProp("s3_access_key_id"); v != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", v)
		props["s3.access-key-id"] = v
	}
	if v := conn.GetProp("s3_secret_access_key"); v != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", v)
		props["s3.secret-access-key"] = v
	}
	if v := conn.GetProp("s3_session_token"); v != "" {
		os.Setenv("AWS_SESSION_TOKEN", v)
		props["s3.session-token"] = v
	}
	if v := conn.GetProp("s3_region"); v != "" {
		os.Setenv("AWS_REGION", v)
		os.Setenv("AWS_DEFAULT_REGION", v)
		props["s3.region"] = v
	}
	if v := conn.GetProp("s3_endpoint"); v != "" {
		os.Setenv("AWS_ENDPOINT", v)
		os.Setenv("AWS_S3_ENDPOINT", v) // iceberg-go gocloud IO fallback
		props["s3.endpoint"] = v
	}
	if v := conn.GetProp("s3_profile"); v != "" {
		os.Setenv("AWS_PROFILE", v)
		props["s3.profile"] = v
	}
	return props
}

func (conn *IcebergConn) icebergAwsConfig() (awsv2.Config, error) {
	region := conn.GetProp("s3_region")
	accessKey := conn.GetProp("s3_access_key_id")
	secret := conn.GetProp("s3_secret_access_key")
	token := conn.GetProp("s3_session_token")
	profile := conn.GetProp("s3_profile")

	opts := []func(*awsv2config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, awsv2config.WithRegion(region))
	}
	switch {
	case accessKey != "" && secret != "":
		g.Debug("iceberg: using static credentials (Key ID: %s)", accessKey)
		opts = append(opts, awsv2config.WithCredentialsProvider(
			awsv2creds.NewStaticCredentialsProvider(accessKey, secret, token),
		))
	case profile != "":
		g.Debug("iceberg: using AWS profile=%s region=%s", profile, region)
		opts = append(opts, awsv2config.WithSharedConfigProfile(profile))
	default:
		g.Debug("iceberg: using default AWS credential chain")
	}

	awsCfg, err := awsv2config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return awsv2.Config{}, g.Error(err, "Failed to create AWS config for iceberg")
	}
	return awsCfg, nil
}

func (conn *IcebergConn) attachIcebergAwsConfig() error {
	if conn.GetProp("s3_region") == "" && conn.GetProp("s3_access_key_id") == "" && conn.GetProp("s3_profile") == "" {
		return nil
	}
	awsCfg, err := conn.icebergAwsConfig()
	if err != nil {
		return err
	}
	conn.context.Ctx = utils.WithAwsConfig(conn.context.Ctx, &awsCfg)
	return nil
}

func icebergNonIcebergCatalogErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not an EXTERNAL_TABLE") || strings.Contains(msg, "not an iceberg table")
}

func (conn *IcebergConn) dropGlueEntry(identifier table.Identifier) error {
	if len(identifier) < 2 {
		return g.Error("invalid Glue table identifier: %v", identifier)
	}
	awsCfg, err := conn.icebergAwsConfig()
	if err != nil {
		return err
	}
	in := &awsv2glue.DeleteTableInput{
		DatabaseName: awsv2.String(identifier[len(identifier)-2]),
		Name:         awsv2.String(identifier[len(identifier)-1]),
	}
	if accountID := conn.GetProp("glue_account_id"); accountID != "" {
		in.CatalogId = awsv2.String(accountID)
	}
	_, err = awsv2glue.NewFromConfig(awsCfg).DeleteTable(conn.Context().Ctx, in)
	if err != nil {
		return g.Error(err, "cannot drop Glue table %s.%s", awsv2.ToString(in.DatabaseName), awsv2.ToString(in.Name))
	}
	return nil
}

func (conn *IcebergConn) connectGlue() error {
	warehouse := conn.GetProp("glue_warehouse")
	awsRegion := conn.GetProp("s3_region")

	if awsRegion == "" {
		return g.Error("AWS region not specified")
	}

	props := map[string]string{"warehouse": warehouse}
	maps.Copy(props, conn.applyIcebergS3Props())

	awsCfg, err := conn.icebergAwsConfig()
	if err != nil {
		return err
	}

	if extra := conn.GetProp("glue_extra_props"); extra != "" {
		extraProps := map[string]string{}
		if err := g.Unmarshal(extra, &extraProps); err != nil {
			return g.Error(err, "could not unmarshal glue_extra_props")
		}
		for k, v := range extraProps {
			props[k] = v
		}
	}

	// Create Glue catalog with AWS config
	opts := []glue.Option{glue.WithAwsConfig(awsCfg), glue.WithAwsProperties(props)}
	cat := glue.NewCatalog(opts...)
	conn.Catalog = cat

	// Set AWS config in connection context for table operations
	conn.context.Ctx = utils.WithAwsConfig(conn.context.Ctx, &awsCfg)

	return nil
}

func (conn *IcebergConn) connectSQL() error {
	// Get SQL catalog configuration
	catalogName := conn.GetProp("sql_catalog_name")
	if catalogName == "" {
		catalogName = "sql"
	}

	connName := conn.GetProp("sql_catalog_conn")
	connPayload := conn.GetProp("sql_conn_payload")
	if connName == "" {
		return g.Error("must provide sql_catalog_conn")
	} else if connPayload == "" {
		return g.Error("did not find provided sql_catalog_conn: %s", connName)
	}

	connMap, err := g.UnmarshalMap(connPayload)
	if err != nil {
		return g.Error(err, "could not unmarshal payload for Iceberg SQL Connection")
	}

	connData := map[string]string{}
	if err = g.JSONConvert(connMap["data"], &connData); err != nil {
		return g.Error(err, "could not convert payload for Iceberg SQL Connection")
	}

	conn.CatalogSQLConn, err = NewConnContext(
		conn.Context().Ctx, cast.ToString(connMap["url"]),
		g.MapToKVArr(connData)...)
	if err != nil {
		return g.Error(err, "could not make object for Iceberg SQL Connection")
	}

	if err = conn.CatalogSQLConn.Connect(); err != nil {
		return g.Error(err, "could not connect to Iceberg SQL Connection")
	}

	// Validate dialect
	var dialect sqlcat.SupportedDialect
	switch conn.CatalogSQLConn.GetType() {
	case dbio.TypeDbPostgres:
		dialect = sqlcat.Postgres
	case dbio.TypeDbMySQL:
		dialect = sqlcat.MySQL
	case dbio.TypeDbSQLite:
		dialect = sqlcat.SQLite
	case dbio.TypeDbSQLServer:
		dialect = sqlcat.MSSQL
	case dbio.TypeDbOracle:
		dialect = sqlcat.Oracle
	default:
		return g.Error("unsupported sql connection type '%s'", dialect)
	}

	// Prepare properties for SQL catalog
	props := iceberg.Properties{
		sqlcat.DialectKey: string(dialect),
		sqlcat.DriverKey:  getDriverName(conn.CatalogSQLConn),
		"uri":             conn.CatalogSQLConn.GetURL(),
	}

	// Add optional properties
	if cast.ToBool(conn.GetProp("sql_catalog_init")) {
		props["init_catalog_tables"] = "true"
	}

	// Add warehouse location if provided
	if warehouse := conn.GetProp("sql_warehouse"); warehouse != "" {
		props["warehouse"] = warehouse
		conn.Warehouse = warehouse
	}

	maps.Copy(props, conn.applyIcebergS3Props())

	// Add any extra properties specified by the user
	if extra := conn.GetProp("sql_extra_props"); extra != "" {
		extraProps := map[string]string{}
		if err := g.Unmarshal(extra, &extraProps); err != nil {
			return g.Error(err, "could not unmarshal sql_extra_props")
		}
		maps.Copy(props, extraProps)
	}

	// Create SQL catalog
	cat, err := sqlcat.NewCatalog(catalogName, conn.CatalogSQLConn.Db().DB, dialect, props)
	if err != nil {
		conn.CatalogSQLConn.Close()
		return g.Error(err, "Failed to create SQL catalog")
	}

	// iceberg-go v0.6 added iceberg_tables.iceberg_type. Existing catalogs
	// created by older clients are missing it; CREATE TABLE IF NOT EXISTS
	// will not add the column.
	if err = conn.migrateSQLCatalogIcebergType(); err != nil {
		conn.CatalogSQLConn.Close()
		return g.Error(err, "Failed to migrate Iceberg SQL catalog schema")
	}

	conn.Catalog = cat
	if err = conn.attachIcebergAwsConfig(); err != nil {
		conn.CatalogSQLConn.Close()
		return err
	}

	return nil
}

func (conn *IcebergConn) migrateSQLCatalogIcebergType() error {
	if conn.CatalogSQLConn == nil {
		return nil
	}

	q := `ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR DEFAULT 'TABLE'`
	switch conn.CatalogSQLConn.GetType() {
	case dbio.TypeDbPostgres:
		q = `ALTER TABLE iceberg_tables ADD COLUMN IF NOT EXISTS iceberg_type VARCHAR NOT NULL DEFAULT 'TABLE'`
	}

	_, err := conn.CatalogSQLConn.Exec(q)
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "duplicate") ||
		strings.Contains(msg, "already exists") ||
		strings.Contains(msg, "does not exist") ||
		strings.Contains(msg, "no such table") ||
		strings.Contains(msg, "unknown table") {
		return nil
	}
	return err
}

// Close closes the connection
func (conn *IcebergConn) Close() error {
	if conn.duck != nil {
		conn.duck.Close()
	}
	if conn.CatalogSQLConn != nil {
		conn.CatalogSQLConn.Close()
	}

	if conn.Catalog == nil {
		return nil
	}

	// Iceberg catalog doesn't have a close method
	conn.Catalog = nil
	return conn.BaseConn.Close()
}

// NewTransaction creates a new transaction
func (conn *IcebergConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// Iceberg operations are transactional by nature when using table transactions
	// But we don't have traditional SQL transactions
	return nil, nil
}

// GetSchemas returns the list of namespaces (schemas)
func (conn *IcebergConn) GetSchemas() (data iop.Dataset, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return data, g.Error(err, "Could not reconnect")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name"))

	namespaces, err := conn.Catalog.ListNamespaces(conn.Context().Ctx, nil)
	if err != nil {
		return data, g.Error(err, "Failed to list namespaces")
	}

	for _, ns := range namespaces {
		// Convert namespace to schema name (join with dot)
		schemaName := strings.Join(ns, ".")
		data.Rows = append(data.Rows, []any{schemaName})
	}

	return data, nil
}

// GetDatabases returns databases (catalogs) for Iceberg connection
func (conn *IcebergConn) GetDatabases() (data iop.Dataset, err error) {
	// In Iceberg, the catalog is the database equivalent
	// Return the current catalog name
	data = iop.NewDataset(iop.NewColumnsFromFields("name"))
	catalogName := conn.GetProp("catalog_name")
	if catalogName == "" {
		catalogName = "iceberg"
	}
	data.Rows = append(data.Rows, []any{catalogName})
	return data, nil
}

// GetTables returns tables for given schema
func (conn *IcebergConn) GetTables(schema string) (data iop.Dataset, err error) {
	return conn.getTablesOrViews(schema)
}

// GetViews returns views for given schema
func (conn *IcebergConn) GetViews(schema string) (data iop.Dataset, err error) {
	// Iceberg doesn't have views in the traditional sense
	data = iop.NewDataset(iop.NewColumnsFromFields("table_name", "is_view"))
	return data, nil
}

// GetTablesAndViews returns tables and views for given schema
func (conn *IcebergConn) GetTablesAndViews(schema string) (data iop.Dataset, err error) {
	return conn.getTablesOrViews(schema)
}

func (conn *IcebergConn) getTablesOrViews(schema string) (data iop.Dataset, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return data, g.Error(err, "Could not reconnect")
	}

	data = iop.NewDataset(iop.NewColumnsFromFields("schema_name", "table_name", "is_view"))

	// Parse namespace from schema string
	var namespace table.Identifier
	if schema == "" {
		namespace = nil // List all tables
	} else {
		namespace = strings.Split(schema, ".")
	}

	// List tables in namespace
	iter := conn.Catalog.ListTables(conn.Context().Ctx, namespace)
	for tblID, err := range iter {
		if err != nil {
			return data, g.Error(err, "Failed to list tables")
		}
		// Get table name (last part of identifier)
		schemaName := tblID[len(tblID)-2]
		tableName := tblID[len(tblID)-1]
		data.Rows = append(data.Rows, []any{schemaName, tableName, false})
	}

	return data, nil
}

// GetColumns returns the columns for a given table
func (conn *IcebergConn) GetColumns(tableFName string, fields ...string) (columns iop.Columns, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return columns, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return nil, g.Error(err, "could parse %s", tableFName)
	}
	tableID := table.Identifier{t.Schema, t.Name}

	// Load table
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID)
	if err != nil {
		if strings.Contains(err.Error(), "Table action can_get_metadata forbidden") {
			return columns, g.Error("%s, check table name", err.Error())
		}
		return columns, g.Error(err, "Failed to load table %s", tableFName)
	}

	// Get table schema
	schema := tbl.Schema()

	// Convert Iceberg schema to iop.Columns
	columns = make(iop.Columns, 0, len(schema.Fields()))

	for i, field := range schema.Fields() {
		col := iop.Column{
			Name:     field.Name,
			Position: i + 1,
			Type:     iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
			DbType:   field.Type.String(),
			Sourced:  true,
		}

		// Check if field is required (not nullable)
		if field.Required {
			col.Constraint = &iop.ColumnConstraint{
				Expression: "not null",
			}
		}

		columns = append(columns, col)
	}

	return columns, nil
}

func (conn *IcebergConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	if table.IsQuery() {
		return nil, g.Error("cannot get columns of a custom query in iceberg")
	}

	return conn.GetColumns(table.FullName(), fields...)
}

func (conn *IcebergConn) GetDataFiles(t Table) (dataFiles []iceberg.DataFile, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID)
	if err != nil {
		return nil, g.Error(err, "could not load existing table: %s", t.FullName())
	}

	fs, err := tbl.FS(conn.Context().Ctx)
	if err != nil {
		return nil, g.Error(err, "could not load table FS => %s", t.FullName())
	}

	currSnapshot := tbl.CurrentSnapshot()
	if currSnapshot == nil {
		return nil, g.Error("no current snapshot found")
	}

	// Check each data file's statistics
	manifests, err := currSnapshot.Manifests(fs)
	if err != nil {
		return nil, g.Error(err, "failed to get manifests from snapshot %d", currSnapshot.SnapshotID)
	}

	for _, manifest := range manifests {
		// Only process data manifests (not delete manifests)
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}

		// Fetch manifest entries
		entries, err := manifest.FetchEntries(fs, true)
		if err != nil {
			return nil, err
		}

		for _, entry := range entries {
			dataFile := entry.DataFile()
			dataFiles = append(dataFiles, dataFile)
		}
	}

	return dataFiles, nil
}

// GetMaxValue gets the maximum value of the given column
func (conn *IcebergConn) GetMaxValue(t Table, colName string) (value any, maxCol iop.Column, err error) {

	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, maxCol, g.Error(err, "Could not reconnect")
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID)
	if err != nil {
		return 0, maxCol, g.Error(err, "could not load existing table: %s", t.FullName())
	}

	field, ok := tbl.Schema().FindFieldByNameCaseInsensitive(colName)
	if !ok {
		return nil, maxCol, g.Error("could not find column %s in table %s", colName, t.FullName())
	}

	dataFiles, err := conn.GetDataFiles(t)
	if err != nil {
		return nil, maxCol, g.Error(err, "could not get data files")
	}

	// Check each data file's statistics
	var globalMax iceberg.Literal

	for _, dataFile := range dataFiles {

		// Get upper bound values (max values) for each column
		upperBounds := dataFile.UpperBoundValues()

		// Check if we have statistics for our column
		for colId, maxBytes := range upperBounds {
			if colId != field.ID {
				continue
			}

			// Convert bytes to literal based on the field type
			maxLiteral, err := iceberg.LiteralFromBytes(field.Type, maxBytes)
			if err != nil {
				return nil, maxCol, err
			}

			// Update global max
			if globalMax == nil {
				globalMax = maxLiteral
			} else if maxLiteral.String() > globalMax.String() {
				globalMax = maxLiteral
			}
		}
	}

	// convert globalMax.String() to value according to field.Type
	maxCol = iop.Column{
		Name:    field.Name,
		Type:    iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
		DbType:  field.Type.String(),
		Sourced: true,
	}

	// cast value
	if globalMax != nil {
		value = iop.NewStreamProcessor().CastVal(0, globalMax.String(), &maxCol)
	}

	return
}

// StreamRowsContext streams the rows of a table or query
func (conn *IcebergConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return nil, g.Error(err, "Could not reconnect")
	}

	opts := getQueryOptions(options)
	fetchedColumns := iop.Columns{}
	if val, ok := opts["columns"].(iop.Columns); ok {
		fetchedColumns = val
	}

	tableSchema := cast.ToString(opts["table_schema"])
	tableName := cast.ToString(opts["table_name"])
	incrementalKey := cast.ToString(opts["incremental_key"])
	incrementalValue := cast.ToString(opts["incremental_value"])
	limit := cast.ToUint64(opts["limit"])

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		return ds, g.Error("Empty Query")
	}

	if tableName == "" || !g.In(conn.CatalogType, dbio.IcebergCatalogTypeREST, dbio.IcebergCatalogTypeGlue, dbio.IcebergCatalogTypeSQL) {
		// if custom SQL, or glue or s3 tables, use DuckDB
		return conn.queryViaDuckDB(ctx, sql, opts)
	}

	queryContext := g.NewContext(ctx)
	conn.LogSQL(sql)

	// Parse table identifier
	tableID := table.Identifier{tableSchema, tableName}

	// Load table
	tbl, err := conn.Catalog.LoadTable(queryContext.Ctx, tableID)
	if err != nil {
		return nil, g.Error(err, "Failed to load table %s.%s", tableSchema, tableName)
	}

	// Get table schema and convert to columns if not provided
	if len(fetchedColumns) == 0 {
		schema := tbl.Schema()
		fetchedColumns = make(iop.Columns, 0, len(schema.Fields()))

		for i, field := range schema.Fields() {
			col := iop.Column{
				Name:     field.Name,
				Position: i + 1,
				Type:     iop.NativeTypeToGeneral(field.Name, field.Type.String(), dbio.TypeDbIceberg),
				DbType:   field.Type.String(),
				Sourced:  true,
				Table:    tableName,
			}
			fetchedColumns = append(fetchedColumns, col)
		}
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.NoDebug = !strings.Contains(sql, noDebugKey)

	// Create table scan
	scanOpts := []table.ScanOption{}
	if limit > 0 {
		scanOpts = append(scanOpts, table.WithLimit(int64(limit)))
	}

	if incrementalKey != "" && incrementalValue != "" {
		incrementalValue = strings.ReplaceAll(incrementalValue, "'", "") // clean
		// Find the field in the table schema to get proper field reference
		field, found := tbl.Schema().FindFieldByNameCaseInsensitive(incrementalKey)
		if !found {
			return nil, g.Error("incremental key field '%s' not found in table schema", incrementalKey)
		}

		// Create the greater than expression using field reference
		fieldRef := iceberg.Reference(field.Name)

		var greaterThanExpr iceberg.UnboundPredicate
		sp := iop.NewStreamProcessor()
		switch field.Type {
		case iceberg.PrimitiveTypes.String:
			greaterThanExpr = iceberg.GreaterThan(fieldRef, incrementalValue)
		case iceberg.PrimitiveTypes.Int32:
			if val, parseErr := cast.ToInt32E(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as integer: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Int64:
			if val, parseErr := cast.ToInt64E(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as long: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Float32:
			if val, parseErr := cast.ToFloat32E(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as float: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Float64:
			if val, parseErr := cast.ToFloat64E(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, val)
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as double: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.Date:
			if val, parseErr := sp.ParseTime(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, iceberg.Date(val.Unix()/86400))
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as date: %v", incrementalValue, parseErr)
			}
		case iceberg.PrimitiveTypes.TimestampTz:
			if val, parseErr := sp.ParseTime(incrementalValue); parseErr == nil {
				greaterThanExpr = iceberg.GreaterThan(fieldRef, iceberg.Timestamp(val.UnixMicro()))
			} else {
				return nil, g.Error("cannot parse incremental value '%s' as timestamp: %v", incrementalValue, parseErr)
			}
		default:
			greaterThanExpr = iceberg.GreaterThan(fieldRef, incrementalValue)
		}

		scanOpts = append(scanOpts, table.WithRowFilter(greaterThanExpr))
	}

	// Add selected fields if specified
	if fieldNames := opts["fields_array"]; fieldNames != nil {
		if fields, err := cast.ToStringSliceE(fieldNames); err == nil {
			scanOpts = append(scanOpts, table.WithSelectedFields(fields...))
		}
	}

	scan := tbl.Scan(scanOpts...)

	// Create a function to fetch next row
	makeNextFunc := func() (nextFunc func(it *iop.Iterator) bool) {
		// Get Arrow record iterator
		_, recordIterator, err := scan.ToArrowRecords(queryContext.Ctx)
		if err != nil {
			queryContext.CaptureErr(g.Error(err, "Failed to create arrow record iterator"))
			return func(it *iop.Iterator) bool { return false }
		}

		var currentRecord arrow.Record
		var currentRowIdx int
		var recordChan = make(chan arrow.Record, 1)
		var done = make(chan bool)

		// Stream records in a goroutine
		go func() {
			defer close(recordChan)
			defer close(done)
			for record, err := range recordIterator {
				if err != nil {
					queryContext.CaptureErr(g.Error(err, "Error reading arrow records"))
					return
				}
				recordChan <- record
			}
		}()

		return func(it *iop.Iterator) bool {
			if limit > 0 && it.Counter >= limit {
				return false
			}

			// Check if we need to fetch next record batch
			if currentRecord == nil || currentRowIdx >= int(currentRecord.NumRows()) {
				select {
				case record, ok := <-recordChan:
					if !ok {
						return false
					}
					currentRecord = record
					currentRowIdx = 0
				case <-done:
					return false
				}
			}

			// Convert current row to interface{} slice
			it.Row = make([]interface{}, currentRecord.NumCols())
			for colIdx := 0; colIdx < int(currentRecord.NumCols()); colIdx++ {
				col := currentRecord.Column(colIdx)
				it.Row[colIdx] = iop.GetValueFromArrowArray(col, currentRowIdx)
			}

			currentRowIdx++
			return true
		}
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, fetchedColumns, makeNextFunc())
	ds.NoDebug = strings.Contains(sql, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()

	if !ds.NoDebug {
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	return ds, nil
}

type icebergResult struct {
	TotalRows uint64
}

func (r icebergResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r icebergResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

// ExecContext executes a write operation
func (conn *IcebergConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {
	switch {
	case strings.HasPrefix(sql, "create schema "):
		schema := strings.Trim(strings.TrimPrefix(sql, "create schema "), `"`)
		return icebergResult{}, conn.CreateNamespaceIfNotExists(schema)
	case strings.HasPrefix(sql, "drop table "):
		name := strings.TrimSpace(strings.TrimPrefix(sql, "drop table "))
		if strings.HasPrefix(strings.ToLower(name), "if exists") {
			name = strings.TrimSpace(name[len("if exists"):])
		}
		return icebergResult{}, conn.DropTable(name)
	case strings.HasPrefix(sql, "drop view "):
		name := strings.TrimSpace(strings.TrimPrefix(sql, "drop view "))
		if strings.HasPrefix(strings.ToLower(name), "if exists") {
			name = strings.TrimSpace(name[len("if exists"):])
		}
		return icebergResult{}, conn.DropTable(name)
	case strings.Contains(sql, `"ddl_columns":`) && strings.Contains(sql, `"table":`):
		m, _ := g.UnmarshalMap(sql)
		var table Table
		var columns iop.Columns
		if err = g.JSONConvert(m["ddl_columns"], &columns); err != nil {
			return nil, g.Error(err, "could not convert ddl_columns")
		}
		if err = g.JSONConvert(m["table"], &table); err != nil {
			return nil, g.Error(err, "could not convert table for ")
		}

		if err = conn.CreateTable(table.FullName(), columns, ""); err != nil {
			return nil, g.Error(err, "could not create table")
		}

		return icebergResult{}, nil
	}

	// Iceberg doesn't support SQL execution directly
	// This would need to be implemented with table operations
	return nil, g.Error("Iceberg does not support direct SQL execution. Use bulk import/export operations instead:\n%s", sql)
}

func (conn *IcebergConn) CreateTable(tableName string, cols iop.Columns, tableDDL string) (err error) {

	t, err := ParseTableName(tableName, conn.Type)
	if err != nil {
		return g.Error(err, "could create parse %s", tableName)
	}
	tableID := table.Identifier{t.Schema, t.Name}
	g.Debug("CreateTable: tableName=%s, tableID=%v", tableName, tableID)

	// First check if namespace exists (if schema is specified)
	if t.Schema != "" {
		if err := conn.CreateNamespaceIfNotExists(t.Schema); err != nil {
			return g.Error(err, "could create namespace %s", t.Schema)
		}
	}

	// Create Iceberg schema from datastream columns
	icebergSchema, err := conn.generateIcebergSchema(cols)
	if err != nil {
		return g.Error(err, "Failed to create Iceberg schema")
	}

	// Create table options
	createOpts := []catalog.CreateTableOpt{}

	// Add table properties including format-version: 2
	props := iceberg.Properties{
		"format-version":               "2",
		"write.format.default":         "parquet",
		"write.delete.mode":            "merge-on-read",
		"write.delete.isolation-level": "snapshot",
		"created-by":                   "sling-cli",
	}

	// Add any additional properties from connection
	if conn.Warehouse != "" {
		props["location"] = conn.Warehouse + "/" + strings.Join(tableID, "/")
	}

	createOpts = append(createOpts, catalog.WithProperties(props))

	// Create the table
	conn.LogSQL(g.F("create table %s (%s)", tableName, g.Marshal(icebergSchema)))

	table, err := conn.Catalog.CreateTable(conn.Context().Ctx, tableID, icebergSchema, createOpts...)
	if err != nil {
		return g.Error(err, "Failed to create table %s", tableName)
	}
	if table == nil {
		return g.Error("CreateTable returned nil table for %s", tableName)
	}

	return nil
}

// GetCount returns -1 to skip validation
func (conn *IcebergConn) GetCount(tableFName string) (count int64, err error) {

	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	dataFiles, err := conn.GetDataFiles(t)
	if err != nil {
		return 0, g.Error(err, "could not get data files")
	}

	for _, dataFile := range dataFiles {
		count = count + dataFile.Count()
	}

	return count, nil
}

// GenerateDDL generates a DDL based on a dataset
func (conn *IcebergConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	// Iceberg doesn't use traditional SQL DDL
	// Table creation is done through the catalog API
	return g.Marshal(g.M("table", table, "ddl_columns", data.Columns)), nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *IcebergConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	// Since Iceberg doesn't support SQL directly, just return the column name
	return conn.Self().Quote(srcCol.Name)
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *IcebergConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream inserts data into a table
func (conn *IcebergConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

func (conn *IcebergConn) TableExists(t Table) (exists bool, err error) {

	identifier := table.Identifier{t.Schema, t.Name}
	exists, err = conn.Catalog.CheckTableExists(conn.context.Ctx, identifier)
	if err != nil {
		// Glue returns this for leftover non-Iceberg tables in the same namespace.
		if icebergNonIcebergCatalogErr(err) {
			return true, nil
		}
		return false, g.Error(err, "cannot check table existence: %s", t.FullName())
	}

	return
}

// DropTable drops given table.
func (conn *IcebergConn) DropTable(tableNames ...string) (err error) {

	for _, tableName := range tableNames {
		t, err := ParseTableName(tableName, conn.Type)
		if err != nil {
			return g.Error(err, "cannot parse table name: %s", tableName)
		}

		exists, err := conn.TableExists(t)
		if err != nil {
			return g.Error(err, "cannot check table existence: %s", tableName)
		}

		identifier := table.Identifier{t.Schema, t.Name}
		if exists {
			// purge table
			catalog, ok := conn.Catalog.(*rest.Catalog)
			if ok {
				if err = catalog.PurgeTable(conn.context.Ctx, identifier); err != nil {
					// try just drop
					err = conn.Catalog.DropTable(conn.context.Ctx, identifier)
				}
			} else {
				err = conn.Catalog.DropTable(conn.context.Ctx, identifier)
			}
			if err != nil && icebergNonIcebergCatalogErr(err) {
				err = conn.dropGlueEntry(identifier)
			}
			if err != nil {
				if g.IsDebug() && strings.Contains(err.Error(), "RuntimeIOException") {
					g.Warn(err.Error())
					return nil
				}
				return g.Error(err, "cannot drop table")
			}
			g.Debug("table %s dropped", tableName)
		}
	}

	return nil
}

func (conn *IcebergConn) CreateNamespaceIfNotExists(schema string) (err error) {

	namespace := table.Identifier{schema}
	exists, nsErr := conn.Catalog.CheckNamespaceExists(conn.Context().Ctx, namespace)
	if nsErr != nil {
		// Some catalogs might not implement namespace checking, log and continue
		g.Debug("could not check if namespace exists: %w", nsErr)
	} else if !exists {
		g.Debug("creating namespace: %s", namespace)
		if err = conn.Catalog.CreateNamespace(conn.Context().Ctx, namespace, nil); err != nil {
			return g.Error(err, "could not create namespace %s", schema)
		}
	}

	return nil
}

// SwapTable swaps two tables by renaming them
// 2025-06-09 => doesn't work, blank error
func (conn *IcebergConn) SwapTable(srcTable string, tgtTable string) (err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return g.Error(err, "Could not reconnect")
	}

	// Parse table names
	srcT, err := ParseTableName(srcTable, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse source table name: %s", srcTable)
	}

	tgtT, err := ParseTableName(tgtTable, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse target table name: %s", tgtTable)
	}

	// Create identifiers
	srcID := table.Identifier{srcT.Schema, srcT.Name}
	tgtID := table.Identifier{tgtT.Schema, tgtT.Name}

	// Create temporary table name
	tempName := tgtT.Name + "_tmp" + g.RandString(g.AlphaRunesLower, 2)
	tempID := table.Identifier{tgtT.Schema, tempName}

	// Drop temp table if exists
	tempT := Table{Schema: tgtT.Schema, Name: tempName}
	exists, err := conn.TableExists(tempT)
	if err != nil {
		return g.Error(err, "could not check temp table existence")
	}
	if exists {
		if err = conn.DropTable(tempT.FullName()); err != nil {
			return g.Error(err, "could not drop temp table %s", tempT.FullName())
		}
	}

	// Rename target table to temp
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, tgtID, tempID)
	if err != nil {
		return g.Error(err, "could not rename table %s to %s", tgtTable, tempT.FullName())
	}

	// Rename source table to target
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, srcID, tgtID)
	if err != nil {
		// Try to rollback
		conn.Catalog.RenameTable(conn.Context().Ctx, tempID, tgtID)
		return g.Error(err, "could not rename table %s to %s", srcTable, tgtTable)
	}

	// Rename temp table to source
	_, err = conn.Catalog.RenameTable(conn.Context().Ctx, tempID, srcID)
	if err != nil {
		// Try to rollback
		conn.Catalog.RenameTable(conn.Context().Ctx, tgtID, srcID)
		conn.Catalog.RenameTable(conn.Context().Ctx, tempID, tgtID)
		return g.Error(err, "could not rename table %s to %s", tempT.FullName(), srcTable)
	}

	g.Debug("successfully swapped tables %s and %s", srcTable, tgtTable)
	return nil
}

// BulkImportStream inserts a stream into a table using Arrow format.
// This method converts the incoming datastream to Apache Arrow format and uses
// the iceberg-go table.AppendTable API to write the data.
func (conn *IcebergConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return 0, g.Error(err, "Could not reconnect")
	}

	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return 0, g.Error(err, "could not parse table name: %s", tableFName)
	}

	// Parse table identifier
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID)
	if err != nil {
		return 0, g.Error(err, "could not load existing table: %s", tableFName)
	}

	// Batch size for writing
	fileMaxRows := 500000
	if bSize := cast.ToInt(conn.GetProp("file_max_rows")); bSize > 0 {
		fileMaxRows = bSize
	}

	// Use snapshot properties
	snapshotProps := map[string]string{
		"operation": "append",
		"source":    "sling-cli",
	}

	// Process batches from the datastream
	for batch := range ds.BatchChan {
		// Create Arrow schema from batch columns (in case they changed)
		arrowSchema := conn.icebergArrowSchema(batch.Columns)

		// Create memory allocator
		alloc := memory.NewGoAllocator()

		// Create record builder for this batch
		recordBuilder := array.NewRecordBuilder(alloc, arrowSchema)
		defer recordBuilder.Release()

		// Collect records for this batch
		records := []arrow.Record{}
		rowCount := 0

		for row := range batch.Rows {
			// Add row to record builder
			for i, val := range row {
				if i >= len(recordBuilder.Fields()) {
					break
				}
				iop.AppendToBuilder(recordBuilder.Field(i), &batch.Columns[i], val)
			}

			rowCount++
			count++

			// Create new record when batch size is reached
			if rowCount >= fileMaxRows {
				record := recordBuilder.NewRecord()
				records = append(records, record)
				rowCount = 0
				// Reset builder for next batch
				recordBuilder = array.NewRecordBuilder(alloc, arrowSchema)
				batch.Close() // close batch to write parquet file
			}
		}

		// Write final record if any rows remain
		if rowCount > 0 {
			record := recordBuilder.NewRecord()
			records = append(records, record)
		}

		// Skip if no records in this batch
		if len(records) == 0 {
			continue
		}

		// Create an Arrow table from records
		arrowTable := array.NewTableFromRecords(arrowSchema, records)
		defer arrowTable.Release()

		// Release individual records
		for _, rec := range records {
			rec.Release()
		}

		// Create a new transaction for this batch
		tx := tbl.NewTransaction()

		// Append the Arrow table to the Iceberg table
		err = tx.AppendTable(conn.Context().Ctx, arrowTable, cast.ToInt64(fileMaxRows), snapshotProps)
		if err != nil {
			return count, g.Error(err, "Failed to append data to Iceberg table %s", tableFName)
		}

		// Commit the transaction
		newTable, err := tx.Commit(conn.Context().Ctx)
		if err != nil {
			return count, g.Error(err, "Failed to commit data to Iceberg table %s", tableFName)
		}

		details := g.M("location", tbl.Location(), "batch_rows", len(batch.Rows))
		if currSnapshot := newTable.CurrentSnapshot(); currSnapshot != nil {
			details["snapshot_id"] = currSnapshot.SnapshotID
		}
		g.Debug("committed iceberg snapshot", details)
	}

	return count, nil
}

// BulkExportStream reads table data in bulk
func (conn *IcebergConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// determine where clause to apply to scanOptions

	sql := table.Select()

	options := g.M("columns", table.Columns)

	parts := strings.Split(sql, "--iceberg-json=")
	if len(parts) == 2 {
		m, err := g.UnmarshalMap(parts[1])
		if err != nil {
			return nil, g.Error(err, "malformed iceberg-json payload")
		}

		for k, v := range m {
			options[k] = v
		}
	}

	// For bulk export, we can use the regular stream rows functionality
	return conn.Self().StreamRows(sql, options)
}

// GetSchemata obtain full schemata info for a schema and/or table
func (conn *IcebergConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	err := conn.Connect()
	if err != nil {
		return schemata, g.Error(err, "could not connect to get schemata")
	}

	schemaNames := []string{schemaName}
	if schemaName == "" {
		data, err := conn.GetSchemas()
		if err != nil {
			return schemata, g.Error(err, "could not get schemas")
		}
		schemaNames = data.ColValuesStr(0)
	}

	schemas := map[string]Schema{}
	ctx := g.NewContext(conn.context.Ctx, 10)

	// Use catalog name as database name
	currDatabase := conn.Warehouse
	if currDatabase == "" {
		currDatabase = "iceberg"
	}

	getOneSchemata := func(schemaName string, tables []string) error {
		defer ctx.Wg.Read.Done()

		var data iop.Dataset

		switch level {
		case SchemataLevelSchema:
			data.Columns = iop.NewColumnsFromFields("schema_name")
			data.Append([]any{schemaName})
		case SchemataLevelTable:
			data, err = conn.GetTablesAndViews(schemaName)
		case SchemataLevelColumn:
			// Get all tables in schema first
			tablesData, err := conn.GetTables(schemaName)
			if err != nil {
				return g.Error(err, "Could not get tables for schema %s", schemaName)
			}

			// Then get columns for each table
			data.Columns = iop.NewColumnsFromFields("schema_name", "table_name", "column_name", "data_type", "position", "is_view")
			for _, row := range tablesData.Rows {
				schemaName := cast.ToString(row[0])
				tableName := cast.ToString(row[1])

				if len(tables) > 0 {
					matched := false
					for _, table := range tables {
						if strings.EqualFold(table, tableName) {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				fullTableName := schemaName + "." + tableName
				columns, err := conn.GetColumns(fullTableName)
				if err != nil {
					g.Warn("Could not get columns for table %s: %v", fullTableName, err)
					continue
				}

				for _, col := range columns {
					data.Append([]any{
						schemaName,
						tableName,
						col.Name,
						col.DbType,
						col.Position,
						false, // is_view
					})
				}
			}
		}

		if err != nil {
			return g.Error(err, "Could not get schemata at %s level for schema=%s", level, schemaName)
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range data.Records() {
			schemaName := cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))

			// Skip if any names contain periods
			if strings.Contains(tableName, ".") ||
				strings.Contains(schemaName, ".") ||
				strings.Contains(columnName, ".") {
				continue
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
					Dialect:  dbio.TypeDbIceberg,
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
					Position: cast.ToInt(rec["position"]),
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
		return nil
	}

	for _, schemaName := range schemaNames {
		if len(tableNames) > 0 && tableNames[0] != "" {
			g.Debug("getting schemata for %s (tables: %s)", schemaName, g.Marshal(tableNames))
		} else {
			g.Debug("getting schemata for %s", schemaName)
		}

		ctx.Wg.Read.Add()
		go func() {
			err := getOneSchemata(schemaName, tableNames)
			ctx.CaptureErr(err)
		}()
	}

	ctx.Wg.Read.Wait()

	if err := ctx.Err(); err != nil {
		return schemata, g.Error(err)
	}

	return schemata, nil
}

// generateIcebergSchema creates an Iceberg schema from iop columns
func (conn *IcebergConn) generateIcebergSchema(columns iop.Columns) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, len(columns))

	for i, col := range columns {
		// Convert iop column type to Iceberg type
		icebergType := conn.iopTypeToIcebergPrimitiveType(col)

		// Create nested field with auto-assigned ID (starting from 1)
		fields[i] = iceberg.NestedField{
			ID:       i + 1,
			Name:     col.Name,
			Type:     icebergType,
			Required: false, // Default to nullable unless we have constraint info
		}

		// Identifier fields must be required (Iceberg spec). PK columns are
		// treated as identifiers for equality-delete merge.
		if icebergColumnIsPK(col) {
			fields[i].Required = true
		}

		// Check if column has NOT NULL constraint
		if col.Constraint != nil && strings.Contains(strings.ToLower(col.Constraint.Expression), "not null") {
			fields[i].Required = true
		}
	}

	identIDs := icebergIdentifierFieldIDs(columns, fields)
	if len(identIDs) == 0 {
		return iceberg.NewSchema(0, fields...), nil
	}
	return iceberg.NewSchemaWithIdentifiers(0, identIDs, fields...), nil
}

func icebergIdentifierFieldIDs(columns iop.Columns, fields []iceberg.NestedField) []int {
	ids := make([]int, 0, len(columns))
	for i, col := range columns {
		if !icebergColumnIsPK(col) {
			continue
		}
		if i < len(fields) {
			ids = append(ids, fields[i].ID)
		}
	}
	return ids
}

func icebergColumnIsPK(col iop.Column) bool {
	if col.IsPrimaryKey() {
		return true
	}
	if col.Metadata == nil {
		return false
	}
	if col.Metadata[iop.ColMetaIsPrimaryKey.String()] != "" {
		return true
	}
	return col.Metadata[iop.PrimaryKey.MetadataKey()] != ""
}

// icebergArrowSchema builds the arrow schema used to append into an iceberg
// table. All timestamp fields get a zone because iopTypeToIcebergPrimitiveType
// declares every timestamp column as iceberg `timestamptz`. A zone-less arrow
// timestamp reads back as iceberg `timestamp`, which iceberg refuses to promote.
func (conn *IcebergConn) icebergArrowSchema(columns iop.Columns) *arrow.Schema {
	schema := iop.ColumnsToArrowSchema(columns)

	fields := schema.Fields()
	changed := false
	for i, field := range fields {
		if i < len(columns) && icebergColumnIsPK(columns[i]) && field.Nullable {
			fields[i].Nullable = false
			changed = true
		}
		tsType, ok := field.Type.(*arrow.TimestampType)
		if !ok || tsType.TimeZone != "" {
			continue
		}
		fields[i].Type = &arrow.TimestampType{Unit: tsType.Unit, TimeZone: "UTC"}
		changed = true
	}

	if !changed {
		return schema
	}
	return arrow.NewSchema(fields, nil)
}

// iopTypeToIcebergPrimitiveType converts iop column type to Iceberg primitive type
func (conn *IcebergConn) iopTypeToIcebergPrimitiveType(col iop.Column) iceberg.Type {
	switch col.Type {
	case iop.BoolType:
		return iceberg.PrimitiveTypes.Bool
	case iop.IntegerType, iop.SmallIntType:
		return iceberg.PrimitiveTypes.Int32
	case iop.BigIntType:
		return iceberg.PrimitiveTypes.Int64
	case iop.FloatType:
		return iceberg.PrimitiveTypes.Float64
	case iop.DecimalType:
		precision := lo.Ternary(col.DbPrecision > env.DdlMinDecLength, col.DbPrecision, env.DdlMinDecLength)
		scale := lo.Ternary(col.DbScale > env.DdlMinDecScale, col.DbScale, env.DdlMinDecScale)
		return iceberg.DecimalTypeOf(precision, scale)
	case iop.DateType:
		return iceberg.PrimitiveTypes.Date
	case iop.DatetimeType, iop.TimestampType:
		return iceberg.PrimitiveTypes.TimestampTz // arrow Timestamp converts to iceberg timestampz
	case iop.TimestampzType:
		return iceberg.PrimitiveTypes.TimestampTz
	case iop.TimeType:
		return iceberg.PrimitiveTypes.Time
	case iop.TimezType:
		// Iceberg doesn't have a specific time with timezone type, use Time
		return iceberg.PrimitiveTypes.Time
	case iop.StringType, iop.TextType:
		return iceberg.PrimitiveTypes.String
	case iop.UUIDType:
		return iceberg.PrimitiveTypes.UUID
	case iop.BinaryType:
		return iceberg.PrimitiveTypes.Binary
	case iop.JsonType:
		// JSON is typically stored as string in Iceberg
		return iceberg.PrimitiveTypes.String
	default:
		// Default to string for unknown types
		return iceberg.PrimitiveTypes.String
	}
}

// queryViaDuckDB executes a custom SQL query using DuckDB with Iceberg REST or Glue catalog
// https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
// https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_s3_tables
// https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_sagemaker_lakehouse
func (conn *IcebergConn) queryViaDuckDB(ctx context.Context, sql string, opts map[string]any) (ds *iop.Datastream, err error) {
	if conn.CatalogType == dbio.IcebergCatalogTypeSQL {
		g.Warn("for querying iceberg with custom SQL via DuckDB, cannot do so with SQL-Catalog. DuckDB only supports REST and Glue catalog types.")
		return nil, g.Error("unsupported catalog for DuckDB Iceberg extension.")
	}
	if conn.CatalogType == dbio.IcebergCatalogTypeGlue {
		g.Warn("for querying iceberg with custom SQL via DuckDB, glue catalog doesn't seem to work well. see https://duckdb.org/docs/stable/core_extensions/iceberg/amazon_sagemaker_lakehouse")
	}

	if !strings.Contains(sql, "iceberg_catalog.") {
		g.Warn("for querying iceberg with custom SQL via DuckDB, the table names need to be qualified with \"iceberg_catalog\". For example: select count(*) cnt from iceberg_catalog.my_namespace.my_table")
		return nil, g.Error("missing qualifier \"iceberg_catalog\"")
	}

	if err = conn.ensureDuckCatalog(); err != nil {
		return nil, err
	}

	return conn.duck.StreamContext(ctx, sql, opts)
}

// ensureDuckCatalog ATTACHes the Iceberg catalog on conn.duck for reads and DuckDB merge.
func (conn *IcebergConn) ensureDuckCatalog() (err error) {
	if conn.duck != nil {
		return nil
	}

	if conn.CatalogType == dbio.IcebergCatalogTypeSQL {
		return g.Error("unsupported catalog for DuckDB Iceberg extension.")
	}

	conn.duck = iop.NewDuckDb(conn.Context().Ctx, "sling_conn_id", conn.GetProp("sling_conn_id"))
	conn.duck.AddExtension("iceberg")
	conn.duck.AddExtension("https")

	if err = conn.duck.Open(); err != nil {
		conn.duck = nil
		return g.Error(err, "could not open DuckDB connection for Iceberg query")
	}

	attachSQL := ""
	catalogType := lo.Ternary(
		conn.isS3TablesViaREST(),
		dbio.IcebergCatalogTypeS3Tables,
		conn.CatalogType,
	)

	switch catalogType {
	case dbio.IcebergCatalogTypeREST:
		secretProps := MakeDuckDbSecretProps(conn, iop.DuckDbSecretTypeIceberg)
		secret := iop.NewDuckDbSecret("iceberg_secret", iop.DuckDbSecretTypeIceberg, secretProps)
		conn.duck.AddSecret(secret)

		restEndpoint := conn.GetProp("rest_uri")
		if restEndpoint == "" {
			conn.duck.Close()
			conn.duck = nil
			return g.Error("rest_uri property is required for REST catalog")
		}
		attachSQL = g.F("ATTACH '%s' AS iceberg_catalog (TYPE ICEBERG, SECRET iceberg_secret, ENDPOINT '%s')", conn.Warehouse, restEndpoint)

	case dbio.IcebergCatalogTypeGlue, dbio.IcebergCatalogTypeS3Tables:
		var storageSecretType iop.DuckDbSecretType
		for key := range conn.properties {
			if storageSecretType != iop.DuckDbSecretTypeUnknown {
				break
			} else if strings.HasPrefix(key, "s3_") {
				storageSecretType = iop.DuckDbSecretTypeS3
			} else if strings.HasPrefix(key, "gcs_") {
				storageSecretType = iop.DuckDbSecretTypeGCS
			}
		}

		if storageSecretType == iop.DuckDbSecretTypeUnknown {
			conn.duck.Close()
			conn.duck = nil
			return g.Error("could not make AWS duckdb Iceberg secret for Iceberg query")
		}

		secretProps := MakeDuckDbSecretProps(conn, storageSecretType)
		secret := iop.NewDuckDbSecret("iceberg_storage_secret", storageSecretType, secretProps)
		conn.duck.AddSecret(secret)

		if catalogType == dbio.IcebergCatalogTypeGlue {
			accountID := conn.GetProp("glue_account_id")
			namespace := conn.GetProp("glue_namespace")
			if accountID == "" || namespace == "" {
				conn.duck.Close()
				conn.duck = nil
				return g.Error("glue_account_id and glue_namespace properties are required for GLUE catalog")
			}

			warehouse := g.F("%s:s3tablescatalog/%s", accountID, namespace)
			attachSQL = g.F("ATTACH '%s' AS iceberg_catalog (TYPE ICEBERG, ENDPOINT_TYPE glue, SECRET iceberg_storage_secret)", warehouse)
		}

		if catalogType == dbio.IcebergCatalogTypeS3Tables {
			arn := conn.GetProp("s3tables_arn", "rest_warehouse")
			if !strings.HasPrefix(arn, "arn:aws:s3tables") {
				conn.duck.Close()
				conn.duck = nil
				return g.Error("warehouse property is required for S3 Tables catalog via DuckDB")
			}

			attachSQL = g.F("ATTACH '%s' AS iceberg_catalog (TYPE ICEBERG, ENDPOINT_TYPE s3_tables, SECRET iceberg_storage_secret)", arn)
		}

	default:
		conn.duck.Close()
		conn.duck = nil
		return g.Error("Unsupported catalog type for DuckDB query: %s", catalogType)
	}

	if _, err = conn.duck.Exec(attachSQL + env.NoDebugKey); err != nil {
		conn.duck.Close()
		conn.duck = nil
		return g.Error(err, "could not attach Iceberg catalog")
	}

	// Do not USE iceberg_catalog — SET schema fails (no catalog+schema named iceberg_catalog).
	return nil
}

// icebergMergeEngine is the writer used for Iceberg incremental+PK / CDC.
type icebergMergeEngine string

const (
	icebergMergeEngineAppend icebergMergeEngine = "append"
	icebergMergeEngineGo     icebergMergeEngine = "go"
	icebergMergeEngineDuckDB icebergMergeEngine = "duckdb"
)

type icebergStorageKind string

const (
	icebergStorageS3      icebergStorageKind = "s3"
	icebergStorageGCS     icebergStorageKind = "gcs"
	icebergStorageAzure   icebergStorageKind = "azure"
	icebergStorageLocal   icebergStorageKind = "local"
	icebergStorageUnknown icebergStorageKind = "unknown"
)

// icebergGoHasRowDelta reports whether the linked iceberg-go build exposes
// Transaction.NewRowDelta (apache iceberg-go v0.6+).
func icebergGoHasRowDelta() bool {
	t := reflect.TypeOf((*table.Transaction)(nil))
	_, ok := t.MethodByName("NewRowDelta")
	return ok
}

type icebergMergeSplit struct {
	Columns iop.Columns
	Upserts [][]any
	Deletes [][]any // PK rows to equality-delete (may include upsert PKs)
	Count   uint64
}

func icebergPKKey(row []any, pkIdx []int) string {
	parts := make([]string, len(pkIdx))
	for i, idx := range pkIdx {
		if idx < len(row) {
			parts[i] = cast.ToString(row[idx])
		}
	}
	return strings.Join(parts, "\x1f")
}

// icebergCatalogKind returns REST / glue / sql / s3tables for routing.
func (conn *IcebergConn) icebergCatalogKind() dbio.IcebergCatalogType {
	if conn != nil && conn.isS3TablesViaREST() {
		return dbio.IcebergCatalogTypeS3Tables
	}
	if conn == nil {
		return ""
	}
	return conn.CatalogType
}

func (conn *IcebergConn) icebergStorageKind() icebergStorageKind {
	if conn == nil {
		return icebergStorageUnknown
	}
	warehouse := conn.Warehouse
	if warehouse == "" {
		warehouse = conn.GetProp("rest_warehouse", "sql_warehouse", "glue_warehouse")
	}
	w := strings.ToLower(warehouse)
	get := func(keys ...string) string {
		for _, k := range keys {
			if v := conn.GetProp(k); v != "" {
				return v
			}
		}
		return ""
	}

	switch {
	case strings.HasPrefix(w, "gs://"), strings.HasPrefix(w, "gcs://"), get("gcs_access_key_id") != "":
		return icebergStorageGCS
	case strings.HasPrefix(w, "abfs"), strings.HasPrefix(w, "abfss"), strings.HasPrefix(w, "wasb"),
		strings.HasPrefix(w, "wasbs"), get("azure_account_name") != "", get("azure_account_key") != "":
		return icebergStorageAzure
	case strings.HasPrefix(w, "s3://"), strings.HasPrefix(w, "s3a://"), strings.HasPrefix(w, "arn:aws:s3tables"),
		get("s3_access_key_id") != "", get("s3_region") != "", get("s3_endpoint") != "":
		return icebergStorageS3
	case strings.HasPrefix(w, "file://"), strings.HasPrefix(w, "/"):
		return icebergStorageLocal
	default:
		return icebergStorageUnknown
	}
}

func (conn *IcebergConn) mergeEngine(needMerge bool) (icebergMergeEngine, error) {
	engine, err := conn.mergeEngineWith(needMerge, icebergGoHasRowDelta())
	if err != nil {
		return "", err
	}
	g.Debug("iceberg merge engine=%s catalog=%s storage=%s", engine, conn.icebergCatalogKind(), conn.icebergStorageKind())
	return engine, nil
}

// mergeEngineWith chooses append / go / duckdb. DuckDB is never used for
// Glue, SQL catalog, or Azure.
func (conn *IcebergConn) mergeEngineWith(needMerge bool, goHasRowDelta bool) (icebergMergeEngine, error) {
	catalog := conn.icebergCatalogKind()
	storage := conn.icebergStorageKind()
	if !needMerge {
		return icebergMergeEngineAppend, nil
	}
	if goHasRowDelta {
		return icebergMergeEngineGo, nil
	}

	duckOK := (catalog == dbio.IcebergCatalogTypeREST || catalog == dbio.IcebergCatalogTypeS3Tables) &&
		(storage == icebergStorageS3 || storage == icebergStorageGCS || storage == icebergStorageUnknown)
	if duckOK {
		return icebergMergeEngineDuckDB, nil
	}

	return "", g.Error("Iceberg merge is not supported for catalog=%s storage=%s", catalog, storage)
}

// CheckMergeSupported fails fast when Iceberg incremental+PK / CDC cannot merge
// for this catalog/storage (no silent append).
func (conn *IcebergConn) CheckMergeSupported() error {
	_, err := conn.mergeEngine(true)
	return err
}

// MergeStream applies incremental upsert or CDC (insert/update/delete) to an
// Iceberg table without a temp catalog table. Buffers one batch in memory.
func (conn *IcebergConn) MergeStream(tableFName string, ds *iop.Datastream, pkFields []string, strategy *MergeStrategy) (count uint64, err error) {
	err = reconnectIfClosed(conn)
	if err != nil {
		return 0, g.Error(err, "Could not reconnect")
	}
	if len(pkFields) == 0 {
		return 0, g.Error("Iceberg merge requires a primary-key")
	}

	data, err := ds.Collect(0)
	if err != nil {
		return 0, g.Error(err, "could not collect merge rows")
	}
	if len(data.Rows) == 0 {
		return 0, nil
	}

	split, err := conn.dedupMergeRows(data.Columns, data.Rows, pkFields, strategy)
	if err != nil {
		return 0, err
	}
	count = split.Count

	engine, err := conn.mergeEngine(true)
	if err != nil {
		return 0, err
	}

	switch engine {
	case icebergMergeEngineGo:
		err = conn.mergeStreamGo(tableFName, split, pkFields)
	case icebergMergeEngineDuckDB:
		err = conn.mergeStreamDuckDB(tableFName, split, pkFields, strategy)
	default:
		err = g.Error("Iceberg merge is not supported for catalog=%s storage=%s", conn.icebergCatalogKind(), conn.icebergStorageKind())
	}
	if err != nil {
		return count, err
	}
	return count, nil
}

func (conn *IcebergConn) dedupMergeRows(columns iop.Columns, rows [][]any, pkFields []string, strategy *MergeStrategy) (icebergMergeSplit, error) {
	out := icebergMergeSplit{Columns: columns, Count: uint64(len(rows))}
	if len(pkFields) == 0 {
		return out, g.Error("Iceberg merge requires a primary-key")
	}

	fieldMap := columns.FieldMap(true)
	pkIdx := make([]int, len(pkFields))
	for i, pk := range pkFields {
		idx, ok := fieldMap[strings.ToLower(pk)]
		if !ok {
			return out, g.Error("primary-key column %s not found in merge stream", pk)
		}
		pkIdx[i] = idx
	}

	opIdx, seqIdx := -1, -1
	if i, ok := fieldMap[strings.ToLower(env.ReservedFields.SyncedOp)]; ok {
		opIdx = i
	}
	if i, ok := fieldMap[strings.ToLower(env.ReservedFields.CDCSeq)]; ok {
		seqIdx = i
	}

	st := MergeStrategyNone
	if strategy != nil {
		st = *strategy
	}
	isCDC := st == MergeStrategyChangeCapture || st == MergeStrategyChangeCaptureSoft
	soft := st == MergeStrategyChangeCaptureSoft

	type kept struct {
		row []any
		seq int64
		idx int
		op  string
	}
	best := map[string]kept{}

	for i, row := range rows {
		key := icebergPKKey(row, pkIdx)
		seq := int64(0)
		if seqIdx >= 0 && seqIdx < len(row) {
			seq = cast.ToInt64(row[seqIdx])
		}
		op := ""
		if opIdx >= 0 && opIdx < len(row) {
			op = strings.ToUpper(strings.TrimSpace(cast.ToString(row[opIdx])))
		}
		prev, ok := best[key]
		if !ok || seq > prev.seq || (seq == prev.seq && i >= prev.idx) {
			best[key] = kept{row: row, seq: seq, idx: i, op: op}
		}
	}

	for _, k := range best {
		op := k.op
		switch {
		case isCDC && op == "D" && !soft:
			out.Deletes = append(out.Deletes, k.row)
		case isCDC && op == "D" && soft:
			out.Deletes = append(out.Deletes, k.row)
			out.Upserts = append(out.Upserts, k.row)
		case isCDC && (op == "U" || op == "I" || op == "S" || op == ""):
			if op == "U" {
				out.Deletes = append(out.Deletes, k.row)
			}
			out.Upserts = append(out.Upserts, k.row)
		default:
			// incremental merge: equality-delete every PK, then append
			out.Deletes = append(out.Deletes, k.row)
			out.Upserts = append(out.Upserts, k.row)
		}
	}

	return out, nil
}

func (conn *IcebergConn) mergeStreamGo(tableFName string, split icebergMergeSplit, pkFields []string) error {
	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse table name: %s", tableFName)
	}
	tableID := table.Identifier{t.Schema, t.Name}
	tbl, err := conn.Catalog.LoadTable(conn.Context().Ctx, tableID)
	if err != nil {
		return g.Error(err, "could not load table: %s", tableFName)
	}

	tbl, err = conn.ensureIdentifierFields(tbl, pkFields)
	if err != nil {
		return err
	}

	pkFieldIDs, err := conn.pkFieldIDs(tbl.Schema(), pkFields)
	if err != nil {
		return err
	}

	tx := tbl.NewTransaction()
	snapshotProps := iceberg.Properties{
		"source": "sling-cli",
	}

	var eqFiles []iceberg.DataFile
	if len(split.Deletes) > 0 {
		delCols, delRows := conn.projectPKRows(split.Columns, split.Deletes, pkFields)
		delArrow, err := conn.pkArrowSchema(tbl.Schema(), pkFields)
		if err != nil {
			return err
		}
		eqRecords, err := conn.rowsToArrowRecords(delCols, delRows, delArrow)
		if err != nil {
			return g.Error(err, "could not build equality-delete records")
		}
		// WriteEqualityDeletes does not take ownership of batches.
		defer releaseArrowRecords(eqRecords)

		eqFiles, err = tx.WriteEqualityDeletes(conn.Context().Ctx, pkFieldIDs, icebergRecordIter(eqRecords))
		if err != nil {
			return g.Error(err, "could not write Iceberg equality deletes")
		}
	}

	var dataFiles []iceberg.DataFile
	if len(split.Upserts) > 0 {
		upCols := conn.columnsInSchema(split.Columns, tbl.Schema())
		upArrow := conn.annotateArrowFieldIDs(conn.icebergArrowSchema(upCols), tbl.Schema())
		upRecords, err := conn.rowsToArrowRecords(split.Columns, split.Upserts, upArrow)
		if err != nil {
			return g.Error(err, "could not build upsert records")
		}
		// WriteRecords releases each yielded batch; retain so our defer is safe.
		defer releaseArrowRecords(upRecords)

		for df, werr := range table.WriteRecords(conn.Context().Ctx, tbl, upArrow, icebergRecordIterRetain(upRecords)) {
			if werr != nil {
				return g.Error(werr, "could not write Iceberg upsert data files")
			}
			dataFiles = append(dataFiles, df)
		}
	}

	if len(eqFiles) == 0 && len(dataFiles) == 0 {
		return nil
	}

	delta := tx.NewRowDelta(snapshotProps)
	if len(eqFiles) > 0 {
		delta.AddDeletes(eqFiles...)
	}
	if len(dataFiles) > 0 {
		delta.AddRows(dataFiles...)
	}
	if err = delta.Commit(conn.Context().Ctx); err != nil {
		return g.Error(err, "could not commit Iceberg row delta")
	}

	newTable, err := tx.Commit(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "could not commit Iceberg merge transaction")
	}

	details := g.M("location", tbl.Location(), "engine", "go", "eq_deletes", len(eqFiles), "data_files", len(dataFiles))
	if currSnapshot := newTable.CurrentSnapshot(); currSnapshot != nil {
		details["snapshot_id"] = currSnapshot.SnapshotID
	}
	g.Debug("committed iceberg snapshot", details)
	return nil
}

func (conn *IcebergConn) ensureIdentifierFields(tbl *table.Table, pkFields []string) (*table.Table, error) {
	if tbl == nil || len(pkFields) == 0 {
		return tbl, nil
	}
	schema := tbl.Schema()
	if len(schema.IdentifierFieldIDs) > 0 {
		return tbl, nil
	}

	paths := make([][]string, 0, len(pkFields))
	for _, pk := range pkFields {
		field, ok := schema.FindFieldByNameCaseInsensitive(pk)
		if !ok {
			g.Warn("iceberg identifier field %s not found in schema", pk)
			continue
		}
		paths = append(paths, []string{field.Name})
	}
	if len(paths) == 0 {
		return tbl, nil
	}

	tx := tbl.NewTransaction()
	if err := tx.UpdateSchema(false, false).SetIdentifierField(paths).Commit(); err != nil {
		return nil, g.Error(err, "could not set Iceberg identifier fields %v", pkFields)
	}
	newTable, err := tx.Commit(conn.Context().Ctx)
	if err != nil {
		return nil, g.Error(err, "could not commit Iceberg identifier fields")
	}
	g.Debug("set iceberg identifier fields %v", pkFields)
	return newTable, nil
}

func (conn *IcebergConn) pkFieldIDs(schema *iceberg.Schema, pkFields []string) ([]int, error) {
	ids := make([]int, 0, len(pkFields))
	for _, pk := range pkFields {
		field, ok := schema.FindFieldByNameCaseInsensitive(pk)
		if !ok {
			return nil, g.Error("primary-key column %s not found in Iceberg schema", pk)
		}
		ids = append(ids, field.ID)
	}
	return ids, nil
}

func (conn *IcebergConn) projectPKRows(columns iop.Columns, rows [][]any, pkFields []string) (iop.Columns, [][]any) {
	fieldMap := columns.FieldMap(true)
	pkIdx := make([]int, 0, len(pkFields))
	pkCols := make(iop.Columns, 0, len(pkFields))
	for _, pk := range pkFields {
		idx, ok := fieldMap[strings.ToLower(pk)]
		if !ok {
			continue
		}
		pkIdx = append(pkIdx, idx)
		pkCols = append(pkCols, columns[idx])
	}
	out := make([][]any, len(rows))
	for i, row := range rows {
		proj := make([]any, len(pkIdx))
		for j, idx := range pkIdx {
			if idx < len(row) {
				proj[j] = row[idx]
			}
		}
		out[i] = proj
	}
	return pkCols, out
}

func (conn *IcebergConn) pkArrowSchema(iceSchema *iceberg.Schema, pkFields []string) (*arrow.Schema, error) {
	if iceSchema == nil {
		return nil, g.Error("Iceberg schema is required for equality deletes")
	}
	fields := make([]iceberg.NestedField, 0, len(pkFields))
	for _, pk := range pkFields {
		field, ok := iceSchema.FindFieldByNameCaseInsensitive(pk)
		if !ok {
			return nil, g.Error("primary-key column %s not found in Iceberg schema", pk)
		}
		fields = append(fields, field)
	}
	sc := iceberg.NewSchema(0, fields...)
	arrowSc, err := table.SchemaToArrowSchema(sc, nil, true, false)
	if err != nil {
		return nil, g.Error(err, "could not convert equality-delete schema to Arrow")
	}
	return arrowSc, nil
}

// columnsInSchema keeps source columns that exist on the Iceberg table.
// Extra source fields (e.g. json_data on an upsert CSV) are dropped so
// WriteRecords schema-compat does not fail.
func (conn *IcebergConn) columnsInSchema(columns iop.Columns, iceSchema *iceberg.Schema) iop.Columns {
	if iceSchema == nil {
		return columns
	}
	out := make(iop.Columns, 0, len(columns))
	for _, col := range columns {
		if _, ok := iceSchema.FindFieldByNameCaseInsensitive(col.Name); ok {
			out = append(out, col)
		}
	}
	if len(out) == 0 {
		return columns
	}
	return out
}

func (conn *IcebergConn) annotateArrowFieldIDs(arrowSchema *arrow.Schema, iceSchema *iceberg.Schema) *arrow.Schema {
	if arrowSchema == nil || iceSchema == nil {
		return arrowSchema
	}
	fields := arrowSchema.Fields()
	changed := false
	for i, f := range fields {
		iceField, ok := iceSchema.FindFieldByNameCaseInsensitive(f.Name)
		if !ok {
			continue
		}
		if _, has := f.Metadata.GetValue(table.ArrowParquetFieldIDKey); has {
			continue
		}
		keys := append(append([]string{}, f.Metadata.Keys()...), table.ArrowParquetFieldIDKey)
		vals := append(append([]string{}, f.Metadata.Values()...), strconv.Itoa(iceField.ID))
		fields[i].Metadata = arrow.NewMetadata(keys, vals)
		changed = true
	}
	if !changed {
		return arrowSchema
	}
	return arrow.NewSchema(fields, nil)
}

func (conn *IcebergConn) rowsToArrowRecords(columns iop.Columns, rows [][]any, arrowSchema *arrow.Schema) ([]arrow.Record, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if arrowSchema == nil {
		arrowSchema = conn.icebergArrowSchema(columns)
	}
	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, arrowSchema)
	defer builder.Release()

	fieldMap := columns.FieldMap(true)
	nFields := len(builder.Fields())

	for _, row := range rows {
		for i := 0; i < nFields; i++ {
			name := arrowSchema.Field(i).Name
			idx, ok := fieldMap[strings.ToLower(name)]
			var val any
			col := &iop.Column{Name: name}
			if ok {
				col = &columns[idx]
				if idx < len(row) {
					val = row[idx]
				}
			} else if i < len(columns) {
				col = &columns[i]
				if i < len(row) {
					val = row[i]
				}
			}
			iop.AppendToBuilder(builder.Field(i), col, val)
		}
	}
	rec := builder.NewRecord()
	return []arrow.Record{rec}, nil
}

func icebergRecordIter(records []arrow.Record) iter.Seq2[arrow.RecordBatch, error] {
	return icebergRecordIterMaybeRetain(records, false)
}

func icebergRecordIterRetain(records []arrow.Record) iter.Seq2[arrow.RecordBatch, error] {
	return icebergRecordIterMaybeRetain(records, true)
}

func icebergRecordIterMaybeRetain(records []arrow.Record, retain bool) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		for _, rec := range records {
			if rec == nil {
				continue
			}
			if retain {
				rec.Retain()
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

func releaseArrowRecords(records []arrow.Record) {
	for _, rec := range records {
		if rec != nil {
			rec.Release()
		}
	}
}

func (conn *IcebergConn) mergeStreamDuckDB(tableFName string, split icebergMergeSplit, pkFields []string, strategy *MergeStrategy) error {
	if err := conn.ensureDuckCatalog(); err != nil {
		return err
	}

	t, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		return g.Error(err, "could not parse table name: %s", tableFName)
	}

	tgt := conn.duckQualified(t.Schema, t.Name)
	srcName := "sling_merge_src"

	fieldMap := split.Columns.FieldMap(true)
	pkIdx := make([]int, 0, len(pkFields))
	for _, pk := range pkFields {
		if i, ok := fieldMap[strings.ToLower(pk)]; ok {
			pkIdx = append(pkIdx, i)
		}
	}
	allRows := make([][]any, 0, len(split.Upserts)+len(split.Deletes))
	seen := map[string]struct{}{}
	for _, row := range append(append([][]any{}, split.Upserts...), split.Deletes...) {
		key := icebergPKKey(row, pkIdx)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		allRows = append(allRows, row)
	}

	if err = conn.duckLoadTempTable(srcName, split.Columns, allRows); err != nil {
		return err
	}

	st := MergeStrategyNone
	if strategy != nil {
		st = *strategy
	}

	sqls, err := conn.duckMergeSQL(tgt, srcName, split.Columns, pkFields, st)
	if err != nil {
		return err
	}

	if _, err = conn.duck.ExecMultiContext(conn.Context().Ctx, sqls...); err != nil {
		return g.Error(err, "DuckDB Iceberg merge failed")
	}

	g.Debug("committed iceberg snapshot", g.M("engine", "duckdb", "target", tgt, "rows", split.Count))
	return nil
}

func (conn *IcebergConn) duckQualified(schema, name string) string {
	q := conn.Quote
	if schema == "" {
		return "iceberg_catalog." + q(name)
	}
	return "iceberg_catalog." + q(schema) + "." + q(name)
}

func (conn *IcebergConn) duckMergeSQL(tgt, src string, columns iop.Columns, pkFields []string, strategy MergeStrategy) ([]string, error) {
	if len(pkFields) == 0 {
		return nil, g.Error("Iceberg merge requires a primary-key")
	}

	if strategy == MergeStrategyNone {
		strategy = MergeStrategy(conn.GetTemplateValue("variable.default_merge_strategy"))
	}
	if strategy == MergeStrategyNone {
		strategy = MergeStrategyDeleteInsert
	}

	tmpl := conn.GetTemplateValue(g.F("core.merge_%s", strategy))
	if tmpl == "" {
		return nil, g.Error("merge strategy `%s` not supported for iceberg", strategy)
	}

	pkMap := map[string]struct{}{}
	pkQuoted := make([]string, len(pkFields))
	pkEqual := make([]string, len(pkFields))
	for i, pk := range pkFields {
		qpk := conn.Quote(pk)
		pkQuoted[i] = qpk
		pkEqual[i] = g.F("src.%s = tgt.%s", qpk, qpk)
		pkMap[strings.ToLower(pk)] = struct{}{}
	}

	insertFields := make([]string, 0, len(columns))
	srcFields := make([]string, 0, len(columns))
	setFields := make([]string, 0, len(columns))
	setFieldsAll := make([]string, 0, len(columns))
	for _, col := range columns {
		qcol := conn.Quote(col.Name)
		insertFields = append(insertFields, qcol)
		srcFields = append(srcFields, qcol)
		setField := g.F("%s = src.%s", qcol, qcol)
		setFieldsAll = append(setFieldsAll, setField)
		if _, isPK := pkMap[strings.ToLower(col.Name)]; !isPK {
			setFields = append(setFields, setField)
		}
	}
	if len(setFields) == 0 {
		setFields = setFieldsAll
	}

	sql := g.R(
		tmpl,
		"src_table", src,
		"tgt_table", tgt,
		"src_tgt_pk_equal", strings.Join(pkEqual, " and "),
		"pk_fields", strings.Join(pkQuoted, ", "),
		"insert_fields", strings.Join(insertFields, ", "),
		"src_fields", strings.Join(srcFields, ", "),
		"set_fields", strings.Join(setFields, ", "),
	)

	sqls := ParseSQLMultiStatements(sql, conn.GetType())
	if len(sqls) == 0 {
		return nil, g.Error("empty merge SQL for iceberg strategy `%s`", strategy)
	}
	return sqls, nil
}

func (conn *IcebergConn) duckLoadTempTable(srcName string, columns iop.Columns, rows [][]any) error {
	folder := path.Join(env.GetTempFolder(), "iceberg-merge", g.RandSuffix("src", 6))
	if err := os.MkdirAll(folder, 0755); err != nil {
		return g.Error(err, "could not create temp dir for Iceberg merge")
	}
	defer env.RemoveAllLocalTempFile(folder)

	csvPath := path.Join(folder, "src.csv")
	f, err := os.Create(csvPath)
	if err != nil {
		return g.Error(err, "could not create temp csv")
	}
	data := iop.NewDataset(columns)
	data.Rows = rows
	if _, err = data.WriteCsv(f); err != nil {
		f.Close()
		return g.Error(err, "could not write merge csv")
	}
	if err = f.Close(); err != nil {
		return err
	}

	escaped := strings.ReplaceAll(csvPath, `'`, `''`)
	sql := g.F("CREATE OR REPLACE TEMP TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true)", srcName, escaped)
	if _, err = conn.duck.Exec(sql + env.NoDebugKey); err != nil {
		return g.Error(err, "could not load DuckDB temp merge table")
	}
	return nil
}
