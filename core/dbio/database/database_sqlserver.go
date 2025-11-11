package database

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"strings"
	"time"
	"unicode/utf16"

	"cloud.google.com/go/cloudsqlconn"
	cloudsqlmssql "cloud.google.com/go/cloudsqlconn/sqlserver/mssql"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"

	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/microsoft/go-mssqldb/azuread"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MsSQLServerConn is a Microsoft SQL Server connection
type MsSQLServerConn struct {
	BaseConn
	URL             string
	versionNum      int
	versionYear     int
	isCloudSQL      bool
	cloudSQLCleanup func()
}

// Init initiates the object
func (conn *MsSQLServerConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbSQLServer
	conn.BaseConn.defaultPort = 1433
	conn.versionYear = 2017 // default version

	conn.SetProp("use_bcp_map_parallel", "false")

	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "true")
	}

	if strings.HasPrefix(conn.URL, "fabric:") {
		conn.Type = dbio.TypeDbFabric
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	// https://github.com/slingdata-io/sling-cli/issues/310
	// If both a portNumber and instanceName are used, the portNumber will take precedence and the instanceName will be ignored.
	// therefore, if instanceName is provided, don't set a defaultPort
	if u, err := dburl.Parse(conn.URL); err == nil {
		if instanceName := strings.TrimPrefix(u.Path, "/"); instanceName != "" {
			// set as 0 so BaseConn.Init won't inject port number
			conn.BaseConn.defaultPort = 0
		}
	}

	return conn.BaseConn.Init()
}

// parseVersion extracts the version information from SQL Server
func (conn *MsSQLServerConn) parseVersion(versionStr string) {
	conn.versionYear = 2017 // default version

	// Look for pattern " - XX.X.XXXX.X" and extract the major version
	versionNums := strings.Split(versionStr, ".")
	if len(versionNums) >= 2 {
		conn.versionNum = cast.ToInt(versionNums[0])
		// Map major version numbers to SQL Server years
		switch conn.versionNum {
		case 17:
			conn.versionYear = 2025
		case 16:
			conn.versionYear = 2022
		case 15:
			conn.versionYear = 2019
		case 14:
			conn.versionYear = 2017
		case 13:
			conn.versionYear = 2016
		case 12:
			conn.versionYear = 2014
		case 11:
			conn.versionYear = 2012
		case 10:
			conn.versionYear = 2008
		}
	}
}

// GetURL returns the processed URL
func (conn *MsSQLServerConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse SQL Server URL")
		return connURL
	}

	return url.String()
}

type SqlServerLogger struct{}

func (l *SqlServerLogger) Printf(format string, v ...any) {
	env.Print(g.F(format, v...))
}
func (l *SqlServerLogger) Println(v ...any) {
	if len(v) == 1 {
		env.Println(cast.ToString(v[0]))
	}
	if len(v) > 1 {
		env.Println(g.F(cast.ToString(v[0]), v...))
	}
}

func (conn *MsSQLServerConn) ConnString() string {

	propMapping := map[string]string{
		"connection timeout": "connection timeout",
		"connection_timeout": "connection timeout",

		"dial timeout": "dial timeout",
		"dial_timeout": "dial timeout",

		"encrypt": "encrypt",

		"authenticator": "authenticator",

		"app name": "app name",
		"app_name": "app name",

		"keepAlive":  "keepAlive",
		"keep_alive": "keepAlive",

		"failoverpartner":  "failoverpartner",
		"failover_partner": "failoverpartner",

		"failoverport":  "failoverport",
		"failover_port": "failoverport",

		"packet size": "packet size",
		"packet_size": "packet size",

		"log": "log",

		"TrustServerCertificate":   "TrustServerCertificate",
		"trust_server_certificate": "TrustServerCertificate",

		"TrustedConnection":  "TrustedConnection",
		"trusted_connection": "TrustedConnection",

		"certificate": "certificate",

		"hostNameInCertificate":   "hostNameInCertificate",
		"hostname_in_certificate": "hostNameInCertificate",

		"tlsmin":  "tlsmin",
		"tls_min": "tlsmin",

		"ServerSPN":  "ServerSPN",
		"server_spn": "ServerSPN",

		"Workstation ID": "Workstation ID",
		"workstation_id": "Workstation ID",

		"ApplicationIntent":  "ApplicationIntent",
		"application_intent": "ApplicationIntent",

		"protocol":          "protocol",
		"columnencryption":  "columnencryption",
		"column_encryption": "columnencryption",

		"multisubnetfailover":   "multisubnetfailover",
		"multi_subnet_failover": "multisubnetfailover",

		"pipe": "pipe",

		// Kerberos
		"krb5-configfile":         "krb5-configfile",
		"krb5-realm":              "krb5-realm",
		"krb5-keytabfile":         "krb5-keytabfile",
		"krb5-credcachefile":      "krb5-credcachefile",
		"krb5-dnslookupkdc":       "krb5-dnslookupkdc",
		"krb5-udppreferencelimit": "krb5-udppreferencelimit",

		"krb5_config_file":          "krb5-configfile",
		"krb5_realm":                "krb5-realm",
		"krb5_keytab_file":          "krb5-keytabfile",
		"krb5_cred_cache_file":      "krb5-credcachefile",
		"krb5_dns_lookup_kdc":       "krb5-dnslookupkdc",
		"krb5_udp_preference_limit": "krb5-udppreferencelimit",

		// Azure Active Directory authentication (https://github.com/microsoft/go-mssqldb?tab=readme-ov-file#azure-active-directory-authentication)
		"fedauth":  "fedauth",
		"fed_auth": "fedauth",

		"clientcertpath":   "clientcertpath",
		"client_cert_path": "clientcertpath",
	}

	U, _ := net.NewURL(conn.GetURL())
	for key, new_key := range propMapping {
		if val := conn.GetProp(key); val != "" {
			U.SetParam(new_key, val)
		}
	}

	AdAuthStrings := []string{
		// azuread.ActiveDirectoryPassword,
		// azuread.ActiveDirectoryIntegrated,
		azuread.ActiveDirectoryMSI,
		azuread.ActiveDirectoryInteractive,
		azuread.ActiveDirectoryDefault,
		azuread.ActiveDirectoryManagedIdentity,
		azuread.ActiveDirectoryServicePrincipal,
		azuread.ActiveDirectoryAzCli,
		azuread.ActiveDirectoryDeviceCode,
		azuread.ActiveDirectoryApplication,
	}
	if g.In(conn.FedAuth(), AdAuthStrings...) {
		conn.SetProp("driver", "azuresql")

		// // Create a credential for Microsoft Entra ID authentication.
		// // DefaultAzureCredential tries several methods, including using the Azure CLI login.
		// cred, err := azidentity.NewDefaultAzureCredential(nil)
		// if err != nil {
		// 	g.Warn("failed to create default Azure credential: %v", err)
		// } else {
		// 	// Get an access token for the SQL database.
		// 	// The resource scope for Azure SQL is "https://database.windows.net/.default".
		// 	token, err := cred.GetToken(context.Background(), policy.TokenRequestOptions{})
		// 	if err != nil {
		// 		g.Warn("failed to get access token: %v", err)
		// 	} else {
		// 		accessToken := token.Token
		// 		g.Warn("accessToken => %s", accessToken)
		// 	}
		// }
	}

	if certPath := os.Getenv("AZURE_CLIENT_CERTIFICATE_PATH"); certPath != "" {
		// https://github.com/microsoft/go-sqlcmd/blob/main/pkg/sqlcmd/azure_auth.go#L40
		U.SetParam("clientcertpath", certPath)
	}

	if val := conn.GetProp("log"); val != "" {
		mssql.SetLogger(&SqlServerLogger{})
	}

	return U.String()
}

func (conn *MsSQLServerConn) FedAuth() string {
	return conn.GetProp("fedauth", "fed_auth")
}

func (conn *MsSQLServerConn) Connect(timeOut ...int) (err error) {

	// Check if this is a Cloud SQL connection
	if conn.GetProp("gcp_instance") != "" {
		conn.isCloudSQL = true
		err = conn.connectCloudSQL(timeOut...)
	} else {
		err = conn.BaseConn.Connect(timeOut...)
	}
	if err != nil {
		return err
	}

	// get version
	data, _ := conn.Query(`select SERVERPROPERTY('productversion') as short_version, @@version as long_version ` + noDebugKey)
	if conn.Type == dbio.TypeDbSQLServer && len(data.Rows) > 0 {
		versionShortStr := cast.ToString(data.Rows[0][0])
		versionLongStr := cast.ToString(data.Rows[0][1])
		conn.parseVersion(versionShortStr)
		g.Trace("%s version is %s => %d", conn.GetProp("sling_conn_id"), versionShortStr, conn.versionYear)

		if strings.Contains(strings.ToLower(versionLongStr), "sql azure") {
			conn.Type = dbio.TypeDbAzure
		} else if strings.Contains(strings.ToLower(versionLongStr), "azure sql data warehouse") {
			conn.Type = dbio.TypeDbAzureDWH
		}
	}

	// detect Fabric Database (Azure SQL)
	if conn.Type == dbio.TypeDbSQLServer && strings.Contains(conn.GetProp("host"), ".database.fabric.microsoft.com") {
		conn.Type = dbio.TypeDbAzure
	}
	// detect Fabric Warehouse
	if conn.Type == dbio.TypeDbSQLServer && strings.Contains(conn.GetProp("host"), ".datawarehouse.fabric.microsoft.com") {
		conn.Type = dbio.TypeDbFabric
	}

	return nil
}

// connectCloudSQL establishes a connection to Google Cloud SQL SQL Server (non-IAM)
// Note: Cloud SQL for SQL Server does NOT support IAM authentication
// This method provides secure Cloud SQL Proxy connectivity but still requires username/password
func (conn *MsSQLServerConn) connectCloudSQL(timeOut ...int) error {
	ctx := conn.Context().Ctx

	// Build instance connection name: project:region:instance
	gcpProject := conn.GetProp("gcp_project")
	gcpRegion := conn.GetProp("gcp_region")
	gcpInstance := conn.GetProp("gcp_instance")

	if gcpProject == "" || gcpRegion == "" || gcpInstance == "" {
		return g.Error("gcp_project, gcp_region, and gcp_instance are required for Cloud SQL connectivity")
	}

	instanceName := fmt.Sprintf("%s:%s:%s", gcpProject, gcpRegion, gcpInstance)
	g.Debug("Cloud SQL instance connection name: %s", instanceName)

	// Build dialer options (WITHOUT IAM authentication - not supported for SQL Server)
	dialerOpts := []cloudsqlconn.Option{}

	// Support lazy refresh for serverless environments
	if cast.ToBool(conn.GetProp("gcp_lazy_refresh")) {
		g.Trace("enabling lazy refresh mode for Cloud SQL connector")
		dialerOpts = append(dialerOpts, cloudsqlconn.WithLazyRefresh())
	}

	// Support three credential methods: ADC (default), credentials file, or credentials JSON
	if credJSON := conn.GetProp("gcp_key_body"); credJSON != "" {
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsJSON([]byte(credJSON)))
	} else if credsFile := conn.GetProp("gcp_key_file"); credsFile != "" {
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsFile(credsFile))
	} else {
		g.Debug("Using Application Default Credentials (ADC) for Cloud SQL connectivity")
	}

	// Support private IP connections
	defaultDialOpts := []cloudsqlconn.DialOption{}
	if cast.ToBool(conn.GetProp("gcp_use_private_ip")) {
		g.Trace("using private IP for Cloud SQL connection")
		defaultDialOpts = append(defaultDialOpts, cloudsqlconn.WithPrivateIP())
	}
	if len(defaultDialOpts) > 0 {
		dialerOpts = append(dialerOpts, cloudsqlconn.WithDefaultDialOptions(defaultDialOpts...))
	}

	// Generate unique driver name to avoid conflicts with multiple Cloud SQL connections
	driverName := fmt.Sprintf("cloudsql-sqlserver-%d", time.Now().UnixNano())
	g.Trace("registering Cloud SQL SQL Server driver: %s", driverName)

	// Register the Cloud SQL SQL Server driver
	cleanup, err := cloudsqlmssql.RegisterDriver(driverName, dialerOpts...)
	if err != nil {
		return g.Error(err, "could not register Cloud SQL SQL Server driver")
	}

	// Store cleanup function for later use
	conn.cloudSQLCleanup = func() {
		if err := cleanup(); err != nil {
			g.LogError(err, "error during Cloud SQL cleanup")
		}
	}

	// Get connection parameters
	user := conn.GetProp("user")
	password := conn.GetProp("password")
	database := conn.GetProp("database")

	if user == "" {
		conn.cloudSQLCleanup()
		return g.Error("user property is required for Cloud SQL connection")
	}

	if password == "" {
		conn.cloudSQLCleanup()
		return g.Error("password property is required for Cloud SQL SQL Server connection (IAM authentication not supported)")
	}

	if database == "" {
		conn.cloudSQLCleanup()
		return g.Error("database property is required for Cloud SQL connection")
	}

	// Build SQL Server connection string for Cloud SQL
	// Format: sqlserver://user:password@host?database=db&cloudsql=project:region:instance
	dsn := fmt.Sprintf("sqlserver://%s:%s@localhost?database=%s&cloudsql=%s",
		user, password, database, instanceName)

	g.Trace("connecting to Cloud SQL SQL Server instance => %s", dsn)

	// Open the connection using the Cloud SQL driver
	db, err := sqlx.Open(driverName, dsn)
	if err != nil {
		conn.cloudSQLCleanup()
		return g.Error(err, "could not open Cloud SQL SQL Server connection")
	}

	conn.db = db

	// Configure connection pool settings
	maxConns := cast.ToInt(conn.GetProp("max_conns"))
	if maxConns == 0 {
		maxConns = 25
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns / 4) // 25% of max connections
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)

	g.Trace("cloud SQL connection pool configured: max_open=%d, max_idle=%d", maxConns, maxConns/4)

	// Set timeout if provided
	timeout := 15
	if len(timeOut) > 0 && timeOut[0] > 0 {
		timeout = timeOut[0]
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	// Test the connection
	if err = db.PingContext(timeoutCtx); err != nil {
		conn.cloudSQLCleanup()
		errorMsg := err.Error()

		// Provide helpful troubleshooting hints
		hints := []string{
			"Common issues:",
			"1. Service account missing 'Cloud SQL Client' role: gcloud projects add-iam-policy-binding PROJECT --member=serviceAccount:SA@PROJECT.iam.gserviceaccount.com --role=roles/cloudsql.client",
			"2. Database user not created or password incorrect (IAM authentication NOT supported for SQL Server)",
			"3. Network connectivity issue (check gcp_use_private_ip setting and VPC configuration)",
			"4. SQL Server instance not running or connection name incorrect",
		}

		return g.Error("Cloud SQL connection ping failed: %s\n%s", errorMsg, strings.Join(hints, "\n"))
	}

	conn.BaseConn.URL = fmt.Sprintf("sqlserver://%s@cloudsql(%s)/%s", user, instanceName, database)
	conn.Type = dbio.TypeDbSQLServer

	return nil
}

// Close closes the SQL Server connection and cleans up Cloud SQL resources if applicable
func (conn *MsSQLServerConn) Close() error {
	// Cleanup Cloud SQL resources first
	if conn.isCloudSQL {
		g.Trace("closing Cloud SQL connection and cleaning up resources")

		if conn.cloudSQLCleanup != nil {
			conn.cloudSQLCleanup()
			conn.cloudSQLCleanup = nil
		}
	}

	// Call base connection close
	return conn.BaseConn.Close()
}

func (conn *MsSQLServerConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

	table.Columns = data.Columns
	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	for _, index := range table.Indexes(data.Columns) {
		ddl = ddl + ";\n" + index.CreateDDL()
	}

	return ddl, nil
}

// BulkImportFlow bulk import flow
func (conn *MsSQLServerConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	if conn.Type == dbio.TypeDbAzureDWH {
		return conn.CopyViaAzure(tableFName, df)
	}

	return conn.BaseConn.BulkImportFlow(tableFName, df)
}

func (conn *MsSQLServerConn) GetTableColumns(table *Table, fields ...string) (columns iop.Columns, err error) {
	columns, err = conn.BaseConn.GetTableColumns(table, fields...)
	if err != nil {
		// try synonym

		// get database first (synonym could point to other db)
		values := g.M("schema", table.Schema, "table", table.Name)
		data, err1 := conn.SubmitTemplate("single", conn.template.Metadata, "synonym_database", values)
		if err1 != nil {
			return columns, g.Error(err1, "could not get table or synonym database")
		} else if len(data.Rows) == 0 {
			return columns, g.Error("did not find table or synonym: %s", table.FullName())
		}

		synDbName := cast.ToString(data.Records(true)[0]["database_name"])
		conn.SetProp("synonym_database", synDbName)
		conn.SetProp("get_synonym", "true")
		columns, err1 = conn.BaseConn.GetTableColumns(table, fields...)
		if err1 != nil {
			return columns, err1
		} else if len(columns) > 0 {
			err = nil
		}
		conn.SetProp("get_synonym", "false")
		conn.SetProp("synonym_database", "") // clear
	}
	return
}

func (conn *MsSQLServerConn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]interface{}) (data iop.Dataset, err error) {
	if cast.ToBool(conn.GetProp("get_synonym")) && name == "columns" {
		name = "columns_synonym"
		values["base_object_database"] = conn.GetProp("synonym_database")
	}
	return conn.BaseConn.SubmitTemplate(level, templateMap, name, values)
}

// BulkImportStream bulk import stream
func (conn *MsSQLServerConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	conn.Commit() // cannot have transaction lock table

	// return conn.BaseConn.InsertBatchStream(tableFName, ds)
	_, err = exec.LookPath(conn.bcpPath())
	if err != nil {
		g.Debug("bcp not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	} else if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	if fedAuth := conn.GetProp("fed_auth"); g.In(fedAuth, azuread.ActiveDirectoryAzCli) {
		// need to az cli tool
		_, err = exec.LookPath(conn.azCliPath())
		if err != nil {
			g.Warn("unable to use bcp since the Azure CLI tool is not found in path. bcp needs an Access Token obtained via the Azure CLI tool for fed_auth=%s. Using cursor...", fedAuth)
			return conn.BaseConn.InsertBatchStream(tableFName, ds)
		}
	}

	// needs to get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = g.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = g.Error(err, "could not shape stream")
		return
	}

	return conn.BcpImportFileParrallel(tableFName, ds)
}

// BcpImportFileParrallel uses goroutine to import partitioned files
func (conn *MsSQLServerConn) BcpImportFileParrallel(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	fileRowLimit := cast.ToInt(conn.GetProp("FILE_MAX_ROWS"))
	if fileRowLimit == 0 {
		fileRowLimit = 200000
	}

	delimiterRep := `$~d$~`
	quoteRep := `$~q$~`
	newlRep := `$~n$~`
	carrRep := `$~r$~`
	emptyRep := `$~e$~`
	postUpdateCol := map[int]uint64{}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	// https://stackoverflow.com/questions/782353/sql-server-bulk-insert-of-csv-file-with-inconsistent-quotes
	// reduces performance by ~25%, but is correct, and still 10x faster then insert into with batch VALUES
	// If we use the parallel way, we gain back the speed by using more power. We also loose order.
	transf := func(row []interface{}) (nRow []interface{}) {
		nRow = row
		for i, val := range row {

			switch v := val.(type) {
			case string:
				nRow[i] = strings.ReplaceAll(
					val.(string), ",", delimiterRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), `"`, quoteRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\r", carrRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\n", newlRep,
				)
				// bcp treats empty space as null
				if !ds.Sp.Config.EmptyAsNull && v == "" {
					nRow[i] = emptyRep
				}
				if nRow[i].(string) != val.(string) {
					postUpdateCol[i]++
				}
			default:
				_ = v
			}
		}
		return
	}

	doImport := func(tableFName string, filePath string) {
		defer ds.Context.Wg.Write.Done()

		// delete csv
		defer func() { env.RemoveLocalTempFile(filePath) }()

		_, err := conn.BcpImportFile(tableFName, filePath)
		ds.Context.CaptureErr(err)
	}

	for batch := range ds.BatchChan {

		if batch.ColumnsChanged() || batch.IsFirst() {
			ds.Context.Lock()
			columns, err := conn.GetColumns(tableFName, batch.Columns.Names()...)
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}
			ds.Context.Unlock()

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		ds.Pause()
		batch.AddTransform(transf)
		ds.Unpause()

		// Write the ds to a temp file

		filePath := path.Join(env.GetTempFolder(), g.NewTsID(g.F("sqlserver.%s", env.CleanTableName(tableFName)))+g.F("%d.csv", len(ds.Batches)))
		csvRowCnt, err := writeCsvWithoutQuotes(filePath, batch, fileRowLimit)

		if err != nil {
			os.Remove(filePath)
			err = g.Error(err, "Error csv.WriteStream(ds) to "+filePath)
			ds.Context.CaptureErr(err)
			ds.Context.Cancel()
			return 0, g.Error(err)
		} else if csvRowCnt == 0 {
			// no data from source
			os.Remove(filePath)
			continue
		}

		ds.Context.Wg.Write.Add()
		go doImport(tableFName, filePath)
	}

	ds.Context.Wg.Write.Wait()

	// post process if strings have been modified
	if len(postUpdateCol) > 0 && ds.Err() == nil {
		setCols := []string{}
		for i, col := range ds.Columns {
			if _, ok := postUpdateCol[i]; !ok {
				continue
			}

			replExpr1 := g.R(
				`REPLACE(CONVERT(NVARCHAR(MAX), {field}), '{delimiterRep}', '{delimiter}')`,
				"field", col.Name,
				"delimiterRep", delimiterRep,
				"delimiter", ",",
			)
			replExpr2 := g.R(
				`REPLACE({replExpr}, '{quoteRep}', '{quote}')`,
				"replExpr", replExpr1,
				"quoteRep", quoteRep,
				"quote", `"`,
			)
			replExpr3 := g.R(
				`REPLACE({replExpr}, '{placeholder}', {newVal})`,
				"replExpr", replExpr2,
				"placeholder", carrRep,
				"newVal", `CHAR(13)`,
			)
			replExpr4 := g.R(
				`REPLACE({replExpr}, '{placeholder}', {newVal})`,
				"replExpr", replExpr3,
				"placeholder", newlRep,
				"newVal", `CHAR(10)`,
			)
			replExpr5 := g.R(
				`REPLACE({replExpr}, '{placeholder}', {newVal})`,
				"replExpr", replExpr4,
				"placeholder", emptyRep,
				"newVal", `''`,
			)
			setCols = append(
				setCols, fmt.Sprintf(`%s = %s`, col.Name, replExpr5),
			)
		}

		// do update statement if needed
		if len(setCols) > 0 {
			setColsStr := strings.Join(setCols, ", ")
			sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr) + noDebugKey
			_, err = conn.Exec(sql)
			if err != nil {
				err = g.Error(err, "could not apply post update query")
				return
			}
		}
	}

	return ds.Count, ds.Err()
}

func (conn *MsSQLServerConn) bcpPath() string {
	if val := conn.GetProp("bcp_path"); val != "" {
		return val
	}
	return "bcp"
}

func (conn *MsSQLServerConn) azCliPath() string {
	if val := conn.GetProp("az_path"); val != "" {
		return val
	}
	return "az"
}

// BcpImportFile Import using bcp tool
// https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15
// bcp dbo.test1 in '/tmp/LargeDataset.csv' -S tcp:sqlserver.host,51433 -d master -U sa -P 'password' -c -t ',' -b 5000
// Limitation: if comma or delimite is in field, it will error.
// need to use delimiter not in field, or do some other transformation
func (conn *MsSQLServerConn) BcpImportFile(tableFName, filePath string) (count uint64, err error) {
	var stderr, stdout bytes.Buffer

	connURL := conn.URL
	if su := conn.GetProp("ssh_url"); su != "" {
		connURL = su // use ssh url if specified
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		return
	}

	// get version
	version := 14
	versionOut, err := exec.Command(conn.bcpPath(), "-v").Output()
	if err != nil {
		g.Debug("could not get bcp version running `bcp -v` (%s)", err.Error())
		version = 0
	}
	regex := *regexp.MustCompile(`Version: (\d+)`)
	verRes := regex.FindStringSubmatch(string(versionOut))
	if len(verRes) == 2 {
		version = cast.ToInt(verRes[1])
	}
	g.Debug("bcp version is %d", version)

	// Import to Database
	batchSize := 50000
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	instance := strings.ReplaceAll(url.Path, "/", "")
	database := url.Query().Get("database")
	user := url.User.Username()

	if port == "" {
		port = cast.ToString(conn.GetType().DefPort())
	}
	hostPort := fmt.Sprintf("tcp:%s,%s", host, port)
	if instance != "" {
		hostPort = g.F("%s\\%s", hostPort, instance)
	}
	errPath := "/dev/stderr"
	if runtime.GOOS == "windows" || true {
		errPath = path.Join(env.GetTempFolder(), g.NewTsID(g.F("sqlserver.%s", env.CleanTableName(tableFName)))+".error")
		defer os.Remove(errPath)
	}

	bcpArgs := []string{
		strings.ReplaceAll(tableFName, `"`, ""),
		"in", filePath,
		"-S", hostPort,
		"-d", database,
		"-t", ",",
		"-m", "1",
		"-w",
		"-q",
		"-b", cast.ToString(batchSize),
		"-F", "2",
		"-e", errPath,
	}

	if bcpAuthString := conn.GetProp("bcp_auth_string"); bcpAuthString != "" {
		bcpAuthParts := []string{}
		err = g.Unmarshal(bcpAuthString, &bcpAuthParts)
		if err != nil {
			err = g.Error("could not parse property `bcp_auth_string` (%s). Is an array of strings provided?", err.Error())
			return
		}
		bcpArgs = append(bcpArgs, bcpAuthParts...)
	} else if fedAuth := conn.GetProp("fed_auth"); g.In(fedAuth, azuread.ActiveDirectoryAzCli) {
		azCliTokenResource := conn.GetProp("bcp_azure_token_resource")
		if azCliTokenResource == "" {
			azCliTokenResource = "https://database.windows.net"
		}
		azCliArgs := []string{"account", "get-access-token", "--resource", azCliTokenResource, "--query", "accessToken", "--output", "tsv"}

		// need to run
		// az account get-access-token --resource https://database.windows.net --query accessToken --output tsv
		azCmd := exec.Command(conn.azCliPath(), azCliArgs...)
		if err != nil {
			return 0, g.Error(err, "could not create az cli command")
		}

		g.Debug(conn.azCliPath() + " " + strings.Join(azCliArgs, ` `))
		output, err := azCmd.Output()
		if err != nil {
			g.Warn("could not obtain access token with the Azure CLI tool...")
			return 0, g.Error(err, output)
		}

		token := strings.TrimSpace(string(output))

		// convert token to UTF-16LE bytes
		utf16Token := utf16.Encode([]rune(token))
		var leBytes []byte
		for _, u := range utf16Token {
			leBytes = append(leBytes, byte(u), byte(u>>8))
		}

		// now write the UTF-16LE token bytes to a temp file
		tokenFilePath := path.Join(env.GetTempFolder(), g.NewTsID("sqlserver.token")+".txt")
		err = os.WriteFile(tokenFilePath, leBytes, 0600)
		if err != nil {
			return 0, g.Error(err, "could not write token to temp file")
		}
		defer os.Remove(tokenFilePath)

		// add BCP authentication args for Azure AD with token file
		bcpArgs = append(bcpArgs, "-G", "-P", tokenFilePath)
	} else {
		if g.In(conn.GetProp("authenticator"), "winsspi") || conn.isTrusted() {
			bcpArgs = append(bcpArgs, "-T")
		} else {
			bcpArgs = append(bcpArgs, "-U", user, "-P", password)
		}

		if cast.ToBool(conn.GetProp("bcp_entra_auth")) {
			bcpArgs = append(bcpArgs, "-G")
		}
	}

	if bcpExtraArgs := conn.GetProp("bcp_extra_args"); bcpExtraArgs != "" {
		bcpExtraParts := []string{}
		err = g.Unmarshal(bcpExtraArgs, &bcpExtraParts)
		if err != nil {
			err = g.Error("could not parse property `bcp_extra_args` (%s). Is an array of strings provided?", err.Error())
			return
		}
		bcpArgs = append(bcpArgs, bcpExtraParts...)
	}

retry:
	proc := exec.Command(conn.bcpPath(), bcpArgs...)
	proc.Stderr = &stderr
	proc.Stdout = &stdout

	if version <= 14 {
		g.Warn("bcp version %d is old. This may give issues with sling, consider upgrading.", version)
	} else if version >= 18 {
		// add u for version 18
		proc.Args = append(proc.Args, "-u")
	}

	// build cmdStr
	args := lo.Map(proc.Args, func(v string, i int) string {
		if !g.In(v, "in", "-S", "-d", "-U", "-P", "-t", "-u", "-m", "-c", "-q", "-b", "-F", "-e", conn.bcpPath()) {
			v = strings.ReplaceAll(v, hostPort, "****")
			if password != "" {
				v = strings.ReplaceAll(v, password, "****")
			}
			return `'` + strings.ReplaceAll(v, `'`, `''`) + `'`
		}
		return v
	})
	cmdStr := strings.Join(args, ` `)
	g.Debug(cmdStr)

	// run and wait for finish
	err = proc.Run()

	// get count
	regex = *regexp.MustCompile(`(?s)(\d+) rows copied.`)
	res := regex.FindStringSubmatch(stdout.String())
	if len(res) == 2 {
		count = cast.ToUint64(res[1])
	}

	if err != nil {
		if strings.Contains(err.Error(), "text file busy") {
			g.Warn("could not start bcp (%s), retrying...", err.Error())
			time.Sleep(1 * time.Second)
			goto retry
		}

		errOut := stderr.String()
		if errPath != "/dev/stderr" {
			errOutB, _ := os.ReadFile(errPath)
			errOut = string(errOutB)
		}

		err = g.Error(
			err,
			fmt.Sprintf(
				"SQL Server BCP Import Command -> %s\nSQL Server BCP Import Error  -> %s\n%s",
				cmdStr, errOut, stdout.String(),
			),
		)
		return
	}

	return
}

// GenerateInsertStatement for transaction insert. Does not work with Fabric
// func (conn *MsSQLServerConn) GenerateInsertStatement(tableName string, cols iop.Columns, numRows int) string {

// 	fields := cols.Names()
// 	// qFields := conn.Type.QuoteNames(fields...) // quoted fields

// 	statement := mssql.CopyIn(tableName, mssql.BulkOptions{
// 		Tablock:      true, // For performance
// 		RowsPerBatch: 50000,
// 		KeepNulls:    true,
// 		// CheckConstraints: true,
// 		// FireTriggers:     true,
// 	}, fields...)

// 	g.Trace("insert statement: %s", statement)
// 	return statement
// }

// BcpExport exports data to datastream
func (conn *MsSQLServerConn) BcpExport() (err error) {
	return
}

// sqlcmd -S localhost -d BcpSampleDB -U sa -P <your_password> -I -Q "select * from TestEmployees;"

// EXPORT
// bcp TestEmployees out ~/test_export.txt -S localhost -U sa -P <your_password> -d BcpSampleDB -c -t ','
// bcp dbo.test1 out ~/test_export.csv -S server.database.windows.net -U user -P 'password' -d db1 -c -t ',' -q -b 10000

// importing from blob
// https://azure.microsoft.com/en-us/updates/files-from-azure-blob-storage-to-sql-database/

//UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/

// GenerateMergeSQL generates the upsert SQL
func (conn *MsSQLServerConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateMergeExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	merge into {tgt_table} tgt
	using (select *	from {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) values  ({src_fields});
	`

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return
}

// CopyViaAzure uses the Azure DWH COPY INTO Table command
func (conn *MsSQLServerConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to azure dwh from container")
		return
	}

	azPath := fmt.Sprintf(
		"https://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		tempCloudStorageFolder,
		tableFName,
	)

	azFs, err := filesys.NewFileSysClient(dbio.TypeFileAzure, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for S3")
		return
	}

	err = filesys.Delete(azFs, azPath+"*")
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+azPath)
	}

	// defer func() { azFs.Delete(azPath + "*") }() // cleanup

	fileReadyChn := make(chan filesys.FileReady, 10000)
	go func() {
		var bw int64
		bw, err = azFs.WriteDataflowReady(df, azPath, fileReadyChn, iop.DefaultStreamConfig())
		g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

		if err != nil {
			err = g.Error(err, "Error writing dataflow to azure blob: "+azPath)
			return
		}
	}()

	g.Info("writing to azure container for import")

	doCopy := func(file filesys.FileReady) {
		defer df.Context.Wg.Write.Done()
		cnt, err := conn.CopyFromAzure(tableFName, file.Node.URI)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "could not copy to azure dwh"))
		} else {
			count += cnt
		}
	}

	for file := range fileReadyChn {
		df.Context.Wg.Write.Add()
		go doCopy(file)
	}

	df.Context.Wg.Write.Wait()
	if df.Err() != nil {
		err = g.Error(df.Err())
	}

	return df.Count(), err
}

func (conn *MsSQLServerConn) isTrusted() bool {
	return strings.Contains(conn.ConnString(), "TrustedConnection")
}

// CopyFromAzure uses the COPY INTO Table command from Azure
// https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest
func (conn *MsSQLServerConn) CopyFromAzure(tableFName, azPath string) (count uint64, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = g.Error("Need to set 'AZURE_SAS_SVC_URL' to copy to azure dwh from container")
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
	azToken := azSasURLArr[1]

	sql := g.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
		"date_format", "ymd",
	)

	conn.SetProp("azToken", azToken) // to not log it in debug logging

	g.Info("copying into azure DWH")
	g.Debug("url: " + azPath)
	_, err = conn.Exec(sql)
	if err != nil {
		conn.SetProp("azToken", azToken)
		return 0, g.Error(err, "SQL Error:\n"+env.Clean(conn.Props(), sql))
	}

	return 0, nil
}

// writeCsvWithoutQuotes writes a CSV file without quotes
// It takes the following parameters:
//   - path: the file path where the CSV will be written
//   - batch: an iop.Batch containing the data to be written
//   - limit: the maximum number of rows to write (0 for no limit)
//
// The function returns:
//   - cnt: the number of rows written
//   - err: any error encountered during the process
//
// The function performs the following steps:
// 1. Creates a new file at the specified path
// 2. Sets up a UTF-16LE encoder and writer
// 3. Writes the UTF-16LE Byte Order Mark (BOM)
// 4. Writes the header row using column names from the batch
// 5. Iterates through the batch rows, writing each as a CSV line
// 6. Handles data type conversions using a Sp (StreamProcessor) object
// 7. Uses platform-specific newline characters
// 8. Writes data without surrounding quotes, separating fields with commas
//
// Note: This function is specifically designed for SQL Server bulk import
// operations, which require UTF-16LE encoding and no quoting of fields.
func writeCsvWithoutQuotes(path string, batch *iop.Batch, limit int) (cnt uint64, err error) {
	file, err := os.Create(path)
	if err != nil {
		return cnt, g.Error(err, "could not create file")
	}
	defer file.Close()

	// Create a UTF-16LE encoder
	encoder := unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder()

	// Create a writer that transforms UTF-8 to UTF-16LE
	writer := transform.NewWriter(file, encoder)
	defer writer.Close()

	// Write the BOM (Byte Order Mark) for UTF-16LE
	_, err = writer.Write([]byte{0xFF, 0xFE})
	if err != nil {
		return cnt, g.Error(err, "could not write BOM to file")
	}

	fields := batch.Columns.Names()

	newLine := "\n"
	if runtime.GOOS == "windows" {
		newLine = "\r\n"
	}

	// Write header
	_, err = writer.Write([]byte(strings.Join(fields, ",") + newLine))
	if err != nil {
		return cnt, g.Error(err, "could not write header to file")
	}

	Sp := batch.Ds().Sp
	timestampLayout := dbio.TypeDbSQLServer.GetTemplateValue("variable.timestamp_layout")
	Sp.SetConfig(map[string]string{"datetime_format": timestampLayout})

	for row0 := range batch.Rows {
		cnt++
		row := make([]string, len(row0))
		for i, val := range row0 {
			row[i] = Sp.CastToStringCSV(i, val, batch.Columns[i].Type)
		}

		// Write row
		_, err = writer.Write([]byte(strings.Join(row, ",") + newLine))
		if err != nil {
			return cnt, g.Error(err, "could not write row to file")
		}

		if batch.Count == int64(limit) {
			batch.Close()
			break
		}
	}

	return cnt, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *MsSQLServerConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)
	srcDbType := strings.ToLower(srcCol.DbType)
	tgtDbType := strings.ToLower(tgtCol.DbType)

	switch {
	// maintain lower case if inserting from uniqueidentifier into nvarchar
	case srcDbType == "uniqueidentifier" && tgtDbType == "nvarchar":
		selectStr = g.F("cast(%s as nvarchar(max))", qName)
	case srcCol.IsString() && tgtCol.IsInteger():
		// assume bool, convert from true/false to 1/0
		sql := `case when {col} = 'true' then 1 when {col} = 'false' then 0 else cast({col} as bigint) end`
		selectStr = g.R(sql, "col", qName)
	default:
		selectStr = qName
	}

	return selectStr
}
