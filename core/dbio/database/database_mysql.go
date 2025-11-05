package database

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	cloudsqlmysql "cloud.google.com/go/cloudsqlconn/mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/xo/dburl"
)

// MySQLConn is a MySQL or MariaDB connection
type MySQLConn struct {
	BaseConn
	URL             string
	isCloudSQL      bool
	cloudSQLCleanup func()
}

// Init initiates the object
func (conn *MySQLConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbMySQL
	conn.BaseConn.defaultPort = 3306

	if strings.HasPrefix(conn.URL, "mariadb://") {
		conn.BaseConn.Type = dbio.TypeDbMariaDB
	}

	// Turn off Bulk export for now
	// the LoadDataOutFile needs special circumstances
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	// InsertBatchStream is faster than LoadDataInFile
	if conn.BaseConn.GetProp("allow_bulk_import") == "" {
		conn.BaseConn.SetProp("allow_bulk_import", "false")
	}

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *MySQLConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}

	connURL = strings.Replace(connURL, "mariadb://", "mysql://", 1)
	u, err := dburl.Parse(connURL)
	if err != nil {
		g.LogError(err, "could not parse MySQL URL")
		return connURL
	}

	// param keys from https://github.com/go-sql-driver/mysql?tab=readme-ov-file#parameters
	paramKeyMapping := map[string]string{
		"allow_all_files":             "allowAllFiles",
		"allow_cleartext_passwords":   "allowCleartextPasswords",
		"allow_fallback_to_plaintext": "allowFallbackToPlaintext",
		"allow_native_passwords":      "allowNativePasswords",
		"allow_old_passwords":         "allowOldPasswords",
		"charset":                     "charset",
		"check_conn_liveness":         "checkConnLiveness",
		"collation":                   "collation",
		"client_found_rows":           "clientFoundRows",
		"columns_with_alias":          "columnsWithAlias",
		"interpolate_params":          "interpolateParams",
		"loc":                         "loc",
		"time_truncate":               "timeTruncate",
		"max_allowed_packet":          "maxAllowedPacket",
		"multi_statements":            "multiStatements",
		"parse_time":                  "parseTime",
		"read_timeout":                "readTimeout",
		"reject_read_only":            "rejectReadOnly",
		"server_pub_key":              "serverPubKey",
		"timeout":                     "timeout",
		"tls":                         "tls",
		"write_timeout":               "writeTimeout",
		"connection_attributes":       "connectionAttributes",
	}

	query := u.Query()
	for key, libKey := range paramKeyMapping {
		if val := conn.GetProp(key); val != "" {
			query.Set(libKey, val)
		}
		if libKey != key {
			if val := conn.GetProp(libKey); val != "" {
				query.Set(libKey, val)
			}
		}
	}

	// make tls
	if query.Get("tls") == "custom" {
		query.Set("tls", conn.GetProp("sling_conn_id"))
	}

	// reconstruct the url
	u.RawQuery = query.Encode()
	u, err = dburl.Parse(u.String())
	if err != nil {
		g.LogError(err, "could not parse MySQL URL")
		return connURL
	}

	return u.DSN
}

func (conn *MySQLConn) Connect(timeOut ...int) (err error) {

	// Check if this is a Cloud SQL connection with IAM auth
	if conn.GetProp("gcp_instance") != "" && conn.GetProp("gcp_use_iam_auth") == "true" {
		conn.isCloudSQL = true
		g.Trace("Detected Cloud SQL connection with IAM authentication")
		return conn.connectCloudSQL(timeOut...)
	}

	// register tls
	tlsConfig, err := conn.makeTlsConfig()
	if err != nil {
		return g.Error(err, "could not register tls")
	} else if tlsConfig != nil {
		mysql.RegisterTLSConfig(conn.GetProp("sling_conn_id"), tlsConfig)
	}

	return conn.BaseConn.Connect(timeOut...)
}

// connectCloudSQL establishes a connection to Google Cloud SQL MySQL using IAM authentication
func (conn *MySQLConn) connectCloudSQL(timeOut ...int) error {
	ctx := conn.Context().Ctx

	// Build instance connection name: project:region:instance
	gcpProject := conn.GetProp("gcp_project")
	gcpRegion := conn.GetProp("gcp_region")
	gcpInstance := conn.GetProp("gcp_instance")

	if gcpProject == "" || gcpRegion == "" || gcpInstance == "" {
		return g.Error("gcp_project, gcp_region, and gcp_instance are required for Cloud SQL IAM authentication")
	}

	instanceName := fmt.Sprintf("%s:%s:%s", gcpProject, gcpRegion, gcpInstance)
	g.Debug("Cloud SQL instance connection name: %s", instanceName)

	// Build dialer options with IAM authentication
	dialerOpts := []cloudsqlconn.Option{
		cloudsqlconn.WithIAMAuthN(),
	}

	// Support lazy refresh for serverless environments
	if cast.ToBool(conn.GetProp("gcp_lazy_refresh")) {
		g.Trace("Enabling lazy refresh mode for Cloud SQL connector")
		dialerOpts = append(dialerOpts, cloudsqlconn.WithLazyRefresh())
	}

	// Support three credential methods: ADC (default), credentials file, or credentials JSON
	if credJSON := conn.GetProp("gcp_key_body"); credJSON != "" {
		g.Trace("Using explicit GCP credentials from JSON string")
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsJSON([]byte(credJSON)))
	} else if credsFile := conn.GetProp("gcp_key_file"); credsFile != "" {
		g.Trace("Using explicit GCP credentials from file: %s", credsFile)
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsFile(credsFile))
	} else {
		g.Trace("Using Application Default Credentials (ADC) for Cloud SQL authentication")
	}

	// Support private IP connections
	defaultDialOpts := []cloudsqlconn.DialOption{}
	if cast.ToBool(conn.GetProp("gcp_use_private_ip")) {
		g.Trace("Using private IP for Cloud SQL connection")
		defaultDialOpts = append(defaultDialOpts, cloudsqlconn.WithPrivateIP())
	}
	if len(defaultDialOpts) > 0 {
		dialerOpts = append(dialerOpts, cloudsqlconn.WithDefaultDialOptions(defaultDialOpts...))
	}

	// Generate unique driver name to avoid conflicts with multiple Cloud SQL connections
	driverName := fmt.Sprintf("cloudsql-mysql-%d", time.Now().UnixNano())
	g.Trace("Registering Cloud SQL MySQL driver: %s", driverName)

	// Register the Cloud SQL MySQL driver
	cleanup, err := cloudsqlmysql.RegisterDriver(driverName, dialerOpts...)
	if err != nil {
		return g.Error(err, "could not register Cloud SQL MySQL driver")
	}

	// Store cleanup function for later use
	conn.cloudSQLCleanup = func() {
		if err := cleanup(); err != nil {
			g.LogError(err, "error during Cloud SQL cleanup")
		}
	}

	// Format the IAM username (MySQL uses short format without domain)
	user := conn.formatIAMUsername()
	database := conn.GetProp("database")

	if database == "" {
		conn.cloudSQLCleanup()
		return g.Error("database property is required for Cloud SQL connection")
	}

	// Build MySQL DSN for Cloud SQL
	// Format: user@driver-name(instance-connection-name)/database?parseTime=true
	dsn := fmt.Sprintf("%s@%s(%s)/%s?parseTime=true",
		user, driverName, instanceName, database)

	g.Debug("Connecting to Cloud SQL MySQL instance => %s", dsn)

	// Open the connection using the Cloud SQL driver
	db, err := sqlx.Open(driverName, dsn)
	if err != nil {
		conn.cloudSQLCleanup()
		return g.Error(err, "could not open Cloud SQL MySQL connection")
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

	g.Trace("Cloud SQL connection pool configured: max_open=%d, max_idle=%d", maxConns, maxConns/4)

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
			"1. IAM authentication not enabled on instance: gcloud sql instances patch INSTANCE --database-flags=cloudsql_iam_authentication=on",
			"2. Service account missing 'Cloud SQL Client' role: gcloud projects add-iam-policy-binding PROJECT --member=serviceAccount:SA@PROJECT.iam.gserviceaccount.com --role=roles/cloudsql.client",
			"3. Database user not created with IAM auth: CREATE USER 'username'@'%' IDENTIFIED WITH authentication_cloudsql_iam;",
			"4. Username format incorrect (should be short format, e.g., 'my-sa' not 'my-sa@project.iam')",
			"5. Network connectivity issue (check gcp_use_private_ip setting and VPC configuration)",
		}

		return g.Error("Cloud SQL connection test failed: %s\n%s", errorMsg, strings.Join(hints, "\n"))
	}

	g.Info("Successfully connected to Cloud SQL MySQL instance: %s", instanceName)
	conn.BaseConn.URL = fmt.Sprintf("mysql://%s@cloudsql(%s)/%s", user, instanceName, database)
	conn.Type = dbio.TypeDbMySQL

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.postConnect()

	return nil
}

// formatIAMUsername formats the username for MySQL Cloud SQL IAM authentication
// MySQL requires a short username format (max 32 chars) without domain suffixes
// Examples:
//   - my-sa@my-project.iam.gserviceaccount.com -> my-sa
//   - my-sa@my-project.iam -> my-sa
//   - user@gmail.com -> user
func (conn *MySQLConn) formatIAMUsername() string {
	user := conn.GetProp("user")

	if user == "" {
		g.Warn("Cloud SQL username not specified, using 'root'")
		return "root"
	}

	originalUser := user

	// Remove .gserviceaccount.com suffix if present
	user = strings.TrimSuffix(user, ".gserviceaccount.com")

	// Remove .iam suffix if present
	user = strings.TrimSuffix(user, ".iam")

	// Extract just the username part (everything before @)
	if idx := strings.Index(user, "@"); idx > 0 {
		user = user[:idx]
	}

	// MySQL has a 32-character username limit
	if len(user) > 32 {
		g.Warn("Username exceeds MySQL's 32-character limit: %s (length: %d)", user, len(user))
	}

	if user != originalUser {
		g.Trace("Formatted IAM username: %s -> %s", originalUser, user)
	}

	return user
}

// Close closes the MySQL connection and cleans up Cloud SQL resources if applicable
func (conn *MySQLConn) Close() error {
	// Cleanup Cloud SQL resources first
	if conn.isCloudSQL {
		g.Trace("Closing Cloud SQL connection and cleaning up resources")

		if conn.cloudSQLCleanup != nil {
			conn.cloudSQLCleanup()
			conn.cloudSQLCleanup = nil
		}
	}

	// Call base connection close
	return conn.BaseConn.Close()
}

func (conn *MySQLConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

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

// BulkInsert
// Common Error: ERROR 3948 (42000) at line 1: Loading local data is disabled; this must be enabled on both the client and server sides
// Need to enable on serer side: https://stackoverflow.com/a/60027776
// mysql server needs to be launched with '--local-infile=1' flag
// mysql --local-infile=1 -h {host} -P {port} -u {user} -p{password} mysql -e "LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' IGNORE 1 LINES;"

// BulkExportStream bulk Export
func (conn *MySQLConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		g.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.StreamRows(table.Select(), g.M("columns", table.Columns))
	} else if runtime.GOOS == "windows" {
		return conn.BaseConn.StreamRows(table.Select(), g.M("columns", table.Columns))
	}

	if conn.BaseConn.GetProp("allow_bulk_export") != "true" {
		return conn.BaseConn.StreamRows(table.Select(), g.M("columns", table.Columns))
	}

	copyCtx := g.NewContext(conn.Context().Ctx)
	stdOutReader, err := conn.LoadDataOutFile(copyCtx, table.Select())
	if err != nil {
		return ds, err
	}

	csv := iop.CSV{Reader: stdOutReader}
	ds, err = csv.ReadStreamContext(copyCtx.Ctx)

	return ds, err
}

// BulkImportStream bulk import stream
func (conn *MySQLConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("mysql")
	if err != nil {
		g.Trace("mysql not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	} else if conn.GetProp("allow_bulk_import") != "true" {
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
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

	return conn.LoadDataInFile(tableFName, ds)
}

// LoadDataOutFile Bulk Export
// Possible error: ERROR 1227 (42000) at line 1: Access denied; you need (at least one of) the FILE privilege(s) for this operation
// File privilege needs to be granted to user
// also the --secure-file-priv option needs to be set properly for it to work.
// https://stackoverflow.com/questions/9819271/why-is-mysql-innodb-insert-so-slow to improve innodb insert speed
func (conn *MySQLConn) LoadDataOutFile(ctx *g.Context, sql string) (stdOutReader io.Reader, err error) {
	var stderr bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")
	query := g.R(
		`{sql} INTO OUTFILE '/dev/stdout'
		FIELDS ENCLOSED BY '"' TERMINATED BY ',' ESCAPED BY '"'
		LINES TERMINATED BY '\r\n'`,
		"sql", sql,
	)
	proc := exec.Command(
		"mysql",
		// "--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", strings.ReplaceAll(query, "\n", " "),
	)
	proc.Stderr = &stderr
	stdOutReader, err = proc.StdoutPipe()

	go func() {
		err := proc.Run()
		if err != nil {
			// bytes, _ := proc.CombinedOutput()
			cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
			err = g.Error(
				err,
				fmt.Sprintf(
					"MySQL Export Command -> %s\nMySQL Export Error  -> %s",
					cmdStr, stderr.String(),
				),
			)
			ctx.CaptureErr(err)
			g.LogError(err, "could not export from MySQL")
		}
	}()

	return stdOutReader, err
}

// LoadDataInFile Bulk Import
func (conn *MySQLConn) LoadDataInFile(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr bytes.Buffer

	connURL := conn.URL
	if su := conn.GetProp("ssh_url"); su != "" {
		connURL = su // use ssh url if specified
	}

	url, err := dburl.Parse(connURL)
	if err != nil {
		err = g.Error(err, "Error dburl.Parse(conn.URL)")
		return
	}

	password, _ := url.User.Password()
	host := strings.ReplaceAll(url.Host, ":"+url.Port(), "")
	database := strings.ReplaceAll(url.Path, "/", "")

	loadQuery := g.R(`LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE {table} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"' IGNORE 1 LINES;`, "table", tableFName)
	proc := exec.Command(
		"mysql",
		"--local-infile=1",
		"-h", host,
		"-P", url.Port(),
		"-u", url.User.Username(),
		"-p"+password,
		database,
		"-e", loadQuery,
	)

	proc.Stderr = &stderr
	proc.Stdin = ds.NewCsvReader(iop.DefaultStreamConfig())

	err = proc.Run()
	if err != nil {
		cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****")
		err = g.Error(
			err,
			fmt.Sprintf(
				"MySQL Import Command -> %s\nMySQL Import Error  -> %s",
				cmdStr, stderr.String(),
			),
		)
		return ds.Count, err
	}

	return ds.Count, nil
}

// UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/
// GenerateMergeSQL generates the upsert SQL
func (conn *MySQLConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateMergeExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	srcT, err := ParseTableName(srcTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not generate parse srcTable")
		return
	}

	tgtT, err := ParseTableName(tgtTable, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not generate parse tgtTable")
		return
	}

	// replace src & tgt to make compatible to MariaDB
	// see https://github.com/slingdata-io/sling-cli/issues/135
	upsertMap["src_tgt_pk_equal"] = strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "src.", srcT.NameQ()+".")
	upsertMap["src_tgt_pk_equal"] = strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", tgtT.NameQ()+".")

	sqlTemplate := `
	delete from {tgt_table}
	where exists (
			select 1
			from {src_table}
			where {src_tgt_pk_equal}
	)
	;

	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from {src_table} src
	`

	sql = g.R(
		sqlTemplate,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(upsertMap["src_tgt_pk_equal"], "tgt.", "upd."),
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)

	return
}
