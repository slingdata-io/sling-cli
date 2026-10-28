package database

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"cloud.google.com/go/cloudsqlconn/postgres/pgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq/oid"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

// PostgresConn is a Postgres connection
type PostgresConn struct {
	BaseConn
	URL             string
	isCloudSQL      bool
	cloudSQLCleanup func()
	cloudSQLConn    *pgx.Conn
}

// Init initiates the object
func (conn *PostgresConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbPostgres
	conn.BaseConn.defaultPort = 5432

	// Turn off Bulk export for now
	// the CopyToStdout function frequently produces error `read |0: file already closed`
	// also is slower than just select?
	conn.BaseConn.SetProp("allow_bulk_export", "false")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Connect connects to the database
func (conn *PostgresConn) Connect(timeOut ...int) error {
	// Check if this is a Cloud SQL connection with IAM auth
	if conn.GetProp("gcp_instance") != "" {
		conn.isCloudSQL = true
		g.Trace("detected Cloud SQL connection with IAM authentication")
		return conn.connectCloudSQL(timeOut...)
	}

	// Standard Postgres connection
	err := conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}

	// Set role if provided
	if val := conn.GetProp("role"); val != "" {
		_, err = conn.Exec("SET ROLE " + val)
		if err != nil {
			return g.Error(err, "could not set role")
		}
	}

	return nil
}

// connectCloudSQL connects to Google Cloud SQL using IAM authentication
func (conn *PostgresConn) connectCloudSQL(timeOut ...int) error {
	ctx := conn.Context().Ctx

	// Build instance connection name: project:region:instance
	instanceName := fmt.Sprintf("%s:%s:%s",
		conn.GetProp("gcp_project"),
		conn.GetProp("gcp_region"),
		conn.GetProp("gcp_instance"),
	)

	g.Trace("connecting to Cloud SQL instance: %s", instanceName)

	// Build dialer options with IAM authentication
	dialerOpts := []cloudsqlconn.Option{
		cloudsqlconn.WithIAMAuthN(),
	}

	// Check for serverless environment (lazy refresh)
	if cast.ToBool(conn.GetProp("gcp_lazy_refresh")) {
		g.Trace("enabling lazy refresh for serverless environment")
		dialerOpts = append(dialerOpts, cloudsqlconn.WithLazyRefresh())
	}

	// Handle credential options (in priority order)
	if credJSON := conn.GetProp("gcp_key_body"); credJSON != "" {
		// Detect and decode base64 if necessary
		decodedCredJSON, err := iop.DecodeJSONIfBase64(credJSON)
		if err != nil {
			return g.Error(err, "could not decode GCP credentials")
		}
		g.Trace("using explicit GCP credentials from JSON content")
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsJSON([]byte(decodedCredJSON)))
	} else if credsFile := conn.GetProp("gcp_key_file"); credsFile != "" {
		g.Trace("using explicit GCP credentials file: %s", credsFile)
		dialerOpts = append(dialerOpts, cloudsqlconn.WithCredentialsFile(credsFile))
	} else {
		g.Trace("using Application Default Credentials (ADC) for Cloud SQL")
	}

	// Build default dial options
	var defaultDialOpts []cloudsqlconn.DialOption
	if conn.GetProp("gcp_use_private_ip") == "true" {
		g.Trace("configuring private IP for Cloud SQL connection")
		defaultDialOpts = append(defaultDialOpts, cloudsqlconn.WithPrivateIP())
	}
	if len(defaultDialOpts) > 0 {
		dialerOpts = append(dialerOpts, cloudsqlconn.WithDefaultDialOptions(defaultDialOpts...))
	}

	// Format IAM username (ensure lowercase, trim .gserviceaccount.com, add .iam suffix)
	user := strings.ToLower(conn.GetProp("user", "username"))
	if user == "" {
		return g.Error("username is required for Cloud SQL IAM authentication")
	}
	user = strings.TrimSuffix(user, ".gserviceaccount.com")

	// Get database name
	dbName := conn.GetProp("database")
	if dbName == "" {
		return g.Error("database is required for Cloud SQL connection")
	}

	// Register the pgxv5 driver with Cloud SQL connector
	// This automatically handles IAM token generation and injection
	driverName := fmt.Sprintf("cloudsql-postgres-%d", time.Now().UnixNano())
	g.Trace("driver options summary: IAM=true, LazyRefresh=%v, PrivateIP=%v",
		cast.ToBool(conn.GetProp("gcp_lazy_refresh")),
		cast.ToBool(conn.GetProp("gcp_use_private_ip")))

	cleanup, err := pgxv5.RegisterDriver(driverName, dialerOpts...)
	if err != nil {
		return g.Error(err, "could not register Cloud SQL driver")
	}

	conn.cloudSQLCleanup = func() {
		if cleanupErr := cleanup(); cleanupErr != nil {
			g.LogError(cleanupErr, "error during Cloud SQL cleanup")
		}
	}

	// Build connection string with instance connection name as host
	// For IAM auth, OMIT password field entirely - IAM handles authentication via token injection
	// The pgxv5 driver will extract the host and use it as the instance connection name
	connString := fmt.Sprintf("host=%s user=%s dbname=%s sslmode=disable", instanceName, user, dbName)
	g.Trace("full connection string format: host=%s user=%s dbname=%s sslmode=disable", instanceName, user, dbName)

	sqlDB, err := sql.Open(driverName, connString)
	if err != nil {
		if conn.cloudSQLCleanup != nil {
			conn.cloudSQLCleanup()
		}
		return g.Error(err, "could not open Cloud SQL connection")
	}

	// Wrap the sql.DB in sqlx.DB for compatibility with the rest of the codebase
	conn.db = sqlx.NewDb(sqlDB, "postgres")

	// Configure connection pool
	maxConns := cast.ToInt(conn.GetProp("max_conns"))
	if maxConns == 0 {
		maxConns = 25
	}
	conn.db.SetMaxOpenConns(maxConns)
	conn.db.SetMaxIdleConns(maxConns / 4)
	conn.db.SetConnMaxLifetime(30 * time.Minute)
	conn.db.SetConnMaxIdleTime(5 * time.Minute)

	g.Trace("cloud SQL connection pool configured: max_conns=%d, max_idle=%d", maxConns, maxConns/4)

	// Test connection with ping
	pingTimeout := 15
	if len(timeOut) > 0 && timeOut[0] != 0 {
		pingTimeout = timeOut[0]
	}
	pingCtx, cancel := context.WithTimeout(ctx, time.Duration(pingTimeout)*time.Second)
	defer cancel()

	err = conn.db.PingContext(pingCtx)
	if err != nil {
		if conn.cloudSQLCleanup != nil {
			conn.cloudSQLCleanup()
		}
		return g.Error(err, "could not ping Cloud SQL instance. Verify: 1) IAM auth flag 'cloudsql.iam_authentication=on', 2) SA '%s' has 'Cloud SQL Instance User' role, 3) SA added as IAM db user with full email, 4) DB privileges granted to '%s', 5) Credentials file valid and matches SA, 6) Cloud SQL Admin API enabled", user, user)
	}

	// Open native pgx connection for COPY operations
	// This connection will be reused throughout the connection lifecycle
	err = conn.openCloudSQLNativePgxConn(ctx, instanceName, user, dbName, dialerOpts)
	if err != nil {
		g.Warn("could not open native pgx connection for COPY operations: %s", err.Error())
		conn.SetProp("use_bulk", "false") // set to not use COPY since IAM auth need pgx conn to COPY
	}

	// Set role if provided
	if val := conn.GetProp("role"); val != "" {
		_, err = conn.Exec("SET ROLE " + val)
		if err != nil {
			return g.Error(err, "could not set role")
		}
	}

	if !cast.ToBool(conn.GetProp("silent")) {
		g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))
	}

	conn.postConnect()

	return nil
}

// openCloudSQLNativePgxConn opens a native pgx connection for Cloud SQL COPY operations
// This is called once during connectCloudSQL and the connection is reused
func (conn *PostgresConn) openCloudSQLNativePgxConn(ctx context.Context, instanceName, user, dbName string, dialerOpts []cloudsqlconn.Option) error {
	// Create Cloud SQL dialer
	dialer, err := cloudsqlconn.NewDialer(ctx, dialerOpts...)
	if err != nil {
		return g.Error(err, "could not create Cloud SQL dialer for native pgx connection")
	}

	// Build pgx connection config
	pgxConfig, err := pgx.ParseConfig(fmt.Sprintf("user=%s dbname=%s sslmode=disable", user, dbName))
	if err != nil {
		return g.Error(err, "could not parse pgx config")
	}

	// Set the dialer function to use Cloud SQL connector
	pgxConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.Dial(ctx, instanceName)
	}

	// Open native pgx connection
	conn.cloudSQLConn, err = pgx.ConnectConfig(ctx, pgxConfig)
	if err != nil {
		return g.Error(err, "could not open native pgx connection for Cloud SQL COPY")
	}

	g.Trace("opened native pgx connection for Cloud SQL COPY operations")
	return nil
}

// closeCloudSQLNativeConn closes the native pgx connection
func (conn *PostgresConn) closeCloudSQLNativeConn() {
	if conn.cloudSQLConn != nil {
		ctx := conn.Context().Ctx
		if err := conn.cloudSQLConn.Close(ctx); err != nil {
			g.LogError(err, "error closing native Cloud SQL connection")
		}
		conn.cloudSQLConn = nil
		g.Trace("closed native pgx connection for Cloud SQL COPY operations")
	}
}

// Close closes the database connection and cleans up Cloud SQL resources
func (conn *PostgresConn) Close() error {
	// Cleanup Cloud SQL resources first
	if conn.isCloudSQL {
		g.Trace("closing Cloud SQL connection and cleaning up resources")

		// Close native pgx connection if open
		conn.closeCloudSQLNativeConn()

		// Call cleanup function to unregister driver
		if conn.cloudSQLCleanup != nil {
			conn.cloudSQLCleanup()
			conn.cloudSQLCleanup = nil
		}
	}

	// Call base connection close
	return conn.BaseConn.Close()
}

// CopyToStdout Copy TO STDOUT
func (conn *PostgresConn) CopyToStdout(ctx *g.Context, sql string) (stdOutReader io.Reader, err error) {
	var stderr bytes.Buffer
	copyQuery := fmt.Sprintf(`\copy ( %s ) TO STDOUT WITH CSV HEADER`, sql)
	copyQuery = strings.ReplaceAll(copyQuery, "\n", " ")

	proc := exec.Command("psql", conn.URL, "-X", "-c", copyQuery)
	proc.Stderr = &stderr
	stdOutReader, err = proc.StdoutPipe()

	go func() {
		if err := proc.Run(); err != nil {
			// bytes, _ := proc.CombinedOutput()
			cmdStr := strings.ReplaceAll(strings.Join(proc.Args, " "), conn.URL, "$DBURL")
			err = g.Error(
				err,
				fmt.Sprintf(
					"COPY FROM Command -> %s\nCOPY FROM Error   -> %s",
					cmdStr, stderr.String(),
				),
			)
			ctx.CaptureErr(err)
			g.LogError(err, "could not PG copy")
		}
	}()

	return stdOutReader, err
}

// GenerateDDL generates a DDL based on a dataset
func (conn *PostgresConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (ddl string, err error) {
	ddl, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	ddl, err = table.AddPrimaryKeyToDDL(ddl, data.Columns)
	if err != nil {
		return ddl, g.Error(err)
	}

	partitionBy := ""
	if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.Template().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by range (%s)", strings.Join(colNames, ", "))
	}
	ddl = strings.ReplaceAll(ddl, "{partition_by}", partitionBy)

	if !temporary {
		for _, index := range table.Indexes(data.Columns) {
			if idxDDL := index.CreateDDL(); idxDDL != "" {
				ddl = ddl + ";\n" + idxDDL
			}
		}

		for _, stmt := range table.ColumnCommentsDDL(conn, data.Columns) {
			ddl = ddl + ";\n" + stmt
		}
	}

	return strings.TrimSpace(ddl), nil
}

// BulkExportStream uses the bulk dumping (COPY)
func (conn *PostgresConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
	// Arrow lane: read through ADBC when the gate marked this connection. A
	// stage 2 decline reads with the native driver.
	if adbcConn, ok := conn.BaseConn.arrowLaneReader(); ok {
		ds, err = adbcConn.laneExportStream(table.Select(), table.Columns)
		if !errors.Is(err, ErrArrowLaneDeclined) {
			return ds, err
		}
	}

	_, err = exec.LookPath("psql")
	if err != nil {
		g.Trace("psql not found in path. Using cursor...")
		return conn.StreamRows(table.Select(), g.M("columns", table.Columns))
	}

	if conn.BaseConn.GetProp("allow_bulk_export") != "true" {
		return conn.StreamRows(table.Select(), g.M("columns", table.Columns))
	}

	copyCtx := g.NewContext(conn.Context().Ctx)
	stdOutReader, err := conn.CopyToStdout(copyCtx, table.Select())
	if err != nil {
		return ds, err
	}

	csv := iop.CSV{Reader: stdOutReader}
	ds, err = csv.ReadStreamContext(copyCtx.Ctx)

	return ds, err
}

// BulkImportStream inserts a stream into a table
func (conn *PostgresConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	if conn.UseADBC() && !conn.isCloudSQL {
		conn.Commit()
		return conn.adbc.BulkImportStream(tableFName, ds)
	}
	return conn.bulkImportStreamPgx(tableFName, ds)
}

// copyFrom runs a pgx COPY on the Cloud SQL connection, or on a connection of the pool.
func (conn *PostgresConn) copyFrom(ctx context.Context, table Table, columns []string, rows [][]any) (n int64, err error) {
	if conn.isCloudSQL {
		if conn.cloudSQLConn == nil {
			return 0, g.Error("native pgx connection not available for Cloud SQL COPY")
		}
		return copyRows(ctx, conn.cloudSQLConn, table, columns, rows)
	}

	sqlConn, err := conn.Db().Conn(ctx)
	if err != nil {
		return 0, g.Error(err, "could not get connection for COPY")
	}
	defer sqlConn.Close()

	err = sqlConn.Raw(func(driverConn any) (err error) {
		stdConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return g.Error("connection is not pgx (%T)", driverConn)
		}
		n, err = copyRows(ctx, stdConn.Conn(), table, columns, rows)
		return err
	})
	return n, err
}

var copyTextEscaper = strings.NewReplacer(`\`, `\\`, "\n", `\n`, "\r", `\r`, "\t", `\t`)

// copyRows runs a binary COPY. If a column type has no binary codec in pgx
// (timetz, an extension type such as geometry), it runs a text COPY, as lib/pq did.
func copyRows(ctx context.Context, c *pgx.Conn, table Table, columns []string, rows [][]any) (int64, error) {
	tableIdent := pgx.Identifier{table.Schema, table.Name}.Sanitize()
	quoted := make([]string, len(columns))
	for i, col := range columns {
		quoted[i] = pgx.Identifier{col}.Sanitize()
	}
	colList := strings.Join(quoted, ", ")

	sd, err := c.PgConn().Prepare(ctx, "", g.F("select %s from %s", colList, tableIdent), nil)
	if err != nil {
		return 0, g.Error(err, "could not get column types of %s", tableIdent)
	}

	binary := true
	for _, field := range sd.Fields {
		if t, ok := c.TypeMap().TypeForOID(field.DataTypeOID); !ok || !t.Codec.FormatSupported(pgx.BinaryFormatCode) {
			binary = false
			break
		}
	}
	if binary {
		return c.CopyFrom(ctx, pgx.Identifier{table.Schema, table.Name}, columns, pgx.CopyFromRows(rows))
	}

	var buf []byte
	for _, row := range rows {
		for i, val := range row {
			if i > 0 {
				buf = append(buf, '\t')
			}
			text, err := c.TypeMap().Encode(sd.Fields[i].DataTypeOID, pgx.TextFormatCode, val, nil)
			if err != nil {
				return 0, g.Error(err, "could not encode value of column %s", columns[i])
			} else if text == nil {
				buf = append(buf, `\N`...)
			} else {
				buf = append(buf, copyTextEscaper.Replace(string(text))...)
			}
		}
		buf = append(buf, '\n')
	}

	tag, err := c.PgConn().CopyFrom(ctx, bytes.NewReader(buf), g.F("copy %s (%s) from stdin", tableIdent, colList))
	return tag.RowsAffected(), err
}

// bulkImportStreamPgx inserts a stream into a table with pgx COPY
func (conn *PostgresConn) bulkImportStreamPgx(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	mux := ds.Context.Mux

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// set OnSchemaChange
	if df := ds.Df(); df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {

			// sleep to allow transaction to close
			time.Sleep(300 * time.Millisecond)

			mux.Lock()
			defer mux.Unlock()

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for Postgres (pgx)")
			}

			return nil
		}
	}

	// COPY runs on a different connection, so close the open transaction
	if err = conn.Commit(); err != nil {
		return 0, g.Error(err, "could not commit for COPY operation")
	}

	for batch := range ds.BatchChan {
		if batch.ColumnsChanged() || batch.IsFirst() {
			mux.Lock()
			columns, err = conn.GetColumns(tableFName, batch.Columns.Names()...)
			mux.Unlock()
			if err != nil {
				return count, g.Error(err, "could not get matching list of columns from table")
			}

			err = batch.Shape(columns)
			if err != nil {
				return count, g.Error(err, "could not shape batch stream")
			}
		}

		err = func() error {
			// Collect rows from batch for CopyFrom
			var rows [][]interface{}
			for row := range batch.Rows() {
				rows = append(rows, row)
			}

			if len(rows) == 0 {
				return nil
			}

			// Use pgx CopyFrom with pgx.Identifier for schema.table
			mux.Lock()
			rowsInserted, err := conn.copyFrom(conn.Context().Ctx, table, columns.Names(), rows)
			mux.Unlock()

			if err != nil {
				return g.Error(err, "could not COPY into table %s using pgx", tableFName)
			}

			count += uint64(rowsInserted)
			g.Trace("pgx CopyFrom inserted %d rows", rowsInserted)

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data (pgx)")
		}
	}

	ds.SetEmpty()

	g.Trace("pgx COPY %d ROWS", count)
	return count, nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *PostgresConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.IsString() && srcCol.Type != iop.JsonType && tgtCol.Type == iop.JsonType:
		selectStr = g.F("%s::jsonb", qName)
	case srcCol.IsString() && !tgtCol.IsString():
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case !srcCol.IsString() && tgtCol.IsString():
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.IsString() && strings.ToLower(tgtCol.DbType) == "uuid":
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.IsBool() && tgtCol.IsInteger():
		selectStr = g.F("%s::int", qName)
	case srcCol.Type != iop.TimestampzType && tgtCol.Type == iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	case srcCol.Type == iop.TimestampzType && tgtCol.Type != iop.TimestampzType:
		selectStr = g.F("%s::%s", qName, tgtCol.DbType)
	default:
		selectStr = qName
	}

	return selectStr
}

// GenerateMergeSQLWithStrategy generates the merge SQL using the specified strategy.
func (conn *PostgresConn) GenerateMergeSQLWithStrategy(srcTable string, tgtTable string, pkFields []string, strategy *MergeStrategy) (sql string, err error) {

	mc, err := conn.BaseConn.GenerateMergeConfigWithStrategy(srcTable, tgtTable, pkFields, strategy)
	if err != nil {
		err = g.Error(err, "could not generate merge variables")
		return
	}

	pkFieldsQ := lo.Map(pkFields, func(f string, i int) string { return conn.Quote(f) })

	tempTable := g.RandSuffix("temp", 5)

	tempTableIndexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", tempTable+"_idx",
		"table", tempTable,
		"cols", strings.Join(pkFieldsQ, ", "),
	)

	finalTableIndexSQL := g.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", tgtTable,
		"cols", strings.Join(pkFieldsQ, ", "),
	)

	sql = g.R(
		mc.Template,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"temp_table", tempTable,
		"final_table_index_sql", finalTableIndexSQL,
		"temp_table_index_sql", tempTableIndexSQL,
		"src_tgt_pk_equal", mc.Map["src_tgt_pk_equal"],
		"src_upd_pk_equal", strings.ReplaceAll(mc.Map["src_tgt_pk_equal"], "tgt.", "upd."),
		"src_del_pk_equal", strings.ReplaceAll(mc.Map["src_tgt_pk_equal"], "tgt.", "del."),
		"pk_fields", mc.Map["pk_fields"],
		"src_fields", mc.Map["src_fields"],
		"tgt_pk_fields", mc.Map["tgt_pk_fields"],
		"set_fields", mc.Map["set_fields"],
		"insert_fields", mc.Map["insert_fields"],
		"src_insert_fields", mc.Map["src_insert_fields"],
	)

	return
}

// openPgx opens a pgx pool that returns timestamptz values in the session
// time zone, as lib/pq did. By default pgx uses the local zone of the process.
func openPgx(connURL string) (*sqlx.DB, error) {
	config, err := pgx.ParseConfig(connURL)
	if err != nil {
		return nil, g.Error(err, "could not parse postgres url")
	}

	// no named prepared statements, as with lib/pq (PgBouncer in transaction mode)
	if !strings.Contains(connURL, "default_query_exec_mode") {
		config.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	}

	keepSessionZone := func(ctx context.Context, c *pgx.Conn) error {
		loc, err := time.LoadLocation(c.PgConn().ParameterStatus("TimeZone"))
		if err != nil {
			// a POSIX zone such as "<-03>+03"
			var offset int
			if err = c.QueryRow(ctx, "select extract(timezone from now())::int").Scan(&offset); err != nil {
				return g.Error(err, "could not get session time zone")
			}
			loc = time.FixedZone("", offset)
		}
		c.TypeMap().RegisterType(&pgtype.Type{Name: "timestamptz", OID: pgtype.TimestamptzOID, Codec: &pgtype.TimestamptzCodec{ScanLocation: loc}})

		// built-in types that pgx does not know (such as timetz) keep their name, and come as text
		for typOid, name := range oid.TypeName {
			if _, ok := c.TypeMap().TypeForOID(uint32(typOid)); !ok {
				c.TypeMap().RegisterType(&pgtype.Type{Name: strings.ToLower(name), OID: uint32(typOid), Codec: &pgtype.TextFormatOnlyCodec{Codec: &pgtype.TextCodec{}}})
			}
		}
		return nil
	}

	// "postgres" gives the $1 bind type in sqlx
	return sqlx.NewDb(stdlib.OpenDB(*config, stdlib.OptionAfterConnect(keepSessionZone)), "postgres"), nil
}

// formatTimetz gives a timetz value in the format of lib/pq: "08:30:45+02" becomes "08:30:45+02:00"
func formatTimetz(str string) string {
	layout := "15:04:05-07"
	for i := 3; i <= 6 && len(str) > i && str[len(str)-i] == ':'; i += 3 {
		layout += ":00"
	}
	if t, err := time.Parse(layout, str); err == nil {
		return t.Format("15:04:05.999999-07:00")
	}
	return str
}
