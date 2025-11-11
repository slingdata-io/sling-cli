package database

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"time"

	"cloud.google.com/go/cloudsqlconn"
	"cloud.google.com/go/cloudsqlconn/postgres/pgxv5"
	"github.com/jackc/pgx/v5"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	"github.com/lib/pq"
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
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by range (%s)", strings.Join(colNames, ", "))
	}
	ddl = strings.ReplaceAll(ddl, "{partition_by}", partitionBy)

	for _, index := range table.Indexes(data.Columns) {
		ddl = ddl + ";\n" + index.CreateDDL()
	}

	return strings.TrimSpace(ddl), nil
}

// BulkExportStream uses the bulk dumping (COPY)
func (conn *PostgresConn) BulkExportStream(table Table) (ds *iop.Datastream, err error) {
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
	// For Cloud SQL with pgx, use a specialized implementation
	if conn.isCloudSQL && conn.cloudSQLConn != nil {
		return conn.bulkImportStreamPgx(tableFName, ds)
	}

	// Standard lib/pq implementation for normal PostgreSQL
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
				return g.Error(err, "could not process ColumnChange for Postgres")
			}

			return nil
		}
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

			// COPY needs a transaction
			if conn.Tx() == nil {
				err = conn.Begin()
				if err != nil {
					return g.Error(err, "could not begin")
				}
				defer conn.Rollback()
			}

			stmt, err := conn.Prepare(pq.CopyInSchema(table.Schema, table.Name, columns.Names()...))
			if err != nil {
				g.Trace("%s: %#v", table, columns.Names())
				return g.Error(err, "could not prepare statement")
			}

			for row := range batch.Rows {
				// g.PP(batch.Columns.MakeRec(row))
				count++
				// Do insert
				mux.Lock()
				_, err := stmt.Exec(row...)
				mux.Unlock()
				if err != nil {
					ds.Context.CaptureErr(g.Error(err, "could not COPY into table %s", tableFName))
					ds.Context.Cancel()
					g.Trace("error for rec: %s", g.Pretty(batch.Columns.MakeRec(row)))
					return g.Error(err, "could not execute statement")
				}
			}

			err = stmt.Close()
			if err != nil {
				g.Warn("%#v", err)
				return g.Error(err, "could not close statement")
			}

			err = conn.Commit()
			if err != nil {
				return g.Error(err, "could not commit transaction")
			}

			return nil
		}()

		if err != nil {
			return count, g.Error(err, "could not copy data")
		}
	}

	ds.SetEmpty()

	g.Trace("COPY %d ROWS", count)
	return count, nil
}

// bulkImportStreamPgx handles COPY for Cloud SQL using native pgx
func (conn *PostgresConn) bulkImportStreamPgx(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var columns iop.Columns

	mux := ds.Context.Mux

	table, err := ParseTableName(tableFName, conn.GetType())
	if err != nil {
		err = g.Error(err, "could not get table name for import")
		return
	}

	// Verify native pgx connection is available
	if conn.cloudSQLConn == nil {
		return count, g.Error("native pgx connection not available for Cloud SQL COPY")
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

	// close the transaction from main connection to allow cloudSQLConn connection to COPY
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
			for row := range batch.Rows {
				rows = append(rows, row)
			}

			if len(rows) == 0 {
				return nil
			}

			// Use pgx CopyFrom with pgx.Identifier for schema.table
			mux.Lock()
			rowsInserted, err := conn.cloudSQLConn.CopyFrom(
				conn.Context().Ctx,
				pgx.Identifier{table.Schema, table.Name},
				columns.Names(),
				pgx.CopyFromRows(rows),
			)
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

// GenerateMergeSQL generates the upsert SQL
func (conn *PostgresConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	mc, err := conn.BaseConn.GenerateMergeConfig(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
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
		"src_fields", mc.Map["src_fields"],
		"tgt_pk_fields", mc.Map["tgt_pk_fields"],
		// "set_fields", strings.ReplaceAll(upsertMap["set_fields"], "src.", "excluded."),
		"set_fields", mc.Map["set_fields"],
		"insert_fields", mc.Map["insert_fields"],
	)

	return
}
