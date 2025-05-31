package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/env"
	"golang.org/x/oauth2/google"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQueryConn is a Google Big Query connection
type BigQueryConn struct {
	BaseConn
	URL       string
	Client    *bigquery.Client
	ProjectID string
	DatasetID string
	Location  string
	Datasets  []string
	Mux       sync.Mutex
}

// Init initiates the object
func (conn *BigQueryConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbBigQuery

	u, err := net.NewURL(conn.BaseConn.URL)
	if err != nil {
		return g.Error(err, "could not parse bigquery url")
	}

	conn.ProjectID = conn.GetProp("project")
	if conn.ProjectID == "" {
		conn.ProjectID = u.U.Host
	}

	conn.DatasetID = conn.GetProp("dataset")
	if conn.DatasetID == "" {
		conn.DatasetID = conn.GetProp("schema")
	}

	conn.Location = conn.GetProp("location")

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	err = conn.BaseConn.Init()
	if err != nil {
		err = g.Error(err, "could not initialize connection")
		return err
	}

	// Google BigQuery has limits
	// https://cloud.google.com/bigquery/quotas
	conn.Context().SetConcurrencyLimit(5)
	// conn.SetProp("FILE_MAX_ROWS", "1000000") // hard code?

	if conn.GetProp("GC_KEY_FILE") == "" {
		conn.SetProp("GC_KEY_FILE", conn.GetProp("keyfile")) // dbt style
	}
	if conn.GetProp("GC_KEY_FILE") == "" {
		conn.SetProp("GC_KEY_FILE", conn.GetProp("credentialsFile"))
	}
	if conn.GetProp("GC_KEY_BODY") == "" {
		conn.SetProp("GC_KEY_BODY", conn.GetProp("KEY_BODY"))
	}

	// set MAX_DECIMALS to fix bigquery import for numeric types
	conn.SetProp("MAX_DECIMALS", "9")

	return nil
}

func (conn *BigQueryConn) getNewClient(timeOut ...int) (client *bigquery.Client, err error) {
	var authOptions []option.ClientOption
	var credJsonBody string

	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}

	// Add specified scopes for BigQuery such as:
	// "https://www.googleapis.com/auth/drive"
	// "https://www.googleapis.com/auth/spreadsheets"
	scopes := []string{"https://www.googleapis.com/auth/bigquery"}
	if val := conn.GetProp("extra_scopes"); val != "" {
		var extraScopes []string
		g.Unmarshal(val, &extraScopes)
		scopes = append(scopes, extraScopes...)
	}

	if val := conn.GetProp("GC_KEY_BODY"); val != "" {
		credJsonBody = val
		authOptions = append(authOptions, option.WithCredentialsJSON([]byte(val)))
		authOptions = append(authOptions, option.WithScopes(scopes...))
	} else if val := conn.GetProp("GC_KEY_FILE"); val != "" {
		authOptions = append(authOptions, option.WithCredentialsFile(val))
		authOptions = append(authOptions, option.WithScopes(scopes...))
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else if val := conn.GetProp("GC_CRED_API_KEY"); val != "" {
		authOptions = append(authOptions, option.WithAPIKey(val))
	} else if val := conn.GetProp("GOOGLE_APPLICATION_CREDENTIALS"); val != "" {
		authOptions = append(authOptions, option.WithCredentialsFile(val))
		authOptions = append(authOptions, option.WithScopes(scopes...))
		b, err := os.ReadFile(val)
		if err != nil {
			return client, g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else {
		creds, err := google.FindDefaultCredentials(conn.BaseConn.Context().Ctx, scopes...)
		if err != nil {
			return client, g.Error(err, "No Google credentials provided or could not find Application Default Credentials.")
		}
		authOptions = append(authOptions, option.WithCredentials(creds))
	}

	if conn.ProjectID == "" && credJsonBody != "" {
		m := g.M()
		g.Unmarshal(credJsonBody, &m)
		conn.ProjectID = cast.ToString(m["project_id"])
	}

	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()

	client, err = bigquery.NewClient(ctx, conn.ProjectID, authOptions...)
	if err != nil {
		return nil, g.Error(err, "Failed to create BigQuery client")
	}

	// set the location if specified
	if conn.Location != "" {
		client.Location = conn.Location
	}

	return client, nil
}

// Connect connects to the database
func (conn *BigQueryConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}

	// get list of datasets
	_, err = conn.GetSchemas()
	if err != nil {
		return g.Error(err, "Failed to get datasets in project: %s", conn.Client.Project())
	}

	return conn.BaseConn.Connect()
}

// NewTransaction creates a new transaction
func (conn *BigQueryConn) NewTransaction(ctx context.Context, options ...*sql.TxOptions) (tx Transaction, err error) {
	// context := g.NewContext(ctx)

	// _, err = conn.ExecContext(ctx, "BEGIN")
	// if err != nil {
	// 	return nil, g.Error(err, "could not begin Tx")
	// }

	// BQ does not support transactions at the moment
	// Tx := &BlankTransaction{Conn: conn.Self(), context: &context}
	// conn.tx = Tx

	return nil, nil
}

type bqResult struct {
	TotalRows uint64
	res       driver.Result
}

func (r bqResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r bqResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.TotalRows), nil
}

func (conn *BigQueryConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {

	if len(args) > 0 {
		for _, arg := range args {
			switch val := arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%d", val), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%f", val), 1)
			case time.Time:
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", val.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			case []byte:
				if len(val) == 0 {
					sql = strings.Replace(sql, "?", "NULL", 1)

				} else {
					newdata := base64.StdEncoding.EncodeToString(val)
					sql = strings.Replace(sql, "?", fmt.Sprintf("FROM_BASE64('%s')", newdata), 1)
				}
			default:
				v := strings.ReplaceAll(cast.ToString(val), "\n", "\\n")
				v = strings.ReplaceAll(v, "'", "\\'")
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", v), 1)
			}
		}
	}

	res := bqResult{}
	conn.LogSQL(sql)

	q := conn.Client.Query(sql)
	q.JobIDConfig.Location = conn.Location
	q.QueryConfig = bigquery.QueryConfig{
		Q:                sql,
		DefaultDatasetID: conn.GetProp("schema"),
		CreateSession:    true,
	}

	it, err := q.Read(ctx)
	if err != nil {
		if strings.Contains(sql, noDebugKey) && !g.IsDebugLow() {
			err = g.Error(err, "Error executing query")
			return
		} else {
			err = g.Error(err, "Error executing "+env.Clean(conn.Props(), sql))
			return
		}
	} else {
		res.TotalRows = it.TotalRows + res.TotalRows
	}

	// if bp, cj := getBytesProcessed(it); bp > 0 {
	// 	g.DebugLow("BigQuery job %s (%d children) => Processed %d bytes", q.JobID, cj, bp)
	// }

	result = res

	return
}

// GenerateDDL generates a DDL based on a dataset
func (conn *BigQueryConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (sql string, err error) {
	sql, err = conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return sql, g.Error(err)
	}

	partitionBy := ""
	if keys, ok := table.Keys[iop.PartitionKey]; ok {
		// allow custom SQL expression for partitioning
		partitionBy = g.F("partition by %s", strings.Join(keys, ", "))
	} else if keyCols := data.Columns.GetKeys(iop.PartitionKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		partitionBy = g.F("partition by %s", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{partition_by}", partitionBy)

	clusterBy := ""
	if keyCols := data.Columns.GetKeys(iop.ClusterKey); len(keyCols) > 0 {
		colNames := conn.GetType().QuoteNames(keyCols.Names()...)
		clusterBy = g.F("cluster by %s", strings.Join(colNames, ", "))
	}
	sql = strings.ReplaceAll(sql, "{cluster_by}", clusterBy)

	return strings.TrimSpace(sql), nil
}

type bQTypeCols struct {
	numericCols  []int
	datetimeCols []int
	dateCols     []int
	boolCols     []int
	timeCols     []int
}

func processBQTypeCols(row []interface{}, bqTC *bQTypeCols, ds *iop.Datastream) []interface{} {
	for _, j := range bqTC.numericCols {
		var vBR *big.Rat
		vBR, ok := row[j].(*big.Rat)
		if ok {
			row[j] = vBR.FloatString(9)
		}
	}
	for _, j := range bqTC.datetimeCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.DateTime)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.Date.String() + " " + vDT.Time.String())
			}
		}
	}
	for _, j := range bqTC.dateCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.Date)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.String())
			}
		}
	}
	for _, j := range bqTC.timeCols {
		if row[j] != nil {
			vDT, ok := row[j].(civil.Time)
			if ok {
				row[j], _ = ds.Sp.ParseTime(vDT.String())
			}
		}
	}
	for _, j := range bqTC.boolCols {
		if row[j] != nil {
			vB, ok := row[j].(bool)
			if ok {
				row[j] = vB
			}
		}
	}
	return row
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BigQueryConn) getItColumns(itSchema bigquery.Schema) (cols iop.Columns, bQTC bQTypeCols) {
	cols = make(iop.Columns, len(itSchema))
	for i, field := range itSchema {
		cols[i] = iop.Column{
			Name:        field.Name,
			Position:    i + 1,
			Type:        NativeTypeToGeneral(field.Name, string(field.Type), conn),
			DbType:      string(field.Type),
			DbPrecision: cast.ToInt(field.Precision),
			DbScale:     cast.ToInt(field.Scale),
			Sourced:     true,
		}
		cols[i].SetLengthPrecisionScale()

		if g.In(field.Type, bigquery.NumericFieldType) {
			bQTC.numericCols = append(bQTC.numericCols, i)
			// https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#decimal_types
			cols[i].DbPrecision = 38
			cols[i].DbScale = 9
		} else if g.In(field.Type, bigquery.FloatFieldType) {
			bQTC.numericCols = append(bQTC.numericCols, i)
			cols[i].Sourced = false // need to infer the decimal lengths
		} else if field.Type == "DATETIME" || field.Type == bigquery.TimestampFieldType {
			bQTC.datetimeCols = append(bQTC.datetimeCols, i)
		} else if field.Type == "DATE" {
			bQTC.dateCols = append(bQTC.dateCols, i)
		} else if field.Type == bigquery.TimeFieldType {
			bQTC.timeCols = append(bQTC.timeCols, i)
		}
	}
	return
}

func (conn *BigQueryConn) StreamRowsContext(ctx context.Context, sql string, options ...map[string]interface{}) (ds *iop.Datastream, err error) {
	bQTC := bQTypeCols{}
	opts := getQueryOptions(options)
	Limit := uint64(0) // infinite
	if val := cast.ToUint64(opts["limit"]); val > 0 {
		Limit = val
	}

	start := time.Now()
	if strings.TrimSpace(sql) == "" {
		g.Warn("Empty Query")
		return ds, nil
	}

	conn.LogSQL(sql)

	queryContext := g.NewContext(ctx)
	q := conn.Client.Query(sql)
	q.QueryConfig = bigquery.QueryConfig{
		Q:                sql,
		DefaultDatasetID: conn.GetProp("schema"),
	}

	it, err := q.Read(queryContext.Ctx)
	if err != nil {
		if strings.Contains(sql, noDebugKey) && !g.IsDebugLow() {
			err = g.Error(err, "SQL Error")
		} else {
			err = g.Error(err, "SQL Error for:\n"+sql)
		}
		return
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}
	conn.Data.NoDebug = !strings.Contains(sql, noDebugKey)

	// need to fetch first row to get schema
	var values []bigquery.Value
	err = it.Next(&values)
	if err != nil && err != iterator.Done {
		return ds, g.Error(err, "Failed to scan")
	}
	conn.Data.Columns, bQTC = conn.getItColumns(it.Schema)

	if err == iterator.Done {
		ds = iop.NewDatastreamContext(queryContext.Ctx, conn.Data.Columns)
		ds.SetReady()
		ds.Close()
		return ds, nil
	}

	nextFunc := func(it2 *iop.Iterator) bool {
		if Limit > 0 && it2.Counter >= Limit {
			return false
		}

		err := it.Next(&values)
		if err == iterator.Done {
			return false
		} else if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Failed to scan"))
			return false
		}

		it2.Row = make([]interface{}, len(values))
		for i := range values {
			it2.Row[i] = values[i]
		}
		it2.Row = processBQTypeCols(it2.Row, &bQTC, ds)
		return true
	}

	ds = iop.NewDatastreamIt(queryContext.Ctx, conn.Data.Columns, nextFunc)
	ds.NoDebug = strings.Contains(sql, noDebugKey)
	ds.Inferred = !InferDBStream && ds.Columns.Sourced()
	if !ds.NoDebug {
		ds.SetMetadata(conn.GetProp("METADATA"))
		ds.SetConfig(conn.Props())
	}

	// add first row pulled to buffer
	row := make([]interface{}, len(values))
	for i := range values {
		row[i] = values[i]
	}
	ds.Buffer = append(ds.Buffer, processBQTypeCols(row, &bQTC, ds))

	err = ds.Start()
	if err != nil {
		queryContext.Cancel()
		return ds, g.Error(err, "could start datastream")
	}

	// if bp, cj := getBytesProcessed(it); bp > 0 {
	// 	g.Debug("BigQuery job %s (%d children) => Processed %d bytes", q.JobID, cj, bp)
	// }

	return
}

// Close closes the connection
func (conn *BigQueryConn) Close() error {
	if conn.Client == nil {
		return nil
	}

	err := conn.Client.Close()
	if err != nil {
		return err
	}
	return conn.BaseConn.Close()
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BigQueryConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

func getBqSchema(columns iop.Columns) (schema bigquery.Schema) {
	schema = make([]*bigquery.FieldSchema, len(columns))
	mapping := map[iop.ColumnType]bigquery.FieldType{
		iop.ColumnType(""): bigquery.StringFieldType,
		iop.StringType:     bigquery.StringFieldType,
		iop.UUIDType:       bigquery.StringFieldType,
		iop.TextType:       bigquery.StringFieldType,
		iop.JsonType:       bigquery.JSONFieldType,
		iop.BoolType:       bigquery.BooleanFieldType,
		iop.BinaryType:     bigquery.BytesFieldType,
		iop.DateType:       bigquery.DateFieldType,
		iop.DatetimeType:   bigquery.TimestampFieldType,
		iop.FloatType:      bigquery.FloatFieldType,
		iop.SmallIntType:   bigquery.IntegerFieldType,
		iop.IntegerType:    bigquery.IntegerFieldType,
		iop.BigIntType:     bigquery.IntegerFieldType,
		// https://stackoverflow.com/questions/55904464/big-query-does-now-cast-automatically-long-decimal-values-to-numeric-when-runni
		iop.DecimalType: bigquery.NumericFieldType,
		// "decimal":   bigquery.FloatFieldType,
		iop.TimeType:       bigquery.StringFieldType,
		iop.TimestampType:  bigquery.TimestampFieldType,
		iop.TimestampzType: bigquery.TimestampFieldType,
	}

	for i, col := range columns {
		g.Trace("bigquery.Schema for %s (%s) -> %#v", col.Name, col.Type, mapping[col.Type])
		schema[i] = &bigquery.FieldSchema{
			Name: col.Name,
			Type: mapping[col.Type],
		}
	}
	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in GCS and then use the COPY command.
func (conn *BigQueryConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	// set OnSchemaChange
	if df != nil && cast.ToBool(conn.GetProp("adjust_column_type")) {
		oldOnColumnChanged := df.OnColumnChanged
		df.OnColumnChanged = func(col iop.Column) error {
			// prevent any new writers
			conn.Mux.Lock()
			defer conn.Mux.Unlock()

			// wait till all current writers are done
			if qs := conn.Context().Wg.Write.GetQueueSize(); qs > 0 {
				conn.Context().Wg.Write.Wait()
			}

			// use pre-defined function
			err = oldOnColumnChanged(col)
			if err != nil {
				return g.Error(err, "could not process ColumnChange for BigQuery")
			}
			return nil
		}
	}

	if gcBucket := conn.GetProp("GC_BUCKET"); gcBucket == "" {
		return conn.importViaLocalStorage(tableFName, df)
	}

	return conn.importViaGoogleStorage(tableFName, df)
}

func (conn *BigQueryConn) importViaLocalStorage(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.GzipCompressorType)

	fs, err := filesys.NewFileSysClient(dbio.TypeFileLocal, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for Local")
		return
	}

	localPath := path.Join(env.GetTempFolder(), "bigquery", env.CleanTableName(tableFName), g.NowFileStr())
	err = filesys.Delete(fs, localPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+localPath)
	}

	df.Defer(func() { env.RemoveAllLocalTempFile(localPath) })

	g.Info("importing into bigquery via local storage")

	fileReadyChn := make(chan filesys.FileReady, 10)

	go func() {
		config := iop.LoaderStreamConfig(true)
		config.TargetType = conn.GetType()
		_, err = fs.WriteDataflowReady(df, localPath, fileReadyChn, config)

		if err != nil {
			df.Context.CaptureErr(g.Error(err, "error writing dataflow to local storage: "+localPath))
			return
		}

	}()

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		err = g.Error(err, "could not parse table name: "+tableFName)
		return
	}

	table.Columns, err = conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "could not get table columns: "+tableFName)
		return
	}

	copyFromLocal := func(localFile filesys.FileReady, table Table) {
		defer conn.Context().Wg.Write.Done()
		defer env.RemoveAllLocalTempFile(localFile.Node.Path())

		g.Debug("Loading %s [%s]", localFile.Node.Path(), humanize.Bytes(cast.ToUint64(localFile.BytesW)))

		err := conn.CopyFromLocal(localFile.Node.Path(), table, localFile.Columns)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying from %s into %s", localFile.Node.Path(), tableFName))
		}
	}

	for localFile := range fileReadyChn {
		if df.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second) // max 5 load jobs per 10 secs

		conn.Mux.Lock() // to not collide with schema change
		conn.Context().Wg.Write.Add()
		go copyFromLocal(localFile, table)
		conn.Mux.Unlock()
	}

	conn.Context().Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), g.Error(df.Err(), "Error importing to BigQuery")
	}

	return df.Count(), nil
}

func (conn *BigQueryConn) importViaGoogleStorage(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn, iop.GzipCompressorType)

	gcBucket := conn.GetProp("GC_BUCKET")

	if gcBucket == "" {
		return count, g.Error("Need to set 'GC_BUCKET' to copy to google storage")
	}
	fs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for GCS")
		return
	}

	gcsPath := fmt.Sprintf(
		"gs://%s/%s/%s.csv",
		gcBucket,
		tempCloudStorageFolder,
		tableFName,
	)

	err = filesys.Delete(fs, gcsPath)
	if err != nil {
		return count, g.Error(err, "Could not Delete: "+gcsPath)
	}

	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(fs, gcsPath)
		}
	})

	g.Info("importing into bigquery via google storage")

	fileReadyChn := make(chan filesys.FileReady, 10)

	go func() {
		config := iop.LoaderStreamConfig(true)
		config.TargetType = conn.GetType()
		_, err = fs.WriteDataflowReady(df, gcsPath, fileReadyChn, config)

		if err != nil {
			g.LogError(err, "error writing dataflow to google storage: "+gcsPath)
			df.Context.CaptureErr(g.Error(err, "error writing dataflow to google storage: "+gcsPath))
			return
		}

	}()

	table, err := ParseTableName(tableFName, conn.Type)
	if err != nil {
		err = g.Error(err, "could not parse table name: "+tableFName)
		return
	}

	table.Columns, err = conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "could not get table columns: "+tableFName)
		return
	}

	copyFromGCS := func(gcsFile filesys.FileReady, table Table) {
		defer conn.Context().Wg.Write.Done()
		g.Debug("Loading %s [%s]", gcsFile.Node.URI, humanize.Bytes(cast.ToUint64(gcsFile.BytesW)))

		err := conn.CopyFromGCS(gcsFile.Node.URI, table, gcsFile.Columns)
		if err != nil {
			df.Context.CaptureErr(g.Error(err, "Error copying from %s into %s", gcsFile.Node.URI, tableFName))
		}
	}

	for gcsFile := range fileReadyChn {
		if df.Err() != nil {
			break
		}
		time.Sleep(2 * time.Second) // max 5 load jobs per 10 secs

		conn.Mux.Lock() // to not collide with schema change
		conn.Context().Wg.Write.Add()
		go copyFromGCS(gcsFile, table)
		conn.Mux.Unlock()
	}

	conn.Context().Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), g.Error(df.Err(), "Error importing to BigQuery")
	}

	return df.Count(), nil
}

// CopyFromGCS into bigquery from google storage
func (conn *BigQueryConn) CopyFromLocal(localURI string, table Table, dsColumns []iop.Column) error {

	file, err := os.Open(localURI)
	if err != nil {
		return g.Error(err, "Failed to open temp file")
	}
	return conn.LoadCSVFromReader(table, file, dsColumns)
}

// LoadCSVFromReader demonstrates loading data into a BigQuery table using a file on the local filesystem.
// https://cloud.google.com/bigquery/docs/batch-loading-data#loading_data_from_local_files
func (conn *BigQueryConn) LoadCSVFromReader(table Table, reader io.Reader, dsColumns []iop.Column) error {
	client, err := conn.getNewClient()
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	source := bigquery.NewReaderSource(reader)
	source.FieldDelimiter = ","
	source.AllowQuotedNewlines = true
	source.Quote = `"`
	source.NullMarker = `\N`
	source.SkipLeadingRows = 1
	source.Schema = getBqSchema(dsColumns)
	source.SourceFormat = bigquery.CSV

	loader := client.Dataset(table.Schema).Table(table.Name).LoaderFrom(source)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.Location = client.Location

	job, err := loader.Run(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in loader.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in task.Wait")
	}

	if err := status.Err(); err != nil {
		return g.Error(err, "Error in Import Task")
	}

	return nil
}

// BulkImportStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = g.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

func (conn *BigQueryConn) CopyFromGCS(gcsURI string, table Table, dsColumns []iop.Column) error {
	client, err := conn.getNewClient()
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	gcsRef.NullMarker = `\N`
	gcsRef.SkipLeadingRows = 1
	gcsRef.Schema = getBqSchema(dsColumns)
	if strings.HasSuffix(strings.ToLower(gcsURI), ".gz") {
		gcsRef.Compression = bigquery.Gzip
	}
	gcsRef.MaxBadRecords = 0
	loader := client.Dataset(table.Schema).Table(table.Name).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.Location = client.Location

	job, err := loader.Run(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in loader.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in task.Wait")
	}

	if err := status.Err(); err != nil {
		return g.Error(err, "Error in Import Task")
	}

	return nil
}

// BulkExportFlow reads in bulk
func (conn *BigQueryConn) BulkExportFlow(table Table) (df *iop.Dataflow, err error) {
	if conn.GetProp("GC_BUCKET") == "" {
		g.Warn("No GCS Bucket was provided, pulling from cursor (which may be slower for big datasets). ")
		return conn.BaseConn.BulkExportFlow(table)
	}

	// get columns
	columns, err := conn.GetSQLColumns(table)
	if err != nil {
		err = g.Error(err, "Could not get columns.")
		return
	}

	gsURL, err := conn.Unload(table)
	if err != nil {
		err = g.Error(err, "Could not unload.")
		return
	}

	fs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Could not get fs client for GCS")
		return
	}

	// set column coercion if specified
	if coerceCols, ok := getColumnsProp(conn); ok {
		cc, _ := getColumnCasingProp(conn)
		tgtType := dbio.Type(conn.GetProp("target_type"))
		columns.Coerce(coerceCols, true, cc, tgtType)
	}

	fs.SetProp("header", "true")
	fs.SetProp("format", "csv")
	fs.SetProp("null_if", `\N`)
	fs.SetProp("columns", g.Marshal(columns))
	fs.SetProp("metadata", conn.GetProp("metadata"))

	// setting empty_as_null=true. no way to export with proper null_marker.
	// gcsRef.NullMarker = `\N` does not work, not way to do so in EXPORT DATA OPTIONS
	// Also, Parquet export doesn't support JSON types
	fs.SetProp("empty_as_null", "true")

	df, err = fs.ReadDataflow(gsURL)
	if err != nil {
		err = g.Error(err, "Could not read "+gsURL)
		return
	}

	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(fs, gsURL)
		}
	})

	return
}

// Unload to Google Cloud Storage
func (conn *BigQueryConn) Unload(tables ...Table) (gsPath string, err error) {
	gcBucket := conn.GetProp("GC_BUCKET")
	if gcBucket == "" {
		err = g.Error("Must provide prop 'GC_BUCKET'")
		return
	}

	unloadCtx := g.NewContext(conn.Context().Ctx)

	doExport := func(table Table, gsPartURL string) {
		defer conn.Context().Wg.Write.Done()

		bucket := conn.GetProp("GC_BUCKET")
		if bucket == "" {
			err = g.Error("need to provide prop 'GC_BUCKET'")
			return
		}

		err = conn.CopyToGCS(table, gsPartURL)
		if err != nil {
			unloadCtx.CaptureErr(g.Error(err, "Could not Copy to GS"))
		}
	}

	gsFs, err := filesys.NewFileSysClient(dbio.TypeFileGoogle, conn.PropArr()...)
	if err != nil {
		err = g.Error(err, "Unable to create GCS Client")
		return
	}

	gsPath = fmt.Sprintf("gs://%s/%s/stream/%s.csv", gcBucket, tempCloudStorageFolder, cast.ToString(g.Now()))

	filesys.Delete(gsFs, gsPath)

	for i, table := range tables {
		gsPathPart := fmt.Sprintf("%s/part%02d-*", gsPath, i+1)
		conn.Context().Wg.Write.Add()
		go doExport(table, gsPathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = unloadCtx.Err()

	if err == nil {
		g.Debug("Unloaded to %s", gsPath)
	}

	return gsPath, err
}

// CopyToGCS Copy table to gc storage
func (conn *BigQueryConn) ExportToGCS(sql string, gcsURI string) error {

	unloadSQL := g.R(
		conn.template.Core["copy_to_gcs"],
		"sql", sql,
		"gcs_path", gcsURI,
	)
	_, err := conn.Exec(unloadSQL)
	if err != nil {
		err = g.Error(err, "could not export data")
	}
	return err
}

func (conn *BigQueryConn) CopyToGCS(table Table, gcsURI string) error {
	if table.IsQuery() || table.IsView {
		return conn.ExportToGCS(table.Select(), gcsURI)
	}

	client, err := conn.getNewClient()
	if err != nil {
		return g.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	// gcsRef.NullMarker = `\N` // does not work for export, only importing
	gcsRef.Compression = bigquery.Gzip
	gcsRef.MaxBadRecords = 0

	extractor := client.DatasetInProject(conn.ProjectID, table.Schema).Table(table.Name).ExtractorTo(gcsRef)
	extractor.DisableHeader = false
	extractor.Location = client.Location

	job, err := extractor.Run(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in extractor.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return g.Error(err, "Error in task.Wait")
	}
	if err := status.Err(); err != nil {
		if strings.Contains(err.Error(), "it is currently a VIEW") || strings.Contains(err.Error(), "it currently has type VIEW") {
			table.IsView = true
			return conn.CopyToGCS(table, gcsURI)
		}
		return g.Error(err, "Error in Export Task")
	}

	g.Info("wrote to %s", gcsURI)
	return nil
}

// CastColumnForSelect casts to the correct target column type
func (conn *BigQueryConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)

	switch {
	case srcCol.IsString() && !srcCol.Type.IsJSON() && tgtCol.Type.IsJSON():
		selectStr = g.F("to_json(%s)", qName)
	case !srcCol.IsFloat() && tgtCol.IsFloat():
		selectStr = g.F("cast(%s as float64)", qName)
	case srcCol.IsString() && tgtCol.IsDecimal():
		selectStr = g.F("parse_numeric(%s)", qName)
	case !srcCol.IsDecimal() && tgtCol.IsDecimal():
		selectStr = g.F("cast(%s as numeric)", qName)
	case !srcCol.IsInteger() && tgtCol.IsInteger():
		selectStr = g.F("cast(%s as int64)", qName)
	case !srcCol.IsString() && tgtCol.IsString():
		selectStr = g.F("cast(%s as string)", qName)
	case (srcCol.IsString() || tgtCol.IsDate()) && tgtCol.IsDatetime():
		selectStr = g.F("cast(%s as timestamp)", qName)
	case (srcCol.IsString() || srcCol.IsDatetime()) && tgtCol.IsDate():
		selectStr = g.F("cast(%s as date)", qName)
	case !strings.EqualFold(srcCol.DbType, "datetime") && strings.EqualFold(tgtCol.DbType, "datetime"):
		selectStr = g.F("cast(%s as datetime)", qName)
	case !strings.EqualFold(srcCol.DbType, "timestamp") && strings.EqualFold(tgtCol.DbType, "timestamp"):
		selectStr = g.F("cast(%s as timestamp)", qName)
	default:
		selectStr = qName
	}

	return selectStr
}

// GenerateUpsertSQL generates the upsert SQL
func (conn *BigQueryConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {

	upsertMap, err := conn.BaseConn.GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = g.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	delete from {tgt_table} tgt
	where exists (
			select 1
			from {src_table} src
			where {src_tgt_pk_equal}
	)
	;

	insert into {tgt_table}
		({insert_fields})
	select {src_fields}
	from {src_table} src
	`

	// MERGE works fine, but needs to scan the target table?
	// sqlTempl := `
	// merge into {tgt_table} tgt
	// using (select {src_fields} from {src_table}) src
	// ON ({src_tgt_pk_equal})
	// WHEN MATCHED THEN
	// 	UPDATE SET {set_fields}
	// WHEN NOT MATCHED THEN
	// 	INSERT ({insert_fields}) values  ({src_fields_values})
	// `

	sql = g.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
		"src_fields_values", strings.ReplaceAll(upsertMap["placeholder_fields"], "ph.", "src."),
	)

	return
}

// GetDatabases returns databases
func (conn *BigQueryConn) GetDatabases() (iop.Dataset, error) {
	// fields: [name]
	data := iop.NewDataset(iop.NewColumnsFromFields("name"))
	data.Append([]interface{}{conn.ProjectID})
	return data, nil
}

// GetSchemas returns schemas
func (conn *BigQueryConn) GetSchemas() (iop.Dataset, error) {
	// fields: [schema_name]

	// get list of datasets
	it := conn.Client.Datasets(conn.Context().Ctx)
	conn.Datasets = []string{}
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		} else if err != nil {
			return iop.Dataset{}, g.Error(err, "Failed to get datasets in project: %s", conn.Client.Project())
		}
		conn.Datasets = append(conn.Datasets, dataset.DatasetID)
		if conn.Location == "" {
			md, err := dataset.Metadata(conn.Context().Ctx)
			if err == nil {
				conn.Location = md.Location
			}
		}
	}

	data := iop.NewDataset(iop.NewColumnsFromFields("schema_name"))
	for _, dataset := range conn.Datasets {
		data.Append([]interface{}{dataset})
	}
	return data, nil
}

// GetSchemata obtain full schemata info for a schema and/or table in current database
func (conn *BigQueryConn) GetSchemata(level SchemataLevel, schemaName string, tableNames ...string) (Schemata, error) {
	schemata := Schemata{
		Databases: map[string]Database{},
		conn:      conn,
	}

	// refresh datasets
	err := conn.Connect()
	if err != nil {
		return schemata, g.Error(err, "could not get connect to get datasets")
	}

	data, err := conn.GetSchemas()
	if err != nil {
		return schemata, g.Error(err, "could not get schemas")
	}

	datasets := data.ColValuesStr(0)
	if schemaName != "" {
		datasets = []string{schemaName}
	}

	currDatabase := conn.ProjectID
	schemas := map[string]Schema{}

	ctx := g.NewContext(conn.context.Ctx, 5)

	getOneSchemata := func(values map[string]interface{}) error {
		defer ctx.Wg.Read.Done()

		var data iop.Dataset
		switch level {
		case SchemataLevelSchema:
			data.Columns = iop.NewColumnsFromFields("schema_name")
			data.Append([]any{values["schema"]})
		case SchemataLevelTable:
			data, err = conn.GetTablesAndViews(schemaName)
		case SchemataLevelColumn:
			data, err = conn.SubmitTemplate(
				"single", conn.template.Metadata, "schemata",
				values,
			)
		}
		if err != nil {
			return g.Error(err, "Could not get schemata at %s level", level)
		}

		defer ctx.Unlock()
		ctx.Lock()

		for _, rec := range data.Records() {
			schemaName = cast.ToString(rec["schema_name"])
			tableName := cast.ToString(rec["table_name"])
			columnName := cast.ToString(rec["column_name"])
			dataType := strings.ToLower(cast.ToString(rec["data_type"]))
			dataType = strings.Split(dataType, "(")[0]
			dataType = strings.Split(dataType, "<")[0]

			// if any of the names contains a period, skip. This messes with the keys
			if strings.Contains(tableName, ".") ||
				strings.Contains(schemaName, ".") ||
				strings.Contains(columnName, ".") {
				continue
			}

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
					Dialect:  dbio.TypeDbBigQuery,
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
		return nil
	}

	for _, dataset := range datasets {
		g.Debug("getting schemata for %s at %s level", dataset, level)
		values := g.M("schema", dataset)

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

		ctx.Wg.Read.Add()
		go getOneSchemata(values)
	}

	ctx.Wg.Read.Wait()

	return schemata, nil
}

func getBytesProcessed(it *bigquery.RowIterator) (bytesProcessed int64, childJobs int64) {
	if job := it.SourceJob(); job != nil {
		if status, err := job.Status(context.Background()); err == nil {
			if stats := status.Statistics; stats != nil {
				childJobs = stats.NumChildJobs
				bytesProcessed = stats.TotalBytesProcessed
			}
		}
	}
	return
}
