package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	h "github.com/flarco/gutil"
	"github.com/slingdata-io/sling/core/iop"
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
}

// bqTuple represents a row item.
type bqTuple struct {
	Fields []string
	Row    []interface{}
}

// Init initiates the object
func (conn *BigQueryConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = BigQueryDbType

	conn.ProjectID = conn.GetProp("PROJECT_ID")
	conn.DatasetID = conn.GetProp("DatasetID")
	if conn.DatasetID == "" {
		conn.DatasetID = conn.GetProp("schema")
	}

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	err := conn.BaseConn.Init()
	if err != nil {
		err = h.Error(err, "could not initialize connection")
		return err
	}

	// Google BigQuery has limits
	// https://cloud.google.com/bigquery/quotas
	conn.Context().SetConcurencyLimit(5)
	conn.SetProp("SLING_FILE_ROW_LIMIT", "1000000") // hard code?

	// set MAX_DECIMALS to fix bigquery import for numeric types
	os.Setenv("MAX_DECIMALS", "9")

	return nil
}

func (conn *BigQueryConn) getNewClient(timeOut ...int) (client *bigquery.Client, err error) {
	var authOption option.ClientOption
	to := 15
	if len(timeOut) > 0 {
		to = timeOut[0]
	}
	if val := conn.GetProp("GC_CRED_JSON_BODY_ENC"); val != "" {
		decodedValue, err2 := url.QueryUnescape(val)
		if err2 != nil {
			err = h.Error(err2, "Could not decode provided GC_CRED_JSON_BODY_ENC")
			return
		}
		jsonBytes := []byte(decodedValue)
		authOption = option.WithCredentialsJSON(jsonBytes)
	} else if val := conn.GetProp("GC_CRED_JSON_BODY"); val != "" {
		authOption = option.WithCredentialsJSON([]byte(val))
	} else if val := conn.GetProp("GC_CRED_API_KEY"); val != "" {
		authOption = option.WithAPIKey(val)
	} else if val := conn.GetProp("GC_CRED_FILE"); val != "" {
		authOption = option.WithCredentialsFile(val)
	}
	if authOption == nil {
		err = h.Error("no Google crendentials provided")
		return
	}
	ctx, cancel := context.WithTimeout(conn.BaseConn.Context().Ctx, time.Duration(to)*time.Second)
	defer cancel()
	return bigquery.NewClient(ctx, conn.ProjectID, authOption)
}

// Connect connects to the database
func (conn *BigQueryConn) Connect(timeOut ...int) error {
	var err error
	conn.Client, err = conn.getNewClient(timeOut...)
	if err != nil {
		return h.Error(err, "Failed to connect to client")
	}
	return conn.BaseConn.Connect()
}

type bqResult struct {
	it  *bigquery.RowIterator
	res driver.Result
}

func (r bqResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (r bqResult) RowsAffected() (int64, error) {
	return cast.ToInt64(r.it.TotalRows), nil
}

// ExecContext runs a sql query with context, returns `error`
func (conn *BigQueryConn) ExecContext(ctx context.Context, sql string, args ...interface{}) (result sql.Result, err error) {

	if len(args) > 0 {
		for _, arg := range args {
			switch arg.(type) {
			case int, int64, int8, int32, int16:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%d", arg), 1)
			case float32, float64:
				sql = strings.Replace(sql, "?", fmt.Sprintf("%f", arg), 1)
			case time.Time:
				t := arg.(time.Time)
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05")), 1)
			case nil:
				sql = strings.Replace(sql, "?", "NULL", 1)
			case []byte:
				data, ok := arg.([]byte)
				if ok {
					if len(data) == 0 {
						sql = strings.Replace(sql, "?", "NULL", 1)

					} else {
						newdata := base64.StdEncoding.EncodeToString(data)
						sql = strings.Replace(sql, "?", fmt.Sprintf("FROM_BASE64('%s')", newdata), 1)
					}
				}
			default:
				val := strings.ReplaceAll(cast.ToString(arg), "\n", "\\n")
				val = strings.ReplaceAll(val, "'", "\\'")
				sql = strings.Replace(sql, "?", fmt.Sprintf("'%s'", val), 1)
			}
		}
	}

	noTrace := strings.Contains(sql, "\n\n-- nT --")
	if !noTrace {
		h.Debug(sql)
	}

	q := conn.Client.Query(sql)
	q.QueryConfig = bigquery.QueryConfig{
		Q:                sql,
		DefaultDatasetID: conn.GetProp("schema"),
	}
	it, err := q.Read(ctx)
	if err != nil {
		err = h.Error(err, "SQL Error for:\n"+sql)
		return
	}

	result = bqResult{it: it}

	if err != nil {
		err = h.Error(err, "Error executing "+sql)
	}

	return
}

type bQTypeCols struct {
	numericCols  []int
	datetimeCols []int
	dateCols     []int
	timeCols     []int
}

func processBQTypeCols(row []interface{}, bqTC *bQTypeCols, ds *iop.Datastream) []interface{} {
	for _, j := range bqTC.numericCols {
		var vBR *big.Rat
		vBR = row[j].(*big.Rat)
		row[j], _ = vBR.Float64()
	}
	for _, j := range bqTC.datetimeCols {
		var vDT civil.DateTime
		if row[j] != nil {
			vDT = row[j].(civil.DateTime)
			row[j], _ = ds.Sp.ParseTime(vDT.Date.String() + " " + vDT.Time.String())
		}
	}
	for _, j := range bqTC.dateCols {
		var vDT civil.Date
		if row[j] != nil {
			vDT = row[j].(civil.Date)
			row[j], _ = ds.Sp.ParseTime(vDT.String())
		}
	}
	for _, j := range bqTC.timeCols {
		var vDT civil.Time
		if row[j] != nil {
			vDT = row[j].(civil.Time)
			row[j], _ = ds.Sp.ParseTime(vDT.String())
		}
	}
	return row
}

// StreamRowsContext streams the rows of a sql query with context, returns `result`, `error`
func (conn *BigQueryConn) StreamRowsContext(ctx context.Context, sql string, limit ...int) (ds *iop.Datastream, err error) {
	Limit := 0 // infinite
	if len(limit) > 0 && limit[0] != 0 {
		Limit = limit[0]
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
	q := conn.Client.Query(sql)
	q.QueryConfig = bigquery.QueryConfig{
		Q:                sql,
		DefaultDatasetID: conn.GetProp("schema"),
	}

	it, err := q.Read(queryContext.Ctx)
	if err != nil {
		err = h.Error(err, "SQL Error for:\n"+sql)
		return
	}

	conn.Data.SQL = sql
	conn.Data.Duration = time.Since(start).Seconds()
	conn.Data.Rows = [][]interface{}{}

	ds = iop.NewDatastreamContext(queryContext.Ctx, nil)

	// collect sample up to 1000 rows and infer types
	counter := 0
	var row []interface{}
	bQTC := bQTypeCols{}
	for {
		var values []bigquery.Value
		err = it.Next(&values)
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = h.Error(err, "Failed to scan")
			ds.Context.CaptureErr(err)
			ds.Context.Cancel()
			return
		}

		if ds.Columns == nil {
			ds.Columns = make([]iop.Column, len(it.Schema))
			for i, col := range it.Schema {
				ds.Columns[i] = iop.Column{
					Name:     col.Name,
					Position: i + 1,
					Type:     cast.ToString(col.Type),
				}
				h.Trace("%s - %s", col.Name, col.Type)
				if col.Type == "NUMERIC" {
					bQTC.numericCols = append(bQTC.numericCols, i)
				} else if col.Type == "DATETIME" {
					bQTC.datetimeCols = append(bQTC.datetimeCols, i)
				} else if col.Type == "DATE" {
					bQTC.dateCols = append(bQTC.dateCols, i)
				} else if col.Type == "TIME" {
					bQTC.timeCols = append(bQTC.timeCols, i)
				}
			}
		}

		row = make([]interface{}, len(ds.Columns))
		for i := range values {
			row[i] = values[i]
		}
		row = processBQTypeCols(row, &bQTC, ds)

		row = ds.Sp.ProcessRow(row)
		ds.Buffer = append(ds.Buffer, row)
		counter++
		if counter == sampleSize {
			break
		}
	}

	sampleData := iop.NewDataset(ds.Columns)
	sampleData.Rows = ds.Buffer
	sampleData.InferColumnTypes()
	ds.Columns = sampleData.Columns

	go func() {
		// Ensure that at the end of the loop we close the channel!
		defer ds.Close()

		for _, row := range ds.Buffer {
			for i, val := range row {
				row[i] = ds.Sp.CastVal(i, val, ds.Columns[i].Type)
			}
			ds.Push(row)
		}

		ds.Ready = true

		for {
			if Limit > 0 && counter == Limit {
				break
			}

			var values []bigquery.Value
			err := it.Next(&values)
			if err == iterator.Done {
				err = nil
				break
			}
			if err != nil {
				ds.Context.CaptureErr(h.Error(err, "Failed to scan"))
				ds.Context.Cancel()
				break
			}

			row = make([]interface{}, len(ds.Columns))
			for i := range values {
				row[i] = values[i]
			}

			row = processBQTypeCols(row, &bQTC, ds)

			counter++
			select {
			case <-ds.Context.Ctx.Done():
				break
			default:
				ds.Push(row)
			}
		}
	}()

	return
}

// Close closes the connection
func (conn *BigQueryConn) Close() error {
	err := conn.Client.Close()
	if err != nil {
		return err
	}
	return nil
}

// InsertBatchStream inserts a stream into a table in batch
func (conn *BigQueryConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// InsertStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return conn.BulkImportStream(tableFName, ds)
}

// Save implements the ValueSaver interface.
func (t *bqTuple) Save() (map[string]bigquery.Value, string, error) {
	rec := map[string]bigquery.Value{}

	for i, col := range t.Fields {
		rec[col] = t.Row[i]
	}

	return rec, "", nil
}

// insertRows demonstrates inserting data into a table using the streaming insert mechanism.
func (conn *BigQueryConn) insertRows(context *h.Context, tableFName string, rows []*bqTuple) (count uint64, err error) {
	defer context.Wg.Write.Done()
	tableID := tableFName

	inserter := conn.Client.Dataset(conn.DatasetID).Table(tableID).Inserter()
	if err := inserter.Put(context.Ctx, rows); err != nil {
		return 0, h.Error(err, "Error in inserter.Put")
	}

	return uint64(len(rows)), nil
}

// importWithInserter import the stream rows in bulk
// does the inserts in parallel
func (conn *BigQueryConn) importWithInserter(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	const batchSize = 100000
	batchRows := make([]*bqTuple, batchSize)
	counter := 0
	for row := range ds.Rows {
		select {
		case <-ds.Context.Ctx.Done():
			counter = 0
			break
		default:
			counter++
			batchRows[counter-1] = &bqTuple{Fields: ds.GetFields(), Row: row}
			if counter == batchSize {
				ds.Context.Wg.Write.Add()
				go conn.insertRows(&ds.Context, tableFName, batchRows)
				counter = 0
				batchRows = make([]*bqTuple, batchSize)
			}
		}
	}

	// insert last batch
	if counter > 0 {
		ds.Context.Wg.Write.Add()
		conn.insertRows(&ds.Context, tableFName, batchRows)
	}

	ds.Context.Wg.Write.Wait()

	return ds.Count, ds.Context.Ctx.Err()
}

func getBqSchema(columns []iop.Column) (schema bigquery.Schema) {
	schema = make([]*bigquery.FieldSchema, len(columns))
	mapping := map[string]bigquery.FieldType{
		"":         bigquery.StringFieldType,
		"string":   bigquery.StringFieldType,
		"text":     bigquery.StringFieldType,
		"json":     bigquery.StringFieldType,
		"bool":     bigquery.BooleanFieldType,
		"bytes":    bigquery.BytesFieldType,
		"binary":   bigquery.BytesFieldType,
		"date":     bigquery.TimestampFieldType,
		"datetime": bigquery.TimestampFieldType,
		"float":    bigquery.FloatFieldType,
		"smallint": bigquery.IntegerFieldType,
		"integer":  bigquery.IntegerFieldType,
		"bigint":   bigquery.IntegerFieldType,
		// https://stackoverflow.com/questions/55904464/big-query-does-now-cast-automatically-long-decimal-values-to-numeric-when-runni
		"decimal": bigquery.NumericFieldType,
		// "decimal":   bigquery.FloatFieldType,
		"time":      bigquery.TimeFieldType,
		"timestamp": bigquery.TimestampFieldType,
	}

	for i, col := range columns {
		h.Trace("bigquery.Schema for %s (%s) -> %#v", col.Name, col.Type, mapping[col.Type])
		schema[i] = &bigquery.FieldSchema{
			Name: col.Name,
			Type: mapping[col.Type],
		}
	}
	return
}

// BulkImportFlow inserts a flow of streams into a table.
// For redshift we need to create CSVs in S3 and then use the COPY command.
func (conn *BigQueryConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	settingMppBulkImportFlow(conn)

	gcBucket := conn.GetProp("GC_BUCKET")

	if gcBucket == "" {
		return count, errors.New("Need to set 'GC_BUCKET' to copy to google storage")
	}
	fs, err := iop.NewFileSysClient(iop.GoogleFileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for GCS")
		return
	}

	gcsPath := fmt.Sprintf(
		"gs://%s/%s/%s.csv",
		gcBucket,
		filePathStorageSlug,
		tableFName,
	)

	err = fs.Delete(gcsPath)
	if err != nil {
		return count, h.Error(err, "Could not Delete: "+gcsPath)
	}

	defer func() { fs.Delete(gcsPath) }() // cleanup

	h.Info("importing into bigquery via google storage")

	fileReadyChn := make(chan string, 10)

	go func() {
		_, err = fs.WriteDataflowReady(df, gcsPath, fileReadyChn)

		if err != nil {
			h.LogError(err, "error writing dataflow to google storage: "+gcsPath)
			df.Context.Cancel()
			conn.Context().Cancel()
			return
		}

	}()

	copyFromGCS := func(gcsURI string, tableFName string) {
		defer conn.Context().Wg.Write.Done()
		h.Debug("Loading %s", gcsURI)
		err := conn.CopyFromGCS(gcsURI, tableFName, df.Columns)
		if err != nil {
			df.Context.CaptureErr(h.Error(err, "Error copying from %s into %s", gcsURI, tableFName))
			df.Context.Cancel()
			conn.Context().Cancel()
		}
	}

	for gcsPartPath := range fileReadyChn {
		time.Sleep(2 * time.Second) // max 5 load jobs per 10 secs
		conn.Context().Wg.Write.Add()
		go copyFromGCS(gcsPartPath, tableFName)
	}

	conn.Context().Wg.Write.Wait()
	if df.Err() != nil {
		return df.Count(), h.Error(df.Context.Err(), "Error importing to BigQuery")
	}

	return df.Count(), nil
}

// BulkImportStream demonstrates loading data into a BigQuery table using a file on the local filesystem.
func (conn *BigQueryConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		err = h.Error(err, "Could not MakeDataFlow")
		return
	}

	return conn.BulkImportFlow(tableFName, df)
}

// CopyFromGCS into bigquery from google storage
func (conn *BigQueryConn) CopyFromGCS(gcsURI string, tableFName string, dsColumns []iop.Column) error {
	client, err := conn.getNewClient()
	if err != nil {
		return h.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	gcsRef.SkipLeadingRows = 1
	gcsRef.Schema = getBqSchema(dsColumns)
	if strings.HasSuffix(strings.ToLower(gcsURI), ".gz") {
		gcsRef.Compression = bigquery.Gzip
	}
	gcsRef.MaxBadRecords = 0

	schema, tableID := SplitTableFullName(tableFName)
	loader := client.Dataset(schema).Table(tableID).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend

	job, err := loader.Run(conn.Context().Ctx)
	if err != nil {
		return h.Error(err, "Error in loader.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return h.Error(err, "Error in task.Wait")
	}

	if status.Err() != nil {
		conn.Context().CaptureErr(err)
		for _, e := range status.Errors {
			conn.Context().CaptureErr(*e)
		}
		return h.Error(conn.Context().Err(), "Error in Import Task")
	}

	return nil
}

// BulkExportFlow reads in bulk
func (conn *BigQueryConn) BulkExportFlow(sqls ...string) (df *iop.Dataflow, err error) {

	gsURL, err := conn.Unload(sqls...)
	if err != nil {
		err = h.Error(err, "Could not unload.")
		return
	}

	fs, err := iop.NewFileSysClient(iop.GoogleFileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for GCS")
		return
	}

	df, err = fs.ReadDataflow(gsURL)
	if err != nil {
		err = h.Error(err, "Could not read "+gsURL)
		return
	}

	df.Defer(func() { fs.Delete(gsURL) })

	return
}

// CopyToGCS demonstrates using an export task to
// write the contents of a table into Cloud Storage as CSV.
// func (conn *BigQueryConn) CopyFromGS(tableFName string, gcsURI string) error {
// 	ctx, cancel := context.WithTimeout(conn.Context().Ctx, time.Second*50)
// 	defer cancel()
// 	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rc.Close()

// 	data, err := ioutil.ReadAll(rc)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return data, nil
// }

// Unload to Google Cloud Storage
func (conn *BigQueryConn) Unload(sqls ...string) (gsPath string, err error) {
	gcBucket := conn.GetProp("GC_BUCKET")
	if gcBucket == "" {
		err = h.Error("Must provide prop 'GC_BUCKET'")
		return
	}

	doExport := func(sql string, gsPartURL string) {
		defer conn.Context().Wg.Write.Done()

		// create temp table
		tableName := h.F(
			"pg_home.sling_%s",
			h.RandString(h.AlphaRunes, 5),
		)

		_, err := conn.Exec(h.F(
			"create table `%s.%s` as \n%s",
			conn.ProjectID,
			tableName, sql,
		))
		if err != nil {
			conn.Context().CaptureErr(h.Error(err, "Could not create table"))
			return
		}

		bucket := conn.GetProp("GC_BUCKET")
		if bucket == "" {
			err = h.Error("need to provide prop 'GC_BUCKET'")
			return
		}

		// gcsURI := h.F("gs://%s/%s.csv/*", bucket, tableName)

		// export
		err = conn.CopyToGCS(tableName, gsPartURL)
		if err != nil {
			conn.Context().CaptureErr(h.Error(err, "Could not Copy to GS"))
		}

		// drop temp table
		err = conn.DropTable(tableName)
		if err != nil {
			conn.Context().CaptureErr(h.Error(err, "Could not Drop table: "+tableName))
		}

	}

	gsFs, err := iop.NewFileSysClient(iop.GoogleFileSys, conn.PropArr()...)
	if err != nil {
		conn.Context().CaptureErr(h.Error(err, "Unable to create GCS Client"))
	}

	gsPath = fmt.Sprintf("gs://%s/%s/stream/%s.csv", gcBucket, filePathStorageSlug, cast.ToString(h.Now()))

	gsFs.Delete(gsPath)

	for i, sql := range sqls {
		gsPathPart := fmt.Sprintf("%s/part%02d", gsPath, i+1)
		conn.Context().Wg.Write.Add()
		go doExport(sql, gsPathPart)
	}

	conn.Context().Wg.Write.Wait()
	err = conn.Context().Err()

	if err == nil {
		h.Debug("Unloaded to %s", gsPath)
	}

	return gsPath, err
}

// CopyToGCS Copy table to gc storage
func (conn *BigQueryConn) CopyToGCS(tableFName string, gcsURI string) error {
	client, err := conn.getNewClient()
	if err != nil {
		return h.Error(err, "Failed to connect to client")
	}
	defer client.Close()

	if strings.ToUpper(conn.GetProp("SLING_COMPRESSION")) == "GZIP" {
		gcsURI = gcsURI + ".gz"
	}
	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.FieldDelimiter = ","
	gcsRef.AllowQuotedNewlines = true
	gcsRef.Quote = `"`
	if strings.ToUpper(conn.GetProp("SLING_COMPRESSION")) == "GZIP" {
		gcsRef.Compression = bigquery.Gzip
	}
	gcsRef.MaxBadRecords = 0

	schema, tableID := SplitTableFullName(tableFName)
	extractor := client.DatasetInProject(conn.ProjectID, schema).Table(tableID).ExtractorTo(gcsRef)
	extractor.DisableHeader = false
	// You can choose to run the task in a specific location for more complex data locality scenarios.
	// Ex: In this example, source dataset and GCS bucket are in the US.
	extractor.Location = "US"

	job, err := extractor.Run(conn.Context().Ctx)
	if err != nil {
		return h.Error(err, "Error in extractor.Execute")
	}
	status, err := job.Wait(conn.Context().Ctx)
	if err != nil {
		return h.Error(err, "Error in task.Wait")
	}
	if err := status.Err(); err != nil {
		conn.Context().CaptureErr(err)
		for _, e := range status.Errors {
			conn.Context().CaptureErr(*e)
		}
		return h.Error(conn.Context().Err(), "Error in Export Task")
	}

	h.Info("wrote to %s", gcsURI)
	return nil
}

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *BigQueryConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = h.Error(err, "could not generate upsert variables")
		return
	}

	sqlTempl := `
	DELETE FROM {tgt_table} tgt
	WHERE EXISTS (
			SELECT 1
			FROM {src_table} src
			WHERE {src_tgt_pk_equal}
	)
	`
	sql := h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
	)
	_, err = conn.Exec(sql)
	if err != nil {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	sqlTempl = `
	INSERT INTO {tgt_table}
		({insert_fields})
	SELECT {src_fields}
	FROM {src_table} src
	`
	sql = h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", upsertMap["src_fields"],
	)
	_, err = conn.Exec(sql)
	if err != nil {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	rowAffCnt = -1
	cnt, _ := conn.GetCount(srcTable)
	if cnt > 0 {
		rowAffCnt = cast.ToInt64(cnt)
	}
	return

}
