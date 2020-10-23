package database

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	h "github.com/flarco/gutil"
	"github.com/jmoiron/sqlx"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MsSQLServerConn is a Microsoft SQL Server connection
type MsSQLServerConn struct {
	BaseConn
	URL        string
	isAzureSQL bool
	isAzureDWH bool
}

// Init initiates the object
func (conn *MsSQLServerConn) Init() error {

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = SQLServerDbType
	conn.BaseConn.defaultPort = 1433

	version := getVersion(conn.GetURL())
	if strings.Contains(strings.ToLower(version), "sql azure") {
		conn.isAzureSQL = true
		conn.Type = AzureSQLDbType
	} else if strings.Contains(strings.ToLower(version), "azure sql data warehouse") {
		conn.isAzureDWH = true
		conn.Type = AzureDWHDbType
	}

	conn.SetProp("use_bcp_map_parallel", "false")

	var instance Connection
	instance = conn
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// GetURL returns the processed URL
func (conn *MsSQLServerConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	url, err := dburl.Parse(connURL)
	if err != nil {
		h.LogError(err, "could not parse SQL Server URL")
		return connURL
	}

	user := url.User.Username()
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	database := strings.ReplaceAll(url.Path, "/", "")

	return fmt.Sprintf(
		"server=%s;user id=%s;password=%s;port=%s;database=%s;",
		host, user, password, port, database,
	)
}

func getVersion(URL string) (version string) {
	db, err := sqlx.Open("mssql", URL)
	if err != nil {
		return
	}
	res, err := db.Queryx("select @@version v")
	if err != nil {
		return
	}
	res.Next()
	row, err := res.SliceScan()
	if err != nil {
		return
	}
	version = cast.ToString(row[0])
	db.Close()
	return
}

// BulkImportFlow bulk import flow
func (conn *MsSQLServerConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if conn.isAzureDWH {
		return conn.CopyViaAzure(tableFName, df)
	}

	_, err = exec.LookPath("bcp")
	if err != nil {
		h.Trace("bcp not found in path. Using cursor...")
		ds := iop.MergeDataflow(df)
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	importStream := func(ds *iop.Datastream) {
		defer df.Context.Wg.Write.Done()
		_, err = conn.BulkImportStream(tableFName, ds)
		if err != nil {
			df.Context.CaptureErr(h.Error(err))
			df.Context.Cancel()
		}
	}

	for ds := range df.StreamCh {
		df.Context.Wg.Write.Add()
		go importStream(ds)
	}

	df.Context.Wg.Write.Wait()

	return df.Count(), df.Context.Err()
}

// BulkImportStream bulk import stream
func (conn *MsSQLServerConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	_, err = exec.LookPath("bcp")
	if err != nil {
		h.Trace("bcp not found in path. Using cursor...")
		return conn.BaseConn.InsertBatchStream(tableFName, ds)
	}

	// needs to get columns to shape stream
	columns, err := conn.GetColumns(tableFName)
	if err != nil {
		err = h.Error(err, "could not get column list")
		return
	}

	ds, err = ds.Shape(columns)
	if err != nil {
		err = h.Error(err, "could not shape stream")
		return
	}

	return conn.BcpImportStreamParrallel(tableFName, ds)
}

// BcpImportStreamParrallel uses goroutine to import partitioned files
func (conn *MsSQLServerConn) BcpImportStreamParrallel(tableFName string, ds *iop.Datastream) (count uint64, err error) {

	fileRowLimit := cast.ToInt(conn.GetProp("SLING_FILE_ROW_LIMIT"))
	if fileRowLimit == 0 {
		fileRowLimit = 200000
	}

	doImport := func(tableFName string, nDs *iop.Datastream) {
		defer ds.Context.Wg.Write.Done()

		_, err := conn.BcpImportStream(tableFName, nDs)
		ds.Context.CaptureErr(err)
	}

	for nDs := range ds.Chunk(cast.ToUint64(fileRowLimit)) {
		ds.Context.Wg.Write.Add()
		go doImport(tableFName, nDs)
	}

	ds.Context.Wg.Write.Wait()

	return ds.Count, ds.Err()
}

// BcpImportStream Import using bcp tool
// https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15
// bcp dbo.test1 in '/tmp/LargeDataset.csv' -S tcp:sqlserver.host,51433 -d master -U sa -P 'password' -c -t ',' -b 5000
// Limitation: if comma or delimite is in field, it will error.
// need to use delimiter not in field, or do some other transformation
func (conn *MsSQLServerConn) BcpImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	var stderr, stdout bytes.Buffer
	url, err := dburl.Parse(conn.URL)
	if err != nil {
		return
	}

	// Write the ds to a temp file
	filePath := fmt.Sprintf(
		"/tmp/sqlserver.bcp.%d.%s.csv",
		time.Now().Unix(),
		h.RandString(h.AlphaRunes, 3),
	)

	csv := iop.CSV{Path: filePath, Delimiter: ','}
	delimiterRep := `$~d$~`
	quoteRep := `$~q$~`
	newlRep := `$~n$~`
	postUpdateCol := map[int]uint64{}

	// transformation to correctly post process quotes, newlines, and delimiter afterwards
	// https://stackoverflow.com/questions/782353/sql-server-bulk-insert-of-csv-file-with-inconsistent-quotes
	// reduces performance by ~25%, but is correct, and still 10x faster then INSERT INTO with batch VALUES
	// If we use the parallel way, we gain back the speed by using more power. We also loose order.
	transf := func(row []interface{}) (nRow []interface{}) {
		nRow = row
		for i, val := range row {

			switch v := val.(type) {
			case string:
				nRow[i] = strings.ReplaceAll(
					val.(string), string(csv.Delimiter), delimiterRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), `"`, quoteRep,
				)
				nRow[i] = strings.ReplaceAll(
					nRow[i].(string), "\n", newlRep,
				)
				if nRow[i].(string) != val.(string) {
					if _, ok := postUpdateCol[i]; ok {
						postUpdateCol[i]++
					} else {
						postUpdateCol[i] = 1
					}
				}
			default:
				_ = v
			}
		}
		return
	}

	if conn.GetProp("use_bcp_map_parallel") == "true" {
		_, err = csv.WriteStream(ds.MapParallel(transf, 20)) // faster but we loose order
	} else {
		_, err = csv.WriteStream(ds.Map(transf))
	}

	// delete csv
	defer os.Remove(filePath)

	if err != nil {
		ds.Context.CaptureErr(h.Error(err, "Error csv.WriteStream(ds) to "+filePath))
		ds.Context.Cancel()
		return 0, ds.Context.Err()
	}

	// Import to Database
	batchSize := 50000
	password, _ := url.User.Password()
	port := url.Port()
	host := strings.ReplaceAll(url.Host, ":"+port, "")
	database := strings.ReplaceAll(url.Path, "/", "")
	user := url.User.Username()
	hostPort := fmt.Sprintf("tcp:%s,%s", host, port)

	proc := exec.Command(
		"bcp",
		tableFName,
		"in", filePath,
		"-S", hostPort,
		"-d", database,
		"-U", user,
		"-P", password,
		"-t", string(csv.Delimiter),
		"-m", "1",
		"-c",
		"-q",
		"-b", cast.ToString(batchSize),
		"-F", "2",
		"-e", "/dev/stderr",
	)
	proc.Stderr = &stderr
	proc.Stdout = &stdout

	// run and wait for finish
	cmdStr := strings.ReplaceAll(strings.ReplaceAll(strings.Join(proc.Args, " "), password, "****"), hostPort, "****")
	h.Trace(cmdStr)
	err = proc.Run()

	// get count
	regex := *regexp.MustCompile(`(?s)(\d+) rows copied.`)
	res := regex.FindStringSubmatch(stdout.String())
	if len(res) == 2 {
		count = cast.ToUint64(res[1])
	}

	if err != nil {
		err = h.Error(
			err,
			fmt.Sprintf(
				"SQL Server BCP Import Command -> %s\nSQL Server BCP Import Error  -> %s\n%s",
				cmdStr, stderr.String(), stdout.String(),
			),
		)
		ds.Context.CaptureErr(err)
		ds.Context.Cancel()
		return
	}

	// post process if strings have been modified
	if len(postUpdateCol) > 0 {
		setCols := []string{}
		for i, col := range ds.Columns {
			if _, ok := postUpdateCol[i]; !ok {
				continue
			}

			replExpr1 := h.R(
				`REPLACE(CONVERT(VARCHAR(MAX), {field}), '{delimiterRep}', '{delimiter}')`,
				"field", col.Name,
				"delimiterRep", delimiterRep,
				"delimiter", string(csv.Delimiter),
			)
			replExpr2 := h.R(
				`REPLACE({replExpr1}, '{quoteRep}', '{quote}')`,
				"replExpr1", replExpr1,
				"quoteRep", quoteRep,
				"quote", `"`,
			)
			replExpr3 := h.R(
				`REPLACE({replExpr2}, '{newlRep}', {newl})`,
				"replExpr2", replExpr2,
				"newlRep", newlRep,
				"newl", `CHAR(10)`,
			)
			setCols = append(
				setCols, fmt.Sprintf(`%s = %s`, col.Name, replExpr3),
			)
		}

		// do update statement if needed
		if len(setCols) > 0 {
			setColsStr := strings.Join(setCols, ", ")
			sql := fmt.Sprintf(`UPDATE %s SET %s`, tableFName, setColsStr)
			_, err = conn.Exec(sql)
			if err != nil {
				err = h.Error(err, "could not apply post update query")
				return
			}
		}
	}

	return
}

// BcpExport exports data to datastream
func (conn *MsSQLServerConn) BcpExport() (err error) {
	return
}

// sqlcmd -S localhost -d BcpSampleDB -U sa -P <your_password> -I -Q "SELECT * FROM TestEmployees;"

// EXPORT
// bcp TestEmployees out ~/test_export.txt -S localhost -U sa -P <your_password> -d BcpSampleDB -c -t ','
// bcp dbo.test1 out ~/test_export.csv -S server.database.windows.net -U user -P 'password' -d db1 -c -t ',' -q -b 10000

// importing from blob
// https://azure.microsoft.com/en-us/updates/files-from-azure-blob-storage-to-sql-database/

//UPSERT
// https://vladmihalcea.com/how-do-upsert-and-merge-work-in-oracle-sql-server-postgresql-and-mysql/

// Upsert inserts / updates from a srcTable into a target table.
// Assuming the srcTable has some or all of the tgtTable fields with matching types
func (conn *MsSQLServerConn) Upsert(srcTable string, tgtTable string, pkFields []string) (rowAffCnt int64, err error) {

	upsertMap, err := conn.Self().GenerateUpsertExpressions(srcTable, tgtTable, pkFields)
	if err != nil {
		err = h.Error(err, "could not generate upsert variables")
		return
	}

	indexSQL := h.R(
		conn.GetTemplateValue("core.create_unique_index"),
		"index", strings.Join(pkFields, "_")+"_idx",
		"table", tgtTable,
		"cols", strings.Join(pkFields, ", "),
	)

	txn, err := conn.Db().BeginTx(conn.Context().Ctx, &sql.TxOptions{Isolation: sql.LevelSerializable, ReadOnly: false})
	if err != nil {
		err = h.Error(err, "Could not begin transaction for upsert")
		return
	}
	_, err = txn.ExecContext(conn.Context().Ctx, indexSQL)
	if err != nil && !strings.Contains(err.Error(), "already used") {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, indexSQL)
		return
	}

	sqlTempl := `
	MERGE INTO {tgt_table} tgt
	USING (SELECT *	FROM {src_table}) src
	ON ({src_tgt_pk_equal})
	WHEN MATCHED THEN
		UPDATE SET {set_fields}
	WHEN NOT MATCHED THEN
		INSERT ({insert_fields}) VALUES ({src_fields});
	`

	sql := h.R(
		sqlTempl,
		"src_table", srcTable,
		"tgt_table", tgtTable,
		"src_tgt_pk_equal", upsertMap["src_tgt_pk_equal"],
		"set_fields", upsertMap["set_fields"],
		"insert_fields", upsertMap["insert_fields"],
		"src_fields", strings.ReplaceAll(upsertMap["placehold_fields"], "ph.", "src."),
	)
	res, err := txn.ExecContext(conn.Context().Ctx, sql)
	if err != nil {
		err = h.Error(err, "Could not execute upsert from %s to %s -> %s", srcTable, tgtTable, sql)
		return
	}

	rowAffCnt, err = res.RowsAffected()
	if err != nil {
		rowAffCnt = -1
	}

	err = txn.Commit()
	if err != nil {
		err = h.Error(err, "Could not commit upsert transaction")
		return
	}
	return
}

// CopyViaAzure uses the Azure DWH COPY INTO Table command
func (conn *MsSQLServerConn) CopyViaAzure(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if !conn.BaseConn.credsProvided("AZURE") {
		err = h.Error(errors.New("Need to set 'AZURE_SAS_SVC_URL', 'AZURE_CONTAINER' and 'AZURE_ACCOUNT' to copy to azure dwh from container"))
		return
	}

	azPath := fmt.Sprintf(
		"https://%s.blob.core.windows.net/%s/%s-%s",
		conn.GetProp("AZURE_ACCOUNT"),
		conn.GetProp("AZURE_CONTAINER"),
		filePathStorageSlug,
		tableFName,
	)

	azFs, err := iop.NewFileSysClient(iop.AzureFileSys, conn.PropArr()...)
	if err != nil {
		err = h.Error(err, "Could not get fs client for S3")
		return
	}

	err = azFs.Delete(azPath + "*")
	if err != nil {
		return count, h.Error(err, "Could not Delete: "+azPath)
	}

	// defer func() { azFs.Delete(azPath + "*") }() // cleanup

	fileReadyChn := make(chan string, 10000)
	go func() {
		var bw int64
		bw, err = azFs.WriteDataflowReady(df, azPath, fileReadyChn)
		h.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), azPath)

		if err != nil {
			err = h.Error(err, "Error writing dataflow to azure blob: "+azPath)
			return
		}
	}()

	h.Info("writing to azure container for import")

	doCopy := func(filePath string) {
		defer df.Context.Wg.Write.Done()
		cnt, err := conn.CopyFromAzure(tableFName, filePath)
		if err != nil {
			df.Context.CaptureErr(h.Error(err, "could not copy to azure dwh"))
		} else {
			count += cnt
		}
	}

	for filePath := range fileReadyChn {
		df.Context.Wg.Write.Add()
		go doCopy(filePath)
	}

	df.Context.Wg.Write.Wait()
	if df.Err() != nil {
		err = h.Error(df.Context.Err())
	}

	return df.Count(), err
}

// CopyFromAzure uses the COPY INTO Table command from Azure
// https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest
func (conn *MsSQLServerConn) CopyFromAzure(tableFName, azPath string) (count uint64, err error) {
	azSasURL := conn.GetProp("AZURE_SAS_SVC_URL")
	if azSasURL == "" {
		err = h.Error(errors.New("Need to set 'AZURE_SAS_SVC_URL' to copy to azure dwh from container"))
		return
	}

	azSasURLArr := strings.Split(azSasURL, "?")
	if len(azSasURLArr) != 2 {
		err = h.Error(
			fmt.Errorf("Invalid provided AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	azToken := azSasURLArr[1]

	sql := h.R(
		conn.template.Core["copy_from_azure"],
		"table", tableFName,
		"azure_path", azPath,
		"azure_sas_token", azToken,
		"date_format", "ymd",
	)
	conn.SetProp("azToken", azToken) // to not log it in debug logging

	h.Info("copying into azure DWH")
	h.Debug("url: " + azPath)
	_, err = conn.Exec(sql)
	if err != nil {
		conn.SetProp("azToken", azToken)
		return 0, h.Error(err, "SQL Error:\n"+conn.CleanSQL(sql))
	}

	return 0, nil
}
