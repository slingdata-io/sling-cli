package database

import (
	"fmt"
	"os"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
	"github.com/xo/dburl"
)

// MsFabricConn is a Microsoft Fabric connection
type MsFabricConn struct {
	MsSQLServerConn
	URL         string
	versionNum  int
	versionYear int
}

// Init initiates the object
func (conn *MsFabricConn) Init() error {

	// replace scheme for connection
	conn.URL = strings.Replace(conn.URL, "fabric://", "sqlserver://", 1)

	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbFabric
	conn.BaseConn.defaultPort = 1433
	conn.versionYear = 2017 // default version

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

func (conn *MsFabricConn) Connect(timeOut ...int) (err error) {
	err = conn.BaseConn.Connect(timeOut...)
	if err != nil {
		return err
	}

	return nil
}

func (conn *MsFabricConn) GenerateDDL(table Table, data iop.Dataset, temporary bool) (string, error) {

	table.Columns = data.Columns
	ddl, err := conn.BaseConn.GenerateDDL(table, data, temporary)
	if err != nil {
		return ddl, g.Error(err)
	}

	return ddl, nil
}

// makeABFSClient creates an ABFS filesystem client for OneLake
func (conn *MsFabricConn) makeABFSClient() (fs filesys.FileSysClient, err error) {
	// Get ABFS properties from connection
	abfsProps := []string{
		"type=abfs",
	}

	// Required properties
	if endpoint := conn.GetProp("abfs_endpoint"); endpoint != "" {
		abfsProps = append(abfsProps, "endpoint="+endpoint)
		account := strings.TrimSuffix(endpoint, ".dfs.fabric.microsoft.com")
		abfsProps = append(abfsProps, "account="+account)
	} else {
		return nil, g.Error("Property 'abfs_endpoint' is required for Fabric bulk operations")
	}

	if filesystem := conn.GetProp("abfs_filesystem"); filesystem != "" {
		abfsProps = append(abfsProps, "filesystem="+filesystem)
	} else {
		return nil, g.Error("Property 'abfs_filesystem' is required for Fabric bulk operations")
	}

	// Optional properties
	if parent := conn.GetProp("abfs_parent"); parent != "" {
		abfsProps = append(abfsProps, "parent="+parent)
	}

	// Authentication properties - pass through from connection
	for _, key := range []string{"ACCOUNT_KEY", "SAS_SVC_URL", "CLIENT_ID", "TENANT_ID", "CLIENT_SECRET"} {
		if val := conn.GetProp(key); val != "" {
			abfsProps = append(abfsProps, key+"="+val)
		}
	}

	// Create ABFS client
	fs, err = filesys.NewFileSysClient(dbio.TypeFileAzureABFS, abfsProps...)
	if err != nil {
		return nil, g.Error(err, "Could not create ABFS client for OneLake")
	}

	return fs, nil
}

// getOneLakePath generates a OneLake path for temporary staging
func (conn *MsFabricConn) getOneLakePath(tableFName string) string {
	endpoint := conn.GetProp("abfs_endpoint")
	filesystem := conn.GetProp("abfs_filesystem")
	parent := conn.GetProp("abfs_parent")

	// Build base path - Format: https://onelake.dfs.fabric.microsoft.com/<workspace>/<lakehouse>.Lakehouse/Files/
	basePath := fmt.Sprintf("https://%s/%s", endpoint, filesystem)
	if parent != "" {
		basePath = fmt.Sprintf("%s/%s", basePath, parent)
	}

	// Add temp staging folder with timestamp
	// Clean table name to remove quotes and schema prefix
	cleanTableName := env.CleanTableName(tableFName)
	return fmt.Sprintf("%s/%s/%s/%s", basePath, tempCloudStorageFolder, cleanTableName, cast.ToString(g.Now()))
}

// CopyFromOneLake uses the COPY INTO command to load data from OneLake
func (conn *MsFabricConn) CopyFromOneLake(tableFName, oneLakePath string, columns iop.Columns, fileFormat dbio.FileType) (err error) {
	// Prepare target columns
	tgtColumns := conn.GetType().QuoteNames(columns.Names()...)

	// Add wildcard pattern to load all files in the directory
	// COPY INTO requires a file pattern, not a directory path
	wildcardPath := oneLakePath + "/*"

	// Prepare credential expression
	credentialExpr := ""
	if accountKey := conn.GetProp("ACCOUNT_KEY"); accountKey != "" {
		// Using Storage Account Key
		credentialExpr = fmt.Sprintf(",\n    CREDENTIAL = (IDENTITY = 'Storage Account Key', SECRET = '%s')", accountKey)
	} else if sasURL := conn.GetProp("SAS_SVC_URL"); sasURL != "" {
		// Extract SAS token from URL
		parts := strings.Split(sasURL, "?")
		if len(parts) == 2 {
			credentialExpr = fmt.Sprintf(",\n    CREDENTIAL = (IDENTITY = 'Shared Access Signature', SECRET = '%s')", parts[1])
		}
	}
	// If neither is provided, EntraID authentication will be used (default, no credential needed)

	// Select template based on format
	templateKey := fmt.Sprintf("copy_from_onelake_%s", fileFormat.String())

	g.Info("copying into fabric warehouse from onelake")

	sql := g.R(
		conn.template.Core[templateKey],
		"table", tableFName,
		"tgt_columns", strings.Join(tgtColumns, ", "),
		"onelake_path", wildcardPath,
		"credential_expr", credentialExpr,
	)

	_, err = conn.Exec(sql)
	if err != nil {
		return g.Error(err, "Error with COPY INTO from OneLake")
	}

	return nil
}

// BulkExportFlow exports data using cursor (OneLake export not yet supported)
func (conn *MsFabricConn) BulkExportFlow(table Table) (*iop.Dataflow, error) {
	// TODO: Implement OneLake export using external tables or CETAS
	// For now, fall back to cursor-based export
	return conn.BaseConn.BulkExportFlow(table)
}

// BulkImportFlow bulk imports using OneLake staging
func (conn *MsFabricConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	defer df.CleanUp()

	// Check if ABFS properties are provided
	if conn.GetProp("abfs_endpoint") == "" || conn.GetProp("abfs_filesystem") == "" {
		g.Warn("Fabric bulk import: ABFS properties not set, falling back to standard insert")
		// Fall back to standard insert behavior
		for ds := range df.StreamCh {
			c, err := conn.BaseConn.InsertBatchStream(tableFName, ds)
			if err != nil {
				return 0, g.Error(err, "could not insert")
			}
			count += c
		}
		return count, nil
	}

	settingMppBulkImportFlow(conn, iop.GzipCompressorType)

	// Get OneLake path
	oneLakePath := conn.getOneLakePath(tableFName)

	// Create ABFS client
	abfsFs, err := conn.makeABFSClient()
	if err != nil {
		return 0, g.Error(err, "Could not create ABFS client")
	}

	// Delete any existing files at path
	err = filesys.Delete(abfsFs, oneLakePath)
	if err != nil {
		return df.Count(), g.Error(err, "Could not delete existing files: "+oneLakePath)
	}

	// Set up cleanup
	df.Defer(func() {
		if !cast.ToBool(os.Getenv("SLING_KEEP_TEMP")) {
			filesys.Delete(abfsFs, oneLakePath)
		}
	})

	g.Info("writing to onelake for fabric import")

	// Determine file format
	fileFormat := dbio.FileType(conn.GetProp("format"))
	if !g.In(fileFormat, dbio.FileTypeCsv, dbio.FileTypeParquet) {
		fileFormat = dbio.FileTypeParquet
	}

	abfsFs.SetProp("format", fileFormat.String())
	abfsFs.SetProp("file_max_rows", `500000`)

	var bw int64
	switch fileFormat {
	case dbio.FileTypeCsv:
		// Fabric COPY INTO treats empty fields as NULL (no NULL_IF parameter available)
		abfsFs.SetProp("null_as", ``)
		abfsFs.SetProp("compression", `gzip`)
		bw, err = filesys.WriteDataflow(abfsFs, df, oneLakePath)
	case dbio.FileTypeParquet:
		if env.UseDuckDbCompute() {
			bw, err = filesys.WriteDataflowViaDuckDB(abfsFs, df, oneLakePath)
		} else {
			bw, err = filesys.WriteDataflow(abfsFs, df, oneLakePath)
		}
	}
	if err != nil {
		return df.Count(), g.Error(err, "Error writing to OneLake")
	}
	g.Debug("total written: %s to %s", humanize.Bytes(cast.ToUint64(bw)), oneLakePath)

	// Execute COPY INTO
	err = conn.CopyFromOneLake(tableFName, oneLakePath, df.Columns, fileFormat)
	if err != nil {
		return df.Count(), g.Error(err, "Error copying into Fabric from OneLake")
	}

	return df.Count(), nil
}

// BulkImportStream bulk imports a stream using OneLake staging
func (conn *MsFabricConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	df, err := iop.MakeDataFlow(ds)
	if err != nil {
		return 0, g.Error(err, "Could not MakeDataFlow")
	}

	return conn.BulkImportFlow(tableFName, df)
}

func (conn *MsFabricConn) AddMissingColumns(table Table, newCols iop.Columns) (ok bool, err error) {
	// cannot have transaction lock table while adding columns
	if err = conn.Commit(); err != nil {
		return false, g.Error(err, "could not commit in order to add column(s): %s", g.Marshal(newCols.Names()))
	}

	ok, err = conn.BaseConn.AddMissingColumns(table, newCols)
	if err != nil {
		return
	}

	if err = conn.Begin(); err != nil {
		return false, g.Error(err, "could not begin new transaction after adding column(s): %s", g.Marshal(newCols.Names()))
	}

	return
}

// CastColumnForSelect casts to the correct target column type
func (conn *MsFabricConn) CastColumnForSelect(srcCol iop.Column, tgtCol iop.Column) (selectStr string) {
	qName := conn.Self().Quote(srcCol.Name)
	srcDbType := strings.ToLower(srcCol.DbType)
	tgtDbType := strings.ToLower(tgtCol.DbType)

	switch {
	// maintain lower case if inserting from uniqueidentifier into varchar
	case srcDbType == "uniqueidentifier" && tgtDbType == "varchar":
		selectStr = g.F("cast(%s as varchar(max))", qName)
	case srcCol.IsString() && tgtCol.IsInteger():
		// assume bool, convert from true/false to 1/0
		sql := `case when {col} = 'true' then 1 when {col} = 'false' then 0 else cast({col} as bigint) end`
		selectStr = g.R(sql, "col", qName)
	default:
		selectStr = qName
	}

	return selectStr
}
