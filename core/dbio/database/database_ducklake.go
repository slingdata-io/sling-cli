package database

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// DuckLake provides versioned, ACID-compliant tables on top of DuckDB
// It supports multiple catalog backends (DuckDB, SQLite, Postgres, MySQL)
// and various storage backends (S3, Azure, GCS, local filesystem)
// For more details see: https://ducklake.select/docs/stable/duckdb/introduction

// DuckLakeConn is a Ducklake connection
type DuckLakeConn struct {
	DuckDbConn

	// Catalog database configuration
	CatalogType    string // duckdb, sqlite, postgres, mysql
	CatalogSchema  string // schema to use in postgres or mysql
	CatalogConnStr string // connection string for catalog database
	DataPath       string // path to data files (local or cloud storage)
	Database       string // the database name to attached
	Encrypted      bool   // whether data is written using Parquet encryption
	InliningLimit  int    // where to enable data inlining when attaching
}

// Init initiates the object
func (conn *DuckLakeConn) Init() error {

	// Extract data path from properties
	conn.CatalogType = conn.GetProp("catalog_type")
	conn.CatalogSchema = conn.GetProp("catalog_schema")
	conn.InliningLimit = cast.ToInt(conn.GetProp("data_inlining_limit"))
	conn.CatalogConnStr = conn.GetProp("catalog_conn_string")
	conn.DataPath = conn.GetProp("data_path")
	conn.Database = "ducklake" // for hard-coded  '__ducklake_metadata_ducklake'
	conn.Encrypted = cast.ToBool(conn.GetProp("encrypted"))

	// Set default catalog type if not specified
	if conn.CatalogType == "" {
		conn.CatalogType = "duckdb"
	}

	if conn.CatalogConnStr == "" {
		return g.Error("did not provide 'catalog_conn_string'")
	}

	if _, ok := conn.properties["copy_method"]; !ok {
		conn.SetProp("copy_method", "arrow_http") // default to arrow_http
	}

	// Set base properties
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbDuckLake

	// Initialize DuckDB instance (inheriting from DuckDbConn)
	conn.duck = iop.NewDuckDb(conn.Context().Ctx, g.MapToKVArr(conn.properties)...)

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Connect establishes the DuckLake connection
func (conn *DuckLakeConn) Connect(timeOut ...int) (err error) {
	// First, connect to DuckDB as the base connection
	// For DuckLake, we always use an in-memory DuckDB instance
	// The actual data is managed through the DuckLake extension
	conn.DuckDbConn.URL = "duckdb::memory:"

	if err = conn.DuckDbConn.Connect(timeOut...); err != nil {
		return g.Error(err, "could not connect to DuckDB for DuckLake")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	// Add required extensions using the DuckDb instance
	// DuckLake is the table format extension that provides versioned, ACID transactions on DuckDB
	conn.duck.AddExtension("ducklake")

	// Add catalog-specific extensions if needed
	switch conn.CatalogType {
	case "sqlite":
		conn.duck.AddExtension("sqlite")
	case "postgres":
		conn.duck.AddExtension("postgres")
	case "mysql":
		conn.duck.AddExtension("mysql")
	}

	// Add storage-specific extensions based on data path
	var secret iop.DuckDbSecret

	switch {
	case strings.HasPrefix(conn.DataPath, "s3://"), strings.HasPrefix(conn.DataPath, "r2://"), conn.GetProp("s3_region") != "", conn.GetProp("s3_endpoint") != "":
		secretType := iop.DuckDbSecretTypeS3
		if strings.HasPrefix(conn.DataPath, "r2://") {
			secretType = iop.DuckDbSecretTypeR2
		}
		secretProps := MakeDuckDbSecretProps(conn, secretType)
		secret = iop.NewDuckDbSecret("s3_secret", secretType, secretProps)
		conn.duck.AddSecret(secret)

	case strings.HasPrefix(conn.DataPath, "az://"), strings.HasPrefix(conn.DataPath, "abfss://"), conn.GetProp("azure_account_name") != "", conn.GetProp("azure_account_key") != "", conn.GetProp("azure_sas_token") != "", conn.GetProp("azure_connection_string") != "":
		secretType := iop.DuckDbSecretTypeAzure
		secretProps := MakeDuckDbSecretProps(conn, secretType)
		secret = iop.NewDuckDbSecret("azure_secret", secretType, secretProps)
		conn.duck.AddSecret(secret)

	case strings.HasPrefix(conn.DataPath, "gs://"), strings.HasPrefix(conn.DataPath, "gcs://"), conn.GetProp("gcs_access_key_id") != "", conn.GetProp("gcs_secret_access_key") != "":
		secretType := iop.DuckDbSecretTypeGCS
		secretProps := MakeDuckDbSecretProps(conn, secretType)
		secret = iop.NewDuckDbSecret("gcs_secret", secretType, secretProps)
		conn.duck.AddSecret(secret)

	case conn.DataPath != "":
		// ensure dir is created
		os.MkdirAll(conn.DataPath, 0775)
	}

	// if catalog type is sqlite, create folder first
	if conn.CatalogType == "sqlite" {
		parentDir := filepath.Dir(conn.CatalogConnStr)
		if err := os.MkdirAll(parentDir, 0775); err != nil {
			g.Warn("could not create parent folder for sqlite catalog file: %s", err.Error())
		}
	}

	// Attach the DuckLake database
	attachSQL := conn.buildAttachSQL()
	_, err = conn.Exec(attachSQL + noDebugKey)
	if err != nil {
		// hide sensitive strings (password)
		if strings.Contains(err.Error(), conn.CatalogConnStr) {
			return g.Error(strings.ReplaceAll(err.Error(), conn.CatalogConnStr, "<REDACTED CONNECTION STRING>"))
		}
		return g.Error(err, "could not attach ducklake database")
	}

	// Use the attached database by default
	_, err = conn.Exec(fmt.Sprintf("USE %s;", conn.Database) + noDebugKey)
	if err != nil {
		return g.Error(err, "could not use ducklake database")
	}

	return nil
}

// buildAttachSQL creates the ATTACH statement based on catalog type
func (conn *DuckLakeConn) buildAttachSQL() string {
	var attachSQL string
	switch conn.CatalogType {
	case "sqlite":
		attachSQL = fmt.Sprintf("ATTACH IF NOT EXISTS 'ducklake:sqlite:%s' AS %s", conn.CatalogConnStr, conn.Database)
	case "postgres":
		attachSQL = fmt.Sprintf("ATTACH IF NOT EXISTS 'ducklake:postgres:%s' AS %s", conn.CatalogConnStr, conn.Database)
	case "mysql":
		attachSQL = fmt.Sprintf("ATTACH IF NOT EXISTS 'ducklake:mysql:%s' AS %s", conn.CatalogConnStr, conn.Database)
	default: // duckdb
		attachSQL = fmt.Sprintf("ATTACH IF NOT EXISTS 'ducklake:%s' AS %s", conn.CatalogConnStr, conn.Database)
	}

	// Add data path if specified
	metaParts := []string{}
	if conn.DataPath != "" {
		metaParts = append(metaParts, g.F("DATA_PATH '%s'", conn.DataPath))
	}
	if conn.CatalogSchema != "" {
		metaParts = append(metaParts, g.F("META_SCHEMA '%s'", conn.CatalogSchema))
	}
	if conn.InliningLimit > 0 {
		metaParts = append(metaParts, g.F("DATA_INLINING_ROW_LIMIT %d", conn.InliningLimit))
	}
	if conn.Encrypted {
		metaParts = append(metaParts, "ENCRYPTED")
	}
	if len(metaParts) > 0 {
		attachSQL += fmt.Sprintf(" (%s)", strings.Join(metaParts, ", "))
	}

	return attachSQL
}

// GetURL returns the processed URL
func (conn *DuckLakeConn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	return connURL
}

// GenerateMergeSQL generates the upsert SQL for DuckLake
func (conn *DuckLakeConn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	// DuckLake supports transactions and proper ACID operations
	// For now, use the same approach as DuckDB
	return conn.DuckDbConn.GenerateMergeSQL(srcTable, tgtTable, pkFields)
}

func (conn *DuckLakeConn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]any) (data iop.Dataset, err error) {

	schemaName := cast.ToString(values["schema"])
	tableName := cast.ToString(values["table"])
	tablesExpr := cast.ToString(values["tables"])

	// point to different template if is a temp table
	if name == "columns" && strings.HasSuffix(tableName, "_sling_duckdb_tmp") {
		name = "columns_temp"
	}

	template, ok := templateMap[name]
	if !ok {
		err = g.Error("Could not find template %s", name)
		return
	}

	template = strings.TrimSpace(template) + noDebugKey
	sql, err := conn.ProcessTemplate(level, template, values)
	if err != nil {
		err = g.Error(err, "error processing template")
		return
	}

	data, err = conn.Self().Query(sql)
	if err != nil {
		return
	}

	// if no columns returned, use describe to get columns (especially for views)
	if len(data.Rows) == 0 && name == "columns" && tableName != "" {
		view := Table{Schema: schemaName, Name: tableName, Dialect: dbio.TypeDbDuckLake}
		cols, err := conn.duck.Describe("select * from " + view.FullName())
		if err != nil {
			return data, g.Error(err, "could not describe "+view.FullName())
		}

		data.Columns = iop.NewColumnsFromFields("column_name", "data_type", "precision", "scale")
		for _, col := range cols {
			data.Rows = append(data.Rows, []any{col.Name, col.DbType, col.DbPrecision, col.DbScale})
		}
	}

	// use describe to get schemata columns for views
	if name == "schemata" && tablesExpr != "" {
		tables := strings.Split(tablesExpr, ", ")
		if len(data.Columns) == 0 {
			data.Columns = iop.NewColumnsFromFields("schema_name", "table_name", "is_view", "column_name", "data_type", "position")
		}

		for _, table := range tables {
			viewName := strings.Trim(table, "'")

			// check that it is a view
			viewsData, err := conn.GetViews(schemaName)
			if err != nil {
				return data, g.Error(err, "could not get views")
			} else {
				viewsData = viewsData.Pick("table_name")
				viewNames := lo.Map(viewsData.ColValues(0), func(v any, i int) string {
					return cast.ToString(v)
				})

				if !g.In(viewName, viewNames...) {
					continue // not a view
				}
			}

			view := Table{Schema: schemaName, Name: viewName, Dialect: dbio.TypeDbDuckLake}
			cols, err := conn.duck.Describe("select * from " + view.FullName())
			if err != nil {
				return data, g.Error(err, "could not describe "+view.FullName())
			}

			for i, col := range cols {
				data.Rows = append(data.Rows, []any{schemaName, viewName, true, col.Name, col.DbType, i + 1})
			}
		}
	}

	return
}

// Close closes the DuckLake connection
func (conn *DuckLakeConn) Close() error {
	// Clean up any DuckLake-specific resources
	// Then close the underlying DuckDB connection
	return conn.DuckDbConn.Close()
}
