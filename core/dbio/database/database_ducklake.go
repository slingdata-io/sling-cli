package database

import (
	"fmt"
	"os"
	"strings"

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
	CatalogConnStr string // connection string for catalog database
	DataPath       string // path to data files (local or cloud storage)
	Database       string // the database name to attached
}

// Init initiates the object
func (conn *DuckLakeConn) Init() error {

	// Extract data path from properties
	conn.CatalogType = conn.GetProp("catalog_type")
	conn.CatalogConnStr = conn.GetProp("catalog_conn_string")
	conn.DataPath = conn.GetProp("data_path")
	conn.Database = "ducklake" // for hard-coded  '__ducklake_metadata_ducklake'

	// Set default catalog type if not specified
	if conn.CatalogType == "" {
		conn.CatalogType = "duckdb"
	}

	if conn.CatalogConnStr == "" {
		return g.Error("did not provide 'catalog_conn_string'")
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
	if conn.DataPath != "" {
		if strings.HasPrefix(conn.DataPath, "s3://") || strings.HasPrefix(conn.DataPath, "r2://") {
			conn.duck.AddExtension("httpfs")
		} else if strings.HasPrefix(conn.DataPath, "az://") || strings.HasPrefix(conn.DataPath, "abfss://") {
			conn.duck.AddExtension("azure")
		} else if strings.HasPrefix(conn.DataPath, "gs://") || strings.HasPrefix(conn.DataPath, "gcs://") {
			conn.duck.AddExtension("httpfs")
		} else {
			// ensure dir is created
			os.MkdirAll(conn.DataPath, 0775)
		}
	}

	// Attach the DuckLake database
	attachSQL := conn.buildAttachSQL()
	_, err = conn.Exec(attachSQL + noDebugKey)
	if err != nil {
		return g.Error(err, "could not attach ducklake database")
	}

	// Configure storage credentials using DuckDB's PrepareFsSecretAndURI method
	if conn.DataPath != "" {
		// Prepare fs_props from connection properties for storage credential configuration
		fsProps := map[string]string{}

		// Map DuckLake connection properties to fs_props format
		if conn.GetProp("s3_access_key_id") != "" {
			fsProps["ACCESS_KEY_ID"] = conn.GetProp("s3_access_key_id")
		}
		if conn.GetProp("s3_secret_access_key") != "" {
			fsProps["SECRET_ACCESS_KEY"] = conn.GetProp("s3_secret_access_key")
		}
		if conn.GetProp("s3_session_token") != "" {
			fsProps["SESSION_TOKEN"] = conn.GetProp("s3_session_token")
		}
		if conn.GetProp("s3_region") != "" {
			fsProps["REGION"] = conn.GetProp("s3_region")
		}
		if conn.GetProp("s3_endpoint") != "" {
			fsProps["ENDPOINT"] = conn.GetProp("s3_endpoint")
		}
		if conn.GetProp("s3_profile") != "" {
			fsProps["PROFILE"] = conn.GetProp("s3_profile")
		}

		// Azure credentials
		if conn.GetProp("azure_account_name") != "" {
			fsProps["ACCOUNT"] = conn.GetProp("azure_account_name")
		}
		if conn.GetProp("azure_account_key") != "" {
			fsProps["ACCOUNT_KEY"] = conn.GetProp("azure_account_key")
		}
		if conn.GetProp("azure_sas_token") != "" {
			fsProps["SAS_TOKEN"] = conn.GetProp("azure_sas_token")
		}
		if conn.GetProp("azure_tenant_id") != "" {
			fsProps["TENANT_ID"] = conn.GetProp("azure_tenant_id")
		}
		if conn.GetProp("azure_client_id") != "" {
			fsProps["CLIENT_ID"] = conn.GetProp("azure_client_id")
		}
		if conn.GetProp("azure_client_secret") != "" {
			fsProps["CLIENT_SECRET"] = conn.GetProp("azure_client_secret")
		}
		if conn.GetProp("azure_connection_string") != "" {
			fsProps["CONN_STR"] = conn.GetProp("azure_connection_string")
		}

		// GCS credentials - HMAC keys for interoperability with S3 API
		if conn.GetProp("gcs_access_key_id") != "" {
			fsProps["ACCESS_KEY_ID"] = conn.GetProp("gcs_access_key_id")
		}
		if conn.GetProp("gcs_secret_access_key") != "" {
			fsProps["SECRET_ACCESS_KEY"] = conn.GetProp("gcs_secret_access_key")
		}
		// Note: For service account key file, DuckDB expects it to be set via environment
		// variables or credential chain provider, not as a direct secret parameter

		// Set fs_props on the duck instance
		conn.duck.SetProp("fs_props", g.Marshal(fsProps))

		// Use DuckDB's PrepareFsSecretAndURI to configure storage secrets
		_ = conn.duck.PrepareFsSecretAndURI(conn.DataPath)
	}

	// Use the attached database by default
	_, err = conn.Exec(fmt.Sprintf("USE %s;", conn.Database) + noDebugKey)
	if err != nil {
		return g.Error(err, "could not use ducklake database")
	}

	g.Debug(`opened "%s" connection (%s)`, conn.Type, conn.GetProp("sling_conn_id"))

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
	if conn.DataPath != "" {
		attachSQL += fmt.Sprintf(" (DATA_PATH '%s')", conn.DataPath)
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

// GenerateUpsertSQL generates the upsert SQL for DuckLake
func (conn *DuckLakeConn) GenerateUpsertSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	// DuckLake supports transactions and proper ACID operations
	// For now, use the same approach as DuckDB
	return conn.DuckDbConn.GenerateUpsertSQL(srcTable, tgtTable, pkFields)
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

// GetDatabases returns the available databases
func (conn *DuckLakeConn) GetDatabases() (iop.Dataset, error) {
	// For DuckLake, we need to query the attached databases
	sql := "SELECT database_name FROM duckdb_databases() WHERE database_name != 'system' ORDER BY database_name" + noDebugKey
	return conn.Query(sql)
}

// GetSchemas returns schemas for DuckLake
func (conn *DuckLakeConn) GetSchemas() (iop.Dataset, error) {
	// Use the current database to get schemas
	sql := `
		SELECT DISTINCT schema_name
		FROM information_schema.schemata
		WHERE catalog_name = current_database()
		ORDER BY schema_name
	` + noDebugKey
	return conn.Query(sql)
}

// CurrentDatabase returns the current database name
func (conn *DuckLakeConn) CurrentDatabase() (string, error) {
	data, err := conn.SubmitTemplate("single", conn.template.Metadata, "current_database", g.M())
	if err != nil {
		err = g.Error(err, "could not get current database")
	} else {
		dbName := cast.ToString(data.FirstVal())
		return dbName, nil
	}

	return "", err
}

// Close closes the DuckLake connection
func (conn *DuckLakeConn) Close() error {
	// Clean up any DuckLake-specific resources
	// Then close the underlying DuckDB connection
	return conn.DuckDbConn.Close()
}
