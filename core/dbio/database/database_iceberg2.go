package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// Iceberg2Conn is an Iceberg connection using DuckDB's native Iceberg extension
// This provides full SQL support including updates, deletes, and ACID transactions
// Supported catalog types: REST, Glue (SageMaker Lakehouse), S3 Tables
// For more details see: https://duckdb.org/docs/stable/core_extensions/iceberg/overview
type Iceberg2Conn struct {
	DuckDbConn

	CatalogType string // rest, glue, s3tables
	CatalogName string // Name for the attached catalog (default: "iceberg")
	Warehouse   string // Warehouse path (for REST catalogs)
	Endpoint    string // REST endpoint URL

	// REST catalog specific
	Token             string // Bearer token
	OAuthClientID     string // OAuth2 client ID
	OAuthClientSecret string // OAuth2 client secret
	OAuthServerURI    string // OAuth2 server URI
	OAuthScope        string // OAuth2 scope

	// Glue catalog specific
	GlueAccountID string // AWS account ID
	GlueRegion    string // AWS region

	// S3 Tables specific
	S3TablesARN string // S3 Tables ARN
}

// Init initiates the object
func (conn *Iceberg2Conn) Init() error {

	// Extract catalog properties
	conn.CatalogType = strings.ToLower(conn.GetProp("catalog_type"))
	conn.CatalogName = conn.GetProp("catalog_name")
	if conn.CatalogName == "" {
		conn.CatalogName = "iceberg"
	}

	// REST catalog properties
	conn.Endpoint = conn.GetProp("rest_uri")
	conn.Warehouse = conn.GetProp("rest_warehouse")
	conn.Token = conn.GetProp("rest_token")
	conn.OAuthClientID = conn.GetProp("rest_oauth_client_id")
	conn.OAuthClientSecret = conn.GetProp("rest_oauth_client_secret")
	conn.OAuthServerURI = conn.GetProp("rest_oauth_server_uri")
	conn.OAuthScope = conn.GetProp("rest_oauth_scope")

	// Glue catalog properties
	conn.GlueAccountID = conn.GetProp("glue_account_id")
	conn.GlueRegion = conn.GetProp("glue_region")
	if conn.GlueRegion == "" {
		conn.GlueRegion = conn.GetProp("s3_region") // fallback to S3 region
	}

	// S3 Tables properties - check s3_tables_arn first, then fall back to rest_warehouse
	conn.S3TablesARN = conn.GetProp("s3_tables_arn")
	if conn.S3TablesARN == "" && strings.HasPrefix(conn.Warehouse, "arn:aws:s3tables") {
		conn.S3TablesARN = conn.Warehouse
	}

	// Auto-detect catalog type from ARN if not explicitly set or if set to 'rest' but has S3Tables ARN
	if strings.HasPrefix(conn.S3TablesARN, "arn:aws:s3tables") {
		conn.CatalogType = "s3tables"

		// Extract region from S3 Tables ARN: arn:aws:s3tables:REGION:ACCOUNT:bucket/NAME
		parts := strings.Split(conn.S3TablesARN, ":")
		if len(parts) >= 4 {
			arnRegion := parts[3]
			existingRegion := conn.GetProp("s3_region")
			if existingRegion != "" && existingRegion != arnRegion {
				return g.Error("ARN region does not match 's3_region' property: %s != %s", arnRegion, existingRegion)
			}
			conn.SetProp("s3_region", arnRegion)
		}
	}

	// Validate required properties based on catalog type
	switch conn.CatalogType {
	case "rest":
		if conn.Endpoint == "" {
			return g.Error("REST catalog requires 'rest_uri' property")
		}
	case "glue":
		if conn.GlueRegion == "" {
			return g.Error("Glue catalog requires 'glue_region' or 's3_region' property")
		}
		// Auto-detect AWS account ID if not provided
		if conn.GlueAccountID == "" {
			accountID, err := conn.getAWSAccountID()
			if err != nil {
				return g.Error(err, "Glue catalog requires 'glue_account_id' property or valid AWS credentials to auto-detect it")
			}
			conn.GlueAccountID = accountID
		}
	case "s3tables":
		if conn.S3TablesARN == "" {
			return g.Error("S3 Tables catalog requires 's3_tables_arn' property")
		}
	default:
		if conn.CatalogType == "" {
			return g.Error("catalog_type is required (rest, glue, or s3tables)")
		}
		return g.Error("unsupported catalog_type: %s (supported: rest, glue, s3tables)", conn.CatalogType)
	}

	// Set default copy method
	if _, ok := conn.properties["copy_method"]; !ok {
		conn.SetProp("copy_method", "arrow_http")
	}

	// Set base properties
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbIceberg2

	// Initialize DuckDB instance
	conn.duck = iop.NewDuckDb(conn.Context().Ctx, g.MapToKVArr(conn.properties)...)

	instance := Connection(conn)
	conn.BaseConn.instance = &instance

	return conn.BaseConn.Init()
}

// Connect establishes the Iceberg connection via DuckDB
func (conn *Iceberg2Conn) Connect(timeOut ...int) (err error) {
	// Connect to DuckDB as the base connection (in-memory)
	conn.DuckDbConn.URL = "duckdb::memory:"

	if err = conn.DuckDbConn.Connect(timeOut...); err != nil {
		return g.Error(err, "could not connect to DuckDB for Iceberg2")
	}

	conn.SetProp("connected", "true")
	conn.SetProp("connect_time", cast.ToString(time.Now()))

	// Add iceberg extension
	conn.duck.AddExtension("iceberg")

	// Configure secrets based on catalog type
	if err = conn.configureSecrets(); err != nil {
		return g.Error(err, "could not configure secrets for Iceberg2")
	}

	// Attach the Iceberg catalog
	attachSQL := conn.buildAttachSQL()
	_, err = conn.Exec(attachSQL + env.NoDebugKey)
	if err != nil {
		return g.Error(err, "could not attach Iceberg catalog")
	}

	// Get available schemas from the attached catalog
	data, err := conn.Query(fmt.Sprintf(`
		SELECT DISTINCT schema_name
		FROM information_schema.schemata
		WHERE catalog_name = '%s'
		ORDER BY schema_name
	`, conn.CatalogName) + env.NoDebugKey)
	if err != nil {
		return g.Error(err, "could not query schemas from Iceberg catalog")
	}

	// Use the first available schema (typically 'public' or 'default')
	if len(data.Rows) > 0 {
		defaultSchema := cast.ToString(data.Rows[0][0])
		_, err = conn.Exec(fmt.Sprintf("USE %s.%s;", conn.CatalogName, defaultSchema) + env.NoDebugKey)
		if err != nil {
			return g.Error(err, "could not use Iceberg catalog schema")
		}
	}

	return nil
}

// configureSecrets sets up the required DuckDB secrets based on catalog type
func (conn *Iceberg2Conn) configureSecrets() error {

	// Configure Iceberg catalog secret for REST catalogs
	if conn.CatalogType == "rest" {
		secretProps := map[string]string{}

		// Bearer token authentication
		if conn.Token != "" {
			secretProps["TOKEN"] = conn.Token
		}

		// OAuth2 authentication
		if conn.OAuthClientID != "" {
			secretProps["CLIENT_ID"] = conn.OAuthClientID
		}
		if conn.OAuthClientSecret != "" {
			secretProps["CLIENT_SECRET"] = conn.OAuthClientSecret
		}
		if conn.OAuthServerURI != "" {
			secretProps["OAUTH2_SERVER_URI"] = conn.OAuthServerURI
		}
		if conn.OAuthScope != "" {
			secretProps["OAUTH2_SCOPE"] = conn.OAuthScope
		}

		// Only create secret if we have authentication details
		if len(secretProps) > 0 {
			secret := iop.NewDuckDbSecret("iceberg_secret", iop.DuckDbSecretTypeIceberg, secretProps)
			conn.duck.AddSecret(secret)
		}
	}

	// Configure S3 storage secret for AWS-based catalogs (Glue, S3Tables) and REST with S3 storage
	if conn.hasS3Credentials() || conn.CatalogType == "glue" || conn.CatalogType == "s3tables" {
		secretProps := MakeDuckDbSecretProps(conn, iop.DuckDbSecretTypeS3)
		if len(secretProps) > 0 {
			secret := iop.NewDuckDbSecret("s3_secret", iop.DuckDbSecretTypeS3, secretProps)
			conn.duck.AddSecret(secret)
		}
	}

	// Configure Azure storage secret if Azure credentials are provided
	if conn.hasAzureCredentials() {
		secretProps := MakeDuckDbSecretProps(conn, iop.DuckDbSecretTypeAzure)
		if len(secretProps) > 0 {
			secret := iop.NewDuckDbSecret("azure_secret", iop.DuckDbSecretTypeAzure, secretProps)
			conn.duck.AddSecret(secret)
		}
	}

	// Configure GCS storage secret if GCS credentials are provided
	if conn.hasGCSCredentials() {
		secretProps := MakeDuckDbSecretProps(conn, iop.DuckDbSecretTypeGCS)
		if len(secretProps) > 0 {
			secret := iop.NewDuckDbSecret("gcs_secret", iop.DuckDbSecretTypeGCS, secretProps)
			conn.duck.AddSecret(secret)
		}
	}

	return nil
}

// hasS3Credentials checks if S3 credentials are configured
func (conn *Iceberg2Conn) hasS3Credentials() bool {
	return conn.GetProp("s3_access_key_id") != "" ||
		conn.GetProp("s3_region") != "" ||
		conn.GetProp("s3_endpoint") != ""
}

// hasAzureCredentials checks if Azure credentials are configured
func (conn *Iceberg2Conn) hasAzureCredentials() bool {
	return conn.GetProp("azure_account_name") != "" ||
		conn.GetProp("azure_account_key") != "" ||
		conn.GetProp("azure_sas_token") != "" ||
		conn.GetProp("azure_connection_string") != ""
}

// hasGCSCredentials checks if GCS credentials are configured
func (conn *Iceberg2Conn) hasGCSCredentials() bool {
	return conn.GetProp("gcs_access_key_id") != "" ||
		conn.GetProp("gcs_secret_access_key") != ""
}

// getAWSAccountID retrieves the AWS account ID using STS GetCallerIdentity
func (conn *Iceberg2Conn) getAWSAccountID() (string, error) {
	accessKeyID := conn.GetProp("s3_access_key_id")
	secretAccessKey := conn.GetProp("s3_secret_access_key")
	sessionToken := conn.GetProp("s3_session_token")
	region := conn.GlueRegion
	if region == "" {
		region = "us-east-1" // Default region for STS
	}

	if accessKeyID == "" || secretAccessKey == "" {
		return "", g.Error("AWS credentials (s3_access_key_id, s3_secret_access_key) required to auto-detect account ID")
	}

	// Create AWS config with static credentials
	cfg := aws.Config{
		Region: region,
		Credentials: credentials.NewStaticCredentialsProvider(
			accessKeyID,
			secretAccessKey,
			sessionToken,
		),
	}

	// Create STS client and get caller identity
	stsClient := sts.NewFromConfig(cfg)
	result, err := stsClient.GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", g.Error(err, "failed to get AWS account ID via STS GetCallerIdentity")
	}

	if result.Account == nil {
		return "", g.Error("STS GetCallerIdentity returned nil account ID")
	}

	g.Debug("iceberg2: auto-detected AWS account ID: %s", *result.Account)
	return *result.Account, nil
}

// buildAttachSQL creates the ATTACH statement based on catalog type
func (conn *Iceberg2Conn) buildAttachSQL() string {
	var attachSQL string
	attachParts := []string{}

	switch conn.CatalogType {
	case "rest":
		// REST catalog
		// ATTACH 'warehouse' AS catalog_name (TYPE iceberg, SECRET iceberg_secret, ENDPOINT 'https://...')
		warehouse := conn.Warehouse
		if warehouse == "" {
			warehouse = "" // empty string is valid for some REST catalogs
		}

		attachSQL = fmt.Sprintf("ATTACH '%s' AS %s", warehouse, conn.CatalogName)
		attachParts = append(attachParts, "TYPE iceberg")

		// Add secret reference if we have authentication
		if conn.Token != "" || conn.OAuthClientID != "" {
			attachParts = append(attachParts, "SECRET iceberg_secret")
		}

		// Add endpoint
		if conn.Endpoint != "" {
			attachParts = append(attachParts, fmt.Sprintf("ENDPOINT '%s'", conn.Endpoint))
		}

	case "glue":
		// AWS Glue (SageMaker Lakehouse) catalog
		// ATTACH 'account_id' AS catalog_name (TYPE iceberg, ENDPOINT_TYPE 'glue', SECRET s3_secret)
		attachSQL = fmt.Sprintf("ATTACH '%s' AS %s", conn.GlueAccountID, conn.CatalogName)
		attachParts = append(attachParts, "TYPE iceberg")
		attachParts = append(attachParts, "ENDPOINT_TYPE 'glue'")
		attachParts = append(attachParts, "SECRET s3_secret")
		attachParts = append(attachParts, "PURGE_REQUESTED false")

	case "s3tables":
		// Amazon S3 Tables
		// ATTACH 'arn:aws:s3tables:...' AS catalog_name (TYPE iceberg, ENDPOINT_TYPE s3_tables, SECRET s3_secret)
		attachSQL = fmt.Sprintf("ATTACH '%s' AS %s", conn.S3TablesARN, conn.CatalogName)
		attachParts = append(attachParts, "TYPE iceberg")
		attachParts = append(attachParts, "ENDPOINT_TYPE s3_tables")
		attachParts = append(attachParts, "SECRET s3_secret")
	}

	// Combine attach parts
	if len(attachParts) > 0 {
		attachSQL += fmt.Sprintf(" (%s)", strings.Join(attachParts, ", "))
	}

	return attachSQL
}

// GetURL returns the processed URL
func (conn *Iceberg2Conn) GetURL(newURL ...string) string {
	connURL := conn.BaseConn.URL
	if len(newURL) > 0 {
		connURL = newURL[0]
	}
	return connURL
}

// GenerateMergeSQL generates the upsert SQL for Iceberg2
func (conn *Iceberg2Conn) GenerateMergeSQL(srcTable string, tgtTable string, pkFields []string) (sql string, err error) {
	// Iceberg via DuckDB supports proper MERGE operations
	return conn.DuckDbConn.GenerateMergeSQL(srcTable, tgtTable, pkFields)
}

// SubmitTemplate submits a template query
func (conn *Iceberg2Conn) SubmitTemplate(level string, templateMap map[string]string, name string, values map[string]any) (data iop.Dataset, err error) {

	schemaName := cast.ToString(values["schema"])
	tableName := cast.ToString(values["table"])
	tablesExpr := cast.ToString(values["tables"])

	// Point to different template if it's a temp table
	if name == "columns" && strings.HasSuffix(tableName, "_sling_duckdb_tmp") {
		name = "columns_temp"
	}

	template, ok := templateMap[name]
	if !ok {
		err = g.Error("Could not find template %s", name)
		return
	}

	template = strings.TrimSpace(template) + env.NoDebugKey
	sql, err := conn.ProcessTemplate(level, template, values)
	if err != nil {
		err = g.Error(err, "error processing template")
		return
	}

	data, err = conn.Self().Query(sql)
	if err != nil {
		return
	}

	// For Iceberg tables, information_schema.columns often returns incomplete/incorrect results
	// (e.g., single row with "__" and "UNKNOWN" type). Use describe as fallback.
	if name == "columns" && tableName != "" {
		// Check if we got invalid results (empty, or the "__" placeholder column)
		hasValidColumns := len(data.Rows) > 0
		if hasValidColumns && len(data.Rows) == 1 {
			// Check if the only column is the "__" placeholder
			colName := cast.ToString(data.Rows[0][0])
			if colName == "__" {
				hasValidColumns = false
			}
		}

		if !hasValidColumns {
			view := Table{Schema: schemaName, Name: tableName, Dialect: dbio.TypeDbIceberg2}
			cols, err := conn.duck.Describe("select * from " + view.FullName())
			if err != nil {
				return data, g.Error(err, "could not describe "+view.FullName())
			}

			data.Columns = iop.NewColumnsFromFields("column_name", "data_type", "precision", "scale")
			data.Rows = nil // Clear the invalid rows
			for _, col := range cols {
				data.Rows = append(data.Rows, []any{col.Name, col.DbType, col.DbPrecision, col.DbScale})
			}
		}
	}

	// Use describe to get schemata columns for views
	if name == "schemata" && tablesExpr != "" {
		tables := strings.Split(tablesExpr, ", ")
		if len(data.Columns) == 0 {
			data.Columns = iop.NewColumnsFromFields("schema_name", "table_name", "is_view", "column_name", "data_type", "position")
		}

		for _, table := range tables {
			viewName := strings.Trim(table, "'")

			// Check that it is a view
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

			view := Table{Schema: schemaName, Name: viewName, Dialect: dbio.TypeDbIceberg2}
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

// Close closes the Iceberg2 connection
func (conn *Iceberg2Conn) Close() error {
	return conn.DuckDbConn.Close()
}
