package database

import (
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"

	_ "github.com/slingdata-io/godbc"
)

// ODBCConn is an ODBC connection
type ODBCConn struct {
	BaseConn
	URL        string
	driverType dbio.Type // Underlying database type for templates
}

// Init initializes the ODBC connection
func (conn *ODBCConn) Init() error {
	conn.BaseConn.URL = conn.URL
	conn.BaseConn.Type = dbio.TypeDbODBC

	instance := Connection(conn)
	conn.BaseConn.instance = &instance
	conn.driverType = conn.GetODBCDriverType()

	if err := conn.BaseConn.Init(); err != nil {
		return err
	}

	// Reload templates with driver-specific overrides
	return conn.LoadTemplates()
}

// GetODBCDriverType detects the underlying database type from DSN or template property
// This allows ODBC to use the correct SQL syntax for the underlying database
func (conn *ODBCConn) GetODBCDriverType() dbio.Type {

	// Detect driver type from connection or template property
	connString := conn.GetProp("conn_string")
	connTemplate := conn.GetProp("conn_template")

	// If explicit template property is set, use it first
	if connTemplate != "" {
		return dbio.Type(strings.ToLower(connTemplate))
	}

	// Auto-detect from connection string
	connStringLower := strings.ToLower(connString)

	// SQL Server patterns
	if strings.Contains(connStringLower, "sql server") ||
		strings.Contains(connStringLower, "sqlserver") ||
		strings.Contains(connStringLower, "odbc driver 17") ||
		strings.Contains(connStringLower, "odbc driver 18") ||
		strings.Contains(connStringLower, "sqlncli") ||
		strings.Contains(connStringLower, "msodbcsql") {
		return dbio.TypeDbSQLServer
	}

	// PostgreSQL patterns
	if strings.Contains(connStringLower, "postgresql") ||
		strings.Contains(connStringLower, "psqlodbc") ||
		strings.Contains(connStringLower, "postgres") {
		return dbio.TypeDbPostgres
	}

	// MySQL patterns
	if strings.Contains(connStringLower, "mysql") ||
		strings.Contains(connStringLower, "mariadb") {
		return dbio.TypeDbMySQL
	}

	// Oracle patterns
	if strings.Contains(connStringLower, "oracle") {
		return dbio.TypeDbOracle
	}

	// SQLite patterns
	if strings.Contains(connStringLower, "sqlite") {
		return dbio.TypeDbSQLite
	}

	// Clickhouse patterns
	if strings.Contains(connStringLower, "clickhouse") {
		return dbio.TypeDbClickhouse
	}

	return dbio.TypeDbODBC // Fallback to generic ODBC template
}

// LoadTemplates loads the appropriate yaml template
// For ODBC, it merges the driver-specific template with the ODBC template
// Driver template is base, ODBC template overrides for ODBC-specific behavior
func (conn *ODBCConn) LoadTemplates() error {
	// Load ODBC template without base
	odbcTemplate, err := dbio.TypeDbODBC.Template(false)
	if err != nil {
		return g.Error(err, "could not load ODBC template")
	}

	// If we have a driver type, start with driver template as base
	if conn.driverType != "" {
		driverTemplate, err := conn.driverType.Template()
		if err != nil {
			return g.Error("could not load driver template for %s: %v.", conn.driverType, err)
		}

		// Start with driver template, then overlay ODBC-specific values
		// This allows driver SQL syntax to be used, with ODBC overrides where needed
		for k, v := range odbcTemplate.Core {
			driverTemplate.Core[k] = v
		}
		for k, v := range odbcTemplate.Metadata {
			driverTemplate.Metadata[k] = v
		}
		for k, v := range odbcTemplate.Analysis {
			driverTemplate.Analysis[k] = v
		}
		for k, v := range odbcTemplate.Function {
			driverTemplate.Function[k] = v
		}
		for k, v := range odbcTemplate.Variable {
			driverTemplate.Variable[k] = v
		}

		conn.template = driverTemplate
		// Note: Unlike ADBC, we do NOT change conn.Type here
		// ODBC must keep its type for proper connection handling
		// We only use the driver template for SQL syntax

		g.Debug("ODBC using %s templates for SQL syntax", conn.driverType)
		return nil
	}

	// load with base
	conn.template, err = dbio.TypeDbODBC.Template(true)
	if err != nil {
		return g.Error(err, "could not load ODBC template")
	}

	return nil
}

// GetType returns the driver type for SQL generation purposes
// This allows ODBC to use the correct SQL dialect (e.g., TOP for SQL Server)
// while keeping the actual connection handling as ODBC
func (conn *ODBCConn) GetType() dbio.Type {
	if conn.driverType != "" {
		return conn.driverType
	}
	return conn.BaseConn.Type
}

// GetTemplateValue returns template values from our custom loaded template
// This overrides BaseConn.GetTemplateValue to use the driver-specific template
func (conn *ODBCConn) GetTemplateValue(path string) (value string) {
	// Use our loaded template (either driver-specific or ODBC with base)
	template := conn.template
	if template.Core == nil {
		// Fallback to base behavior if template not loaded
		return conn.BaseConn.GetTemplateValue(path)
	}

	prefixes := map[string]map[string]string{
		"core.":             template.Core,
		"analysis.":         template.Analysis,
		"function.":         template.Function,
		"metadata.":         template.Metadata,
		"general_type_map.": template.GeneralTypeMap,
		"native_type_map.":  template.NativeTypeMap,
		"variable.":         template.Variable,
	}

	for prefix, dict := range prefixes {
		if strings.HasPrefix(path, prefix) {
			key := strings.Replace(path, prefix, "", 1)
			value = dict[key]
			break
		}
	}

	return value
}

// GetURL returns the processed URL for the ODBC driver
// Supports two formats:
//   - "DSN=mydsn;UID=user;PWD=password"
//   - "Driver={SQL Server};Server=localhost;Database=mydb;UID=user;PWD=password"
func (conn *ODBCConn) GetURL(newURL ...string) string {
	return conn.GetProp("conn_string")
}

// BulkImportStream is not supported for ODBC (read-only connector)
func (conn *ODBCConn) BulkImportStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return 0, g.Error("ODBC connector is read-only and does not support bulk import")
}

// BulkImportFlow is not supported for ODBC (read-only connector)
func (conn *ODBCConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	return 0, g.Error("ODBC connector is read-only and does not support bulk import")
}

// InsertBatchStream is not supported for ODBC (read-only connector)
func (conn *ODBCConn) InsertBatchStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return 0, g.Error("ODBC connector is read-only and does not support insert operations")
}

// InsertStream is not supported for ODBC (read-only connector)
func (conn *ODBCConn) InsertStream(tableFName string, ds *iop.Datastream) (count uint64, err error) {
	return 0, g.Error("ODBC connector is read-only and does not support insert operations")
}
