package database

import (
	"strings"

	"github.com/slingdata-io/sling-cli/core/dbio"
)

// GetArrowDBCDriverType maps ADBC driver names to corresponding database types
// This allows using driver-specific SQL templates
func GetArrowDBCDriverType(driverName string) dbio.Type {
	mapping := map[string]dbio.Type{
		"postgresql": dbio.TypeDbPostgres,
		"postgres":   dbio.TypeDbPostgres,
		"mssql":      dbio.TypeDbSQLServer,
		"sqlserver":  dbio.TypeDbSQLServer,
		"snowflake":  dbio.TypeDbSnowflake,
		"sqlite":     dbio.TypeDbSQLite,
		"duckdb":     dbio.TypeDbDuckDb,
		"bigquery":   dbio.TypeDbBigQuery,
		"mysql":      dbio.TypeDbMySQL,
		"trino":      dbio.TypeDbTrino,
	}
	if t, ok := mapping[strings.ToLower(driverName)]; ok {
		return t
	}
	return dbio.TypeDbArrowDBC // Fallback to ADBC template
}
