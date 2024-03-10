package dbio

import (
	"strings"

	"github.com/flarco/g"
)

// Kind is the connection kind
type Kind string

const (
	// KindDatabase for databases
	KindDatabase Kind = "database"
	// KindFile for files (cloud, sftp)
	KindFile Kind = "file"
	// KindUnknown for unknown
	KindUnknown Kind = ""
)

// Type is the connection type
type Type string

const (
	TypeUnknown Type = ""

	TypeFileLocal  Type = "file"
	TypeFileHDFS   Type = "hdfs"
	TypeFileS3     Type = "s3"
	TypeFileAzure  Type = "azure"
	TypeFileGoogle Type = "gs"
	TypeFileFtp    Type = "ftp"
	TypeFileSftp   Type = "sftp"
	TypeFileHTTP   Type = "http"

	TypeDbPostgres   Type = "postgres"
	TypeDbRedshift   Type = "redshift"
	TypeDbStarRocks  Type = "starrocks"
	TypeDbMySQL      Type = "mysql"
	TypeDbMariaDB    Type = "mariadb"
	TypeDbOracle     Type = "oracle"
	TypeDbBigTable   Type = "bigtable"
	TypeDbBigQuery   Type = "bigquery"
	TypeDbSnowflake  Type = "snowflake"
	TypeDbSQLite     Type = "sqlite"
	TypeDbDuckDb     Type = "duckdb"
	TypeDbMotherDuck Type = "motherduck"
	TypeDbSQLServer  Type = "sqlserver"
	TypeDbAzure      Type = "azuresql"
	TypeDbAzureDWH   Type = "azuredwh"
	TypeDbTrino      Type = "trino"
	TypeDbClickhouse Type = "clickhouse"
)

// ValidateType returns true is type is valid
func ValidateType(tStr string) (Type, bool) {
	t := Type(strings.ToLower(tStr))

	tMap := map[string]Type{
		"postgresql": TypeDbPostgres,
		"file":       TypeFileLocal,
	}

	if tMatched, ok := tMap[tStr]; ok {
		t = tMatched
	}

	switch t {
	case
		TypeFileLocal, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp, TypeFileFtp,
		TypeDbPostgres, TypeDbRedshift, TypeDbStarRocks, TypeDbMySQL, TypeDbMariaDB, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH, TypeDbDuckDb, TypeDbMotherDuck, TypeDbClickhouse, TypeDbTrino:
		return t, true
	}

	return t, false
}

// String returns string instance
func (t Type) String() string {
	return string(t)
}

// DefPort returns the default port
func (t Type) DefPort() int {
	connTypesDefPort := map[Type]int{
		TypeDbPostgres:   5432,
		TypeDbRedshift:   5439,
		TypeDbStarRocks:  9030,
		TypeDbMySQL:      3306,
		TypeDbMariaDB:    3306,
		TypeDbOracle:     1521,
		TypeDbSQLServer:  1433,
		TypeDbAzure:      1433,
		TypeDbTrino:      8080,
		TypeDbClickhouse: 9000,
		TypeFileFtp:      21,
		TypeFileSftp:     22,
	}
	return connTypesDefPort[t]
}

// DBNameUpperCase returns true is upper case is default
func (t Type) DBNameUpperCase() bool {
	return g.In(t, TypeDbOracle, TypeDbSnowflake)
}

// Kind returns the kind of connection
func (t Type) Kind() Kind {
	switch t {
	case TypeDbPostgres, TypeDbRedshift, TypeDbStarRocks, TypeDbMySQL, TypeDbMariaDB, TypeDbOracle, TypeDbBigQuery, TypeDbBigTable,
		TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbClickhouse, TypeDbTrino, TypeDbDuckDb, TypeDbMotherDuck:
		return KindDatabase
	case TypeFileLocal, TypeFileHDFS, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileSftp, TypeFileFtp, TypeFileHTTP, Type("https"):
		return KindFile
	}
	return KindUnknown
}

// IsDb returns true if database connection
func (t Type) IsDb() bool {
	return t.Kind() == KindDatabase
}

// IsDb returns true if database connection
func (t Type) IsNoSQL() bool {
	return t == TypeDbBigTable
}

// IsFile returns true if file connection
func (t Type) IsFile() bool {
	return t.Kind() == KindFile
}

// IsUnknown returns true if unknown
func (t Type) IsUnknown() bool {
	return t.Kind() == KindUnknown
}

// NameLong return the type long name
func (t Type) NameLong() string {
	mapping := map[Type]string{
		TypeFileLocal:    "FileSys - Local",
		TypeFileHDFS:     "FileSys - HDFS",
		TypeFileS3:       "FileSys - S3",
		TypeFileAzure:    "FileSys - Azure",
		TypeFileGoogle:   "FileSys - Google",
		TypeFileSftp:     "FileSys - Sftp",
		TypeFileFtp:      "FileSys - Ftp",
		TypeFileHTTP:     "FileSys - HTTP",
		Type("https"):    "FileSys - HTTP",
		TypeDbPostgres:   "DB - PostgreSQL",
		TypeDbRedshift:   "DB - Redshift",
		TypeDbStarRocks:  "DB - StarRocks",
		TypeDbMySQL:      "DB - MySQL",
		TypeDbMariaDB:    "DB - MariaDB",
		TypeDbOracle:     "DB - Oracle",
		TypeDbBigQuery:   "DB - BigQuery",
		TypeDbBigTable:   "DB - BigTable",
		TypeDbSnowflake:  "DB - Snowflake",
		TypeDbSQLite:     "DB - SQLite",
		TypeDbDuckDb:     "DB - DuckDB",
		TypeDbMotherDuck: "DB - MotherDuck",
		TypeDbSQLServer:  "DB - SQLServer",
		TypeDbAzure:      "DB - Azure",
		TypeDbTrino:      "DB - Trino",
		TypeDbClickhouse: "DB - Clickhouse",
	}

	return mapping[t]
}

// Name return the type name
func (t Type) Name() string {
	mapping := map[Type]string{
		TypeFileLocal:    "Local",
		TypeFileHDFS:     "HDFS",
		TypeFileS3:       "S3",
		TypeFileAzure:    "Azure",
		TypeFileGoogle:   "Google",
		TypeFileSftp:     "Sftp",
		TypeFileFtp:      "Ftp",
		TypeFileHTTP:     "HTTP",
		Type("https"):    "HTTP",
		TypeDbPostgres:   "PostgreSQL",
		TypeDbRedshift:   "Redshift",
		TypeDbStarRocks:  "StarRocks",
		TypeDbMySQL:      "MySQL",
		TypeDbMariaDB:    "MariaDB",
		TypeDbOracle:     "Oracle",
		TypeDbBigQuery:   "BigQuery",
		TypeDbBigTable:   "BigTable",
		TypeDbSnowflake:  "Snowflake",
		TypeDbSQLite:     "SQLite",
		TypeDbDuckDb:     "DuckDB",
		TypeDbMotherDuck: "MotherDuck",
		TypeDbSQLServer:  "SQLServer",
		TypeDbTrino:      "Trino",
		TypeDbClickhouse: "Clickhouse",
		TypeDbAzure:      "Azure",
	}

	return mapping[t]
}
