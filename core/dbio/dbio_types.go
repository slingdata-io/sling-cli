package dbio

import (
	"bufio"
	"embed"
	"strings"

	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
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
	TypeDbMongoDB    Type = "mongodb"
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
		TypeDbPostgres, TypeDbRedshift, TypeDbStarRocks, TypeDbMySQL, TypeDbMariaDB, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH, TypeDbDuckDb, TypeDbMotherDuck, TypeDbClickhouse, TypeDbTrino, TypeDbMongoDB:
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
		TypeDbMongoDB:    27017,
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
		TypeDbSnowflake, TypeDbSQLite, TypeDbSQLServer, TypeDbAzure, TypeDbClickhouse, TypeDbTrino, TypeDbDuckDb, TypeDbMotherDuck, TypeDbMongoDB:
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
		TypeDbMongoDB:    "DB - MongoDB",
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
		TypeDbMongoDB:    "MongoDB",
		TypeDbAzure:      "Azure",
	}

	return mapping[t]
}

//go:embed templates/*
var templatesFolder embed.FS

// Template is a database YAML template
type Template struct {
	Core           map[string]string `yaml:"core"`
	Metadata       map[string]string `yaml:"metadata"`
	Analysis       map[string]string `yaml:"analysis"`
	Function       map[string]string `yaml:"function"`
	GeneralTypeMap map[string]string `yaml:"general_type_map"`
	NativeTypeMap  map[string]string `yaml:"native_type_map"`
	NativeStatsMap map[string]bool   `yaml:"native_stat_map"`
	Variable       map[string]string `yaml:"variable"`
}

// ToData convert is dataset
func (template Template) ToData() (data iop.Dataset) {
	columns := []string{"key", "value"}
	data = iop.NewDataset(iop.NewColumnsFromFields(columns...))
	data.Rows = append(data.Rows, []interface{}{"core", template.Core})
	data.Rows = append(data.Rows, []interface{}{"analysis", template.Analysis})
	data.Rows = append(data.Rows, []interface{}{"function", template.Function})
	data.Rows = append(data.Rows, []interface{}{"metadata", template.Metadata})
	data.Rows = append(data.Rows, []interface{}{"general_type_map", template.GeneralTypeMap})
	data.Rows = append(data.Rows, []interface{}{"native_type_map", template.NativeTypeMap})
	data.Rows = append(data.Rows, []interface{}{"variable", template.Variable})

	return
}

var typeTemplate = map[Type]Template{}

func (t Type) Template() (template Template, err error) {
	if val, ok := typeTemplate[t]; ok {
		return val, nil
	}

	template = Template{
		Core:           map[string]string{},
		Metadata:       map[string]string{},
		Analysis:       map[string]string{},
		Function:       map[string]string{},
		GeneralTypeMap: map[string]string{},
		NativeTypeMap:  map[string]string{},
		NativeStatsMap: map[string]bool{},
		Variable:       map[string]string{},
	}

	connTemplate := Template{}

	baseTemplateBytes, err := templatesFolder.ReadFile("templates/base.yaml")
	if err != nil {
		return template, g.Error(err, "io.ReadAll(baseTemplateFile)")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &template); err != nil {
		return template, g.Error(err, "yaml.Unmarshal")
	}

	templateBytes, err := templatesFolder.ReadFile("templates/" + t.String() + ".yaml")
	if err != nil {
		return template, g.Error(err, "io.ReadAll(templateFile) for "+t.String())
	}

	err = yaml.Unmarshal([]byte(templateBytes), &connTemplate)
	if err != nil {
		return template, g.Error(err, "yaml.Unmarshal")
	}

	for key, val := range connTemplate.Core {
		template.Core[key] = val
	}

	for key, val := range connTemplate.Analysis {
		template.Analysis[key] = val
	}

	for key, val := range connTemplate.Function {
		template.Function[key] = val
	}

	for key, val := range connTemplate.Metadata {
		template.Metadata[key] = val
	}

	for key, val := range connTemplate.Variable {
		template.Variable[key] = val
	}

	TypesNativeFile, err := templatesFolder.Open("templates/types_native_to_general.tsv")
	if err != nil {
		return template, g.Error(err, `cannot open types_native_to_general`)
	}

	TypesNativeCSV := iop.CSV{Reader: bufio.NewReader(TypesNativeFile)}
	TypesNativeCSV.Delimiter = '\t'
	TypesNativeCSV.NoDebug = true

	data, err := TypesNativeCSV.Read()
	if err != nil {
		return template, g.Error(err, `TypesNativeCSV.Read()`)
	}

	for _, rec := range data.Records() {
		if rec["database"] == t.String() {
			nt := strings.TrimSpace(cast.ToString(rec["native_type"]))
			gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
			s := strings.TrimSpace(cast.ToString(rec["stats_allowed"]))
			template.NativeTypeMap[nt] = cast.ToString(gt)
			template.NativeStatsMap[nt] = cast.ToBool(s)
		}
	}

	TypesGeneralFile, err := templatesFolder.Open("templates/types_general_to_native.tsv")
	if err != nil {
		return template, g.Error(err, `cannot open types_general_to_native`)
	}

	TypesGeneralCSV := iop.CSV{Reader: bufio.NewReader(TypesGeneralFile)}
	TypesGeneralCSV.Delimiter = '\t'
	TypesGeneralCSV.NoDebug = true

	data, err = TypesGeneralCSV.Read()
	if err != nil {
		return template, g.Error(err, `TypesGeneralCSV.Read()`)
	}

	for _, rec := range data.Records() {
		gt := strings.TrimSpace(cast.ToString(rec["general_type"]))
		template.GeneralTypeMap[gt] = cast.ToString(rec[t.String()])
	}

	// cache
	typeTemplate[t] = template

	return template, nil
}

// ParseURL parses a URL
func ParseURL(uri string) (uType Type, host string, path string, err error) {
	u, err := net.NewURL(uri)
	if err != nil {
		err = g.Error(err, "Unable to parse URL "+uri)
		return
	}

	scheme := u.U.Scheme
	host = u.Hostname()
	path = strings.TrimLeft(u.U.Path, "/")

	if scheme == "" || host == "" {
		err = g.Error("Invalid URL: " + uri)
	}

	// handle azure blob
	if scheme == "https" && strings.HasSuffix(host, ".blob.core.windows.net") {
		return TypeFileAzure, host, path, nil
	}

	uType, ok := ValidateType(scheme)
	if !ok {
		err = g.Error("unrecognized url type: %s", scheme)
	}

	return
}
