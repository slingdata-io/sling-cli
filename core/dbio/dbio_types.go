package dbio

import (
	"bufio"
	"embed"
	"encoding/csv"
	"io"
	"strings"
	"unicode"

	"github.com/flarco/g"
	"gopkg.in/yaml.v2"
)

// Kind is the connection kind
type Kind string

const (
	// KindDatabase for databases
	KindDatabase Kind = "database"
	// KindFile for files (cloud, sftp)
	KindFile Kind = "file"
	// KindAPI for apis
	KindAPI Kind = "api"
	// KindUnknown for unknown
	KindUnknown Kind = ""
)

var AllKind = []struct {
	Value  Kind
	TSName string
}{
	{KindDatabase, "KindDatabase"},
	{KindFile, "KindFile"},
	{KindAPI, "KindAPI"},
	{KindUnknown, "KindUnknown"},
}

// Type is the connection type
type Type string

const (
	TypeUnknown Type = ""

	TypeApi Type = "api"

	TypeFileLocal       Type = "file"
	TypeFileHDFS        Type = "hdfs"
	TypeFileS3          Type = "s3"
	TypeFileAzure       Type = "azure"
	TypeFileGoogle      Type = "gs"
	TypeFileGoogleDrive Type = "gdrive"
	TypeFileFtp         Type = "ftp"
	TypeFileSftp        Type = "sftp"
	TypeFileHTTP        Type = "http"

	TypeDbPostgres      Type = "postgres"
	TypeDbRedshift      Type = "redshift"
	TypeDbStarRocks     Type = "starrocks"
	TypeDbMySQL         Type = "mysql"
	TypeDbMariaDB       Type = "mariadb"
	TypeDbOracle        Type = "oracle"
	TypeDbBigTable      Type = "bigtable"
	TypeDbBigQuery      Type = "bigquery"
	TypeDbSnowflake     Type = "snowflake"
	TypeDbSQLite        Type = "sqlite"
	TypeDbD1            Type = "d1"
	TypeDbDuckDb        Type = "duckdb"
	TypeDbDuckLake      Type = "ducklake"
	TypeDbMotherDuck    Type = "motherduck"
	TypeDbSQLServer     Type = "sqlserver"
	TypeDbAzure         Type = "azuresql"
	TypeDbAzureDWH      Type = "azuredwh"
	TypeDbTrino         Type = "trino"
	TypeDbClickhouse    Type = "clickhouse"
	TypeDbMongoDB       Type = "mongodb"
	TypeDbElasticsearch Type = "elasticsearch"
	TypeDbPrometheus    Type = "prometheus"
	TypeDbProton        Type = "proton"
	TypeDbAthena        Type = "athena"
	TypeDbIceberg       Type = "iceberg"
)

var AllType = []struct {
	Value  Type
	TSName string
}{
	{TypeUnknown, "TypeUnknown"},
	{TypeApi, "TypeApi"},
	{TypeFileLocal, "TypeFileLocal"},
	{TypeFileHDFS, "TypeFileHDFS"},
	{TypeFileS3, "TypeFileS3"},
	{TypeFileAzure, "TypeFileAzure"},
	{TypeFileGoogle, "TypeFileGoogle"},
	{TypeFileGoogleDrive, "TypeFileGoogleDrive"},
	{TypeFileFtp, "TypeFileFtp"},
	{TypeFileSftp, "TypeFileSftp"},
	{TypeFileHTTP, "TypeFileHTTP"},
	{TypeDbPostgres, "TypeDbPostgres"},
	{TypeDbRedshift, "TypeDbRedshift"},
	{TypeDbStarRocks, "TypeDbStarRocks"},
	{TypeDbMySQL, "TypeDbMySQL"},
	{TypeDbMariaDB, "TypeDbMariaDB"},
	{TypeDbOracle, "TypeDbOracle"},
	{TypeDbBigTable, "TypeDbBigTable"},
	{TypeDbBigQuery, "TypeDbBigQuery"},
	{TypeDbSnowflake, "TypeDbSnowflake"},
	{TypeDbSQLite, "TypeDbSQLite"},
	{TypeDbD1, "TypeDbD1"},
	{TypeDbDuckDb, "TypeDbDuckDb"},
	{TypeDbDuckLake, "TypeDbDuckLake"},
	{TypeDbMotherDuck, "TypeDbMotherDuck"},
	{TypeDbSQLServer, "TypeDbSQLServer"},
	{TypeDbAzure, "TypeDbAzure"},
	{TypeDbAzureDWH, "TypeDbAzureDWH"},
	{TypeDbTrino, "TypeDbTrino"},
	{TypeDbAthena, "TypeDbAthena"},
	{TypeDbIceberg, "TypeDbIceberg"},
	{TypeDbClickhouse, "TypeDbClickhouse"},
	{TypeDbElasticsearch, "TypeDbElasticsearch"},
	{TypeDbMongoDB, "TypeDbMongoDB"},
	{TypeDbPrometheus, "TypeDbPrometheus"},
	{TypeDbProton, "TypeDbProton"},
}

// ValidateType returns true is type is valid
func ValidateType(tStr string) (Type, bool) {
	t := Type(strings.ToLower(tStr))

	tMap := map[string]Type{
		"postgresql":  TypeDbPostgres,
		"mongodb+srv": TypeDbMongoDB,
		"file":        TypeFileLocal,
	}

	if tMatched, ok := tMap[tStr]; ok {
		t = tMatched
	}

	switch t {
	case
		TypeApi,
		TypeFileLocal, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileGoogleDrive, TypeFileSftp, TypeFileFtp,
		TypeDbPostgres, TypeDbRedshift, TypeDbStarRocks, TypeDbMySQL, TypeDbMariaDB, TypeDbOracle, TypeDbBigQuery, TypeDbSnowflake, TypeDbSQLite, TypeDbD1, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH, TypeDbDuckDb, TypeDbDuckLake, TypeDbMotherDuck, TypeDbClickhouse, TypeDbTrino, TypeDbAthena, TypeDbIceberg, TypeDbMongoDB, TypeDbElasticsearch, TypeDbPrometheus:
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
		TypeDbPostgres:      5432,
		TypeDbRedshift:      5439,
		TypeDbStarRocks:     9030,
		TypeDbMySQL:         3306,
		TypeDbMariaDB:       3306,
		TypeDbOracle:        1521,
		TypeDbSQLServer:     1433,
		TypeDbAzure:         1433,
		TypeDbTrino:         8080,
		TypeDbAthena:        443,
		TypeDbIceberg:       443,
		TypeDbClickhouse:    9000,
		TypeDbMongoDB:       27017,
		TypeDbElasticsearch: 9200,
		TypeDbPrometheus:    9090,
		TypeDbProton:        8463,
		TypeFileFtp:         21,
		TypeFileSftp:        22,
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
		TypeDbSnowflake, TypeDbSQLite, TypeDbD1, TypeDbSQLServer, TypeDbAzure, TypeDbClickhouse, TypeDbTrino, TypeDbAthena, TypeDbIceberg, TypeDbDuckDb, TypeDbDuckLake, TypeDbMotherDuck, TypeDbMongoDB, TypeDbElasticsearch, TypeDbPrometheus, TypeDbProton:
		return KindDatabase
	case TypeFileLocal, TypeFileHDFS, TypeFileS3, TypeFileAzure, TypeFileGoogle, TypeFileGoogleDrive, TypeFileSftp, TypeFileFtp, TypeFileHTTP, Type("https"):
		return KindFile
	case TypeApi:
		return KindAPI
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

// IsAPI returns true if api connection
func (t Type) IsAPI() bool {
	return t.Kind() == KindAPI
}

// IsUnknown returns true if unknown
func (t Type) IsUnknown() bool {
	return t.Kind() == KindUnknown
}

// IsSQLServer returns true is sql server flavor
func (t Type) IsSQLServer() bool {
	return g.In(t, TypeDbSQLServer, TypeDbAzure, TypeDbAzureDWH)
}

// NameLong return the type long name
func (t Type) NameLong() string {
	mapping := map[Type]string{
		TypeApi:             "API - Spec",
		TypeFileLocal:       "FileSys - Local",
		TypeFileHDFS:        "FileSys - HDFS",
		TypeFileS3:          "FileSys - S3",
		TypeFileAzure:       "FileSys - Azure",
		TypeFileGoogle:      "FileSys - Google Cloud Storage",
		TypeFileGoogleDrive: "FileSys - Google Drive",
		TypeFileSftp:        "FileSys - Sftp",
		TypeFileFtp:         "FileSys - Ftp",
		TypeFileHTTP:        "FileSys - HTTP",
		Type("https"):       "FileSys - HTTP",
		TypeDbPostgres:      "DB - PostgreSQL",
		TypeDbRedshift:      "DB - Redshift",
		TypeDbStarRocks:     "DB - StarRocks",
		TypeDbMySQL:         "DB - MySQL",
		TypeDbMariaDB:       "DB - MariaDB",
		TypeDbOracle:        "DB - Oracle",
		TypeDbBigQuery:      "DB - BigQuery",
		TypeDbBigTable:      "DB - BigTable",
		TypeDbSnowflake:     "DB - Snowflake",
		TypeDbD1:            "DB - D1",
		TypeDbSQLite:        "DB - SQLite",
		TypeDbDuckDb:        "DB - DuckDB",
		TypeDbDuckLake:      "DB - DuckLake",
		TypeDbMotherDuck:    "DB - MotherDuck",
		TypeDbSQLServer:     "DB - SQLServer",
		TypeDbAzure:         "DB - Azure",
		TypeDbTrino:         "DB - Trino",
		TypeDbAthena:        "DB - Athena",
		TypeDbIceberg:       "DB - Iceberg",
		TypeDbClickhouse:    "DB - Clickhouse",
		TypeDbPrometheus:    "DB - Prometheus",
		TypeDbElasticsearch: "DB - Elasticsearch",
		TypeDbMongoDB:       "DB - MongoDB",
		TypeDbProton:        "DB - Proton",
	}

	return mapping[t]
}

// Name return the type name
func (t Type) Name() string {
	mapping := map[Type]string{
		TypeApi:             "API",
		TypeFileLocal:       "Local",
		TypeFileHDFS:        "HDFS",
		TypeFileS3:          "S3",
		TypeFileAzure:       "Azure",
		TypeFileGoogle:      "Google Cloud Storage",
		TypeFileGoogleDrive: "Google Drive",
		TypeFileSftp:        "Sftp",
		TypeFileFtp:         "Ftp",
		TypeFileHTTP:        "HTTP",
		Type("https"):       "HTTP",
		TypeDbPostgres:      "PostgreSQL",
		TypeDbRedshift:      "Redshift",
		TypeDbStarRocks:     "StarRocks",
		TypeDbMySQL:         "MySQL",
		TypeDbMariaDB:       "MariaDB",
		TypeDbOracle:        "Oracle",
		TypeDbBigQuery:      "BigQuery",
		TypeDbBigTable:      "BigTable",
		TypeDbSnowflake:     "Snowflake",
		TypeDbD1:            "D1",
		TypeDbSQLite:        "SQLite",
		TypeDbDuckDb:        "DuckDB",
		TypeDbDuckLake:      "DuckLake",
		TypeDbMotherDuck:    "MotherDuck",
		TypeDbSQLServer:     "SQLServer",
		TypeDbTrino:         "Trino",
		TypeDbAthena:        "Athena",
		TypeDbIceberg:       "Iceberg",
		TypeDbClickhouse:    "Clickhouse",
		TypeDbPrometheus:    "Prometheus",
		TypeDbElasticsearch: "Elasticsearch",
		TypeDbMongoDB:       "MongoDB",
		TypeDbAzure:         "Azure",
		TypeDbProton:        "Proton",
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
func (template Template) Value(path string) (value string) {
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

// a cache for templates (so we only read once)
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
		return template, g.Error(err, "could not read base.yaml")
	}

	if err := yaml.Unmarshal([]byte(baseTemplateBytes), &template); err != nil {
		return template, g.Error(err, "could not unmarshal baseTemplateBytes")
	}

	templateBytes, err := templatesFolder.ReadFile("templates/" + t.String() + ".yaml")
	if err != nil {
		return template, g.Error(err, "could not read "+t.String()+".yaml")
	}

	err = yaml.Unmarshal([]byte(templateBytes), &connTemplate)
	if err != nil {
		return template, g.Error(err, "could not unmarshal templateBytes")
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

	csvReader := csv.NewReader(bufio.NewReader(TypesNativeFile))
	csvReader.Comma = '\t'

	var records []map[string]string

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return template, g.Error(err, "failed to read header from types_native_to_general.tsv")
	}

	// Read records
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return template, g.Error(err, "failed to read row from types_native_to_general.tsv")
		}

		record := make(map[string]string)
		for i, value := range row {
			record[header[i]] = value
		}
		records = append(records, record)
	}

	for _, rec := range records {
		if rec["database"] == t.String() {
			nt := strings.TrimSpace(rec["native_type"])
			gt := strings.TrimSpace(rec["general_type"])
			s := strings.TrimSpace(rec["stats_allowed"])
			template.NativeTypeMap[nt] = gt
			template.NativeStatsMap[nt] = s == "true"
		}
	}

	TypesGeneralFile, err := templatesFolder.Open("templates/types_general_to_native.tsv")
	if err != nil {
		return template, g.Error(err, `cannot open types_general_to_native`)
	}

	csvReader = csv.NewReader(bufio.NewReader(TypesGeneralFile))
	csvReader.Comma = '\t'

	// Read header
	header, err = csvReader.Read()
	if err != nil {
		return template, g.Error(err, "failed to read header from types_general_to_native.tsv")
	}

	// Read records
	records = []map[string]string{} // reset
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return template, g.Error(err, "failed to read row from types_general_to_native.tsv")
		}

		record := make(map[string]string)
		for i, value := range row {
			record[header[i]] = value
		}
		records = append(records, record)
	}

	for _, rec := range records {
		gt := strings.TrimSpace(rec["general_type"])
		template.GeneralTypeMap[gt] = rec[t.String()]
	}

	// cache
	typeTemplate[t] = template

	return template, nil
}

// Unquote removes quotes to the field name
func (t Type) Unquote(field string) string {
	template, _ := t.Template()
	q := template.Variable["quote_char"]
	return strings.ReplaceAll(field, q, "")
}

// Quote adds quotes to the field name
func (t Type) Quote(field string) string {
	// don't normalize, causes issues.
	// see https://github.com/slingdata-io/sling-cli/issues/538
	// we should determine the casing upstream, configuration phase
	Normalize := false

	template, _ := t.Template()
	// always normalize if case is uniform. Why would you quote and not normalize?
	if !HasVariedCase(field) && Normalize {
		if t.DBNameUpperCase() {
			field = strings.ToUpper(field)
		} else {
			field = strings.ToLower(field)
		}
	}
	q := template.Variable["quote_char"]
	field = t.Unquote(field)
	return q + field + q
}

func (t Type) QuoteNames(names ...string) (newNames []string) {
	newNames = make([]string, len(names))
	for i := range names {
		newNames[i] = t.Quote(names[i])
	}
	return newNames
}

func HasVariedCase(text string) bool {
	hasUpper := false
	hasLower := false
	for _, c := range text {
		if unicode.IsUpper(c) {
			hasUpper = true
		}
		if unicode.IsLower(c) {
			hasLower = true
		}
		if hasUpper && hasLower {
			break
		}
	}

	return hasUpper && hasLower
}

// HasStrangeChar returns true if the text has a non-typical SQL database ID character.
// Should only allow characters: a-z, A-Z, 0-9 and _
func HasStrangeChar(text string) bool {
	for _, r := range text {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return true
		}
	}
	return false
}

func (t Type) GetTemplateValue(path string) (value string) {

	template, _ := t.Template()
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

type FileType string

const (
	FileTypeNone      FileType = ""
	FileTypeCsv       FileType = "csv"
	FileTypeXml       FileType = "xml"
	FileTypeExcel     FileType = "xlsx"
	FileTypeJson      FileType = "json"
	FileTypeParquet   FileType = "parquet"
	FileTypeArrow     FileType = "arrow"
	FileTypeAvro      FileType = "avro"
	FileTypeSAS       FileType = "sas7bdat"
	FileTypeJsonLines FileType = "jsonlines"
	FileTypeIceberg   FileType = "iceberg"
	FileTypeDelta     FileType = "delta"
	FileTypeRaw       FileType = "raw"
)

var AllFileType = []struct {
	Value  FileType
	TSName string
}{
	{FileTypeNone, "FileTypeNone"},
	{FileTypeCsv, "FileTypeCsv"},
	{FileTypeXml, "FileTypeXml"},
	{FileTypeExcel, "FileTypeExcel"},
	{FileTypeJson, "FileTypeJson"},
	{FileTypeParquet, "FileTypeParquet"},
	{FileTypeAvro, "FileTypeAvro"},
	{FileTypeArrow, "FileTypeArrow"},
	{FileTypeSAS, "FileTypeSAS"},
	{FileTypeJsonLines, "FileTypeJsonLines"},
	{FileTypeIceberg, "FileTypeIceberg"},
	{FileTypeDelta, "FileTypeDelta"},
	{FileTypeRaw, "FileTypeRaw"},
}

func (ft FileType) Ext() string {
	switch ft {
	case FileTypeJsonLines:
		return ".jsonl"
	default:
		return "." + string(ft)
	}
}

func (ft FileType) IsJson() bool {
	switch ft {
	case FileTypeJson, FileTypeJsonLines:
		return true
	}
	return false
}
