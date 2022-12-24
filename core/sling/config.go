package sling

import (
	"database/sql/driver"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/dbio/database"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/spf13/cast"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	jsoniter "github.com/json-iterator/go"
	"gopkg.in/yaml.v2"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
)

// Mode is a load mode
type Mode string

const (
	// TruncateMode is to truncate
	TruncateMode Mode = "truncate"
	// FullRefreshMode is to drop
	FullRefreshMode Mode = "full-refresh"
	// IncrementalMode is to incremental
	IncrementalMode Mode = "incremental"
	// SnapshotMode is to snapshot
	SnapshotMode Mode = "snapshot"
)

// NewConfig return a config object from a YAML / JSON string
func NewConfig(cfgStr string) (cfg *Config, err error) {
	// set default, unmarshalling will overwrite
	cfg = &Config{}

	err = cfg.Unmarshal(cfgStr)
	if err != nil {
		err = g.Error(err, "Unable to parse config payload")
		return
	}

	err = cfg.Prepare()
	if err != nil {
		err = g.Error(err, "Unable to prepare config")
		return
	}
	return
}

// SetDefault sets default options
func (cfg *Config) SetDefault() {

	// set source options
	var sourceOptions SourceOptions
	switch cfg.SrcConn.Type.Kind() {
	case dbio.KindFile:
		sourceOptions = SourceFileOptionsDefault
	case dbio.KindDatabase, dbio.KindAPI, dbio.KindAirbyte:
		sourceOptions = SourceDBOptionsDefault
	default:
		sourceOptions = SourceDBOptionsDefault
	}

	if cfg.Source.Options == nil {
		cfg.Source.Options = &sourceOptions
	}
	if cfg.Source.Options.TrimSpace == nil {
		cfg.Source.Options.TrimSpace = sourceOptions.TrimSpace
	}
	if cfg.Source.Options.EmptyAsNull == nil {
		cfg.Source.Options.EmptyAsNull = sourceOptions.EmptyAsNull
	}
	if cfg.Source.Options.Header == nil {
		cfg.Source.Options.Header = sourceOptions.Header
	}
	if cfg.Source.Options.Compression == nil {
		cfg.Source.Options.Compression = sourceOptions.Compression
	}
	if cfg.Source.Options.NullIf == nil {
		cfg.Source.Options.NullIf = sourceOptions.NullIf
	}
	if cfg.Source.Options.DatetimeFormat == "" {
		cfg.Source.Options.DatetimeFormat = sourceOptions.DatetimeFormat
	}
	if cfg.Source.Options.SkipBlankLines == nil {
		cfg.Source.Options.SkipBlankLines = sourceOptions.SkipBlankLines
	}
	if cfg.Source.Options.Delimiter == "" {
		cfg.Source.Options.Delimiter = sourceOptions.Delimiter
	}
	if cfg.Source.Options.MaxDecimals == nil {
		cfg.Source.Options.MaxDecimals = sourceOptions.MaxDecimals
	}

	// set target options
	var targetOptions TargetOptions
	switch cfg.TgtConn.Type.Kind() {
	case dbio.KindFile:
		targetOptions = TargetFileOptionsDefault
	case dbio.KindDatabase, dbio.KindAPI, dbio.KindAirbyte:
		targetOptions = TargetDBOptionsDefault
	default:
		targetOptions = TargetDBOptionsDefault
	}

	if cfg.Target.Options == nil {
		cfg.Target.Options = &targetOptions
	}
	if cfg.Target.Options.Header == nil {
		cfg.Target.Options.Header = targetOptions.Header
	}
	if cfg.Target.Options.Compression == nil {
		cfg.Target.Options.Compression = targetOptions.Compression
	}
	if cfg.Target.Options.Concurrency == 0 {
		cfg.Target.Options.Concurrency = targetOptions.Concurrency
	}
	if cfg.Target.Options.FileMaxRows == 0 {
		cfg.Target.Options.FileMaxRows = targetOptions.FileMaxRows
	}
	if cfg.Target.Options.UseBulk == nil {
		cfg.Target.Options.UseBulk = targetOptions.UseBulk
	}
	if cfg.Target.Options.AdjustColumnType == nil {
		cfg.Target.Options.AdjustColumnType = targetOptions.AdjustColumnType
		if cfg.SrcConn.Type.Kind() == dbio.KindFile || cfg.Options.StdIn {
			// if source stream is file, we have no schema reference
			cfg.Target.Options.AdjustColumnType = g.Bool(true)
		}
	}
	if cfg.Target.Options.DatetimeFormat == "" {
		cfg.Target.Options.DatetimeFormat = targetOptions.DatetimeFormat
	}
	if cfg.Target.Options.Delimiter == "" {
		cfg.Target.Options.Delimiter = targetOptions.Delimiter
	}
	if cfg.Target.Options.MaxDecimals == nil {
		cfg.Target.Options.MaxDecimals = targetOptions.MaxDecimals
	}

}

// Unmarshal parse a configuration file path or config text
func (cfg *Config) Unmarshal(cfgStr string) error {
	cfgBytes := []byte(cfgStr)
	if _, err := os.Stat(cfgStr); err == nil {
		cfgFile, err := os.Open(cfgStr)
		if err != nil {
			return g.Error(err, "Unable to open cfgStr: "+cfgStr)
		}

		cfgBytes, err = ioutil.ReadAll(cfgFile)
		if err != nil {
			return g.Error(err, "could not read from cfgFile")
		}
	}

	err := yaml.Unmarshal(cfgBytes, cfg)
	if err != nil {
		return g.Error(err, "Error parsing cfgBytes")
	}

	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}

	if cfg.Source.Data == nil {
		cfg.Source.Data = g.M()
	}

	if cfg.SrcConn.Data == nil {
		cfg.SrcConn.Data = g.M()
	}

	if cfg.Target.Data == nil {
		cfg.Target.Data = g.M()
	}

	if cfg.TgtConn.Data == nil {
		cfg.TgtConn.Data = g.M()
	}

	return nil
}

// setSchema sets the default schema
func setSchema(schema string, obj string) string {

	// fill table schema if needed
	if schema != "" && obj != "" && !strings.Contains(obj, ".") {
		obj = g.F("%s.%s", schema, obj)
	}

	return obj
}

func (cfg *Config) DetermineType() (Type JobType, err error) {

	srcFileProvided := cfg.Options.StdIn || cfg.SrcConn.Info().Type.IsFile()
	tgtFileProvided := cfg.Options.StdOut || cfg.TgtConn.Info().Type.IsFile()
	srcDbProvided := cfg.SrcConn.Info().Type.IsDb()
	tgtDbProvided := cfg.TgtConn.Info().Type.IsDb()
	srcAPIProvided := cfg.SrcConn.Info().Type.IsAPI() || cfg.SrcConn.Info().Type.IsAirbyte()
	srcStreamProvided := cfg.Source.Stream != ""

	summary := g.F("srcFileProvided: %t, tgtFileProvided: %t, srcDbProvided: %t, tgtDbProvided: %t, srcStreamProvided: %t, srcAPIProvided: %t", srcFileProvided, tgtFileProvided, srcDbProvided, tgtDbProvided, srcStreamProvided, srcAPIProvided)
	g.Trace(summary)

	if cfg.Mode == "" {
		cfg.Mode = FullRefreshMode
	}

	validMode := g.In(cfg.Mode, FullRefreshMode, IncrementalMode, SnapshotMode, TruncateMode)
	if !validMode {
		err = g.Error("must specify valid mode: full-refresh, incremental, snapshot or truncate")
		return
	}

	if cfg.Mode == IncrementalMode {
		if cfg.SrcConn.Info().Type == dbio.TypeDbBigTable {
			// use default keys if none are provided
			if len(cfg.Source.PrimaryKey) == 0 {
				cfg.Source.PrimaryKey = []string{"_bigtable_key"}
			}

			if cfg.Source.UpdateKey == "" {
				cfg.Source.UpdateKey = "_bigtable_timestamp"
			}
		} else if srcFileProvided && cfg.Source.UpdateKey == slingLoadedAtColumn {
			// need to loaded_at column for file incremental
			MetadataLoadedAt = true
		} else if cfg.Source.UpdateKey == "" && len(cfg.Source.PrimaryKey) == 0 {
			err = g.Error("must specify value for 'update_key' and/or 'primary_key' for incremental mode. See docs for more details: https://docs.slingdata.io/sling-cli/configuration#mode")
			return
		}
	}

	if srcDbProvided && tgtDbProvided {
		Type = DbToDb
	} else if srcFileProvided && tgtDbProvided {
		Type = FileToDB
	} else if srcDbProvided && srcStreamProvided && !tgtDbProvided && tgtFileProvided {
		Type = DbToFile
	} else if srcFileProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		Type = FileToFile
	} else if srcAPIProvided && srcStreamProvided && tgtDbProvided {
		Type = APIToDb
	} else if srcAPIProvided && srcStreamProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		Type = APIToFile
	} else if tgtDbProvided && cfg.Target.Options != nil && cfg.Target.Options.PostSQL != "" {
		cfg.Target.Object = cfg.Target.Options.PostSQL
		Type = DbSQL
	}

	if Type == "" {
		// g.PP(t)
		sourceErrMsg := ""
		targetErrMsg := ""

		if !cfg.Options.StdIn {
			if cfg.SrcConn.Name == "" {
				targetErrMsg = g.F("source connection is missing, need to provide")
			} else if cfg.SrcConn.Name != "" && cfg.SrcConn.Info().Type.IsUnknown() {
				sourceErrMsg = g.F("source connection '%s' not valid / found in environment", cfg.SrcConn.Name)
			}
		}

		if !cfg.Options.StdOut {
			if cfg.TgtConn.Name == "" {
				targetErrMsg = g.F("target connection is missing, need to provide")
			} else if cfg.TgtConn.Name != "" && cfg.TgtConn.Info().Type.IsUnknown() {
				targetErrMsg = g.F("target connection '%s' not valid / found in environment", cfg.TgtConn.Name)
			}
		}

		output := []string{}
		if sourceErrMsg != "" {
			output = append(output, g.F("error -> %s", sourceErrMsg))
		}
		if targetErrMsg != "" {
			output = append(output, g.F("error -> %s", targetErrMsg))
		}

		// []string{
		// 	g.F("Source File Provided: %t", srcFileProvided),
		// 	g.F("Target File Provided: %t", tgtFileProvided),
		// 	g.F("Source DB Provided: %t", srcDbProvided),
		// 	g.F("Target DB Provided: %t", tgtDbProvided),
		// 	g.F("Source Stream Provided: %t", srcStreamProvided),
		// 	g.F("Source API Provided: %t", srcAPIProvided),
		// }

		err = g.Error("invalid Task Configuration. Must specify valid source conn / file or target connection / output.\n  %s", strings.Join(output, "\n  "))
	}
	return Type, err
}

func schemeType(url string) dbio.Type {
	if !strings.Contains(url, "://") {
		return dbio.TypeUnknown
	}

	scheme := strings.Split(url, "://")[0]
	t, _ := dbio.ValidateType(scheme)
	return t
}

// Prepare prepares the config
func (cfg *Config) Prepare() (err error) {
	if cfg.Prepared {
		return
	}

	// get local connections
	connsMap := lo.KeyBy(connection.GetLocalConns(), func(c connection.ConnEntry) string {
		return strings.ToLower(c.Connection.Name)
	})

	// Check Inputs
	if !cfg.Options.StdIn && cfg.Source.Conn == "" && cfg.Target.Conn == "" {
		return g.Error("invalid source connection (blank or not found)")
	}
	if !cfg.Options.StdOut && cfg.Target.Conn == "" && cfg.Target.Object == "" {
		if os.Getenv("SLING_CLI") == "TRUE" {
			return g.Error("invalid target connection (blank or not found). Did you mean to use the `--stdout` flag?")
		}
		return g.Error("invalid target connection (blank or not found)")
	}

	if cfg.Options.Debug && os.Getenv("_DEBUG") == "" {
		os.Setenv("_DEBUG", "DEBUG")
	}
	if cfg.Options.StdIn && cfg.Source.Stream == "" {
		cfg.Source.Stream = "stdin"
	}

	// Set Target
	cfg.Target.Object = strings.TrimSpace(cfg.Target.Object)
	if cfg.Target.Data == nil || len(cfg.Target.Data) == 0 {
		cfg.Target.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Target.Conn)]; ok {
			cfg.TgtConn = c.Connection
		} else if !strings.Contains(cfg.Target.Conn, "://") && cfg.Target.Conn != "" && cfg.TgtConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Target.Conn)
		} else if cfg.TgtConn.Data == nil {
			cfg.TgtConn.Data = g.M()
		}
		cfg.Target.Data = cfg.TgtConn.Data
	}

	if schemeType(cfg.Target.Object).IsFile() {
		// format target name, especially veriable hostname
		err = cfg.FormatTargetObjectName()
		if err != nil {
			return g.Error(err, "could not format target object name")
		}
	} else if cast.ToString(cfg.Target.Data["url"]) == "" {
		cfg.Target.Data["url"] = cfg.Target.Conn
	}

	if cfg.TgtConn.Type.IsUnknown() {
		tgtConn, err := connection.NewConnectionFromMap(g.M(
			"name", cfg.Target.Conn, "data", cfg.Target.Data,
		))
		if err != nil {
			return g.Error(err, "could not create data conn for target")
		}
		cfg.TgtConn = tgtConn
	}

	if cfg.Options.StdOut {
		os.Setenv("CONCURRENCY", "1")
	}
	if val := os.Getenv("SLING_LIMIT"); val != "" {
		cfg.Source.Limit = cast.ToInt(val)
	}

	// Set Source
	cfg.Source.Stream = strings.TrimSpace(cfg.Source.Stream)
	if cfg.Source.Data == nil || len(cfg.Source.Data) == 0 {
		cfg.Source.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Source.Conn)]; ok {
			cfg.SrcConn = c.Connection
		} else if !strings.Contains(cfg.Source.Conn, "://") && cfg.Source.Conn != "" && cfg.SrcConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Source.Conn)
		} else if cfg.SrcConn.Data == nil {
			cfg.SrcConn.Data = g.M()
		}
		cfg.Source.Data = cfg.SrcConn.Data
	}

	if schemeType(cfg.Source.Stream).IsFile() {
		cfg.Source.Data["url"] = cfg.Source.Stream
		cfg.SrcConn.Data["url"] = cfg.Source.Stream
	} else if cast.ToString(cfg.Source.Data["url"]) == "" {
		cfg.Source.Data["url"] = cfg.Source.Conn
	}

	if cfg.SrcConn.Type.IsUnknown() {
		srcConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Source.Conn, "data", cfg.Source.Data))
		if err != nil {
			return g.Error(err, "could not create data conn for source")
		}
		cfg.SrcConn = srcConn
	}

	// format target name, now we have source info
	err = cfg.FormatTargetObjectName()
	if err != nil {
		return g.Error(err, "could not format target object name")
	} else if strings.Contains(cfg.Target.Object, "{") {
		words := []string{}
		for _, m := range g.Matches(cfg.Target.Object, `\{([^}]+)\}`) {
			if len(m.Group) > 0 {
				words = append(words, m.Group[0])
			}
		}
		// return g.Error("unformatted target object name: %s", strings.Join(words, ", "))
		g.Debug("Could not successfully format target object name. Blank values for: %s", strings.Join(words, ", "))
		for _, word := range words {
			cfg.Target.Object = strings.ReplaceAll(cfg.Target.Object, "{"+word+"}", "")
		}
	}

	// done
	cfg.Prepared = true
	return
}

// Marshal marshals into JSON
func (cfg *Config) Marshal() (cfgBytes []byte, err error) {

	cfg.Source.Conn = cfg.SrcConn.Info().Name
	cfg.Source.Data = cfg.SrcConn.Info().Data

	cfg.Target.Conn = cfg.TgtConn.Info().Name
	cfg.Target.Data = cfg.TgtConn.Info().Data

	cfgBytes, err = json.Marshal(cfg)
	if err != nil {
		err = g.Error(err, "Could not encode provided configuration into JSON")
		return
	}
	return
}

func (cfg *Config) FormatTargetObjectName() (err error) {
	m := g.M(
		"run_timestamp", time.Now().Format("2006_01_02_150405"),
	)

	if cfg.Source.Conn != "" {
		m["source_name"] = strings.ToLower(cfg.Source.Conn)
	}

	if cfg.Target.Conn != "" {
		m["target_name"] = strings.ToLower(cfg.Target.Conn)
	}

	if cfg.SrcConn.Type.IsAPI() {
		m["stream_name"] = strings.ToLower(cfg.Source.Stream)
	}

	if cfg.SrcConn.Type.IsDb() {
		table, err := database.ParseTableName(cfg.Source.Stream, cfg.SrcConn.Type)
		if err != nil {
			return g.Error(err, "could not parse stream table name")
		} else if table.SQL == "" {
			if table.Schema != "" {
				m["stream_schema"] = table.Schema
			}
			m["stream_table"] = table.Name
			m["stream_name"] = strings.ToLower(cfg.Source.Stream)
		}
	}

	if cfg.TgtConn.Type.IsDb() {
		if targetSchema := cast.ToString(cfg.Target.Data["schema"]); targetSchema != "" {
			m["target_schema"] = strings.ToLower(targetSchema)
		}
	}

	if cfg.SrcConn.Type.IsFile() {
		re, _ := regexp.Compile(`\W+`)
		url, err := net.NewURL(cfg.Source.Stream)
		if err != nil {
			return g.Error(err, "could not parse source stream url")
		}
		m["stream_name"] = strings.ToLower(cfg.Source.Stream)

		filePath := string(re.ReplaceAll([]byte(strings.TrimPrefix(url.Path(), "/")), []byte("_")))
		pathArr := strings.Split(strings.TrimSuffix(url.Path(), "/"), "/")
		fileName := string(re.ReplaceAll([]byte(pathArr[len(pathArr)-1]), []byte("_")))
		fileFolder := lo.Ternary(len(pathArr) > 1, pathArr[len(pathArr)-2], "")
		switch cfg.SrcConn.Type {
		case dbio.TypeFileS3:
			m["source_bucket"] = cfg.SrcConn.Data["bucket"]
			m["stream_file_folder"] = fileFolder
			m["stream_file_name"] = fileName
		case dbio.TypeFileGoogle:
			m["source_bucket"] = cfg.SrcConn.Data["bucket"]
			m["stream_file_folder"] = fileFolder
			m["stream_file_name"] = fileName
		case dbio.TypeFileAzure:
			m["source_account"] = cfg.SrcConn.Data["account"]
			m["source_container"] = cfg.SrcConn.Data["container"]
			m["stream_file_folder"] = fileFolder
			m["stream_file_name"] = fileName
			filePath = strings.TrimPrefix(filePath, cast.ToString(m["source_container"])+"_")
		case dbio.TypeFileLocal:
			path := strings.TrimPrefix(cfg.Source.Stream, "file://")
			fileFolder, fileName := filepath.Split(path)
			m["stream_file_folder"] = string(re.ReplaceAll([]byte(strings.TrimPrefix(fileFolder, "/")), []byte("_")))
			m["stream_file_name"] = string(re.ReplaceAll([]byte(strings.TrimPrefix(fileName, "/")), []byte("_")))
			filePath = string(re.ReplaceAll([]byte(strings.TrimPrefix(path, "/")), []byte("_")))
		}
		if filePath != "" {
			m["stream_file_path"] = filePath
		}
	}

	if t := schemeType(cfg.Target.Object); t.IsFile() {
		switch t {
		case dbio.TypeFileS3:
			m["target_bucket"] = cfg.Target.Data["bucket"]
		case dbio.TypeFileGoogle:
			m["target_bucket"] = cfg.Target.Data["bucket"]
		case dbio.TypeFileAzure:
			m["target_account"] = cfg.Target.Data["account"]
			m["target_container"] = cfg.Target.Data["container"]
		}
	}

	// pass env values
	for k, v := range cfg.Env {
		if _, found := m[k]; !found && v != "" {
			m[k] = v
		}
	}

	// check that no value is blank
	blankKeys := []string{}
	for k, v := range m {
		if cast.ToString(v) == "" {
			blankKeys = append(blankKeys, k)
		}
	}

	if len(blankKeys) > 0 {
		// return g.Error("blank values for: %s", strings.Join(blankKeys, ", "))
		g.Debug("Could not successfully format target object name. Blank values for: %s", strings.Join(blankKeys, ", "))
	}

	// replace placeholders
	cfg.Target.Object = strings.ToLower(g.Rm(cfg.Target.Object, m))

	// apply date map
	dateMap := iop.GetISO8601DateMap(time.Now())
	cfg.Target.Object = strings.TrimSpace(g.Rm(cfg.Target.Object, dateMap))

	if schemeType(cfg.Target.Object).IsFile() {
		cfg.Target.Data["url"] = cfg.Target.Object
		cfg.TgtConn.Data["url"] = cfg.Target.Object
	}

	return nil
}

// Config is the new config struct
type Config struct {
	Source  Source            `json:"source,omitempty" yaml:"source,omitempty"`
	Target  Target            `json:"target" yaml:"target"`
	Mode    Mode              `json:"mode,omitempty" yaml:"mode,omitempty"`
	Options ConfigOptions     `json:"options,omitempty" yaml:"options,omitempty"`
	Env     map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	SrcConn        connection.Connection `json:"_src_conn,omitempty" yaml:"_src_conn,omitempty"`
	TgtConn        connection.Connection `json:"_tgt_conn,omitempty" yaml:"_tgt_conn,omitempty"`
	Prepared       bool                  `json:"_prepared,omitempty" yaml:"_prepared,omitempty"`
	IncrementalVal string                `json:"-" yaml:"-"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (cfg *Config) Scan(value interface{}) error {
	return g.JSONScanner(cfg, value)
}

// Value return json value, implement driver.Valuer interface
func (cfg Config) Value() (driver.Value, error) {
	return g.JSONValuer(cfg, "{}")
}

// ConfigOptions are configuration options
type ConfigOptions struct {
	Debug  bool `json:"debug,omitempty" yaml:"debug,omitempty"`
	StdIn  bool `json:"-"`                                        // whether stdin is passed
	StdOut bool `json:"stdout,omitempty" yaml:"stdout,omitempty"` // whether to output to stdout
}

// Source is a source of data
type Source struct {
	Conn       string                 `json:"conn" yaml:"conn"`
	Stream     string                 `json:"stream,omitempty" yaml:"stream,omitempty"`
	Columns    []string               `json:"columns,omitempty" yaml:"columns,omitempty"`
	PrimaryKey []string               `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey  string                 `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	Limit      int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Options    *SourceOptions         `json:"options,omitempty" yaml:"options,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	columns iop.Columns `json:"-" yaml:"-"`
}

// Target is a target of data
type Target struct {
	Conn    string                 `json:"conn" yaml:"conn"`
	Object  string                 `json:"object,omitempty" yaml:"object,omitempty"`
	Options *TargetOptions         `json:"options,omitempty" yaml:"options,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	TmpTableCreated bool        `json:"-" yaml:"-"`
	columns         iop.Columns `json:"-" yaml:"-"`
}

// SourceOptions are connection and stream processing options
type SourceOptions struct {
	TrimSpace      *bool               `json:"trim_space,omitempty" yaml:"trim_space,omitempty"`
	EmptyAsNull    *bool               `json:"empty_as_null,omitempty" yaml:"empty_as_null,omitempty"`
	Header         *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Flatten        *bool               `json:"flatten,omitempty" yaml:"flatten,omitempty"`
	Compression    *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	NullIf         *string             `json:"null_if,omitempty" yaml:"null_if,omitempty"`
	DatetimeFormat string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	SkipBlankLines *bool               `json:"skip_blank_lines,omitempty" yaml:"skip_blank_lines,omitempty"`
	Delimiter      string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	MaxDecimals    *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
}

// TargetOptions are target connection and stream processing options
type TargetOptions struct {
	Header           *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Compression      *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	Concurrency      int                 `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	DatetimeFormat   string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	Delimiter        string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	FileMaxRows      int64               `json:"file_max_rows,omitempty" yaml:"file_max_rows,omitempty"`
	MaxDecimals      *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
	UseBulk          *bool               `json:"use_bulk,omitempty" yaml:"use_bulk,omitempty"`
	AddNewColumns    bool                `json:"add_new_columns,omitempty" yaml:"add_new_columns,omitempty"`
	AdjustColumnType *bool               `json:"adjust_column_type,omitempty" yaml:"adjust_column_type,omitempty"`

	TableTmp string `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
	TableDDL string `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	PreSQL   string `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`
	PostSQL  string `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`
}

var SourceFileOptionsDefault = SourceOptions{
	TrimSpace:      g.Bool(false),
	EmptyAsNull:    g.Bool(true),
	Header:         g.Bool(true),
	Flatten:        g.Bool(false),
	Compression:    iop.CompressorTypePtr(iop.AutoCompressorType),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	SkipBlankLines: g.Bool(false),
	Delimiter:      ",",
	MaxDecimals:    g.Int(9),
}

var SourceDBOptionsDefault = SourceOptions{
	EmptyAsNull:    g.Bool(true),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	MaxDecimals:    g.Int(9),
}

var SourceAPIOptionsDefault = SourceOptions{
	EmptyAsNull:    g.Bool(true),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	MaxDecimals:    g.Int(9),
}

var TargetFileOptionsDefault = TargetOptions{
	Header: g.Bool(true),
	Compression: lo.Ternary(
		os.Getenv("COMPRESSION") != "",
		iop.CompressorTypePtr(iop.CompressorType(os.Getenv("COMPRESSION"))),
		iop.CompressorTypePtr(iop.AutoCompressorType),
	),
	Concurrency: lo.Ternary(
		os.Getenv("CONCURRENCY") != "",
		cast.ToInt(os.Getenv("CONCURRENCY")),
		runtime.NumCPU(),
	),
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_ROWS")),
		0,
	),
	UseBulk:        g.Bool(true),
	DatetimeFormat: "auto",
	Delimiter:      ",",
	MaxDecimals:    g.Int(-1),
}

var TargetDBOptionsDefault = TargetOptions{
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_ROWS")),
		0,
	),
	UseBulk:        g.Bool(true),
	AddNewColumns:  true,
	DatetimeFormat: "auto",
	MaxDecimals:    g.Int(-1),
}

var TargetAPIOptionsDefault = TargetOptions{
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_ROWS")),
		0,
	),
	UseBulk:        g.Bool(true),
	DatetimeFormat: "auto",
	MaxDecimals:    g.Int(-1),
}
