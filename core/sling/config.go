package sling

import (
	"database/sql/driver"
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/g/net"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"

	"github.com/jmespath/go-jmespath"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"gopkg.in/yaml.v2"
)

// Mode is a load mode
type Mode string

const (
	// TruncateMode is to truncate
	TruncateMode Mode = "truncate"
	// FullRefreshMode is to drop
	FullRefreshMode Mode = "full-refresh"
	// AppendMode is to append
	AppendMode Mode = "append"
	// IncrementalMode is to incremental
	IncrementalMode Mode = "incremental"
	// SnapshotMode is to snapshot
	SnapshotMode Mode = "snapshot"
)

// NewConfig return a config object from a YAML / JSON string
func NewConfig(cfgStr string) (cfg *Config, err error) {
	// set default, unmarshalling will overwrite
	cfg = &Config{}
	cfg.SetDefault()

	err = cfg.Unmarshal(cfgStr)
	if err != nil {
		err = g.Error(err, "Unable to parse config string")
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
	cfg.Source.Options = sourceFileOptionsDefault
	cfg.Target.Options = targetFileOptionsDefault
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
		cfg.Env = g.M()
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

	// set default options
	m := g.M()
	g.Unmarshal(string(cfgBytes), &m)

	if cfg.Source.Stream != "" {
		val, err := jmespath.Search("source.options.header", m)
		if val == nil || err != nil {
			cfg.Source.Options.Header = true
		}
		val, err = jmespath.Search("source.options.delimiter", m)
		if val == nil || err != nil {
			cfg.Source.Options.Delimiter = ","
		}
	}

	if cfg.Target.Object != "" {
		val, err := jmespath.Search("target.options.header", m)
		if val == nil || err != nil {
			cfg.Target.Options.Header = true
		}
		val, err = jmespath.Search("target.options.delimiter", m)
		if val == nil || err != nil {
			cfg.Target.Options.Delimiter = ","
		}
		val, err = jmespath.Search("target.options.use_bulk", m)
		if val == nil || err != nil {
			cfg.Target.Options.UseBulk = true
		}
		val, err = jmespath.Search("target.options.concurrency", m)
		if val == nil || err != nil {
			cfg.Target.Options.Concurrency = runtime.NumCPU()
		}
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

// Prepare prepares the config
func (cfg *Config) Prepare() (err error) {
	if cfg.Prepared {
		return
	}

	// get local connections
	connsMap := lo.KeyBy(env.GetLocalConns(), func(c env.Conn) string {
		return strings.ToLower(c.Connection.Name)
	})

	// Check Inputs
	if !cfg.Options.StdIn && cfg.Source.Conn == "" && cfg.Target.Conn == "" {
		return g.Error("invalid source connection. Input is blank or not found")
	}
	if !cfg.Options.StdOut && cfg.Target.Conn == "" && cfg.Target.Object == "" {
		return g.Error("invalid target connection. Input is blank or not found")
	}

	if cfg.Options.Debug && os.Getenv("_DEBUG") == "" {
		os.Setenv("_DEBUG", "DEBUG")
	}

	isFileURL := func(url string) bool {
		if !strings.Contains(url, "://") {
			return false
		}
		U, err := net.NewURL(url)
		if err != nil {
			return false
		}
		t, _ := dbio.ValidateType(U.U.Scheme)
		return t.IsFile()
	}

	// Set Target
	cfg.Target.Object = strings.TrimSpace(cfg.Target.Object)
	if cfg.Target.Data == nil || len(cfg.Target.Data) == 0 {
		cfg.Target.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Target.Conn)]; ok {
			cfg.TgtConn = c.Connection
		}
	}

	if isFileURL(cfg.Target.Object) {
		cfg.Target.Data["url"] = cfg.Target.Object
		cfg.TgtConn.Data["url"] = cfg.Target.Object
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
		cfg.Target.Options.Concurrency = 1
		os.Setenv("CONCURRENCY", "1")
	}

	// Set Source
	cfg.Source.Stream = strings.TrimSpace(cfg.Source.Stream)
	if cfg.Source.Data == nil || len(cfg.Source.Data) == 0 {
		cfg.Source.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Source.Conn)]; ok {
			cfg.SrcConn = c.Connection
		}
	}

	if isFileURL(cfg.Source.Stream) {
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

	// set mode if primary key is providedand mode isnn't
	if len(cfg.Source.PrimaryKey) > 0 && cfg.Mode == "" {
		cfg.Mode = IncrementalMode
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

// Config is the new config struct
type Config struct {
	Source  Source                 `json:"source,omitempty" yaml:"source,omitempty"`
	Target  Target                 `json:"target" yaml:"target"`
	Mode    Mode                   `json:"mode,omitempty" yaml:"mode,omitempty"`
	Options ConfigOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Env     map[string]interface{} `json:"env" yaml:"env"`

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
	Debug  bool `json:"debug" yaml:"debug"`
	StdIn  bool `json:"-"`                    // whether stdin is passed
	StdOut bool `json:"stdout" yaml:"stdout"` // whether to output to stdout
}

// Source is a source of data
type Source struct {
	Conn       string                 `json:"conn" yaml:"conn"`
	Stream     string                 `json:"stream,omitempty" yaml:"stream,omitempty"`
	PrimaryKey []string               `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey  string                 `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	Limit      int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Options    SourceOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	Columns iop.Columns `json:"-" yaml:"-"`
}

// Target is a target of data
type Target struct {
	Conn    string                 `json:"conn" yaml:"conn"`
	Object  string                 `json:"object,omitempty" yaml:"object,omitempty"`
	Options TargetOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	TmpTableCreated bool        `json:"-" yaml:"-"`
	Columns         iop.Columns `json:"-" yaml:"-"`
}

// SourceOptions are connection and stream processing options
type SourceOptions struct {
	TrimSpace      bool               `json:"trim_space" yaml:"trim_space"`
	EmptyAsNull    bool               `json:"empty_as_null" yaml:"empty_as_null"`
	Header         bool               `json:"header" yaml:"header"`
	Compression    iop.CompressorType `json:"compression" yaml:"compression"`
	NullIf         string             `json:"null_if" yaml:"null_if"`
	DatetimeFormat string             `json:"datetime_format" yaml:"datetime_format"`
	SkipBlankLines bool               `json:"skip_blank_lines" yaml:"skip_blank_lines"`
	Delimiter      string             `json:"delimiter" yaml:"delimiter"`
	MaxDecimals    int                `json:"max_decimals" yaml:"max_decimals"`
}

// TargetOptions are target connection and stream processing options
type TargetOptions struct {
	Header         bool               `json:"header" yaml:"header"`
	Compression    iop.CompressorType `json:"compression" yaml:"compression"`
	Concurrency    int                `json:"concurrency" yaml:"concurrency"`
	DatetimeFormat string             `json:"datetime_format" yaml:"datetime_format"`
	Delimiter      string             `json:"delimiter" yaml:"delimiter"`
	FileMaxRows    int64              `json:"file_max_rows" yaml:"file_max_rows"`
	MaxDecimals    int                `json:"max_decimals" yaml:"max_decimals"`

	UseBulk  bool   `json:"use_bulk" yaml:"use_bulk"`
	TableDDL string `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	TableTmp string `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
	PreSQL   string `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`
	PostSQL  string `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`
}

var sourceFileOptionsDefault = SourceOptions{
	TrimSpace:      false,
	EmptyAsNull:    true,
	Header:         true,
	Compression:    iop.AutoCompressorType,
	NullIf:         "null",
	DatetimeFormat: "auto",
	SkipBlankLines: false,
	Delimiter:      ",",
	MaxDecimals:    -1,
}

var targetFileOptionsDefault = TargetOptions{
	Header: true,
	Compression: lo.Ternary(
		os.Getenv("COMPRESSION") != "",
		iop.CompressorType(os.Getenv("COMPRESSION")),
		iop.AutoCompressorType,
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
	UseBulk:        true,
	DatetimeFormat: "auto",
	Delimiter:      ",",
	MaxDecimals:    -1,
}
