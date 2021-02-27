package elt

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/flarco/dbio"
	"github.com/flarco/dbio/connection"
	"github.com/flarco/g/net"

	"github.com/jmespath/go-jmespath"
	"github.com/slingdata-io/sling/core/dbt"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"gopkg.in/yaml.v2"
)

// Mode is a load mode
type Mode string

const (
	// TruncateMode is to truncate
	TruncateMode Mode = "truncate"
	// DropMode is to drop
	DropMode Mode = "drop"
	// AppendMode is to append
	AppendMode Mode = "append"
	// UpsertMode is to upsert
	UpsertMode Mode = "upsert"
	// SnapshotMode is to snapshot
	SnapshotMode Mode = "snapshot"
)

// NewConfig return a config object from a string
func NewConfig(cfgStr string) (cfg Config, err error) {
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

	if cfg.Target.Data == nil {
		cfg.Target.Data = g.M()
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

	// Check Inputs
	if !cfg.Options.StdIn && cfg.Source.Conn == "" && cfg.Target.Conn == "" {
		return g.Error("invalid source connection. Input is blank or not found")
	}
	if !cfg.Options.StdOut && cfg.Target.Conn == "" && cfg.Target.Object == "" {
		return g.Error("invalid target connection. Input is blank or not found")
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
	if cfg.Target.Data == nil {
		cfg.Target.Data = g.M()
	}
	if isFileURL(cfg.Target.Object) {
		cfg.Target.Data["url"] = cfg.Target.Object
	} else {
		cfg.Target.Data["url"] = cfg.Target.Conn
	}
	tgtConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Target.Conn, "data", cfg.Target.Data))
	if err != nil {
		return g.Error(err, "could not create data conn for target")
	}
	cfg.TgtConn = tgtConn

	// Set Source
	cfg.Source.Stream = strings.TrimSpace(cfg.Source.Stream)
	if cfg.Source.Data == nil {
		cfg.Source.Data = g.M()
	}
	if isFileURL(cfg.Source.Stream) {
		cfg.Source.Data["url"] = cfg.Source.Stream
	} else {
		cfg.Source.Data["url"] = cfg.Source.Conn
	}
	srcConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Source.Conn, "data", cfg.Source.Data))
	if err != nil {
		if cfg.Target.DbtConfig == nil {
			return g.Error(err, "could not create data conn for source")
		}
		err = nil
	}
	cfg.SrcConn = srcConn

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

// Decrypt decrypts the sensitive connection strings
//func (cfg *Config) Decrypt(secret string) (err error) {
//	decrypt := func(dConn *connection.DataConn) error {
//		dConn.URL, err = g.Decrypt(dConn.URL, secret)
//		if err != nil {
//			return g.Error(err, "could not decrypt")
//		}
//		for k := range dConn.Data {
//			if _, ok := fileOptionsDefault[k]; ok {
//				continue
//			}
//			dConn.Data[k], err = g.Decrypt(dConn.DataS()[k], secret)
//			if err != nil {
//				return g.Error(err, "could not decrypt")
//			}
//		}
//		return nil
//	}
//
//	dConns := []*connection.DataConn{&cfg.SrcConn, &cfg.TgtConn}
//	for i := range dConns {
//		err = decrypt(dConns[i])
//		if err != nil {
//			return g.Error(err, "could not decrypt fields for "+dConns[i].Name)
//		}
//	}
//	return nil
//}

// Encrypt encrypts the sensitive connection strings
//func (cfg *Config) Encrypt(secret string) (err error) {
//	encrypt := func(dConn *connection.DataConn) error {
//		dConn.URL, err = g.Encrypt(dConn.URL, secret)
//		if err != nil {
//			return g.Error(err, "could not decrypt")
//		}
//		for k := range dConn.Data {
//			if _, ok := fileOptionsDefault[k]; ok {
//				continue
//			}
//			dConn.Data[k], err = g.Encrypt(dConn.DataS()[k], secret)
//			if err != nil {
//				return g.Error(err, "could not decrypt")
//			}
//		}
//		return nil
//	}
//
//	dConns := []*connection.DataConn{&cfg.SrcConn, &cfg.TgtConn}
//	for i := range dConns {
//		err = encrypt(dConns[i])
//		if err != nil {
//			return g.Error(err, "could not encrypt fields for "+dConns[i].Name)
//		}
//	}
//	return nil
//}

// Config is the new config struct
type Config struct {
	Source  Source                 `json:"source,omitempty" yaml:"source,omitempty"`
	Target  Target                 `json:"target" yaml:"target"`
	Options ConfigOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Env     map[string]interface{} `json:"env" yaml:"env"`

	SrcConn   connection.Connection `json:"sc,omitempty" yaml:"sc,omitempty"`
	TgtConn   connection.Connection `json:"tc,omitempty" yaml:"tc,omitempty"`
	Prepared  bool                  `json:"prepared,omitempty" yaml:"prepared,omitempty"`
	UpsertVal string                `json:"-" yaml:"-"`
}

// ConfigOptions are configuration options
type ConfigOptions struct {
	StdIn     bool    `json:"-"`                    // whether stdin is passed
	StdOut    bool    `json:"stdout" yaml:"stdout"` // whether to output to stdout
	ProxyPIDs *string `json:"proxy_pids,omitempty" yaml:"proxy_pids,omitempty"`
}

// Source is a source of data
type Source struct {
	Conn    string                 `json:"conn" yaml:"conn"`
	Stream  string                 `json:"stream" yaml:"stream"`
	Limit   int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Options SourceOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	Columns iop.Columns `json:"-"`
}

// Target is a target of data
type Target struct {
	Conn       string                 `json:"conn" yaml:"conn"`
	Object     string                 `json:"object,omitempty" yaml:"object,omitempty"`
	Options    TargetOptions          `json:"options,omitempty" yaml:"options,omitempty"`
	Mode       Mode                   `json:"mode,omitempty" yaml:"mode,omitempty"`
	Dbt        string                 `json:"dbt,omitempty" yaml:"dbt,omitempty"` // the model string to run
	PrimaryKey []string               `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey  string                 `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	DbtConfig       *dbt.Dbt    `json:"dbt_config,omitempty" yaml:"dbt_config,omitempty"`
	TmpTableCreated bool        `json:"-"`
	Columns         iop.Columns `json:"-"`
}

// Compression is the compression to use
type Compression string

const (
	// CompressionAuto is Auto
	CompressionAuto Compression = "auto"
	// CompressionZip is Zip
	CompressionZip Compression = "zip"
	// CompressionGzip is Gzip
	CompressionGzip Compression = "gzip"
	// CompressionSnappy is Snappy
	CompressionSnappy Compression = "snappy"
	// CompressionNone is None
	CompressionNone Compression = ""
)

// SourceOptions are connection and stream processing options
type SourceOptions struct {
	TrimSpace      bool        `json:"trim_space" yaml:"trim_space"`
	EmptyAsNull    bool        `json:"empty_as_null" yaml:"empty_as_null"`
	Header         bool        `json:"header" yaml:"header"`
	Compression    Compression `json:"compression" yaml:"compression"`
	NullIf         string      `json:"null_if" yaml:"null_if"`
	DatetimeFormat string      `json:"datetime_format" yaml:"datetime_format"`
	SkipBlankLines bool        `json:"skip_blank_lines" yaml:"skip_blank_lines"`
	Delimiter      string      `json:"delimiter" yaml:"delimiter"`
	MaxDecimals    int         `json:"max_decimals" yaml:"max_decimals"`
}

// TargetOptions are target connection and stream processing options
type TargetOptions struct {
	Header         bool        `json:"header" yaml:"header"`
	Compression    Compression `json:"compression" yaml:"compression"`
	DatetimeFormat string      `json:"datetime_format" yaml:"datetime_format"`
	Delimiter      string      `json:"delimiter" yaml:"delimiter"`
	FileMaxRows    int64       `json:"file_max_rows" yaml:"file_max_rows"`
	MaxDecimals    int         `json:"max_decimals" yaml:"max_decimals"`

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
	Compression:    CompressionAuto,
	NullIf:         "null",
	DatetimeFormat: "auto",
	SkipBlankLines: false,
	Delimiter:      ",",
	MaxDecimals:    -1,
}

var targetFileOptionsDefault = TargetOptions{
	Header:         true,
	Compression:    CompressionAuto,
	DatetimeFormat: "auto",
	Delimiter:      ",",
	MaxDecimals:    -1,
}
