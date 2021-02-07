package elt

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/flarco/dbio/connection"

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

	if cfg.Props == nil {
		cfg.Props = g.Map{}
	}

	if cfg.Env == nil {
		cfg.Env = g.Map{}
	}

	if cfg.Source.Data == nil {
		cfg.Source.Data = g.Map{}
	}

	if cfg.Target.Data == nil {
		cfg.Target.Data = g.Map{}
	}
	return nil
}

// Prepare prepares the config
func (cfg *Config) Prepare() (err error) {
	if cfg.prepared {
		return
	}

	// Set Source
	if cfg.Source.Conn != "" && cfg.Source.URL == "" {
		cfg.Source.URL = cfg.Source.Conn
	}
	if !cfg.StdIn && cfg.Source.URL == "" && cfg.Target.Dbt == nil && cfg.Target.URL == "" {
		return g.Error("invalid source connection. URL is blank or not found")
	}

	// Set Target
	if cfg.Target.Conn != "" && cfg.Target.URL == "" {
		cfg.Target.URL = cfg.Target.Conn
	}
	if !cfg.StdOut && cfg.Target.URL == "" {
		return g.Error("invalid target connection. URL is blank or not found")
	}

	// Target
	if cfg.Target.Data == nil {
		cfg.Target.Data = g.M()
	}
	cfg.Target.Data["url"] = cfg.Target.URL
	tgtConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Target.Conn, "data", cfg.Target.Data))
	if err != nil {
		return g.Error(err, "could not create data conn for target")
	}
	cfg.TgtConn = tgtConn

	// Source
	if cfg.Source.Data == nil {
		cfg.Source.Data = g.M()
	}
	cfg.Source.Data["url"] = cfg.Source.URL
	srcConn, err := connection.NewConnectionFromMap(g.M("name", cfg.Source.Conn, "data", cfg.Source.Data))
	if err != nil {
		if cfg.Target.Dbt == nil {
			return g.Error(err, "could not create data conn for source")
		} else {
			err = nil
		}
	}
	cfg.SrcConn = srcConn

	// fill target table schema if needed
	if schema, ok := cfg.Target.Data["schema"]; ok && cfg.Target.Table != "" && !strings.Contains(cfg.Target.Table, ".") {
		cfg.Target.Table = g.F("%s.%s", schema, cfg.Target.Table)
	}

	cfg.prepared = true
	return
}

// Marshal marshals into JSON
func (cfg *Config) Marshal() (cfgBytes []byte, err error) {

	cfg.Source.Conn = cfg.SrcConn.Info().Name
	cfg.Source.URL = cfg.SrcConn.URL()
	cfg.Source.Data = cfg.SrcConn.Info().Data

	cfg.Target.Conn = cfg.TgtConn.Info().Name
	cfg.Target.URL = cfg.TgtConn.URL()
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
	SrcConn connection.Connection `json:"-"`
	Source  Source                `json:"source,omitempty" yaml:"source,omitempty"`

	TgtConn connection.Connection  `json:"-"`
	Target  Target                 `json:"target" yaml:"target"`
	Vars    map[string]interface{} `json:"vars,omitempty" yaml:"vars,omitempty"` // remove?

	StdIn  bool                   `json:"-"`                    // whether stdin is passed
	StdOut bool                   `json:"stdout" yaml:"stdout"` // whether to output to stdout
	Props  map[string]interface{} `json:"props" yaml:"props"`   // remove?
	Env    map[string]interface{} `json:"env" yaml:"env"`       // remove ?

	UpsertVal string `json:"-"`
	prepared  bool   `json:"-"`
}

// Source is a source of data
type Source struct {
	Conn    string  `json:"conn" yaml:"conn"`
	Object  string  `json:"object" yaml:"object"`
	Limit   int     `json:"limit,omitempty" yaml:"limit,omitempty"`
	Options Options `json:"options,omitempty" yaml:"options,omitempty"`

	URL   string                 `json:"url,omitempty" yaml:"url,omitempty"`     // use conn
	SQL   string                 `json:"sql,omitempty" yaml:"sql,omitempty"`     // use object
	Table string                 `json:"table,omitempty" yaml:"table,omitempty"` // use object
	Data  map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`   // remove

	Columns iop.Columns `json:"-"`
}

// Target is a target of data
type Target struct {
	Conn       string   `json:"conn" yaml:"conn"`
	Object     string   `json:"object,omitempty" yaml:"object,omitempty"`
	Options    Options  `json:"options,omitempty" yaml:"options,omitempty"`
	Mode       Mode     `json:"mode,omitempty" yaml:"mode,omitempty"`
	Dbt        *dbt.Dbt `json:"dbt,omitempty" yaml:"dbt,omitempty"`
	PrimaryKey []string `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey  string   `json:"update_key,omitempty" yaml:"update_key,omitempty"`

	TableDDL string                 `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"` // use options.TableDDL
	TableTmp string                 `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"` // use options.TableTmp
	PreSQL   string                 `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`     // use options.PreSQL
	PostSQL  string                 `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`   // use options.PostSQL
	URL      string                 `json:"url,omitempty" yaml:"url,omitempty"`             // use object
	Table    string                 `json:"table,omitempty" yaml:"table,omitempty"`         // use object
	Data     map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`           //remove ?

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

// Options are connection and stream processing options
type Options struct {
	TrimSpace      bool        `json:"trim_space" yaml:"trim_space"`
	EmptyAsNull    bool        `json:"empty_as_null" yaml:"empty_as_null"`
	Header         bool        `json:"header" yaml:"header"`
	Compression    Compression `json:"compression" yaml:"compression"`
	NullIf         string      `json:"null_if" yaml:"null_if"`
	DatetimeFormat string      `json:"datetime_format" yaml:"datetime_format"`
	SkipBlankLines bool        `json:"skip_blank_lines" yaml:"skip_blank_lines"`
	Delimiter      rune        `json:"delimiter" yaml:"delimiter"`
	FileMaxRows    int64       `json:"file_max_rows" yaml:"file_max_rows"`
	MaxDecimals    int         `json:"max_decimals" yaml:"max_decimals"`
	TableDDL       string      `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	TableTmp       string      `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
}

var fileOptionsDefault = Options{
	TrimSpace:      false,
	EmptyAsNull:    true,
	Header:         true,
	Compression:    CompressionAuto,
	NullIf:         "null",
	DatetimeFormat: "auto",
	SkipBlankLines: false,
	Delimiter:      ',',
	FileMaxRows:    0,
	MaxDecimals:    -1,
}
