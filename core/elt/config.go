package elt

import (
	"encoding/json"
	"github.com/flarco/dbio/connection"
	"io/ioutil"
	"os"

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

	err := yaml.Unmarshal(cfgBytes, &cfg)
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

var fileVarsDefault = map[string]string{
	"NULL_IF":             "NULL",
	"COMPRESSION":         "AUTO", // TODO: ZIP | GZIP | SNAPPY | NONE
	"EMPTY_FIELD_AS_NULL": "TRUE",
	"TRIM_SPACE":          "FALSE",
	"DATE_FORMAT":         "AUTO",
	"TIME_FORMAT":         "AUTO",
	"SKIP_BLANK_LINES":    "FALSE",
	"FILE_MAX_ROWS":       "0",
	"DELIMITER":           ",",
	"HEADER":              "TRUE",
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
	tgtConn, err := connection.NewConnectionFromMap(g.M("id", cfg.Target.Conn, "data", cfg.Target.Data))
	if err != nil {
		return g.Error(err, "could not create data conn for target")
	}
	cfg.TgtConn = tgtConn

	// Source
	if cfg.Source.Data == nil {
		cfg.Source.Data = g.M()
	}
	cfg.Source.Data["url"] = cfg.Source.URL
	srcConn, err := connection.NewConnectionFromMap(g.M("id", cfg.Source.Conn, "data", cfg.Source.Data))
	if err != nil {
		if cfg.Target.Dbt == nil {
			return g.Error(err, "could not create data conn for source")
		} else {
			err = nil
		}
	}
	cfg.SrcConn = srcConn

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
//			if _, ok := fileVarsDefault[k]; ok {
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
//			return g.Error(err, "could not decrypt fields for "+dConns[i].ID)
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
//			if _, ok := fileVarsDefault[k]; ok {
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
//			return g.Error(err, "could not encrypt fields for "+dConns[i].ID)
//		}
//	}
//	return nil
//}

// Config is the new config struct
type Config struct {
	SrcConn connection.Connection `json:"-"`
	Source  Source              `json:"source,omitempty" yaml:"source,omitempty"`

	TgtConn connection.Connection    `json:"-"`
	Target  Target                 `json:"target,omitempty" yaml:"target,omitempty"`
	Vars    map[string]interface{} `json:"vars,omitempty" yaml:"vars,omitempty"`

	StdIn  bool                   `json:"-"`                    // whether stdin is passed
	StdOut bool                   `json:"stdout" yaml:"stdout"` // whether to output to stdout
	Props  map[string]interface{} `json:"props" yaml:"props"`   // remove?
	Env    map[string]interface{} `json:"env" yaml:"env"`       // remove ?

	UpsertVal string `json:"-"`
	prepared  bool   `json:"-"`
}

// FileConfig is for file source / targets
type FileConfig struct {
	NullIf           string `json:"null_if" yaml:"null_if"`
	Compression      string `json:"compression" yaml:"compression"`
	EmptyFieldAsNull bool   `json:"empty_field_as_null" yaml:"empty_field_as_null"`
	TrimSpace        bool   `json:"trim_space" yaml:"trim_space"`
	DateFormat       string `json:"date_format" yaml:"date_format"`
	TimeFormat       string `json:"time_format" yaml:"time_format"`
	SkipBlankLines   bool   `json:"skip_blank_lines" yaml:"skip_blank_lines"`
	FileMaxRows      int    `json:"file_max_rows" yaml:"file_max_rows"`
	Delimiter        rune   `json:"delimiter" yaml:"delimiter"`
	Header           bool   `json:"header" yaml:"header"`
}

// NewFileConfig returns a default FileConfig
func NewFileConfig() *FileConfig {
	return &FileConfig{
		NullIf:           "NULL",
		Compression:      "AUTO",
		EmptyFieldAsNull: true,
		TrimSpace:        false,
		DateFormat:       "AUTO",
		TimeFormat:       "AUTO",
		SkipBlankLines:   false,
		FileMaxRows:      0,
		Delimiter:        ',',
		Header:           true,
	}
}

// Source is a source of data
type Source struct {
	Conn       string                 `json:"conn,omitempty" yaml:"conn,omitempty"`
	URL        string                 `json:"url,omitempty" yaml:"url,omitempty"`
	SQL        string                 `json:"sql,omitempty" yaml:"sql,omitempty"`
	Table      string                 `json:"table,omitempty" yaml:"table,omitempty"`
	Limit      int                    `json:"limit,omitempty" yaml:"limit,omitempty"`
	FileConfig *FileConfig            `json:"file_config,omitempty" yaml:"file_config,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	Columns iop.Columns `json:"-"`
}

// Target is a target of data
type Target struct {
	Conn       string                 `json:"conn,omitempty" yaml:"conn,omitempty"`
	URL        string                 `json:"url,omitempty" yaml:"url,omitempty"`
	Table      string                 `json:"table,omitempty" yaml:"table,omitempty"`
	TableDDL   string                 `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	TableTmp   string                 `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
	PreSQL     string                 `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`
	PostSQL    string                 `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`
	Mode       Mode                   `json:"mode,omitempty" yaml:"mode,omitempty"`
	Dbt        *dbt.Dbt               `json:"dbt,omitempty" yaml:"dbt,omitempty"`
	PrimaryKey []string               `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey  string                 `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	FileConfig *FileConfig            `json:"file_config,omitempty" yaml:"file_config,omitempty"`
	Data       map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	TmpTableCreated bool        `json:"-"`
	Columns         iop.Columns `json:"-"`
}
