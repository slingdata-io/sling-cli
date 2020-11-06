package elt

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling/core/iop"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
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
		cfg.Props = map[string]interface{}{}
	}

	if cfg.Env == nil {
		cfg.Env = map[string]interface{}{}
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
	cfg.SrcConn = iop.DataConn{Vars: map[string]interface{}{}}
	cfg.TgtConn = iop.DataConn{Vars: map[string]interface{}{}}
	cfg.SrcFile = iop.DataConn{Vars: map[string]interface{}{}}
	cfg.TgtFile = iop.DataConn{Vars: map[string]interface{}{}}

	prepFile := func(file *iop.DataConn, fileObj interface{}, defName string) {
		switch fileObj.(type) {
		case map[interface{}]interface{}:
			m := fileObj.(map[interface{}]interface{})
			for ki, vi := range m {
				k := strings.ToUpper(cast.ToString(ki))
				v := cast.ToString(vi)
				if k == "URL" || k == "PATH" {
					file.URL = v
				} else if k == "NAME" || k == "CONN" || k == "ID" {
					file.ID = v
				} else {
					if k == "FILE_MAX_ROWS" {
						k = "SLING_FILE_ROW_LIMIT"
					}
					if k == "DELIMITER" {
						os.Setenv("SLING_DELIMITER", v) // so that it can detected
					}
					if k == "COMPRESSION" {
						k = "SLING_COMPRESSION"
						v = strings.ToUpper(v)
					}

					file.Vars[k] = v
					if strings.HasPrefix(v, "$") {
						if newV := os.Getenv(strings.TrimLeft(v, "$")); newV != "" {
							file.Vars[k] = newV
						}
					}
				}
			}
		default:
			file.URL = cast.ToString(fileObj)
		}

		for k, v := range fileVarsDefault {
			if _, ok := file.Vars[k]; !ok {
				file.Vars[k] = v
			}
		}
	}

	prepConn := func(conn *iop.DataConn, connObj interface{}, defName string) {
		switch connObj.(type) {
		case map[interface{}]interface{}:
			m := connObj.(map[interface{}]interface{})
			for ki, vi := range m {
				k := strings.ToUpper(cast.ToString(ki))
				v := cast.ToString(vi)
				if k == "URL" {
					conn.URL = v
				} else if k == "NAME" || k == "ID" {
					conn.ID = v
				} else {
					conn.Vars[k] = v
					if strings.HasPrefix(v, "$") {
						if newV := os.Getenv(strings.TrimLeft(v, "$")); newV != "" {
							conn.Vars[k] = newV
						}
					}
				}
			}
		default:
			objStr := cast.ToString(connObj)
			if strings.Contains(objStr, "://") || strings.Contains(objStr, ";") {
				conn.URL = objStr
			} else {
				conn.ID = objStr
				conn.URL = os.Getenv(conn.ID)
			}
		}
	}

	prepConn(&cfg.SrcConn, cfg.SrcConnObj, "sourceDB")
	prepConn(&cfg.TgtConn, cfg.TgtConnObj, "targetDB")
	prepFile(&cfg.SrcFile, cfg.SrcFileObj, "sourceFile")
	prepFile(&cfg.TgtFile, cfg.TgtFileObj, "targetFile")
	cfg.SrcConnObj = nil
	cfg.TgtConnObj = nil
	cfg.SrcFileObj = nil
	cfg.TgtFileObj = nil

	cfg.prepared = true
	return
}

// Marshal marshals into JSON
func (cfg *Config) Marshal() (cfgBytes []byte, err error) {
	cfg.SrcFileObj = nil
	cfg.TgtFileObj = nil
	cfg.SrcConnObj = nil
	cfg.TgtConnObj = nil

	srcDbProvided := cfg.SrcConn.URL != ""
	tgtDbProvided := cfg.TgtConn.URL != ""

	if srcDbProvided {
		srcM := map[string]string{
			"id":  cfg.SrcConn.ID,
			"url": cfg.SrcConn.URL,
		}
		if cfg.SrcConn.ID == "" {
			delete(srcM, "id")
		}
		for k, v := range cfg.SrcConn.VarsS() {
			srcM[k] = v
		}
		cfg.SrcConnObj = srcM
	}

	if tgtDbProvided {
		tgtM := map[string]string{
			"id":  cfg.TgtConn.ID,
			"url": cfg.TgtConn.URL,
		}
		if cfg.TgtConn.ID == "" {
			delete(tgtM, "id")
		}
		for k, v := range cfg.TgtConn.VarsS() {
			tgtM[k] = v
		}
		cfg.TgtConnObj = tgtM
	}

	if cfg.SrcFile.URL != "" {
		srcM := map[string]string{
			"id":  cfg.SrcFile.ID,
			"url": cfg.SrcFile.URL,
		}
		if cfg.SrcFile.ID == "" {
			delete(srcM, "id")
		}
		for k, v := range cfg.SrcFile.VarsS() {
			srcM[k] = v
		}
		cfg.SrcFileObj = srcM
	}

	if cfg.TgtFile.URL != "" {
		tgtM := map[string]string{
			"id":  cfg.TgtFile.ID,
			"url": cfg.TgtFile.URL,
		}
		if cfg.TgtFile.ID == "" {
			delete(tgtM, "id")
		}
		for k, v := range cfg.TgtFile.VarsS() {
			tgtM[k] = v
		}
		cfg.TgtFileObj = tgtM
	}

	cfgBytes, err = json.Marshal(cfg)
	if err != nil {
		err = g.Error(err, "Could not encode provided configuration into JSON")
		return
	}
	return
}

// Decrypt decrypts the sensitive connection strings
func (cfg *Config) Decrypt(secret string) (err error) {
	decrypt := func(dConn *iop.DataConn) error {
		dConn.URL, err = g.Decrypt(dConn.URL, secret)
		if err != nil {
			return g.Error(err, "could not decrypt")
		}
		for k := range dConn.Vars {
			if _, ok := fileVarsDefault[k]; ok {
				continue
			}
			dConn.Vars[k], err = g.Decrypt(dConn.VarsS()[k], secret)
			if err != nil {
				return g.Error(err, "could not decrypt")
			}
		}
		return nil
	}

	dConns := []*iop.DataConn{&cfg.SrcConn, &cfg.SrcFile, &cfg.TgtConn, &cfg.TgtFile}
	for i := range dConns {
		err = decrypt(dConns[i])
		if err != nil {
			return g.Error(err, "could not decrypt fields for "+dConns[i].ID)
		}
	}
	return nil
}

// Encrypt encrypts the sensitive connection strings
func (cfg *Config) Encrypt(secret string) (err error) {
	encrypt := func(dConn *iop.DataConn) error {
		dConn.URL, err = g.Encrypt(dConn.URL, secret)
		if err != nil {
			return g.Error(err, "could not decrypt")
		}
		for k := range dConn.Vars {
			if _, ok := fileVarsDefault[k]; ok {
				continue
			}
			dConn.Vars[k], err = g.Encrypt(dConn.VarsS()[k], secret)
			if err != nil {
				return g.Error(err, "could not decrypt")
			}
		}
		return nil
	}

	dConns := []*iop.DataConn{&cfg.SrcConn, &cfg.SrcFile, &cfg.TgtConn, &cfg.TgtFile}
	for i := range dConns {
		err = encrypt(dConns[i])
		if err != nil {
			return g.Error(err, "could not encrypt fields for "+dConns[i].ID)
		}
	}
	return nil
}

// Config is a config for the sling task
type Config struct {
	SrcConn    iop.DataConn `json:"-"`
	SrcConnObj interface{}  `json:"src_conn" yaml:"src_conn"`
	SrcTable   string       `json:"src_table" yaml:"src_table"`
	SrcSQL     string       `json:"src_sql" yaml:"src_sql"`
	Limit      uint64       `json:"limit" yaml:"limit"`

	TgtConn     iop.DataConn `json:"-"`
	TgtConnObj  interface{}  `json:"tgt_conn" yaml:"tgt_conn"`
	TgtTable    string       `json:"tgt_table" yaml:"tgt_table"`
	TgtTableDDL string       `json:"tgt_table_ddl" yaml:"tgt_table_ddl"`
	TgtTableTmp string       `json:"tgt_table_tmp" yaml:"tgt_table_tmp"`
	TgtPreSQL   string       `json:"pre_sql" yaml:"pre_sql"`
	TgtPostSQL  string       `json:"post_sql" yaml:"post_sql"`
	TgtPostDbt  g.Map    `json:"post_dbt" yaml:"post_dbt"`
	PrimaryKey  string       `json:"primary_key" yaml:"primary_key"`
	UpdateKey   string       `json:"update_key" yaml:"update_key"`
	Mode        string       `json:"mode" yaml:"mode"` // append, upsert, truncate, drop

	SrcFile    iop.DataConn `json:"-"`
	SrcFileObj interface{}  `json:"src_file" yaml:"src_file"`

	TgtFile    iop.DataConn `json:"-"`
	TgtFileObj interface{}  `json:"tgt_file" yaml:"tgt_file"`

	Options string                 `json:"options" yaml:"options"`
	Props   map[string]interface{} `json:"props" yaml:"props"`
	Env     map[string]interface{} `json:"env" yaml:"env"`

	Remote bool   `json:"remote" yaml:"remote"`
	Email  string `json:"email" yaml:"email"` // email those addresses after

	UpsertVal       string `json:"-"`
	StdIn           bool   `json:"-"`                    // whether stdin is passed
	StdOut          bool   `json:"stdout" yaml:"stdout"` // whether to output to stdout
	SrcColumns      iop.Columns
	TgtColumns      iop.Columns
	TmpTableCreated bool
	prepared        bool
}
