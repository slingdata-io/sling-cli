package sling

import (
	"database/sql/driver"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/slingdata-io/sling-cli/core/dbio/filesys"
	"github.com/spf13/cast"

	"github.com/flarco/g"
	jsoniter "github.com/json-iterator/go"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
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
	// BackfillMode is to backfill
	BackfillMode Mode = "backfill"
)

// ColumnCasing is the casing method to use
type ColumnCasing string

const (
	SourceColumnCasing ColumnCasing = "source" // keeps source column name casing. The default.
	TargetColumnCasing ColumnCasing = "target" // converts casing according to target database. Lower-case for files.
	SnakeColumnCasing  ColumnCasing = "snake"  // converts snake casing according to target database. Lower-case for files.
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
	case dbio.KindDatabase:
		sourceOptions = SourceDBOptionsDefault
	default:
		sourceOptions = SourceDBOptionsDefault
	}

	if cfg.Source.Options == nil {
		cfg.Source.Options = &SourceOptions{}
	}
	cfg.Source.Options.SetDefaults(sourceOptions)

	// set target options
	var targetOptions TargetOptions
	switch cfg.TgtConn.Type.Kind() {
	case dbio.KindFile:
		targetOptions = TargetFileOptionsDefault
	case dbio.KindDatabase:
		targetOptions = TargetDBOptionsDefault
	default:
		targetOptions = TargetDBOptionsDefault
	}

	if cfg.Target.Options == nil {
		cfg.Target.Options = &TargetOptions{}
	}
	cfg.Target.Options.SetDefaults(targetOptions)

	if cfg.Target.Options.AdjustColumnType == nil && (cfg.SrcConn.Type.Kind() == dbio.KindFile || cfg.Options.StdIn) {
		// if source stream is file, we have no schema reference
		cfg.Target.Options.AdjustColumnType = g.Bool(false)
	}

	// set max_decimals
	switch cfg.TgtConn.Type {
	case dbio.TypeDbBigQuery, dbio.TypeDbBigTable:
		cfg.Source.Options.MaxDecimals = g.Int(9)
		cfg.Target.Options.MaxDecimals = g.Int(9)
	case dbio.TypeDbClickhouse:
		cfg.Source.Options.MaxDecimals = g.Int(11)
		cfg.Target.Options.MaxDecimals = g.Int(11)
	}

	// set default transforms
	switch cfg.SrcConn.Type {
	case dbio.TypeDbMySQL, dbio.TypeDbMariaDB, dbio.TypeDbStarRocks:
		// parse_bit for MySQL
		cfg.Source.Options.extraTransforms = append(cfg.Source.Options.extraTransforms, "parse_bit")
	}

	// set default metadata
	switch {
	case g.In(cfg.TgtConn.Type, dbio.TypeDbStarRocks):
		cfg.Source.Options.extraTransforms = append(cfg.Source.Options.extraTransforms, "parse_bit")
	case g.In(cfg.TgtConn.Type, dbio.TypeDbBigQuery):
		cfg.Target.Options.DatetimeFormat = "2006-01-02 15:04:05.000000-07"
	}

	// set vars
	for k, v := range cfg.Env {
		os.Setenv(k, v)
	}

	// default mode
	if cfg.Mode == "" {
		cfg.Mode = FullRefreshMode
	}

	if val := os.Getenv("SLING_LOADED_AT_COLUMN"); val != "" {
		cfg.MetadataLoadedAt = g.Bool(cast.ToBool(val))
	}
	if val := os.Getenv("SLING_STREAM_URL_COLUMN"); val != "" {
		cfg.MetadataStreamURL = cast.ToBool(val)
	}
	if val := os.Getenv("SLING_ROW_ID_COLUMN"); val != "" {
		cfg.MetadataRowID = cast.ToBool(val)
	}
	if val := os.Getenv("SLING_ROW_NUM_COLUMN"); val != "" {
		cfg.MetadataRowNum = cast.ToBool(val)
	}
	if val := os.Getenv("SAMPLE_SIZE"); val != "" {
		iop.SampleSize = cast.ToInt(val)
	}
}

// Unmarshal parse a configuration file path or config text
func (cfg *Config) Unmarshal(cfgStr string) error {
	// expand variables
	cfgStr = expandEnvVars(cfgStr)

	cfgBytes := []byte(cfgStr)
	_, errStat := os.Stat(cfgStr)
	if errStat == nil {
		cfgFile, err := os.Open(cfgStr)
		if err != nil {
			return g.Error(err, "Unable to open cfgStr: "+cfgStr)
		}

		cfgBytes, err = io.ReadAll(cfgFile)
		if err != nil {
			return g.Error(err, "could not read from cfgFile")
		}
	}

	err := yaml.Unmarshal(cfgBytes, cfg)
	if err != nil {
		if errStat != nil && !strings.Contains(cfgStr, "\n") && !strings.Contains(cfgStr, ": ") {
			return g.Error(errStat, "Error parsing config. Invalid path or raw config provided")
		} else if strings.Contains(string(cfgBytes), "streams:") || strings.Contains(string(cfgBytes), `"streams"`) {
			return g.Error("Error parsing config. Is your config file a replication? If so, please use the -r flag instead of -c")
		}
		return g.Error(err, "Error parsing config")
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

	// add config path
	if g.PathExists(cfgStr) && !cfg.ReplicationMode {
		cfg.Env["SLING_CONFIG_PATH"] = cfgStr
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

func (cfg *Config) sourceIsFile() bool {
	return cfg.Options.StdIn || cfg.SrcConn.Info().Type.IsFile()
}

func (cfg *Config) DetermineType() (Type JobType, err error) {

	srcFileProvided := cfg.sourceIsFile()
	tgtFileProvided := cfg.Options.StdOut || cfg.TgtConn.Info().Type.IsFile()
	srcDbProvided := cfg.SrcConn.Info().Type.IsDb()
	tgtDbProvided := cfg.TgtConn.Info().Type.IsDb()
	srcStreamProvided := cfg.Source.Stream != ""

	summary := g.F("srcFileProvided: %t, tgtFileProvided: %t, srcDbProvided: %t, tgtDbProvided: %t, srcStreamProvided: %t", srcFileProvided, tgtFileProvided, srcDbProvided, tgtDbProvided, srcStreamProvided)
	g.Trace(summary)

	if cfg.Mode == "" {
		if cfg.Source.PrimaryKeyI != nil || cfg.Source.UpdateKey != "" {
			cfg.Mode = IncrementalMode
		} else {
			cfg.Mode = FullRefreshMode
		}
	}

	validMode := g.In(cfg.Mode, FullRefreshMode, IncrementalMode, BackfillMode, SnapshotMode, TruncateMode)
	if !validMode {
		err = g.Error("must specify valid mode: full-refresh, incremental, backfill, snapshot or truncate")
		return
	}

	if cfg.Mode == IncrementalMode {
		if cfg.SrcConn.Info().Type == dbio.TypeDbBigTable {
			// use default keys if none are provided
			if len(cfg.Source.PrimaryKey()) == 0 {
				cfg.Source.PrimaryKeyI = []string{"_bigtable_key"}
			}

			if cfg.Source.UpdateKey == "" {
				cfg.Source.UpdateKey = "_bigtable_timestamp"
			}
		} else if srcFileProvided && cfg.Source.UpdateKey == slingLoadedAtColumn {
			// need to loaded_at column for file incremental
			cfg.MetadataLoadedAt = g.Bool(true)
		} else if cfg.Source.UpdateKey == "" && len(cfg.Source.PrimaryKey()) == 0 {
			err = g.Error("must specify value for 'update_key' and/or 'primary_key' for incremental mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			if args := os.Getenv("SLING_CLI_ARGS"); strings.Contains(args, "-src-conn") || strings.Contains(args, "-tgt-conn") {
				err = g.Error("must specify value for '--update-key' and/or '--primary-key' for incremental mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			}
			return
		}
	} else if cfg.Mode == BackfillMode {
		if cfg.Source.UpdateKey == "" || len(cfg.Source.PrimaryKey()) == 0 {
			err = g.Error("must specify value for 'update_key' and 'primary_key' for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			if args := os.Getenv("SLING_CLI_ARGS"); strings.Contains(args, "-src-conn") || strings.Contains(args, "-tgt-conn") {
				err = g.Error("must specify value for '--update-key' and '--primary-key' for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			}
			return
		}
		if cfg.Source.Options == nil || cfg.Source.Options.Range == nil {
			err = g.Error("must specify range (source.options.range or --range) for backfill mode. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			return
		} else if rangeArr := strings.Split(*cfg.Source.Options.Range, ","); len(rangeArr) != 2 {
			err = g.Error("must specify valid range value for backfill mode separated by one comma, for example `2021-01-01,2021-02-01`. See docs for more details: https://docs.slingdata.io/sling-cli/run/configuration")
			return
		}
	} else if cfg.Mode == SnapshotMode {
		cfg.MetadataLoadedAt = g.Bool(true) // needed for snapshot mode
	}

	if srcDbProvided && tgtDbProvided {
		Type = DbToDb
	} else if srcFileProvided && tgtDbProvided {
		Type = FileToDB
	} else if srcDbProvided && srcStreamProvided && !tgtDbProvided && tgtFileProvided {
		Type = DbToFile
	} else if srcFileProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		Type = FileToFile
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

		err = g.Error("invalid Task Configuration. Must specify valid source conn / file or target connection / output.\n  %s", strings.Join(output, "\n  "))
	}
	return Type, err
}

func (cfg *Config) HasWildcard() bool {
	if strings.HasSuffix(cfg.Source.Stream, ".*") {
		return true
	}

	if strings.Contains(cfg.Source.Stream, `/*.`) || strings.Contains(cfg.Source.Stream, `\*.`) {
		return true
	}

	return false
}

func (cfg *Config) AsReplication() (rc ReplicationConfig) {
	rc = ReplicationConfig{
		Source: cfg.Source.Conn,
		Target: cfg.Target.Conn,
		Defaults: ReplicationStreamConfig{
			SourceOptions: cfg.Source.Options,
			TargetOptions: cfg.Target.Options,
			Select:        cfg.Source.Select,
			Object:        cfg.Target.Object,
			Mode:          cfg.Mode,
			PrimaryKeyI:   cfg.Source.PrimaryKeyI,
			UpdateKey:     cfg.Source.UpdateKey,
		},
		Streams: map[string]*ReplicationStreamConfig{
			cfg.Source.Stream: {},
		},
	}

	return rc
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
		if cast.ToBool(os.Getenv("SLING_CLI")) {
			return g.Error("invalid target connection (blank or not found). Did you mean to use the `--stdout` flag?")
		}
		return g.Error("invalid target connection (blank or not found)")
	}

	if cfg.Options.Debug && os.Getenv("DEBUG") == "" {
		os.Setenv("DEBUG", "LOW")
	}
	if cfg.Options.StdIn && cfg.Source.Stream == "" {
		cfg.Source.Stream = "stdin"
	}

	// set from shell env variable, if value starts with $ and found
	if cfg.Env == nil {
		cfg.Env = map[string]string{}
	}

	for k, v := range cfg.Env {
		cfg.Env[k] = os.ExpandEnv(v)
	}

	// Set Target
	cfg.Target.Object = strings.TrimSpace(cfg.Target.Object)
	if cfg.Target.Data == nil || len(cfg.Target.Data) == 0 {
		cfg.Target.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Target.Conn)]; ok {
			cfg.TgtConn = *c.Connection.Copy()
		} else if connType := connection.SchemeType(cfg.Target.Conn); !connType.IsUnknown() {
			cfg.TgtConn, err = connection.NewConnectionFromURL(connType.String(), cfg.Target.Conn)
			if err != nil {
				return g.Error(err, "could not init target connection")
			}
		} else if !strings.Contains(cfg.Target.Conn, "://") && cfg.Target.Conn != "" && cfg.TgtConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Target.Conn)
		} else if cfg.TgtConn.Data == nil {
			cfg.TgtConn.Data = g.M()
		}
		cfg.Target.Data = cfg.TgtConn.Data
	}

	if connection.SchemeType(cfg.Target.Object).IsFile() {
		if connection.SchemeType(cfg.Target.Object) == dbio.TypeFileLocal {
			cfg.Target.Object = strings.ReplaceAll(cfg.Target.Object, `\`, `/`) // windows path fix
		}

		// format target name, especially variable hostname
		err = cfg.FormatTargetObjectName()
		if err != nil {
			return g.Error(err, "could not format target object name")
		}
	} else {
		if cfg.TgtConn.Type.IsFile() && cfg.Target.Object != "" {
			fc, err := cfg.TgtConn.AsFile()
			if err != nil {
				return g.Error(err, "could not init file connection")
			}
			// object is not url, but relative path, needs to be normalized
			cfg.Target.Data["url"] = filesys.NormalizeURI(fc, cfg.Target.Object)
		}
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

	if cfg.Options.StdOut && os.Getenv("CONCURRENCY") == "" {
		os.Setenv("CONCURRENCY", "1")
	}

	if cfg.Target.Options == nil {
		cfg.Target.Options = &TargetOptions{}
	}

	// Set Source
	cfg.Source.Stream = strings.TrimSpace(cfg.Source.Stream)
	if cfg.Source.Data == nil || len(cfg.Source.Data) == 0 {
		cfg.Source.Data = g.M()
		if c, ok := connsMap[strings.ToLower(cfg.Source.Conn)]; ok {
			cfg.SrcConn = *c.Connection.Copy()
		} else if connType := connection.SchemeType(cfg.Source.Conn); !connType.IsUnknown() {
			cfg.SrcConn, err = connection.NewConnectionFromURL(connType.String(), cfg.Source.Conn)
			if err != nil {
				return g.Error(err, "could not init source connection")
			}
		} else if !strings.Contains(cfg.Source.Conn, "://") && cfg.Source.Conn != "" && cfg.SrcConn.Data == nil {
			return g.Error("could not find connection %s", cfg.Source.Conn)
		} else if cfg.SrcConn.Data == nil {
			cfg.SrcConn.Data = g.M()
		}
		cfg.Source.Data = cfg.SrcConn.Data
	}

	if connection.SchemeType(cfg.Source.Stream) == dbio.TypeFileLocal {
		cfg.Source.Stream = strings.ReplaceAll(cfg.Source.Stream, `\`, `/`) // windows path fix
	}

	if connection.SchemeType(cfg.Source.Stream).IsFile() && !strings.HasSuffix(cfg.Source.Stream, ".sql") {
		cfg.Source.Data["url"] = cfg.Source.Stream
		cfg.SrcConn.Data["url"] = cfg.Source.Stream
	} else {
		if cfg.SrcConn.Type.IsFile() && cfg.Source.Stream != "" {
			// stream is not url, but relative path
			fc, err := cfg.SrcConn.AsFile()
			if err != nil {
				return g.Error(err, "could not init file connection")
			}
			// object is not url, but relative path, needs to be normalized
			cfg.Source.Data["url"] = filesys.NormalizeURI(fc, cfg.Source.Stream)
		}
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

	// add md5 of options, so that wee reconnect for various options
	// see variable `connPool`
	cfg.SrcConn.Data["_source_options_md5"] = g.MD5(g.Marshal(cfg.Source.Options))
	cfg.TgtConn.Data["_target_options_md5"] = g.MD5(g.Marshal(cfg.Target.Options))

	// validate table keys
	if tkMap := cfg.Target.Options.TableKeys; tkMap != nil {
		for _, kt := range lo.Keys(tkMap) {
			// append _key
			ktAllowed := g.Marshal(iop.KeyTypes)
			ktAllowed = strings.TrimSuffix(strings.TrimPrefix(ktAllowed, "["), "]")
			if !g.In(kt, iop.KeyTypes...) {
				g.Warn("%s is not a valid table key type. Valid table key types are: %s", kt, ktAllowed)
				delete(cfg.Target.Options.TableKeys, kt)
			}
		}
	}

	// validate conn data keys
	for key := range cfg.SrcConn.Data {
		if strings.Contains(key, ":") {
			g.Warn("source connection %s has an invalid property -> %s", cfg.Source.Conn, key)
		}
	}

	for key := range cfg.TgtConn.Data {
		if strings.Contains(key, ":") {
			g.Warn("target connection %s has an invalid property -> %s", cfg.Target.Conn, key)
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
	m, err := cfg.GetFormatMap()
	if err != nil {
		return g.Error(err, "could not get formatting variables")
	}

	// replace placeholders
	cfg.Target.Object = strings.TrimSpace(g.Rm(cfg.Target.Object, m))

	if cfg.TgtConn.Type.IsDb() {
		// normalize casing of object names
		table, err := database.ParseTableName(cfg.Target.Object, cfg.TgtConn.Type)
		if err != nil {
			return g.Error(err, "could not get parse target table name")
		}
		cfg.Target.Object = table.FullName()
	}

	if connection.SchemeType(cfg.Target.Object).IsFile() {
		cfg.Target.Data["url"] = cfg.Target.Object
		cfg.TgtConn.Data["url"] = cfg.Target.Object
	} else if cfg.TgtConn.Type.IsFile() {
		url := cast.ToString(cfg.Target.Data["url"])
		cfg.Target.Data["url"] = strings.TrimSpace(g.Rm(url, m))
	}

	return nil
}

// GetFormatMap returns a map to format a string with provided with variables
func (cfg *Config) GetFormatMap() (m map[string]any, err error) {
	replacePattern := regexp.MustCompile("[^_0-9a-zA-Z]+") // to clean name
	cleanUp := func(o string) string {
		return string(replacePattern.ReplaceAll([]byte(o), []byte("_")))
	}

	m = g.M(
		"run_timestamp", time.Now().Format("2006_01_02_150405"),
	)

	if cfg.SrcConn.Type.String() != "" {
		m["source_type"] = cfg.SrcConn.Type
	}
	if cfg.TgtConn.Type.String() != "" {
		m["target_type"] = cfg.TgtConn.Type
	}

	if cfg.Source.Conn != "" {
		m["source_name"] = strings.ToLower(cfg.Source.Conn)
	}

	if cfg.Target.Conn != "" {
		m["target_name"] = strings.ToLower(cfg.Target.Conn)
	}

	if cfg.SrcConn.Type.IsDb() {
		table, err := database.ParseTableName(cfg.Source.Stream, cfg.SrcConn.Type)
		if err != nil {
			return m, g.Error(err, "could not parse stream table name")
		}

		if table.SQL != "" && cfg.StreamName != "" {
			table, err = database.ParseTableName(cfg.StreamName, cfg.SrcConn.Type)
			if err != nil {
				return m, g.Error(err, "could not parse stream name: %s", cfg.StreamName)
			}
		}
		if table.Schema != "" {
			m["stream_schema"] = strings.ToLower(table.Schema)
		}
		if table.Name != "" {
			m["stream_table"] = strings.ToLower(table.Name)
		}

		if cfg.StreamName != "" {
			m["stream_name"] = strings.ToLower(cfg.StreamName)
		}
	}

	if cfg.TgtConn.Type.IsDb() {
		m["object_name"] = cfg.Target.Object

		table, err := database.ParseTableName(cfg.Target.Object, cfg.TgtConn.Type)
		if err != nil {
			return m, g.Error(err, "could not parse target table name")
		}

		m["object_schema"] = table.Schema
		m["object_table"] = table.Name

		if targetSchema := cast.ToString(cfg.Target.Data["schema"]); targetSchema != "" {
			m["target_schema"] = targetSchema
			if table.Schema == "" {
				m["object_schema"] = targetSchema
			}
		}

		// legacy
		m["target_table"] = m["object_table"]
	}

	if cfg.SrcConn.Type.IsFile() {
		uri := cfg.SrcConn.URL()
		m["stream_name"] = strings.ToLower(cfg.Source.Stream)

		fc, err := cfg.SrcConn.AsFile()
		if err != nil {
			return m, g.Error(err, "could not init source conn as file")
		}

		filePath, err := fc.GetPath(uri)
		if err != nil {
			return m, g.Error(err, "could not parse file path")
		}
		if filePath != "" {
			m["stream_file_path"] = cleanUp(filePath)
		}

		pathArr := strings.Split(strings.TrimPrefix(strings.TrimSuffix(filePath, "/"), "/"), "/")
		fileName := pathArr[len(pathArr)-1]
		m["stream_file_name"] = cleanUp(fileName)

		fileFolder := ""
		if len(pathArr) > 1 {
			fileFolder = pathArr[len(pathArr)-2]
			m["stream_file_folder"] = cleanUp(strings.TrimPrefix(fileFolder, "/"))
		}

		switch cfg.SrcConn.Type {
		case dbio.TypeFileS3, dbio.TypeFileGoogle:
			m["source_bucket"] = cfg.SrcConn.Data["bucket"]
		case dbio.TypeFileAzure:
			m["source_account"] = cfg.SrcConn.Data["account"]
			m["source_container"] = cfg.SrcConn.Data["container"]
		}

		if fileNameArr := strings.Split(fileName, "."); len(fileNameArr) > 1 {
			// remove extension
			m["stream_file_ext"] = fileNameArr[len(fileNameArr)-1]
			if len(fileNameArr) >= 3 {
				// in case of compression (2 extension tokens)
				for _, suff := range []string{"gz", "zst", "snappy"} {
					if m["stream_file_ext"] == suff {
						m["stream_file_ext"] = fileNameArr[len(fileNameArr)-2] + "_" + fileNameArr[len(fileNameArr)-1]
						break
					}
				}
			}

			m["stream_file_name"] = strings.TrimSuffix(
				cast.ToString(m["stream_file_name"]),
				"_"+cast.ToString(m["stream_file_ext"]),
			)
		}
	}

	if t := connection.SchemeType(cfg.Target.Object); t.IsFile() {
		m["object_name"] = strings.ToLower(cfg.Target.Object)

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
		g.Warn("Could not successfully get format values. Blank values for: %s", strings.Join(blankKeys, ", "))
	}

	// apply date variables
	for k, v := range iop.GetISO8601DateMap(time.Now()) {
		m[k] = v
	}

	// log format map
	g.Trace("object format map: %s", g.Marshal(m))

	return
}

// Config is the new config struct
type Config struct {
	Source  Source            `json:"source,omitempty" yaml:"source,omitempty"`
	Target  Target            `json:"target" yaml:"target"`
	Mode    Mode              `json:"mode,omitempty" yaml:"mode,omitempty"`
	Options ConfigOptions     `json:"options,omitempty" yaml:"options,omitempty"`
	Env     map[string]string `json:"env,omitempty" yaml:"env,omitempty"`

	StreamName      string                `json:"stream_name,omitempty" yaml:"stream_name,omitempty"`
	SrcConn         connection.Connection `json:"_src_conn,omitempty" yaml:"_src_conn,omitempty"`
	TgtConn         connection.Connection `json:"_tgt_conn,omitempty" yaml:"_tgt_conn,omitempty"`
	Prepared        bool                  `json:"_prepared,omitempty" yaml:"_prepared,omitempty"`
	IncrementalVal  string                `json:"-" yaml:"-"`
	ReplicationMode bool                  `json:"-" yaml:"-"`

	MetadataLoadedAt  *bool `json:"-" yaml:"-"`
	MetadataStreamURL bool  `json:"-" yaml:"-"`
	MetadataRowNum    bool  `json:"-" yaml:"-"`
	MetadataRowID     bool  `json:"-" yaml:"-"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (cfg *Config) Scan(value interface{}) error {
	return g.JSONScanner(cfg, value)
}

// Value return json value, implement driver.Valuer interface
func (cfg Config) Value() (driver.Value, error) {
	jBytes, err := json.Marshal(cfg)
	if err != nil {
		return nil, g.Error(err, "could not marshal")
	}

	out := string(jBytes)
	out = strings.ReplaceAll(out, `,"_src_conn":{}`, ``)
	out = strings.ReplaceAll(out, `,"_tgt_conn":{}`, ``)
	out = strings.ReplaceAll(out, `,"primary_key":null`, ``)

	return []byte(out), err
}

func (cfg *Config) MD5() string {
	payload := g.Marshal([]any{
		g.M("source", cfg.Source.MD5()),
		g.M("target", cfg.Target.MD5()),
		g.M("mode", cfg.Mode),
		g.M("options", cfg.Options),
	})

	// clean up
	if strings.Contains(cfg.Source.Conn, "://") {
		payload = cleanConnURL(payload, cfg.Source.Conn)
	}

	if strings.Contains(cfg.Target.Conn, "://") {
		payload = cleanConnURL(payload, cfg.Target.Conn)
	}

	return g.MD5(payload)
}

// ConfigOptions are configuration options
type ConfigOptions struct {
	Debug  bool `json:"debug,omitempty" yaml:"debug,omitempty"`
	StdIn  bool `json:"-"`                                        // whether stdin is passed
	StdOut bool `json:"stdout,omitempty" yaml:"stdout,omitempty"` // whether to output to stdout
}

// Source is a source of data
type Source struct {
	Conn        string                 `json:"conn,omitempty" yaml:"conn,omitempty"`
	Stream      string                 `json:"stream,omitempty" yaml:"stream,omitempty"`
	Select      []string               `json:"select,omitempty" yaml:"select,omitempty"` // Select or exclude columns. Exclude with prefix "-".
	PrimaryKeyI any                    `json:"primary_key,omitempty" yaml:"primary_key,omitempty"`
	UpdateKey   string                 `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	Options     *SourceOptions         `json:"options,omitempty" yaml:"options,omitempty"`
	Data        map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	columns iop.Columns `json:"-" yaml:"-"`
}

func (s *Source) Limit() int {
	if val := os.Getenv("SLING_LIMIT"); val != "" {
		return cast.ToInt(val)
	}

	if s.Options.Limit == nil {
		return 0
	}
	return *s.Options.Limit
}

func (s *Source) HasUpdateKey() bool {
	return s.UpdateKey != ""
}

func (s *Source) HasPrimaryKey() bool {
	return strings.Join(s.PrimaryKey(), "") != ""
}

func (s *Source) PrimaryKey() []string {
	return castKeyArray(s.PrimaryKeyI)
}

func (s *Source) MD5() string {
	payload := g.Marshal([]any{
		g.M("conn", s.Conn),
		g.M("stream", s.Stream),
		g.M("primary_key", s.PrimaryKeyI),
		g.M("update_key", s.UpdateKey),
		g.M("options", s.Options),
	})

	if strings.Contains(s.Conn, "://") {
		payload = cleanConnURL(payload, s.Conn)
	}

	return g.MD5(payload)
}

// Target is a target of data
type Target struct {
	Conn    string                 `json:"conn,omitempty" yaml:"conn,omitempty"`
	Object  string                 `json:"object,omitempty" yaml:"object,omitempty"`
	Options *TargetOptions         `json:"options,omitempty" yaml:"options,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	TmpTableCreated bool        `json:"-" yaml:"-"`
	columns         iop.Columns `json:"-" yaml:"-"`
}

func (t *Target) MD5() string {
	payload := g.Marshal([]any{
		g.M("conn", t.Conn),
		g.M("object", t.Object),
		g.M("options", t.Options),
	})

	if strings.Contains(t.Conn, "://") {
		payload = cleanConnURL(payload, t.Conn)
	}

	return g.MD5(payload)
}

// SourceOptions are connection and stream processing options
type SourceOptions struct {
	TrimSpace      *bool               `json:"trim_space,omitempty" yaml:"trim_space,omitempty"`
	EmptyAsNull    *bool               `json:"empty_as_null,omitempty" yaml:"empty_as_null,omitempty"`
	Header         *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Flatten        *bool               `json:"flatten,omitempty" yaml:"flatten,omitempty"`
	FieldsPerRec   *int                `json:"fields_per_rec,omitempty" yaml:"fields_per_rec,omitempty"`
	Compression    *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	Format         *filesys.FileType   `json:"format,omitempty" yaml:"format,omitempty"`
	NullIf         *string             `json:"null_if,omitempty" yaml:"null_if,omitempty"`
	DatetimeFormat string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	SkipBlankLines *bool               `json:"skip_blank_lines,omitempty" yaml:"skip_blank_lines,omitempty"`
	Delimiter      string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	Escape         string              `json:"escape,omitempty" yaml:"escape,omitempty"`
	MaxDecimals    *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
	JmesPath       *string             `json:"jmespath,omitempty" yaml:"jmespath,omitempty"`
	Sheet          *string             `json:"sheet,omitempty" yaml:"sheet,omitempty"`
	Range          *string             `json:"range,omitempty" yaml:"range,omitempty"`
	Limit          *int                `json:"limit,omitempty" yaml:"limit,omitempty"`
	Columns        any                 `json:"columns,omitempty" yaml:"columns,omitempty"`
	Transforms     any                 `json:"transforms,omitempty" yaml:"transforms,omitempty"`

	extraTransforms []string `json:"-" yaml:"-"`
}

// TargetOptions are target connection and stream processing options
type TargetOptions struct {
	Header           *bool               `json:"header,omitempty" yaml:"header,omitempty"`
	Compression      *iop.CompressorType `json:"compression,omitempty" yaml:"compression,omitempty"`
	Concurrency      int                 `json:"concurrency,omitempty" yaml:"concurrency,omitempty"`
	DatetimeFormat   string              `json:"datetime_format,omitempty" yaml:"datetime_format,omitempty"`
	Delimiter        string              `json:"delimiter,omitempty" yaml:"delimiter,omitempty"`
	FileMaxRows      int64               `json:"file_max_rows,omitempty" yaml:"file_max_rows,omitempty"`
	FileMaxBytes     int64               `json:"file_max_bytes,omitempty" yaml:"file_max_bytes,omitempty"`
	Format           filesys.FileType    `json:"format,omitempty" yaml:"format,omitempty"`
	MaxDecimals      *int                `json:"max_decimals,omitempty" yaml:"max_decimals,omitempty"`
	UseBulk          *bool               `json:"use_bulk,omitempty" yaml:"use_bulk,omitempty"`
	AddNewColumns    *bool               `json:"add_new_columns,omitempty" yaml:"add_new_columns,omitempty"`
	AdjustColumnType *bool               `json:"adjust_column_type,omitempty" yaml:"adjust_column_type,omitempty"`
	ColumnCasing     *ColumnCasing       `json:"column_casing,omitempty" yaml:"column_casing,omitempty"`

	TableKeys database.TableKeys `json:"table_keys,omitempty" yaml:"table_keys,omitempty"`
	TableTmp  string             `json:"table_tmp,omitempty" yaml:"table_tmp,omitempty"`
	TableDDL  string             `json:"table_ddl,omitempty" yaml:"table_ddl,omitempty"`
	PreSQL    string             `json:"pre_sql,omitempty" yaml:"pre_sql,omitempty"`
	PostSQL   string             `json:"post_sql,omitempty" yaml:"post_sql,omitempty"`
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
	// Delimiter:      ",",
	FieldsPerRec: g.Int(-1),
	MaxDecimals:  g.Int(-1),
}

var SourceDBOptionsDefault = SourceOptions{
	EmptyAsNull:    g.Bool(false),
	NullIf:         g.String("NULL"),
	DatetimeFormat: "AUTO",
	MaxDecimals:    g.Int(-1),
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
		7,
	),
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_ROWS")),
		0,
	),
	FileMaxBytes: lo.Ternary(
		os.Getenv("FILE_MAX_BYTES") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_BYTES")),
		0,
	),
	Format:         filesys.FileTypeNone,
	UseBulk:        g.Bool(true),
	AddNewColumns:  g.Bool(true),
	DatetimeFormat: "auto",
	Delimiter:      ",",
	MaxDecimals:    g.Int(-1),
	ColumnCasing:   (*ColumnCasing)(g.String(string(SourceColumnCasing))),
}

var TargetDBOptionsDefault = TargetOptions{
	FileMaxRows: lo.Ternary(
		os.Getenv("FILE_MAX_ROWS") != "",
		cast.ToInt64(os.Getenv("FILE_MAX_ROWS")),
		0,
	),
	UseBulk:          g.Bool(true),
	AddNewColumns:    g.Bool(true),
	AdjustColumnType: g.Bool(false),
	DatetimeFormat:   "auto",
	MaxDecimals:      g.Int(-1),
	ColumnCasing:     (*ColumnCasing)(g.String(string(SourceColumnCasing))),
}

func (o *SourceOptions) SetDefaults(sourceOptions SourceOptions) {

	if o == nil {
		o = &sourceOptions
	}
	if o.TrimSpace == nil {
		o.TrimSpace = sourceOptions.TrimSpace
	}
	if o.EmptyAsNull == nil {
		o.EmptyAsNull = sourceOptions.EmptyAsNull
	}
	if o.Header == nil {
		o.Header = sourceOptions.Header
	}
	if o.Compression == nil {
		o.Compression = sourceOptions.Compression
	}
	if o.NullIf == nil {
		o.NullIf = sourceOptions.NullIf
	}
	if o.FieldsPerRec == nil {
		o.FieldsPerRec = sourceOptions.FieldsPerRec
	}
	if o.JmesPath == nil {
		o.JmesPath = sourceOptions.JmesPath
	}
	if o.Sheet == nil {
		o.Sheet = sourceOptions.Sheet
	}
	if o.Range == nil {
		o.Range = sourceOptions.Range
	}
	if o.DatetimeFormat == "" {
		o.DatetimeFormat = sourceOptions.DatetimeFormat
	}
	if o.SkipBlankLines == nil {
		o.SkipBlankLines = sourceOptions.SkipBlankLines
	}
	if o.Delimiter == "" {
		o.Delimiter = sourceOptions.Delimiter
	}
	if o.Escape == "" {
		o.Escape = sourceOptions.Escape
	}
	if o.MaxDecimals == nil {
		o.MaxDecimals = sourceOptions.MaxDecimals
	}
	if o.Columns == nil {
		o.Columns = sourceOptions.Columns
	}
	if o.Transforms == nil {
		o.Transforms = sourceOptions.Transforms
	}

}

func (o *TargetOptions) SetDefaults(targetOptions TargetOptions) {

	if o == nil {
		o = &targetOptions
	}
	if o.Header == nil {
		o.Header = targetOptions.Header
	}
	if o.Compression == nil {
		o.Compression = targetOptions.Compression
	}
	if o.Format == filesys.FileTypeNone {
		o.Format = targetOptions.Format
	}
	if o.Concurrency == 0 {
		o.Concurrency = targetOptions.Concurrency
	}
	if o.FileMaxRows == 0 {
		o.FileMaxRows = targetOptions.FileMaxRows
	}
	if o.FileMaxBytes == 0 {
		o.FileMaxBytes = targetOptions.FileMaxBytes
	}
	if o.UseBulk == nil {
		o.UseBulk = targetOptions.UseBulk
	}
	if o.PreSQL == "" {
		o.PreSQL = targetOptions.PreSQL
	}
	if o.PostSQL == "" {
		o.PostSQL = targetOptions.PostSQL
	}
	if o.AdjustColumnType == nil {
		o.AdjustColumnType = targetOptions.AdjustColumnType
	}

	if o.AddNewColumns == nil {
		o.AddNewColumns = targetOptions.AddNewColumns
	}
	if o.DatetimeFormat == "" {
		o.DatetimeFormat = targetOptions.DatetimeFormat
	}
	if o.Delimiter == "" {
		o.Delimiter = targetOptions.Delimiter
	}
	if o.MaxDecimals == nil {
		o.MaxDecimals = targetOptions.MaxDecimals
	}
	if o.ColumnCasing == nil {
		o.ColumnCasing = targetOptions.ColumnCasing
	}
	if o.TableKeys == nil {
		o.TableKeys = targetOptions.TableKeys
	}
}

func castKeyArray(keyI any) (key []string) {
	switch keyV := keyI.(type) {
	case []string:
		return keyV
	case string:
		return []string{keyV}
	case *string:
		return []string{*keyV}
	case []any:
		for _, v := range keyV {
			key = append(key, cast.ToString(v))
		}
		return key
	}
	return
}

// expandEnvVars replaces $KEY or ${KEY} with its environment variable value
// only if the variable is present in the environment.
// If not present, $KEY or ${KEY} will remain in the config text.
func expandEnvVars(text string) string {
	for key, value := range g.KVArrToMap(os.Environ()...) {
		text = strings.ReplaceAll(text, "$"+key+"", value)
		text = strings.ReplaceAll(text, "${"+key+"}", value)
	}
	return text
}

func cleanConnURL(payload, connURL string) string {
	cleanSource := strings.Split(connURL, "://")[0] + "://"
	payload = strings.ReplaceAll(payload, g.Marshal(connURL), g.Marshal(cleanSource))
	return payload
}
