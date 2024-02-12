package sling

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/dbio/database"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type ReplicationConfig struct {
	Source   string                              `json:"source,omitempty" yaml:"source,omitempty"`
	Target   string                              `json:"target,omitempty" yaml:"target,omitempty"`
	Defaults ReplicationStreamConfig             `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Streams  map[string]*ReplicationStreamConfig `json:"streams,omitempty" yaml:"streams,omitempty"`
	Env      map[string]any                      `json:"env,omitempty" yaml:"env,omitempty"`

	streamsOrdered []string
	originalCfg    string
}

// OriginalCfg returns original config
func (rd *ReplicationConfig) OriginalCfg() string {
	return rd.originalCfg
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (rd *ReplicationConfig) Scan(value interface{}) error {
	return g.JSONScanner(rd, value)
}

// Value return json value, implement driver.Valuer interface
func (rd ReplicationConfig) Value() (driver.Value, error) {
	if rd.OriginalCfg() != "" {
		return []byte(rd.OriginalCfg()), nil
	}

	jBytes, err := json.Marshal(rd)
	if err != nil {
		return nil, g.Error(err, "could not marshal")
	}

	return jBytes, err
}

// StreamsOrdered returns the stream names as ordered in the YAML file
func (rd ReplicationConfig) StreamsOrdered() []string {
	return rd.streamsOrdered
}

// HasStream returns true if the stream name exists
func (rd ReplicationConfig) HasStream(name string) bool {

	normalize := func(n string) string {
		n = strings.ReplaceAll(n, "`", "")
		n = strings.ReplaceAll(n, `"`, "")
		n = strings.ToLower(n)
		return n
	}

	for streamName := range rd.Streams {
		if normalize(streamName) == normalize(name) {
			return true
		}
	}
	return false
}

// ProcessWildcards process the streams using wildcards
// such as `my_schema.*` or `my_schema.my_prefix_*` or `my_schema.*_my_suffix`
func (rd *ReplicationConfig) ProcessWildcards() (err error) {
	wildcardNames := []string{}
	for name := range rd.Streams {
		if name == "*" {
			return g.Error("Must specify schema or path when using wildcard: 'my_schema.*', 'file://./my_folder/*', not '*'")
		} else if strings.Contains(name, "*") {
			wildcardNames = append(wildcardNames, name)
		}
	}
	if len(wildcardNames) == 0 {
		return
	}

	// get local connections
	connsMap := lo.KeyBy(connection.GetLocalConns(), func(c connection.ConnEntry) string {
		return strings.ToLower(c.Connection.Name)
	})
	c, ok := connsMap[strings.ToLower(rd.Source)]
	if !ok {
		if strings.EqualFold(rd.Source, "local://") || strings.EqualFold(rd.Source, "file://") {
			c = connection.LocalFileConnEntry()
		} else {
			return
		}
	}

	if c.Connection.Type.IsDb() {
		return rd.ProcessWildcardsDatabase(c, wildcardNames)
	}

	if c.Connection.Type.IsFile() {
		return rd.ProcessWildcardsFile(c, wildcardNames)
	}

	return g.Error("invalid connection for wildcards: %s", rd.Source)
}

func (rd *ReplicationConfig) ProcessWildcardsDatabase(c connection.ConnEntry, wildcardNames []string) (err error) {

	g.DebugLow("processing wildcards for %s", rd.Source)

	conn, err := c.Connection.AsDatabase()
	if err != nil {
		return g.Error(err, "could not init connection for wildcard processing: %s", rd.Source)
	} else if err = conn.Connect(); err != nil {
		return g.Error(err, "could not connect to database for wildcard processing: %s", rd.Source)
	}

	for _, wildcardName := range wildcardNames {
		schemaT, err := database.ParseTableName(wildcardName, c.Connection.Type)
		if err != nil {
			return g.Error(err, "could not parse stream name: %s", wildcardName)
		} else if schemaT.Schema == "" {
			continue
		}

		if strings.Contains(schemaT.Name, "*") {
			// get all tables in schema
			g.Debug("getting tables for %s", wildcardName)
			data, err := conn.GetTables(schemaT.Schema)
			if err != nil {
				return g.Error(err, "could not get tables for schema: %s", schemaT.Schema)
			}

			for _, row := range data.Rows {
				table := database.Table{
					Schema:  schemaT.Schema,
					Name:    cast.ToString(row[0]),
					Dialect: conn.GetType(),
				}

				if rd.HasStream(table.FullName()) {
					continue
				}

				// add to stream map
				if g.WildCardMatch(
					strings.ToLower(table.FullName()),
					[]string{strings.ToLower(schemaT.FullName())},
				) {
					newCfg := ReplicationStreamConfig{}
					g.Unmarshal(g.Marshal(rd.Streams[wildcardName]), &newCfg) // copy config over
					rd.Streams[table.FullName()] = &newCfg
					rd.streamsOrdered = append(rd.streamsOrdered, table.FullName())
				}
			}

			// delete * from stream map
			delete(rd.Streams, wildcardName)
			rd.streamsOrdered = lo.Filter(rd.streamsOrdered, func(v string, i int) bool {
				return v != wildcardName
			})

		}
	}
	return
}

func (rd *ReplicationConfig) ProcessWildcardsFile(c connection.ConnEntry, wildcardNames []string) (err error) {
	g.DebugLow("processing wildcards for %s", rd.Source)

	fs, err := c.Connection.AsFile()
	if err != nil {
		return g.Error(err, "could not init connection for wildcard processing: %s", rd.Source)
	} else if err = fs.Init(context.Background()); err != nil {
		return g.Error(err, "could not connect to file system for wildcard processing: %s", rd.Source)
	}

	for _, wildcardName := range wildcardNames {
		nameParts := strings.Split(wildcardName, "/")
		lastPart := nameParts[len(nameParts)-1]

		if strings.Contains(lastPart, "*") {
			parent := strings.TrimSuffix(wildcardName, lastPart)

			paths, err := fs.ListRecursive(parent)
			if err != nil {
				return g.Error(err, "could not list %s", parent)
			}

			for _, path := range paths {
				if g.WildCardMatch(path, []string{lastPart}) && !rd.HasStream(path) {
					newCfg := ReplicationStreamConfig{}
					g.Unmarshal(g.Marshal(rd.Streams[wildcardName]), &newCfg) // copy config over
					rd.Streams[path] = &newCfg
					rd.streamsOrdered = append(rd.streamsOrdered, path)
				}
			}

			// delete from stream map
			delete(rd.Streams, wildcardName)
			rd.streamsOrdered = lo.Filter(rd.streamsOrdered, func(v string, i int) bool {
				return v != wildcardName && v != parent
			})
		}
	}

	return
}

type ReplicationStreamConfig struct {
	Mode          Mode           `json:"mode,omitempty" yaml:"mode,omitempty"`
	Object        string         `json:"object,omitempty" yaml:"object,omitempty"`
	Select        []string       `json:"select,omitempty" yaml:"select,flow,omitempty"`
	PrimaryKeyI   any            `json:"primary_key,omitempty" yaml:"primary_key,flow,omitempty"`
	UpdateKey     string         `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	SQL           string         `json:"sql,omitempty" yaml:"sql,omitempty"`
	Schedule      *string        `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	SourceOptions *SourceOptions `json:"source_options,omitempty" yaml:"source_options,omitempty"`
	TargetOptions *TargetOptions `json:"target_options,omitempty" yaml:"target_options,omitempty"`
	Disabled      bool           `json:"disabled,omitempty" yaml:"disabled,omitempty"`
}

func (s *ReplicationStreamConfig) PrimaryKey() []string {
	return castKeyArray(s.PrimaryKeyI)
}

func SetStreamDefaults(stream *ReplicationStreamConfig, replicationCfg ReplicationConfig) {

	if stream.Schedule == nil {
		stream.Schedule = replicationCfg.Defaults.Schedule
	}
	if stream.PrimaryKeyI == nil {
		stream.PrimaryKeyI = replicationCfg.Defaults.PrimaryKeyI
	}
	if string(stream.Mode) == "" {
		stream.Mode = replicationCfg.Defaults.Mode
	}
	if stream.UpdateKey == "" {
		stream.UpdateKey = replicationCfg.Defaults.UpdateKey
	}
	if stream.Object == "" {
		stream.Object = replicationCfg.Defaults.Object
	}
	if stream.SourceOptions == nil {
		stream.SourceOptions = replicationCfg.Defaults.SourceOptions
	} else if replicationCfg.Defaults.SourceOptions != nil {
		stream.SourceOptions.SetDefaults(*replicationCfg.Defaults.SourceOptions)
	}

	if stream.TargetOptions == nil {
		stream.TargetOptions = replicationCfg.Defaults.TargetOptions
	} else if replicationCfg.Defaults.TargetOptions != nil {
		stream.TargetOptions.SetDefaults(*replicationCfg.Defaults.TargetOptions)
	}
}

// UnmarshalReplication converts a yaml file to a replication
func UnmarshalReplication(replicYAML string) (config ReplicationConfig, err error) {

	m := g.M()
	err = yaml.Unmarshal([]byte(replicYAML), &m)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	// source and target
	source, ok := m["source"]
	if !ok {
		err = g.Error("did not find 'source' key")
		return
	}

	target, ok := m["target"]
	if !ok {
		err = g.Error("did not find 'target' key")
		return
	}

	defaults, ok := m["defaults"]
	if !ok {
		defaults = g.M() // defaults not mandatory
	}

	streams, ok := m["streams"]
	if !ok {
		err = g.Error("did not find 'streams' key")
		return
	}

	config = ReplicationConfig{
		Source: cast.ToString(source),
		Target: cast.ToString(target),
		Env:    map[string]any{},
	}

	// parse defaults
	err = g.Unmarshal(g.Marshal(defaults), &config.Defaults)
	if err != nil {
		err = g.Error(err, "could not parse 'defaults'")
		return
	}

	// parse env
	if env, ok := m["env"]; ok {
		err = g.Unmarshal(g.Marshal(env), &config.Env)
		if err != nil {
			err = g.Error(err, "could not parse 'env'")
			return
		}
	} else {
		config.Env = map[string]any{}
	}

	// parse streams
	err = g.Unmarshal(g.Marshal(streams), &config.Streams)
	if err != nil {
		err = g.Error(err, "could not parse 'streams'")
		return
	}

	// get streams & columns order
	rootMap := yaml.MapSlice{}
	err = yaml.Unmarshal([]byte(replicYAML), &rootMap)
	if err != nil {
		err = g.Error(err, "Error parsing yaml content")
		return
	}

	for _, rootNode := range rootMap {
		if cast.ToString(rootNode.Key) == "defaults" {
			for _, defaultsNode := range rootNode.Value.(yaml.MapSlice) {
				if cast.ToString(defaultsNode.Key) == "source_options" {
					value, ok := defaultsNode.Value.(yaml.MapSlice)
					if ok {
						config.Defaults.SourceOptions.Columns = getSourceOptionsColumns(value)
					}
				}
			}
		}
	}

	for _, rootNode := range rootMap {
		if cast.ToString(rootNode.Key) == "streams" {
			streamsNodes, ok := rootNode.Value.(yaml.MapSlice)
			if !ok {
				continue
			}

			for _, streamsNode := range streamsNodes {
				key := cast.ToString(streamsNode.Key)
				config.streamsOrdered = append(config.streamsOrdered, key)
				if streamsNode.Value == nil {
					continue
				}
				for _, streamConfigNode := range streamsNode.Value.(yaml.MapSlice) {
					if cast.ToString(streamConfigNode.Key) == "source_options" {
						if stream, ok := config.Streams[key]; ok {
							if stream.SourceOptions == nil {
								g.Unmarshal(g.Marshal(config.Defaults.SourceOptions), stream.SourceOptions)
							}
							value, ok := streamConfigNode.Value.(yaml.MapSlice)
							if ok {
								stream.SourceOptions.Columns = getSourceOptionsColumns(value)
							}
						}
					}
				}
			}
		}
	}

	return
}

func getSourceOptionsColumns(sourceOptionsNodes yaml.MapSlice) (columns map[string]any) {
	columns = map[string]any{}
	for _, sourceOptionsNode := range sourceOptionsNodes {
		if cast.ToString(sourceOptionsNode.Key) == "columns" {
			for _, columnNode := range sourceOptionsNode.Value.(yaml.MapSlice) {
				columns[cast.ToString(columnNode.Key)] = cast.ToString(columnNode.Value)
			}
		}
	}

	return columns
}

func LoadReplicationConfig(cfgPath string) (config ReplicationConfig, err error) {
	cfgFile, err := os.Open(cfgPath)
	if err != nil {
		err = g.Error(err, "Unable to open replication path: "+cfgPath)
		return
	}

	cfgBytes, err := io.ReadAll(cfgFile)
	if err != nil {
		err = g.Error(err, "could not read from replication path: "+cfgPath)
		return
	}

	config, err = UnmarshalReplication(string(cfgBytes))
	if err != nil {
		err = g.Error(err, "Error parsing replication config")
		return
	}

	config.originalCfg = g.Marshal(config)
	config.Env["SLING_CONFIG_PATH"] = cfgPath

	return
}
