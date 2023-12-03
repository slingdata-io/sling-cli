package sling

import (
	"database/sql/driver"
	"io"
	"os"
	"strings"

	"github.com/flarco/dbio/connection"
	"github.com/flarco/dbio/database"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/samber/lo"
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
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (rd *ReplicationConfig) Scan(value interface{}) error {
	return g.JSONScanner(rd, value)
}

// Value return json value, implement driver.Valuer interface
func (rd ReplicationConfig) Value() (driver.Value, error) {
	return g.JSONValuer(rd, "{}")
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
			return g.Error("Must specify schema when using wildcard: 'my_schema.*', not '*'")
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
	if !ok || !c.Connection.Type.IsDb() {
		// wildcards only apply to database source connections
		return
	}

	g.Debug("processing wildcards for %s", rd.Source)

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

		if schemaT.Name == "*" {
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
				newCfg := ReplicationStreamConfig{}
				g.Unmarshal(g.Marshal(rd.Streams[wildcardName]), &newCfg) // copy config over
				rd.Streams[table.FullName()] = &newCfg
				rd.streamsOrdered = append(rd.streamsOrdered, table.FullName())
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
	return castPrimaryKey(s.PrimaryKeyI)
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
					config.Defaults.SourceOptions.Columns = getSourceOptionsColumns(defaultsNode.Value.(yaml.MapSlice))
				}
			}
		}
	}

	for _, rootNode := range rootMap {
		if cast.ToString(rootNode.Key) == "streams" {
			streamsNodes := rootNode.Value.(yaml.MapSlice)
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
							stream.SourceOptions.Columns = getSourceOptionsColumns(streamConfigNode.Value.(yaml.MapSlice))
						}
					}
				}
			}
		}
	}

	return
}

func getSourceOptionsColumns(sourceOptionsNodes yaml.MapSlice) (columns iop.Columns) {
	for _, sourceOptionsNode := range sourceOptionsNodes {
		if cast.ToString(sourceOptionsNode.Key) == "columns" {
			for _, columnNode := range sourceOptionsNode.Value.(yaml.MapSlice) {
				col := iop.Column{
					Name: cast.ToString(columnNode.Key),
					Type: iop.ColumnType(cast.ToString(columnNode.Value)),
				}
				columns = append(columns, col)
			}
			columns = iop.NewColumns(columns...)
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
	}

	return
}
