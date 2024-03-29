package sling

import (
	"context"
	"database/sql/driver"
	"io"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
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

// MD5 returns a md5 hash of the config
func (rd *ReplicationConfig) MD5() string {
	payload := g.Marshal([]any{
		g.M("source", rd.Source),
		g.M("target", rd.Target),
		g.M("defaults", rd.Defaults),
		g.M("streams", rd.Streams),
	})

	// clean up
	if strings.Contains(rd.Source, "://") {
		cleanSource := strings.Split(rd.Source, "://")[0] + "://"
		payload = strings.ReplaceAll(payload, g.Marshal(rd.Source), g.Marshal(cleanSource))
	}

	if strings.Contains(rd.Target, "://") {
		cleanTarget := strings.Split(rd.Target, "://")[0] + "://"
		payload = strings.ReplaceAll(payload, g.Marshal(rd.Target), g.Marshal(cleanTarget))
	}

	return g.MD5(payload)
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

// GetStream returns the stream if the it exists
func (rd ReplicationConfig) GetStream(name string) (streamName string, cfg *ReplicationStreamConfig, found bool) {

	for streamName, streamCfg := range rd.Streams {
		if rd.Normalize(streamName) == rd.Normalize(name) {
			return streamName, streamCfg, true
		}
	}
	return
}

// GetStream returns the stream if the it exists
func (rd ReplicationConfig) MatchStreams(pattern string) (streams map[string]*ReplicationStreamConfig) {
	streams = map[string]*ReplicationStreamConfig{}
	gc, err := glob.Compile(strings.ToLower(pattern))
	for streamName, streamCfg := range rd.Streams {
		if rd.Normalize(streamName) == rd.Normalize(pattern) {
			streams[streamName] = streamCfg
		} else if err == nil && gc.Match(strings.ToLower(rd.Normalize(streamName))) {
			streams[streamName] = streamCfg
		}
	}
	return
}

// Normalize normalized the name
func (rd ReplicationConfig) Normalize(n string) string {
	n = strings.ReplaceAll(n, "`", "")
	n = strings.ReplaceAll(n, `"`, "")
	n = strings.ToLower(n)
	return n
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

func (rd *ReplicationConfig) AddStream(key string, cfg *ReplicationStreamConfig) {
	newCfg := ReplicationStreamConfig{}
	g.Unmarshal(g.Marshal(cfg), &newCfg) // copy config over
	rd.Streams[key] = &newCfg
	rd.streamsOrdered = append(rd.streamsOrdered, key)
}

func (rd *ReplicationConfig) DeleteStream(key string) {
	delete(rd.Streams, key)
	rd.streamsOrdered = lo.Filter(rd.streamsOrdered, func(v string, i int) bool {
		return v != key
	})
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

			gc, err := glob.Compile(strings.ToLower(schemaT.Name))
			if err != nil {
				return g.Error(err, "could not parse pattern: %s", schemaT.Name)
			}

			for _, row := range data.Rows {
				table := database.Table{
					Schema:  schemaT.Schema,
					Name:    cast.ToString(row[0]),
					Dialect: conn.GetType(),
				}

				// add to stream map
				if gc.Match(strings.ToLower(table.Name)) {

					streamName, streamConfig, found := rd.GetStream(table.FullName())
					if found {
						// keep in step with order, delete and add again
						rd.DeleteStream(streamName)
						rd.AddStream(table.FullName(), streamConfig)
						continue
					}

					cfg := rd.Streams[wildcardName]
					rd.AddStream(table.FullName(), cfg)
				}
			}

			// delete * from stream map
			rd.DeleteStream(wildcardName)

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
		nodes, err := fs.ListRecursive(wildcardName)
		if err != nil {
			return g.Error(err, "could not list %s", wildcardName)
		}

		added := 0
		for _, node := range nodes {
			streamName, streamConfig, found := rd.GetStream(node.URI)
			if found {
				// keep in step with order, delete and add again
				rd.DeleteStream(streamName)
				rd.AddStream(node.URI, streamConfig)
				continue
			}

			newCfg := ReplicationStreamConfig{}
			g.Unmarshal(g.Marshal(rd.Streams[wildcardName]), &newCfg) // copy config over
			rd.Streams[node.URI] = &newCfg
			rd.streamsOrdered = append(rd.streamsOrdered, node.URI)
			added++
		}

		// delete from stream map
		rd.DeleteStream(wildcardName)

		if added == 0 {
			g.Debug("0 streams added for %#v (nodes=%d)", wildcardName, len(nodes))
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
	Schedule      []string       `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	SourceOptions *SourceOptions `json:"source_options,omitempty" yaml:"source_options,omitempty"`
	TargetOptions *TargetOptions `json:"target_options,omitempty" yaml:"target_options,omitempty"`
	Disabled      bool           `json:"disabled,omitempty" yaml:"disabled,omitempty"`

	State *StreamIncrementalState `json:"state,omitempty" yaml:"state,omitempty"`
}

type StreamIncrementalState struct {
	Value int64            `json:"value,omitempty" yaml:"value,omitempty"`
	Files map[string]int64 `json:"files,omitempty" yaml:"files,omitempty"`
}

func (s *ReplicationStreamConfig) PrimaryKey() []string {
	return castKeyArray(s.PrimaryKeyI)
}

func SetStreamDefaults(stream *ReplicationStreamConfig, replicationCfg ReplicationConfig) {

	if len(stream.Schedule) == 0 {
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
	if stream.SQL == "" {
		stream.SQL = replicationCfg.Defaults.SQL
	}
	if len(stream.Select) == 0 {
		stream.Select = replicationCfg.Defaults.Select
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

	// parse env & expand variables
	var Env map[string]any
	g.Unmarshal(g.Marshal(m["env"]), &Env)
	for k, v := range Env {
		Env[k] = os.ExpandEnv(cast.ToString(v))
	}

	// replace variables across the yaml file
	Env = lo.Ternary(Env == nil, map[string]any{}, Env)
	replicYAML = g.Rm(replicYAML, Env)

	// parse again
	m = g.M()
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
		Env:    Env,
	}

	// parse defaults
	err = g.Unmarshal(g.Marshal(defaults), &config.Defaults)
	if err != nil {
		err = g.Error(err, "could not parse 'defaults'")
		return
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
				stream, found := config.Streams[key]

				config.streamsOrdered = append(config.streamsOrdered, key)
				if streamsNode.Value == nil {
					continue
				}
				for _, streamConfigNode := range streamsNode.Value.(yaml.MapSlice) {
					if cast.ToString(streamConfigNode.Key) == "source_options" {
						if found {
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

	// set originalCfg
	config.originalCfg = g.Marshal(config)

	return
}

func getSourceOptionsColumns(sourceOptionsNodes yaml.MapSlice) (columns map[string]any) {
	columns = map[string]any{}
	for _, sourceOptionsNode := range sourceOptionsNodes {
		if cast.ToString(sourceOptionsNode.Key) == "columns" {
			if slice, ok := sourceOptionsNode.Value.(yaml.MapSlice); ok {
				for _, columnNode := range slice {
					columns[cast.ToString(columnNode.Key)] = cast.ToString(columnNode.Value)
				}
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

	config.Env["SLING_CONFIG_PATH"] = cfgPath

	return
}
