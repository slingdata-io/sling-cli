package sling

import (
	"database/sql/driver"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
)

type ReplicationConfig struct {
	Source   string                              `json:"source,omitempty" yaml:"source,omitempty"`
	Target   string                              `json:"target,omitempty" yaml:"target,omitempty"`
	Defaults ReplicationStreamConfig             `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Streams  map[string]*ReplicationStreamConfig `json:"streams,omitempty" yaml:"streams,omitempty"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (rd *ReplicationConfig) Scan(value interface{}) error {
	return g.JSONScanner(rd, value)
}

// Value return json value, implement driver.Valuer interface
func (rd ReplicationConfig) Value() (driver.Value, error) {
	return g.JSONValuer(rd, "{}")
}

type ReplicationStreamConfig struct {
	Mode          Mode           `json:"mode,omitempty" yaml:"mode,omitempty"`
	Object        string         `json:"object,omitempty" yaml:"object,omitempty"`
	Columns       []string       `json:"columns,omitempty" yaml:"columns,flow,omitempty"`
	PrimaryKey    []string       `json:"primary_key,omitempty" yaml:"primary_key,flow,omitempty"`
	UpdateKey     string         `json:"update_key,omitempty" yaml:"update_key,omitempty"`
	SQL           string         `json:"sql,omitempty" yaml:"sql,omitempty"`
	Schedule      *string        `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	SourceOptions *SourceOptions `json:"source_options,omitempty" yaml:"source_options,omitempty"`
	TargetOptions *TargetOptions `json:"target_options,omitempty" yaml:"target_options,omitempty"`
	Disabled      bool           `json:"disabled,omitempty" yaml:"disabled,omitempty"`
}

func SetStreamDefaults(stream *ReplicationStreamConfig, replicationCfg ReplicationConfig) {

	if stream.Schedule == nil {
		stream.Schedule = replicationCfg.Defaults.Schedule
	}
	if len(stream.PrimaryKey) == 0 {
		stream.PrimaryKey = replicationCfg.Defaults.PrimaryKey
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
	}
	if stream.TargetOptions == nil {
		stream.TargetOptions = replicationCfg.Defaults.TargetOptions
	}

	// FIXME: temp fix for full-refresh+incremental
	if stream.Mode == "full-refresh+incremental" {
		stream.Mode = FullRefreshMode
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
		err = g.Error("did not find 'defaults' key")
		return
	}

	streams, ok := m["streams"]
	if !ok {
		err = g.Error("did not find 'streams' key")
		return
	}

	config = ReplicationConfig{
		Source: cast.ToString(source),
		Target: cast.ToString(target),
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

	return
}
