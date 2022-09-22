package sling

import (
	"database/sql/driver"

	"github.com/flarco/g"
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
	ObjectName    string         `json:"object_name,omitempty" yaml:"object_name,omitempty"`
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
	if stream.ObjectName == "" {
		stream.ObjectName = replicationCfg.Defaults.ObjectName
	}
	if stream.SourceOptions == nil {
		stream.SourceOptions = replicationCfg.Defaults.SourceOptions
	}
	if stream.TargetOptions == nil {
		stream.TargetOptions = replicationCfg.Defaults.TargetOptions
	}
}
