package sling

import (
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// RuntimeState is for runtime state
type RuntimeState struct {
	Hooks    map[string]map[string]any `json:"hooks,omitempty"`
	DateTime DateTimeState             `json:"datetime,omitempty"`
	Source   ConnState                 `json:"source,omitempty"`
	Target   ConnState                 `json:"target,omitempty"`
	Stream   *StreamState              `json:"stream,omitempty"`
	Object   *ObjectState              `json:"object,omitempty"`
	Runs     map[string]*RunState      `json:"runs,omitempty"`
	Run      *RunState                 `json:"run,omitempty"`
}

type DateTimeState struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	FileName  string    `json:"file_name,omitempty"`
	Rfc3339   string    `json:"rfc3339,omitempty"`
	Date      string    `json:"date,omitempty"`
	Datetime  string    `json:"datetime,omitempty"`
	YYYY      string    `json:"YYYY,omitempty"`
	YY        string    `json:"YY,omitempty"`
	MMM       string    `json:"MMM,omitempty"`
	MM        string    `json:"MM,omitempty"`
	DD        string    `json:"DD,omitempty"`
	HH        string    `json:"HH,omitempty"`
}

type RunState struct {
	Stream     *StreamState   `json:"stream,omitempty"`
	Object     *ObjectState   `json:"object,omitempty"`
	TotalBytes uint64         `json:"total_bytes,omitempty"`
	RowCount   uint64         `json:"row_count,omitempty"`
	Status     ExecStatus     `json:"status,omitempty"`
	StartTime  *time.Time     `json:"start_time,omitempty"`
	EndTime    *time.Time     `json:"end_time,omitempty"`
	Error      *string        `json:"error,omitempty"`
	Task       *TaskExecution `json:"-"`
}

type ConnState struct {
	Name      string    `json:"name,omitempty"`
	Type      dbio.Type `json:"type,omitempty"`
	Kind      dbio.Kind `json:"kind,omitempty"`
	Account   string    `json:"account,omitempty"`
	Bucket    string    `json:"bucket,omitempty"`
	Container string    `json:"container,omitempty"`
}

type StreamState struct {
	FileFolder string `json:"file_folder,omitempty"`
	FileName   string `json:"file_name,omitempty"`
	FileExt    string `json:"file_ext,omitempty"`
	FilePath   string `json:"file_path,omitempty"`
	Name       string `json:"name,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
}

type ObjectState struct {
	Schema string `json:"schema,omitempty"`
	Table  string `json:"table,omitempty"`
	Name   string `json:"name,omitempty"`
}

func StateSet(t *TaskExecution) {
	StoreSet(t)

	if t.Replication != nil && t.Config != nil && t.Context != nil {
		t.Context.Lock()
		defer t.Context.Unlock()

		key := iop.CleanName(t.Replication.Normalize(t.Config.StreamName))
		run := t.Replication.state.Runs[key]

		if run == nil {
			fMap, _ := t.Config.GetFormatMap()
			run = &RunState{
				Status: ExecStatusCreated,
				Stream: &StreamState{
					FileFolder: cast.ToString(fMap["stream_file_folder"]),
					FileName:   cast.ToString(fMap["stream_file_name"]),
					FileExt:    cast.ToString(fMap["stream_file_ext"]),
					FilePath:   cast.ToString(fMap["stream_file_path"]),
					Name:       cast.ToString(fMap["stream_name"]),
					Schema:     cast.ToString(fMap["stream_schema"]),
					Table:      cast.ToString(fMap["stream_table"]),
				},
				Object: &ObjectState{
					Name:   cast.ToString(fMap["object_name"]),
					Schema: cast.ToString(fMap["object_schema"]),
					Table:  cast.ToString(fMap["object_table"]),
				}}
			t.Replication.state.Runs[key] = run
		}

		t.Replication.state.Stream = run.Stream
		t.Replication.state.Object = run.Object

		bytes, _ := t.GetBytes()
		run.TotalBytes = bytes
		run.RowCount = t.GetCount()
		run.Status = t.Status
		run.StartTime = t.StartTime
		run.EndTime = t.EndTime
		run.Task = t

		if t.Err != nil {
			run.Error = g.Ptr(t.Err.Error())
		}

		// set as active run
		t.Replication.state.Run = run
	}
}
