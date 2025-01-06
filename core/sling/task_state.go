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
	Hooks     map[string]map[string]any `json:"hooks,omitempty"`
	Timestamp DateTimeState             `json:"timestamp,omitempty"`
	Source    ConnState                 `json:"source,omitempty"`
	Target    ConnState                 `json:"target,omitempty"`
	Stream    *StreamState              `json:"stream,omitempty"`
	Object    *ObjectState              `json:"object,omitempty"`
	Runs      map[string]*RunState      `json:"runs,omitempty"`
	Run       *RunState                 `json:"run,omitempty"`
}

type DateTimeState struct {
	Timestamp time.Time `json:"timestamp,omitempty"`
	Unix      int64     `json:"unix,omitempty"`
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

func (dts *DateTimeState) Update() {
	now := time.Now()
	dts.Timestamp = now
	dts.Unix = now.Unix()
	dts.FileName = now.Format("2006_01_02_150405")
	dts.Rfc3339 = now.Format(time.RFC3339)
	dts.Date = now.Format(time.DateOnly)
	dts.Datetime = now.Format(time.DateTime)
	dts.YYYY = now.Format("2006")
	dts.YY = now.Format("06")
	dts.MMM = now.Format("Jan")
	dts.MM = now.Format("01")
	dts.DD = now.Format("02")
	dts.HH = now.Format("15")
}

type RunState struct {
	Stream     *StreamState            `json:"stream,omitempty"`
	Object     *ObjectState            `json:"object,omitempty"`
	TotalBytes uint64                  `json:"total_bytes,omitempty"`
	RowCount   uint64                  `json:"row_count,omitempty"`
	Status     ExecStatus              `json:"status,omitempty"`
	StartTime  *time.Time              `json:"start_time,omitempty"`
	EndTime    *time.Time              `json:"end_time,omitempty"`
	Error      *string                 `json:"error,omitempty"`
	Config     ReplicationStreamConfig `json:"config,omitempty"`
	Task       *TaskExecution          `json:"-"`
}

type ConnState struct {
	Name      string    `json:"name,omitempty"`
	Type      dbio.Type `json:"type,omitempty"`
	Kind      dbio.Kind `json:"kind,omitempty"`
	Account   string    `json:"account,omitempty"`
	Bucket    string    `json:"bucket,omitempty"`
	Container string    `json:"container,omitempty"`
	Database  string    `json:"database,omitempty"`
	Instance  string    `json:"instance,omitempty"`
	Schema    string    `json:"schema,omitempty"`
	User      string    `json:"user,omitempty"`
	Host      string    `json:"host,omitempty"`
}

type StreamState struct {
	FileFolder string `json:"file_folder,omitempty"`
	FileName   string `json:"file_name,omitempty"`
	FileExt    string `json:"file_ext,omitempty"`
	FilePath   string `json:"file_path,omitempty"`
	Name       string `json:"name,omitempty"`
	Schema     string `json:"schema,omitempty"`
	Table      string `json:"table,omitempty"`
	FullName   string `json:"full_name,omitempty"`
}

type ObjectState struct {
	Schema   string `json:"schema,omitempty"`
	Table    string `json:"table,omitempty"`
	Name     string `json:"name,omitempty"`
	FullName string `json:"full_name,omitempty"`
}

func StateSet(t *TaskExecution) {
	StoreSet(t)

	if t.Replication != nil && t.Config != nil && t.Context != nil {
		t.Context.Lock()
		defer t.Context.Unlock()

		state, err := t.Replication.RuntimeState()
		if err != nil {
			return
		}

		key := iop.CleanName(t.Replication.Normalize(t.Config.StreamName))
		run := state.Runs[key]
		if run == nil {
			run = &RunState{
				Status: ExecStatusCreated,
				Stream: &StreamState{},
				Object: &ObjectState{},
			}
		}

		fMap, _ := t.Config.GetFormatMap()

		run.Stream.FileFolder = cast.ToString(fMap["stream_file_folder"])
		run.Stream.FileName = cast.ToString(fMap["stream_file_name"])
		run.Stream.FileExt = cast.ToString(fMap["stream_file_ext"])
		run.Stream.FilePath = cast.ToString(fMap["stream_file_path"])
		run.Stream.Name = cast.ToString(fMap["stream_name"])
		run.Stream.Schema = cast.ToString(fMap["stream_schema"])
		run.Stream.Table = cast.ToString(fMap["stream_table"])

		run.Object.Name = cast.ToString(fMap["object_name"])
		run.Object.Schema = cast.ToString(fMap["object_schema"])
		run.Object.Table = cast.ToString(fMap["object_table"])

		state.Runs[key] = run

		state.Stream = run.Stream
		state.Object = run.Object

		bytes, _ := t.GetBytes()
		run.TotalBytes = bytes
		run.RowCount = t.GetCount()
		run.Status = t.Status
		run.StartTime = t.StartTime
		run.EndTime = t.EndTime
		run.Config = g.PtrVal(t.Config.ReplicationStream)
		run.Config.Hooks = HookMap{} // no nested values
		run.Task = t

		if t.Err != nil {
			run.Error = g.Ptr(t.Err.Error())
		}

		// set as active run
		state.Run = run
	}
}
