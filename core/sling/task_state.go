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
	Env       map[string]any            `json:"env,omitempty"`
	Timestamp DateTimeState             `json:"timestamp,omitempty"`
	Execution ExecutionState            `json:"execution,omitempty"`
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
	DDD       string    `json:"DDD,omitempty"`
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
	dts.DDD = now.Format("Mon")
}

type ExecutionState struct {
	ID         string     `json:"id,omitempty"`
	FilePath   string     `json:"string,omitempty"`
	TotalBytes uint64     `json:"total_bytes,omitempty"`
	TotalRows  uint64     `json:"total_rows,omitempty"`
	Status     StatusMap  `json:"status,omitempty"`
	StartTime  *time.Time `json:"start_time,omitempty"`
	EndTime    *time.Time `json:"end_time,omitempty"`
	Duration   int64      `json:"duration,omitempty"`
	Error      *string    `json:"error,omitempty"`
}

type StatusMap struct {
	Count     int `json:"count"`
	Success   int `json:"success"`
	Running   int `json:"running"`
	Skipped   int `json:"skipped"`
	Cancelled int `json:"cancelled"`
	Warning   int `json:"warning"`
	Error     int `json:"error"`
}

type RunState struct {
	ID         string                  `json:"id,omitempty"`
	Stream     *StreamState            `json:"stream,omitempty"`
	Object     *ObjectState            `json:"object,omitempty"`
	TotalBytes uint64                  `json:"total_bytes,omitempty"`
	TotalRows  uint64                  `json:"total_rows,omitempty"`
	Status     ExecStatus              `json:"status,omitempty"`
	StartTime  *time.Time              `json:"start_time,omitempty"`
	EndTime    *time.Time              `json:"end_time,omitempty"`
	Duration   int64                   `json:"duration,omitempty"`
	Error      *string                 `json:"error,omitempty"`
	Config     ReplicationStreamConfig `json:"config,omitempty"`
	Task       *TaskExecution          `json:"-"`
}

type ConnState struct {
	Name      string    `json:"name,omitempty"`
	Type      dbio.Type `json:"type,omitempty"`
	Kind      dbio.Kind `json:"kind,omitempty"`
	Bucket    string    `json:"bucket,omitempty"`
	Container string    `json:"container,omitempty"`
	Database  string    `json:"database,omitempty"`
	Instance  string    `json:"instance,omitempty"`
	Schema    string    `json:"schema,omitempty"`
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

		state.Execution.FilePath = t.Config.Env["SLING_CONFIG_PATH"]

		fMap, _ := t.Config.GetFormatMap()

		runID := iop.CleanName(t.Replication.Normalize(t.Config.StreamName))
		if id := cast.ToString(fMap["stream_run_id"]); id != "" {
			runID = id
		}

		run := state.Runs[runID]
		if run == nil {
			run = &RunState{
				Status: ExecStatusCreated,
				Stream: &StreamState{},
				Object: &ObjectState{},
			}
		}

		run.Stream.FileFolder = cast.ToString(fMap["stream_file_folder"])
		run.Stream.FileName = cast.ToString(fMap["stream_file_name"])
		run.Stream.FileExt = cast.ToString(fMap["stream_file_ext"])
		run.Stream.FilePath = cast.ToString(fMap["stream_file_path"])
		run.Stream.Name = cast.ToString(fMap["stream_name"])
		run.Stream.FullName = cast.ToString(fMap["stream_full_name"])
		run.Stream.Schema = cast.ToString(fMap["stream_schema"])
		run.Stream.Table = cast.ToString(fMap["stream_table"])

		run.Object.Name = cast.ToString(fMap["object_name"])
		run.Object.FullName = cast.ToString(fMap["object_full_name"])
		run.Object.Schema = cast.ToString(fMap["object_schema"])
		run.Object.Table = cast.ToString(fMap["object_table"])

		state.Runs[runID] = run

		state.Stream = run.Stream
		state.Object = run.Object

		bytes, _ := t.GetBytes()
		run.TotalBytes = bytes
		run.TotalRows = t.GetCount()
		run.Status = t.Status
		run.StartTime = t.StartTime
		run.EndTime = t.EndTime
		run.Config = g.PtrVal(t.Config.ReplicationStream)
		run.Config.Hooks = HookMap{} // no nested values
		if run.StartTime != nil {
			if run.EndTime != nil {
				run.Duration = cast.ToInt64(run.EndTime.Sub(g.PtrVal(run.StartTime)).Seconds())
			} else {
				run.Duration = cast.ToInt64(time.Since(g.PtrVal(run.StartTime)).Seconds())
			}
		}
		run.Task = t

		if t.Err != nil {
			run.Error = g.Ptr(t.Err.Error())
		}

		// aggregate statuses, rows and bytes
		errGroup := g.ErrorGroup{}
		state.Execution.Status = StatusMap{}
		state.Execution.TotalBytes = 0
		state.Execution.TotalRows = 0

		for _, run := range state.Runs {
			state.Execution.TotalBytes = state.Execution.TotalBytes + run.TotalBytes
			state.Execution.TotalRows = state.Execution.TotalRows + run.TotalRows

			switch run.Status {
			case ExecStatusSuccess:
				state.Execution.Status.Success++
			case ExecStatusError:
				state.Execution.Status.Error++
			case ExecStatusWarning:
				state.Execution.Status.Warning++
			case ExecStatusSkipped:
				state.Execution.Status.Skipped++
			case ExecStatusRunning:
				state.Execution.Status.Running++
			}
			state.Execution.Status.Count++

			if run.Error != nil {
				errGroup.Add(g.Error(*run.Error))
			}
		}

		// determine if ended
		if state.Execution.Status.Count == (state.Execution.Status.Success+state.Execution.Status.Error+state.Execution.Status.Warning+state.Execution.Status.Skipped) && state.Execution.Status.Running == 0 {
			state.Execution.EndTime = g.Ptr(time.Now())
			state.Execution.Duration = state.Execution.EndTime.Unix() - state.Execution.StartTime.Unix()
		} else {
			state.Execution.Duration = time.Now().Unix() - state.Execution.StartTime.Unix()
		}

		if err := errGroup.Err(); err != nil {
			state.Execution.Error = g.Ptr(err.Error())
		}

		// set as active run
		state.Run = run
	}
}
