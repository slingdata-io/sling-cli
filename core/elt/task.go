package elt

import (
	"bytes"
	"context"
	"os/exec"
	"time"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	pb "gopkg.in/cheggaaa/pb.v2"
)

// TaskProcess is a slingELT task / execution process
type TaskProcess struct {
	Task      Task         `json:"-"`
	JobID     int          `json:"job_id" mapstructure:"job_id"`
	ExecID    int          `json:"exec_id" mapstructure:"exec_id"`
	Pid       int          `json:"pid" mapstructure:"pid"`
	Stderr    string       `json:"stderr" mapstructure:"stderr"`
	Stdout    string       `json:"stdout" mapstructure:"stdout"`
	Err       error        `json:"-"`
	StartTime time.Time    `json:"start_time" mapstructure:"start_time"`
	EndTime   time.Time    `json:"end_time" mapstructure:"end_time"`
	ExitCode  int          `json:"exit_code" mapstructure:"exit_code"`
	RowRate   int          `json:"-"`
	StderrBuf bytes.Buffer `json:"-"`
	StdoutBuf bytes.Buffer `json:"-"`
	Cmd       *exec.Cmd    `json:"-"`
}

// ExecutionStatus is an execution status object
type ExecutionStatus struct {
	JobID       int        `json:"job_id,omitempty"`
	ExecID      int        `json:"exec_id,omitempty"`
	Status      ExecStatus `json:"status,omitempty"`
	Text        string     `json:"text,omitempty"`
	Rows        int        `json:"rows,omitempty"`
	Percent     int        `json:"percent,omitempty"`
	Stalled     bool       `json:"stalled,omitempty"`
	Duration    *int       `json:"duration,omitempty"`
	AvgDuration int        `json:"avg_duration,omitempty"`
}

// ToMap converts to map of interface
func (tp *TaskProcess) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"job_id":     tp.JobID,
		"exec_id":    tp.ExecID,
		"pid":        tp.Pid,
		"stderr":     tp.Stderr,
		"stdout":     tp.Stdout,
		"start_time": tp.StartTime,
		"end_time":   tp.EndTime,
		"exit_code":  tp.ExitCode,
	}
	return m
}

// NewTask creates a Sling task with given configuration
func NewTask(execID int, cfg Config) (t Task) {
	err := cfg.Prepare()
	if err != nil {
		t.Err = g.Error(err, "could not prepare task")
		return
	}

	t = Task{
		ExecID:       execID,
		Cfg:          cfg,
		Status:       ExecStatusCreated,
		df:           iop.NewDataflow(),
		pbar:         iop.NewPBar(time.Second),
		ProgressHist: []string{},
	}

	srcFileProvided := cfg.Options.StdIn || cfg.SrcConn.Info().Type.IsFile()
	tgtFileProvided := cfg.Options.StdOut || cfg.TgtConn.Info().Type.IsFile()
	srcDbProvided := cfg.SrcConn.Info().Type.IsDb()
	tgtDbProvided := cfg.TgtConn.Info().Type.IsDb()
	srcTableQueryProvided := cfg.Source.Stream != ""

	if cfg.Target.Mode == "" {
		cfg.Target.Mode = AppendMode
	}
	validMode := cfg.Target.Mode != Mode("")
	if !validMode {
		t.Err = g.Error("must specify valid mode: append, drop, upsert or truncate")
		return
	}

	if cfg.Target.Mode == "upsert" && (len(cfg.Target.PrimaryKey) == 0 || len(cfg.Target.UpdateKey) == 0) {
		t.Err = g.Error("must specify value for 'primary_key' and 'update_key' for mode upsert in configration text (with: append, drop, upsert or truncate")
		return
	}

	if srcDbProvided && tgtDbProvided && cfg.Target.DbtConfig == nil {
		if cfg.Target.Mode == "upsert" && (len(cfg.Target.UpdateKey) == 0 || len(cfg.Target.PrimaryKey) == 0) {
			t.Err = g.Error("Must specify update_key / primary_key for 'upsert' mode")
			return
		}
		t.Type = DbToDb
	} else if srcFileProvided && tgtDbProvided && cfg.Target.DbtConfig == nil {
		t.Type = FileToDB
	} else if srcDbProvided && srcTableQueryProvided && !tgtDbProvided && tgtFileProvided {
		t.Type = DbToFile
	} else if srcFileProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		t.Type = FileToFile
	} else if tgtDbProvided && cfg.Target.Dbt != "" {
		t.Type = DbDbt
	} else if tgtDbProvided && cfg.Target.Object != "" {
		t.Type = DbSQL
	}

	if t.Type == "" {
		t.Err = g.Error("invalid Task Configuration. Must specify source conn / file or target connection / output. srcFileProvided: %t, tgtFileProvided: %t, srcDbProvided: %t, tgtDbProvided: %t, srcTableQueryProvided: %t", srcFileProvided, tgtFileProvided, srcDbProvided, tgtDbProvided, srcTableQueryProvided)
	}

	return
}

// Task is a sling ELT task run, synonymous to an execution
type Task struct {
	ExecID        int `json:"exec_id"`
	Cfg           Config
	Type          JobType          `json:"type"`
	Status        ExecStatus       `json:"status"`
	Err           error            `json:"error"`
	StartTime     *time.Time       `json:"start_time"`
	EndTime       *time.Time       `json:"end_time"`
	Bytes         uint64           `json:"bytes"`
	Ctx           context.Context  `json:"-"`
	SubTasks      map[string]*Task `json:"-"`
	Progress      string           `json:"progress"`
	df            *iop.Dataflow    `json:"-"`
	prevCount     uint64
	lastIncrement time.Time       // the time of last row increment (to determine stalling)
	ProgressHist  []string        `json:"progress_hist"`
	pbar          *pb.ProgressBar `json:"-"`
}

// SetProgress sets the progress
func (t *Task) SetProgress(progressText string, args ...interface{}) {
	g.Info(progressText, args...)
	progressText = g.F(progressText, args...)
	t.ProgressHist = append(t.ProgressHist, progressText)
	t.Progress = progressText
}
