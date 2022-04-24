package sling

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// ExecutionStatus is an execution status object
type ExecutionStatus struct {
	JobID       int        `json:"job_id,omitempty"`
	ExecID      int64      `json:"exec_id,omitempty"`
	Status      ExecStatus `json:"status,omitempty"`
	Text        string     `json:"text,omitempty"`
	Rows        uint64     `json:"rows,omitempty"`
	Bytes       uint64     `json:"bytes,omitempty"`
	Percent     int        `json:"percent,omitempty"`
	Stalled     bool       `json:"stalled,omitempty"`
	Duration    *int       `json:"duration,omitempty"`
	AvgDuration int        `json:"avg_duration,omitempty"`
}

// NewTask creates a Sling task with given configuration
func NewTask(execID int64, cfg *Config) (t *TaskExecution) {
	t = &TaskExecution{
		ExecID:       execID,
		Config:       cfg,
		Status:       ExecStatusCreated,
		df:           iop.NewDataflow(),
		PBar:         NewPBar(time.Second),
		ProgressHist: []string{},
	}

	err := cfg.Prepare()
	if err != nil {
		t.Err = g.Error(err, "could not prepare task")
		return
	}

	if ShowProgress {
		// progress bar ticker
		t.PBar = NewPBar(time.Second)
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for {
				select {
				case <-ticker.C:
					cnt := t.df.Count()
					if cnt > 1000 {
						t.PBar.Start()
						t.PBar.bar.SetCurrent(cast.ToInt64(cnt))
						t.PBar.bar.Set("bytes", t.GetBytesString())
						rowRate, byteRate := t.GetRate(4)
						t.PBar.bar.Set("rowRate", g.F("%s r/s", humanize.Comma(rowRate)))
						t.PBar.bar.Set("byteRate", g.F("%s/s", humanize.Bytes(cast.ToUint64(byteRate))))
					}
				default:
					time.Sleep(100 * time.Millisecond)
					if t.PBar.finished || t.df.Err() != nil {
						t.PBar.bar.SetCurrent(cast.ToInt64(t.df.Count()))
						t.PBar.Finish()
						return
					}
				}
			}
		}()
	}

	srcFileProvided := cfg.Options.StdIn || cfg.SrcConn.Info().Type.IsFile()
	tgtFileProvided := cfg.Options.StdOut || cfg.TgtConn.Info().Type.IsFile()
	srcDbProvided := cfg.SrcConn.Info().Type.IsDb()
	tgtDbProvided := cfg.TgtConn.Info().Type.IsDb()
	srcAPIProvided := cfg.SrcConn.Info().Type.IsAPI() || cfg.SrcConn.Info().Type.IsAirbyte()
	srcStreamProvided := cfg.Source.Stream != ""

	summary := g.F("srcFileProvided: %t, tgtFileProvided: %t, srcDbProvided: %t, tgtDbProvided: %t, srcStreamProvided: %t, srcAPIProvided: %t", srcFileProvided, tgtFileProvided, srcDbProvided, tgtDbProvided, srcStreamProvided, srcAPIProvided)
	g.Trace(summary)

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

	if srcDbProvided && tgtDbProvided {
		if cfg.Target.Mode == "upsert" && (len(cfg.Target.UpdateKey) == 0 || len(cfg.Target.PrimaryKey) == 0) {
			t.Err = g.Error("Must specify update_key / primary_key for 'upsert' mode")
			return
		}
		t.Type = DbToDb
	} else if srcFileProvided && tgtDbProvided {
		t.Type = FileToDB
	} else if srcDbProvided && srcStreamProvided && !tgtDbProvided && tgtFileProvided {
		t.Type = DbToFile
	} else if srcFileProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		t.Type = FileToFile
	} else if srcAPIProvided && srcStreamProvided && tgtDbProvided {
		t.Type = APIToDb
	} else if srcAPIProvided && srcStreamProvided && !srcDbProvided && !tgtDbProvided && tgtFileProvided {
		t.Type = APIToFile
	} else if tgtDbProvided && cfg.Target.Options.PostSQL != "" {
		cfg.Target.Object = cfg.Target.Options.PostSQL
		t.Type = DbSQL
	}

	if t.Type == "" {
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

		// []string{
		// 	g.F("Source File Provided: %t", srcFileProvided),
		// 	g.F("Target File Provided: %t", tgtFileProvided),
		// 	g.F("Source DB Provided: %t", srcDbProvided),
		// 	g.F("Target DB Provided: %t", tgtDbProvided),
		// 	g.F("Source Stream Provided: %t", srcStreamProvided),
		// 	g.F("Source API Provided: %t", srcAPIProvided),
		// }

		t.Err = g.Error("invalid Task Configuration. Must specify valid source conn / file or target connection / output.\n  %s", strings.Join(output, "\n  "))
	}

	return
}

// TaskExecution is a sling ELT task run, synonymous to an execution
type TaskExecution struct {
	ExecID        int64           `json:"exec_id"`
	Config        *Config         `json:"config"`
	Type          JobType         `json:"type"`
	Status        ExecStatus      `json:"status"`
	Err           error           `json:"error"`
	StartTime     *time.Time      `json:"start_time"`
	EndTime       *time.Time      `json:"end_time"`
	Bytes         uint64          `json:"bytes"`
	Ctx           context.Context `json:"-"`
	Progress      string          `json:"progress"`
	df            *iop.Dataflow   `json:"-"`
	prevRowCount  uint64
	prevByteCount uint64
	lastIncrement time.Time // the time of last row increment (to determine stalling)

	ProgressHist   []string     `json:"progress_hist"`
	PBar           *ProgressBar `json:"-"`
	ProcStatsStart g.ProcStats  `json:"-"` // process stats at beginning
}

// SetProgress sets the progress
func (t *TaskExecution) SetProgress(progressText string, args ...interface{}) {
	progressText = g.F(progressText, args...)
	t.ProgressHist = append(t.ProgressHist, progressText)
	t.Progress = progressText
	if !t.PBar.started || t.PBar.finished {
		g.Info(progressText)
	} else {
		t.PBar.SetStatus(progressText)
	}
}

// GetTotalBytes gets the inbound/oubound bytes of the task
func (t *TaskExecution) GetTotalBytes() (rcBytes, txBytes uint64) {
	procStatsEnd := g.GetProcStats(os.Getpid())

	switch {
	case g.In(t.Config.SrcConn.Type, dbio.TypeDbPostgres, dbio.TypeDbOracle, dbio.TypeDbMySQL):
		rcBytes = procStatsEnd.RcBytes - t.ProcStatsStart.RcBytes
	case g.In(t.Config.SrcConn.Type, dbio.TypeDbSnowflake, dbio.TypeDbBigQuery, dbio.TypeDbRedshift):
		rcBytes = procStatsEnd.RcBytes - t.ProcStatsStart.RcBytes
	case g.In(t.Config.SrcConn.Type, dbio.TypeFileLocal):
		rcBytes = procStatsEnd.ReadBytes - t.ProcStatsStart.ReadBytes
	default:
	}

	switch {
	case g.In(t.Config.TgtConn.Type, dbio.TypeDbPostgres, dbio.TypeDbOracle, dbio.TypeDbMySQL):
		txBytes = procStatsEnd.TxBytes - t.ProcStatsStart.TxBytes
	case g.In(t.Config.TgtConn.Type, dbio.TypeDbSnowflake, dbio.TypeDbBigQuery, dbio.TypeDbRedshift):
		txBytes = procStatsEnd.TxBytes - t.ProcStatsStart.TxBytes
	case g.In(t.Config.TgtConn.Type, dbio.TypeFileLocal):
		txBytes = procStatsEnd.WriteBytes - t.ProcStatsStart.WriteBytes
	default:
	}

	switch {
	case t.Type == DbToDb:
	case t.Type == DbToFile:
	case t.Type == FileToDB:
	case t.Type == FileToFile:
	}
	return
}
