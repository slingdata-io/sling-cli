package sling

import (
	"math"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/dbio"
	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// Set in the store/store.go file for history keeping
var StoreInsert, StoreUpdate func(t *TaskExecution)

// TaskExecution is a sling ELT task run, synonymous to an execution
type TaskExecution struct {
	ExecID    int64      `json:"exec_id"`
	Config    *Config    `json:"config"`
	Type      JobType    `json:"type"`
	Status    ExecStatus `json:"status"`
	Err       error      `json:"error"`
	StartTime *time.Time `json:"start_time"`
	EndTime   *time.Time `json:"end_time"`
	Bytes     uint64     `json:"bytes"`
	Context   *g.Context `json:"-"`
	Progress  string     `json:"progress"`

	df            *iop.Dataflow `json:"-"`
	prevRowCount  uint64
	prevByteCount uint64
	lastIncrement time.Time // the time of last row increment (to determine stalling)

	Replication    *ReplicationConfig
	ProgressHist   []string     `json:"progress_hist"`
	PBar           *ProgressBar `json:"-"`
	ProcStatsStart g.ProcStats  `json:"-"` // process stats at beginning
	cleanupFuncs   []func()
}

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
		cleanupFuncs: []func(){},
	}

	err := cfg.Prepare()
	if err != nil {
		t.Err = g.Error(err, "could not prepare task")
		return
	}

	t.Type, err = cfg.DetermineType()
	if err != nil {
		t.Err = g.Error(err, "could not determine type")
		return
	}

	if ShowProgress {
		// progress bar ticker
		t.PBar = NewPBar(time.Second)
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					cnt := t.df.Count()
					if cnt > 1000 {
						t.PBar.Start()
						t.PBar.bar.SetCurrent(cast.ToInt64(cnt))
						t.PBar.bar.Set("bytes", t.GetBytesString())
						rowRate, byteRate := t.GetRate(1)
						t.PBar.bar.Set("rowRate", g.F("%s r/s", humanize.Comma(rowRate)))
						t.PBar.bar.Set("byteRate", g.F("%s/s", humanize.Bytes(cast.ToUint64(byteRate))))
					}

					// update rows every 1sec
					StoreUpdate(t)
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

	return
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

// IsStalled determines if the task has stalled (no row increment)
func (t *TaskExecution) IsStalled(window float64) bool {
	if strings.Contains(t.Progress, "pre-sql") || strings.Contains(t.Progress, "post-sql") {
		return false
	}
	return time.Since(t.lastIncrement).Seconds() > window
}

// GetBytes return the current total of bytes processed
func (t *TaskExecution) GetBytes() (inBytes, outBytes uint64) {
	if t.df == nil {
		return
	}

	inBytes, outBytes = t.df.Bytes()
	if inBytes == 0 && outBytes == 0 {
		// use tx/rc bytes
		// stats := g.GetProcStats(os.Getpid())
		// inBytes = stats.RcBytes - t.ProcStatsStart.RcBytes
		// outBytes = stats.TxBytes - t.ProcStatsStart.TxBytes
	}
	return
}

func (t *TaskExecution) GetBytesString() (s string) {
	inBytes, _ := t.GetBytes()
	if inBytes == 0 {
		return ""
	}
	return g.F("%s", humanize.Bytes(inBytes))
	// if inBytes > 0 && inBytes == outBytes {
	// 	return g.F("%s", humanize.Bytes(inBytes))
	// }
	// return g.F("%s -> %s", humanize.Bytes(inBytes), humanize.Bytes(outBytes))
}

// GetCount return the current count of rows processed
func (t *TaskExecution) GetCount() (count uint64) {
	if t.StartTime == nil {
		return
	}

	return t.df.Count()
}

// GetRate return the speed of flow (rows / sec and bytes / sec)
// secWindow is how many seconds back to measure (0 is since beginning)
func (t *TaskExecution) GetRate(secWindow int) (rowRate, byteRate int64) {
	var secElapsed float64
	count := t.GetCount()
	bytes, _ := t.GetBytes()
	if t.StartTime == nil || t.StartTime.IsZero() {
		return
	} else if t.EndTime == nil || t.EndTime.IsZero() {
		st := *t.StartTime
		if secWindow <= 0 {
			secElapsed = time.Since(st).Seconds()
			rowRate = cast.ToInt64(math.Round(cast.ToFloat64(count) / secElapsed))
			byteRate = cast.ToInt64(math.Round(cast.ToFloat64(bytes) / secElapsed))
		} else {
			rowRate = cast.ToInt64(math.Round(cast.ToFloat64((count - t.prevRowCount) / cast.ToUint64(secWindow))))
			byteRate = cast.ToInt64(math.Round(cast.ToFloat64((bytes - t.prevByteCount) / cast.ToUint64(secWindow))))
			if t.prevRowCount < count {
				t.lastIncrement = time.Now()
			}
			t.prevRowCount = count
			t.prevByteCount = bytes
		}
	} else {
		st := *t.StartTime
		et := *t.EndTime
		secElapsed = cast.ToFloat64(et.UnixNano()-st.UnixNano()) / 1000000000.0
		rowRate = cast.ToInt64(math.Round(cast.ToFloat64(count) / secElapsed))
		byteRate = cast.ToInt64(math.Round(cast.ToFloat64(bytes) / secElapsed))
	}
	return
}

func (t *TaskExecution) getMetadata() (metadata iop.Metadata) {
	// need to loaded_at column for file incremental
	if t.Config.MetadataLoadedAt || t.Type == FileToDB {
		metadata.LoadedAt.Key = slingLoadedAtColumn
		metadata.LoadedAt.Value = t.StartTime.Unix()
	}
	if t.Config.MetadataStreamURL {
		metadata.StreamURL.Key = slingStreamURLColumn
	}
	return metadata
}

func (t *TaskExecution) isUsingPool() bool {
	if val := os.Getenv("SLING_POOL"); val != "" && !cast.ToBool(val) {
		return false
	}
	return cast.ToBool(os.Getenv("SLING_CLI")) && t.Config.ReplicationMode
}

func (t *TaskExecution) AddCleanupTask(f func()) {
	t.Context.Mux.Lock()
	defer t.Context.Mux.Unlock()
	t.cleanupFuncs = append(t.cleanupFuncs, f)
}

func (t *TaskExecution) Cleanup() {
	t.Context.Mux.Lock()
	defer t.Context.Mux.Unlock()

	for i, f := range t.cleanupFuncs {
		f()
		t.cleanupFuncs[i] = func() {} // in case it gets called again
	}
	if t.df != nil {
		t.df.CleanUp()
	}
}

// usingCheckpoint means it has an update_key and is incremental mode
func (t *TaskExecution) usingCheckpoint() bool {
	return t.Config.Source.HasUpdateKey() && t.Config.Mode == IncrementalMode
}

func (t *TaskExecution) sourceOptionsMap() (options map[string]any) {
	options = g.M()
	g.Unmarshal(g.Marshal(t.Config.Source.Options), &options)
	options["METADATA"] = g.Marshal(t.getMetadata())

	if t.Config.Source.Options.Columns != nil {
		columns := iop.Columns{}
		switch colsCasted := t.Config.Source.Options.Columns.(type) {
		case map[string]any:
			for colName, colType := range colsCasted {
				col := iop.Column{
					Name: colName,
					Type: iop.ColumnType(cast.ToString(colType)),
				}
				columns = append(columns, col)
			}
		case map[any]any:
			for colName, colType := range colsCasted {
				col := iop.Column{
					Name: cast.ToString(colName),
					Type: iop.ColumnType(cast.ToString(colType)),
				}
				columns = append(columns, col)
			}
		case []map[string]any:
			for _, colItem := range colsCasted {
				col := iop.Column{}
				g.Unmarshal(g.Marshal(colItem), &col)
				columns = append(columns, col)
			}
		case []any:
			for _, colItem := range colsCasted {
				col := iop.Column{}
				g.Unmarshal(g.Marshal(colItem), &col)
				columns = append(columns, col)
			}
		case iop.Columns:
			columns = colsCasted
		default:
			g.Warn("Config.Source.Options.Columns not handled: %T", t.Config.Source.Options.Columns)
		}

		// set as string so that StreamProcessor parses it
		options["columns"] = g.Marshal(iop.NewColumns(columns...))
	}
	if t.Config.Source.Options.Transforms != nil && len(t.Config.Source.Options.Transforms) > 0 {
		// set as string so that StreamProcessor parses it
		options["transforms"] = g.Marshal(t.Config.Source.Options.Transforms)
	}
	return
}

var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

// apply column casing
func applyColumnCasingToDf(df *iop.Dataflow, connType dbio.Type, casing *ColumnCasing) {

	if casing == nil || *casing == SourceColumnCasing {
		return
	}

	// convert to target system casing
	for i := range df.Columns {
		df.Columns[i].Name = applyColumnCasing(df.Columns[i].Name, *casing == SnakeColumnCasing, connType)
	}

	// propagate names
	for _, ds := range df.Streams {
		for i := range ds.Columns {
			ds.Columns[i].Name = applyColumnCasing(ds.Columns[i].Name, *casing == SnakeColumnCasing, connType)
		}

		for i := range ds.CurrentBatch.Columns {
			ds.CurrentBatch.Columns[i].Name = applyColumnCasing(ds.CurrentBatch.Columns[i].Name, *casing == SnakeColumnCasing, connType)
		}
	}
}

func applyColumnCasing(name string, toSnake bool, connType dbio.Type) string {
	// convert to snake case
	if toSnake {
		name = matchAllCap.ReplaceAllString(name, "${1}_${2}")
	}

	// clean up other weird chars
	name = iop.CleanName(name)

	// lower case for target file system
	if connType.DBNameUpperCase() {
		return strings.ToUpper(name)
	}
	return strings.ToLower(name)
}

func ErrorHelper(err error) (helpString string) {
	if err != nil {
		errString := strings.ToLower(err.Error())
		E, ok := err.(*g.ErrType)
		if ok && E.Debug() != "" {
			errString = strings.ToLower(E.Full())
		}

		switch {
		case strings.Contains(errString, "utf8") || strings.Contains(errString, "ascii"):
			helpString = "Perhaps the 'transforms' source option could help with encodings? See https://docs.slingdata.io/sling-cli/run/configuration#source"
		case strings.Contains(errString, "failed to verify certificate"):
			helpString = "Perhaps specifying `encrypt=true` and `TrustServerCertificate=true` properties could help? See https://docs.slingdata.io/connections/database-connections/sqlserver"
		case strings.Contains(errString, "ssl is not enabled on the server"):
			helpString = "Perhaps setting the 'sslmode' option could help? See https://docs.slingdata.io/connections/database-connections/postgres"
		case strings.Contains(errString, "invalid input syntax for type") || (strings.Contains(errString, " value ") && strings.Contains(errString, "is not recognized")) || strings.Contains(errString, "invalid character value"):
			helpString = "Perhaps setting a higher 'SAMPLE_SIZE' environment variable could help? This represents the number of records to process in order to infer column types (especially for file sources). The default is 900. Try 2000 or even higher.\nYou can also manually specify the column types with the `columns` source option. See https://docs.slingdata.io/sling-cli/run/configuration#source"
		case strings.Contains(errString, "bcp import"):
			helpString = "If facing issues with Microsoft's BCP, try disabling Bulk Loading with `use_bulk=false`. See https://docs.slingdata.io/sling-cli/run/configuration#target"
		}
	}
	return
}
