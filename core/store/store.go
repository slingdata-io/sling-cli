package store

import (
	"database/sql/driver"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gorm.io/gorm/clause"
)

func init() {

	sling.StoreInsert = func(t *sling.TaskExecution) error {
		_, err := StoreInsert(t)
		return err
	}

	sling.StoreUpdate = func(t *sling.TaskExecution) error {
		_, err := StoreUpdate(t)
		return err
	}
}

var syncStatus = func(e *Execution) {
	// drain channel for now
	for {
		select {
		case <-e.TaskExec.OutputLines:
		default:
			return
		}
	}
}

// Execution is a task execute in the store. PK = exec_id + stream_id
type Execution struct {
	// ID auto-increments
	ID int64 `json:"id,omitempty" gorm:"primaryKey"`

	ExecID string `json:"exec_id,omitempty" gorm:"index"`

	// StreamID represents the stream inside the replication that is running.
	// Is an MD5 construct:`md5(Source, Target, Stream, Object)`.
	StreamID string `json:"stream_id,omitempty" sql:"not null" gorm:"index"`

	// ConfigMD5 points to config table. not null
	TaskMD5        string `json:"task_md5,omitempty" sql:"not null" gorm:"index"`
	ReplicationMD5 string `json:"replication_md5,omitempty" sql:"not null" gorm:"index"`

	Status    sling.ExecStatus `json:"status,omitempty" gorm:"index"`
	Err       *string          `json:"error,omitempty"`
	StartTime *time.Time       `json:"start_time,omitempty" gorm:"index"`
	EndTime   *time.Time       `json:"end_time,omitempty" gorm:"index"`
	Bytes     uint64           `json:"bytes,omitempty"`
	ExitCode  int              `json:"exit_code,omitempty"`
	Output    string           `json:"output,omitempty" sql:"default ''"`
	Rows      uint64           `json:"rows,omitempty"`
	Pid       int              `json:"pid,omitempty"`
	Version   string           `json:"version,omitempty"`

	// ProjectID represents the project or the repository.
	// If .git exists, grab first commit with `git rev-list --max-parents=0 HEAD`.
	// if not, use md5 of path of folder. Can be `null` if using task.
	ProjectID *string `json:"project_id,omitempty" gorm:"index"`

	// FilePath represents the path to a file.
	// We would need this to refer back to the same file, even if
	// the contents change. So this should just be the relative path
	// of the replication.yaml or task.yaml from the root of the project.
	// If Ad-hoc from CLI flags, let it be `null`.
	FilePath *string `json:"file_path,omitempty" gorm:"index"`

	// WorkPath is where the sling process ran from
	WorkPath *string `json:"work_path,omitempty"`

	CreatedDt time.Time `json:"created_dt,omitempty" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt,omitempty" gorm:"autoUpdateTime"`

	Task        *Task        `json:"task,omitempty" gorm:"-"`
	Replication *Replication `json:"replication,omitempty" gorm:"-"`

	TaskExec *sling.TaskExecution `json:"-" gorm:"-"`
}

type Task struct {
	ProjectID *string `json:"project_id,omitempty" gorm:"index"`

	// MD5 is MD5 of Config json string
	MD5 string `json:"md5" gorm:"primaryKey"`

	Type sling.JobType `json:"type"  gorm:"index"`

	Config sling.Config `json:"config"`

	CreatedDt time.Time `json:"created_dt" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt" gorm:"autoUpdateTime"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (t *Task) Scan(value interface{}) error {
	return g.JSONScanner(t, value)
}

// Value return json value, implement driver.Valuer interface
func (t Task) Value() (driver.Value, error) {
	return g.JSONValuer(t, "{}")
}

type Replication struct {
	Name string `json:"name"  gorm:"index"`

	ProjectID *string `json:"project_id,omitempty" gorm:"index"`

	// MD5 is MD5 of Config json string
	MD5 string `json:"md5" gorm:"primaryKey"`

	Type sling.JobType `json:"type"  gorm:"index"`

	Config string `json:"config"` // Original config

	ID *string `json:"id"  gorm:"index"`

	CreatedDt time.Time `json:"created_dt" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt" gorm:"autoUpdateTime"`
}

// Scan scan value into Jsonb, implements sql.Scanner interface
func (r *Replication) Scan(value interface{}) error {
	return g.JSONScanner(r, value)
}

// Value return json value, implement driver.Valuer interface
func (r Replication) Value() (driver.Value, error) {
	return g.JSONValuer(r, "{}")
}

// Store saves the task into the local sqlite
func ToExecutionObject(t *sling.TaskExecution) *Execution {

	bytes, _ := t.GetBytes()

	exec := Execution{
		ExecID:    t.ExecID,
		StreamID:  t.Config.StreamID(),
		Status:    t.Status,
		StartTime: t.StartTime,
		EndTime:   t.EndTime,
		Bytes:     bytes,
		Output:    t.Output.String(),
		Rows:      t.GetCount(),
		ProjectID: g.String(t.Config.Env["SLING_PROJECT_ID"]),
		FilePath:  g.String(t.Config.Env["SLING_CONFIG_PATH"]),
		WorkPath:  g.String(t.Config.Env["SLING_WORK_PATH"]),
		Pid:       os.Getpid(),
		Version:   core.Version,
		TaskExec:  t,
	}

	if t.Err != nil {
		err, ok := t.Err.(*g.ErrType)
		if ok {
			exec.Err = g.String(err.Debug())
		} else {
			exec.Err = g.String(t.Err.Error())
		}
	}

	if fileName := os.Getenv("SLING_REPLICATION_NAME"); fileName != "" {
		exec.FilePath = g.String(fileName)
	} else if t.Replication != nil && t.Replication.Env["SLING_CONFIG_PATH"] != nil {
		exec.FilePath = g.String(cast.ToString(t.Replication.Env["SLING_CONFIG_PATH"]))
	}

	return &exec
}

func ToConfigObject(t *sling.TaskExecution) (task *Task, replication *Replication) {
	if t.Config == nil {
		return
	}

	task = &Task{
		Type:   t.Type,
		Config: *t.Config,
	}

	projID := t.Config.Env["SLING_PROJECT_ID"]
	if projID != "" {
		task.ProjectID = g.String(projID)
	}

	if t.Replication != nil {
		replication = &Replication{
			Name:   t.Config.Env["SLING_CONFIG_PATH"],
			Type:   t.Type,
			MD5:    t.Replication.MD5(),
			Config: t.Replication.OriginalCfg(),
		}
		if id := os.Getenv("SLING_REPLICATION_ID"); id != "" {
			replication.ID = g.String(id)
		}

		if projID != "" {
			replication.ProjectID = g.String(projID)
		}
	}

	// clean up
	if strings.Contains(task.Config.Source.Conn, "://") {
		task.Config.Source.Conn = strings.Split(task.Config.Source.Conn, "://")[0] + "://"
	}

	if strings.Contains(task.Config.Target.Conn, "://") {
		task.Config.Target.Conn = strings.Split(task.Config.Target.Conn, "://")[0] + "://"
	}

	task.Config.Source.Data = nil
	task.Config.Target.Data = nil

	task.Config.SrcConn = connection.Connection{}
	task.Config.TgtConn = connection.Connection{}

	task.Config.Prepared = false

	delete(task.Config.Env, "SLING_PROJECT_ID")
	delete(task.Config.Env, "SLING_CONFIG_PATH")
	delete(task.Config.Env, "SLING_WORK_PATH")

	// set md5
	task.MD5 = t.Config.MD5()

	return
}

// Store saves the task into the local sqlite
func StoreInsert(t *sling.TaskExecution) (exec *Execution, err error) {
	if Db == nil {
		return
	}

	// make execution
	exec = ToExecutionObject(t)

	// determine if execution already exists
	result := g.M()
	Db.Raw(`select count(1) cnt from executions where exec_id = ? and stream_id = ?`, exec.ExecID, exec.StreamID).Scan(&result)
	if cnt := cast.ToInt(result["cnt"]); cnt > 0 {
		return StoreUpdate(t)
	}

	// insert config
	task, replication := ToConfigObject(t)
	err = Db.Clauses(clause.OnConflict{DoNothing: true}).Create(task).Error
	if err != nil {
		g.Error(err, "could not insert task config into local .sling.db.")
		return
	}
	exec.Task = task
	exec.TaskMD5 = task.MD5

	if replication != nil {
		clauses := lo.Ternary(
			replication.ID != nil,
			clause.OnConflict{UpdateAll: true},
			clause.OnConflict{DoNothing: true},
		)
		err = Db.Clauses(clauses).Create(replication).Error
		if err != nil {
			g.Error(err, "could not insert replication config into local .sling.db.")
			return
		}
		exec.Replication = replication
		exec.ReplicationMD5 = replication.MD5
	}

	// insert execution
	err = Db.Create(exec).Error
	if err != nil {
		g.Error(err, "could not insert execution into local .sling.db.")
		return
	}

	t.ExecID = exec.ExecID

	// sync status
	syncStatus(exec)

	return
}

// Store saves the task into the local sqlite
func StoreUpdate(t *sling.TaskExecution) (exec *Execution, err error) {
	if Db == nil {
		return
	}
	e := ToExecutionObject(t)

	exec = &Execution{ExecID: t.ExecID, StreamID: e.StreamID, TaskExec: t}
	err = Db.Omit("output").Where("exec_id = ? and stream_id = ?", t.ExecID, e.StreamID).First(exec).Error
	if err != nil {
		g.Error(err, "could not select execution from local .sling.db.")
		return
	}

	exec.StartTime = e.StartTime
	exec.EndTime = e.EndTime
	exec.Status = e.Status
	exec.Err = e.Err
	exec.Bytes = e.Bytes
	exec.Rows = e.Rows
	exec.Output = e.Output

	err = Db.Updates(exec).Error
	if err != nil {
		g.Error(err, "could not update execution into local .sling.db")
		return
	}

	// sync status
	syncStatus(exec)

	return
}
