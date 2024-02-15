package store

import (
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/connection"
	"github.com/slingdata-io/sling-cli/core/sling"
	"github.com/spf13/cast"
	"gorm.io/gorm/clause"
)

func init() {
	sling.StoreInsert = StoreInsert
	sling.StoreUpdate = StoreUpdate
}

type Execution struct {
	// ID auto-increments
	ID int64 `json:"id" gorm:"primaryKey"`

	// StreamID represents the stream inside the replication that is running.
	// Is an MD5 construct:`md5(Source, Target, Stream)`.
	StreamID string `json:"stream_id" sql:"not null" gorm:"index"`

	// ConfigMD5 points to config table. not null
	TaskMD5        string `json:"task_md5" sql:"not null" gorm:"index"`
	ReplicationMD5 string `json:"replication_md5" sql:"not null" gorm:"index"`

	Status    sling.ExecStatus `json:"status" gorm:"index"`
	Err       *string          `json:"error"`
	StartTime *time.Time       `json:"start_time" gorm:"index"`
	EndTime   *time.Time       `json:"end_time" gorm:"index"`
	Bytes     uint64           `json:"bytes"`
	ExitCode  int              `json:"exit_code"`
	Output    string           `json:"output" sql:"default ''"`
	Rows      uint64           `json:"rows"`
	Pid       int              `json:"pid"`

	// ProjectID represents the project or the repository.
	// If .git exists, grab first commit with `git rev-list --max-parents=0 HEAD`.
	// if not, use md5 of path of folder. Can be `null` if using task.
	ProjectID *string `json:"project_id" gorm:"index"`

	// FilePath represents the path to a file.
	// We would need this to refer back to the same file, even if
	// the contents change. So this should just be the relative path
	// of the replication.yaml or task.yaml from the root of the project.
	// If Ad-hoc from CLI flags, let it be `null`.
	FilePath *string `json:"file_path" gorm:"index"`

	CreatedDt time.Time `json:"created_dt" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt" gorm:"autoUpdateTime"`
}

type Task struct {
	// MD5 is MD5 of Config json string
	MD5 string `json:"md5" gorm:"primaryKey"`

	Type sling.JobType `json:"type"  gorm:"index"`

	Task sling.Config `json:"task"`

	CreatedDt time.Time `json:"created_dt" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt" gorm:"autoUpdateTime"`
}

type Replication struct {
	// MD5 is MD5 of Config json string
	MD5 string `json:"md5" gorm:"primaryKey"`

	Type sling.JobType `json:"type"  gorm:"index"`

	Replication sling.ReplicationConfig `json:"replication"`

	CreatedDt time.Time `json:"created_dt" gorm:"autoCreateTime"`
	UpdatedDt time.Time `json:"updated_dt" gorm:"autoUpdateTime"`
}

// Store saves the task into the local sqlite
func ToExecutionObject(t *sling.TaskExecution) *Execution {

	inputArgs := ""
	if val := os.Getenv("SLING_CLI_ARGS"); val != "" {
		inputArgs = " -- args: " + val
	}

	exec := Execution{
		ID:        t.ExecID,
		StreamID:  g.MD5(t.Config.Source.Conn, t.Config.Target.Conn, t.Config.Source.Stream),
		Status:    t.Status,
		StartTime: t.StartTime,
		EndTime:   t.EndTime,
		Bytes:     t.Bytes,
		Output:    inputArgs,
		Rows:      t.GetCount(),
		ProjectID: g.String(t.Config.Env["SLING_PROJECT_ID"]),
		FilePath:  g.String(t.Config.Env["SLING_CONFIG_PATH"]),
		Pid:       os.Getpid(),
	}

	if t.Err != nil {
		exec.Err = g.String(t.Err.Error())
	}

	if t.Replication != nil && t.Replication.Env["SLING_CONFIG_PATH"] != nil {
		exec.FilePath = g.String(cast.ToString(t.Replication.Env["SLING_CONFIG_PATH"]))
	}

	return &exec
}

func ToConfigObject(t *sling.TaskExecution) (task *Task, replication *Replication) {
	if t.Config == nil {
		return
	}

	task = &Task{
		Type: t.Type,
		Task: *t.Config,
	}

	if t.Replication != nil {
		replication = &Replication{
			Type:        t.Type,
			Replication: *t.Replication,
		}

		// set md5
		replication.MD5 = t.Replication.MD5()
	}

	// clean up
	if strings.Contains(task.Task.Source.Conn, "://") {
		task.Task.Source.Conn = strings.Split(task.Task.Source.Conn, "://")[0] + "://"
	}

	if strings.Contains(task.Task.Target.Conn, "://") {
		task.Task.Target.Conn = strings.Split(task.Task.Target.Conn, "://")[0] + "://"
	}

	task.Task.Source.Data = nil
	task.Task.Target.Data = nil

	task.Task.SrcConn = connection.Connection{}
	task.Task.TgtConn = connection.Connection{}

	task.Task.Prepared = false

	delete(task.Task.Env, "SLING_PROJECT_ID")
	delete(task.Task.Env, "SLING_CONFIG_PATH")

	// set md5
	task.MD5 = t.Config.MD5()

	return
}

// Store saves the task into the local sqlite
func StoreInsert(t *sling.TaskExecution) {
	if Db == nil {
		return
	}

	// make execution
	exec := ToExecutionObject(t)

	// insert config
	task, replication := ToConfigObject(t)
	err := Db.Clauses(clause.OnConflict{DoNothing: true}).
		Create(task).Error
	if err != nil {
		g.Debug("could not insert task config into local .sling.db. %s", err.Error())
		return
	}
	exec.TaskMD5 = task.MD5

	if replication != nil {
		err := Db.Clauses(clause.OnConflict{DoNothing: true}).
			Create(replication).Error
		if err != nil {
			g.Debug("could not insert replication config into local .sling.db. %s", err.Error())
			return
		}
		exec.ReplicationMD5 = replication.MD5
	}

	// insert execution
	err = Db.Create(exec).Error
	if err != nil {
		g.Debug("could not insert execution into local .sling.db. %s", err.Error())
		return
	}

	t.ExecID = exec.ID
}

// Store saves the task into the local sqlite
func StoreUpdate(t *sling.TaskExecution) {
	if Db == nil {
		return
	}

	exec := &Execution{ID: t.ExecID}
	err := Db.First(exec).Error
	if err != nil {
		g.Debug("could not select execution from local .sling.db. %s", err.Error())
		return
	}
	execNew := ToExecutionObject(t)

	exec.StartTime = execNew.StartTime
	exec.EndTime = execNew.EndTime
	exec.Status = execNew.Status
	exec.Err = execNew.Err
	exec.Bytes = execNew.Bytes
	exec.Rows = execNew.Rows
	exec.Output = execNew.Output

	err = Db.Updates(exec).Error
	if err != nil {
		g.Debug("could not update execution into local .sling.db. %s", err.Error())
		return
	}

}
