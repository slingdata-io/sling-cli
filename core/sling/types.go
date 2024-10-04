package sling

// JobType is an enum type for jobs
type JobType string

// ConnTest is for a connection test
const ConnTest JobType = "conn-test"

// ConnTest is for a connection discover
const ConnDiscover JobType = "conn-discover"

// ConnTest is for a connection exec
const ConnExec JobType = "conn-exec"

// DbToDb is from db to db
const DbToDb JobType = "db-db"

// FileToDB is from db to db
const FileToDB JobType = "file-db"

// DbToFile is from db to file
const DbToFile JobType = "db-file"

// FileToFile is from file to file
const FileToFile JobType = "file-file"

// DbSQL is for a db sql query
const DbSQL JobType = "db-sql"

var AllJobType = []struct {
	Value  JobType
	TSName string
}{
	{ConnTest, "ConnTest"},
	{ConnDiscover, "ConnDiscover"},
	{ConnExec, "ConnExec"},
	{DbToDb, "DbToDb"},
	{FileToDB, "FileToDB"},
	{DbToFile, "DbToFile"},
	{FileToFile, "FileToFile"},
	{DbSQL, "DbSQL"},
}

// ExecStatus is the status of an execution
type ExecStatus string

const (
	// ExecStatusCreated = created
	ExecStatusCreated ExecStatus = "created"
	// ExecStatusQueued = queued
	ExecStatusQueued ExecStatus = "queued"
	// ExecStatusStarted = started
	ExecStatusStarted ExecStatus = "started"
	// ExecStatusRunning = running
	ExecStatusRunning ExecStatus = "running"
	// ExecStatusSuccess = success
	ExecStatusSuccess ExecStatus = "success"
	// ExecStatusTerminated = terminated
	ExecStatusTerminated ExecStatus = "terminated"
	// ExecStatusInterrupted = interrupted
	ExecStatusInterrupted ExecStatus = "interrupted"
	// ExecStatusTimedOut = timed-out (when no heartbeat sent for 30 sec)
	ExecStatusTimedOut ExecStatus = "timed-out"
	// ExecStatusError = error
	ExecStatusError ExecStatus = "error"
	// ExecStatusSkipped = skipped
	ExecStatusSkipped ExecStatus = "skipped"
	// ExecStatusStalled = stalled (when still heartbeating, but rows are unchanged for a while)
	ExecStatusStalled ExecStatus = "stalled"
	// ExecStatusWarning = cancelled
	ExecStatusCancelled ExecStatus = "cancelled"
	// ExecStatusWarning = warning
	ExecStatusWarning ExecStatus = "warning"
)

var AllExecStatus = []struct {
	Value  ExecStatus
	TSName string
}{
	{ExecStatusCreated, "ExecStatusCreated"},
	{ExecStatusQueued, "ExecStatusQueued"},
	{ExecStatusStarted, "ExecStatusStarted"},
	{ExecStatusRunning, "ExecStatusRunning"},
	{ExecStatusSuccess, "ExecStatusSuccess"},
	{ExecStatusTerminated, "ExecStatusTerminated"},
	{ExecStatusInterrupted, "ExecStatusInterrupted"},
	{ExecStatusTimedOut, "ExecStatusTimedOut"},
	{ExecStatusError, "ExecStatusError"},
	{ExecStatusSkipped, "ExecStatusSkipped"},
	{ExecStatusStalled, "ExecStatusStalled"},
}

// IsRunning returns true if an execution is running
func (s ExecStatus) IsRunning() bool {
	switch s {
	case ExecStatusCreated, ExecStatusStarted, ExecStatusRunning:
		return true
	}
	return false
}

// IsFinished returns true if an execution is finished
func (s ExecStatus) IsFinished() bool {
	switch s {
	case ExecStatusSuccess, ExecStatusError,
		ExecStatusTerminated, ExecStatusStalled,
		ExecStatusInterrupted, ExecStatusTimedOut:
		return true
	}
	return false
}

// IsFailure returns true if an execution is failed
func (s ExecStatus) IsFailure() bool {
	switch s {
	case ExecStatusError, ExecStatusTerminated, ExecStatusInterrupted, ExecStatusStalled, ExecStatusTimedOut:
		return true
	}
	return false
}

// IsSuccess returns true if an execution is successful
func (s ExecStatus) IsSuccess() bool {
	switch s {
	case ExecStatusSuccess:
		return true
	}
	return false
}
