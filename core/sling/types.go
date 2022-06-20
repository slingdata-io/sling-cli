package sling

// JobType is an enum type for jobs
type JobType string

const (

	// APIToDb is from api to db
	APIToDb JobType = "api-db"
	// APIToFile is from api to file
	APIToFile JobType = "api-file"

	// ConnTest is for a connection test
	ConnTest JobType = "conn-test"
)

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
	// ExecStatusInterrupted = terminated
	ExecStatusInterrupted ExecStatus = "interrupted"
	// ExecStatusTimedOut = timed-out (when no heartbeat sent for 30 sec)
	ExecStatusTimedOut ExecStatus = "timed-out"
	// ExecStatusError = error
	ExecStatusError ExecStatus = "error"
	// ExecStatusSkipped = skipped
	ExecStatusSkipped ExecStatus = "skipped"
	// ExecStatusStalled = stalled (when still heartbeating, but rows are unchanged for a while)
	ExecStatusStalled ExecStatus = "stalled"
)

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
	case ExecStatusError, ExecStatusTerminated, ExecStatusStalled, ExecStatusTimedOut:
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
