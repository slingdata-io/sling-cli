package sling

import (
	"os"

	"github.com/flarco/g"
)

// Sling accepts a configuration and runs an Extract-Load task
func Sling(cfg *Config) (err error) {
	task := NewTask(os.Getenv("SLING_EXEC_ID"), cfg)
	if task.Err != nil {
		return g.Error(task.Err, "error creating Sling task")
	}

	err = task.Execute()
	if err != nil {
		return g.Error(err, "error running Sling task")
	}

	return
}

func IsReplicationRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "replication"
}

func IsPipelineRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "pipeline"
}
