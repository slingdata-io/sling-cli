package sling

import (
	"os"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
	"github.com/spf13/cast"
)

// Sling accepts a configuration and runs an Extract-Load task
func Sling(cfg *Config) (err error) {
	task := NewTask(env.ExecID, cfg)
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

// IsContinuousRunMode returns true when the run is in continuous mode
func IsContinuousRunMode() bool {
	return cast.ToBool(os.Getenv("SLING_CONTINUOUS"))
}

func IsPipelineRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "pipeline"
}

func IsBuildRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "build"
}
