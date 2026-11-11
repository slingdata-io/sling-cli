package sling

import (
	"os"
	"sync/atomic"

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

var continuousMode atomic.Bool

// IsContinuousRunMode returns true when the run is in continuous mode
func IsContinuousRunMode() bool {
	return continuousMode.Load()
}

// SetContinuousRunMode sets the continuous mode of this process. The mode is
// not an env var, so child processes do not inherit it.
func SetContinuousRunMode(on bool) {
	continuousMode.Store(on)
}

// LoadContinuousRunMode reads SLING_CONTINUOUS one time, then removes it from
// the env so that child processes (command hooks) run one-shot.
func LoadContinuousRunMode() {
	if val, ok := os.LookupEnv("SLING_CONTINUOUS"); ok {
		os.Unsetenv("SLING_CONTINUOUS")
		if cast.ToBool(val) {
			SetContinuousRunMode(true)
		}
	}
}

func IsPipelineRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "pipeline"
}

func IsBuildRunMode() bool {
	return os.Getenv("SLING_RUN_MODE") == "build"
}
