package sling

import (
	"time"

	"github.com/flarco/g"
)

// Sling accepts a configuration and runs an Extract-Load task
func Sling(cfg Config) (err error) {

	err = cfg.Prepare()
	if err != nil {
		return g.Error(err, "unable to accept Sling config")
	}

	task := NewTask(int(time.Now().Unix()), cfg)
	if task.Err != nil {
		return g.Error(task.Err, "error creating Sling task")
	}

	err = task.Execute()
	if err != nil {
		return g.Error(err, "error running Sling task")
	}

	return
}
