//go:build !darwin && !windows

package main

import (
	"os"
	"syscall"
)

// statusSignals ask a continuous run to print its status.
var statusSignals = []os.Signal{syscall.SIGUSR1}
