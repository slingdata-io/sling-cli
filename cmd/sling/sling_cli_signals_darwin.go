package main

import (
	"os"
	"syscall"
)

// statusSignals ask a continuous run to print its status (SIGINFO is Ctrl-T).
var statusSignals = []os.Signal{syscall.SIGUSR1, syscall.SIGINFO}
