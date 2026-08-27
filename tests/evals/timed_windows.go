//go:build windows

package evals

import (
	"os"
	"os/exec"
)

func setProcGroup(cmd *exec.Cmd) {}

func killProcGroup(pid int) {
	if p, err := os.FindProcess(pid); err == nil {
		_ = p.Kill()
	}
}

func pidAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return p.Signal(os.Interrupt) == nil
}
