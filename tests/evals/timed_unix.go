//go:build unix

package evals

import (
	"os/exec"
	"syscall"
)

func setProcGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func killProcGroup(pid int) {
	_ = syscall.Kill(-pid, syscall.SIGKILL)
}

func pidAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil
}
