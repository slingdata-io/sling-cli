//go:build unix

package iop

import "syscall"

// duckDbSysProcAttr puts the sidecar in its own process group, so a terminal
// Ctrl-C reaches only sling, which then cancels the query.
func duckDbSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
