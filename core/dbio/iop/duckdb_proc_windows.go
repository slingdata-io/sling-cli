//go:build windows

package iop

import "syscall"

// duckDbSysProcAttr puts the sidecar in its own process group, so a console
// Ctrl-C (exit status 0xc000013a) reaches only sling, which then cancels the query.
func duckDbSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}
