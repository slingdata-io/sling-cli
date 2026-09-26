//go:build unix

package iop

import (
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
)

// duckDbSysProcAttr puts the sidecar in its own process group, so a terminal
// Ctrl-C reaches only sling, which then cancels the query.
func duckDbSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}

// newDuckOutput makes a named pipe. The spare write end keeps the pipe open
// until the query ends: the reader does not block on open, and it gets EOF
// only after the COPY closes the pipe and the query ends.
func newDuckOutput() (out *duckOutput, err error) {
	dir, err := os.MkdirTemp(env.GetTempFolder(), "sling-duckdb-")
	if err != nil {
		return nil, g.Error(err, "could not create temp folder for duckdb output")
	}
	out = &duckOutput{dir: dir, path: filepath.Join(dir, "output")}

	if err = syscall.Mkfifo(out.path, 0600); err != nil {
		out.Close()
		return nil, g.Error(err, "could not create named pipe for duckdb output")
	}
	if out.reader, err = os.OpenFile(out.path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err != nil {
		out.Close()
		return nil, g.Error(err, "could not open named pipe for duckdb output")
	}
	if out.hold, err = os.OpenFile(out.path, os.O_WRONLY, 0); err != nil {
		out.Close()
		return nil, g.Error(err, "could not open named pipe for duckdb output")
	}
	// the poller does not take a named pipe on all systems
	if err = syscall.SetNonblock(int(out.reader.Fd()), false); err != nil {
		out.Close()
		return nil, g.Error(err, "could not set named pipe for duckdb output")
	}
	return out, nil
}

// open returns the output while the query runs. waitQuery returns when the
// query ends.
func (out *duckOutput) open(waitQuery func()) (io.Reader, error) {
	go func() {
		waitQuery()
		out.hold.Close()
	}()
	return out.reader, nil
}
