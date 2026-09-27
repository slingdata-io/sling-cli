//go:build windows

package iop

import (
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/env"
)

// duckDbSysProcAttr puts the sidecar in its own process group, so a console
// Ctrl-C (exit status 0xc000013a) reaches only sling, which then cancels the query.
func duckDbSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// newDuckOutput sets a temp file path. Windows has no /dev/stdout, and its
// named pipes are not files that DuckDB can COPY to.
func newDuckOutput() (out *duckOutput, err error) {
	dir, err := os.MkdirTemp(env.GetTempFolder(), "sling-duckdb-")
	if err != nil {
		return nil, g.Error(err, "could not create temp folder for duckdb output")
	}
	return &duckOutput{dir: dir, path: filepath.Join(dir, "output")}, nil
}

// open returns the output after the query ends. waitQuery returns when the
// query ends.
func (out *duckOutput) open(waitQuery func()) (_ io.Reader, err error) {
	waitQuery()
	if out.reader, err = os.Open(out.path); err != nil {
		return nil, g.Error(err, "could not open duckdb output file")
	}
	return out.reader, nil
}
