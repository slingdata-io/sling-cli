//go:build unix

package iop

import (
	"context"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The sidecar must not share the process group of sling, or a terminal
// Ctrl-C kills it before sling can cancel the query.
func TestDuckDbOwnProcessGroup(t *testing.T) {
	duck := NewDuckDb(context.Background())
	defer duck.Close()

	_, err := duck.Exec("select 1")
	require.NoError(t, err)

	childPgid, err := syscall.Getpgid(duck.Proc.Pid)
	require.NoError(t, err)
	assert.Equal(t, duck.Proc.Pid, childPgid)
	assert.NotEqual(t, syscall.Getpgrp(), childPgid)
}
