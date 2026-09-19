//go:build !cgo

package database

import (
	"github.com/flarco/g"
)

type ArrowDBConn struct {
	BaseConn
	URL string
}

func (conn *ArrowDBConn) Init() error {
	return g.Error("ADBC connection requires CGO")
}

// NewAdbcConn is a stub when CGO is disabled
func NewAdbcConn(parentConn Connection) (adbcConn Connection, err error) {
	return nil, g.Error("ADBC connection requires CGO")
}
