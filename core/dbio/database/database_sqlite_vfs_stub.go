//go:build !cgo

package database

import (
	"github.com/flarco/g"
)

func registerHttpVFS(httpURL string) error {
	return g.Error("sqlite httpvfs requires CGO")
}
