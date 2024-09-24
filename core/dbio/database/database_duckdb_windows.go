//go:build windows

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	return conn.importViaTempCSVs(tableFName, df)
}
