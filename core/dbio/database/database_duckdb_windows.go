//go:build windows

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	switch conn.GetProp("copy_method") {
	case "csv_files":
		return conn.importViaTempCSVs(tableFName, df)
	case "http_server":
		return conn.importViaHTTP(tableFName, df)
	default:
		return conn.importViaTempCSVs(tableFName, df)
	}
}
