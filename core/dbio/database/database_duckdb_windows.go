//go:build windows

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	switch conn.GetProp("copy_method") {
	case "csv_files":
		return conn.importViaTempCSVs(tableFName, df)
	case "csv_http":
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	default:
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	}
}
