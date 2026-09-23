//go:build windows

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if conn.useADBC() {
		// the CLI import paths below read the CSV through the CLI session,
		// which cannot share the instance file with the ADBC handle
		return conn.adbc.BulkImportFlow(tableFName, df)
	}
	switch conn.GetProp("copy_method") {
	case "csv_files":
		return conn.importViaTempCSVs(tableFName, df)
	case "csv_http":
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	case "arrow_http":
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeArrow)
	default:
		return conn.importViaHTTP(tableFName, df, dbio.FileTypeCsv)
	}
}
