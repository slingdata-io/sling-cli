//go:build windows

package database

import (
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

func (conn *DuckDbConn) BulkImportFlow(tableFName string, df *iop.Dataflow) (count uint64, err error) {
	if conn.useADBC() {
		// the CLI import paths below read the CSV through the CLI session,
		// which cannot share the instance file with the ADBC handle
		return conn.adbc.BulkImportFlow(tableFName, df)
	}
	// the deprecated copy_method value csv_files selects a csv transport of its own
	if conn.GetProp("copy_format") == "" && conn.GetProp("copy_method") == "csv_files" {
		return conn.importViaTempCSVs(tableFName, df)
	}

	format, err := conn.duck.SessionFormat()
	if err != nil {
		return 0, err
	}
	return conn.importViaHTTP(tableFName, df, format)
}
