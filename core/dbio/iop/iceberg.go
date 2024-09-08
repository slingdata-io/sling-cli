package iop

import (
	"context"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

type IcebergReader struct {
	URI     string
	Duck    *DuckDb
	columns Columns
}

func NewIcebergReader(uri string, props ...string) (*IcebergReader, error) {
	duck := NewDuckDb(context.Background(), props...)

	// load extension
	duck.AddExtension("iceberg")

	uri = duck.PrepareFsSecretAndURI(uri)

	return &IcebergReader{
		URI:  uri,
		Duck: duck,
	}, nil
}

func (r *IcebergReader) Columns() (Columns, error) {
	if len(r.columns) > 0 {
		return r.columns, nil
	}

	var err error
	r.columns, err = r.Duck.Describe(r.MakeQuery(FileStreamConfig{}))
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	return r.columns, nil
}

func (i *IcebergReader) Close() error {
	return i.Duck.Close()
}

func (r *IcebergReader) MakeQuery(sc FileStreamConfig) string {
	if sql := sc.SQL; sql != "" {
		scannerFunc := dbio.TypeDbDuckDb.GetTemplateValue("function.iceberg_scanner")
		sql = g.R(sql, "stream_scanner", scannerFunc)
		sql = g.R(sql, "uri", r.URI)
		return sql
	}
	return r.Duck.MakeScanSelectQuery("iceberg_scan", r.URI, sc.Select, sc.IncrementalKey, sc.IncrementalValue, cast.ToUint64(sc.Limit))
}
