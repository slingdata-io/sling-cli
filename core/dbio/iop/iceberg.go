package iop

import (
	"context"

	"github.com/flarco/g"
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

func (i *IcebergReader) Columns() (Columns, error) {
	if len(i.columns) > 0 {
		return i.columns, nil
	}

	var err error
	i.columns, err = i.Duck.Describe(i.MakeSelectQuery(nil, 0))
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	return i.columns, nil
}

func (i *IcebergReader) Close() error {
	return i.Duck.Close()
}

func (i *IcebergReader) MakeSelectQuery(fields []string, limit uint64) string {
	return i.Duck.MakeScanSelectQuery("iceberg_scan", i.URI, fields, limit)
}
