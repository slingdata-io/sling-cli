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
	sql := r.Duck.MakeScanQuery("iceberg_scanner", r.URI, sc)
	return sql
}
