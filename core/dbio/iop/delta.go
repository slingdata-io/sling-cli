package iop

import (
	"context"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
)

type DeltaReader struct {
	URI     string
	Duck    *DuckDb
	columns Columns
}

func NewDeltaReader(uri string, props ...string) (*DeltaReader, error) {
	duck := NewDuckDb(context.Background(), props...)

	// load extension
	duck.AddExtension("delta")

	uri = duck.PrepareFsSecretAndURI(uri)

	return &DeltaReader{
		URI:  uri,
		Duck: duck,
	}, nil
}

func (r *DeltaReader) Columns() (Columns, error) {
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

func (r *DeltaReader) Close() error {
	return r.Duck.Close()
}

func (r *DeltaReader) MakeQuery(sc FileStreamConfig) string {
	sql := r.Duck.MakeScanQuery(dbio.FileTypeDelta, r.URI, sc)
	return sql
}
