package iop

import (
	"context"

	"github.com/flarco/g"
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

func (d *DeltaReader) Columns() (Columns, error) {
	if len(d.columns) > 0 {
		return d.columns, nil
	}

	var err error
	d.columns, err = d.Duck.Describe(d.MakeSelectQuery(nil, 0, "", nil))
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	return d.columns, nil
}

func (d *DeltaReader) Close() error {
	return d.Duck.Close()
}

func (d *DeltaReader) MakeSelectQuery(fields []string, limit uint64, incrementalKey string, incrementalValue any) string {
	return d.Duck.MakeScanSelectQuery("delta_scan", d.URI, fields, incrementalKey, incrementalValue, limit)
}
