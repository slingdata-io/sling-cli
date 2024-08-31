package iop

import (
	"context"

	"github.com/flarco/g"
)

type ParquetDuckDb struct {
	URI     string
	Duck    *DuckDb
	columns Columns
}

func NewParquetReaderDuckDb(uri string, props ...string) (*ParquetDuckDb, error) {
	duck := NewDuckDb(context.Background(), props...)

	uri = duck.PrepareFsSecretAndURI(uri)

	return &ParquetDuckDb{
		URI:  uri,
		Duck: duck,
	}, nil
}

func (p *ParquetDuckDb) Columns() (Columns, error) {
	if len(p.columns) > 0 {
		return p.columns, nil
	}

	// query := fmt.Sprintf("SELECT path_in_schema as column_name, type as column_type, column_id, num_values, total_uncompressed_size FROM parquet_metadata('%s') order by column_id", p.URI)

	var err error
	p.columns, err = p.Duck.Describe(p.MakeSelectQuery(nil, 0, "", nil))
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	return p.columns, nil
}

func (p *ParquetDuckDb) Close() error {
	return p.Duck.Close()
}

func (p *ParquetDuckDb) MakeSelectQuery(fields []string, limit uint64, incrementalKey string, incrementalValue any) string {
	return p.Duck.MakeScanSelectQuery("parquet_scan", p.URI, fields, incrementalKey, incrementalValue, limit)
}
