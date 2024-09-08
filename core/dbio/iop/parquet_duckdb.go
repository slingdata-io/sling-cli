package iop

import (
	"context"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
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

func (r *ParquetDuckDb) Columns() (Columns, error) {
	if len(r.columns) > 0 {
		return r.columns, nil
	}

	// query := fmt.Sprintf("SELECT path_in_schema as column_name, type as column_type, column_id, num_values, total_uncompressed_size FROM parquet_metadata('%s') order by column_id", p.URI)

	var err error
	r.columns, err = r.Duck.Describe(r.MakeQuery(FileStreamConfig{}))
	if err != nil {
		return nil, g.Error(err, "could not get columns")
	}
	return r.columns, nil
}

func (r *ParquetDuckDb) Close() error {
	return r.Duck.Close()
}

func (r *ParquetDuckDb) MakeQuery(sc FileStreamConfig) string {
	sql := r.Duck.MakeScanQuery(dbio.FileTypeParquet, r.URI, sc)
	return sql
}
