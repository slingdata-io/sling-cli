package iop

import (
	"context"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

type CsvDuckDb struct {
	URI     string
	Duck    *DuckDb
	columns Columns
	sc      *StreamConfig
}

func NewCsvReaderDuckDb(uri string, sc *StreamConfig, props ...string) (*CsvDuckDb, error) {
	duck := NewDuckDb(context.Background(), props...)

	uri = duck.PrepareFsSecretAndURI(uri)

	if sc == nil {
		sc = DefaultStreamConfig()
		sc.Header = true
	}

	if sc.Delimiter == "" {
		sc.Delimiter = ","
	}

	if sc.Escape == "" {
		sc.Escape = `"`
	}

	if sc.Quote == "" {
		sc.Quote = `"`
	}

	if sc.NullIf == "" {
		sc.NullIf = `\N`
	}

	return &CsvDuckDb{
		URI:  uri,
		Duck: duck,
		sc:   sc,
	}, nil
}

func (r *CsvDuckDb) Columns() (Columns, error) {
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

func (r *CsvDuckDb) Close() error {
	return r.Duck.Close()
}

func (r *CsvDuckDb) MakeQuery(fsc FileStreamConfig) string {
	quote := r.Duck.GetProp("quote_char")
	if quote == "" {
		quote = r.sc.Quote
	}

	sql := r.Duck.MakeScanQuery(dbio.FileTypeCsv, r.URI, fsc)

	sql = g.R(sql, "delim", r.sc.Delimiter)
	sql = g.R(sql, "header", cast.ToString(r.sc.Header))
	// sql = g.R(sql, "columns", cfg.Columns)
	sql = g.R(sql, "quote", quote)
	sql = g.R(sql, "escape", r.sc.Escape)
	sql = g.R(sql, "nullstr", r.sc.NullIf)

	return sql
}
