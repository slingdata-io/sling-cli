package iop

import (
	// "encoding/csv"
	// "io"

	"os"
	"strings"

	h "github.com/flarco/gutil"
	"github.com/xitongsys/parquet-go-source/writerfile"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
	// "github.com/xitongsys/parquet-go/reader"
)

// Parquet is a parquet object
type Parquet struct {
	Path    string
	Columns []Column
	File    *os.File
	PFile   source.ParquetFile
	Data    *Dataset
}

func getParquetCsvSchema(columns []Column) []string {
	typeMap := map[string]string{
		"bool":    "BOOLEAN",
		"int32":   "INT32",
		"int64":   "INT64",
		"float32": "FLOAT",
		"float64": "DOUBLE",
		"string":  "UTF8",
		"uint32":  "UINT_32",
		"uint64":  "UINT_64",
		// "int32": "DATE",
		// "int32": "TIME_MILLIS",
		// "int64": "TIME_MICROS",
		// "int64": "TIMESTAMP_MILLIS",
		// "int64": "TIMESTAMP_MICROS",
		"slice": "LIST",
		"map":   "MAP",

		"integer":  "INT64",
		"decimal":  "DOUBLE",
		"datetime": "UTF8",
	}
	schema := make([]string, len(columns))
	for i, col := range columns {

		Type := ""
		if _, ok := typeMap[col.Type]; ok {
			Type = typeMap[col.Type]
		} else if Type != "" {
			h.Debug("getParquetCsvSchema - type '%s' not found for '%s'", Type, col.Type)
			Type = "UTF8"
		} else {
			Type = "UTF8"
		}

		colSchema := make([]string, 2)
		colSchema[0] = "name=" + col.Name
		colSchema[1] = "type=" + Type
		schema[i] = strings.Join(colSchema, ", ")
	}
	return schema
}

// WriteStream to Parquet file from datastream
func (p *Parquet) WriteStream(ds *Datastream) error {

	if p.File == nil {
		file, err := os.Create(p.Path)
		if err != nil {
			return err
		}
		p.File = file
	}

	p.PFile = writerfile.NewWriterFile(p.File)

	defer p.File.Close()

	schema := getParquetCsvSchema(ds.Columns)

	pw, err := writer.NewCSVWriter(schema, p.PFile, 4)
	if err != nil {
		return err
	}
	// defer pw.Flush(true)

	for row := range ds.Rows {
		err := pw.Write(row)
		if err != nil {
			return h.Error(err, "error write row to parquet file")
		}
	}

	err = pw.WriteStop()

	return err
}

// func NewFileReader(file io.Reader) (source.ParquetFile, error) {
// 	return &Parquet{}, nil
// }

// ReadStream returns the read Parquet stream into a Datastream
// https://github.com/xitongsys/parquet-go/blob/master/example/read_partial.go
func (p *Parquet) ReadStream() (*Datastream, error) {
	var ds *Datastream

	if p.File == nil {
		file, err := os.Open(p.Path)
		if err != nil {
			return ds, err
		}
		p.File = file
	}

	// reader.NewParquetReader(p.File, interface{}, 4)
	// r := csv.NewReader(c.File)
	// row0, err := r.Read()
	// if err != nil {
	// 	return ds, err
	// } else if err == io.EOF {
	// 	return ds, nil
	// }

	// ds = Datastream{
	// 	Rows:    MakeRowsChan(),
	// 	Columns: c.Columns,
	// }

	// if ds.Columns == nil {
	// 	ds.setFields(row0)
	// }

	// go func() {
	// 	defer c.File.Close()

	// 	count := 1
	// 	for {
	// 		row0, err := r.Read()
	// 		if err == io.EOF {
	// 			break
	// 		} else if err != nil {
	// 			Check(err, "Error reading file")
	// 			break
	// 		}

	// 		count++
	// 		row := make([]interface{}, len(row0))
	// 		for i, val := range row0 {
	// 			row[i] = castVal(val, ds.Columns[i].Type)
	// 		}
	// 		ds.Push(row)

	// 	}
	// 	// Ensure that at the end of the loop we close the channel!
	// 	ds.Close()
	// }()

	return ds, nil
}
