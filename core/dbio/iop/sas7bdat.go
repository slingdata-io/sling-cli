package iop

import (
	"io"
	"os"
	"strings"

	"github.com/flarco/g"
	"github.com/kshedden/datareader"
	"github.com/spf13/cast"
)

// SAS is a sas7bdat object
type SAS struct {
	Path         string
	Reader       *datareader.SAS7BDAT
	Data         *Dataset
	colMap       map[string]int
	batchSize    int
	seriesCache  []*datareader.Series
	seriesLength int
	dataIndex    int
	eof          bool
}

func NewSASStream(reader io.ReadSeeker, columns Columns) (s *SAS, err error) {
	sr, err := datareader.NewSAS7BDATReader(reader)
	if err != nil {
		err = g.Error(err, "could not read sas reader")
		return
	}

	s = &SAS{
		Reader:      sr,
		batchSize:   10000,
		dataIndex:   -1,
		seriesCache: []*datareader.Series{},
	}

	if bs := cast.ToInt(os.Getenv("SLING_SAS7BDAT_BATCH_SIZE")); bs > 0 {
		s.batchSize = bs
	}

	s.colMap = s.Columns().FieldMap(true)

	return
}

func (s *SAS) Columns() Columns {

	// g.Info(g.Marshal(s.Reader.ColumnLabels()))
	// g.Info(g.Marshal(s.Reader.ColumnNames()))
	// g.Info(g.Marshal(s.Reader.ColumnTypes()))

	typeMap := map[datareader.ColumnTypeT]ColumnType{
		datareader.SASNumericType: FloatType,
		datareader.SASStringType:  StringType,
	}

	names := s.Reader.ColumnNames()
	if cast.ToBool(os.Getenv("SLING_SAS7BDAT_USE_COLUMN_LABELS")) {
		names = s.Reader.ColumnLabels()
	}

	colTypes := s.Reader.ColumnTypes()
	cols := NewColumnsFromFields(names...)
	for i := range s.Reader.ColumnTypes() {
		typeKey := colTypes[i]

		if typ, ok := typeMap[typeKey]; ok {
			cols[i].Type = typ
		}
	}

	// add column descriptions
	for i, label := range s.Reader.ColumnLabels() {
		cols[i].Description = label
	}

	return cols
}

func (s *SAS) pullBatch() (eof bool, err error) {
	if !(s.Data == nil || s.dataIndex >= len(s.Data.Rows)) {
		return s.eof, nil
	}
	series, err := s.Reader.Read(s.batchSize)
	if err != nil {
		if err == io.EOF {
			s.eof = true
			return s.eof, nil
		}
		return false, g.Error(err, "could not read sas batch records")
	}

	s.seriesLength = series[0].Length()
	// g.Info("pulled batch %d => %d", s.batchSize, s.seriesLength)

	// Populate dataset
	data := NewDataset(s.Columns())
	data.Rows = make([][]any, s.seriesLength)

	for colI, colType := range s.Reader.ColumnTypes() {
		var values []any
		var missing []bool
		var valuesF []float64
		var valuesS []string

		if colType == datareader.SASNumericType {
			valuesF, missing, err = series[colI].AsFloat64Slice()
			if err != nil {
				return false, g.Error(err, "could not convert sas batch records to float")
			}
			values = make([]any, len(valuesF))
			for i, val := range valuesF {
				values[i] = val
			}
		} else {
			valuesS, missing, err = series[colI].AsStringSlice()
			if err != nil {
				return false, g.Error(err, "could not convert sas batch records to string")
			}
			values = make([]any, len(valuesS))
			for i, val := range valuesS {
				val = strings.TrimSpace(val)
				if val == "" {
					values[i] = nil
				} else {
					values[i] = val
				}
			}
		}

		for valI, value := range values {
			if len(data.Rows[valI]) == 0 {
				data.Rows[valI] = make([]any, len(data.Columns))
			}
			data.Rows[valI][colI] = value
		}
		for valI, isMissing := range missing {
			if isMissing {
				data.Rows[valI][colI] = nil
			}
		}
	}

	s.Data = &data
	s.dataIndex = 0

	return false, nil
}

func (s *SAS) nextFunc(it *Iterator) bool {

	eof, err := s.pullBatch()
	if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not read sas batch records"))
		return false
	} else if eof {
		return false
	} else if s.dataIndex > len(s.Data.Rows)-1 {
		return false
	}

	it.Row = s.Data.Rows[s.dataIndex]
	s.dataIndex++ // increment dataIndex

	return true
}
