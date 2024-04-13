package iop

import (
	"strings"

	"github.com/spf13/cast"
)

type spreadsheet struct {
	Props map[string]string
}

func (s *spreadsheet) makeDatasetAuto(rows [][]string) (data Dataset) {
	var blankCellCnt, trailingBlankRows int
	rowWidthDistro := map[int]int{}
	allRows := [][]string{}
	maxCount := 0
	// widthMostUsed := 0
	for _, row0 := range rows {
		blankCellCnt = 0
		row := make([]string, len(row0))
		for i, val := range row0 {
			val = strings.TrimSpace(val)
			if val == "" {
				blankCellCnt++
			} else {
				blankCellCnt = 0
			}
			row[i] = val
		}

		rowWidthDistro[len(row)]++
		allRows = append(allRows, row)

		if blankCellCnt == len(row) {
			trailingBlankRows++
		} else {
			trailingBlankRows = 0
		}

		if rowWidthDistro[len(row)] > maxCount {
			maxCount = rowWidthDistro[len(row)]
		}
	}

	// g.Debug("trailingBlankRows: %d", trailingBlankRows)
	data = NewDataset(nil)
	data.Sp.SetConfig(s.Props)
	hasHeader := cast.ToBool(s.Props["header"])

	for i, row0 := range allRows[:len(allRows)-trailingBlankRows] {
		if i == 0 {
			if hasHeader {
				// assume first row is header row
				row0 = CleanHeaderRow(row0)
				data.SetFields(row0)
				continue
			} else if len(data.Columns) == 0 {
				data.SetFields(CreateDummyFields(len(row0)))
			}
		}

		row := make([]interface{}, len(row0))
		for i, val := range row0 {
			row[i] = val
		}
		row = data.Sp.CastRow(row, data.Columns)
		data.Rows = append(data.Rows, row)

		if i == SampleSize {
			data.InferColumnTypes()
			for i, row := range data.Rows {
				data.Rows[i] = data.Sp.CastRow(row, data.Columns)
			}
		}
	}
	if !data.Inferred {
		data.InferColumnTypes()
		for i, row := range data.Rows {
			data.Rows[i] = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}
func (s *spreadsheet) makeDatasetStr(rangeRows [][]string) (data Dataset) {
	data = NewDataset(nil)
	data.Sp.SetConfig(s.Props)
	for i, row0 := range rangeRows {
		if i == 0 {
			// assume first row is header row
			data.SetFields(CleanHeaderRow(row0))
			continue
		}

		row := make([]interface{}, len(row0))
		for i, val := range row0 {
			row[i] = val
		}
		data.Append(row)

		if i == SampleSize {
			data.InferColumnTypes()
			for i, row := range data.Rows {
				data.Rows[i] = data.Sp.CastRow(row, data.Columns)
			}
		}
	}

	if !data.Inferred {
		data.InferColumnTypes()
		for i, row := range data.Rows {
			data.Rows[i] = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}

func (s *spreadsheet) makeDatasetInterf(rangeRows [][]interface{}) (data Dataset) {
	data = NewDataset(nil)
	data.Sp.SetConfig(s.Props)
	for i, row := range rangeRows {
		if i == 0 {
			// assume first row is header row
			row0 := make([]string, len(row))
			for i, val := range row {
				row0[i] = cast.ToString(val)
			}
			data.SetFields(CleanHeaderRow(row0))
			continue
		}

		data.Append(row)

		if i == SampleSize {
			data.InferColumnTypes()
			for i, row := range data.Rows {
				data.Rows[i] = data.Sp.CastRow(row, data.Columns)
			}
		}
	}

	if !data.Inferred {
		data.InferColumnTypes()
		for i, row := range data.Rows {
			data.Rows[i] = data.Sp.CastRow(row, data.Columns)
		}
	}
	return
}
