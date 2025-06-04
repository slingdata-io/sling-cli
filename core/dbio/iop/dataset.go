package iop

import (
	"context"
	"fmt"
	"io"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/flarco/g"
	"github.com/flarco/g/csv"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jmoiron/sqlx"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// Dataset is a query returned dataset
type Dataset struct {
	Result        *sqlx.Rows       `json:"-"`
	Columns       Columns          `json:"columns"`
	Rows          [][]any          `json:"rows"`
	SQL           string           `json:"sql"`
	Duration      float64          `json:"duration"`
	Sp            *StreamProcessor `json:"-"`
	Inferred      bool             `json:"inferred"`
	SafeInference bool             `json:"safe_inference"`
	NoDebug       bool             `json:"no_debug"`
}

// NewDataset return a new dataset
func NewDataset(columns Columns) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: NewColumns(columns...),
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	return
}

// NewDatasetFromMap return a new dataset
func NewDatasetFromMap(m map[string]interface{}) (data Dataset) {
	data = Dataset{
		Result:  nil,
		Columns: Columns{},
		Rows:    [][]interface{}{},
		Sp:      NewStreamProcessor(),
	}

	if fieldsI, ok := m["headers"]; ok {
		fields := []string{}
		for _, f := range fieldsI.([]interface{}) {
			fields = append(fields, cast.ToString(f))
		}
		data.Columns = NewColumnsFromFields(fields...)
	}
	if rowsI, ok := m["rows"]; ok {
		for _, rowI := range rowsI.([]interface{}) {
			data.Rows = append(data.Rows, rowI.([]interface{}))
		}
	}

	return
}

// SetColumns sets the columns
func (data *Dataset) AddColumns(newCols Columns, overwrite bool) (added Columns) {
	mergedCols, colsAdded, _ := data.Columns.Merge(newCols, overwrite)
	data.Columns = mergedCols
	added = colsAdded.AddedCols
	return added
}

// Sort sorts by cols
// example: `data.Sort(0, 2, 3, false)` will sort
// col0, col2, col3 descending
// example: `data.Sort(0, 2, true)` will sort
// col0, col2 ascending
func (data *Dataset) Sort(args ...any) {

	colIDs := []int{}

	asc := true
	for _, arg := range args {
		switch val := arg.(type) {
		case int:
			colIDs = append(colIDs, val)
		case bool:
			asc = val
		}
	}

	less := func(i, j int) bool {
		arrA := []string{}
		arrB := []string{}
		var a, b string

		for _, colID := range colIDs {
			colType := data.Columns[colID].Type
			valI := data.Rows[i][colID]
			valJ := data.Rows[j][colID]

			if colType.IsInteger() {
				// zero pad for correct sorting
				a = fmt.Sprintf("%20d", valI)
				b = fmt.Sprintf("%20d", valJ)
			} else if colType.IsDecimal() {
				// zero pad for correct sorting
				a = fmt.Sprintf("%20.9f", valI)
				b = fmt.Sprintf("%20.9f", valJ)
			} else {
				a = data.Sp.CastToString(colID, valI, colType)
				b = data.Sp.CastToString(colID, valJ, colType)
			}

			arrA = append(arrA, a)
			arrB = append(arrB, b)
		}
		if asc {
			return strings.Join(arrA, "-") < strings.Join(arrB, "-")
		}
		return strings.Join(arrA, "-") > strings.Join(arrB, "-")
	}

	sort.SliceStable(data.Rows, less)
}

// Print pretty prints the data with a limit
// 0 is unlimited
func (data *Dataset) Print(limit int) {

	tf := "2006-01-02 15:04:05"
	T := table.NewWriter()
	header := table.Row{}
	for _, val := range data.Columns.Names() {
		header = append(header, val)
	}
	T.AppendHeader(header)

	limited := false
	for j, row := range data.Rows {
		for i, col := range data.Columns {
			sVal := cast.ToString(row[i])
			switch {
			case col.IsDatetime() || (strings.HasPrefix(sVal, "20") && strings.HasSuffix(sVal, "Z")):
				val, err := data.Sp.CastToTime(row[i])
				if err != nil {
					row[i] = sVal
				} else {
					row[i] = val.Format(tf)
				}
			case col.IsNumber():
				row[i] = humanize.Comma(cast.ToInt64(row[i]))
			default:
				row[i] = sVal
			}
		}
		T.AppendRow(row)

		if limit > 0 && j+1 == limit {
			limited = true
			break
		}
	}

	println(T.Render())

	if limited {
		g.Warn("results were limited to %d rows.", limit)
	}
}

func (data *Dataset) PrettyTable(fields ...string) (output string) {
	if len(fields) > 0 {
		fieldMap := data.Columns.FieldMap(true)
		colIndexes := []int{}
		names := []string{}
		for _, field := range fields {
			if i, ok := fieldMap[strings.ToLower(field)]; ok {
				colIndexes = append(colIndexes, i)
				names = append(names, field)
			}
		}

		rows := make([][]any, len(data.Rows))
		for i, row0 := range data.Rows {
			row := make([]any, len(colIndexes))
			for ii, index := range colIndexes {
				row[ii] = row0[index]
			}
			rows[i] = row
		}
		return g.PrettyTable(names, rows)
	}
	return g.PrettyTable(data.Columns.Names(), data.Rows)
}

// WriteCsv writes to a writer
func (data *Dataset) WriteCsv(dest io.Writer) (tbw int, err error) {
	w := csv.NewWriter(dest)
	defer w.Flush()

	tbw, err = w.Write(data.GetFields())
	if err != nil {
		return tbw, g.Error(err, "error write row to csv file")
	}

	for _, row := range data.Rows {
		rec := make([]string, len(row))
		for i, val := range row {
			rec[i] = data.Sp.CastToString(i, val, data.Columns[i].Type)
		}
		bw, err := w.Write(rec)
		if err != nil {
			return tbw, g.Error(err, "error write row to csv file")
		}
		tbw = tbw + bw
	}
	return
}

// GetFields return the fields of the Data
func (data *Dataset) GetFields(lower ...bool) []string {
	Lower := false
	if len(lower) > 0 {
		Lower = lower[0]
	}
	fields := make([]string, len(data.Columns))

	for j, column := range data.Columns {
		if Lower {
			fields[j] = strings.ToLower(column.Name)
		} else {
			fields[j] = column.Name
		}
	}

	return fields
}

// SetFields sets the fields/columns of the Datastream
func (data *Dataset) SetFields(fields []string) {
	if data.Columns == nil || len(data.Columns) != len(fields) {
		data.Columns = make(Columns, len(fields))
	}

	for i, field := range fields {
		data.Columns[i].Name = field
		data.Columns[i].Position = i + 1
		if string(data.Columns[i].Type) == "" {
			data.Columns[i].Type = StringType
		}
	}
}

// Append appends a new row
func (data *Dataset) Append(row ...[]any) {
	data.Rows = append(data.Rows, row...)
}

// Stream returns a datastream of the dataset
func (data *Dataset) Stream(Props ...map[string]string) *Datastream {
	rows := MakeRowsChan()
	nextFunc := func(it *Iterator) bool {
		for it.Row = range rows {
			return true
		}
		return false
	}

	ds := NewDatastreamIt(context.Background(), data.Columns, nextFunc)
	ds.it.IsCasted = true
	ds.Inferred = data.Inferred
	ds.Sp = data.Sp
	ds.Sp.ds = ds
	if len(Props) > 0 {
		ds.SetConfig(Props[0])
	}

	go func() {
		defer close(rows)
		for _, row := range data.Rows {
			rows <- row
		}
	}()

	err := ds.Start()
	if err != nil {
		ds.Context.CaptureErr(err)
	}

	return ds
}

// FirstVal returns the first value from the first row
func (data *Dataset) FirstVal() interface{} {
	if len(data.Rows) > 0 && len(data.Rows[0]) > 0 {
		return data.Rows[0][0]
	}
	return nil
}

// FirstRow returns the first row
func (data *Dataset) FirstRow() []interface{} {
	if len(data.Rows) > 0 {
		return data.Rows[0]
	}
	return nil
}

// ColValues returns the values of a one column as array
func (data *Dataset) ColValues(col int) []interface{} {
	vals := make([]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = row[col]
	}
	return vals
}

// Pick returns a new dataset with specified columns
func (data *Dataset) Pick(colNames ...string) (nData Dataset) {
	nData = NewDataset(Columns{})
	nData.SetFields(colNames)

	for _, rec := range data.Records() {
		row := []any{}
		for _, colName := range colNames {
			row = append(row, rec[strings.ToLower(colName)])
		}
		nData.Append(row)
	}

	return nData
}

// ColValuesStr returns the values of a one column as array or string
func (data *Dataset) ColValuesStr(col int) []string {
	vals := make([]string, len(data.Rows))
	for i, row := range data.Rows {
		vals[i] = data.Sp.CastToString(i, row[col], data.Columns[col].Type)
	}
	return vals

}

// Records return rows of maps
func (data *Dataset) Records(lower ...bool) []map[string]interface{} {
	Lower := true
	if len(lower) > 0 {
		Lower = lower[0]
	}
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields(Lower) {
			rec[field] = row[j]
		}
		records[i] = rec
	}
	return records
}

// RecordsString return rows of maps or string values
func (data *Dataset) RecordsString(lower ...bool) []map[string]string {
	Lower := true
	if len(lower) > 0 {
		Lower = lower[0]
	}
	records := make([]map[string]string, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]string{}
		for j, field := range data.GetFields(Lower) {
			if row[j] == nil {
				rec[field] = ""
			} else {
				rec[field] = cast.ToString(row[j])
			}
		}
		records[i] = rec
	}
	return records
}

// RecordsCasted return rows of maps or casted values
func (data *Dataset) RecordsCasted(lower ...bool) []map[string]interface{} {
	Lower := true
	if len(lower) > 0 {
		Lower = lower[0]
	}
	records := make([]map[string]interface{}, len(data.Rows))
	for i, row := range data.Rows {
		rec := map[string]interface{}{}
		for j, field := range data.GetFields(Lower) {
			if row[j] == nil {
				rec[field] = nil
			} else {
				rec[field] = data.Sp.ParseString(cast.ToString(row[j]), j)
			}
		}
		records[i] = rec
	}
	return records
}

// ToJSONMap converst to a JSON object
func (data *Dataset) ToJSONMap() map[string]interface{} {
	return g.M("headers", data.GetFields(), "rows", data.Rows)
}

// InferColumnTypes determines the columns types
func (data *Dataset) InferColumnTypes() {
	var columns Columns

	if len(data.Rows) == 0 {
		g.Trace("skipping InferColumnTypes [no rows]")
		// For empty datasets, set default types to StringType for all columns
		for i, column := range data.Columns {
			column.Type = StringType
			column.Position = i + 1
			data.Columns[i] = column
		}
		data.Inferred = true
		return
	}

	for i, column := range data.Columns {
		column.Type = lo.Ternary(column.Type == "", StringType, column.Type)
		column.Stats = ColumnStats{
			Min:    math.MaxInt64 - 1000,
			Max:    math.MinInt64 + 1000,
			MinLen: math.MaxInt32,
		}
		column.Position = i + 1
		columns = append(columns, column)
	}

	// g.Trace("InferColumnTypes with sample size %d", SampleSize)
	for i, row := range data.Rows {
		if i >= SampleSize {
			break
		}

		for j, val := range row {
			if j >= len(columns) {
				// Skip if column index is out of range
				continue
			}

			valStr := ""
			if val != nil {
				valStr = cast.ToString(val)
				// Handle maps by marshaling them to JSON
				if reflect.TypeOf(val) != nil && reflect.TypeOf(val).Kind() == reflect.Map {
					valStr = g.Marshal(val)
				}
			}

			columns[j].Stats.TotalCnt++

			// Count empty strings as nulls to better handle CSV data
			if val == nil || valStr == "" || (data.Sp.Config.NullIf != "" && data.Sp.Config.NullIf == valStr) {
				columns[j].Stats.NullCnt++
				continue
			}

			l := len(valStr)
			if l > columns[j].Stats.MaxLen {
				columns[j].Stats.MaxLen = l
			}
			if l < columns[j].Stats.MinLen {
				columns[j].Stats.MinLen = l
			}

		reclassify:
			// Make sure val is not nil before type switch
			if val == nil {
				columns[j].Stats.NullCnt++
				continue
			}

			switch v := val.(type) {
			case time.Time:
				if isDate(&v) {
					columns[j].Stats.DateCnt++
				} else if isUTC(&v) {
					columns[j].Stats.DateTimeCnt++
				} else {
					columns[j].Stats.DateTimeZCnt++
				}
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
				columns[j].Stats.IntCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}
			case float32, float64:
				columns[j].Stats.DecCnt++
				val0 := cast.ToInt64(val)
				if val0 > columns[j].Stats.Max {
					columns[j].Stats.Max = val0
				}
				if val0 < columns[j].Stats.Min {
					columns[j].Stats.Min = val0
				}

				if parts := strings.Split(cast.ToString(val), "."); len(parts) == 2 {
					decLen := len(parts[1])
					if decLen > columns[j].Stats.MaxDecLen {
						columns[j].Stats.MaxDecLen = decLen
					}
				}

			case bool:
				columns[j].Stats.BoolCnt++
			case string, []uint8:
				// For strings, attempt to parse as other types first
				valP := data.Sp.ParseString(strings.TrimSpace(valStr), j)
				if valP != nil {
					if _, ok := valP.(string); !ok {
						val = valP
						goto reclassify
					}

					if looksLikeJson(valStr) {
						var v interface{}
						if err := g.Unmarshal(valStr, &v); err == nil {
							columns[j].Stats.JsonCnt++
						} else {
							columns[j].Stats.StringCnt++
						}
					} else {
						columns[j].Stats.StringCnt++
					}
				} else {
					columns[j].Stats.StringCnt++
				}
			default:
				if reflect.TypeOf(val) != nil && (reflect.TypeOf(val).Kind() == reflect.Slice || reflect.TypeOf(val).Kind() == reflect.Map) {
					columns[j].Stats.JsonCnt++
				} else {
					valP := data.Sp.ParseString(strings.TrimSpace(valStr), j)
					if _, ok := valP.(string); !ok && valP != nil {
						val = valP
						goto reclassify
					} else {
						columns[j].Stats.StringCnt++
					}
				}
			}
		}
	}

	data.Columns = InferFromStats(columns, data.SafeInference, data.NoDebug)

	// overwrite if found in config.columns
	data.Columns = data.Columns.Coerce(
		data.Sp.Config.Columns, data.Sp.Config.Header,
		data.Sp.Config.ColumnCasing, data.Sp.Config.TargetType)

	data.Inferred = true
}

func looksLikeJson(s string) bool {
	return (strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}")) || (strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")) || (strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`))
}
