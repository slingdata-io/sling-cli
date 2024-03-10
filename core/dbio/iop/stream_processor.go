package iop

import (
	"math"
	"math/big"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// StreamProcessor processes rows and values
type StreamProcessor struct {
	N                 uint64
	dateLayoutCache   string
	stringTypeCache   map[int]string
	colStats          map[int]*ColumnStats
	rowChecksum       []uint64
	unrecognizedDate  string
	warn              bool
	parseFuncs        map[string]func(s string) (interface{}, error)
	decReplRegex      *regexp.Regexp
	ds                *Datastream
	dateLayouts       []string
	config            *streamConfig
	rowBlankValCnt    int
	accentTransformer transform.Transformer
}

type streamConfig struct {
	TrimSpace      bool                       `json:"trim_space"`
	EmptyAsNull    bool                       `json:"empty_as_null"`
	Header         bool                       `json:"header"`
	Compression    string                     `json:"compression"` // AUTO | ZIP | GZIP | SNAPPY | NONE
	NullIf         string                     `json:"null_if"`
	DatetimeFormat string                     `json:"datetime_format"`
	SkipBlankLines bool                       `json:"skip_blank_lines"`
	Delimiter      string                     `json:"delimiter"`
	FileMaxRows    int64                      `json:"file_max_rows"`
	MaxDecimals    int                        `json:"max_decimals"`
	Flatten        bool                       `json:"flatten"`
	FieldsPerRec   int                        `json:"fields_per_rec"`
	Jmespath       string                     `json:"jmespath"`
	BoolAsInt      bool                       `json:"-"`
	Columns        Columns                    `json:"columns"` // list of column types. Can be partial list! likely is!
	transforms     map[string][]TransformFunc // array of transform functions to apply
}

type TransformFunc func(*StreamProcessor, string) (string, error)

// NewStreamProcessor returns a new StreamProcessor
func NewStreamProcessor() *StreamProcessor {
	sp := StreamProcessor{
		stringTypeCache: map[int]string{},
		colStats:        map[int]*ColumnStats{},
		decReplRegex:    regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`),
		config: &streamConfig{
			EmptyAsNull: true,
			MaxDecimals: -1,
			Columns:     Columns{},
			transforms:  map[string][]TransformFunc{},
		},
		accentTransformer: transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC),
	}
	if val := os.Getenv("MAX_DECIMALS"); val != "" && val != "-1" {
		sp.config.MaxDecimals = cast.ToInt(os.Getenv("MAX_DECIMALS"))
	}

	// if val is '0400', '0401'. Such as codes.
	hasZeroPrefix := func(s string) bool { return len(s) >= 2 && s[0] == '0' && s[1] != '.' }

	sp.parseFuncs = map[string]func(s string) (interface{}, error){
		"int": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, g.Error("number has zero prefix, treat as string")
			}
			// return fastfloat.ParseInt64(s)
			return strconv.ParseInt(s, 10, 64)
		},
		"float": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, g.Error("number has zero prefix, treat as string")
			}
			return strconv.ParseFloat(s, 64)
		},
		"time": func(s string) (interface{}, error) {
			return sp.ParseTime(s)
		},
		"bool": func(s string) (interface{}, error) {
			return cast.ToBoolE(s)
		},
	}
	sp.dateLayouts = []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05.000000",
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05.000000Z",
		"02-Jan-06",
		"02-Jan-06 15:04:05",
		"02-Jan-06 03:04:05 PM",
		"02-Jan-06 03.04.05.000000 PM",
		"2006-01-02T15:04:05-0700",
		time.RFC3339,
		"2006-01-02T15:04:05",  // iso8601 without timezone
		"2006-01-02T15:04:05Z", // iso8601 with timezone
		time.RFC1123Z,
		time.RFC1123,
		time.RFC822Z,
		time.RFC822,
		time.RFC850,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		"2006-01-02 15:04:05.999999999 -0700 MST", // Time.String()
		"02 Jan 2006",
		"2006-01-02T15:04:05-0700", // RFC3339 without timezone hh:mm colon
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05Z07:00", // RFC3339 without T
		"2006-01-02 15:04:05Z0700",  // RFC3339 without T or timezone hh:mm colon
		"2006-01-02 15:04:05 MST",
		time.Kitchen,
		time.Stamp,
		time.StampMilli,
		time.StampMicro,
		time.StampNano,
		"1/2/06",
		"01/02/06",
		"1/2/2006",
		"01/02/2006",
		"01/02/2006 15:04",
		"01/02/2006 15:04:05",
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"01/02/2006 03:04:05 PM", // "8/17/1994 12:00:00 AM"
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006/01/02 15:04:05",
	}
	return &sp
}

// SetConfig sets the data.Sp.config values
func (sp *StreamProcessor) SetConfig(configMap map[string]string) {
	if sp == nil {
		sp = NewStreamProcessor()
	}

	if configMap["fields_per_rec"] != "" {
		sp.config.FieldsPerRec = cast.ToInt(configMap["fields_per_rec"])
	}

	if configMap["delimiter"] != "" {
		sp.config.Delimiter = configMap["delimiter"]
	}

	if configMap["file_max_rows"] != "" {
		sp.config.FileMaxRows = cast.ToInt64(configMap["file_max_rows"])
	}

	if configMap["header"] != "" {
		sp.config.Header = cast.ToBool(configMap["header"])
	} else {
		sp.config.Header = true
	}

	if configMap["flatten"] != "" {
		sp.config.Flatten = cast.ToBool(configMap["flatten"])
	}

	if configMap["max_decimals"] != "" && configMap["max_decimals"] != "-1" {
		var err error
		sp.config.MaxDecimals, err = cast.ToIntE(configMap["max_decimals"])
		if err != nil {
			sp.config.MaxDecimals = -1
		}
	}

	if configMap["empty_as_null"] != "" {
		sp.config.EmptyAsNull = cast.ToBool(configMap["empty_as_null"])
	}
	if configMap["null_if"] != "" {
		sp.config.NullIf = configMap["null_if"]
	}
	if configMap["trim_space"] != "" {
		sp.config.TrimSpace = cast.ToBool(configMap["trim_space"])
	}
	if configMap["jmespath"] != "" {
		sp.config.Jmespath = cast.ToString(configMap["jmespath"])
	}
	if configMap["skip_blank_lines"] != "" {
		sp.config.SkipBlankLines = cast.ToBool(configMap["skip_blank_lines"])
	}
	if configMap["bool_at_int"] != "" {
		sp.config.BoolAsInt = cast.ToBool(configMap["bool_at_int"])
	}
	if configMap["columns"] != "" {
		g.Unmarshal(configMap["columns"], &sp.config.Columns)
	}
	if configMap["transforms"] != "" {
		columnTransforms := map[string][]string{}
		g.Unmarshal(configMap["transforms"], &columnTransforms)
		sp.config.transforms = map[string][]TransformFunc{}
		for key, names := range columnTransforms {
			key = strings.ToLower(key)
			sp.config.transforms[key] = []TransformFunc{}
			for _, name := range names {
				f, ok := Transforms[name]
				if ok {
					sp.config.transforms[key] = append(sp.config.transforms[key], f)
				} else {
					g.Warn("did find find tranform named: '%s'", name)
				}
			}
		}
	}
	sp.config.Compression = configMap["compression"]

	if configMap["datetime_format"] != "" {
		sp.config.DatetimeFormat = Iso8601ToGoLayout(configMap["datetime_format"])
		// put in first
		sp.dateLayouts = append(
			[]string{sp.config.DatetimeFormat},
			sp.dateLayouts...)
	}
}

// CastVal  casts the type of an interface based on its value
// From html/template/content.go
// Copyright 2011 The Go Authors. All rights reserved.
// indirect returns the value, after dereferencing as many times
// as necessary to reach the base type (or nil).
func (sp *StreamProcessor) indirect(a interface{}) interface{} {
	if a == nil {
		return nil
	}
	if t := reflect.TypeOf(a); t.Kind() != reflect.Ptr {
		// Avoid creating a reflect.Value if it's not a pointer.
		return a
	}
	v := reflect.ValueOf(a)
	for v.Kind() == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
	}
	return v.Interface()
}

func (sp *StreamProcessor) toFloat64E(i interface{}) (float64, error) {
	i = sp.indirect(i)

	switch s := i.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case string:
		v, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case []uint8:
		v, err := strconv.ParseFloat(string(s), 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case int:
		return float64(s), nil
	case int64:
		return float64(s), nil
	case int32:
		return float64(s), nil
	case int16:
		return float64(s), nil
	case int8:
		return float64(s), nil
	case uint:
		return float64(s), nil
	case uint64:
		return float64(s), nil
	case uint32:
		return float64(s), nil
	case uint16:
		return float64(s), nil
	case uint8:
		return float64(s), nil
	case bool:
		if s {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	}
}

// CastType casts the type of an interface
// CastType is used to cast the interface place holders?
func (sp *StreamProcessor) CastType(val interface{}, typ ColumnType) interface{} {
	var nVal interface{}

	switch {
	case typ.IsString():
		nVal = cast.ToString(val)
	case typ == SmallIntType:
		nVal = cast.ToInt(val)
	case typ.IsInteger():
		nVal = cast.ToInt64(val)
	case typ.IsDecimal():
		// nVal = cast.ToFloat64(val)
		nVal = val
	case typ.IsBool():
		// nVal = cast.ToBool(val)
		nVal = val
	case typ.IsDatetime():
		nVal = cast.ToTime(val)
	default:
		nVal = cast.ToString(val)
	}

	return nVal
}

// GetType returns the type of an interface
func (sp *StreamProcessor) GetType(val interface{}) (typ ColumnType) {
	data := NewDataset(NewColumnsFromFields("col"))
	data.Append([]any{val})
	if ds := sp.ds; ds != nil {
		data.SafeInference = ds.SafeInference
		data.Sp.dateLayouts = ds.Sp.dateLayouts
	}
	data.InferColumnTypes()
	return data.Columns[0].Type
}

// commitChecksum increments the checksum. This is needed due to reprocessing rows
func (sp *StreamProcessor) commitChecksum() {
	for i, val := range sp.rowChecksum {
		cs, ok := sp.colStats[i]
		if !ok {
			sp.colStats[i] = &ColumnStats{}
			cs = sp.colStats[i]
		}
		cs.Checksum = cs.Checksum + val
		sp.ds.Columns[i].Stats.Checksum = cs.Checksum
	}
}

// CastVal casts values with stats collection
// which degrades performance by ~10%
// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessVal'
func (sp *StreamProcessor) CastVal(i int, val interface{}, col *Column) interface{} {
	cs, ok := sp.colStats[i]
	if !ok {
		sp.colStats[i] = &ColumnStats{}
		cs = sp.colStats[i]
	}

	var nVal interface{}
	var sVal string
	isString := false

	if val == nil {
		cs.TotalCnt++
		cs.NullCnt++
		sp.rowBlankValCnt++
		return nil
	}

	switch v := val.(type) {
	case big.Int:
		val = v.Int64()
	case *big.Int:
		val = v.Int64()
	case []uint8:
		sVal = string(v)
		val = sVal
		isString = true
	case string, *string:
		switch v2 := v.(type) {
		case string:
			sVal = v2
		case *string:
			sVal = *v2
		}

		isString = true
		if sp.config.TrimSpace {
			sVal = strings.TrimSpace(sVal)
			val = sVal
		}
		if sVal == "" {
			sp.rowBlankValCnt++
			if sp.config.EmptyAsNull || !sp.ds.Columns[i].IsString() {
				cs.TotalCnt++
				cs.NullCnt++
				return nil
			}
		} else if sp.config.NullIf == sVal {
			cs.TotalCnt++
			cs.NullCnt++
			return nil
		}
	}

	switch {
	case col.Type.IsString():
		if sVal == "" && val != nil {
			if reflect.TypeOf(val).Kind() == reflect.Slice || reflect.TypeOf(val).Kind() == reflect.Map {
				sVal = g.Marshal(val)
			} else {
				sVal = cast.ToString(val)
			}
		}

		// apply transforms
		key := strings.ToLower(col.Name)
		if transforms, ok := sp.config.transforms[key]; ok {
			for _, t := range transforms {
				sVal, _ = t(sp, sVal)
			}
		}
		if transforms, ok := sp.config.transforms["*"]; ok {
			for _, t := range transforms {
				sVal, _ = t(sp, sVal)
			}
		}

		if len(sVal) > cs.MaxLen {
			cs.MaxLen = len(sVal)
		}

		if looksLikeJson(sVal) {
			cs.JsonCnt++
			sp.rowChecksum[i] = uint64(len(strings.ReplaceAll(sVal, " ", "")))
			cs.TotalCnt++
			return sVal
		}

		cond1 := cs.TotalCnt > 0 && cs.NullCnt == cs.TotalCnt
		cond2 := !isString && cs.StringCnt == 0

		if (cond1 || cond2) && sp.ds != nil && !col.Sourced {
			// this is an attempt to cast correctly "uncasted" columns
			// (defaulting at string). This will not work in most db insert cases,
			// as the ds.Shape() function will change it back to the "string" type,
			// to match the target table column type. This takes priority.
			nVal = sp.ParseString(cast.ToString(val))
			sp.ds.ChangeColumn(i, sp.GetType(nVal))
			if !sp.ds.Columns[i].IsString() { // so we don't loop
				return sp.CastVal(i, nVal, &sp.ds.Columns[i])
			}
			cs.StringCnt++
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		} else {
			if col.Type == JsonType {
				sp.ds.ChangeColumn(i, StringType) // change to string, since it's not really json
			}
			cs.StringCnt++
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		}
	case col.Type == SmallIntType:
		iVal, err := cast.ToInt32E(val)
		if err != nil {
			fVal, err := sp.toFloat64E(val)
			if err != nil || sp.ds == nil {
				// is string
				sp.ds.ChangeColumn(i, StringType)
				cs.StringCnt++
				cs.TotalCnt++
				sVal = cast.ToString(val)
				sp.rowChecksum[i] = uint64(len(sVal))
				return sVal
			}
			// is decimal
			sp.ds.ChangeColumn(i, DecimalType)
			return fVal
		}

		if int64(iVal) > cs.Max {
			cs.Max = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			sp.rowChecksum[i] = uint64(-iVal)
		} else {
			sp.rowChecksum[i] = uint64(iVal)
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		nVal = iVal
	case col.Type.IsInteger():
		iVal, err := cast.ToInt64E(val)
		if err != nil {
			fVal, err := sp.toFloat64E(val)
			if err != nil || sp.ds == nil {
				// is string
				sp.ds.ChangeColumn(i, StringType)
				cs.StringCnt++
				cs.TotalCnt++
				sVal = cast.ToString(val)
				sp.rowChecksum[i] = uint64(len(sVal))
				return sVal
			}
			// is decimal
			sp.ds.ChangeColumn(i, DecimalType)
			return fVal
		}

		if iVal > cs.Max {
			cs.Max = iVal
		}
		if int64(iVal) < cs.Min {
			cs.Min = int64(iVal)
		}
		cs.IntCnt++
		if iVal < 0 {
			sp.rowChecksum[i] = uint64(-iVal)
		} else {
			sp.rowChecksum[i] = uint64(iVal)
		}
		nVal = iVal
	case col.Type.IsNumber():
		fVal, err := sp.toFloat64E(val)
		if err == nil && math.IsNaN(fVal) {
			// set as null
			cs.NullCnt++
			return nil
		} else if err != nil {
			// is string
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			cs.TotalCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			return sVal
		}

		intVal := int64(fVal)
		if intVal > cs.Max {
			cs.Max = intVal
		}
		if intVal < cs.Min {
			cs.Min = intVal
		}

		if fVal < 0 {
			sp.rowChecksum[i] = uint64(-fVal)
		} else {
			sp.rowChecksum[i] = uint64(fVal)
		}

		isInt := float64(intVal) == fVal
		if isInt {
			cs.IntCnt++ // is an integer
		} else {
			cs.DecCnt++
		}

		if sp.config.MaxDecimals > -1 && !isInt {
			format := "%." + cast.ToString(sp.config.MaxDecimals) + "f"
			nVal = g.F(format, fVal)
		} else {
			nVal = cast.ToString(val) // use string to keep accuracy
		}

	case col.Type.IsBool():
		var err error
		bVal, err := cast.ToBoolE(val)
		if err != nil {
			// is string
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			cs.TotalCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			return sVal
		} else {
			nVal = strconv.FormatBool(bVal) // keep as string
			sp.rowChecksum[i] = uint64(len(nVal.(string)))
		}

		cs.BoolCnt++
	case col.Type.IsDatetime():
		dVal, err := sp.CastToTime(val)
		if err != nil {
			// sp.unrecognizedDate = g.F(
			// 	"N: %d, ind: %d, val: %s", sp.N, i, cast.ToString(val),
			// )
			// sp.warn = true
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		} else if dVal.IsZero() {
			nVal = nil
			cs.NullCnt++
			sp.rowBlankValCnt++
		} else {
			nVal = dVal
			cs.DateCnt++
			sp.rowChecksum[i] = uint64(dVal.UnixMicro())
		}
	}
	cs.TotalCnt++
	return nVal
}

// CastToString to string. used for csv writing
// slows processing down 5% with upstream CastRow or 35% without upstream CastRow
func (sp *StreamProcessor) CastToString(i int, val interface{}, valType ...ColumnType) string {
	typ := ColumnType("")
	switch v := val.(type) {
	case time.Time:
		typ = DatetimeType
	default:
		_ = v
	}

	if len(valType) > 0 {
		typ = valType[0]
	}

	switch {
	case val == nil:
		return ""
	case sp.config.BoolAsInt && typ.IsBool():
		switch cast.ToString(val) {
		case "true", "1", "TRUE":
			return "1"
		}
		return "0"
	case typ.IsDecimal():
		if RemoveTrailingDecZeros {
			// attempt to remove trailing zeros, but is 10 times slower
			return sp.decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
		} else if sp.config.MaxDecimals > -1 {
			fVal, _ := sp.toFloat64E(val)
			format := "%." + cast.ToString(sp.config.MaxDecimals) + "f"
			val = g.F(format, fVal)
		}
		return cast.ToString(val)
		// return fmt.Sprintf("%v", val)
	case typ.IsDatetime():
		tVal, _ := sp.CastToTime(val)
		if tVal.IsZero() {
			return ""
		} else if sp.config.DatetimeFormat != "" && strings.ToLower(sp.config.DatetimeFormat) != "auto" {
			return tVal.Format(sp.config.DatetimeFormat)
		}
		return tVal.Format("2006-01-02 15:04:05.000000 -07")
	default:
		return cast.ToString(val)
	}
}

// CastValWithoutStats casts the value without counting stats
func (sp *StreamProcessor) CastValWithoutStats(i int, val interface{}, typ ColumnType) interface{} {
	var nVal interface{}
	if nil == val {
		return nil
	}

	switch v := val.(type) {
	case []uint8:
		val = cast.ToString(val)
	default:
		_ = v
	}

	switch typ {
	case "string", "text", "json", "time", "bytes":
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	case "smallint":
		nVal = cast.ToInt(val)
	case "integer", "bigint":
		nVal = cast.ToInt64(val)
	case "decimal", "float":
		// max 9 decimals for bigquery compatibility
		// nVal = math.Round(fVal*1000000000) / 1000000000
		nVal = val // use string to keep accuracy
	case "bool":
		nVal = cast.ToBool(val)
	case "datetime", "date", "timestamp", "timestampz":
		dVal, err := sp.CastToTime(val)
		if err != nil {
			nVal = val // keep string
		} else if dVal.IsZero() {
			nVal = nil
		} else {
			nVal = dVal
		}
	default:
		nVal = cast.ToString(val)
		if nVal == "" {
			nVal = nil
		}
	}

	return nVal
}

// CastToTime converts interface to time
func (sp *StreamProcessor) CastToTime(i interface{}) (t time.Time, err error) {
	i = sp.indirect(i)
	switch v := i.(type) {
	case nil:
		return
	case time.Time:
		return v, nil
	case string:
		return sp.ParseTime(i.(string))
	default:
		return cast.ToTimeE(i)
	}
}

// ParseTime parses a date string and returns time.Time
func (sp *StreamProcessor) ParseTime(i interface{}) (t time.Time, err error) {
	s := cast.ToString(i)
	if s == "" {
		return t, g.Error("blank val")
	}

	// date layouts to try out
	for _, layout := range sp.dateLayouts {
		// use cache to decrease parsing computation next iteration
		if sp.dateLayoutCache != "" {
			t, err = time.Parse(sp.dateLayoutCache, s)
			if err == nil {
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			sp.dateLayoutCache = layout
			return
		}
	}
	return
}

// ParseString return an interface
// string: "varchar"
// integer: "integer"
// decimal: "decimal"
// date: "date"
// datetime: "timestamp"
// timestamp: "timestamp"
// text: "text"
func (sp *StreamProcessor) ParseString(s string, jj ...int) interface{} {
	if s == "" {
		return nil
	}

	j := -1
	if len(jj) > 0 {
		j = jj[0]
	}

	stringTypeCache := sp.stringTypeCache[j]

	if stringTypeCache != "" {
		i, err := sp.parseFuncs[stringTypeCache](s)
		if err == nil {
			return i
		}
	}

	// int
	i, err := sp.parseFuncs["int"](s)
	if err == nil {
		// if s = 0100, casting to int64 will return 64
		// need to mitigate by when s starts with 0
		if len(s) > 1 && s[0] == '0' {
			return s
		}
		sp.stringTypeCache[j] = "int"
		return i
	}

	// float
	f, err := sp.parseFuncs["float"](s)
	if err == nil {
		sp.stringTypeCache[j] = "float"
		return f
	}

	// date/time
	t, err := sp.parseFuncs["time"](s)
	if err == nil {
		sp.stringTypeCache[j] = "time"
		return t
	}

	// boolean
	// FIXME: causes issues in SQLite and Oracle, needed for correct boolean parsing
	b, err := sp.parseFuncs["bool"](s)
	if err == nil {
		sp.stringTypeCache[j] = "bool"
		return b
	}

	return s
}

// ProcessVal processes a value
func (sp *StreamProcessor) ProcessVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case []uint8:
		nVal = cast.ToString(val)
	default:
		nVal = val
		_ = v
	}
	return nVal

}

// ParseVal parses the value into its appropriate type
func (sp *StreamProcessor) ParseVal(val interface{}) interface{} {
	var nVal interface{}
	switch v := val.(type) {
	case time.Time:
		nVal = cast.ToTime(val)
	case nil:
		nVal = val
	case int:
		nVal = cast.ToInt64(val)
	case int8:
		nVal = cast.ToInt64(val)
	case int16:
		nVal = cast.ToInt64(val)
	case int32:
		nVal = cast.ToInt64(val)
	case int64:
		nVal = cast.ToInt64(val)
	case float32:
		nVal = cast.ToFloat32(val)
	case float64:
		nVal = cast.ToFloat64(val)
	case bool:
		nVal = cast.ToBool(val)
	case []uint8:
		nVal = sp.ParseString(cast.ToString(val))
	default:
		nVal = sp.ParseString(cast.ToString(val))
		_ = v
		// fmt.Printf("%T\n", val)
	}
	return nVal
}

// CastRow casts each value of a row
// slows down processing about 40%?
func (sp *StreamProcessor) CastRow(row []interface{}, columns Columns) []interface{} {
	sp.N++
	// Ensure usable types
	sp.rowBlankValCnt = 0
	sp.rowChecksum = make([]uint64, len(row))
	for i, val := range row {
		// fmt.Printf("| (%s) %#v", columns[i].Type, val)
		row[i] = sp.CastVal(i, val, &columns[i])
	}

	for len(row) < len(columns) {
		row = append(row, nil)
	}

	// debug a row, prev
	if sp.warn {
		g.Trace("%s -> %#v", sp.unrecognizedDate, row)
		sp.warn = false
	}

	return row
}

// ProcessRow processes a row
func (sp *StreamProcessor) ProcessRow(row []interface{}) []interface{} {
	// Ensure usable types
	for i, val := range row {
		row[i] = sp.ProcessVal(val)
	}
	return row
}
