package iop

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/flarco/g"
	"github.com/shopspring/decimal"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
	"golang.org/x/text/encoding/charmap"
	encUnicode "golang.org/x/text/encoding/unicode"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// StreamProcessor processes rows and values
type StreamProcessor struct {
	N                uint64
	dateLayoutCache  string
	stringTypeCache  map[int]string
	colStats         map[int]*ColumnStats
	rowChecksum      []uint64
	unrecognizedDate string
	warn             bool
	skipCurrent      bool // whether to skip current row (for constraints)
	parseFuncs       map[string]func(s string) (interface{}, error)
	decReplRegex     *regexp.Regexp
	ds               *Datastream
	dateLayouts      []string
	Config           StreamConfig
	rowBlankValCnt   int
	transformers     Transformers
	digitString      map[int]string
}

type StreamConfig struct {
	EmptyAsNull       bool                     `json:"empty_as_null"`
	Header            bool                     `json:"header"`
	Compression       CompressorType           `json:"compression"` // AUTO | ZIP | GZIP | SNAPPY | NONE
	NullIf            string                   `json:"null_if"`
	NullAs            string                   `json:"null_as"`
	DatetimeFormat    string                   `json:"datetime_format"`
	SkipBlankLines    bool                     `json:"skip_blank_lines"`
	Format            dbio.FileType            `json:"format"`
	Delimiter         string                   `json:"delimiter"`
	Escape            string                   `json:"escape"`
	Quote             string                   `json:"quote"`
	FileMaxRows       int64                    `json:"file_max_rows"`
	FileMaxBytes      int64                    `json:"file_max_bytes"`
	BatchLimit        int64                    `json:"batch_limit"`
	MaxDecimals       int                      `json:"max_decimals"`
	Flatten           int                      `json:"flatten"`
	FieldsPerRec      int                      `json:"fields_per_rec"`
	Jmespath          string                   `json:"jmespath"`
	Sheet             string                   `json:"sheet"`
	ColumnCasing      ColumnCasing             `json:"column_casing"`
	TargetType        dbio.Type                `json:"target_type"`
	BoolAsInt         bool                     `json:"-"`
	Columns           Columns                  `json:"columns"` // list of column types. Can be partial list! likely is!
	transforms        map[string]TransformList // array of transform functions to apply
	maxDecimalsFormat string                   `json:"-"`

	Map map[string]string `json:"-"`
}

func (sc *StreamConfig) ToMap() map[string]string {
	m := g.M()
	g.Unmarshal(g.Marshal(sc), &m)
	return g.ToMapString(m)
}

type Transformers struct {
	Accent transform.Transformer

	DecodeUTF8        transform.Transformer
	DecodeUTF8BOM     transform.Transformer
	DecodeUTF16       transform.Transformer
	DecodeISO8859_1   transform.Transformer
	DecodeISO8859_5   transform.Transformer
	DecodeISO8859_15  transform.Transformer
	DecodeWindows1250 transform.Transformer
	DecodeWindows1252 transform.Transformer

	EncodeUTF8        transform.Transformer
	EncodeUTF8BOM     transform.Transformer
	EncodeUTF16       transform.Transformer
	EncodeISO8859_1   transform.Transformer
	EncodeISO8859_5   transform.Transformer
	EncodeISO8859_15  transform.Transformer
	EncodeWindows1250 transform.Transformer
	EncodeWindows1252 transform.Transformer
}

func NewTransformers() Transformers {
	win16be := encUnicode.UTF16(encUnicode.BigEndian, encUnicode.IgnoreBOM)
	return Transformers{
		Accent: transform.Chain(
			norm.NFD,
			runes.Remove(runes.In(unicode.Mn)),
			runes.Map(func(r rune) rune {
				switch r {
				// Polish special characters
				case 'Ł', 'Ɫ':
					return 'L'
				case 'ł':
					return 'l'
				// Other special characters and their variations
				case 'Æ', 'Ǽ':
					return 'A'
				case 'æ', 'ǽ':
					return 'a'
				case 'Ø', 'Ǿ':
					return 'O'
				case 'ø', 'ǿ':
					return 'o'
				case 'Þ':
					return 'T'
				case 'þ':
					return 't'
				case 'Ð':
					return 'D'
				case 'ð':
					return 'd'
				case 'ß', 'ẞ':
					return 's'
				case 'Œ':
					return 'O'
				case 'œ':
					return 'o'
				case 'Ĳ':
					return 'I'
				case 'ĳ':
					return 'i'
				case 'ƒ':
					return 'f'
				case 'Ŋ':
					return 'N'
				case 'ŋ':
					return 'n'
				case 'Ɲ':
					return 'N'
				case 'ɲ':
					return 'n'
				case 'Ƴ':
					return 'Y'
				case 'ƴ':
					return 'y'
				case 'Ɣ':
					return 'G'
				case 'ɣ':
					return 'g'
				default:
					return r
				}
			}),
			norm.NFC,
		),

		DecodeUTF8:        encUnicode.UTF8.NewDecoder(),
		DecodeUTF8BOM:     encUnicode.UTF8BOM.NewDecoder(),
		DecodeUTF16:       encUnicode.BOMOverride(win16be.NewDecoder()),
		DecodeISO8859_1:   charmap.ISO8859_1.NewDecoder(),
		DecodeISO8859_5:   charmap.ISO8859_5.NewDecoder(),
		DecodeISO8859_15:  charmap.ISO8859_15.NewDecoder(),
		DecodeWindows1250: charmap.Windows1250.NewDecoder(),
		DecodeWindows1252: charmap.Windows1252.NewDecoder(),

		EncodeUTF8:        encUnicode.UTF8.NewEncoder(),
		EncodeUTF8BOM:     encUnicode.UTF8BOM.NewEncoder(),
		EncodeUTF16:       encUnicode.BOMOverride(win16be.NewEncoder()),
		EncodeISO8859_1:   charmap.ISO8859_1.NewEncoder(),
		EncodeISO8859_5:   charmap.ISO8859_5.NewEncoder(),
		EncodeISO8859_15:  charmap.ISO8859_15.NewEncoder(),
		EncodeWindows1250: charmap.Windows1250.NewEncoder(),
		EncodeWindows1252: charmap.Windows1252.NewEncoder(),
	}
}

// NewStreamProcessor returns a new StreamProcessor
func NewStreamProcessor() *StreamProcessor {
	sp := StreamProcessor{
		stringTypeCache: map[int]string{},
		colStats:        map[int]*ColumnStats{},
		decReplRegex:    regexp.MustCompile(`^(\d*[\d.]*?)\.?0*$`),
		transformers:    NewTransformers(),
		digitString:     map[int]string{0: "0"},
	}

	sp.ResetConfig()
	if val := os.Getenv("MAX_DECIMALS"); val != "" && val != "-1" {
		sp.Config.MaxDecimals = cast.ToInt(os.Getenv("MAX_DECIMALS"))
		if sp.Config.MaxDecimals > -1 {
			sp.Config.maxDecimalsFormat = "%." + cast.ToString(sp.Config.MaxDecimals) + "f"
		}
	}

	// if val is '0400', '0401'. Such as codes.
	hasZeroPrefix := func(s string) bool { return len(s) >= 2 && s[0] == '0' && s[1] != '.' }

	sp.parseFuncs = map[string]func(s string) (interface{}, error){
		"int": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, errors.New("number has zero prefix, treat as string")
			}
			// return fastfloat.ParseInt64(s)
			return strconv.ParseInt(s, 10, 64)
		},
		"float": func(s string) (interface{}, error) {
			if hasZeroPrefix(s) {
				return s, errors.New("number has zero prefix, treat as string")
			}
			return strconv.ParseFloat(strings.Replace(s, ",", ".", 1), 64)
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
		"2006-01-02 15:04:05.999",
		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05.999999Z",
		"2006-01-02 15:04:05.999 Z",    // snowflake export format
		"2006-01-02 15:04:05.999999 Z", // snowflake export format
		"02-Jan-06",
		"02-Jan-06 15:04:05",
		"02-Jan-06 03:04:05 PM",
		"02-Jan-06 03.04.05.999999 PM",
		"2006-01-02T15:04:05-0700",
		"2006-01-02 15:04:05-07",        // duckdb
		"2006-01-02 15:04:05.999-07",    // duckdb
		"2006-01-02 15:04:05.999999-07", // duckdb
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
		"02-01-2006",
		"02-01-2006 15:04:05",
		"Mon, 02 Jan 2006 15:04:05 -0700",
	}

	// up to 90 digits. This is done for CastToStringSafeMask
	// shopspring/decimal is buggy and can segfault. Using val.NumDigit,
	// we can create a approximate value mask to output the correct number of bytes
	digitString := "0"
	for i := 1; i <= 90; i++ {
		sp.digitString[i] = digitString
		digitString = digitString + "0"
	}
	return &sp
}

func DefaultStreamConfig() StreamConfig {
	return StreamConfig{
		MaxDecimals: -1,
		transforms:  nil,
		Map:         map[string]string{"delimiter": "-1"},
	}
}

func LoaderStreamConfig(header bool) StreamConfig {
	return StreamConfig{
		Header:         header,
		Delimiter:      ",",
		Escape:         `"`,
		Quote:          `"`,
		NullAs:         `\N`,
		DatetimeFormat: "auto",
		MaxDecimals:    -1,
		transforms:     nil,
	}
}

func (sp *StreamProcessor) ColStats() map[int]*ColumnStats {
	return sp.colStats
}

func (sp *StreamProcessor) ResetConfig() {
	sp.Config = DefaultStreamConfig()
}

// SetConfig sets the data.Sp.config values
func (sp *StreamProcessor) SetConfig(configMap map[string]string) {
	if sp == nil {
		sp = NewStreamProcessor()
	}

	sp.Config.Map = configMap

	if val, ok := configMap["fields_per_rec"]; ok {
		sp.Config.FieldsPerRec = cast.ToInt(val)
	}

	if val, ok := configMap["delimiter"]; ok {
		sp.Config.Delimiter = val
	}

	if val, ok := configMap["escape"]; ok {
		sp.Config.Escape = val
	}

	if val, ok := configMap["quote"]; ok {
		sp.Config.Quote = val
	}

	if val, ok := configMap["file_max_rows"]; ok {
		sp.Config.FileMaxRows = cast.ToInt64(val)
	}

	if val, ok := configMap["file_max_bytes"]; ok {
		sp.Config.FileMaxBytes = cast.ToInt64(val)
	}

	if val, ok := configMap["batch_limit"]; ok {
		sp.Config.BatchLimit = cast.ToInt64(val)
	}

	if val, ok := configMap["header"]; ok {
		sp.Config.Header = cast.ToBool(val)
	} else {
		sp.Config.Header = true
	}

	if val, ok := configMap["flatten"]; ok {
		sp.Config.Flatten = cast.ToInt(val)
	}

	if configMap["max_decimals"] != "" && configMap["max_decimals"] != "-1" {
		var err error
		sp.Config.MaxDecimals, err = cast.ToIntE(configMap["max_decimals"])
		if err != nil {
			sp.Config.MaxDecimals = -1
		} else if sp.Config.MaxDecimals > -1 {
			sp.Config.maxDecimalsFormat = "%." + cast.ToString(sp.Config.MaxDecimals) + "f"
		}
	}

	if val, ok := configMap["empty_as_null"]; ok {
		sp.Config.EmptyAsNull = cast.ToBool(val)
	}

	if val, ok := configMap["null_if"]; ok {
		sp.Config.NullIf = val
	}

	if val, ok := configMap["format"]; ok {
		sp.Config.Format = dbio.FileType(val)
	}

	if val, ok := configMap["null_as"]; ok {
		sp.Config.NullAs = val
	}

	if val, ok := configMap["jmespath"]; ok {
		sp.Config.Jmespath = cast.ToString(val)
	}

	if val, ok := configMap["sheet"]; ok {
		sp.Config.Sheet = cast.ToString(val)
	}

	if val, ok := configMap["skip_blank_lines"]; ok {
		sp.Config.SkipBlankLines = cast.ToBool(val)
	}

	if val, ok := configMap["column_casing"]; ok {
		sp.Config.ColumnCasing = ColumnCasing(val)
	}

	if val, ok := configMap["target_type"]; ok {
		sp.Config.TargetType = dbio.Type(val)
	}

	if val, ok := configMap["bool_at_int"]; ok {
		sp.Config.BoolAsInt = cast.ToBool(val)
	}

	if val, ok := configMap["columns"]; ok {
		g.Unmarshal(val, &sp.Config.Columns)
	}

	if val, ok := configMap["transforms"]; ok {
		sp.applyTransforms(val)
	}

	if val, ok := configMap["compression"]; ok {
		sp.Config.Compression = CompressorType(strings.ToLower(val))
	}

	if val, ok := configMap["datetime_format"]; ok {
		sp.Config.DatetimeFormat = Iso8601ToGoLayout(val)
		// put in first
		sp.dateLayouts = append(
			[]string{sp.Config.DatetimeFormat},
			sp.dateLayouts...)
	}
}

func makeColumnTransforms(transformsPayload string) map[string][]string {
	columnTransforms := map[string][]string{}
	g.Unmarshal(transformsPayload, &columnTransforms)
	return columnTransforms
}

func (sp *StreamProcessor) applyTransforms(transformsPayload string) {
	columnTransforms := makeColumnTransforms(transformsPayload)
	sp.Config.transforms = map[string]TransformList{}
	for key, names := range columnTransforms {
		key = strings.ToLower(key)
		sp.Config.transforms[key] = TransformList{}
		for _, name := range names {
			t, ok := TransformsMap[name]
			if ok {
				sp.Config.transforms[key] = append(sp.Config.transforms[key], t)
			} else if n := strings.TrimSpace(string(name)); strings.Contains(n, "(") && strings.HasSuffix(n, ")") {
				// parse transform with a parameter
				parts := strings.Split(string(name), "(")
				if len(parts) != 2 {
					g.Warn("invalid transform: '%s'", name)
					continue
				}
				tName := parts[0]
				param := strings.TrimSuffix(parts[1], ")")
				if t, ok := TransformsMap[tName]; ok {
					if t.makeFunc == nil {
						g.Warn("makeFunc not found for transform '%s'. Please contact support", tName)
						continue
					}
					var params []any
					for _, p := range strings.Split(param, ",") {
						params = append(params, strings.TrimSpace(p))
					}
					err := t.makeFunc(&t, params...)
					if err != nil {
						g.Warn("invalid parameter for transform '%s' (%s)", tName, err.Error())
					} else {
						sp.Config.transforms[key] = append(sp.Config.transforms[key], t)
					}
				} else {
					g.Warn("did find find transform with params named: '%s'", tName)
				}
			} else {
				g.Warn("did find find transform named: '%s'", name)
			}
		}
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
		v, err := strconv.ParseFloat(strings.Replace(s, ",", ".", 1), 64)
		if err == nil {
			return v, nil
		}
		return 0, g.Error("unable to cast %#v of type %T to float64", i, i)
	case []uint8:
		v, err := strconv.ParseFloat(strings.Replace(string(s), ",", ".", 1), 64)
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
	case typ.IsBinary():
		nVal = []byte(cast.ToString(val))
	case typ.IsString():
		nVal = cast.ToString(val)
	case typ == SmallIntType:
		nVal = cast.ToInt(val)
	case typ.IsInteger():
		nVal = cast.ToInt64(val)
	case typ.IsFloat():
		nVal = val
	case typ.IsDecimal():
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

// Clickhouse JSON data
type chJSON interface {
	MarshalJSON() ([]byte, error)
}

// CastVal casts values with stats collection
// which degrades performance by ~10%
// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessVal'
func (sp *StreamProcessor) CastVal(i int, val interface{}, col *Column) interface{} {
	cs, ok := sp.colStats[i]
	if !ok {
		sp.colStats[i] = &ColumnStats{}
		cs = sp.colStats[i]
		if len(sp.rowChecksum) <= i {
			sp.rowChecksum = append(sp.rowChecksum, 0)
		}
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

	colKey := strings.ToLower(col.Name)

	switch v := val.(type) {
	case big.Int:
		val = v.Int64()
	case *big.Int:
		val = v.Int64()
	case []uint8:
		sVal = string(v)
		val = sVal
		isString = true
	case chJSON: // Clickhouse JSON / Variant
		sBytes, _ := v.MarshalJSON()
		sVal = string(sBytes)
	case string, *string:
		switch v2 := v.(type) {
		case string:
			sVal = v2
		case *string:
			sVal = *v2
		}

		isString = true
		if !col.IsString() {
			// if colType is not string, and the value is string, we should trim it
			// in case it comes from a CSV. If it's empty, it should be considered nil
			sVal = strings.TrimSpace(sVal)
			val = sVal
		}
		if sVal == "" {
			sp.rowBlankValCnt++
			if sp.Config.EmptyAsNull || !col.IsString() || sp.Config.transforms[colKey].HasTransform(TransformEmptyAsNull) {
				cs.TotalCnt++
				cs.NullCnt++
				return nil
			}
		} else if sp.Config.NullIf != "" && sp.Config.NullIf == sVal {
			cs.TotalCnt++
			cs.NullCnt++
			return nil
		}
	}

	// get transforms
	transforms := append(sp.Config.transforms[colKey], sp.Config.transforms["*"]...)

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
		for _, t := range transforms {
			if t.FuncString != nil {
				sVal, _ = t.FuncString(sp, sVal)
			}
		}

		l := len(sVal)
		if l > cs.MaxLen {
			cs.MaxLen = l
		} else if l == 0 {
			cs.TotalCnt++
			if col.Type == JsonType {
				cs.NullCnt++
				return nil // if json, empty should be null
			}
			return sVal
		}

		if looksLikeJson(sVal) {
			cs.JsonCnt++
			sp.rowChecksum[i] = uint64(len(strings.ReplaceAll(sVal, " ", "")))
			cs.TotalCnt++
			return sVal
		}

		// above 4000 is considered text
		if l > 4000 && !g.In(col.Type, TextType, BinaryType) {
			sp.ds.ChangeColumn(i, TextType) // change to text
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
			if col.Type == JsonType && !col.Sourced {
				sp.ds.ChangeColumn(i, StringType) // change to string, since it's not really json
			}
			cs.StringCnt++
			sp.rowChecksum[i] = uint64(len(sVal))
			if col.Type == BinaryType {
				nVal = []byte(sVal)
			} else if col.Type == JsonType && sVal == "" {
				cs.NullCnt++
				nVal = nil // if json, empty should be null
			} else {
				nVal = sVal
			}
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
			// if value is boolean casted as int
			switch val {
			case "true", true:
				iVal = 1
			case "false", false:
				iVal = 0
			default:
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
	case col.Type == FloatType:
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

		cs.DecCnt++
		if fVal < 0 {
			sp.rowChecksum[i] = uint64(-fVal)
		} else {
			sp.rowChecksum[i] = uint64(fVal)
		}
		nVal = fVal

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

		if sp.Config.MaxDecimals > -1 && !isInt {
			nVal = g.F(sp.Config.maxDecimalsFormat, fVal)
		} else {
			nVal = strings.Replace(cast.ToString(val), ",", ".", 1) // use string to keep accuracy, replace comma as decimal point
		}

	case col.Type.IsBool():
		var err error
		bVal, err := sp.CastToBool(val)
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
	case col.Type.IsDatetime() || col.Type.IsDate():
		dVal, err := sp.CastToTime(val)
		if err != nil && !g.In(val, "0000-00-00", "0000-00-00 00:00:00") {
			sp.ds.ChangeColumn(i, StringType)
			cs.StringCnt++
			sVal = cast.ToString(val)
			sp.rowChecksum[i] = uint64(len(sVal))
			nVal = sVal
		} else if g.In(val, "0000-00-00", "0000-00-00 00:00:00") {
			nVal = nil
			cs.NullCnt++
			sp.rowBlankValCnt++
		} else {
			// apply transforms
			for _, t := range transforms {
				if t.FuncTime != nil {
					_ = t.FuncTime(sp, &dVal)
				}

				// column needs to be set to timestampz
				if t.Name == "set_timezone" && col.Type != TimestampzType {
					sp.ds.ChangeColumn(i, TimestampzType)
				}
			}
			nVal = dVal
			if isDate(&dVal) {
				cs.DateCnt++
			} else if isUTC(&dVal) {
				cs.DateTimeCnt++
			} else {
				cs.DateTimeZCnt++
			}
			em := dVal.UnixMicro()
			sp.rowChecksum[i] = uint64(em)
			if em > cs.Max {
				cs.Max = em
			}
			if em < cs.Min {
				cs.Min = em
			}
		}
	}
	cs.TotalCnt++
	return nVal
}

// Pre-computed hex digits to avoid runtime computation
var hexDigits = []byte("0123456789abcdef")

func (sp *StreamProcessor) bytesToHexEscape(b []byte) string {
	// Each byte becomes \xXX (4 chars)
	result := make([]byte, len(b)*4)

	for i, v := range b {
		pos := i * 4
		result[pos] = '\\'
		result[pos+1] = 'x'
		result[pos+2] = hexDigits[v>>4]
		result[pos+3] = hexDigits[v&0x0f]
	}

	return string(result)
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
		return sp.Config.NullAs
	case sp.Config.BoolAsInt && typ.IsBool():
		switch cast.ToString(val) {
		case "true", "1", "TRUE":
			return "1"
		}
		return "0"
	case typ.IsDecimal() || typ.IsFloat():
		if RemoveTrailingDecZeros {
			// attempt to remove trailing zeros, but is 10 times slower
			return sp.decReplRegex.ReplaceAllString(cast.ToString(val), "$1")
		}
		return cast.ToString(val)
		// return fmt.Sprintf("%v", val)
	case typ.IsDate():
		tVal, err := sp.CastToTime(val)
		if err != nil {
			return cast.ToString(val)
		}
		return tVal.Format("2006-01-02")
	case typ.IsDatetime():
		tVal, err := sp.CastToTime(val)
		if err != nil {
			return cast.ToString(val)
		} else if sp.Config.DatetimeFormat != "" && strings.ToLower(sp.Config.DatetimeFormat) != "auto" {
			return tVal.Format(sp.Config.DatetimeFormat)
		} else if tVal.Location() == nil {
			return tVal.Format("2006-01-02 15:04:05.999999999") + " +00"
		}
		return tVal.Format("2006-01-02 15:04:05.999999999 -07")
	case typ.IsBinary() && g.In(sp.Config.TargetType, dbio.TypeDbSnowflake, dbio.TypeDbBigQuery):
		return Transforms.BinaryToHex(cast.ToString(val))
	default:
		strVal := cast.ToString(val)
		if !utf8.ValidString(strVal) {
			// Replace invalid chars with Unicode replacement character
			return strings.ToValidUTF8(strVal, "�")
			// if not valid utf8, return hex
			return sp.bytesToHexEscape([]byte(strVal))
		}
		return strVal
	}
}

// CastToStringSafe to masks to count bytes (even safer)
func (sp *StreamProcessor) CastToStringSafeMask(i int, val interface{}, valType ...ColumnType) string {
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
	case sp.Config.BoolAsInt && typ.IsBool():
		return "0" // as a mask
	case typ.IsBool():
		return cast.ToString(val)
	case typ.IsDecimal() || typ.IsFloat():
		if valD, ok := val.(decimal.Decimal); ok {
			// shopspring/decimal is buggy and can segfault. Using val.NumDigit,
			// we can create a approximate value mask to output the correct number of bytes
			// return sp.digitString[valD.NumDigits()]
			_ = valD          // to avoid segfault
			return "000.0000" // assume 8 bytes for each decimal
		}
		return cast.ToString(val)
	case typ.IsDate():
		return "2006-01-02" // as a mask
	case typ.IsDatetime():
		return "2006-01-02 15:04:05.999999 +00" // as a mask
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

// CastToBool converts interface to bool
func (sp *StreamProcessor) CastToBool(i interface{}) (b bool, err error) {
	i = sp.indirect(i)

	switch b := i.(type) {
	case bool:
		return b, nil
	case nil:
		return false, nil
	case int64, int32, int16, int8, uint, uint64, uint32, uint16, uint8, float64, float32:
		return cast.ToInt(i) != 0, nil
	case string:
		return strconv.ParseBool(i.(string))
	case json.Number:
		v, err := cast.ToInt64E(b)
		if err == nil {
			return v != 0, nil
		}
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", i, i)
	default:
		return false, fmt.Errorf("unable to cast %#v of type %T to bool", i, i)
	}
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
		return t, nil // return zero time, so it become nil
	}

	// date layouts to try out
	for _, layout := range sp.dateLayouts {
		// use cache to decrease parsing computation next iteration
		if sp.dateLayoutCache != "" {
			t, err = time.Parse(sp.dateLayoutCache, s)
			if err == nil {
				if isDate(&t) {
					t = t.UTC() // convert to utc for dates
				}
				return
			}
		}
		t, err = time.Parse(layout, s)
		if err == nil {
			sp.dateLayoutCache = layout
			if isDate(&t) {
				t = t.UTC() // convert to utc for dates
			}
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
		col := &columns[i]
		row[i] = sp.CastVal(i, val, col)
		if row[i] != nil && row[i] != "" {
			sp.colStats[i].LastVal = row[i]
		}

		// evaluate constraint
		if col.Constraint != nil {
			if err := col.EvaluateConstraint(row[i], sp); err != nil {
				switch os.Getenv("SLING_ON_CONSTRAINT_FAILURE") {
				case "abort":
					sp.ds.Context.CaptureErr(err)
				case "skip":
					sp.skipCurrent = true
				}
			}
		}
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
