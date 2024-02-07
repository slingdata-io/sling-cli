package iop

import (
	"io"
	"strings"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/linkedin/goavro/v2"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// Avro is a avro` object
type Avro struct {
	Path   string
	Reader *goavro.OCFReader
	Data   *Dataset
	colMap map[string]int
	codec  *goavro.Codec
}

func NewAvroStream(reader io.ReadSeeker, columns Columns) (a *Avro, err error) {
	ar, err := goavro.NewOCFReader(reader)
	if err != nil {
		err = g.Error(err, "could not read avro reader")
		return
	}

	a = &Avro{Reader: ar, codec: ar.Codec()}
	a.colMap = a.Columns().FieldMap(true)

	return
}

func (a *Avro) Columns() Columns {

	typeMap := map[string]ColumnType{
		"string": StringType,
		"int":    IntegerType,
		"long":   BigIntType,
		"float":  DecimalType,
		"double": DecimalType,
		"bytes":  BinaryType,
		"null":   StringType,
		"array":  JsonType,
		"map":    JsonType,
		"record": JsonType,
		"enum":   StringType,
	}

	type avroField struct {
		Name string `json:"name"`
		Type any    `json:"type"`
	}

	type avroSchema struct {
		Name   string      `json:"name"`
		Fields []avroField `json:"fields"`
	}

	schema := avroSchema{}

	g.Unmarshal(a.codec.Schema(), &schema)

	fields := lo.Map(
		schema.Fields,
		func(f avroField, i int) string { return f.Name },
	)

	cols := NewColumnsFromFields(fields...)
	for i, field := range schema.Fields {
		key := g.Marshal(field.Type)
		key = strings.TrimPrefix(key, `"`)
		key = strings.TrimSuffix(key, `"`)

		if strings.HasPrefix(key, "{") {
			keyI, err := jmespath.Search("type", field.Type)
			if err == nil {
				key = cast.ToString(keyI)
			}
		} else if strings.HasPrefix(key, "[") {
			key = "map"
		}

		cols[i].Type = StringType
		if typ, ok := typeMap[key]; ok {
			cols[i].Type = typ
		}
	}

	return cols
}

func (a *Avro) nextFunc(it *Iterator) bool {
	if !a.Reader.Scan() {
		return false
	}

	if err := a.Reader.Err(); err != nil {
		it.Context.CaptureErr(g.Error(err, "could not read Avro row"))
		return false
	}

	datum, err := a.Reader.Read()
	if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not read Avro record"))
		return false
	}

	buf, err := a.codec.TextualFromNative(nil, datum)
	if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not convert to Avro record"))
		return false
	}

	rec, err := g.JSONUnmarshalToMap(buf)
	if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not unmarshal Avro record"))
		return false
	}

	it.Row = make([]interface{}, len(it.ds.Columns))
	for k, v := range rec {
		col := it.ds.Columns[a.colMap[strings.ToLower(k)]]
		i := col.Position - 1
		if col.Type == JsonType {
			v = g.Marshal(v)
		}
		it.Row[i] = v
	}

	return true
}
