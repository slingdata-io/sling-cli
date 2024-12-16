package iop

import (
	"io"
	"reflect"
	"sort"
	"strings"

	"github.com/flarco/g"
	"github.com/jmespath/go-jmespath"
	"github.com/nqd/flat"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

type decoderLike interface {
	Decode(obj any) error
}

type jsonStream struct {
	ColumnMap     map[string]*Column
	HasMapPayload bool // if we expect a map record

	ds       *Datastream
	sp       *StreamProcessor
	decoder  decoderLike
	jmespath string
	flatten  bool
	buffer   chan []interface{}
}

func NewJSONStream(ds *Datastream, decoder decoderLike, flatten bool, jmespath string) *jsonStream {
	js := &jsonStream{
		ColumnMap: map[string]*Column{},
		ds:        ds,
		decoder:   decoder,
		flatten:   flatten,
		jmespath:  jmespath,
		buffer:    make(chan []interface{}, 100000),
		sp:        NewStreamProcessor(),
	}
	if !flatten {
		col := &Column{Position: 1, Name: "data", Type: JsonType, FileURI: cast.ToString(js.ds.Metadata.StreamURL.Value)}
		js.ColumnMap[col.Name] = col
		js.addColumn(*col)
		js.ds.Inferred = true
	} else {
		// add existing columns
		for _, col := range ds.Columns {
			js.ColumnMap[col.Name] = &col
		}
	}

	return js
}

func (js *jsonStream) NextFunc(it *Iterator) bool {
	var recordsInterf []map[string]interface{}
	var err error
	if it.Closed {
		return false
	}

	select {
	case row := <-js.buffer:
		it.Row = row
		return true
	default:
	}

	var payload interface{}
	if js.HasMapPayload {
		m := g.M()
		err = js.decoder.Decode(&m)
		payload = m
	} else {
		err = js.decoder.Decode(&payload)
	}

	if err == io.EOF {
		return false
	} else if err != nil {
		it.Context.CaptureErr(g.Error(err, "could not decode JSON body"))
		return false
	}

	if js.jmespath != "" {
		payload, err = jmespath.Search(js.jmespath, payload)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "could not search jmespath: %s", js.jmespath))
			return false
		}
	}

	switch payloadV := payload.(type) {
	case map[string]interface{}:
		// is one record
		recordsInterf = js.extractNestedArray(payloadV)
		if len(recordsInterf) == 0 {
			recordsInterf = []map[string]interface{}{payloadV}
		}
	case map[interface{}]interface{}:
		// is one record
		interf := map[string]interface{}{}
		for k, v := range payloadV {
			interf[cast.ToString(k)] = v
		}
		recordsInterf = js.extractNestedArray(interf)
		if len(recordsInterf) == 0 {
			recordsInterf = []map[string]interface{}{interf}
		}
	case []interface{}:
		recordsInterf = []map[string]interface{}{}
		recList := payloadV
		if len(recList) == 0 {
			return js.NextFunc(it)
		}

		switch recList[0].(type) {
		case map[interface{}]interface{}:
			for _, rec := range recList {
				newRec := map[string]interface{}{}
				for k, v := range rec.(map[interface{}]interface{}) {
					newRec[cast.ToString(k)] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case map[string]interface{}:
			for _, val := range recList {
				recordsInterf = append(recordsInterf, val.(map[string]interface{}))
			}
		default:
			// is array of single values
			for _, val := range recList {
				recordsInterf = append(recordsInterf, map[string]interface{}{"data": val})
			}
		}
	case []map[interface{}]interface{}:
		recordsInterf = []map[string]interface{}{}
		for _, rec := range payloadV {
			newRec := map[string]interface{}{}
			for k, v := range rec {
				newRec[cast.ToString(k)] = v
			}
			recordsInterf = append(recordsInterf, newRec)
		}
	case []map[string]interface{}:
		recordsInterf = payloadV
	default:
		err = g.Error("unhandled JSON interface type: %#v", payloadV)
		it.Context.CaptureErr(err)
		return false
	}

	// parse records
	js.parseRecords(recordsInterf)

	if err = it.Context.Err(); err != nil {
		err = g.Error(err, "error parsing records")
		it.Context.CaptureErr(err)
		return false
	}

	select {
	// wait for row
	case row := <-js.buffer:
		it.Row = row
		return true
	}
}

func (js *jsonStream) addColumn(cols ...Column) {
	mux := js.ds.Context.Mux
	if df := js.ds.Df(); df != nil {
		mux = df.Context.Mux
	}

	mux.Lock()
	js.ds.AddColumns(cols, false)
	mux.Unlock()
}

func (js *jsonStream) parseRecords(records []map[string]interface{}) {

	for _, rec := range records {
		if !js.flatten {
			js.buffer <- []interface{}{g.Marshal(rec)}
			continue
		}

		newRec, _ := flat.Flatten(rec, &flat.Options{Delimiter: "__", Safe: true})
		keys := lo.Keys(newRec)
		sort.Strings(keys)

		row := make([]interface{}, len(js.ds.Columns))
		colsToAdd := Columns{}
		for _, colName := range keys {
			// cast arrays as string
			if arr, ok := newRec[colName].([]interface{}); ok {
				newRec[colName] = g.Marshal(arr)
			}

			col, ok := js.ColumnMap[colName]
			if !ok {
				col = &Column{
					Name:     colName,
					Type:     js.ds.Sp.GetType(newRec[colName]),
					Position: len(js.ds.Columns) + len(colsToAdd) + 1,
					FileURI:  cast.ToString(js.ds.Metadata.StreamURL.Value),
				}
				colsToAdd = append(colsToAdd, *col)
				row = append(row, nil)
				js.ColumnMap[col.Name] = col
			}
			i := col.Position - 1
			row[i] = newRec[colName]
		}

		if len(colsToAdd) > 0 {
			js.addColumn(colsToAdd...)
		}

		js.buffer <- row
	}
	// g.Debug("JSON Stream -> Parsed %d records", len(records))
}

func (js *jsonStream) extractNestedArray(rec map[string]interface{}) (recordsInterf []map[string]interface{}) {
	if !js.flatten {
		return []map[string]interface{}{rec}
	}

	recordsInterf = []map[string]interface{}{}
	sliceKeyValLen := map[string]int{}
	maxLen := 0

	for k, v := range rec {
		value := reflect.ValueOf(v)
		if value.Kind() == reflect.Slice || value.Kind() == reflect.Array {
			sliceKeyValLen[k] = value.Len()
			if value.Len() > maxLen {
				maxLen = value.Len()
			}
		}
	}

	keys := lo.Filter(lo.Keys(sliceKeyValLen), func(k string, i int) bool {
		return sliceKeyValLen[k] == maxLen
	})

	var payload interface{}
	for _, key := range keys {
		// have predefined list for now
		switch strings.ToLower(key) {
		case "data", "records", "rows", "result":
			payload = rec[key]
		}
	}

	switch payloadV := payload.(type) {
	case []interface{}:
		recordsInterf = []map[string]interface{}{}
		recList := payloadV
		if len(recList) == 0 {
			return
		}

		switch recList[0].(type) {
		case map[interface{}]interface{}:
			for _, rec := range recList {
				newRec := map[string]interface{}{}
				for k, v := range rec.(map[interface{}]interface{}) {
					newRec[cast.ToString(k)] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case map[string]interface{}:
			for _, val := range recList {
				recordsInterf = append(recordsInterf, val.(map[string]interface{}))
			}
		default:
			// is array of single values
			for _, val := range recList {
				recordsInterf = append(recordsInterf, map[string]interface{}{"data": val})
			}
		}
	case []map[interface{}]interface{}:
		recordsInterf = []map[string]interface{}{}
		for _, rec := range payloadV {
			newRec := map[string]interface{}{}
			for k, v := range rec {
				newRec[cast.ToString(k)] = v
			}
			recordsInterf = append(recordsInterf, newRec)
		}
	case []map[string]interface{}:
		recordsInterf = payloadV
	}

	return recordsInterf
}
