package iop

import (
	"bytes"
	"encoding/base64"
	"io"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/flarco/g/json"
	"github.com/itchyny/gojq"
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
	jq       string
	flatten  int
	buffer   chan []any
	selector *Selector

	// post-select column order; seeded from raw bytes by NextFunc or from
	// an external producer via SetOrderedKeys (1-buffered, first-write-wins).
	OrderedKeys   []string
	orderedKeysCh chan []string
}

func NewJSONStream(ds *Datastream, decoder decoderLike, flatten int, jmespath string, jq string) *jsonStream {
	js := &jsonStream{
		ColumnMap:     map[string]*Column{},
		ds:            ds,
		decoder:       decoder,
		flatten:       flatten,
		jmespath:      jmespath,
		jq:            jq,
		buffer:        make(chan []any, 100000),
		sp:            NewStreamProcessor(),
		orderedKeysCh: make(chan []string, 1),
	}

	if ds != nil && ds.Sp != nil && flatten >= 0 && len(ds.Sp.Config.Select) > 0 {
		js.selector = NewSelector(ds.Sp.Config.Select, ds.Sp.Config.ColumnCasing)
	}

	if flatten < 0 {
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

// SetOrderedKeys publishes the post-select column order. First call wins;
// subsequent calls are dropped (the channel is 1-buffered).
// Flatten returns the jsonStream's flatten depth (max nesting level expanded
// into `__`-joined keys). Negative means records are kept as raw JSON.
func (js *jsonStream) Flatten() int {
	if js == nil {
		return 0
	}
	return js.flatten
}

func (js *jsonStream) SetOrderedKeys(keys []string) {
	if js == nil || js.orderedKeysCh == nil {
		return
	}
	select {
	case js.orderedKeysCh <- keys:
	default:
	}
}

func (js *jsonStream) NextFunc(it *Iterator) bool {
	var recordsInterf []map[string]any
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

	// Peek the raw bytes of the top-level value when the decoder is a stock
	// *json.Decoder, so selector-driven `*` expansion can use source key
	// order. The API path uses SetOrderedKeys instead (its per-record JSON
	// is alphabetized at the pipe boundary). Skip when jmespath/jq is set —
	// those run on the parsed payload, which may reshape keys.
	var payload any
	var raw json.RawMessage
	var sourceKeys []string
	canPeek := js.selector != nil && len(js.OrderedKeys) == 0 && js.jmespath == "" && js.jq == ""
	if canPeek {
		if _, ok := js.decoder.(*json.Decoder); !ok {
			canPeek = false
		}
	}

	if canPeek {
		err = js.decoder.Decode(&raw)
	} else if js.HasMapPayload {
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

	// The producer (if any) publishes before writing to the pipe, so the
	// channel is guaranteed populated by the time Decode returns.
	if len(js.OrderedKeys) == 0 {
		select {
		case keys := <-js.orderedKeysCh:
			js.OrderedKeys = keys
		default:
		}
	}

	if canPeek {
		if len(js.OrderedKeys) == 0 {
			sourceKeys, _ = FirstObjectKeysInOrder(raw)
		}
		if js.HasMapPayload {
			m := g.M()
			if err = json.Unmarshal(raw, &m); err != nil {
				it.Context.CaptureErr(g.Error(err, "could not decode JSON body"))
				return false
			}
			payload = m
		} else {
			if err = json.Unmarshal(raw, &payload); err != nil {
				it.Context.CaptureErr(g.Error(err, "could not decode JSON body"))
				return false
			}
		}
	}

	if js.jmespath != "" {
		payload, err = jmespath.Search(js.jmespath, payload)
		if err != nil {
			it.Context.CaptureErr(g.Error(err, "could not search jmespath: %s", js.jmespath))
			return false
		}
	} else if js.jq != "" {
		results, err := JqRun(js.jq, payload)
		if err != nil {
			it.Context.CaptureErr(err)
			return false
		}
		payload = results
	}

	switch payloadV := payload.(type) {
	case nil:
		recordsInterf = nil
	case map[string]any:
		// is one record
		recordsInterf = js.extractNestedArray(payloadV)
		if len(recordsInterf) == 0 {
			recordsInterf = []map[string]any{payloadV}
		}
	case map[any]any:
		// is one record
		interf := map[string]any{}
		for k, v := range payloadV {
			interf[cast.ToString(k)] = v
		}
		recordsInterf = js.extractNestedArray(interf)
		if len(recordsInterf) == 0 {
			recordsInterf = []map[string]any{interf}
		}
	case []any:
		recordsInterf = []map[string]any{}
		recList := payloadV
		if len(recList) == 0 {
			return js.NextFunc(it)
		}

		switch recList[0].(type) {
		case map[any]any:
			for _, rec := range recList {
				newRec := map[string]any{}
				for k, v := range rec.(map[any]any) {
					newRec[cast.ToString(k)] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case map[string]any:
			for _, val := range recList {
				recordsInterf = append(recordsInterf, val.(map[string]any))
			}
		default:
			// is array of single values
			for _, val := range recList {
				recordsInterf = append(recordsInterf, map[string]any{"data": val})
			}
		}
	case []map[any]any:
		recordsInterf = []map[string]any{}
		for _, rec := range payloadV {
			newRec := map[string]any{}
			for k, v := range rec {
				newRec[cast.ToString(k)] = v
			}
			recordsInterf = append(recordsInterf, newRec)
		}
	case []map[string]any:
		recordsInterf = payloadV
	default:
		err = g.Error("unhandled JSON interface type: %#v", payloadV)
		it.Context.CaptureErr(err)
		return false
	}

	// parse records
	js.parseRecords(recordsInterf, sourceKeys)

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

// orderKeys returns rec's keys in the preferred order (case-insensitive),
// followed by any remaining keys in alphabetical order.
func orderKeys(rec map[string]any, preferred []string) []string {
	if len(preferred) == 0 {
		keys := lo.Keys(rec)
		sort.Strings(keys)
		return keys
	}

	keys := make([]string, 0, len(rec))
	seen := make(map[string]struct{}, len(rec))
	available := make(map[string]string, len(rec))
	for k := range rec {
		available[strings.ToLower(k)] = k
	}
	for _, k := range preferred {
		actual, ok := available[strings.ToLower(k)]
		if !ok {
			continue
		}
		if _, dup := seen[actual]; dup {
			continue
		}
		seen[actual] = struct{}{}
		keys = append(keys, actual)
	}
	remaining := make([]string, 0, len(rec))
	for k := range rec {
		if _, done := seen[k]; done {
			continue
		}
		remaining = append(remaining, k)
	}
	sort.Strings(remaining)
	return append(keys, remaining...)
}

// FlattenRecord flattens a record's nested objects/arrays into `__`-joined
// keys, matching the jsonStream's flattening (Delimiter "__", Safe). depth is
// the max flatten level (use 1 for the API default); depth < 0 returns the
// record unchanged. This lets callers apply a `select` against flattened
// field names (e.g. `owner__login`) the same way the stream does downstream.
func FlattenRecord(rec map[string]any, depth int) map[string]any {
	if depth < 0 {
		return rec
	}
	out, err := flat.Flatten(rec, &flat.Options{Delimiter: "__", Safe: true, MaxDepth: depth})
	if err != nil {
		return rec
	}
	return out
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

func (js *jsonStream) parseRecords(records []map[string]any, sourceKeys []string) {
	if records == nil {
		js.buffer <- nil
		return
	}

	for _, rec := range records {
		if js.flatten < 0 {
			js.buffer <- []any{g.Marshal(rec)}
			continue
		}

		newRec, _ := flat.Flatten(rec, &flat.Options{Delimiter: "__", Safe: true, MaxDepth: js.flatten})

		if js.selector != nil {
			// Resolve select against flattened names before applying renames.
			// Keep the source order for keys that survived flattening.
			if len(js.OrderedKeys) == 0 {
				js.OrderedKeys = js.selector.OrderFields(orderKeys(newRec, sourceKeys))
			}
			renamed := make(map[string]any, len(newRec))
			for name, val := range newRec {
				if newName, include := js.selector.Apply(name); include {
					renamed[newName] = val
				}
			}
			newRec = renamed
		}

		keys := orderKeys(newRec, js.OrderedKeys)

		row := make([]any, len(js.ds.Columns))
		colsToAdd := Columns{}
		for _, colName := range keys {
			if arr, ok := newRec[colName].([]any); ok {
				newRec[colName] = g.Marshal(arr) // arrays serialize to string
			}

			col, ok := js.ColumnMap[colName]
			if !ok {
				col = &Column{
					Name:     colName,
					Type:     js.ds.Sp.CheckType(newRec[colName]),
					Position: len(js.ds.Columns) + len(colsToAdd) + 1,
					FileURI:  cast.ToString(js.ds.Metadata.StreamURL.Value),
				}
				colsToAdd = append(colsToAdd, *col)
				row = append(row, nil)
				js.ColumnMap[col.Name] = col
			}
			i := col.Position - 1
			if i < len(row) {
				row[i] = newRec[colName]
			} else {
				errMsg := g.F("JSON column position out of bounds. column (position %d) cannot be assigned to zero-based row index %d (row length: %d). This may indicate a column name case mismatch between a JSON field and pre-defined column configuration (such as column types or column casing). Please ensure the column names match case-wise.", col.Position, i, len(row))
				js.ds.Context.CaptureErr(g.Error(errMsg))
			}
		}

		if len(colsToAdd) > 0 {
			js.addColumn(colsToAdd...)
		}

		js.buffer <- row
	}
}

func (js *jsonStream) extractNestedArray(rec map[string]any) (recordsInterf []map[string]any) {
	if js.flatten < 0 {
		return []map[string]any{rec}
	}

	recordsInterf = []map[string]any{}
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

	var payload any
	for _, key := range keys {
		// have predefined list for now
		switch strings.ToLower(key) {
		case "data", "records", "rows", "result":
			payload = rec[key]
		}
	}

	switch payloadV := payload.(type) {
	case []any:
		recordsInterf = []map[string]any{}
		recList := payloadV
		if len(recList) == 0 {
			return
		}

		switch recList[0].(type) {
		case map[any]any:
			for _, rec := range recList {
				newRec := map[string]any{}
				for k, v := range rec.(map[any]any) {
					newRec[cast.ToString(k)] = v
				}
				recordsInterf = append(recordsInterf, newRec)
			}
		case map[string]any:
			for _, val := range recList {
				recordsInterf = append(recordsInterf, val.(map[string]any))
			}
		default:
			// is array of single values
			for _, val := range recList {
				recordsInterf = append(recordsInterf, map[string]any{"data": val})
			}
		}
	case []map[any]any:
		recordsInterf = []map[string]any{}
		for _, rec := range payloadV {
			newRec := map[string]any{}
			for k, v := range rec {
				newRec[cast.ToString(k)] = v
			}
			recordsInterf = append(recordsInterf, newRec)
		}
	case []map[string]any:
		recordsInterf = payloadV
	}

	return recordsInterf
}

// DecodeJSONIfBase64 returns jsonBody as-is when it's already valid JSON,
// otherwise tries base64-decode and returns the decoded JSON. Falls back to
// the original on any failure (malformed JSON passes through unchanged).
func DecodeJSONIfBase64(jsonBody string) (string, error) {
	if json.Valid([]byte(jsonBody)) {
		return jsonBody, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(jsonBody)
	if err != nil {
		return jsonBody, nil
	}

	if json.Valid(decoded) {
		return string(decoded), nil
	}
	return jsonBody, nil
}

var (
	jqCache   = map[string]*gojq.Code{}
	jqCacheMu sync.RWMutex
)

// JqCompile returns a compiled jq code for the given expression, using a cache.
func JqCompile(expr string) (*gojq.Code, error) {
	jqCacheMu.RLock()
	code, ok := jqCache[expr]
	jqCacheMu.RUnlock()
	if ok {
		return code, nil
	}

	query, err := gojq.Parse(expr)
	if err != nil {
		return nil, g.Error(err, "could not parse jq expression: %s", expr)
	}

	code, err = gojq.Compile(query)
	if err != nil {
		return nil, g.Error(err, "could not compile jq expression: %s", expr)
	}

	jqCacheMu.Lock()
	jqCache[expr] = code
	jqCacheMu.Unlock()

	return code, nil
}

// JqRun compiles and runs a jq expression against input, returning all results.
func JqRun(expr string, input any) ([]any, error) {
	code, err := JqCompile(expr)
	if err != nil {
		return nil, err
	}

	results := []any{}
	iter := code.Run(input)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := v.(error); ok {
			return nil, g.Error(err, "jq runtime error for expression: %s", expr)
		}
		results = append(results, v)
	}
	return results, nil
}

// FirstObjectKeysInOrder returns the source-order keys of the first
// top-level object (or the first array element). Recovers what
// json.Unmarshal into map[string]any loses. Returns nil for non-object roots.
func FirstObjectKeysInOrder(b []byte) ([]string, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); ok && d == '[' {
		tok, err = dec.Token()
		if err != nil {
			return nil, err
		}
	}
	d, ok := tok.(json.Delim)
	if !ok || d != '{' {
		return nil, nil
	}
	keys := []string{}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return keys, err
		}
		name, ok := t.(string)
		if !ok {
			return keys, nil
		}
		keys = append(keys, name)
		// Decode consumes the value so the next Token() lands on the next key.
		var v any
		if err := dec.Decode(&v); err != nil {
			return keys, err
		}
	}
	return keys, nil
}

