package iop

import (
	"bufio"
	"crypto/sha256"
	"crypto/sha512"
	"embed"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"github.com/jmespath/go-jmespath"
	"github.com/maja42/goval"
	"github.com/spf13/cast"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var (
	TransformsMap = map[string]Transform{}

	GlobalFunctionMap map[string]goval.ExpressionFunction

	GetTransformFunction = func(string) goval.ExpressionFunction {
		return nil
	}

	FunctionToTransform = func(name string, f goval.ExpressionFunction, params ...any) Transform {
		return Transform{
			Name: name,
			Func: func(sp *StreamProcessor, vals ...any) (any, error) {
				if len(params) > 0 {
					vals = append(vals, params...) // append params
				}
				val, err := f(vals...)
				return val, err
			},
		}
	}
)

func init() {
	for _, t := range []Transform{
		TransformDecodeLatin1,
		TransformDecodeLatin5,
		TransformDecodeLatin9,
		TransformDecodeUtf8,
		TransformDecodeUtf8Bom,
		TransformDecodeUtf16,
		TransformDecodeWindows1250,
		TransformDecodeWindows1252,
		TransformDuckdbListToText,
		TransformEncodeLatin1,
		TransformEncodeLatin5,
		TransformEncodeLatin9,
		TransformEncodeUtf8,
		TransformEncodeUtf8Bom,
		TransformEncodeUtf16,
		TransformEncodeWindows1250,
		TransformEncodeWindows1252,
		TransformHashMd5,
		TransformHashSha256,
		TransformHashSha512,
		TransformParseBit,
		TransformBinaryToDecimal,
		TransformBinaryToHex,
		TransformParseFix,
		TransformParseUuid,
		TransformParseMsUuid,
		TransformReplace0x00,
		TransformReplaceAccents,
		TransformReplaceNonPrintable,
		TransformTrimSpace,
		TransformLower,
		TransformUpper,
		TransformSetTimezone,
	} {
		TransformsMap[t.Name] = t
	}
}

//go:embed templates/*
var templatesFolder embed.FS

var Transforms transformsNS

// transformsNS is a namespace for transforms
type transformsNS struct{}

type Transform struct {
	Name       string
	Func       func(*StreamProcessor, ...any) (any, error)
	FuncString func(*StreamProcessor, string) (string, error)
	FuncTime   func(*StreamProcessor, *time.Time) error
	makeFunc   func(t *Transform, params ...any) error
}

type TransformList []Transform

func (tl TransformList) HasTransform(t Transform) bool {
	for _, t0 := range tl {
		if t.Name == t0.Name {
			return true
		}
	}
	return false
}

type Encoding string

var (
	EncodingLatin1      Encoding = "latin1"
	EncodingLatin5      Encoding = "latin5"
	EncodingLatin9      Encoding = "latin9"
	EncodingUtf8        Encoding = "utf8"
	EncodingUtf8Bom     Encoding = "utf8_bom"
	EncodingUtf16       Encoding = "utf16"
	EncodingWindows1250 Encoding = "windows1250"
	EncodingWindows1252 Encoding = "windows1252"
)

func (e Encoding) String() string {
	return string(e)
}

var (
	TransformDecodeLatin1 = Transform{
		Name: EncodingLatin1.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformDecodeLatin5 = Transform{
		Name: EncodingLatin5.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformDecodeLatin9 = Transform{
		Name: EncodingLatin9.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8 = Transform{
		Name: EncodingUtf8.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8Bom = Transform{
		Name: EncodingUtf8Bom.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformDecodeUtf16 = Transform{
		Name: EncodingUtf16.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF16, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1250 = Transform{
		Name: EncodingWindows1250.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeWindows1250, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1252 = Transform{
		Name: EncodingWindows1252.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeWindows1252, val)
			return newVal, err
		},
	}

	TransformDuckdbListToText = Transform{
		Name: "duckdb_list_to_text",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.duckDbListAsText(val), nil
		},
	}

	TransformEncodeLatin1 = Transform{
		Name: EncodingLatin1.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformEncodeLatin5 = Transform{
		Name: EncodingLatin5.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformEncodeLatin9 = Transform{
		Name: EncodingLatin9.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8 = Transform{
		Name: EncodingUtf8.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return fmt.Sprintf("%q", val), nil
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8Bom = Transform{
		Name: EncodingUtf8Bom.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformEncodeUtf16 = Transform{
		Name: EncodingUtf16.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF16, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1250 = Transform{
		Name: EncodingWindows1250.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeWindows1250, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1252 = Transform{
		Name: EncodingWindows1252.String(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeWindows1252, val)
			return newVal, err
		},
	}

	TransformHashMd5 = Transform{
		Name: "hash_md5",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return g.MD5(val), nil
		},
	}

	TransformHashSha256 = Transform{
		Name: "hash_sha256",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.SHA256(val), nil
		},
	}

	TransformHashSha512 = Transform{
		Name: "hash_sha512",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.SHA512(val), nil
		},
	}

	TransformParseBit = Transform{
		Name: "parse_bit",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseBit(sp, val)
		},
	}

	TransformBinaryToDecimal = Transform{
		Name: "binary_to_decimal",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.BinaryToDecimal(sp, val)
		},
	}

	TransformBinaryToHex = Transform{
		Name: "binary_to_hex",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.BinaryToHex(val), nil
		},
	}

	TransformParseFix = Transform{
		Name: "parse_fix",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseFIX(sp, val)
		},
	}

	TransformParseUuid = Transform{
		Name: "parse_uuid",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseUUID(sp, val)
		},
	}

	TransformParseMsUuid = Transform{
		Name: "parse_ms_uuid",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseMsUUID(sp, val)
		},
	}

	TransformReplace0x00 = Transform{
		Name: "replace_0x00",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.Replace0x00(sp, val)
		},
	}

	TransformReplaceAccents = Transform{
		Name: "replace_accents",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.Accent, val)
			return newVal, err
		},
	}

	TransformReplaceNonPrintable = Transform{
		Name: "replace_non_printable",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ReplaceNonPrintable(val), nil
		},
	}

	TransformTrimSpace = Transform{
		Name: "trim_space",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.TrimSpace(val), nil
		},
	}

	TransformLower = Transform{
		Name: "lower",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.ToLower(val), nil
		},
	}

	TransformUpper = Transform{
		Name: "upper",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.ToUpper(val), nil
		},
	}

	// used as lookup, cannot return null since is not pointer
	TransformEmptyAsNull = Transform{
		Name: "empty_as_null",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return val, nil
		},
	}

	TransformSetTimezone = Transform{
		Name: "set_timezone",
		makeFunc: func(t *Transform, location ...any) error {
			if len(location) == 0 {
				return g.Error("param for 'set_timezone' should be the a compatible IANA Time Zone")
			}
			loc, err := time.LoadLocation(strings.Trim(cast.ToString(location[0]), `"'`))
			if err != nil {
				return g.Error(err, "could not load timezone (%s), should be the a compatible IANA Time Zone", location[0])
			}

			// create FuncTime with provided location
			t.FuncTime = func(sp *StreamProcessor, val *time.Time) error {
				newVal := val.In(loc)
				*val = newVal
				return nil
			}

			return nil
		},
	}
)

var fixDelimiter string

var fixMapping = map[int]string{}

// ParseFIXMap converts a FIX message into a json format
func (t transformsNS) ParseFIXMap(message string) (fixMap map[string]any, err error) {
	delimiters := []string{"|", " ", ""}

	fixInit := func() {
		maxVals := 0
		for _, deli := range delimiters {
			if cnt := len(strings.Split(message, deli)); cnt > maxVals {
				maxVals = cnt
			}
		}

		for _, deli := range delimiters {
			if cnt := len(strings.Split(message, deli)); cnt == maxVals {
				fixDelimiter = deli
			}
		}

		// from https://www.wireshark.org/docs/dfref/f/fix.html
		fixMappingBytes, err := templatesFolder.Open("templates/fix_mapping.tsv")
		g.LogFatal(err)

		fileMappingTSV := CSV{Reader: bufio.NewReader(fixMappingBytes)}
		fileMappingTSV.Delimiter = "\t"
		fileMappingTSV.NoDebug = true

		data, err := fileMappingTSV.Read()
		g.LogFatal(err)

		for _, rec := range data.Records() {
			name := strings.TrimPrefix(cast.ToString(rec["field_name"]), "fix.")
			description := cast.ToString(rec["description"])

			codeParts := strings.Split(description, "(")
			if len(codeParts) < 2 {
				continue
			}

			codeB := []byte{}
			for _, r := range codeParts[1] {
				if unicode.IsDigit(r) {
					codeB = append(codeB, byte(r))
				}
			}
			code := cast.ToInt(string(codeB))
			rec["code"] = code

			// convert to snake
			matchAllCap := regexp.MustCompile("([a-z0-9])([A-Z])")
			name = matchAllCap.ReplaceAllString(name, "${1}_${2}")
			fixMapping[code] = strings.ToLower(CleanName(name))
		}
		// g.P(fixMapping)
	}

	// auto detect delimiter
	if fixDelimiter == "" {
		fixInit()
	}

	message = strings.TrimSpace(message)
	parts := strings.Split(message, fixDelimiter)
	if len(parts) == 1 {
		fixInit()
		parts = strings.Split(message, fixDelimiter)
	}

	fixMap = map[string]any{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		tagValue := strings.Split(part, "=")
		if len(tagValue) != 2 {
			return nil, g.Error("did not get tag/value after split: %#v", part)
		}

		tagInt := cast.ToInt(tagValue[0])
		if tag, ok := fixMapping[tagInt]; ok {
			fixMap[tag] = tagValue[1]
		} else {
			fixMap[tagValue[0]] = tagValue[1]
		}
	}

	return
}

// duckDbListAsText adds a space suffix to lists. This is used as
// a workaround to not cast these values as JSON.
// Lists / Arrays do not conform to JSON spec and can error out
// In a case, [0121] is valid in DuckDB as VARCHAR[], but not JSON (zero prefix)
// since we have to manually infer the stdout output from the duckdb binary
func (t transformsNS) duckDbListAsText(val string) string {
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		return val + " "
	}
	return val
}

func (t transformsNS) Decode(sp *StreamProcessor, decoder *encoding.Decoder, val string) (string, error) {
	sUTF8, err := decoder.String(val)
	if err != nil {
		return val, g.Error(err, "could not decode value")
	}
	return sUTF8, nil
}

func (t transformsNS) ParseUUID(sp *StreamProcessor, val string) (string, error) {
	if len(val) == 16 {
		newVal, err := uuid.FromBytes([]byte(val))
		if err != nil {
			return val, g.Error(err, "could not transform while running ParseUUID")
		}
		return newVal.String(), nil
	}
	return val, nil
}

func (t transformsNS) ParseMsUUID(sp *StreamProcessor, val string) (string, error) {
	if len(val) == 16 {
		data := []byte(val)
		var a = binary.LittleEndian.Uint32(data[0:])
		var b = binary.LittleEndian.Uint16(data[4:])
		var c = binary.LittleEndian.Uint16(data[6:])

		var d = binary.BigEndian.Uint16(data[8:])
		var e = binary.BigEndian.Uint16(data[10:])
		var f = binary.BigEndian.Uint32(data[12:])

		var uid = fmt.Sprintf("%08x-%04x-%04x-%04x-%04x%08x", a, b, c, d, e, f)
		return uid, nil
	}
	return val, nil
}

func (t transformsNS) ParseBit(sp *StreamProcessor, val string) (string, error) {
	if len(val) == 1 && (val == "\x00" || val == "\x01") {
		return fmt.Sprintf("%b", []uint8(val)[0]), nil
	}
	return val, nil
}

func (t transformsNS) BinaryToDecimal(sp *StreamProcessor, val string) (string, error) {
	// Handle MySQL BIT type which can be 1 to 64 bits (1 to 8 bytes)
	// Convert binary data to decimal representation for better compatibility
	if len(val) > 0 && len(val) <= 8 {
		// Check if it's binary data (all bytes are either printable or control chars)
		isBinary := true
		for _, b := range []byte(val) {
			// If we have high bit values or control characters, treat as binary
			if b > 127 || (b < 32 && b != 9 && b != 10 && b != 13) {
				isBinary = true
				break
			}
			// If we have regular ASCII text, don't treat as binary
			if b >= 32 && b <= 126 {
				isBinary = false
			}
		}

		if isBinary {
			// Convert binary data to uint64 (big-endian)
			var result uint64
			for i, b := range []byte(val) {
				result |= uint64(b) << (8 * (len(val) - 1 - i))
			}
			return fmt.Sprintf("%d", result), nil
		}
	}
	return val, nil
}

func (t transformsNS) BinaryToHex(val string) string {
	// Convert binary data to hexadecimal representation for Snowflake COPY
	if len(val) == 0 {
		return ""
	}

	// Convert each byte to hex and concatenate
	hexStr := ""
	for _, b := range []byte(val) {
		hexStr += fmt.Sprintf("%02X", b)
	}

	return hexStr
}

func (t transformsNS) Replace0x00(sp *StreamProcessor, val string) (string, error) {
	return strings.ReplaceAll(strings.ReplaceAll(val, "\x00", ""), "\\u0000", "u-0000"), nil // replace the NUL character
}

// ParseFIX converts a FIX message into a json format
func (t transformsNS) ParseFIX(sp *StreamProcessor, message string) (string, error) {
	fixMap, err := t.ParseFIXMap(message)
	if err != nil {
		return message, err
	}
	return g.Marshal(fixMap), nil
}

func (t transformsNS) SHA256(val string) string {
	h := sha256.New()
	h.Write([]byte(val))
	return string(h.Sum(nil))
}

func (t transformsNS) SHA512(val string) string {
	h := sha512.New()
	h.Write([]byte(val))
	return string(h.Sum(nil))
}

// https://stackoverflow.com/a/46637343/2295355
// https://web.itu.edu.tr/sgunduz/courses/mikroisl/ascii.html
func (t transformsNS) ReplaceNonPrintable(val string) string {

	var newVal strings.Builder

	for _, r := range val {
		if r == 0 { // NULL
			continue // remove those
		}

		if r < 9 || (r > 13 && r < 32) {
			newVal.WriteRune(' ') // replace with space
			continue
		}

		if r < 127 {
			// add these
			newVal.WriteRune(r)
			continue
		}

		switch r {
		case 127: //
			continue // remove those
		case 160: // NO-BREAK SPACE
			newVal.WriteRune(' ') // replace with space
			continue
		}

		if !unicode.IsGraphic(r) {
			continue
		}

		// add any other
		newVal.WriteRune(r)
	}

	return newVal.String()
}

type Evaluator struct {
	Eval         *goval.Evaluator
	State        map[string]any
	NoComputeKey string

	bracketRegex *regexp.Regexp
}

func NewEvaluator(states ...map[string]any) *Evaluator {
	// set state
	stateMap := g.M()
	for _, state := range states {
		for k, v := range state {
			stateMap[k] = v
		}
	}

	return &Evaluator{
		Eval:         goval.NewEvaluator(),
		State:        stateMap,
		NoComputeKey: "__sling_no_compute__",
		bracketRegex: regexp.MustCompile(`\{([^{}]+)\}`),
	}
}

func (e *Evaluator) RenderString(val string, extras ...map[string]any) (newVal string, err error) {
	output, err := e.RenderAny(val, extras...)
	if err != nil {
		return
	}

	switch output.(type) {
	case map[string]string, map[string]any, map[any]any, []any, []string:
		newVal = g.Marshal(output)
	default:
		newVal = cast.ToString(output)
	}

	return
}

func (e *Evaluator) RenderAny(input any, extras ...map[string]any) (output any, err error) {

	// check if it's a payload and render
	switch input.(type) {
	case map[any]any, map[string]any, []any, []string:
		return e.RenderPayload(input, extras...)
	}

	toString := func(in any) (out string) {
		inStr, err := cast.ToStringE(in)
		if err != nil {
			inStr = g.Marshal(input)
		}
		return inStr
	}

	inputStr := toString(input)
	if inputStr == "" && input != nil && input != "" {
		return nil, g.Error("unable to convert RenderAny input to string")
	}

	matches := e.bracketRegex.FindAllStringSubmatch(inputStr, -1)

	keysToReplace := []string{}
	for _, match := range matches {
		keysToReplace = append(keysToReplace, match[1])
	}

	// Initialize output with input value
	output = input

	noCompute := false // especially in SQL queries
	stateMap := e.State
	for _, extra := range extras {
		for k, v := range extra {
			stateMap[k] = v
			if k == e.NoComputeKey {
				noCompute = true
			}
		}
	}

	// Create evaluator for expression evaluation
	eval := goval.NewEvaluator()

	canRender := func(e string) bool {
		e = strings.TrimSpace(e)
		can := false
		for _, prefix := range g.ArrStr("state", "store", "env", "run", "target", "source", "stream", "object", "timestamp", "execution", "loop") {
			if strings.HasPrefix(e, prefix) {
				can = true
			}
		}
		return can
	}

	// we'll use those keys as jmespath expr
	for _, expr := range keysToReplace {
		var value any
		callsFuncOrEvals := false

		// Check if expression contains function calls
		for funcName := range GlobalFunctionMap {
			if noCompute {
				break
			} else if strings.Contains(expr, funcName+"(") && strings.Contains(expr, ")") {
				callsFuncOrEvals = true
				break
			}
		}

		// Check for operators that indicate evaluation/computation is needed
		// Based on goval documentation: https://github.com/maja42/goval
		if !callsFuncOrEvals && !noCompute {
			evaluationOperators := []string{
				// Comparison operators
				"==", "!=", "<=", ">=", "<", ">",
				// Arithmetic operators
				"+", "-", "*", "/", "%",
				// Logical operators
				"&&", "||", "!",
				// Bitwise operators
				"|", "&", "^", "~", "<<", ">>",
				// Ternary operator
				"?", ":",
				// Array/string operations
				" in ", "[", // array contains, slicing
			}

			for _, op := range evaluationOperators {
				if strings.Contains(expr, op) {
					callsFuncOrEvals = true
					break
				}
			}
		}

		if !canRender(expr) && !callsFuncOrEvals {
			continue
		}

		// Try jmespath first for simple path expressions (when no evaluation operators are detected)
		validJmesPath := false
		jmespathOperators := []string{
			"[", "]", "*", "|", "?", ":", ".", "@",
			"&&", "||", "!",
			"==", "!=", "<", "<=", ">", ">=",
			"abs(",
			"avg(",
			"contains(",
			"ceil(",
			"ends_with(",
			"floor(",
			"join(",
			"keys(",
			"length(",
			"map(",
			"max(",
			"max_by(",
			"merge(",
			"min(",
			"min_by(",
			"not_null(",
			"reverse(",
			"sort(",
			"sort_by(",
			"starts_with(",
			"sum(",
			"to_array(",
			"to_string(",
			"to_number(",
			"type(",
			"values(",
		}
		for _, op := range jmespathOperators {
			if strings.Contains(expr, op) {
				validJmesPath = true
				break
			}
		}
		var jpValue any
		jpValue, err = jmespath.Search(expr, stateMap)

		// If jmespath failed or if we detected evaluation operators/functions, use goval
		if callsFuncOrEvals || err != nil {
			value, err = eval.Evaluate(expr, stateMap, GlobalFunctionMap)
			if err != nil {
				// check if jmespath rendered
				if jpValue != nil && validJmesPath {
					value = jpValue
					err = nil // use jmespath result
				} else {
					return "", g.Error(err, "could not render expression: %s", expr)
				}
			}
		} else {
			value = jpValue
		}

		key := "{" + expr + "}"
		if strings.TrimSpace(inputStr) == key {
			output = value
		} else {
			// treat as string if whole input isn't the expression
			switch value.(type) {
			case nil:
				// Replace nil values with empty string
				output = strings.ReplaceAll(toString(output), key, "")
			case map[string]string, map[string]any, map[any]any, []any, []string:
				output = strings.ReplaceAll(toString(output), key, g.Marshal(value))
			default:
				output = strings.ReplaceAll(toString(output), key, cast.ToString(value))
			}
		}
	}

	return output, nil
}

func (e *Evaluator) RenderPayload(val any, extras ...map[string]any) (newVal any, err error) {
	newVal = val

	// Handle different types
	switch v := val.(type) {
	case string:
		// For strings, use the existing renderAny method
		return e.RenderAny(v, extras...)
	case map[any]any:
		// For map[any]any, render each value
		resultMap := make(map[string]any)
		for k, mapVal := range v {
			renderedVal, err := e.RenderPayload(mapVal, extras...)
			if err != nil {
				return nil, g.Error(err, "could not render map value for key: %v", k)
			}
			resultMap[cast.ToString(k)] = renderedVal
		}
		return resultMap, nil
	case map[string]any:
		// For map[string]any, render each value
		resultMap := make(map[string]any)
		for k, mapVal := range v {
			renderedVal, err := e.RenderPayload(mapVal, extras...)
			if err != nil {
				return nil, g.Error(err, "could not render map value for key: %s", k)
			}
			resultMap[k] = renderedVal
		}
		return resultMap, nil
	case []any:
		// For []any, render each element
		resultArray := make([]any, len(v))
		for i, arrVal := range v {
			renderedVal, err := e.RenderPayload(arrVal, extras...)
			if err != nil {
				return nil, g.Error(err, "could not render array value at index: %d", i)
			}
			resultArray[i] = renderedVal
		}
		return resultArray, nil
	case []string:
		// For []string, render each string
		resultArray := make([]any, len(v))
		for i, strVal := range v {
			renderedVal, err := e.RenderString(strVal, extras...)
			if err != nil {
				return nil, g.Error(err, "could not render string array value at index: %d", i)
			}
			resultArray[i] = renderedVal
		}
		return resultArray, nil
	default:
		// For other types, return as-is
		return val, nil
	}
}
