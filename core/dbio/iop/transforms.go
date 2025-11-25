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
	"github.com/jinzhu/copier"
	"github.com/jmespath/go-jmespath"
	"github.com/maja42/goval"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/spf13/cast"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var (
	TransformsLegacyMap = map[string]TransformLegacy{}

	GlobalFunctionMap map[string]goval.ExpressionFunction

	GetTransformFunction = func(string) goval.ExpressionFunction {
		return nil
	}

	FunctionToTransform = func(name string, f goval.ExpressionFunction, params ...any) TransformLegacy {
		return TransformLegacy{
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

	LocalConnections = cmap.New[map[string]any]()
)

func init() {
	for _, t := range []TransformLegacy{
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
	} {
		TransformsLegacyMap[t.Name] = t
	}
}

//go:embed templates/*
var templatesFolder embed.FS

var Transforms transformsNS

// transformsNS is a namespace for transforms
type transformsNS struct{}

type TransformLegacy struct {
	Name       string
	Func       func(*StreamProcessor, ...any) (any, error)
	FuncString func(*StreamProcessor, string) (string, error)
	FuncTime   func(*StreamProcessor, *time.Time) error
}

type TransformLegacyList []TransformLegacy

func (tl TransformLegacyList) HasTransform(t TransformLegacy) bool {
	for _, t0 := range tl {
		if t.Name == t0.Name {
			return true
		}
	}
	return false
}

type Transform interface {
	Evaluate(row []any) (newRow []any, err error)
	Casted() bool
}

var NewTransform = func(t []map[string]string, _ *StreamProcessor) Transform {
	return nil
}

var ParseStageTransforms = func(payload any) ([]map[string]string, error) {
	return nil, g.Error("please use the official sling-cli release for using transforms")
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

	Encodings = []Encoding{
		EncodingLatin1,
		EncodingLatin5,
		EncodingLatin9,
		EncodingUtf8,
		EncodingUtf8Bom,
		EncodingUtf16,
		EncodingWindows1250,
		EncodingWindows1252,
	}
)

func (e Encoding) String() string {
	return string(e)
}

func (e Encoding) DecodeString() string {
	return g.F("decode_%s", e)
}

func (e Encoding) EncodeString() string {
	return g.F("encode_%s", e)
}

var (
	TransformDecodeLatin1 = TransformLegacy{
		Name: EncodingLatin1.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformDecodeLatin5 = TransformLegacy{
		Name: EncodingLatin5.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformDecodeLatin9 = TransformLegacy{
		Name: EncodingLatin9.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8 = TransformLegacy{
		Name: EncodingUtf8.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8Bom = TransformLegacy{
		Name: EncodingUtf8Bom.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformDecodeUtf16 = TransformLegacy{
		Name: EncodingUtf16.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF16, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1250 = TransformLegacy{
		Name: EncodingWindows1250.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeWindows1250, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1252 = TransformLegacy{
		Name: EncodingWindows1252.DecodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeWindows1252, val)
			return newVal, err
		},
	}

	TransformDuckdbListToText = TransformLegacy{
		Name: "duckdb_list_to_text",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.duckDbListAsText(val), nil
		},
	}

	TransformEncodeLatin1 = TransformLegacy{
		Name: EncodingLatin1.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformEncodeLatin5 = TransformLegacy{
		Name: EncodingLatin5.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformEncodeLatin9 = TransformLegacy{
		Name: EncodingLatin9.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8 = TransformLegacy{
		Name: EncodingUtf8.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return fmt.Sprintf("%q", val), nil
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8Bom = TransformLegacy{
		Name: EncodingUtf8Bom.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformEncodeUtf16 = TransformLegacy{
		Name: EncodingUtf16.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF16, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1250 = TransformLegacy{
		Name: EncodingWindows1250.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeWindows1250, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1252 = TransformLegacy{
		Name: EncodingWindows1252.EncodeString(),
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeWindows1252, val)
			return newVal, err
		},
	}

	TransformHashMd5 = TransformLegacy{
		Name: "hash_md5",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return g.MD5(val), nil
		},
	}

	TransformHashSha256 = TransformLegacy{
		Name: "hash_sha256",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.SHA256(val), nil
		},
	}

	TransformHashSha512 = TransformLegacy{
		Name: "hash_sha512",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.SHA512(val), nil
		},
	}

	TransformParseBit = TransformLegacy{
		Name: "parse_bit",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseBit(sp, val)
		},
	}

	TransformBinaryToDecimal = TransformLegacy{
		Name: "binary_to_decimal",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.BinaryToDecimal(sp, val)
		},
	}

	TransformBinaryToHex = TransformLegacy{
		Name: "binary_to_hex",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.BinaryToHex(val), nil
		},
	}

	TransformParseFix = TransformLegacy{
		Name: "parse_fix",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseFIX(sp, val)
		},
	}

	TransformParseUuid = TransformLegacy{
		Name: "parse_uuid",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseUUID(sp, val)
		},
	}

	TransformParseMsUuid = TransformLegacy{
		Name: "parse_ms_uuid",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ParseMsUUID(sp, val)
		},
	}

	TransformReplace0x00 = TransformLegacy{
		Name: "replace_0x00",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.Replace0x00(sp, val)
		},
	}

	TransformReplaceAccents = TransformLegacy{
		Name: "replace_accents",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.Accent, val)
			return newVal, err
		},
	}

	TransformReplaceNonPrintable = TransformLegacy{
		Name: "replace_non_printable",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return Transforms.ReplaceNonPrintable(val), nil
		},
	}

	TransformTrimSpace = TransformLegacy{
		Name: "trim_space",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.TrimSpace(val), nil
		},
	}

	TransformLower = TransformLegacy{
		Name: "lower",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.ToLower(val), nil
		},
	}

	TransformUpper = TransformLegacy{
		Name: "upper",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return strings.ToUpper(val), nil
		},
	}

	// used as lookup, cannot return null since is not pointer
	TransformEmptyAsNull = TransformLegacy{
		Name: "empty_as_null",
		Func: func(sp *StreamProcessor, vals ...any) (any, error) {
			if len(vals) == 0 {
				return nil, nil
			}
			if vals[0] == "" {
				return nil, nil
			}
			return vals[0], nil
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
	// Remove null bytes and common null representations
	val = strings.ReplaceAll(val, "\x00", "")    // actual null byte
	val = strings.ReplaceAll(val, "\\u0000", "") // escaped unicode null
	val = strings.ReplaceAll(val, "\\x00", "")   // escaped hex null
	val = strings.ReplaceAll(val, "\u0000", "")  // unicode null character
	return val, nil
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
		case 127: // DEL character
			continue // remove those
		case 160: // NO-BREAK SPACE
			newVal.WriteRune(' ') // replace with space
			continue

		// Zero-width characters - remove these
		case 0x200B: // ZERO WIDTH SPACE
			continue
		case 0x200C: // ZERO WIDTH NON-JOINER
			continue
		case 0x200D: // ZERO WIDTH JOINER
			continue
		case 0x2060: // WORD JOINER
			continue
		case 0xFEFF: // ZERO WIDTH NO-BREAK SPACE (BOM)
			continue

		// Directional formatting characters - remove these
		case 0x200E: // LEFT-TO-RIGHT MARK
			continue
		case 0x200F: // RIGHT-TO-LEFT MARK
			continue
		case 0x202A: // LEFT-TO-RIGHT EMBEDDING
			continue
		case 0x202B: // RIGHT-TO-LEFT EMBEDDING
			continue
		case 0x202C: // POP DIRECTIONAL FORMATTING
			continue
		case 0x202D: // LEFT-TO-RIGHT OVERRIDE
			continue
		case 0x202E: // RIGHT-TO-LEFT OVERRIDE
			continue

		// Other problematic characters
		case 0x00AD: // SOFT HYPHEN
			continue // remove
		case 0xFFFC: // OBJECT REPLACEMENT CHARACTER
			continue // remove
		case 0xFFFD: // REPLACEMENT CHARACTER
			continue // remove
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
	Eval            *goval.Evaluator
	State           map[string]any
	NoComputeKey    string
	VarPrefixes     []string
	KeepMissingExpr bool // allows us to leave any missing sub-expression intact
	AllowNoPrefix   bool

	bracketRegex *regexp.Regexp
}

func NewEvaluator(varPrefixes []string, states ...map[string]any) *Evaluator {
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
		VarPrefixes:  varPrefixes,
		bracketRegex: regexp.MustCompile(`\{([^{}]+)\}`),
	}
}

// ExtractVars identifies variable references in a string expression,
// ignoring those inside double quotes. It can recognize patterns like env.VAR, state.VAR, secrets.VAR, and auth.VAR.
// When AllowNoPrefix is true, it also captures unprefixed variables like MY_VAR, some_value, etc.
func (e *Evaluator) ExtractVars(expr string) []string {
	// To track found variable references
	var references []string

	if len(e.VarPrefixes) == 0 && !e.AllowNoPrefix {
		g.Warn("did not set VarPrefixes in Evaluator")
		return []string{}
	}

	// First, we need to identify string literals to exclude them
	// Track positions of string literals
	inString := false
	stringRanges := make([][]int, 0)
	var start int

	for i, char := range expr {
		if char == '"' {
			// Check if the quote is escaped
			if i > 0 && expr[i-1] == '\\' {
				continue
			}

			if !inString {
				// Start of a string
				inString = true
				start = i
			} else {
				// End of a string
				inString = false
				stringRanges = append(stringRanges, []int{start, i})
			}
		}
	}

	// Helper function to check if match is inside a string literal
	isInStringRange := func(match []int) bool {
		for _, strRange := range stringRanges {
			if match[0] >= strRange[0] && match[1] <= strRange[1] {
				return true
			}
		}
		return false
	}

	// Regular expression for finding prefixed variable references
	// Matches env., state., secrets., auth. followed by one or more nested variable names
	// example regex: `(env|state|secrets|auth|response|request|sync)(\.\w+)+`
	if len(e.VarPrefixes) > 0 {
		prefixes := strings.Join(e.VarPrefixes, "|")
		refRegex := regexp.MustCompile(`(` + prefixes + `)(\.\w+)+`)
		matches := refRegex.FindAllStringIndex(expr, -1)

		// Filter out references that are inside string literals
		for _, match := range matches {
			if !isInStringRange(match) {
				// Extract the actual reference
				reference := expr[match[0]:match[1]]
				references = append(references, reference)
			}
		}
	}

	// When AllowNoPrefix is enabled, also capture unprefixed variables
	// Matches standalone identifiers (word characters, not starting with a digit)
	if e.AllowNoPrefix {
		// Match word-only identifiers that aren't part of a dotted path
		// Negative lookbehind/lookahead to avoid matching parts of prefixed vars or function names
		noPrefixRegex := regexp.MustCompile(`\b([a-zA-Z_]\w*)\b`)
		matches := noPrefixRegex.FindAllStringIndex(expr, -1)

		for _, match := range matches {
			if !isInStringRange(match) {
				reference := expr[match[0]:match[1]]

				// Skip if this is part of a prefixed variable (already captured)
				// Check if it's preceded by a dot (part of a path) or followed by a dot (a prefix)
				isPrefixOrPath := false
				if match[0] > 0 && expr[match[0]-1] == '.' {
					isPrefixOrPath = true // Part of a path like "env.VAR"
				}
				if match[1] < len(expr) && expr[match[1]] == '.' {
					isPrefixOrPath = true // A prefix like "env" in "env.VAR"
				}

				// Skip if it's a known prefix
				if g.In(reference, e.VarPrefixes...) {
					isPrefixOrPath = true
				}

				// Skip if it's a function call (followed by opening parenthesis)
				if match[1] < len(expr) && expr[match[1]] == '(' {
					isPrefixOrPath = true // Function name like "if(" or "coalesce("
				}

				if !isPrefixOrPath && !g.In(reference, references...) {
					references = append(references, reference)
				}
			}
		}
	}

	return references

}

func (e *Evaluator) FillMissingKeys(stateMap map[string]any, varsToCheck []string) map[string]any {
	if stateMap == nil {
		stateMap = g.M()
	}

	for _, varToCheck := range varsToCheck {
		varToCheck = strings.TrimSpace(varToCheck)
		parts := strings.Split(varToCheck, ".")

		// Handle unprefixed variables when AllowNoPrefix is enabled
		if len(parts) == 1 && e.AllowNoPrefix {
			// Single word variable without prefix - store at root level
			if _, exists := stateMap[varToCheck]; !exists {
				stateMap[varToCheck] = nil
			}
			continue
		}

		if len(parts) < 2 {
			continue
		}

		section := parts[0]
		if !g.In(section, e.VarPrefixes...) {
			continue
		}

		// Navigate and create nested structure as needed
		current := stateMap
		for i, key := range parts {
			if i == len(parts)-1 {
				// Last key - set to nil if it doesn't exist
				if _, exists := current[key]; !exists {
					current[key] = nil
				}
			} else {
				// Intermediate key - ensure it exists as a map
				if _, exists := current[key]; !exists {
					// Key doesn't exist - create new map
					current[key] = make(map[string]any)
				}

				// Navigate to the next level using type assertion
				if nextMap, ok := current[key].(map[string]any); ok {
					current = nextMap
				} else {
					// Try casting with g.CastToMapAnyE as fallback
					nextMap, err := g.CastToMapAnyE(current[key])
					if err != nil || nextMap == nil {
						// Value exists but is not a map - don't replace it, skip this var
						// The evaluator will error when it tries to access nested keys on a non-map value
						break
					} else {
						current = nextMap
					}
				}
			}
		}
	}

	return stateMap
}

func (e *Evaluator) RenderString(val any, extras ...map[string]any) (newVal string, err error) {
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

	if val == nil || val == "" {
		return "", nil
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

	expressions := []string{}
	varsToCheck := []string{} // to ensure existence in state maps
	for _, match := range matches {
		expression := match[1]
		expressions = append(expressions, expression)
		varsToCheck = append(varsToCheck, e.ExtractVars(expression)...)
	}

	// Initialize output with input value
	output = input

	noCompute := false // especially in SQL queries
	stateMap := g.M("null", nil)
	err = copier.CopyWithOption(&stateMap, &e.State, copier.Option{DeepCopy: true})
	if err != nil {
		return nil, g.Error(err, "could not deep copy for evaluation")
	}

	for _, extra := range extras {
		for k, v := range extra {
			stateMap[k] = v
			if k == e.NoComputeKey {
				noCompute = true
			}
		}
	}

	// Create evaluator for expression evaluation
	canRender := func(expr string) bool {
		expr = strings.TrimSpace(expr)
		can := false
		for _, prefix := range e.VarPrefixes {
			if strings.Contains(expr, prefix+".") {
				can = true
			}
		}
		if strings.Contains(expr, "runtime_state") {
			can = true
		}

		// When AllowNoPrefix is enabled, allow simple identifiers (unprefixed variables)
		if e.AllowNoPrefix && !can {
			// Check if expression is a simple identifier without dots or complex operations
			// This allows {MY_VAR} to be rendered even without a prefix
			simpleIdentifierRegex := regexp.MustCompile(`^[a-zA-Z_]\w*$`)
			if simpleIdentifierRegex.MatchString(expr) {
				can = true
			}
		}

		return can
	}

	// we'll use those keys as jmespath expr
	for _, expr := range expressions {
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

		// ensure vars exist. if it doesn't, set at nil
		if !e.KeepMissingExpr {
			stateMap = e.FillMissingKeys(stateMap, varsToCheck)
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

		key := "{" + expr + "}"
		// If jmespath failed or if we detected evaluation operators/functions, use goval
		if callsFuncOrEvals || err != nil || e.KeepMissingExpr {
			value, err = e.Eval.Evaluate(expr, stateMap, GlobalFunctionMap)
			if err != nil {
				// check if jmespath rendered
				if jpValue != nil && validJmesPath {
					value = jpValue
					err = nil // use jmespath result
				} else if e.KeepMissingExpr && (strings.Contains(err.Error(), "no member") || strings.Contains(err.Error(), "does not exist")) {
					// keeps the expression untouched
					value = key
				} else {
					if errChk := e.Check(expr); errChk != nil {
						return "", g.Error(errChk, "invalid expression: %s", expr)
					}
					return "", g.Error(err, "could not render expression: %s", expr)
				}
			}
		} else {
			value = jpValue
		}

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

func (e *Evaluator) Check(expr string) (err error) {
	inDouble := false
	parenCount := 0

	runes := []rune(expr)
	for i, c := range runes {
		if c == '\'' && !inDouble {
			return g.Error("cannot use single quotes (') for strings in expression, use double quotes (\"): %s", expr)
		} else if c == '"' {
			// Check if this quote is escaped by counting preceding backslashes
			backslashCount := 0
			for j := i - 1; j >= 0 && runes[j] == '\\'; j-- {
				backslashCount++
			}
			// If even number of backslashes (including 0), the quote is not escaped
			if backslashCount%2 == 0 {
				inDouble = !inDouble
			}
		} else if !inDouble {
			// Only track parentheses when not inside double quotes
			switch c {
			case '(':
				parenCount++
			case ')':
				parenCount--
				if parenCount < 0 {
					return g.Error("unmatched closing parenthesis ')' in expression: %s", expr)
				}
			}
		}
	}

	if inDouble {
		return g.Error("unclosed double quote in expression: %s", expr)
	}

	if parenCount > 0 {
		return g.Error("unclosed parenthesis '(' in expression: %s", expr)
	}

	return nil
}
