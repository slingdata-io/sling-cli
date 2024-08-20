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
	"github.com/spf13/cast"
	"golang.org/x/text/encoding"
	"golang.org/x/text/transform"
)

var TransformsMap = map[string]Transform{}

func init() {
	TransformsMap[TransformDecodeLatin1.Name] = TransformDecodeLatin1
	TransformsMap[TransformDecodeLatin5.Name] = TransformDecodeLatin5
	TransformsMap[TransformDecodeLatin9.Name] = TransformDecodeLatin9
	TransformsMap[TransformDecodeUtf8.Name] = TransformDecodeUtf8
	TransformsMap[TransformDecodeUtf8Bom.Name] = TransformDecodeUtf8Bom
	TransformsMap[TransformDecodeUtf16.Name] = TransformDecodeUtf16
	TransformsMap[TransformDecodeWindows1250.Name] = TransformDecodeWindows1250
	TransformsMap[TransformDecodeWindows1252.Name] = TransformDecodeWindows1252
	TransformsMap[TransformDuckdbListToText.Name] = TransformDuckdbListToText
	TransformsMap[TransformEncodeLatin1.Name] = TransformEncodeLatin1
	TransformsMap[TransformEncodeLatin5.Name] = TransformEncodeLatin5
	TransformsMap[TransformEncodeLatin9.Name] = TransformEncodeLatin9
	TransformsMap[TransformEncodeUtf8.Name] = TransformEncodeUtf8
	TransformsMap[TransformEncodeUtf8Bom.Name] = TransformEncodeUtf8Bom
	TransformsMap[TransformEncodeUtf16.Name] = TransformEncodeUtf16
	TransformsMap[TransformEncodeWindows1250.Name] = TransformEncodeWindows1250
	TransformsMap[TransformEncodeWindows1252.Name] = TransformEncodeWindows1252
	TransformsMap[TransformHashMd5.Name] = TransformHashMd5
	TransformsMap[TransformHashSha256.Name] = TransformHashSha256
	TransformsMap[TransformHashSha512.Name] = TransformHashSha512
	TransformsMap[TransformParseBit.Name] = TransformParseBit
	TransformsMap[TransformParseFix.Name] = TransformParseFix
	TransformsMap[TransformParseUuid.Name] = TransformParseUuid
	TransformsMap[TransformParseMsUuid.Name] = TransformParseMsUuid
	TransformsMap[TransformReplace0x00.Name] = TransformReplace0x00
	TransformsMap[TransformReplaceAccents.Name] = TransformReplaceAccents
	TransformsMap[TransformReplaceNonPrintable.Name] = TransformReplaceNonPrintable
	TransformsMap[TransformTrimSpace.Name] = TransformTrimSpace
	TransformsMap[TransformSetTimezone.Name] = TransformSetTimezone
}

//go:embed templates/*
var templatesFolder embed.FS

var Transforms transformsNS

// transformsNS is a namespace for transforms
type transformsNS struct{}

type Transform struct {
	Name       string
	FuncString func(*StreamProcessor, string) (string, error)
	FuncTime   func(*StreamProcessor, *time.Time) error
	makeFunc   func(t *Transform, params ...any) error
}

var (
	TransformDecodeLatin1 = Transform{
		Name: "decode_latin1",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformDecodeLatin5 = Transform{
		Name: "decode_latin5",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformDecodeLatin9 = Transform{
		Name: "decode_latin9",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8 = Transform{
		Name: "decode_utf8",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8, val)
			return newVal, err
		},
	}

	TransformDecodeUtf8Bom = Transform{
		Name: "decode_utf8_bom",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformDecodeUtf16 = Transform{
		Name: "decode_utf16",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeUTF16, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1250 = Transform{
		Name: "decode_windows1250",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.DecodeWindows1250, val)
			return newVal, err
		},
	}

	TransformDecodeWindows1252 = Transform{
		Name: "decode_windows1252",
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
		Name: "encode_latin1",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_1, val)
			return newVal, err
		},
	}

	TransformEncodeLatin5 = Transform{
		Name: "encode_latin5",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_5, val)
			return newVal, err
		},
	}

	TransformEncodeLatin9 = Transform{
		Name: "encode_latin9",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeISO8859_15, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8 = Transform{
		Name: "encode_utf8",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			return fmt.Sprintf("%q", val), nil
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8, val)
			return newVal, err
		},
	}

	TransformEncodeUtf8Bom = Transform{
		Name: "encode_utf8_bom",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF8BOM, val)
			return newVal, err
		},
	}

	TransformEncodeUtf16 = Transform{
		Name: "encode_utf16",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeUTF16, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1250 = Transform{
		Name: "encode_windows1250",
		FuncString: func(sp *StreamProcessor, val string) (string, error) {
			newVal, _, err := transform.String(sp.transformers.EncodeWindows1250, val)
			return newVal, err
		},
	}

	TransformEncodeWindows1252 = Transform{
		Name: "encode_windows1252",
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
		fileMappingTSV.Delimiter = '\t'
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
