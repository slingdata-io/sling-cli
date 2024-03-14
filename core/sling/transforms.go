package sling

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"strings"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

var decISO8859_1 = charmap.ISO8859_1.NewDecoder()
var decISO8859_5 = charmap.ISO8859_5.NewDecoder()
var decISO8859_15 = charmap.ISO8859_15.NewDecoder()
var decWindows1250 = charmap.Windows1250.NewDecoder()
var decWindows1252 = charmap.Windows1252.NewDecoder()

var transforms = map[string]iop.TransformFunc{
	"replace_accents":       func(sp *iop.StreamProcessor, val string) (string, error) { return iop.ReplaceAccents(sp, val) },
	"replace_0x00":          func(sp *iop.StreamProcessor, val string) (string, error) { return Replace0x00(sp, val) },
	"replace_non_printable": func(sp *iop.StreamProcessor, val string) (string, error) { return ReplaceNonPrint(sp, val) },
	"trim_space":            func(sp *iop.StreamProcessor, val string) (string, error) { return strings.TrimSpace(val), nil },
	"parse_uuid":            func(sp *iop.StreamProcessor, val string) (string, error) { return ParseUUID(sp, val) },
	"parse_fix":             func(sp *iop.StreamProcessor, val string) (string, error) { return ParseFIX(sp, val) },
	"parse_bit":             func(sp *iop.StreamProcessor, val string) (string, error) { return ParseBit(sp, val) },
	"decode_latin1":         func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_1, val) },
	"decode_latin5":         func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_5, val) },
	"decode_latin9":         func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_15, val) },
	"decode_windows1250":    func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decWindows1250, val) },
	"decode_windows1252":    func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decWindows1252, val) },
	"hash_md5":              func(sp *iop.StreamProcessor, val string) (string, error) { return g.MD5(val), nil },
	"hash_sha256":           func(sp *iop.StreamProcessor, val string) (string, error) { return SHA256(val), nil },
	"hash_sha512":           func(sp *iop.StreamProcessor, val string) (string, error) { return SHA512(val), nil },
	"duckdb_list_to_text":   func(sp *iop.StreamProcessor, val string) (string, error) { return duckDbListAsTest(val), nil },
}

func init() {
	// set transforms on init
	for k, f := range transforms {
		iop.Transforms[k] = f
	}
}

func Decode(sp *iop.StreamProcessor, decoder *encoding.Decoder, val string) (string, error) {
	sUTF8, err := decoder.String(val)
	if err != nil {
		return val, g.Error(err, "could not decode value")
	}
	return sUTF8, nil
}

func ParseUUID(sp *iop.StreamProcessor, val string) (string, error) {
	if len(val) == 16 {
		newVal, err := uuid.FromBytes([]byte(val))
		if err != nil {
			return val, g.Error(err, "could not transform while running ParseUUID")
		}
		return newVal.String(), nil
	}
	return val, nil
}

func ParseBit(sp *iop.StreamProcessor, val string) (string, error) {
	if len(val) == 1 && (val == "\x00" || val == "\x01") {
		return fmt.Sprintf("%b", []uint8(val)[0]), nil
	}
	return val, nil
}

func Replace0x00(sp *iop.StreamProcessor, val string) (string, error) {
	return strings.ReplaceAll(val, "\x00", ""), nil // replace the NUL character
}

/*
https://web.itu.edu.tr/sgunduz/courses/mikroisl/ascii.html
*/
func ReplaceNonPrint(sp *iop.StreamProcessor, val string) (string, error) {
	return iop.ReplaceNonPrintable(val), nil
}

// ParseFIX converts a FIX message into a json format
func ParseFIX(sp *iop.StreamProcessor, message string) (string, error) {
	fixMap, err := iop.ParseFIX(message)
	if err != nil {
		return message, err
	}
	return g.Marshal(fixMap), nil
}

func SHA256(val string) string {
	h := sha256.New()
	h.Write([]byte(val))
	return string(h.Sum(nil))
}

func SHA512(val string) string {
	h := sha512.New()
	h.Write([]byte(val))
	return string(h.Sum(nil))
}

// duckDbListAsTest adds a space suffix to lists. This is used as
// a workaround to not cast these values as JSON.
// Lists / Arrays do not conform to JSON spec and can error out
// In a case, [0121] is valid in DuckDB as VARCHAR[], but not JSON (zero prefix)
// since we have to manually infer the stdout output from the duckdb binary
func duckDbListAsTest(val string) string {
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		return val + " "
	}
	return val
}
