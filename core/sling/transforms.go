package sling

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/flarco/g"
	"github.com/google/uuid"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"golang.org/x/text/encoding"
)

var transforms = map[iop.Transform]iop.TransformFunc{
	iop.TransformDecodeLatin1: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeLatin1, val)
	},
	iop.TransformDecodeLatin5: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeLatin5, val)
	},
	iop.TransformDecodeLatin9: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeLatin9, val)
	},
	iop.TransformDecodeUtf8: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeUtf8, val)
	},
	iop.TransformDecodeUtf8Bom: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeUtf8Bom, val)
	},
	iop.TransformDecodeUtf16: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeUtf16, val)
	},
	iop.TransformDecodeWindows1250: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeWindows1250, val)
	},
	iop.TransformDecodeWindows1252: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformDecodeWindows1252, val)
	},
	iop.TransformEncodeUtf8: func(sp *iop.StreamProcessor, val string) (string, error) {
		return fmt.Sprintf("%q", val), nil
	},
	iop.TransformDuckdbListToText: func(sp *iop.StreamProcessor, val string) (string, error) { return duckDbListAsText(val), nil },
	iop.TransformHashMd5:          func(sp *iop.StreamProcessor, val string) (string, error) { return g.MD5(val), nil },
	iop.TransformHashSha256:       func(sp *iop.StreamProcessor, val string) (string, error) { return SHA256(val), nil },
	iop.TransformHashSha512:       func(sp *iop.StreamProcessor, val string) (string, error) { return SHA512(val), nil },
	iop.TransformParseBit:         func(sp *iop.StreamProcessor, val string) (string, error) { return ParseBit(sp, val) },
	iop.TransformParseFix:         func(sp *iop.StreamProcessor, val string) (string, error) { return ParseFIX(sp, val) },
	iop.TransformParseUuid:        func(sp *iop.StreamProcessor, val string) (string, error) { return ParseUUID(sp, val) },
	iop.TransformParseMsUuid:      func(sp *iop.StreamProcessor, val string) (string, error) { return ParseMsUUID(sp, val) },
	iop.TransformReplace0x00:      func(sp *iop.StreamProcessor, val string) (string, error) { return Replace0x00(sp, val) },
	iop.TransformReplaceAccents: func(sp *iop.StreamProcessor, val string) (string, error) {
		return sp.EncodingTransform(iop.TransformReplaceAccents, val)
	},
	iop.TransformReplaceNonPrintable: func(sp *iop.StreamProcessor, val string) (string, error) { return ReplaceNonPrint(sp, val) },
	iop.TransformTrimSpace:           func(sp *iop.StreamProcessor, val string) (string, error) { return strings.TrimSpace(val), nil },
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

func ParseMsUUID(sp *iop.StreamProcessor, val string) (string, error) {
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

func ParseBit(sp *iop.StreamProcessor, val string) (string, error) {
	if len(val) == 1 && (val == "\x00" || val == "\x01") {
		return fmt.Sprintf("%b", []uint8(val)[0]), nil
	}
	return val, nil
}

func Replace0x00(sp *iop.StreamProcessor, val string) (string, error) {
	return strings.ReplaceAll(strings.ReplaceAll(val, "\x00", ""), "\\u0000", "u-0000"), nil // replace the NUL character
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

// duckDbListAsText adds a space suffix to lists. This is used as
// a workaround to not cast these values as JSON.
// Lists / Arrays do not conform to JSON spec and can error out
// In a case, [0121] is valid in DuckDB as VARCHAR[], but not JSON (zero prefix)
// since we have to manually infer the stdout output from the duckdb binary
func duckDbListAsText(val string) string {
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		return val + " "
	}
	return val
}
