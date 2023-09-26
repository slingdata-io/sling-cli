package sling

import (
	"strings"

	"github.com/flarco/dbio/iop"
	"github.com/flarco/g"
	"github.com/google/uuid"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
)

var decISO8859_1 = charmap.ISO8859_1.NewDecoder()
var decISO8859_5 = charmap.ISO8859_5.NewDecoder()
var decISO8859_15 = charmap.ISO8859_15.NewDecoder()
var decWindows1250 = charmap.Windows1250.NewDecoder()
var decWindows1252 = charmap.Windows1252.NewDecoder()

var transforms = map[string]iop.TransformFunc{
	"replace_accents":    func(sp *iop.StreamProcessor, val string) (string, error) { return iop.ReplaceAccents(sp, val) },
	"trim_space":         func(sp *iop.StreamProcessor, val string) (string, error) { return strings.TrimSpace(val), nil },
	"parse_uuid":         func(sp *iop.StreamProcessor, val string) (string, error) { return ParseUUID(sp, val) },
	"decode_latin1":      func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_1, val) },
	"decode_latin5":      func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_5, val) },
	"decode_latin9":      func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decISO8859_15, val) },
	"decode_windows1250": func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decWindows1250, val) },
	"decode_windows1252": func(sp *iop.StreamProcessor, val string) (string, error) { return Decode(sp, decWindows1252, val) },
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
