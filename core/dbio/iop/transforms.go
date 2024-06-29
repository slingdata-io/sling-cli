package iop

import (
	"bufio"
	"embed"
	"regexp"
	"strings"
	"unicode"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

//go:embed templates/*
var templatesFolder embed.FS

var Transforms = map[Transform]TransformFunc{}

type Transform string

const (
	TransformDecodeLatin1        Transform = "decode_latin1"
	TransformDecodeLatin5        Transform = "decode_latin5"
	TransformDecodeLatin9        Transform = "decode_latin9"
	TransformDecodeUtf8          Transform = "decode_utf8"
	TransformDecodeUtf8Bom       Transform = "decode_utf8_bom"
	TransformDecodeUtf16         Transform = "decode_utf16"
	TransformDecodeWindows1250   Transform = "decode_windows1250"
	TransformDecodeWindows1252   Transform = "decode_windows1252"
	TransformDuckdbListToText    Transform = "duckdb_list_to_text"
	TransformEncodeLatin1        Transform = "encode_latin1"
	TransformEncodeLatin5        Transform = "encode_latin5"
	TransformEncodeLatin9        Transform = "encode_latin9"
	TransformEncodeUtf8          Transform = "encode_utf8"
	TransformEncodeUtf8Bom       Transform = "encode_utf8_bom"
	TransformEncodeUtf16         Transform = "encode_utf16"
	TransformEncodeWindows1250   Transform = "encode_windows1250"
	TransformEncodeWindows1252   Transform = "encode_windows1252"
	TransformHashMd5             Transform = "hash_md5"
	TransformHashSha256          Transform = "hash_sha256"
	TransformHashSha512          Transform = "hash_sha512"
	TransformParseBit            Transform = "parse_bit"
	TransformParseFix            Transform = "parse_fix"
	TransformParseUuid           Transform = "parse_uuid"
	TransformParseMsUuid         Transform = "parse_ms_uuid"
	TransformReplace0x00         Transform = "replace_0x00"
	TransformReplaceAccents      Transform = "replace_accents"
	TransformReplaceNonPrintable Transform = "replace_non_printable"
	TransformTrimSpace           Transform = "trim_space"
)

// https://stackoverflow.com/a/46637343/2295355
// https://web.itu.edu.tr/sgunduz/courses/mikroisl/ascii.html
func ReplaceNonPrintable(val string) string {

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

var fixDelimiter string

var fixMapping = map[int]string{}

// ParseFIX converts a FIX message into a json format
func ParseFIX(message string) (fixMap map[string]any, err error) {
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
