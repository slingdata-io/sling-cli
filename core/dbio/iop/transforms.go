package iop

import (
	"strings"
	"unicode"

	"github.com/flarco/g"
	"golang.org/x/text/transform"
)

var Transforms = map[string]TransformFunc{}

func ReplaceAccents(sp *StreamProcessor, val string) (string, error) {
	newVal, _, err := transform.String(sp.accentTransformer, val)
	if err != nil {
		return val, g.Error(err, "could not transform while running ReplaceAccents")
	}
	return newVal, nil
}

// https://stackoverflow.com/a/46637343/2295355
// https://web.itu.edu.tr/sgunduz/courses/mikroisl/ascii.html
func TrimNonPrintable(val string) string {

	var newVal strings.Builder

	for _, r := range val {
		if r < 9 || (r > 13 && r < 32) {
			continue // remove those
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
