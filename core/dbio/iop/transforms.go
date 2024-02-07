package iop

import (
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
