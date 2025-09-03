package iop

import (
	"encoding/xml"
	"io"
	"strconv"
	"strings"

	"github.com/flarco/g"
)

type xmlDecoder struct {
	reader     io.Reader
	decoder    *xml.Decoder
	exhausted  bool
	discovered bool
	rootTag    string
	recordTag  string
}

func NewXMLDecoder(reader io.Reader) *xmlDecoder {
	return &xmlDecoder{
		reader:  reader,
		decoder: xml.NewDecoder(reader),
	}
}

func (xd *xmlDecoder) Decode(obj any) error {
	if xd.exhausted {
		return io.EOF
	}

	if !xd.discovered {
		// Discover root and first record tag, parse first record.
		var err error
		xd.rootTag, xd.recordTag, err = xd.discoverAndParseFirst(obj)
		if err != nil {
			return err
		}
		xd.discovered = true
		return nil
	}

	// For subsequent records: advance to next matching StartElement.
	for {
		token, err := xd.decoder.Token()
		if err != nil {
			if err == io.EOF {
				xd.exhausted = true
				return io.EOF
			}
			return g.Error(err, "token read error")
		}

		if se, ok := token.(xml.StartElement); ok && se.Name.Local == xd.recordTag {
			// Parse this record.
			recordMap, err := xd.parseRecord(&se)
			if err != nil {
				return err
			}
			// Set to obj (assuming *interface{} like original).
			if i, ok := obj.(*interface{}); ok {
				*i = recordMap
			} else {
				return g.Error(nil, "obj must be *interface{}")
			}
			return nil
		}

		if ee, ok := token.(xml.EndElement); ok && ee.Name.Local == xd.rootTag {
			// End of root: no more records.
			xd.exhausted = true
			return io.EOF
		}
	}
}

// discoverAndParseFirst skips prologue, finds root, then first record, parses it, and sets tags.
func (xd *xmlDecoder) discoverAndParseFirst(obj any) (rootTag, recordTag string, err error) {
	// Skip to root StartElement.
	for {
		token, err := xd.decoder.Token()
		if err != nil {
			return "", "", g.Error(err, "could not find root")
		}
		if se, ok := token.(xml.StartElement); ok {
			rootTag = se.Name.Local
			break
		}
		// Skip ProcInst, etc.
	}

	// Now find first child StartElement (record).
	var firstSE xml.StartElement
	for {
		token, err := xd.decoder.Token()
		if err != nil {
			return "", "", g.Error(err, "could not find first record")
		}
		if se, ok := token.(xml.StartElement); ok {
			firstSE = se
			recordTag = se.Name.Local
			break
		}
	}

	// Parse the first record.
	recordMap, err := xd.parseRecord(&firstSE)
	if err != nil {
		return "", "", err
	}

	// Set to obj.
	if i, ok := obj.(*interface{}); ok {
		*i = recordMap
	} else {
		return "", "", g.Error(nil, "obj must be *interface{}")
	}

	return rootTag, recordTag, nil
}

// parseRecord builds map for the subtree starting at 'start', until matching EndElement.
func (xd *xmlDecoder) parseRecord(start *xml.StartElement) (map[string]interface{}, error) {
	m := make(map[string]interface{})

	// Handle attributes if any (prefix with "@").
	for _, attr := range start.Attr {
		m["@"+attr.Name.Local] = attr.Value
	}

	for {
		token, err := xd.decoder.Token()
		if err != nil {
			return nil, g.Error(err, "parse record error")
		}

		if ee, ok := token.(xml.EndElement); ok && ee.Name.Local == start.Name.Local {
			return m, nil
		}

		if se, ok := token.(xml.StartElement); ok {
			// For this child: collect CharData until its End.
			var textBuilder strings.Builder
			childKey := se.Name.Local
			for {
				tt, err := xd.decoder.Token()
				if err != nil {
					return nil, g.Error(err, "parse child error")
				}
				if cd, ok := tt.(xml.CharData); ok {
					textBuilder.Write(cd)
				} else if ee, ok := tt.(xml.EndElement); ok && ee.Name.Local == childKey {
					break
				} else {
					// Unexpected (e.g., nested StartElement); for simple flat, error or extend.
					return nil, g.Error(nil, "nested content not supported in simple mode")
				}
			}
			text := strings.TrimSpace(textBuilder.String())
			if text == "" {
				continue // Empty element.
			}

			// Type conversion like mxj: try float64, then bool, else string.
			if f, err := strconv.ParseFloat(text, 64); err == nil {
				m[childKey] = f
			} else if b, err := strconv.ParseBool(text); err == nil {
				m[childKey] = b
			} else {
				m[childKey] = text
			}
		}
		// Ignore other tokens (e.g., comments).
	}
}
