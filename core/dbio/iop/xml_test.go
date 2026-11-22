package iop

import (
	"strings"
	"testing"
)

func TestXMLDecoderTextValuesStayStrings(t *testing.T) {
	xmlData := `<root><row><name>Nan</name><code>Inf</code><label>Infinity</label><grade>F</grade><flag>t</flag><price>1.5</price><ok>true</ok><off>false</off></row></root>`
	xd := NewXMLDecoder(strings.NewReader(xmlData))

	var obj interface{}
	if err := xd.Decode(&obj); err != nil {
		t.Fatal(err)
	}
	rec := obj.(map[string]interface{})

	for _, k := range []string{"name", "code", "label", "grade", "flag"} {
		if _, ok := rec[k].(string); !ok {
			t.Errorf("%s: expected string, got %T (%v)", k, rec[k], rec[k])
		}
	}
	if rec["name"] != "Nan" || rec["code"] != "Inf" || rec["label"] != "Infinity" {
		t.Errorf("text values were altered: %v", rec)
	}
	if f, ok := rec["price"].(float64); !ok || f != 1.5 {
		t.Errorf("price: expected float64 1.5, got %T (%v)", rec["price"], rec["price"])
	}
	if b, ok := rec["ok"].(bool); !ok || !b {
		t.Errorf("ok: expected bool true, got %T (%v)", rec["ok"], rec["ok"])
	}
	if b, ok := rec["off"].(bool); !ok || b {
		t.Errorf("off: expected bool false, got %T (%v)", rec["off"], rec["off"])
	}
}
