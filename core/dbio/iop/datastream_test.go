package iop

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/flarco/g/csv"
	"github.com/spf13/cast"
)

func TestBW(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected int64
	}{
		{
			name:     "ASCII only",
			input:    []string{"hello", "world", "123"},
			expected: 16, // "hello,world,123\n" = 5+1+5+1+3+1 = 13
		},
		{
			name:     "With Unicode",
			input:    []string{"hello", "世界", "123"},
			expected: 17, // "hello,世界,123\n" = 5+1+4+1+3+1 = 14
		},
		{
			name:     "Empty strings",
			input:    []string{"", "", ""},
			expected: 3, // ",,\n" = 1+1+1 = 3
		},
		{
			name:     "Mixed content",
			input:    []string{"ABC", "世界", "123"},
			expected: 15, // "ABC,世界,123\n" = 3+1+4+1+3+1 = 12
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test original writeBwCsv
			ds1 := NewDatastream(nil)
			ds1.bwCsv = csv.NewWriter(io.Discard)
			ds1.writeBwCsv(tt.input)
			originalBytes := ds1.Bytes.Load()

			// Test new writeBwCsvSafe
			ds2 := NewDatastream(nil)
			ds2.writeBwCsvSafe(tt.input)
			safeBytes := ds2.Bytes.Load()

			// Compare results
			if originalBytes != safeBytes {
				t.Errorf("Byte count mismatch for %s: original=%d, safe=%d",
					tt.name, originalBytes, safeBytes)
			}

			// Verify against expected
			if safeBytes != cast.ToUint64(tt.expected) {
				t.Errorf("Expected %d bytes for %s, got %d",
					tt.expected, tt.name, safeBytes)
			}
		})
	}
}

func TestEncodeRowAsJSONObject(t *testing.T) {
	stringCol := func(name string) Column { return Column{Name: name, Type: StringType} }
	jsonCol := func(name string) Column { return Column{Name: name, Type: JsonType} }

	tests := []struct {
		name    string
		row     []any
		columns Columns
		want    string
		wantErr bool
	}{
		{
			name:    "empty row produces empty object",
			row:     []any{},
			columns: Columns{},
			want:    `{}`,
		},
		{
			name:    "single field",
			row:     []any{42},
			columns: Columns{stringCol("id")},
			want:    `{"id":42}`,
		},
		{
			name:    "multiple fields preserve column order (not alphabetical)",
			row:     []any{1, "alice", true},
			columns: Columns{stringCol("zeta"), stringCol("alpha"), stringCol("mu")},
			want:    `{"zeta":1,"alpha":"alice","mu":true}`,
		},
		{
			name:    "nil values render as JSON null",
			row:     []any{nil, "x", nil},
			columns: Columns{stringCol("a"), stringCol("b"), stringCol("c")},
			want:    `{"a":null,"b":"x","c":null}`,
		},
		{
			name:    "JSON-typed column with valid JSON object string is inlined",
			row:     []any{`{"k":1}`},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":{"k":1}}`,
		},
		{
			name:    "JSON-typed column with valid JSON array string is inlined",
			row:     []any{`[1,2,3]`},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":[1,2,3]}`,
		},
		{
			name:    "JSON-typed column with literal 'null' string becomes JSON null",
			row:     []any{"null"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":null}`,
		},
		{
			name:    "JSON-typed column with non-JSON-looking string stays quoted",
			row:     []any{"hello"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":"hello"}`,
		},
		{
			name:    "JSON-typed column with malformed JSON-looking string stays quoted",
			row:     []any{"{not-json"},
			columns: Columns{jsonCol("payload")},
			want:    `{"payload":"{not-json"}`,
		},
		{
			name:    "string-typed column with JSON-looking string stays quoted (no inlining)",
			row:     []any{`{"k":1}`},
			columns: Columns{stringCol("payload")},
			want:    `{"payload":"{\"k\":1}"}`,
		},
		{
			name:    "column names with special characters are escaped",
			row:     []any{1, 2},
			columns: Columns{stringCol(`a"b`), stringCol("c\nd")},
			want:    `{"a\"b":1,"c\nd":2}`,
		},
		{
			name:    "values with special characters are escaped",
			row:     []any{`he said "hi"` + "\n"},
			columns: Columns{stringCol("msg")},
			want:    `{"msg":"he said \"hi\"\n"}`,
		},
		{
			name:    "row longer than columns is truncated",
			row:     []any{1, 2, 3, 4},
			columns: Columns{stringCol("a"), stringCol("b")},
			want:    `{"a":1,"b":2}`,
		},
		{
			name:    "row shorter than columns stops at row length",
			row:     []any{1},
			columns: Columns{stringCol("a"), stringCol("b")},
			want:    `{"a":1}`,
		},
		{
			name:    "mixed JSON and scalar columns keep declared order",
			row:     []any{1, `{"nested":true}`, "tail"},
			columns: Columns{stringCol("id"), jsonCol("meta"), stringCol("tag")},
			want:    `{"id":1,"meta":{"nested":true},"tag":"tail"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeRowAsJSONObject(tt.row, tt.columns)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got none; output=%s", string(got))
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("output mismatch\n  got:  %s\n  want: %s", string(got), tt.want)
			}
			// Belt-and-suspenders: every successful result must be valid JSON.
			if !json.Valid(got) {
				t.Fatalf("output is not valid JSON: %s", string(got))
			}
		})
	}
}

func TestReaderReadyRetriesFailedOpenAndClose(t *testing.T) {
	opens := 0
	rr := &ReaderReady{
		URI: "s3://bucket/file.csv",
		Open: func() (io.Reader, error) {
			opens++
			if opens == 1 {
				return nil, errors.New("temporary")
			}
			return io.NopCloser(strings.NewReader("ok")), nil
		},
	}

	if _, err := rr.GetReader(); err == nil {
		t.Fatal("expected first Open to fail")
	}
	r, err := rr.GetReader()
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if opens != 2 {
		t.Fatalf("opens=%d, want 2", opens)
	}
	buf := make([]byte, 2)
	n, _ := r.Read(buf)
	if string(buf[:n]) != "ok" {
		t.Fatalf("got %q", buf[:n])
	}
	if err := rr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rr.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := rr.GetReader(); err == nil {
		t.Fatal("GetReader after Close should fail")
	}
}
