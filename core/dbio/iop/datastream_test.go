package iop

import (
	"io"
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
