package iop

import (
	"testing"

	"github.com/spf13/cast"
)

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessDecimal'
func BenchmarkProcessDecimal1(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.CountDigits("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal2(b *testing.B) {

	for n := 0; n < b.N; n++ {
		cast.ToFloat64E("1273192311231.9382471941")
	}
}

func TestStreamProcessor_countDigits(t *testing.T) {
	sp := NewStreamProcessor()

	tests := []struct {
		name      string
		number    string
		precision int
		scale     int
	}{
		// Integer cases
		{
			name:      "single digit",
			number:    "5",
			precision: 1,
			scale:     0,
		},
		{
			name:      "multiple digits",
			number:    "12345",
			precision: 5,
			scale:     0,
		},
		{
			name:      "zero",
			number:    "0",
			precision: 1,
			scale:     0,
		},
		{
			name:      "large integer",
			number:    "9876543210",
			precision: 10,
			scale:     0,
		},

		// Decimal cases
		{
			name:      "simple decimal",
			number:    "123.45",
			precision: 5, // total digits: 1+2+3+4+5 = 5
			scale:     2, // digits after decimal: 4+5 = 2
		},
		{
			name:      "decimal with zero integer part",
			number:    "0.123",
			precision: 4, // total digits: 0+1+2+3 = 4
			scale:     3, // digits after decimal: 1+2+3 = 3
		},
		{
			name:      "decimal with trailing zeros",
			number:    "123.450",
			precision: 6, // total digits: 1+2+3+4+5+0 = 6
			scale:     3, // digits after decimal: 4+5+0 = 3
		},
		{
			name:      "decimal with leading zeros",
			number:    "00123.45",
			precision: 7, // total digits: 0+0+1+2+3+4+5 = 7
			scale:     2, // digits after decimal: 4+5 = 2
		},
		{
			name:      "very small decimal",
			number:    "0.000001",
			precision: 7, // total digits: 0+0+0+0+0+0+1 = 7
			scale:     6, // digits after decimal: 0+0+0+0+0+1 = 6
		},
		{
			name:      "decimal with many places",
			number:    "123.456789012345",
			precision: 15, // total digits: 3 before + 12 after = 15
			scale:     12, // digits after decimal: 12
		},

		// Edge cases
		{
			name:      "empty string",
			number:    "",
			precision: 0,
			scale:     0,
		},
		{
			name:      "just decimal point",
			number:    ".",
			precision: 0,
			scale:     0,
		},
		{
			name:      "decimal point at start",
			number:    ".123",
			precision: 3, // total digits: 1+2+3 = 3
			scale:     3, // digits after decimal: 1+2+3 = 3
		},
		{
			name:      "decimal point at end",
			number:    "123.",
			precision: 3, // total digits: 1+2+3 = 3
			scale:     0, // no digits after decimal
		},
		{
			name:      "multiple decimal points", // This is an edge case - function will count digits after first dot
			number:    "12.34.56",
			precision: 6, // total digits: 1+2+3+4+5+6 = 6 (excluding decimal points)
			scale:     4, // digits after first decimal: 3+4+5+6 = 4
		},

		// Scientific notation (treated as string)
		{
			name:      "scientific notation",
			number:    "1.23e+10",
			precision: 7, // total digits: 1+2+3+e(letter not counted)+1+0 = 7
			scale:     6, // digits after decimal: 2+3+e+1+0 = 6 (e treated as character)
		},

		// Financial numbers
		{
			name:      "currency amount",
			number:    "1234567.89",
			precision: 9, // total digits: 1+2+3+4+5+6+7+8+9 = 9
			scale:     2, // digits after decimal: 8+9 = 2
		},
		{
			name:      "percentage",
			number:    "99.99",
			precision: 4, // total digits: 9+9+9+9 = 4
			scale:     2, // digits after decimal: 9+9 = 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			precision, scale := sp.CountDigits(tt.number)

			if precision != tt.precision {
				t.Errorf("countDigits(%q) precision = %d, want %d", tt.number, precision, tt.precision)
			}

			if scale != tt.scale {
				t.Errorf("countDigits(%q) scale = %d, want %d", tt.number, scale, tt.scale)
			}
		})
	}
}
