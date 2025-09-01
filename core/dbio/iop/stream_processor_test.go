package iop

import (
	"testing"
)

// go test -benchmem -run='^$ github.com/slingdata-io/sling-cli/core/dbio/iop' -bench '^BenchmarkProcessDecimal'
func BenchmarkProcessDecimal1(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.CountDigits("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal2(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.parseStringFloat("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal3(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.parseStringDecimal("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal4(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.parseStringRational("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal5(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.parseStringBigFloat("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal6(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.parseStringIsDecimal("1273192311231.9382471941")
	}
}
func BenchmarkProcessDecimal7(b *testing.B) {
	sp := NewStreamProcessor()
	for n := 0; n < b.N; n++ {
		sp.TruncateDecimalString("1273192311231.9382471941", 9)
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

func TestStreamProcessor_TruncateDecimalString(t *testing.T) {
	sp := NewStreamProcessor()

	tests := []struct {
		name     string
		number   string
		decCount int
		expected string
	}{
		// Basic decimal rounding
		{
			name:     "round to 2 decimals",
			number:   "123.456789",
			decCount: 2,
			expected: "123.45",
		},
		{
			name:     "round to 1 decimal",
			number:   "123.456789",
			decCount: 1,
			expected: "123.4",
		},
		{
			name:     "round to 0 decimals",
			number:   "123.456789",
			decCount: 0,
			expected: "123",
		},

		// Exact decimal count matches
		{
			name:     "exact decimal count match",
			number:   "123.45",
			decCount: 2,
			expected: "123.45",
		},
		{
			name:     "fewer decimals than requested",
			number:   "123.4",
			decCount: 3,
			expected: "123.4",
		},

		// Integer cases
		{
			name:     "integer with decimal count",
			number:   "123",
			decCount: 2,
			expected: "123",
		},

		// Edge cases with zeros
		{
			name:     "trailing zeros",
			number:   "123.450000",
			decCount: 2,
			expected: "123.45",
		},
		{
			name:     "leading zeros in decimal",
			number:   "123.001234",
			decCount: 3,
			expected: "123.001",
		},

		// Zero values
		{
			name:     "zero value",
			number:   "0.123456",
			decCount: 2,
			expected: "0.12",
		},
		{
			name:     "zero integer",
			number:   "0",
			decCount: 2,
			expected: "0",
		},

		// Large numbers
		{
			name:     "large number",
			number:   "1234567890.123456789",
			decCount: 4,
			expected: "1234567890.1234",
		},

		// Small decimals
		{
			name:     "very small decimal",
			number:   "0.000001234",
			decCount: 6,
			expected: "0.000001",
		},

		// Decimal point at end
		{
			name:     "decimal point at end",
			number:   "123.",
			decCount: 2,
			expected: "123.",
		},

		// Decimal point at start
		{
			name:     "decimal point at start",
			number:   ".123456",
			decCount: 2,
			expected: ".12",
		},

		// Empty string
		{
			name:     "empty string",
			number:   "",
			decCount: 2,
			expected: "",
		},

		// Just decimal point
		{
			name:     "just decimal point",
			number:   ".",
			decCount: 2,
			expected: ".",
		},

		// Non-numeric characters (should return original)
		{
			name:     "non-numeric string",
			number:   "abc",
			decCount: 2,
			expected: "abc",
		},
		{
			name:     "mixed characters",
			number:   "123.45abc",
			decCount: 2,
			expected: "123.45abc", // returns original when encountering non-digit
		},

		// Multiple decimal points (truncate at second decimal point)
		{
			name:     "multiple decimal points",
			number:   "12.34.56",
			decCount: 1,
			expected: "12.3", // truncate at second decimal point
		},

		// Negative numbers (note: the method doesn't handle signs, treats as non-digit)
		{
			name:     "negative number",
			number:   "-123.456",
			decCount: 2,
			expected: "-123.45",
		},

		// Scientific notation (treated as string)
		{
			name:     "scientific notation",
			number:   "1.23e+10",
			decCount: 2,
			expected: "1.23e+10", // returns original due to 'e' being non-digit
		},

		// Very high decimal count
		{
			name:     "very high decimal count",
			number:   "123.456",
			decCount: 100,
			expected: "123.456",
		},

		// Negative decimal count behavior
		{
			name:     "negative decimal count",
			number:   "123.456",
			decCount: -1,
			expected: "123", // truncates at decimal point since -1 <= 0
		},

		// Edge case: zero decimal count
		{
			name:     "zero decimal count with integer",
			number:   "123",
			decCount: 0,
			expected: "123",
		},

		// Very long decimal precision
		{
			name:     "very long decimal",
			number:   "1.123456789012345678901234567890",
			decCount: 10,
			expected: "1.1234567890",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sp.TruncateDecimalString(tt.number, tt.decCount)

			if result != tt.expected {
				t.Errorf("RoundDecimalString(%q, %d) = %q, want %q", tt.number, tt.decCount, result, tt.expected)
			}
		})
	}
}
