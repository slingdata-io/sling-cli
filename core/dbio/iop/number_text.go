package iop

import "strings"

// normalizeNumberText gives text that strconv, decimal and big can parse.
//   - 1,234,567 and 1,234.5 lose the commas (thousands separator)
//   - 1,5 and 1234,567 change the comma to a dot (decimal comma)
//   - 3,000 does not change, since it is ambiguous
func normalizeNumberText(s string) string {
	if strings.IndexByte(s, ',') < 0 {
		return s
	}

	start := 0
	if s[0] == '+' || s[0] == '-' {
		start = 1
	}

	p := start
	for p < len(s) && isASCIIDigit(s[p]) {
		p++
	}
	intLen := p - start
	if intLen == 0 || p >= len(s) || s[p] != ',' {
		return s
	}

	// single comma followed by digits only: decimal comma or ambiguous
	r := p + 1
	for r < len(s) && isASCIIDigit(s[r]) {
		r++
	}
	if r == len(s) && r > p+1 {
		isZero := intLen == 1 && s[start] == '0'
		if r-p-1 != 3 || isZero || intLen > 3 {
			return replaceByteAt(s, p, '.')
		}
		return s
	}

	// thousands groups: ,ddd
	if intLen > 3 {
		return s
	}
	groups := 0
	q := p
	for q < len(s) && s[q] == ',' {
		e := q + 1
		for e < len(s) && isASCIIDigit(s[e]) {
			e++
		}
		if e-q-1 != 3 {
			break
		}
		groups++
		q = e
	}
	if groups == 0 {
		return s
	}

	if q < len(s) {
		// only a fraction may follow: .d+
		if s[q] != '.' || q+1 >= len(s) {
			return s
		}
		for _, c := range []byte(s[q+1:]) {
			if !isASCIIDigit(c) {
				return s
			}
		}
	}

	return strings.ReplaceAll(s, ",", "")
}

func isASCIIDigit(c byte) bool { return c >= '0' && c <= '9' }

func replaceByteAt(s string, i int, c byte) string {
	b := []byte(s)
	b[i] = c
	return string(b)
}

// normalizedNumberText returns the normalized text of a string or byte value
// that holds a comma, when the normalization changes it.
func normalizedNumberText(val any) (string, bool) {
	var s string
	switch v := val.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return "", false
	}
	n := normalizeNumberText(s)
	return n, n != s
}
