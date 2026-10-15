package database

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeMsUUID(t *testing.T) {
	cases := map[string]string{
		"6f9619ff8b86d011b42d00c04fc964ff":     "6f9619ff-8b86-d011-b42d-00c04fc964ff",
		"6F9619FF8B86D011B42D00C04FC964FF":     "6f9619ff-8b86-d011-b42d-00c04fc964ff",
		"6F9619FF-8B86-D011-B42D-00C04FC964FF": "6F9619FF-8B86-D011-B42D-00C04FC964FF",
		"zz9619ff8b86d011b42d00c04fc964ff":     "zz9619ff8b86d011b42d00c04fc964ff",
		"":                                     "",
	}
	for in, want := range cases {
		assert.Equal(t, want, normalizeMsUUID(in), in)
	}
}

func TestSummarizeBcpOutput(t *testing.T) {
	out := "Starting copy...\n" +
		strings.Repeat("SQLState = 22018, NativeError = 0\nError = Invalid character value for cast specification\n", 1000) +
		"1000 rows sent to SQL Server. Total sent: 1000\n" +
		"#@ Row 1, Column 3: bad @#\n1,secret,a\n" +
		strings.Repeat("y", 2000) + "\n" +
		"BCP copy in failed\n"

	got := summarizeBcpOutput(out)
	assert.Equal(t, []string{
		"SQLState = 22018, NativeError = 0",
		"Error = Invalid character value for cast specification",
		"#@ Row 1, Column 3: bad @#",
		strings.Repeat("y", 500) + "...",
		"BCP copy in failed",
	}, strings.Split(got, "\n"))

	var many []string
	for i := 0; i < 25; i++ {
		many = append(many, strings.Repeat("e", i+1))
	}
	lines := strings.Split(summarizeBcpOutput(strings.Join(many, "\n")), "\n")
	assert.Len(t, lines, 21)
	assert.Equal(t, "... (5 more lines)", lines[20])
}
