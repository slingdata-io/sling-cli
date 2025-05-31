package iop

import (
	"os"
	"testing"

	"github.com/flarco/g"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestNonPrintable(t *testing.T) {
	chars := []string{"\x00", "\u00A0", " ", "\t", "\n", "\x01"}
	for _, char := range chars {
		g.Info("%#v => %d => %#v => %#v", char, char[0], char[0], Transforms.ReplaceNonPrintable(char))
	}
	uints := []uint8{0, 1, 2, 3, 49, 127, 160}
	for _, uintVal := range uints {
		g.Warn("%#v => %d => %#v", string(uintVal), uintVal, Transforms.ReplaceNonPrintable(string(uintVal)))
	}
}

func TestFIX(t *testing.T) {
	messages := []string{
		"8=FIX.4.2|9=332|35=8|49=XXX|56=SYS1|34=190|52=20181106-08:00:23|128=802c88|1=802c88_ISA|6=1.2557|11=7314956|14=12|15=GBP|17=EAVVA18KA1117184|20=0|22=4|30=XLON|31=1.2557|32=12|37=OAVVA18KA8302522|38=12|39=2|40=1|48=JE00B6173J15|54=2|55=GCP|59=1|60=20181106-08:00:21|63=6|64=20181108|76=CSTEGB21|110=0|119=15.0684|120=GBP|150=2|151=0|167=CS|207=XLON|10=105|",
		"8=FIX.4.2|9=393|35=8|49=XXX|56=SYS1|34=191|52=20181106-08:00:33|128=802c11|1=569_C11_TPAB|6=0.2366|11=16669868|14=6061|15=GBP|17=EBSTI18KA1117185|20=0|21=2|22=4|30=XOFF|31=0.2366|32=6061|37=OBSTI18KA8302657|38=6061|39=2|40=2|44=0.2366|48=GB00B0DG3H29|54=1|55=SXX|59=6|60=20181106-08:00:31|63=3|64=20181108|76=WNTSGB2LBIC|110=0|119=1434.03|120=GBP|126=20181106-23:00:00|150=2|151=0|152=1434.03|167=CS|207=XLON|10=178|",
		"8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|",
		"8=FIX.4.2 | 9=178 | 35=8 | 49=PHLX | 56=PERS | 52=20071123-05:30:00.000 | 11=ATOMNOCCC9990900 | 20=3 | 150=E | 39=E | 55=MSFT | 167=CS | 54=1 | 38=15 | 40=2 | 44=15 | 58=PHLX EQUITY TESTING | 59=0 | 47=C | 32=0 | 31=0 | 151=15 | 14=0 | 6=0 | 10=128 |",
		"8=FIX.4.09=12835=D34=249=TW52=20060102-15:04:0556=ISLD115=116=CS128=MG129=CB11=ID21=338=10040=w54=155=INTC60=20060102-15:04:0510=123",
	}
	for i, message := range messages {
		fixMap, err := Transforms.ParseFIXMap(message)
		g.LogFatal(err)

		switch i {
		case 0:
			assert.Contains(t, fixMap, "account")
			assert.Contains(t, fixMap, "avg_px")
		case 1:
			assert.Contains(t, fixMap, "account")
			assert.Contains(t, fixMap, "settl_curr_amt")
		case 3:
			assert.Contains(t, fixMap, "begin_string")
			assert.Contains(t, fixMap, "sending_time")
		case 4:
			assert.Contains(t, fixMap, "cl_ord_id")
			assert.Contains(t, fixMap, "deliver_to_sub_id")
		}
		// g.Info("%s", g.Marshal(fixMap))
	}
}

func TestDecode(t *testing.T) {
	filePath := "test/my_file.utf16.csv"
	bytes, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	for i, r := range bytes {
		if i > 6 {
			break
		}
		g.Info("%#v, %#v, %d", string(r), r, r)
	}
}

func TestTransformMsUUID(t *testing.T) {
	uuidBytes := []byte{0x78, 0x56, 0x34, 0x12, 0x34, 0x12, 0x34, 0x12, 0x12, 0x34, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc}
	sp := NewStreamProcessor()
	val, _ := Transforms.ParseMsUUID(sp, cast.ToString(uuidBytes))
	assert.Equal(t, "12345678-1234-1234-1234-123456789abc", val)
}

func TestBinaryToDecimal(t *testing.T) {
	sp := NewStreamProcessor()

	// Test cases for various BIT sizes
	testCases := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "BIT(1) - 0",
			input:    []byte{0x00},
			expected: "0",
		},
		{
			name:     "BIT(1) - 1",
			input:    []byte{0x01},
			expected: "1",
		},
		{
			name:     "BIT(8) - 255",
			input:    []byte{0xFF},
			expected: "255",
		},
		{
			name:     "BIT(16) - 65535",
			input:    []byte{0xFF, 0xFF},
			expected: "65535",
		},
		{
			name:     "BIT(24) - 16777215",
			input:    []byte{0xFF, 0xFF, 0xFF},
			expected: "16777215",
		},
		{
			name:     "BIT(32) - 4294967295",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF},
			expected: "4294967295",
		},
		{
			name:     "BIT(64) - max value",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: "18446744073709551615",
		},
		{
			name:     "BIT(8) - binary 10101010",
			input:    []byte{0xAA}, // binary 10101010
			expected: "170",
		},
		{
			name:     "BIT(16) - binary pattern",
			input:    []byte{0x12, 0x34}, // 0x1234 = 4660
			expected: "4660",
		},
		{
			name:     "Regular text should not be converted",
			input:    []byte("hello"),
			expected: "hello",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := Transforms.BinaryToDecimal(sp, string(tc.input))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, val, "Failed for test case: %s", tc.name)
		})
	}
}

func TestBinaryToHex(t *testing.T) {
	// Test cases for ToHex transform
	testCases := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "Empty input",
			input:    []byte{},
			expected: "",
		},
		{
			name:     "Single byte - 0x00",
			input:    []byte{0x00},
			expected: "00",
		},
		{
			name:     "Single byte - 0x01",
			input:    []byte{0x01},
			expected: "01",
		},
		{
			name:     "Single byte - 0xFF",
			input:    []byte{0xFF},
			expected: "FF",
		},
		{
			name:     "Two bytes - 0x1234",
			input:    []byte{0x12, 0x34},
			expected: "1234",
		},
		{
			name:     "Four bytes - 0xDEADBEEF",
			input:    []byte{0xDE, 0xAD, 0xBE, 0xEF},
			expected: "DEADBEEF",
		},
		{
			name:     "Text - Hello",
			input:    []byte("Hello"),
			expected: "48656C6C6F",
		},
		{
			name:     "Eight bytes - all 0xFF",
			input:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			expected: "FFFFFFFFFFFFFFFF",
		},
		{
			name:     "Alternating pattern",
			input:    []byte{0xAA, 0x55, 0xAA, 0x55},
			expected: "AA55AA55",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := Transforms.BinaryToHex(string(tc.input))
			assert.Equal(t, tc.expected, result, "Failed for test case: %s", tc.name)
		})
	}
}
