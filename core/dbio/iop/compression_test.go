package iop

import (
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func TestCompression(t *testing.T) {
	var reader, cReader, dReader io.Reader
	var err error
	value := "testing"
	result := []byte("")

	// gzip
	reader = strings.NewReader(value)
	cp := NewCompressor(GzipCompressorType)
	cReader = cp.Compress(reader)
	dReader, err = cp.Decompress(cReader)
	g.AssertNoError(t, err)
	result, err = ioutil.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

	// zstandard
	reader = strings.NewReader(value)
	cp = NewCompressor(ZStandardCompressorType)
	cReader = cp.Compress(reader)
	dReader, err = cp.Decompress(cReader)
	g.AssertNoError(t, err)
	result, err = ioutil.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

	// snappy
	reader = strings.NewReader(value)
	cp = NewCompressor(SnappyCompressorType)
	cReader = cp.Compress(reader)
	dReader, err = cp.Decompress(cReader)
	g.AssertNoError(t, err)
	result, err = ioutil.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

}
