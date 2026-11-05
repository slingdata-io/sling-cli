package iop

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
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
	result, err = io.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

	// zstandard
	reader = strings.NewReader(value)
	cp = NewCompressor(ZStandardCompressorType)
	cReader = cp.Compress(reader)
	dReader, err = cp.Decompress(cReader)
	g.AssertNoError(t, err)
	result, err = io.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

	// snappy
	reader = strings.NewReader(value)
	cp = NewCompressor(SnappyCompressorType)
	cReader = cp.Compress(reader)
	dReader, err = cp.Decompress(cReader)
	g.AssertNoError(t, err)
	result, err = io.ReadAll(dReader)
	g.AssertNoError(t, err)
	assert.Equal(t, value, string(result))

}

func TestUntarGz(t *testing.T) {
	writeTarGz := func(path string, files map[string]string) {
		f, err := os.Create(path)
		g.AssertNoError(t, err)
		gw := gzip.NewWriter(f)
		tw := tar.NewWriter(gw)
		for name, body := range files {
			err = tw.WriteHeader(&tar.Header{Name: name, Mode: 0755, Size: int64(len(body)), Typeflag: tar.TypeReg})
			g.AssertNoError(t, err)
			_, err = tw.Write([]byte(body))
			g.AssertNoError(t, err)
		}
		g.AssertNoError(t, tw.Close())
		g.AssertNoError(t, gw.Close())
		g.AssertNoError(t, f.Close())
	}

	dir := t.TempDir()

	t.Run("extract", func(t *testing.T) {
		src := filepath.Join(dir, "ok.tar.gz")
		dest := filepath.Join(dir, "ok")
		writeTarGz(src, map[string]string{"duckdb": "binary", "sub/a.txt": "a"})

		nodes, err := UntarGz(src, dest)
		g.AssertNoError(t, err)
		assert.Len(t, nodes, 2)

		body, err := os.ReadFile(filepath.Join(dest, "duckdb"))
		g.AssertNoError(t, err)
		assert.Equal(t, "binary", string(body))

		stat, err := os.Stat(filepath.Join(dest, "duckdb"))
		g.AssertNoError(t, err)
		assert.NotZero(t, stat.Mode()&0100, "keeps the exec bit")

		body, err = os.ReadFile(filepath.Join(dest, "sub", "a.txt"))
		g.AssertNoError(t, err)
		assert.Equal(t, "a", string(body))
	})

	t.Run("zip slip", func(t *testing.T) {
		src := filepath.Join(dir, "bad.tar.gz")
		dest := filepath.Join(dir, "bad")
		writeTarGz(src, map[string]string{"../evil": "x"})

		_, err := UntarGz(src, dest)
		assert.ErrorContains(t, err, "illegal file path")
		assert.NoFileExists(t, filepath.Join(dir, "evil"))
	})
}
