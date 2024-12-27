package iop

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/flarco/g"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// Compressor implements differnt kind of compression
type Compressor interface {
	Self() Compressor
	Compress(io.Reader) io.Reader
	Decompress(io.Reader) (io.Reader, error)
	Suffix() string
}

// CompressorType is an int type for enum for the Compressor Type
type CompressorType string

const (
	// AutoCompressorType is for auto compression
	AutoCompressorType CompressorType = "auto"
	// NoneCompressorType is for no compression
	NoneCompressorType CompressorType = "none"
	// ZipCompressorType is for Zip compression
	ZipCompressorType CompressorType = "zip"
	// GzipCompressorType is for Gzip compression
	GzipCompressorType CompressorType = "gzip"
	// SnappyCompressorType is for Snappy compression
	SnappyCompressorType CompressorType = "snappy"
	// ZStandardCompressorType is for ZStandard
	ZStandardCompressorType CompressorType = "zstd"
)

var AllCompressorType = []struct {
	Value  CompressorType
	TSName string
}{
	{AutoCompressorType, "AutoCompressorType"},
	{NoneCompressorType, "NoneCompressorType"},
	{ZipCompressorType, "ZipCompressorType"},
	{GzipCompressorType, "GzipCompressorType"},
	{SnappyCompressorType, "SnappyCompressorType"},
	{ZStandardCompressorType, "ZStandardCompressorType"},
}

// Normalize converts to lowercase
func (ct CompressorType) Normalize() *CompressorType {
	return g.Ptr(CompressorType(ct.String()))
}

// String converts to lowercase
func (ct CompressorType) String() string {
	return strings.ToLower(string(ct))
}

// CompressorTypePtr returns a pointer to the CompressorType value passed in.
func CompressorTypePtr(v CompressorType) *CompressorType {
	return &v
}

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(src string, dest string) (nodes []map[string]any, err error) {

	r, err := zip.OpenReader(src)
	if err != nil {
		return nodes, g.Error(err)
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)
		g.Trace("unzipping to: " + fpath)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return nodes, g.Error("%s: illegal file path", fpath)
		}

		node := map[string]any{
			"uri":     "file://" + fpath,
			"is_dir":  f.FileInfo().IsDir(),
			"updated": f.Modified.Unix(),
			"size":    f.UncompressedSize64,
		}
		nodes = append(nodes, node)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return nodes, g.Error(err)
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return nodes, g.Error(err)
		}

		rc, err := f.Open()
		if err != nil {
			return nodes, g.Error(err)
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return nodes, g.Error(err)
		}
	}
	return nodes, nil
}

func NewCompressor(cpType CompressorType) Compressor {
	var compressor Compressor
	switch cpType {
	// case ZipCompressorType:
	case GzipCompressorType:
		compressor = &GzipCompressor{cpType: cpType, suffix: ".gz"}
	case SnappyCompressorType:
		compressor = &SnappyCompressor{cpType: cpType, suffix: ".snappy"}
	case ZStandardCompressorType:
		compressor = &ZStandardCompressor{cpType: cpType, suffix: ".zst"}
	default:
		compressor = &NoneCompressor{cpType: NoneCompressorType, suffix: ""}
	}
	return compressor
}

type NoneCompressor struct {
	Compressor
	cpType CompressorType
	suffix string
}

func (cp *NoneCompressor) Compress(reader io.Reader) io.Reader {
	return reader
}

func (cp *NoneCompressor) Decompress(reader io.Reader) (gReader io.Reader, err error) {
	return reader, err
}

func (cp *NoneCompressor) Suffix() string {
	return cp.suffix
}

type GzipCompressor struct {
	Compressor
	cpType CompressorType
	suffix string
}

// Compress uses gzip to compress
func (cp *GzipCompressor) Compress(reader io.Reader) io.Reader {
	pr, pw := io.Pipe()
	gw, _ := gzip.NewWriterLevel(pw, gzip.BestSpeed)
	go func() {
		_, err := io.Copy(gw, reader)
		if err != nil {
			g.LogError(g.Error(err, "could not compress stream with gzip"))
		}
		gw.Close()
		pw.Close()
	}()

	return pr
}

// Decompress uses gzip to decompress if it is gzip. Otherwise return same reader
func (cp *GzipCompressor) Decompress(reader io.Reader) (gReader io.Reader, err error) {
	gReader, err = gzip.NewReader(reader)
	if err != nil {
		return reader, g.Error(err, "Error using gzip decompressor")
	}

	return gReader, nil
}

func (cp *GzipCompressor) Suffix() string {
	return cp.suffix
}

type SnappyCompressor struct {
	Compressor
	cpType CompressorType
	suffix string
}

// Compress uses gzip to compress
func (cp *SnappyCompressor) Compress(reader io.Reader) io.Reader {

	pr, pw := io.Pipe()
	w := s2.NewWriter(pw)
	go func() {
		_, err := io.Copy(w, reader)
		if err != nil {
			g.LogError(g.Error(err, "could not compress stream with snappy"))
		}
		w.Close()
		pw.Close()
	}()

	return pr
}

// Decompress uses gzip to decompress if it is gzip. Otherwise return same reader
func (cp *SnappyCompressor) Decompress(reader io.Reader) (sReader io.Reader, err error) {

	bReader := bufio.NewReader(reader)
	testBytes, err := bReader.Peek(6)
	if err != nil {
		return bReader, g.Error(err, "Error Peeking")
	}

	// https://github.com/google/snappy/blob/master/framing_format.txt
	// if string(testBytes) == "sNaPpY" {
	_ = testBytes
	sReader = s2.NewReader(bReader)

	return sReader, err
}
func (cp *SnappyCompressor) Suffix() string {
	return cp.suffix
}

type ZStandardCompressor struct {
	Compressor
	cpType CompressorType
	suffix string
}

// Compress uses gzip to compress
func (cp *ZStandardCompressor) Compress(reader io.Reader) io.Reader {

	pr, pw := io.Pipe()
	w, err := zstd.NewWriter(pw, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		g.Warn("Could not compress using ZStandard")
		return reader
	}

	go func() {
		_, err := io.Copy(w, reader)
		if err != nil {
			g.LogError(g.Error(err, "could not compress stream with zstandard"))
		}
		w.Close()
		pw.Close()
	}()

	return pr
}

// Decompress uses gzip to decompress if it is gzip. Otherwise return same reader
func (cp *ZStandardCompressor) Decompress(reader io.Reader) (sReader io.Reader, err error) {
	sReader, err = zstd.NewReader(reader)
	if err != nil {
		return nil, g.Error(err, "Error decompressing with Zstandard")
	}

	return sReader, err
}
func (cp *ZStandardCompressor) Suffix() string {
	return cp.suffix
}

// AutoDecompress auto detects compression to decompress. Otherwise return same reader
func AutoDecompress(reader io.Reader) (gReader io.Reader, err error) {
	bReader, ok := reader.(*bufio.Reader)
	if !ok {
		bReader = bufio.NewReader(reader)
	}

	testBytes, err := bReader.Peek(2)
	if err != nil {
		// return bReader, g.Error(err, "Error Peeking")
		return bReader, nil
	}

	// https://stackoverflow.com/a/28332019
	if testBytes[0] == 31 && testBytes[1] == 139 {
		// is gzip
		gReader, err = gzip.NewReader(bReader)
		if err != nil {
			return bReader, g.Error(err, "Error using gzip.NewReader")
		}
	} else {
		gReader = bReader
	}

	return gReader, err
}
