package iop

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	h "github.com/flarco/gutil"
	"github.com/pkg/sftp"
)

// Compressor implements differnt kind of compression
type Compressor interface {
	Self() Compressor
	Compress(io.Reader) io.Reader
	Decompress(io.Reader) (io.Reader, error)
}

// CompressorType is an int type for enum for the Compressor Type
type CompressorType int

const (
	// ZipCompressorType is for Zip compression
	ZipCompressorType CompressorType = iota
	// GzipCompressorType is for Gzip compression
	GzipCompressorType
	// SnappyCompressorType is for Snappy compression
	SnappyCompressorType
)

// Unzip will decompress a zip archive, moving all files and folders
// within the zip file (parameter 1) to an output directory (parameter 2).
func Unzip(src string, dest string) ([]string, error) {

	var filenames []string

	r, err := zip.OpenReader(src)
	if err != nil {
		return filenames, h.Error(err)
	}
	defer r.Close()

	for _, f := range r.File {

		// Store filename/path for returning and using later on
		fpath := filepath.Join(dest, f.Name)
		h.Trace("unzipping to: " + fpath)

		// Check for ZipSlip. More Info: http://bit.ly/2MsjAWE
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return filenames, h.Error(fmt.Errorf("%s: illegal file path", fpath))
		}

		filenames = append(filenames, fpath)

		if f.FileInfo().IsDir() {
			// Make Folder
			os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		// Make File
		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return filenames, h.Error(err)
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return filenames, h.Error(err)
		}

		rc, err := f.Open()
		if err != nil {
			return filenames, h.Error(err)
		}

		_, err = io.Copy(outFile, rc)

		// Close the file without defer to close before next iteration of loop
		outFile.Close()
		rc.Close()

		if err != nil {
			return filenames, h.Error(err)
		}
	}
	return filenames, nil
}

// Compress uses gzip to compress
func Compress(reader io.Reader) io.Reader {
	pr, pw := io.Pipe()
	gw := gzip.NewWriter(pw)
	go func() {
		_, err := io.Copy(gw, reader)
		if err != nil {
			h.LogError(h.Error(err, "could not compress stream with gzip"))
		}
		gw.Close()
		pw.Close()
	}()

	return pr
}

// Decompress uses gzip to decompress if it is gzip. Otherwise return same reader
func Decompress(reader io.Reader) (gReader io.Reader, err error) {

	bReader := bufio.NewReader(reader)
	testBytes, err := bReader.Peek(2)
	if err != nil {
		// return bReader, h.Error(err, "Error Peeking")
		return bReader, nil
	}

	// https://stackoverflow.com/a/28332019
	if testBytes[0] == 31 && testBytes[1] == 139 {
		// is gzip
		gReader, err = gzip.NewReader(bReader)
		if err != nil {
			return bReader, h.Error(err, "Error using gzip.NewReader")
		}
	} else {
		gReader = bReader
	}

	return gReader, err
}

type BaseCompressor struct {
	Compressor
	properties map[string]string
	context    h.Context
	cpType     CompressorType
}

// Context provides a pointer to context
func (cp *BaseCompressor) Context() (context *h.Context) {
	return &cp.context
}

// ZipCompressor is for Zip Compression
type ZipCompressor struct {
	BaseCompressor
	context   h.Context
	client    *sftp.Client
	sshClient *SSHClient
}
