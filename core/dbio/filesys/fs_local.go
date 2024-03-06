package filesys

import (
	"bufio"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// LocalFileSysClient is a file system client to write file to local file sys.
type LocalFileSysClient struct {
	BaseFileSysClient
	context g.Context
}

// Init initializes the fs client
func (fs *LocalFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return
}

func cleanLocalFilePath(path string) string {
	return strings.TrimPrefix(path, "file://")
}

// Delete deletes the given path (file or directory)
func (fs *LocalFileSysClient) delete(path string) (err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Stat(path)
	if err != nil {
		return nil // likely means the path does not exist, no need to delete
	}

	switch mode := file.Mode(); {
	case mode.IsDir():
		// some safety measure to not delete root system folders
		slashCount := len(path) - len(strings.ReplaceAll(path, "/", ""))
		if slashCount <= 2 && (strings.HasPrefix(path, "/") || strings.Contains(path, ":/")) {
			g.Warn("directory '%s' is close to root. Not deleting.", path)
			return
		}

		err = os.RemoveAll(path)
		if err != nil {
			err = g.Error(err, "Unable to delete "+path)
		}
	case mode.IsRegular():
		err = os.Remove(path)
		if err != nil {
			err = g.Error(err, "Unable to delete "+path)
		}
	}
	return
}

// GetReader return a reader for the given path
func (fs *LocalFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}

	return bufio.NewReader(file), err
}

// GetDatastream return a datastream for the given path
func (fs *LocalFileSysClient) GetDatastream(path string) (ds *iop.Datastream, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return nil, err
	}

	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = path
	ds.SetConfig(fs.Props())

	// set selectFields for pruning at source
	selectFields := []string{}
	g.Unmarshal(fs.GetProp("selectFields"), &selectFields)
	ds.Columns = iop.NewColumnsFromFields(selectFields...)

	if strings.Contains(strings.ToLower(path), ".xlsx") {
		g.Debug("reading datastream from %s", path)
		eDs, err := getExcelStream(fs.Self(), bufio.NewReader(file))
		if err != nil {
			err = g.Error(err, "Error consuming Excel reader")
			return ds, err
		}
		return eDs, nil
	}

	fileFormat := FileType(cast.ToString(fs.GetProp("FORMAT")))
	if string(fileFormat) == "" {
		fileFormat = InferFileFormat(path)
		fs.SetProp("FORMAT", string(fileFormat))
	}

	go func() {
		// recover from panic
		defer func() {
			if r := recover(); r != nil {
				err := g.Error("panic occurred! %#v\n%s", r, string(debug.Stack()))
				ds.Context.CaptureErr(err)
			}
		}()

		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		g.Debug("reading datastream from %s [format=%s]", path, fileFormat)

		switch fileFormat {
		case FileTypeJson, FileTypeJsonLines:
			err = ds.ConsumeJsonReader(bufio.NewReader(file))
		case FileTypeXml:
			err = ds.ConsumeXmlReader(bufio.NewReader(file))
		case FileTypeParquet:
			err = ds.ConsumeParquetReaderSeeker(file)
		case FileTypeAvro:
			err = ds.ConsumeAvroReaderSeeker(file)
		case FileTypeSAS:
			err = ds.ConsumeSASReaderSeeker(file)
		case FileTypeCsv:
			err = ds.ConsumeCsvReader(bufio.NewReader(file))
		default:
			g.Warn("LocalFileSysClient | File Format not recognized: %s. Using CSV parsing", fileFormat)
			err = ds.ConsumeCsvReader(bufio.NewReader(file))
		}

		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", path))
		}

	}()

	return ds, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *LocalFileSysClient) GetWriter(path string) (writer io.Writer, err error) {
	path = cleanLocalFilePath(path)
	file, err := os.Create(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}

// MkdirAll creates child directories
func (fs *LocalFileSysClient) MkdirAll(path string) (err error) {
	path = cleanLocalFilePath(path)
	return os.MkdirAll(path, 0755)
}

// Write creates the file if non-existent and writes from the reader
func (fs *LocalFileSysClient) Write(filePath string, reader io.Reader) (bw int64, err error) {
	filePath = cleanLocalFilePath(filePath)
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	// create folder if needed
	folderPath := path.Dir(filePath)
	if !g.PathExists(folderPath) {
		err = os.MkdirAll(folderPath, 0777)
		if err != nil {
			go io.Copy(io.Discard, reader)
			err = g.Error(err, "Unable to create folder "+folderPath)
			return
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		go io.Copy(io.Discard, reader)
		err = g.Error(err, "Unable to open "+filePath)
		return
	}
	defer file.Close()

	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// List lists the file in given directory path
func (fs *LocalFileSysClient) List(path string) (paths []string, err error) {
	path = cleanLocalFilePath(path)

	s, err := os.Stat(path)
	if err == nil && !s.IsDir() {
		return []string{path}, nil
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		err = g.Error(err, "Error listing "+path)
		return
	}

	for _, file := range files {
		// file.ModTime()
		paths = append(paths, "file://"+path+"/"+file.Name())
	}
	return
}

// ListRecursive lists the file in given directory path recursively
func (fs *LocalFileSysClient) ListRecursive(path string) (paths []string, err error) {
	path = cleanLocalFilePath(path)
	ts := fs.GetRefTs()

	walkFunc := func(subPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (ts.IsZero() || info.ModTime().IsZero() || info.ModTime().After(ts)) {
			paths = append(paths, "file://"+subPath)
		}
		return nil
	}
	err = filepath.Walk(path, walkFunc)
	if err != nil {
		err = g.Error(err, "Error listing "+path)
	}
	return
}
