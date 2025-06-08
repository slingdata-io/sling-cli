package filesys

import (
	"bufio"
	"context"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime/debug"
	"strings"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
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

// Prefix returns the url prefix
func (fs *LocalFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://", fs.FsType().String()) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *LocalFileSysClient) GetPath(uri string) (path string, err error) {
	uri = NormalizeURI(fs, uri)
	path = strings.TrimPrefix(uri, fs.Prefix())
	path = strings.TrimRight(path, makePathSuffix(path))
	return
}

// Delete deletes the given path (file or directory)
func (fs *LocalFileSysClient) delete(uri string) (err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	file, err := os.Stat(path)
	if err != nil {
		return nil // likely means the path does not exist, no need to delete
	}

	switch mode := file.Mode(); {
	case mode.IsDir():
		// some safety measure to not delete root system folders
		slashCount := len(path) - len(strings.ReplaceAll(path, "/", ""))
		if slashCount <= 2 && (strings.HasPrefix(path, "/") || strings.Contains(path, ":/")) {
			g.Warn("directory '%s' is close too root. Not deleting.", path)
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
func (fs *LocalFileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	file, err := os.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}

	return bufio.NewReader(file), err
}

// GetDatastream return a datastream for the given path
func (fs *LocalFileSysClient) GetDatastream(uri string, cfg ...iop.FileStreamConfig) (ds *iop.Datastream, err error) {
	Cfg := iop.FileStreamConfig{}
	if len(cfg) > 0 {
		Cfg = cfg[0]
	}

	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	var file *os.File
	if !Cfg.ShouldUseDuckDB() {
		file, err = os.Open(path)
		if err != nil {
			err = g.Error(err, "Unable to open "+path)
			return nil, err
		}
	}

	ds = iop.NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetMetadata(fs.GetProp("METADATA"))
	ds.Metadata.StreamURL.Value = path
	ds.SetConfig(fs.Props())

	// set selectFields for pruning at source
	ds.Columns = iop.NewColumnsFromFields(Cfg.Select...)

	if Cfg.Format == dbio.FileTypeNone {
		Cfg.Format = InferFileFormat(path)
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

		g.Debug("reading datastream from %s [format=%s]", path, Cfg.Format)

		// no reader for iceberg, delta, duckdb will handle it
		if Cfg.ShouldUseDuckDB() {
			file.Close() // no need to keep the file open
			Cfg.Props = map[string]string{"fs_props": g.Marshal(fs.Props())}
			switch Cfg.Format {
			case dbio.FileTypeIceberg:
				err = ds.ConsumeIcebergReader("file://"+path, Cfg)
			case dbio.FileTypeDelta:
				err = ds.ConsumeDeltaReader("file://"+path, Cfg)
			case dbio.FileTypeParquet:
				err = ds.ConsumeParquetReaderDuckDb("file://"+path, Cfg)
			case dbio.FileTypeCsv:
				err = ds.ConsumeCsvReaderDuckDb("file://"+path, Cfg)
			}
			if err != nil {
				ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", path))
			}
			return
		}

		switch Cfg.Format {
		case dbio.FileTypeJson, dbio.FileTypeJsonLines:
			err = ds.ConsumeJsonReader(bufio.NewReader(file))
		case dbio.FileTypeXml:
			err = ds.ConsumeXmlReader(bufio.NewReader(file))
		case dbio.FileTypeParquet:
			err = ds.ConsumeParquetReaderSeeker(file)
		case dbio.FileTypeArrow:
			err = ds.ConsumeArrowReaderSeeker(file)
		case dbio.FileTypeAvro:
			err = ds.ConsumeAvroReaderSeeker(file)
		case dbio.FileTypeSAS:
			err = ds.ConsumeSASReaderSeeker(file)
		case dbio.FileTypeExcel:
			err = ds.ConsumeExcelReaderSeeker(file, fs.properties)
		case dbio.FileTypeCsv:
			err = ds.ConsumeCsvReader(bufio.NewReader(file))
		default:
			g.Warn("LocalFileSysClient | File Format not recognized: %s. Using CSV parsing", Cfg.Format)
			err = ds.ConsumeCsvReader(bufio.NewReader(file))
		}

		if err != nil {
			ds.Context.CaptureErr(g.Error(err, "Error consuming reader for %s", path))
		}

	}()

	return ds, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *LocalFileSysClient) GetWriter(uri string) (writer io.Writer, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	file, err := os.Create(path)
	if err != nil {
		err = g.Error(err, "Unable to create "+path)
		return
	}
	writer = io.Writer(file)
	return
}

// MkdirAll creates child directories
func (fs *LocalFileSysClient) MkdirAll(uri string) (err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	return os.MkdirAll(path, 0755)
}

// Write creates the file if non-existent and writes from the reader
func (fs *LocalFileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	filePath, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

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
func (fs *LocalFileSysClient) List(uri string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	s, err := os.Stat(path)
	if err == nil && (!s.IsDir() || !strings.HasSuffix(path, "/")) {
		node := FileNode{
			URI:     "file://" + path,
			Updated: s.ModTime().Unix(),
			Size:    cast.ToUint64(s.Size()),
			IsDir:   s.IsDir(),
		}
		nodes.Add(node)
		return
	}

	if path == "" {
		path = "/"
	}

	files, err := os.ReadDir(path)
	if err != nil {
		return nodes, nil // should not error if path doesn't exist
	}

	// path is dir
	if path != "" {
		path = strings.TrimSuffix(path, "/")
	}

	for _, file := range files {
		fInfo, _ := file.Info()
		node := FileNode{
			URI:     "file://" + path + "/" + file.Name(),
			Updated: fInfo.ModTime().Unix(),
			Size:    cast.ToUint64(fInfo.Size()),
			IsDir:   file.IsDir(),
		}
		node.URI = strings.ReplaceAll(node.URI, `\`, "/")
		nodes.AddWhere(pattern, 0, node)
	}
	return
}

// ListRecursive lists the file in given directory path recursively
func (fs *LocalFileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	ts := fs.GetRefTs().Unix()

	walkFunc := func(subPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		subPath = strings.ReplaceAll(subPath, `\`, "/")
		node := FileNode{
			URI:     "file://" + subPath,
			Updated: info.ModTime().Unix(),
			Size:    cast.ToUint64(info.Size()),
			IsDir:   info.IsDir(),
		}
		if !info.IsDir() {
			nodes.AddWhere(pattern, ts, node)
		}
		return nil
	}

	err = filepath.Walk(path, walkFunc)
	if err != nil {
		err = g.Error(err, "Error listing "+path)
	}
	return
}
