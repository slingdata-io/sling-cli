package iop

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/minio/minio-go"
	"github.com/slingdata/sling/core/env"

	gcstorage "cloud.google.com/go/storage"
	azstorage "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	h "github.com/flarco/gutil"
	"github.com/pkg/sftp"
	"github.com/spf13/cast"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// FileSysClient is a client to a file systems
// such as local, s3, hdfs, azure storage, google cloud storage
type FileSysClient interface {
	Self() FileSysClient
	Init(ctx context.Context) (err error)
	Client() *BaseFileSysClient
	Context() (context *h.Context)
	FsType() FileSysType
	Delete(path string) (err error)
	GetReader(path string) (reader io.Reader, err error)
	GetReaders(paths ...string) (readers []io.Reader, err error)
	GetDatastream(path string) (ds *Datastream, err error)
	GetWriter(path string) (writer io.Writer, err error)
	List(path string) (paths []string, err error)
	ListRecursive(path string) (paths []string, err error)
	Write(path string, reader io.Reader) (bw int64, err error)

	ReadDataflow(url string) (df *Dataflow, err error)
	WriteDataflow(df *Dataflow, url string) (bw int64, err error)
	WriteDataflowReady(df *Dataflow, url string, fileReadyChn chan string) (bw int64, err error)
	GetProp(key string) (val string)
	SetProp(key string, val string)
	MkdirAll(path string) (err error)
}

// FileSysType is an int type for enum for the File system Type
type FileSysType int

const (
	// LocalFileSys is local file system
	LocalFileSys FileSysType = iota
	// HDFSFileSys is HDFS file system
	HDFSFileSys
	// S3FileSys is S3 file system
	S3FileSys
	// S3cFileSys is S3 compatible file system
	S3cFileSys
	// AzureFileSys is Azure file system
	AzureFileSys
	// GoogleFileSys is Google file system
	GoogleFileSys
	// SftpFileSys is SFTP / SCP / SSH file system
	SftpFileSys
	// HTTPFileSys is HTTP / HTTPS file system
	HTTPFileSys
)

const defaultConcurencyLimit = 10

// NewFileSysClient create a file system client
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClient(fst FileSysType, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientContext(context.Background(), fst, props...)
}

// NewFileSysClientContext create a file system client with context
// such as local, s3, azure storage, google cloud storage
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientContext(ctx context.Context, fst FileSysType, props ...string) (fsClient FileSysClient, err error) {
	concurencyLimit := defaultConcurencyLimit
	if os.Getenv("SLINGELT_CONCURENCY_LIMIT") != "" {
		concurencyLimit = cast.ToInt(os.Getenv("SLINGELT_CONCURENCY_LIMIT"))
	}

	switch fst {
	case LocalFileSys:
		fsClient = &LocalFileSysClient{}
		concurencyLimit = 20
		fsClient.Client().fsType = LocalFileSys
	case S3FileSys:
		fsClient = &S3FileSysClient{}
		fsClient.Client().fsType = S3FileSys
	case S3cFileSys:
		fsClient = &S3cFileSysClient{}
		fsClient.Client().fsType = S3cFileSys
	case SftpFileSys:
		fsClient = &SftpFileSysClient{}
		fsClient.Client().fsType = SftpFileSys
	// case HDFSFileSys:
	// 	fsClient = fsClient
	case AzureFileSys:
		fsClient = &AzureFileSysClient{}
		fsClient.Client().fsType = AzureFileSys
	case GoogleFileSys:
		fsClient = &GoogleFileSysClient{}
		fsClient.Client().fsType = GoogleFileSys
	case HTTPFileSys:
		fsClient = &HTTPFileSysClient{}
		fsClient.Client().fsType = HTTPFileSys
	default:
		err = h.Error(errors.New("Unrecognized File System"), "")
		return
	}

	// set properties
	for k, v := range h.KVArrToMap(props...) {
		fsClient.SetProp(k, v)
	}

	if fsClient.GetProp("SLINGELT_CONCURENCY_LIMIT") != "" {
		concurencyLimit = cast.ToInt(fsClient.GetProp("SLINGELT_CONCURENCY_LIMIT"))
	}

	for k, v := range env.EnvVars() {
		if fsClient.GetProp(k) == "" {
			fsClient.SetProp(k, v)
		}
	}

	// Init Limit
	err = fsClient.Init(ctx)
	if err != nil {
		err = h.Error(err, "Error initiating File Sys Client")
	}
	fsClient.Context().SetConcurencyLimit(concurencyLimit)

	return
}

// NewFileSysClientFromURL returns the proper fs client for the given path
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURL(url string, props ...string) (fsClient FileSysClient, err error) {
	return NewFileSysClientFromURLContext(context.Background(), url, props...)
}

// NewFileSysClientFromURLContext returns the proper fs client for the given path with context
// props are provided as `"Prop1=Value1", "Prop2=Value2", ...`
func NewFileSysClientFromURLContext(ctx context.Context, url string, props ...string) (fsClient FileSysClient, err error) {
	switch {
	case strings.HasPrefix(url, "s3://"):
		if v, ok := h.KVArrToMap(props...)["AWS_ENDPOINT"]; ok && v != "" {
			return NewFileSysClientContext(ctx, S3FileSys, props...)
		}
		return NewFileSysClientContext(ctx, S3FileSys, props...)
	case strings.HasPrefix(url, "sftp://"):
		props = append(props, "SFTP_URL="+url)
		return NewFileSysClientContext(ctx, SftpFileSys, props...)
	case strings.HasPrefix(url, "gs://"):
		return NewFileSysClientContext(ctx, GoogleFileSys, props...)
	case strings.Contains(url, ".core.windows.net") || strings.HasPrefix(url, "azure://"):
		return NewFileSysClientContext(ctx, AzureFileSys, props...)
	case strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://"):
		return NewFileSysClientContext(ctx, HTTPFileSys, props...)
	case strings.Contains(url, "://"):
		err = h.Error(
			fmt.Errorf("Unable to determine FileSysClient for "+url),
			"",
		)
		return
	default:
		fsClient = &LocalFileSysClient{}
		props = append(props, h.F("concurencyLimit=%d", 20))
		return NewFileSysClientContext(ctx, LocalFileSys, props...)
	}
}

// ParseURL parses a URL
func ParseURL(urlStr string) (host string, path string, err error) {

	u, err := url.Parse(urlStr)
	if err != nil {
		err = h.Error(err, "Unable to parse URL "+urlStr)
		return
	}

	scheme := u.Scheme
	host = u.Hostname()
	path = u.Path

	if scheme == "" || host == "" {
		err = h.Error(errors.New("Invalid URL: "+urlStr), "")
	}

	return
}

func getExcelStream(fs FileSysClient, reader io.Reader) (ds *Datastream, err error) {
	xls, err := NewExcelFromReader(reader)
	if err != nil {
		err = h.Error(err, "Unable to open Excel File from reader")
		return nil, err
	}
	xls.properties = fs.Client().properties

	sheetName := fs.GetProp("sheet")
	if sheetName == "" {
		sheetName = xls.Sheets[0]
	}

	sheetRange := fs.GetProp("range")
	if sheetRange != "" {
		data, err := xls.GetDatasetFromRange(sheetName, sheetRange)
		if err != nil {
			err = h.Error(err, "Unable to get range data for %s!%s", sheetName, sheetRange)
			return nil, err
		}
		ds = data.Stream()
	} else {
		data := xls.GetDataset(sheetName)
		ds = data.Stream()
	}
	return ds, nil
}

////////////////////// BASE

// BaseFileSysClient is the base file system type.
type BaseFileSysClient struct {
	FileSysClient
	properties map[string]string
	instance   *FileSysClient
	context    h.Context
	fsType     FileSysType
}

// Context provides a pointer to context
func (fs *BaseFileSysClient) Context() (context *h.Context) {
	return &fs.context
}

// Client provides a pointer to itself
func (fs *BaseFileSysClient) Client() *BaseFileSysClient {
	return fs
}

// Instance returns the respective connection Instance
// This is useful to refer back to a subclass method
// from the superclass level. (Aka overloading)
func (fs *BaseFileSysClient) Self() FileSysClient {
	return *fs.instance
}

// FsType return the type of the client
func (fs *BaseFileSysClient) FsType() FileSysType {
	return fs.fsType
}

// GetProp returns the value of a property
func (fs *BaseFileSysClient) GetProp(key string) string {
	return fs.properties[key]
}

// SetProp sets the value of a property
func (fs *BaseFileSysClient) SetProp(key string, val string) {
	if fs.properties == nil {
		fs.properties = map[string]string{}
	}
	fs.properties[key] = val
}

// GetDatastream return a datastream for the given path
func (fs *BaseFileSysClient) GetDatastream(urlStr string) (ds *Datastream, err error) {

	ds = NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetConfig(fs.properties)

	if strings.Contains(strings.ToLower(urlStr), ".xlsx") {
		reader, err := fs.Self().GetReader(urlStr)
		if err != nil {
			err = h.Error(err, "Error getting Excel reader")
			return ds, err
		}

		// Wait for reader to start reading or err
		for {
			// Try peeking
			if b := bufio.NewReader(reader).Size(); b > 0 {
				// h.P(b)
				break
			}

			if fs.Context().Err() != nil {
				// has errorred
				return ds, fs.Context().Err()
			}
			time.Sleep(50 * time.Millisecond)
		}

		eDs, err := getExcelStream(fs.Self(), reader)
		if err != nil {
			err = h.Error(err, "Error consuming Excel reader")
			return ds, err
		}
		return eDs, nil
	}

	go func() {
		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		reader, err := fs.Self().GetReader(urlStr)
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error getting reader"))
			h.LogError(fs.Context().Err())
			fs.Context().Cancel()
			return
		}

		// Wait for reader to start reading or err
		for {
			// Try peeking
			if b := bufio.NewReader(reader).Size(); b > 0 {
				// h.P(b)
				break
			}

			if fs.Context().Err() != nil {
				// has errorred
				return
			}
			time.Sleep(50 * time.Millisecond)
		}

		err = ds.ConsumeReader(reader)
		if err != nil {
			ds.Context.CaptureErr(h.Error(err, "Error consuming reader"))
			ds.Context.Cancel()
			fs.Context().CaptureErr(h.Error(err, "Error consuming reader"))
			// fs.Context().Cancel()
			h.LogError(fs.Context().Err())
		}

	}()

	return ds, err
}

// ReadDataflow read
func (fs *BaseFileSysClient) ReadDataflow(url string) (df *Dataflow, err error) {
	if strings.HasSuffix(strings.ToLower(url), ".zip") {
		localFs, err := NewFileSysClient(LocalFileSys)
		if err != nil {
			return df, h.Error(err, "could not initialize localFs")
		}

		reader, err := fs.Self().GetReader(url)
		if err != nil {
			return df, h.Error(err, "could not get zip reader")
		}

		folderPath, err := ioutil.TempDir("", "sling_temp_")
		if err != nil {
			return df, h.Error(err, "could not get create temp file")
		}

		zipPath := folderPath + ".zip"
		_, err = localFs.Write(zipPath, reader)
		if err != nil {
			return df, h.Error(err, "could not write to "+zipPath)
		}

		paths, err := Unzip(zipPath, folderPath)
		if err != nil {
			return df, h.Error(err, "Error unzipping")
		}
		// delete zip file
		os.RemoveAll(zipPath)

		// TODO: handle multiple files, yielding multiple schemas
		df, err = GetDataflow(localFs.Self(), paths...)
		if err != nil {
			return df, h.Error(err, "Error making dataflow")
		}

		// delete unzipped folder when done
		df.Defer(func() { os.RemoveAll(folderPath) })

		return df, nil
	}

	paths, err := fs.Self().ListRecursive(url)
	if err != nil {
		err = h.Error(err, "Error getting paths")
		return
	}
	df, err = GetDataflow(fs.Self(), paths...)
	if err != nil {
		err = h.Error(err, "Error getting dataflow")
		return
	}

	df.FsURL = url
	return
}

// WriteDataflow writes a dataflow to a file sys.
func (fs *BaseFileSysClient) WriteDataflow(df *Dataflow, url string) (bw int64, err error) {
	// handle excel file here, generate reader
	if strings.Contains(strings.ToLower(url), ".xlsx") {
		xls := NewExcel()

		sheetName := fs.GetProp("sheet")
		if sheetName == "" {
			sheetName = "Sheet1"
		}

		err = xls.WriteSheet(sheetName, MergeDataflow(df), "overwrite")
		if err != nil {
			err = h.Error(err, "error writing to excel file")
			return
		}

		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			err = xls.WriteToWriter(pw)
			if err != nil {
				h.LogError(err, "error writing to excel file")
			}
		}()

		bw, err = fs.Self().Write(url, pr)
		return
	}

	fileReadyChn := make(chan string, 10000)

	h.Trace("writing dataflow to %s", url)
	go func() {
		bw, err = fs.Self().WriteDataflowReady(df, url, fileReadyChn)
	}()

	for range fileReadyChn {
		// do nothing, wait for completion
	}

	return
}

// GetReaders returns one or more readers from specified paths in specified FileSysClient
func (fs *BaseFileSysClient) GetReaders(paths ...string) (readers []io.Reader, err error) {
	if len(paths) == 0 {
		err = fmt.Errorf("Provided 0 files for: %#v", paths)
		return
	}

	for _, path := range paths {
		reader, err := fs.Self().GetReader(path)
		if err != nil {
			return nil, h.Error(err, "Unable to process "+path)
		}
		readers = append(readers, reader)
	}

	return readers, nil
}

// WriteDataflowReady writes to a file sys and notifies the fileReady chan.
func (fs *BaseFileSysClient) WriteDataflowReady(df *Dataflow, url string, fileReadyChn chan string) (bw int64, err error) {
	fsClient := fs.Self()
	defer close(fileReadyChn)
	gzip := strings.ToUpper(fs.GetProp("SLINGELT_COMPRESSION")) == "GZIP"
	fileRowLimit := cast.ToInt(fs.GetProp("SLINGELT_FILE_ROW_LIMIT"))

	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	singleFile := fileRowLimit == 0 && len(df.Streams) == 1

	processStream := func(ds *Datastream, partURL string) {
		defer df.Context.Wg.Read.Done()
		conc := runtime.NumCPU()
		if conc > 7 {
			conc = 7
		}
		localCtx := h.NewContext(ds.Context.Ctx, conc)

		writePart := func(reader io.Reader, partURL string) {
			defer localCtx.Wg.Read.Done()

			bw0, err := fsClient.Write(partURL, reader)
			fileReadyChn <- partURL
			if err != nil {
				df.Context.CaptureErr(h.Error(err))
				ds.Context.Cancel()
				df.Context.Cancel()
			} else {
				h.Trace("wrote %s to %s", humanize.Bytes(cast.ToUint64(bw0)), partURL)
				bw += bw0
			}
		}

		// pre-add to WG to not hold next reader in memory while waiting
		localCtx.Wg.Read.Add()
		fileCount := 0
		for reader := range ds.NewCsvBufferReaderChnl(fileRowLimit) {
			// for reader := range ds.NewCsvReaderChnl(fileRowLimit) {
			fileCount++
			subPartURL := fmt.Sprintf("%s.%04d.csv", partURL, fileCount)
			if singleFile {
				subPartURL = partURL
				if strings.HasSuffix(partURL, ".gz") {
					gzip = true
					partURL = strings.TrimSuffix(partURL, ".gz")
				}
			}

			if gzip {
				gzReader := Compress(reader)
				subPartURL = subPartURL + ".gz"
				h.Trace("writing compressed stream to " + subPartURL)
				go writePart(gzReader, subPartURL)
			} else {
				go writePart(reader, subPartURL)
			}
			localCtx.Wg.Read.Add()

			if df.Err() != nil {
				break
			}
		}
		if ds.Err() != nil {
			df.Context.CaptureErr(h.Error(ds.Err()))
		}
		localCtx.Wg.Read.Done() // clear that pre-added WG
		localCtx.Wg.Read.Wait()
	}

	err = fsClient.Delete(url)
	if err != nil {
		err = h.Error(err, "Could not delete url")
		return
	}

	if !singleFile && (fsClient.FsType() == LocalFileSys || fsClient.FsType() == SftpFileSys) {
		err = fsClient.MkdirAll(url)
		if err != nil {
			err = h.Error(err, "could not create directory")
			return
		}
	}

	partCnt := 1
	for ds := range df.StreamCh {
		partURL := fmt.Sprintf("%s/part.%02d", url, partCnt)
		if singleFile {
			partURL = url
		}
		if fsClient.FsType() == AzureFileSys {
			partURL = fmt.Sprintf("%s/part.%02d", url, partCnt)
		}
		h.Trace("starting process to write %s with file row limit %d", partURL, fileRowLimit)

		df.Context.Wg.Read.Add()
		go processStream(ds, partURL)
		partCnt++
	}

	df.Context.Wg.Read.Wait()
	if df.Context.Err() != nil {
		err = h.Error(df.Context.Err())
	}

	return
}

////////////////////// LOCAL

// LocalFileSysClient is a file system client to write file to local file sys.
type LocalFileSysClient struct {
	BaseFileSysClient
	context h.Context
}

// Init initializes the fs client
func (fs *LocalFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)
	return
}

// Delete deletes the given path (file or directory)
func (fs *LocalFileSysClient) Delete(path string) (err error) {

	file, err := os.Stat(path)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			// path is already deleted
			return nil
		}

		err = h.Error(err, "Unable to Stat "+path)
		return
	}

	switch mode := file.Mode(); {
	case mode.IsDir():
		err = os.RemoveAll(path)
		if err != nil {
			err = h.Error(err, "Unable to delete "+path)
		}
	case mode.IsRegular():
		err = os.Remove(path)
		if err != nil {
			err = h.Error(err, "Unable to delete "+path)
		}
	}
	return
}

// GetReader return a reader for the given path
func (fs *LocalFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	file, err := os.Open(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}

	return bufio.NewReader(file), err
}

// GetDatastream return a datastream for the given path
func (fs *LocalFileSysClient) GetDatastream(path string) (ds *Datastream, err error) {
	file, err := os.Open(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return nil, err
	}

	ds = NewDatastreamContext(fs.Context().Ctx, nil)
	ds.SafeInference = true
	ds.SetConfig(fs.BaseFileSysClient.properties)

	if strings.Contains(strings.ToLower(path), ".xlsx") {
		eDs, err := getExcelStream(fs.Self(), bufio.NewReader(file))
		if err != nil {
			err = h.Error(err, "Error consuming Excel reader")
			return ds, err
		}
		return eDs, nil
	}

	go func() {
		// manage concurrency
		defer fs.Context().Wg.Read.Done()
		fs.Context().Wg.Read.Add()

		reader := bufio.NewReader(file)

		err = ds.ConsumeReader(reader)
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error consuming reader"))
			fs.Context().Cancel()
			h.LogError(fs.Context().Err())
		}

	}()

	return ds, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *LocalFileSysClient) GetWriter(path string) (writer io.Writer, err error) {
	file, err := os.Create(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}

// MkdirAll creates child directories
func (fs *LocalFileSysClient) MkdirAll(path string) (err error) {
	return os.MkdirAll(path, 0655)
}

// Write creates the file if non-existent and writes from the reader
func (fs *LocalFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	file, err := os.Create(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}
	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = h.Error(err, "Error writing from reader")
	}
	return
}

// List lists the file in given directory path
func (fs *LocalFileSysClient) List(path string) (paths []string, err error) {

	files, err := ioutil.ReadDir("./")
	if err != nil {
		err = h.Error(err, "Error listing "+path)
		return
	}

	for _, file := range files {
		paths = append(paths, path+"/"+file.Name())
	}
	return
}

// ListRecursive lists the file in given directory path recursively
func (fs *LocalFileSysClient) ListRecursive(path string) (paths []string, err error) {

	paths, err = filepath.Glob(path)
	// err = filepath.Walk(path, func(subPath string, info os.FileInfo, err error) error {
	// 	if !info.IsDir() {
	// 		paths = append(paths, subPath)
	// 	}
	// 	return nil
	// })
	if err != nil {
		err = h.Error(err, "Error listing "+path)
	}
	return
}

///////////////////////////// S3

// S3FileSysClient is a file system client to write file to Amazon's S3 file sys.
type S3FileSysClient struct {
	BaseFileSysClient
	context   h.Context
	bucket    string
	session   *session.Session
	RegionMap map[string]string
	mux       sync.Mutex
}

// Init initializes the fs client
func (fs *S3FileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)

	fs.bucket = fs.GetProp("AWS_BUCKET")
	if fs.bucket == "" {
		fs.bucket = os.Getenv("AWS_BUCKET")
	}
	fs.RegionMap = map[string]string{}

	return fs.Connect()
}

const defaultRegion = "us-east-1"

type fakeWriterAt struct {
	w io.Writer
}

func (fw fakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)]
	}
	return key
}

// Connect initiates the Google Cloud Storage client
func (fs *S3FileSysClient) Connect() (err error) {

	if fs.GetProp("AWS_ACCESS_KEY_ID") == "" || fs.GetProp("AWS_SECRET_ACCESS_KEY") == "" {
		return h.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to interact with S3"))
	}

	region := fs.GetProp("AWS_REGION")
	endpoint := fs.GetProp("AWS_ENDPOINT")

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	fs.session, err = session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			fs.GetProp("AWS_ACCESS_KEY_ID"),
			fs.GetProp("AWS_SECRET_ACCESS_KEY"),
			fs.GetProp("AWS_SESSION_TOKEN"),
		),
		Region:                         aws.String(region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		Endpoint:                       aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	})
	if err != nil {
		err = h.Error(err, "Could not create AWS session")
		return
	}

	return
}

// getSession returns the session and sets the region based on the bucket
func (fs *S3FileSysClient) getSession() (sess *session.Session) {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	endpoint := fs.GetProp("AWS_ENDPOINT")
	if strings.HasSuffix(endpoint, ".digitaloceanspaces.com") {
		region := strings.TrimRight(endpoint, ".digitaloceanspaces.com")
		region = strings.TrimLeft(endpoint, "https://")
		fs.RegionMap[fs.bucket] = region
	} else if fs.RegionMap[fs.bucket] == "" && fs.bucket != "" {
		region, err := s3manager.GetBucketRegion(fs.Context().Ctx, fs.session, fs.bucket, defaultRegion)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				h.Debug("unable to find bucket %s's region not found", fs.bucket)
				h.Debug("Region not found for " + fs.bucket)
			} else {
				h.Debug(h.Error(err, "Error getting Region for "+fs.bucket).Error())
			}
		} else {
			fs.RegionMap[fs.bucket] = region
		}
	}
	fs.session.Config.Region = aws.String(fs.RegionMap[fs.bucket])

	return fs.session
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) Delete(path string) (err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	// Create S3 service client
	svc := s3.New(fs.getSession())

	paths, err := fs.ListRecursive(path)
	if err != nil {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, subPath := range paths {
		_, subPath, err = ParseURL(subPath)
		if err != nil {
			err = h.Error(err, "Error Parsing url: "+path)
			return
		}
		subPath = cleanKey(subPath)
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(subPath)})
	}

	if len(objects) == 0 {
		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}
	_, err = svc.DeleteObjectsWithContext(fs.Context().Ctx, input)

	if err != nil {
		return h.Error(err, fmt.Sprintf("Unable to delete S3 objects:\n%#v", input))
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return h.Error(err, "error WaitUntilObjectNotExists")
	}

	return
}

// GetReader return a reader for the given path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt` or `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(fs.getSession())
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		// Write the contents of S3 Object to the file
		_, err := downloader.DownloadWithContext(
			fs.Context().Ctx,
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error downloading S3 File -> "+key))
			h.LogError(fs.Context().Err())
			fs.Context().Cancel()
			return
		}
	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) GetWriter(path string) (writer io.Writer, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	uploader := s3manager.NewUploader(fs.getSession())
	uploader.Concurrency = fs.Context().Wg.Limit

	pipeR, pipeW := io.Pipe()

	fs.Context().Wg.Write.Add()
	go func() {
		// manage concurrency
		defer fs.Context().Wg.Write.Done()
		defer pipeR.Close()

		// Upload the file to S3.
		_, err := uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   pipeR,
		})
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error uploading S3 File -> "+key))
			h.LogError(fs.Context().Err())
			fs.Context().Cancel()
		}
	}()

	writer = pipeW

	return
}

func (fs *S3FileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	uploader := s3manager.NewUploader(fs.getSession())
	uploader.Concurrency = fs.Context().Wg.Limit

	// Create pipe to get bytes written
	pr, pw := io.Pipe()
	fs.Context().Wg.Write.Add()
	go func() {
		defer fs.Context().Wg.Write.Done()
		defer pw.Close()
		bw, err = io.Copy(pw, reader)
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error Copying"))
		}
	}()

	// Upload the file to S3.
	_, err = uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   pr,
	})
	if err != nil {
		err = h.Error(err, "failed to upload file: "+key)
		return
	}

	err = fs.Context().Err()

	return
}

// List lists the file in given directory path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	urlPrefix := fmt.Sprintf("s3://%s/", bucket)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(key),
		Delimiter: aws.String("/"),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	return fs.doList(svc, input, urlPrefix)
}

// ListRecursive lists the file in given directory path recusively
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	urlPrefix := fmt.Sprintf("s3://%s/", bucket)
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	return fs.doList(svc, input, urlPrefix)
}

func (fs *S3FileSysClient) doList(svc *s3.S3, input *s3.ListObjectsV2Input, urlPrefix string) (paths []string, err error) {

	result, err := svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
	if err != nil {
		err = h.Error(err, fmt.Sprintf("Error with ListObjectsV2 for: %#v", input))
		return paths, err
	}

	prefixes := []string{}
	for {

		for _, cp := range result.CommonPrefixes {
			prefixes = append(prefixes, urlPrefix+*cp.Prefix)
		}

		for _, obj := range result.Contents {
			paths = append(paths, urlPrefix+*obj.Key)
		}

		if *result.IsTruncated {
			input.SetContinuationToken(*result.NextContinuationToken)
			result, err = svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
			if err != nil {
				err = h.Error(err, fmt.Sprintf("Error with ListObjectsV2 for: %#v", input))
				return paths, err
			}
		} else {
			break
		}
	}

	sort.Strings(prefixes)
	sort.Strings(paths)
	paths = append(prefixes, paths...)
	return
}

///////////////////////////// S3 compatible

// S3cFileSysClient is a file system client to write file to Amazon's S3 file sys.
type S3cFileSysClient struct {
	BaseFileSysClient
	context   h.Context
	bucket    string
	client    *minio.Client
	RegionMap map[string]string
	mux       sync.Mutex
}

// Init initializes the fs client
func (fs *S3cFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)

	fs.bucket = fs.GetProp("AWS_BUCKET")
	if fs.bucket == "" {
		fs.bucket = os.Getenv("AWS_BUCKET")
	}
	fs.RegionMap = map[string]string{}

	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *S3cFileSysClient) Connect() (err error) {

	if fs.GetProp("AWS_ACCESS_KEY_ID") == "" || fs.GetProp("AWS_SECRET_ACCESS_KEY") == "" {
		return h.Error(errors.New("Need to set 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' to interact with S3"))
	}

	endpoint := fs.GetProp("AWS_ENDPOINT")

	fs.client, err = minio.New(endpoint, fs.GetProp("AWS_ACCESS_KEY_ID"), fs.GetProp("AWS_SECRET_ACCESS_KEY"), true)
	if err != nil {
		err = h.Error(err, "Could not create S3 session")
		return
	}

	return
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3cFileSysClient) Delete(path string) (err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	objectsCh := make(chan string)

	// Send object names that are needed to be removed to objectsCh
	go func() {

		defer close(objectsCh)
		doneCh := make(chan struct{})

		// Indicate to our routine to exit cleanly upon return.
		defer close(doneCh)

		// List all objects from a bucket-name with a matching prefix.
		for object := range fs.client.ListObjects(bucket, key, true, doneCh) {
			if object.Err != nil {
				log.Fatalln(object.Err)
			}
			objectsCh <- object.Key
		}
	}()

	// Call RemoveObjects API
	errorCh := fs.client.RemoveObjects(bucket, objectsCh)

	// Print errors received from RemoveObjects API
	for e := range errorCh {
		err = h.Error(e.Err, "could not delete file")
		return
	}

	return
}

// List lists the file in given directory path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	urlPrefix := fmt.Sprintf("s3://%s/", bucket)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// List all objects from a bucket-name with a matching prefix.
	for object := range fs.client.ListObjectsV2(bucket, key, false, doneCh) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return
		}
		// fmt.Println(object)
		paths = append(paths, urlPrefix+object.Key)
	}
	return
}

// ListRecursive lists the file in given directory path recusively
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	urlPrefix := fmt.Sprintf("s3://%s/", bucket)

	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	// List all objects from a bucket-name with a matching prefix.
	for object := range fs.client.ListObjectsV2(bucket, key, true, doneCh) {
		if object.Err != nil {
			fmt.Println(object.Err)
			return
		}
		paths = append(paths, urlPrefix+object.Key)
	}
	return
}

// GetReader return a reader for the given path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt` or `s3://my_bucket/key/to/directory`
func (fs *S3cFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)

	reader, err = fs.client.GetObjectWithContext(fs.Context().Ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		err = h.Error(err, "failed to download file: "+key)
		return
	}

	return
}

func (fs *S3cFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	fs.bucket = bucket
	key = cleanKey(key)
	size := int64(-1)

	bw, err = fs.client.PutObjectWithContext(fs.Context().Ctx, bucket, key, reader, size, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		err = h.Error(err, "failed to upload file: "+key)
		return
	}

	err = fs.Context().Err()

	return
}

///////////////////////////// GC

// GoogleFileSysClient is a file system client to write file to Amazon's S3 file sys.
type GoogleFileSysClient struct {
	BaseFileSysClient
	client  *gcstorage.Client
	context h.Context
	bucket  string
}

// Init initializes the fs client
func (fs *GoogleFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)

	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *GoogleFileSysClient) Connect() (err error) {
	var authOpthion option.ClientOption

	if val := fs.GetProp("GC_CRED_JSON_BODY"); val != "" {
		authOpthion = option.WithCredentialsJSON([]byte(val))
	} else if val := fs.GetProp("GC_CRED_API_KEY"); val != "" {
		authOpthion = option.WithAPIKey(val)
	} else if val := fs.GetProp("GC_CRED_FILE"); val != "" {
		authOpthion = option.WithCredentialsFile(val)
	} else {
		return h.Error(fmt.Errorf("Could not connect. Did not provide GC_CRED_JSON_BODY, GC_CRED_API_KEY or GC_CRED_FILE"), "")
	}

	fs.bucket = fs.GetProp("GC_BUCKET")

	fs.client, err = gcstorage.NewClient(fs.Context().Ctx, authOpthion)
	if err != nil {
		err = h.Error(err, "Could not connect to GS Storage")
		return
	}

	return nil
}

func (fs *GoogleFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)

	obj := fs.client.Bucket(bucket).Object(key)
	wc := obj.NewWriter(fs.Context().Ctx)
	bw, err = io.Copy(wc, reader)
	if err != nil {
		err = h.Error(err, "Error Copying")
		return
	}

	if err = wc.Close(); err != nil {
		err = h.Error(err, "Error Closing writer")
		return
	}
	return
}

// GetReader returns the reader for the given path
func (fs *GoogleFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)
	reader, err = fs.client.Bucket(bucket).Object(key).NewReader(fs.Context().Ctx)
	if err != nil {
		err = h.Error(err, "Could not get reader for "+path)
		return
	}
	return
}

// List returns the list of objects
func (fs *GoogleFileSysClient) List(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)
	keyArr := strings.Split(key, "/")

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name"})
	it := fs.client.Bucket(bucket).Objects(fs.Context().Ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = h.Error(err, "Error Iterating")
			return paths, err
		}
		if attrs.Name == "" {
			continue
		}
		if len(strings.Split(attrs.Name, "/")) == len(keyArr)+1 {
			paths = append(paths, h.F("gs://%s/%s", bucket, attrs.Name))
		}
	}
	return
}

// ListRecursive returns the list of objects recursively
func (fs *GoogleFileSysClient) ListRecursive(path string) (paths []string, err error) {
	bucket, key, err := ParseURL(path)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+path)
		return
	}
	key = cleanKey(key)

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name"})
	it := fs.client.Bucket(bucket).Objects(fs.Context().Ctx, query)
	for {
		attrs, err := it.Next()
		// h.P(attrs)
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = h.Error(err, "Error Iterating")
			return paths, err
		}
		if attrs.Name == "" {
			continue
		}
		paths = append(paths, h.F("gs://%s/%s", bucket, attrs.Name))
	}
	return
}

// Delete list objects in path
func (fs *GoogleFileSysClient) Delete(urlStr string) (err error) {
	bucket, key, err := ParseURL(urlStr)
	if err != nil || bucket == "" {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	key = cleanKey(key)
	urlStrs, err := fs.ListRecursive(urlStr)
	if err != nil {
		err = h.Error(err, "Error List from url: "+urlStr)
		return
	}

	delete := func(key string) {
		defer fs.Context().Wg.Write.Done()
		o := fs.client.Bucket(bucket).Object(key)
		if err = o.Delete(fs.Context().Ctx); err != nil {
			if strings.Contains(err.Error(), "doesn't exist") {
				h.Debug("tried to delete %s\n%s", urlStr, err.Error())
				err = nil
			} else {
				err = h.Error(err, "Could not delete "+urlStr)
				fs.Context().CaptureErr(err)
			}
		}
	}

	for _, path := range urlStrs {
		bucket, key, err = ParseURL(path)
		if err != nil || bucket == "" {
			err = h.Error(err, "Error Parsing url: "+path)
			return
		}
		key = cleanKey(key)
		fs.Context().Wg.Write.Add()
		go delete(key)
	}
	fs.Context().Wg.Write.Wait()
	if fs.Context().Err() != nil {
		err = h.Error(fs.Context().Err(), "Could not delete "+urlStr)
	}
	return
}

///////////////////////////// Azure

// AzureFileSysClient is a file system client to write file to Microsoft's Azure file sys.
type AzureFileSysClient struct {
	BaseFileSysClient
	client  azstorage.Client
	context h.Context
}

// Init initializes the fs client
func (fs *AzureFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)
	return fs.Connect()
}

// Connect initiates the fs client connection
func (fs *AzureFileSysClient) Connect() (err error) {

	if cs := fs.GetProp("AZURE_CONN_STR"); cs != "" {
		connProps := h.KVArrToMap(strings.Split(cs, ";")...)
		fs.SetProp("AZURE_ACCOUNT", connProps["AccountName"])
		fs.SetProp("AZURE_KEY", connProps["AccountKey"])
		fs.client, err = azstorage.NewClientFromConnectionString(cs)
		if err != nil {
			err = h.Error(err, "Could not connect using provided AZURE_CONN_STR")
			return
		}
	} else if cs := fs.GetProp("AZURE_SAS_SVC_URL"); cs != "" {
		csArr := strings.Split(cs, "?")
		if len(csArr) != 2 {
			err = h.Error(
				fmt.Errorf("Invalid provided AZURE_SAS_SVC_URL"),
				"",
			)
			return
		}

		fs.client, err = azstorage.NewAccountSASClientFromEndpointToken(csArr[0], csArr[1])
		if err != nil {
			err = h.Error(err, "Could not connect using provided AZURE_SAS_SVC_URL")
			return
		}
	}
	return
}

func (fs *AzureFileSysClient) getContainers() (containers []azstorage.Container, err error) {
	params := azstorage.ListContainersParameters{}
	resp, err := fs.client.GetBlobService().ListContainers(params)
	if err != nil {
		err = h.Error(err, "Could not ListContainers")
		return
	}
	containers = resp.Containers
	return
}

func (fs *AzureFileSysClient) getBlobs(container *azstorage.Container, params azstorage.ListBlobsParameters) (blobs []azstorage.Blob, err error) {
	resp, err := container.ListBlobs(params)
	if err != nil {
		err = h.Error(err, "Could not ListBlobs for: "+container.GetURL())
		return
	}
	blobs = resp.Blobs
	return
}

func (fs *AzureFileSysClient) getAuthContainerURL(container *azstorage.Container) (containerURL azblob.ContainerURL, err error) {

	if fs.GetProp("AZURE_CONN_STR") != "" {
		credential, err := azblob.NewSharedKeyCredential(fs.GetProp("AZURE_ACCOUNT"), fs.GetProp("AZURE_KEY"))
		if err != nil {
			err = h.Error(err, "Unable to Authenticate")
			return containerURL, err
		}
		pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
		contURL, _ := url.Parse(container.GetURL())
		containerURL = azblob.NewContainerURL(*contURL, pipeline)
	} else if fs.GetProp("AZURE_SAS_SVC_URL") != "" {
		sasSvcURL, _ := url.Parse(fs.GetProp("AZURE_SAS_SVC_URL"))
		serviceURL := azblob.NewServiceURL(*sasSvcURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
		containerURL = serviceURL.NewContainerURL(container.Name)
	} else {
		err = h.Error(
			fmt.Errorf("Need to provide credentials AZURE_CONN_STR or AZURE_SAS_SVC_URL"),
			"",
		)
		return
	}
	return
}

// List list objects in path
func (fs *AzureFileSysClient) List(url string) (paths []string, err error) {
	host, path, err := ParseURL(url)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+url)
		return
	}

	path = cleanKey(path)

	svc := fs.client.GetBlobService()

	pathArr := strings.Split(path, "/")
	if path == "" {
		// list container
		containers, err := fs.getContainers()
		if err != nil {
			err = h.Error(err, "Could not getContainers for: "+url)
			return paths, err
		}
		for _, container := range containers {
			paths = append(
				paths,
				h.F("https://%s/%s", host, container.Name),
			)
		}
	} else if len(pathArr) == 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		blobs, err := fs.getBlobs(container, azstorage.ListBlobsParameters{Delimiter: "/"})
		if err != nil {
			err = h.Error(err, "Could not ListBlobs for: "+url)
			return paths, err
		}
		for _, blob := range blobs {
			paths = append(
				paths,
				h.F("https://%s/%s/%s", host, container.Name, blob.Name),
			)
		}
	} else if len(pathArr) > 1 {
		container := svc.GetContainerReference(pathArr[0])
		prefix := strings.Join(pathArr[1:], "/")
		blobs, err := fs.getBlobs(
			container,
			azstorage.ListBlobsParameters{Delimiter: "/", Prefix: prefix + "/"},
		)
		if err != nil {
			err = h.Error(err, "Could not ListBlobs for: "+url)
			return paths, err
		}
		for _, blob := range blobs {
			paths = append(
				paths,
				h.F("https://%s/%s/%s", host, container.Name, blob.Name),
			)
		}

		if len(blobs) == 0 {
			// maybe a file
			prefixArr := strings.Split(prefix, "/")
			parentPrefix := strings.Join(prefixArr[0:len(prefixArr)-1], "/")
			if parentPrefix != "" {
				parentPrefix = parentPrefix + "/"
			}
			blobs, err = fs.getBlobs(
				container,
				azstorage.ListBlobsParameters{Delimiter: "/", Prefix: parentPrefix},
			)

			if err != nil {
				err = h.Error(err, "Could not ListBlobs for: "+url)
				return paths, err
			}
			for _, blob := range blobs {
				blobFile := h.F("https://%s/%s/%s", host, container.Name, blob.Name)
				if blobFile == url {
					paths = append(
						paths,
						blobFile,
					)
				}
			}
		}

	} else {
		err = h.Error(fmt.Errorf("Invalid Azure path: " + url))
		return
	}

	return
}

// ListRecursive list objects in path
func (fs *AzureFileSysClient) ListRecursive(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+url)
		return
	}

	path = cleanKey(path)

	if path != "" {
		// list blobs
		return fs.List(url)
	}

	// list blobs of each continer
	containers, err := fs.getContainers()
	if err != nil {
		err = h.Error(err, "Could not getContainers for: "+url)
		return paths, err
	}
	for _, container := range containers {
		contURL := container.GetURL()
		contPaths, err := fs.List(contURL)
		if err != nil {
			err = h.Error(err, "Could not List blobs for conatiner: "+contURL)
			return paths, err
		}
		paths = append(paths, contPaths...)
	}

	return
}

// Delete list objects in path
func (fs *AzureFileSysClient) Delete(urlStr string) (err error) {
	suffixWildcard := false
	if strings.HasSuffix(urlStr, "*") {
		urlStr = urlStr[:len(urlStr)-1]
		suffixWildcard = true
	}

	host, path, err := ParseURL(urlStr)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKey(path)
	svc := fs.client.GetBlobService()

	pathArr := strings.Split(path, "/")
	if path == "" {
		// list container
		containers, err := fs.getContainers()
		if err != nil {
			err = h.Error(err, "Could not getContainers for: "+urlStr)
			return err
		}
		for _, container := range containers {
			options := azstorage.DeleteContainerOptions{}
			err = container.Delete(&options)
			if err != nil {
				return h.Error(err, "Could not delete container: "+container.GetURL())
			}
		}
	} else if len(pathArr) == 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		blobs, err := fs.getBlobs(container, azstorage.ListBlobsParameters{Delimiter: "/"})
		if err != nil {
			err = h.Error(err, "Could not ListBlobs for: "+urlStr)
			return err
		}
		for _, blob := range blobs {
			if suffixWildcard && !strings.HasPrefix(blob.Name, pathArr[1]) {
				continue
			}
			options := azstorage.DeleteBlobOptions{}
			err = blob.Delete(&options)
			if err != nil {
				if strings.Contains(err.Error(), "BlobNotFound") {
					err = nil
				} else {
					return h.Error(err, "Could not delete blob: "+blob.GetURL())
				}
			}
		}
	} else if len(pathArr) > 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		prefix := strings.Join(pathArr[1:], "/")
		blobs, err := fs.getBlobs(
			container,
			azstorage.ListBlobsParameters{Delimiter: "/", Prefix: prefix + "/"},
		)
		if err != nil {
			err = h.Error(err, "Could not ListBlobs for: "+urlStr)
			return err
		}
		for _, blob := range blobs {
			if suffixWildcard && !strings.HasPrefix(blob.Name, pathArr[1]) {
				continue
			}
			options := azstorage.DeleteBlobOptions{}
			err = blob.Delete(&options)
			if err != nil {
				if strings.Contains(err.Error(), "BlobNotFound") {
					err = nil
				} else {
					return h.Error(err, "Could not delete blob: "+blob.GetURL())
				}
			}
		}
		if len(blobs) == 0 {
			// maybe a file
			prefixArr := strings.Split(prefix, "/")
			parentPrefix := strings.Join(prefixArr[0:len(prefixArr)-1], "/")
			if parentPrefix != "" {
				parentPrefix = parentPrefix + "/"
			}

			blobs, err = fs.getBlobs(
				container,
				azstorage.ListBlobsParameters{Delimiter: "/", Prefix: parentPrefix},
			)
			if err != nil {
				err = h.Error(err, "Could not ListBlobs for: "+urlStr)
				return err
			}

			for _, blob := range blobs {
				blobFile := h.F("https://%s/%s/%s", host, container.Name, blob.Name)
				if blobFile == urlStr {
					options := azstorage.DeleteBlobOptions{}
					err = blob.Delete(&options)
					if err != nil {
						if strings.Contains(err.Error(), "BlobNotFound") {
							err = nil
						} else {
							return h.Error(err, "Could not delete blob: "+blob.GetURL())
						}
					}
				}
			}
		}
	} else {
		err = h.Error(
			fmt.Errorf("Invalid Azure path: "+urlStr),
			"",
		)
	}
	return
}

func (fs *AzureFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	host, path, err := ParseURL(urlStr)
	if err != nil || host == "" {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKey(path)

	pathArr := strings.Split(path, "/")

	if len(pathArr) < 2 {
		err = fmt.Errorf("Invalid Azure path (need blob URL): " + urlStr)
		err = h.Error(err)
		return
	}

	svc := fs.client.GetBlobService()
	container := svc.GetContainerReference(pathArr[0])
	_, err = container.CreateIfNotExists(&azstorage.CreateContainerOptions{Timeout: 20})
	if err != nil {
		err = h.Error(err, "Unable to create container: "+container.GetURL())
		return
	}

	blobPath := strings.Join(pathArr[1:], "/")
	blob := container.GetBlobReference(blobPath)
	err = blob.CreateBlockBlob(&azstorage.PutBlobOptions{})
	if err != nil {
		err = h.Error(err, "Unable to CreateBlockBlob: "+blob.GetURL())
		return
	}

	containerURL, err := fs.getAuthContainerURL(container)
	if err != nil {
		err = h.Error(err, "Unable to getAuthContainerURL: "+container.GetURL())
		return
	}

	blockBlobURL := containerURL.NewBlockBlobURL(blob.Name)

	pr, pw := io.Pipe()
	fs.Context().Wg.Write.Add()
	go func() {
		defer fs.Context().Wg.Write.Done()
		defer pw.Close()
		bw, err = io.Copy(pw, reader)
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error Copying"))
		}
	}()
	_, err = azblob.UploadStreamToBlockBlob(
		fs.Context().Ctx, pr, blockBlobURL,
		azblob.UploadStreamToBlockBlobOptions{
			BufferSize: 2 * 1024 * 1024,
			MaxBuffers: 3,
		},
	)
	fs.Context().Wg.Write.Wait()
	if err != nil {
		err = h.Error(err, "Error UploadStreamToBlockBlob: "+blockBlobURL.String())
		return
	}

	return
}

// GetReader returns an Azure FS reader
func (fs *AzureFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	host, path, err := ParseURL(urlStr)
	if err != nil || host == "" {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKey(path)

	pathArr := strings.Split(path, "/")

	if len(pathArr) < 2 {
		err = fmt.Errorf("Invalid Azure path (need blob URL): " + urlStr)
		err = h.Error(err)
		return
	}

	svc := fs.client.GetBlobService()
	container := svc.GetContainerReference(pathArr[0])

	// containerURL, err := fs.getAuthContainerURL(container)
	// if err != nil {
	// 	err = h.Error(err, "Unable to getAuthContainerURL: "+container.GetURL())
	// 	return
	// }
	// blobURL := containerURL.NewBlobURL(pathArr[1])

	// data := make([]byte, 1000)
	// err = azblob.DownloadBlobToBuffer(
	// 	fs.context.Ctx, blobURL, 0, 0, data,
	// 	azblob.DownloadFromBlobOptions{
	// 		Parallelism: 10,
	// 	},
	// )
	// if err != nil {
	// 	err = h.Error(err, "Unable to DownloadBlobToBuffer Blob: "+blobURL.String())
	// 	return
	// }
	// reader = bytes.NewReader(data)

	blobPath := strings.Join(pathArr[1:], "/")
	blob := container.GetBlobReference(blobPath)
	options := &azstorage.GetBlobOptions{}
	reader, err = blob.Get(options)
	if err != nil {
		err = h.Error(err, "Unable to Get Blob: "+blob.GetURL())
		return
	}

	return
}

///////////////////////////// SFTP

// SftpFileSysClient is for SFTP / SSH file ops
type SftpFileSysClient struct {
	BaseFileSysClient
	context   h.Context
	client    *sftp.Client
	sshClient *SSHClient
}

// Init initializes the fs client
func (fs *SftpFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)
	return fs.Connect()
}

// Connect initiates the Google Cloud Storage client
func (fs *SftpFileSysClient) Connect() (err error) {

	if fs.GetProp("SSH_PRIVATE_KEY") == "" {
		fs.SetProp("SFTP_PRIVATE_KEY", os.Getenv("SSH_PRIVATE_KEY"))
	}

	if fs.GetProp("SFTP_URL") != "" {
		u, err := url.Parse(fs.GetProp("SFTP_URL"))
		if err != nil {
			return h.Error(err, "could not parse SFTP URL")
		}

		host := u.Hostname()
		user := u.User.Username()
		password, _ := u.User.Password()
		sshPort := cast.ToInt(u.Port())
		if sshPort == 0 {
			sshPort = 22
		}

		if user != "" {
			fs.SetProp("SFTP_USER", user)
		}
		if password != "" {
			fs.SetProp("SFTP_PASSWORD", password)
		}
		if host != "" {
			fs.SetProp("SFTP_HOST", host)
		}
		if sshPort != 0 {
			fs.SetProp("SFTP_PORT", cast.ToString(sshPort))
		}
	}

	fs.sshClient = &SSHClient{
		Host:       fs.GetProp("SFTP_HOST"),
		Port:       cast.ToInt(fs.GetProp("SFTP_PORT")),
		User:       fs.GetProp("SFTP_USER"),
		Password:   fs.GetProp("SFTP_PASSWORD"),
		PrivateKey: fs.GetProp("SFTP_PRIVATE_KEY"),
	}

	err = fs.sshClient.Connect()
	if err != nil {
		return h.Error(err, "unable to connect to ssh server ")
	}

	fs.client, err = fs.sshClient.SftpClient()
	if err != nil {
		return h.Error(err, "unable to start SFTP client")
	}

	return nil
}

func (fs *SftpFileSysClient) getPrefix() string {
	return h.F(
		"sftp://%s@%s:%s",
		fs.GetProp("SFTP_USER"),
		fs.GetProp("SFTP_HOST"),
		fs.GetProp("SFTP_PORT"),
	)
}

func (fs *SftpFileSysClient) cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}
	return key
}

// List list objects in path
func (fs *SftpFileSysClient) List(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)

	stat, err := fs.client.Stat(path)
	if err != nil {
		return paths, h.Error(err, "error stating path")
	}

	var files []os.FileInfo
	if stat.IsDir() {
		files, err = fs.client.ReadDir(path)
		if err != nil {
			return paths, h.Error(err, "error listing path")
		}
		path = path + "/"
	} else {
		paths = append(paths, h.F("%s%s", fs.getPrefix(), path))
		return
	}

	for _, file := range files {
		path := h.F("%s%s%s", fs.getPrefix(), path, file.Name())
		paths = append(paths, path)
	}
	sort.Strings(paths)

	return
}

// ListRecursive list objects in path recursively
func (fs *SftpFileSysClient) ListRecursive(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)

	stat, err := fs.client.Stat(path)
	if err != nil {
		return paths, h.Error(err, "error stating path")
	}

	var files []os.FileInfo
	if stat.IsDir() {
		files, err = fs.client.ReadDir(path)
		if err != nil {
			return paths, h.Error(err, "error listing path")
		}
		path = path + "/"
	} else {
		paths = append(paths, h.F("%s%s", fs.getPrefix(), path))
		return
	}

	for _, file := range files {
		path := h.F("%s%s%s", fs.getPrefix(), path, file.Name())
		if file.IsDir() {
			subPaths, err := fs.ListRecursive(path)
			// h.P(subPaths)
			if err != nil {
				return []string{}, h.Error(err, "error listing sub path")
			}
			paths = append(paths, subPaths...)
		} else {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)

	return
}

// Delete list objects in path
func (fs *SftpFileSysClient) Delete(urlStr string) (err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	path = "/" + fs.cleanKey(path)
	paths, err := fs.ListRecursive(urlStr)
	if err != nil {
		return h.Error(err, "error listing path")
	}

	for _, sPath := range paths {
		_, sPath, _ = ParseURL(sPath)
		sPath = "/" + fs.cleanKey(sPath)
		err = fs.client.Remove(sPath)
		if err != nil {
			return h.Error(err, "error deleting path "+sPath)
		}
	}

	err = fs.client.Remove(path)
	if err != nil && !strings.Contains(err.Error(), "not exist") {
		return h.Error(err, "error deleting path")
	}
	return nil
}

// MkdirAll creates child directories
func (fs *SftpFileSysClient) MkdirAll(path string) (err error) {
	return fs.client.MkdirAll(path)
}

func (fs *SftpFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	filePathArr := strings.Split(path, "/")
	if len(filePathArr) > 1 {
		folderPath := strings.Join(filePathArr[:len(filePathArr)-1], "/")
		err = fs.client.MkdirAll(folderPath)
		if err != nil {
			err = h.Error(err, "Unable to create directory "+folderPath)
			return
		}
	}

	file, err := fs.client.Create(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}
	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = h.Error(err, "Error writing from reader")
	}
	return
}

// GetReader return a reader for the given path
func (fs *SftpFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	file, err := fs.client.Open(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		reader = bufio.NewReader(file)

		_, err = io.Copy(pipeW, reader)
		if err != nil {
			fs.Context().CaptureErr(h.Error(err, "Error writing from reader"))
			fs.Context().Cancel()
			h.LogError(fs.Context().Err())
		}

	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *SftpFileSysClient) GetWriter(urlStr string) (writer io.Writer, err error) {
	_, path, err := ParseURL(urlStr)
	if err != nil {
		err = h.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	file, err := fs.client.Create(path)
	if err != nil {
		err = h.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}

///////////////////////////// HTTP

// HTTPFileSysClient is for HTTP files
type HTTPFileSysClient struct {
	BaseFileSysClient
	context  h.Context
	client   *http.Client
	username string
	password string
}

// Init initializes the fs client
func (fs *HTTPFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = h.NewContext(ctx)
	return fs.Connect()
}

// Connect initiates the http client
func (fs *HTTPFileSysClient) Connect() (err error) {
	// fs.client = &http.Client{Timeout: 20 * time.Second}
	fs.client = &http.Client{}
	fs.username = fs.GetProp("HTTP_USER")
	fs.password = fs.GetProp("HTTP_PASSWORD")
	return
}

func (fs *HTTPFileSysClient) cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}
	return key
}

func (fs *HTTPFileSysClient) doGet(url string) (resp *http.Response, err error) {
	// Request the HTML page.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, h.Error(err, "could not construct request")
	}

	if fs.username != "" && fs.password != "" {
		req.SetBasicAuth(fs.username, fs.password)
	}

	req.Header.Set("DNT", "1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")

	// req.Header.Write(os.Stdout)
	h.Trace(url)
	resp, err = fs.client.Do(req)
	if err != nil {
		return nil, h.Error(err, "could not load HTTP url")
	}
	h.Trace("Content-Type: " + resp.Header.Get("Content-Type"))

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		err = fmt.Errorf("status code error: %d %s", resp.StatusCode, resp.Status)
		return resp, h.Error(err, "status code error: %d %s", resp.StatusCode, resp.Status)
	}

	return
}

// Delete :
func (fs *HTTPFileSysClient) Delete(path string) (err error) {
	err = fmt.Errorf("cannot delete a HTTP file")
	h.LogError(h.Error(err))
	return
}

// List lists all urls on the page
func (fs *HTTPFileSysClient) List(url string) (paths []string, err error) {

	if strings.HasPrefix(url, "https://docs.google.com/spreadsheets/d") {
		return []string{url}, nil
	}

	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	resp, err := fs.doGet(url)
	if err != nil {
		return nil, h.Error(err, "could not load HTTP url")
	}

	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		// We have no easy way of determining if the url is a page with link
		// or if the url is a body of data. Fow now, return url if not "text/html"
		paths = append(paths, url)
		return
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return paths, h.Error(err, "could not parse HTTP body")
	}
	// html, _ := doc.Html()
	// h.Trace("Html: " + html)

	// Find the review items
	urlParent := url
	urlArr := strings.Split(url, "/")
	if len(urlArr) > 3 {
		urlParent = strings.Join(urlArr[:len(urlArr)-1], "/")
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the band and title
		link, ok := s.Attr("href")
		if ok {
			if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
				paths = append(paths, link)
			} else {
				paths = append(paths, urlParent+"/"+link)
			}
		}
	})
	return
}

// ListRecursive lists all urls on the page
func (fs *HTTPFileSysClient) ListRecursive(url string) (paths []string, err error) {
	return fs.List(url)
}

// GetReader gets a reader for an HTTP resource (download)
func (fs *HTTPFileSysClient) GetReader(url string) (reader io.Reader, err error) {
	if strings.HasPrefix(url, "https://docs.google.com/spreadsheets/d") {
		ggs, err := NewGoogleSheetFromURL(
			url, "GSHEET_CLIENT_JSON_BODY="+fs.GetProp("GSHEET_CLIENT_JSON_BODY"),
		)
		if err != nil {
			return nil, h.Error(err, "could not load google sheets")
		}
		for k, v := range fs.Client().properties {
			ggs.properties[k] = v
		}

		data, err := ggs.GetDataset(fs.GetProp("GSHEET_SHEET_NAME"))
		if err != nil {
			return nil, h.Error(err, "could not open sheet: "+fs.GetProp("GSHEET_SHEET_NAME"))
		}
		return data.Stream().NewCsvReader(0), nil
	}

	resp, err := fs.doGet(url)
	if err != nil {
		return nil, h.Error(err, "could not load HTTP url")
	}
	h.Trace("ContentLength: %d", resp.ContentLength)

	return resp.Body, nil
}

// Write uploads an HTTP file
func (fs *HTTPFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	if strings.HasPrefix(urlStr, "https://docs.google.com/spreadsheets/d") {
		ggs, err := NewGoogleSheetFromURL(
			urlStr, "GSHEET_CLIENT_JSON_BODY="+fs.GetProp("GSHEET_CLIENT_JSON_BODY"),
		)
		if err != nil {
			return 0, h.Error(err, "could not load google sheets")
		}

		csv := CSV{Reader: reader}
		ds, err := csv.ReadStream()
		if err != nil {
			return 0, h.Error(err, "could not parse csv stream")
		}

		err = ggs.WriteSheet(fs.GetProp("GSHEET_SHEET_NAME"), ds, fs.GetProp("GSHEET_MODE"))
		if err != nil {
			return 0, h.Error(err, "could not write to sheet: "+fs.GetProp("GSHEET_SHEET_NAME"))
		}
		return 0, nil
	}

	err = fmt.Errorf("cannot write a HTTP file (yet)")
	h.LogError(h.Error(err))
	return
}

/////////////////////////////

// GetDataflow returns a dataflow from specified paths in specified FileSysClient
func GetDataflow(fs FileSysClient, paths ...string) (df *Dataflow, err error) {

	if len(paths) == 0 {
		err = fmt.Errorf("Provided 0 files for: %#v", paths)
		return
	}

	df = NewDataflow()
	df.Context = h.NewContext(fs.Context().Ctx)
	go func() {
		defer df.Close()
		dss := []*Datastream{}

		for _, path := range paths {
			if strings.HasSuffix(path, "/") {
				h.Debug("skipping %s because is not file", path)
				continue
			}
			h.Debug("reading datastream from %s", path)
			ds, err := fs.GetDatastream(path)
			if err != nil {
				fs.Context().CaptureErr(h.Error(err, "Unable to process "+path))
				return
			}
			dss = append(dss, ds)
		}

		df.PushStreams(dss...)

	}()

	// wait for first ds to start streaming.
	// columns need to be populated
	err = df.WaitReady()
	if err != nil {
		return df, err
	}

	return df, nil
}

// MakeDatastream create a datastream from a reader
func MakeDatastream(reader io.Reader) (ds *Datastream, err error) {

	csv := CSV{Reader: reader}
	ds, err = csv.ReadStream()
	if err != nil {
		return nil, err
	}

	return ds, nil
}

// WriteDatastream writes a datasream to a writer
// or use fs.Write(path, ds.NewCsvReader(0))
func WriteDatastream(writer io.Writer, ds *Datastream) (bw int64, err error) {
	reader := ds.NewCsvReader(0)
	return Write(reader, writer)
}

// Write writer to a writer from a reader
func Write(reader io.Reader, writer io.Writer) (bw int64, err error) {
	bw, err = io.Copy(writer, reader)
	if err != nil {
		err = h.Error(err, "Error writing from reader")
	}
	return
}

// TestFsPermissions tests read/write permisions
func TestFsPermissions(fs FileSysClient, pathURL string) (err error) {
	testString := "abcde"

	// Create file/folder
	bw, err := fs.Write(pathURL, strings.NewReader(testString))
	if err != nil {
		return h.Error(err, "failed testing permissions: Create file/folder")
	} else if bw == 0 {
		return h.Error(fmt.Errorf("failed testing permissions: Create file/folder returned 0 bytes"))
	}

	// List File
	paths, err := fs.List(pathURL)
	if err != nil {
		return h.Error(err, "failed testing permissions: List File")
	} else if len(paths) == 0 {
		return h.Error(fmt.Errorf("failed testing permissions: List File is zero"))
	}

	// Read File
	reader, err := fs.GetReader(pathURL)
	if err != nil {
		return h.Error(err, "failed testing permissions: Read File")
	}

	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return h.Error(err, "failed testing permissions: Read File, reading reader")
	}

	if string(content) != testString {
		return h.Error(fmt.Errorf("failed testing permissions: Read File content mismatch"))
	}

	// Delete file/folder
	err = fs.Delete(pathURL)
	if err != nil {
		return h.Error(err, "failed testing permissions: Delete file/folder")
	}

	return
}
