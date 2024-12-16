package filesys

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/flarco/g"
	"github.com/flarco/g/net"
	"github.com/gobwas/glob"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// S3FileSysClient is a file system client to write file to Amazon's S3 file sys.
type S3FileSysClient struct {
	BaseFileSysClient
	context   g.Context
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
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	for _, key := range g.ArrStr("BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "REGION", "DEFAULT_REGION", "SESSION_TOKEN", "ENDPOINT", "ROLE_ARN", "ROLE_SESSION_NAME", "PROFILE") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("AWS_"+key))
		}
	}

	fs.bucket = fs.GetProp("BUCKET")
	fs.RegionMap = map[string]string{}

	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *S3FileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s", fs.FsType().String(), fs.bucket) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *S3FileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	host, path, err := ParseURL(uri)
	if err != nil {
		return
	}

	if fs.bucket != host {
		err = g.Error("URL bucket differs from connection bucket. %s != %s", host, fs.bucket)
	}

	return path, err
}

const defaultRegion = "us-east-1"

type fakeWriterAt struct {
	w io.Writer
}

func (fw fakeWriterAt) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return fw.w.Write(p)
}

// Connect initiates the S3 client
func (fs *S3FileSysClient) Connect() (err error) {

	endpoint := fs.GetProp("endpoint")

	// via SSH tunnel
	if sshTunnelURL := fs.GetProp("ssh_tunnel"); sshTunnelURL != "" {

		endpointU, err := url.Parse(endpoint)
		if err != nil {
			return g.Error(err, "could not parse endpoint URL for SSH forwarding")
		}

		endpointPort := cast.ToInt(endpointU.Port())
		if endpointPort == 0 {
			if strings.HasPrefix(endpoint, "https") {
				endpointPort = 443
			} else if strings.HasPrefix(endpoint, "http") {
				endpointPort = 80
			}
		}

		tunnelPrivateKey := fs.GetProp("ssh_private_key")
		tunnelPassphrase := fs.GetProp("ssh_passphrase")

		localPort, err := iop.OpenTunnelSSH(endpointU.Hostname(), endpointPort, sshTunnelURL, tunnelPrivateKey, tunnelPassphrase)
		if err != nil {
			return g.Error(err, "could not connect to ssh tunnel server")
		}

		fs.SetProp("endpoint", "127.0.0.1:"+cast.ToString(localPort))
	}

	region := fs.GetProp("REGION", "DEFAULT_REGION")
	if region == "" {
		region = defaultRegion
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	awsConfig := &aws.Config{
		Region:                         aws.String(region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		Endpoint:                       aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}

	if _, err := credentials.NewEnvCredentials().Get(); err == nil {
		// Use environment credentials (AWS SDK will automatically pick these up)
		g.Debug("using default AWS environment credentials")
	} else if profile := fs.GetProp("PROFILE"); profile != "" {
		// Fall back to profile if specified
		creds := credentials.NewSharedCredentials("", profile)
		if _, err := creds.Get(); err != nil {
			return g.Error(err, "Failed to load credentials for profile '%s'. Please check if profile exists in ~/.aws/credentials", profile)
		}
		awsConfig.Credentials = creds
	} else if fs.GetProp("ACCESS_KEY_ID") != "" && fs.GetProp("SECRET_ACCESS_KEY") != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(
			fs.GetProp("ACCESS_KEY_ID"),
			fs.GetProp("SECRET_ACCESS_KEY"),
			fs.GetProp("SESSION_TOKEN"),
		)
	}

	fs.session, err = session.NewSession(awsConfig)
	if err != nil {
		err = g.Error(err, "Could not create AWS session (did not provide ACCESS_KEY_ID/SECRET_ACCESS_KEY or default AWS profile).")
		return
	}

	if role := fs.GetProp("ROLE_ARN"); role != "" {
		fs.session.Config.Credentials = stscreds.NewCredentials(fs.session, role)
	}

	return
}

// getSession returns the session and sets the region based on the bucket
func (fs *S3FileSysClient) getSession() (sess *session.Session) {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	endpoint := fs.GetProp("ENDPOINT")
	region := fs.GetProp("REGION")

	if fs.bucket == "" {
		return fs.session
	} else if region != "" {
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".digitaloceanspaces.com") {
		region := strings.TrimSuffix(endpoint, ".digitaloceanspaces.com")
		region = strings.TrimPrefix(region, "https://")
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".cloudflarestorage.com") {
		fs.RegionMap[fs.bucket] = "auto"
	} else if endpoint == "" && fs.RegionMap[fs.bucket] == "" {
		region, err := s3manager.GetBucketRegion(fs.Context().Ctx, fs.session, fs.bucket, defaultRegion)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				g.Debug("unable to find bucket %s's region not found", fs.bucket)
				g.Debug("Region not found for " + fs.bucket)
			} else {
				g.Debug(g.Error(err, "Error getting Region for "+fs.bucket).Error())
			}
		} else {
			fs.RegionMap[fs.bucket] = region
		}
	}

	fs.session.Config.Region = aws.String(fs.RegionMap[fs.bucket])
	if fs.RegionMap[fs.bucket] == "" {
		fs.session.Config.Region = g.String(defaultRegion)
	}

	return fs.session
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) delete(uri string) (err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	nodes, err := fs.ListRecursive(uri)
	if err != nil {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, subNode := range nodes {
		subNode.URI, err = fs.GetPath(subNode.URI)
		if err != nil {
			err = g.Error(err, "Error Parsing url: "+uri)
			return
		}
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(subNode.URI)})
	}

	if len(objects) == 0 {
		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(fs.bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}
	_, err = svc.DeleteObjectsWithContext(fs.Context().Ctx, input)

	if err != nil {
		return g.Error(err, fmt.Sprintf("Unable to delete S3 objects:\n%#v", input))
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return g.Error(err, "error WaitUntilObjectNotExists")
	}

	return
}

func (fs *S3FileSysClient) getConcurrency() int {
	conc := cast.ToInt(fs.GetProp("CONCURRENCY"))
	if conc == 0 {
		conc = runtime.NumCPU()
	}
	return conc
}

// GetReader return a reader for the given path
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt` or `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	// https://github.com/chanzuckerberg/s3parcp
	PartSize := int64(os.Getpagesize()) * 1024 * 10
	Concurrency := fs.getConcurrency()
	BufferSize := 64 * 1024
	svc := s3.New(fs.getSession())

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(fs.getSession(), func(d *s3manager.Downloader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = s3manager.NewPooledBufferedWriterReadFromProvider(BufferSize)
		d.S3 = svc
	})
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		// Write the contents of S3 Object to the file
		_, err := downloader.DownloadWithContext(
			fs.Context().Ctx,
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(fs.bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error downloading S3 File -> "+key))
			return
		}
	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) GetWriter(uri string) (writer io.Writer, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	// https://github.com/chanzuckerberg/s3parcp
	PartSize := int64(os.Getpagesize()) * 1024 * 10
	Concurrency := fs.getConcurrency()
	BufferSize := 10485760 // 10MB
	svc := s3.New(fs.getSession())

	uploader := s3manager.NewUploader(fs.getSession(), func(d *s3manager.Uploader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = s3manager.NewBufferedReadSeekerWriteToPool(BufferSize)
		d.S3 = svc
	})
	uploader.Concurrency = fs.Context().Wg.Limit

	pipeR, pipeW := io.Pipe()

	fs.Context().Wg.Write.Add()
	go func() {
		// manage concurrency
		defer fs.Context().Wg.Write.Done()
		defer pipeR.Close()

		// Upload the file to S3.
		ServerSideEncryption, SSEKMSKeyId := fs.getEncryptionParams()
		_, err := uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
			Bucket:               aws.String(fs.bucket),
			Key:                  aws.String(key),
			Body:                 pipeR,
			ServerSideEncryption: ServerSideEncryption,
			SSEKMSKeyId:          SSEKMSKeyId,
		})
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error uploading S3 File -> "+key))
		}
	}()

	writer = pipeW

	return
}

func (fs *S3FileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return
	}

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
			fs.Context().CaptureErr(g.Error(err, "Error Copying"))
		}
	}()

	// Upload the file to S3.
	ServerSideEncryption, SSEKMSKeyId := fs.getEncryptionParams()
	_, err = uploader.UploadWithContext(fs.Context().Ctx, &s3manager.UploadInput{
		Bucket:               aws.String(fs.bucket),
		Key:                  aws.String(key),
		Body:                 pr,
		ServerSideEncryption: ServerSideEncryption,
		SSEKMSKeyId:          SSEKMSKeyId,
	})
	if err != nil {
		err = g.Error(err, "failed to upload file: "+key)
		return
	}

	err = fs.Context().Err()

	return
}

// getEncryptionParams returns the encryption params if specified
func (fs *S3FileSysClient) getEncryptionParams() (sse, kmsKeyId *string) {
	if val := fs.GetProp("encryption_algorithm"); val != "" {
		if g.In(val, "AES256", "aws:kms", "aws:kms:dsse") {
			sse = aws.String(val)
		}
	}

	if val := fs.GetProp("encryption_kms_key"); val != "" {
		if sse != nil && g.In(*sse, "aws:kms", "aws:kms:dsse") {
			kmsKeyId = aws.String(val)
		}
	}

	return
}

// Buckets returns the buckets found in the account
func (fs *S3FileSysClient) Buckets() (paths []string, err error) {
	// Create S3 service client
	svc := s3.New(fs.getSession())
	result, err := svc.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, g.Error(err, "could not list buckets")
	}

	for _, bucket := range result.Buckets {
		paths = append(paths, g.F("s3://%s", *bucket.Name))
	}
	return
}

// List lists the file in given directory path
func (fs *S3FileSysClient) List(uri string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	g.Trace("path = %s", path)

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	nodes, err = fs.doList(svc, input, fs.Prefix("/"), nil)
	if err != nil {
		return
	} else if path == "" {
		// root level
		return
	}

	// s3.List return all objects matching the path
	// need to match exactly the parent folder to not
	// return whatever objects partially match the beginning
	for _, n := range nodes {
		if !strings.HasSuffix(n.URI, "/") && path == n.Path() {
			return FileNodes{n}, err
		}
	}

	prefix := strings.TrimSuffix(path, "/") + "/"
	nodes2 := FileNodes{}
	for _, p := range nodes {
		if strings.HasPrefix(p.Path(), prefix) {
			nodes2 = append(nodes2, p)
		}
	}

	// if path is folder, need to read inside
	if len(nodes2) == 1 {
		nPath := nodes2[0]
		if strings.HasSuffix(nPath.URI, "/") && strings.TrimSuffix(nPath.URI, "/") == strings.TrimSuffix(path, "/") {
			return fs.List(nPath.URI)
		}
	} else {
		// exclude the input path if folder and is part of result (happens for digital-ocean
		nodes2 = lo.Filter(nodes2, func(n FileNode, i int) bool { return !(n.IsDir && n.Path() == path) })
	}
	return nodes2, err
}

// ListRecursive lists the file in given directory path recusively
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/directory`
func (fs *S3FileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(fs.bucket),
		Prefix: aws.String(path),
	}

	// Create S3 service client
	svc := s3.New(fs.getSession())

	return fs.doList(svc, input, fs.Prefix("/"), pattern)
}

func (fs *S3FileSysClient) doList(svc *s3.S3, input *s3.ListObjectsV2Input, urlPrefix string, pattern *glob.Glob) (nodes FileNodes, err error) {
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)
	result, err := svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
	if err != nil {
		err = g.Error(err, "Error with ListObjectsV2 for: %#v", input)
		return nodes, err
	}

	g.Trace("%#v", result)

	ts := fs.GetRefTs().Unix()

	for {

		for _, cp := range result.CommonPrefixes {
			nodes.Add(FileNode{URI: urlPrefix + *cp.Prefix, IsDir: true})
		}

		for _, obj := range result.Contents {
			if obj == nil {
				continue
			}

			node := FileNode{
				URI:     urlPrefix + *obj.Key,
				Updated: obj.LastModified.Unix(),
				Size:    cast.ToUint64(*obj.Size),
			}
			if obj.Owner != nil {
				node.Owner = *obj.Owner.DisplayName
			}

			nodes.AddWhere(pattern, ts, node)
			if len(nodes) >= maxItems {
				g.Warn("Limiting S3 list results at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
				return
			}
		}

		if *result.IsTruncated {
			input.SetContinuationToken(*result.NextContinuationToken)
			result, err = svc.ListObjectsV2WithContext(fs.Context().Ctx, input)
			if err != nil {
				err = g.Error(err, "Error with ListObjectsV2 for: %#v", input)
				return nodes, err
			}
		} else {
			break
		}
	}

	return
}

func (fs *S3FileSysClient) GenerateS3PreSignedURL(s3URL string, dur time.Duration) (httpURL string, err error) {

	s3U, err := net.NewURL(s3URL)
	if err != nil {
		err = g.Error(err, "Could not parse s3 url")
		return
	}

	svc := s3.New(fs.getSession())
	req, _ := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s3U.Hostname()),
		Key:    aws.String(strings.TrimPrefix(s3U.Path(), "/")),
	})

	httpURL, err = req.Presign(dur)
	if err != nil {
		err = g.Error(err, "Could not request pre-signed s3 url")
		return
	}

	return
}
