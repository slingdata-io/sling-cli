package filesys

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
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
	awsConfig aws.Config
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

		// Preserve the protocol scheme when setting the tunnel endpoint
		scheme := "http"
		if strings.HasPrefix(endpoint, "https") {
			scheme = "https"
		}
		fs.SetProp("endpoint", scheme+"://127.0.0.1:"+cast.ToString(localPort))
	}

	region := fs.GetProp("REGION", "DEFAULT_REGION")
	if region == "" {
		region = defaultRegion
	}

	// Configure options for AWS SDK v2
	configOptions := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	// Add endpoint if specified
	if endpoint != "" {
		// Get the updated endpoint value in case it was modified (e.g., by SSH tunnel)
		finalEndpoint := fs.GetProp("endpoint")

		// Ensure final endpoint has proper protocol scheme
		if !strings.HasPrefix(finalEndpoint, "http://") && !strings.HasPrefix(finalEndpoint, "https://") {
			// Default to https for security
			finalEndpoint = "https://" + finalEndpoint
		}

		configOptions = append(configOptions, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               finalEndpoint,
					HostnameImmutable: true,
				}, nil
			})))
	}

	if cast.ToBool(fs.GetProp("USE_ENVIRONMENT")) {
		goto useEnv
	} else if profile := fs.GetProp("PROFILE"); profile != "" {
		// Fall back to profile if specified
		configOptions = append(configOptions, config.WithSharedConfigProfile(profile))
		goto skipUseEnv
	} else if fs.GetProp("ACCESS_KEY_ID") != "" && fs.GetProp("SECRET_ACCESS_KEY") != "" {
		configOptions = append(configOptions, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			fs.GetProp("ACCESS_KEY_ID"),
			fs.GetProp("SECRET_ACCESS_KEY"),
			fs.GetProp("SESSION_TOKEN"),
		)))
		goto skipUseEnv
	} else if val := fs.GetProp("USE_ENVIRONMENT"); val != "" && !cast.ToBool(val) {
		goto skipUseEnv
	}

useEnv:
	// Use environment credentials (AWS SDK will automatically pick these up)
	g.Debug("using default AWS environment credentials")

skipUseEnv:

	fs.awsConfig, err = config.LoadDefaultConfig(fs.Context().Ctx, configOptions...)
	if err != nil {
		err = g.Error(err, "Could not load AWS configuration (did not provide ACCESS_KEY_ID/SECRET_ACCESS_KEY or default AWS profile).")
		return
	}

	if role := fs.GetProp("ROLE_ARN"); role != "" {
		stsSvc := sts.NewFromConfig(fs.awsConfig)
		fs.awsConfig.Credentials = stscreds.NewAssumeRoleProvider(stsSvc, role)
	}

	return
}

// getSession returns the aws config and sets the region based on the bucket
func (fs *S3FileSysClient) getConfig() aws.Config {
	fs.mux.Lock()
	defer fs.mux.Unlock()
	endpoint := fs.GetProp("ENDPOINT")
	region := fs.GetProp("REGION")

	if fs.bucket == "" {
		return fs.awsConfig
	} else if region != "" {
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".digitaloceanspaces.com") {
		region := strings.TrimSuffix(endpoint, ".digitaloceanspaces.com")
		region = strings.TrimPrefix(region, "https://")
		fs.RegionMap[fs.bucket] = region
	} else if strings.HasSuffix(endpoint, ".cloudflarestorage.com") {
		fs.RegionMap[fs.bucket] = "auto"
	} else if endpoint == "" && fs.RegionMap[fs.bucket] == "" {
		s3Client := s3.NewFromConfig(fs.awsConfig)
		region, err := manager.GetBucketRegion(fs.Context().Ctx, s3Client, fs.bucket, func(o *s3.Options) {
			o.Region = defaultRegion
		})
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NotFound" {
				g.Debug("unable to find bucket %s's region not found", fs.bucket)
				g.Debug("Region not found for " + fs.bucket)
			} else {
				g.Debug(g.Error(err, "Error getting Region for "+fs.bucket).Error())
			}
		} else {
			fs.RegionMap[fs.bucket] = region
		}
	}

	// Create a copy of the config with the appropriate region
	configCopy := fs.awsConfig.Copy()
	if fs.RegionMap[fs.bucket] != "" {
		configCopy.Region = fs.RegionMap[fs.bucket]
	} else {
		configCopy.Region = defaultRegion
	}

	return configCopy
}

// Delete deletes the given path (file or directory)
// path should specify the full path with scheme:
// `s3://my_bucket/key/to/file.txt`
func (fs *S3FileSysClient) delete(uri string) (err error) {
	_, err = fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	// Create S3 service client
	svc := s3.NewFromConfig(fs.getConfig())

	nodes, err := fs.ListRecursive(uri)
	if err != nil {
		return
	}

	objects := []types.ObjectIdentifier{}
	for _, subNode := range nodes {
		subNode.URI, err = fs.GetPath(subNode.URI)
		if err != nil {
			err = g.Error(err, "Error Parsing url: "+uri)
			return
		}
		objects = append(objects, types.ObjectIdentifier{Key: aws.String(subNode.URI)})
	}

	if len(objects) == 0 {
		return
	}

	input := &s3.DeleteObjectsInput{
		Bucket: aws.String(fs.bucket),
		Delete: &types.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	}
	_, err = svc.DeleteObjects(fs.Context().Ctx, input)

	if err != nil {
		return g.Error(err, fmt.Sprintf("Unable to delete S3 objects:\n%#v", input))
	}

	// Note: AWS SDK v2 doesn't have WaitUntilObjectNotExists in the same way
	// We could implement a custom waiter if needed, but for now we'll skip this check

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
	svc := s3.NewFromConfig(fs.getConfig())

	// Create a downloader with the config and default options
	downloader := manager.NewDownloader(svc, func(d *manager.Downloader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(BufferSize)
	})
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		// Write the contents of S3 Object to the file
		_, err := downloader.Download(
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
	svc := s3.NewFromConfig(fs.getConfig())

	uploader := manager.NewUploader(svc, func(d *manager.Uploader) {
		d.PartSize = PartSize
		d.Concurrency = Concurrency
		d.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(BufferSize)
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
		_, err := uploader.Upload(fs.Context().Ctx, &s3.PutObjectInput{
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

	svc := s3.NewFromConfig(fs.getConfig())
	uploader := manager.NewUploader(svc)
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
	_, err = uploader.Upload(fs.Context().Ctx, &s3.PutObjectInput{
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
func (fs *S3FileSysClient) getEncryptionParams() (sse types.ServerSideEncryption, kmsKeyId *string) {
	if val := fs.GetProp("encryption_algorithm"); val != "" {
		if g.In(val, "AES256", "aws:kms", "aws:kms:dsse") {
			sse = types.ServerSideEncryption(val)
		}
	}

	if val := fs.GetProp("encryption_kms_key"); val != "" {
		if sse != "" && g.In(string(sse), "aws:kms", "aws:kms:dsse") {
			kmsKeyId = aws.String(val)
		}
	}

	return
}

// Buckets returns the buckets found in the account
func (fs *S3FileSysClient) Buckets() (paths []string, err error) {
	// Create S3 service client
	svc := s3.NewFromConfig(fs.getConfig())
	result, err := svc.ListBuckets(fs.Context().Ctx, &s3.ListBucketsInput{})
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

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	g.Trace("path = %s", path)

	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(fs.bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
	}

	// Create S3 service client
	svc := s3.NewFromConfig(fs.getConfig())

	nodes, err = fs.doList(svc, input, fs.Prefix("/"), pattern)
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
	svc := s3.NewFromConfig(fs.getConfig())

	return fs.doList(svc, input, fs.Prefix("/"), pattern)
}

func (fs *S3FileSysClient) doList(svc *s3.Client, input *s3.ListObjectsV2Input, urlPrefix string, pattern *glob.Glob) (nodes FileNodes, err error) {
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)
	result, err := svc.ListObjectsV2(fs.Context().Ctx, input)
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
			input.ContinuationToken = result.NextContinuationToken
			result, err = svc.ListObjectsV2(fs.Context().Ctx, input)
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

	svc := s3.NewFromConfig(fs.getConfig())

	// Create a presign client
	presignClient := s3.NewPresignClient(svc)

	req, err := presignClient.PresignGetObject(fs.Context().Ctx, &s3.GetObjectInput{
		Bucket: aws.String(s3U.Hostname()),
		Key:    aws.String(strings.TrimPrefix(s3U.Path(), "/")),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = dur
	})
	if err != nil {
		err = g.Error(err, "Could not request pre-signed s3 url")
		return
	}

	httpURL = req.URL

	return
}
