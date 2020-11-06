package iop

import (
	"fmt"
	"io"
	"sort"

	"github.com/flarco/g/sizedwaitgroup"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	h "github.com/flarco/g"
)

// S3 is a AWS s3 object
type S3 struct {
	Bucket string
	Region string
	swg    sizedwaitgroup.SizedWaitGroup
}

const defaultS3ConcurentLimit = 10

// Write writes to an S3 bucket key (upload)
// Example: Database or CSV stream into S3 file
func (s *S3) Write(key string, reader io.Reader) error {
	s.GetRegion()

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Uploader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))
	uploader := s3manager.NewUploader(sess)
	uploader.Concurrency = defaultS3ConcurentLimit

	// Upload the file to S3.
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload file, %v", err)
	}
	return nil
}

// WriteStream  write to an S3 bucket (upload)
// Example: Database or CSV stream into S3 file
func (s *S3) WriteStream(key string, ds *Datastream) error {
	s.GetRegion()
	reader := ds.NewCsvReader(0)
	return s.Write(key, reader)
}

// ReadStream returns a CSV datastreams from a S3 file
func (s *S3) ReadStream(key string) (ds *Datastream, err error) {

	reader, err := s.GetReader(key)
	if err != nil {
		return nil, err
	}

	csv := CSV{Reader: reader}
	ds, err = csv.ReadStream()
	if err != nil {
		return nil, err
	}

	return ds, nil
}

// ReadStreams returns one or more CSV datastreams from one or more S3 files
func (s *S3) ReadStreams(keys ...string) (dss []*Datastream, err error) {
	readers, err := s.GetReaders(keys...)
	if err != nil {
		return nil, err
	}

	for _, reader := range readers {
		csv := CSV{Reader: reader}
		ds, err := csv.ReadStream()
		if err != nil {
			return nil, err
		}
		dss = append(dss, ds)
		readers = append(readers, reader)
	}

	return dss, nil
}

// GetReaders returns one or more readers from specified keys in an S3 bucket
// For Fan-In sequential reading, use `multiReader := io.MultiReader(readers...)`
func (s *S3) GetReaders(keys ...string) (readers []io.Reader, err error) {
	for _, key := range keys {
		reader, err := s.GetReader(key)
		if err != nil {
			return nil, err
		}
		readers = append(readers, reader)
	}

	return readers, nil
}

// GetReader returns the reader from an S3 bucket (download)
// Example: S3 file stream into Database or CSV
func (s *S3) GetReader(key string) (io.Reader, error) {
	s.GetRegion()
	if s.swg.Size == 0 {
		s.swg = sizedwaitgroup.New(defaultS3ConcurentLimit)
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/
	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)
	downloader.Concurrency = 1

	pipeR, pipeW := io.Pipe()

	go func() {
		defer s.swg.Done()
		s.swg.Add()
		h.P(s.Bucket)
		h.P(key)
		// Write the contents of S3 Object to the file
		_, err := downloader.Download(
			fakeWriterAt{pipeW},
			&s3.GetObjectInput{
				Bucket: aws.String(s.Bucket),
				Key:    aws.String(key),
			})
		if err != nil {
			h.LogError(h.Error(err, "Error downloading S3 File -> "+key))
		}
		pipeW.Close()
	}()

	return pipeR, nil
}

// GetRegion determines the region of the bucket
func (s *S3) GetRegion() (region string) {

	if s.Region != "" {
		return s.Region
	}

	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:                    credentials.NewEnvCredentials(),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
	}))
	region, err := s3manager.GetBucketRegion(aws.BackgroundContext(), sess, s.Bucket, "us-east-1")
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			h.Debug("unable to find bucket %s's region not found", s.Bucket)
		}
		h.LogError(err, "Region not found for "+s.Bucket)
	}

	s.Region = region
	return region
}

// Delete deletes an s3 object at provided key
func (s *S3) Delete(key string) (err error) {
	s.GetRegion()
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:                    credentials.NewEnvCredentials(),
		Region:                         aws.String(s.Region),
		S3ForcePathStyle:               aws.Bool(true),
		DisableRestProtocolURICleaning: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	paths, err := s.List(key)
	if err != nil {
		return
	}

	objects := []*s3.ObjectIdentifier{}
	for _, path := range paths {
		objects = append(objects, &s3.ObjectIdentifier{Key: aws.String(path)})
	}

	_, err = svc.DeleteObjects(&s3.DeleteObjectsInput{
		Bucket: aws.String(s.Bucket),
		Delete: &s3.Delete{
			Objects: objects,
			Quiet:   aws.Bool(true),
		},
	})

	if err != nil {
		return h.Error(err, "Unable to delete S3 object: "+key)
	}

	err = svc.WaitUntilObjectNotExists(&s3.HeadObjectInput{
		Bucket: aws.String(s.Bucket),
		Key:    aws.String(key),
	})

	return err
}

// List S3 objects from a key/prefix
func (s *S3) List(key string) (paths []string, err error) {
	s.GetRegion()
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewEnvCredentials(),
		Region:           aws.String(s.Region),
		S3ForcePathStyle: aws.Bool(true),
		// Endpoint:    aws.String(endpoint),
		// LogLevel: aws.LogLevel(aws.LogDebugWithHTTPBody),
	}))

	// Create S3 service client
	svc := s3.New(sess)

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Bucket),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int64(100000),
	}

	result, err := svc.ListObjectsV2(input)
	if err != nil {
		return paths, err
	}

	for _, obj := range result.Contents {
		// paths = append(paths, F(`s3://%s/%s`, s.Bucket, *obj.Key))
		paths = append(paths, *obj.Key)
	}

	sort.Strings(paths)
	return paths, err
}
