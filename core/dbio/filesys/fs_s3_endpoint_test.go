package filesys

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/slingdata-io/sling-cli/core/dbio"
)

type endpointCaptureTransport struct {
	requests []*http.Request
}

func (tr *endpointCaptureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	tr.requests = append(tr.requests, req.Clone(req.Context()))
	body := "payload"
	if req.URL.Query().Get("list-type") == "2" {
		body = `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>archive-bucket</Name><IsTruncated>false</IsTruncated><KeyCount>0</KeyCount></ListBucketResult>`
	}
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

// Reproduces OSS rejecting path-style requests, while protecting the existing
// behavior of other custom endpoints and the default AWS endpoint.
func TestS3EndpointAddressing(t *testing.T) {
	for _, tc := range []struct {
		name, endpoint, style, host, pathPrefix string
	}{
		{"oss-internal", "https://oss-cn-shenzhen-internal.aliyuncs.com", "vhost", "archive-bucket.oss-cn-shenzhen-internal.aliyuncs.com", ""},
		{"oss-public-no-scheme", "oss-cn-shenzhen.aliyuncs.com", "vhost", "archive-bucket.oss-cn-shenzhen.aliyuncs.com", ""},
		{"custom-default-unchanged", "https://storage.example.com", "", "storage.example.com", "/archive-bucket"},
		{"custom-explicit-path", "https://storage.example.com", "path", "storage.example.com", "/archive-bucket"},
		{"aws-default", "", "", "archive-bucket.s3.us-east-1.amazonaws.com", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewFileSysClient(dbio.TypeFileS3,
				"bucket=archive-bucket", "region=us-east-1",
				"access_key_id=EXAMPLEACCESSKEY", "secret_access_key=not-a-real-secret",
				"endpoint="+tc.endpoint, "url_style="+tc.style)
			if err != nil {
				t.Fatal(err)
			}
			fs := client.(*S3FileSysClient)
			tr := &endpointCaptureTransport{}
			fs.awsConfig.HTTPClient = &http.Client{Transport: tr}
			svc := s3.NewFromConfig(fs.getConfig())
			ctx := context.Background()
			if _, err = svc.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String("archive-bucket")}); err != nil {
				t.Fatal(err)
			}
			if _, err = svc.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("archive-bucket"), Key: aws.String("trial/data.parquet"), Body: strings.NewReader("payload")}); err != nil {
				t.Fatal(err)
			}
			obj, err := svc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("archive-bucket"), Key: aws.String("trial/data.parquet")})
			if err != nil {
				t.Fatal(err)
			}
			obj.Body.Close()
			if len(tr.requests) != 3 {
				t.Fatalf("expected list, upload and read requests, got %d", len(tr.requests))
			}
			for i, req := range tr.requests {
				auth := req.Header.Get("Authorization")
				_, signedHeaders, _ := strings.Cut(auth, "SignedHeaders=")
				signedHeaders, _, _ = strings.Cut(signedHeaders, ",")
				if !strings.Contains(auth, "/us-east-1/s3/aws4_request") || !strings.Contains(";"+signedHeaders+";", ";host;") {
					t.Errorf("request %d: expected SigV4 S3 authentication with a signed host", i)
				}
				wantPath := tc.pathPrefix
				if i > 0 {
					wantPath += "/trial/data.parquet"
				} else if wantPath == "" {
					wantPath = "/"
				}
				if req.URL.Host != tc.host || req.URL.Path != wantPath {
					t.Errorf("request %d: got %s%s, want %s%s", i, req.URL.Host, req.URL.Path, tc.host, wantPath)
				}
			}
		})
	}
}
