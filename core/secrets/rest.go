package secrets

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// restClient calls one vendor REST API: base URL, auth header, retry with
// backoff on 429 and 5xx, JSON decode. It does not retry 401, 403 or 404.
type restClient struct {
	base    string
	http    *http.Client
	header  http.Header
	redact  func(string) string
	tries   int
	backoff time.Duration
}

type restOptions struct {
	CACert     string // PEM text or a file path
	SkipVerify bool
	Header     map[string]string
}

func newRESTClient(cfg ProviderConfig, base string, opts restOptions) (*restClient, error) {
	tlsCfg := &tls.Config{InsecureSkipVerify: opts.SkipVerify}
	if opts.CACert != "" {
		pem := []byte(opts.CACert)
		if !strings.Contains(opts.CACert, "-----BEGIN") {
			b, err := os.ReadFile(opts.CACert)
			if err != nil {
				return nil, fmt.Errorf("could not read CA certificate: %w", err)
			}
			pem = b
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("CA certificate has no valid PEM block")
		}
		tlsCfg.RootCAs = pool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg

	header := http.Header{}
	for k, v := range opts.Header {
		header.Set(k, v)
	}
	return &restClient{
		base:    strings.TrimRight(base, "/"),
		http:    &http.Client{Transport: transport},
		header:  header,
		redact:  cfg.Redact,
		tries:   3,
		backoff: 250 * time.Millisecond,
	}, nil
}

// statusError is a non-2xx HTTP answer.
type statusError struct {
	Code int
	Body string
}

func (e *statusError) Error() string {
	msg := fmt.Sprintf("HTTP %d %s", e.Code, http.StatusText(e.Code))
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// httpStatus returns the HTTP status code in err, or 0.
func httpStatus(err error) int {
	var se *statusError
	if errors.As(err, &se) {
		return se.Code
	}
	return 0
}

// Do sends one request. body is []byte, url.Values (form), or a value to
// JSON-encode. out, when not nil, receives the JSON answer.
func (c *restClient) Do(ctx context.Context, method, path string, body any, header http.Header, out any) ([]byte, error) {
	target := path
	if !strings.HasPrefix(path, "http://") && !strings.HasPrefix(path, "https://") {
		target = c.base + "/" + strings.TrimLeft(path, "/")
	}

	var payload []byte
	contentType := ""
	switch b := body.(type) {
	case nil:
	case []byte:
		payload = b
	case url.Values:
		payload = []byte(b.Encode())
		contentType = "application/x-www-form-urlencoded"
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			return nil, err
		}
		payload = enc
		contentType = "application/json"
	}

	var lastErr error
	for try := 0; try < c.tries; try++ {
		if try > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(c.backoff * time.Duration(1<<(try-1))):
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		for k, v := range c.header {
			req.Header[k] = v
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if contentType != "" && req.Header.Get("Content-Type") == "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.Header.Set("Accept", "application/json")

		resp, err := c.http.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			lastErr = errors.New(c.clean(err.Error()))
			continue
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(data) > 0 {
				if err := json.Unmarshal(data, out); err != nil {
					return nil, fmt.Errorf("could not decode answer from %s: %w", c.base, err)
				}
			}
			return data, nil
		}
		lastErr = &statusError{Code: resp.StatusCode, Body: truncate(c.clean(string(data)), 300)}
		if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
			return nil, lastErr
		}
	}
	return nil, lastErr
}

func (c *restClient) clean(s string) string {
	s = strings.TrimSpace(s)
	if c.redact != nil {
		s = c.redact(s)
	}
	return s
}
