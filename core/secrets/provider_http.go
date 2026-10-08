package secrets

import (
	"context"
	"fmt"
	"net/http"
)

func init() { Register("httpjson", newHTTPJSONProvider) }

// httpJSONProvider reads a document with an HTTP GET:
// `ref+httpjson://config.acme.com/sling/mydb#/password` gets
// https://config.acme.com/sling/mydb. `?insecure=true` uses plain HTTP.
type httpJSONProvider struct {
	client *restClient
}

func newHTTPJSONProvider(cfg ProviderConfig) (Provider, error) {
	header := map[string]string{}
	switch h := cfg.Props["headers"].(type) {
	case map[string]any:
		for k, v := range h {
			header[k] = fmt.Sprint(v)
		}
	case map[string]string:
		for k, v := range h {
			header[k] = v
		}
	case nil:
	default:
		return nil, fmt.Errorf("httpjson `headers` must be a map")
	}
	client, err := newRESTClient(cfg, "", restOptions{
		CACert:     cfg.Get("ca_cert"),
		SkipVerify: cfg.Bool("skip_verify"),
		Header:     header,
	})
	if err != nil {
		return nil, err
	}
	return &httpJSONProvider{client: client}, nil
}

func (p *httpJSONProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	scheme := "https://"
	if ref.Params.Get("insecure") == "true" {
		scheme = "http://"
	}
	return p.client.Do(ctx, http.MethodGet, scheme+ref.Path, nil, nil, nil)
}
