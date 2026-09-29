package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

func init() { Register("tss", newTSSProvider, "delinea", "thycotic") }

// tssProvider reads secrets from Delinea Secret Server. `ref+tss://<id>`
// returns a JSON object of field slug to value, so `#/password` selects one.
// `ref+tss://<id>/<slug>` returns one field.
type tssProvider struct {
	cfg    ProviderConfig
	client *restClient

	mu    sync.Mutex
	token string
}

func newTSSProvider(cfg ProviderConfig) (Provider, error) {
	base := cfg.Get("server_url", "TSS_SERVER_URL")
	if base == "" {
		return nil, fmt.Errorf("delinea secret server needs `server_url` or TSS_SERVER_URL")
	}
	client, err := newRESTClient(cfg, base, restOptions{SkipVerify: cfg.Bool("skip_verify")})
	if err != nil {
		return nil, err
	}
	return &tssProvider{cfg: cfg, client: client}, nil
}

func (p *tssProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	id, field, _ := strings.Cut(strings.Trim(ref.Path, "/"), "/")
	token, err := p.login(ctx)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Items []struct {
			Slug      string `json:"slug"`
			FieldName string `json:"fieldName"`
			ItemValue string `json:"itemValue"`
		} `json:"items"`
	}
	header := http.Header{"Authorization": {"Bearer " + token}}
	if _, err := p.client.Do(ctx, http.MethodGet, "api/v1/secrets/"+url.PathEscape(id), nil, header, &resp); err != nil {
		return nil, err
	}
	fields := map[string]string{}
	for _, item := range resp.Items {
		key := item.Slug
		if key == "" {
			key = strings.ToLower(item.FieldName)
		}
		fields[key] = item.ItemValue
	}
	if field != "" {
		v, ok := fields[field]
		if !ok {
			return nil, fmt.Errorf("field %q not found in secret %s", field, id)
		}
		return []byte(v), nil
	}
	return json.Marshal(fields)
}

// login returns the configured token, else an OAuth password grant token.
func (p *tssProvider) login(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" {
		return p.token, nil
	}
	if t := p.cfg.Get("token", "TSS_TOKEN"); t != "" {
		p.token = t
		return t, nil
	}
	form := url.Values{
		"grant_type": {"password"},
		"username":   {p.cfg.Get("username", "TSS_USERNAME")},
		"password":   {p.cfg.Get("password", "TSS_PASSWORD")},
	}
	if d := p.cfg.Get("domain", "TSS_DOMAIN"); d != "" {
		form.Set("domain", d)
	}
	var resp struct {
		AccessToken string `json:"access_token"`
	}
	if _, err := p.client.Do(ctx, http.MethodPost, "oauth2/token", form, nil, &resp); err != nil {
		return "", fmt.Errorf("delinea login failed: %w", err)
	}
	if resp.AccessToken == "" {
		return "", fmt.Errorf("delinea login returned no token")
	}
	p.token = resp.AccessToken
	return p.token, nil
}
