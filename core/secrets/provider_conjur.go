package secrets

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func init() { Register("conjur", newConjurProvider) }

// conjurTokenLife is shorter than the 8 minutes a Conjur token lives.
const conjurTokenLife = 5 * time.Minute

// conjurProvider reads variables from CyberArk Conjur (vals form:
// `ref+conjur://<variable/id>`).
type conjurProvider struct {
	client  *restClient
	account string
	login   string
	apiKey  string

	mu       sync.Mutex
	token    string
	tokenAge time.Time
}

func newConjurProvider(cfg ProviderConfig) (Provider, error) {
	p := &conjurProvider{
		account: cfg.Get("account", "CONJUR_ACCOUNT"),
		login:   cfg.Get("login", "CONJUR_AUTHN_LOGIN"),
		apiKey:  cfg.Get("api_key", "CONJUR_AUTHN_API_KEY"),
	}
	base := cfg.Get("appliance_url", "CONJUR_APPLIANCE_URL")
	if base == "" || p.account == "" || p.login == "" || p.apiKey == "" {
		return nil, fmt.Errorf("conjur needs appliance_url, account, login and api_key (or CONJUR_APPLIANCE_URL, CONJUR_ACCOUNT, CONJUR_AUTHN_LOGIN, CONJUR_AUTHN_API_KEY)")
	}
	client, err := newRESTClient(cfg, base, restOptions{
		CACert:     cfg.Get("ca_cert", "CONJUR_SSL_CERTIFICATE", "CONJUR_CERT_FILE"),
		SkipVerify: cfg.Bool("skip_verify"),
	})
	if err != nil {
		return nil, err
	}
	p.client = client
	return p, nil
}

func (p *conjurProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	data, err := p.read(ctx, ref)
	if httpStatus(err) == http.StatusUnauthorized {
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
		data, err = p.read(ctx, ref)
	}
	return data, err
}

func (p *conjurProvider) read(ctx context.Context, ref Ref) ([]byte, error) {
	token, err := p.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	id := strings.Trim(ref.Path, "/")
	// the resource ID form: <account>:variable:<id>
	if account, rest, ok := strings.Cut(id, ":variable:"); ok {
		if account != p.account {
			return nil, fmt.Errorf("variable account %q is not the configured account %q", account, p.account)
		}
		id = rest
	}
	path := "secrets/" + url.PathEscape(p.account) + "/variable/" + url.PathEscape(id)
	header := http.Header{"Authorization": {`Token token="` + token + `"`}}
	return p.client.Do(ctx, http.MethodGet, path, nil, header, nil)
}

// authenticate returns a base64 access token, cached for a short time.
func (p *conjurProvider) authenticate(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" && time.Since(p.tokenAge) < conjurTokenLife {
		return p.token, nil
	}
	path := "authn/" + url.PathEscape(p.account) + "/" + url.PathEscape(p.login) + "/authenticate"
	raw, err := p.client.Do(ctx, http.MethodPost, path, []byte(p.apiKey), http.Header{"Content-Type": {"text/plain"}}, nil)
	if err != nil {
		return "", fmt.Errorf("conjur login failed: %w", err)
	}
	p.token = base64.StdEncoding.EncodeToString(raw)
	p.tokenAge = time.Now()
	return p.token, nil
}
