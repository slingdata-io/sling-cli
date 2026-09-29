package secrets

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

func init() { Register("infisical", newInfisicalProvider) }

// infisicalProvider reads one secret from Infisical. Two reference forms:
// `ref+infisical://<project_id>/<env>/<folder/path>/<KEY>`, and the vals form
// `ref+infisical://<KEY>?project=<id>&environment=<env>&path=<folder>`.
type infisicalProvider struct {
	cfg    ProviderConfig
	client *restClient

	mu    sync.Mutex
	token string
}

func newInfisicalProvider(cfg ProviderConfig) (Provider, error) {
	site := cfg.Get("site_url", "INFISICAL_API_URL", "INFISICAL_URL")
	if site == "" {
		site = "https://app.infisical.com"
	}
	site = strings.TrimSuffix(strings.TrimRight(site, "/"), "/api")
	client, err := newRESTClient(cfg, site, restOptions{})
	if err != nil {
		return nil, err
	}
	return &infisicalProvider{cfg: cfg, client: client}, nil
}

func (p *infisicalProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	project, env, folder, key := ref.Params.Get("project"), ref.Params.Get("environment"), ref.Params.Get("path"), ""
	if project != "" {
		key = strings.Trim(ref.Path, "/")
	} else {
		parts := strings.Split(strings.Trim(ref.Path, "/"), "/")
		if len(parts) < 3 {
			return nil, fmt.Errorf("use ref+infisical://<project_id>/<env>/<folder/path>/<KEY>")
		}
		project, env, key = parts[0], parts[1], parts[len(parts)-1]
		folder = "/" + strings.Join(parts[2:len(parts)-1], "/")
	}
	if folder == "" {
		folder = "/"
	}
	if env == "" {
		return nil, fmt.Errorf("the infisical environment is not set")
	}

	token, err := p.login(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{"workspaceId": {project}, "environment": {env}, "secretPath": {folder}}
	if t := ref.Params.Get("type"); t != "" {
		q.Set("type", t)
	}
	var resp struct {
		Secret struct {
			SecretValue *string `json:"secretValue"`
		} `json:"secret"`
	}
	path := "api/v3/secrets/raw/" + url.PathEscape(key) + "?" + q.Encode()
	if _, err := p.client.Do(ctx, http.MethodGet, path, nil, http.Header{"Authorization": {"Bearer " + token}}, &resp); err != nil {
		return nil, err
	}
	if resp.Secret.SecretValue == nil {
		return nil, fmt.Errorf("infisical returned no value for %q", key)
	}
	return []byte(*resp.Secret.SecretValue), nil
}

// login returns a token: the configured one, else a universal-auth login.
func (p *infisicalProvider) login(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" {
		return p.token, nil
	}
	if t := p.cfg.Get("token", "INFISICAL_TOKEN"); t != "" {
		p.token = t
		return t, nil
	}
	id := p.cfg.Get("client_id", "INFISICAL_UNIVERSAL_AUTH_CLIENT_ID")
	secret := p.cfg.Get("client_secret", "INFISICAL_UNIVERSAL_AUTH_CLIENT_SECRET")
	if id == "" || secret == "" {
		return "", fmt.Errorf("no infisical credentials. Set `client_id` and `client_secret`, or `token`")
	}
	var resp struct {
		AccessToken string `json:"accessToken"`
	}
	body := map[string]string{"clientId": id, "clientSecret": secret}
	if _, err := p.client.Do(ctx, http.MethodPost, "api/v1/auth/universal-auth/login", body, nil, &resp); err != nil {
		return "", fmt.Errorf("infisical login failed: %w", err)
	}
	if resp.AccessToken == "" {
		return "", fmt.Errorf("infisical login returned no token")
	}
	p.token = resp.AccessToken
	return p.token, nil
}
