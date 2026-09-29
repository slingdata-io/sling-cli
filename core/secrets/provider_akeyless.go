package secrets

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

func init() { Register("akeyless", newAkeylessProvider) }

// akeylessProvider reads static secrets from Akeyless:
// `ref+akeyless:///prod/db/password`.
type akeylessProvider struct {
	client    *restClient
	accessID  string
	accessKey string

	mu    sync.Mutex
	token string
}

func newAkeylessProvider(cfg ProviderConfig) (Provider, error) {
	p := &akeylessProvider{
		accessID:  cfg.Get("access_id", "AKEYLESS_ACCESS_ID"),
		accessKey: cfg.Get("access_key", "AKEYLESS_ACCESS_KEY"),
	}
	if p.accessID == "" || p.accessKey == "" {
		return nil, fmt.Errorf("akeyless needs `access_id` and `access_key` (or AKEYLESS_ACCESS_ID, AKEYLESS_ACCESS_KEY)")
	}
	base := cfg.Get("gateway_url", "AKEYLESS_GATEWAY_URL")
	if base == "" {
		base = "https://api.akeyless.io"
	}
	client, err := newRESTClient(cfg, base, restOptions{})
	if err != nil {
		return nil, err
	}
	p.client = client
	return p, nil
}

func (p *akeylessProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	name := "/" + strings.TrimLeft(ref.Path, "/")
	token, err := p.login(ctx)
	if err != nil {
		return nil, err
	}
	var resp map[string]any
	body := map[string]any{"names": []string{name}, "token": token}
	if _, err := p.client.Do(ctx, http.MethodPost, "get-secret-value", body, nil, &resp); err != nil {
		return nil, err
	}
	v, ok := resp[name]
	if !ok && len(resp) == 1 {
		for _, only := range resp {
			v, ok = only, true
		}
	}
	if !ok || v == nil {
		return nil, fmt.Errorf("akeyless returned no value for %q", name)
	}
	return []byte(fmt.Sprint(v)), nil
}

func (p *akeylessProvider) login(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" {
		return p.token, nil
	}
	var resp struct {
		Token string `json:"token"`
	}
	body := map[string]string{"access-id": p.accessID, "access-key": p.accessKey, "access-type": "access_key"}
	if _, err := p.client.Do(ctx, http.MethodPost, "auth", body, nil, &resp); err != nil {
		return "", fmt.Errorf("akeyless login failed: %w", err)
	}
	if resp.Token == "" {
		return "", fmt.Errorf("akeyless login returned no token")
	}
	p.token = resp.Token
	return p.token, nil
}
