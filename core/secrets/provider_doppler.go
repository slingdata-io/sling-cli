package secrets

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

func init() { Register("doppler", newDopplerProvider) }

// dopplerProvider reads secrets from Doppler.
// `ref+doppler://<project>/<config>/<NAME>`, or `ref+doppler://<NAME>` with
// the project and config from the provider config or a scoped service token.
type dopplerProvider struct {
	cfg    ProviderConfig
	client *restClient
}

func newDopplerProvider(cfg ProviderConfig) (Provider, error) {
	token := cfg.Get("token", "DOPPLER_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("no doppler token. Set `token` in secret_providers or DOPPLER_TOKEN")
	}
	base := cfg.Get("api_url", "DOPPLER_API_ADDR")
	if base == "" {
		base = "https://api.doppler.com"
	}
	client, err := newRESTClient(cfg, base, restOptions{Header: map[string]string{"Authorization": "Bearer " + token}})
	if err != nil {
		return nil, err
	}
	return &dopplerProvider{cfg: cfg, client: client}, nil
}

// target splits a ref into project, config and secret name.
func (p *dopplerProvider) target(ref Ref) (project, config, name string, err error) {
	parts := strings.Split(strings.Trim(ref.Path, "/"), "/")
	switch len(parts) {
	case 1:
		return p.cfg.Get("project", "DOPPLER_PROJECT"), p.cfg.Get("config", "DOPPLER_CONFIG", "DOPPLER_ENVIRONMENT"), parts[0], nil
	case 3:
		return parts[0], parts[1], parts[2], nil
	}
	return "", "", "", fmt.Errorf("use ref+doppler://<project>/<config>/<NAME> or ref+doppler://<NAME>")
}

func scopeQuery(project, config string) url.Values {
	q := url.Values{}
	if project != "" {
		q.Set("project", project)
	}
	if config != "" {
		q.Set("config", config)
	}
	return q
}

func (p *dopplerProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	project, config, name, err := p.target(ref)
	if err != nil {
		return nil, err
	}
	q := scopeQuery(project, config)
	q.Set("name", name)
	var resp struct {
		Value struct {
			Raw *string `json:"raw"`
		} `json:"value"`
	}
	if _, err := p.client.Do(ctx, http.MethodGet, "v3/configs/config/secret?"+q.Encode(), nil, nil, &resp); err != nil {
		return nil, err
	}
	if resp.Value.Raw == nil {
		return nil, fmt.Errorf("doppler returned no value for %q", name)
	}
	return []byte(*resp.Value.Raw), nil
}

// GetMany downloads each project/config once and picks the names.
func (p *dopplerProvider) GetMany(ctx context.Context, refs []Ref) (map[string][]byte, error) {
	type scope struct{ project, config string }
	groups := map[scope][]Ref{}
	names := map[string]string{}
	for _, ref := range refs {
		project, config, name, err := p.target(ref)
		if err != nil {
			return nil, err
		}
		s := scope{project, config}
		groups[s] = append(groups[s], ref)
		names[ref.Key()] = name
	}

	out := map[string][]byte{}
	for s, group := range groups {
		q := scopeQuery(s.project, s.config)
		q.Set("format", "json")
		var all map[string]any
		if _, err := p.client.Do(ctx, http.MethodGet, "v3/configs/config/secrets/download?"+q.Encode(), nil, nil, &all); err != nil {
			return nil, err
		}
		for _, ref := range group {
			if v, ok := all[names[ref.Key()]]; ok && v != nil {
				out[ref.Key()] = []byte(fmt.Sprint(v))
			}
		}
	}
	return out, nil
}
