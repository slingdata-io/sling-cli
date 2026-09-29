package secrets

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"sync"

	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
	secretmanager "google.golang.org/api/secretmanager/v1"
)

func init() { Register("gcpsecrets", newGCPSecrets, "gcp_secret_manager") }

// gcpSecretsProvider reads GCP Secret Manager.
// `ref+gcpsecrets://<project>/<name>?version=latest`, or the resource name
// `ref+gcpsecrets://projects/<p>[/locations/<l>]/secrets/<n>[/versions/<v>]`.
type gcpSecretsProvider struct {
	cfg ProviderConfig

	mu   sync.Mutex
	svcs map[string]*secretmanager.Service // by location, "" is global
}

func newGCPSecrets(cfg ProviderConfig) (Provider, error) {
	return &gcpSecretsProvider{cfg: cfg, svcs: map[string]*secretmanager.Service{}}, nil
}

// service returns the client of a location. Regional secrets need the
// regional endpoint.
func (p *gcpSecretsProvider) service(ctx context.Context, location string) (*secretmanager.Service, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if svc, ok := p.svcs[location]; ok {
		return svc, nil
	}

	var opts []option.ClientOption
	if endpoint := p.cfg.Get("endpoint"); endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	} else if location != "" {
		opts = append(opts, option.WithEndpoint("https://secretmanager."+location+".rep.googleapis.com/"))
	}
	if p.cfg.Bool("no_auth") {
		opts = append(opts, option.WithoutAuthentication())
	} else {
		var credOpts []option.ClientOption
		if creds := p.cfg.Get("credentials_json"); creds != "" {
			if strings.HasPrefix(strings.TrimSpace(creds), "{") {
				credOpts = append(credOpts, option.WithCredentialsJSON([]byte(creds)))
			} else {
				b, err := os.ReadFile(creds)
				if err != nil {
					return nil, fmt.Errorf("could not read credentials_json file: %w", err)
				}
				credOpts = append(credOpts, option.WithCredentialsJSON(b))
			}
		}
		if target := p.cfg.Get("impersonate_service_account"); target != "" {
			ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
				TargetPrincipal: target,
				Scopes:          []string{"https://www.googleapis.com/auth/cloud-platform"},
			}, credOpts...)
			if err != nil {
				return nil, fmt.Errorf("could not impersonate %s: %w", target, err)
			}
			opts = append(opts, option.WithTokenSource(ts))
		} else {
			opts = append(opts, credOpts...)
		}
	}

	// the service uses a background context: it outlives this fetch
	svc, err := secretmanager.NewService(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("could not create GCP Secret Manager client: %w", err)
	}
	p.svcs[location] = svc
	return svc, nil
}

// versionName builds projects/<p>[/locations/<l>]/secrets/<n>/versions/<v>
// from the ref. It also returns the location of a regional secret.
func (p *gcpSecretsProvider) versionName(ref Ref) (name, location string, err error) {
	version := ref.Params.Get("version")
	if version == "" {
		version = "latest"
	}
	path := strings.Trim(strings.TrimPrefix(ref.Path, "//secretmanager.googleapis.com/"), "/")
	if strings.HasPrefix(path, "projects/") {
		if !strings.Contains(path, "/versions/") {
			path += "/versions/" + version
		}
		if parts := strings.Split(path, "/"); len(parts) > 3 && parts[2] == "locations" {
			location = parts[3]
		}
		return path, location, nil
	}
	parts := strings.Split(path, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("reference path must be <project>/<name>")
	}
	return fmt.Sprintf("projects/%s/secrets/%s/versions/%s", parts[0], parts[1], version), "", nil
}

func (p *gcpSecretsProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	name, location, err := p.versionName(ref)
	if err != nil {
		return nil, err
	}
	svc, err := p.service(ctx, location)
	if err != nil {
		return nil, err
	}
	var resp *secretmanager.AccessSecretVersionResponse
	if location != "" {
		resp, err = svc.Projects.Locations.Secrets.Versions.Access(name).Context(ctx).Do()
	} else {
		resp, err = svc.Projects.Secrets.Versions.Access(name).Context(ctx).Do()
	}
	if err != nil {
		return nil, err
	}
	if resp.Payload == nil {
		return nil, fmt.Errorf("secret version %s has no payload", name)
	}
	data, err := base64.StdEncoding.DecodeString(resp.Payload.Data)
	if err != nil {
		return nil, fmt.Errorf("could not decode payload of %s", name)
	}
	return data, nil
}
