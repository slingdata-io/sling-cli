package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

func init() {
	Register("vault", func(cfg ProviderConfig) (Provider, error) {
		return newVaultProvider(cfg, vaultEnv{prefix: "VAULT", tokenFiles: []string{".vault-token"}})
	}, "hashicorp_vault")
	Register("openbao", func(cfg ProviderConfig) (Provider, error) {
		return newVaultProvider(cfg, vaultEnv{prefix: "BAO", tokenFiles: []string{".bao-token", ".vault-token"}})
	}, "bao")
}

// vaultEnv holds the env var prefix and token files of Vault or OpenBao.
type vaultEnv struct {
	prefix     string   // VAULT or BAO
	tokenFiles []string // in the home folder
}

func (e vaultEnv) key(name string) string { return e.prefix + "_" + name }

// vaultMount is one secret engine mount, from sys/internal/ui/mounts.
type vaultMount struct {
	path    string // "secret/"
	version int    // 1 or 2
}

// vaultProvider reads KV v1 and v2 secrets from Vault or OpenBao. It returns
// the data map as JSON, so a pointer selects one key.
type vaultProvider struct {
	cfg    ProviderConfig
	env    vaultEnv
	client *restClient
	auth   string

	mu     sync.Mutex
	token  string
	mounts []vaultMount
	done   chan struct{} // closed by Close: stops lease renewal
}

func newVaultProvider(cfg ProviderConfig, env vaultEnv) (*vaultProvider, error) {
	addr := cfg.Get("address", env.key("ADDR"))
	if addr == "" {
		return nil, fmt.Errorf("vault address is not set. Set `address` in secret_providers or %s", env.key("ADDR"))
	}
	header := map[string]string{}
	if ns := cfg.Get("namespace", env.key("NAMESPACE")); ns != "" {
		header["X-Vault-Namespace"] = ns
	}
	client, err := newRESTClient(cfg, addr, restOptions{
		CACert:     cfg.Get("ca_cert", env.key("CACERT")),
		SkipVerify: cfg.Bool("skip_verify", env.key("SKIP_VERIFY")),
		Header:     header,
	})
	if err != nil {
		return nil, err
	}
	auth := strings.ToLower(cfg.Get("auth", env.key("AUTH_METHOD")))
	if auth == "" {
		auth = "token"
	}
	switch auth {
	case "token", "approle", "jwt", "kubernetes", "aws":
	default:
		return nil, fmt.Errorf("vault auth %q is not supported. Use token, approle, jwt, kubernetes or aws", auth)
	}
	return &vaultProvider{cfg: cfg, env: env, client: client, auth: auth}, nil
}

func (p *vaultProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	data, err := p.read(ctx, ref)
	if status := httpStatus(err); (status == http.StatusForbidden || status == http.StatusUnauthorized) && p.auth != "token" {
		// the login token can expire: log in again once
		p.mu.Lock()
		p.token = ""
		p.mu.Unlock()
		data, err = p.read(ctx, ref)
	}
	if err != nil {
		return nil, err
	}
	return json.Marshal(data)
}

func (p *vaultProvider) read(ctx context.Context, ref Ref) (map[string]any, error) {
	token, err := p.login(ctx)
	if err != nil {
		return nil, err
	}
	header := http.Header{"X-Vault-Token": {token}}
	path := strings.Trim(ref.Path, "/")

	mount := p.mount(ctx, path, header)
	switch ref.Params.Get("kv") {
	case "1":
		mount.version = 1
	case "2":
		mount.version = 2
	}

	apiPath := "v1/" + path
	if mount.version == 2 && mount.path == "" {
		// mount not known: the first segment is the mount
		first, _, _ := strings.Cut(path, "/")
		mount.path = first + "/"
	}
	if mount.version == 2 {
		rel := strings.TrimPrefix(path, strings.Trim(mount.path, "/"))
		rel = strings.TrimPrefix(rel, "/")
		if !strings.HasPrefix(rel, "data/") {
			rel = "data/" + rel
		}
		apiPath = "v1/" + strings.Trim(mount.path, "/") + "/" + rel
		if v := ref.Params.Get("version"); v != "" {
			apiPath += "?version=" + v
		}
	}

	var resp struct {
		Data          map[string]any `json:"data"`
		LeaseID       string         `json:"lease_id"`
		LeaseDuration int            `json:"lease_duration"`
		Renewable     bool           `json:"renewable"`
	}
	if _, err := p.client.Do(ctx, http.MethodGet, apiPath, nil, header, &resp); err != nil {
		return nil, err
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("vault returned no data for %q", path)
	}
	if resp.LeaseID != "" && resp.Renewable && resp.LeaseDuration > 0 && p.cfg.Get("renew_leases") != "false" {
		p.renewLease(resp.LeaseID, resp.LeaseDuration)
	}
	if mount.version == 2 {
		inner, ok := resp.Data["data"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("vault returned no KV v2 data for %q. Add ?kv=1 if the mount is KV v1", path)
		}
		return inner, nil
	}
	return resp.Data, nil
}

// renewLease keeps a dynamic secret lease (such as database/creds/<role>)
// alive during long tasks. It renews at 2/3 of the lease duration until
// Vault stops the renewal (max TTL) or Close runs.
func (p *vaultProvider) renewLease(leaseID string, seconds int) {
	p.mu.Lock()
	if p.done == nil {
		p.done = make(chan struct{})
	}
	done := p.done
	p.mu.Unlock()

	go func() {
		for {
			wait := time.Duration(seconds) * time.Second * 2 / 3
			select {
			case <-done:
				return
			case <-time.After(max(wait, 500*time.Millisecond)):
			}

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			token, err := p.login(ctx)
			if err != nil {
				cancel()
				return
			}
			var resp struct {
				LeaseDuration int  `json:"lease_duration"`
				Renewable     bool `json:"renewable"`
			}
			body := map[string]any{"lease_id": leaseID, "increment": seconds}
			_, err = p.client.Do(ctx, http.MethodPut, "v1/sys/leases/renew", body, http.Header{"X-Vault-Token": {token}}, &resp)
			cancel()
			if err != nil || !resp.Renewable || resp.LeaseDuration <= 0 {
				return
			}
			seconds = resp.LeaseDuration
		}
	}()
}

// Close stops lease renewal. The leases stay valid until they expire.
func (p *vaultProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done != nil {
		close(p.done)
		p.done = nil
	}
	return nil
}

// mount finds the mount of path. When the lookup fails (no permission), the
// path is read as is (KV v1 form). The ?kv= param overrides it.
func (p *vaultProvider) mount(ctx context.Context, path string, header http.Header) vaultMount {
	p.mu.Lock()
	for _, m := range p.mounts {
		if strings.HasPrefix(path+"/", m.path) {
			p.mu.Unlock()
			return m
		}
	}
	p.mu.Unlock()

	var resp struct {
		Data struct {
			Path    string            `json:"path"`
			Type    string            `json:"type"`
			Options map[string]string `json:"options"`
		} `json:"data"`
	}
	if _, err := p.client.Do(ctx, http.MethodGet, "v1/sys/internal/ui/mounts/"+path, nil, header, &resp); err != nil || resp.Data.Path == "" {
		return vaultMount{version: 1}
	}
	m := vaultMount{path: resp.Data.Path, version: 1}
	if strings.HasPrefix(resp.Data.Type, "kv") && resp.Data.Options["version"] == "2" {
		m.version = 2
	}
	p.mu.Lock()
	p.mounts = append(p.mounts, m)
	p.mu.Unlock()
	return m
}

// login returns the client token. It logs in once and reuses the token.
func (p *vaultProvider) login(ctx context.Context) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.token != "" {
		return p.token, nil
	}

	cfg := p.cfg
	var body map[string]any
	mount := cfg.Get("mount")
	switch p.auth {
	case "token":
		token := cfg.Get("token", p.env.key("TOKEN"))
		if token == "" {
			token = p.tokenFromFile()
		}
		if token == "" {
			return "", fmt.Errorf("no vault token. Set `token` in secret_providers, %s, or log in with the CLI", p.env.key("TOKEN"))
		}
		p.token = token
		return token, nil
	case "approle":
		body = map[string]any{
			"role_id":   cfg.Get("role_id", p.env.key("ROLE_ID")),
			"secret_id": cfg.Get("secret_id", p.env.key("SECRET_ID")),
		}
		if mount == "" {
			mount = "approle"
		}
	case "jwt":
		jwt := cfg.Get("jwt")
		if jwt == "" && cfg.Get("jwt_file") != "" {
			b, err := os.ReadFile(cfg.Get("jwt_file"))
			if err != nil {
				return "", fmt.Errorf("could not read jwt_file: %w", err)
			}
			jwt = strings.TrimSpace(string(b))
		}
		if jwt == "" {
			return "", fmt.Errorf("vault jwt auth needs `jwt` or `jwt_file`")
		}
		body = map[string]any{"role": cfg.Get("role"), "jwt": jwt}
		if mount == "" {
			mount = "jwt"
		}
	case "kubernetes":
		file := cfg.Get("token_file")
		if file == "" {
			file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		}
		b, err := os.ReadFile(file)
		if err != nil {
			return "", fmt.Errorf("could not read the service account token: %w", err)
		}
		body = map[string]any{"role": cfg.Get("role"), "jwt": strings.TrimSpace(string(b))}
		if mount == "" {
			mount = "kubernetes"
		}
	case "aws":
		var err error
		if body, err = p.awsLoginBody(ctx); err != nil {
			return "", err
		}
		if mount == "" {
			mount = "aws"
		}
	}

	var resp struct {
		Auth struct {
			ClientToken string `json:"client_token"`
		} `json:"auth"`
	}
	if _, err := p.client.Do(ctx, http.MethodPost, "v1/auth/"+strings.Trim(mount, "/")+"/login", body, nil, &resp); err != nil {
		return "", fmt.Errorf("vault %s login failed: %w", p.auth, err)
	}
	if resp.Auth.ClientToken == "" {
		return "", fmt.Errorf("vault %s login returned no token", p.auth)
	}
	p.token = resp.Auth.ClientToken
	return p.token, nil
}

func (p *vaultProvider) tokenFromFile() string {
	files := []string{}
	if f := p.cfg.Get("token_file", p.env.key("TOKEN_FILE")); f != "" {
		files = append(files, f)
	}
	if home, err := os.UserHomeDir(); err == nil {
		for _, name := range p.env.tokenFiles {
			files = append(files, filepath.Join(home, name))
		}
	}
	for _, f := range files {
		if b, err := os.ReadFile(f); err == nil {
			if t := strings.TrimSpace(string(b)); t != "" {
				return t
			}
		}
	}
	return ""
}

// awsLoginBody signs an sts:GetCallerIdentity request with the default AWS
// credential chain. Vault sends it to AWS to prove the identity.
func (p *vaultProvider) awsLoginBody(ctx context.Context) (map[string]any, error) {
	region := p.cfg.Get("region", "AWS_REGION", "AWS_DEFAULT_REGION")
	if region == "" {
		region = "us-east-1"
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("could not load AWS config: %w", err)
	}
	creds, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not get AWS credentials: %w", err)
	}

	endpoint := "https://sts.amazonaws.com/"
	if region != "us-east-1" {
		endpoint = "https://sts." + region + ".amazonaws.com/"
	}
	payload := "Action=GetCallerIdentity&Version=2011-06-15"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
	if v := p.cfg.Get("header_value"); v != "" {
		req.Header.Set("X-Vault-AWS-IAM-Server-ID", v)
	}
	sum := sha256.Sum256([]byte(payload))
	if err := v4.NewSigner().SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "sts", region, time.Now()); err != nil {
		return nil, fmt.Errorf("could not sign the AWS request: %w", err)
	}
	headers, err := json.Marshal(req.Header)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"role":                    p.cfg.Get("role"),
		"iam_http_request_method": http.MethodPost,
		"iam_request_url":         base64.StdEncoding.EncodeToString([]byte(endpoint)),
		"iam_request_body":        base64.StdEncoding.EncodeToString([]byte(payload)),
		"iam_request_headers":     base64.StdEncoding.EncodeToString(headers),
	}, nil
}
