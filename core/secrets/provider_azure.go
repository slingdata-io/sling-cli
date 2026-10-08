package secrets

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
)

func init() { Register("azurekeyvault", newAzureKeyVault, "azure_key_vault") }

// azureVaultDomains maps the `cloud` param to the Key Vault DNS suffix.
var azureVaultDomains = map[string]string{
	"public": "vault.azure.net",
	"usgov":  "vault.usgovcloudapi.net",
	"china":  "vault.azure.cn",
}

// Test hooks: tests replace the credential and the client options.
var (
	newAzureCredential = defaultAzureCredential
	azureClientOptions *azsecrets.ClientOptions
)

// azureKeyVaultProvider reads Azure Key Vault secrets.
// `ref+azurekeyvault://<vault>/<name>[/<version>]`, where <vault> is a name or
// a full DNS name (`gov-kv.vault.usgovcloudapi.net`), or the secret identifier
// `ref+azurekeyvault://https://<vault>.vault.azure.net/secrets/<name>[/<version>]`.
type azureKeyVaultProvider struct {
	cfg ProviderConfig

	mu      sync.Mutex
	cred    azcore.TokenCredential
	clients map[string]*azsecrets.Client
}

func newAzureKeyVault(cfg ProviderConfig) (Provider, error) {
	return &azureKeyVaultProvider{cfg: cfg, clients: map[string]*azsecrets.Client{}}, nil
}

func defaultAzureCredential(cfg ProviderConfig) (azcore.TokenCredential, error) {
	tenant := cfg.Get("tenant_id", "AZURE_TENANT_ID")
	clientID := cfg.Get("client_id", "AZURE_CLIENT_ID")
	secret := cfg.Get("client_secret", "AZURE_CLIENT_SECRET")
	if tenant != "" && clientID != "" && secret != "" {
		return azidentity.NewClientSecretCredential(tenant, clientID, secret, nil)
	}
	return azidentity.NewDefaultAzureCredential(nil)
}

func (p *azureKeyVaultProvider) client(vaultURL string) (*azsecrets.Client, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[vaultURL]; ok {
		return c, nil
	}
	if p.cred == nil {
		cred, err := newAzureCredential(p.cfg)
		if err != nil {
			return nil, fmt.Errorf("could not get Azure credential: %w", err)
		}
		p.cred = cred
	}
	c, err := azsecrets.NewClient(vaultURL, p.cred, azureClientOptions)
	if err != nil {
		return nil, err
	}
	p.clients[vaultURL] = c
	return c, nil
}

func (p *azureKeyVaultProvider) Get(ctx context.Context, ref Ref) ([]byte, error) {
	if strings.HasPrefix(ref.Path, "https://") {
		return p.getByID(ctx, ref.Path)
	}
	parts := strings.Split(strings.Trim(ref.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("reference path must be <vault>/<name>[/<version>]")
	}
	version := ""
	if len(parts) == 3 {
		version = parts[2]
	}

	vaultURL := p.cfg.Get("vault_url")
	if vaultURL == "" {
		cloud := strings.ToLower(ref.Params.Get("cloud"))
		if cloud == "" {
			cloud = strings.ToLower(p.cfg.Get("cloud"))
		}
		if cloud == "" {
			cloud = "public"
		}
		domain, ok := azureVaultDomains[cloud]
		if !ok {
			return nil, fmt.Errorf("unknown cloud %q. Use public, usgov or china", cloud)
		}
		vaultURL = "https://" + parts[0] + "." + domain
		if strings.Contains(parts[0], ".") {
			vaultURL = "https://" + parts[0] // full DNS name, for other clouds
		}
	}

	return p.get(ctx, vaultURL, parts[1], version)
}

// getByID reads a secret from its identifier, as the Azure portal shows it.
func (p *azureKeyVaultProvider) getByID(ctx context.Context, id string) ([]byte, error) {
	u, err := url.Parse(id)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid secret identifier %q", id)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "secrets" || parts[1] == "" {
		return nil, fmt.Errorf("secret identifier must be https://<vault host>/secrets/<name>[/<version>]")
	}
	if strings.HasSuffix(u.Hostname(), ".managedhsm.azure.net") {
		return nil, fmt.Errorf("managed HSM does not store secrets: use a Key Vault secret identifier")
	}
	version := ""
	if len(parts) == 3 {
		version = parts[2]
	}
	host := strings.TrimSuffix(u.Host, ":443")
	return p.get(ctx, "https://"+host, parts[1], version)
}

func (p *azureKeyVaultProvider) get(ctx context.Context, vaultURL, name, version string) ([]byte, error) {
	c, err := p.client(vaultURL)
	if err != nil {
		return nil, err
	}
	resp, err := c.GetSecret(ctx, name, version, nil)
	if err != nil {
		return nil, err
	}
	if resp.Value == nil {
		return nil, fmt.Errorf("secret %s has no value", name)
	}
	return []byte(*resp.Value), nil
}
